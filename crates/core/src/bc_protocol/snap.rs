// use futures::{StreamExt, TryStreamExt};

use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};
use std::convert::TryFrom;

/// The byte limit [`BcCamera::get_snapshot`] applies: 16 MiB.
pub const DEFAULT_SNAPSHOT_LIMIT: usize = 16 * 1024 * 1024;

/// Collects the binary packets of one snapshot and refuses any byte past the
/// caller's limit. The size the camera declares sets the first allocation and
/// is not a bound. A camera may send more or fewer bytes than it declares.
/// Neolink has always accepted those images, and upstream commit 0f28154
/// lowered the mismatch log from warn to debug.
#[derive(Debug)]
struct SnapshotBuffer {
    data: Vec<u8>,
    declared: usize,
    limit: usize,
    logged_overrun: bool,
}

impl SnapshotBuffer {
    /// Refuses a declared size over `limit` before any binary packet is read.
    fn new(declared: usize, limit: usize) -> Result<Self> {
        if declared > limit {
            return Err(Error::SnapshotTooLarge {
                declared,
                received: 0,
                limit,
            });
        }
        Ok(Self {
            // declared is at most limit here
            data: Vec::with_capacity(declared),
            declared,
            limit,
            logged_overrun: false,
        })
    }

    /// Appends one packet, or refuses it whole when it would pass the limit.
    /// Nothing of a refused packet is kept, and the capacity never grows past
    /// the limit.
    fn push(&mut self, chunk: &[u8]) -> Result<()> {
        let total = match self.data.len().checked_add(chunk.len()) {
            Some(total) if total <= self.limit => total,
            _ => {
                return Err(Error::SnapshotTooLarge {
                    declared: self.declared,
                    received: self.data.len(),
                    limit: self.limit,
                })
            }
        };
        if total > self.declared && !self.logged_overrun {
            self.logged_overrun = true;
            log::debug!(
                "Snap is longer than its declared {} bytes, accepting up to the {} byte limit",
                self.declared,
                self.limit
            );
        }
        if total > self.data.capacity() {
            // Grow by doubling as Vec would, but never past the limit
            let target = total
                .max(self.data.capacity().saturating_mul(2))
                .min(self.limit);
            self.data.reserve_exact(target - self.data.len());
        }
        self.data.extend_from_slice(chunk);
        Ok(())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn into_inner(self) -> Vec<u8> {
        self.data
    }
}

impl BcCamera {
    /// Get the snapshot image, refusing one larger than
    /// [`DEFAULT_SNAPSHOT_LIMIT`]. See [`BcCamera::get_snapshot_with_limit`].
    pub async fn get_snapshot(&self) -> Result<Vec<u8>> {
        self.get_snapshot_with_limit(DEFAULT_SNAPSHOT_LIMIT).await
    }

    /// Get the snapshot image, holding at most `limit` bytes of it.
    ///
    /// Returns [`Error::SnapshotTooLarge`] when the size the camera declares
    /// is over `limit`, before any image byte is read, or when the bytes the
    /// camera sends would pass `limit`. Bytes past the declared size are
    /// accepted up to `limit`, with a debug log line. On that error the subscription for
    /// the image is dropped and the connection stays open. Packets the camera
    /// still sends for the refused image are ignored by the connection.
    pub async fn get_snapshot_with_limit(&self, limit: usize) -> Result<Vec<u8>> {
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_get = connection.subscribe(MSG_ID_SNAP, msg_num).await?;
        let get = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_SNAP,
                channel_id: self.channel_id,
                msg_num,
                response_code: 0,
                stream_type: 0,
                class: 0x6414,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    channel_id: Some(self.channel_id),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::BcXml(BcXml {
                    snap: Some(Snap {
                        version: "1.1".to_string(),
                        channel_id: self.channel_id,
                        logic_channel: Some(self.channel_id),
                        time: 0,
                        full_frame: Some(0),
                        stream_type: Some("main".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            }),
        };

        sub_get.send(get).await?;
        let msg = sub_get.recv().await?;
        if msg.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: msg.meta.msg_id,
                code: msg.meta.response_code,
            });
        }

        if let BcBody::ModernMsg(ModernMsg {
            payload:
                Some(BcPayloads::BcXml(BcXml {
                    snap:
                        Some(Snap {
                            file_name: Some(filename),
                            picture_size: Some(expected_size),
                            ..
                        }),
                    ..
                })),
            ..
        }) = msg.body
        {
            drop(sub_get); // Ensure that we are NOT listening on that msgnum as the reply can come on ANY msgnum
            log::trace!("Got snap XML {} with size {}", filename, expected_size);
            // Refuse an oversized image before listening for its packets
            let expected_size = usize::try_from(expected_size).unwrap_or(usize::MAX);
            let mut result = SnapshotBuffer::new(expected_size, limit)?;
            // Messages are now sent on ID 109 but not with the same message ID
            // preumably because the camera considers it to be a new message rather
            // than a reply
            //
            // This means we need to listen for the next 109 grab the message num and
            // subscribe to it. This is what `subscribe_to_next` is for
            let mut sub_get = connection.subscribe_to_id(MSG_ID_SNAP).await?;

            log::trace!("Waiting for packets on {}", msg_num);
            let mut msg = sub_get.recv().await?;

            while msg.meta.response_code == 200 {
                // sends 200 while more is to come
                //       201 when finished

                if let BcBody::ModernMsg(ModernMsg {
                    extension:
                        Some(Extension {
                            binary_data: Some(1),
                            ..
                        }),
                    payload: Some(BcPayloads::Binary(data)),
                }) = msg.body
                {
                    result.push(&data)?;
                } else {
                    return Err(Error::UnintelligibleReply {
                        reply: std::sync::Arc::new(Box::new(msg)),
                        why: "Expected binary data but got something else",
                    });
                }
                log::trace!(
                    "Got packet size is now {} of {}",
                    result.len(),
                    expected_size
                );
                msg = sub_get.recv().await?;
            }

            if msg.meta.response_code == 201 {
                // 201 means all binary data sent
                if let BcBody::ModernMsg(ModernMsg {
                    extension:
                        Some(Extension {
                            binary_data: Some(1),
                            ..
                        }),
                    payload,
                }) = msg.body
                {
                    if let Some(BcPayloads::Binary(data)) = payload {
                        // Add last data if present (may be zero if preveious packet contained it)
                        result.push(&data)?;
                    }
                    log::trace!(
                        "Got all packets size is now {} of {}",
                        result.len(),
                        expected_size
                    );
                    if result.len() != expected_size {
                        log::debug!("Snap did not recieve expected number of bytes");
                    }
                } else {
                    return Err(Error::UnintelligibleReply {
                        reply: std::sync::Arc::new(Box::new(msg)),
                        why: "Expected binary data but got something else",
                    });
                }
            } else {
                // anything else is an error
                return Err(Error::CameraServiceUnavailable {
                    id: msg.meta.msg_id,
                    code: msg.meta.response_code,
                });
            }

            // let binary_stream = sub_get.payload_stream();
            // let result: Vec<_>= binary_stream
            //     .map_ok(|i| tokio_stream::iter(i).map(Result::Ok))
            //     .try_flatten()
            //     .take(expected_size)
            //     .try_collect()
            //     .await?;
            log::trace!("Snapshot recieved: {} of {}", result.len(), expected_size);
            Ok(result.into_inner())
        } else {
            Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "Expected Snap xml but it was not recieved",
            })
        }
    }
}

#[cfg(test)]
mod tests {
    //! An in-process fake camera: no network and no real device. Every image
    //! byte and file name here is SYNTHETIC.
    use super::*;
    use crate::bc_protocol::{BcConnSink, BcConnSource, BcConnection, Credentials};
    use futures::{channel::mpsc::unbounded, SinkExt, StreamExt};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    // The pure buffer

    #[test]
    fn buffer_under_the_limit_is_unchanged() {
        let image: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let mut buf = SnapshotBuffer::new(image.len(), 4096).expect("declared under limit");
        for chunk in image.chunks(300) {
            buf.push(chunk).expect("chunk inside the declared size");
        }
        assert_eq!(buf.into_inner(), image);
    }

    #[test]
    fn buffer_refuses_a_declared_size_over_the_limit() {
        match SnapshotBuffer::new(4097, 4096) {
            Err(Error::SnapshotTooLarge {
                declared: 4097,
                received: 0,
                limit: 4096,
            }) => {}
            other => panic!("expected SnapshotTooLarge, got {:?}", other),
        }
    }

    #[test]
    fn buffer_accepts_data_past_the_declared_size_up_to_the_limit() {
        // A camera that sends a few bytes more than it declares, as before
        let mut buf = SnapshotBuffer::new(100, 4096).expect("declared under limit");
        buf.push(&[1u8; 60]).expect("first chunk");
        buf.push(&[2u8; 48])
            .expect("past the declared size, under the limit");
        assert_eq!(buf.len(), 108);
        assert!(buf.data.capacity() <= 4096);
    }

    #[test]
    fn buffer_refuses_a_chunk_past_the_limit_whole() {
        let mut buf = SnapshotBuffer::new(100, 200).expect("declared under limit");
        buf.push(&[1u8; 150])
            .expect("past the declared size, under the limit");
        match buf.push(&[2u8; 51]) {
            Err(Error::SnapshotTooLarge {
                declared: 100,
                received: 150,
                limit: 200,
            }) => {}
            other => panic!("expected SnapshotTooLarge, got {:?}", other),
        }
        // Nothing of the refused chunk is kept and nothing grew past the limit
        assert_eq!(buf.len(), 150);
        assert!(buf.data.capacity() <= 200);
        buf.push(&[3u8; 50]).expect("exactly the limit");
        assert_eq!(buf.len(), 200);
    }

    #[test]
    fn buffer_never_holds_more_than_the_limit() {
        let limit = 1024;
        // Declared at the limit, and declared as zero, which grows from empty
        for declared in [limit, 0] {
            let mut buf = SnapshotBuffer::new(declared, limit).expect("declared at most the limit");
            let mut refused = false;
            for _ in 0..100 {
                if buf.push(&[7u8; 100]).is_err() {
                    refused = true;
                    break;
                }
                assert!(buf.data.capacity() <= limit);
            }
            assert!(refused);
            assert_eq!(buf.len(), 1000);
            assert!(buf.data.capacity() <= limit);
        }
    }

    #[test]
    fn buffer_takes_exactly_the_declared_size() {
        let mut buf = SnapshotBuffer::new(10, 10).expect("declared at the limit");
        buf.push(&[3u8; 10]).expect("exactly the declared size");
        buf.push(&[]).expect("an empty last packet");
        assert_eq!(buf.into_inner(), vec![3u8; 10]);
    }

    // The fake camera

    /// What the fake camera does for one snap request.
    struct Plan {
        declared: u32,
        chunks: Vec<Vec<u8>>,
    }

    fn snap_reply(request: &Bc, declared: u32) -> Bc {
        Bc {
            meta: BcMeta {
                response_code: 200,
                ..request.meta
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::BcXml(BcXml {
                    snap: Some(Snap {
                        version: "1.1".to_string(),
                        file_name: Some("01_20000101000000.jpg".to_string()),
                        picture_size: Some(declared),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            }),
        }
    }

    fn binary_packet(msg_num: u16, code: u16, data: Vec<u8>) -> Bc {
        Bc {
            meta: BcMeta {
                msg_id: MSG_ID_SNAP,
                channel_id: 0,
                msg_num,
                response_code: code,
                stream_type: 0,
                class: 0x0000,
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    binary_data: Some(1),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::Binary(data)),
            }),
        }
    }

    /// A fake camera that answers each snap request with the next plan: the
    /// snap XML, then each chunk as a 200 packet on a new message number,
    /// then an empty 201. `sent` counts the binary packets it sent.
    async fn fake_camera(plans: Vec<Plan>) -> (BcCamera, Arc<AtomicUsize>) {
        let sent = Arc::new(AtomicUsize::new(0));
        let counter = sent.clone();
        let (to_cam, mut cam_rx) = unbounded::<Bc>();
        let (cam_tx, from_cam) = unbounded::<Result<Bc>>();
        tokio::spawn(async move {
            let mut plans = plans.into_iter();
            let mut data_num = 900u16;
            while let Some(request) = cam_rx.next().await {
                let Some(plan) = plans.next() else { break };
                let _ = cam_tx.unbounded_send(Ok(snap_reply(&request, plan.declared)));
                // The camera starts the image a moment later. With paused
                // time this sleep ends once the caller waits for packets.
                tokio::time::sleep(Duration::from_millis(10)).await;
                for chunk in plan.chunks {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let _ = cam_tx.unbounded_send(Ok(binary_packet(data_num, 200, chunk)));
                }
                let _ = cam_tx.unbounded_send(Ok(binary_packet(data_num, 201, vec![])));
                data_num += 1;
            }
        });
        let sink: BcConnSink =
            Box::new(to_cam.sink_map_err(|_| Error::Other("fake camera is gone")));
        let source: BcConnSource = Box::new(from_cam);
        let connection = BcConnection::new(sink, source)
            .await
            .expect("in-memory connection");
        let camera = BcCamera {
            channel_id: 0,
            connection: Arc::new(connection),
            logged_in: AtomicBool::new(true),
            message_num: AtomicU16::new(0),
            credentials: Credentials::new("fake", None::<String>),
            abilities: RwLock::new(HashMap::new()),
            cancel: CancellationToken::new(),
        };
        (camera, sent)
    }

    fn image(len: usize) -> Vec<u8> {
        (0..=255u8).cycle().take(len).collect()
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn snapshot_under_the_limit_is_unchanged() {
        let picture = image(10_000);
        let (camera, _) = fake_camera(vec![Plan {
            declared: picture.len() as u32,
            chunks: picture.chunks(4096).map(<[u8]>::to_vec).collect(),
        }])
        .await;
        let got = tokio::time::timeout(
            Duration::from_secs(60),
            camera.get_snapshot_with_limit(16_384),
        )
        .await
        .expect("snapshot hung")
        .expect("snapshot");
        assert_eq!(got, picture);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn snapshot_a_little_past_its_declared_size_is_accepted() {
        // The camera declares 8000 bytes and sends 8100, which neolink has
        // always accepted
        let picture = image(8100);
        let (camera, _) = fake_camera(vec![Plan {
            declared: 8000,
            chunks: picture.chunks(4096).map(<[u8]>::to_vec).collect(),
        }])
        .await;
        let got = tokio::time::timeout(
            Duration::from_secs(60),
            camera.get_snapshot_with_limit(16_384),
        )
        .await
        .expect("snapshot hung")
        .expect("snapshot");
        assert_eq!(got, picture);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn snapshot_past_the_limit_is_refused_and_the_connection_works() {
        // The camera declares 8000 bytes and sends 100 packets of 4096
        let good = image(3000);
        let (camera, _) = fake_camera(vec![
            Plan {
                declared: 8000,
                chunks: vec![vec![9u8; 4096]; 100],
            },
            Plan {
                declared: good.len() as u32,
                chunks: vec![good.clone()],
            },
        ])
        .await;
        let refused = tokio::time::timeout(
            Duration::from_secs(60),
            camera.get_snapshot_with_limit(16_384),
        )
        .await
        .expect("snapshot hung");
        // Four packets fill the limit exactly and the fifth is refused
        match refused {
            Err(Error::SnapshotTooLarge {
                declared: 8000,
                received: 16_384,
                limit: 16_384,
            }) => {}
            other => panic!("expected SnapshotTooLarge, got {:?}", other),
        }

        // The same connection serves the next snapshot
        let got = tokio::time::timeout(Duration::from_secs(60), camera.get_snapshot())
            .await
            .expect("second snapshot hung")
            .expect("second snapshot");
        assert_eq!(got, good);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn snapshot_with_a_declared_size_over_the_limit_is_refused_early() {
        let (camera, sent) = fake_camera(vec![Plan {
            declared: 20_000,
            chunks: vec![vec![5u8; 4096]; 5],
        }])
        .await;
        let refused = tokio::time::timeout(
            Duration::from_secs(60),
            camera.get_snapshot_with_limit(16_384),
        )
        .await
        .expect("snapshot hung");
        match refused {
            Err(Error::SnapshotTooLarge {
                declared: 20_000,
                received: 0,
                limit: 16_384,
            }) => {}
            other => panic!("expected SnapshotTooLarge, got {:?}", other),
        }
        // Refused on the snap XML, before the camera sent any image packet
        assert_eq!(sent.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn get_snapshot_applies_the_default_limit() {
        let over = (DEFAULT_SNAPSHOT_LIMIT + 1) as u32;
        let (camera, sent) = fake_camera(vec![Plan {
            declared: over,
            chunks: vec![vec![5u8; 4096]],
        }])
        .await;
        let refused = tokio::time::timeout(Duration::from_secs(60), camera.get_snapshot())
            .await
            .expect("snapshot hung");
        match refused {
            Err(Error::SnapshotTooLarge {
                declared,
                received: 0,
                limit: DEFAULT_SNAPSHOT_LIMIT,
            }) if declared == DEFAULT_SNAPSHOT_LIMIT + 1 => {}
            other => panic!("expected SnapshotTooLarge, got {:?}", other),
        }
        assert_eq!(sent.load(Ordering::SeqCst), 0);
    }
}
