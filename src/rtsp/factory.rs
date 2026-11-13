use gstreamer::ClockTime;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

use crate::{
    common::NeoInstance,
    config::{AudioCodec, CameraConfig},
    rtsp::gst::{MediaKey, NeoMediaFactory},
    AnyResult,
};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

/// Sample rate and channel count of the camera's AAC, read from the ADTS
/// header of a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct AacFormat {
    pub(super) rate: i32,
    pub(super) channels: i32,
}

impl AacFormat {
    /// Reads the fixed ADTS header (ISO/IEC 14496-3, 1.A.2.2.1): a 12 bit
    /// syncword, then at bit 18 the 4 bit sampling frequency index and at
    /// bit 23 the 3 bit channel configuration. Returns `None` for anything
    /// else, and for an index or configuration that does not name a fixed
    /// rate or channel count.
    pub(super) fn from_adts(data: &[u8]) -> Option<Self> {
        if data.len() < 7 || data[0] != 0xff || data[1] & 0xf0 != 0xf0 {
            return None;
        }
        let rate = match (data[2] >> 2) & 0x0f {
            0 => 96000,
            1 => 88200,
            2 => 64000,
            3 => 48000,
            4 => 44100,
            5 => 32000,
            6 => 24000,
            7 => 22050,
            8 => 16000,
            9 => 12000,
            10 => 11025,
            11 => 8000,
            12 => 7350,
            _ => return None,
        };
        let channels = match ((data[2] & 0x01) << 2) | (data[3] >> 6) {
            // 0 means the layout is in a program config element instead.
            0 => return None,
            7 => 8,
            n => n as i32,
        };
        Some(Self { rate, channels })
    }
}

#[derive(Clone, Debug)]
pub(super) struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
    aac_format: Option<AacFormat>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
            aac_format: None,
        })
    }

    /// A config for a pipeline test that has no camera to ask.
    #[cfg(test)]
    pub(super) fn for_test(fps: u32) -> Self {
        StreamConfig {
            resolution: [0, 0],
            bitrate: 0,
            fps,
            bitrate_table: vec![],
            fps_table: vec![],
            vid_type: None,
            aud_type: None,
            aac_format: None,
        }
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    pub(super) fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(aac) => {
                self.aud_type = Some(AudioType::Aac);
                if let Some(format) = AacFormat::from_adts(&aac.data) {
                    self.aac_format = Some(format);
                }
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element, _key| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

/// How long a connecting RTSP client will wait for its pipeline to be built
/// before we give up. This bounds the time the gst-rtsp-server client thread
/// can be blocked inside the media factory callback. Without a bound, one
/// camera that will not deliver frames wedges that thread forever, and every
/// subsequent client on every camera queues behind it.
const CLIENT_BUILD_TIMEOUT: Duration = Duration::from_secs(15);

/// How long to wait for enough frames to identify the stream format before
/// giving up and building whatever we know about. Must be comfortably less
/// than `CLIENT_BUILD_TIMEOUT`.
const STREAM_LEARN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an idle frame-pusher thread wakes to check whether its pipeline
/// still exists. Bounds how long a thread survives after its client has gone
/// away without the camera sending another frame.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Backoff between attempts to resubscribe to the camera after its stream
/// ends while a client is still watching. Doubles up to the max.
const RESUBSCRIBE_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RESUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Longest wait for one resubscribe attempt, so the pusher keeps checking
/// whether its client is still there while the camera is unreachable.
const RESUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a frame pusher waits for its pipeline to go live. gst-rtsp-server
/// prepares the media right after the factory returns it, so a pipeline
/// still NULL after this was never used, for example because its client gave
/// up just as the pipeline was handed over.
const PIPELINE_LIVE_DEADLINE: Duration = Duration::from_secs(30);

/// Decides from its pipeline's state whether a frame pusher keeps going.
struct PipelineWatch {
    started: std::time::Instant,
    was_live: bool,
}

impl PipelineWatch {
    fn new(now: std::time::Instant) -> Self {
        Self {
            started: now,
            was_live: false,
        }
    }

    /// False once the pusher should stop: its pipeline was live and has gone
    /// back to NULL, or it never went live within `PIPELINE_LIVE_DEADLINE`.
    /// The pipeline is still NULL while gst-rtsp-server prepares it, so NULL
    /// alone does not mean gone.
    fn keep_going(&mut self, alive: bool, now: std::time::Instant) -> bool {
        if alive {
            self.was_live = true;
            true
        } else if self.was_live {
            false
        } else {
            now.saturating_duration_since(self.started) < PIPELINE_LIVE_DEADLINE
        }
    }
}

/// True while an appsrc is still attached to a live pipeline.
fn source_alive(src: &AppSrc) -> bool {
    let element: &Element = src.upcast_ref();
    element.parent().is_some() && element.current_state() != gstreamer::State::Null
}

/// True while at least one of this client's sources is still live.
fn sources_alive(vid: &Option<AppSrc>, aud: &Option<AppSrc>) -> bool {
    vid.as_ref().is_some_and(source_alive) || aud.as_ref().is_some_and(source_alive)
}

/// Subscribes to the camera again after its stream ended while a client is
/// still watching. The frame pusher calls this from its plain std thread,
/// which has no runtime context, so `subscribe` and its timeout are both made
/// inside `runtime.block_on`. Before each attempt it sleeps `backoff`, which
/// doubles up to `RESUBSCRIBE_BACKOFF_MAX`, and gives up on an attempt after
/// `limit`. Returns the new stream, or `None` once `watching` says the client
/// has gone.
fn resubscribe<T, Fut>(
    runtime: &tokio::runtime::Handle,
    backoff: &mut Duration,
    limit: Duration,
    mut watching: impl FnMut() -> bool,
    mut subscribe: impl FnMut() -> Fut,
    label: &str,
) -> Option<T>
where
    Fut: std::future::Future<Output = AnyResult<T>>,
{
    loop {
        std::thread::sleep(*backoff);
        *backoff = (*backoff * 2).min(RESUBSCRIBE_BACKOFF_MAX);
        if !watching() {
            return None;
        }
        // Bounded, so a client that leaves while the camera is offline is
        // still noticed on the next pass.
        match runtime.block_on(async { tokio::time::timeout(limit, subscribe()).await }) {
            Ok(Ok(new_rx)) => {
                log::debug!("{label}: Camera stream resubscribed");
                return Some(new_rx);
            }
            Ok(Err(e)) => {
                log::debug!("{label}: Camera stream resubscribe failed: {e:?}");
            }
            Err(_) => {
                log::debug!("{label}: Camera stream resubscribe timed out");
            }
        }
    }
}

/// Runs when a frame pusher exits, however it exits. Nothing will feed this
/// media again, so end its streams and retire its cache key; otherwise the
/// shared media stays cached and every later client joins a pipeline that
/// never gets another frame. Retiring the key works on every GStreamer
/// version, unlike `set_shared(false)`, which the factory only consults from
/// 1.24 on.
struct PusherExit {
    vid_src: Option<AppSrc>,
    aud_src: Option<AppSrc>,
    key: MediaKey,
}

impl Drop for PusherExit {
    fn drop(&mut self) {
        for src in self.vid_src.iter().chain(self.aud_src.iter()) {
            let _ = src.end_of_stream();
        }
        self.key.retire();
    }
}

/// Starts a frame pusher on a thread named for its camera and stream, so a
/// panic message names them too. When the pusher ends on an error or a panic,
/// a warn line names the camera, the stream and the cause. The pusher's own
/// cleanup, `PusherExit`, runs on both paths as before, so the RTSP client
/// sees the same end of stream.
fn spawn_pusher<F>(
    camera: &str,
    stream: StreamKind,
    body: F,
) -> std::io::Result<std::thread::JoinHandle<()>>
where
    F: FnOnce() -> AnyResult<()> + Send + 'static,
{
    let camera = camera.replace('\0', "");
    std::thread::Builder::new()
        .name(format!("pusher {camera}::{stream}"))
        .spawn(
            move || match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => log::warn!(
                    "Frame pusher for camera {camera} stream {stream} ended on an error: {e:#}"
                ),
                Err(panic) => {
                    let cause = panic
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("a panic with no message");
                    log::warn!(
                        "Frame pusher for camera {camera} stream {stream} ended on a panic: {cause}"
                    );
                }
            },
        )
}

enum ClientMsg {
    NewClient {
        element: Element,
        /// A std channel rather than a tokio oneshot: the receiving end is
        /// waited on from the gst-rtsp-server client thread, which is not a
        /// tokio thread, and `recv_timeout` needs no runtime handle.
        reply: std::sync::mpsc::SyncSender<Element>,
        key: MediaKey,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient {
                    element,
                    reply,
                    key,
                } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let mut media_rx = camera.stream_while_live(stream).await?;

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type
                        let mut buffer = vec![];
                        let mut frame_count = 0usize;

                        let mut stream_config = StreamConfig::new(&camera, stream).await?;
                        // Bounded: a camera that never delivers must not hold
                        // the client thread that is waiting on `reply`.
                        let learn_deadline = tokio::time::Instant::now() + STREAM_LEARN_TIMEOUT;
                        loop {
                            match tokio::time::timeout_at(learn_deadline, media_rx.recv()).await {
                                Ok(Some(media)) => {
                                    stream_config.update_from_media(&media);
                                    buffer.push(media);
                                    if frame_count > 10
                                        || (stream_config.vid_type.is_some()
                                            && stream_config.aud_type.is_some())
                                    {
                                        break;
                                    }
                                    frame_count += 1;
                                }
                                Ok(None) => {
                                    log::debug!(
                                        "{name}::{stream}: Camera stream closed while learning format"
                                    );
                                    break;
                                }
                                Err(_) => {
                                    log::warn!(
                                        "{name}::{stream}: Timed out after {}s learning stream format ({} frames); continuing with what is known",
                                        STREAM_LEARN_TIMEOUT.as_secs(),
                                        frame_count
                                    );
                                    break;
                                }
                            }
                        }

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                // A splash here would be cached as the shared
                                // media and end after its test buffers, so
                                // every later client would get a dead stream.
                                // Refuse this client instead; it will retry.
                                log::warn!(
                                    "{name}::{stream}: No video while learning stream format; refusing client"
                                );
                                return AnyResult::Ok(());
                            }
                        }?;

                        // Build the right audio pipeline
                        let camera_config = camera.config().await?.borrow().clone();
                        let aud_src = build_audio_for(&element, &camera_config, &stream_config)?;

                        if let Some(app) = vid_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }
                        if let Some(app) = aud_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start
                        // If this fails the client already gave up waiting, so
                        // returning here drops `media_rx` and releases the
                        // camera's use-counter permit instead of leaking both
                        // the stream and the pusher thread below.
                        if reply.send(element).is_err() {
                            log::debug!(
                                "{name}::{stream}: Client gave up before the pipeline was ready; releasing stream"
                            );
                            return AnyResult::Ok(());
                        }

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        // The handle lets the thread wait on the async channel
                        // with a timeout; a plain std thread is never inside an
                        // async context, so block_on is safe here. For the
                        // same reason every tokio timer this thread uses must
                        // be made inside block_on. Made outside, it panics
                        // with "there is no reactor running".
                        let runtime = tokio::runtime::Handle::current();
                        let camera_name = name.clone();
                        let pusher = spawn_pusher(&camera_name, stream, move || {
                            let _exit = PusherExit {
                                vid_src: vid_src.clone(),
                                aud_src: aud_src.clone(),
                                key,
                            };
                            let mut clock = MediaClock::new(stream_config.fps);
                            let mut stats = TsStats::new(format!("{name}::{stream}"));
                            // P-frames are dropped until a keyframe has been
                            // pushed: at start, after any dropped frame and
                            // after resubscribing to the camera.
                            let mut need_keyframe = true;

                            log::trace!("{name}::{stream}: Sending buffered frames");
                            for buffered in buffer.drain(..) {
                                send_to_sources(
                                    buffered,
                                    &vid_src,
                                    &aud_src,
                                    &mut clock,
                                    &stream_config,
                                    &mut stats,
                                    &mut need_keyframe,
                                )?;
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            let mut backoff = RESUBSCRIBE_BACKOFF_MIN;
                            let mut watch = PipelineWatch::new(std::time::Instant::now());
                            'frames: loop {
                                // Bounded wait so that a client that has gone
                                // away is noticed even if the camera has also
                                // stopped sending. An unbounded blocking_recv
                                // here strands the thread, and with it the
                                // camera stream it holds open, until restart.
                                let next = runtime.block_on(async {
                                    tokio::time::timeout(IDLE_CHECK_INTERVAL, media_rx.recv()).await
                                });
                                let data = match next {
                                    Ok(Some(data)) => {
                                        backoff = RESUBSCRIBE_BACKOFF_MIN;
                                        data
                                    }
                                    Ok(None) => {
                                        // The camera side ended (relogin,
                                        // dropped connection) but the client
                                        // is still watching. Subscribe again
                                        // rather than leave it on a media
                                        // that will never get another frame.
                                        match resubscribe(
                                            &runtime,
                                            &mut backoff,
                                            RESUBSCRIBE_TIMEOUT,
                                            || sources_alive(&vid_src, &aud_src),
                                            || camera.stream_while_live(stream),
                                            &format!("{name}::{stream}"),
                                        ) {
                                            Some(new_rx) => {
                                                media_rx = new_rx;
                                                need_keyframe = true;
                                                clock.camera_restarted();
                                                continue 'frames;
                                            }
                                            None => break 'frames,
                                        }
                                    }
                                    Err(_) => {
                                        if sources_alive(&vid_src, &aud_src) {
                                            continue;
                                        }
                                        log::debug!(
                                            "{name}::{stream}: Pipeline gone while idle; stopping"
                                        );
                                        break;
                                    }
                                };
                                // Follow a frame rate change reported
                                // mid-stream.
                                if let BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
                                | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) = &data
                                {
                                    stream_config.update_fps(*fps as u32);
                                    clock.set_fps(stream_config.fps);
                                }
                                let r = send_to_sources(
                                    data,
                                    &vid_src,
                                    &aud_src,
                                    &mut clock,
                                    &stream_config,
                                    &mut stats,
                                    &mut need_keyframe,
                                );
                                if let Err(r) = &r {
                                    // Normal at teardown: the media was
                                    // unprepared while this thread still had
                                    // frames in flight.
                                    log::debug!("Failed to send to source: {r:?}");
                                }
                                r?;
                                // An unprepared media leaves this bin in a
                                // NULL pipeline where pushes still succeed
                                // (as Flushing), so check after every frame
                                // rather than only when the camera goes idle.
                                // Otherwise this thread keeps the camera
                                // stream open for a pipeline nobody plays.
                                if !watch.keep_going(
                                    sources_alive(&vid_src, &aud_src),
                                    std::time::Instant::now(),
                                ) {
                                    log::debug!(
                                        "{name}::{stream}: Pipeline gone or never went live; stopping"
                                    );
                                    break;
                                }
                            }
                            log::trace!("All media recieved");
                            AnyResult::Ok(())
                        });
                        if let Err(e) = pusher {
                            log::warn!(
                                "Frame pusher for camera {camera_name} stream {stream} did not start: {e}"
                            );
                        }
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element, key| {
        let (reply, new_element) = std::sync::mpsc::sync_channel::<Element>(1);
        // try_send, not blocking_send: if the builder task is wedged we must
        // fail this client rather than block the shared client thread.
        client_tx
            .try_send(ClientMsg::NewClient {
                element,
                reply,
                key,
            })
            .map_err(|e| anyhow!("Pipeline builder is not accepting clients: {e}"))?;

        match new_element.recv_timeout(CLIENT_BUILD_TIMEOUT) {
            Ok(element) => Ok(Some(element)),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow!("Pipeline builder gave up without a pipeline"))
            }
            Err(e) => Err(anyhow!(
                "Timed out after {}s waiting for the pipeline: {e}",
                CLIENT_BUILD_TIMEOUT.as_secs()
            )),
        }
    })
    .await?;
    Ok((factory, thread))
}

/// Generates presentation timestamps for both media channels off one origin.
///
/// Each channel advances at its own nominal rate - the frame interval for
/// video, the decoded frame duration for audio - so spacing stays even. Both
/// are then nudged each frame to stay locked to a shared monotonic clock.
///
/// Without the lock, either channel accumulates error for the whole session:
/// a real 14.99fps against a nominal 15 is 667ppm, and audio has the same
/// problem via its sample rate plus any lost frame, which is permanent once
/// it happens. Sharing one origin is what keeps the two channels in sync with
/// each other rather than merely each being close to correct on its own.
struct MediaClock {
    started: std::time::Instant,
    vid_pts_us: u64,
    vid_step_us: u64,
    /// PTS of the last video frame handed out. `vid_pts_us` is already the
    /// next frame's PTS, one step later. None until the first video frame.
    last_vid_pts_us: Option<u64>,
    /// Camera timestamp of the last video frame, for frame spacing.
    last_cam_us: Option<u32>,
    aud_pts_us: u64,
    aud_started: bool,
    /// Extra lead applied to audio, in microseconds. Positive moves audio
    /// earlier. Set at runtime with NEOLINK_AUDIO_LEAD_MS for cameras that
    /// buffer more than a single frame before transmitting.
    aud_lead_us: i64,
    vid_snaps: u64,
    aud_snaps: u64,
}

impl MediaClock {
    /// Video further behind the wall clock than this jumps forward instead
    /// of slewing.
    const VID_SNAP_US: i64 = 1_000_000;
    /// Audio further behind the video timeline than this jumps forward
    /// instead of slewing. Wider than two 64ms AAC frames, so audio that
    /// arrives a few frames at a time does not trip it.
    const AUD_RESYNC_US: i64 = 150_000;

    fn new(fps: u32) -> Self {
        Self {
            started: std::time::Instant::now(),
            vid_pts_us: 0,
            vid_step_us: 1_000_000 / (fps.max(1) as u64),
            last_vid_pts_us: None,
            last_cam_us: None,
            aud_pts_us: 0,
            aud_started: false,
            aud_lead_us: std::env::var("NEOLINK_AUDIO_LEAD_MS")
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(0)
                .saturating_mul(1000),
            vid_snaps: 0,
            aud_snaps: 0,
        }
    }

    fn wall_us(&self) -> i64 {
        self.started.elapsed().as_micros() as i64
    }

    /// A new camera stream may restart its frame timestamps, so the next
    /// frame is spaced by the nominal step rather than by a bogus delta.
    fn camera_restarted(&mut self) {
        self.last_cam_us = None;
    }

    /// Nominal frame rate, used where the camera gives no frame spacing.
    fn set_fps(&mut self, fps: u32) {
        self.vid_step_us = 1_000_000 / (fps.max(1) as u64);
    }

    /// PTS for the next video frame, in microseconds. `cam_us` is the
    /// camera's timestamp for this frame.
    fn next_video(&mut self, cam_us: u32, name: &str) -> u64 {
        let wall_us = self.wall_us();
        self.next_video_at(wall_us, cam_us, name)
    }

    /// `next_video` with the wall clock passed in, so tests can drive it.
    ///
    /// PTS only ever moves forward. A backward step makes downstream
    /// elements and players drop or reorder frames.
    fn next_video_at(&mut self, wall_us: i64, cam_us: u32, name: &str) -> u64 {
        let pts = self.vid_pts_us;
        let err = pts as i64 - wall_us;

        // Space frames by the camera's own timestamps, so PTS follows the
        // rate the camera actually delivers rather than the configured one.
        // A camera set to 15 fps that sends 12 would otherwise fall behind
        // the wall clock faster than the slew can follow, and snap every
        // few seconds. Clamped so one bad timestamp cannot run away.
        let nominal = self.vid_step_us as i64;
        let step = match self.last_cam_us.replace(cam_us) {
            Some(last) => (cam_us.wrapping_sub(last) as i64).clamp(nominal / 4, nominal * 4),
            None => nominal,
        };

        if err < -Self::VID_SNAP_US {
            // Too far behind to slew: the stream stalled, the camera slept,
            // or we are at startup. Jump rather than compressing real elapsed
            // time into a slow correction.
            self.vid_snaps += 1;
            log::debug!("{name}: video clock snap, was {}ms out", err / 1000);
            let wall = wall_us.max(0) as u64;
            self.vid_pts_us = wall.saturating_add(step as u64);
            self.last_vid_pts_us = Some(wall);
            return wall;
        }

        // Behind: capped at 1% of a frame interval so frame spacing stays
        // even. Ahead, typically after the burst that follows a stall: up to
        // half an interval, so the lead drains in seconds while every PTS
        // still lands after the one before it.
        let corr = (-err / 64).clamp(-(step / 2), (step / 100).max(1));
        self.vid_pts_us = self.vid_pts_us.saturating_add((step + corr).max(1) as u64);
        self.last_vid_pts_us = Some(pts);
        pts
    }

    /// PTS for the next audio frame, in microseconds. `duration_us` is the
    /// decoded length of this frame.
    ///
    /// Audio carries no camera timestamp - `BcMediaAac` and `BcMediaAdpcm`
    /// hold only `data`, unlike the video frames which have `microseconds`.
    /// The camera conveys audio position by interleaving it with video, so
    /// the video timeline is the anchor, NOT the wall clock. Anchoring to
    /// arrival time instead puts audio permanently one frame duration late
    /// and turns every lost packet into accumulated error.
    ///
    /// Returns None until the first video frame, since there is nothing to
    /// anchor to yet. Like video, audio PTS only ever moves forward.
    fn next_audio(&mut self, duration_us: u64, name: &str) -> Option<u64> {
        let last_vid_pts_us = self.last_vid_pts_us?;
        // The frame that has just arrived covers roughly
        // [anchor - duration, anchor] on the video timeline, because the
        // encoder could not emit it until the last of its samples existed.
        let anchor = (last_vid_pts_us as i64)
            .saturating_sub(duration_us as i64)
            .saturating_sub(self.aud_lead_us)
            .max(0);

        if !self.aud_started {
            self.aud_started = true;
            self.aud_pts_us = anchor as u64;
        }

        let err = self.aud_pts_us as i64 - anchor;

        if err < -Self::AUD_RESYNC_US {
            // Audio frames went missing, so there is a real gap here. Jumping
            // to the anchor renders it as the short silence it actually is.
            // Slewing instead would compress the surviving audio and hold the
            // desync for minutes.
            self.aud_snaps += 1;
            log::debug!(
                "{name}: audio resync, was {}ms off the video clock",
                err / 1000
            );
            self.aud_pts_us = anchor as u64;
            let pts = self.aud_pts_us;
            self.aud_pts_us = self.aud_pts_us.saturating_add(duration_us);
            return Some(pts);
        }

        // Otherwise stay contiguous and slew out the residual. Audio ahead of
        // the anchor is normal when frames arrive a few at a time, so it is
        // never jumped back. The cap of 1/8 of a frame keeps every PTS after
        // the one before it.
        let max_corr = (duration_us / 8).max(1) as i64;
        let corr = (-err / 64).clamp(-max_corr, max_corr);
        let pts = self.aud_pts_us;
        self.aud_pts_us = self
            .aud_pts_us
            .saturating_add((duration_us as i64 + corr).max(1) as u64);
        Some(pts)
    }
}

/// Frames discarded by the appsrc queue cap, across all streams.
/// Diagnostic only.
static DROPPED_FRAMES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tracks how far the synthesised presentation timestamps have drifted from
/// real elapsed time. Purely diagnostic - it reads state, never changes it.
struct TsStats {
    name: String,
    started: std::time::Instant,
    last_report: std::time::Instant,
    vid_frames: u64,
    aud_frames: u64,
    last_aud_pts_us: u64,
    aud_covered_us: u64,
    drops_at_last_report: u64,
}

impl TsStats {
    fn new(name: String) -> Self {
        let now = std::time::Instant::now();
        Self {
            name,
            started: now,
            last_report: now,
            vid_frames: 0,
            aud_frames: 0,
            last_aud_pts_us: 0,
            aud_covered_us: 0,
            drops_at_last_report: DROPPED_FRAMES.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn note_audio(&mut self, pts_us: u64, duration_us: u64) {
        self.aud_frames += 1;
        self.last_aud_pts_us = pts_us;
        self.aud_covered_us = self.aud_covered_us.saturating_add(duration_us);
    }

    fn note_video(&mut self, cam_us: u32, pts_us: u64, nominal_fps: u32, clock: &MediaClock) {
        self.vid_frames += 1;
        if self.last_report.elapsed() < std::time::Duration::from_secs(60) {
            return;
        }
        self.last_report = std::time::Instant::now();

        let wall_us = self.started.elapsed().as_micros() as i128;
        let drift_ms = (pts_us as i128 - wall_us) / 1000;
        let secs = self.started.elapsed().as_secs_f64();
        let real_fps = if secs > 0.0 {
            self.vid_frames as f64 / secs
        } else {
            0.0
        };
        let drops = DROPPED_FRAMES.load(std::sync::atomic::Ordering::Relaxed);
        let new_drops = drops.saturating_sub(self.drops_at_last_report);
        self.drops_at_last_report = drops;

        // Skew between the two channels at this instant. Negative means the
        // audio timeline sits behind the video timeline, i.e. audio plays
        // early. This is the number to watch for A/V sync.
        let av_skew_ms = (self.last_aud_pts_us as i128 - pts_us as i128) / 1000;
        // Fraction of elapsed time actually covered by received audio.
        // Below 100% means audio frames are being lost in transit, which is a
        // network problem and not a timestamping one.
        let aud_coverage = if wall_us > 0 {
            (self.aud_covered_us as f64 / wall_us as f64) * 100.0
        } else {
            0.0
        };

        log::info!(
            "{}: uptime {:.0}s | pts {:.1}s vs wall {:.1}s | drift {:+}ms | a/v skew {:+}ms | fps real {:.2} nominal {} | aud frames {} cover {:.1}% | camera ts {:.1}s | dropped {} (+{}) | snaps v{} a{}",
            self.name,
            secs,
            pts_us as f64 / 1e6,
            wall_us as f64 / 1e6,
            drift_ms,
            av_skew_ms,
            real_fps,
            nominal_fps,
            self.aud_frames,
            aud_coverage,
            cam_us as f64 / 1e6,
            drops,
            new_drops,
            clock.vid_snaps,
            clock.aud_snaps,
        );
    }
}

fn send_to_sources(
    data: BcMedia,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    clock: &mut MediaClock,
    stream_config: &StreamConfig,
    stats: &mut TsStats,
    need_keyframe: &mut bool,
) -> AnyResult<()> {
    let keyframe = matches!(data, BcMedia::Iframe(_));
    // Update TS
    match data {
        BcMedia::Aac(aac) => {
            let duration = aac.duration().expect("Could not calculate AAC duration");
            if let Some(pts) = clock.next_audio(duration as u64, &stats.name) {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::debug!("Sending AAC: {:?}", Duration::from_micros(pts));
                    send_to_appsrc(aud_src, aac.data, Duration::from_micros(pts))?;
                }
                stats.note_audio(pts, duration as u64);
            }
        }
        BcMedia::Adpcm(adpcm) => {
            let duration = adpcm
                .duration()
                .expect("Could not calculate ADPCM duration");
            if let Some(pts) = clock.next_audio(duration as u64, &stats.name) {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::trace!("Sending ADPCM: {:?}", Duration::from_micros(pts));
                    send_to_appsrc(aud_src, adpcm.data, Duration::from_micros(pts))?;
                }
                stats.note_audio(pts, duration as u64);
            }
        }
        BcMedia::Iframe(BcMediaIframe {
            data, microseconds, ..
        })
        | BcMedia::Pframe(BcMediaPframe {
            data, microseconds, ..
        }) => {
            let pts = clock.next_video(microseconds, &stats.name);
            if let Some(vid_src) = vid_src.as_ref() {
                // A P-frame after a missing frame decodes against the wrong
                // reference and smears until the next keyframe, so wait for
                // that keyframe instead.
                if keyframe || !*need_keyframe {
                    log::trace!("Sending VID: {:?}", Duration::from_micros(pts));
                    *need_keyframe = !send_to_appsrc(vid_src, data, Duration::from_micros(pts))?;
                }
            }
            stats.note_video(microseconds, pts, stream_config.fps, clock);
        }
        _ => {}
    }
    Ok(())
}

/// How much slack we allow on top of the appsrc's configured `max-bytes`
/// before we start dropping frames.
///
/// `appsrc` is configured with `block = false`, which means `push_buffer`
/// will keep appending to its internal queue even once `max-bytes` is
/// exceeded (it only emits `enough-data`, which nothing here listens for).
/// Without an explicit cap a client that stops consuming makes that queue
/// grow without limit.
const APPSRC_QUEUE_SLACK: u64 = 4;

/// Returns whether the frame was pushed; false when it was dropped.
fn send_to_appsrc(appsrc: &AppSrc, data: Vec<u8>, mut ts: Duration) -> AnyResult<bool> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(false);
            }
        } else {
            // Clock not up yet
            return Ok(false);
        }
    }
    // Don't let the appsrc queue grow without bound if the client stalls.
    // This is a live camera feed: dropping is correct, growing is not.
    let max_bytes = appsrc.max_bytes();
    if max_bytes > 0 && appsrc.current_level_bytes() > max_bytes.saturating_mul(APPSRC_QUEUE_SLACK)
    {
        DROPPED_FRAMES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!(
            "{}: appsrc queue at {} bytes (max-bytes {}), dropping frame",
            appsrc.name(),
            appsrc.current_level_bytes(),
            max_bytes,
        );
        return Ok(false);
    }

    // Hand the frame we already own straight to gstreamer.
    //
    // The previous implementation kept a `HashMap<usize, BufferPool>` keyed on
    // the exact byte length of each frame. Compressed frame sizes are
    // effectively unique, so that map gained a new pool for nearly every
    // frame, and each new pool immediately preallocated `min_buffers` buffers
    // of that size. Nothing ever evicted a pool, so the process grew for as
    // long as it streamed. Wrapping the `Vec` we were handed has no such
    // retention, and skips a memcpy as a bonus.
    let buf = {
        let mut new_buf = gstreamer::Buffer::from_mut_slice(data);
        {
            let gst_buf_mut = new_buf
                .get_mut()
                .ok_or_else(|| anyhow!("Freshly created buffer was not uniquely owned"))?;
            let time = ClockTime::from_useconds(ts.as_micros() as u64);
            gst_buf_mut.set_dts(time);
            gst_buf_mut.set_pts(time);
        }
        new_buf
    };

    // Push buffer into the appsrc
    let pushed = match appsrc.push_buffer(buf) {
        Ok(_) => {
            // log::info!(
            //     "Send {}{} on {}",
            //     data.data.len(),
            //     if data.keyframe { " (keyframe)" } else { "" },
            //     appsrc.name()
            // );
            Ok(true)
        }
        Err(FlowError::Flushing) => {
            // NOT "buffer full" - appsrc returns Flushing when the element is
            // in NULL/READY or a flush is in progress, i.e. the pipeline is
            // not running. Log the states so this is diagnosable.
            log::debug!(
                "Flushing on {}: element state {:?}, parent state {:?}. Frame discarded.",
                appsrc.name(),
                appsrc.current_state(),
                appsrc
                    .parent()
                    .and_then(|p| p.downcast::<Element>().ok())
                    .map(|p| p.current_state()),
            );
            Ok(false)
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }?;
    // NOTE: previously this toggled the appsrc element between PAUSED and
    // PLAYING depending on its fill level. With a client consuming in real
    // time the level sits near empty, so the appsrc spent most of its time
    // PAUSED and released frames in bursts, which shows up as stutter-then-
    // catch-up in viewers. Smoothing is the downstream `queue` element's job;
    // it is bounded, so it cannot grow without limit the way this appsrc can.
    Ok(pushed)
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    // Repeat SPS and PPS with every IDR frame, so a client that joins or
    // loses packets mid-stream can start decoding at the next keyframe.
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    // Repeat VPS, SPS and PPS with every IDR frame, as for H264.
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback. The silence is pinned to the camera's rate and channel
    // count. Unpinned, audiotestsrc picks its own rate, 44100 Hz by default,
    // and the encoder would take it, so the RTP clock rate would differ from
    // the one in the SDP. Without the camera's format there is no fallback.
    let fallback = match (
        make_element("fallbackswitch", "audfallbackswitch"),
        stream_config.aac_format,
    ) {
        (Ok(fallback_switch), Some(format)) => {
            fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
            fallback_switch.set_property("immediate-fallback", true);
            let silence = make_element("audiotestsrc", "audsilence")?;
            silence.set_property_from_str("wave", "silence");
            let silence_caps = make_element("capsfilter", "audsilencecaps")?;
            silence_caps.set_property(
                "caps",
                Caps::builder("audio/x-raw")
                    .field("rate", format.rate)
                    .field("channels", format.channels)
                    .build(),
            );
            Some((fallback_switch, silence, silence_caps))
        }
        (Ok(_), None) => {
            log::warn!("The camera's AAC format is unknown, so the audio has no silence fallback");
            None
        }
        (Err(_), _) => None,
    };

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Some((fallback_switch, silence, silence_caps)) = fallback.as_ref() {
        bin.add_many([silence, silence_caps, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([silence, silence_caps, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // then the tail for the configured codec, see `audio_tail`

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);
    // adpcmdec is a GstAudioDecoder, which fails on a BYTES segment, the
    // appsrc default. The buffers carry presentation times, so the segment
    // is in time.
    source.set_format(gstreamer::Format::Time);

    // The camera sends one DVI block per frame: the 4 byte predictor header
    // then `block_size` bytes of samples. GStreamer's block_align counts the
    // header too.
    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "dvi")
            .field("block_align", (block_size + 4) as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

/// AAC encoders in order of preference. `voaacenc` comes from
/// gst-plugins-bad. `avenc_aac` needs gst-libav and FFmpeg, so it is only the
/// fallback.
pub(super) const AAC_ENCODERS: [&str; 2] = ["voaacenc", "avenc_aac"];

/// Bit rate of the AAC track. It fits AAC-LC mono at every rate the camera
/// sends, 8000 Hz included.
const AAC_BITRATE: &str = "32000";

/// RTP payload type of the AAC track. Dynamic, and apart from the 96 that the
/// video payloaders use.
const AAC_PAYLOAD_TYPE: u32 = 97;

/// The first AAC encoder in `AAC_ENCODERS` that `installed` reports.
pub(super) fn pick_aac_encoder(installed: impl Fn(&str) -> bool) -> Option<&'static str> {
    AAC_ENCODERS.iter().copied().find(|kind| installed(kind))
}

/// The AAC encoder this host's GStreamer registry offers, if any.
pub(super) fn installed_aac_encoder() -> Option<&'static str> {
    pick_aac_encoder(|kind| ElementFactory::find(kind).is_some())
}

/// Builds the audio branch of a client pipeline: the camera's AAC or ADPCM is
/// decoded, then encoded in `codec` and payloaded as `pay1`. Returns `None`,
/// with nothing added to the bin, when there is no audio track to serve.
pub(super) fn build_audio(
    bin: &Element,
    aud_type: Option<&AudioType>,
    codec: AudioCodec,
    stream_config: &StreamConfig,
) -> Result<Option<AppSrc>> {
    build_audio_with(bin, aud_type, codec, stream_config, installed_aac_encoder())
}

/// Builds the audio branch in the codec that `camera` names, for the audio
/// that `stream_config` learned from the camera.
pub(super) fn build_audio_for(
    bin: &Element,
    camera: &CameraConfig,
    stream_config: &StreamConfig,
) -> Result<Option<AppSrc>> {
    build_audio(
        bin,
        stream_config.aud_type.as_ref(),
        camera.audio_codec,
        stream_config,
    )
}

/// `build_audio` with the AAC encoder given, so a test can take it away.
pub(super) fn build_audio_with(
    bin: &Element,
    aud_type: Option<&AudioType>,
    codec: AudioCodec,
    stream_config: &StreamConfig,
    aac_encoder: Option<&'static str>,
) -> Result<Option<AppSrc>> {
    let Some(aud_type) = aud_type else {
        return Ok(None);
    };
    let aac_encoder = match (codec, aac_encoder) {
        (AudioCodec::None, _) => return Ok(None),
        (AudioCodec::Aac, None) => {
            // Serving L16 instead would break clients that accept only AAC,
            // so the stream goes out with video alone and says why.
            log::error!(
                "No AAC encoder: install voaacenc (gst-plugins-bad) or avenc_aac (gst-libav). Serving this stream without audio"
            );
            return Ok(None);
        }
        (_, encoder) => encoder,
    };
    let linked = match aud_type {
        AudioType::Aac => pipe_aac(bin, stream_config)?,
        AudioType::Adpcm(block_size) => pipe_adpcm(bin, *block_size, stream_config)?,
    };
    audio_tail(bin, &linked.output, codec, aac_encoder)?;
    Ok(Some(linked.appsrc))
}

/// Encodes and payloads decoded audio from `input` in `codec`.
fn audio_tail(
    bin: &Element,
    input: &Element,
    codec: AudioCodec,
    aac_encoder: Option<&'static str>,
) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let chain = match codec {
        AudioCodec::Aac => {
            let kind = aac_encoder.ok_or_else(|| anyhow!("No AAC encoder installed"))?;
            let resample = make_element("audioresample", "audresample")?;
            let mono = make_element("capsfilter", "audcaps")?;
            mono.set_property(
                "caps",
                Caps::builder("audio/x-raw").field("channels", 1i32).build(),
            );
            let encoder = make_element(kind, "audaacenc")?;
            encoder.set_property_from_str("bitrate", AAC_BITRATE);
            let parser = make_element("aacparse", "audaacparse")?;
            let payload = make_element("rtpmp4gpay", "pay1")?;
            payload.set_property("pt", AAC_PAYLOAD_TYPE);
            vec![resample, mono, encoder, parser, payload]
        }
        AudioCodec::Pcmu => {
            // rtppcmupay accepts only 8000 Hz mono, so caps negotiation makes
            // audioresample and the audioconvert before it produce that.
            let resample = make_element("audioresample", "audresample")?;
            let encoder = make_element("mulawenc", "audmulawenc")?;
            let payload = make_element("rtppcmupay", "pay1")?;
            vec![resample, encoder, payload]
        }
        AudioCodec::L16 => vec![make_element("rtpL16pay", "pay1")?],
        AudioCodec::None => return Ok(()),
    };

    bin.add_many(&chain)?;
    Element::link_many(std::iter::once(input).chain(chain.iter()))?;
    Ok(())
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "rtpmp4gpay" => "rtp (gst-plugins-good)",
            "rtppcmupay" => "rtp (gst-plugins-good)",
            "mulawenc" => "mulaw (gst-plugins-good)",
            "voaacenc" => "voaacenc (gst-plugins-bad)",
            "avenc_aac" => "libav (gst-libav)",
            "faad" => "faad (gst-plugins-bad)",
            "audioresample" => "audioresample (gst-plugins-base)",
            "fallbackswitch" => "fallbackswitch (gst-plugins-rs)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 0.1 seconds (according to bitrate) or 4kb what ever is larger
    std::cmp::max(bitrate * 2 / 8u32, 4u32 * 1024u32)
}

#[cfg(test)]
mod watch_tests {
    use super::{PipelineWatch, PIPELINE_LIVE_DEADLINE};
    use std::time::{Duration, Instant};

    #[test]
    fn waits_for_the_pipeline_to_go_live() {
        let t0 = Instant::now();
        let mut watch = PipelineWatch::new(t0);
        assert!(watch.keep_going(false, t0 + Duration::from_secs(1)));
        assert!(watch.keep_going(true, t0 + Duration::from_secs(2)));
        // Once live, the deadline no longer applies.
        assert!(watch.keep_going(true, t0 + PIPELINE_LIVE_DEADLINE * 10));
    }

    #[test]
    fn stops_when_a_live_pipeline_goes() {
        let t0 = Instant::now();
        let mut watch = PipelineWatch::new(t0);
        assert!(watch.keep_going(true, t0));
        assert!(!watch.keep_going(false, t0 + Duration::from_millis(100)));
    }

    #[test]
    fn stops_when_the_pipeline_never_goes_live() {
        let t0 = Instant::now();
        let mut watch = PipelineWatch::new(t0);
        let just_before = t0 + PIPELINE_LIVE_DEADLINE - Duration::from_millis(1);
        assert!(watch.keep_going(false, just_before));
        assert!(!watch.keep_going(false, t0 + PIPELINE_LIVE_DEADLINE));
    }
}

/// The frame pusher resubscribes from a plain std thread, which has no tokio
/// runtime context. These tests call `resubscribe` from such a thread, with
/// no camera and no network, the way the pusher does after a camera drop.
#[cfg(test)]
mod resubscribe_tests {
    use super::{resubscribe, RESUBSCRIBE_BACKOFF_MAX};
    use crate::common::{NeoCamCommand, NeoInstance};
    use anyhow::anyhow;
    use neolink_core::bc_protocol::{BcCamera, StreamKind};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    };
    use std::time::{Duration, Instant};
    use tokio_util::sync::CancellationToken;

    const NO_BACKOFF: Duration = Duration::from_millis(1);

    /// Runs `f` on a new std thread, as the pusher runs, and returns its
    /// result or the message it panicked with.
    fn on_plain_thread<T: Send + 'static>(
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, String> {
        std::thread::spawn(f).join().map_err(|panic| {
            panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "a panic with no message".to_string())
        })
    }

    #[test]
    fn resubscribe_on_a_plain_thread_returns_the_new_stream() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runtime = rt.handle().clone();
        let got = on_plain_thread(move || {
            let attempts = AtomicUsize::new(0);
            let mut backoff = NO_BACKOFF;
            resubscribe(
                &runtime,
                &mut backoff,
                Duration::from_secs(5),
                || true,
                || {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        // A real resubscribe spawns its stream tasks, which
                        // needs the runtime context too.
                        tokio::spawn(async {}).await?;
                        if n == 0 {
                            Err(anyhow!("camera still asleep"))
                        } else {
                            crate::AnyResult::Ok(n)
                        }
                    }
                },
                "test::mainStream",
            )
        });
        assert_eq!(got, Ok(Some(1)), "the second attempt's stream comes back");
    }

    #[test]
    fn resubscribe_on_a_plain_thread_times_out_a_hung_attempt() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runtime = rt.handle().clone();
        let limit = Duration::from_millis(100);
        let got = on_plain_thread(move || {
            let attempts = AtomicUsize::new(0);
            let mut backoff = NO_BACKOFF;
            let started = Instant::now();
            let got = resubscribe(
                &runtime,
                &mut backoff,
                limit,
                || true,
                || {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if n == 0 {
                            std::future::pending::<()>().await;
                        }
                        crate::AnyResult::Ok(n)
                    }
                },
                "test::mainStream",
            );
            (got, started.elapsed())
        });
        let (got, took) = got.expect("resubscribe ran without a panic");
        assert_eq!(got, Some(1), "the hung attempt gave way to the next one");
        assert!(took >= limit, "the hung attempt was held for its limit");
        assert!(took < Duration::from_secs(5), "took {:?}", took);
    }

    #[test]
    fn resubscribe_stops_once_the_client_has_gone() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runtime = rt.handle().clone();
        let got = on_plain_thread(move || {
            let attempts = AtomicUsize::new(0);
            let mut backoff = NO_BACKOFF;
            let got: Option<()> = resubscribe(
                &runtime,
                &mut backoff,
                Duration::from_secs(5),
                || false,
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { crate::AnyResult::Ok(()) }
                },
                "test::mainStream",
            );
            (got, attempts.load(Ordering::SeqCst))
        });
        assert_eq!(got, Ok((None, 0)), "no attempt once nobody watches");
    }

    /// The same call the pusher makes, `NeoInstance::stream_while_live`, on
    /// an instance whose camera is gone, so every attempt fails at once. The
    /// backoff before each check is what keeps the pusher from spinning while
    /// the camera sleeps, so the test also checks that it slept and doubled.
    #[test]
    fn resubscribe_to_a_gone_camera_retries_until_the_client_leaves() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runtime = rt.handle().clone();
        let (_camera_tx, camera_watch) = tokio::sync::watch::channel(Weak::<BcCamera>::new());
        let (control_tx, control_rx) = tokio::sync::mpsc::channel::<NeoCamCommand>(1);
        drop(control_rx);
        let camera = NeoInstance::new(camera_watch, control_tx, CancellationToken::new()).unwrap();
        let start = Duration::from_millis(10);
        let got = on_plain_thread(move || {
            let checks = AtomicUsize::new(0);
            let mut backoff = start;
            let started = Instant::now();
            let got = resubscribe(
                &runtime,
                &mut backoff,
                Duration::from_secs(5),
                || checks.fetch_add(1, Ordering::SeqCst) < 3,
                || camera.stream_while_live(StreamKind::Main),
                "test::mainStream",
            );
            (
                got.is_some(),
                checks.load(Ordering::SeqCst),
                backoff,
                started.elapsed(),
            )
        });
        let (got, checks, backoff, took) = got.expect("resubscribe ran without a panic");
        assert_eq!(
            (got, checks),
            (false, 4),
            "three failed attempts, then the client was gone"
        );
        // One sleep before each of the 4 checks: 10, 20, 40 and 80 ms.
        assert_eq!(backoff, start * 16, "the backoff doubled once per check");
        assert!(
            took >= start * 15,
            "slept before each check, took {:?}",
            took
        );
    }

    /// The backoff stops doubling at `RESUBSCRIBE_BACKOFF_MAX`, so a camera
    /// that sleeps for hours is still tried every 10 seconds.
    #[test]
    fn resubscribe_backoff_stops_at_its_max() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let runtime = rt.handle().clone();
        let got = on_plain_thread(move || {
            let mut backoff = RESUBSCRIBE_BACKOFF_MAX;
            let started = Instant::now();
            let got: Option<()> = resubscribe(
                &runtime,
                &mut backoff,
                Duration::from_secs(5),
                || false,
                || async { crate::AnyResult::Ok(()) },
                "test::mainStream",
            );
            (got, backoff, started.elapsed())
        });
        let (got, backoff, took) = got.expect("resubscribe ran without a panic");
        assert_eq!(got, None, "nobody watches, so no stream");
        assert_eq!(backoff, RESUBSCRIBE_BACKOFF_MAX, "the backoff kept its max");
        assert!(
            took >= RESUBSCRIBE_BACKOFF_MAX,
            "slept the max, took {:?}",
            took
        );
    }
}

#[cfg(test)]
mod tests {
    use super::MediaClock;

    const AAC_US: u64 = 64_000;

    enum Ev {
        /// A video frame with its camera timestamp.
        Video(u32),
        Audio,
    }

    #[derive(Default)]
    struct Run {
        vid_back: u32,
        aud_back: u32,
        vid_snaps: u64,
        aud_snaps: u64,
    }

    /// Feeds `(wall_us, event)` pairs, in arrival order, through a clock
    /// that nominally runs at 15 fps and counts PTS that fail to increase.
    fn run(mut events: Vec<(i64, Ev)>) -> Run {
        events.sort_by_key(|(wall, _)| *wall);
        let mut clock = MediaClock::new(15);
        clock.aud_lead_us = 0;
        let mut out = Run::default();
        let (mut last_vid, mut last_aud) = (None, None);
        for (wall, ev) in events {
            match ev {
                Ev::Video(cam_us) => {
                    let pts = clock.next_video_at(wall, cam_us, "test");
                    if last_vid.is_some_and(|last| pts <= last) {
                        out.vid_back += 1;
                    }
                    last_vid = Some(pts);
                }
                Ev::Audio => {
                    if let Some(pts) = clock.next_audio(AAC_US, "test") {
                        if last_aud.is_some_and(|last| pts <= last) {
                            out.aud_back += 1;
                        }
                        last_aud = Some(pts);
                    }
                }
            }
        }
        out.vid_snaps = clock.vid_snaps;
        out.aud_snaps = clock.aud_snaps;
        out
    }

    /// Video frames every `interval_us` over `[from_us, to_us)`, each
    /// arriving as soon as the camera stamps it.
    fn video(from_us: i64, to_us: i64, interval_us: i64) -> Vec<(i64, Ev)> {
        (0..)
            .map(|i| from_us + i * interval_us)
            .take_while(|t| *t < to_us)
            .map(|t| (t, Ev::Video(t as u32)))
            .collect()
    }

    /// Audio frames back to back over `(0, to_us)`, each arriving as its
    /// last sample is captured.
    fn audio(to_us: i64) -> Vec<(i64, Ev)> {
        (1..to_us / AAC_US as i64)
            .map(|i| (i * AAC_US as i64, Ev::Audio))
            .collect()
    }

    #[test]
    fn stall_then_burst_never_steps_back() {
        let step = 1_000_000 / 15;
        // 10 s steady, a 2 s stall, the 30 held frames in 100 ms, then 60 s
        // steady. Audio stalls with video and arrives in the same burst.
        let mut events = video(0, 10_000_000, step);
        // The camera kept stamping frames through the stall.
        events.extend((0..30).map(|i| {
            let cam_us = 10_000_000 + i * step;
            (12_000_000 + i * 100_000 / 30, Ev::Video(cam_us as u32))
        }));
        events.extend(video(12_100_000, 72_100_000, step));
        for i in 0..(72_000_000 / AAC_US as i64) {
            let t = (i + 1) * AAC_US as i64;
            let t = if (10_000_000..12_000_000).contains(&t) {
                12_000_000 + (t - 10_000_000) / 20
            } else {
                t
            };
            events.push((t, Ev::Audio));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.vid_snaps <= 2, "{} video snaps", r.vid_snaps);
    }

    #[test]
    fn bursty_audio_stays_locked() {
        // Audio arrives three 64 ms frames at a time every 192 ms.
        let mut events = video(0, 60_000_000, 1_000_000 / 15);
        for burst in 1..(60_000_000 / 192_000) {
            events.extend((0..3).map(|_| (burst * 192_000, Ev::Audio)));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.aud_snaps <= 5, "{} audio resyncs in 60 s", r.aud_snaps);
    }

    #[test]
    fn jittery_audio_stays_locked() {
        // Audio arrival jitters by up to +/-30 ms around its nominal time.
        let mut events = video(0, 60_000_000, 1_000_000 / 15);
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        for i in 1..(60_000_000 / AAC_US as i64) {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let jitter = ((seed >> 33) % 60_001) as i64 - 30_000;
            events.push((i * AAC_US as i64 + jitter, Ev::Audio));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.aud_snaps <= 5, "{} audio resyncs in 60 s", r.aud_snaps);
    }

    #[test]
    fn slow_camera_follows_its_own_timestamps() {
        // The camera is configured for 15 fps but delivers fewer.
        for real_fps in [8, 10, 12, 14] {
            let mut events = video(0, 60_000_000, 1_000_000 / real_fps);
            events.extend(audio(60_000_000));
            let r = run(events);
            assert_eq!(r.vid_back, 0, "{real_fps} fps");
            assert_eq!(r.aud_back, 0, "{real_fps} fps");
            assert_eq!(r.vid_snaps, 0, "{real_fps} fps");
            assert!(r.aud_snaps <= 2, "{real_fps} fps: {} resyncs", r.aud_snaps);
        }
    }

    const STEP_US: i64 = 1_000_000 / 15;

    /// Feeds video frames, as `(wall_us, cam_us)` in arrival order, through
    /// `clock` and returns each PTS with its wall time.
    fn video_pts(clock: &mut MediaClock, frames: &[(i64, u32)]) -> Vec<(i64, u64)> {
        frames
            .iter()
            .map(|&(wall, cam_us)| (wall, clock.next_video_at(wall, cam_us, "test")))
            .collect()
    }

    fn assert_increasing(pts: &[(i64, u64)]) {
        for pair in pts.windows(2) {
            assert!(pair[1].1 > pair[0].1, "PTS stepped back: {:?}", pair);
        }
    }

    /// Largest lead of PTS over wall time, in microseconds.
    fn max_lead_us(pts: &[(i64, u64)]) -> i64 {
        pts.iter()
            .map(|&(wall, pts)| pts as i64 - wall)
            .max()
            .unwrap_or(0)
    }

    /// 15 fps frames `i` in `range`, arriving on time, stamped by `cam`.
    fn steady(range: std::ops::Range<i64>, cam: impl Fn(i64) -> u32) -> Vec<(i64, u32)> {
        range.map(|i| (i * STEP_US, cam(i))).collect()
    }

    #[test]
    fn camera_timestamp_wrap_keeps_spacing() {
        // The camera's u32 microsecond clock wraps every 71.6 minutes. Start
        // 2 s before the wrap and run 60 s across it.
        let start = u32::MAX - 2_000_000;
        let frames = steady(0..900, |i| start.wrapping_add((i * STEP_US) as u32));
        let mut clock = MediaClock::new(15);
        let pts = video_pts(&mut clock, &frames);
        assert_increasing(&pts);
        assert_eq!(clock.vid_snaps, 0);
        for pair in pts.windows(2) {
            let delta = (pair[1].1 - pair[0].1) as i64;
            assert!(
                (delta - STEP_US).abs() <= STEP_US / 50,
                "spacing {} us around {:?}",
                delta,
                pair
            );
        }
    }

    #[test]
    fn camera_timestamp_reset_without_notice_is_bounded() {
        // 30 s stamped from 50 s, then the camera restarts its clock at 0 on
        // the same stream. The backward jump reads as a huge forward step,
        // which the clamp limits to four frame intervals.
        let frames = steady(0..1800, |i| {
            if i < 900 {
                (50_000_000 + i * STEP_US) as u32
            } else {
                ((i - 900) * STEP_US) as u32
            }
        });
        let mut clock = MediaClock::new(15);
        let pts = video_pts(&mut clock, &frames);
        assert_increasing(&pts);
        assert_eq!(clock.vid_snaps, 0);
        assert!(
            max_lead_us(&pts) <= 4 * STEP_US,
            "lead {} us",
            max_lead_us(&pts)
        );
        // The lead drains: 10 s later PTS is back within a frame of wall time.
        let (wall, last) = *pts.last().unwrap();
        assert!((last as i64 - wall).abs() < STEP_US);
    }

    #[test]
    fn camera_restarted_spaces_the_next_frame_nominally() {
        let mut clock = MediaClock::new(15);
        let before = video_pts(
            &mut clock,
            &steady(0..150, |i| (80_000_000 + i * STEP_US) as u32),
        );
        clock.camera_restarted();
        let after = video_pts(
            &mut clock,
            &steady(150..300, |i| ((i - 150) * STEP_US) as u32),
        );
        let joined: Vec<_> = before.iter().chain(after.iter()).copied().collect();
        assert_increasing(&joined);
        let first_gap = (after[0].1 - before.last().unwrap().1) as i64;
        assert!(
            (first_gap - STEP_US).abs() <= STEP_US / 50,
            "gap {} us",
            first_gap
        );
        assert!(max_lead_us(&joined) < STEP_US / 2);
        assert_eq!(clock.vid_snaps, 0);
    }

    #[test]
    fn fps_change_follows_the_new_rate() {
        // 30 s at 15 fps, then the camera reports and delivers 25 fps.
        let step25 = 1_000_000 / 25;
        let mut clock = MediaClock::new(15);
        let mut pts = video_pts(&mut clock, &steady(0..450, |i| (i * STEP_US) as u32));
        clock.set_fps(25);
        let switch = 450 * STEP_US;
        let later: Vec<(i64, u32)> = (0..750)
            .map(|i| (switch + i * step25, (switch + i * step25) as u32))
            .collect();
        pts.extend(video_pts(&mut clock, &later));
        assert_increasing(&pts);
        assert_eq!(clock.vid_snaps, 0);
        let tail = &pts[pts.len() - 100..];
        for pair in tail.windows(2) {
            let delta = (pair[1].1 - pair[0].1) as i64;
            assert!(
                (delta - step25).abs() <= step25 / 50,
                "spacing {} us",
                delta
            );
        }
        assert!(max_lead_us(&pts) < step25);
    }

    #[test]
    fn zero_fps_does_not_divide_by_zero() {
        let mut clock = MediaClock::new(0);
        clock.set_fps(0);
        let pts = video_pts(&mut clock, &[(0, 0), (1_000_000, 1_000_000)]);
        assert_increasing(&pts);
    }

    #[test]
    fn pts_lead_over_wall_is_bounded() {
        // Steady camera timestamps, arrival jittered by up to +/-30 ms: PTS
        // never leads the wall clock by more than the jitter plus a frame.
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut frames: Vec<(i64, u32)> = (0..900)
            .map(|i| {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let jitter = ((seed >> 33) % 60_001) as i64 - 30_000;
                ((i * STEP_US + jitter).max(0), (i * STEP_US) as u32)
            })
            .collect();
        frames.sort_by_key(|f| f.0);
        let mut clock = MediaClock::new(15);
        let pts = video_pts(&mut clock, &frames);
        assert_increasing(&pts);
        assert!(
            max_lead_us(&pts) <= 30_000 + STEP_US,
            "lead {} us",
            max_lead_us(&pts)
        );

        // After a 2 s stall the held frames arrive in a 100 ms burst. The
        // lead is bounded by the camera time the burst covers, and drains.
        let mut clock = MediaClock::new(15);
        let mut frames = steady(0..150, |i| (i * STEP_US) as u32);
        frames.extend((0..30).map(|i| {
            (
                12_000_000 + i * 100_000 / 30,
                (10_000_000 + i * STEP_US) as u32,
            )
        }));
        frames.extend((0..450).map(|i| {
            let t = 12_100_000 + i * STEP_US;
            (t, (12_000_000 + i * STEP_US) as u32)
        }));
        let pts = video_pts(&mut clock, &frames);
        assert_increasing(&pts);
        assert!(
            max_lead_us(&pts) <= 30 * STEP_US,
            "lead {} us",
            max_lead_us(&pts)
        );
        let (wall, last) = *pts.last().unwrap();
        assert!(
            (last as i64 - wall).abs() < STEP_US,
            "lead {} us after 30 s",
            last as i64 - wall
        );
    }
}

/// A frame pusher ends in one of two ways besides a clean stop: its body
/// returns an error, or it panics. Each must leave a warn line that names the
/// camera, the stream and the cause, and the pusher's cleanup must run.
#[cfg(test)]
mod pusher_end_tests {
    use super::spawn_pusher;
    use anyhow::anyhow;
    use neolink_core::bc_protocol::StreamKind;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Once,
    };

    /// Keeps every log record of this test binary, so a test can find the
    /// lines its own pusher wrote.
    struct Capture;

    static LINES: Mutex<Vec<(log::Level, String)>> = Mutex::new(Vec::new());
    static CAPTURE: Capture = Capture;

    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            LINES
                .lock()
                .unwrap()
                .push((record.level(), record.args().to_string()));
        }
        fn flush(&self) {}
    }

    fn capture_logs() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            log::set_logger(&CAPTURE).expect("no other logger in this test binary");
            log::set_max_level(log::LevelFilter::Trace);
        });
    }

    fn lines_naming(camera: &str) -> Vec<(log::Level, String)> {
        LINES
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, line)| line.contains(camera))
            .cloned()
            .collect()
    }

    /// Stands in for `PusherExit`: set when the pusher's body is dropped.
    struct Cleanup(Arc<AtomicBool>);

    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Runs `body` as a pusher for `camera` on the sub stream, waits for it,
    /// and returns the name its thread had and whether its cleanup ran.
    fn run_pusher(
        camera: &str,
        body: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
    ) -> (Option<String>, bool) {
        let cleaned = Arc::new(AtomicBool::new(false));
        let thread_name = Arc::new(Mutex::new(None));
        let cleanup = Cleanup(cleaned.clone());
        let seen = thread_name.clone();
        let handle = spawn_pusher(camera, StreamKind::Sub, move || {
            let _cleanup = cleanup;
            *seen.lock().unwrap() = std::thread::current().name().map(str::to_string);
            body()
        })
        .expect("the pusher thread starts");
        handle
            .join()
            .expect("the pusher's end is handled on its own thread");
        let name = thread_name.lock().unwrap().clone();
        (name, cleaned.load(Ordering::SeqCst))
    }

    #[test]
    fn pusher_error_logs_camera_stream_and_cause_at_warn() {
        capture_logs();
        let (thread_name, cleaned) = run_pusher("yard-err", || {
            Err(anyhow!("appsrc refused the buffer")).map_err(|e| e.context("push failed"))
        });
        assert_eq!(thread_name.as_deref(), Some("pusher yard-err::subStream"));
        assert!(cleaned, "the pusher's cleanup did not run");
        let lines = lines_naming("yard-err");
        assert_eq!(
            lines,
            vec![(
                log::Level::Warn,
                "Frame pusher for camera yard-err stream subStream ended on an error: \
                 push failed: appsrc refused the buffer"
                    .to_string()
            )]
        );
    }

    #[test]
    fn pusher_panic_logs_camera_stream_and_cause_at_warn() {
        capture_logs();
        let (thread_name, cleaned) = run_pusher("yard-panic", || {
            let frames: Vec<u8> = vec![];
            panic!("index {} out of range for {} frames", 3, frames.len())
        });
        assert_eq!(thread_name.as_deref(), Some("pusher yard-panic::subStream"));
        assert!(cleaned, "the pusher's cleanup did not run");
        let lines = lines_naming("yard-panic");
        assert_eq!(
            lines,
            vec![(
                log::Level::Warn,
                "Frame pusher for camera yard-panic stream subStream ended on a panic: \
                 index 3 out of range for 0 frames"
                    .to_string()
            )]
        );
    }

    /// A panic on a string literal, as `panic!` with no arguments or
    /// `Option::unwrap` on `None` gives, carries a `&'static str` and not a
    /// `String`.
    #[test]
    fn pusher_literal_panic_logs_its_message_at_warn() {
        capture_logs();
        let (thread_name, cleaned) =
            run_pusher("yard-literal", || panic!("appsrc has no source pad"));
        assert_eq!(
            thread_name.as_deref(),
            Some("pusher yard-literal::subStream")
        );
        assert!(cleaned, "the pusher's cleanup did not run");
        let lines = lines_naming("yard-literal");
        assert_eq!(
            lines,
            vec![(
                log::Level::Warn,
                "Frame pusher for camera yard-literal stream subStream ended on a panic: \
                 appsrc has no source pad"
                    .to_string()
            )]
        );
    }

    /// A thread name may not hold a NUL, and TOML lets a camera name hold
    /// one. The pusher must still start, under the name without the NUL.
    #[test]
    fn pusher_starts_for_a_camera_name_with_a_nul() {
        capture_logs();
        let (thread_name, cleaned) = run_pusher("yard\0nul", || Ok(()));
        assert_eq!(thread_name.as_deref(), Some("pusher yardnul::subStream"));
        assert!(cleaned, "the pusher's cleanup did not run");
    }

    #[test]
    fn pusher_clean_stop_logs_no_warn() {
        capture_logs();
        let (_, cleaned) = run_pusher("yard-ok", || Ok(()));
        assert!(cleaned, "the pusher's cleanup did not run");
        assert!(lines_naming("yard-ok").is_empty());
    }
}
