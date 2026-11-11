use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use mc_proto::transcoder::transcoder_server::Transcoder;
use mc_proto::transcoder::{TranscodeChunk, TranscodeRequest};
use mc_storage::{Key, LocalStorage, Storage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

const CHUNK_SIZE: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
struct FormatSpec {
    extension: &'static str,
    content_type: &'static str,
    codec: &'static str,
    muxer: &'static str,
}

#[derive(Clone)]
pub struct TranscoderService {
    storage: Arc<LocalStorage>,
    cache_dir: PathBuf,
    cache_max_bytes: u64,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl TranscoderService {
    pub fn new(storage: LocalStorage, cache_dir: PathBuf, cache_max_bytes: u64) -> Self {
        Self {
            storage: Arc::new(storage),
            cache_dir,
            cache_max_bytes,
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn prepare(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.cache_dir).await?;
        self.prune_cache().await;
        Ok(())
    }

    async fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn run_transcode(
        &self,
        request: TranscodeRequest,
        tx: &mpsc::Sender<Result<TranscodeChunk, Status>>,
    ) -> Result<(), Status> {
        let spec = format_spec(&request.format)
            .ok_or_else(|| Status::invalid_argument("unsupported transcode format"))?;
        let bitrate = match request.bitrate_kbps {
            0 => 128,
            value => value.clamp(32, 320),
        };
        let key = Key::new(request.source_key.as_str())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let source = self
            .storage
            .local_path(&key)
            .ok_or_else(|| Status::invalid_argument("invalid source key"))?;
        let metadata = tokio::fs::metadata(&source)
            .await
            .map_err(|_| Status::not_found("source file not found"))?;

        if !metadata.is_file() {
            return Err(Status::not_found("source file not found"));
        }

        let cache_key = cache_key(&source, request.source_key.as_str(), spec, bitrate).await?;
        let cache_path = self
            .cache_dir
            .join(format!("{cache_key}.{}", spec.extension));
        let lock = self.key_lock(&cache_key).await;
        let _guard = lock.lock().await;

        if let Ok(metadata) = tokio::fs::metadata(&cache_path).await {
            if metadata.is_file() && metadata.len() > 0 {
                metrics::counter!("mc_transcoder_cache_hits_total").increment(1);
                return stream_file(&cache_path, spec, tx).await;
            }
        }

        self.transcode_to_cache(&source, &cache_path, spec, bitrate, tx)
            .await
    }

    async fn transcode_to_cache(
        &self,
        source: &Path,
        cache_path: &Path,
        spec: FormatSpec,
        bitrate: u32,
        tx: &mpsc::Sender<Result<TranscodeChunk, Status>>,
    ) -> Result<(), Status> {
        let temporary = self.cache_dir.join(format!(
            ".{}.{}.tmp",
            cache_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("transcode"),
            Uuid::now_v7()
        ));

        let mut command = Command::new("ffmpeg");
        command
            .args(["-hide_banner", "-loglevel", "error", "-i"])
            .arg(source)
            .args(["-vn", "-c:a", spec.codec, "-b:a"])
            .arg(format!("{bitrate}k"))
            .args(["-f", spec.muxer, "-"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(|error| Status::internal(format!("start ffmpeg: {error}")))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| Status::internal("ffmpeg stdout is unavailable"))?;
        let mut cache_file = tokio::fs::File::create(&temporary).await.ok();
        let mut cache_ok = cache_file.is_some();
        let mut buffer = vec![0_u8; CHUNK_SIZE];
        let mut client_gone = false;
        let mut sent_any = false;

        loop {
            let read = stdout
                .read(&mut buffer)
                .await
                .map_err(|error| Status::internal(format!("read ffmpeg output: {error}")))?;

            if read == 0 {
                break;
            }

            sent_any = true;

            if let Some(file) = &mut cache_file {
                if let Err(error) = file.write_all(&buffer[..read]).await {
                    warn!(%error, "failed to write transcode cache");
                    cache_ok = false;
                    cache_file = None;
                }
            }

            if tx
                .send(Ok(TranscodeChunk {
                    data: buffer[..read].to_vec(),
                    content_type: spec.content_type.to_owned(),
                }))
                .await
                .is_err()
            {
                client_gone = true;
                break;
            }
        }

        if let Some(mut file) = cache_file {
            let _ = file.flush().await;
        }

        if client_gone {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = tokio::fs::remove_file(&temporary).await;
            return Ok(());
        }

        let status = child
            .wait()
            .await
            .map_err(|error| Status::internal(format!("wait for ffmpeg: {error}")))?;

        if !status.success() || !sent_any {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(Status::internal("ffmpeg failed to transcode the file"));
        }

        if !cache_ok {
            let _ = tokio::fs::remove_file(&temporary).await;
        } else if let Err(error) = tokio::fs::rename(&temporary, cache_path).await {
            warn!(%error, "failed to publish transcode cache file");
            let _ = tokio::fs::remove_file(&temporary).await;
        } else {
            info!(cache = %cache_path.display(), "transcode cached");
            self.prune_cache().await;
        }

        Ok(())
    }

    async fn prune_cache(&self) {
        let Ok(mut directory) = tokio::fs::read_dir(&self.cache_dir).await else {
            return;
        };

        let mut files = Vec::new();
        let mut total = 0_u64;

        while let Ok(Some(entry)) = directory.next_entry().await {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if name.starts_with('.') {
                continue;
            }

            let Ok(metadata) = entry.metadata().await else {
                continue;
            };

            if !metadata.is_file() {
                continue;
            }

            total = total.saturating_add(metadata.len());
            let modified = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |duration| duration.as_millis());

            files.push((modified, path, metadata.len()));
        }

        if total <= self.cache_max_bytes {
            return;
        }

        files.sort_by_key(|(modified, _, _)| *modified);

        for (_, path, size) in files {
            if total <= self.cache_max_bytes {
                break;
            }

            if tokio::fs::remove_file(&path).await.is_ok() {
                total = total.saturating_sub(size);
            }
        }
    }
}

#[tonic::async_trait]
impl Transcoder for TranscoderService {
    type TranscodeStream = ReceiverStream<Result<TranscodeChunk, Status>>;

    async fn transcode(
        &self,
        request: Request<TranscodeRequest>,
    ) -> Result<Response<Self::TranscodeStream>, Status> {
        let (tx, rx) = mpsc::channel(8);
        let service = self.clone();
        let request = request.into_inner();
        metrics::counter!(
            "mc_transcoder_requests_total",
            "format" => request.format.clone()
        )
        .increment(1);

        tokio::spawn(async move {
            if let Err(error) = service.run_transcode(request, &tx).await {
                let _ = tx.send(Err(error)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn stream_file(
    path: &Path,
    spec: FormatSpec,
    tx: &mpsc::Sender<Result<TranscodeChunk, Status>>,
) -> Result<(), Status> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| Status::internal(format!("open transcode cache: {error}")))?;
    let mut buffer = vec![0_u8; CHUNK_SIZE];

    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| Status::internal(format!("read transcode cache: {error}")))?;

        if read == 0 {
            return Ok(());
        }

        if tx
            .send(Ok(TranscodeChunk {
                data: buffer[..read].to_vec(),
                content_type: spec.content_type.to_owned(),
            }))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
}

async fn cache_key(
    source: &Path,
    source_key: &str,
    spec: FormatSpec,
    bitrate: u32,
) -> Result<String, Status> {
    let metadata = tokio::fs::metadata(source)
        .await
        .map_err(|_| Status::not_found("source file not found"))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_millis());
    let input = format!(
        "{source_key}:{}:{modified}:{}:{bitrate}",
        metadata.len(),
        spec.extension
    );

    Ok(blake3::hash(input.as_bytes()).to_hex().to_string())
}

fn format_spec(format: &str) -> Option<FormatSpec> {
    match format.to_ascii_lowercase().as_str() {
        "" | "mp3" => Some(FormatSpec {
            extension: "mp3",
            content_type: "audio/mpeg",
            codec: "libmp3lame",
            muxer: "mp3",
        }),
        "opus" | "ogg" => Some(FormatSpec {
            extension: "opus",
            content_type: "audio/ogg",
            codec: "libopus",
            muxer: "ogg",
        }),
        "aac" | "m4a" => Some(FormatSpec {
            extension: "aac",
            content_type: "audio/aac",
            codec: "aac",
            muxer: "adts",
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::format_spec;

    #[test]
    fn maps_supported_formats() {
        assert_eq!(format_spec("mp3").unwrap().content_type, "audio/mpeg");
        assert_eq!(format_spec("opus").unwrap().muxer, "ogg");
        assert_eq!(format_spec("aac").unwrap().muxer, "adts");
        assert!(format_spec("wav").is_none());
    }
}
