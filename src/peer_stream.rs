use super::protopack;
use super::protopack::TunnelData;
use super::protopack::TunnelPack;
use super::stream_flow::StreamFlow;
use super::stream_flow::StreamFlowErr;
use super::stream_flow::StreamFlowInfo;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct PeerStream {}

impl PeerStream {
    pub async fn start(
        is_spawn: bool,
        stream: StreamFlow,
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
    ) -> anyhow::Result<()> {
        if is_spawn {
            tokio::spawn(async move {
                let ret: anyhow::Result<()> = async {
                    PeerStream::do_start(
                        true,
                        stream,
                        stream_to_peer_stream_rx,
                        peer_stream_to_peer_client_tx,
                    )
                    .await?;
                    Ok(())
                }
                .await;
                ret.unwrap_or_else(|e| log::error!("err:do_start => e:{}", e));
            });
        } else {
            let ret: anyhow::Result<()> = async {
                PeerStream::do_start(
                    false,
                    stream,
                    stream_to_peer_stream_rx,
                    peer_stream_to_peer_client_tx,
                )
                .await?;
                Ok(())
            }
            .await;
            ret.unwrap_or_else(|e| log::error!("err:do_start => e:{}", e));
        }
        Ok(())
    }

    pub async fn do_start(
        is_spawn: bool,
        mut stream: StreamFlow,
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
    ) -> anyhow::Result<()> {
        let stream_info = Arc::new(std::sync::Mutex::new(StreamFlowInfo::new()));
        stream.set_stream_info(Some(stream_info.clone()));
        let (r, w) = tokio::io::split(stream);
        let is_close = Arc::new(AtomicBool::new(false));

        let ret: anyhow::Result<()> = async {
            tokio::select! {
                biased;
                ret = PeerStream::read_stream(r,  is_close.clone(), peer_stream_to_peer_client_tx.clone(), stream_to_peer_stream_rx.clone()) => {
                    ret.map_err(|e| anyhow::anyhow!("err:read_stream => e:{}", e))?;
                    Ok(())
                }
                ret = PeerStream::write_stream(w, is_close.clone(), peer_stream_to_peer_client_tx, stream_to_peer_stream_rx) => {
                    ret.map_err(|e| anyhow::anyhow!("err:write_stream => e:{}", e))?;
                    Ok(())
                }
                ret = PeerStream::read_flow(is_spawn, stream_info.clone()) => {
                    ret.map_err(|e| anyhow::anyhow!("err:read_flow => e:{}", e))?;
                    Ok(())
                }
                else => {
                    return Err(anyhow::anyhow!("err:select"));
                }
            }
        }
        .await;
        if let Err(e) = ret {
            let err = { stream_info.lock().unwrap().err.clone() };
            if err as i32 >= StreamFlowErr::WriteTimeout as i32 {
                return Err(e)?;
            }
        }
        Ok(())
    }

    async fn read_stream<R: AsyncRead + std::marker::Unpin>(
        r: R,
        is_close: Arc<AtomicBool>,
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
    ) -> anyhow::Result<()> {
        let mut buf_read = BufReader::new(r);
        let mut slice = [0u8; protopack::TUNNEL_MAX_HEADER_SIZE];
        loop {
            if is_close.load(Ordering::SeqCst) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let pack = match protopack::read_pack(&mut buf_read, &mut slice)
                .await
                .map_err(|e| anyhow::anyhow!("err:read_pack => e:{}", e))
            {
                Err(e) => {
                    //socket失败直接退出
                    PeerStream::close(peer_stream_to_peer_client_tx, stream_to_peer_stream_rx);
                    return Err(e)?;
                }
                Ok(pack) => pack,
            };

            match pack {
                TunnelPack::TunnelHello(value) => {
                    return Err(anyhow::anyhow!(
                        "err:TunnelHello => TunnelHello:{:?}",
                        value
                    ))?;
                }
                TunnelPack::TunnelData(value) => {
                    log::trace!(
                        "peer_stream_to_peer_client_tx TunnelData:{:?}",
                        value.header
                    );
                    //send失败， 让recv退出， 保证数据能到达tcp缓冲区
                    if let Err(_) = peer_stream_to_peer_client_tx.send(value).await {
                        is_close.swap(true, Ordering::SeqCst);
                        continue;
                    }
                }
            }
        }
    }

    async fn write_stream<W: AsyncWrite + std::marker::Unpin>(
        w: W,
        is_close: Arc<AtomicBool>,
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
    ) -> anyhow::Result<()> {
        let mut buf_writer = BufWriter::new(w);
        loop {
            let tunnel_data = match tokio::time::timeout(
                tokio::time::Duration::from_secs(1),
                stream_to_peer_stream_rx.recv(),
            )
            .await
            {
                Ok(tunnel_data) => {
                    if tunnel_data.is_err() {
                        PeerStream::close(peer_stream_to_peer_client_tx, stream_to_peer_stream_rx);
                        return Ok(());
                    }
                    tunnel_data.unwrap()
                }
                Err(_) => {
                    if is_close.load(Ordering::SeqCst) {
                        PeerStream::close(peer_stream_to_peer_client_tx, stream_to_peer_stream_rx);
                        return Ok(());
                    }
                    continue;
                }
            };

            log::trace!("TunnelData:{:?}", tunnel_data.header);
            if let Err(e) = protopack::write_tunnel_data(&mut buf_writer, &tunnel_data)
                .await
                .map_err(|e| anyhow::anyhow!("err:write_tunnel_data => e:{}", e))
            {
                PeerStream::close(peer_stream_to_peer_client_tx, stream_to_peer_stream_rx);
                return Err(e)?;
            }
        }
    }

    fn close(
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
    ) {
        log::debug!("stream_to_peer_stream_rx close");
        stream_to_peer_stream_rx.close();
        std::mem::drop(stream_to_peer_stream_rx);
        log::debug!("peer_stream_to_peer_client_tx close");
        peer_stream_to_peer_client_tx.close();
        std::mem::drop(peer_stream_to_peer_client_tx);
    }

    async fn read_flow(
        is_spawn: bool,
        stream_info: Arc<std::sync::Mutex<StreamFlowInfo>>,
    ) -> anyhow::Result<()> {
        let time = 10;
        let name = if is_spawn { "client" } else { "server" };
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(time)).await;

            let mut stream_info = stream_info.lock().unwrap();
            let read = stream_info.read;
            let write = stream_info.write;
            stream_info.read = 0;
            stream_info.write = 0;

            log::debug!(
                "name:{}, read:{:.3}M, write:{:.3}M",
                name,
                read as f64 / 1024 as f64 / 1024 as f64 / time as f64,
                write as f64 / 1024 as f64 / 1024 as f64 / time as f64,
            );
        }
    }
}
