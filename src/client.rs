use super::peer_client::PeerClient;
use super::peer_stream_connect::PeerStreamConnect;
use super::stream::Stream;
use chrono::prelude::*;
use lazy_static::lazy_static;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

lazy_static! {
    static ref CLIENT_ID: AtomicU32 = AtomicU32::new(1);
    static ref STREAM_ID: AtomicU32 = AtomicU32::new(1);
}

#[derive(Clone)]
pub struct Client {
    pid: i32,
}

impl Client {
    pub fn new() -> Client {
        let pid = unsafe { libc::getpid() };
        Client { pid }
    }

    pub async fn connect(
        &self,
        peer_stream_connect: Arc<Box<dyn PeerStreamConnect>>,
    ) -> anyhow::Result<(Stream, SocketAddr, SocketAddr)> {
        let connect_addr = peer_stream_connect.connect_addr().await?;
        let (stream, local_addr, remote_addr) = peer_stream_connect.connect(&connect_addr).await?;
        let session_id = {
            let client_id = CLIENT_ID.fetch_add(1, Ordering::Relaxed);
            format!(
                "{}{:?}{}{}{}{}",
                self.pid,
                std::thread::current().id(),
                client_id,
                local_addr,
                remote_addr,
                Local::now().timestamp_millis(),
            )
        };

        let (stream_to_peer_stream_tx, stream_to_peer_stream_rx) = async_channel::bounded(200);
        let (peer_stream_to_peer_client_tx, peer_stream_to_peer_client_rx) =
            async_channel::bounded(100);
        let (peer_client_to_stream_tx, peer_client_to_stream_rx) = mpsc::channel(100);

        let peer_client = PeerClient::new(stream_to_peer_stream_rx, peer_stream_to_peer_client_tx);
        peer_client
            .create_peer_stream(true, session_id.clone(), stream)
            .await?;
        tokio::spawn(async move {
            let ret: anyhow::Result<()> = async {
                peer_client
                    .start(
                        peer_client_to_stream_tx,
                        peer_stream_to_peer_client_rx,
                        session_id,
                        Some((peer_stream_connect, connect_addr)),
                    )
                    .await?;
                Ok(())
            }
            .await;
            ret.unwrap_or_else(|e| log::error!("err:PeerPack => e:{}", e));
        });

        Ok((
            Stream::new(stream_to_peer_stream_tx, peer_client_to_stream_rx),
            local_addr,
            remote_addr,
        ))
    }
}
