use super::peer_stream::PeerStream;
use super::peer_stream_connect::PeerStreamConnect;
use super::protopack;
use super::protopack::TunnelData;
use super::protopack::TunnelHello;
use super::protopack::TUNNEL_VERSION;
use super::stream_flow::StreamFlow;
use anyhow::anyhow;
use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct PeerClient {
    stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
    peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
}

impl PeerClient {
    pub fn new(
        stream_to_peer_stream_rx: async_channel::Receiver<TunnelData>,
        peer_stream_to_peer_client_tx: async_channel::Sender<TunnelData>,
    ) -> PeerClient {
        PeerClient {
            stream_to_peer_stream_rx,
            peer_stream_to_peer_client_tx,
        }
    }
    pub async fn start(
        &self,
        peer_client_to_stream_tx: mpsc::Sender<TunnelData>,
        peer_stream_to_peer_client_rx: async_channel::Receiver<TunnelData>,
        session_id: String,
        peer_stream_connect: Option<(Arc<Box<dyn PeerStreamConnect>>, SocketAddr)>,
    ) -> Result<()> {
        tokio::select! {
            biased;
            ret = self.peer_client_to_stream(peer_client_to_stream_tx, peer_stream_to_peer_client_rx) => {
                ret.map_err(|e| anyhow!("err:peer_client_to_stream => e:{}", e))?;
                Ok(())
            }
            ret = self.create_connect(session_id,peer_stream_connect) => {
                ret.map_err(|e| anyhow!("err:create_connect => e:{}", e))?;
                Ok(())
            }
            else => {
                return Err(anyhow!("err:select"));
            }
        }
    }

    async fn peer_client_to_stream(
        &self,
        peer_client_to_stream_tx: mpsc::Sender<TunnelData>,
        peer_stream_to_peer_client_rx: async_channel::Receiver<TunnelData>,
    ) -> Result<()> {
        let mut pack_id = 1u32;
        let mut send_pack_id_map = HashMap::<u32, ()>::new();
        let mut recv_pack_cache_map = HashMap::<u32, TunnelData>::new();
        loop {
            let tunnel_data = peer_stream_to_peer_client_rx.recv().await;
            if tunnel_data.is_err() {
                log::debug!("peer_stream_to_peer_client_rx close");
                return Ok(());
            }
            let tunnel_data = tunnel_data?;
            if send_pack_id_map.get(&tunnel_data.header.pack_id).is_some() {
                return Err(anyhow!(
                    "err:pack_id exist => pack_id:{}",
                    tunnel_data.header.pack_id
                ))?;
            }
            if tunnel_data.header.pack_id == pack_id {
                log::trace!("tunnel_data.header:{:?}", tunnel_data.header);
                send_pack_id_map.insert(tunnel_data.header.pack_id, ());
                pack_id += 1;
                if let Err(_) = peer_client_to_stream_tx.send(tunnel_data).await {
                    log::debug!("peer_client_to_stream_tx close");
                    return Ok(());
                }

                while let Some(tunnel_data) = recv_pack_cache_map.remove(&pack_id) {
                    log::trace!("tunnel_data.header:{:?}", tunnel_data.header);
                    send_pack_id_map.insert(tunnel_data.header.pack_id, ());
                    pack_id += 1;
                    if let Err(_) = peer_client_to_stream_tx.send(tunnel_data).await {
                        log::debug!("peer_client_to_stream_tx close");
                        return Ok(());
                    }
                }
            } else {
                recv_pack_cache_map.insert(tunnel_data.header.pack_id, tunnel_data);
            }
        }
    }

    async fn create_connect(
        &self,
        session_id: String,
        peer_stream_connect: Option<(Arc<Box<dyn PeerStreamConnect>>, SocketAddr)>,
    ) -> Result<()> {
        let mut peer_stream_len = 1;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            if peer_stream_connect.is_none() {
                continue;
            }
            log::debug!("peer_stream_len:{}", peer_stream_len);
            let (peer_stream_connect, connect_addr) = peer_stream_connect.as_ref().unwrap();
            let peer_stream_max_len = peer_stream_connect.max_connect().await;
            if peer_stream_len >= peer_stream_max_len {
                continue;
            }

            let ret: Result<()> = async {
                log::debug!("connect_addr:{}", connect_addr);
                let (stream, _, _) = peer_stream_connect
                    .connect(connect_addr)
                    .await
                    .map_err(|e| anyhow!("err:connect => e:{}", e))?;
                let session_id = session_id.clone();
                self.create_peer_stream(true, session_id, stream)
                    .await
                    .map_err(|e| anyhow!("err:create_peer_stream => e:{}", e))?;
                Ok(())
            }
            .await;
            if ret.is_err() {
                ret.unwrap_or_else(|e| log::error!("err:create_peer_stream => e:{}", e));
                continue;
            }
            peer_stream_len += 1;
        }
    }

    pub async fn create_peer_stream(
        &self,
        is_spawn: bool,
        session_id: String,
        mut stream: StreamFlow,
    ) -> Result<()> {
        let stream_to_peer_stream_rx = self.stream_to_peer_stream_rx.clone();
        let peer_stream_to_peer_client_tx = self.peer_stream_to_peer_client_tx.clone();
        let tunnel_hello = TunnelHello {
            version: TUNNEL_VERSION.to_string(),
            session_id,
        };
        log::debug!("client tunnel_hello:{:?}", tunnel_hello);
        let (_, mut w) = tokio::io::split(&mut stream);
        protopack::write_pack(
            &mut w,
            protopack::TunnelHeaderType::TunnelHello,
            &tunnel_hello,
            true,
        )
        .await
        .map_err(|e| anyhow!("err:protopack::write_pack => e:{}", e))?;

        PeerStream::start(
            is_spawn,
            stream,
            stream_to_peer_stream_rx,
            peer_stream_to_peer_client_tx,
        )
        .await
        .map_err(|e| anyhow!("err:PeerStream::start => e:{}", e))?;
        Ok(())
    }

    pub async fn create_server_peer_stream(
        &self,
        is_spawn: bool,
        stream: StreamFlow,
    ) -> Result<()> {
        let stream_to_peer_stream_rx = self.stream_to_peer_stream_rx.clone();
        let peer_stream_to_peer_client_tx = self.peer_stream_to_peer_client_tx.clone();

        PeerStream::start(
            is_spawn,
            stream,
            stream_to_peer_stream_rx,
            peer_stream_to_peer_client_tx,
        )
        .await
        .map_err(|e| anyhow!("err:PeerStream::start => e:{}", e))?;
        Ok(())
    }
}
