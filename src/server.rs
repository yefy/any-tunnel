use super::peer_client::PeerClient;
use super::protopack;
use super::stream::Stream;
use super::stream_flow::AsyncReadAsyncWrite;
use super::stream_flow::StreamFlow;
use hashbrown::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

pub type AcceptSenderType = async_channel::Sender<(Stream, SocketAddr, SocketAddr)>;
pub type AcceptReceiverType = async_channel::Receiver<(Stream, SocketAddr, SocketAddr)>;

pub struct Listener {
    accept_rx: AcceptReceiverType,
}

impl Listener {
    pub fn new(accept_rx: AcceptReceiverType) -> Listener {
        Listener { accept_rx }
    }

    pub async fn accept(&mut self) -> anyhow::Result<(Stream, SocketAddr, SocketAddr)> {
        let accept = self.accept_rx.recv().await?;
        log::debug!("accept_rx recv");
        Ok(accept)
    }
}

#[derive(Clone)]
pub struct Publish {
    accept_tx: AcceptSenderType,
    server: Server,
}

impl Publish {
    pub fn new(accept_tx: AcceptSenderType, server: Server) -> Publish {
        Publish { accept_tx, server }
    }

    pub async fn push_peer_stream<RW: 'static + AsyncReadAsyncWrite>(
        &self,
        stream: RW,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) -> anyhow::Result<()> {
        self.server
            .create_connect(
                StreamFlow::new(Box::new(stream)),
                local_addr,
                remote_addr,
                self.accept_tx.clone(),
            )
            .await
    }
}

pub struct ServerContext {
    peer_client_map: HashMap<String, Option<PeerClient>>,
}

#[derive(Clone)]
pub struct Server {
    context: Arc<Mutex<ServerContext>>,
}

impl Server {
    pub fn new() -> Server {
        Server {
            context: Arc::new(Mutex::new(ServerContext {
                peer_client_map: HashMap::new(),
            })),
        }
    }

    pub async fn create_connect(
        &self,
        mut stream: StreamFlow,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        accept_tx: AcceptSenderType,
    ) -> anyhow::Result<()> {
        let (mut r, _) = tokio::io::split(&mut stream);
        let tunnel_hello = protopack::read_tunnel_hello(&mut r).await?;
        if tunnel_hello.is_none() {
            return Err(anyhow::anyhow!("err:read_tunnel_hello"))?;
        }
        let tunnel_hello = tunnel_hello.unwrap();
        log::debug!("server tunnel_hello:{:?}", tunnel_hello);

        let session_id = tunnel_hello.session_id;

        let peer_client = {
            self.context
                .lock()
                .await
                .peer_client_map
                .get(&session_id)
                .cloned()
        };
        if peer_client.is_some() {
            let peer_client = peer_client.unwrap();
            if peer_client.is_some() {
                let peer_client = peer_client.unwrap();
                peer_client.create_server_peer_stream(false, stream).await?;
                return Ok(());
            } else {
                log::info!("session_id closed:{}", session_id);
                return Ok(());
            }
        }

        let (stream_to_peer_stream_tx, stream_to_peer_stream_rx) = async_channel::bounded(200);
        let (peer_stream_to_peer_client_tx, peer_stream_to_peer_client_rx) =
            async_channel::bounded(100);
        let (peer_client_to_stream_tx, peer_client_to_stream_rx) = mpsc::channel(100);

        let peer_client = PeerClient::new(stream_to_peer_stream_rx, peer_stream_to_peer_client_tx);
        {
            self.context
                .lock()
                .await
                .peer_client_map
                .insert(session_id.clone(), Some(peer_client.clone()));
        }
        let peer_client2 = peer_client.clone();
        let context = self.context.clone();
        tokio::spawn(async move {
            let ret: anyhow::Result<()> = async {
                peer_client
                    .start(
                        peer_client_to_stream_tx,
                        peer_stream_to_peer_client_rx,
                        session_id.clone(),
                        None,
                    )
                    .await?;
                Ok(())
            }
            .await;
            {
                let mut context = context.lock().await;
                let mut peer_client = context.peer_client_map.get_mut(&session_id);
                if peer_client.is_some() {
                    let peer_client = peer_client.as_mut().unwrap();
                    peer_client.take();
                }
            }
            ret.unwrap_or_else(|e| log::error!("err:PeerPack => e:{}", e));
        });

        accept_tx
            .send((
                Stream::new(stream_to_peer_stream_tx, peer_client_to_stream_rx),
                local_addr,
                remote_addr,
            ))
            .await
            .map_err(|_| anyhow::anyhow!("err:accept_tx"))?;

        peer_client2
            .create_server_peer_stream(false, stream)
            .await?;
        Ok(())
    }

    pub async fn listen(&self) -> (Listener, Publish) {
        let (accept_tx, accept_rx) = async_channel::unbounded::<(Stream, SocketAddr, SocketAddr)>();
        (
            Listener::new(accept_rx),
            Publish::new(accept_tx, self.clone()),
        )
    }
}
