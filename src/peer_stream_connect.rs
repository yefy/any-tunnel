use super::stream_flow::StreamFlow;
use super::Protocol4;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;

#[async_trait]
pub trait PeerStreamConnect: Send + Sync {
    async fn connect(&self, _: &SocketAddr) -> Result<(StreamFlow, SocketAddr, SocketAddr)>;
    async fn connect_addr(&self) -> Result<SocketAddr>; //这个是域名获取到的ip地址
    async fn address(&self) -> String; //地址或域名  配上文件上的
    async fn protocol4(&self) -> Protocol4; //tcp 或 udp  四层协议
    async fn protocol7(&self) -> String; //tcp udp  quic http 等7层协议
    async fn max_connect(&self) -> usize;
    async fn stream_send_timeout(&self) -> usize;
    async fn stream_recv_timeout(&self) -> usize;
}

pub struct PeerStreamConnectTcp {
    addr: String,
    max_connect: usize,
}

impl PeerStreamConnectTcp {
    pub fn new(addr: String, max_connect: usize) -> PeerStreamConnectTcp {
        PeerStreamConnectTcp { addr, max_connect }
    }
}

#[async_trait]
impl PeerStreamConnect for PeerStreamConnectTcp {
    async fn connect(
        &self,
        connect_addr: &SocketAddr,
    ) -> Result<(StreamFlow, SocketAddr, SocketAddr)> {
        let stream = TcpStream::connect(connect_addr)
            .await
            .map_err(|e| anyhow!("err:TcpStream::connect => e:{}", e))?;
        let local_addr = stream.local_addr()?;
        let remote_addr = stream.peer_addr()?;
        Ok((StreamFlow::new(Box::new(stream)), local_addr, remote_addr))
    }
    async fn connect_addr(&self) -> Result<SocketAddr> {
        let connect_addr = self
            .addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "err:empty address"))
            .map_err(|e| anyhow!("err:to_socket_addrs => e:{}", e))?;
        Ok(connect_addr)
    }
    async fn address(&self) -> String {
        self.addr.clone()
    }
    async fn protocol4(&self) -> Protocol4 {
        Protocol4::TCP
    }
    async fn protocol7(&self) -> String {
        "tcp".to_string()
    }
    async fn max_connect(&self) -> usize {
        self.max_connect
    }
    async fn stream_send_timeout(&self) -> usize {
        10
    }
    async fn stream_recv_timeout(&self) -> usize {
        10
    }
}
