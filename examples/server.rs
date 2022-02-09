use any_tunnel::server;
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = log4rs::init_file("examples/log4rs.yaml", Default::default()) {
        eprintln!("err:log4rs::init_file => e:{}", e);
        return Err(anyhow::anyhow!("err:log4rs::init_fil"))?;
    }

    let listen_addr = "127.0.0.1:28080".to_socket_addrs()?.next().unwrap();
    log::info!("listen_addr:{}", listen_addr);
    let listener = TcpListener::bind(listen_addr.clone()).await?;
    let server = server::Server::new();

    let (mut listen, publish) = server.listen().await;
    tokio::spawn(async move {
        loop {
            let (mut stream, _, _) = listen.accept().await.unwrap();
            log::info!("tunnel2 listen.accept");
            tokio::spawn(async move {
                let ret: anyhow::Result<()> = async {
                    stream.read_i32().await?;
                    let mut n = 0;
                    loop {
                        n += 1;
                        stream.write_i32(n).await?;
                        log::info!("write n:{}", n);
                        if n > 10000000 {
                            break;
                        }
                    }
                    Ok(())
                }
                .await;
                ret.unwrap_or_else(|e| log::error!("err:stream => e:{}", e));
                stream.close();
                log::info!("stream.close()");
            });
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;

        let local_addr = stream.local_addr()?;
        let remote_addr = stream.peer_addr()?;
        log::info!("listener.accept()");

        let publish = publish.clone();
        tokio::spawn(async move {
            if let Err(e) = publish
                .push_peer_stream(stream, local_addr, remote_addr)
                .await
            {
                log::error!("err: server stream => e:{}", e);
            }
            log::info!("server stream close");
        });
    }
}
