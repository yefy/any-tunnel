use serde::ser;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub const TUNNEL_MAX_HEADER_SIZE: usize = 4096;
pub const TUNNEL_VERSION: &'static str = "tunnel.0.1.0";

//TunnelHeaderSize_u16 TunnelHeader TunnelHello
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelHeader {
    pub header_type: u8,
    pub body_size: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TunnelHeaderType {
    TunnelHello = 1,
    TunnelData = 2,
    Max = 3,
}

#[derive(Clone, Debug)]
pub enum TunnelPack {
    TunnelHello(TunnelHello),
    TunnelData(TunnelData),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelHello {
    pub version: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TunnelDataHeader {
    pub pack_id: u32,
    pub pack_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelData {
    pub header: TunnelDataHeader,
    pub datas: Vec<u8>,
}

pub async fn read_tunnel_hello<R: AsyncRead + std::marker::Unpin>(
    buf_reader: &mut R,
) -> anyhow::Result<Option<TunnelHello>> {
    let mut slice = [0u8; TUNNEL_MAX_HEADER_SIZE];
    let pack = read_pack(buf_reader, &mut slice).await?;
    match pack {
        TunnelPack::TunnelHello(tunnel_hello) => {
            return Ok(Some(tunnel_hello));
        }
        _ => return Err(anyhow::anyhow!("err:not tunnel_hello")),
    }
}

pub async fn write_tunnel_data<R: AsyncWrite + std::marker::Unpin>(
    buf_writer: &mut R,
    value: &TunnelData,
) -> anyhow::Result<()> {
    let typ = TunnelHeaderType::TunnelData;
    write_pack(buf_writer, typ, &value.header, false).await?;
    buf_writer.write_all(&value.datas).await?;
    log::trace!("write_pack datas len:{:?}", value.datas.len());
    buf_writer.flush().await?;
    Ok(())
}

pub async fn write_pack<T: ?Sized, W: AsyncWrite + std::marker::Unpin>(
    buf_writer: &mut W,
    typ: TunnelHeaderType,
    value: &T,
    is_flush: bool,
) -> anyhow::Result<()>
where
    T: ser::Serialize,
{
    let slice = toml::to_vec(value)?;
    let header = TunnelHeader {
        header_type: typ as u8,
        body_size: slice.len() as u16,
    };
    let header_slice = toml::to_vec(&header)?;
    buf_writer.write_u16(header_slice.len() as u16).await?;
    log::trace!("write_pack header len:{:?}", header_slice.len());
    buf_writer.write_all(&header_slice).await?;
    log::trace!("write_pack header:{:?}", header);
    buf_writer.write_all(&slice).await?;
    log::trace!("write_pack body len:{:?}", slice.len());
    if is_flush {
        buf_writer.flush().await?;
    }
    Ok(())
}

pub async fn read_pack<R: AsyncRead + std::marker::Unpin>(
    buf_reader: &mut R,
    slice: &mut [u8],
) -> anyhow::Result<TunnelPack> {
    //let mut slice = [0u8; TUNNEL_MAX_HEADER_SIZE];
    let header_size = buf_reader
        .read_u16()
        .await
        .map_err(|e| anyhow::anyhow!("err:header_size => e:{}", e))?;

    log::trace!("header_size:{}", header_size);
    if header_size as usize > slice.len() || header_size <= 0 {
        return Err(anyhow::anyhow!(
            "err:header_size > slice.len() => header_size:{}",
            header_size
        ));
    }

    let header_slice = &mut slice[..header_size as usize];
    buf_reader
        .read_exact(header_slice)
        .await
        .map_err(|e| anyhow::anyhow!("err:header_slice => e:{}", e))?;
    let header: TunnelHeader = toml::from_slice(header_slice)
        .map_err(|e| anyhow::anyhow!("err:TunnelData header=> e:{}", e))?;

    if header.header_type >= TunnelHeaderType::Max as u8 {
        return Err(anyhow::anyhow!("err:TunnelData header_type"));
    }

    if header.body_size as usize > slice.len() {
        return Err(anyhow::anyhow!("err:TunnelData body_size"));
    }
    log::trace!("read_pack header:{:?}", header);

    let body_slice = &mut slice[..header.body_size as usize];
    if header.body_size > 0 {
        buf_reader
            .read_exact(body_slice)
            .await
            .map_err(|e| anyhow::anyhow!("err:body_slice => e:{}", e))?;
    }

    if header.header_type == TunnelHeaderType::TunnelHello as u8 {
        let value: TunnelHello = toml::from_slice(body_slice)
            .map_err(|e| anyhow::anyhow!("err:TunnelDataAck=> e:{}", e))?;
        Ok(TunnelPack::TunnelHello(value))
    } else if header.header_type == TunnelHeaderType::TunnelData as u8 {
        let header: TunnelDataHeader = toml::from_slice(body_slice)
            .map_err(|e| anyhow::anyhow!("err:TunnelData=> e:{}", e))?;
        log::trace!("read_pack body:{:?}", header);
        let mut datas = vec![0u8; header.pack_size as usize];
        let mut datas_slice = datas.as_mut_slice();
        buf_reader
            .read_exact(&mut datas_slice)
            .await
            .map_err(|e| anyhow::anyhow!("err:datas => e:{}", e))?;

        log::trace!("read_pack datas.len:{:?}", datas.len());
        Ok(TunnelPack::TunnelData(TunnelData { header, datas }))
    } else {
        Err(anyhow::anyhow!("err:TunnelPack type"))
    }
}
