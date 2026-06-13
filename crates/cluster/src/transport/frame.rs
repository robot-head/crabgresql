//! Length-prefixed (u32 BE) postcard frames over any async byte stream.
use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::MAX_FRAME;

/// Serialize `msg` with postcard and write it as a u32-BE length prefix + body.
pub async fn write_msg<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let bytes =
        postcard::to_stdvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read one length-prefixed postcard frame and deserialize it as `T`.
pub async fn read_msg<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    postcard::from_bytes(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_round_trips_over_a_duplex() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let sent = vec![("hello".to_string(), 42u64), ("x".into(), 7)];
        let writer = sent.clone();
        let h = tokio::spawn(async move { write_msg(&mut a, &writer).await.expect("write_msg") });
        let got: Vec<(String, u64)> = read_msg(&mut b).await.expect("read_msg");
        h.await.expect("task join");
        assert_eq!(got, sent);
    }
}
