use bytes::Bytes;
use common_telemetry::log::info;
use futures::{SinkExt as _, StreamExt};
use redis_protocol::codec::Resp3;
use redis_protocol::resp3::types::BytesFrame;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct Connection<T> {
    stream: Framed<T, Resp3>,
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(inner: T) -> Self {
        let framed_stream = Framed::new(inner, Resp3::default());
        Self {
            stream: framed_stream,
        }
    }

    pub async fn start(&mut self) {
        while let Some(frame) = self.stream.next().await {
            let frame = frame.unwrap();
            info!("Received: {:?}", frame);
            let response = BytesFrame::SimpleString {
                data: Bytes::from_static(b"OK"),
                attributes: None,
            };
            self.stream.send(response).await.unwrap();
        }

        // Rest of the code...
    }
}
