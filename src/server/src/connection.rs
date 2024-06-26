use bytes::Bytes;
use commands::commands::GLOBAL_COMMANDS_TABLE;
use common_base::bytes::StringBytes;
use common_telemetry::log::debug;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt as _, StreamExt};
use redis_protocol::codec::Resp3;
use redis_protocol::resp3::types::{BytesFrame, FrameKind};
use roxy::storage::StorageRef;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::error::{Result, UnknownCommandSnafu};

type Stream<T> = SplitStream<Framed<T, Resp3>>;
type Sink<T> = SplitSink<Framed<T, Resp3>, BytesFrame>;

pub struct Connection<T> {
    reader: Stream<T>,
    writer: Sink<T>,
    namespace: Bytes,
    storage: StorageRef,
    command_tokens_buf: Vec<Bytes>,
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(inner: T, storage: StorageRef) -> Self {
        let frame = Framed::new(inner, Resp3::default());
        let (writer, reader) = frame.split();
        Self {
            reader,
            writer,
            storage,
            namespace: Bytes::from_static(b"default"),
            command_tokens_buf: Vec::with_capacity(64),
        }
    }

    fn frame_as_bytes_array(&mut self, frame: BytesFrame) {
        self.command_tokens_buf.clear();
        match frame {
            BytesFrame::Array { data, .. } => {
                data.iter()
                    .map(|b| match b {
                        BytesFrame::SimpleString { data, .. } => data.clone(),
                        BytesFrame::BlobString { data, .. } => data.clone(),
                        _ => unreachable!(),
                    })
                    .collect_into(&mut self.command_tokens_buf);
            }
            _ => unreachable!(),
        }
    }

    pub async fn start(&mut self) {
        while let Some(frame) = self.reader.next().await {
            let frame = frame.unwrap();
            debug!("Received: {:?}", frame);
            self.frame_as_bytes_array(frame);
            let response = self.to_response(self.execute_command(&self.command_tokens_buf[..]));
            let res = self.writer.send(response).await;
            assert!(res.is_ok());
        }
    }
    fn to_response(&self, response: Result<BytesFrame>) -> BytesFrame {
        match response {
            Ok(frame) => frame,
            Err(e) => {
                debug!("Error: {:?}", e);
                (FrameKind::SimpleError, e.to_string()).try_into().unwrap()
            }
        }
    }

    fn execute_command(&self, command_tokens: &[Bytes]) -> Result<BytesFrame> {
        if let Some(c) = command_tokens.first() {
            let s = StringBytes::new(c.clone());
            if let Some(command) = GLOBAL_COMMANDS_TABLE.get(s.as_utf8()) {
                let mut command_inst = command.create_instance();
                // Skip the first token, which is the command name
                command_inst.parse(&command_tokens[1..]).unwrap();
                Ok(command_inst.execute(&self.storage, self.namespace.clone())?)
            } else {
                UnknownCommandSnafu { cmd: s.as_utf8() }.fail()
            }
        } else {
            UnknownCommandSnafu { cmd: "" }.fail()
        }
    }
}

#[cfg(test)]
pub mod test_utility {
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::{Buf, BufMut};
    use redis_protocol::codec;
    use redis_protocol::resp3::encode::complete::encode_bytes;
    use redis_protocol::resp3::types::{OwnedFrame, Resp3Frame};
    use tokio::io::{self, AsyncRead, AsyncWrite};

    pub struct MockingTcpStream {
        pub read_buf: Cursor<Vec<u8>>,
        pub write_buf: Vec<u8>,
    }

    impl MockingTcpStream {
        pub fn sending_cmd(cmd: &str) -> MockingTcpStream {
            MockingTcpStream {
                read_buf: Cursor::new(resp3_encode_command(cmd)),
                write_buf: vec![],
            }
        }
        pub fn response(&self) -> OwnedFrame {
            let (frame, _) = redis_protocol::resp3::decode::complete::decode(&self.write_buf)
                .unwrap()
                .unwrap();
            frame
        }
    }

    impl AsyncRead for MockingTcpStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            while self.read_buf.has_remaining() && buf.remaining() > 0 {
                buf.put_u8(self.read_buf.get_u8());
            }
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockingTcpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.write_buf.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    pub fn resp3_encode_command(cmd: &str) -> Vec<u8> {
        let frame = codec::resp3_encode_command(cmd);
        let mut buf = vec![0; frame.encode_len()];
        encode_bytes(&mut buf, &frame).unwrap();
        buf
    }
}

/*

#[cfg(test)]
mod tests {

    use common_telemetry::log::init_ut_logging;
    use redis_protocol::resp3::types::OwnedFrame;
    use tests::test_utility::MockingTcpStream;

    use super::*;

    #[tokio::test]
    async fn test_hello() {
        let mut mocking_stream = MockingTcpStream::sending_cmd("HELLO");
        {
            let mut conn = Connection::new(&mut mocking_stream);
            conn.start().await;
        }
        assert_eq!(
            mocking_stream.response(),
            OwnedFrame::SimpleString {
                data: b"OK"[..].to_vec(),
                attributes: None
            }
        );
    }

    #[tokio::test]
    async fn test() {
        init_ut_logging();
        let mut mocking_stream = MockingTcpStream::sending_cmd("SET key \"Value\"");
        {
            let mut conn = Connection::new(&mut mocking_stream);
            conn.start().await;
        }
        mocking_stream.response()
    }
}

*/
