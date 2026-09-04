use futures_util::{SinkExt, stream::SplitSink};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use super::{TransportCloseReason, TransportClosure};
use crate::peer::{codec::PeerFrameCodec, frame::PeerFrame};

/// Runs the only ordered socket sink for one PeerTransport. The coalesced wake
/// signal tells the transport actor to retry draining its bounded per-stream
/// queues after aggregate capacity becomes available.
pub(super) async fn run_writer(
    mut sink: SplitSink<Framed<TcpStream, PeerFrameCodec>, PeerFrame>,
    mut frames: mpsc::Receiver<PeerFrame>,
    wake: mpsc::Sender<()>,
    closure: TransportClosure,
) {
    let close = closure.token().clone();
    loop {
        tokio::select! {
            () = close.cancelled() => break,
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                let send = sink.send(frame);
                tokio::select! {
                    () = close.cancelled() => break,
                    result = send => {
                        if result.is_err() {
                            closure.fail(TransportCloseReason::WriterFailed);
                            break;
                        }
                        match wake.try_send(()) {
                            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                }
            }
        }
    }
}
