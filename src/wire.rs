//! The wire: one WebSocket per space, speaking the Loro Protocol.
//!
//! The socket task owns the socket and nothing else. It encodes what the engine sends, decodes
//! what arrives, answers keepalives, and splits and joins fragments. It knows nothing about Loro
//! docs; those belong to the engine.

use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use loro_protocol::{BatchId, ProtocolMessage, decode, encode};
use tokio::{
    sync::mpsc,
    time::{Instant, interval},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

/// What the engine uses to send. Dropping it closes the socket.
pub type Outbox = mpsc::UnboundedSender<ProtocolMessage>;

#[derive(Debug)]
pub enum WireEvent {
    Message(ProtocolMessage),
    /// The socket is gone. `revoked` means the relay closed it because this device was revoked,
    /// so reconnecting is pointless.
    Closed {
        revoked: bool,
    },
}

const FRAGMENT_SIZE: usize = 200 * 1024; // the protocol caps a message at 256 KiB
const PING_EVERY: Duration = Duration::from_secs(20);
const DEAD_AFTER: Duration = Duration::from_secs(60);
const CLOSE_REVOKED: u16 = 4003;

/// Connect and spawn the socket task. Every incoming message, and finally `Closed`, is passed to
/// `emit`, which forwards it into the engine's inbox.
pub async fn connect(url: &str, emit: impl Fn(WireEvent) + Send + Sync + 'static) -> Result<Outbox> {
    let (ws, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(url)).await??;
    let (outbox, mut outgoing) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let revoked = run(ws, &mut outgoing, &emit).await;
        emit(WireEvent::Closed { revoked });
    });
    Ok(outbox)
}

pub fn batch_id() -> BatchId {
    BatchId(rand::random())
}

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The socket loop. Returns whether the relay closed us for being revoked.
async fn run(ws: Socket, outgoing: &mut mpsc::UnboundedReceiver<ProtocolMessage>, emit: &impl Fn(WireEvent)) -> bool {
    let (mut sink, mut stream) = ws.split();
    let mut ping = interval(PING_EVERY);
    let mut last_heard = Instant::now();
    let mut partials: HashMap<BatchId, Vec<Option<Vec<u8>>>> = HashMap::new();

    loop {
        tokio::select! {
            msg = outgoing.recv() => {
                let Some(msg) = msg else {
                    // The engine dropped its outbox: we're done.
                    let _ = sink.close().await;
                    return false;
                };
                for frame in frames(msg) {
                    if sink.send(Message::binary(frame)).await.is_err() {
                        return false;
                    }
                }
            }
            frame = stream.next() => {
                let Some(Ok(frame)) = frame else { return false };
                last_heard = Instant::now();
                match frame {
                    Message::Text(t) if t.as_str() == "ping" => {
                        let _ = sink.send(Message::text("pong")).await;
                    }
                    Message::Binary(bytes) => match decode(&bytes) {
                        Ok(msg) => {
                            if let Some(msg) = reassemble(&mut partials, msg) {
                                emit(WireEvent::Message(msg));
                            }
                        }
                        Err(e) => eprintln!("relay sent a message we can't decode: {e}"),
                    },
                    Message::Close(frame) => return is_revoked(frame),
                    _ => {}
                }
            }
            _ = ping.tick() => {
                // No traffic at all for a minute (not even our pings' pongs): the link is dead,
                // even if the OS hasn't noticed yet.
                if last_heard.elapsed() > DEAD_AFTER || sink.send(Message::text("ping")).await.is_err() {
                    return false;
                }
            }
        }
    }
}

fn is_revoked(frame: Option<CloseFrame>) -> bool {
    frame.is_some_and(|f| u16::from(f.code) == CLOSE_REVOKED)
}

/// Encode a message, splitting a single oversized update into a header and fragments.
fn frames(msg: ProtocolMessage) -> Vec<Vec<u8>> {
    let msgs = match msg {
        ProtocolMessage::DocUpdate { crdt, room_id, updates, batch_id }
            if updates.len() == 1 && updates[0].len() > FRAGMENT_SIZE =>
        {
            let update = &updates[0];
            let chunks: Vec<&[u8]> = update.chunks(FRAGMENT_SIZE).collect();
            let mut msgs = vec![ProtocolMessage::DocUpdateFragmentHeader {
                crdt,
                room_id: room_id.clone(),
                batch_id,
                fragment_count: chunks.len() as u64,
                total_size_bytes: update.len() as u64,
            }];
            msgs.extend(chunks.into_iter().enumerate().map(|(i, chunk)| ProtocolMessage::DocUpdateFragment {
                crdt,
                room_id: room_id.clone(),
                batch_id,
                index: i as u64,
                fragment: chunk.to_vec(),
            }));
            msgs
        }
        msg => vec![msg],
    };
    msgs.iter().filter_map(|m| encode(m).map_err(|e| eprintln!("can't encode message: {e}")).ok()).collect()
}

/// Pass messages through, except fragments: hold those until the batch is complete, then emit
/// one `DocUpdate`.
fn reassemble(partials: &mut HashMap<BatchId, Vec<Option<Vec<u8>>>>, msg: ProtocolMessage) -> Option<ProtocolMessage> {
    match msg {
        ProtocolMessage::DocUpdateFragmentHeader { batch_id, fragment_count, .. } => {
            partials.insert(batch_id, vec![None; fragment_count as usize]);
            None
        }
        ProtocolMessage::DocUpdateFragment { crdt, room_id, batch_id, index, fragment } => {
            let parts = partials.get_mut(&batch_id)?;
            *parts.get_mut(index as usize)? = Some(fragment);
            if parts.iter().any(Option::is_none) {
                return None;
            }
            let parts = partials.remove(&batch_id)?;
            Some(ProtocolMessage::DocUpdate {
                crdt,
                room_id,
                updates: vec![parts.into_iter().flatten().flatten().collect()],
                batch_id,
            })
        }
        msg => Some(msg),
    }
}
