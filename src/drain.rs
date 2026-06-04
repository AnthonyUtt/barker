//! The drain loop used by [`MessageBus::process_messages`](crate::MessageBus::process_messages).

use std::sync::RwLock;
use std::time::Instant;

use crate::error::{BarkerError, Result};
use crate::handler::{Handler, HandlerType};
use crate::message::Envelope;

// Drain up to `limit` messages from `receiver`, dispatching each one to every matching
// handler registered in `handlers_lock`.
//
// Messages whose TTL has elapsed (measured from envelope.timestamp) are skipped before
// dispatch. Dispatch order is the order in which handlers were registered — the
// `priority()` method on `Message` is *not* consulted, even though it is declared on the
// trait. See the crate-level docs for rationale.
pub(crate) fn process(
    receiver: &flume::Receiver<Envelope>,
    handlers_lock: &RwLock<Vec<Handler>>,
    limit: Option<usize>,
) -> Result<()> {
    let limit = limit.unwrap_or(usize::MAX);
    let mail: Vec<Envelope> = receiver.try_iter().take(limit).collect();

    let handlers = handlers_lock
        .read()
        .map_err(|_| BarkerError::HandlerLockPoisoned)?;

    for envelope in mail {
        if let Some(ttl) = envelope.message.ttl() {
            if Instant::now() - envelope.timestamp > ttl {
                continue;
            }
        }

        let type_id = envelope.message.message_type_id();

        for handler in handlers.iter() {
            match handler.type_id {
                HandlerType::Generic => handler.inner.call(&*envelope.message),
                HandlerType::Typed(tid) if tid == type_id => handler.inner.call(&*envelope.message),
                _ => continue,
            }
        }
    }

    Ok(())
}
