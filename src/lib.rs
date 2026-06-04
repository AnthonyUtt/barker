//! `barker` is a small, synchronous, trait-object event bus.
//!
//! Think of it as a town crier: anyone can shout a message into the crowd; some
//! listeners stop because the message is addressed to their type, others (generic
//! handlers) listen to everything that flies by, and the rest walk on.
//!
//! # Quick start
//!
//! ```
//! use std::any::{Any, TypeId};
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use barker::{Message, MessageHandler, MessageBus};
//!
//! // 1. Define a message. Any `'static + Send + Sync + Debug` type works.
//! #[derive(Debug)]
//! struct Ping(&'static str);
//!
//! impl Message for Ping {
//!     fn as_any(&self) -> &dyn Any { self }
//!     fn as_any_mut(&mut self) -> &mut dyn Any { self }
//! }
//!
//! // 2. Define a handler. Use `as_any().downcast_ref::<T>()` to recover the concrete
//! //    message type. Share mutable state via Arc + atomics or Mutex.
//! struct Counter(Arc<AtomicUsize>);
//! impl MessageHandler for Counter {
//!     fn call(&self, msg: &dyn Message) {
//!         if msg.as_any().downcast_ref::<Ping>().is_some() {
//!             self.0.fetch_add(1, Ordering::SeqCst);
//!         }
//!     }
//! }
//!
//! // 3. Wire it up on an owned bus instance (or use the process-wide global — see below).
//! let bus = MessageBus::new();
//! let count = Arc::new(AtomicUsize::new(0));
//! bus.register_handler(
//!     Box::new(Counter(count.clone())),
//!     Some(TypeId::of::<Ping>()),
//! ).unwrap();
//!
//! bus.send(Ping("hello")).unwrap();
//! bus.process_messages(None).unwrap();
//!
//! assert_eq!(count.load(Ordering::SeqCst), 1);
//! ```
//!
//! # How it works
//!
//! - **Trait-based messages.** Anything implementing [`Message`] can flow through the
//!   bus. There is no central `enum` — downstream crates can define their own message
//!   types without modifying barker.
//! - **Filtering.** Handlers register with either `Some(TypeId::of::<T>())` (typed — fires
//!   only for messages of type `T`) or `None` (generic — fires for every message).
//!   Matching uses [`TypeId`], so a typed handler never sees messages
//!   of an unrelated type even when both share a category.
//! - **Buffered send, explicit drain.** [`MessageBus::send`] enqueues onto an internal
//!   [`flume`] channel and returns immediately. Handlers do not run until someone calls
//!   [`MessageBus::process_messages`].
//! - **TTL enforcement.** Messages whose [`Message::ttl`] has elapsed by drain time are
//!   silently skipped.
//! - **Registration-order dispatch.** Within a drain, handlers are invoked in the order
//!   they were registered. There is no priority-based reordering.
//!
//! # Static global vs owned instance
//!
//! For low-ceremony, application-wide event flows, use the free functions [`send`],
//! [`register_handler`], and [`process_messages`] — they all delegate to a process-wide
//! [`MessageBus`] accessible via [`MessageBus::global`]. For tests, plugins with isolated
//! event streams, or library code that should not touch global state, construct your own
//! bus with [`MessageBus::new`] or [`MessageBus::bounded`].
//!
//! # Aspirational metadata
//!
//! The [`Message`] trait declares [`priority`](Message::priority) and
//! [`requires_ack`](Message::requires_ack), but neither is currently consulted by the
//! drain. They are preserved on the trait for forward compatibility; document your
//! intent on a per-message basis, but do not rely on the bus to honour them.
//!
//! # Origin
//!
//! Extracted from the [VITRIOL game engine](https://github.com/AnthonyUtt/vitriol),
//! where the bus is used to decouple input, window, and system events across the
//! plugin architecture.

#![deny(missing_docs)]

mod bus;
mod drain;
mod error;
mod handler;
mod message;

pub use bus::MessageBus;
pub use error::{BarkerError, Result};
pub use handler::MessageHandler;
pub use message::Message;

use std::any::TypeId;

/// Enqueue `msg` on the process-wide [`MessageBus::global`].
///
/// Convenience wrapper for `MessageBus::global().send(msg)`. See
/// [`MessageBus::send`] for full semantics.
///
/// # Examples
///
/// ```
/// # use std::any::Any;
/// use barker::{send, Message};
/// # #[derive(Debug)] struct Ping;
/// # impl Message for Ping {
/// #     fn as_any(&self) -> &dyn Any { self }
/// #     fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// # }
/// let _id = send(Ping).expect("send");
/// ```
pub fn send<M: Message>(msg: M) -> Result<u64> {
    MessageBus::global().send(msg)
}

/// Register `handler` on the process-wide [`MessageBus::global`].
///
/// Convenience wrapper for `MessageBus::global().register_handler(...)`. Pass
/// `Some(TypeId::of::<T>())` to register a typed handler, or `None` for a generic
/// handler that fires on every message. See [`MessageBus::register_handler`] for full
/// semantics.
///
/// # Examples
///
/// ```
/// # use std::any::{Any, TypeId};
/// use barker::{register_handler, Message, MessageHandler};
/// # #[derive(Debug)] struct Ping;
/// # impl Message for Ping {
/// #     fn as_any(&self) -> &dyn Any { self }
/// #     fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// # }
/// struct Noop;
/// impl MessageHandler for Noop { fn call(&self, _: &dyn Message) {} }
///
/// register_handler(Box::new(Noop), Some(TypeId::of::<Ping>())).unwrap();
/// ```
pub fn register_handler(handler: Box<dyn MessageHandler>, msg_type: Option<TypeId>) -> Result<()> {
    MessageBus::global().register_handler(handler, msg_type)
}

/// Drain up to `limit` messages from the process-wide [`MessageBus::global`] and
/// dispatch them.
///
/// Convenience wrapper for `MessageBus::global().process_messages(limit)`. Pass `None`
/// to drain everything currently queued. See [`MessageBus::process_messages`] for full
/// semantics.
///
/// # Examples
///
/// ```
/// use barker::process_messages;
/// process_messages(None).unwrap();
/// ```
pub fn process_messages(limit: Option<usize>) -> Result<()> {
    MessageBus::global().process_messages(limit)
}
