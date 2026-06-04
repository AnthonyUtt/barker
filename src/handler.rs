//! The [`MessageHandler`] trait plus the internal registry types.

use std::any::TypeId;

use crate::message::Message;

/// A subscriber that reacts to messages drained from a
/// [`MessageBus`](crate::MessageBus).
///
/// Handlers receive `&dyn Message`. They are usually registered with a type filter via
/// [`MessageBus::register_handler`](crate::MessageBus::register_handler) — passing
/// `Some(TypeId::of::<T>())` restricts the handler to messages of type `T`, while passing
/// `None` registers a generic handler that fires for every message.
///
/// Inside [`call`](MessageHandler::call), use `msg.as_any().downcast_ref::<T>()` to
/// access the concrete message.
///
/// Implementations must be `Send + Sync` because the bus shares its handler registry
/// across threads; `call` itself takes `&self`, so handlers cannot mutate their own state
/// directly — wrap mutable state in an [`Arc<Mutex<_>>`](std::sync::Mutex),
/// [`AtomicUsize`](std::sync::atomic::AtomicUsize), or similar.
///
/// # Examples
///
/// A handler that counts incoming `Ping` messages:
///
/// ```
/// use std::any::Any;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use barker::{Message, MessageHandler};
///
/// #[derive(Debug)]
/// struct Ping;
/// impl Message for Ping {
///     fn as_any(&self) -> &dyn Any { self }
///     fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// }
///
/// struct Counter(Arc<AtomicUsize>);
/// impl MessageHandler for Counter {
///     fn call(&self, msg: &dyn Message) {
///         if msg.as_any().downcast_ref::<Ping>().is_some() {
///             self.0.fetch_add(1, Ordering::SeqCst);
///         }
///     }
/// }
/// ```
pub trait MessageHandler: Send + Sync {
    /// Invoked by the bus for every message that matches this handler's type filter.
    ///
    /// `msg` is borrowed for the duration of the call only; if the handler needs to
    /// retain data, it must clone or copy what it needs.
    fn call(&self, msg: &dyn Message);
}

// Internal registry entry: a handler plus its routing filter.
pub(crate) struct Handler {
    pub(crate) inner: Box<dyn MessageHandler>,
    pub(crate) type_id: HandlerType,
}

// Routing filter for a registered handler.
pub(crate) enum HandlerType {
    // Fires for every message.
    Generic,
    // Fires only for messages whose `message_type_id()` equals this TypeId.
    Typed(TypeId),
}
