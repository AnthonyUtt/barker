//! The [`Message`] trait and the internal [`Envelope`] used by the bus.

use std::any::{Any, TypeId, type_name};
use std::fmt::Debug;
use std::time::{Duration, Instant};

/// Any value that can flow through a [`MessageBus`](crate::MessageBus).
///
/// Messages are erased into `Box<dyn Message>` at send time and may sit in a channel
/// across threads before being delivered, so the trait requires `'static + Send + Sync +
/// Debug`. Most implementors only need to provide [`as_any`](Message::as_any) and
/// [`as_any_mut`](Message::as_any_mut); the remaining methods carry sensible defaults.
///
/// Handlers see messages as `&dyn Message` and use [`as_any`](Message::as_any) plus
/// `downcast_ref` to recover the concrete type.
///
/// # Examples
///
/// Defining a message type and downcasting it inside a handler:
///
/// ```
/// use std::any::Any;
/// use barker::{Message, MessageHandler};
///
/// #[derive(Debug)]
/// struct Ping(&'static str);
///
/// impl Message for Ping {
///     fn as_any(&self) -> &dyn Any { self }
///     fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// }
///
/// struct Printer;
/// impl MessageHandler for Printer {
///     fn call(&self, msg: &dyn Message) {
///         if let Some(ping) = msg.as_any().downcast_ref::<Ping>() {
///             println!("ping: {}", ping.0);
///         }
///     }
/// }
/// ```
pub trait Message
where
    Self: 'static + Send + Sync + Debug,
{
    /// [`TypeId`] of the concrete type. Used by the bus to route typed handlers.
    ///
    /// Defaults to `TypeId::of::<Self>()`; you should not normally override this.
    fn message_type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    /// Human-readable type name, e.g. `"my_crate::Ping"`.
    ///
    /// Defaults to [`std::any::type_name`] of `Self`. Useful for logging.
    fn message_type_name(&self) -> &'static str {
        type_name::<Self>()
    }

    /// Priority hint, higher = more important. Defaults to `0`.
    ///
    /// **Aspirational metadata.** The bus reads this method but does *not* currently
    /// reorder dispatch based on it — handlers fire in registration order. The method is
    /// preserved on the trait for forward compatibility.
    fn priority(&self) -> u32 {
        0
    }

    /// Whether this message should be acknowledged. Defaults to `false`.
    ///
    /// **Aspirational metadata.** Declared for forward compatibility but not enforced by
    /// the current drain.
    fn requires_ack(&self) -> bool {
        false
    }

    /// Time-to-live for this message. Defaults to `None` (never expires).
    ///
    /// When `Some(duration)`, [`process_messages`](crate::MessageBus::process_messages)
    /// will skip delivery if the message has sat in the channel for longer than
    /// `duration`. Timing is measured from the [`Instant`] at which the envelope was
    /// enqueued.
    fn ttl(&self) -> Option<Duration> {
        None
    }

    /// Optional category string for logical grouping. Defaults to `None`.
    ///
    /// Categories are not used by the bus itself; they exist for downstream telemetry,
    /// filtering, or routing in user code.
    fn category(&self) -> Option<&str> {
        None
    }

    /// Returns `self` as `&dyn Any`, enabling `downcast_ref` inside handlers to recover
    /// the concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Mutable counterpart to [`as_any`](Message::as_any).
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// Internal wrapper that pairs a message with the instant it was enqueued.
// The timestamp is used by the drain to enforce TTL.
pub(crate) struct Envelope {
    pub(crate) message: Box<dyn Message>,
    pub(crate) timestamp: Instant,
}
