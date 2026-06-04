//! The [`MessageBus`] struct and the process-wide global bus.

use std::any::TypeId;
use std::sync::{
    LazyLock, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use crate::drain;
use crate::error::{BarkerError, Result};
use crate::handler::{Handler, HandlerType, MessageHandler};
use crate::message::{Envelope, Message};

static GLOBAL_BUS: LazyLock<MessageBus> = LazyLock::new(MessageBus::new);

/// A buffered, synchronous event bus that routes [`Message`] values to registered
/// [`MessageHandler`]s.
///
/// # Semantics
///
/// - **Buffered send, explicit drain.** [`send`](MessageBus::send) enqueues the message
///   onto an internal channel and returns immediately. Handlers do **not** run until
///   someone calls [`process_messages`](MessageBus::process_messages).
/// - **Filtering.** Handlers are registered with either `Some(TypeId)` (typed — fires
///   only when the envelope's `message_type_id()` matches) or `None` (generic — fires
///   for every message).
/// - **TTL.** Messages whose [`ttl`](Message::ttl) has elapsed by the time they are
///   drained are silently skipped.
/// - **Dispatch order.** Handlers fire in the order they were registered. The
///   [`priority`](Message::priority) method is read off the trait but is **not** used
///   to reorder dispatch.
///
/// # Static global vs owned instance
///
/// Most users will reach for the free functions [`send`](crate::send),
/// [`register_handler`](crate::register_handler), and
/// [`process_messages`](crate::process_messages), which delegate to a process-wide bus
/// accessible via [`MessageBus::global`]. For tests, embeddable libraries, or applications
/// that want isolated buses, construct an instance with [`MessageBus::new`] or
/// [`MessageBus::bounded`] and call methods on it directly.
///
/// # Examples
///
/// ```
/// use std::any::{Any, TypeId};
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use barker::{Message, MessageHandler, MessageBus};
///
/// #[derive(Debug)] struct Tick;
/// impl Message for Tick {
///     fn as_any(&self) -> &dyn Any { self }
///     fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// }
///
/// struct Counter(Arc<AtomicUsize>);
/// impl MessageHandler for Counter {
///     fn call(&self, _: &dyn Message) { self.0.fetch_add(1, Ordering::SeqCst); }
/// }
///
/// let bus = MessageBus::new();
/// let count = Arc::new(AtomicUsize::new(0));
/// bus.register_handler(
///     Box::new(Counter(count.clone())),
///     Some(TypeId::of::<Tick>()),
/// ).unwrap();
///
/// bus.send(Tick).unwrap();
/// bus.send(Tick).unwrap();
/// bus.process_messages(None).unwrap();
///
/// assert_eq!(count.load(Ordering::SeqCst), 2);
/// ```
pub struct MessageBus {
    sender: flume::Sender<Envelope>,
    receiver: flume::Receiver<Envelope>,
    handlers: RwLock<Vec<Handler>>,
    next_message_id: AtomicU64,
}

impl MessageBus {
    /// Construct a new bus backed by an **unbounded** [`flume`] channel.
    ///
    /// This is the right choice unless you specifically want to apply back-pressure on
    /// producers; in that case, use [`MessageBus::bounded`].
    ///
    /// # Examples
    ///
    /// ```
    /// use barker::MessageBus;
    /// let bus = MessageBus::new();
    /// ```
    pub fn new() -> Self {
        let (sender, receiver) = flume::unbounded();
        Self {
            sender,
            receiver,
            handlers: RwLock::new(Vec::new()),
            next_message_id: AtomicU64::new(0),
        }
    }

    /// Construct a new bus backed by a **bounded** [`flume`] channel of the given
    /// capacity.
    ///
    /// Once `capacity` messages are queued, further [`send`](MessageBus::send) calls
    /// will fail until [`process_messages`](MessageBus::process_messages) drains some
    /// of the backlog. Use this when producers must not outrun consumers.
    ///
    /// # Examples
    ///
    /// ```
    /// use barker::MessageBus;
    /// let bus = MessageBus::bounded(64);
    /// ```
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = flume::bounded(capacity);
        Self {
            sender,
            receiver,
            handlers: RwLock::new(Vec::new()),
            next_message_id: AtomicU64::new(0),
        }
    }

    /// Borrow the process-wide global [`MessageBus`].
    ///
    /// The global bus is lazily initialised on first access via
    /// [`std::sync::LazyLock`]. Calls to the free-function wrappers
    /// [`send`](crate::send), [`register_handler`](crate::register_handler), and
    /// [`process_messages`](crate::process_messages) operate on this same instance.
    ///
    /// Prefer the global bus for application-wide event flows; construct your own with
    /// [`MessageBus::new`] when you need isolation (tests, plugins with private event
    /// streams, etc.).
    ///
    /// # Examples
    ///
    /// ```
    /// use barker::MessageBus;
    /// let bus: &MessageBus = MessageBus::global();
    /// ```
    pub fn global() -> &'static MessageBus {
        &GLOBAL_BUS
    }

    /// Enqueue a message onto the bus, returning a monotonically increasing message ID.
    ///
    /// The message is wrapped in an internal envelope tagged with the current
    /// [`Instant`], which the drain uses to enforce TTL. The handler is **not** invoked
    /// until a subsequent call to [`process_messages`](MessageBus::process_messages).
    ///
    /// # Errors
    ///
    /// Returns [`BarkerError::Send`] if the underlying channel is disconnected. This
    /// cannot happen on a live `MessageBus` — the bus owns both ends of the channel —
    /// but the error variant exists for completeness.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::any::Any;
    /// use barker::{Message, MessageBus};
    /// # #[derive(Debug)] struct Ping;
    /// # impl Message for Ping {
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// #     fn as_any_mut(&mut self) -> &mut dyn Any { self }
    /// # }
    /// let bus = MessageBus::new();
    /// let id = bus.send(Ping).expect("send");
    /// assert_eq!(id, 0);
    /// ```
    pub fn send<M: Message>(&self, message: M) -> Result<u64> {
        let id = self.next_message_id.fetch_add(1, Ordering::SeqCst);

        let envelope = Envelope {
            message: Box::new(message),
            timestamp: Instant::now(),
        };

        self.sender.send(envelope).map_err(|_| BarkerError::Send)?;

        Ok(id)
    }

    /// Register a [`MessageHandler`] with an optional type filter.
    ///
    /// - `msg_type = Some(TypeId::of::<T>())` registers a **typed** handler that fires
    ///   only when the envelope's `message_type_id()` equals the given TypeId.
    /// - `msg_type = None` registers a **generic** handler that fires for every message.
    ///
    /// Handlers are appended to the registry in registration order; the drain dispatches
    /// in that same order. There is currently no API to unregister handlers — once
    /// added, they live for the lifetime of the bus.
    ///
    /// # Errors
    ///
    /// Returns [`BarkerError::HandlerLockPoisoned`] if the registry lock has been
    /// poisoned by a previous panic.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::any::{Any, TypeId};
    /// use barker::{Message, MessageHandler, MessageBus};
    /// # #[derive(Debug)] struct Ping;
    /// # impl Message for Ping {
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// #     fn as_any_mut(&mut self) -> &mut dyn Any { self }
    /// # }
    /// struct Logger;
    /// impl MessageHandler for Logger {
    ///     fn call(&self, msg: &dyn Message) {
    ///         println!("got {}", msg.message_type_name());
    ///     }
    /// }
    ///
    /// let bus = MessageBus::new();
    /// bus.register_handler(Box::new(Logger), Some(TypeId::of::<Ping>())).unwrap();
    /// ```
    pub fn register_handler(
        &self,
        handler: Box<dyn MessageHandler>,
        msg_type: Option<TypeId>,
    ) -> Result<()> {
        let type_id = match msg_type {
            Some(t) => HandlerType::Typed(t),
            None => HandlerType::Generic,
        };

        let mut handlers = self
            .handlers
            .write()
            .map_err(|_| BarkerError::HandlerLockPoisoned)?;

        handlers.push(Handler {
            inner: handler,
            type_id,
        });
        Ok(())
    }

    /// Drain pending messages and dispatch each to every matching handler.
    ///
    /// - `limit = Some(n)` stops after draining at most `n` messages; the remainder stay
    ///   on the channel for the next call.
    /// - `limit = None` drains every message currently queued.
    ///
    /// Messages whose [`ttl`](Message::ttl) has elapsed by the time they are drained are
    /// silently skipped. Handlers fire in registration order; the
    /// [`priority`](Message::priority) method is **not** consulted.
    ///
    /// This call is non-blocking — it drains what is currently queued and returns. It
    /// never waits for new messages to arrive.
    ///
    /// # Errors
    ///
    /// Returns [`BarkerError::HandlerLockPoisoned`] if the registry lock has been
    /// poisoned by a previous panic.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::any::Any;
    /// use barker::{Message, MessageBus};
    /// # #[derive(Debug)] struct Ping;
    /// # impl Message for Ping {
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// #     fn as_any_mut(&mut self) -> &mut dyn Any { self }
    /// # }
    /// let bus = MessageBus::new();
    /// bus.send(Ping).unwrap();
    /// bus.process_messages(None).unwrap();
    /// ```
    pub fn process_messages(&self, limit: Option<usize>) -> Result<()> {
        drain::process(&self.receiver, &self.handlers, limit)
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}
