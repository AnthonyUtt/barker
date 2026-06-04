//! Error types returned by bus operations.

use thiserror::Error;

/// Errors returned by [`MessageBus`](crate::MessageBus) operations.
///
/// Both variants describe failure modes that should be rare in well-behaved programs:
/// channel disconnection cannot happen on a live `MessageBus` (the bus owns both ends of
/// the channel), and lock poisoning only occurs when another thread panics while holding
/// the handler-registry lock.
#[derive(Debug, Error)]
pub enum BarkerError {
    /// Returned by [`MessageBus::send`](crate::MessageBus::send) when the underlying
    /// channel reports a disconnect. Not expected to occur in practice — the bus owns
    /// both sender and receiver, so the channel cannot disconnect while the bus is alive.
    #[error("failed to enqueue message: bus channel disconnected")]
    Send,

    /// Returned by [`register_handler`](crate::MessageBus::register_handler) or
    /// [`process_messages`](crate::MessageBus::process_messages) when the handler
    /// registry's `RwLock` has been poisoned by a previous panic. Indicates a programming
    /// bug — a panic inside a handler `call` or while holding the registry lock.
    #[error("handler registry lock poisoned")]
    HandlerLockPoisoned,
}

/// Convenience alias for [`std::result::Result`] specialised to [`BarkerError`].
pub type Result<T> = std::result::Result<T, BarkerError>;
