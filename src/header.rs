use std::alloc::Layout;
use std::cell::Cell;
use std::fmt;
use std::task::Waker;

use crate::raw::TaskVTable;
use crate::state::*;
use crate::utils::{abort_on_panic, extend};

/// The header of a task.
///
/// This header is stored right at the beginning of every heap-allocated task.
pub(crate) struct Header {
    /// Current state of the task.
    ///
    /// Contains flags representing the current state and the reference count.
    pub(crate) state: usize,

    /// The task that is blocked on the `JoinHandle`.
    ///
    /// This waker needs to be woken once the task completes or is closed.
    pub(crate) awaiter: Cell<Option<Waker>>,

    /// The virtual table.
    ///
    /// In addition to the actual waker virtual table, it also contains pointers to several other
    /// methods necessary for bookkeeping the heap-allocated task.
    pub(crate) vtable: &'static TaskVTable,
}

impl Header {
    /// Cancels the task.
    ///
    /// This method will only mark the task as closed and will notify the awaiter, but it won't
    /// reschedule the task if it's not completed.
    pub(crate) fn cancel(&self) {
        unsafe {
            let state = &mut (*(self as *const Self as *mut Self)).state;

            // If the task has been completed or closed, it can't be cancelled.
            if *state & (COMPLETED | CLOSED) != 0 {
                return;
            }

            // Mark the task as closed.
            *state = *state | CLOSED;
            // Notify the awaiter that the task has been closed.
            if *state & AWAITER != 0 {
                self.notify();
            }
        }
    }

    /// Notifies the task blocked on the task.
    ///
    /// If there is a registered waker, it will be removed from the header and woken.
    #[inline]
    pub(crate) fn notify(&self) {
        if let Some(waker) = self.swap_awaiter(None) {
            // We need a safeguard against panics because waking can panic.
            abort_on_panic(|| {
                waker.wake();
            });
        }
    }

    /// Notifies the task blocked on the task unless its waker matches `current`.
    ///
    /// If there is a registered waker, it will be removed from the header.
    #[inline]
    pub(crate) fn notify_unless(&self, current: &Waker) {
        if let Some(waker) = self.swap_awaiter(None) {
            if !waker.will_wake(current) {
                // We need a safeguard against panics because waking can panic.
                abort_on_panic(|| {
                    waker.wake();
                });
            }
        }
    }

    /// Swaps the awaiter and returns the previous value.
    #[inline]
    pub(crate) fn swap_awaiter(&self, new: Option<Waker>) -> Option<Waker> {
        let new_is_none = new.is_none();
        unsafe {
            let state = &mut (*(self as *const Self as *mut Self)).state;

            // Replace the awaiter.
            let old = self.awaiter.replace(new);

            if new_is_none {
                *state &= !AWAITER;
            } else {
                *state |= AWAITER;
            }

            old
        }
    }

    /// Returns the offset at which the tag of type `T` is stored.
    #[inline]
    pub(crate) fn offset_tag<T>() -> usize {
        let layout_header = Layout::new::<Header>();
        let layout_t = Layout::new::<T>();
        let (_, offset_t) = extend(layout_header, layout_t);
        offset_t
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state;

        f.debug_struct("Header")
            .field("scheduled", &(state & SCHEDULED != 0))
            .field("running", &(state & RUNNING != 0))
            .field("completed", &(state & COMPLETED != 0))
            .field("closed", &(state & CLOSED != 0))
            .field("awaiter", &(state & AWAITER != 0))
            .field("handle", &(state & HANDLE != 0))
            .field("ref_count", &(state / REFERENCE))
            .finish()
    }
}
