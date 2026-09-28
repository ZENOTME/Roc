use asyncband::shutdown::{self, Shutdown, ShutdownWatch};
use futures::{StreamExt, stream::FuturesUnordered};

/// Runtime-independent cancellation backed by asyncband.
///
/// Clones share cancellation. A child observes its ancestors, but cancelling a
/// child does not cancel its parent or siblings. Dropping a handle does not cancel.
#[derive(Clone, Debug)]
pub struct Cancel {
    shutdown: Shutdown,
    watch: ShutdownWatch,
    ancestors: Vec<ShutdownWatch>,
}

impl Cancel {
    pub fn new() -> Self {
        let (shutdown, guard) = shutdown::new();
        Self {
            shutdown,
            watch: guard.into_watch(),
            ancestors: Vec::new(),
        }
    }

    /// Creates an independently cancellable child that also observes this handle.
    pub fn child(&self) -> Self {
        let mut child = Self::new();
        child.ancestors.clone_from(&self.ancestors);
        child.ancestors.push(self.watch.clone());
        child
    }

    pub fn cancel(&self) {
        self.shutdown.request_shutdown();
    }

    pub fn is_cancelled(&self) -> bool {
        self.watch.is_shutdown_requested()
            || self
                .ancestors
                .iter()
                .any(ShutdownWatch::is_shutdown_requested)
    }

    /// Waits for cancellation without requesting it or waiting for task completion.
    /// Dropping this future does not cancel the handle.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        if self.ancestors.is_empty() {
            self.watch.shutdown_requested().await;
        } else {
            let mut requests: FuturesUnordered<_> = std::iter::once(&self.watch)
                .chain(&self.ancestors)
                .map(ShutdownWatch::shutdown_requested)
                .collect();
            requests.next().await;
        }
    }
}

impl Default for Cancel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{executor::block_on, poll};
    use std::pin::pin;

    #[test]
    fn waiting_and_dropping_waiters_do_not_cancel() {
        block_on(async {
            let cancel = Cancel::new();
            {
                let mut waiting = pin!(cancel.cancelled());
                assert!(poll!(&mut waiting).is_pending());
            }
            assert!(!cancel.is_cancelled());
            drop(cancel.clone());
            assert!(!cancel.is_cancelled());
            let mut waiting = pin!(cancel.cancelled());
            assert!(poll!(&mut waiting).is_pending());
            cancel.clone().cancel();
            assert!(poll!(&mut waiting).is_ready());
            cancel.cancel();
            assert!(poll!(pin!(cancel.cancelled())).is_ready());
        });
    }

    #[test]
    fn child_cancellation_is_isolated_and_parent_cancellation_reaches_descendants() {
        block_on(async {
            let parent = Cancel::new();
            let child = parent.child();
            let sibling = parent.child();
            let grandchild = child.child();
            let mut waiting = pin!(grandchild.cancelled());
            assert!(poll!(&mut waiting).is_pending());
            child.cancel();
            assert!(poll!(&mut waiting).is_ready());
            assert!(grandchild.is_cancelled());
            assert!(!parent.is_cancelled());
            assert!(!sibling.is_cancelled());
            let mut sibling_wait = pin!(sibling.cancelled());
            assert!(poll!(&mut sibling_wait).is_pending());
            parent.cancel();
            assert!(poll!(&mut sibling_wait).is_ready());
            assert!(sibling.is_cancelled());
            let late_child = parent.child().child();
            assert!(poll!(pin!(late_child.cancelled())).is_ready());
        });
    }

    #[test]
    fn ancestor_request_wakes_all_descendant_waiters_without_a_runtime() {
        let parent = Cancel::new();
        let descendant = parent.child().child();
        let first = descendant.clone();
        let second = descendant.clone();
        block_on(async {
            let mut first_wait = pin!(first.cancelled());
            let mut second_wait = pin!(second.cancelled());
            assert!(poll!(&mut first_wait).is_pending());
            assert!(poll!(&mut second_wait).is_pending());
            std::thread::spawn(move || parent.cancel()).join().unwrap();
            futures::join!(first_wait, second_wait);
            assert!(descendant.is_cancelled());
        });
    }
}
