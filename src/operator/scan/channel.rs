// Copyright 2026 The Roc Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::record_batch::RecordBatch;

/// The fixed data-plane boundary between a custom storage implementation and
/// the execution engine. Storage-specific descriptors remain generic; decoded
/// output does not.
pub struct ScanSender<E> {
    inner: asyncband::mpmc::BoundedSender<Result<RecordBatch, E>>,
}

pub struct ScanReceiver<E> {
    inner: asyncband::mpmc::BoundedReceiver<Result<RecordBatch, E>>,
}

#[derive(Debug)]
pub struct ScanSendError<E>(pub Result<RecordBatch, E>);

pub fn scan_channel<E>(capacity: usize) -> (ScanSender<E>, ScanReceiver<E>) {
    let (sender, receiver) = asyncband::mpmc::bounded(capacity);
    (
        ScanSender { inner: sender },
        ScanReceiver { inner: receiver },
    )
}

impl<E> ScanSender<E> {
    pub async fn send(&self, item: Result<RecordBatch, E>) -> Result<(), ScanSendError<E>> {
        self.inner
            .send(item)
            .await
            .map_err(|error| ScanSendError(error.into_inner()))
    }
}

impl<E> Clone for ScanSender<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<E> ScanReceiver<E> {
    /// Drains buffered items after all senders are dropped, then returns `None`.
    pub async fn recv(&self) -> Option<Result<RecordBatch, E>> {
        self.inner.recv().await.ok()
    }
}

impl<E> Clone for ScanReceiver<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn receiver_is_a_shared_work_queue() {
        futures::executor::block_on(async {
            let (sender, first) = scan_channel::<()>(1);
            let second = first.clone();
            sender
                .send(Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))))
                .await
                .unwrap();
            drop(sender);
            assert!(first.recv().await.is_some());
            assert!(second.recv().await.is_none());
        });
    }

    #[test]
    fn full_queue_applies_backpressure_and_drains_before_disconnect() {
        futures::executor::block_on(async {
            let (sender, receiver) = scan_channel::<u32>(1);
            let last_sender = sender.clone();
            sender.send(Err(1)).await.unwrap();
            let mut pending = Box::pin(sender.send(Err(2)));
            assert!(futures::poll!(&mut pending).is_pending());
            assert!(matches!(receiver.recv().await, Some(Err(1))));
            pending.await.unwrap();
            drop(sender);
            assert!(matches!(receiver.recv().await, Some(Err(2))));
            let mut pending = Box::pin(receiver.recv());
            assert!(futures::poll!(&mut pending).is_pending());
            drop(last_sender);
            assert!(matches!(
                futures::poll!(&mut pending),
                std::task::Poll::Ready(None)
            ));
        });
    }

    #[test]
    fn dropping_last_receiver_wakes_sender_and_returns_unsent_item() {
        futures::executor::block_on(async {
            let (sender, receiver) = scan_channel::<u32>(1);
            let last_receiver = receiver.clone();
            sender.send(Err(1)).await.unwrap();
            let mut pending = Box::pin(sender.send(Err(2)));
            assert!(futures::poll!(&mut pending).is_pending());
            drop(receiver);
            assert!(futures::poll!(&mut pending).is_pending());
            drop(last_receiver);
            assert!(matches!(
                futures::poll!(&mut pending),
                std::task::Poll::Ready(Err(ScanSendError(Err(2))))
            ));
            assert!(matches!(
                sender.send(Err(3)).await,
                Err(ScanSendError(Err(3)))
            ));
        });
    }

    #[test]
    fn dropping_pending_receive_does_not_consume_an_item() {
        futures::executor::block_on(async {
            let (sender, first) = scan_channel::<u32>(1);
            let second = first.clone();
            let mut pending = Box::pin(first.recv());
            assert!(futures::poll!(&mut pending).is_pending());
            drop(pending);
            sender.send(Err(7)).await.unwrap();
            assert!(matches!(second.recv().await, Some(Err(7))));
            drop(sender);
            assert!(first.recv().await.is_none());
        });
    }
}
