use arrow::record_batch::RecordBatch;

/// The fixed data-plane boundary between a custom storage implementation and
/// the execution engine. Storage-specific descriptors remain generic; decoded
/// output does not.
pub struct ScanSender<E> {
    inner: async_channel::Sender<Result<RecordBatch, E>>,
}

pub struct ScanReceiver<E> {
    inner: async_channel::Receiver<Result<RecordBatch, E>>,
}

#[derive(Debug)]
pub struct ScanSendError<E>(pub Result<RecordBatch, E>);

pub fn scan_channel<E>(capacity: usize) -> (ScanSender<E>, ScanReceiver<E>) {
    let (sender, receiver) = async_channel::bounded(capacity);
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
            .map_err(|error| ScanSendError(error.0))
    }

    pub fn close(&self) -> bool {
        self.inner.close()
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
    /// Returns `None` when every producer has closed its side of the channel.
    pub async fn recv(&self) -> Option<Result<RecordBatch, E>> {
        self.inner.recv().await.ok()
    }

    pub fn close(&self) -> bool {
        self.inner.close()
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
}
