use arrow::{array::{ArrayRef, Int64Array}, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch};
use asyncband::shutdown::ShutdownGuard;
use futures::{future::BoxFuture, executor::block_on};
use roc::{error::Result, exec::{ScanExec, SourceExec}, operator::{ScanConsumer, ScanHandle, ScanOperator, ScanReceiver, ScanRequest, ScanStorage, scan_channel}};
use std::{hint::black_box, sync::Arc, task::Poll, time::Instant};

struct Storage { batch: RecordBatch, pending: bool, receiver: Option<ScanReceiver<roc::error::Error>> }
struct Handle { batch: RecordBatch, pending: bool, receiver: Option<ScanReceiver<roc::error::Error>> }
struct Consumer { batch: RecordBatch, pending: bool, receiver: Option<ScanReceiver<roc::error::Error>> }
impl ScanStorage for Storage {
    type StorageTaskDesc = ();
    fn start_scan(&self, _: ScanRequest<()>) -> Result<Arc<dyn ScanHandle>> {
        Ok(Arc::new(Handle { batch: self.batch.clone(), pending: self.pending, receiver: self.receiver.clone() }))
    }
}
impl ScanHandle for Handle {
    fn consumer(&self) -> Box<dyn ScanConsumer> {
        Box::new(Consumer { batch: self.batch.clone(), pending: self.pending, receiver: self.receiver.clone() })
    }
    fn finish(&self) -> BoxFuture<'_, Result<()>> { Box::pin(async { Ok(()) }) }
}
impl ScanConsumer for Consumer {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if let Some(receiver) = &self.receiver {
                return receiver.recv().await.transpose();
            }
            let mut first = self.pending;
            futures::future::poll_fn(|cx| {
                if first {
                    first = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(Some(self.batch.clone())))
                }
            }).await
        })
    }
}

fn sample(mode: &str, count: usize) -> f64 {
    let array = Arc::new(Int64Array::from_iter_values(0..2048));
    let original: ArrayRef = array.clone();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])), vec![array.clone()]).unwrap();
    let receiver = if mode == "ready_channel" {
        let (sender, receiver) = scan_channel(count);
        block_on(async { for _ in 0..count { sender.send(Ok(batch.clone())).await.unwrap(); } });
        drop(sender);
        Some(receiver)
    } else { None };
    let storage = Arc::new(Storage { batch, pending: mode == "pending_once", receiver });
    let scan = ScanExec::new(ScanOperator::new((), storage));
    let (_shutdown, guard): (_, ShutdownGuard) = asyncband::shutdown::new();
    let global = scan.init_global_context(&guard).unwrap();
    let mut exec = scan.new_executor(global.clone()).unwrap();
    let start = Instant::now();
    block_on(async {
        for _ in 0..count {
            let batch = black_box(exec.next_batch(black_box(&guard)).await.unwrap().unwrap());
            assert_eq!(batch.num_rows(), 2048);
            assert!(Arc::ptr_eq(batch.column(0), &original));
            if mode == "ready_sum" {
                let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(black_box(arrow::compute::sum(black_box(values))), Some(2_096_128));
            }
            black_box(batch);
        }
    });
    let ns = start.elapsed().as_nanos() as f64 / count as f64;
    drop(exec);
    block_on(scan.finalize(global, &guard)).unwrap();
    ns
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let reverse = args.get(1).is_some_and(|v| v == "reverse");
    let mut modes = vec!["ready", "ready_channel", "ready_sum", "pending_once"];
    if reverse { modes.reverse(); }
    println!("mode,sample,ns_per_batch");
    for mode in modes {
        sample(mode, 3000);
        for i in 0..9 {
            println!("{mode},{i},{:.3}", sample(mode, 100_000));
        }
    }
}
