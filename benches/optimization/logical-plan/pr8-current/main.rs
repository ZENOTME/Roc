use std::{hint::black_box,sync::Arc,time::Instant};
use arrow::{array::{Array,ArrayRef,BooleanArray,Float64Array,Int64Array},datatypes::{DataType,Field,Schema},record_batch::RecordBatch};
use roc::{exec::{SinkExec,SourceExec},expr::{ExpressionResultType,agg::{AggregateExpression,AggregateFunction},scalar::{ReferenceExpression,ScalarExprRef}},operator::{AggregateOperator,Projection}};
const ROWS:usize=1<<20;
fn reference(index:usize,dtype:DataType,nullable:bool)->ScalarExprRef {ReferenceExpression::new(index,ExpressionResultType::new(dtype,nullable)).into_ref()}
fn aggregate(function:AggregateFunction,arg:Option<ScalarExprRef>,filtered:bool)->Arc<AggregateExpression>{
 let dtype=if function==AggregateFunction::Avg {DataType::Float64}else{DataType::Int64};
 #[cfg(feature="baseline")]
 let mut expression=AggregateExpression::new(function,arg.into_iter().collect(),dtype,function!=AggregateFunction::Count);
 #[cfg(not(feature="baseline"))]
 let mut expression=AggregateExpression::new(function,arg,dtype,function!=AggregateFunction::Count);
 if filtered {expression=expression.with_filter(reference(2,DataType::Boolean,false));}
 Arc::new(expression)
}
fn operator(schema:Arc<Schema>,case:&str)->AggregateOperator {
 let value=||Some(reference(1,DataType::Int64,true));
 let functions=match case {
  "count_star"=>vec![aggregate(AggregateFunction::Count,None,false)],
  "sum"=>vec![aggregate(AggregateFunction::Sum,value(),false)],
  "mixed"=>vec![aggregate(AggregateFunction::Sum,value(),false),aggregate(AggregateFunction::Count,value(),false),aggregate(AggregateFunction::Avg,value(),false)],
  "filtered_sum"=>vec![aggregate(AggregateFunction::Sum,value(),true)],
  _=>unreachable!()
 };
 AggregateOperator::try_new(Projection::from_indices(schema,&[0]).unwrap(),functions).unwrap()
}
fn run(operator:&AggregateOperator,batches:&[RecordBatch])->Vec<RecordBatch>{
 futures::executor::block_on(async {
  let (sink,source)=operator.clone().into_execs();
  let (_shutdown,guard)=asyncband::shutdown::new();
  let global=sink.init_global_context(&guard).unwrap();
  let mut executor=sink.new_executor(global.clone()).unwrap();
  for batch in batches {black_box(executor.sink(&guard,black_box(batch)).await.unwrap());}
  executor.combine(&guard).await.unwrap();sink.finalize(global,&guard).await.unwrap();
  let mut source=source.new_executor(source.init_global_context(&guard).unwrap()).unwrap();
  let mut output=vec![];while let Some(batch)=source.next_batch(&guard).await.unwrap(){output.push(batch)}
  output
 })
}
fn verify(output:&[RecordBatch],case:&str){
 let mut seen=vec![false;256];let mut sums=vec![0i64;256];let mut counts=vec![0i64;256];
 for i in 0..ROWS {if i%7!=0 && (case!="filtered_sum"||i%4!=0){sums[i%256]+=(i%97)as i64-48;counts[i%256]+=1;}}
 for batch in output {let keys=batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
  for row in 0..batch.num_rows(){let key=keys.value(row)as usize;assert!(!seen[key]);seen[key]=true;
   let v=batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
   if case=="count_star" {assert_eq!(v.value(row),(ROWS/256)as i64)}
   else if counts[key]==0 {assert!(v.is_null(row))}else{assert_eq!(v.value(row),sums[key])}
   if case=="mixed" {let count=batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();assert_eq!(count.value(row),counts[key]);let avg=batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();assert!((avg.value(row)-sums[key]as f64/counts[key]as f64).abs()<1e-12)}
  }
 }
 assert!(seen.into_iter().all(|x|x));
}
fn main(){
 let args=std::env::args().collect::<Vec<_>>();let process=&args[1];let label=&args[2];
 let key=Arc::new(Int64Array::from_iter_values((0..ROWS).map(|i|(i%256)as i64)))as ArrayRef;
 let value=Arc::new(Int64Array::from_iter((0..ROWS).map(|i|if i%7==0{None}else{Some((i%97)as i64-48)})))as ArrayRef;
 let filter=Arc::new(BooleanArray::from((0..ROWS).map(|i|i%4!=0).collect::<Vec<_>>()))as ArrayRef;
 println!("process,variant,case,batch_rows,columns,sample,loops,ns_per_run");
 let mut settings=vec![];for rows in [128,2048,8192]{for width in [3,32]{for case in ["count_star","sum","mixed","filtered_sum"]{settings.push((rows,width,case));}}}
 // Reverse case order in odd replicas to avoid always assigning one workload to one thermal phase.
 if process.parse::<usize>().unwrap()%2==1 {settings.reverse()}
 for (rows,width,case) in settings {
  let mut columns=vec![key.clone(),value.clone(),filter.clone()];columns.extend((3..width).map(|_|value.clone()));
  let schema=Arc::new(Schema::new(columns.iter().enumerate().map(|(i,a)|Field::new(format!("c{i}"),a.data_type().clone(),a.null_count()!=0)).collect::<Vec<_>>()));
  let batch=RecordBatch::try_new(schema.clone(),columns).unwrap();let batches=(0..ROWS).step_by(rows).map(|i|batch.slice(i,rows.min(ROWS-i))).collect::<Vec<_>>();
  let plan=operator(schema,case);verify(&run(&plan,&batches),case);
  let started=Instant::now();black_box(run(&plan,&batches));let one=started.elapsed().as_nanos()as f64;
  let loops=(20_000_000.0/one).ceil().clamp(1.0,64.0)as usize;
  for sample in 0..8 {
   let started=Instant::now();for _ in 0..loops{black_box(run(&plan,&batches));}
   println!("{process},{label},{case},{rows},{width},{sample},{loops},{:.3}",started.elapsed().as_nanos()as f64/loops as f64);
  }
 }
}
