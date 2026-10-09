#![allow(dead_code)]
mod error;
mod aggregate_function;
mod expr { pub mod agg { pub use crate::aggregate_function::AggregateFunction; } }
#[path = "generated/enum_dynamic.rs"] mod enum_dynamic;
#[path = "generated/enum_static.rs"] mod enum_static;
#[path = "generated/bound_dynamic.rs"] mod bound_dynamic;
#[path = "generated/bound_static.rs"] mod bound_static;
#[path = "generated/before.rs"] mod before;
#[path = "generated/after.rs"] mod after;
use arrow::{array::{Array, ArrayRef, Int64Array, UInt64Array, Float64Array, StringArray}, datatypes::DataType};
use aggregate_function::AggregateFunction as F;
use std::{hint::black_box, sync::Arc, time::Instant, collections::HashSet};
// All variants use the same hash seed within each process; construction is
// excluded from timing. Preserve RandomState's production hashing algorithm.
fn new_group_set() -> HashSet<Vec<u8>> {
    static SEED: std::sync::OnceLock<std::collections::hash_map::RandomState> = std::sync::OnceLock::new();
    HashSet::with_hasher(SEED.get_or_init(std::collections::hash_map::RandomState::new).clone())
}
const NAMES: [&str;6] = ["enum_dynamic","enum_static","bound_dynamic","bound_static","before","after"];
const CASES: [&str;13] = ["count_star","count_i64","count_distinct_i64","sum_i64","sum_u64","sum_f64","avg_i64","covar_i64","min_i64","max_i64","min_utf8","max_utf8","mixed_7"];
type Runner = Box<dyn FnMut(usize) -> (f64, Vec<ArrayRef>)>;
#[derive(Clone)] struct Fixture { x: ArrayRef, y: ArrayRef, u: ArrayRef, f: ArrayRef, s: ArrayRef, ids: Arc<[usize]> }
fn fixture(rows: usize, nullable: bool) -> Fixture {
    let value = |i: usize| ((i*73+17)%4096) as i64-2048;
    let valid = |i: usize| !nullable || i%17!=0;
    let x = Arc::new(Int64Array::from_iter((0..rows).map(|i| valid(i).then(||value(i))))) as ArrayRef;
    let y = Arc::new(Int64Array::from_iter((0..rows).map(|i| (!nullable || i%23!=0).then(|| ((i*37+31)%997) as i64-498)))) as ArrayRef;
    let u = Arc::new(UInt64Array::from_iter((0..rows).map(|i|valid(i).then(||(value(i)+2048) as u64)))) as ArrayRef;
    let f = Arc::new(Float64Array::from_iter((0..rows).map(|i|valid(i).then(||value(i) as f64)))) as ArrayRef;
    let s = Arc::new(StringArray::from_iter((0..rows).map(|i|valid(i).then(||format!("{:05}",value(i)+2048))))) as ArrayRef;
    Fixture { x,y,u,f,s,ids:(0..rows).map(|i|i%256).collect::<Vec<_>>().into() }
}
fn specs(case:usize,input:&Fixture) -> Vec<(F,bool,Vec<ArrayRef>,DataType)> {
    match case {
        0=>vec![(F::Count,false,vec![],DataType::Int64)],
        1|2=>vec![(F::Count,case==2,vec![input.x.clone()],DataType::Int64)],
        3=>vec![(F::Sum,false,vec![input.x.clone()],DataType::Int64)],
        4=>vec![(F::Sum,false,vec![input.u.clone()],DataType::UInt64)],
        5=>vec![(F::Sum,false,vec![input.f.clone()],DataType::Float64)],
        6=>vec![(F::Avg,false,vec![input.x.clone()],DataType::Float64)],
        7=>vec![(F::CovarPop,false,vec![input.x.clone(),input.y.clone()],DataType::Float64)],
        8|9=>vec![(if case==8 {F::Min}else{F::Max},false,vec![input.x.clone()],DataType::Int64)],
        10|11=>vec![(if case==10 {F::Min}else{F::Max},false,vec![input.s.clone()],DataType::Utf8)],
        _=>[1,2,3,6,7,8,9].into_iter().flat_map(|i|specs(i,input)).collect(),
    }
}
macro_rules! prepare {
    ($name:ident, $module:ident) => {
        fn $name(case:usize,input:&Fixture) -> Runner {
            let mut states = specs(case,input).into_iter().map(|(function,distinct,values,output)| {
                let types=values.iter().map(|v|v.data_type().clone()).collect::<Vec<_>>();
                let mut acc=$module::Accumulator::new(black_box(function),distinct,&types,&output).unwrap();
                acc.resize(257);
                (acc,values)
            }).collect::<Vec<_>>();
            let ids=input.ids.clone();
            Box::new(move |iterations| {
                let start=Instant::now();
                for _ in 0..iterations {
                    for (acc,values) in &mut states {
                        black_box(acc).update(black_box(values),black_box(&ids)).unwrap();
                    }
                }
                let us=start.elapsed().as_secs_f64()*1e6/iterations as f64;
                let output=states.iter().map(|(acc,_)|acc.evaluate().unwrap()).collect();
                (us,output)
            })
        }
    }
}
prepare!(prepare_enum_dynamic,enum_dynamic); prepare!(prepare_enum_static,enum_static);
prepare!(prepare_bound_dynamic,bound_dynamic); prepare!(prepare_bound_static,bound_static);
prepare!(prepare_before,before); prepare!(prepare_after,after);
fn equal(a:&[ArrayRef],b:&[ArrayRef]) { assert_eq!(a.len(),b.len()); for (a,b) in a.iter().zip(b) { assert_eq!(a.to_data(),b.to_data()); } }
fn reference(case:usize,input:&Fixture) -> ArrayRef {
    let x=input.x.as_any().downcast_ref::<Int64Array>().unwrap();
    let y=input.y.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut count=vec![0i64;257]; let mut sum=vec![0i64;257]; let mut usum=vec![0u64;257];
    let mut min=vec![None::<i64>;257]; let mut max=min.clone(); let mut seen=vec![HashSet::new();257];
    let mut pairs=vec![Vec::<(f64,f64)>::new();257];
    for (i,&g) in input.ids.iter().enumerate() {
        if case==0 {count[g]+=1; continue;}
        if x.is_valid(i) {
            count[g]+=1; sum[g]+=x.value(i); usum[g]+=(x.value(i)+2048) as u64;
            min[g]=Some(min[g].map_or(x.value(i),|v|v.min(x.value(i))));
            max[g]=Some(max[g].map_or(x.value(i),|v|v.max(x.value(i)))); seen[g].insert(x.value(i));
            if y.is_valid(i) {pairs[g].push((x.value(i) as f64,y.value(i) as f64));}
        }
    }
    match case {
        0|1=>Arc::new(Int64Array::from(count)),
        2=>Arc::new(Int64Array::from(seen.iter().map(|s|s.len() as i64).collect::<Vec<_>>())),
        3=>Arc::new(Int64Array::from((0..257).map(|g|(count[g]!=0).then_some(sum[g])).collect::<Vec<_>>())),
        4=>Arc::new(UInt64Array::from((0..257).map(|g|(count[g]!=0).then_some(usum[g])).collect::<Vec<_>>())),
        5=>Arc::new(Float64Array::from((0..257).map(|g|(count[g]!=0).then_some(sum[g] as f64)).collect::<Vec<_>>())),
        6=>Arc::new(Float64Array::from((0..257).map(|g|(count[g]!=0).then(||sum[g] as f64/count[g] as f64)).collect::<Vec<_>>())),
        7=>Arc::new(Float64Array::from(pairs.iter().map(|p| {
            if p.is_empty() {return None;}
            let n=p.len() as f64; let mx=p.iter().map(|v|v.0).sum::<f64>()/n; let my=p.iter().map(|v|v.1).sum::<f64>()/n;
            Some(p.iter().map(|v|(v.0-mx)*(v.1-my)).sum::<f64>()/n)
        }).collect::<Vec<_>>())),
        8=>Arc::new(Int64Array::from(min)), 9=>Arc::new(Int64Array::from(max)),
        10|11=>Arc::new(StringArray::from_iter((if case==10 {min}else{max}).iter().map(|v|v.map(|v|format!("{:05}",v+2048))))),
        _=>unreachable!(),
    }
}
fn validate_reference(case:usize,input:&Fixture,result:&[ArrayRef]) {
    let cases=if case==12 {vec![1,2,3,6,7,8,9]}else{vec![case]};
    for (case,out) in cases.into_iter().zip(result) {
        let expected=reference(case,input);
        if case==7 {
            let out=out.as_any().downcast_ref::<Float64Array>().unwrap();let expected=expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for (actual,expected) in out.iter().zip(expected) { match (actual,expected) {(Some(a),Some(e))=>assert!((a-e).abs()<1e-7+e.abs()*1e-12), (None,None)=>{},_=>panic!("covariance validity mismatch")}}
        } else {assert_eq!(out.to_data(),expected.to_data());}
    }
}
fn main() {
    let process=std::env::args().nth(1).unwrap_or("0".into()).parse::<usize>().unwrap();
    let mut cases=(0..3).flat_map(|r|(0..2).flat_map(move |n|(0..13).map(move|c|(r,n,c)))).collect::<Vec<_>>();
    let mut seed=0x517cc1b727220a95u64^(process as u64+1);
    for i in (1..cases.len()).rev(){seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;cases.swap(i,seed as usize%(i+1));}
    println!("process,rows,nullable,case,sample,iterations,position,variant,us_per_batch");
    for (r,n,case) in cases {
        let rows=[128,8192,65536][r];let nullable=n!=0;let input=fixture(rows,nullable);
        let makers:[fn(usize,&Fixture)->Runner;6]=[prepare_enum_dynamic,prepare_enum_static,prepare_bound_dynamic,prepare_bound_static,prepare_before,prepare_after];
        let mut runners=makers.map(|p|p(case,&input));let mut reference_result:Option<Vec<ArrayRef>>=None;
        for runner in &mut runners {
            let (_,out)=runner(1);validate_reference(case,&input,&out);
            if let Some(ref expected)=reference_result {equal(&out,expected);}else{reference_result=Some(out);}
            black_box(runner(32));
        }
        let mut fastest=f64::INFINITY;
        for runner in &mut runners {fastest=fastest.min(runner(16).0);}
        let iterations=(15000.0/fastest).ceil().clamp(8.0,50000.0) as usize;
        for sample in 0..12 {
            let offset=(sample+process)%6;let mut outputs:Vec<Option<Vec<ArrayRef>>>=(0..6).map(|_|None).collect();
            for position in 0..6 {
                let variant=(offset+position)%6;let (us,out)=runners[variant](iterations);outputs[variant]=Some(out);
                println!("{process},{rows},{nullable},{},{sample},{iterations},{position},{},{us:.6}",CASES[case],NAMES[variant]);
            }
            for out in &outputs[1..] {equal(outputs[0].as_ref().unwrap(),out.as_ref().unwrap());}
        }
        eprintln!("process {process}: {rows} rows nullable={nullable} {} passed",CASES[case]);
    }
}
