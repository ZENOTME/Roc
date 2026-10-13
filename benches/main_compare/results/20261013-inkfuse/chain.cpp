#include "algebra/TableScan.h"
#include "algebra/ExpressionOp.h"
#include "algebra/Print.h"
#include "exec/QueryExecutor.h"
#include "interpreter/FragmentCache.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
using namespace inkfuse;
using Clock=std::chrono::steady_clock;
double ms(Clock::time_point a,Clock::time_point b){return std::chrono::duration<double,std::milli>(b-a).count();}
std::unique_ptr<Print> query(StoredRelation& rel,int depth){
 auto scan=TableScan::build(rel,{"x"},"scan");
 if(depth==0){auto out=scan->getOutput();std::vector<RelAlgOpPtr> child;child.push_back(std::move(scan));return Print::build(std::move(child),out,{"result"},"sink");}
 std::vector<ExpressionOp::NodePtr> nodes;
 auto* current=nodes.emplace_back(std::make_unique<ExpressionOp::IURefNode>(scan->getOutput()[0])).get();
 for(int i=1;i<=depth;i++) current=nodes.emplace_back(std::make_unique<ExpressionOp::ComputeNode>(ExpressionOp::ComputeNode::Type::Add,IR::SI<8>::build(i),current)).get();
 std::vector<RelAlgOpPtr> children; children.push_back(std::move(scan));
 auto expr=ExpressionOp::build(std::move(children),"chain",{current},std::move(nodes));
 auto outputs=expr->getOutput(); children.clear(); children.push_back(std::move(expr));
 return Print::build(std::move(children),outputs,{"result"},"sink");
}
int main(int argc,char**argv){
 const size_t rows=4'000'000; const int reps=argc>1?std::stoi(argv[1]):15;
 auto start=Clock::now();FragmentCache::instance();std::cerr<<"fragment_startup_ms="<<ms(start,Clock::now())<<'\n';
 StoredRelation rel;auto& col=rel.attachPODColumn("x",IR::SignedInt::build(8));
 for(size_t i=0;i<rows;i++)col.loadValue("1",1);
 for(size_t i=0;i<rows;i++)reinterpret_cast<int64_t*>(col.getRawData())[i]=int64_t(i%1000)-500;
 std::ofstream out("/source/results-20261013/chain.csv");out<<"depth,mode,rep,prepare_ms,elapsed_ms,rows\n";
 using E=PipelineExecutor::ExecutionMode;
 for(int depth:{0,1,16,64})for(int rep=-1;rep<reps;rep++)for(int k=0;k<2;k++){
  bool fused=(k+rep+1)%2;auto mode=fused?E::Fused:E::Interpreted;
  auto root=query(rel,depth);auto* printer=root->printer.get();std::ostringstream values;
  if(rep==-1)printer->setOstream(values);
  auto cb=std::make_shared<PipelineExecutor::QueryControlBlock>(std::move(root));
  auto t0=Clock::now();auto exec=std::make_unique<QueryExecutor::StepwiseExecutor>(cb,mode,"chain_"+std::to_string(depth)+"_"+std::to_string(rep+1)+"_"+std::to_string(k),1);
  exec->prepareQuery();if(fused)exec->waitForCompilationForBenchmark();auto t1=Clock::now();
  exec->runQuery();exec.reset();auto t2=Clock::now();
  if(printer->num_rows!=rows)throw std::runtime_error("wrong row count");
  out<<depth<<','<<(fused?"fused":"interpreted")<<','<<rep<<','<<ms(t0,t1)<<','<<ms(t1,t2)<<','<<printer->num_rows<<'\n';out.flush();
  if(rep==-1){std::istringstream input(values.str());std::string header;std::getline(input,header);int64_t value;size_t i=0;
   while(input>>value){if(value!=int64_t(i%1000)-500+depth*(depth+1)/2)throw std::runtime_error("wrong value");i++;}
   if(i!=rows)throw std::runtime_error("validation row count");std::cerr<<"validated depth="<<depth<<" fused="<<fused<<" rows="<<i<<'\n';}
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
 }
}
