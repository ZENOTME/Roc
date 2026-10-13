#include "common/Helpers.h"
#include "common/TPCH.h"
#include "exec/QueryExecutor.h"
#include "interpreter/FragmentCache.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <filesystem>
using namespace inkfuse;
using Clock = std::chrono::steady_clock;
double ms(Clock::time_point a, Clock::time_point b) { return std::chrono::duration<double,std::milli>(b-a).count(); }
int main(int argc, char** argv) {
  const int threads = argc>1 ? std::stoi(argv[1]) : 1;
  const int reps = argc>2 ? std::stoi(argv[2]) : 7;
  const std::string sf = argc>3 ? argv[3] : "1";
  const std::string only = argc>4 ? argv[4] : "all";
  auto start=Clock::now(); FragmentCache::instance();
  std::cerr << "fragment_startup_ms=" << ms(start,Clock::now()) << '\n';
  for (auto& f:std::filesystem::directory_iterator("/tmp")) if(f.path().extension()==".c" && f.path().filename().string().find("fragment")!=std::string::npos) std::filesystem::copy_file(f.path(), std::filesystem::path("/source/results-20261013")/f.path().filename(), std::filesystem::copy_options::overwrite_existing);
  auto schema=tpch::getTPCHSchema(); start=Clock::now(); helpers::loadDataInto(schema,"data",true);
  std::cerr << "load_ms=" << ms(start,Clock::now()) << '\n';
  std::ofstream out("/source/results-20261013/samples_sf"+sf+"_t"+std::to_string(threads)+"_"+only+".csv");
  out << "sf,threads,query,mode,rep,prepare_ms,elapsed_ms,compile_stall_ms,runtime_st_ms,runtime_mt_ms,rows\n";
  const std::vector<std::pair<std::string,decltype(tpch::q1)*>> qs={{"q1",tpch::q1},{"q3",tpch::q3},{"q4",tpch::q4},{"q5",tpch::q5},{"q6",tpch::q6},{"q14",tpch::q14},{"q19",tpch::q19},{"q_bigjoin",tpch::q_bigjoin}};
  using E=PipelineExecutor::ExecutionMode;
  const std::vector<std::pair<std::string,E>> modes={{"interpreted",E::Interpreted},{"fused_prepared",E::Fused}};
  for(const auto& [q,fn]:qs) {
    if(only!="all" && only!=q) continue;
    for(int rep=-1;rep<reps;rep++) for(size_t k=0;k<modes.size();k++) {
      auto [label,mode]=modes[(k+rep+1)%modes.size()];
      auto root=fn(schema); auto* printer=root->printer.get();
      std::ostringstream result; result<<std::setprecision(17);
      if(rep==-1) printer->setOstream(result);
      auto cb=std::make_shared<PipelineExecutor::QueryControlBlock>(std::move(root));
      std::string name=q+"_"+label+"_t"+std::to_string(threads)+"_r"+std::to_string(rep+1);
      double prep=0; PipelineExecutor::PipelineStats stats;
      auto t0=Clock::now();
      if(true) {
        auto exec=std::make_unique<QueryExecutor::StepwiseExecutor>(cb,mode,name,threads);
        exec->prepareQuery(); if(mode==E::Fused) exec->waitForCompilationForBenchmark();
        auto t1=Clock::now(); prep=ms(t0,t1); t0=t1;
        stats=exec->runQuery(); exec.reset();
      } else { stats=QueryExecutor::runQuery(cb,mode,name,threads); }
      auto t2=Clock::now();
      out<<sf<<','<<threads<<','<<q<<','<<label<<','<<rep<<','<<prep<<','<<ms(t0,t2)<<','<<stats.codegen_microseconds/1000.0<<','<<stats.runtime_microseconds_st/1000.0<<','<<stats.runtime_microseconds_mt/1000.0<<','<<printer->num_rows<<'\n'; out.flush();
      if(rep==-1) { std::ofstream val("/source/results-20261013/validation_sf"+sf+"_t"+std::to_string(threads)+"_"+q+"_"+label+".csv"); val<<result.str(); }
      // Allow asynchronous hybrid compiler cancellation to settle between samples.
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    std::cerr<<"finished "<<q<<" threads="<<threads<<'\n';
  }
}
