; ModuleID = 'roc'
source_filename = "roc"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-i128:128-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

; Function Attrs: nofree norecurse nosync nounwind memory(readwrite, inaccessiblemem: none)
define noundef i32 @roc_kernel_0(i64 %0, i64 %1, i64 %2, i64 %3, i64 %4) local_unnamed_addr #0 {
  %6 = inttoptr i64 %1 to ptr
  %7 = load i64, ptr %6, align 1
  %8 = getelementptr i8, ptr %6, i64 8
  %9 = load i64, ptr %8, align 1
  %10 = getelementptr i8, ptr %6, i64 16
  %11 = load i64, ptr %10, align 1
  %12 = getelementptr i8, ptr %6, i64 24
  %13 = load i64, ptr %12, align 1
  %14 = inttoptr i64 %2 to ptr
  %15 = load i64, ptr %14, align 1
  %16 = getelementptr i8, ptr %14, i64 8
  %17 = load i64, ptr %16, align 1
  %18 = icmp eq i64 %9, 0
  %19 = zext i1 %18 to i64
  %20 = icmp eq i64 %13, 0
  %21 = select i1 %20, i64 2, i64 0
  %22 = or disjoint i64 %21, %19
  switch i64 %22, label %default.unreachable [
    i64 0, label %79
    i64 1, label %132
    i64 2, label %195
    i64 3, label %258
  ]

.loopexit.loopexit174.unr-lcssa:                  ; preds = %346, %.lr.ph66
  %.unr212 = phi i64 [ 0, %.lr.ph66 ], [ %411, %346 ]
  %lcmp.mod213.not = icmp eq i64 %xtraiter210, 0
  br i1 %lcmp.mod213.not, label %.loopexit, label %.epil.preheader209

.epil.preheader209:                               ; preds = %.loopexit.loopexit174.unr-lcssa, %.epil.preheader209
  %23 = phi i64 [ %39, %.epil.preheader209 ], [ %.unr212, %.loopexit.loopexit174.unr-lcssa ]
  %epil.iter211 = phi i64 [ %epil.iter211.next, %.epil.preheader209 ], [ 0, %.loopexit.loopexit174.unr-lcssa ]
  %24 = shl i64 %23, 2
  %25 = getelementptr i8, ptr %81, i64 %24
  %26 = load i32, ptr %25, align 1
  %27 = zext i32 %26 to i64
  %28 = shl nuw nsw i64 %27, 3
  %29 = getelementptr i8, ptr %82, i64 %28
  %30 = load i64, ptr %29, align 1
  %31 = getelementptr i8, ptr %83, i64 %28
  %32 = load i64, ptr %31, align 1
  %33 = add i64 %32, %30
  %34 = xor i64 %33, %30
  %35 = xor i64 %33, %32
  %36 = and i64 %34, %35
  %.lobit45.epil = lshr i64 %36, 63
  %37 = getelementptr i8, ptr %84, i64 %28
  store i64 %33, ptr %37, align 1
  %38 = getelementptr i8, ptr %85, i64 %28
  store i64 %.lobit45.epil, ptr %38, align 1
  %39 = add nuw i64 %23, 1
  %epil.iter211.next = add i64 %epil.iter211, 1
  %epil.iter211.cmp.not = icmp eq i64 %epil.iter211.next, %xtraiter210
  br i1 %epil.iter211.cmp.not, label %.loopexit, label %.epil.preheader209, !llvm.loop !0

.loopexit.loopexit177.unr-lcssa:                  ; preds = %453, %.lr.ph64
  %.unr201 = phi i64 [ 0, %.lr.ph64 ], [ %510, %453 ]
  %lcmp.mod202.not = icmp eq i64 %xtraiter199, 0
  br i1 %lcmp.mod202.not, label %.loopexit, label %.epil.preheader198

.epil.preheader198:                               ; preds = %.loopexit.loopexit177.unr-lcssa, %.epil.preheader198
  %40 = phi i64 [ %54, %.epil.preheader198 ], [ %.unr201, %.loopexit.loopexit177.unr-lcssa ]
  %epil.iter200 = phi i64 [ %epil.iter200.next, %.epil.preheader198 ], [ 0, %.loopexit.loopexit177.unr-lcssa ]
  %41 = shl i64 %40, 2
  %42 = getelementptr i8, ptr %136, i64 %41
  %43 = load i32, ptr %42, align 1
  %44 = zext i32 %43 to i64
  %45 = shl nuw nsw i64 %44, 3
  %46 = getelementptr i8, ptr %137, i64 %45
  %47 = load i64, ptr %46, align 1
  %48 = add i64 %47, %134
  %49 = xor i64 %48, %134
  %50 = xor i64 %48, %47
  %51 = and i64 %49, %50
  %.lobit43.epil = lshr i64 %51, 63
  %52 = getelementptr i8, ptr %138, i64 %45
  store i64 %48, ptr %52, align 1
  %53 = getelementptr i8, ptr %139, i64 %45
  store i64 %.lobit43.epil, ptr %53, align 1
  %54 = add nuw i64 %40, 1
  %epil.iter200.next = add i64 %epil.iter200, 1
  %epil.iter200.cmp.not = icmp eq i64 %epil.iter200.next, %xtraiter199
  br i1 %epil.iter200.cmp.not, label %.loopexit, label %.epil.preheader198, !llvm.loop !2

.loopexit.loopexit180.unr-lcssa:                  ; preds = %552, %.lr.ph62
  %.unr190 = phi i64 [ 0, %.lr.ph62 ], [ %609, %552 ]
  %lcmp.mod191.not = icmp eq i64 %xtraiter188, 0
  br i1 %lcmp.mod191.not, label %.loopexit, label %.epil.preheader187

.epil.preheader187:                               ; preds = %.loopexit.loopexit180.unr-lcssa, %.epil.preheader187
  %55 = phi i64 [ %69, %.epil.preheader187 ], [ %.unr190, %.loopexit.loopexit180.unr-lcssa ]
  %epil.iter189 = phi i64 [ %epil.iter189.next, %.epil.preheader187 ], [ 0, %.loopexit.loopexit180.unr-lcssa ]
  %56 = shl i64 %55, 2
  %57 = getelementptr i8, ptr %199, i64 %56
  %58 = load i32, ptr %57, align 1
  %59 = zext i32 %58 to i64
  %60 = shl nuw nsw i64 %59, 3
  %61 = getelementptr i8, ptr %200, i64 %60
  %62 = load i64, ptr %61, align 1
  %63 = add i64 %62, %197
  %64 = xor i64 %63, %62
  %65 = xor i64 %63, %197
  %66 = and i64 %64, %65
  %.lobit41.epil = lshr i64 %66, 63
  %67 = getelementptr i8, ptr %201, i64 %60
  store i64 %63, ptr %67, align 1
  %68 = getelementptr i8, ptr %202, i64 %60
  store i64 %.lobit41.epil, ptr %68, align 1
  %69 = add nuw i64 %55, 1
  %epil.iter189.next = add i64 %epil.iter189, 1
  %epil.iter189.cmp.not = icmp eq i64 %epil.iter189.next, %xtraiter188
  br i1 %epil.iter189.cmp.not, label %.loopexit, label %.epil.preheader187, !llvm.loop !3

.loopexit.loopexit183.unr-lcssa:                  ; preds = %643, %.lr.ph
  %.unr = phi i64 [ 0, %.lr.ph ], [ %708, %643 ]
  %lcmp.mod.not = icmp eq i64 %xtraiter, 0
  br i1 %lcmp.mod.not, label %.loopexit, label %.epil.preheader

.epil.preheader:                                  ; preds = %.loopexit.loopexit183.unr-lcssa, %.epil.preheader
  %70 = phi i64 [ %78, %.epil.preheader ], [ %.unr, %.loopexit.loopexit183.unr-lcssa ]
  %epil.iter = phi i64 [ %epil.iter.next, %.epil.preheader ], [ 0, %.loopexit.loopexit183.unr-lcssa ]
  %71 = shl i64 %70, 2
  %72 = getelementptr i8, ptr %264, i64 %71
  %73 = load i32, ptr %72, align 1
  %74 = zext i32 %73 to i64
  %75 = shl nuw nsw i64 %74, 3
  %76 = getelementptr i8, ptr %269, i64 %75
  store i64 %265, ptr %76, align 1
  %77 = getelementptr i8, ptr %270, i64 %75
  store i64 %.lobit, ptr %77, align 1
  %78 = add nuw i64 %70, 1
  %epil.iter.next = add i64 %epil.iter, 1
  %epil.iter.cmp.not = icmp eq i64 %epil.iter.next, %xtraiter
  br i1 %epil.iter.cmp.not, label %.loopexit, label %.epil.preheader, !llvm.loop !4

.loopexit:                                        ; preds = %.loopexit.loopexit183.unr-lcssa, %.epil.preheader, %scalar.ph.prol.loopexit, %scalar.ph, %.loopexit.loopexit180.unr-lcssa, %.epil.preheader187, %scalar.ph111.prol.loopexit, %scalar.ph111, %.loopexit.loopexit177.unr-lcssa, %.epil.preheader198, %scalar.ph133.prol.loopexit, %scalar.ph133, %.loopexit.loopexit174.unr-lcssa, %.epil.preheader209, %scalar.ph160.prol.loopexit, %scalar.ph160, %middle.block, %middle.block110, %middle.block132, %middle.block159, %.preheader59, %.preheader57, %.preheader55, %.preheader53, %.preheader51, %.preheader49, %.preheader47, %.preheader
  ret i32 0

79:                                               ; preds = %5
  %80 = icmp eq i64 %3, 0
  %.not74 = icmp eq i64 %4, 0
  br i1 %80, label %.preheader, label %.preheader47

.preheader47:                                     ; preds = %79
  br i1 %.not74, label %.loopexit, label %.lr.ph66

.lr.ph66:                                         ; preds = %.preheader47
  %81 = inttoptr i64 %3 to ptr
  %82 = inttoptr i64 %7 to ptr
  %83 = inttoptr i64 %11 to ptr
  %84 = inttoptr i64 %15 to ptr
  %85 = inttoptr i64 %17 to ptr
  %86 = add i64 %4, -1
  %xtraiter210 = and i64 %4, 3
  %87 = icmp ult i64 %86, 3
  br i1 %87, label %.loopexit.loopexit174.unr-lcssa, label %.lr.ph66.new

.lr.ph66.new:                                     ; preds = %.lr.ph66
  %unroll_iter214 = and i64 %4, -4
  br label %346

.preheader:                                       ; preds = %79
  br i1 %.not74, label %.loopexit, label %.lr.ph67

.lr.ph67:                                         ; preds = %.preheader
  %88 = inttoptr i64 %7 to ptr
  %89 = inttoptr i64 %11 to ptr
  %90 = inttoptr i64 %15 to ptr
  %91 = inttoptr i64 %17 to ptr
  %min.iters.check161 = icmp ult i64 %4, 12
  br i1 %min.iters.check161, label %scalar.ph160.preheader, label %vector.memcheck149

scalar.ph160.preheader:                           ; preds = %middle.block159, %vector.memcheck149, %.lr.ph67
  %.ph = phi i64 [ 0, %vector.memcheck149 ], [ 0, %.lr.ph67 ], [ %n.vec164, %middle.block159 ]
  %xtraiter216 = and i64 %4, 3
  %lcmp.mod217.not = icmp eq i64 %xtraiter216, 0
  br i1 %lcmp.mod217.not, label %scalar.ph160.prol.loopexit, label %scalar.ph160.prol

scalar.ph160.prol:                                ; preds = %scalar.ph160.preheader, %scalar.ph160.prol
  %92 = phi i64 [ %104, %scalar.ph160.prol ], [ %.ph, %scalar.ph160.preheader ]
  %prol.iter218 = phi i64 [ %prol.iter218.next, %scalar.ph160.prol ], [ 0, %scalar.ph160.preheader ]
  %93 = shl i64 %92, 3
  %94 = getelementptr i8, ptr %88, i64 %93
  %95 = load i64, ptr %94, align 1
  %96 = getelementptr i8, ptr %89, i64 %93
  %97 = load i64, ptr %96, align 1
  %98 = add i64 %97, %95
  %99 = xor i64 %98, %95
  %100 = xor i64 %98, %97
  %101 = and i64 %99, %100
  %.lobit46.prol = lshr i64 %101, 63
  %102 = getelementptr i8, ptr %90, i64 %93
  store i64 %98, ptr %102, align 1
  %103 = getelementptr i8, ptr %91, i64 %93
  store i64 %.lobit46.prol, ptr %103, align 1
  %104 = add nuw i64 %92, 1
  %prol.iter218.next = add i64 %prol.iter218, 1
  %prol.iter218.cmp.not = icmp eq i64 %prol.iter218.next, %xtraiter216
  br i1 %prol.iter218.cmp.not, label %scalar.ph160.prol.loopexit, label %scalar.ph160.prol, !llvm.loop !5

scalar.ph160.prol.loopexit:                       ; preds = %scalar.ph160.prol, %scalar.ph160.preheader
  %.unr219 = phi i64 [ %.ph, %scalar.ph160.preheader ], [ %104, %scalar.ph160.prol ]
  %105 = sub i64 %.ph, %4
  %106 = icmp ugt i64 %105, -4
  br i1 %106, label %.loopexit, label %scalar.ph160

vector.memcheck149:                               ; preds = %.lr.ph67
  %107 = sub i64 %17, %15
  %diff.check150 = icmp ult i64 %107, 64
  %108 = sub i64 %15, %7
  %diff.check151 = icmp ult i64 %108, 64
  %conflict.rdx152 = or i1 %diff.check150, %diff.check151
  %109 = sub i64 %15, %11
  %diff.check153 = icmp ult i64 %109, 64
  %conflict.rdx154 = or i1 %conflict.rdx152, %diff.check153
  %110 = sub i64 %17, %7
  %diff.check155 = icmp ult i64 %110, 64
  %conflict.rdx156 = or i1 %conflict.rdx154, %diff.check155
  %111 = sub i64 %17, %11
  %diff.check157 = icmp ult i64 %111, 64
  %conflict.rdx158 = or i1 %conflict.rdx156, %diff.check157
  br i1 %conflict.rdx158, label %scalar.ph160.preheader, label %vector.ph162

vector.ph162:                                     ; preds = %vector.memcheck149
  %n.vec164 = and i64 %4, -8
  br label %vector.body166

vector.body166:                                   ; preds = %vector.body166, %vector.ph162
  %index167 = phi i64 [ 0, %vector.ph162 ], [ %index.next172, %vector.body166 ]
  %112 = shl i64 %index167, 3
  %113 = getelementptr i8, ptr %88, i64 %112
  %114 = getelementptr i8, ptr %113, i64 32
  %wide.load168 = load <4 x i64>, ptr %113, align 1
  %wide.load169 = load <4 x i64>, ptr %114, align 1
  %115 = getelementptr i8, ptr %89, i64 %112
  %116 = getelementptr i8, ptr %115, i64 32
  %wide.load170 = load <4 x i64>, ptr %115, align 1
  %wide.load171 = load <4 x i64>, ptr %116, align 1
  %117 = add <4 x i64> %wide.load170, %wide.load168
  %118 = add <4 x i64> %wide.load171, %wide.load169
  %119 = xor <4 x i64> %117, %wide.load168
  %120 = xor <4 x i64> %118, %wide.load169
  %121 = xor <4 x i64> %117, %wide.load170
  %122 = xor <4 x i64> %118, %wide.load171
  %123 = and <4 x i64> %119, %121
  %124 = and <4 x i64> %120, %122
  %125 = lshr <4 x i64> %123, <i64 63, i64 63, i64 63, i64 63>
  %126 = lshr <4 x i64> %124, <i64 63, i64 63, i64 63, i64 63>
  %127 = getelementptr i8, ptr %90, i64 %112
  %128 = getelementptr i8, ptr %127, i64 32
  store <4 x i64> %117, ptr %127, align 1
  store <4 x i64> %118, ptr %128, align 1
  %129 = getelementptr i8, ptr %91, i64 %112
  %130 = getelementptr i8, ptr %129, i64 32
  store <4 x i64> %125, ptr %129, align 1
  store <4 x i64> %126, ptr %130, align 1
  %index.next172 = add nuw i64 %index167, 8
  %131 = icmp eq i64 %index.next172, %n.vec164
  br i1 %131, label %middle.block159, label %vector.body166, !llvm.loop !6

middle.block159:                                  ; preds = %vector.body166
  %cmp.n173 = icmp eq i64 %n.vec164, %4
  br i1 %cmp.n173, label %.loopexit, label %scalar.ph160.preheader

132:                                              ; preds = %5
  %133 = inttoptr i64 %7 to ptr
  %134 = load i64, ptr %133, align 1
  %135 = icmp eq i64 %3, 0
  %.not72 = icmp eq i64 %4, 0
  br i1 %135, label %.preheader49, label %.preheader51

.preheader51:                                     ; preds = %132
  br i1 %.not72, label %.loopexit, label %.lr.ph64

.lr.ph64:                                         ; preds = %.preheader51
  %136 = inttoptr i64 %3 to ptr
  %137 = inttoptr i64 %11 to ptr
  %138 = inttoptr i64 %15 to ptr
  %139 = inttoptr i64 %17 to ptr
  %140 = add i64 %4, -1
  %xtraiter199 = and i64 %4, 3
  %141 = icmp ult i64 %140, 3
  br i1 %141, label %.loopexit.loopexit177.unr-lcssa, label %.lr.ph64.new

.lr.ph64.new:                                     ; preds = %.lr.ph64
  %unroll_iter203 = and i64 %4, -4
  br label %453

.preheader49:                                     ; preds = %132
  br i1 %.not72, label %.loopexit, label %.lr.ph65

.lr.ph65:                                         ; preds = %.preheader49
  %142 = inttoptr i64 %11 to ptr
  %143 = inttoptr i64 %15 to ptr
  %144 = inttoptr i64 %17 to ptr
  %min.iters.check134 = icmp ult i64 %4, 16
  br i1 %min.iters.check134, label %scalar.ph133.preheader, label %vector.memcheck126

scalar.ph133.preheader:                           ; preds = %middle.block132, %vector.memcheck126, %.lr.ph65
  %.ph175 = phi i64 [ 0, %vector.memcheck126 ], [ 0, %.lr.ph65 ], [ %n.vec137, %middle.block132 ]
  %xtraiter205 = and i64 %4, 3
  %lcmp.mod206.not = icmp eq i64 %xtraiter205, 0
  br i1 %lcmp.mod206.not, label %scalar.ph133.prol.loopexit, label %scalar.ph133.prol

scalar.ph133.prol:                                ; preds = %scalar.ph133.preheader, %scalar.ph133.prol
  %145 = phi i64 [ %155, %scalar.ph133.prol ], [ %.ph175, %scalar.ph133.preheader ]
  %prol.iter207 = phi i64 [ %prol.iter207.next, %scalar.ph133.prol ], [ 0, %scalar.ph133.preheader ]
  %146 = shl i64 %145, 3
  %147 = getelementptr i8, ptr %142, i64 %146
  %148 = load i64, ptr %147, align 1
  %149 = add i64 %148, %134
  %150 = xor i64 %149, %134
  %151 = xor i64 %149, %148
  %152 = and i64 %150, %151
  %.lobit44.prol = lshr i64 %152, 63
  %153 = getelementptr i8, ptr %143, i64 %146
  store i64 %149, ptr %153, align 1
  %154 = getelementptr i8, ptr %144, i64 %146
  store i64 %.lobit44.prol, ptr %154, align 1
  %155 = add nuw i64 %145, 1
  %prol.iter207.next = add i64 %prol.iter207, 1
  %prol.iter207.cmp.not = icmp eq i64 %prol.iter207.next, %xtraiter205
  br i1 %prol.iter207.cmp.not, label %scalar.ph133.prol.loopexit, label %scalar.ph133.prol, !llvm.loop !9

scalar.ph133.prol.loopexit:                       ; preds = %scalar.ph133.prol, %scalar.ph133.preheader
  %.unr208 = phi i64 [ %.ph175, %scalar.ph133.preheader ], [ %155, %scalar.ph133.prol ]
  %156 = sub i64 %.ph175, %4
  %157 = icmp ugt i64 %156, -4
  br i1 %157, label %.loopexit, label %scalar.ph133

vector.memcheck126:                               ; preds = %.lr.ph65
  %158 = sub i64 %17, %15
  %diff.check127 = icmp ult i64 %158, 128
  %159 = sub i64 %15, %11
  %diff.check128 = icmp ult i64 %159, 128
  %conflict.rdx129 = or i1 %diff.check127, %diff.check128
  %160 = sub i64 %17, %11
  %diff.check130 = icmp ult i64 %160, 128
  %conflict.rdx131 = or i1 %conflict.rdx129, %diff.check130
  br i1 %conflict.rdx131, label %scalar.ph133.preheader, label %vector.ph135

vector.ph135:                                     ; preds = %vector.memcheck126
  %n.vec137 = and i64 %4, -16
  %broadcast.splatinsert145 = insertelement <4 x i64> poison, i64 %134, i64 0
  %broadcast.splat146 = shufflevector <4 x i64> %broadcast.splatinsert145, <4 x i64> poison, <4 x i32> zeroinitializer
  br label %vector.body139

vector.body139:                                   ; preds = %vector.body139, %vector.ph135
  %index140 = phi i64 [ 0, %vector.ph135 ], [ %index.next147, %vector.body139 ]
  %161 = shl i64 %index140, 3
  %162 = getelementptr i8, ptr %142, i64 %161
  %163 = getelementptr i8, ptr %162, i64 32
  %164 = getelementptr i8, ptr %162, i64 64
  %165 = getelementptr i8, ptr %162, i64 96
  %wide.load141 = load <4 x i64>, ptr %162, align 1
  %wide.load142 = load <4 x i64>, ptr %163, align 1
  %wide.load143 = load <4 x i64>, ptr %164, align 1
  %wide.load144 = load <4 x i64>, ptr %165, align 1
  %166 = add <4 x i64> %wide.load141, %broadcast.splat146
  %167 = add <4 x i64> %wide.load142, %broadcast.splat146
  %168 = add <4 x i64> %wide.load143, %broadcast.splat146
  %169 = add <4 x i64> %wide.load144, %broadcast.splat146
  %170 = xor <4 x i64> %166, %broadcast.splat146
  %171 = xor <4 x i64> %167, %broadcast.splat146
  %172 = xor <4 x i64> %168, %broadcast.splat146
  %173 = xor <4 x i64> %169, %broadcast.splat146
  %174 = xor <4 x i64> %166, %wide.load141
  %175 = xor <4 x i64> %167, %wide.load142
  %176 = xor <4 x i64> %168, %wide.load143
  %177 = xor <4 x i64> %169, %wide.load144
  %178 = and <4 x i64> %170, %174
  %179 = and <4 x i64> %171, %175
  %180 = and <4 x i64> %172, %176
  %181 = and <4 x i64> %173, %177
  %182 = lshr <4 x i64> %178, <i64 63, i64 63, i64 63, i64 63>
  %183 = lshr <4 x i64> %179, <i64 63, i64 63, i64 63, i64 63>
  %184 = lshr <4 x i64> %180, <i64 63, i64 63, i64 63, i64 63>
  %185 = lshr <4 x i64> %181, <i64 63, i64 63, i64 63, i64 63>
  %186 = getelementptr i8, ptr %143, i64 %161
  %187 = getelementptr i8, ptr %186, i64 32
  %188 = getelementptr i8, ptr %186, i64 64
  %189 = getelementptr i8, ptr %186, i64 96
  store <4 x i64> %166, ptr %186, align 1
  store <4 x i64> %167, ptr %187, align 1
  store <4 x i64> %168, ptr %188, align 1
  store <4 x i64> %169, ptr %189, align 1
  %190 = getelementptr i8, ptr %144, i64 %161
  %191 = getelementptr i8, ptr %190, i64 32
  %192 = getelementptr i8, ptr %190, i64 64
  %193 = getelementptr i8, ptr %190, i64 96
  store <4 x i64> %182, ptr %190, align 1
  store <4 x i64> %183, ptr %191, align 1
  store <4 x i64> %184, ptr %192, align 1
  store <4 x i64> %185, ptr %193, align 1
  %index.next147 = add nuw i64 %index140, 16
  %194 = icmp eq i64 %index.next147, %n.vec137
  br i1 %194, label %middle.block132, label %vector.body139, !llvm.loop !10

middle.block132:                                  ; preds = %vector.body139
  %cmp.n148 = icmp eq i64 %n.vec137, %4
  br i1 %cmp.n148, label %.loopexit, label %scalar.ph133.preheader

195:                                              ; preds = %5
  %196 = inttoptr i64 %11 to ptr
  %197 = load i64, ptr %196, align 1
  %198 = icmp eq i64 %3, 0
  %.not70 = icmp eq i64 %4, 0
  br i1 %198, label %.preheader53, label %.preheader55

.preheader55:                                     ; preds = %195
  br i1 %.not70, label %.loopexit, label %.lr.ph62

.lr.ph62:                                         ; preds = %.preheader55
  %199 = inttoptr i64 %3 to ptr
  %200 = inttoptr i64 %7 to ptr
  %201 = inttoptr i64 %15 to ptr
  %202 = inttoptr i64 %17 to ptr
  %203 = add i64 %4, -1
  %xtraiter188 = and i64 %4, 3
  %204 = icmp ult i64 %203, 3
  br i1 %204, label %.loopexit.loopexit180.unr-lcssa, label %.lr.ph62.new

.lr.ph62.new:                                     ; preds = %.lr.ph62
  %unroll_iter192 = and i64 %4, -4
  br label %552

.preheader53:                                     ; preds = %195
  br i1 %.not70, label %.loopexit, label %.lr.ph63

.lr.ph63:                                         ; preds = %.preheader53
  %205 = inttoptr i64 %7 to ptr
  %206 = inttoptr i64 %15 to ptr
  %207 = inttoptr i64 %17 to ptr
  %min.iters.check112 = icmp ult i64 %4, 16
  br i1 %min.iters.check112, label %scalar.ph111.preheader, label %vector.memcheck105

scalar.ph111.preheader:                           ; preds = %middle.block110, %vector.memcheck105, %.lr.ph63
  %.ph178 = phi i64 [ 0, %vector.memcheck105 ], [ 0, %.lr.ph63 ], [ %n.vec115, %middle.block110 ]
  %xtraiter194 = and i64 %4, 3
  %lcmp.mod195.not = icmp eq i64 %xtraiter194, 0
  br i1 %lcmp.mod195.not, label %scalar.ph111.prol.loopexit, label %scalar.ph111.prol

scalar.ph111.prol:                                ; preds = %scalar.ph111.preheader, %scalar.ph111.prol
  %208 = phi i64 [ %218, %scalar.ph111.prol ], [ %.ph178, %scalar.ph111.preheader ]
  %prol.iter196 = phi i64 [ %prol.iter196.next, %scalar.ph111.prol ], [ 0, %scalar.ph111.preheader ]
  %209 = shl i64 %208, 3
  %210 = getelementptr i8, ptr %205, i64 %209
  %211 = load i64, ptr %210, align 1
  %212 = add i64 %211, %197
  %213 = xor i64 %212, %211
  %214 = xor i64 %212, %197
  %215 = and i64 %213, %214
  %.lobit42.prol = lshr i64 %215, 63
  %216 = getelementptr i8, ptr %206, i64 %209
  store i64 %212, ptr %216, align 1
  %217 = getelementptr i8, ptr %207, i64 %209
  store i64 %.lobit42.prol, ptr %217, align 1
  %218 = add nuw i64 %208, 1
  %prol.iter196.next = add i64 %prol.iter196, 1
  %prol.iter196.cmp.not = icmp eq i64 %prol.iter196.next, %xtraiter194
  br i1 %prol.iter196.cmp.not, label %scalar.ph111.prol.loopexit, label %scalar.ph111.prol, !llvm.loop !11

scalar.ph111.prol.loopexit:                       ; preds = %scalar.ph111.prol, %scalar.ph111.preheader
  %.unr197 = phi i64 [ %.ph178, %scalar.ph111.preheader ], [ %218, %scalar.ph111.prol ]
  %219 = sub i64 %.ph178, %4
  %220 = icmp ugt i64 %219, -4
  br i1 %220, label %.loopexit, label %scalar.ph111

vector.memcheck105:                               ; preds = %.lr.ph63
  %221 = sub i64 %17, %15
  %diff.check106 = icmp ult i64 %221, 128
  %222 = sub i64 %15, %7
  %diff.check107 = icmp ult i64 %222, 128
  %conflict.rdx = or i1 %diff.check106, %diff.check107
  %223 = sub i64 %17, %7
  %diff.check108 = icmp ult i64 %223, 128
  %conflict.rdx109 = or i1 %conflict.rdx, %diff.check108
  br i1 %conflict.rdx109, label %scalar.ph111.preheader, label %vector.ph113

vector.ph113:                                     ; preds = %vector.memcheck105
  %n.vec115 = and i64 %4, -16
  %broadcast.splatinsert122 = insertelement <4 x i64> poison, i64 %197, i64 0
  %broadcast.splat123 = shufflevector <4 x i64> %broadcast.splatinsert122, <4 x i64> poison, <4 x i32> zeroinitializer
  br label %vector.body117

vector.body117:                                   ; preds = %vector.body117, %vector.ph113
  %index118 = phi i64 [ 0, %vector.ph113 ], [ %index.next124, %vector.body117 ]
  %224 = shl i64 %index118, 3
  %225 = getelementptr i8, ptr %205, i64 %224
  %226 = getelementptr i8, ptr %225, i64 32
  %227 = getelementptr i8, ptr %225, i64 64
  %228 = getelementptr i8, ptr %225, i64 96
  %wide.load = load <4 x i64>, ptr %225, align 1
  %wide.load119 = load <4 x i64>, ptr %226, align 1
  %wide.load120 = load <4 x i64>, ptr %227, align 1
  %wide.load121 = load <4 x i64>, ptr %228, align 1
  %229 = add <4 x i64> %wide.load, %broadcast.splat123
  %230 = add <4 x i64> %wide.load119, %broadcast.splat123
  %231 = add <4 x i64> %wide.load120, %broadcast.splat123
  %232 = add <4 x i64> %wide.load121, %broadcast.splat123
  %233 = xor <4 x i64> %229, %wide.load
  %234 = xor <4 x i64> %230, %wide.load119
  %235 = xor <4 x i64> %231, %wide.load120
  %236 = xor <4 x i64> %232, %wide.load121
  %237 = xor <4 x i64> %229, %broadcast.splat123
  %238 = xor <4 x i64> %230, %broadcast.splat123
  %239 = xor <4 x i64> %231, %broadcast.splat123
  %240 = xor <4 x i64> %232, %broadcast.splat123
  %241 = and <4 x i64> %233, %237
  %242 = and <4 x i64> %234, %238
  %243 = and <4 x i64> %235, %239
  %244 = and <4 x i64> %236, %240
  %245 = lshr <4 x i64> %241, <i64 63, i64 63, i64 63, i64 63>
  %246 = lshr <4 x i64> %242, <i64 63, i64 63, i64 63, i64 63>
  %247 = lshr <4 x i64> %243, <i64 63, i64 63, i64 63, i64 63>
  %248 = lshr <4 x i64> %244, <i64 63, i64 63, i64 63, i64 63>
  %249 = getelementptr i8, ptr %206, i64 %224
  %250 = getelementptr i8, ptr %249, i64 32
  %251 = getelementptr i8, ptr %249, i64 64
  %252 = getelementptr i8, ptr %249, i64 96
  store <4 x i64> %229, ptr %249, align 1
  store <4 x i64> %230, ptr %250, align 1
  store <4 x i64> %231, ptr %251, align 1
  store <4 x i64> %232, ptr %252, align 1
  %253 = getelementptr i8, ptr %207, i64 %224
  %254 = getelementptr i8, ptr %253, i64 32
  %255 = getelementptr i8, ptr %253, i64 64
  %256 = getelementptr i8, ptr %253, i64 96
  store <4 x i64> %245, ptr %253, align 1
  store <4 x i64> %246, ptr %254, align 1
  store <4 x i64> %247, ptr %255, align 1
  store <4 x i64> %248, ptr %256, align 1
  %index.next124 = add nuw i64 %index118, 16
  %257 = icmp eq i64 %index.next124, %n.vec115
  br i1 %257, label %middle.block110, label %vector.body117, !llvm.loop !12

middle.block110:                                  ; preds = %vector.body117
  %cmp.n125 = icmp eq i64 %n.vec115, %4
  br i1 %cmp.n125, label %.loopexit, label %scalar.ph111.preheader

default.unreachable:                              ; preds = %5
  unreachable

258:                                              ; preds = %5
  %259 = inttoptr i64 %7 to ptr
  %260 = load i64, ptr %259, align 1
  %261 = inttoptr i64 %11 to ptr
  %262 = load i64, ptr %261, align 1
  %263 = icmp eq i64 %3, 0
  %.not68 = icmp eq i64 %4, 0
  br i1 %263, label %.preheader57, label %.preheader59

.preheader59:                                     ; preds = %258
  br i1 %.not68, label %.loopexit, label %.lr.ph

.lr.ph:                                           ; preds = %.preheader59
  %264 = inttoptr i64 %3 to ptr
  %265 = add i64 %262, %260
  %266 = xor i64 %265, %260
  %267 = xor i64 %265, %262
  %268 = and i64 %266, %267
  %.lobit = lshr i64 %268, 63
  %269 = inttoptr i64 %15 to ptr
  %270 = inttoptr i64 %17 to ptr
  %271 = add i64 %4, -1
  %xtraiter = and i64 %4, 7
  %272 = icmp ult i64 %271, 7
  br i1 %272, label %.loopexit.loopexit183.unr-lcssa, label %.lr.ph.new

.lr.ph.new:                                       ; preds = %.lr.ph
  %unroll_iter = and i64 %4, -8
  br label %643

.preheader57:                                     ; preds = %258
  br i1 %.not68, label %.loopexit, label %.lr.ph61

.lr.ph61:                                         ; preds = %.preheader57
  %273 = add i64 %262, %260
  %274 = xor i64 %273, %260
  %275 = xor i64 %273, %262
  %276 = and i64 %274, %275
  %.lobit40 = lshr i64 %276, 63
  %277 = inttoptr i64 %15 to ptr
  %278 = inttoptr i64 %17 to ptr
  %min.iters.check = icmp ult i64 %4, 16
  %279 = sub i64 %17, %15
  %diff.check = icmp ult i64 %279, 128
  %or.cond = select i1 %min.iters.check, i1 true, i1 %diff.check
  br i1 %or.cond, label %scalar.ph.preheader, label %vector.ph

scalar.ph.preheader:                              ; preds = %middle.block, %.lr.ph61
  %.ph181 = phi i64 [ 0, %.lr.ph61 ], [ %n.vec, %middle.block ]
  %xtraiter184 = and i64 %4, 7
  %lcmp.mod185.not = icmp eq i64 %xtraiter184, 0
  br i1 %lcmp.mod185.not, label %scalar.ph.prol.loopexit, label %scalar.ph.prol

scalar.ph.prol:                                   ; preds = %scalar.ph.preheader, %scalar.ph.prol
  %280 = phi i64 [ %284, %scalar.ph.prol ], [ %.ph181, %scalar.ph.preheader ]
  %prol.iter = phi i64 [ %prol.iter.next, %scalar.ph.prol ], [ 0, %scalar.ph.preheader ]
  %281 = shl i64 %280, 3
  %282 = getelementptr i8, ptr %277, i64 %281
  store i64 %273, ptr %282, align 1
  %283 = getelementptr i8, ptr %278, i64 %281
  store i64 %.lobit40, ptr %283, align 1
  %284 = add nuw i64 %280, 1
  %prol.iter.next = add i64 %prol.iter, 1
  %prol.iter.cmp.not = icmp eq i64 %prol.iter.next, %xtraiter184
  br i1 %prol.iter.cmp.not, label %scalar.ph.prol.loopexit, label %scalar.ph.prol, !llvm.loop !13

scalar.ph.prol.loopexit:                          ; preds = %scalar.ph.prol, %scalar.ph.preheader
  %.unr186 = phi i64 [ %.ph181, %scalar.ph.preheader ], [ %284, %scalar.ph.prol ]
  %285 = sub i64 %.ph181, %4
  %286 = icmp ugt i64 %285, -8
  br i1 %286, label %.loopexit, label %scalar.ph

vector.ph:                                        ; preds = %.lr.ph61
  %n.vec = and i64 %4, -16
  %broadcast.splatinsert = insertelement <4 x i64> poison, i64 %273, i64 0
  %broadcast.splat = shufflevector <4 x i64> %broadcast.splatinsert, <4 x i64> poison, <4 x i32> zeroinitializer
  %broadcast.splatinsert103 = insertelement <4 x i64> poison, i64 %.lobit40, i64 0
  %broadcast.splat104 = shufflevector <4 x i64> %broadcast.splatinsert103, <4 x i64> poison, <4 x i32> zeroinitializer
  br label %vector.body

vector.body:                                      ; preds = %vector.body, %vector.ph
  %index = phi i64 [ 0, %vector.ph ], [ %index.next, %vector.body ]
  %287 = shl i64 %index, 3
  %288 = getelementptr i8, ptr %277, i64 %287
  %289 = getelementptr i8, ptr %288, i64 32
  %290 = getelementptr i8, ptr %288, i64 64
  %291 = getelementptr i8, ptr %288, i64 96
  store <4 x i64> %broadcast.splat, ptr %288, align 1
  store <4 x i64> %broadcast.splat, ptr %289, align 1
  store <4 x i64> %broadcast.splat, ptr %290, align 1
  store <4 x i64> %broadcast.splat, ptr %291, align 1
  %292 = getelementptr i8, ptr %278, i64 %287
  %293 = getelementptr i8, ptr %292, i64 32
  %294 = getelementptr i8, ptr %292, i64 64
  %295 = getelementptr i8, ptr %292, i64 96
  store <4 x i64> %broadcast.splat104, ptr %292, align 1
  store <4 x i64> %broadcast.splat104, ptr %293, align 1
  store <4 x i64> %broadcast.splat104, ptr %294, align 1
  store <4 x i64> %broadcast.splat104, ptr %295, align 1
  %index.next = add nuw i64 %index, 16
  %296 = icmp eq i64 %index.next, %n.vec
  br i1 %296, label %middle.block, label %vector.body, !llvm.loop !14

middle.block:                                     ; preds = %vector.body
  %cmp.n = icmp eq i64 %n.vec, %4
  br i1 %cmp.n, label %.loopexit, label %scalar.ph.preheader

scalar.ph160:                                     ; preds = %scalar.ph160.prol.loopexit, %scalar.ph160
  %297 = phi i64 [ %345, %scalar.ph160 ], [ %.unr219, %scalar.ph160.prol.loopexit ]
  %298 = shl i64 %297, 3
  %299 = getelementptr i8, ptr %88, i64 %298
  %300 = load i64, ptr %299, align 1
  %301 = getelementptr i8, ptr %89, i64 %298
  %302 = load i64, ptr %301, align 1
  %303 = add i64 %302, %300
  %304 = xor i64 %303, %300
  %305 = xor i64 %303, %302
  %306 = and i64 %304, %305
  %.lobit46 = lshr i64 %306, 63
  %307 = getelementptr i8, ptr %90, i64 %298
  store i64 %303, ptr %307, align 1
  %308 = getelementptr i8, ptr %91, i64 %298
  store i64 %.lobit46, ptr %308, align 1
  %309 = shl i64 %297, 3
  %310 = add i64 %309, 8
  %311 = getelementptr i8, ptr %88, i64 %310
  %312 = load i64, ptr %311, align 1
  %313 = getelementptr i8, ptr %89, i64 %310
  %314 = load i64, ptr %313, align 1
  %315 = add i64 %314, %312
  %316 = xor i64 %315, %312
  %317 = xor i64 %315, %314
  %318 = and i64 %316, %317
  %.lobit46.1 = lshr i64 %318, 63
  %319 = getelementptr i8, ptr %90, i64 %310
  store i64 %315, ptr %319, align 1
  %320 = getelementptr i8, ptr %91, i64 %310
  store i64 %.lobit46.1, ptr %320, align 1
  %321 = shl i64 %297, 3
  %322 = add i64 %321, 16
  %323 = getelementptr i8, ptr %88, i64 %322
  %324 = load i64, ptr %323, align 1
  %325 = getelementptr i8, ptr %89, i64 %322
  %326 = load i64, ptr %325, align 1
  %327 = add i64 %326, %324
  %328 = xor i64 %327, %324
  %329 = xor i64 %327, %326
  %330 = and i64 %328, %329
  %.lobit46.2 = lshr i64 %330, 63
  %331 = getelementptr i8, ptr %90, i64 %322
  store i64 %327, ptr %331, align 1
  %332 = getelementptr i8, ptr %91, i64 %322
  store i64 %.lobit46.2, ptr %332, align 1
  %333 = shl i64 %297, 3
  %334 = add i64 %333, 24
  %335 = getelementptr i8, ptr %88, i64 %334
  %336 = load i64, ptr %335, align 1
  %337 = getelementptr i8, ptr %89, i64 %334
  %338 = load i64, ptr %337, align 1
  %339 = add i64 %338, %336
  %340 = xor i64 %339, %336
  %341 = xor i64 %339, %338
  %342 = and i64 %340, %341
  %.lobit46.3 = lshr i64 %342, 63
  %343 = getelementptr i8, ptr %90, i64 %334
  store i64 %339, ptr %343, align 1
  %344 = getelementptr i8, ptr %91, i64 %334
  store i64 %.lobit46.3, ptr %344, align 1
  %345 = add nuw i64 %297, 4
  %exitcond88.not.3 = icmp eq i64 %345, %4
  br i1 %exitcond88.not.3, label %.loopexit, label %scalar.ph160, !llvm.loop !15

346:                                              ; preds = %346, %.lr.ph66.new
  %347 = phi i64 [ 0, %.lr.ph66.new ], [ %411, %346 ]
  %niter215 = phi i64 [ 0, %.lr.ph66.new ], [ %niter215.next.3, %346 ]
  %348 = shl i64 %347, 2
  %349 = getelementptr i8, ptr %81, i64 %348
  %350 = load i32, ptr %349, align 1
  %351 = zext i32 %350 to i64
  %352 = shl nuw nsw i64 %351, 3
  %353 = getelementptr i8, ptr %82, i64 %352
  %354 = load i64, ptr %353, align 1
  %355 = getelementptr i8, ptr %83, i64 %352
  %356 = load i64, ptr %355, align 1
  %357 = add i64 %356, %354
  %358 = xor i64 %357, %354
  %359 = xor i64 %357, %356
  %360 = and i64 %358, %359
  %.lobit45 = lshr i64 %360, 63
  %361 = getelementptr i8, ptr %84, i64 %352
  store i64 %357, ptr %361, align 1
  %362 = getelementptr i8, ptr %85, i64 %352
  store i64 %.lobit45, ptr %362, align 1
  %363 = shl i64 %347, 2
  %364 = or disjoint i64 %363, 4
  %365 = getelementptr i8, ptr %81, i64 %364
  %366 = load i32, ptr %365, align 1
  %367 = zext i32 %366 to i64
  %368 = shl nuw nsw i64 %367, 3
  %369 = getelementptr i8, ptr %82, i64 %368
  %370 = load i64, ptr %369, align 1
  %371 = getelementptr i8, ptr %83, i64 %368
  %372 = load i64, ptr %371, align 1
  %373 = add i64 %372, %370
  %374 = xor i64 %373, %370
  %375 = xor i64 %373, %372
  %376 = and i64 %374, %375
  %.lobit45.1 = lshr i64 %376, 63
  %377 = getelementptr i8, ptr %84, i64 %368
  store i64 %373, ptr %377, align 1
  %378 = getelementptr i8, ptr %85, i64 %368
  store i64 %.lobit45.1, ptr %378, align 1
  %379 = shl i64 %347, 2
  %380 = or disjoint i64 %379, 8
  %381 = getelementptr i8, ptr %81, i64 %380
  %382 = load i32, ptr %381, align 1
  %383 = zext i32 %382 to i64
  %384 = shl nuw nsw i64 %383, 3
  %385 = getelementptr i8, ptr %82, i64 %384
  %386 = load i64, ptr %385, align 1
  %387 = getelementptr i8, ptr %83, i64 %384
  %388 = load i64, ptr %387, align 1
  %389 = add i64 %388, %386
  %390 = xor i64 %389, %386
  %391 = xor i64 %389, %388
  %392 = and i64 %390, %391
  %.lobit45.2 = lshr i64 %392, 63
  %393 = getelementptr i8, ptr %84, i64 %384
  store i64 %389, ptr %393, align 1
  %394 = getelementptr i8, ptr %85, i64 %384
  store i64 %.lobit45.2, ptr %394, align 1
  %395 = shl i64 %347, 2
  %396 = or disjoint i64 %395, 12
  %397 = getelementptr i8, ptr %81, i64 %396
  %398 = load i32, ptr %397, align 1
  %399 = zext i32 %398 to i64
  %400 = shl nuw nsw i64 %399, 3
  %401 = getelementptr i8, ptr %82, i64 %400
  %402 = load i64, ptr %401, align 1
  %403 = getelementptr i8, ptr %83, i64 %400
  %404 = load i64, ptr %403, align 1
  %405 = add i64 %404, %402
  %406 = xor i64 %405, %402
  %407 = xor i64 %405, %404
  %408 = and i64 %406, %407
  %.lobit45.3 = lshr i64 %408, 63
  %409 = getelementptr i8, ptr %84, i64 %400
  store i64 %405, ptr %409, align 1
  %410 = getelementptr i8, ptr %85, i64 %400
  store i64 %.lobit45.3, ptr %410, align 1
  %411 = add nuw i64 %347, 4
  %niter215.next.3 = add i64 %niter215, 4
  %niter215.ncmp.3 = icmp eq i64 %niter215.next.3, %unroll_iter214
  br i1 %niter215.ncmp.3, label %.loopexit.loopexit174.unr-lcssa, label %346

scalar.ph133:                                     ; preds = %scalar.ph133.prol.loopexit, %scalar.ph133
  %412 = phi i64 [ %452, %scalar.ph133 ], [ %.unr208, %scalar.ph133.prol.loopexit ]
  %413 = shl i64 %412, 3
  %414 = getelementptr i8, ptr %142, i64 %413
  %415 = load i64, ptr %414, align 1
  %416 = add i64 %415, %134
  %417 = xor i64 %416, %134
  %418 = xor i64 %416, %415
  %419 = and i64 %417, %418
  %.lobit44 = lshr i64 %419, 63
  %420 = getelementptr i8, ptr %143, i64 %413
  store i64 %416, ptr %420, align 1
  %421 = getelementptr i8, ptr %144, i64 %413
  store i64 %.lobit44, ptr %421, align 1
  %422 = shl i64 %412, 3
  %423 = add i64 %422, 8
  %424 = getelementptr i8, ptr %142, i64 %423
  %425 = load i64, ptr %424, align 1
  %426 = add i64 %425, %134
  %427 = xor i64 %426, %134
  %428 = xor i64 %426, %425
  %429 = and i64 %427, %428
  %.lobit44.1 = lshr i64 %429, 63
  %430 = getelementptr i8, ptr %143, i64 %423
  store i64 %426, ptr %430, align 1
  %431 = getelementptr i8, ptr %144, i64 %423
  store i64 %.lobit44.1, ptr %431, align 1
  %432 = shl i64 %412, 3
  %433 = add i64 %432, 16
  %434 = getelementptr i8, ptr %142, i64 %433
  %435 = load i64, ptr %434, align 1
  %436 = add i64 %435, %134
  %437 = xor i64 %436, %134
  %438 = xor i64 %436, %435
  %439 = and i64 %437, %438
  %.lobit44.2 = lshr i64 %439, 63
  %440 = getelementptr i8, ptr %143, i64 %433
  store i64 %436, ptr %440, align 1
  %441 = getelementptr i8, ptr %144, i64 %433
  store i64 %.lobit44.2, ptr %441, align 1
  %442 = shl i64 %412, 3
  %443 = add i64 %442, 24
  %444 = getelementptr i8, ptr %142, i64 %443
  %445 = load i64, ptr %444, align 1
  %446 = add i64 %445, %134
  %447 = xor i64 %446, %134
  %448 = xor i64 %446, %445
  %449 = and i64 %447, %448
  %.lobit44.3 = lshr i64 %449, 63
  %450 = getelementptr i8, ptr %143, i64 %443
  store i64 %446, ptr %450, align 1
  %451 = getelementptr i8, ptr %144, i64 %443
  store i64 %.lobit44.3, ptr %451, align 1
  %452 = add nuw i64 %412, 4
  %exitcond86.not.3 = icmp eq i64 %452, %4
  br i1 %exitcond86.not.3, label %.loopexit, label %scalar.ph133, !llvm.loop !16

453:                                              ; preds = %453, %.lr.ph64.new
  %454 = phi i64 [ 0, %.lr.ph64.new ], [ %510, %453 ]
  %niter204 = phi i64 [ 0, %.lr.ph64.new ], [ %niter204.next.3, %453 ]
  %455 = shl i64 %454, 2
  %456 = getelementptr i8, ptr %136, i64 %455
  %457 = load i32, ptr %456, align 1
  %458 = zext i32 %457 to i64
  %459 = shl nuw nsw i64 %458, 3
  %460 = getelementptr i8, ptr %137, i64 %459
  %461 = load i64, ptr %460, align 1
  %462 = add i64 %461, %134
  %463 = xor i64 %462, %134
  %464 = xor i64 %462, %461
  %465 = and i64 %463, %464
  %.lobit43 = lshr i64 %465, 63
  %466 = getelementptr i8, ptr %138, i64 %459
  store i64 %462, ptr %466, align 1
  %467 = getelementptr i8, ptr %139, i64 %459
  store i64 %.lobit43, ptr %467, align 1
  %468 = shl i64 %454, 2
  %469 = or disjoint i64 %468, 4
  %470 = getelementptr i8, ptr %136, i64 %469
  %471 = load i32, ptr %470, align 1
  %472 = zext i32 %471 to i64
  %473 = shl nuw nsw i64 %472, 3
  %474 = getelementptr i8, ptr %137, i64 %473
  %475 = load i64, ptr %474, align 1
  %476 = add i64 %475, %134
  %477 = xor i64 %476, %134
  %478 = xor i64 %476, %475
  %479 = and i64 %477, %478
  %.lobit43.1 = lshr i64 %479, 63
  %480 = getelementptr i8, ptr %138, i64 %473
  store i64 %476, ptr %480, align 1
  %481 = getelementptr i8, ptr %139, i64 %473
  store i64 %.lobit43.1, ptr %481, align 1
  %482 = shl i64 %454, 2
  %483 = or disjoint i64 %482, 8
  %484 = getelementptr i8, ptr %136, i64 %483
  %485 = load i32, ptr %484, align 1
  %486 = zext i32 %485 to i64
  %487 = shl nuw nsw i64 %486, 3
  %488 = getelementptr i8, ptr %137, i64 %487
  %489 = load i64, ptr %488, align 1
  %490 = add i64 %489, %134
  %491 = xor i64 %490, %134
  %492 = xor i64 %490, %489
  %493 = and i64 %491, %492
  %.lobit43.2 = lshr i64 %493, 63
  %494 = getelementptr i8, ptr %138, i64 %487
  store i64 %490, ptr %494, align 1
  %495 = getelementptr i8, ptr %139, i64 %487
  store i64 %.lobit43.2, ptr %495, align 1
  %496 = shl i64 %454, 2
  %497 = or disjoint i64 %496, 12
  %498 = getelementptr i8, ptr %136, i64 %497
  %499 = load i32, ptr %498, align 1
  %500 = zext i32 %499 to i64
  %501 = shl nuw nsw i64 %500, 3
  %502 = getelementptr i8, ptr %137, i64 %501
  %503 = load i64, ptr %502, align 1
  %504 = add i64 %503, %134
  %505 = xor i64 %504, %134
  %506 = xor i64 %504, %503
  %507 = and i64 %505, %506
  %.lobit43.3 = lshr i64 %507, 63
  %508 = getelementptr i8, ptr %138, i64 %501
  store i64 %504, ptr %508, align 1
  %509 = getelementptr i8, ptr %139, i64 %501
  store i64 %.lobit43.3, ptr %509, align 1
  %510 = add nuw i64 %454, 4
  %niter204.next.3 = add i64 %niter204, 4
  %niter204.ncmp.3 = icmp eq i64 %niter204.next.3, %unroll_iter203
  br i1 %niter204.ncmp.3, label %.loopexit.loopexit177.unr-lcssa, label %453

scalar.ph111:                                     ; preds = %scalar.ph111.prol.loopexit, %scalar.ph111
  %511 = phi i64 [ %551, %scalar.ph111 ], [ %.unr197, %scalar.ph111.prol.loopexit ]
  %512 = shl i64 %511, 3
  %513 = getelementptr i8, ptr %205, i64 %512
  %514 = load i64, ptr %513, align 1
  %515 = add i64 %514, %197
  %516 = xor i64 %515, %514
  %517 = xor i64 %515, %197
  %518 = and i64 %516, %517
  %.lobit42 = lshr i64 %518, 63
  %519 = getelementptr i8, ptr %206, i64 %512
  store i64 %515, ptr %519, align 1
  %520 = getelementptr i8, ptr %207, i64 %512
  store i64 %.lobit42, ptr %520, align 1
  %521 = shl i64 %511, 3
  %522 = add i64 %521, 8
  %523 = getelementptr i8, ptr %205, i64 %522
  %524 = load i64, ptr %523, align 1
  %525 = add i64 %524, %197
  %526 = xor i64 %525, %524
  %527 = xor i64 %525, %197
  %528 = and i64 %526, %527
  %.lobit42.1 = lshr i64 %528, 63
  %529 = getelementptr i8, ptr %206, i64 %522
  store i64 %525, ptr %529, align 1
  %530 = getelementptr i8, ptr %207, i64 %522
  store i64 %.lobit42.1, ptr %530, align 1
  %531 = shl i64 %511, 3
  %532 = add i64 %531, 16
  %533 = getelementptr i8, ptr %205, i64 %532
  %534 = load i64, ptr %533, align 1
  %535 = add i64 %534, %197
  %536 = xor i64 %535, %534
  %537 = xor i64 %535, %197
  %538 = and i64 %536, %537
  %.lobit42.2 = lshr i64 %538, 63
  %539 = getelementptr i8, ptr %206, i64 %532
  store i64 %535, ptr %539, align 1
  %540 = getelementptr i8, ptr %207, i64 %532
  store i64 %.lobit42.2, ptr %540, align 1
  %541 = shl i64 %511, 3
  %542 = add i64 %541, 24
  %543 = getelementptr i8, ptr %205, i64 %542
  %544 = load i64, ptr %543, align 1
  %545 = add i64 %544, %197
  %546 = xor i64 %545, %544
  %547 = xor i64 %545, %197
  %548 = and i64 %546, %547
  %.lobit42.3 = lshr i64 %548, 63
  %549 = getelementptr i8, ptr %206, i64 %542
  store i64 %545, ptr %549, align 1
  %550 = getelementptr i8, ptr %207, i64 %542
  store i64 %.lobit42.3, ptr %550, align 1
  %551 = add nuw i64 %511, 4
  %exitcond84.not.3 = icmp eq i64 %551, %4
  br i1 %exitcond84.not.3, label %.loopexit, label %scalar.ph111, !llvm.loop !17

552:                                              ; preds = %552, %.lr.ph62.new
  %553 = phi i64 [ 0, %.lr.ph62.new ], [ %609, %552 ]
  %niter193 = phi i64 [ 0, %.lr.ph62.new ], [ %niter193.next.3, %552 ]
  %554 = shl i64 %553, 2
  %555 = getelementptr i8, ptr %199, i64 %554
  %556 = load i32, ptr %555, align 1
  %557 = zext i32 %556 to i64
  %558 = shl nuw nsw i64 %557, 3
  %559 = getelementptr i8, ptr %200, i64 %558
  %560 = load i64, ptr %559, align 1
  %561 = add i64 %560, %197
  %562 = xor i64 %561, %560
  %563 = xor i64 %561, %197
  %564 = and i64 %562, %563
  %.lobit41 = lshr i64 %564, 63
  %565 = getelementptr i8, ptr %201, i64 %558
  store i64 %561, ptr %565, align 1
  %566 = getelementptr i8, ptr %202, i64 %558
  store i64 %.lobit41, ptr %566, align 1
  %567 = shl i64 %553, 2
  %568 = or disjoint i64 %567, 4
  %569 = getelementptr i8, ptr %199, i64 %568
  %570 = load i32, ptr %569, align 1
  %571 = zext i32 %570 to i64
  %572 = shl nuw nsw i64 %571, 3
  %573 = getelementptr i8, ptr %200, i64 %572
  %574 = load i64, ptr %573, align 1
  %575 = add i64 %574, %197
  %576 = xor i64 %575, %574
  %577 = xor i64 %575, %197
  %578 = and i64 %576, %577
  %.lobit41.1 = lshr i64 %578, 63
  %579 = getelementptr i8, ptr %201, i64 %572
  store i64 %575, ptr %579, align 1
  %580 = getelementptr i8, ptr %202, i64 %572
  store i64 %.lobit41.1, ptr %580, align 1
  %581 = shl i64 %553, 2
  %582 = or disjoint i64 %581, 8
  %583 = getelementptr i8, ptr %199, i64 %582
  %584 = load i32, ptr %583, align 1
  %585 = zext i32 %584 to i64
  %586 = shl nuw nsw i64 %585, 3
  %587 = getelementptr i8, ptr %200, i64 %586
  %588 = load i64, ptr %587, align 1
  %589 = add i64 %588, %197
  %590 = xor i64 %589, %588
  %591 = xor i64 %589, %197
  %592 = and i64 %590, %591
  %.lobit41.2 = lshr i64 %592, 63
  %593 = getelementptr i8, ptr %201, i64 %586
  store i64 %589, ptr %593, align 1
  %594 = getelementptr i8, ptr %202, i64 %586
  store i64 %.lobit41.2, ptr %594, align 1
  %595 = shl i64 %553, 2
  %596 = or disjoint i64 %595, 12
  %597 = getelementptr i8, ptr %199, i64 %596
  %598 = load i32, ptr %597, align 1
  %599 = zext i32 %598 to i64
  %600 = shl nuw nsw i64 %599, 3
  %601 = getelementptr i8, ptr %200, i64 %600
  %602 = load i64, ptr %601, align 1
  %603 = add i64 %602, %197
  %604 = xor i64 %603, %602
  %605 = xor i64 %603, %197
  %606 = and i64 %604, %605
  %.lobit41.3 = lshr i64 %606, 63
  %607 = getelementptr i8, ptr %201, i64 %600
  store i64 %603, ptr %607, align 1
  %608 = getelementptr i8, ptr %202, i64 %600
  store i64 %.lobit41.3, ptr %608, align 1
  %609 = add nuw i64 %553, 4
  %niter193.next.3 = add i64 %niter193, 4
  %niter193.ncmp.3 = icmp eq i64 %niter193.next.3, %unroll_iter192
  br i1 %niter193.ncmp.3, label %.loopexit.loopexit180.unr-lcssa, label %552

scalar.ph:                                        ; preds = %scalar.ph.prol.loopexit, %scalar.ph
  %610 = phi i64 [ %642, %scalar.ph ], [ %.unr186, %scalar.ph.prol.loopexit ]
  %611 = shl i64 %610, 3
  %612 = getelementptr i8, ptr %277, i64 %611
  store i64 %273, ptr %612, align 1
  %613 = getelementptr i8, ptr %278, i64 %611
  store i64 %.lobit40, ptr %613, align 1
  %614 = shl i64 %610, 3
  %615 = add i64 %614, 8
  %616 = getelementptr i8, ptr %277, i64 %615
  store i64 %273, ptr %616, align 1
  %617 = getelementptr i8, ptr %278, i64 %615
  store i64 %.lobit40, ptr %617, align 1
  %618 = shl i64 %610, 3
  %619 = add i64 %618, 16
  %620 = getelementptr i8, ptr %277, i64 %619
  store i64 %273, ptr %620, align 1
  %621 = getelementptr i8, ptr %278, i64 %619
  store i64 %.lobit40, ptr %621, align 1
  %622 = shl i64 %610, 3
  %623 = add i64 %622, 24
  %624 = getelementptr i8, ptr %277, i64 %623
  store i64 %273, ptr %624, align 1
  %625 = getelementptr i8, ptr %278, i64 %623
  store i64 %.lobit40, ptr %625, align 1
  %626 = shl i64 %610, 3
  %627 = add i64 %626, 32
  %628 = getelementptr i8, ptr %277, i64 %627
  store i64 %273, ptr %628, align 1
  %629 = getelementptr i8, ptr %278, i64 %627
  store i64 %.lobit40, ptr %629, align 1
  %630 = shl i64 %610, 3
  %631 = add i64 %630, 40
  %632 = getelementptr i8, ptr %277, i64 %631
  store i64 %273, ptr %632, align 1
  %633 = getelementptr i8, ptr %278, i64 %631
  store i64 %.lobit40, ptr %633, align 1
  %634 = shl i64 %610, 3
  %635 = add i64 %634, 48
  %636 = getelementptr i8, ptr %277, i64 %635
  store i64 %273, ptr %636, align 1
  %637 = getelementptr i8, ptr %278, i64 %635
  store i64 %.lobit40, ptr %637, align 1
  %638 = shl i64 %610, 3
  %639 = add i64 %638, 56
  %640 = getelementptr i8, ptr %277, i64 %639
  store i64 %273, ptr %640, align 1
  %641 = getelementptr i8, ptr %278, i64 %639
  store i64 %.lobit40, ptr %641, align 1
  %642 = add nuw i64 %610, 8
  %exitcond82.not.7 = icmp eq i64 %642, %4
  br i1 %exitcond82.not.7, label %.loopexit, label %scalar.ph, !llvm.loop !18

643:                                              ; preds = %643, %.lr.ph.new
  %644 = phi i64 [ 0, %.lr.ph.new ], [ %708, %643 ]
  %niter = phi i64 [ 0, %.lr.ph.new ], [ %niter.next.7, %643 ]
  %645 = shl i64 %644, 2
  %646 = getelementptr i8, ptr %264, i64 %645
  %647 = load i32, ptr %646, align 1
  %648 = zext i32 %647 to i64
  %649 = shl nuw nsw i64 %648, 3
  %650 = getelementptr i8, ptr %269, i64 %649
  store i64 %265, ptr %650, align 1
  %651 = getelementptr i8, ptr %270, i64 %649
  store i64 %.lobit, ptr %651, align 1
  %652 = shl i64 %644, 2
  %653 = or disjoint i64 %652, 4
  %654 = getelementptr i8, ptr %264, i64 %653
  %655 = load i32, ptr %654, align 1
  %656 = zext i32 %655 to i64
  %657 = shl nuw nsw i64 %656, 3
  %658 = getelementptr i8, ptr %269, i64 %657
  store i64 %265, ptr %658, align 1
  %659 = getelementptr i8, ptr %270, i64 %657
  store i64 %.lobit, ptr %659, align 1
  %660 = shl i64 %644, 2
  %661 = or disjoint i64 %660, 8
  %662 = getelementptr i8, ptr %264, i64 %661
  %663 = load i32, ptr %662, align 1
  %664 = zext i32 %663 to i64
  %665 = shl nuw nsw i64 %664, 3
  %666 = getelementptr i8, ptr %269, i64 %665
  store i64 %265, ptr %666, align 1
  %667 = getelementptr i8, ptr %270, i64 %665
  store i64 %.lobit, ptr %667, align 1
  %668 = shl i64 %644, 2
  %669 = or disjoint i64 %668, 12
  %670 = getelementptr i8, ptr %264, i64 %669
  %671 = load i32, ptr %670, align 1
  %672 = zext i32 %671 to i64
  %673 = shl nuw nsw i64 %672, 3
  %674 = getelementptr i8, ptr %269, i64 %673
  store i64 %265, ptr %674, align 1
  %675 = getelementptr i8, ptr %270, i64 %673
  store i64 %.lobit, ptr %675, align 1
  %676 = shl i64 %644, 2
  %677 = or disjoint i64 %676, 16
  %678 = getelementptr i8, ptr %264, i64 %677
  %679 = load i32, ptr %678, align 1
  %680 = zext i32 %679 to i64
  %681 = shl nuw nsw i64 %680, 3
  %682 = getelementptr i8, ptr %269, i64 %681
  store i64 %265, ptr %682, align 1
  %683 = getelementptr i8, ptr %270, i64 %681
  store i64 %.lobit, ptr %683, align 1
  %684 = shl i64 %644, 2
  %685 = or disjoint i64 %684, 20
  %686 = getelementptr i8, ptr %264, i64 %685
  %687 = load i32, ptr %686, align 1
  %688 = zext i32 %687 to i64
  %689 = shl nuw nsw i64 %688, 3
  %690 = getelementptr i8, ptr %269, i64 %689
  store i64 %265, ptr %690, align 1
  %691 = getelementptr i8, ptr %270, i64 %689
  store i64 %.lobit, ptr %691, align 1
  %692 = shl i64 %644, 2
  %693 = or disjoint i64 %692, 24
  %694 = getelementptr i8, ptr %264, i64 %693
  %695 = load i32, ptr %694, align 1
  %696 = zext i32 %695 to i64
  %697 = shl nuw nsw i64 %696, 3
  %698 = getelementptr i8, ptr %269, i64 %697
  store i64 %265, ptr %698, align 1
  %699 = getelementptr i8, ptr %270, i64 %697
  store i64 %.lobit, ptr %699, align 1
  %700 = shl i64 %644, 2
  %701 = or disjoint i64 %700, 28
  %702 = getelementptr i8, ptr %264, i64 %701
  %703 = load i32, ptr %702, align 1
  %704 = zext i32 %703 to i64
  %705 = shl nuw nsw i64 %704, 3
  %706 = getelementptr i8, ptr %269, i64 %705
  store i64 %265, ptr %706, align 1
  %707 = getelementptr i8, ptr %270, i64 %705
  store i64 %.lobit, ptr %707, align 1
  %708 = add nuw i64 %644, 8
  %niter.next.7 = add i64 %niter, 8
  %niter.ncmp.7 = icmp eq i64 %niter.next.7, %unroll_iter
  br i1 %niter.ncmp.7, label %.loopexit.loopexit183.unr-lcssa, label %643
}

attributes #0 = { nofree norecurse nosync nounwind memory(readwrite, inaccessiblemem: none) "target-cpu"="alderlake" "target-features"="+prfchw,-cldemote,+avx,+aes,+sahf,+pclmul,-xop,+crc32,+xsaves,-avx512fp16,-usermsr,-sm4,-egpr,+sse4.1,-avx512ifma,+xsave,+sse4.2,-tsxldtrk,-sm3,+ptwrite,+widekl,+invpcid,+64bit,+xsavec,-avx10.1-512,-avx512vpopcntdq,+cmov,-avx512vp2intersect,-avx512cd,+movbe,-avxvnniint8,-ccmp,-amx-int8,+kl,-avx10.1-256,-sha512,+avxvnni,-rtm,+adx,+avx2,+hreset,+movdiri,+serialize,+vpclmulqdq,-avx512vl,-uintr,-cf,+clflushopt,-raoint,-cmpccxadd,+bmi,-amx-tile,+sse,+gfni,-avxvnniint16,-amx-fp16,-ndd,+xsaveopt,+rdrnd,-avx512f,-amx-bf16,-avx512bf16,-avx512vnni,-push2pop2,+cx8,-avx512bw,+sse3,+pku,+fsgsbase,-clzero,-mwaitx,-lwp,+lzcnt,+sha,+movdir64b,-ppx,-wbnoinvd,-enqcmd,-avxneconvert,-tbm,+pconfig,-amx-complex,+ssse3,+cx16,+bmi2,+fma,+popcnt,-avxifma,+f16c,-avx512bitalg,-rdpru,+clwb,+mmx,+sse2,+rdseed,-avx512vbmi2,-prefetchi,+rdpid,-fma4,-avx512vbmi,+shstk,+vaes,+waitpkg,-sgx,+fxsr,-avx512dq,-sse4a" }

!0 = distinct !{!0, !1}
!1 = !{!"llvm.loop.unroll.disable"}
!2 = distinct !{!2, !1}
!3 = distinct !{!3, !1}
!4 = distinct !{!4, !1}
!5 = distinct !{!5, !1}
!6 = distinct !{!6, !7, !8}
!7 = !{!"llvm.loop.isvectorized", i32 1}
!8 = !{!"llvm.loop.unroll.runtime.disable"}
!9 = distinct !{!9, !1}
!10 = distinct !{!10, !7, !8}
!11 = distinct !{!11, !1}
!12 = distinct !{!12, !7, !8}
!13 = distinct !{!13, !1}
!14 = distinct !{!14, !7, !8}
!15 = distinct !{!15, !7}
!16 = distinct !{!16, !7}
!17 = distinct !{!17, !7}
!18 = distinct !{!18, !7}
