; bitmap_predicate
  pacibsp
  unwind Aarch64SetPointerAuth { return_addresses: true }
  stp fp, lr, [sp, #-16]!
  unwind PushFrameRegs { offset_upward_to_caller_sp: 16 }
  mov fp, sp
  unwind DefineNewFrame { offset_upward_to_caller_sp: 16, offset_downward_to_clobbers: 64 }
  stp d14, d15, [sp, #-16]!
  unwind SaveReg { clobber_offset: 48, reg: p14f }
  unwind SaveReg { clobber_offset: 56, reg: p15f }
  stp d12, d13, [sp, #-16]!
  unwind SaveReg { clobber_offset: 32, reg: p12f }
  unwind SaveReg { clobber_offset: 40, reg: p13f }
  stp d10, d11, [sp, #-16]!
  unwind SaveReg { clobber_offset: 16, reg: p10f }
  unwind SaveReg { clobber_offset: 24, reg: p11f }
  stp d8, d9, [sp, #-16]!
  unwind SaveReg { clobber_offset: 0, reg: p8f }
  unwind SaveReg { clobber_offset: 8, reg: p9f }
  sub sp, sp, #16
  unwind StackAlloc { size: 16 }
block0:
  ldr x3, [x0]
  ldr x0, [x0, #8]
  movz x6, #0
  mov x0, x6
  b label1
block1:
  sub x4, x1, x6
  subs xzr, x4, #64
  b.hs label8 ; b label2
block2:
  movz x14, #0
  cbnz x4, label3 ; b label4
block3:
  mov x5, x6
  b label5
block4:
  add sp, sp, #16
  ldp d8, d9, [sp], #16
  ldp d10, d11, [sp], #16
  ldp d12, d13, [sp], #16
  ldp d14, d15, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block5:
  subs xzr, x5, x1
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  str x14, [x2, x1]
  fmov d0, x14
  cnt v0.8b, v0.8b
  addv b1, v0.8b
  umov w3, v1.b[0]
  add x0, x0, x3
  add sp, sp, #16
  ldp d8, d9, [sp], #16
  ldp d10, d11, [sp], #16
  ldp d12, d13, [sp], #16
  ldp d14, d15, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x7, [x3, x5, LSL #3]
  add x4, x5, #1
  movn x8, #499
  subs xzr, x7, x8
  cset x7, lt
  uxtb w7, w7
  sub x5, x5, x6
  lsl x5, x7, x5
  orr x14, x14, x5
  mov x5, x4
  b label5
block8:
  lsl x5, x6, #3
  add x4, x3, x6, LSL 3
  ldr q15, [x3, x5]
  ldr q3, [x4, #16]
  ldr q13, [x4, #32]
  ldr q14, [x4, #48]
  ldr q11, [x4, #64]
  ldr q12, [x4, #80]
  ldr q9, [x4, #96]
  ldr q10, [x4, #112]
  ldr q2, [x4, #128]
  ldr q8, [x4, #144]
  ldr q0, [x4, #160]
  ldr q1, [x4, #176]
  ldr q30, [x4, #192]
  ldr q31, [x4, #208]
  ldr q28, [x4, #224]
  ldr q29, [x4, #240]
  ldr q26, [x4, #256]
  ldr q27, [x4, #272]
  ldr q24, [x4, #288]
  ldr q25, [x4, #304]
  ldr q22, [x4, #320]
  ldr q23, [x4, #336]
  ldr q20, [x4, #352]
  ldr q21, [x4, #368]
  ldr q18, [x4, #384]
  ldr q19, [x4, #400]
  ldr q16, [x4, #416]
  ldr q17, [x4, #432]
  ldr q5, [x4, #448]
  ldr q6, [x4, #464]
  ldr q4, [x4, #480]
  str q4, [sp]
  ldr q4, [x4, #496]
  movn x4, #499
  dup v7.2d, x4
  cmgt v15.2d, v7.2d, v15.2d
  cmgt v3.2d, v7.2d, v3.2d
  uzp1 v3.4s, v15.4s, v3.4s
  cmgt v13.2d, v7.2d, v13.2d
  cmgt v14.2d, v7.2d, v14.2d
  uzp1 v13.4s, v13.4s, v14.4s
  uzp1 v3.8h, v3.8h, v13.8h
  cmgt v11.2d, v7.2d, v11.2d
  cmgt v12.2d, v7.2d, v12.2d
  uzp1 v11.4s, v11.4s, v12.4s
  cmgt v9.2d, v7.2d, v9.2d
  cmgt v10.2d, v7.2d, v10.2d
  uzp1 v9.4s, v9.4s, v10.4s
  uzp1 v9.8h, v11.8h, v9.8h
  uzp1 v3.16b, v3.16b, v9.16b
  sshr v3.16b, v3.16b, #7
  movz x4, #513
  movk x4, x4, #2052, LSL #16
  movk x4, x4, #8208, LSL #32
  movk x4, x4, #32832, LSL #48
  dup v9.2d, x4
  and v3.16b, v3.16b, v9.16b
  ext v9.16b, v3.16b, v3.16b, #8
  zip1 v3.16b, v3.16b, v9.16b
  addv h3, v3.8h
  umov w13, v3.h[0]
  uxth w4, w13
  cmgt v3.2d, v7.2d, v2.2d
  cmgt v2.2d, v7.2d, v8.2d
  uzp1 v3.4s, v3.4s, v2.4s
  cmgt v0.2d, v7.2d, v0.2d
  cmgt v1.2d, v7.2d, v1.2d
  uzp1 v0.4s, v0.4s, v1.4s
  uzp1 v3.8h, v3.8h, v0.8h
  cmgt v30.2d, v7.2d, v30.2d
  cmgt v31.2d, v7.2d, v31.2d
  uzp1 v30.4s, v30.4s, v31.4s
  cmgt v28.2d, v7.2d, v28.2d
  cmgt v29.2d, v7.2d, v29.2d
  uzp1 v28.4s, v28.4s, v29.4s
  uzp1 v28.8h, v30.8h, v28.8h
  uzp1 v3.16b, v3.16b, v28.16b
  sshr v0.16b, v3.16b, #7
  movz x5, #513
  movk x5, x5, #2052, LSL #16
  movk x5, x5, #8208, LSL #32
  movk x5, x5, #32832, LSL #48
  dup v3.2d, x5
  and v28.16b, v0.16b, v3.16b
  ext v29.16b, v28.16b, v28.16b, #8
  zip1 v28.16b, v28.16b, v29.16b
  addv h28, v28.8h
  umov w13, v28.h[0]
  uxth w5, w13
  orr x4, x4, x5, LSL 16
  cmgt v3.2d, v7.2d, v26.2d
  cmgt v26.2d, v7.2d, v27.2d
  uzp1 v3.4s, v3.4s, v26.4s
  cmgt v24.2d, v7.2d, v24.2d
  cmgt v25.2d, v7.2d, v25.2d
  uzp1 v24.4s, v24.4s, v25.4s
  uzp1 v3.8h, v3.8h, v24.8h
  cmgt v22.2d, v7.2d, v22.2d
  cmgt v23.2d, v7.2d, v23.2d
  uzp1 v22.4s, v22.4s, v23.4s
  cmgt v20.2d, v7.2d, v20.2d
  cmgt v21.2d, v7.2d, v21.2d
  uzp1 v20.4s, v20.4s, v21.4s
  uzp1 v20.8h, v22.8h, v20.8h
  uzp1 v3.16b, v3.16b, v20.16b
  sshr v0.16b, v3.16b, #7
  movz x5, #513
  movk x5, x5, #2052, LSL #16
  movk x5, x5, #8208, LSL #32
  movk x5, x5, #32832, LSL #48
  dup v3.2d, x5
  and v20.16b, v0.16b, v3.16b
  ext v21.16b, v20.16b, v20.16b, #8
  zip1 v20.16b, v20.16b, v21.16b
  addv h20, v20.8h
  umov w13, v20.h[0]
  uxth w5, w13
  orr x4, x4, x5, LSL 32
  cmgt v3.2d, v7.2d, v18.2d
  cmgt v18.2d, v7.2d, v19.2d
  uzp1 v3.4s, v3.4s, v18.4s
  cmgt v16.2d, v7.2d, v16.2d
  cmgt v17.2d, v7.2d, v17.2d
  uzp1 v16.4s, v16.4s, v17.4s
  uzp1 v3.8h, v3.8h, v16.8h
  cmgt v5.2d, v7.2d, v5.2d
  cmgt v6.2d, v7.2d, v6.2d
  uzp1 v5.4s, v5.4s, v6.4s
  ldr q26, [sp]
  cmgt v6.2d, v7.2d, v26.2d
  cmgt v4.2d, v7.2d, v4.2d
  uzp1 v4.4s, v6.4s, v4.4s
  uzp1 v4.8h, v5.8h, v4.8h
  uzp1 v3.16b, v3.16b, v4.16b
  sshr v0.16b, v3.16b, #7
  movz x5, #513
  movk x5, x5, #2052, LSL #16
  movk x5, x5, #8208, LSL #32
  movk x5, x5, #32832, LSL #48
  dup v3.2d, x5
  and v5.16b, v0.16b, v3.16b
  ext v7.16b, v5.16b, v5.16b, #8
  zip1 v16.16b, v5.16b, v7.16b
  addv h16, v16.8h
  umov w13, v16.h[0]
  uxth w5, w13
  orr x4, x4, x5, LSL 48
  lsr x5, x6, #3
  str x4, [x2, x5]
  add x6, x6, #64
  fmov d1, x4
  cnt v3.8b, v1.8b
  addv b5, v3.8b
  umov w7, v5.b[0]
  add x0, x0, x7
  b label1

; bitmap_project
  unwind Aarch64SetPointerAuth { return_addresses: false }
block0:
  ldr x5, [x0]
  ldr x12, [x0, #8]
  ldr x4, [x3]
  movz x8, #0
  mov x0, x8
  b label1
block1:
  subs xzr, x8, x1
  b.lo label3 ; b label2
block2:
  ret
block3:
  lsr x3, x8, #3
  ldr x7, [x2, x3]
  cbz x7, label4 ; b label5
block4:
  b label41
block5:
  movn x6, #0
  movz x9, #64
  sub x3, x1, x8
  subs xzr, x3, x9
  csel x10, x3, x9, lo
  sub x3, x9, x10
  lsr x3, x6, x3
  add x6, x8, x10
  subs xzr, x7, x3
  b.eq label6 ; b label7
block6:
  mov x7, x8
  b label15
block7:
  b label8
block8:
  cbnz x7, label10 ; b label9
block9:
  b label41
block10:
  rbit x3, x7
  clz x3, x3
  add x6, x8, x3
  movz x9, #3
  ldr x3, [x5, x6, LSL #3]
  smulh x10, x3, x9
  madd x9, x3, x9, xzr
  asr x3, x9, #63
  subs xzr, x10, x3
  b.ne label11 ; b label12
block12:
  ldr x3, [x12, x6, LSL #3]
  adds x6, x9, x3
  cset x3, vs
  ands wzr, w3, #255
  b.ne label13 ; b label14
block14:
  str x6, [x4, x0, LSL #3]
  sub x3, x7, #1
  and x7, x7, x3
  add x0, x0, #1
  b label8
block15:
  sub x3, x6, x7
  subs xzr, x3, #4
  b.hs label23 ; b label16
block16:
  subs xzr, x7, x6
  b.lo label18 ; b label17
block17:
  b label41
block18:
  movz x9, #3
  ldr x3, [x5, x7, LSL #3]
  smulh x10, x3, x9
  madd x9, x3, x9, xzr
  asr x3, x9, #63
  subs xzr, x10, x3
  b.ne label19 ; b label20
block20:
  ldr x3, [x12, x7, LSL #3]
  adds x9, x9, x3
  cset x3, vs
  ands wzr, w3, #255
  b.ne label21 ; b label22
block22:
  str x9, [x4, x0, LSL #3]
  add x7, x7, #1
  add x0, x0, #1
  b label15
block23:
  movz x9, #3
  ldr x3, [x5, x7, LSL #3]
  smulh x10, x3, x9
  madd x9, x3, x9, xzr
  asr x3, x9, #63
  subs xzr, x10, x3
  b.ne label24 ; b label25
block25:
  ldr x10, [x12, x7, LSL #3]
  adds x9, x9, x10
  cset x10, vs
  ands wzr, w10, #255
  b.ne label26 ; b label27
block27:
  movz x10, #3
  str x9, [x4, x0, LSL #3]
  add x9, x7, #1
  ldr x13, [x5, x9, LSL #3]
  smulh x11, x13, x10
  madd x10, x13, x10, xzr
  asr x13, x10, #63
  subs xzr, x11, x13
  b.ne label28 ; b label29
block29:
  ldr x13, [x12, x9, LSL #3]
  adds x11, x10, x13
  cset x13, vs
  ands wzr, w13, #255
  b.ne label30 ; b label31
block31:
  add x3, x0, #1
  movz x15, #3
  str x11, [x4, x3, LSL #3]
  add x9, x7, #2
  ldr x3, [x5, x9, LSL #3]
  smulh x11, x3, x15
  madd x10, x3, x15, xzr
  asr x3, x10, #63
  subs xzr, x11, x3
  b.ne label32 ; b label33
block33:
  ldr x3, [x12, x9, LSL #3]
  adds x9, x10, x3
  cset x3, vs
  ands wzr, w3, #255
  b.ne label34 ; b label35
block35:
  add x3, x0, #2
  movz x10, #3
  str x9, [x4, x3, LSL #3]
  add x9, x7, #3
  ldr x3, [x5, x9, LSL #3]
  smulh x11, x3, x10
  madd x10, x3, x10, xzr
  asr x3, x10, #63
  subs xzr, x11, x3
  b.ne label36 ; b label37
block37:
  ldr x3, [x12, x9, LSL #3]
  adds x9, x10, x3
  cset x3, vs
  ands wzr, w3, #255
  b.ne label38 ; b label39
block39:
  add x3, x0, #3
  str x9, [x4, x3, LSL #3]
  add x7, x7, #4
  add x0, x0, #4
  b label15
block41:
  sub x6, x1, x8
  movz x3, #64
  subs xzr, x6, x3
  csel x3, x6, x3, lo
  add x8, x8, x3
  b label1
block11:
  b label40
block13:
  b label40
block19:
  b label40
block21:
  b label40
block24:
  b label40
block26:
  b label40
block28:
  b label40
block30:
  b label40
block32:
  b label40
block34:
  b label40
block36:
  b label40
block38:
  b label40
block40:
  movn x0, #0
  ret
