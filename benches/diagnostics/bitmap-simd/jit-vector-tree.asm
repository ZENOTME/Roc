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
  sub sp, sp, #32
  unwind StackAlloc { size: 32 }
block0:
  ldr x10, [x0]
  ldr x11, [x0, #8]
  movz x6, #0
  mov x0, x6
  b label1
block1:
  sub x11, x1, x6
  subs xzr, x11, #64
  b.hs label8 ; b label2
block2:
  movz x14, #0
  cbnz x11, label3 ; b label4
block3:
  mov x4, x6
  b label5
block4:
  add sp, sp, #32
  ldp d8, d9, [sp], #16
  ldp d10, d11, [sp], #16
  ldp d12, d13, [sp], #16
  ldp d14, d15, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block5:
  subs xzr, x4, x1
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  str x14, [x2, x1]
  fmov d16, x14
  cnt v18.8b, v16.8b
  addv b20, v18.8b
  umov w1, v20.b[0]
  add x0, x0, x1
  add sp, sp, #32
  ldp d8, d9, [sp], #16
  ldp d10, d11, [sp], #16
  ldp d12, d13, [sp], #16
  ldp d14, d15, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x5, [x10, x4, LSL #3]
  add x3, x4, #1
  movn x7, #499
  subs xzr, x5, x7
  cset x5, lt
  uxtb w5, w5
  sub x4, x4, x6
  lsl x4, x5, x4
  orr x14, x14, x4
  mov x4, x3
  b label5
block8:
  lsl x4, x6, #3
  add x3, x10, x6, LSL 3
  ldr q22, [x10, x4]
  ldr q15, [x3, #16]
  ldr q14, [x3, #32]
  ldr q13, [x3, #48]
  ldr q12, [x3, #64]
  ldr q11, [x3, #80]
  ldr q10, [x3, #96]
  ldr q9, [x3, #112]
  ldr q8, [x3, #128]
  ldr q21, [x3, #144]
  ldr q20, [x3, #160]
  ldr q19, [x3, #176]
  ldr q18, [x3, #192]
  ldr q17, [x3, #208]
  ldr q16, [x3, #224]
  ldr q7, [x3, #240]
  ldr q6, [x3, #256]
  ldr q5, [x3, #272]
  ldr q4, [x3, #288]
  ldr q3, [x3, #304]
  ldr q2, [x3, #320]
  ldr q1, [x3, #336]
  ldr q0, [x3, #352]
  ldr q31, [x3, #368]
  ldr q30, [x3, #384]
  ldr q29, [x3, #400]
  ldr q28, [x3, #416]
  ldr q27, [x3, #432]
  ldr q26, [x3, #448]
  ldr q25, [x3, #464]
  ldr q23, [x3, #480]
  str q23, [sp, #16]
  ldr q24, [x3, #496]
  str q24, [sp]
  movn x3, #499
  dup v24.2d, x3
  cmgt v22.2d, v24.2d, v22.2d
  ldr q23, [const(31)]
  and v22.16b, v22.16b, v23.16b
  cmgt v15.2d, v24.2d, v15.2d
  ldr q23, [const(30)]
  and v23.16b, v15.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v14.2d, v24.2d, v14.2d
  ldr q23, [const(29)]
  and v14.16b, v14.16b, v23.16b
  cmgt v13.2d, v24.2d, v13.2d
  ldr q23, [const(28)]
  and v23.16b, v13.16b, v23.16b
  orr v23.16b, v14.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v12.2d, v24.2d, v12.2d
  ldr q23, [const(27)]
  and v12.16b, v12.16b, v23.16b
  cmgt v11.2d, v24.2d, v11.2d
  ldr q23, [const(26)]
  and v23.16b, v11.16b, v23.16b
  orr v11.16b, v12.16b, v23.16b
  cmgt v10.2d, v24.2d, v10.2d
  ldr q23, [const(25)]
  and v10.16b, v10.16b, v23.16b
  cmgt v9.2d, v24.2d, v9.2d
  ldr q23, [const(24)]
  and v23.16b, v9.16b, v23.16b
  orr v23.16b, v10.16b, v23.16b
  orr v23.16b, v11.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v8.2d, v24.2d, v8.2d
  ldr q23, [const(23)]
  and v8.16b, v8.16b, v23.16b
  cmgt v21.2d, v24.2d, v21.2d
  ldr q23, [const(22)]
  and v23.16b, v21.16b, v23.16b
  orr v21.16b, v8.16b, v23.16b
  cmgt v20.2d, v24.2d, v20.2d
  ldr q23, [const(21)]
  and v20.16b, v20.16b, v23.16b
  cmgt v19.2d, v24.2d, v19.2d
  ldr q23, [const(20)]
  and v23.16b, v19.16b, v23.16b
  orr v23.16b, v20.16b, v23.16b
  orr v19.16b, v21.16b, v23.16b
  cmgt v18.2d, v24.2d, v18.2d
  ldr q23, [const(19)]
  and v18.16b, v18.16b, v23.16b
  cmgt v17.2d, v24.2d, v17.2d
  ldr q23, [const(18)]
  and v23.16b, v17.16b, v23.16b
  orr v17.16b, v18.16b, v23.16b
  cmgt v16.2d, v24.2d, v16.2d
  ldr q23, [const(17)]
  and v16.16b, v16.16b, v23.16b
  cmgt v7.2d, v24.2d, v7.2d
  ldr q23, [const(16)]
  and v23.16b, v7.16b, v23.16b
  orr v23.16b, v16.16b, v23.16b
  orr v23.16b, v17.16b, v23.16b
  orr v23.16b, v19.16b, v23.16b
  orr v7.16b, v22.16b, v23.16b
  cmgt v22.2d, v24.2d, v6.2d
  ldr q23, [const(15)]
  and v22.16b, v22.16b, v23.16b
  cmgt v5.2d, v24.2d, v5.2d
  ldr q23, [const(14)]
  and v23.16b, v5.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v4.2d, v24.2d, v4.2d
  ldr q23, [const(13)]
  and v4.16b, v4.16b, v23.16b
  cmgt v3.2d, v24.2d, v3.2d
  ldr q23, [const(12)]
  and v23.16b, v3.16b, v23.16b
  orr v23.16b, v4.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v2.2d, v24.2d, v2.2d
  ldr q23, [const(11)]
  and v2.16b, v2.16b, v23.16b
  cmgt v1.2d, v24.2d, v1.2d
  ldr q23, [const(10)]
  and v23.16b, v1.16b, v23.16b
  orr v1.16b, v2.16b, v23.16b
  cmgt v0.2d, v24.2d, v0.2d
  ldr q23, [const(9)]
  and v0.16b, v0.16b, v23.16b
  cmgt v31.2d, v24.2d, v31.2d
  ldr q23, [const(8)]
  and v23.16b, v31.16b, v23.16b
  orr v23.16b, v0.16b, v23.16b
  orr v23.16b, v1.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  cmgt v30.2d, v24.2d, v30.2d
  ldr q23, [const(7)]
  and v30.16b, v30.16b, v23.16b
  cmgt v29.2d, v24.2d, v29.2d
  ldr q23, [const(6)]
  and v23.16b, v29.16b, v23.16b
  orr v29.16b, v30.16b, v23.16b
  cmgt v28.2d, v24.2d, v28.2d
  ldr q23, [const(5)]
  and v28.16b, v28.16b, v23.16b
  cmgt v27.2d, v24.2d, v27.2d
  ldr q23, [const(4)]
  and v23.16b, v27.16b, v23.16b
  orr v23.16b, v28.16b, v23.16b
  orr v27.16b, v29.16b, v23.16b
  cmgt v26.2d, v24.2d, v26.2d
  ldr q23, [const(3)]
  and v26.16b, v26.16b, v23.16b
  cmgt v25.2d, v24.2d, v25.2d
  ldr q23, [const(2)]
  and v23.16b, v25.16b, v23.16b
  orr v25.16b, v26.16b, v23.16b
  ldr q6, [sp, #16]
  cmgt v23.2d, v24.2d, v6.2d
  ldr q26, [const(1)]
  and v23.16b, v23.16b, v26.16b
  ldr q16, [sp]
  cmgt v24.2d, v24.2d, v16.2d
  ldr q26, [const(0)]
  and v24.16b, v24.16b, v26.16b
  orr v23.16b, v23.16b, v24.16b
  orr v23.16b, v25.16b, v23.16b
  orr v23.16b, v27.16b, v23.16b
  orr v22.16b, v22.16b, v23.16b
  orr v22.16b, v7.16b, v22.16b
  mov x3, v22.d[0]
  mov x4, v22.d[1]
  orr x3, x3, x4
  lsr x4, x6, #3
  str x3, [x2, x4]
  add x6, x6, #64
  fmov d20, x3
  cnt v22.8b, v20.8b
  addv b24, v22.8b
  umov w3, v24.b[0]
  add x0, x0, x3
  b label1

; bitmap_project
  unwind Aarch64SetPointerAuth { return_addresses: false }
block0:
  ldr x5, [x0]
  ldr x11, [x0, #8]
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
  ldr x6, [x2, x3]
  cbz x6, label4 ; b label5
block4:
  b label23
block5:
  movn x7, #0
  movz x9, #64
  sub x3, x1, x8
  subs xzr, x3, x9
  csel x10, x3, x9, lo
  sub x3, x9, x10
  lsr x3, x7, x3
  add x7, x8, x10
  subs xzr, x6, x3
  b.eq label6 ; b label7
block6:
  mov x6, x8
  b label15
block7:
  b label8
block8:
  cbnz x6, label10 ; b label9
block9:
  b label23
block10:
  rbit x3, x6
  clz x7, x3
  add x7, x8, x7
  movz x10, #3
  ldr x3, [x5, x7, LSL #3]
  madd x9, x3, x10, xzr
  smulh x3, x3, x10
  subs xzr, x3, x9, ASR 63
  cset x10, ne
  ands wzr, w10, #255
  b.ne label11 ; b label12
block12:
  ldr x7, [x11, x7, LSL #3]
  adds x7, x9, x7
  cset x9, vs
  ands wzr, w9, #255
  b.ne label13 ; b label14
block14:
  str x7, [x4, x0, LSL #3]
  sub x12, x6, #1
  and x6, x6, x12
  add x0, x0, #1
  b label8
block15:
  subs xzr, x6, x7
  b.lo label17 ; b label16
block16:
  b label23
block17:
  movz x15, #3
  ldr x3, [x5, x6, LSL #3]
  madd x13, x3, x15, xzr
  smulh x15, x3, x15
  subs xzr, x15, x13, ASR 63
  cset x3, ne
  ands wzr, w3, #255
  b.ne label18 ; b label19
block19:
  ldr x3, [x11, x6, LSL #3]
  adds x9, x13, x3
  cset x3, vs
  ands wzr, w3, #255
  b.ne label20 ; b label21
block21:
  str x9, [x4, x0, LSL #3]
  add x6, x6, #1
  add x0, x0, #1
  b label15
block23:
  sub x6, x1, x8
  movz x3, #64
  subs xzr, x6, x3
  csel x3, x6, x3, lo
  add x8, x8, x3
  b label1
block11:
  b label22
block13:
  b label22
block18:
  b label22
block20:
  b label22
block22:
  movn x0, #0
  ret
