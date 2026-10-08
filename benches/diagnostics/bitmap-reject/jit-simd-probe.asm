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
  ldr x9, [x0]
  ldr x10, [x0, #8]
  movz x6, #0
  mov x0, x6
  b label1
block1:
  sub x10, x1, x6
  subs xzr, x10, #64
  b.hs label8 ; b label2
block2:
  movz x14, #0
  cbnz x10, label3 ; b label4
block3:
  mov x4, x6
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
  subs xzr, x4, x1
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  str x14, [x2, x1]
  fmov d16, x14
  cnt v17.8b, v16.8b
  addv b19, v17.8b
  umov w1, v19.b[0]
  add x0, x0, x1
  add sp, sp, #16
  ldp d8, d9, [sp], #16
  ldp d10, d11, [sp], #16
  ldp d12, d13, [sp], #16
  ldp d14, d15, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x5, [x9, x4, LSL #3]
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
  add x3, x9, x6, LSL 3
  ldr q21, [x9, x4]
  ldr q15, [x3, #16]
  ldr q14, [x3, #32]
  ldr q13, [x3, #48]
  ldr q12, [x3, #64]
  ldr q11, [x3, #80]
  ldr q10, [x3, #96]
  ldr q9, [x3, #112]
  ldr q8, [x3, #128]
  ldr q20, [x3, #144]
  ldr q19, [x3, #160]
  ldr q18, [x3, #176]
  ldr q17, [x3, #192]
  ldr q16, [x3, #208]
  ldr q7, [x3, #224]
  ldr q6, [x3, #240]
  ldr q5, [x3, #256]
  ldr q4, [x3, #272]
  ldr q3, [x3, #288]
  ldr q2, [x3, #304]
  ldr q1, [x3, #320]
  ldr q0, [x3, #336]
  ldr q31, [x3, #352]
  ldr q30, [x3, #368]
  ldr q29, [x3, #384]
  ldr q28, [x3, #400]
  ldr q27, [x3, #416]
  ldr q26, [x3, #432]
  ldr q25, [x3, #448]
  ldr q24, [x3, #464]
  ldr q23, [x3, #480]
  ldr q22, [x3, #496]
  str q22, [sp]
  movn x3, #499
  dup v22.2d, x3
  cmgt v21.2d, v22.2d, v21.2d
  mov x3, v21.d[1]
  mov x4, v21.d[0]
  lsr x3, x3, #63
  lsr x4, x4, #63
  add x3, x4, x3, LSL 1
  uxtb w3, w3
  cmgt v21.2d, v22.2d, v15.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 2
  cmgt v21.2d, v22.2d, v14.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 4
  cmgt v21.2d, v22.2d, v13.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 6
  cmgt v21.2d, v22.2d, v12.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 8
  cmgt v21.2d, v22.2d, v11.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 10
  cmgt v21.2d, v22.2d, v10.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 12
  cmgt v21.2d, v22.2d, v9.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 14
  cmgt v21.2d, v22.2d, v8.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 16
  cmgt v21.2d, v22.2d, v20.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 18
  cmgt v21.2d, v22.2d, v19.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 20
  cmgt v21.2d, v22.2d, v18.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 22
  cmgt v21.2d, v22.2d, v17.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 24
  cmgt v21.2d, v22.2d, v16.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 26
  cmgt v21.2d, v22.2d, v7.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 28
  cmgt v21.2d, v22.2d, v6.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 30
  cmgt v21.2d, v22.2d, v5.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 32
  cmgt v21.2d, v22.2d, v4.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 34
  cmgt v21.2d, v22.2d, v3.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 36
  cmgt v21.2d, v22.2d, v2.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 38
  cmgt v21.2d, v22.2d, v1.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 40
  cmgt v21.2d, v22.2d, v0.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 42
  cmgt v21.2d, v22.2d, v31.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 44
  cmgt v21.2d, v22.2d, v30.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 46
  cmgt v21.2d, v22.2d, v29.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 48
  cmgt v21.2d, v22.2d, v28.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 50
  cmgt v21.2d, v22.2d, v27.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 52
  cmgt v21.2d, v22.2d, v26.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 54
  cmgt v21.2d, v22.2d, v25.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 56
  cmgt v21.2d, v22.2d, v24.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 58
  cmgt v21.2d, v22.2d, v23.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 60
  ldr q23, [sp]
  cmgt v21.2d, v22.2d, v23.2d
  mov x4, v21.d[1]
  mov x5, v21.d[0]
  lsr x4, x4, #63
  lsr x5, x5, #63
  add x4, x5, x4, LSL 1
  uxtb w4, w4
  orr x3, x3, x4, LSL 62
  lsr x4, x6, #3
  str x3, [x2, x4]
  add x6, x6, #64
  fmov d19, x3
  cnt v21.8b, v19.8b
  addv b23, v21.8b
  umov w3, v23.b[0]
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
