; bitmap_predicate
  pacibsp
  unwind Aarch64SetPointerAuth { return_addresses: true }
  stp fp, lr, [sp, #-16]!
  unwind PushFrameRegs { offset_upward_to_caller_sp: 16 }
  mov fp, sp
  unwind DefineNewFrame { offset_upward_to_caller_sp: 16, offset_downward_to_clobbers: 16 }
  str x19, [sp, #-16]!
  unwind SaveReg { clobber_offset: 0, reg: p19i }
block0:
  ldr x3, [x0]
  ldr x0, [x0, #8]
  movz x6, #0
  mov x0, x6
  b label1
block1:
  sub x5, x1, x6
  subs xzr, x5, #64
  b.hs label8 ; b label2
block2:
  movz x4, #0
  cbnz x5, label3 ; b label4
block3:
  mov x7, x6
  b label5
block4:
  ldr x19, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block5:
  subs xzr, x7, x1
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  str x4, [x2, x1]
  fmov d29, x4
  cnt v31.8b, v29.8b
  addv b0, v31.8b
  umov w1, v0.b[0]
  add x0, x0, x1
  ldr x19, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x8, [x3, x7, LSL #3]
  add x5, x7, #1
  movn x9, #499
  subs xzr, x8, x9
  cset x8, lt
  uxtb w8, w8
  sub x7, x7, x6
  lsl x7, x8, x7
  orr x4, x4, x7
  mov x7, x5
  b label5
block8:
  movz x7, #0
  movn x5, #499
  mov x4, x7
  b label9
block9:
  add x8, x6, x7
  ldr x15, [x3, x8, LSL #3]
  movz x9, #8
  add x9, x9, x8, LSL 3
  ldr x19, [x3, x9]
  movz x9, #16
  add x9, x9, x8, LSL 3
  ldr x14, [x3, x9]
  movz x9, #24
  add x9, x9, x8, LSL 3
  ldr x13, [x3, x9]
  movz x9, #32
  add x9, x9, x8, LSL 3
  ldr x12, [x3, x9]
  movz x9, #40
  add x9, x9, x8, LSL 3
  ldr x11, [x3, x9]
  movz x9, #48
  add x9, x9, x8, LSL 3
  ldr x10, [x3, x9]
  movz x9, #56
  add x8, x9, x8, LSL 3
  ldr x9, [x3, x8]
  add x8, x7, #8
  subs xzr, x15, x5
  cset x15, lt
  uxtb w15, w15
  subs xzr, x19, x5
  cset x19, lt
  uxtb w19, w19
  orr x15, x15, x19, LSL 1
  subs xzr, x14, x5
  cset x14, lt
  uxtb w14, w14
  orr x14, x15, x14, LSL 2
  subs xzr, x13, x5
  cset x13, lt
  uxtb w13, w13
  orr x13, x14, x13, LSL 3
  subs xzr, x12, x5
  cset x12, lt
  uxtb w12, w12
  orr x12, x13, x12, LSL 4
  subs xzr, x11, x5
  cset x11, lt
  uxtb w11, w11
  orr x11, x12, x11, LSL 5
  subs xzr, x10, x5
  cset x10, lt
  uxtb w10, w10
  orr x10, x11, x10, LSL 6
  subs xzr, x9, x5
  cset x9, lt
  uxtb w9, w9
  orr x9, x10, x9, LSL 7
  lsl x7, x9, x7
  orr x4, x4, x7
  subs xzr, x8, #64
  b.lo label10 ; b label11
block10:
  mov x7, x8
  b label9
block11:
  lsr x5, x6, #3
  str x4, [x2, x5]
  add x6, x6, #64
  fmov d0, x4
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w4, v0.b[0]
  add x0, x0, x4
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
