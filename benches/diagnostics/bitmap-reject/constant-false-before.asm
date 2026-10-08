; bitmap_predicate
  unwind Aarch64SetPointerAuth { return_addresses: false }
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
  mov x4, x6
  b label5
block4:
  ret
block5:
  subs xzr, x4, x1
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  str x14, [x2, x1]
  fmov d0, x14
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w1, v0.b[0]
  add x0, x0, x1
  ret
block7:
  ldr x5, [x3, x4, LSL #3]
  add x4, x4, #1
  b label5
block8:
  ldr x4, [x3, x6, LSL #3]
  movz x4, #8
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #16
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #24
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #32
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #40
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #48
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #56
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #64
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #72
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #80
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #88
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #96
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #104
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #112
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #120
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #128
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #136
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #144
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #152
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #160
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #168
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #176
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #184
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #192
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #200
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #208
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #216
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #224
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #232
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #240
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #248
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #256
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #264
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #272
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #280
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #288
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #296
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #304
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #312
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #320
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #328
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #336
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #344
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #352
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #360
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #368
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #376
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #384
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #392
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #400
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #408
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #416
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #424
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #432
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #440
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #448
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #456
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #464
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #472
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #480
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #488
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #496
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #504
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x4, #0
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
