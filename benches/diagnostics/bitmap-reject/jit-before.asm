; bitmap_predicate
  pacibsp
  unwind Aarch64SetPointerAuth { return_addresses: true }
  stp fp, lr, [sp, #-16]!
  unwind PushFrameRegs { offset_upward_to_caller_sp: 16 }
  mov fp, sp
  unwind DefineNewFrame { offset_upward_to_caller_sp: 16, offset_downward_to_clobbers: 80 }
  stp x27, x28, [sp, #-16]!
  unwind SaveReg { clobber_offset: 64, reg: p27i }
  unwind SaveReg { clobber_offset: 72, reg: p28i }
  stp x25, x26, [sp, #-16]!
  unwind SaveReg { clobber_offset: 48, reg: p25i }
  unwind SaveReg { clobber_offset: 56, reg: p26i }
  stp x23, x24, [sp, #-16]!
  unwind SaveReg { clobber_offset: 32, reg: p23i }
  unwind SaveReg { clobber_offset: 40, reg: p24i }
  stp x21, x22, [sp, #-16]!
  unwind SaveReg { clobber_offset: 16, reg: p21i }
  unwind SaveReg { clobber_offset: 24, reg: p22i }
  stp x19, x20, [sp, #-16]!
  unwind SaveReg { clobber_offset: 0, reg: p19i }
  unwind SaveReg { clobber_offset: 8, reg: p20i }
  sub sp, sp, #352
  unwind StackAlloc { size: 352 }
block0:
  str x1, [sp]
  str x2, [sp, #8]
  ldr x3, [x0]
  ldr x0, [x0, #8]
  movz x6, #0
  mov x0, x6
  ldr x2, [sp]
  b label1
block1:
  sub x1, x2, x6
  str x2, [sp]
  subs xzr, x1, #64
  b.hs label8 ; b label2
block2:
  movz x14, #0
  cbnz x1, label3 ; b label4
block3:
  mov x2, x6
  ldr x4, [sp]
  b label5
block4:
  add sp, sp, #352
  ldp x19, x20, [sp], #16
  ldp x21, x22, [sp], #16
  ldp x23, x24, [sp], #16
  ldp x25, x26, [sp], #16
  ldp x27, x28, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block5:
  subs xzr, x2, x4
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  ldr x4, [sp, #8]
  str x14, [x4, x1]
  fmov d0, x14
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w1, v0.b[0]
  add x0, x0, x1
  add sp, sp, #352
  ldp x19, x20, [sp], #16
  ldp x21, x22, [sp], #16
  ldp x23, x24, [sp], #16
  ldp x25, x26, [sp], #16
  ldp x27, x28, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x4, [x3, x2, LSL #3]
  add x1, x2, #1
  movn x5, #499
  subs xzr, x4, x5
  cset x4, lt
  uxtb w4, w4
  sub x2, x2, x6
  lsl x2, x4, x2
  orr x14, x14, x2
  mov x2, x1
  ldr x4, [sp]
  b label5
block8:
  ldr x28, [x3, x6, LSL #3]
  movz x1, #8
  add x1, x1, x6, LSL 3
  ldr x2, [x3, x1]
  movz x1, #16
  add x1, x1, x6, LSL 3
  ldr x27, [x3, x1]
  movz x1, #24
  add x1, x1, x6, LSL 3
  ldr x1, [x3, x1]
  movz x4, #32
  add x4, x4, x6, LSL 3
  ldr x26, [x3, x4]
  movz x4, #40
  add x4, x4, x6, LSL 3
  ldr x25, [x3, x4]
  movz x4, #48
  add x4, x4, x6, LSL 3
  ldr x24, [x3, x4]
  movz x4, #56
  add x4, x4, x6, LSL 3
  ldr x23, [x3, x4]
  movz x4, #64
  add x4, x4, x6, LSL 3
  ldr x22, [x3, x4]
  movz x4, #72
  add x4, x4, x6, LSL 3
  ldr x21, [x3, x4]
  movz x4, #80
  add x4, x4, x6, LSL 3
  ldr x20, [x3, x4]
  movz x4, #88
  add x4, x4, x6, LSL 3
  ldr x19, [x3, x4]
  movz x4, #96
  add x4, x4, x6, LSL 3
  ldr x15, [x3, x4]
  movz x4, #104
  add x4, x4, x6, LSL 3
  ldr x14, [x3, x4]
  movz x4, #112
  add x4, x4, x6, LSL 3
  ldr x13, [x3, x4]
  movz x4, #120
  add x4, x4, x6, LSL 3
  ldr x12, [x3, x4]
  movz x4, #128
  add x4, x4, x6, LSL 3
  ldr x11, [x3, x4]
  movz x4, #136
  add x4, x4, x6, LSL 3
  ldr x10, [x3, x4]
  movz x4, #144
  add x4, x4, x6, LSL 3
  ldr x9, [x3, x4]
  movz x4, #152
  add x4, x4, x6, LSL 3
  ldr x8, [x3, x4]
  movz x4, #160
  add x4, x4, x6, LSL 3
  ldr x5, [x3, x4]
  movz x4, #168
  add x4, x4, x6, LSL 3
  ldr x4, [x3, x4]
  movz x7, #176
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #344]
  movz x7, #184
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #336]
  movz x7, #192
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #328]
  movz x7, #200
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #320]
  movz x7, #208
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #312]
  movz x7, #216
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #304]
  movz x7, #224
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #296]
  movz x7, #232
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #288]
  movz x7, #240
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #280]
  movz x7, #248
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #272]
  movz x7, #256
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #264]
  movz x7, #264
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #256]
  movz x7, #272
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #248]
  movz x7, #280
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #240]
  movz x7, #288
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #232]
  movz x7, #296
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #224]
  movz x7, #304
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #216]
  movz x7, #312
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #208]
  movz x7, #320
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #200]
  movz x7, #328
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #192]
  movz x7, #336
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #184]
  movz x7, #344
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #176]
  movz x7, #352
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #168]
  movz x7, #360
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #160]
  movz x7, #368
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #152]
  movz x7, #376
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #144]
  movz x7, #384
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #136]
  movz x7, #392
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #128]
  movz x7, #400
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #120]
  movz x7, #408
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #112]
  movz x7, #416
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #104]
  movz x7, #424
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #96]
  movz x7, #432
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #88]
  movz x7, #440
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #80]
  movz x7, #448
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #72]
  movz x7, #456
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #64]
  movz x7, #464
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #56]
  movz x7, #472
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #48]
  movz x7, #480
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #40]
  movz x7, #488
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #32]
  movz x7, #496
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #24]
  movz x7, #504
  add x7, x7, x6, LSL 3
  ldr x7, [x3, x7]
  str x7, [sp, #16]
  movn x7, #499
  subs xzr, x28, x7
  cset x28, lt
  uxtb w28, w28
  subs xzr, x2, x7
  cset x2, lt
  uxtb w2, w2
  orr x2, x28, x2, LSL 1
  subs xzr, x27, x7
  cset x27, lt
  uxtb w27, w27
  orr x2, x2, x27, LSL 2
  subs xzr, x1, x7
  cset x1, lt
  uxtb w1, w1
  orr x1, x2, x1, LSL 3
  subs xzr, x26, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 4
  subs xzr, x25, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 5
  subs xzr, x24, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 6
  subs xzr, x23, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 7
  subs xzr, x22, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 8
  subs xzr, x21, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 9
  subs xzr, x20, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 10
  subs xzr, x19, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 11
  subs xzr, x15, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 12
  subs xzr, x14, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 13
  subs xzr, x13, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 14
  subs xzr, x12, x7
  cset x2, lt
  uxtb w2, w2
  orr x2, x1, x2, LSL 15
  subs xzr, x11, x7
  cset x11, lt
  uxtb w11, w11
  orr x11, x2, x11, LSL 16
  subs xzr, x10, x7
  cset x10, lt
  uxtb w10, w10
  orr x10, x11, x10, LSL 17
  subs xzr, x9, x7
  cset x9, lt
  uxtb w9, w9
  orr x9, x10, x9, LSL 18
  subs xzr, x8, x7
  cset x8, lt
  uxtb w8, w8
  orr x8, x9, x8, LSL 19
  subs xzr, x5, x7
  cset x9, lt
  uxtb w9, w9
  orr x8, x8, x9, LSL 20
  subs xzr, x4, x7
  cset x9, lt
  uxtb w9, w9
  orr x8, x8, x9, LSL 21
  ldr x4, [sp, #344]
  subs xzr, x4, x7
  cset x9, lt
  uxtb w9, w9
  orr x9, x8, x9, LSL 22
  ldr x27, [sp, #336]
  subs xzr, x27, x7
  cset x10, lt
  uxtb w10, w10
  orr x10, x9, x10, LSL 23
  ldr x2, [sp, #328]
  subs xzr, x2, x7
  cset x11, lt
  uxtb w11, w11
  orr x11, x10, x11, LSL 24
  ldr x28, [sp, #320]
  subs xzr, x28, x7
  cset x12, lt
  uxtb w12, w12
  orr x12, x11, x12, LSL 25
  ldr x1, [sp, #312]
  subs xzr, x1, x7
  cset x13, lt
  uxtb w13, w13
  orr x13, x12, x13, LSL 26
  ldr x26, [sp, #304]
  subs xzr, x26, x7
  cset x14, lt
  uxtb w14, w14
  orr x14, x13, x14, LSL 27
  ldr x25, [sp, #296]
  subs xzr, x25, x7
  cset x15, lt
  uxtb w15, w15
  orr x15, x14, x15, LSL 28
  ldr x24, [sp, #288]
  subs xzr, x24, x7
  cset x1, lt
  uxtb w1, w1
  orr x1, x15, x1, LSL 29
  ldr x23, [sp, #280]
  subs xzr, x23, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 30
  ldr x22, [sp, #272]
  subs xzr, x22, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 31
  ldr x21, [sp, #264]
  subs xzr, x21, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 32
  ldr x20, [sp, #256]
  subs xzr, x20, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 33
  ldr x19, [sp, #248]
  subs xzr, x19, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 34
  ldr x15, [sp, #240]
  subs xzr, x15, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 35
  ldr x14, [sp, #232]
  subs xzr, x14, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 36
  ldr x13, [sp, #224]
  subs xzr, x13, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 37
  ldr x12, [sp, #216]
  subs xzr, x12, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 38
  ldr x11, [sp, #208]
  subs xzr, x11, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 39
  ldr x10, [sp, #200]
  subs xzr, x10, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 40
  ldr x9, [sp, #192]
  subs xzr, x9, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 41
  ldr x8, [sp, #184]
  subs xzr, x8, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 42
  ldr x5, [sp, #176]
  subs xzr, x5, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 43
  ldr x2, [sp, #168]
  subs xzr, x2, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 44
  ldr x28, [sp, #160]
  subs xzr, x28, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 45
  ldr x27, [sp, #152]
  subs xzr, x27, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 46
  ldr x26, [sp, #144]
  subs xzr, x26, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 47
  ldr x25, [sp, #136]
  subs xzr, x25, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 48
  ldr x24, [sp, #128]
  subs xzr, x24, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 49
  ldr x23, [sp, #120]
  subs xzr, x23, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 50
  ldr x22, [sp, #112]
  subs xzr, x22, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 51
  ldr x21, [sp, #104]
  subs xzr, x21, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 52
  ldr x20, [sp, #96]
  subs xzr, x20, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 53
  ldr x19, [sp, #88]
  subs xzr, x19, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 54
  ldr x15, [sp, #80]
  subs xzr, x15, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 55
  ldr x14, [sp, #72]
  subs xzr, x14, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 56
  ldr x13, [sp, #64]
  subs xzr, x13, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 57
  ldr x12, [sp, #56]
  subs xzr, x12, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 58
  ldr x11, [sp, #48]
  subs xzr, x11, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 59
  ldr x10, [sp, #40]
  subs xzr, x10, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 60
  ldr x9, [sp, #32]
  subs xzr, x9, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 61
  ldr x8, [sp, #24]
  subs xzr, x8, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 62
  ldr x5, [sp, #16]
  subs xzr, x5, x7
  cset x2, lt
  uxtb w2, w2
  orr x1, x1, x2, LSL 63
  lsr x2, x6, #3
  ldr x4, [sp, #8]
  str x1, [x4, x2]
  add x6, x6, #64
  fmov d0, x1
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w1, v0.b[0]
  add x0, x0, x1
  ldr x2, [sp]
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
