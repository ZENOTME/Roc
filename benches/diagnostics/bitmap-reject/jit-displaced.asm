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
  sub sp, sp, #368
  unwind StackAlloc { size: 368 }
block0:
  str x1, [sp]
  str x2, [sp, #8]
  ldr x6, [x0]
  str x6, [sp, #352]
  ldr x0, [x0, #8]
  movz x6, #0
  mov x0, x6
  b label1
block1:
  sub x2, x1, x6
  subs xzr, x2, #64
  b.hs label8 ; b label2
block2:
  movz x14, #0
  cbnz x2, label3 ; b label4
block3:
  mov x2, x6
  b label5
block4:
  add sp, sp, #368
  ldp x19, x20, [sp], #16
  ldp x21, x22, [sp], #16
  ldp x23, x24, [sp], #16
  ldp x25, x26, [sp], #16
  ldp x27, x28, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block5:
  subs xzr, x2, x1
  str x1, [sp]
  b.lo label7 ; b label6
block6:
  lsr x1, x6, #3
  ldr x13, [sp, #8]
  str x14, [x13, x1]
  fmov d0, x14
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w1, v0.b[0]
  add x0, x0, x1
  add sp, sp, #368
  ldp x19, x20, [sp], #16
  ldp x21, x22, [sp], #16
  ldp x23, x24, [sp], #16
  ldp x25, x26, [sp], #16
  ldp x27, x28, [sp], #16
  ldp fp, lr, [sp], #16
  retabsp
block7:
  ldr x27, [sp, #352]
  str x0, [sp, #16]
  ldr x3, [x27, x2, LSL #3]
  add x1, x2, #1
  movn x4, #499
  subs xzr, x3, x4
  cset x3, lt
  uxtb w3, w3
  sub x2, x2, x6
  lsl x2, x3, x2
  orr x14, x14, x2
  mov x2, x1
  ldr x1, [sp]
  b label5
block8:
  ldr x27, [sp, #352]
  str x1, [sp]
  str x0, [sp, #16]
  add x5, x27, x6, LSL 3
  ldr x4, [x27, x6, LSL #3]
  ldr x28, [x5, #8]
  ldr x27, [x5, #16]
  ldr x3, [x5, #24]
  ldr x26, [x5, #32]
  ldr x25, [x5, #40]
  ldr x24, [x5, #48]
  ldr x23, [x5, #56]
  ldr x22, [x5, #64]
  ldr x21, [x5, #72]
  ldr x20, [x5, #80]
  ldr x19, [x5, #88]
  ldr x15, [x5, #96]
  ldr x14, [x5, #104]
  ldr x1, [x5, #112]
  ldr x13, [x5, #120]
  ldr x2, [x5, #128]
  ldr x12, [x5, #136]
  ldr x11, [x5, #144]
  ldr x10, [x5, #152]
  ldr x9, [x5, #160]
  ldr x8, [x5, #168]
  ldr x7, [x5, #176]
  ldr x0, [x5, #184]
  str x0, [sp, #344]
  ldr x0, [x5, #192]
  str x0, [sp, #336]
  ldr x0, [x5, #200]
  str x0, [sp, #328]
  ldr x0, [x5, #208]
  str x0, [sp, #320]
  ldr x0, [x5, #216]
  str x0, [sp, #312]
  ldr x0, [x5, #224]
  str x0, [sp, #304]
  ldr x0, [x5, #232]
  str x0, [sp, #296]
  ldr x0, [x5, #240]
  str x0, [sp, #288]
  ldr x0, [x5, #248]
  str x0, [sp, #280]
  ldr x0, [x5, #256]
  str x0, [sp, #272]
  ldr x0, [x5, #264]
  str x0, [sp, #264]
  ldr x0, [x5, #272]
  str x0, [sp, #256]
  ldr x0, [x5, #280]
  str x0, [sp, #248]
  ldr x0, [x5, #288]
  str x0, [sp, #240]
  ldr x0, [x5, #296]
  str x0, [sp, #232]
  ldr x0, [x5, #304]
  str x0, [sp, #224]
  ldr x0, [x5, #312]
  str x0, [sp, #216]
  ldr x0, [x5, #320]
  str x0, [sp, #208]
  ldr x0, [x5, #328]
  str x0, [sp, #200]
  ldr x0, [x5, #336]
  str x0, [sp, #192]
  ldr x0, [x5, #344]
  str x0, [sp, #184]
  ldr x0, [x5, #352]
  str x0, [sp, #176]
  ldr x0, [x5, #360]
  str x0, [sp, #168]
  ldr x0, [x5, #368]
  str x0, [sp, #160]
  ldr x0, [x5, #376]
  str x0, [sp, #152]
  ldr x0, [x5, #384]
  str x0, [sp, #144]
  ldr x0, [x5, #392]
  str x0, [sp, #136]
  ldr x0, [x5, #400]
  str x0, [sp, #128]
  ldr x0, [x5, #408]
  str x0, [sp, #120]
  ldr x0, [x5, #416]
  str x0, [sp, #112]
  ldr x0, [x5, #424]
  str x0, [sp, #104]
  ldr x0, [x5, #432]
  str x0, [sp, #96]
  ldr x0, [x5, #440]
  str x0, [sp, #88]
  ldr x0, [x5, #448]
  str x0, [sp, #80]
  ldr x0, [x5, #456]
  str x0, [sp, #72]
  ldr x0, [x5, #464]
  str x0, [sp, #64]
  ldr x0, [x5, #472]
  str x0, [sp, #56]
  ldr x0, [x5, #480]
  str x0, [sp, #48]
  ldr x0, [x5, #488]
  str x0, [sp, #40]
  ldr x0, [x5, #496]
  str x0, [sp, #32]
  ldr x0, [x5, #504]
  str x0, [sp, #24]
  movn x5, #499
  subs xzr, x4, x5
  cset x0, lt
  uxtb w4, w0
  subs xzr, x28, x5
  cset x0, lt
  uxtb w0, w0
  orr x4, x4, x0, LSL 1
  subs xzr, x27, x5
  cset x0, lt
  uxtb w0, w0
  orr x4, x4, x0, LSL 2
  subs xzr, x3, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x4, x0, LSL 3
  subs xzr, x26, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 4
  subs xzr, x25, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 5
  subs xzr, x24, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 6
  subs xzr, x23, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 7
  subs xzr, x22, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 8
  subs xzr, x21, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 9
  subs xzr, x20, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 10
  subs xzr, x19, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 11
  subs xzr, x15, x5
  cset x0, lt
  uxtb w0, w0
  orr x3, x3, x0, LSL 12
  subs xzr, x14, x5
  cset x4, lt
  uxtb w4, w4
  orr x3, x3, x4, LSL 13
  subs xzr, x1, x5
  cset x4, lt
  uxtb w4, w4
  orr x3, x3, x4, LSL 14
  subs xzr, x13, x5
  cset x4, lt
  uxtb w4, w4
  orr x3, x3, x4, LSL 15
  subs xzr, x2, x5
  cset x4, lt
  uxtb w4, w4
  orr x4, x3, x4, LSL 16
  subs xzr, x12, x5
  cset x12, lt
  uxtb w12, w12
  orr x12, x4, x12, LSL 17
  subs xzr, x11, x5
  cset x11, lt
  uxtb w11, w11
  orr x11, x12, x11, LSL 18
  subs xzr, x10, x5
  cset x10, lt
  uxtb w10, w10
  orr x10, x11, x10, LSL 19
  subs xzr, x9, x5
  cset x9, lt
  uxtb w9, w9
  orr x9, x10, x9, LSL 20
  subs xzr, x8, x5
  cset x10, lt
  uxtb w10, w10
  orr x9, x9, x10, LSL 21
  subs xzr, x7, x5
  cset x10, lt
  uxtb w10, w10
  orr x10, x9, x10, LSL 22
  ldr x0, [sp, #344]
  subs xzr, x0, x5
  cset x11, lt
  uxtb w11, w11
  orr x11, x10, x11, LSL 23
  ldr x4, [sp, #336]
  subs xzr, x4, x5
  cset x12, lt
  uxtb w12, w12
  orr x12, x11, x12, LSL 24
  ldr x28, [sp, #328]
  subs xzr, x28, x5
  cset x13, lt
  uxtb w13, w13
  orr x13, x12, x13, LSL 25
  ldr x3, [sp, #320]
  subs xzr, x3, x5
  cset x14, lt
  uxtb w14, w14
  orr x14, x13, x14, LSL 26
  ldr x26, [sp, #312]
  subs xzr, x26, x5
  cset x15, lt
  uxtb w15, w15
  orr x15, x14, x15, LSL 27
  ldr x25, [sp, #304]
  subs xzr, x25, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x15, x0, LSL 28
  ldr x24, [sp, #296]
  subs xzr, x24, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 29
  ldr x23, [sp, #288]
  subs xzr, x23, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 30
  ldr x22, [sp, #280]
  subs xzr, x22, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 31
  ldr x21, [sp, #272]
  subs xzr, x21, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 32
  ldr x20, [sp, #264]
  subs xzr, x20, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 33
  ldr x19, [sp, #256]
  subs xzr, x19, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 34
  ldr x15, [sp, #248]
  subs xzr, x15, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 35
  ldr x0, [sp, #240]
  subs xzr, x0, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 36
  ldr x2, [sp, #232]
  subs xzr, x2, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 37
  ldr x12, [sp, #224]
  subs xzr, x12, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 38
  ldr x11, [sp, #216]
  subs xzr, x11, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 39
  ldr x10, [sp, #208]
  subs xzr, x10, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 40
  ldr x9, [sp, #200]
  subs xzr, x9, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 41
  ldr x8, [sp, #192]
  subs xzr, x8, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 42
  ldr x7, [sp, #184]
  subs xzr, x7, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 43
  ldr x4, [sp, #176]
  subs xzr, x4, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 44
  ldr x28, [sp, #168]
  subs xzr, x28, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 45
  ldr x27, [sp, #160]
  subs xzr, x27, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 46
  ldr x26, [sp, #152]
  subs xzr, x26, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 47
  ldr x25, [sp, #144]
  subs xzr, x25, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 48
  ldr x24, [sp, #136]
  subs xzr, x24, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 49
  ldr x23, [sp, #128]
  subs xzr, x23, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 50
  ldr x22, [sp, #120]
  subs xzr, x22, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 51
  ldr x21, [sp, #112]
  subs xzr, x21, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 52
  ldr x20, [sp, #104]
  subs xzr, x20, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 53
  ldr x19, [sp, #96]
  subs xzr, x19, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 54
  ldr x15, [sp, #88]
  subs xzr, x15, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 55
  ldr x14, [sp, #80]
  subs xzr, x14, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 56
  ldr x13, [sp, #72]
  subs xzr, x13, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 57
  ldr x12, [sp, #64]
  subs xzr, x12, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 58
  ldr x11, [sp, #56]
  subs xzr, x11, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 59
  ldr x10, [sp, #48]
  subs xzr, x10, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 60
  ldr x9, [sp, #40]
  subs xzr, x9, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 61
  ldr x8, [sp, #32]
  subs xzr, x8, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 62
  ldr x7, [sp, #24]
  subs xzr, x7, x5
  cset x0, lt
  uxtb w0, w0
  orr x1, x1, x0, LSL 63
  lsr x0, x6, #3
  ldr x13, [sp, #8]
  str x1, [x13, x0]
  add x6, x6, #64
  fmov d0, x1
  cnt v0.8b, v0.8b
  addv b0, v0.8b
  umov w1, v0.b[0]
  ldr x0, [sp, #16]
  add x0, x0, x1
  ldr x1, [sp]
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
