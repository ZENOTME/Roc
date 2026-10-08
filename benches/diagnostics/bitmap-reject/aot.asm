
target/release/deps/query_compilation-dbbfb9da2f1c62bd:	file format mach-o arm64

Disassembly of section __TEXT,__text:

0000000100017de4 <_roc_bitmap_aot_reject>:
100017de4: d10583ff    	sub	sp, sp, #0x160
100017de8: 6d113bef    	stp	d15, d14, [sp, #0x110]
100017dec: 6d1233ed    	stp	d13, d12, [sp, #0x120]
100017df0: 6d132beb    	stp	d11, d10, [sp, #0x130]
100017df4: 6d1423e9    	stp	d9, d8, [sp, #0x140]
100017df8: a9156ffc    	stp	x28, x27, [sp, #0x150]
100017dfc: f9400008    	ldr	x8, [x0]
100017e00: f101003f    	cmp	x1, #0x40
100017e04: 54001ec3    	b.lo	0x1000181dc <_roc_bitmap_aot_reject+0x3f8>
100017e08: d2800009    	mov	x9, #0x0                ; =0
100017e0c: d2800000    	mov	x0, #0x0                ; =0
100017e10: 92803e6a    	mov	x10, #-0x1f4            ; =-500
100017e14: 4e080d40    	dup.2d	v0, x10
100017e18: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e1c: 3dc10141    	ldr	q1, [x10, #0x400]
100017e20: 3d8043e1    	str	q1, [sp, #0x100]
100017e24: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e28: 3dc10541    	ldr	q1, [x10, #0x410]
100017e2c: 3d803fe1    	str	q1, [sp, #0xf0]
100017e30: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e34: 3dc10941    	ldr	q1, [x10, #0x420]
100017e38: 3d803be1    	str	q1, [sp, #0xe0]
100017e3c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e40: 3dc10d41    	ldr	q1, [x10, #0x430]
100017e44: 3d8037e1    	str	q1, [sp, #0xd0]
100017e48: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e4c: 3dc11141    	ldr	q1, [x10, #0x440]
100017e50: 3d8033e1    	str	q1, [sp, #0xc0]
100017e54: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e58: 3dc11541    	ldr	q1, [x10, #0x450]
100017e5c: 3d802fe1    	str	q1, [sp, #0xb0]
100017e60: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e64: 3dc11941    	ldr	q1, [x10, #0x460]
100017e68: 3d802be1    	str	q1, [sp, #0xa0]
100017e6c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e70: 3dc11d41    	ldr	q1, [x10, #0x470]
100017e74: 3d8027e1    	str	q1, [sp, #0x90]
100017e78: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e7c: 3dc12141    	ldr	q1, [x10, #0x480]
100017e80: 3d8023e1    	str	q1, [sp, #0x80]
100017e84: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e88: 3dc12541    	ldr	q1, [x10, #0x490]
100017e8c: 3d801fe1    	str	q1, [sp, #0x70]
100017e90: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017e94: 3dc12941    	ldr	q1, [x10, #0x4a0]
100017e98: 3d801be1    	str	q1, [sp, #0x60]
100017e9c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ea0: 3dc12d41    	ldr	q1, [x10, #0x4b0]
100017ea4: 3d8017e1    	str	q1, [sp, #0x50]
100017ea8: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017eac: 3dc13141    	ldr	q1, [x10, #0x4c0]
100017eb0: 3d8013e1    	str	q1, [sp, #0x40]
100017eb4: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017eb8: 3dc13541    	ldr	q1, [x10, #0x4d0]
100017ebc: 3d800fe1    	str	q1, [sp, #0x30]
100017ec0: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ec4: 3dc13941    	ldr	q1, [x10, #0x4e0]
100017ec8: 3d800be1    	str	q1, [sp, #0x20]
100017ecc: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ed0: 3dc13d41    	ldr	q1, [x10, #0x4f0]
100017ed4: 3d8007e1    	str	q1, [sp, #0x10]
100017ed8: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017edc: 3dc14141    	ldr	q1, [x10, #0x500]
100017ee0: 3d8003e1    	str	q1, [sp]
100017ee4: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ee8: 3dc1455a    	ldr	q26, [x10, #0x510]
100017eec: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ef0: 3dc1495b    	ldr	q27, [x10, #0x520]
100017ef4: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017ef8: 3dc14d5c    	ldr	q28, [x10, #0x530]
100017efc: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f00: 3dc1515d    	ldr	q29, [x10, #0x540]
100017f04: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f08: 3dc1555e    	ldr	q30, [x10, #0x550]
100017f0c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f10: 3dc1595f    	ldr	q31, [x10, #0x560]
100017f14: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f18: 3dc15d48    	ldr	q8, [x10, #0x570]
100017f1c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f20: 3dc16149    	ldr	q9, [x10, #0x580]
100017f24: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f28: 3dc1654a    	ldr	q10, [x10, #0x590]
100017f2c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f30: 3dc1694b    	ldr	q11, [x10, #0x5a0]
100017f34: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f38: 3dc16d4c    	ldr	q12, [x10, #0x5b0]
100017f3c: b00052ea    	adrp	x10, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f40: 3dc1714d    	ldr	q13, [x10, #0x5c0]
100017f44: d2f0000a    	mov	x10, #-0x8000000000000000 ; =-9223372036854775808
100017f48: b00052eb    	adrp	x11, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f4c: 3dc1756e    	ldr	q14, [x11, #0x5d0]
100017f50: aa0803eb    	mov	x11, x8
100017f54: b00052ec    	adrp	x12, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100017f58: 3dc1798f    	ldr	q15, [x12, #0x5e0]
100017f5c: aa0203ec    	mov	x12, x2
100017f60: 3cce8161    	ldur	q1, [x11, #0xe8]
100017f64: 3cc58162    	ldur	q2, [x11, #0x58]
100017f68: 3ccd8163    	ldur	q3, [x11, #0xd8]
100017f6c: 3cc18164    	ldur	q4, [x11, #0x18]
100017f70: 3cc98165    	ldur	q5, [x11, #0x98]
100017f74: 3cc38166    	ldur	q6, [x11, #0x38]
100017f78: 3ccb8167    	ldur	q7, [x11, #0xb8]
100017f7c: 3cc78170    	ldur	q16, [x11, #0x78]
100017f80: 4ef03410    	cmgt.2d	v16, v0, v16
100017f84: 4ee73407    	cmgt.2d	v7, v0, v7
100017f88: 4ee63406    	cmgt.2d	v6, v0, v6
100017f8c: 4ee53405    	cmgt.2d	v5, v0, v5
100017f90: 4ee43404    	cmgt.2d	v4, v0, v4
100017f94: 4ee33411    	cmgt.2d	v17, v0, v3
100017f98: 4ee23403    	cmgt.2d	v3, v0, v2
100017f9c: 3dc01be2    	ldr	q2, [sp, #0x60]
100017fa0: 4e221e31    	and.16b	v17, v17, v2
100017fa4: 3dc017e2    	ldr	q2, [sp, #0x50]
100017fa8: 4e221c92    	and.16b	v18, v4, v2
100017fac: ad418bf9    	ldp	q25, q2, [sp, #0x30]
100017fb0: 4e221cb3    	and.16b	v19, v5, v2
100017fb4: 9104216d    	add	x13, x11, #0x108
100017fb8: ad4097e2    	ldp	q2, q5, [sp, #0x10]
100017fbc: 4e251cf5    	and.16b	v21, v7, v5
100017fc0: ad4491b7    	ldp	q23, q4, [x13, #0x90]
100017fc4: ad4659a7    	ldp	q7, q22, [x13, #0xc0]
100017fc8: ad43d1b8    	ldp	q24, q20, [x13, #0x70]
100017fcc: 4e221e05    	and.16b	v5, v16, v2
100017fd0: 3dc02db0    	ldr	q16, [x13, #0xb0]
100017fd4: 3dc019a2    	ldr	q2, [x13, #0x60]
100017fd8: 4ef83418    	cmgt.2d	v24, v0, v24
100017fdc: 4ef03410    	cmgt.2d	v16, v0, v16
100017fe0: 4e2e1e10    	and.16b	v16, v16, v14
100017fe4: 4e2f1f18    	and.16b	v24, v24, v15
100017fe8: 4eb81ca5    	orr.16b	v5, v5, v24
100017fec: 4eb01eb0    	orr.16b	v16, v21, v16
100017ff0: ad4161b5    	ldp	q21, q24, [x13, #0x20]
100017ff4: 4e391cc6    	and.16b	v6, v6, v25
100017ff8: 4ef83418    	cmgt.2d	v24, v0, v24
100017ffc: 4ef73417    	cmgt.2d	v23, v0, v23
100018000: 4e2c1ef7    	and.16b	v23, v23, v12
100018004: 4e2d1f18    	and.16b	v24, v24, v13
100018008: 4eb81cc6    	orr.16b	v6, v6, v24
10001800c: 4eb71e73    	orr.16b	v19, v19, v23
100018010: ad4061b7    	ldp	q23, q24, [x13]
100018014: 4ef83418    	cmgt.2d	v24, v0, v24
100018018: 4ef63416    	cmgt.2d	v22, v0, v22
10001801c: 4e2a1ed6    	and.16b	v22, v22, v10
100018020: 4e2b1f18    	and.16b	v24, v24, v11
100018024: 4eb81e52    	orr.16b	v18, v18, v24
100018028: 4eb61e31    	orr.16b	v17, v17, v22
10001802c: ad4261b6    	ldp	q22, q24, [x13, #0x40]
100018030: 4ef83418    	cmgt.2d	v24, v0, v24
100018034: 3dc01ff9    	ldr	q25, [sp, #0x70]
100018038: 4e391c63    	and.16b	v3, v3, v25
10001803c: 4e291f18    	and.16b	v24, v24, v9
100018040: 4eb81c63    	orr.16b	v3, v3, v24
100018044: 3cc88178    	ldur	q24, [x11, #0x88]
100018048: 4ef83418    	cmgt.2d	v24, v0, v24
10001804c: 4ef43414    	cmgt.2d	v20, v0, v20
100018050: 3dc023f9    	ldr	q25, [sp, #0x80]
100018054: 4e391f18    	and.16b	v24, v24, v25
100018058: 4e281e94    	and.16b	v20, v20, v8
10001805c: 4eb41f14    	orr.16b	v20, v24, v20
100018060: 3cc08178    	ldur	q24, [x11, #0x8]
100018064: 4ef83418    	cmgt.2d	v24, v0, v24
100018068: 4ef73417    	cmgt.2d	v23, v0, v23
10001806c: 3dc027f9    	ldr	q25, [sp, #0x90]
100018070: 4e391f18    	and.16b	v24, v24, v25
100018074: 4e3f1ef7    	and.16b	v23, v23, v31
100018078: 4eb71f17    	orr.16b	v23, v24, v23
10001807c: 3ccc8178    	ldur	q24, [x11, #0xc8]
100018080: 4ef83418    	cmgt.2d	v24, v0, v24
100018084: 4ee73407    	cmgt.2d	v7, v0, v7
100018088: 3dc02bf9    	ldr	q25, [sp, #0xa0]
10001808c: 4e391f18    	and.16b	v24, v24, v25
100018090: 4e3e1ce7    	and.16b	v7, v7, v30
100018094: 4ea71f07    	orr.16b	v7, v24, v7
100018098: 3cc48178    	ldur	q24, [x11, #0x48]
10001809c: 4ef83418    	cmgt.2d	v24, v0, v24
1000180a0: 4ef63416    	cmgt.2d	v22, v0, v22
1000180a4: 3dc02ff9    	ldr	q25, [sp, #0xb0]
1000180a8: 4e391f18    	and.16b	v24, v24, v25
1000180ac: 4e3d1ed6    	and.16b	v22, v22, v29
1000180b0: 4eb61f16    	orr.16b	v22, v24, v22
1000180b4: 3cca8178    	ldur	q24, [x11, #0xa8]
1000180b8: 4ef83418    	cmgt.2d	v24, v0, v24
1000180bc: 4ee43404    	cmgt.2d	v4, v0, v4
1000180c0: 3dc033f9    	ldr	q25, [sp, #0xc0]
1000180c4: 4e391f18    	and.16b	v24, v24, v25
1000180c8: 4e3c1c84    	and.16b	v4, v4, v28
1000180cc: 4ea41f04    	orr.16b	v4, v24, v4
1000180d0: 3cc28178    	ldur	q24, [x11, #0x28]
1000180d4: 4ef83418    	cmgt.2d	v24, v0, v24
1000180d8: 4ee13401    	cmgt.2d	v1, v0, v1
1000180dc: 3dc037f9    	ldr	q25, [sp, #0xd0]
1000180e0: 4e391f18    	and.16b	v24, v24, v25
1000180e4: 4ef53415    	cmgt.2d	v21, v0, v21
1000180e8: 4e3b1eb5    	and.16b	v21, v21, v27
1000180ec: 4eb51f15    	orr.16b	v21, v24, v21
1000180f0: 3dc039b8    	ldr	q24, [x13, #0xe0]
1000180f4: 4ef83418    	cmgt.2d	v24, v0, v24
1000180f8: 3dc03bf9    	ldr	q25, [sp, #0xe0]
1000180fc: 4e391c21    	and.16b	v1, v1, v25
100018100: 4e3a1f18    	and.16b	v24, v24, v26
100018104: 4eb81c21    	orr.16b	v1, v1, v24
100018108: 3cc68178    	ldur	q24, [x11, #0x68]
10001810c: 4ef83418    	cmgt.2d	v24, v0, v24
100018110: 4ee23402    	cmgt.2d	v2, v0, v2
100018114: 3dc03ff9    	ldr	q25, [sp, #0xf0]
100018118: 4e391f18    	and.16b	v24, v24, v25
10001811c: 3dc003f9    	ldr	q25, [sp]
100018120: 4e391c42    	and.16b	v2, v2, v25
100018124: 4ea21f02    	orr.16b	v2, v24, v2
100018128: 4ea11c41    	orr.16b	v1, v2, v1
10001812c: 4ea41ea2    	orr.16b	v2, v21, v4
100018130: 4ea71ec4    	orr.16b	v4, v22, v7
100018134: 4ea11c41    	orr.16b	v1, v2, v1
100018138: 4eb41ee2    	orr.16b	v2, v23, v20
10001813c: 4ea41c42    	orr.16b	v2, v2, v4
100018140: 4eb11c63    	orr.16b	v3, v3, v17
100018144: 4eb31e44    	orr.16b	v4, v18, v19
100018148: 4eb01cc6    	orr.16b	v6, v6, v16
10001814c: 4ea31c83    	orr.16b	v3, v4, v3
100018150: 3ccf8164    	ldur	q4, [x11, #0xf8]
100018154: 4ee43404    	cmgt.2d	v4, v0, v4
100018158: 3dc043e7    	ldr	q7, [sp, #0x100]
10001815c: 4e271c84    	and.16b	v4, v4, v7
100018160: 4ea41ca4    	orr.16b	v4, v5, v4
100018164: 4ea41cc4    	orr.16b	v4, v6, v4
100018168: 4ea11c41    	orr.16b	v1, v2, v1
10001816c: 4ea41c62    	orr.16b	v2, v3, v4
100018170: 4ea21c21    	orr.16b	v1, v1, v2
100018174: 6e014022    	ext.16b	v2, v1, v1, #0x8
100018178: 0ea21c21    	orr.8b	v1, v1, v2
10001817c: f940016d    	ldr	x13, [x11]
100018180: b107d1bf    	cmn	x13, #0x1f4
100018184: 1a9fa7ed    	cset	w13, lt
100018188: f940fd6e    	ldr	x14, [x11, #0x1f8]
10001818c: b107d1df    	cmn	x14, #0x1f4
100018190: 9a9fb14e    	csel	x14, x10, xzr, lt
100018194: 9e66002f    	fmov	x15, d1
100018198: aa0e01ee    	orr	x14, x15, x14
10001819c: aa0d01cd    	orr	x13, x14, x13
1000181a0: f800858d    	str	x13, [x12], #0x8
1000181a4: 9e6701a1    	fmov	d1, x13
1000181a8: 0e205821    	cnt.8b	v1, v1
1000181ac: 0e31b821    	addv.8b	b1, v1
1000181b0: 9e66002d    	fmov	x13, d1
1000181b4: 8b0001a0    	add	x0, x13, x0
1000181b8: d1010129    	sub	x9, x9, #0x40
1000181bc: 8b09002d    	add	x13, x1, x9
1000181c0: 9108016b    	add	x11, x11, #0x200
1000181c4: f100fdbf    	cmp	x13, #0x3f
1000181c8: 54ffecc8    	b.hi	0x100017f60 <_roc_bitmap_aot_reject+0x17c>
1000181cc: cb0903e9    	neg	x9, x9
1000181d0: eb09002b    	subs	x11, x1, x9
1000181d4: 540000c8    	b.hi	0x1000181ec <_roc_bitmap_aot_reject+0x408>
1000181d8: 14000058    	b	0x100018338 <_roc_bitmap_aot_reject+0x554>
1000181dc: d2800009    	mov	x9, #0x0                ; =0
1000181e0: d2800000    	mov	x0, #0x0                ; =0
1000181e4: eb09002b    	subs	x11, x1, x9
1000181e8: 54000a89    	b.ls	0x100018338 <_roc_bitmap_aot_reject+0x554>
1000181ec: f100217f    	cmp	x11, #0x8
1000181f0: 54000082    	b.hs	0x100018200 <_roc_bitmap_aot_reject+0x41c>
1000181f4: d280000d    	mov	x13, #0x0               ; =0
1000181f8: aa0903ea    	mov	x10, x9
1000181fc: 14000040    	b	0x1000182fc <_roc_bitmap_aot_reject+0x518>
100018200: 927df16c    	and	x12, x11, #0xfffffffffffffff8
100018204: 8b0c012a    	add	x10, x9, x12
100018208: 4e080d20    	dup.2d	v0, x9
10001820c: 900052ed    	adrp	x13, 0x100a74000 <_anon.0ab3406f5e7995b1184b9813d8a9fd04.141+0x75>
100018210: 3dc001a1    	ldr	q1, [x13]
100018214: 4ee18400    	add.2d	v0, v0, v1
100018218: 8b090d0d    	add	x13, x8, x9, lsl #3
10001821c: 910081ad    	add	x13, x13, #0x20
100018220: 6f00e401    	movi.2d	v1, #0000000000000000
100018224: 5280004e    	mov	w14, #0x2               ; =2
100018228: 4e080dc2    	dup.2d	v2, x14
10001822c: 5280008e    	mov	w14, #0x4               ; =4
100018230: 4e080dc3    	dup.2d	v3, x14
100018234: 528000ce    	mov	w14, #0x6               ; =6
100018238: 4e080dc4    	dup.2d	v4, x14
10001823c: 92803e6e    	mov	x14, #-0x1f4            ; =-500
100018240: 4e080dc5    	dup.2d	v5, x14
100018244: 5280002e    	mov	w14, #0x1               ; =1
100018248: 4e080dc6    	dup.2d	v6, x14
10001824c: 528007ee    	mov	w14, #0x3f              ; =63
100018250: 4e080dc7    	dup.2d	v7, x14
100018254: 5280010e    	mov	w14, #0x8               ; =8
100018258: 4e080dd0    	dup.2d	v16, x14
10001825c: 927df16e    	and	x14, x11, #0xfffffffffffffff8
100018260: 6f00e411    	movi.2d	v17, #0000000000000000
100018264: 6f00e412    	movi.2d	v18, #0000000000000000
100018268: 6f00e413    	movi.2d	v19, #0000000000000000
10001826c: 4ee28414    	add.2d	v20, v0, v2
100018270: 4ee38415    	add.2d	v21, v0, v3
100018274: 4ee48416    	add.2d	v22, v0, v4
100018278: ad7f61b7    	ldp	q23, q24, [x13, #-0x20]
10001827c: acc269b9    	ldp	q25, q26, [x13], #0x40
100018280: 4ef734b7    	cmgt.2d	v23, v5, v23
100018284: 4e261ef7    	and.16b	v23, v23, v6
100018288: 4ef834b8    	cmgt.2d	v24, v5, v24
10001828c: 4e261f18    	and.16b	v24, v24, v6
100018290: 4ef934b9    	cmgt.2d	v25, v5, v25
100018294: 4e261f39    	and.16b	v25, v25, v6
100018298: 4efa34ba    	cmgt.2d	v26, v5, v26
10001829c: 4e261f5a    	and.16b	v26, v26, v6
1000182a0: 4e271c1b    	and.16b	v27, v0, v7
1000182a4: 4e271e94    	and.16b	v20, v20, v7
1000182a8: 4e271eb5    	and.16b	v21, v21, v7
1000182ac: 4e271ed6    	and.16b	v22, v22, v7
1000182b0: 6efb46f7    	ushl.2d	v23, v23, v27
1000182b4: 6ef44714    	ushl.2d	v20, v24, v20
1000182b8: 6ef54735    	ushl.2d	v21, v25, v21
1000182bc: 6ef64756    	ushl.2d	v22, v26, v22
1000182c0: 4ea11ee1    	orr.16b	v1, v23, v1
1000182c4: 4eb11e91    	orr.16b	v17, v20, v17
1000182c8: 4eb21eb2    	orr.16b	v18, v21, v18
1000182cc: 4eb31ed3    	orr.16b	v19, v22, v19
1000182d0: 4ef08400    	add.2d	v0, v0, v16
1000182d4: f10021ce    	subs	x14, x14, #0x8
1000182d8: 54fffca1    	b.ne	0x10001826c <_roc_bitmap_aot_reject+0x488>
1000182dc: 4ea11e20    	orr.16b	v0, v17, v1
1000182e0: 4ea01e40    	orr.16b	v0, v18, v0
1000182e4: 4ea01e60    	orr.16b	v0, v19, v0
1000182e8: 6e004001    	ext.16b	v1, v0, v0, #0x8
1000182ec: 0ea11c00    	orr.8b	v0, v0, v1
1000182f0: 9e66000d    	fmov	x13, d0
1000182f4: eb0c017f    	cmp	x11, x12
1000182f8: 54000120    	b.eq	0x10001831c <_roc_bitmap_aot_reject+0x538>
1000182fc: f86a790b    	ldr	x11, [x8, x10, lsl #3]
100018300: b107d17f    	cmn	x11, #0x1f4
100018304: 1a9fa7eb    	cset	w11, lt
100018308: 9aca216b    	lsl	x11, x11, x10
10001830c: 9100054a    	add	x10, x10, #0x1
100018310: aa0d016d    	orr	x13, x11, x13
100018314: eb0a003f    	cmp	x1, x10
100018318: 54ffff21    	b.ne	0x1000182fc <_roc_bitmap_aot_reject+0x518>
10001831c: d343fd28    	lsr	x8, x9, #3
100018320: f828684d    	str	x13, [x2, x8]
100018324: 9e6701a0    	fmov	d0, x13
100018328: 0e205800    	cnt.8b	v0, v0
10001832c: 0e31b800    	addv.8b	b0, v0
100018330: 9e660008    	fmov	x8, d0
100018334: 8b000100    	add	x0, x8, x0
100018338: a9556ffc    	ldp	x28, x27, [sp, #0x150]
10001833c: 6d5423e9    	ldp	d9, d8, [sp, #0x140]
100018340: 6d532beb    	ldp	d11, d10, [sp, #0x130]
100018344: 6d5233ed    	ldp	d13, d12, [sp, #0x120]
100018348: 6d513bef    	ldp	d15, d14, [sp, #0x110]
10001834c: 910583ff    	add	sp, sp, #0x160
100018350: d65f03c0    	ret
