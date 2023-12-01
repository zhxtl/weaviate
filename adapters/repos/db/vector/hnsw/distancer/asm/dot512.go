//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build ignore
// +build ignore

package main

import (
    . "github.com/mmcloughlin/avo/build"
    . "github.com/mmcloughlin/avo/operand"
    . "github.com/mmcloughlin/avo/reg"
)

var unroll = 4

func main() {
    TEXT("dot512", NOSPLIT, "func(x, y []float32) float32")
    x := Mem{Base: Load(Param("x").Base(), GP64())}
    y := Mem{Base: Load(Param("y").Base(), GP64())}
    n := Load(Param("x").Len(), GP64())

    acc := make([]VecVirtual, unroll)
    for i := 0; i < unroll; i++ {
        acc[i] = ZMM()
    }

    for i := 0; i < unroll; i++ {
        VXORPS(acc[i], acc[i], acc[i])
    }

    blockitems := 16 * unroll // Adjusted for 512-bit registers
    blocksize := 4 * blockitems
    Label("blockloop")
    CMPQ(n, U32(blockitems))
    JL(LabelRef("tail"))

    // Load x.
    xs := make([]VecVirtual, unroll)
    for i := 0; i < unroll; i++ {
        xs[i] = ZMM()
    }

    for i := 0; i < unroll; i++ {
        VMOVUPS(x.Offset(64*i), xs[i]) // Adjusted for 512-bit registers
    }

    // The actual FMA.
    for i := 0; i < unroll; i++ {
        VFMADD231PS(y.Offset(64*i), xs[i], acc[i]) // Adjusted for 512-bit registers
    }

    ADDQ(U32(blocksize), x.Base)
    ADDQ(U32(blocksize), y.Base)
    SUBQ(U32(blockitems), n)
    JMP(LabelRef("blockloop"))

    // Process any trailing entries.
    Label("tail")
    tail := XMM()
    VXORPS(tail, tail, tail)

    Label("tailloop")
    CMPQ(n, U32(0))
    JE(LabelRef("reduce"))

    xt := XMM()
    VMOVSS(x, xt)
    VFMADD231SS(y, xt, tail)

    ADDQ(U32(4), x.Base)
    ADDQ(U32(4), y.Base)
    DECQ(n)
    JMP(LabelRef("tailloop"))

    // Reduce the lanes to one.
    Label("reduce")
    if unroll != 4 {
        panic("addition is hard-coded")
    }

    // Manual reduction for ZMM
    VADDPS(acc[0], acc[1], acc[0])
    VADDPS(acc[2], acc[3], acc[2])
    VADDPS(acc[0], acc[2], acc[0])

    result := XMM()
    VEXTRACTF32X4(U8(0), acc[0], result) // Extract lower 128 bits
    top := XMM()
    VEXTRACTF32X4(U8(1), acc[0], top) // Extract next 128 bits
    VADDPS(result, top, result)
    VEXTRACTF32X4(U8(2), acc[0], top) // Extract next 128 bits
    VADDPS(result, top, result)
    VEXTRACTF32X4(U8(3), acc[0], top) // Extract upper 128 bits
    VADDPS(result, top, result)

    VADDPS(result, tail, result)
    VHADDPS(result, result, result)
    VHADDPS(result, result, result)
    Store(result, ReturnIndex(0))

    RET()

    Generate()
}
