#include "textflag.h"

// func PrefetchCache(addr uintptr)
TEXT ·Prefetch(SB), NOSPLIT, $0-8
    MOVD addr+0(FP), R0      // Move the 64-bit address into R0
    PRFM (R0), PLDL1KEEP     // Prefetch the data into the L1 cache (PLDL1KEEP)
    RET

// func PrefetchCacheStreamed(addr uintptr)
TEXT ·PrefetchStreamed(SB), NOSPLIT, $0-8
    MOVD addr+0(FP), R0      // Move the 64-bit address into R0
    PRFM (R0), PSTL2STRM   // Prefetch the data into the L1 cache (PLDL1STRM)
    RET
