#include "textflag.h"

TEXT ·nativeCPUID(SB), NOSPLIT, $0-24
	MOVL leaf+0(FP), AX
	MOVL index+4(FP), CX
	CPUID
	MOVL AX, a+8(FP)
	MOVL BX, b+12(FP)
	MOVL CX, c+16(FP)
	MOVL DX, d+20(FP)
	RET

TEXT ·nativeXCR0(SB), NOSPLIT, $0-8
	XORL CX, CX
	XGETBV
	MOVL AX, low+0(FP)
	MOVL DX, high+4(FP)
	RET
