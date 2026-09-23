package main

import (
	"strings"
	"testing"
)

func TestParseTerminalWorkerFramesOmitsArguments(t *testing.T) {
	stacks := "goroutine 12 [select]:\n" +
		"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode.(*ChannelHub).dispatch(0xc000, {0x123, 0x456}, {0xabc, 0xdef})\n" +
		"\t/path/channel.go:85 +0x12\n" +
		"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler.(*Worker).Run(0xc000, {0x789, 0x999}, 0x123)\n" +
		"\t/path/worker.go:91 +0x20\n\n"
	frames := parseTerminalWorkerFrames(stacks)
	if len(frames) != 2 || strings.Contains(strings.Join(frames, " "), "0x") {
		t.Fatalf("unsafe or missing diagnostic frames: %q", frames)
	}
}
