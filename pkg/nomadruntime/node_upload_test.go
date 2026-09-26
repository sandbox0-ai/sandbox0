package nomadruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

type uploadOrderAuthority struct {
	rootFSWriterAuthority
	events *[]string
	err    error
}

func (a uploadOrderAuthority) RecordRootFSNodeUpload(ctx context.Context, _ rootfshandoff.StageRequest, _ string, _ rootfsblock.ObjectReference, uploaded bool) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("upload must be time bounded")
	}
	if uploaded {
		*a.events = append(*a.events, "ack")
	} else {
		*a.events = append(*a.events, "reserve")
	}
	return a.err
}

func (a uploadOrderAuthority) RecordRootFSRebaseNodeUpload(ctx context.Context, _ string, _ rootfsblock.ObjectReference, uploaded bool) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("upload must be time bounded")
	}
	if uploaded {
		*a.events = append(*a.events, "ack")
	} else {
		*a.events = append(*a.events, "reserve")
	}
	return a.err
}

type uploadOrderPublisher struct {
	events *[]string
	err    error
}

func (p uploadOrderPublisher) PutImmutable(_ context.Context, _ string, _ []byte) error {
	*p.events = append(*p.events, "put")
	return p.err
}

func TestNodeObjectPublisherReservesBeforePUTAndAcknowledgesAfterSuccess(t *testing.T) {
	for _, test := range []struct {
		name               string
		rebase             bool
		reserveErr, putErr error
		want               []string
	}{
		{name: "offline rebase", rebase: true, want: []string{"reserve", "put", "ack"}},
		{name: "published", want: []string{"reserve", "put", "ack"}},
		{name: "registration refused", reserveErr: errors.New("queued deletion"), want: []string{"reserve"}},
		{name: "upload interrupted", putErr: errors.New("network failure"), want: []string{"reserve", "put"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			publisher := nodeJournalPublisher{rebase: test.rebase, authority: uploadOrderAuthority{events: &events, err: test.reserveErr}, publisher: uploadOrderPublisher{events: &events, err: test.putErr}, operation: "checkpoint"}
			err := publisher.PutImmutable(t.Context(), "rootfs/v2/packs/sha256/object", []byte("payload"))
			if test.reserveErr != nil || test.putErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.want, events)
		})
	}
}
