package nodefs

import (
	"errors"
	"testing"
)

func TestNodeIDRoundTripAtBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		slot    Slot
		localID uint64
	}{
		{name: "minimum", slot: 1, localID: 1},
		{name: "maximum local", slot: 1, localID: MaxLocalID},
		{name: "maximum slot", slot: Slot(MaxSlot), localID: 1},
		{name: "maximum encoded id", slot: Slot(MaxSlot), localID: MaxLocalID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := EncodeNodeID(tt.slot, tt.localID)
			if err != nil {
				t.Fatalf("EncodeNodeID() error = %v", err)
			}
			slot, localID, err := DecodeNodeID(encoded)
			if err != nil {
				t.Fatalf("DecodeNodeID() error = %v", err)
			}
			if slot != tt.slot || localID != tt.localID {
				t.Fatalf("DecodeNodeID() = (%d, %d), want (%d, %d)", slot, localID, tt.slot, tt.localID)
			}
		})
	}
}

func TestHandleIDRoundTrip(t *testing.T) {
	t.Parallel()
	encoded, err := EncodeHandleID(42, 99)
	if err != nil {
		t.Fatalf("EncodeHandleID() error = %v", err)
	}
	slot, localID, err := DecodeHandleID(encoded)
	if err != nil {
		t.Fatalf("DecodeHandleID() error = %v", err)
	}
	if slot != 42 || localID != 99 {
		t.Fatalf("DecodeHandleID() = (%d, %d), want (42, 99)", slot, localID)
	}
}

func TestEncodeRejectsInvalidSlotAndLocalID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		slot    Slot
		localID uint64
		wantErr error
	}{
		{name: "slot zero", slot: 0, localID: 1, wantErr: ErrInvalidSlot},
		{name: "slot overflow", slot: Slot(MaxSlot + 1), localID: 1, wantErr: ErrInvalidSlot},
		{name: "local zero", slot: 1, localID: 0, wantErr: ErrInvalidLocalID},
		{name: "local overflow", slot: 1, localID: MaxLocalID + 1, wantErr: ErrInvalidLocalID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := EncodeNodeID(tt.slot, tt.localID); !errors.Is(err, tt.wantErr) {
				t.Fatalf("EncodeNodeID() error = %v, want %v", err, tt.wantErr)
			}
			if _, err := EncodeHandleID(tt.slot, tt.localID); !errors.Is(err, tt.wantErr) {
				t.Fatalf("EncodeHandleID() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeRejectsReservedAndMalformedIDs(t *testing.T) {
	t.Parallel()
	if _, _, err := DecodeNodeID(ShardRootNodeID); !errors.Is(err, ErrSyntheticNode) {
		t.Fatalf("DecodeNodeID(shard root) error = %v, want %v", err, ErrSyntheticNode)
	}
	if _, _, err := DecodeNodeID(2); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("DecodeNodeID(slot zero) error = %v, want %v", err, ErrInvalidSlot)
	}
	if _, _, err := DecodeNodeID(uint64(1) << LocalIDBits); !errors.Is(err, ErrInvalidLocalID) {
		t.Fatalf("DecodeNodeID(local zero) error = %v, want %v", err, ErrInvalidLocalID)
	}
	if _, _, err := DecodeHandleID(0); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("DecodeHandleID(0) error = %v, want %v", err, ErrInvalidSlot)
	}
}

func TestNewSlotRejectsOverflowBeforeConversion(t *testing.T) {
	t.Parallel()
	if _, err := NewSlot(0); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("NewSlot(0) error = %v, want %v", err, ErrInvalidSlot)
	}
	if _, err := NewSlot(uint64(MaxSlot) + 1); !errors.Is(err, ErrInvalidSlot) {
		t.Fatalf("NewSlot(overflow) error = %v, want %v", err, ErrInvalidSlot)
	}
	if slot, err := NewSlot(uint64(MaxSlot)); err != nil || slot != Slot(MaxSlot) {
		t.Fatalf("NewSlot(max) = (%d, %v), want (%d, nil)", slot, err, MaxSlot)
	}
}
