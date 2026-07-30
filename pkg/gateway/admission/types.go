package admission

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type State string

const (
	StateAllowed    State = "allowed"
	StateRestricted State = "restricted"
)

const (
	maxSourceLength = 128
	maxReasonLength = 512
)

var (
	ErrInvalidUpdate   = errors.New("invalid admission update")
	ErrVersionConflict = errors.New("admission version conflict")
)

type Record struct {
	TeamID    string    `json:"team_id"`
	Version   int64     `json:"version"`
	State     State     `json:"state"`
	Source    string    `json:"source"`
	Reason    string    `json:"reason"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Update struct {
	Version int64  `json:"version"`
	State   State  `json:"state"`
	Source  string `json:"source"`
	Reason  string `json:"reason"`
}

type PutResult struct {
	Record  Record `json:"admission"`
	Applied bool   `json:"applied"`
}

func (u Update) Validate() (Update, error) {
	u.Source = strings.TrimSpace(u.Source)
	u.Reason = strings.TrimSpace(u.Reason)
	if u.Version < 0 {
		return Update{}, fmt.Errorf("%w: version must not be negative", ErrInvalidUpdate)
	}
	if u.State != StateAllowed && u.State != StateRestricted {
		return Update{}, fmt.Errorf("%w: state must be %q or %q", ErrInvalidUpdate, StateAllowed, StateRestricted)
	}
	if u.Source == "" {
		return Update{}, fmt.Errorf("%w: source is required", ErrInvalidUpdate)
	}
	if len(u.Source) > maxSourceLength {
		return Update{}, fmt.Errorf("%w: source must not exceed %d bytes", ErrInvalidUpdate, maxSourceLength)
	}
	if len(u.Reason) > maxReasonLength {
		return Update{}, fmt.Errorf("%w: reason must not exceed %d bytes", ErrInvalidUpdate, maxReasonLength)
	}
	return u, nil
}

func (r Record) Matches(update Update) bool {
	return r.Version == update.Version &&
		r.State == update.State &&
		r.Source == update.Source &&
		r.Reason == update.Reason
}
