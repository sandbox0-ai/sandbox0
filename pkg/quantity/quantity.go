// Package quantity parses the bounded CPU and byte quantities used by the
// runtime-neutral sandbox API.
package quantity

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

var quantityPattern = regexp.MustCompile(`^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))((?:[eE][+-]?[0-9]+)|[numkKMGTPE]i?)?$`)

// Quantity is an immutable exact rational quantity.
type Quantity struct {
	value *big.Rat
	raw   string
}

// New returns an exact integer quantity.
func New(value int64) Quantity {
	return Quantity{value: new(big.Rat).SetInt64(value), raw: strconv.FormatInt(value, 10)}
}

// Parse parses an exact decimal or binary SI quantity. The
// supported suffixes are n, u, m, k/K, M, G, T, P, E and Ki through Ei.
func Parse(raw string) (Quantity, error) {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return Quantity{}, fmt.Errorf("quantity must be non-empty and contain no surrounding whitespace")
	}
	parts := quantityPattern.FindStringSubmatch(raw)
	if parts == nil {
		return Quantity{}, fmt.Errorf("invalid quantity %q", raw)
	}
	value, ok := new(big.Rat).SetString(parts[1])
	if !ok {
		return Quantity{}, fmt.Errorf("invalid quantity %q", raw)
	}
	factor, err := suffixFactor(parts[2])
	if err != nil {
		return Quantity{}, err
	}
	value.Mul(value, factor)
	return Quantity{value: value, raw: raw}, nil
}

// MustParse parses raw and panics if it is invalid.
func MustParse(raw string) Quantity {
	parsed, err := Parse(raw)
	if err != nil {
		panic(err)
	}
	return parsed
}

// NewMilli returns a decimal quantity from an exact millicpu value.
func NewMilli(value int64) Quantity {
	raw := strconv.FormatInt(value, 10) + "m"
	if value%1000 == 0 {
		raw = strconv.FormatInt(value/1000, 10)
	}
	return Quantity{value: new(big.Rat).SetFrac(big.NewInt(value), big.NewInt(1000)), raw: raw}
}

// Sign returns -1, 0, or 1 according to the quantity value.
func (q Quantity) Sign() int {
	if q.value == nil {
		return 0
	}
	return q.value.Sign()
}

// Cmp compares q and other.
func (q Quantity) Cmp(other Quantity) int {
	return q.rat().Cmp(other.rat())
}

// Value returns the quantity rounded away from zero to an integer.
func (q Quantity) Value() int64 {
	return roundedInt64(q.rat())
}

// MilliValue returns the quantity in milli-units, rounded away from zero.
func (q Quantity) MilliValue() int64 {
	scaled := new(big.Rat).Mul(q.rat(), big.NewRat(1000, 1))
	return roundedInt64(scaled)
}

// String returns the submitted representation, or 0 for the zero value.
func (q Quantity) String() string {
	if q.raw == "" {
		return "0"
	}
	return q.raw
}

func (q Quantity) rat() *big.Rat {
	if q.value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(q.value)
}

func roundedInt64(value *big.Rat) int64 {
	quotient, remainder := new(big.Int).QuoRem(value.Num(), value.Denom(), new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	} else if remainder.Sign() < 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		if quotient.Sign() < 0 {
			return -1 << 63
		}
		return 1<<63 - 1
	}
	return quotient.Int64()
}

func suffixFactor(suffix string) (*big.Rat, error) {
	if len(suffix) >= 2 && suffix[len(suffix)-1] == 'i' {
		exponentByPrefix := map[byte]int{'K': 1, 'M': 2, 'G': 3, 'T': 4, 'P': 5, 'E': 6}
		exponent, ok := exponentByPrefix[suffix[0]]
		if !ok || len(suffix) != 2 {
			return nil, fmt.Errorf("invalid binary quantity suffix %q", suffix)
		}
		factor := new(big.Int).Exp(big.NewInt(1024), big.NewInt(int64(exponent)), nil)
		return new(big.Rat).SetInt(factor), nil
	}
	if strings.HasPrefix(suffix, "e") || strings.HasPrefix(suffix, "E") {
		exponent, err := strconv.Atoi(suffix[1:])
		if err != nil || exponent < -100 || exponent > 100 {
			return nil, fmt.Errorf("invalid decimal quantity exponent %q", suffix)
		}
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exponent))), nil)
		if exponent < 0 {
			return new(big.Rat).SetFrac(big.NewInt(1), factor), nil
		}
		return new(big.Rat).SetInt(factor), nil
	}
	powers := map[string]int{"": 0, "n": -9, "u": -6, "m": -3, "k": 3, "K": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}
	exponent, ok := powers[suffix]
	if !ok {
		return nil, fmt.Errorf("invalid decimal quantity suffix %q", suffix)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exponent))), nil)
	if exponent < 0 {
		return new(big.Rat).SetFrac(big.NewInt(1), factor), nil
	}
	return new(big.Rat).SetInt(factor), nil
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
