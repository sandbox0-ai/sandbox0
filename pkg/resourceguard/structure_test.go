package resourceguard

import "testing"

func TestStructureValidatesNestedCollectionsAndMapValues(t *testing.T) {
	type nested struct {
		Values map[string]string `json:"values"`
		Items  []string          `json:"items"`
	}
	limits := StructureLimits{
		MaxMapItems:            1,
		MaxSliceItems:          1,
		MaxMapStringValueBytes: 2,
	}
	if err := Structure("object", nested{
		Values: map[string]string{"a": "12"},
		Items:  []string{"a"},
	}, limits); err != nil {
		t.Fatalf("Structure(boundary) error = %v", err)
	}
	if err := Structure("object", nested{
		Values: map[string]string{"a": "123"},
	}, limits); !IsTooLarge(err) {
		t.Fatalf("Structure(map value over) error = %v, want TooLargeError", err)
	}
	if err := Structure("object", nested{
		Items: []string{"a", "b"},
	}, limits); !IsTooLarge(err) {
		t.Fatalf("Structure(slice over) error = %v, want TooLargeError", err)
	}
}

func TestStructureSkipsJSONIgnoredFields(t *testing.T) {
	type value struct {
		Ignored []string `json:"-"`
	}
	if err := Structure("object", value{
		Ignored: []string{"one", "two"},
	}, StructureLimits{
		MaxMapItems:            1,
		MaxSliceItems:          1,
		MaxMapStringValueBytes: 1,
	}); err != nil {
		t.Fatalf("Structure() error = %v for JSON-ignored field", err)
	}
}
