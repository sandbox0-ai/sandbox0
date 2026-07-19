package resourceguard

import (
	"fmt"
	"reflect"
	"strings"
)

// StructureLimits configures recursive collection checks. Negative limits
// disable the corresponding check.
type StructureLimits struct {
	MaxMapItems            int
	MaxSliceItems          int
	MaxMapStringValueBytes int64
}

// Structure recursively validates maps and slices in a typed object. It is
// intended for decoded control-plane models whose nested collection shapes
// would otherwise require duplicated guard code.
func Structure(resource string, value any, limits StructureLimits) error {
	return validateStructure(resource, reflect.ValueOf(value), limits)
}

func validateStructure(path string, value reflect.Value, limits StructureLimits) error {
	if !value.IsValid() {
		return nil
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Struct:
		valueType := value.Type()
		for i := 0; i < value.NumField(); i++ {
			fieldType := valueType.Field(i)
			if fieldType.PkgPath != "" {
				continue
			}
			fieldName := fieldType.Name
			tag := strings.Split(fieldType.Tag.Get("json"), ",")[0]
			if tag == "-" {
				continue
			}
			if tag != "" {
				fieldName = tag
			}
			if err := validateStructure(path+"."+fieldName, value.Field(i), limits); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		if limits.MaxMapItems >= 0 {
			if err := Map(path, value.Len(), limits.MaxMapItems); err != nil {
				return err
			}
		}
		iter := value.MapRange()
		for iter.Next() {
			mapValue := iter.Value()
			unwrapped := mapValue
			for unwrapped.IsValid() &&
				(unwrapped.Kind() == reflect.Interface || unwrapped.Kind() == reflect.Pointer) {
				if unwrapped.IsNil() {
					break
				}
				unwrapped = unwrapped.Elem()
			}
			if limits.MaxMapStringValueBytes >= 0 &&
				unwrapped.IsValid() &&
				unwrapped.Kind() == reflect.String {
				if err := String(
					path+" value",
					unwrapped.String(),
					limits.MaxMapStringValueBytes,
				); err != nil {
					return err
				}
			}
			if err := validateStructure(path+" value", mapValue, limits); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return nil
		}
		if limits.MaxSliceItems >= 0 {
			if err := Slice(path, value.Len(), limits.MaxSliceItems); err != nil {
				return err
			}
		}
		for i := 0; i < value.Len(); i++ {
			if err := validateStructure(
				fmt.Sprintf("%s item %d", path, i),
				value.Index(i),
				limits,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
