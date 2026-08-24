package sandboxspec

import "reflect"

func clone[T any](in *T) *T {
	if in == nil {
		return nil
	}
	out := cloneValue(reflect.ValueOf(in)).Interface().(*T)
	return out
}

func cloneValue(in reflect.Value) reflect.Value {
	if !in.IsValid() {
		return in
	}
	switch in.Kind() {
	case reflect.Pointer:
		if in.IsNil() {
			return reflect.Zero(in.Type())
		}
		out := reflect.New(in.Type().Elem())
		out.Elem().Set(cloneValue(in.Elem()))
		return out
	case reflect.Interface:
		if in.IsNil() {
			return reflect.Zero(in.Type())
		}
		out := cloneValue(in.Elem())
		wrapped := reflect.New(in.Type()).Elem()
		wrapped.Set(out)
		return wrapped
	case reflect.Slice:
		if in.IsNil() {
			return reflect.Zero(in.Type())
		}
		out := reflect.MakeSlice(in.Type(), in.Len(), in.Cap())
		for i := 0; i < in.Len(); i++ {
			out.Index(i).Set(cloneValue(in.Index(i)))
		}
		return out
	case reflect.Map:
		if in.IsNil() {
			return reflect.Zero(in.Type())
		}
		out := reflect.MakeMapWithSize(in.Type(), in.Len())
		iterator := in.MapRange()
		for iterator.Next() {
			out.SetMapIndex(cloneValue(iterator.Key()), cloneValue(iterator.Value()))
		}
		return out
	case reflect.Struct:
		out := reflect.New(in.Type()).Elem()
		out.Set(in)
		for i := 0; i < in.NumField(); i++ {
			if out.Field(i).CanSet() && in.Type().Field(i).PkgPath == "" {
				out.Field(i).Set(cloneValue(in.Field(i)))
			}
		}
		return out
	default:
		return in
	}
}
