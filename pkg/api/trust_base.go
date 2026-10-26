package api

import (
	"reflect"

	"github.com/unicitynetwork/bft-go-base/types"
)

// IsNilTrustBase also recognizes an interface holding a nil implementation.
func IsNilTrustBase(tb types.RootTrustBase) bool {
	if tb == nil {
		return true
	}
	switch v := reflect.ValueOf(tb); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
