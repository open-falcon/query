package utils

import "reflect"

func GetMapKeys(FunctionMap []reflect.Value) (keys []string) {
	for _, v := range FunctionMap {
		keys = append(keys, v.String())
	}
	return
}
