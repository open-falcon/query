package http

import "testing"

func TestConvertDurationToPoint(t *testing.T) {
	var tests = []struct {
		in              string
		outFrom         int64
		outTo           int64
		checkOffsetOnly bool
	}{
		{"1d", 0, 86400, true},
		{"3min", 0, 180, true},
		{"1462204800,1462377600", 1462204800, 1462377600, false},
	}

	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	for _, v := range tests {
		from, to := convertDurationToPoint(v.in, result)
		if v.checkOffsetOnly {
			if (v.outTo - v.outFrom) != (to - from) {
				t.Errorf("duration test failed. in %v", v.in)
				t.Errorf("to is %v, from is %v", to, from)
				t.Errorf("v.outTo is %v, v.outFrom is %v", v.outTo, v.outFrom)
			}
		} else {
			if to != v.outTo || from != v.outFrom {
				t.Error("timestamp test failed.")
			}
		}
	}
}
