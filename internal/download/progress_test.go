package download

import "testing"

func TestStepOverall(t *testing.T) {
	tests := []struct {
		step        string
		value, want float64
	}{{"download", 50, 16.5}, {"merge", 50, 49.5}, {"upload", 50, 83}, {"upload", 100, 100}}
	for _, test := range tests {
		if got := stepOverall(test.step, test.value); got != test.want {
			t.Errorf("stepOverall(%q,%v)=%v want %v", test.step, test.value, got, test.want)
		}
	}
}
