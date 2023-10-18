package util

import "testing"

func TestMessageType_String(t *testing.T) {
	tests := []struct {
		name string
		m    MessageType
		want string
	}{
		// TODO: Add test cases.
		{"1", BLOCK, "1"},
		{"2", VOTE0, "6"},
		{"3", E, "7"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.StringTest(); got != tt.want {
				t.Errorf("MessageType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
