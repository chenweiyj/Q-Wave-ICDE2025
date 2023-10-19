package expt9

import (
	"testing"
)


func TestSuperNode_makeClientGroup(t *testing.T) {
	tests := []struct {
		name   string
		fields *SuperNode
	}{
		// TODO: Add test cases.
		{"1", NewSuperNode(500)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.makeClientGroup()
		})
	}
}
