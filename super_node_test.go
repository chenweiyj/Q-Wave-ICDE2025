package main

import (
	"testing"
)

func isSuperNodeValid(node *SuperNode) bool {
	// 1. check group count
	if node.groupCount != len(node.clientGroup) {
		return false
	}
	// 2. check client number in client group
	sum := 0
	for _, group := range node.clientGroup {
		sum += len(group)
	}
	if sum != node.clientNo {
		return false
	}
	// 3. check each group has sufficient client
	q := node.clientNo / node.groupCount
	for _, group := range node.clientGroup {
		if len(group) < q {
			return false
		}
	}
	// 4. check all elements in groups are distinct
	m := make(map[int]struct{})
	for _, group := range node.clientGroup {
		for _, element := range group {
			m[element] = struct{}{}
		}
	}

	return len(m) == node.clientNo
}

func TestSuperNode_makeClientGroup(t *testing.T) {
	tests := []struct {
		name   string
		fields *SuperNode
	}{
		// TODO: Add test cases.
		{"1", NewSuperNode(70)},
		{"2", NewSuperNode(5000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &SuperNode{
				clientNo:    tt.fields.clientNo,
				groupCount:  tt.fields.groupCount,
				clientGroup: tt.fields.clientGroup,
			}
			node.makeClientGroup()
			if !isSuperNodeValid(node) {
				t.Errorf("supernode is not valid")
			}
		})
	}
}
