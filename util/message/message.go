package util

import "fmt"

//go:generate stringer -type=MessageType
type MessageType int8

const (
	LEADER MessageType = iota
	BLOCK
	VOTE
	DONE
	LEADER0
	BLOCK0
	VOTE0
	E
	E1
	I_INDEX
)

func (m MessageType) StringTest() string {
	return fmt.Sprintf("%d", m)
}

func (m MessageType) String() string {
	return []string{
		"LEADER",
		"BLOCK",
		"VOTE",
		"DONE",
		"LEADER0",
		"BLOCK0",
		"VOTE0",
		"E",
		"E1", "I_INDEX"}[m]
}