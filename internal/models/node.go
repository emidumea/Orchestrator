package models

import (
	"time"
)

type NodeState string

const (
	NodeActive NodeState = "ACTIVE"
	NodeDown NodeState = "DOWN"
)

type Node struct {
	ID string
	Address string
	State NodeState
	LastSeen time.Time // timestamp of the last heartbeat
}