package gossip

import (
	"sync"
	"time"
)

type NodeState string

const (
	Active NodeState = "ACTIVE"
	Down NodeState = "DOWN"
)

type Member struct {
	ID string `json:"id"`
	IP string `json:"ip"`
	GossipPort string `json:"gossip_port"`
	APIPort string `json:"api_port"`
	State NodeState `json:"state"`
	Timestamp int64 `json:"timestamp"`
	MemoryFree uint64 `json:"memory_free"` // MB
	CPUFree float64 `json:"cpu_free"`
}


type MembershipList struct {
	Members map[string]Member
	mu sync.Mutex
}

func CreateMembershipList() *MembershipList {
	return &MembershipList {
		Members: make(map[string]Member),
	}
}

func (ml *MembershipList) UpdateMember(id string, ip string, gossipPort string, apiPort string, memoryFree uint64, cpuFree float64) {
	ml.mu.Lock()

	ml.Members[id] = Member {
			ID: id,
			IP: ip,
			GossipPort: gossipPort,
			APIPort: apiPort,
			State: Active,
			Timestamp: time.Now().Unix(),
			MemoryFree: memoryFree,
			CPUFree: cpuFree,
		}
	
	ml.mu.Unlock()
}


func (ml *MembershipList) Merge(incomingMembers map[string]Member) {
	ml.mu.Lock()
	for _, member := range incomingMembers {
		localMember, exists := ml.Members[member.ID]

		if !exists {
			ml.Members[member.ID] = member
		} else if member.Timestamp > localMember.Timestamp{
			ml.Members[member.ID] = member
		}
	}

	ml.mu.Unlock()
}

func (ml *MembershipList) GetActiveMembers(nodeID string) []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	var activeMembers []Member
	for id, member := range ml.Members{
		if id != nodeID && member.State == Active {
			activeMembers = append(activeMembers, member)
		}
	}
	return activeMembers
}

func (ml *MembershipList) RemoveMember(id string) {
	ml.mu.Lock()
	defer ml.mu.Lock()
	delete(ml.Members, id)
}