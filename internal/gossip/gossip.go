package gossip

import (
	"fmt"
	"math/rand"
	"time"
)


type GossipManager struct {
	NodeID string
	APIPort string
	MemList *MembershipList
	Transport *Transport
}

func CreateGossipManager(nodeID string, udpPort string, apiPort string) *GossipManager {
	ml := CreateMembershipList()

	ml.UpdateMember(nodeID, "localhost"+udpPort, apiPort)

	transport := CreateTransport(udpPort, ml)

	return &GossipManager {
		NodeID: nodeID,
		APIPort: apiPort,
		MemList: ml,
		Transport: transport,
	}
}

func (gm *GossipManager) Start() {

	go func () {
		if err := gm.Transport.StartListening(); err != nil {
			fmt.Printf("[Gossip] Error starting gossip transport: %v\n", err)
		}
	}()


	go gm.gossipLoop()
	go gm.cleanDeadMembersLoop()

}


func (gm *GossipManager) gossipLoop() {
	for {
		gm.MemList.UpdateMember(gm.NodeID, gm.Transport.Port, gm.APIPort)

		time.Sleep(2 * time.Second)

		gm.MemList.mu.Lock()
		membersSlice := make([]Member, 0, len(gm.MemList.Members))
		for id, member := range gm.MemList.Members {
			if id != gm.NodeID && member.State == Active {
				membersSlice = append(membersSlice, member)
			}
		}
		gm.MemList.mu.Unlock()

		if len(membersSlice) == 0 {
			continue
		}

		rand.Shuffle(len(membersSlice), func(i, j int) {
			membersSlice[i], membersSlice[j] = membersSlice[j], membersSlice[i]
		})
		
		targetCount := 2
		if len(membersSlice) < targetCount {
			targetCount = len(membersSlice)
		}

		for i := 0; i < targetCount; i++ {
			err := gm.Transport.SendGossip(membersSlice[i].Address)
			if err != nil {
				fmt.Printf("[Gossip] Error sending gossip to %s: %v\n", membersSlice[i].Address, err)
			}
		}
	}
}

func (gm *GossipManager) cleanDeadMembersLoop() {
	for {
		time.Sleep(5 * time.Second)

		gm.MemList.mu.Lock()
		for id, member := range gm.MemList.Members {
			if id == gm.NodeID {
				continue
			}

			if time.Now().Unix() - member.Timestamp > 15 {
				if member.State == Active {
					fmt.Printf("[Gossip] WARNING: Node %s is DOWN\n", id)

					member.State = Down
					gm.MemList.Members[id] = member
				}
			}
		}

		gm.MemList.mu.Unlock()
	}
}