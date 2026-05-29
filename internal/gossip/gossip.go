package gossip

import (
	"log"
	"math/rand"
	"time"

	"orchestrator/internal/models"
	"orchestrator/internal/utils"
)


type GossipManager struct {
	NodeID string
	APIPort string
	MemList *MembershipList
	Transport *Transport
	OnNodeDown func(nodeID string)
	GetMetrics func() models.SystemMetrics
}

func CreateGossipManager(nodeID string, gossipPort string, apiPort string) *GossipManager {
	ml := CreateMembershipList()

	localIP := utils.GetLocalIP()
	ml.UpdateMember(nodeID, localIP, gossipPort, apiPort, 0, 0)

	transport := CreateTransport(gossipPort, ml)

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
			log.Printf("[Gossip] Error starting gossip transport: %v\n", err)
		}
	}()


	go gm.gossipLoop()
	go gm.cleanDeadMembersLoop()

}


func (gm *GossipManager) gossipLoop() {
	for {

		if (gm.GetMetrics != nil) {
			metrics := gm.GetMetrics()
			gm.MemList.UpdateMember(gm.NodeID, utils.GetLocalIP(), gm.Transport.Port, gm.APIPort, metrics.MemoryFree, metrics.CPUFree)
		} else {
			gm.MemList.UpdateMember(gm.NodeID, utils.GetLocalIP(), gm.Transport.Port, gm.APIPort, 0, 0)
		}

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
			err := gm.Transport.SendGossip(membersSlice[i].IP + membersSlice[i].GossipPort)
			if err != nil {
				log.Printf("[Gossip] Error sending gossip to %s: %v\n", membersSlice[i].IP, err)
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
					log.Printf("[Gossip] WARNING: Node %s is DOWN\n", id)

					member.State = Down
					gm.MemList.Members[id] = member

					if gm.OnNodeDown != nil {
						go gm.OnNodeDown(id)
					}
				}
			}
		}

		gm.MemList.mu.Unlock()
	}
}