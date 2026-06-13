package gossip

import (
	"log"
	"math/rand"
	"sync"
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
	pendingAcks map[string]chan bool // nodeID -> channel which reveives true for ACK
	ackMu sync.Mutex
}



func CreateGossipManager(nodeID string, gossipPort string, apiPort string) *GossipManager {
	ml := CreateMembershipList()

	localIP := utils.GetLocalIP()
	ml.UpdateMember(nodeID, localIP, gossipPort, apiPort, 0, 0)

	transport := CreateTransport(gossipPort, ml)

	gm := &GossipManager {
		NodeID: nodeID,
		APIPort: apiPort,
		MemList: ml,
		Transport: transport,
		pendingAcks: make(map[string]chan bool),
	}

	transport.OnAck = func(nodeID string) {
		gm.ackMu.Lock()
		if ch, exists := gm.pendingAcks[nodeID]; exists {
			select {
			case ch <- true:
			default:
			}
		}
		gm.ackMu.Unlock()
	}

	return gm
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
			err := gm.Transport.SendGossip(membersSlice[i].IP + membersSlice[i].GossipPort, gm.NodeID)
			if err != nil {
				log.Printf("[Gossip] Error sending gossip to %s: %v\n", membersSlice[i].IP, err)
			}
		}
	}
}

func (gm *GossipManager) probeNode(member Member) bool {
	ackChan := make(chan bool, 1)

	gm.ackMu.Lock()
	gm.pendingAcks[member.ID] = ackChan
	gm.ackMu.Unlock()

	defer func() {
		gm.ackMu.Lock()
		delete(gm.pendingAcks, member.ID)
		gm.ackMu.Unlock()
	}()

	gm.Transport.sendPing(member.IP+member.GossipPort, gm.NodeID)

	select {
	case <-ackChan:
		return true

	case <-time.After(1 * time.Second):
		return false
	}
}

func (gm *GossipManager) cleanDeadMembersLoop() {
	for {
		time.Sleep(5 * time.Second)

		gm.MemList.mu.Lock()
		suspects := make([]Member, 0)
		for id, member := range gm.MemList.Members {
			if id == gm.NodeID {
				continue
			}

			if time.Now().Unix() - member.Timestamp > 15 {
				if member.State == Active {
					suspects = append(suspects, member)
					// log.Printf("[Gossip] WARNING: Node %s is DOWN\n", id)

					// member.State = Down
					// gm.MemList.Members[id] = member

					// if gm.OnNodeDown != nil {
					// 	go gm.OnNodeDown(id)
					// }
				}
			}
		}

		gm.MemList.mu.Unlock()

		for _, suspect := range suspects {
			if gm.probeNode(suspect) {
				// node responded, is alive, so we update its timestamp

				gm.MemList.mu.Lock()

				m := gm.MemList.Members[suspect.ID]
				m.Timestamp = time.Now().Unix()
				gm.MemList.Members[suspect.ID] = m

				gm.MemList.mu.Unlock()

				log.Printf("[Gossip] Node %s responded to ping, still alive\n", suspect.ID)
			} else
			{
				gm.MemList.mu.Lock()

				m := gm.MemList.Members[suspect.ID]
				m.State = Down
				gm.MemList.Members[suspect.ID] = m

				gm.MemList.mu.Unlock()

				log.Printf("[Gossip] Node %s did not respond to ping, marked DOWN\n", suspect.ID)

				if gm.OnNodeDown != nil {
					go gm.OnNodeDown(suspect.ID)
				}
			}
		}
	}
}