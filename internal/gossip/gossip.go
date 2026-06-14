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
	pendingAcks map[string][]chan bool // list of channels for each node (nodeID -> channel which reveives true for ACK)
	ackMu sync.Mutex
	seedAddrs []string // bootstrap addresses
}



func CreateGossipManager(nodeID string, gossipPort string, apiPort string, seedAddrs []string) *GossipManager {
	ml := CreateMembershipList()

	localIP := utils.GetLocalIP()
	ml.UpdateMember(nodeID, localIP, gossipPort, apiPort, 0, 0)

	transport := CreateTransport(gossipPort, ml)

	gm := &GossipManager {
		NodeID: nodeID,
		APIPort: apiPort,
		MemList: ml,
		Transport: transport,
		pendingAcks: make(map[string][]chan bool),
		seedAddrs: seedAddrs,
	}

	transport.OnAck = func(aboutID string) {
		gm.ackMu.Lock()
		channels := gm.pendingAcks[aboutID]
		for _, ch := range channels {
			select {
			case ch <- true:
			default:
			}
		}
		gm.ackMu.Unlock()
	}

	transport.OnPingReq = func(requesterID, targetID, targetAddr string) {
		alive := gm.probeNode(targetID, targetAddr)

		if alive {
			gm.Transport.sendAck(requesterID, targetID)
		}
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
			for _, seed := range gm.seedAddrs {
				gm.Transport.SendGossip(seed, gm.NodeID)
			}
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


func (gm *GossipManager) registerAck(nodeID string) chan bool {
	ackChan := make(chan bool, 1)
	gm.ackMu.Lock()
	gm.pendingAcks[nodeID] = append(gm.pendingAcks[nodeID], ackChan)
	gm.ackMu.Unlock()
	return ackChan
}

func (gm *GossipManager) unregisterAck(nodeID string, ackChan chan bool) {
	gm.ackMu.Lock()
	defer gm.ackMu.Unlock()

	channels := gm.pendingAcks[nodeID]
	for i, ch := range channels {
		if ch == ackChan {
			gm.pendingAcks[nodeID] = append(channels[:i], channels[i+1:]...)
			break
		}
	}

	if len(gm.pendingAcks[nodeID]) == 0 {
		delete(gm.pendingAcks, nodeID)
	}

}
func (gm *GossipManager) probeNode(targetID, targetAddr string) bool {
	ackChan := gm.registerAck(targetID)
	defer gm.unregisterAck(targetID, ackChan)

	gm.Transport.sendPing(targetAddr, gm.NodeID)

	select {
	case <-ackChan:
		return true

	case <-time.After(1 * time.Second):
		return false
	}
}

func (gm *GossipManager) markAlive(nodeID string) {
	gm.MemList.mu.Lock()
	if member, exists := gm.MemList.Members[nodeID]; exists {
		member.Timestamp = time.Now().Unix()
		member.State = Active
		gm.MemList.Members[nodeID] = member
	}
}

func (gm *GossipManager) markDown(nodeID string) {
	gm.MemList.mu.Lock()
	wasActive := false
	if member, exists := gm.MemList.Members[nodeID]; exists && member.State == Active {
		member.State = Down
		gm.MemList.Members[nodeID] = member
		wasActive = true
	}
	gm.MemList.mu.Unlock()

	if wasActive && gm.OnNodeDown != nil {
		go gm.OnNodeDown(nodeID)
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

			if time.Now().Unix() - member.Timestamp > 10 {
				if member.State == Active {
					suspects = append(suspects, member)
					// log.Printf("[Gossip] WARNING: Node %s is DOWN\n", id)
				}
			}
		}

		gm.MemList.mu.Unlock()

		for _, suspect := range suspects {
			if gm.probeNode(suspect.ID, suspect.IP+suspect.GossipPort) {
				// node responded, is alive, so we update its timestamp
				log.Printf("[Gossip SWIM] Node %s is alive (direct ping)\n", suspect.ID)
				gm.markAlive(suspect.ID)
				continue
			} 

			log.Printf("[Gossip SWIM] Direct ping to %s failed, trying indirect probe...\n", suspect.ID)
			if gm.indirectProbe(suspect) {
				log.Printf("[Gossip SWIM] Node %s is alive (indirect probe)", suspect.ID)
				gm.markAlive(suspect.ID)
				continue
			}

			log.Printf("[Gossip SWIM] Node %s did not respond to any pings (both probes failed), marked DOWN\n", suspect.ID)
			gm.markDown(suspect.ID)
		}
	}
}

func (gm *GossipManager) indirectProbe(target Member) bool {
	const k = 2

	gm.MemList.mu.Lock()
	helpers := make([]Member, 0)
	for id, m := range gm.MemList.Members {
		if id != gm.NodeID && id != target.ID && m.State == Active {
			helpers = append(helpers, m)
		}
	}
	gm.MemList.mu.Unlock()

	if len(helpers) == 0 {
		return false // no other members to ask
	}

	rand.Shuffle(len(helpers), func(i, j int) {
		helpers[i], helpers[j] = helpers[j], helpers[i]
	})

	if len(helpers) > k {
		helpers = helpers[:k]
	}

	ackChan := gm.registerAck(target.ID)
	defer gm.unregisterAck(target.ID, ackChan)


	targetAddr := target.IP + target.GossipPort
	for _, helper := range helpers {
		gm.Transport.sendPingReq(helper.IP+helper.GossipPort, target.ID, targetAddr)
	}

	select {
	case <-ackChan:
		return true // a helper confirmed the target is alive
	case <-time.After(2 * time.Second):
		return false // no helper managed to confirm
	}

}