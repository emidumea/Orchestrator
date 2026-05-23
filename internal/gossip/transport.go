package gossip

import (
	"encoding/json"
	"net"
	"log"
)

type Transport struct {
	Port string
	MemList *MembershipList
}

func CreateTransport(port string, ml *MembershipList) *Transport {
	return &Transport {
		Port: port,
		MemList: ml,
	}
}

func (t *Transport) StartListening() error {
	addr, err := net.ResolveUDPAddr("udp", t.Port)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("[Gossip] Listening for gossip messages on port %s...\n", t.Port)

	buffer := make([]byte, 4096)

	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("[Gossip] Error reading from UDP socket:", err)
			continue
		}

		var incomingMembers map[string]Member
		err = json.Unmarshal(buffer[:n], &incomingMembers)
		if err != nil {
			log.Println("[Gossip] Received invalid gossip message:", err)
			continue
		}

		t.MemList.Merge(incomingMembers)
	}
}

func (t *Transport) SendGossip(targetAddress string) error {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	defer conn.Close()

	t.MemList.mu.Lock()
	data, err := json.Marshal(t.MemList.Members)
	t.MemList.mu.Unlock()

	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}