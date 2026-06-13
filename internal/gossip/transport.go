package gossip

import (
	"encoding/json"
	"net"
	"log"
)

type Transport struct {
	Port string
	SelfNodeID string
	MemList *MembershipList
	OnAck func (nodeID string)
	OnPingReq func (requesterID string, targetID string, targetAddr string)
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
		n, senderAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("[Gossip] Error reading from UDP socket:", err)
			continue
		}

		// var incomingMembers map[string]Member
		// err = json.Unmarshal(buffer[:n], &incomingMembers)
		// if err != nil {
		// 	log.Println("[Gossip] Received invalid gossip message:", err)
		// 	continue
		// }
		var msg Message
		err = json.Unmarshal(buffer[:n], &msg)
		if err != nil {
			log.Println("[Gossip] Received invalid gossip message:", err)
			continue
		}
		t.handleMessage(msg, senderAddr)
	}
}

func (t *Transport) handleMessage(msg Message, senderAddr *net.UDPAddr) {
	switch msg.Type {
	case MsgGossip:
		t.MemList.Merge(msg.Members)

	case MsgPing:
		t.sendAck(msg.SenderID, t.SelfNodeID)

	case MsgPingReq:
		if t.OnPingReq != nil {
			t.OnPingReq(msg.SenderID, msg.TargetID, msg.TargetAddr)
		}

	case MsgAck:
		if t.OnAck != nil {
			t.OnAck(msg.SenderID)
		}

	default:
		log.Printf("[Gossip] Unknown message type: %s\n", msg.Type)
	}
}

func (t *Transport) SendGossip(targetAddress string, senderID string) error {
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
	msg := Message{
		Type: MsgGossip,
		SenderID: senderID,
		Members: t.MemList.Members,
	}
	data, err := json.Marshal(msg)
	t.MemList.mu.Unlock()

	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}

func (t *Transport) sendMessage(targetAddress string, msg Message) error {
	addr, err := net.ResolveUDPAddr("udp", targetAddress)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	
	defer conn.Close()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}

func (t *Transport) sendPing(targetAddress string, senderID string) error {
	msg := Message{
		Type: MsgPing,
		SenderID: senderID,
	}

	return t.sendMessage(targetAddress, msg)
}

func (t *Transport) sendAck(toID string, aboutID string) error {
	// find the address of the node who sent the ping

	t.MemList.mu.Lock()
	to, exists := t.MemList.Members[toID]
	t.MemList.mu.Unlock()

	if (!exists) {
		log.Println("[Gossip] sendAck error: couldn't find the target")
		return nil
	}

	msg := Message{
		Type: MsgAck,
		SenderID: t.SelfNodeID,
		AboutID: aboutID,
	}
	
	return t.sendMessage(to.IP+to.GossipPort, msg)
}

func (t *Transport) sendPingReq(helperAddr string, targetID string, targetAddr string) error {
	msg := Message{
		Type: MsgPingReq,
		SenderID: t.SelfNodeID,
		TargetID: targetID,
		TargetAddr: targetAddr,
	}

	return t.sendMessage(helperAddr, msg)
}