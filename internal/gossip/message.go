package gossip

type MessageType string

const (
	MsgGossip MessageType = "GOSSIP"
	MsgPing MessageType = "PING"
	MsgAck MessageType = "ACK"
	MsgPingReq MessageType = "PINGREQ"
)

type Message struct {
	Type MessageType `json:"type"`
	SenderID string `json:"sender_id"`
	Members map[string]Member `json:"members,omitempty"`
	TargetID string `json:"target_id,omitempty"` // for PINGREQ
	TargetAddr string `json:"target_addr,omitempty"`
	AboutID string `json:"about_id"` // for ACK (node to be confirmed)
}

