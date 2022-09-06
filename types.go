package ring

import (
	"net"
	"time"
)

// Handler interface to get notified when a new member joins or existing member leaves the ring.
type Handler interface {
	Join(nodeName string, tags map[string]string) error
	Leave(nodeName string, tags map[string]string) error
}

// HashFunction hashes key (string) to uint64.
type HashFunction interface {
	Hash(key string) uint64
}

type MemberType uint8

const (
	ShardMember        MemberType = iota // RingMember takes part in the sharding.
	LoadBalancerMember                   // Doesn't take part in the sharding, but knows the addresses of member.
)

// ShardResponsibilityHandler to listen to responsibility change when any new member joins the ring at the next position of the current node.
type ShardResponsibilityHandler interface {
	// OnChange This will be fired if the current node is affected by the new members.
	OnChange([]ShardResponsibility)
}

// ServerID is a unique string identifying a server for all time.
type ServerID string

// ServerAddress is a network address for a server that a transport can contact.
type ServerAddress string

// Server tracks the information about a single server in a configuration.
type Server struct {
	// Address is its network address that a transport can contact.
	Address ServerAddress
}

type RequestType uint8

const (
	RingRequestType RequestType = iota
	PaxosRequestType
)

type PaxosMessageType uint8

const (
	PrepareMsgType PaxosMessageType = iota
	ProposalMsgType
	PromiseMsgType
	AcceptMsgType
)

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}
