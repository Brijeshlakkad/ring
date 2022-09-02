package ring

// Handler interface to get notified when a new member joins or existing member leaves the ring.
type Handler interface {
	Join(nodeName string, tags map[string]string) error
	Leave(nodeName string) error
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
