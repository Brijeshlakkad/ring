package ring

// Handler interface to get notified when a new member joins or existing member leaves the ring.
type Handler interface {
	Join(nodeKey string, vNodeCount int, memberType MemberType) error
	Leave(nodeKey string, memberType MemberType) error
}

// HashFunction hashes key (string) to uint64.
type HashFunction interface {
	hash(key string) uint64
}

type MemberType uint8

const (
	ShardMember        MemberType = iota // RingMember takes part in the sharding.
	LoadBalancerMember                   // Doesn't take part in the sharding, but knows the addresses of member.
)
