package ring

// Handler interface to get notified when a new member joins or existing member leaves the ring.
type Handler interface {
	Join(nodeKey string, vNodeCount int) error
	Leave(nodeKey string) error
}

// HashFunction hashes key (string) to uint64.
type HashFunction interface {
	hash(key string) uint64
}
