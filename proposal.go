package ring

import (
	"fmt"
)

// proposal represents a proposed value for specified key,
// the actual agreed on value will be decided by the consensus algorithm
type proposal struct {
	Number int    // proposal number used to decide which proposal to promise
	Key    string // key to identify the value
	Value  []byte // the actual value / data we store once proposal is accepted
}

// String returns a human-readable representation
func (p *proposal) String() string {
	return fmt.Sprintf("(num=%d, key=%s, value=%s)", p.Number, p.Key, p.Value)
}
