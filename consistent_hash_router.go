package ring

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type ConsistentHashRouter struct {
	hashFunction HashFunction
	realNodes    map[uint64]*ParentNode
	virtualNodes map[uint64]*virtualNode
	sortedMap    []uint64

	lock sync.RWMutex
}

type ParentNode struct {
	nodeKey      string
	virtualNodes map[uint64]*virtualNode
}

func (p *ParentNode) GetKey() string {
	return p.nodeKey
}

type HashFunction interface {
	hash(name string) uint64
}

func NewConsistentHashRouter(hashFunction HashFunction) (*ConsistentHashRouter, error) {
	if hashFunction == nil {
		// Default hash function
		hashFunction = &MD5HashFunction{}
	}
	return &ConsistentHashRouter{
		hashFunction: hashFunction,
		realNodes:    map[uint64]*ParentNode{},
		virtualNodes: map[uint64]*virtualNode{},
		sortedMap:    []uint64{},
	}, nil
}

func (c *ConsistentHashRouter) Join(nodeKey string, vNodeCount int) error {
	if vNodeCount < 0 {
		return errors.New("virtual node count should be equal or greater than zero")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	pNode := c.createOrGetParentNode(nodeKey)
	startIndex := len(pNode.virtualNodes)
	for i := startIndex; i < startIndex+vNodeCount; i++ {
		vNode := virtualNode{
			parentNode: *pNode,
			index:      i,
		}
		hash := c.hashFunction.hash(vNode.GetKey())

		c.sortedMap = append(c.sortedMap, hash)
		pNode.virtualNodes[hash] = &vNode
		c.virtualNodes[hash] = &vNode

		// Enables to do binary search
		sort.Slice(c.sortedMap, func(i, j int) bool {
			return c.sortedMap[i] < c.sortedMap[j]
		})
	}
	return nil
}

func (c *ConsistentHashRouter) Leave(nodeKey string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	pNode, found := c.getParentNode(nodeKey)
	if found {
		for _, vNode := range pNode.virtualNodes {
			hash := c.hashFunction.hash(vNode.GetKey())
			delete(pNode.virtualNodes, hash)

			// Dichotomous search to the find the virtual node
			index := sort.Search(len(c.sortedMap), func(i int) bool {
				return c.sortedMap[i] == hash
			})

			if index < len(c.sortedMap) {
				c.sortedMap = append(c.sortedMap[:index], c.sortedMap[index+1:]...)
			}
		}
		delete(c.realNodes, c.hashFunction.hash(pNode.GetKey()))

		return nil
	}
	return errors.New("Real node found!")
}

func (c *ConsistentHashRouter) createOrGetParentNode(nodeKey string) *ParentNode {
	pNode, found := c.getParentNode(nodeKey)
	if !found {
		pNode = &ParentNode{
			nodeKey:      nodeKey,
			virtualNodes: map[uint64]*virtualNode{},
		}
		c.realNodes[c.hashFunction.hash(pNode.GetKey())] = pNode
	}
	return pNode
}

func (c *ConsistentHashRouter) getParentNode(nodeKey string) (*ParentNode, bool) {
	if pNode, ok := c.realNodes[c.hashFunction.hash(nodeKey)]; ok {
		return pNode, true
	}
	return nil, false
}

// Get clockwise nearest real node based on the key
func (c *ConsistentHashRouter) Get(key string) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// No real node currently
	if len(c.sortedMap) == 0 {
		return nil, false
	}

	// Calculate the hash value
	hash := c.hashFunction.hash(key)

	// Dichotomous lookup
	// because the virtual nodes are reordered each time a node is added
	// so the first node queried is our target node
	// remainder will give us a circular list effect, finding nodes clockwise
	index := sort.Search(len(c.sortedMap), func(i int) bool {
		return c.sortedMap[i] >= hash
	}) % len(c.sortedMap)

	// virtual nodes -> physical nodes mapping
	return c.virtualNodes[c.sortedMap[index]].getRealNode(), true
}

func (c *ConsistentHashRouter) GetVirtualNodes(key string) ([]virtualNode, bool) {
	if pNode, ok := c.realNodes[c.hashFunction.hash(key)]; ok {
		var virtualNodes []virtualNode
		for _, vNode := range pNode.virtualNodes {
			virtualNodes = append(virtualNodes, *vNode)
		}
		return virtualNodes, true
	}
	return nil, false
}

// virtualNode allows to distribute data across nodes at a finer granularity than can be easily achieved using a single-token architecture.
type virtualNode struct {
	parentNode ParentNode
	index      int
}

func (v *virtualNode) GetKey() string {
	return fmt.Sprintf("%s-%d", v.parentNode.GetKey(), v.index)
}

func (v *virtualNode) isVirtualNodeOf(key string) bool {
	return v.parentNode.GetKey() == key
}

func (v *virtualNode) getRealNode() string {
	return v.parentNode.GetKey()
}

// MD5HashFunction Default hash function
type MD5HashFunction struct {
	HashFunction
}

func (m *MD5HashFunction) hash(name string) uint64 {
	data := []byte(name)
	b := md5.Sum(data)
	return binary.LittleEndian.Uint64(b[:])
}
