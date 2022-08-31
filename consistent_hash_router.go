package ring

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

var (
	ErrRealNodeNotFound = errors.New("real node not found")
)

type consistentHashRouter struct {
	hashFunction HashFunction
	realNodes    map[uint64]*parentNode
	virtualNodes map[uint64]*virtualNode
	sortedMap    []uint64

	lock          sync.RWMutex
	loadBalancers map[string]*loadBalancer
}

type loadBalancer struct {
	rpcAddr string
}

type parentNode struct {
	nodeKey      string
	virtualNodes map[uint64]*virtualNode
}

func (p *parentNode) GetKey() string {
	return p.nodeKey
}

func newConsistentHashRouter(hashFunction HashFunction) (*consistentHashRouter, error) {
	if hashFunction == nil {
		// Default hash function
		hashFunction = &MD5HashFunction{}
	}
	return &consistentHashRouter{
		hashFunction:  hashFunction,
		realNodes:     map[uint64]*parentNode{},
		virtualNodes:  map[uint64]*virtualNode{},
		sortedMap:     []uint64{},
		loadBalancers: map[string]*loadBalancer{},
	}, nil
}

func (c *consistentHashRouter) Join(nodeKey string, tags map[string]string) error {
	vNodeCount, err := strconv.Atoi(tags[virtualNodesJSON])
	if err != nil {
		return err
	}
	memberTypeInt, err := strconv.ParseUint(tags[memberTypeJSON], 10, 64)
	if err != nil {
		return err
	}
	memberType := MemberType(uint8(memberTypeInt))
	if vNodeCount < 0 {
		return errors.New("virtual node count should be equal or greater than zero")
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	if memberType == ShardMember {
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
	} else if memberType == LoadBalancerMember {
		c.loadBalancers[nodeKey] = &loadBalancer{
			rpcAddr: nodeKey,
		}
		return nil
	}
	return nil
}

func (c *consistentHashRouter) Leave(nodeKey string) error {
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
	} else {
		delete(c.loadBalancers, nodeKey)
	}
	return nil
}

func (c *consistentHashRouter) createOrGetParentNode(nodeKey string) *parentNode {
	pNode, found := c.getParentNode(nodeKey)
	if !found {
		pNode = &parentNode{
			nodeKey:      nodeKey,
			virtualNodes: map[uint64]*virtualNode{},
		}
		c.realNodes[c.hashFunction.hash(pNode.GetKey())] = pNode
	}
	return pNode
}

func (c *consistentHashRouter) getParentNode(nodeKey string) (*parentNode, bool) {
	hash := c.hashFunction.hash(nodeKey)
	if pNode, ok := c.realNodes[hash]; ok {
		return pNode, true
	}
	return nil, false
}

// Get clockwise nearest real node based on the key
func (c *consistentHashRouter) Get(key string) (interface{}, bool) {
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

func (c *consistentHashRouter) GetLoadBalancers() []string {
	c.lock.Lock()
	defer c.lock.Unlock()

	var loadBalancers []string
	for _, loadbalancer := range c.loadBalancers {
		loadBalancers = append(loadBalancers, loadbalancer.rpcAddr)
	}
	return loadBalancers
}

func (c *consistentHashRouter) GetVirtualNodes(key string) ([]virtualNode, bool) {
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
	parentNode parentNode
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
