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

	*shardChangeHandler
	currentNodeKey string
}

type loadBalancer struct {
	rpcAddr string
}

type parentNode struct {
	nodeKey      string
	virtualNodes map[uint64]*virtualNode
	tags         map[string]string
}

func (p *parentNode) GetKey() string {
	return p.nodeKey
}

func newConsistentHashRouter(hashFunction HashFunction, nodeKey string, tags map[string]string) (*consistentHashRouter, error) {
	if hashFunction == nil {
		// Default hash function
		hashFunction = &MD5HashFunction{}
	}
	ch := &consistentHashRouter{
		hashFunction:  hashFunction,
		realNodes:     map[uint64]*parentNode{},
		virtualNodes:  map[uint64]*virtualNode{},
		sortedMap:     []uint64{},
		loadBalancers: map[string]*loadBalancer{},
		shardChangeHandler: &shardChangeHandler{
			listeners: make(map[string]ShardResponsibilityHandler),
		},
		currentNodeKey: nodeKey,
	}
	if tags != nil {
		err := ch.Join(nodeKey, tags)
		if err != nil {
			return nil, err
		}
	}
	return ch, nil
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

	// remove unnecessary information.
	delete(tags, virtualNodesJSON)
	delete(tags, ringRPCAddrJSON)

	if memberType == ShardMember {
		pNode := c.createOrGetParentNode(nodeKey)
		pNode.tags = tags
		startIndex := len(pNode.virtualNodes)

		var newNodes []uint64

		startup := len(c.sortedMap) == 0

		// Nodes that will be re-sharded.
		for i := startIndex; i < startIndex+vNodeCount; i++ {
			vNode := virtualNode{
				parentNode: pNode,
				index:      i,
			}
			newNodeHash := c.hashFunction.Hash(vNode.GetKey())

			c.sortedMap = append(c.sortedMap, newNodeHash)
			pNode.virtualNodes[newNodeHash] = &vNode
			c.virtualNodes[newNodeHash] = &vNode

			// Enables to do binary search
			sort.Slice(c.sortedMap, func(i, j int) bool {
				return c.sortedMap[i] < c.sortedMap[j]
			})

			newNodes = append(newNodes, newNodeHash)
		}

		// If the new node is not the only node on the ring.
		if !startup && len(newNodes) > 0 {
			c.handleResharding(newNodes)
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

type newHashNode struct {
	hash uint64
	prev *newHashNode
	next *newHashNode
}

func (c *consistentHashRouter) handleResharding(newNodes []uint64) {
	// Sort the new nodes.
	sort.Slice(newNodes, func(i, j int) bool {
		return newNodes[i] < newNodes[j]
	})

	nodeHashMap := make(map[uint64]uint64)
	newNodeMap := make(map[uint64]bool)

	for _, newNode := range newNodes {
		newNodeMap[newNode] = true
	}

	for _, newNodeHash := range newNodes {
		// Dichotomous lookup to find the previous node which will send its data to the new node.
		previousNodeIndex := binarySearchUint64(c.sortedMap, 0, len(c.sortedMap), newNodeHash)
		if previousNodeIndex == 0 {
			previousNodeIndex = len(c.sortedMap) - 1
		} else {
			previousNodeIndex -= 1
		}

		nodeHashMap[newNodeHash] = c.sortedMap[previousNodeIndex]
	}

	affectedNodeMap := make(map[uint64][]uint64)

	for nodeHash, previousNodeHash := range nodeHashMap {
		for newNodeMap[previousNodeHash] {
			// Find previous node that is not the new node.
			previousNodeHash = nodeHashMap[previousNodeHash]
		}
		affectedNodeMap[previousNodeHash] = append(affectedNodeMap[previousNodeHash], nodeHash)
	}

	// Build a map to send the changes in a single batch.
	var batch []ShardResponsibility
	for currentNodeHash, newNodesOfCurrent := range affectedNodeMap {
		if c.virtualNodes[currentNodeHash].getRealNode() == c.currentNodeKey {
			sort.Slice(newNodesOfCurrent, func(i, j int) bool {
				return newNodesOfCurrent[i] < newNodesOfCurrent[j]
			})

			for i := 0; i < len(newNodesOfCurrent); i++ {
				var endKey interface{}
				if i+1 == len(newNodesOfCurrent) {
					endKey = nil
				} else {
					endKey = newNodesOfCurrent[i+1]
				}
				batch = append(batch, newShardResponsibility(
					newNodesOfCurrent[i],
					endKey,
					c.virtualNodes[newNodesOfCurrent[i]].getRealNode(),
					c.virtualNodes[newNodesOfCurrent[i]].getTags(),
					c.hashFunction,
				))
			}
		}
	}
	//
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].start < batch[j].start
	})
	// Notify listeners for the current node changes only.
	c.notifyListeners(batch)
}

func (c *consistentHashRouter) Leave(nodeKey string, tags map[string]string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	pNode, found := c.getParentNode(nodeKey)
	if found {
		for _, vNode := range pNode.virtualNodes {
			hash := c.hashFunction.Hash(vNode.GetKey())
			delete(pNode.virtualNodes, hash)

			// Dichotomous search to the find the virtual node
			index := sort.Search(len(c.sortedMap), func(i int) bool {
				return c.sortedMap[i] == hash
			})

			if index < len(c.sortedMap) {
				c.sortedMap = append(c.sortedMap[:index], c.sortedMap[index+1:]...)
			}
		}
		delete(c.realNodes, c.hashFunction.Hash(pNode.GetKey()))

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
		c.realNodes[c.hashFunction.Hash(pNode.GetKey())] = pNode
	}
	return pNode
}

func (c *consistentHashRouter) getParentNode(nodeKey string) (*parentNode, bool) {
	hash := c.hashFunction.Hash(nodeKey)
	if pNode, ok := c.realNodes[hash]; ok {
		return pNode, true
	}
	return nil, false
}

// Get clockwise nearest real node based on the key
func (c *consistentHashRouter) Get(key string) (map[string]string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// No real node currently
	if len(c.sortedMap) == 0 {
		return nil, false
	}

	// Calculate the hash value
	hash := c.hashFunction.Hash(key)

	// Dichotomous lookup
	// because the virtual nodes are reordered each time a node is added
	// so the first node queried is our target node
	// remainder will give us a circular list effect, finding nodes clockwise
	index := sort.Search(len(c.sortedMap), func(i int) bool {
		return c.sortedMap[i] >= hash
	}) % len(c.sortedMap)

	// virtual nodes -> physical nodes mapping
	return c.virtualNodes[c.sortedMap[index]].getTags(), true
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
	if pNode, ok := c.realNodes[c.hashFunction.Hash(key)]; ok {
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
	parentNode *parentNode
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

func (v *virtualNode) getTags() map[string]string {
	return v.parentNode.tags
}

// MD5HashFunction Default hash function
type MD5HashFunction struct {
	HashFunction
}

func (m *MD5HashFunction) Hash(name string) uint64 {
	data := []byte(name)
	b := md5.Sum(data)
	return binary.LittleEndian.Uint64(b[:])
}

type shardChangeHandler struct {
	listeners map[string]ShardResponsibilityHandler
	lock      sync.Mutex
}

func (sch *shardChangeHandler) AddListener(listenerId string, listener ShardResponsibilityHandler) {
	sch.lock.Lock()
	defer sch.lock.Unlock()

	sch.listeners[listenerId] = listener
}

func (sch *shardChangeHandler) RemoveListener(listenerId string) {
	sch.lock.Lock()
	defer sch.lock.Unlock()

	delete(sch.listeners, listenerId)
}

func (sch *shardChangeHandler) notifyListeners(batch []ShardResponsibility) {
	sch.lock.Lock()
	defer sch.lock.Unlock()

	for _, listener := range sch.listeners {
		listener.OnChange(batch)
	}
}

// ShardResponsibility to determine if an object should get be transferred to the given node.
type ShardResponsibility struct {
	start        uint64
	end          interface{}
	newNode      string
	tags         map[string]string
	hashFunction HashFunction
}

func newShardResponsibility(
	start interface{},
	end interface{},
	newNode string,
	tags map[string]string,
	hashFunction HashFunction,
) ShardResponsibility {
	return ShardResponsibility{
		start:        start.(uint64),
		end:          end,
		newNode:      newNode,
		tags:         tags,
		hashFunction: hashFunction,
	}
}

func (s *ShardResponsibility) Transfer(objectKey string) bool {
	objectHash := s.hashFunction.Hash(objectKey)
	if objectHash <= s.start {
		if s.end == nil {
			return true
		}
		return objectHash > s.end.(uint64)
	}
	return false
}

func (s *ShardResponsibility) ResponsibleNodeTags() map[string]string {
	return s.tags
}

func (s *ShardResponsibility) ResponsibleNode() string {
	return s.newNode
}
