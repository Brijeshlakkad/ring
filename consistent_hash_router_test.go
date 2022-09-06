package ring

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const fakeData = "fake_file_name"
const nodeKey0 = "node-0"

func TestConsistentHashRouter_Get(t *testing.T) {
	hashFunction := &MD5HashFunction{}
	nodeName := "shard-node-0"
	rConfig := routerConfig{
		HashFunction: hashFunction,
		NodeName:     nodeName,
	}
	ch, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	var nodeKeys []string

	vNodeCount := 0
	for i := 0; i < 4; i++ {
		nodeKey := fmt.Sprintf("node-%d", i)

		err = ch.Join(nodeKey, setupTags(t, vNodeCount, ShardMember))
		require.Error(t, err, ErrVirtualNodeCount)

		nodeKeys = append(nodeKeys, nodeKey)
	}

	// We have no virtual nodes
	_, found := ch.Get(fakeData)
	require.Equal(t, false, found)

	vNodeCount = 1
	for i := 0; i < 4; i++ {
		nodeKey := fmt.Sprintf("node-%d", i)

		err = ch.Join(nodeKey, setupTags(t, vNodeCount, ShardMember))
		require.NoError(t, err)

		nodeKeys = append(nodeKeys, nodeKey)
	}

	vNodeCount = 1
	for i := 0; i < 4; i++ {
		nodeKey := fmt.Sprintf("node-%d", i)

		err = ch.Join(nodeKey, setupTags(t, vNodeCount, ShardMember))
		require.Error(t, err, ErrNodeIsAlreadyOnRing)

		nodeKeys = append(nodeKeys, nodeKey)
	}

	_, found = ch.Get(fakeData)
	require.Equal(t, true, found)
}

func TestVirtualNode_GetKey(t *testing.T) {
	nodeName := "shard-node-0"
	rConfig := routerConfig{
		NodeName: nodeName,
	}
	ch, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	// 4 virtual nodes
	vNodeCount := 4
	tags := setupTags(t, vNodeCount, ShardMember)
	tags["node_name"] = nodeKey0
	err = ch.Join(nodeKey0, tags)
	require.NoError(t, err)

	nodeTags, found := ch.Get(fakeData)
	require.Equal(t, true, found)

	receivedNodeKey := nodeTags["node_name"]
	require.Equal(t, nodeKey0, receivedNodeKey)
	vNodes, found := ch.GetVirtualNodes(receivedNodeKey)
	require.Equal(t, true, found)
	require.Equal(t, vNodeCount, len(vNodes))
}

func TestConsistentHashRouter_Leave(t *testing.T) {
	nodeName := "shard-node-0"
	rConfig := routerConfig{
		NodeName: nodeName,
	}
	ch, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	// 1 virtual nodes
	vNodeCount := 1
	tags := setupTags(t, vNodeCount, ShardMember)
	tags["node_name"] = nodeKey0
	err = ch.Join(nodeKey0, tags)

	require.NoError(t, err)

	nodeTags, found := ch.Get(fakeData)
	require.Equal(t, true, found)

	receivedNodeKey := nodeTags["node_name"]
	require.Equal(t, nodeKey0, receivedNodeKey)

	vNodes, found := ch.GetVirtualNodes(receivedNodeKey)
	require.Equal(t, true, found)
	require.Equal(t, vNodeCount, len(vNodes))

	err = ch.Leave(nodeKey0, nil)
	require.NoError(t, err)

	_, found = ch.Get(fakeData)
	require.Equal(t, false, found)
}

func TestConsistentHashRouter_JoinLeave(t *testing.T) {
	hashFunction := &MD5HashFunction{}
	nodeName := "shard-node-0"
	rConfig := routerConfig{
		HashFunction: hashFunction,
		NodeName:     nodeName,
	}
	ch, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)
	require.NoError(t, err)

	for i := 1; i < 5; i++ {
		// 4 virtual nodes
		vNodeCount := 4
		err = ch.Join(fmt.Sprintf("shard-node-%d", i), setupTags(t, vNodeCount, ShardMember))
		require.NoError(t, err)
	}

	var expected []string
	for i := 1; i < 5; i++ {
		nodeKey := fmt.Sprintf("load-balancer-key-%d", i)
		err = ch.Join(nodeKey, setupTags(t, 1, LoadBalancerMember))
		expected = append(expected, nodeKey)
		require.NoError(t, err)
	}

	loadBalancers := ch.GetLoadBalancers()
	for _, actualLoadBalancer := range loadBalancers {
		found := false
		i := -1
		for index, expectedLoadBalancer := range expected {
			if expectedLoadBalancer == actualLoadBalancer {
				i = index
				found = true
			}
		}
		require.True(t, found)
		expected[i] = expected[len(expected)-1]
		expected = expected[:len(expected)-1]
	}
	require.Equal(t, 0, len(expected))
}

func TestConsistentHashRouter_Join_VirtualNodes(t *testing.T) {
	hashFunction := &MD5HashFunction{}
	nodeName := "shard-node-0"
	rConfig := routerConfig{
		HashFunction: hashFunction,
		NodeName:     nodeName,
	}
	ch, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	require.Equal(t, 0, len(ch.sortedMap))

	vNode := 2
	err = ch.Join(fmt.Sprintf(nodeName), setupTags(t, vNode, ShardMember))
	require.NoError(t, err)

	require.Equal(t, vNode, len(ch.sortedMap))

	pNode := ch.createOrGetParentNode(nodeName)

	require.Equal(t, vNode, len(pNode.virtualNodes))
}

// TestConsistentHashRouter_HandleResharding_SingleJoin tests if the current node gets notified with the correct range of keys to transfer to the new nodes.
func TestConsistentHashRouter_HandleResharding_SingleJoin(t *testing.T) {
	nodes := []string{
		"shard-node-2",
		"shard-node-10",
		"shard-node-20",
		"shard-node-1",
		"shard-node-4",
		"shard-node-5",
		"shard-node-11",
		"shard-node-15",
		"shard-node-21",
	}
	hashFunction := &testHashFunction{
		cache: map[string]uint64{
			nodes[0]: 2,
			nodes[1]: 10,
			nodes[2]: 20,
			nodes[3]: 1,
			nodes[4]: 4,
			nodes[5]: 5,
			nodes[6]: 11,
			nodes[7]: 15,
			nodes[8]: 21,
		},
	}

	currentNode := nodes[0]
	rConfig := routerConfig{
		HashFunction: hashFunction,
		NodeName:     currentNode,
	}
	c, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	handler := &testChangeHandler{
		events: make(chan *testChangeItem, 1),
	}
	c.AddListener("testChangeHandler", handler)

	var newNode string
	var newEvent *testChangeItem

	// Join node[0]
	newNode = nodes[0]
	testSingleJoin(t, c, hashFunction, newNode)
	// Ring: 2
	require.Equal(t, 0, len(handler.events))

	// Join node[1]
	newNode = nodes[1]
	testSingleJoin(t, c, hashFunction, newNode)
	// Ring: 2 - 10
	require.Equal(t, 1, len(handler.events))
	newEvent = <-handler.events
	require.Equal(t, newNode, newEvent.newNode)
	require.Equal(t, hashFunction.cache[newNode], newEvent.start)
	require.Nil(t, newEvent.end)

	// Join node[2]
	newNode = nodes[2]
	testSingleJoin(t, c, hashFunction, newNode)
	// Ring: 2 - 10 - 20
	require.Equal(t, 0, len(handler.events)) // currentNode doesn't get affected when newNode joins

	// Join node[3]
	newNode = nodes[3]
	testSingleJoin(t, c, hashFunction, newNode)
	// Ring: 1 - 2 - 10 - 20
	require.Equal(t, 0, len(handler.events)) // currentNode doesn't get affected when newNode joins

	// Join node[4]
	newNode = nodes[4]
	// Ring: 1 - 2 - 4 - 10 - 20
	testSingleJoin(t, c, hashFunction, newNode)
	require.Equal(t, 1, len(handler.events))
	newEvent = <-handler.events
	require.Equal(t, newNode, newEvent.newNode)
	require.Equal(t, hashFunction.cache[newNode], newEvent.start)
	require.Nil(t, newEvent.end)
}

// TestConsistentHashRouter_HandleResharding_MultipleJoin tests when the multiple virtual nodes of the same node joins the ring at the same time
// and checks if the current node gets notified with the correct range of keys to transfer to the new nodes.
func TestConsistentHashRouter_HandleResharding_MultipleJoin(t *testing.T) {
	nodes := []string{
		"shard-node-0",
		"shard-node-1",
	}
	hashFunction := &testHashFunction{
		cache: map[string]uint64{
			"shard-node-0": 0,
			"shard-node-1": 1,
		},
	}
	nodeHashes := map[string][]int{
		nodes[0]: {
			2,
			10,
			20,
		},
		nodes[1]: {
			1,
			4,
			5,
			11,
			15,
			21,
		},
	}

	currentNode := nodes[0]
	rConfig := routerConfig{
		HashFunction: hashFunction,
		NodeName:     currentNode,
	}
	c, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	handler := &testChangeHandler{
		events: make(chan *testChangeItem, 6),
	}
	c.AddListener("testChangeHandler", handler)

	var newNode string
	var newEvents []*testChangeItem

	// Join node[0]
	newNode = nodes[0]
	testMultipleJoin(t, c, hashFunction, newNode, nodeHashes[newNode])
	// Ring: 2 - 10 - 20
	require.Equal(t, 0, len(handler.events))

	// Join node[1]
	newNode = nodes[1]
	testMultipleJoin(t, c, hashFunction, newNode, nodeHashes[newNode])
	// Ring: 1 - 2 - 4 - 5 - 10 - 11 - 15 - 20 - 21
	require.Eventually(t, func() bool {
		return 6 == len(handler.events)
	}, 1*time.Second, 250*time.Millisecond)

	expectedHashes := nodeHashes[newNode]
	for i := 0; i < 6; i++ {
		newEvent := <-handler.events
		newEvents = append(newEvents, newEvent)
	}

	sort.Slice(newEvents, func(i, j int) bool {
		return newEvents[i].start.(uint64) < newEvents[j].start.(uint64)
	})

	// The below code to check the responsibilities against the ring: 1 - 2 - 4 - 5 - 10 - 11 - 15 - 20 - 21
	require.Equal(t, newNode, newEvents[0].newNode)
	require.Equal(t, uint64(expectedHashes[0]), newEvents[0].start)
	require.Equal(t, uint64(expectedHashes[len(expectedHashes)-1]), newEvents[0].end)

	require.Equal(t, newNode, newEvents[1].newNode)
	require.Equal(t, uint64(expectedHashes[1]), newEvents[1].start)
	require.Equal(t, uint64(expectedHashes[2]), newEvents[1].end)

	require.Equal(t, newNode, newEvents[2].newNode)
	require.Equal(t, uint64(expectedHashes[2]), newEvents[2].start)
	require.Nil(t, newEvents[2].end)

	require.Equal(t, newNode, newEvents[3].newNode)
	require.Equal(t, uint64(expectedHashes[3]), newEvents[3].start)
	require.Equal(t, uint64(expectedHashes[4]), newEvents[3].end)

	require.Equal(t, newNode, newEvents[4].newNode)
	require.Equal(t, uint64(expectedHashes[4]), newEvents[4].start)
	require.Nil(t, newEvents[4].end)

	require.Equal(t, newNode, newEvents[5].newNode)
	require.Equal(t, uint64(expectedHashes[5]), newEvents[5].start)
	require.Nil(t, newEvents[5].end)
}

func TestConsistentHashRouter_GetConfig(t *testing.T) {
	nodeName := "node-0"
	nodeCount := 4
	nodes := make(map[uint64]*parentNode, nodeCount)
	hashFunction := &MD5HashFunction{}

	// Expected router configuration.
	for i := 0; i < nodeCount; i++ {
		node := &parentNode{
			nodeKey: fmt.Sprintf("node-%d", i),
			tags: map[string]string{
				"rpc_addr": fmt.Sprintf("127.0.0.1:1%d34", i),
			},
		}

		// virtual nodes
		virtualNodes := make(map[uint64]*virtualNode)
		for i := 0; i < 3; i++ {
			vNode := &virtualNode{
				parentNode: node,
				index:      i,
			}
			virtualNodes[hashFunction.Hash(vNode.GetKey())] = vNode
		}

		node.virtualNodes = virtualNodes

		nodes[hashFunction.Hash(node.GetKey())] = node
	}

	// create startup configuration.
	startupConfig := &StartupConfig{
		Nodes: make(map[uint64]ConfigurationNode),
	}
	for _, pNode := range nodes {
		VirtualNodes := make(map[uint64]int)
		for vNodeHash, vNode := range pNode.virtualNodes {
			VirtualNodes[vNodeHash] = vNode.index
		}
		startupConfig.Nodes[hashFunction.Hash(pNode.GetKey())] = ConfigurationNode{
			NodeKey:      pNode.nodeKey,
			Tags:         pNode.tags,
			VirtualNodes: VirtualNodes,
		}
	}

	rConfig := routerConfig{
		HashFunction:  &MD5HashFunction{},
		NodeName:      nodeName,
		startupConfig: startupConfig,
	}
	c, err := newConsistentHashRouter(rConfig)
	require.NoError(t, err)

	// check if the configuration set is correct.
	for pNodeHash, pNode := range c.realNodes {
		expectedNode, ok := nodes[pNodeHash]
		require.True(t, ok)
		require.Equal(t, expectedNode, pNode)

		for vNodeHash, expectedVNode := range expectedNode.virtualNodes {
			found := false
			for _, savedVNode := range c.sortedMap {
				if savedVNode == vNodeHash {
					found = true
					break
				}
			}
			require.True(t, found)

			vNode, ok := pNode.virtualNodes[vNodeHash]
			require.True(t, ok)

			require.Equal(t, expectedVNode, vNode)
		}
	}

	// Check if the configuration is correct.
	configResp := c.getConfig()
	require.Equal(t, nodeCount, len(configResp.Nodes))

	for hash, node := range configResp.Nodes {
		pNode, ok := nodes[hash]
		require.True(t, ok)

		require.Equal(t, pNode.tags, node.Tags)
		require.Equal(t, pNode.nodeKey, node.NodeKey)
		require.Equal(t, len(pNode.virtualNodes), len(node.VirtualNodes))

		for vNodeHash, vNodeIndex := range node.VirtualNodes {
			vNode, ok := pNode.virtualNodes[vNodeHash]
			require.True(t, ok)
			require.Equal(t, vNode.index, vNodeIndex)
		}
	}
}

type testChangeHandler struct {
	events chan *testChangeItem
}

type testChangeItem struct {
	start   interface{}
	end     interface{}
	newNode string
}

func (ch *testChangeHandler) OnChange(batch []ShardResponsibility) {
	if ch.events != nil {
		for _, event := range batch {
			ch.events <- &testChangeItem{
				start:   event.start,
				end:     event.end,
				newNode: event.newNode,
			}
		}
	}
}

func setupTags(t *testing.T, virtualNodes int, memberType MemberType) map[string]string {
	t.Helper()

	ports := dynaport.Get(1)
	return map[string]string{
		"rpc_addr":      fmt.Sprintf("%s:%d", "127.0.0.1", ports[0]),
		"virtual_nodes": strconv.Itoa(virtualNodes),
		"member_type":   strconv.Itoa(int(memberType)),
	}
}

// testHashFunction to have control over the hash and order while testing other units.
type testHashFunction struct {
	cache map[string]uint64
}

func (h *testHashFunction) Hash(key string) uint64 {
	return h.cache[key]
}

// testSingleJoin to join a single virtual node with the pre-defined hash value.
func testSingleJoin(t *testing.T, c *consistentHashRouter, hashFunction *testHashFunction, nodeKey string) {
	t.Helper()
	vNode := &virtualNode{
		parentNode: &parentNode{
			nodeKey: nodeKey,
		},
		index: 0,
	}
	// store the hash value of this node key.
	hashFunction.cache[vNode.GetKey()] = hashFunction.cache[nodeKey]
	err := c.Join(nodeKey, setupTags(t, 1, ShardMember))
	require.NoError(t, err)
}

// testMultipleJoin to join the virtual nodes with the pre-defined hash values.
func testMultipleJoin(t *testing.T, c *consistentHashRouter, hashFunction *testHashFunction, nodeKey string, nodes []int) {
	t.Helper()

	pNode := c.createOrGetParentNode(nodeKey)
	startIndex := len(pNode.virtualNodes)

	var newNodes []uint64

	startup := len(c.sortedMap) == 0

	// Nodes that will be re-sharded.
	for start, nodeValue := range nodes {
		i := startIndex + start
		vNode := virtualNode{
			parentNode: pNode,
			index:      i,
		}
		newNodeHash := uint64(nodeValue)
		hashFunction.cache[vNode.GetKey()] = newNodeHash

		c.sortedMap = append(c.sortedMap, newNodeHash)
		pNode.virtualNodes[newNodeHash] = &vNode
		c.virtualNodes[newNodeHash] = &vNode

		sort.Slice(c.sortedMap, func(i, j int) bool {
			return c.sortedMap[i] < c.sortedMap[j]
		})

		newNodes = append(newNodes, newNodeHash)
	}
	if !startup && len(newNodes) > 0 {
		c.handleResharding(newNodes)
	}
}
