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
	ch, err := newConsistentHashRouter(hashFunction, nodeName, setupTags(t, 0, ShardMember))
	require.NoError(t, err)

	var nodeKeys []string

	vNodeCount := 0
	for i := 0; i < 4; i++ {
		nodeKey := fmt.Sprintf("node-%d", i)

		err = ch.Join(nodeKey, setupTags(t, vNodeCount, ShardMember))
		require.NoError(t, err)

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

	_, found = ch.Get(fakeData)
	require.Equal(t, true, found)
}

func TestVirtualNode_GetKey(t *testing.T) {
	nodeName := "shard-node-0"
	ch, err := newConsistentHashRouter(nil, nodeName, setupTags(t, 0, ShardMember))
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
	ch, err := newConsistentHashRouter(nil, nodeName, setupTags(t, 0, ShardMember))
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

	err = ch.Leave(nodeKey0)
	require.NoError(t, err)

	_, found = ch.Get(fakeData)
	require.Equal(t, false, found)
}

func TestConsistentHashRouter_JoinLeave(t *testing.T) {
	hashFunction := &MD5HashFunction{}
	nodeName := "shard-node-0"
	ch, err := newConsistentHashRouter(hashFunction, nodeName, setupTags(t, 0, ShardMember))
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
		err = ch.Join(nodeKey, setupTags(t, 0, LoadBalancerMember))
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
	ch, err := newConsistentHashRouter(hashFunction, nodeName, setupTags(t, 0, ShardMember))
	require.NoError(t, err)

	require.Equal(t, 0, len(ch.sortedMap))

	vNode := 2
	totalNodes := vNode
	err = ch.Join(fmt.Sprintf(nodeName), setupTags(t, vNode, ShardMember))
	require.NoError(t, err)

	require.Equal(t, vNode, len(ch.sortedMap))

	pNode := ch.createOrGetParentNode(nodeName)

	require.Equal(t, vNode, len(pNode.virtualNodes))

	totalNodes += vNode
	err = ch.Join(fmt.Sprintf(nodeName), setupTags(t, vNode, ShardMember))
	require.NoError(t, err)

	require.Equal(t, totalNodes, len(ch.sortedMap))
	require.Equal(t, totalNodes, len(pNode.virtualNodes))
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
	tags := setupTags(t, 0, ShardMember)
	c, err := newConsistentHashRouter(hashFunction, currentNode, tags)
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
	tags := setupTags(t, 0, ShardMember)
	c, err := newConsistentHashRouter(hashFunction, currentNode, tags)
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

type testChangeHandler struct {
	events chan *testChangeItem
}

type testChangeItem struct {
	start   interface{}
	end     interface{}
	newNode string
}

func (ch *testChangeHandler) OnChange(start interface{}, end interface{}, newNode string) {
	if ch.events != nil {
		ch.events <- &testChangeItem{
			start:   start,
			end:     end,
			newNode: newNode,
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

func (h *testHashFunction) hash(key string) uint64 {
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
