package ring

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const fakeData = "fake_file_name"
const nodeKey0 = "node-0"

func TestConsistentHashRouter_Get(t *testing.T) {
	hashFunction := &MD5HashFunction{}
	ch, err := newConsistentHashRouter(hashFunction)
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
	ch, err := newConsistentHashRouter(nil)
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
	ch, err := newConsistentHashRouter(nil)
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
	ch, err := newConsistentHashRouter(hashFunction)
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
	ch, err := newConsistentHashRouter(hashFunction)
	require.NoError(t, err)

	nodeName := "shard-node-0"

	err = ch.Join(fmt.Sprintf(nodeName), setupTags(t, 0, ShardMember))
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

func setupTags(t *testing.T, virtualNodes int, memberType MemberType) map[string]string {
	t.Helper()

	ports := dynaport.Get(1)
	return map[string]string{
		"rpc_addr":      fmt.Sprintf("%s:%d", "127.0.0.1", ports[0]),
		"virtual_nodes": strconv.Itoa(virtualNodes),
		"member_type":   strconv.Itoa(int(memberType)),
	}
}
