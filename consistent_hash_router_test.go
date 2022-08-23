package ring

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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

		err = ch.Join(nodeKey, vNodeCount)
		require.NoError(t, err)

		nodeKeys = append(nodeKeys, nodeKey)
	}

	// We have no virtual nodes
	_, found := ch.Get(fakeData)
	require.Equal(t, false, found)

	vNodeCount = 1
	for i := 0; i < 4; i++ {
		nodeKey := fmt.Sprintf("node-%d", i)

		err = ch.Join(nodeKey, vNodeCount)
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
	err = ch.Join(nodeKey0, vNodeCount)
	require.NoError(t, err)

	nodeInterface, found := ch.Get(fakeData)
	require.Equal(t, true, found)

	receivedNodeKey := nodeInterface.(string)
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
	err = ch.Join(nodeKey0, vNodeCount)

	require.NoError(t, err)

	nodeInterface, found := ch.Get(fakeData)
	require.Equal(t, true, found)

	receivedNodeKey := nodeInterface.(string)
	require.Equal(t, nodeKey0, receivedNodeKey)
	vNodes, found := ch.GetVirtualNodes(receivedNodeKey)
	require.Equal(t, true, found)
	require.Equal(t, vNodeCount, len(vNodes))

	err = ch.Leave(nodeKey0)
	require.NoError(t, err)

	_, found = ch.Get(fakeData)
	require.Equal(t, false, found)
}
