package ring_test

import (
	"testing"

	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
)

const fakeData = "fake_file_name"
const nodeKey0 = "node-0"

func TestLocusManager(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
	){
		"create and get": testCreateGetConsistentHashing,
		"virtual nodes":  testVirtualNode,
		"leave node":     testLeaveNode,
	} {
		t.Run(scenario, func(t *testing.T) {
			fn(t)
		})
	}
}

func testCreateGetConsistentHashing(t *testing.T) {
	ch, err := ring.NewConsistentHashRouter(nil)
	require.NoError(t, err)

	vNodeCount := 0
	err = ch.Join(nodeKey0, vNodeCount)

	require.NoError(t, err)

	// We have no virtual nodes
	_, found := ch.Get(fakeData)
	require.Equal(t, false, found)
}

func testVirtualNode(t *testing.T) {
	ch, err := ring.NewConsistentHashRouter(nil)
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

func testLeaveNode(t *testing.T) {
	ch, err := ring.NewConsistentHashRouter(nil)
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
