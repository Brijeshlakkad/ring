package ring

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const (
	objectKey = "object-test-key"
)

func TestRing_GetLoadBalancers(t *testing.T) {
	h := &testHandler{}
	memberCount := 3
	h.joins = make(chan map[string]string, memberCount)
	h.leaves = make(chan string, memberCount)
	leaderIndex := 0
	loadBalancerIndex := 2
	ringMembers := setupTestRingMembers(t, memberCount, func(ringMemberConfig *Config, i int) {
		if i == loadBalancerIndex {
			ringMemberConfig.MemberType = LoadBalancerMember
		} else {
			ringMemberConfig.MemberType = ShardMember
		}
	}, func(ringMember *Ring, i int) {
		if i == leaderIndex {
			ringMember.AddListener("test-listener-0", h)
		}
	})

	loadBalancers := ringMembers[leaderIndex].GetLoadBalancers()
	require.Equal(t, 1, len(loadBalancers))

	_, ok := ringMembers[loadBalancerIndex].Tags["rpc_addr"]
	require.True(t, ok)

	require.Equal(t, ringMembers[loadBalancerIndex].NodeName, loadBalancers[0])

	defer func() {
		for _, ringMember := range ringMembers {
			err := ringMember.Shutdown()
			require.NoError(t, err)
		}
	}()
}

func TestRing_ConsistentHashRouter_For_Single_Node_Ring(t *testing.T) {
	ringMembers := setupTestRingMembers(t, 1, nil, nil)

	time.Sleep(100 * time.Millisecond)

	node, ok := ringMembers[0].GetNode(objectKey)
	require.Equal(t, true, ok)
	require.NotEmpty(t, node)
}

func TestRing_ConsistentHashRouter(t *testing.T) {
	ringMembers := setupTestRingMembers(t, 2, nil, nil)

	node, ok := ringMembers[0].GetNode(objectKey)
	require.Equal(t, true, ok)
	require.NotEmpty(t, node)
}

func TestRing_ConfigurationRequest(t *testing.T) {
	h := &testHandler{}
	h.joins = make(chan map[string]string, 1)
	h.leaves = make(chan string, 1)

	// 3 nodes on the ring
	ringMember0 := setupRing_Member(t, []string{}, 0, func(config *Config, i int) {
		config.MemberType = ShardMember
	})
	ringMember1 := setupRing_Member(t, []string{ringMember0.BindAddr}, 1, func(config *Config, i int) {
		config.MemberType = ShardMember
	})
	ringMember2 := setupRing_Member(t, []string{ringMember0.BindAddr, ringMember1.BindAddr}, 2, func(config *Config, i int) {
		config.MemberType = ShardMember
	})
	ringMember3 := setupRing_Member(t, []string{ringMember0.BindAddr, ringMember1.BindAddr}, 2, func(config *Config, i int) {
		config.MemberType = ShardMember
	})

	resp := &ConfigurationResponse{}
	err := ringMember2.transport.SendConfigurationRequest(ServerAddress(ringMember0.BindAddr), &ConfigurationRequest{}, resp)
	require.NoError(t, err)
	require.NotNil(t, resp.StartupConfig)

	require.Equal(t, 3, len(resp.StartupConfig.Nodes)) // total nodes on the ring.

	for _, node := range resp.StartupConfig.Nodes {
		require.Equal(t, 3, len(node.VirtualNodes)) // we have set the number of virtual nodes to 3.
	}

	resp = &ConfigurationResponse{}
	err = ringMember3.transport.SendConfigurationRequest(ServerAddress(ringMember1.BindAddr), &ConfigurationRequest{}, resp)
	require.NoError(t, err)
	require.NotNil(t, resp.StartupConfig)

	require.Equal(t, 3, len(resp.StartupConfig.Nodes)) // total nodes on the ring.

	for _, node := range resp.StartupConfig.Nodes {
		require.Equal(t, 3, len(node.VirtualNodes)) // we have set the number of virtual nodes to 3.
	}
}

type testHandler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *testHandler) Join(nodeKey string, tags map[string]string) error {
	if h.joins != nil {
		h.joins <- tags
	}
	return nil
}

func (h *testHandler) Leave(nodeKey string, tags map[string]string) error {
	if h.leaves != nil {
		h.leaves <- nodeKey
	}
	return nil
}

func setupTestRingMembers(t *testing.T, count int, beforeRingMember func(*Config, int), afterRingMember func(*Ring, int)) []*Ring {
	var ringMembers []*Ring
	for i := 0; i < count; i++ {
		var seedAddresses []string
		if i > 0 {
			seedAddresses = []string{ringMembers[0].BindAddr}
		}

		ringMember := setupRing_Member(t, seedAddresses, i, beforeRingMember)
		if afterRingMember != nil {
			afterRingMember(ringMember, i)
		}
		ringMembers = append(ringMembers, ringMember)
	}
	return ringMembers
}

func setupRing_Member(t *testing.T, seedAddresses []string, i int, beforeRingMember func(config *Config, i int)) *Ring {
	t.Helper()

	ports := dynaport.Get(2)
	bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	rpcPort := ports[1]

	config := Config{
		NodeName: fmt.Sprintf("Ring Member %d", i),
		BindAddr: bindAddr,
		Tags: map[string]string{
			"rpc_addr": fmt.Sprintf("localhost:%d", rpcPort),
		},
		VirtualNodeCount: 3,
		SeedAddresses:    seedAddresses,
		Timeout:          time.Second,
	}

	if beforeRingMember != nil {
		beforeRingMember(&config, i)
	}

	ringMember, err := NewRing(config)
	require.NoError(t, err)

	return ringMember
}
