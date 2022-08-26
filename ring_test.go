package ring_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const (
	objectKey = "object-test-key"
)

func TestRing_Listener(t *testing.T) {
	h := &handler{}
	h.joins = make(chan map[string]string, 3)
	h.leaves = make(chan string, 3)
	ringMembers := setupTestRingMembers(t, 3, func(ringMemberConfig *ring.Config, i int) {
		if i == 2 {
			ringMemberConfig.MemberType = ring.LoadBalancerMember
		} else {
			ringMemberConfig.MemberType = ring.ShardMember
		}
	},
		func(ringMember *ring.Ring, i int) {
			if i == 0 {
				ringMember.AddListener("test-listener-0", h)
			}
			if i == 2 {
				ringMember.MemberType = ring.LoadBalancerMember
			}
		})

	require.Eventually(t, func() bool {
		return 2 == len(h.joins) &&
			0 == len(h.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	memberId := <-h.joins
	memberAddr, err := ringMembers[1].RPCAddr()
	require.NoError(t, err)

	require.Equal(t, memberAddr, memberId["rpc_addr"])

	defer func() {
		err := ringMembers[1].Shutdown()
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return 1 == len(h.joins) &&
				1 == len(h.leaves)
		}, 3*time.Second, 250*time.Millisecond)

		err = ringMembers[2].Shutdown()
		require.NoError(t, err)

		err = ringMembers[0].Shutdown()
		require.NoError(t, err)
	}()
}

func TestRing_GetLoadBalancers(t *testing.T) {
	h := &handler{}
	h.joins = make(chan map[string]string, 3)
	h.leaves = make(chan string, 3)
	leaderIndex := 0
	loadBalancerIndex := 2
	ringMembers := setupTestRingMembers(t, 3, func(ringMemberConfig *ring.Config, i int) {
		if i == 2 {
			ringMemberConfig.MemberType = ring.LoadBalancerMember
		} else {
			ringMemberConfig.MemberType = ring.ShardMember
		}
	}, func(ringMember *ring.Ring, i int) {
		if i == leaderIndex {
			ringMember.AddListener("test-listener-0", h)
		}
		if i == loadBalancerIndex {
			ringMember.MemberType = ring.LoadBalancerMember
		}
	})

	loadBalancers := ringMembers[leaderIndex].GetLoadBalancers()
	require.Equal(t, 1, len(loadBalancers))

	loadBalancerRPCAddr, err := ringMembers[loadBalancerIndex].RPCAddr()
	require.NoError(t, err)

	require.Equal(t, loadBalancerRPCAddr, loadBalancers[0])

	defer func() {
		for _, ringMember := range ringMembers {
			err := ringMember.Shutdown()
			require.NoError(t, err)
		}
	}()
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(nodeKey string, vNodeCount int, memberType ring.MemberType) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"rpc_addr":      nodeKey,
			"virtual_nodes": strconv.Itoa(vNodeCount),
			"member_type":   strconv.Itoa(int(memberType)),
		}
	}
	return nil
}

func (h *handler) Leave(id string, memberType ring.MemberType) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func TestRing_ConsistentHashRouter_For_Single_Node_Ring(t *testing.T) {
	ringMembers := setupTestRingMembers(t, 1, nil, nil)

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

func setupTestRingMembers(t *testing.T, count int, beforeRingMember func(*ring.Config, int), afterRingMember func(*ring.Ring, int)) []*ring.Ring {
	var ringMembers []*ring.Ring

	for i := 0; i < count; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		var seedAddresses []string
		if i > 0 {
			seedAddresses = []string{ringMembers[0].Config.BindAddr}
		}

		config := ring.Config{
			NodeName:         fmt.Sprintf("Ring Member %d", i),
			BindAddr:         bindAddr,
			RPCPort:          rpcPort,
			VirtualNodeCount: 3,
			SeedAddresses:    seedAddresses,
		}

		if beforeRingMember != nil {
			beforeRingMember(&config, i)
		}

		ringMember, err := ring.NewRing(config)
		require.NoError(t, err)

		if afterRingMember != nil {
			afterRingMember(ringMember, i)
		}

		ringMembers = append(ringMembers, ringMember)
	}

	return ringMembers
}
