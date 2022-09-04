package ring_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const (
	objectKey = "object-test-key"
)

func TestRing_GetLoadBalancers(t *testing.T) {
	h := &handler{}
	memberCount := 3
	h.joins = make(chan map[string]string, memberCount)
	h.leaves = make(chan string, memberCount)
	leaderIndex := 0
	loadBalancerIndex := 2
	ringMembers := setupTestRingMembers(t, memberCount, func(ringMemberConfig *ring.Config, i int) {
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

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(nodeKey string, tags map[string]string) error {
	if h.joins != nil {
		h.joins <- tags
	}
	return nil
}

func (h *handler) Leave(nodeKey string, tags map[string]string) error {
	if h.leaves != nil {
		h.leaves <- nodeKey
	}
	return nil
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
			NodeName: fmt.Sprintf("Ring Member %d", i),
			BindAddr: bindAddr,
			Tags: map[string]string{
				"rpc_addr": fmt.Sprintf("localhost:%d", rpcPort),
			},
			VirtualNodeCount: 3,
			SeedAddresses:    seedAddresses,
			StreamLayer:      setupRing_StreamLayer(t),
			Timeout:          time.Millisecond,
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

func setupRing_StreamLayer(t *testing.T) ring.StreamLayer {
	t.Helper()
	ports := dynaport.Get(1)
	rpcAddr := fmt.Sprintf("localhost:%d", ports[0])
	listener, err := net.Listen("tcp", rpcAddr)
	require.NoError(t, err)

	streamLayer := &testStreamLayer{
		listener: listener,
	}
	require.NoError(t, err)

	return streamLayer
}

type testStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
}

// Dial implements the StreamLayer interface.
func (t *testStreamLayer) Dial(address ring.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
func (t *testStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *testStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *testStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
