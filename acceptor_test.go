package ring

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"strconv"
	"testing"
	"time"
)

func TestNewAcceptor(t *testing.T) {
	nodeCount := 10
	var nodes []*testPaxosNode
	var members []*membership
	for i := 0; i < nodeCount; i++ {
		transport := setupAcceptor_Transport(t)
		acceptor := newAcceptor(fmt.Sprintf("acceptor-%d", i), transport, nil)
		proposer := newProposer(fmt.Sprintf("proposer-%d", i), transport, nil)

		node := &testPaxosNode{
			acceptor: acceptor,
			proposor: proposer,
		}
		members = setupAcceptor_Membership(t, members, ShardMember, node)
		nodes = append(nodes, node)
	}

	require.Equal(t, nodeCount, len(members[0].Members()))

	time.Sleep(1 * time.Second)

	// maintain the members
	require.Equal(t, nodeCount, len(members[0].Members()))

	key := "round-1"
	for i := 0; i < nodeCount; i++ {
		proposal := &proposal{
			Key:   key,
			Value: []byte(fmt.Sprintf("Value_%d", i)),
		}

		err := nodes[i].proposor.Propose(proposal)
		require.NoError(t, err)
	}

	value := &proposal{}
	// We need the same value from all learners.
	for i, node := range nodes {
		accepted, err := node.acceptor.learn(key, 2*time.Second)

		require.NoError(t, err)

		if i == 0 {
			value = accepted
		} else {
			require.Equal(t, value, accepted)
		}
	}

	defer func() {
		for _, member := range members {
			err := member.Leave()
			require.NoError(t, err)
		}
	}()
}

type testPaxosNode struct {
	acceptor *acceptor
	proposor *proposer
}

func setupAcceptor_Transport(t *testing.T) *Transport {
	t.Helper()
	ports := dynaport.Get(1)
	rpcAddr := fmt.Sprintf("localhost:%d", ports[0])

	streamLayer, err := newTCPStreamLayer(rpcAddr, nil)
	require.NoError(t, err)

	return NewTransportWithConfig(&TransportConfig{
		Stream:  streamLayer,
		Timeout: 250 * time.Millisecond,
	})
}

func setupAcceptor_Membership(t *testing.T, members []*membership, memberType MemberType, node *testPaxosNode) []*membership {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr":       addr,
		virtualNodesJSON: "3",
		memberTypeJSON:   strconv.Itoa(int(memberType)),
		ringRPCAddrJSON:  node.proposor.transport.stream.Addr().String(),
	}
	c := MembershipConfig{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handlerWrapper{listeners: make(map[string]Handler)}
	h.listeners["learner"] = node.acceptor
	h.listeners["proposer"] = node.proposor
	if len(members) != 0 {
		c.SeedAddresses = []string{members[0].BindAddr}
	}
	m, err := newMemberShip(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members
}
