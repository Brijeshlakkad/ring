package ring

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil, ShardMember)
	m, _ = setupMember(t, m, LoadBalancerMember)
	m, _ = setupMember(t, m, ShardMember)
	m, _ = setupMember(t, m, ShardMember)
	m, _ = setupMember(t, m, LoadBalancerMember)

	require.Eventually(t, func() bool {
		return 5 == len(handler.joins) &&
			5 == len(m[0].Members()) &&
			0 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 5 == len(handler.joins) &&
			5 == len(m[0].Members()) &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			1 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, m[2].NodeName, <-handler.leaves)
}

func setupMember(t *testing.T, members []*membership, memberType MemberType) ([]*membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr":       addr,
		virtualNodesJSON: "3",
		memberTypeJSON:   strconv.Itoa(int(memberType)),
	}
	c := MembershipConfig{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 5)
		h.leaves = make(chan string, 5)
	} else {
		c.SeedAddresses = []string{members[0].BindAddr}
	}
	m, err := newMemberShip(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
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
