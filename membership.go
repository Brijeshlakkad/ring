package ring

import (
	"net"
	"strconv"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership Type wrapping Serf to provide discovery and cluster membership to our service.
type Membership struct {
	MembershipConfig
	handler Handler
	cluster *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func NewMemberShip(handler Handler, config MembershipConfig) (*Membership, error) {
	c := &Membership{
		MembershipConfig: config,
		handler:          handler,
		logger:           zap.L().Named("membership"),
	}
	if err := c.setupCluster(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *Membership) setupCluster() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName
	m.cluster, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.SeedAddresses != nil && len(m.SeedAddresses) > 0 {
		_, err = m.cluster.Join(m.SeedAddresses, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// Runs in a loop reading events sent by Serf into the events channel, handling each incoming event according to the event’s type.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	vNodeCount, err := strconv.Atoi(member.Tags["virtual_nodes"])
	if err != nil {
		m.logError(err, "failed to join", member)
	}
	if err := m.handler.Join(
		member.Tags["rpc_addr"],
		vNodeCount,
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member by checking the members’ names.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.cluster.LocalMember().Name == member.Name
}

// Members Returns a point-in-time snapshot of the cluster’s Serf members.
func (m *Membership) Members() []serf.Member {
	return m.cluster.Members()
}

// Leave Tells this member to leave the Serf cluster.
func (m *Membership) Leave() error {
	return m.cluster.Leave()
}

// Logs the given error and message.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

type MembershipConfig struct {
	NodeName      string
	BindAddr      string
	Tags          map[string]string
	SeedAddresses []string
	virtualNodes  int
}
