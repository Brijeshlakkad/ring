package ring

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
)

type Member struct {
	Config
	handler *HandlerWrapper

	server     *grpc.Server
	router     *ConsistentHashRouter
	membership *Membership

	shutdownListeners []func() error
	shutdown          bool
	shutdowns         chan struct{}
	shutdownLock      sync.Mutex
}

type Config struct {
	BindAddr         string
	RPCPort          int
	NodeName         string
	SeedAddresses    []string
	VirtualNodeCount int
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func NewMember(config Config) (*Member, error) {
	r := &Member{
		Config:    config,
		shutdowns: make(chan struct{}),
		handler:   NewHandlerWrapper(),
	}
	setup := []func() (func() error, error){
		r.setupConsistentHashRouter,
		r.setupMembership,
	}
	for _, fn := range setup {
		shutdownListener, err := fn()
		if err != nil {
			return nil, err
		}
		if shutdownListener != nil {
			r.shutdownListeners = append(r.shutdownListeners, shutdownListener)
		}
	}
	return r, nil
}

func (m *Member) setupConsistentHashRouter() (func() error, error) {
	var err error
	m.router, err = NewConsistentHashRouter(nil)
	if err != nil {
		return nil, err
	}
	m.AddListener("router", m.router)

	return nil, nil
}

func (m *Member) setupMembership() (func() error, error) {
	rpcAddr, err := m.Config.RPCAddr()
	if err != nil {
		return nil, err
	}
	m.membership, err = NewMemberShip(m.handler, MembershipConfig{
		NodeName: m.Config.NodeName,
		BindAddr: m.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr":      rpcAddr,
			"virtual_nodes": strconv.Itoa(m.Config.VirtualNodeCount),
		},
		SeedAddresses: m.Config.SeedAddresses,
	})
	if err != nil {
		return nil, err
	}
	return func() error {
		if err := m.membership.Leave(); err != nil {
			return err
		}
		return nil
	}, nil
}

func (m *Member) AddListener(listenerId string, handler Handler) {
	m.handler.lock.Lock()
	defer m.handler.lock.Unlock()

	m.handler.listeners[listenerId] = handler

	// Will be removed when Member shuts down.
	m.shutdownListeners = append(m.shutdownListeners, func() error {
		m.RemoveListener(listenerId)
		return nil
	})
}

func (m *Member) RemoveListener(listenerId string) {
	m.handler.lock.Lock()
	defer m.handler.lock.Unlock()

	delete(m.handler.listeners, listenerId)
}

func (m *Member) Shutdown() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()
	if m.shutdown {
		return nil
	}
	m.shutdown = true
	close(m.shutdowns)

	for _, fn := range m.shutdownListeners {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

type HandlerWrapper struct {
	listeners map[string]Handler
	lock      sync.Mutex
}

func NewHandlerWrapper() *HandlerWrapper {
	return &HandlerWrapper{
		listeners: map[string]Handler{},
	}
}

func (h *HandlerWrapper) Join(nodeKey string, vNodeCount int) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, listener := range h.listeners {
		if err := listener.Join(nodeKey, vNodeCount); err != nil {
			return err
		}
	}
	return nil
}

func (h *HandlerWrapper) Leave(nodeKey string) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, listener := range h.listeners {
		if err := listener.Leave(nodeKey); err != nil {
			return err
		}
	}
	return nil
}
