package ring

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

type Ring struct {
	Config
	handler *handlerWrapper

	router     *consistentHashRouter
	membership *membership

	shutdownListeners []func() error
	shutdown          bool
	shutdowns         chan struct{}
	shutdownLock      sync.Mutex

	logger    hclog.Logger
	transport *Transport

	acceptor *acceptor
	proposor *proposer
}

type Config struct {
	BindAddr         string
	Tags             map[string]string
	NodeName         string
	SeedAddresses    []string
	VirtualNodeCount int
	HashFunction     HashFunction
	Logger           hclog.Logger
	MemberType       MemberType
	// Dialer
	StreamLayer StreamLayer
	Timeout     time.Duration
}

func NewRing(config Config) (*Ring, error) {
	logger := config.Logger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "ring",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  config.StreamLayer,
			Logger:  logger,
			Timeout: config.Timeout,
		},
	)
	r := &Ring{
		Config:    config,
		shutdowns: make(chan struct{}),
		handler: newHandlerWrapper(&handlerWrapperConfig{
			Logger: config.Logger,
		}),
		logger:    logger,
		transport: transport,
	}
	setup := []func() (func() error, error){
		r.setupConsistentHashRouter,
		r.setupMembership,
		r.setupPaxos,
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

func (r *Ring) setupConsistentHashRouter() (func() error, error) {
	var err error
	r.router, err = newConsistentHashRouter(r.HashFunction, r.NodeName, nil)
	if err != nil {
		return nil, err
	}

	r.AddListener(r.NodeName, r.router)

	// Remove listener upon shutdown.
	return func() error {
		r.RemoveListener(r.NodeName)
		return nil
	}, nil
}

func (r *Ring) setupMembership() (func() error, error) {
	var err error
	r.membership, err = newMemberShip(r.handler, MembershipConfig{
		NodeName:      r.Config.NodeName,
		BindAddr:      r.Config.BindAddr,
		Tags:          r.tags(),
		SeedAddresses: r.Config.SeedAddresses,
	})
	if err != nil {
		return nil, err
	}
	return func() error {
		if err := r.membership.Leave(); err != nil {
			return err
		}
		return nil
	}, nil
}

func (r *Ring) setupPaxos() (func() error, error) {
	r.acceptor = newAcceptor(r.NodeName, r.transport, r.logger)
	r.proposor = newProposer(r.NodeName, r.transport, r.logger)

	r.AddListener("paxos-proposer", r.proposor)
	return nil, nil
}

// AddListener registers the listener that will be called upon the node join/leave event in the ring.
func (r *Ring) AddListener(listenerId string, handler Handler) {
	r.handler.lock.Lock()
	defer r.handler.lock.Unlock()

	if r.shutdown {
		return
	}
	r.handler.listeners[listenerId] = handler
}

// RemoveListener removes the listener using the listenerId.
func (r *Ring) RemoveListener(listenerId string) {
	r.handler.lock.Lock()
	defer r.handler.lock.Unlock()

	if r.shutdown {
		return
	}
	delete(r.handler.listeners, listenerId)
}

// AddResponsibilityChangeListener registers the listener that will be called when the current node responsibility changes due to joining of the new nodes.
func (r *Ring) AddResponsibilityChangeListener(listenerId string, handler ShardResponsibilityHandler) {
	r.router.AddListener(listenerId, handler)
}

// RemoveResponsibilityChangeListener removes the listener from the router using the listenerId.
func (r *Ring) RemoveResponsibilityChangeListener(listenerId string) {
	r.router.RemoveListener(listenerId)
}

// GetNode gets the node responsible for the given #objKey.
func (r *Ring) GetNode(objKey string) (map[string]string, bool) {
	if r.shutdown {
		return nil, false
	}
	return r.router.Get(objKey)
}

// GetLoadBalancers gets the load balancers of the ring.
func (r *Ring) GetLoadBalancers() []string {
	if r.shutdown {
		return nil
	}
	return r.router.GetLoadBalancers()
}

func (r *Ring) Shutdown() error {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()
	if r.shutdown {
		return nil
	}
	r.shutdown = true
	close(r.shutdowns)

	for _, fn := range r.shutdownListeners {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

const (
	memberTypeJSON   = "member_type"
	virtualNodesJSON = "virtual_nodes"
	ringRPCAddrJSON  = "ring_rpc_addr"
)

func (r *Ring) tags() map[string]string {
	tagsToBeSent := make(map[string]string)
	for k, v := range r.Tags {
		tagsToBeSent[k] = v
	}
	tagsToBeSent[virtualNodesJSON] = strconv.Itoa(r.Config.VirtualNodeCount)
	tagsToBeSent[memberTypeJSON] = strconv.Itoa(int(r.Config.MemberType))
	if r.StreamLayer != nil {
		tagsToBeSent[ringRPCAddrJSON] = r.StreamLayer.Addr().String()
	}
	return tagsToBeSent
}

type handlerWrapper struct {
	listeners map[string]Handler
	lock      sync.Mutex
	logger    hclog.Logger
}

type handlerWrapperConfig struct {
	Logger hclog.Logger
}

func newHandlerWrapper(config *handlerWrapperConfig) *handlerWrapper {
	return &handlerWrapper{
		listeners: map[string]Handler{},
		logger:    config.Logger,
	}
}

func (h *handlerWrapper) Join(nodeName string, tags map[string]string) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for listenerId, listener := range h.listeners {
		if err := listener.Join(nodeName, tags); err != nil {
			h.logger.Error(fmt.Sprintf("Error while joining %s", listenerId), "error", err)
		}
	}
	return nil
}

func (h *handlerWrapper) Leave(rpcAddr string, tags map[string]string) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for listenerId, listener := range h.listeners {
		if err := listener.Leave(rpcAddr, tags); err != nil {
			h.logger.Error(fmt.Sprintf("Error while leaving %s", listenerId), "error", err)
		}
	}
	return nil
}
