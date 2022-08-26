package ring

import (
	"fmt"
	"net"
	"strconv"
	"sync"

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
}

type Config struct {
	BindAddr         string
	RPCPort          int
	NodeName         string
	SeedAddresses    []string
	VirtualNodeCount int
	HashFunction     HashFunction
	Logger           hclog.Logger
	MemberType       MemberType
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func NewRing(config Config) (*Ring, error) {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "ring",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	r := &Ring{
		Config:    config,
		shutdowns: make(chan struct{}),
		handler: newHandlerWrapper(&handlerWrapperConfig{
			Logger: config.Logger,
		}),
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

func (r *Ring) setupConsistentHashRouter() (func() error, error) {
	var err error
	r.router, err = newConsistentHashRouter(r.HashFunction)
	if err != nil {
		return nil, err
	}

	rpcAddr, err := r.RPCAddr()
	if err != nil {
		return nil, err
	}

	// Add this node on the ring.
	err = r.router.Join(rpcAddr, r.VirtualNodeCount, r.MemberType)
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
	rpcAddr, err := r.Config.RPCAddr()
	if err != nil {
		return nil, err
	}
	r.membership, err = newMemberShip(r.handler, MembershipConfig{
		NodeName: r.Config.NodeName,
		BindAddr: r.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr":      rpcAddr,
			"virtual_nodes": strconv.Itoa(r.Config.VirtualNodeCount),
			"member_type":   strconv.Itoa(int(r.Config.MemberType)),
		},
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

// GetNode gets the node responsible for the given #objKey.
func (r *Ring) GetNode(objKey string) (interface{}, bool) {
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

func (h *handlerWrapper) Join(rpcAddr string, vNodeCount int, memberType MemberType) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for listenerId, listener := range h.listeners {
		if err := listener.Join(rpcAddr, vNodeCount, memberType); err != nil {
			h.logger.Error(fmt.Sprintf("Error while joining %s", listenerId), "error", err)
		}
	}
	return nil
}

func (h *handlerWrapper) Leave(rpcAddr string, memberType MemberType) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	for listenerId, listener := range h.listeners {
		if err := listener.Leave(rpcAddr, memberType); err != nil {
			h.logger.Error(fmt.Sprintf("Error while leaving %s", listenerId), "error", err)
		}
	}
	return nil
}
