package ring

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/travisjeffery/go-dynaport"

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

	// rpcCh comes from the transport.
	rpcCh <-chan rpc

	membershipBindAddr      string
	membershipSeedAddresses []string // To join the cluster.
	startupConfig           *StartupConfig
}

type Config struct {
	BindAddr         string
	Tags             map[string]string
	NodeName         string
	SeedAddresses    []string // Ring RPC address of the nodes
	VirtualNodeCount int
	HashFunction     HashFunction
	MemberType       MemberType
	Timeout          time.Duration
	Logger           hclog.Logger
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
	// setup membership binding address
	host, _, err := net.SplitHostPort(config.BindAddr)
	if err != nil {
		return nil, err
	}
	membershipBindPort := dynaport.Get(1)[0]
	membershipBindAddr := fmt.Sprintf("%s:%d", host, membershipBindPort)

	// create transport
	list, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, err
	}
	streamLayer := newRingStreamLayer(list, nil, nil)
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Logger:  logger,
			Timeout: config.Timeout,
		},
	)
	r := &Ring{
		Config:    config,
		shutdowns: make(chan struct{}),
		handler: newHandlerWrapper(&handlerWrapperConfig{
			Logger: logger,
		}),
		logger:             logger,
		transport:          transport,
		rpcCh:              transport.RingConsumer(),
		membershipBindAddr: membershipBindAddr,
	}
	_, err = r.startup(config.SeedAddresses)
	if err != nil {
		return nil, err
	}
	// Join the ring if
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
	go r.run()

	return r, nil
}

func (r *Ring) run() {
	for {
		select {
		case rpc := <-r.rpcCh:
			r.processCommand(rpc)
		case <-r.shutdowns:
			return
		}
	}
}

func (r *Ring) processCommand(rpc rpc) {
	switch rpc.Command.(type) {
	case *ConfigurationRequest:
		startupConfig := r.router.getConfig()
		resp := &ConfigurationResponse{
			StartupConfig: startupConfig,
			BindAddr:      r.membershipBindAddr,
		}
		rpc.Respond(resp, nil)
	default:
		r.logger.Error("unexpected command", rpc.Command, "from", rpc.From)
	}
}

func (r *Ring) setupConsistentHashRouter() (func() error, error) {
	var err error
	rConfig := routerConfig{
		NodeName:      r.NodeName,
		HashFunction:  r.HashFunction,
		startupConfig: r.startupConfig,
	}
	r.router, err = newConsistentHashRouter(rConfig)
	if err != nil {
		return nil, err
	}

	// register Handler to membership
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
		BindAddr:      r.membershipBindAddr,
		Tags:          r.tags(),
		SeedAddresses: r.membershipSeedAddresses,
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
	tagsToBeSent[ringRPCAddrJSON] = r.BindAddr
	return tagsToBeSent
}

// startup fetches the startup configuration to join the cluster and prepare the ring.
func (r *Ring) startup(seeds []string) (int, error) {
	numSuccess := 0
	var startupConfig *StartupConfig = nil
	var errs error
	var memberSeedAddresses []string
SeedLoop:
	for _, nodeAddr := range seeds {
		// fetch the ring configuration from any seed node.
		resp := &ConfigurationResponse{}
		err := r.transport.SendConfigurationRequest(ServerAddress(nodeAddr), &ConfigurationRequest{}, resp)
		if err != nil {
			return 0, err
		}
		if startupConfig != nil {
			if len(startupConfig.Nodes) != len(resp.StartupConfig.Nodes) {
				continue SeedLoop
			}
			// checks consistency.
			for nodeKey, nodeValue := range startupConfig.Nodes {
				rNodeValue, ok := resp.StartupConfig.Nodes[nodeKey]
				if !ok || nodeValue.NodeKey != rNodeValue.NodeKey || len(nodeValue.Tags) != len(rNodeValue.Tags) || len(nodeValue.VirtualNodes) != len(rNodeValue.VirtualNodes) {
					continue SeedLoop
				}
				// compare tags
				for tagKey, tagValue := range nodeValue.Tags {
					rTagValue, ok := rNodeValue.Tags[tagKey]
					if !ok || tagValue != rTagValue {
						continue SeedLoop
					}
				}
				// compare virtual nodes
				for virtualNodeKey, virtualNodeValue := range nodeValue.VirtualNodes {
					rVirtualNodeValue, ok := rNodeValue.VirtualNodes[virtualNodeKey]
					if !ok || virtualNodeValue != rVirtualNodeValue {
						continue SeedLoop
					}
				}
			}
		}
		startupConfig = resp.StartupConfig
		memberSeedAddresses = append(memberSeedAddresses, resp.BindAddr)
		numSuccess++
	}
	if numSuccess > 0 {
		errs = nil
	}
	r.startupConfig = startupConfig
	r.membershipSeedAddresses = memberSeedAddresses
	return numSuccess, errs
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

// StartupConfig to set the router according to the ring.
type StartupConfig struct {
	Nodes map[uint64]ConfigurationNode
}
