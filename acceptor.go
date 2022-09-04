package ring

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"sync"
	"time"
)

// acceptor ...
type acceptor struct {
	nodeName string

	promised map[string]*proposal
	accepted map[string]*proposal

	// Shutdown channel to exit, protected to prevent concurrent exits
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// rpcCh comes from the transport.
	rpcCh     <-chan rpc
	transport *Transport

	logger       hclog.Logger
	acceptorLock sync.RWMutex

	// To learn the consensus value for the given key.
	learners    map[string]*paxosClient
	acceptors   map[string]map[string]*proposal
	learnerLock sync.RWMutex
}

// newAcceptor creates a new acceptor instance
func newAcceptor(nodeName string, transport *Transport, logger hclog.Logger) *acceptor {
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "acceptor",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	acceptor := &acceptor{
		nodeName:  nodeName,
		promised:  make(map[string]*proposal),
		accepted:  make(map[string]*proposal),
		learners:  make(map[string]*paxosClient),
		acceptors: make(map[string]map[string]*proposal),
		rpcCh:     transport.Consumer(), // Comes from the transport.
		transport: transport,
		logger:    logger,
	}

	// TODO: Should keep track of goroutines for shutdown
	go acceptor.run()

	return acceptor
}

func (a *acceptor) run() {
	for {
		select {
		case rpc := <-a.rpcCh:
			msg := rpc.Command.(*Message)
			switch msg.MsgType {
			case PrepareMsgType:
				resp, err := a.ReceivePrepare(msg.Data)
				if resp != nil {
					msg := &Message{
						Data:    resp,
						MsgType: PromiseMsgType,
						From:    ServerAddress(a.transport.stream.Addr().String()),
					}
					rpc.Respond(msg, err)
				} else {
					rpc.Respond(nil, err)
				}
			case ProposalMsgType:
				accepted, err := a.ReceivePropose(msg.Data)
				if accepted != nil {
					msg := &Message{
						Data:    accepted,
						MsgType: AcceptMsgType,
						From:    ServerAddress(a.transport.stream.Addr().String()),
					}
					rpc.Respond(msg, err)
				} else {
					rpc.Respond(nil, err)
				}
			case AcceptMsgType:
				a.receiveAccepted(msg)
				// Respond with nil as learner doesn't respond with anything.
				rpc.Respond(nil, nil)
			}
		case <-a.shutdownCh:
			return
		}
	}
}

// ReceivePrepare If an acceptor receives a prepare request with number n greater
// than that of any prepare request to which it has already responded,
// then it responds to the request with a promise not to accept any more
// proposals numbered less than n and with the highest-numbered proposal
// (if any) that it has accepted.
func (a *acceptor) ReceivePrepare(proposal *proposal) (*proposal, error) {
	a.acceptorLock.RLock()
	defer a.acceptorLock.RUnlock()

	// Do we already have a promise for this proposal
	promised, ok := a.promised[proposal.Key]

	// Ignore lesser or equally numbered proposals
	if ok && promised.Number >= proposal.Number {
		return nil, fmt.Errorf(
			"already promised to accept %s which is >= than requested %s",
			promised,
			proposal,
		)
	}

	// Promise to accept the proposal
	a.promised[proposal.Key] = proposal

	a.logger.Info(a.nodeName, "Promised to accept", proposal)

	return proposal, nil
}

// ReceivePropose If an acceptor receives a propose request for a proposal numbered
// n, it accepts the proposal unless it has already responded to a prepare
// request having a number greater than n.
func (a *acceptor) ReceivePropose(proposal *proposal) (*proposal, error) {
	a.acceptorLock.RLock()
	defer a.acceptorLock.RUnlock()

	// Do we already have a promise for this proposal
	promised, ok := a.promised[proposal.Key]

	// Ignore lesser numbered proposals
	if ok && promised.Number > proposal.Number {
		return nil, fmt.Errorf(
			"already promised to accept %s which is > than requested %s",
			promised,
			proposal,
		)
	}

	// Unexpected proposal
	if ok && promised.Number < proposal.Number {
		return nil, fmt.Errorf("received unexpected proposal %s", proposal)
	}

	// Accept the proposal
	a.accepted[proposal.Key] = proposal

	a.logger.Info(a.nodeName, "Accepted", proposal)

	// Send the accepted proposal to all learners.
	for _, learnerClient := range a.learners {
		_, _ = learnerClient.SendPaxosMessage(proposal, AcceptMsgType)
	}

	return proposal, nil
}

// Join adds the newly joined node to the list of acceptor clients.
func (a *acceptor) Join(nodeName string, tags map[string]string) error {
	a.acceptorLock.Lock()
	defer a.acceptorLock.Unlock()

	ringRPCAddrStr, ok := tags[ringRPCAddrJSON]
	if ok {
		a.learners[ringRPCAddrStr] = &paxosClient{
			Transport: a.transport,
			address:   ServerAddress(ringRPCAddrStr),
		}
	}
	return nil
}

// Leave adds the newly joined node to the list of acceptor clients.
func (a *acceptor) Leave(nodeName string, tags map[string]string) error {
	a.acceptorLock.Lock()
	defer a.acceptorLock.Unlock()

	ringRPCAddrStr, ok := tags[ringRPCAddrJSON]
	if ok {
		delete(a.learners, ringRPCAddrStr)
	}
	return nil
}

func (a *acceptor) learn(key string, timeout time.Duration) (*proposal, error) {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	for {
		select {
		case <-timer:
			a.logger.Info("learner", a.nodeName, "timeout learning the value for", key)
			return nil, errors.New(fmt.Sprintf("timeout learning the value for %s", key))
		default:
			a.learnerLock.RLock()
			accept, ok := a.chosen(key)
			a.learnerLock.RUnlock()
			if ok {
				a.logger.Info("learner", a.nodeName, "learned the chosen propose", accept)
				return accept, nil
			}
		}
	}
}

// A value is learned when a single proposal with that value has been accepted by
// a majority of the acceptors.
func (a *acceptor) receiveAccepted(msg *Message) {
	a.learnerLock.Lock()
	defer a.learnerLock.Unlock()

	accepted := msg.Data
	from := msg.From

	if _, ok := a.acceptors[string(from)]; !ok {
		a.acceptors[string(from)] = make(map[string]*proposal)
	}
	// Get the previous promise
	previousPromise, ok := a.acceptors[string(from)][accepted.Key]

	// Previous promise is equal or greater than the new proposal, continue
	if ok && previousPromise.Number >= accepted.Number {
		return
	}

	a.logger.Info("learner", a.nodeName, " received a new accepted proposal", accepted, string(from))
	a.acceptors[string(from)][accepted.Key] = accepted
}

func (a *acceptor) majority() int { return len(a.acceptors)/2 + 1 }

// A proposal is chosen when it has been accepted by a majority of the
// acceptors.
// The leader might choose multiple proposals when it learns multiple times,
// but we guarantee that all chosen proposals have the same value.
func (a *acceptor) chosen(key string) (*proposal, bool) {
	counts := make(map[int]int)
	accepteds := make(map[int]*proposal)

	for _, accepted := range a.acceptors {
		if accepted[key] != nil {
			counts[accepted[key].Number]++
			accepteds[accepted[key].Number] = accepted[key]
		}
	}

	for n, count := range counts {
		if count >= a.majority() {
			return accepteds[n], true
		}
	}
	return nil, false
}
