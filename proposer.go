package ring

import (
	"github.com/hashicorp/go-hclog"
	"sync"
)

// proposer proposes new values to acceptors
type proposer struct {
	nodeName         string
	acceptorClients  map[string]*paxosClient
	acceptorPromises map[string]map[string]*proposal
	transport        *Transport
	logger           hclog.Logger
	lock             sync.RWMutex
}

// newProposer creates a new proposer instance
func newProposer(nodeName string, transport *Transport, logger hclog.Logger) *proposer {
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "proposer",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	return &proposer{
		nodeName:         nodeName,
		acceptorClients:  make(map[string]*paxosClient),
		acceptorPromises: make(map[string]map[string]*proposal),
		transport:        transport,
		logger:           logger,
	}
}

// Join adds the newly joined node to the list of acceptor clients.
func (p *proposer) Join(nodeName string, tags map[string]string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	ringRPCAddrStr, ok := tags[ringRPCAddrJSON]
	if ok {
		ringRPCAddr := ServerAddress(ringRPCAddrStr)
		p.acceptorClients[ringRPCAddrStr] = &paxosClient{
			address:   ringRPCAddr,
			Transport: p.transport,
		}
		p.acceptorPromises[ringRPCAddrStr] = make(map[string]*proposal)
	}
	return nil
}

// Leave adds the newly joined node to the list of acceptor clients.
func (p *proposer) Leave(nodeName string, tags map[string]string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	ringRPCAddrStr, ok := tags[ringRPCAddrJSON]
	if ok {
		delete(p.acceptorClients, ringRPCAddrStr)
		delete(p.acceptorPromises, ringRPCAddrStr)
	}
	return nil
}

// Propose sends a proposal request to the peers
func (p *proposer) Propose(proposal *proposal) error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// Stage 1: Prepare proposals until majority is reached
	for !p.majorityReached(proposal) {
		if err := p.prepare(proposal); err != nil {
			return err
		}
	}
	p.logger.Info(p.nodeName, "reached majority", p.majority())

	// Stage 2: Propose the value agreed on by majority of acceptors
	return p.propose(proposal)
}

// prepare chooses a new proposal number n and sends a request to
// each member of some set of acceptors, asking it to respond with:
// (a) A promise never again to accept a proposal numbered less than n, and
// (b) The proposal with the highest number less than n that it has accepted, if any.
func (p *proposer) prepare(prop *proposal) error {
	// Increment the proposal number
	prop.Number++

	p.logger.Info(p.nodeName, "proposing", prop)

	i := 0
	for _, acceptorClient := range p.acceptorClients {
		if i == p.majority() {
			break
		}
		promise, err := acceptorClient.SendPaxosMessage(prop, PrepareMsgType)
		if err != nil {
			continue
		}

		// Get the previous promise
		previousPromise, ok := p.acceptorPromises[acceptorClient.String()][prop.Key]

		// Previous promise is equal or greater than the new proposal, continue
		if ok && previousPromise.Number > promise.Number {
			continue
		}

		// Save the new promise
		p.acceptorPromises[acceptorClient.String()][prop.Key] = promise

		// Update the proposal to the one with bigger number
		if promise.Number > prop.Number {
			p.logger.Info(p.nodeName, "Updating the proposal to", promise)
			prop.Number = promise.Number
			prop.Value = promise.Value
		}
		i++
	}

	return nil
}

// If the proposer receives the requested responses from a majority of
// the acceptors, then it can issue a proposal with number n and value
// v, where v is the value of the highest-numbered proposal among the
// responses, or is any value selected by the proposer if the responders
// reported no proposals.
func (p *proposer) propose(prop *proposal) error {
	for _, acceptorClient := range p.acceptorClients {
		accepted, err := acceptorClient.SendPaxosMessage(prop, ProposalMsgType)
		if err != nil {
			continue
		}

		p.logger.Info(p.nodeName, acceptorClient.String(), "has accepted", accepted)
	}

	// Truncate acceptor promises map
	for _, acceptorClient := range p.acceptorClients {
		p.acceptorPromises[acceptorClient.String()] = make(map[string]*proposal)
	}

	return nil
}

// majority returns simple majority of acceptor nodes
func (p *proposer) majority() int {
	return len(p.acceptorClients)/2 + 1
}

// majorityReached returns true if number of matching promises from acceptors
// is equal or greater than simple majority of acceptor nodes
func (p *proposer) majorityReached(proposal *proposal) bool {
	var matches = 0

	// Iterate over promised values for each acceptor
	for _, promiseMap := range p.acceptorPromises {
		// Skip if the acceptor has not yet promised a proposal for this key
		promised, ok := promiseMap[proposal.Key]
		if !ok {
			continue
		}

		// If the promised and proposal number is the same, increment matches count
		if promised.Number == proposal.Number {
			matches++
		}
	}

	if matches >= p.majority() {
		return true
	}
	return false
}
