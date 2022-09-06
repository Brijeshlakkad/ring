package ring

import (
	"errors"
)

var (
	ErrRingShutdown = errors.New("ring is closed")
)

type Promise interface {
	Error() error
	Response() interface{}
}

type promiseError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (p *promiseError) init() {
	p.errCh = make(chan error, 1)
}

func (p *promiseError) Error() error {
	if p.err != nil {
		return p.err
	}
	if p.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case p.err = <-p.errCh:
	case <-p.ShutdownCh:
		p.err = ErrRingShutdown
	}
	return p.err
}

func (p *promiseError) respondError(err error) {
	if p.responded {
		return
	}
	if p.errCh != nil {
		p.errCh <- err
		close(p.errCh)
	}
	p.responded = true
}

type Message struct {
	Data    *proposal
	MsgType PaxosMessageType
	From    ServerAddress
}

type proposalPromise struct {
	promiseError
	req  *Message
	resp *Message
}

func (c *proposalPromise) init() {
	c.promiseError.init()
}

func (c *proposalPromise) Request() *Message {
	return c.req
}

func (c *proposalPromise) Response() interface{} {
	return c.resp
}

func (c *proposalPromise) respond(resp interface{}) *proposalPromise {
	c.resp = resp.(*Message)
	return c
}

type ConfigurationRequest struct {
}

// ConfigurationNode has the reusable router information.
type ConfigurationNode struct {
	NodeKey      string
	VirtualNodes map[uint64]int
	Tags         map[string]string
}

type ConfigurationResponse struct {
	StartupConfig *StartupConfig
	BindAddr      string
}

type configurationPromise struct {
	promiseError
	req  *ConfigurationRequest
	resp *ConfigurationResponse
}

func (c *configurationPromise) init() {
	c.promiseError.init()
}

func (c *configurationPromise) Request() *ConfigurationRequest {
	return c.req
}

func (c *configurationPromise) Response() interface{} {
	return c.resp
}

func (c *configurationPromise) respond(resp interface{}) *configurationPromise {
	c.resp = resp.(*ConfigurationResponse)
	return c
}

// rpcResponse captures both a response and a potential error.
type rpcResponse struct {
	Response interface{}
	Error    error
}

type RPCInfo struct {
	From ServerAddress
}

// rpc has a command, and provides a response mechanism.
type rpc struct {
	Command  interface{}
	RespChan chan<- rpcResponse
	*RPCInfo
}

// Respond is used to respondError with a response, error or both
func (r *rpc) Respond(resp interface{}, err error) {
	r.RespChan <- rpcResponse{resp, err}
}
