package ring

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
)

const (
	// rpcMaxPipeline controls the maximum number of outstanding RPC calls.
	rpcMaxPipeline = 128

	// reqBufferSize Size of the buffer for the request
	reqBufferSize = 256 * 1024 // 256KB

	// reqBufferSize Size of the buffer for the response
	respBufferSize = 256 * 1024 // 256KB
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrCommandNotFound to notify the sender that command is invalid.
	ErrCommandNotFound = errors.New("command couldn't be handled")
)

/*
Transport provides a network based transport that can be
used to communicate with Raft on remote machines. It requires
an underlying stream layer to provide a stream abstraction, which can
be simple TCP, TLS, etc.

This transport is very simple and lightweight. Each RPC request is
framed by sending a byte that indicates the MsgPack encoded request.

The response is an error string followed by the response object,
both are encoded using MsgPack.
*/
type Transport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	maxPool int

	stream StreamLayer

	logger hclog.Logger

	// streamCtx is used to cancel existing connection handlers.
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout time.Duration

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	acceptorCh chan rpc
	ringCh     chan rpc
}

// TransportConfig encapsulates configuration for the network transport layer.
type TransportConfig struct {
	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration

	MaxPool int
}

// NewTransportWithConfig creates a new network transport with the given config struct
func NewTransportWithConfig(
	config *TransportConfig,
) *Transport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "ring-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	trans := &Transport{
		connPool:   make(map[ServerAddress][]*netConn),
		logger:     config.Logger,
		maxPool:    config.MaxPool,
		acceptorCh: make(chan rpc),
		ringCh:     make(chan rpc),
		shutdownCh: make(chan struct{}),
		stream:     config.Stream,
		timeout:    config.Timeout,
	}

	// Create the connection context and then start our listener.
	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// setupStreamContext is used to create a new stream context. This should be
// called with the stream acceptorLock held.
func (transport *Transport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	transport.streamCtx = ctx
	transport.streamCancel = cancel
}

// getStreamContext is used retrieve the current stream context.
func (transport *Transport) getStreamContext() context.Context {
	transport.streamCtxLock.RLock()
	defer transport.streamCtxLock.RUnlock()
	return transport.streamCtx
}

// CloseStreams closes the current streams.
func (transport *Transport) CloseStreams() {
	transport.connPoolLock.Lock()
	defer transport.connPoolLock.Unlock()

	// Shutdown all the connections in the connection pool and then remove their
	// entry.
	for k, e := range transport.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(transport.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the acceptorLock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	transport.streamCtxLock.Lock()
	transport.streamCancel()
	transport.setupStreamContext()
	transport.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (transport *Transport) Close() error {
	transport.shutdownLock.Lock()
	defer transport.shutdownLock.Unlock()

	if !transport.shutdown {
		close(transport.shutdownCh)
		transport.stream.Close()
		transport.shutdown = true
	}
	return nil
}

// AcceptorConsumer returns the channel to consume the incoming request for paxos request type.
func (transport *Transport) AcceptorConsumer() <-chan rpc {
	return transport.acceptorCh
}

// RingConsumer returns the channel to consume the incoming request for ring request type.
func (transport *Transport) RingConsumer() <-chan rpc {
	return transport.ringCh
}

// LocalAddr returns the local address of this node.
func (transport *Transport) LocalAddr() ServerAddress {
	return ServerAddress(transport.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (transport *Transport) IsShutdown() bool {
	select {
	case <-transport.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (transport *Transport) getPooledConn(target ServerAddress) *netConn {
	transport.connPoolLock.Lock()
	defer transport.connPoolLock.Unlock()

	conns, ok := transport.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	transport.connPool[target] = conns[:num-1]
	return conn
}

// getConn is used to get a connection from the pool.
func (transport *Transport) getConn(target ServerAddress) (*netConn, error) {
	// Check for a pooled conn
	if conn := transport.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := transport.stream.Dial(target, transport.timeout)
	if err != nil {
		return nil, err
	}

	// Wrap the net.conn
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, reqBufferSize),
	}

	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (transport *Transport) returnConn(conn *netConn) {
	transport.connPoolLock.Lock()
	defer transport.connPoolLock.Unlock()

	key := conn.target
	conns, _ := transport.connPool[key]

	if !transport.IsShutdown() && len(conns) < transport.maxPool {
		transport.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

// SendPaxosMessage uses genericRPC to send the prepare/proposal/accepted request.
func (transport *Transport) SendPaxosMessage(target ServerAddress, req *Message, resp *Message) error {
	return transport.genericRPC(target, PaxosRequestType, req, resp)
}

// SendConfigurationRequest fetches the startup configuration from the target.
func (transport *Transport) SendConfigurationRequest(target ServerAddress, req *ConfigurationRequest, resp interface{}) error {
	return transport.genericRPC(target, RingRequestType, req, resp)
}

// genericRPC handles a simple request/response RPC.
func (transport *Transport) genericRPC(target ServerAddress, reqType RequestType, args interface{}, resp interface{}) error {
	// Get a conn
	conn, err := transport.getConn(target)
	if err != nil {
		return err
	}

	// Set a deadline
	if transport.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(transport.timeout))
	}

	// Send the RPC
	if err = sendRPC(conn, reqType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		transport.returnConn(conn)
	}

	return err
}

// listen is used to handling incoming connections.
func (transport *Transport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accept incoming connections
		conn, err := transport.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !transport.IsShutdown() {
				transport.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-transport.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay
		loopDelay = 0

		transport.logger.Debug("accepted connection", "local-address", transport.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// Handle the connection in dedicated routine
		go transport.handleConn(transport.getStreamContext(), conn)
	}
}

// handleConn is used to handle an inbound connection for its lifespan. The
// handler will exit when the passed context is cancelled or the connection is
// closed.
func (transport *Transport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReaderSize(conn, respBufferSize)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-connCtx.Done():
			transport.logger.Debug("stream layer is closed")
			return
		default:
		}

		if err := transport.handleCommand(conn, r, dec, enc); err != nil {
			if err != io.EOF {
				transport.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			transport.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand is used to decode and dispatch a single command.
func (transport *Transport) handleCommand(conn net.Conn, r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	// Get the rpc type
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// Create the RPC object
	respCh := make(chan rpcResponse, 1)
	rpc := rpc{
		RespChan: respCh,
		RPCInfo: &RPCInfo{
			From: ServerAddress(conn.RemoteAddr().String()),
		},
	}

	ch := transport.ringCh
	switch RequestType(rpcType) {
	case PaxosRequestType:
		ch = transport.acceptorCh
		req := &Message{}
		if err := dec.Decode(req); err != nil {
			return err
		}
		rpc.Command = req
	case RingRequestType:
		// ch is already set to ringCh
		req := &ConfigurationRequest{}
		if err := dec.Decode(req); err != nil {
			return err
		}
		rpc.Command = req
	default:
		return ErrCommandNotFound
	}

	// Dispatch the RPC
	select {
	case ch <- rpc:
	case <-transport.shutdownCh:
		return ErrTransportShutdown
	}

	// Wait for response
	select {
	case resp := <-respCh:
		// Send the error first
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// Send the response
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-transport.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse is used to decode an RPC response and reports whether
// the connection can be reused.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

// sendRPC is used to encode and send the RPC.
func sendRPC(conn *netConn, reqType RequestType, args interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(uint8(reqType)); err != nil {
		conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}
