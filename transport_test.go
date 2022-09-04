package ring

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransport_SendPaxosMessage(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 1 is consumer
	trans1 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Timeout: 4 * time.Second,
		},
	)
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := Message{
		Data: &proposal{
			Number: 1,
			Key:    "round-1",
			Value:  []byte("127.0.0.1:1234"),
		},
		MsgType: PrepareMsgType,
	}

	resp := Message{
		Data: &proposal{
			Number: 1,
			Key:    "round-1",
			Value:  []byte("127.0.0.1:1234"),
		},
		MsgType: PromiseMsgType,
	}

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*Message)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request
	streamLayer2, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 2 is consumer
	trans2 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer2,
			Timeout: 4 * time.Second,
		},
	)
	defer trans2.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(1)

	appendFunc := func() {
		defer wg.Done()
		var out Message
		if err := trans2.SendPaxosMessage(trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}

	// Try to do parallel appends, should stress the conn pool
	go appendFunc()

	// Wait for the routines to finish
	wg.Wait()

}

func TestTransport_StartStop(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 1 is consumer
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Timeout: 4 * time.Second,
		},
	)

	transport.Close()
}
