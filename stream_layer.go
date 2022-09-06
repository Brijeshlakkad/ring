package ring

import (
	"crypto/tls"
	"net"
	"time"
)

type ringStreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func newRingStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *ringStreamLayer {
	return &ringStreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

func (p *ringStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	if p.peerTLSConfig != nil {
		conn = tls.Client(conn, p.peerTLSConfig)
	}
	return conn, err
}

func (p *ringStreamLayer) Accept() (net.Conn, error) {
	conn, err := p.ln.Accept()
	if err != nil {
		return nil, err
	}
	if p.serverTLSConfig != nil {
		return tls.Server(conn, p.serverTLSConfig), nil
	}
	return conn, nil
}

func (p *ringStreamLayer) Close() error {
	return p.ln.Close()
}

func (p *ringStreamLayer) Addr() net.Addr {
	return p.ln.Addr()
}
