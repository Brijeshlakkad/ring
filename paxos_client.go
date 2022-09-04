package ring

type paxosClient struct {
	*Transport
	address ServerAddress
}

func (c *paxosClient) String() string {
	return string(c.address)
}

// SendPaxosMessage sends a prepare request to acceptor
func (c *paxosClient) SendPaxosMessage(proposal *proposal, msgType PaxosMessageType) (*proposal, error) {
	var resp = &Message{}
	req := &Message{
		Data:    proposal,
		MsgType: msgType,
		From:    ServerAddress(c.stream.Addr().String()),
	}
	err := c.Transport.SendPaxosMessage(c.address, req, resp)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}
