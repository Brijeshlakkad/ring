# ring
Go utility for Peer to Peer Architecture.

## Ring Configuration parameters
| Parameters       | Type     | Usage                                                                                                                                                                                                      |
|------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BindAddr         | string   | The address that Ring will bind to for communication with other members on the ring. By default this is "0.0.0.0:7946". [Read more](https://www.serf.io/docs/agent/options.html#bind)                      |
| RPCPort          | int      | The address that Ring will bind to for the member's RPC server. By default this is "127.0.0.1:7373", allowing only loopback connections. [Read more](https://www.serf.io/docs/agent/options.html#rpc-addr) |
| NodeName         | string   | Unique node name to identify this member.                                                                                                                                                                  |
| SeedAddresses    | []string | Addresses of other members to join upon start up.                                                                                                                                                          |
| VirtualNodeCount | int      | Number of virtual nodes to create on the ring for this member.                                                                                                                                             |

## Quick Start
Create a new ring member.
```go
import (
	// Other dependencies....
	"github.com/Brijeshlakkad/ring"
)

ringMember, err := ring.NewMember(ring.Config{
    NodeName:           'Unique_node_name_0',   // Node name.
    SeedAddresses:      []string{},             // Addresses of other members to join upon start up.
    BindAddr:           '127.0.0.1:7946',       // The address that Ring will bind to for communication with other members on the ring. By default this is "0.0.0.0:7946".
    RPCPort:            '7373',                 // The address that Ring will bind to for the member's RPC server. By default this is "127.0.0.1:7373", allowing only loopback connections.
    VirtualNodeCount:   '3'                     // This will create 3 virtual nodes on the ring.
})
```

Use `ringMember#AddListener` method to add a new handler to be notified when new node joins on the ring.
Look at [this Handler interface](https://github.com/Brijeshlakkad/ring/blob/1022cee940b0f31aa73b6b4d115d5b6a4e208546/membership.go#L141) to implement your own handler.
```go
ringMember.AddListener(listenerId string, handler Handler)
```

Use `ringMember#AddListener` method to remove the handler from listening to the joining/leaving of other members.
All the handlers' `RemoveListener` will get called when the member shutdowns.
```go
ringMember.RemoveListener(listenerId string)
```

### **Can I contribute to this project?**
Feel free to create a PR, I’m more than happy to review and merge it.

### **What's the long-term goal?**
- Onboard videos and documentation
- Clean code, full test coverage and minimal tech debt
- This utility will evolve over time!!!

## References (Thank you!)
1. [Serf](https://github.com/hashicorp/serf)
2. https://en.wikipedia.org/wiki/Consistent_hashing