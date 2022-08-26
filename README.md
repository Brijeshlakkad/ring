# ring
Go utility for Peer to Peer Architecture.

## Ring Configuration parameters
| Parameters       | Type                                                                                                             | Usage                                                                                                                                                                                                      |
|------------------|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BindAddr         | string                                                                                                           | The address that Ring will bind to for communication with other members on the ring. By default this is "0.0.0.0:7946". [Read more](https://www.serf.io/docs/agent/options.html#bind)                      |
| RPCPort          | int                                                                                                              | The address that Ring will bind to for the member's RPC server. By default this is "127.0.0.1:7373", allowing only loopback connections. [Read more](https://www.serf.io/docs/agent/options.html#rpc-addr) |
| NodeName         | string                                                                                                           | Unique node name to identify this member.                                                                                                                                                                  |
| SeedAddresses    | []string                                                                                                         | Addresses of other members to join upon start up.                                                                                                                                                          |
| VirtualNodeCount | int                                                                                                              | Number of virtual nodes to create on the ring for this member.                                                                                                                                             |
| HashFunction     | [HashFunction](https://github.com/Brijeshlakkad/ring/blob/f6306cf287105f18f831db916ef01823ef867fd4/types.go#L10) | Hash function to calculate position of the server on the ring.                                                                                                                                             |
| MemberType       | [MemberType](https://github.com/Brijeshlakkad/ring/blob/ff61485ce23d72714bfb67d7201dc42f4933afa1/types.go#L16)   | Type of the membership: 1. ShardMember 2. LoadBalancerMember.                                                                                                                                              |
## Quick Start
Create a new ring member. ```LoadBalancerMember``` member has the list of all types of members. 
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
    VirtualNodeCount:   '3',                    // This will create 3 virtual nodes on the ring.
    MemberType:         LoadBalancerMember,     // This member will not take part in the sharding, but has the list of members (ShardMember) who is responsible for sharding. 
})
```

Use `ringMember#AddListener` method to add a new handler to be notified when new node joins on the ring.
Look at [this Handler interface](https://github.com/Brijeshlakkad/ring/blob/f6306cf287105f18f831db916ef01823ef867fd4/types.go#L4) to implement your own handler.
```go
ringMember.AddListener(listenerId string, handler Handler)
```

Use `ringMember#AddListener` method to remove the handler from listening to the joining/leaving of other members.
All the handlers' `RemoveListener` will get called when the member shutdowns.
```go
ringMember.RemoveListener(listenerId string)
```

Upon join a listener will receive the below parameters in the function call:
1. `rpcAddr` - RPC address of the new member that has joined the ring.
2. `vNodeCount` - Number of virtual nodes of the new member on the ring.

### **Can I contribute to this project?**
Feel free to create a PR, I’m more than happy to review and merge it.

### **What's the long-term goal?**
- Onboard videos and documentation
- Clean code, full test coverage and minimal tech debt
- This utility will evolve over time!!!

## References (Thank you!)
1. [Serf](https://github.com/hashicorp/serf)
2. https://en.wikipedia.org/wiki/Consistent_hashing