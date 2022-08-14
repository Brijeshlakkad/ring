package ring_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Brijeshlakkad/ring"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestAgent(t *testing.T) {
	var ringMembers []*ring.Member
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		var seedAddress string
		if i != 0 {
			seedAddress = ringMembers[0].Config.BindAddr
		}

		ringMember, err := ring.NewMember(ring.Config{
			NodeName:         fmt.Sprintf("%d", i),
			SeedAddress:      seedAddress,
			BindAddr:         bindAddr,
			RPCPort:          rpcPort,
			VirtualNodeCount: 3,
		})
		require.NoError(t, err)

		ringMembers = append(ringMembers, ringMember)
	}
	defer func() {
		for _, ringMember := range ringMembers {
			err := ringMember.Shutdown()
			require.NoError(t, err)
		}
	}()

	time.Sleep(3 * time.Second)
}
