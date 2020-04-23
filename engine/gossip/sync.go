package gossip

import (
	"math/rand"

	"github.com/sunmxt/sladder"
)

func (e *EngineInstance) gossip(stop chan struct{}) {
	fanout := e.getGossipFanout()

	nodes, cnt := make([]*sladder.Node, 0, fanout), int32(0)

	// select M from N.
	e.cluster.RangeNodes(func(node *sladder.Node) bool {
		if int32(len(nodes)) < fanout {
			nodes = append(nodes, node)
		} else if n := rand.Int31n(cnt + 1); n < fanout {
			nodes[n] = node
		}
		cnt++
		return true
	}, true)

	// get names.
	//mapNames := make(map[*sladder.Node][]string, len(nodes))
}
