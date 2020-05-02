package gossip

import (
	"container/heap"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/sunmxt/sladder"
	"github.com/sunmxt/sladder/engine/gossip/pb"
)

type suspection struct {
	notAfter   time.Time
	node       *sladder.Node
	queueIndex int
}

type suspectionQueue []*suspection

func (q *suspectionQueue) Len() int           { return len(*q) }
func (q *suspectionQueue) Less(i, j int) bool { return (*q)[i].notAfter.Before((*q)[j].notAfter) }
func (q *suspectionQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
	(*q)[i].queueIndex = i
	(*q)[j].queueIndex = j
}
func (q *suspectionQueue) Push(x interface{}) {
	s := x.(*suspection)
	s.queueIndex = len(*q)
	(*q) = append((*q), s)
}
func (q *suspectionQueue) Pop() (x interface{}) {
	x = (*q)[(*q).Len()]
	(*q) = (*q)[:(*q).Len()-1]
	return
}

type proxyPingRequest struct {
	origin []string
	id     uint64
}

type pingContext struct {
	id       uint64
	start    time.Time
	proxyFor []*proxyPingRequest
}

func (e *EngineInstance) goDetectFailure() {
	e.tickGossipPeriodGo(func(deadline time.Time) {
		e.detectFailure()
	})

	e.tickGossipPeriodGo(func(deadline time.Time) {
		e.clearSuspections()
	})
}

func (e *EngineInstance) onNodeRemovedClearFailureDetector(node *sladder.Node) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// remove node from all failure detector fields.
	delete(e.inPing, node)
	delete(e.roundTrips, node)
	if s, _ := e.suspectionNodeIndex[node]; s != nil {
		heap.Remove(&e.suspectionQueue, s.queueIndex)
	}
}

func (e *EngineInstance) traceSuspections(node *sladder.Node, tag *SWIMTag) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// trace suspection states.
	s, suspected := e.suspectionNodeIndex[node]
	if tag.State != SUSPECTED {
		if suspected {
			heap.Remove(&e.suspectionQueue, s.queueIndex)
			delete(e.suspectionNodeIndex, node)
		}
	} else if !suspected {
		s = &suspection{
			notAfter: time.Now().Add(e.getGossipPeriod() * 10),
			node:     node,
		}
		heap.Push(&e.suspectionQueue, s)
		e.suspectionNodeIndex[node] = s
	}
}

func (e *EngineInstance) gossipForLeaving(node *sladder.Node) {
	e.leavingNodes[node] = struct{}{}
	time.AfterFunc(e.getGossipPeriod()*10, func() {
		delete(e.leavingNodes, node)
	})
}

func (e *EngineInstance) removeIfDead(node *sladder.Node, tag *SWIMTag) {
	if tag.State != DEAD {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	nodeSet, exists := e.withRegion[tag.Region]
	if !exists {
		// should not reach this.
		e.log.Fatal("[BUGS] a node not traced by region map.")
		e.cluster.RemoveNode(node)
		e.gossipForLeaving(node)
	}
	if _, inNodeSet := nodeSet[node]; !inNodeSet {
		// should not reach this.
		e.log.Fatal("[BUGS] a node not in region node set.")
		e.cluster.RemoveNode(node)
		e.gossipForLeaving(node)
	}
	if uint(len(nodeSet)) <= e.minRegionPeer { // limitation of region peer count.
		return
	}
	e.cluster.RemoveNode(node)
	e.gossipForLeaving(node)
}

func (e *EngineInstance) removeIfLeft(node *sladder.Node, tag *SWIMTag) {
	if tag.State != LEFT {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	e.cluster.RemoveNode(node)
	e.gossipForLeaving(node)
}

func (e *EngineInstance) clearSuspections() {
	e.lock.Lock()
	defer e.lock.Unlock()

	var deads []*sladder.Node

	if e.suspectionQueue.Len() < 1 {
		// no suspection.
		return
	}
	now := time.Now()

	for e.suspectionQueue.Len() > 0 { // find all expired suspection.
		s := e.suspectionQueue[0]
		if !now.After(s.notAfter) {
			break
		}
		if err := s.node.Keys(e.swimTagKey).Txn(func(tag *SWIMTagTxn) (bool, error) {
			// claim dead.
			tag.ClaimDead()
			return true, nil
		}).Error; err != nil {
			e.log.Fatal("claiming dead transaction failure, got " + err.Error())
			break
		}
		deads = append(deads, s.node)
		heap.Pop(&e.suspectionQueue)
	}
}

func (e *EngineInstance) detectFailure() {
	nodes := e.selectRandomNodes(e.getGossipFanout(), true)
	if len(nodes) < 1 {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	for _, node := range nodes {
		e.ping(node, nil)
	}
}

func (e *EngineInstance) estimatedRoundTrip(node *sladder.Node) time.Duration {
	rtt, estimated := e.roundTrips[node]
	if !estimated || rtt < 1 {
		return e.getGossipPeriod()
	}
	return rtt
}

func (e *EngineInstance) processFailureDetectionProto(from []string, msg *pb.GossipMessage) {
	switch msg.Type {
	case pb.GossipMessage_Ack:
		ack := &pb.Ack{}
		if err := ptypes.UnmarshalAny(msg.Body, ack); err != nil {
			e.log.Fatal("invalid ack body, got " + err.Error())
			break
		}
		e.onPingAck(from, ack)

	case pb.GossipMessage_Ping:
		ping := &pb.Ping{}
		if err := ptypes.UnmarshalAny(msg.Body, ping); err != nil {
			e.log.Fatal("invalid ping body, got " + err.Error())
			break
		}
		e.onPing(from, ping)

	case pb.GossipMessage_PingReq:
		pingReq := &pb.PingReq{}
		if err := ptypes.UnmarshalAny(msg.Body, pingReq); err != nil {
			e.log.Fatal("invalid ping-req body, got " + err.Error())
			break
		}
		e.onPingReq(from, pingReq)
	}
}

func (e *EngineInstance) ping(node *sladder.Node, pingReq *pb.PingReq) {
	pingCtx, _ := e.inPing[node]
	if pingCtx == nil { // not in progres.
		setTimeout := func(node *sladder.Node) {
			// after a ping timeout, a ping-req may be sent.
			time.AfterFunc(e.estimatedRoundTrip(node), func() {
				e.pingTimeoutEvent <- node
			})
		}

		id, err := pb.NewMessageID()
		if err != nil {
			e.log.Fatal("cannot generate message id, got " + err.Error())
			return
		}
		e.sendProto(node.Names(), &pb.Ping{
			Id: id,
		})

		pingCtx = &pingContext{
			id:    id,
			start: time.Now(),
		}
		setTimeout(node)
		e.inPing[node] = pingCtx
	}

	if pingReq != nil && len(pingReq.Name) > 0 {
		pingCtx.proxyFor = append(pingCtx.proxyFor, &proxyPingRequest{
			origin: pingReq.Name,
			id:     pingReq.Id,
		})
	}
}

func (e *EngineInstance) onPingAck(from []string, msg *pb.Ack) {
	node := e.cluster.MostPossibleNode(from)
	if node == nil {
		return
	}
	e.lock.Lock()
	defer e.lock.Unlock()

	pingCtx, _ := e.inPing[node]
	if pingCtx == nil {
		return
	}
	// save estimated round-trip time.
	rtt := time.Now().Sub(pingCtx.start)
	e.roundTrips[node] = rtt

	for _, pingReq := range pingCtx.proxyFor {
		// ack for all related ping-req.
		e.sendProto(pingReq.origin, &pb.Ack{
			NamesProxyFor: pingReq.origin,
			Id:            pingReq.id,
		})
	}

	delete(e.inPing, node)
}

func (e *EngineInstance) onPing(from []string, msg *pb.Ping) {
	if msg == nil {
		return
	}

	// ack.
	e.sendProto(from, &pb.Ack{
		Id: msg.Id,
	})
}

func (e *EngineInstance) processPingTimeout() {
	var node *sladder.Node

	setTimeout := func(node *sladder.Node, timeout time.Duration) {
		time.AfterFunc(timeout, func() {
			e.pingReqTimeoutEvent <- node
		})
	}

	for {
		select {
		case <-e.arbiter.Exit():
			break
		case node = <-e.pingTimeoutEvent:
		}

		e.lock.Lock()
		pingCtx, _ := e.inPing[node]
		if pingCtx != nil { // timeout.
			req := &pb.PingReq{
				Id:   pingCtx.id,
				Name: node.Names(),
			}

			timeout := time.Duration(0)
			for _, proxy := range e.selectRandomNodes(e.getPingProxiesCount(), true) {
				if proxy == node {
					continue
				}
				proxyNames := proxy.Names()
				e.sendProto(proxyNames, req) // ping-req.

				// find minimal proxier round-trip.
				if rtt := e.estimatedRoundTrip(proxy); timeout < 1 || rtt < timeout {
					timeout = rtt
				}
			}

			if gossipPeriod := e.getGossipPeriod(); gossipPeriod > timeout {
				timeout = gossipPeriod
			}
			setTimeout(node, timeout*time.Duration(e.getMinPingReqTimeoutTimes()))
		}
		e.lock.Unlock()
	}
}

func (e *EngineInstance) processPingReqTimeout() {
	var node *sladder.Node
	for {
		select {
		case <-e.arbiter.Exit():
			break
		case node = <-e.pingReqTimeoutEvent:
		}

		e.lock.Lock()
		pingCtx, _ := e.inPing[node]
		if pingCtx != nil { // timeout.
			if err := node.Keys(e.swimTagKey).Txn(func(swim *SWIMTagTxn) (bool, error) {
				swim.ClaimSuspected() // raise suspection.
				return true, nil
			}).Error; err != nil {
				e.log.Fatal("txn commit failure when claims suspection, got " + err.Error())
			}
		}
		e.lock.Unlock()
	}
}

func (e *EngineInstance) onPingReq(from []string, msg *pb.PingReq) {
	if msg == nil || len(msg.Name) < 1 {
		return
	}

	node := e.cluster.MostPossibleNode(msg.Name)
	if node == nil {
		return
	}

	e.ping(node, msg) // proxy ping.
}
