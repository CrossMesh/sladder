package gossip

import (
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/sunmxt/sladder"
	pb "github.com/sunmxt/sladder/engine/gossip/pb"
	"github.com/sunmxt/sladder/proto"
	gproto "github.com/sunmxt/sladder/proto"
)

func (e *EngineInstance) goClusterSync() {
	if !e.disableSync {
		e.tickGossipPeriodGo(func(deadline time.Time) {
			e.ClusterSync()
		})
	}
}

func (e *EngineInstance) newSyncClusterSnapshot() *proto.Cluster {
	var snap gproto.Cluster

	leavingNodeUpdated := false

	// TODO: refactor.
	e.cluster.ProtobufSnapshot(&snap, func(node *sladder.Node) bool {
		if !leavingNodeUpdated {
			for leaving := range e.leavingNodes {
				if poss := e.cluster.MostPossibleNode(leaving.Names()); poss != nil {
					delete(e.leavingNodes, node)
				}
			}
			leavingNodeUpdated = true
		}
		return true
	})

	for node := range e.leavingNodes {
		msg := &proto.Node{}
		node.ProtobufSnapshot(msg)
		snap.Nodes = append(snap.Nodes, msg)
	}

	return &snap
}

// ClusterSync does one cluster sync process.
func (e *EngineInstance) ClusterSync() {
	nodes := e.selectRandomNodes(e.getGossipFanout(), true)
	if len(nodes) < 1 {
		return
	}

	setTimeout := func(id uint64) {
		time.AfterFunc(e.getGossipPeriod()*10, func() {
			e.lock.Lock()
			defer e.lock.Unlock()
			delete(e.inSync, id)
			atomic.AddUint64(&e.statistics.TimeoutSyncs, 1)
		})
	}

	// prepare snapshot.
	snap := e.newSyncClusterSnapshot()

	e.lock.Lock()
	defer e.lock.Unlock()

	// send to peers.
	for _, node := range nodes {
		for {
			id, err := pb.NewMessageID()
			if err != nil {
				e.log.Fatal("cannot generate message id, got " + err.Error())
			}
			_, used := e.inSync[id]
			if used {
				continue
			}
			e.inSync[id] = struct{}{}
			atomic.AddUint64(&e.statistics.InSync, 1)
			setTimeout(id)
			e.sendProto(node.Names(), &pb.Sync{
				Id:      id,
				Cluster: snap,
			})
			break
		}
	}
}

func (e *EngineInstance) processSyncGossipProto(from []string, msg *pb.GossipMessage) {
	if msg == nil {
		return
	}

	var sync pb.Sync

	if err := ptypes.UnmarshalAny(msg.Body, &sync); err != nil {
		e.log.Warn("invalid sync body, got " + err.Error())
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	e.cluster.SyncFromProtobufSnapshot(sync.Cluster, true, func(node *sladder.Node, kvs []*proto.Node_KeyValue) bool {
		for _, kv := range kvs {
			if kv.Key != e.swimTagKey {
				continue
			}
			tag := &SWIMTag{}
			if err := tag.Decode(kv.Value); err != nil {
				e.log.Warn("drop a node with invalid swim tag in sync message. decode got: " + err.Error())
				return false
			}
			if (tag.State == DEAD || tag.State == LEFT) && node == nil {
				// this node was removed from cluster. we won't accept the old states.
				return false
			}
			break
		}
		return true
	})

	if _, inSync := e.inSync[sync.Id]; inSync {
		delete(e.inSync, sync.Id)
		atomic.AddUint64(&e.statistics.InSync, 0xFFFFFFFFFFFFFFFF) // -1
		atomic.AddUint64(&e.statistics.FinishedSync, 1)
		return
	}

	e.sendProto(from, &pb.Sync{
		Id:      sync.Id,
		Cluster: e.newSyncClusterSnapshot(),
	})
}
