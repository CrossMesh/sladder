package gossip

import (
	"time"

	"github.com/golang/protobuf/ptypes"
	pb "github.com/sunmxt/sladder/engine/gossip/pb"
	gproto "github.com/sunmxt/sladder/proto"
)

func (e *EngineInstance) goClusterSync() {
	e.tickGossipPeriodGo(func(deadline time.Time) {
		e.clusterSync()
	})
}

func (e *EngineInstance) clusterSync() {
	nodes := e.selectRandomNodes(e.getGossipFanout(), true)
	if len(nodes) < 1 {
		return
	}

	setTimeout := func(id uint64) {
		time.AfterFunc(e.getGossipPeriod()*10, func() {
			e.lock.Lock()
			defer e.lock.Unlock()
			delete(e.inSync, id)
		})
	}

	// prepare snapshot.
	var snap gproto.Cluster
	e.cluster.ProtobufSnapshot(&snap)

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
			setTimeout(id)
			e.sendProto(node.Names(), &pb.Sync{
				Id:      id,
				Cluster: &snap,
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

	e.cluster.SyncFromProtobufSnapshot(sync.Cluster, true)
	if _, inSync := e.inSync[sync.Id]; inSync {
		delete(e.inSync, sync.Id)
		return
	}

	e.cluster.ProtobufSnapshot(sync.Cluster)
	e.sendProto(from, &pb.Sync{
		Id:      sync.Id,
		Cluster: sync.Cluster,
	})
}
