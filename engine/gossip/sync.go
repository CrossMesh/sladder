package gossip

import (
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/sunmxt/sladder"
	pb "github.com/sunmxt/sladder/engine/gossip/pb"
	"github.com/sunmxt/sladder/proto"
	"github.com/sunmxt/sladder/util"
)

func (e *EngineInstance) goClusterSync() {
	if !e.disableSync {
		e.tickGossipPeriodGo(func(deadline time.Time) {
			e.ClusterSync()
		})
	}
}

func (e *EngineInstance) newSyncClusterSnapshot(t *sladder.Transaction) (snap *proto.Cluster) {
	snap = &proto.Cluster{}

	readSnap := func(n *sladder.Node) {
		nmsg := &proto.Node{}
		t.ReadNodeSnapshot(n, nmsg)
		snap.Nodes = append(snap.Nodes, nmsg)
	}

	t.RangeNode(func(n *sladder.Node) bool { // prepare snapshot.
		readSnap(n)
		return true
	}, false, true)

	e.lock.Lock()
	for leaving := range e.leavingNodes {
		if poss := e.cluster.MostPossibleNode(leaving.Names()); poss != nil {
			delete(e.leavingNodes, leaving)
		}
	}
	for node := range e.leavingNodes {
		readSnap(node)
	}
	e.lock.Unlock()

	return
}

// ClusterSync performs one cluster sync process.
func (e *EngineInstance) ClusterSync() {
	var (
		nodes [][]string
		snap  *proto.Cluster
	)

	fanout := e.getGossipFanout()

	e.cluster.Txn(func(t *sladder.Transaction) bool {
		cnt := int32(0)

		// select nodes to gossip.
		t.RangeNode(func(n *sladder.Node) bool {
			names := t.Names(n)
			if len(names) < 1 { // annoymous node.
				return false
			}
			if int32(len(nodes)) < fanout {
				nodes = append(nodes, names)
			} else if i := rand.Int31n(cnt + 1); i < fanout {
				nodes[i] = names
			}
			cnt++

			if len(nodes) < 1 {
				return false
			}

			return false
		}, true, true)

		snap = e.newSyncClusterSnapshot(t)

		return false
	}, sladder.MembershipModification())

	setTimeout := func(id uint64) {
		time.AfterFunc(e.getGossipPeriod()*10, func() {
			e.inSync.Delete(id)
			atomic.AddUint64(&e.statistics.TimeoutSyncs, 1)
		})
	}

	for _, node := range nodes {
		id := e.generateMessageID()
		atomic.AddUint64(&e.statistics.InSync, 1)
		e.inSync.Store(id, node)
		setTimeout(id)
		e.sendProto(node, &pb.Sync{
			Id:      id,
			Cluster: snap,
		})
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

	var (
		errs sladder.Errors
		snap *proto.Cluster
	)
	if err := e.cluster.Txn(func(t *sladder.Transaction) bool {
		// mark txn internal.
		e.innerTxnIDs.Store(t.ID(), struct{}{})

		snap = e.newSyncClusterSnapshot(t) // read a snapshot first.

		// start apply snapshot to cluster.
		var newNode *sladder.Node
		for _, mnode := range sync.Cluster.Nodes {
			node, names, err := t.MostPossibleNodeFromProtobuf(mnode.Kvs) // find related node.
			if err != nil {
				errs = append(errs, err, fmt.Errorf("node lookup failure"))
				return false
			}
			if len(names) < 1 { // skip in case of a remote anonymous node.
				continue
			}

			tagKeyIndex := -1
			for idx, kv := range mnode.Kvs {
				if kv.Key == e.swimTagKey {
					tagKeyIndex = idx
					break
				}
			}

			if node == nil { // no related node found.
				// check swim tag.
				if tagKeyIndex < 0 {
					e.log.Warn("drop a node without swim tag.")
					continue
				}

				tag := &SWIMTag{}
				if err := tag.Decode(mnode.Kvs[tagKeyIndex].Value); err != nil {
					e.log.Warnf("drop a node with invalid swim tag in sync message. (decode err = \"%v\").", err.Error())
					return false
				}
				if tag.State == DEAD || tag.State == LEFT {
					// this node seem to be removed from cluster. we won't accept the old states. skip.
					continue
				}

				if newNode == nil { // allocate a new node.
					if newNode, err = t.NewNode(); err != nil {
						errs = append(errs, err, fmt.Errorf("cannot create new node"))
						return false
					}
				}
				node = newNode

			} else if tagKeyIndex < 0 && t.KeyExists(node, e.swimTagKey) {
				continue
			}

			useNewNode := false
			if tagKeyIndex > 0 {
				// apply swim tag first.
				tempNode := proto.Node{
					Kvs: []*proto.Node_KeyValue{mnode.Kvs[tagKeyIndex]},
				}
				if err = t.MergeNodeSnapshot(node, &tempNode, false, true, true); err != nil {
					e.log.Warnf("apply node swim tag failure. skip. (err = \"%v\") {node = %v}", err, node.PrintableName())
					continue
				} else {
					newNode, useNewNode = nil, true
				}
				mnode.Kvs[tagKeyIndex] = mnode.Kvs[len(mnode.Kvs)-1]
				mnode.Kvs = mnode.Kvs[:len(mnode.Kvs)-1]
			}

			if t.KeyExists(node, e.swimTagKey) {
				rtx, err := t.KV(node, e.swimTagKey)
				if err != nil {
					e.log.Warnf("cannot get swim tag. skip. (err = \"%v\") {node = %v}", err, node.PrintableName())
					if useNewNode { // remove corrupted node.
						t.RemoveNode(node)
					}
					continue
				}
				tag := rtx.(*SWIMTagTxn)

				// refine message.
				entryList := tag.EntryList(false)
				sort.Slice(mnode.Kvs, func(i, j int) bool { return mnode.Kvs[i].Key < mnode.Kvs[j].Key })
				eliIdx, idx, eIdx := 0, 0, 0
				for idx < len(mnode.Kvs) {
					kv := mnode.Kvs[idx]
					if eIdx < len(entryList) {
						if kv.Key == e.swimTagKey || kv.Key == entryList[eIdx] {
							// accept
							mnode.Kvs[eliIdx] = mnode.Kvs[idx]
							eliIdx++
							idx++
						} else if kv.Key < entryList[eIdx] {
							// drop entry as key is not in entry list.
							idx++
						} else {
							eIdx++
						}
					} else {
						break
					}
				}
				mnode.Kvs = mnode.Kvs[:eliIdx]
			}

			// now apply snapshot.
			if err = t.MergeNodeSnapshot(node, mnode, false, false, false); err != nil {
				e.log.Warnf("apply node snapshot failure. skip. (err = \"%v\") {node = %v}", err, node.PrintableName())
				continue
			} else {
				newNode = nil
			}
		}

		if newNode != nil { // extra unused empty node.
			if err := t.RemoveNode(newNode); err != nil {
				errs = append(errs, err)
				return false
			}
		}

		return true
	}, sladder.MembershipModification()); err != nil { // in order to lock entire cluster, we are required to use MembershipModification().
		errs = append(errs, err)
	}

	if err := errs.AsError(); err != nil {
		e.log.Warnf("a sync failure raised. %v", err.Error())
	}

	isResponse := false
	if r, inSync := e.inSync.Load(sync.Id); inSync {
		to := r.([]string)
		sort.Strings(from)
		util.RangeOverStringSortedSet(from, to, nil, nil, func(s *string) bool {
			isResponse = true
			return false
		})
		atomic.AddUint64(&e.statistics.InSync, 0xFFFFFFFFFFFFFFFF) // -1
		atomic.AddUint64(&e.statistics.FinishedSync, 1)
	}

	if isResponse {
		e.inSync.Delete(sync.Id)
	} else {
		e.sendProto(from, &pb.Sync{
			Id:      sync.Id,
			Cluster: snap,
		})
	}

}
