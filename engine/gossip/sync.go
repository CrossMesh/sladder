package gossip

import (
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/crossmesh/sladder"
	pb "github.com/crossmesh/sladder/engine/gossip/pb"
	"github.com/crossmesh/sladder/proto"
	"github.com/crossmesh/sladder/util"
	"github.com/golang/protobuf/ptypes"
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
	removeIndics := []int{}
	for idx, leaving := range e.leavingNodes {
		if poss := t.MostPossibleNode(leaving.names); poss != nil {
			removeIndics = append(removeIndics, idx)
		}
	}
	e._removeLeavingNode(removeIndics...)
	for _, node := range e.leavingNodes {
		snap.Nodes = append(snap.Nodes, node.snapshot)
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
			if _, exists := e.inSync.Load(id); exists {
				atomic.AddUint64(&e.Metrics.Sync.Timeout, 1)
				atomic.AddUint64(&e.Metrics.Sync.InProgress, 0xFFFFFFFFFFFFFFFF)
				e.inSync.Delete(id)
			}
		})
	}

	for _, node := range nodes {
		id := e.generateMessageID()
		e.inSync.Store(id, node)
		setTimeout(id)
		e.sendProto(node, &pb.Sync{
			Id:      id,
			Cluster: snap,
		})

		atomic.AddUint64(&e.Metrics.Sync.InProgress, 1)
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

			if node == nil { // no related node found in cluster.
				// stage: check swim tag.
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

				// stage: check whether node is in leaving state.
				var leaving *leavingNode
				{
					hist, max, hit := make(map[int]uint), uint(0), -1
					e.lock.RLock()
					for _, name := range names {
						idx, hasNode := e.leaveingNodeNameIndex[name]
						if !hasNode {
							continue
						}

						cnt, exists := hist[idx]
						if !exists {
							cnt = 0
						}
						cnt++
						hist[idx] = cnt
						if cnt > max {
							hit, max = idx, cnt
						}
					}
					if hit >= 0 {
						leaving = e.leavingNodes[hit]
					}
					e.lock.RUnlock()
				}
				if leaving != nil {
					leavingTagIdx, oldSnapshot := leaving.tagIdx, leaving.snapshot
					if len(oldSnapshot.Kvs) <= leavingTagIdx {
						e.log.Warnf("[BUG!] tagIdx in leaving node exceeds length of snapshot. {node = %v, invalid_index = %v}", leaving.names, leavingTagIdx)
						leavingTagIdx = -1
					}
					if leavingTagIdx >= 0 {
						if key := oldSnapshot.Kvs[leavingTagIdx].Key; key != e.swimTagKey {
							e.log.Warnf("[BUG!] tagIdx in leaving node points to another entry instead of SWIM tag. {node = %v, invalid_index = %v, mispoint_entry_key = %v}", leaving.names, leavingTagIdx, key)
							leavingTagIdx = -1
						}
					}
					if leavingTagIdx < 0 {
						for idx, entry := range oldSnapshot.Kvs {
							if entry.Key == e.swimTagKey {
								leavingTagIdx = idx
								break
							}
						}
					}
					if leavingTagIdx < 0 {
						e.log.Warnf("[BUG!] a leaving node is with no SWIM tag. {node = %v}", leaving.names)
						e.untraceLeaveingNode(leaving)
					} else {
						raw, leavingTag := leaving.snapshot.Kvs[leavingTagIdx].Value, &SWIMTag{}
						if err := leavingTag.Decode(raw); err != nil {
							e.log.Warnf("a leaving node is with invalid SWIM tag. (decode err = \"%v\").", err.Error())
							e.untraceLeaveingNode(leaving)
							node = nil
						} else if tag.Version <= leavingTag.Version {
							// this is a lag message. won't be accepted.
							continue
						} else {
							node = nil // seem to be revived. accept it but should be as a new node.
						}
					}
				}

				// now, it's certainly a new node.
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
			if _, err := t.RemoveNode(newNode); err != nil {
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
		atomic.AddUint64(&e.Metrics.Sync.InProgress, 0xFFFFFFFFFFFFFFFF) // -1
	}

	if isResponse {
		atomic.AddUint64(&e.Metrics.Sync.Success, 1)
		e.inSync.Delete(sync.Id)

	} else {
		atomic.AddUint64(&e.Metrics.Sync.Incoming, 1)

		e.sendProto(from, &pb.Sync{
			Id:      sync.Id,
			Cluster: snap,
		})
	}

}
