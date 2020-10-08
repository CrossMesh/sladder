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
	"github.com/golang/protobuf/ptypes"
)

func (e *EngineInstance) goClusterSync() {
	if !e.disableSync {
		e.tickGossipPeriodGo(func(deadline time.Time) {
			e.ClusterSync()
		})
	}
}

func (e *EngineInstance) _clearNodeFromSyncer(node *sladder.Node) {
	delete(e.reversedExistence, node)
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
	if !e.arbiter.ShouldRun() {
		return
	}

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

	for _, node := range nodes {
		id := e.generateMessageID()
		e.sendProto(node, &pb.Sync{
			Id:      id,
			Cluster: snap,
			Type:    pb.Sync_PushPull,
		})

		atomic.AddUint64(&e.Metrics.Sync.PushPull, 1)
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

	needPush := false
	switch sync.Type {
	case pb.Sync_Unknown:
		// won't ack to prevent infinite loop. treat it as a push.
		needPush = false
		atomic.AddUint64(&e.Metrics.Sync.IncomingPush, 1)

	case pb.Sync_Push:
		needPush = false
		atomic.AddUint64(&e.Metrics.Sync.IncomingPush, 1)

	case pb.Sync_PushPull:
		needPush = true
		atomic.AddUint64(&e.Metrics.Sync.IncomingPushPull, 1)

	default:
		return
	}

	asyncSendPush := func(snap *proto.Cluster) {
		e.arbiter.Go(func() {
			e.sendProto(from, &pb.Sync{
				Id:      sync.Id,
				Cluster: snap,
				Type:    pb.Sync_Push,
			})
			atomic.AddUint64(&e.Metrics.Sync.Push, 1)
		})
	}

	var errs sladder.Errors
	if err := e.cluster.Txn(func(t *sladder.Transaction) bool {
		type relatedInfo struct {
			node      *sladder.Node
			names     []string
			tagKeyIdx int
			tagInMsg  *SWIMTag
		}
		var err error

		fromNode, selfSeen := t.MostPossibleNode(from), false

		// mark txn internal.
		e.innerTxnIDs.Store(t.ID(), struct{}{})

		self := e.cluster.Self()

		fastPush := true

		infos := make([]relatedInfo, len(sync.Cluster.Nodes))
		for idx, mnode := range sync.Cluster.Nodes { // build relation info.

			info := &infos[idx]
			info.tagKeyIdx = -1

			if infos[idx].node, info.names, err = t.MostPossibleNodeFromProtobuf(mnode.Kvs); err != nil {
				errs = append(errs, err, fmt.Errorf("node lookup failure"))
				return false
			}
			if len(info.names) < 1 { // skip in case of a remote anonymous node.
				continue
			}

			// search for SWIM tag.
			for idx, kv := range mnode.Kvs {
				if kv.Key == e.swimTagKey {
					info.tagKeyIdx = idx
					break
				}
			}
			// pre-decode
			if info.tagKeyIdx >= 0 {
				info.tagInMsg = &SWIMTag{}
				if err := info.tagInMsg.Decode(mnode.Kvs[info.tagKeyIdx].Value); err != nil {
					e.log.Warnf("drop a node with invalid swim tag in sync message. (decode err = \"%v\").", err.Error())
					return false
				}
			}
			if needPush {
				// can send response immediately?
				if fastPush && info.node != nil && info.tagInMsg != nil && info.tagInMsg.State == LEFT {
					fastPush = false
				}
			}
		}

		if needPush {
			// send response.
			snap := e.newSyncClusterSnapshot(t)
			if fastPush {
				asyncSendPush(snap)
			} else {
				t.DeferOnCommit(func() { asyncSendPush(snap) })
			}
		}

		// start apply snapshot to cluster.
		var newNode *sladder.Node
		for idx, mnode := range sync.Cluster.Nodes {
			info := infos[idx]
			tagKeyIndex, node, names := info.tagKeyIdx, info.node, info.names

			if len(names) < 1 {
				continue
			}

			if node == nil { // no related node found in cluster.
				// stage: check swim tag.
				if tagKeyIndex < 0 {
					e.log.Warn("drop a node without swim tag.")
					continue
				}

				tag := info.tagInMsg

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

			} else {
				if tagKeyIndex < 0 && t.KeyExists(node, e.swimTagKey) {
					continue
				}

				if !needPush && node == self {
					selfSeen = true // the remote knows myself.
				}
			}

			useNewNode := false

			// stage: may apply swim tag first. (if any)
			if tagKeyIndex > 0 {
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

			// stage: may drop invalid entries in snapshot according existing SWIM tag (if any).
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

				// drop invalid entries; refine message.
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

		if !needPush {
			rawMessageID := sync.Id - e.counterSeed

			// stage: trace self existence seen by others.
			if fromNode != nil {
				e.lock.Lock()

				trace, _ := e.reversedExistence[fromNode]
				if trace == nil {
					trace = &reversedExistenceItem{}
					e.reversedExistence[fromNode] = trace
				}
				if rawMessageID > trace.epoch {
					trace.epoch, trace.exist = rawMessageID, selfSeen
				}
				e.lock.Unlock()
			}

			if e.quitAfter > 0 && selfSeen { // quiting. determine whether quit condition is reached.
				// ensure LEAVE state included in this synchronizaion.
				if rawMessageID >= e.quitAfter {
					// LEAVE state has spreaded.
					e.canQuit = true
				}
			}
		}

		// end snapshot applying.
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
}
