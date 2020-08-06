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
		id := atomic.AddUint64(&e.syncCounter, 1)
		atomic.AddUint64(&e.statistics.InSync, 1)
		e.inSync.Store(id, struct{}{})
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

	// we first refine messages.
	eliNodeIdx, nIdx := 0, 0
	for nIdx < len(sync.Cluster.Nodes) {
		pn := sync.Cluster.Nodes[nIdx]

		var tag *SWIMTag = nil

		eliIdx, Idx, eIdx, reject := 0, 0, 0, false
		sort.Slice(pn.Kvs, func(i, j int) bool { return pn.Kvs[i].Key < pn.Kvs[j].Key })

		for Idx < len(pn.Kvs) {
			kv := pn.Kvs[Idx]
			if tag == nil {
				if kv.Key == e.swimTagKey {
					// find entry list first.
					t := &SWIMTag{}
					if err := t.Decode(pn.Kvs[Idx].Value); err != nil {
						e.log.Warn("drop a node with invalid swim tag in sync message. decode err = \"%v\"." + err.Error())
						reject = true
					} else {
						tag = t
					}
					// reset index then we start to filter entries.
					Idx, eliIdx = 0, 0
					continue
				}
				Idx++

			} else {
				// filter entries.
				if eIdx < len(tag.EntryList) {
					if kv.Key == e.swimTagKey || kv.Key == tag.EntryList[eIdx] {
						// accept
						pn.Kvs[eliIdx] = pn.Kvs[Idx]
						eliIdx++
						Idx++
					} else if kv.Key < tag.EntryList[eIdx] {
						// drop entry as key is not in entry list.
						Idx++
					} else {
						eIdx++
					}

				} else {
					break
				}
			}
		}
		if tag != nil {
			pn.Kvs = pn.Kvs[:eliIdx]
		}

		if !reject {
			sync.Cluster.Nodes[eliNodeIdx] = sync.Cluster.Nodes[nIdx]
			eliNodeIdx++
		}
		nIdx++
	}
	sync.Cluster.Nodes = sync.Cluster.Nodes[:eliNodeIdx]

	var (
		errs sladder.Errors
		snap *proto.Cluster
	)
	if err := e.cluster.Txn(func(t *sladder.Transaction) bool {
		// mark txn internal.
		e.innerTxnIDs.Store(t.ID(), struct{}{})

		snap = e.newSyncClusterSnapshot(t) // read a snapshot first.

		var newNode *sladder.Node

		// start apply snapshot to cluster.
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

	if _, inSync := e.inSync.Load(sync.Id); inSync {
		e.inSync.Delete(sync.Id)
		atomic.AddUint64(&e.statistics.InSync, 0xFFFFFFFFFFFFFFFF) // -1
		atomic.AddUint64(&e.statistics.FinishedSync, 1)
	}

	e.sendProto(from, &pb.Sync{
		Id:      sync.Id,
		Cluster: snap,
	})
}
