package gossip

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/crossmesh/sladder"
	"github.com/stretchr/testify/assert"
)

func dumpNodeKVEntry(buf *bytes.Buffer, entries []*sladder.KeyValue, before string) bool {
	if len(entries) < 1 {
		return false
	}

	buf.WriteString(fmt.Sprintln(before))
	for _, entry := range entries {
		buf.WriteString(fmt.Sprintln("      key = ", entry.Key, "  value = ", entry.Value))
	}
	return true
}

func dumpViewPoint(t *testing.T, vps []*testClusterViewPoint, names, kv bool) bool {
	if !names && !kv {
		return false
	}
	for _, vp := range vps {
		if names {
			nodeNames := []interface{}{}
			vp.cv.RangeNodes(func(n *sladder.Node) bool {
				nodeNames = append(nodeNames, n.PrintableName())
				return true
			}, false, false)
			t.Log("self =", vp.cv.Self().PrintableName(), ",all =", nodeNames)
		} else {
			t.Log("self =", vp.cv.Self().PrintableName())
		}
		if kv {
			vp.cv.RangeNodes(func(n *sladder.Node) bool {
				entries := n.KeyValueEntries(true)
				t.Log("    node = ", n.PrintableName(), "entry count = ", len(entries))
				for _, entry := range entries {
					t.Log("      key = ", entry.Key, "  value = ", entry.Value)
				}
				return true
			}, false, false)
		}
	}

	return true
}

func dumpEntryDiff(buf *bytes.Buffer, olds, news []*sladder.KeyValue, before string) bool {
	// assume: key entry is unique.

	sort.Slice(olds, func(i, j int) bool { return olds[i].Key < olds[j].Key })
	sort.Slice(news, func(i, j int) bool { return news[i].Key < news[j].Key })
	oIdx, nIdx, dumpedBefore := 0, 0, false

	dumpBefore := func() {
		if dumpedBefore {
			return
		}
		buf.WriteString(before + "\n")
		dumpedBefore = true
	}

	logDeletion := func(e *sladder.KeyValue) {
		dumpBefore()
		buf.WriteString("[deletion]     key = " + e.Key + ", value = " + e.Value)
	}
	logChange := func(ori, new *sladder.KeyValue) {
		dumpBefore()
		buf.WriteString("[value change] key = " + ori.Key + ", value = " + ori.Value + " --> " + new.Value + "\n")
	}
	logAddition := func(e *sladder.KeyValue) {
		dumpBefore()
		buf.WriteString("[addition]     key = " + e.Key + ", value = " + e.Value + "\n")
	}

	for oIdx < len(olds) && nIdx < len(news) {
		old, new := olds[oIdx], news[nIdx]
		if old.Key < new.Key {
			logDeletion(old)
			oIdx++

		} else if old.Key == new.Key {
			if old.Value != new.Value {
				logChange(old, new)
			}
			oIdx++
			nIdx++
		} else {
			logAddition(new)
			nIdx++
		}
	}

	for ; oIdx < len(olds); oIdx++ {
		logDeletion(olds[oIdx])
	}

	for ; nIdx < len(news); nIdx++ {
		logAddition(news[nIdx])
	}

	return dumpedBefore
}

func syncLoop(t *testing.T, vps []*testClusterViewPoint, round int, visitRound func(int) bool, logNames, logKV bool) int {
	type snapshot struct {
		name       string
		entries    []*sladder.KeyValue
		matchCount int
	}

	var snaps map[*testClusterViewPoint][]*snapshot

	logBuf := bytes.NewBuffer(nil)

	dump := func() {
		hasSnaps := snaps != nil
		if !hasSnaps {
			snaps = make(map[*testClusterViewPoint][]*snapshot)
			if dumpViewPoint(t, vps, logNames, logKV) {
				t.Log("dump initial states finished...")
			}
		}

		for _, vp := range vps {

			inlSnap := make([]*snapshot, 0, len(vps))
			vp.cv.RangeNodes(func(n *sladder.Node) bool {
				inlSnap = append(inlSnap, &snapshot{
					name:    n.PrintableName(),
					entries: n.KeyValueEntries(true),
				})
				return true
			}, false, false)
			sort.Slice(inlSnap, func(i, j int) bool { return inlSnap[i].name < inlSnap[j].name })

			prev, hasSnap := snaps[vp]
			if hasSnap {
				logBuf.Reset()
				dumped := false

				// show diff
				for _, snap := range inlSnap {
					matchIdx := sort.Search(len(prev), func(i int) bool { return prev[i].name >= snap.name })
					matched := false
					if matchIdx < len(prev) {
						match := prev[matchIdx]
						if match.name == snap.name {
							// dump diffs
							matched = true
							match.matchCount++
							dumped = dumped || dumpEntryDiff(logBuf, match.entries, snap.entries, "node = "+snap.name)
						}
					}
					if !matched {
						// dump new node
						dumped = dumped || dumpNodeKVEntry(logBuf, snap.entries,
							"NEW node = "+snap.name+", entry count = "+strconv.FormatInt(int64(len(snap.entries)), 10))
					}
				}
				// find the olds.
				for _, snap := range prev {
					if snap.matchCount < 1 {
						// dump old node
						dumped = dumped || dumpNodeKVEntry(logBuf, snap.entries,
							"DELETED node = "+snap.name+", entry count = "+strconv.FormatInt(int64(len(snap.entries)), 10))
					}
				}
				if dumped {
					t.Log("changes of " + vp.cv.Self().PrintableName() + ":")
					t.Log("\n" + string(logBuf.Bytes()))
				}
			}

			snaps[vp] = inlSnap
		}
	}

	i := 0
	cont := visitRound(i)
	dump()
	for i < round && cont {
		for _, vp := range vps {
			vp.engine.ClusterSync()
		}
		time.Sleep(time.Millisecond * 1)
		for _, vp := range vps {
			vp.cv.EventBarrier()
		}
		cont = visitRound(i)
		dump()
		t.Log("sync round", i, "finished")
		if !cont {
			break
		}
		i++
	}
	return i
}

func TestGossipEngineSync(t *testing.T) {

	god, ctl, err := newClusterGod("tst", 2, 10, nil, nil)

	assert.NotNil(t, god)
	assert.NotNil(t, ctl)
	assert.NoError(t, err)
	god.RangeVP(func(vp *testClusterViewPoint) bool {
		vp.cv.EventBarrier()
		t.Log("vp: ", vp.cv.Self().PrintableName())
		return true
	})

	vps := god.VPList()
	for _, vp := range vps {
		m := vp.engine.WrapVersionKVValidator(sladder.StringValidator{})
		assert.NoError(t, vp.cv.RegisterKey("key1", m, true, 0))
		assert.NoError(t, vp.cv.RegisterKey("key2", m, true, 0))
		assert.NoError(t, vp.cv.RegisterKey("key3", m, true, 0))
	}

	// seed
	for i := 1; i < len(vps); i++ {
		vp, nvp := vps[i-1], vps[i]

		n, err := vp.cv.NewNode()
		assert.NoError(t, err)
		if !assert.NoError(t, err) {
			break
		}
		if terr := vp.cv.Txn(func(t *sladder.Transaction) bool {
			{
				rtx, ierr := t.KV(n, "idkey")
				if ierr != nil {
					err = ierr
					return false
				}
				tx := rtx.(*sladder.TestNamesInKeyTxn)
				tx.AddName(nvp.cv.Self().Names()...)
			}
			return true
		}); !assert.NoError(t, terr) {
			break
		}
		if !assert.NoError(t, err) {
			break
		}
	}

	t.Run("basic_sync", func(t *testing.T) {
		consistAt := syncLoop(t, vps, 500, func(round int) bool {
			return !god.AllViewpointConsist(true, false)
		}, true, false)
		assert.Less(t, consistAt, 500, "node entry cannot be consist within 500 round.")
		t.Log("cluster is consist at round", consistAt)
	})

	time.Sleep(time.Second * 1)

	t.Run("key_value_sync", func(t *testing.T) {
		rvps, need := []*testClusterViewPoint{}, 3
		{
			idx := 0
			for _, vp := range vps {
				if len(rvps) < need {
					rvps = append(rvps, vp)
				} else if n := rand.Intn(idx + 1); n < need {
					rvps[n] = vp
				}
				idx++
			}
		}

		t.Run("addition", func(t *testing.T) {

			assert.NoError(t, rvps[0].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[0].cv.Self(), "key1")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("3")
				}
				return true
			}))
			assert.NoError(t, rvps[1].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[1].cv.Self(), "key2")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("444")
				}
				{
					rtx, err := tx.KV(rvps[1].cv.Self(), "key3")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("1933")
				}
				return true
			}))
			assert.NoError(t, rvps[2].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[2].cv.Self(), "key1")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("4")
				}
				return true
			}))

			t.Log("initial states: ")
			dumpViewPoint(t, rvps, true, true)
			t.Log("initial modified states dump finished.")
			consistAt := syncLoop(t, vps, 500, func(round int) bool {
				return !god.AllViewpointConsist(true, true)
			}, false, true)
			assert.Less(t, consistAt, 500, "node entry cannot be consist within 500 round.")
			t.Log("cluster is consist at round", consistAt)
		})

		t.Run("deletion", func(t *testing.T) {
			// delete.
			t.Log("test sync deletion...")
			assert.NoError(t, rvps[1].cv.Txn(func(tx *sladder.Transaction) bool {
				assert.NoError(t, tx.Delete(rvps[1].cv.Self(), "key3"))
				return true
			}))
			t.Log("initial state:")
			dumpViewPoint(t, rvps, false, true)
			t.Log("dump initial state finished.")
			consistAt := syncLoop(t, vps, 500, func(round int) bool {
				return !god.AllViewpointConsist(true, true)
			}, false, true)
			assert.Less(t, consistAt, 500, "node entry cannot be consist within 500 round.")
			t.Log("cluster is consist at round ", consistAt)
		})

		t.Run("hybird", func(t *testing.T) {
			t.Log("test sync hybird...")
			assert.NoError(t, rvps[0].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[0].cv.Self(), "key1")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("78")
				}
				{
					rtx, err := tx.KV(rvps[0].cv.Self(), "key2")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("444")
				}
				return true
			}))
			assert.NoError(t, rvps[1].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[1].cv.Self(), "key2")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("444")
				}
				{
					rtx, err := tx.KV(rvps[1].cv.Self(), "key3")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("1933d")
				}
				return true
			}))
			assert.NoError(t, rvps[2].cv.Txn(func(tx *sladder.Transaction) bool {
				{
					rtx, err := tx.KV(rvps[2].cv.Self(), "key3")
					assert.NoError(t, err)
					if err != nil {
						return false
					}
					txn := rtx.(*sladder.StringTxn)
					txn.Set("dkk")
				}
				tx.Delete(rvps[2].cv.Self(), "key1")
				return true
			}))
			dumpViewPoint(t, rvps, false, true)
			t.Log("dump initial state finished.")
			consistAt := syncLoop(t, vps, 500, func(round int) bool {
				return !god.AllViewpointConsist(true, true)
			}, false, true)
			assert.Less(t, consistAt, 500, "node entry cannot be consist within 500 round.")
			t.Log("cluster is consist at round", consistAt)
		})
	})
}
