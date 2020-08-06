package gossip

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sunmxt/sladder"
)

func TestGossipEngineSync(t *testing.T) {
	god, ctl, err := newClusterGod("tst", 2, 20, nil, nil)

	assert.NotNil(t, god)
	assert.NotNil(t, ctl)
	assert.NoError(t, err)
	god.RangeVP(func(vp *testClusterViewPoint) bool {
		vp.cv.EventBarrier()
		t.Log("vp: ", vp.cv.Self().Names())
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
		// sync until all viewpoints are consistent.
		i, maxTimes := 0, 500
		for i < maxTimes {
			for _, vp := range vps {
				vp.engine.ClusterSync()
			}
			time.Sleep(time.Microsecond * 2)
			for _, vp := range vps {
				vp.cv.EventBarrier()
				nodeNames := []interface{}{}
				vp.cv.RangeNodes(func(n *sladder.Node) bool {
					nodeNames = append(nodeNames, n.Names())
					return true
				}, false, false)
				t.Log("self =", vp.cv.Self().Names(), ",all =", nodeNames)
			}
			t.Log("sync round", i, "finished")
			if god.AllViewpointConsist(true, false) {
				t.Log("node list consist in sync round", i)
				break
			}
			i++
		}
		assert.Less(t, i, maxTimes, "node list cannot be consistency within "+strconv.FormatInt(int64(maxTimes), 10)+" rounds.")
	})
	//
	//	time.Sleep(time.Second * 2)
	//	t.Run("key_value_sync", func(t *testing.T) {
	//		rvps, need := []*testClusterViewPoint{}, 3
	//		{
	//			idx := 0
	//			for _, vp := range vps {
	//				if len(rvps) < need {
	//					rvps = append(rvps, vp)
	//				} else if n := rand.Intn(idx + 1); n < need {
	//					rvps[n] = vp
	//				}
	//				idx++
	//			}
	//		}
	//
	//		assert.NoError(t, rvps[0].cv.Txn(func(tx *sladder.Transaction) bool {
	//			{
	//				rtx, err := tx.KV(rvps[0].cv.Self(), "key1")
	//				assert.NoError(t, err)
	//				if err != nil {
	//					return false
	//				}
	//				txn := rtx.(*sladder.StringTxn)
	//				txn.Set("3")
	//			}
	//			return true
	//		}))
	//		assert.NoError(t, rvps[1].cv.Txn(func(tx *sladder.Transaction) bool {
	//			{
	//				rtx, err := tx.KV(rvps[1].cv.Self(), "key2")
	//				assert.NoError(t, err)
	//				if err != nil {
	//					return false
	//				}
	//				txn := rtx.(*sladder.StringTxn)
	//				txn.Set("444")
	//			}
	//			{
	//				rtx, err := tx.KV(rvps[1].cv.Self(), "key3")
	//				assert.NoError(t, err)
	//				if err != nil {
	//					return false
	//				}
	//				txn := rtx.(*sladder.StringTxn)
	//				txn.Set("1933")
	//			}
	//			return true
	//		}))
	//		assert.NoError(t, rvps[2].cv.Txn(func(tx *sladder.Transaction) bool {
	//			{
	//				rtx, err := tx.KV(rvps[2].cv.Self(), "key1")
	//				assert.NoError(t, err)
	//				if err != nil {
	//					return false
	//				}
	//				txn := rtx.(*sladder.StringTxn)
	//				txn.Set("4")
	//			}
	//			return true
	//		}))
	//		// Log node states.
	//		for _, vp := range rvps {
	//			vp.cv.EventBarrier()
	//			t.Log("self = ", vp.cv.Self().Names())
	//
	//			vp.cv.RangeNodes(func(n *sladder.Node) bool {
	//				entries := n.KeyValueEntries(true)
	//				t.Log("    node = ", n.Names(), "entry count = ", len(entries))
	//				for _, entry := range entries {
	//					t.Log("      key = ", entry.Key, "  value = ", entry.Value)
	//				}
	//				return true
	//			}, false, false)
	//		}
	//		t.FailNow()
	//		{
	//			t.Log("starting syncing test...")
	//			i, maxTimes := 0, 500
	//			for i < maxTimes {
	//				for _, vp := range vps {
	//					vp.engine.ClusterSync()
	//					vp.cv.EventBarrier()
	//				}
	//
	//				// Log node states.
	//				for _, vp := range vps {
	//					t.Log("self = ", vp.cv.Self().Names())
	//
	//					vp.cv.RangeNodes(func(n *sladder.Node) bool {
	//						t.Log("    node = ", n.Names())
	//						for _, entry := range n.KeyValueEntries(false) {
	//							t.Log("      key = ", entry.Key, "  value = ", entry.Value)
	//						}
	//						return true
	//					}, false, false)
	//				}
	//
	//				t.Log("sync round", i, "finished")
	//				if god.AllViewpointConsist(true, false) {
	//					t.Log("node list consist in sync round", i)
	//					break
	//				}
	//				i++
	//			}
	//		}
	//	})
}
