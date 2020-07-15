package gossip

import (
	"math/rand"
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

	t.Run("key_value_sync", func(t *testing.T) {
		vp := vps[rand.Intn(len(vps))]
		vp.cv.Txn(func(tx *sladder.Transaction) bool {
			{
				rtx, err := tx.KV(vp.cv.Self(), "key1")
				assert.NoError(t, err)
				if err != nil {
					return false
				}
				txn := rtx.(*sladder.StringTxn)
				txn.Set("3")
			}
			return true
		})
		for _, v := range vp.cv.Self().KeyValueEntries(true) {
			t.Log(v)
		}
	})
}
