package gossip

import (
	"testing"
	"time"

	"github.com/crossmesh/sladder"
	"github.com/stretchr/testify/assert"
)

func checkInitialSWIMStates(t *testing.T, god *testClusterGod) {
	// check initial states.
	god.RangeVP(func(vp *testClusterViewPoint) bool {
		vp.cv.Txn(func(tx *sladder.Transaction) bool {
			var nodes []*sladder.Node

			tx.RangeNode(func(n *sladder.Node) bool {
				nodes = append(nodes, n)
				return true
			}, false, false)

			for _, node := range nodes {
				rtx, err := tx.KV(node, vp.engine.swimTagKey)
				if assert.NoError(t, err) {
					return false
				}
				tag := rtx.(*SWIMTagTxn)
				assert.Equal(t, ALIVE, tag.State())
			}

			return false
		})
		return true
	})
}

func TestFailureDetector(t *testing.T) {
	period := time.Millisecond * 50

	t.Run("quit", func(t *testing.T) {
		t.Parallel()

		t.Run("trivial", func(t *testing.T) {
			t.Parallel()

			god, ctl, err := newClusterGod("tst", 2, 4, nil, nil)
			assert.NotNil(t, god)
			assert.NotNil(t, ctl)
			assert.NoError(t, err)

			god.RangeVP(func(vp *testClusterViewPoint) bool {
				assert.NotNil(t, vp)
				assert.NoError(t, vp.cv.Quit())
				return true
			})
		})

		t.Run("normal", func(t *testing.T) {
			t.Parallel()
			requiredQuorum := uint(8)

			god, ctl, err := newHealthyClusterGod(t, "fd-tst", 2, 8, []sladder.EngineOption{
				WithGossipPeriod(period),
				WithMinRegionPeer(requiredQuorum),
			}, nil)

			assert.NotNil(t, god)
			assert.NotNil(t, ctl)
			assert.NoError(t, err)
			checkInitialSWIMStates(t, god)

			vps := god.VPList()
			quitVP, quited := vps[0], false
			quitNodeNames := quitVP.cv.Self().Names()
			t.Log("quit node =", quitNodeNames)

			go func() {
				assert.NoError(t, quitVP.cv.Quit())
				ctl.RemoveTransportTarget(quitNodeNames...)
				t.Log("quiting node shutdown.")
				quited = true
			}()

			for _, vp := range vps[1:] {
				// validate state change.
				vp.cv.Keys(vp.engine.swimTagKey).
					Watch(func(ctx *sladder.WatchEventContext, meta sladder.KeyValueEventMetadata) {
						if sladder.ValueChanged == meta.Event() {
							tag := &SWIMTag{}
							meta := meta.(sladder.KeyChangeEventMetadata)
							if !assert.NoError(t, tag.Decode(meta.New())) {
								return
							}
							// should not go to DEAD.
							assert.NotEqual(t, DEAD, tag.State)
						}
					})
			}

			consistAt := syncLoop(t, vps, 200, func(round int) (unconsist bool) {
				time.Sleep(period)
				unconsist = !ViewpointConsist(vps[1:], true, true)
				if unconsist {
					return
				}
				unconsist = false
				for _, vp := range vps[1:] {
					vp.cv.Txn(func(t *sladder.Transaction) bool {
						node := t.MostPossibleNode(quitNodeNames)
						if node != nil {
							unconsist = true
						}
						return false
					})
					if unconsist {
						break
					}
				}
				return
			}, true, true, false, func(e *EngineInstance) {
				e.ClusterSync()
				e.DetectFailure()
				e.ClearSuspections()
			})
			assert.True(t, quited)
			if !assert.Less(t, consistAt, 200, "cluster state doesn't reach the required within 200 round.") {
				dumpViewPoint(t, vps, true, true)
			} else {
				t.Log("one of other viewpoints:")
				dumpSingleViewPoint(t, vps[1], true, true)
			}
		})
	})

	t.Run("one_node_failure", func(t *testing.T) {
		t.Parallel()
		god, ctl, err := newHealthyClusterGod(t, "fd-tst", 2, 8, []sladder.EngineOption{
			WithGossipPeriod(period),
		}, nil)
		assert.NotNil(t, god)
		assert.NotNil(t, ctl)
		assert.NoError(t, err)
		vps := god.VPList()
		checkInitialSWIMStates(t, god)

		// seperate this node.
		failVP := vps[0]

		t.Log("dead node: ", failVP.cv.Self().Names())

		ctl.NetworkOutJam(failVP.cv.Self().Names())
		ctl.NetworkInJam(failVP.cv.Self().Names())

		consistAt := syncLoop(t, vps, 200, func(round int) bool {
			time.Sleep(period)
			hasAnother := false
			failVP.cv.RangeNodes(func(node *sladder.Node) bool {
				hasAnother = true
				return false
			}, true, false)
			return hasAnother || !ViewpointConsist(vps[1:], true, true)
		}, true, true, false, func(e *EngineInstance) {
			e.ClusterSync()
			e.DetectFailure()
			e.ClearSuspections()
		})
		if !assert.Less(t, consistAt, 200, "cluster state doesn't reach the required within 500 round.") {
			dumpViewPoint(t, vps, true, true)
		} else {
			t.Log("seperated node viewpoint:")
			dumpSingleViewPoint(t, failVP, true, true)
			t.Log("one of other viewpoints:")
			dumpSingleViewPoint(t, vps[1], true, true)
		}
		t.Log("cluster state is consist at round", consistAt)
	})

	t.Run("preseve_region_quorum", func(t *testing.T) {
		t.Parallel()
		requiredQuorum := uint(3)

		god, ctl, err := newHealthyClusterGod(t, "fd-tst", 2, 8, []sladder.EngineOption{
			WithGossipPeriod(period),
			WithMinRegionPeer(requiredQuorum),
		}, nil)
		assert.NotNil(t, god)
		assert.NotNil(t, ctl)
		assert.NoError(t, err)
		checkInitialSWIMStates(t, god)

		vps := god.VPList()
		// seperate this node.
		seperatedVP := vps[0]
		t.Log("seperated node: ", seperatedVP.cv.Self().Names())
		ctl.NetworkOutJam(seperatedVP.cv.Self().Names())
		ctl.NetworkInJam(seperatedVP.cv.Self().Names())

		quorums := uint(1)
		consistAt := syncLoop(t, vps, 200, func(round int) bool {
			time.Sleep(period)
			quorums = uint(1)
			nodes := []*sladder.Node{}
			seperatedVP.cv.RangeNodes(func(node *sladder.Node) bool {
				quorums++
				nodes = append(nodes, node)
				return true
			}, true, false)
			allDead := true
			seperatedVP.cv.Txn(func(tx *sladder.Transaction) bool {
				for _, node := range nodes {
					rtx, err := tx.KV(node, seperatedVP.engine.swimTagKey)
					if !assert.NoError(t, err) {
						t.FailNow()
						return false
					}
					tag := rtx.(*SWIMTagTxn)
					if tag.State() != DEAD {
						allDead = false
					}
				}
				return true
			})
			return !allDead || quorums != requiredQuorum || !ViewpointConsist(vps[1:], true, true)
		}, true, true, false, func(e *EngineInstance) {
			e.ClusterSync()
			e.DetectFailure()
			e.ClearSuspections()
		})
		if !assert.Less(t, consistAt, 200, "cluster state doesn't reach the required within 500 round.") {
			dumpViewPoint(t, vps, true, true)
		} else {
			t.Log("seperated node viewpoint:")
			dumpSingleViewPoint(t, seperatedVP, true, true)
			t.Log("one of other viewpoints:")
			dumpSingleViewPoint(t, vps[1], true, true)
		}
		t.Log("cluster state is consist at round", consistAt, ", quorum of seperated node = ", quorums)
	})

	t.Run("partition_recover", func(t *testing.T) {
		t.Parallel()
		requiredQuorum := uint(3)
		groupQuorum := uint(2)

		god, ctl, err := newHealthyClusterGod(t, "fd-tst", 2, 6, []sladder.EngineOption{
			WithGossipPeriod(period),
			WithMinRegionPeer(requiredQuorum),
		}, nil)
		assert.NotNil(t, god)
		assert.NotNil(t, ctl)
		assert.NoError(t, err)
		checkInitialSWIMStates(t, god)

		vps := god.VPList()

		buildGroup := func(vps []*testClusterViewPoint) (names [][]string) {
			for _, vp := range vps {
				names = append(names, vp.self.Names())
			}
			return names
		}

		gvp1, gvp2 := vps[:groupQuorum], vps[groupQuorum:]
		grp1, grp2 := buildGroup(gvp1), buildGroup(gvp2)
		t.Log("group1 = ", grp1, "group2 = ", grp2)
		np, err := ctl.NetworkPartition(grp1, grp2)
		assert.NoError(t, err)
		assert.NotNil(t, np)

		{

			t.Log("================= stage 1 =================")
			t.Log("cut network into two group. wait for consistency of group states.")
			consistAt := syncLoop(t, vps, 200, func(round int) (unconsist bool) {
				time.Sleep(period)

				actualQuorumRequired := requiredQuorum
				if groupQuorum > actualQuorumRequired {
					actualQuorumRequired = groupQuorum
				}
				for _, vp := range gvp1 {
					quorums := uint(1)
					nodes := []*sladder.Node{}
					vp.cv.RangeNodes(func(node *sladder.Node) bool {
						quorums++
						nodes = append(nodes, node)
						return true
					}, true, false)
					if quorums != actualQuorumRequired {
						return true
					}

					if !assert.GreaterOrEqual(t, uint(len(nodes))+1, requiredQuorum) {
						t.FailNow()
					}

					allDead := true
					selfName := vp.self.Names()
					vp.cv.Txn(func(tx *sladder.Transaction) bool {
						for _, node := range nodes {
							names := tx.Names(node)
							if np.Jam(names, selfName) {
								rtx, err := tx.KV(node, vp.engine.swimTagKey)
								if !assert.NoError(t, err) {
									t.FailNow()
									return false
								}
								tag := rtx.(*SWIMTagTxn)
								if tag.State() != DEAD {
									allDead = false
									return false
								}
							}
						}
						return true
					})
					if !allDead {
						return true
					}
				}

				actualQuorumRequired = requiredQuorum
				if quorum := uint(len(vps)) - groupQuorum; quorum > actualQuorumRequired {
					actualQuorumRequired = quorum
				}
				for _, vp := range gvp2 {
					quorums := uint(1)
					nodes := []*sladder.Node{}
					allDead := true
					vp.cv.RangeNodes(func(node *sladder.Node) bool {
						quorums++
						nodes = append(nodes, node)
						return true
					}, true, false)
					if quorums != actualQuorumRequired {
						return true
					}

					if !assert.GreaterOrEqual(t, uint(len(nodes))+1, requiredQuorum) {
						t.FailNow()
					}

					allDead = true
					selfName := vp.self.Names()
					vp.cv.Txn(func(tx *sladder.Transaction) bool {
						for _, node := range nodes {
							names := tx.Names(node)
							if np.Jam(names, selfName) {
								rtx, err := tx.KV(node, vp.engine.swimTagKey)
								if !assert.NoError(t, err) {
									t.FailNow()
									return false
								}
								tag := rtx.(*SWIMTagTxn)
								if tag.State() != DEAD {
									allDead = false
									return false
								}
							}
						}
						return true
					})
					if !allDead {
						return true
					}
				}

				return false

				//return !ViewpointConsist(gvp1, true, true) || !ViewpointConsist(gvp2, true, true)
			}, true, true, false, func(e *EngineInstance) {
				e.ClusterSync()
				e.DetectFailure()
				e.ClearSuspections()
			})

			if !assert.Less(t, consistAt, 200, "group state doesn't reach the required within 200 round.") {
				t.Log("group 1: ")
				dumpViewPoint(t, gvp1, true, true)
				t.Log("group 2: ")
				dumpViewPoint(t, gvp2, true, true)
				t.FailNow()
			} else {
				t.Log("group state is consist at round", consistAt)
				t.Log("one of viewpoint in group 1:")
				dumpSingleViewPoint(t, gvp1[0], true, true)
				t.Log("one of viewpoint in group 2:")
				dumpSingleViewPoint(t, gvp2[0], true, true)
			}
		}
		{
			t.Log("================= stage 2 =================")
			t.Log("remove partition. cluster should recover from bad state itself.")
			ctl.RemovePartition(np)

			consistAt := syncLoop(t, vps, 200, func(round int) (unconsist bool) {
				time.Sleep(period)
				unconsist = !ViewpointConsist(vps, true, true)
				if unconsist {
					return
				}
				vp := vps[0]
				vp.cv.Txn(func(tx *sladder.Transaction) bool {
					var nodes []*sladder.Node
					tx.RangeNode(func(n *sladder.Node) bool {
						nodes = append(nodes, n)
						return true
					}, false, false)
					assert.Equal(t, len(vps), len(nodes))
					for _, node := range nodes {
						rtx, err := tx.KV(node, vp.engine.swimTagKey)
						if !assert.NoError(t, err) {
							t.FailNow()
							return false
						}
						tag := rtx.(*SWIMTagTxn)
						if ALIVE != tag.State() {
							unconsist = true
							return false
						}
					}
					return false
				})
				return unconsist
			}, true, true, false, func(e *EngineInstance) {
				e.ClusterSync()
				e.DetectFailure()
				e.ClearSuspections()
			})
			if !assert.Less(t, consistAt, 200, "cluster doesn't recover from network partiation within 200 round.") {
				t.Log("group 1: ")
				dumpViewPoint(t, gvp1, true, true)
				t.Log("group 2: ")
				dumpViewPoint(t, gvp2, true, true)
				t.FailNow()
			} else {
				t.Log("group state is consist at round", consistAt)
				t.Log("one of viewpoint: ")
				dumpSingleViewPoint(t, vps[0], true, true)
			}
		}
	})

	t.Run("region_changes", func(t *testing.T) {
		t.Parallel()
		god, ctl, err := newHealthyClusterGod(t, "fd-tst", 2, 8, []sladder.EngineOption{
			WithGossipPeriod(period),
		}, nil)
		assert.NotNil(t, god)
		assert.NotNil(t, ctl)
		assert.NoError(t, err)
		vps := god.VPList()
		checkInitialSWIMStates(t, god)

		// node to change region.
		changeVP := vps[0]
		old, err := changeVP.engine.SetRegion("tst-region-changed")
		assert.NoError(t, err)
		assert.Equal(t, "", old)
		assert.Equal(t, "tst-region-changed", changeVP.engine.Region())
		consistAt := syncLoop(t, vps, 200, func(round int) (unconsist bool) {
			return !ViewpointConsist(vps, true, true)
		}, true, true, false, func(e *EngineInstance) {
			e.ClusterSync()
			e.DetectFailure()
			e.ClearSuspections()
		})

		if !assert.Less(t, consistAt, 200, "cluster doesn't be consist within 200 round.") {
			time.Sleep(period)
			dumpViewPoint(t, vps, true, true)
		} else {
			t.Log("group state is consist at round", consistAt)
			t.Log("one of viewpoint: ")
			dumpSingleViewPoint(t, vps[0], true, true)
		}
	})
}
