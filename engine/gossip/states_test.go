package gossip

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sunmxt/sladder"
)

func TestSWIMTagValidator(t *testing.T) {
	god, ctl, err := newClusterGod("test_validator", 1, 2, nil, nil)
	assert.NotNil(t, god)
	assert.NotNil(t, ctl)
	assert.NoError(t, err)

	v := &SWIMTagValidator{}
	god.RangeVP(func(vp *testClusterViewPoint) bool {
		v.engine = vp.engine
		return false
	})

	assert.True(t, v.Validate(sladder.KeyValue{Key: "tst", Value: ""}))
	assert.True(t, v.Validate(sladder.KeyValue{Key: "tst", Value: "{}"}))
	assert.False(t, v.Validate(sladder.KeyValue{Key: "tst", Value: "[]"}))
	assert.False(t, v.Validate(sladder.KeyValue{Key: "tst", Value: "daslkjklad"}))

	// sync tests.
	{
		txn, err := v.Txn(sladder.KeyValue{Key: "tst", Value: ""})
		assert.NoError(t, err)
		assert.IsType(t, &SWIMTagTxn{}, txn)
	}
	{
		txn, err := v.Txn(sladder.KeyValue{Key: "tst", Value: "{}"})
		assert.NoError(t, err)
		assert.IsType(t, &SWIMTagTxn{}, txn)
	}
	{
		ok, err := v.Sync(nil, nil)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
	{
		ok, err := v.Sync(&sladder.KeyValue{}, nil)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
	{
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: "mk",
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: ""})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "", e.Value)
	}
	{
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: "mk",
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: "kk"})
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, "mk", e.Value)
	}
	// accept new version.
	{
		old := (&SWIMTag{
			Version: 1, State: DEAD, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 2, State: ALIVE, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, new, e.Value)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 2, State: ALIVE, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, old, e.Value)
	}
	// accept remote suspection
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, SUSPECTED)
		assert.Equal(t, tag.Version, uint32(3))
	}
	// ALIVE cannot overwrite SUSPECTED.
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.False(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, SUSPECTED)
		assert.Equal(t, tag.Version, uint32(3))
	}
	// DEAD overwrite ALIVE, SUSPECTED.
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, DEAD)
		assert.Equal(t, tag.Version, uint32(3))
	}
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, DEAD)
		assert.Equal(t, tag.Version, uint32(3))
	}
	// LEFT overwrite any.
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, LEFT)
		assert.Equal(t, tag.Version, uint32(3))
	}
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, LEFT)
		assert.Equal(t, tag.Version, uint32(3))
	}
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		new := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region1",
		}).Encode()
		e := &sladder.KeyValue{
			Key:   "tst",
			Value: old,
		}
		ok, err := v.Sync(e, &sladder.KeyValue{Key: "tst", Value: new})
		assert.NoError(t, err)
		assert.True(t, ok)
		tag := &SWIMTag{}
		assert.NoError(t, tag.Decode(e.Value))
		assert.Equal(t, tag.Region, "region2")
		assert.Equal(t, tag.State, LEFT)
		assert.Equal(t, tag.Version, uint32(3))
	}

	// transaction tests.
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		assert.IsType(t, &SWIMTagTxn{}, rtx)
		txn := rtx.(*SWIMTagTxn)
		assert.Equal(t, old, txn.Before())
		{
			changed, val := txn.Updated(), txn.After()
			assert.False(t, changed)
			assert.Equal(t, old, val)
		}
		assert.Equal(t, "region2", txn.Region())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, uint32(3), txn.Version())
	}
	// ClaimDead() overwrites SUSPECTED and ALIVE but doesn't change version.
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimDead()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, DEAD, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimDead()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, DEAD, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimDead()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, LEFT, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimDead()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, DEAD, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	// ClaimSuspected
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimSuspected()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, SUSPECTED, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimSuspected()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, SUSPECTED, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimSuspected()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, DEAD, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimSuspected()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, LEFT, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	// Leave.
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.Leave()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, LEFT, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.Leave()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, LEFT, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	// ClaimAlive
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimAlive()
		txn.ClaimAlive()
		txn.ClaimAlive()
		assert.Equal(t, uint32(3), txn.Version())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.False(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: SUSPECTED, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimAlive()
		txn.ClaimAlive()
		txn.ClaimAlive()
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: DEAD, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimAlive()
		txn.ClaimAlive()
		txn.ClaimAlive()
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: LEFT, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.ClaimAlive()
		txn.ClaimAlive()
		txn.ClaimAlive()
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, "region2", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}

	// SetRegion()
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.SetRegion("region3")
		txn.SetRegion("region3")
		txn.SetRegion("region3")
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, ALIVE, txn.State())
		assert.Equal(t, "region3", txn.Region())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	// Entry List.
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2",
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.AddToEntryList()
		txn.AddToEntryList("1", "2", "3")
		txn.AddToEntryList("1", "2", "3")
		txn.AddToEntryList("1", "2", "3")
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, 3, len(txn.EntryList(false)))
		assert.Contains(t, txn.EntryList(false), "1")
		assert.Contains(t, txn.EntryList(false), "2")
		assert.Contains(t, txn.EntryList(false), "3")
		assert.Equal(t, ALIVE, txn.State())
		changed := txn.Updated()
		assert.True(t, changed)
	}
	{
		old := (&SWIMTag{
			Version: 3, State: ALIVE, Region: "region2", EntryList: []string{"1", "2", "3", "dd", "cc", "ee"},
		}).Encode()
		rtx, err := v.Txn(sladder.KeyValue{
			Key: "r", Value: old,
		})
		assert.NoError(t, err)
		txn := rtx.(*SWIMTagTxn)
		txn.RemoveFromEntryList()
		txn.RemoveFromEntryList("dd", "cc")
		txn.RemoveFromEntryList("dd", "cc")
		txn.RemoveFromEntryList("dd", "cc", "ee")
		txn.RemoveFromEntryList("ee")
		assert.Equal(t, uint32(4), txn.Version())
		assert.Equal(t, 3, len(txn.EntryList(false)))
		assert.Contains(t, txn.EntryList(false), "1")
		assert.Contains(t, txn.EntryList(false), "2")
		assert.Contains(t, txn.EntryList(false), "3")
		assert.Equal(t, ALIVE, txn.State())
		changed := txn.Updated()
		assert.True(t, changed)
	}

}

func TestSWIMTag(t *testing.T) {
	tag := &SWIMTag{}
	assert.NotPanics(t, func() {
		tag.Encode()
	})

	s := tag.Encode()
	assert.NoError(t, tag.Decode(s))

	assert.Equal(t, "alive", ALIVE.String())
	assert.Equal(t, "suspected", SUSPECTED.String())
	assert.Equal(t, "dead", DEAD.String())
	assert.Equal(t, "left", LEFT.String())
	assert.Equal(t, "undefined", SWIMState(255).String())
}
