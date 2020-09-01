package gossip

import (
	"encoding/json"
	"sort"

	"github.com/crossmesh/sladder"
	"github.com/crossmesh/sladder/util"
)

const (
	// ALIVE State.
	ALIVE = SWIMState(0)
	// SUSPECTED State.
	SUSPECTED = SWIMState(1)
	// DEAD State.
	DEAD = SWIMState(2)
	// LEFT State.
	LEFT = SWIMState(3)
)

// SWIMState stores state of gossip node.
type SWIMState uint8

// SWIMStateNames contains printable name of SWIMState
var SWIMStateNames = map[SWIMState]string{
	ALIVE:     "alive",
	SUSPECTED: "suspected",
	DEAD:      "dead",
	LEFT:      "left",
}

func (s SWIMState) String() string {
	name, exist := SWIMStateNames[s]
	if !exist {
		return "undefined"
	}
	return name
}

// SWIMTag represents node gossip tag.
type SWIMTag struct {
	Version   uint32    `json:"v,omitempty"`
	State     SWIMState `json:"s,omitempty"`
	Region    string    `json:"r,omitempty"`
	EntryList []string  `json:"l,omitempty"`
}

// Encode serializes SWIMTags.
func (t *SWIMTag) Encode() string {
	raw, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// Decode deserializes SWIMTags.
func (t *SWIMTag) Decode(v string) error {
	if v == "" {
		v = "{}"
	}
	if err := json.Unmarshal([]byte(v), t); err != nil {
		return err
	}
	sort.Strings(t.EntryList)
	return nil
}

// SWIMTagValidator validates SWIMTags.
type SWIMTagValidator struct {
	engine *EngineInstance
}

// Sync synchronizes SWIMTag
func (c *SWIMTagValidator) Sync(entry *sladder.KeyValue, remote *sladder.KeyValue) (bool, error) {
	if remote == nil {
		return false, nil
	}

	remoteTag, localTag := &SWIMTag{}, &SWIMTag{}
	if err := remoteTag.Decode(remote.Value); err != nil { // reject invalid tag.
		c.engine.log.Warn("reject a invalid remote SWIM tag")
		return false, nil
	}

	if err := localTag.Decode(entry.Value); err != nil {
		// invalid local tag. drop it and replace with the remote.
		c.engine.log.Warn("drop invalid local SWIM tag")
		entry.Value = remote.Value
		return true, nil
	}

	// SWIM rule 1: accept newer tag version.
	if remoteTag.Version > localTag.Version {
		entry.Value = remote.Value
		return true, nil
	}
	// SWIM rule 2: reject all older version.
	if remoteTag.Version < localTag.Version {
		return false, nil
	}
	changed := false
	switch {
	case remoteTag.State == SUSPECTED:
		// SWIM rule 3: suspection can be raised by any cluster member, overwriting ALIVE.
		if localTag.State == ALIVE {
			localTag.State, changed = SUSPECTED, true
		}

	case remoteTag.State == DEAD:
		// SWIM rule 4: dead claim overwrites ALIVE, SUSPECTED.
		if localTag.State != LEFT {
			localTag.State, changed = DEAD, true
		}

	case remoteTag.State == LEFT:
		// extended SWIM Rule: LEFT overwrites any.
		localTag.State, changed = LEFT, true
	}

	if changed {
		// update value.
		entry.Value = localTag.Encode()
	}

	return changed, nil
}

// Validate checks whether raw SWIMTags.
func (c *SWIMTagValidator) Validate(kv sladder.KeyValue) bool {
	if kv.Value == "" {
		return true
	}
	tag := &SWIMTagValidator{}
	if err := json.Unmarshal([]byte(kv.Value), tag); err != nil {
		return false
	}
	return true
}

// Txn begins an transaction.
func (c *SWIMTagValidator) Txn(x sladder.KeyValue) (sladder.KVTransaction, error) {
	txn := &SWIMTagTxn{changed: false, oldRaw: x.Value}
	if err := txn.tag.Decode(x.Value); err != nil {
		return nil, err
	}
	txn.OldVersion = txn.tag.Version
	return txn, nil
}

// SWIMTagTxn implements SWIM tag transaction.
type SWIMTagTxn struct {
	tag        SWIMTag
	oldRaw     string
	changed    bool
	OldVersion uint32
}

// After returns modified value.
func (t *SWIMTagTxn) After() string { return t.tag.Encode() }

// Updated checks whether tag is updated.
func (t *SWIMTagTxn) Updated() bool { return t.changed }

// Before return old value.
func (t *SWIMTagTxn) Before() string { return t.oldRaw }

// SetRawValue apply raw value to Txn.
func (t *SWIMTagTxn) SetRawValue(x string) error {
	newTag := SWIMTag{}
	if err := newTag.Decode(x); err != nil {
		return err
	}
	t.tag = newTag
	t.changed = x != t.oldRaw
	return nil
}

// Region returns region of tag snapshot
func (t *SWIMTagTxn) Region() string { return t.tag.Region }

// State returns current SWIM state.
func (t *SWIMTagTxn) State() SWIMState { return t.tag.State }

// Version returns current SWIM tag version.
func (t *SWIMTagTxn) Version() uint32 { return t.tag.Version }

// AddToEntryList add valid entries to entry list.
func (t *SWIMTagTxn) AddToEntryList(keys ...string) {
	if len(keys) < 0 {
		return
	}
	if new := util.AddStringSortedSet(t.tag.EntryList, keys...); len(new) != len(t.tag.EntryList) {
		t.BumpVersion()
		t.tag.EntryList = new
	}
}

// RemoveFromEntryList add valid entries to entry list.
func (t *SWIMTagTxn) RemoveFromEntryList(keys ...string) {
	if len(keys) < 0 {
		return
	}
	sort.Strings(t.tag.EntryList)
	if new := util.RemoveStringSortedSet(t.tag.EntryList, keys...); len(new) != len(t.tag.EntryList) {
		t.BumpVersion()
		t.tag.EntryList = new
	}
}

// EntryList returns valid entry key.
func (t *SWIMTagTxn) EntryList(clone bool) (l []string) {
	if !clone {
		return t.tag.EntryList
	}
	l = append(l, t.tag.EntryList...)
	return
}

// BumpVersion forces to advance tag version.
func (t *SWIMTagTxn) BumpVersion() uint32 {
	if t.tag.Version <= t.OldVersion {
		t.tag.Version++
		t.changed = true
	}
	return t.tag.Version
}

// ClaimDead set SWIM state to dead.
func (t *SWIMTagTxn) ClaimDead() bool {
	if t.tag.State == LEFT {
		// extended SWIM Rule: LEFT overwrites any.
		return false
	}
	if t.tag.State != DEAD {
		// SWIM Rule: DEAD overwrites SUSPECTED and ALIVE.
		t.changed = true
		t.tag.State = DEAD
		return true
	}
	return false
}

// ClaimSuspected set SWIM state to SUSPECTED.
func (t *SWIMTagTxn) ClaimSuspected() bool {
	if t.tag.State == DEAD {
		// SWIM Rule: DEAD overwrites SUSPECTED.
		return false
	}
	if t.tag.State == LEFT {
		// extended SWIM Rule: LEFT overwrites any.
		return false
	}
	if t.tag.State == SUSPECTED {
		return false
	}
	t.tag.State, t.changed = SUSPECTED, true
	return true
}

// ClaimAlive clears false positive and ensure SWIM state is ALIVE.
func (t *SWIMTagTxn) ClaimAlive() bool {
	if t.tag.State != ALIVE {
		// clear false positive by increasing version.
		t.BumpVersion()
		t.tag.State, t.changed = ALIVE, true
		return true
	}
	return false
}

// Leave set states to LEFT.
func (t *SWIMTagTxn) Leave() bool {
	if t.tag.State == LEFT {
		return true
	}
	t.tag.State, t.changed = LEFT, true
	return true
}

// SetRegion updates region.
func (t *SWIMTagTxn) SetRegion(region string) string {
	old := t.tag.Region
	if old != region {
		t.tag.Region, t.changed = region, true
		t.BumpVersion()
	}
	return old
}

// ensureTransactionCommitIntegrity adds some missing necessary operations according to user operation logs.
func (e *EngineInstance) ensureTransactionCommitIntegrity(t *sladder.Transaction, isEngineTxn bool, rcs []*sladder.TransactionOperation) (accepted bool, err error) {

	addList, removeList := []string{}, []string{}
	nodeOps := []*sladder.TransactionOperation{}

	self := e.cluster.Self()
	// ensure that SWIM tag exists.
	if !t.KeyExists(self, e.swimTagKey) {
		rtx, err := t.KV(self, e.swimTagKey)
		if err != nil {
			e.log.Fatalf("cannot get kv transaction when recovering from missing SWIM tag. (err = %v) ", err.Error())
			return false, err
		}
		tag := rtx.(*SWIMTagTxn)
		tag.SetRegion(e.Region)
		tag.ClaimAlive()
		tag.BumpVersion()
	}

	for idx := 0; idx < len(rcs); idx++ {
		rc := rcs[idx]
		if rc.Txn == nil { // node operation
			nodeOps = append(nodeOps, rc)
			continue
		}

		if self == rc.Node && rc.Key != e.swimTagKey {
			switch {
			case !rc.PastExists && rc.Exists:
				addList = append(addList, rc.Key)
			case rc.PastExists && !rc.Exists:
				removeList = append(removeList, rc.Key)
			}
		}
	}

	if len(addList)+len(removeList) > 0 { // sync existing keys to entry list.
		rtx, err := t.KV(self, e.swimTagKey)
		if err != nil {
			e.log.Fatalf("engine cannot update entry list in swim tag. err = \"%v\"", err)
			return false, err
		}
		swim := rtx.(*SWIMTagTxn)
		if len(addList) > 0 {
			sort.Strings(addList)
			swim.AddToEntryList(addList...)
		}
		if len(removeList) > 0 {
			sort.Strings(removeList)
			swim.RemoveFromEntryList(removeList...)
		}
	}

	return true, nil
}

func (e *EngineInstance) onSelfSWIMStateChanged(self *sladder.Node, old, new *SWIMTag) {
	if new.State != ALIVE && e.quitAfter == 0 {
		// clear false postive.
		if err := e.cluster.Txn(func(t *sladder.Transaction) bool {
			rtx, err := t.KV(self, e.swimTagKey)
			if err != nil {
				e.log.Fatal("cannot get kv transaction when clearing false positives, got " + err.Error())
				return false
			}
			return rtx.(*SWIMTagTxn).ClaimAlive()
		}); err != nil {
			e.log.Fatal("cannot commit transaction when clearing false positives, got " + err.Error())
		}
	}
}
