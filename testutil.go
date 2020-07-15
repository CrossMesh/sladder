package sladder

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"

	"github.com/sunmxt/sladder/util"
)

type TestRandomNameResolver struct {
	NumOfNames int
}

func (r *TestRandomNameResolver) Resolve(...*KeyValue) (ns []string, err error) {
	for n := 0; n < r.NumOfNames; n++ {
		ns = append(ns, fmt.Sprintf("%x", rand.Uint64()))
	}
	return ns, nil
}

func (r *TestRandomNameResolver) Keys() []string {
	return nil
}

type TestNamesInKeyNameResolver struct {
	Key string
}

type TestNamesInKeyTag struct {
	Names   []string `json:"ns"`
	Version uint32   `json:"v"`
}

func (r *TestNamesInKeyNameResolver) Keys() []string {
	return []string{r.Key}
}

func (r *TestNamesInKeyNameResolver) Resolve(kvs ...*KeyValue) (ids []string, err error) {
	var nameStruct TestNamesInKeyTag

	for _, kv := range kvs {
		if kv.Key == r.Key {
			if err = json.Unmarshal([]byte(kv.Value), &nameStruct); err != nil {
				return nil, err
			}
			ids = nameStruct.Names
			break
		}
	}

	return
}

type TestNamesInKeyTxn struct {
	tag       TestNamesInKeyTag
	changed   bool
	origin    string
	originVer uint32
}

func (t *TestNamesInKeyTxn) AddName(names ...string) {
	if len(names) < 1 {
		return
	}
	sort.Strings(names)
	t.tag.Names, t.changed, t.tag.Version = util.MergeStringSortedSet(t.tag.Names, names), true, t.originVer+1
}

func (t *TestNamesInKeyTxn) RemoveName(names ...string) {
	if len(names) < 1 {
		return
	}
	t.tag.Names, t.changed, t.tag.Version = util.RemoveStringSortedSet(t.tag.Names, names...), true, t.originVer+1
}

func (t *TestNamesInKeyTxn) After() (changed bool, new string) {
	if !t.changed {
		return false, t.origin
	}
	if nv, err := json.Marshal(t.tag); err != nil {
		panic(err)
	} else {
		new = string(nv)
	}
	return true, new
}

func (t *TestNamesInKeyTxn) Before() string { return t.origin }

type TestNamesInKeyIDValidator struct{}

func (v *TestNamesInKeyIDValidator) Sync(entry *KeyValue, remote *KeyValue) (accepted bool, err error) {
	if remote == nil {
		return false, nil
	}

	var remoteName, localName TestNamesInKeyTag

	if err = json.Unmarshal([]byte(remote.Value), &remoteName); err != nil {
		return false, nil
	}
	if entry.Value == "" {
		entry.Value = "{}"
	}
	if err = json.Unmarshal([]byte(entry.Value), &localName); err != nil {
		return false, err
	}
	if remoteName.Version <= localName.Version {
		return false, nil
	}
	entry.Value = remote.Value
	return true, nil
}

func (v *TestNamesInKeyIDValidator) Validate(kv KeyValue) bool {
	var names TestNamesInKeyTag

	if err := json.Unmarshal([]byte(kv.Value), &names); err != nil {
		return false
	}
	return true
}

func (v *TestNamesInKeyIDValidator) Txn(kv KeyValue) (KVTransaction, error) {
	t := &TestNamesInKeyTxn{}
	if kv.Value == "" {
		kv.Value = "{}"
	}
	if err := json.Unmarshal([]byte(kv.Value), &t.tag); err != nil {
		return nil, err
	}
	t.origin, t.changed = kv.Value, false
	sort.Strings(t.tag.Names)
	return t, nil
}
