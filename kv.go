package sladder

import (
	"sync"
)

// KVCoordinator guards consistency of KeyValue.
type KVCoordinator interface {

	// Sync validates given KeyValue and update local key-value entry with it.
	Sync(*KeyValueEntry, *KeyValue) (bool, error)

	// Validate validates given KeyValue
	Validate(KeyValue) bool

	// Txn begins an transaction.
	Txn() interface{}
}

type KeyValueEntry struct {
	KeyValue

	coordinator KVCoordinator
	lock        sync.RWMutex
}

// KeyValue stores one metadata key of the node.
type KeyValue struct {
	Key   string
	Value string
}

//func (v *KeyValueEntry) Serialize(buf []byte) (raw []byte, err error) {
//	v.lock.RLock()
//	defer v.lock.RUnlock()
//
//	msg := &proto.KeyValue{
//		Key:   v.Key,
//		Value: v.Value,
//	}
//	buffer := pb.NewBuffer(buf)
//	if err = buffer.Marshal(msg); err != nil {
//		return nil, err
//	}
//	return buffer.Bytes(), nil
//}
//
//func (v *KeyValueEntry) Deserizlize(buf []byte) (err error) {
//	if buf == nil {
//		return nil
//	}
//	v.lock.Lock()
//	defer v.lock.Unlock()
//
//	msg, new := &proto.KeyValue{}, &KeyValue{}
//	if err = pb.Unmarshal(buf, msg); err != nil {
//		return err
//	}
//	if new.Extra, err = v.commiter.DeserizlizeExtra(msg.Extras); err != nil {
//		return err
//	}
//	new.Key, new.Value = msg.Key, msg.Value
//	if accepted, err := v.commiter.IsAccepted(new, &v.KeyValue); !accepted {
//		return err
//	}
//	// submit
//	v.Key, v.Value, v.Extra = new.Key, new.Value, new.Extra
//	return nil
//}
