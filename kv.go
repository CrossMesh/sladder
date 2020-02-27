package sladder

import (
	"sync"

	pb "github.com/golang/protobuf/proto"
	"github.com/sunmxt/sladder/proto"
)

// Committer guards KeyValue.
type Committer interface {
	IsAccepted(new, old *KeyValue) (bool, error)

	SerializeExtra(interface{}) ([]byte, error)
	DeserizlizeExtra([]byte) (interface{}, error)
}

type keyValue struct {
	KeyValue

	commiter Committer
	lock     sync.RWMutex
}

// KeyValue stores one metadata key of the node.
type KeyValue struct {
	Key   string
	Value string
	Extra interface{}
}

func (v *keyValue) Serialize(buf []byte) (raw []byte, err error) {
	v.lock.RLock()
	defer v.lock.RUnlock()

	msg := &proto.KeyValue{
		Key:   v.Key,
		Value: v.Value,
	}
	if msg.Extras, err = v.commiter.SerializeExtra(v.Extra); err != nil {
		return nil, err
	}
	buffer := pb.NewBuffer(buf)
	if err = buffer.Marshal(msg); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (v *keyValue) Deserizlize(buf []byte) (err error) {
	if buf == nil {
		return nil
	}
	v.lock.Lock()
	defer v.lock.Unlock()

	msg, new := &proto.KeyValue{}, &KeyValue{}
	if err = pb.Unmarshal(buf, msg); err != nil {
		return err
	}
	if new.Extra, err = v.commiter.DeserizlizeExtra(msg.Extras); err != nil {
		return err
	}
	new.Key, new.Value = msg.Key, msg.Value
	if accepted, err := v.commiter.IsAccepted(new, &v.KeyValue); !accepted {
		return err
	}
	// submit
	v.Key, v.Value, v.Extra = new.Key, new.Value, new.Extra
	return nil
}
