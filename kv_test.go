package sladder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyValue(t *testing.T) {
	kv := &KeyValue{
		Key:   "k",
		Value: "v",
	}
	c := kv.Clone()
	assert.Equal(t, kv.Key, c.Key)
	assert.Equal(t, kv.Value, c.Value)
}
