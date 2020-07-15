package gossip

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/sunmxt/sladder"
)

func TestKVWrapper(t *testing.T) {
	t.Run("version_kv", func(t *testing.T) {
		{
			m := &sladder.MockKVValidator{}
			m.On("Sync", (*sladder.KeyValue)(nil), (*sladder.KeyValue)(nil)).Return(false, nil)
			m.On("Sync", mock.Anything, (*sladder.KeyValue)(nil)).Return(true, nil)
			m.On("Sync", mock.Anything, mock.Anything).Return(true, nil)

			validator := WrapVersionKVValidator(m, nil)
			{
				changed, err := validator.Sync(nil, nil)
				assert.NoError(t, err)
				assert.False(t, changed)
			}
			{
				changed, err := validator.Sync(&sladder.KeyValue{}, nil)
				assert.NoError(t, err)
				assert.True(t, changed)
			}
			{
				changed, err := validator.Sync(&sladder.KeyValue{Value: "["}, &sladder.KeyValue{})
				assert.Error(t, err)
				assert.False(t, changed)
			}
			{
				changed, err := validator.Sync(&sladder.KeyValue{Value: "["}, nil)
				assert.Error(t, err)
				assert.False(t, changed)
			}
			{
				changed, err := validator.Sync(&sladder.KeyValue{Value: "{}"}, &sladder.KeyValue{Value: "]"})
				assert.Error(t, err)
				assert.False(t, changed)
			}

		}
		{
			m := &sladder.MockKVValidator{}
			validator := WrapVersionKVValidator(m, nil)

			// normal
			m.On("Sync", mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "" && v.Key == "d"
			}), mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "sss"
			})).Run(func(args mock.Arguments) {
				l, r := args.Get(0).(*sladder.KeyValue), args.Get(1).(*sladder.KeyValue)
				l.Value = r.Value
			}).Return(true, nil)
			{
				o, n := &sladder.KeyValue{Key: "d", Value: ""}, &sladder.KeyValue{Key: "d", Value: "{\"v\": 1, \"o\": \"sss\"}"}
				changed, err := validator.Sync(o, n)
				assert.NoError(t, err)
				assert.True(t, changed)
				assert.Equal(t, "d", o.Key)
				wrap := &wrapVersionKV{}
				assert.NoError(t, wrap.Decode(o.Value))
				assert.Equal(t, "sss", wrap.Value)
				assert.Equal(t, uint32(1), wrap.Version)
			}
			m.On("Sync", mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "" && v.Key == "k"
			}), mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "bbb" && v.Key == "k"
			})).Run(func(args mock.Arguments) {
				l, r := args.Get(0).(*sladder.KeyValue), args.Get(1).(*sladder.KeyValue)
				l.Value = r.Value
			}).Return(false, nil)
			{
				o, n := &sladder.KeyValue{Key: "k", Value: ""}, &sladder.KeyValue{Key: "k", Value: "{\"v\": 1, \"o\": \"bbb\"}"}
				changed, err := validator.Sync(o, n)
				assert.NoError(t, err)
				assert.True(t, changed)
				assert.Equal(t, "k", o.Key)
				wrap := &wrapVersionKV{}
				assert.NoError(t, wrap.Decode(o.Value))
				assert.Equal(t, "bbb", wrap.Value)
				assert.Equal(t, uint32(1), wrap.Version)
			}
			m.On("Sync", mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "" && v.Key == "k2"
			}), mock.MatchedBy(func(v *sladder.KeyValue) bool {
				return v.Value == "bbb" && v.Key == "k2"
			})).Run(func(args mock.Arguments) {
				l, r := args.Get(0).(*sladder.KeyValue), args.Get(1).(*sladder.KeyValue)
				l.Value = r.Value
			}).Return(false, errors.New("test1"))
			{
				o, n := &sladder.KeyValue{Key: "k2", Value: ""}, &sladder.KeyValue{Key: "k2", Value: "{\"v\": 1, \"o\": \"bbb\"}"}
				changed, err := validator.Sync(o, n)
				assert.Error(t, err)
				assert.False(t, changed)
				assert.Equal(t, "k2", o.Key)
				assert.Equal(t, "", o.Value)
			}
		}
		// validate
		{
			m := &sladder.MockKVValidator{}
			validator := WrapVersionKVValidator(m, nil)
			{
				o := &sladder.KeyValue{Key: "k", Value: "{"}
				assert.False(t, validator.Validate(*o))
			}

			m.On("Validate", mock.MatchedBy(func(v sladder.KeyValue) bool {
				return v.Key == "k1"
			})).Return(true)
			{
				o := &sladder.KeyValue{Key: "k1", Value: ""}
				assert.True(t, validator.Validate(*o))
			}

			m.On("Validate", mock.MatchedBy(func(v sladder.KeyValue) bool {
				return v.Key == "k2"
			})).Return(false)
			{
				o := &sladder.KeyValue{Key: "k2", Value: ""}
				assert.False(t, validator.Validate(*o))
			}
		}
	})
}
