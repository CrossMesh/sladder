package gossip

import (
	"encoding/json"
	"time"

	"github.com/sunmxt/sladder"
)

// Transport is to send or receive gossip message.
type Transport interface {
	Send([]string, []byte)
	Receive() []byte
}

// engine options

type minRegionPeer uint
type region string
type swimTagKey string
type suspectTimeout time.Duration

const (
	defaultSuspectTimeout    = time.Minute * 5
	defaultMinimumRegionPeer = 2
	defaultSWIMTagKey        = "_swim_tag"
)

// WithMinRegionPeer creates option to limit minimum region peer.
func WithMinRegionPeer(min uint) sladder.EngineOption {
	if min < 1 {
		min = 1
	}
	return min
}

// WithRegion creates option of gossip region.
func WithRegion(name string) sladder.EngineOption { return region(name) }

// WithSuspectTimeout creates option of suspection timeout.
func WithSuspectTimeout(t time.Duration) sladder.EngineOption { return suspectTimeout(t) }

// WithSWIMTagKey creates option of SWIM tag key.
func WithSWIMTagKey(key string) sladder.EngineOption { return swimTagKey(key) }

// Engine provides methods to create gossip engine instance.
type Engine struct{}

// New create gossip engine instance.
func (e Engine) New(transport Transport, options ...sladder.EngineOption) sladder.EngineInstance {
	if transport == nil {
		panic("transport is nil")
	}
	instance := newInstanceDefault()
	for _, option := range options {
		switch v := option.(type) {
		case minRegionPeer:
			instance.minRegionPeer = uint(v)
		case region:
			instance.Region = string(v)
		case suspectTimeout:
			instance.SuspectTimeout = time.Duration(v)
		case swimTagKey:
			instance.swimTagKey = string(v)
		}
	}
	return instance
}

// EngineInstance is live gossip engine instance.
type EngineInstance struct {
	term uint32

	minRegionPeer  uint
	swimTagKey     string
	SuspectTimeout time.Duration
	Region         string

	withRegion map[string]map[*sladder.Node]struct{}

	transport Transport
}

func newInstanceDefault() *EngineInstance {
	return &EngineInstance{
		SuspectTimeout: defaultSuspectTimeout,
		minRegionPeer:  defaultMinimumRegionPeer,
		term:           0,
		withRegion:     make(map[string]map[*sladder.Node]struct{}),
		swimTagKey:     defaultSWIMTagKey,
	}
}

func (e *EngineInstance) onClusterEvent(ctx *sladder.ClusterEventContext, event sladder.Event, node *sladder.Node) {
	switch event {
	case sladder.EmptyNodeJoined:
		node.Set(e.swimTagKey, "") // insert SWIM tag.
	}
}

// Init attaches to cluster.
func (e *EngineInstance) Init(c *sladder.Cluster) (err error) {
	// register SWIM tag.
	if err = c.RegisterKey(e.swimTagKey, &SWIMTagValidator{}, true); err != nil {
		return err
	}

	// watch event to insert tags.
	c.Watch(e.onClusterEvent)

	return nil
}

// Close shutdown gossip engine instance.
func (e *EngineInstance) Close() error {
	return nil
}

const (
	// ALIVE State.
	ALIVE = SWIMState(0)
	// SUSPECTED State.
	SUSPECTED = SWIMState(1)
	// DEAD State.
	DEAD = SWIMState(2)
)

// SWIMState stores state of gossip node.
type SWIMState uint8

// SWIMStateNames contains printable name of SWIMState
var SWIMStateNames = map[SWIMState]string{
	ALIVE:     "alive",
	SUSPECTED: "suspected",
	DEAD:      "dead",
}

func (s SWIMState) String() string {
	name, exist := SWIMStateNames[s]
	if !exist {
		return "undefined"
	}
	return name
}

// SWIMTags represents node gossip tag.
type SWIMTags struct {
	Version uint32    `json:"v"`
	State   SWIMState `json:"s"`
}

// SWIMTagValidator validates SWIMTags.
type SWIMTagValidator struct{}

// Sync synchronizes SWIMTag
func (c *SWIMTagValidator) Sync(entry *sladder.KeyValueEntry, new *sladder.KeyValue) (bool, error) {
	return false, nil
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

func (c *SWIMTagValidator) Txn() interface{} {
	return nil
}
