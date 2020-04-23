package gossip

import (
	"sync"
	"time"

	"github.com/sunmxt/sladder"
)

// Transport is to send or receive gossip message.
type Transport interface {
	Send([]string, []byte)
	Receive() []byte
}

// engine options

const (
	defaultSuspectTimeout    = time.Minute * 5
	defaultMinimumRegionPeer = 2
	defaultSWIMTagKey        = "_swim_tag"
)

type minRegionPeer uint

// WithMinRegionPeer creates option to limit minimum region peer.
func WithMinRegionPeer(min uint) sladder.EngineOption {
	if min < 1 {
		min = 1
	}
	return min
}

type region string

// WithRegion creates option of gossip region.
func WithRegion(name string) sladder.EngineOption { return region(name) }

type suspectTimeout time.Duration

// WithSuspectTimeout creates option of suspection timeout.
func WithSuspectTimeout(t time.Duration) sladder.EngineOption { return suspectTimeout(t) }

type swimTagKey string

// WithSWIMTagKey creates option of SWIM tag key.
func WithSWIMTagKey(key string) sladder.EngineOption { return swimTagKey(key) }

type logger sladder.Logger

// WithLogger creates option of engine logger.
func WithLogger(log sladder.Logger) sladder.EngineOption { return logger(log) }

type fanout int32

// WithFanout creates option of gossip fanout.
func WithFanout(n int32) sladder.EngineOption { return fanout(n) }

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
		case logger:
			if v != nil {
				instance.log = sladder.Logger(v)
			}
		case fanout:
			fanout := int32(v)
			if fanout < 1 {
				fanout = 1
			}
			instance.Fanout = fanout
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
	Fanout         int32

	lock       sync.Mutex
	stop       chan struct{}
	cluster    *sladder.Cluster
	withRegion map[string]map[*sladder.Node]struct{}

	transport Transport

	log sladder.Logger
}

func newInstanceDefault() *EngineInstance {
	return &EngineInstance{
		SuspectTimeout: defaultSuspectTimeout,
		minRegionPeer:  defaultMinimumRegionPeer,
		term:           0,
		withRegion:     make(map[string]map[*sladder.Node]struct{}),
		swimTagKey:     defaultSWIMTagKey,
		log:            sladder.DefaultLogger,
		Fanout:         1,
	}
}

func (e *EngineInstance) getGossipFanout() int32 {
	fanout := e.Fanout
	if fanout < 1 {
		return 1
	}
	return fanout
}

func (e *EngineInstance) onClusterEvent(ctx *sladder.ClusterEventContext, event sladder.Event, node *sladder.Node) {
	switch event {
	case sladder.EmptyNodeJoined:
		if e.cluster.Self() == node {
			e.onSelfSWIMTagMissing(node)
		}
	}
}

func (e *EngineInstance) onSelfSWIMStateUpdated(ctx *sladder.WatchEventContext, meta sladder.KeyValueEventMetadata) {
	switch meta.Event() {
	case sladder.ValueChanged:
		meta := meta.(sladder.KeyChangeEventMetadata)
		e.onSelfSWIMStateChanged(meta.Node(), meta.Old(), meta.New())

	case sladder.KeyDelete:
		e.onSelfSWIMTagMissing(meta.Node())
	}
}

// SWIMTagValidator creates new SWIM tag validator.
func (e *EngineInstance) SWIMTagValidator() *SWIMTagValidator { return &SWIMTagValidator{engine: e} }

// Init attaches to cluster.
func (e *EngineInstance) Init(c *sladder.Cluster) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.stop != nil {
		return nil
	}

	// register SWIM tag.
	if err = c.RegisterKey(e.swimTagKey, &SWIMTagValidator{}, true, 0); err != nil {
		return err
	}

	// watch event to sync SWIM states.
	c.Watch(e.onClusterEvent)
	c.Keys(e.swimTagKey).Nodes(c.Self()).Watch(e.onSelfSWIMStateUpdated)

	e.stop = make(chan struct{})
	e.cluster = c

	return nil
}

// Close shutdown gossip engine instance.
func (e *EngineInstance) Close() error {
	return nil
}
