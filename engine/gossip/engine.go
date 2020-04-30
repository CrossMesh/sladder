package gossip

import (
	"context"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	arbit "github.com/sunmxt/arbiter"
	"github.com/sunmxt/sladder"
	"github.com/sunmxt/sladder/engine/gossip/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoiface"
)

// Transport is to send or receive gossip message.
type Transport interface {
	Send([]string, []byte)
	Receive(context.Context) ([]string, []byte)
}

// engine options

const (
	defaultSuspectTimeout    = time.Minute * 5
	defaultGossipPeriod      = time.Second
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

type gossipPeriod time.Duration

// WithGossipPeriod creates option of gossip period.
func WithGossipPeriod(t time.Duration) sladder.EngineOption { return gossipPeriod(t) }

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
		case gossipPeriod:
			instance.GossipPeriod = time.Duration(v)
		}
	}
	return instance
}

// EngineInstance is live gossip engine instance.
type EngineInstance struct {
	// parameters fields.
	minRegionPeer  uint
	swimTagKey     string
	SuspectTimeout time.Duration
	GossipPeriod   time.Duration
	Region         string
	Fanout         int32

	log       sladder.Logger
	transport Transport

	lock    sync.RWMutex
	arbiter *arbit.Arbiter
	cluster *sladder.Cluster

	withRegion map[string]map[*sladder.Node]struct{}

	// sync fields.
	inSync map[uint64]struct{}

	// failure detector fields
	inPing              map[*sladder.Node]*pingContext  // nodes in ping progress
	roundTrips          map[*sladder.Node]time.Duration // round-trip time trace.
	pingTimeoutEvent    chan *sladder.Node              // ping timeout event.
	pingReqTimeoutEvent chan *sladder.Node              // ping-req timeout event.
	suspectionNodeIndex map[*sladder.Node]*suspection   // suspection indexed by node ptr.
	suspectionQueue     suspectionQueue                 // heap order by by suspection.notAfter.
}

func newInstanceDefault() *EngineInstance {
	return &EngineInstance{
		SuspectTimeout: defaultSuspectTimeout,
		minRegionPeer:  defaultMinimumRegionPeer,
		withRegion:     make(map[string]map[*sladder.Node]struct{}),
		swimTagKey:     defaultSWIMTagKey,
		log:            sladder.DefaultLogger,
		GossipPeriod:   defaultGossipPeriod,
		Fanout:         1,

		inSync: make(map[uint64]struct{}),

		inPing:              make(map[*sladder.Node]*pingContext),
		roundTrips:          make(map[*sladder.Node]time.Duration),
		pingTimeoutEvent:    make(chan *sladder.Node, 20),
		pingReqTimeoutEvent: make(chan *sladder.Node, 20),
	}
}

func (e *EngineInstance) getGossipFanout() int32 {
	fanout := e.Fanout
	if fanout < 1 {
		return 1
	}
	return fanout
}

func (e *EngineInstance) getGossipPeriod() (d time.Duration) {
	d = e.GossipPeriod
	if e.GossipPeriod < 1 {
		d = defaultGossipPeriod
	}
	return
}

func (e *EngineInstance) getPingProxiesCount() int32 {
	return 5
}

func (e *EngineInstance) getMinPingReqTimeoutTimes() uint {
	return 2
}

func (e *EngineInstance) tickGossipPeriodGo(proc func(time.Time)) {
	period := e.getGossipPeriod()
	e.arbiter.TickGo(func(cancel func(), deadline time.Time) {
		proc(deadline)
		if nextPeriod := e.getGossipPeriod(); nextPeriod != period { // period changed.
			cancel()
			e.goClusterSync()
		}
	}, period, 1)
}

func (e *EngineInstance) selectRandomNodes(n int32, excludeSelf bool) []*sladder.Node {
	nodes, cnt := make([]*sladder.Node, 0, n), int32(0)

	// select M from N.
	e.cluster.RangeNodes(func(node *sladder.Node) bool {
		if int32(len(nodes)) < n {
			nodes = append(nodes, node)
		} else if i := rand.Int31n(cnt + 1); i < n {
			nodes[n] = node
		}
		cnt++
		return true
	}, true)

	return nodes
}

func (e *EngineInstance) sendProto(names []string, body protoiface.MessageV1) {
	if len(names) < 1 {
		return
	}

	var (
		msg pb.GossipMessage
		err error
		raw []byte
	)

	ty := reflect.TypeOf(body)
	typeID, valid := pb.GossipMessageTypeID[ty]
	if !valid {
		e.log.Fatal("sendProtobuf() got invalid protobuf message type \"" + ty.Name() + "\"")
		return
	}
	msg.Type = typeID
	if msg.Body, err = ptypes.MarshalAny(body); err != nil {
		e.log.Fatal("failed to marshal message body, got: " + err.Error())
		return
	}
	if raw, err = proto.Marshal(&msg); err != nil {
		e.log.Fatal("failed to marshal gossip message, got: " + err.Error())
		return
	}
	e.transport.Send(names, raw)
}

func (e *EngineInstance) dispatchGossipMessage() {
	var msg pb.GossipMessage

	for e.arbiter.ShouldRun() {
		from, raw := e.transport.Receive(e.arbiter.Context())
		if len(from) < 1 || raw == nil {
			continue
		}
		if err := proto.Unmarshal(raw, &msg); err != nil {
			e.log.Warn("invalid gossip message received, decoder got " + err.Error())
			continue
		}
		switch msg.Type {
		case pb.GossipMessage_Ack, pb.GossipMessage_Ping, pb.GossipMessage_PingReq:
			e.processFailureDetectionProto(from, &msg)

		case pb.GossipMessage_Sync:
			e.processSyncGossipProto(from, &msg)
		}
	}

}

func (e *EngineInstance) onClusterEvent(ctx *sladder.ClusterEventContext, event sladder.Event, node *sladder.Node) {
	switch event {
	case sladder.EmptyNodeJoined:
		if e.cluster.Self() == node {
			e.onSelfSWIMTagMissing(node)
		}
	}
}

func (e *EngineInstance) insertToRegion(region string, node *sladder.Node) {
	nodeSet, exists := e.withRegion[region]
	if !exists {
		nodeSet = make(map[*sladder.Node]struct{})
		e.withRegion[region] = nodeSet
	}
	nodeSet[node] = struct{}{}
}

func (e *EngineInstance) removeFromRegion(region string, node *sladder.Node, lowerLimit int) bool {
	nodeSet, exist := e.withRegion[region]
	if !exist {
		return true
	}
	if lowerLimit > 0 && lowerLimit >= len(nodeSet) {
		_, exist := nodeSet[node]
		return !exist
	}

	delete(nodeSet, node)
	if len(nodeSet) < 1 {
		delete(e.withRegion, region)
	}
	return true
}

func (e *EngineInstance) updateRegion(old, new string, node *sladder.Node) {
	if old == new {
		return
	}
	e.removeFromRegion(old, node, -1)
	e.insertToRegion(new, node)
}

func (e *EngineInstance) insertRegion(node *sladder.Node, tag *SWIMTag) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.insertToRegion(tag.Region, node)
}

func (e *EngineInstance) updateRegionFromTag(node *sladder.Node, old, new *SWIMTag) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.updateRegion(old.Region, new.Region, node)
}

func (e *EngineInstance) removeRegion(node *sladder.Node, tag *SWIMTag) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.removeFromRegion(tag.Region, node, -1)
}

func (e *EngineInstance) onSWIMTagUpdated(ctx *sladder.WatchEventContext, meta sladder.KeyValueEventMetadata) {
	switch meta.Event() {
	case sladder.KeyInsert:
		meta, tag := meta.(sladder.KeyInsertEventMetadata), &SWIMTag{}
		if err := tag.Decode(meta.Key()); err != nil {
			e.log.Fatal("cannot decode inserted swim tag, got " + err.Error())
			break
		}
		e.insertRegion(meta.Node(), tag)

	case sladder.ValueChanged:
		old, new := &SWIMTag{}, &SWIMTag{}
		meta := meta.(sladder.KeyChangeEventMetadata)
		if err := old.Decode(meta.Old()); err != nil {
			e.log.Fatal("cannot decode old swim tag, got " + err.Error())
			break
		}
		if err := new.Decode(meta.New()); err != nil {
			e.log.Fatal("cannot decode new swim tag, got " + err.Error())
			break
		}
		if meta.Node() == e.cluster.Self() {
			e.onSelfSWIMStateChanged(meta.Node(), old, new)
		}
		e.updateSuspections(meta.Node(), old, new)
		e.updateRegionFromTag(meta.Node(), old, new)

	case sladder.KeyDelete:
		meta, tag := meta.(sladder.KeyInsertEventMetadata), &SWIMTag{}
		if err := tag.Decode(meta.Key()); err != nil {
			e.log.Fatal("cannot decode deleted swim tag, got " + err.Error())
			break
		}
		if meta.Node() == e.cluster.Self() {
			e.onSelfSWIMTagMissing(meta.Node())
		}
		e.removeRegion(meta.Node(), tag)
	}
}

// SWIMTagValidator creates new SWIM tag validator.
func (e *EngineInstance) SWIMTagValidator() *SWIMTagValidator { return &SWIMTagValidator{engine: e} }

// Init attaches to cluster.
func (e *EngineInstance) Init(c *sladder.Cluster) (err error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.arbiter != nil {
		return nil
	}

	// register SWIM tag.
	if err = c.RegisterKey(e.swimTagKey, &SWIMTagValidator{}, true, 0); err != nil {
		return err
	}

	// watch event to sync SWIM states.
	c.Watch(e.onClusterEvent)
	c.Keys(e.swimTagKey).Watch(e.onSWIMTagUpdated)

	e.arbiter = arbit.New()
	e.cluster = c

	e.goClusterSync()   // start cluster sync process.
	e.goDetectFailure() // start failure detection process.

	return nil
}

// Close shutdown gossip engine instance.
func (e *EngineInstance) Close() error {
	return nil
}
