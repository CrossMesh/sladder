package gossip

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crossmesh/sladder"
	"github.com/crossmesh/sladder/engine/gossip/pb"
	spb "github.com/crossmesh/sladder/proto"
	"github.com/golang/protobuf/ptypes"
	arbit "github.com/sunmxt/arbiter"
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
	defaultMinimumRegionPeer = 1
	defaultSWIMTagKey        = "_swim_tag"
	defaultQuitTimeout       = time.Second * 30
)

type minRegionPeer uint

// WithMinRegionPeer creates option to limit minimum region peer.
func WithMinRegionPeer(min uint) sladder.EngineOption {
	if min < 1 {
		min = 1
	}
	return minRegionPeer(min)
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

type manualSync struct{}

// ManualSync disables auto sync. (for testing)
func ManualSync() sladder.EngineOption { return manualSync{} }

type manualFailureDetect struct{}

// ManualFailureDetect disables auto failure detection. (for testing)
func ManualFailureDetect() sladder.EngineOption { return manualFailureDetect{} }

type manualClearSuspections struct{}

// ManualClearSuspections disables auto suspections clearing. (for testing)
func ManualClearSuspections() sladder.EngineOption { return manualClearSuspections{} }

type quitTimeout time.Duration

// WithQuitTimeout sets timeout for quiting process.
// When QuitTimeout is reached, engine will be forced to shutdown despite incompleted spreading of LEAVE state.
// Specially, 0 means infinite timeout.
func WithQuitTimeout(d time.Duration) sladder.EngineOption { return quitTimeout(d) }

// Engine provides methods to create gossip engine instance.
type Engine struct{}

// New create gossip engine instance.
func New(transport Transport, options ...sladder.EngineOption) sladder.EngineInstance {
	if transport == nil {
		panic("transport is nil")
	}
	instance := newInstanceDefault(transport)
	for _, option := range options {
		switch v := option.(type) {
		case minRegionPeer:
			instance.minRegionPeer = uint(v)
		case region:
			instance.region = string(v)
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
		case manualClearSuspections:
			instance.disableClearSuspections = true
		case manualFailureDetect:
			instance.disableFailureDetect = true
		case manualSync:
			instance.disableSync = true
		case quitTimeout:
			instance.QuitTimeout = time.Duration(v)
		}
	}
	return instance
}

type leavingNode struct {
	names    []string
	snapshot *spb.Node
	tagIdx   int
}

type reversedExistenceItem struct {
	exist bool
	epoch uint64
}

// EngineInstance is live gossip engine instance.
type EngineInstance struct {
	// parameters fields.
	disableSync             bool
	disableFailureDetect    bool
	disableClearSuspections bool
	minRegionPeer           uint
	swimTagKey              string
	SuspectTimeout          time.Duration
	GossipPeriod            time.Duration
	QuitTimeout             time.Duration
	region                  string
	Fanout                  int32

	log       sladder.Logger
	transport Transport

	lock    sync.RWMutex
	arbiter *arbit.Arbiter
	cluster *sladder.Cluster

	withRegion map[string]map[*sladder.Node]struct{}

	messageCounter uint64 // message counter
	counterSeed    uint64 // seed is to randomize message ID. (formula: message ID = seed + counter).
	quitAfter      uint64

	// txn fields.
	innerTxnIDs sync.Map // map[uint32]struct{}

	// sync fields.
	leavingNodes          []*leavingNode
	leaveingNodeNameIndex map[string]int
	reversedExistence     map[*sladder.Node]*reversedExistenceItem // trace self-existence from other nodes.
	canQuit               bool

	// failure detector fields.
	inPing              map[*sladder.Node]*pingContext  // nodes in ping progress
	roundTrips          map[*sladder.Node]time.Duration // round-trip time trace.
	suspectionNodeIndex map[*sladder.Node]*suspection   // suspection indexed by node ptr.
	suspectionQueue     suspectionQueue                 // heap order by suspection.notAfter.
	pingTimeoutEvent    chan *sladder.Node              // ping timeout event.
	pingReqTimeoutEvent chan *sladder.Node              // ping-req timeout event.

	Metrics Metrics
}

func newInstanceDefault(transport Transport) *EngineInstance {
	return &EngineInstance{
		SuspectTimeout: defaultSuspectTimeout,
		minRegionPeer:  defaultMinimumRegionPeer,
		swimTagKey:     defaultSWIMTagKey,
		log:            sladder.DefaultLogger,
		GossipPeriod:   defaultGossipPeriod,
		Fanout:         1,
		QuitTimeout:    defaultQuitTimeout,

		withRegion: make(map[string]map[*sladder.Node]struct{}),

		leaveingNodeNameIndex: make(map[string]int),
		reversedExistence:     make(map[*sladder.Node]*reversedExistenceItem),
		canQuit:               false,

		inPing:              make(map[*sladder.Node]*pingContext),
		roundTrips:          make(map[*sladder.Node]time.Duration),
		suspectionNodeIndex: make(map[*sladder.Node]*suspection),

		pingTimeoutEvent:    make(chan *sladder.Node, 20),
		pingReqTimeoutEvent: make(chan *sladder.Node, 20),
		transport:           transport,
	}
}

// SWIMTagKey returns current SWIM Tag key name.
func (e *EngineInstance) SWIMTagKey() string { return e.swimTagKey }

// Region returns region name to which self belongs.
func (e *EngineInstance) Region() string { return e.region }

// SetRegion sets region to which self belongs.
func (e *EngineInstance) SetRegion(new string) (old string, err error) {
	var errs sladder.Errors

	errs.Trace(e.cluster.Txn(func(t *sladder.Transaction) bool {
		rtx, err := t.KV(e.cluster.Self(), e.swimTagKey)
		if err != nil {
			errs = append(errs, err)
			return false
		}
		tag := rtx.(*SWIMTagTxn)
		if old := tag.Region(); old == new {
			return false
		}
		tag.SetRegion(new)
		return true
	}))

	if err = errs.AsError(); err != nil {
		old = ""
	}
	return
}

// SetMinRegionPeer sets the minimum number of peers in region.
// DEAD peers will be preserved to achieve the minimum number. This helps to recover from network partiation.
func (e *EngineInstance) SetMinRegionPeer(n uint) (old uint) {
	e.lock.Lock()
	old = e.minRegionPeer
	e.minRegionPeer = n
	e.lock.Unlock()

	if old > n {
		e.delayClearDeads(0)
	}

	return
}

func (e *EngineInstance) generateMessageID() uint64 {
	return atomic.AddUint64(&e.messageCounter, 1) + e.counterSeed
}

func (e *EngineInstance) getGossipFanout() int32 {
	fanout := e.Fanout
	if fanout < 1 {
		return 1
	}

	atomic.StoreUint32(&e.Metrics.GossipFanout, uint32(fanout))

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

	atomic.StoreUint64(&e.Metrics.GossipPeriod, uint64(period))

	e.arbiter.TickGo(func(cancel func(), deadline time.Time) {
		proc(deadline)
		if nextPeriod := e.getGossipPeriod(); nextPeriod != period { // period changed.
			cancel()

			go e.tickGossipPeriodGo(proc) // re-sched.
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
			nodes[i] = node
		}
		cnt++
		return true
	}, true, false)

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
		e.log.Error("sendProtobuf() got invalid protobuf message type \"" + ty.Name() + "\"")
		return
	}
	msg.Type = typeID
	if msg.Body, err = ptypes.MarshalAny(body); err != nil {
		e.log.Error("failed to marshal message body, got: " + err.Error())
		return
	}
	if raw, err = proto.Marshal(&msg); err != nil {
		e.log.Error("failed to marshal gossip message, got: " + err.Error())
		return
	}
	e.transport.Send(names, raw)
}

func (e *EngineInstance) dispatchEvents() {
	for {
		select {
		case <-e.arbiter.Exit():
			return

		case node := <-e.pingReqTimeoutEvent:
			e.processPingReqTimeout(node)

		case node := <-e.pingTimeoutEvent:
			e.processPingTimeout(node)
		}
	}
}

func (e *EngineInstance) _removeLeavingNode(index ...int) {
	if len(index) < 1 {
		return
	}
	sort.Ints(index)

	depit := func(idx int) {
		for _, name := range e.leavingNodes[idx].names {
			delete(e.leaveingNodeNameIndex, name)
		}
	}

	pit, flat, lpit := 0, len(e.leavingNodes), len(index)
	prevPit, prevlastPit := -1, -1
	for pit < lpit {
		// search for pit.
		newIdx := index[pit]
		if newIdx < 0 || prevPit == newIdx {
			pit++
			continue
		}

		lastPit := index[lpit-1]
		if lastPit < 0 || lastPit == prevlastPit { // de-dup
			lpit--
			continue
		}
		prevlastPit = flat - 1
		if lastPit == flat-1 { // ptr to pit.
			depit(flat - 1)
			lpit--
			flat--
			continue
		}

		prevPit = newIdx

		depit(newIdx)

		e.leavingNodes[newIdx] = e.leavingNodes[flat-1]
		for _, name := range e.leavingNodes[newIdx].names {
			e.leaveingNodeNameIndex[name] = newIdx
		}

		pit++
		flat--
	}
	e.leavingNodes = e.leavingNodes[:flat]

}

func (e *EngineInstance) dispatchGossipMessage(from []string, msg *pb.GossipMessage) {
	switch msg.Type {
	case pb.GossipMessage_Ack, pb.GossipMessage_Ping, pb.GossipMessage_PingReq:
		e.processFailureDetectionProto(from, msg)

	case pb.GossipMessage_Sync:
		e.processSyncGossipProto(from, msg)
	}
}

func (e *EngineInstance) pumpGossipMessage() {
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

		e.dispatchGossipMessage(from, &msg)
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

func (e *EngineInstance) onSWIMTagUpdated(ctx *sladder.WatchEventContext, meta sladder.KeyValueEventMetadata) {
	switch meta.Event() {
	case sladder.ValueChanged:
		old, new := &SWIMTag{}, &SWIMTag{}
		meta := meta.(sladder.KeyChangeEventMetadata)
		if err := old.Decode(meta.Old()); err != nil {
			e.log.Error("cannot decode old swim tag, got " + err.Error())
			break
		}
		if err := new.Decode(meta.New()); err != nil {
			e.log.Error("cannot decode new swim tag, got " + err.Error())
			break
		}

		if meta.Node() == e.cluster.Self() {
			e.onSelfSWIMStateChanged(meta.Node(), old, new)
		} else {
			e.removeIfDeadOrLeft(meta.Node(), new)
		}
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

	var seedBuf [8]byte
	if _, err = crand.Read(seedBuf[:]); err != nil {
		return err
	}
	e.counterSeed = binary.LittleEndian.Uint64(seedBuf[:])

	e.cluster = c

	// register SWIM tag.
	if err = c.RegisterKey(e.swimTagKey, &SWIMTagValidator{}, true, 0); err != nil {
		return err
	}

	e.arbiter = arbit.New()

	// watch event to sync SWIM states.
	c.Keys(e.swimTagKey).Watch(e.onSWIMTagUpdated)

	e.goClusterSync()   // start cluster sync process.
	e.goDetectFailure() // start failure detection process.

	e.arbiter.Go(func() { e.dispatchEvents() })
	e.arbiter.Go(func() { e.pumpGossipMessage() })

	return nil
}

// Inited returns true if engine has already inited.
func (e *EngineInstance) Inited() bool { return e.arbiter != nil }

func (e *EngineInstance) canQuitTrivial() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()

	canQuit := true
	for _, existenceTrace := range e.reversedExistence {
		if existenceTrace.epoch < 1 {
			continue
		}
		if existenceTrace.exist {
			canQuit = false
			break
		}
	}

	return canQuit
}

// Close shutdown gossip engine instance.
func (e *EngineInstance) Close() error {
	// change state of self to LEAVE.
	if err := e.cluster.Txn(func(t *sladder.Transaction) bool {
		{
			rtx, err := t.KV(e.cluster.Self(), e.swimTagKey)
			if err != nil {
				e.log.Error("transaction get key-value failure, got " + err.Error())
				return false
			}
			tag := rtx.(*SWIMTagTxn)
			tag.Leave()
		}
		return true
	}); err != nil {
		e.log.Error("leave transaction failure, got " + err.Error())
		return err
	}

	e.quitAfter = atomic.AddUint64(&e.messageCounter, 1)
	timeout := e.QuitTimeout
	notWaitAfter := time.Now().Add(timeout)

	// TODO(xutao): deal with the situation that more then one nodes are racing to quit.
	e.tickGossipPeriodGo(func(deadline time.Time) {
		canShutdown := e.canQuit
		if !canShutdown {
			canShutdown = e.canQuitTrivial()
		}

		if !canShutdown && timeout > 0 && time.Now().After(notWaitAfter) {
			e.log.Warnf("leaving process seems not to be finished within %v. force to quit cluster.", timeout)
			canShutdown = true
		}

		if canShutdown {
			e.arbiter.Shutdown()
		}
	})

	e.arbiter.Join()
	return nil
}
