package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sunmxt/sladder"
	"github.com/sunmxt/sladder/util"
)

type testClusterViewPoint struct {
	engine *EngineInstance
	self   *sladder.Node
	cv     *sladder.Cluster
	ns     *sladder.TestNamesInKeyNameResolver
}

type testClusterGod struct {
	vps         map[*testClusterViewPoint]struct{}
	indexByName map[string]*testClusterViewPoint
}

func newTestClusterGod() *testClusterGod {
	return &testClusterGod{
		vps:         make(map[*testClusterViewPoint]struct{}),
		indexByName: make(map[string]*testClusterViewPoint),
	}
}

func (g *testClusterGod) RangeVP(visit func(*testClusterViewPoint) bool) {
	for vp := range g.vps {
		if !visit(vp) {
			break
		}
	}
}

func (g *testClusterGod) VPList() (vps []*testClusterViewPoint) {
	for vp := range g.vps {
		vps = append(vps, vp)
	}
	return
}

func (g *testClusterGod) AllViewpointConsist(nodes, entries bool) (consist bool) {
	consist = true

	stopFn := func(s *string) bool {
		consist = false
		return false
	}

	// node
	var prevList []string
	for vp := range g.vps {
		nameList := []string{}
		vp.cv.RangeNodes(func(n *sladder.Node) bool {
			names := n.Names()
			sort.Strings(names)
			nameList = append(nameList, "\""+strings.Join(names, "\",\"")+"\"")
			return true
		}, false, false)
		sort.Strings(nameList)

		if prevList != nil {
			util.RangeOverStringSortedSet(prevList, nameList, stopFn, stopFn, nil)
		}
		if !consist {
			return
		}
		prevList = nameList
	}

	// entries.
	type kvList struct {
		Key   string `json:"k"`
		Value string `json:"v"`
	}

	prevList = nil
	for vp := range g.vps {
		entriesList := []string{}

		vp.cv.RangeNodes(func(n *sladder.Node) bool {
			for _, kv := range n.KeyValueEntries(true) {
				b, err := json.Marshal(&kvList{Key: kv.Key, Value: kv.Value})
				if err != nil {
					panic(err)
				}
				entriesList = append(entriesList, string(b))
			}
			return true
		}, false, false)
		sort.Strings(entriesList)

		if prevList != nil {
			util.RangeOverStringSortedSet(prevList, entriesList, stopFn, stopFn, nil)
		}
		if !consist {
			return
		}
		prevList = entriesList
	}

	return
}

type mockTransportMessage struct {
	msg  []byte
	from []string
	next *mockTransportMessage

	gid uint32
}

type mockMessageGroupMeta struct {
	numOfMsg int32
	id       uint32
	next     *mockMessageGroupMeta
}

type MockTransport struct {
	lock sync.Mutex

	names []string
	c     *TestTransportControl

	mqh     *mockTransportMessage
	mqt     *mockTransportMessage
	watcher map[chan struct{}]struct{}
}

type TestTransportControl struct {
	lock                       sync.RWMutex
	condNewMessageGroupRelease *sync.Cond
	concurrencyLeases          int32
	messageGroup               uint32
	messageGroupMetas          *mockMessageGroupMeta
	messageGroupMetasTail      *mockMessageGroupMeta
	transports                 map[string]Transport

	MaxConcurrentMessage int32
}

func NewTestTransportControl() (c *TestTransportControl) {
	c = &TestTransportControl{
		transports:           make(map[string]Transport),
		MaxConcurrentMessage: 3,
		concurrencyLeases:    0,
		messageGroup:         0,
	}
	c.condNewMessageGroupRelease = sync.NewCond(&c.lock)
	return
}

func (c *TestTransportControl) GetTransport(create bool, names ...string) (t *MockTransport) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.getTransport(create, names...)
}

func (c *TestTransportControl) getTransport(create bool, names ...string) (t *MockTransport) {
	var mt *MockTransport

	for _, name := range names {
		t, hasTransport := c.transports[name]
		if hasTransport && t != nil {
			mt = t.(*MockTransport)
			break
		}
	}
	if mt == nil && create {
		mt = &MockTransport{
			names:   names,
			c:       c,
			watcher: make(map[chan struct{}]struct{}),
		}
		for _, name := range names {
			c.transports[name] = mt
		}
	} else {
		for _, name := range mt.names {
			if t, _ := c.transports[name]; t != Transport(mt) {
				delete(c.transports, name)
			}
		}
		for _, name := range names {
			c.transports[name] = mt
		}
		mt.names = names
		mt.c = c
	}

	return mt
}

func (c *TestTransportControl) Transport(names ...string) (t Transport) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.getTransport(true, names...)
}

func (c *TestTransportControl) appendMessageGroup(numOfMsg int32, gid uint32) {
	new := &mockMessageGroupMeta{
		numOfMsg: numOfMsg,
		id:       gid,
		next:     nil,
	}
	if c.messageGroupMetas == nil {
		c.messageGroupMetas = new
		c.condNewMessageGroupRelease.Broadcast()
	} else {
		c.messageGroupMetasTail.next = new
	}
	c.messageGroupMetasTail = new
}

func (c *TestTransportControl) dropMessageGroup() {
	if c.messageGroupMetas == nil {
		return
	}
	c.messageGroupMetas = c.messageGroupMetas.next
	if c.messageGroupMetas == nil {
		c.messageGroupMetasTail = nil
	} else {
		c.condNewMessageGroupRelease.Broadcast()
	}
}

func (c *TestTransportControl) traceNewMessage() (gid uint32) {
	if c.MaxConcurrentMessage > 0 {
		for {
			if c.concurrencyLeases > 0 {
				c.concurrencyLeases--
				break
			}
			c.concurrencyLeases = rand.Int31n(c.MaxConcurrentMessage) + 1
			c.messageGroup++
			c.appendMessageGroup(c.concurrencyLeases, c.messageGroup)
		}
	}
	return c.messageGroup
}

func (c *TestTransportControl) TraceNewMessage() (gid uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.traceNewMessage()
}

func (c *TestTransportControl) releaseMessage(gid uint32) {
	for {
		if c.messageGroupMetas == nil || c.messageGroupMetas.id < gid {
			c.condNewMessageGroupRelease.Wait()
			continue
		}

		c.messageGroupMetas.numOfMsg--
		if c.messageGroupMetas.numOfMsg < 1 {
			c.dropMessageGroup()
		}
		break
	}

}

func (c *TestTransportControl) ReleaseMessage(gid uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.releaseMessage(gid)
}

func (t *MockTransport) Send(names []string, buf []byte) {
	target := t.c.GetTransport(false, names...)
	if target == nil {
		return
	}

	m := &mockTransportMessage{
		msg:  buf,
		from: names,
		gid:  t.c.TraceNewMessage(),
	}

	target.lock.Lock()
	defer target.lock.Unlock()

	// enqueue
	if target.mqh == nil {
		target.mqh, target.mqt = m, m
	} else {
		target.mqt.next = m
		target.mqt = target.mqt.next
	}

	// notify.
	for wc := range target.watcher {
		wc <- struct{}{}
		delete(target.watcher, wc)
	}
}

func (t *MockTransport) Receive(ctx context.Context) (from []string, buf []byte) {
	var gid uint32

	t.lock.Lock()

	var wc chan struct{}

	for {
		if t.mqh != nil {
			from, buf, gid = t.mqh.from, t.mqh.msg, t.mqh.gid
			// dequeue.
			t.mqh = t.mqh.next
			if t.mqh == nil {
				t.mqt = nil
			}
			break
		}

		if wc == nil {
			wc = make(chan struct{}, 1)
		}
		t.watcher[wc] = struct{}{}

		t.lock.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-wc:
		}
		t.lock.Lock()
	}

	t.lock.Unlock()

	t.c.ReleaseMessage(gid)

	return
}

func newClusterGod(namePrefix string, numOfName, numOfNode int,
	engineOptions []sladder.EngineOption,
	clusterOptions []sladder.ClusterOption) (god *testClusterGod, ctl *TestTransportControl, err error) {

	if len(namePrefix) > 0 && !strings.HasSuffix(namePrefix, "-") {
		namePrefix = namePrefix + "-"
	}

	if numOfName < 1 {
		numOfName = 1
	}

	god, ctl = newTestClusterGod(), NewTestTransportControl()

	//defer func() {
	//	if err != nil {
	//		for vp := range god.vps {
	//			vp.cv.Quit()
	//		}
	//	}
	//}()

	for i := 0; i < numOfNode; i++ {
		var names []string

		for ni := 0; ni < numOfName; ni++ {
			names = append(names, namePrefix+strconv.FormatInt(int64(i), 10)+"-"+strconv.FormatInt(int64(ni), 10))
		}

		vp := &testClusterViewPoint{}

		engineOptions = append(engineOptions, ManualClearSuspections(), ManualFailureDetect(), ManualSync())

		vp.engine = New(ctl.Transport(names...), engineOptions...).(*EngineInstance)
		vp.ns = &sladder.TestNamesInKeyNameResolver{
			Key: "idkey",
		}
		if vp.cv, vp.self, err = sladder.NewClusterWithNameResolver(vp.engine, vp.ns, clusterOptions...); err != nil {
			return nil, nil, err
		}
		if err = vp.cv.RegisterKey("idkey", &sladder.TestNamesInKeyIDValidator{}, false, 0); err != nil {
			return nil, nil, err
		}
		if terr := vp.cv.Txn(func(t *sladder.Transaction) bool {
			{
				rtx, ierr := t.KV(vp.cv.Self(), "idkey")
				if ierr != nil {
					err = ierr
					return false
				}
				tx := rtx.(*sladder.TestNamesInKeyTxn)
				tx.AddName(names...)
			}
			return true
		}); terr != nil {
			return nil, nil, terr
		}
		if err != nil {
			return nil, nil, err
		}

		god.vps[vp] = struct{}{}
		for _, name := range names {
			god.indexByName[name] = vp
		}
	}

	return
}

func TestGossipEngine(t *testing.T) {
	t.Run("new_engine", func(t *testing.T) {
		ctl := NewTestTransportControl()
		tp := ctl.Transport("default")

		assert.Panics(t, func() {
			New(nil)
		})

		// test engine options
		re := New(tp, WithMinRegionPeer(4))
		assert.NotNil(t, re)
		assert.Equal(t, uint(4), re.(*EngineInstance).minRegionPeer)

		re = New(tp, WithMinRegionPeer(0))
		assert.NotNil(t, re)
		assert.Greater(t, re.(*EngineInstance).minRegionPeer, uint(0))

		re = New(tp, WithRegion("rg1"))
		assert.NotNil(t, re)
		assert.Equal(t, "rg1", re.(*EngineInstance).Region)
		randomTimeout := time.Duration(rand.Int63n(10000000))

		re = New(tp, WithSuspectTimeout(randomTimeout))
		assert.NotNil(t, re)
		assert.Equal(t, randomTimeout, re.(*EngineInstance).SuspectTimeout)
		randomKey := fmt.Sprintf("%x", rand.Int63n(0x7FFFFFFFFFFFFFFF))

		re = New(tp, WithSWIMTagKey(randomKey))
		assert.NotNil(t, re)
		assert.Equal(t, randomKey, re.(*EngineInstance).SWIMTagKey())

		mlog := &sladder.MockLogger{}
		re = New(tp, WithLogger(mlog))
		assert.NotNil(t, re)
		assert.Equal(t, mlog, re.(*EngineInstance).log.(*sladder.MockLogger))

		re = New(tp, WithFanout(0))
		assert.NotNil(t, re)
		assert.Greater(t, re.(*EngineInstance).Fanout, int32(0))
		re.(*EngineInstance).Fanout = 0
		assert.Greater(t, re.(*EngineInstance).getGossipFanout(), int32(0))

		re = New(tp, WithFanout(5))
		assert.NotNil(t, re)
		assert.Equal(t, int32(5), re.(*EngineInstance).Fanout)
		assert.Equal(t, int32(5), re.(*EngineInstance).getGossipFanout())

		re = New(tp, WithGossipPeriod(randomTimeout))
		assert.NotNil(t, re)
		assert.Equal(t, randomTimeout, re.(*EngineInstance).getGossipPeriod())
		re.(*EngineInstance).GossipPeriod = 0
		assert.Equal(t, defaultGossipPeriod, re.(*EngineInstance).getGossipPeriod())
	})

}
