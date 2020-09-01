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

	"github.com/crossmesh/sladder"
	"github.com/crossmesh/sladder/util"
	"github.com/stretchr/testify/assert"
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

func ViewpointConsist(vps []*testClusterViewPoint, nodes, entries bool) (consist bool) {
	consist = true

	stopFn := func(s *string) bool {
		consist = false
		return false
	}

	var prevList []string

	if nodes {
		// node
		for _, vp := range vps {
			nameList := []string{}
			var nodes []*sladder.Node
			vp.cv.RangeNodes(func(n *sladder.Node) bool {
				nodes = append(nodes, n)
				return true
			}, false, false)
			for _, n := range nodes {
				names := n.Names()
				sort.Strings(names)
				nameList = append(nameList, "\""+strings.Join(names, "\",\"")+"\"")
			}
			sort.Strings(nameList)

			if prevList != nil {
				util.RangeOverStringSortedSet(prevList, nameList, stopFn, stopFn, nil)
			}
			if !consist {
				return
			}
			prevList = nameList
		}
	}

	if entries {
		// entries.
		type kvList struct {
			Key   string `json:"k"`
			Value string `json:"v"`
		}

		prevList = nil
		for _, vp := range vps {
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
	}

	return
}

func (g *testClusterGod) AllViewpointConsist(nodes, entries bool) (consist bool) {
	return ViewpointConsist(g.VPList(), nodes, entries)
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

type MockTransportRef struct {
	source []string
	*MockTransport
}

func (r *MockTransportRef) Send(names []string, buf []byte) {
	mt := r.MockTransport
	if mt == nil {
		return
	}
	mt.Send(r.source, names, buf)
}

type networkJam struct {
	from string
	to   string
}

type networkPart struct {
	group  int
	nodeID int
}

type NetworkPartition struct {
	groups  [][][]string
	indices map[string]*networkPart
}

func (n *NetworkPartition) updateIndices() error {
	n.indices = make(map[string]*networkPart)
	for gid, group := range n.groups {
		for nid, node := range group {
			for _, name := range node {
				if part, exists := n.indices[name]; exists {
					return fmt.Errorf("group overlapped: %v is already in group %v", name, n.groups[part.group])
				}
				n.indices[name] = &networkPart{
					group: gid, nodeID: nid,
				}
			}
		}
	}
	return nil
}

func (n *NetworkPartition) Jam(from, to []string) (exist bool) {
	var gfrom, gto *networkPart

	for _, from := range from {
		for _, to := range to {
			if gfrom, exist = n.indices[from]; !exist {
				continue
			}
			if gto, exist = n.indices[to]; !exist {
				continue
			}
			if gfrom.group == gto.group {
				continue
			}
			return true
		}
	}

	return false
}

type TestTransportControl struct {
	lock sync.RWMutex

	condNewMessageGroupRelease *sync.Cond
	concurrencyLeases          int32
	messageGroup               uint32
	messageGroupMetas          *mockMessageGroupMeta
	messageGroupMetasTail      *mockMessageGroupMeta

	transports map[string]*MockTransport

	jams       map[networkJam]struct{}
	partiation map[*NetworkPartition]struct{}

	MaxConcurrentMessage int32
}

func NewTestTransportControl() (c *TestTransportControl) {
	c = &TestTransportControl{
		transports:           make(map[string]*MockTransport),
		MaxConcurrentMessage: 4,
		concurrencyLeases:    0,
		messageGroup:         0,
		jams:                 make(map[networkJam]struct{}),
		partiation:           make(map[*NetworkPartition]struct{}),
	}
	c.condNewMessageGroupRelease = sync.NewCond(&c.lock)
	return
}

func (c *TestTransportControl) networkJam(from, to string) {
	c.jams[networkJam{
		from: from, to: to,
	}] = struct{}{}
}

func (c *TestTransportControl) NetworkJam(from, to string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.networkJam(from, to)
}

func (c *TestTransportControl) clearNetworkJam(from, to string) {
	delete(c.jams, networkJam{
		from: from, to: to,
	})
}

func (c *TestTransportControl) ClearNetworkJam(from, to string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clearNetworkJam(from, to)
}

func (c *TestTransportControl) networkInJam(names []string) {
	for _, name := range names {
		c.jams[networkJam{
			from: "", to: name,
		}] = struct{}{}
	}
}

func (c *TestTransportControl) networkOutJam(names []string) {
	for _, name := range names {
		c.jams[networkJam{
			from: name, to: "",
		}] = struct{}{}
	}
}

func (c *TestTransportControl) NetworkOutJam(names []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.networkOutJam(names)
}

func (c *TestTransportControl) NetworkInJam(names []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.networkInJam(names)
}

func (c *TestTransportControl) NetworkPartition(groups ...[][]string) (part *NetworkPartition, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(groups) < 1 {
		return nil, nil
	}

	getPart := func() *NetworkPartition {
		if part == nil {
			part = &NetworkPartition{
				indices: make(map[string]*networkPart),
			}
		}
		return part
	}

	for _, group := range groups {
		nIdx, eliIdx := 0, 0
		for ; nIdx < len(group); nIdx++ {
			node := group[nIdx]
			if len(node) < 1 {
				continue
			}
			if eliIdx != nIdx {
				node[eliIdx] = node[nIdx]
			}
			eliIdx++
		}
		group = group[:eliIdx]
		if len(group) < 1 {
			continue
		}
		part := getPart()
		part.groups = append(part.groups, group)
	}
	if part != nil {
		err = part.updateIndices()
		c.partiation[part] = struct{}{}
	}
	return
}

func (c *TestTransportControl) RemovePartition(part *NetworkPartition) {
	if part == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.partiation, part)
}

func (c *TestTransportControl) JamDropMessage(from, to []string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, from := range from {
		if _, drop := c.jams[networkJam{
			from: from, to: "",
		}]; drop {
			return true
		}
	}

	for _, to := range to {
		if _, drop := c.jams[networkJam{
			from: "", to: to,
		}]; drop {
			return true
		}
	}

	for _, from := range from {
		for _, to := range to {
			if _, drop := c.jams[networkJam{
				from: from, to: to,
			}]; drop {
				return true
			}
		}
	}

	// check network partition.
	for rule := range c.partiation {
		if rule.Jam(from, to) {
			return true
		}
	}

	return false
}

func (c *TestTransportControl) getTransport(create bool, source []string, names ...string) (r *MockTransportRef) {
	t := c.getMockTransport(create, names...)
	if t == nil {
		return nil
	}

	return &MockTransportRef{
		source:        source,
		MockTransport: t,
	}
}

func (c *TestTransportControl) getMockTransport(create bool, names ...string) (mt *MockTransport) {
	for _, name := range names {
		t, hasTransport := c.transports[name]
		if hasTransport && t != nil {
			mt = t
			break
		}
	}
	if !create {
		return
	}

	if mt == nil {
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
			if t, _ := c.transports[name]; t != mt {
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

func (c *TestTransportControl) RemoveTransportTarget(names ...string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	t := c.getMockTransport(false, names...)
	if t == nil {
		return
	}
	for _, name := range names {
		delete(c.transports, name)
	}
	go t.FlushQueue()
}

func (c *TestTransportControl) Transport(source []string, names ...string) (t Transport) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.getTransport(true, source, names...)
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
				return c.messageGroup
			}
			c.concurrencyLeases = rand.Int31n(c.MaxConcurrentMessage) + 1
			c.messageGroup++
			c.appendMessageGroup(c.concurrencyLeases, c.messageGroup)
		}
	}
	return
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

func (t *MockTransport) Send(from []string, names []string, buf []byte) {
	target := t.c.getMockTransport(false, names...)
	if target == nil || t.c.JamDropMessage(from, names) {
		return
	}

	target.lock.Lock()
	defer target.lock.Unlock()

	m := &mockTransportMessage{
		msg:  buf,
		from: from,
		gid:  t.c.TraceNewMessage(),
	}

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

func (t *MockTransport) dequeueMessage() (from []string, buf []byte, gid uint32) {
	if t.mqh != nil {
		from, buf, gid = t.mqh.from, t.mqh.msg, t.mqh.gid
		// dequeue.
		t.mqh = t.mqh.next
		if t.mqh == nil {
			t.mqt = nil
		}
	}
	return
}

func (t *MockTransport) Receive(ctx context.Context) (from []string, buf []byte) {
	var gid uint32

	t.lock.Lock()

	var wc chan struct{}

	for {
		if from, buf, gid = t.dequeueMessage(); buf != nil {
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

func (t *MockTransport) FlushQueue() {
	for {
		t.lock.Lock()
		_, buf, gid := t.dequeueMessage()
		if buf == nil {
			break
		}
		t.lock.Unlock()

		t.c.ReleaseMessage(gid)
	}
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

	ctl.MaxConcurrentMessage = 32

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

		vp.engine = New(ctl.Transport(names, names...), engineOptions...).(*EngineInstance)
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
		tp := ctl.Transport([]string{"default"}, "default")

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
