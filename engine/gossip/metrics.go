package gossip

import (
	"sync"
	"time"
)

// StateMetrics collects gossip member state metrics.
type StateMetrics struct {
	lock sync.Mutex

	StateMetricIncrement
}

// StateMetricIncrement contains state metric body.
type StateMetricIncrement struct {
	Alive     uint32 // nodes in ALIVE state.
	Suspected uint32 // nodes in SUSPECTED state.
	Dead      uint32 // nodes in DEAD state.
	Left      uint32 // nodes in LEFT state.
}

// ApplyIncrement applies metrics increment.
func (m *StateMetrics) ApplyIncrement(inc *StateMetricIncrement) {
	if inc == nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.Alive += inc.Alive
	m.Dead += inc.Dead
	m.Left += inc.Left
	m.Suspected += inc.Suspected
}

// SyncMetrics collects metrics of synchronization.
type SyncMetrics struct {
	lock sync.Mutex

	SyncMetricIncrement
}

// ApplyIncrement applys SyncMetricsIncrement.
func (m *SyncMetrics) ApplyIncrement(inc *SyncMetricIncrement) {
	if inc == nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.IncomingPushPull += inc.IncomingPushPull
	m.IncomingPush += inc.IncomingPush
	m.PushPull += inc.PushPull
	m.Push += inc.Push
}

// SyncMetricIncrement contains synchronization metric body.
type SyncMetricIncrement struct {
	IncomingPushPull uint64 // incoming push-pull requests.
	PushPull         uint64 // sent push-pull requests.

	IncomingPush uint64 // incoming push requests.
	Push         uint64 // send push requests.
}

// FailureDetectorMetrics collects failure detector metrics.
type FailureDetectorMetrics struct {
	lock sync.Mutex

	FailureDetectorMetricIncrement
}

// ApplyIncrement applys FailureDetectorMetricIncrement.
func (m *FailureDetectorMetrics) ApplyIncrement(inc *FailureDetectorMetricIncrement) {
	if inc == nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.Ping += inc.Ping
	m.PingIndirect += inc.PingIndirect
	m.Success += inc.Success
	m.Failure += inc.Failure
	m.ProxyPing += inc.ProxyPing
	m.ProxySuccess += inc.ProxySuccess
	m.ProxyFailure += inc.ProxyFailure
}

// FailureDetectorMetricIncrement contains metrics of failure detector.
type FailureDetectorMetricIncrement struct {
	Ping         uint32 // direct pings in progress.
	PingIndirect uint32 // indirect pings in progress.
	Success      uint64 // successful pings.
	Failure      uint64 // failed pings.

	ProxyPing    uint32 // proxy ping requests in progress.
	ProxySuccess uint64 // successful proxy ping.
	ProxyFailure uint64 // failed proxy ping.
}

// Metrics collects gossip engine statistics.
type Metrics struct {
	lock sync.Mutex

	GossipPeriod uint64 // gossip period in nanosecond.
	GossipFanout uint32

	Sync            SyncMetrics
	State           StateMetrics
	FailureDetector FailureDetectorMetrics
}

// PublishGossipPeriod publishs gossip period to metric.
func (m *Metrics) PublishGossipPeriod(period time.Duration) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.GossipPeriod = uint64(period)
}

// PublishGossipFanout publishs gossip fanout to metric.
func (m *Metrics) PublishGossipFanout(fanout uint32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.GossipFanout = fanout
}
