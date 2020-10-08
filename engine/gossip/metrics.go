package gossip

import "sync/atomic"

// StateMetrics collects gossip member state metrics.
type StateMetrics struct {
	Alive     uint32 // nodes in ALIVE state.
	Suspected uint32 // nodes in SUSPECTED state.
	Dead      uint32 // nodes in DEAD state.
	Left      uint32 // nodes in LEFT state.
}

// AtomicAdd applies metrics inremental.
func (m *StateMetrics) AtomicAdd(inc *StateMetrics) {
	if inc == nil {
		return
	}

	if inc := inc.Alive; inc > 0 {
		atomic.AddUint32(&m.Alive, inc)
	}
	if inc := inc.Suspected; inc > 0 {
		atomic.AddUint32(&m.Suspected, inc)
	}
	if inc := inc.Left; inc > 0 {
		atomic.AddUint32(&m.Left, inc)
	}
	if inc := inc.Dead; inc > 0 {
		atomic.AddUint32(&m.Dead, inc)
	}
}

// Metrics collects gossip engine statistics.
type Metrics struct {
	GossipPeriod uint64 // gossip period in nanosecond.
	GossipFanout uint32

	Sync struct {
		IncomingPushPull uint64 // incoming push-pull requests.
		PushPull         uint64 // sent push-pull requests.

		IncomingPush uint64 // incoming push requests.
		Push         uint64 // send push requests.
	}

	State StateMetrics

	FailureDetector struct {
		Ping         uint32 // direct pings in progress.
		PingIndirect uint32 // indirect pings in progress.
		Success      uint64 // successful pings.
		Failure      uint64 // failed pings.

		ProxyPing    uint32 // proxy ping requests in progress.
		ProxySuccess uint64 // successful proxy ping.
		ProxyFailure uint64 // failed proxy ping.
	}
}
