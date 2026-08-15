package raftnode

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	proposals        *prometheus.CounterVec
	proposalDuration prometheus.Histogram
	snapshots        *prometheus.CounterVec
	snapshotDuration prometheus.Histogram
}

func newMetrics(registerer prometheus.Registerer, node *Node) (*metrics, error) {
	m := &metrics{
		proposals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relaygate",
			Subsystem: "raft",
			Name:      "proposals_total",
			Help:      "Control proposals observed by result.",
		}, []string{"result"}),
		proposalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "relaygate",
			Subsystem: "raft",
			Name:      "proposal_duration_seconds",
			Help:      "Time spent waiting for a control proposal outcome.",
			Buckets:   prometheus.DefBuckets,
		}),
		snapshots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relaygate",
			Subsystem: "raft",
			Name:      "snapshots_total",
			Help:      "Explicit snapshots observed by result.",
		}, []string{"result"}),
		snapshotDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "relaygate",
			Subsystem: "raft",
			Name:      "snapshot_duration_seconds",
			Help:      "Time spent waiting for an explicit snapshot outcome.",
			Buckets:   prometheus.DefBuckets,
		}),
	}
	if registerer == nil {
		return m, nil
	}
	collectors := []prometheus.Collector{
		m.proposals,
		m.proposalDuration,
		m.snapshots,
		m.snapshotDuration,
		&statusCollector{node: node},
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, previous := range registered {
				registerer.Unregister(previous)
			}
			return nil, fmt.Errorf("register Raft metric: %w", err)
		}
		registered = append(registered, collector)
	}
	return m, nil
}

func (m *metrics) observeProposal(result string, duration time.Duration) {
	m.proposals.WithLabelValues(result).Inc()
	m.proposalDuration.Observe(duration.Seconds())
}

func (m *metrics) observeSnapshot(result string, duration time.Duration) {
	m.snapshots.WithLabelValues(result).Inc()
	m.snapshotDuration.Observe(duration.Seconds())
}

type statusCollector struct {
	node *Node
}

var (
	raftRole = prometheus.NewDesc(
		"relaygate_raft_role",
		"Current Raft role; exactly one finite role is 1.",
		[]string{"role"},
		nil,
	)
	raftHasLeader = prometheus.NewDesc(
		"relaygate_raft_has_leader",
		"Whether this node currently knows a leader.",
		nil,
		nil,
	)
	raftReady = prometheus.NewDesc(
		"relaygate_raft_ready",
		"Whether control state is initialized, a leader is known, and shutdown has not begun.",
		nil,
		nil,
	)
	raftTerm = prometheus.NewDesc(
		"relaygate_raft_term",
		"Current Raft term.",
		nil,
		nil,
	)
	raftCommitIndex = prometheus.NewDesc(
		"relaygate_raft_commit_index",
		"Highest committed Raft log index.",
		nil,
		nil,
	)
	raftAppliedIndex = prometheus.NewDesc(
		"relaygate_raft_applied_index",
		"Highest Raft log index applied to the control FSM.",
		nil,
		nil,
	)
	raftSnapshotIndex = prometheus.NewDesc(
		"relaygate_raft_last_snapshot_index",
		"Highest Raft log index represented by a local snapshot.",
		nil,
		nil,
	)
	raftPendingFSM = prometheus.NewDesc(
		"relaygate_raft_fsm_pending",
		"Control commands waiting to be applied to the FSM.",
		nil,
		nil,
	)
	raftPeers = prometheus.NewDesc(
		"relaygate_raft_peers",
		"Number of other servers in the latest Raft configuration.",
		nil,
		nil,
	)
)

func (c *statusCollector) Describe(channel chan<- *prometheus.Desc) {
	channel <- raftRole
	channel <- raftHasLeader
	channel <- raftReady
	channel <- raftTerm
	channel <- raftCommitIndex
	channel <- raftAppliedIndex
	channel <- raftSnapshotIndex
	channel <- raftPendingFSM
	channel <- raftPeers
}

func (c *statusCollector) Collect(channel chan<- prometheus.Metric) {
	status := c.node.Status()
	for _, role := range []string{"Follower", "Candidate", "Leader", "Shutdown"} {
		value := 0.0
		if role == status.Role {
			value = 1
		}
		channel <- prometheus.MustNewConstMetric(raftRole, prometheus.GaugeValue, value, role)
	}
	hasLeader := 0.0
	if status.LeaderAddress != "" {
		hasLeader = 1
	}
	ready := 0.0
	if status.Ready {
		ready = 1
	}
	channel <- prometheus.MustNewConstMetric(raftHasLeader, prometheus.GaugeValue, hasLeader)
	channel <- prometheus.MustNewConstMetric(raftReady, prometheus.GaugeValue, ready)
	channel <- prometheus.MustNewConstMetric(raftTerm, prometheus.GaugeValue, float64(status.Term))
	channel <- prometheus.MustNewConstMetric(raftCommitIndex, prometheus.GaugeValue, float64(status.CommitIndex))
	channel <- prometheus.MustNewConstMetric(raftAppliedIndex, prometheus.GaugeValue, float64(status.AppliedIndex))
	channel <- prometheus.MustNewConstMetric(raftSnapshotIndex, prometheus.GaugeValue, float64(status.LastSnapshotIndex))
	channel <- prometheus.MustNewConstMetric(raftPendingFSM, prometheus.GaugeValue, float64(status.PendingFSMCommands))
	channel <- prometheus.MustNewConstMetric(raftPeers, prometheus.GaugeValue, float64(status.PeerCount))
}
