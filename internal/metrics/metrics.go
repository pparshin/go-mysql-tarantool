package metrics

import "github.com/prometheus/client_golang/prometheus"

type ReplState int8

const (
	StateStopped ReplState = iota
	StateDumping
	StateRunning
)

var (
	secondsBehindMaster = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mysql2tarantool",
		Name:      "seconds_behind",
		Help:      "Current replication lag of the replicator",
	})

	replState = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mysql2tarantool",
		Name:      "state",
		Help:      "The replication running state: 0=stopped, 1=dumping, 2=running",
	})
)

func Init() {
	prometheus.MustRegister(secondsBehindMaster)
	prometheus.MustRegister(replState)
}

func SetSecondsBehindMaster(value uint32) {
	secondsBehindMaster.Set(float64(value))
}

func SetReplicationState(state ReplState) {
	replState.Set(float64(state))
}
