package sandboxobservability

import "time"

const (
	// DefaultRuntimeSampleInterval is the node-local sandbox runtime collection cadence.
	DefaultRuntimeSampleInterval = 15 * time.Second
	// DefaultRuntimeSampleJitter spreads node-local CRI collection across the interval.
	DefaultRuntimeSampleJitter = 1500 * time.Millisecond
	// DefaultRuntimeSampleMaxConcurrency bounds concurrent CRI stats calls per node.
	DefaultRuntimeSampleMaxConcurrency = 4
)
