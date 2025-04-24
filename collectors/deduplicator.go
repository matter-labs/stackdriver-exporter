package collectors

import (
	"sort"
	"strconv"
	"time"

	"github.com/prometheus-community/stackdriver_exporter/hash"
)

// MetricDeduplicator helps prevent sending duplicate metrics within a single scrape cycle.
// It tracks metrics based on a signature derived from FQName, labels, and timestamp.
type MetricDeduplicator struct {
	sentSignatures map[uint64]struct{}
}

// NewMetricDeduplicator creates a new deduplicator instance.
func NewMetricDeduplicator() *MetricDeduplicator {
	return &MetricDeduplicator{
		sentSignatures: make(map[uint64]struct{}),
	}
}

// CheckAndMark checks if a metric signature has already been seen for this deduplicator instance.
// If seen, it returns true (indicating a duplicate).
// If not seen, it marks the signature as seen and returns false.
func (d *MetricDeduplicator) CheckAndMark(fqName string, labelKeys, labelValues []string, ts time.Time) bool {
	signature := d.hashLabelsTimestamp(fqName, labelKeys, labelValues, ts)
	if _, exists := d.sentSignatures[signature]; exists {
		return true // Duplicate detected
	}
	d.sentSignatures[signature] = struct{}{} // Mark as seen
	return false // Not a duplicate
}

// hashLabelsTimestamp calculates a hash based on FQName, sorted labels, and timestamp.
func (d *MetricDeduplicator) hashLabelsTimestamp(fqName string, labelKeys, labelValues []string, ts time.Time) uint64 {
	dh := hash.New()
	dh = hash.Add(dh, fqName)
	dh = hash.AddByte(dh, hash.SeparatorByte)

	// Create label pairs for stable sorting
	pairs := make([]struct {
		Key   string
		Value string
	}, len(labelKeys))
	for i, key := range labelKeys {
		// Ensure we don't go out of bounds if labelValues is shorter (shouldn't happen in normal flow)
		val := ""
		if i < len(labelValues) {
			val = labelValues[i]
		}
		pairs[i] = struct {
			Key   string
			Value string
		}{Key: key, Value: val}
	}

	// Sort pairs by key
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})

	// Add sorted key-value pairs to hash
	for _, pair := range pairs {
		dh = hash.Add(dh, pair.Key)
		dh = hash.AddByte(dh, hash.SeparatorByte)
		dh = hash.Add(dh, pair.Value)
		dh = hash.AddByte(dh, hash.SeparatorByte)
	}

	// Add timestamp (converted to string)
	dh = hash.Add(dh, strconv.FormatInt(ts.UnixNano(), 10))
	return dh
}
