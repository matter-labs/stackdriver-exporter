package collectors

import (
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
	return false                             // Not a duplicate
}

// hashLabelsTimestamp calculates a hash based on FQName, sorted labels, and timestamp.
func (d *MetricDeduplicator) hashLabelsTimestamp(fqName string, labelKeys, labelValues []string, ts time.Time) uint64 {
	dh := hash.New()
	dh = hash.Add(dh, fqName)
	dh = hash.AddByte(dh, hash.SeparatorByte)

	// Create indices for stable sorting
	indices := make([]int, len(labelKeys))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by key using a simple insertion sort
	// This is faster for small slices than sort.Slice
	for i := 0; i < len(indices); i++ {
		for j := i + 1; j < len(indices); j++ {
			if labelKeys[indices[i]] > labelKeys[indices[j]] {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}

	// Add sorted key-value pairs to hash
	for _, idx := range indices {
		dh = hash.Add(dh, labelKeys[idx])
		dh = hash.AddByte(dh, hash.SeparatorByte)

		// Ensure we don't go out of bounds if labelValues is shorter
		if idx < len(labelValues) {
			dh = hash.Add(dh, labelValues[idx])
		}
		dh = hash.AddByte(dh, hash.SeparatorByte)
	}

	// Add timestamp using binary operations instead of string conversion
	tsNano := ts.UnixNano()

	// Mix in the timestamp bytes directly using the FNV-1a algorithm
	dh = hash.AddUint64(dh, uint64(tsNano))

	// Mix in the high bits if they exist (for timestamps far in the future)
	if tsNano > 0xFFFFFFFF {
		dh = hash.AddUint64(dh, uint64(tsNano>>32))
	}

	return dh
}
