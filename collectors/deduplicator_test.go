package collectors

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetricDeduplicator(t *testing.T) {
	dedup := NewMetricDeduplicator()
	now := time.Now()
	later := now.Add(time.Second)

	fqName1 := "metric_one"
	keys1 := []string{"a", "b"}
	vals1 := []string{"val_a", "val_b"}

	fqName2 := "metric_two"
	keys2 := []string{"c"}
	vals2 := []string{"val_c"}

	// Test Case 1: First time seeing a metric
	isDup1 := dedup.CheckAndMark(fqName1, keys1, vals1, now)
	assert.False(t, isDup1, "First metric should not be a duplicate")

	// Test Case 2: Seeing the exact same metric again
	isDup2 := dedup.CheckAndMark(fqName1, keys1, vals1, now)
	assert.True(t, isDup2, "Second identical metric should be a duplicate")

	// Test Case 3: Same metric, different timestamp
	isDup3 := dedup.CheckAndMark(fqName1, keys1, vals1, later)
	assert.False(t, isDup3, "Metric with different timestamp should not be a duplicate")

	// Test Case 4: Different metric, same timestamp as first
	isDup4 := dedup.CheckAndMark(fqName2, keys2, vals2, now)
	assert.False(t, isDup4, "Different metric should not be a duplicate")

	// Test Case 5: Same metric as first, different label order (should be detected as duplicate due to sorting in hash)
	keys1_reordered := []string{"b", "a"}
	vals1_reordered := []string{"val_b", "val_a"}
	isDup5 := dedup.CheckAndMark(fqName1, keys1_reordered, vals1_reordered, now)
	assert.True(t, isDup5, "Metric with reordered labels should be a duplicate")

	// Test Case 6: Same metric, different label value
	vals1_changed := []string{"val_a", "val_b_changed"}
	isDup6 := dedup.CheckAndMark(fqName1, keys1, vals1_changed, now)
	assert.False(t, isDup6, "Metric with different label value should not be a duplicate")

	// Test Case 7: Check the previously skipped different label value metric again (should now be duplicate)
	isDup7 := dedup.CheckAndMark(fqName1, keys1, vals1_changed, now)
	assert.True(t, isDup7, "Second instance of metric with different label value should be a duplicate")

	// Test Case 8: New deduplicator instance should have empty state
	dedup2 := NewMetricDeduplicator()
	isDup8 := dedup2.CheckAndMark(fqName1, keys1, vals1, now)
	assert.False(t, isDup8, "Metric should not be duplicate in a new deduplicator instance")
}

// Test hash stability with different label orders
func TestHashLabelsTimestampStability(t *testing.T) {
	dedup := NewMetricDeduplicator() // Use instance to access the method
	now := time.Now()
	fqName := "stable_metric"

	keys1 := []string{"region", "zone", "instance"}
	vals1 := []string{"us-central1", "us-central1-a", "instance-1"}

	keys2 := []string{"zone", "instance", "region"}
	vals2 := []string{"us-central1-a", "instance-1", "us-central1"}

	hash1 := dedup.hashLabelsTimestamp(fqName, keys1, vals1, now)
	hash2 := dedup.hashLabelsTimestamp(fqName, keys2, vals2, now)

	assert.Equal(t, hash1, hash2, "Hashes should be equal for different label orders")

	// Test with different timestamp
	later := now.Add(time.Minute)
	hash3 := dedup.hashLabelsTimestamp(fqName, keys1, vals1, later)
	assert.NotEqual(t, hash1, hash3, "Hashes should be different for different timestamps")

	// Test with different fqName
	hash4 := dedup.hashLabelsTimestamp("different_metric", keys1, vals1, now)
	assert.NotEqual(t, hash1, hash4, "Hashes should be different for different fqNames")

	// Test with different label value
	vals3 := []string{"us-central1", "us-central1-b", "instance-1"}
	hash5 := dedup.hashLabelsTimestamp(fqName, keys1, vals3, now)
	assert.NotEqual(t, hash1, hash5, "Hashes should be different for different label values")
}
