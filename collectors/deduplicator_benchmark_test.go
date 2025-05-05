package collectors

import (
	"testing"
	"time"
)

func BenchmarkHashLabelsTimestamp(b *testing.B) {
	dedup := NewMetricDeduplicator()
	now := time.Now()
	fqName := "benchmark_metric"
	keys := []string{"region", "zone", "instance", "project", "service", "method", "version"}
	vals := []string{"us-central1", "us-central1-a", "instance-1", "my-project", "api-service", "get", "v1"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dedup.hashLabelsTimestamp(fqName, keys, vals, now)
	}
}
