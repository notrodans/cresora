package reaper

import (
	"testing"
	"time"
)

func TestDefaultsAreBounded(t *testing.T) {
	config := Defaults()
	if config.BatchSize != DefaultBatchSize {
		t.Fatalf("default batch size = %d, want %d", config.BatchSize, DefaultBatchSize)
	}
	if config.ExpiredLeaseGrace != DefaultExpiredLeaseGrace {
		t.Fatalf("default lease grace = %s, want %s", config.ExpiredLeaseGrace, DefaultExpiredLeaseGrace)
	}
	if config.RetryDelay != DefaultRetryDelay {
		t.Fatalf("default retry delay = %s, want %s", config.RetryDelay, DefaultRetryDelay)
	}
	if config.BatchSize > MaxBatchSize {
		t.Fatalf("default batch size = %d, exceeds max %d", config.BatchSize, MaxBatchSize)
	}
}

func TestConfigRejectsInvalidOrNegativePolicy(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{name: "zero batch", config: Config{BatchSize: 0}},
		{name: "negative batch", config: Config{BatchSize: -1}},
		{name: "negative grace", config: Config{BatchSize: 1, ExpiredLeaseGrace: -time.Second}},
		{name: "negative retry delay", config: Config{BatchSize: 1, RetryDelay: -time.Second}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("config normalization did not panic")
				}
			}()
			_ = test.config.normalize()
		})
	}
}

func TestConfigCapsBatchSize(t *testing.T) {
	config := (Config{BatchSize: MaxBatchSize + 1}).normalize()
	if config.BatchSize != MaxBatchSize {
		t.Fatalf("normalized batch size = %d, want %d", config.BatchSize, MaxBatchSize)
	}
}
