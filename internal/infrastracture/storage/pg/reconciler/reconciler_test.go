package reconciler

import (
	"errors"
	"testing"
)

func TestConfigDefaultsAndBounds(t *testing.T) {
	if got := Defaults().BatchSize; got != DefaultBatchSize {
		t.Fatalf("default batch size = %d, want %d", got, DefaultBatchSize)
	}
	config := Config{BatchSize: MaxBatchSize + 1}
	if got := config.normalize().BatchSize; got != MaxBatchSize {
		t.Fatalf("normalized batch size = %d, want %d", got, MaxBatchSize)
	}
}

func TestConfigRejectsNonPositiveBatchSize(t *testing.T) {
	for _, batchSize := range []int{0, -1} {
		if err := (Config{BatchSize: batchSize}).Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("batch size %d validation = %v, want invalid config", batchSize, err)
		}
	}
}
