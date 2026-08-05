package accountowner

import (
	"context"
	"time"
)

func (registry *Registry) makeCapacity() *runtimeEntry {
	if registry.liveCount() < registry.config.Capacity {
		return nil
	}
	now := time.Now()
	for _, slot := range registry.slots {
		isIdle := slot.idling(registry.config.IdleTimeout, now)
		if isIdle {
			rentry := slot.currentEntry()
			slot.closeAdmission()
			return rentry
		}
	}
	return nil
}

func (registry *Registry) liveCount() int {
	count := 0
	for _, slot := range registry.slots {
		live := slot.living()
		if live {
			count++
		}
	}
	return count
}

func (registry *Registry) reapIdle() {
	interval := registry.config.IdleTimeout
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(registry.reaperDone)
	for {
		select {
		case <-ticker.C:
			registry.evictIdle()
		case <-registry.stopReaper:
			return
		}
	}
}

func (registry *Registry) evictIdle() {
	var retired []struct {
		slot  *accountSlot
		entry *runtimeEntry
	}
	now := time.Now()
	registry.mu.Lock()
	for _, slot := range registry.slots {
		isIdle := slot.idling(registry.config.IdleTimeout, now)
		if isIdle {
			rentry := slot.currentEntry()
			slot.closeAdmission()
			retired = append(retired, struct {
				slot  *accountSlot
				entry *runtimeEntry
			}{slot: slot, entry: rentry})
		}
	}
	registry.mu.Unlock()

	for _, item := range retired {
		_ = registry.teardown(context.Background(), item.slot, item.entry)
	}
}
