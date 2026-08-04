package accountowner

import (
	"context"
	"time"
)

func (registry *Registry) makeCapacityLocked() *runtimeEntry {
	if registry.liveCountLocked() < registry.config.Capacity {
		return nil
	}
	now := time.Now()
	for _, slot := range registry.slots {
		idle := slot.current != nil && !slot.closed && !slot.stopping && slot.refs == 0 && slot.active == 0 && now.Sub(slot.lastUsed) >= registry.config.IdleTimeout
		if idle {
			rentry := slot.currentEntry()
			slot.closeAdmissionLocked()
			return rentry
		}
	}
	return nil
}

func (registry *Registry) liveCountLocked() int {
	count := 0
	for _, slot := range registry.slots {
		slot.mu.Lock()
		live := slot.current != nil && !slot.closed && !slot.stopping
		slot.mu.Unlock()
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
		if slot.current != nil && !slot.closed && !slot.stopping && slot.refs == 0 && slot.active == 0 && now.Sub(slot.lastUsed) >= registry.config.IdleTimeout {
			rentry := slot.currentEntry()
			slot.closeAdmissionLocked()
			retired = append(retired, struct {
				slot  *accountSlot
				entry *runtimeEntry
			}{slot: slot, entry: rentry})
		}
	}
	registry.mu.Unlock()

	for _, item := range retired {
		_ = registry.teardown(item.slot, item.entry, context.Background())
	}
}
