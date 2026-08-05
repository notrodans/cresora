package accountowner

import "github.com/notrodans/cresora/internal/domain/operatoraccount"

type fenceSet struct {
	records map[accountKey]stoppedFence
	clock   uint64
	limit   int
}

func newFenceSet(limit int) fenceSet {
	return fenceSet{
		records: make(map[accountKey]stoppedFence),
		limit:   limit,
	}
}

func (fences *fenceSet) unprotect(
	key accountKey,
	version operatoraccount.Version,
) {
	fence, exists := fences.records[key]
	if !exists || fence.version != version {
		return
	}
	fence.protected = false
	fences.records[key] = fence
}

// stoppedFence prevents the old version from restarting after it has been
// stopped.
type stoppedFence struct {
	version   operatoraccount.Version
	stamp     uint64
	protected bool
}

// recordFence records the greatest stopped version for key. The caller
// must hold registry.mu.
func (registry *Registry) recordFence(key accountKey, version operatoraccount.Version, protected bool) error {
	fence, exists := registry.fences.records[key]
	if exists {
		if version < fence.version {
			return ErrStaleAdmission
		}
		if version > fence.version {
			fence.version = version
		}
		fence.protected = fence.protected || protected
	} else {
		fence = stoppedFence{version: version, protected: protected}
	}
	if !exists {
		if registry.fences.limit <= 0 {
			return ErrFenceCapacity
		}
		if len(registry.fences.records) >= registry.fences.limit {
			oldestKey, evictable := registry.oldEvictableFence()
			if !evictable {
				return ErrFenceCapacity
			}
			delete(registry.fences.records, oldestKey)
		}
	}
	registry.fences.clock++
	fence.stamp = registry.fences.clock
	registry.fences.records[key] = fence
	return nil
}

// oldEvictableFence finds the state fence evictable first when capacity is
// reached. Caller must hold registry.mu.
func (registry *Registry) oldEvictableFence() (accountKey, bool) {
	var oldestKey accountKey
	var oldest uint64
	found := false
	for key, fence := range registry.fences.records {
		if fence.protected || (found && fence.stamp >= oldest) {
			continue
		}
		oldestKey, oldest, found = key, fence.stamp, true
	}
	return oldestKey, found
}

func (registry *Registry) unprotectFence(key accountKey, version operatoraccount.Version) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.fences.unprotect(key, version)
	registry.trimFenceRecords()
}

// trimFenceRecords drops unprotected fences beyond capacity. Caller must hold
// registry.mu.
func (registry *Registry) trimFenceRecords() {
	for len(registry.fences.records) > registry.fences.limit {
		oldestKey, evictable := registry.oldEvictableFence()
		if !evictable {
			return
		}
		delete(registry.fences.records, oldestKey)
	}
}
