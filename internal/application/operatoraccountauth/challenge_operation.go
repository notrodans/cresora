package operatoraccountauth

import "context"

// challengeOperation is the per-call view of one challenge used by Code,
// Password, and Cancel. It serializes every provider RPC and terminal state
// transition through the owning record mutex.
type challengeOperation struct {
	registry  *challengeRegistry
	record    *challengeRecord
	ctx       context.Context
	completed bool
	aborted   bool
	account   Account
}

func (operation *challengeOperation) Challenge() Challenge {
	return operation.registry.projection(operation.record)
}

func (operation *challengeOperation) AuthTarget() AuthTarget {
	return operation.record.target
}

func (operation *challengeOperation) PhoneCodeHash() PhoneCodeHash {
	return operation.record.hash
}

func (operation *challengeOperation) Context() context.Context {
	return operation.ctx
}

func (operation *challengeOperation) PendingProfile() (Profile, bool) {
	if operation.record.pendingProfile == nil {
		return Profile{}, false
	}
	return *operation.record.pendingProfile, true
}

func (operation *challengeOperation) ReserveCodeAttempt() bool {
	if operation.record.codeAttempts >= maxCodeAttempts {
		return false
	}
	operation.record.codeAttempts++
	return true
}

func (operation *challengeOperation) ReservePasswordAttempt() bool {
	if operation.record.passwordAttempts >= maxPasswordAttempts {
		return false
	}
	operation.record.passwordAttempts++
	return true
}

func (operation *challengeOperation) SetPendingProfile(profile Profile) {
	copy := profile
	operation.record.pendingProfile = &copy
	operation.record.hash = PhoneCodeHash{}
	operation.record.ready = false
}

func (operation *challengeOperation) SetStage(stage Stage) error {
	if stage != StageCode && stage != StagePassword {
		return ErrInvalidInput
	}
	operation.record.stage = stage
	return nil
}

func (operation *challengeOperation) Complete(account Account) {
	if operation.completed || operation.aborted {
		return
	}
	operation.registry.removeWithTombstone(operation.record, &account, false)
	operation.completed = true
	operation.account = account
}

func (operation *challengeOperation) Abort() {
	if operation.completed || operation.aborted {
		return
	}
	operation.registry.removeWithTombstone(operation.record, nil, true)
	operation.aborted = true
}
