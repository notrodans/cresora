package operatoraccount

// Version is the optimistic-concurrency version of an account's current
// lifecycle state.
type Version uint64

// InitialVersion is the version assigned to a newly created account.
const InitialVersion Version = 1
