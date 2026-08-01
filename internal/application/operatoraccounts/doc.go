// Package operatoraccounts contains the application boundary for
// operator-owned account lifecycle operations.
//
// The current-only lifecycle graph is:
//
//	authenticating -> active | disconnecting
//	active -> reauth_required | disconnecting
//	reauth_required -> authenticating | disconnecting
//	disconnected -> authenticating
//	disconnecting -> disconnected
//
// Active and reauthentication-required snapshots have a canonical Telegram
// identity; authenticating snapshots have an authentication expiry.
//
// RuntimeTarget is the application-owned admission value for runtime work. It
// carries the actor, account, lifecycle status, and optimistic version without
// importing transport runtime types.
//
// It deliberately depends on the domain account ID and the authenticated
// application actor. Telegram transport session scopes belong to an adapter,
// not to this package.
package operatoraccounts
