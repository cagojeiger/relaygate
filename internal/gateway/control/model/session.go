// Package controlmodel defines transport-neutral control identities shared by
// the controller authority and Gateway control client.
package controlmodel

type AuthorityRef struct {
	ClusterEpoch string
	AuthorityID  string
}

type SessionRef struct {
	ClusterEpoch      string
	AuthorityID       string
	ControlSessionID  string
	GatewayID         string
	GatewayInstanceID string
}

type Session struct {
	Ref  SessionRef
	Done <-chan struct{}
}
