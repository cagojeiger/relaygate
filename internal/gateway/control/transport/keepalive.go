package controltransport

import "time"

const (
	KeepaliveTime        = 10 * time.Second
	KeepaliveTimeout     = 5 * time.Second
	KeepaliveMinPingTime = 5 * time.Second
)
