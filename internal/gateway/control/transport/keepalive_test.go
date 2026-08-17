package controltransport

import "testing"

func TestKeepalivePolicyLeavesEnforcementMargin(t *testing.T) {
	if KeepaliveMinPingTime <= 0 || KeepaliveMinPingTime*2 > KeepaliveTime {
		t.Fatalf("keepalive enforcement margin = %s minimum / %s peer interval", KeepaliveMinPingTime, KeepaliveTime)
	}
	if KeepaliveTimeout <= 0 || KeepaliveTimeout > KeepaliveTime {
		t.Fatalf("keepalive timeout = %s for %s peer interval", KeepaliveTimeout, KeepaliveTime)
	}
}
