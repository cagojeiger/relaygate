package controlstate

import (
	"fmt"
	"testing"
)

var (
	benchmarkEpoch       string
	benchmarkBindingSlot BindingSlot
	benchmarkGatewaySlot GatewaySlot
)

func BenchmarkFSMPointReadsByBindingCardinality(b *testing.B) {
	for _, cardinality := range []int{0, 512, 100_000} {
		b.Run(fmt.Sprintf("bindings=%d", cardinality), func(b *testing.B) {
			fsm, liveKey, tombstoneKey := benchmarkFSM(cardinality)

			b.Run("cluster_epoch", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkEpoch = fsm.ClusterEpoch()
				}
			})
			b.Run("binding_live_or_missing", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkBindingSlot = fsm.Lookup(liveKey)
				}
			})
			b.Run("binding_tombstone_or_missing", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkBindingSlot = fsm.Lookup(tombstoneKey)
				}
			})
			b.Run("gateway", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkGatewaySlot = fsm.LookupGateway("gateway-owner")
				}
			})
		})
	}
}

func benchmarkFSM(cardinality int) (*FSM, BindingKey, BindingKey) {
	fsm := NewFSM()
	fsm.clusterEpoch = "epoch-1"
	fsm.gateways["gateway-owner"] = GatewaySlot{
		GatewayID:  "gateway-owner",
		Generation: 1,
		Ref:        &GatewayRegistrationRef{GatewayInstanceID: "instance-owner"},
	}

	liveKey := BindingKey{ClientID: "client-a", EndpointPattern: "/missing-live", TargetID: "worker"}
	tombstoneKey := BindingKey{ClientID: "client-a", EndpointPattern: "/missing-tombstone", TargetID: "worker"}
	for index := 0; index < cardinality; index++ {
		key := BindingKey{
			ClientID:        "client-a",
			EndpointPattern: fmt.Sprintf("/jobs/%06d", index),
			TargetID:        "worker",
		}
		slot := BindingSlot{Key: key, Generation: 2}
		if index%2 == 0 {
			tombstoneKey = key
		} else {
			ref := ListenerBindingRef{
				GatewayID:         "gateway-owner",
				GatewayInstanceID: "instance-owner",
				ListenerBindingID: fmt.Sprintf("listener-%06d", index),
			}
			slot.Generation = 1
			slot.Ref = &ref
			liveKey = key
		}
		fsm.bindings[key] = slot
	}
	return fsm, liveKey, tombstoneKey
}
