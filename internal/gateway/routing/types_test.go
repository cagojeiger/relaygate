package routing

import (
	"errors"
	"strings"
	"testing"
)

func TestLiveBindingValidation(t *testing.T) {
	valid := LiveBinding{
		Key: BindingKey{ClientID: "client-a", EndpointPattern: "/echo", TargetID: "target-a"},
		Ref: ListenerBindingRef{GatewayID: "gateway-a", GatewayInstanceID: "instance-a", ListenerBindingID: "binding-a"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid binding: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*LiveBinding)
	}{
		{name: "missing client", mutate: func(binding *LiveBinding) { binding.Key.ClientID = "" }},
		{name: "missing endpoint", mutate: func(binding *LiveBinding) { binding.Key.EndpointPattern = "" }},
		{name: "oversized endpoint", mutate: func(binding *LiveBinding) {
			binding.Key.EndpointPattern = strings.Repeat("e", MaxEndpointPatternBytes+1)
		}},
		{name: "missing target", mutate: func(binding *LiveBinding) { binding.Key.TargetID = "" }},
		{name: "missing gateway", mutate: func(binding *LiveBinding) { binding.Ref.GatewayID = "" }},
		{name: "missing gateway instance", mutate: func(binding *LiveBinding) { binding.Ref.GatewayInstanceID = "" }},
		{name: "missing listener binding", mutate: func(binding *LiveBinding) { binding.Ref.ListenerBindingID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestValidateIdentityBoundaries(t *testing.T) {
	if err := ValidateIdentity("identity", strings.Repeat("i", MaxIdentityBytes)); err != nil {
		t.Fatalf("maximum identity: %v", err)
	}
	for _, value := range []string{"", strings.Repeat("i", MaxIdentityBytes+1)} {
		if err := ValidateIdentity("identity", value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ValidateIdentity(%d bytes) = %v, want ErrInvalid", len(value), err)
		}
	}
}
