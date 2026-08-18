package main

import "testing"

func TestParseCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		arguments []string
		want      command
		wantError bool
	}{
		{name: "serve", arguments: []string{"serve", "go"}, want: command{name: "serve", target: "go"}},
		{name: "send", arguments: []string{"send", "rust", "hello", "world"}, want: command{name: "send", target: "rust", message: "hello world"}},
		{name: "missing message", arguments: []string{"send", "go"}, wantError: true},
		{name: "invalid target", arguments: []string{"serve", "bad target"}, wantError: true},
		{name: "unknown", arguments: []string{"chat", "go"}, wantError: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseCommand(test.arguments)
			if test.wantError {
				if err == nil {
					t.Fatal("parseCommand() error = nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCommand() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("parseCommand() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLoadSettingsRequiresAPIKey(t *testing.T) {
	t.Parallel()
	if _, err := loadSettings(func(string) string { return "" }); err == nil {
		t.Fatal("loadSettings() error = nil")
	}
}

func TestLoadSettingsUsesLocalDefaults(t *testing.T) {
	t.Parallel()
	configuration, err := loadSettings(func(name string) string {
		if name == "RELAYGATE_ECHO_API_KEY" {
			return "secret"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("loadSettings() error = %v", err)
	}
	if configuration.address != defaultAddress || configuration.clientID != defaultClientID || configuration.apiKeyID != defaultAPIKeyID || configuration.endpoint != defaultEndpoint {
		t.Fatalf("loadSettings() = %#v", configuration)
	}
}
