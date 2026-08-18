package main

import (
	"testing"
)

func TestParseConfigPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		environ map[string]string
		want    string
	}{
		{name: "default", want: "relaygate.yaml"},
		{name: "environment", environ: map[string]string{"RELAYGATE_CONFIG": "env.yaml"}, want: "env.yaml"},
		{name: "flag overrides environment", args: []string{"-config", "flag.yaml"}, environ: map[string]string{"RELAYGATE_CONFIG": "env.yaml"}, want: "flag.yaml"},
	} {
		t.Run(test.name, func(t *testing.T) {
			getenv := func(key string) string { return test.environ[key] }
			got, err := parseConfigPath(test.args, getenv)
			if err != nil {
				t.Fatalf("parseConfigPath(): %v", err)
			}
			if got != test.want {
				t.Fatalf("parseConfigPath() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestParseConfigPathRejectsUnknownFlag(t *testing.T) {
	if _, err := parseConfigPath([]string{"-unknown"}, func(string) string { return "" }); err == nil {
		t.Fatal("parseConfigPath() succeeded with unknown flag")
	}
}

func TestParseConfigPathRejectsSubcommandAfterGlobalFlag(t *testing.T) {
	args := []string{"-config", "relaygate.yaml", "membership", "list"}
	if _, err := parseConfigPath(args, func(string) string { return "" }); err == nil {
		t.Fatal("parseConfigPath() accepted a membership subcommand after global flags")
	}
}
