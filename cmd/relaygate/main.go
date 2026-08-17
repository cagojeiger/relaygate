package main

import (
	"flag"
	"fmt"
	"os"

	relaygateapp "github.com/cagojeiger/relaygate/internal/app/relaygate"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "relaygate: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, getenv func(string) string) error {
	configPath, err := parseConfigPath(args, getenv)
	if err != nil {
		return err
	}
	return relaygateapp.Run(configPath, version)
}

func parseConfigPath(args []string, getenv func(string) string) (string, error) {
	configPath := getenv("RELAYGATE_CONFIG")
	if configPath == "" {
		configPath = "relaygate.yaml"
	}
	flags := flag.NewFlagSet("relaygate", flag.ContinueOnError)
	flags.StringVar(&configPath, "config", configPath, "path to RelayGate YAML config")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	return configPath, nil
}
