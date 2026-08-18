package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/cagojeiger/relaygate/internal/app/config"
	raftmembership "github.com/cagojeiger/relaygate/internal/raft/membership"
)

const defaultMembershipTimeout = 15 * time.Second

type membershipCommand struct {
	action      string
	configPath  string
	nodeID      string
	raftAddress string
	timeout     time.Duration
}

type membershipClient interface {
	List(context.Context) (raftmembership.Result, error)
	Add(context.Context, string, string) (raftmembership.Result, error)
	Remove(context.Context, string) (raftmembership.Result, error)
	Close() error
}

type membershipDialer func(context.Context, string) (membershipClient, error)

func runMembership(args []string, getenv func(string) string, output io.Writer) error {
	return runMembershipWithDial(args, getenv, output, func(ctx context.Context, path string) (membershipClient, error) {
		return raftmembership.Dial(ctx, path)
	})
}

func runMembershipWithDial(args []string, getenv func(string) string, output io.Writer, dial membershipDialer) (resultErr error) {
	command, err := parseMembershipCommand(args, getenv)
	if err != nil {
		return err
	}
	appConfig, err := config.Load(command.configPath)
	if err != nil {
		return err
	}
	if appConfig.Runtime.Role != config.RuntimeRoleController {
		return fmt.Errorf("membership commands require runtime.role=%q", config.RuntimeRoleController)
	}

	ctx, cancel := context.WithTimeout(context.Background(), command.timeout)
	defer cancel()
	client, err := dial(ctx, raftmembership.SocketPath(appConfig.Raft.DataDir))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, client.Close()) }()

	var result raftmembership.Result
	switch command.action {
	case "list":
		result, err = client.List(ctx)
	case "add":
		result, err = client.Add(ctx, command.nodeID, command.raftAddress)
	case "remove":
		result, err = client.Remove(ctx, command.nodeID)
	default:
		return fmt.Errorf("unsupported membership action %q", command.action)
	}
	if err != nil {
		return err
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return fmt.Errorf("write membership result: %w", err)
	}
	return nil
}

func parseMembershipCommand(args []string, getenv func(string) string) (membershipCommand, error) {
	if len(args) == 0 {
		return membershipCommand{}, fmt.Errorf("membership action is required: list, add, or remove")
	}
	command := membershipCommand{
		action:     args[0],
		configPath: getenv("RELAYGATE_CONFIG"),
		timeout:    defaultMembershipTimeout,
	}
	if command.configPath == "" {
		command.configPath = "relaygate.yaml"
	}

	flags := flag.NewFlagSet("relaygate membership "+command.action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&command.configPath, "config", command.configPath, "path to RelayGate YAML config")
	flags.DurationVar(&command.timeout, "timeout", command.timeout, "membership operation timeout")
	switch command.action {
	case "list":
	case "add":
		flags.StringVar(&command.nodeID, "node-id", "", "fresh Raft node ID")
		flags.StringVar(&command.raftAddress, "raft-address", "", "Raft advertise address")
	case "remove":
		flags.StringVar(&command.nodeID, "node-id", "", "Raft node ID to remove")
	default:
		return membershipCommand{}, fmt.Errorf("unknown membership action %q: want list, add, or remove", command.action)
	}
	if err := flags.Parse(args[1:]); err != nil {
		return membershipCommand{}, err
	}
	if flags.NArg() != 0 {
		return membershipCommand{}, fmt.Errorf("membership %s does not accept positional arguments", command.action)
	}
	if command.timeout <= 0 {
		return membershipCommand{}, fmt.Errorf("membership timeout must be positive")
	}
	if (command.action == "add" || command.action == "remove") && command.nodeID == "" {
		return membershipCommand{}, fmt.Errorf("membership %s requires -node-id", command.action)
	}
	if command.action == "add" && command.raftAddress == "" {
		return membershipCommand{}, fmt.Errorf("membership add requires -raft-address")
	}
	return command, nil
}
