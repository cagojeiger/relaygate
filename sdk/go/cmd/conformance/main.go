package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	relaygate "github.com/cagojeiger/relaygate/sdk/go"
)

const (
	defaultRelayAddress = "127.0.0.1:27420"
	stageTimeout        = 12 * time.Second
	processTimeout      = 45 * time.Second
)

type settings struct {
	role         string
	caseName     string
	endpoint     string
	target       string
	relayAddress string
	clientID     string
	apiKeyID     string
	apiKey       string
}

func main() {
	watchdog := time.AfterFunc(processTimeout, func() {
		_, _ = fmt.Fprintln(os.Stderr, "conformance process timeout")
		os.Exit(124)
	})
	defer watchdog.Stop()

	configuration, err := loadSettings()
	if err == nil {
		switch configuration.role {
		case "listener":
			err = runListener(configuration)
		case "caller":
			err = runCaller(configuration)
		default:
			err = fmt.Errorf("unsupported role %q", configuration.role)
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "SDK_FAIL %s: %v\n", configuration.caseName, err)
		os.Exit(1)
	}
	fmt.Printf("SDK_PASS %s\n", configuration.caseName)
}

func loadSettings() (settings, error) {
	configuration := settings{
		role:         os.Getenv("RELAYGATE_SDK_ROLE"),
		caseName:     os.Getenv("RELAYGATE_SDK_CASE"),
		target:       "exact",
		relayAddress: os.Getenv("RELAYGATE_SDK_RELAY_ADDRESS"),
		clientID:     os.Getenv("RELAYGATE_SDK_CLIENT_ID"),
		apiKeyID:     os.Getenv("RELAYGATE_SDK_API_KEY_ID"),
		apiKey:       os.Getenv("RELAYGATE_SDK_API_KEY"),
	}
	if configuration.role == "" || configuration.caseName == "" || configuration.clientID == "" || configuration.apiKeyID == "" || configuration.apiKey == "" {
		return configuration, errors.New("role, case, client ID, API key ID, and API key are required")
	}
	if !safeCaseName(configuration.caseName) {
		return configuration, errors.New("case must contain only lowercase letters, digits, or hyphens")
	}
	if configuration.role != "listener" && configuration.role != "caller" {
		return configuration, errors.New("role must be listener or caller")
	}
	if configuration.relayAddress == "" {
		configuration.relayAddress = defaultRelayAddress
	}
	if !loopbackAddress(configuration.relayAddress) {
		return configuration, errors.New("relay address must be a loopback host and port")
	}
	configuration.endpoint = "/sdk/conformance/" + configuration.caseName
	return configuration, nil
}

func loopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return false
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return false
	}
	if host == "localhost" || host == "localhost." {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func safeCaseName(value string) bool {
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return value != ""
}

func runListener(configuration settings) error {
	client, err := connect(configuration)
	if err != nil {
		return err
	}
	defer func() { _ = closeClient(client) }()

	var listener *relaygate.Listener
	if err := withStage("bind", func(ctx context.Context) error {
		var bindErr error
		listener, bindErr = client.Bind(ctx, configuration.endpoint, configuration.target)
		return bindErr
	}); err != nil {
		return err
	}
	fmt.Printf("SDK_READY %s\n", configuration.caseName)

	var offer *relaygate.Offer
	if err := withStage("next offer", func(ctx context.Context) error {
		var nextErr error
		offer, nextErr = listener.Next(ctx)
		return nextErr
	}); err != nil {
		return err
	}
	if offer.AttemptID() == "" || offer.ListenerID() != listener.ID() || offer.CallerSessionID() == "" || offer.Endpoint() != configuration.endpoint || offer.Target() != configuration.target {
		return fmt.Errorf("offer metadata did not match the exact binding")
	}

	var pipe *relaygate.Pipe
	if err := withStage("accept", func(ctx context.Context) error {
		var acceptErr error
		pipe, acceptErr = offer.Accept(ctx)
		return acceptErr
	}); err != nil {
		return err
	}
	for index, expected := range callerFrames(configuration.caseName) {
		if err := withStage(fmt.Sprintf("receive caller frame %d", index+1), func(ctx context.Context) error {
			payload, receiveErr := pipe.Recv(ctx)
			if receiveErr != nil {
				return receiveErr
			}
			if !bytes.Equal(payload, expected) {
				return fmt.Errorf("caller frame %d = %q, want %q", index+1, payload, expected)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for index, payload := range listenerFrames(configuration.caseName) {
		if err := withStage(fmt.Sprintf("send listener frame %d", index+1), func(ctx context.Context) error {
			return pipe.Send(ctx, payload)
		}); err != nil {
			return err
		}
	}
	if err := waitPipeTerminal(pipe); err != nil {
		return err
	}
	if err := withStage("unbind", listener.Unbind); err != nil {
		return err
	}
	if err := closeClient(client); err != nil {
		return err
	}
	client = nil
	return nil
}

func runCaller(configuration settings) error {
	client, err := connect(configuration)
	if err != nil {
		return err
	}
	defer func() { _ = closeClient(client) }()

	var pipe *relaygate.Pipe
	if err := withStage("open", func(ctx context.Context) error {
		var openErr error
		pipe, openErr = client.Open(ctx, configuration.endpoint, configuration.target)
		return openErr
	}); err != nil {
		return err
	}
	for index, payload := range callerFrames(configuration.caseName) {
		if err := withStage(fmt.Sprintf("send caller frame %d", index+1), func(ctx context.Context) error {
			return pipe.Send(ctx, payload)
		}); err != nil {
			return err
		}
	}
	for index, expected := range listenerFrames(configuration.caseName) {
		if err := withStage(fmt.Sprintf("receive listener frame %d", index+1), func(ctx context.Context) error {
			payload, receiveErr := pipe.Recv(ctx)
			if receiveErr != nil {
				return receiveErr
			}
			if !bytes.Equal(payload, expected) {
				return fmt.Errorf("listener frame %d = %q, want %q", index+1, payload, expected)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := withStage("close pipe", pipe.Close); err != nil {
		return err
	}
	if err := waitPipeTerminal(pipe); err != nil {
		return err
	}
	if err := closeClient(client); err != nil {
		return err
	}
	client = nil
	return nil
}

func connect(configuration settings) (*relaygate.Client, error) {
	var client *relaygate.Client
	err := withStage("connect", func(ctx context.Context) error {
		config := relaygate.NewConfig(configuration.relayAddress, configuration.clientID, configuration.apiKeyID, configuration.apiKey).WithInsecureLocal()
		var connectErr error
		client, connectErr = relaygate.Connect(ctx, config)
		return connectErr
	})
	return client, err
}

func withStage(name string, operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), stageTimeout)
	defer cancel()
	if err := operation(ctx); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func waitPipeTerminal(pipe *relaygate.Pipe) error {
	ctx, cancel := context.WithTimeout(context.Background(), stageTimeout)
	defer cancel()
	select {
	case <-pipe.Done():
		return nil
	case <-ctx.Done():
		return fmt.Errorf("observe pipe terminal: %w", ctx.Err())
	}
}

func closeClient(client *relaygate.Client) error {
	if client == nil {
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- client.Close() }()
	select {
	case err := <-result:
		if err != nil {
			return fmt.Errorf("close client: %w", err)
		}
		return nil
	case <-time.After(stageTimeout):
		return errors.New("close client: stage timeout")
	}
}

func callerFrames(caseName string) [][]byte {
	return [][]byte{
		[]byte("caller-frame-1:" + caseName),
		[]byte("caller-frame-2:" + caseName),
	}
}

func listenerFrames(caseName string) [][]byte {
	return [][]byte{
		[]byte("listener-frame-1:" + caseName),
		[]byte("listener-frame-2:" + caseName),
	}
}
