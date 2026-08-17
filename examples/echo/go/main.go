package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	relaygate "github.com/cagojeiger/relaygate/sdk/go"
)

const (
	defaultAddress   = "127.0.0.1:27420"
	defaultClientID  = "local-development"
	defaultAPIKeyID  = "primary"
	defaultEndpoint  = "/examples/echo"
	operationTimeout = 12 * time.Second
)

var errEchoPipeEnded = errors.New("echo pipe ended")

type command struct {
	name    string
	target  string
	message string
}

type settings struct {
	address  string
	clientID string
	apiKeyID string
	apiKey   string
	endpoint string
}

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "relaygate echo: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string, getenv func(string) string) error {
	cmd, err := parseCommand(arguments)
	if err != nil {
		return err
	}
	configuration, err := loadSettings(getenv)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd.name {
	case "serve":
		return serve(ctx, configuration, cmd.target)
	case "send":
		return send(ctx, configuration, cmd.target, []byte(cmd.message))
	default:
		return fmt.Errorf("unsupported command %q", cmd.name)
	}
}

func parseCommand(arguments []string) (command, error) {
	if len(arguments) < 2 {
		return command{}, errors.New("usage: relaygate-echo serve <target> | send <target> <message>")
	}
	if !validTarget(arguments[1]) {
		return command{}, errors.New("target must contain only letters, digits, '.', '-', or '_'")
	}
	switch arguments[0] {
	case "serve":
		if len(arguments) != 2 {
			return command{}, errors.New("usage: relaygate-echo serve <target>")
		}
		return command{name: "serve", target: arguments[1]}, nil
	case "send":
		if len(arguments) < 3 {
			return command{}, errors.New("usage: relaygate-echo send <target> <message>")
		}
		message := strings.Join(arguments[2:], " ")
		if message == "" {
			return command{}, errors.New("message must not be empty")
		}
		return command{name: "send", target: arguments[1], message: message}, nil
	default:
		return command{}, fmt.Errorf("unknown command %q", arguments[0])
	}
}

func validTarget(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '.' && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func loadSettings(getenv func(string) string) (settings, error) {
	configuration := settings{
		address:  valueOrDefault(getenv("RELAYGATE_ECHO_ADDRESS"), defaultAddress),
		clientID: valueOrDefault(getenv("RELAYGATE_ECHO_CLIENT_ID"), defaultClientID),
		apiKeyID: valueOrDefault(getenv("RELAYGATE_ECHO_API_KEY_ID"), defaultAPIKeyID),
		apiKey:   getenv("RELAYGATE_ECHO_API_KEY"),
		endpoint: valueOrDefault(getenv("RELAYGATE_ECHO_ENDPOINT"), defaultEndpoint),
	}
	if configuration.apiKey == "" {
		return settings{}, errors.New("RELAYGATE_ECHO_API_KEY is required")
	}
	return configuration, nil
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func connect(parent context.Context, configuration settings) (*relaygate.Client, error) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	defer cancel()
	client, err := relaygate.Connect(ctx, relaygate.NewConfig(
		configuration.address,
		configuration.clientID,
		configuration.apiKeyID,
		configuration.apiKey,
	).WithInsecureLocal())
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return client, nil
}

func serve(ctx context.Context, configuration settings, target string) (result error) {
	client, err := connect(ctx, configuration)
	if err != nil {
		return err
	}

	bindCtx, cancelBind := context.WithTimeout(ctx, operationTimeout)
	listener, err := client.Bind(bindCtx, configuration.endpoint, target)
	cancelBind()
	if err != nil {
		result = fmt.Errorf("bind: %w", err)
		if closeErr := closeClient(client); closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close client: %w", closeErr))
		}
		return result
	}
	defer func() {
		unbindCtx, cancelUnbind := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
		defer cancelUnbind()
		if unbindErr := listener.Unbind(unbindCtx); unbindErr != nil {
			result = errors.Join(result, fmt.Errorf("unbind: %w", unbindErr))
		}
		if closeErr := closeClient(client); closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close client: %w", closeErr))
		}
	}()

	fmt.Printf("ECHO_READY %s\n", target)
	for {
		offer, nextErr := listener.Next(ctx)
		if nextErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("next offer: %w", nextErr)
		}
		acceptCtx, cancelAccept := context.WithTimeout(ctx, operationTimeout)
		pipe, acceptErr := offer.Accept(acceptCtx)
		cancelAccept()
		if acceptErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", acceptErr)
		}
		if echoErr := echoPipe(ctx, pipe); echoErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(echoErr, errEchoPipeEnded) {
				continue
			}
			return echoErr
		}
	}
}

func echoPipe(ctx context.Context, pipe *relaygate.Pipe) error {
	for {
		payload, err := pipe.Recv(ctx)
		if err != nil {
			if errors.Is(err, relaygate.ErrPipeClosed) {
				return errEchoPipeEnded
			}
			return fmt.Errorf("receive payload: %w", err)
		}
		sendCtx, cancelSend := context.WithTimeout(ctx, operationTimeout)
		err = pipe.Send(sendCtx, payload)
		cancelSend()
		if err != nil {
			return fmt.Errorf("echo payload: %w", err)
		}
	}
}

func send(ctx context.Context, configuration settings, target string, message []byte) error {
	client, err := connect(ctx, configuration)
	if err != nil {
		return err
	}
	clientClosed := false
	defer func() {
		if !clientClosed {
			_ = closeClient(client)
		}
	}()

	openCtx, cancelOpen := context.WithTimeout(ctx, operationTimeout)
	pipe, err := client.Open(openCtx, configuration.endpoint, target)
	cancelOpen()
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	sendCtx, cancelSend := context.WithTimeout(ctx, operationTimeout)
	err = pipe.Send(sendCtx, message)
	cancelSend()
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}

	receiveCtx, cancelReceive := context.WithTimeout(ctx, operationTimeout)
	reply, err := pipe.Recv(receiveCtx)
	cancelReceive()
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}
	if !bytes.Equal(reply, message) {
		return fmt.Errorf("reply %q does not match message %q", reply, message)
	}

	closeCtx, cancelClose := context.WithTimeout(ctx, operationTimeout)
	err = pipe.Close(closeCtx)
	cancelClose()
	if err != nil {
		return fmt.Errorf("close pipe: %w", err)
	}
	terminalTimer := time.NewTimer(operationTimeout)
	defer terminalTimer.Stop()
	select {
	case <-pipe.Done():
	case <-ctx.Done():
		return ctx.Err()
	case <-terminalTimer.C:
		return errors.New("observe pipe terminal: timeout")
	}
	clientClosed = true
	if err := closeClient(client); err != nil {
		return fmt.Errorf("close client: %w", err)
	}
	fmt.Printf("ECHO_REPLY %s\n", reply)
	return nil
}

func closeClient(client *relaygate.Client) error {
	if client == nil {
		return nil
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		return err
	case <-time.After(operationTimeout):
		return errors.New("close client: timeout")
	}
}
