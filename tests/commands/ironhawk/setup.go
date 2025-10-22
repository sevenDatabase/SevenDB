// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package ironhawk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sevenDatabase/SevenDB/internal/server/ironhawk"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/sevenDatabase/SevenDB/internal/wal"

	"github.com/dicedb/dicedb-go"
	"github.com/sevenDatabase/SevenDB/config"
	derrors "github.com/sevenDatabase/SevenDB/internal/errors"
)

// ensureTestServer starts a singleton test server on the default port if not running.
var (
	testServerOnce sync.Once
)

//nolint:unused
func getLocalConnection() *dicedb.Client {
	// If some tests mutated the port (e.g., watch WAL replay), reset to default for generic tests.
	const defaultPort = 7379
	if config.Config.Port == 0 {
		config.Config.Port = defaultPort
	}

	// First attempt to connect on the current port.
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err == nil {
		return client
	}

	// If connection failed and port isn't the default, switch back to default and try starting the server.
	if config.Config.Port != defaultPort {
		config.Config.Port = defaultPort
	}

	// Start the test server once.
	testServerOnce.Do(func() {
		var wg sync.WaitGroup
		RunTestServer(&wg)
		// Give the server a brief moment to bind and start listening.
		time.Sleep(200 * time.Millisecond)
	})

	// Retry connecting a few times with small backoff.
	var lastErr error
	for i := 0; i < 5; i++ {
		client, lastErr = dicedb.NewClient("localhost", config.Config.Port)
		if lastErr == nil {
			return client
		}
		time.Sleep(200 * time.Millisecond)
	}
	// If still failing, panic to surface a clear error in tests.
	panic(lastErr)
}

func ClosePublisherSubscribers(publisher net.Conn, subscribers []net.Conn) error {
	if err := publisher.Close(); err != nil {
		return fmt.Errorf("error closing publisher connection: %v", err)
	}
	for _, sub := range subscribers {
		time.Sleep(100 * time.Millisecond) // [TODO] why is this needed?
		if err := sub.Close(); err != nil {
			return fmt.Errorf("error closing subscriber connection: %v", err)
		}
	}
	return nil
}

// //nolint:unused
// func unsubscribeFromWatchUpdates(t *testing.T, subscribers []net.Conn, cmd, fingerprint string) {
// 	t.Helper()
// 	for _, subscriber := range subscribers {
// 		rp := fireCommandAndGetRESPParser(subscriber, fmt.Sprintf("%s.UNWATCH %s", cmd, fingerprint))
// 		assert.NotNil(t, rp)
// 		v, err := rp.DecodeOne()
// 		assert.NoError(t, err)
// 		castedValue, ok := v.(string)
// 		if !ok {
// 			t.Errorf("Type assertion to string failed for value: %v", v)
// 		}
// 		assert.Equal(t, castedValue, "OK")
// 	}
// }

// //nolint:unused
// func unsubscribeFromWatchUpdatesSDK(t *testing.T, subscribers []WatchSubscriber, cmd, fingerprint string) {
// 	for _, subscriber := range subscribers {
// 		err := subscriber.watch.Unwatch(context.Background(), cmd, fingerprint)
// 		assert.Nil(t, err)
// 	}
// }

// // deleteTestKeys is a utility to delete a list of keys before running a test
// //
// //nolint:unused
// func deleteTestKeys(keysToDelete []string, store *dstore.Store) {
// 	for _, key := range keysToDelete {
// 		store.Del(key)
// 	}
// }

//nolint:unused
func getLocalSdk() *dicedb.Client {
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		panic(err)
	}
	return client
}

// type WatchSubscriber struct {
// 	client *dicedb.Client
// 	watch  *dicedb.WatchConn
// }

// func ClosePublisherSubscribersSDK(publisher *dicedb.Client, subscribers []WatchSubscriber) error {
// 	if err := publisher.Close(); err != nil {
// 		return fmt.Errorf("error closing publisher connection: %v", err)
// 	}
// 	for _, sub := range subscribers {
// 		if err := sub.watch.Close(); err != nil {
// 			return fmt.Errorf("error closing subscriber watch connection: %v", err)
// 		}
// 		if err := sub.client.Close(); err != nil {
// 			return fmt.Errorf("error closing subscriber connection: %v", err)
// 		}
// 	}
// 	return nil
// }

func RunTestServer(wg *sync.WaitGroup) {
	// Ensure metadata dir exists as early as possible to avoid status writer errors
	_ = os.MkdirAll(config.MetadataDir, 0o700)

	// Always bind test server to an available ephemeral port to avoid collisions with
	// any locally running sevendb instance or parallel test runs using the default port.
	// We probe the OS for a free port using :0 then set config.Config.Port accordingly.
	if config.Config.Port == 0 || true { // force dynamic port for test server
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			addr := ln.Addr().(*net.TCPAddr)
			_ = ln.Close()
			config.Config.Host = "127.0.0.1"
			config.Config.Port = addr.Port
		} else {
			// If ephemeral selection fails, keep existing port but continue; server.Run will report errors.
			slog.Warn("could not choose ephemeral port; falling back to configured port", slog.Any("error", err))
		}
	}
	// #1261: Added here to prevent resp integration tests from failing on lower-spec machines
	gec := make(chan error)
	shardManager := shardmanager.NewShardManager(1, gec)
	ioThreadManager := ironhawk.NewIOThreadManager()
	// Use constructor to initialize internal maps; direct struct literal leaves maps nil and causes panics
	watchManager := ironhawk.NewWatchManager()
	wal.SetupWAL()

	testServer := ironhawk.NewServer(shardManager, ioThreadManager, watchManager)

	ctx, cancel := context.WithCancel(context.Background())
	fmt.Println("Starting the test server on port", config.Config.Port)

	shardManagerCtx, cancelShardManager := context.WithCancel(ctx)
	// Support nil WaitGroup callers by using an internal WaitGroup when not provided.
	effectiveWG := wg
	if effectiveWG == nil {
		effectiveWG = &sync.WaitGroup{}
	}
	effectiveWG.Add(1)
	go func(wgLocal *sync.WaitGroup) {
		defer wgLocal.Done()
		shardManager.Run(shardManagerCtx)
	}(effectiveWG)

	// Start the server in a goroutine
	effectiveWG.Add(1)
	go func(wgLocal *sync.WaitGroup) {
		defer wgLocal.Done()
		if err := testServer.Run(ctx); err != nil {
			if errors.Is(err, derrors.ErrAborted) {
				cancelShardManager()
				return
			}
			slog.Error("Test server encountered an error", slog.Any("error", err))
			os.Exit(1)
		}
	}(effectiveWG)

	go func() {
		for err := range gec {
			if err != nil && errors.Is(err, derrors.ErrAborted) {
				// if either the AsyncServer/RESPServer or the HTTPServer received an abort command,
				// cancel the context, helping gracefully exiting all servers
				cancel()
			}
		}
	}()
}
