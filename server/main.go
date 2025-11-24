// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
    "path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/internal/auth"
	"github.com/sevenDatabase/SevenDB/internal/cmd"
	"github.com/sevenDatabase/SevenDB/internal/observability"
	"github.com/sevenDatabase/SevenDB/internal/server/ironhawk"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"

	"github.com/sevenDatabase/SevenDB/internal/wal"

	"github.com/sevenDatabase/SevenDB/config"
	diceerrors "github.com/sevenDatabase/SevenDB/internal/errors"
)

func printConfiguration() {
	slog.Info("starting SevenDB", slog.String("version", config.DiceDBVersion))
	slog.Info("running with", slog.String("engine", config.Config.Engine))
	slog.Info("running with", slog.Int("port", config.Config.Port))
	slog.Info("running on", slog.Int("cores", runtime.NumCPU()))

	// Conditionally add the number of shards to be used for DiceDB
	numShards := runtime.NumCPU()
	if config.Config.NumShards > 0 {
		numShards = config.Config.NumShards
	}
	slog.Info("running with", slog.Int("shards", numShards))
}

func printBanner() {
	fmt.Print(`

███████╗███████╗██╗   ██╗███████╗███╗   ██╗  ██████╗ ██████╗
██╔════╝██╔════╝██║   ██║██╔════╝████╗  ██║  ██╔══██╗██╔══██╗
███████╗█████╗  ██║   ██║█████╗  ██╔██╗ ██║  ██║  ██║██████╔╝
╚════██║██╔══╝  ██║   ██║██╔══╝  ██║╚██╗██║  ██║  ██║██╔══██╗
███████║███████╗╚██████╔╝███████╗██║ ╚████║  ██████╔╝██████╔╝
╚══════╝╚══════╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝  ╚═════╝ ╚═════╝

`)
}

const EngineRESP = "resp"
const EngineIRONHAWK = "ironhawk"
const EngineSILVERPINE = "silverpine"

func Start() {
	printBanner()
	printConfiguration()

	// TODO: Handle the addition of the default user
	// and new users in a much better way. Doing this using
	// and empty password check is not a good solution.
	if config.Config.Password != "" {
		user, _ := auth.UserStore.Add(config.Config.Username)
		_ = user.SetPassword(config.Config.Password)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Handle SIGTERM and SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	var (
		serverErrCh = make(chan error, 2)
	)

	// Surface effective WAL directory and detect potential legacy dir mismatches before init
	if config.Config.EnableWAL {
		// Log resolved metadata and wal directories for visibility
		slog.Info("wal configuration",
			slog.String("metadata_dir", config.MetadataDir),
			slog.String("wal_dir", config.Config.WALDir),
		)

		// Best-effort warning for legacy relative "logs" in CWD vs anchored path under metadata dir
		// This helps diagnose cases where users previously wrote to ./logs and now read from .sevendb_meta/logs
		func() {
			cwd, _ := os.Getwd()
			legacy := filepath.Join(cwd, "logs")
			// Only warn when the effective wal_dir differs from the legacy path
			if legacy != config.Config.WALDir {
				// Count seg-*.wal files in each dir without failing startup
				countSegs := func(dir string) int {
					matches, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
					if err != nil {
						return 0
					}
					return len(matches)
				}
				newCnt := countSegs(config.Config.WALDir)
				oldCnt := countSegs(legacy)
				// Warn if the new dir is empty but the legacy dir has data; suggest actions
				if newCnt == 0 && oldCnt > 0 {
					slog.Warn("wal dir is empty but legacy './logs' has segments; database may restore from a different set than expected",
						slog.String("effective_wal_dir", config.Config.WALDir),
						slog.Int("effective_segments", newCnt),
						slog.String("legacy_dir", legacy),
						slog.Int("legacy_segments", oldCnt),
					)
					slog.Info("to use legacy data, either set 'wal-dir' to the legacy absolute path, or move/copy existing seg-*.wal files into the effective wal-dir before restart")
				}
			}
		}()

		// Initialize WAL after visibility logs
		wal.SetupWAL()
	}

	// Get the number of available CPU cores on the machine using runtime.NumCPU().
	// This determines the total number of logical processors that can be utilized
	// for parallel execution. Setting the maximum number of CPUs to the available
	// core count ensures the application can make full use of all available hardware.
	var numShards int
	numShards = runtime.NumCPU()
	if config.Config.NumShards > 0 {
		numShards = config.Config.NumShards
	}

	// The runtime.GOMAXPROCS(numShards) call limits the number of operating system
	// threads that can execute Go code simultaneously to the number of CPU cores.
	// This enables Go to run more efficiently, maximizing CPU utilization and
	// improving concurrency performance across multiple goroutines.
	runtime.GOMAXPROCS(runtime.NumCPU())

	shardManager := shardmanager.NewShardManager(numShards, serverErrCh)
	watchManager := ironhawk.NewWatchManager()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		shardManager.Run(ctx)
	}()

	var serverWg sync.WaitGroup

	if config.EnableProfile {
		stopProfiling, err := startProfiling()
		if err != nil {
			slog.Error("Profiling could not be started", slog.Any("error", err))
			sigs <- syscall.SIGKILL
		}
		defer stopProfiling()
	}

	ioThreadManager := ironhawk.NewIOThreadManager()
	ironhawkServer := ironhawk.NewServer(shardManager, ioThreadManager, watchManager)
	// If emission contract is enabled, swap in the BridgeSender per shard so notifier can deliver.
	if config.Config != nil && config.Config.EmissionContractEnabled {
		bridge := ironhawk.NewBridgeSender(watchManager)
		// Set the same bridge for all shards for now (single Notifier per shard will fan out via WatchManager)
		for i := 0; i < int(shardManager.ShardCount()); i++ {
			shardManager.SetEmissionSender(i, bridge)
		}
	}

	// Restore the database from WAL logs
	if config.Config.EnableWAL {
		slog.Info("restoring database from WAL")
		callback := func(cd *wire.Command) error {
			switch cd.Cmd {
			case "SUBSCRIBE":
				if len(cd.Args) < 3 {
					slog.Warn("skipping malformed SUBSCRIBE entry during WAL replay", slog.Int("args_count", len(cd.Args)))
					return nil
				}
				clientID := cd.Args[0]
				commandStr := cd.Args[1]
				fpStr := cd.Args[2]
				fp, err := strconv.ParseUint(fpStr, 10, 64)
				if err != nil {
					slog.Warn("skipping SUBSCRIBE entry with invalid fingerprint", slog.String("fingerprint", fpStr), slog.Any("error", err))
					return nil
				}
				return watchManager.RestoreSubscription(clientID, commandStr, fp)
			case "UNSUBSCRIBE":
				if len(cd.Args) < 2 {
					slog.Warn("skipping malformed UNSUBSCRIBE entry during WAL replay", slog.Int("args_count", len(cd.Args)))
					return nil
				}
				clientID := cd.Args[0]
				fpStr := cd.Args[1]
				fp, err := strconv.ParseUint(fpStr, 10, 64)
				if err != nil {
					slog.Warn("skipping UNSUBSCRIBE entry with invalid fingerprint", slog.String("fingerprint", fpStr), slog.Any("error", err))
					return nil
				}
				return watchManager.RemoveSubscription(clientID, fp)
			default:
				cmdTemp := cmd.Cmd{C: cd, IsReplay: true}
				_, err := cmdTemp.Execute(shardManager)
				if err != nil {
					// Tolerate expired time-based GETEX PXAT entries whose absolute timestamp
					// is now in the past. During normal execution they were valid, but on
					// replay we re-evaluate with current wall time and the PXAT value can
					// become <= now(), triggering an invalid value error and aborting boot.
					// We treat these as no-ops because the original effect (key existed and
					// had its expiry set in the past) means no additional mutation needs
					// to be applied at restore time.
					if cd.Cmd == "GETEX" && strings.Contains(err.Error(), "PXAT") {
						// Silently skip expired GETEX PXAT entries during WAL replay. Originally this logged a warning
						// per occurrence, but these are expected when absolute expirations have already passed while
						// the server was down. Treat as a no-op with no log noise.
						return nil
					}

					// Similarly, tolerate SET with EXAT/PXAT whose absolute timestamp is already in the past.
					// These can occur if the server was down past the intended expiry time. Treat them as no-ops
					// during WAL replay to avoid aborting restore due to now-invalid absolute timestamps.
					if cd.Cmd == "SET" {
						// Look for EXAT/PXAT options and check if the provided absolute timestamp is in the past.
						isPast := false
						for i := 2; i < len(cd.Args); i++ {
							opt := strings.ToUpper(cd.Args[i])
							switch opt {
							case "EXAT":
								if i+1 < len(cd.Args) {
									if tv, perr := strconv.ParseInt(cd.Args[i+1], 10, 64); perr == nil {
										if tv <= time.Now().Unix() {
											isPast = true
											break
										}
									}
								}
							case "PXAT":
								if i+1 < len(cd.Args) {
									if tv, perr := strconv.ParseInt(cd.Args[i+1], 10, 64); perr == nil {
										if tv <= time.Now().UnixMilli() {
											isPast = true
											break
										}
									}
								}
							}
						}
						if isPast {
							return nil
						}
					}
					return fmt.Errorf("error handling WAL replay: %w", err)
				}
				return nil
			}
		}
		if err := wal.DefaultWAL.ReplayCommand(callback); err != nil {
			slog.Error("error restoring from WAL", slog.Any("error", err))
		}
		slog.Info("database restored from WAL")
	}

	slog.Info("ready to accept connections")

	// Optional HTTP /metrics for Prometheus scraping
	if config.Config != nil && config.Config.MetricsHTTPEnabled {
		mux := http.NewServeMux()
		observability.SetupPrometheus(mux)
		addr := config.Config.MetricsHTTPAddr
		slog.Info("metrics http server starting", slog.String("addr", addr))
		go func() {
			if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics http server exited", slog.Any("error", err))
			}
		}()
	}
	serverWg.Add(1)
	go runServer(ctx, &serverWg, ironhawkServer, serverErrCh)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sigs
		cancel()
	}()

	go func() {
		serverWg.Wait()
		close(serverErrCh) // Close the channel when both servers are done
	}()

	for err := range serverErrCh {
		if err != nil && errors.Is(err, diceerrors.ErrAborted) {
			// if either the AsyncServer/RESPServer or the HTTPServer received an abort command,
			// cancel the context, helping gracefully exiting all servers
			cancel()
		}
	}

	close(sigs)

	if config.Config.EnableWAL {
		wal.TeardownWAL()
	}

	cancel()
	wg.Wait()
}

func runServer(ctx context.Context, wg *sync.WaitGroup, srv *ironhawk.Server, errCh chan<- error) {
	defer wg.Done()
	if err := srv.Run(ctx); err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			slog.Debug(fmt.Sprintf("%T was canceled", srv))
		case errors.Is(err, diceerrors.ErrAborted):
			slog.Debug(fmt.Sprintf("%T received abort command", srv))
		case errors.Is(err, http.ErrServerClosed):
			slog.Debug(fmt.Sprintf("%T received abort command", srv))
		default:
			slog.Error(fmt.Sprintf("%T error", srv), slog.Any("error", err))
		}
		errCh <- err
	} else {
		slog.Debug("bye.")
	}
}
func startProfiling() (func(), error) {
	// Start CPU profiling
	cpuFile, err := os.Create("cpu.prof")
	if err != nil {
		return nil, fmt.Errorf("could not create cpu.prof: %w", err)
	}

	if err = pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		return nil, fmt.Errorf("could not start CPU profile: %w", err)
	}

	// Start memory profiling
	memFile, err := os.Create("mem.prof")
	if err != nil {
		pprof.StopCPUProfile()
		cpuFile.Close()
		return nil, fmt.Errorf("could not create mem.prof: %w", err)
	}

	// Start block profiling
	runtime.SetBlockProfileRate(1)

	// Start execution trace
	traceFile, err := os.Create("trace.out")
	if err != nil {
		runtime.SetBlockProfileRate(0)
		memFile.Close()
		pprof.StopCPUProfile()
		cpuFile.Close()
		return nil, fmt.Errorf("could not create trace.out: %w", err)
	}

	if err := trace.Start(traceFile); err != nil {
		traceFile.Close()
		runtime.SetBlockProfileRate(0)
		memFile.Close()
		pprof.StopCPUProfile()
		cpuFile.Close()
		return nil, fmt.Errorf("could not start trace: %w", err)
	}

	// Return a cleanup function
	return func() {
		// Stop the CPU profiling and close cpuFile
		pprof.StopCPUProfile()
		cpuFile.Close()

		// Write heap profile
		runtime.GC()
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			slog.Warn("could not write memory profile", slog.Any("error", err))
		}

		memFile.Close()

		// Write block profile
		blockFile, err := os.Create("block.prof")
		if err != nil {
			slog.Warn("could not create block profile", slog.Any("error", err))
		} else {
			if err := pprof.Lookup("block").WriteTo(blockFile, 0); err != nil {
				slog.Warn("could not write block profile", slog.Any("error", err))
			}
			blockFile.Close()
		}

		runtime.SetBlockProfileRate(0)

		// Stop trace and close traceFile
		trace.Stop()
		traceFile.Close()
	}, nil
}
