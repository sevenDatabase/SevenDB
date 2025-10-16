// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	DiceDBVersion = "-"
)

// init initializes the DiceDBVersion variable by reading the
// VERSION file from the project root.
// This function runs automatically when the package is imported.
func init() {
	// Get the absolute path of the current file (config.go)
	// using runtime reflection
	_, currentFile, _, _ := runtime.Caller(0) //nolint:dogsled

	// Navigate up two directories from config.go to reach the project root
	// (config.go is in the config/ directory, so we need to go up twice)
	projectRoot := filepath.Dir(filepath.Dir(currentFile))

	// Read the VERSION file from the project root
	// This approach works regardless of where the program is executed from
	version, err := os.ReadFile(filepath.Join(projectRoot, "VERSION"))
	if err != nil {
		slog.Error("could not read the version file", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Store the version string in the package-level DiceDBVersion variable
	DiceDBVersion = strings.TrimSpace(string(version))

	// Ensure Config is non-nil with default values for tests and simple runs
	if Config == nil {
		Config = initDefaultConfig()
	}
}

var Config *DiceDBConfig

type DiceDBConfig struct {
	Host string `mapstructure:"host" default:"0.0.0.0" description:"the host address to bind to"`
	Port int    `mapstructure:"port" default:"7379" description:"the port to bind to"`

	Username string `mapstructure:"username" default:"dicedb" description:"the username to use for authentication"`
	Password string `mapstructure:"password" default:"" description:"the password to use for authentication"`

	LogLevel string `mapstructure:"log-level" default:"info" description:"the log level"`

	EnableWatch bool `mapstructure:"enable-watch" default:"false" description:"enable support for .WATCH commands and real-time reactivity"`
	MaxClients  int  `mapstructure:"max-clients" default:"20000" description:"the maximum number of clients to accept"`
	NumShards   int  `mapstructure:"num-shards" default:"-1" description:"number of shards to create. defaults to number of cores"`

	Engine string `mapstructure:"engine" default:"ironhawk" description:"the engine to use, values: ironhawk"`

	EnableWAL                   bool   `mapstructure:"enable-wal" default:"false" description:"enable write-ahead logging"`
	WALVariant                  string `mapstructure:"wal-variant" default:"forge" description:"wal variant to use, values: forge"`
	WALDir                      string `mapstructure:"wal-dir" default:"logs" description:"the directory to store WAL segments"`
	WALBufferSizeMB             int    `mapstructure:"wal-buffer-size-mb" default:"1" description:"the size of the wal write buffer in megabytes"`
	WALRotationMode             string `mapstructure:"wal-rotation-mode" default:"time" description:"wal rotation mode to use, values: segment-size, time"`
	WALMaxSegmentSizeMB         int    `mapstructure:"wal-max-segment-size-mb" default:"16" description:"the maximum size of a wal segment file in megabytes before rotation"`
	WALSegmentRotationTimeSec   int    `mapstructure:"wal-max-segment-rotation-time-sec" default:"60" description:"the time interval (in seconds) after which wal a segment is rotated"`
	WALBufferSyncIntervalMillis int    `mapstructure:"wal-buffer-sync-interval-ms" default:"200" description:"the interval (in milliseconds) at which the wal write buffer is synced to disk"`

	// WAL manifest & format enforcement
	WALAutoCreateManifest bool   `mapstructure:"wal-auto-create-manifest" default:"true" description:"auto-create WAL.MANIFEST on startup if missing using detected format"`
	WALRequireUWAL1       bool   `mapstructure:"wal-require-uwal1" default:"false" description:"if true and no manifest, require UWAL1 format; fail startup if legacy/mixed detected"`
	WALManifestEnforce    string `mapstructure:"wal-manifest-enforce" default:"warn" description:"manifest enforcement mode: warn | strict"`

	// Raft / replication flags (MVP – subject to change; kept flat for simple flag binding)
	RaftEnabled                  bool     `mapstructure:"raft-enabled" default:"false" description:"enable raft replication (experimental)"`
	RaftNodes                    []string `mapstructure:"raft-nodes" description:"comma separated raft peer addresses (host:port) for static cluster"`
	RaftHeartbeatMillis          int      `mapstructure:"raft-heartbeat-ms" default:"100" description:"raft heartbeat interval in ms"`
	RaftElectionTimeoutMillis    int      `mapstructure:"raft-election-timeout-ms" default:"1000" description:"raft election timeout base in ms"`
	RaftSnapshotThresholdEntries int      `mapstructure:"raft-snapshot-threshold-entries" default:"10000" description:"create shard snapshot after this many new committed entries"`
	RaftSnapshotThresholdBytes   int      `mapstructure:"raft-snapshot-threshold-bytes" default:"104857600" description:"create shard snapshot if approximate added bytes exceed this"`
	RaftSnapshotIntervalSec      int      `mapstructure:"raft-snapshot-interval-sec" default:"300" description:"force snapshot if this many seconds pass without one"`
	RaftPersistentDir            string   `mapstructure:"raft-persistent-dir" default:"raftdata" description:"base directory for raft logs & snapshots"`
	RaftEngine                   string   `mapstructure:"raft-engine" default:"stub" description:"raft engine implementation: stub | etcd"`
	RaftNodeID                   string   `mapstructure:"raft-node-id" description:"local raft node id (uint64 as string); required for multi-node"`
	RaftListenAddr               string   `mapstructure:"raft-listen-addr" default:":7090" description:"address host:port for local raft gRPC server to listen on"`
	RaftAdvertiseAddr            string   `mapstructure:"raft-advertise-addr" description:"public address host:port other peers use to reach this node; defaults to raft-listen-addr if empty"`

	StatusFilePath string `mapstructure:"status-file-path" description:"optional explicit path for periodic raft status JSON (status.json). If set, writer uses only this path"`
}

func Load(flags *pflag.FlagSet) {
	configureMetadataDir()
	// Prefer sevendb.yaml, fallback to dicedb.yaml for backward compatibility.
	triedSeven := false
	viper.SetConfigType("yaml")
	viper.AddConfigPath(MetadataDir)
	viper.SetConfigName("sevendb")
	if err := viper.ReadInConfig(); err == nil {
		triedSeven = true
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok && err.Error() != "While parsing config: yaml: control characters are not allowed" {
			panic(err)
		}
	}
	if !triedSeven { // attempt dicedb
		viper.SetConfigName("dicedb")
		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok && err.Error() != "While parsing config: yaml: control characters are not allowed" {
				panic(err)
			}
		}
	}

	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Name == "help" {
			return
		}

		// For slice/array flags we must set the underlying []string, not the formatted string.
		// stringSlice and stringArray are distinct in pflag; retrieval helpers differ.
		if flag.Value.Type() == "stringSlice" || flag.Value.Type() == "stringArray" {
			if flag.Changed || !viper.IsSet(flag.Name) {
				var ss []string
				var err error
				if flag.Value.Type() == "stringSlice" {
					ss, err = flags.GetStringSlice(flag.Name)
				} else { // stringArray
					ss, err = flags.GetStringArray(flag.Name)
				}
				if err == nil {
					viper.Set(flag.Name, ss)
				} else {
					viper.Set(flag.Name, flag.Value.String()) // fallback
				}
			}
			return
		}
		// Primitive flags: Only update parsed configs if user set value or viper lacks it
		if flag.Changed || !viper.IsSet(flag.Name) {
			viper.Set(flag.Name, flag.Value.String())
		}
	})

	if err := viper.Unmarshal(&Config); err != nil {
		panic(err)
	}

	// --- Path Normalization ---
	// WALDir is user-configurable and historically defaulted to a relative path ("logs").
	// When the server is started from a different working directory than the one it
	// originally wrote WAL segments to, recovery would look in the new CWD, creating the
	// appearance of data loss / mismatch. To make behavior deterministic we anchor any
	// non-absolute WALDir under the resolved MetadataDir (which itself is normalized in
	// configureMetadataDir). Absolute user-provided paths are respected unchanged.
	if Config.WALDir == "" { // extremely defensive; default tag should set it
		Config.WALDir = "logs"
	}
	if !filepath.IsAbs(Config.WALDir) {
		Config.WALDir = filepath.Join(MetadataDir, Config.WALDir)
	}
	// Ensure the directory exists early so later components can rely on it.
	if err := os.MkdirAll(Config.WALDir, 0o755); err != nil {
		panic(fmt.Errorf("could not create wal-dir '%s': %w", Config.WALDir, err))
	}
	// Debug log for raft-nodes to troubleshoot parsing (only if set)
	if len(Config.RaftNodes) > 0 {
		slog.Info("config loaded raft-nodes", slog.Any("raft-nodes", Config.RaftNodes))
	}
}

// InitConfig initializes the config file.
// If the config file does not exist, it creates a new one.
// If the config file exists, it overwrites the existing config with the new key-values.
// and overwrite should replace the existing config with the new
// key-values and default values.
// If the metadata direcoty is inaccessible, then it uses the current working directory
// as the metadata directory.
func InitConfig(flags *pflag.FlagSet) {
	Load(flags)
	// Write primary config as sevendb.yaml. If legacy dicedb.yaml exists we do not delete it.
	configPath := filepath.Join(MetadataDir, "sevendb.yaml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		err := viper.WriteConfigAs(configPath)
		if err != nil {
			slog.Error("could not write the config file",
				slog.String("path", configPath),
				slog.String("error", err.Error()))
			os.Exit(1)
		}
		slog.Info("config created", slog.String("path", configPath))
	} else {
		if overwrite, _ := flags.GetBool("overwrite"); overwrite {
			err := viper.WriteConfigAs(configPath)
			if err != nil {
				slog.Error("could not write the config file",
					slog.String("path", configPath),
					slog.String("error", err.Error()))
				os.Exit(1)
			}

			// Current behavior - If key changed, then only overwrides.
			// TODO: Ideally, we should have a config-update function that updates the
			// existing config with the new key-values.
			// and overwrite should replace the existing config with the new
			// key-values and default values.
			slog.Info("config overwritten", slog.String("path", configPath))
		} else {
			slog.Info("config already exists. skipping.", slog.String("path", configPath))
			slog.Info("run with --overwrite to overwrite the existing config")
		}
	}
}

// configureMetadataDir creates the default metadata directory to be used
// for DiceDB metadataother persistent data
func configureMetadataDir() {
	// If MetadataDir is not an absolute path, anchor it to current working directory.
	if !filepath.IsAbs(MetadataDir) {
		cwd, _ := os.Getwd()
		MetadataDir = filepath.Join(cwd, MetadataDir)
	}
	if err := os.MkdirAll(MetadataDir, 0o700); err != nil {
		fmt.Printf("could not create metadata directory at %s. error: %s\n", MetadataDir, err)
		fmt.Println("using current directory as metadata directory")
		MetadataDir = "."
	}
}

func initDefaultConfig() *DiceDBConfig {
	defaultConfig := &DiceDBConfig{}
	configType := reflect.TypeOf(*defaultConfig)
	configValue := reflect.ValueOf(defaultConfig).Elem()

	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		value := configValue.Field(i)

		tag := field.Tag.Get("default")
		if tag != "" {
			switch value.Kind() {
			case reflect.String:
				value.SetString(tag)
			case reflect.Int:
				intVal := 0
				_, err := fmt.Sscanf(tag, "%d", &intVal)
				if err == nil {
					value.SetInt(int64(intVal))
				}
			case reflect.Bool:
				boolVal := false
				_, err := fmt.Sscanf(tag, "%t", &boolVal)
				if err == nil {
					value.SetBool(boolVal)
				}
			}
		}
	}

	return defaultConfig
}

func ForceInit(config *DiceDBConfig) {
	defaultConfig := initDefaultConfig()

	configType := reflect.TypeOf(*config)
	configValue := reflect.ValueOf(config).Elem()

	defaultConfigValue := reflect.ValueOf(defaultConfig).Elem()

	for i := 0; i < configType.NumField(); i++ {
		value := configValue.Field(i)
		defaultValue := defaultConfigValue.Field(i)
		// Use IsZero to avoid panicking on comparison of uncomparable types (e.g. slices)
		// Original code compared interfaces which triggers a runtime panic for slices & maps.
		if value.IsZero() {
			value.Set(defaultValue)
		}
	}

	Config = config
}
