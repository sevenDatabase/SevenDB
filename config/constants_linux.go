// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

//go:build linux

package config

// Default on Linux previously pointed to /etc/dicedb which caused confusion for
// local development (needing sudo and hiding generated files like status.json).
// We now default to a relative hidden folder in the working directory unless
// explicitly overridden by environment in the future.
// The variable is still a var so tests or advanced deployments can override it.
var MetadataDir = ".sevendb_meta" // created under CWD (see configureMetadataDir)
