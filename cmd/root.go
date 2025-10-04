// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/logger"
	"github.com/sevenDatabase/SevenDB/server"
	"github.com/spf13/cobra"
)

func init() {
	flags := rootCmd.PersistentFlags()

	c := config.DiceDBConfig{}
	_type := reflect.TypeOf(c)
	for i := 0; i < _type.NumField(); i++ {
		field := _type.Field(i)
		yamlTag := field.Tag.Get("mapstructure")
		descriptionTag := field.Tag.Get("description")
		defaultTag := field.Tag.Get("default")

		switch field.Type.Kind() {
		case reflect.String:
			flags.String(yamlTag, defaultTag, descriptionTag)
		case reflect.Int:
			val, _ := strconv.Atoi(defaultTag)
			flags.Int(yamlTag, val, descriptionTag)
		case reflect.Bool:
			val, _ := strconv.ParseBool(defaultTag)
			flags.Bool(yamlTag, val, descriptionTag)
		case reflect.Slice:
			// Support []string slice flags (e.g. --raft-nodes). Use StringArray so repeated flags append cleanly.
			if field.Type.Elem().Kind() == reflect.String {
				var defVal []string
				if defaultTag != "" {
					for _, seg := range strings.Split(defaultTag, ",") {
						trim := strings.TrimSpace(seg)
						if trim != "" {
							defVal = append(defVal, trim)
						}
					}
				}
				if len(defVal) == 0 {
					flags.StringArray(yamlTag, []string{}, descriptionTag)
				} else {
					// Cobra lacks direct default for StringArray; fall back to StringSlice for defaults then convert post-load if needed.
					flags.StringSlice(yamlTag, defVal, descriptionTag)
				}
			}
		}
	}
}

var rootCmd = &cobra.Command{
	Use:     "sevendb",
	Aliases: []string{"dicedb"}, // backward compatibility
	Short:   "SevenDB - an in-memory database;",
	Run: func(cmd *cobra.Command, args []string) {
		config.Load(cmd.Flags())
		slog.SetDefault(logger.New())
		server.Start()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
