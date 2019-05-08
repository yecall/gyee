/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package main

import (
	"errors"
	"fmt"

	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
)

var (
	configCommand = cli.Command{
		Name:     "config",
		Usage:    "Manage config",
		Category: "CONFIG COMMANDS",
		Description: `
Manage gyee config, generate a default config file.`,

		Subcommands: []cli.Command{
			{
				Name:      "new",
				Usage:     "Generate a default config file",
				Action:    config.MergeFlags(createDefaultConfig),
				ArgsUsage: "<filename>",
				Description: `
Generate a a default config file.`,
			},
			{
				Name:      "save",
				Usage:     "Generate a default config file",
				Action:    config.MergeFlags(saveConfig),
				ArgsUsage: "<filename>",
				Description: `
Generate a a default config file.`,
			},
		},
	}
)

func saveConfig(ctx *cli.Context) error {
	var err error = nil
	fileName := ctx.Args().First()
	if len(fileName) == 0 {
		fmt.Println("please give a config file arg!!!")
		err = errors.New("please give a config file arg!!!")
	} else {
		node := makeNode(ctx)
		err = config.SaveConfigToFile(fileName, node.Config())
	}
	
	return err
}

func createDefaultConfig(ctx *cli.Context) error {
	fileName := ctx.Args().First()
	if len(fileName) == 0 {
		fmt.Println("please give a config file arg!!!")
		return nil
	}
	//config.CreateDefaultConfigFile(fileName)
	fmt.Printf("create default config %s\n", fileName)
	return nil
}
