/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  the gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  the gyee library is distributed in the hope that it will be useful,
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
	"fmt"
	"github.com/urfave/cli"
	"github.com/ychain/gyee/config"
	"github.com/ychain/gyee/node"
	"log"
	"os"
	"path/filepath"
	"sort"
)

var (
	app = cli.NewApp()
)

func init() {
	app.Name = filepath.Base(os.Args[0])
	app.Author = ""
	app.Email = ""
	app.Version = ""
	app.Usage = "the gyee command line interface"
	app.HideVersion = true
	app.Copyright = "Copyright 2017-2018 The gyee Authors"
	app.Flags = []cli.Flag{
		config.TestnetFlag,
		config.DataDirFlag,
	}
	app.Commands = []cli.Command{
		ConsoleCommand,
	}
	sort.Sort(cli.CommandsByName(app.Commands))
	app.Before = func(ctx *cli.Context) error {
		return nil
	}
	app.After = func(ctx *cli.Context) error {
		return nil
	}
	app.Action = gyee
}

func main() {
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

//gyee is the main entry point
func gyee(ctx *cli.Context) error {
	//start the node
	config := config.GetConfig(ctx)
	nd, _ := node.New(config)
	nd.Start()

	return nil
}
