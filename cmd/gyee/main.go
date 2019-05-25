// Copyright (C) 2017 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/node"
	"github.com/yeeco/gyee/utils/fdlimit"
	"github.com/yeeco/gyee/utils/logging"
	"github.com/yeeco/gyee/version"
)

var (
	app = cli.NewApp()
)

func init() {
	app.Name = filepath.Base(os.Args[0])
	app.Author = ""
	app.Email = ""
	app.Version = version.Version
	app.Usage = "The gyee command line interface"
	app.HideVersion = true
	app.Copyright = "Copyright 2017-2018 The gyee Authors"
	app.Flags = []cli.Flag{
		config.TestnetFlag,
		config.NodeConfigFlag,
		config.NodeNameFlag,
		config.NodeDirFlag,
	}
	app.Flags = append(app.Flags, config.AppFlags...)
	app.Flags = append(app.Flags, config.NetworkFlags...)
	app.Flags = append(app.Flags, config.RpcFlags...)
	app.Flags = append(app.Flags, config.ChainFlags...)
	app.Flags = append(app.Flags, config.MetricsFlags...)
	app.Flags = append(app.Flags, config.MiscFlags...)
	sort.Sort(cli.FlagsByName(app.Flags))

	app.Commands = []cli.Command{
		consoleCommand,
		attachCommand,
		configCommand,
		accountCommand,
		licenseCommand,
		versionCommand,
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
	runtime.GOMAXPROCS(runtime.NumCPU())

	if err := app.Run(os.Args); err != nil {
		logging.Logger.Fatal(err)
	}
}

//gyee is the main entry point
func gyee(ctx *cli.Context) error {
	//create and start the node
	//logging.Logger.SetLevel(logrus.WarnLevel)

	conf := config.GetConfig(ctx)
	if err := fdlimit.FixFdLimit(); err != nil {
		log.Error("failed to update fd limit", err)
	}

	n, err := node.NewNode(conf)
	if err != nil {
		logging.Logger.Fatal(err)
	}

	if err := n.Start(); err != nil {
		return err
	}

	n.WaitForShutdown()
	return nil
}
