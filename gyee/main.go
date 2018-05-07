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
/*
1. 命令行及参数处理
2. 创建节点并启动
3. console，寻找已启动节点，ipc连接打开admin console
 */

import (
	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/node"
	"github.com/yeeco/gyee/utils/logging"
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
		logging.Logger.Fatal(err)
	}
}

//gyee is the main entry point
func gyee(ctx *cli.Context) error {
	//start the node
	config := config.GetConfig(ctx)
	nd, err := node.New(config)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	nd.Start()

	return nil
}
