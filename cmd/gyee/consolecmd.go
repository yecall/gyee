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
	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/gyee/console"
)

var (
	consoleCommand = cli.Command{
		Name:        "console",
		Usage:       "Start an interactive JavaScript console",
		Category:    "CONSOLE COMMANDS",
		Description: "",
		Action:      config.MergeFlags(consoleStart),
	}
)

func consoleStart(ctx *cli.Context) error {
	//node := makeNode(ctx)
	console := console.NewConsole()
	console.Setup()
	console.Interactive()
	defer console.Stop()

	return nil
}

//TODO:console如何与node连接上，ipc
