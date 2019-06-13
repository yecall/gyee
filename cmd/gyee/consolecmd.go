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
	"context"
	"net"

	"github.com/urfave/cli"
	"github.com/yeeco/gyee/cmd/gyee/console"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/node"
	"google.golang.org/grpc"
)

var (
	consoleCommand = cli.Command{
		Name:        "console",
		Usage:       "Start an interactive JavaScript console",
		Category:    "CONSOLE COMMANDS",
		Description: "",
		Action:      config.MergeFlags(consoleStart),
	}

	attachCommand = cli.Command{
		Name:        "attach",
		Usage:       "Start an interactive JavaScript console to running node",
		Category:    "CONSOLE COMMANDS",
		Description: "",
		Action:      config.MergeFlags(consoleAttach),
	}
)

func consoleStart(ctx *cli.Context) error {
	//node := makeNode(ctx)
	console := console.NewConsole(nil)
	console.Setup()
	console.Interactive()
	defer console.Stop()

	return nil
}

func consoleAttach(ctx *cli.Context) error {
	conf := config.GetConfig(ctx)
	target := conf.IPCEndpoint()

	// grpc connection
	conn, err := grpc.Dial(target, grpc.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, e error) {
			return node.NewIPCConn(ctx, addr)
		}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := console.NewConsole(conn)
	c.Setup()
	defer c.Stop()

	c.Interactive()

	return nil
}
