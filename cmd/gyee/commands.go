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
	"net/rpc"

	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/utils/logging"
)

type Args struct {
	S string
}

var (
	ipcCommand = cli.Command{
		Name:      "ipc",
		Usage:     "gyee ipc test",
		ArgsUsage: "",
		Flags: []cli.Flag{
			cli.StringFlag{Name: "test"},
		},
		Category:    "",
		Description: "",
		Action: func(ctx *cli.Context) error {
			conf := config.GetConfig(ctx)
			client, err := rpc.Dial("unix", conf.IPCEndpoint())
			if err != nil {
				logging.Logger.Info(err)
				return err
			}

			var reply string

			args := Args{S: "test"}
			err = client.Call("JSService.Hello", args, &reply)
			if err != nil {
				logging.Logger.Info(err)
				return err
			}
			logging.Logger.Info(reply)
			return nil
		},
	}
)
