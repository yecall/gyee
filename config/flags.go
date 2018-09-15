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

package config

import (
	"github.com/urfave/cli"
)

var (
	TestnetFlag = cli.BoolFlag{
		Name:  "testnet, t",
		Usage: "test network: pre-configured proof-of-work test network",
	}

	NodeNameFlag = cli.StringFlag{
		Name:  "nodename",
		Usage: "gyee node name",
		Value: "MyNode",
	}

	NodeDirFlag = cli.StringFlag{
		Name:  "nodedir",
		Usage: "gyee node root directory",
	}

	//AppConfig Flag


)

func getAppConfig(ctx *cli.Context, cfg *Config) {
	if cfg.App == nil {
		cfg.App = &AppConfig{}
	}
}

func getNetworkConfig(ctx *cli.Context, cfg *Config) {
	if cfg.P2p == nil {
		cfg.P2p = &P2pConfig{}
	}
}

func getRpcConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Rpc == nil {
		cfg.Rpc = &RpcConfig{}
	}
}

func getChainConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Chain == nil {
		cfg.Chain = &ChainConfig{}
	}
}

func getMetricsConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Metrics == nil {
		cfg.Metrics = &MetricsConfig{}
	}
}

func getMiscConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Misc == nil {
		cfg.Misc = &MiscConfig{}
	}
}


func MergeFlags(action func(ctx *cli.Context) error) func(*cli.Context) error {
	return func(ctx *cli.Context) error {
		for _, name := range ctx.FlagNames() {
			if ctx.IsSet(name) {
				ctx.GlobalSet(name, ctx.String(name))
			}
		}
		return action(ctx)
	}
}
