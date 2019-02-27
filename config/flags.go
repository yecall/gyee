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
	"strings"
)

var (
	TestnetFlag = cli.BoolFlag{
		Name:  "testnet, t",
		Usage: "test network: pre-configured test network",
	}

	NodeConfigFlag = cli.StringFlag{
		Name:  "config, c",
		Usage: "load configuration from `FILE`",
	}

	NodeNameFlag = cli.StringFlag{
		Name:  "nodename",
		Usage: "gyee node name",
		Value: "MyNode",
	}

	NodeDirFlag = cli.StringFlag{
		Name:  "nodedir, d",
		Usage: "gyee node root directory",
	}

	//AppConfig Flags
	AppFlags = []cli.Flag{
		AppLogLevelFlag,
		AppLogFileFlag,
		AppEnableCrashReportFlag,
		AppCrashReportUrlFlag,
	}

	AppLogLevelFlag = cli.StringFlag{
		Name:  "loglevel",
		Usage: "log level",
	}

	AppLogFileFlag = cli.StringFlag{
		Name:  "logfile",
		Usage: "log file",
	}

	AppEnableCrashReportFlag = cli.BoolFlag{
		Name:  "enable_crash_report",
		Usage: "enable crash report",
	}

	AppCrashReportUrlFlag = cli.StringSliceFlag{
		Name:  "crash_report_url",
		Usage: "crash report url",
	}

	//NetworkConfig Flags
	NetworkFlags = []cli.Flag{
		NetworkBootNodeFlag,
		NetworkListenFlag,
	}

	NetworkBootNodeFlag = cli.StringSliceFlag{
		Name:  "bootnode",
		Usage: "boot node",
	}

	NetworkListenFlag = cli.StringSliceFlag{
		Name:  "p2p_listen",
		Usage: "p2p netowrk listen port",
	}

	//RpcConfig Flags
	RpcFlags = []cli.Flag{
		RpcIpcPathFlag,
		RpcListenFlag,
		RpcHttpListenFlag,
	}

	RpcIpcPathFlag = cli.StringFlag{
		Name:  "ipcpath",
		Usage: "ipc path",
	}

	RpcListenFlag = cli.StringSliceFlag{
		Name:  "rpc_listen",
		Usage: "rpc listen",
	}

	RpcHttpListenFlag = cli.StringSliceFlag{
		Name:  "http_listen",
		Usage: "http listen",
	}

	//ChainConfig Flags
	ChainFlags = []cli.Flag{
		ChainIDFlag,
		ChainDataDirFlag,
		ChainKeyDirFlag,
		ChainGenesisFlag,
		ChainMineFlag,
		ChainCoinbaseFlag,
		ChainPwdFileFlag,
	}

	ChainIDFlag = cli.IntFlag{
		Name:  "chainid",
		Usage: "chain id",
	}

	ChainDataDirFlag = cli.StringFlag{
		Name:  "datadir",
		Usage: "chain data dir",
	}

	ChainKeyDirFlag = cli.StringFlag{
		Name:  "keydir",
		Usage: "key dir",
	}

	ChainGenesisFlag = cli.StringFlag{
		Name:  "genesis",
		Usage: "genesis file path",
	}

	ChainMineFlag = cli.BoolFlag{
		Name:  "mine",
		Usage: "mine",
	}

	ChainCoinbaseFlag = cli.StringFlag{
		Name:  "coinbase",
		Usage: "coinbase address for node",
	}

	ChainPwdFileFlag = cli.StringFlag{
		Name:  "pwdfile",
		Usage: "pwdfile for coinbase keystore",
	}

	//MetricsConfig Flags
	MetricsFlags = []cli.Flag{
		MetricsEnableFlag,
		MetricsEnableReportFlag,
		MetricsReportUrlFlag,
	}

	MetricsEnableFlag = cli.BoolFlag{
		Name:  "metrics_enable",
		Usage: "metrics enable",
	}

	MetricsEnableReportFlag = cli.BoolFlag{
		Name:  "metrics_report",
		Usage: "metrics enable report",
	}

	MetricsReportUrlFlag = cli.StringSliceFlag{
		Name:  "metrics_report_url",
		Usage: "metrics report url",
	}

	//MiscConfig Flags
	MiscFlags = []cli.Flag{}
)

func getAppConfig(ctx *cli.Context, cfg *Config) {
	if cfg.App == nil {
		cfg.App = &AppConfig{}
	}

	if ctx.GlobalIsSet(FlagName(AppLogLevelFlag.Name)) {
		cfg.App.LogLevel = ctx.GlobalString(FlagName(AppLogLevelFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(AppLogFileFlag.Name)) {
		cfg.App.LogFile = ctx.GlobalString(FlagName(AppLogFileFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(AppEnableCrashReportFlag.Name)) {
		cfg.App.EnableCrashReport = ctx.GlobalBool(FlagName(AppEnableCrashReportFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(AppCrashReportUrlFlag.Name)) {
		cfg.App.CrashReportUrl = ctx.GlobalStringSlice(FlagName(AppCrashReportUrlFlag.Name))
	}
}

func getNetworkConfig(ctx *cli.Context, cfg *Config) {
	if cfg.P2p == nil {
		cfg.P2p = &P2pConfig{}
	}

	if ctx.GlobalIsSet(FlagName(NetworkBootNodeFlag.Name)) {
		cfg.P2p.BootNode = ctx.GlobalStringSlice(FlagName(NetworkBootNodeFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(NetworkListenFlag.Name)) {
		cfg.P2p.Listen = ctx.GlobalStringSlice(FlagName(NetworkListenFlag.Name))
	}
}

func getRpcConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Rpc == nil {
		cfg.Rpc = &RpcConfig{}
	}

	if ctx.GlobalIsSet(FlagName(RpcIpcPathFlag.Name)) {
		cfg.Rpc.IpcPath = ctx.GlobalString(FlagName(RpcIpcPathFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(RpcListenFlag.Name)) {
		cfg.Rpc.RpcListen = ctx.GlobalStringSlice(FlagName(RpcListenFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(RpcHttpListenFlag.Name)) {
		cfg.Rpc.HttpListen = ctx.GlobalStringSlice(FlagName(RpcHttpListenFlag.Name))
	}
}

func getChainConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Chain == nil {
		cfg.Chain = &ChainConfig{}
	}

	if ctx.GlobalIsSet(FlagName(ChainIDFlag.Name)) {
		cfg.Chain.ChainID = uint32(ctx.GlobalInt(FlagName(ChainIDFlag.Name)))
	}

	if ctx.GlobalIsSet(FlagName(ChainDataDirFlag.Name)) {
		cfg.Chain.DataDir = ctx.GlobalString(FlagName(ChainDataDirFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(ChainKeyDirFlag.Name)) {
		cfg.Chain.KeyDir = ctx.GlobalString(FlagName(ChainKeyDirFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(ChainGenesisFlag.Name)) {
		cfg.Chain.Genesis = ctx.GlobalString(FlagName(ChainGenesisFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(ChainMineFlag.Name)) {
		cfg.Chain.Mine = ctx.GlobalBool(FlagName(ChainMineFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(ChainCoinbaseFlag.Name)) {
		cfg.Chain.Coinbase = ctx.GlobalString(FlagName(ChainCoinbaseFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(ChainPwdFileFlag.Name)) {
		cfg.Chain.PwdFile = ctx.GlobalString(FlagName(ChainPwdFileFlag.Name))
	}
}

func getMetricsConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Metrics == nil {
		cfg.Metrics = &MetricsConfig{}
	}

	if ctx.GlobalIsSet(FlagName(MetricsEnableFlag.Name)) {
		cfg.Metrics.EnableMetrics = ctx.GlobalBool(FlagName(MetricsEnableFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(MetricsEnableReportFlag.Name)) {
		cfg.Metrics.EnableMetricsReport = ctx.GlobalBool(FlagName(MetricsEnableReportFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(MetricsReportUrlFlag.Name)) {
		cfg.Metrics.MetricsReportUrl = ctx.GlobalStringSlice(FlagName(MetricsReportUrlFlag.Name))
	}
}

func getMiscConfig(ctx *cli.Context, cfg *Config) {
	if cfg.Misc == nil {
		cfg.Misc = &MiscConfig{}
	}
}

func FlagName(name string) string {
	if strings.Contains(name, ",") {
		return strings.Split(name, ",")[0]
	}
	return name
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
