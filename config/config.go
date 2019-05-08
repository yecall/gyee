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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"github.com/yeeco/gyee/res"
	"github.com/yeeco/gyee/utils"
	"github.com/yeeco/gyee/utils/logging"
)

type Config struct {
	Name    string         `toml:"name"`
	NodeDir string         `toml:"node_dir"`
	App     *AppConfig     `toml:"app"`
	P2p     *P2pConfig     `toml:"network"`
	Rpc     *RpcConfig     `toml:"rpc"`
	Chain   *ChainConfig   `toml:"chain"`
	Metrics *MetricsConfig `toml:"metrics"`
	Misc    *MiscConfig    `toml:"misc"`
}

type AppConfig struct {
	LogLevel          string   `toml:"log_level"`
	LogFile           string   `toml:"log_file"`
	EnableCrashReport bool     `toml:"enable_crash_report"`
	CrashReportUrl    []string `toml:"crash_report_url"`
}

//P2P Config, bootnode, MaxConn, MaxIncoming, MaxOutgoing, Listen Port,..
type P2pConfig struct {
	BootNode []string `toml:"bootnode"`
	Listen   []string `toml:"listen"`

	AppType           int      `toml:"app_type"`
	Name              string   `toml:"name"`
	Validator         bool     `toml:"validator"`
	BootstrapNode     bool     `toml:"bootstrap_node"`
	BootstrapNodes    []string `toml:"bootstrap_nodes"`
	DhtBootstrapNodes []string `toml:"dht_bootstrap_nodes"`
	LocalNodeIp       string   `toml:"local_node_ip"`
	LocalUdpPort      uint16   `toml:"local_udp_port"`
	LocalTcpPort      uint16   `toml:"local_tcp_port"`
	LocalDhtIp        string   `toml:"local_dht_ip"`
	LocalDhtPort      uint16   `toml:"local_dht_port"`
	NodeDataDir       string   `toml:"node_data_path"`
	NodeDatabase      string   `toml:"node_database"`
	SubNetMaskBits    int      `toml:"subnet_mask_bits"`
	EvKeepTime        int      `toml:"ev_keep_time"`
	DedupTime         int      `toml:"dedup_time"`
	BootstrapTime     int      `toml:"bootstrap_time"`
	NatType           string   `toml:"nat_type"`
	GatewayIp         string   `toml:"gateway_ip"`
}

//Listen addr, modules, access right
type RpcConfig struct {
	IpcPath    string   `toml:"ipc_path"`
	RpcListen  []string `toml:"rpc_listen"`
	HttpListen []string `toml:"http_listen"`
}

//Genesis, ChainID, Keydir, Coinbase, gas...
type ChainConfig struct {
	ChainID  uint32 `toml:"chain_id"`
	DataDir  string `toml:"data_dir"`
	KeyDir   string `toml:"key_dir"`
	Genesis  string `toml:"genesis"`
	Mine     bool   `toml:"mine"`
	Coinbase string `toml:"coinbase"`
	PwdFile  string `toml:"pwdfile"`
	Key      []byte // raw private key used in unit test
}

//cpu, mem, disk profile,
type MetricsConfig struct {
	EnableMetrics       bool     `toml:"enable_metrics"`
	EnableMetricsReport bool     `toml:"enable_metrics_report"`
	MetricsReportUrl    []string `toml:"metrics_report_url"`
}

type MiscConfig struct {
}

func GetConfig(ctx *cli.Context) *Config {
	config := GetDefaultConfig()

	//这个地方如果Flag用了datadir，d形式的alternate，貌似都找不到，所以取name的第一段
	if ctx.GlobalIsSet(FlagName(NodeConfigFlag.Name)) {
		configFile := ctx.GlobalString(FlagName(NodeConfigFlag.Name))
		GetConfigFromFile(configFile, config)
	}

	if ctx.GlobalIsSet(FlagName(NodeNameFlag.Name)) {
		config.Name = ctx.GlobalString(FlagName(NodeNameFlag.Name))
	}

	if ctx.GlobalIsSet(FlagName(NodeDirFlag.Name)) {
		config.NodeDir = ctx.GlobalString(FlagName(NodeDirFlag.Name))
	}

	//TODO: dir是绝对路径还是相对路径要判断一下并处理

	//Get config of modules
	getAppConfig(ctx, config)
	getNetworkConfig(ctx, config)
	getRpcConfig(ctx, config)
	getChainConfig(ctx, config)
	getMetricsConfig(ctx, config)
	getMiscConfig(ctx, config)

	return config
}

func GetDefaultConfig() *Config {
	var config = &Config{
		NodeDir: utils.DefaultNodeDir(),
	}

	cdata, err := res.Asset("config/config_test.toml")
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatal("Failed to read the default config")
	}

	if _, err := toml.Decode(string(cdata), &config); err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatal("Failed to decode the default config")
		return nil
	}

	return config
}

func GetConfigFromFile(file string, config *Config) *Config {
	cdata, err := ioutil.ReadFile(file)

	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatalf("Failed to read the config file: %s", file)
	}

	if _, err := toml.Decode(string(cdata), config); err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatalf("Failed to decode the config file: %s", file)
	}

	return config
}

func SaveConfigToFile(file string, config *Config) error {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0766)
	defer f.Close()

	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatalf("Failed to write the config file: %s", file)
		return err
	}

	encoder := toml.NewEncoder(f)

	err = encoder.Encode(config)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Fatal("Failed to encode the config")
		return err
	}

	return nil
}

func (c *Config) IPCEndpoint() string {
	// Short circuit if IPC has not been enabled
	if c.Rpc.IpcPath == "" {
		return ""
	}
	// On windows we can only use plain top-level pipes
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(c.Rpc.IpcPath, `\\.\pipe\`) {
			return c.Rpc.IpcPath
		}
		return `\\.\pipe\` + c.Rpc.IpcPath
	}
	// Resolve names into the data directory full paths otherwise
	if filepath.Base(c.Rpc.IpcPath) == c.Rpc.IpcPath {
		if c.NodeDir == "" {
			return filepath.Join(os.TempDir(), c.Rpc.IpcPath)
		}
		return filepath.Join(c.NodeDir, c.Rpc.IpcPath)
	}
	return c.Rpc.IpcPath
}
