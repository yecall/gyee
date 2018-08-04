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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/urfave/cli"
	"github.com/yeeco/gyee/res"
	"github.com/yeeco/gyee/utils"
)

type Config struct {
	Name    string
	DataDir string
	App     *AppConfig
	P2p     *P2pConfig
	Rpc     *RpcConfig
	Chain   *ChainConfig
	Metrics *MetricsConfig
	Misc    *MiscConfig
}

type AppConfig struct {
	Version           string
	LogLevel          string
	LogFile           string
	EnableCrashReport bool
	CrashReportUrl    string
}

//P2P Config, bootnode, MaxConn, MaxIncoming, MaxOutgoing, Listen Port,..
type P2pConfig struct {
	BootNode []string
}

//Listen addr, modules, access right
type RpcConfig struct {
	IpcPath string
}

//Genesis, ChainId, Keydir, Coinbase, gas...
type ChainConfig struct {
}

//cpu, mem, disk profile,
type MetricsConfig struct {
}

type MiscConfig struct {
}

//var DefaultConfig = Config{
//	Name:    "gyee",
//	DataDir: utils.DefaultDataDir(),
//	Rpc: &RpcConfig{
//		IpcPath: "gyee.ipc",
//	},
//}

func GetConfig(ctx *cli.Context) *Config {
	//config := DefaultConfig
	config := GetDefaultConfig()
	//TODO: 这个地方如果Flag用了datadir，d形式的alternate，貌似都找不到
	if ctx.GlobalIsSet(DataDirFlag.Name) {
		config.DataDir = ctx.GlobalString(DataDirFlag.Name)
	}
	fmt.Println(config.DataDir)
	fmt.Println(config.Name)
	fmt.Println(config.Rpc.IpcPath)
	return config
}

func GetDefaultConfig() *Config {
	var config Config
	config.DataDir = utils.DefaultDataDir()

	cdata, err := res.Asset("config/config.toml")
	if err != nil {
		// Asset was not found.
	}

	if _, err := toml.Decode(string(cdata), &config); err != nil {
		fmt.Println(err)
		return nil
	}

	return &config
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
		if c.DataDir == "" {
			return filepath.Join(os.TempDir(), c.Rpc.IpcPath)
		}
		return filepath.Join(c.DataDir, c.Rpc.IpcPath)
	}
	return c.Rpc.IpcPath
}

func CreateDefaultConfigFile(filename string) {
	//if err := ioutil.WriteFile(filename, []byte(defaultConfig()), 0644); err != nil {
	//
	//}

}
