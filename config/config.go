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

package config

import (
	"fmt"
	"github.com/urfave/cli"
	"github.com/ychain/gyee/utils"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type Config struct {
	Name    string
	DataDir string
	IPCPath string
}

var DefaultConfig = Config{
	Name:    "gyee",
	DataDir: utils.DefaultDataDir(),
	IPCPath: "gyee.ipc",
}

func GetConfig(ctx *cli.Context) *Config {
	config := DefaultConfig
	//TODO: 这个地方如果Flag用了datadir，d形式的alternate，貌似都找不到
	if ctx.GlobalIsSet(DataDirFlag.Name) {
		config.DataDir = ctx.GlobalString(DataDirFlag.Name)
	}
	fmt.Println(config.DataDir)
	return &config
}

func (c *Config) IPCEndpoint() string {
	// Short circuit if IPC has not been enabled
	if c.IPCPath == "" {
		return ""
	}
	// On windows we can only use plain top-level pipes
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(c.IPCPath, `\\.\pipe\`) {
			return c.IPCPath
		}
		return `\\.\pipe\` + c.IPCPath
	}
	// Resolve names into the data directory full paths otherwise
	if filepath.Base(c.IPCPath) == c.IPCPath {
		if c.DataDir == "" {
			return filepath.Join(os.TempDir(), c.IPCPath)
		}
		return filepath.Join(c.DataDir, c.IPCPath)
	}
	return c.IPCPath
}
