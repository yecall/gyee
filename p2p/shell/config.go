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


package shell

import (
	"github.com/yeeco/gyee/p2p/config"
)

//
// Get default configuration
//
func ShellDefaultConfig() *config.Config {
	return config.P2pDefaultConfig()
}

//
// Set configuration
//
func ShellSetConfig(name string, cfg *config.Config) (string, config.P2pCfgErrno) {
	return config.P2pSetConfig(name, cfg)
}

//
// Get configuration
//
func ShellGetConfig(name string) *config.Config {
	return config.P2pGetConfig(name)
}
