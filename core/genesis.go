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

package core

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/yeeco/gyee/res"
)

type Genesis struct {
	Consensus struct {
		Tetris struct {
			Validators []string
		}
	}
	InitYeeDist []struct {
		Address, Value string
	}
}

func LoadGenesis(id ChainID) (*Genesis, error) {
	switch id {
	case MainNetID:
		return loadGenesis("config/genesis_main.toml")
	case TestNetID:
		return loadGenesis("config/genesis_test.toml")
	default:
		panic(fmt.Errorf("unknown chainID %v", id))
	}
}

func loadGenesis(fn string) (*Genesis, error) {
	data, err := res.Asset(fn)
	if err != nil {
		return nil, err
	}
	genesis := new(Genesis)
	if err := toml.Unmarshal(data, genesis); err != nil {
		return nil, err
	}
	return genesis, nil
}

func NewGenesisBlock() {

}
