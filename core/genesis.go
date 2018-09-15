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

	"github.com/yeeco/gyee/res"
	"github.com/BurntSushi/toml"
)

type Genesis struct {
	Consensus struct{Tetris struct{Validators []string}}
	Init_Token_Dist []*struct{Address,Value string}
}

func LoadGenesis() (*Genesis, error) {
	var genesis Genesis
	data, err := res.Asset("config/genesis.toml")
	if err != nil {
		// Asset was not found.
		return nil, err
	}

	if _, err := toml.Decode(string(data), &genesis); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &genesis, nil
}

func NewGenesisBlock() {

}