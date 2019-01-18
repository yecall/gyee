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
	"testing"
)

func TestLoadGenesis(t *testing.T) {
	genesis, _ := LoadGenesis(MainNetID)

	fmt.Printf("%v\n", genesis)
}

func TestMainNetGenesis(t *testing.T) {
	genesis, err := LoadGenesis(MainNetID)
	if err != nil {
		t.Errorf("failed to decode genesis toml: %v", err)
	}
	if count := len(genesis.Consensus.Tetris.Validators); count != 4 {
		t.Errorf("wrong validator count %v", count)
	}
}

func TestTestNetGenesis(t *testing.T) {
	genesis, err := LoadGenesis(TestNetID)
	if err != nil {
		t.Errorf("failed to decode genesis toml: %v", err)
	}
	if count := len(genesis.Consensus.Tetris.Validators); count != 4 {
		t.Errorf("wrong validator count %v", count)
	}
}
