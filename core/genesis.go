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
	"math/big"

	"github.com/BurntSushi/toml"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/persistent"
	"github.com/yeeco/gyee/res"
)

type Genesis struct {
	ChainID   ChainID
	Time      int64
	Extra     string
	Consensus struct {
		Tetris struct {
			Validators []string
		}
	}
	InitYeeDist []struct {
		Address, Value string
	}
	// block header hash generated with info above
	Hash string
}

func LoadGenesis(id ChainID) (*Genesis, error) {
	switch id {
	case MainNetID:
		return loadGenesis(id, "config/genesis_main.toml")
	case TestNetID:
		return loadGenesis(id, "config/genesis_test.toml")
	default:
		panic(fmt.Errorf("unknown chainID %v", id))
	}
}

func loadGenesis(id ChainID, fn string) (*Genesis, error) {
	data, err := res.Asset(fn)
	if err != nil {
		return nil, err
	}
	genesis := new(Genesis)
	if err := toml.Unmarshal(data, genesis); err != nil {
		return nil, err
	}
	genesis.ChainID = id
	return genesis, nil
}

func (g *Genesis) genBlock(stateDB state.Database) (*Block, error) {
	if stateDB == nil {
		// mem storage needs no cache in state.Database
		stateDB = state.NewDatabase(persistent.NewMemoryStorage())
	}
	accountTrie, err := state.NewAccountTrie(common.EmptyHash, stateDB)
	if err != nil {
		return nil, err
	}
	for _, dist := range g.InitYeeDist {
		addr, err := AddressParse(dist.Address)
		if err != nil {
			return nil, err
		}
		value, ok := new(big.Int).SetString(dist.Value, 0)
		if !ok {
			return nil, fmt.Errorf("failed to parse value %v", dist.Value)
		}
		account := accountTrie.GetAccount(addr.CommonAddress(), true)
		account.SetBalance(value)
	}
	consensusTrie, err := state.NewConsensusTrie(common.EmptyHash, stateDB)
	if err != nil {
		return nil, err
	}
	consensusTrie.SetValidators(g.Consensus.Tetris.Validators)
	h := &BlockHeader{
		ChainID: uint32(g.ChainID),
	}
	if h.StateRoot, err = accountTrie.Commit(); err != nil {
		return nil, err
	}
	if h.ConsensusRoot, err = consensusTrie.Commit(); err != nil {
		return nil, err
	}
	b := NewBlock(h, nil)
	b.stateTrie = accountTrie
	b.consensusTrie = consensusTrie
	return b, nil
}

// commit genesis to stateDB, assuming service not started, no lock required
func (g *Genesis) Commit(stateDB state.Database, putter persistent.Putter) (*Block, error) {
	b, err := g.genBlock(stateDB)
	if err != nil {
		return nil, err
	}
	if err := b.Write(putter); err != nil {
		return nil, err
	}
	putLastBlock(putter, b.Hash())
	return b, nil
}

func NewGenesisBlock() {

}
