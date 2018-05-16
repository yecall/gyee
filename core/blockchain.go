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

import "github.com/yeeco/gyee/utils/logging"

type BlockChain struct {
	core *Core
	chainID uint32
	genesis  *Block

}

func NewBlockChain(core *Core) (*BlockChain, error) {
	logging.Logger.Info("Create New Blockchain")
	bc := &BlockChain{
		core: core,
		chainID:0,
	}
	return bc, nil
}

func (b *BlockChain) Start() {
    logging.Logger.Info("BlockChain Start...")
}

func (b *BlockChain) Stop() {
    logging.Logger.Info("BlockChain Stop...")
}

func (b *BlockChain) CurrentBlockHeight() uint64 {
	return 0
}

func (b *BlockChain) GetValidators() map[string] uint {
	//从state取
	//测试先取一个固定的
	return  map[string] uint{
		"aaaa":1,
		"bbbb":2,
		"cccc":3,
		"dddd":4,
	}
}
