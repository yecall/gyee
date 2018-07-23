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
	"sync"

	"github.com/yeeco/gyee/utils/logging"
)

type BlockChain struct {
	core    *Core
	chainID uint32
	genesis *Block

	blockPool *BlockPool
	txPool *TransactionPool

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockChain(core *Core) (*BlockChain, error) {
	logging.Logger.Info("Create New Blockchain")
	bp, err := NewBlockPool(core)
	if err != nil {

	}

	tp, err := NewTransactionPool(core)
	if err != nil {

	}

	bc := &BlockChain{
		core:    core,
		chainID: 0,
		blockPool: bp,
		txPool:tp,
		quitCh: make(chan struct{}),
	}
	return bc, nil
}

func (b *BlockChain) Start() {
	b.lock.Lock()
	defer b.lock.Unlock()
	logging.Logger.Info("BlockChain Start...")
	b.blockPool.Start()
	b.txPool.Start()

	go b.loop()
}

func (b *BlockChain) Stop() {
	b.lock.Lock()
	defer b.lock.Unlock()
	logging.Logger.Info("BlockChain Stop...")
	close(b.quitCh)
	b.blockPool.Stop()
	b.txPool.Stop()
	b.wg.Wait()
}

func (b *BlockChain) loop() {
	logging.Logger.Info("BlockChain loop...")
	b.wg.Add(1)
	defer b.wg.Done()

	for {
		select {
		case <-b.quitCh:
			logging.Logger.Info("BlockChain loop end.")
			return
		}
	}
}

func (b *BlockChain) CurrentBlockHeight() uint64 {
	return 0
}

func (b *BlockChain) GetValidators() map[string]uint {
	//从state取
	//测试先取一个固定的
	return map[string]uint{
		"aaaa": 1,
		"bbbb": 2,
		"cccc": 3,
		"dddd": 4,
	}
}

//非验证节点，是否需要启txPool?