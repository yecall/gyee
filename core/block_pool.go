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

type BlockPool struct {
	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockPool() (*BlockPool, error) {
	logging.Logger.Info("Create New BlockPool")
	bp := &BlockPool{

	}
	return bp, nil
}


func (bp *BlockPool) Start() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	logging.Logger.Info("BlockPool Start...")
	go bp.loop()
}

func (bp *BlockPool) Stop() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	logging.Logger.Info("BlockPool Stop...")
	close(bp.quitCh)
	bp.wg.Wait()
}

func (bp *BlockPool) loop() {
	logging.Logger.Info("BlockPool loop...")
	bp.wg.Add(1)
	defer bp.wg.Done()

	for {
		select {
		case <-bp.quitCh:
			logging.Logger.Info("BlockPool loop end.")
			return
		}
	}
}
