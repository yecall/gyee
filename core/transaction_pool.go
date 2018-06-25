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

 /*
 接收tx
 验证
 提交给共识模块
 拉取tx

  */

package core

import (
	"sync"

	"github.com/yeeco/gyee/utils/logging"
)

type TransactionPool struct {
	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewTransactionPool() (*TransactionPool, error) {
	logging.Logger.Info("Create New TransactionPool")
	bp := &TransactionPool{
		quitCh: make(chan struct{}),
	}
	return bp, nil
}

func (tp *TransactionPool) Start() {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	logging.Logger.Info("TransactionPool Start...")
	go tp.loop()
}

func (tp *TransactionPool) Stop() {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	logging.Logger.Info("TransactionPool Stop...")
	close(tp.quitCh)
	tp.wg.Wait()
}

func (tp *TransactionPool) loop() {
	logging.Logger.Info("TransactionPool loop...")
	tp.wg.Add(1)
	defer tp.wg.Done()

	for {
		select {
		case <-tp.quitCh:
			logging.Logger.Info("TransactionPool loop end.")
			return
		}
	}
}
