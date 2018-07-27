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
 接收peer发布出来的block header，如果是minor，需要同步块数据
 block验证
 block收到足够数量的签名，即最终确认
 block确认后，在新的高度开始共识计算
 自己签名发布的block也要进入这里？还是直接在blockchain中处理？

*/

package core

import (
	"sync"

	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/utils/logging"
)

type BlockPool struct {
	core       *Core
	subscriber *p2p.Subscriber

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockPool(core *Core) (*BlockPool, error) {
	logging.Logger.Info("Create New BlockPool")
	bp := &BlockPool{
		core:   core,
		quitCh: make(chan struct{}),
	}
	return bp, nil
}

func (bp *BlockPool) Start() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	logging.Logger.Info("BlockPool Start...")

	bp.subscriber = p2p.NewSubscriber(bp, make(chan p2p.Message), p2p.MessageTypeBlock)
	p2p := bp.core.node.P2pService()
	p2p.Register(bp.subscriber)

	go bp.loop()
}

func (bp *BlockPool) Stop() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	logging.Logger.Info("BlockPool Stop...")

	p2p := bp.core.node.P2pService()
	p2p.UnRegister(bp.subscriber)

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
		case msg := <-bp.subscriber.MsgChan:
			logging.Logger.Info("block pool receive ", msg.MsgType, " ", msg.From)
		}
	}
}
