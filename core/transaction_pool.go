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
	"errors"
	"sync"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
)

const TooFarTx = 8192

var (
	ErrTxChainID = errors.New("transaction chainID mismatch")
)

type TransactionPool struct {
	core       *Core
	subscriber *p2p.Subscriber

	// requesting tx hash pool
	reqPool map[common.Hash]struct{}

	// pending tx pool
	pendingPool map[common.Hash]*Transaction

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewTransactionPool(core *Core) (*TransactionPool, error) {
	log.Info("Create New TransactionPool")
	bp := &TransactionPool{
		core:        core,
		reqPool:     make(map[common.Hash]struct{}),
		pendingPool: make(map[common.Hash]*Transaction),
		quitCh:      make(chan struct{}),
	}
	return bp, nil
}

func (tp *TransactionPool) Start() {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	log.Info("TransactionPool Start...")

	tp.subscriber = p2p.NewSubscriber(tp, make(chan p2p.Message), p2p.MessageTypeTx)
	tp.core.node.P2pService().Register(tp.subscriber)

	go tp.loop()
}

func (tp *TransactionPool) Stop() {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	log.Info("TransactionPool Stop...")

	tp.core.node.P2pService().UnRegister(tp.subscriber)

	close(tp.quitCh)
	tp.wg.Wait()
}

func (tp *TransactionPool) loop() {
	log.Trace("TransactionPool loop...")
	tp.wg.Add(1)
	defer tp.wg.Done()

	for {
		select {
		case <-tp.quitCh:
			log.Info("TransactionPool loop end.")
			return
		case msg := <-tp.subscriber.MsgChan:
			//log.Info("tx pool receive ", msg.MsgType, " ", msg.From)
			tp.processMsg(msg)
		}
	}
}

func (tp *TransactionPool) processMsg(msg p2p.Message) {
	switch msg.MsgType {
	case p2p.MessageTypeTx:
		var tx = new(Transaction)
		if err := tx.Decode(msg.Data); err != nil {
			tp.markBadPeer(msg)
			break
		}
		tp.processTx(tx)
	default:
		log.Crit("unhandled msg sent to txPool", "msg", msg)
	}
}

func (tp *TransactionPool) processTx(tx *Transaction) {
	// validate tx integrity
	if err := tp.core.blockChain.verifyTx(tx); err != nil {
		log.Warn("processTx() verify fails", "err", err, "tx", tx)
		// TODO: mark bad peer?
		return
	}
	if err := tx.VerifySig(); err != nil {
		log.Warn("tx sig verify failed", "err", err)
		// TODO: mark bad peer?
		return
	}

	// search in-mem request, if we are requesting for this tx
	if _, ok := tp.reqPool[*tx.hash]; ok {
		delete(tp.reqPool, *tx.hash)
		tp.pendingPool[*tx.hash] = tx

		// TODO: check if block can be sealed
		return
	}

	// search chain, if tx has been sealed
	// this may not be sufficient, legacy tx may be dropped from storage
	// in such cases a nonce check would cover
	if hasTransaction(tp.core.storage, *tx.Hash()) {
		// TODO: mark bad peer?
		return
	}

	// basic check tx
	//  nonce not too far
	account := tp.core.blockChain.LastBlock().stateTrie.GetAccount(*tx.from, false)
	if account == nil {
		log.Warn("ignore tx for non-exist account", "tx", tx)
		// TODO: mark bad peer?
		return
	}
	currNonce := account.Nonce()
	if (currNonce > tx.nonce) || (currNonce+TooFarTx < tx.nonce) {
		log.Warn("tx nonce too far", "nonce", currNonce, "tx", tx)
		// TODO: mark bad peer?
		return
	}

	// put tx to DHT
	// TODO:

	// send tx to consensus
	if tp.core.engine != nil {
		tp.core.engine.SendTx(*tx.Hash())
	}
}

func (tp *TransactionPool) TxBroadcast(tx *Transaction) {
	data, err := tx.Encode()
	if err != nil {
		log.Error("TxBroadcast encode", "err", err)
		return
	}
	go func(msg p2p.Message) {
		err = tp.core.node.P2pService().BroadcastMessage(msg)
		if err != nil {
			log.Error("TxBroadcast", "err", err)
		}
	}(p2p.Message{
		MsgType: p2p.MessageTypeTx,
		From:    "node1",
		Data:    data,
	})
}

func (tp *TransactionPool) markBadPeer(msg p2p.Message) {
	// TODO: inform bad peed msg.From to p2p module
}
