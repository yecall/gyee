// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"sync"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
)

type sealRequest struct {
	h   uint64
	t   uint64
	txs Transactions
}

type BlockBuilder struct {
	core  *Core
	chain *BlockChain

	requestMap map[uint64]*sealRequest
	sealChan   chan *sealRequest

	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockBuilder(core *Core, chain *BlockChain) (*BlockBuilder, error) {
	bb := &BlockBuilder{
		core:       core,
		chain:      chain,
		requestMap: make(map[uint64]*sealRequest),
		sealChan:   make(chan *sealRequest),
		quitCh:     make(chan struct{}),
	}
	go bb.loop()
	return bb, nil
}

func (bb *BlockBuilder) AddSealRequest(h, t uint64, txs Transactions) {
	req := &sealRequest{
		h:   h,
		txs: txs,
	}
	bb.sealChan <- req
}

func (bb *BlockBuilder) Stop() {
	close(bb.quitCh)
	bb.wg.Wait()
}

func (bb *BlockBuilder) loop() {
	bb.wg.Add(1)
	defer bb.wg.Done()

	for {
		select {
		case <-bb.quitCh:
			log.Info("BlockBuilder loop end.")
			return
		case sealRequest := <-bb.sealChan:
			log.Info("BlockBuilder prepares to seal", "request", sealRequest)
			bb.handleSealRequest(sealRequest)
		}
	}
}

func (bb *BlockBuilder) handleSealRequest(req *sealRequest) {
	currHeight := bb.chain.CurrentBlockHeight()
	switch {
	case req.h <= currHeight:
		// already had this block, ignore
		return
	case req.h > currHeight+1:
		// not next block, maybe tx fetch pending
		// record and wait
		bb.requestMap[req.h] = req
		return
	}

	// req.h == currentHeight + 1 : build next block and any pending req
	for {
		if req.h != currHeight+1 {
			log.Crit("wrong request height", "req", req, "chain", bb.chain)
		}
		currBlock := bb.chain.GetBlockByNumber(currHeight)
		currState, err := bb.chain.StateAt(currBlock.StateRoot())
		if err != nil {
			log.Crit("failed to get state of current block", "err", err)
			break
		}
		// engine output not ordered by nonce
		txs := organizeTxs(currState, req.txs)
		// build next block
		nextBlock, err := bb.chain.BuildNextBlock(currBlock, req.t, txs)
		if err != nil {
			log.Crit("failed to build next block", "parent", currBlock,
				"err", err)
		}
		log.Info("block sealed", "txs", len(req.txs), "hash", nextBlock.Hash())
		// insert chain
		if err := bb.chain.AddBlock(nextBlock); err != nil {
			log.Warn("failed to seal block", "err", err)
			break
		}
		delete(bb.requestMap, currHeight)
		// broadcast block
		if encoded, err := nextBlock.ToBytes(); err != nil {
			log.Warn("failed to encode block", "block", nextBlock, "err", err)
		} else {
			_ = bb.core.node.P2pService().BroadcastMessage(p2p.Message{
				MsgType: p2p.MessageTypeBlock,
				Data:    encoded,
			})
		}

		currHeight++
		var ok bool
		req, ok = bb.requestMap[currHeight+1]
		if !ok {
			break
		}
	}
}

func organizeTxs(state state.AccountTrie, txs Transactions) Transactions {
	if len(txs) < 2 {
		return txs
	}
	var (
		output   Transactions
		nonceMap = make(map[common.Address]uint64)
	)
	for {
		txCount := len(txs)
		var nextRound Transactions

		// sweep txs
		for _, tx := range txs {
			if tx.from == nil {
				// TODO: ignore for now
				log.Warn("tx ignored due to nil from")
				continue
			}
			from := *tx.from
			nonce, ok := nonceMap[from]
			if !ok {
				account := state.GetAccount(from, false)
				if account != nil {
					nonce = account.Nonce()
				} else {
					nonce = 0
				}
				nonceMap[from] = nonce
			}
			switch {
			case tx.nonce == nonce:
				output = append(output, tx)
				nonceMap[from]++
			case tx.nonce > nonce:
				nextRound = append(nextRound, tx)
			default:
				// TODO: ignore for now
				log.Warn("tx nonce too low", "nonce", nonce, "tx", tx)
			}
		}

		// check if we need another round
		txs = nextRound
		if len(txs) == 0 {
			break
		}
		if txCount == len(txs) {
			log.Warn("engine output nonce not possible", "remain", txs)
			break
		}
	}
	return output
}
