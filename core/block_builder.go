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

	"github.com/yeeco/gyee/log"
)

type sealRequest struct {
	h   uint64
	txs Transactions
}

type BlockBuilder struct {
	chain *BlockChain

	requestMap map[uint64]*sealRequest
	sealChan   chan *sealRequest

	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockBuilder(chain *BlockChain) (*BlockBuilder, error) {
	bb := &BlockBuilder{
		chain:    chain,
		sealChan: make(chan *sealRequest),
		quitCh:   make(chan struct{}),
	}
	go bb.loop()
	return bb, nil
}

func (bb *BlockBuilder) AddSealRequest(h uint64, txs Transactions) {
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
		nextBlock := bb.chain.BuildNextBlock(currBlock, req.txs)
		if err := bb.chain.AddBlock(nextBlock); err != nil {
			log.Warn("failed to seal block", "err", err)
			break
		}
		delete(bb.requestMap, currHeight)

		currHeight++
		var ok bool
		req, ok = bb.requestMap[currHeight+1]
		if ! ok {
			break
		}
	}
}
