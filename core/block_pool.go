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
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
)

const TooFarBlocks = 120

var (
	ErrBlockChainID        = errors.New("block chainID mismatch")
	ErrBlockTooFarForChain = errors.New("block too far for chain head")
)

type BlockPool struct {
	core       *Core
	chain      *BlockChain
	subscriber *p2p.Subscriber

	requestMap map[uint64]*Block

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockPool(core *Core) (*BlockPool, error) {
	log.Info("Create New BlockPool")
	bp := &BlockPool{
		core:       core,
		chain:      core.blockChain,
		requestMap: make(map[uint64]*Block),
		quitCh:     make(chan struct{}),
	}
	return bp, nil
}

func (bp *BlockPool) Start() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	log.Info("BlockPool Start...")

	bp.subscriber = p2p.NewSubscriber(bp, make(chan p2p.Message), p2p.MessageTypeBlock)
	bp.core.node.P2pService().Register(bp.subscriber)

	go bp.loop()
}

func (bp *BlockPool) Stop() {
	bp.lock.Lock()
	defer bp.lock.Unlock()
	log.Info("BlockPool Stop...")

	bp.core.node.P2pService().UnRegister(bp.subscriber)

	close(bp.quitCh)
	bp.wg.Wait()
}

func (bp *BlockPool) loop() {
	log.Trace("BlockPool loop...")
	bp.wg.Add(1)
	defer bp.wg.Done()

	for {
		select {
		case <-bp.quitCh:
			log.Info("BlockPool loop end.")
			return
		case msg := <-bp.subscriber.MsgChan:
			log.Trace("block pool receive ", "type", msg.MsgType, "from", msg.From)
			bp.processMsg(msg)
		}
	}
}

func (bp *BlockPool) processMsg(msg p2p.Message) {
	switch msg.MsgType {
	case p2p.MessageTypeBlockHeader:
		var h = new(corepb.SignedBlockHeader)
		if err := proto.Unmarshal(msg.Data, h); err != nil {
			bp.markBadPeer(msg)
			break
		}
		// TODO:
	case p2p.MessageTypeBlock:
		var b = new(Block)
		if err := b.setBytes(msg.Data); err != nil {
			log.Warn("block decode failure", "msg", msg)
			bp.markBadPeer(msg)
			break
		}
		bp.processBlock(b)
	default:
		log.Crit("unhandled msg sent to blockPool", "msg", msg)
	}
}

func (bp *BlockPool) processBlock(blk *Block) {
	sigMap, err := bp.chain.verifyBlock(blk, false)
	if err != nil {
		log.Warn("processBlock() verify fails", "err", err)
		// TODO: mark bad peer?
		return
	}
	currHeight := bp.chain.CurrentBlockHeight()
	if blk.Number() <= currHeight {
		// TODO: refresh in chain block signature
		return
	}
	if knownBlock, ok := bp.requestMap[blk.Number()]; ok {
		if blk.Hash() != knownBlock.Hash() {
			// TODO:
			log.Crit("fork block!!!")
			return
		}
		err := knownBlock.mergeSignature(sigMap)
		if err != nil {
			log.Warn("failed to merge signature", "blk", knownBlock, "err", err)
			return
		}
		blk = knownBlock
	} else {
		blk.signatureMap = sigMap
		bp.requestMap[blk.Number()] = blk
	}

	if blk.Number() > currHeight+1 {
		// not next block, wait
		return
	}

	// blk.Number() == currHeight + 1
	for {
		if blk.Number() != currHeight+1 {
			log.Crit("wrong block height", "blk", blk.Number(), "chain", bp.chain)
		}
		sigCount := len(blk.signatureMap)
		validatorCount := len(bp.chain.LastBlock().ValidatorAddr())
		if sigCount*3 < validatorCount*2 {
			// not enough signature, wait
			break
		}
		log.Info("signature count reached", "H", blk.Number(), "hash", blk.Hash(),
			"sCnt", sigCount, "vCnt", validatorCount)
		if err := bp.core.blockChain.AddBlock(blk); err != nil {
			log.Warn("processBlock() add fail", "err", err)
			return
		}
		delete(bp.requestMap, blk.Number())

		currHeight++
		var ok bool
		blk, ok = bp.requestMap[currHeight+1]
		if !ok {
			break
		}
	}
}

func (bp *BlockPool) markBadPeer(msg p2p.Message) {
	// TODO: inform bad peed msg.From to p2p module
}
