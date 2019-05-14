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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/golang-lru"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
)

const TooFarBlocks = 120

var (
	ErrBlockChainID        = errors.New("block chainID mismatch")
	ErrBlockTooFarForChain = errors.New("block too far for chain head")
)

type sealRequest struct {
	h   uint64
	t   uint64
	txs Transactions
}

func (sr *sealRequest) String() string {
	return fmt.Sprintf("sealReq{H %d txs %d}", sr.h, len(sr.txs))
}

type BlockPool struct {
	core  *Core
	chain *BlockChain

	subscriber *p2p.Subscriber

	// chan for block with valid signature(maybe not enough)
	blockChan chan *Block
	// chan for consensus engine seal request
	sealChan chan *sealRequest

	// cache for confirmed blocks
	cacheNum2Hash *lru.Cache
	cacheHash2Blk *lru.Cache

	// pending block / request
	blockMap map[uint64]*Block
	sealMap  map[uint64]*sealRequest

	syncing int32 // full sync in progress

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewBlockPool(core *Core) (*BlockPool, error) {
	log.Info("Create New BlockPool")
	bp := &BlockPool{
		core:      core,
		chain:     core.blockChain,
		blockChan: make(chan *Block),
		sealChan:  make(chan *sealRequest, 10),
		blockMap:  make(map[uint64]*Block),
		sealMap:   make(map[uint64]*sealRequest),
		quitCh:    make(chan struct{}),
	}
	bp.cacheNum2Hash, _ = lru.New(1024)
	bp.cacheHash2Blk, _ = lru.New(1024)
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

func (bp *BlockPool) AddSealRequest(h, t uint64, txs Transactions) {
	req := &sealRequest{
		h:   h,
		txs: txs,
	}
	bp.sealChan <- req
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
			bp.core.metrics.p2pMsgRecv.Mark(1)
			switch msg.MsgType {
			case p2p.MessageTypeBlock:
				bp.core.metrics.p2pMsgRecvBlk.Mark(1)
				go bp.processMsgBlock(msg)
			case p2p.MessageTypeBlockHeader:
				bp.core.metrics.p2pMsgRecvH.Mark(1)
				go bp.processMsgHeader(msg)
			default:
				log.Crit("unhandled msg sent to blockPool", "msg", msg)
			}
		case b := <-bp.blockChan:
			bp.processVerifiedBlock(b)
		case sealRequest := <-bp.sealChan:
			log.Info("BlockBuilder prepares to seal", "request", sealRequest)
			bp.handleSealRequest(sealRequest)
		}
	}
}

func (bp *BlockPool) processMsgHeader(msg p2p.Message) {
	bp.wg.Add(1)
	defer bp.wg.Done()

	var h = new(corepb.SignedBlockHeader)
	if err := proto.Unmarshal(msg.Data, h); err != nil {
		bp.markBadPeer(msg)
		return
	}
	// TODO:
}

func (bp *BlockPool) processMsgBlock(msg p2p.Message) {
	bp.wg.Add(1)
	defer bp.wg.Done()

	var b = new(Block)
	if err := b.setBytes(msg.Data); err != nil {
		log.Warn("block decode failure", "msg", msg)
		bp.markBadPeer(msg)
		return
	}
	bp.processBlock(b)
}

func (bp *BlockPool) processBlock(blk *Block) {
	if err := bp.chain.verifyBlock(blk, false); err != nil {
		log.Warn("processBlock() verify fails", "err", err)
		// TODO: mark bad peer?
		return
	}
	bp.blockChan <- blk
}

func (bp *BlockPool) processVerifiedBlock(blk *Block) {
	log.Info("processVerifiedBlock", "H", blk.Number(), "hash", blk.Hash(),
		"sigCnt", len(blk.signatureMap), "sigs", blk.signatureMap)
	currHeight := bp.chain.CurrentBlockHeight()
	if blk.Number() <= currHeight {
		bp.handleNewSignature(blk)
		return
	}
	if blk.Number() > currHeight+3 {
		bp.startFullSync()
	}
	if knownBlock, ok := bp.blockMap[blk.Number()]; ok {
		if blk.Hash() != knownBlock.Hash() {
			// TODO:
			log.Crit("fork block!!!")
			return
		}
		_, err := knownBlock.mergeSignature(blk)
		if err != nil {
			log.Warn("failed to merge signature", "blk", knownBlock, "err", err)
			return
		}
		blk = knownBlock
	} else {
		bp.blockMap[blk.Number()] = blk
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
		// block signatures may be checked against lastBlock when received
		if !blk.checkAgainstParent {
			err := bp.chain.verifySignature(blk, true)
			if err != nil {
				log.Warn("verifySignature() verify fails", "blk", blk, "err", err)
				break
			}
		}
		// if signature was enouth
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
		bp.cacheNum2Hash.Add(blk.Number(), blk.Hash())
		bp.cacheHash2Blk.Add(blk.Hash(), blk)
		delete(bp.blockMap, blk.Number())

		currHeight++
		var ok bool
		blk, ok = bp.blockMap[currHeight+1]
		if !ok {
			break
		}
	}
}

func (bp *BlockPool) handleSealRequest(req *sealRequest) {
	currHeight := bp.chain.CurrentBlockHeight()
	switch {
	case req.h <= currHeight:
		// already had this block, ignore
		return
	case req.h > currHeight+1:
		// not next block, maybe tx fetch pending
		// record and wait
		bp.sealMap[req.h] = req
		return
	}

	// req.h == currentHeight + 1 : build next block and any pending req
	for {
		if req.h != currHeight+1 {
			log.Crit("wrong request height", "req", req, "chain", bp.chain)
		}
		currBlock := bp.chain.GetBlockByNumber(currHeight)
		currState, err := bp.chain.StateAt(currBlock.StateRoot())
		if err != nil {
			log.Crit("failed to get state of current block", "err", err)
			break
		}
		// engine output not ordered by nonce
		txs := organizeTxs(currState, req.txs)
		// build next block
		nextBlock, err := bp.chain.BuildNextBlock(currBlock, req.t, txs)
		if err != nil {
			log.Crit("failed to build next block", "parent", currBlock,
				"err", err)
		}
		if err := bp.core.signBlock(nextBlock); err != nil {
			log.Crit("failed to sign block", "err", err)
		}
		log.Info("block sealed", "H", nextBlock.header.Number, "txs", len(nextBlock.transactions), "hash", nextBlock.Hash())
		// merge with received signatures
		if knownBlock, ok := bp.blockMap[nextBlock.Number()]; ok {
			if knownBlock.Hash() != nextBlock.Hash() {
				// TODO:
				log.Crit("fork block!!!")
				return
			}
			if _, err := nextBlock.mergeSignature(knownBlock); err != nil {
				log.Warn("failed to merge signature", "blk", knownBlock, "err", err)
			}
		}
		// insert chain
		if err := bp.chain.AddBlock(nextBlock); err != nil {
			log.Warn("failed to seal block", "err", err)
			break
		}
		bp.cacheNum2Hash.Add(nextBlock.Number(), nextBlock.Hash())
		bp.cacheHash2Blk.Add(nextBlock.Hash(), nextBlock)
		delete(bp.sealMap, currHeight)
		// broadcast block
		if encoded, err := nextBlock.ToBytes(); err != nil {
			log.Warn("failed to encode block", "block", nextBlock, "err", err)
		} else {
			go func(msg p2p.Message) {
				bp.core.metrics.p2pMsgSent.Mark(1)
				if err := bp.core.node.P2pService().BroadcastMessage(msg); err != nil {
					bp.core.metrics.p2pMsgSendFail.Mark(1)
				}
			}(p2p.Message{
				MsgType: p2p.MessageTypeBlock,
				Data:    encoded,
			})
		}

		currHeight++
		var ok bool
		req, ok = bp.sealMap[currHeight+1]
		if !ok {
			break
		}
	}
}

func (bp *BlockPool) handleNewSignature(blk *Block) {
	h := blk.Hash()
	var currBlock *Block
	if cached, ok := bp.cacheHash2Blk.Get(h); ok {
		currBlock = CopyBlock(cached.(*Block))
	} else {
		currBlock = bp.chain.GetBlockByHash(h)
	}
	if currBlock == nil {
		// TODO:
		log.Crit("fork block")
		return
	}
	changed, err := currBlock.mergeSignature(blk)
	if err != nil {
		log.Warn("failed to merge signature", "err", err)
		return
	}
	if changed {
		log.Info("block signature added", "H", currBlock.Number(), "cnt", len(currBlock.signatureMap), "hash", currBlock.Hash())
		bp.cacheHash2Blk.Add(currBlock.Hash(), currBlock)
		// TODO: less disk write
		putHeader(bp.chain.storage, currBlock.pbHeader)
	}
}

func (bp *BlockPool) markBadPeer(msg p2p.Message) {
	// TODO: inform bad peed msg.From to p2p module
}

func (bp *BlockPool) isSyncing() bool {
	return atomic.LoadInt32(&bp.syncing) != 0
}

func (bp *BlockPool) startFullSync() {
	if bp.isSyncing() {
		return
	}
	go bp.syncLoop()
}

func (bp *BlockPool) syncLoop() {
	bp.wg.Add(1)
	defer bp.wg.Done()

	if !atomic.CompareAndSwapInt32(&bp.syncing, 0, 1) {
		// lock not acquired
		return
	}
	defer atomic.StoreInt32(&bp.syncing, 0)
	log.Info("[sync] block pool sync started", "localH", bp.chain.CurrentBlockHeight())

	remoteHeight, err := bp.core.GetRemoteLatestNumber()
	if err != nil {
		log.Warn("failed to get remote height", "err", err)
		return
	}
	log.Info("[sync] remote height", remoteHeight)
	h := bp.chain.CurrentBlockHeight() + 1
	for h <= remoteHeight {
		b, err := bp.core.GetRemoteBlockByNumber(h)
		if err != nil {
			log.Warn("failed to get remote block", "err", err)
			return
		}
		log.Info("[sync] got remote block", "H", b.Number(), "hash", b.Hash())
		bp.processBlock(b)
		h++
	}
	log.Info("[sync] block pool sync finished")
}

func (bp *BlockPool) GetBlockByNumber(number uint64) *Block {
	hash := bp.GetBlockNum2Hash(number)
	if hash == nil {
		return nil
	}
	return bp.GetBlockByHash(*hash)
}

func (bp *BlockPool) GetBlockByHash(hash common.Hash) *Block {
	if cached, ok := bp.cacheHash2Blk.Get(hash); ok {
		b := new(Block)
		*b = *cached.(*Block)
		return b
	}
	return bp.chain.GetBlockByHash(hash)
}

func (bp *BlockPool) GetBlockNum2Hash(number uint64) *common.Hash {
	if cached, ok := bp.cacheNum2Hash.Get(number); ok {
		return cached.(common.Hash).Copy()
	}
	return bp.chain.GetBlockNum2Hash(number)
}
