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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/consensus"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
)

var (
	ErrBlockChainNoStorage    = errors.New("core.chain: must provide block chain storage")
	ErrBlockChainIDMismatch   = errors.New("core.chain: chainID mismatch")
	ErrBlockStateTrieMismatch = errors.New("core.chain: trie root hash mismatch")
)

// BlockChain is a Data Manager that
//   created with a Storage, for chain trie/data storage
//   created with a Genesis block
//   handles tx / block lookup within the chain
//   check on  block arrival, receive block on signatures confirmation
//   notify sub routines to stop, while wait for them to stop
type BlockChain struct {
	chainID ChainID
	storage persistent.Storage
	stateDB state.Database
	engine  consensus.Engine

	genesis *Block

	lastBlock atomic.Value

	chainmu sync.RWMutex

	stopped int32          // state
	quitCh  chan struct{}  // loop stop notifier
	wg      sync.WaitGroup // sub routine wait group
}

func NewBlockChainWithCore(core *Core) (*BlockChain, error) {
	return NewBlockChain(ChainID(core.config.Chain.ChainID), core.storage, core.engine)
}

func NewBlockChain(chainID ChainID, storage persistent.Storage, engine consensus.Engine) (*BlockChain, error) {
	log.Info("Create New Blockchain")

	// check storage
	if storage == nil {
		return nil, ErrBlockChainNoStorage
	}

	if err := prepareStorage(storage, chainID); err != nil {
		return nil, err
	}

	bc := &BlockChain{
		chainID: chainID,
		storage: storage,
		stateDB: GetStateDB(storage),
		engine:  engine,
		quitCh:  make(chan struct{}),
	}

	bc.genesis = bc.GetBlockByNumber(0)
	if bc.genesis == nil {
		genesis, err := LoadGenesis(chainID)
		if err != nil {
			return nil, err
		}
		bc.genesis, err = genesis.Commit(bc.stateDB, storage)
		if err != nil {
			return nil, err
		}
	}

	if err := bc.loadLastBlock(); err != nil {
		return nil, err
	}

	go bc.loop()
	return bc, nil
}

func (bc *BlockChain) loadLastBlock() error {
	lastHash := getLastBlock(bc.storage)
	if lastHash == common.EmptyHash {
		log.Warn("getLastBlock() empty, reset chain")
		return bc.Reset()
	}
	b := bc.GetBlockByHash(lastHash)
	if b == nil {
		log.Warn("GetBlockByHash() nil, reset chain",
			"hash", lastHash)
		return bc.Reset()
	}
	var err error
	if b.stateTrie, err = state.NewAccountTrie(b.StateRoot(), bc.stateDB); err != nil {
		log.Warn("lastBlock state trie missing",
			"number", b.Number(), "hash", b.Hash())
		if err := bc.repair(&b); err != nil {
			return err
		}
	}
	if b.consensusTrie, err = state.NewConsensusTrie(b.ConsensusRoot(), bc.stateDB); err != nil {
		log.Warn("lastBlock consensus trie missing",
			"number", b.Number(), "hash", b.Hash())
		if err := bc.repair(&b); err != nil {
			return err
		}
	}

	// lastBlock verified
	bc.lastBlock.Store(b)

	log.Info("Loaded local block",
		"number", b.Number(), "Hash", b.Hash())

	return nil
}

// repair broken lastBlock trie, by rewinding through chain
func (bc *BlockChain) repair(head **Block) error {
	for {
		parentHash := (*head).ParentHash()
		if parentHash == common.EmptyHash {
			log.Warn("genesis trie broken, resetting")
			return bc.Reset()
		}
		b := bc.GetBlockByHash(parentHash)
		if b == nil {
			return fmt.Errorf("broken chain %d %x", (*head).Number()-1, parentHash)
		}
		*head = b
		if _, err := state.NewAccountTrie(b.StateRoot(), bc.stateDB); err == nil {
			log.Info("Rewond chain",
				"number", b.Number(), "hash", b.Hash())
			return nil
		}
	}
}

func (bc *BlockChain) Stop() {
	if !atomic.CompareAndSwapInt32(&bc.stopped, 0, 1) {
		// already stopped
		return
	}
	log.Info("BlockChain Stop...")

	close(bc.quitCh)
	bc.wg.Wait()

	// flush caches to storage
}

// reset chain to genesis block
func (bc *BlockChain) Reset() error {
	genesis := bc.genesis

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	if err := genesis.Write(bc.storage); err != nil {
		return err
	}
	putLastBlock(bc.storage, genesis.Hash())
	return nil
}

// store a verified block in chain, not marking as last block
func (bc *BlockChain) storeBlock(b *Block) error {
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	// make chain wait for block commit
	bc.wg.Add(1)
	defer bc.wg.Done()

	if b.stateTrie == nil {
		var err error
		b.stateTrie, err = state.NewAccountTrie(b.header.StateRoot, bc.stateDB)
		if err != nil {
			return err
		}
	}

	batch := bc.storage.NewBatch()

	if err := b.Write(batch); err != nil {
		return err
	}

	// batch writing to storage
	if err := batch.Write(); err != nil {
		return err
	}

	return nil
}

// add a checked block to block chain, as last block
func (bc *BlockChain) AddBlock(b *Block) error {
	if err := bc.storeBlock(b); err != nil {
		return err
	}

	bc.lastBlock.Store(b)

	if engine := bc.engine; engine != nil {
		txs := make([]common.Hash, 0, len(b.transactions))
		for _, tx := range b.transactions {
			txs = append(txs, *tx.Hash())
		}
		engine.OnTxSealed(b.Number(), txs)
	}

	return nil
}

func (bc *BlockChain) GetBlockByNumber(number uint64) *Block {
	hash := getBlockNum2Hash(bc.storage, number)
	if hash == common.EmptyHash {
		return nil
	}
	return bc.GetBlockByHash(hash)
}

func (bc *BlockChain) GetBlockByHash(hash common.Hash) *Block {
	signedHeader := getHeader(bc.storage, hash)
	if signedHeader == nil {
		return nil
	}
	body := getBlockBody(bc.storage, hash)
	if body == nil {
		return nil
	}
	header := new(BlockHeader)
	if err := rlp.DecodeBytes(signedHeader.Header, header); err != nil {
		return nil
	}
	return &Block{
		header:    header,
		signature: signedHeader,
		body:      body,
	}
}

/*
Build Next block from parent block, with transactions
 */
func (bc *BlockChain) BuildNextBlock(parent *Block, txs Transactions) *Block {
	next := &Block{
		header:       CopyHeader(parent.header),
		body:         new(corepb.BlockBody),
		transactions: make(Transactions, 0, len(txs)),
	}
	// next block number
	next.header.Number++

	// state trie
	stateTrie, err := state.NewAccountTrie(parent.StateRoot(), bc.stateDB)
	if err != nil {
		log.Crit("NewAccountTrie for next block", "error", err,
			"parentHash", parent.Hash(), "trieRoot", parent.StateRoot())
	}
	next.stateTrie = stateTrie

	// consensus trie
	consensusTrie, err := state.NewConsensusTrie(parent.ConsensusRoot(), bc.stateDB)
	if err != nil {
		log.Crit("NewConsensusTrie for next block", "error", err,
			"parentHash", parent.Hash(), "trieRoot", parent.ConsensusRoot())
	}
	next.consensusTrie = consensusTrie

	// iterate txs for state changes
	for _, tx := range txs {
		next.transactions = append(next.transactions, tx)
		accountFrom := stateTrie.GetAccount(*tx.from, false)
		if accountFrom == nil {
			// TODO: mark tx failure
			continue
		}
		if accountFrom.Nonce() != tx.nonce {
			// TODO: mark tx failure
			continue
		}
		if accountFrom.Balance().Cmp(tx.amount) < 0 {
			// TODO: mark tx failure
			continue
		}
		accountTo := stateTrie.GetAccount(*tx.to, true)
		// checked, update balance nonce
		accountFrom.AddNonce(1)
		accountFrom.SubBalance(tx.amount)
		accountTo.AddBalance(tx.amount)
	}

	if err := next.UpdateHeader(); err != nil {
		log.Crit("UpdateHeader for next block failed", "error", err)
	}
	return next
}

func (bc *BlockChain) LastBlock() *Block {
	return bc.lastBlock.Load().(*Block)
}

func (bc *BlockChain) loop() {
	log.Info("BlockChain loop...")
	bc.wg.Add(1)
	defer bc.wg.Done()

	for {
		select {
		case <-bc.quitCh:
			log.Info("BlockChain loop end.")
			return
		}
	}
}

func (bc *BlockChain) CurrentBlockHeight() uint64 {
	return bc.LastBlock().Number()
}

func (bc *BlockChain) GetValidators() []string {
	b := bc.LastBlock()
	return b.consensusTrie.GetValidators()
}

func GetStateDB(storage persistent.Storage) state.Database {
	// TODO: stateDB cache
	stateDB := state.NewDatabaseWithCache(
		persistent.NewTable(storage, KeyPrefixStateTrie),
		0)
	return stateDB
}

//非验证节点，是否需要启txPool?
