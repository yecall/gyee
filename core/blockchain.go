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
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
)

var (
	ErrBlockChainNoStorage    = errors.New("core.chain: must provide block chain storage")
	ErrBlockChainIDMismatch   = errors.New("core.chain: chainID mismatch")
	ErrBlockStateTrieMismatch = errors.New("core.chain: trie root hash mismatch")
	ErrBlockParentMissing     = errors.New("core.chain: block parent missing")
	ErrBlockParentMismatch    = errors.New("core.chain: block parent mismatch")
	ErrBlockSignatureMismatch = errors.New("core.chain: block signature mismatch")
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
	putLastBlock(bc.storage, *genesis.Hash())
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
		b.stateTrie, err = bc.StateAt(b.header.StateRoot)
		if err != nil {
			// state root not in storage, try replay txs from parent block
			prevBlk := bc.GetBlockByNumber(b.header.Number - 1)
			if prevBlk == nil {
				return err
			}
			// get state for prev block
			stateTrie, err := bc.StateAt(prevBlk.header.StateRoot)
			if err != nil {
				return err
			}
			// replay txs from prev block
			_, err = bc.replayTxs(stateTrie, b.transactions)
			if err != nil {
				return err
			}
			// check state root hash
			h, err := stateTrie.Commit()
			if err != nil {
				return err
			}
			if h != b.header.StateRoot {
				return ErrBlockStateTrieMismatch
			}
			// all set
			b.stateTrie = stateTrie
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
	// check parent block
	if b.Number() > 0 {
		pb := bc.GetBlockByNumber(b.Number() - 1)
		if pb == nil {
			return ErrBlockParentMissing
		}
		if *pb.Hash() != b.ParentHash() {
			return ErrBlockParentMismatch
		}
	}
	// add to storage
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
	stateTrie, err := state.NewAccountTrie(header.StateRoot, bc.stateDB)
	if err != nil {
		return nil
	}
	consensusTrie, err := state.NewConsensusTrie(header.ConsensusRoot, bc.stateDB)
	if err != nil {
		return nil
	}
	return &Block{
		header:        header,
		signature:     signedHeader,
		body:          body,
		stateTrie:     stateTrie,
		consensusTrie: consensusTrie,
	}
}

// Build Next block from parent block, with transactions
func (bc *BlockChain) BuildNextBlock(parent *Block, t uint64, txs Transactions) (*Block, error) {
	next := &Block{
		header:       CopyHeader(parent.header),
		body:         new(corepb.BlockBody),
		transactions: make(Transactions, 0, len(txs)),
	}
	// next block number
	next.header.Number++
	next.header.ParentHash = *parent.Hash()
	next.header.Time = t

	// state trie
	stateTrie, err := state.NewAccountTrie(parent.StateRoot(), bc.stateDB)
	if err != nil {
		return nil, err
	}
	next.stateTrie = stateTrie

	// consensus trie
	consensusTrie, err := state.NewConsensusTrie(parent.ConsensusRoot(), bc.stateDB)
	if err != nil {
		return nil, err
	}
	next.consensusTrie = consensusTrie

	// iterate txs for state changes
	next.transactions, err = bc.replayTxs(next.stateTrie, txs)
	if err != nil {
		log.Crit("replayTxs", "err", err)
	}

	if err := next.updateBody(); err != nil {
		return nil, err
	}

	if err := next.updateHeader(); err != nil {
		return nil, err
	}
	return next, nil
}

func (bc *BlockChain) replayTxs(stateTrie state.AccountTrie, txs Transactions) (Transactions, error) {
	inBlockTxs := make(Transactions, 0, len(txs))
	for _, tx := range txs {
		if tx.from == nil {
			continue
		}
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

		inBlockTxs = append(inBlockTxs, tx)
	}
	return inBlockTxs, nil
}

func (bc *BlockChain) LastBlock() *Block {
	return bc.lastBlock.Load().(*Block)
}

func (bc *BlockChain) CurrentBlockHeight() uint64 {
	return bc.LastBlock().Number()
}

func (bc *BlockChain) State() (state.AccountTrie, error) {
	root := bc.LastBlock().StateRoot()
	return bc.StateAt(root)
}

func (bc *BlockChain) StateAt(root common.Hash) (state.AccountTrie, error) {
	return state.NewAccountTrie(root, bc.stateDB)
}

func (bc *BlockChain) GetValidators() []string {
	b := bc.LastBlock()
	return b.consensusTrie.GetValidators()
}

// check if block header is valid and belongs to chain
func (bc *BlockChain) verifyHeader(h *BlockHeader) error {
	if ChainID(h.ChainID) != bc.chainID {
		return ErrBlockChainID
	}
	currentHeight := bc.CurrentBlockHeight()
	height := h.Number
	if height+TooFarBlocks < currentHeight || height > currentHeight+TooFarBlocks {
		return ErrBlockTooFarForChain
		// ignore too far block
	}
	// TODO: check if header exists
	return nil
}

// check block header signature, return matched validator count
func (bc *BlockChain) verifySignature(b *Block) (map[common.Address]crypto.Signature, error) {
	prevBlock := bc.GetBlockByNumber(b.Number() - 1)
	if prevBlock == nil {
		return nil, ErrBlockParentMissing
	}
	signers, err := b.Signers()
	if err != nil {
		return nil, err
	}
	// prepare validator set
	validators := make(map[common.Address]*struct{})
	for _, addr := range prevBlock.consensusTrie.GetValidatorAddr() {
		validators[addr] = new(struct{})
	}
	// count valid signatures
	var (
		matched = make(map[common.Address]crypto.Signature)
		unknown = make([]common.Address, 0)
	)
	for signer, signature := range signers {
		if validators[signer] != nil {
			matched[signer] = signature
		} else {
			unknown = append(unknown, signer)
		}
	}
	if len(unknown) > 0 {
		log.Warn("unknown block signature", "block", b,
			"unknown", unknown, "matched", matched, "validators", validators)
	}
	if len(matched) == 0 {
		log.Warn("no valid block signature found", "block", b, "unknown", unknown,
			"validators", validators)
		return nil, ErrBlockSignatureMismatch
	}
	return matched, nil
}

// check if block is valid and belongs to chain
func (bc *BlockChain) verifyBlock(b *Block) (map[common.Address]crypto.Signature, error) {
	// verify block header
	if err := bc.verifyHeader(b.header); err != nil {
		return nil, err
	}
	// verify block body matches header
	if err := b.VerifyBody(); err != nil {
		return nil, err
	}
	// verify block signature
	return bc.verifySignature(b)
}

// check if tx is valid and belongs to chain
func (bc *BlockChain) verifyTx(tx *Transaction) error {
	if ChainID(tx.chainID) != bc.chainID {
		return ErrTxChainID
	}
	return nil
}

func GetStateDB(storage persistent.Storage) state.Database {
	// TODO: stateDB cache
	stateDB := state.NewDatabaseWithCache(
		persistent.NewTable(storage, KeyPrefixStateTrie),
		0)
	return stateDB
}

func (bc *BlockChain) GetChainData(kind string, key []byte) []byte {
	// TODO:
	return nil
}

//非验证节点，是否需要启txPool?
