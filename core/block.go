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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/core/state"
	sha3 "github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/persistent"
)

// Block Header of yee chain
//
// Encoded with RLP into byte[] for hashing
// stored as value in Storage, with hash as key
type BlockHeader struct {
	// chain
	ChainID    uint32      `json:"chainID"`
	Number     uint64      `json:"number"`
	ParentHash common.Hash `json:"parentHash"`

	// trie root hashes
	ConsensusRoot common.Hash `json:"consensusRoot"`
	StateRoot     common.Hash `json:"stateRoot"`
	TxsRoot       common.Hash `json:"transactionsRoot"`
	ReceiptsRoot  common.Hash `json:"receiptsRoot"`

	// block time in milli seconds
	Time uint64 `json:"timestamp"`

	// extra binary data
	Extra []byte `json:"extraData"`
}

func CopyHeader(header *BlockHeader) *BlockHeader {
	cpy := *header
	return &cpy
}

func (bh *BlockHeader) Hash() ([]byte, error) {
	enc, err := bh.ToBytes()
	if err != nil {
		return nil, err
	}
	return sha3.Sha3256(enc), nil
}

func (bh *BlockHeader) ToBytes() ([]byte, error) {
	return rlp.EncodeToBytes(bh)
}

func (bh *BlockHeader) toSignedProto() (*corepb.SignedBlockHeader, error) {
	enc, err := bh.ToBytes()
	if err != nil {
		return nil, err
	}
	// TODO: bloom signature
	return &corepb.SignedBlockHeader{
		Header: enc,
	}, nil
}

// In-memory representative for the block concept
type Block struct {
	// header
	header    *BlockHeader
	signature *corepb.SignedBlockHeader

	// body
	body *corepb.BlockBody

	stateTrie     state.AccountTrie
	consensusTrie state.ConsensusTrie
	transactions  Transactions
	// TODO: receipts

	// cache
	hash atomic.Value
}

func NewBlock(header *BlockHeader, txs []*Transaction) *Block {
	b := &Block{header: CopyHeader(header)}

	if len(txs) == 0 {
		// TODO: header TxsRoot for empty txs
	} else {
		// TODO: header TxsRoot for txs
		b.transactions = make([]*Transaction, len(txs))
		copy(b.transactions, txs)
	}

	return b
}

func (b *Block) ChainID() uint32         { return b.header.ChainID }
func (b *Block) Number() uint64          { return b.header.Number }
func (b *Block) ParentHash() common.Hash { return b.header.ParentHash }

func (b *Block) StateRoot() common.Hash    { return b.header.StateRoot }
func (b *Block) TxsRoot() common.Hash      { return b.header.TxsRoot }
func (b *Block) ReceiptsRoot() common.Hash { return b.header.ReceiptsRoot }

func (b *Block) Time() uint64  { return b.header.Time }
func (b *Block) Extra() []byte { return b.header.Extra }

func (b *Block) Hash() common.Hash {
	if hash := b.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	bytes, _ := b.header.Hash()
	hash := common.Hash{}
	hash.SetBytes(bytes)
	b.hash.Store(hash)
	return hash
}

func (b *Block) getBody() *corepb.BlockBody {
	return b.body
}

func (b *Block) Seal() {
}

func (b *Block) ToBytes() ([]byte, error) {
	pbSignedHeader, err := b.header.toSignedProto()
	if err != nil {
		return nil, err
	}
	pbBlock := &corepb.Block{
		Header: pbSignedHeader,
		Body:   b.body,
	}
	enc, err := proto.Marshal(pbBlock)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (b *Block) setBytes(enc []byte) error {
	pbBlock := &corepb.Block{}
	if err := proto.Unmarshal(enc, pbBlock); err != nil {
		return err
	}
	header := new(BlockHeader)
	if err := rlp.DecodeBytes(pbBlock.Header.Header, header); err != nil {
		return err
	}
	b.header = header
	b.body = pbBlock.Body
	return nil
}

func (b *Block) Write(putter persistent.Putter) error {
	// commit account state trie
	if b.stateTrie == nil {
		return errors.New("nil stateTrie")
	} else {
		stateRoot, err := b.stateTrie.Commit()
		if err != nil {
			return err
		}
		if stateRoot != b.header.StateRoot {
			return ErrBlockStateTrieMismatch
		}
	}
	// add block header to storage
	pbHeader, err := b.header.toSignedProto()
	if err != nil {
		return err
	}
	hashHeader := putHeader(putter, pbHeader)
	// add block body to storage
	body := b.getBody()
	if body == nil {
		// write empty body if not provided
		body = new(corepb.BlockBody)
	}
	putBlockBody(putter, hashHeader, body)
	// block mapping
	putBlockHash2Num(putter, hashHeader, b.header.Number)
	putBlockNum2Hash(putter, b.header.Number, hashHeader)
	// add block txs to storage, key "tx"+tx.hash
	if err := b.transactions.Write(putter); err != nil {
		return err
	}

	return nil
}

func ParseBlock(enc []byte) (*Block, error) {
	b := new(Block)
	if err := b.setBytes(enc); err != nil {
		return nil, err
	}
	return b, nil
}
