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
	"bytes"
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/crypto"
	sha3 "github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
)

var (
	EmptyRootHash = DeriveHash(Transactions{})

	ErrBlockBodyTxsMismatch = errors.New("block body txs mismatch")
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

func (b *Block) ConsensusRoot() common.Hash { return b.header.ConsensusRoot }
func (b *Block) StateRoot() common.Hash     { return b.header.StateRoot }
func (b *Block) TxsRoot() common.Hash       { return b.header.TxsRoot }
func (b *Block) ReceiptsRoot() common.Hash  { return b.header.ReceiptsRoot }

func (b *Block) Time() uint64  { return b.header.Time }
func (b *Block) Extra() []byte { return b.header.Extra }

func (b *Block) Hash() *common.Hash {
	if hash := b.hash.Load(); hash != nil {
		return hash.(common.Hash).Copy()
	}
	bytes, _ := b.header.Hash()
	hash := new(common.Hash)
	hash.SetBytes(bytes)
	b.hash.Store(*hash)
	return hash
}

func (b *Block) getBody() *corepb.BlockBody {
	return b.body
}

/*
Update contents of header to match with block body.
State Trie should be already operated to match with txs.
 */
func (b *Block) updateHeader() error {
	if b.header == nil {
		log.Crit("must have header with essential data when sealing", "block", b)
	}
	var err error
	if b.header.StateRoot, err = b.stateTrie.Commit(); err != nil {
		return err
	}
	if b.header.ConsensusRoot, err = b.consensusTrie.Commit(); err != nil {
		return err
	}
	b.header.TxsRoot = DeriveHash(b.transactions)
	if b.signature != nil {
		log.Crit("update signed header")
	}
	b.signature, err = b.header.toSignedProto()
	return err
}

func (b *Block) updateBody() error {
	if len(b.body.RawTransactions) > 0 {
		return errors.New("updating block body which already contains txs")
	}
	// ensure txs encoded in buffer
	if err := b.transactions.encode(); err != nil {
		return err
	}
	rawTxs := make([][]byte, 0, len(b.transactions))
	for _, tx := range b.transactions {
		encoded := make([]byte, len(tx.raw))
		copy(encoded, tx.raw)
		rawTxs = append(rawTxs, encoded)
	}
	b.body.RawTransactions = rawTxs
	return nil
}

func (b *Block) Sign(signer crypto.Signer) error {
	sig, err := signer.Sign(b.Hash()[:])
	if err != nil {
		return err
	}
	for _, s := range b.signature.Signatures {
		if s.SigAlgorithm == uint32(sig.Algorithm) && bytes.Equal(s.Signature, sig.Signature) {
			// signature already exists
			return nil
		}
	}
	pbSig := &corepb.Signature{
		SigAlgorithm:uint32(sig.Algorithm),
		Signature:sig.Signature,
	}
	b.signature.Signatures = append(b.signature.Signatures, pbSig)
	return nil
}

func (b *Block) Signers() (map[common.Address]crypto.Signature, error) {
	result := make(map[common.Address]crypto.Signature)
	signer := secp256k1.NewSecp256k1Signer()
	for _, sig := range b.signature.Signatures {
		sig := crypto.Signature{
			Algorithm: crypto.Algorithm(sig.SigAlgorithm),
			Signature: sig.Signature,
		}
		pubkey, err := signer.RecoverPublicKey(b.Hash().Bytes(), &sig)
		if err != nil {
			return nil, err
		}
		addr, err := address.NewAddressFromPublicKey(pubkey)
		if err != nil {
			return nil, err
		}
		result[*addr.CommonAddress()] = sig
	}
	return result, nil
}

/*
Verify block body match with hash in header
 */
func (b *Block) VerifyBody() error {
	txHash := DeriveHash(b.transactions)
	if txHash != b.header.TxsRoot {
		return ErrBlockBodyTxsMismatch
	}
	for _, tx := range b.transactions {
		if err := tx.VerifySig(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Block) ToBytes() ([]byte, error) {
	pbBlock := &corepb.Block{
		Header: b.signature,
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
	b.signature = pbBlock.Header
	b.body = pbBlock.Body
	b.transactions = make(Transactions, 0, len(b.body.RawTransactions))
	for _, raw := range b.body.RawTransactions {
		tx := new(Transaction)
		if err := tx.Decode(raw); err != nil {
			return err
		}
		tx.raw = raw
		b.transactions = append(b.transactions, tx)
	}
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
