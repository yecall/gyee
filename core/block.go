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
	"sync/atomic"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/core/state"
)

// Block Header of yee chain
// Encoded with RLP into byte[] for hashing
// stored as value in Storage, with hash as key
type BlockHeader struct {
	// chain
	ChainID    uint32      `json:"chainID"`
	Number     uint64      `json:"number"`
	ParentHash common.Hash `json:"parentHash"`

	// trie root hashes
	StateRoot    common.Hash `json:"stateRoot"`
	TxsRoot      common.Hash `json:"transactionsRoot"`
	ReceiptsRoot common.Hash `json:"receiptsRoot"`

	// block time in milli seconds
	Time int64 `json:"timestamp"`

	// extra binary data
	Extra []byte `json:"extraData"`
}

// In-memory representative for the block concept
type Block struct {
	// header
	header    *BlockHeader
	signature *corepb.SignedBlockHeader

	stateTrie    *state.AccountTrie
	transactions []*Transaction
	// TODO: receipts

	// cache
	hash atomic.Value
}

func (b *Block) Seal() {
}
