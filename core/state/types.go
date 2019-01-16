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

package state

import (
	"math/big"

	"github.com/yeeco/gyee/common"
)

// interface for single account
// 1. cache existing account data in memory
// 2. cache created / updated account, also providing such operations
// 3. TODO: handle account storage trie
// NO CONCURRENCY is allowed
type Account interface {
	// account address
	Address() *common.Address

	// account transaction nonce start from 0
	Nonce() uint64

	// account balance in minimum unit
	Balance() big.Int

	// binary representation for account used as trie value
	ToBytes() ([]byte, error)
}

// interface for account trie
// 1. cache existing account trie in memory
// 2. cache created / updated trie, also providing such operations
// NO CONCURRENCY is allowed
type AccountTrie interface {
	// Reset trie to a trie root hash
	Reset(root common.Hash) error

	// Commit trie to backing storage
	Commit() (root common.Hash, err error)

	// Get account from trie, create if requested
	GetAccount(address common.Address, createIfMissing bool) Account
}
