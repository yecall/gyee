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
	"fmt"
	"sync"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/trie"
	"github.com/yeeco/gyee/persistent"
)

// wraps access to state trie, with key length common.AddressLength(20 bytes)
// wraps access to account storage trie, with key length of hash output(32 bytes)
// concurrency is allowed for backing multi account trie in different goroutines
type Database interface {
	// open trie with root hash
	OpenTrie(root common.Hash) (Trie, error)

	// copy whatever inner implementation was used
	CopyTrie(trie Trie) Trie

	// retrieves the backing trie DB
	TrieDB() *trie.Database
}

// interface wrapper for trie.Trie
type Trie interface {
	TryGet(key []byte) ([]byte, error)
	TryUpdate(key, value []byte) error
	TryDelete(key []byte) error
	Commit(onleaf trie.LeafCallback) (common.Hash, error)
	Hash() common.Hash
	NodeIterator(startKey []byte) trie.NodeIterator
}

func NewDatabase(storage persistent.Storage) Database {
	return NewDatabaseWithCache(storage, 0)
}

func NewDatabaseWithCache(storage persistent.Storage, cache int) Database {
	return &cachingDB{
		db: trie.NewDatabaseWithCache(storage, cache),
	}
}

// implements Database
// TODO: cache recent committed trie
type cachingDB struct {
	db *trie.Database

	mu sync.Mutex
}

func (db *cachingDB) OpenTrie(root common.Hash) (Trie, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// TODO: search cache

	tr, err := trie.New(root, db.db)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func (db *cachingDB) CopyTrie(t Trie) Trie {
	switch t := t.(type) {
	case *trie.Trie:
		return t.Copy()
	default:
		panic(fmt.Errorf("unknown trie type %T: %x", t, t))
	}
}

func (db *cachingDB) TrieDB() *trie.Database {
	return db.db
}
