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

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/log"
)

type accountTrie struct {
	db      Database
	trie    Trie
	trieErr error

	// cached account object
	accounts map[common.Address]*accountObj
}

// Create account trie with root hash and backing database
func NewAccountTrie(root common.Hash, db Database) (AccountTrie, error) {
	at := &accountTrie{
		db:       db,
		accounts: make(map[common.Address]*accountObj),
	}
	if err := at.Reset(root); err != nil {
		return nil, err
	}
	return at, nil
}

// Reset account trie to root hash
func (at *accountTrie) Reset(root common.Hash) error {
	tr, err := at.db.OpenTrie(root)
	if err != nil {
		return err
	}
	at.trie = tr
	return nil
}

func (at *accountTrie) Commit() (common.Hash, error) {
	for _, account := range at.accounts {
		isDirty := account.dirty
		switch {
		case isDirty:
			at.updateAccount(account)
		}
		account.dirty = false
	}
	root, err := at.trie.Commit(func(leaf []byte, parent common.Hash) error {
		// TODO: mark account trie node reference to parent Hash
		return nil
	})
	if err != nil {
		return common.EmptyHash, err
	}
	if err := at.db.TrieDB().Commit(root, true); err != nil {
		return common.EmptyHash, err
	}
	return root, nil
}

func (at *accountTrie) GetAccount(address common.Address, createIfMissing bool) Account {
	// if already exists
	if existing := at.getAccount(address); existing != nil {
		if existing.deleted {
			panic(fmt.Errorf("got deleted account %v", existing))
		}
		return existing
	}
	// not found
	if !createIfMissing {
		return nil
	}
	// create
	account := newAccount(at, address)
	enc, err := account.ToBytes()
	if err != nil {
		panic("failed to encode empty account")
	}
	if err := at.trie.TryUpdate(address[:], enc); err != nil {
		return nil
	}
	at.accounts[address] = account
	return account
}

//
// trie ops
//

func (at *accountTrie) setTrieErr(err error) {
	if at.trieErr == nil {
		at.trieErr = err
	}
}

func (at *accountTrie) getAccount(address common.Address) *accountObj {
	// try cached first
	if cached := at.accounts[address]; cached != nil {
		if cached.deleted {
			return nil
		}
		return cached
	}

	// not found, load from trie
	enc, err := at.trie.TryGet(address[:])
	if len(enc) == 0 {
		// nothing loaded
		at.setTrieErr(err)
		return nil
	}

	account := newAccount(at, address)
	if err := account.setBytes(enc); err != nil {
		log.Error("failed to decode accountObj", "addr", address, "err", err)
		at.setTrieErr(err)
		return nil
	}
	at.accounts[address] = account
	return account
}

func (at *accountTrie) delAccount(account *accountObj) {
	address := account.Address()[:]
	// check ownership
	if account.trie != at {
		panic(fmt.Errorf("del account %v not in trie %v", address, at))
	}
	// delete from trie
	if err := at.trie.TryDelete(address); err != nil {
		panic(fmt.Errorf("del account %v trie error %x", address, err))

	}
	account.deleted = true
}

func (at *accountTrie) updateAccount(account *accountObj) {
	address := account.Address()[:]
	data, err := account.ToBytes()
	if err != nil {
		panic(fmt.Errorf("failed to encode accountObj %x: %v", address, account))
	}
	at.setTrieErr(at.trie.TryUpdate(address, data))
}
