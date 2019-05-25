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
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/log"
)

const TrieKeyValidators = "Validators"

type consensusTrie struct {
	db      Database
	trie    Trie
	trieErr error
}

func NewConsensusTrie(root common.Hash, db Database) (ConsensusTrie, error) {
	ct := &consensusTrie{
		db: db,
	}
	if err := ct.Reset(root); err != nil {
		return nil, err
	}
	return ct, nil
}

func (ct *consensusTrie) setTrieErr(err error) {
	if ct.trieErr == nil {
		ct.trieErr = err
	}
}

func (ct *consensusTrie) Reset(root common.Hash) error {
	tr, err := ct.db.OpenTrie(root)
	if err != nil {
		return err
	}
	ct.trie = tr
	return nil
}

func (ct *consensusTrie) Root() common.Hash {
	return ct.trie.Hash()
}

func (ct *consensusTrie) Commit() (common.Hash, error) {
	// TODO: write pending data
	root, err := ct.trie.Commit(func(leaf []byte, parent common.Hash) error {
		// TODO: mark node reference
		return nil
	})
	if err != nil {
		return common.EmptyHash, err
	}
	if err := ct.db.TrieDB().Commit(root, reportTriePersistence); err != nil {
		return common.EmptyHash, err
	}
	return root, nil
}

func (ct *consensusTrie) GetValidatorAddr() []common.Address {
	sList := ct.GetValidators()
	if sList == nil {
		return nil
	}
	result := make([]common.Address, len(sList))
	for i, str := range sList {
		addr, err := address.AddressParse(str)
		if err != nil {
			log.Crit("failed to parse address", "str", str, "err", err)
		}
		result[i] = *addr.CommonAddress()
	}
	return result
}

func (ct *consensusTrie) GetValidators() []string {
	enc, err := ct.trie.TryGet([]byte(TrieKeyValidators))
	if err != nil {
		ct.setTrieErr(err)
		return nil
	}
	var result []string
	if err := rlp.DecodeBytes(enc, &result); err != nil {
		ct.setTrieErr(err)
		return nil
	}
	return result
}

func (ct *consensusTrie) SetValidators(validators []string) {
	enc, err := rlp.EncodeToBytes(validators)
	if err != nil {
		log.Crit("SetValidators()", "validators", validators, "err", err)
	}
	ct.setTrieErr(ct.trie.TryUpdate([]byte(TrieKeyValidators), enc))
}
