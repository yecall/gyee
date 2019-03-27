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

package core

import (
	"bytes"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/trie"
)

// DerivableList can be add to a trie for hashing, getting a proof to verify the list
type DerivableList interface {
	Len() int
	GetEncoded(index int) []byte
}

// Add list elements to a in-mem trie, with index as key,
// trie root hash is returned.
func DeriveHash(list DerivableList) common.Hash {
	keybuf := new(bytes.Buffer)
	t := new(trie.Trie)
	for i := 0; i < list.Len(); i++ {
		keybuf.Reset()
		_ = rlp.Encode(keybuf, uint(i))
		t.Update(keybuf.Bytes(), list.GetEncoded(i))
	}
	return t.Hash()
}
