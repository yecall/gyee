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

package trie

import (
	"testing"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/persistent"
)

func newEmpty() *Trie {
	memStorage, _ := persistent.NewMemoryStorage()
	trie, _ := New(common.Hash{}, NewDatabase(memStorage))
	return trie
}

func TestEmptyRootHash(t *testing.T) {
	res := common.BytesToHash(hash.Sha3256([]byte{0x80}))
	exp := emptyRoot
	if res != exp {
		t.Errorf("expected %x got %x", exp, res)
	}
}
