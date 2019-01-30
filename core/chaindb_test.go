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
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/persistent"
)

func TestDBKeys(t *testing.T) {
	if string(keyChainID()) != KeyChainID {
		t.Errorf("wrong keyChainID()")
	}

	num := rand.Uint64()
	keyNum2Hash := keyBlockNum2Hash(num)
	if binary.BigEndian.Uint64(keyNum2Hash[len(keyNum2Hash)-8:]) != num {
		t.Errorf("wrong keyBlockNum2Hash()")
	}
}

func TestChainDB(t *testing.T) {
	mem := persistent.NewMemoryStorage()

	keyNonExist := common.BytesToHash([]byte("test key"))
	if h := getHeader(mem, keyNonExist); h != nil {
		t.Errorf("getHeader() non-exist got %v", h)
	}
	if b := getBlockBody(mem, keyNonExist); b != nil {
		t.Errorf("getBlockBody() non-exist got %v", b)
	}

	// header
	key := putHeader(mem, &corepb.SignedBlockHeader{
		Header: []byte("test header bytes"),
	})
	if h := getHeader(mem, key); h == nil {
		t.Errorf("getHeader() exist got %v", h)
	}

	// body
	putBlockBody(mem, key, &corepb.BlockBody{
		RawTransactions: [][]byte{[]byte("test tx bytes")},
	})
	if b := getBlockBody(mem, key); b == nil {
		t.Errorf("getBlockBody() exist got %v", b)
	}
}
