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
	"testing"

	"github.com/yeeco/gyee/persistent"
)

func TestNewBlockChain(t *testing.T) {
	storage, _ := persistent.NewMemoryStorage()
	_, err := NewBlockChain(MainNetID, storage)
	if err != nil {
		t.Fatalf("NewBlockChain %v", err)
	}
}

func TestPrepareStorage(t *testing.T) {
	storage, _ := persistent.NewMemoryStorage()
	if err := prepareStorage(storage, MainNetID); err != nil {
		t.Fatalf("prepareStorage %v", err)
	}
	// reuse storage with test net should fail
	if err := prepareStorage(storage, TestNetID); err != ErrBlockChainIDMismatch {
		t.Fatalf("chainDB didn't detect chainID mismatch")
	}
}

func TestBlockChainStorageCheck(t *testing.T) {
	storage, _ := persistent.NewMemoryStorage()
	if err := prepareStorage(storage, MainNetID); err != nil {
		t.Fatalf("prepareStorage %v", err)
	}
	// reuse storage with test net should fail
	_, err := NewBlockChain(TestNetID, storage)
	if err != ErrBlockChainIDMismatch {
		t.Fatalf("prepareStorage %v", err)
	}
}

// TODO: test for blockchain rejects storage with wrong genesis block

// TODO: test for blockchain generate genesis block if none found in storage
