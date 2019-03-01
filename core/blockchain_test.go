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
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"testing"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/persistent"
)

func TestNewBlockChain(t *testing.T) {
	storage := persistent.NewMemoryStorage()
	_, err := NewBlockChain(MainNetID, storage, nil)
	if err != nil {
		t.Fatalf("NewBlockChain %v", err)
	}
}

func TestPrepareStorage(t *testing.T) {
	storage := persistent.NewMemoryStorage()
	if err := prepareStorage(storage, MainNetID); err != nil {
		t.Fatalf("prepareStorage %v", err)
	}
	// reuse storage with test net should fail
	if err := prepareStorage(storage, TestNetID); err != ErrBlockChainIDMismatch {
		t.Fatalf("chainDB didn't detect chainID mismatch")
	}
}

func TestBlockChainStorageCheck(t *testing.T) {
	storage := persistent.NewMemoryStorage()
	if err := prepareStorage(storage, MainNetID); err != nil {
		t.Fatalf("prepareStorage %v", err)
	}
	// reuse storage with test net should fail
	_, err := NewBlockChain(TestNetID, storage, nil)
	if err != ErrBlockChainIDMismatch {
		t.Fatalf("prepareStorage %v", err)
	}
}

func TestBlockChainGrow(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "yee-chain-test")
	if err != nil {
		t.Fatalf("TempDir() %v", err)
	}
	storage, err := persistent.NewLevelStorage(tmpDir)
	if err != nil {
		t.Fatalf("newDB() %v", err)
	}
	chain, err := NewBlockChain(TestNetID, storage, nil)
	if err != nil {
		t.Fatalf("newChain() %v", err)
	}

	lastBlock := chain.LastBlock()
	trie := lastBlock.stateTrie
	if trie == nil {
		t.Fatalf("nil stateTrie")
	}

	account0, err := address.AddressParse("0105cfa04d12fb46fcea51d22cf1f340631bbe930dc0e026ba21")
	if err != nil {
		t.Fatalf("AddressParse %v", err)
	}
	addrFrom := account0.CommonAddress()
	amount := big.NewInt(1)

	nonce := uint64(0)
	for i := 0; i < 100; i++ {
		var txs Transactions
		for j := 0; j < 100; j++ {
			tx := new(Transaction)
			tx.from = addrFrom
			tx.to = new(common.Address)
			tx.to[0] = byte(i)
			tx.to[1] = byte(j)
			tx.amount = amount
			tx.nonce = nonce
			nonce++

			txs = append(txs, tx)
		}
		lastBlock = chain.BuildNextBlock(lastBlock, txs)
		if err := chain.AddBlock(lastBlock); err != nil {
			t.Fatalf("AddBlock() %v", err)
		}
	}
	fmt.Printf("end")
	chain.Stop()
}

func benchAddBlock(b *testing.B, storage persistent.Storage, cnt int) {
	if err := prepareStorage(storage, TestNetID); err != nil {
		b.Fatalf("prepareStorage() failed %v", err)
	}
	chain, err := NewBlockChain(TestNetID, storage, nil)
	if err != nil {
		b.Fatalf("NewBlockChain() failed %v", err)
	}
	genesis := chain.GetBlockByNumber(0)
	if genesis == nil {
		b.Fatalf("missing genesis")
	}

	hash := genesis.Hash()
	for n := int(1); n < cnt; n++ {
		header := &BlockHeader{
			ChainID:    genesis.header.ChainID,
			Number:     uint64(n),
			ParentHash: hash,
		}
		bytes, err := header.Hash()
		if err != nil {
			b.Fatalf("header hash: %v", err)
		}
		hash.SetBytes(bytes)

		block := NewBlock(header, nil)
		if err := chain.AddBlock(block); err != nil {
			b.Errorf("AddBlock(): %v", err)
		}
	}
	fmt.Println(hash)
}

func benchWriteBlock(b *testing.B, cnt int) {
	dir, err := ioutil.TempDir("", "yee-chain-bench")
	if err != nil {
		b.Fatalf("create tempDir failed: %v", err)
	}
	defer os.RemoveAll(dir)

	lvldb, err := persistent.NewLevelStorage(dir)
	if err != nil {
		b.Fatalf("create leveldb failed: %v", err)
	}
	benchAddBlock(b, lvldb, cnt)
}

func Benchmark_Write_50k(b *testing.B) {
	benchWriteBlock(b, 50000)
}

func Benchmark_Write_100k(b *testing.B) {
	benchWriteBlock(b, 100000)
}

// TODO: test for blockchain rejects storage with wrong genesis block

// TODO: test for blockchain generate genesis block if none found in storage
