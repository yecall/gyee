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

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
	sha3 "github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
)

// Key / KeyPrefix for blockchain used in persistent.Storage
const (
	KeyChainID = "ChainID"

	KeyLastBlock = "LastBlock"

	KeyPrefixStateTrie = "sTrie-" // stateTrie Hash => trie node

	KeyPrefixTx     = "tx-"   // txHash => encodedTx
	KeyPrefixHeader = "blkH-" // blockHash => encodedBlockHeader
	KeyPrefixBody   = "blkB-" // blockHash => encodedBlockBody

	KeyPrefixBlockNum2Hash = "bn2h-" // blockNum => blockHash
	KeyPrefixBlockHash2Num = "bh2n-" // blockHash => blockNum
)

func prepareStorage(storage persistent.Storage, id ChainID) error {
	key := keyChainID()
	exists, err := storage.Has(key)
	if err != nil {
		return err
	}
	if exists {
		encChainID, err := storage.Get(key)
		if err != nil {
			return err
		}
		decoded := binary.BigEndian.Uint32(encChainID)
		if ChainID(decoded) != id {
			return ErrBlockChainIDMismatch
		}
	} else {
		encChainID := make([]byte, 4)
		binary.BigEndian.PutUint32(encChainID, uint32(id))
		return storage.Put(key, encChainID)
	}
	return nil
}

func getLastBlock(getter persistent.Getter) common.Hash {
	enc, err := getter.Get(keyLastBlock())
	if err != nil {
		log.Error("getLastBlock()", err)
		return common.EmptyHash
	}
	if len(enc) == 0 {
		return common.EmptyHash
	}
	return common.BytesToHash(enc)
}

func putLastBlock(putter persistent.Putter, hash common.Hash) {
	if err := putter.Put(keyLastBlock(), hash[:]); err != nil {
		log.Crit("putLastBlock()", err)
	}
}

func getHeader(getter persistent.Getter, hash common.Hash) (*corepb.SignedBlockHeader, error) {
	enc, err := getter.Get(keyHeader(hash))
	if err != nil {
		if err == persistent.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	pb := new(corepb.SignedBlockHeader)
	if err := proto.Unmarshal(enc, pb); err != nil {
		return nil, err
	}
	return pb, nil
}

func putHeader(putter persistent.Putter, header *corepb.SignedBlockHeader) common.Hash {
	enc, err := proto.Marshal(header)
	if err != nil {
		log.Crit("putHeader()", err)
	}
	hash := common.BytesToHash(sha3.Sha3256(header.Header))
	if err := putter.Put(keyHeader(hash), enc); err != nil {
		log.Crit("putHeader()", err)
	}
	return hash
}

func getBlockBody(getter persistent.Getter, hash common.Hash) (*corepb.BlockBody, error) {
	if exists, err := getter.Has(keyBlockBody(hash)); !exists || err != nil {
		return nil, err
	}
	enc, err := getter.Get(keyBlockBody(hash))
	if err != nil {
		return nil, err
	}
	pb := new(corepb.BlockBody)
	if err := proto.Unmarshal(enc, pb); err != nil {
		return nil, err
	}
	return pb, nil
}

func putBlockBody(putter persistent.Putter, hash common.Hash, body *corepb.BlockBody) {
	enc, err := proto.Marshal(body)
	if err != nil {
		log.Crit("putBlockBody()", err)
	}
	if err := putter.Put(keyBlockBody(hash), enc); err != nil {
		log.Crit("putBlockBody()", err)
	}
}

func getBlockHash2Num(getter persistent.Getter, hash common.Hash) *uint64 {
	enc, err := getter.Get(keyBlockHash2Num(hash))
	if err != nil {
		return nil
	}
	value := binary.BigEndian.Uint64(enc)
	return &value
}

func putBlockHash2Num(putter persistent.Putter, hash common.Hash, num uint64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)
	if err := putter.Put(keyBlockHash2Num(hash), buf); err != nil {
		log.Crit("putBlockHash2Num()", err)
	}
}

func getBlockNum2Hash(getter persistent.Getter, num uint64) (hash common.Hash) {
	enc, err := getter.Get(keyBlockNum2Hash(num))
	if err != nil {
		return common.EmptyHash
	}
	hash.SetBytes(enc)
	return
}

func putBlockNum2Hash(putter persistent.Putter, num uint64, hash common.Hash) {
	if err := putter.Put(keyBlockNum2Hash(num), hash[:]); err != nil {
		log.Crit("putBlockNum2Hash()", err)
	}
}

func keyChainID() []byte {
	return []byte(KeyChainID)
}

func keyLastBlock() []byte {
	return []byte(KeyLastBlock)
}

func keyHeader(hash common.Hash) []byte {
	return append([]byte(KeyPrefixHeader), hash[:]...)
}

func keyBlockBody(hash common.Hash) []byte {
	return append([]byte(KeyPrefixBody), hash[:]...)
}

func keyBlockHash2Num(hash common.Hash) []byte {
	return append([]byte(KeyPrefixBlockHash2Num), hash[:]...)
}

func keyBlockNum2Hash(num uint64) []byte {
	buf := append([]byte(KeyPrefixBlockNum2Hash), make([]byte, 8)...)
	binary.BigEndian.PutUint64(buf[len(buf)-8:], num)
	return buf
}
