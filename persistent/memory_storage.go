/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package persistent

import (
	"encoding/hex"
	"github.com/yeeco/gyee/common"
	"sync"
)

type MemoryStorage struct {
	data *sync.Map
}

type kv struct {
	k, v []byte
	del  bool
}

type memoryBatch struct {
	db      *MemoryStorage
	entries []*kv
	size    int
}

func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		data: new(sync.Map),
	}, nil
}

func (db *MemoryStorage) Has(key []byte) (bool, error) {
	_, ok := db.data.Load(hex.EncodeToString(key))
	return ok, nil
}

func (db *MemoryStorage) Get(key []byte) ([]byte, error) {
	if entry, ok := db.data.Load(hex.EncodeToString(key)); ok {
		return entry.([]byte), nil
	}
	return nil, ErrKeyNotFound
}

func (db *MemoryStorage) Put(key []byte, value []byte) error {
	db.data.Store(hex.EncodeToString(key), common.CopyBytes(value))
	return nil
}

func (db *MemoryStorage) Del(key []byte) error {
	db.data.Delete(hex.EncodeToString(key))
	return nil
}

func (db *MemoryStorage) Close() error {
	return nil
}

func (db *MemoryStorage) NewBatch() Batch {
	return &memoryBatch{db: db}
}

func (b *memoryBatch) Put(key, value []byte) error {
	b.entries = append(b.entries, &kv{
		common.CopyBytes(key), common.CopyBytes(value),
		false})
	b.size += len(value)
	return nil
}

func (b *memoryBatch) Del(key []byte) error {
	b.entries = append(b.entries, &kv{
		common.CopyBytes(key), nil,
		true})
	b.size += 1
	return nil
}

func (b *memoryBatch) ValueSize() int {
	return b.size
}

func (b *memoryBatch) Write() error {
	for _, kv := range b.entries {
		if kv.del {
			b.db.Del(kv.k)
		} else {
			b.db.Put(kv.k, kv.v)
		}
	}
	return nil
}

func (b *memoryBatch) Reset() {
	b.entries = b.entries[:0]
	b.size = 0
}
