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
	"sync"
)

type MemoryStorage struct {
	data *sync.Map
}

type kv struct{ k, v []byte }

type MemoryBatch struct {
	db      *MemoryStorage
	entries []*kv
}

func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		data: new(sync.Map),
	}, nil
}

func (db *MemoryStorage) Get(key []byte) ([]byte, error) {
	if entry, ok := db.data.Load(hex.EncodeToString(key)); ok {
		return entry.([]byte), nil
	}
	return nil, ErrKeyNotFound
}

func (db *MemoryStorage) Put(key []byte, value []byte) error {
	db.data.Store(hex.EncodeToString(key), value)
	return nil
}

func (db *MemoryStorage) Del(key []byte) error {
	db.data.Delete(hex.EncodeToString(key))
	return nil
}

func (db *MemoryStorage) EnableBatch() {
}

func (db *MemoryStorage) Flush() error {
	return nil
}

func (db *MemoryStorage) DisableBatch() {
}

func (db *MemoryStorage) Close() error {
	return nil
}
