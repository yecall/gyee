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

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelStorage struct {
	db          *leveldb.DB
	enableBatch bool
	mutex       sync.Mutex
	batchOps    map[string]*batchOp
}

type batchOp struct {
	key   []byte
	value []byte
	del   bool
}

func NewLevelStorage(path string) (*LevelStorage, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: 500,
		BlockCacheCapacity:     8 * opt.MiB,
		BlockSize:              4 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
	})

	if err != nil {
		return nil, err
	}

	return &LevelStorage{
		db:          db,
		enableBatch: false,
		batchOps:    make(map[string]*batchOp),
	}, nil
}

func (storage *LevelStorage) Get(key []byte) ([]byte, error) {
	value, err := storage.db.Get(key, nil)
	if err != nil && err == leveldb.ErrNotFound {
		return nil, ErrKeyNotFound
	}

	return value, err
}

func (storage *LevelStorage) Put(key []byte, value []byte) error {
	if storage.enableBatch {
		storage.mutex.Lock()
		defer storage.mutex.Unlock()

		storage.batchOps[hex.EncodeToString(key)] = &batchOp{
			key:   key,
			value: value,
			del:   false,
		}

		return nil
	}

	return storage.db.Put(key, value, nil)
}

func (storage *LevelStorage) Del(key []byte) error {
	if storage.enableBatch {
		storage.mutex.Lock()
		defer storage.mutex.Unlock()

		storage.batchOps[hex.EncodeToString(key)] = &batchOp{
			key: key,
			del: true,
		}

		return nil
	}

	return storage.db.Delete(key, nil)
}

func (storage *LevelStorage) Close() error {
	return storage.db.Close()
}

func (storage *LevelStorage) EnableBatch() {
	storage.enableBatch = true
}

func (storage *LevelStorage) DisableBatch() {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	storage.batchOps = make(map[string]*batchOp)
	storage.enableBatch = false
}

func (storage *LevelStorage) Flush() error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	if !storage.enableBatch {
		return nil
	}

	batch := new(leveldb.Batch)
	for _, op := range storage.batchOps {
		if op.del {
			batch.Delete(op.key)
		} else {
			batch.Put(op.key, op.value)
		}
	}
	storage.batchOps = make(map[string]*batchOp)

	return storage.db.Write(batch, nil)
}
