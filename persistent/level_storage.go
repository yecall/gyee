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
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelStorage struct {
	db *leveldb.DB
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
		db: db,
	}, nil
}

func (storage *LevelStorage) Has(key []byte) (bool, error) {
	return storage.db.Has(key, nil)
}

func (storage *LevelStorage) Get(key []byte) ([]byte, error) {
	val, err := storage.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		err = ErrKeyNotFound
	}
	return val, err
}

func (storage *LevelStorage) Put(key []byte, value []byte) error {
	return storage.db.Put(key, value, nil)
}

func (storage *LevelStorage) Del(key []byte) error {
	return storage.db.Delete(key, nil)
}

func (storage *LevelStorage) Close() error {
	return storage.db.Close()
}

func (storage *LevelStorage) GetLevelDB() *leveldb.DB {
	return storage.db
}

func (storage *LevelStorage) NewBatch() Batch {
	return &ldbBatch{db: storage.db, b: new(leveldb.Batch)}
}

type ldbBatch struct {
	db   *leveldb.DB
	b    *leveldb.Batch
	size int
}

func (b *ldbBatch) Put(key, value []byte) error {
	b.b.Put(key, value)
	b.size += len(value)
	return nil
}

func (b *ldbBatch) Del(key []byte) error {
	b.b.Delete(key)
	b.size += 1
	return nil
}

func (b *ldbBatch) ValueSize() int {
	return b.size
}

func (b *ldbBatch) Write() error {
	return b.db.Write(b.b, nil)
}

func (b *ldbBatch) Reset() {
	b.b.Reset()
	b.size = 0
}
