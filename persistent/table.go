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

package persistent

type table struct {
	storage Storage
	prefix  string
}

// NewTable returns a wrapped storage, which prefixes all keys
func NewTable(storage Storage, prefix string) Storage {
	return &table{
		storage: storage,
		prefix:  prefix,
	}
}

func (t *table) Get(key []byte) ([]byte, error) {
	return t.storage.Get(append([]byte(t.prefix), key...))
}

func (t *table) Has(key []byte) (bool, error) {
	return t.storage.Has(append([]byte(t.prefix), key...))
}

func (t *table) Put(key []byte, value []byte) error {
	return t.storage.Put(append([]byte(t.prefix), key...), value)
}

func (t *table) Del(key []byte) error {
	return t.storage.Del(append([]byte(t.prefix), key...))
}

func (t *table) Close() error {
	return nil
}

func (t *table) NewBatch() Batch {
	return &tableBatch{
		batch:  t.storage.NewBatch(),
		prefix: t.prefix,
	}
}

type tableBatch struct {
	batch  Batch
	prefix string
}

func (tb *tableBatch) Put(key []byte, value []byte) error {
	return tb.batch.Put(append([]byte(tb.prefix), key...), value)
}

func (tb *tableBatch) Del(key []byte) error {
	return tb.batch.Del(append([]byte(tb.prefix), key...))
}

func (tb *tableBatch) ValueSize() int {
	return tb.batch.ValueSize()
}

func (tb *tableBatch) Write() error {
	return tb.batch.Write()
}

func (tb *tableBatch) Reset() {
	tb.batch.Reset()
}
