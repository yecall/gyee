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
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestNewLevelStorage(t *testing.T) {
	storage, err := NewLevelStorage("level.db")
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{[]byte("1"), []byte("2")}
	values := [][]byte{[]byte("1"), []byte("2")}
	storage.Put(keys[0], values[0])
	storage.Put(keys[1], values[1])
	value1, err1 := storage.Get(keys[0])

	if err1 != nil {
		t.Error(err1)
	}
	if !bytes.Equal(value1, values[0]) {
		t.Errorf("not equal %x %x", value1, values[0])
	}
	storage.Del(keys[1])
	_, err2 := storage.Get(keys[1])

	if err2 == nil {
		t.Error("get del")
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func TestLeveldbBenchmark(t *testing.T) {
	file := "benchmark.db"
	db, err := leveldb.OpenFile(file, &opt.Options{
		OpenFilesCacheCapacity: 500,
		BlockCacheCapacity:     8 * opt.MiB,
		WriteBuffer:            4 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		key   []byte
		value []byte
		count int64
	}{
		{"1", []byte("key1"), []byte("value1"), 0},
		//{"2", []byte("key2"), []byte("value2"), 10000},
		//{"3", []byte("key3"), []byte("value3"), 100000},
		//{"4", []byte("key4"), []byte("value4"), 1000000},
		//{"5", []byte("key5"), []byte("value5"), 4000000},
	}

	count := int64(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := db.Put(tt.key, tt.value, nil)
			if err != nil {
				t.Error(err)
			}

			count = count + tt.count
			for i := int64(0); i < tt.count; i++ {
				err := db.Put(randBytes(32), randBytes(rand.Intn(1024)), nil)
				if err != nil {
					t.Error(err)
				}
			}

			start := time.Now().UnixNano()
			value, err := db.Get(tt.key, nil)
			duration := time.Now().UnixNano() - start
			if err != nil {
				t.Error(err)
			}
			if !bytes.Equal(tt.value, value) {
				t.Errorf("not equal %x %x", tt.value, value)
			}

			t.Log("count:", count, "duration:", duration, " Nano")
		})
	}
	db.Close()
	os.Remove(file)
}
