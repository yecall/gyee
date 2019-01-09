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

import "errors"

// Code using batches should try to add this much data to the batch.
// The value was determined empirically.
const IdealBatchSize = 100 * 1024

var (
	ErrKeyNotFound = errors.New("not found")
)

type Getter interface {
	Get(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
}

type Putter interface {
	Put(key []byte, value []byte) error
}

type Deleter interface {
	Del(key []byte) error
}

type Storage interface {
	Getter
	Putter
	Deleter

	Close() error

	NewBatch() Batch
}

type Batch interface {
	Putter
	Deleter

	ValueSize() int
	Write() error
	Reset()
}
