/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  the gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  the gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package dht

import "time"


//
// Data store based on "map" in memory, for test only
//
type MapDatastore struct {
	ds map[DsKey]DsValue		// (key, value) map
}

//
// New map datastore
//
func NewMapDatastore() *MapDatastore {
	return &MapDatastore{
		ds: make(map[DsKey]DsValue, 0),
	}
}

//
// Put
//
func (mds *MapDatastore)Put(k []byte, v DsValue, kt time.Duration) DhtErrno {
	dsKey := DsKey{}
	copy(dsKey[0:], k)
	mds.ds[dsKey] = v
	return DhtEnoNone
}

//
// Get
//
func (mds *MapDatastore)Get(k []byte) (eno DhtErrno, value DsValue) {
	dsKey := DsKey{}
	copy(dsKey[0:], k)
	v, ok := mds.ds[dsKey]
	if !ok {
		return DhtEnoNotFound, nil
	}
	return DhtEnoNone, &v
}

//
// Delete
//
func (mds *MapDatastore)Delete(k []byte) DhtErrno {
	dsKey := DsKey{}
	copy(dsKey[0:], k)
	delete(mds.ds, dsKey)
	return DhtEnoNone
}

//
// Clsoe
//
func (mds *MapDatastore)Close() DhtErrno {
	mds.ds = nil
	return DhtEnoNone
}
