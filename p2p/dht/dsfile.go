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

import (
	"fmt"
	ipfsds "github.com/ipfs/go-datastore"
	ipfsfs "github.com/ipfs/go-ds-flatfs"
	log "github.com/yeeco/gyee/p2p/logger"
)

//
// all are based on ipfs-datastore packages, see ipfs (datastore, flatfs, ...)
// for more please.
//

const (
	sfnPrefix		= "prefix"
	sfnSuffix		= "suffix"
	sfnNextToLast	= "next-to-last"
)

var sfn2ShardIdV1 = map[string](func (padLen int) *ipfsfs.ShardIdV1) {
	sfnPrefix:		ipfsfs.Prefix,
	sfnSuffix:		ipfsfs.Suffix,
	sfnNextToLast:	ipfsfs.NextToLast,
}

type FileDatastoreConfig struct {
	path				string				// data store path
	shardFuncName		string				// shard function name
	padLength			int					// padding length
	sync				bool				// sync file store flag
}

type FileDatastore struct {
	cfg		FileDatastoreConfig				// configuration
	ffs		*ipfsfs.Datastore				// file datastore pointer
}

//
// New file data store
//
func NewFileDatastore(cfg *FileDatastoreConfig) *FileDatastore {

	if cfg == nil {
		log.LogCallerFileLine("NewFileDatastore: nil configuration")
		return nil
	}

	sidv1 := sfn2ShardIdV1[cfg.shardFuncName](cfg.padLength)
	ffs, err := ipfsfs.CreateOrOpen(cfg.path, sidv1, cfg.sync)

	if err != nil {
		log.LogCallerFileLine("NewFileDatastore: CreateOrOpen faialed, err: %s", err.Error())
		return nil
	}

	return &FileDatastore {
		cfg: *cfg,
		ffs: ffs,
	}
}

//
// Put
//
func (fds *FileDatastore)Put(k *DsKey, v DsValue) DhtErrno {

	strKey := fmt.Sprintf("%x", *k)
	dsk := ipfsds.NewKey(strKey)
	log.LogCallerFileLine("Put: key: %s", dsk);

	if err := fds.ffs.Put(dsk, v); err != nil {
		log.LogCallerFileLine("Put: Put failed, err: %s", err.Error())
		return DhtEnoDatastore
	}

	return DhtEnoNone
}

//
// Get
//
func (fds *FileDatastore)Get(k *DsKey) (eno DhtErrno, value DsValue) {

	strKey := fmt.Sprintf("%x", *k)
	dsk := ipfsds.NewKey(strKey)
	log.LogCallerFileLine("Get: key: %s", dsk);

	val, err := fds.ffs.Get(dsk)

	if err != nil {

		if err == ipfsds.ErrNotFound {
			log.LogCallerFileLine("Get: not found")
			return DhtEnoNotFound, nil
		}

		log.LogCallerFileLine("Get: Get failed, err: %s", err.Error())
		return DhtEnoDatastore, nil
	}

	return DhtEnoNone, val
}

//
// Delete
//
func (fds *FileDatastore)Delete(k *DsKey) DhtErrno {

	strKey := fmt.Sprintf("%x", *k)
	dsk := ipfsds.NewKey(strKey)
	log.LogCallerFileLine("Delete: key: %s", dsk);

	if err := fds.ffs.Delete(dsk); err != nil {
		log.LogCallerFileLine("Delete: Delete failed, err: %s", err.Error())
		return DhtEnoDatastore
	}

	return DhtEnoNone
}
