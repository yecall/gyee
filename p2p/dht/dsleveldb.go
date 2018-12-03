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
	"github.com/yeeco/gyee/persistent"
	log "github.com/yeeco/gyee/p2p/logger"
)

type LeveldbDatastoreConfig struct {
	Path					string
	OpenFilesCacheCapacity	int
	BlockCacheCapacity		int
	BlockSize				int
	FilterBits				int
}

type LeveldbDatastore struct {
	ldsCfg		*LeveldbDatastoreConfig
	ls			*persistent.LevelStorage
}

func NewLeveldbDatastore(cfg *LeveldbDatastoreConfig) *LeveldbDatastore {
	ds := LeveldbDatastore {
		ldsCfg:	cfg,
	}
	ls, err := persistent.NewLevelStorage(cfg.Path)
	if err != nil {
		log.Debug("NewLeveldbDatastore: failed, error: %s", err.Error())
		return  nil
	}
	ds.ls = ls
	return &ds
}

func (lds *LeveldbDatastore)Put(k *DsKey, v DsValue) DhtErrno {
	if err := lds.ls.Put(k[0:], v.([]byte)); err != nil {
		log.Debug("Put: failed, error: %s", err.Error())
		return DhtEnoDatastore
	}
	return DhtEnoNone
}

func (lds *LeveldbDatastore)Get(k *DsKey) (eno DhtErrno, value DsValue) {
	err := error(nil)
	value, err = lds.ls.B(k[0:])
	if err != nil {
		log.Debug("Get: failed, error: %s", err.Error())
		eno = DhtEnoDatastore
		value = nil
	} else {
		eno = DhtEnoNone
	}
	return
}

func (lds *LeveldbDatastore)Delete(k *DsKey) DhtErrno {
	if err := lds.ls.Del(k[0:]); err != nil {
		log.Debug("Delete: failed, error: %s", err.Error())
		return DhtEnoDatastore
	}
	return DhtEnoNone
}

