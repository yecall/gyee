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
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)


//
// Datastore key
//
type DsKey = config.NodeID

//
// Datastore value
//
type DsValue []byte

//
// Common datastore interface
//
type Datastore interface {

	//
	// Put (key, value) to data store
	//

	Put(key *DsKey, value *DsValue) DhtErrno

	//
	// Get (key, value) from data store
	//

	Get(key *DsKey) (eno DhtErrno, value *DsValue)

	//
	// Delete (key, value) from data store
	//

	Delete(key *DsKey) DhtErrno

	//
	// Query (key, value) pairs in data store
	//

	Query(query interface{}) (eno DhtErrno, result interface{})
}

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
		mds: make(map[DsKey]DsValue, 0),
	}
}

//
// Put
//
func (mds *MapDatastore)Put(k *DsKey, v *DsValue) DhtErrno {
	mds.ds[*k] = *v
	return DhtEnoNone
}

//
// Get
//
func (mds *MapDatastore)Get(k *DsKey) (eno DhtErrno, value *DsValue) {
	v, ok := mds.ds[*k]
	if !ok {
		return DhtEnoNotFound, nil
	}
	return DhtEnoNone, &v
}

//
// Delete
//
func (mds *MapDatastore)Delete(k *DsKey) DhtErrno {
	delete(mds.ds, *k)
	return DhtEnoNone
}

//
// Query
//
func (mds *MapDatastore)Query(q interface{}) (eno DhtErrno, result interface{}) {
	k, ok := q.(*DsKey)
	if !ok {
		return DhtEnoMismatched, nil
	}
	return mds.Get(k)
}

//
// Data store manager name registered in scheduler
//
const DsMgrName = sch.DhtDsMgrName

//
// Data store manager
//
type DsMgr struct {
	name		string					// my name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to task node of myself
	ptnDhtMgr	interface{}				// pointer to dht manager task node
	ptnQryMgr	interface{}				// pointer to query manager task node
	ds			Datastore				// data store
}

//
// Create data store manager
//
func NewDsMgr() *DsMgr {

	dsMgr := DsMgr{
		name:		DsMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnDhtMgr:	nil,
		ptnQryMgr:	nil,
		ds:			NewMapDatastore(),
	}

	dsMgr.tep = dsMgr.dsMgrProc

	return &dsMgr
}

//
// Entry point exported to scheduler
//
func (dsMgr *DsMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dsMgr.tep(ptn, msg)
}

//
// Provider manager entry
//
func (dsMgr *DsMgr)dsMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	eno := sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
	case sch.EvSchPoweroff:
	case sch.EvDhtDsMgrAddValReq:
	case sch.EvDhtQryMgrQueryResultInd:
	case sch.EvDhtDsMgrGetValReq:
	case sch.EvDhtDsMgrGetValRsp:
	default:
		eno = sch.SchEnoParameter
	}

	return eno
}
