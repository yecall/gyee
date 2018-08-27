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
	log "github.com/yeeco/gyee/p2p/logger"
)


//
// Datastore key
//
const DsKeyLength = config.NodeIDBytes
type DsKey =  [DsKeyLength]byte

//
// Datastore value
//
type DsValue = interface{}

//
// Datastore query
//
type DsQuery = interface{}

//
// Datastore query result
//
type DsQueryResult = interface {}

//
// Common datastore interface
//
type Datastore interface {

	//
	// Put (key, value) to data store
	//

	Put(key *DsKey, value DsValue) DhtErrno

	//
	// Get (key, value) from data store
	//

	Get(key *DsKey) (eno DhtErrno, value DsValue)

	//
	// Delete (key, value) from data store
	//

	Delete(key *DsKey) DhtErrno

	//
	// Query (key, value) pairs in data store
	//

	Query(query DsQuery) (eno DhtErrno, result DsQueryResult)
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
		ds: make(map[DsKey]DsValue, 0),
	}
}

//
// Put
//
func (mds *MapDatastore)Put(k *DsKey, v DsValue) DhtErrno {
	mds.ds[*k] = v
	return DhtEnoNone
}

//
// Get
//
func (mds *MapDatastore)Get(k *DsKey) (eno DhtErrno, value DsValue) {
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
func (mds *MapDatastore)Query(q DsQuery) (eno DhtErrno, result DsQueryResult) {
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

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("dsMgrProc: invalid parameters")
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dsMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = dsMgr.poweroff(ptn)

	case sch.EvDhtDsMgrAddValReq:
		eno = dsMgr.addValReq(msg.Body.(*sch.MsgDhtDsMgrAddValReq))

	case sch.EvDhtDsMgrPutValReq:
		eno = dsMgr.putValReq(msg.Body.(*sch.MsgDhtDsMgrPutValReq))

	case sch.EvDhtQryMgrQueryResultInd:
		eno = dsMgr.qryMgrQueryResultInd(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtDsMgrGetValReq:
		eno = dsMgr.getValReq(msg.Body.(*sch.MsgDhtDsMgrGetValReq))

	default:
		eno = sch.SchEnoParameter
		log.LogCallerFileLine("dsMgrProc: unknown message: %d", msg.Id)
	}

	return eno
}

//
// poweron handler
//
func (dsMgr *DsMgr)poweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dsMgr *DsMgr)poweroff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// add value request handler
//
func (dsMgr *DsMgr)addValReq(msg *sch.MsgDhtDsMgrAddValReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// qryMgr query result indication handler
//
func (dsMgr *DsMgr)qryMgrQueryResultInd(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// put value request handler
//
func (dsMgr *DsMgr)putValReq(msg *sch.MsgDhtDsMgrPutValReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// get value request handler
//
func (dsMgr *DsMgr)getValReq(msg *sch.MsgDhtDsMgrGetValReq) sch.SchErrno {
	return sch.SchEnoNone
}


