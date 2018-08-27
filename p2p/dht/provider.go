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
	"time"
	config "github.com/yeeco/gyee/p2p/config"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
)

//
// Provider manager name registered in scheduler
//
const PrdMgrName = sch.DhtPrdMgrName

//
// Providers cache parameters
//
const (
	prdCacheSize = 256					// cache size
	prdCleanupInterval = time.Hour * 1	// cleanup period
	prdLifeCached = time.Hour * 24		// lifetime
)

//
// Provider manager
//
type PrdMgr struct {
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// my name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to task node of myself
	ptnDhtMgr	interface{}				// pointer to dht manager task node
	ptnQryMgr	interface{}				// pointer to query manager task node
	clrTid		int						// cleanup timer identity
	ds			Datastore				// data store
	prdCache	*lru.Cache				// providers cache
}

//
// Provider set
//
type PrdSet struct {
	set 	[]config.NodeID				// provider set
	addTime	map[config.NodeID]time.Time	// time for providers added
}

//
// Create provider manager
//
func NewPrdMgr() *PrdMgr {

	prdMgr := PrdMgr{
		name:		PrdMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnDhtMgr:	nil,
		ptnQryMgr:	nil,
		clrTid:		sch.SchInvalidTid,
		ds:			nil,
		prdCache:	nil,
	}

	prdMgr.tep = prdMgr.prdMgrProc

	return &prdMgr
}

//
// Entry point exported to shceduler
//
func (prdMgr *PrdMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return prdMgr.tep(ptn, msg)
}

//
// Provider manager entry
//
func (prdMgr *PrdMgr)prdMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("prdMgrProc: invalid parameters")
		return DhtEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = prdMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = prdMgr.poweroff(ptn)

	case sch.EvDhtPrdMgrCleanupTimer:
		eno = prdMgr.cleanupTimer()

	case sch.EvDhtPrdMgrAddProviderReq:
		eno = prdMgr.localAddProviderReq(msg.Body.(*sch.MsgDhtPrdMgrAddProviderReq))

	case sch.EvDhtMgrGetProviderReq:
		eno = prdMgr.localGetProviderReq(msg.Body.(*sch.MsgDhtMgrGetProviderReq))

	case sch.EvDhtQryMgrQueryResultInd:
		eno = prdMgr.qryMgrQueryResultInd(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtPrdMgrPutProviderReq:
		eno = prdMgr.putProviderReq(msg.Body.(*sch.MsgDhtPrdMgrPutProviderReq))

	case sch.EvDhtPrdMgrGetProviderReq:
		eno = prdMgr.getProviderReq(msg.Body.(*sch.MsgDhtPrdMgrGetProviderReq))

	default:
		eno = sch.SchEnoParameter
		log.LogCallerFileLine("prdMgrProc: unknown message: %d", msg.Id)
	}

	return eno
}

//
// power on handler
//
func (prdMgr *PrdMgr)poweron(ptn interface{}) sch.SchErrno {

	prdMgr.sdl = sch.SchGetScheduler(ptn)
	prdMgr.ptnMe = ptn
	_, prdMgr.ptnQryMgr = prdMgr.sdl.SchGetTaskNodeByName(QryMgrName)
	_, prdMgr.ptnDhtMgr = prdMgr.sdl.SchGetTaskNodeByName(DsMgrName)

	prdMgr.prdCache, _ = lru.New(prdCacheSize)
	prdMgr.ds = NewMapDatastore()

	var td = sch.TimerDescription {
		Name:	"TmPrdMgrCleanup",
		Utid:	sch.DhtPrdMgrCleanupTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	prdCleanupInterval,
		Extra:	nil,
	}

	eno, tid := prdMgr.sdl.SchSetTimer(prdMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		log.LogCallerFileLine("poweron: SchSetTimer failed, eno: %d", eno)
		return eno
	}
	prdMgr.clrTid = tid

	return sch.SchEnoNone
}

//
// power off handler
//
func (prdMgr *PrdMgr)poweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("poweroff: task will be done ...")
	return prdMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}

//
// cleanup timer handler
//
func (prdMgr *PrdMgr)cleanupTimer() sch.SchErrno {

	c := prdMgr.prdCache
	now := time.Now()
	keys := prdMgr.prdCache.Keys()

	for _, k := range keys {

		if i, ok := c.Get(k); ok {

			del := []config.NodeID{}
			ps := i.(*PrdSet)

			for id, t := range ps.addTime {
				if now.Sub(t) >= prdLifeCached {
					del = append(del, id)
				}
			}

			for _, id := range del {
				delete(ps.addTime, id)
			}

			ps.set = nil
			for id := range ps.addTime {
				ps.set = append(ps.set, id)
			}
		}
	}

	return sch.SchEnoNone
}

//
// local add provider request handler
//
func (prdMgr *PrdMgr)localAddProviderReq(msg *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {

	//
	// we are commanded to add a provider by external module of local
	//

	if len(msg.Key) != DsKeyLength {
		log.LogCallerFileLine("localAddProviderReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// cache it
	//

	if eno := prdMgr.cache(&k, &msg.Prd.ID); eno != DhtEnoNone {
		log.LogCallerFileLine("localAddProviderReq: cache failed, eno: %d", eno)
		prdMgr.localAddProviderRsp(msg.Key, eno)
		return sch.SchEnoUserTask
	}

	//
	// store it
	//

	if eno := prdMgr.store(&k, &msg.Prd.ID); eno != DhtEnoNone {
		log.LogCallerFileLine("localAddProviderReq: store failed, eno: %d", eno)
		prdMgr.localAddProviderRsp(msg.Key, eno)
		return sch.SchEnoUserTask
	}

	//
	// publish it to our neighbors
	//

	qry := sch.MsgDhtQryMgrQueryStartReq {
		Target:		config.NodeID(k),
		Value:		msg,
		ForWhat:	MID_PUTPROVIDER,
	}

	schMsg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, prdMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return prdMgr.sdl.SchSendMessage(&schMsg)
}

//
// local get provider request handler
//
func (prdMgr *PrdMgr)localGetProviderReq(msg *sch.MsgDhtMgrGetProviderReq) sch.SchErrno {

	if len(msg.Key) != DsKeyLength {
		return sch.SchEnoParameter
	}

	var dsk DsKey
	copy(dsk[0:], msg.Key)

	var prds = []*config.NodeID{}
	var eno = DhtErrno(DhtEnoUnknown)
	var qry = sch.MsgDhtQryMgrQueryStartReq{}
	var schMsg = sch.SchMessage{}

	//
	// lookup cache
	//

	if prdSet := prdMgr.prdFromCache(&dsk); prdSet != nil {
		for _, id := range prdSet.set {
			prds = append(prds, &id)
		}
		if len(prds) >0 {
			eno = DhtEnoNone
			goto _rsp2DhtMgr
		}
	}

	//
	// lookup local data store
	//

	if prdSet := prdMgr.prdFromStore(&dsk); prdSet != nil {
		for _, id := range prdSet.set {
			prds = append(prds, &id)
		}
		if len(prds) >0 {
			eno = DhtEnoNone
			goto _rsp2DhtMgr
		}
	}

	//
	// lookup our neighbors
	//

	qry = sch.MsgDhtQryMgrQueryStartReq {
		Target:		config.NodeID(dsk),
		Value:		nil,
		ForWhat:	MID_GETPROVIDER_REQ,
	}

	prdMgr.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, prdMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return prdMgr.sdl.SchSendMessage(&schMsg)

_rsp2DhtMgr:

	return prdMgr.localGetProviderRsp(msg.Key, prds, eno)
}

//
// qryMgr query result indication handler
//
func (prdMgr *PrdMgr)qryMgrQueryResultInd(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {

	if msg.ForWhat == MID_PUTPROVIDER {

		key := msg.Target[0:]
		return prdMgr.localAddProviderRsp(key, DhtErrno(msg.Eno))

	} else if msg.ForWhat == MID_GETPROVIDER_REQ {

		key := msg.Target[0:]
		prds := msg.Prds

		//
		// cache and store providers reported
		//

		var dsk DsKey
		copy(dsk[0:], key)

		for _, prd := range prds {

			if prdMgr.cache(&dsk, prd) != DhtEnoNone {
				log.LogCallerFileLine("qryMgrQueryResultInd: cache failed")
			}

			if prdMgr.store(&dsk, prd) != DhtEnoNone {
				log.LogCallerFileLine("qryMgrQueryResultInd: store failed")
			}
		}

		return prdMgr.localGetProviderRsp(key, prds, DhtErrno(msg.Eno))

	} else {
		log.LogCallerFileLine("qryMgrQueryResultInd: not matched with prdMgr")
	}

	return sch.SchEnoMismatched
}

//
// put provider request handler
//
func (prdMgr *PrdMgr)putProviderReq(msg *sch.MsgDhtPrdMgrPutProviderReq) sch.SchErrno {

	//
	// we are required to put-provider by remote peer, we just put it into the
	// cache and data store.
	//

	dsk := DsKey{}
	pp := msg.Msg.(*PutProvider)
	for _, prd := range pp.Providers {
		copy(dsk[0:], prd.Key)
		if prdMgr.cache(&dsk, &prd.Node.ID) != DhtEnoNone {
			log.LogCallerFileLine("putProviderReq: cache failed")
		}
		if prdMgr.store(&dsk, &prd.Node.ID) != DhtEnoNone {
			log.LogCallerFileLine("putProviderReq: store failed")
		}
	}

	return sch.SchEnoNone
}

//
// get provider handler
//
func (prdMgr *PrdMgr)getProviderReq(msg *sch.MsgDhtPrdMgrGetProviderReq) sch.SchErrno {

	//
	// we are required to get-provider by remote peer
	//

	return sch.SchEnoNone
}

//
// try get provider from cache
//
func (prdMgr *PrdMgr)prdFromCache(key *DsKey) *PrdSet {
	return nil
}

//
// try get provider form data store
//
func (prdMgr *PrdMgr)prdFromStore(key *DsKey) *PrdSet {
	return nil
}

//
// store a provider
//
func (prdMgr *PrdMgr)store(key *DsKey, peerId *config.NodeID) DhtErrno {
	return DhtEnoNone
}

//
// encode provider record to protobuf record
//
func (prdMgr *PrdMgr)encodeProvider(key *DsKey, peerId *config.NodeID) (DhtErrno, []byte) {
	return 	DhtEnoNone, nil
}

//
// response to local dhtMgr for "add-provider"
//
func (prdMgr *PrdMgr)localAddProviderRsp(key []byte, eno DhtErrno) sch.SchErrno {
	rsp := sch.MsgDhtPrdMgrAddProviderRsp {
		Key: key,
		Eno: int(eno),
	}
	msg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&msg, prdMgr.ptnMe, prdMgr.ptnDhtMgr, sch.EvDhtMgrPutProviderRsp, &rsp)
	return prdMgr.sdl.SchSendMessage(&msg)
}

//
// response to local dhtMgr for "get-provider"
//
func (prdMgr *PrdMgr)localGetProviderRsp(key []byte, prds []*config.NodeID, eno DhtErrno) sch.SchErrno {
	rsp := sch.MsgDhtMgrGetProviderRsp {
		Eno:	int(eno),
		Key:	key,
		Prds:	prds,
	}
	msg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&msg, prdMgr.ptnMe, prdMgr.ptnDhtMgr, sch.EvDhtMgrGetProviderRsp, &rsp)
	return prdMgr.sdl.SchSendMessage(&msg)
}

//
// cache a provider
//
func (prdMgr *PrdMgr)cache(k *DsKey, prd *config.NodeID) DhtErrno {

	if prdSet := prdMgr.prdFromCache(k); prdSet != nil {

		prdSet.append(prd, time.Now())

	} else {

		newPrd := PrdSet{
			set:     []config.NodeID{*prd},
			addTime: map[config.NodeID]time.Time{*prd: time.Now()},
		}

		prdMgr.prdCache.Add(&k, &newPrd)
	}

	return DhtEnoNone
}

//
// add new provider to set
//
func (prdSet *PrdSet)append(peerId *config.NodeID, addTime time.Time) {
	if _, exist := prdSet.addTime[*peerId]; !exist {
		prdSet.set = append(prdSet.set, *peerId)
	}
	prdSet.addTime[*peerId] = addTime
}

