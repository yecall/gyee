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
	"bytes"
	"sync"
	"time"

	"github.com/yeeco/gyee/log"
	lru "github.com/hashicorp/golang-lru"
	"github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// debug
//
type prdMgrLogger struct {
	debug__ bool
}

var prdLog = prdMgrLogger{
	debug__: false,
}

func (log prdMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// Provider manager name registered in scheduler
//
const PrdMgrName = sch.DhtPrdMgrName

//
// Providers cache parameters
//
const (
	prdCacheSize       = 4096           // cache size
	prdCleanupInterval = time.Hour * 1  // cleanup period
	prdLifeCached      = time.Hour * 24 // lifetime
	prdDftKeepTime     = time.Hour * 24 // default duration to keep [key, provider] pair
)

//
// Provider manager
//
type PrdMgr struct {
	sdl       *sch.Scheduler    // pointer to scheduler
	name      string            // my name
	tep       sch.SchUserTaskEp // task entry
	ptnMe     interface{}       // pointer to task node of myself
	ptnDhtMgr interface{}       // pointer to dht manager task node
	ptnQryMgr interface{}       // pointer to query manager task node
	ptnRutMgr interface{}       // pointer to route manager task node
	clrTid    int               // cleanup timer identity
	ds        Datastore         // data store
	lockStore sync.Mutex        // sync with store
	prdCache  *lru.Cache        // providers cache
	lockCache sync.Mutex        // sync with cache operations
	tmMgr     *TimerManager     // timer manager
}

//
// Provider set
//
type PrdSet struct {
	set     map[DsKey]config.Node // provider set
	addTime map[DsKey]time.Time   // time for providers added
}

//
// Provider data store record
//
type PsRecord struct {
	Key   DsKey         // provider record key
	Value DsValue       // provider record value
	KT    time.Duration // duratio to keep this [key, val] pair
}

//
// Create provider manager
//
func NewPrdMgr() *PrdMgr {

	prdMgr := PrdMgr{
		name:   PrdMgrName,
		clrTid: sch.SchInvalidTid,
		tmMgr:  NewTimerManager(),
	}

	prdMgr.tep = prdMgr.prdMgrProc

	return &prdMgr
}

//
// Entry point exported to shceduler
//
func (prdMgr *PrdMgr) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return prdMgr.tep(ptn, msg)
}

//
// Provider manager entry
//
func (prdMgr *PrdMgr) prdMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		prdLog.Debug("prdMgrProc: invalid parameters")
		return sch.SchEnoParameter
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

	case sch.EvDhtRutMgrNearestRsp:
		eno = prdMgr.rutMgrNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	default:
		eno = sch.SchEnoParameter
		prdLog.Debug("prdMgrProc: unknown message: %d", msg.Id)
	}

	return eno
}

//
// power on handler
//
func (prdMgr *PrdMgr) poweron(ptn interface{}) sch.SchErrno {

	prdMgr.sdl = sch.SchGetScheduler(ptn)
	prdMgr.ptnMe = ptn
	_, prdMgr.ptnQryMgr = prdMgr.sdl.SchGetUserTaskNode(QryMgrName)
	_, prdMgr.ptnDhtMgr = prdMgr.sdl.SchGetUserTaskNode(DsMgrName)

	prdMgr.prdCache, _ = lru.New(prdCacheSize)
	prdMgr.ds = NewMapDatastore()

	var td = sch.TimerDescription{
		Name:  "TmPrdMgrCleanup",
		Utid:  sch.DhtPrdMgrCleanupTimerId,
		Tmt:   sch.SchTmTypePeriod,
		Dur:   prdCleanupInterval,
		Extra: nil,
	}

	eno, tid := prdMgr.sdl.SchSetTimer(prdMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		prdLog.Debug("poweron: SchSetTimer failed, eno: %d", eno)
		return eno
	}
	prdMgr.clrTid = tid

	return sch.SchEnoNone
}

//
// power off handler
//
func (prdMgr *PrdMgr) poweroff(ptn interface{}) sch.SchErrno {
	prdLog.Debug("poweroff: task will be done ...")
	return prdMgr.sdl.SchTaskDone(ptn, prdMgr.name, sch.SchEnoKilled)
}

//
// cleanup timer handler
//
func (prdMgr *PrdMgr) cleanupTimer() sch.SchErrno {

	prdMgr.lockCache.Lock()
	defer prdMgr.lockCache.Unlock()

	c := prdMgr.prdCache
	now := time.Now()
	keys := prdMgr.prdCache.Keys()

	for _, k := range keys {

		if i, ok := c.Get(k); ok {

			del := make([]DsKey, 0)
			ps := i.(*PrdSet)

			for id, t := range ps.addTime {
				if now.Sub(t) >= prdLifeCached {
					del = append(del, id)
				}
			}

			for _, id := range del {
				delete(ps.addTime, id)
				delete(ps.set, id)
			}

			if len(ps.set) == 0 || len(ps.addTime) == 0 {
				prdMgr.prdCache.Remove(i)
			}
		}
	}

	return sch.SchEnoNone
}

//
// local add provider request handler
//
func (prdMgr *PrdMgr) localAddProviderReq(msg *sch.MsgDhtPrdMgrAddProviderReq) sch.SchErrno {

	//
	// we are commanded to add a provider by external module of local
	//

	if len(msg.Key) != DsKeyLength {
		prdLog.Debug("localAddProviderReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// cache it
	//

	if eno := prdMgr.cache(&k, &msg.Prd); eno != DhtEnoNone {
		prdLog.Debug("localAddProviderReq: cache failed, eno: %d", eno)
		prdMgr.localAddProviderRsp(msg.Key, nil, eno)
		return sch.SchEnoUserTask
	}

	//
	// store it
	//

	if eno := prdMgr.store(&k, &msg.Prd); eno != DhtEnoNone {
		prdLog.Debug("localAddProviderReq: store failed, eno: %d", eno)
		prdMgr.localAddProviderRsp(msg.Key, nil, eno)
		return sch.SchEnoUserTask
	}

	//
	// publish it to our neighbors
	//

	qry := sch.MsgDhtQryMgrQueryStartReq{
		Target:  k,
		Msg:     msg,
		ForWhat: MID_PUTPROVIDER,
		Seq:     GetQuerySeqNo(prdMgr.sdl.SchGetP2pCfgName()),
	}

	schMsg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, prdMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return prdMgr.sdl.SchSendMessage(&schMsg)
}

//
// local get provider request handler
//
func (prdMgr *PrdMgr) localGetProviderReq(msg *sch.MsgDhtMgrGetProviderReq) sch.SchErrno {

	if len(msg.Key) != DsKeyLength {
		return sch.SchEnoParameter
	}

	var dsk DsKey
	copy(dsk[0:], msg.Key)

	var prds = make([]*config.Node, 0)
	var eno = DhtErrno(DhtEnoUnknown)
	var qry = sch.MsgDhtQryMgrQueryStartReq{}
	var schMsg *sch.SchMessage

	//
	// lookup cache
	//

	if prdSet := prdMgr.prdFromCache(&dsk); prdSet != nil {
		for _, id := range prdSet.set {
			prds = append(prds, &id)
		}
		if len(prds) > 0 {
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
		if len(prds) > 0 {
			eno = DhtEnoNone
			goto _rsp2DhtMgr
		}
	}

	//
	// lookup our neighbors
	//

	qry = sch.MsgDhtQryMgrQueryStartReq{
		Target:  dsk,
		Msg:     nil,
		ForWhat: MID_GETPROVIDER_REQ,
		Seq:     GetQuerySeqNo(prdMgr.sdl.SchGetP2pCfgName()),
	}

	schMsg = new(sch.SchMessage)
	prdMgr.sdl.SchMakeMessage(schMsg, prdMgr.ptnMe, prdMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return prdMgr.sdl.SchSendMessage(schMsg)

_rsp2DhtMgr:

	return prdMgr.localGetProviderRsp(msg.Key, prds, eno)
}

//
// qryMgr query result indication handler
//
func (prdMgr *PrdMgr) qryMgrQueryResultInd(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {

	if msg.ForWhat == MID_PUTPROVIDER {

		key := msg.Target[0:]
		return prdMgr.localAddProviderRsp(key, msg.Peers, DhtErrno(msg.Eno))

	} else if msg.ForWhat == MID_GETPROVIDER_REQ {

		key := msg.Target[0:]
		prds := msg.Prds

		//
		// cache and store providers indicated
		//

		var dsk DsKey
		copy(dsk[0:], key)

		for _, prd := range prds {

			if prdMgr.cache(&dsk, prd) != DhtEnoNone {
				prdLog.Debug("qryMgrQueryResultInd: cache failed")
			}

			if prdMgr.store(&dsk, prd) != DhtEnoNone {
				prdLog.Debug("qryMgrQueryResultInd: store failed")
			}
		}

		return prdMgr.localGetProviderRsp(key, prds, DhtErrno(msg.Eno))

	} else {
		prdLog.Debug("qryMgrQueryResultInd: not matched with prdMgr")
	}

	return sch.SchEnoMismatched
}

//
// put provider request handler
//
func (prdMgr *PrdMgr) putProviderReq(msg *sch.MsgDhtPrdMgrPutProviderReq) sch.SchErrno {

	//
	// we are required to put-provider by remote peer, we just put it into the
	// cache and data store.
	//

	dsk := DsKey{}
	pp := msg.Msg.(*PutProvider)
	prd := pp.Provider

	copy(dsk[0:], prd.Key)
	for _, n := range prd.Nodes {
		if prdMgr.cache(&dsk, n) != DhtEnoNone {
			prdLog.Debug("putProviderReq: cache failed")
		}
		if prdMgr.store(&dsk, n) != DhtEnoNone {
			prdLog.Debug("putProviderReq: store failed")
		}
	}

	return sch.SchEnoNone
}

//
// get provider handler
//
func (prdMgr *PrdMgr) getProviderReq(msg *sch.MsgDhtPrdMgrGetProviderReq) sch.SchErrno {

	//
	// we are required to get-provider by remote peer
	//

	dsk := DsKey{}
	req := msg.Msg.(*GetProviderReq)
	ci := msg.ConInst.(*ConInst)
	rsp := GetProviderRsp{
		From:     *ci.local,
		To:       ci.hsInfo.peer,
		Provider: nil,
		Key:      nil,
		Nodes:    nil,
		Pcs:      nil,
		Id:       req.Id,
		Extra:    nil,
	}

	makeDhtPrd := func(dsk *DsKey, ps *PrdSet) *DhtProvider {
		prd := DhtProvider{
			Key:   dsk[0:],
			Nodes: nil,
		}
		for _, p := range ps.set {
			prd.Nodes = append(prd.Nodes, &p)
		}
		return &prd
	}

	//
	// check cache and data store
	//

	var dhtPrd *DhtProvider = nil
	copy(dsk[0:], req.Key)

	if prdSet := prdMgr.prdFromCache(&dsk); prdSet != nil {

		dhtPrd = makeDhtPrd(&dsk, prdSet)

	} else if prdSet = prdMgr.prdFromStore(&dsk); prdSet != nil {

		dhtPrd = makeDhtPrd(&dsk, prdSet)
	}

	if dhtPrd == nil {
		log.Debugf("getProviderReq: no providers for key: %x", dsk)
		return sch.SchEnoNotFound
	}

	rsp.Provider = dhtPrd

	//
	// if providers got, we then response peer
	//

	if rsp.Provider != nil {

		dhtMsg := DhtMessage{
			Mid:            MID_GETPROVIDER_RSP,
			GetProviderRsp: &rsp,
		}

		dhtPkg := DhtPackage{}
		if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
			prdLog.Debug("getProviderReq: GetPackage failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		txReq := sch.MsgDhtConInstTxDataReq{
			Task:    prdMgr.ptnMe,
			WaitRsp: false,
			WaitMid: -1,
			WaitSeq: -1,
			Payload: &dhtPkg,
		}

		schMsg := sch.SchMessage{}
		prdMgr.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstTxDataReq, &txReq)
		return prdMgr.sdl.SchSendMessage(&schMsg)
	}

	//
	// we have to ask the route manager for nearest for key requested
	//

	fnReq := sch.MsgDhtRutMgrNearestReq{
		Target:  dsk,
		Max:     rutMgrMaxNearest,
		NtfReq:  false,
		Task:    prdMgr.ptnMe,
		ForWhat: MID_FINDNODE,
		Msg:     msg,
	}
	schMsg := sch.SchMessage{}
	ci.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, prdMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &fnReq)
	return ci.sdl.SchSendMessage(&schMsg)
}

//
// nearest response handler
//
func (prdMgr *PrdMgr) rutMgrNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	//
	// see prdMgr.getProviderReq for more please. we assume that no more "get-provider"
	// would be sent from the same connection instance until this function called.
	//

	if msg.Eno != int(DhtEnoNone) {
		prdLog.Debug("rutMgrNearestRsp: router failed, eno: %d", msg.Eno)
		return sch.SchEnoUserTask
	}
	var nodes []*config.Node
	bns := msg.Peers.([]*rutMgrBucketNode)
	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	ci := msg.Msg.(*sch.MsgDhtPrdMgrGetProviderReq).ConInst.(*ConInst)
	req := msg.Msg.(*sch.MsgDhtPrdMgrGetProviderReq).Msg.(*GetProviderReq)
	rsp := GetProviderRsp{
		From:     *ci.local,
		To:       ci.hsInfo.peer,
		Provider: nil,
		Key:      req.Key,
		Nodes:    nodes,
		Pcs:      msg.Pcs.([]int),
		Id:       req.Id,
		Extra:    nil,
	}

	dhtMsg := DhtMessage{
		Mid:            MID_GETPROVIDER_RSP,
		GetProviderRsp: &rsp,
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		prdLog.Debug("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	txReq := sch.MsgDhtConInstTxDataReq{
		Task:    prdMgr.ptnMe,
		WaitRsp: false,
		WaitMid: -1,
		WaitSeq: -1,
		Payload: &dhtPkg,
	}

	schMsg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&schMsg, prdMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstTxDataReq, &txReq)
	return prdMgr.sdl.SchSendMessage(&schMsg)
}

//
// try get provider from cache
//
func (prdMgr *PrdMgr) prdFromCache(key *DsKey) *PrdSet {

	prdMgr.lockCache.Lock()
	defer prdMgr.lockCache.Unlock()

	if key == nil {
		return nil
	}

	if val, ok := prdMgr.prdCache.Get(key); ok {
		return val.(*PrdSet)
	}

	return nil
}

//
// try get provider form data store
//
func (prdMgr *PrdMgr) prdFromStore(key *DsKey) *PrdSet {

	prdMgr.lockStore.Lock()
	defer prdMgr.lockStore.Unlock()

	if key == nil {
		return nil
	}

	eno, val := prdMgr.ds.Get(key[0:])
	if eno != DhtEnoNone || val == nil {
		return nil
	}

	return val.(*PrdSet)
}

//
// store a provider
//
func (prdMgr *PrdMgr) store(key *DsKey, peerId *config.Node) DhtErrno {

	prdMgr.lockStore.Lock()
	defer prdMgr.lockStore.Unlock()

	if key == nil || peerId == nil {
		return DhtEnoParameter
	}

	var dpsr = &DhtProviderStoreRecord{
		Key:       key[0:],
		Providers: nil,
		Extra:     nil,
	}

	if eno, val := prdMgr.ds.Get(key[0:]); eno == DhtEnoNone && val != nil {
		psr := val.(*PsRecord)
		if eno := dpsr.DecPsRecord(psr); eno != DhtEnoNone {
			prdLog.Debug("store: DecPsRecord failed, eno: %d", eno)
			return eno
		}
	}

	for _, prd := range dpsr.Providers {
		if bytes.Equal(prd.ID[0:], peerId.ID[0:]) {
			prdLog.Debug("store: duplicated provider")
			return DhtEnoNone
		}
	}

	var psr = PsRecord{KT: prdDftKeepTime}
	dpsr.Providers = append(dpsr.Providers, peerId)
	if eno := dpsr.EncPsRecord(&psr); eno != DhtEnoNone {
		prdLog.Debug("store: EncPsRecord failed, eno: %d", eno)
		return eno
	}

	return prdMgr.ds.Put(key[0:], psr.Value, psr.KT)
}

//
// response to local dhtMgr for "add-provider"
//
func (prdMgr *PrdMgr) localAddProviderRsp(key []byte, peers []*config.Node, eno DhtErrno) sch.SchErrno {
	rsp := sch.MsgDhtPrdMgrAddProviderRsp{
		Eno:   int(eno),
		Key:   key,
		Peers: peers,
	}
	msg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&msg, prdMgr.ptnMe, prdMgr.ptnDhtMgr, sch.EvDhtMgrPutProviderRsp, &rsp)
	return prdMgr.sdl.SchSendMessage(&msg)
}

//
// response to local dhtMgr for "get-provider"
//
func (prdMgr *PrdMgr) localGetProviderRsp(key []byte, prds []*config.Node, eno DhtErrno) sch.SchErrno {
	rsp := sch.MsgDhtMgrGetProviderRsp{
		Eno:  int(eno),
		Key:  key,
		Prds: prds,
	}
	msg := sch.SchMessage{}
	prdMgr.sdl.SchMakeMessage(&msg, prdMgr.ptnMe, prdMgr.ptnDhtMgr, sch.EvDhtMgrGetProviderRsp, &rsp)
	return prdMgr.sdl.SchSendMessage(&msg)
}

//
// cache a provider
//
func (prdMgr *PrdMgr) cache(k *DsKey, prd *config.Node) DhtErrno {

	if prdSet := prdMgr.prdFromCache(k); prdSet != nil {

		prdMgr.lockCache.Lock()
		defer prdMgr.lockCache.Unlock()

		if _, dup := prdSet.set[*k]; !dup {
			prdSet.append(*k, prd, time.Now())
		}

	} else {

		newPrd := PrdSet{
			set:     map[DsKey]config.Node{*k: *prd},
			addTime: map[DsKey]time.Time{*k: time.Now()},
		}

		prdMgr.lockCache.Lock()
		defer prdMgr.lockCache.Unlock()

		prdMgr.prdCache.Add(&k, &newPrd)
	}

	return DhtEnoNone
}

//
// add new provider to set
//
func (prdSet *PrdSet) append(key DsKey, peerId *config.Node, addTime time.Time) {
	if _, exist := prdSet.addTime[key]; !exist {
		prdSet.set[key] = *peerId
	}
	prdSet.addTime[key] = addTime
}
