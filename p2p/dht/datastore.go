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
	"path"
	"strconv"
	"strings"
	"runtime"
	"container/list"
	"github.com/syndtr/goleveldb/leveldb/opt"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type dsLogger struct {
	debug__		bool
}

var dsLog = dsLogger {
	debug__:	false,
}

func (log dsLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Datastore key
//
const DsKeyLength = config.DhtKeyLength
type DsKey =  config.DsKey

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

	Put(key []byte, value DsValue, kt time.Duration) DhtErrno

	//
	// Get (key, value) from data store
	//

	Get(key []byte) (eno DhtErrno, value DsValue)

	//
	// Delete (key, value) from data store
	//

	Delete(key []byte) DhtErrno

	//
	// Close
	//

	Close() DhtErrno
}

//
// Data store record
//
type DsRecord struct {
	Key		DsKey				// record key
	Value	DsValue				// record value
}

//
// Data store manager name registered in scheduler
//
const DsMgrName = sch.DhtDsMgrName

//
// data store type
//
const (
	dstMemoryMap	= iota
	dstFileSystem
	dstLevelDB
)

//
// data store type applying
//
var dsType = dstLevelDB

//
// infinite
//
const DsMgrDurInf = time.Duration(0)


//
// Data store manager
//
type DsMgr struct {
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// my name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to task node of myself
	ptnDhtMgr	interface{}				// pointer to dht manager task node
	ptnQryMgr	interface{}				// pointer to query manager task node
	ptnRutMgr	interface{}				// pointer to route manager task node
	getfromPeer	bool					// do not try getting value from local store, true for debug/test only
	ds			Datastore				// data store
	dsExp		Datastore				// data store for expired time
	fdsCfg		FileDatastoreConfig		// file data store configuration
	ldsCfg		LeveldbDatastoreConfig	// levelDB stat store configuration
	tmMgr		*TimerManager			// timer manager
	tidTick		int						// tick timer identity
}

//
// Create data store manager
//
func NewDsMgr() *DsMgr {

	dsMgr := DsMgr{
		name:			DsMgrName,
		getfromPeer:	false,
		fdsCfg:			FileDatastoreConfig{},
		ldsCfg:			LeveldbDatastoreConfig{},
		tmMgr:			NewTimerManager(),
		tidTick:		sch.SchInvalidTid,
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
// Datastore manager entry
//
func (dsMgr *DsMgr)dsMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	dsLog.Debug("dsMgrProc: name: %s, msg.Id: %d", dsMgr.name, msg.Id)

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dsMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = dsMgr.poweroff(ptn)

	case sch.EvDhtDsMgrTickTimer:
		eno = dsMgr.tickTimerHandler()

	case sch.EvDhtDsMgrAddValReq:
		eno = dsMgr.localAddValReq(msg.Body.(*sch.MsgDhtDsMgrAddValReq))

	case sch.EvDhtMgrGetValueReq:
		eno = dsMgr.localGetValueReq(msg.Body.(*sch.MsgDhtMgrGetValueReq))

	case sch.EvDhtQryMgrQueryResultInd:
		eno = dsMgr.qryMgrQueryResultInd(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtDsMgrPutValReq:
		eno = dsMgr.putValReq(msg.Body.(*sch.MsgDhtDsMgrPutValReq))

	case sch.EvDhtDsMgrGetValReq:
		eno = dsMgr.getValReq(msg.Body.(*sch.MsgDhtDsMgrGetValReq))

	case sch.EvDhtRutMgrNearestRsp:
		eno = dsMgr.rutMgrNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	case sch.EvDhtQryMgrQueryStartRsp:
		eno = dsMgr.qryStartRsp(msg.Body.(*sch.MsgDhtQryMgrQueryStartRsp))

	default:
		eno = sch.SchEnoParameter
		dsLog.Debug("dsMgrProc: unknown message: %d", msg.Id)
	}

	dsLog.Debug("dsMgrProc: get out, name: %s, msg.Id: %d", dsMgr.name, msg.Id)

	return eno
}

//
// put
//
func (dsMgr *DsMgr)Put(k []byte, v DsValue, kt time.Duration) DhtErrno {

	if eno := dsMgr.ds.Put(k, v, kt); eno != DhtEnoNone {
		dsLog.Debug("Put: failed, eno: %d", eno)
		return DhtEnoDatastore
	}

	// notice following codes does not delete the [key, value] had been stored
	// even timer failed to be startup.

	if kt != DsMgrDurInf {

		ptm, eno := dsMgr.tmMgr.GetTimer(kt, nil, nil)
		if eno != TmEnoNone {
			dsLog.Debug("Put: GetTimer failed, eno: %d", eno)
			return DhtEnoTimer
		}

		tm := ptm.(*timer)
		dsMgr.tmMgr.SetTimerData(tm, tm)
		dsMgr.tmMgr.SetTimerKey(tm, k)
		dsMgr.tmMgr.SetTimerHandler(tm, dsMgr.cleanUpTimerCb)

		tm.to = time.Now().Add(kt)
		ek := dsMgr.makeExpiredKey(k, tm.to)
		if eno = dsMgr.dsExp.Put(ek, k, sch.Keep4Ever); eno != DhtEnoNone {
			dsLog.Debug("Put: failed, eno: %d", eno)
			return DhtEnoDatastore
		}

		if err := dsMgr.tmMgr.StartTimer(tm); err != nil {
			dsLog.Debug("Put: StartTimer failed, error: %s", err.Error())
			dsMgr.ds.Delete(k)
			dsMgr.dsExp.Delete(ek)
			return DhtEnoTimer
		}
	}

	return DhtEnoNone
}

//
// get
//
func (dsMgr *DsMgr)Get(k []byte) (eno DhtErrno, value DsValue) {
	return dsMgr.ds.Get(k)
}

//
// delete
//
func (dsMgr *DsMgr)Delete(k []byte) DhtErrno {
	// timer might be in running, and would be removed when expired if any,
	// just delete [key, val] from the "real" store here.
	return dsMgr.ds.Delete(k)
}

//
// poweron handler
//
func (dsMgr *DsMgr)poweron(ptn interface{}) sch.SchErrno {

	if ptn == nil {
		dsLog.Debug("poweron: invalid ptn")
		return sch.SchEnoInternal
	}

	sdl := sch.SchGetScheduler(ptn)
	dsMgr.sdl = sdl
	if sdl == nil {
		dsLog.Debug("poweron: invalid sdl")
		return sch.SchEnoInternal
	}

	dsMgr.ptnMe = ptn
	_, dsMgr.ptnDhtMgr = sdl.SchGetUserTaskNode(DhtMgrName)
	_, dsMgr.ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName)
	_, dsMgr.ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName)

	if dsMgr.ptnDhtMgr == nil ||
		dsMgr.ptnQryMgr == nil ||
		dsMgr.ptnRutMgr == nil {
		dsLog.Debug("poweron: invalid ptn")
		return sch.SchEnoInternal
	}

	if dsType == dstMemoryMap {

		dsMgr.ds = NewMapDatastore()
		dsMgr.dsExp = NewMapDatastore()

	} else if dsType == dstFileSystem {

		fdc := FileDatastoreConfig{}
		if eno := dsMgr.getFileDatastoreConfig(&fdc); eno != DhtEnoNone {
			dsLog.Debug("poweron: getFileDatastoreConfig failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		dsMgr.ds = NewFileDatastore(&fdc)

		fdcExp := fdc
		fdcExp.path = path.Join(fdcExp.path, "expired")
		dsMgr.dsExp = NewFileDatastore(&fdcExp)

	} else if dsType == dstLevelDB {

		ldc := LeveldbDatastoreConfig{}
		if eno := dsMgr.getLeveldbDatastoreConfig(&ldc); eno != DhtEnoNone {
			dsLog.Debug("poweron: getFileDatastoreConfig failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		if runtime.GOOS == "windows" {
			p2plog.Debug("poweron: dht datastore path: %s",
				strings.Replace(ldc.Path, "/", "\\", -1))
		} else {
			p2plog.Debug("poweron: dht datastore path: %s", ldc.Path)
		}
		dsMgr.ds = NewLeveldbDatastore(&ldc)

		ldcExp := ldc
		ldcExp.Path = path.Join(ldcExp.Path, "expired")
		if runtime.GOOS == "windows" {
			p2plog.Debug("poweron: dht expiration datastore path: %s",
				strings.Replace(ldcExp.Path, "/", "\\", -1))
		} else {
			p2plog.Debug("poweron: dht expiration datastore path: %s", ldcExp.Path)
		}
		dsMgr.dsExp = NewLeveldbDatastore(&ldcExp)

	} else {
		dsLog.Debug("poweron: invalid datastore type: %d", dsType)
		return sch.SchEnoNotImpl
	}

	if dsMgr.ds == nil {
		dsLog.Debug("poweron: nil datastore")
		return sch.SchEnoUserTask
	}

	if eno := dsMgr.cleanUpReboot(); eno != DhtEnoNone {
		dsLog.Debug("poweron: cleanUp failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if eno := dsMgr.startTickTimer(); eno != DhtEnoNone {
		dsLog.Debug("poweron: startTickTimer failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dsMgr *DsMgr)poweroff(ptn interface{}) sch.SchErrno {
	dsLog.Debug("poweroff: task will be done ...")
	dsMgr.ds.Close()
	dsMgr.dsExp.Close()
	return dsMgr.sdl.SchTaskDone(dsMgr.ptnMe, sch.SchEnoKilled)
}

//
// tick timer handler
//
func (dsMgr *DsMgr)tickTimerHandler() sch.SchErrno {
	if err := dsMgr.tmMgr.TickProc(); err != TmEnoNone {
		dsLog.Debug("TickProc: error: %s", err.Error())
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

//
// add value request handler
//
func (dsMgr *DsMgr)localAddValReq(msg *sch.MsgDhtDsMgrAddValReq) sch.SchErrno {

	if len(msg.Key) != DsKeyLength {
		dsLog.Debug("localAddValReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// store it
	//

	if eno := dsMgr.store(&k, msg.Val, msg.KT); eno != DhtEnoNone {
		dsLog.Debug("localAddValReq: store failed, eno: %d", eno)
		dsMgr.localAddValRsp(k[0:], nil, eno)
		return sch.SchEnoUserTask
	}

	//
	// publish it to our neighbors
	//

	qry := sch.MsgDhtQryMgrQueryStartReq {
		Target:		k,
		Msg:		msg,
		ForWhat:	MID_PUTVALUE,
		Seq:		GetQuerySeqNo(),
	}

	schMsg := sch.SchMessage{}
	dsMgr.sdl.SchMakeMessage(&schMsg, dsMgr.ptnMe, dsMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return dsMgr.sdl.SchSendMessage(&schMsg)
}

//
// local node get-value request handler
//
func (dsMgr *DsMgr)localGetValueReq(msg *sch.MsgDhtMgrGetValueReq) sch.SchErrno {

	if len(msg.Key) != DsKeyLength {
		dsLog.Debug("localGetValueReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// try local data store
	//

	if !dsMgr.getfromPeer {
		if val := dsMgr.fromStore(&k); val != nil && len(val) > 0 {
			return dsMgr.localGetValRsp(k[0:], val, DhtEnoNone)
		}
	}

	//
	// try to fetch the value from peers
	//

	qry := sch.MsgDhtQryMgrQueryStartReq {
		Target:		k,
		Msg:		msg,
		ForWhat:	MID_GETVALUE_REQ,
		Seq:		GetQuerySeqNo(),
	}

	schMsg := sch.SchMessage{}
	dsMgr.sdl.SchMakeMessage(&schMsg, dsMgr.ptnMe, dsMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &qry)
	return dsMgr.sdl.SchSendMessage(&schMsg)
}

//
// qryMgr query result indication handler
//
func (dsMgr *DsMgr)qryMgrQueryResultInd(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {

	if msg.ForWhat == MID_PUTVALUE {

		return dsMgr.localAddValRsp(msg.Target[0:], msg.Peers, DhtErrno(msg.Eno))

	} else if msg.ForWhat == MID_GETVALUE_REQ {

		return dsMgr.localGetValRsp(msg.Target[0:], msg.Val, DhtErrno(msg.Eno))

	} else {
		dsLog.Debug("qryMgrQueryResultInd: unknown what's for")
	}

	return sch.SchEnoMismatched
}

//
// put value request handler
//
func (dsMgr *DsMgr)putValReq(msg *sch.MsgDhtDsMgrPutValReq) sch.SchErrno {

	//
	// we are requested to put value from remote peer
	//

	pv, _ := msg.Msg.(*PutValue)
	dsk := DsKey{}

	for _, v := range pv.Values {

		copy(dsk[0:], v.Key)
		dsLog.Debug("putValReq: key: %x", dsk)

		if eno := dsMgr.store(&dsk, v.Val, pv.KT); eno != DhtEnoNone {
			dsLog.Debug("putValReq: store failed, eno: %d", eno)
		}
	}

	return sch.SchEnoNone
}

//
// get value request handler
//
func (dsMgr *DsMgr)getValReq(msg *sch.MsgDhtDsMgrGetValReq) sch.SchErrno {

	//
	// we are requested to get value for remote peer
	//

	gvReq, _ := msg.Msg.(*GetValueReq)
	conInst := msg.ConInst.(*ConInst)
	gvRsp := GetValueRsp {
		From:		*conInst.local,
		To:			conInst.hsInfo.peer,
		Value:		nil,
		Nodes:		nil,
		Pcs:		nil,
		Id:			gvReq.Id,
		Extra:		nil,
	}

	dsk := DsKey{}
	copy(dsk[0:], gvReq.Key)

	dsLog.Debug("getValReq: key: %x", dsk)

	rsp2Peer := func() sch.SchErrno {

		dhtMsg := DhtMessage {
			Mid:			MID_GETVALUE_RSP,
			GetValueRsp:	&gvRsp,
		}

		dhtPkg := DhtPackage{}
		if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
			dsLog.Debug("getValReq: GetPackage failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		txReq := sch.MsgDhtConInstTxDataReq {
			Task:		dsMgr.ptnMe,
			WaitRsp:	false,
			WaitMid:	-1,
			WaitSeq:	-1,
			Payload:	&dhtPkg,
		}

		schMsg := sch.SchMessage{}
		dsMgr.sdl.SchMakeMessage(&schMsg, dsMgr.ptnMe, conInst.ptnMe, sch.EvDhtConInstTxDataReq, &txReq)
		return dsMgr.sdl.SchSendMessage(&schMsg)
	}

	//
	// check local data store
	//

	if val := dsMgr.fromStore(&dsk); len(val) > 0 {
		gvRsp.Value = &DhtValue {
			Key:	dsk[0:],
			Val:	val,
		}
		return rsp2Peer()
	}

	//
	// check provider manager
	//

	prdMgr, ok := dsMgr.sdl.SchGetTaskObject(PrdMgrName).(*PrdMgr)
	if !ok || prdMgr == nil {
		dsLog.Debug("getValReq: get provider manager failed")
		return sch.SchEnoInternal
	}

	if prdSet := prdMgr.prdFromCache(&dsk); prdSet != nil {
		for _, p := range prdSet.set {
			n := p
			gvRsp.Nodes = append(gvRsp.Nodes, &n)
		}
		return rsp2Peer()
	}

	//
	// we have to ask route manager for nearest for key requested
	//

	schMsg := sch.SchMessage{}
	fnReq := sch.MsgDhtRutMgrNearestReq{
		Target:		dsk,
		Max:		rutMgrMaxNearest,
		NtfReq:		false,
		Task:		dsMgr.ptnMe,
		ForWhat:	MID_FINDNODE,
		Msg:		msg,
	}

	conInst.sdl.SchMakeMessage(&schMsg, dsMgr.ptnMe, dsMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &fnReq)
	return conInst.sdl.SchSendMessage(&schMsg)
}

//
// nearest response handler
//
func (dsMgr *DsMgr)rutMgrNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	//
	// see dsMgr.getValReq for more please. we assume that no more "get-value"
	// would be sent from the same connection instance until this function called.
	//

	if msg == nil {
		dsLog.Debug("rutMgrNearestRsp: invalid message")
		return sch.SchEnoParameter
	}

	var nodes []*config.Node
	bns := msg.Peers.([]*rutMgrBucketNode)
	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	ci, _ := msg.Msg.(*sch.MsgDhtDsMgrGetValReq).ConInst.(*ConInst)
	req, _ := msg.Msg.(*sch.MsgDhtDsMgrGetValReq).Msg.(*GetValueReq)
	rsp := GetValueRsp {
		From:		*ci.local,
		To:			ci.hsInfo.peer,
		Key:		req.Key,
		Value:		nil,
		Nodes:		nodes,
		Pcs:		msg.Pcs.([]int),
		Id:			req.Id,
		Extra:		nil,
	}

	dhtMsg := DhtMessage {
		Mid:			MID_GETVALUE_RSP,
		GetValueRsp:	&rsp,
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		dsLog.Debug("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	txReq := sch.MsgDhtConInstTxDataReq {
		Task:		dsMgr.ptnMe,
		WaitRsp:	false,
		WaitMid:	-1,
		WaitSeq:	-1,
		Payload:	&dhtPkg,
	}

	schMsg := sch.SchMessage{}
	dsMgr.sdl.SchMakeMessage(&schMsg, dsMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstTxDataReq, &txReq)
	return dsMgr.sdl.SchSendMessage(&schMsg)
}

func (dsMgr *DsMgr)qryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	if msg.Eno != DhtEnoNone.GetEno() {
		dsLog.Debug("qryStartRsp: errors reported, eno: %d", msg.Eno)
	}
	return sch.SchEnoNone
}

//
// get value from store by key
//
func (dsMgr *DsMgr)fromStore(k *DsKey) []byte {

	eno, val := dsMgr.ds.Get(k[0:])
	if eno != DhtEnoNone {
		return  nil
	}

	dsr := DsRecord {
		Key:	*k,
		Value:	val,
	}

	ddsr := &DhtDatastoreRecord {
		Key:	k[0:],
		Value:	nil,
		Extra:	nil,
	}

	if eno := ddsr.DecDsRecord(&dsr); eno != DhtEnoNone {
		dsLog.Debug("fromStore: DecDsRecord failed, eno: %d", eno)
		return nil
	}

	return  ddsr.Value
}

//
// store (key, value) pair to data store
//
func (dsMgr *DsMgr)store(k *DsKey, v DsValue, kt time.Duration) DhtErrno {

	ddsr := DhtDatastoreRecord {
		Key:	k[0:],
		Value:	v.([]byte),
		Extra:	nil,
	}

	dsr := new(DsRecord)
	if eno := ddsr.EncDsRecord(dsr); eno != DhtEnoNone {
		dsLog.Debug("store: EncDsRecord failed, eno: %d", eno)
		return eno
	}

	return dsMgr.ds.Put(dsr.Key[0:], dsr.Value, kt)
}

//
// response the add-value request sender task
//
func (dsMgr *DsMgr)localAddValRsp(key []byte, peers []*config.Node, eno DhtErrno) sch.SchErrno {

	rsp := sch.MsgDhtMgrPutValueRsp {
		Eno:	int(eno),
		Key:	key,
		Peers:	peers,
	}

	msg := sch.SchMessage{}
	dsMgr.sdl.SchMakeMessage(&msg, dsMgr.ptnMe, dsMgr.ptnDhtMgr, sch.EvDhtMgrPutValueRsp, &rsp)
	return dsMgr.sdl.SchSendMessage(&msg)
}

//
// response the get-value request sender task
//
func (dsMgr *DsMgr)localGetValRsp(key []byte, val []byte, eno DhtErrno) sch.SchErrno {

	rsp := sch.MsgDhtMgrGetValueRsp {
		Eno:	int(eno),
		Key:	key,
		Val:	val,
	}

	msg := sch.SchMessage{}
	dsMgr.sdl.SchMakeMessage(&msg, dsMgr.ptnMe, dsMgr.ptnDhtMgr, sch.EvDhtMgrGetValueRsp, &rsp)
	return dsMgr.sdl.SchSendMessage(&msg)
}

//
// get file data store configuration
//
func (dsMgr *DsMgr)getFileDatastoreConfig(fdc *FileDatastoreConfig) DhtErrno {

	cfg := config.P2pConfig4DhtFileDatastore(dsMgr.sdl.SchGetP2pCfgName())
	dsMgr.fdsCfg = FileDatastoreConfig {
		path:			path.Join(cfg.Path, "fds"),
		shardFuncName:	cfg.ShardFuncName,
		padLength:		cfg.PadLength,
		sync:			cfg.Sync,
	}

	*fdc = dsMgr.fdsCfg

	return DhtEnoNone
}

//
// get levelDB data store configuration
//
func (dsMgr *DsMgr)getLeveldbDatastoreConfig(ldc *LeveldbDatastoreConfig) DhtErrno {

	cfg := config.P2pConfig4DhtFileDatastore(dsMgr.sdl.SchGetP2pCfgName())
	dsMgr.ldsCfg = LeveldbDatastoreConfig {
		Path:					path.Join(cfg.Path, "lds"),
		OpenFilesCacheCapacity:	500,
		BlockCacheCapacity:		8 * opt.MiB,
		BlockSize:				4 * opt.MiB,
		FilterBits:				10,
	}

	*ldc = dsMgr.ldsCfg

	return DhtEnoNone
}

//
// cleanup in power on stage
//
func (dsMgr *DsMgr)cleanUpReboot() DhtErrno {

	// clean up those [key, val] pairs out of keep time. we loop the "expired" database
	// keys, split them get the "real" key for the value, and then delete the "expired"
	// keys and the "real" [key, val] pairs.

	if dsType == dstMemoryMap {

		return DhtEnoNone

	} else if dsType == dstFileSystem {

		return DhtEnoNotSup

	} else if dsType == dstLevelDB {

		lds, _ := dsMgr.dsExp.(*LeveldbDatastore)
		ldb := lds.ls.GetLevelDB()
		it := ldb.NewIterator(nil, nil)

		for it.First() {

			ek := it.Key()
			t, k := dsMgr.splitExpiredKey(ek)
			secondes, err := strconv.ParseInt(string(t), 10, 64)

			if err != nil {
				dsLog.Debug("cleanUpReboot: invalid time string: %s", string(t))
				continue
			}

			if time.Now().Unix() >=  secondes {

				dsMgr.Delete(k)
				lds.Delete(ek)

			} else {

				kt := time.Second * time.Duration(secondes - time.Now().Unix())
				tm, err := dsMgr.tmMgr.GetTimer(kt, nil, nil)

				if err != nil {
					dsLog.Debug("cleanUpReboot: GetTimer failed, error: %s", err.Error())
					continue
				}

				dsMgr.tmMgr.SetTimerData(tm, tm)
				dsMgr.tmMgr.SetTimerKey(tm, k)
				dsMgr.tmMgr.SetTimerHandler(tm, dsMgr.cleanUpTimerCb)
			}
		}
	} else {

		dsLog.Debug("cleanUpReboot: unknown data store type: %d", dsType)
		return DhtEnoDatastore
	}

	return DhtEnoNone
}

func (dsMgr *DsMgr)startTickTimer() DhtErrno {
	td := sch.TimerDescription {
		Name:	"_dsMgrTickTimer",
		Utid:	sch.DhtDsMgrTickTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	oneTick,
		Extra:	nil,
	}

	eno, tid := dsMgr.sdl.SchSetTimer(dsMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		dsLog.Debug("startTickTimer: SchSetTimer failed, eno: %d", eno)
		return DhtEnoScheduler
	}

	dsMgr.tidTick = tid

	return DhtEnoNone
}

//
// called back to clean [key, val] out of keep time
//
func (dsMgr *DsMgr)cleanUpTimerCb(el *list.Element, data interface{})interface{} {

	// notice: need not to call dsMgr.tmMgr.KillTimer, since the timer would
	// be removed by timer manager itself.

	err := DhtErrno(DhtEnoNone)
	tm, ok := data.(*timer)
	if !ok {
		dsLog.Debug("cleanUpTimerCb: invalid timer")
		return DhtEnoMismatched
	}

	expKey := dsMgr.makeExpiredKey(tm.k, tm.to)
	if eno := dsMgr.ds.Delete(tm.k[0:]); eno != DhtEnoNone {
		err = DhtErrno(DhtEnoDatastore)
		goto _cleanUpfailed
	}

	if eno := dsMgr.dsExp.Delete(expKey); eno != DhtEnoNone {
		err = DhtErrno(DhtEnoDatastore)
		goto _cleanUpfailed
	}

_cleanUpfailed:

	if err != DhtEnoNone {
		dsLog.Debug("cleanUpTimerCb: Delete failed, error: %s", err.Error())
		return err
	}

	return nil
}

//
// make a "expired" key for [key, val] which expired
//
func (dsMgr *DsMgr)makeExpiredKey(k []byte, to time.Time) []byte {
	strTime := strconv.FormatInt(to.Unix(), 10)
	strTime = strings.Repeat("0", 16 - len(strTime)) + strTime
	ek := append([]byte(strTime), k...)
	return ek
}

//
// split a "expired" key into "expired time" and "real key"
//
func (dsMgr *DsMgr)splitExpiredKey(expKey []byte) (t []byte, k []byte) {
	return expKey[0:16], expKey[16:]
}

