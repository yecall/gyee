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
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
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
}

//
// Data store record
//
type DsRecord struct {
	Key		DsKey				// record key
	Value	DsValue				// record value
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
// Data store manager name registered in scheduler
//
const DsMgrName = sch.DhtDsMgrName

//
// Memory map store for test flag
//
const mapDs4Test = false

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
	ds			Datastore				// data store
	fdsCfg		FileDatastoreConfig		// file data store configuration
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
		ds:			nil,
		fdsCfg:		FileDatastoreConfig{},
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

	if ptn == nil || msg == nil {
		log.Debug("dsMgrProc: invalid parameters")
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dsMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = dsMgr.poweroff(ptn)

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

	default:
		eno = sch.SchEnoParameter
		log.Debug("dsMgrProc: unknown message: %d", msg.Id)
	}

	return eno
}

//
// poweron handler
//
func (dsMgr *DsMgr)poweron(ptn interface{}) sch.SchErrno {

	if ptn == nil {
		log.Debug("poweron: invalid ptn")
		return sch.SchEnoInternal
	}

	sdl := sch.SchGetScheduler(ptn)
	dsMgr.sdl = sdl
	if sdl == nil {
		log.Debug("poweron: invalid sdl")
		return sch.SchEnoInternal
	}

	dsMgr.ptnMe = ptn
	_, dsMgr.ptnDhtMgr = sdl.SchGetTaskNodeByName(DsMgrName)
	_, dsMgr.ptnQryMgr = sdl.SchGetTaskNodeByName(QryMgrName)
	_, dsMgr.ptnRutMgr = sdl.SchGetTaskNodeByName(RutMgrName)

	if dsMgr.ptnDhtMgr == nil ||
		dsMgr.ptnQryMgr == nil ||
		dsMgr.ptnRutMgr == nil {
		log.Debug("poweron: invalid ptn")
		return sch.SchEnoInternal
	}

	if mapDs4Test == true {

		dsMgr.ds = NewMapDatastore()

	} else {

		fdc := FileDatastoreConfig{}
		if eno := dsMgr.getFileDatastoreConfig(&fdc); eno != DhtEnoNone {
			log.Debug("poweron: getFileDatastoreConfig failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}

		dsMgr.ds = NewFileDatastore(&fdc)
	}

	if dsMgr.ds == nil {
		log.Debug("poweron: invalid ds")
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// poweroff handler
//
func (dsMgr *DsMgr)poweroff(ptn interface{}) sch.SchErrno {
	log.Debug("poweroff: task will be done ...")
	return dsMgr.sdl.SchTaskDone(dsMgr.ptnMe, sch.SchEnoKilled)
}

//
// add value request handler
//
func (dsMgr *DsMgr)localAddValReq(msg *sch.MsgDhtDsMgrAddValReq) sch.SchErrno {

	if len(msg.Key) != DsKeyLength {
		log.Debug("localAddValReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// store it
	//

	if eno := dsMgr.store(&k, msg.Val); eno != DhtEnoNone {
		log.Debug("localAddValReq: store failed, eno: %d", eno)
		dsMgr.localAddValRsp(k[0:], nil, eno)
		return sch.SchEnoUserTask
	}

	//
	// publish it to our neighbors
	//

	qry := sch.MsgDhtQryMgrQueryStartReq {
		Target:		config.NodeID(k),
		Msg:		msg,
		ForWhat:	MID_PUTVALUE,
		Seq:		time.Now().UnixNano(),
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
		log.Debug("localGetValueReq: invalid key length")
		return sch.SchEnoParameter
	}

	var k DsKey
	copy(k[0:], msg.Key)

	//
	// try local data store
	//

	if val := dsMgr.fromStore(&k); val != nil && len(val) > 0 {
		return dsMgr.localGetValRsp(k[0:], val, DhtEnoNone)
	}

	//
	// try to fetch the value from peers
	//

	qry := sch.MsgDhtQryMgrQueryStartReq {
		Target:		config.NodeID(k),
		Msg:		msg,
		ForWhat:	MID_GETVALUE_REQ,
		Seq:		time.Now().UnixNano(),
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
		log.Debug("qryMgrQueryResultInd: unknown what's for")
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

	pv := msg.Msg.(PutValue)
	dsk := DsKey{}

	for _, v := range pv.Values {

		copy(dsk[0:], v.Key)

		if eno := dsMgr.ds.Put(&dsk, v.Val); eno != DhtEnoNone {
			log.Debug("putValReq: put failed, eno: %d", eno)
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

	gvReq := msg.Msg.(GetValueReq)
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

	rsp2Peer := func() sch.SchErrno {

		dhtMsg := DhtMessage {
			Mid:			MID_GETVALUE_RSP,
			GetValueRsp:	&gvRsp,
		}

		dhtPkg := DhtPackage{}
		if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
			log.Debug("getValReq: GetPackage failed, eno: %d", eno)
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
	// check provier manager
	//

	prdMgr, ok := dsMgr.sdl.SchGetUserTaskIF(PrdMgrName).(*PrdMgr)
	if !ok || prdMgr == nil {
		log.Debug("getValReq: get provider manager failed")
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
		Target:		config.NodeID(dsk),
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
		log.Debug("rutMgrNearestRsp: invalid message")
		return sch.SchEnoParameter
	}

	var nodes []*config.Node
	bns := msg.Peers.([]*rutMgrBucketNode)
	for idx := 0; idx < len(bns); idx++ {
		nodes = append(nodes, &bns[idx].node)
	}

	ci := msg.Msg.(*sch.MsgDhtDsMgrGetValReq).ConInst.(*ConInst)
	req := msg.Msg.(*sch.MsgDhtDsMgrGetValReq).Msg.(*GetValueReq)
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
		Mid:			MID_GETPROVIDER_RSP,
		GetValueRsp:	&rsp,
	}

	dhtPkg := DhtPackage{}
	if eno := dhtMsg.GetPackage(&dhtPkg); eno != DhtEnoNone {
		log.Debug("rutMgrNearestRsp: GetPackage failed, eno: %d", eno)
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

//
// get value from store by key
//
func (dsMgr *DsMgr)fromStore(k *DsKey) []byte {

	eno, val := dsMgr.ds.Get(k)
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
		log.Debug("fromStore: DecDsRecord failed, eno: %d", eno)
		return nil
	}

	return  dsr.Value.([]byte)
}

//
// store (key, value) pair to data store
//
func (dsMgr *DsMgr)store(k *DsKey, v DsValue) DhtErrno {

	ddsr := DhtDatastoreRecord {
		Key:	k[0:],
		Value:	v.([]byte),
		Extra:	nil,
	}

	dsr := new(DsRecord)
	if eno := ddsr.EncDsRecord(dsr); eno != DhtEnoNone {
		log.Debug("store: EncDsRecord failed, eno: %d", eno)
		return eno
	}

	return dsMgr.ds.Put(&dsr.Key, dsr.Value)
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
		path:			cfg.Path,
		shardFuncName:	cfg.ShardFuncName,
		padLength:		cfg.PadLength,
		sync:			cfg.Sync,
	}
	*fdc = dsMgr.fdsCfg
	return DhtEnoNone
}
