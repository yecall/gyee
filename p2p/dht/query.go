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
	"container/list"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	config "github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	nat "github.com/yeeco/gyee/p2p/nat"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	log "github.com/yeeco/gyee/log"
)

//
// debug
//
type qryMgrLogger struct {
	debug__ bool
}

var qryLog = qryMgrLogger{
	debug__: false,
}

func (log qryMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// Constants
//
const (
	QryMgrName        = sch.DhtQryMgrName                         	// query manage name registered in shceduler
	QryMgrMailboxSize = 1024 * 24								  		// mail box size
	qryMgrMaxPendings = 512                                       		// max pendings can be held in the list, 0 is unlimited
	qryMgrMaxActInsts = 32                                        		// max concurrent actived instances for one query
	qryMgrQryExpired  = time.Second * 60                          	// duration to get expired for a query
	qryMgrQryMaxWidth = 64                                        		// not the true "width", the max number of peers queryied
	qryMgrQryMaxDepth = 8                                         		// the max depth for a query
	qryInstExpired    = time.Second * 16                          	// duration to get expired for a query instance
	natMapKeepTime    = nat.MinKeepDuration                       	// NAT map keep time
	natMapRefreshTime = nat.MinKeepDuration - nat.MinRefreshDelta	// NAT map refresh time
	qryGvbUnitSize		= 10											// get-value-batch unit size
)

//
// Query manager configuration
//
type qryMgrCfg struct {
	local          *config.Node  // pointer to local node specification
	maxPendings    int           // max pendings can be held in the list
	maxActInsts    int           // max concurrent actived instances for one query
	qryExpired     time.Duration // duration to get expired for a query
	qryInstExpired time.Duration // duration to get expired for a query instance
}

//
// Query control block status
//
type QryStatus = int

const (
	qsNull      = iota // null state
	qsPreparing        // in preparing, waiting for the nearest response from route manager
	qsInited           // had been initialized
)

//
// Query result node info
//
type qryResultInfo struct {
	node config.Node        // peer node info
	pcs  conMgrPeerConnStat // connection status
	dist int                // distance from local node
}

//
// Query pending node info
//
type qryPendingInfo struct {
	rutMgrBucketNode     // bucket node
	depth            int // depth
}

//
// Query control block
//
type qryCtrlBlock struct {
	ptnOwner   interface{}                         // owner task node pointer
	qryReq     *sch.MsgDhtQryMgrQueryStartReq      // original query request message
	seq        int                                 // sequence number
	forWhat    int                                 // what's the query control block for
	target     config.DsKey                        // target for looking up
	status     QryStatus                           // query status
	qryHistory map[config.NodeID]*qryPendingInfo   // history peers had been queried
	qryPending *list.List                          // pending peers to be queried, with type qryPendingInfo
	qryActived map[config.NodeID]*qryInstCtrlBlock // queries activated
	qryResult  *list.List                          // list of qryResultNodeInfo type object
	qryTid     int                                 // query timer identity
	icbSeq     int                                 // query instance control block sequence number
	rutNtfFlag bool                                // if notification asked for
	width      int                                 // the current number of peer had been queried
	depth      int                                 // the current max depth of query
}

//
// get-value-batch control block
//
type gvbCtrlBlock struct {
	gvbId		int				// batch identity
	keys		*[][]byte		// total keys
	output		chan<-[]byte	// value output channel
	head		int				// index of key to query next
	status		int				// current sutatus
	got			int				// number of got
	getting		int				// number of getting
	failed		int				// number of failed
	size		int				// batch size
	remain		int				// number of remain
	pending		map[config.DsKey] interface{} // target key in pending map
}

//
// Query instance status
//
const (
	qisNull         = iota // null state
	qisInited              // had been inited to ready to start
	qisWaitConnect         // connect request sent, wait response from connection manager task
	qisWaitResponse        // query sent, wait response from peer
	qisDone                // done with exception
	qisDoneOk              // done normally
)

//
// Query instance control block
//
type qryInstCtrlBlock struct {
	sdl        *sch.Scheduler                 // pointer to scheduler
	sdlName    string						  // scheduler name
	seq        int                            // sequence number
	qryReq     *sch.MsgDhtQryMgrQueryStartReq // original query request message
	name       string                         // instance name
	ptnInst    interface{}                    // pointer to query instance task node
	ptnConMgr  interface{}                    // pointer to connection manager task node
	ptnQryMgr  interface{}                    // pointer to query manager task node
	ptnRutMgr  interface{}                    // pointer to rute manager task node
	status     int                            // instance status
	local      *config.Node                   // pointer to local node specification
	target     config.DsKey                   // target is looking up
	to         config.Node                    // to whom the query message sent
	dir        int                            // connection direction
	qTid       int                            // query timer identity
	begTime    time.Time                      // query begin time
	endTime    time.Time                      // query end time
	conBegTime time.Time                      // time to start connection
	conEndTime time.Time                      // time connection established
	depth      int                            // the current depth of the query instance
}

//
// Query manager
//
type QryMgr struct {
	sdl          *sch.Scheduler                 // pointer to scheduler
	sdlName		 string							// scheduler name
	name         string                         // query manager name
	tep          sch.SchUserTaskEp              // task entry
	ptnMe        interface{}                    // pointer to task node of myself
	ptnRutMgr    interface{}                    // pointer to task node of route manager
	ptnDhtMgr    interface{}                    // pointer to task node of dht manager
	ptnNatMgr    interface{}                    // pointer to task naode of nat manager
	ptnDsMgr	 interface{}					 // pointer to task datastore manager
	instSeq      int                            // query instance sequence number
	qcbTab       map[config.DsKey]*qryCtrlBlock // query control blocks
	gvbTab		 map[int]*gvbCtrlBlock			// get-value-batch control blocks
	qmCfg        qryMgrCfg                      // query manager configuration
	qcbSeq       int                            // query control block sequence number
	natTcpResult bool                           // result about nap mapping for tcp
	pubTcpIp     net.IP                         // should be same as pubUdpIp
	pubTcpPort   int                            // public port form nat to be announced for tcp
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qmCfg := qryMgrCfg{
		maxPendings:    qryMgrMaxPendings,
		maxActInsts:    qryMgrMaxActInsts,
		qryExpired:     qryMgrQryExpired,
		qryInstExpired: qryInstExpired,
	}

	qryMgr := QryMgr{
		name:       QryMgrName,
		instSeq:    0,
		qcbTab:     map[config.DsKey]*qryCtrlBlock{},
		gvbTab:		map[int]*gvbCtrlBlock{},
		qmCfg:      qmCfg,
		qcbSeq:     0,
		pubTcpIp:   net.IPv4zero,
		pubTcpPort: 0,
	}

	qryMgr.tep = qryMgr.qryMgrProc

	return &qryMgr
}

//
// Entry point exported to shceduler
//
func (qryMgr *QryMgr) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return qryMgr.tep(ptn, msg)
}

//
// Query manager entry
//
func (qryMgr *QryMgr) qryMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Tracef("qryMgrProc: ptn: %p, msg.Id: %d", ptn, msg.Id)

	eno := sch.SchEnoUnknown
	switch msg.Id {

	case sch.EvSchPoweron:
		eno = qryMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = qryMgr.poweroff(ptn)

	case sch.EvDhtQryMgrQueryStartReq:
		sender := qryMgr.sdl.SchGetSender(msg)
		eno = qryMgr.queryStartReq(sender, msg.Body.(*sch.MsgDhtQryMgrQueryStartReq))

	case sch.EvDhtDsGvbStartReq:
		eno = qryMgr.dsGvbStartReq(msg.Body.(*sch.MsgDhtDsGvbStartReq))

	case sch.EvDhtDsGvbStopReq:
		eno = qryMgr.dsGvbStopReq(msg.Body.(*sch.MsgDhtDsGvbStopReq))

	case sch.EvDhtRutMgrNearestRsp:
		eno = qryMgr.rutNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	case sch.EvDhtQryMgrQcbTimer:
		qcb := msg.Body.(*qryCtrlBlock)
		eno = qryMgr.qcbTimerHandler(qcb)

	case sch.EvDhtQryMgrQueryStopReq:
		sender := qryMgr.sdl.SchGetSender(msg)
		eno = qryMgr.queryStopReq(sender, msg.Body.(*sch.MsgDhtQryMgrQueryStopReq))

	case sch.EvDhtRutMgrNotificationInd:
		eno = qryMgr.rutNotificationInd(msg.Body.(*sch.MsgDhtRutMgrNotificationInd))

	case sch.EvDhtQryInstStatusInd:
		eno = qryMgr.instStatusInd(msg.Body.(*sch.MsgDhtQryInstStatusInd))

	case sch.EvDhtQryInstResultInd:
		eno = qryMgr.instResultInd(msg.Body.(*sch.MsgDhtQryInstResultInd))

	case sch.EvNatMgrReadyInd:
		eno = qryMgr.natMgrReadyInd(msg.Body.(*sch.MsgNatMgrReadyInd))

	case sch.EvNatMgrMakeMapRsp:
		eno = qryMgr.natMakeMapRsp(msg)

	case sch.EvNatMgrPubAddrUpdateInd:
		eno = qryMgr.natPubAddrUpdateInd(msg)

	default:
		log.Debugf("qryMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	log.Tracef("qryMgrProc: get out, ptn: %p, msg.Id: %d", ptn, msg.Id)

	return eno
}

//
// Poweron handler
//
func (qryMgr *QryMgr) poweron(ptn interface{}) sch.SchErrno {
	var eno sch.SchErrno
	qryMgr.ptnMe = ptn
	if qryMgr.sdl = sch.SchGetScheduler(ptn); qryMgr.sdl == nil {
		log.Errorf("poweron: nil scheduler")
		return sch.SchEnoInternal
	}
	qryMgr.sdlName = qryMgr.sdl.SchGetP2pCfgName()
	if eno, qryMgr.ptnDhtMgr = qryMgr.sdl.SchGetUserTaskNode(DhtMgrName); eno != sch.SchEnoNone {
		log.Errorf("poweron: get task failed, task: %s", DhtMgrName)
		return eno
	}
	if eno, qryMgr.ptnRutMgr = qryMgr.sdl.SchGetUserTaskNode(RutMgrName); eno != sch.SchEnoNone {
		log.Errorf("poweron: get task failed, task: %s", RutMgrName)
		return eno
	}
	if eno, qryMgr.ptnNatMgr = qryMgr.sdl.SchGetUserTaskNode(nat.NatMgrName); eno != sch.SchEnoNone {
		log.Errorf("poweron: get task failed, task: %s", nat.NatMgrName)
		return eno
	}
	if eno, qryMgr.ptnDsMgr = qryMgr.sdl.SchGetUserTaskNode(DsMgrName); eno != sch.SchEnoNone {
		log.Errorf("poweron: get task failed, task: %s", DsMgrName)
		return eno
	}
	if dhtEno := qryMgr.qryMgrGetConfig(); dhtEno != DhtEnoNone {
		log.Errorf("poweron: qryMgrGetConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}
	mapQrySeqLock[qryMgr.sdlName] = sync.Mutex{}
	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (qryMgr *QryMgr) poweroff(ptn interface{}) sch.SchErrno {
	log.Debugf("poweroff: task will be done ...")
	for _, qcb := range qryMgr.qcbTab {
		for _, qry := range qcb.qryActived {
			po := sch.SchMessage{}
			qryMgr.sdl.SchMakeMessage(&po, qryMgr.ptnMe, qry.ptnInst, sch.EvSchPoweroff, nil)
			po.TgtName = qry.name
			qryMgr.sdl.SchSendMessage(&po)
		}
	}
	return qryMgr.sdl.SchTaskDone(qryMgr.ptnMe, qryMgr.name, sch.SchEnoKilled)
}

//
// Query start request handler
//
func (qryMgr *QryMgr) queryStartReq(sender interface{}, msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	if sender == nil || msg == nil {
		log.Errorf("queryStartReq: invalid prameters, sdl: %s", qryMgr.sdlName)
		return sch.SchEnoParameter
	}
	if msg.ForWhat != MID_PUTVALUE &&
		msg.ForWhat != MID_PUTPROVIDER &&
		msg.ForWhat != MID_FINDNODE &&
		msg.ForWhat != MID_GETPROVIDER_REQ &&
		msg.ForWhat != MID_GETVALUE_REQ {
		log.Debugf("queryStartReq: unknown what's for")
		return sch.SchEnoMismatched
	}

	log.Debugf("queryStartReq: sdl: %s, ForWhat: %d, sender: %s, batch: %t, batchId: %d, target: %x",
		qryMgr.sdlName, msg.ForWhat, qryMgr.sdl.SchGetTaskName(sender), msg.Batch, msg.BatchId, msg.Target)

	var forWhat = msg.ForWhat
	var rsp = sch.MsgDhtQryMgrQueryStartRsp{
		ForWhat: msg.ForWhat,
		Target: msg.Target,
		Eno: DhtEnoUnknown.GetEno(),
	}
	var qcb *qryCtrlBlock
	var schMsg *sch.SchMessage

	//
	// set "NtfReq" to be true to tell route manager that we need notifications,
	// see handler about this event in route.go pls.
	//

	var nearestReq = sch.MsgDhtRutMgrNearestReq{
		Target:  msg.Target,
		Max:     rutMgrMaxNearest,
		NtfReq:  true,
		Task:    qryMgr.ptnMe,
		ForWhat: forWhat,
	}

	if _, dup := qryMgr.qcbTab[msg.Target]; dup {
		log.Debugf("queryStartReq: duplicated, sdl: %s, ForWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Target)
		rsp.Eno = DhtEnoDuplicated.GetEno()
		goto _rsp2Sender
	}

	qcb = new(qryCtrlBlock)
	qcb.ptnOwner = sender
	qcb.qryReq = msg
	qcb.seq = qryMgr.qcbSeq
	qryMgr.qcbSeq++
	qcb.forWhat = forWhat
	qcb.icbSeq = 0
	qcb.target = msg.Target
	qcb.status = qsNull
	qcb.qryHistory = make(map[config.NodeID]*qryPendingInfo, 0)
	qcb.qryPending = nil
	qcb.qryActived = make(map[config.NodeID]*qryInstCtrlBlock, qryMgr.qmCfg.maxActInsts)
	qcb.qryResult = nil
	qcb.rutNtfFlag = nearestReq.NtfReq
	qcb.status = qsPreparing
	qcb.width = 0
	qcb.depth = 0
	qryMgr.qcbTab[msg.Target] = qcb

	log.Debugf("queryStartReq: going to fetch nearests for target, " +
		"sdl: %s, ForWhat: %d, target: %x",
		qryMgr.sdlName, qcb.forWhat, qcb.target)

	schMsg = new(sch.SchMessage)
	qryMgr.sdl.SchMakeMessage(schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &nearestReq)
	if eno := qryMgr.sdl.SchSendMessage(schMsg); eno != sch.SchEnoNone {
		log.Errorf("queryStartReq: send message failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, qcb.forWhat, eno, qcb.target)
		rsp.Eno = DhtEnoScheduler.GetEno()
		goto _rsp2Sender
	}
	rsp.Eno = DhtEnoNone.GetEno()

_rsp2Sender:

	if rsp.Eno != DhtEnoNone.GetEno() {
		delete(qryMgr.qcbTab, msg.Target)
		schMsg = new(sch.SchMessage)
		qryMgr.sdl.SchMakeMessage(schMsg, qryMgr.ptnMe, sender, sch.EvDhtQryMgrQueryStartRsp, &rsp)
		if eno := qryMgr.sdl.SchSendMessage(schMsg); eno != sch.SchEnoNone {
			log.Errorf("queryStartReq: send message failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
				qryMgr.sdlName, qcb.forWhat, eno, qcb.target)
			return eno
		}
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

//
// Start get-value-batch handler
//
func (qryMgr *QryMgr)dsGvbStartReq(msg *sch.MsgDhtDsGvbStartReq) sch.SchErrno {
	gvbCb := &gvbCtrlBlock{
		gvbId: msg.GvbId,
		keys: msg.Keys,
		output: msg.Output,
	}
	gvbCb.status = sch.GVBS_NULL
	gvbCb.size = len(*gvbCb.keys)
	gvbCb.head = 0
	gvbCb.got = 0
	gvbCb.getting = 0
	gvbCb.failed = 0
	gvbCb.remain = gvbCb.size
	gvbCb.pending = make(map[config.DsKey] interface{}, 0)
	if _, dup := qryMgr.gvbTab[gvbCb.gvbId]; dup {
		gvbCb.status = sch.GVBS_TERMED
		qryMgr.gvbStatusReport(gvbCb, "dsGvbStartReq")
		return sch.SchEnoDuplicated
	}

	log.Debugf("dsGvbStartReq: sdl: %s, gvbId: %d, size: %d",
		qryMgr.sdlName, msg.GvbId, gvbCb.size)

	gvbCb.status = sch.GVBS_WORKING
	qryMgr.gvbTab[gvbCb.gvbId] = gvbCb
	for loop := 0; loop < qryGvbUnitSize; loop++ {
		if gvbCb.remain <= 0 {
			break
		}
		if dhtEno, schEno := qryMgr.gvbStartNext(gvbCb); dhtEno != DhtEnoNone  || schEno != sch.SchEnoNone {
			log.Warnf("dsGvbStartReq: gvbStartNext failed, sdl: %s, dhtEno: %d, schEno: %d",
				qryMgr.sdlName, dhtEno, schEno)
		}
	}

	return qryMgr.gvbStatusReport(gvbCb, "dsGvbStartReq")
}

//
// Stop get-value-batch handler
//
func (qryMgr *QryMgr)dsGvbStopReq(msg *sch.MsgDhtDsGvbStopReq) sch.SchErrno {

	log.Debugf("dsGvbStopReq: sdl: %s, gvbId: %d",
		qryMgr.sdlName, msg.GvbId)

	gvbCb, ok := qryMgr.gvbTab[msg.GvbId]
	if !ok {
		log.Warnf("dsGvbStopReq: not found, sdl: %s, id: %d", qryMgr.sdlName, msg.GvbId)
		return sch.SchEnoNotFound
	}
	if gvbCb.status != sch.GVBS_WORKING {
		log.Warnf("dsGvbStopReq: mismatched, sdl: %s, id: %d, status: %s",
			qryMgr.sdlName, msg.GvbId, gvbCb.status)
		return sch.SchEnoMismatched
	}
	for key := range gvbCb.pending {
		if eno := qryMgr.qryMgrDelQcb(delQcb4Command, key); eno != DhtEnoNone {
			log.Warnf("dsGvbStopReq: qryMgrDelQcb failed, sdl: %s, id: %d, eno: %d, key: %x",
				qryMgr.sdlName, gvbCb.gvbId, eno, key)
		}
		delete(gvbCb.pending, key)
	}
	log.Debugf("dsGvbStopReq: all stopped,  sdl: %s, id: %d", qryMgr.sdlName, gvbCb.gvbId)
	delete(qryMgr.gvbTab, gvbCb.gvbId)
	gvbCb.status = sch.GVBS_TERMED
	return qryMgr.gvbStatusReport(gvbCb, "dsGvbStopReq")
}

//
// Nearest response handler
//
func (qryMgr *QryMgr) rutNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	if msg.ForWhat != MID_PUTVALUE &&
		msg.ForWhat != MID_PUTPROVIDER &&
		msg.ForWhat != MID_FINDNODE &&
		msg.ForWhat != MID_GETPROVIDER_REQ &&
		msg.ForWhat != MID_GETVALUE_REQ {
		log.Warnf("rutNearestRsp: unknown what's for, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Target)
		return sch.SchEnoMismatched
	}

	log.Debugf("rutNearestRsp: sdl: %s, ForWhat: %d, target: %x", qryMgr.sdlName, msg.ForWhat, msg.Target)

	forWhat := msg.ForWhat
	target := msg.Target
	qcb, ok := qryMgr.qcbTab[target]
	if !ok {
		log.Debugf("rutNearestRsp: qcb not exist, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, forWhat, target)
		return sch.SchEnoNotFound
	}
	if qcb == nil {
		log.Errorf("rutNearestRsp: nil qcb, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, forWhat, target)
		return sch.SchEnoInternal
	}
	if qcb.status != qsPreparing {
		log.Errorf("rutNearestRsp: qcb status mismatched, sdl: %s, forWhat: %d, status: %d, target: %x",
			qryMgr.sdlName, forWhat, qcb.status, target)
		return sch.SchEnoMismatched
	}

	qryFailed2Sender := func(eno DhtErrno) {
		rsp := sch.MsgDhtQryMgrQueryStartRsp{
			ForWhat: forWhat,
			Target: msg.Target,
			Eno:    int(eno),
		}
		schMsg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryStartRsp, &rsp)
		if eno := qryMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Errorf("qryFailed2Sender: send message failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
				qryMgr.sdlName, msg.ForWhat, eno, msg.Target)
		}
	}

	qryOk2Sender := func(peer *config.Node) {
		ind := sch.MsgDhtQryMgrQueryResultInd{
			Eno:     DhtEnoNone.GetEno(),
			ForWhat: msg.ForWhat,
			Target:  target,
			Peers:   []*config.Node{peer},
		}
		schMsg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
		if eno := qryMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Errorf("qryOk2Sender: send message failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
				qryMgr.sdlName, msg.ForWhat, eno, msg.Target)
		}
	}

	if msg.Eno != DhtEnoNone.GetEno() {
		log.Debugf("rutNearestRsp: route reports failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Eno, msg.Target)
		qryFailed2Sender(DhtErrno(msg.Eno))
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoNone
	}

	if (msg.Eno == DhtEnoNone.GetEno()) && (msg.Peers == nil || msg.Pcs == nil) {
		log.Errorf("rutNearestRsp: invalid empty nearest set reported, " +
			"sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Eno, msg.Target)
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoParameter
	}

	peers := msg.Peers.([]*rutMgrBucketNode)
	pcs := msg.Pcs.([]int)
	dists := msg.Dists.([]int)
	if (msg.Eno == DhtEnoNone.GetEno()) && (len(peers) != len(pcs) || len(peers) != len(dists)) {
		log.Errorf("rutNearestRsp: invalid nearest set reported, " +
			"sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Eno, msg.Target)
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoParameter
	}

	if len(peers) == 0 {
		log.Errorf("rutNearestRsp: invalid empty nearest set reported, " +
			"sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Eno, msg.Target)
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoParameter
	}

	//
	// check if target found in local while updating the query result by the
	// nearests reported.
	//

	qcb.qryResult = list.New()
	for idx, peer := range peers {
		if forWhat == MID_FINDNODE ||
			forWhat == MID_GETPROVIDER_REQ {
			if bytes.Compare(peer.hash[0:], target[0:]) == 0 {
				log.Debugf("rutNearestRsp: sdl: %s, forWhat: %d, target found: %x",
					qryMgr.sdlName, forWhat, target)
				qryOk2Sender(&peer.node)
				qryMgr.qryMgrDelQcb(delQcb4TargetInLocal, target)
				return sch.SchEnoNone
			}
		}

		qri := qryResultInfo{
			node: peer.node,
			pcs:  pcs[idx],
			dist: dists[idx],
		}
		qcb.qcbUpdateResult(&qri)
	}

	//
	// start queries by putting nearest to pending queue and then putting
	// pending nodes to be activated.
	//

	qcb.qryPending = list.New()
	pendInfo := make([]*qryPendingInfo, 0)
	for idx := 0; idx < len(peers); idx++ {
		pi := qryPendingInfo{
			rutMgrBucketNode: *peers[idx],
			depth:            0,
		}
		pendInfo = append(pendInfo, &pi)
	}

	//
	// notice: qryMgrQcbPutPending always return DhtEnoNone(with normal parameters), but
	// some peers planed to be queried might be discarded there, see comments there pls.
	//

	var dhtEno = DhtErrno(DhtEnoNone)
	if dhtEno = qcb.qryMgrQcbPutPending(pendInfo, qryMgr.qmCfg.maxPendings); dhtEno == DhtEnoNone {
		if dhtEno = qryMgr.qryMgrQcbStartTimer(qcb); dhtEno == DhtEnoNone {
			if eno, insts := qryMgr.qryMgrQcbPutActived(qcb); eno != DhtEnoNone  || insts == 0{
				log.Debugf("rutNearestRsp: active query failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
					qryMgr.sdlName, qcb.forWhat, eno, qcb.target)
			}
			qcb.status = qsInited
			return sch.SchEnoNone
		} else {
			log.Debugf("rutNearestRsp: start timer failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
				qryMgr.sdlName, qcb.forWhat, dhtEno, qcb.target)
		}
	} else {
		log.Debugf("rutNearestRsp: put query to pending queue failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, qcb.forWhat, dhtEno, qcb.target)
	}

	qryFailed2Sender(dhtEno)
	qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
	return sch.SchEnoResource
}

//
// Query stop request handler
//
func (qryMgr *QryMgr) queryStopReq(sender interface{}, msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {
	target := msg.Target
	rsp := sch.MsgDhtQryMgrQueryStopRsp{Target: target, Eno: DhtEnoNone.GetEno()}
	rsp2Sender := func(rsp *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
		schMsg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, sender, sch.EvDhtQryMgrQueryStopRsp, rsp)
		return qryMgr.sdl.SchSendMessage(&schMsg)
	}
	rsp.Eno = int(qryMgr.qryMgrDelQcb(delQcb4Command, target))
	return rsp2Sender(&rsp)
}

//
//Route notification handler
//
func (qryMgr *QryMgr) rutNotificationInd(msg *sch.MsgDhtRutMgrNotificationInd) sch.SchErrno {
	qcb := (*qryCtrlBlock)(nil)
	target := msg.Target
	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		log.Debugf("rutNotificationInd: target not found: %x", target)
		return sch.SchEnoParameter
	}

	if qcb.status != qsInited {
		log.Debugf("rutNotificationInd: query not inited yet for target: %x", target)
		return sch.SchEnoUserTask
	}

	qpi := msg.Peers.([]*rutMgrBucketNode)
	pendInfo := make([]*qryPendingInfo, 0)
	for idx := 0; idx < len(qpi); idx++ {
		var pi = qryPendingInfo{
			rutMgrBucketNode: *qpi[idx],
			depth:            0,
		}
		pendInfo = append(pendInfo, &pi)
	}

	qcb.qryMgrQcbPutPending(pendInfo, qryMgr.qmCfg.maxPendings)
	qryMgr.qryMgrQcbPutActived(qcb)

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		log.Debugf("rutNotificationInd: internal errors")
		return sch.SchEnoUserTask
	}

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0 {
		if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
			log.Debugf("rutNotificationInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			log.Debugf("rutNotificationInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (qryMgr *QryMgr) instStatusInd(msg *sch.MsgDhtQryInstStatusInd) sch.SchErrno {
	switch msg.Status {
	case qisNull:
		log.Debugf("instStatusInd: qisNull")
	case qisInited:
		log.Debugf("instStatusInd: qisInited")
	case qisWaitConnect:
		log.Debugf("instStatusInd: qisWaitConnect")
	case qisWaitResponse:
		log.Debugf("instStatusInd: qisWaitResponse")
	case qisDoneOk:
		log.Debugf("instStatusInd: qisDoneOk")

	case qisDone:
		log.Debugf("instStatusInd: qisDone")
		qcb, exist := qryMgr.qcbTab[msg.Target]
		if !exist {
			log.Debugf("instStatusInd: qcb not found")
			return sch.SchEnoNotFound
		}
		if dhtEno := qryMgr.qryMgrDelIcb(delQcb4QryInstDoneInd, &msg.Target, &msg.Peer); dhtEno != DhtEnoNone {
			log.Debugf("instStatusInd: qryMgrDelIcb failed, eno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
		if eno, num := qryMgr.qryMgrQcbPutActived(qcb); true {
			log.Debugf("instStatusInd: qryMgrQcbPutActived return with eno: %d, num: %d", eno, num)
		}
		if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
			log.Debugf("instStatusInd: internal errors")
			return sch.SchEnoUserTask
		}

		// if pending queue and active queue all are empty, we just report query
		// result and end the query.
		log.Debugf("instStatusInd: pending: %d, actived: %d", qcb.qryPending.Len(), len(qcb.qryActived))

		if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0 {
			log.Debugf("instStatusInd: query done: %x", qcb.target)
			if qcb.forWhat == MID_PUTVALUE || qcb.forWhat == MID_PUTPROVIDER {
				if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
					log.Debugf("instStatusInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
					return sch.SchEnoUserTask
				}
			} else {
				if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
					log.Debugf("instStatusInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
					return sch.SchEnoUserTask
				}
			}
			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
				log.Debugf("instStatusInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
				return sch.SchEnoUserTask
			}
		}

	default:
		log.Debugf("instStatusInd: invalid instance status: %d", msg.Status)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Instance query result indication handler
//
func (qryMgr *QryMgr) instResultInd(msg *sch.MsgDhtQryInstResultInd) sch.SchErrno {

	log.Debugf("instResultInd: sdl: %s, forWhat: %d, target: %x", qryMgr.sdlName, msg.ForWhat, msg.Target)

	if msg.ForWhat != sch.EvDhtMgrPutValueReq &&
		msg.ForWhat != sch.EvDhtMgrPutProviderReq &&
		msg.ForWhat != sch.EvDhtConInstNeighbors &&
		msg.ForWhat != sch.EvDhtConInstGetProviderRsp &&
		msg.ForWhat != sch.EvDhtConInstGetValRsp {
		log.Errorf("instResultInd: mismatched, sdl: %s, forWhat: %d", qryMgr.sdlName, msg.ForWhat)
		return sch.SchEnoMismatched
	}

	var (
		qcb			*qryCtrlBlock = nil
		qpiList		= make([]*qryPendingInfo, 0)
		hashList    = make([]*Hash, 0)
		distList    = make([]int, 0)
		rutMgr      = qryMgr.sdl.SchGetTaskObject(RutMgrName).(*RutMgr)
	)

	if len(msg.Peers) != len(msg.Pcs) {
		log.Errorf("instResultInd: mismatched Peers and Pcs, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Target)
		return sch.SchEnoMismatched
	}
	if rutMgr == nil {
		log.Errorf("instResultInd: nil route manager, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, msg.Target)
		return sch.SchEnoInternal
	}

	for idx, peer := range msg.Peers {
		if bytes.Compare(peer.ID[0:], qryMgr.qmCfg.local.ID[0:]) == 0 {
			if idx != len(msg.Peers) - 1 {
				msg.Peers = append(msg.Peers[0:idx], msg.Peers[idx+1:]...)
				msg.Pcs = append(msg.Pcs[0:idx], msg.Pcs[idx+1:]...)
			} else {
				msg.Peers = msg.Peers[0:idx]
				msg.Pcs = msg.Pcs[0:idx]
			}
			break
		}
	}

	from := msg.From
	latency := msg.Latency
	updateReq2RutMgr := func(peer *config.Node, dur time.Duration) sch.SchErrno {
		updateReq := sch.MsgDhtRutMgrUpdateReq{
			Why: rutMgrUpdate4Query,
			Eno: DhtEnoNone.GetEno(),
			Seens: []config.Node{
				*peer,
			},
			Duras: []time.Duration{
				dur,
			},
		}
		schMsg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &updateReq)
		if eno := qryMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Errorf("instResultInd: send EvDhtRutMgrUpdateReq failed, sdl: %s, forWhat: %d, target: %x",
				qryMgr.sdlName, msg.ForWhat, msg.Target)
			return eno
		}
		return sch.SchEnoNone
	}

	updateReq2RutMgr(&from, latency)

	target := msg.Target
	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		log.Debugf("instResultInd: not found, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, target)
		return sch.SchEnoUserTask
	}

	icb, ok := qcb.qryActived[from.ID]
	if !ok || icb == nil {
		log.Debugf("instResultInd: target not found, sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, target)
		return sch.SchEnoUserTask
	}

	depth := icb.depth

	for idx, peer := range msg.Peers {

		hash := rutMgrNodeId2Hash(peer.ID)
		hashList = append(hashList, hash)
		dist := rutMgr.rutMgrLog2Dist(nil, hash)
		distList = append(distList, dist)

		qri := qryResultInfo{
			node: *peer,
			pcs:  conMgrPeerConnStat(msg.Pcs[idx]),
			dist: distList[idx],
		}

		qcb.qcbUpdateResult(&qri)
	}

	if msg.ForWhat == sch.EvDhtConInstNeighbors {
		for _, peer := range msg.Peers {
			key := rutMgrNodeId2Hash(peer.ID)
			if bytes.Compare((*key)[0:], target[0:]) == 0 {
				qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, msg.Value, msg.Provider)
				if dhtEno := qryMgr.qryMgrDelQcb(delQcb4TargetFound, qcb.target); dhtEno != DhtEnoNone {
					log.Errorf("instResultInd: qryMgrDelQcb failed, " +
						"sdl: %s, forWhat: %d, eno: %d, target: %x",
						qryMgr.sdlName, msg.ForWhat, dhtEno, target)
					return sch.SchEnoUserTask
				}
				return sch.SchEnoNone
			}
		}
	} else if msg.ForWhat == sch.EvDhtConInstGetValRsp {
		if msg.Value != nil && len(msg.Value) > 0 {
			qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, msg.Value, nil)
			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4TargetFound, qcb.target); dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrDelQcb failed, " +
					"sdl: %s, forWhat: %d, eno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, target)
				return sch.SchEnoUserTask
			}
			return sch.SchEnoNone
		}
	} else if msg.ForWhat == sch.EvDhtConInstGetProviderRsp {
		if msg.Provider != nil {
			qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, nil, msg.Provider)
			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4TargetFound, qcb.target); dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrDelQcb failed, " +
					"sdl: %s, forWhat: %d, eno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, target)
				return sch.SchEnoUserTask
			}
			return sch.SchEnoNone
		}
	}

	if eno := qryMgr.qryMgrDelIcb(delQcb4QryInstResultInd, &msg.Target, &msg.From.ID); eno != DhtEnoNone {
		log.Errorf("instResultInd: qryMgrDelIcb failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
			qryMgr.sdlName, msg.ForWhat, eno, target)
		return sch.SchEnoUserTask
	}

	if depth > qcb.depth {
		qcb.depth = depth
	}

	if msg.ForWhat == sch.EvDhtConInstNeighbors ||
		msg.ForWhat == sch.EvDhtConInstGetProviderRsp ||
		msg.ForWhat == sch.EvDhtConInstGetValRsp {
		if qcb.depth > qryMgrQryMaxDepth || len(qcb.qryHistory) >= qryMgrQryMaxWidth {
			log.Debugf("instResultInd: query limited to stop, forWhat: %d, depth: %d, width: %d",
				msg.ForWhat, qcb.depth, len(qcb.qryHistory))
			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil);
				dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrResultReport failed, " +
					"sdl: %s, forWhat: %d, dhtEno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, qcb.target)
				return sch.SchEnoUserTask
			}
			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrDelQcb failed, " +
					"sdl: %x, forWhat: %d, dhtEno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, qcb.target)
				return sch.SchEnoUserTask
			}
			return sch.SchEnoNone
		}

		for idx, peer := range msg.Peers {
			qpi := qryPendingInfo{
				rutMgrBucketNode: rutMgrBucketNode{
					node: *peer,
					hash: *hashList[idx],
					dist: distList[idx],
				},
				depth: depth + 1,
			}
			qpiList = append(qpiList, &qpi)
		}

		qcb.qryMgrQcbPutPending(qpiList, qryMgr.qmCfg.maxPendings)
		qryMgr.qryMgrQcbPutActived(qcb)
	}

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		log.Errorf("instResultInd: internal errors")
		return sch.SchEnoUserTask
	}

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0 {
		if msg.ForWhat == sch.EvDhtConInstNeighbors ||
			msg.ForWhat == sch.EvDhtConInstGetProviderRsp ||
			msg.ForWhat == sch.EvDhtConInstGetValRsp {
			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil);
				dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrResultReport failed, " +
					"sdl: %s, forWhat: %d, dhtEno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, qcb.target)
			}
		} else {
			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, nil, nil);
				dhtEno != DhtEnoNone {
				log.Errorf("instResultInd: qryMgrResultReport failed, " +
					"sdl: %s, forWhat: %d, dhtEno: %d, target: %x",
					qryMgr.sdlName, msg.ForWhat, dhtEno, qcb.target)
			}
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			log.Errorf("instResultInd: qryMgrDelQcb failed, sdl: %s, forWhat: %d, dhtEno: %d, target: %x",
				qryMgr.sdlName, msg.ForWhat, dhtEno, qcb.target)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// nat ready to work
//
func (qryMgr *QryMgr) natMgrReadyInd(msg *sch.MsgNatMgrReadyInd) sch.SchErrno {
	log.Debugf("natMgrReadyInd: nat type: %s", msg.NatType)
	if msg.NatType == config.NATT_NONE {
		qryMgr.pubTcpIp = qryMgr.qmCfg.local.IP
		qryMgr.pubTcpPort = int(qryMgr.qmCfg.local.TCP)
	} else {
		req := sch.MsgNatMgrMakeMapReq{
			Proto:      "tcp",
			FromPort:   int(qryMgr.qmCfg.local.TCP),
			ToPort:     int(qryMgr.qmCfg.local.TCP),
			DurKeep:    natMapKeepTime,
			DurRefresh: natMapRefreshTime,
		}
		schMsg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnNatMgr, sch.EvNatMgrMakeMapReq, &req)
		if eno := qryMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Debugf("natMgrReadyInd: SchSendMessage failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}
	}
	return sch.SchEnoNone
}

//
// nat make mapping response
//
func (qryMgr *QryMgr) natMakeMapRsp(msg *sch.SchMessage) sch.SchErrno {
	// see comments in function tabMgrNatMakeMapRsp for more please.
	mmr := msg.Body.(*sch.MsgNatMgrMakeMapRsp)
	if !nat.NatIsResultOk(mmr.Result) {
		log.Debugf("natMakeMapRsp: fail reported, mmr: %+v", *mmr)
	}
	log.Debugf("natMakeMapRsp: proto: %s, ip:port = %s:%d",
		mmr.Proto, mmr.PubIp.String(), mmr.PubPort)

	proto := strings.ToLower(mmr.Proto)
	if proto == "tcp" {
		qryMgr.natTcpResult = nat.NatIsStatusOk(mmr.Status)
		if qryMgr.natTcpResult {
			log.Debugf("natMakeMapRsp: public dht addr: %s:%d",
				mmr.PubIp.String(), mmr.PubPort)
			qryMgr.pubTcpIp = mmr.PubIp
			qryMgr.pubTcpPort = mmr.PubPort
			if eno := qryMgr.switch2NatAddr(proto); eno != DhtEnoNone {
				log.Debugf("natMakeMapRsp: switch2NatAddr failed, eno: %d", eno)
				return sch.SchEnoUserTask
			}
		} else {
			qryMgr.pubTcpIp = net.IPv4zero
			qryMgr.pubTcpPort = 0
		}
		_, ptrConMgr := qryMgr.sdl.SchGetUserTaskNode(ConMgrName)
		qryMgr.sdl.SchSetRecver(msg, ptrConMgr)
		return qryMgr.sdl.SchSendMessage(msg)
	}

	log.Warnf("natMakeMapRsp: unknown protocol reported: %s", proto)
	return sch.SchEnoParameter
}

//
// public address changed indication
//
func (qryMgr *QryMgr) natPubAddrUpdateInd(msg *sch.SchMessage) sch.SchErrno {
	// see comments in function tabMgrNatPubAddrUpdateInd for more please. to query manager,
	// when status from nat is bad, nothing to do but just backup the status.
	_, ptrConMgr := qryMgr.sdl.SchGetUserTaskNode(ConMgrName)
	qryMgr.sdl.SchSetRecver(msg, ptrConMgr)
	qryMgr.sdl.SchSendMessage(msg)

	ind := msg.Body.(*sch.MsgNatMgrPubAddrUpdateInd)
	proto := strings.ToLower(ind.Proto)
	if proto != nat.NATP_TCP {
		log.Debugf("natPubAddrUpdateInd: bad protocol: %s", proto)
		return sch.SchEnoParameter
	}

	oldResult := qryMgr.natTcpResult
	if qryMgr.natTcpResult = nat.NatIsStatusOk(ind.Status); !qryMgr.natTcpResult {
		log.Debugf("natPubAddrUpdateInd: result bad")
		return sch.SchEnoNone
	}

	log.Debugf("natPubAddrUpdateInd: proto: %s, old: %s:%d; new: %s:%d",
		ind.Proto, qryMgr.pubTcpIp.String(), qryMgr.pubTcpPort, ind.PubIp.String(), ind.PubPort)

	qryMgr.pubTcpIp = ind.PubIp
	qryMgr.pubTcpPort = ind.PubPort
	if !oldResult || !ind.PubIp.Equal(qryMgr.pubTcpIp) || ind.PubPort != qryMgr.pubTcpPort {
		log.Debugf("natPubAddrUpdateInd: call natMapSwitch")
		if eno := qryMgr.natMapSwitch(); eno != DhtEnoNone {
			log.Debugf("natPubAddrUpdateInd: natMapSwitch failed, error: %s", eno.Error())
			return sch.SchEnoUserTask
		}
	}
	return sch.SchEnoNone
}

//
// Get query manager configuration
//
func (qryMgr *QryMgr) qryMgrGetConfig() DhtErrno {
	cfg := config.P2pConfig4DhtQryManager(qryMgr.sdl.SchGetP2pCfgName())
	qmCfg := &qryMgr.qmCfg
	qmCfg.local = cfg.Local
	qmCfg.maxActInsts = cfg.MaxActInsts
	qmCfg.qryExpired = cfg.QryExpired
	qmCfg.qryInstExpired = cfg.QryInstExpired
	return DhtEnoNone
}

//
// Delete query control blcok from manager
//
const (
	delQcb4TargetFound      = iota // target had been found
	delQcb4NoMoreQueries           // no pendings and no actived instances
	delQcb4Timeout                 // query manager time out for the control block
	delQcb4Command                 // required by other module
	delQcb4NoSeeds                 // no seeds for query
	delQcb4TargetInLocal           // target found in local
	delQcb4QryInstDoneInd          // query instance done is indicated
	delQcb4QryInstResultInd        // query instance result indicated
	delQcb4InteralErrors           // internal errors while tring to query
	delQcb4PubAddrSwitch           // public address switching
)

func (qryMgr *QryMgr) qryMgrDelQcb(why int, target config.DsKey) DhtErrno {
	var strDebug = ""
	switch why {
	case delQcb4TargetFound:
		strDebug = "delQcb4TargetFound"
	case delQcb4NoMoreQueries:
		strDebug = "delQcb4NoMoreQueries"
	case delQcb4Timeout:
		strDebug = "delQcb4Timeout"
	case delQcb4NoSeeds:
		strDebug = "delQcb4NoSeeds"
	case delQcb4TargetInLocal:
		strDebug = "delQcb4TargetInLocal"
	case delQcb4QryInstDoneInd:
		strDebug = "delQcb4QryInstDoneInd"
	case delQcb4InteralErrors:
		strDebug = "delQcb4InteralErrors"
	case delQcb4PubAddrSwitch:
		strDebug = "delQcb4PubAddrSwitch"
	case delQcb4Command:
		strDebug = "delQcb4Command"
	default:
		log.Debugf("qryMgrDelQcb: parameters mismatched, why: %d", why)
		return DhtEnoMismatched
	}

	log.Debugf("qryMgrDelQcb: why: %s", strDebug)

	qcb, ok := qryMgr.qcbTab[target]
	if !ok {
		log.Debugf("qryMgrDelQcb: target not found: %x", target)
		return DhtEnoNotFound
	}

	if qcb.status != qsInited {
		delete(qryMgr.qcbTab, target)
		return DhtEnoNone
	}

	if qcb.qryTid != sch.SchInvalidTid {
		qryMgr.sdl.SchKillTimer(qryMgr.ptnMe, qcb.qryTid)
		qcb.qryTid = sch.SchInvalidTid
	}

	for _, icb := range qcb.qryActived {
		po := sch.SchMessage{}
		icb.sdl.SchMakeMessage(&po, qryMgr.ptnMe, icb.ptnInst, sch.EvSchPoweroff, nil)
		po.TgtName = icb.name
		icb.sdl.SchSendMessage(&po)
	}

	if qcb.rutNtfFlag == true {
		req := sch.MsgDhtRutMgrStopNofiyReq{
			Task:   qryMgr.ptnMe,
			Target: qcb.target,
		}
		msg := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&msg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrStopNotifyReq, &req)
		qryMgr.sdl.SchSendMessage(&msg)
	}

	delete(qryMgr.qcbTab, target)
	return DhtEnoNone
}

//
// Delete query instance control block
//
func (qryMgr *QryMgr) qryMgrDelIcb(why int, target *config.DsKey, peer *config.NodeID) DhtErrno {

	log.Debugf("qryMgrDelIcb: why: %d", why)

	if why != delQcb4QryInstDoneInd && why != delQcb4QryInstResultInd {
		log.Debugf("qryMgrDelIcb: why delete?! why: %d", why)
		return DhtEnoMismatched
	}
	qcb, ok := qryMgr.qcbTab[*target]
	if !ok {
		log.Debugf("qryMgrDelIcb: target not found: %x", target)
		return DhtEnoNotFound
	}
	icb, ok := qcb.qryActived[*peer]
	if !ok {
		log.Debugf("qryMgrDelIcb: target not found: %x", target)
		return DhtEnoNotFound
	}

	log.Debugf("qryMgrDelIcb: icb: %s", icb.name)

	if why == delQcb4QryInstResultInd {
		eno, ptn := icb.sdl.SchGetUserTaskNode(icb.name)
		if eno == sch.SchEnoNone && ptn != nil && ptn == icb.ptnInst {
			po := sch.SchMessage{}
			icb.sdl.SchMakeMessage(&po, qryMgr.ptnMe, icb.ptnInst, sch.EvSchPoweroff, nil)
			po.TgtName = icb.name
			icb.sdl.SchSendMessage(&po)
		} else {
			log.Debugf("qryMgrDelIcb: not found, icb: %s", icb.name)
		}
	}
	delete(qcb.qryActived, *peer)
	return DhtEnoNone
}

//
// Update query result of query control block
//
func (qcb *qryCtrlBlock) qcbUpdateResult(qri *qryResultInfo) DhtErrno {
	li := qcb.qryResult
	for el := li.Front(); el != nil; el = el.Next() {
		v := el.Value.(*qryResultInfo)
		if qri.dist < v.dist {
			li.InsertBefore(qri, el)
			return DhtEnoNone
		}
	}
	li.PushBack(qri)
	return DhtEnoNone
}

//
// Put node to pending queue
//
func (qcb *qryCtrlBlock) qryMgrQcbPutPending(nodes []*qryPendingInfo, size int) DhtErrno {
	// this function is trying to put a batch of peers for a query into pending queue.
	// we would like to limit the queue size not so big, but this might result in lost
	// of some or all peers for a query, and also, it's hard to decide what should be
	// returned when just some of peers put. currently this function always return ok,
	// so the "size" should be large enough. if "size" is 0, no limited then.
	if len(nodes) == 0 || size < 0 {
		log.Debugf("qryMgrQcbPutPending: no pendings to be put")
		return DhtEnoParameter
	}

	log.Debugf("qryMgrQcbPutPending: " +
		"number of nodes to be put: %d, size(zero is unlimited): %d", len(nodes), size)

	li := qcb.qryPending
	for _, n := range nodes {
		if _, dup := qcb.qryHistory[n.node.ID]; dup {
			log.Debugf("qryMgrQcbPutPending: duplicated, n: %+v", n)
			continue
		}
		pb := true
		for el := li.Front(); el != nil; el = el.Next() {
			v := el.Value.(*qryPendingInfo)
			if v.node.ID == n.node.ID {
				pb = false
				break
			}
			if n.dist < v.dist {
				li.InsertBefore(n, el)
				pb = false
				break
			}
		}
		if pb {
			log.Debugf("qryMgrQcbPutPending: PushBack, n: %+v", n)
			li.PushBack(n)
		}
	}

	for li.Len() > size && size > 0{
		li.Remove(li.Back())
	}

	return DhtEnoNone
}

//
// Put node to actived queue and start query to the node
//
func (qryMgr *QryMgr) qryMgrQcbPutActived(qcb *qryCtrlBlock) (DhtErrno, int) {

	if qcb.qryPending == nil || qcb.qryPending.Len() == 0 {
		log.Debugf("qryMgrQcbPutActived: no pending, sdl: %s", qryMgr.sdlName)
		return DhtEnoNotFound, 0
	}

	if len(qcb.qryActived) >= qryMgr.qmCfg.maxActInsts {
		log.Errorf("qryMgrQcbPutActived: too much, sdl: %s, forWhat: %d, max: %d",
			qryMgr.sdlName, qcb.forWhat, qryMgr.qmCfg.maxActInsts)
		return DhtEnoResource, 0
	}

	act := make([]*list.Element, 0)
	cnt := 0
	dhtEno := DhtEnoNone

	for el := qcb.qryPending.Front(); el != nil; el = el.Next() {
		if len(qcb.qryActived) >= qryMgr.qmCfg.maxActInsts {
			log.Debugf("qryMgrQcbPutActived: full, sdl: %s, forWhat: %d, max: %d",
				qryMgr.sdlName, qcb.forWhat, qryMgr.qmCfg.maxActInsts)
			break
		}

		pending := el.Value.(*qryPendingInfo)
		act = append(act, el)

		if _, dup := qcb.qryActived[pending.node.ID]; dup == true {
			log.Warnf("qryMgrQcbPutActived: duplicated, sdl: %s, forWhat: %d, target: %x",
				qryMgr.sdlName, qcb.forWhat, qcb.target)
			continue
		}

		icb := qryInstCtrlBlock{
			sdl:        qryMgr.sdl,
			seq:        qcb.icbSeq,
			qryReq:     qcb.qryReq,
			name:       "qryMgrIcb" + fmt.Sprintf("_q%d_i%d", qcb.seq, qcb.icbSeq),
			ptnInst:    nil,
			ptnConMgr:  nil,
			ptnRutMgr:  nil,
			ptnQryMgr:  nil,
			local:      qryMgr.qmCfg.local,
			status:     qisNull,
			target:     qcb.target,
			to:         pending.node,
			dir:        ConInstDirUnknown,
			qTid:       sch.SchInvalidTid,
			begTime:    time.Time{},
			endTime:    time.Time{},
			conBegTime: time.Time{},
			conEndTime: time.Time{},
			depth:      pending.depth,
		}

		log.Debugf("qryMgrQcbPutActived: sdl: %s, ForWhat: %d, target: %x",
			qryMgr.sdlName, icb.qryReq.ForWhat, qcb.target)

		qryInst := NewQryInst()
		qryInst.icb = &icb
		td := sch.SchTaskDescription{
			Name:   icb.name,
			MbSize: sch.SchDftMbSize,
			Ep:     qryInst,
			Wd:     &sch.SchWatchDog{HaveDog: false},
			Flag:   sch.SchCreatedGo,
			DieCb:  nil,
			UserDa: &icb,
		}

		eno, ptn := qryMgr.sdl.SchCreateTask(&td)
		if eno != sch.SchEnoNone || ptn == nil {

			log.Errorf("qryMgrQcbPutActived: " +
				"SchCreateTask failed, sdl: %s, forWhat: %d, eno: %d, target: %x",
					qryMgr.sdlName, qcb.forWhat, eno, qcb.target)

			dhtEno = DhtEnoScheduler
			break
		}

		icb.ptnInst = ptn
		qcb.icbSeq++

		po := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&po, qryMgr.ptnMe, ptn, sch.EvSchPoweron, nil)
		if eno := qryMgr.sdl.SchSendMessage(&po); eno != sch.SchEnoNone {
			log.Errorf("qryMgrQcbPutActived: send EvSchPoweron failed, " +
				"sdl: %s, forWhat: %d, eno: %d, target: %x",
				qryMgr.sdlName, qcb.forWhat, eno, qcb.target)
			qryMgr.sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoKilled)
			dhtEno = DhtEnoScheduler
			break
		}

		start := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&start, qryMgr.ptnMe, ptn, sch.EvDhtQryInstStartReq, nil)
		if eno := qryMgr.sdl.SchSendMessage(&start); eno != sch.SchEnoNone {
			log.Errorf("qryMgrQcbPutActived: send EvDhtQryInstStartReq failed, " +
				"sdl: %s, eno: %d, target: %x",
				qryMgr.sdlName, eno, qcb.target)
			qryMgr.sdl.SchTaskDone(icb.ptnInst, icb.name, sch.SchEnoKilled)
			dhtEno = DhtEnoScheduler
			break
		}

		cnt++
		qcb.width++
		qcb.qryActived[icb.to.ID] = &icb
		qcb.qryHistory[icb.to.ID] = pending

		log.Debugf("qryMgrQcbPutActived: EvSchPoweron and EvDhtQryInstStartReq sent, " +
			"sdl: %s, forWhat: %d, target: %x",
			qryMgr.sdlName, qcb.forWhat, qcb.target)
	}

	for _, el := range act {
		qcb.qryPending.Remove(el)
	}

	log.Debugf("qryMgrQcbPutActived: " +
		"sdl: %s, forWhat: %d, pending: %d, actived: %d, history: %d, target: %x",
		qryMgr.sdlName, qcb.forWhat, qcb.qryPending.Len(), len(qcb.qryActived), len(qcb.qryHistory), qcb.target)

	return DhtErrno(dhtEno), cnt
}

//
// Start timer for query control block
//
func (qryMgr *QryMgr) qryMgrQcbStartTimer(qcb *qryCtrlBlock) DhtErrno {
	td := sch.TimerDescription{
		Name:  "qryMgrQcbTimer" + fmt.Sprintf("%d", qcb.seq),
		Utid:  sch.DhtQryMgrQcbTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   qryMgr.qmCfg.qryExpired,
		Extra: qcb,
	}
	tid := sch.SchInvalidTid
	eno := sch.SchEnoUnknown
	if eno, tid = qryMgr.sdl.SchSetTimer(qryMgr.ptnMe, &td); eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debugf("qryMgrQcbStartTimer: SchSetTimer failed, eno: %d", eno)
		return DhtEnoScheduler
	}
	qcb.qryTid = tid
	return DhtEnoNone
}

//
// Query control block timer handler
//
func (qryMgr *QryMgr) qcbTimerHandler(qcb *qryCtrlBlock) sch.SchErrno {
	log.Debugf("qcbTimerHandler: timeout, " +
		"sdl: %s, status: %d, forWhat: %d, width: %d, depth: %d, target: %x",
		qryMgr.sdlName,	qcb.status, qcb.forWhat, qcb.width, qcb.depth, qcb.target)
	qryMgr.qryMgrResultReport(qcb, DhtEnoTimeout.GetEno(), nil, nil, nil)
	qryMgr.qryMgrDelQcb(delQcb4Timeout, qcb.target)
	return sch.SchEnoNone
}

//
// Query result report
//
func (qryMgr *QryMgr) qryMgrResultReport(
	qcb *qryCtrlBlock,
	eno int,
	peer *config.Node,
	val []byte,
	prd *sch.Provider) DhtErrno {

	//
	// deal with batch operation, currently it's only get-value-batch supported.
	//

	dhtEno := DhtEnoNone
	schEno := sch.SchEnoNone

	if qcb.qryReq.Batch {
		id := qcb.qryReq.BatchId
		switch qcb.forWhat {
		case MID_GETVALUE_REQ :
			inst, ok := qryMgr.gvbTab[id]
			if !ok {
				log.Warnf("qryMgrResultReport: batch not found, sdl: %s, id: %d", qryMgr.sdlName, id)
				return DhtEnoNotFound
			}
			if eno == int(DhtEnoNone) {
				inst.output<-val
				inst.got++
			} else {
				inst.failed++
			}
			inst.getting--
			delete(inst.pending, qcb.qryReq.Target)

			log.Debugf("qryMgrResultReport: gvb, " +
				"sdl: %s, id: %d, staus: %d, getting: %d, got: %d, failed: %d, remain: %d",
					qryMgr.sdlName, id, inst.status, inst.getting, inst.got, inst.failed, inst.remain)

			if inst.getting == 0 {
				log.Debugf("qryMgrResultReport: gvb over, " +
					"sdl: %s, id: %d, staus: %d, getting: %d, got: %d, failed: %d, remain: %d",
					qryMgr.sdlName, id, inst.status, inst.getting, inst.got, inst.failed, inst.remain)
				if inst.failed > 0 {
					inst.status = sch.GVBS_TERMED
					if eno := qryMgr.gvbStatusReport(inst, "qryMgrResultReport"); eno != sch.SchEnoNone {
						log.Errorf("qryMgrResultReport: gvbStatusReport failed, " +
							"sdl: %s, id: %d, staus: %d, getting: %d, got: %d, failed: %d, remain: %d",
							qryMgr.sdlName, id, inst.status, inst.getting, inst.got, inst.failed, inst.remain)
						dhtEno = DhtEnoInternal
					}
				} else {
					inst.status = sch.GVBS_DONE
					if eno := qryMgr.gvbStatusReport(inst, "qryMgrResultReport"); eno != sch.SchEnoNone {
						log.Errorf("qryMgrResultReport: gvbStatusReport failed, " +
							"sdl: %s, id: %d, staus: %d, getting: %d, got: %d, failed: %d, remain: %d",
							qryMgr.sdlName, id, inst.status, inst.getting, inst.got, inst.failed, inst.remain)
						dhtEno = DhtEnoInternal
					}
				}
				delete(qryMgr.gvbTab, id)
			}
			if inst.remain > 0 {
				if dhtEno, schEno = qryMgr.gvbStartNext(inst); dhtEno != DhtEnoNone || schEno != sch.SchEnoNone {
					log.Errorf("qryMgrResultReport: gvbStartNext failed, "+
						"sdl: %s, id: %d, staus: %d, getting: %d, got: %d, failed: %d, remain: %d",
						qryMgr.sdlName, id, inst.status, inst.getting, inst.got, inst.failed, inst.remain)
				}
			}

		default:
			log.Errorf("qryMgrResultReport: batch not support, %d, sdl: %s, id: %d",
				qcb.forWhat, qryMgr.sdlName, id)
			return DhtEnoMismatched
		}

		if dhtEno != DhtEnoNone {
			return dhtEno
		}
		if schEno != sch.SchEnoNone {
			return DhtEnoScheduler
		}
		return DhtEnoNone
	}

	//
	// notice:
	//
	// 0) the target backup in qcb has it's meaning with "forWhat", as:
	//
	//		target						forWhat
	// =================================================
	//		peer identity				FIND_NODE
	//		key of value				GET_VALUE
	//		key of value				GET_PROVIDER
	//
	// 1) if eno indicated none of errors, then, the target is found, according to "forWhat",
	// one can obtain peer or value or provider from parameters passed in;
	//
	// 2) if eno indicated anything than EnoNone, then parameters "peer", "val", "prd" are
	// all be nil, and the peer(node) identities suggested to by query are backup in the
	// query result in "qcb";
	//
	// the event EvDhtQryMgrQueryResultInd handler should take the above into account to deal
	// with this event when it's received in the owner task of the "qcb".
	//
	// notice: "peer" passed in is not used, the "qcb.qryResult"
	//

	var ind = sch.MsgDhtQryMgrQueryResultInd{
		Eno:     eno,
		ForWhat: qcb.forWhat,
		Target:  qcb.target,
		Val:     val,
		Prds:    nil,
		Peers:   nil,
	}

	if prd != nil {
		ind.Prds = prd.Nodes
	}

	li := qcb.qryResult
	if li.Len() > 0 {
		idx := 0
		ind.Peers = make([]*config.Node, li.Len())
		for el := li.Front(); el != nil; el = el.Next() {
			v := el.Value.(*qryResultInfo)
			ind.Peers[idx] = &v.node
		}
	}

	log.Debugf("qryMgrResultReport: sdl: %s, eno: %d, ForWhat: %d, task: %s, target: %x",
		qryMgr.sdlName, ind.Eno, ind.ForWhat, qryMgr.sdl.SchGetTaskName(qcb.ptnOwner), ind.Target)

	msg := new(sch.SchMessage)
	qryMgr.sdl.SchMakeMessage(msg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
	if eno := qryMgr.sdl.SchSendMessage(msg); eno != sch.SchEnoNone {
		log.Errorf("qryMgrResultReport: send message failed, eno: %d", eno)
		return DhtEnoScheduler
	}
	return DhtEnoNone
}

//
// switch address to that reported from nat manager
//
func (qryMgr *QryMgr) switch2NatAddr(proto string) DhtErrno {
	if proto == nat.NATP_TCP {
		qryMgr.qmCfg.local.IP = qryMgr.pubTcpIp
		qryMgr.qmCfg.local.TCP = uint16(qryMgr.pubTcpPort & 0xffff)
		return DhtEnoNone
	}
	log.Debugf("switch2NatAddr: invalid protocol: %s", proto)
	return DhtEnoParameter
}

//
// start nat map switching procedure
//
func (qryMgr *QryMgr) natMapSwitch() DhtErrno {
	log.Debugf("natMapSwitch: entered")
	for k, qcb := range qryMgr.qcbTab {
		fw := qcb.forWhat
		ind := sch.MsgDhtQryMgrQueryResultInd{
			Eno:     DhtEnoNatMapping.GetEno(),
			ForWhat: fw,
			Target:  k,
			Peers:   make([]*config.Node, 0),
			Val:     make([]byte, 0),
			Prds:    make([]*config.Node, 0),
		}
		msg := sch.SchMessage{}
		if qcb.ptnOwner != nil {
			qryMgr.sdl.SchMakeMessage(&msg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
			qryMgr.sdl.SchSendMessage(&msg)
		}
		log.Debugf("natMapSwitch: EvDhtQryMgrQueryResultInd sent")

		if eno := qryMgr.qryMgrDelQcb(delQcb4PubAddrSwitch, k); eno != DhtEnoNone {
			log.Debugf("natMapSwitch: qryMgrDelQcb failed, eno: %d", eno)
			return eno
		}
		log.Debugf("natMapSwitch: qcb deleted, key: %x", k)
	}

	log.Debugf("natMapSwitch: call switch2NatAddr")
	qryMgr.switch2NatAddr(nat.NATP_TCP)

	msg := sch.SchMessage{}
	qryMgr.sdl.SchMakeMessage(&msg, qryMgr.ptnMe, qryMgr.ptnDhtMgr, sch.EvDhtQryMgrPubAddrSwitchInd, nil)
	qryMgr.sdl.SchSendMessage(&msg)
	log.Debugf("natMapSwitch: EvDhtQryMgrPubAddrSwitchInd sent")

	return DhtEnoNone
}

//
// report get-value-batch status to datastore manager
//
func (qryMgr *QryMgr)gvbStatusReport(gvbCb *gvbCtrlBlock, ctxt string) sch.SchErrno {
	ind := sch.MsgDhtDsStatusInd {
		GvbId: gvbCb.gvbId,
		Status: gvbCb.status,
		Got: gvbCb.got,
		Getting: gvbCb.getting,
		Failed: gvbCb.failed,
		Remain: gvbCb.remain,
	}
	log.Debugf("gvbStatusReport: ctxt: %s, ind[id,status,getting,got,failed,remain]:%v",
		ctxt, ind)
	msg := new(sch.SchMessage)
	qryMgr.sdl.SchMakeMessage(msg, qryMgr.ptnMe, qryMgr.ptnDsMgr, sch.EvDhtDsGvbStatusInd, &ind)
	if eno := qryMgr.sdl.SchSendMessage(msg); eno != sch.SchEnoNone {
		log.Errorf("gvbStatusReport: send EvDhtDsGvbStatusInd failed, sdl: %s, eno: %d",
			qryMgr.sdlName, eno)
		return eno
	}
	return sch.SchEnoNone
}

//
// get-value-batch start to query the next key
//
func (qryMgr *QryMgr)gvbStartNext(gvbCb *gvbCtrlBlock) (DhtErrno, sch.SchErrno) {
	if gvbCb.remain <= 0 {
		log.Warnf("gvbStartNext: none, sdl: %s", qryMgr.sdlName)
		return DhtEnoInternal, sch.SchEnoNone
	}
	next := gvbCb.head
	gvbCb.head += 1
	gvr := sch.MsgDhtMgrGetValueReq {
		Key: (*gvbCb.keys)[next],
	}
	qry := sch.MsgDhtQryMgrQueryStartReq{
		Msg: &gvr,
		ForWhat: MID_GETVALUE_REQ,
		Seq: GetQuerySeqNo(qryMgr.sdlName),
		Batch: true,
		BatchId: gvbCb.gvbId,
	}
	copy(qry.Target[0:], gvr.Key)
	if _, dup := gvbCb.pending[qry.Target]; dup {
		log.Warnf("gvbStartNext: duplicated target, sdl: %s, target: %x", qryMgr.sdlName, gvr.Key)
		gvbCb.failed += 1
		gvbCb.remain -= 1
		return DhtEnoDuplicated, sch.SchEnoNone
	}
	if eno := qryMgr.queryStartReq(qryMgr.ptnMe, &qry); eno != sch.SchEnoNone {
		log.Warnf("gvbStartNext: start query failed, sdl: %s, eno: %d", qryMgr.sdlName, eno)
		gvbCb.failed += 1
		gvbCb.remain -= 1
		return DhtEnoInternal, eno
	}
	gvbCb.getting += 1
	gvbCb.remain -= 1
	gvbCb.pending[qry.Target] = gvbCb
	return DhtEnoNone, sch.SchEnoNone
}

//
// get unique sequence number all query
//
var mapQrySeqLLock sync.Mutex
var mapQrySeqLock = make(map[string]sync.Mutex, 0)

func GetQuerySeqNo(name string) int64 {
	mapQrySeqLLock.Lock()
	qrySeqLock, ok := mapQrySeqLock[name]
	mapQrySeqLLock.Unlock()
	if !ok {
		panic("GetQuerySeqNo: internal error! seems system not ready")
	}
	qrySeqLock.Lock()
	defer qrySeqLock.Unlock()
	return time.Now().UnixNano()
}

