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
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
	log "github.com/yeeco/gyee/p2p/logger"
)

//
// Constants
//
const (
	QryMgrName = sch.DhtQryMgrName		// query manage name registered in shceduler
	qryMgrMaxActInsts = 4				// max concurrent actived instances for one query
	qryMgrQryExpired = time.Second * 16	// duration to get expired for a query
)


//
// Query control block
//
type QryStatus = int

const (
	qsNull		= iota		// null state
	qsPrepare				// in preparing, waiting for the nearest response from route manager
	qsInited				// had been initialized
)

type qryCtrlBlock struct {
	ptnOwner	interface{}								// owner task node pointer
	target		config.NodeID							// target is looking up
	status		QryStatus								// query status
	qryHist		map[config.NodeID]*rutMgrBucketNode		// history peers had been queried
	qryPending	map[config.NodeID]*rutMgrBucketNode		// pending peers to be queried
	qryActived	map[config.NodeID]*qryInstCtrlBlock		// queries activated
}

//
// Query instance control block
//
type qryInstCtrlBlock struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// instance name
	ptnInst		interface{}			// pointer to query instance task node
	target		config.NodeID		// target is looking up
	to			rutMgrBucketNode	// to whom the query message sent
	qTid		int					// query timer identity
	begTime		time.Time			// query begin time
	endTime		time.Time			// query end time
}

//
// Query manager
//
type QryMgr struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// query manager name
	tep			sch.SchUserTaskEp				// task entry
	ptnMe		interface{}						// pointer to task node of myself
	ptnRutMgr	interface{}						// pointer to task node of route manager
	ptnDhtMgr	interface{}						// pointer to task node of dht manager
	instSeq		int								// query instance sequence number
	qcbTab		map[config.NodeID]*qryCtrlBlock	// query control blocks
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qryMgr := QryMgr{
		sdl:		nil,
		name:		QryMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnDhtMgr:	nil,
		instSeq:	0,
		qcbTab:		map[config.NodeID]*qryCtrlBlock{},
	}

	qryMgr.tep = qryMgr.qryMgrProc

	return &qryMgr
}

//
// Entry point exported to shceduler
//
func (qryMgr *QryMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return qryMgr.tep(ptn, msg)
}

//
// Query manager entry
//
func (qryMgr *QryMgr)qryMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("qryMgrProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = qryMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = qryMgr.poweroff(ptn)

	case sch.EvDhtQryMgrQueryStartReq:
		sender := qryMgr.sdl.SchGetSender(msg)
		eno = qryMgr.queryStartReq(sender, msg.Body.(*sch.MsgDhtQryMgrQueryStartReq))

	case sch.EvDhtRutMgrNearestRsp:
		eno = qryMgr.rutNearestRsp(msg.Body.(*sch.MsgDhtRutMgrNearestRsp))

	case sch.EvDhtQryMgrQueryStopReq:
		eno = qryMgr.queryStopReq(msg.Body.(*sch.MsgDhtQryMgrQueryStopReq))

	case sch.EvDhtRutMgrNotificationInd:
		eno = qryMgr.rutNotificationInd(msg.Body.(*sch.MsgDhtRutMgrNotificationInd))

	case sch.EvDhtQryInstResultInd:
		eno = qryMgr.instResultInd(msg.Body.(*sch.MsgDhtQryInstResultInd))

	case sch.EvDhtQryInstStopRsp:
		eno = qryMgr.instStopRsp(msg.Body.(*sch.MsgDhtQryInstStopRsp))

	default:
		log.LogCallerFileLine("qryMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (qryMgr *QryMgr)poweron(ptn interface{}) sch.SchErrno {

	var eno sch.SchErrno

	qryMgr.ptnMe = ptn
	if qryMgr.sdl = sch.SchGetScheduler(ptn); qryMgr.sdl == nil {
		log.LogCallerFileLine("poweron: nil scheduler")
		return sch.SchEnoInternal
	}

	if eno, qryMgr.ptnDhtMgr = qryMgr.sdl.SchGetTaskNodeByName(DhtMgrName);
	eno != sch.SchEnoNone {
		log.LogCallerFileLine("poweron: get task failed, task: %s", DhtMgrName)
		return eno
	}

	if eno, qryMgr.ptnRutMgr = qryMgr.sdl.SchGetTaskNodeByName(RutMgrName);
	eno != sch.SchEnoNone {
		log.LogCallerFileLine("poweron: get task failed, task: %s", RutMgrName)
		return eno
	}

	if dhtEno := qryMgr.qryMgrGetConfig(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("poweron: qryMgrGetConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (qryMgr *QryMgr)poweroff(ptn interface{}) sch.SchErrno {

	log.LogCallerFileLine("poweroff: task will be done ...")

	po := sch.SchMessage{}

	for _, qcb := range qryMgr.qcbTab {
		for _, qry := range qcb.qryActived {
			qryMgr.sdl.SchMakeMessage(&po, qryMgr.ptnMe, qry.ptnInst, sch.EvSchPoweroff, nil)
			qryMgr.sdl.SchSendMessage(&po)
		}
	}

	return qryMgr.sdl.SchTaskDone(qryMgr.ptnMe, sch.SchEnoKilled)
}

//
// Query start request handler
//
func (qryMgr *QryMgr)queryStartReq(sender interface{}, msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {

	var target = msg.Target
	var rsp = sch.MsgDhtQryMgrQueryStartRsp{Target:target, Eno:DhtEnoUnknown}
	var qcb *qryCtrlBlock = nil
	var schMsg = sch.SchMessage{}

	var nearestReq = sch.MsgDhtRutMgrNearestReq{
		Target:	target,
		Max:	rutMgrMaxNearest,
		NtfReq:	true,
		Task:	qryMgr.ptnMe,
	}

	if _, dup := qryMgr.qcbTab[target]; dup {
		rsp.Eno = DhtEnoDuplicated
		goto _rsp2Sender
	}

	qcb = new(qryCtrlBlock)
	qcb.ptnOwner = sender
	qcb.target = target
	qcb.status = qsNull
	qcb.qryHist = make(map[config.NodeID]*rutMgrBucketNode, 0)
	qcb.qryPending = make(map[config.NodeID]*rutMgrBucketNode, 0)
	qcb.qryActived = make(map[config.NodeID]*qryInstCtrlBlock, 0)

	qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &nearestReq)
	qryMgr.sdl.SchSendMessage(&schMsg)

	rsp.Eno = DhtEnoNone

_rsp2Sender:

	qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, sender, sch.EvDhtQryMgrQueryStartRsp, &rsp)
	qryMgr.sdl.SchSendMessage(&schMsg)

	if rsp.Eno != DhtEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Nearest response handler
//
func (qryMgr *QryMgr)rutNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Query stop request handler
//
func (qryMgr *QryMgr)queryStopReq(msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
//Route notification handler
//
func (qryMgr *QryMgr)rutNotificationInd(msg *sch.MsgDhtRutMgrNotificationInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance query result indication handler
//
func (qryMgr *QryMgr)instResultInd(msg *sch.MsgDhtQryInstResultInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance stop response handler
//
func (qryMgr *QryMgr)instStopRsp(msg *sch.MsgDhtQryInstStopRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Get query manager configuration
//
func (qryMgr *QryMgr)qryMgrGetConfig() DhtErrno {
	return DhtEnoNone
}