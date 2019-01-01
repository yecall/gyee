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
	"time"
	"bytes"
	"container/list"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	config	"github.com/yeeco/gyee/p2p/config"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type qryMgrLogger struct {
	debug__		bool
}

var qryLog = qryMgrLogger  {
	debug__:	false,
}

func (log qryMgrLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Constants
//
const (
	QryMgrName = sch.DhtQryMgrName		// query manage name registered in shceduler
	qryMgrMaxPendings = 64				// max pendings can be held in the list
	qryMgrMaxActInsts = 8				// max concurrent actived instances for one query
	qryMgrQryExpired = time.Second * 60	// duration to get expired for a query
	qryMgrQryMaxWidth = 64				// not the true "width", the max number of peers queryied
	qryMgrQryMaxDepth = 8				// the max depth for a query
	qryInstExpired = time.Second * 16	// duration to get expired for a query instance
)

//
// Query manager configuration
//
type qryMgrCfg struct {
	local			*config.Node		// pointer to local node specification
	maxPendings		int					// max pendings can be held in the list
	maxActInsts		int					// max concurrent actived instances for one query
	qryExpired		time.Duration		// duration to get expired for a query
	qryInstExpired	time.Duration		// duration to get expired for a query instance
}

//
// Query control block status
//
type QryStatus = int

const (
	qsNull		= iota		// null state
	qsPreparing				// in preparing, waiting for the nearest response from route manager
	qsInited				// had been initialized
)

//
// Query result node info
//
type qryResultInfo struct {
	node	config.Node				// peer node info
	pcs		conMgrPeerConnStat		// connection status
	dist	int						// distance from local node
}

//
// Query pending node info
//
type qryPendingInfo struct {
	rutMgrBucketNode				// bucket node
	depth		int					// depth
}

//
// Query control block
//
type qryCtrlBlock struct {
	ptnOwner	interface{}								// owner task node pointer
	qryReq		*sch.MsgDhtQryMgrQueryStartReq			// original query request message
	seq			int										// sequence number
	forWhat		int										// what's the query control block for
	target		config.DsKey							// target for looking up
	status		QryStatus								// query status
	qryHistory	map[config.NodeID]*qryPendingInfo		// history peers had been queried
	qryPending	*list.List								// pending peers to be queried, with type qryPendingInfo
	qryActived	map[config.NodeID]*qryInstCtrlBlock		// queries activated
	qryResult	*list.List								// list of qryResultNodeInfo type object
	qryTid		int										// query timer identity
	icbSeq		int										// query instance control block sequence number
	rutNtfFlag	bool									// if notification asked for
	width		int										// the current number of peer had been queried
	depth		int										// the current max depth of query
}

//
// Query instance status
//
const (
	qisNull		= iota				// null state
	qisInited						// had been inited to ready to start
	qisWaitConnect					// connect request sent, wait response from connection manager task
	qisWaitResponse					// query sent, wait response from peer
	qisDone							// done
)

//
// Query instance control block
//
type qryInstCtrlBlock struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	seq			int								// sequence number
	qryReq		*sch.MsgDhtQryMgrQueryStartReq	// original query request message
	name		string							// instance name
	ptnInst		interface{}						// pointer to query instance task node
	ptnConMgr	interface{}						// pointer to connection manager task node
	ptnQryMgr	interface{}						// pointer to query manager task node
	ptnRutMgr	interface{}						// pointer to rute manager task node
	status		int								// instance status
	local		*config.Node					// pointer to local node specification
	target		config.DsKey					// target is looking up
	to			config.Node						// to whom the query message sent
	dir			int								// connection direction
	qTid		int								// query timer identity
	begTime		time.Time						// query begin time
	endTime		time.Time						// query end time
	conBegTime	time.Time						// time to start connection
	conEndTime	time.Time						// time connection established
	depth		int								// the current depth of the query instance
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
	qcbTab		map[config.DsKey]*qryCtrlBlock	// query control blocks
	qmCfg		qryMgrCfg						// query manager configuration
	qcbSeq		int								// query control block sequence number
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qmCfg := qryMgrCfg {
		maxPendings:	qryMgrMaxPendings,
		maxActInsts:	qryMgrMaxActInsts,
		qryExpired:		qryMgrQryExpired,
		qryInstExpired:	qryInstExpired,
	}

	qryMgr := QryMgr{
		name:		QryMgrName,
		instSeq:	0,
		qcbTab:		map[config.DsKey]*qryCtrlBlock{},
		qmCfg:		qmCfg,
		qcbSeq:		0,
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

	qryLog.Debug("qryMgrProc: ptn: %p, msg.Id: %d", ptn, msg.Id)
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

	case sch.EvDhtQryInstStopRsp:
		eno = qryMgr.instStopRsp(msg.Body.(*sch.MsgDhtQryInstStopRsp))

	default:
		qryLog.Debug("qryMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	qryLog.Debug("qryMgrProc: get out, ptn: %p, msg.Id: %d", ptn, msg.Id)

	return eno
}

//
// Poweron handler
//
func (qryMgr *QryMgr)poweron(ptn interface{}) sch.SchErrno {

	var eno sch.SchErrno

	qryMgr.ptnMe = ptn
	if qryMgr.sdl = sch.SchGetScheduler(ptn); qryMgr.sdl == nil {
		qryLog.Debug("poweron: nil scheduler")
		return sch.SchEnoInternal
	}

	if eno, qryMgr.ptnDhtMgr = qryMgr.sdl.SchGetUserTaskNode(DhtMgrName);
	eno != sch.SchEnoNone {
		qryLog.Debug("poweron: get task failed, task: %s", DhtMgrName)
		return eno
	}

	if eno, qryMgr.ptnRutMgr = qryMgr.sdl.SchGetUserTaskNode(RutMgrName);
	eno != sch.SchEnoNone {
		qryLog.Debug("poweron: get task failed, task: %s", RutMgrName)
		return eno
	}

	if dhtEno := qryMgr.qryMgrGetConfig(); dhtEno != DhtEnoNone {
		qryLog.Debug("poweron: qryMgrGetConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (qryMgr *QryMgr)poweroff(ptn interface{}) sch.SchErrno {

	qryLog.Debug("poweroff: task will be done ...")

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

	if sender == nil || msg == nil {
		qryLog.Debug("queryStartReq: invalid prameters")
		return sch.SchEnoParameter
	}

	if msg.ForWhat != MID_PUTVALUE &&
		msg.ForWhat != MID_PUTPROVIDER &&
		msg.ForWhat != MID_FINDNODE &&
		msg.ForWhat != MID_GETPROVIDER_REQ &&
		msg.ForWhat != MID_GETVALUE_REQ {
		qryLog.Debug("queryStartReq: unknown what's for")
		return sch.SchEnoMismatched
	}

	qryLog.Debug("queryStartReq: sender: %p, msg: %+v", sender, msg)

	var forWhat = msg.ForWhat
	var rsp = sch.MsgDhtQryMgrQueryStartRsp{Target: msg.Target, Eno:DhtEnoUnknown.GetEno()}
	var qcb *qryCtrlBlock = nil
	var schMsg = sch.SchMessage{}

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
		qryLog.Debug("queryStartReq: duplicated target: %x", msg.Target)
		rsp.Eno = DhtEnoDuplicated.GetEno()
		goto _rsp2Sender
	}

	qcb = new(qryCtrlBlock)
	qcb.ptnOwner = sender
	qcb.qryReq = msg
	qcb.seq = qryMgr.qcbSeq
	qcb.forWhat = forWhat
	qryMgr.qcbSeq++
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

	qryLog.Debug("queryStartReq: qcb: %+v", *qcb)

	qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &nearestReq)
	qryMgr.sdl.SchSendMessage(&schMsg)

	rsp.Eno = DhtEnoNone.GetEno()

_rsp2Sender:

	qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, sender, sch.EvDhtQryMgrQueryStartRsp, &rsp)
	qryMgr.sdl.SchSendMessage(&schMsg)

	if rsp.Eno != DhtEnoNone.GetEno() {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Nearest response handler
//
func (qryMgr *QryMgr)rutNearestRsp(msg *sch.MsgDhtRutMgrNearestRsp) sch.SchErrno {

	if msg == nil {
		qryLog.Debug("rutNearestRsp: invalid parameter")
		return sch.SchEnoParameter
	}

	qryLog.Debug("rutNearestRsp: msg: %+v", msg)

	if msg.ForWhat != MID_PUTVALUE &&
		msg.ForWhat != MID_PUTPROVIDER &&
		msg.ForWhat != MID_FINDNODE &&
		msg.ForWhat != MID_GETPROVIDER_REQ &&
		msg.ForWhat != MID_GETVALUE_REQ {
		qryLog.Debug("rutNearestRsp: unknown what's for")
		return sch.SchEnoMismatched
	}

	forWhat := msg.ForWhat
	target := msg.Target

	qcb, ok := qryMgr.qcbTab[target]
	if !ok {
		qryLog.Debug("rutNearestRsp: qcb not exist, target: %x", target)
		return sch.SchEnoNotFound
	}

	if qcb == nil {
		qryLog.Debug("rutNearestRsp: nil qcb, target: %x", target)
		return sch.SchEnoInternal
	}

	if qcb.status != qsPreparing {
		qryLog.Debug("rutNearestRsp: qcb status mismatched, status: %d, target: %x",
			qcb.status, target)
		return sch.SchEnoMismatched
	}

	qryFailed2Sender := func (eno DhtErrno) {
		schMsg := sch.SchMessage{}
		rsp := sch.MsgDhtQryMgrQueryStartRsp{
			Target:	msg.Target,
			Eno:	int(eno),
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryStartRsp, &rsp)
		qryMgr.sdl.SchSendMessage(&schMsg)
	}

	qryOk2Sender := func(peer *config.Node) {
		schMsg := sch.SchMessage{}
		ind := sch.MsgDhtQryMgrQueryResultInd{
			Eno:		DhtEnoNone.GetEno(),
			ForWhat:	msg.ForWhat,
			Target:		target,
			Peers:		[]*config.Node{peer},
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
		qryMgr.sdl.SchSendMessage(&schMsg)
	}

	if msg.Eno != DhtEnoNone.GetEno() {
		qryFailed2Sender(DhtErrno(msg.Eno))
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoNone
	}

	if (msg.Eno == DhtEnoNone.GetEno()) && (msg.Peers == nil || msg.Pcs == nil || msg.Peers == nil) {
		qryLog.Debug("rutNearestRsp: invalid empty nearest set reported")
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoParameter
	}

	peers := msg.Peers.([]*rutMgrBucketNode)
	pcs := msg.Pcs.([]int)
	dists := msg.Dists.([]int)
	if (msg.Eno == DhtEnoNone.GetEno()) && (len(peers) != len(pcs) || len(peers) != len(dists)) {
		qryLog.Debug("rutNearestRsp: invalid nearest set reported")
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoParameter
	}

	if len(peers) == 0 {
		qryLog.Debug("rutNearestRsp: invalid empty nearest set reported")
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
			forWhat == MID_GETPROVIDER_REQ ||
			forWhat == MID_GETVALUE_REQ {
			if bytes.Compare(peer.hash[0:], target[0:]) == 0 {
				qryLog.Debug("rutNearestRsp: target found: %x", target)
				qryOk2Sender(&peer.node)
				qryMgr.qryMgrDelQcb(delQcb4TargetInLocal, target)
				return sch.SchEnoNone
			}
		}

		qri := qryResultInfo {
			node:	peer.node,
			pcs:	pcs[idx],
			dist:	dists[idx],
		}
		qcb.qcbUpdateResult(&qri)
	}

	//
	// start queries by putting nearests to pending queue and then putting
	// pending nodes to be activated.
	//

	qcb.qryPending = list.New()

	pendInfo := make([]*qryPendingInfo, 0)
	for idx := 0; idx < len(peers); idx++ {
		pi := qryPendingInfo{
			rutMgrBucketNode:*peers[idx],
			depth: 0,
		}
		pendInfo = append(pendInfo, &pi)
	}

	var dhtEno = DhtErrno(DhtEnoNone)
	if dhtEno = qcb.qryMgrQcbPutPending(pendInfo, qryMgr.qmCfg.maxPendings); dhtEno == DhtEnoNone {
		if dhtEno = qryMgr.qryMgrQcbStartTimer(qcb); dhtEno == DhtEnoNone {
			qryMgr.qryMgrQcbPutActived(qcb)
			qcb.status = qsInited
			return sch.SchEnoNone
		}
	}

	qryLog.Debug("rutNearestRsp: qryMgrQcbPutPending failed, eno: %d", dhtEno)
	qryFailed2Sender(dhtEno)
	qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
	return sch.SchEnoResource

}

//
// Query stop request handler
//
func (qryMgr *QryMgr)queryStopReq(sender interface{}, msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {

	target := msg.Target
	rsp := sch.MsgDhtQryMgrQueryStopRsp{Target:target, Eno:DhtEnoNone.GetEno()}

	rsp2Sender := func (rsp *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
		var schMsg = sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, sender, sch.EvDhtQryMgrQueryStopRsp, rsp)
		return qryMgr.sdl.SchSendMessage(&schMsg)
	}

	rsp.Eno = int(qryMgr.qryMgrDelQcb(delQcb4Command, target))

	return rsp2Sender(&rsp)
}

//
//Route notification handler
//
func (qryMgr *QryMgr)rutNotificationInd(msg *sch.MsgDhtRutMgrNotificationInd) sch.SchErrno {

	//
	// we get this indication from route manager for we had reigstered to it while
	// we handling the event EvDhtQryMgrQueryStartReq, see it pls.
	//

	var qcb *qryCtrlBlock

	target := msg.Target
	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		qryLog.Debug("rutNotificationInd: target not found: %x", target)
		return sch.SchEnoParameter
	}

	if qcb.status != qsInited {
		qryLog.Debug("rutNotificationInd: query not inited yet for target: %x", target)
		return sch.SchEnoUserTask
	}

	qpi := msg.Peers.([]*rutMgrBucketNode)
	pendInfo := make([]*qryPendingInfo, 0)
	for idx := 0; idx < len(qpi); idx++ {
		var pi = qryPendingInfo{
			rutMgrBucketNode:*qpi[idx],
			depth: 0,
		}
		pendInfo = append(pendInfo, &pi)
	}

	qcb.qryMgrQcbPutPending(pendInfo, qryMgr.qmCfg.maxPendings)

	//
	// try to active more instances since pendings added
	//

	qryMgr.qryMgrQcbPutActived(qcb)

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		qryLog.Debug("rutNotificationInd: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// check if query should be end
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
			qryLog.Debug("rutNotificationInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			qryLog.Debug("rutNotificationInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (qryMgr *QryMgr)instStatusInd(msg *sch.MsgDhtQryInstStatusInd) sch.SchErrno {

	switch msg.Status {
	case qisNull:
		qryLog.Debug("instStatusInd: qisNull")
	case qisInited:
		qryLog.Debug("instStatusInd: qisInited")
	case qisWaitConnect:
		qryLog.Debug("instStatusInd: qisWaitConnect")
	case qisWaitResponse:
		qryLog.Debug("instStatusInd: qisWaitResponse")

	case qisDone:

		//
		// done reported, we delete the instance from manager
		//

		qryLog.Debug("instStatusInd: qisDone")

		qcb, exist := qryMgr.qcbTab[msg.Target]
		if !exist {
			qryLog.Debug("instStatusInd: qcb not found")
			return sch.SchEnoNotFound
		}

		if dhtEno := qryMgr.qryMgrDelIcb(delQcb4QryInstDoneInd, &msg.Target, &msg.Peer); dhtEno != DhtEnoNone {
			qryLog.Debug("instStatusInd: qryMgrDelIcb failed, eno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		//
		// try to activate more
		//

		if eno, num := qryMgr.qryMgrQcbPutActived(qcb); true {
			qryLog.Debug("instStatusInd: " +
				"qryMgrQcbPutActived return with eno: %d, num: %d", eno, num)
		}

		//
		// check against abnormal cases
		//

		if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
			qryLog.Debug("instStatusInd: internal errors")
			return sch.SchEnoUserTask
		}

		//
		// if pending queue and actived queue all are empty, we just report query
		// result and end the query.
		//

		qryLog.Debug("instStatusInd: pending: %d, actived: %d",
			qcb.qryPending.Len(), len(qcb.qryActived))

		if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

			qryLog.Debug("instStatusInd: query done: %x", qcb.target)

			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
				qryLog.Debug("instStatusInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
				return sch.SchEnoUserTask
			}

			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
				qryLog.Debug("instStatusInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
				return sch.SchEnoUserTask
			}
		}

	default:
		qryLog.Debug("instStatusInd: invalid instance status: %d", msg.Status)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Instance query result indication handler
//
func (qryMgr *QryMgr)instResultInd(msg *sch.MsgDhtQryInstResultInd) sch.SchErrno {

	if msg == nil {
		qryLog.Debug("instResultInd: invalid parameter")
		return sch.SchEnoParameter
	}

	qryLog.Debug("instResultInd: msg: %+v", *msg)

	if msg.ForWhat != sch.EvDhtMgrPutValueReq &&
		msg.ForWhat != sch.EvDhtMgrPutProviderReq &&
		msg.ForWhat != sch.EvDhtConInstNeighbors &&
		msg.ForWhat != sch.EvDhtConInstGetProviderRsp &&
		msg.ForWhat != sch.EvDhtConInstGetValRsp {

		qryLog.Debug("instResultInd: mismatched, it's %d", msg.ForWhat)
		return sch.SchEnoMismatched
	}

	var qcb *qryCtrlBlock= nil
	var qpiList = make([]*qryPendingInfo, 0)
	var hashList = make([]*Hash, 0)
	var distList = make([]int, 0)
	var rutMgr = qryMgr.sdl.SchGetTaskObject(RutMgrName).(*RutMgr)

	if len(msg.Peers) != len(msg.Pcs) {
		qryLog.Debug("instResultInd: mismatched Peers and Pcs")
		return sch.SchEnoMismatched
	}

	if rutMgr == nil {
		qryLog.Debug("instResultInd: nil route manager")
		return sch.SchEnoInternal
	}

	//
	// remove possible local node identity from message
	//

	for idx, peer := range msg.Peers {
		if bytes.Compare(peer.ID[0:], qryMgr.qmCfg.local.ID[0:]) == 0 {
			msg.Peers = append(msg.Peers[0:idx], msg.Peers[idx+1:]...)
			msg.Pcs = append(msg.Pcs[0:idx], msg.Pcs[idx+1:]...)
		}
	}

	//
	// for the peer send the message, update route manager
	//

	from := msg.From
	latency := msg.Latency

	updateReq2RutMgr := func(peer *config.Node, dur time.Duration) sch.SchErrno {
		var schMsg= sch.SchMessage{}
		var updateReq = sch.MsgDhtRutMgrUpdateReq{
			Why: rutMgrUpdate4Query,
			Eno: DhtEnoNone.GetEno(),
			Seens: []config.Node{
				*peer,
			},
			Duras: []time.Duration{
				dur,
			},
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &updateReq)
		return qryMgr.sdl.SchSendMessage(&schMsg)
	}

	updateReq2RutMgr(&from, latency)

	//
	// update query result
	//

	target := msg.Target
	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		qryLog.Debug("instResultInd: not found, target: %x", target)
		return sch.SchEnoUserTask
	}

	icb, ok := qcb.qryActived[from.ID]
	if !ok || icb == nil {
		qryLog.Debug("instResultInd: target not found: %x", target)
		return sch.SchEnoUserTask
	}

	depth := icb.depth

	for idx, peer := range msg.Peers {

		hash := rutMgrNodeId2Hash(peer.ID)
		hashList = append(hashList, hash)
		dist := rutMgr.rutMgrLog2Dist(nil, hash)
		distList = append(distList, dist)

		qri := qryResultInfo {
			node:	*peer,
			pcs:	conMgrPeerConnStat(msg.Pcs[idx]),
			dist:	distList[idx],
		}

		qcb.qcbUpdateResult(&qri)
	}

	//
	// check if target found: if true, query should be ended, report the result
	//

	if msg.ForWhat == sch.EvDhtConInstNeighbors ||
		msg.ForWhat == sch.EvDhtConInstGetProviderRsp ||
		msg.ForWhat == sch.EvDhtConInstGetValRsp {

		for _, peer := range msg.Peers {

			key := rutMgrNodeId2Hash(peer.ID)

			if bytes.Compare((*key)[0:], target[0:]) == 0 {

				qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), peer, msg.Value, msg.Provider)

				if dhtEno := qryMgr.qryMgrDelQcb(delQcb4TargetFound, qcb.target); dhtEno != DhtEnoNone {
					qryLog.Debug("instResultInd: qryMgrDelQcb failed, eno: %d", dhtEno)
					return sch.SchEnoUserTask
				}

				return sch.SchEnoNone
			}
		}
	}

	//
	// delete query instance control block since we got the result
	//

	if eno := qryMgr.qryMgrDelIcb(delQcb4QryInstResultInd, &msg.Target, &msg.From.ID); eno != DhtEnoNone {
		qryLog.Debug("instResultInd: qryMgrDelIcb failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	//
	// check the query depth and width, if reached, we end the query by reporting the result and
	// removing the query control block
	//

	if depth > qcb.depth {
		qcb.depth = depth
	}

	if msg.ForWhat == sch.EvDhtConInstNeighbors ||
		msg.ForWhat == sch.EvDhtConInstGetProviderRsp ||
		msg.ForWhat == sch.EvDhtConInstGetValRsp {

		if qcb.depth > qryMgrQryMaxDepth || len(qcb.qryHistory) >= qryMgrQryMaxWidth {

			qryLog.Debug("instResultInd: limited to stop query, depth: %d, width: %d", qcb.depth, len(qcb.qryHistory))

			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
				qryLog.Debug("instResultInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
				return sch.SchEnoUserTask
			}

			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
				qryLog.Debug("instResultInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
				return sch.SchEnoUserTask
			}

			return sch.SchEnoNone
		}

		//
		// put peers reported in the message into query control block pending queue,
		// and try to active more query instances after that.
		//

		for idx, peer := range msg.Peers {
			var qpi = qryPendingInfo{
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

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		qryLog.Debug("instResultInd: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// if pending queue and actived queue all are empty, we just report query
	// result and end the query.
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if msg.ForWhat == sch.EvDhtConInstNeighbors ||
			msg.ForWhat == sch.EvDhtConInstGetProviderRsp ||
			msg.ForWhat == sch.EvDhtConInstGetValRsp {
			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
				qryLog.Debug("instResultInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			}
		} else {
			if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNone.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
				qryLog.Debug("instResultInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			}
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			qryLog.Debug("instResultInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Instance stop response handler
//
func (qryMgr *QryMgr)instStopRsp(msg *sch.MsgDhtQryInstStopRsp) sch.SchErrno {

	var qcb *qryCtrlBlock
	var icb *qryInstCtrlBlock

	target := msg.Target
	to := msg.To.ID

	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		qryLog.Debug("instStopRsp: target not found: %x", target)
		return sch.SchEnoUserTask
	}

	//
	// done the instancce task. Since here the instance tells it's stopped, we
	// need not to send power off event, we done it directly.
	//

	if icb = qcb.qryActived[to]; icb == nil {
		qryLog.Debug("instStopRsp: instance not found: %x", to)
		icb.sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)
		return sch.SchEnoUserTask
	}

	delete(qcb.qryActived, to)

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		qryLog.Debug("instStopRsp: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// chcek if some activateds and pendings, if none of them, the query is over
	// then, we can report the result about this query and free it.
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if dhtEno := qryMgr.qryMgrResultReport(qcb, DhtEnoNotFound.GetEno(), nil, nil, nil); dhtEno != DhtEnoNone {
			qryLog.Debug("instStopRsp: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			qryLog.Debug("instStopRsp: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		return sch.SchEnoNone
	}

	//
	// here we try active more instances for one had been removed
	//

	if dhtEno, _ := qryMgr.qryMgrQcbPutActived(qcb); dhtEno != DhtEnoNone {
		qryLog.Debug("instStopRsp: qryMgrQcbPutActived failed, eno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Get query manager configuration
//
func (qryMgr *QryMgr)qryMgrGetConfig() DhtErrno {

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
	delQcb4TargetFound	= iota		// target had been found
	delQcb4NoMoreQueries			// no pendings and no actived instances
	delQcb4Timeout					// query manager time out for the control block
	delQcb4Command					// required by other module
	delQcb4NoSeeds					// no seeds for query
	delQcb4TargetInLocal			// target found in local
	delQcb4QryInstDoneInd			// query instance done is indicated
	delQcb4QryInstResultInd			// query instance result indicated
	delQcb4InteralErrors			// internal errors while tring to query
)

func (qryMgr *QryMgr)qryMgrDelQcb(why int, target config.DsKey) DhtErrno {

	event := sch.EvSchPoweroff

	switch why {

	case delQcb4TargetFound:
	case delQcb4NoMoreQueries:
	case delQcb4Timeout:
	case delQcb4NoSeeds:
	case delQcb4TargetInLocal:
	case delQcb4QryInstDoneInd:
	case delQcb4InteralErrors:

		//
		// nothing for above cases, see bellow outside of this "switch".
		//

	case delQcb4Command:
		event = sch.EvDhtQryInstStopReq

	default:
		qryLog.Debug("qryMgrDelQcb: parameters mismatched, why: %d", why)
		return DhtEnoMismatched
	}

	qcb, ok := qryMgr.qcbTab[target]
	if !ok {
		qryLog.Debug("qryMgrDelQcb: target not found: %x", target)
		return DhtEnoNotFound
	}

	if qcb.status != qsInited {
		delete(qryMgr.qcbTab, target)
		return DhtEnoNone
	}

	//
	// when we try to delete the query instance for external module command (why == delQcb4Command),
	// we send request to each instance to ask them to stop and send response to us, so we do not
	// release the query control block in this case.
	//

	if event == sch.EvDhtQryInstStopReq {
		for _, icb := range qcb.qryActived {
			req := sch.SchMessage{}
			body := sch.MsgDhtQryInstStopReq{
				Target: target,
				Peer: icb.to.ID,
				Eno: DhtEnoNone.GetEno(),
			}
			icb.sdl.SchMakeMessage(&req, qryMgr.ptnMe, icb.ptnInst, sch.EvDhtQryInstStopReq, &body)
			icb.sdl.SchSendMessage(&req)
		}
		return DhtEnoNone
	}

	//
	// other cases than a command, we send power off message to each instance, so they would kill
	// themself, no responses for us, we free the query control block here.
	//

	if qcb.qryTid != sch.SchInvalidTid {
		qryMgr.sdl.SchKillTimer(qryMgr.ptnMe, qcb.qryTid)
		qcb.qryTid = sch.SchInvalidTid
	}

	if event == sch.EvSchPoweroff {
		for _, icb := range qcb.qryActived {
			po := sch.SchMessage{}
			icb.sdl.SchMakeMessage(&po, qryMgr.ptnMe, icb.ptnInst, sch.EvSchPoweroff, nil)
			icb.sdl.SchSendMessage(&po)
		}
	}

	if qcb.rutNtfFlag == true {
		var schMsg = sch.SchMessage{}
		var req = sch.MsgDhtRutMgrStopNofiyReq {
			Task:	qryMgr.ptnMe,
			Target:	qcb.target,
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrStopNotifyReq, &req)
		qryMgr.sdl.SchSendMessage(&schMsg)
	}

	delete(qryMgr.qcbTab, target)

	return DhtEnoNone
}

//
// Delete query instance control block
//
func (qryMgr *QryMgr)qryMgrDelIcb(why int, target *config.DsKey, peer *config.NodeID) DhtErrno {

	if why != delQcb4QryInstDoneInd && why != delQcb4QryInstResultInd {
		qryLog.Debug("qryMgrDelIcb: why delete?! why: %d", why)
		return DhtEnoMismatched
	}

	qcb, ok := qryMgr.qcbTab[*target]
	if !ok {
		qryLog.Debug("qryMgrDelIcb: target not found: %x", target)
		return DhtEnoNotFound
	}

	icb, ok := qcb.qryActived[*peer]
	if !ok {
		qryLog.Debug("qryMgrDelIcb: target not found: %x", target)
		return DhtEnoNotFound
	}

	if why == delQcb4QryInstResultInd {
		eno, ptn := icb.sdl.SchGetUserTaskNode(icb.name)
		if eno == sch.SchEnoNone && ptn != nil && ptn == icb.ptnInst {
			po := sch.SchMessage{}
			icb.sdl.SchMakeMessage(&po, qryMgr.ptnMe, icb.ptnInst, sch.EvSchPoweroff, nil)
			icb.sdl.SchSendMessage(&po)
		}
	}

	delete(qcb.qryActived, *peer)

	return DhtEnoNone
}

//
// Update query result of query control block
//
func (qcb *qryCtrlBlock)qcbUpdateResult(qri *qryResultInfo) DhtErrno {

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
func (qcb *qryCtrlBlock)qryMgrQcbPutPending(nodes []*qryPendingInfo, size int) DhtErrno {

	if len(nodes) == 0 || size <= 0 {
		qryLog.Debug("qryMgrQcbPutPending: no pendings to be put")
		return DhtEnoParameter
	}

	qryLog.Debug("qryMgrQcbPutPending: " +
		"number of nodes to be put: %d, size: %d", len(nodes), size)

	li := qcb.qryPending

	for _, n := range nodes {

		if _, dup := qcb.qryHistory[n.node.ID]; dup {
			qryLog.Debug("qryMgrQcbPutPending: duplicated, n: %+v", n)
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
			qryLog.Debug("qryMgrQcbPutPending: PushBack, n: %+v", n)
			li.PushBack(n)
		}
	}

	for li.Len() > size {
		li.Remove(li.Back())
	}

	return DhtEnoNone
}

//
// Put node to actived queue and start query to the node
//
func (qryMgr *QryMgr)qryMgrQcbPutActived(qcb *qryCtrlBlock) (DhtErrno, int) {

	if qcb == nil {
		qryLog.Debug("qryMgrQcbPutActived: invalid parameter")
		return DhtEnoParameter, 0
	}

	if qcb.qryPending == nil || qcb.qryPending.Len() == 0 {
		qryLog.Debug("qryMgrQcbPutActived: no pending")
		return DhtEnoNotFound, 0
	}

	if len(qcb.qryActived) == qryMgr.qmCfg.maxActInsts {
		qryLog.Debug("qryMgrQcbPutActived: no room")
		return DhtEnoResource, 0
	}

	act := make([]*list.Element, 0)
	cnt := 0
	dhtEno := DhtEnoNone

	for el := qcb.qryPending.Front(); el != nil; el = el.Next() {

		if len(qcb.qryActived) >= qryMgr.qmCfg.maxActInsts {
			break
		}

		pending := el.Value.(*qryPendingInfo)
		act = append(act, el)

		if _, dup := qcb.qryActived[pending.node.ID]; dup == true {
			qryLog.Debug("qryMgrQcbPutActived: duplicated node: %X", pending.node.ID)
			continue
		}

		qryLog.Debug("qryMgrQcbPutActived: pending to be activated: %+v", *pending)

		icb := qryInstCtrlBlock {
			sdl:		qryMgr.sdl,
			seq:		qcb.icbSeq,
			qryReq:		qcb.qryReq,
			name:		"qryMgrIcb" + fmt.Sprintf("_q%d_i%d", qcb.seq, qcb.icbSeq),
			ptnInst:	nil,
			ptnConMgr:	nil,
			ptnRutMgr:	nil,
			ptnQryMgr:	nil,
			local:		qryMgr.qmCfg.local,
			status:		qisNull,
			target:		qcb.target,
			to:			pending.node,
			dir:		ConInstDirUnknown,
			qTid:		sch.SchInvalidTid,
			begTime:	time.Time{},
			endTime:	time.Time{},
			conBegTime:	time.Time{},
			conEndTime:	time.Time{},
		}

		qryLog.Debug("qryMgrQcbPutActived: icb: %+v", icb)

		td := sch.SchTaskDescription{
			Name:		icb.name,
			MbSize:		sch.SchMaxMbSize,
			Ep:			NewQryInst(),
			Wd:			&sch.SchWatchDog{HaveDog:false,},
			Flag:		sch.SchCreatedGo,
			DieCb:		nil,
			UserDa:		&icb,
		}

		qcb.qryActived[icb.to.ID] = &icb
		qcb.qryHistory[icb.to.ID] = pending
		cnt++

		eno, ptn := qryMgr.sdl.SchCreateTask(&td)
		if eno != sch.SchEnoNone || ptn == nil {

			qryLog.Debug("qryMgrQcbPutActived: " +
				"SchCreateTask failed, eno: %d, ptn: %p", eno, ptn)

			dhtEno = DhtEnoScheduler
			break
		}
		icb.ptnInst = ptn
		qcb.icbSeq++

		po := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&po, qryMgr.ptnMe, icb.ptnInst, sch.EvSchPoweron, nil)
		qryMgr.sdl.SchSendMessage(&po)

		start := sch.SchMessage{}
		qryMgr.sdl.SchMakeMessage(&start, qryMgr.ptnMe, icb.ptnInst, sch.EvDhtQryInstStartReq, nil)
		qryMgr.sdl.SchSendMessage(&start)
	}

	for _, el := range act {
		qcb.qryPending.Remove(el)
	}

	qryLog.Debug("qryMgrQcbPutActived: " +
		"pending: %d, actived: %d, history: %d",
		qcb.qryPending.Len(), len(qcb.qryActived), len(qcb.qryHistory))

	return DhtErrno(dhtEno), cnt
}

//
// Start timer for query control block
//
func (qryMgr *QryMgr)qryMgrQcbStartTimer(qcb *qryCtrlBlock) DhtErrno {

	var td = sch.TimerDescription {
		Name:	"qryMgrQcbTimer" + fmt.Sprintf("%d", qcb.seq),
		Utid:	sch.DhtQryMgrQcbTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	qryMgr.qmCfg.qryExpired,
		Extra:	qcb,
	}

	tid := sch.SchInvalidTid
	eno := sch.SchEnoUnknown

	if eno, tid = qryMgr.sdl.SchSetTimer(qryMgr.ptnMe, &td);
		eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		qryLog.Debug("qryMgrQcbStartTimer: SchSetTimer failed, eno: %d", eno)
		return DhtEnoScheduler
	}

	qcb.qryTid = tid

	return DhtEnoNone
}

//
// Query control block timer handler
//
func (qryMgr *QryMgr)qcbTimerHandler(qcb *qryCtrlBlock) sch.SchErrno {

	if qcb == nil {
		return sch.SchEnoParameter
	}

	qryMgr.qryMgrResultReport(qcb, DhtEnoTimeout.GetEno(), nil, nil, nil)
	qryMgr.qryMgrDelQcb(delQcb4Timeout, qcb.target)

	return sch.SchEnoNone
}

//
// Query result report
//
func (qryMgr *QryMgr)qryMgrResultReport(
	qcb		*qryCtrlBlock,
	eno		int,
	peer	*config.Node,
	val		[]byte,
	prd		*sch.Provider) DhtErrno {

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

	var msg = sch.SchMessage{}
	var ind = sch.MsgDhtQryMgrQueryResultInd{
		Eno:		eno,
		ForWhat:	qcb.forWhat,
		Target:		qcb.target,
		Val:		val,
		Prds:		nil,
		Peers:		nil,
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

	qryMgr.sdl.SchMakeMessage(&msg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
	qryMgr.sdl.SchSendMessage(&msg)

	return DhtEnoNone
}
