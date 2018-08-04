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
	"container/list"
)

//
// Constants
//
const (
	QryMgrName = sch.DhtQryMgrName		// query manage name registered in shceduler
	qryMgrMaxActInsts = 8				// max concurrent actived instances for one query
	qryMgrQryExpired = time.Second * 60	// duration to get expired for a query
	qryInstExpired = time.Second * 16	// duration to get expired for a query instance
)

//
// Query manager configuration
//
type qryMgrCfg struct {
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
type qryPendingInfo = rutMgrBucketNode

//
// Query control block
//
type qryCtrlBlock struct {
	ptnOwner	interface{}								// owner task node pointer
	target		config.NodeID							// target is looking up
	status		QryStatus								// query status
	qryHist		map[config.NodeID]*rutMgrBucketNode		// history peers had been queried
	qryPending	*list.List								// pending peers to be queried, with type qryPendingInfo
	qryActived	map[config.NodeID]*qryInstCtrlBlock		// queries activated
	qryResult	*list.List								// list of qryResultNodeInfo type object
	qryTid		int										// query timer identity
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
	qmCfg		qryMgrCfg						// query manager configuration
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qmCfg := qryMgrCfg {
		maxActInsts:	qryMgrMaxActInsts,
		qryExpired:		qryMgrQryExpired,
		qryInstExpired:	qryInstExpired,
	}

	qryMgr := QryMgr{
		sdl:		nil,
		name:		QryMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnDhtMgr:	nil,
		instSeq:	0,
		qcbTab:		map[config.NodeID]*qryCtrlBlock{},
		qmCfg:		qmCfg,
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

	case sch.EvDhtQryMgrQcbTimer:
		qcb := msg.Body.(*qryCtrlBlock)
		eno = qryMgr.qcbTimerHandler(qcb)

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
	qcb.qryPending = nil
	qcb.qryActived = make(map[config.NodeID]*qryInstCtrlBlock, qryMgr.qmCfg.maxActInsts)
	qcb.qryResult = nil

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

	var dhtEno = DhtErrno(DhtEnoNone)

	target := msg.Target
	peers := msg.Peers.([]*rutMgrBucketNode)
	dists := msg.Dists.([]int)
	qcb, ok := qryMgr.qcbTab[target]

	if !ok {
		log.LogCallerFileLine("rutNearestRsp: qcb not exist, target: %x", target)
		return sch.SchEnoNotFound
	}

	if qcb == nil {
		log.LogCallerFileLine("rutNearestRsp: nil qcb, target: %x", target)
		return sch.SchEnoInternal
	}

	if qcb.status != qsPreparing {
		log.LogCallerFileLine("rutNearestRsp: qcb status mismatched, status: %d, target: %x",
			qcb.status, target)
		return sch.SchEnoMismatched
	}

	qryFailed2Sender := func (eno DhtErrno) {
		var schMsg = sch.SchMessage{}
		var rsp = sch.MsgDhtQryMgrQueryStartRsp{
			Target:	msg.Target,
			Eno:	int(eno),
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryStartRsp, &rsp)
		qryMgr.sdl.SchSendMessage(&schMsg)
	}

	qryOk2Sender := func(peer *config.Node) {
		var schMsg = sch.SchMessage{}
		var ind = sch.MsgDhtQryMgrQueryResultInd{
			Eno:	DhtEnoNone,
			Target:	target,
			Peers:	[]*config.Node{peer},
		}
		qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qcb.ptnOwner, sch.EvDhtQryMgrQueryResultInd, &ind)
		qryMgr.sdl.SchSendMessage(&schMsg)
	}

	if msg.Eno != DhtEnoNone {
		qryFailed2Sender(DhtErrno(msg.Eno))
		qryMgr.qryMgrDelQcb(target)
		return sch.SchEnoNone
	}

	if len(peers) == 0 {
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(target)
		return sch.SchEnoNone
	}

	//
	// check if target found in local while updating the query result by the
	// nearests reported.
	//

	for idx, peer := range peers {

		if peer.node.ID == target {
			qryOk2Sender(&peer.node)
			qryMgr.qryMgrDelQcb(target)
			return sch.SchEnoNone
		}

		qri := qryResultInfo {
			node:	peer.node,
			pcs:	pcsConnYes,
			dist:	dists[idx],
		}

		qcb.qcbUpdateResult(&qri)
	}

	//
	// start queries by putting nearests to pending queue and then putting
	// pending nodes to be activated.
	//

	qcb.qryPending = list.New()
	qcb.qryResult = list.New()

	if dhtEno = qcb.qryMgrQcbPutPending(peers); dhtEno == DhtEnoNone {
		if dhtEno = qryMgr.qryMgrQcbPutActived(qcb); dhtEno == DhtEnoNone {
			if dhtEno = qryMgr.qryMgrQcbStartTimer(qcb); dhtEno == DhtEnoNone {
				qcb.status = qsInited
				return sch.SchEnoNone
			}
		}
	}

	log.LogCallerFileLine("rutNearestRsp: failed, dhtEno: %d", dhtEno)
	qryFailed2Sender(dhtEno)
	qryMgr.qryMgrDelQcb(target)

	return sch.SchEnoUserTask
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

	cfg := config.P2pConfig4DhtQryManager(qryMgr.sdl.SchGetP2pCfgName())

	qmCfg := &qryMgr.qmCfg
	qmCfg.maxActInsts = cfg.MaxActInsts
	qmCfg.qryExpired = cfg.QryExpired
	qmCfg.qryInstExpired = cfg.QryInstExpired

	return DhtEnoNone
}

//
// Delete query control blcok from manager
//
func (qryMgr *QryMgr)qryMgrDelQcb(target config.NodeID) DhtErrno {

	qcb, ok := qryMgr.qcbTab[target]
	if !ok { return DhtEnoNotFound }

	if qcb.status != qsInited {
		delete(qryMgr.qcbTab, target)
		return DhtEnoNone
	}

	if qcb.qryTid != sch.SchInvalidTid {
		qryMgr.sdl.SchKillTimer(qryMgr.ptnMe, qcb.qryTid)
		qcb.qryTid = sch.SchInvalidTid
	}

	for _, qicb := range qcb.qryActived {
		qryMgr.sdl.SchTaskDone(qicb.ptnInst, sch.SchEnoKilled)
	}

	delete(qryMgr.qcbTab, target)
	return DhtEnoNone
}

//
// Update query result info of query control block
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
func (qcb *qryCtrlBlock)qryMgrQcbPutPending(nodes []*qryPendingInfo) DhtErrno {

	li := qcb.qryPending

	for _, n := range nodes {
		for el := li.Front(); el != nil; el = el.Next() {
			v := el.Value.(*qryPendingInfo)
			if n.dist < v.dist {
				li.InsertBefore(n, el)
				break
			}
		}
		li.PushBack(n)
	}

	return DhtEnoNone
}

//
// Put node to actived queue and start query to the node
//
func (qryMgr *QryMgr)qryMgrQcbPutActived(qcb *qryCtrlBlock) DhtErrno {
	return DhtEnoNone
}

//
// Start timer for query control block
//
func (qryMgr *QryMgr)qryMgrQcbStartTimer(qcb *qryCtrlBlock) DhtErrno {

	var td = sch.TimerDescription {
		Name:	"dhtQyrMgrQcbTimer",
		Utid:	sch.DhtQryMgrQcbTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	qryMgr.qmCfg.qryExpired,
		Extra:	qcb,
	}

	tid := sch.SchInvalidTid
	eno := sch.SchEnoUnknown

	if eno, tid = qryMgr.sdl.SchSetTimer(qryMgr.ptnMe, &td);
		eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.LogCallerFileLine("qryMgrQcbStartTimer: SchSetTimer failed, eno: %d", eno)
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

	qryMgr.qryMgrResultReport(qcb)
	qryMgr.qryMgrDelQcb(qcb.target)

	return sch.SchEnoNone
}

//
// Query result report
//
func (qryMgr *QryMgr)qryMgrResultReport(qcb *qryCtrlBlock) DhtErrno {

	var msg = sch.SchMessage{}
	var ind = sch.MsgDhtQryMgrQueryResultInd{
		Eno:	DhtEnoTimeout,
		Target:	qcb.target,
		Peers:	nil,
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
