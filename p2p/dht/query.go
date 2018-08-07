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
	"container/list"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
	log "github.com/yeeco/gyee/p2p/logger"
)

//
// Constants
//
const (
	QryMgrName = sch.DhtQryMgrName		// query manage name registered in shceduler
	qryMgrMaxPendings = 32				// max pendings can be held in the list
	qryMgrMaxActInsts = 8				// max concurrent actived instances for one query
	qryMgrQryExpired = time.Second * 60	// duration to get expired for a query
	qryInstExpired = time.Second * 16	// duration to get expired for a query instance
)

//
// Query manager configuration
//
type qryMgrCfg struct {
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
type qryPendingInfo = rutMgrBucketNode

//
// Query control block
//
type qryCtrlBlock struct {
	ptnOwner	interface{}								// owner task node pointer
	seq			int										// sequence number
	target		config.NodeID							// target is looking up
	status		QryStatus								// query status
	qryHistory	map[config.NodeID]*qryPendingInfo		// history peers had been queried
	qryPending	*list.List								// pending peers to be queried, with type qryPendingInfo
	qryActived	map[config.NodeID]*qryInstCtrlBlock		// queries activated
	qryResult	*list.List								// list of qryResultNodeInfo type object
	qryTid		int										// query timer identity
	icbSeq		int										// query instance control block sequence number
	rutNtfFlag	bool									// if notification asked for
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
	sdl			*sch.Scheduler		// pointer to scheduler
	seq			int					// sequence number
	name		string				// instance name
	ptnInst		interface{}			// pointer to query instance task node
	ptnConMgr	interface{}			// pointer to connection manager task node
	ptnQryMgr	interface{}			// pointer to query manager task node
	status		int					// instance status
	target		config.NodeID		// target is looking up
	to			config.Node			// to whom the query message sent
	qTid		int					// query timer identity
	begTime		time.Time			// query begin time
	endTime		time.Time			// query end time
	conBegTime	time.Time			// time to start connection
	conEndTime	time.Time			// time connection established
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
		sdl:		nil,
		name:		QryMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnDhtMgr:	nil,
		instSeq:	0,
		qcbTab:		map[config.NodeID]*qryCtrlBlock{},
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

	//
	// set "NtfReq" to be true to tell route manager that we need notifications,
	// see handler about this event handler in route.go pls.
	//

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
	qcb.seq = qryMgr.qcbSeq
	qryMgr.qcbSeq++
	qcb.icbSeq = 0
	qcb.target = target
	qcb.status = qsNull
	qcb.qryHistory = make(map[config.NodeID]*rutMgrBucketNode, 0)
	qcb.qryPending = nil
	qcb.qryActived = make(map[config.NodeID]*qryInstCtrlBlock, qryMgr.qmCfg.maxActInsts)
	qcb.qryResult = nil
	qcb.rutNtfFlag = nearestReq.NtfReq

	qryMgr.sdl.SchMakeMessage(&schMsg, qryMgr.ptnMe, qryMgr.ptnRutMgr, sch.EvDhtRutMgrNearestReq, &nearestReq)
	qryMgr.sdl.SchSendMessage(&schMsg)
	qcb.status = qsPreparing

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
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoNone
	}

	if len(peers) == 0 {
		qryFailed2Sender(DhtEnoRoute)
		qryMgr.qryMgrDelQcb(delQcb4NoSeeds, target)
		return sch.SchEnoNone
	}

	//
	// check if target found in local while updating the query result by the
	// nearests reported.
	//

	for idx, peer := range peers {

		if peer.node.ID == target {
			qryOk2Sender(&peer.node)
			qryMgr.qryMgrDelQcb(delQcb4TargetInLocal, target)
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

	if dhtEno = qcb.qryMgrQcbPutPending(peers, qryMgr.qmCfg.maxPendings); dhtEno == DhtEnoNone {

		if dhtEno, _ = qryMgr.qryMgrQcbPutActived(qcb); dhtEno == DhtEnoNone {

			if dhtEno = qryMgr.qryMgrQcbStartTimer(qcb); dhtEno == DhtEnoNone {

				qcb.status = qsInited

				return sch.SchEnoNone
			}
		}
	}

	log.LogCallerFileLine("rutNearestRsp: failed, dhtEno: %d", dhtEno)

	qryFailed2Sender(dhtEno)
	qryMgr.qryMgrDelQcb(delQcb4InteralErrors, target)

	return sch.SchEnoUserTask
}

//
// Query stop request handler
//
func (qryMgr *QryMgr)queryStopReq(sender interface{}, msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {

	target := msg.Target
	rsp := sch.MsgDhtQryMgrQueryStopRsp{Target:target, Eno:DhtEnoNone}

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
		log.LogCallerFileLine("rutNotificationInd: target not found: %x", target)
		return sch.SchEnoParameter
	}

	if qcb.status != qsInited {
		log.LogCallerFileLine("rutNotificationInd: query not inited yet for target: %s", target)
		return sch.SchEnoUserTask
	}

	qpi := msg.Peers.([]*qryPendingInfo)
	qcb.qryMgrQcbPutPending(qpi, qryMgr.qmCfg.maxPendings)

	//
	// try to active more instances since pendings added
	//

	qryMgr.qryMgrQcbPutActived(qcb)

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		log.LogCallerFileLine("rutNotificationInd: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// check if query should be end
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if dhtEno := qryMgr.qryMgrResultReport(qcb); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("rutNotificationInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("rutNotificationInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (qryMgr *QryMgr)instStatusInd(msg *sch.MsgDhtQryInstStatusInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance query result indication handler
//
func (qryMgr *QryMgr)instResultInd(msg *sch.MsgDhtQryInstResultInd) sch.SchErrno {

	var qcb *qryCtrlBlock= nil
	var qpiList = []*qryPendingInfo{}
	var hashList = []*Hash{}
	var distList = []int{}
	var rutMgr = qryMgr.sdl.SchGetUserTaskIF(RutMgrName).(*RutMgr)

	if len(msg.Peers) != len(msg.Pcs) {
		log.LogCallerFileLine("instResultInd: mismatched Peers and Pcs")
		return sch.SchEnoMismatched
	}

	if rutMgr == nil {
		log.LogCallerFileLine("instResultInd: nil route manager")
		return sch.SchEnoInternal
	}

	//
	// update route manager in any cases
	//

	from := msg.From
	latency	:= msg.Latency

	updateReq2RutMgr := func (peer *config.Node, dur time.Duration) sch.SchErrno {
		var schMsg = sch.SchMessage{}
		var updateReq = sch.MsgDhtRutMgrUpdateReq{
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

	target := msg.Target
	if qcb = qryMgr.qcbTab[target]; qcb == nil {
		log.LogCallerFileLine("instResultInd: not found, target: %x", target)
		return sch.SchEnoUserTask
	}

	for _, peer := range msg.Peers {
		if peer.ID == target {
			qryMgr.qryMgrResultReport(qcb)
			if dhtEno := qryMgr.qryMgrDelQcb(delQcb4TargetFound, qcb.target); dhtEno != DhtEnoNone {
				return sch.SchEnoUserTask
			}
			return sch.SchEnoNone
		}
	}

	//
	// put peers reported in the message into query control block pending queue,
	// and try to active more query instance after that.
	//

	for idx, peer := range msg.Peers {
		var qpi = qryPendingInfo {
			node: *peer,
			hash: *hashList[idx],
			dist: distList[idx],
		}
		qpiList = append(qpiList, &qpi)
	}

	qcb.qryMgrQcbPutPending(qpiList, qryMgr.qmCfg.maxPendings)
	qryMgr.qryMgrQcbPutActived(qcb)

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		log.LogCallerFileLine("instResultInd: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// if pending queue and actived queue all are empty, we just report query
	// result and end the query.
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if dhtEno := qryMgr.qryMgrResultReport(qcb); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("instResultInd: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("instResultInd: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
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
		log.LogCallerFileLine("instStopRsp: target not found: %x", target)
		return sch.SchEnoUserTask
	}

	//
	// done the instancce task. Since here the instance tells it's stopped, we
	// need not to send power off event, we done it directly.
	//

	if icb = qcb.qryActived[to]; icb == nil {
		log.LogCallerFileLine("instStopRsp: instance not found: %x", to)
		icb.sdl.SchTaskDone(icb.ptnInst, sch.SchEnoKilled)
		return sch.SchEnoUserTask
	}

	delete(qcb.qryActived, to)

	//
	// check against abnormal cases
	//

	if qcb.qryPending.Len() > 0 && len(qcb.qryActived) < qryMgr.qmCfg.maxActInsts {
		log.LogCallerFileLine("instStopRsp: internal errors")
		return sch.SchEnoUserTask
	}

	//
	// chcek if some activateds and pendings, if none of them, the query is over
	// then, we can report the result about this query and free it.
	//

	if qcb.qryPending.Len() == 0 && len(qcb.qryActived) == 0{

		if dhtEno := qryMgr.qryMgrResultReport(qcb); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("instStopRsp: qryMgrResultReport failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		if dhtEno := qryMgr.qryMgrDelQcb(delQcb4NoMoreQueries, qcb.target); dhtEno != DhtEnoNone {
			log.LogCallerFileLine("instStopRsp: qryMgrDelQcb failed, dhtEno: %d", dhtEno)
			return sch.SchEnoUserTask
		}

		return sch.SchEnoNone
	}

	//
	// here we try active more instances for one had been removed
	//

	if dhtEno, _ := qryMgr.qryMgrQcbPutActived(qcb); dhtEno == DhtEnoNone {
		return sch.SchEnoNone
	}

	return sch.SchEnoUserTask
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
const (
	delQcb4TargetFound	= iota		// target had been found
	delQcb4NoMoreQueries			// no pendings and no actived instances
	delQcb4Timeout					// query manager time out for the control block
	delQcb4Command					// required by other module
	delQcb4NoSeeds					// no seeds for query
	delQcb4TargetInLocal			// target found in local
	delQcb4InteralErrors			// internal errors while tring to query
)

func (qryMgr *QryMgr)qryMgrDelQcb(why int, target config.NodeID) DhtErrno {

	event := sch.EvSchPoweroff

	switch why {
	case delQcb4TargetFound:
	case delQcb4NoMoreQueries:
	case delQcb4Timeout:
	case delQcb4NoSeeds:
	case delQcb4TargetInLocal:
	case delQcb4InteralErrors:

	case delQcb4Command:
		event = sch.EvDhtQryInstStopReq

	default:
		log.LogCallerFileLine("qryMgrDelQcb: parameters mismatched, why: %d", why)
		return DhtEnoMismatched
	}

	qcb, ok := qryMgr.qcbTab[target]
	if !ok {
		log.LogCallerFileLine("qryMgrDelQcb: target not found: %x", target)
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
				Eno: DhtEnoNone,
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
func (qcb *qryCtrlBlock)qryMgrQcbPutPending(nodes []*qryPendingInfo, size int) DhtErrno {

	li := qcb.qryPending

	for _, n := range nodes {

		if _, dup := qcb.qryHistory[n.node.ID]; dup {
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

		if pb { li.PushBack(n) }
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

	if qcb.qryPending == nil || qcb.qryPending.Len() == 0 {
		log.LogCallerFileLine("qryMgrQcbPutActived: no pending")
		return DhtEnoNotFound, 0
	}

	if len(qcb.qryActived) == qryMgr.qmCfg.maxActInsts {
		log.LogCallerFileLine("qryMgrQcbPutActived: no room")
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
			log.LogCallerFileLine("qryMgrQcbPutActived: duplicated node: %X", pending.node.ID)
			continue
		}

		icb := qryInstCtrlBlock {
			sdl:		qryMgr.sdl,
			seq:		qcb.icbSeq,
			name:		"qryMgrIcb" + fmt.Sprintf("%d", qcb.icbSeq),
			ptnInst:	nil,
			ptnConMgr:	nil,
			ptnQryMgr:	qryMgr,
			status:		qisNull,
			target:		qcb.target,
			to:			pending.node,
			qTid:		sch.SchInvalidTid,
			begTime:	time.Time{},
			endTime:	time.Time{},
			conBegTime:	time.Time{},
			conEndTime:	time.Time{},
		}

		td := sch.SchTaskDescription{
			Name:		icb.name,
			MbSize:		-1,
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

			log.LogCallerFileLine("qryMgrQcbPutActived: " +
				"SchCreateTask failed, eno: %d, ptn: %p",
				eno, ptn)

			dhtEno = DhtEnoScheduler
			break;
		}
		icb.ptnInst = ptn

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
	qryMgr.qryMgrDelQcb(delQcb4Timeout, qcb.target)

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
