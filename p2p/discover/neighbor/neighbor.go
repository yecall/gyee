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

package neighbor

import (
	"net"
	"sync"
	"time"
	"fmt"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	config	"github.com/yeeco/gyee/p2p/config"
	log		"github.com/yeeco/gyee/p2p/logger"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
)


//
// errno
//
const (
	NgbMgrEnoNone	= iota
	NgbMgrEnoParameter
	NgbMgrEnoTimeout
	NgbMgrEnoNotImpl
	NgbMgrEnoEncode
	NgbMgrEnoUdp
	NgbMgrEnoDuplicated
	NgbMgrEnoMismatched
	NgbMgrEnoScheduler
	NgbMgrEnoTable
)

type NgbMgrErrno int

//
// Neighbor task name: since this type of instance is created dynamic, no fixed name defined,
// instead, peer node id string is applied as the task name, and this is prefixxed, Please see
// how this type of task instance is created for more.
//
const NgbProcName = "ngbproto"

//
// Mailbox size of a ngighbor instance
//
const ngbProcMailboxSize = 8

//
// The control block of neighbor task instance
//
type neighborInst struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	ngbMgr	*NeighborManager	// pointer to neighbor manager
	ptn		interface{}			// task node pointer
	name	string				// task instance name
	tep		sch.SchUserTaskEp	// entry
	msgType	um.UdpMsgType		// message type to inited this instance
	msgBody	interface{}			// message body
	tidFN	int					// FindNode timer identity
	tidPP	int					// Pingpong timer identity
}

//
// Protocol handler errno
//
const (
	NgbProtoEnoNone	= 0
	NgbProtoEnoParameter = iota + 100	// +100, an offset is necessary to distinct this errno from
										// those NgbMgrEnoxxx.
	NgbProtoEnoScheduler
	NgbProtoEnoOs
	NgbProtoEnoEncode
	NgbProtoEnoTimeout
	NgbProtoEnoUdp
)

type NgbProtoErrno int

//
// Timeouts, zero value would be no timeout
//
const (
	NgbProtoWriteTimeout			= 8 * time.Second
	NgbProtoReadTimeout				= 0
	NgbProtoPingResponseTimeout		= 20 * time.Second
	NgbProtoFindNodeResponseTimeout = 20 * time.Second
)

//
// Entry point exported to shceduler
//
func (inst *neighborInst)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return inst.tep(ptn, msg)
}

//
// Neighbor task entry
//
func (inst *neighborInst)ngbProtoProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var protoEno NgbProtoErrno

	switch msg.Id {

	case sch.EvNblFindNodeReq:
		protoEno = inst.NgbProtoFindNodeReq(ptn, msg.Body.(*um.FindNode))

	case sch.EvNblPingpongReq:
		protoEno = inst.NgbProtoPingReq(ptn, msg.Body.(*um.Ping))

	case sch.EvNblPingpongRsp:
		protoEno = inst.NgbProtoPingRsp(msg.Body.(*um.Pong))

	case sch.EvNblFindNodeRsp:
		protoEno = inst.NgbProtoFindNodeRsp(msg.Body.(*um.Neighbors))

	case sch.EvNblFindNodeTimer:
		protoEno = inst.NgbProtoFindNodeTimeout()

	case sch.EvNblPingpongTimer:
		protoEno = inst.NgbProtoPingTimeout()

	default:

		log.LogCallerFileLine("NgbProtoProc: " +
			"invalid message, msg.Id: %d",
			msg.Id)

		protoEno = NgbProtoEnoParameter
	}

	if protoEno != NgbProtoEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// FindNode request handler
//
func (inst *neighborInst) NgbProtoFindNodeReq(ptn interface{}, fn *um.FindNode) NgbProtoErrno {

	//
	// check FindNode request
	//

	if inst.msgType != um.UdpMsgTypeFindNode || inst.msgBody == nil {

		log.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"invalid find node request")

		return NgbProtoEnoParameter
	}

	//
	// encode request
	//

	var pum = new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypeFindNode, fn); eno != um.UdpMsgEnoNone {

		log.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"encode failed, eno: %d",
			eno)

		return NgbProtoEnoEncode
	}

	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes == 0 {

		log.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"invalid encoded  message")

		return NgbProtoEnoEncode
	}

	//
	// send encoded request
	//

	var dst = net.UDPAddr{}
	dst.IP = append(dst.IP, fn.To.IP...)
	dst.Port = int(fn.To.UDP)

	if eno := sendUdpMsg(inst.sdl, inst.ngbMgr.ptnLsn, inst.ptn, buf, &dst); eno != sch.SchEnoNone {

		//
		// response FindNode  NgbProtoEnoUdp to table task
		//

		log.LogCallerFileLine("NgbProtoFindNodeReq：" +
			"sendUdpMsg failed, dst: %s, eno: %d",
			dst.String(), eno)

		var rsp = sch.NblFindNodeRsp{}
		var schMsg  = sch.SchMessage{}

		rsp.Result = (NgbProtoEnoUdp << 16) + tab.TabMgrEnoUdp
		rsp.FindNode = inst.msgBody.(*um.FindNode)

		inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
		inst.sdl.SchSendMessage(&schMsg)

		//
		// remove ourself from map in manager. notice: since we had setup the calback
		// for task done(killed), this cleaning would carried out by scheduler, but
		// we do cleaning here to obtain a more clear seen.
		//

		inst.ngbMgr.cleanMap(inst.name)

		//
		// done the instance task
		//

		inst.sdl.SchTaskDone(inst.ptn, sch.SchEnoNone)

	 	return NgbProtoEnoUdp
	 }

	 //
	 // start timer to wait response("Neighbors" message) from peer
	 //

	 var tmd  = sch.TimerDescription {
		 Name:	NgbProcName + "_timer_findnode",
		 Utid:	sch.NblFindNodeTimerId,
		 Tmt:	sch.SchTmTypeAbsolute,
		 Dur:	NgbProtoFindNodeResponseTimeout,
		 Extra:	nil,
	 }

	 var (
	 	eno	sch.SchErrno
	 	tid int
	 )

	 if eno, tid = inst.sdl.SchSetTimer(ptn, &tmd); eno != sch.SchEnoNone {
	 	return NgbProtoEnoScheduler
	 }

	 inst.tidFN = tid

	 return NgbProtoEnoNone
}

//
// Ping request handler
//
func (inst *neighborInst) NgbProtoPingReq(ptn interface{}, ping *um.Ping) NgbProtoErrno {

	//
	// check Ping request
	//

	if inst.msgType != um.UdpMsgTypePing || inst.msgBody == nil {

		log.LogCallerFileLine("NgbProtoPingReq:" +
			" invalid ping request")

		return NgbProtoEnoParameter
	}

	//
	// encode request
	//

	var pum = new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypePing, ping); eno != um.UdpMsgEnoNone {

		log.LogCallerFileLine("NgbProtoPingReq: " +
			"encode failed, eno: %d",
			eno)

		return NgbProtoEnoEncode
	}

	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes == 0 {

		log.LogCallerFileLine("NgbProtoPingReq: " +
			"invalid encoded  message")

		return NgbProtoEnoEncode
	}

	//
	// send encoded request
	//

	var dst = net.UDPAddr{}
	dst.IP = append(dst.IP, ping.To.IP...)
	dst.Port = int(ping.To.UDP)

	if eno := sendUdpMsg(inst.sdl, inst.ngbMgr.ptnLsn, inst.ptn, buf, &dst); eno != sch.SchEnoNone {

		log.LogCallerFileLine("NgbProtoPingReq：" +
			"sendUdpMsg failed, dst: %s, eno: %d",
			dst.String(), eno)

		return NgbProtoEnoOs
	}

	//
	// start time for response
	//

	var tmd  = sch.TimerDescription {
		Name:	NgbProcName + "_timer_pingpong",
		Utid:	sch.NblPingpongTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	NgbProtoPingResponseTimeout,
		Extra:	nil,
	}

	var (
		eno	sch.SchErrno
		tid int
	)

	if eno, tid = inst.sdl.SchSetTimer(ptn, &tmd); eno != sch.SchEnoNone {
		return NgbProtoEnoScheduler
	}

	inst.tidPP = tid

	return NgbProtoEnoNone
}

//
// Ping response handler
//
func (inst *neighborInst) NgbProtoPingRsp(msg *um.Pong) NgbProtoErrno {

	//
	// Check the response, 1) if it's good, dispatch it to table task, and
	// then done this neighbor task instance; 2) if needed, kill timers in
	// running; 3) done the task, clean the map held by manager task...
	//

	// we must be a "Ping" instance
	if inst.msgType != um.UdpMsgTypePing || inst.msgBody == nil {

		log.LogCallerFileLine("NgbProtoPingRsp: " +
			"response mismatched with request")

		return NgbProtoEnoParameter
	}

	//
	// this "Pong" must be from node which we had pinged, notice that the
	// function "CompareWith" make a full matching between tow nodes.
	//

	var ping = inst.msgBody.(*um.Ping)
	if equ := ping.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {

		log.LogCallerFileLine("NgbProtoPingRsp: " +
			"node mismatched, equ: %d",
			equ)

		return NgbProtoEnoParameter
	}

	//
	// kill pingpong timer if needed
	//

	if inst.tidPP != sch.SchInvalidTid {
		if eno := inst.sdl.SchKillTimer(inst.ptn, inst.tidPP); eno != sch.SchEnoNone {
			return NgbProtoEnoScheduler
		}
		inst.tidPP = sch.SchInvalidTid
	}

	//
	// send response to table task
	//

	var rsp = sch.NblPingRsp{}
	var schMsg  = sch.SchMessage{}

	rsp.Result = NgbProtoEnoNone
	rsp.Ping = inst.msgBody.(*um.Ping)
	rsp.Pong = msg

	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	inst.ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	inst.sdl.SchTaskDone(inst.ptn, sch.SchEnoNone)

	return NgbProtoEnoNone
}

//
// Find response handler
//
func (inst *neighborInst) NgbProtoFindNodeRsp(msg *um.Neighbors) NgbProtoErrno {

	//
	// Check the response, 1) if it's good, dispatch it to table task, and
	// then done this neighbor task instance; 2) if needed, kill timers in
	// running; 3) done the task, clean the map held by manager task...
	//

	// we must be a "FindNode" instance
	if inst.msgType != um.UdpMsgTypeFindNode || inst.msgBody == nil {

		log.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"response mismatched with request")

		return NgbProtoEnoParameter
	}

	//
	// this response must be from node which we had pinged, notice that the
	// function "CompareWith" make a full matching between tow nodes.
	//

	var findNode = inst.msgBody.(*um.FindNode)
	if equ := findNode.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {

		log.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"node mismatched, equ: %d",
			equ)

		return NgbProtoEnoParameter
	}

	//
	// kill findnode timer if needed
	//

	if inst.tidFN != sch.SchInvalidTid {
		if eno := inst.sdl.SchKillTimer(inst.ptn, inst.tidFN); eno != sch.SchEnoNone {
			return NgbProtoEnoScheduler
		}
		inst.tidFN = sch.SchInvalidTid
	}

	//
	// send response to table task
	//

	var rsp = sch.NblFindNodeRsp{}
	var schMsg  = sch.SchMessage{}

	rsp.Result = (NgbProtoEnoNone << 16) + tab.TabMgrEnoNone
	rsp.FindNode = inst.msgBody.(*um.FindNode)
	rsp.Neighbors = msg

	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	inst.ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	inst.sdl.SchTaskDone(inst.ptn, sch.SchEnoNone)

	return NgbProtoEnoNone
}

//
// FindNode timeout handler
//
func (inst *neighborInst) NgbProtoFindNodeTimeout() NgbProtoErrno {

	//
	// response FindNode timeout to table task
	//

	var rsp = sch.NblFindNodeRsp{}
	var schMsg  = sch.SchMessage{}

	rsp.Result = (NgbProtoEnoTimeout << 16) + tab.TabMgrEnoTimeout
	rsp.FindNode = inst.msgBody.(*um.FindNode)

	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	inst.ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	inst.sdl.SchTaskDone(inst.ptn, sch.SchEnoNone)

	return NgbProtoEnoNone
}

//
// Ping timeout handler
//
func (inst *neighborInst) NgbProtoPingTimeout() NgbProtoErrno {

	//
	// response Ping timeout to table task
	//

	var rsp = sch.NblPingRsp{}
	var schMsg  = sch.SchMessage{}

	rsp.Result = NgbProtoEnoTimeout
	rsp.Ping = inst.msgBody.(*um.Ping)

	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	inst.ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	inst.sdl.SchTaskDone(inst.ptn, sch.SchEnoNone)

	return NgbProtoEnoNone
}

//
// Callbacked when died
//
func (ni *neighborInst) NgbProtoDieCb(ptn interface{}) sch.SchErrno {

	//
	// kill any timer if needed, should not care the result returned from
	// scheduler, for timer might have been killed by scheduler.
	//

	if ni.tidPP != sch.SchInvalidTid {
		ni.sdl.SchKillTimer(ni.ptn, ni.tidPP)
		ni.tidPP = sch.SchInvalidTid
	}

	if ni.tidFN != sch.SchInvalidTid {
		ni.sdl.SchKillTimer(ni.ptn, ni.tidFN)
		ni.tidFN = sch.SchInvalidTid
	}

	//
	// clean the map
	//

	ni.ngbMgr.cleanMap(ni.name)

	//
	// any more ...?
	//

	return sch.SchEnoNone
}

//
// Neighbor manager task name
//
const NgbMgrName = sch.NgbMgrName

//
// Timeouts
//
const (
	expiration  = 20 * time.Second
)

//
// Control block of neighbor manager task
//
type NeighborManager struct {
	cfg			config.Cfg4UdpNgbManager		// configuration
	lock		sync.Mutex					// lock for protection
	sdl			*sch.Scheduler				// pointer to scheduler
	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	ptnMe		interface{}					// pointer to task node of myself
	ptnTab		interface{}					// pointer to task node of table task
	tabMgr		*tab.TableManager			// pointer to table manager
	ptnLsn		interface{}					// pointer to task node of listner
	ngbMap		map[string]*neighborInst	// map neighbor node id to task node pointer
	fnInstSeq	int							// findnode instance sequence number
	ppInstSeq	int							// pingpong instance sequence number
}

//
// Create neighbor manager
//
func NewNgbMgr() *NeighborManager {
	var ngbMgr = NeighborManager{
		sdl:       nil,
		name:      NgbMgrName,
		tep:       nil,
		ptnMe:     nil,
		ptnTab:    nil,
		ngbMap:    make(map[string]*neighborInst),
		fnInstSeq: 0,
		ppInstSeq: 0,
	}

	ngbMgr.tep = ngbMgr.ngbMgrProc

	return &ngbMgr
}

//
// Entry point exported to shceduler
//
func (ngbMgr *NeighborManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return ngbMgr.tep(ptn, msg)
}

//
// Neighbor manager task entry
//
func (ngbMgr *NeighborManager)ngbMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	//
	// Messages are from udp listener task or table task. The former would dispatch udpmsgs
	// (which are decoded from raw protobuf messages received by UDP); the later, would request
	// us to init Ping procedure or FindNode procedure. By checking the sch.SchMessage.Id we
	// can determine what the current message for. Here, we play game like following:
	//
	// 1) for reqest from table task, the manager create a neighbor task to handle it, and
	// backups the peer node identity in a map;
	// 2) for udpmsg indication messages which could is not in the map, the manager response
	// them at once;
	// 3) for udpmsg indication messages which are from those peer nodes in the map, the manager
	// dispatch them to the neighbor task instances created in step 1);
	// 4) the neighbor task should install a callback to clean itself from the map when its'
	// instance done;
	//

	var eno NgbMgrErrno

	switch msg.Id {

	case sch.EvSchPoweron:
		return ngbMgr.PoweronHandler(ptn)

	case sch.EvSchPoweroff:
		return ngbMgr.PoweroffHandler(ptn)

	// udpmsg from udp listener task
	case sch.EvNblMsgInd:
		eno = ngbMgr.UdpMsgInd(msg.Body.(*UdpMsgInd))

	// request to find node from table task
	case sch.EvNblFindNodeReq:
		eno = ngbMgr.FindNodeReq(msg.Body.(*um.FindNode))

	// request to ping from table task
	case sch.EvNblPingpongReq:
		eno = ngbMgr.PingpongReq(msg.Body.(*um.Ping))

	default:

		log.LogCallerFileLine("NgbMgrProc: " +
			"invalid message id: %d",
			msg.Id)

		eno = NgbMgrEnoParameter
	}

	if eno != NgbMgrEnoNone {

		log.LogCallerFileLine("NgbMgrProc: " +
			"errors, eno: %d",
			eno)

		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweron handler
//
func (ngbMgr *NeighborManager)PoweronHandler(ptn interface{}) sch.SchErrno {

	var eno sch.SchErrno
	var ptnTab interface{}
	var ptnLsn interface{}

	ngbMgr.ptnMe = ptn
	ngbMgr.sdl = sch.SchGetScheduler(ptn)
	ngbMgr.tabMgr = ngbMgr.sdl.SchGetUserTaskIF(sch.TabMgrName).(*tab.TableManager)
	eno, ptnTab = ngbMgr.sdl.SchGetTaskNodeByName(sch.TabMgrName)
	eno, ptnLsn = ngbMgr.sdl.SchGetTaskNodeByName(sch.NgbLsnName)


	if eno = ngbMgr.setupConfig(); eno != sch.SchEnoNone {
		log.LogCallerFileLine("PoweronHandler: " +
			"setupConfig failed, eno: %d",
			eno)
		return eno
	}

	if ptnTab == nil {
		log.LogCallerFileLine("PoweronHandler: " +
			"invalid table task node pointer")
		return sch.SchEnoUnknown
	}

	ngbMgr.ptnMe = ptn
	ngbMgr.ptnTab = ptnTab
	ngbMgr.ptnLsn = ptnLsn

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (ngbMgr *NeighborManager)PoweroffHandler(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("PoweroffHandler: done for poweroff event")
	return ngbMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}

//
// udpmsg handler
//
func (ngbMgr *NeighborManager)UdpMsgInd(msg *UdpMsgInd) NgbMgrErrno {

	var eno NgbMgrErrno

	switch msg.msgType {

	case um.UdpMsgTypePing:
		eno = ngbMgr.PingHandler(msg.msgBody.(*um.Ping))

	case um.UdpMsgTypePong:
		eno = ngbMgr.PongHandler(msg.msgBody.(*um.Pong))

	case um.UdpMsgTypeFindNode:
		eno = ngbMgr.FindNodeHandler(msg.msgBody.(*um.FindNode))

	case um.UdpMsgTypeNeighbors:
		eno = ngbMgr.NeighborsHandler(msg.msgBody.(*um.Neighbors))

	default:

		log.LogCallerFileLine("NgbMgrUdpMsgHandler: " +
			"invalid udp message type: %d",
			msg.msgType)

		eno = NgbMgrEnoParameter
	}

	if eno != NgbMgrEnoNone {
		log.LogCallerFileLine("NgbMgrProc: errors, eno: %d", eno)
	}

	return eno
}

//
// Ping handler
//
func (ngbMgr *NeighborManager)PingHandler(ping *um.Ping) NgbMgrErrno {

	//
	// Here we are pinged by another node
	//
	// currently relay is not supported, check if we are the target, if false, we
	// just discard this ping message.
	//

	if ping.To.NodeId != ngbMgr.cfg.ID {

		log.LogCallerFileLine("PingHandler: " +
			"not the target: %s",
			config.P2pNodeId2HexString(ngbMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(ping.Expiration) {
		log.LogCallerFileLine("PingHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	//
	// send Pong if the sub network identity spcefied is matched
	//

	matched := false

	for _, snid := range ngbMgr.cfg.SubNetIdList {
		if snid == ping.SubNetId {
			matched = true
			break
		}
	}

	if !matched {
		log.LogCallerFileLine("PingHandler: subnet mismatched")
		return NgbMgrEnoMismatched
	}

	pong := um.Pong{
		From:			ping.To,
		To:				ping.From,
		FromSubNetId:	ngbMgr.cfg.SubNetIdList,
		SubNetId:		ping.SubNetId,
		Id:				uint64(time.Now().UnixNano()),
		Expiration:		0,
		Extra:			nil,
	}

	toAddr := net.UDPAddr {
		IP: 	ping.From.IP,
		Port:	int(ping.From.UDP),
		Zone:	"",
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypePong, &pong); eno != um.UdpMsgEnoNone {

		log.LogCallerFileLine("PingHandler: " +
			"Encode failed, eno: %d",
			eno)

		return NgbMgrEnoEncode
	}

	if buf, bytes := pum.GetRawMessage(); buf != nil && bytes > 0 {

		if eno := sendUdpMsg(ngbMgr.sdl, ngbMgr.ptnLsn, ngbMgr.ptnMe, buf, &toAddr); eno != sch.SchEnoNone {

			log.LogCallerFileLine("PingHandler: " +
				"sendUdpMsg failed, eno: %d",
				eno)

			return NgbMgrEnoUdp
		}
	} else {
		log.LogCallerFileLine("PingHandler: " +
			"invalid buffer, buf: %p, length: %d",
			interface{}(buf), bytes)
		return NgbMgrEnoEncode
	}

	//
	// check if neighbor task instance exist for the sender node, if none,
	// we then send message to table manager to tell we are pinged, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := config.P2pNodeId2HexString(ping.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(ping.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == true {

		log.LogCallerFileLine("PingHandler: " +
			"neighbor instance exist: %s",
			strPeerNodeId)

		return NgbMgrEnoNone
	}

	var schMsg = sch.SchMessage{}

	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingedInd, ping)
	ngbMgr.sdl.SchSendMessage(&schMsg)

	return NgbMgrEnoNone
}

//
// Pong handler
//
func (ngbMgr *NeighborManager)PongHandler(pong *um.Pong) NgbMgrErrno {

	//
	// Here we got pong from another node
	//
	// currently relay is not supported, check if we are the target, if false, we
	// just discard this Pong message.
	//

	if pong.To.NodeId != ngbMgr.cfg.ID {

		log.LogCallerFileLine("PongHandler: " +
			"not the target: %s",
			config.P2pNodeId2HexString(ngbMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(pong.Expiration) {
		log.LogCallerFileLine("PongHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	//
	// check if neighbor task instance exist for the sender node, if none,
	// we then send message to table manager to tell we are pinged, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := config.P2pNodeId2HexString(pong.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(pong.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == false {

		log.LogCallerFileLine("PongHandler: " +
			"indicate that we are ponged by node: %s",
			fmt.Sprintf("%+v", pong.From))

		var schMsg = sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPongedInd, pong)
		ngbMgr.sdl.SchSendMessage(&schMsg)

		return NgbMgrEnoNone
	}

	//
	// neighbor task instance exist, we just dispatch this Pong to it
	//

	ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn

	var schMsg = sch.SchMessage{}
	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblPingpongRsp, pong)
	ngbMgr.sdl.SchSendMessage(&schMsg)

	return NgbMgrEnoNone
}

//
// FindNode handler
//
func (ngbMgr *NeighborManager)FindNodeHandler(findNode *um.FindNode) NgbMgrErrno {

	//
	// Here we are requested to FindNode by another node. We need to check our buckets
	// to response the peer node with our neighbors, but we do not start neighbor task
	// instance to init a procedure of finding nodes(notice that, this is defferent from
	// that when we receiving Ping or Pong from peers, if the instances for peers are not
	// exist, we do start instances to init Ping procedures). This is saying that: if a
	// node send FindNode message to us, then our node must be known to the sender, so we
	// need not to carry a bounding procedure for this sender.
	//
	// For the reason described aboved, We just call interface exported by table module
	// to add the sender node to bucket and node database.
	//

	if findNode.To.NodeId != ngbMgr.cfg.ID {
		log.LogCallerFileLine("FindNodeHandler: " +
				"local is not the destination: %s",
				fmt.Sprintf("%X", findNode.To.NodeId))
		return NgbMgrEnoParameter
	}

	if expired(findNode.Expiration) {
		log.LogCallerFileLine("FindNodeHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	//
	// Response the sender with our neighbors closest to it.
	// Notice: if the target of this FindNode message is same as the sender
	// node identity, then the buckets of the sender might be all empty, in
	// such case, if the closest set is empty, we then append our local node
	// as a neighbor for the sender; if the closest not exceed the max, we
	// append local node; if the closet is max, substitute the tail node with
	// local. See bellow please.
	//

	var nodes = make([]*tab.Node, 0)
	var umNodes = make([]*um.Node, 0)

	local := ngbMgr.localNode()

	if findNode.SubNetId != config.AnySubNet {

		mgr := ngbMgr.tabMgr.TabGetInstBySubNetId(&findNode.SubNetId)

		if mgr == nil {

			log.LogCallerFileLine("FindNodeHandler: table manager mismatched")
			return NgbMgrEnoMismatched
		}

		nodes = append(nodes, mgr.TabClosest(tab.Closest4Queried, tab.NodeID(findNode.Target), tab.TabInstQPendingMax)...)

	} else {

		var nodeMap = make(map[tab.NodeID]*tab.Node, 0)
		var mgr *tab.TableManager

		for _, mgr = range ngbMgr.tabMgr.SubNetMgrList {

			if mgr == nil {
				continue
			}

			ns := mgr.TabClosest(tab.Closest4Queried, tab.NodeID(findNode.Target), tab.TabInstQPendingMax)

			for _, n := range ns {
				nodeMap[n.ID] = n
			}
		}

		for _, n := range nodeMap {
			nodes = append(nodes, n)
		}
	}

	cfgNode := config.Node{
		IP:		ngbMgr.cfg.IP,
		UDP:	ngbMgr.cfg.UDP,
		TCP:	ngbMgr.cfg.TCP,
		ID:		ngbMgr.cfg.ID,
	}

	for idx, n := range nodes {
		if n.ID == findNode.From.NodeId {
			nodes = append(nodes[:idx], nodes[idx+1:]...)
			break
		}
	}

	if len(nodes) == 0 && findNode.SubNetId == config.AnySubNet {

		nodes = append(nodes, tab.TabBuildNode(&cfgNode))

	} else if findNode.From.NodeId == findNode.Target {

		num := len(nodes)
		if num < tab.TabInstQPendingMax {

			nodes = append(nodes, tab.TabBuildNode(&cfgNode))

		} else {

			nodes[num-1] = tab.TabBuildNode(&cfgNode)
		}
	}

	for _, n := range nodes {
		umn := um.Node {
			IP:		n.IP,
			UDP:	n.UDP,
			TCP:	n.TCP,
			NodeId:	n.ID,
		}
		umNodes = append(umNodes, &umn)
	}

	neighbors := um.Neighbors{
		From: 			*local,
		To:				findNode.From,
		FromSubNetId:	ngbMgr.cfg.SubNetIdList,
		SubNetId:		findNode.SubNetId,
		Id:				uint64(time.Now().UnixNano()),
		Nodes:			umNodes,
		Expiration:		0,
		Extra:			nil,

	}

	toAddr := net.UDPAddr {
		IP: 	findNode.From.IP,
		Port:	int(findNode.From.UDP),
		Zone:	"",
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypeNeighbors, &neighbors); eno != um.UdpMsgEnoNone {

		log.LogCallerFileLine("FindNodeHandler: " +
			"Encode failed, eno: %d", eno)

		return NgbMgrEnoEncode
	}

	if buf, bytes := pum.GetRawMessage(); buf != nil && bytes > 0 {

		if eno := sendUdpMsg(ngbMgr.sdl, ngbMgr.ptnLsn, ngbMgr.ptnMe, buf, &toAddr); eno != sch.SchEnoNone {

			log.LogCallerFileLine("FindNodeHandler: " +
				"sendUdpMsg failed, eno: %d", eno)

			return NgbMgrEnoUdp
		}

	} else {

		log.LogCallerFileLine("FindNodeHandler: " +
			"invalid buffer, buf: %p, length: %d",
			interface{}(buf), bytes)

		return NgbMgrEnoEncode
	}

	//
	// here we had been queried, we send message to table manager to tell this, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := config.P2pNodeId2HexString(findNode.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(findNode.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == false {

		var schMsg= sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblQueriedInd, findNode)
		ngbMgr.sdl.SchSendMessage(&schMsg)
	}

	return NgbMgrEnoNone
}

//
// Neighbors handler
//
func (ngbMgr *NeighborManager)NeighborsHandler(nbs *um.Neighbors) NgbMgrErrno {

	//
	// Here we got Neighbors from another node
	//
	// currently relay is not supported, check if we are the target, if false, we
	// just discard this ping message.
	//
	// Notice: unlike Ping or Pong, if an instance of neighbor task is not exist
	// for peer node, we do not start a neighbor instance here in this function,
	// instead, we discard this "Neighbors" message then return at once.
	//

	if nbs.To.NodeId != ngbMgr.cfg.ID {

		log.LogCallerFileLine("NeighborsHandler: " +
			"not the target: %s",
			config.P2pNodeId2HexString(ngbMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(nbs.Expiration) {
		log.LogCallerFileLine("NeighborsHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	strPeerNodeId := config.P2pNodeId2HexString(nbs.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(nbs.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeFindNode) == false {

		log.LogCallerFileLine("NeighborsHandler: " +
			"discarded for neighbor instance not exist: %s",
			strPeerNodeId)

		return NgbMgrEnoMismatched
	}

	ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn

	var schMsg = sch.SchMessage{}
	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblFindNodeRsp, nbs)
	ngbMgr.sdl.SchSendMessage(&schMsg)

	return NgbMgrEnoNone
}

//
// FindNode request handler
//
func (ngbMgr *NeighborManager)FindNodeReq(findNode *um.FindNode) NgbMgrErrno {

	//
	// Here we are requested to FindNode by local table task: we encode and send the message to
	// destination node, and the create a neighbor task to deal with the findnode procedure. See
	// comments in NgbMgrProc for more pls.
	//

	var rsp = sch.NblFindNodeRsp{}
	var schMsg  = sch.SchMessage{}

	var funcRsp2Tab = func () NgbMgrErrno {

		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}

	//
	// check if duplicated: if true, tell table task it's duplicated
	// by event EvNblFindNodeRsp
	//

	strPeerNodeId := config.P2pNodeId2HexString(findNode.To.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(findNode.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {

		log.LogCallerFileLine("FindNodeReq: " +
			"duplicated neighbor instance: %s", strPeerNodeId)

		rsp.Result = (NgbMgrEnoDuplicated << 16) + tab.TabMgrEnoDuplicated
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	//
	// create a neighbor instance and setup the map
	//

	var ngbInst = neighborInst {
		sdl:		ngbMgr.sdl,
		ngbMgr:		ngbMgr,
		ptn:		nil,
		name:		strPeerNodeId,
		msgType:	um.UdpMsgTypeFindNode,
		msgBody:	findNode,
		tidFN:		sch.SchInvalidTid,
		tidPP:		sch.SchInvalidTid,
	}
	ngbInst.tep = ngbInst.ngbProtoProc

	var noDog = sch.SchWatchDog {
		HaveDog:false,
	}

	//
	// notice: instance task created to be suspended, and it would be started
	// when everything is ok, see bellow pls.
	//

	ngbMgr.fnInstSeq++

	var dc = sch.SchTaskDescription {
		Name:	fmt.Sprintf("%s%d_findnode_%s", NgbProcName, ngbMgr.fnInstSeq, strPeerNodeId),
		MbSize:	ngbProcMailboxSize,
		Ep:		&ngbInst,
		Wd:		&noDog,
		Flag:	sch.SchCreatedSuspend,
		DieCb:	ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := ngbMgr.sdl.SchCreateTask(&dc)
	if eno != sch.SchEnoNone {

		log.LogCallerFileLine("FindNodeReq: " +
			"SchCreateTask failed, eno: %d",
			eno)

		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	rsp.Result = NgbMgrEnoNone
	rsp.FindNode = findNode

	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblFindNodeReq, findNode);
	ngbMgr.sdl.SchSendMessage(&schMsg)

	//
	// backup task pointer; setup the map; start instance;
	//

	ngbInst.ptn = ptn
	ngbMgr.setupMap(strPeerNodeId, &ngbInst)

	if eno := ngbMgr.sdl.SchStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {

		log.LogCallerFileLine("FindNodeReq: " +
			"start instance failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	return NgbMgrEnoNone
}

//
// Pingpong(ping) request handler
//
func (ngbMgr *NeighborManager)PingpongReq(ping *um.Ping) NgbMgrErrno {

	//
	// Here we are requested to Ping another node by local table task
	//

	var rsp = sch.NblPingRsp{}
	var schMsg  = sch.SchMessage{}

	var funcRsp2Tab = func () NgbMgrErrno {
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}

	var funcReq2Inst = func(ptn interface{}) NgbMgrErrno {
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblPingpongReq, ping)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}

	//
	// check if duplicated: if true, tell table task it's duplicated by event EvNblFindNodeRsp
	//

	strPeerNodeId := config.P2pNodeId2HexString(ping.To.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(ping.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {

		log.LogCallerFileLine("PingpongReq: " +
			"duplicated neighbor instance: %s",
			strPeerNodeId)

		rsp.Result = NgbMgrEnoDuplicated
		rsp.Ping = ping

		return funcRsp2Tab()
	}

	//
	// create a neighbor instance and setup the map
	//

	var ngbInst = neighborInst {
		sdl:		ngbMgr.sdl,
		ngbMgr:		ngbMgr,
		ptn:		nil,
		name:		strPeerNodeId,
		msgType:	um.UdpMsgTypePing,
		msgBody:	ping,
		tidFN:		sch.SchInvalidTid,
		tidPP:		sch.SchInvalidTid,
	}
	ngbInst.tep = ngbInst.ngbProtoProc

	var noDog = sch.SchWatchDog {
		HaveDog:false,
	}

	ngbMgr.ppInstSeq++

	var dc = sch.SchTaskDescription {
		Name:	fmt.Sprintf("%s%d_pingpong_%s", NgbProcName, ngbMgr.ppInstSeq, strPeerNodeId),
		MbSize:	ngbProcMailboxSize,
		Ep:		&ngbInst,
		Wd:		&noDog,
		Flag:	sch.SchCreatedSuspend,
		DieCb:	ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := ngbMgr.sdl.SchCreateTask(&dc)

	if eno != sch.SchEnoNone {

		log.LogCallerFileLine("PingpongReq: " +
			"SchCreateTask failed, eno: %d",
			eno)

		rsp.Result = NgbMgrEnoScheduler
		rsp.Ping = ping
		return funcRsp2Tab()
	}

	if eno := funcReq2Inst(ptn); eno != NgbMgrEnoNone {

		log.LogCallerFileLine("PingpongReq: " +
			"funcReq2Inst failed, eno: %d",
			eno)

		rsp.Result = int(eno)
		rsp.Ping = ping

		return funcRsp2Tab()
	}

	//
	// backup task pointer; setup the map; start instance;
	//

	ngbInst.ptn = ptn
	ngbMgr.setupMap(strPeerNodeId, &ngbInst)

	if eno := ngbMgr.sdl.SchStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {

		log.LogCallerFileLine("PingpongReq: " +
			"start instance failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	return NgbMgrEnoNone
}

//
// Setup map for neighbor instance
//
func (ngbMgr *NeighborManager) setupMap(name string, inst *neighborInst) {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	ngbMgr.ngbMap[name] = inst
}

//
// Clean map for neighbor instance
//
func (ngbMgr *NeighborManager) cleanMap(name string) {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	delete(ngbMgr.ngbMap, name)
}

//
// Check map for neighbor instance
//
func (ngbMgr *NeighborManager) checkMap(name string, umt um.UdpMsgType) bool {

	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()

	ngb, ok := ngbMgr.ngbMap[name]

	if umt == um.UdpMsgTypeAny {
		return ok
	}

	return ok && ngb.msgType == umt
}

//
// Get instance from map
//
func (ngbMgr *NeighborManager) getMap(name string) *neighborInst {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	return ngbMgr.ngbMap[name]
}

//
// Construct local udpmsg.Endpoint object
//
func (ngbMgr *NeighborManager) localEndpoint() *um.Endpoint {
	return &um.Endpoint {
		IP:		ngbMgr.cfg.IP,
		UDP:	ngbMgr.cfg.UDP,
		TCP:	ngbMgr.cfg.TCP,
	}
}

//
// Construct local udpmsg.Node object
//
func (ngbMgr *NeighborManager) localNode() *um.Node {
	return &um.Node {
		IP:		ngbMgr.cfg.IP,
		UDP:	ngbMgr.cfg.UDP,
		TCP:	ngbMgr.cfg.TCP,
		NodeId:	ngbMgr.cfg.ID,
	}
}

//
// Check if request or response timeout
//
func expired(ts uint64) bool {

	//
	// If ts is zero, it would be never expired; but notice
	// if (ts < 0) is true, it would be always expired.
	//

	if ts == 0 { return false }

	return time.Unix(int64(ts), 0).Before(time.Now())
}

//
//Setup configuraion
//
func (ngbMgr *NeighborManager)setupConfig() sch.SchErrno {

	var ptCfg *config.Cfg4UdpNgbManager = nil

	if ptCfg = config.P2pConfig4UdpNgbManager(ngbMgr.sdl.SchGetP2pCfgName()); ptCfg == nil {
		log.LogCallerFileLine("setupConfig: P2pConfig4UdpNgbManager failed")
		return sch.SchEnoConfig
	}

	ngbMgr.cfg.IP			= ptCfg.IP
	ngbMgr.cfg.UDP			= ptCfg.UDP
	ngbMgr.cfg.TCP			= ptCfg.TCP
	ngbMgr.cfg.ID			= ptCfg.ID
	ngbMgr.cfg.SubNetIdList	= ptCfg.SubNetIdList

	if len(ngbMgr.cfg.SubNetIdList) == 0 && ngbMgr.cfg.NetworkType == config.P2pNewworkTypeDynamic {
		ngbMgr.cfg.SubNetIdList = append(ngbMgr.cfg.SubNetIdList, config.AnySubNet)
	}

	return sch.SchEnoNone
}

