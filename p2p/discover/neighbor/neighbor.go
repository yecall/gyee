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
	sch		"github.com/yeeco/p2p/scheduler"
	um		"github.com/yeeco/p2p/discover/udpmsg"
	ycfg	"github.com/yeeco/p2p/config"
	yclog	"github.com/yeeco/p2p/logger"
	tab		"github.com/yeeco/p2p/discover/table"
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
	ptn		interface{}		// task node pointer
	name	string			// task instance name
	msgType	um.UdpMsgType	// message type to inited this instance
	msgBody	interface{}		// message body
	tidFN	int				// FindNode timer identity
	tidPP	int				// Pingpong timer identity
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
// Neighbor task entry
//
func NgbProtoProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	yclog.LogCallerFileLine("NgbProtoProc: " +
		"scheduled, sender: %s, recver: %s, msg: %d",
		sch.SchinfGetMessageSender(msg),
		sch.SchinfGetMessageRecver(msg),
		msg.Id)

	var protoEno NgbProtoErrno
	var inst *neighborInst

	inst = sch.SchinfGetUserDataArea(ptn).(*neighborInst)

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

		yclog.LogCallerFileLine("NgbProtoProc: " +
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

		yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"invalid find node request")

		return NgbProtoEnoParameter
	}

	//
	// encode request
	//

	var pum = new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypeFindNode, fn); eno != um.UdpMsgEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"encode failed, eno: %d",
			eno)

		return NgbProtoEnoEncode
	}

	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes == 0 {

		yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"invalid encoded  message")

		return NgbProtoEnoEncode
	}

	//
	// send encoded request
	//

	var dst = net.UDPAddr{}
	dst.IP = append(dst.IP, fn.To.IP...)
	dst.Port = int(fn.To.UDP)

	if eno := sendUdpMsg(buf, &dst); eno != sch.SchEnoNone {

		//
		// response FindNode  NgbProtoEnoUdp to table task
		//

		yclog.LogCallerFileLine("NgbProtoFindNodeReq：" +
			"sendUdpMsg failed, dst: %s, eno: %d",
			dst.String(), eno)

		var rsp = sch.NblFindNodeRsp{}
		var schMsg  = sch.SchMessage{}

		rsp.Result = (NgbProtoEnoUdp << 16) + tab.TabMgrEnoUdp
		rsp.FindNode = inst.msgBody.(*um.FindNode)

		if eno := sch.SchinfMakeMessage(&schMsg, inst.ptn, ngbMgr.ptnTab,
			sch.EvNblFindNodeRsp, &rsp); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoFindNodeReq: "+
				"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
				eno,
				sch.SchinfGetMessageSender(&schMsg),
				sch.SchinfGetMessageRecver(&schMsg))

			return NgbMgrEnoScheduler
		}

		yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
			"EvNblFindNodeRsp sent ok, target: %s",
			sch.SchinfGetTaskName(ngbMgr.ptnTab))

		//
		// remove ourself from map in manager. notice: since we had setup the calback
		// for task done(killed), this cleaning would carried out by scheduler, but
		// we do cleaning here to obtain a more clear seen.
		//

		ngbMgr.cleanMap(inst.name)

		//
		// done the instance task
		//

		if eno := sch.SchinfTaskDone(inst.ptn, sch.SchEnoNone); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
				"SchinfTaskDone failed, eno: %d, name: %s",
				eno,
				sch.SchinfGetTaskName(inst.ptn))

			return NgbProtoEnoScheduler
		}

	 	return NgbProtoEnoUdp
	 }

	yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
		"FindNode message sent ok, peer: %s",
		fmt.Sprintf("%+v", fn.To))

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

	 if eno, tid = sch.SchInfSetTimer(ptn, &tmd); eno != sch.SchEnoNone {

	 	yclog.LogCallerFileLine("NgbProtoFindNodeReq: " +
	 		"set timer failed, eno: %d",
	 		eno)

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

		yclog.LogCallerFileLine("NgbProtoPingReq:" +
			" invalid ping request")

		return NgbProtoEnoParameter
	}

	//
	// encode request
	//

	var pum = new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypePing, ping); eno != um.UdpMsgEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingReq: " +
			"encode failed, eno: %d",
			eno)

		return NgbProtoEnoEncode
	}

	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes == 0 {

		yclog.LogCallerFileLine("NgbProtoPingReq: " +
			"invalid encoded  message")

		return NgbProtoEnoEncode
	}

	//
	// send encoded request
	//

	var dst = net.UDPAddr{}
	dst.IP = append(dst.IP, ping.To.IP...)
	dst.Port = int(ping.To.UDP)

	if eno := sendUdpMsg(buf, &dst); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingReq：" +
			"sendUdpMsg failed, dst: %s, eno: %d",
			dst.String(), eno)

		return NgbProtoEnoOs
	}

	yclog.LogCallerFileLine("NgbProtoPingReq: " +
		"Ping message sent ok, peer: %s",
		fmt.Sprintf("%+v", ping.To))

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

	if eno, tid = sch.SchInfSetTimer(ptn, &tmd); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingReq: " +
			"set timer failed, eno: %d",
			eno)

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

		yclog.LogCallerFileLine("NgbProtoPingRsp: " +
			"response mismatched with request")

		return NgbProtoEnoParameter
	}

	//
	// this "Pong" must be from node which we had pinged, notice that the
	// function "CompareWith" make a full matching between tow nodes.
	//

	var ping = inst.msgBody.(*um.Ping)
	if equ := ping.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {

		yclog.LogCallerFileLine("NgbProtoPingRsp: " +
			"node mismatched, equ: %d",
			equ)

		return NgbProtoEnoParameter
	}

	//
	// kill pingpong timer if needed
	//

	if inst.tidPP != sch.SchInvalidTid {

		if eno := sch.SchinfKillTimer(inst.ptn, inst.tidPP); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoPingRsp: " +
				"SchinfKillTimer failed, tid: %d, eno: %d",
				inst.tidPP, eno)

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

	if eno := sch.SchinfMakeMessage(&schMsg, inst.ptn, ngbMgr.ptnTab,
		sch.EvNblPingpongRsp, &rsp); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingRsp: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingRsp: "+
			"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
			eno,
			sch.SchinfGetMessageSender(&schMsg),
			sch.SchinfGetMessageRecver(&schMsg))

		return NgbMgrEnoScheduler
	}

	yclog.LogCallerFileLine("NgbProtoPingRsp: " +
		"send EvNblPingpongRsp to table manager ok, pong: %s",
		fmt.Sprintf("%+v", rsp.Pong))

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	if eno := sch.SchinfTaskDone(inst.ptn, sch.SchEnoNone); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingRsp: " +
			"SchinfTaskDone failed, eno: %d, name: %s",
			eno,
			sch.SchinfGetTaskName(inst.ptn))

		return NgbProtoEnoScheduler
	}

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

		yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"response mismatched with request")

		return NgbProtoEnoParameter
	}

	//
	// this response must be from node which we had pinged, notice that the
	// function "CompareWith" make a full matching between tow nodes.
	//

	var findNode = inst.msgBody.(*um.FindNode)
	if equ := findNode.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {

		yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"node mismatched, equ: %d",
			equ)

		return NgbProtoEnoParameter
	}

	//
	// kill findnode timer if needed
	//

	if inst.tidFN != sch.SchInvalidTid {

		if eno := sch.SchinfKillTimer(inst.ptn, inst.tidFN); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
				"SchinfKillTimer failed, tid: %d, eno: %d",
				inst.tidFN, eno)

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

	if eno := sch.SchinfMakeMessage(&schMsg, inst.ptn, ngbMgr.ptnTab,
		sch.EvNblFindNodeRsp, &rsp); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeRsp: "+
			"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
			eno,
			sch.SchinfGetMessageSender(&schMsg),
			sch.SchinfGetMessageRecver(&schMsg))

		return NgbMgrEnoScheduler
	}

	yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
		"EvNblFindNodeRsp sent ok, target: %s",
		sch.SchinfGetTaskName(ngbMgr.ptnTab))

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	if eno := sch.SchinfTaskDone(inst.ptn, sch.SchEnoNone); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeRsp: " +
			"SchinfTaskDone failed, eno: %d, name: %s",
			eno, sch.SchinfGetTaskName(inst.ptn))

		return NgbProtoEnoScheduler
	}

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

	if eno := sch.SchinfMakeMessage(&schMsg, inst.ptn, ngbMgr.ptnTab,
		sch.EvNblFindNodeRsp, &rsp); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeTimeout: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeTimeout: "+
			"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
			eno,
			sch.SchinfGetMessageSender(&schMsg),
			sch.SchinfGetMessageRecver(&schMsg))

		return NgbMgrEnoScheduler
	}

	yclog.LogCallerFileLine("NgbProtoFindNodeTimeout: " +
		"EvNblFindNodeRsp sent ok, target: %s",
		sch.SchinfGetTaskName(ngbMgr.ptnTab))

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	if eno := sch.SchinfTaskDone(inst.ptn, sch.SchEnoNone); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeTimeout: " +
			"SchinfTaskDone failed, eno: %d, name: %s",
			eno,
			sch.SchinfGetTaskName(inst.ptn))

		return NgbProtoEnoScheduler
	}

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

	if eno := sch.SchinfMakeMessage(&schMsg, inst.ptn, ngbMgr.ptnTab,
		sch.EvNblPingpongRsp, &rsp); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingTimeout: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoPingTimeout: "+
			"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
			eno,
			sch.SchinfGetMessageSender(&schMsg),
			sch.SchinfGetMessageRecver(&schMsg))

		return NgbMgrEnoScheduler
	}

	//
	// remove ourself from map in manager. notice: since we had setup the calback
	// for task done(killed), this cleaning would carried out by scheduler, but
	// we do cleaning here to obtain a more clear seen.
	//

	ngbMgr.cleanMap(inst.name)

	//
	// done the instance task
	//

	if eno := sch.SchinfTaskDone(inst.ptn, sch.SchEnoNone); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NgbProtoFindNodeTimeout: " +
			"SchinfTaskDone failed, eno: %d, name: %s",
			eno, sch.SchinfGetTaskName(inst.ptn))

		return NgbProtoEnoScheduler
	}

	return NgbProtoEnoNone
}

//
// Callbacked when died
//
func (ni *neighborInst) NgbProtoDieCb(ptn interface{}) sch.SchErrno {

	//
	// here we are called while task is exiting, we need to free resources had been
	// allocated to the instance
	//

	var inst *neighborInst

	//
	// get user data pointer which points to our instance
	//

	if inst = sch.SchinfGetUserDataArea(ptn).(*neighborInst); inst == nil {

		yclog.LogCallerFileLine("NgbProtoDieCb: " +
			"invalid user data area, name: %s",
			sch.SchinfGetTaskName(ptn))

		return sch.SchEnoInternal
	}

	//
	// kill any timer if needed, should not care the result returned from
	// scheduler, for timer might have been killed by scheduler.
	//

	if ni.tidPP != sch.SchInvalidTid {

		if eno := sch.SchinfKillTimer(ni.ptn, ni.tidPP); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoDieCb: " +
				"SchinfKillTimer fialed, eno: %d",
				eno)
		}
	}

	if ni.tidFN != sch.SchInvalidTid {

		if eno := sch.SchinfKillTimer(ni.ptn, ni.tidFN); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("NgbProtoDieCb: " +
				"SchinfKillTimer fialed, eno: %d",
				eno)
		}
	}

	//
	// clean the map
	//

	ngbMgr.cleanMap(inst.name)

	//
	// More ... ?
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
type neighborManager struct {
	lock		sync.Mutex					// lock for protection
	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	ptnMe		interface{}					// pointer to task node of myself
	ptnTab		interface{}					// pointer to task node of table task
	ngbMap		map[string]*neighborInst	// map neighbor node id to task node pointer
}

//
// It's a static task, only one instance would be
//
var ngbMgr = &neighborManager {
	name:	NgbMgrName,
	tep:	nil,
	ptnMe:	nil,
	ptnTab:	nil,
	ngbMap:	make(map[string]*neighborInst),
}

//
// The scheduler need unique task name, here is sequence for dynamic instance task name
//
var fnInstSeq = 0	// for findnode instance
var ppInstSeq = 0	// for pingpong instance

//
// To escape the compiler "initialization loop" error
//
func init() {
	ngbMgr.tep = NgbMgrProc
}

//
// Neighbor manager task entry
//
func NgbMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	yclog.LogCallerFileLine("NgbMgrProc: scheduled, msg: %d", msg.Id)

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

		yclog.LogCallerFileLine("NgbMgrProc: " +
			"invalid message id: %d",
			msg.Id)

		eno = NgbMgrEnoParameter
	}

	if eno != NgbMgrEnoNone {

		yclog.LogCallerFileLine("NgbMgrProc: " +
			"errors, eno: %d",
			eno)

		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweron handler
//
func (ngbMgr *neighborManager)PoweronHandler(ptn interface{}) sch.SchErrno {

	ngbMgr.ptnMe = ptn
	eno, ptnTab := sch.SchinfGetTaskNodeByName(sch.TabMgrName)

	if 	eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("PoweronHandler: " +
			"SchinfGetTaskNodeByName failed, eno: %d, name: %s",
			eno, sch.TabMgrName)
		return eno
	}

	if ptnTab == nil {
		yclog.LogCallerFileLine("PoweronHandler: " +
			"invalid table task node pointer")
		return sch.SchEnoUnknown
	}

	ngbMgr.ptnMe = ptn
	ngbMgr.ptnTab = ptnTab

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (ngbMgr *neighborManager)PoweroffHandler(ptn interface{}) sch.SchErrno {
	yclog.LogCallerFileLine("PoweroffHandler: done for poweroff event")
	return sch.SchinfTaskDone(ptn, sch.SchEnoKilled)
}

//
// udpmsg handler
//
func (ngbMgr *neighborManager)UdpMsgInd(msg *UdpMsgInd) NgbMgrErrno {

	yclog.LogCallerFileLine("UdpMsgInd: " +
		"udp message indication received, type: %d",
		msg.msgType)

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

		yclog.LogCallerFileLine("NgbMgrUdpMsgHandler: " +
			"invalid udp message type: %d",
			msg.msgType)

		eno = NgbMgrEnoParameter
	}

	if eno != NgbMgrEnoNone {
		yclog.LogCallerFileLine("NgbMgrProc: errors, eno: %d", eno)
	}

	return eno
}

//
// Ping handler
//
func (ngbMgr *neighborManager)PingHandler(ping *um.Ping) NgbMgrErrno {

	//
	// Here we are pinged by another node
	//
	// currently relay is not supported, check if we are the target, if false, we
	// just discard this ping message.
	//

	yclog.LogCallerFileLine("PingHandler: handle Ping message from peer")

	if ping.To.NodeId != lsnMgr.cfg.ID {

		yclog.LogCallerFileLine("PingHandler: " +
			"not the target: %s",
			ycfg.P2pNodeId2HexString(lsnMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(ping.Expiration) {
		yclog.LogCallerFileLine("PingHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	//
	// send Pong always
	//

	pong := um.Pong{
		From:		ping.To,
		To:			ping.From,
		Expiration:	0,
		Extra:		nil,
	}

	toAddr := net.UDPAddr {
		IP: 	ping.From.IP,
		Port:	int(ping.From.UDP),
		Zone:	"",
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypePong, &pong); eno != um.UdpMsgEnoNone {

		yclog.LogCallerFileLine("PingHandler: " +
			"Encode failed, eno: %d",
			eno)

		return NgbMgrEnoEncode
	}

	if buf, bytes := pum.GetRawMessage(); buf != nil && bytes > 0 {

		if eno := sendUdpMsg(buf, &toAddr); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("PingHandler: " +
				"sendUdpMsg failed, eno: %d",
				eno)

			return NgbMgrEnoUdp
		}
	} else {
		yclog.LogCallerFileLine("PingHandler: " +
			"invalid buffer, buf: %p, length: %d",
			interface{}(buf), bytes)
		return NgbMgrEnoEncode
	}

	yclog.LogCallerFileLine("PingHandler: " +
		"send Pong message ok, peer: %s",
		fmt.Sprintf("%+v", pong.To))

	//
	// check if neighbor task instance exist for the sender node, if none,
	// we then send message to table manager to tell we are pinged, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := ycfg.P2pNodeId2HexString(ping.From.NodeId)
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == true {

		yclog.LogCallerFileLine("PingHandler: " +
			"neighbor instance exist: %s",
			strPeerNodeId)

		return NgbMgrEnoNone
	}

	yclog.LogCallerFileLine("PingHandler: " +
		"indicate that we are pinged by node: %s",
		fmt.Sprintf("%+v", ping.From))

	var schMsg = sch.SchMessage{}

	if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingedInd, ping);
	eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("PingHandler: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)
		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("PingHandler: " +
			"SchinfSendMessage EvNblPingedInd failed, eno: %d",
			eno)
		return NgbMgrEnoScheduler
	}

	yclog.LogCallerFileLine("PingHandler: " +
		"send EvNblPingedInd ok, target: %s",
		sch.SchinfGetTaskName(ngbMgr.ptnTab))

	return NgbMgrEnoNone
}

//
// Pong handler
//
func (ngbMgr *neighborManager)PongHandler(pong *um.Pong) NgbMgrErrno {

	//
	// Here we got pong from another node
	//
	// currently relay is not supported, check if we are the target, if false, we
	// just discard this Pong message.
	//

	yclog.LogCallerFileLine("PongHandler: handle Pong message from peer")

	if pong.To.NodeId != lsnMgr.cfg.ID {

		yclog.LogCallerFileLine("PongHandler: " +
			"not the target: %s",
			ycfg.P2pNodeId2HexString(lsnMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(pong.Expiration) {
		yclog.LogCallerFileLine("PongHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	//
	// check if neighbor task instance exist for the sender node, if none,
	// we then send message to table manager to tell we are pinged, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := ycfg.P2pNodeId2HexString(pong.From.NodeId)

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == false {

		yclog.LogCallerFileLine("PongHandler: " +
			"indicate that we are ponged by node: %s",
			fmt.Sprintf("%+v", pong.From))

		var schMsg = sch.SchMessage{}

		if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPongedInd, pong);
			eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("PongHandler: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)
			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("PongHandler: " +
				"SchinfSendMessage EvNblPongedInd failed, eno: %d",
				eno)
			return NgbMgrEnoScheduler
		}

		yclog.LogCallerFileLine("PongHandler: " +
			"send EvNblPongedInd ok, target: %s",
			sch.SchinfGetTaskName(ngbMgr.ptnTab))

		return NgbMgrEnoNone
	}

	//
	// neighbor task instance exist, we just dispatch this Pong to it
	//

	ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn

	var schMsg = sch.SchMessage{}

	if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblPingpongRsp, pong);
	eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("PongHandler: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("PongHandler: " +
			"SchinfSendMessage failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	yclog.LogCallerFileLine("PongHandler: " +
		"dispatch Pong to neighbor instance task ok, peer: %s",
		strPeerNodeId)

	return NgbMgrEnoNone
}

//
// FindNode handler
//
func (ngbMgr *neighborManager)FindNodeHandler(findNode *um.FindNode) NgbMgrErrno {

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

	if findNode.To.NodeId != lsnMgr.cfg.ID {
		yclog.LogCallerFileLine("FindNodeHandler: " +
				"local is not the destination: %s",
				fmt.Sprintf("%X", findNode.To.NodeId))
		return NgbMgrEnoParameter
	}

	if expired(findNode.Expiration) {
		yclog.LogCallerFileLine("FindNodeHandler: request timeout")
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
	nodes = append(nodes, tab.TabClosest(tab.NodeID(findNode.Target), tab.TabInstQPendingMax)...)

	if len(nodes) == 0 {

		nodes = append(nodes, tab.TabBuildNode(&ycfg.PtrConfig.Local))

	} else if findNode.From.NodeId == findNode.Target {

		num := len(nodes)
		if num < tab.TabInstQPendingMax {

			nodes = append(nodes, tab.TabBuildNode(&ycfg.PtrConfig.Local))

		} else {

			nodes[num-1] = tab.TabBuildNode(&ycfg.PtrConfig.Local)
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
		From: 		*local,
		To:			findNode.From,
		Id:			uint64(time.Now().UnixNano()),
		Nodes:		umNodes,
		Expiration:	0,
		Extra:		nil,

	}

	toAddr := net.UDPAddr {
		IP: 	findNode.From.IP,
		Port:	int(findNode.From.UDP),
		Zone:	"",
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypeNeighbors, &neighbors); eno != um.UdpMsgEnoNone {

		yclog.LogCallerFileLine("FindNodeHandler: " +
			"Encode failed, eno: %d", eno)

		return NgbMgrEnoEncode
	}

	if buf, bytes := pum.GetRawMessage(); buf != nil && bytes > 0 {

		if eno := sendUdpMsg(buf, &toAddr); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("FindNodeHandler: " +
				"sendUdpMsg failed, eno: %d", eno)

			return NgbMgrEnoUdp
		}

	} else {

		yclog.LogCallerFileLine("FindNodeHandler: " +
			"invalid buffer, buf: %p, length: %d",
			interface{}(buf), bytes)

		return NgbMgrEnoEncode
	}

	//
	// here we had been queried, we send message to table manager to tell this, so it
	// could determine what to do in its' running context.
	//

	strPeerNodeId := ycfg.P2pNodeId2HexString(findNode.From.NodeId)
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == false {

		var schMsg= sch.SchMessage{}

		if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblQueriedInd, findNode);
			eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("FindNodeHandler: "+
				"SchinfMakeMessage failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("FindNodeHandler: "+
				"SchinfSendMessage EvNblQueriedInd failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}
	}

	return NgbMgrEnoNone
}

//
// Neighbors handler
//
func (ngbMgr *neighborManager)NeighborsHandler(nbs *um.Neighbors) NgbMgrErrno {

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

	if nbs.To.NodeId != lsnMgr.cfg.ID {

		yclog.LogCallerFileLine("NeighborsHandler: " +
			"not the target: %s",
			ycfg.P2pNodeId2HexString(lsnMgr.cfg.ID))

		return NgbMgrEnoParameter
	}

	if expired(nbs.Expiration) {
		yclog.LogCallerFileLine("NeighborsHandler: request timeout")
		return NgbMgrEnoTimeout
	}

	strPeerNodeId := ycfg.P2pNodeId2HexString(nbs.From.NodeId)
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeFindNode) == false {

		yclog.LogCallerFileLine("NeighborsHandler: " +
			"discarded for neighbor instance not exist: %s",
			strPeerNodeId)

		return NgbMgrEnoMismatched
	}

	ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn

	var schMsg = sch.SchMessage{}

	if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblFindNodeRsp, nbs);
	eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NeighborsHandler: " +
			"SchinfMakeMessage failed, eno: %d", eno)
		return NgbMgrEnoScheduler
	}

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("NeighborsHandler: " +
			"SchinfSendMessage failed, eno: %d", eno)

		return NgbMgrEnoScheduler
	}

	return NgbMgrEnoNone
}

//
// FindNode request handler
//
func (ngbMgr *neighborManager)FindNodeReq(findNode *um.FindNode) NgbMgrErrno {

	//
	// Here we are requested to FindNode by local table task: we encode and send the message to
	// destination node, and the create a neighbor task to deal with the findnode procedure. See
	// comments in NgbMgrProc for more pls.
	//

	var rsp = sch.NblFindNodeRsp{}
	var schMsg  = sch.SchMessage{}

	var funcRsp2Tab = func () NgbMgrErrno {

		if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp);
		eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("FindNodeReq: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("FindNodeReq: "+
				"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
				eno,
				sch.SchinfGetMessageSender(&schMsg),
				sch.SchinfGetMessageRecver(&schMsg))

			return NgbMgrEnoScheduler
		}

		return NgbMgrEnoNone
	}

	//
	// check if duplicated: if true, tell table task it's duplicated
	// by event EvNblFindNodeRsp
	//

	strPeerNodeId := ycfg.P2pNodeId2HexString(findNode.To.NodeId)

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {

		yclog.LogCallerFileLine("FindNodeReq: " +
			"duplicated neighbor instance: %s", strPeerNodeId)

		rsp.Result = (NgbMgrEnoDuplicated << 16) + tab.TabMgrEnoDuplicated
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	//
	// create a neighbor instance and setup the map
	//

	var ngbInst = neighborInst {
		ptn:		nil,
		name:		strPeerNodeId,
		msgType:	um.UdpMsgTypeFindNode,
		msgBody:	findNode,
		tidFN:		sch.SchInvalidTid,
		tidPP:		sch.SchInvalidTid,
	}

	var noDog = sch.SchWatchDog {
		HaveDog:false,
	}

	//
	// notice: instance task created to be suspended, and it would be started
	// when everything is ok, see bellow pls.
	//

	fnInstSeq++
	var dc = sch.SchTaskDescription {
		Name:	fmt.Sprintf("%s%d_findnode_%s", NgbProcName, fnInstSeq, strPeerNodeId),
		MbSize:	ngbProcMailboxSize,
		Ep:		NgbProtoProc,
		Wd:		&noDog,
		Flag:	sch.SchCreatedSuspend,
		DieCb:	ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := sch.SchinfCreateTask(&dc)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("FindNodeReq: " +
			"SchinfCreateTask failed, eno: %d",
			eno)

		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblFindNodeReq, findNode);
	eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("FindNodeReq: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	rsp.Result = NgbMgrEnoNone
	rsp.FindNode = findNode

	if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("FindNodeReq: " +
			"SchinfSendMessage failed, eno: %d",
			eno)

		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode

		return funcRsp2Tab()
	}

	//
	// backup task pointer; setup the map; start instance;
	//

	ngbInst.ptn = ptn
	ngbMgr.setupMap(strPeerNodeId, &ngbInst)

	if eno := sch.SchinfStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("FindNodeReq: " +
			"start instance failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	return NgbMgrEnoNone
}

//
// Pingpong(ping) request handler
//
func (ngbMgr *neighborManager)PingpongReq(ping *um.Ping) NgbMgrErrno {

	//
	// Here we are requested to Ping another node by local table task
	//

	var rsp = sch.NblPingRsp{}
	var schMsg  = sch.SchMessage{}

	var funcRsp2Tab = func () NgbMgrErrno {

		if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp);
		eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("PingpongReq: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("PingpongReq: "+
				"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
				eno,
				sch.SchinfGetMessageSender(&schMsg),
				sch.SchinfGetMessageRecver(&schMsg))

			return NgbMgrEnoScheduler
		}

		return NgbMgrEnoNone
	}

	var funcReq2Inst = func(ptn interface{}) NgbMgrErrno {

		if eno := sch.SchinfMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblPingpongReq, ping);
		eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("PingpongReq: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)

			return NgbMgrEnoScheduler
		}

		if eno := sch.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("PingpongReq: "+
				"SchinfSendMessage failed, eno: %d, sender: %s, recver: %s",
				eno,
				sch.SchinfGetMessageSender(&schMsg),
				sch.SchinfGetMessageRecver(&schMsg))

			return NgbMgrEnoScheduler
		}

		return NgbMgrEnoNone
	}

	//
	// check if duplicated: if true, tell table task it's duplicated by event EvNblFindNodeRsp
	//

	strPeerNodeId := ycfg.P2pNodeId2HexString(ping.To.NodeId)

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {

		yclog.LogCallerFileLine("PingpongReq: " +
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
		ptn:		nil,
		name:		strPeerNodeId,
		msgType:	um.UdpMsgTypePing,
		msgBody:	ping,
		tidFN:		sch.SchInvalidTid,
		tidPP:		sch.SchInvalidTid,
	}

	var noDog = sch.SchWatchDog {
		HaveDog:false,
	}

	ppInstSeq++

	var dc = sch.SchTaskDescription {
		Name:	fmt.Sprintf("%s%d_pingpong_%s", NgbProcName, ppInstSeq, strPeerNodeId),
		MbSize:	ngbProcMailboxSize,
		Ep:		NgbProtoProc,
		Wd:		&noDog,
		Flag:	sch.SchCreatedSuspend,
		DieCb:	ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := sch.SchinfCreateTask(&dc)

	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("PingpongReq: " +
			"SchinfCreateTask failed, eno: %d",
			eno)

		rsp.Result = NgbMgrEnoScheduler
		rsp.Ping = ping
		return funcRsp2Tab()
	}

	if eno := funcReq2Inst(ptn); eno != NgbMgrEnoNone {

		yclog.LogCallerFileLine("PingpongReq: " +
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

	if eno := sch.SchinfStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("PingpongReq: " +
			"start instance failed, eno: %d",
			eno)

		return NgbMgrEnoScheduler
	}

	return NgbMgrEnoNone
}

//
// Setup map for neighbor instance
//
func (ngbMgr *neighborManager) setupMap(name string, inst *neighborInst) {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	ngbMgr.ngbMap[name] = inst
}

//
// Clean map for neighbor instance
//
func (ngbMgr *neighborManager) cleanMap(name string) {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	delete(ngbMgr.ngbMap, name)
}

//
// Check map for neighbor instance
//
func (ngbMgr *neighborManager) checkMap(name string, umt um.UdpMsgType) bool {

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
func (ngbMgr *neighborManager) getMap(name string) *neighborInst {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	return ngbMgr.ngbMap[name]
}

//
// Construct local udpmsg.Endpoint object
//
func (ngbMgr *neighborManager) localEndpoint() *um.Endpoint {
	return &um.Endpoint {
		IP:		lsnMgr.cfg.IP,
		UDP:	lsnMgr.cfg.UDP,
		TCP:	lsnMgr.cfg.TCP,
	}
}

//
// Construct local udpmsg.Node object
//
func (ngbMgr *neighborManager) localNode() *um.Node {
	return &um.Node {
		IP:		lsnMgr.cfg.IP,
		UDP:	lsnMgr.cfg.UDP,
		TCP:	lsnMgr.cfg.TCP,
		NodeId:	lsnMgr.cfg.ID,
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
