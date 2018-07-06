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


package discover

import (
	"fmt"
	config	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log	"github.com/yeeco/gyee/p2p/logger"
)



//
// errno
//
const (
	DcvMgrEnoNone		= iota
	DcvMgrEnoParameter
	DcvMgrEnoScheduler
)

type DcvMgrErrno int

//
// Discover manager
//
const DcvMgrName = sch.DcvMgrName

type DiscoverManager struct {
	name		string				// name
	tep			sch.SchUserTaskEp	// entry
	ptnMe		interface{}			// task node pointer to myself
	ptnTab		interface{}			// task node pointer to table manager task
	ptnPeMgr	interface{}			// task node pointer to peer manager task
	more		int					// number more peers are needed
	sdl			*sch.Scheduler		// pointer to scheduler
}


//
// Init, we put more in poweron event handler. We can't init the entry point filed
// "tep" with dcvMgr declartion which would result in complier "initialization loop"
// error.
//
func NewDcvMgr() *DiscoverManager {
	var dcvMgr = DiscoverManager {
		name:     DcvMgrName,
		tep:      nil,
		ptnMe:    nil,
		ptnTab:   nil,
		ptnPeMgr: nil,
		more:     config.MaxOutbounds,
	}

	dcvMgr.tep = dcvMgr.dcvMgrProc

	return &dcvMgr
}

//
// Entry point exported to shceduler
//
func (dcvMgr *DiscoverManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dcvMgr.tep(ptn, msg)
}

//
// Discover manager entry
//
func (dcvMgr *DiscoverManager)dcvMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno DcvMgrErrno = DcvMgrEnoNone

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = dcvMgr.DcvMgrPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = dcvMgr.DcvMgrPoweroff(ptn)

	case sch.EvDcvFindNodeReq:
		eno = dcvMgr.DcvMgrFindNodeReq(msg.Body.(*sch.MsgDcvFindNodeReq))

	case sch.EvTabRefreshRsp:
		eno = dcvMgr.DcvMgrTabRefreshRsp(msg.Body.(*sch.MsgTabRefreshRsp))

	default:
		log.LogCallerFileLine("DcvMgrProc: invalid message: %d", msg.Id)
		return sch.SchEnoUserTask
	}

	if eno != DcvMgrEnoNone {
		log.LogCallerFileLine("DcvMgrProc: errors, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweron handler
//
func (dcvMgr *DiscoverManager)DcvMgrPoweron(ptn interface{}) DcvMgrErrno {

	var eno sch.SchErrno

	dcvMgr.ptnMe = ptn
	dcvMgr.sdl = sch.SchGetScheduler(ptn)
	sdl := dcvMgr.sdl

	if eno, dcvMgr.ptnTab = sdl.SchGetTaskNodeByName(sch.TabMgrName); eno != sch.SchEnoNone {
		log.LogCallerFileLine("DcvMgrPoweron: get task node failed, task: %s", sch.TabMgrName)
		return DcvMgrEnoScheduler
	}

	if eno, dcvMgr.ptnPeMgr = dcvMgr.sdl.SchGetTaskNodeByName(sch.PeerMgrName); eno != sch.SchEnoNone {
		log.LogCallerFileLine("DcvMgrPoweron: get task node failed, task: %s", sch.PeerMgrName)
		return DcvMgrEnoScheduler
	}

	if dcvMgr.ptnMe == nil || dcvMgr.ptnTab == nil || dcvMgr.ptnPeMgr == nil {
		log.LogCallerFileLine("DcvMgrPoweron: internal errors, invalid task node pointers")
		return DcvMgrEnoScheduler
	}

	return DcvMgrEnoNone
}


//
// Poweroff handler
//
func (dcvMgr *DiscoverManager)DcvMgrPoweroff(ptn interface{}) DcvMgrErrno {

	sdl := sch.SchGetScheduler(ptn)
	if eno := sdl.SchTaskDone(ptn, sch.SchEnoKilled); eno != sch.SchEnoNone {

		log.LogCallerFileLine("DcvMgrPoweroff: done task failed, eno: %d", eno)
		return DcvMgrEnoScheduler
	}

	log.LogCallerFileLine("DcvMgrPoweroff: task done")

	return DcvMgrEnoNone
}

//
// FindNode request handler
//
func (dcvMgr *DiscoverManager)DcvMgrFindNodeReq(req *sch.MsgDcvFindNodeReq) DcvMgrErrno {

	//
	// When peer manager task considers that more peers needed, it then send FindNode
	// request to here the discover task to ask for more, see function peMgrAsk4More
	// for details about please.
	//
	// When EvDcvFindNodeReq received, we should requtst the table manager task to
	// refresh itself to get more by sending sch.EvTabRefreshReq to it, and we would
	// responsed by sch.EvTabRefreshRsp, with message type as sch.MsgTabRefreshRsp.
	// And then, we can response the peer manager task with sch.EvDcvFindNodeRsp event
	// with sch.MsgDcvFindNodeRsp message.
	//

	var schMsg = sch.SchMessage{}
	var reqRefresh = sch.MsgTabRefreshReq{nil,nil}

	//
	// Update "more" counter
	//

	if dcvMgr.more = req.More; dcvMgr.more <= 0 {

		log.LogCallerFileLine("DcvMgrFindNodeReq: " +
			"no more needed, more: %d",
			dcvMgr.more)

		return DcvMgrEnoNone
	}

	//
	// More needed, ask the table task to refresh
	//

	if eno := dcvMgr.sdl.SchMakeMessage(&schMsg, dcvMgr.ptnMe, dcvMgr.ptnTab, sch.EvTabRefreshReq, &reqRefresh);
		eno != sch.SchEnoNone {

		log.LogCallerFileLine("DcvMgrFindNodeReq: SchMakeMessage failed, eno: %d", eno)
		return DcvMgrEnoScheduler
	}

	if eno := dcvMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {

		log.LogCallerFileLine("DcvMgrFindNodeReq: " +
			"SchSendMessage failed, eno: %d, target: %s",
			eno, dcvMgr.sdl.SchGetTaskName(dcvMgr.ptnTab))

		return DcvMgrEnoScheduler
	}

	return DcvMgrEnoNone
}

//
// Table refreshed response handler
//
func (dcvMgr *DiscoverManager)DcvMgrTabRefreshRsp(rsp *sch.MsgTabRefreshRsp) DcvMgrErrno {

	//
	// We receive the response about event sch.EvTabRefreshReq we hand sent to table
	// manager task. For more, see comments aboved in function DcvMgrFindNodeReq pls.
	//

	if dcvMgr.more <= 0 {

		log.LogCallerFileLine("DcvMgrTabRefreshRsp: " +
			"discarded, no more needed, more: %d, rsp: %s",
			dcvMgr.more,
			fmt.Sprintf("%+v", rsp))

		return DcvMgrEnoNone
	}

	//
	// Report nodes to peer manager and update "more" counter
	//

	var schMsg = sch.SchMessage{}
	var r = sch.MsgDcvFindNodeRsp{}
	r.Nodes = rsp.Nodes

	if eno := dcvMgr.sdl.SchMakeMessage(&schMsg, dcvMgr.ptnMe, dcvMgr.ptnPeMgr, sch.EvDcvFindNodeRsp, &r);
	eno != sch.SchEnoNone {

		log.LogCallerFileLine("DcvMgrTabRefreshRsp: SchMakeMessage failed, eno: %d", eno)
		return DcvMgrEnoScheduler
	}

	if eno := dcvMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {

		log.LogCallerFileLine("DcvMgrTabRefreshRsp: " +
			"SchSendMessage failed, eno: %d, target: %s",
			eno, dcvMgr.sdl.SchGetTaskName(dcvMgr.ptnTab))

		return DcvMgrEnoScheduler
	}

	//
	// Update "more" counter
	//

	dcvMgr.more -= len(r.Nodes)

	return DcvMgrEnoNone
}




