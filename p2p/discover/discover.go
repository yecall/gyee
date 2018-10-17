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
	config	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
)

// errno
const (
	DcvMgrEnoNone		= iota
	DcvMgrEnoParameter
	DcvMgrEnoScheduler
)

type DcvMgrErrno int

// Discover manager
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

func (dcvMgr *DiscoverManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dcvMgr.tep(ptn, msg)
}

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

func (dcvMgr *DiscoverManager)DcvMgrPoweron(ptn interface{}) DcvMgrErrno {
	var eno sch.SchErrno
	dcvMgr.ptnMe = ptn
	dcvMgr.sdl = sch.SchGetScheduler(ptn)
	sdl := dcvMgr.sdl

	// if it's a static type, no discover manager needed
	if sdl.SchGetP2pConfig().NetworkType == config.P2pNetworkTypeStatic {
		log.LogCallerFileLine("DcvMgrPoweron: static type, dcvMgr is not needed")
		sdl.SchTaskDone(ptn, sch.SchEnoNone)
		return DcvMgrEnoNone
	}

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

func (dcvMgr *DiscoverManager)DcvMgrPoweroff(ptn interface{}) DcvMgrErrno {
	log.LogCallerFileLine("DcvMgrPoweroff: task will be done, name: %s", dcvMgr.name)
	if dcvMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled) != sch.SchEnoNone {
		return DcvMgrEnoScheduler
	}
	return DcvMgrEnoNone
}

func (dcvMgr *DiscoverManager)DcvMgrFindNodeReq(req *sch.MsgDcvFindNodeReq) DcvMgrErrno {
	var schMsg = sch.SchMessage{}
	var reqRefresh = sch.MsgTabRefreshReq{req.Snid,nil,nil}
	if dcvMgr.more = req.More; dcvMgr.more <= 0 {
		log.LogCallerFileLine("DcvMgrFindNodeReq: no more needed, subnet: %x, more: %d",
			reqRefresh.Snid, dcvMgr.more)
		return DcvMgrEnoNone
	}
	dcvMgr.sdl.SchMakeMessage(&schMsg, dcvMgr.ptnMe, dcvMgr.ptnTab, sch.EvTabRefreshReq, &reqRefresh)
	dcvMgr.sdl.SchSendMessage(&schMsg)
	return DcvMgrEnoNone
}

func (dcvMgr *DiscoverManager)DcvMgrTabRefreshRsp(rsp *sch.MsgTabRefreshRsp) DcvMgrErrno {
	if dcvMgr.more <= 0 {
		// since nodes reported to peer manager might be useless for these nods might
		// be duplicated ones in current implement, we should improve this later.
		// return DcvMgrEnoNone
	}
	var schMsg = sch.SchMessage{}
	var r = sch.MsgDcvFindNodeRsp{
		Snid:	rsp.Snid,
		Nodes:	rsp.Nodes,
	}
	dcvMgr.sdl.SchMakeMessage(&schMsg, dcvMgr.ptnMe, dcvMgr.ptnPeMgr, sch.EvDcvFindNodeRsp, &r)
	dcvMgr.sdl.SchSendMessage(&schMsg)
	dcvMgr.more -= len(r.Nodes)
	return DcvMgrEnoNone
}




