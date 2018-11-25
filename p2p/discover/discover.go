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
	"github.com/yeeco/gyee/p2p/config"
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
type DcvMgrReconfig = sch.MsgDcvReconfigReq

// Discover manager
const DcvMgrName = sch.DcvMgrName

type DiscoverManager struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// name
	tep			sch.SchUserTaskEp	// entry
	ptnMe		interface{}			// task node pointer to myself
	ptnTab		interface{}			// task node pointer to table manager task
	ptnPeMgr	interface{}			// task node pointer to peer manager task
	more		int					// number more peers are needed
	reCfg		DcvMgrReconfig		// reconfiguration request from peer manager
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
	if sch.Debug__ {
		sdl := sch.SchGetScheduler(ptn).SchGetP2pCfgName()
		log.Debug("dcvMgrProc: sdl: %s, msg: %d", sdl, msg.Id)
	}

	var eno DcvMgrErrno
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = dcvMgr.DcvMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = dcvMgr.DcvMgrPoweroff(ptn)
	case sch.EvDcvFindNodeReq:
		eno = dcvMgr.DcvMgrFindNodeReq(msg.Body.(*sch.MsgDcvFindNodeReq))
	case sch.EvTabRefreshRsp:
		eno = dcvMgr.DcvMgrTabRefreshRsp(msg.Body.(*sch.MsgTabRefreshRsp))
	case sch.EvDcvReconfigReq:
		eno = dcvMgr.DcvMgrReconfigReq(msg.Body.(*sch.MsgDcvReconfigReq))
	default:
		log.Debug("DcvMgrProc: invalid message: %d", msg.Id)
		return sch.SchEnoUserTask
	}

	if sch.Debug__ {
		sdl := sch.SchGetScheduler(ptn).SchGetP2pCfgName()
		log.Debug("dcvMgrProc: get out, sdl: %s, msg: %d", sdl, msg.Id)
	}

	if eno != DcvMgrEnoNone {
		log.Debug("DcvMgrProc: errors, eno: %d", eno)
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
		log.Debug("DcvMgrPoweron: static type, dcvMgr is not needed")
		sdl.SchTaskDone(ptn, sch.SchEnoNone)
		return DcvMgrEnoNone
	}

	if eno, dcvMgr.ptnTab = sdl.SchGetUserTaskNode(sch.TabMgrName); eno != sch.SchEnoNone {
		log.Debug("DcvMgrPoweron: get task node failed, task: %s", sch.TabMgrName)
		return DcvMgrEnoScheduler
	}

	if eno, dcvMgr.ptnPeMgr = dcvMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName); eno != sch.SchEnoNone {
		log.Debug("DcvMgrPoweron: get task node failed, task: %s", sch.PeerMgrName)
		return DcvMgrEnoScheduler
	}

	if dcvMgr.ptnMe == nil || dcvMgr.ptnTab == nil || dcvMgr.ptnPeMgr == nil {
		log.Debug("DcvMgrPoweron: internal errors, invalid task node pointers")
		return DcvMgrEnoScheduler
	}

	return DcvMgrEnoNone
}

func (dcvMgr *DiscoverManager)DcvMgrPoweroff(ptn interface{}) DcvMgrErrno {
	log.Debug("DcvMgrPoweroff: task will be done, name: %s", dcvMgr.name)
	if dcvMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled) != sch.SchEnoNone {
		return DcvMgrEnoScheduler
	}
	return DcvMgrEnoNone
}

func (dcvMgr *DiscoverManager)DcvMgrFindNodeReq(req *sch.MsgDcvFindNodeReq) DcvMgrErrno {
	var schMsg = sch.SchMessage{}
	var reqRefresh = sch.MsgTabRefreshReq{req.Snid,nil,nil}
	if dcvMgr.more = req.More; dcvMgr.more <= 0 {
		log.Debug("DcvMgrFindNodeReq: no more needed, subnet: %x, more: %d",
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
		// be duplicated ones in current implement, we can't return here, this should
		// be improve this later.
		// return DcvMgrEnoNone
	}

	if _, inDeling := dcvMgr.reCfg.DelList[rsp.Snid]; inDeling {
		log.Debug("DcvMgrTabRefreshRsp: discarded for reconfiguration, snid: %x", rsp.Snid)
		return DcvMgrEnoNone
	}

	if _, inAdding := dcvMgr.reCfg.AddList[rsp.Snid]; !inAdding {
		log.Debug("DcvMgrTabRefreshRsp: discarded for reconfiguration, snid: %x", rsp.Snid)
		return DcvMgrEnoNone
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

func (dcvMgr *DiscoverManager)DcvMgrReconfigReq(req *sch.MsgDcvReconfigReq) DcvMgrErrno {
	dcvMgr.reCfg = *req
	return DcvMgrEnoNone
}



