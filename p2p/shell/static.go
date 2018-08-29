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


package shell

import (
	"time"
	config	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	dcv		"github.com/yeeco/gyee/p2p/discover"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	ngb		"github.com/yeeco/gyee/p2p/discover/neighbor"
	peer	"github.com/yeeco/gyee/p2p/peer"
	dht		"github.com/yeeco/gyee/p2p/dht"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// watch dog is not implemented
//
var noDog = sch.SchWatchDog {
	HaveDog:false,
}

//
// Create description about static tasks
//

type P2pType int

const (
	P2P_TYPE_CHAIN	P2pType = 0
	P2P_TYPE_DHT	P2pType = 1
	P2P_TYPE_ALL	P2pType = 2
)

func P2pCreateStaticTaskTab(what P2pType) []sch.TaskStaticDescription {

	//
	// Following are static tasks for ycp2p module internal. Notice that fields of struct
	// sch.TaskStaticDescription like MbSize, Wd, Flag will be set to default values internal
	// scheduler, please see function schimplSchedulerStart for details pls.
	//

	if what == P2P_TYPE_CHAIN {

		return []sch.TaskStaticDescription{
			{Name: dcv.DcvMgrName,		Tep: dcv.NewDcvMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: tab.TabMgrName,		Tep: tab.NewTabMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: tab.NdbcName,		Tep: tab.NewNdbCleaner(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: ngb.LsnMgrName,		Tep: ngb.NewLsnMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: ngb.NgbMgrName,		Tep: ngb.NewNgbMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: peer.PeerLsnMgrName,	Tep: peer.NewLsnMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: peer.PeerMgrName,	Tep: peer.NewPeerMgr(),		MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
		}

	} else if what == P2P_TYPE_DHT {

		return []sch.TaskStaticDescription{
			{Name: dht.DhtMgrName,	Tep: dht.NewDhtMgr(),	MbSize: -1,	DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.DsMgrName,	Tep: dht.NewDsMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.LsnMgrName,	Tep: dht.NewLsnMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.PrdMgrName,	Tep: dht.NewPrdMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.QryMgrName,	Tep: dht.NewQryMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.RutMgrName,	Tep: dht.NewRutMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.ConMgrName,	Tep: dht.NewConMgr(),	MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
		}
	}

	log.LogCallerFileLine("P2pCreateStaticTaskTab: invalid type: %d", what)

	return nil
}

//
// Poweron order of static user tasks for chain application
//
var taskStaticPoweronOrder4Chain = []string {
	dcv.DcvMgrName,
	tab.TabMgrName,
	tab.NdbcName,
	ngb.LsnMgrName,
	ngb.NgbMgrName,
	peer.PeerMgrName,
	peer.PeerLsnMgrName,
}

//
// Poweron order of static user tasks for dht application
//
var taskStaticPoweronOrder4Dht = [] string {
	dht.DhtMgrName,
	dht.DsMgrName,
	dht.ConMgrName,
	dht.QryMgrName,
	dht.PrdMgrName,
	dht.RutMgrName,
	dht.LsnMgrName,
}

//
// Create p2p instance
//
func P2pCreateInstance(cfg *config.Config) (*sch.Scheduler, sch.SchErrno) {
	return sch.SchSchedulerInit(cfg)
}

//
// Start p2p instance
//
func P2pStart(sdl *sch.Scheduler, what P2pType) sch.SchErrno {

	//
	// Start all static tasks
	//

	var eno sch.SchErrno

	switch what {
	case P2P_TYPE_CHAIN:
		eno, _ = sdl.SchSchedulerStart(P2pCreateStaticTaskTab(what), taskStaticPoweronOrder4Chain)
	case P2P_TYPE_DHT:
		eno, _ = sdl.SchSchedulerStart(P2pCreateStaticTaskTab(what), taskStaticPoweronOrder4Dht)
	default:
		eno = sch.SchEnoParameter
	}

	if eno != sch.SchEnoNone {
		return eno
	}

	//
	// Check peer manager init result, would be blocked until its init
	// procedure ended.
	//

	var pmEno peer.PeMgrErrno

	peMgr := sdl.SchGetUserTaskIF(sch.PeerMgrName).(*peer.PeerManager)
	pmEno = peMgr.PeMgrInited()

	if pmEno != peer.PeMgrEnoNone {
		return sch.SchEnoUserTask
	}

	//
	// Startup peer manager
	//

	pmEno = peMgr.PeMgrStart()

	if pmEno != peer.PeMgrEnoNone {

		log.LogCallerFileLine("P2pStart: " +
			"PeMgrStart failed, eno: %d",
			pmEno)

		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Stop p2p instance
//
func P2pStop(sdl *sch.Scheduler) sch.SchErrno {

	//
	// Set power off stage first, and after that, we send poweroff message
	// to all static tasks if it's still exist.
	// Notice: some tasks might be not alived, according to the network type,
	// they might be done when they receive the poweron message.
	//

	p2pInstName := sdl.SchGetP2pCfgName()

	powerOff := sch.SchMessage {
		Id:		sch.EvSchPoweroff,
		Body:	nil,
	}

	sdl.SchSetPoweroffStage()

	for _, taskName := range taskStaticPoweronOrder4Chain {

		if sdl.SchTaskExist(taskName) != true {
			log.LogCallerFileLine("P2pStop: p2pInst: %s, task not exist: %s", p2pInstName, taskName)
			continue
		}

		if eno := sdl.SchSendMessageByName(taskName, sch.RawSchTaskName, &powerOff); eno != sch.SchEnoNone {

			log.LogCallerFileLine("P2pStop: p2pInst: %s, " +
				"SchSendMessageByName failed, eno: %d, task: %s",
				p2pInstName, eno, taskName)

		} else {

			log.LogCallerFileLine("P2pStop: p2pInst: %s, " +
				"SchSendMessageByName with EvSchPoweroff ok, eno: %d, task: %s",
				p2pInstName, eno, taskName)
		}
	}

	log.LogCallerFileLine("P2pStop: p2pInst: %s total tasks: %d", p2pInstName, sdl.SchGetTaskNumber())
	log.LogCallerFileLine("P2pStop: p2pInst: %s, wait all tasks to be done ...", p2pInstName)

	//
	// just wait all to be done
	//

	seconds := 0
	tasks := 0

	for {

		time.Sleep(time.Second )
		seconds++

		tasks = sdl.SchGetTaskNumber()

		if tasks == 0 {
			log.LogCallerFileLine("P2pStop: p2pInst: %s, all tasks are done", p2pInstName)
			break;
		}

		log.LogCallerFileLine("P2pStop: " +
			"p2pInst: %s, wait seconds: %d, remain tasks: %d",
			p2pInstName, seconds, tasks)
	}

	return sch.SchEnoNone
}
