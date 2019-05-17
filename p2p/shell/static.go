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

	config "github.com/yeeco/gyee/p2p/config"
	dht "github.com/yeeco/gyee/p2p/dht"
	dcv "github.com/yeeco/gyee/p2p/discover"
	ngb "github.com/yeeco/gyee/p2p/discover/neighbor"
	tab "github.com/yeeco/gyee/p2p/discover/table"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	nat "github.com/yeeco/gyee/p2p/nat"
	peer "github.com/yeeco/gyee/p2p/peer"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	log "github.com/yeeco/gyee/log"
)

//
// debug
//
type staticTaskLogger struct {
	debug__ bool
}

var stLog = staticTaskLogger{
	debug__: false,
}

func SwitchStaticDebugFlag(flag bool) {
	stLog.debug__ = flag
}

func (log staticTaskLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// watch dog is not implemented
//
var noDog = sch.SchWatchDog{
	HaveDog: false,
}

//
// Create description about static tasks
//

type P2pType = config.P2pAppType

func P2pCreateStaticTaskTab(what P2pType) []sch.TaskStaticDescription {

	//
	// Following are static tasks for ycp2p module internal. Notice that fields of struct
	// sch.TaskStaticDescription like MbSize, Wd, Flag will be set to default values internal
	// scheduler, please see function schimplSchedulerStart for details pls.
	// notice: nat manager is invoked in both chain application and dht application, since
	// these applications are hosted in different schedulers, one can launch twos.
	//

	if what == config.P2P_TYPE_CHAIN {

		return []sch.TaskStaticDescription{
			{Name: sch.NatMgrName, Tep: nat.NewNatMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dcv.DcvMgrName, Tep: dcv.NewDcvMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: tab.NdbcName, Tep: tab.NewNdbCleaner(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: ngb.LsnMgrName, Tep: ngb.NewLsnMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: ngb.NgbMgrName, Tep: ngb.NewNgbMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: tab.TabMgrName, Tep: tab.NewTabMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: peer.PeerLsnMgrName, Tep: peer.NewLsnMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: sch.PeerMgrName, Tep: peer.NewPeerMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: sch.ShMgrName, Tep: NewShellMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
		}

	} else if what == config.P2P_TYPE_DHT {

		return []sch.TaskStaticDescription{
			{Name: sch.NatMgrName, Tep: nat.NewNatMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.DhtMgrName, Tep: dht.NewDhtMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.DsMgrName, Tep: dht.NewDsMgr(), MbSize: dht.DsMgrMailboxSize, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.LsnMgrName, Tep: dht.NewLsnMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.PrdMgrName, Tep: dht.NewPrdMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.QryMgrName, Tep: dht.NewQryMgr(), MbSize: dht.QryMgrMailboxSize, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.RutMgrName, Tep: dht.NewRutMgr(), MbSize: -1, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: dht.ConMgrName, Tep: dht.NewConMgr(), MbSize: dht.ConMgrMailboxSize, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
			{Name: sch.DhtShMgrName, Tep: NewDhtShellMgr(), MbSize: ShMgrMailboxSize, DieCb: nil, Wd: noDog, Flag: sch.SchCreatedSuspend},
		}
	}

	stLog.Debug("P2pCreateStaticTaskTab: invalid type: %d", what)

	return nil
}

//
// Poweron order of static user tasks for chain application.
// Notice: there are some dependencies between the tasks, one should check them
// to modify this table if necessary.
//
var taskStaticPoweronOrder4Chain = []string{
	nat.NatMgrName,
	dcv.DcvMgrName,
	tab.NdbcName,
	ngb.LsnMgrName,
	ngb.NgbMgrName,
	tab.TabMgrName,
	sch.PeerMgrName,
	peer.PeerLsnMgrName,
	sch.ShMgrName,
}

//
// Poweroff order of static user tasks for chain application.
// Notice: there are some dependencies between the tasks, one should check them
// to modify this table if necessary.
//
var taskStaticPoweroffOrder4Chain = []string{
	nat.NatMgrName,
	sch.ShMgrName,
	dcv.DcvMgrName,
	tab.NdbcName,
	sch.PeerMgrName,
	ngb.LsnMgrName,
	ngb.NgbMgrName,
	peer.PeerLsnMgrName,
	tab.TabMgrName,
}

//
// Poweron order of static user tasks for dht application
// Notice: there are some dependencies between the tasks, one should check them
// to modify this table if necessary.
//
var taskStaticPoweronOrder4Dht = []string{
	nat.NatMgrName,
	dht.DhtMgrName,
	dht.DsMgrName,
	dht.ConMgrName,
	dht.QryMgrName,
	dht.PrdMgrName,
	dht.RutMgrName,
	dht.LsnMgrName,
	sch.DhtShMgrName,
}

//
// Poweroff order of static user tasks for dht application
// Notice: there are some dependencies between the tasks, one should check them
// to modify this table if necessary.
//
var taskStaticPoweroffOrder4Dht = []string{
	nat.NatMgrName,
	sch.DhtShMgrName,
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
func P2pStart(sdl *sch.Scheduler) sch.SchErrno {

	//
	// Start all static tasks
	//

	var eno sch.SchErrno

	what := P2pType(sdl.SchGetAppType())
	log.Infof("P2pStart: what: %d, inst: %s", what, sdl.SchGetP2pCfgName())

	switch what {

	case config.P2P_TYPE_CHAIN:
		eno, _ = sdl.SchSchedulerStart(P2pCreateStaticTaskTab(what), taskStaticPoweronOrder4Chain)

	case config.P2P_TYPE_DHT:
		eno, _ = sdl.SchSchedulerStart(P2pCreateStaticTaskTab(what), taskStaticPoweronOrder4Dht)

	case config.P2P_TYPE_ALL:
		log.Errorf("P2pStart: not supported type: %d", what)
		return sch.SchEnoNotImpl

	default:
		log.Errorf("P2pStart: invalid application type: %d", what)
		return sch.SchEnoParameter
	}

	if eno != sch.SchEnoNone {
		log.Errorf("P2pStart: failed, eno: %d", eno)
		return eno
	}

	//
	// Check peer manager init result, would be blocked until its initialization
	// procedure ended.
	//

	if what == config.P2P_TYPE_CHAIN {

		var pmEno peer.PeMgrErrno

		peMgr := sdl.SchGetTaskObject(sch.PeerMgrName).(*peer.PeerManager)
		log.Errorf("P2pStart: wait peer manager inited, inst: %s", sdl.SchGetP2pCfgName())
		pmEno = peMgr.PeMgrInited()
		log.Errorf("P2pStart: get out, peEno: %d, inst: %s", pmEno, sdl.SchGetP2pCfgName())

		if pmEno != peer.PeMgrEnoNone {
			log.Errorf("P2pStart: pmEno: %d", pmEno)
			return sch.SchEnoUserTask
		}

		//
		// start peer manager
		//

		pmEno = peMgr.PeMgrStart()

		if pmEno != peer.PeMgrEnoNone {
			log.Errorf("P2pStart: PeMgrStart failed, pmEno: %d", pmEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Stop p2p instance
//
func P2pStop(sdl *sch.Scheduler, ch chan bool) sch.SchErrno {

	staticTasks := make([]string, 0)
	powerOff := sch.SchMessage{
		Id: sch.EvSchPoweroff,
	}

	p2pInstName := sdl.SchGetP2pCfgName()
	appType := sdl.SchGetAppType()
	log.Infof("P2pStop: inst: %s, total tasks: %d", p2pInstName, sdl.SchGetTaskNumber())

	if P2pType(appType) == config.P2P_TYPE_CHAIN {
		staticTasks = taskStaticPoweroffOrder4Chain
	} else if P2pType(appType) == config.P2P_TYPE_DHT {
		staticTasks = taskStaticPoweroffOrder4Dht
	} else {
		log.Infof("P2pStop: inst: %s, invalid application type: %d", p2pInstName, appType)
		return sch.SchEnoMismatched
	}

	sdl.SchSetPoweroffStage()

	for loop := 0; loop < len(staticTasks); loop++ {
		taskName := staticTasks[loop]
		powerOff.TgtName = taskName
		if sdl.SchTaskExist(taskName) != true {
			log.Infof("P2pStop: inst: %s, type: %d, task not exist: %s", p2pInstName, appType, taskName)
			continue
		}

		log.Infof("P2pStop: EvSchPoweroff will be sent to inst: %s, type: %d, task: %s",
			p2pInstName, appType, taskName)

		if eno := sdl.SchSendMessageByName(taskName, sch.RawSchTaskName, &powerOff); eno != sch.SchEnoNone {
			log.Infof("P2pStop: SchSendMessageByName failed, inst: %s, type: %d, eno: %d, task: %s",
				p2pInstName, appType, eno, taskName)
		} else {
			log.Infof("P2pStop: send EvSchPoweroff ok, inst: %s, type: %d, eno: %d, task: %s",
				p2pInstName, appType, eno, taskName)
			for sdl.SchTaskExist(taskName) {
				log.Infof("P2pStop: waiting inst: %s, type: %d, task: %s", p2pInstName, appType, taskName)
				time.Sleep(time.Millisecond * 500)
			}
			log.Infof("P2pStop: done, inst: %s, type: %d, task: %s", p2pInstName, appType, taskName)
		}
	}

	log.Infof("P2pStop: inst: %s, type: %d, total tasks: %d", p2pInstName, appType, sdl.SchGetTaskNumber())
	log.Infof("P2pStop: inst: %s, wait all tasks to be done ...", p2pInstName)

	if true {
		seconds := 0
		for {
			time.Sleep(time.Second)
			seconds++
			tasks := sdl.SchGetTaskNumber()
			if tasks == 0 {
				log.Infof("P2pStop: inst: %s, type: %d, all tasks are done", p2pInstName, appType)
				break
			}
			tkNames := sdl.SchShowTaskName()
			log.Infof("P2pStop: wait seconds: %d, inst: %s, type: %d, remain tasks: %d, names: %s",
				seconds, p2pInstName, appType, tasks, tkNames)
		}
	} else {
		time.Sleep(time.Second * 2)
	}

	ch <- true

	return sch.SchEnoNone
}
