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
	"crypto/rand"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	log	"github.com/yeeco/gyee/p2p/logger"
	cfg "github.com/yeeco/gyee/p2p/config"
)

//
// Route manager name registered in scheduler
//
const RutMgrName = sch.DhtRutMgrName

//
// Route manager
//
type RutMgr struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// my name
	tep			sch.SchUserTaskEp	// task entry
	ptnMe		interface{}			// pointer to task node of myself
	ptnQryMgr	interface{}			// pointer to query manager task node
	bpCfg		bootstrapPolicy		// bootstrap policy configuration
	bpTid		int					// bootstrap timer identity
}

//
// Bootstrap policy configuration
//
type bootstrapPolicy struct {
	randomQryNum	int				// times to try query for a random peer identity
	period			time.Duration	// timer period to fire a bootstrap
}

var defautBspCfg = bootstrapPolicy {
	randomQryNum:	2,
	period:			time.Minute * 1,
}

//
// Create route manager
//
func NewRutMgr() *RutMgr {

	rutMgr := RutMgr{
		sdl:		nil,
		name:		RutMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnQryMgr:	nil,
		bpCfg:		defautBspCfg,
		bpTid:		sch.SchInvalidTid,
	}

	rutMgr.tep = rutMgr.rutMgrProc

	return &rutMgr
}

//
// Entry point exported to shceduler
//
func (rutMgr *RutMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return rutMgr.tep(ptn, msg)
}

//
// Discover manager entry
//
func (rutMgr *RutMgr)rutMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = rutMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = rutMgr.poweroff(ptn)

	case sch.EvDhtRutBootstrapTimer:
		eno = rutMgr.bootstarpTimerHandler()

	case sch.EvDhtRutMgrNearestReq:
		eno = rutMgr.nearestReq(msg.Body.(*sch.MsgDhtRutMgrNearestReq))

	case sch.EvDhtRutMgrUpdateReq:
		eno = rutMgr.updateReq(msg.Body.(*sch.MsgDhtRutMgrUpdateReq))

	default:
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron signal handler
//
func (rutMgr *RutMgr)poweron(ptn interface{}) sch.SchErrno {

	if ptn == nil {
		log.LogCallerFileLine("poweron: nil task node pointer")
		return sch.SchEnoParameter
	}

	var eno sch.SchErrno

	rutMgr.ptnMe = ptn
	rutMgr.sdl = sch.SchGetScheduler(ptn)
	eno, rutMgr.ptnQryMgr = rutMgr.sdl.SchGetTaskNodeByName(QryMgrName)
	
	if eno != sch.SchEnoNone || rutMgr.ptnQryMgr == nil {
		log.LogCallerFileLine("poweron: nil task node pointer")
		return eno
	}

	if dhtEno := rutMgr.getRouteConfig(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("poweron: getRouteConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if dhtEno := rutMgr.startBspTimer(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("poweron: startBspTimer failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff signal handler
//
func (rutMgr *RutMgr)poweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("poweroff: task will be done")
	return sch.SchGetScheduler(ptn).SchTaskDone(ptn, sch.SchEnoKilled)
}

//
// Bootstrap timer expired event handler
//
func (rutMgr *RutMgr)bootstarpTimerHandler() sch.SchErrno {

	sdl := rutMgr.sdl

	for loop := 0; loop < rutMgr.bpCfg.randomQryNum; loop++ {

		var msg = sch.SchMessage{}
		var req = sch.MsgDhtQryMgrQueryStartReq {
			Target: RutMgrRandomPeerId(),
		}

		sdl.SchMakeMessage(&msg, rutMgr.ptnMe, rutMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &req)
		sdl.SchSendMessage(&msg)
	}

	return sch.SchEnoNone
}

//
// Nearest peer request handler
//
func (rutMgr *RutMgr)nearestReq(req *sch.MsgDhtRutMgrNearestReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Update route table request handler
//
func (rutMgr *RutMgr)updateReq(req *sch.MsgDhtRutMgrUpdateReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Get route manager configuration
//
func (rutMgr *RutMgr)getRouteConfig() DhtErrno {

	rutCfg := cfg.P2pConfig4DhtRouteManager(RutMgrName)
	rutMgr.bpCfg.randomQryNum = rutCfg.RandomQryNum
	rutMgr.bpCfg.period = rutCfg.Period

	return DhtEnoNone
}

//
// Start bootstrap timer
//
func (rutMgr *RutMgr)startBspTimer() DhtErrno {

	var td = sch.TimerDescription {
		Name:	"dhtRutBspTimer",
		Utid:	sch.DhtRutBootstrapTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	rutMgr.bpCfg.period,
		Extra:	nil,
	}

	if eno, tid := rutMgr.sdl.SchSetTimer(rutMgr.ptnMe, &td);
	eno != sch.SchEnoNone || tid == sch.SchInvalidTid {

		log.LogCallerFileLine("startBspTimer: " +
			"SchSetTimer failed, eno: %d, tid: %d",
			eno, tid)

		return DhtEnoScheduler
	}

	return DhtEnoNone
}

//
// Stop bootstrap timer
//
func (rutMgr *RutMgr)stopBspTimer() DhtErrno {

	var dhtEno DhtErrno = DhtEnoNone

	if rutMgr.bpTid != sch.SchInvalidTid {
		if eno := rutMgr.sdl.SchKillTimer(rutMgr.ptnMe, rutMgr.bpTid); eno != sch.SchEnoNone {
			dhtEno = DhtEnoScheduler
		}
	}
	rutMgr.bpTid = sch.SchInvalidTid

	return dhtEno
}

//
// Build random node identity
//
func RutMgrRandomPeerId() cfg.NodeID {
	var nid cfg.NodeID
	rand.Read(nid[:])
	return nid
}
