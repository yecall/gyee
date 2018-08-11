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
	"net"
	"fmt"
	"sync"
	"time"
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)

//
// Listener manager name registered in scheduler
//
const LsnMgrName = sch.DhtLsnMgrName

//
// Listener manager
//
type LsnMgr struct {
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// my name
	status		int						// status
	config		lsnMgrCfg				// configuration
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to task node of myself
	ptnConMgr	interface{}				// pointer to connection manager task node
	listener	net.Listener			// listener of net
	listenAddr	*net.TCPAddr			// listen address
	lock		sync.Mutex				// lock for forcing to get out of accept
}

//
// Tcp accept deadline
//
const lmAcceptTimeout = time.Second * 4	// so task might be block for this duration

//
// Listener manager status
//
const (
	lmsNull		= iota					// not inited
	lmsStartup							// all are ready
	lmsWorking							// in trying to accept more
	lmsPaused							// paused
	lmsStopped							// underlying network errors, listener might be destroy
)

//
// Configuration
//
type lsnMgrCfg struct {
	network		string					// network name like "tcp", "udp", only "tcp" supported currently
	ip			net.IP					// ip address
	port		uint16					// port numbers
}

//
// Create listener manager
//
func NewLsnMgr() *LsnMgr {

	lsnMgr := LsnMgr {
		sdl:		nil,
		name:		LsnMgrName,
		status:		lmsNull,
		config:		lsnMgrCfg {
			network:	"tcp",
			ip:			net.IP{127,0,0,1},
			port:		8899,
		},
		tep:		nil,
		ptnMe:		nil,
		ptnConMgr:	nil,
		listener:	nil,
		listenAddr:	nil,
	}

	lsnMgr.tep = lsnMgr.lsnMgrProc

	return &lsnMgr
}

//
// Entry point exported to shceduler
//
func (lsnMgr *LsnMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return lsnMgr.tep(ptn, msg)
}

//
// Listener manager entry
//
func (lsnMgr *LsnMgr)lsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("lsnMgrProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = lsnMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = lsnMgr.poweroff(ptn)

	case sch.EvDhtLsnMgrStartReq:
		eno = lsnMgr.startReq()

	case sch.EvDhtLsnMgrStopReq:
		eno = lsnMgr.stopReq()

	case sch.EvDhtLsnMgrPauseReq:
		eno = lsnMgr.pauseReq()

	case sch.EvDhtLsnMgrResumeReq:
		eno = lsnMgr.resumeReq()

	case sch.EvDhtLsnMgrDriveSelf:
		eno = lsnMgr.driveSelf()

	default:
		log.LogCallerFileLine("lsnMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron event handler
//
func (lsnMgr *LsnMgr)poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)
	lsnMgr.sdl = sdl
	lsnMgr.ptnMe = ptn
	_, lsnMgr.ptnConMgr = sdl.SchGetTaskNodeByName(ConMgrName)

	if lsnMgr.sdl == nil || lsnMgr.ptnMe == nil || lsnMgr.ptnConMgr == nil {
		log.LogCallerFileLine("poweron: scheduler failed")
		return sch.SchEnoInternal
	}

	cfg := config.P2pConfig4DhtLsnManager(lsnMgr.sdl.SchGetP2pCfgName())
	lsnMgr.config.network = "tcp"
	lsnMgr.config.ip = cfg.IP
	lsnMgr.config.port = cfg.PortTcp

	lsnMgr.status = lmsNull
	lsnMgr.dispStaus()

	return sch.SchEnoNone
}

//
// Poweroff event handler
//
func (lsnMgr *LsnMgr)poweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("poweroff: task will be done ...")
	lsnMgr.sdl.SchTaskDone(lsnMgr.ptnMe, sch.SchEnoKilled)
	return sch.SchEnoNone
}

//
// Start-requst event handler
//
func (lsnMgr *LsnMgr)startReq() sch.SchErrno {

	if lsnMgr.status != lmsStopped && lsnMgr.status != lmsNull {
		log.LogCallerFileLine("startReq: status mismatched: %d", lsnMgr.status)
		return sch.SchEnoUserTask
	}

	if dhtEno := lsnMgr.setupListener(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("setupReq: setupListener failed, eno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	sdl := lsnMgr.sdl
	msg := sch.SchMessage{}
	sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnMe, sch.EvDhtLsnMgrDriveSelf, nil)
	sdl.SchSendMessage(&msg)

	lsnMgr.status = lmsStartup
	lsnMgr.dispStaus()

	return sch.SchEnoNone
}

//
// Stop-request event handler
//
func (lsnMgr *LsnMgr)stopReq() sch.SchErrno {

	if lsnMgr.status == lmsNull || lsnMgr.status == lmsStopped {
		log.LogCallerFileLine("stopReq: status mismatched: %d", lsnMgr.status)
		return sch.SchEnoUserTask
	}

	if lsnMgr.listener == nil {
		log.LogCallerFileLine("stopReq: nil listener")
		return sch.SchEnoUserTask
	}

	lsnMgr.listener.Close()
	lsnMgr.listener = nil
	lsnMgr.status = lmsStopped
	lsnMgr.dispStaus()

	return sch.SchEnoNone
}

//
// Pause-request event handler
//
func (lsnMgr *LsnMgr)pauseReq() sch.SchErrno {

	if lsnMgr.status != lmsWorking {
		log.LogCallerFileLine("pauseReq: status mismatched: %d", lsnMgr.status)
		return sch.SchEnoUserTask
	}

	lsnMgr.status = lmsPaused
	lsnMgr.dispStaus()

	return sch.SchEnoNone
}

//
// Resume-request event handler
//
func (lsnMgr *LsnMgr)resumeReq() sch.SchErrno {

	if lsnMgr.status != lmsPaused {
		log.LogCallerFileLine("resumeReq: status mismatched: %d", lsnMgr.status)
		return sch.SchEnoUserTask
	}

	sdl := lsnMgr.sdl
	msg := sch.SchMessage{}
	sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnMe, sch.EvDhtLsnMgrDriveSelf, nil)
	sdl.SchSendMessage(&msg)

	lsnMgr.status = lmsWorking
	lsnMgr.dispStaus()

	return sch.SchEnoNone
}

//
// Drive self event handler
//
func (lsnMgr *LsnMgr)driveSelf() sch.SchErrno {

	//
	// to support force to get out of accept
	//

	lsnMgr.lock.Lock()
	defer lsnMgr.lock.Unlock()

	//
	// if the status is "lmsNull", it should be the first time that event
	// sch.EvDhtLsnMgrDriveSelf be received, in a "start"/"stop" round, see
	// handler for sch.EvDhtLsnMgrStartReq pls.
	//

	if lsnMgr.status == lmsNull {
		log.LogCallerFileLine("driveSelf: begig to work")
		lsnMgr.status = lmsWorking
		lsnMgr.dispStaus()
	}

	if lsnMgr.status != lmsWorking {
		log.LogCallerFileLine("driveSelf: not in working")
		return sch.SchEnoUserTask
	}

	//
	// we might be block for a duration lmAcceptTimeout, but we also provide
	// interface to force ourself to get out from accept action, see function
	// ForceAcceptOut.
	//

	lsnMgr.listener.(*net.TCPListener).SetDeadline(time.Now().Add(lmAcceptTimeout))
	con, err := lsnMgr.listener.Accept()

	if err != nil {

		log.LogCallerFileLine("driveSelf: accept error: %s", err.Error())

		lsnMgr.listener.Close()
		lsnMgr.listener = nil
		lsnMgr.status = lmsStopped
		lsnMgr.dispStaus()

		return sch.SchEnoUserTask
	}

	if con == nil {

		log.LogCallerFileLine("driveSelf: nil connection without accept error")

		lsnMgr.driveMore()

		return sch.SchEnoOS
	}

	msg := sch.SchMessage{}
	ind := sch.MsgDhtLsnMgrAcceptInd{
		Con: con,
	}

	lsnMgr.sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnConMgr, sch.EvDhtLsnMgrAcceptInd, &ind)
	lsnMgr.sdl.SchSendMessage(&msg)

	return sch.SchEnoNone
}

//
// Setup net lsitener
//
func (lsnMgr *LsnMgr)setupListener() DhtErrno {

	var err error

	network := lsnMgr.config.network
	ip := lsnMgr.config.ip.String()
	port := lsnMgr.config.port
	lsnAddr := fmt.Sprintf("%s:%d", ip, port)

	if lsnMgr.listener, err = net.Listen(network, lsnAddr); err != nil {
		log.LogCallerFileLine("setupListener: "+
			"listen failed, addr: %s, err: %s",
			lsnAddr, err.Error())
		return DhtEnoOs
	}

	lsnMgr.listenAddr = lsnMgr.listener.Addr().(*net.TCPAddr)

	log.LogCallerFileLine("setupListener: "+
		"task inited ok, listening address: %s",
		lsnMgr.listenAddr.String())

	return DhtEnoNone
}

//
// Report current status to connection maanger
//
func (lsnMgr *LsnMgr)dispStaus() DhtErrno {
	msg := sch.SchMessage{}
	ind := sch.MsgDhtLsnMgrStatusInd{Status:lsnMgr.status}
	lsnMgr.sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnConMgr, sch.EvDhtLsnMgrStatusInd, &ind)
	lsnMgr.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Drive our manager self one more times
//
func (lsnMgr *LsnMgr)driveMore() DhtErrno {
	msg := sch.SchMessage{}
	lsnMgr.sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnMe, sch.EvDhtLsnMgrDriveSelf, nil)
	lsnMgr.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

//
// Froce to get out from accept call
//
func (lsnMgr *LsnMgr) ForceAcceptOut() DhtErrno {

	lsnMgr.lock.Lock()
	defer lsnMgr.lock.Unlock()

	if lsnMgr.status == lmsNull || lsnMgr.status == lmsStopped {
		log.LogCallerFileLine("ForceAcceptOut: status mismatched: %d", lsnMgr.status)
		return DhtEnoMismatched
	}

	if lsnMgr.listener == nil {
		log.LogCallerFileLine("ForceAcceptOut: nil listener")
		return DhtEnoInternal
	}

	lsnMgr.listener.Close()
	lsnMgr.listener = nil
	lsnMgr.status = lmsNull

	return DhtEnoNone
}
