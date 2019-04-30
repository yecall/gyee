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

package peer

import (
	"fmt"
	"net"

	"github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// debug
//
type lsnMgrLogger struct {
	debug__ bool
}

var lsnLog = lsnMgrLogger{
	debug__: false,
}

func (log lsnMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// peer listen manager
//

const PeerLsnMgrName = sch.PeerLsnMgrName

type ListenerManager struct {
	sdl        *sch.Scheduler           // pointer to scheduler
	name       string                   // name
	tep        sch.SchUserTaskEp        // entry
	ptn        interface{}              // the listner task node pointer
	ptnPeerMgr interface{}              // the peer manager task node pointer
	cfg        *config.Cfg4PeerListener // configuration
	listener   net.Listener             // listener of net
	listenAddr *net.TCPAddr             // listen address
	accepter   *acceptTskCtrlBlock      // pointer to accepter
}

func NewLsnMgr() *ListenerManager {
	var lsnMgr = ListenerManager{
		name: PeerLsnMgrName,
	}
	lsnMgr.tep = lsnMgr.lsnMgrProc
	return &lsnMgr
}

func (lsnMgr *ListenerManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return lsnMgr.tep(ptn, msg)
}

func (lsnMgr *ListenerManager) lsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	var eno sch.SchErrno

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = lsnMgr.lsnMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = lsnMgr.lsnMgrPoweroff(ptn)
	case sch.EvPeLsnStartReq:
		eno = lsnMgr.lsnMgrStart()
	case sch.EvPeLsnStopReq:
		eno = lsnMgr.lsnMgrStop()
	case sch.EvPeLsnRestart:
		if eno = lsnMgr.lsnMgrStop(); eno == sch.SchEnoNone {
			eno = lsnMgr.lsnMgrStart()
		}
	default:
		lsnLog.Debug("LsnMgrProc: invalid message: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

func (lsnMgr *ListenerManager) lsnMgrPoweron(ptn interface{}) sch.SchErrno {
	// if does not accept inbound, done this task
	sdl := sch.SchGetScheduler(ptn)
	if sdl.SchGetP2pConfig().NoAccept == true {
		lsnLog.Debug("lsnMgrPoweron: do not accept, done ...")
		return sdl.SchTaskDone(ptn, lsnMgr.name, sch.SchEnoNone)
	}

	var eno sch.SchErrno
	lsnMgr.ptn = ptn
	lsnMgr.sdl = sdl
	eno, lsnMgr.ptnPeerMgr = lsnMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName);
	if eno != sch.SchEnoNone {
		return eno
	}
	if lsnMgr.ptnPeerMgr == nil {
		return sch.SchEnoInternal
	}

	lsnMgr.cfg = config.P2pConfig4PeerListener(lsnMgr.sdl.SchGetP2pCfgName())
	if lsnMgr.cfg == nil {
		return sch.SchEnoConfig
	}

	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) lsnMgrSetupListener() sch.SchErrno {
	var err error
	lsnAddr := fmt.Sprintf("%s:%d", lsnMgr.cfg.IP.String(), lsnMgr.cfg.Port)
	if lsnMgr.listener, err = net.Listen("tcp", lsnAddr); err != nil {
		lsnLog.Debug("lsnMgrSetupListener: listen failed, addr: %s, err: %s", lsnAddr, err.Error())
		return sch.SchEnoOS
	}
	lsnMgr.listenAddr = lsnMgr.listener.Addr().(*net.TCPAddr)
	lsnLog.Debug("lsnMgrSetupListener: task inited ok, listening address: %s", lsnMgr.listenAddr.String())
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) lsnMgrPoweroff(ptn interface{}) sch.SchErrno {
	lsnLog.Debug("lsnMgrPoweroff: task will be done, name: %s", lsnMgr.sdl.SchGetTaskName(ptn))
	lsnMgr.lsnMgrStop()
	return lsnMgr.sdl.SchTaskDone(ptn, lsnMgr.name, sch.SchEnoKilled)
}

func (lsnMgr *ListenerManager) lsnMgrStart() sch.SchErrno {
	// we should have remove all reference to the accpeter if we had been requested
	// to close it, see function lsnMgrStop. BUT in extreme case, the accepter task
	// might be still alive in the scheduler, so we had to check this by the name of
	// accepter with the scheduler.
	if eno, _ := lsnMgr.sdl.SchGetUserTaskNode(PeerAccepterName); eno == sch.SchEnoNone {
		return sch.SchEnoDuplicated
	}
	if lsnMgr.accepter != nil {
		return sch.SchEnoUserTask
	}
	if eno := lsnMgr.lsnMgrSetupListener(); eno != sch.SchEnoNone {
		lsnLog.Debug("lsnMgrStart: setup listener failed, eno: %d", eno)
		return eno
	}
	var accepter = acceptTskCtrlBlock{
		sdl:    lsnMgr.sdl,
		name:	PeerAccepterName,
		lsnMgr: lsnMgr,
		stopCh: make(chan bool, 1),
	}
	accepter.tep = accepter.peerAcceptProc
	lsnMgr.accepter = &accepter
	var tskDesc = sch.SchTaskDescription{
		Name:   accepter.name,
		MbSize: 0,
		Ep:     &accepter,
		Wd:     &sch.SchWatchDog{HaveDog: false},
		Flag:   sch.SchCreatedGo,
	}
	if eno, ptn := lsnMgr.sdl.SchCreateTask(&tskDesc); eno != sch.SchEnoNone {
		lsnLog.Debug("lsnMgrStart: SchCreateTask failed, eno: %d, ptn: %X",
			eno, ptn.(*interface{}))
		return sch.SchEnoInternal
	}
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) lsnMgrStop() sch.SchErrno {
	lsnLog.Debug("lsnMgrStop: listner will be closed")
	if lsnMgr.accepter == nil {
		lsnLog.Debug("lsnMgrStop: nil accepter")
		return sch.SchEnoMismatched
	}
	if len(lsnMgr.accepter.stopCh) > 0 {
		lsnLog.Debug("lsnMgrStop: stop channel is not empty")
		return sch.SchEnoMismatched
	}
	if lsnMgr.listener == nil {
		lsnLog.Debug("lsnMgrStop: nil listener")
		return sch.SchEnoUserTask
	}
	// notice: here we fire the channel to ask the accepter to stop and close
	// the listener for the accepter might be blocked in accepting currently.
	// BUT when all these done, the accepter task might be still alive in the
	// scheduler for some time.
	lsnMgr.accepter.stopCh <- true
	lsnMgr.accepter = nil
	lsnMgr.listener.Close()
	lsnMgr.listener = nil
	return sch.SchEnoNone
}

// Accepter task
const PeerAccepterName = sch.PeerAccepterName

type acceptTskCtrlBlock struct {
	sdl       *sch.Scheduler    // pointer to scheduler
	name      string			// name
	tep       sch.SchUserTaskEp // entry
	lsnMgr    *ListenerManager  // pointer to listener manager
	ptnPeMgr  interface{}       // pointer to peer manager task node
	ptnLsnMgr interface{}       // pointer to listener manager task node
	listener  net.Listener      // the listener
	stopCh    chan bool         // channel to stop accepter
}

type msgConnAcceptedInd struct {
	conn       net.Conn     // underlying network connection
	localAddr  *net.TCPAddr // local tcp address
	remoteAddr *net.TCPAddr // remote tcp address
}

func (accepter *acceptTskCtrlBlock) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return accepter.tep(ptn, msg)
}

func (accepter *acceptTskCtrlBlock) peerAcceptProc(ptn interface{}, _ *sch.SchMessage) sch.SchErrno {
	_, accepter.ptnLsnMgr = accepter.sdl.SchGetUserTaskNode(PeerLsnMgrName)
	if accepter.ptnLsnMgr == nil {
		lsnLog.Debug("PeerAcceptProc: invalid listener manager task pointer")
		accepter.sdl.SchTaskDone(ptn, accepter.name, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}

	_, accepter.ptnPeMgr = accepter.sdl.SchGetUserTaskNode(sch.PeerMgrName)
	if accepter.ptnPeMgr == nil {
		lsnLog.Debug("PeerAcceptProc: invalid peer manager task pointer")
		accepter.sdl.SchTaskDone(ptn, accepter.name, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}

	accepter.listener = accepter.lsnMgr.listener
	if accepter.listener == nil {
		lsnLog.Debug("PeerAcceptProc: invalid listener, done accepter")
		accepter.sdl.SchTaskDone(ptn, accepter.name, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}
	lsnLog.Debug("PeerAcceptProc: inited ok, tring to accept ...")

	stop := false

acceptLoop:
	for {
		select {
		case stop = <-accepter.stopCh:
			if stop {
				lsnLog.Debug("PeerAcceptProc: break the loop to stop on command")
				break acceptLoop
			}
		default:
		}

		// notice: here the listener might have been closed, or would be closed
		// while accepting, see function lsnMgrStop for more. in these cases, we
		// expect an error fired from underlying network library, so we can jump
		// out of the loop to done the accepter.

		listener := accepter.listener
		if listener == nil {
			lsnLog.Debug("PeerAcceptProc: break the loop for nil listener")
			break acceptLoop
		}

		conn, err := listener.Accept()
		if lsnLog.debug__ {
			lsnLog.Debug("PeerAcceptProc: get out from Accept()")
		}

		if err != nil && !err.(net.Error).Temporary() {
			lsnLog.Debug("PeerAcceptProc: break loop for non-temporary error while accepting, " +
				"err: %s", err.Error())
			break acceptLoop
		}

		if conn == nil {
			lsnLog.Debug("PeerAcceptProc: break loop for null connection")
			break acceptLoop
		}

		msgBody := msgConnAcceptedInd{
			conn:       conn,
			localAddr:  conn.LocalAddr().(*net.TCPAddr),
			remoteAddr: conn.RemoteAddr().(*net.TCPAddr),
		}
		msg := sch.SchMessage{}
		accepter.sdl.SchMakeMessage(&msg, ptn, accepter.ptnPeMgr, sch.EvPeLsnConnAcceptedInd, &msgBody)
		accepter.sdl.SchSendMessage(&msg)
	}

	doneFor := sch.SchEnoKilled
	if !stop {
		doneFor = sch.SchEnoOS
	}
	return accepter.sdl.SchTaskDone(ptn, accepter.name, doneFor)
}
