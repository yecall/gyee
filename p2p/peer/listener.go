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
	"net"
	"fmt"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	log		"github.com/yeeco/gyee/p2p/logger"
	"sync"
)

const PeerLsnMgrName = sch.PeerLsnMgrName

type ListenerManager struct {
	sdl			*sch.Scheduler				// pointer to scheduler
	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	ptn			interface{}					// the listner task node pointer
	ptnPeerMgr	interface{}					// the peer manager task node pointer
	cfg			*config.Cfg4PeerListener	// configuration
	listener	net.Listener				// listener of net
	listenAddr	*net.TCPAddr				// listen address
	accepter	*acceptTskCtrlBlock			// pointer to accepter
}

func NewLsnMgr() *ListenerManager {
	var lsnMgr = ListenerManager {
		name: PeerLsnMgrName,
	}
	lsnMgr.tep = lsnMgr.lsnMgrProc
	return &lsnMgr
}

func (lsnMgr *ListenerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return lsnMgr.tep(ptn, msg)
}

func (lsnMgr *ListenerManager)lsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
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
	default:
		log.LogCallerFileLine("LsnMgrProc: invalid message: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

func (lsnMgr *ListenerManager)lsnMgrPoweron(ptn interface{}) sch.SchErrno {
	// if does not accept inbound, done this task
	sdl := sch.SchGetScheduler(ptn)
	if sdl.SchGetP2pConfig().NoAccept == true {
		log.LogCallerFileLine("lsnMgrPoweron: do not accept, done ...")
		return sdl.SchTaskDone(ptn, sch.SchEnoNone)
	}

	var eno sch.SchErrno
	lsnMgr.ptn = ptn
	lsnMgr.sdl = sch.SchGetScheduler(ptn)
	if eno, lsnMgr.ptnPeerMgr = lsnMgr.sdl.SchGetTaskNodeByName(PeerMgrName); eno != sch.SchEnoNone {
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

func (lsnMgr *ListenerManager)lsnMgrSetupListener() sch.SchErrno {
	var err error
	lsnAddr := fmt.Sprintf("%s:%d", lsnMgr.cfg.IP.String(), lsnMgr.cfg.Port)
	if lsnMgr.listener, err = net.Listen("tcp", lsnAddr); err != nil {
		log.LogCallerFileLine("lsnMgrSetupListener: listen failed, addr: %s, err: %s", lsnAddr, err.Error())
		return sch.SchEnoOS
	}
	lsnMgr.listenAddr = lsnMgr.listener.Addr().(*net.TCPAddr)
	log.LogCallerFileLine("lsnMgrSetupListener: task inited ok, listening address: %s", lsnMgr.listenAddr.String())
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager)lsnMgrPoweroff(ptn interface{}) sch.SchErrno {
	log.LogCallerFileLine("lsnMgrPoweroff: task will be done, name: %s", lsnMgr.sdl.SchGetTaskName(ptn))
	if _, ptn := lsnMgr.sdl.SchGetTaskNodeByName(PeerAccepterName); ptn != nil {
		lsnMgr.lsnMgrStop()
	}
	return lsnMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}

func (lsnMgr *ListenerManager)lsnMgrStart() sch.SchErrno {
	log.LogCallerFileLine("lsnMgrStart: try to create accept task ...")
	if eno := lsnMgr.lsnMgrSetupListener(); eno != sch.SchEnoNone {
		log.LogCallerFileLine("lsnMgrStart: setup listener failed, eno: %d", eno)
		return eno
	}

	var accepter = acceptTskCtrlBlock {
		sdl:		lsnMgr.sdl,
		lsnMgr:		lsnMgr,
		event:		sch.SchEnoNone,
	}
	accepter.tep = accepter.peerAcceptProc
	lsnMgr.accepter = &accepter

	var tskDesc = sch.SchTaskDescription{
		Name:		PeerAccepterName,
		MbSize:		0,
		Ep:			&accepter,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
	}

	if eno, ptn := lsnMgr.sdl.SchCreateTask(&tskDesc); eno != sch.SchEnoNone {
		log.LogCallerFileLine("lsnMgrStart: SchCreateTask failed, eno: %d, ptn: %X",
			eno, ptn.(*interface{}))
		return sch.SchEnoInternal
	}
	peMgr := lsnMgr.sdl.SchGetUserTaskIF(sch.PeerMgrName).(*PeerManager)
	peMgr.accepter = &accepter

	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager)lsnMgrStop() sch.SchErrno {
	log.LogCallerFileLine("lsnMgrStop: listner will be closed")
	if lsnMgr.accepter != nil {
		lsnMgr.accepter.lockTcb.Lock()
		defer lsnMgr.accepter.lockTcb.Unlock()
		lsnMgr.accepter.event = sch.SchEnoKilled
		lsnMgr.accepter.listener = nil
	}

	if err := lsnMgr.listener.Close(); err != nil {
		log.LogCallerFileLine("lsnMgrStop: try to close listner fialed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	log.LogCallerFileLine("lsnMgrStop: listner closed ok")
	lsnMgr.listener = nil
	return sch.SchEnoNone
}

// Accepter task
const PeerAccepterName = sch.PeerAccepterName
type acceptTskCtrlBlock struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	tep			sch.SchUserTaskEp	// entry
	lsnMgr		*ListenerManager	// pointer to listener manager
	ptnPeMgr	interface{}			// pointer to peer manager task node
	ptnLsnMgr	interface{}			// pointer to listener manager task node
	listener	net.Listener		// the listener
	event		sch.SchErrno		// event fired
	curError	error				// current error fired
	lockTcb		sync.Mutex			// lock to protect this control block
	lockAccept	sync.Mutex			// lock to pause/resume acception
}

type msgConnAcceptedInd struct {
	conn		net.Conn			// underlying network connection
	localAddr	*net.TCPAddr		// local tcp address
	remoteAddr	*net.TCPAddr		// remote tcp address
}

func (accepter *acceptTskCtrlBlock)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return accepter.tep(ptn, msg)
}

func (accepter *acceptTskCtrlBlock)peerAcceptProc(ptn interface{}, _ *sch.SchMessage) sch.SchErrno {
	_, accepter.ptnLsnMgr = accepter.sdl.SchGetTaskNodeByName(PeerLsnMgrName)
	if accepter.ptnLsnMgr == nil {
		log.LogCallerFileLine("PeerAcceptProc: invalid listener manager task pointer")
		accepter.sdl.SchTaskDone(ptn, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}

	_, accepter.ptnPeMgr = accepter.sdl.SchGetTaskNodeByName(PeerMgrName)
	if accepter.ptnPeMgr == nil {
		log.LogCallerFileLine("PeerAcceptProc: invalid peer manager task pointer")
		accepter.sdl.SchTaskDone(ptn, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}

	accepter.listener = accepter.lsnMgr.listener
	if accepter.listener == nil {
		log.LogCallerFileLine("PeerAcceptProc: invalid listener, done accepter")
		accepter.sdl.SchTaskDone(ptn, sch.SchEnoInternal)
		return sch.SchEnoInternal
	}

	accepter.event = sch.EvSchNull
	accepter.curError = nil
	log.LogCallerFileLine("PeerAcceptProc: inited ok, tring to accept ...")

acceptLoop:

	for {
		// lock to know if we are allowed to accept
		accepter.lockAccept.Lock()
		// Check if had been kill by manager: we first obtain the lock then check the listener and
		// event to see if we hav been killed, if not, we backup the listener for later accept operation
		// and free the lock. See function lsnMgrStop for more please.
		// Notice: seems we can apply "chan" to implement the "stop" logic moer better than what we
		// do currently.
		accepter.lockTcb.Lock()
		if accepter.listener == nil || accepter.event != sch.SchEnoNone {
			log.LogCallerFileLine("PeerAcceptProc: break the loop, for we might have been killed")
			break acceptLoop
		}
		listener := accepter.listener
		accepter.lockTcb.Unlock()
		accepter.lockAccept.Unlock()

		// Try to accept. Since we had never set deadline for the listener, we
		// would work in a blocked mode; and if here the manager had close the
		// listener, accept would get errors from underlying network.
		conn, err := listener.Accept()
		log.LogCallerFileLine("%s", "PeerAcceptProc: get out from Accept()")

		// Lock the control block to access
		accepter.lockTcb.Lock()
		if err != nil && !err.(net.Error).Temporary() {
			log.LogCallerFileLine("PeerAcceptProc: break loop for non-temporary error while accepting," +
				"err: %s", err.Error())
			accepter.curError = err
			break acceptLoop
		}
		// Check connection accepted
		if conn == nil {
			log.LogCallerFileLine("PeerAcceptProc: break loop for null connection accepted without errors")
			accepter.event = sch.EvSchException
			break acceptLoop
		}
		log.LogCallerFileLine("PeerAcceptProc: accept one: %s", conn.RemoteAddr().String())

		// Connection got, hand it up to peer manager task, notice that we will continue the loop
		// event when we get errors to make and send the message to peer manager, see bellow.
		var msg = sch.SchMessage{}
		var msgBody = msgConnAcceptedInd {
			conn: 		conn,
			localAddr:	conn.LocalAddr().(*net.TCPAddr),
			remoteAddr:	conn.RemoteAddr().(*net.TCPAddr),
		}
		accepter.sdl.SchMakeMessage(&msg, ptn, accepter.ptnPeMgr, sch.EvPeLsnConnAcceptedInd, &msgBody)
		accepter.sdl.SchSendMessage(&msg)

		// unlock the control block
		accepter.lockTcb.Unlock()
	}

	// Notice: when loop is broken to here, the Lock is still obtained by us,
	// see above pls, do not lock again.
	// Here we get out! We should check what had happened to break the loop
	// for accepting above.
	if accepter.curError != nil && accepter.event != sch.SchEnoNone {
		// This is the normal case: the loop is broken by manager task killing the accepter,
		// in this case, the accepter.listener will be closed by manager so we get errors in
		// accepting, see event sch.EvPeLsnStopReq handler for more pls.
		log.LogCallerFileLine("PeerAcceptProc: broken for event: %d", accepter.event)
		accepter.sdl.SchTaskDone(ptn, accepter.event)
		accepter.lockTcb.Unlock()
		return accepter.event
	}

	// Abnormal case, we should never come here, debug out and then done the
	// accepter task.
	if accepter.curError != nil {
		log.LogCallerFileLine("PeerAcceptProc: abnormal exit, event: %d, err: %s",
			accepter.event, accepter.curError.Error())
	} else {
		log.LogCallerFileLine("PeerAcceptProc: abnormal exit, event: %d, err: nil",
			accepter.event)
	}

	accepter.lockTcb.Unlock()
	accepter.sdl.SchTaskDone(ptn, sch.SchEnoUnknown)
	return sch.SchEnoUnknown
}

func (accepter *acceptTskCtrlBlock)PauseAccept() bool {
	log.LogCallerFileLine("PauseAccept: try to pause inbound")
	accepter.lockAccept.Lock()
	return true
}

func (accepter *acceptTskCtrlBlock)ResumeAccept() bool {
	log.LogCallerFileLine("ResumeAccept: try to resume inbound")
	accepter.lockAccept.Unlock()
	return true
}
