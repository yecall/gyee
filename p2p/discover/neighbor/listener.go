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
	"os"
	"syscall"
	"net"
	"fmt"
	"time"
	sch		"github.com/yeeco/p2p/scheduler"
	cfg		"github.com/yeeco/p2p/config"
	umsg	"github.com/yeeco/p2p/discover/udpmsg"
	yclog	"github.com/yeeco/p2p/logger"
)

//
// Listen manager
//
const LsnMgrName = sch.NgbLsnName

type listenerConfig struct {
	IP	net.IP		// IP
	UDP	uint16		// UDP port number
	TCP	uint16		// TCP port number
	ID	cfg.NodeID	// node identity: the public key
}

type listenerManager struct {
	name		string				// name
	tep			sch.SchUserTaskEp	// entry
	cfg			listenerConfig		// configuration
	conn		*net.UDPConn		// udp connection
	addr		net.UDPAddr			// real udp address
	state		int					// state
	ptnMe		interface{}			// pointer to myself task
	ptnReader	interface{}			// pointer to udp reader task
}

//
// listener manager task state
//
const (
	LmsNull		= iota		// not be inited, configurations are all invalid
	LmsInited				// configurated but not started
	LmsStarted				// in running
	LmsStopped				// stopped, configurations are still validate
)

var lsnMgr = listenerManager{
	name:		LsnMgrName,
	tep:		nil,
	conn:		nil,
	state:		LmsNull,
	ptnMe:		nil,
	ptnReader:	nil,
}

//
// To escape the compiler "initialization loop" error
//
func init() {
	lsnMgr.tep			= LsnMgrProc
	udpReader.tep		= udpReaderLoop
	udpReader.desc.Ep	= udpReaderLoop
}

//
// Listen manager entry
//
func LsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	yclog.LogCallerFileLine("LsnMgrProc: " +
		"scheduled, sender: %s, recver: %s, msg: %d",
		sch.SchinfGetMessageSender(msg), sch.SchinfGetMessageRecver(msg), msg.Id)

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = lsnMgr.procPoweron()

	case sch.EvSchPoweroff:
		eno = lsnMgr.procPoweroff()

	case sch.EvNblStart:
		eno = lsnMgr.procStart()

	case sch.EvNblStop:
		eno = lsnMgr.procStop()

	default:
		yclog.LogCallerFileLine("LsnMgrProc: unknow message: %d", msg.Id)
		return sch.SchEnoMismatched
	}

	return eno
}

//
//Setup configuraion
//
func (mgr *listenerManager) setupConfig() sch.SchErrno {

	var ptCfg *cfg.Cfg4UdpListener = nil

	if ptCfg = cfg.P2pConfig4UdpListener(); ptCfg == nil {
		yclog.LogCallerFileLine("setupConfig: P2pConfig4UdpListener failed")
		return sch.SchEnoConfig
	}

	lsnMgr.cfg.IP	= ptCfg.IP
	lsnMgr.cfg.UDP	= ptCfg.UDP
	lsnMgr.cfg.TCP	= ptCfg.TCP
	lsnMgr.cfg.ID	= ptCfg.ID

	return sch.SchEnoNone
}

//
// Setup manager configuration, UDP conection
//
func (mgr *listenerManager)setupUdpConn() sch.SchErrno {

	var conn		*net.UDPConn = nil
	var realAddr	*net.UDPAddr = nil

	// setup udp address
	strAddr := fmt.Sprintf("%s:%d", lsnMgr.cfg.IP.String(), lsnMgr.cfg.UDP)
	udpAddr, err := net.ResolveUDPAddr("udp", strAddr)
	if err != nil {
		yclog.LogCallerFileLine("setupUdpConn: ResolveUDPAddr failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	// setup connection
	conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil || conn == nil {
		yclog.LogCallerFileLine("setupUdpConn: ListenUDP failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	// get real address
	realAddr = conn.LocalAddr().(*net.UDPAddr)
	if realAddr == nil {
		yclog.LogCallerFileLine("setupUdpConn: LocalAddr failed")
		return sch.SchEnoOS
	}
	yclog.LogCallerFileLine("setupUdpConn: real address: %s", realAddr.String())

	// backup connection and real address
	mgr.addr = *realAddr
	mgr.conn = conn

	return sch.SchEnoNone
}

//
// Start
//
func (mgr *listenerManager) start() sch.SchErrno {

	var eno sch.SchErrno
	var msg sch.SchMessage

	//
	// check if we can start
	//

	if eno = mgr.canStart(); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("start: could not start")
		return eno
	}

	//
	// send ourself a "start" message
	//

	eno = sch.SchinfMakeMessage(&msg, mgr.ptnMe, mgr.ptnMe, sch.EvNblStart, nil)
	if eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("start: " +
			"SchinfMakeMessage failed, sender: %s, recver: %s, eno: %d",
			sch.SchinfGetMessageSender(&msg),
			sch.SchinfGetMessageRecver(&msg),
			eno)
		return eno
	}

	eno = sch.SchinfSendMessage(&msg)
	if eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("start: " +
			"SchinfSendMessage failed, sender: %s, recver: %s, eno: %d",
			sch.SchinfGetMessageSender(&msg),
			sch.SchinfGetMessageRecver(&msg),
			eno)
		return eno
	}

	yclog.LogCallerFileLine("start: " +
		"send EvNblStart ok, target: %s",
		sch.SchinfGetTaskName(mgr.ptnMe))

	return sch.SchEnoNone
}

//
// Transfer to next state
//
func (mgr *listenerManager) nextState(s int) sch.SchErrno {
	yclog.LogCallerFileLine("nextState: transfer from %d to %d", lsnMgr.state, s)
	lsnMgr.state = s
	return sch.SchEnoNone
}

//
// Check if we can start
//
func (mgr *listenerManager) canStart() sch.SchErrno {
	if mgr.state == LmsInited || mgr.state == LmsStopped {
		return sch.SchEnoNone
	}
	return sch.SchEnoMismatched
}

//
// Check if we can stop
//
func (mgr *listenerManager) canStop() sch.SchErrno {

	//
	// check state, ptnReader, conn to allow a stop
	//

	if mgr.state == LmsStarted &&
		mgr.ptnReader != nil &&
		mgr.conn != nil	{
		return sch.SchEnoNone
	}

	return sch.SchEnoMismatched
}

//
// Poweron event handler
//
func (mgr *listenerManager) procPoweron() sch.SchErrno {

	var eno sch.SchErrno

	//
	// get pointer to myself
	//

	if eno, mgr.ptnMe = sch.SchinfGetTaskNodeByName(mgr.name); eno != sch.SchEnoNone || mgr.ptnMe == nil {
		if eno == sch.SchEnoNone {
			yclog.LogCallerFileLine("procPoweron: internal errors, eno mismatched")
			eno = sch.SchEnoInternal
		}
		yclog.LogCallerFileLine("procPoweron: " +
			"SchinfGetTaskNodeByName failed, name: %s, eno: %d",
			eno, mgr.name)
		return eno
	}

	//
	// update state
	//

	mgr.nextState(LmsNull)

	//
	// fetch configurations
	//

	if eno = lsnMgr.setupConfig(); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("procPoweron：setupConfig failed, eno: %d", eno)
		return eno
	}

	//
	// update state
	//

	mgr.nextState(LmsInited)

	//
	// start listening(reading on udp)
	//

	if eno = lsnMgr.start(); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("procPoweron：start failed, eno: %d", eno)
		return eno
	}

	return eno
}

//
// Poweroff event handler
//
func (mgr *listenerManager) procPoweroff() sch.SchErrno {

	//
	// Stop reader task
	//

	if eno := mgr.procStop(); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("procPoweroff: " +
			"procStop failed, eno: %d",
			eno)
	}

	//
	// Done ourselves
	//

	return sch.SchinfTaskDone(mgr.ptnMe, sch.SchEnoKilled)
}

//
// Start reading event handler
//
func (mgr *listenerManager) procStart() sch.SchErrno {

	//
	// here we create a task for udp reading loop, notice that udp connection
	// must have been established when coming here.
	//

	var eno = sch.SchEnoUnknown
	var ptnLoop interface{} = nil

	//
	// setup connection
	//

	if eno = lsnMgr.setupUdpConn(); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("procStart：setupUdpConn failed, eno: %d", eno)
		return eno
	}

	//
	// create the reader task
	//

	udpReader.conn = mgr.conn
	eno, ptnLoop = sch.SchinfCreateTask(&udpReader.desc)

	if eno != sch.SchEnoNone || ptnLoop == nil {

		yclog.LogCallerFileLine("procStart: " +
			"SchinfCreateTask failed, eno: %d, ptn: %p",
			eno, ptnLoop)

		if eno == sch.SchEnoNone {
			eno = sch.SchEnoMismatched
		}

		return eno
	}

	yclog.LogCallerFileLine("procStart: " +
		"udp reader task created ok, task: %s",
		sch.SchinfGetTaskName(ptnLoop))

	//
	// we believe reader is working, update the state. more better is to
	// update the state inside the reader before it going to its' longlong
	// loop.
	//

	mgr.ptnReader = ptnLoop

	return mgr.nextState(LmsStarted)
}

//
// stop reading event handler
//
func (mgr *listenerManager) procStop() sch.SchErrno {

	var eno sch.SchErrno

	//
	// check if we can stop
	//

	if eno = mgr.canStop(); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("procStop: we can't stop, eno: %d", eno)
		return eno
	}

	//
	// stop reader by its' pointer: the reader might be blocked currently
	// in reading, we close the connection to get it out firstly. also
	// notice that we set connection pointer to be nil to tell reader that
	// it's is closed by the manager.
	//

	mgr.conn = nil
	mgr.conn.Close()

	//
	// stop task after connection had been closed. notice that the reader
	// is in another routine than the its' task since it's a longlong loop,
	// see scheduler for details pls.
	//

	if eno = sch.SchinfStopTask(mgr.ptnReader); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("procStop: SchinfStopTask failed, eno: %d", eno)
		return eno
	}

	//
	// update manager state
	//

	return mgr.nextState(LmsStopped)
}

//
// Reader task on UDP connection
//
const udpReaderName = sch.NgbReaderName
const udpMaxMsgSize = 1024 * 32

var noDog = sch.SchWatchDog {
	HaveDog:false,
}

type udpReaderTask struct {
	name		string					// name
	tep			sch.SchUserTaskEp		// entry
	conn		*net.UDPConn			// udp connection
	ptnMe		interface{}				// pointer to myself task
	ptnNgbMgr	interface{}				// pointer to neighbor manager task
	desc		sch.SchTaskDescription	// description
}

var udpReader  = udpReaderTask {
	name:	udpReaderName,
	tep:	nil,
	conn:	nil,

	//
	// description: notice that this task would going in a dead loop, so
	// it has no chance to deal with any scheduling messages sent to it,
	// no mailbox needed for it.
	//

	desc:	sch.SchTaskDescription{
		Name:	udpReaderName,
		MbSize: 0,
		Ep:		nil,
		Wd:		&noDog,
		Flag:	sch.SchCreatedGo,
		DieCb:	nil,
	},
}


//
// EvNblMsgInd message body
//
type UdpMsgInd struct {
	msgType	umsg.UdpMsgType	// message type
	msgBody	interface{}		// message body, like Ping, Pong, ... see udpmsg.go
}

//
// Reader task entry
//
func udpReaderLoop(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	yclog.LogCallerFileLine("udpReaderLoop: the reader loop entered")

	//
	// should never not be scheduled for any messages
	//

	var _ = msg

	eno := sch.SchEnoNone
	buf := make([]byte, udpMaxMsgSize)

	//
	// get related task node pointers
	//

	udpReader.ptnMe = ptn
	eno, udpReader.ptnNgbMgr = sch.SchinfGetTaskNodeByName(NgbMgrName)

	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("udpReaderLoop: " +
			"SchinfGetTaskNodeByName failed, name: %s, eno: %d",
			NgbMgrName, eno)

		return eno
	}

	//
	// We just read until errors fired from udp, for example, when
	// the mamager is asked to stop the reader, it can close the
	// connection. See function procStop for details please.
	//
	// When a message recevied, the reader decode it to get an UDP
	// discover protocol message, it than create a protocol task to
	// deal with the message received.
	//

	yclog.LogCallerFileLine("udpReaderLoop: going into a longlong loop for reading ...")

_loop:

	for {

		//
		// try reading. only (NgbProtoReadTimeout > 0) not true, we work in
		// blocked mode while reading.
		//

		if NgbProtoReadTimeout > 0 {
			udpReader.conn.SetReadDeadline(time.Now().Add(NgbProtoReadTimeout))
		}

		bys, peer, err := udpReader.conn.ReadFromUDP(buf)

		//
		// check error
		//

		if err != nil {

			if udpReader.canErrIgnored(err) != true {

				//
				// can't be ignored, we break the loop
				//

				yclog.LogCallerFileLine("udpReaderLoop: broken, err: %s", err.Error())

				break _loop
			}
		} else {

			//
			// deal with the message
			//

			yclog.LogCallerFileLine("udpReaderLoop: bytes received: %d", bys)

			udpReader.msgHandler(&buf, bys, peer)
		}
	}

	//
	// here we get out, but this might be caused by abnormal cases than we
	// are closed by manager task, we check this: if it is the later, the
	// connection pointer held by manager must be nil, see lsnMgr.procStop
	// for details pls.
	//
	// if it's an abnormal case that the reader task still in running, we
	// need to make it done.
	//

	if lsnMgr.conn == nil {

		yclog.LogCallerFileLine("udpReaderLoop: seems we are closed by manager task")

	} else {

		yclog.LogCallerFileLine("udpReaderLoop: abnormal case, stop the task")

		(&lsnMgr).procStop()
	}

	//
	// always set connection to be nil here and we need not to do anything,
	// just exit.
	//

	yclog.LogCallerFileLine("udpReaderLoop: exit ...")

	udpReader.conn = nil

	return eno
}

//
// Check if an error can be ignored while reading
//
func (rd udpReaderTask) canErrIgnored(err error) bool {
	const WSAEMSGSIZE = syscall.Errno(10040)
	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Temporary() || opErr.Err.(*os.SyscallError).Err == WSAEMSGSIZE
	}
	return false
}

//
// Decode message
//
func (rd udpReaderTask) msgHandler(pbuf *[]byte, len int, from *net.UDPAddr) sch.SchErrno {

	//
	// We need not to interprete the message, we jsut decode it and
	// then hand it over to protocol handler task, see file neighbor.go
	// for details please.
	//

	var schEno sch.SchErrno
	var msg sch.SchMessage
	var eno umsg.UdpMsgErrno

	if eno := umsg.PtrUdpMsg.SetRawMessage(pbuf, len, from); eno != umsg.UdpMsgEnoNone {
		yclog.LogCallerFileLine("msgHandler: SetRawMessage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if eno = umsg.PtrUdpMsg.Decode(); eno != umsg.UdpMsgEnoNone {
		yclog.LogCallerFileLine("msgHandler: Decode failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	//
	// Dispatch the UDP message deocoded ok to neighbor manager task
	//

	var udpMsgInd = UdpMsgInd {
		msgType:umsg.PtrUdpMsg.GetDecodedMsgType(),
		msgBody:umsg.PtrUdpMsg.GetDecodedMsg(),
	}

	// check this message agaigst the endpoint sent it
	if umsg.PtrUdpMsg.CheckUdpMsgFromPeer(from) != true {
		yclog.LogCallerFileLine("msgHandler: invalid udp message, CheckUdpMsg failed")
		return sch.SchEnoUserTask
	}

	schEno = sch.SchinfMakeMessage(&msg, rd.ptnMe, rd.ptnNgbMgr, sch.EvNblMsgInd, &udpMsgInd)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("msgHandler: " +
			"SchinfMakeMessage failed, sender: %s, recver: %s, eno: %d",
			sch.SchinfGetMessageSender(&msg),
			sch.SchinfGetMessageRecver(&msg),
			eno)

		return schEno
	}

	schEno = sch.SchinfSendMessage(&msg)

	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("msgHandler: " +
			"SchinfSendMessage failed, sender: %s, recver: %s, eno: %d",
			sch.SchinfGetMessageSender(&msg),
			sch.SchinfGetMessageRecver(&msg),
			schEno)

		return schEno
	}

	yclog.LogCallerFileLine("msgHandler: " +
		"udp message dispatched, type: %d",
		udpMsgInd.msgType)

	return sch.SchEnoNone
}

//
// Send message: we might need a singal task to handle the sending later.
//
func sendUdpMsg(buf []byte, toAddr *net.UDPAddr) sch.SchErrno {

	if udpReader.conn == nil {
		yclog.LogCallerFileLine("sendUdpMsg: invalid UDP connection")
		return sch.SchEnoInternal
	}

	if len(buf) == 0 || toAddr == nil {
		yclog.LogCallerFileLine("")
		return sch.SchEnoParameter
	}

	if err := udpReader.conn.SetWriteDeadline(time.Now().Add(NgbProtoWriteTimeout)); err != nil {
		yclog.LogCallerFileLine("sendUdpMsg: SetDeadline failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	sent, err := udpReader.conn.WriteToUDP(buf, toAddr)

	if err != nil {
		yclog.LogCallerFileLine("sendUdpMsg: WriteToUDP failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	if sent != len(buf) {
		yclog.LogCallerFileLine("sendUdpMsg: " +
			"WriteToUDP failed, len: %d, sent: %d",
			len(buf), sent)
		return sch.SchEnoOS
	}

	yclog.LogCallerFileLine("sendUdpMsg: ok, bytes sent: %d", sent)

	return sch.SchEnoNone
}

