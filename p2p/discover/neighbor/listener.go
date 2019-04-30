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
	"crypto/ecdsa"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	config "github.com/yeeco/gyee/p2p/config"
	umsg "github.com/yeeco/gyee/p2p/discover/udpmsg"
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

// the listener task name
const LsnMgrName = sch.NgbLsnName

type listenerConfig struct {
	IP        net.IP        // IP
	UDP       uint16        // UDP port number
	TCP       uint16        // TCP port number
	ID        config.NodeID // node identity: the public key
	CheckAddr bool          // check the address reported against the source address
}

type ListenerManager struct {
	sdl       *sch.Scheduler    // pointer to scheduler
	name      string            // name
	tep       sch.SchUserTaskEp // entry
	cfg       listenerConfig    // configuration
	conn      *net.UDPConn      // udp connection
	addr      net.UDPAddr       // real udp address
	state     int               // state
	ptnMe     interface{}       // pointer to myself task
	ptnReader interface{}       // pointer to udp reader task
	lock      sync.Mutex        // lock for stop udp reader
}

// listener manager task state
const (
	LmsNull    = iota // not be inited, configurations are all invalid
	LmsInited         // configurated but not started
	LmsStarted        // in running
	LmsStopped        // stopped, configurations are still validate
)

func NewLsnMgr() *ListenerManager {
	var lsnMgr = ListenerManager{
		name:  LsnMgrName,
		state: LmsNull,
	}
	lsnMgr.tep = lsnMgr.lsnMgrProc
	return &lsnMgr
}

func (lsnMgr *ListenerManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return lsnMgr.tep(ptn, msg)
}

func (lsnMgr *ListenerManager) lsnMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	var eno = sch.SchEnoUnknown

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = lsnMgr.procPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = lsnMgr.procPoweroff(ptn)
	case sch.EvNblStart:
		eno = lsnMgr.procStart()
	case sch.EvNblStop:
		eno = lsnMgr.procStop()
	case sch.EvNblDataReq:
		eno = lsnMgr.nblDataReq(ptn, msg.Body)
	default:
		lsnLog.Debug("LsnMgrProc: unknown message: %d", msg.Id)
		return sch.SchEnoMismatched
	}

	return eno
}

func (lsnMgr *ListenerManager) setupConfig() sch.SchErrno {
	var ptCfg *config.Cfg4UdpNgbListener = nil
	if ptCfg = config.P2pConfig4UdpNgbListener(lsnMgr.sdl.SchGetP2pCfgName()); ptCfg == nil {
		return sch.SchEnoConfig
	}
	lsnMgr.cfg.IP = ptCfg.IP
	lsnMgr.cfg.UDP = ptCfg.UDP
	lsnMgr.cfg.TCP = ptCfg.TCP
	lsnMgr.cfg.ID = ptCfg.ID
	lsnMgr.cfg.CheckAddr = ptCfg.CheckAddr
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) setupUdpConn() sch.SchErrno {
	var conn *net.UDPConn = nil
	var realAddr *net.UDPAddr = nil

	strAddr := fmt.Sprintf("%s:%d", lsnMgr.cfg.IP.String(), lsnMgr.cfg.UDP)
	udpAddr, err := net.ResolveUDPAddr("udp", strAddr)
	if err != nil {
		lsnLog.Debug("setupUdpConn: ResolveUDPAddr failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil || conn == nil {
		lsnLog.Debug("setupUdpConn: ListenUDP failed, err: %s", err.Error())
		return sch.SchEnoOS
	}

	realAddr = conn.LocalAddr().(*net.UDPAddr)
	if realAddr == nil {
		lsnLog.Debug("setupUdpConn: LocalAddr failed")
		return sch.SchEnoOS
	}
	lsnLog.Debug("setupUdpConn: real address: %s", realAddr.String())

	lsnMgr.addr = *realAddr
	lsnMgr.conn = conn
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) start() sch.SchErrno {
	var eno sch.SchErrno
	if eno = lsnMgr.canStart(); eno != sch.SchEnoNone {
		lsnLog.Debug("start: could not start, eno: %d", eno)
		return eno
	}
	msg := sch.SchMessage{}
	lsnMgr.sdl.SchMakeMessage(&msg, lsnMgr.ptnMe, lsnMgr.ptnMe, sch.EvNblStart, nil)
	lsnMgr.sdl.SchSendMessage(&msg)
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) nextState(s int) sch.SchErrno {
	lsnMgr.state = s
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) canStart() sch.SchErrno {
	if lsnMgr.state == LmsInited || lsnMgr.state == LmsStopped {
		return sch.SchEnoNone
	}
	return sch.SchEnoMismatched
}

func (lsnMgr *ListenerManager) canStop() sch.SchErrno {
	if lsnMgr.state == LmsStarted &&
		lsnMgr.ptnReader != nil &&
		lsnMgr.conn != nil {
		return sch.SchEnoNone
	}
	return sch.SchEnoMismatched
}

func (lsnMgr *ListenerManager) procPoweron(ptn interface{}) sch.SchErrno {
	var eno sch.SchErrno
	lsnMgr.ptnMe = ptn
	lsnMgr.sdl = sch.SchGetScheduler(ptn)
	sdl := lsnMgr.sdl

	if sdl.SchGetP2pConfig().NetworkType == config.P2pNetworkTypeStatic {
		lsnLog.Debug("procPoweron: static type, lsnMgr is not needed, done it ...")
		return sdl.SchTaskDone(ptn, lsnMgr.name, sch.SchEnoNone)
	}

	lsnMgr.nextState(LmsNull)
	if eno = lsnMgr.setupConfig(); eno != sch.SchEnoNone {
		lsnLog.Debug("procPoweron：setupConfig failed, eno: %d", eno)
		return eno
	}

	lsnMgr.nextState(LmsInited)
	if eno = lsnMgr.start(); eno != sch.SchEnoNone {
		lsnLog.Debug("procPoweron：start failed, eno: %d", eno)
	}

	return eno
}

func (lsnMgr *ListenerManager) procPoweroff(ptn interface{}) sch.SchErrno {
	lsnLog.Debug("procPoweroff: task will be done, name: %s", lsnMgr.sdl.SchGetTaskName(ptn))
	if eno := lsnMgr.procStop(); eno != sch.SchEnoNone {
		lsnLog.Debug("procPoweroff: procStop failed, eno: %d", eno)
		return eno
	}
	return lsnMgr.sdl.SchTaskDone(lsnMgr.ptnMe, lsnMgr.name, sch.SchEnoKilled)
}

func (lsnMgr *ListenerManager) procStart() sch.SchErrno {
	// Notice: in currently implement, event EvNblStart should be sent one time to us,
	// for no sync logic is applied between the listener and the reader task; if one
	// wants to support any start-stop sequence, he should append sync logic. Please
	// see reader task also.
	var eno = sch.SchEnoUnknown
	var ptnLoop interface{} = nil
	if eno = lsnMgr.setupUdpConn(); eno != sch.SchEnoNone {
		lsnLog.Debug("procStart：setupUdpConn failed, eno: %d", eno)
		lsnMgr.sdl.SchTaskDone(lsnMgr.ptnMe, lsnMgr.name, eno)
		return eno
	}
	var udpReader = NewUdpReader()
	udpReader.lsnMgr = lsnMgr
	udpReader.sdl = lsnMgr.sdl
	udpReader.conn = lsnMgr.conn
	udpReader.chkAddr = lsnMgr.cfg.CheckAddr
	eno, ptnLoop = lsnMgr.sdl.SchCreateTask(&udpReader.desc)
	if eno != sch.SchEnoNone {
		lsnLog.Debug("procStart: SchCreateTask failed, eno: %d, ptn: %p", eno, ptnLoop)
		return eno
	}
	lsnMgr.ptnReader = ptnLoop
	return lsnMgr.nextState(LmsStarted)
}

func (lsnMgr *ListenerManager) procStop() sch.SchErrno {
	// To stop the reader task, we check if it's started, and simply close the connection
	// if it is. Notice that we should not try to "done" the reader here for it's a task
	// without a mailbox, it's a longlong loop than one scheduled by messages, so it can
	// be really done by itself to break its' loop.
	lsnMgr.lock.Lock()
	defer lsnMgr.lock.Unlock()
	if eno := lsnMgr.canStop(); eno != sch.SchEnoNone {
		lsnLog.Debug("procStop: we can't stop, eno: %d", eno)
		return eno
	}
	lsnMgr.conn.Close()
	lsnMgr.conn = nil
	return lsnMgr.nextState(LmsStopped)
}

func (lsnMgr *ListenerManager) nblDataReq(ptn interface{}, msg interface{}) sch.SchErrno {
	_ = ptn
	req := msg.(*sch.NblDataReq)
	return lsnMgr.sendUdpMsg(req.Payload, req.TgtAddr)
}

// Reader task on UDP connection
const udpReaderName = sch.NgbReaderName
const udpMaxMsgSize = 1024 * 32

var noDog = sch.SchWatchDog{
	HaveDog: false,
}

type UdpReaderTask struct {
	sdl       *sch.Scheduler         // pointer to scheduler
	lsnMgr    *ListenerManager       // pointer to listener manager
	name      string                 // name
	tep       sch.SchUserTaskEp      // entry
	priKey    *ecdsa.PrivateKey      // local node private key
	conn      *net.UDPConn           // udp connection
	ptnMe     interface{}            // pointer to myself task
	ptnNgbMgr interface{}            // pointer to neighbor manager task
	desc      sch.SchTaskDescription // description
	udpMsg    *umsg.UdpMsg           // decode/encode wrapper
	chkAddr   bool                   // check sender ip with that reported
}

type UdpMsgInd struct {
	msgType umsg.UdpMsgType // message type
	msgBody interface{}     // message body, like Ping, Pong, ... see udpmsg.go
	from *net.UDPAddr		// underlying address
}

func NewUdpReader() *UdpReaderTask {
	var udpReader = UdpReaderTask{
		name: udpReaderName,
		tep:  nil,
		conn: nil,
		desc: sch.SchTaskDescription{
			Name:   udpReaderName,
			MbSize: 0,
			Ep:     nil,
			Wd:     &noDog,
			Flag:   sch.SchCreatedGo,
			DieCb:  nil,
		},
		udpMsg: umsg.NewUdpMsg(),
	}
	udpReader.tep = udpReader.udpReaderLoop
	udpReader.desc.Ep = &udpReader
	return &udpReader
}

func (udpReader *UdpReaderTask) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return udpReader.tep(ptn, msg)
}

func (udpReader *UdpReaderTask) udpReaderLoop(ptn interface{}, _ *sch.SchMessage) sch.SchErrno {
	var eno = sch.SchEnoNone
	buf := make([]byte, udpMaxMsgSize)
	udpReader.ptnMe = ptn
	_, udpReader.ptnNgbMgr = udpReader.sdl.SchGetUserTaskNode(NgbMgrName)
	udpReader.priKey = udpReader.sdl.SchGetP2pConfig().PrivateKey
	udpReader.udpMsg.Key = udpReader.priKey

	// We just read until errors fired from udp, for example, when
	// the mamager is asked to stop the reader, it can close the
	// connection. See function procStop for details please.
	// When a message recevied, the reader decode it to get an UDP
	// discover protocol message, it than create a protocol task to
	// deal with the message received.

_loop:
	for {
		if NgbProtoReadTimeout > 0 {
			if err := udpReader.conn.SetReadDeadline(time.Now().Add(NgbProtoReadTimeout)); err != nil {
				eno = sch.SchEnoOS
				break _loop
			}
		}
		bys, peer, err := udpReader.conn.ReadFromUDP(buf)
		if err != nil && udpReader.canErrIgnored(err) != true {
			eno = sch.SchEnoOS
			break _loop
		}
		udpReader.msgHandler(&buf, bys, peer)
	}
	// Here we get out, but this might be caused by abnormal cases than we
	// are closed by manager task, we check this: if it is the later, the
	// connection pointer held by manager must be nil, see lsnMgr.procStop
	// for details pls.
	// If it's an abnormal case that the reader task still in running, we
	// need to make it done.
	if udpReader.sdl.SchGetPoweroffStage() {
		eno = udpReader.sdl.SchTaskDone(udpReader.ptnMe, udpReader.name, eno)
		goto _udpReaderLoop_exit
	}

	udpReader.lsnMgr.lock.Lock()
	if udpReader.lsnMgr.conn == nil {
		eno = udpReader.sdl.SchTaskDone(udpReader.ptnMe, udpReader.name, eno)
		udpReader.lsnMgr.lock.Unlock()
		goto _udpReaderLoop_exit
	}

	udpReader.lsnMgr.lock.Unlock()
	eno = udpReader.lsnMgr.procStop()

_udpReaderLoop_exit:
	lsnLog.Debug("udpReaderLoop: exit ...")
	return eno
}

func (udpReader *UdpReaderTask) canErrIgnored(err error) bool {
	const WSAEMSGSIZE = syscall.Errno(10040)
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Temporary() {
			return true
		}
		if sce, ok := opErr.Err.(*os.SyscallError); ok {
			if sce.Err == WSAEMSGSIZE {
				return true
			}
		}
	}
	return false
}

func (udpReader *UdpReaderTask) msgHandler(pbuf *[]byte, len int, from *net.UDPAddr) sch.SchErrno {
	var eno umsg.UdpMsgErrno
	if eno := udpReader.udpMsg.SetRawMessage(pbuf, len, from); eno != umsg.UdpMsgEnoNone {
		return sch.SchEnoUserTask
	}
	if eno = udpReader.udpMsg.Decode(); eno != umsg.UdpMsgEnoNone {
		return sch.SchEnoUserTask
	}
	udpMsgInd := UdpMsgInd{
		msgType: udpReader.udpMsg.GetDecodedMsgType(),
		msgBody: udpReader.udpMsg.GetDecodedMsg(),
		from: from,
	}
	if eno = udpReader.udpMsg.CheckUdpMsgFromPeer(from, udpReader.chkAddr); eno != umsg.UdpMsgEnoNone {
		lsnLog.Debug("msgHandler: CheckUdpMsgFromPeer failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}
	udpReader.udpMsg.DebugMessageFromPeer()
	msg := sch.SchMessage{}
	udpReader.sdl.SchMakeMessage(&msg, udpReader.ptnMe, udpReader.ptnNgbMgr, sch.EvNblMsgInd, &udpMsgInd)
	udpReader.sdl.SchSendMessage(&msg)
	return sch.SchEnoNone
}

func (lsnMgr *ListenerManager) sendUdpMsg(buf []byte, toAddr *net.UDPAddr) sch.SchErrno {
	lsnMgr.lock.Lock()
	defer lsnMgr.lock.Unlock()
	if lsnMgr.conn == nil {
		lsnLog.Debug("sendUdpMsg: invalid UDP connection")
		return sch.SchEnoInternal
	}
	if len(buf) == 0 || toAddr == nil {
		lsnLog.Debug("sendUdpMsg: empty to send")
		return sch.SchEnoParameter
	}
	if err := lsnMgr.conn.SetWriteDeadline(time.Now().Add(NgbProtoWriteTimeout)); err != nil {
		lsnLog.Debug("sendUdpMsg: SetDeadline failed, err: %s", err.Error())
		return sch.SchEnoOS
	}
	sent, err := lsnMgr.conn.WriteToUDP(buf, toAddr)
	if err != nil {
		lsnLog.Debug("sendUdpMsg: WriteToUDP failed, err: %s", err.Error())
		return sch.SchEnoOS
	}
	if sent != len(buf) {
		lsnLog.Debug("sendUdpMsg: WriteToUDP failed, len: %d, sent: %d", len(buf), sent)
		return sch.SchEnoOS
	}
	return sch.SchEnoNone
}

func sendUdpMsg(sdl *sch.Scheduler, lsn interface{}, sender interface{}, buf []byte, toAddr *net.UDPAddr) sch.SchErrno {
	req := sch.NblDataReq{
		Payload: buf,
		TgtAddr: toAddr,
	}
	schMsg := sch.SchMessage{}
	sdl.SchMakeMessage(&schMsg, sender, lsn, sch.EvNblDataReq, &req)
	sdl.SchSendMessage(&schMsg)
	return sch.SchEnoNone
}

func SendUdpMsg(sdl *sch.Scheduler, lsn interface{}, sender interface{}, buf []byte, toAddr *net.UDPAddr) sch.SchErrno {
	return sendUdpMsg(sdl, lsn, sender, buf, toAddr)
}
