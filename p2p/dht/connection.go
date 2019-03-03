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
	"fmt"
	"time"
	"net"
	"io"
	"sync"
	"strings"
	"container/list"
	lru		"github.com/hashicorp/golang-lru"
	ggio	"github.com/gogo/protobuf/io"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	nat		"github.com/yeeco/gyee/p2p/nat"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type connLogger struct {
	debug__		bool
}

var connLog = connLogger {
	debug__:	false,
}

func (log connLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Connection manager name registered in scheduler
//
const ConMgrName = sch.DhtConMgrName

//
// Peer connection status
//
type conMgrPeerConnStat = int

const (
	pcsConnNo	= iota			// not connected
	pcsConnYes					// connected in service
)

//
// Connection manager configuration
//
type conMgrCfg struct {
	local			*config.Node					// pointer to local node specification
	bootstarpNode	bool							// bootstrap node flag
	maxCon			int								// max number of connection
	minCon			int								// min number of connection
	hsTimeout		time.Duration					// handshake timeout duration
}

//
// Connection cache key and value
//
type instLruKey struct {
	peer			config.NodeID					// peer node identity
	dir				ConInstDir						// direction
}

type instLruValue *ConInst

//
// Connection manager
//
const (
	pasInNull	= iota
	pasInSwitching
)
type ConMgr struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// my name
	cfg				conMgrCfg						// configuration
	tep				sch.SchUserTaskEp				// task entry
	ptnMe			interface{}						// pointer to task node of myself
	ptnRutMgr		interface{}						// pointer to route manager task node
	ptnQryMgr		interface{}						// pointer to query manager task node
	ptnLsnMgr		interface{}						// pointer to the listner manager task node
	ptnDhtMgr		interface{}						// pointer to dht manager task node
	instCache		*lru.Cache						// connection instance cache
	lockInstTab		sync.Mutex						// lock for connection instance table
	ciTab			map[conInstIdentity]*ConInst	// connection instance table
	ciSeq			int64							// connection instance sequence number
	txQueTab		map[conInstIdentity]*list.List	// outbound data pending queue
	ibInstTemp		map[string]*ConInst				// temp map for inbound instances
	natTcpResult	bool							// nat make map result
	pubTcpIp		net.IP							// public ip address
	pubTcpPort		int								// public port
	pasStatus		int								// public addr switching status
	mfilter			interface{}						// message filter
	tidMonitor		int								// monitor timer identity
	bakReq2Conn		map[string]interface{}			// connection request backup map, k: task name, v: message
	instInClosing	map[conInstIdentity]*ConInst	// instance in closing waiting for response from instance
}

//
// Create route manager
//
func NewConMgr() *ConMgr {
	conMgr := ConMgr{
		name:			ConMgrName,
		ciTab:			make(map[conInstIdentity]*ConInst, 0),
		ciSeq:			0,
		txQueTab:		make(map[conInstIdentity]*list.List, 0),
		ibInstTemp: 	make(map[string]*ConInst, 0),
		pubTcpIp:		net.IPv4zero,
		pubTcpPort:		0,
		tidMonitor:		sch.SchInvalidTid,
		bakReq2Conn:	make(map[string]interface{}, 0),
		instInClosing:	make(map[conInstIdentity]*ConInst, 0),
	}
	conMgr.tep = conMgr.conMgrProc
	chConMgrReady = make(chan bool, 1)
	return &conMgr
}

//
// Entry point exported to shceduler
//
func (conMgr *ConMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conMgr.tep(ptn, msg)
}

//
// Connection manager entry
//
func (conMgr *ConMgr)conMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	connLog.Debug("conMgrProc: ptn: %p, msg.Id: %d", ptn, msg.Id)

	eno := sch.SchEnoUnknown
	if conMgr.mfilter != nil {
		if eno := conMgr.mfilter.(func(*sch.SchMessage)DhtErrno)(msg); eno != DhtEnoNone {
			connLog.Debug("conMgrProc: filtered out, id: %d", msg.Id)
			return sch.SchEnoMismatched
		}
	}

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conMgr.poweroff(ptn)

	case sch.EvDhtConMgrConnectReq:
		eno = conMgr.connctReq(msg.Body.(*sch.MsgDhtConMgrConnectReq))

	case sch.EvDhtConMgrCloseReq:
		eno = conMgr.closeReq(msg.Body.(*sch.MsgDhtConMgrCloseReq))

	case sch.EvDhtLsnMgrAcceptInd:
		eno = conMgr.acceptInd(msg.Body.(*sch.MsgDhtLsnMgrAcceptInd))

	case sch.EvDhtConInstHandshakeRsp:
		eno = conMgr.handshakeRsp(msg.Body.(*sch.MsgDhtConInstHandshakeRsp))

	case sch.EvDhtLsnMgrStatusInd:
		eno = conMgr.lsnMgrStatusInd(msg.Body.(*sch.MsgDhtLsnMgrStatusInd))

	case sch.EvDhtConMgrSendReq:
		eno = conMgr.sendReq(msg.Body.(*sch.MsgDhtConMgrSendReq))

	case sch.EvDhtConInstStatusInd:
		eno = conMgr.instStatusInd(msg.Body.(*sch.MsgDhtConInstStatusInd))

	case sch.EvDhtConInstCloseRsp:
		eno = conMgr.instCloseRsp(msg.Body.(*sch.MsgDhtConInstCloseRsp))

	case sch.EvDhtRutPeerRemovedInd:
		eno = conMgr.rutPeerRemoveInd(msg.Body.(*sch.MsgDhtRutPeerRemovedInd))

	case sch.EvNatMgrReadyInd:
		eno = conMgr.natReadyInd(msg.Body.(*sch.MsgNatMgrReadyInd))

	case sch.EvNatMgrMakeMapRsp:
		eno = conMgr.natMakeMapRsp(msg.Body.(*sch.MsgNatMgrMakeMapRsp))

	case sch.EvNatMgrPubAddrUpdateInd:
		eno = conMgr.natPubAddrUpdateInd(msg.Body.(*sch.MsgNatMgrPubAddrUpdateInd))

	case sch.EvDhtConMgrMonitorTimer:
		eno = conMgr.monitorTimer()

	default:
		connLog.Debug("conMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	connLog.Debug("conMgrProc: get out, ptn: %p, msg.Id: %d", ptn, msg.Id)

	return eno
}

//
// Poweron handler
//
func (conMgr *ConMgr)poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)

	conMgr.sdl = sdl
	conMgr.ptnMe = ptn

	_, conMgr.ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName)
	_, conMgr.ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName)
	_, conMgr.ptnLsnMgr = sdl.SchGetUserTaskNode(LsnMgrName)
	_, conMgr.ptnDhtMgr = sdl.SchGetUserTaskNode(DhtMgrName)

	if conMgr.ptnRutMgr == nil || conMgr.ptnQryMgr == nil ||
		conMgr.ptnLsnMgr == nil || conMgr.ptnDhtMgr == nil {
		connLog.Debug("poweron: internal errors")
		return sch.SchEnoInternal
	}

	if dhtEno := conMgr.getConfig(); dhtEno != DhtEnoNone {
		connLog.Debug("poweron: getConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if conMgr.instCache, _ = lru.NewWithEvict(conMgr.cfg.maxCon, conMgr.onInstEvicted); conMgr.instCache == nil {
		connLog.Debug("poweron: lru.New failed")
		return sch.SchEnoUserTask
	}

	var td = sch.TimerDescription {
		Name:	"TmPrdMgrCleanup",
		Utid:	sch.DhtConMgrMonitorTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	time.Second * 1,
		Extra:	nil,
	}
	if eno, tid := conMgr.sdl.SchSetTimer(conMgr.ptnMe, &td); eno != sch.SchEnoNone {
		connLog.Debug("poweron: SchSetTimer failed, eno: %d", eno)
		return eno
	} else {
		conMgr.tidMonitor = tid
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conMgr *ConMgr)poweroff(ptn interface{}) sch.SchErrno {

	connLog.Debug("poweroff: task will be done ...")

	po := sch.SchMessage{}
	close(chConMgrReady)

	for _, ci := range conMgr.ciTab {
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}

	for _, ci := range conMgr.ibInstTemp {
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}

	conMgr.sdl.SchTaskDone(conMgr.ptnMe, sch.SchEnoKilled)

	return sch.SchEnoNone
}

//
// Inbound connection accepted indication handler
//
func (conMgr *ConMgr)acceptInd(msg *sch.MsgDhtLsnMgrAcceptInd) sch.SchErrno {

	//
	// at this moment, we do not know any peer information, since it's just only the
	// inbound connection accepted. we can check the peer only after the handshake
	// procedure is completed.
	//

	sdl := conMgr.sdl
	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq), true)
	if dhtEno := conMgr.setupConInst(ci, conMgr.ptnLsnMgr, nil, msg); dhtEno != DhtEnoNone {
		connLog.Debug("acceptInd: setupConInst failed, eno: %d", dhtEno)
		return sch.SchEnoUserTask
	}
	conMgr.ciSeq++

	td := sch.SchTaskDescription{
		Name:		ci.name,
		MbSize:		sch.SchMaxMbSize,
		Ep:			ci,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		nil,
	}

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		connLog.Debug("acceptInd: SchCreateTask failed, eno: %d", eno)
		return eno
	}

	ci.ptnMe = ptn
	conMgr.ibInstTemp[ci.name] = ci
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hs := sch.SchMessage{}
	hsreq := sch.MsgDhtConInstHandshakeReq {
		DurHs: conMgr.cfg.hsTimeout,
	}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	return sch.SchEnoNone
}

//
// Handshake response handler
//
func (conMgr *ConMgr)handshakeRsp(msg *sch.MsgDhtConInstHandshakeRsp) sch.SchErrno {

	//
	// for inbound connection instances, we still not map them into "conMgr.ciTab",
	// we need to do this if handshake-ok reported, since we can obtain the peer
	// information in this case here.
	//

	var ci *ConInst = nil
	rsp2Tasks4Outbound := func(ci *ConInst, msg *sch.MsgDhtConInstHandshakeRsp, dhtEno DhtErrno) sch.SchErrno {
		eno, ptn := ci.sdl.SchGetUserTaskNode(ci.srcTaskName)
		if eno == sch.SchEnoNone && ptn != nil && ptn == ci.ptnSrcTsk {
			if ci.isBlind {
				rsp := sch.MsgDhtBlindConnectRsp{
					Eno:  dhtEno.GetEno(),
					Ptn:  ci.ptnMe,
					Peer: msg.Peer,
				}
				schMsg := new(sch.SchMessage)
				conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, ci.ptnSrcTsk, sch.EvDhtBlindConnectRsp, &rsp)
				conMgr.sdl.SchSendMessage(schMsg)
			} else {
				rsp := sch.MsgDhtConMgrConnectRsp{
					Eno:  dhtEno.GetEno(),
					Peer: msg.Peer,
				}
				schMsg := new(sch.SchMessage)
				conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, ci.ptnSrcTsk, sch.EvDhtConMgrConnectRsp, &rsp)
				conMgr.sdl.SchSendMessage(schMsg)
			}
		}
		for name, req := range conMgr.bakReq2Conn {
			if eno, ptn := conMgr.sdl.SchGetUserTaskNode(name);
				eno == sch.SchEnoNone && ptn == req.(*sch.MsgDhtConMgrConnectReq).Task {
				rsp := sch.MsgDhtConMgrConnectRsp{
					Eno:  dhtEno.GetEno(),
					Peer: msg.Peer,
				}
				schMsg := new(sch.SchMessage)
				conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, ptn, sch.EvDhtConMgrConnectRsp, &rsp)
				conMgr.sdl.SchSendMessage(schMsg)
			}
		}
		conMgr.bakReq2Conn = make(map[string]interface{}, 0)
		return sch.SchEnoNone
	}


	if msg.Eno != DhtEnoNone.GetEno() {

		//
		// if it's a blind outbound, the request sender should be responsed here. at this
		// moment here, the ci.ptnSrcTsk should be the pointer to the sender task node.
		//
		if ci = msg.Inst.(*ConInst); ci == nil {
			connLog.Debug("handshakeRsp: nil connection instance")
			return sch.SchEnoInternal
		}

		connLog.Debug("handshakeRsp: handshake failed, inst: %s", ci.name)

		//
		// remove temp map for inbound connection instance, and for outbounds,
		// we just remove outbound instance from the map table(for outbounds). notice that
		// inbound instance still not be mapped into ciTab and no tx data should be pending.
		//
		if ci.dir == ConInstDirInbound {

			delete(conMgr.ibInstTemp, ci.name)

		} else if msg.Dir == ConInstDirOutbound {

			cid := conInstIdentity{
				nid: msg.Peer.ID,
				dir: ConInstDirOutbound,
			}

			delete(conMgr.ciTab, cid)

			if li, ok := conMgr.txQueTab[cid]; ok {
				for li.Len() > 0 {
					li.Remove(li.Front())
				}
				delete(conMgr.txQueTab, cid)
			}

			//
			// connect-response to source task
			//
			rsp2Tasks4Outbound(ci, msg, DhtErrno(msg.Eno))

			//
			// update route manager
			//
			update := sch.MsgDhtRutMgrUpdateReq {
				Why:	rutMgrUpdate4Handshake,
				Eno:	msg.Eno,
				Seens:	[]config.Node {
					*msg.Peer,
				},
				Duras:	[]time.Duration {
					0,
				},
			}

			schMsg := new(sch.SchMessage)
			conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
			conMgr.sdl.SchSendMessage(schMsg)
		}

		//
		// power off the connection instance: do not apply ci.sdl.SchTaskDone() directly
		// since cleaning work is needed for releasing the instance.
		//
		schMsg := new(sch.SchMessage)
		conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		return conMgr.sdl.SchSendMessage(schMsg)
	}

	connLog.Debug("handshakeRsp: ok responsed, msg: %+v", *msg)

	cid := conInstIdentity{
		nid: msg.Peer.ID,
		dir: ConInstDir(msg.Dir),
	}
	if msg.Dir != ConInstDirInbound && msg.Dir != ConInstDirOutbound {

		connLog.Debug("handshakeRsp: invalid direction, dht: %s, dir: %d", msg.Dir)
		return sch.SchEnoUserTask

	} else if msg.Dir == ConInstDirInbound {

		//
		// put inbound instance to map and remove the temp map for it, but before
		// doing this, duplicated cases must be check.
		//
		ci = msg.Inst.(*ConInst)
		cid := conInstIdentity {
			nid: msg.Peer.ID,
			dir: msg.Dir,
		}
		_, dup1 := conMgr.ciTab[cid]
		_, dup2 := conMgr.instInClosing[cid]
		if dup1 || dup2 {
			schMsg := new(sch.SchMessage)
			conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
			return conMgr.sdl.SchSendMessage(schMsg)
		}
		conMgr.ciTab[cid] = ci
		delete(conMgr.ibInstTemp, ci.name)

	} else {

		if ci = conMgr.lookupOutboundConInst(&msg.Peer.ID); ci == nil {
			connLog.Debug("handshakeRsp: not found, id: %x", msg.Peer.ID)
			return sch.SchEnoUserTask
		}

		//
		// connect-response to source task
		//
		rsp2Tasks4Outbound(ci, msg, DhtEnoNone)

		//
		// submit the pending packages for outbound instance if any
		//
		if li, ok := conMgr.txQueTab[cid]; ok {
			for li.Len() > 0 {
				el := li.Front()
				tx := el.Value.(*sch.MsgDhtConMgrSendReq)
				pkg := conInstTxPkg {
					task:		tx.Task,
					responsed:	nil,
					waitMid:	-1,
					waitSeq:	-1,
					submitTime:	time.Now(),
					payload:	tx.Data,
				}
				if tx.WaitRsp == true {
					pkg.responsed = make(chan bool, 1)
					pkg.waitMid = tx.WaitMid
					pkg.waitSeq = int64(tx.WaitSeq)
				}
				ci.txPutPending(&pkg)
				li.Remove(el)
			}
			delete(conMgr.txQueTab, cid)
		}
	}

	//
	// update instance cache
	//
	key := instLruKey{
		peer: msg.Peer.ID,
		dir: msg.Dir,
	}
	conMgr.instCache.Add(&key, ci)

	//
	// update the route manager
	//
	connLog.Debug("handshakeRsp: all ok, try to update route manager, " +
		"inst: %s, dir: %d, local: %s, remote: %s",
		ci.name, ci.dir, ci.con.LocalAddr().String(), ci.con.RemoteAddr().String())

	update := sch.MsgDhtRutMgrUpdateReq {
		Why: rutMgrUpdate4Handshake,
		Eno: DhtEnoNone.GetEno(),
		Seens: []config.Node {
			*msg.Peer,
		},
		Duras: []time.Duration {
			msg.Dur,
		},
	}

	schMsg := new(sch.SchMessage)
	conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
	return conMgr.sdl.SchSendMessage(schMsg)
}

//
// Listener manager status indication handler
//
func (conMgr *ConMgr)lsnMgrStatusInd(msg *sch.MsgDhtLsnMgrStatusInd) sch.SchErrno {
	connLog.Debug("lsnMgrStatusInd: listener manager status reported: %d", msg.Status)
	if conMgr.pasStatus == pasInNull {
		if msg.Status == lmsNull || msg.Status == lmsStopped {
			schMsg := sch.SchMessage{}
			conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrStartReq, nil)
			return conMgr.sdl.SchSendMessage(&schMsg)
		}
	} else {
		connLog.Debug("lsnMgrStatusInd: discarded for pasStatus: %d", conMgr.pasStatus)
	}
	return sch.SchEnoNone
}

//
// Connect-request handler
//
func (conMgr *ConMgr)connctReq(msg *sch.MsgDhtConMgrConnectReq) sch.SchErrno {

	var rspNormal = sch.MsgDhtConMgrConnectRsp {
		Eno:	DhtEnoNone.GetEno(),
		Peer:	msg.Peer,
	}
	var rspBlind = sch.MsgDhtBlindConnectRsp {
		Eno:	DhtEnoNone.GetEno(),
		Peer:	msg.Peer,
		Ptn:	nil,
	}
	var rsp interface{}
	var rspEvent int
	var ptrEno *int
	var sender = msg.Task
	var sdl = conMgr.sdl

	if msg.IsBlind {
		rsp = &rspBlind
		ptrEno = &rspBlind.Eno
		rspEvent = sch.EvDhtBlindConnectRsp
	} else {
		rsp = &rspNormal
		ptrEno = &rspNormal.Eno
		rspEvent = sch.EvDhtConMgrConnectRsp
	}

	rsp2Sender := func(eno DhtErrno) sch.SchErrno {
		msg := sch.SchMessage{}
		*ptrEno = int(eno)
		sdl.SchMakeMessage(&msg, conMgr.ptnMe, sender, rspEvent, rsp)
		return sdl.SchSendMessage(&msg)
	}

	backupConnectRequest := func(msg *sch.MsgDhtConMgrConnectReq) DhtErrno {
		task := conMgr.sdl.SchGetTaskName(msg.Task)
		if len(task) == 0 {
			return DhtEnoScheduler
		}
		if _, dup := conMgr.bakReq2Conn[task]; dup {
			return DhtEnoDuplicated
		}
		conMgr.bakReq2Conn[task] = msg
		return DhtEnoNone
	}

	dupConnProc := func (ci *ConInst) sch.SchErrno {
		status := ci.getStatus()
		if status == CisHandshaked || status == CisInService {
			return rsp2Sender(DhtErrno(DhtEnoDuplicated))
		} else if status == CisOutOfService || status == CisInKilling || status == CisClosed {
			return rsp2Sender(DhtErrno(DhtEnoResource))
		} else if eno := backupConnectRequest(msg); eno != DhtEnoNone {
			return rsp2Sender(eno)
		}
		return sch.SchEnoNone
	}

	isInstInClosing := func () bool {
		cid := conInstIdentity{
			nid:	msg.Peer.ID,
			dir:	ConInstDirOutbound,
		}
		if _, yes := conMgr.instInClosing[cid]; yes {
			return true
		}
		cid.dir = ConInstDirInbound
		_, yes := conMgr.instInClosing[cid]
		return yes
	}

	if isInstInClosing() {
		return rsp2Sender(DhtErrno(DhtEnoResource))
	} else if ci := conMgr.lookupOutboundConInst(&msg.Peer.ID); ci != nil {
		return  dupConnProc(ci)
	} else if ci := conMgr.lookupInboundConInst(&msg.Peer.ID); ci != nil {
		return  dupConnProc(ci)
	}

	connLog.Debug("connctReq: create connection instance, peer ip: %s", msg.Peer.IP.String())

	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq), msg.IsBlind)
	if eno := conMgr.setupConInst(ci, sender, msg.Peer, nil); eno != DhtEnoNone {
		connLog.Debug("connctReq: setupConInst failed, eno: %d", eno)
		return rsp2Sender(eno)
	}
	conMgr.ciSeq++

	td := sch.SchTaskDescription {
		Name:		ci.name,
		MbSize:		sch.SchMaxMbSize,
		Ep:			ci,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		nil,
	}

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		connLog.Debug("connctReq: SchCreateTask failed, eno: %d", eno)
		return rsp2Sender(DhtErrno(DhtEnoScheduler))
	}

	ci.ptnMe = ptn
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hs := sch.SchMessage{}
	hsreq := sch.MsgDhtConInstHandshakeReq {
		DurHs: conMgr.cfg.hsTimeout,
	}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	cid := conInstIdentity{
		nid:	msg.Peer.ID,
		dir:	ConInstDirOutbound,
	}
	conMgr.ciTab[cid] = ci

	// do not send connect-response message to source task here, instead,
	// this is done when handshake procedure is completed.
	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)closeReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {

	connLog.Debug("closeReq: peer id: %x, dir: %d", msg.Peer.ID, msg.Dir)

	cid := conInstIdentity {
		nid:	msg.Peer.ID,
		dir:	ConInstDir(msg.Dir),
	}
	if cid.dir == ConInstDirUnknown {
		cid.dir = ConInstDirAllbound
	}

	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl
	_, sender := sdl.SchGetUserTaskNode(msg.Task)

	rsp2Sender := func(eno DhtErrno) sch.SchErrno{
		if sender == nil {
			connLog.Debug("closeReq: rsp2Sender: nil sender")
			return sch.SchEnoMismatched
		}
		rsp := sch.MsgDhtConMgrCloseRsp {
			Eno:	int(eno),
			Peer:	msg.Peer,
			Dir:	msg.Dir,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, sender, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}

	req2Inst := func(inst *ConInst) sch.SchErrno {
		connLog.Debug("closeReq: req2Inst: inst: %s", inst.name)
		conMgr.instInClosing[cid] = inst
		delete(conMgr.ciTab, cid)
		req := sch.MsgDhtConInstCloseReq {
			Peer:	&msg.Peer.ID,
			Why:	sch.EvDhtConMgrCloseReq,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false
	dup := false
	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if status := ci.getStatus(); status >= CisInKilling {
				dup = true
			} else if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	connLog.Debug("closeReq: found: %t, err: %t, dup: %t", found, err, dup)

	if !found {
		return rsp2Sender(DhtEnoNotFound)
	} else if dup {
		return rsp2Sender(DhtEnoDuplicated)
	} else if err {
		return rsp2Sender(DhtEnoScheduler)
	}

	return sch.SchEnoNone
}

//
// Send-data-request handler
//
func (conMgr *ConMgr)sendReq(msg *sch.MsgDhtConMgrSendReq) sch.SchErrno {

	//
	// notice: when requested to send, the connection instance might not be exist,
	// we need to establesh the connection and backup the data if it is the case;
	//
	// notice: it must be a outbound instance to bear the data requested to be sent,
	// even we have an instance of inbound with the same node identity, we do not
	// send on it.
	//

	if msg == nil {
		connLog.Debug("sendReq: invalid parameter")
		return sch.SchEnoParameter
	}

	ci := conMgr.lookupOutboundConInst(&msg.Peer.ID)
	if ci == nil {
		ci = conMgr.lookupInboundConInst(&msg.Peer.ID)
	}

	if ci != nil {
		connLog.Debug("sendReq: connection instance found: %+v", *ci)
		if curStat := ci.getStatus(); curStat != CisInService {
			connLog.Debug("sendReq: can't send in status: %d", curStat)
			return sch.SchEnoUserTask
		}

		key := instLruKey {
			peer: ci.hsInfo.peer.ID,
			dir: ci.dir,
		}

		conMgr.instCache.Add(&key, ci)

		pkg := conInstTxPkg{
			task:       msg.Task,
			responsed:  nil,
			submitTime: time.Now(),
			payload:    msg.Data,
		}

		if msg.WaitRsp == true {
			pkg.responsed = make(chan bool, 1)
			pkg.waitMid = msg.WaitMid
			pkg.waitSeq = msg.WaitSeq
		}

		if eno := ci.txPutPending(&pkg); eno != DhtEnoNone {
			connLog.Debug("sendReq: txPutPending failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}
		return sch.SchEnoNone
	}

	connLog.Debug("sendReq: instance not found, tell connection manager to build it ...")

	schMsg := sch.SchMessage{}
	req := sch.MsgDhtConMgrConnectReq {
		Task:		msg.Task,
		Peer:		msg.Peer,
		IsBlind:	true,
	}

	conMgr.sdl.SchMakeMessage(&schMsg, msg.Task, conMgr.ptnMe, sch.EvDhtConMgrConnectReq, &req)
	conMgr.sdl.SchSendMessage(&schMsg)

	cid := conInstIdentity {
		nid: msg.Peer.ID,
		dir: ConInstDirOutbound,
	}

	li, ok := conMgr.txQueTab[cid]
	if ok {
		li.PushBack(msg)
	} else {
		li = list.New()
		conMgr.txQueTab[cid] = li
		li.PushBack(msg)
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (conMgr *ConMgr)instStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	cid := conInstIdentity {
		nid:	*msg.Peer,
		dir:	ConInstDir(msg.Dir),
	}
	cis := conMgr.lookupConInst(&cid)
	if len(cis) == 0 {
		connLog.Debug("instStatusInd: none of instances found")
		return sch.SchEnoNotFound
	}
	if len(cis) > 1 {
		connLog.Debug("instStatusInd: too much found, cid: %+v", cid)
		return sch.SchEnoNotFound
	}

	connLog.Debug("instStatusInd: inst: %s, msg.Status: %d, current status: %d",
		cis[0].name, msg.Status, cis[0].getStatus())

	switch msg.Status {
	case CisOutOfService:
		conMgr.instOutOfServiceInd(msg)

	case CisClosed:
		conMgr.instClosedInd(msg)

	case CisNull:
	case CisConnecting:
	case CisConnected:
	case CisAccepted:
	case CisInHandshaking:
	case CisHandshaked:
	case CisInService:
	case CisInKilling:
	default:
		connLog.Debug("instStatusInd: invalid status: %d", msg.Status)
		return sch.SchEnoMismatched
	}

	schMsg := new(sch.SchMessage)
	conMgr.sdl.SchMakeMessage(schMsg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConInstStatusInd, msg)
	conMgr.sdl.SchSendMessage(schMsg)

	if conMgr.pasStatus == pasInSwitching && len(conMgr.ciTab) == 0 {
		if eno := conMgr.natMapSwitchEnd(); eno != DhtEnoNone {
			connLog.Debug("instStatusInd: natMapSwitchEnd failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)instCloseRsp(msg *sch.MsgDhtConInstCloseRsp) sch.SchErrno {
	cid := conInstIdentity {
		nid:	*msg.Peer,
		dir:	ConInstDir(msg.Dir),
	}
	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl
	rsp2Sender := func(eno DhtErrno, peer *config.Node, task interface{}) sch.SchErrno{
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:	int(eno),
			Peer:	peer,
			Dir:	msg.Dir,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, task, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}

	rutUpdate := func(node *config.Node) sch.SchErrno {
		update := sch.MsgDhtRutMgrUpdateReq {
			Why:	rutMgrUpdate4Closed,
			Eno:	DhtEnoNone.GetEno(),
			Seens:	[]config.Node {
				*node,
			},
			Duras:	[]time.Duration {
				-1,
			},
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false
	if ci := conMgr.instInClosing[cid]; ci != nil {
		found = true
		delete(conMgr.instInClosing, cid)
		connLog.Debug("instCloseRsp: inst: %s, current status: %d", ci.name, ci.getStatus())
		if eno := rsp2Sender(DhtEnoNone, &ci.hsInfo.peer, ci.ptnSrcTsk); eno != sch.SchEnoNone {
			connLog.Debug("instCloseRsp: inst: %s, rsp2Sender failed, eno: %d", ci.name, eno)
			err = true
		}
		if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
			connLog.Debug("instCloseRsp: isnt: %s, rutUpdate failed, eno: %d", ci.name, eno)
			err = true
		}
	}
	if !found {
		connLog.Debug("instCloseRsp: none is found, id: %x", msg.Peer)
		return sch.SchEnoNotFound
	}
	if err {
		connLog.Debug("instCloseRsp: seems some errors")
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

//
// Peer removed from route indication handler
//
func (conMgr *ConMgr)rutPeerRemoveInd(msg *sch.MsgDhtRutPeerRemovedInd) sch.SchErrno {

	//
	// when peer is removed from route table, we close all bound connection
	// instances if any.
	//
	cid := conInstIdentity {
		nid:	msg.Peer,
		dir:	ConInstDirAllbound,
	}
	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl
	req2Inst := func(inst *ConInst) sch.SchErrno {
		req := sch.MsgDhtConInstCloseReq{
			Peer:	&msg.Peer,
			Why:	sch.EvDhtRutPeerRemovedInd,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false
	dup := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if ci.getStatus() >= CisInKilling {
				dup = true
			} else if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	if !found {
		connLog.Debug("rutPeerRemoveInd: not found, id: %x", msg.Peer)
		return sch.SchEnoNotFound
	}
	if dup {
		connLog.Debug("rutPeerRemoveInd: kill more than once")
		return sch.SchEnoDuplicated
	}
	if err {
		connLog.Debug("rutPeerRemoveInd: seems some errors")
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

//
// NAT ready indication handler
//
func (conMgr *ConMgr)natReadyInd(msg *sch.MsgNatMgrReadyInd) sch.SchErrno {
	if msg.NatType == config.NATT_NONE {
		conMgr.natTcpResult = true
		conMgr.pubTcpIp = conMgr.cfg.local.IP
		conMgr.pubTcpPort = int(conMgr.cfg.local.TCP)
		chConMgrReady<-true
	}
	return sch.SchEnoNone
}

//
// NAT make map response handler
//
func (conMgr *ConMgr)natMakeMapRsp(msg *sch.MsgNatMgrMakeMapRsp) sch.SchErrno {
	if !nat.NatIsResultOk(msg.Result) {
		connLog.Debug("natMakeMapRsp: fail reported, msg: %+v", *msg)
	}
	proto := strings.ToLower(msg.Proto)
	if proto == "tcp" {
		conMgr.natTcpResult = nat.NatIsStatusOk(msg.Status)
		if conMgr.natTcpResult {
			connLog.Debug("natMakeMapRsp: public dht addr: %s:%d",
				msg.PubIp.String(), msg.PubPort)
			conMgr.pubTcpIp = msg.PubIp
			conMgr.pubTcpPort = msg.PubPort
			if eno := conMgr.switch2NatAddr(proto); eno != DhtEnoNone  {
				connLog.Debug("natMakeMapRsp: switch2NatAddr failed, eno: %d", eno)
				return sch.SchEnoUserTask
			}
			chConMgrReady<-true
		} else {
			conMgr.pubTcpIp = net.IPv4zero
			conMgr.pubTcpPort = 0
		}
		return sch.SchEnoNone
	}
	connLog.Debug("natMakeMapRsp: unknown protocol reported: %s", proto)
	return sch.SchEnoParameter
}

//
// Public address update indication handler
//
func (conMgr *ConMgr)natPubAddrUpdateInd(msg *sch.MsgNatMgrPubAddrUpdateInd) sch.SchErrno {
	proto := strings.ToLower(msg.Proto)
	if proto == "tcp" {
		conMgr.natTcpResult = nat.NatIsStatusOk(msg.Status)
		if conMgr.natTcpResult {
			connLog.Debug("natPubAddrUpdateInd: public dht addr: %s:%d",
				msg.PubIp.String(), msg.PubPort)
			if conMgr.pasStatus != pasInNull {
				connLog.Debug("natPubAddrUpdateInd: had been in switching, pasStatus: %d", conMgr.pasStatus)
				return sch.SchEnoMismatched
			}
			conMgr.pubTcpIp = msg.PubIp
			conMgr.pubTcpPort = msg.PubPort
			if eno := conMgr.natMapSwitch(); eno != DhtEnoNone {
				connLog.Debug("natPubAddrUpdateInd: natMapSwitch failed, eno: %d", eno)
				return sch.SchEnoUserTask
			}
		} else {
			conMgr.pubTcpIp = net.IPv4zero
			conMgr.pubTcpPort = 0
		}
		return sch.SchEnoNone
	}
	connLog.Debug("natPubAddrUpdateInd: unknown protocol reported: %s", proto)
	return sch.SchEnoParameter
}

//
// monitor timer to keep enough connections
//
func (conMgr *ConMgr)monitorTimer() sch.SchErrno {
	if conMgr.natTcpResult {
		if len(conMgr.ciTab) < conMgr.cfg.minCon {
			msg := new(sch.SchMessage)
			conMgr.sdl.SchMakeMessage(msg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtConMgrBootstrapReq, nil)
			conMgr.sdl.SchSendMessage(msg)
		}
	}
	return sch.SchEnoNone
}

//
// Get configuration for connection mananger
//
func (conMgr *ConMgr)getConfig() DhtErrno {
	cfg := config.P2pConfig4DhtConManager(conMgr.sdl.SchGetP2pCfgName())
	conMgr.cfg.local = cfg.Local
	conMgr.cfg.bootstarpNode = cfg.BootstrapNode
	conMgr.cfg.maxCon = cfg.MaxCon
	conMgr.cfg.minCon = cfg.MinCon
	conMgr.cfg.hsTimeout = cfg.HsTimeout
	return DhtEnoNone
}

//
// Lookup connection instance by instance identity
//
func (conMgr *ConMgr)lookupConInst(cid *conInstIdentity) []*ConInst {
	conMgr.lockInstTab.Lock()
	defer conMgr.lockInstTab.Unlock()

	if cid == nil {
		return nil
	}

	if cid.dir == ConInstDirInbound || cid.dir == ConInstDirOutbound {
		if inst := conMgr.ciTab[*cid]; inst != nil {
			return []*ConInst{conMgr.ciTab[*cid]}
		}
	} else if cid.dir == ConInstDirAllbound {
		inCid := *cid
		inCid.dir = ConInstDirInbound
		outCid := *cid
		outCid.dir = ConInstDirOutbound
		inst := []*ConInst {
			conMgr.ciTab[inCid],
			conMgr.ciTab[outCid],
		}
		if inst[0] != nil && inst[1] != nil {
			return inst
		} else if inst[0] != nil {
			return []*ConInst{inst[0]}
		} else if inst[1] != nil {
			return []*ConInst{inst[0]}
		}
	}
	return nil
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupOutboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	ConInstDirOutbound,
	}
	if tab := conMgr.lookupConInst(ci); tab != nil && len(tab) == 1{
		return tab[0]
	}
	return nil
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupInboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	ConInstDirInbound,
	}
	if tab := conMgr.lookupConInst(ci); tab != nil && len(tab) == 1 {
		return tab[0]
	}
	return nil
}

//
// Setup outbound connection instance
//
func (conMgr *ConMgr)setupConInst(ci *ConInst, srcTask interface{}, peer *config.Node, msg interface{}) DhtErrno {

	ci.sdl = conMgr.sdl
	ci.bootstrapNode = conMgr.cfg.bootstarpNode
	ci.ptnSrcTsk = srcTask
	if ci.srcTaskName = ci.sdl.SchGetTaskName(srcTask); len(ci.srcTaskName) == 0 {
		connLog.Debug("setupConInst: source task without name")
		return DhtEnoNotFound
	}

	ci.local = conMgr.cfg.local
	ci.ptnConMgr = conMgr.ptnMe
	_, ci.ptnDhtMgr = conMgr.sdl.SchGetUserTaskNode(DhtMgrName)
	_, ci.ptnRutMgr = conMgr.sdl.SchGetUserTaskNode(RutMgrName)
	_, ci.ptnDsMgr = conMgr.sdl.SchGetUserTaskNode(DsMgrName)
	_, ci.ptnPrdMgr = conMgr.sdl.SchGetUserTaskNode(PrdMgrName)

	if ci.ptnSrcTsk == nil ||
		ci.ptnConMgr == nil ||
		ci.ptnDhtMgr == nil ||
		ci.ptnRutMgr == nil ||
		ci.ptnDsMgr == nil ||
		ci.ptnPrdMgr == nil {
		connLog.Debug("setupConInst: internal errors")
		return DhtEnoInternal
	}

	if peer != nil {

		ci.hsInfo.peer = *peer
		ci.dir = ConInstDirOutbound

	} else {

		ci.updateStatus(CisAccepted)
		ci.con = msg.(*sch.MsgDhtLsnMgrAcceptInd).Con
		ci.dir = ConInstDirInbound

		r := ci.con.(io.Reader)
		ci.ior = ggio.NewDelimitedReader(r, ciMaxPackageSize)

		w := ci.con.(io.Writer)
		ci.iow = ggio.NewDelimitedWriter(w)
	}

	if ci.dir == ConInstDirOutbound {
		ci.cid = conInstIdentity{
			nid: peer.ID,
			dir: ci.dir,
		}
	} else {
		ci.cid = conInstIdentity{
			nid: config.NodeID{},
			dir: ci.dir,
		}
	}

	return DhtEnoNone
}

//
// Instance closed status indication handler
//
func (conMgr *ConMgr)instClosedInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	cid := conInstIdentity {
		nid:	*msg.Peer,
		dir:	ConInstDir(msg.Dir),
	}

	sdl := conMgr.sdl

	rutUpdate := func(node *config.Node) sch.SchErrno {
		schMsg := sch.SchMessage{}
		update := sch.MsgDhtRutMgrUpdateReq {
			Why:	rutMgrUpdate4Closed,
			Eno:	DhtEnoNone.GetEno(),
			Seens:	[]config.Node {
				*node,
			},
			Duras:	[]time.Duration {
				-1,
			},
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false
	cis := conMgr.lookupConInst(&cid)
	if len(cis) <= 0 {
		connLog.Debug("instClosedInd: not found, %+v", cid)
	} else if len(cis) > 1 {
		connLog.Debug("instClosedInd: too much, inst: %s, %s", cis[0].name, cis[1].name)
	} else {
		connLog.Debug("instClosedInd: inst: %s, current status: %d", cis[0].name , cis[0].getStatus())
	}

	for _, ci := range cis {
		if ci != nil {
			found = true
			if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
				connLog.Debug("instClosedInd: rutUpdate failed, eno: %d", eno)
				err = true
			}

			delete(conMgr.ciTab, cid)

			key := instLruKey{
				peer: ci.hsInfo.peer.ID,
				dir: ci.dir,
			}
			conMgr.instCache.Remove(&key)
		}
	}

	if !found {
		connLog.Debug("instClosedInd: none is found, id: %x", msg.Peer)
		return sch.SchEnoNotFound
	} else if err {
		connLog.Debug("instClosedInd: seems some errors")
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// instance out of service status indication handler
//
func (conMgr *ConMgr)instOutOfServiceInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	cid := conInstIdentity {
		nid:	*msg.Peer,
		dir:	ConInstDir(msg.Dir),
	}

	sdl := conMgr.sdl

	rutUpdate := func(node *config.Node) sch.SchErrno {
		schMsg := sch.SchMessage{}
		update := sch.MsgDhtRutMgrUpdateReq {
			Why:	rutMgrUpdate4Closed,
			Eno:	DhtEnoNone.GetEno(),
			Seens:	[]config.Node {
				*node,
			},
			Duras:	[]time.Duration {
				-1,
			},
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}

	cis := conMgr.lookupConInst(&cid)
	if len(cis) <= 0 {
		connLog.Debug("instOutOfServiceInd: not found, %+v", cid)
	} else if len(cis) > 1 {
		connLog.Debug("instOutOfServiceInd: too much, inst: %s, %s", cis[0].name, cis[1].name)
	} else {
		connLog.Debug("instOutOfServiceInd: inst: %s, msg.Status: %d, current status: %d",
			cis[0].name, msg.Status, cis[0].getStatus())
	}

	for _, ci := range cis {
		if ci != nil {
			if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
				connLog.Debug("instOutOfServiceInd: rutUpdate failed, eno: %d", eno)
			}
			if status := ci.getStatus(); status < CisInKilling {
				connLog.Debug("instOutOfServiceInd: inst: %s, current satus: %d", ci.name, status);

				conMgr.instInClosing[cid] = ci
				delete(conMgr.ciTab, cid)

				req := sch.MsgDhtConInstCloseReq{
					Peer:	msg.Peer,
					Why:	sch.EvDhtConInstStatusInd,
				}
				msg := new(sch.SchMessage)
				sdl.SchMakeMessage(msg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstCloseReq, &req)
				sdl.SchSendMessage(msg)
			}
		}
	}

	return sch.SchEnoNone
}

//
// instance tx timeout
//
func (conMgr *ConMgr)instTxTimeoutInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {
	connLog.Debug("instTxTimeoutInd: peer: %x, dir: %d, status: %d", *msg.Peer, msg.Dir, msg.Status)
	return conMgr.instOutOfServiceInd(msg)
}

//
// deal with connection instance evicted
//
func (conMgr *ConMgr)onInstEvicted(key interface{}, value interface{}) {
	// notice: here we can send EvDhtLsnMgrPauseReq event to listener manager so
	// later connections from outside would not be accepted, since at this moment,
	// max connection threshold had been reached. this is not implemented, for it
	// needs EvDhtLsnMgrResumeReq to be sent when number of connection is under
	// the threshold later.
	k, _ := key.(*instLruKey)
	ci, _ := value.(instLruValue)
	if k == nil || ci == nil {
		connLog.Debug("onInstEvicted: invalid key or value")
		return
	}
	req := sch.MsgDhtConMgrCloseReq {
		Task: ConMgrName,
		Peer: &ci.hsInfo.peer,
		Dir: ci.dir,
	}
	msg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&msg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConMgrCloseReq, &req)
	conMgr.sdl.SchSendMessage(&msg)
}

//
// Switch to public address
//
func (conMgr *ConMgr)switch2NatAddr(proto string) DhtErrno {
	if proto == nat.NATP_TCP {
		conMgr.cfg.local.IP = conMgr.pubTcpIp
		conMgr.cfg.local.TCP = uint16(conMgr.pubTcpPort & 0xffff)
		return DhtEnoNone
	}
	connLog.Debug("switch2NatAddr: invalid protocol: %s", proto)
	return DhtEnoParameter
}

//
// switch to public address from nat manager
//
func (conMgr *ConMgr)natMapSwitch() DhtErrno {
	sdl := conMgr.sdl

	msg := new(sch.SchMessage)
	sdl.SchMakeMessage(msg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConMgrPubAddrSwitchBeg, nil)
	sdl.SchSendMessage(msg)

	msg = new(sch.SchMessage)
	sdl.SchMakeMessage(msg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrPauseReq, nil)
	sdl.SchSendMessage(msg)

	for cid, ci := range conMgr.ciTab {
		if ci.getStatus() >= CisInKilling {
			continue
		}
		ci.updateStatus(CisInKilling)
		ind := sch.MsgDhtConInstStatusInd {
			Peer: &cid.nid,
			Dir: int(ci.dir),
			Status: CisInKilling,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConInstStatusInd, &ind)
		sdl.SchSendMessage(&schMsg)

		req := sch.MsgDhtConInstCloseReq {
			Peer:	&cid.nid,
			Why:	sch.EvNatMgrPubAddrUpdateInd,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstCloseReq, &req)
		sdl.SchSendMessage(&schMsg)
	}

	po := sch.SchMessage{}
	for _, ci := range conMgr.ibInstTemp {
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}

	conMgr.pasStatus = pasInSwitching
	conMgr.mfilter = conMgr.msgFilter
	return DhtEnoNone
}

//
// post public address switching proc
//
func (conMgr *ConMgr)natMapSwitchEnd() DhtErrno {
	conMgr.pasStatus = pasInNull
	conMgr.instCache.Purge()
	conMgr.txQueTab = make(map[conInstIdentity]*list.List, 0)
	conMgr.ibInstTemp = make(map[string]*ConInst, 0)
	conMgr.switch2NatAddr(nat.NATP_TCP)
	conMgr.mfilter = nil

	msg := new(sch.SchMessage)
	conMgr.sdl.SchMakeMessage(msg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrResumeReq, nil)
	conMgr.sdl.SchSendMessage(msg)

	msg = new(sch.SchMessage)
	conMgr.sdl.SchMakeMessage(msg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConMgrPubAddrSwitchEnd, nil)
	conMgr.sdl.SchSendMessage(msg)

	return DhtEnoNone
}

//
// message filter
//
func (conMgr *ConMgr)msgFilter(msg *sch.SchMessage) DhtErrno {
	if conMgr.pasStatus == pasInNull {
		return DhtEnoNone
	}
	switch msg.Id {
	case sch.EvSchPoweroff:
	case sch.EvDhtConInstStatusInd:
	case sch.EvDhtConInstCloseRsp:
	case sch.EvDhtLsnMgrStatusInd:
	default:
		return DhtEnoMismatched
	}
	return DhtEnoNone
}

//
// Signal connection manager ready
//
var chConMgrReady chan bool
func ConMgrReady() bool {
	r, ok := <-chConMgrReady
	if !ok {
		return false
	}
	return r
}

