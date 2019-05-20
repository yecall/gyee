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
	"io"
	"net"
	"strings"
	"sync"
	"time"

	ggio "github.com/gogo/protobuf/io"
	lru "github.com/hashicorp/golang-lru"
	config "github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	nat "github.com/yeeco/gyee/p2p/nat"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	log "github.com/yeeco/gyee/log"
)

//
// debug
//
type connLogger struct {
	debug__      bool
	debugForce__ bool
}

var connLog = connLogger{
	debug__:      false,
	debugForce__: false,
}

func (log connLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

func (log connLogger) ForceDebug(fmt string, args ...interface{}) {
	if log.debugForce__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// Connection manager name registered in scheduler
//
const ConMgrName = sch.DhtConMgrName
const ConMgrMailboxSize = 1024 * 16

//
// Peer connection status
//
type conMgrPeerConnStat = int

const (
	pcsConnNo  = iota // not connected
	pcsConnYes        // connected in service
)

//
// Connection manager configuration
//
type conMgrCfg struct {
	chainId		  uint32		// chain identity
	local         *config.Node  // pointer to local node specification
	bootstarpNode bool          // bootstrap node flag
	maxCon        int           // max number of connection
	minCon        int           // min number of connection
	hsTimeout     time.Duration // handshake timeout duration
}

//
// Connection cache key and value
//
type instLruKey *ConInst
type instLruValue *ConInst

//
// Connection manager
//
const (
	pasInNull = iota
	pasInSwitching
)

type ConMgr struct {
	sdl           *sch.Scheduler               // pointer to scheduler
	sdlName       string                       // scheduler name
	name          string                       // my name
	busy		  bool						   // is dht too busy
	cfg           conMgrCfg                    // configuration
	tep           sch.SchUserTaskEp            // task entry
	ptnMe         interface{}                  // pointer to task node of myself
	ptnRutMgr     interface{}                  // pointer to route manager task node
	ptnQryMgr     interface{}                  // pointer to query manager task node
	ptnLsnMgr     interface{}                  // pointer to the listner manager task node
	ptnDhtMgr     interface{}                  // pointer to dht manager task node
	instCache     *lru.Cache                   // connection instance cache
	lockInstTab   sync.Mutex                   // lock for connection instance table
	ciTab         map[conInstIdentity]*ConInst // connection instance table
	ciSeq         int64                        // connection instance sequence number
	ibInstTemp    map[string]*ConInst          // temp map for inbound instances
	natTcpResult  bool                         // nat make map result
	pubTcpIp      net.IP                       // public ip address
	pubTcpPort    int                          // public port
	pasStatus     int                          // public addr switching status
	tidMonitor    int                          // monitor timer identity
	instInClosing map[conInstIdentity]*ConInst // instance in closing waiting for response from instance
}

//
// Create route manager
//
func NewConMgr() *ConMgr {
	conMgr := ConMgr{
		name:          ConMgrName,
		ciTab:         make(map[conInstIdentity]*ConInst, 0),
		ciSeq:         0,
		ibInstTemp:    make(map[string]*ConInst, 0),
		pubTcpIp:      net.IPv4zero,
		pubTcpPort:    0,
		tidMonitor:    sch.SchInvalidTid,
		instInClosing: make(map[conInstIdentity]*ConInst, 0),
	}
	conMgr.tep = conMgr.conMgrProc
	return &conMgr
}

//
// Entry point exported to shceduler
//
func (conMgr *ConMgr) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conMgr.tep(ptn, msg)
}

//
// Connection manager entry
//

func (conMgr *ConMgr) IsBusy() bool {
	return conMgr.busy
}

func (conMgr *ConMgr) checkMailBox() {
	if conMgr.sdl != nil && conMgr.ptnMe != nil {
		capacity := conMgr.sdl.SchGetTaskMailboxCapacity(conMgr.ptnMe)
		space := conMgr.sdl.SchGetTaskMailboxSpace(conMgr.ptnMe)
		conMgr.busy = space < (capacity >> 3)
	}
}

func (conMgr *ConMgr) conMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Tracef("conMgrProc: sdl: %s, msg.Id: %d", conMgr.sdlName, msg.Id)

	conMgr.checkMailBox()

	eno := sch.SchEnoUnknown
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
		log.Debugf("conMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	log.Tracef("conMgrProc: get out, sdl: %s, msg.Id: %d", conMgr.sdlName, msg.Id)

	return eno
}

//
// Poweron handler
//
func (conMgr *ConMgr) poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)

	conMgr.sdl = sdl
	conMgr.sdlName = sdl.SchGetP2pCfgName()
	conMgr.ptnMe = ptn

	_, conMgr.ptnRutMgr = sdl.SchGetUserTaskNode(RutMgrName)
	_, conMgr.ptnQryMgr = sdl.SchGetUserTaskNode(QryMgrName)
	_, conMgr.ptnLsnMgr = sdl.SchGetUserTaskNode(LsnMgrName)
	_, conMgr.ptnDhtMgr = sdl.SchGetUserTaskNode(DhtMgrName)

	if conMgr.ptnRutMgr == nil || conMgr.ptnQryMgr == nil ||
		conMgr.ptnLsnMgr == nil || conMgr.ptnDhtMgr == nil {
		log.Errorf("poweron: internal errors")
		return sch.SchEnoInternal
	}

	if dhtEno := conMgr.getConfig(); dhtEno != DhtEnoNone {
		log.Errorf("poweron: getConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if conMgr.instCache, _ = lru.NewWithEvict(conMgr.cfg.maxCon, conMgr.onInstEvicted); conMgr.instCache == nil {
		log.Errorf("poweron: lru.New failed")
		return sch.SchEnoUserTask
	}

	var td = sch.TimerDescription{
		Name:  "TmPrdMgrCleanup",
		Utid:  sch.DhtConMgrMonitorTimerId,
		Tmt:   sch.SchTmTypePeriod,
		Dur:   time.Second * 1,
		Extra: nil,
	}
	if eno, tid := conMgr.sdl.SchSetTimer(conMgr.ptnMe, &td); eno != sch.SchEnoNone {
		log.Errorf("poweron: SchSetTimer failed, eno: %d", eno)
		return eno
	} else {
		conMgr.tidMonitor = tid
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conMgr *ConMgr) poweroff(ptn interface{}) sch.SchErrno {
	log.Debugf("poweroff: task will be done, sdl: %s", conMgr.sdlName)

	CloseChConMgrReady(conMgr.sdl.SchGetP2pCfgName())

	for _, ci := range conMgr.ciTab {
		log.Debugf("poweroff: sent EvSchPoweroff to sdl: %s, inst: %s, dir: %d, statue: %d",
			conMgr.sdlName, ci.name, ci.dir, ci.status)
		po := sch.SchMessage{}
		po.TgtName = ci.name
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}
	for _, ci := range conMgr.ibInstTemp {
		log.Debugf("poweroff: sent EvSchPoweroff to sdl: %s, inst: %s, dir: %d, statue: %d",
			conMgr.sdlName, ci.name, ci.dir, ci.status)
		po := sch.SchMessage{}
		po.TgtName = ci.name
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		conMgr.sdl.SchSendMessage(&po)
	}
	return conMgr.sdl.SchTaskDone(conMgr.ptnMe, conMgr.name, sch.SchEnoKilled)
}

//
// Inbound connection accepted indication handler
//
func (conMgr *ConMgr) acceptInd(msg *sch.MsgDhtLsnMgrAcceptInd) sch.SchErrno {

	//
	// at this moment, we do not know any peer information, since it's just only the
	// inbound connection accepted. we can check the peer only after the handshake
	// procedure is completed.
	//

	sdl := conMgr.sdl
	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq), false)
	if dhtEno := conMgr.setupConInst(ci, conMgr.ptnLsnMgr, nil, msg); dhtEno != DhtEnoNone {
		log.Debugf("acceptInd: setupConInst failed, sdl: %s, eno: %d", conMgr.sdlName, dhtEno)
		return sch.SchEnoUserTask
	}
	conMgr.ciSeq++

	td := sch.SchTaskDescription{
		Name:   ci.name,
		MbSize: sch.SchDftMbSize,
		Ep:     ci,
		Wd:     &sch.SchWatchDog{HaveDog: false},
		Flag:   sch.SchCreatedGo,
		DieCb:  nil,
		UserDa: nil,
	}

	log.Debugf("acceptInd: inbound sdl: %s, inst: %s, peer: %s",
		conMgr.sdlName, ci.name, msg.Con.RemoteAddr().String())

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		log.Debugf("acceptInd: SchCreateTask failed, sdl: %s, eno: %d", conMgr.sdlName, eno)
		return eno
	}

	ci.ptnMe = ptn
	conMgr.ibInstTemp[ci.name] = ci
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hsreq := sch.MsgDhtConInstHandshakeReq{
		DurHs: conMgr.cfg.hsTimeout,
	}
	hs := sch.SchMessage{}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	return sch.SchEnoNone
}

//
// Handshake response handler
//
func (conMgr *ConMgr) handshakeRsp(msg *sch.MsgDhtConInstHandshakeRsp) sch.SchErrno {

	//
	// for inbound connection instances, we still not map them into "conMgr.ciTab",
	// we need to do this if handshake-ok reported, since we can obtain the peer
	// information at that moment. notice that: msg.Peer might be nil if it's an
	// inbound instance.
	//

	ci, ok := msg.Inst.(*ConInst)
	if ci == nil || !ok {
		log.Debugf("handshakeRsp: nil instance reported")
		return sch.SchEnoUserTask
	}

	rsp2TasksPending := func(ci *ConInst, msg *sch.MsgDhtConInstHandshakeRsp, dhtEno DhtErrno) sch.SchErrno {
		rsp := (interface{})(nil)
		ev := sch.EvDhtConMgrConnectRsp
		if eno, ptn := ci.sdl.SchGetUserTaskNode(ci.srcTaskName); eno != sch.SchEnoNone || ptn == nil || ptn != ci.ptnSrcTsk {
			log.Debugf("handshakeRsp: source task not found, sdl: %s, inst: %s, dir: %d, src: %s",
				conMgr.sdlName, ci.name, ci.dir, ci.srcTaskName)
		} else {
			if ci.isBlind {
				rsp = &sch.MsgDhtBlindConnectRsp{
					Eno:  dhtEno.GetEno(),
					Ptn:  ci.ptnMe,
					Peer: msg.Peer,
					Dir:  ci.dir,
				}
				ev = sch.EvDhtBlindConnectRsp
			} else {
				rsp = &sch.MsgDhtConMgrConnectRsp{
					Eno:  dhtEno.GetEno(),
					Peer: msg.Peer,
					Dir:  ci.dir,
				}
			}
			log.Debugf("rsp2TasksPending: sdl: %s, inst: %s, dir: %d, src: %s, ev: %d, ",
				conMgr.sdlName, ci.name, ci.dir, ci.srcTaskName, ev)
			schMsg := sch.SchMessage{}
			conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, ci.ptnSrcTsk, ev, rsp)
			conMgr.sdl.SchSendMessage(&schMsg)
		}
		for name, req := range ci.bakReq2Conn {
			if eno, ptn := conMgr.sdl.SchGetUserTaskNode(name); eno == sch.SchEnoNone && ptn == req.(*sch.MsgDhtConMgrConnectReq).Task {
				cr := req.(*sch.MsgDhtConMgrConnectReq)
				rsp = (interface{})(nil)
				ev = sch.EvDhtConMgrConnectRsp
				if cr.IsBlind {
					rsp = &sch.MsgDhtBlindConnectRsp{
						Eno:  dhtEno.GetEno(),
						Ptn:  ci.ptnMe,
						Peer: msg.Peer,
						Dir:  ci.dir,
					}
					ev = sch.EvDhtBlindConnectRsp
				} else {
					rsp = &sch.MsgDhtConMgrConnectRsp{
						Eno:  dhtEno.GetEno(),
						Peer: msg.Peer,
						Dir:  ci.dir,
					}
				}
				log.Debugf("rsp2TasksPending: sdl: %s, inst: %s, dir: %d, src: %s, name: %s, ev: %d, ",
					conMgr.sdlName, ci.name, ci.dir, ci.srcTaskName, name, ev)
				schMsg := sch.SchMessage{}
				conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, ptn, ev, rsp)
				conMgr.sdl.SchSendMessage(&schMsg)
			}
		}
		ci.bakReq2Conn = make(map[string]interface{}, 0)
		return sch.SchEnoNone
	}

	if msg.Eno != DhtEnoNone.GetEno() {

		log.Debugf("handshakeRsp: failed reported, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, ci.dir)

		//
		// remove temp map for inbound connection instance, and for outbounds,
		// we just remove outbound instance from the map table(for outbounds). notice that
		// inbound instance still not be mapped into ciTab and no tx data should be pending.
		//
		rsp2TasksPending(ci, msg, DhtErrno(msg.Eno))

		if ci.dir == ConInstDirInbound {

			delete(conMgr.ibInstTemp, ci.name)
			return conMgr.sdl.SchTaskDone(ci.ptnMe, ci.name, sch.SchEnoKilled)

		} else if msg.Dir == ConInstDirOutbound {

			cid := conInstIdentity{
				nid: msg.Peer.ID,
				dir: ConInstDir(msg.Dir),
			}

			if _, ok := conMgr.ciTab[cid]; ok {

				delete(conMgr.ciTab, cid)

				//
				// update route manager
				//
				update := sch.MsgDhtRutMgrUpdateReq{
					Why: rutMgrUpdate4Handshake,
					Eno: msg.Eno,
					Seens: []config.Node{
						*msg.Peer,
					},
					Duras: []time.Duration{
						0,
					},
				}

				schMsg := sch.SchMessage{}
				conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
				conMgr.sdl.SchSendMessage(&schMsg)

				return conMgr.sdl.SchTaskDone(ci.ptnMe, ci.name, sch.SchEnoKilled)
			}

			//
			// the instance reported can be in closing(conMgr.instInClosing), this
			// case we do nothing since EvDhtConInstCloseReq had been sent to it.
			//

			log.Debugf("handshakeRsp: too late, sdl: %s, inst: %s, dir: %d",
				conMgr.sdlName, ci.name, ci.dir)

			return sch.SchEnoNone
		}
	}

	log.Debugf("handshakeRsp: ok reported, sdl: %s, inst: %s, dir: %d",
		conMgr.sdlName, ci.name, ci.dir)

	cid := conInstIdentity{
		nid: msg.Peer.ID,
		dir: ConInstDir(msg.Dir),
	}
	if msg.Dir != ConInstDirInbound && msg.Dir != ConInstDirOutbound {
		log.Debugf("handshakeRsp: invalid direction, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, msg.Dir)
		panic("handshakeRsp: invalid direction")
	}

	if msg.Dir == ConInstDirInbound {
		delete(conMgr.ibInstTemp, ci.name)
	}

	if _, dup := conMgr.instInClosing[cid]; dup {

		//
		// if the instance is an outbound one, then it must have been requested
		// to be closed and EvDhtConInstCloseReq must have been sent to it, while
		// the handshake response of this instance comes here just now. in this
		// case, we discard the handshakeRsp message;
		// if the instance is an inbound one, this one must be a different instance
		// than that in closing. this can caused by that the peer connects us for
		// some reasons while an old instance outgoing from the same peer is in
		// closing in our side. we done the new one in this case;
		//

		if ci.dir == ConInstDirInbound {
			log.Debugf("handshakeRsp: done inbound duplicated to closing, sdl: %s, inst: %s, dir: %d",
				conMgr.sdlName, ci.name, ci.dir)
			return conMgr.sdl.SchTaskDone(ci.ptnMe, ci.name, sch.SchEnoKilled)
		}

		log.Debugf("handshakeRsp: ignored for outbound duplicated to closing, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, ci.dir)

		return sch.SchEnoNone
	}

	if msg.Dir == ConInstDirInbound {

		//
		// peer connects us for some reasons while an inbound instance for
		// the peer exist in our side, done the new one reported.
		//

		if duped, dup := conMgr.ciTab[cid]; dup {
			log.Debugf("handshakeRsp: duplicated, sdl: %s, inst: %s, dir: %d, duped: %s, dupdir: %d",
				conMgr.sdlName, ci.name, msg.Dir, duped.name, duped.dir)
			return conMgr.sdl.SchTaskDone(ci.ptnMe, ci.name, sch.SchEnoKilled)
		}

		conMgr.ciTab[cid] = ci

	} else if duped, dup := conMgr.ciTab[cid]; !dup || duped != ci {

		//
		// it's possible, the peer had changed node identity but kept the
		// same ip address and port, we done it. see function outboundHandshake
		// in file coninst.go for more please.
		//

		log.Debugf("handshakeRsp: not found, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, msg.Dir)

		return conMgr.sdl.SchTaskDone(ci.ptnMe, ci.name, sch.SchEnoKilled)
	}

	//
	// update instance cache
	//
	conMgr.instCache.Add(ci, ci)

	//
	// update the route manager
	//
	log.Debugf("handshakeRsp: update router, " +
		"sdl: %s, inst: %s, dir: %d",
		conMgr.sdlName, ci.name, ci.dir)

	update := sch.MsgDhtRutMgrUpdateReq{
		Why: rutMgrUpdate4Handshake,
		Eno: DhtEnoNone.GetEno(),
		Seens: []config.Node{
			*msg.Peer,
		},
		Duras: []time.Duration{
			msg.Dur,
		},
	}
	schMsg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
	conMgr.sdl.SchSendMessage(&schMsg)

	//
	// startup the instance
	//
	log.Debugf("handshakeRsp: send EvDhtConInstStartupReq, " +
		"sdl: %s, inst: %s, dir: %d",
		conMgr.sdlName, ci.name, ci.dir)

	req := sch.MsgDhtConInstStartupReq{
		EnoCh: make(chan int, 0),
	}

	reqLost := false
	mscb := func(rc sch.SchErrno) {
		if rc != sch.SchEnoNone {
			reqLost = true
			req.EnoCh <- DhtEnoScheduler.GetEno()
		}
	}

	schMsg = sch.SchMessage{
		Mscb: mscb,
		TgtName: ci.name,
	}

	conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstStartupReq, &req)
	schEno := conMgr.sdl.SchSendMessage(&schMsg)
	if schEno == sch.SchEnoNone {

		log.Debugf("handshakeRsp: send EvDhtConInstStartupReq ok, " +
			"sdl: %s, inst: %s, dir: %d, eno: %d",
			conMgr.sdlName, ci.name, ci.dir, schEno)

		eno, ok := <-req.EnoCh
		if !ok {
			log.Errorf("handshakeRsp: impossible, sdl: %s", conMgr.sdlName)
			eno = int(DhtEnoInternal)
		} else {
			close(req.EnoCh)
		}

		if !reqLost && eno != DhtEnoNone.GetEno() {
			log.Errorf("handshakeRsp: internal errors, sdl: %s", conMgr.sdlName)
		}

		if reqLost {
			log.Debugf("handshakeRsp: EvDhtConInstStartupReq lost, " +
				"sdl: %s, inst: %s, dir: %d, eno: %d",
				conMgr.sdlName, ci.name, ci.dir, eno)
			return rsp2TasksPending(ci, msg, DhtEnoScheduler)
		}

		log.Debugf("handshakeRsp: EvDhtConInstStartupReq confirmed ok, " +
			"sdl: %s, inst: %s, dir: %d, eno: %d",
			conMgr.sdlName, ci.name, ci.dir, eno)

		return rsp2TasksPending(ci, msg, DhtEnoNone)
	}

	log.Debugf("handshakeRsp: send EvDhtConInstStartupReq failed, " +
		"sdl: %s, inst: %s, dir: %d, eno: %d",
		conMgr.sdlName, ci.name, ci.dir, schEno)

	return rsp2TasksPending(ci, msg, DhtEnoScheduler)
}

//
// Listener manager status indication handler
//
func (conMgr *ConMgr) lsnMgrStatusInd(msg *sch.MsgDhtLsnMgrStatusInd) sch.SchErrno {
	log.Debugf("lsnMgrStatusInd: listener manager status reported: %d", msg.Status)
	if conMgr.pasStatus == pasInNull {
		if msg.Status == lmsNull || msg.Status == lmsStopped {
			schMsg := sch.SchMessage{}
			conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrStartReq, nil)
			return conMgr.sdl.SchSendMessage(&schMsg)
		}
	} else {
		log.Debugf("lsnMgrStatusInd: discarded for pasStatus: %d", conMgr.pasStatus)
	}
	return sch.SchEnoNone
}

//
// Connect-request handler
//
func (conMgr *ConMgr) connctReq(msg *sch.MsgDhtConMgrConnectReq) sch.SchErrno {

	var rspNormal = sch.MsgDhtConMgrConnectRsp{
		Eno:  DhtEnoNone.GetEno(),
		Peer: msg.Peer,
		Dir:  ConInstDirUnknown,
	}
	var rspBlind = sch.MsgDhtBlindConnectRsp{
		Eno:  DhtEnoNone.GetEno(),
		Peer: msg.Peer,
		Ptn:  nil,
		Dir:  ConInstDirUnknown,
	}
	var rsp interface{}
	var rspEvent int
	var ptrEno *int
	var ptrDir *int
	var sender = msg.Task
	var sdl = conMgr.sdl

	if msg.IsBlind {
		rsp = &rspBlind
		ptrEno = &rspBlind.Eno
		ptrDir = &rspBlind.Dir
		rspEvent = sch.EvDhtBlindConnectRsp
	} else {
		rsp = &rspNormal
		ptrEno = &rspNormal.Eno
		ptrDir = &rspNormal.Dir
		rspEvent = sch.EvDhtConMgrConnectRsp
	}

	rsp2Sender := func(eno DhtErrno, dir int) sch.SchErrno {
		log.Debugf("rsp2Sender: eno: %d, ev: %d, dir: %d, owner: %s", eno, rspEvent, dir, msg.Name)
		*ptrEno = int(eno)
		*ptrDir = dir
		msg := sch.SchMessage{}
		sdl.SchMakeMessage(&msg, conMgr.ptnMe, sender, rspEvent, rsp)
		return sdl.SchSendMessage(&msg)
	}

	backupConnectRequest := func(ci *ConInst, msg *sch.MsgDhtConMgrConnectReq) DhtErrno {
		task := conMgr.sdl.SchGetTaskName(msg.Task)
		if len(task) == 0 {
			return DhtEnoScheduler
		}
		if _, dup := ci.bakReq2Conn[task]; dup {
			return DhtEnoDuplicated
		}
		ci.bakReq2Conn[task] = msg
		return DhtEnoNone
	}

	dupConnProc := func(ci *ConInst) sch.SchErrno {
		status := ci.getStatus()
		log.Debugf("dupConnProc: inst: %s, dir: %d, status: %d, owner: %s", ci.name, ci.dir, status, msg.Name)
		if status == CisInService {
			return rsp2Sender(DhtErrno(DhtEnoDuplicated), ci.dir)
		} else if status == CisOutOfService || status == CisClosed {
			return rsp2Sender(DhtErrno(DhtEnoResource), ci.dir)
		} else if eno := backupConnectRequest(ci, msg); eno != DhtEnoNone {
			return rsp2Sender(DhtEnoResource, ci.dir)
		}
		return sch.SchEnoNone
	}

	isInstInClosing := func() (bool, *ConInst) {
		cid := conInstIdentity{
			nid: msg.Peer.ID,
			dir: ConInstDirOutbound,
		}
		if inst, yes := conMgr.instInClosing[cid]; yes {
			return true, inst
		}
		cid.dir = ConInstDirInbound
		inst, yes := conMgr.instInClosing[cid]
		return yes, inst
	}

	if yes, ci := isInstInClosing(); yes {
		log.Debugf("connctReq: in closing, inst: %s , owner: %s", ci.name, msg.Name)
		return rsp2Sender(DhtErrno(DhtEnoResource), ci.dir)
	} else if ci := conMgr.lookupOutboundConInst(&msg.Peer.ID); ci != nil {
		log.Debugf("connctReq: outbound duplicated, inst: %s, owner: %s", ci.name, msg.Name)
		return dupConnProc(ci)
	} else if ci := conMgr.lookupInboundConInst(&msg.Peer.ID); ci != nil {
		log.Debugf("connctReq: inbound duplicated, inst: %s, owner: %s", ci.name, msg.Name)
		return dupConnProc(ci)
	}

	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq), msg.IsBlind)
	if eno := conMgr.setupConInst(ci, sender, msg.Peer, nil); eno != DhtEnoNone {
		log.Errorf("connctReq: setupConInst failed, inst: %s, dir: %d, owner: %s, eno: %d",
			ci.name, ci.dir, msg.Name, eno)
		return rsp2Sender(eno, ci.dir)
	}
	conMgr.ciSeq++
	td := sch.SchTaskDescription{
		Name:   ci.name,
		MbSize: sch.SchDftMbSize,
		Ep:     ci,
		Wd:     &sch.SchWatchDog{HaveDog: false},
		Flag:   sch.SchCreatedGo,
		DieCb:  nil,
		UserDa: nil,
	}
	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		log.Debugf("connctReq: SchCreateTask failed, sdl: %s, inst: %s, dir: %d, eno: %d",
			conMgr.sdlName, ci.name, ci.dir, eno)
		return rsp2Sender(DhtErrno(DhtEnoScheduler), ci.dir)
	}

	log.Debugf("connctReq: outbound instance, sdl: %s, inst: %s, dir: %d, owner: %s, ip: %s",
		conMgr.sdlName, ci.name, ci.dir, msg.Name, msg.Peer.IP.String())

	ci.ptnMe = ptn
	po := sch.SchMessage{}
	sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweron, nil)
	sdl.SchSendMessage(&po)

	hsreq := sch.MsgDhtConInstHandshakeReq{
		DurHs: conMgr.cfg.hsTimeout,
	}
	hs := sch.SchMessage{}
	sdl.SchMakeMessage(&hs, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstHandshakeReq, &hsreq)
	sdl.SchSendMessage(&hs)

	cid := conInstIdentity{
		nid: msg.Peer.ID,
		dir: ConInstDirOutbound,
	}
	conMgr.ciTab[cid] = ci
	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr) closeReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {

	log.Debugf("closeReq: sdl: %s, task: %s, id: %x, dir: %d",
		conMgr.sdlName, msg.Task, msg.Peer.ID, msg.Dir)

	cid := conInstIdentity{
		nid: msg.Peer.ID,
		dir: ConInstDir(msg.Dir),
	}
	if cid.dir != ConInstDirInbound && cid.dir != ConInstDirOutbound {
		log.Debugf("closeReq: invalid direction: sdl: %s, inst: %s, %d",
			conMgr.sdlName, msg.Task, cid.dir)
		panic("closeReq: invalid direction")
	}

	sdl := conMgr.sdl
	_, sender := sdl.SchGetUserTaskNode(msg.Task)
	rsp2Sender := func(eno DhtErrno) sch.SchErrno {
		if sender == nil {
			log.Debugf("closeReq: rsp2Sender: nil sender, sdl: %s", conMgr.sdlName)
			return sch.SchEnoMismatched
		}
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:  int(eno),
			Peer: msg.Peer,
			Dir:  msg.Dir,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, sender, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}
	req2Inst := func(inst *ConInst) sch.SchErrno {
		log.Debugf("closeReq: req2Inst: sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, inst.name, inst.dir)
		conMgr.instInClosing[cid] = inst
		delete(conMgr.ciTab, cid)
		req := sch.MsgDhtConInstCloseReq{
			Peer: &msg.Peer.ID,
			Why:  sch.EvDhtConMgrCloseReq,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		schMsg.Keep = sch.SchMsgKeepFromPoweroff
		sdl.SchSendMessage(&schMsg)
		return sch.SchEnoNone
	}

	found := false
	err := false
	dup := false
	if cis, _ := conMgr.lookupConInst(&cid); cis != nil {
		for _, ci := range cis {
			if ci != nil {
				found = true
				log.Debugf("closeReq: found, sdl: %s, inst: %s, dir: %d",
					conMgr.sdlName, ci.name, ci.dir)
				if status := ci.getStatus(); status > CisOutOfService {
					log.Debugf("closeReq: mismatched, sdl: %s, inst: %s, dir: %d, status: %d",
						conMgr.sdlName, ci.name, ci.dir, status)
					dup = true
				} else if req2Inst(ci) != sch.SchEnoNone {
					err = true
				}
			}
		}
	}

	log.Debugf("closeReq: sdl: %s, found: %t, err: %t, dup: %t",
		conMgr.sdlName, found, err, dup)

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
func (conMgr *ConMgr) sendReq(msg *sch.MsgDhtConMgrSendReq) sch.SchErrno {

	//
	// notice: when requested to send, the connection instance might not be exist,
	// we need to establish the connection and backup the data if it is the case;
	// notice: it must be a outbound instance to bear the data requested to be sent,
	// even we have an instance of inbound with the same node identity, we do not
	// send on it.
	//

	if msg == nil {
		log.Warnf("sendReq: invalid parameter")
		return sch.SchEnoParameter
	}

	ci := (*ConInst)(nil)
	cio := conMgr.lookupOutboundConInst(&msg.Peer.ID)
	cii := conMgr.lookupInboundConInst(&msg.Peer.ID)
	if cio != nil && conMgr.instInClosing[cio.cid] == nil {
		log.Debugf("sendReq: outbound instance selected")
		ci = cio
	} else if cii != nil && conMgr.instInClosing[cii.cid] == nil {
		ci = cii
		log.Debugf("sendReq: inbound instance selected")
	}

	if ci == nil {
		log.Warnf("sendReq: not found, peer id: %x", msg.Peer.ID)
		return sch.SchEnoResource
	}

	log.Debugf("sendReq: connection instance found: inst: %s, dir: %d, peer: %s",
		ci.name, ci.dir, ci.hsInfo.peer.IP.String())

	if curStat := ci.getStatus(); curStat != CisInService {
		log.Warnf("sendReq: can't send in status: %d", curStat)
		return sch.SchEnoResource
	}
	conMgr.instCache.Add(ci, ci)
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
		log.Warnf("sendReq: txPutPending failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (conMgr *ConMgr) instStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {

	cid := conInstIdentity{
		nid: *msg.Peer,
		dir: ConInstDir(msg.Dir),
	}
	cis, _ := conMgr.lookupConInst(&cid)
	if len(cis) == 0 {
		log.Debugf("instStatusInd: none of instances found")
		return sch.SchEnoNotFound
	}

	log.Debugf("instStatusInd: inst: %s, msg.Status: %d, current status: %d",
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
	case CisHandshook:
	case CisInService:
	default:
		log.Debugf("instStatusInd: invalid status: %d", msg.Status)
		return sch.SchEnoMismatched
	}

	schMsg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConInstStatusInd, msg)
	schMsg.Keep = sch.SchMsgKeepFromPoweroff
	conMgr.sdl.SchSendMessage(&schMsg)

	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr) instCloseRsp(msg *sch.MsgDhtConInstCloseRsp) sch.SchErrno {
	cid := conInstIdentity{
		nid: *msg.Peer,
		dir: ConInstDir(msg.Dir),
	}
	sdl := conMgr.sdl
	rsp2Sender := func(eno DhtErrno, peer *config.Node, task interface{}) sch.SchErrno {
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:  int(eno),
			Peer: peer,
			Dir:  msg.Dir,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, task, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}
	rutUpdate := func(node *config.Node) sch.SchErrno {
		update := sch.MsgDhtRutMgrUpdateReq{
			Why: rutMgrUpdate4Closed,
			Eno: DhtEnoNone.GetEno(),
			Seens: []config.Node{
				*node,
			},
			Duras: []time.Duration{
				-1,
			},
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}

	log.Debugf("instCloseRsp: sdl: %s, cid: %+v", conMgr.sdlName, cid)

	found := false
	err := false

	if ci := conMgr.instInClosing[cid]; ci != nil {

		log.Debugf("instCloseRsp: sdl: %s, inst: %s, current status: %d",
			conMgr.sdlName, ci.name, ci.getStatus())

		found = true
		delete(conMgr.instInClosing, cid)
		if eno := rsp2Sender(DhtEnoNone, &ci.hsInfo.peer, ci.ptnSrcTsk); eno != sch.SchEnoNone {
			log.Debugf("instCloseRsp: inst: %s, rsp2Sender failed, eno: %d", ci.name, eno)
			err = true
		}
		if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
			log.Debugf("instCloseRsp: inst: %s, rutUpdate failed, eno: %d", ci.name, eno)
			err = true
		}
	}

	if !found {
		log.Debugf("instCloseRsp: not found, sdl: %s", conMgr.sdlName)
		return sch.SchEnoNotFound
	}
	if err {
		log.Debugf("instCloseRsp: seems some errors, sdl: %s", conMgr.sdlName)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Peer removed from route indication handler
//
func (conMgr *ConMgr) rutPeerRemoveInd(msg *sch.MsgDhtRutPeerRemovedInd) sch.SchErrno {
	cid := conInstIdentity{
		nid: msg.Peer,
		dir: ConInstDirAllbound,
	}
	sdl := conMgr.sdl
	req2Inst := func(inst *ConInst) sch.SchErrno {
		log.Debugf("rutPeerRemoveInd: req2Inst: sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, inst.name, inst.dir)
		cid.dir = inst.dir
		conMgr.instInClosing[cid] = inst
		delete(conMgr.ciTab, cid)
		req := sch.MsgDhtConInstCloseReq{
			Peer: &msg.Peer,
			Why:  sch.EvDhtRutPeerRemovedInd,
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		schMsg.Keep = sch.SchMsgKeepFromPoweroff
		return sdl.SchSendMessage(&schMsg)
	}
	closeInst := func(ci *ConInst) (found, dup, err bool) {
		found = true
		dup = false
		err = false
		if status := ci.getStatus(); status > CisOutOfService {
			log.Debugf("rutPeerRemoveInd: mismatched, sdl: %s, status: %d",
				conMgr.sdlName, status)
			dup = true
		} else if req2Inst(ci) != sch.SchEnoNone {
			err = true
		}
		return
	}

	if cis, _ := conMgr.lookupConInst(&cid); cis != nil {
		for _, ci := range cis {
			if ci != nil {
				f, d, e := closeInst(ci)
				log.Debugf("rutPeerRemoveInd: sdl: %s, found: %t, dup: %t, err: %t",
					conMgr.sdlName, f, d, e)
			}
		}
	}
	return sch.SchEnoNone
}

//
// NAT ready indication handler
//
func (conMgr *ConMgr) natReadyInd(msg *sch.MsgNatMgrReadyInd) sch.SchErrno {
	if msg.NatType == config.NATT_NONE {
		conMgr.natTcpResult = true
		conMgr.pubTcpIp = conMgr.cfg.local.IP
		conMgr.pubTcpPort = int(conMgr.cfg.local.TCP)
		mapChConMgrReady[conMgr.sdl.SchGetP2pCfgName()] <- true
	}
	return sch.SchEnoNone
}

//
// NAT make map response handler
//
func (conMgr *ConMgr) natMakeMapRsp(msg *sch.MsgNatMgrMakeMapRsp) sch.SchErrno {
	if !nat.NatIsResultOk(msg.Result) {
		log.Warnf("natMakeMapRsp: fail reported, msg: %+v", *msg)
	}
	proto := strings.ToLower(msg.Proto)
	if proto == "tcp" {
		conMgr.natTcpResult = nat.NatIsStatusOk(msg.Status)
		if conMgr.natTcpResult {
			log.Debugf("natMakeMapRsp: public dht addr: %s:%d",
				msg.PubIp.String(), msg.PubPort)
			conMgr.pubTcpIp = msg.PubIp
			conMgr.pubTcpPort = msg.PubPort
			if eno := conMgr.switch2NatAddr(proto); eno != DhtEnoNone {
				log.Warnf("natMakeMapRsp: switch2NatAddr failed, eno: %d", eno)
				return sch.SchEnoUserTask
			}
			mapChConMgrReady[conMgr.sdl.SchGetP2pCfgName()] <- true
		} else {
			conMgr.pubTcpIp = net.IPv4zero
			conMgr.pubTcpPort = 0
		}
		return sch.SchEnoNone
	}
	log.Debugf("natMakeMapRsp: unknown protocol reported: %s", proto)
	return sch.SchEnoParameter
}

//
// Public address update indication handler
//
func (conMgr *ConMgr) natPubAddrUpdateInd(msg *sch.MsgNatMgrPubAddrUpdateInd) sch.SchErrno {
	proto := strings.ToLower(msg.Proto)
	if proto == "tcp" {
		conMgr.natTcpResult = nat.NatIsStatusOk(msg.Status)
		if conMgr.natTcpResult {
			log.Debugf("natPubAddrUpdateInd: public dht addr: %s:%d",
				msg.PubIp.String(), msg.PubPort)
			if conMgr.pasStatus != pasInNull {
				log.Debugf("natPubAddrUpdateInd: had been in switching, pasStatus: %d", conMgr.pasStatus)
				return sch.SchEnoMismatched
			}
			conMgr.pubTcpIp = msg.PubIp
			conMgr.pubTcpPort = msg.PubPort
			log.Debugf("natPubAddrUpdateInd: enter pasInSwitching, call natMapSwitch")
			conMgr.pasStatus = pasInSwitching
			if eno := conMgr.natMapSwitch(); eno != DhtEnoNone {
				log.Warnf("natPubAddrUpdateInd: natMapSwitch failed, eno: %d", eno)
				return sch.SchEnoUserTask
			}
		} else {
			conMgr.pubTcpIp = net.IPv4zero
			conMgr.pubTcpPort = 0
		}
		return sch.SchEnoNone
	}
	log.Debugf("natPubAddrUpdateInd: unknown protocol reported: %s", proto)
	return sch.SchEnoParameter
}

//
// monitor timer to keep enough connections
//
func (conMgr *ConMgr) monitorTimer() sch.SchErrno {
	if conMgr.natTcpResult {
		if len(conMgr.ciTab) < conMgr.cfg.minCon {
			msg := sch.SchMessage{}
			conMgr.sdl.SchMakeMessage(&msg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtConMgrBootstrapReq, nil)
			conMgr.sdl.SchSendMessage(&msg)
		}
	}
	return sch.SchEnoNone
}

//
// Get configuration for connection mananger
//
func (conMgr *ConMgr) getConfig() DhtErrno {
	cfg := config.P2pConfig4DhtConManager(conMgr.sdl.SchGetP2pCfgName())
	conMgr.cfg.chainId = cfg.ChainId
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
func (conMgr *ConMgr) lookupConInst(cid *conInstIdentity) ([]*ConInst, ConInstDir) {
	conMgr.lockInstTab.Lock()
	defer conMgr.lockInstTab.Unlock()
	if cid == nil {
		return nil, ConInstDirUnknown
	}
	if cid.dir == ConInstDirInbound || cid.dir == ConInstDirOutbound {
		if inst := conMgr.ciTab[*cid]; inst != nil {
			return []*ConInst{conMgr.ciTab[*cid]}, cid.dir
		}
	} else if cid.dir == ConInstDirAllbound {
		inCid := *cid
		inCid.dir = ConInstDirInbound
		outCid := *cid
		outCid.dir = ConInstDirOutbound
		inst := []*ConInst{
			conMgr.ciTab[inCid],
			conMgr.ciTab[outCid],
		}
		if inst[0] != nil && inst[1] != nil {
			return inst, ConInstDirAllbound
		} else if inst[0] != nil {
			return []*ConInst{inst[0]}, ConInstDirInbound
		} else if inst[1] != nil {
			return []*ConInst{inst[0]}, ConInstDirOutbound
		}
	}
	return nil, ConInstDirUnknown
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr) lookupOutboundConInst(nid *config.NodeID) *ConInst {
	conMgr.lockInstTab.Lock()
	defer conMgr.lockInstTab.Unlock()
	cid := conInstIdentity{
		nid: *nid,
		dir: ConInstDirOutbound,
	}
	return conMgr.ciTab[cid]
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr) lookupInboundConInst(nid *config.NodeID) *ConInst {
	conMgr.lockInstTab.Lock()
	defer conMgr.lockInstTab.Unlock()
	cid := conInstIdentity{
		nid: *nid,
		dir: ConInstDirInbound,
	}
	return conMgr.ciTab[cid]
}

//
// Setup outbound connection instance
//
func (conMgr *ConMgr) setupConInst(ci *ConInst, srcTask interface{}, peer *config.Node, msg interface{}) DhtErrno {

	ci.sdl = conMgr.sdl
	ci.sdlName = conMgr.sdlName
	ci.chainId = conMgr.cfg.chainId
	ci.bootstrapNode = conMgr.cfg.bootstarpNode
	ci.ptnSrcTsk = srcTask
	if ci.srcTaskName = ci.sdl.SchGetTaskName(srcTask); len(ci.srcTaskName) == 0 {
		log.Errorf("setupConInst: source task without name")
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
		log.Errorf("setupConInst: internal errors")
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
func (conMgr *ConMgr) instClosedInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {
	cid := conInstIdentity{
		nid: *msg.Peer,
		dir: ConInstDir(msg.Dir),
	}
	sdl := conMgr.sdl
	rutUpdate := func(node *config.Node) sch.SchErrno {
		update := sch.MsgDhtRutMgrUpdateReq{
			Why: rutMgrUpdate4Closed,
			Eno: DhtEnoNone.GetEno(),
			Seens: []config.Node{
				*node,
			},
			Duras: []time.Duration{
				-1,
			},
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}

	if cis, _ := conMgr.lookupConInst(&cid); cis != nil {
		for _, ci := range cis {
			found := false
			err := false
			if ci != nil {
				found = true
				if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
					log.Debugf("instClosedInd: rutUpdate failed, eno: %d", eno)
					err = true
				}
				conMgr.instCache.Remove(ci)
			}
			log.Debugf("instClosedInd: found: %t, err: %t", found, err)
		}
	}
	return sch.SchEnoNone
}

//
// instance out of service status indication handler
//
func (conMgr *ConMgr) instOutOfServiceInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {
	cid := conInstIdentity{
		nid: *msg.Peer,
		dir: ConInstDir(msg.Dir),
	}
	sdl := conMgr.sdl
	rutUpdate := func(node *config.Node) sch.SchErrno {
		update := sch.MsgDhtRutMgrUpdateReq{
			Why: rutMgrUpdate4Closed,
			Eno: DhtEnoNone.GetEno(),
			Seens: []config.Node{
				*node,
			},
			Duras: []time.Duration{
				-1,
			},
		}
		schMsg := sch.SchMessage{}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, conMgr.ptnRutMgr, sch.EvDhtRutMgrUpdateReq, &update)
		return sdl.SchSendMessage(&schMsg)
	}
	if cis, _ := conMgr.lookupConInst(&cid); cis != nil {
		for _, ci := range cis {
			if ci != nil {
				if eno := rutUpdate(&ci.hsInfo.peer); eno != sch.SchEnoNone {
					log.Debugf("instOutOfServiceInd: rutUpdate failed, sdl: %s, eno: %d",
						conMgr.sdlName, eno)
				}
				if status := ci.getStatus(); status > CisOutOfService {
					log.Debugf("instOutOfServiceInd: mismatched, sdl: %s, status: %d",
						conMgr.sdlName, status)
				} else {
					log.Debugf("instOutOfServiceInd: sdl: %s, inst: %s, dir: %d, current status: %d",
						conMgr.sdlName, ci.name, ci.dir, status)
					cid.dir = ci.dir
					conMgr.instInClosing[cid] = ci
					delete(conMgr.ciTab, cid)
					req := sch.MsgDhtConInstCloseReq{
						Peer: msg.Peer,
						Why:  sch.EvDhtConInstStatusInd,
					}
					msg := sch.SchMessage{}
					sdl.SchMakeMessage(&msg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConInstCloseReq, &req)
					msg.Keep = sch.SchMsgKeepFromPoweroff
					sdl.SchSendMessage(&msg)

				}
			}
		}
	}

	return sch.SchEnoNone
}

//
// deal with connection instance evicted
//
func (conMgr *ConMgr) onInstEvicted(key interface{}, value interface{}) {
	// notice: here we can send EvDhtLsnMgrPauseReq event to listener manager so
	// later connections from outside would not be accepted, since at this moment,
	// max connection threshold had been reached. this is not implemented, for it
	// needs EvDhtLsnMgrResumeReq to be sent when number of connection is under
	// the threshold later.
	k, _ := key.(*instLruKey)
	ci, _ := value.(instLruValue)
	if k == nil || ci == nil {
		log.Debugf("onInstEvicted: invalid key or value")
		return
	}

	log.Debugf("onInstEvicted: send EvDhtConMgrCloseReq, sdl: %s, inst: %s, dir: %d",
		conMgr.sdlName, ci.name, ci.dir)

	req := sch.MsgDhtConMgrCloseReq{
		Task: ConMgrName,
		Peer: &ci.hsInfo.peer,
		Dir:  ci.dir,
	}
	msg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&msg, conMgr.ptnMe, ci.ptnMe, sch.EvDhtConMgrCloseReq, &req)
	if eno := conMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debugf("onInstEvicted: send EvDhtConMgrCloseReq failed," +
			"sdl: %s, inst: %s, dir: %d, eno: %d",
			conMgr.sdlName, ci.name, ci.dir, eno)
	}
}

//
// Switch to public address
//
func (conMgr *ConMgr) switch2NatAddr(proto string) DhtErrno {
	if proto == nat.NATP_TCP {
		conMgr.cfg.local.IP = conMgr.pubTcpIp
		conMgr.cfg.local.TCP = uint16(conMgr.pubTcpPort & 0xffff)
		return DhtEnoNone
	}
	log.Debugf("switch2NatAddr: invalid protocol: %s", proto)
	return DhtEnoParameter
}

//
// switch to public address from nat manager
//
func (conMgr *ConMgr) natMapSwitch() DhtErrno {

	log.Debugf("natMapSwitch: swithching begin...")

	sdl := conMgr.sdl
	msg := sch.SchMessage{}
	sdl.SchMakeMessage(&msg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConMgrPubAddrSwitchBeg, nil)
	sdl.SchSendMessage(&msg)
	log.Debugf("natMapSwitch: EvDhtConMgrPubAddrSwitchBeg sent")

	msg = sch.SchMessage{}
	sdl.SchMakeMessage(&msg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrPauseReq, nil)
	sdl.SchSendMessage(&msg)
	log.Debugf("natMapSwitch: EvDhtLsnMgrPauseReq sent")

	conMgr.instInClosing = make(map[conInstIdentity]*ConInst, 0)

	for _, ci := range conMgr.ciTab {
		po := sch.SchMessage{}
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		po.TgtName = ci.name
		conMgr.sdl.SchSendMessage(&po)
		log.Debugf("natMapSwitch: EvSchPoweroff sent, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, ci.dir)
	}
	conMgr.ciTab = make(map[conInstIdentity]*ConInst, 0)

	for _, ci := range conMgr.ibInstTemp {
		po := sch.SchMessage{}
		conMgr.sdl.SchMakeMessage(&po, conMgr.ptnMe, ci.ptnMe, sch.EvSchPoweroff, nil)
		po.TgtName = ci.name
		conMgr.sdl.SchSendMessage(&po)
		log.Debugf("natMapSwitch: EvSchPoweroff sent, sdl: %s, inst: %s, dir: %d",
			conMgr.sdlName, ci.name, ci.dir)
	}
	conMgr.ibInstTemp = make(map[string]*ConInst, 0)

	log.Debugf("natMapSwitch: call natMapSwitchEnd")
	if eno := conMgr.natMapSwitchEnd(); eno != DhtEnoNone {
		log.Warnf("natMapSwitch: natMapSwitchEnd failed, eno: %d", eno)
		return eno
	}

	return DhtEnoNone
}

//
// post public address switching proc
//
func (conMgr *ConMgr) natMapSwitchEnd() DhtErrno {
	log.Debugf("natMapSwitchEnd: enter pasInNull")
	conMgr.pasStatus = pasInNull
	conMgr.instCache.Purge()
	conMgr.ibInstTemp = make(map[string]*ConInst, 0)
	conMgr.switch2NatAddr(nat.NATP_TCP)

	msg := sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&msg, conMgr.ptnMe, conMgr.ptnLsnMgr, sch.EvDhtLsnMgrResumeReq, nil)
	conMgr.sdl.SchSendMessage(&msg)
	log.Debugf("natMapSwitchEnd: EvDhtLsnMgrResumeReq sent")

	msg = sch.SchMessage{}
	conMgr.sdl.SchMakeMessage(&msg, conMgr.ptnMe, conMgr.ptnDhtMgr, sch.EvDhtConMgrPubAddrSwitchEnd, nil)
	conMgr.sdl.SchSendMessage(&msg)
	log.Debugf("natMapSwitchEnd: EvDhtConMgrPubAddrSwitchEnd sent")

	return DhtEnoNone
}

//
// Signal connection manager ready
//
var mapChConMgrReady = make(map[string]chan bool, 0)

func SetChConMgrReady(name string, ch chan bool) {
	mapChConMgrReady[name] = ch
}

func CloseChConMgrReady(name string) {
	if ch, ok := mapChConMgrReady[name]; ok && ch != nil {
		close(ch)
	}
}

func ConMgrReady(name string) bool {
	r, ok := <- mapChConMgrReady[name]
	if !ok {
		log.Errorf("ConMgrReady: internal error, not found: %s", name)
	}
	return r && ok
}
