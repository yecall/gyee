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
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
	"time"
)

//
// Connection manager name registered in scheduler
//
const ConMgrName = sch.DhtConMgrName

//
// Peer connection status
//
type conMgrPeerConnStat int

const (
	pcsConnNo	= iota			// not connected
	pcsConnYes					// connected in service
)

//
// Connection manager configuration
//
type conMgrCfg struct {
	maxCon		int								// max number of connection
	hsTimeout	time.Duration					// handshake timeout duration
}

//
// Connection manager
//
type ConMgr struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// my name
	cfg			conMgrCfg						// configuration
	tep			sch.SchUserTaskEp				// task entry
	ptnMe		interface{}						// pointer to task node of myself
	ptnRutMgr	interface{}						// pointer to route manager task node
	ptnQryMgr	interface{}						// pointer to query manager task node
	ciTab		map[conInstIdentity]*ConInst	// connection instance table
	ciSeq		int64							// connection instance sequence number
}

//
// Create route manager
//
func NewConMgr() *ConMgr {

	conMgr := ConMgr{
		sdl:		nil,
		name:		ConMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnQryMgr:	nil,
		ciTab:		make(map[conInstIdentity]*ConInst, 0),
		ciSeq:		0,
	}

	conMgr.tep = conMgr.conMgrProc

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

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("conMgrProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

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

	default:
		log.LogCallerFileLine("conMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (conMgr *ConMgr)poweron(ptn interface{}) sch.SchErrno {

	sdl := sch.SchGetScheduler(ptn)

	conMgr.ptnMe = ptn
	_, conMgr.ptnRutMgr = sdl.SchGetTaskNodeByName(RutMgrName)
	_, conMgr.ptnQryMgr = sdl.SchGetTaskNodeByName(QryMgrName)

	if conMgr.ptnRutMgr == nil || conMgr.ptnQryMgr == nil {
		log.LogCallerFileLine("poweron: internal errors")
		return sch.SchEnoInternal
	}

	if dhtEno := conMgr.getConfig(); dhtEno != DhtEnoNone {
		log.LogCallerFileLine("poweron: getConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conMgr *ConMgr)poweroff(ptn interface{}) sch.SchErrno {

	log.LogCallerFileLine("poweroff: task will be done ...")

	po := sch.SchMessage{}
	for _, ci := range conMgr.ciTab {
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
	return sch.SchEnoNone
}

//
// Handshake response handler
//
func (conMgr *ConMgr)handshakeRsp(msg *sch.MsgDhtConInstHandshakeRsp) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Listener manager status indication handler
//
func (conMgr *ConMgr)lsnMgrStatusInd(msg *sch.MsgDhtLsnMgrStatusInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Connect-request handler
//
func (conMgr *ConMgr)connctReq(msg *sch.MsgDhtConMgrConnectReq) sch.SchErrno {

	var rsp = sch.MsgDhtConMgrConnectRsp {
		Peer:	msg.Peer,
		Eno:	DhtEnoNone,
	}

	var sender = msg.Task
	var sdl = conMgr.sdl

	var rsp2Sender = func(eno DhtErrno) sch.SchErrno {
		msg := sch.SchMessage{}
		rsp.Eno = int(eno)
		sdl.SchMakeMessage(&msg, conMgr.ptnMe, sender, sch.EvDhtConMgrConnectRsp, &rsp)
		return sdl.SchSendMessage(&msg)
	}

	if conMgr.lookupOutboundConInst(&msg.Peer.ID) != nil {
		log.LogCallerFileLine("connctReq: outbound instance duplicated, id: %x", msg.Peer.ID)
		return rsp2Sender(DhtErrno(DhtEnoDuplicated))
	}

	ci := newConInst(fmt.Sprintf("%d", conMgr.ciSeq))
	conMgr.setupOutboundInst(ci, msg.Task, msg.Peer)
	conMgr.ciSeq++

	td := sch.SchTaskDescription{
		Name:		ci.name,
		MbSize:		-1,
		Ep:			ci,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		nil,
	}

	eno, ptn := conMgr.sdl.SchCreateTask(&td)
	if eno != sch.SchEnoNone || ptn == nil {
		log.LogCallerFileLine("")
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
		dir:	conInstDirOutbound,
	}
	conMgr.ciTab[cid] = ci

	return rsp2Sender(DhtErrno(DhtEnoNone))
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)closeReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {

	cid := conInstIdentity {
		nid:	msg.Peer.ID,
		dir:	conInstDir(msg.Dir),
	}

	schMsg := sch.SchMessage{}
	sender := msg.Task
	sdl := conMgr.sdl

	rsp2Sender := func(eno DhtErrno) sch.SchErrno{
		rsp := sch.MsgDhtConMgrCloseRsp{
			Eno:	int(eno),
			Peer:	msg.Peer,
			Dir:	msg.Dir,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, sender, sch.EvDhtConMgrCloseRsp, &rsp)
		return sdl.SchSendMessage(&schMsg)
	}

	req2Inst := func(inst *ConInst) sch.SchErrno {
		req := sch.MsgDhtConInstCloseReq{
			Peer:	&msg.Peer.ID,
			Why:	sch.EvDhtConMgrCloseReq,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	if !found {
		return rsp2Sender(DhtEnoNotFound)
	}

	if err {
		return rsp2Sender(DhtEnoScheduler)
	}

	return sch.SchEnoNone
}

//
// Send-data-request handler
//
func (conMgr *ConMgr)sendReq(msg *sch.MsgDhtConMgrSendReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance status indication handler
//
func (conMgr *ConMgr)instStatusInd(msg *sch.MsgDhtConInstStatusInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)instCloseRsp(msg *sch.MsgDhtConInstCloseRsp) sch.SchErrno {

	cid := conInstIdentity {
		nid:	msg.Peer.ID,
		dir:	conInstDir(msg.Dir),
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

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if rsp2Sender(DhtEnoNone, &ci.hsInfo.peer, ci.ptnSrcTsk) != sch.SchEnoNone {
				err = true
			} else {
				delete(conMgr.ciTab, cid)
			}
		}
	}

	if !found {
		log.LogCallerFileLine("instCloseRsp: not found, id: %x", msg.Peer)
	}

	if err {
		log.LogCallerFileLine("instCloseRsp: seems some errors")
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
		nid:	msg.Peer.ID,
		dir:	conInstDirAllbound,
	}

	schMsg := sch.SchMessage{}
	sdl := conMgr.sdl

	req2Inst := func(inst *ConInst) sch.SchErrno {
		req := sch.MsgDhtConInstCloseReq{
			Peer:	&msg.Peer.ID,
			Why:	sch.EvDhtRutPeerRemovedInd,
		}
		sdl.SchMakeMessage(&schMsg, conMgr.ptnMe, inst.ptnMe, sch.EvDhtConInstCloseReq, &req)
		return sdl.SchSendMessage(&schMsg)
	}

	found := false
	err := false

	cis := conMgr.lookupConInst(&cid)
	for _, ci := range cis {
		if ci != nil {
			found = true
			if req2Inst(ci) != sch.SchEnoNone {
				err = true
			}
		}
	}

	if !found {
		log.LogCallerFileLine("rutPeerRemoveInd: not found, id: %x", msg.Peer)
	}

	if err {
		log.LogCallerFileLine("rutPeerRemoveInd: seems some errors")
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Get configuration for connection mananger
//
func (conMgr *ConMgr)getConfig() DhtErrno {
	return DhtEnoNone
}

//
// Lookup connection instance by instance identity
//
func (conMgr *ConMgr)lookupConInst(cid *conInstIdentity) []*ConInst {

	if cid == nil {
		return []*ConInst{nil}
	}

	if cid.dir == conInstDirInbound || cid.dir == conInstDirOutbound {
		return []*ConInst{conMgr.ciTab[*cid]}
	} else if cid.dir == conInstDirAllbound {
		inCid := *cid
		inCid.dir = conInstDirInbound
		outCid := *cid
		outCid.dir = conInstDirOutbound
		return []*ConInst {
			conMgr.ciTab[inCid],
			conMgr.ciTab[outCid],
		}
	}

	return []*ConInst{nil}
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupOutboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	conInstDirOutbound,
	}
	return conMgr.lookupConInst(ci)[0]
}

//
// Lookup outbound connection instance by node identity
//
func (conMgr *ConMgr)lookupInboundConInst(nid *config.NodeID) *ConInst {
	ci := &conInstIdentity {
		nid:	*nid,
		dir:	conInstDirInbound,
	}
	return conMgr.lookupConInst(ci)[0]
}

//
// Setup outbound connection instance
//
func (conMgr *ConMgr)setupOutboundInst(ci *ConInst, srcTask interface{}, peer *config.Node) DhtErrno {
	ci.sdl = conMgr.sdl
	ci.ptnConMgr = conMgr.ptnMe
	ci.ptnSrcTsk = srcTask
	ci.hsInfo.peer = *peer
	ci.cid = conInstIdentity{
		nid: peer.ID,
		dir: conInstDirInbound,
	}
	return DhtEnoNone
}
