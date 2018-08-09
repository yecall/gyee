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
	log "github.com/yeeco/gyee/p2p/logger"
	sch	"github.com/yeeco/gyee/p2p/scheduler"
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
	pcsConnYes					// connected
)

//
// Connection manager
//
type ConMgr struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// my name
	tep			sch.SchUserTaskEp				// task entry
	ptnMe		interface{}						// pointer to task node of myself
	ptnRutMgr	interface{}						// pointer to route manager task node
	ptnQryMgr	interface{}						// pointer to query manager task node
	ciTab		map[conInstIdentity]*ConInst	// connection instance table
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

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conMgr.poweroff(ptn)

	case sch.EvDhtLsnMgrAcceptInd:
		eno = conMgr.acceptInd(msg.Body.(*sch.MsgDhtLsnMgrAcceptInd))

	case sch.EvDhtConInstHandshakeRsp:
		eno = conMgr.handshakeRsp(msg.Body.(*sch.MsgDhtConInstHandshakeRsp))

	case sch.EvDhtLsnMgrStatusInd:
		eno = conMgr.lsnMgrStatusInd(msg.Body.(*sch.MsgDhtLsnMgrStatusInd))

	case sch.EvDhtConMgrConnectReq:
		eno = conMgr.connctReq(msg.Body.(*sch.MsgDhtConMgrConnectReq))

	case sch.EvDhtConMgrCloseReq:
		eno = conMgr.closeReq(msg.Body.(*sch.MsgDhtConMgrCloseReq))

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
	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conMgr *ConMgr)poweroff(ptn interface{}) sch.SchErrno {
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
	return sch.SchEnoNone
}

//
// Close-instance-request handler
//
func (conMgr *ConMgr)closeReq(msg *sch.MsgDhtConMgrCloseReq) sch.SchErrno {
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
	return sch.SchEnoNone
}

//
// Peer removed from route indication handler
//
func (conMgr *ConMgr)rutPeerRemoveInd(msg *sch.MsgDhtRutPeerRemovedInd) sch.SchErrno {
	return sch.SchEnoNone
}


