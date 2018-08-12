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
	"time"
	"sync"
	log "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
)

//
// Dht connection instance
//
type ConInst struct {
	sdl			*sch.Scheduler			// pointer to scheduler
	name		string					// task name
	tep			sch.SchUserTaskEp		// task entry
	ptnMe		interface{}				// pointer to myself task node
	ptnConMgr	interface{}				// pointer to connection manager task node
	ptnSrcTsk	interface{}				// for outbound, the source task requests the connection
	status		conInstStatus			// instance status
	cid			conInstIdentity			// connection instance identity
	con			net.Conn				// connection
	hsInfo		conInstHandshakeInfo	// handshake information
	txPending	[]*conInstTxPkg			// pending package to be sent
	txLock		sync.Mutex				// tx lock
}

//
// Connection instance identity
//
type conInstIdentity struct {
	nid			config.NodeID		// node identity
	dir			conInstDir			// connection direction
}

//
// Connection instance status
//
const (
	cisNull			= iota			// null, not inited
	cisConnected					// connected
	cisInHandshaking				// handshaking
	cisHandshaked					// handshaked
	cisInService					// in service
	cisClosed						// closed
)

type conInstStatus int

//
// Connection instance direction
//
const (
	conInstDirInbound	= 0			// out from local
	conInstDirOutbound	= 1			// in from peer
	conInstDirAllbound	= 2			// in & out
)

type conInstDir int

//
// Handshake information
//
type conInstHandshakeInfo struct {
	peer		config.Node			// peer node identity
	extra		interface{}			// extra information
}

//
// Outcoming package
//
type conInstTxPkg struct {
	submitTime	time.Time			// time the payload submitted
	payload		[]byte				// payload buffer
}

//
// Create connection instance
//
func newConInst(postFixed string) *ConInst {

	conInst := ConInst {
		name:		"conInst" + postFixed,
		tep:		nil,
		ptnMe:		nil,
		ptnConMgr:	nil,
		ptnSrcTsk:	nil,
	}

	conInst.tep = conInst.conInstProc

	return &conInst
}

//
// Entry point exported to shceduler
//
func (conInst *ConInst)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return conInst.tep(ptn, msg)
}

//
// Connection instance entry
//
func (conInst *ConInst)conInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		log.LogCallerFileLine("conInstProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	var eno = sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = conInst.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = conInst.poweroff(ptn)

	case sch.EvDhtConInstHandshakeReq:
		eno = conInst.handshakeReq(msg.Body.(*sch.MsgDhtConInstHandshakeReq))

	case sch.EvDhtConInstCloseReq:
		eno = conInst.closeReq(msg.Body.(*sch.MsgDhtConInstCloseReq))

	case sch.EvDhtConInstTxDataReq:
		eno = conInst.txDataReq(msg.Body.(*sch.MsgDhtConInstTxDataReq))

	default:
		log.LogCallerFileLine("conInstProc: unknown event: %d", msg.Id)
		return sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (conInst *ConInst)poweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (conInst *ConInst)poweroff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Handshake-request handler
//
func (conInst *ConInst)handshakeReq(msg *sch.MsgDhtConInstHandshakeReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance-close-request handler
//
func (conInst *ConInst)closeReq(msg *sch.MsgDhtConInstCloseReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Send-data-request handler
//
func (conInst *ConInst)txDataReq(msg *sch.MsgDhtConInstTxDataReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Map connection instance status to "peer connection status"
//
func ConInstStatus2PCS(cis conInstStatus) conMgrPeerConnStat {
	cis2pcs := map[conInstStatus] conMgrPeerConnStat {
		cisNull:			pcsConnNo,
		cisConnected:		pcsConnNo,
		cisInHandshaking:	pcsConnNo,
		cisHandshaked:		pcsConnNo,
		cisInService:		pcsConnYes,
		cisClosed:			pcsConnNo,
	}
	return cis2pcs[cis]
}