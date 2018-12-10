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

package shell

import (
	"github.com/ethereum/go-ethereum/log"
	"github.com/yeeco/gyee/p2p/config"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	"github.com/yeeco/gyee/p2p/peer"
)

const (
	ShMgrName = sch.ShMgrName					// name registered in scheduler
	rxChanSize = 128							// total rx chan capacity
)

type shellPeerID struct {
	snid		config.SubNetworkID 			// sub network identity
	dir    		int         	  				// direct
	nodeId		config.NodeID					// node identity
}

type shellPeerInst struct {
	shellPeerID									// shell peer identity
	txChan		chan *peer.P2pPackage			// tx channel of peer instance
	rxChan		chan *peer.P2pPackageRx			// rx channel of peer instance
	hsInfo		*peer.Handshake					// handshake info about peer
	status		int								// active peer instance status
}

const (
	pisActive	= iota		// active status
	pisClosing				// in-closing status
)

type shellManager struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// my name
	tep				sch.SchUserTaskEp				// task entry
	ptnMe			interface{}						// pointer to task node of myself
	ptnPeMgr		interface{}						// pointer to task node of peer manager
	ptnTabMgr		interface{}						// pointer to task node of table manager
	peerActived		map[shellPeerID]shellPeerInst	// active peers
	rxChan			chan *peer.P2pPackageRx			// total rx channel, for rx packages from all intances
}

//
// Create shell manager
//
func NewShellMgr() *shellManager  {
	shMgr := shellManager {
		name: ShMgrName,
		peerActived: make(map[shellPeerID]shellPeerInst, 0),
		rxChan: make(chan *peer.P2pPackageRx, rxChanSize),
	}
	shMgr.tep = shMgr.shMgrProc
	return &shMgr
}

//
// Entry point exported to scheduler
//
func (shMgr *shellManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return shMgr.tep(ptn, msg)
}

//
// Shell manager entry
//
func (shMgr *shellManager)shMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	eno := sch.SchEnoUnknown
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = shMgr.powerOn(ptn)
	case sch.EvSchPoweroff:
		eno = shMgr.powerOff(ptn)
	case sch.EvShellPeerActiveInd:
		eno = shMgr.peerActiveInd(msg.Body.(*sch.MsgShellPeerActiveInd))
	case sch.EvShellPeerCloseCfm:
		eno = shMgr.peerCloseCfm(msg.Body.(*sch.MsgShellPeerCloseCfm))
	case sch.EvShellPeerCloseInd:
		eno = shMgr.peerCloseInd(msg.Body.(*sch.MsgShellPeerCloseInd))
	case sch.EvShellPeerAskToCloseInd:
		eno = shMgr.peerAskToCloseInd(msg.Body.(*sch.MsgShellPeerAskToCloseInd))
	case sch.EvShellReconfigReq:
		eno = shMgr.reconfigReq(msg.Body.(*sch.MsgShellReconfigReq))
	case sch.EvShellBroadcastReq:
		eno = shMgr.broadcastReq(msg.Body.(*sch.MsgShellBroadcastReq))
	default:
		log.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (shMgr *shellManager)powerOn(ptn interface{}) sch.SchErrno {
	shMgr.ptnMe = ptn
	shMgr.sdl = sch.SchGetScheduler(ptn)
	_, shMgr.ptnPeMgr = shMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName)
	_, shMgr.ptnTabMgr = shMgr.sdl.SchGetUserTaskNode(sch.TabMgrName)
	return sch.SchEnoNone
}

func (shMgr *shellManager)powerOff(ptn interface{}) sch.SchErrno {
	log.Debug("powerOff: task will be done ...")
	return shMgr.sdl.SchTaskDone(shMgr.ptnMe, sch.SchEnoPowerOff)
}

func (shMgr *shellManager)peerActiveInd(ind *sch.MsgShellPeerActiveInd) sch.SchErrno {
	txChan, _ := ind.TxChan.(chan *peer.P2pPackage)
	rxChan, _ := ind.RxChan.(chan *peer.P2pPackageRx)
	peerInfo, _ := ind.PeerInfo.(*peer.Handshake)
	peerId := shellPeerID {
		snid: peerInfo.Snid,
		nodeId: peerInfo.NodeId,
		dir: peerInfo.Dir,
	}
	peerInst := shellPeerInst {
		shellPeerID: peerId,
		txChan: txChan,
		rxChan: rxChan,
		hsInfo: peerInfo,
		status: pisActive,
	}
	if _, dup := shMgr.peerActived[peerId]; dup {
		log.Debug("peerActiveInd: duplicated, peerId: %+v", peerId)
		return sch.SchEnoUserTask
	}
	shMgr.peerActived[peerId] = peerInst

	go func() {
		for {
			select {
			case rxPkg, ok := <-peerInst.rxChan:
				if !ok {
					log.Debug("exit for rxChan closed")
					return
				}
				shMgr.rxChan<-rxPkg
			}
		}
	}()

	return sch.SchEnoNone
}

func (shMgr *shellManager)peerCloseCfm(cfm *sch.MsgShellPeerCloseCfm) sch.SchErrno {
	peerId := shellPeerID {
		snid: cfm.Snid,
		nodeId: cfm.PeerId,
		dir: cfm.Dir,
	}
	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		log.Debug("peerCloseCfm: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisClosing {
		log.Debug("peerCloseCfm: status mismatched, status: %d, peerId: %+v", peerInst.status, peerId)
		return sch.SchEnoMismatched
	}
	log.Debug("peerCloseCfm: peerId: %+v", peerId)
	delete(shMgr.peerActived, peerId)
	return sch.SchEnoNone
}

func (shMgr *shellManager)peerCloseInd(ind *sch.MsgShellPeerCloseInd) sch.SchErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement, instead, event EvShellPeerAskToCloseInd should be sent
	// to us to do this.
	panic("peerCloseInd: should never be called!!!")
	return sch.SchEnoInternal
}

func (shMgr *shellManager)peerAskToCloseInd(ind *sch.MsgShellPeerAskToCloseInd) sch.SchErrno {
	peerId := shellPeerID {
		snid: ind.Snid,
		nodeId: ind.PeerId,
		dir: ind.Dir,
	}
	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		log.Debug("peerAskToCloseInd: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisActive {
		log.Debug("peerAskToCloseInd : status mismatched, status: %d, peerId: %+v", peerInst.status, peerId)
		return sch.SchEnoMismatched
	} else {
		log.Debug("peerAskToCloseInd: send EvPeCloseReq to peer manager...")
		var req = sch.MsgPeCloseReq {
			Ptn: nil,
			Snid: peerId.snid,
			Node: config.Node{
				ID: peerId.nodeId,
			},
			Dir: peerId.dir,
		}
		msg := sch.SchMessage{}
		shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnPeMgr, sch.EvPeCloseReq, &req)
		shMgr.sdl.SchSendMessage(&msg)
		peerInst.status = pisClosing
		return sch.SchEnoNone
	}
}

func (shMgr *shellManager)GetRxChan() chan *peer.P2pPackageRx {
	return shMgr.rxChan
}

func (shMgr *shellManager)reconfigReq(req *sch.MsgShellReconfigReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnPeMgr, sch.EvShellReconfigReq, req)
	if eno := shMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		return eno
	}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnTabMgr, sch.EvShellReconfigReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *shellManager)broadcastReq(req *sch.MsgShellBroadcastReq) sch.SchErrno {
	return sch.SchEnoNone
}
