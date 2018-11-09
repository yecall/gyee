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
	log "github.com/ethereum/go-ethereum/log"
	config "github.com/yeeco/gyee/p2p/config"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	peer "github.com/yeeco/gyee/p2p/peer"
)

const ShMgrName = sch.ShMgrName

type ShellPeerID struct {
	Snid		config.SubNetworkID // sub network identity
	Dir    		int         	  	// direct
	NodeId		config.NodeID		// node identity
}

type ShellPeerInst struct {
	ShellPeerID									// shell peer identity
	TxChan		chan *peer.P2pPackage			// tx channel of peer instance
	RxChan		chan *peer.P2pPackageRx			// rx channel of peer instance
	HsInfo		*peer.Handshake					// handshake info about peer
	status			int							// active peer instance status
}

const (
	pisActive	= iota		// active status
	pisClosing				// in-closing status
)

type ShellManager struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// my name
	tep				sch.SchUserTaskEp				// task entry
	ptnMe			interface{}						// pointer to task node of myself
	ptnPeMgr		interface{}						// pointer to task node of peer manager
	peerActived		map[ShellPeerID]ShellPeerInst	// active peers
}

//
// Create shell manager
//
func NewShellMgr() *ShellManager  {
	shMgr := ShellManager {
		name: ShMgrName,
		peerActived: make(map[ShellPeerID]ShellPeerInst, 0),
	}
	shMgr.tep = shMgr.shMgrProc
	return &shMgr
}

//
// Entry point exported to scheduler
//
func (shMgr *ShellManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return shMgr.tep(ptn, msg)
}

//
// Shell manager entry
//
func (shMgr *ShellManager)shMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
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
	default:
		log.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (shMgr *ShellManager)powerOn(ptn interface{}) sch.SchErrno {
	shMgr.ptnMe = ptn
	shMgr.sdl = sch.SchGetScheduler(ptn)
	_, shMgr.ptnPeMgr = shMgr.sdl.SchGetTaskNodeByName(sch.PeerMgrName)
	return sch.SchEnoNone
}

func (shMgr *ShellManager)powerOff(ptn interface{}) sch.SchErrno {
	log.Debug("powerOff: task will be done ...")
	return shMgr.sdl.SchTaskDone(shMgr.ptnMe, sch.SchEnoPowerOff)
}

func (shMgr *ShellManager)peerActiveInd(ind *sch.MsgShellPeerActiveInd) sch.SchErrno {
	txChan, _ := ind.TxChan.(chan *peer.P2pPackage)
	rxChan, _ := ind.RxChan.(chan *peer.P2pPackageRx)
	peerInfo, _ := ind.PeerInfo.(*peer.Handshake)
	peerId := ShellPeerID {
		Snid: peerInfo.Snid,
		NodeId: peerInfo.NodeId,
		Dir: peerInfo.Dir,
	}
	peer := ShellPeerInst {
		ShellPeerID: peerId,
		TxChan: txChan,
		RxChan: rxChan,
		HsInfo: peerInfo,
		status: pisActive,
	}
	if _, dup := shMgr.peerActived[peerId]; dup {
		log.Debug("peerActiveInd: duplicated, peerId: %+v", peerId)
		return sch.SchEnoUserTask
	}
	shMgr.peerActived[peerId] = peer
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerCloseCfm(cfm *sch.MsgShellPeerCloseCfm) sch.SchErrno {
	peerId := ShellPeerID {
		Snid: cfm.Snid,
		NodeId: cfm.PeerId,
		Dir: cfm.Dir,
	}
	if peer, ok := shMgr.peerActived[peerId]; !ok {
		log.Debug("peerCloseCfm: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peer.status != pisClosing {
		log.Debug("peerCloseCfm: status mismatched, status: %d, peerId: %+v", peer.status, peerId)
		return sch.SchEnoMismatched
	}
	log.Debug("peerCloseCfm: peerId: %+v", peerId)
	delete(shMgr.peerActived, peerId)
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerCloseInd(ind *sch.MsgShellPeerCloseInd) sch.SchErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement, instead, event EvShellPeerAskToCloseInd should be sent
	// to us to do this.
	panic("peerCloseInd: should never be called!!!")
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerAskToCloseInd(ind *sch.MsgShellPeerAskToCloseInd) sch.SchErrno {
	return sch.SchEnoNone
}

