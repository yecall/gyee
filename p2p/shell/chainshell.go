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
	"bytes"
	config	"github.com/yeeco/gyee/p2p/config"
	peer	"github.com/yeeco/gyee/p2p/peer"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)

//
// debug
//
type chainShellLogger struct {
	debug__		bool
}

var chainLog = chainShellLogger {
	debug__:	false,
}

func (log chainShellLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// chain shell
//

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

type ShellManager struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// my name
	tep				sch.SchUserTaskEp				// task entry
	ptnMe			interface{}						// pointer to task node of myself
	ptnPeMgr		interface{}						// pointer to task node of peer manager
	ptnTabMgr		interface{}						// pointer to task node of table manager
	peerActived		map[shellPeerID]*shellPeerInst	// active peers
	rxChan			chan *peer.P2pPackageRx			// total rx channel, for rx packages from all instances
}

//
// Create shell manager
//
func NewShellMgr() *ShellManager  {
	shMgr := ShellManager {
		name: ShMgrName,
		peerActived: make(map[shellPeerID]*shellPeerInst, 0),
		rxChan: make(chan *peer.P2pPackageRx, rxChanSize),
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
	case sch.EvShellReconfigReq:
		eno = shMgr.reconfigReq(msg.Body.(*sch.MsgShellReconfigReq))
	case sch.EvShellBroadcastReq:
		eno = shMgr.broadcastReq(msg.Body.(*sch.MsgShellBroadcastReq))
	default:
		chainLog.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (shMgr *ShellManager)powerOn(ptn interface{}) sch.SchErrno {
	shMgr.ptnMe = ptn
	shMgr.sdl = sch.SchGetScheduler(ptn)
	_, shMgr.ptnPeMgr = shMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName)
	_, shMgr.ptnTabMgr = shMgr.sdl.SchGetUserTaskNode(sch.TabMgrName)
	return sch.SchEnoNone
}

func (shMgr *ShellManager)powerOff(ptn interface{}) sch.SchErrno {
	chainLog.Debug("powerOff: task will be done ...")
	return shMgr.sdl.SchTaskDone(shMgr.ptnMe, sch.SchEnoPowerOff)
}

func (shMgr *ShellManager)peerActiveInd(ind *sch.MsgShellPeerActiveInd) sch.SchErrno {
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
		chainLog.Debug("peerActiveInd: duplicated, peerId: %+v", peerId)
		return sch.SchEnoUserTask
	}
	shMgr.peerActived[peerId] = &peerInst

	chainLog.Debug("peerActiveInd: peer info: %+v", *peerInfo)

	go func() {
		for {
			select {
			case rxPkg, ok := <-peerInst.rxChan:
				if !ok {
					chainLog.Debug("peerActiveInd: exit for rxChan closed, peer info: %+v", *peerInfo)
					return
				}
				shMgr.rxChan<-rxPkg
			}
		}
	}()

	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerCloseCfm(cfm *sch.MsgShellPeerCloseCfm) sch.SchErrno {
	peerId := shellPeerID {
		snid: cfm.Snid,
		nodeId: cfm.PeerId,
		dir: cfm.Dir,
	}
	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		chainLog.Debug("peerCloseCfm: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisClosing {
		chainLog.Debug("peerCloseCfm: status mismatched, status: %d, peerId: %+v", peerInst.status, peerId)
		return sch.SchEnoMismatched
	} else {
		chainLog.Debug("peerCloseCfm: peer info: %+v", *peerInst.hsInfo)
		delete(shMgr.peerActived, peerId)
		return sch.SchEnoNone
	}
}

func (shMgr *ShellManager)peerCloseInd(ind *sch.MsgShellPeerCloseInd) sch.SchErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement, instead, event EvShellPeerAskToCloseInd should be sent
	// to us to do this.
	panic("peerCloseInd: should never be called!!!")
	return sch.SchEnoInternal
}

func (shMgr *ShellManager)peerAskToCloseInd(ind *sch.MsgShellPeerAskToCloseInd) sch.SchErrno {
	peerId := shellPeerID {
		snid: ind.Snid,
		nodeId: ind.PeerId,
		dir: ind.Dir,
	}
	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		chainLog.Debug("peerAskToCloseInd: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisActive {
		chainLog.Debug("peerAskToCloseInd : status mismatched, status: %d, peerId: %+v", peerInst.status, peerId)
		return sch.SchEnoMismatched
	} else {
		chainLog.Debug("peerAskToCloseInd: send EvPeCloseReq to peer manager, peer info: %+v", *peerInst.hsInfo)
		req := sch.MsgPeCloseReq {
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

func (shMgr *ShellManager)GetRxChan() chan *peer.P2pPackageRx {
	return shMgr.rxChan
}

func (shMgr *ShellManager)reconfigReq(req *sch.MsgShellReconfigReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnPeMgr, sch.EvShellReconfigReq, req)
	if eno := shMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		return eno
	}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnTabMgr, sch.EvShellReconfigReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *ShellManager)broadcastReq(req *sch.MsgShellBroadcastReq) sch.SchErrno {
	
	//
	// MSBR_MT_EV(event):
	// the local node must be a validator, and the Ev should be broadcast
	// over the validator-subnet.
	//
	// MSBR_MT_TX(transaction):
	// if local node is a validator, the Tx should be broadcast over the
	// validator-subnet; else the Tx should be broadcast over the dynamic
	// subnet.
	//
	// MSBR_MT_BLKH(block header):
	// the Bh should be broadcast over the any-subnet.
	//
	// MSBR_MT_BLK(block): would not come here, blocks just needed to be
	// backup into DHT.
	//

	switch req.MsgType {

	case sch.MSBR_MT_EV:
		for id, pe := range shMgr.peerActived {
			if pe.status != pisActive {
				chainLog.Debug("broadcastReq: not active, snid: %x, peer: %s", id.snid, pe.hsInfo.IP.String())
			} else if bytes.Compare(id.snid[0:], config.VSubNet[0:]) == 0 {
				shMgr.send2Peer(pe, req)
			}
		}

	case sch.MSBR_MT_TX:
		for id, pe := range shMgr.peerActived {
			if pe.status != pisActive {
				chainLog.Debug("broadcastReq: not active, snid: %x, peer: %s", id.snid, pe.hsInfo.IP.String())
			} else if bytes.Compare(id.snid[0:], config.VSubNet[0:]) == 0 {
				shMgr.send2Peer(pe, req)
			} else if bytes.Compare(id.snid[0:], req.LocalSnid) == 0 {
				 shMgr.send2Peer(pe, req)
			}
		}

	case sch.MSBR_MT_BLKH:
		for id, pe := range shMgr.peerActived {
			if pe.status != pisActive {
				chainLog.Debug("broadcastReq: not active, snid: %x, peer: %s", id.snid, pe.hsInfo.IP.String())
			} else if bytes.Compare(id.snid[0:], config.AnySubNet[0:]) == 0 {
				shMgr.send2Peer(pe, req)
			}
		}

	default:
		chainLog.Debug("broadcastReq: invalid message type: %d", req.MsgType)
		return sch.SchEnoParameter
	}

	return sch.SchEnoNone
}

func (shMgr *ShellManager)bcr2Package(req *sch.MsgShellBroadcastReq) *peer.P2pPackage {
	pkg := new(peer.P2pPackage)
	pkg.Pid = uint32(peer.PID_EXT)
	pkg.Mid = uint32(req.MsgType)
	pkg.Key = req.Key
	pkg.PayloadLength = uint32(len(req.Data))
	pkg.Payload = req.Data
	return pkg
}

func (shMgr *ShellManager)send2Peer(peer *shellPeerInst, req *sch.MsgShellBroadcastReq) sch.SchErrno {
	if len(peer.txChan) >= cap(peer.txChan) {
		chainLog.Debug("send2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			peer.snid, peer.dir, peer.nodeId)
		return sch.SchEnoResource
	}

	if pkg := shMgr.bcr2Package(req); pkg == nil {
		chainLog.Debug("send2Peer: bcr2Package failed")
		return sch.SchEnoUserTask
	} else {
		peer.txChan<-pkg
		return sch.SchEnoNone
	}
}
