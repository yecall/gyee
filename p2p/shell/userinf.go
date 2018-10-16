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
	"fmt"
	"github.com/yeeco/gyee/p2p/peer"
	log "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


//
// errno about this interface
//
type P2pErrno	int

const (
	P2pEnoNone		P2pErrno = 0	// none of errors
	P2pEnoParameter	P2pErrno = 1	// invalid parameters
	P2pEnoScheduler	P2pErrno = 2	// shceduler
	P2pEnoNotImpl	P2pErrno = 3	// not implemented
	P2pEnoInternal	P2pErrno = 4	// internal
	P2pEnoUnknown	P2pErrno = 5	// unknown
	P2pEnoMax		P2pErrno = 6	// max, for bound checking
)

//
// Description about user interface errno
//
var P2pErrnoDescription = []string {
	"none of errors",
	"invalid parameters",
	"shceduler",
	"not implemented",
	"internal",
	"unknown",
	"max value can errno be",
}

//
// Errno string
//
func (eno P2pErrno) P2pErrnoString() string {
	if eno < P2pEnoNone || eno >= P2pEnoMax {
		return ""
	}
	return P2pErrnoDescription[eno]
}

//
// Error interface
//
func (eno P2pErrno) Error() string {
	return eno.P2pErrnoString()
}

//
// Register user callback function to p2p
//

const (
	P2pIndCb = peer.P2pIndCb	// callback type for indication
	P2pPkgCb = peer.P2pPkgCb	// callback type for incoming packages
)

const (
	P2pIndPeerActivated	= peer.P2pIndPeerActivated	// indication for a peer activated to work
	P2pIndConnStatus	= peer.P2pIndConnStatus		// indication for peer connection status changed
	P2pIndPeerClosed	= peer.P2pIndPeerClosed		// indication for peer connection closed
)

func P2pRegisterCallback(what int, cb interface{}, target interface{}) P2pErrno {

	if what != P2pIndCb && what != P2pPkgCb {
		log.LogCallerFileLine("P2pRegisterCallback: unknown what is: %d", what)
		return P2pEnoParameter
	}

	if what == P2pIndCb {

		sdl := target.(*sch.Scheduler)
		peMgr := sdl.SchGetUserTaskIF(sch.PeerMgrName).(*peer.PeerManager)

		sdlName := sdl.SchGetP2pCfgName()
		log.LogCallerFileLine("P2pRegisterCallback: Lock4Cb.Lock, sdlName: %s", sdlName)
		peMgr.Lock4Cb.Lock()

		if peMgr.P2pIndHandler != nil {
			log.LogCallerFileLine("P2pRegisterCallback: old handler will be overlapped")
		}

		if cb == nil {
			log.LogCallerFileLine("P2pRegisterCallback: user registers nil indication handler")
		}

		peMgr.P2pIndHandler = cb.(peer.P2pIndCallback)

		peMgr.Lock4Cb.Unlock()
		log.LogCallerFileLine("P2pRegisterCallback: Lock4Cb.Unlock, sdlName: %s", sdlName)

		return P2pEnoNone
	}

	var peerInst = target

	if peerInst == nil {
		log.LogCallerFileLine("P2pRegisterCallback: nil task node pointer")
		return P2pEnoParameter
	}

	if cb == nil {
		log.LogCallerFileLine("P2pRegisterCallback: user registers nil package handler")
	}

	if eno := peer.SetP2pkgCallback(cb, peerInst); eno != peer.PeMgrEnoNone {
		log.LogCallerFileLine("P2pRegisterCallback: " +
			"SetP2pkgCallback failed, eno: %d",
			eno)
		return P2pEnoInternal
	}

	return P2pEnoNone
}

//
// Send message to peer
//
func P2pSendPackage(pkg *peer.P2pPackage2Peer) P2pErrno {

	if eno := peer.SendPackage(pkg); eno != peer.PeMgrEnoNone {

		log.LogCallerFileLine("P2pSendPackage: " +
			"SendPackage failed, eno: %d, pkg: %s",
			eno,
			fmt.Sprintf("%+v", *pkg))

		return P2pEnoInternal
	}

	return P2pEnoNone
}

//
// Disconnect peer
//
func P2pClosePeer(sdl *sch.Scheduler, snid *peer.SubNetworkID, id *peer.PeerId) P2pErrno {

	peMgr := sdl.SchGetUserTaskIF(sch.PeerMgrName).(*peer.PeerManager)

	if eno := peMgr.ClosePeer(snid, id); eno != peer.PeMgrEnoNone {

		log.LogCallerFileLine("P2pSendPackage: " +
			"ClosePeer failed, eno: %d, peer: %s",
			eno,
			fmt.Sprintf("%+v", *id))

		return P2pEnoInternal
	}

	return P2pEnoNone
}

//
// Turn off specific p2p instance
//
func P2pPoweroff(p2pInst *sch.Scheduler) P2pErrno {

	stopChain := make(chan bool, 1)

	if eno := P2pStop(p2pInst, stopChain); eno != sch.SchEnoNone {
		log.LogCallerFileLine("P2pPoweroff: P2pStop failed, eno: %d", eno)
		close(stopChain)
		return P2pEnoScheduler
	}

	<-stopChain
	close(stopChain)

	return P2pEnoNone
}
