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
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

const ShMgrName = sch.ShMgrName

type ShellManager struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// my name
	tep			sch.SchUserTaskEp	// task entry
	ptnMe		interface{}			// pointer to task node of myself
	ptnPeMgr	interface{}			// pointer to task node of peer manager
	ptnDhtMgr	interface{}			// pointer to task node dht manager
}

//
// Create shell manager
//
func NewShellMgr() *ShellManager  {
	shMgr := ShellManager {
		name: ShMgrName,
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
	default:
		log.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (shMgr *ShellManager)powerOn(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

func (shMgr *ShellManager)powerOff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerActiveInd(ind *sch.MsgShellPeerActiveInd) sch.SchErrno {
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerCloseCfm(cfm *sch.MsgShellPeerCloseCfm) sch.SchErrno {
	return sch.SchEnoNone
}

func (shMgr *ShellManager)peerCloseInd(ind *sch.MsgShellPeerCloseInd) sch.SchErrno {
	return sch.SchEnoNone
}
