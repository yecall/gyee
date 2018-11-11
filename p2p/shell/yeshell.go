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
	log "github.com/yeeco/gyee/p2p/logger"
	config "github.com/yeeco/gyee/p2p/config"
	yep2p "github.com/yeeco/gyee/p2p"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

type yeShellManager struct {
	chainInst			*sch.Scheduler
	ptnChainShell		interface{}
	ptChainShMgr		*shellManager
	dhtInst				*sch.Scheduler
	ptnDhtShell			interface{}
	ptDhtShMgr			*dhtShellManager
}

func NewYeshellManager(cfg *config.Config) *yeShellManager {
	yeShMgr := yeShellManager{}
	var eno sch.SchErrno
	var ok bool

	yeShMgr.chainInst, eno = P2pCreateInstance(cfg)
	if eno != sch.SchEnoNone || yeShMgr.chainInst == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	eno, yeShMgr.ptnChainShell = yeShMgr.chainInst.SchGetTaskNodeByName(sch.ShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnChainShell == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptChainShMgr, ok = yeShMgr.chainInst.SchGetUserTaskIF(sch.ShMgrName).(*shellManager)
	if !ok || yeShMgr.ptChainShMgr == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.dhtInst, eno = P2pCreateInstance(cfg)
	if eno != sch.SchEnoNone || yeShMgr.dhtInst == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	eno, yeShMgr.ptnDhtShell = yeShMgr.dhtInst.SchGetTaskNodeByName(sch.DhtShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnDhtShell == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptDhtShMgr, ok = yeShMgr.dhtInst.SchGetUserTaskIF(sch.DhtShMgrName).(*dhtShellManager)
	if !ok || yeShMgr.ptDhtShMgr == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	return &yeShMgr
}

func (yeShMgr *yeShellManager)Start() error {
	if eno := P2pStart(yeShMgr.dhtInst); eno != sch.SchEnoNone {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	if eno := P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)Stop() {
	stopCh := make(chan bool, 0)
	P2pStop(yeShMgr.dhtInst, stopCh)
	P2pStop(yeShMgr.chainInst, stopCh)
}

func (yeShMgr *yeShellManager)BroadcastMessage(message yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)BroadcastMessageOsn(message yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)Register(subscriber *yep2p.Subscriber) {
	return
}

func (yeShMgr *yeShellManager)UnRegister(subscriber *yep2p.Subscriber) {
	return
}

func (yeShMgr *yeShellManager)DhtGetValue(key []byte) ([]byte, error) {
	return nil, nil
}

func (yeShMgr *yeShellManager)DhtSetValue(key []byte, value []byte) error {
	return nil
}

func (yeShMgr *yeShellManager)DhtFindNode() error {
	return nil
}

func (yeShMgr *yeShellManager)DhtBlindConnect() error {
	return nil
}

func (yeShMgr *yeShellManager)DhtGetProvider(key []byte) error {
	return nil
}

func (yeShMgr *yeShellManager)DhtSetProvider(key []byte) error {
	return nil
}
