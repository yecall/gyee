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
	dht "github.com/yeeco/gyee/p2p/dht"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	"github.com/yeeco/gyee/log"
)

//
// get dht manager hosted in a scheduler
//
func DhtGetManager(sdl *sch.Scheduler) *dht.DhtMgr {
	if sdl == nil {
		log.Errorf("DhtGetManager: nil scheduler pointer")
		return nil
	}
	dhtMgr, ok := sdl.SchGetTaskObject(dht.DhtMgrName).(*dht.DhtMgr)
	if !ok {
		log.Errorf("DhtGetManager: dht manager task not exist")
		return nil
	}

	return dhtMgr
}

//
// install callback for dht manager
//
func DhtInstallCallback(dhtMgr *dht.DhtMgr, cbf dht.DhtCallback) dht.DhtErrno {
	if dhtMgr == nil {
		log.Errorf("DhtInstallCallback: nil dht manager")
		return dht.DhtEnoParameter
	}
	return dhtMgr.InstallEventCallback(cbf)
}

//
// uninstall callback for dht manager
//
func DhtRemoveCallback(dhtMgr *dht.DhtMgr) dht.DhtErrno {
	if dhtMgr == nil {
		log.Errorf("DhtRemoveCallback: nil dht manager")
		return dht.DhtEnoParameter
	}
	return DhtInstallCallback(dhtMgr, nil)
}

//
// execute dht command
//
func DhtCommand(dhtMgr *dht.DhtMgr, cmd int, msg interface{}) sch.SchErrno {
	if dhtMgr == nil {
		log.Errorf("DhtCommand: nil dht manager")
		return sch.SchEnoParameter
	}
	return dhtMgr.DhtCommand(cmd, msg)
}
