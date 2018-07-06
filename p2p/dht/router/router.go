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


package route

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
)

//
// Route manager
//
const DhtroMgrName = "DhtroMgr"

type DhtRouteManager struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

//
// Create dht rout manager
//
func NewDhtrMgr() *DhtRouteManager {
	var dhtrMgr = DhtRouteManager{
		name: DhtroMgrName,
	}

	dhtrMgr.tep = dhtrMgr.dhtroMgrProc
	return &dhtrMgr
}

//
// Entry point exported to shceduler
//
func (dhtrMgr *DhtRouteManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return dhtrMgr.tep(ptn, msg)
}

//
// Table manager entry
//
func (dhtrMgr *DhtRouteManager) dhtroMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

