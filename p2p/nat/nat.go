// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package nat

import (
	"time"
	"net"
	"fmt"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


//
// errno
//
type NatEno int
const (
	NatEnoNone = NatEno(iota)
	NatEnoUnknown
)

func (ne NatEno)Error() string {
	return fmt.Sprintf("NatEno: %d", ne)
}

func (ne NatEno)Errno() int {
	return int(ne)
}

//
// configuration
//
type natConfig struct {
	natType		string			// "pmp", "upnp", "none"
	gwIp		net.IP			// gateway ip address when "pmp" specified
	durKeep		time.Duration	// duration for map to be kept
}

//
// interface for nat
//
type natInterface interface {

	// make map between local address to public address
	makeMap(proto string, locPort int, pubPort int, durKeep time.Duration) NatEno

	// remove map make by makeMap
	removeMap(proto string, locPort int, pubPort int) NatEno

	// get public address
	getPublicIpAddr() (net.IP, NatEno)
}

//
// nat manager
//
const NatMgrName = sch.NatMgrName

type NatManager struct {
	sdl			*sch.Scheduler				// pointer to scheduler
	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	cfg			natConfig					// configuration
	nat			interface{}					// nil or pointer to pmpCtrlBlock or upnpCtrlBlock
}


func NewNatMgr() *NatManager {
	var lsnMgr = NatManager {
		name: NatMgrName,
	}
	lsnMgr.tep = lsnMgr.natMgrProc
	return &lsnMgr
}

func (natMgr *NatManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return natMgr.tep(ptn, msg)
}

func (lsnMgr *NatManager)natMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}