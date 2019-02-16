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
	p2plog	"github.com/yeeco/gyee/p2p/logger"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
)


//
// debug
//
type natMgrLogger struct {
	debug__		bool
}

var natLog = natMgrLogger  {
	debug__:	false,
}

func (log natMgrLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

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
	natType		string		// "pmp", "upnp", "none"
	gwIp		net.IP		// gateway ip address when "pmp" specified
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
// map instance
//
type NatMapInstID	struct {
	proto		string			// the prototcol, "tcp" or "udp"
	fromPort	int				// local port number be mapped
}

type NatMapInstance struct {
	id			NatMapInstID	// map item identity
	toPort		int				// target port number requested
	durKeep		time.Duration	// duration for map to be kept
	durRefresh	time.Duration	// interval duration to refresh the map
	result		NatEno			// map result
	pubIp		net.IP			// public address
	pubPort		int				// public port
}

//
// nat manager
//
const NatMgrName = sch.NatMgrName

type NatManager struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// name
	tep			sch.SchUserTaskEp				// entry
	cfg			natConfig						// configuration
	nat			interface{}						// nil or pointer to pmpCtrlBlock or upnpCtrlBlock
	instTab		map[NatMapInstID]NatMapInstance // instance table
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

func (natMgr *NatManager)natMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	var eno sch.SchErrno
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = natMgr.poweron(msg)
	case sch.EvSchPoweroff:
		eno = natMgr.poweroff(msg)
	case sch.EvNatMgrDiscoverReq:
		eno = natMgr.discoverReq(msg)
	case sch.EvNatRefreshTimer:
		eno = natMgr.refreshTimerHandler(msg)
	case sch.EvNatMgrMakeMapReq:
		eno = natMgr.makeMapReq(msg)
	case sch.EvNatMgrRemoveMapReq:
		eno = natMgr.removeMapReq(msg)
	case sch.EvNatMgrGetPublicAddrReq:
		eno = natMgr.getPubAddrReq(msg)
	default:
		natLog.Debug("natMgrProc: unknown message: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (natMgr *NatManager)poweron(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

func (natMgr *NatManager)poweroff(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

func (natMgr *NatManager)discoverReq(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}
func (natMgr *NatManager)refreshTimerHandler(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

func (natMgr *NatManager)makeMapReq(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

func (natMgr *NatManager)removeMapReq(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

func (natMgr *NatManager)getPubAddrReq(msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}
