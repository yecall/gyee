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
	"strings"
	"github.com/jackpal/go-nat-pmp"
)


type pmpCtrlBlock struct {
	gateWay	net.IP				// gateway ip address
	client	*natpmp.Client		// client to interactive with the gateway
}

func NewPmpInterface(gw net.IP) *pmpCtrlBlock {
	if gw == nil {
		natLog.Debug("NewPmpInterface: nil gateway ip address")
		return nil
	}
	cb := pmpCtrlBlock{
		gateWay: gw,
		client: natpmp.NewClient(gw),
	}
	return &cb
}

func (pmp *pmpCtrlBlock)makeMap(name string, proto string, locPort int, pubPort int, durKeep time.Duration) NatEno {
	seconds := int(durKeep/time.Second)
	if _, err := pmp.client.AddPortMapping(strings.ToLower(proto), locPort, pubPort, seconds);
		err != nil {
		natLog.Debug("makeMap: AddPortMapping failed, error: %s", err.Error())
		return NatEnoFromPmpLib
	}
	return NatEnoNone
}

func (pmp *pmpCtrlBlock)removeMap(proto string, locPort int, pubPort int) NatEno {
	if _, err := pmp.client.AddPortMapping(strings.ToLower(proto), locPort, 0, 0);
		err != nil {
		natLog.Debug("removeMap: AddPortMapping(0,0) failed, error: %s", err.Error())
		return NatEnoFromPmpLib
	}
	return NatEnoNone
}

func (pmp *pmpCtrlBlock)getPublicIpAddr() (net.IP, NatEno) {
	rsp, err := pmp.client.GetExternalAddress()
	if err != nil {
		natLog.Debug("makeMap: GetExternalAddress failed, error: %s", err.Error())
		return nil, NatEnoFromPmpLib
	}
	return rsp.ExternalIPAddress[:], NatEnoNone
}
