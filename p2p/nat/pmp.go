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
	"net"
	"strings"
	"time"

	"github.com/jackpal/go-nat-pmp"
)

const (
	pmpClientTimout = time.Second * 8
)

type pmpCtrlBlock struct {
	gateWay net.IP         // gateway ip address
	client  *natpmp.Client // client to interactive with the gateway
}

func NewPmpInterface(gw net.IP) *pmpCtrlBlock {
	if gw == nil {
		natLog.Debug("NewPmpInterface: nil gateway ip address")
		return (*pmpCtrlBlock)(nil)
	}
	cb := pmpCtrlBlock{
		gateWay: gw,
		client:  natpmp.NewClient(gw),
		//client: natpmp.NewClientWithTimeout(gw, pmpClientTimout),
	}
	if cb.client == nil {
		return (*pmpCtrlBlock)(nil)
	}
	return &cb
}

func (pmp *pmpCtrlBlock) makeMap(name string, proto string, locPort int, pubPort int, durKeep time.Duration) NatEno {
	seconds := int(durKeep / time.Second)
	if _, err := pmp.client.AddPortMapping(strings.ToLower(proto), locPort, pubPort, seconds); err != nil {
		natLog.Debug("makeMap: AddPortMapping failed, error: %s", err.Error())
		return NatEnoFromPmpLib
	}
	return NatEnoNone
}

func (pmp *pmpCtrlBlock) removeMap(proto string, locPort int, pubPort int) NatEno {
	if _, err := pmp.client.AddPortMapping(strings.ToLower(proto), locPort, 0, 0); err != nil {
		natLog.Debug("removeMap: AddPortMapping(0,0) failed, error: %s", err.Error())
		return NatEnoFromPmpLib
	}
	return NatEnoNone
}

func (pmp *pmpCtrlBlock) getPublicIpAddr() (net.IP, NatEno) {
	rsp, err := pmp.client.GetExternalAddress()
	if err != nil {
		natLog.Debug("makeMap: GetExternalAddress failed, error: %s", err.Error())
		return nil, NatEnoFromPmpLib
	}
	return rsp.ExternalIPAddress[:], NatEnoNone
}

/*
 * kinds of private ip address are listed as bellow. when nat type "pmp" is configured
 * but no gateway ip is set, we had to guess the gatway ip as: b1.b2.b3.1 or b1.b2.1.1
 * see bellow please.
 *
 *	type	IP								CIDR
 * ==========================================================
 * 	A		10.0.0.0~10.255.255.255			10.0.0.0/8
 * 	B		172.16.0.0~172.31.255.255		172.16.0.0/12
 * 	C		192.168.0.0~192.168.255.255		192.168.0.0/16
 */

var _, privateCidrA, _ = net.ParseCIDR("10.0.0.0/8")
var _, privateCidrB, _ = net.ParseCIDR("172.16.0.0/12")
var _, privateCidrC, _ = net.ParseCIDR("192.168.0.0/16")

func guessPossibleGateways() (gws []net.IP, eno NatEno) {
	dedup := make(map[string]bool, 0)
	itfList, err := net.Interfaces()
	if err != nil {
		return nil, NatEnoFromSystem
	}
	for _, itf := range itfList {
		addrList, err := itf.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrList {
			switch t := addr.(type) {
			case *net.IPNet:
				if privateCidrA.Contains(t.IP) {
					ip := t.IP.Mask(t.Mask).To4()
					if ip != nil {
						ip[3] = ip[3] | 0x01
						if _, dup := dedup[ip.String()]; !dup {
							gws = append(gws, ip)
							dedup[ip.String()] = true
						}
					}
				} else if privateCidrB.Contains(t.IP) || privateCidrC.Contains(t.IP) {
					ip := t.IP.Mask(t.Mask).To4()
					if ip != nil {
						ip[2] = ip[2] | 0x01
						ip[3] = ip[3] | 0x01
						if _, dup := dedup[ip.String()]; !dup {
							gws = append(gws, ip)
							dedup[ip.String()] = true
						}
					}
				}
			}
		}
	}
	return gws, NatEnoNone
}
