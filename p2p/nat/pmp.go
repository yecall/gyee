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
	"github.com/jackpal/go-nat-pmp"
)


type pmpCtrlBlock struct {
	gateWay	net.IP				// gateway ip address
	client	*natpmp.Client		// client to interactive with the gateway
}



func (pmp *pmpCtrlBlock)makeMap(proto string, locPort int, pubPort int, durKeep time.Duration) NatEno {
	return NatEnoNone
}

func (pmp *pmpCtrlBlock)removeMap(proto string, locPort int, pubPort int) NatEno {
	return NatEnoNone
}

func (pmp *pmpCtrlBlock)getPublicIpAddr() (net.IP, NatEno) {
	return nil, NatEnoNone
}
