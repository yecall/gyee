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
	"time"
	/*
	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/internetgateway1"
	"github.com/huin/goupnp/dcps/internetgateway2"
	*/
)



type upnpCtrlBlock struct {
	devType		string			// device type, URN_WANConnectionDevice_1 or URN_WANConnectionDevice_2
	srvType		string			// URN_WANIPConnection_1, URN_WANPPPConnection_1, URN_WANIPConnection_2,
	gwConn		interface{}		// *internetgateway2.WANIPConnection1 or *internetgateway2.WANIPConnection2
}



func (upnp *upnpCtrlBlock)makeMap(proto string, locPort int, pubPort int, durKeep time.Duration) NatEno {
	return NatEnoNone
}

func (upnp *upnpCtrlBlock)removeMap(proto string, locPort int, pubPort int) NatEno {
	return NatEnoNone
}

func (upnp *upnpCtrlBlock)getPublicIpAddr() (net.IP, NatEno) {
	return nil, NatEnoNone
}





