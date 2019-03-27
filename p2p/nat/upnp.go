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

	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/internetgateway1"
	"github.com/huin/goupnp/dcps/internetgateway2"
)

// see following packages under github.com/huin/goupnp for details:
// internetgateway1.WANIPConnection1, internetgateway1.WANPPPConnection1,
// internetgateway2.WANIPConnection1, internetgateway2.WANIPConnection2, internetgateway2.WANPPPConnection1
const (
	devType1     = internetgateway1.URN_WANConnectionDevice_1
	devType1Str  = "internetgateway1.URN_WANConnectionDevice_1"
	devType2     = internetgateway2.URN_WANConnectionDevice_2
	devType2Str  = "internetgateway2.URN_WANConnectionDevice_2"
	srvType11    = internetgateway1.URN_WANIPConnection_1
	srvType11Str = "internetgateway1.URN_WANIPConnection_1"
	srvType12    = internetgateway1.URN_WANPPPConnection_1
	srvType12Str = "internetgateway1.URN_WANPPPConnection_1"
	srvType21    = internetgateway2.URN_WANIPConnection_1
	srvType21Str = "internetgateway2.URN_WANIPConnection_1"
	srvType22    = internetgateway2.URN_WANIPConnection_2
	srvType22Str = "internetgateway2.URN_WANIPConnection_2"
	srvType23    = internetgateway2.URN_WANPPPConnection_1
	srvType23Str = "internetgateway2.URN_WANPPPConnection_1"
)

const upnpClientTimout = 3 * time.Second

type upnpClient interface {
	GetExternalIPAddress() (string, error)
	AddPortMapping(string, uint16, string, uint16, string, bool, string, uint32) error
	DeletePortMapping(string, uint16, string) error
	GetNATRSIPStatus() (sip bool, nat bool, err error)
}

type upnpCtrlBlock struct {
	devType string             // device type
	srvType string             // service type
	device  *goupnp.RootDevice // root device
	client  upnpClient         // client to access services
}

func NewUpnpInterface() *upnpCtrlBlock {
	cb := queryUpnp()
	if cb == nil {
		natLog.Debug("NewUpnpInterface: queryUpnp failed")
		return (*upnpCtrlBlock)(nil)
	}
	return cb
}

func (upnp *upnpCtrlBlock) makeMap(name string, proto string, locPort int, pubPort int, durKeep time.Duration) NatEno {
	ip, eno := upnp.getLocalAddress()
	if eno != NatEnoNone {
		natLog.Debug("makeMap: getLocalAddress failed, error: %s", eno.Error())
		return eno
	}
	proto = strings.ToUpper(proto)
	seconds := uint32(durKeep / time.Second)
	// we had filtered out the duplicated case in function makeMapReq,
	// but would someone else(other application)... try removing...
	upnp.removeMap(proto, locPort, pubPort)
	err := upnp.client.AddPortMapping("", uint16(pubPort), proto, uint16(locPort), ip.String(), true, name, seconds)
	if err != nil {
		natLog.Debug("makeMap: AddPortMapping failed, error: %s", err.Error())
		return NatEnoFromUpnpLib
	}
	return NatEnoNone
}

func (upnp *upnpCtrlBlock) removeMap(proto string, locPort int, pubPort int) NatEno {
	proto = strings.ToUpper(proto)
	if err := upnp.client.DeletePortMapping("", uint16(pubPort), proto); err != nil {
		natLog.Debug("removeMap: DeletePortMapping failed, error: %s", err.Error())
		return NatEnoFromUpnpLib
	}
	return NatEnoNone
}

func (upnp *upnpCtrlBlock) getPublicIpAddr() (net.IP, NatEno) {
	ipStr, err := upnp.client.GetExternalIPAddress()
	if err != nil {
		natLog.Debug("getPublicIpAddr: GetExternalIPAddress failed, error: %s", err.Error())
		return nil, NatEnoFromUpnpLib
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		natLog.Debug("getPublicIpAddr: ParseIP failed, ipStr: %s", ipStr)
		return nil, NatEnoFromUpnpLib
	}
	return ip, NatEnoNone
}

func (upnp *upnpCtrlBlock) getLocalAddress() (net.IP, NatEno) {
	devaddr, err := net.ResolveUDPAddr("udp4", upnp.device.URLBase.Host)
	if err != nil {
		natLog.Debug("getLocalAddress: bad device address: %s", upnp.device.URLBase.Host)
		return nil, NatEnoFromUpnpLib
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		natLog.Debug("getLocalAddress: Interfaces failed, error: %s", err.Error())
		return nil, NatEnoFromSystem
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			natLog.Debug("getLocalAddress: Addrs failed, error: %s", err.Error())
			return nil, NatEnoFromSystem
		}
		for _, addr := range addrs {
			switch x := addr.(type) {
			case *net.IPNet:
				if x.Contains(devaddr.IP) {
					return x.IP, NatEnoNone
				}
			}
		}
	}
	return nil, NatEnoNotFound
}

func devType1Matcher(dev *goupnp.RootDevice, sc goupnp.ServiceClient) *upnpCtrlBlock {
	switch sc.Service.ServiceType {
	case srvType11:
		return &upnpCtrlBlock{
			devType: devType1Str,
			srvType: srvType11Str,
			device:  dev,
			client:  &internetgateway1.WANIPConnection1{ServiceClient: sc},
		}
	case srvType12:
		return &upnpCtrlBlock{
			devType: devType1Str,
			srvType: srvType12Str,
			device:  dev,
			client:  &internetgateway1.WANPPPConnection1{ServiceClient: sc},
		}
	}
	return nil
}

type matcherT func(*goupnp.RootDevice, goupnp.ServiceClient) *upnpCtrlBlock

func devType2Mathcer(dev *goupnp.RootDevice, sc goupnp.ServiceClient) *upnpCtrlBlock {
	switch sc.Service.ServiceType {
	case srvType21:
		return &upnpCtrlBlock{
			devType: devType2Str,
			srvType: srvType21Str,
			device:  dev,
			client:  &internetgateway2.WANIPConnection1{ServiceClient: sc},
		}
	case srvType22:
		return &upnpCtrlBlock{
			devType: devType2Str,
			srvType: srvType22Str,
			device:  dev,
			client:  &internetgateway2.WANIPConnection2{ServiceClient: sc},
		}
	case srvType23:
		return &upnpCtrlBlock{
			devType: devType2Str,
			srvType: srvType23Str,
			device:  dev,
			client:  &internetgateway2.WANPPPConnection1{ServiceClient: sc},
		}
	}
	return nil
}

func queryUpnp() *upnpCtrlBlock {
	found := make(chan *upnpCtrlBlock, 2)
	go query(found, devType1, devType1Matcher)
	go query(found, devType2, devType2Mathcer)
	for i := 0; i < cap(found); i++ {
		if c := <-found; c != nil {
			return c
		}
	}
	return nil
}

func query(found chan<- *upnpCtrlBlock, target string, matcher matcherT) {
	devs, err := goupnp.DiscoverDevices(target)
	if err != nil {
		found <- nil
		return
	}
	getone := false
	for i := 0; i < len(devs) && !getone; i++ {
		if devs[i].Root == nil {
			continue
		}
		devs[i].Root.Device.VisitServices(func(service *goupnp.Service) {
			if getone {
				return
			}
			sc := goupnp.ServiceClient{
				SOAPClient: service.NewSOAPClient(),
				RootDevice: devs[i].Root,
				Location:   devs[i].Location,
				Service:    service,
			}
			sc.SOAPClient.HTTPClient.Timeout = upnpClientTimout
			upnp := matcher(devs[i].Root, sc)
			if upnp == nil {
				return
			}
			if _, nat, err := upnp.client.GetNATRSIPStatus(); err != nil || !nat {
				return
			}
			found <- upnp
			getone = true
		})
	}
	if !getone {
		found <- nil
	}
}
