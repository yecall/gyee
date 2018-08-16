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

package dht

import (
	"net"
	config "github.com/yeeco/gyee/p2p/config"
	pb "github.com/yeeco/gyee/p2p/dht/pb"
)

//
// Protocol
//
const DhtProtoBytes	= 4
type DhtProtocol struct {
	Pid				uint32					// protocol identity
	Ver				[DhtProtoBytes]byte		// protocol version: M.m0.m1.m2
}

var DhtVersion = [DhtProtoBytes]byte {1, 0, 0, 0}

//
// Protocol identity
//
const (
	PID_DHT = pb.ProtocolId_PID_DHT			// dht internal
	PID_EXT = pb.ProtocolId_PID_EXT			// external, for dht users
)

//
// Message type identity
//
const (
	MID_HANDSHAKE           = 0
	MID_FINDNODE            = 1
	MID_NEIGHBORS           = 2
	MID_PUTVALUE            = 3
	MID_GETVALUE_REQ        = 4
	MID_GETVALUE_RSP        = 5
	MID_PUTPROVIDER         = 6
	MID_GETPROVIDER_REQ     = 7
	MID_GETPROVIDER_RSP     = 8
	MID_PING                = 9
	MID_PONG                = 10
)

//
// Value
//
type DhtKey []byte
type DhtVal	[]byte
type DhtValue struct {
	Key				DhtKey					// key of value
	Val				DhtVal					// value
}

//
// Provider
//
type DhtProvider struct {
	Key				DhtKey					// key for provider
	Node			config.Node				// node of provider
}

//
// Package for DHT protocol
//
type DhtPackage struct {
	Pid				uint32					// protocol identity
	PayloadLength	uint32					// payload length
	Payload			[]byte					// payload
}

//
// Message for DHT protocol
//
type DhtMessage struct {
	Mid				uint32					// message identity
	Handshake		*Handshake				// handshake message
	FindNode		*FindNode				// find node message
	Neighbors		*Neighbors				// neighbors message
	PutValue		*PutValue				// put value message
	GetValueReq		*GetValueReq			// get value request message
	GetValueRsp		*GetValueRsp			// get value response message
	PutProvider		*PutProvider			// put provider message
	GetProviderReq	*GetProviderReq			// get provider request message
	GetProviderRsp	*GetProviderRsp			// get provider response message
	Ping			*Ping					// ping message
	Pong			*Pong					// pong message
}

type Handshake struct {
	Dir				int						// direct
	NodeId			config.NodeID			// node identity
	IP				net.IP					// ip address
	UDP				uint32					// udp port number
	TCP				uint32					// tcp port number
	ProtoNum		uint32					// number of protocols supported
	Protocols		[]DhtProtocol			// version of protocol
}

type FindNode struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Target			config.NodeID			// target node identity
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type Neighbors struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Nodes			[]*config.Node			// neighbor nodes
	Pcs				[]int					// peer connection status
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type PutValue struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Values			[]DhtValue				// values
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetValueReq struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Keys			[]DhtKey				// keys requested
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetValueRsp struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Values			[]DhtValue				// values
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type PutProvider struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Providers		[]DhtProvider			// providers
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderReq struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Key				DhtKey					// key wanted
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderRsp struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Providers		[]DhtProvider			// providers
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type Ping struct {
	From			config.Node				// from whom
	To				config.Node				// to whom
	Seq				uint64					// sequence
	Extra			[]byte					// extra info
}

type Pong struct {
	From			config.Node				// from whom
	To				config.Node				// to whom
	Seq				uint64					// sequence
	Extra			[]byte					// extra info
}


//
// Send package
//
func (dhtPkg *DhtPackage)SendPackage(ci *ConInst) DhtErrno {
	return DhtEnoNone
}

//
// Reveived package
//
func (dhtpkg *DhtPackage)RecvPackage(ci *ConInst) DhtErrno {
	return DhtEnoNone
}

//
// Enocode package to bytes
//
func (dhtPkg *DhtPackage)Bytes() []byte {
	return []byte{}
}

//
// Extract message from package
//
func (dhtPkg *DhtPackage)GetMessage(dhtMsg *DhtMessage) DhtErrno {
	return DhtEnoNone
}

//
// Setup dht package from protobuf package
//
func (dhtPkg *DhtPackage)FromPbPackage(pbPkg *pb.DhtPackage) DhtErrno {
	return DhtEnoNone
}

//
// Setup protobuf package from dht package
//
func (dhtPkg *DhtPackage)ToPbPackage(pbPkg *pb.DhtPackage) DhtErrno {
	return DhtEnoNone
}

//
// Setup package from message
//
func (dhtMsg *DhtMessage)GetPackage(dhtPkg *DhtPackage) DhtErrno {
	return DhtEnoNone
}

//
// Setup protobuf package from message
//
func (dhtMsg *DhtMessage)GetPbPackage() *pb.DhtPackage {

	dhtPkg := DhtPackage{}
	dhtMsg.GetPackage(&dhtPkg)

	pbPkg := new(pb.DhtPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = pb.ProtocolId(dhtPkg.Pid)
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = dhtPkg.PayloadLength
	pbPkg.Payload = dhtPkg.Payload

	return pbPkg
}

