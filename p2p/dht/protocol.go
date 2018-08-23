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
	"time"
	pb "github.com/yeeco/gyee/p2p/dht/pb"
	config "github.com/yeeco/gyee/p2p/config"
	log "github.com/yeeco/gyee/p2p/logger"
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
	MID_UNKNOWN				= -1
)

//
// Value
//
type DhtKey []byte
type DhtVal	[]byte

type DhtValue struct {
	Key				DhtKey					// key of value
	Val				DhtVal					// value
	Extra			interface{}				// extra inforamtion
}

//
// Provider
//
type DhtProvider struct {
	Key				DhtKey					// key for what provided
	Node			config.Node				// node of provider
	Extra			interface{}				// extra inforamtion
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
	Pcs				[]int					// prividers connection status
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderReq struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Keys			[]DhtKey					// key wanted
	Id				uint64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderRsp struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Providers		[]DhtProvider			// providers
	Pcs				[]int					// prividers connection status
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
// Extract message from package
//
func (dhtPkg *DhtPackage)GetMessage(dhtMsg *DhtMessage) DhtErrno {

	if dhtMsg == nil {
		return DhtEnoParameter
	}

	if dhtPkg.Pid != uint32(PID_DHT) {
		return DhtEnoMismatched
	}

	if dhtPkg.PayloadLength == 0 || int(dhtPkg.PayloadLength) != len(dhtPkg.Payload) {
		return DhtEnoSerialization
	}

	pbMsg := new(pb.DhtMessage)
	if err := pbMsg.Unmarshal(dhtPkg.Payload); err != nil {
		log.LogCallerFileLine("GetMessage: Unmarshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	eno := DhtErrno(DhtEnoUnknown)
	mid := *pbMsg.MsgType

	switch mid {

	case pb.DhtMessage_MID_HANDSHAKE:
		eno = dhtMsg.GetHandshakeMessage(pbMsg.Handshake)

	case pb.DhtMessage_MID_FINDNODE:
		eno = dhtMsg.GetFindNodeMessage(pbMsg.FindNode)

	case pb.DhtMessage_MID_NEIGHBORS:
		eno = dhtMsg.GetNeighborsMessage(pbMsg.Neighbors)

	case pb.DhtMessage_MID_PUTVALUE:
		eno = dhtMsg.GetPutValueMessage(pbMsg.PutValue)

	case pb.DhtMessage_MID_GETVALUE_REQ:
		eno = dhtMsg.GetGetValueReqMessage(pbMsg.GetValueReq)

	case pb.DhtMessage_MID_GETVALUE_RSP:
		eno = dhtMsg.GetGetValueRspMessage(pbMsg.GetValueRsp)

	case pb.DhtMessage_MID_PUTPROVIDER:
		eno = dhtMsg.GetPutProviderMessage(pbMsg.PutProvider)

	case pb.DhtMessage_MID_GETPROVIDER_REQ:
		eno = dhtMsg.GetGetProviderReqMessage(pbMsg.GetProviderReq)

	case pb.DhtMessage_MID_GETPROVIDER_RSP:
		eno = dhtMsg.GetGetProviderRspMessage(pbMsg.GetProviderRsp)

	case pb.DhtMessage_MID_PING:
		eno = dhtMsg.GetPingMessage(pbMsg.Ping)

	case pb.DhtMessage_MID_PONG:
		eno = dhtMsg.GetPongMessage(pbMsg.Pong)

	default:
		log.LogCallerFileLine("GetMessage: invalid mid: %d", mid)
		return DhtEnoSerialization
	}

	return eno
}

//
// Setup dht package from protobuf package
//
func (dhtPkg *DhtPackage)FromPbPackage(pbPkg *pb.DhtPackage) DhtErrno {
	if pbPkg == nil {
		return DhtEnoParameter
	}
	dhtPkg.Pid = uint32(*pbPkg.Pid)
	dhtPkg.PayloadLength = *pbPkg.PayloadLength
	dhtPkg.Payload = pbPkg.Payload
	return DhtEnoNone
}

//
// Setup protobuf package from dht package
//
func (dhtPkg *DhtPackage)ToPbPackage(pbPkg *pb.DhtPackage) DhtErrno {
	if pbPkg == nil {
		return DhtEnoParameter
	}
	if pbPkg.Pid == nil {
		pbPkg.Pid = new(pb.ProtocolId)
	}
	if pbPkg.Payload == nil {
		pbPkg.PayloadLength = new(uint32)
	}
	*pbPkg.Pid = pb.ProtocolId(dhtPkg.Pid)
	*pbPkg.PayloadLength = dhtPkg.PayloadLength
	pbPkg.Payload = dhtPkg.Payload
	return DhtEnoNone
}

//
// Setup package from message
//
func (dhtMsg *DhtMessage)GetPackage(dhtPkg *DhtPackage) DhtErrno {
	if dhtPkg == nil {
		return DhtEnoParameter
	}

	eno := DhtErrno(DhtEnoUnknown)
	mid := dhtMsg.Mid

	switch mid {

	case MID_HANDSHAKE:
		eno = dhtMsg.GetHandshakePackage(dhtPkg)

	case MID_FINDNODE:
		eno = dhtMsg.GetFindNodePackage(dhtPkg)

	case MID_NEIGHBORS:
		eno = dhtMsg.GetNeighborsPackage(dhtPkg)

	case MID_PUTVALUE:
		eno = dhtMsg.GetPutValuePackage(dhtPkg)

	case MID_GETVALUE_REQ:
		eno = dhtMsg.GetGetValueReqPackage(dhtPkg)

	case MID_GETVALUE_RSP:
		eno = dhtMsg.GetGetValueRspPackage(dhtPkg)

	case MID_PUTPROVIDER:
		eno = dhtMsg.GetPutProviderPackage(dhtPkg)

	case MID_GETPROVIDER_REQ:
		eno = dhtMsg.GetGetProviderReqPackage(dhtPkg)

	case MID_GETPROVIDER_RSP:
		eno = dhtMsg.GetGetProviderRspPackage(dhtPkg)

	case MID_PING:
		eno = dhtMsg.GetPingPackage(dhtPkg)

	case MID_PONG:
		eno = dhtMsg.GetPongPackage(dhtPkg)

	default:
		log.LogCallerFileLine("GetPackage: invalid mid: %d", mid)
		return DhtEnoSerialization
	}

	return eno
}

//
// Reset dht message
//
func (dhtMsg *DhtMessage)reset() {
	*dhtMsg = DhtMessage{}
	dhtMsg.Mid = MID_UNKNOWN
}

//
// Get dht node specification from protobuf node
//
func (dhtMsg *DhtMessage)getNode(n *pb.DhtMessage_Node) *config.Node {
	if n == nil {
		return nil
	}
	dn := config.Node{
		IP:		n.IP,
		TCP:	uint16(*n.TCP & 0xffff),
		UDP:	uint16(*n.UDP & 0xffff),
	}
	copy(dn.ID[0:], n.NodeId)
	return &dn
}

//
// Set protobuf node from dht node specification
//
func (dhtMsg *DhtMessage)setNode(n *config.Node, ct pb.DhtMessage_ConnectionType) *pb.DhtMessage_Node {
	if n == nil {
		return nil
	}
	pbn := new(pb.DhtMessage_Node)
	pbn.IP = n.IP
	pbn.TCP = new(uint32)
	*pbn.TCP = uint32(n.TCP)
	pbn.UDP = new(uint32)
	*pbn.UDP = uint32(n.UDP)
	pbn.NodeId = n.ID[0:]
	pbn.ConnType = new(pb.DhtMessage_ConnectionType)
	*pbn.ConnType = ct
	return pbn
}

//
// Setup protobuf package from message
//
func (dhtMsg *DhtMessage)GetPbPackage() *pb.DhtPackage {
	dhtPkg := DhtPackage{}
	dhtMsg.GetPackage(&dhtPkg)

	pbPkg := new(pb.DhtPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	pbPkg.PayloadLength = new(uint32)

	*pbPkg.Pid = pb.ProtocolId(dhtPkg.Pid)
	*pbPkg.PayloadLength = dhtPkg.PayloadLength
	pbPkg.Payload = dhtPkg.Payload

	return pbPkg
}

//
// Setup dht handshake message from protobuf message
//
func (dhtMsg *DhtMessage)GetHandshakeMessage(pbMsg *pb.DhtMessage_Handshake) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	hs := new(Handshake)

	hs.Dir = int(*pbMsg.Dir)

	if len(pbMsg.NodeId) != cap(hs.NodeId) {
		return DhtEnoSerialization
	}
	copy(hs.NodeId[0:], pbMsg.NodeId)

	hs.IP = pbMsg.IP
	hs.TCP = *pbMsg.TCP
	hs.UDP = *pbMsg.UDP

	hs.ProtoNum = *pbMsg.ProtoNum
	if hs.ProtoNum != uint32(len(pbMsg.Protocols)) {
		return DhtEnoSerialization
	}

	dhtSup := false

	hs.Protocols = make([]DhtProtocol, hs.ProtoNum)
	for idx, p := range pbMsg.Protocols {
		if hs.Protocols[idx].Pid = uint32(*p.Pid); *p.Pid == PID_DHT {
			dhtSup = true
		}
		if len(p.Ver) != DhtProtoBytes {
			return DhtEnoSerialization
		}
		copy(hs.Protocols[idx].Ver[0:], p.Ver)
	}

	if !dhtSup {
		log.LogCallerFileLine("GetHandshakeMessage: DHT not supported")
		return DhtEnoNotSup
	}

	dhtMsg.reset()
	dhtMsg.Mid = MID_HANDSHAKE
	dhtMsg.Handshake = hs

	return DhtEnoNone
}

//
// Setup dht find-node message from protobuf message
//
func (dhtMsg *DhtMessage)GetFindNodeMessage(pbMsg *pb.DhtMessage_FindNode) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	fn := new(FindNode)

	fn.From = *dhtMsg.getNode(pbMsg.From)
	fn.To = *dhtMsg.getNode(pbMsg.To)
	copy(fn.Target[0:], pbMsg.Target)
	fn.Id = *pbMsg.Id
	fn.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_FINDNODE
	dhtMsg.FindNode = fn

	return DhtEnoNone
}

//
// Setup dht neighbors message from protobuf message
//
func (dhtMsg *DhtMessage)GetNeighborsMessage(pbMsg *pb.DhtMessage_Neighbors) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	nbs := new(Neighbors)

	nbs.From = *dhtMsg.getNode(pbMsg.From)
	nbs.To = *dhtMsg.getNode(pbMsg.To)
	for _, n := range pbMsg.Nodes {
		nbs.Nodes = append(nbs.Nodes, dhtMsg.getNode(n))
		nbs.Pcs = append(nbs.Pcs, int(*n.ConnType))
	}
	nbs.Id = *pbMsg.Id
	nbs.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_NEIGHBORS
	dhtMsg.Neighbors = nbs
	
	return DhtEnoNone
}

//
// Setup dht put-value message from protobuf message
//
func (dhtMsg *DhtMessage)GetPutValueMessage(pbMsg *pb.DhtMessage_PutValue) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	pv := new(PutValue)

	pv.From = *dhtMsg.getNode(pbMsg.From)
	pv.To = *dhtMsg.getNode(pbMsg.To)

	for _, v := range(pbMsg.Values) {
		val := DhtValue {
			Key: v.Key,
			Val: v.Val,
		}
		pv.Values = append(pv.Values, val)
	}

	pv.Id = *pbMsg.Id
	pv.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_PUTVALUE
	dhtMsg.PutValue = pv

	return DhtEnoNone
}

//
// Setup dht get-value-req message from protobuf message
//
func (dhtMsg *DhtMessage)GetGetValueReqMessage(pbMsg *pb.DhtMessage_GetValueReq) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	gvr := new(GetValueReq)

	gvr.From = *dhtMsg.getNode(pbMsg.From)
	gvr.To = *dhtMsg.getNode(pbMsg.To)

	for  _, k := range pbMsg.Keys {
		dhtK := DhtKey(k)
		gvr.Keys = append(gvr.Keys, dhtK)
	}

	gvr.Id = *pbMsg.Id
	gvr.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_GETVALUE_REQ
	dhtMsg.GetValueReq = gvr

	return DhtEnoNone
}

//
// Setup dht get-value-rsp message from protobuf message
//
func (dhtMsg *DhtMessage)GetGetValueRspMessage(pbMsg *pb.DhtMessage_GetValueRsp) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	gvr := new(GetValueRsp)

	gvr.From = *dhtMsg.getNode(pbMsg.From)
	gvr.To = *dhtMsg.getNode(pbMsg.To)

	for _, v := range pbMsg.Values {
		dhtValue := DhtValue{
			Key: DhtKey(v.Key),
			Val: DhtVal(v.Val),
		}
		gvr.Values = append(gvr.Values, dhtValue)

	}

	gvr.Id = *pbMsg.Id
	gvr.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_GETVALUE_RSP
	dhtMsg.GetValueRsp = gvr

	return DhtEnoNone
}

//
// Setup dht put-provider message from protobuf message
//
func (dhtMsg *DhtMessage)GetPutProviderMessage(pbMsg *pb.DhtMessage_PutProvider) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	pp := new(PutProvider)

	pp.From = *dhtMsg.getNode(pbMsg.From)
	pp.To = *dhtMsg.getNode(pbMsg.To)

	for _, p := range pbMsg.Providers {
		dhtP := DhtProvider{
			Key: 	DhtKey(p.Key),
			Node:	*dhtMsg.getNode(p.Node),
		}
		pp.Providers = append(pp.Providers, dhtP)
	}

	pp.Id = *pbMsg.Id
	pp.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_PUTPROVIDER
	dhtMsg.PutProvider = pp

	return DhtEnoNone
}

//
// Setup dht get-provider-req message from protobuf message
//
func (dhtMsg *DhtMessage)GetGetProviderReqMessage(pbMsg *pb.DhtMessage_GetProviderReq) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	gpr := new(GetProviderReq)

	gpr.From = *dhtMsg.getNode(pbMsg.From)
	gpr.To = *dhtMsg.getNode(pbMsg.To)

	for _, k := range pbMsg.Keys {
		dhtK := DhtKey(k)
		gpr.Keys = append(gpr.Keys, dhtK)
	}

	dhtMsg.reset()
	dhtMsg.Mid = MID_GETPROVIDER_REQ
	dhtMsg.GetProviderReq = gpr

	return DhtEnoNone
}

//
// Setup dht get-provider-rsp message from protobuf message
//
func (dhtMsg *DhtMessage)GetGetProviderRspMessage(pbMsg *pb.DhtMessage_GetProviderRsp) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	gpr := new(GetProviderRsp)

	gpr.From = *dhtMsg.getNode(pbMsg.From)
	gpr.To = *dhtMsg.getNode(pbMsg.To)

	for _, p := range pbMsg.Providers {
		dhtP := DhtProvider{
			Key:	DhtKey(p.Key),
			Node:	*dhtMsg.getNode(p.Node),
		}
		gpr.Providers = append(gpr.Providers, dhtP)
	}

	gpr.Id = *pbMsg.Id
	gpr.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_GETPROVIDER_RSP
	dhtMsg.GetProviderRsp = gpr

	return DhtEnoNone
}

//
// Setup dht ping message from protobuf message
//
func (dhtMsg *DhtMessage)GetPingMessage(pbMsg *pb.DhtMessage_Ping) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	ping := new(Ping)

	ping.From = *dhtMsg.getNode(pbMsg.From)
	ping.To = *dhtMsg.getNode(pbMsg.To)
	ping.Seq = *pbMsg.Seq
	ping.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_PING
	dhtMsg.Ping = ping

	return DhtEnoNone
}

//
// Setup dht pong message from protobuf message
//
func (dhtMsg *DhtMessage)GetPongMessage(pbMsg *pb.DhtMessage_Pong) DhtErrno {

	if pbMsg == nil {
		return DhtEnoParameter
	}

	pong := new(Pong)

	pong.From = *dhtMsg.getNode(pbMsg.From)
	pong.To = *dhtMsg.getNode(pbMsg.To)
	pong.Seq = *pbMsg.Seq
	pong.Extra = pbMsg.Extra

	dhtMsg.reset()
	dhtMsg.Mid = MID_PONG
	dhtMsg.Pong = pong

	return DhtEnoNone
}

//
// Setup dht handshake package from dht message
//
func (dhtMsg *DhtMessage)GetHandshakePackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbHs := new(pb.DhtMessage_Handshake)
	pbMsg := pb.DhtMessage{
		MsgType:	new(pb.DhtMessage_MessageType),
		Handshake:	pbHs,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_HANDSHAKE
	hs := dhtMsg.Handshake

	pbHs.Dir = new(int32)
	*pbHs.Dir = int32(hs.Dir)

	pbHs.NodeId = hs.NodeId[0:]
	pbHs.IP = hs.IP
	pbHs.UDP = new(uint32)
	*pbHs.UDP = hs.UDP
	pbHs.TCP = new(uint32)
	*pbHs.TCP = hs.TCP
	pbHs.ProtoNum = new(uint32)
	*pbHs.ProtoNum = hs.ProtoNum

	for _, p := range hs.Protocols {
		pbp := &pb.DhtMessage_Protocol {
			Pid:	new(pb.ProtocolId),
			Ver:	p.Ver[0:],
		}
		*pbp.Pid = pb.ProtocolId(p.Pid)
		pbHs.Protocols = append(pbHs.Protocols, pbp)
	}

	pbHs.Id = new(uint64)
	*pbHs.Id = uint64(time.Now().UnixNano())
	pbHs.Extra = nil

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetHandshakePackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht find-node package from dht message
//
func (dhtMsg *DhtMessage)GetFindNodePackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbFn := new(pb.DhtMessage_FindNode)
	pbMsg := pb.DhtMessage {
		MsgType:	new(pb.DhtMessage_MessageType),
		FindNode:	pbFn,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_FINDNODE
	fn := dhtMsg.FindNode

	pbFn.From = dhtMsg.setNode(&fn.From, pb.DhtMessage_CONT_YES)
	pbFn.To = dhtMsg.setNode(&fn.To, pb.DhtMessage_CONT_YES)
	pbFn.Target = fn.Target[0:]
	pbFn.Id = new(uint64)
	*pbFn.Id = uint64(time.Now().UnixNano())
	pbFn.Extra = fn.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetFindNodePackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht neighbors package from dht message
//
func (dhtMsg *DhtMessage)GetNeighborsPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbNbs := new(pb.DhtMessage_Neighbors)
	pbMsg := pb.DhtMessage {
		MsgType:	new(pb.DhtMessage_MessageType),
		Neighbors:	pbNbs,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_NEIGHBORS
	nbs := dhtMsg.Neighbors

	for idx, nb := range nbs.Nodes {
		pbn := dhtMsg.setNode(nb, pb.DhtMessage_ConnectionType(nbs.Pcs[idx]))
		pbNbs.Nodes = append(pbNbs.Nodes, pbn)
	}

	pbNbs.From = dhtMsg.setNode(&nbs.From, pb.DhtMessage_CONT_YES)
	pbNbs.To = dhtMsg.setNode(&nbs.To, pb.DhtMessage_CONT_YES)
	pbNbs.Id = new(uint64)
	*pbNbs.Id = uint64(time.Now().UnixNano())
	pbNbs.Extra = nbs.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetNeighborsPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht put-value package from dht message
//
func (dhtMsg *DhtMessage)GetPutValuePackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbPv := new(pb.DhtMessage_PutValue)
	pbMsg := pb.DhtMessage {
		MsgType:	new(pb.DhtMessage_MessageType),
		PutValue:	pbPv,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_PUTVALUE
	pv := dhtMsg.PutValue

	pbPv.From = dhtMsg.setNode(&pv.From, pb.DhtMessage_CONT_YES)
	pbPv.To = dhtMsg.setNode(&pv.To, pb.DhtMessage_CONT_YES)

	for _, v := range pv.Values {
		pbV := &pb.DhtMessage_Value {
			Key:	v.Key,
			Val:	v.Val,
		}
		pbPv.Values = append(pbPv.Values, pbV)
	}

	pbPv.Id = new(uint64)
	*pbPv.Id = pv.Id
	pbPv.Extra = pv.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetPutValuePackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht get-value-req package from dht message
//
func (dhtMsg *DhtMessage)GetGetValueReqPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbGvr := new(pb.DhtMessage_GetValueReq)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetValueReq:	pbGvr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETVALUE_REQ

	pbGvr.From = dhtMsg.setNode(&dhtMsg.GetProviderReq.From, pb.DhtMessage_CONT_YES)
	pbGvr.To = dhtMsg.setNode(&dhtMsg.GetValueReq.To, pb.DhtMessage_CONT_YES)

	for _, k := range dhtMsg.GetValueReq.Keys {
		pbGvr.Keys = append(pbGvr.Keys, k)
	}

	pbGvr.Id = new(uint64)
	*pbGvr.Id = dhtMsg.GetValueReq.Id
	pbGvr.Extra = dhtMsg.GetProviderReq.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetGetValueReqPackage: failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht get-value-rsp package from dht message
//
func (dhtMsg *DhtMessage)GetGetValueRspPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbGvr := new(pb.DhtMessage_GetValueRsp)
	pbMsg := pb.DhtMessage {
		MsgType:		new(pb.DhtMessage_MessageType),
		GetValueRsp:	pbGvr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETVALUE_RSP

	pbGvr.From = dhtMsg.setNode(&dhtMsg.GetValueRsp.From, pb.DhtMessage_CONT_YES)
	pbGvr.To = dhtMsg.setNode(&dhtMsg.GetValueRsp.To, pb.DhtMessage_CONT_YES)

	for _, v := range dhtMsg.GetValueRsp.Values {
		pbV := &pb.DhtMessage_Value {
			Key:	v.Key,
			Val:	v.Val,
		}
		pbGvr.Values = append(pbGvr.Values, pbV)
	}

	pbGvr.Id = new(uint64)
	*pbGvr.Id = dhtMsg.GetValueRsp.Id
	pbGvr.Extra = dhtMsg.GetValueRsp.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetGetValueRspPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht put-provider package from dht message
//
func (dhtMsg *DhtMessage)GetPutProviderPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbPP := new(pb.DhtMessage_PutProvider)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		PutProvider:	pbPP,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_PUTPROVIDER

	pbPP.From = dhtMsg.setNode(&dhtMsg.PutProvider.From, pb.DhtMessage_CONT_YES)
	pbPP.To = dhtMsg.setNode(&dhtMsg.PutProvider.To, pb.DhtMessage_CONT_YES)

	for idx, p := range dhtMsg.PutProvider.Providers {
		ct := dhtMsg.PutProvider.Pcs[idx]
		pbp := &pb.DhtMessage_Provider {
			Key:	p.Key,
			Node:	dhtMsg.setNode(&p.Node, pb.DhtMessage_ConnectionType(ct)),
		}
		pbPP.Providers = append(pbPP.Providers, pbp)
	}

	pbPP.Id = new(uint64)
	*pbPP.Id = dhtMsg.PutProvider.Id
	pbPP.Extra = dhtMsg.PutProvider.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetPutProviderPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht get-provider-req package from dht message
//
func (dhtMsg *DhtMessage)GetGetProviderReqPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbGpr := new(pb.DhtMessage_GetProviderReq)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetProviderReq:	pbGpr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETPROVIDER_REQ

	pbGpr.From = dhtMsg.setNode(&dhtMsg.GetProviderReq.From, pb.DhtMessage_CONT_YES)
	pbGpr.To = dhtMsg.setNode(&dhtMsg.GetProviderReq.To, pb.DhtMessage_CONT_YES)

	for _, k := range dhtMsg.GetProviderReq.Keys {
		pbGpr.Keys = append(pbGpr.Keys, k)
	}

	pbGpr.Id = new(uint64)
	*pbGpr.Id = dhtMsg.GetProviderReq.Id
	pbGpr.Extra = dhtMsg.GetProviderReq.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetGetProviderReqPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht get-provider-rsp package from dht message
//
func (dhtMsg *DhtMessage)GetGetProviderRspPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbGpr := new(pb.DhtMessage_GetProviderRsp)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetProviderRsp:	pbGpr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETPROVIDER_RSP

	pbGpr.From = dhtMsg.setNode(&dhtMsg.GetProviderRsp.From, pb.DhtMessage_CONT_YES)
	pbGpr.To = dhtMsg.setNode(&dhtMsg.GetProviderRsp.To, pb.DhtMessage_CONT_YES)

	for idx, p := range dhtMsg.GetProviderRsp.Providers {
		ct := dhtMsg.GetProviderRsp.Pcs[idx]
		pbp := &pb.DhtMessage_Provider{
			Key:	p.Key,
			Node:	dhtMsg.setNode(&p.Node, pb.DhtMessage_ConnectionType(ct)),
		}
		pbGpr.Providers = append(pbGpr.Providers, pbp)
	}

	pbGpr.Id = new(uint64)
	*pbGpr.Id = dhtMsg.GetProviderRsp.Id
	pbGpr.Extra = dhtMsg.GetProviderRsp.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetGetProviderRspPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht ping package from dht message
//
func (dhtMsg *DhtMessage)GetPingPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbPing := new(pb.DhtMessage_Ping)
	pbMsg := pb.DhtMessage{
		MsgType:	new(pb.DhtMessage_MessageType),
		Ping:		pbPing,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_PING

	pbPing.From = dhtMsg.setNode(&dhtMsg.Ping.From, pb.DhtMessage_CONT_YES)
	pbPing.To = dhtMsg.setNode(&dhtMsg.Ping.To, pb.DhtMessage_CONT_YES)
	pbPing.Seq = new(uint64)
	*pbPing.Seq = dhtMsg.Ping.Seq
	pbPing.Extra = dhtMsg.Ping.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetPingPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Setup dht pong package from dht message
//
func (dhtMsg *DhtMessage)GetPongPackage(dhtPkg *DhtPackage) DhtErrno {

	if dhtPkg == nil {
		return DhtEnoParameter
	}

	pbPong := new(pb.DhtMessage_Pong)
	pbMsg := pb.DhtMessage{
		MsgType:	new(pb.DhtMessage_MessageType),
		Pong:		pbPong,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_PONG

	pbPong.From = dhtMsg.setNode(&dhtMsg.Pong.From, pb.DhtMessage_CONT_YES)
	pbPong.To = dhtMsg.setNode(&dhtMsg.Pong.To, pb.DhtMessage_CONT_YES)
	pbPong.Seq = new(uint64)
	*pbPong.Seq = dhtMsg.Pong.Seq
	pbPong.Extra = dhtMsg.Pong.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		log.LogCallerFileLine("GetPongPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}
