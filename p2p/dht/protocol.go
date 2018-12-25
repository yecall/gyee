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
	"bytes"
	pb		"github.com/yeeco/gyee/p2p/dht/pb"
	config	"github.com/yeeco/gyee/p2p/config"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type protoLogger struct {
	debug__		bool
}

var protoLog = protoLogger  {
	debug__:	true,
}

func (log protoLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

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
	MID_UNKNOWN				= 0xffffffff
)

//
// Value
//
type DhtKey = []byte
type DhtVal	= []byte

type DhtValue struct {
	Key				DhtKey					// key of value
	Val				DhtVal					// value
	Extra			interface{}				// extra inforamtion
}

//
// Provider
//
type DhtProvider struct {
	Key				DhtKey					// key of provider
	Nodes			[]*config.Node			// node of provider
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
	Target			config.DsKey			// target node identity
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type Neighbors struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Nodes			[]*config.Node			// neighbor nodes
	Pcs				[]int					// peer connection status
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type PutValue struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Values			[]DhtValue				// values
	Id				int64					// message identity
	KT				time.Duration			// duration to keep this [key, val] pair
	Extra			[]byte					// extra info
}

type GetValueReq struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Key				DhtKey					// keys requested
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type GetValueRsp struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Value			*DhtValue				// values
	Key				DhtKey					// keys requested
	Nodes			[]*config.Node			// neighbor nodes
	Pcs				[]int					// peer connection status
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type PutProvider struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Provider		*DhtProvider			// providers
	Pcs				[]int					// prividers connection status
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderReq struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Key				DhtKey					// key wanted
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type GetProviderRsp struct {
	From			config.Node				// source node
	To				config.Node				// destination node
	Provider		*DhtProvider			// provider
	Key				DhtKey					// key
	Nodes			[]*config.Node			// neighbor nodes
	Pcs				[]int					// peer connection status
	Id				int64					// message identity
	Extra			[]byte					// extra info
}

type Ping struct {
	From			config.Node				// from whom
	To				config.Node				// to whom
	Seq				int64					// sequence
	Extra			[]byte					// extra info
}

type Pong struct {
	From			config.Node				// from whom
	To				config.Node				// to whom
	Seq				int64					// sequence
	Extra			[]byte					// extra info
}

//
// Data store (key, value) record
//
type DhtDatastoreRecord struct {
	Key				[]byte					// key
	Value			[]byte					// value
	Extra			[]byte					// extra info
}

//
// Provider store (key, provider) record
//
type DhtProviderStoreRecord struct {
	Key				[]byte					// key
	Providers		[]*config.Node			// providers
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
		protoLog.Debug("GetMessage: Unmarshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetMessage: invalid mid: %d", mid)
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
		protoLog.Debug("GetPackage: invalid mid: %d", mid)
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
		protoLog.Debug("GetHandshakeMessage: invalid parameters")
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
		protoLog.Debug("GetHandshakeMessage: DHT not supported")
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
		protoLog.Debug("GetFindNodeMessage: invalid parameters")
		return DhtEnoParameter
	}

	fn := new(FindNode)

	fn.From = *dhtMsg.getNode(pbMsg.From)
	fn.To = *dhtMsg.getNode(pbMsg.To)
	copy(fn.Target[0:], pbMsg.Target)
	fn.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetNeighborsMessage: invalid parameters")
		return DhtEnoParameter
	}

	nbs := new(Neighbors)

	nbs.From = *dhtMsg.getNode(pbMsg.From)
	nbs.To = *dhtMsg.getNode(pbMsg.To)
	for _, n := range pbMsg.Nodes {
		nbs.Nodes = append(nbs.Nodes, dhtMsg.getNode(n))
		nbs.Pcs = append(nbs.Pcs, int(*n.ConnType))
	}
	nbs.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetPutValueMessage: invalid parameters")
		return DhtEnoParameter
	}

	pv := new(PutValue)

	pv.From = *dhtMsg.getNode(pbMsg.From)
	pv.To = *dhtMsg.getNode(pbMsg.To)

	for _, v := range pbMsg.Values {
		val := DhtValue {
			Key: v.Key,
			Val: v.Val,
		}
		pv.Values = append(pv.Values, val)
	}

	pv.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetGetValueReqMessage: invalid parameters")
		return DhtEnoParameter
	}

	gvr := new(GetValueReq)

	gvr.From = *dhtMsg.getNode(pbMsg.From)
	gvr.To = *dhtMsg.getNode(pbMsg.To)

	k := pbMsg.Key
	dhtK := DhtKey(k)
	gvr.Key = dhtK

	gvr.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetGetValueRspMessage: invalid parameters")
		return DhtEnoParameter
	}

	gvr := new(GetValueRsp)

	gvr.From = *dhtMsg.getNode(pbMsg.From)
	gvr.To = *dhtMsg.getNode(pbMsg.To)

	v := pbMsg.Value
	dhtValue := DhtValue {
		Key: DhtKey(v.Key),
		Val: DhtVal(v.Val),
	}
	gvr.Value = &dhtValue

	gvr.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetPutProviderMessage: invalid parameters")
		return DhtEnoParameter
	}

	pp := new(PutProvider)

	pp.From = *dhtMsg.getNode(pbMsg.From)
	pp.To = *dhtMsg.getNode(pbMsg.To)

	p := pbMsg.Provider
	dhtP := DhtProvider {
		Key: 	DhtKey(p.Key),
	}
	for _, n := range p.Nodes {
		dhtP.Nodes = append(dhtP.Nodes, dhtMsg.getNode(n))
	}
	pp.Provider = &dhtP

	pp.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetGetProviderReqMessage: invalid parameters")
		return DhtEnoParameter
	}

	gpr := new(GetProviderReq)

	gpr.From = *dhtMsg.getNode(pbMsg.From)
	gpr.To = *dhtMsg.getNode(pbMsg.To)

	k := pbMsg.Key
	dhtK := DhtKey(k)
	gpr.Key = dhtK

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
		protoLog.Debug("GetGetProviderRspMessage: invalid parameters")
		return DhtEnoParameter
	}

	gpr := new(GetProviderRsp)

	gpr.From = *dhtMsg.getNode(pbMsg.From)
	gpr.To = *dhtMsg.getNode(pbMsg.To)

	p := pbMsg.Provider
	gpr.Provider = &DhtProvider{
		Key:	DhtKey(p.Key),
	}
	for _, n := range p.Nodes {
		gpr.Provider.Nodes = append(gpr.Provider.Nodes, dhtMsg.getNode(n))
	}

	gpr.Id = int64(*pbMsg.Id)
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
		protoLog.Debug("GetPingMessage: invalid parameters")
		return DhtEnoParameter
	}

	ping := new(Ping)

	ping.From = *dhtMsg.getNode(pbMsg.From)
	ping.To = *dhtMsg.getNode(pbMsg.To)
	ping.Seq = int64(*pbMsg.Seq)
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
		protoLog.Debug("GetPongMessage: invalid parameters")
		return DhtEnoParameter
	}

	pong := new(Pong)

	pong.From = *dhtMsg.getNode(pbMsg.From)
	pong.To = *dhtMsg.getNode(pbMsg.To)
	pong.Seq = int64(*pbMsg.Seq)
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
		protoLog.Debug("GetHandshakePackage: invalid parameters")
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
		protoLog.Debug("GetHandshakePackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetFindNodePackage: invalid parameters")
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
	*pbFn.Id = uint64(fn.Id)
	pbFn.Extra = fn.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetFindNodePackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetNeighborsPackage: invalid parameters")
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
	*pbNbs.Id = uint64(nbs.Id)
	pbNbs.Extra = nbs.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetNeighborsPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetPutValuePackage: invalid parameters")
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
	*pbPv.Id = uint64(pv.Id)
	pbPv.Extra = pv.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetPutValuePackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetGetValueReqPackage: invalid parameters")
		return DhtEnoParameter
	}

	pbGvr := new(pb.DhtMessage_GetValueReq)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetValueReq:	pbGvr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETVALUE_REQ
	gvr := dhtMsg.GetValueReq

	pbGvr.From = dhtMsg.setNode(&gvr.From, pb.DhtMessage_CONT_YES)
	pbGvr.To = dhtMsg.setNode(&gvr.To, pb.DhtMessage_CONT_YES)
	pbGvr.Key = gvr.Key

	pbGvr.Id = new(uint64)
	*pbGvr.Id = uint64(gvr.Id)
	pbGvr.Extra = dhtMsg.GetProviderReq.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetGetValueReqPackage: failed, err: %s", err.Error())
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
		protoLog.Debug("GetGetValueRspPackage: invalid parameters")
		return DhtEnoParameter
	}

	pbGvr := new(pb.DhtMessage_GetValueRsp)
	pbMsg := pb.DhtMessage {
		MsgType:		new(pb.DhtMessage_MessageType),
		GetValueRsp:	pbGvr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETVALUE_RSP
	gvr := dhtMsg.GetValueRsp

	pbGvr.From = dhtMsg.setNode(&gvr.From, pb.DhtMessage_CONT_YES)
	pbGvr.To = dhtMsg.setNode(&gvr.To, pb.DhtMessage_CONT_YES)

	v := gvr.Value
	pbV := &pb.DhtMessage_Value {
		Key:	v.Key,
		Val:	v.Val,
	}
	pbGvr.Value = pbV

	pbGvr.Id = new(uint64)
	*pbGvr.Id = uint64(gvr.Id)
	pbGvr.Extra = gvr.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetGetValueRspPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetPutProviderPackage: invalid parameters")
		return DhtEnoParameter
	}

	pbPP := new(pb.DhtMessage_PutProvider)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		PutProvider:	pbPP,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_PUTPROVIDER
	pp := dhtMsg.PutProvider

	pbPP.From = dhtMsg.setNode(&pp.From, pb.DhtMessage_CONT_YES)
	pbPP.To = dhtMsg.setNode(&pp.To, pb.DhtMessage_CONT_YES)
	pbPP.Provider = &pb.DhtMessage_Provider {
		Key:	pp.Provider.Key,
	}

	for idx, p := range pp.Provider.Nodes {
		ct := pp.Pcs[idx]
		pbPP.Provider.Nodes = append(pbPP.Provider.Nodes, dhtMsg.setNode(p, pb.DhtMessage_ConnectionType(ct)))
	}

	pbPP.Id = new(uint64)
	*pbPP.Id = uint64(pp.Id)
	pbPP.Extra = pp.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetPutProviderPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetGetProviderReqPackage: invalid parameters")
		return DhtEnoParameter
	}

	pbGpr := new(pb.DhtMessage_GetProviderReq)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetProviderReq:	pbGpr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETPROVIDER_REQ
	gpr := dhtMsg.GetProviderReq

	pbGpr.From = dhtMsg.setNode(&gpr.From, pb.DhtMessage_CONT_YES)
	pbGpr.To = dhtMsg.setNode(&gpr.To, pb.DhtMessage_CONT_YES)
	pbGpr.Key = gpr.Key
	pbGpr.Id = new(uint64)
	*pbGpr.Id = uint64(gpr.Id)
	pbGpr.Extra = gpr.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetGetProviderReqPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetGetProviderRspPackage: invalid parameters")
		return DhtEnoParameter
	}

	pbGpr := new(pb.DhtMessage_GetProviderRsp)
	pbMsg := pb.DhtMessage{
		MsgType:		new(pb.DhtMessage_MessageType),
		GetProviderRsp:	pbGpr,
	}
	*pbMsg.MsgType = pb.DhtMessage_MID_GETPROVIDER_RSP
	gpr := dhtMsg.GetProviderRsp

	pbGpr.From = dhtMsg.setNode(&gpr.From, pb.DhtMessage_CONT_YES)
	pbGpr.To = dhtMsg.setNode(&gpr.To, pb.DhtMessage_CONT_YES)
	pbGpr.Provider = &pb.DhtMessage_Provider {
		Key:	gpr.Provider.Key,
	}

	for idx, p := range gpr.Provider.Nodes {
		ct := gpr.Pcs[idx]
		pbGpr.Provider.Nodes = append(pbGpr.Provider.Nodes,dhtMsg.setNode(p, pb.DhtMessage_ConnectionType(ct)))
	}

	pbGpr.Id = new(uint64)
	*pbGpr.Id = uint64(gpr.Id)
	pbGpr.Extra = gpr.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetGetProviderRspPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetPingPackage: invalid parameters")
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
	*pbPing.Seq = uint64(dhtMsg.Ping.Seq)
	pbPing.Extra = dhtMsg.Ping.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetPingPackage: Marshal failed, err: %s", err.Error())
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
		protoLog.Debug("GetPongPackage: invalid parameters")
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
	*pbPong.Seq = uint64(dhtMsg.Pong.Seq)
	pbPong.Extra = dhtMsg.Pong.Extra

	pl, err := pbMsg.Marshal()
	if err != nil {
		protoLog.Debug("GetPongPackage: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	dhtPkg.Pid = uint32(PID_DHT)
	dhtPkg.PayloadLength = uint32(len(pl))
	dhtPkg.Payload = pl

	return DhtEnoNone
}

//
// Encode data store (key, value) record
//
func (dhtDsRec *DhtDatastoreRecord)EncDsRecord(dsr *DsRecord) DhtErrno {

	if dsr == nil {
		protoLog.Debug("EncDsRecord: invalid parameters")
		return DhtEnoParameter
	}

	if len(dhtDsRec.Key) != cap(dsr.Key) {
		protoLog.Debug("EncDsRecord: key length mismatched")
		return DhtEnoMismatched
	}
	copy(dsr.Key[0:], dhtDsRec.Key)

	pbr := pb.DhtRecord {
		Key:	dhtDsRec.Key,
		Value:	dhtDsRec.Value,
		Extra:	dhtDsRec.Extra,
	}

	val, err :=pbr.Marshal()
	if err != nil {
		protoLog.Debug("EncDsRecord: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}
	dsr.Value = val

	return DhtEnoNone
}

//
// Decode data store (key, value) record
//
func (dhtDsRec *DhtDatastoreRecord)DecDsRecord(dsr *DsRecord) DhtErrno {

	if dsr == nil {
		protoLog.Debug("DecDsRecord: invalid parameters")
		return DhtEnoParameter
	}

	pbdpr := pb.DhtRecord{}
	if err := pbdpr.Unmarshal(dsr.Value.([]byte)); err != nil {
		protoLog.Debug("DecDsRecord: Unmarshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	if bytes.Equal(pbdpr.Key, dsr.Key[0:]) != true {
		protoLog.Debug("DecDsRecord: key mismatched")
		return DhtEnoMismatched
	}

	dhtDsRec.Key = pbdpr.Key
	dhtDsRec.Value = pbdpr.Value
	dhtDsRec.Extra = pbdpr.Extra

	return DhtEnoNone
}

//
// Encode provider store (key, provider) record
//
func (dhtPsRec *DhtProviderStoreRecord)EncPsRecord(psr *PsRecord) DhtErrno {

	if psr == nil {
		protoLog.Debug("EncPsRecord: invalid parameters")
		return DhtEnoParameter
	}

	pbdpr := new(pb.DhtProviderRecord)
	pbdpr.Key = dhtPsRec.Key
	pbdpr.Extra = dhtPsRec.Extra

	for _, prd := range dhtPsRec.Providers {
		pbprd := new(pb.DhtProviderRecord_Provider)
		pbprd.Extra = nil
		pbprd.Node = &pb.DhtProviderRecord_Node {
			NodeId:	prd.ID[0:],
			IP:		prd.IP,
			TCP:	new(uint32),
			UDP:	new(uint32),
			Extra:	nil,
		}
		*pbprd.Node.TCP = uint32(prd.TCP)
		*pbprd.Node.UDP = uint32(prd.UDP)
		pbdpr.Providers = append(pbdpr.Providers, pbprd)
	}

	val, err := pbdpr.Marshal()
	if err != nil {
		protoLog.Debug("EncPsRecord: Marshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	copy(psr.Key[0:], pbdpr.Key)
	psr.Value = val

	return DhtEnoNone
}

//
// Decode provider store (key, provider) record
//
func (dhtPsRec *DhtProviderStoreRecord)DecPsRecord(psr *PsRecord) DhtErrno {

	if psr == nil {
		protoLog.Debug("DecPsRecord: invalid parameters")
		return DhtEnoParameter
	}

	pbdpr := pb.DhtProviderRecord{}
	if err := pbdpr.Unmarshal(psr.Value.([]byte)); err != nil {
		protoLog.Debug("DecPsRecord: Unmarshal failed, err: %s", err.Error())
		return DhtEnoSerialization
	}

	if bytes.Equal(pbdpr.Key, psr.Key[0:]) != true {
		protoLog.Debug("DecPsRecord：key mismatched")
		return DhtEnoMismatched
	}

	dhtPsRec.Key = pbdpr.Key
	dhtPsRec.Extra = pbdpr.Extra
	for _, prd := range pbdpr.Providers {
		n := new(config.Node)
		copy(n.ID[0:], prd.Node.NodeId)
		n.IP = prd.Node.IP
		n.UDP = uint16(*prd.Node.UDP & 0xffff)
		n.TCP = uint16(*prd.Node.TCP & 0xffff)
		dhtPsRec.Providers = append(dhtPsRec.Providers, n)
	}

	return DhtEnoNone
}
