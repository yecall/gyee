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

package udpmsg

import (
	"net"
	log	"github.com/yeeco/gyee/p2p/logger"
	config	"github.com/yeeco/gyee/p2p/config"
	pb		"github.com/yeeco/gyee/p2p/discover/udpmsg/pb"
)

//
// UDP messages for discovering protocol tasks
//
const (
	UdpMsgTypePing		= iota
	UdpMsgTypePong
	UdpMsgTypeFindNode
	UdpMsgTypeNeighbors
	UdpMsgTypeUnknown
	UdpMsgTypeAny
)

type UdpMsgType int
type SubNetworkID = config.SubNetworkID

type (

	// Endpoint
	Endpoint struct {
		IP			net.IP		// ip address
		UDP			uint16		// udp port number
		TCP			uint16		// tcp port number
	}

	// Node: endpoint with node identity
	Node struct {
		IP			net.IP			// ip address
		UDP, TCP	uint16			// udp port number
		NodeId		config.NodeID	// node identity
	}

	//
	// Notice: if (Expiration == 0) is true, it would be never expired
	// for a message.
	//

	// Ping
	Ping struct {
		From			Node			// source node
		To				Node			// destination node
		FromSubNetId	[]SubNetworkID	// sub network identities of "From"
		SubNetId		SubNetworkID	// sub network identity
		Id				uint64			// message identity
		Expiration		uint64			// time to expired of this message
		Extra			[]byte			// extra info
	}

	// Pong: response to Ping
	Pong struct {
		From			Node			// source node
		To				Node			// destination node
		FromSubNetId	[]SubNetworkID	// sub network identities of "From"
		SubNetId		SubNetworkID	// sub network identity
		Id				uint64			// message identity
		Expiration		uint64			// time to expired of this message
		Extra			[]byte			// extra info
	}

	// FindNode: request the endpoint of the target
	FindNode struct {
		From			Node			// source node
		To				Node			// destination node
		FromSubNetId	[]SubNetworkID	// sub network identities of "From"
		SubNetId		SubNetworkID	// sub network identity
		Target			config.NodeID	// target node identity
		Id				uint64			// message identity
		Expiration		uint64			// time to expired of this message
		Extra			[]byte			// extra info
	}

	// Neighbors: response to FindNode
	Neighbors struct {
		From			Node			// source node
		To				Node			// destination node
		FromSubNetId	[]SubNetworkID	// sub network identities of "From"
		SubNetId		SubNetworkID	// sub network identity
		Nodes			[]*Node			// neighbor nodes
		Id				uint64			// message identity
		Expiration		uint64			// time to expired of this message
		Extra			[]byte			// extra info
	}
)

//
// UDP message: tow parts, the first is the raw bytes ... the seconde is
// protobuf message. for decoding, protobuf message will be extract from
// the raw one; for encoding, bytes will be wriiten into raw buffer.
//
type UdpMsg struct {
	Pbuf	*[]byte			// buffer pointer
	Len		int				// bytes buffered
	From	*net.UDPAddr	// source address from underlying network library
	Msg		*pb.UdpMessage	// protobuf message
	Eno		UdpMsgErrno		// current errno
}

//
// errno
//
const (
	UdpMsgEnoNone 		= iota
	UdpMsgEnoParameter
	UdpMsgEnoEncodeFailed
	UdpMsgEnoDecodeFailed
	UdpMsgEnoMessage
	UdpMsgEnoUnknown
)

type UdpMsgErrno int

//
// Create UdpMsg object
//
func NewUdpMsg() *UdpMsg {
	return &UdpMsg {
		Pbuf:	nil,
		Len:	0,
		From:	nil,
		Msg:	nil,
		Eno:	UdpMsgEnoUnknown,
	}
}

//
// Set raw message
//
func (pum *UdpMsg) SetRawMessage(pbuf *[]byte, bytes int, from *net.UDPAddr) UdpMsgErrno {

	if pbuf == nil || bytes == 0 || from == nil {
		log.LogCallerFileLine("SetRawMessage: invalid parameter(s)")
		return UdpMsgEnoParameter
	}

	pum.Eno = UdpMsgEnoNone
	pum.Pbuf = pbuf
	pum.Len = bytes
	pum.From = from

	return UdpMsgEnoNone
}

//
// Decoding
//
func (pum *UdpMsg) Decode() UdpMsgErrno {

	pum.Msg = new(pb.UdpMessage)

	if err := (pum.Msg).Unmarshal((*pum.Pbuf)[0:pum.Len]); err != nil {

		log.LogCallerFileLine("Decode: " +
			"Unmarshal failed, err: %s",
			err.Error())

		return UdpMsgEnoDecodeFailed
	}

	return UdpMsgEnoNone
}

//
// Get decoded message
//
func (pum *UdpMsg) GetPbMessage() *pb.UdpMessage {
	return pum.Msg
}

//
// Get decoded message
//
func (pum *UdpMsg) GetDecodedMsg() interface{} {

	//
	// get type
	//

	mt := pum.GetDecodedMsgType()

	if mt == UdpMsgTypeUnknown {

		log.LogCallerFileLine("GetDecodedMsg: " +
			"GetDecodedMsgType failed, mt: %d",
			mt)

		return nil
	}

	//
	// map type to function and the get
	//

	var funcMap = map[UdpMsgType]interface{} {

		UdpMsgTypePing: pum.GetPing,
		UdpMsgTypePong: pum.GetPong,
		UdpMsgTypeFindNode: pum.GetFindNode,
		UdpMsgTypeNeighbors: pum.GetNeighbors,
	}

	var f interface{}
	var ok bool

	if f, ok = funcMap[mt]; !ok {

		log.LogCallerFileLine("GetDecodedMsg: " +
			"invalid message type: %d",
			mt)

		return nil
	}

	return f.(func()interface{})()
}

//
// Get deocded message type
//
func (pum *UdpMsg) GetDecodedMsgType() UdpMsgType {

	var pbMap = map[pb.UdpMessage_MessageType]UdpMsgType {

		pb.UdpMessage_PING:			UdpMsgTypePing,
		pb.UdpMessage_PONG:			UdpMsgTypePong,
		pb.UdpMessage_FINDNODE:		UdpMsgTypeFindNode,
		pb.UdpMessage_NEIGHBORS:	UdpMsgTypeNeighbors,
	}

	var key pb.UdpMessage_MessageType
	var val UdpMsgType
	var ok bool

	key = pum.Msg.GetMsgType()

	if val, ok = pbMap[key]; !ok {

		log.LogCallerFileLine("GetDecodedMsgType: invalid message type")
		return UdpMsgTypeUnknown
	}

	return val
}

//
// Get decoded Ping
//
func (pum *UdpMsg) GetPing() interface{} {

	pbPing := pum.Msg.Ping
	ping := new(Ping)

	ping.From.IP = append(ping.From.IP, pbPing.From.IP...)
	ping.From.TCP = uint16(*pbPing.From.TCP)
	ping.From.UDP = uint16(*pbPing.From.UDP)
	copy(ping.From.NodeId[:], pbPing.From.NodeId)

	ping.To.IP = append(ping.To.IP, pbPing.To.IP...)
	ping.To.TCP = uint16(*pbPing.To.TCP)
	ping.To.UDP = uint16(*pbPing.To.UDP)
	copy(ping.To.NodeId[:], pbPing.To.NodeId)

	for _, snid := range pbPing.FromSubNetId {
		var id SubNetworkID
		copy(id[0:], snid.Id[:])
		ping.FromSubNetId = append(ping.FromSubNetId, id)
	}

	copy(ping.SubNetId[0:], pbPing.SubNetId.Id[:])

	ping.Id = *pbPing.Id
	ping.Expiration = *pbPing.Expiration
	ping.Extra = append(ping.Extra, pbPing.Extra...)

	return ping
}

//
// Get decoded Pong
//
func (pum *UdpMsg) GetPong() interface{} {

	pbPong := pum.Msg.Pong
	pong := new(Pong)

	pong.From.IP = append(pong.From.IP, pbPong.From.IP...)
	pong.From.TCP = uint16(*pbPong.From.TCP)
	pong.From.UDP = uint16(*pbPong.From.UDP)
	copy(pong.From.NodeId[:], pbPong.From.NodeId)

	pong.To.IP = append(pong.To.IP, pbPong.To.IP...)
	pong.To.TCP = uint16(*pbPong.To.TCP)
	pong.To.UDP = uint16(*pbPong.To.UDP)
	copy(pong.To.NodeId[:], pbPong.To.NodeId)

	for _, snid := range pbPong.FromSubNetId {
		var id SubNetworkID
		copy(id[0:], snid.Id[:])
		pong.FromSubNetId = append(pong.FromSubNetId, id)
	}

	copy(pong.SubNetId[0:], pbPong.SubNetId.Id[:])

	pong.Id = *pbPong.Id
	pong.Expiration = *pbPong.Expiration
	pong.Extra = append(pong.Extra, pbPong.Extra...)

	return pong
}

//
// Get decoded FindNode
//
func (pum *UdpMsg) GetFindNode() interface{} {

	pbFN := pum.Msg.FindNode
	fn := new(FindNode)

	fn.From.IP = append(fn.From.IP, pbFN.From.IP...)
	fn.From.TCP = uint16(*pbFN.From.TCP)
	fn.From.UDP = uint16(*pbFN.From.UDP)
	copy(fn.From.NodeId[:], pbFN.From.NodeId)

	fn.To.IP = append(fn.To.IP, pbFN.To.IP...)
	fn.To.TCP = uint16(*pbFN.To.TCP)
	fn.To.UDP = uint16(*pbFN.To.UDP)
	copy(fn.To.NodeId[:], pbFN.To.NodeId)

	for _, snid := range pbFN.FromSubNetId {
		var id SubNetworkID
		copy(id[0:], snid.Id[:])
		fn.FromSubNetId = append(fn.FromSubNetId, id)
	}

	copy(fn.SubNetId[0:], pbFN.SubNetId.Id[:])

	copy(fn.Target[:], pbFN.Target)

	fn.Id = *pbFN.Id
	fn.Expiration = *pbFN.Expiration
	fn.Extra = append(fn.Extra, pbFN.Extra...)

	return fn
}

//
// Get decoded Neighbors
//
func (pum *UdpMsg) GetNeighbors() interface{} {

	pbNgb := pum.Msg.Neighbors
	ngb := new(Neighbors)

	ngb.From.IP = append(ngb.From.IP, pbNgb.From.IP...)
	ngb.From.TCP = uint16(*pbNgb.From.TCP)
	ngb.From.UDP = uint16(*pbNgb.From.UDP)
	copy(ngb.From.NodeId[:], pbNgb.From.NodeId)

	ngb.To.IP = append(ngb.To.IP, pbNgb.To.IP...)
	ngb.To.TCP = uint16(*pbNgb.To.TCP)
	ngb.To.UDP = uint16(*pbNgb.To.UDP)
	copy(ngb.To.NodeId[:], pbNgb.To.NodeId)

	for _, snid := range pbNgb.FromSubNetId {
		var id SubNetworkID
		copy(id[0:], snid.Id[:])
		ngb.FromSubNetId = append(ngb.FromSubNetId, id)
	}

	copy(ngb.SubNetId[0:], pbNgb.SubNetId.Id[:])

	ngb.Id = *pbNgb.Id
	ngb.Expiration = *pbNgb.Expiration
	ngb.Extra = append(ngb.Extra, pbNgb.Extra...)

	ngb.Nodes = make([]*Node, len(pbNgb.Nodes))
	for idx, n := range pbNgb.Nodes {
		pn := new(Node)
		pn.IP = append(pn.IP, n.IP...)
		pn.TCP = uint16(*n.TCP)
		pn.UDP = uint16(*n.UDP)
		copy(pn.NodeId[:], n.NodeId)
		ngb.Nodes[idx] = pn
	}

	return ngb
}

//
// Check decoded message with endpoint where the message from
//
func (pum *UdpMsg) CheckUdpMsgFromPeer(from *net.UDPAddr) bool {

	//
	// We just check the ip address simply now, more might be needed.
	// Also notice that, only IPV4 supported currently.
	//

	var ipv4 = net.IPv4zero

	if *pum.Msg.MsgType == pb.UdpMessage_PING {

		ipv4 = net.IP(pum.Msg.Ping.From.IP).To4()

	} else if *pum.Msg.MsgType == pb.UdpMessage_PONG  {

		ipv4 = net.IP(pum.Msg.Pong.From.IP).To4()

	} else if *pum.Msg.MsgType == pb.UdpMessage_FINDNODE {

		ipv4 = net.IP(pum.Msg.FindNode.From.IP).To4()

	} else if *pum.Msg.MsgType == pb.UdpMessage_NEIGHBORS {

		ipv4 = net.IP(pum.Msg.Neighbors.From.IP).To4()

	} else {

		return false
	}

	return ipv4.Equal(from.IP.To4())
}

//
// Encode directly from protobuf message.
// Notice: pb message to be encoded must be setup and buffer for encoded bytes
// must be allocated firstly for this function.
//
func (pum *UdpMsg) EncodePbMsg() UdpMsgErrno {

	var err error

	if *pum.Pbuf, err = pum.Msg.Marshal(); err != nil {

		log.LogCallerFileLine("Encode: " +
			"Marshal failed, err: %s",
			err.Error())

		pum.Eno = UdpMsgEnoEncodeFailed

		return pum.Eno
	}

	pum.Eno = UdpMsgEnoNone

	return pum.Eno
}

//
// Encode for UDP messages
//
func (pum *UdpMsg) Encode(t int, msg interface{}) UdpMsgErrno {

	var eno UdpMsgErrno

	pum.Msg = new(pb.UdpMessage)

	switch t {

	case UdpMsgTypePing:
		eno = pum.EncodePing(msg.(*Ping))

	case UdpMsgTypePong:
		eno = pum.EncodePong(msg.(*Pong))

	case UdpMsgTypeFindNode:
		eno = pum.EncodeFindNode(msg.(*FindNode))

	case UdpMsgTypeNeighbors:
		eno = pum.EncodeNeighbors(msg.(*Neighbors))

	default:
		eno = UdpMsgEnoParameter
	}

	if eno != UdpMsgEnoNone {
		log.LogCallerFileLine("Encode: failed, type: %d", t)
	}

	pum.Eno = eno

	return eno
}

//
// Encode Ping
//
func (pum *UdpMsg) EncodePing(ping *Ping) UdpMsgErrno {

	var pbm = pum.Msg
	var pbPing *pb.UdpMessage_Ping

	pbm.MsgType = new(pb.UdpMessage_MessageType)
	pbPing = new(pb.UdpMessage_Ping)
	*pbm.MsgType = pb.UdpMessage_PING
	pbm.Ping = pbPing
	pbm.Pong = nil
	pbm.FindNode = nil
	pbm.Neighbors = nil
	pbm.XXX_unrecognized = nil

	pbPing.From = new(pb.UdpMessage_Node)
	pbPing.From.UDP = new(uint32)
	pbPing.From.TCP = new(uint32)

	pbPing.From.IP =  append(pbPing.From.IP, ping.From.IP...)
	*pbPing.From.TCP = uint32(ping.From.TCP)
	*pbPing.From.UDP = uint32(ping.From.UDP)
	pbPing.From.NodeId = append(pbPing.From.NodeId, ping.From.NodeId[:]...)

	pbPing.To = new(pb.UdpMessage_Node)
	pbPing.To.UDP = new(uint32)
	pbPing.To.TCP = new(uint32)

	pbPing.To.IP = append(pbPing.To.IP, ping.To.IP[:]...)
	*pbPing.To.TCP = uint32(ping.To.TCP)
	*pbPing.To.UDP = uint32(ping.To.UDP)
	pbPing.To.NodeId = append(pbPing.To.NodeId, ping.To.NodeId[:]...)

	for _, snid := range ping.FromSubNetId {
		pbSnid := new(pb.UdpMessage_SubNetworkID)
		pbSnid.Id = append(pbSnid.Id, snid[:]...)
		pbPing.FromSubNetId = append(pbPing.FromSubNetId, pbSnid)
	}

	subNetId := new(pb.UdpMessage_SubNetworkID)
	subNetId.Id = append(subNetId.Id, ping.SubNetId[:]...)
	pbPing.SubNetId = subNetId

	pbPing.Id = new(uint64)
	*pbPing.Id = ping.Id

	pbPing.Expiration = new(uint64)
	*pbPing.Expiration = ping.Expiration

	pbPing.Extra = append(pbPing.Extra, ping.Extra...)

	var err error
	var buf []byte

	if buf, err = pbm.Marshal(); err != nil {

		log.LogCallerFileLine("EncodePing: fialed, err: %s", err.Error())
		return UdpMsgEnoEncodeFailed
	}

	pum.Pbuf = &buf
	pum.Len = len(buf)

	return UdpMsgEnoNone
}

//
// Encode Pong
//
func (pum *UdpMsg) EncodePong(pong *Pong) UdpMsgErrno {

	var pbm = pum.Msg
	var pbPong *pb.UdpMessage_Pong

	pbm.MsgType = new(pb.UdpMessage_MessageType)
	*pbm.MsgType = pb.UdpMessage_PONG
	pbPong = new(pb.UdpMessage_Pong)
	pbm.Ping = nil
	pbm.Pong = pbPong
	pbm.FindNode = nil
	pbm.Neighbors = nil
	pbm.XXX_unrecognized = nil


	pbPong.From = new(pb.UdpMessage_Node)
	pbPong.From.UDP = new(uint32)
	pbPong.From.TCP = new(uint32)

	pbPong.From.IP = append(pbPong.From.IP, pong.From.IP...)
	*pbPong.From.TCP = uint32(pong.From.TCP)
	*pbPong.From.UDP = uint32(pong.From.UDP)
	pbPong.From.NodeId = append(pbPong.From.NodeId, pong.From.NodeId[:]...)

	pbPong.To = new(pb.UdpMessage_Node)
	pbPong.To.UDP = new(uint32)
	pbPong.To.TCP = new(uint32)

	pbPong.To.IP = append(pbPong.To.IP, pong.To.IP...)
	*pbPong.To.TCP = uint32(pong.To.TCP)
	*pbPong.To.UDP = uint32(pong.To.UDP)
	pbPong.To.NodeId = append(pbPong.To.NodeId, pong.To.NodeId[:]...)

	for _, snid := range pong.FromSubNetId {
		pbSnid := new(pb.UdpMessage_SubNetworkID)
		pbSnid.Id = append(pbSnid.Id, snid[:]...)
		pbPong.FromSubNetId = append(pbPong.FromSubNetId, pbSnid)
	}

	subNetId := new(pb.UdpMessage_SubNetworkID)
	subNetId.Id = append(subNetId.Id, pong.SubNetId[:]...)
	pbPong.SubNetId = subNetId

	pbPong.Id = new(uint64)
	*pbPong.Id = pong.Id

	pbPong.Expiration = new(uint64)
	*pbPong.Expiration = pong.Expiration

	pbPong.Extra = append(pbPong.Extra, pong.Extra...)

	var err error
	var buf []byte

	if buf, err = pbm.Marshal(); err != nil {

		log.LogCallerFileLine("EncodePong: fialed, err: %s", err.Error())
		return UdpMsgEnoEncodeFailed
	}

	pum.Pbuf = &buf
	pum.Len = len(buf)

	return UdpMsgEnoNone
}

//
// Encode FindNode
//
func (pum *UdpMsg) EncodeFindNode(fn *FindNode) UdpMsgErrno {

	var pbm = pum.Msg
	var pbFN *pb.UdpMessage_FindNode

	pbm.MsgType = new(pb.UdpMessage_MessageType)

	pbFN = &pb.UdpMessage_FindNode {

		From: &pb.UdpMessage_Node {
			IP:					make([]byte,0),
			UDP:				new(uint32),
			TCP:				new(uint32),
			NodeId:				make([]byte,0),
			XXX_unrecognized:	make([]byte,0),
		},

		To: &pb.UdpMessage_Node {
			IP:					make([]byte,0),
			UDP:				new(uint32),
			TCP:				new(uint32),
			NodeId:				make([]byte,0),
			XXX_unrecognized:	make([]byte,0),
		},

		FromSubNetId: make([]*pb.UdpMessage_SubNetworkID, 0),
		SubNetId: &pb.UdpMessage_SubNetworkID {
			Id: make([]byte, 0),
		},

		Target:				make([]byte, 0),
		Id:					new(uint64),
		Expiration:			new(uint64),
		Extra:				make([]byte, 0),
		XXX_unrecognized:	make([]byte, 0),
	}

	*pbm.MsgType = pb.UdpMessage_FINDNODE
	pbm.Ping = nil
	pbm.Pong = nil
	pbm.FindNode = pbFN
	pbm.Neighbors = nil
	pbm.XXX_unrecognized = nil

	pbFN.From.IP = append(pbFN.From.IP, fn.From.IP...)
	*pbFN.From.TCP = uint32(fn.From.TCP)
	*pbFN.From.UDP = uint32(fn.From.UDP)
	pbFN.From.NodeId = append(pbFN.From.NodeId, fn.From.NodeId[:]...)

	pbFN.To.IP = append(pbFN.To.IP, fn.To.IP...)
	*pbFN.To.TCP = uint32(fn.To.TCP)
	*pbFN.To.UDP = uint32(fn.To.UDP)
	pbFN.To.NodeId = append(pbFN.To.NodeId, fn.To.NodeId[:]...)

	for _, snid := range fn.FromSubNetId {
		pbSnid := new(pb.UdpMessage_SubNetworkID)
		pbSnid.Id = append(pbSnid.Id, snid[:]...)
		pbFN.FromSubNetId = append(pbFN.FromSubNetId, pbSnid)
	}

	subNetId := new(pb.UdpMessage_SubNetworkID)
	subNetId.Id = append(subNetId.Id, fn.SubNetId[:]...)
	pbFN.SubNetId = subNetId

	pbFN.Target = append(pbFN.Target, fn.Target[:]...)

	*pbFN.Id = fn.Id
	*pbFN.Expiration = fn.Expiration
	pbFN.Extra = append(pbFN.Extra, fn.Extra...)

	var err error
	var buf []byte

	if buf, err = pbm.Marshal(); err != nil {

		log.LogCallerFileLine("EncodeFindNode: fialed, err: %s", err.Error())
		return UdpMsgEnoEncodeFailed
	}

	pum.Pbuf = &buf
	pum.Len = len(buf)

	return UdpMsgEnoNone
}

//
// Encode Neighbors
//
func (pum *UdpMsg) EncodeNeighbors(ngb *Neighbors) UdpMsgErrno {

	var pbm = pum.Msg
	var pbNgb *pb.UdpMessage_Neighbors

	pbm.MsgType = new(pb.UdpMessage_MessageType)
	pbNgb = new(pb.UdpMessage_Neighbors)
	*pbm.MsgType = pb.UdpMessage_NEIGHBORS
	pbm.Ping = nil
	pbm.Pong = nil
	pbm.FindNode = nil
	pbm.Neighbors = pbNgb
	pbm.XXX_unrecognized = nil

	pbNgb.From = new(pb.UdpMessage_Node)
	pbNgb.From.TCP = new(uint32)
	pbNgb.From.UDP = new(uint32)

	pbNgb.From.IP = append(pbNgb.From.IP, ngb.From.IP...)
	*pbNgb.From.TCP = uint32(ngb.From.TCP)
	*pbNgb.From.UDP = uint32(ngb.From.UDP)
	pbNgb.From.NodeId = append(pbNgb.From.NodeId, ngb.From.NodeId[:]...)

	pbNgb.To = new(pb.UdpMessage_Node)
	pbNgb.To.TCP = new(uint32)
	pbNgb.To.UDP = new(uint32)

	pbNgb.To.IP = append(pbNgb.To.IP, ngb.To.IP...)
	*pbNgb.To.TCP = uint32(ngb.To.TCP)
	*pbNgb.To.UDP = uint32(ngb.To.UDP)
	pbNgb.To.NodeId = append(pbNgb.To.NodeId, ngb.To.NodeId[:]...)

	for _, snid := range ngb.FromSubNetId {
		pbSnid := new(pb.UdpMessage_SubNetworkID)
		pbSnid.Id = append(pbSnid.Id, snid[:]...)
		pbNgb.FromSubNetId = append(pbNgb.FromSubNetId, pbSnid)
	}

	subNetId := new(pb.UdpMessage_SubNetworkID)
	subNetId.Id = append(subNetId.Id, ngb.SubNetId[:]...)
	pbNgb.SubNetId = subNetId

	pbNgb.Id = new(uint64)
	*pbNgb.Id = ngb.Id

	pbNgb.Expiration = new(uint64)
	*pbNgb.Expiration = ngb.Expiration

	pbNgb.Extra = append(pbNgb.Extra, ngb.Extra...)

	pbNgb.Nodes = make([]*pb.UdpMessage_Node, len(ngb.Nodes))

	for idx, n := range ngb.Nodes {

		nn := new(pb.UdpMessage_Node)
		nn.TCP = new(uint32)
		nn.UDP = new(uint32)

		nn.IP = append(nn.IP, n.IP...)
		*nn.TCP = uint32(n.TCP)
		*nn.UDP = uint32(n.UDP)
		nn.NodeId = append(nn.NodeId, n.NodeId[:]...)

		pbNgb.Nodes[idx] = nn
	}

	var err error
	var buf []byte

	if buf, err = pbm.Marshal(); err != nil {

		log.LogCallerFileLine("EncodeNeighbors: fialed, err: %s", err.Error())
		return UdpMsgEnoEncodeFailed
	}

	pum.Pbuf = &buf
	pum.Len = len(buf)

	return UdpMsgEnoNone
}

//
// Get buffer and length of bytes for message encoded
//
func (pum *UdpMsg) GetRawMessage() (buf []byte, len int) {

	if pum.Eno != UdpMsgEnoNone {

		return nil, 0
	}

	return *pum.Pbuf, pum.Len
}

//
// Compare two nodes
//
const (
	CmpNodeEqu		= iota
	CmpNodeNotEquId
	CmpNodeNotEquIp
	CmpNodeNotEquUdpPort
	CmpNodeNotEquTcpPort
)

func (n1 *Node) CompareWith(n2 *Node) int {

	if n1.UDP != n2.UDP {

		return CmpNodeNotEquUdpPort

	} else if n1.TCP != n2.TCP {

		return CmpNodeNotEquTcpPort

	} else if n1.IP.Equal(n2.IP) != true {

		return CmpNodeNotEquIp

	}	else	if n1.NodeId != n2.NodeId {

		return CmpNodeNotEquId
	}

	return CmpNodeEqu
}
