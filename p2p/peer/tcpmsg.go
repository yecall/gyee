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

package peer

import (
	"io"
	"time"
	"net"
	ggio "github.com/gogo/protobuf/io"
	ycfg	"github.com/yeeco/p2p/config"
	pb		"github.com/yeeco/p2p/peer/pb"
	yclog	"github.com/yeeco/p2p/logger"
)


//
// Max protocols supported
//
const MaxProtocols = ycfg.MaxProtocols

//
// Protocol identities
//
const (
	PID_P2P			= pb.ProtocolId_PID_P2P
	PID_EXT			= pb.ProtocolId_PID_EXT
)

//
// Message identities
//
const (
	MID_HANDSHAKE	= pb.MessageId_MID_HANDSHAKE
	MID_PING		= pb.MessageId_MID_PING
	MID_PONG		= pb.MessageId_MID_PONG
)

//
// Protocol
//
type Protocol struct {
	Pid		uint32	// protocol identity
	Ver		[4]byte	// protocol version: M.m0.m1.m2
}

//
// Handshake message
//
type Handshake struct {
	NodeId		ycfg.NodeID	// node identity
	IP			net.IP		// ip address
	UDP			uint32		// udp port number
	TCP			uint32		// tcp port number
	ProtoNum	uint32		// number of protocols supported
	Protocols	[]Protocol	// version of protocol
}

//
// PingPong message
//
type Pingpong struct {
	Seq			uint64		// sequence
	Extra		[]byte		// extra info
}

//
// Package for TCP message
//
type P2pPackage struct {
	Pid				uint32	// protocol identity
	PayloadLength	uint32	// payload length
	Payload			[]byte	// payload
}

//
// Message for TCP message
//
type P2pMessage struct {
	Mid				uint32
	Ping			*Pingpong
	Pong			*Pingpong
	Handshake		*Handshake
}

//
// Read handshake message from inbound peer
//
func (upkg *P2pPackage)getHandshakeInbound(inst *peerInstance) (*Handshake, PeMgrErrno) {

	//
	// read "package" message firstly
	//

	if inst.hto != 0 {
		inst.conn.SetReadDeadline(time.Now().Add(inst.hto))
	} else {
		inst.conn.SetReadDeadline(time.Time{})
	}

	r := inst.conn.(io.Reader)
	gr := ggio.NewDelimitedReader(r, inst.maxPkgSize)
	pkg := new(pb.P2PPackage)

	if err := gr.ReadMsg(pkg); err != nil {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"ReadMsg faied, err: %s",
			err.Error())

		return nil, PeMgrEnoOs
	}

	//
	// check the package read
	//

	if *pkg.Pid != PID_P2P {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"not a p2p package, pid: %d",
			*pkg.Pid)

		return nil, PeMgrEnoMessage
	}

	if *pkg.PayloadLength <= 0 {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"invalid payload length: %d",
			*pkg.PayloadLength)

		return nil, PeMgrEnoMessage
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"payload length mismatched, PlLen: %d, real: %d",
			*pkg.PayloadLength, len(pkg.Payload))

		return nil, PeMgrEnoMessage
	}

	//
	// decode the payload to get "handshake" message
	//

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(pkg.Payload); err != nil {

		yclog.LogCallerFileLine("getHandshakeInbound:" +
			"Unmarshal failed, err: %s",
			err.Error())

		return nil, PeMgrEnoMessage
	}

	//
	// check the "handshake" message
	//
	
	if *pbMsg.Mid != MID_HANDSHAKE {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"it's not a handshake message, mid: %d",
			*pbMsg.Mid)

		return nil, PeMgrEnoMessage
	}

	pbHS := pbMsg.Handshake

	if pbHS == nil {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"invalid handshake message pointer: %p",
			pbHS)

		return nil, PeMgrEnoMessage
	}

	if len(pbHS.NodeId) != ycfg.NodeIDBytes {

		yclog.LogCallerFileLine("getHandshakeInbound:" +
			"invalid node identity length: %d",
				len(pbHS.NodeId))

		return nil, PeMgrEnoMessage
	}

	if *pbHS.ProtoNum > MaxProtocols {

		yclog.LogCallerFileLine("getHandshakeInbound:" +
			"too much protocols: %d",
			*pbHS.ProtoNum)

		return nil, PeMgrEnoMessage
	}

	if int(*pbHS.ProtoNum) != len(pbHS.Protocols) {

		yclog.LogCallerFileLine("getHandshakeInbound: " +
			"number of protocols mismathced, ProtoNum: %d, real: %d",
			int(*pbHS.ProtoNum), len(pbHS.Protocols))

		return nil, PeMgrEnoMessage
	}

	//
	// get handshake info to return
	//

	var ptrMsg = new(Handshake)
	copy(ptrMsg.NodeId[:], pbHS.NodeId)
	ptrMsg.IP = append(ptrMsg.IP, pbHS.IP...)
	ptrMsg.UDP = *pbHS.UDP
	ptrMsg.TCP = *pbHS.TCP
	ptrMsg.ProtoNum = *pbHS.ProtoNum

	ptrMsg.Protocols = make([]Protocol, len(pbHS.Protocols))
	for i, p := range pbHS.Protocols {
		ptrMsg.Protocols[i].Pid = uint32(*p.Pid)
		copy(ptrMsg.Protocols[i].Ver[:], p.Ver)
	}

	return ptrMsg, PeMgrEnoNone
}

//
// Write handshake message to peer
//
func (upkg *P2pPackage)putHandshakeOutbound(inst *peerInstance, hs *Handshake) PeMgrErrno {

	//
	// encode "handshake" message as the payload of "package" message
	//

	pbHandshakeMsg := new(pb.P2PMessage_Handshake)
	pbHandshakeMsg.NodeId = append(pbHandshakeMsg.NodeId, hs.NodeId[:] ...)
	pbHandshakeMsg.IP = append(pbHandshakeMsg.IP, hs.IP...)
	pbHandshakeMsg.TCP = &hs.TCP
	pbHandshakeMsg.UDP = &hs.UDP
	pbHandshakeMsg.ProtoNum = &hs.ProtoNum
	pbHandshakeMsg.Protocols = make([]*pb.P2PMessage_Protocol, *pbHandshakeMsg.ProtoNum)

	for i, p := range hs.Protocols {
		var pbProto = new(pb.P2PMessage_Protocol)
		pbHandshakeMsg.Protocols[i] = pbProto
		pbProto.Pid = new(pb.ProtocolId)
		*pbProto.Pid = pb.ProtocolId_PID_P2P
		pbProto.Ver = append(pbProto.Ver, p.Ver[:]...)
	}

	pbMsg := new(pb.P2PMessage)
	pbMsg.Mid = new(pb.MessageId)
	*pbMsg.Mid = pb.MessageId_MID_HANDSHAKE
	pbMsg.Handshake = pbHandshakeMsg

	payload, err1 := pbMsg.Marshal()
	if err1 != nil {

		yclog.LogCallerFileLine("putHandshakeOutbound:" +
			"Marshal failed, err: %s",
			err1.Error())

		return PeMgrEnoMessage
	}

	//
	// setup the "package"
	//

	pbPkg := new(pb.P2PPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = pb.ProtocolId_PID_P2P
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(len(payload))
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// send package to peer, notice need to encode here directly, it would be
	// done in calling of gw.WriteMsg, see bellow.
	//

	if inst.hto != 0 {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.hto))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	w := inst.conn.(io.Writer)
	gw := ggio.NewDelimitedWriter(w)

	if err := gw.WriteMsg(pbPkg); err != nil {

		yclog.LogCallerFileLine("putHandshakeOutbound:" +
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Ping
//
func (upkg *P2pPackage)ping(inst *peerInstance, ping *Pingpong) PeMgrErrno {

	//
	// encode ping message to payload
	//

	pbPing := pb.P2PMessage{
		Mid: 		new(pb.MessageId),
		Handshake:	nil,
		Ping:		&pb.P2PMessage_Ping{
			Seq:	&ping.Seq,
			Extra:	make([]byte, 0),
		},
		Pong:		nil,
	}

	*pbPing.Mid = pb.MessageId_MID_PING
	pbPing.Ping.Extra = append(pbPing.Ping.Extra, ping.Extra...)

	payload, err := pbPing.Marshal()

	if len(payload) == 0 || err != nil {
		yclog.LogCallerFileLine("ping: empty payload")
		return PeMgrEnoMessage
	}

	//
	// setup package for payload
	//

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = pb.ProtocolId_PID_P2P
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// Set deadline
	//

	if inst.hto != 0 {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	//
	// write package to peer. in practice, we find that two routines can not
	// share a conection to write without sync.
	//

	inst.p2pkgLock.Lock()

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		yclog.LogCallerFileLine("ping:" +
			"Write failed, err: %s",
			err.Error())

		inst.p2pkgLock.Unlock()

		return PeMgrEnoOs
	}

	inst.p2pkgLock.Unlock()

	return PeMgrEnoNone
}

//
// Ping
//
func (upkg *P2pPackage)pong(inst *peerInstance, pong *Pingpong) PeMgrErrno {

	//
	// encode pong message to payload.
	//

	pbPong := pb.P2PMessage{
		Mid: 		new(pb.MessageId),
		Handshake:	nil,
		Ping:		nil,
		Pong:		&pb.P2PMessage_Pong{
			Seq:	&pong.Seq,
			Extra:	make([]byte, 0),
		},
	}

	*pbPong.Mid = pb.MessageId_MID_PONG
	pbPong.Pong.Extra = append(pbPong.Pong.Extra, pong.Extra...)

	payload, err := pbPong.Marshal()

	if len(payload) == 0 || err != nil {
		yclog.LogCallerFileLine("pong: empty payload")
		return PeMgrEnoMessage
	}

	//
	// setup package for payload
	//

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = pb.ProtocolId_PID_P2P
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// Set deadline
	//

	if inst.hto != 0 {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	//
	// write package to peer. in practice, we find that two routines can not
	// share a conection to write without sync.
	//

	inst.p2pkgLock.Lock()

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		yclog.LogCallerFileLine("pong:" +
			"Write failed, err: %s",
			err.Error())

		inst.p2pkgLock.Unlock()

		return PeMgrEnoOs
	}

	inst.p2pkgLock.Unlock()

	return PeMgrEnoNone
}

//
// Send user packege
//

func (upkg *P2pPackage)SendPackage(inst *peerInstance) PeMgrErrno {

	if inst == nil {
		yclog.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	//
	// Setup the protobuf "package"
	//

	pbPkg := new(pb.P2PPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = pb.ProtocolId(upkg.Pid)
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(upkg.PayloadLength)
	pbPkg.Payload = append(pbPkg.Payload, upkg.Payload...)

	//
	// Set deadline
	//

	if inst.ato != 0 {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	//
	// Write package to peer. In practice, we find that two routines can not
	// share a conection to write without sync. But sync is unnecessary here
	// for this function here had enter the critical section, see how it is
	// called pls.
	//

	if err := inst.iow.WriteMsg(pbPkg); err != nil {

		yclog.LogCallerFileLine("SendPackage:" +
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Receive user package
//
func (upkg *P2pPackage)RecvPackage(inst *peerInstance) PeMgrErrno {

	if inst == nil {
		yclog.LogCallerFileLine("RecvPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	//
	// Setup the reader
	//

	if inst.ato != 0 {
		inst.conn.SetReadDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetReadDeadline(time.Time{})
	}

	//
	// New protobuf "package" and read peer into it
	//

	pkg := new(pb.P2PPackage)

	if err := inst.ior.ReadMsg(pkg); err != nil {

		yclog.LogCallerFileLine("RecvPackage: " +
			"ReadMsg faied, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	//
	// setup upkg(user package) with type P2pPackage from pb.P2pPackage
	// object obtained above.
	//

	upkg.Pid			= uint32(*pkg.Pid)
	upkg.PayloadLength	= *pkg.PayloadLength
	upkg.Payload		= append(upkg.Payload, pkg.Payload ...)

	yclog.LogCallerFileLine("RecvPackage: " +
		"package got, Pid: %d, PayloadLength: %d",
		upkg.Pid, upkg.PayloadLength)

	return PeMgrEnoNone
}

//
// Decode message from package
//
func (upkg *P2pPackage)GetMessage(pmsg *P2pMessage) PeMgrErrno {

	//
	// upkg must hold the package extracted from a pb.pb.P2PPackage before
	// calling into here of this function.
	//

	if pmsg == nil {
		yclog.LogCallerFileLine("GetMessage: invalid parameter")
		return PeMgrEnoParameter
	}

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(upkg.Payload); err != nil {

		yclog.LogCallerFileLine("GetMessage:" +
			"Unmarshal failed, err: %s",
			err.Error())

		return PeMgrEnoMessage
	}

	pmsg.Mid = uint32(*pbMsg.Mid)
	pmsg.Handshake = nil
	pmsg.Ping = nil
	pmsg.Pong = nil

	if pmsg.Mid == uint32(MID_HANDSHAKE) {

		hs := new(Handshake)
		pmsg.Handshake = hs

		pbHS := pbMsg.Handshake
		copy(hs.NodeId[:], pbHS.NodeId)
		hs.ProtoNum = *pbHS.ProtoNum
		hs.Protocols = make([]Protocol, len(pbHS.Protocols))
		for i, p := range pbHS.Protocols {
			hs.Protocols[i].Pid = uint32(*p.Pid)
			copy(hs.Protocols[i].Ver[:], p.Ver)
		}

	} else if pmsg.Mid == uint32(MID_PING) {

		ping := new(Pingpong)
		pmsg.Ping = ping

		ping.Seq = *pbMsg.Ping.Seq
		ping.Extra = append(ping.Extra, pbMsg.Ping.Extra...)

	} else if pmsg.Mid == uint32(MID_PONG) {

		pong := new(Pingpong)
		pmsg.Pong = pong

		pong.Seq = *pbMsg.Pong.Seq
		pong.Extra = append(pong.Extra, pbMsg.Pong.Extra...)

	} else {

		yclog.LogCallerFileLine("GetMessage: " +
			"unknown message identity: %d",
			pmsg.Mid)

		return PeMgrEnoMessage
	}

	return PeMgrEnoNone
}
