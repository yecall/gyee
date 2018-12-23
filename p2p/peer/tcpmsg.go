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
	config "github.com/yeeco/gyee/p2p/config"
	pb "github.com/yeeco/gyee/p2p/peer/pb"
	log	"github.com/yeeco/gyee/p2p/logger"
)


var debug__ = true

//
// Max protocols supported
//
const MaxProtocols = config.MaxProtocols

//
// Protocol identities
//
const (
	PID_P2P			= pb.ProtocolId_PID_P2P		// p2p internal
	PID_EXT			= pb.ProtocolId_PID_EXT		// external protocol
)

//
// Message identities
//
const (

	// internal MID for PID_P2P
	MID_HANDSHAKE	= pb.MessageId_MID_HANDSHAKE	// handshake
	MID_PING		= pb.MessageId_MID_PING			// ping
	MID_PONG		= pb.MessageId_MID_PONG			// pong

	// external MID for PID_EXT
	MID_TX			= pb.MessageId_MID_TX
	MID_EVENT		= pb.MessageId_MID_EVENT
	MID_BLOCKHEADER	= pb.MessageId_MID_BLOCKHEADER
	MID_BLOCK		= pb.MessageId_MID_BLOCK

	// invalid MID
	MID_INVALID		= pb.MessageId_MID_INVALID
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
	Snid		SubNetworkID	// sub network identity
	Dir			int				// direct
	NodeId		config.NodeID	// node identity
	IP			net.IP			// ip address
	UDP			uint32			// udp port number
	TCP			uint32			// tcp port number
	ProtoNum	uint32			// number of protocols supported
	Protocols	[]Protocol		// version of protocol
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
	Mid				uint32	// message identity
	Key				[]byte	// key of message
	PayloadLength	uint32	// payload length
	Payload			[]byte	// payload
}

//
// Message for TCP message
//
type P2pMessage struct {
	Mid				uint32		// message identity
	Ping			*Pingpong	// ping message
	Pong			*Pingpong	// pong message
	Handshake		*Handshake	// handshake message
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
	inst.ior = ggio.NewDelimitedReader(r, inst.maxPkgSize)
	pkg := new(pb.P2PPackage)

	if err := inst.ior.ReadMsg(pkg); err != nil {

		if debug__ {
			log.Debug("getHandshakeInbound: "+
				"ReadMsg faied, err: %s",
				err.Error())
		}

		return nil, PeMgrEnoOs
	}

	//
	// check the package read
	//

	if *pkg.Pid != PID_P2P {

		log.Debug("getHandshakeInbound: " +
			"not a p2p package, pid: %d",
			*pkg.Pid)

		return nil, PeMgrEnoMessage
	}

	if *pkg.PayloadLength <= 0 {

		log.Debug("getHandshakeInbound: " +
			"invalid payload length: %d",
			*pkg.PayloadLength)

		return nil, PeMgrEnoMessage
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {

		log.Debug("getHandshakeInbound: " +
			"payload length mismatched, PlLen: %d, real: %d",
			*pkg.PayloadLength, len(pkg.Payload))

		return nil, PeMgrEnoMessage
	}

	//
	// decode the payload to get "handshake" message
	//

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(pkg.Payload); err != nil {

		log.Debug("getHandshakeInbound:" +
			"Unmarshal failed, err: %s",
			err.Error())

		return nil, PeMgrEnoMessage
	}

	//
	// check the "handshake" message
	//
	
	if *pbMsg.Mid != MID_HANDSHAKE {

		log.Debug("getHandshakeInbound: " +
			"it's not a handshake message, mid: %d",
			*pbMsg.Mid)

		return nil, PeMgrEnoMessage
	}

	pbHS := pbMsg.Handshake

	if upkg.verifyInbound(inst, pbHS) != true {
		log.Debug("putHandshakeOutbound: verifyInbound failed")
		return nil, PeMgrEnoVerify
	}

	if pbHS == nil {

		log.Debug("getHandshakeInbound: " +
			"invalid handshake message pointer: %p",
			pbHS)

		return nil, PeMgrEnoMessage
	}

	if len(pbHS.NodeId) != config.NodeIDBytes {

		log.Debug("getHandshakeInbound:" +
			"invalid node identity length: %d",
				len(pbHS.NodeId))

		return nil, PeMgrEnoMessage
	}

	if *pbHS.ProtoNum > MaxProtocols {

		log.Debug("getHandshakeInbound:" +
			"too much protocols: %d",
			*pbHS.ProtoNum)

		return nil, PeMgrEnoMessage
	}

	if int(*pbHS.ProtoNum) != len(pbHS.Protocols) {

		log.Debug("getHandshakeInbound: " +
			"number of protocols mismathced, ProtoNum: %d, real: %d",
			int(*pbHS.ProtoNum), len(pbHS.Protocols))

		return nil, PeMgrEnoMessage
	}

	//
	// get handshake info to return
	//

	var ptrMsg = new(Handshake)
	copy(ptrMsg.Snid[:], pbHS.SubNetId)
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
	pbHandshakeMsg.SubNetId = append(pbHandshakeMsg.SubNetId, hs.Snid[:]...)
	pbHandshakeMsg.NodeId = append(pbHandshakeMsg.NodeId, hs.NodeId[:]...)
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

	if upkg.signOutbound(inst, pbHandshakeMsg) != true {
		log.Debug("putHandshakeOutbound: signOutbound failed")
		return PeMgrEnoSign
	}

	pbMsg := new(pb.P2PMessage)
	pbMsg.Mid = new(pb.MessageId)
	*pbMsg.Mid = pb.MessageId_MID_HANDSHAKE
	pbMsg.Handshake = pbHandshakeMsg

	payload, err1 := pbMsg.Marshal()
	if err1 != nil {

		log.Debug("putHandshakeOutbound:" +
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
	pbPkg.ExtMid = new(pb.MessageId)
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(len(payload))
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// send package to peer, notice need to encode here directly, it would be
	// done in calling of gw.WriteMsg, see bellow.
	//

	if inst.hto != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.hto))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	w := inst.conn.(io.Writer)
	inst.iow = ggio.NewDelimitedWriter(w)

	if err := inst.iow.WriteMsg(pbPkg); err != nil {

		if debug__ {
			log.Debug("putHandshakeOutbound: "+
				"Write failed, err: %s",
				err.Error())
		}

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

	*pbPing.Mid = MID_PING
	pbPing.Ping.Extra = append(pbPing.Ping.Extra, ping.Extra...)

	payload, err := pbPing.Marshal()

	if len(payload) == 0 || err != nil {
		log.Debug("ping: empty payload")
		return PeMgrEnoMessage
	}

	//
	// setup package for payload
	//

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = pb.ProtocolId_PID_P2P
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// Set deadline
	//

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	//
	// write package to peer
	//

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		if debug__ {
			log.Debug("ping:"+
				"Write failed, err: %s",
				err.Error())
		}

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Pong
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

	*pbPong.Mid = MID_PONG
	pbPong.Pong.Extra = append(pbPong.Pong.Extra, pong.Extra...)

	payload, err := pbPong.Marshal()

	if len(payload) == 0 || err != nil {
		log.Debug("pong: empty payload")
		return PeMgrEnoMessage
	}

	//
	// setup package for payload
	//

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = pb.ProtocolId_PID_P2P
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	//
	// Set deadline
	//

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	//
	// write package to peer
	//

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		if debug__ {
			log.Debug("pong:"+
				"Write failed, err: %s",
				err.Error())
		}

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Send user packege
//

func (upkg *P2pPackage)SendPackage(inst *peerInstance) PeMgrErrno {

	if inst == nil {
		log.Debug("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	//
	// Setup the protobuf "package"
	//

	pbPkg := new(pb.P2PPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = pb.ProtocolId(upkg.Pid)
	pbPkg.ExtMid = new(pb.MessageId)
	*pbPkg.ExtMid = pb.MessageId(upkg.Mid)
	pbPkg.ExtKey = upkg.Key
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(upkg.PayloadLength)
	pbPkg.Payload = append(pbPkg.Payload, upkg.Payload...)

	//
	// Set deadline
	//

	if inst.ato != time.Duration(0) {
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

		if debug__ {
			log.Debug("SendPackage: "+
				"Write failed, err: %s",
				err.Error())
		}

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Receive user package
//
func (upkg *P2pPackage)RecvPackage(inst *peerInstance) PeMgrErrno {

	if inst == nil {
		log.Debug("RecvPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	//
	// Setup the reader
	//

	if inst.ato != time.Duration(0) {
		inst.conn.SetReadDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetReadDeadline(time.Time{})
	}

	//
	// New protobuf "package" and read peer into it
	//

	pkg := new(pb.P2PPackage)

	if err := inst.ior.ReadMsg(pkg); err != nil {

		if debug__ {
			log.Debug("RecvPackage: "+
				"ReadMsg faied, err: %s",
				err.Error())
		}

		return PeMgrEnoOs
	}

	//
	// setup upkg(user package) with type P2pPackage from pb.P2pPackage
	// object obtained above.
	//

	upkg.Pid			= uint32(*pkg.Pid)
	upkg.Mid			= uint32(*pkg.ExtMid)
	upkg.Key			= append(upkg.Key[0:], pkg.ExtKey...)
	upkg.PayloadLength	= *pkg.PayloadLength
	upkg.Payload		= append(upkg.Payload, pkg.Payload ...)

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
		log.Debug("GetMessage: invalid parameter")
		return PeMgrEnoParameter
	}

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(upkg.Payload); err != nil {

		log.Debug("GetMessage:" +
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

		log.Debug("GetMessage: " +
			"unknown message identity: %d",
			pmsg.Mid)

		return PeMgrEnoMessage
	}

	return PeMgrEnoNone
}

func (upkg *P2pPackage)signOutbound(inst *peerInstance, hs *pb.P2PMessage_Handshake) bool {
	cfg := inst.sdl.SchGetP2pConfig()
	r, s, err := config.P2pSign(cfg.PrivateKey, hs.NodeId)
	if err != nil {
		log.Debug("signOutbound: P2pSign failed, error: %s", err.Error())
		return false
	}
	hs.SignR = new(int32)
	*hs.SignR = int32(config.P2pSignBigInt(r))
	hs.R = append(hs.R, config.P2pBigIntAbs2Bytes(r)...)
	hs.SignS = new(int32)
	*hs.SignS = int32(config.P2pSignBigInt(s))
	hs.S = append(hs.S, config.P2pBigIntAbs2Bytes(s)...)
	return true
}

func (upkg *P2pPackage)verifyInbound(inst *peerInstance, hs *pb.P2PMessage_Handshake) bool {
	pubKey := config.P2pNodeId2Pubkey(hs.NodeId)
	r := config.P2pBigInt(int(*hs.SignR), hs.R)
	s := config.P2pBigInt(int(*hs.SignS), hs.S)
	return config.P2pVerify(pubKey, hs.NodeId, r, s)
}
