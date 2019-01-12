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
	"fmt"
	ggio "github.com/gogo/protobuf/io"
	config "github.com/yeeco/gyee/p2p/config"
	pb "github.com/yeeco/gyee/p2p/peer/pb"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)

//
// debug
//
type tcpmsgLogger struct {
	debug__		bool
}

var tcpmsgLog = tcpmsgLogger {
	debug__:	true,
}

func (log tcpmsgLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

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
	MID_CHKK		= pb.MessageId_MID_CHKK			// check key
	MID_RPTK		= pb.MessageId_MID_RPTK			// report key

	// external MID for PID_EXT
	MID_TX			= pb.MessageId_MID_TX
	MID_EVENT		= pb.MessageId_MID_EVENT
	MID_BLOCKHEADER	= pb.MessageId_MID_BLOCKHEADER
	MID_BLOCK		= pb.MessageId_MID_BLOCK

	// invalid MID
	MID_INVALID		= pb.MessageId_MID_INVALID
)

//
// Key status
//
const (
	KS_NOTEXIST		= pb.KeyStatus_KS_NOTEXIST
	KS_EXIST		= pb.KeyStatus_KS_EXIST
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
// Check key
//
type CheckKey struct {
	Key			[]byte		// key
	Extra		[]byte		// extra info
}

//
// Report key
//
type ReportKey struct {
	Key			[]byte		// key
	Status		int32		// key status
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
// Message for internal TCP message
//
type P2pMessage struct {
	Mid				uint32		// message identity
	Ping			*Pingpong	// ping message
	Pong			*Pingpong	// pong message
	Handshake		*Handshake	// handshake message
	Chkk			*CheckKey	// check key message
	Rptk			*ReportKey	// report key message
}

//
// Message for external TCP message
//
type ExtMessage struct {
	Mid				uint32		// message identity
	Chkk			*CheckKey	// check key message
	Rptk			*ReportKey	// report key message
}

//
// Read handshake message from inbound peer
//
func (upkg *P2pPackage)getHandshakeInbound(inst *PeerInstance) (*Handshake, PeMgrErrno) {

	if inst.hto != 0 {
		inst.conn.SetReadDeadline(time.Now().Add(inst.hto))
	} else {
		inst.conn.SetReadDeadline(time.Time{})
	}

	r := inst.conn.(io.Reader)
	inst.ior = ggio.NewDelimitedReader(r, inst.maxPkgSize)
	pkg := new(pb.P2PPackage)

	if err := inst.ior.ReadMsg(pkg); err != nil {

		tcpmsgLog.Debug("getHandshakeInbound: "+
			"ReadMsg faied, err: %s",
			err.Error())

		return nil, PeMgrEnoOs
	}

	if *pkg.Pid != PID_P2P {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"not a p2p package, pid: %d",
			*pkg.Pid)

		return nil, PeMgrEnoMessage
	}

	if *pkg.PayloadLength <= 0 {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"invalid payload length: %d",
			*pkg.PayloadLength)

		return nil, PeMgrEnoMessage
	}

	if len(pkg.Payload) != int(*pkg.PayloadLength) {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"payload length mismatched, PlLen: %d, real: %d",
			*pkg.PayloadLength, len(pkg.Payload))

		return nil, PeMgrEnoMessage
	}

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(pkg.Payload); err != nil {

		tcpmsgLog.Debug("getHandshakeInbound:" +
			"Unmarshal failed, err: %s",
			err.Error())

		return nil, PeMgrEnoMessage
	}

	if *pbMsg.Mid != MID_HANDSHAKE {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"it's not a handshake message, mid: %d",
			*pbMsg.Mid)

		return nil, PeMgrEnoMessage
	}

	pbHS := pbMsg.Handshake

	if upkg.verifyInbound(inst, pbHS) != true {
		tcpmsgLog.Debug("putHandshakeOutbound: verifyInbound failed")
		return nil, PeMgrEnoVerify
	}

	if pbHS == nil {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"invalid handshake message pointer: %p",
			pbHS)

		return nil, PeMgrEnoMessage
	}

	if len(pbHS.NodeId) != config.NodeIDBytes {

		tcpmsgLog.Debug("getHandshakeInbound:" +
			"invalid node identity length: %d",
				len(pbHS.NodeId))

		return nil, PeMgrEnoMessage
	}

	if *pbHS.ProtoNum > MaxProtocols {

		tcpmsgLog.Debug("getHandshakeInbound:" +
			"too much protocols: %d",
			*pbHS.ProtoNum)

		return nil, PeMgrEnoMessage
	}

	if int(*pbHS.ProtoNum) != len(pbHS.Protocols) {

		tcpmsgLog.Debug("getHandshakeInbound: " +
			"number of protocols mismathced, ProtoNum: %d, real: %d",
			int(*pbHS.ProtoNum), len(pbHS.Protocols))

		return nil, PeMgrEnoMessage
	}

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
func (upkg *P2pPackage)putHandshakeOutbound(inst *PeerInstance, hs *Handshake) PeMgrErrno {

	pbHandshakeMsg := new(pb.P2PMessage_Handshake)
	pbHandshakeMsg.SubNetId = append(pbHandshakeMsg.SubNetId, hs.Snid[:]...)
	pbHandshakeMsg.NodeId = append(pbHandshakeMsg.NodeId, hs.NodeId[:]...)
	pbHandshakeMsg.IP = append(pbHandshakeMsg.IP, hs.IP...)
	pbHandshakeMsg.TCP = &hs.TCP
	pbHandshakeMsg.UDP = &hs.UDP
	pbHandshakeMsg.ProtoNum = &hs.ProtoNum
	pbHandshakeMsg.Protocols = make([]*pb.P2PMessage_Protocol, *pbHandshakeMsg.ProtoNum)

	for i, p := range hs.Protocols {
		pbProto := new(pb.P2PMessage_Protocol)
		pbHandshakeMsg.Protocols[i] = pbProto
		pbProto.Pid = new(pb.ProtocolId)
		*pbProto.Pid = PID_P2P
		pbProto.Ver = append(pbProto.Ver, p.Ver[:]...)
	}

	if upkg.signOutbound(inst, pbHandshakeMsg) != true {
		tcpmsgLog.Debug("putHandshakeOutbound: signOutbound failed")
		return PeMgrEnoSign
	}

	pbMsg := new(pb.P2PMessage)
	pbMsg.Mid = new(pb.MessageId)
	*pbMsg.Mid = pb.MessageId_MID_HANDSHAKE
	pbMsg.Handshake = pbHandshakeMsg

	payload, err1 := pbMsg.Marshal()
	if err1 != nil {

		tcpmsgLog.Debug("putHandshakeOutbound:" +
			"Marshal failed, err: %s",
			err1.Error())

		return PeMgrEnoMessage
	}

	pbPkg := new(pb.P2PPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = PID_P2P
	pbPkg.ExtMid = new(pb.MessageId)
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(len(payload))
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	if inst.hto != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.hto))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	w := inst.conn.(io.Writer)
	inst.iow = ggio.NewDelimitedWriter(w)

	if err := inst.iow.WriteMsg(pbPkg); err != nil {

		tcpmsgLog.Debug("putHandshakeOutbound: "+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Ping
//
func (upkg *P2pPackage)ping(inst *PeerInstance, ping *Pingpong) PeMgrErrno {

	pbPing := pb.P2PMessage{
		Mid: 		new(pb.MessageId),
		Ping:		&pb.P2PMessage_Ping{
			Seq:	&ping.Seq,
			Extra:	make([]byte, 0),
		},
	}

	*pbPing.Mid = MID_PING
	pbPing.Ping.Extra = append(pbPing.Ping.Extra, ping.Extra...)

	payload, err := pbPing.Marshal()

	if len(payload) == 0 || err != nil {
		tcpmsgLog.Debug("ping: empty payload")
		return PeMgrEnoMessage
	}

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = PID_P2P
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		tcpmsgLog.Debug("ping:"+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Pong
//
func (upkg *P2pPackage)pong(inst *PeerInstance, pong *Pingpong) PeMgrErrno {

	pbPong := pb.P2PMessage{
		Mid: 		new(pb.MessageId),
		Pong:		&pb.P2PMessage_Pong{
			Seq:	&pong.Seq,
			Extra:	make([]byte, 0),
		},
	}

	*pbPong.Mid = MID_PONG
	pbPong.Pong.Extra = append(pbPong.Pong.Extra, pong.Extra...)

	payload, err := pbPong.Marshal()

	if len(payload) == 0 || err != nil {
		tcpmsgLog.Debug("pong: empty payload")
		return PeMgrEnoMessage
	}

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = PID_P2P
	*pbPkg.ExtMid = MID_INVALID
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		tcpmsgLog.Debug("pong:"+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Check key
//
func (upkg *P2pPackage)CheckKey(inst *PeerInstance, chkk *CheckKey) PeMgrErrno {

	pbChkk := pb.ExtMessage{
		Mid: 		new(pb.MessageId),
		CheckKey:	&pb.ExtMessage_CheckKey {
			Extra:	make([]byte, 0),
		},
	}

	*pbChkk.Mid = MID_CHKK
	pbChkk.CheckKey.Extra = append(pbChkk.CheckKey.Extra, chkk.Extra...)

	payload, err := pbChkk.Marshal()

	if len(payload) == 0 || err != nil {
		tcpmsgLog.Debug("chkk: empty payload")
		return PeMgrEnoMessage
	}

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		ExtKey:			make([]byte, 0),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = PID_EXT
	*pbPkg.ExtMid = MID_CHKK
	pbPkg.ExtKey = append(pbPkg.ExtKey, chkk.Key...)
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		tcpmsgLog.Debug("chkk:"+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Report key
//
func (upkg *P2pPackage)ReportKey(inst *PeerInstance, rptk *ReportKey) PeMgrErrno {
	
	pbRptk := pb.ExtMessage{
		Mid: 		new(pb.MessageId),
		ReportKey:	&pb.ExtMessage_ReportKey {
			Status:	new(pb.KeyStatus),
			Extra:	make([]byte, 0),
		},
	}

	*pbRptk.Mid = MID_RPTK
	*pbRptk.ReportKey.Status = pb.KeyStatus(rptk.Status)
	pbRptk.ReportKey.Extra = append(pbRptk.ReportKey.Extra, rptk.Extra...)

	payload, err := pbRptk.Marshal()

	if len(payload) == 0 || err != nil {
		tcpmsgLog.Debug("rptk: empty payload")
		return PeMgrEnoMessage
	}

	pbPkg := pb.P2PPackage {
		Pid:			new(pb.ProtocolId),
		ExtMid:			new(pb.MessageId),
		PayloadLength:	new(uint32),
		Payload:		make([]byte, 0),
	}

	*pbPkg.PayloadLength = uint32(len(payload))
	*pbPkg.Pid = PID_EXT
	*pbPkg.ExtMid = MID_RPTK
	pbPkg.ExtKey = append(pbPkg.ExtKey, rptk.Key...)
	pbPkg.Payload = append(pbPkg.Payload, payload...)

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	if err := inst.iow.WriteMsg(&pbPkg); err != nil {

		tcpmsgLog.Debug("rptk:"+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Send user packege
//

func (upkg *P2pPackage)SendPackage(inst *PeerInstance) PeMgrErrno {

	if inst == nil {
		tcpmsgLog.Debug("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	pbPkg := new(pb.P2PPackage)
	pbPkg.Pid = new(pb.ProtocolId)
	*pbPkg.Pid = pb.ProtocolId(upkg.Pid)
	pbPkg.ExtMid = new(pb.MessageId)
	*pbPkg.ExtMid = pb.MessageId(upkg.Mid)
	pbPkg.ExtKey = upkg.Key
	pbPkg.PayloadLength = new(uint32)
	*pbPkg.PayloadLength = uint32(upkg.PayloadLength)
	pbPkg.Payload = append(pbPkg.Payload, upkg.Payload...)

	if inst.ato != time.Duration(0) {
		inst.conn.SetWriteDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetWriteDeadline(time.Time{})
	}

	if err := inst.iow.WriteMsg(pbPkg); err != nil {

		tcpmsgLog.Debug("SendPackage: "+
			"Write failed, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

	return PeMgrEnoNone
}

//
// Receive user package
//
func (upkg *P2pPackage)RecvPackage(inst *PeerInstance) PeMgrErrno {

	if inst == nil {
		tcpmsgLog.Debug("RecvPackage: invalid parameter")
		return PeMgrEnoParameter
	}

	if inst.ato != time.Duration(0) {
		inst.conn.SetReadDeadline(time.Now().Add(inst.ato))
	} else {
		inst.conn.SetReadDeadline(time.Time{})
	}

	pkg := new(pb.P2PPackage)

	if err := inst.ior.ReadMsg(pkg); err != nil {

		tcpmsgLog.Debug("RecvPackage: "+
			"ReadMsg faied, err: %s",
			err.Error())

		return PeMgrEnoOs
	}

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

	if pmsg == nil {
		tcpmsgLog.Debug("GetMessage: invalid parameter")
		return PeMgrEnoParameter
	}

	pbMsg := new(pb.P2PMessage)

	if err := pbMsg.Unmarshal(upkg.Payload); err != nil {

		tcpmsgLog.Debug("GetMessage:" +
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

		tcpmsgLog.Debug("GetMessage: " +
			"unknown message identity: %d",
			pmsg.Mid)

		return PeMgrEnoMessage
	}

	return PeMgrEnoNone
}

func (upkg *P2pPackage)GetExtMessage(extMsg *ExtMessage) PeMgrErrno {
	// Notice: the underlying p2p would not try really to decode the application user's
	// message, except those "CheckKey" and "ReportKey" messages, which are applied for
	// the deduplication function implemented currently in p2p.
	if extMsg == nil {
		tcpmsgLog.Debug("GetExtMessage: invalid parameter")
		return PeMgrEnoParameter
	}

	pbMsg := new(pb.ExtMessage)

	if err := pbMsg.Unmarshal(upkg.Payload); err != nil {

		tcpmsgLog.Debug("GetExtMessage:" +
			"Unmarshal failed, err: %s",
			err.Error())

		return PeMgrEnoMessage
	}

	extMsg.Mid = uint32(*pbMsg.Mid)
	extMsg.Chkk = nil
	extMsg.Rptk = nil

	if extMsg.Mid == uint32(MID_CHKK) {

		chkk := new(CheckKey)
		chkk.Key = append(chkk.Key, upkg.Key...)
		extMsg.Chkk = chkk

	} else if extMsg.Mid == uint32(MID_RPTK) {

		rptk := new(ReportKey)
		rptk.Key = append(rptk.Key, upkg.Key...)
		rptk.Status = int32(*pbMsg.ReportKey.Status)
		extMsg.Rptk = rptk

	} else {

		tcpmsgLog.Debug("GetExtMessage: " +
			"unknown message identity: %d",
			extMsg.Mid)

		return PeMgrEnoMessage
	}

	return PeMgrEnoNone
}

func (upkg *P2pPackage)signOutbound(inst *PeerInstance, hs *pb.P2PMessage_Handshake) bool {
	r, s, err := config.P2pSign(&inst.priKey, hs.NodeId)
	if err != nil {
		tcpmsgLog.Debug("signOutbound: P2pSign failed, error: %s", err.Error())
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

func (upkg *P2pPackage)verifyInbound(inst *PeerInstance, hs *pb.P2PMessage_Handshake) bool {
	pubKey := config.P2pNodeId2Pubkey(hs.NodeId)
	r := config.P2pBigInt(int(*hs.SignR), hs.R)
	s := config.P2pBigInt(int(*hs.SignS), hs.S)
	return config.P2pVerify(pubKey, hs.NodeId, r, s)
}

func (upkg *P2pPackage)String() string {
	if !tcpmsgLog.debug__ {
		return ""
	} else {
		strPkg := fmt.Sprintf("P2pPackage: Key: %x\n", upkg.Key)
		strPkg += fmt.Sprintf("\tPid: %d, Mid: %d, PayloadLength: %d",
			upkg.Pid, upkg.Mid, upkg.PayloadLength)
		return strPkg
	}
}

func (ck *CheckKey)String() string {
	return fmt.Sprintf("CheckKey: key: %x", ck.Key)
}

func (rk *ReportKey)String() string {
	return fmt.Sprintf("ReportKey: status: %d, key: %x", rk.Status, rk.Key)
}

func (upkg *P2pPackage)DebugPeerPackage() {
	if tcpmsgLog.debug__ {
		tcpmsgLog.Debug("DebugPeerPackage: %s", upkg.String())
	}
}