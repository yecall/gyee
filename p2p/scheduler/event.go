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

//
// Notice： all events for those tasks scheduled by the scheduler module
// should be defined in this file, and messages for inter-module actions
// should be defined here also, while those messages needed just for inner
// module actions please not defined here. This file is shred by all modules
// based on the shceduler, see it please.
//

package scheduler

import (
	"crypto/ecdsa"
	"net"
	"time"

	"github.com/yeeco/gyee/p2p/config"
	um "github.com/yeeco/gyee/p2p/discover/udpmsg"
)

//
// Null event: nothing;
// Poweron: scheduler just started;
// Poweroff: scheduler will be stopped.
//
const (
	EvSchNull     = 0
	EvSchPoweron  = EvSchNull + 1
	EvSchPoweroff = EvSchNull + 2
	EvSchDone     = EvSchNull + 3
)

//
// Message for task done
//
type MsgTaskDone struct {
	why SchErrno // why done
}

//
// Scheduler internal event
//
const (
	EvSchBase        = 10
	EvSchTaskCreated = EvSchBase + 1
)

//
// Timer event: for an user task, it could hold most timer number as schMaxTaskTimer,
// and then, when timer n which in [0,schMaxTaskTimer-1] is expired, message with event
// id as n would be sent to user task, means schMessage.id would be set to n.
//
const EvTimerBase = 1000

//
// Chain shell manager event
//
const (
	EvShellBase              = 1100
	EvShellPeerActiveInd     = EvShellBase + 1
	EvShellPeerCloseCfm      = EvShellBase + 2
	EvShellPeerCloseInd      = EvShellBase + 3
	EvShellPeerAskToCloseInd = EvShellBase + 4
	EvShellReconfigReq       = EvShellBase + 5
	EvShellBroadcastReq      = EvShellBase + 6
	EvShellSubnetUpdateReq   = EvShellBase + 7
	EvShellGetChainInfoReq   = EvShellBase + 8
	EvShellGetChainInfoRsp   = EvShellBase + 9
)

// EvShellPeerActiveInd
type MsgShellPeerActiveInd struct {
	TxChan   interface{} // channel for packages sending
	RxChan   interface{} // channel for packages received
	PeerInfo interface{} // handshake info about peer
	PeerInst interface{} // peer instance
}

// EvShellPeerCloseCfm
type MsgShellPeerCloseCfm struct {
	Result int                 // result
	Dir    int                 // direction
	Snid   config.SubNetworkID // sub network identity
	PeerId config.NodeID       // target node
}

// EvShellPeerCloseInd
type MsgShellPeerCloseInd struct {
	Cause  int                 // tell why it's closed
	Dir    int                 // direction
	Snid   config.SubNetworkID // sub network identity
	PeerId config.NodeID       // target node
}

// EvShellPeerAskToCloseInd
type MsgShellPeerAskToCloseInd struct {
	Snid   config.SubNetworkID // sub network identity
	PeerId config.NodeID       // target node
	Dir    int                 // direction
	Why    interface{}         // tell why it's closed
}

// EvShellReconfigReq
type SingleSubnetDescriptor struct {
	SubNetKey          ecdsa.PrivateKey    // key for sub-node
	SubNetNode         config.Node         // sub-node identity
	SubNetMaxPeers     int                 // max peers would be
	SubNetMaxOutbounds int                 // max concurrency outbounds
	SubNetMaxInBounds  int                 // max concurrency inbounds
	SubNetId           config.SubNetworkID // sub network identity
}

type MsgShellReconfigReq struct {
	SnidAdd  []SingleSubnetDescriptor // common sub network identities to be added
	SnidDel  []config.SubNetworkID    // common sub network identities to be deleted
	MaskBits int                      // mask bits for subnet identity
}

// EvShellBroadcastReq, see tcpmsg.proto please.
const (
	MSBR_MT_TX   = 3 // tx type
	MSBR_MT_EV   = 4 // event type
	MSBR_MT_BLKH = 5 // block header type
	MSBR_MT_BLK  = 6 // block type
)

type MsgShellBroadcastReq struct {
	MsgType   int             // message type, see above constants
	From      string          // from
	Key       []byte          // key
	Data      []byte          // payload bytes
	LocalSnid []config.NodeID // local sut network identity
	Exclude   *config.NodeID  // node to be excluded
}

// EvShellGetChainInfoReq
type MsgShellGetChainInfoReq struct {
	Seq			uint64		// sequence
	Kind		string		// kind
	Key			[]byte		// key
}

// EvShellGetChainInfoRsp
type MsgShellGetChainInfoRsp struct {
	Peer		interface{}	// peer info pointer
	Seq			uint64		// sequence
	Kind		string		// kind
	Key			[]byte		// key
	Data		[]byte		// data
}

//
// Table manager event
//
const (
	TabRefreshTimerId  = 0
	TabPingpongTimerId = 1
	TabFindNodeTimerId = 2
)

const (
	EvTabMgrBase       = 1200
	EvTabRefreshTimer  = EvTimerBase + TabRefreshTimerId
	EvTabPingpongTimer = EvTimerBase + TabPingpongTimerId
	EvTabFindNodeTimer = EvTimerBase + TabFindNodeTimerId
	EvTabRefreshReq    = EvTabMgrBase + 1
	EvTabRefreshRsp    = EvTabMgrBase + 2
)

// EvTabRefreshReq
type MsgTabRefreshReq struct {
	Snid    config.SubNetworkID // sub network identity
	Include []*config.NodeID    // wanted, it can be an advice for discover
	Exclude []*config.NodeID    // filter out from response if any
}

// EvTabRefreshRsp
type MsgTabRefreshRsp struct {
	Snid  config.SubNetworkID // sub network identity
	Nodes []*config.Node      // nodes found
}

//
// NodeDb cleaner event
//
const NdbCleanerTimerId = 1
const (
	EvNdbCleanerTimer = EvTimerBase + NdbCleanerTimerId
)

//
// Discover manager event
//
const (
	EvDcvMgrBase     = 1300
	EvDcvFindNodeReq = EvDcvMgrBase + 1
	EvDcvFindNodeRsp = EvDcvMgrBase + 2
	EvDcvReconfigReq = EvDcvMgrBase + 3
)

// EvDcvFindNodeReq
type MsgDcvFindNodeReq struct {
	Snid    config.SubNetworkID // sub network identity
	More    int                 // number of more peers needed
	Include []*config.NodeID    // wanted, it can be an advice for discover
	Exclude []*config.NodeID    // filter out from response if any
}

// EvDcvFindNodeRsp
type MsgDcvFindNodeRsp struct {
	Snid  config.SubNetworkID // sub network identity
	Nodes []*config.Node      // nodes found
}

// EvDcvReconfigReq
type MsgDcvReconfigReq struct {
	DelList map[config.SubNetworkID]interface{} // sub networks to be deleted
	AddList map[config.SubNetworkID]interface{} // sub networks to be added
}

//
// Neighbor lookup on Udp event
//
const NblFindNodeTimerId = 0
const NblPingpongTimerId = 1
const (
	EvNblUdpBase       = 1400
	EvNblFindNodeTimer = EvTimerBase + NblFindNodeTimerId
	EvNblPingpongTimer = EvTimerBase + NblPingpongTimerId
	EvNblFindNodeReq   = EvNblUdpBase + 1
	EvNblFindNodeRsp   = EvNblUdpBase + 2
	EvNblPingpongReq   = EvNblUdpBase + 3
	EvNblPingpongRsp   = EvNblUdpBase + 4
	EvNblPingedInd     = EvNblUdpBase + 5
	EvNblPongedInd     = EvNblUdpBase + 6
	EvNblQueriedInd    = EvNblUdpBase + 7
	EvNblCleanMapReq   = EvNblUdpBase + 8
)

// EvNblFindNodeRsp message
type NblFindNodeRsp struct {
	Result    int           // result, 0: ok, others: errno
	FindNode  *um.FindNode  // FindNode message from table task
	Neighbors *um.Neighbors // Neighbors message from peer node
}

// EvNblPingpongrRsp message
type NblPingRsp struct {
	Result int      // result, 0: ok, others: errno
	Ping   *um.Ping // Ping message from table task
	Pong   *um.Pong // Pong message from peer
}

// EvNblPingedInd
type NblPingedInd struct {
	Ping *um.Ping // ping from remote node
}

// EvNblPongedInd
type NblPongedInd struct {
	Pong *um.Pong // pong from remote node
}

// EvNblQueriedInd
type NblQueriedInd struct {
	FindNode *um.FindNode // findnode from remote node
}

//
// Neighbor listenner event
//
const (
	EvNblListennerBase = 1500
	EvNblMsgInd        = EvNblListennerBase + 1
	EvNblStart         = EvNblListennerBase + 2
	EvNblStop          = EvNblListennerBase + 3
	EvNblDataReq       = EvNblListennerBase + 4
)

// EvNblDataReq
type NblDataReq struct {
	Payload []byte       // payload
	TgtAddr *net.UDPAddr // target address
}

//
// Peer manager event
//
const (
	EvPeerMgrBase = 1600
)

//
// Peer listerner event
//
const (
	EvPeerLsnBase          = 1700
	EvPeLsnConnAcceptedInd = EvPeerLsnBase + 1
	EvPeLsnStartReq        = EvPeerLsnBase + 2
	EvPeLsnStopReq         = EvPeerLsnBase + 3
	EvPeLsnRestart         = EvPeerLsnBase
)

// Peer connection establishment event
const (
	PePingpongTimerId       = 0
	PeDcvFindNodeTimerId    = 1
	PeMinOcrCleanupTimerId  = 2
	PeConflictAccessTimerId = 3
	PeReconfigTimerId       = 4
)

const (
	EvPeerEstBase           = 1800
	EvPePingpongTimer       = EvTimerBase + PePingpongTimerId
	EvPeDcvFindNodeTimer    = EvTimerBase + PeDcvFindNodeTimerId
	EvPeOcrCleanupTimer     = EvTimerBase + PeMinOcrCleanupTimerId
	EvPeConflictAccessTimer = EvTimerBase + PeConflictAccessTimerId
	EvPeReconfigTimer       = EvTimerBase + PeReconfigTimerId
	EvPeConnOutReq          = EvPeerEstBase + 1
	EvPeConnOutRsp          = EvPeerEstBase + 2
	EvPeHandshakeReq        = EvPeerEstBase + 3
	EvPeHandshakeRsp        = EvPeerEstBase + 4
	EvPePingpongReq         = EvPeerEstBase + 5
	EvPeCloseReq            = EvPeerEstBase + 7
	EvPeCloseCfm            = EvPeerEstBase + 8
	EvPeCloseInd            = EvPeerEstBase + 9
	EvPeOutboundReq         = EvPeerEstBase + 10
	EvPeEstablishedInd      = EvPeerEstBase + 11
	EvPeMgrStartReq         = EvPeerEstBase + 12
	EvPeTxDataReq           = EvPeerEstBase + 13
	EvPeRxDataInd           = EvPeerEstBase + 14
)

// EvPeCloseReq
const (
	PEC_FOR_NONE         = "None"
	PEC_FOR_COMMAND      = "Command"
	PEC_FOR_SETDEADLINE  = "SetDeadline failed"
	PEC_FOR_PINGPONG     = "PeMgrEnoPingpongTh"
	PEC_FOR_RXERROR      = "RecvPackage"
	PEC_FOR_TXERROR      = "SendPackage"
	PEC_FOR_RECONFIG     = "Reconfig"
	PEC_FOR_RECONFIG_REQ = "ReconfigReq"
	PEC_FOR_BEASKEDTO    = "EvShellPeerAskToCloseInd"
)

type MsgPeCloseReq struct {
	Ptn  interface{}         // pointer to peer instance task if not nil
	Snid config.SubNetworkID // sub network identity
	Node config.Node         // peer node
	Dir  int                 // direction
	Why  interface{}         // cause
}

// EvPeTxDataReq
type MsgPeDataReq struct {
	SubNetId config.SubNetworkID // sub network identity
	PeerId   config.NodeID       // peer node identity
	Pkg      interface{}         // package pointer
}

//
// DHT manager event
//
const (
	EvDhtMgrBase           = 1900
	EvDhtMgrFindPeerReq    = EvDhtMgrBase + 1
	EvDhtMgrFindPeerRsp    = EvDhtQryMgrQueryResultInd
	EvDhtMgrPutProviderReq = EvDhtMgrBase + 3
	EvDhtMgrPutProviderRsp = EvDhtMgrBase + 4
	EvDhtMgrGetProviderReq = EvDhtMgrBase + 5
	EvDhtMgrGetProviderRsp = EvDhtMgrBase + 6
	EvDhtMgrPutValueReq    = EvDhtMgrBase + 7
	EvDhtMgrPutValueRsp    = EvDhtMgrBase + 8
	EvDhtMgrPutValueLocalRsp = EvDhtMgrBase + 9
	EvDhtMgrGetValueReq    = EvDhtMgrBase + 10
	EvDhtMgrGetValueRsp    = EvDhtMgrBase + 11
	EvDhtMgrGetValueBatchReq	= EvDhtMgrBase + 12
	EvDhtMgrQueryStopReq   = EvDhtMgrBase + 13
	EvDhtBlindConnectReq   = EvDhtMgrBase + 14
	EvDhtBlindConnectRsp   = EvDhtMgrBase + 15
)

// EvDhtMgrGetProviderReq
type MsgDhtMgrGetProviderReq struct {
	Key []byte // key wanted
}

// EvDhtMgrPutProviderRsp
type MsgDhtMgrGetProviderRsp struct {
	Eno  int            // result code
	Key  []byte         // key wanted
	Prds []*config.Node // providers
}

// EvDhtMgrPutValueReq
type MsgDhtMgrPutValueReq struct {
	Key      []byte        // key wanted
	Val      []byte        // value
	KeepTime time.Duration // duration for the value to be kept
}

// EvDhtMgrPutValueRsp
type MsgDhtMgrPutValueRsp struct {
	Eno   int            // result code
	Key   []byte         // key wanted
	Peers []*config.Node // extren peers where the value put beside local
}

// EvDhtMgrPutValueLocalRsp
type MsgDhtMgrPutValueLocalRsp struct {
	Eno   int            // result code
	Key   []byte         // key wanted
}

// EvDhtMgrGetValueReq
type MsgDhtMgrGetValueReq struct {
	Key []byte // key wanted
}

// EvDhtMgrGetValueRsp
type MsgDhtMgrGetValueRsp struct {
	Eno int    // result code
	Key []byte // key wanted
	Val []byte // value
}

// EvDhtMgrGetValueBatchReq
const DefaultDhtGetBatchTimeout = time.Second  * 128	// default batch get timeout
const MaxDhtGetBatchSize = 1024 * 16						// max keys can be
type MsgDhtMgrGetValueBatchReq struct {
	Keys	[][]byte		// batch keys
	ValCh	chan<-[]byte	// channel to output values
	Timeout	time.Duration	// timeout required specified by dht user module
}

// EvDhtBlindConnectReq
type MsgDhtBlindConnectReq struct {
	Peer *config.Node // peer to be connected
}

// EvDhtBlindConnectRsp
type MsgDhtBlindConnectRsp struct {
	Eno  int          // result code
	Peer *config.Node // peer to be connected
	Ptn  interface{}  // pointer to connection instance task
	Dir  int          // direction
}

//
// DHT listener manager event
//
const (
	EvDhtLsnMgrBase      = 2000
	EvDhtLsnMgrStartReq  = EvDhtLsnMgrBase + 1
	EvDhtLsnMgrStopReq   = EvDhtLsnMgrBase + 2
	EvDhtLsnMgrPauseReq  = EvDhtLsnMgrBase + 3
	EvDhtLsnMgrResumeReq = EvDhtLsnMgrBase + 4
	EvDhtLsnMgrDriveSelf = EvDhtLsnMgrBase + 5
	EvDhtLsnMgrAcceptInd = EvDhtLsnMgrBase + 6
	EvDhtLsnMgrStatusInd = EvDhtLsnMgrBase + 7
)

// EvDhtLsnMgrAcceptInd
type MsgDhtLsnMgrAcceptInd struct {
	Con net.Conn // connection accepted
}

// EvDhtLsnMgrStatusInd
type MsgDhtLsnMgrStatusInd struct {
	Status int // current listener manager status
}

//
// DHT connection manager event
//
const DhtConMgrMonitorTimerId = 1
const (
	EvDhtConMgrBase             = 2100
	EvDhtConMgrMonitorTimer     = EvTimerBase + DhtConMgrMonitorTimerId
	EvDhtConMgrConnectReq       = EvDhtConMgrBase + 1
	EvDhtConMgrConnectRsp       = EvDhtConMgrBase + 2
	EvDhtConMgrSendReq          = EvDhtConMgrBase + 3
	EvDhtConMgrSendCfm          = EvDhtConMgrBase + 4
	EvDhtConMgrCloseReq         = EvDhtConMgrBase + 5
	EvDhtConMgrCloseRsp         = EvDhtConMgrBase + 6
	EvDhtConMgrPubAddrSwitchBeg = EvDhtConMgrBase + 7
	EvDhtConMgrPubAddrSwitchEnd = EvDhtConMgrBase + 8
	EvDhtConMgrBootstrapReq     = EvDhtConMgrBase + 9
)

// EvDhtConMgrConnectReq
type MsgDhtConMgrConnectReq struct {
	Task    interface{}  // pointer to owner task node
	Name    string       // owner task name
	Peer    *config.Node // peer to be connected
	IsBlind bool         // is blind
}

// EvDhtConMgrConnectRsp
type MsgDhtConMgrConnectRsp struct {
	Eno  int          // result code
	Peer *config.Node // peer to be connected
	Dir  int          // direction
}

// EvDhtConMgrSendReq
type MsgDhtConMgrSendReq struct {
	Task    interface{}  // pointer to task node
	WaitRsp bool         // wait response from peer
	WaitMid int          // wait message identity
	WaitSeq int64        // wait message sequence number
	NeedCfm bool         // if confirm needed
	CfmInfo interface{}  // confirm owner with this
	Peer    *config.Node // peer where data sent to
	Data    interface{}  // data to be sent
}

// EvDhtConMgrSendCfm
type MsgDhtConMgrSendCfm struct {
	Eno     int          // result
	CfmInfo interface{}  // confirm owner with this
	Peer    *config.Node // peer where data sent to
}

// EvDhtConMgrCloseReq
type MsgDhtConMgrCloseReq struct {
	Task string       // owner task name
	Peer *config.Node // peer to be connected
	Dir  int          // instance direction
}

// EvDhtConMgrCloseRsp
type MsgDhtConMgrCloseRsp struct {
	Eno  int          // result code
	Peer *config.Node // peer to be connected
	Dir  int          // instance direction
}

// DHT connection instance event
const DhtConInstTxTimerId = 0
const (
	EvDhtConInstBase           = 2200
	EvDhtConInstTxTimer        = EvTimerBase + DhtConInstTxTimerId
	EvDhtConInstHandshakeReq   = EvDhtConInstBase + 1
	EvDhtConInstHandshakeRsp   = EvDhtConInstBase + 2
	EvDhtConInstTxDataReq      = EvDhtConInstBase + 3
	EvDhtConInstStatusInd      = EvDhtConInstBase + 4
	EvDhtConInstCloseReq       = EvDhtConInstBase + 5
	EvDhtConInstCloseRsp       = EvDhtConInstBase + 6
	EvDhtConInstGetProviderRsp = EvDhtConInstBase + 7
	EvDhtConInstGetValRsp      = EvDhtConInstBase + 8
	EvDhtConInstNeighbors      = EvDhtConInstBase + 9
	EvDhtConInstTxInd          = EvDhtConInstBase + 10
	EvDhtConInstStartupReq     = EvDhtConInstBase + 11
)

// EvDhtConInstHandshakeReq
type MsgDhtConInstHandshakeReq struct {
	DurHs time.Duration // timeout duration
}

// EvDhtConInstHandshakeRsp
type MsgDhtConInstHandshakeRsp struct {
	Eno    int           // result code
	Inst   interface{}   // pointer connection instance
	Peer   *config.Node  // peer
	Dir    int           // connection instance direction
	HsInfo interface{}   // handshake information
	Dur    time.Duration // duration for handshake
}

// EvDhtConInstTxDataReq
type MsgDhtConInstTxDataReq struct {
	Task    interface{} // owner task node pointer
	WaitRsp bool        // wait response from peer
	WaitMid int         // wait message identity
	WaitSeq int64       // wait message sequence number
	Payload interface{} // payload
}

// EvDhtConInstCloseReq
type MsgDhtConInstCloseReq struct {
	Peer *config.NodeID // peer identity
	Why  int            // why to close
}

// EvDhtConInstCloseRsp
type MsgDhtConInstCloseRsp struct {
	Peer *config.NodeID // peer identity
	Dir  int            // instance direction
}

// EvDhtConInstStatusInd
type MsgDhtConInstStatusInd struct {
	Peer   *config.NodeID // peer identity
	Dir    int            // instance direction
	Status int            // status
}

// EvDhtConInstGetProviderRsp
type MsgDhtConInstGetProviderRsp struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtConInstGetValRsp
type MsgDhtConInstGetValRsp struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtConInstTxInd
type MsgDhtConInstTxInd struct {
	Eno     int   // result code
	WaitMid int   // wait message identity
	WaitSeq int64 // wait message sequence number
}

// EvDhtConInstStartupReq
type MsgDhtConInstStartupReq struct {
	EnoCh chan int // channel for result
}

//
// DHT query manager event
//
const DhtQryMgrQcbTimerId = 0
const DhtQryMgrIcbTimerId = 1
const (
	EvDhtQryMgrBase             = 2300
	EvDhtQryMgrQcbTimer         = EvTimerBase + DhtQryMgrQcbTimerId
	EvDhtQryMgrIcbTimer         = EvTimerBase + DhtQryMgrIcbTimerId
	EvDhtQryMgrQueryStartReq    = EvDhtQryMgrBase + 1
	EvDhtQryMgrQueryStartRsp    = EvDhtQryMgrBase + 2
	EvDhtQryMgrQueryStopReq     = EvDhtQryMgrBase + 3
	EvDhtQryMgrQueryStopRsp     = EvDhtQryMgrBase + 4
	EvDhtQryMgrQueryResultInd   = EvDhtQryMgrBase + 5
	EvDhtQryMgrPubAddrSwitchInd = EvDhtQryMgrBase + 6
)

// EvDhtQryMgrQueryStartReq
type MsgDhtQryMgrQueryStartReq struct {
	Target  	config.DsKey // can be config.NodeID or [config.DhtKeyLength]byte as a key
	Msg     	interface{}  // original request which results this query
	ForWhat 	int          // find-node; get-provider; get-value; put-value; ...
	Seq     	int64        // sequence number
	Batch		bool		// if owned by batch operation
	BatchId		int			// batch operation identity
}

// EvDhtQryMgrQueryStartRsp
type MsgDhtQryMgrQueryStartRsp struct {
	ForWhat int			// what's for
	Target config.DsKey // target node identity
	Eno    int          // result code
}

// EvDhtQryMgrQueryStopReq
type MsgDhtQryMgrQueryStopReq struct {
	Target config.DsKey // target node identity
}

// EvDhtQryMgrQueryStopRsp
type MsgDhtQryMgrQueryStopRsp struct {
	Target config.DsKey // target node identity
	Eno    int          // result code
}

// EvDhtQryMgrQueryResultInd
type MsgDhtQryMgrQueryResultInd struct {
	Eno     int            // result code. notice: when timeout, closests reported
	ForWhat int            // what's the original query for: find-node; get-value; get-provider; put-value; put provider; ...
	Target  config.DsKey   // target or key to be looked up
	Peers   []*config.Node // peers list, if target got, it always be the first one
	Val     []byte         // value
	Prds    []*config.Node // providers
}

//
// DHT query instance event
//
const (
	EvDhtQryInstBase        = 2400
	EvDhtQryInstStartReq    = EvDhtQryInstBase + 1
	EvDhtQryInstResultInd   = EvDhtQryInstBase + 4
	EvDhtQryInstStatusInd   = EvDhtQryInstBase + 5
	EvDhtQryInstProtoMsgInd = EvDhtQryInstBase + 6
)

// EvDhtQryInstStopReq
type MsgDhtQryInstStopReq struct {
	Target config.DsKey  // target to be looked up
	Peer   config.NodeID // peer to be queried
	Eno    int           // why stop
}

// EvDhtQryInstStatusInd
type MsgDhtQryInstStatusInd struct {
	Target config.DsKey  // target node identity
	Peer   config.NodeID // peer to be queried
	Status int           // status
}

// EvDhtQryInstResultInd
type Provider struct {
	Key   []byte         // key
	Nodes []*config.Node // node
	Extra interface{}    // extra
}

type MsgDhtQryInstResultInd struct {
	From     config.Node    // the peer who tells us
	Target   config.DsKey   // target node identity
	Latency  time.Duration  // latency about response to request
	ForWhat  int            // what this indication for
	Peers    []*config.Node // neighbors of target for find-node
	Provider *Provider      // providers for get-provider
	Value    []byte         // value for get-value
	Pcs      []int          // peer connection status, see dht.conMgrPeerConnStat pls
}

// EvDhtQryInstStopRsp
type MsgDhtQryInstStopRsp struct {
	To     config.Node  // whom is queried by the instance
	Target config.DsKey // target node identity
}

// EvDhtQryInstProtoMsgInd
type MsgDhtQryInstProtoMsgInd struct {
	From    *config.Node // where data is sent from
	Msg     interface{}  // dht message pointer
	ForWhat int          // what this message for
}

//
// DHT route manager event
//
const DhtRutBootstrapTimerId = 0
const (
	EvDhtRutMgrBase            = 2500
	EvDhtRutBootstrapTimer     = EvTimerBase + DhtRutBootstrapTimerId
	EvDhtRutMgrNearestReq      = EvDhtRutMgrBase + 1
	EvDhtRutMgrNearestRsp      = EvDhtRutMgrBase + 2
	EvDhtRutMgrUpdateReq       = EvDhtRutMgrBase + 3
	EvDhtRutMgrNotificationInd = EvDhtRutMgrBase + 4
	EvDhtRutPeerRemovedInd     = EvDhtRutMgrBase + 5
	EvDhtRutMgrStopNotifyReq   = EvDhtRutMgrBase + 6
	EvDhtRutPingInd            = EvDhtRutMgrBase + 7
	EvDhtRutPongInd            = EvDhtRutMgrBase + 8
	EvDhtRutRefreshReq         = EvDhtRutMgrBase + 9
)

// EvDhtRutMgrNearestReq
type MsgDhtRutMgrNearestReq struct {
	Target  config.DsKey // could be config.NodeID or [config.DhtKeyLength]byte as of key
	Max     int          // max items returned could be
	NtfReq  bool         // ask for notification when route updated
	Task    interface{}  // task who loves the notification
	ForWhat int          // what the request for
	Msg     interface{}  // backup for original message
	Filter  interface{}  // filter function
}

// EvDhtRutMgrNearestRsp
type MsgDhtRutMgrNearestRsp struct {
	Eno     int          // result code
	ForWhat int          // what for
	Target  config.DsKey // target peer identity
	Peers   interface{}  // nearest nodes table
	Dists   interface{}  // distances of nearest nodes
	Pcs     interface{}  // peer connection status table
	Msg     interface{}  // backup for original request
}

// EvDhtRutMgrUpdateReq
type MsgDhtRutMgrUpdateReq struct {
	Why   int             // why to request to upadte
	Eno   int             // result code
	Seens []config.Node   // nodes seen
	Duras []time.Duration // durations/latencies about seen nodes
}

// EvDhtRutMgrNotificationInd
type MsgDhtRutMgrNotificationInd struct {
	Target config.DsKey // target peer identity
	Peers  interface{}  // nearest nodes table
	Dists  interface{}  // distances of nearest nodes
}

// EvDhtRutPeerRemovedInd
type MsgDhtRutPeerRemovedInd struct {
	Peer config.NodeID // target peer identity
}

// EvDhtRutMgrStopNotifyReq
type MsgDhtRutMgrStopNofiyReq struct {
	Task   interface{}  // owner task of the notifee registered
	Target config.DsKey // target peer identity
}

// EvDhtRutPingInd
type MsgDhtRutPingInd struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtRutPongInd
type MsgDhtRutPongInd struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

//
// DHT provider manager event
//
const DhtPrdMgrCleanupTimerId = 0
const (
	EvDhtPrdMgrBase           = 2600
	EvDhtPrdMgrCleanupTimer   = EvTimerBase + DhtPrdMgrCleanupTimerId
	EvDhtPrdMgrAddProviderReq = EvDhtPrdMgrBase + 1
	EvDhtPrdMgrAddProviderRsp = EvDhtPrdMgrBase + 2
	EvDhtPrdMgrPutProviderReq = EvDhtPrdMgrBase + 3
	EvDhtPrdMgrGetProviderReq = EvDhtPrdMgrBase + 4
)

// EvDhtPrdMgrAddProviderReq
type MsgDhtPrdMgrAddProviderReq struct {
	Key []byte      // key of what is provided
	Prd config.Node // provider node
}

// EvDhtPrdMgrAddProviderRsp
type MsgDhtPrdMgrAddProviderRsp struct {
	Key   []byte         // key of what is provided
	Eno   int            // result code
	Peers []*config.Node // peers list, if target got, it always be the first one
}

// EvDhtPrdMgrPutProviderReq
type MsgDhtPrdMgrPutProviderReq struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtPrdMgrPutProviderRsp
type MsgDhtPrdMgrPutProviderRsp struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtPrdMgrGetProviderReq
type MsgDhtPrdMgrGetProviderReq struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

//
// DHT data store manager event
//
const DhtDsMgrTickTimerId = 0
const DhtDsGetValueBatchTimerId = 1
const (
	EvDhtDsMgrBase      = 2700
	EvDhtDsMgrTickTimer = EvTimerBase + DhtDsMgrTickTimerId
	EvDhtDsGvbTimer	  = EvTimerBase + DhtDsGetValueBatchTimerId
	EvDhtDsMgrAddValReq = EvDhtDsMgrBase + 1
	EvDhtDsMgrPutValReq = EvDhtDsMgrBase + 2
	EvDhtDsMgrGetValReq = EvDhtDsMgrBase + 3
	EvDhtDsGvbStartReq  = EvDhtDsMgrBase + 4
	EvDhtDsGvbStopReq   = EvDhtDsMgrBase + 5
	EvDhtDsGvbStatusInd = EvDhtDsMgrBase + 6
)

// EvDhtDsGvbStartReq
type MsgDhtDsGvbStartReq struct {
	GvbId		int				// get-value-batch identity
	Keys		*[][]byte		// keys of batch
	Output		chan<-[]byte	// output channel for values got
}

// EvDhtDsGvbStopReq
type MsgDhtDsGvbStopReq struct {
	GvbId		int				// get-value-batch identity
}

// EvDhtDsGvbStatusInd
const (
	GVBS_NULL	= iota			// spare
	GVBS_WORKING				// in working
	GVBS_TERMED				// terminated (interrupted)
	GVBS_DONE					// ok done
)
type MsgDhtDsStatusInd struct {
	GvbId		int				// get-value-batch identitys
	Status		int				// current sutatus
	Getting		int				// number of getting
	Got			int				// number of got
	Failed		int				// number of failed
	Remain		int				// number of remain
}

// EvDhtDsMgrAddValReq
const Keep4Ever = time.Duration(-1)

type MsgDhtDsMgrAddValReq struct {
	Key []byte        // key
	Val []byte        // value
	KT  time.Duration // duration to keep this [key, val] pair
}

// EvDhtDsMgrPutValReq
type MsgDhtDsMgrPutValReq struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

// EvDhtDsMgrGetValReq
type MsgDhtDsMgrGetValReq struct {
	ConInst interface{} // connection instance who sent this meeage
	Msg     interface{} // the message pointer
}

//
// DHT shell manager event
//
const (
	EvDhtShellBase  = 2800
	EvDhtShEventInd = EvDhtShellBase + 1
)

//
// EvDhtShEventInd
//
type MsgDhtShEventInd struct {
	Evt int         // event indication type
	Msg interface{} // event body pointer
}

//
// NAT manager event
//
const NatMgrRefreshTimerId = 0
const NatMgrDebugTimerId = 1
const (
	EvNatMgrBase             = 2900
	EvNatRefreshTimer        = EvTimerBase + NatMgrRefreshTimerId
	EvNatDebugTimer          = EvTimerBase + NatMgrDebugTimerId
	EvNatMgrDiscoverReq      = EvNatMgrBase + 1
	EvNatMgrDiscoverRsp      = EvNatMgrBase + 2
	EvNatMgrMakeMapReq       = EvNatMgrBase + 3
	EvNatMgrMakeMapRsp       = EvNatMgrBase + 4
	EvNatMgrRemoveMapReq     = EvNatMgrBase + 5
	EvNatMgrRemoveMapRsp     = EvNatMgrBase + 6
	EvNatMgrGetPublicAddrReq = EvNatMgrBase + 7
	EvNatMgrGetPublicAddrRsp = EvNatMgrBase + 8
	EvNatMgrPubAddrUpdateInd = EvNatMgrBase + 9
	EvNatMgrReadyInd         = EvNatMgrBase + 10
	EvNatPubAddrSwitchInd    = EvNatMgrBase + 11
)

//EvNatMgrReadyInd
type MsgNatMgrReadyInd struct {
	NatType string // type: "pmp", "upnp", "none"
}

// EvNatMgrDiscoverReq
type MsgNatMgrDiscoverReq struct {
	NatType string // type: "pmp", "upnp", "none", "any"
	GwIp    net.IP // gateway ip address when "pmp" specified
}

// EvNatMgrDiscoverRsp
type MsgNatMgrDiscoverRsp struct {
	Result int // result
}

// EvNatMgrMakeMapReq
type MsgNatMgrMakeMapReq struct {
	Proto      string        // the prototcol, "tcp" or "udp"
	FromPort   int           // local port number be mapped
	ToPort     int           // public port number mapped to
	DurKeep    time.Duration // duration for map to be kept
	DurRefresh time.Duration // interval duration to refresh the map
}

// EvNatMgrMakeMapRsp
type MsgNatMgrMakeMapRsp struct {
	Result   int    // result
	Status   int    // map status
	Proto    string // protocol
	FromPort int    // local port number be mapped
	PubIp    net.IP // public address
	PubPort  int    // public port number
}

// EvNatMgrRemoveMapReq
type MsgNatMgrRemoveMapReq struct {
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
}

// EvNatMgrRemoveMapRsp
type MsgNatMgrRemoveMapRsp struct {
	Result   int    // result
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
}

// EvNatMgrGetPublicAddrReq
type MsgNatMgrGetPublicAddrReq struct {
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
}

// EvNatMgrGetPublicAddrRsp
type MsgNatMgrGetPublicAddrRsp struct {
	Result   int    // result
	Status   int    // map status
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
	PubIp    net.IP // public address
	PubPort  int    // public port number
}

// EvNatMgrPubAddrUpdateInd
type MsgNatMgrPubAddrUpdateInd struct {
	Status   int    // map status
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
	PubIp    net.IP // public address
	PubPort  int    // public port number
}

// EvNatPubAddrSwitchInd
type MsgNatPubAddrSwitchInd struct {
	Proto    string // the prototcol, "tcp" or "udp"
	FromPort int    // local port number be mapped
	PubIp    net.IP // public address
	PubPort  int    // public port number
}
