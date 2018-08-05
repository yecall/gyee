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
	"net"
	config	"github.com/yeeco/gyee/p2p/config"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	"time"
)

//
// Null event: nothing;
// Poweron: scheduler just started;
// Poweroff: scheduler will be stopped.
//
const (
	EvSchNull		= 0
	EvSchPoweron	= EvSchNull + 1
	EvSchPoweroff	= EvSchNull + 2
	EvSchException	= EvSchNull + 3
)

//
// Scheduler internal event
//
const (
	EvSchBase			= 10
	EvSchTaskCreated	= EvSchBase + 1
)

//
// Timer event: for an user task, it could hold most timer number as schMaxTaskTimer,
// and then, when timer n which in [0,schMaxTaskTimer-1] is expired, message with event
// id as n would be sent to user task, means schMessage.id would be set to n.
//
const EvTimerBase = 1000

//
// Shell event
//
const EvShellBase = 1100

//
// Table manager event
//
const (
	TabRefreshTimerId	= 0
	TabPingpongTimerId	= 1
	TabFindNodeTimerId	= 2
)

const (
	EvTabMgrBase 		= 1200
	EvTabRefreshTimer	= EvTimerBase + TabRefreshTimerId
	EvTabPingpongTimer	= EvTimerBase + TabPingpongTimerId
	EvTabFindNodeTimer	= EvTimerBase + TabFindNodeTimerId
	EvTabRefreshReq		= EvTabMgrBase + 1
	EvTabRefreshRsp		= EvTabMgrBase + 2
)

//
// EvTabRefreshReq
//
type MsgTabRefreshReq struct {
	Snid	config.SubNetworkID	// sub network identity
	Include	[]*config.NodeID	// wanted, it can be an advice for discover
	Exclude	[]*config.NodeID	// filter out from response if any
}

//
// EvTabRefreshRsp
//
type MsgTabRefreshRsp struct {
	Snid	config.SubNetworkID	// sub network identity
	Nodes	[]*config.Node		// nodes found
}

//
// NodeDb cleaner event
//
const NdbCleanerTimerId = 1
const (
	EvNdbCleanerTimer	= EvTimerBase + NdbCleanerTimerId
)

//
// Discover manager event
//
const (
	EvDcvMgrBase		= 1300
	EvDcvFindNodeReq	= EvDcvMgrBase	+ 1
	EvDcvFindNodeRsp	= EvDcvMgrBase	+ 2
)

// EvDcvFindNodeReq
type MsgDcvFindNodeReq struct {
	Snid	config.SubNetworkID	// sub network identity
	More	int					// number of more peers needed
	Include	[]*config.NodeID	// wanted, it can be an advice for discover
	Exclude	[]*config.NodeID	// filter out from response if any
}

// EvDcvFindNodeRsp
type MsgDcvFindNodeRsp struct {
	Snid	config.SubNetworkID	// sub network identity
	Nodes	[]*config.Node		// nodes found
}

//
// Neighbor lookup on Udp event
//
const NblFindNodeTimerId	= 0
const NblPingpongTimerId	= 1
const (
	EvNblUdpBase			= 1400
	EvNblFindNodeTimer		= EvTimerBase	+ NblFindNodeTimerId
	EvNblPingpongTimer		= EvTimerBase	+ NblPingpongTimerId
	EvNblFindNodeReq		= EvNblUdpBase	+ 1
	EvNblFindNodeRsp		= EvNblUdpBase	+ 2
	EvNblPingpongReq		= EvNblUdpBase	+ 3
	EvNblPingpongRsp		= EvNblUdpBase	+ 4
	EvNblPingedInd			= EvNblUdpBase	+ 5
	EvNblPongedInd			= EvNblUdpBase	+ 6
	EvNblQueriedInd			= EvNblUdpBase	+ 7
)

//
// EvNblFindNodeRsp message
//
type NblFindNodeRsp struct {
	Result		int					// result, 0: ok, others: errno
	FindNode	*um.FindNode		// FindNode message from table task
	Neighbors	*um.Neighbors		// Neighbors message from peer node
}

//
// EvNblPingpongrRsp message
//
type NblPingRsp struct {
	Result		int					// result, 0: ok, others: errno
	Ping		*um.Ping			// Ping message from table task
	Pong		*um.Pong			// Pong message from peer
}

//
// EvNblPingedInd
//
type NblPingedInd struct {
	Ping		*um.Ping			// ping from remote node
}

//
// EvNblPongedInd
//
type NblPongedInd struct {
	Pong		*um.Pong			// pong from remote node
}

//
// EvNblQueriedInd
//
type NblQueriedInd struct {
	FindNode	*um.FindNode		// findnode from remote node
}

//
// Neighbor listenner event
//
const (
	EvNblListennerBase	= 1500
	EvNblMsgInd			= EvNblListennerBase + 1
	EvNblStart			= EvNblListennerBase + 2
	EvNblStop			= EvNblListennerBase + 3
	EvNblDataReq		= EvNblListennerBase + 4
)

//
// EvNblDataReq
//
type NblDataReq struct {
	Payload	[]byte					// payload
	TgtAddr	*net.UDPAddr			// target address
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
	EvPeerLsnBase = 1700
	EvPeLsnConnAcceptedInd	= EvPeerLsnBase + 1
	EvPeLsnStartReq			= EvPeerLsnBase + 2
	EvPeLsnStopReq			= EvPeerLsnBase + 3
)

//
// Peer connection establishment event
//
const PePingpongTimerId		= 0
const PeDcvFindNodeTimerId	= 1
const PeTestStatTimerId		= 2

const (
	EvPeerEstBase			= 1800
	EvPePingpongTimer		= EvTimerBase	+ PePingpongTimerId
	EvPeDcvFindNodeTimer	= EvTimerBase	+ PeDcvFindNodeTimerId
	EvPeTestStatTimer		= EvTimerBase	+ PeTestStatTimerId
	EvPeConnOutReq			= EvPeerEstBase + 1
	EvPeConnOutRsp			= EvPeerEstBase + 2
	EvPeHandshakeReq		= EvPeerEstBase + 3
	EvPeHandshakeRsp		= EvPeerEstBase + 4
	EvPePingpongReq			= EvPeerEstBase + 5
	EvPePingpongRsp			= EvPeerEstBase + 6
	EvPeCloseReq			= EvPeerEstBase + 7
	EvPeCloseCfm			= EvPeerEstBase + 8
	EvPeCloseInd			= EvPeerEstBase + 9
	EvPeOutboundReq			= EvPeerEstBase + 10
	EvPeEstablishedInd		= EvPeerEstBase + 11
	EvPeMgrStartReq			= EvPeerEstBase + 12
	EvPeDataReq				= EvPeerEstBase + 13
)

//
// EvPeCloseReq
//
type MsgPeCloseReq struct {
	Ptn		interface{}				// pointer to peer task instance node
	Snid	config.SubNetworkID		// sub network identity
	Node	config.Node				// peer node
	Dir		int						// direction
}

//
// DHT manager event
//
const (
	EvDhtMgrBase			= 1900
	EvDhtMgrFindPeerReq		= EvDhtMgrBase + 1
	EvDhtMgrFindPeerRsp		= EvDhtMgrBase + 2
	EvDhtMgrPutProviderReq	= EvDhtMgrBase + 3
	EvDhtMgrPutProviderRsp	= EvDhtMgrBase + 4
	EvDhtMgrGetProviderReq	= EvDhtMgrBase + 5
	EvDhtMgrGetProviderRsp	= EvDhtMgrBase + 6
	EvDhtMgrPutValueReq		= EvDhtMgrBase + 7
	EvDhtMgrPutValueRsp		= EvDhtMgrBase + 8
	EvDhtMgrGetValueReq		= EvDhtMgrBase + 9
	EvDhtMgrGetValueRsp		= EvDhtMgrBase + 10
)

//
// DHT listener manager event
//
const (
	EvDhtLsnMgrBase			= 2000
	EvDhtLsnMgrStartReq		= EvDhtLsnMgrBase + 1
	EvDhtLsnMgrStopReq		= EvDhtLsnMgrBase + 2
	EvDhtLsnMgrPauseReq		= EvDhtLsnMgrBase + 3
	EvDhtLsnMgrAcceptInd	= EvDhtLsnMgrBase + 4
	EvDhtLsnMgrStatusInd	= EvDhtLsnMgrBase + 5
)

//
// DHT connection manager event
//
const (
	EvDhtConMgrBase			= 2100
	EvDhtConMgrConnectReq	= EvDhtConMgrBase + 1
	EvDhtConMgrConnectRsp	= EvDhtConMgrBase + 2
	EvDhtConMgrSendReq		= EvDhtConMgrBase + 3
	EvDhtConMgrSendRsp		= EvDhtConMgrBase + 4
	EvDhtConMgrCloseReq		= EvDhtConMgrBase + 5
	EvDhtConMgrCloseRsp		= EvDhtConMgrBase + 6
)

//
// DHT connection instance event
//
const (
	EvDhtConInstBase		= 2200
	EvDhtConInstMsgInd		= EvDhtConInstBase + 1
	EvDhtConInstStatusInd	= EvDhtConInstBase + 2
	EvDhtConInstCloseReq	= EvDhtConInstBase + 3
	EvDhtConInstCloseRsp	= EvDhtConInstBase + 4
)

//
// DHT query manager event
//
const DhtQryMgrQcbTimerId	= 0
const DhtQryMgrIcbTimerId	= 1
const (
	EvDhtQryMgrBase				= 2300
	EvDhtQryMgrQcbTimer			= EvTimerBase + DhtQryMgrQcbTimerId
	EvDhtQryMgrIcbTimer			= EvTimerBase + DhtQryMgrIcbTimerId
	EvDhtQryMgrQueryStartReq	= EvDhtQryMgrBase + 1
	EvDhtQryMgrQueryStartRsp	= EvDhtQryMgrBase + 2
	EvDhtQryMgrQueryStopReq		= EvDhtQryMgrBase + 3
	EvDhtQryMgrQueryStopRsp		= EvDhtQryMgrBase + 4
	EvDhtQryMgrQueryResultInd	= EvDhtQryMgrBase + 5
)

//
// EvDhtQryMgrQueryStartReq
//
type MsgDhtQryMgrQueryStartReq struct {
	Target	config.NodeID			// target node identity
}

//
// EvDhtQryMgrQueryStartRsp
//
type MsgDhtQryMgrQueryStartRsp struct {
	Target	config.NodeID			// target node identity
	Eno		int						// result code
}

//
// EvDhtQryMgrQueryStopReq
//
type MsgDhtQryMgrQueryStopReq struct {
	Target	config.NodeID			// target node identity
}

//
// EvDhtQryMgrQueryStopRsp
//
type MsgDhtQryMgrQueryStopRsp struct {
	Target	config.NodeID			// target node identity
	Eno		int						// result code
}

//
// EvDhtQryMgrQueryResultInd
//
type MsgDhtQryMgrQueryResultInd struct {
	Eno		int							// result code. notice: when timeout, closests reported
	Target	config.NodeID				// target to be looked up
	Peers	[]*config.Node				// peers list, if target got, it always be the first one
}


//
// DHT query instance event
//
const (
	EvDhtQryInstBase			= 2400
	EvDhtQryInstStopReq			= EvDhtQryInstBase + 1
	EvDhtQryInstStopRsp			= EvDhtQryInstBase + 2
	EvDhtQryInstResultInd		= EvDhtQryInstBase + 3
)

//
// EvDhtQryInstResultInd
//
type MsgDhtQryInstResultInd struct {
	From		config.Node			// the peer who tells us
	Target		config.NodeID		// target node identity
	Latency		time.Duration		// latency about response to request
	Peers		[]*config.Node		// neighbors of target
	Pcs			[]int				// peer connection status, see dht.conMgrPeerConnStat pls
}

//
// EvDhtQryInstStopRsp
//
type MsgDhtQryInstStopRsp struct {
	To			config.Node			// whom is queried by the instance
	Target		config.NodeID		// target node identity
}

//
// DHT route manager event
//
const DhtRutBootstrapTimerId	= 0
const (
	EvDhtRutMgrBase				= 2500
	EvDhtRutBootstrapTimer		= EvTimerBase + DhtRutBootstrapTimerId
	EvDhtRutMgrNearestReq		= EvDhtRutMgrBase + 1
	EvDhtRutMgrNearestRsp		= EvDhtRutMgrBase + 2
	EvDhtRutMgrUpdateReq		= EvDhtRutMgrBase + 3
	EvDhtRutMgrNotificationInd	= EvDhtRutMgrBase + 4
	EvDhtRutPeerRemovedInd		= EvDhtRutMgrBase + 5
	EvDhtRutMgrStopNotifyReq	= EvDhtRutMgrBase + 6
)

//
// EvDhtRutMgrNearestReq
//
type MsgDhtRutMgrNearestReq struct {
	Target		config.NodeID		// target peer identity
	Max			int					// max items returned could be
	NtfReq		bool				// ask for notification when route updated
	Task		interface{}			// task who loves the notification
}

//
// EvDhtRutMgrNearestRsp
//
type MsgDhtRutMgrNearestRsp struct {
	Eno		int						// result code
	Target	config.NodeID			// target peer identity
	Peers	interface{}				// nearest nodes table
	Dists	interface{}				// distances of nearest nodes
}

//
// EvDhtRutMgrUpdateReq
//
type MsgDhtRutMgrUpdateReq struct {
	Seens	[]config.Node			// nodes seen
	Duras	[]time.Duration			// durations/latencies about seen nodes
}

//
// EvDhtRutMgrNotificationInd
//
type MsgDhtRutMgrNotificationInd struct {
	Target	config.NodeID			// target peer identity
	Peers	interface{}				// nearest nodes table
	Dists	interface{}				// distances of nearest nodes
}

//
// EvDhtRutPeerRemovedInd
//
type MsgDhtRutPeerRemovedInd struct {
	Target	config.NodeID			// target peer identity
}

//
// EvDhtRutMgrStopNotifyReq
//
type MsgDhtRutMgrStopNofiyReq struct {
	Task	interface{}				// owner task of the notifee registered
	Target	config.NodeID			// target peer identity
}

//
// DHT provider manager event
//
const (
	EvDhtPrdMgrBase				= 2600
	EvDhtPrdMgrAddProviderReq	= EvDhtPrdMgrBase + 1
	EvDhtPrdMgrAddProviderRsp	= EvDhtPrdMgrBase + 2
	EvDhtPrdMgrGetProviderReq	= EvDhtPrdMgrBase + 3
	EvDhtPrdMgrGetProviderRsp	= EvDhtPrdMgrBase + 4
	EvDhtPrdMgrUpdateReq		= EvDhtPrdMgrBase + 5
)
