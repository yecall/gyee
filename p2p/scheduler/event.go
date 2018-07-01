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
	ycfg	"github.com/yeeco/p2p/config"
	um		"github.com/yeeco/p2p/discover/udpmsg"
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
	Include	[]*ycfg.NodeID	// wanted, it can be an advice for discover
	Exclude	[]*ycfg.NodeID	// filter out from response if any
}

//
// EvTabRefreshRsp
//
type MsgTabRefreshRsp struct {
	Nodes	[]*ycfg.Node	// nodes found
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
	More	int				// number of more peers needed
	Include	[]*ycfg.NodeID	// wanted, it can be an advice for discover
	Exclude	[]*ycfg.NodeID	// filter out from response if any
}

// EvDcvFindNodeRsp
type MsgDcvFindNodeRsp struct {
	Nodes	[]*ycfg.Node	// nodes found
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
	Result		int				// result, 0: ok, others: errno
	FindNode	*um.FindNode	// FindNode message from table task
	Neighbors	*um.Neighbors	// Neighbors message from peer node
}

//
// EvNblPingpongrRsp message
//
type NblPingRsp struct {
	Result		int			// result, 0: ok, others: errno
	Ping		*um.Ping	// Ping message from table task
	Pong		*um.Pong	// Pong message from peer
}

//
// EvNblPingedInd
//
type NblPingedInd struct {
	Ping		*um.Ping		// ping from remote node
}

//
// EvNblPongedInd
//
type NblPongedInd struct {
	Pong		*um.Pong		// pong from remote node
}

//
// EvNblQueriedInd
//
type NblQueriedInd struct {
	FindNode	*um.FindNode	// findnode from remote node
}

//
// Neighbor listenner event
//
const (
	EvNblListennerBase	= 1500
	EvNblMsgInd			= EvNblListennerBase + 1
	EvNblStop			= EvNblListennerBase + 2
	EvNblStart			= EvNblListennerBase + 3
)

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
const PePingpongTimerId	= 0
const PeDcvFindNodeTimerId = 1
const (
	EvPeerEstBase			= 1800
	EvPePingpongTimer		= EvTimerBase	+ PePingpongTimerId
	EvPeDcvFindNodeTimer	= EvTimerBase	+ PeDcvFindNodeTimerId
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
	Ptn		interface{}		// pointer to peer task instance node
	Node	ycfg.Node		// peer node
}

//
// DHT manager event
//
const EvDhtMgrBase = 1900

//
// DHT peer lookup on Tcp event
//
const EvDhtPeerLkBase = 2000

//
// DHT provider event
//
const EvDhtPrdBase = 2100
