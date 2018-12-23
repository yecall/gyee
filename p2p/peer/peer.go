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
	"net"
	"time"
	"fmt"
	"math/rand"
	"reflect"
	ggio 	"github.com/gogo/protobuf/io"
	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	log		"github.com/yeeco/gyee/p2p/logger"
)

//
// stand alone for TEST when it's true
//
const _TEST_ = false


// Peer manager errno
const (
	PeMgrEnoNone	= iota
	PeMgrEnoParameter
	PeMgrEnoScheduler
	PeMgrEnoConfig
	PeMgrEnoResource
	PeMgrEnoOs
	PeMgrEnoMessage
	PeMgrEnoDuplicated
	PeMgrEnoNotfound
	PeMgrEnoMismatched
	PeMgrEnoInternal
	PeMgrEnoPingpongTh
	PeMgrEnoRecofig
	PeMgrEnoSign
	PeMgrEnoVerify
	PeMgrEnoUnknown
)

type PeMgrErrno int
type SubNetworkID = config.SubNetworkID

type PeerIdEx struct {
	Id				config.NodeID					// node identity
	Dir				int								// direction
}

type PeerIdExx struct {
	Snid			config.SubNetworkID				// sub network identity
	Node			config.Node						// node
	Dir				int								// direction
}

type PeerReconfig struct {
	delList			map[config.SubNetworkID]interface{}	// sub networks to be deleted
	addList			map[config.SubNetworkID]interface{}	// sub networks to be added
}

// Peer identity as string
type PeerId = config.NodeID

// Peer information
type PeerInfo Handshake

// Peer manager parameters
const (
	defaultConnectTimeout = 16 * time.Second		// default dial outbound timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

	defaultHandshakeTimeout = 8 * time.Second		// default handshake timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

	defaultActivePeerTimeout = 0 * time.Second		// default read/write operation timeout after a peer
													// connection is activaged in working.
	maxTcpmsgSize = 1024 * 1024 * 4					// max size of a tcpmsg package could be, currently
													// it's a fixed value here than can be configurated
													// by other module.

	durDcvFindNodeTimer = time.Second * 4			// duration to wait for find node response from discover task,
													// should be (findNodeExpiration + delta).

	durStaticRetryTimer = time.Second * 4			// duration to check and retry connect to static peers

	maxIndicationQueueSize = 512					// max indication queue size

	minDuration4FindNodeReq = time.Second * 2			// min duration to send find-node-request again
	minDuration4OutboundConnectReq = time.Second * 2	// min duration to try oubound connect for a specific
														// sub-network and peer

	conflictAccessDelayLower = 1000						// conflict delay lower bounder in time.Millisecond
	conflictAccessDelayUpper = 4000						// conflict delay upper bounder in time.Millisecond

	reconfigDelay = time.Second * 4						// reconfiguration delay time duration
)

// peer status
const (
	peerIdle			= iota						// idle
	peerConnectOutInited							// connecting out inited
	peerActivated									// had been activated
	peerKilling										// in killing
)

// peer manager configuration
type peMgrConfig struct {
	cfgName				string						// p2p configuration name
	ip					net.IP						// ip address
	port				uint16						// tcp port number
	udp					uint16						// udp port number, used with handshake procedure
	nodeId				config.NodeID				// the node's public key
	noDial				bool						// do not dial outbound
	noAccept			bool						// do not accept inbound
	bootstrapNode		bool						// local is a bootstrap node
	defaultCto			time.Duration				// default connect outbound timeout
	defaultHto			time.Duration				// default handshake timeout
	defaultAto			time.Duration				// default active read/write timeout
	maxMsgSize			int							// max tcpmsg package size
	protoNum			uint32						// local protocol number
	protocols			[]Protocol					// local protocol table
	networkType			int							// p2p network type
	staticMaxPeers		int							// max peers would be
	staticMaxOutbounds	int							// max concurrency outbounds
	staticMaxInBounds	int							// max concurrency inbounds
	staticNodes			[]*config.Node				// static nodes
	staticSubNetId		SubNetworkID				// static network identity
	subNetMaxPeers		map[SubNetworkID]int		// max peers would be
	subNetMaxOutbounds	map[SubNetworkID]int		// max concurrency outbounds
	subNetMaxInBounds	map[SubNetworkID]int		// max concurrency inbounds
	subNetIdList		[]SubNetworkID				// sub network identity list. do not put the identity
	ibpNumTotal			int							// total number of concurrency inbound peers
}

// peer manager
type PeerManager struct {
	debug__			bool							// if in debuging
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// name
	inited			chan PeMgrErrno					// result of initialization
	isInited		bool							// is manager initialized ok
	tep				sch.SchUserTaskEp				// entry
	cfg				peMgrConfig						// configuration
	tidFindNode		map[SubNetworkID]int			// find node timer identity
	ptnMe			interface{}						// pointer to myself(peer manager task node)
	ptnTab			interface{}						// pointer to table task node
	ptnLsn			interface{}						// pointer to peer listener manager task node
	ptnAcp			interface{}						// pointer to peer acceptor manager task node
	ptnDcv			interface{}						// pointer to discover task node
	ptnShell		interface{}						// pointer to shell task node

	//
	// Notice !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// here we backup the pointer of table manger to access it, this is dangerous
	// for the procedure of "poweroff", we take this into account in the "poweroff"
	// order of these two tasks, see var taskStaticPoweroffOrder4Chain please. We
	// should solve this issue later, the "accepter" pointer is the same case.
	//

	tabMgr			*tab.TableManager				// pointer to table manager

	ibInstSeq		int											// inbound instance sequence number
	obInstSeq		int											// outbound instance sequence number
	peers			map[interface{}]*peerInstance				// map peer instance's task node pointer to instance pointer
	nodes			map[SubNetworkID]map[PeerIdEx]*peerInstance	// map peer node identity to instance pointer
	workers			map[SubNetworkID]map[PeerIdEx]*peerInstance	// map peer node identity to pointer of instance in work
	wrkNum			map[SubNetworkID]int						// worker peer number
	ibpNum			map[SubNetworkID]int						// active inbound peer number
	obpNum			map[SubNetworkID]int						// active outbound peer number
	ibpTotalNum		int											// total active inbound peer number
	randoms			map[SubNetworkID][]*config.Node				// random nodes found by discover
	indChan			chan interface{}							// indication signal
	indCb			P2pIndCallback								// indication callback
	indCbUserData	interface{}									// user data pointer for callback
	staticsStatus	map[PeerIdEx]int							// status about static nodes

	ocrTid			int											// OCR(outbound connect request) timestamp cleanup timer
	tmLastOCR		map[SubNetworkID]map[PeerId]time.Time		// time of last outbound connect request for sub-netowerk
																// and peer-identity
	tmLastFNR		map[SubNetworkID]time.Time					// time of last find node request sent for sub network

	reCfg			PeerReconfig								// sub network reconfiguration
	reCfgTid		int											// reconfiguration timer
}

func NewPeerMgr() *PeerManager {
	var peMgr = PeerManager {
		debug__:		true,
		name:        	sch.PeerMgrName,
		inited:      	make(chan PeMgrErrno, 1),
		cfg:         	peMgrConfig{},
		tidFindNode: 	map[SubNetworkID]int{},
		peers:			map[interface{}]*peerInstance{},
		nodes:			map[SubNetworkID]map[PeerIdEx]*peerInstance{},
		workers:		map[SubNetworkID]map[PeerIdEx]*peerInstance{},
		wrkNum:      	map[SubNetworkID]int{},
		ibpNum:      	map[SubNetworkID]int{},
		obpNum:      	map[SubNetworkID]int{},
		ibpTotalNum:	0,
		indChan:     	make(chan interface{}, maxIndicationQueueSize),
		randoms:     	map[SubNetworkID][]*config.Node{},
		staticsStatus:	map[PeerIdEx]int{},
		ocrTid:			sch.SchInvalidTid,
		tmLastOCR:		make(map[SubNetworkID]map[PeerId]time.Time, 0),
		tmLastFNR:		make(map[SubNetworkID]time.Time, 0),
		reCfg:			PeerReconfig{
			delList:	make(map[config.SubNetworkID]interface{}, 0),
			addList:	make(map[config.SubNetworkID]interface{}, 0),
		},
		reCfgTid:		sch.SchInvalidTid,
	}
	peMgr.tep = peMgr.peerMgrProc
	return &peMgr
}

func (peMgr *PeerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return peMgr.tep(ptn, msg)
}

func (peMgr *PeerManager)peerMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	if peMgr.debug__ && peMgr.sdl != nil {
		sdl := peMgr.sdl.SchGetP2pCfgName()
		log.Debug("peerMgrProc: sdl: %s, name: %s, msg.Id: %d", sdl, peMgr.name, msg.Id)
	}

	if !peMgr.isInited {
		if msg.Id != sch.EvSchPoweron {
			sdl := peMgr.sdl.SchGetP2pCfgName()
			log.Debug("peerMgrProc: not be initialized, message discarded, "+
				"sdl: %s, name: %s, msg.Id: %d", sdl, peMgr.name, msg.Id)
			return PeMgrEnoMismatched
		}
	}

	var schEno = sch.SchEnoNone
	var eno PeMgrErrno = PeMgrEnoNone

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = peMgr.peMgrPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = peMgr.peMgrPoweroff(ptn)

	case sch.EvPeConflictAccessTimer:
		eno = peMgr.peMgrCatHandler(msg.Body)

	case sch.EvShellReconfigReq:
		eno = peMgr.shellReconfigReq(msg.Body.(*sch.MsgShellReconfigReq))

	case sch.EvPeReconfigTimer:
		eno = peMgr.reconfigTimerHandler()

	case sch.EvPeOcrCleanupTimer:
		peMgr.ocrTimestampCleanup()

	case sch.EvPeMgrStartReq:
		eno = peMgr.peMgrStartReq(msg.Body)

	case sch.EvDcvFindNodeRsp:
		eno = peMgr.peMgrDcvFindNodeRsp(msg.Body)

	case sch.EvPeDcvFindNodeTimer:
		eno = peMgr.peMgrDcvFindNodeTimerHandler(msg.Body)

	case sch.EvPeLsnConnAcceptedInd:
		eno = peMgr.peMgrLsnConnAcceptedInd(msg.Body)

	case sch.EvPeOutboundReq:
		eno = peMgr.peMgrOutboundReq(msg.Body)

	case sch.EvPeConnOutRsp:
		eno = peMgr.peMgrConnOutRsp(msg.Body)

	case sch.EvPeHandshakeRsp:
		eno = peMgr.peMgrHandshakeRsp(msg.Body)

	case sch.EvPePingpongRsp:
		eno = peMgr.peMgrPingpongRsp(msg.Body)

	case sch.EvPeCloseReq:
		eno = peMgr.peMgrCloseReq(msg)

	case sch.EvPeCloseCfm:
		eno = peMgr.peMgrConnCloseCfm(msg.Body)

	case sch.EvPeCloseInd:
		eno = peMgr.peMgrConnCloseInd(msg.Body)

	case sch.EvPeTxDataReq:
		eno = peMgr.peMgrDataReq(msg.Body)

	default:
		log.Debug("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if peMgr.debug__ && peMgr.sdl != nil {
		sdl := peMgr.sdl.SchGetP2pCfgName()
		log.Debug("peerMgrProc: get out, sdl: %s, name: %s, msg.Id: %d", sdl, peMgr.name, msg.Id)
	}

	if eno != PeMgrEnoNone {
		schEno = sch.SchEnoUserTask
	}

	return schEno
}

func (peMgr *PeerManager)peMgrPoweron(ptn interface{}) PeMgrErrno {
	peMgr.ptnMe	= ptn
	peMgr.sdl = sch.SchGetScheduler(ptn)
	_, peMgr.ptnLsn = peMgr.sdl.SchGetUserTaskNode(PeerLsnMgrName)

	var cfg *config.Cfg4PeerManager
	if cfg = config.P2pConfig4PeerManager(peMgr.sdl.SchGetP2pCfgName()); cfg == nil {
		peMgr.inited<-PeMgrEnoConfig
		return PeMgrEnoConfig
	}

	// with static network type that tabMgr and dcvMgr would be done while power on
	if cfg.NetworkType == config.P2pNetworkTypeDynamic {
		peMgr.tabMgr = peMgr.sdl.SchGetTaskObject(sch.TabMgrName).(*tab.TableManager)
		_, peMgr.ptnTab = peMgr.sdl.SchGetUserTaskNode(sch.TabMgrName)
		_, peMgr.ptnDcv = peMgr.sdl.SchGetUserTaskNode(sch.DcvMgrName)
	}

	if _TEST_ == false {
		var ok sch.SchErrno
		ok, peMgr.ptnShell = peMgr.sdl.SchGetUserTaskNode(sch.ShMgrName)
		if ok != sch.SchEnoNone || peMgr.ptnShell == nil {
			log.Debug("peMgrPoweron: shell not found")
			return PeMgrEnoScheduler
		}
	}

	peMgr.cfg = peMgrConfig {
		cfgName:			cfg.CfgName,
		ip:					cfg.IP,
		port:				cfg.Port,
		udp:				cfg.UDP,
		nodeId:				cfg.ID,
		noDial:				cfg.NoDial,
		noAccept:			cfg.NoAccept,
		bootstrapNode:		cfg.BootstrapNode,
		defaultCto:			defaultConnectTimeout,
		defaultHto:			defaultHandshakeTimeout,
		defaultAto:			defaultActivePeerTimeout,
		maxMsgSize:			maxTcpmsgSize,
		protoNum:			cfg.ProtoNum,
		protocols:			make([]Protocol, 0),

		networkType:		cfg.NetworkType,
		staticMaxPeers:		cfg.StaticMaxPeers,
		staticMaxOutbounds:	cfg.StaticMaxOutbounds,
		staticMaxInBounds:	cfg.StaticMaxInBounds,
		staticNodes:		cfg.StaticNodes,
		staticSubNetId:		cfg.StaticNetId,
		subNetMaxPeers:		cfg.SubNetMaxPeers,
		subNetMaxOutbounds:	cfg.SubNetMaxOutbounds,
		subNetMaxInBounds:	cfg.SubNetMaxInBounds,
		subNetIdList:		cfg.SubNetIdList,
		ibpNumTotal:		0,
	}

	peMgr.cfg.ibpNumTotal = peMgr.cfg.staticMaxInBounds
	for _, ibpNum := range peMgr.cfg.subNetMaxInBounds {
		peMgr.cfg.ibpNumTotal += ibpNum
	}

	for _, p := range cfg.Protocols {
		peMgr.cfg.protocols = append(peMgr.cfg.protocols, Protocol{ Pid:p.Pid, Ver:p.Ver,})
	}

	// Notice: even the network type is P2pNetworkTypeDynamic, a static sub network can
	// still be exist, if static nodes are specified.
	for _, sn := range peMgr.cfg.staticNodes {
		idEx := PeerIdEx{Id:sn.ID, Dir:PeInstDirOutbound}
		peMgr.staticsStatus[idEx] = peerIdle
		idEx.Dir = PeInstDirInbound
		peMgr.staticsStatus[idEx] = peerIdle
	}

	if len(peMgr.cfg.staticNodes) > 0 {
		staticSnid := peMgr.cfg.staticSubNetId
		peMgr.nodes[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.workers[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.wrkNum[staticSnid] = 0
		peMgr.ibpNum[staticSnid] = 0
		peMgr.obpNum[staticSnid] = 0
	}

	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		// setup each dynamic sub network: if no identities are provided, we set a AnySubNet,
		// else deal with each one; dynamic sub network identities are also put into the "add"
		// list of the reconfiguration struct.
		if len(peMgr.cfg.subNetIdList) == 0 {
			peMgr.cfg.subNetIdList = append(peMgr.cfg.subNetIdList, config.AnySubNet)
			peMgr.cfg.subNetMaxPeers[config.AnySubNet] = config.MaxPeers
			peMgr.cfg.subNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
			peMgr.cfg.subNetMaxInBounds[config.AnySubNet] = config.MaxInbounds
		}
		for _, snid := range peMgr.cfg.subNetIdList {
			peMgr.nodes[snid] = make(map[PeerIdEx]*peerInstance)
			peMgr.workers[snid] = make(map[PeerIdEx]*peerInstance)
			peMgr.wrkNum[snid] = 0
			peMgr.ibpNum[snid] = 0
			peMgr.obpNum[snid] = 0
			peMgr.reCfg.addList[snid] = nil
		}

		// tell discover manager that sub networks changed, notice that at this moment, it's
		// in the power on procedure of system, the discover manager task must be powered on
		// now, see power on order table in file static.go please.
		peMgr.peMgrRecfg2DcvMgr()

	} else if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		staticSnid := peMgr.cfg.staticSubNetId
		peMgr.nodes[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.workers[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.wrkNum[staticSnid] = 0
		peMgr.ibpNum[staticSnid] = 0
		peMgr.obpNum[staticSnid] = 0
	} else {
		log.Debug("peMgrPoweron: invalid network type: %d", peMgr.cfg.networkType)
		return PeMgrEnoConfig
	}

	// tell initialization result, and EvPeMgrStartReq would be sent to us
	// some moment later.
	peMgr.isInited = true
	peMgr.inited<-PeMgrEnoNone

	return PeMgrEnoNone
}

func (peMgr *PeerManager)PeMgrInited() PeMgrErrno {
	return <-peMgr.inited
}

func (peMgr *PeerManager)PeMgrStart() PeMgrErrno {
	log.Debug("PeMgrStart: EvPeMgrStartReq will be sent, sdl: %s, target: %s",
		peMgr.sdl.SchGetP2pCfgName(), peMgr.sdl.SchGetTaskName(peMgr.ptnMe))
	msg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
	peMgr.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrPoweroff(ptn interface{}) PeMgrErrno {
	sdl := peMgr.sdl.SchGetP2pCfgName()
	log.Debug("peMgrPoweroff: sdl: %s, task will be done, name: %s",
		sdl, peMgr.sdl.SchGetTaskName(ptn))

	powerOff := sch.SchMessage {
		Id:		sch.EvSchPoweroff,
		Body:	nil,
	}

	peMgr.sdl.SchSetSender(&powerOff, &sch.RawSchTask)
	for _, peerInst := range peMgr.peers {
		peMgr.sdl.SchSetRecver(&powerOff, peerInst.ptnMe)
		peMgr.sdl.SchSendMessage(&powerOff)
	}

	close(peMgr.indChan)
	if peMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrStartReq(_ interface{}) PeMgrErrno {

	sdl := peMgr.sdl.SchGetP2pCfgName()
	log.Debug("peMgrStartReq: sdl: %s, task: %s", sdl, peMgr.name)

	var schMsg = sch.SchMessage{}

	// start peer listener if necessary
	if peMgr.cfg.noAccept == false {
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	// setup outbound timestamp cleaner timer
	var tdOcr = sch.TimerDescription {
		Name:	"_pocrTimer",
		Utid:	sch.PeMinOcrCleanupTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	minDuration4OutboundConnectReq,
		Extra:	nil,
	}

	var eno sch.SchErrno
	eno, peMgr.ocrTid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &tdOcr)
	if eno != sch.SchEnoNone || peMgr.ocrTid == sch.SchInvalidTid {
		log.Debug("peMgrStartReq: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	// drive ourselves to startup outbound
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)ocrTimestampCleanup() {
	now := time.Now()
	for _, tmPeers := range peMgr.tmLastOCR {
		idList := make([]PeerId, 0)
		for id, t := range tmPeers {
			if now.Sub(t) >= minDuration4OutboundConnectReq {
				idList = append(idList, id)
			}
		}
		for _, id := range idList {
			delete(tmPeers, id)
		}
	}
}

func (peMgr *PeerManager)peMgrDcvFindNodeRsp(msg interface{}) PeMgrErrno {
	var rsp = msg.(*sch.MsgDcvFindNodeRsp)
	if peMgr.dynamicSubNetIdExist(&rsp.Snid) != true {
		log.Debug("peMgrDcvFindNodeRsp: subnet not exist")
		return PeMgrEnoNotfound
	}

	if _, ok := peMgr.reCfg.delList[rsp.Snid]; ok {
		log.Debug("peMgrDcvFindNodeRsp: discarded for reconfiguration, snid: %x", rsp.Snid)
		return PeMgrEnoRecofig
	}

	var snid = rsp.Snid
	var appended = make(map[SubNetworkID]int, 0)
	var dup bool
	var idEx PeerIdEx

	for _, n := range rsp.Nodes {
		idEx.Id = n.ID
		idEx.Dir = PeInstDirOutbound
		if _, ok := peMgr.nodes[snid][idEx]; ok {
			continue
		}

		idEx.Dir = PeInstDirInbound
		if _, ok := peMgr.nodes[snid][idEx]; ok {
			continue
		}

		dup = false
		for _, rn := range peMgr.randoms[snid] {
			if rn.ID == n.ID {
				dup = true
				break
			}
		}
		if dup { continue }

		dup = false
		for _, s := range peMgr.cfg.staticNodes {
			if s.ID == n.ID && snid == peMgr.cfg.staticSubNetId {
				dup = true
				break
			}
		}
		if dup { continue }

		if len(peMgr.randoms[snid]) >= peMgr.cfg.subNetMaxPeers[snid] {
			log.Debug("peMgrDcvFindNodeRsp: too much, some are truncated")
			continue
		}

		peMgr.randoms[snid] = append(peMgr.randoms[snid], n)
		appended[snid]++
	}

	// drive ourselves to startup outbound for nodes appended
	for snid := range appended {
		var schMsg sch.SchMessage
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &snid)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDcvFindNodeTimerHandler(msg interface{}) PeMgrErrno {
	nwt := peMgr.cfg.networkType
	snid := msg.(*SubNetworkID)

	if nwt == config.P2pNetworkTypeStatic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.staticMaxOutbounds {
			return PeMgrEnoNone
		}
	} else if nwt == config.P2pNetworkTypeDynamic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
			return PeMgrEnoNone
		}
	}

	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, snid)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoInternal
}

func (peMgr *PeerManager)peMgrLsnConnAcceptedInd(msg interface{}) PeMgrErrno {
	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var ibInd = msg.(*msgConnAcceptedInd)
	var peInst = new(peerInstance)

	*peInst				= peerInstDefault
	peInst.debug__		= true
	peInst.sdl			= peMgr.sdl
	peInst.peMgr		= peMgr
	peInst.tep			= peInst.peerInstProc
	peInst.ptnMgr		= peMgr.ptnMe
	peInst.state		= peInstStateAccepted
	peInst.cto			= peMgr.cfg.defaultCto
	peInst.hto			= peMgr.cfg.defaultHto
	peInst.ato			= peMgr.cfg.defaultAto
	peInst.maxPkgSize	= peMgr.cfg.maxMsgSize
	peInst.dialer		= nil
	peInst.conn			= ibInd.conn
	peInst.laddr		= ibInd.localAddr
	peInst.raddr		= ibInd.remoteAddr
	peInst.dir			= PeInstDirInbound

	peInst.txChan		= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.rxChan		= make(chan *P2pPackageRx, PeInstMaxP2packages)
	peInst.rxDone		= make(chan PeMgrErrno)
	peInst.rxtxRuning	= false

	// Create peer instance task
	peMgr.ibInstSeq++
	peInst.name = peInst.name + fmt.Sprintf("_inbound_%s",
		fmt.Sprintf("%d_", peMgr.ibInstSeq) + peInst.raddr.String())

	var tskDesc  = sch.SchTaskDescription {
		Name:		peInst.name,
		MbSize:		PeInstMailboxSize,
		Ep:			peInst,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		peInst,
	}

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {
		log.Debug("peMgrLsnConnAcceptedInd: SchCreateTask failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	peInst.ptnMe = ptnInst

	// Send handshake request to the instance created aboved
	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeHandshakeReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)
	peMgr.peers[peInst.ptnMe] = peInst

	// Pause inbound peer accepter if necessary
	if peMgr.ibpTotalNum++; peMgr.ibpTotalNum >= peMgr.cfg.ibpNumTotal {
		if !peMgr.cfg.noAccept {
			// we stop accepter simply, a duration of delay should be apply before pausing it,
			// this should be improved later.
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStopReq, nil)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrOutboundReq(msg interface{}) PeMgrErrno {
	if peMgr.cfg.noDial || peMgr.cfg.bootstrapNode {
		return PeMgrEnoNone
	}

	// if sub network identity is not specified, try to start all
	var snid *SubNetworkID

	if msg != nil { snid = msg.(*SubNetworkID) }

	if snid == nil {

		if eno := peMgr.peMgrStaticSubNetOutbound(); eno != PeMgrEnoNone {
			return eno
		}

		if peMgr.cfg.networkType != config.P2pNetworkTypeStatic {

			for _, id := range peMgr.cfg.subNetIdList {

				if eno := peMgr.peMgrDynamicSubNetOutbound(&id); eno != PeMgrEnoNone {
					return eno
				}
			}
		}

	} else if peMgr.cfg.networkType == config.P2pNetworkTypeStatic &&
		*snid == peMgr.cfg.staticSubNetId {

		return peMgr.peMgrStaticSubNetOutbound()

	} else if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {

		if peMgr.dynamicSubNetIdExist(snid) == true {

			return peMgr.peMgrDynamicSubNetOutbound(snid)

		} else if peMgr.staticSubNetIdExist(snid) {

			return peMgr.peMgrStaticSubNetOutbound()
		}
	}

	return PeMgrEnoNotfound
}

func (peMgr *PeerManager)peMgrStaticSubNetOutbound() PeMgrErrno {
	if len(peMgr.cfg.staticNodes) == 0 {
		return PeMgrEnoNone
	}

	snid := peMgr.cfg.staticSubNetId
	if peMgr.wrkNum[snid] >= peMgr.cfg.staticMaxPeers {
		return PeMgrEnoNone
	}

	if peMgr.obpNum[snid] >= peMgr.cfg.staticMaxOutbounds {
		return PeMgrEnoNone
	}

	var candidates = make([]*config.Node, 0)
	var count = 0
	var idEx = PeerIdEx {
		Id:		config.NodeID{},
		Dir:	PeInstDirOutbound,
	}

	for _, n := range peMgr.cfg.staticNodes {
		idEx.Id = n.ID
		_, dup := peMgr.nodes[snid][idEx]
		if !dup && peMgr.staticsStatus[idEx] == peerIdle {
			candidates = append(candidates, n)
			count++
		}
	}

	// Create outbound instances for candidates if any.
	var failed = 0
	var ok = 0
	idEx = PeerIdEx{Id:config.NodeID{}, Dir:PeInstDirOutbound}

	for cdNum := len(candidates); cdNum > 0; cdNum-- {

		idx := rand.Intn(cdNum)
		n := candidates[idx]
		idEx.Id = n.ID
		candidates = append(candidates[:idx], candidates[idx+1:]...)

		if eno := peMgr.peMgrCreateOutboundInst(&snid, n); eno != PeMgrEnoNone {
			if _, static := peMgr.staticsStatus[idEx]; static {
				peMgr.staticsStatus[idEx] = peerIdle
			}
			failed++
			continue
		}

		peMgr.staticsStatus[idEx] = peerConnectOutInited
		ok++

		if peMgr.obpNum[snid] >= peMgr.cfg.staticMaxOutbounds {
			break
		}
	}

	// If outbounds are not enough, ask discoverer for more
	if peMgr.obpNum[snid] < peMgr.cfg.staticMaxOutbounds {
		if eno := peMgr.peMgrAsk4More(&snid); eno != PeMgrEnoNone {
			return eno
		}
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDynamicSubNetOutbound(snid *SubNetworkID) PeMgrErrno {
	if peMgr.wrkNum[*snid] >= peMgr.cfg.subNetMaxPeers[*snid] {
		return PeMgrEnoNone
	}

	if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
		return PeMgrEnoNone
	}

	var candidates = make([]*config.Node, 0)
	var idEx PeerIdEx

	for _, n := range peMgr.randoms[*snid] {

		idEx.Id = n.ID
		idEx.Dir = PeInstDirOutbound

		if _, ok := peMgr.nodes[*snid][idEx]; !ok {

			idEx.Dir = PeInstDirInbound

			if _, ok := peMgr.nodes[*snid][idEx]; !ok {
				candidates = append(candidates, n)
			}
		}
	}

	peMgr.randoms[*snid] = make([]*config.Node, 0)

	// Create outbound instances for candidates if any
	var failed = 0
	var ok = 0
	maxOutbound := peMgr.cfg.subNetMaxOutbounds[*snid]

	for _, n := range candidates {

		if tmPeers, ok := peMgr.tmLastOCR[*snid]; ok {

			if t, ok := tmPeers[n.ID]; ok {
				if time.Now().Sub(t) <= minDuration4OutboundConnectReq {
					continue
				}
			}

			tmPeers[n.ID] = time.Now()

		} else {

			tmPeers := make(map[PeerId]time.Time, 0)
			tmPeers[n.ID] = time.Now()
			peMgr.tmLastOCR[*snid] = tmPeers
		}

		if eno := peMgr.peMgrCreateOutboundInst(snid, n); eno != PeMgrEnoNone {
			failed++
			continue
		}

		ok++
		if peMgr.obpNum[*snid] >= maxOutbound {
			break
		}
	}

	// If outbounds are not enough, ask discover to find more
	if peMgr.obpNum[*snid] < maxOutbound {
		if eno := peMgr.peMgrAsk4More(snid); eno != PeMgrEnoNone {
			return eno
		}
	}

	return PeMgrEnoNone
}

//
// Outbound response handler
//
func (peMgr *PeerManager)peMgrConnOutRsp(msg interface{}) PeMgrErrno {
	var rsp = msg.(*msgConnOutRsp)
	if rsp.result != PeMgrEnoNone {

		// here the outgoing instance might have been killed in function
		// peMgrHandshakeRsp due to the duplication nodes, so we should
		// check this to kill it.

		if _, lived := peMgr.peers[rsp.ptn]; lived {

			if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, PeInstDirOutbound); eno != PeMgrEnoNone {
				log.Debug("peMgrConnOutRsp: peMgrKillInst failed, eno: %d", eno)
				return eno
			}

			// drive ourselves to startup outbound
			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &rsp.snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}

		return PeMgrEnoNone
	}

	// request the instance to handshake
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeHandshakeReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrHandshakeRsp(msg interface{}) PeMgrErrno {
	// This is an event from an instance task of outbound or inbound peer, telling
	// the result about the handshake procedure between a pair of peers.
	var rsp = msg.(*msgHandshakeRsp)
	var inst *peerInstance
	var lived bool

	if inst, lived = peMgr.peers[rsp.ptn]; inst == nil || !lived {
		log.Debug("peMgrHandshakeRsp: instance not found, sdl: %s, rsp: %s",
			peMgr.sdl.SchGetP2pCfgName(),
			fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoNotfound
	}

	if inst.snid != rsp.snid || inst.dir != rsp.dir {
		log.Debug("peMgrHandshakeRsp: response mismatched with instance, rsp: %s",
			fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoParameter
	}

	if _, ok := peMgr.reCfg.delList[rsp.snid]; ok {
		log.Debug("peMgrHandshakeRsp: kill instance for reconfiguration, snid: %x", rsp.snid)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
		return PeMgrEnoRecofig
	}

	// Check result, if failed, kill the instance
	idEx := PeerIdEx{Id:rsp.peNode.ID, Dir:rsp.dir}

	if rsp.result != PeMgrEnoNone {

		peMgr.updateStaticStatus(rsp.snid, idEx, peerKilling)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)

		if inst.dir == PeInstDirOutbound {

			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &inst.snid)
			peMgr.sdl.SchSendMessage(&schMsg)

		}

		return PeMgrEnoNone
	}

	// Check duplicated for inbound instance. Notice: only here the peer manager can known the
	// identity of peer to determine if it's duplicated to an outbound instance, which is an
	// instance connect from local to outside.
	var maxInbound = 0
	var maxOutbound = 0
	var maxPeers = 0
	snid := rsp.snid

	if peMgr.cfg.networkType == config.P2pNetworkTypeStatic &&
		peMgr.staticSubNetIdExist(&snid) == true {

		maxInbound = peMgr.cfg.staticMaxInBounds
		maxOutbound = peMgr.cfg.staticMaxOutbounds
		maxPeers = peMgr.cfg.staticMaxPeers

	} else if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {

		if peMgr.dynamicSubNetIdExist(&snid) == true {

			maxInbound = peMgr.cfg.subNetMaxInBounds[snid]
			maxOutbound = peMgr.cfg.subNetMaxOutbounds[snid]
			maxPeers = peMgr.cfg.subNetMaxPeers[snid]

		} else if peMgr.staticSubNetIdExist(&snid) == true {

			maxInbound = peMgr.cfg.staticMaxInBounds
			maxOutbound = peMgr.cfg.staticMaxOutbounds
			maxPeers = peMgr.cfg.staticMaxPeers
		}
	}

	if peMgr.wrkNum[snid] >= maxPeers {
		peMgr.updateStaticStatus(snid, idEx, peerKilling)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, rsp.dir)
		return PeMgrEnoResource
	}

	idEx.Dir = PeInstDirInbound
	if _, dup := peMgr.workers[snid][idEx]; dup {
		if _, dup := peMgr.workers[snid][idEx]; dup {
			peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
			return PeMgrEnoDuplicated
		}
	}

	idEx.Dir = PeInstDirOutbound
	if _, dup := peMgr.workers[snid][idEx]; dup {
		if _, dup := peMgr.workers[snid][idEx]; dup {
			peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
			return PeMgrEnoDuplicated
		}
	}

	if inst.dir == PeInstDirInbound {

		if peMgr.isStaticSubNetId(snid) {

			peMgr.workers[snid][idEx] = inst

		} else {

			if peMgr.ibpNum[snid] >= maxInbound {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoResource
			}

			idEx.Dir = PeInstDirOutbound
			if _, dup := peMgr.nodes[snid][idEx]; dup {
				// this case, both instances are not in working, we kill one instance local,
				// but the peer might kill another, then two connections are lost, we need a
				// protection for this.
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)
				return PeMgrEnoDuplicated
			}
		}

		idEx.Dir = PeInstDirInbound
		peMgr.nodes[snid][idEx] = inst
		peMgr.workers[snid][idEx] = inst
		peMgr.ibpNum[snid]++

	} else if inst.dir == PeInstDirOutbound {

		if peMgr.isStaticSubNetId(snid) {

			peMgr.workers[snid][idEx] = inst

		} else {

			if peMgr.obpNum[snid] >= maxOutbound {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoResource
			}

			idEx.Dir = PeInstDirInbound
			if _, dup := peMgr.nodes[snid][idEx]; dup {
				// this case, both instances are not in working, we kill one instance local,
				// but the peer might kill another, then two connections are lost, we need a
				// protection for this.
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)
				return PeMgrEnoDuplicated
			}

			idEx.Dir = PeInstDirOutbound
			peMgr.workers[snid][idEx] = inst
			peMgr.updateStaticStatus(snid, idEx, peerActivated)
		}
	}

	schMsg := sch.SchMessage{}
	cfmCh := make(chan int)
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeEstablishedInd, &cfmCh)
	peMgr.sdl.SchSendMessage(&schMsg)

	if eno, ok := <-cfmCh; eno != PeMgrEnoNone || !ok {
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
		return PeMgrErrno(eno)
	}

	inst.state = peInstStateActivated
	peMgr.wrkNum[snid]++

	if inst.dir == PeInstDirInbound  &&
		inst.peMgr.cfg.networkType != config.P2pNetworkTypeStatic {

		// Notice: even the network type is not static, the "snid" can be a static subnet
		// in a configuration where "dynamic" and "static" are exist both. So, calling functions
		// TabBucketAddNode or TabUpdateNode might be failed since these functions would not
		// work for a static case.

		lastQuery := time.Time{}
		lastPing := time.Now()
		lastPong := time.Now()
		n := um.Node{
			IP:     rsp.peNode.IP,
			UDP:    rsp.peNode.UDP,
			TCP:    rsp.peNode.TCP,
			NodeId: rsp.peNode.ID,
		}

		tabEno := peMgr.tabMgr.TabBucketAddNode(snid, &n, &lastQuery, &lastPing, &lastPong)

		if tabEno != tab.TabMgrEnoNone {
			if peMgr.debug__ {
				log.Debug("peMgrHandshakeRsp: TabBucketAddNode failed, eno: %d, snid: %x, node: %s",
					tabEno, snid, fmt.Sprintf("%+v", *rsp.peNode))
			}
		}

		tabEno = peMgr.tabMgr.TabUpdateNode(snid, &n)
		if tabEno != tab.TabMgrEnoNone {
			if peMgr.debug__ {
				log.Debug("peMgrHandshakeRsp: TabUpdateNode failed, eno: %d, snid: %x, node: %s",
					tabEno, snid, fmt.Sprintf("%+v", *rsp.peNode))
			}
		}
	}

	i := P2pIndPeerActivatedPara {
		P2pInst: peMgr.sdl,
		RxChan: inst.rxChan,
		PeerInfo: & Handshake {
			Snid:		inst.snid,
			Dir:		inst.dir,
			NodeId:		inst.node.ID,
			ProtoNum:	inst.protoNum,
			Protocols:	inst.protocols,
		},
	}

	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerActiveInd{
			TxChan: inst.txChan,
			RxChan: inst.rxChan,
			PeerInfo: i.PeerInfo,
		}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerActiveInd, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
		return PeMgrEnoNone
	}

	return peMgr.peMgrIndEnque(&i)
}

func (peMgr *PeerManager)peMgrPingpongRsp(msg interface{}) PeMgrErrno {
	var rsp = msg.(*msgPingpongRsp)
	if rsp.result != PeMgrEnoNone {
		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, rsp.dir); eno != PeMgrEnoNone {
			log.Debug("peMgrPingpongRsp: kill instance failed, inst: %s, node: %s",
				peMgr.sdl.SchGetTaskName(rsp.ptn),
				config.P2pNodeId2HexString(rsp.peNode.ID))
			return eno
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCloseReq(msg *sch.SchMessage) PeMgrErrno {
	// here it's asked to close a peer instance, this might happen in following cases:
	// 1) the shell task ask to do this;
	// 2) a peer instance gone into bad status so it asks to be closed;
	// 3) function ClosePeer called from outside modules
	// with 2), if shell task is present, we should not send EvPeCloseReq to peer instance
	// since the shell might access to the peer (we had tell it with EvShellPeerActiveInd),
	// instead we should inform the shell task, so it should send us EvPeCloseReq, and then
	// we can send EvPeCloseReq to the peer instance.
	var schMsg = sch.SchMessage{}
	var req = msg.Body.(*sch.MsgPeCloseReq)
	var snid = req.Snid
	var idEx = PeerIdEx{Id: req.Node.ID, Dir: req.Dir}

	inst := peMgr.getWorkerInst(snid, &idEx)
	if inst == nil {
		return PeMgrEnoNotfound
	}

	if inst.state == peInstStateKilling {
		return PeMgrEnoDuplicated
	}

	if ptnSender := peMgr.sdl.SchGetSender(msg); ptnSender != peMgr.ptnShell {
		if req.Ptn != nil {
			ind := sch.MsgShellPeerAskToCloseInd {
				Snid: snid,
				PeerId: idEx.Id,
				Dir: idEx.Dir,
				Why: req.Why,
			}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerAskToCloseInd, &ind)
			peMgr.sdl.SchSendMessage(&schMsg)
			return PeMgrEnoNone
		}
	}

	peMgr.updateStaticStatus(snid, idEx, peerKilling)
	req.Node = inst.node
	req.Ptn = inst.ptnMe
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConnCloseCfm(msg interface{}) PeMgrErrno {
	var schMsg = sch.SchMessage{}
	var cfm = msg.(*MsgCloseCfm)

	if eno := peMgr.peMgrKillInst(cfm.ptn, cfm.peNode, cfm.dir); eno != PeMgrEnoNone {
		return PeMgrEnoScheduler
	}

	i := P2pIndPeerClosedPara {
		P2pInst:	peMgr.sdl,
		Snid:		cfm.snid,
		PeerId:		cfm.peNode.ID,
		Dir:		cfm.dir,
	}

	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerCloseCfm{
			Result: int(cfm.result),
			Dir: cfm.dir,
			Snid: cfm.snid,
			PeerId: cfm.peNode.ID,
		}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerCloseCfm, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
	} else {
		peMgr.peMgrIndEnque(&i)
	}

	// drive ourselves to startup outbound
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &cfm.snid)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConnCloseInd(msg interface{}) PeMgrErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement.
	panic("peMgrConnCloseInd: should never be called!!!")

	schMsg := sch.SchMessage{}
	ind := msg.(*MsgCloseInd)

	if eno := peMgr.peMgrKillInst(ind.ptn, ind.peNode, ind.dir); eno != PeMgrEnoNone {
		return PeMgrEnoScheduler
	}

	i := P2pIndPeerClosedPara {
		P2pInst:	peMgr.sdl,
		Snid:		ind.snid,
		PeerId:		ind.peNode.ID,
		Dir:		ind.dir,
	}

	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerCloseInd{
			Cause: int(ind.cause),
			Dir: ind.dir,
			Snid: ind.snid,
			PeerId: ind.peNode.ID,
		}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerCloseInd, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
	} else {
		peMgr.peMgrIndEnque(&i)
	}

	// drive ourselves to startup outbound
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &ind.snid)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDataReq(msg interface{}) PeMgrErrno {
	var inst *peerInstance = nil
	var idEx = PeerIdEx{}
	var req = msg.(*sch.MsgPeDataReq)

	idEx.Id = req.PeerId
	idEx.Dir = PeInstDirOutbound

	if inst = peMgr.getWorkerInst(req.SubNetId, &idEx); inst == nil {
		idEx.Dir = PeInstDirInbound
		if inst = peMgr.getWorkerInst(req.SubNetId, &idEx); inst == nil {
			return PeMgrEnoNotfound
		}
	}

	// Notice: when we are requested to send data with the specific instance, it's
	// possible that the instance is in killing, we had to check the state of it to
	// discard the request if it is the case.
	if inst.state != peInstStateActivated {
		return PeMgrEnoNotfound
	}

	if len(inst.txChan) >= cap(inst.txChan) {
		log.Debug("peMgrDataReq: tx queue full, inst: %s", inst.name)
		return PeMgrEnoResource
	}

	_pkg := req.Pkg.(*P2pPackage)
	inst.txChan<-_pkg
	inst.txPendNum += 1

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCreateOutboundInst(snid *config.SubNetworkID, node *config.Node) PeMgrErrno {
	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var peInst = new(peerInstance)

	*peInst				= peerInstDefault
	peInst.sdl			= peMgr.sdl
	peInst.peMgr		= peMgr
	peInst.tep			= peInst.peerInstProc
	peInst.ptnMgr		= peMgr.ptnMe
	peInst.state		= peInstStateConnOut
	peInst.cto			= peMgr.cfg.defaultCto
	peInst.hto			= peMgr.cfg.defaultHto
	peInst.ato			= peMgr.cfg.defaultAto
	peInst.maxPkgSize	= peMgr.cfg.maxMsgSize
	peInst.dialer		= &net.Dialer{Timeout: peMgr.cfg.defaultCto}
	peInst.conn			= nil
	peInst.laddr		= nil
	peInst.raddr		= nil
	peInst.dir			= PeInstDirOutbound
	peInst.snid			= *snid
	peInst.node			= *node

	peInst.txChan		= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.rxChan		= make(chan *P2pPackageRx, PeInstMaxP2packages)
	peInst.rxDone		= make(chan PeMgrErrno)
	peInst.rxtxRuning	= false

	peMgr.obInstSeq++
	peInst.name = peInst.name + fmt.Sprintf("_Outbound_%s", fmt.Sprintf("%d", peMgr.obInstSeq))
	tskDesc := sch.SchTaskDescription {
		Name:		peInst.name,
		MbSize:		PeInstMailboxSize,
		Ep:			peInst,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		peInst,
	}

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {
		log.Debug("peMgrCreateOutboundInst: SchCreateTask failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	peInst.ptnMe = ptnInst
	peMgr.peers[peInst.ptnMe] = peInst
	idEx := PeerIdEx{Id:peInst.node.ID, Dir:peInst.dir}
	peMgr.nodes[*snid][idEx] = peInst
	peMgr.obpNum[*snid]++

	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeConnOutReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrKillInst(ptn interface{}, node *config.Node, dir int) PeMgrErrno {

	if peMgr.debug__ {
		log.Debug("peMgrKillInst: sdl: %s, task: %s",
			peMgr.sdl.SchGetP2pCfgName(), peMgr.sdl.SchGetTaskName(ptn))
	}

	var peInst = peMgr.peers[ptn]
	if peInst == nil {
		log.Debug("peMgrKillInst: instance not found, node: %s",
			config.P2pNodeId2HexString(node.ID))
		return PeMgrEnoNotfound
	}

	if peInst.dir != dir {
		log.Debug("peMgrKillInst: invalid parameters")
		return PeMgrEnoParameter
	}

	if peInst.ppTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(ptn, peInst.ppTid)
		peInst.ppTid = sch.SchInvalidTid
	}

	if peInst.conn != nil {
		peInst.conn.Close()
	}

	// Remove maps for the node: we must check the instance state and connection
	// direction to step ahead.
	snid := peInst.snid
	idEx := PeerIdEx{Id:peInst.node.ID, Dir:peInst.dir}
	if _, exist := peMgr.workers[snid][idEx]; exist {
		delete(peMgr.workers[snid], idEx)
		peMgr.wrkNum[snid]--
	}

	if peInst.dir == PeInstDirOutbound {
		delete(peMgr.nodes[snid], idEx)
		delete(peMgr.peers, ptn)
		peMgr.obpNum[snid]--
	} else if peInst.dir == PeInstDirInbound {
		delete(peMgr.peers, ptn)
		if _, exist := peMgr.nodes[snid][idEx]; exist {
			delete(peMgr.nodes[snid], idEx)
		}
		peMgr.ibpTotalNum--
		if peInst.state == peInstStateActivated || peInst.state == peInstStateKilling {
			peMgr.ibpNum[snid]--
		}
	}

	peMgr.updateStaticStatus(snid, idEx, peerIdle)

	// resume accepter if necessary
	if peMgr.cfg.noAccept == false &&
		peMgr.ibpTotalNum < peMgr.cfg.ibpNumTotal {
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	// Stop instance task
	peInst.state = peInstStateKilled
	peMgr.sdl.SchStopTask(ptn)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConflictAccessProtect(snid config.SubNetworkID, peer *config.Node, dir int) PeMgrErrno {
	delay := conflictAccessDelayLower + rand.Intn(conflictAccessDelayUpper - conflictAccessDelayLower)
	dur := time.Millisecond * time.Duration(delay)
	idexx := PeerIdExx {
		Snid: snid,
		Node: *peer,
		Dir: dir,
	}

	td := sch.TimerDescription {
		Name:	"_conflictTimer",
		Utid:	sch.PeConflictAccessTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	dur,
		Extra:	&idexx,
	}

	if eno, _ := peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone {
		log.Debug("peMgrConflictAccessProtect: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCatHandler(msg interface{}) PeMgrErrno {
	idexx := msg.(*PeerIdExx)
	schMsg := sch.SchMessage{}
	r := sch.MsgDcvFindNodeRsp{
		Snid:	idexx.Snid,
		Nodes:	[]*config.Node{&idexx.Node},
	}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvDcvFindNodeRsp, &r)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)reconfigTimerHandler() PeMgrErrno {
	peMgr.reCfgTid = sch.SchInvalidTid
	for del := range peMgr.reCfg.delList {
		wks, ok := peMgr.workers[del]
		if !ok {
			continue
		}
		msg := sch.SchMessage{}
		for _, peerInst := range wks {
			req := sch.MsgPeCloseReq {
				Ptn:	peerInst.ptnMe,
				Snid:	peerInst.snid,
				Node:	peerInst.node,
				Dir:	peerInst.dir,
				Why:	msg,
			}
			peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
			peMgr.sdl.SchSendMessage(&msg)
		}
	}
	peMgr.reCfg.delList = make(map[config.SubNetworkID]interface{}, 0)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)shellReconfigReq(msg *sch.MsgShellReconfigReq) PeMgrErrno {
	// notice: if last reconfiguration is not completed, this one would be failed.
	//
	// for sub networks deletion:
	// 1) kill half of the total peer instances;
	// 2) start a timer and when it's expired, kill all outbounds and inbounds;
	// 3) before the timer expired, no inbounds accepted and no outbounds request;
	// 4) clean the randoms for outbound connection;
	// 5) tell discover manager the sub networks changed;
	// for sub networks adding:
	// 1)add each sub network and make all of them ready to play;
	//
	// notice: the same sub network should not be presented in both deleting list
	// and the adding list of "msg" pass to this function.

	if peMgr.reCfgTid != sch.SchInvalidTid {
		log.Debug("shellReconfigReq: previous reconfiguration not completed")
		return PeMgrEnoRecofig
	}

	delList := make([]config.SubNetworkID, 0)
	addList := make([]config.SubNetworkID, 0)
	delList = append(append(delList, msg.VSnidDel...), msg.SnidDel...)
	addList = append(append(addList, msg.VSnidAdd...), msg.SnidAdd...)

	// filter out deleting list
	filterOut := make([]int, 0)
	for idx, del := range delList {
		_, inAdding := peMgr.reCfg.addList[del]
		_, inDeling := peMgr.reCfg.delList[del]
		if !inAdding || inDeling { continue }
		filterOut = append(filterOut, idx)
	}
	for _, fo := range filterOut {
		if fo == len(delList) - 1 {
			delList = delList[0:fo-1]
		} else {
			delList = append(delList[fo:], delList[fo])
		}
	}

	for _, del := range delList {
		peMgr.reCfg.delList[del] = nil
	}

	// filter out adding list
	filterOut = make([]int, 0)
	for idx, add := range addList {
		_, inAdding := peMgr.reCfg.addList[add]
		_, inDeling := peMgr.reCfg.delList[add]
		if inAdding { continue }
		filterOut = append(filterOut, idx)
		if inDeling {
			delete(peMgr.reCfg.delList, add)
		}
	}
	for _, fo := range filterOut {
		if fo == len(addList) - 1 {
			addList = addList[0:fo-1]
		} else {
			addList = append(addList[fo:], addList[fo])
		}
	}

	for _, add := range addList {
		peMgr.reCfg.addList[add] = nil
	}

	// config adding part sub networks
	for _, add := range addList {
		peMgr.nodes[add] = make(map[PeerIdEx]*peerInstance)
		peMgr.workers[add] = make(map[PeerIdEx]*peerInstance)
		peMgr.wrkNum[add] = 0
		peMgr.ibpNum[add] = 0
		peMgr.obpNum[add] = 0
		peMgr.cfg.subNetMaxPeers[add] = config.MaxPeers
		peMgr.cfg.subNetMaxOutbounds[add] = config.MaxOutbounds
		peMgr.cfg.subNetMaxInBounds[add] = config.MaxInbounds
	}

	// kill part of peer instances of each sub network to deleted
	for _, del := range delList {
		wks, ok := peMgr.workers[del]
		if !ok { continue }
		wkNum := len(wks)
		count := 0
		schMsg := sch.SchMessage{}
		for _, peerInst := range wks {
			if count++; count >= wkNum / 2 {
				break
			}
			req := sch.MsgPeCloseReq {
				Ptn:	peerInst.ptnMe,
				Snid:	peerInst.snid,
				Node:	peerInst.node,
				Dir:	peerInst.dir,
				Why:	msg,
			}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
	}

	// start timer for remain peer instances of deleting part
	td := sch.TimerDescription {
		Name:	"_recfgTimer",
		Utid:	sch.PeReconfigTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	reconfigDelay,
		Extra:	nil,
	}
	if eno, tid := peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone {
		log.Debug("shellReconfigReq: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	} else {
		peMgr.reCfgTid = tid
	}

	// cleanup the randoms
	for _, del := range delList {
		if _, ok :=	peMgr.randoms[del]; ok {
			delete(peMgr.randoms, del)
		}
	}

	// tell discover manager that sub networks changed
	peMgr.peMgrRecfg2DcvMgr()

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrRecfg2DcvMgr() PeMgrErrno {
	schMsg := sch.SchMessage{}
	req := sch.MsgDcvReconfigReq {
		DelList: make(map[config.SubNetworkID]interface{}, 0),
		AddList: make(map[config.SubNetworkID]interface{}, 0),
	}
	for del := range peMgr.reCfg.delList {
		req.DelList[del] = nil
	}
	for add := range peMgr.reCfg.addList {
		req.AddList[add] = nil
	}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvReconfigReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrAsk4More(snid *SubNetworkID) PeMgrErrno {
	var timerName = ""
	var eno sch.SchErrno
	var tid int

	if t, ok := peMgr.tmLastFNR[*snid]; ok {
		if time.Now().Sub(t) <= minDuration4FindNodeReq {
			return PeMgrEnoNone
		}
	}

	peMgr.tmLastFNR[*snid] = time.Now()
	dur := durStaticRetryTimer

	if *snid != peMgr.cfg.staticSubNetId {

		dur = durDcvFindNodeTimer
		more := peMgr.cfg.subNetMaxOutbounds[*snid] - peMgr.obpNum[*snid]

		if more <= 0 {
			log.Debug("peMgrAsk4More: no more needed, obpNum: %d, max: %d",
				peMgr.obpNum[*snid],
				peMgr.cfg.subNetMaxOutbounds[*snid])
			return PeMgrEnoNone
		}

		schMsg := sch.SchMessage{}
		req := sch.MsgDcvFindNodeReq{
			Snid:	*snid,
			More:    more,
			Include: nil,
			Exclude: nil,
		}

		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvFindNodeReq, &req)
		peMgr.sdl.SchSendMessage(&schMsg)
		timerName = sch.PeerMgrName + "_DcvFindNode"

		if peMgr.debug__ {
			log.Debug("peMgrAsk4More: "+
				"cfgName: %s, subnet: %x, obpNum: %d, ibpNum: %d, ibpTotalNum: %d, wrkNum: %d, more: %d",
				peMgr.cfg.cfgName,
				*snid,
				peMgr.obpNum[*snid],
				peMgr.ibpNum[*snid],
				peMgr.ibpTotalNum,
				peMgr.wrkNum[*snid],
				more)
		}

	} else {
		timerName = sch.PeerMgrName + "_static"
	}

	// set a ABS timer
	var td = sch.TimerDescription {
		Name:	timerName,
		Utid:	sch.PeDcvFindNodeTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	dur,
		Extra:	snid,
	}

	if tid, ok := peMgr.tidFindNode[*snid]; ok && tid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
		peMgr.tidFindNode[*snid] = sch.SchInvalidTid
	}

	if eno, tid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debug("peMgrAsk4More: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	peMgr.tidFindNode[*snid] = tid

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrIndEnque(ind interface{}) PeMgrErrno {
	if len(peMgr.indChan) >= cap(peMgr.indChan) {
		panic("peMgrIndEnque: system overload")
	}
	if peMgr.debug__ {
		log.Debug("peMgrIndEnque: sdl: %s", peMgr.sdl.SchGetP2pCfgName())
	}
	peMgr.indChan<-ind
	return PeMgrEnoNone
}

//
// Dynamic peer instance task
//
const peInstTaskName = "peInstTsk"
const (
	peInstStateNull		= iota				// null
	peInstStateConnOut						// outbound connection inited
	peInstStateAccepted						// inbound accepted, need handshake
	peInstStateConnected					// outbound connected, need handshake
	peInstStateHandshook					// handshook
	peInstStateActivated					// actived in working
	peInstStateKilling	= -1				// in killing
	peInstStateKilled	= -2				// killed
)

type peerInstState int	// instance state type

const PeInstDirNull			= -1			// null, so connection should be nil
const PeInstDirInbound		= 0				// inbound connection
const PeInstDirOutbound		= 1				// outbound connection

const PeInstMailboxSize 	= 512				// mailbox size
const PeInstMaxP2packages	= 128				// max p2p packages pending to be sent
const PeInstMaxPingpongCnt	= 8					// max pingpong counter value
const PeInstPingpongCycle	= time.Second * 16	// pingpong period

type peerInstance struct {
	debug__		bool						// if in debuging
	sdl			*sch.Scheduler				// pointer to scheduler

	// Notice !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// We backup the pointer to peer manager here to access it while an instance
	// is running, this might bring us those problems in "SYNC" between instances
	// and the peer manager.
	peMgr		*PeerManager				// pointer to peer manager

	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	ptnMe		interface{}					// the instance task node pointer
	ptnMgr		interface{}					// the peer manager task node pointer
	state		peerInstState				// state
	cto			time.Duration				// connect timeout value
	hto			time.Duration				// handshake timeout value
	ato			time.Duration				// active peer connection read/write timeout value
	dialer		*net.Dialer					// dialer to make outbound connection
	conn		net.Conn					// connection
	iow			ggio.WriteCloser			// IO writer
	ior			ggio.ReadCloser				// IO reader
	laddr		*net.TCPAddr				// local ip address
	raddr		*net.TCPAddr				// remote ip address
	dir			int							// direction: outbound(+1) or inbound(-1)
	snid		config.SubNetworkID			// sub network identity
	node		config.Node					// peer "node" information
	protoNum	uint32						// peer protocol number
	protocols	[]Protocol					// peer protocol table
	maxPkgSize	int							// max size of tcpmsg package
	ppTid		int							// pingpong timer identity
	rxChan		chan *P2pPackageRx			// rx pending channel
	txChan		chan *P2pPackage			// tx pending channel
	txPendNum	int							// tx pending number
	txSeq		int64						// statistics sequence number
	txOkCnt		int64						// tx ok counter
	txFailedCnt	int64						// tx failed counter
	rxDone		chan PeMgrErrno				// RX chan
	rxtxRuning	bool						// indicating that rx and tx routines are running
	ppSeq		uint64						// pingpong sequence no.
	ppCnt		int							// pingpong counter
	rxEno		PeMgrErrno					// rx errno
	txEno		PeMgrErrno					// tx errno
	ppEno		PeMgrErrno					// pingpong errno
}

var peerInstDefault = peerInstance {
	name:		peInstTaskName,
	state:		peInstStateNull,
	cto:		0,
	hto:		0,
	ato:		0,
	dir:		PeInstDirNull,
	node:		config.Node{},
	maxPkgSize:	maxTcpmsgSize,
	protoNum:	0,
	protocols:	[]Protocol{{}},
	ppTid:		sch.SchInvalidTid,
	ppSeq:		0,
	ppCnt:		0,
	rxEno:		PeMgrEnoNone,
	txEno:		PeMgrEnoNone,
	ppEno:		PeMgrEnoNone,
}

type msgConnOutRsp struct {
	result	PeMgrErrno				// result of outbound connect action
	snid	config.SubNetworkID		// sub network identity
	peNode 	*config.Node			// target node
	ptn		interface{}				// pointer to task instance node of sender
}

type msgHandshakeRsp struct {
	result	PeMgrErrno				// result of handshake action
	dir		int						// inbound or outbound
	snid	config.SubNetworkID		// sub network identity
	peNode 	*config.Node			// target node
	ptn		interface{}				// pointer to task instance node of sender
}

type msgPingpongRsp struct {
	result	PeMgrErrno				// result of pingpong action
	dir		int						// direction
	peNode 	*config.Node			// target node
	ptn		interface{}				// pointer to task instance node of sender
}

type MsgCloseCfm struct {
	result	PeMgrErrno				// result of pingpong action
	dir		int						// direction
	snid	config.SubNetworkID		// sub network identity
	peNode 	*config.Node			// target node
	ptn		interface{}				// pointer to task instance node of sender
}

type MsgCloseInd struct {
	cause	PeMgrErrno				// tell why it's closed
	dir		int						// direction
	snid	config.SubNetworkID		// sub network identity
	peNode 	*config.Node			// target node
	ptn		interface{}				// pointer to task instance node of sender
}

type MsgPingpongReq struct {
	seq		uint64					// init sequence no.
}

func (pi *peerInstance)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return pi.tep(ptn, msg)
}

func (pi *peerInstance)peerInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	if pi.debug__ && pi.sdl != nil {
		sdl := pi.sdl.SchGetP2pCfgName()
		log.Debug("ngbProtoProc: sdl: %s, ngbMgr.name: %s, msg.Id: %d", sdl, pi.name, msg.Id)
	}

	var eno PeMgrErrno

	switch msg.Id {
	case sch.EvSchPoweroff:
		eno = pi.piPoweroff(ptn)

	case sch.EvPeConnOutReq:
		eno = pi.piConnOutReq(msg.Body)

	case sch.EvPeHandshakeReq:
		eno = pi.piHandshakeReq(msg.Body)

	case sch.EvPePingpongReq:
		eno = pi.piPingpongReq(msg.Body)

	case sch.EvPeCloseReq:
		eno = pi.piCloseReq(msg.Body)

	case sch.EvPeEstablishedInd:
		eno = pi.piEstablishedInd(msg.Body)

	case sch.EvPePingpongTimer:
		eno = pi.piPingpongTimerHandler()

	case sch.EvPeTxDataReq:
		eno = pi.piTxDataReq(msg.Body)

	case sch.EvPeRxDataInd:
		eno = pi.piRxDataInd(msg.Body)

	default:
		log.Debug("PeerInstProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if eno != PeMgrEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (pi *peerInstance)piPoweroff(ptn interface{}) PeMgrErrno {
	sdl := pi.sdl.SchGetP2pCfgName()
	if pi.state == peInstStateKilling {
		log.Debug("piPoweroff: already in killing, done at once, sdl: %s, name: %s",
			sdl, pi.sdl.SchGetTaskName(pi.ptnMe))

		if pi.sdl.SchTaskDone(pi.ptnMe, sch.SchEnoKilled) != sch.SchEnoNone {
			return PeMgrEnoScheduler
		}

		return PeMgrEnoNone
	}
	log.Debug("piPoweroff: task will be done, sdl: %s, name: %s, state: %d",
		sdl, pi.sdl.SchGetTaskName(pi.ptnMe), pi.state)

	if pi.rxtxRuning {
		if pi.txChan != nil {
			close(pi.txChan)
		}

		if pi.rxDone != nil {
			pi.rxDone <- PeMgrEnoNone
			<-pi.rxDone
		}

		if pi.rxChan != nil {
			close(pi.rxChan)
		}
	}

	if pi.conn != nil {
		pi.conn.Close()
		pi.conn = nil
	}

	pi.state = peInstStateKilled
	pi.rxtxRuning = false

	if pi.sdl.SchTaskDone(pi.ptnMe, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (pi *peerInstance)piConnOutReq(_ interface{}) PeMgrErrno {
	if pi.dialer == nil ||
		pi.dir != PeInstDirOutbound  ||
		pi.state != peInstStateConnOut {
		log.Debug("piConnOutReq: instance mismatched, sdl: %s, pi: %s", pi.sdl.SchGetP2pCfgName(), pi.name)
		return PeMgrEnoInternal
	}

	var addr = &net.TCPAddr{IP: pi.node.IP, Port: int(pi.node.TCP)}
	var conn net.Conn = nil
	var err error
	var eno PeMgrErrno = PeMgrEnoNone

	pi.dialer.Timeout = pi.cto

	if conn, err = pi.dialer.Dial("tcp", addr.String()); err != nil {
		if pi.debug__ {
			// Notice "local" not the address used to connect to peer but the address listened in local
			log.Debug("piConnOutReq: dial failed, local: %s, to: %s, err: %s",
				fmt.Sprintf("%s:%d", pi.peMgr.cfg.ip.String(), pi.peMgr.cfg.port),
				addr.String(), err.Error())
		}
		eno = PeMgrEnoOs
	} else {
		pi.conn = conn
		pi.laddr = conn.LocalAddr().(*net.TCPAddr)
		pi.raddr = conn.RemoteAddr().(*net.TCPAddr)
		pi.state = peInstStateConnected

		if pi.debug__ {
			log.Debug("piConnOutReq: dial ok, laddr: %s, raddr: %s",
				pi.laddr.String(),
				pi.raddr.String())
		}
	}

	var schMsg = sch.SchMessage{}
	var rsp = msgConnOutRsp {
		result:	eno,
		snid:	pi.snid,
		peNode:	&pi.node,
		ptn:	pi.ptnMe,
	}

	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeConnOutRsp, &rsp)
	pi.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *peerInstance)piHandshakeReq(_ interface{}) PeMgrErrno {
	if pi == nil {
		log.Debug("piHandshakeReq: invalid instance")
		return PeMgrEnoParameter
	}

	if pi.state != peInstStateConnected && pi.state != peInstStateAccepted {
		log.Debug("piHandshakeReq: instance mismatched")
		return PeMgrEnoInternal
	}

	if pi.conn == nil {
		log.Debug("piHandshakeReq: invalid instance")
		return PeMgrEnoInternal
	}

	// Carry out action according to the direction of current peer instance connection.
	var eno PeMgrErrno
	if pi.dir == PeInstDirInbound {
		eno = pi.piHandshakeInbound(pi)
	} else if pi.dir == PeInstDirOutbound {
		eno = pi.piHandshakeOutbound(pi)
	} else {
		log.Debug("piHandshakeReq: invalid instance direction: %d", pi.dir)
		eno = PeMgrEnoInternal
	}

	if pi.debug__ {
		log.Debug("piHandshakeReq: handshake result: %d, dir: %d, laddr: %s, raddr: %s, peer: %s",
					eno,
					pi.dir,
					pi.laddr.String(),
					pi.raddr.String(),
					fmt.Sprintf("%+v", pi.node))
	}

	var rsp = msgHandshakeRsp {
		result:	eno,
		dir:	pi.dir,
		snid:	pi.snid,
		peNode:	&pi.node,
		ptn:	pi.ptnMe,
	}

	var schMsg = sch.SchMessage{}
	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeHandshakeRsp, &rsp)
	pi.sdl.SchSendMessage(&schMsg)

	return eno
}

func (pi *peerInstance)piPingpongReq(msg interface{}) PeMgrErrno {
	if pi.ppEno != PeMgrEnoNone {
		log.Debug("piPingpongReq: nothing done, ppEno: %d", pi.ppEno)
		return PeMgrEnoResource
	}

	if pi.conn == nil {
		log.Debug("piPingpongReq: connection had been closed")
		return PeMgrEnoResource
	}

	pi.ppSeq = msg.(*MsgPingpongReq).seq
	ping := Pingpong {
		Seq:	pi.ppSeq,
		Extra:	nil,
	}
	pi.ppSeq++

	upkg := new(P2pPackage)
	if eno := upkg.ping(pi, &ping); eno != PeMgrEnoNone {
		pi.ppEno = eno
		i := P2pIndConnStatusPara {
			Ptn:		pi.ptnMe,
			PeerInfo:	&Handshake{
				Snid:      pi.snid,
				NodeId:    pi.node.ID,
				ProtoNum:  pi.protoNum,
				Protocols: pi.protocols,
			},
			Status		:	int(eno),
			Flag		:	false,
			Description	:	"piPingpongReq: failed",
		}

		req := sch.MsgPeCloseReq {
			Ptn: pi.ptnMe,
			Snid: pi.snid,
			Node: pi.node,
			Dir: pi.dir,
			Why: &i,
		}

		msg := sch.SchMessage{}
		pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
		pi.sdl.SchSendMessage(&msg)

		return eno
	}

	return PeMgrEnoNone
}

func (pi *peerInstance)piCloseReq(_ interface{}) PeMgrErrno {
	sdl := pi.sdl.SchGetP2pCfgName()
	if pi.state == peInstStateKilling {
		log.Debug("piCloseReq: already in killing, sdl: %s, task: %s",
			sdl, pi.sdl.SchGetTaskName(pi.ptnMe))
		return PeMgrEnoDuplicated
	}

	pi.state = peInstStateKilling
	node := pi.node

	if pi.rxtxRuning {
		if pi.txChan != nil {
			close(pi.txChan)
		}
		pi.rxDone <- PeMgrEnoNone
		<-pi.rxDone
		if pi.rxChan != nil {
			close(pi.rxChan)
		}
		pi.rxtxRuning = false
	}

	cfm := MsgCloseCfm {
		result: PeMgrEnoNone,
		dir:	pi.dir,
		snid:	pi.snid,
		peNode:	&node,
		ptn:	pi.ptnMe,
	}

	peMgr := pi.peMgr
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, pi.ptnMe, peMgr.ptnMe, sch.EvPeCloseCfm, &cfm)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *peerInstance)piEstablishedInd(msg interface{}) PeMgrErrno {
	cfmCh := *msg.(*chan int)
	sdl := pi.sdl.SchGetP2pCfgName()

	var schEno sch.SchErrno
	var tid int
	var tmDesc = sch.TimerDescription {
		Name:  sch.PeerMgrName + "_PePingpong",
		Utid:  sch.PePingpongTimerId,
		Tmt:   sch.SchTmTypePeriod,
		Dur:   PeInstPingpongCycle,
		Extra: nil,
	}

	if schEno, tid = pi.sdl.SchSetTimer(pi.ptnMe, &tmDesc);
		schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debug("piEstablishedInd: SchSetTimer failed, sdl: %s, pi: %s, eno: %d",
			sdl, pi.name, schEno)
		cfmCh<-PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	pi.ppTid = tid
	pi.txEno = PeMgrEnoNone
	pi.rxEno = PeMgrEnoNone
	pi.ppEno = PeMgrEnoNone

	if err := pi.conn.SetDeadline(time.Time{}); err != nil {
		log.Debug("piEstablishedInd: SetDeadline failed, error: %s", err.Error())
		msg := sch.SchMessage{}
		req := sch.MsgPeCloseReq{
			Ptn: pi.ptnMe,
			Snid: pi.snid,
			Node: config.Node {
				ID: pi.node.ID,
			},
			Dir: pi.dir,
		}
		pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
		pi.sdl.SchSendMessage(&msg)
		cfmCh<-PeMgrEnoOs
		return PeMgrEnoOs
	}

	go piTx(pi)
	go piRx(pi)

	pi.rxtxRuning = true
	cfmCh<-PeMgrEnoNone

	return PeMgrEnoNone
}

func (pi *peerInstance)piPingpongTimerHandler() PeMgrErrno {
	msg := sch.SchMessage{}
	if pi.ppCnt++; pi.ppCnt > PeInstMaxPingpongCnt {
		pi.ppEno = PeMgrEnoPingpongTh

		i := P2pIndConnStatusPara {
			Ptn:		pi.ptnMe,
			PeerInfo:	&Handshake {
				Snid:		pi.snid,
				NodeId:		pi.node.ID,
				ProtoNum:	pi.protoNum,
				Protocols:	pi.protocols,
			},
			Status		:	PeMgrEnoPingpongTh,
			Flag		:	false,
			Description	:	"piPingpongTimerHandler: threshold reached",
		}

		req := sch.MsgPeCloseReq {
			Ptn: pi.ptnMe,
			Snid: pi.snid,
			Node: pi.node,
			Dir: pi.dir,
			Why: &i,
		}

		pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
		pi.sdl.SchSendMessage(&msg)

		return pi.ppEno
	}

	pr := MsgPingpongReq {
		seq: uint64(time.Now().UnixNano()),
	}

	pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMe, sch.EvPePingpongReq, &pr)
	pi.sdl.SchSendMessage(&msg)

	return PeMgrEnoNone
}

func (pi *peerInstance)piTxDataReq(_ interface{}) PeMgrErrno {
	// not applied
	return PeMgrEnoMismatched
}

func (pi *peerInstance)piRxDataInd(msg interface{}) PeMgrErrno {
	return pi.piP2pPkgProc(msg.(*P2pPackage))
}

func (pi *peerInstance)piHandshakeInbound(inst *peerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs *Handshake

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		if pi.debug__ {
			log.Debug("piHandshakeInbound: read inbound Handshake message failed, eno: %d", eno)
		}
		return eno
	}

	if inst.peMgr.dynamicSubNetIdExist(&hs.Snid) == false &&
		inst.peMgr.staticSubNetIdExist(&hs.Snid) == false {
		log.Debug("piHandshakeInbound: local node does not attach to subnet: %x", hs.Snid)
		return PeMgrEnoNotfound
	}

	// backup info about protocols supported by peer. notice that here we can
	// check against the ip and tcp port from handshake with that obtained from
	// underlying network, but we not now.
	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	inst.snid = hs.Snid
	inst.node.ID = hs.NodeId
	inst.node.IP = append(inst.node.IP, hs.IP...)
	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)

	// write outbound handshake to remote peer
	hs.Snid = inst.snid
	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = pi.peMgr.cfg.protocols
	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {
		if pi.debug__ {
			log.Debug("piHandshakeInbound: write outbound Handshake message failed, eno: %d", eno)
		}
		return eno
	}

	inst.state = peInstStateHandshook

	return PeMgrEnoNone
}

func (pi *peerInstance)piHandshakeOutbound(inst *peerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs = new(Handshake)

	// write outbound handshake to remote peer
	hs.Snid = pi.snid
	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = append(hs.Protocols, pi.peMgr.cfg.protocols ...)

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {
		if pi.debug__ {
			log.Debug("piHandshakeOutbound: write outbound Handshake message failed, eno: %d", eno)
		}
		return eno
	}

	// read inbound handshake from remote peer
	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		if pi.debug__ {
			log.Debug("piHandshakeOutbound: read inbound Handshake message failed, eno: %d", eno)
		}
		return eno
	}

	// check sub network identity
	if hs.Snid != inst.snid {
		log.Debug("piHandshakeOutbound: subnet identity mismathced")
		return PeMgrEnoMessage
	}

	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos,
	// and we update the remains.
	if hs.NodeId != inst.node.ID {
		log.Debug("piHandshakeOutbound: node identity mismathced")
		return PeMgrEnoMessage
	}

	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)
	inst.node.IP = append(inst.node.IP, hs.IP ...)

	// backup info about protocols supported by peer;
	// update instance state;
	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	inst.state = peInstStateHandshook

	return PeMgrEnoNone
}

func SendPackage(pkg *P2pPackage2Peer) (PeMgrErrno){
	// this function exported for user to send messages to specific peers, please
	// notice that it plays with the "messaging" based on scheduler. it's not the
	// only method to send messages, since active peers are backup in shell manager
	// of chain, see function shellManager.broadcastReq for details please.
	if len(pkg.IdList) == 0 {
		log.Debug("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}
	pem := pkg.P2pInst.SchGetTaskObject(sch.PeerMgrName)
	if pem == nil {
		if sch.Debug__ {
			log.Debug("SendPackage: nil peer manager, might be in power off stage")
		}
		return PeMgrEnoNotfound
	}
	peMgr := pem.(*PeerManager)
	for _, pid := range pkg.IdList {
		_pkg := new(P2pPackage)
		_pkg.Pid = uint32(pkg.ProtoId)
		_pkg.Mid = uint32(pkg.Mid)
		_pkg.Key = pkg.Key
		_pkg.PayloadLength = uint32(pkg.PayloadLength)
		_pkg.Payload = append(_pkg.Payload, pkg.Payload...)
		req := sch.MsgPeDataReq {
			SubNetId: pkg.SubNetId,
			PeerId: pid,
			Pkg: _pkg,
		}
		msg := sch.SchMessage{}
		pkg.P2pInst.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeTxDataReq, &req)
		pkg.P2pInst.SchSendMessage(&msg)
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)ClosePeer(snid *SubNetworkID, id *PeerId) PeMgrErrno {
	idExOut := PeerIdEx{Id: *id, Dir: PeInstDirOutbound}
	idExIn := PeerIdEx{Id: *id, Dir: PeInstDirInbound}
	idExList := []PeerIdEx{idExOut, idExIn}
	for _, idEx := range idExList {
		var req = sch.MsgPeCloseReq{
			Ptn: nil,
			Snid: *snid,
			Node: config.Node {
				ID: *id,
			},
			Dir: idEx.Dir,
		}
		var schMsg= sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
		peMgr.sdl.SchSendMessage(&schMsg)
	}
	return PeMgrEnoNone
}

func piTx(pi *peerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit for "txChan" closed.
	sdl := pi.sdl.SchGetP2pCfgName()

txLoop:
	for {
		upkg, ok := <-pi.txChan
		if !ok {
			break txLoop
		}
		pi.txPendNum -= 1
		pi.txSeq += 1
		// carry out Tx
		if eno := upkg.SendPackage(pi); eno == PeMgrEnoNone {
			pi.txOkCnt += 1
		} else {
			// 1) if failed, callback to the user, so he can close this peer seems in troubles,
			// we will be done then.
			// 2) it is possible that, while we are blocked here in writing and the connection
			// is closed for some reasons(for example the user close the peer), in this case,
			// we would get an error.
			pi.txFailedCnt += 1
			pi.txEno = eno
			hs := Handshake {
				Snid:		pi.snid,
				NodeId:		pi.node.ID,
				ProtoNum:	pi.protoNum,
				Protocols:	pi.protocols,
			}
			i := P2pIndConnStatusPara{
				Ptn:		pi.ptnMe,
				PeerInfo:	&hs,
				Status:		int(eno),
				Flag:		false,
				Description:"piTx: SendPackage failed",
			}
			req := sch.MsgPeCloseReq {
				Ptn: pi.ptnMe,
				Snid: pi.snid,
				Node: pi.node,
				Dir: pi.dir,
				Why: &i,
			}
			// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning of
			// this instance, BUT at this moment, the message queue of peer manager might
			// be FULL, so the instance would be blocked while sending; AND the peer manager
			// might had fired pi.txDone and been blocked by pi.txExit. panic is called
			// for such a overload system, see scheduler please.
			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
			pi.sdl.SchSendMessage(&msg)

			log.Debug("piTx: failed, EvPeCloseReq sent. sdl: %s, snid: %x, dir: %d, peer: %x",
				sdl, pi.snid, pi.dir, pi.node.ID)
		}

		if pi.txSeq & 0x0f == 0 {
			log.Debug("piTx: txSeq: %d, txOkCnt: %d, txFailedCnt: %d, sent. sdl: %s, snid: %x, dir: %d, peer: %x",
				pi.txSeq, pi.txOkCnt, pi.txFailedCnt, sdl, pi.snid, pi.dir, pi.node.ID)
		}
	}
	return PeMgrEnoNone
}

func piRx(pi *peerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit.
	sdl := pi.sdl.SchGetP2pCfgName()
	var done PeMgrErrno = PeMgrEnoNone
	var ok = true
	var peerInfo = PeerInfo{}
	var pkgCb = P2pPackageRx{}

rxLoop:
	for {
		// check if we are done
		select {
		case done, ok = <-pi.rxDone:
			if pi.debug__ {
				log.Debug("piRx: sdl: %s, pi: %s, done with: %d", sdl, pi.name, done)
			}

			if ok {
				close(pi.rxDone)
			}

			break rxLoop

		default:
		}

		// try reading the peer
		if pi.rxEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}

		upkg := new(P2pPackage)
		if eno := upkg.RecvPackage(pi); eno != PeMgrEnoNone {

			// 1) if failed, callback to the user, so he can close this peer seems in troubles,
			// we will be done then.
			// 2) it is possible that, while we are blocked here in reading and the connection
			// is closed for some reasons(for example the user close the peer), in this case,
			// we would get an error.

			pi.rxEno = eno
			hs := Handshake {
				Snid:		pi.snid,
				NodeId:		pi.node.ID,
				ProtoNum:	pi.protoNum,
				Protocols:	pi.protocols,
			}

			i := P2pIndConnStatusPara{
				Ptn:		pi.ptnMe,
				PeerInfo:	&hs,
				Status:		int(eno),
				Flag:		false,
				Description:"piRx: RecvPackage failed",
			}

			req := sch.MsgPeCloseReq {
				Ptn: pi.ptnMe,
				Snid: pi.snid,
				Node: pi.node,
				Dir: pi.dir,
				Why: &i,
			}

			// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning
			// this instance, BUT at this moment, the message queue of peer manager might
			// be FULL, so the instance would be blocked while sending; AND the peer manager
			// might had fired pi.txDone and been blocked by pi.txExit. panic is called
			// for such a overload system, see scheduler please.

			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
			pi.sdl.SchSendMessage(&msg)
			continue
		}

		if upkg.Pid == uint32(PID_P2P) {
			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMe, sch.EvPeRxDataInd, upkg)
			pi.sdl.SchSendMessage(&msg)
		} else if upkg.Pid == uint32(PID_EXT) {
			if len(pi.rxChan) >= cap(pi.rxChan) {
				log.Debug("piRx: rx queue full, sdl: %s, snid: %x, dir: %d, pi: %s, peer: %x",
					sdl, pi.snid, pi.dir, pi.name, pi.node.ID)
			} else {
				peerInfo.Protocols = nil
				peerInfo.Snid = pi.snid
				peerInfo.NodeId = pi.node.ID
				peerInfo.ProtoNum = pi.protoNum
				peerInfo.Protocols = append(peerInfo.Protocols, pi.protocols...)
				pkgCb.Ptn = pi.ptnMe
				pkgCb.Payload = nil
				pkgCb.PeerInfo = &peerInfo
				pkgCb.ProtoId = int(upkg.Pid)
				pkgCb.MsgId = int(upkg.Mid)
				pkgCb.PayloadLength = int(upkg.PayloadLength)
				pkgCb.Payload = append(pkgCb.Payload, upkg.Payload...)
				pi.rxChan <- &pkgCb
			}
		} else {
			log.Debug("piRx: package discarded for unknown pid: sdl: %s, pi: %s, %d",
				 sdl, pi.name, upkg.Pid)
		}
	}

	return done
}

func (pi *peerInstance)piP2pPkgProc(upkg *P2pPackage) PeMgrErrno {
	if upkg.Pid != uint32(PID_P2P) {
		log.Debug("piP2pPkgProc: not a p2p package, pid: %d", upkg.Pid)
		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {
		log.Debug("piP2pPkgProc: invalid payload length: %d", upkg.PayloadLength)
		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {
		log.Debug("piP2pPkgProc: payload length mismatched, PlLen: %d, real: %d",
			upkg.PayloadLength, len(upkg.Payload))
		return PeMgrEnoMessage
	}

	msg := P2pMessage{}
	if eno := upkg.GetMessage(&msg); eno != PeMgrEnoNone {
		log.Debug("piP2pPkgProc: GetMessage failed, eno: %d", eno	)
		return eno
	}

	// check message identity. we discard any handshake messages received here
	// since handshake procedure had been passed, and dynamic handshake is not
	// supported currently.

	switch msg.Mid {

	case uint32(MID_HANDSHAKE):
		log.Debug("piP2pPkgProc: MID_HANDSHAKE, discarded")
		return PeMgrEnoMessage

	case uint32(MID_PING):
		return pi.piP2pPingProc(msg.Ping)

	case uint32(MID_PONG):
		return pi.piP2pPongProc(msg.Pong)

	default:
		log.Debug("piP2pPkgProc: unknown mid: %d", msg.Mid)
		return PeMgrEnoMessage
	}

	return PeMgrEnoUnknown
}

func (pi *peerInstance)piP2pPingProc(ping *Pingpong) PeMgrErrno {
	upkg := new(P2pPackage)
	pong := Pingpong {
		Seq:	ping.Seq,
		Extra:	nil,
	}
	pi.ppCnt = 0
	if eno := upkg.pong(pi, &pong); eno != PeMgrEnoNone {
		log.Debug("piP2pPingProc: pong failed, eno: %d, pi: %s",
			eno, fmt.Sprintf("%+v", *pi))
		return eno
	}
	return PeMgrEnoNone
}

func (pi *peerInstance)piP2pPongProc(pong *Pingpong) PeMgrErrno {
	// Currently, the heartbeat checking does not apply pong message from
	// peer, instead, a counter for ping messages and a timer are invoked,
	// see it please. We just simply debug out the pong message here.
	// A more better method is to check the sequences of the pong message
	// against those of ping messages had been set, and then send evnet
	// EvPePingpongRsp to peer manager. The event EvPePingpongRsp is not
	// applied currently. We leave this work later.
	return PeMgrEnoNone
}

func (pis peerInstState) compare(s peerInstState) int {
	// See definition about peerInstState pls.
	if pis < 0 {
		panic(fmt.Sprintf("compare: exception, pis: %d", pis))
	}
	return int(pis - s)
}

func (peMgr *PeerManager)updateStaticStatus(snid SubNetworkID, idEx PeerIdEx, status int) {
	if snid == peMgr.cfg.staticSubNetId {
		if _, static := peMgr.staticsStatus[idEx]; static == true {
			peMgr.staticsStatus[idEx] = status
		}
	}
}
func (peMgr *PeerManager)dynamicSubNetIdExist(snid *SubNetworkID) bool {
	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		for _, id := range peMgr.cfg.subNetIdList {
			if id == *snid {
				return true
			}
		}
	}
	return false
}

func (peMgr *PeerManager)staticSubNetIdExist(snid *SubNetworkID) bool {
	if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		return peMgr.cfg.staticSubNetId == *snid
	} else if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		return len(peMgr.cfg.staticNodes) > 0 && peMgr.cfg.staticSubNetId == *snid
	}
	return false
}

func (peMgr *PeerManager)isStaticSubNetId(snid SubNetworkID) bool {
	return	(peMgr.cfg.networkType == config.P2pNetworkTypeStatic &&
		peMgr.staticSubNetIdExist(&snid) == true) ||
		(peMgr.cfg.networkType == config.P2pNetworkTypeDynamic &&
			peMgr.staticSubNetIdExist(&snid) == true)
}

func (peMgr *PeerManager) getWorkerInst(snid SubNetworkID, idEx *PeerIdEx) *peerInstance {
	return peMgr.workers[snid][*idEx]
}

func (peMgr *PeerManager)GetInstIndChannel() chan interface{} {
	// This function implements the "Channel" schema to hand up the indications
	// from peer instances to higher module. After this function called, the caller
	// can then go a routine to pull indications from the channel returned.
	return peMgr.indChan
}

func (peMgr *PeerManager)RegisterInstIndCallback(cb interface{}, userData interface{}) PeMgrErrno {
	// This function implements the "Callback" schema to hand up the indications
	// from peer instances to higher module. In this schema, a routine is started
	// in this function to pull indications, check what indication type it is and
	// call the function registered.
	if peMgr.ptnShell != nil {
		log.Debug("RegisterInstIndCallback: register failed for shell task in running")
		return PeMgrEnoMismatched
	}

	if peMgr.indCb != nil {
		log.Debug("RegisterInstIndCallback: callback duplicated")
		return PeMgrEnoDuplicated
	}

	if cb == nil {
		log.Debug("RegisterInstIndCallback: try to register nil callback")
		return PeMgrEnoParameter
	}

	icb, ok := cb.(P2pIndCallback)
	if !ok {
		log.Debug("RegisterInstIndCallback: invalid callback interface")
		return PeMgrEnoParameter
	}

	peMgr.indCb = icb
	peMgr.indCbUserData = userData

	go func() {
		for {
			select {
			case ind, ok := <-peMgr.indChan:
				if !ok  {
					log.Debug("P2pIndCallback: indication channel closed, done")
					return
				}
				indType := reflect.TypeOf(ind).Elem().Name()
				switch indType {
				case "P2pIndPeerActivatedPara":
					peMgr.indCb(P2pIndPeerActivated, ind, peMgr.indCbUserData)
				case "P2pIndPeerClosedPara":
					peMgr.indCb(P2pIndPeerClosed, ind, peMgr.indCbUserData)
				default:
					log.Debug("P2pIndCallback: discard unknown indication type: %s", indType)
				}
			}
		}
	}()

	return PeMgrEnoNone
}
