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
	"sync"
	"math/rand"
	"container/list"
	ggio 	"github.com/gogo/protobuf/io"
	config	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	log		"github.com/yeeco/gyee/p2p/logger"
)

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
	PeMgrEnoUnknown
)

type PeMgrErrno int

// Peer identity as string
type PeerId = config.NodeID

// Peer information
type PeerInfo Handshake

// Peer manager configuration
const (
	defaultConnectTimeout = 15 * time.Second		// default dial outbound timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

	defaultHandshakeTimeout = 8 * time.Second		// default handshake timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

	defaultActivePeerTimeout = 15 * time.Second		// default read/write operation timeout after a peer
													// connection is activaged in working.
	maxTcpmsgSize = 1024*1024*4						// max size of a tcpmsg package could be, currently
													// it's a fixed value here than can be configurated
													// by other module.

	durDcvFindNodeTimer = time.Second * 20			// duration to wait for find node response from discover task,
													// should be (findNodeExpiration + delta).

	durStaticRetryTimer = time.Second * 4			// duration to check and retry connect to static peers

	maxIndicationQueueSize = 256					// max indication queue size
)

const (
	peerIdle			= iota						// idle
	peerConnectOutInited							// connecting out inited
	peerActivated									// had been activated
	peerKilling										// in killing
)

type SubNetworkID = config.SubNetworkID

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
const PeerMgrName = sch.PeerMgrName
type PeerIdEx struct {
	Id				config.NodeID					// node identity
	Dir				int								// direction
}
type PeerManager struct {
	sdl				*sch.Scheduler					// pointer to scheduler
	name			string							// name
	inited			chan PeMgrErrno					// result of initialization
	tep				sch.SchUserTaskEp				// entry
	cfg				peMgrConfig						// configuration
	tidFindNode		map[SubNetworkID]int			// find node timer identity
	ptnMe			interface{}						// pointer to myself(peer manager task node)
	ptnTab			interface{}						// pointer to table task node
	ptnLsn			interface{}						// pointer to peer listener manager task node
	ptnAcp			interface{}						// pointer to peer acceptor manager task node
	ptnDcv			interface{}						// pointer to discover task node
	tabMgr			*tab.TableManager				// pointer to table manager
	accepter		*acceptTskCtrlBlock				// pointer to accepter
	ibInstSeq		int								// inbound instance seqence number
	obInstSeq		int								// outbound instance seqence number
	peers			map[interface{}]*peerInstance	// map peer instance's task node pointer to instance pointer
	nodes			map[SubNetworkID]map[PeerIdEx]*peerInstance	// map peer node identity to instance pointer
	workers			map[SubNetworkID]map[PeerIdEx]*peerInstance	// map peer node identity to pointer of instance in work
	wrkNum			map[SubNetworkID]int			// worker peer number
	ibpNum			map[SubNetworkID]int			// active inbound peer number
	obpNum			map[SubNetworkID]int			// active outbound peer number
	ibpTotalNum		int								// total active inbound peer number
	acceptPaused	bool							// if accept task paused
	randoms			map[SubNetworkID][]*config.Node	// random nodes found by discover
	indChan			chan interface{}				// indication signal
	indQueue		*list.List						// indication queue
	indQueSize		int								// indication queue max size
	indDiscarded	int								// number of indication discarded
	ssTid			int								// statistics timer identity
	staticsStatus	map[PeerIdEx]int				// status about static nodes
}

func NewPeerMgr() *PeerManager {
	var peMgr = PeerManager{
		name:         	PeerMgrName,
		inited:       	make(chan PeMgrErrno),
		cfg:          	peMgrConfig{},
		tidFindNode:  	map[SubNetworkID]int{},
		peers:        	map[interface{}]*peerInstance{},
		nodes:        	map[SubNetworkID]map[PeerIdEx]*peerInstance{},
		workers:      	map[SubNetworkID]map[PeerIdEx]*peerInstance{},
		wrkNum:       	map[SubNetworkID]int{},
		ibpNum:       	map[SubNetworkID]int{},
		obpNum:       	map[SubNetworkID]int{},
		ibpTotalNum:	0,
		acceptPaused: 	false,
		indChan:		make(chan interface{}),
		indQueue:		list.New(),
		indQueSize:		maxIndicationQueueSize,
		indDiscarded:	0,
		randoms:      	map[SubNetworkID][]*config.Node{},
		staticsStatus:	map[PeerIdEx]int{},
	}
	peMgr.tep = peMgr.peerMgrProc
	return &peMgr
}

func (peMgr *PeerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return peMgr.tep(ptn, msg)
}

func (peMgr *PeerManager)peerMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if peMgr.sdl != nil {
		sdl := peMgr.sdl.SchGetP2pCfgName()
		log.LogCallerFileLine("peerMgrProc: enter, sdl: %s, name: %s, msg.Id: %d, sender: %s",
			sdl, peMgr.name, msg.Id, peMgr.sdl.SchGetTaskName(peMgr.sdl.SchGetSender(msg)))
	}

	var schEno = sch.SchEnoNone
	var eno PeMgrErrno = PeMgrEnoNone

	switch msg.Id {
	case sch.EvSchPoweron:
		eno = peMgr.peMgrPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = peMgr.peMgrPoweroff(ptn)
	case sch.EvPeTestStatTimer:
		peMgr.logPeerStat()
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
		eno = peMgr.peMgrCloseReq(msg.Body)
	case sch.EvPeCloseCfm:
		eno = peMgr.peMgrConnCloseCfm(msg.Body)
	case sch.EvPeCloseInd:
		eno = peMgr.peMgrConnCloseInd(msg.Body)
	case sch.EvPeDataReq:
		eno = peMgr.peMgrDataReq(msg.Body)
	default:
		log.LogCallerFileLine("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if eno != PeMgrEnoNone {
		schEno = sch.SchEnoUserTask
	}

	if peMgr.sdl != nil {
		sdl := peMgr.sdl.SchGetP2pCfgName()
		log.LogCallerFileLine("peerMgrProc: exit, sdl: %s, name: %s, msg.Id: %d, sender: %s",
			sdl, peMgr.name, msg.Id, peMgr.sdl.SchGetTaskName(peMgr.sdl.SchGetSender(msg)))
	}

	return schEno
}

func (peMgr *PeerManager)peMgrPoweron(ptn interface{}) PeMgrErrno {
	peMgr.ptnMe	= ptn
	peMgr.sdl = sch.SchGetScheduler(ptn)
	_, peMgr.ptnLsn = peMgr.sdl.SchGetTaskNodeByName(PeerLsnMgrName)

	// fetch configration
	var cfg *config.Cfg4PeerManager
	if cfg = config.P2pConfig4PeerManager(peMgr.sdl.SchGetP2pCfgName()); cfg == nil {

		log.LogCallerFileLine("peMgrPoweron: P2pConfig4PeerManager failed")

		peMgr.inited<-PeMgrEnoConfig
		return PeMgrEnoConfig
	}

	// with static network type that tabMgr and dcvMgr would be done while power on
	if cfg.NetworkType == config.P2pNetworkTypeDynamic {
		peMgr.tabMgr = peMgr.sdl.SchGetUserTaskIF(sch.TabMgrName).(*tab.TableManager)
		_, peMgr.ptnTab = peMgr.sdl.SchGetTaskNodeByName(sch.TabMgrName)
		_, peMgr.ptnDcv = peMgr.sdl.SchGetTaskNodeByName(sch.DcvMgrName)
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
		peMgr.cfg.protocols = append(peMgr.cfg.protocols,
			Protocol{ Pid:p.Pid, Ver:p.Ver,},
		)
	}

	for _, sn := range peMgr.cfg.staticNodes {
		idEx := PeerIdEx{Id:sn.ID, Dir:PeInstOutPos}
		peMgr.staticsStatus[idEx] = peerIdle
		idEx.Dir = PeInstInPos
		peMgr.staticsStatus[idEx] = peerIdle
	}

	if len(peMgr.cfg.subNetIdList) == 0 && peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		peMgr.cfg.subNetIdList = append(peMgr.cfg.subNetIdList, config.AnySubNet)
		peMgr.cfg.subNetMaxPeers[config.AnySubNet] = config.MaxPeers
		peMgr.cfg.subNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
		peMgr.cfg.subNetMaxInBounds[config.AnySubNet] = config.MaxInbounds
	}

	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		for _, snid := range peMgr.cfg.subNetIdList {
			peMgr.nodes[snid] = make(map[PeerIdEx]*peerInstance)
			peMgr.workers[snid] = make(map[PeerIdEx]*peerInstance)
			peMgr.wrkNum[snid] = 0
			peMgr.ibpNum[snid] = 0
			peMgr.obpNum[snid] = 0
		}
		if len(peMgr.cfg.staticNodes) > 0 {
			staticSnid := peMgr.cfg.staticSubNetId
			peMgr.nodes[staticSnid] = make(map[PeerIdEx]*peerInstance)
			peMgr.workers[staticSnid] = make(map[PeerIdEx]*peerInstance)
			peMgr.wrkNum[staticSnid] = 0
			peMgr.ibpNum[staticSnid] = 0
			peMgr.obpNum[staticSnid] = 0
		}
	} else if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		staticSnid := peMgr.cfg.staticSubNetId
		peMgr.nodes[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.workers[staticSnid] = make(map[PeerIdEx]*peerInstance)
		peMgr.wrkNum[staticSnid] = 0
		peMgr.ibpNum[staticSnid] = 0
		peMgr.obpNum[staticSnid] = 0
	}

	// tell initialization result, and EvPeMgrStartReq would be sent to us
	// some moment later.
	peMgr.inited<-PeMgrEnoNone
	return PeMgrEnoNone
}

func (peMgr *PeerManager)PeMgrInited() PeMgrErrno {
	return <-peMgr.inited
}

func (peMgr *PeerManager)PeMgrStart() PeMgrErrno {
	log.LogCallerFileLine("PeMgrStart: EvPeMgrStartReq sent ok, target: %s",
		peMgr.sdl.SchGetTaskName(peMgr.ptnMe))
	var msg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
	peMgr.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrPoweroff(ptn interface{}) PeMgrErrno {
	sdl := peMgr.sdl.SchGetP2pCfgName()
	log.LogCallerFileLine("peMgrPoweroff: sdl: %s, task will be done, name: %s",
		sdl, peMgr.sdl.SchGetTaskName(ptn))

	powerOff := sch.SchMessage {
		Id:		sch.EvSchPoweroff,
		Body:	nil,
	}

	peMgr.sdl.SchSetSender(&powerOff, &sch.RawSchTask)
	for _, peerInst := range peMgr.peers {
		SetP2pkgCallback(nil, peerInst.ptnMe)
		peMgr.sdl.SchSetRecver(&powerOff, peerInst.ptnMe)
		peMgr.sdl.SchSendMessage(&powerOff)
	}

	if peMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrStartReq(_ interface{}) PeMgrErrno {
	var schMsg = sch.SchMessage{}

	// start peer listener if necessary
	if peMgr.cfg.noAccept == true {
		peMgr.accepter = nil
	} else {
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	// drive ourself to startup outbound
	time.Sleep(time.Microsecond * 100)
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	// set timer to debug print statistics about peer managers for test cases
	var td = sch.TimerDescription {
		Name:	"_ptsTimer",
		Utid:	sch.PeTestStatTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	time.Second * 2,
		Extra:	nil,
	}
	if peMgr.ssTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, peMgr.ssTid)
		peMgr.ssTid = sch.SchInvalidTid
	}

	var eno sch.SchErrno
	eno, peMgr.ssTid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td)
	if eno != sch.SchEnoNone || peMgr.ssTid == sch.SchInvalidTid {
		log.LogCallerFileLine("peMgrStartReq: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDcvFindNodeRsp(msg interface{}) PeMgrErrno {
	var rsp = msg.(*sch.MsgDcvFindNodeRsp)
	if peMgr.dynamicSubNetIdExist(&rsp.Snid) != true {
		log.LogCallerFileLine("peMgrDcvFindNodeRsp: subnet not exist")
		return PeMgrEnoNotfound
	}

	var snid = rsp.Snid
	var appended = make(map[SubNetworkID]int, 0)
	var dup bool
	var idEx = PeerIdEx {
			Id:		config.NodeID{},
			Dir:	PeInstOutPos,
		}

	for _, n := range rsp.Nodes {
		idEx.Id = n.ID
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
			log.LogCallerFileLine("peMgrDcvFindNodeRsp: too much, some are truncated")
			continue
		}
		peMgr.randoms[snid] = append(peMgr.randoms[snid], n)
		appended[snid]++
	}

	// drive ourself to startup outbound for nodes appended
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

	var schMsg = sch.SchMessage{}
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

	peInst.txPendSig	= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.txDone		= make(chan PeMgrErrno, 1)
	peInst.txExit		= make(chan PeMgrErrno)
	peInst.rxDone		= make(chan PeMgrErrno, 1)
	peInst.rxExit		= make(chan PeMgrErrno)

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
		log.LogCallerFileLine("peMgrLsnConnAcceptedInd: SchCreateTask failed, eno: %d", eno)
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
		if !peMgr.cfg.noAccept && !peMgr.acceptPaused {
			// bugs: we can not pause accepter simply, a duration of delay should
			// be apply before pausing it.

			//log.LogCallerFileLine("peMgrLsnConnAcceptedInd: going to pause accepter, " +
			//	"cfgName: %s", peMgr.cfg.cfgName)
			//peMgr.acceptPaused = peMgr.accepter.PauseAccept()
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
		Dir:	PeInstOutPos,
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
	idEx = PeerIdEx{Id:config.NodeID{}, Dir:PeInstOutPos}
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

	// If outbounds are not enougth, ask discover for more
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
	var rdCnt = 0
	var idEx = PeerIdEx{Dir:PeInstOutPos}
	for _, n := range peMgr.randoms[*snid] {
		idEx.Id = n.ID
		if _, ok := peMgr.nodes[*snid][idEx]; !ok {
			candidates = append(candidates, n)
		}
		rdCnt++
	}

	if rdCnt > 0 {
		peMgr.randoms[*snid] = append(peMgr.randoms[*snid][:0], peMgr.randoms[*snid][rdCnt:]...)
	}

	// Create outbound instances for candidates if any
	var failed = 0
	var ok = 0
	maxOutbound := peMgr.cfg.subNetMaxOutbounds[*snid]
	for _, n := range candidates {
		if eno := peMgr.peMgrCreateOutboundInst(snid, n); eno != PeMgrEnoNone {
			failed++
			continue
		}
		ok++
		if peMgr.obpNum[*snid] >= maxOutbound {
			break
		}
	}

	// If outbounds are not enougth, ask discover to find more
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
				log.LogCallerFileLine("peMgrConnOutRsp: peMgrKillInst failed, eno: %d", eno)
				return eno
			}
			// drive ourself to startup outbound
			var schMsg = sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &rsp.snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
		return PeMgrEnoNone
	}

	// request the instance to handshake
	var schMsg = sch.SchMessage{}
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
		log.LogCallerFileLine("peMgrHandshakeRsp: instance not found, rsp: %s",
			fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoNotfound
	}
	if inst.snid != rsp.snid || inst.dir != rsp.dir {
		log.LogCallerFileLine("peMgrHandshakeRsp: response mismatched with instance, rsp: %s",
			fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoParameter
	}

	// Check result, if failed, kill the instance
	idEx := PeerIdEx{Id:rsp.peNode.ID, Dir:rsp.dir}
	if rsp.result != PeMgrEnoNone {
		peMgr.updateStaticStatus(rsp.snid, idEx, peerKilling)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
		if inst.dir == PeInstDirOutbound {
			var schMsg = sch.SchMessage{}
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
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
		return PeMgrEnoResource
	}

	if inst.dir == PeInstDirInbound {
		idEx := PeerIdEx{Id:rsp.peNode.ID, Dir:PeInstInPos}
		if peMgr.isStaticSubNetId(snid) {
			if _, dup := peMgr.workers[snid][idEx]; dup {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoDuplicated
			}
			peMgr.workers[snid][idEx] = inst
		} else {
			if peMgr.ibpNum[snid] >= maxInbound {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoResource
			}
			if _, dup := peMgr.workers[snid][idEx]; dup {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoDuplicated
			}
			idEx.Dir = PeInstOutPos
			if peMgr.instStateCmpKill(inst, rsp.ptn, snid, rsp.peNode, idEx) == PeMgrEnoDuplicated {
				return PeMgrEnoDuplicated
			}
		}
		idEx.Dir = PeInstInPos
		peMgr.nodes[snid][idEx] = inst
		peMgr.workers[snid][idEx] = inst
		peMgr.ibpNum[snid]++
	} else if inst.dir == PeInstDirOutbound {
		idEx := PeerIdEx{Id:rsp.peNode.ID, Dir:PeInstOutPos}
		if peMgr.isStaticSubNetId(snid) {
			if _, dup := peMgr.workers[snid][idEx]; dup {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoDuplicated
			}
			peMgr.workers[snid][idEx] = inst
		} else {
			if peMgr.obpNum[snid] >= maxOutbound {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoResource
			}
			if _, dup := peMgr.workers[snid][idEx]; dup {
				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir)
				return PeMgrEnoDuplicated
			}
			idEx.Dir = PeInstInPos
			if peMgr.instStateCmpKill(inst, rsp.ptn, snid, rsp.peNode, idEx) == PeMgrEnoDuplicated {
				return PeMgrEnoDuplicated
			}
		}
		idEx.Dir = PeInstDirOutbound
		peMgr.workers[snid][idEx] = inst
		peMgr.updateStaticStatus(snid, idEx, peerActivated)
	}

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeEstablishedInd, nil)
	peMgr.sdl.SchSendMessage(&schMsg)
	inst.state = peInstStateActivated
	peMgr.wrkNum[snid]++
	if inst.dir == PeInstDirInbound  &&
		inst.peMgr.cfg.networkType != config.P2pNetworkTypeStatic {
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
			log.LogCallerFileLine("peMgrHandshakeRsp: TabBucketAddNode failed, eno: %d, node: %s",
				tabEno, fmt.Sprintf("%+v", *rsp.peNode))
		}
		tabEno = peMgr.tabMgr.TabUpdateNode(snid, &n)
		if tabEno != tab.TabMgrEnoNone {
			log.LogCallerFileLine("peMgrHandshakeRsp: TabUpdateNode failed, eno: %d, node: %s",
				tabEno, fmt.Sprintf("%+v", *rsp.peNode))
		}
	}

	i := P2pIndPeerActivatedPara {
		Ptn: inst.ptnMe,
		PeerInfo: & Handshake {
			Snid:		inst.snid,
			Dir:		inst.dir,
			NodeId:		inst.node.ID,
			ProtoNum:	inst.protoNum,
			Protocols:	inst.protocols,
		},
	}
	return peMgr.peMgrIndEnque(&i)
}

func (peMgr *PeerManager)peMgrPingpongRsp(msg interface{}) PeMgrErrno {
	var rsp = msg.(*msgPingpongRsp)
	if rsp.result != PeMgrEnoNone {
		log.LogCallerFileLine("peMgrPingpongRsp: result: %d, node: %s",
			rsp.result, config.P2pNodeId2HexString(rsp.peNode.ID))
		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, rsp.dir); eno != PeMgrEnoNone {
			log.LogCallerFileLine("peMgrPingpongRsp: kill instance failed, inst: %s, node: %s",
				peMgr.sdl.SchGetTaskName(rsp.ptn),
				config.P2pNodeId2HexString(rsp.peNode.ID))
			return eno
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCloseReq(msg interface{}) PeMgrErrno {
	var req = msg.(*sch.MsgPeCloseReq)
	var snid = req.Snid
	var idEx = PeerIdEx{Id: req.Node.ID, Dir: req.Dir}
	inst := peMgr.getWorkerInst(snid, &idEx)
	if inst == nil {
		return PeMgrEnoNotfound
	}
	if inst.killing == true {
		return PeMgrEnoDuplicated
	}
	peMgr.updateStaticStatus(snid, idEx, peerKilling)
	schMsg := sch.SchMessage{}
	req.Node = inst.node
	req.Ptn = inst.ptnMe
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConnCloseCfm(msg interface{}) PeMgrErrno {
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConnCloseInd(msg interface{}) PeMgrErrno {
	var ind = msg.(*MsgCloseInd)
	if eno := peMgr.peMgrKillInst(ind.ptn, ind.peNode, ind.dir); eno != PeMgrEnoNone {
		return PeMgrEnoScheduler
	}
	i := P2pIndPeerClosedPara {
		Ptn:		peMgr.ptnMe,
		Snid:		ind.snid,
		PeerId:		ind.peNode.ID,
	}
	peMgr.peMgrIndEnque(&i)
	// drive ourselves to startup outbound
	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &ind.snid)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDataReq(msg interface{}) PeMgrErrno {
	var inst *peerInstance = nil
	var idEx = PeerIdEx{}
	var req = msg.(*sch.MsgPeDataReq)
	idEx.Id = req.PeerId
	idEx.Dir = PeInstOutPos
	if inst = peMgr.getWorkerInst(req.SubNetId, &idEx); inst == nil {
		idEx.Dir = PeInstInPos
		if inst = peMgr.getWorkerInst(req.SubNetId, &idEx); inst == nil {
			return PeMgrEnoNotfound
		}
	}
	if inst.txPendNum >= PeInstMaxP2packages {
		log.LogCallerFileLine("peMgrDataReq: tx buffer full")
		return PeMgrEnoResource
	}
	_pkg := req.Pkg.(*P2pPackage)
	inst.txPendSig<-_pkg
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

	peInst.txPendSig	= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.txDone		= make(chan PeMgrErrno, 1)
	peInst.txExit		= make(chan PeMgrErrno)
	peInst.rxDone		= make(chan PeMgrErrno, 1)
	peInst.rxExit		= make(chan PeMgrErrno)

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
		log.LogCallerFileLine("peMgrCreateOutboundInst: SchCreateTask failed, eno: %d", eno)
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
	log.LogCallerFileLine("peMgrKillInst: done task, sdl: %s, task: %s",
		peMgr.sdl.SchGetP2pCfgName(), peMgr.sdl.SchGetTaskName(ptn))

	if ptn == nil && node == nil {
		log.LogCallerFileLine("peMgrKillInst: invalid parameters")
		return PeMgrEnoParameter
	}

	var peInst = peMgr.peers[ptn]
	if peInst == nil {
		log.LogCallerFileLine("peMgrKillInst: instance not found, node: %s",
			config.P2pNodeId2HexString(node.ID))
		return PeMgrEnoNotfound
	}

	if peInst.dir != dir {
		log.LogCallerFileLine("peMgrKillInst: invalid parameters")
		return PeMgrEnoParameter
	}

	if peInst.ppTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(ptn, peInst.ppTid)
		peInst.ppTid = sch.SchInvalidTid
	}

	if peInst.conn != nil {
		peInst.conn.Close()
		peInst.conn = nil
	}

	// Remove maps for the node: we must check the instance state and connection
	// direction to step ahead.
	snid := peInst.snid
	idEx := PeerIdEx{Id:peInst.node.ID, Dir:peInst.dir}
	if peInst.state == peInstStateActivated {
		delete(peMgr.workers[snid], idEx)
		peMgr.wrkNum[snid]--
	}

	if peInst.dir == PeInstDirOutbound {
		delete(peMgr.nodes[snid], idEx)
		delete(peMgr.peers, ptn)
	} else if peInst.dir == PeInstDirInbound {
		delete(peMgr.peers, ptn)
		if peInst.state == peInstStateActivated {
			delete(peMgr.nodes[snid], idEx)
		}
	}

	if peInst.dir == PeInstDirOutbound {
		peMgr.obpNum[snid]--
	} else if peInst.dir == PeInstDirInbound {
		peMgr.ibpTotalNum--
		if peInst.state == peInstStateActivated {
			peMgr.ibpNum[snid]--
		}
	}

	peMgr.updateStaticStatus(snid, idEx, peerIdle)

	// resume accepter if necessary
	if peMgr.cfg.noAccept == false &&
		peMgr.acceptPaused == true  &&
		peMgr.ibpTotalNum < peMgr.cfg.ibpNumTotal {
		log.LogCallerFileLine("peMgrLsnConnAcceptedInd: resume accepter, cfgName: %s", peMgr.cfg.cfgName)
		peMgr.acceptPaused = !peMgr.accepter.ResumeAccept()
	}

	// Stop instance task
	peMgr.sdl.SchStopTask(ptn)

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrAsk4More(snid *SubNetworkID) PeMgrErrno {
	var timerName = ""
	var eno sch.SchErrno
	var tid int
	log.LogCallerFileLine("peMgrAsk4More: " +
		"cfgName: %s, subnet: %x, obpNum: %d, ibpNum: %d, ibpTotalNum: %d, wrkNum: %d",
		peMgr.cfg.cfgName,
		*snid,
		peMgr.obpNum[*snid],
		peMgr.ibpNum[*snid],
		peMgr.ibpTotalNum,
		peMgr.wrkNum[*snid])

	dur := durStaticRetryTimer
	if *snid != peMgr.cfg.staticSubNetId {
		dur = durDcvFindNodeTimer
		more := peMgr.cfg.subNetMaxOutbounds[*snid] - peMgr.obpNum[*snid]
		if more <= 0 {
			log.LogCallerFileLine("peMgrAsk4More: no more needed, obpNum: %d, max: %d",
				peMgr.obpNum[*snid],
				peMgr.cfg.subNetMaxOutbounds[*snid])
			return PeMgrEnoNone
		}

		var schMsg= sch.SchMessage{}
		var req = sch.MsgDcvFindNodeReq{
			Snid:	*snid,
			More:    more,
			Include: nil,
			Exclude: nil,
		}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvFindNodeReq, &req)
		peMgr.sdl.SchSendMessage(&schMsg)
		timerName = PeerMgrName + "_DcvFindNode"
	} else {
		timerName = PeerMgrName + "_static"
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
		log.LogCallerFileLine("peMgrAsk4More: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	peMgr.tidFindNode[*snid] = tid
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrIndEnque(ind interface{}) PeMgrErrno {
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
)

type peerInstState int	// instance state type

const PeInstDirNull			= -1			// null, so connection should be nil
const PeInstDirOutbound		= 1				// outbound connection
const PeInstDirInbound		= 0				// inbound connection
const PeInstOutPos			= 1				// outbound position
const PeInstInPos			= 0				// inbound position

const PeInstMailboxSize 	= 32				// mailbox size
const PeInstMaxP2packages	= 128				// max p2p packages pending to be sent
const PeInstMaxPingpongCnt	= 8					// max pingpong counter value
const PeInstPingpongCycle	= time.Second * 16	// pingpong period

type peerInstance struct {
	sdl			*sch.Scheduler				// pointer to scheduler
	peMgr		*PeerManager				// pointer to peer manager
	name		string						// name
	tep			sch.SchUserTaskEp			// entry
	ptnMe		interface{}					// the instance task node pointer
	ptnMgr		interface{}					// the peer manager task node pointer
	state		peerInstState				// state
	killing		bool						// is instance in killing
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
	p2pkgLock	sync.Mutex					// lock for p2p package tx-sync
	rxChan		chan *P2pPackageRx			// rx pending channel
	txPendSig	chan *P2pPackage			// tx pending channel
	txPendNum	int							// tx pending number
	txDone		chan PeMgrErrno				// TX chan
	txExit		chan PeMgrErrno				// TX had been done
	rxDone		chan PeMgrErrno				// RX chan
	rxExit		chan PeMgrErrno				// RX had been done
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
	dir:		PeInstDirNull,
	node:		config.Node{},
	maxPkgSize:	maxTcpmsgSize,
	protoNum:	0,
	protocols:	[]Protocol{{}},
	ppTid:		sch.SchInvalidTid,
	p2pkgLock:	sync.Mutex{},
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
	case sch.EvPeDataReq:
		eno = pi.piDataReq(msg.Body)
	default:
		log.LogCallerFileLine("PeerInstProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	sdl := pi.sdl.SchGetP2pCfgName()
	log.LogCallerFileLine("peerInstProc: sdl: %s, pi.name: %s, msg.Id: %d, sender: %s",
		sdl,  pi.name, msg.Id, pi.sdl.SchGetTaskName(pi.sdl.SchGetSender(msg)))

	if eno != PeMgrEnoNone {
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

func (inst *peerInstance)piPoweroff(ptn interface{}) PeMgrErrno {
	sdl := inst.sdl.SchGetP2pCfgName()
	if inst.killing == true {
		log.LogCallerFileLine("piPoweroff: task already in killing, sdl: %s, name: %s",
			sdl, inst.sdl.SchGetTaskName(inst.ptnMe))
		return PeMgrEnoNone
	}

	log.LogCallerFileLine("piPoweroff: task will be done, sdl: %s, name: %s",
		sdl, inst.sdl.SchGetTaskName(inst.ptnMe))

	if inst.state == peInstStateActivated {
		if inst.txPendSig != nil {
			close(inst.txPendSig)
		}

		if inst.txDone != nil {
			inst.txDone <- PeMgrEnoNone
			<-inst.txExit
			close(inst.txDone)
		}

		if inst.rxDone != nil {
			inst.rxDone <- PeMgrEnoNone
			<-inst.rxExit
			close(inst.rxDone)
		}
	}

	if inst.conn != nil {
		inst.conn.Close()
		inst.conn = nil
	}

	if inst.sdl.SchTaskDone(inst.ptnMe, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (inst *peerInstance)piConnOutReq(_ interface{}) PeMgrErrno {
	if inst.dialer == nil ||
		inst.dir != PeInstDirOutbound  ||
		inst.state != peInstStateConnOut {
		log.LogCallerFileLine("piConnOutReq: instance mismatched")
		return PeMgrEnoInternal
	}

	var addr = &net.TCPAddr{IP: inst.node.IP, Port: int(inst.node.TCP)}
	var conn net.Conn = nil
	var err error
	var eno PeMgrErrno = PeMgrEnoNone
	inst.dialer.Timeout = inst.cto
	if conn, err = inst.dialer.Dial("tcp", addr.String()); err != nil {
		log.LogCallerFileLine("piConnOutReq: dial failed, to: %s, err: %s", addr.String(), err.Error())
		eno = PeMgrEnoOs
	} else {
		inst.conn = conn
		inst.laddr = conn.LocalAddr().(*net.TCPAddr)
		inst.raddr = conn.RemoteAddr().(*net.TCPAddr)
		inst.state = peInstStateConnected
		log.LogCallerFileLine("piConnOutReq: dial ok, laddr: %s, raddr: %s",
			inst.laddr.String(),
			inst.raddr.String())
	}

	var schMsg = sch.SchMessage{}
	var rsp = msgConnOutRsp {
		result:	eno,
		snid:	inst.snid,
		peNode:	&inst.node,
		ptn:	inst.ptnMe,
	}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptnMe, inst.ptnMgr, sch.EvPeConnOutRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (inst *peerInstance)piHandshakeReq(_ interface{}) PeMgrErrno {
	if inst == nil {
		log.LogCallerFileLine("piHandshakeReq: invalid instance")
		return PeMgrEnoParameter
	}
	if inst.state != peInstStateConnected && inst.state != peInstStateAccepted {
		log.LogCallerFileLine("piHandshakeReq: instance mismatched")
		return PeMgrEnoInternal
	}
	if inst.conn == nil {
		log.LogCallerFileLine("piHandshakeReq: invalid instance")
		return PeMgrEnoInternal
	}

	// Carry out action according to the direction of current peer instance connection.
	var eno PeMgrErrno
	if inst.dir == PeInstDirInbound {
		eno = inst.piHandshakeInbound(inst)
	} else if inst.dir == PeInstDirOutbound {
		eno = inst.piHandshakeOutbound(inst)
	} else {
		log.LogCallerFileLine("piHandshakeReq: invalid instance direction: %d", inst.dir)
		eno = PeMgrEnoInternal
	}

	log.LogCallerFileLine("piHandshakeReq: handshake result: %d, dir: %d, laddr: %s, raddr: %s, peer: %s",
			eno,
			inst.dir,
			inst.laddr.String(),
			inst.raddr.String(),
			fmt.Sprintf("%+v", inst.node)	)

	var rsp = msgHandshakeRsp {
		result:	eno,
		dir:	inst.dir,
		snid:	inst.snid,
		peNode:	&inst.node,
		ptn:	inst.ptnMe,
	}
	var schMsg = sch.SchMessage{}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptnMe, inst.ptnMgr, sch.EvPeHandshakeRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	return eno
}

func (inst *peerInstance)piPingpongReq(msg interface{}) PeMgrErrno {
	if inst.ppEno != PeMgrEnoNone {
		log.LogCallerFileLine("piPingpongReq: nothing done, ppEno: %d", inst.ppEno)
		return PeMgrEnoResource
	}
	if inst.conn == nil {
		log.LogCallerFileLine("piPingpongReq: connection had been closed")
		return PeMgrEnoResource
	}
	inst.ppSeq = msg.(*MsgPingpongReq).seq
	ping := Pingpong {
		Seq:	inst.ppSeq,
		Extra:	nil,
	}
	inst.ppSeq++

	upkg := new(P2pPackage)
	if eno := upkg.ping(inst, &ping); eno != PeMgrEnoNone {
		inst.ppEno = eno
		i := P2pIndConnStatusPara {
			Ptn:		inst.ptnMe,
			PeerInfo:	&Handshake{
				Snid:      inst.snid,
				NodeId:    inst.node.ID,
				ProtoNum:  inst.protoNum,
				Protocols: inst.protocols,
			},
		}
		req := sch.MsgPeCloseReq {
			Ptn: inst.ptnMe,
			Snid: inst.snid,
			Node: inst.node,
			Dir: inst.dir,
			Why: &i,
		}
		msg := sch.SchMessage{}
		inst.sdl.SchMakeMessage(&msg, inst.ptnMe, inst.ptnMgr, sch.EvPeCloseReq, &req)
		inst.sdl.SchSendMessage(&msg)
		return eno
	}

	return PeMgrEnoNone
}

func (inst *peerInstance)piCloseReq(_ interface{}) PeMgrErrno {
	sdl := inst.sdl.SchGetP2pCfgName()
	if inst.killing == true {
		log.LogCallerFileLine("piCloseReq: already in killing, sdl: %s, task: %s",
			sdl, inst.sdl.SchGetTaskName(inst.ptnMe))
		return PeMgrEnoDuplicated
	}
	inst.killing = true
	node := inst.node
	if inst.state == peInstStateActivated {
		if inst.txPendSig != nil {
			close(inst.txPendSig)
		}
		inst.rxDone <- PeMgrEnoNone
		<-inst.rxExit
		inst.txDone <- PeMgrEnoNone
		<-inst.txExit
	}
	close(inst.rxDone)
	close(inst.rxExit)
	close(inst.txDone)
	close(inst.txExit)

	cfm := MsgCloseCfm {
		result: PeMgrEnoNone,
		dir:	inst.dir,
		snid:	inst.snid,
		peNode:	&node,
		ptn:	inst.ptnMe,
	}
	peMgr := inst.peMgr
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, inst.ptnMe, peMgr.ptnMe, sch.EvPeCloseCfm, &cfm)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (inst *peerInstance)piEstablishedInd( msg interface{}) PeMgrErrno {
	sdl := inst.sdl.SchGetP2pCfgName()
	var schEno sch.SchErrno
	var tid int
	var tmDesc = sch.TimerDescription {
		Name:	PeerMgrName + "_PePingpong",
		Utid:	sch.PePingpongTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	PeInstPingpongCycle,
		Extra:	nil,
	}
	if schEno, tid = inst.sdl.SchSetTimer(inst.ptnMe, &tmDesc);
		schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.LogCallerFileLine("piEstablishedInd: SchSetTimer failed, sdl: %s, inst: %s, eno: %d",
			sdl, inst.name, schEno)
		return PeMgrEnoScheduler
	}
	inst.ppTid = tid
	inst.txEno = PeMgrEnoNone
	inst.rxEno = PeMgrEnoNone
	inst.ppEno = PeMgrEnoNone
	inst.conn.SetDeadline(time.Time{})

	go piTx(inst)
	go piRx(inst)

	return PeMgrEnoNone
}

func (inst *peerInstance)piPingpongTimerHandler() PeMgrErrno {
	msg := sch.SchMessage{}
	if inst.ppCnt++; inst.ppCnt > PeInstMaxPingpongCnt {
		inst.ppEno = PeMgrEnoPingpongTh
		i := P2pIndConnStatusPara {
			Ptn:		inst.ptnMe,
			PeerInfo:	&Handshake {
				Snid:		inst.snid,
				NodeId:		inst.node.ID,
				ProtoNum:	inst.protoNum,
				Protocols:	inst.protocols,
			},
			Status		:	PeMgrEnoPingpongTh,
			Flag		:	true,
			Description	:	"piPingpongTimerHandler: threshold reached",
		}
		req := sch.MsgPeCloseReq {
			Ptn: inst.ptnMe,
			Snid: inst.snid,
			Node: inst.node,
			Dir: inst.dir,
			Why: &i,
		}
		inst.sdl.SchMakeMessage(&msg, inst.ptnMe, inst.ptnMgr, sch.EvPeCloseReq, &req)
		inst.sdl.SchSendMessage(&msg)
		return inst.ppEno
	}
	pr := MsgPingpongReq {
		seq: uint64(time.Now().UnixNano()),
	}
	inst.sdl.SchMakeMessage(&msg, inst.ptnMe, inst.ptnMe, sch.EvPePingpongReq, &pr)
	inst.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (inst *peerInstance)piDataReq(_ interface{}) PeMgrErrno {
	return PeMgrEnoNone
}

func (pi *peerInstance)piHandshakeInbound(inst *peerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs *Handshake
	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		log.LogCallerFileLine("piHandshakeInbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}
	if inst.peMgr.dynamicSubNetIdExist(&hs.Snid) == false &&
		inst.peMgr.staticSubNetIdExist(&hs.Snid) == false {
		log.LogCallerFileLine("piHandshakeInbound: local node does not attach to subnet: %x", hs.Snid)
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
		log.LogCallerFileLine("piHandshakeInbound: write outbound Handshake message failed, eno: %d", eno)
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
		log.LogCallerFileLine("piHandshakeOutbound: write outbound Handshake message failed, eno: %d", eno)
		return eno
	}
	// read inbound handshake from remote peer
	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		log.LogCallerFileLine("piHandshakeOutbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}
	// check sub network identity
	if hs.Snid != inst.snid {
		log.LogCallerFileLine("piHandshakeOutbound: subnet identity mismathced")
		return PeMgrEnoMessage
	}
	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos,
	// and we update the remains.
	if hs.NodeId != inst.node.ID {
		log.LogCallerFileLine("piHandshakeOutbound: node identity mismathced")
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

func SetP2pkgCallback(cb interface{}, ptn interface{}) PeMgrErrno {
	return PeMgrEnoNone
}

func SendPackage(pkg *P2pPackage2Peer) (PeMgrErrno){
	if len(pkg.IdList) == 0 {
		log.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}
	peMgr := pkg.P2pInst.SchGetUserTaskIF(sch.PeerMgrName).(*PeerManager)
	for _, pid := range pkg.IdList {
		_pkg := new(P2pPackage)
		_pkg.Pid = uint32(pkg.ProtoId)
		_pkg.PayloadLength = uint32(pkg.PayloadLength)
		_pkg.Payload = append(_pkg.Payload, pkg.Payload...)

		req := sch.MsgPeDataReq {
			SubNetId: pkg.SubNetId,
			PeerId: pid,
			Pkg: _pkg,
		}
		msg := sch.SchMessage{}
		pkg.P2pInst.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeDataReq, &req)
		pkg.P2pInst.SchSendMessage(&msg)
	}
	return PeMgrEnoUnknown
}

func (peMgr *PeerManager)ClosePeer(snid *SubNetworkID, id *PeerId) PeMgrErrno {
	idExOut := PeerIdEx{Id: *id, Dir: PeInstOutPos}
	idExIn := PeerIdEx{Id: *id, Dir: PeInstInPos}
	idExList := []PeerIdEx{idExOut, idExIn}
	for _, idEx := range idExList {
		var req = sch.MsgPeCloseReq{
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

func piTx(inst *peerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When use try to close the peer, this routine
	// would then exit.
	sdl := inst.sdl.SchGetP2pCfgName()
	var done PeMgrErrno = PeMgrEnoNone

txBreak:
	for {
		// check if we are done
chkDone:
		select {
		case done = <-inst.txDone:
			log.LogCallerFileLine("piTx: sdl: %s, inst: %s, done with: %d", sdl, inst.name, done)
			inst.txExit<-done
			break txBreak
		default:
		}
		// if errors, we wait and then continue
		if inst.txEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}
		// check if some pending, if the signal closed, we check if it's done
		upkg, ok := <-(inst.txPendSig)
		if !ok {
			goto chkDone
		}
		inst.txPendNum -= 1
		// carry out Tx
		if eno := upkg.SendPackage(inst); eno != PeMgrEnoNone {
			// 1) if failed, callback to the user, so he can close
			// this peer seems in troubles, we will be done then.
			// 2) it is possible that, while we are blocked here in
			// writing and the connection is closed for some reasons
			// (for example the user close the peer), in this case,
			// we would get an error.
			inst.txEno = eno
			hs := Handshake {
				Snid:		inst.snid,
				NodeId:		inst.node.ID,
				ProtoNum:	inst.protoNum,
				Protocols:	inst.protocols,
			}
			i := P2pIndConnStatusPara{
				Ptn:		inst.ptnMe,
				PeerInfo:	&hs,
				Status:		int(eno),
				Flag:		false,
				Description:"piTx: SendPackage failed",
			}
			req := sch.MsgPeCloseReq {
				Ptn: inst.ptnMe,
				Snid: inst.snid,
				Node: inst.node,
				Dir: inst.dir,
				Why: &i,
			}
			msg := sch.SchMessage{}
			inst.sdl.SchMakeMessage(&msg, inst.ptnMe, inst.ptnMgr, sch.EvPeCloseReq, &req)
			inst.sdl.SchSendMessage(&msg)
		}
	}
	return done
}

func piRx(inst *peerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When use try to close the peer, this routine
	// would then exit.
	sdl := inst.sdl.SchGetP2pCfgName()
	var done PeMgrErrno = PeMgrEnoNone
	var peerInfo = PeerInfo{}
	var pkgCb = P2pPackageRx{}

rxBreak:
	for {
		// check if we are done
		select {
		case done = <-inst.rxDone:
			log.LogCallerFileLine("piRx: sdl: %s, inst: %s, done with: %d", sdl, inst.name, done)
			inst.rxExit<-done
			break rxBreak
		default:
		}
		// try reading the peer
		if inst.rxEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}
		upkg := new(P2pPackage)
		if eno := upkg.RecvPackage(inst); eno != PeMgrEnoNone {
			// 1) if failed, callback to the user, so he can close
			// this peer seems in troubles, we will be done then.
			// 2) it is possible that, while we are blocked here in
			// reading and the connection is closed for some reasons
			// (for example the user close the peer), in this case,
			// we would get an error.
			inst.rxEno = eno
			hs := Handshake {
				Snid:		inst.snid,
				NodeId:		inst.node.ID,
				ProtoNum:	inst.protoNum,
				Protocols:	inst.protocols,
			}
			i := P2pIndConnStatusPara{
				Ptn:		inst.ptnMe,
				PeerInfo:	&hs,
				Status:		int(eno),
				Flag:		false,
				Description:"piRx: RecvPackage failed",
			}
			req := sch.MsgPeCloseReq {
				Ptn: inst.ptnMe,
				Snid: inst.snid,
				Node: inst.node,
				Dir: inst.dir,
				Why: &i,
			}
			msg := sch.SchMessage{}
			inst.sdl.SchMakeMessage(&msg, inst.ptnMe, inst.ptnMgr, sch.EvPeCloseReq, &req)
			inst.sdl.SchSendMessage(&msg)
			continue
		}
		// check the package received to filter out those not for p2p internal only
		if upkg.Pid == uint32(PID_P2P) {
			if eno := inst.piP2pPkgProc(upkg); eno != PeMgrEnoNone {
				log.LogCallerFileLine("piRx: piP2pMsgProc failed, sdl: %s, inst: %s, eno: %d",
					sdl, inst.name, eno)
			}
		} else if upkg.Pid == uint32(PID_EXT) {
			peerInfo.Protocols	= nil
			peerInfo.Snid		= inst.snid
			peerInfo.NodeId		= inst.node.ID
			peerInfo.ProtoNum	= inst.protoNum
			peerInfo.Protocols	= append(peerInfo.Protocols, inst.protocols...)
			pkgCb.Ptn			= inst.ptnMe
			pkgCb.Payload		= nil
			pkgCb.PeerInfo		= &peerInfo
			pkgCb.ProtoId		= int(upkg.Pid)
			pkgCb.PayloadLength	= int(upkg.PayloadLength)
			pkgCb.Payload		= append(pkgCb.Payload, upkg.Payload...)
			inst.rxChan<-&pkgCb
		} else {
			log.LogCallerFileLine("piRx: package discarded for unknown pid: sdl: %s, inst: %s, %d",
				 sdl, inst.name, upkg.Pid)
		}
	}
	return done
}

func (pi *peerInstance)piP2pPkgProc(upkg *P2pPackage) PeMgrErrno {
	if upkg.Pid != uint32(PID_P2P) {
		log.LogCallerFileLine("piP2pPkgProc: not a p2p package, pid: %d", upkg.Pid)
		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {
		log.LogCallerFileLine("piP2pPkgProc: invalid payload length: %d", upkg.PayloadLength)
		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {
		log.LogCallerFileLine("piP2pPkgProc: payload length mismatched, PlLen: %d, real: %d",
			upkg.PayloadLength, len(upkg.Payload))
		return PeMgrEnoMessage
	}

	var msg = P2pMessage{}
	if eno := upkg.GetMessage(&msg); eno != PeMgrEnoNone {
		log.LogCallerFileLine("piP2pPkgProc: GetMessage failed, eno: %d", eno	)
		return eno
	}

	// check message identity. we discard any handshake messages received here
	// since handshake procedure had been passed, and dynamic handshake is not
	// supported currently.
	switch msg.Mid {
	case uint32(MID_HANDSHAKE):
		log.LogCallerFileLine("piP2pPkgProc: MID_HANDSHAKE, discarded")
		return PeMgrEnoMessage
	case uint32(MID_PING):
		return pi.piP2pPingProc(msg.Ping)
	case uint32(MID_PONG):
		return pi.piP2pPongProc(msg.Pong)
	default:
		log.LogCallerFileLine("piP2pPkgProc: unknown mid: %d", msg.Mid)
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
		log.LogCallerFileLine("piP2pPingProc: pong failed, eno: %d, pi: %s",
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

func (peMgr *PeerManager) instStateCmpKill(inst *peerInstance, ptn interface{}, snid SubNetworkID, node *config.Node, idEx PeerIdEx) PeMgrErrno {
	if _, dup := peMgr.nodes[snid][idEx]; dup {
		var ptn2Kill interface{} = nil
		var node2Kill *config.Node = nil
		dupInst := peMgr.nodes[snid][idEx]
		cmp := inst.state.compare(dupInst.state)
		obKilled := false
		if cmp < 0 {
			ptn2Kill = ptn
			node2Kill = node
		} else if cmp > 0 {
			ptn2Kill = dupInst.ptnMe
			node2Kill = &dupInst.node
			obKilled = dupInst.dir == PeInstDirOutbound
		} else {
			if rand.Int()&0x01 == 0 {
				ptn2Kill = ptn
				node2Kill = node
			} else {
				ptn2Kill = dupInst.ptnMe
				node2Kill = &dupInst.node
				obKilled = dupInst.dir == PeInstDirOutbound
			}
		}
		_ = node2Kill
		peMgr.peMgrKillInst(ptn, node, inst.dir)
		if ptn2Kill == ptn {
			return PeMgrEnoDuplicated
		}
		if obKilled {
			var schMsg = sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
		return PeMgrEnoDuplicated
	}
	return PeMgrEnoNone
}

// Print peer statistics, for test only
const doLogPeerStat  = true
func (peMgr *PeerManager)logPeerStat() {
	if !doLogPeerStat {
		return
	}
	var obpNumSum = 0
	var ibpNumSum = 0
	var wrkNumSum = 0
	var ibpNumTotal = peMgr.ibpTotalNum

	for _, num := range peMgr.obpNum {
		obpNumSum += num
	}

	for _, num := range peMgr.ibpNum {
		ibpNumSum += num
	}

	for _, num := range peMgr.wrkNum {
		wrkNumSum += num
	}

	var dbgMsg = ""
	strSum := fmt.Sprintf("================================= logPeerStat: =================================\n" +
		"logPeerStat: p2pInst: %s, obpNumSum: %d, ibpNumSum: %d, ibpNumTotal: %d, wrkNumSum: %d, accepting: %t\n",
		peMgr.cfg.cfgName,
		obpNumSum, ibpNumSum, ibpNumTotal, wrkNumSum, !peMgr.acceptPaused)
	dbgMsg += strSum

	var subNetIdList = make([]SubNetworkID, 0)
	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		subNetIdList = append(subNetIdList, peMgr.cfg.subNetIdList...)
		if len(peMgr.cfg.staticNodes) > 0 {
			subNetIdList = append(subNetIdList, peMgr.cfg.staticSubNetId)
		}
	} else if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		if len(peMgr.cfg.staticNodes) > 0 {
			subNetIdList = append(subNetIdList, peMgr.cfg.staticSubNetId)
		}
	}

	for _, snid := range subNetIdList {
		strSubnet := fmt.Sprintf("logPeerStat: p2pInst: %s, subnet: %x, obpNum: %d, ibpNum: %d, wrkNum: %d\n",
			peMgr.cfg.cfgName,
			snid,
			peMgr.obpNum[snid],
			peMgr.ibpNum[snid],
			peMgr.wrkNum[snid])
		dbgMsg += strSubnet
	}
	fmt.Printf("%s", dbgMsg)
}


