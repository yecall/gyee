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
	ggio 	"github.com/gogo/protobuf/io"
	config	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	log		"github.com/yeeco/gyee/p2p/logger"
	golog	"log"
)

//
// Peer manager errno
//
const (
	PeMgrEnoNone	= iota
	PeMgrEnoParameter
	PeMgrEnoScheduler
	PeMgrEnoConfig
	PeMgrEnoResource
	PeMgrEnoOs
	PeMgrEnoMessage
	PeMgrEnoDuplicaated
	PeMgrEnoNotfound
	PeMgrEnoInternal
	PeMgrEnoPingpongTh
	PeMgrEnoUnknown
)

type PeMgrErrno int

//
// Peer identity as string
//
type PeerId = config.NodeID

//
// Peer information
//
type PeerInfo Handshake

//
// Peer manager configuration
//
const defaultConnectTimeout = 15 * time.Second		// default dial outbound timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

const defaultHandshakeTimeout = 8 * time.Second		// default handshake timeout value, currently
													// it's a fixed value here than can be configurated
													// by other module.

const defaultActivePeerTimeout = 15 * time.Second	// default read/write operation timeout after a peer
													// connection is activaged in working.

const maxTcpmsgSize = 1024*1024*4					// max size of a tcpmsg package could be, currently
													// it's a fixed value here than can be configurated
													// by other module.

const durDcvFindNodeTimer = time.Second * 20		// duration to wait for find node response from discover task,
													// should be (findNodeExpiration + delta).

const durStaticRetryTimer = time.Second * 4			// duration to check and retry connect to static peers

const (
	peerIdle			= iota						// idle
	peerConnectOutInited							// connecting out inited
	peerActivated									// had been activated
	peerKilling										// in killing
)

type SubNetworkID = config.SubNetworkID

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

//
// Peer manager
//
const PeerMgrName = sch.PeerMgrName

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
	nodes			map[SubNetworkID]map[config.NodeID]*peerInstance	// map peer node identity to instance pointer
	workers			map[SubNetworkID]map[config.NodeID]*peerInstance	// map peer node identity to pointer of instance in work
	wrkNum			map[SubNetworkID]int			// worker peer number
	ibpNum			map[SubNetworkID]int			// active inbound peer number
	obpNum			map[SubNetworkID]int			// active outbound peer number
	ibpTotalNum		int								// total active inbound peer number
	acceptPaused	bool							// if accept task paused
	randoms			map[SubNetworkID][]*config.Node	// random nodes found by discover

	txLock			sync.Mutex						// lock for data sending action from shell
	Lock4Cb			sync.Mutex						// lock for indication callback
	P2pIndHandler	P2pIndCallback					// indication callback installed by p2p user from shell

	ssTid			int								// statistics timer identity
	staticsStatus	map[config.NodeID]int			// statu about static nodes
}

//
// Create peer manager
//
func NewPeerMgr() *PeerManager {
	var peMgr = PeerManager{
		sdl:			nil,
		name:         	PeerMgrName,
		inited:       	make(chan PeMgrErrno),
		tep:          	nil,
		cfg:          	peMgrConfig{},
		tidFindNode:  	map[SubNetworkID]int{},
		ptnMe:        	nil,
		ptnTab:       	nil,
		ptnLsn:       	nil,
		ptnAcp:			nil,
		ptnDcv:			nil,
		tabMgr:			nil,
		accepter:		nil,
		peers:        	map[interface{}]*peerInstance{},
		nodes:        	map[SubNetworkID]map[config.NodeID]*peerInstance{},
		workers:      	map[SubNetworkID]map[config.NodeID]*peerInstance{},
		wrkNum:       	map[SubNetworkID]int{},
		ibpNum:       	map[SubNetworkID]int{},
		obpNum:       	map[SubNetworkID]int{},
		ibpTotalNum:		0,
		acceptPaused: 	false,
		randoms:      	map[SubNetworkID][]*config.Node{},
		P2pIndHandler:	nil,
		staticsStatus:	map[config.NodeID]int{},
	}

	peMgr.tep = peMgr.peerMgrProc

	return &peMgr
}

//
// Entry point exported to shceduler
//
func (peMgr *PeerManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return peMgr.tep(ptn, msg)
}

//
// Peer manager entry
//
func (peMgr *PeerManager)peerMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

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

	default:
		log.LogCallerFileLine("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if eno != PeMgrEnoNone {
		schEno = sch.SchEnoUserTask
	}

	return schEno
}

//
// Poweron event handler
//
func (peMgr *PeerManager)peMgrPoweron(ptn interface{}) PeMgrErrno {

	//
	// backup pointers of related tasks
	//

	peMgr.ptnMe	= ptn
	peMgr.sdl = sch.SchGetScheduler(ptn)

	_, peMgr.ptnLsn = peMgr.sdl.SchGetTaskNodeByName(PeerLsnMgrName)

	//
	// fetch configration
	//

	var cfg *config.Cfg4PeerManager = nil

	if cfg = config.P2pConfig4PeerManager(peMgr.sdl.SchGetP2pCfgName()); cfg == nil {

		log.LogCallerFileLine("peMgrPoweron: P2pConfig4PeerManager failed")

		peMgr.inited<-PeMgrEnoConfig
		return PeMgrEnoConfig
	}

	//
	// with static network type that tabMgr and dcvMgr would be done while power on
	//

	if cfg.NetworkType == config.P2pNewworkTypeDynamic {
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
		peMgr.staticsStatus[sn.ID] = peerIdle
	}

	if len(peMgr.cfg.subNetIdList) == 0 && peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {
		peMgr.cfg.subNetIdList = append(peMgr.cfg.subNetIdList, config.AnySubNet)
		peMgr.cfg.subNetMaxPeers[config.AnySubNet] = config.MaxPeers
		peMgr.cfg.subNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
		peMgr.cfg.subNetMaxInBounds[config.AnySubNet] = config.MaxInbounds
	}

	if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {
		for _, snid := range peMgr.cfg.subNetIdList {
			peMgr.nodes[snid] = make(map[config.NodeID]*peerInstance)
			peMgr.workers[snid] = make(map[config.NodeID]*peerInstance)
			peMgr.wrkNum[snid] = 0
			peMgr.ibpNum[snid] = 0
			peMgr.obpNum[snid] = 0
		}
		if len(peMgr.cfg.staticNodes) > 0 {
			staticSnid := peMgr.cfg.staticSubNetId
			peMgr.nodes[staticSnid] = make(map[config.NodeID]*peerInstance)
			peMgr.workers[staticSnid] = make(map[config.NodeID]*peerInstance)
			peMgr.wrkNum[staticSnid] = 0
			peMgr.ibpNum[staticSnid] = 0
			peMgr.obpNum[staticSnid] = 0
		}
	} else if peMgr.cfg.networkType == config.P2pNewworkTypeStatic {
		staticSnid := peMgr.cfg.staticSubNetId
		peMgr.nodes[staticSnid] = make(map[config.NodeID]*peerInstance)
		peMgr.workers[staticSnid] = make(map[config.NodeID]*peerInstance)
		peMgr.wrkNum[staticSnid] = 0
		peMgr.ibpNum[staticSnid] = 0
		peMgr.obpNum[staticSnid] = 0
	}

	//
	// tell initialization result, and EvPeMgrStartReq would be sent to us
	// at some moment.
	//

	peMgr.inited<-PeMgrEnoNone

	return PeMgrEnoNone
}

//
// Get initialization result of peer manager. This function is exported to
// outside telling the initialization result of peer manager.
//
func (peMgr *PeerManager)PeMgrInited() PeMgrErrno {
	return <-peMgr.inited
}

//
// Startup the peer manager. This function is exported to outside modules to
// choose a "good" chance to start the manager up.
//
func (peMgr *PeerManager)PeMgrStart() PeMgrErrno {

	//
	// Notice: in current implement, the peer module would start its inbound and outbound
	// procedures only after event sch.EvPeMgrStartReq received, and the inbound and outbound
	// are carried out at the same time(see is event handler), this might be an issue leads to
	// the eclipse attack... Not so much considered about this yet, we just start the peer
	// manager here as following.
	//

	var msg = sch.SchMessage{}

	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
	peMgr.sdl.SchSendMessage(&msg)

	log.LogCallerFileLine("PeMgrStart: " +
		"EvPeMgrStartReq sent ok, target: %s",
		peMgr.sdl.SchGetTaskName(peMgr.ptnMe))

	return PeMgrEnoNone
}


//
// Poweroff event handler
//
func (peMgr *PeerManager)peMgrPoweroff(ptn interface{}) PeMgrErrno {

	log.LogCallerFileLine("peMgrPoweroff: pwoeroff received, done the task")

	for snid, tid := range peMgr.tidFindNode {
		if tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid);
			peMgr.tidFindNode[snid] = sch.SchInvalidTid
		}
	}

	peMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)

	return PeMgrEnoNone
}

//
// Peer manager start request handler
//
func (peMgr *PeerManager)peMgrStartReq(msg interface{}) PeMgrErrno {

	//
	// Notice: when this event received, we are required startup to deal with
	// peers in both inbound and outbound direction. For inbound, the manager
	// can control the inbound listener with event sch.EvPeLsnStartReq; while
	// for outbound, the event sch.EvPeOutboundReq, is for self-driven for the
	// manager. This is the basic, but when to start the inbound and outbound
	// might be considerable, since it's security issues related. Currently,
	// we simply start both as the "same time" here in this function, one can
	// start outbound firstly, and then counte the successful outbound peers,
	// at last, start inbound when the number of outbound peers reach a predefined
	// threshold, and son on.
	//

	_ = msg

	var schMsg = sch.SchMessage{}

	//
	// start peer listener if necessary
	//

	if peMgr.cfg.noAccept == true {
		peMgr.accepter = nil
	} else {
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	//
	// drive ourself to startup outbound. set following message body to be nil
	// so all sub networks would try to connect outound.
	//

	time.Sleep(time.Microsecond * 100)

	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	//
	// set timer to debug print statistics about peer managers for test cases
	//

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
	}

	return PeMgrEnoNone
}

//
// FindNode response handler
//
func (peMgr *PeerManager)peMgrDcvFindNodeRsp(msg interface{}) PeMgrErrno {

	//
	// notice: currntly the sub network identity for neighbor nodes should be
	// the same one which is specified in FindNode messages.
	//

	if msg == nil {
		log.LogCallerFileLine("peMgrDcvFindNodeRsp: invalid FindNode response")
		return PeMgrEnoParameter
	}

	var rsp = msg.(*sch.MsgDcvFindNodeRsp)
	if peMgr.dynamicSubNetIdExist(&rsp.Snid) != true {
		log.LogCallerFileLine("peMgrDcvFindNodeRsp: subnet not exist")
		return PeMgrEnoNotfound
	}

	var snid = rsp.Snid
	var appended = make(map[SubNetworkID]int, 0)
	var dup bool

	for _, n := range rsp.Nodes {

		//
		// Check if duplicated instances
		//

		if _, ok := peMgr.nodes[snid][n.ID]; ok {
			continue
		}

		//
		// Check if duplicated randoms
		//

		dup = false

		for _, rn := range peMgr.randoms[snid] {
			if rn.ID == n.ID {
				dup = true
				break
			}
		}

		if dup { continue }

		//
		// Check if duplicated statics
		//

		dup = false

		for _, s := range peMgr.cfg.staticNodes {
			if s.ID == n.ID && snid == peMgr.cfg.staticSubNetId {
				dup = true
				break
			}
		}

		if dup { continue }

		//
		// backup node, max to the number of most peers can be
		//

		if len(peMgr.randoms[snid]) >= peMgr.cfg.subNetMaxPeers[snid] {
			log.LogCallerFileLine("peMgrDcvFindNodeRsp: too much, some are truncated")
			continue
		}

		peMgr.randoms[snid] = append(peMgr.randoms[snid], n)
		appended[snid]++
	}

	//
	// drive ourself to startup outbound if some nodes appended, we set following message
	// body to be nil so all sub networks would be request to try to connect outbound.
	//

	for snid, _ := range appended {
		var schMsg sch.SchMessage
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &snid)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	return PeMgrEnoNone
}

//
// handler of timer for find node response expired
//
func (peMgr *PeerManager)peMgrDcvFindNodeTimerHandler(msg interface{}) PeMgrErrno {

	nwt := peMgr.cfg.networkType
	snid := msg.(*SubNetworkID)

	if nwt == config.P2pNewworkTypeStatic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.staticMaxOutbounds {
			return PeMgrEnoNone
		}
	} else if nwt == config.P2pNewworkTypeDynamic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
			return PeMgrEnoNone
		}
	}

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, snid)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoInternal
}

//
// Peer connection accepted indication handler
//
func (peMgr *PeerManager)peMgrLsnConnAcceptedInd(msg interface{}) PeMgrErrno {

	//
	// Here we are indicated that an inbound connection had been accepted. We should
	// check the number of the active inbound peer number currently to carry out action
	// accordingly.
	//

	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var ibInd = msg.(*msgConnAcceptedInd)

	//
	// Init peer instance control block
	//

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

	peInst.p2pkgLock	= sync.Mutex{}
	peInst.p2pkgRx		= nil
	peInst.p2pkgTx		= make([]*P2pPackage, 0, PeInstMaxP2packages)
	peInst.txDone		= make(chan PeMgrErrno, 1)
	peInst.txExit		= make(chan PeMgrErrno)
	peInst.rxDone		= make(chan PeMgrErrno, 1)
	peInst.rxExit		= make(chan PeMgrErrno)

	//
	// Create peer instance task
	//

	peMgr.ibInstSeq++

	var tskDesc  = sch.SchTaskDescription {
		Name:		fmt.Sprintf("inbound_%s", fmt.Sprintf("%d_", peMgr.ibInstSeq) + peInst.raddr.String()),
		MbSize:		PeInstMailboxSize,
		Ep:			peInst,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		peInst,
	}
	peInst.name = peInst.name + tskDesc.Name

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {

		log.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"SchCreateTask failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	peInst.ptnMe = ptnInst

	//
	// Send handshake request to the instance created aboved
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeHandshakeReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	//
	// Map the instance, notice that we do not konw the node identity yet since
	// this is an inbound connection just accepted at this moment.
	//

	peMgr.peers[peInst.ptnMe] = peInst

	//
	// Pause inbound peer accepter if necessary
	//

	if peMgr.ibpTotalNum++; peMgr.ibpTotalNum >= peMgr.cfg.ibpNumTotal {
		if !peMgr.cfg.noAccept && !peMgr.acceptPaused {
			log.LogCallerFileLine("peMgrLsnConnAcceptedInd: going to pause accepter, cfgName: %s", peMgr.cfg.cfgName)
			peMgr.acceptPaused = peMgr.accepter.PauseAccept()
		}
	}

	return PeMgrEnoNone
}

//
// Outbound request handler
//
func (peMgr *PeerManager)peMgrOutboundReq(msg interface{}) PeMgrErrno {

	var snid *SubNetworkID = nil
	if msg != nil {
		snid = msg.(*SubNetworkID)
	}

	if peMgr.cfg.noDial {

		log.LogCallerFileLine("peMgrOutboundReq: "+
			"abandon for noDial flag set: %t",
			peMgr.cfg.noDial)

		return PeMgrEnoNone
	}

	if peMgr.cfg.bootstrapNode {

		log.LogCallerFileLine("peMgrOutboundReq: "+
			"abandon for bootstrapNode flag set: %t",
			peMgr.cfg.bootstrapNode)

		return PeMgrEnoNone
	}

	//
	// if sub network identity is not specified, try start all
	//

	if snid == nil {

		if eno := peMgr.peMgrStaticSubNetOutbound(); eno != PeMgrEnoNone {

			return eno
		}

		if peMgr.cfg.networkType != config.P2pNewworkTypeStatic {

			for _, id := range peMgr.cfg.subNetIdList {

				if eno := peMgr.peMgrDynamicSubNetOutbound(&id); eno != PeMgrEnoNone {

					return eno
				}
			}
		}

	} else if peMgr.cfg.networkType == config.P2pNewworkTypeStatic &&
		*snid == peMgr.cfg.staticSubNetId {

		return peMgr.peMgrStaticSubNetOutbound()

	} else if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {

		if peMgr.dynamicSubNetIdExist(snid) == true {

			return peMgr.peMgrDynamicSubNetOutbound(snid)

		} else if peMgr.staticSubNetIdExist(snid) {

			return peMgr.peMgrStaticSubNetOutbound()

		}
	}

	return PeMgrEnoNotfound
}

//
// static outbound
//
func (peMgr *PeerManager)peMgrStaticSubNetOutbound() PeMgrErrno {

	if len(peMgr.cfg.staticNodes) == 0 {
		log.LogCallerFileLine("peMgrStaticSubNetOutbound: none of static nodes exist")
		return PeMgrEnoNone
	}

	snid := peMgr.cfg.staticSubNetId
	if peMgr.wrkNum[snid] >= peMgr.cfg.staticMaxPeers {
		log.LogCallerFileLine("peMgrStaticSubNetOutbound: peers full")
		return PeMgrEnoNone
	}

	if peMgr.obpNum[snid] >= peMgr.cfg.staticMaxOutbounds {
		log.LogCallerFileLine("peMgrStaticSubNetOutbound: outbounds full")
		return PeMgrEnoNone
	}

	var candidates = make([]*config.Node, 0)
	var count = 0

	for _, n := range peMgr.cfg.staticNodes {

		_, dup := peMgr.nodes[snid][n.ID];

		if !dup && peMgr.staticsStatus[n.ID] == peerIdle {

			candidates = append(candidates, n)
			count++
		}
	}

	//
	// Create outbound instances for candidates if any
	//

	var failed = 0
	var ok = 0

	for _, n := range candidates {

		if eno := peMgr.peMgrCreateOutboundInst(&snid, n); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrStaticSubNetOutbound: " +
				"create outbound instance failed, eno: %d", eno)

			if _, static := peMgr.staticsStatus[n.ID]; static {
				peMgr.staticsStatus[n.ID] = peerIdle
			}

			failed++
			continue
		}

		peMgr.staticsStatus[n.ID] = peerConnectOutInited
		ok++

		if peMgr.obpNum[snid] >= peMgr.cfg.staticMaxOutbounds {

			log.LogCallerFileLine("peMgrStaticSubNetOutbound: " +
				"too much candidates, the remains are discarded")

			break
		}
	}

	//
	// If outbounds are not enougth, ask discover to find more
	//

	if peMgr.obpNum[snid] < peMgr.cfg.staticMaxOutbounds {

		if eno := peMgr.peMgrAsk4More(&snid); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrOutboundReq: " +
				"peMgrAsk4More failed, eno: %d", eno)

			return eno
		}
	}

	return PeMgrEnoNone
}

//
// dynamic outbound
//
func (peMgr *PeerManager)peMgrDynamicSubNetOutbound(snid *SubNetworkID) PeMgrErrno {

	if peMgr.wrkNum[*snid] >= peMgr.cfg.subNetMaxPeers[*snid] {
		return PeMgrEnoNone
	}

	if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
		return PeMgrEnoNone
	}

	var candidates = make([]*config.Node, 0)
	var rdCnt = 0

	for _, n := range peMgr.randoms[*snid] {
		if _, ok := peMgr.nodes[*snid][n.ID]; !ok {
			candidates = append(candidates, n)
		}
		rdCnt++
	}

	if rdCnt > 0 {
		peMgr.randoms[*snid] = append(peMgr.randoms[*snid][:0], peMgr.randoms[*snid][rdCnt:]...)
	}

	//
	// Create outbound instances for candidates if any
	//

	var failed = 0
	var ok = 0

	maxOutbound := peMgr.cfg.subNetMaxOutbounds[*snid]

	for _, n := range candidates {

		if eno := peMgr.peMgrCreateOutboundInst(snid, n); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrDynamicSubNetOutbound: " +
				"create outbound instance failed, eno: %d", eno)

			failed++
			continue
		}

		ok++

		if peMgr.obpNum[*snid] >= maxOutbound {

			log.LogCallerFileLine("peMgrDynamicSubNetOutbound: " +
				"too much candidates, the remains are discarded")

			break
		}
	}

	//
	// If outbounds are not enougth, ask discover to find more
	//

	if peMgr.obpNum[*snid] < maxOutbound {

		if eno := peMgr.peMgrAsk4More(snid); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrDynamicSubNetOutbound: " +
				"peMgrAsk4More failed, eno: %d", eno)

			return eno
		}
	}

	return PeMgrEnoNone
}

//
// Outbound response handler
//
func (peMgr *PeerManager)peMgrConnOutRsp(msg interface{}) PeMgrErrno {

	//
	// This is an event from an instance task of outbound peer, telling the result
	// about action "connect to".
	//

	var rsp = msg.(*msgConnOutRsp)

	//
	// Check result
	//

	if rsp.result != PeMgrEnoNone {

		//
		// failed, kill instance
		//

		log.LogCallerFileLine("peMgrConnOutRsp: " +
			"outbound failed, result: %d, node: %s",
			rsp.result, fmt.Sprintf("%+v", rsp.peNode.ID))

		//
		// Notice: here the outgoing instance might have been killed in function
		// peMgrHandshakeRsp due to the duplication nodes, so we should check this
		// to kill it.
		//

		if _, lived := peMgr.peers[rsp.ptn]; lived {

			if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

				log.LogCallerFileLine("peMgrConnOutRsp: "+
					"peMgrKillInst failed, eno: %d",
					eno)

				return eno
			}

			//
			// since an outbound instance killed, we need to drive ourself to startup outbound
			//

			var schMsg = sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &rsp.snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}

		return PeMgrEnoNone
	}

	//
	// Send EvPeHandshakeReq to instance
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeHandshakeReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Handshake response handler
//
func (peMgr *PeerManager)peMgrHandshakeRsp(msg interface{}) PeMgrErrno {

	//
	// This is an event from an instance task of outbound or inbound peer, telling
	// the result about the handshake procedure between a pair of peers.
	// Notice: here we could receive response that sent by a peer instance task had
	// been killed for a duplicated inbound/outbound case, for details, see bellow
	// of this function please. We should check this case to discard the response
	// than do anything.
	//

	var rsp = msg.(*msgHandshakeRsp)
	var inst *peerInstance
	var lived bool

	if inst, lived = peMgr.peers[rsp.ptn]; inst == nil || !lived {

		log.LogCallerFileLine("peMgrHandshakeRsp: " +
			"instance not found, rsp: %s",
			fmt.Sprintf("%+v", *rsp))

		return PeMgrEnoNotfound
	}

	//
	// Check result, if failed, kill the instance
	//

	if rsp.result != PeMgrEnoNone {

		log.LogCallerFileLine("peMgrHandshakeRsp: " +
			"handshake failed, result: %d, node: %s",
			rsp.result,
			fmt.Sprintf("%X", rsp.peNode.ID))

		peMgr.updateStaticStatus(rsp.snid, rsp.peNode.ID, peerKilling)

		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrHandshakeRsp: " +
				"peMgrKillInst failed, node: %s",
				fmt.Sprintf("%X", rsp.peNode.ID))

			return eno
		}

		if rsp.dir == PeInstDirOutbound {
			var schMsg = sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &rsp.snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}

		return PeMgrEnoNone
	}

	//
	// Check duplicated for inbound instance. Notice: only here the peer manager can known the
	// identity of peer to determine if it's duplicated to a outbound instance, which is an
	// instance connect from local to outside.
	//

	var maxInbound = 0

	snid := rsp.snid
	id := rsp.peNode.ID

	if inst.dir == PeInstDirInbound {

		if peMgr.cfg.networkType == config.P2pNewworkTypeStatic &&
			peMgr.staticSubNetIdExist(&snid) == true {

			maxInbound = peMgr.cfg.staticMaxOutbounds

		} else if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {

			if peMgr.dynamicSubNetIdExist(&snid) == true {

				maxInbound = peMgr.cfg.subNetMaxInBounds[snid]

			} else if peMgr.staticSubNetIdExist(&snid) == true {

				maxInbound = peMgr.cfg.staticMaxOutbounds

			}
		}

		if peMgr.ibpNum[snid] >= maxInbound {

			log.LogCallerFileLine("peMgrHandshakeRsp: " +
				"too much inbound, subnet: %x, ibpNum: %d, max: %d",
				snid,
				peMgr.ibpNum[snid],
				maxInbound)

			peMgr.updateStaticStatus(snid, rsp.peNode.ID, peerKilling)

			if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

				log.LogCallerFileLine("peMgrHandshakeRsp: "+
					"peMgrKillInst failed, subnet: %x, node: %X",
					snid, rsp.peNode.ID)

				return eno
			}

			//
			// update ibpTotalNum and resume accepter if necessary
			//

			if peMgr.ibpTotalNum--; peMgr.ibpTotalNum < peMgr.cfg.ibpNumTotal {

				if peMgr.cfg.noAccept == false && peMgr.acceptPaused == true {

					log.LogCallerFileLine("peMgrHandshakeRsp: " +
						"going to resume accepter, cfgName: %s", peMgr.cfg.cfgName)

					peMgr.acceptPaused = !peMgr.accepter.ResumeAccept()
				}
			}

			return PeMgrEnoResource
		}

		if _, dup := peMgr.nodes[snid][id]; dup {

			log.LogCallerFileLine("peMgrHandshakeRsp: "+
				"duplicated, node: %s",
				fmt.Sprintf("%X", rsp.peNode.ID))

			//
			// Here we could not kill instance rudely, the instance state should be
			// compared with each other to determine whom would be killed. Since here
			// handshake response received, this duplicated inbound instance must be
			// in "handshook" state.
			//

			var ptn2Kill interface{} = nil
			var node2Kill *config.Node = nil

			dupInst := peMgr.nodes[snid][id]
			cmp := inst.state.compare(dupInst.state)
			obKilled := false

			if cmp < 0 {
				ptn2Kill = rsp.ptn
				node2Kill = rsp.peNode
			} else if cmp > 0 {
				ptn2Kill = dupInst.ptnMe
				node2Kill = &dupInst.node
				obKilled = dupInst.dir == PeInstDirOutbound
			} else {
				if rand.Int() & 0x01 == 0 {
					ptn2Kill = rsp.ptn
					node2Kill = rsp.peNode
				} else {
					ptn2Kill = dupInst.ptnMe
					node2Kill = &dupInst.node
					obKilled = dupInst.dir == PeInstDirOutbound
				}
			}

			//
			// Kill instance selected above. Notice: the one to be killed might be busy in
			// handshake procedure (must be the inbound one), if it's killed, the peer manager
			// might receive a handshake response message without mapping rsp.ptn to instance
			// pointer, see function peMgrKillInst please, the map between these twos removed
			// there, so the peer manager must check this case to discard that response. See
			// above of this function(handshake response handler) please.
			//

			log.LogCallerFileLine("peMgrHandshakeRsp: " +
				"node2Kill: %s",
				fmt.Sprintf("%X", *node2Kill))

			peMgr.updateStaticStatus(snid, node2Kill.ID, peerKilling)

			if eno := peMgr.peMgrKillInst(ptn2Kill, node2Kill); eno != PeMgrEnoNone {

				log.LogCallerFileLine("peMgrHandshakeRsp: "+
					"peMgrKillInst failed, node: %s",
					fmt.Sprintf("%X", rsp.peNode.ID))

				return eno
			}

			//
			// If the response instance killed, return then
			//

			if ptn2Kill == rsp.ptn {
				return PeMgrEnoDuplicaated
			}

			//
			// If the killed on is an outbound instance, we need to send EvPeOutboundReq
			// to peer manager so it can try outgoing connection.
			//

			if obKilled {
				var schMsg= sch.SchMessage{}
				peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &snid)
				peMgr.sdl.SchSendMessage(&schMsg)
			}
		}
	}

	//
	// Send EvPeEstablishedInd to instance
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeEstablishedInd, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	//
	// Map the instance, notice that, only at this moment we can know the node
	// identity of a inbound peer.
	//

	peMgr.updateStaticStatus(snid, rsp.peNode.ID, peerActivated)

	inst.state = peInstStateActivated
	peMgr.workers[snid][id] = inst
	peMgr.wrkNum[snid]++

	if inst.dir == PeInstDirInbound {
		peMgr.nodes[snid][id] = inst
		peMgr.ibpNum[snid]++
	}

	//
	// Since the peer node is accepted and handshake is passed here now,
	// we add this peer node to bucket. But notice that this operation
	// possible fail for some reasons such as it's a duplicated one. We
	// should not care the result returned from interface of table module.
	//

	if inst.dir == PeInstDirInbound  &&
		inst.peMgr.cfg.networkType != config.P2pNewworkTypeStatic {

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

			log.LogCallerFileLine("peMgrHandshakeRsp: "+
				"TabBucketAddNode failed, node: %s",
				fmt.Sprintf("%+v", *rsp.peNode))
		}

		//
		// Backup peer node to node database. Notice that this operation
		// possible fail for some reasons such as it's a duplicated one. We
		// should not care the result returned from interface of table module.
		//

		tabEno = peMgr.tabMgr.TabUpdateNode(snid, &n)
		if tabEno != tab.TabMgrEnoNone {

			log.LogCallerFileLine("peMgrHandshakeRsp: "+
				"TabUpdateNode failed, node: %s",
				fmt.Sprintf("%+v", *rsp.peNode))
		}
	}

	return PeMgrEnoNone
}

//
// Pingpong response handler
//
func (peMgr *PeerManager)peMgrPingpongRsp(msg interface{}) PeMgrErrno {

	//
	// This is an event from an instance task of outbound or inbound peer, telling
	// the result about pingpong procedure between a pair of peers.
	//

	var rsp = msg.(*msgPingpongRsp)

	//
	// Check result
	//

	if rsp.result != PeMgrEnoNone {

		//
		// failed, kill instance
		//

		log.LogCallerFileLine("peMgrPingpongRsp: " +
			"outbound failed, result: %d, node: %s",
			rsp.result, config.P2pNodeId2HexString(rsp.peNode.ID))

		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

			log.LogCallerFileLine("peMgrPingpongRsp: " +
				"kill instance failed, inst: %s, node: %s",
				peMgr.sdl.SchGetTaskName(rsp.ptn),
				config.P2pNodeId2HexString(rsp.peNode.ID)	)
		}
	}

	return PeMgrEnoNone
}

//
// Event request to close peer handler
//
func (peMgr *PeerManager)peMgrCloseReq(msg interface{}) PeMgrErrno {

	//
	// This is an event from other module requests to close a peer connection,
	// or sent by peer manager itself while pingpong failed. All cases the instance
	// is in WORKING state. The peer to be closed should be included in the message
	// passed in.
	//

	var req = msg.(*sch.MsgPeCloseReq)
	var snid = req.Snid

	inst := peMgr.nodes[snid][req.Node.ID]

	if inst == nil {
		log.LogCallerFileLine("peMgrCloseReq: none of instance for subnet: %x, id: %x", snid, req.Node.ID)
		return PeMgrEnoNotfound
	}

	if inst.killing {
		log.LogCallerFileLine("peMgrCloseReq: try to kill same instance twice")
		return PeMgrEnoDuplicaated
	}

	//
	// Send close-request to instance
	//

	var schMsg = sch.SchMessage{}

	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)

	inst.killing = true
	peMgr.updateStaticStatus(snid, req.Node.ID, peerKilling)

	return PeMgrEnoNone
}

//
// Peer connection closed confirm handler
//
func (peMgr *PeerManager)peMgrConnCloseCfm(msg interface{}) PeMgrErrno {

	//
	// This is an event from an instance task of outbound or inbound peer whom
	// is required to be closed by the peer manager, confiming that the connection
	// had been closed.
	//

	var eno PeMgrErrno
	var cfm = msg.(*MsgCloseCfm)

	//
	// Do not care the result, kill in anyway
	//

	if cfm.result != PeMgrEnoNone {

		log.LogCallerFileLine("peMgrConnCloseCfm, " +
			"result: %d, node: %s",
			cfm.result, config.P2pNodeId2HexString(cfm.peNode.ID))
	}

	if eno = peMgr.peMgrKillInst(cfm.ptn, cfm.peNode); eno != PeMgrEnoNone {

		log.LogCallerFileLine("peMgrConnCloseCfm: " +
			"kill instance failed, inst: %s, node: %s",
			peMgr.sdl.SchGetTaskName(cfm.ptn),
			config.P2pNodeId2HexString(cfm.peNode.ID))

		return PeMgrEnoScheduler
	}

	peMgr.updateStaticStatus(cfm.snid, cfm.peNode.ID, peerIdle)

	//
	// callback to the user of p2p to tell peer closed
	//

	peMgr.Lock4Cb.Lock()

	if peMgr.P2pIndHandler != nil {

		para := P2pIndPeerClosedPara {
			Ptn:		peMgr.ptnMe,
			Snid:		cfm.snid,
			PeerId:		cfm.peNode.ID,
		}

		peMgr.P2pIndHandler(P2pIndPeerClosed, &para)

	} else {
		log.LogCallerFileLine("peMgrConnCloseCfm: indication callback not installed yet")
	}

	peMgr.Lock4Cb.Unlock()


	//
	// since we had lost a peer, we need to drive ourself to startup outbound
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &cfm.snid)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Peer connection closed indication handler
//
func (peMgr *PeerManager)peMgrConnCloseInd(msg interface{}) PeMgrErrno {

	//
	// This is an event from an instance task of outbound or inbound peer whom
	// is not required to be closed by the peer manager, but the connection had
	// been closed for some other reasons.
	//
	// Notice: we need EvPeOutboundReq to be sent to drive the outbound, when an
	// inbound instance killed, in currently implement, we have to send EvPeOutboundReq
	// in handshake procedure when instances killed and we had to do the same in
	// pingpong procedure.
	//

	var ind = msg.(*MsgCloseInd)

	//
	// Do not care the result, kill always
	//

	if eno := peMgr.peMgrKillInst(ind.ptn, ind.peNode); eno != PeMgrEnoNone {

		log.LogCallerFileLine("peMgrConnCloseInd: " +
			"kill instance failed, inst: %s, node: %s",
			peMgr.sdl.SchGetTaskName(ind.ptn),
			config.P2pNodeId2HexString(ind.peNode.ID))

		return PeMgrEnoScheduler
	}

	//
	// callback to the user of p2p to tell peer closed
	//

	peMgr.Lock4Cb.Lock()

	if peMgr.P2pIndHandler != nil {

		para := P2pIndPeerClosedPara {
			Ptn:		peMgr.ptnMe,
			Snid:		ind.snid,
			PeerId:		ind.peNode.ID,
		}

		peMgr.P2pIndHandler(P2pIndPeerClosed, &para)

	} else {
		log.LogCallerFileLine("peMgrConnCloseInd: indication callback not installed yet")
	}

	peMgr.Lock4Cb.Unlock()

	//
	// since we had lost a peer, we need to drive ourself to startup outbound
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &ind.snid)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Create outbound instance
//
func (peMgr *PeerManager)peMgrCreateOutboundInst(snid *config.SubNetworkID, node *config.Node) PeMgrErrno {

	//
	// Create outbound task instance for specific node
	//

	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil

	//
	// Init peer instance control block
	//

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

	peInst.p2pkgLock	= sync.Mutex{}
	peInst.p2pkgRx		= nil
	peInst.p2pkgTx		= make([]*P2pPackage, 0, PeInstMaxP2packages)
	peInst.txDone		= make(chan PeMgrErrno, 1)
	peInst.txExit		= make(chan PeMgrErrno)
	peInst.rxDone		= make(chan PeMgrErrno, 1)
	peInst.rxExit		= make(chan PeMgrErrno)

	//
	// Create peer instance task
	//

	peMgr.obInstSeq++

	var tskDesc  = sch.SchTaskDescription {
		Name:		fmt.Sprintf("Outbound_%s", fmt.Sprintf("%d", peMgr.obInstSeq)),
		MbSize:		PeInstMailboxSize,
		Ep:			peInst,
		Wd:			&sch.SchWatchDog{HaveDog:false,},
		Flag:		sch.SchCreatedGo,
		DieCb:		nil,
		UserDa:		peInst,
	}
	peInst.name = peInst.name + tskDesc.Name

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {

		log.LogCallerFileLine("peMgrCreateOutboundInst: " +
			"SchCreateTask failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	//
	// Map the instance
	//

	peInst.ptnMe = ptnInst
	peMgr.peers[peInst.ptnMe] = peInst
	peMgr.nodes[*snid][peInst.node.ID] = peInst
	peMgr.obpNum[*snid]++

	//
	// Send EvPeConnOutReq request to the instance created aboved
	//

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeConnOutReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Kill specific instance
//
func (peMgr *PeerManager)peMgrKillInst(ptn interface{}, node *config.Node) PeMgrErrno {

	if ptn == nil && node == nil {
		log.LogCallerFileLine("peMgrKillInst: invalid parameters")
		return PeMgrEnoParameter
	}

	var peInst = peMgr.peers[ptn]
	if peInst == nil {

		log.LogCallerFileLine("peMgrKillInst: " +
			"instance not found, node: %s",
			config.P2pNodeId2HexString(node.ID))

		return PeMgrEnoNotfound
	}

	if peInst.ppTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(ptn, peInst.ppTid)
		peInst.ppTid = sch.SchInvalidTid
	}

	if peInst.conn != nil {
		peInst.conn.Close()
		peInst.conn = nil
	}

	//
	// Stop instance task
	//

	peMgr.sdl.SchStopTask(ptn)

	//
	// Remove maps for the node: we must check the instance state and connection
	// direction to step.
	//

	snid := peInst.snid
	id := peInst.node.ID

	if peInst.state == peInstStateActivated {

		delete(peMgr.workers[snid], id)
		peMgr.wrkNum[snid]--
	}

	if peInst.dir == PeInstDirOutbound {

		delete(peMgr.nodes[snid], id)
		delete(peMgr.peers, ptn)

	} else if peInst.dir == PeInstDirInbound {

		delete(peMgr.peers, ptn)
		if peInst.state == peInstStateActivated {
			delete(peMgr.nodes[snid], id)
		}
	}

	if peInst.dir == PeInstDirOutbound {

		peMgr.obpNum[snid]--

	} else if peInst.dir == PeInstDirInbound {

		peMgr.ibpTotalNum--

		if peInst.state == peInstStateActivated {

			peMgr.ibpNum[snid]--

		}
	} else {

		log.LogCallerFileLine("peMgrKillInst: " +
			"invalid peer instance direction: %d",
			peInst.dir)
	}

	//
	// Try update static ndoe status
	//

	peMgr.updateStaticStatus(snid, node.ID, peerIdle)

	//
	// Check if the accepter task paused, resume it if necessary
	//

	if peMgr.cfg.noAccept == false && peMgr.acceptPaused == true {
		log.LogCallerFileLine("peMgrLsnConnAcceptedInd: going to resume accepter, cfgName: %s", peMgr.cfg.cfgName)
		peMgr.acceptPaused = !peMgr.accepter.ResumeAccept()
	}

	return PeMgrEnoNone
}

//
// Request the discover task to findout more node for outbound
//
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

	//
	// no discovering needed for static nodes, only dynamics need it
	//

	dur := durStaticRetryTimer

	if *snid != peMgr.cfg.staticSubNetId {

		dur = durDcvFindNodeTimer

		more := peMgr.cfg.subNetMaxOutbounds[*snid] - peMgr.obpNum[*snid]

		if more <= 0 {

			log.LogCallerFileLine("peMgrAsk4More: "+
				"no more needed, obpNum: %d, max: %d",
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

	//
	// set a ABS timer
	//

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

//
// Dynamic peer task
//
const peInstTaskName = "peInstTsk"

const (
	peInstStateNull		= iota	// null
	peInstStateConnOut			// outbound connection inited
	peInstStateAccepted			// inbound accepted, need handshake
	peInstStateConnected		// outbound connected, need handshake
	peInstStateHandshook		// handshook
	peInstStateActivated		// actived in working
	peInstStateKilledReq		// peer manager is required to kill the instance
)

type peerInstState int	// instance state type

const PeInstDirNull			= 0		// null, so connection should be nil
const PeInstDirOutbound		= +1	// outbound connection
const PeInstDirInbound		= -1	// inbound connection

const PeInstMailboxSize 	= 32				// mailbox size
const PeInstMaxP2packages	= 128				// max p2p packages pending to be sent
const PeInstMaxPingpongCnt	= 4					// max pingpong counter value
const PeInstPingpongCycle	= time.Second *2	// pingpong period

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
	p2pkgRx		P2pPkgCallback				// incoming p2p package callback
	p2pkgTx		[]*P2pPackage				// outcoming p2p packages
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

//
// Clear seen with Explicit initialization
//
var peerInstDefault = peerInstance {
	name:		peInstTaskName,
	tep:		nil,
	ptnMe:		nil,
	ptnMgr:		nil,
	state:		peInstStateNull,
	cto:		0,
	hto:		0,
	dialer:		nil,
	conn:		nil,
	iow:		nil,
	ior:		nil,
	laddr:		nil,
	raddr:		nil,
	dir:		PeInstDirNull,
	node:		config.Node{},
	maxPkgSize:	maxTcpmsgSize,
	protoNum:	0,
	protocols:	[]Protocol{{}},
	ppTid:		sch.SchInvalidTid,
	p2pkgLock:	sync.Mutex{},
	p2pkgRx:	nil,
	p2pkgTx:	nil,
	txDone:		nil,
	txExit:		nil,
	rxDone:		nil,
	rxExit:		nil,
	ppSeq:		0,
	ppCnt:		0,
	rxEno:		PeMgrEnoNone,
	txEno:		PeMgrEnoNone,
	ppEno:		PeMgrEnoNone,
}

//
// EvPeConnOutRsp message
//
type msgConnOutRsp struct {
	result	PeMgrErrno				// result of outbound connect action
	snid	config.SubNetworkID		// sub network identity
	peNode 	*config.Node			// target node
	ptn		interface{}			// pointer to task instance node of sender
}

//
// EvPeHandshakeRsp message
//
type msgHandshakeRsp struct {
	result	PeMgrErrno			// result of handshake action
	dir		int					// inbound or outbound
	snid	config.SubNetworkID	// sub network identity
	peNode 	*config.Node		// target node
	ptn		interface{}			// pointer to task instance node of sender
}

//
// EvPePingpongRsp message
//
type msgPingpongRsp struct {
	result	PeMgrErrno		// result of pingpong action
	peNode 	*config.Node	// target node
	ptn		interface{}		// pointer to task instance node of sender
}

//
// EvPeCloseCfm message
//
type MsgCloseCfm struct {
	result	PeMgrErrno			// result of pingpong action
	snid	config.SubNetworkID	// sub network identity
	peNode 	*config.Node		// target node
	ptn		interface{}			// pointer to task instance node of sender
}

//
// EvPeCloseInd message
//
type MsgCloseInd struct {
	cause	PeMgrErrno			// tell why it's closed
	snid	config.SubNetworkID	// sub network identity
	peNode 	*config.Node		// target node
	ptn		interface{}			// pointer to task instance node of sender
}

//
// EvPePingpongReq message
//
type MsgPingpongReq struct {
	seq		uint64		// init sequence no.
}

//
// Entry point exported to shceduler
//
func (pi *peerInstance)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return pi.tep(ptn, msg)
}

//
// Peer instance entry
//
func (pi *peerInstance)peerInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	var eno PeMgrErrno

	switch msg.Id {

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

	if eno != PeMgrEnoNone {
		log.LogCallerFileLine("PeerInstProc: instance errors, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Outbound connect to peer request handler
//
func (inst *peerInstance)piConnOutReq(msg interface{}) PeMgrErrno {

	_ = msg

	//
	// Check instance
	//

	if inst.dialer == nil ||
		inst.dir != PeInstDirOutbound  ||
		inst.state != peInstStateConnOut {
		log.LogCallerFileLine("piConnOutReq: instance mismatched")
		return PeMgrEnoInternal
	}

	//
	// Dial to peer node
	//

	var addr = &net.TCPAddr{IP: inst.node.IP, Port: int(inst.node.TCP)}
	var conn net.Conn = nil
	var err error
	var eno PeMgrErrno = PeMgrEnoNone

	inst.dialer.Timeout = inst.cto

	if conn, err = inst.dialer.Dial("tcp", addr.String()); err != nil {

		log.LogCallerFileLine("piConnOutReq: " +
			"dial failed, to: %s, err: %s",
			addr.String(), err.Error())

		eno = PeMgrEnoOs

	} else {

		//
		// Backup connection and update instance state
		//

		inst.conn = conn
		inst.laddr = conn.LocalAddr().(*net.TCPAddr)
		inst.raddr = conn.RemoteAddr().(*net.TCPAddr)
		inst.state = peInstStateConnected

		log.LogCallerFileLine("piConnOutReq: " +
			"dial ok, laddr: %s, raddr: %s",
			inst.laddr.String(),
			inst.raddr.String())
	}

	//
	// Response to peer manager task
	//

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

//
// Handshake request handler
//
func (inst *peerInstance)piHandshakeReq(msg interface{}) PeMgrErrno {

	_ = msg

	//
	// Check instance
	//

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

	//
	// Carry out action according to the direction of current peer instance
	// connection.
	//

	var eno PeMgrErrno

	if inst.dir == PeInstDirInbound {

		eno = inst.piHandshakeInbound(inst)

	} else if inst.dir == PeInstDirOutbound {

		eno = inst.piHandshakeOutbound(inst)

	} else {

		log.LogCallerFileLine("piHandshakeReq: " +
			"invalid instance direction: %d",
			inst.dir)

		eno = PeMgrEnoInternal
	}

	log.LogCallerFileLine("piHandshakeReq: " +
			"handshake result: %d, dir: %d, laddr: %s, raddr: %s, peer: %s",
			eno,
			inst.dir,
			inst.laddr.String(),
			inst.raddr.String(),
			fmt.Sprintf("%+v", inst.node)	)

	//
	// response to peer manager with handshake result
	//

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

//
// Ping-Request handler
//
func (inst *peerInstance)piPingpongReq(msg interface{}) PeMgrErrno {

	//
	// The ping procedure is fired by a timer internal the peer task
	// instance, or from outside module for some purpose. Notice, it
	// is just for "ping" here, not for "pong" which is sent when peer
	// ping message received.
	//
	// Notice:
	//
	// If errors had been fired on the conection, we should return do
	// nothing;
	//
	// If the connection had been closed, we should not try to ping,
	// this is possible for the message needs some time to be shcedled
	// here.
	//

	if inst.ppEno != PeMgrEnoNone {

		log.LogCallerFileLine("piPingpongReq: " +
			"nothing done, ppEno: %d",
			inst.ppEno)

		return PeMgrEnoResource
	}

	if inst.conn == nil {

		log.LogCallerFileLine("piPingpongReq: " +
			"connection had been closed")

		return PeMgrEnoResource
	}

	if msg != nil {

		log.LogCallerFileLine("piPingpongReq: " +
			"ppSeq: %d, will be inited to be: %d",
			inst.ppSeq, msg.(*MsgPingpongReq).seq)

		inst.ppSeq = msg.(*MsgPingpongReq).seq
	}

	ping := Pingpong {
		Seq:	inst.ppSeq,
		Extra:	nil,
	}

	inst.ppSeq++

	upkg := new(P2pPackage)
	if eno := upkg.ping(inst, &ping); eno != PeMgrEnoNone {

		//
		// failed, we callback to tell user about this
		//

		log.LogCallerFileLine("piPingpongReq: " +
			"upkg.ping failed, eno: %d, peer: %s",
			eno,
			fmt.Sprintf("%X", inst.node.ID))

		inst.peMgr.Lock4Cb.Lock()

		inst.ppEno = eno

		if inst.peMgr.P2pIndHandler != nil {

			para := P2pIndConnStatusPara {
				Ptn:		inst.ptnMe,
				PeerInfo:	&Handshake {
					Snid:		inst.snid,
					NodeId:		inst.node.ID,
					ProtoNum:	inst.protoNum,
					Protocols:	inst.protocols,
				},
				Status		:	int(eno),
				Flag		:	false,
				Description	:"piPingpongReq: upkg.ping failed",
			}

			inst.peMgr.P2pIndHandler(P2pIndConnStatus, &para)

		} else {
			log.LogCallerFileLine("piPingpongReq: indication callback not installed yet")
		}

		inst.peMgr.Lock4Cb.Unlock()

		return eno
	}

	return PeMgrEnoNone
}

//
// Close-Request handler
//
func (inst *peerInstance)piCloseReq(msg interface{}) PeMgrErrno {

	//
	// Notice: do not kill the instance task here in this function, just the
	// connection of the peer is closed, and event EvPeCloseCfm sent to the
	// peer manager. The instance task would be killed by peer manager when
	// EvPeCloseCfm event received, see it pls.
	//

	_ = msg

	if inst == nil {
		log.LogCallerFileLine("piCloseReq: invalid parameters")
		return PeMgrEnoParameter
	}

	var node = inst.node

	//
	// stop tx/rx rontines
	//

	if inst.state == peInstStateActivated {

		inst.rxDone <- PeMgrEnoNone
		<-inst.rxExit

		inst.txDone <- PeMgrEnoNone
		<-inst.txExit
	}

	close(inst.rxDone)
	inst.rxDone = nil
	close(inst.rxExit)
	inst.rxExit = nil
	close(inst.txDone)
	inst.txDone = nil
	close(inst.txExit)
	inst.txExit = nil

	inst.p2pkgLock.Lock()
	inst.p2pkgRx = nil
	inst.p2pkgTx = nil
	inst.p2pkgLock.Unlock()

	//
	// stop timer
	//

	if inst.ppTid != sch.SchInvalidTid {
		inst.sdl.SchKillTimer(inst.ptnMe, inst.ppTid)
		inst.ppTid = sch.SchInvalidTid
	}

	//
	// close connection
	//

	if inst.conn != nil {

		if err := inst.conn.Close(); err != nil {

			log.LogCallerFileLine("piCloseReq: " +
				"close connection failed, err: %s",
				err.Error())

			return PeMgrEnoOs
		}

		inst.conn = nil
	}

	//
	// send close-confirm to peer manager
	//

	var req = MsgCloseCfm {
		result: PeMgrEnoNone,
		snid:	inst.snid,
		peNode:	&node,
		ptn:	inst.ptnMe,
	}

	var schMsg = sch.SchMessage{}

	inst.sdl.SchMakeMessage(&schMsg, inst.peMgr.ptnMe, inst.peMgr.ptnMe, sch.EvPeCloseCfm, &req)
	inst.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Peer-Established indication handler
//
func (inst *peerInstance)piEstablishedInd( msg interface{}) PeMgrErrno {

	//
	// When sch.EvPeEstablishedInd received, an peer instance should go into serving,
	// means data sending and receiving. In this case, an instance should first the
	// pingpong timer, and then update the instance state, and make anything ready to
	// serve for peers interaction. Currently, no response event is defined for peer
	// manager, says that the manager always believe that a peer instance must be in
	// service after it sending the sch.EvPeEstablishedInd, and would not wait any
	// response about this event sent.
	//

	var schEno sch.SchErrno
	_ = msg

	//
	// setup pingpong timer
	//

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
		log.LogCallerFileLine("piEstablishedInd: SchSetTimer failed, eno: %d", schEno)
		return PeMgrEnoScheduler
	}

	inst.ppTid = tid

	//
	// modify deadline of peer connection for we had set specific value while
	// handshake procedure. we set deadline to value 0, so action on connection
	// would be blocked until it's completed.
	//

	inst.txEno = PeMgrEnoNone
	inst.rxEno = PeMgrEnoNone
	inst.ppEno = PeMgrEnoNone
	inst.conn.SetDeadline(time.Time{})

	//
	// callback to the user of p2p
	//

	inst.peMgr.Lock4Cb.Lock()

	if inst.peMgr.P2pIndHandler != nil {

		para := P2pIndPeerActivatedPara {
			Ptn: inst.ptnMe,
			PeerInfo: & Handshake {
				Snid:		inst.snid,
				NodeId:		inst.node.ID,
				ProtoNum:	inst.protoNum,
				Protocols:	inst.protocols,
			},
		}

		inst.peMgr.P2pIndHandler(P2pIndPeerActivated, &para)

	} else {
		log.LogCallerFileLine("piEstablishedInd: indication callback not installed yet")
	}

	inst.peMgr.Lock4Cb.Unlock()

	//
	// :( here we go routines for tx/rx on the activated peer):
	//

	go piTx(inst)
	go piRx(inst)

	log.LogCallerFileLine("piEstablishedInd: " +
		"piTx and piRx are in going ... inst: %s",
		fmt.Sprintf("%+v", *inst))

	return PeMgrEnoNone
}

//
// Pingpong timer handler
//
func (inst *peerInstance)piPingpongTimerHandler() PeMgrErrno {

	//
	// This timer is for pingpong after peer is established, as heartbit.
	// We send EvPePingpongReq event to ourselves with nil message, see
	// this event handler pls.
	//
	// Also, here we need to check the pingpong counter to findout if it
	// reachs to the threshold(this is a simple method, in fact, we can
	// do better, basing on the pingpong procedure).
	//

	schMsg := sch.SchMessage{}

	//
	// Check the pingpong timer: when this expired event comes, the timer
	// might have been stop for instance currently in closing procedure.
	// We discard this event in this case.
	//

	if inst.ppTid == sch.SchInvalidTid {
		log.LogCallerFileLine("piPingpongTimerHandler: no timer, discarded")
		return PeMgrEnoNone
	}

	//
	// Check pingpong counter with the threshold
	//

	if inst.ppCnt++; inst.ppCnt > PeInstMaxPingpongCnt {

		//
		// callback to tell user about this, and then close the connection
		// of this peer instance.
		//

		log.LogCallerFileLine("piPingpongTimerHandler: " +
			"call P2pIndHandler noping threshold reached, ppCnt: %d",
			inst.ppCnt)

		inst.peMgr.Lock4Cb.Lock()

		if inst.peMgr.P2pIndHandler != nil {

			para := P2pIndConnStatusPara {
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

			inst.peMgr.P2pIndHandler(P2pIndConnStatus, &para)

		} else {
			log.LogCallerFileLine("piPingpongTimerHandler: indication callback not installed yet")
		}

		inst.peMgr.Lock4Cb.Unlock()

		//
		// close the peer instance
		//

		inst.sdl.SchMakeMessage(&schMsg, inst.ptnMe, inst.ptnMe, sch.EvPeCloseReq, nil)
		inst.sdl.SchSendMessage(&schMsg)

		return PeMgrEnoNone
	}

	//
	// Send pingpong request
	//

	inst.sdl.SchMakeMessage(&schMsg, inst.ptnMe, inst.ptnMe, sch.EvPePingpongReq, nil)
	inst.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Data-Request(send data) handler
//
func (inst *peerInstance)piDataReq(msg interface{}) PeMgrErrno {
	_ = msg
	return PeMgrEnoNone
}

//
// Handshake for inbound
//
func (pi *peerInstance)piHandshakeInbound(inst *peerInstance) PeMgrErrno {

	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs *Handshake

	//
	// read inbound handshake from remote peer
	//

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {

		log.LogCallerFileLine("piHandshakeInbound: " +
			"read inbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	//
	// check subnet identity
	//

	if inst.peMgr.dynamicSubNetIdExist(&hs.Snid) == false &&
		inst.peMgr.staticSubNetIdExist(&hs.Snid) == false {

		log.LogCallerFileLine("piHandshakeInbound: " +
			"local node does not attach to subnet: %x",
			hs.Snid)

		return PeMgrEnoNotfound
	}

	//
	// backup info about protocols supported by peer. notice that here we can
	// check against the ip and tcp port from handshake with that obtained from
	// underlying network, but we not now.
	//

	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	inst.snid = hs.Snid
	inst.node.ID = hs.NodeId
	inst.node.IP = append(inst.node.IP, hs.IP...)
	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)

	//
	// write outbound handshake to remote peer
	//

	hs.Snid = inst.snid
	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = pi.peMgr.cfg.protocols

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {

		log.LogCallerFileLine("piHandshakeInbound: " +
			"write outbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	//
	// update instance state
	//

	inst.state = peInstStateHandshook

	return PeMgrEnoNone
}

//
// Handshake for outbound
//
func (pi *peerInstance)piHandshakeOutbound(inst *peerInstance) PeMgrErrno {

	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs = new(Handshake)

	//
	// write outbound handshake to remote peer
	//

	hs.Snid = pi.snid
	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = append(hs.Protocols, pi.peMgr.cfg.protocols ...)

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {

		log.LogCallerFileLine("piHandshakeOutbound: " +
			"write outbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	//
	// read inbound handshake from remote peer
	//

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {

		log.LogCallerFileLine("piHandshakeOutbound: " +
			"read inbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	//
	// check sub network identity
	//

	if hs.Snid != inst.snid {
		log.LogCallerFileLine("piHandshakeOutbound: subnet identity mismathced")
		return PeMgrEnoMessage
	}

	//
	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos,
	// and we update the remains.
	//

	if hs.NodeId != inst.node.ID {
		log.LogCallerFileLine("piHandshakeOutbound: node identity mismathced")
		return PeMgrEnoMessage
	}

	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)
	inst.node.IP = append(inst.node.IP, hs.IP ...)

	//
	// backup info about protocols supported by peer;
	// update instance state;
	//

	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	inst.state = peInstStateHandshook

	return PeMgrEnoNone
}

//
// Set callback for incoming packages
//
func SetP2pkgCallback(cb interface{}, ptn interface{}) PeMgrErrno {

	if ptn == nil {
		log.LogCallerFileLine("SetP2pkgCallback: invalid parameters")
		return PeMgrEnoParameter
	}

	sdl := sch.SchGetScheduler(ptn)
	inst := sdl.SchGetUserDataArea(ptn).(*peerInstance)

	if inst == nil {

		log.LogCallerFileLine("SetP2pkgCallback: " +
			"nil instance data area, task: %s",
			inst.sdl.SchGetTaskName(ptn))

		return PeMgrEnoUnknown
	}

	inst.p2pkgLock.Lock()
	defer inst.p2pkgLock.Unlock()

	if inst.p2pkgRx != nil {
		log.LogCallerFileLine("SetP2pkgCallback: old one will be overlapped")
	}
	inst.p2pkgRx = cb.(P2pPkgCallback)

	return PeMgrEnoNone
}

//
// Send package
//
func SendPackage(pkg *P2pPackage2Peer) (PeMgrErrno, []*PeerId){

	//
	// Notice: if PeMgrEnoParameter returned, then the fail list return with nil,
	// but in this case, sendind to all peers are failed.
	//

	if pkg == nil {
		log.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter, nil
	}

	if len(pkg.IdList) == 0 {
		log.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter, nil
	}

	peMgr := pkg.P2pInst.SchGetUserTaskIF(sch.PeerMgrName).(*PeerManager)
	peMgr.txLock.Lock()
	defer peMgr.txLock.Unlock()

	var failed = make([]*PeerId, 0)
	var inst *peerInstance = nil

	for _, pid := range pkg.IdList {

		if inst = peMgr.workers[pkg.SubNetId][pid]; inst == nil {

			log.LogCallerFileLine("SendPackage: " +
				"instance not exist, id: %s",
				fmt.Sprintf("%X", pid))

			failed = append(failed, &pid)
			continue
		}

		inst.p2pkgLock.Lock()

		if len(inst.p2pkgTx) >= PeInstMaxP2packages {
			log.LogCallerFileLine("SendPackage: tx buffer full")
			failed = append(failed, &pid)
			inst.p2pkgLock.Unlock()
			continue
		}

		_pkg := new(P2pPackage)
		_pkg.Pid = uint32(pkg.ProtoId)
		_pkg.PayloadLength = uint32(pkg.PayloadLength)
		_pkg.Payload = append(_pkg.Payload, pkg.Payload...)

		inst.p2pkgTx = append(inst.p2pkgTx, _pkg)

		inst.p2pkgLock.Unlock()
	}

	if len(failed) == 0 {
		return PeMgrEnoNone, nil
	}

	return PeMgrEnoUnknown, failed
}

//
// Close connection to a peer
//
func (peMgr *PeerManager)ClosePeer(snid *SubNetworkID, id *PeerId) PeMgrErrno {

	//
	// Notice: this function should only be called to kill instance when it
	// is in active state(peInstStateActivated), if it's not the case, one
	// should call peMgrKillInst to do that, see it pls.
	//

	//
	// get instance by its' identity passed in
	//

	var inst *peerInstance = nil

	if inst = peMgr.workers[*snid][config.NodeID(*id)]; inst == nil {

		log.LogCallerFileLine("ClosePeer: " +
			"instance not exist, id: %s",
			fmt.Sprintf("%X", *id))

		return PeMgrEnoUnknown
	}

	//
	// send close-request to peer manager
	//

	var req = sch.MsgPeCloseReq {
		Ptn:	inst.ptnMe,
		Node:	inst.node,
	}

	var schMsg = sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

//
// Instance TX routine
//
func piTx(inst *peerInstance) PeMgrErrno {

	//
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When use try to close the peer, this routine
	// would then exit.
	//

	var done PeMgrErrno = PeMgrEnoNone

txBreak:

	for {

		//
		// check if we are done
		//

		select {

		case done = <-inst.txDone:

			log.LogCallerFileLine("piTx: done with: %d", done)

			inst.txExit<-done
			break txBreak

		default:
		}

		//
		// send user package, lock needed
		//

		if inst.txEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}

		inst.p2pkgLock.Lock()

		if len(inst.p2pkgTx) > 0 {

			//
			// pick the first one
			//

			upkg := inst.p2pkgTx[0]
			inst.p2pkgTx = inst.p2pkgTx[1:]

			//
			// encode and send it
			//

			if eno := upkg.SendPackage(inst); eno != PeMgrEnoNone {

				//
				// 1) if failed, callback to the user, so he can close
				// this peer seems in troubles, we will be done then.
				//
				// 2) it is possible that, while we are blocked here in
				// writing and the connection is closed for some reasons
				// (for example the user close the peer), in this case,
				// we would get an error.
				//

				log.LogCallerFileLine("piTx: " +
					"call P2pIndHandler for SendPackage failed, eno: %d",
					eno)

				inst.txEno = eno

				inst.peMgr.Lock4Cb.Lock()

				if inst.peMgr.P2pIndHandler != nil {

					hs := Handshake {
						Snid:		inst.snid,
						NodeId:		inst.node.ID,
						ProtoNum:	inst.protoNum,
						Protocols:	inst.protocols,
					}

					info := P2pIndConnStatusPara{
						Ptn:		inst.ptnMe,
						PeerInfo:	&hs,
						Status:		int(eno),
						Flag:		false,
						Description:"piTx: SendPackage failed",
					}

					inst.peMgr.P2pIndHandler(P2pIndConnStatus, &info)

				} else {
					log.LogCallerFileLine("piTx: indication callback not installed yet")
				}

				inst.peMgr.Lock4Cb.Unlock()
			}
		}

		inst.p2pkgLock.Unlock()
	}

	return done
}

//
// Instance RX routine
//
func piRx(inst *peerInstance) PeMgrErrno {

	//
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When use try to close the peer, this routine
	// would then exit.
	//

	var done PeMgrErrno = PeMgrEnoNone
	var peerInfo = PeerInfo{}
	var pkgCb = P2pPackage4Callback{}

rxBreak:

	for {

		//
		// check if we are done
		//

		select {

		case done = <-inst.rxDone:

			log.LogCallerFileLine("piRx: done with: %d", done)

			inst.rxExit<-done
			break rxBreak

		default:
		}

		//
		// try reading the peer
		//

		if inst.rxEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}

		upkg := new(P2pPackage)

		if eno := upkg.RecvPackage(inst); eno != PeMgrEnoNone {

			//
			// 1) if failed, callback to the user, so he can close
			// this peer seems in troubles, we will be done then.
			//
			// 2) it is possible that, while we are blocked here in
			// reading and the connection is closed for some reasons
			// (for example the user close the peer), in this case,
			// we would get an error.
			//

			log.LogCallerFileLine("piRx: " +
				"call P2pIndHandler for RecvPackage failed, eno: %d",
				eno)

			inst.peMgr.Lock4Cb.Lock()

			inst.rxEno = eno

			if inst.peMgr.P2pIndHandler != nil {

				hs := Handshake {
					Snid:		inst.snid,
					NodeId:		inst.node.ID,
					ProtoNum:	inst.protoNum,
					Protocols:	inst.protocols,
				}

				info := P2pIndConnStatusPara{
					Ptn:		inst.ptnMe,
					PeerInfo:	&hs,
					Status:		int(eno),
					Flag:		false,
					Description:"piRx: RecvPackage failed",
				}

				inst.peMgr.P2pIndHandler(P2pIndConnStatus, &info)

			} else {
				log.LogCallerFileLine("piRx: indication callback not installed yet")
			}

			inst.peMgr.Lock4Cb.Unlock()

			continue
		}

		//
		// check the package received to filter out those not for p2p internal only
		//

		if upkg.Pid == uint32(PID_P2P) {

			if eno := inst.piP2pPkgProc(upkg); eno != PeMgrEnoNone {

				log.LogCallerFileLine("piRx: " +
					"piP2pMsgProc failed, eno: %d, inst: %s",
					eno,
					fmt.Sprintf("%+v", *inst))
			}

		} else if upkg.Pid == uint32(PID_EXT) {

			//
			// callback to the user for package incoming
			//

			inst.p2pkgLock.Lock()

			if inst.p2pkgRx != nil {

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

				inst.p2pkgRx(&pkgCb)

			} else {
				log.LogCallerFileLine("piRx: package callback not installed yet")
			}

			inst.p2pkgLock.Unlock()

		} else {

			//
			// unknown protocol identity
			//

			log.LogCallerFileLine("piRx: " +
				"package discarded for unknown pid: %d",
				upkg.Pid)
		}
	}

	return done
}

//
// Handler for p2p packages recevied
//
func (pi *peerInstance)piP2pPkgProc(upkg *P2pPackage) PeMgrErrno {

	//
	// check the package
	//

	if upkg == nil {
		log.LogCallerFileLine("piP2pPkgProc: invalid parameters")
		return PeMgrEnoParameter
	}

	if upkg.Pid != uint32(PID_P2P) {

		log.LogCallerFileLine("piP2pPkgProc: " +
			"not a p2p package, pid: %d",
			upkg.Pid)

		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {

		log.LogCallerFileLine("piP2pPkgProc: " +
			"invalid payload length: %d",
			upkg.PayloadLength)

		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {

		log.LogCallerFileLine("piP2pPkgProc: " +
			"payload length mismatched, PlLen: %d, real: %d",
			upkg.PayloadLength,
			len(upkg.Payload))

		return PeMgrEnoMessage
	}

	//
	// extract message from package payload
	//

	var msg = P2pMessage{}

	if eno := upkg.GetMessage(&msg); eno != PeMgrEnoNone {

		log.LogCallerFileLine("piP2pPkgProc: " +
			"GetMessage failed, eno: %d",
			eno	)

		return eno
	}

	//
	// check message identity. we discard any handshake messages received here
	// since handshake procedure had been passed, and dynamic handshake is not
	// supported currently.
	//

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

	return PeMgrEnoNone
}

//
// handler for ping message from peer
//
func (pi *peerInstance)piP2pPingProc(ping *Pingpong) PeMgrErrno {

	upkg := new(P2pPackage)

	pong := Pingpong {
		Seq:	ping.Seq,
		Extra:	nil,
	}

	//
	// clear pingpong counter of this instance
	//

	pi.ppCnt = 0

	//
	// pong the peer
	//

	if eno := upkg.pong(pi, &pong); eno != PeMgrEnoNone {

		log.LogCallerFileLine("piP2pPingProc: " +
			"pong failed, eno: %d, pi: %s",
			eno,
			fmt.Sprintf("%+v", *pi))

		return eno
	}

	return PeMgrEnoNone
}

//
// handler for pong message from peer
//
func (pi *peerInstance)piP2pPongProc(pong *Pingpong) PeMgrErrno {

	//
	// Currently, the heartbeat checking does not apply pong message from
	// peer, instead, a counter for ping messages and a timer are invoked,
	// see it please. We just simply debug out the pong message here.
	//
	// A more better method is to check the sequences of the pong message
	// against those of ping messages had been set, and then send evnet
	// EvPePingpongRsp to peer manager. The event EvPePingpongRsp is not
	// applied currently. We leave this work later.
	//

	return PeMgrEnoNone
}

//
// Compare peer instance to a specific state
//
func (pis peerInstState) compare(s peerInstState) int {

	//
	// See definition about peerInstState pls.
	//

	return int(pis - s)
}

//
// Update static nodes' status
//
func (peMgr *PeerManager)updateStaticStatus(snid SubNetworkID, id config.NodeID, status int) {
	if snid == peMgr.cfg.staticSubNetId {
		if _, static := peMgr.staticsStatus[id]; static == true {
			peMgr.staticsStatus[id] = status
		}
	}
}

//
// Check if we have sub network identity as "snid"
//
func (peMgr *PeerManager)dynamicSubNetIdExist(snid *SubNetworkID) bool {

	if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {

		for _, id := range peMgr.cfg.subNetIdList {

			if id == *snid {
				return true
			}
		}
	}

	return false
}

//
// Check if specific subnet is current static sub network identity
//
func (peMgr *PeerManager)staticSubNetIdExist(snid *SubNetworkID) bool {

	if peMgr.cfg.networkType == config.P2pNewworkTypeStatic {

		return peMgr.cfg.staticSubNetId == *snid

	} else if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {

		return len(peMgr.cfg.staticNodes) > 0 && peMgr.cfg.staticSubNetId == *snid
	}

	return false
}

//
// Print peer statistics
//
func (peMgr *PeerManager)logPeerStat() {

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

	if peMgr.cfg.networkType == config.P2pNewworkTypeDynamic {
		subNetIdList = append(subNetIdList, peMgr.cfg.subNetIdList...)
		if len(peMgr.cfg.staticNodes) > 0 {
			subNetIdList = append(subNetIdList, peMgr.cfg.staticSubNetId)
		}
	} else if peMgr.cfg.networkType == config.P2pNewworkTypeStatic {
		if len(peMgr.cfg.staticNodes) > 0 {
			subNetIdList = append(subNetIdList, peMgr.cfg.staticSubNetId)
		}
	}

	for _, snid := range subNetIdList {
		strSubnet := fmt.Sprintf("logPeerStat: p2pInst: %s, subnet: %x, obpNumSum: %d, ibpNumSum: %d, wrkNumSum: %d\n",
			peMgr.cfg.cfgName,
			snid,
			peMgr.obpNum[snid],
			peMgr.ibpNum[snid],
			peMgr.wrkNum[snid])
		dbgMsg += strSubnet
	}

	//log.LogCallerFileLine("%s", dbgMsg)
	golog.Printf("%s", dbgMsg)
}