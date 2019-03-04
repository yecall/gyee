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
	"sync"
	"bytes"
	"reflect"
	"crypto/ecdsa"
	ggio 	"github.com/gogo/protobuf/io"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	nat		"github.com/yeeco/gyee/p2p/nat"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)

//
// debug
//
type peerLogger struct {
	debug__		bool
}

var peerLog = peerLogger {
	debug__:	true,
}

func (log peerLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

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

func (ide PeerIdExx)toString() string {
	return fmt.Sprintf("%x:%x:%s:%d:%d",
		ide.Snid,
		ide.Node.ID,
		ide.Node.IP.String(),
		ide.Node.TCP,
		ide.Dir)
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

	durDcvFindNodeTimer = time.Second * 2			// duration to wait for find node response from discover task,
													// should be (findNodeExpiration + delta).

	durStaticRetryTimer = time.Second * 2			// duration to check and retry connect to static peers

	maxIndicationQueueSize = 512					// max indication queue size

	minDuration4FindNodeReq = time.Second * 2			// min duration to send find-node-request again
	minDuration4OutboundConnectReq = time.Second * 1	// min duration to try oubound connect for a specific
														// sub-network and peer

	conflictAccessDelayLower = 500						// conflict delay lower bounder in time.Millisecond
	conflictAccessDelayUpper = 2000						// conflict delay upper bounder in time.Millisecond

	reconfigDelay = time.Second * 4						// reconfiguration delay time duration
)

// peer status
const (
	peerIdle			= iota			// idle
	peerConnectOutInited				// connecting out inited
	peerActivated						// had been activated
	peerKilling							// in killing
)

// peer manager configuration
type peMgrConfig struct {
	cfgName				string								// p2p configuration name
	ip					net.IP								// ip address
	port				uint16								// tcp port number
	udp					uint16								// udp port number, used with handshake procedure
	noDial				bool								// do not dial outbound
	noAccept			bool								// do not accept inbound
	bootstrapNode		bool								// local is a bootstrap node
	defaultCto			time.Duration						// default connect outbound timeout
	defaultHto			time.Duration						// default handshake timeout
	defaultAto			time.Duration						// default active read/write timeout
	maxMsgSize			int									// max tcpmsg package size
	protoNum			uint32								// local protocol number
	protocols			[]Protocol							// local protocol table
	networkType			int									// p2p network type
	staticMaxPeers		int									// max peers would be
	staticMaxOutbounds	int									// max concurrency outbounds
	staticMaxInBounds	int									// max concurrency inbounds
	staticNodes			[]*config.Node						// static nodes
	staticSubNetId		SubNetworkID						// static network identity
	subNetMaxPeers		map[SubNetworkID]int				// max peers would be
	subNetMaxOutbounds	map[SubNetworkID]int				// max concurrency outbounds
	subNetMaxInBounds	map[SubNetworkID]int				// max concurrency inbounds
	subNetKeyList		map[SubNetworkID]ecdsa.PrivateKey	// keys for sub-node
	subNetNodeList		map[SubNetworkID]config.Node		// sub-node identities
	subNetIdList		[]SubNetworkID						// sub network identity list. do not put the identity
	ibpNumTotal			int									// total number of concurrency inbound peers
}

// start/stop/addr-switching... related
const (
	peMgrInNull		= 0
	peMgrInStartup	= 1
	peMgrInStoping	= 2
	peMgrInStopped	= 3
	pwMgrPubAddrOutofSwitching	= 0
	peMgrPubAddrInSwitching		= 1
	peMgrPubAddrDelaySwitching	= 2
)

const (
	PEMGR_STOP4NAT	= "pubAddrSwitch"
)

type pasBackupItem struct {
	snid	config.SubNetworkID		// sub network identity
	node	config.Node				// peer "node" information
}

// peer manager
type PeerManager struct {
	sdl					*sch.Scheduler						// pointer to scheduler
	name				string								// name
	inited				chan PeMgrErrno						// result of initialization
	isInited			bool								// is manager initialized ok
	tep					sch.SchUserTaskEp					// entry
	cfg					peMgrConfig							// configuration
	tidFindNode			map[SubNetworkID]int				// find node timer identity
	ptnMe				interface{}							// pointer to myself(peer manager task node)
	ptnTab				interface{}							// pointer to table task node
	ptnLsn				interface{}							// pointer to peer listener manager task node
	ptnAcp				interface{}							// pointer to peer acceptor manager task node
	ptnDcv				interface{}							// pointer to discover task node
	ptnShell			interface{}							// pointer to shell task node
	tabMgr				*tab.TableManager							// pointer to table manager
	ibInstSeq			int											// inbound instance sequence number
	obInstSeq			int											// outbound instance sequence number
	lock				sync.Mutex									// for peer instance to access peer manager
	peers				map[interface{}]*PeerInstance				// map peer instance's task node pointer to instance pointer
	nodes				map[SubNetworkID]map[PeerIdEx]*PeerInstance	// map peer node identity to instance pointer
	workers				map[SubNetworkID]map[PeerIdEx]*PeerInstance	// map peer node identity to pointer of instance in work
	wrkNum				map[SubNetworkID]int						// worker peer number
	ibpNum				map[SubNetworkID]int						// active inbound peer number
	obpNum				map[SubNetworkID]int						// active outbound peer number
	ibpTotalNum			int											// total active inbound peer number
	randoms				map[SubNetworkID][]*config.Node				// random nodes found by discover
	indChan				chan interface{}							// indication signal
	indCb				P2pIndCallback								// indication callback
	indCbUserData		interface{}									// user data pointer for callback
	staticsStatus		map[PeerIdEx]int							// status about static nodes
	caTids				map[string]int								// conflict access timer identity
	ocrTid				int											// OCR(outbound connect request) timestamp cleanup timer
	tmLastOCR			map[SubNetworkID]map[PeerId]time.Time		// time of last outbound connect request for sub-netowerk
	tmLastFNR			map[SubNetworkID]time.Time					// time of last find node request sent for sub network
	reCfg				PeerReconfig								// sub network reconfiguration
	reCfgTid			int											// reconfiguration timer
	inStartup			int											// if had been requestd to startup
	natResult			bool										// nat status
	pubTcpIp			net.IP										// public tcp ip
	pubTcpPort			int											// public tcp port
	pasStatus			int											// public addr switching status
	pasBackup			[]pasBackupItem								// backup list for nat public address switching
}

func NewPeerMgr() *PeerManager {
	var peMgr = PeerManager {
		name:        	sch.PeerMgrName,
		inited:      	make(chan PeMgrErrno, 1),
		cfg:         	peMgrConfig{},
		tidFindNode: 	map[SubNetworkID]int{},
		peers:			map[interface{}]*PeerInstance{},
		nodes:			map[SubNetworkID]map[PeerIdEx]*PeerInstance{},
		workers:		map[SubNetworkID]map[PeerIdEx]*PeerInstance{},
		wrkNum:      	map[SubNetworkID]int{},
		ibpNum:      	map[SubNetworkID]int{},
		obpNum:      	map[SubNetworkID]int{},
		ibpTotalNum:	0,
		indChan:     	make(chan interface{}, maxIndicationQueueSize),
		randoms:     	map[SubNetworkID][]*config.Node{},
		staticsStatus:	map[PeerIdEx]int{},
		caTids:			make(map[string]int, 0),
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

	peerLog.Debug("peerMgrProc: name: %s, msg.Id: %d", peMgr.name, msg.Id)

	if peMgr.msgFilter(msg) != PeMgrEnoNone {
		peerLog.Debug("peerMgrProc: filtered out, id: %d", msg.Id)
		return sch.SchEnoNone
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

	case sch.EvPeCloseReq:
		eno = peMgr.peMgrCloseReq(msg)

	case sch.EvPeCloseCfm:
		eno = peMgr.peMgrConnCloseCfm(msg.Body)

	case sch.EvPeCloseInd:
		eno = peMgr.peMgrConnCloseInd(msg.Body)

	case sch.EvPeTxDataReq:
		eno = peMgr.peMgrDataReq(msg.Body)

	case sch.EvNatMgrReadyInd:
		eno = peMgr.natMgrReadyInd(msg.Body.(*sch.MsgNatMgrReadyInd))

	case sch.EvNatMgrMakeMapRsp:
		eno = peMgr.natMakeMapRsp(msg.Body.(*sch.MsgNatMgrMakeMapRsp))

	case sch.EvNatMgrPubAddrUpdateInd:
		eno = peMgr.natPubAddrUpdateInd(msg.Body.(*sch.MsgNatMgrPubAddrUpdateInd))

	default:
		peerLog.Debug("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	peerLog.Debug("peerMgrProc: get out, name: %s, msg.Id: %d", peMgr.name, msg.Id)

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

	var ok sch.SchErrno
	ok, peMgr.ptnShell = peMgr.sdl.SchGetUserTaskNode(sch.ShMgrName)
	if ok != sch.SchEnoNone || peMgr.ptnShell == nil {
		peerLog.Debug("peMgrPoweron: shell not found")
		return PeMgrEnoScheduler
	}

	peMgr.cfg = peMgrConfig {
		cfgName:			cfg.CfgName,
		ip:					cfg.IP,
		port:				cfg.Port,
		udp:				cfg.UDP,
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
		subNetKeyList:		cfg.SubNetKeyList,
		subNetNodeList:		cfg.SubNetNodeList,
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
		peMgr.nodes[staticSnid] = make(map[PeerIdEx]*PeerInstance)
		peMgr.workers[staticSnid] = make(map[PeerIdEx]*PeerInstance)
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
			peMgr.nodes[snid] = make(map[PeerIdEx]*PeerInstance)
			peMgr.workers[snid] = make(map[PeerIdEx]*PeerInstance)
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
		peMgr.nodes[staticSnid] = make(map[PeerIdEx]*PeerInstance)
		peMgr.workers[staticSnid] = make(map[PeerIdEx]*PeerInstance)
		peMgr.wrkNum[staticSnid] = 0
		peMgr.ibpNum[staticSnid] = 0
		peMgr.obpNum[staticSnid] = 0
	} else {
		peerLog.Debug("peMgrPoweron: invalid network type: %d", peMgr.cfg.networkType)
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
	peerLog.Debug("PeMgrStart: EvPeMgrStartReq will be sent, target: %s", sch.PeerMgrName)
	msg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
	peMgr.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrPoweroff(ptn interface{}) PeMgrErrno {
	peerLog.Debug("peMgrPoweroff: task will be done, name: %s", sch.PeerMgrName)

	powerOff := sch.SchMessage {
		Id:		sch.EvSchPoweroff,
		Body:	nil,
	}

	peMgr.sdl.SchSetSender(&powerOff, &sch.RawSchTask)
	for _, peerInst := range peMgr.peers {
		peerLog.Debug("peMgrPoweroff: inst: %s", peMgr.sdl.SchGetTaskName(peerInst.ptnMe))
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
	peMgr.inStartup = peMgrInStartup
	if !peMgr.natResult {
		peerLog.Debug("peMgrStartReq: still not mapped by nat")
		return PeMgrEnoNone
	}
	peerLog.Debug("peMgrStartReq: task: %s", peMgr.name)
	return peMgr.start()
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
	if (peMgr.inStartup != peMgrInStartup || !peMgr.natResult) {
		peerLog.Debug("peMgrDcvFindNodeRsp: not ready, inStartup: %t, natResult: %t",
			peMgr.inStartup, peMgr.natResult)
		return PeMgrEnoNone
	}

	var rsp, _ = msg.(*sch.MsgDcvFindNodeRsp)
	if rsp == nil {
		peerLog.Debug("peMgrDcvFindNodeRsp: invalid message")
		return PeMgrEnoParameter
	}

	if peerLog.debug__ {
		dbgStr := fmt.Sprintf("peMgrDcvFindNodeRsp: snid: %x, nodes: ", rsp.Snid)
		for idx := 0; idx < len(rsp.Nodes); idx++ {
			dbgStr = dbgStr + rsp.Nodes[idx].IP.String() + ","
		}
		peerLog.Debug(dbgStr)
	}

	if peMgr.dynamicSubNetIdExist(&rsp.Snid) != true {
		peerLog.Debug("peMgrDcvFindNodeRsp: subnet not exist")
		return PeMgrEnoNotfound
	}

	if _, ok := peMgr.reCfg.delList[rsp.Snid]; ok {
		peerLog.Debug("peMgrDcvFindNodeRsp: discarded for reconfiguration, snid: %x", rsp.Snid)
		return PeMgrEnoRecofig
	}

	var (
		snid = rsp.Snid
		appended = make(map[SubNetworkID]int, 0)
		dup bool
		idEx PeerIdEx
	)

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
			peerLog.Debug("peMgrDcvFindNodeRsp: too much, some are truncated")
			continue
		}

		peMgr.randoms[snid] = append(peMgr.randoms[snid], n)
		appended[snid]++
	}

	// drive ourselves to startup outbound for nodes appended
	for snid := range appended {
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &snid)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrDcvFindNodeTimerHandler(msg interface{}) PeMgrErrno {
	nwt := peMgr.cfg.networkType
	snid := msg.(*SubNetworkID)

	peerLog.Debug("peMgrDcvFindNodeTimerHandler: nwt: %d, snid: %x", nwt, *snid)

	if _, ok := peMgr.tidFindNode[*snid]; ok {
		peMgr.tidFindNode[*snid] = sch.SchInvalidTid
	} else {
		peerLog.Debug("peMgrDcvFindNodeTimerHandler: no timer for snid: %x", *snid)
	}

	if nwt == config.P2pNetworkTypeStatic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.staticMaxOutbounds {
			peerLog.Debug("peMgrDcvFindNodeTimerHandler: reach threshold: %d", peMgr.obpNum[*snid])
			return PeMgrEnoNone
		}
	} else if nwt == config.P2pNetworkTypeDynamic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
			peerLog.Debug("peMgrDcvFindNodeTimerHandler: reach threshold: %d", peMgr.obpNum[*snid])
			return PeMgrEnoNone
		}
	} else {
		peerLog.Debug("peMgrDcvFindNodeTimerHandler: invalid network type: %d", nwt)
		return PeMgrEnoParameter
	}

	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, snid)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrLsnConnAcceptedInd(msg interface{}) PeMgrErrno {
	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var ibInd, _ = msg.(*msgConnAcceptedInd)
	var peInst = new(PeerInstance)

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

	peInst.txChan		= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.ppChan		= make(chan *P2pPackage, PeInstMaxPings)
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
		peerLog.Debug("peMgrLsnConnAcceptedInd: SchCreateTask failed, eno: %d", eno)
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
		peerLog.Debug("PeerManager: no outbound for noDial or boostrapNode: %t, %t")
		return PeMgrEnoNone
	}

	// if sub network identity is not specified, try to start all
	var snid *SubNetworkID

	if msg != nil { snid = msg.(*SubNetworkID) }

	if snid == nil {

		if eno := peMgr.peMgrStaticSubNetOutbound(); eno != PeMgrEnoNone {
			peerLog.Debug("peMgrOutboundReq: peMgrStaticSubNetOutbound failed, eno: %d", eno)
			return eno
		}

		if peMgr.cfg.networkType != config.P2pNetworkTypeStatic {

			for _, id := range peMgr.cfg.subNetIdList {

				if eno := peMgr.peMgrDynamicSubNetOutbound(&id); eno != PeMgrEnoNone {
					peerLog.Debug("peMgrOutboundReq: peMgrDynamicSubNetOutbound failed, eno: %d", eno)
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
		if idx != len(candidates) - 1 {
			candidates = append(candidates[:idx], candidates[idx+1:]...)
		} else {
			candidates = candidates[0:idx]
		}

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

		if tmPeers, exist := peMgr.tmLastOCR[*snid]; exist {

			if t, ok := tmPeers[n.ID]; ok {
				if time.Now().Sub(t) <= minDuration4OutboundConnectReq {
					peerLog.Debug("peMgrDynamicSubNetOutbound: too early, snid: %x, peer-ip: %x", *snid, n.IP.String())
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
		if _, lived := peMgr.peers[rsp.ptn]; lived {
			if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, PeInstDirOutbound, PKI_FOR_BOUNDOUT_FAILED);
				eno != PeMgrEnoNone {
				peerLog.Debug("peMgrConnOutRsp: peMgrKillInst failed, eno: %d", eno)
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
	var inst *PeerInstance
	var lived bool

	if inst, lived = peMgr.peers[rsp.ptn]; inst == nil || !lived {
		peerLog.Debug("peMgrHandshakeRsp: instance not found, rsp: %s", fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoNotfound
	}

	if inst.snid != rsp.snid || inst.dir != rsp.dir {
		peerLog.Debug("peMgrHandshakeRsp: response mismatched with instance, rsp: %s",
			fmt.Sprintf("%+v", *rsp))
		return PeMgrEnoParameter
	}

	if rsp.dir == PeInstDirInbound {
		peMgr.ibpNum[rsp.snid] += 1
	}

	if _, ok := peMgr.reCfg.delList[rsp.snid]; ok {
		peerLog.Debug("peMgrHandshakeRsp: kill instance for reconfiguration, snid: %x", rsp.snid)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_RECONFIG)
		return PeMgrEnoRecofig
	}

	// Check result, if failed, kill the instance
	idEx := PeerIdEx{Id:rsp.peNode.ID, Dir:rsp.dir}

	if rsp.result != PeMgrEnoNone {

		peerLog.Debug("peMgrHandshakeRsp: failed, result: %d", rsp.result)

		peMgr.updateStaticStatus(rsp.snid, idEx, peerKilling)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_HANDSHAKE_FAILED)

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

		peerLog.Debug("peMgrHandshakeRsp: too much workers, snid: %x, wrkNum: %d",
			snid, peMgr.wrkNum[snid])

		peMgr.updateStaticStatus(snid, idEx, peerKilling)
		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, rsp.dir, PKI_FOR_HANDSHAKE_FAILED)
		return PeMgrEnoResource
	}

	idEx.Dir = PeInstDirInbound
	if _, dup := peMgr.workers[snid][idEx]; dup {

		peerLog.Debug("peMgrHandshakeRsp: duplicate inbound worker, snid: %x, peer-ip: %s",
			snid, rsp.peNode.IP.String())

		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_DUPLICATED)
		return PeMgrEnoDuplicated
	}

	idEx.Dir = PeInstDirOutbound
	if _, dup := peMgr.workers[snid][idEx]; dup {

		peerLog.Debug("peMgrHandshakeRsp: duplicate outbound worker, snid: %x, peer-ip: %s",
			snid, rsp.peNode.IP.String())

		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_WORKERS)
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &inst.snid)
		peMgr.sdl.SchSendMessage(&schMsg)

		return PeMgrEnoDuplicated
	}

	if inst.dir == PeInstDirInbound {

		if peMgr.isStaticSubNetId(snid) {

			peMgr.workers[snid][idEx] = inst

		} else {

			if peMgr.ibpNum[snid] >= maxInbound {

				peerLog.Debug("peMgrHandshakeRsp: too much inbound workers, snid: %x, ibpNum: %d",
					snid, peMgr.ibpNum[snid])

				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_INBOUNDS)
				return PeMgrEnoResource
			}

			idEx.Dir = PeInstDirOutbound
			if _, dup := peMgr.nodes[snid][idEx]; dup {

				// this case, both instances are not in working, we kill one instance local,
				// but the peer might kill another, then two connections are lost, we need a
				// protection for this.

				peerLog.Debug("peMgrHandshakeRsp: inbound conflicts to outbound, snid: %x, peer-ip: %s",
					snid, rsp.peNode.IP.String())

				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_DUPLICATED)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)

				return PeMgrEnoDuplicated
			}
		}

		idEx.Dir = PeInstDirInbound
		peMgr.nodes[snid][idEx] = inst
		peMgr.workers[snid][idEx] = inst

	} else if inst.dir == PeInstDirOutbound {

		if peMgr.isStaticSubNetId(snid) {

			peMgr.workers[snid][idEx] = inst

		} else {

			if peMgr.obpNum[snid] >= maxOutbound {

				peerLog.Debug("peMgrHandshakeRsp: too much outbound workers, snid: %x, obpNum: %d",
					snid, peMgr.obpNum[snid])

				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_OUTBOUNDS)
				return PeMgrEnoResource
			}

			idEx.Dir = PeInstDirInbound
			if _, dup := peMgr.nodes[snid][idEx]; dup {

				// this case, both instances are not in working, we kill one instance local,
				// but the peer might kill another, then two connections are lost, we need a
				// protection for this.

				peerLog.Debug("peMgrHandshakeRsp: outbound conflicts to inbound, snid: %x, peer-ip: %s",
					snid, rsp.peNode.IP.String())

				peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_TOOMUCH_DUPLICATED)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)
				schMsg := sch.SchMessage{}
				peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &inst.snid)
				peMgr.sdl.SchSendMessage(&schMsg)

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

		peerLog.Debug("peMgrHandshakeRsp: instance deal with EvPeEstablishedInd failed, snid: %x, eno: %d, peer-ip: %s",
			snid, eno, rsp.peNode.IP.String())

		peMgr.peMgrKillInst(rsp.ptn, rsp.peNode, inst.dir, PKI_FOR_WITHOUT_INST_CFM)
		return PeMgrErrno(eno)
	}

	inst.state = peInstStateActivated
	peMgr.wrkNum[snid]++

	if inst.dir == PeInstDirInbound  &&
		inst.networkType != config.P2pNetworkTypeStatic {

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
			peerLog.Debug("peMgrHandshakeRsp: TabBucketAddNode failed, eno: %d, snid: %x, node: %s",
				tabEno, snid, fmt.Sprintf("%+v", *rsp.peNode))
		}

		tabEno = peMgr.tabMgr.TabUpdateNode(snid, &n)
		if tabEno != tab.TabMgrEnoNone {
			peerLog.Debug("peMgrHandshakeRsp: TabUpdateNode failed, eno: %d, snid: %x, node: %s",
				tabEno, snid, fmt.Sprintf("%+v", *rsp.peNode))
		}
	}

	i := P2pIndPeerActivatedPara {
		P2pInst: peMgr.sdl,
		RxChan: inst.rxChan,
		PeerInfo: & Handshake {
			Snid:		inst.snid,
			Dir:		inst.dir,
			NodeId:		inst.node.ID,
			IP:			net.IP{},
			UDP:		uint32(inst.node.UDP),
			TCP:		uint32(inst.node.TCP),
			ProtoNum:	inst.protoNum,
			Protocols:	inst.protocols,
		},
	}
	i.PeerInfo.IP = append(i.PeerInfo.IP, inst.node.IP...)

	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerActiveInd{
			TxChan: inst.txChan,
			RxChan: inst.rxChan,
			PeerInfo: i.PeerInfo,
			PeerInst: inst,
		}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerActiveInd, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
		return PeMgrEnoNone
	}

	return peMgr.peMgrIndEnque(&i)
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

	why, _ := req.Why.(string)
	peerLog.Debug("peMgrCloseReq: why: %s, snid: %x, dir: %d, ip: %s, port: %d",
		why, req.Snid, req.Dir, req.Node.IP.String(), req.Node.TCP)

	inst := peMgr.getWorkerInst(snid, &idEx)
	if inst == nil {
		peerLog.Debug("peMgrCloseReq: worker not found")
		return PeMgrEnoNotfound
	}

	if inst.state == peInstStateKilling {
		peerLog.Debug("peMgrCloseReq: worker already in killing")
		return PeMgrEnoDuplicated
	}

	if ptnSender := peMgr.sdl.SchGetSender(msg); ptnSender != peMgr.ptnShell {
		// req.Ptn is nil, means the sender is peMgr.ptnShell, so need not to
		// send EvShellPeerAskToCloseInd to it again.
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
	// cleanup
	var cfm = msg.(*MsgCloseCfm)
	if eno := peMgr.peMgrKillInst(cfm.ptn, cfm.peNode, cfm.dir, PKI_FOR_CLOSE_CFM); eno != PeMgrEnoNone {
		peerLog.Debug("peMgrConnCloseCfm: peMgrKillInst, failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	i := P2pIndPeerClosedPara {
		P2pInst:	peMgr.sdl,
		Snid:		cfm.snid,
		PeerId:		cfm.peNode.ID,
		Dir:		cfm.dir,
	}
	if peMgr.ptnShell != nil {
		schMsg := new(sch.SchMessage)
		ind2Sh := sch.MsgShellPeerCloseCfm{
			Result: int(cfm.result),
			Dir: cfm.dir,
			Snid: cfm.snid,
			PeerId: cfm.peNode.ID,
		}
		peMgr.sdl.SchMakeMessage(schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerCloseCfm, &ind2Sh)
		peMgr.sdl.SchSendMessage(schMsg)
	} else {
		peMgr.peMgrIndEnque(&i)
	}

	peerLog.Debug("peMgrConnCloseCfm: inStartup: %d, pasStatus: %d, peers: %d, nodes: %d, workers: %d",
		peMgr.inStartup, peMgr.pasStatus, len(peMgr.peers), len(peMgr.nodes), len(peMgr.workers))

	if peMgr.inStartup == peMgrInStartup {

		peerLog.Debug("peMgrConnCloseCfm: send EvPeOutboundReq")
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &cfm.snid)
		peMgr.sdl.SchSendMessage(&schMsg)

	} else if peMgr.inStartup == peMgrInStoping {

		if len(peMgr.peers) == 0 && len(peMgr.nodes) == 0 && len(peMgr.workers) == 0 {
			peerLog.Debug("peMgrConnCloseCfm: transfer to peMgrInStopped")
			peMgr.inStartup = peMgrInStopped
		}

	} else if peMgr.pasStatus == peMgrPubAddrInSwitching  ||
		peMgr.pasStatus == peMgrPubAddrDelaySwitching {

		if len(peMgr.peers) == 0 && len(peMgr.nodes) == 0 && len(peMgr.workers) == 0 {
			peerLog.Debug("peMgrConnCloseCfm: calll pubAddrSwitch")
			peMgr.pubAddrSwitch()
		}
	} else {
		peerLog.Debug("peMgrConnCloseCfm")
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrConnCloseInd(msg interface{}) PeMgrErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement.
	panic("peMgrConnCloseInd: should never come here!!!")

	schMsg := sch.SchMessage{}
	ind := msg.(*MsgCloseInd)

	if eno := peMgr.peMgrKillInst(ind.ptn, ind.peNode, ind.dir, PKI_FOR_CLOSE_IND); eno != PeMgrEnoNone {
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

func (peMgr *PeerManager)natMgrReadyInd(msg *sch.MsgNatMgrReadyInd) PeMgrErrno {
	if peMgr.natResult = msg.NatType == nat.NATT_NONE; peMgr.natResult {
		peMgr.pubTcpIp = peMgr.cfg.ip
		peMgr.pubTcpPort = int(peMgr.cfg.port)
		if peMgr.inStartup == peMgrInStartup {
			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)natMakeMapRsp(msg *sch.MsgNatMgrMakeMapRsp) PeMgrErrno {
	// switch to the public address, if the nat is configured as "none", the old
	// address is kept. MORE might be needed.
	if nat.NatIsResultOk(msg.Result) && nat.NatIsStatusOk(msg.Status) && msg.Proto == nat.NATP_TCP {
		peMgr.natResult = true
		peMgr.pubTcpIp = msg.PubIp
		peMgr.pubTcpPort = msg.PubPort
		for k, n := range peMgr.cfg.subNetNodeList {
			n.IP = msg.PubIp
			n.TCP = uint16(msg.PubPort & 0xffff)
			peMgr.cfg.subNetNodeList[k] = n
		}
		if peMgr.inStartup == peMgrInStartup {
			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)natPubAddrUpdateInd(msg *sch.MsgNatMgrPubAddrUpdateInd) PeMgrErrno {

	peerLog.Debug("natPubAddrUpdateInd: entered")

	oldNatResult := peMgr.natResult
	oldIp := peMgr.pubTcpIp
	oldTcp := peMgr.pubTcpPort

	natMapRecovered := func() {
		if !peMgr.pubTcpIp.Equal(oldIp) || peMgr.pubTcpPort != oldTcp {
			if peMgr.pasStatus == pwMgrPubAddrOutofSwitching {
				if peMgr.reCfgTid != sch.SchInvalidTid {
					peerLog.Debug("natMapRecovered: enter peMgrPubAddrDelaySwitching")
					peMgr.pasStatus = peMgrPubAddrDelaySwitching
				} else {
					peerLog.Debug("natMapRecovered: enter peMgrPubAddrInSwitching")
					peMgr.pasStatus = peMgrPubAddrInSwitching

					peerLog.Debug("natMapRecovered: call pubAddrSwitchPrepare")
					peMgr.pubAddrSwitchPrepare()

					peerLog.Debug("natMapRecovered: call stop")
					peMgr.stop(PEMGR_STOP4NAT)
				}
			}
		} else if peMgr.inStartup == peMgrInStartup {
			peerLog.Debug("natMapRecovered: call PeMgrStart")
			peMgr.PeMgrStart()
		}
	}

	natMapLost := func() {
		if oldNatResult {
			peerLog.Debug("natMapLost: call stop")
			peMgr.stop(PEMGR_STOP4NAT)
		}
	}

	if nat.NatIsStatusOk(msg.Status) {
		if msg.Proto == nat.NATP_TCP {
			if !peMgr.natResult {
				peerLog.Debug("natPubAddrUpdateInd: call natMapRecovered")
				natMapRecovered()
			}
		}
	} else if msg.Proto == nat.NATP_TCP {
		old := peMgr.natResult
		peMgr.natResult = false
		peMgr.pubTcpIp = net.IPv4zero
		peMgr.pubTcpPort = 0
		if old {
			peerLog.Debug("natPubAddrUpdateInd: call natMapLost")
			natMapLost()
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)start() PeMgrErrno {
	msg := new(sch.SchMessage)
	if peMgr.cfg.noAccept == false {
		peMgr.sdl.SchMakeMessage(msg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(msg)
		peerLog.Debug("start: EvPeLsnStartReq sent")
	}

	tdOcr := sch.TimerDescription {
		Name:	"_pocrTimer",
		Utid:	sch.PeMinOcrCleanupTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	minDuration4OutboundConnectReq,
		Extra:	nil,
	}
	eno := sch.SchEnoNone
	eno, peMgr.ocrTid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &tdOcr)
	if eno != sch.SchEnoNone || peMgr.ocrTid == sch.SchInvalidTid {
		peerLog.Debug("start: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	peerLog.Debug("start: ocrTid start ok")

	msg = new(sch.SchMessage)
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)
	peerLog.Debug("start: EvPeOutboundReq sent")

	return PeMgrEnoNone
}

func (peMgr *PeerManager)stop(why interface{}) PeMgrErrno {
	if peMgr.reCfgTid != sch.SchInvalidTid {
		peerLog.Debug("stop: faied, in reconfiguring")
		return PeMgrEnoRecofig
	}

	peerLog.Debug("stop: kill caTids")
	for _, tid := range peMgr.caTids {
		if tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
		}
	}
	peMgr.caTids = make(map[string]int, 0)

	if peMgr.cfg.noAccept == false {
		msg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStopReq, nil)
		peMgr.sdl.SchSendMessage(&msg)
		peerLog.Debug("stop: EvPeLsnStopReq sent")
	}

	peerLog.Debug("stop: randoms cleared")
	peMgr.randoms = make(map[SubNetworkID][]*config.Node, 0)

	peerLog.Debug("stop: kill ocrTid")
	if peMgr.ocrTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, peMgr.ocrTid)
		peMgr.ocrTid = sch.SchInvalidTid
	}

	peerLog.Debug("stop: kill tidFindNode")
	for _, tid := range peMgr.tidFindNode {
		if tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
		}
	}
	peMgr.tidFindNode = make(map[SubNetworkID]int, 0)

	// try to close all peer instances: need to check the state of the instance to
	// be closed.
	peerLog.Debug("stop: try to kill all peer instances")
	for ptn, pi := range peMgr.peers {

		peerLog.Debug("stop: peer, snid: %x, dir: %d, ip: %s", pi.snid, pi.dir, pi.node.IP.String())

		node := pi.node
		snid := pi.snid
		dir := pi.dir
		idex := PeerIdEx{
			Id:  pi.node.ID,
			Dir: dir,
		}

		// check to ask shell manager to close the peer
		if piOfSubnet, ok := peMgr.nodes[snid]; ok && piOfSubnet != nil {
			if _, ok := piOfSubnet[idex]; ok {
				if pi.state >= peInstStateHandshook {
					ind := sch.MsgShellPeerAskToCloseInd{
						Snid:   snid,
						PeerId: idex.Id,
						Dir:    idex.Dir,
						Why:    why,
					}
					msg := sch.SchMessage{}
					peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerAskToCloseInd, &ind)
					peMgr.sdl.SchSendMessage(&msg)
					peerLog.Debug("stop: EvShellPeerAskToCloseInd sent, snid: %x, ip: %s, dir: %d",
						snid, pi.node.IP.String(), pi.dir)
					continue
				}
			}
		}
		// else kill directly
		req := sch.MsgPeCloseReq {
			Ptn: ptn,
			Snid: snid,
			Node: node,
			Dir: dir,
			Why: why,
		}
		msg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
		peMgr.sdl.SchSendMessage(&msg)
		peerLog.Debug("stop: EvPeCloseReq sent, snid: %x, ip: %s, dir: %d",
			snid, pi.node.IP.String(), pi.dir)
	}

	peerLog.Debug("stop: transfer to peMgrInStoping")
	peMgr.inStartup = peMgrInStoping
	return PeMgrEnoNone
}

func (peMgr *PeerManager)msgFilter(msg *sch.SchMessage) PeMgrErrno {
	eno := PeMgrErrno(PeMgrEnoNone)
	if peMgr.inStartup == peMgrInNull {
		switch msg.Id {
		case sch.EvSchPoweron:
		case sch.EvSchPoweroff:
		case sch.EvNatMgrReadyInd:
		case sch.EvNatMgrMakeMapRsp:
		case sch.EvPeMgrStartReq:
		default:
			peerLog.Debug("msgFilter: filtered out for peMgrInNull, msg.Id: %d", msg.Id)
			eno = PeMgrEnoMismatched
		}
	} else if peMgr.inStartup != peMgrInStartup {
		switch msg.Id {
		case sch.EvSchPoweroff:
		case sch.EvPeCloseReq:
		case sch.EvPeCloseCfm:
		case sch.EvPeCloseInd:
		default:
			peerLog.Debug("msgFilter: filtered out for inStartup: %d, msg.Id: %d", peMgr.inStartup, msg.Id)
			eno = PeMgrEnoMismatched
		}
	}
	return eno
}

func (peMgr *PeerManager)pubAddrSwitchPrepare() PeMgrErrno {
	peMgr.pasBackup = make([]pasBackupItem, 0)
	for _, piOfSubnet := range peMgr.workers {
		if piOfSubnet != nil && len(piOfSubnet) > 0 {
			for _, pi := range(piOfSubnet) {
				item := pasBackupItem {
					snid: pi.snid,
					node: pi.node,
				}
				peerLog.Debug("pubAddrSwitchPrepare: peer backup, name, snid: %x, ip: %s",
					pi.name, pi.snid, pi.node.IP.String())
				peMgr.pasBackup = append(peMgr.pasBackup, item)
			}
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager)pubAddrSwitch() PeMgrErrno {
	peerLog.Debug("pubAddrSwitch: transfer to peMgrInStartup and pwMgrPubAddrOutofSwitching")
	peMgr.inStartup = peMgrInStartup
	peMgr.pasStatus = pwMgrPubAddrOutofSwitching

	peerLog.Debug("pubAddrSwitch: restore peers in bakup list")
	peMgr.randoms = make(map[SubNetworkID][]*config.Node, 0)
	for _, item := range peMgr.pasBackup {
		snid := item.snid
		if _, ok := peMgr.randoms[snid]; !ok {
			peMgr.randoms[snid] = make([]*config.Node, 0)
		}
		peMgr.randoms[snid] = append(peMgr.randoms[snid], &item.node)
		peerLog.Debug("pubAddrSwitch: backup one, snid: %x, ip: %s",
			item.snid, item.node.IP.String())
	}

	peerLog.Debug("pubAddrSwitch: switch to new ip: %s, port: %d",
		peMgr.pubTcpIp.String(), peMgr.pubTcpPort)

	peMgr.cfg.ip = peMgr.pubTcpIp
	peMgr.cfg.port = uint16(peMgr.pubTcpPort)
	peMgr.cfg.udp = uint16(peMgr.pubTcpPort)
	for k, old := range peMgr.cfg.subNetNodeList {
		n := config.Node {
			ID: old.ID,
			IP: peMgr.pubTcpIp,
			TCP: uint16(peMgr.pubTcpPort),
			UDP: uint16(peMgr.pubTcpPort),
		}
		peMgr.cfg.subNetNodeList[k] = n
	}

	peerLog.Debug("pubAddrSwitch: call start")
	return peMgr.start()
}

func (peMgr *PeerManager)peMgrDataReq(msg interface{}) PeMgrErrno {
	var inst *PeerInstance = nil
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
		peerLog.Debug("peMgrDataReq: discarded, tx queue full, inst: %s", inst.name)
		return PeMgrEnoResource
	}

	_pkg := req.Pkg.(*P2pPackage)
	inst.txChan<-_pkg
	inst.txPendNum += 1

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCreateOutboundInst(snid *config.SubNetworkID, node *config.Node) PeMgrErrno {

	peerLog.Debug("peMgrCreateOutboundInst: snid: %x, peer-ip: %s", *snid, node.IP.String())

	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var peInst = new(PeerInstance)

	*peInst					= peerInstDefault
	peInst.sdl				= peMgr.sdl
	peInst.peMgr			= peMgr
	peInst.tep				= peInst.peerInstProc
	peInst.ptnMgr			= peMgr.ptnMe
	peInst.state			= peInstStateConnOut
	peInst.cto				= peMgr.cfg.defaultCto
	peInst.hto				= peMgr.cfg.defaultHto
	peInst.ato				= peMgr.cfg.defaultAto
	peInst.maxPkgSize		= peMgr.cfg.maxMsgSize
	peInst.dialer			= &net.Dialer{Timeout: peMgr.cfg.defaultCto}
	peInst.conn				= nil
	peInst.laddr			= nil
	peInst.raddr			= nil
	peInst.dir				= PeInstDirOutbound
	peInst.networkType		= peMgr.cfg.networkType
	peInst.snid				= *snid
	peInst.priKey			= peMgr.cfg.subNetKeyList[*snid]
	peInst.localNode		= peMgr.cfg.subNetNodeList[*snid]
	peInst.localProtoNum	= peMgr.cfg.protoNum
	peInst.localProtocols	= peMgr.cfg.protocols

	peInst.node				= *node

	peInst.txChan			= make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.ppChan			= make(chan *P2pPackage, PeInstMaxPings)
	peInst.rxChan			= make(chan *P2pPackageRx, PeInstMaxP2packages)
	peInst.rxDone			= make(chan PeMgrErrno)
	peInst.rxtxRuning		= false

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
		peerLog.Debug("peMgrCreateOutboundInst: SchCreateTask failed, eno: %d", eno)
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

const (
	PKI_FOR_NONE				= "none"
	PKI_FOR_CLOSE_CFM			= "peMgrConnCloseCfm"
	PKI_FOR_CLOSE_IND			= "peMgrConnCloseInd"
	PKI_FOR_BOUNDOUT_FAILED		= "peMgrConnOutRsp"
	PKI_FOR_RECONFIG			= "reConfig"
	PKI_FOR_HANDSHAKE_FAILED	= "hsFailed"
	PKI_FOR_TOOMUCH_WORKERS		= "maxWorks"
	PKI_FOR_TOOMUCH_OUTBOUNDS	= "maxOutbounds"
	PKI_FOR_TOOMUCH_INBOUNDS	= "maxInbounds"
	PKI_FOR_TOOMUCH_DUPLICATED	= "maxInbounds"
	PKI_FOR_WITHOUT_INST_CFM	= "noInstCfm"
)
func (peMgr *PeerManager)peMgrKillInst(ptn interface{}, node *config.Node, dir int, why interface{}) PeMgrErrno {

	why = why.(string)
	peerLog.Debug("peMgrKillInst: task: %s, why: %s", peMgr.sdl.SchGetTaskName(ptn), why)

	var peInst = peMgr.peers[ptn]
	if peInst == nil {
		peerLog.Debug("peMgrKillInst: instance not found, node: %x", node.ID)
		return PeMgrEnoNotfound
	}

	if peInst.dir != dir {
		peerLog.Debug("peMgrKillInst: invalid parameters")
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
		if peMgr.obpNum[snid]--; peMgr.obpNum[snid] < 0 {
			panic(fmt.Sprintf("peMgrKillInst: internal errors, snid: %x", snid))
		}
	} else if peInst.dir == PeInstDirInbound {
		delete(peMgr.peers, ptn)
		if _, exist := peMgr.nodes[snid][idEx]; exist {
			delete(peMgr.nodes[snid], idEx)
		}
		if peMgr.ibpTotalNum--; peMgr.ibpTotalNum < 0 {
			panic(fmt.Sprintf("peMgrKillInst: internal errors, snid: %x", snid))
		}
		if peMgr.ibpNum[snid]--; peMgr.ibpNum[snid] < 0 {
			panic(fmt.Sprintf("peMgrKillInst: internal errors, snid: %x", snid))
		}
	}

	peMgr.updateStaticStatus(snid, idEx, peerIdle)
	if peMgr.cfg.noAccept == false &&
		peMgr.ibpTotalNum < peMgr.cfg.ibpNumTotal {
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}
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

	peerLog.Debug("peMgrConflictAccessProtect: set timer, utid: %d, dur: %d, idexx: %+v", td.Utid, td.Dur, idexx)

	eno, tid := peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		peerLog.Debug("peMgrConflictAccessProtect: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	idexx.toString()
	peMgr.caTids[idexx.toString()] = tid
	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrCatHandler(msg interface{}) PeMgrErrno {
	idexx := msg.(*PeerIdExx)
	delete(peMgr.caTids, (*idexx).toString())
	schMsg := sch.SchMessage{}
	r := sch.MsgDcvFindNodeRsp{
		Snid:	idexx.Snid,
		Nodes:	[]*config.Node{&idexx.Node},
	}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvDcvFindNodeRsp, &r)
	peMgr.sdl.SchSendMessage(&schMsg)
	peerLog.Debug("peMgrCatHandler: EvDcvFindNodeRsp sent, peer-ip: %s, idexx: %+v", idexx.Node.IP.String(), idexx)
	return PeMgrEnoNone
}

func (peMgr *PeerManager)reconfigTimerHandler() PeMgrErrno {
	peMgr.reCfgTid = sch.SchInvalidTid
	for del := range peMgr.reCfg.delList {
		wks, ok := peMgr.workers[del]
		if !ok {
			continue
		}
		why := sch.PEC_FOR_RECONFIG
		msg := sch.SchMessage{}
		for _, peerInst := range wks {
			req := sch.MsgPeCloseReq {
				Ptn:	peerInst.ptnMe,
				Snid:	peerInst.snid,
				Node:	peerInst.node,
				Dir:	peerInst.dir,
				Why:	why,
			}
			peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
			peMgr.sdl.SchSendMessage(&msg)
		}
	}
	peMgr.reCfg.delList = make(map[config.SubNetworkID]interface{}, 0)

	// carry out public address switching if necessary
	if peMgr.pasStatus == peMgrPubAddrDelaySwitching {
		peMgr.pasStatus = peMgrPubAddrInSwitching
		peMgr.pubAddrSwitchPrepare()
		return peMgr.stop(PEMGR_STOP4NAT)
	}

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
		peerLog.Debug("shellReconfigReq: previous reconfiguration not completed")
		return PeMgrEnoRecofig
	}

	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()

	delList := msg.SnidDel
	addList := msg.SnidAdd
	newDelList := make([]config.SubNetworkID, 0)
	newAddList := make([]sch.SingleSubnetDescriptor, 0)

	// filter out deleting list
	for _, del := range delList {
		if _, inAdd := peMgr.reCfg.addList[del]; inAdd {
			newDelList = append(newDelList, del)
		}
	}

	// filter out adding list
	for _, add := range addList {
		if _, inAdd := peMgr.reCfg.addList[add.SubNetId]; !inAdd {
			newAddList = append(newAddList, add)
		}
	}

	// config adding part sub networks
	for _, add := range newAddList {
		peMgr.nodes[add.SubNetId] = make(map[PeerIdEx]*PeerInstance)
		peMgr.workers[add.SubNetId] = make(map[PeerIdEx]*PeerInstance)
		peMgr.wrkNum[add.SubNetId] = 0
		peMgr.ibpNum[add.SubNetId] = 0
		peMgr.obpNum[add.SubNetId] = 0
		peMgr.cfg.subNetKeyList[add.SubNetId] = add.SubNetKey
		peMgr.cfg.subNetNodeList[add.SubNetId] = add.SubNetNode
		peMgr.cfg.subNetMaxPeers[add.SubNetId] = add.SubNetMaxPeers
		peMgr.cfg.subNetMaxOutbounds[add.SubNetId] = add.SubNetMaxOutbounds
		peMgr.cfg.subNetMaxInBounds[add.SubNetId] = add.SubNetMaxInBounds
		peMgr.reCfg.addList[add.SubNetId] = nil
	}

	// kill part of peer instances of each sub network to deleted
	for _, del := range newDelList {
		wks, ok := peMgr.workers[del]
		if !ok {
			continue
		}
		peMgr.reCfg.delList[del] = nil

		wkNum := len(wks)
		count := 0
		schMsg := sch.SchMessage{}
		for _, peerInst := range wks {
			if count++; count >= wkNum / 2 {
				break
			}
			why := sch.PEC_FOR_RECONFIG_REQ
			req := sch.MsgPeCloseReq {
				Ptn:	peerInst.ptnMe,
				Snid:	peerInst.snid,
				Node:	peerInst.node,
				Dir:	peerInst.dir,
				Why:	why,
			}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
			if eno := peMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
				peerLog.Debug("shellReconfigReq: SchSendMessage failed, eno: %d", eno)
				panic("shellReconfigReq: SchSendMessage failed")
			}
		}

		if tid, ok := peMgr.tidFindNode[del]; ok && tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
			delete(peMgr.tidFindNode, del)
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
		peerLog.Debug("shellReconfigReq: SchSetTimer failed, eno: %d", eno)
		panic("shellReconfigReq: SchSendMessage failed")
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
	if eno := peMgr.peMgrRecfg2DcvMgr(); eno != PeMgrEnoNone {
		peerLog.Debug("shellReconfigReq: peMgrRecfg2DcvMgr failed, eno: %d", eno)
		panic("shellReconfigReq: SchSendMessage failed")
	}

	// tell shell manager to update subnet info
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellSubnetUpdateReq, nil)
	if eno := peMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		peerLog.Debug("shellReconfigReq: SchSendMessage failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

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
	var tid = sch.SchInvalidTid

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
			peerLog.Debug("peMgrAsk4More: no more needed, obpNum: %d, max: %d",
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
		timerName = fmt.Sprintf("%s%x", sch.PeerMgrName + "_DcvFindNodeTimer_", *snid)

		peerLog.Debug("peMgrAsk4More: "+
			"cfgName: %s, subnet: %x, obpNum: %d, ibpNum: %d, ibpTotalNum: %d, wrkNum: %d, more: %d",
			peMgr.cfg.cfgName,
			*snid,
			peMgr.obpNum[*snid],
			peMgr.ibpNum[*snid],
			peMgr.ibpTotalNum,
			peMgr.wrkNum[*snid],
			more)

	} else {
		timerName = sch.PeerMgrName + "_static"
	}

	// set a ABS timer. notice: we must update tidFindNode for subnet when
	// find-node timer expired, see function peMgrDcvFindNodeTimerHandler
	// for more please. also notice that, this function might be called before
	// the timer is expired, so we need to check to kill it if it's the case.
	extra := *snid
	var td = sch.TimerDescription {
		Name:	timerName,
		Utid:	sch.PeDcvFindNodeTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	dur,
		Extra:	&extra,
	}

	if oldTid, ok := peMgr.tidFindNode[*snid]; ok && oldTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, oldTid)
		peMgr.tidFindNode[*snid] = sch.SchInvalidTid
	}

	if eno, tid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		peerLog.Debug("peMgrAsk4More: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	peMgr.tidFindNode[*snid] = tid

	if peerLog.debug__ {
		dbgStr := fmt.Sprintf("peMgrAsk4More: ptn: %p, updated snid_tid: [%x,%d], now: ",
			peMgr.ptnMe, *snid, tid)
		for k, v := range peMgr.tidFindNode {
			snid_tid := fmt.Sprintf("[%x,%d],", k, v)
			dbgStr = dbgStr + snid_tid
		}
		peerLog.Debug(dbgStr)
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager)peMgrIndEnque(ind interface{}) PeMgrErrno {
	if len(peMgr.indChan) >= cap(peMgr.indChan) {
		panic("peMgrIndEnque: system overload")
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

const PeInstDirNull			= -1				// null, so connection should be nil
const PeInstDirInbound		= 0					// inbound connection
const PeInstDirOutbound		= 1					// outbound connection

const PeInstMailboxSize 	= 512				// mailbox size
const PeInstMaxP2packages	= 512				// max p2p packages pending to be sent
const PeInstMaxPings		= 8					// max pings pending to be sent
const PeInstMaxPingpongCnt	= 4					// max pingpong counter value
const PeInstPingpongCycle	= time.Second * 16	// pingpong period

type PeerInstance struct {
	sdl				*sch.Scheduler				// pointer to scheduler
	peMgr			*PeerManager				// pointer to peer manager
	name			string						// name
	tep				sch.SchUserTaskEp			// entry
	ptnMe			interface{}					// the instance task node pointer
	ptnMgr			interface{}					// the peer manager task node pointer
	state			peerInstState				// state
	cto				time.Duration				// connect timeout value
	hto				time.Duration				// handshake timeout value
	ato				time.Duration				// active peer connection read/write timeout value
	dialer			*net.Dialer					// dialer to make outbound connection
	conn			net.Conn					// connection
	iow				ggio.WriteCloser			// IO writer
	ior				ggio.ReadCloser				// IO reader
	laddr			*net.TCPAddr				// local ip address
	raddr			*net.TCPAddr				// remote ip address
	dir				int							// direction: outbound(+1) or inbound(-1)
	snid			config.SubNetworkID			// sub network identity

	networkType		int							// network type
	priKey			ecdsa.PrivateKey			// local node private key
	localNode		config.Node					// local "node" information
	localProtoNum	uint32						// local protocol number
	localProtocols	[]Protocol					// local protocol table

	node			config.Node					// peer "node" information
	protoNum		uint32						// peer protocol number
	protocols		[]Protocol					// peer protocol table
	maxPkgSize		int							// max size of tcpmsg package
	ppTid			int							// pingpong timer identity
	rxChan			chan *P2pPackageRx			// rx pending channel
	txChan			chan *P2pPackage			// tx pending channel
	ppChan			chan *P2pPackage			// ping channel
	txPendNum		int							// tx pending number
	txSeq			int64						// statistics sequence number
	txOkCnt			int64						// tx ok counter
	txFailedCnt		int64						// tx failed counter
	rxDone			chan PeMgrErrno				// RX chan
	rxtxRuning		bool						// indicating that rx and tx routines are running
	ppSeq			uint64						// pingpong sequence no.
	ppCnt			int							// pingpong counter
	rxEno			PeMgrErrno					// rx errno
	txEno			PeMgrErrno					// tx errno
	ppEno			PeMgrErrno					// pingpong errno
	rxDiscard		int64						// number of rx messages discarded
	rxOkCnt			int64						// number of rx messages accepted
}

var peerInstDefault = PeerInstance {
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

func (pi *PeerInstance)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return pi.tep(ptn, msg)
}

func (pi *PeerInstance)peerInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	peerLog.Debug("peerInstProc: name: %s, msg.Id: %d", pi.name, msg.Id)

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
		peerLog.Debug("PeerInstProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	peerLog.Debug("peerInstProc: get out, name: %s, msg.Id: %d", pi.name, msg.Id)

	if eno != PeMgrEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (pi *PeerInstance)piPoweroff(ptn interface{}) PeMgrErrno {
	if pi.state == peInstStateKilling {
		peerLog.Debug("piPoweroff: already in killing, done at once, name: %s",
			pi.sdl.SchGetTaskName(pi.ptnMe))

		if pi.sdl.SchTaskDone(pi.ptnMe, sch.SchEnoKilled) != sch.SchEnoNone {
			return PeMgrEnoScheduler
		}

		return PeMgrEnoNone
	}
	peerLog.Debug("piPoweroff: task will be done, name: %s, state: %d",
		pi.sdl.SchGetTaskName(pi.ptnMe), pi.state)

	pi.stopRxTx()

	if pi.sdl.SchTaskDone(pi.ptnMe, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (pi *PeerInstance)piConnOutReq(_ interface{}) PeMgrErrno {
	if pi.dialer == nil ||
		pi.dir != PeInstDirOutbound  ||
		pi.state != peInstStateConnOut {
		peerLog.Debug("piConnOutReq: instance mismatched, pi: %s", pi.name)
		return PeMgrEnoInternal
	}

	var (
		addr = &net.TCPAddr{IP: pi.node.IP, Port: int(pi.node.TCP)}
		conn net.Conn = nil
		err error
		eno PeMgrErrno = PeMgrEnoNone
	)

	peerLog.Debug("piConnOutReq: try to dial target: %s", addr.String())

	pi.dialer.Timeout = pi.cto
	if conn, err = pi.dialer.Dial("tcp", addr.String()); err != nil {
		peerLog.Debug("piConnOutReq: dial failed, local: %s, to: %s, err: %s",
			fmt.Sprintf("%s:%d", pi.node.IP.String(), pi.node.TCP),
			addr.String(), err.Error())
		eno = PeMgrEnoOs
	} else {
		pi.conn = conn
		pi.laddr = conn.LocalAddr().(*net.TCPAddr)
		pi.raddr = conn.RemoteAddr().(*net.TCPAddr)
		pi.state = peInstStateConnected

		peerLog.Debug("piConnOutReq: dial ok, laddr: %s, raddr: %s",
			pi.laddr.String(),
			pi.raddr.String())
	}

	schMsg := sch.SchMessage{}
	rsp := msgConnOutRsp {
		result:	eno,
		snid:	pi.snid,
		peNode:	&pi.node,
		ptn:	pi.ptnMe,
	}

	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeConnOutRsp, &rsp)
	pi.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *PeerInstance)piHandshakeReq(_ interface{}) PeMgrErrno {
	if pi == nil {
		peerLog.Debug("piHandshakeReq: invalid instance")
		return PeMgrEnoParameter
	}

	if pi.state != peInstStateConnected && pi.state != peInstStateAccepted {
		peerLog.Debug("piHandshakeReq: instance mismatched")
		return PeMgrEnoInternal
	}

	if pi.conn == nil {
		peerLog.Debug("piHandshakeReq: invalid instance")
		return PeMgrEnoInternal
	}

	var eno PeMgrErrno

	if pi.dir == PeInstDirInbound {
		eno = pi.piHandshakeInbound(pi)
	} else if pi.dir == PeInstDirOutbound {
		eno = pi.piHandshakeOutbound(pi)
	} else {
		peerLog.Debug("piHandshakeReq: invalid instance direction: %d", pi.dir)
		eno = PeMgrEnoInternal
	}

	peerLog.Debug("piHandshakeReq: handshake result: %d, snid: %x, dir: %d, laddr: %s, raddr: %s, peer: %s",
				eno,
				pi.snid,
				pi.dir,
				pi.laddr.String(),
				pi.raddr.String(),
				fmt.Sprintf("%+v", pi.node))

	rsp := msgHandshakeRsp {
		result:	eno,
		dir:	pi.dir,
		snid:	pi.snid,
		peNode:	&pi.node,
		ptn:	pi.ptnMe,
	}
	schMsg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeHandshakeRsp, &rsp)
	pi.sdl.SchSendMessage(&schMsg)

	return eno
}

func (pi *PeerInstance)piPingpongReq(msg interface{}) PeMgrErrno {
	if pi.ppEno != PeMgrEnoNone {
		peerLog.Debug("piPingpongReq: nothing done, ppEno: %d", pi.ppEno)
		return PeMgrEnoResource
	}

	if pi.conn == nil {
		peerLog.Debug("piPingpongReq: connection had been closed")
		return PeMgrEnoResource
	}

	pi.ppSeq = msg.(*MsgPingpongReq).seq
	ping := Pingpong {
		Seq:	pi.ppSeq,
		Extra:	nil,
	}

	upkg := new(P2pPackage)
	if eno := upkg.ping(pi, &ping, false); eno != PeMgrEnoNone {
		peerLog.Debug("piPingpongReq: ping failed, eno: %d", eno)
		return eno
	}

	pi.ppChan<-upkg

	return PeMgrEnoNone
}

func (pi *PeerInstance)piCloseReq(_ interface{}) PeMgrErrno {
	if pi.state == peInstStateKilling {
		peerLog.Debug("piCloseReq: already in killing, task: %s", pi.sdl.SchGetTaskName(pi.ptnMe))
		return PeMgrEnoDuplicated
	}

	pi.state = peInstStateKilling
	node := pi.node

	pi.stopRxTx()

	cfm := MsgCloseCfm {
		result: PeMgrEnoNone,
		dir:	pi.dir,
		snid:	pi.snid,
		peNode:	&node,
		ptn:	pi.ptnMe,
	}

	schMsg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseCfm, &cfm)
	pi.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *PeerInstance)piEstablishedInd(msg interface{}) PeMgrErrno {
	cfmCh := *msg.(*chan int)
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
		peerLog.Debug("piEstablishedInd: SchSetTimer failed, pi: %s, eno: %d", pi.name, schEno)
		cfmCh<-PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	pi.ppTid = tid
	pi.txEno = PeMgrEnoNone
	pi.rxEno = PeMgrEnoNone
	pi.ppEno = PeMgrEnoNone

	if err := pi.conn.SetDeadline(time.Time{}); err != nil {
		peerLog.Debug("piEstablishedInd: SetDeadline failed, error: %s", err.Error())
		why := sch.PEC_FOR_SETDEADLINE
		msg := sch.SchMessage{}
		req := sch.MsgPeCloseReq{
			Ptn: pi.ptnMe,
			Snid: pi.snid,
			Node: config.Node {
				ID: pi.node.ID,
			},
			Dir: pi.dir,
			Why: why,
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

func (pi *PeerInstance)piPingpongTimerHandler() PeMgrErrno {
	msg := sch.SchMessage{}
	if pi.ppCnt++; pi.ppCnt > PeInstMaxPingpongCnt {
		pi.ppEno = PeMgrEnoPingpongTh
		why := sch.PEC_FOR_PINGPONG
		req := sch.MsgPeCloseReq {
			Ptn: pi.ptnMe,
			Snid: pi.snid,
			Node: pi.node,
			Dir: pi.dir,
			Why: why,
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

func (pi *PeerInstance)piTxDataReq(_ interface{}) PeMgrErrno {
	// not applied
	return PeMgrEnoMismatched
}

func (pi *PeerInstance)piRxDataInd(msg interface{}) PeMgrErrno {
	return pi.piP2pPkgProc(msg.(*P2pPackage))
}

func (pi *PeerInstance)checkHandshakeInfo(hs *Handshake) bool {
	pass := false
	if pi.peMgr.dynamicSubNetIdExist(&hs.Snid) {
		pass = true
	} else if pi.peMgr.staticSubNetIdExist(&hs.Snid){
		for _, sn := range pi.peMgr.cfg.staticNodes {
			if bytes.Compare(sn.ID[0:], hs.NodeId[0:]) == 0 {
				pass = true
				break
			}
		}
	}
	return pass
}

func (pi *PeerInstance)piHandshakeInbound(inst *PeerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs *Handshake

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		peerLog.Debug("piHandshakeInbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}

	peerLog.Debug("piHandshakeInbound: snid: %x, peer-ip: %s, hs: %+v",
		hs.Snid, hs.IP.String(), *hs)

	if pi.checkHandshakeInfo(hs) != true {
		peerLog.Debug("piHandshakeInbound: checkHandshakeInfo failed, snid: %x, peer-ip: %s, hs: %+v",
			hs.Snid, hs.IP.String(), *hs)
		return PeMgrEnoNotfound
	}

	// backup info about protocols supported by peer. notice that here we can
	// check against the ip and tcp port from handshake with that obtained from
	// underlying network, but we not now.
	inst.snid = hs.Snid
	pi.peMgr.setHandshakeParameters(inst, hs.Snid)

	inst.node.ID = hs.NodeId
	inst.node.IP = append(inst.node.IP, hs.IP...)
	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)
	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols

	// write outbound handshake to remote peer
	hs2peer := Handshake{}
	hs2peer.Snid = inst.snid
	hs2peer.NodeId = inst.localNode.ID
	hs2peer.IP = append(hs2peer.IP, inst.localNode.IP ...)
	hs2peer.UDP = uint32(inst.localNode.UDP)
	hs2peer.TCP = uint32(inst.localNode.TCP)
	hs2peer.ProtoNum = inst.localProtoNum
	hs2peer.Protocols = inst.localProtocols

	if eno = pkg.putHandshakeOutbound(inst, &hs2peer); eno != PeMgrEnoNone {
		peerLog.Debug("piHandshakeInbound: write outbound Handshake message failed, eno: %d", eno)
		return eno
	}
	inst.state = peInstStateHandshook

	return PeMgrEnoNone
}

func (pi *PeerInstance)piHandshakeOutbound(inst *PeerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs = new(Handshake)

	// write outbound handshake to remote peer
	hs.Snid = pi.snid
	hs.NodeId = pi.localNode.ID
	hs.IP = append(hs.IP, pi.localNode.IP...)
	hs.UDP = uint32(pi.localNode.UDP)
	hs.TCP = uint32(pi.localNode.TCP)
	hs.ProtoNum = pi.localProtoNum
	hs.Protocols = append(hs.Protocols, pi.localProtocols...)

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {
		peerLog.Debug("piHandshakeOutbound: write outbound Handshake message failed, eno: %d", eno)
		return eno
	}

	// read inbound handshake from remote peer
	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		peerLog.Debug("piHandshakeOutbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}

	// check handshake
	if pi.checkHandshakeInfo(hs) != true {
		peerLog.Debug("piHandshakeOutbound: checkHandshakeInfo failed, snid: %x, peer-ip: %s, hs: %+v",
			hs.Snid, hs.IP.String(), *hs)
		return PeMgrEnoNotfound
	}

	// check sub network identity
	if hs.Snid != inst.snid {
		peerLog.Debug("piHandshakeOutbound: subnet identity mismathced")
		return PeMgrEnoMessage
	}

	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos.
	if hs.NodeId != inst.node.ID ||
		inst.node.TCP != uint16(hs.TCP) ||
		bytes.Compare(inst.node.IP, hs.IP) != 0	{
		peerLog.Debug("piHandshakeOutbound: handshake mismathced, ip: %s, port: %d, id: %x",
			hs.IP.String(), hs.TCP, hs.NodeId)
		return PeMgrEnoMessage
	}

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
	// of chain, see function ShellManager.broadcastReq for details please.
	if len(pkg.IdList) == 0 {
		peerLog.Debug("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}
	pem := pkg.P2pInst.SchGetTaskObject(sch.PeerMgrName)
	if pem == nil {
		peerLog.Debug("SendPackage: nil peer manager, might be in power off stage")
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
	why := sch.PEC_FOR_COMMAND
	for _, idEx := range idExList {
		var req = sch.MsgPeCloseReq{
			Ptn: nil,
			Snid: *snid,
			Node: config.Node {
				ID: *id,
			},
			Dir: idEx.Dir,
			Why: why,
		}
		var schMsg= sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
		peMgr.sdl.SchSendMessage(&schMsg)
	}
	return PeMgrEnoNone
}

func piTx(pi *PeerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit for "txChan" closed.

	defer func() {
		if err := recover(); err != nil {
			peerLog.Debug("piTx: exception raised, wait done...")
			for {
				if _, ok := <-pi.txChan; !ok {
					peerLog.Debug("piTx: done")
					return
				}
			}
		}
	}()

	ask4Close := func(eno PeMgrErrno) {

		// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning of
		// this instance, BUT at this moment, the message queue of peer manager might
		// be FULL, so the instance would be blocked while sending; AND the peer manager
		// might had fired pi.txDone and been blocked by pi.txExit. panic is called
		// for such a overload system, see scheduler please.

		pi.txEno = eno
		why := sch.PEC_FOR_TXERROR
		req := sch.MsgPeCloseReq{
			Ptn:  pi.ptnMe,
			Snid: pi.snid,
			Node: pi.node,
			Dir:  pi.dir,
			Why:  why,
		}

		msg := sch.SchMessage{}
		pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
		pi.sdl.SchSendMessage(&msg)

		peerLog.Debug("piTx: failed, EvPeCloseReq sent. snid: %x, dir: %d, peer-ip: %s",
			pi.snid, pi.dir, pi.node.IP.String())
	}

	var (
		isPP	bool
		isData	bool
		okPP	bool
		okData	bool
		ppkg	*P2pPackage
		upkg	*P2pPackage
	)

_txLoop:

	for {

		isPP	= false
		isData	= false
		okPP	= false
		okData	= false
		ppkg	= (*P2pPackage)(nil)
		upkg	= (*P2pPackage)(nil)

		select {
		case ppkg, okPP = <-pi.ppChan:
			isPP	= true
			isData	= false
		case upkg, okData = <-pi.txChan:
			isPP	= false
			isData	= true
		}

		// check if any pendings or done
		if isPP {

			if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			if okPP && ppkg != nil {

				pi.txSeq += 1

				if eno := ppkg.SendPackage(pi); eno == PeMgrEnoNone {

					pi.txOkCnt += 1

				} else {

					pi.txFailedCnt += 1
					ask4Close(eno)
				}
			}

		} else if isData {

			if !okData {
				break _txLoop
			}

			if upkg == nil {
				peerLog.Debug("piTx: nil package")
				continue
			}

			if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			pi.txPendNum -= 1
			pi.txSeq += 1

			if eno := upkg.SendPackage(pi); eno == PeMgrEnoNone {

				pi.txOkCnt += 1

			} else {

				pi.txFailedCnt += 1
				ask4Close(eno)
			}
		}

		if pi.txSeq & 0x3ff == 0 {
			peerLog.Debug("piTx: txSeq: %d, txOkCnt: %d, txFailedCnt: %d, sent. snid: %x, dir: %d, peer: %x",
				pi.txSeq, pi.txOkCnt, pi.txFailedCnt, pi.snid, pi.dir, pi.node.ID)
		}
	}

	peerLog.Debug("piTx: exit, snid: %x, dir: %d, peer: %x", pi.snid, pi.dir, pi.node.ID)
	return PeMgrEnoNone
}

func piRx(pi *PeerInstance) PeMgrErrno {

	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit.

	defer func() {
		if err := recover(); err != nil {
			peerLog.Debug("piRx: exception raised, wait done...")
			<-pi.rxDone
			close(pi.rxDone)
		}
	}()

	var done PeMgrErrno = PeMgrEnoNone
	var ok = true

_rxLoop:

	for {

		select {
		case done, ok = <-pi.rxDone:
			peerLog.Debug("piRx: pi: %s, done with: %d", pi.name, done)
			if ok {
				close(pi.rxDone)
			}
			break _rxLoop

		default:
		}

		// if in errors, sleep then continue to check done
		if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 10)
			continue
		}

		// try reading the peer
		upkg := new(P2pPackage)
		if eno := upkg.RecvPackage(pi); eno != PeMgrEnoNone {

			// 1) if failed, ask the user to done, so he can close this peer seems in troubles,
			// and we will be done then;
			// 2) it is possible that, while we are blocked here in reading and the connection
			// is closed for some reasons(for example the user close the peer), in this case,
			// we would get an error;

			peerLog.Debug("piRx: error, snid: %x, ip: %s", pi.snid, pi.node.IP.String())
			why := sch.PEC_FOR_RXERROR
			pi.rxEno = eno
			req := sch.MsgPeCloseReq {
				Ptn: pi.ptnMe,
				Snid: pi.snid,
				Node: pi.node,
				Dir: pi.dir,
				Why: why,
			}

			// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning
			// this instance, BUT at this moment, the message queue of peer manager might
			// be FULL, so the instance would be blocked while sending; AND the peer manager
			// might had fired pi.txDone and been blocked by pi.txExit. panic is called
			// for such a overload system, see scheduler please.

			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
			pi.sdl.SchSendMessage(&msg)

			peerLog.Debug("piRx: failed, EvPeCloseReq sent. snid: %x, dir: %d, peer-ip: %s",
				pi.snid, pi.dir, pi.node.IP.String())

			continue
		}

		upkg.DebugPeerPackage()

		if upkg.Pid == uint32(PID_P2P) {

			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMe, sch.EvPeRxDataInd, upkg)
			pi.sdl.SchSendMessage(&msg)

		} else if upkg.Pid == uint32(PID_EXT) {

			if len(pi.rxChan) >= cap(pi.rxChan) {

				peerLog.Debug("piRx: rx queue full, snid: %x, dir: %d, pi: %s, peer: %x",
					pi.snid, pi.dir, pi.name, pi.node.ID)

				if pi.rxDiscard += 1; pi.rxDiscard & 0x1f == 0 {
					peerLog.Debug("piRx: snid: %x, dir: %d, pi: %s, rxDiscard: %d",
						pi.snid, pi.dir, pi.name, pi.rxDiscard)
				}

			} else {

				peerInfo := PeerInfo{}
				pkgCb := P2pPackageRx{}
				peerInfo.Protocols = nil
				peerInfo.Snid = pi.snid
				peerInfo.Dir = pi.dir
				peerInfo.NodeId = pi.node.ID
				peerInfo.IP = pi.node.IP
				peerInfo.TCP = uint32(pi.node.TCP)
				peerInfo.UDP = uint32(pi.node.UDP)
				peerInfo.ProtoNum = pi.protoNum
				peerInfo.Protocols = append(peerInfo.Protocols, pi.protocols...)
				pkgCb.Ptn = pi.ptnMe
				pkgCb.Payload = nil
				pkgCb.PeerInfo = &peerInfo
				pkgCb.ProtoId = int(upkg.Pid)
				pkgCb.MsgId = int(upkg.Mid)
				pkgCb.Key = upkg.Key
				pkgCb.PayloadLength = int(upkg.PayloadLength)
				pkgCb.Payload = append(pkgCb.Payload, upkg.Payload...)

				pi.rxChan <- &pkgCb

				if pi.rxOkCnt += 1; pi.rxOkCnt & 0x3ff == 0 {
					peerLog.Debug("piRx: rxOkCnt: %d, snid: %x, dir: %d, pi: %s",
						pi.rxOkCnt, pi.snid, pi.dir, pi.name)
				}
			}
		} else {
			peerLog.Debug("piRx: package discarded for unknown pid: pi: %s, %d", pi.name, upkg.Pid)
		}
	}

	peerLog.Debug("piRx: exit, snid: %x, dir: %d, peer: %x", pi.snid, pi.dir, pi.node.ID)
	return done
}

func (pi *PeerInstance)piP2pPkgProc(upkg *P2pPackage) PeMgrErrno {
	if upkg.Pid != uint32(PID_P2P) {
		peerLog.Debug("piP2pPkgProc: not a p2p package, pid: %d", upkg.Pid)
		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {
		peerLog.Debug("piP2pPkgProc: invalid payload length: %d", upkg.PayloadLength)
		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {
		peerLog.Debug("piP2pPkgProc: payload length mismatched, PlLen: %d, real: %d",
			upkg.PayloadLength, len(upkg.Payload))
		return PeMgrEnoMessage
	}

	msg := P2pMessage{}
	if eno := upkg.GetMessage(&msg); eno != PeMgrEnoNone {
		peerLog.Debug("piP2pPkgProc: GetMessage failed, eno: %d", eno	)
		return eno
	}

	// check message identity. we discard any handshake messages received here
	// since handshake procedure had been passed, and dynamic handshake is not
	// supported currently.

	switch msg.Mid {

	case uint32(MID_HANDSHAKE):
		peerLog.Debug("piP2pPkgProc: MID_HANDSHAKE, discarded")
		return PeMgrEnoMessage

	case uint32(MID_PING):
		return pi.piP2pPingProc(msg.Ping)

	case uint32(MID_PONG):
		return pi.piP2pPongProc(msg.Pong)

	default:
		peerLog.Debug("piP2pPkgProc: unknown mid: %d", msg.Mid)
		return PeMgrEnoMessage
	}

	return PeMgrEnoUnknown
}

func (pi *PeerInstance)piP2pPingProc(ping *Pingpong) PeMgrErrno {
	pong := Pingpong {
		Seq:	ping.Seq,
		Extra:	nil,
	}
	pi.ppCnt = 0
	upkg := new(P2pPackage)
	if eno := upkg.pong(pi, &pong, false); eno != PeMgrEnoNone {
		peerLog.Debug("piP2pPingProc: pong failed, eno: %d, pi: %s",
			eno, fmt.Sprintf("%+v", *pi))
		return eno
	}
	pi.ppChan<-upkg
	return PeMgrEnoNone
}

func (pi *PeerInstance)piP2pPongProc(pong *Pingpong) PeMgrErrno {
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
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		_, ok := peMgr.cfg.subNetNodeList[*snid]
		return ok
	}
	return false
}

func (peMgr *PeerManager)staticSubNetIdExist(snid *SubNetworkID) bool {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		return peMgr.cfg.staticSubNetId == *snid
	} else if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		return len(peMgr.cfg.staticNodes) > 0 && peMgr.cfg.staticSubNetId == *snid
	}
	return false
}

func (peMgr *PeerManager)setHandshakeParameters(inst *PeerInstance, snid config.SubNetworkID) {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	inst.networkType = peMgr.cfg.networkType
	inst.priKey = peMgr.cfg.subNetKeyList[snid]
	inst.localNode = peMgr.cfg.subNetNodeList[snid]
	inst.localProtoNum = peMgr.cfg.protoNum
	inst.localProtocols = peMgr.cfg.protocols
}

func (peMgr *PeerManager)GetLocalSubnetInfo()([]config.SubNetworkID, map[config.SubNetworkID]config.Node) {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if !peMgr.isInited {
		return nil, nil
	}
	return peMgr.cfg.subNetIdList, peMgr.cfg.subNetNodeList
}

func (peMgr *PeerManager)isStaticSubNetId(snid SubNetworkID) bool {
	return	(peMgr.cfg.networkType == config.P2pNetworkTypeStatic &&
		peMgr.staticSubNetIdExist(&snid) == true) ||
		(peMgr.cfg.networkType == config.P2pNetworkTypeDynamic &&
			peMgr.staticSubNetIdExist(&snid) == true)
}

func (peMgr *PeerManager) getWorkerInst(snid SubNetworkID, idEx *PeerIdEx) *PeerInstance {
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
		peerLog.Debug("RegisterInstIndCallback: register failed for shell task in running")
		return PeMgrEnoMismatched
	}

	if peMgr.indCb != nil {
		peerLog.Debug("RegisterInstIndCallback: callback duplicated")
		return PeMgrEnoDuplicated
	}

	if cb == nil {
		peerLog.Debug("RegisterInstIndCallback: try to register nil callback")
		return PeMgrEnoParameter
	}

	icb, ok := cb.(P2pIndCallback)
	if !ok {
		peerLog.Debug("RegisterInstIndCallback: invalid callback interface")
		return PeMgrEnoParameter
	}

	peMgr.indCb = icb
	peMgr.indCbUserData = userData

	go func() {
		for {
			select {
			case ind, ok := <-peMgr.indChan:
				if !ok  {
					peerLog.Debug("P2pIndCallback: indication channel closed, done")
					return
				}
				indType := reflect.TypeOf(ind).Elem().Name()
				switch indType {
				case "P2pIndPeerActivatedPara":
					peMgr.indCb(P2pIndPeerActivated, ind, peMgr.indCbUserData)
				case "P2pIndPeerClosedPara":
					peMgr.indCb(P2pIndPeerClosed, ind, peMgr.indCbUserData)
				default:
					peerLog.Debug("P2pIndCallback: discard unknown indication type: %s", indType)
				}
			}
		}
	}()

	return PeMgrEnoNone
}

func (pi *PeerInstance)stopRxTx() {
	if pi.rxtxRuning {
		if pi.conn != nil {
			pi.conn.Close()
			pi.conn = nil
		}

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
	pi.state = peInstStateKilled
	pi.rxtxRuning = false
}
