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
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"time"

	ggio "github.com/gogo/protobuf/io"
	"github.com/yeeco/gyee/log"
	config "github.com/yeeco/gyee/p2p/config"
	tab "github.com/yeeco/gyee/p2p/discover/table"
	um "github.com/yeeco/gyee/p2p/discover/udpmsg"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	nat "github.com/yeeco/gyee/p2p/nat"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// debug
//
type peerLogger struct {
	debug__      bool
	debugForce__ bool
}

var peerLog = peerLogger{
	debug__:      false,
	debugForce__: false,
}

func (log peerLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

func (log peerLogger) ForceDebug(fmt string, args ...interface{}) {
	if log.debugForce__ {
		p2plog.Debug(fmt, args...)
	}
}

// Peer manager errno
const (
	PeMgrEnoNone = iota
	PeMgrEnoParameter
	PeMgrEnoScheduler
	PeMgrEnoConfig
	PeMgrEnoResource
	PeMgrEnoOs
	PeMgrEnoNetTemporary
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
	Id  config.NodeID // node identity
	Dir int           // direction
}

type PeerIdExx struct {
	Snid config.SubNetworkID // sub network identity
	Node config.Node         // node
	Dir  int                 // direction
}

func (ide PeerIdExx) toString() string {
	return fmt.Sprintf("%x:%x:%s:%d:%d",
		ide.Snid,
		ide.Node.ID,
		ide.Node.IP.String(),
		ide.Node.TCP,
		ide.Dir)
}

type PeerReconfig struct {
	delList map[config.SubNetworkID]interface{} // sub networks to be deleted
	addList map[config.SubNetworkID]interface{} // sub networks to be added
}

// Peer identity as string
type PeerId = config.NodeID

// Peer information
type PeerInfo Handshake

// Peer manager parameters
const (
	defaultConnectTimeout = 16 * time.Second // default dial outbound timeout value, currently
	// it's a fixed value here than can be configurated
	// by other module.

	defaultHandshakeTimeout = 8 * time.Second // default handshake timeout value, currently
	// it's a fixed value here than can be configurated
	// by other module.

	defaultActivePeerTimeout = 0 * time.Second // default read/write operation timeout after a peer
	// connection is activaged in working.
	maxTcpmsgSize = 1024 * 1024 * 4 // max size of a tcpmsg package could be, currently
	// it's a fixed value here than can be configurated
	// by other module.

	durDcvFindNodeTimer = time.Second * 2 // duration to wait for find node response from discover task,
	// should be (findNodeExpiration + delta).

	durStaticRetryTimer = time.Second * 2 // duration to check and retry connect to static peers

	maxIndicationQueueSize = 512 // max indication queue size

	minDuration4FindNodeReq        = time.Second * 2 // min duration to send find-node-request again
	minDuration4OutboundConnectReq = time.Second * 1 // min duration to try oubound connect for a specific
	// sub-network and peer

	conflictAccessDelayLower = 500  // conflict delay lower bounder in time.Millisecond
	conflictAccessDelayUpper = 2000 // conflict delay upper bounder in time.Millisecond

	reconfigDelay = time.Second * 4 // reconfiguration delay time duration
)

// peer status
const (
	peerIdle             = iota // idle
	peerConnectOutInited        // connecting out inited
	peerActivated               // had been activated
	peerKilling                 // in killing
)

// peer manager configuration
type peMgrConfig struct {
	cfgName            string                            // p2p configuration name
	chainId			   uint32							 // chain identity
	ip                 net.IP                            // ip address
	port               uint16                            // tcp port number
	udp                uint16                            // udp port number, used with handshake procedure
	noDial             bool                              // do not dial outbound
	noAccept           bool                              // do not accept inbound
	bootstrapNode      bool                              // local is a bootstrap node
	defaultCto         time.Duration                     // default connect outbound timeout
	defaultHto         time.Duration                     // default handshake timeout
	defaultAto         time.Duration                     // default active read/write timeout
	maxMsgSize         int                               // max tcpmsg package size
	protoNum           uint32                            // local protocol number
	protocols          []Protocol                        // local protocol table
	networkType        int                               // p2p network type
	staticMaxPeers     int                               // max peers would be
	staticMaxOutbounds int                               // max concurrency outbounds
	staticMaxInBounds  int                               // max concurrency inbounds
	staticNodes        []*config.Node                    // static nodes
	staticSubNetId     SubNetworkID                      // static network identity
	subNetMaxPeers     map[SubNetworkID]int              // max peers would be
	subNetMaxOutbounds map[SubNetworkID]int              // max concurrency outbounds
	subNetMaxInBounds  map[SubNetworkID]int              // max concurrency inbounds
	subNetKeyList      map[SubNetworkID]ecdsa.PrivateKey // keys for sub-node
	subNetNodeList     map[SubNetworkID]config.Node      // sub-node identities
	subNetIdList       []SubNetworkID                    // sub network identity list. do not put the identity
	ibpNumTotal        int                               // total number of concurrency inbound peers
}

// start/stop/addr-switching... related
const (
	peMgrInNull                = 0
	peMgrInStartup             = 1
	peMgrInStoping             = 2
	peMgrInStopped             = 3
	pwMgrPubAddrOutofSwitching = 0
	peMgrPubAddrInSwitching    = 1
	peMgrPubAddrDelaySwitching = 2
)

const (
	PEMGR_STOP4NAT = "pubAddrSwitch"
)

type pasBackupItem struct {
	snid config.SubNetworkID // sub network identity
	node config.Node         // peer "node" information
}

// peer manager
type PeerManager struct {
	sdl           *sch.Scheduler                              // pointer to scheduler
	name          string                                      // name
	inited        chan PeMgrErrno                             // result of initialization
	isInited      bool                                        // is manager initialized ok
	tep           sch.SchUserTaskEp                           // entry
	cfg           peMgrConfig                                 // configuration
	tidFindNode   map[SubNetworkID]int                        // find node timer identity
	ptnMe         interface{}                                 // pointer to myself(peer manager task node)
	ptnTab        interface{}                                 // pointer to table task node
	ptnLsn        interface{}                                 // pointer to peer listener manager task node
	ptnAcp        interface{}                                 // pointer to peer acceptor manager task node
	ptnDcv        interface{}                                 // pointer to discover task node
	ptnShell      interface{}                                 // pointer to shell task node
	tabMgr        *tab.TableManager                           // pointer to table manager
	ibInstSeq     int                                         // inbound instance sequence number
	obInstSeq     int                                         // outbound instance sequence number
	lock          sync.Mutex                                  // for peer instance to access peer manager
	peers         map[interface{}]*PeerInstance               // map peer instance's task node pointer to instance pointer
	nodes         map[SubNetworkID]map[PeerIdEx]*PeerInstance // map peer node identity to instance pointer
	workers       map[SubNetworkID]map[PeerIdEx]*PeerInstance // map peer node identity to pointer of instance in work
	wrkNum        map[SubNetworkID]int                        // worker peer number
	ibpNum        map[SubNetworkID]int                        // active inbound peer number
	obpNum        map[SubNetworkID]int                        // active outbound peer number
	ibpTotalNum   int                                         // total active inbound peer number
	randoms       map[SubNetworkID][]*config.Node             // random nodes found by discover
	indChan       chan interface{}                            // indication signal
	indCb         P2pIndCallback                              // indication callback
	indCbUserData interface{}                                 // user data pointer for callback
	staticsStatus map[PeerIdEx]int                            // status about static nodes
	caTids        map[string]int                              // conflict access timer identity
	ocrTid        int                                         // OCR(outbound connect request) timestamp cleanup timer
	tmLastOCR     map[SubNetworkID]map[PeerId]time.Time       // time of last outbound connect request for sub-netowerk
	tmLastFNR     map[SubNetworkID]time.Time                  // time of last find node request sent for sub network
	reCfg         PeerReconfig                                // sub network reconfiguration
	reCfgTid      int                                         // reconfiguration timer
	inStartup     int                                         // if had been requestd to startup
	natResult     bool                                        // nat status
	pubTcpIp      net.IP                                      // public tcp ip
	pubTcpPort    int                                         // public tcp port
	pasStatus     int                                         // public addr switching status
	pasBackup     []pasBackupItem                             // backup list for nat public address switching
}

func NewPeerMgr() *PeerManager {
	var peMgr = PeerManager{
		name:          sch.PeerMgrName,
		inited:        make(chan PeMgrErrno, 1),
		cfg:           peMgrConfig{},
		tidFindNode:   map[SubNetworkID]int{},
		peers:         map[interface{}]*PeerInstance{},
		nodes:         map[SubNetworkID]map[PeerIdEx]*PeerInstance{},
		workers:       map[SubNetworkID]map[PeerIdEx]*PeerInstance{},
		wrkNum:        map[SubNetworkID]int{},
		ibpNum:        map[SubNetworkID]int{},
		obpNum:        map[SubNetworkID]int{},
		ibpTotalNum:   0,
		indChan:       make(chan interface{}, maxIndicationQueueSize),
		randoms:       map[SubNetworkID][]*config.Node{},
		staticsStatus: map[PeerIdEx]int{},
		caTids:        make(map[string]int, 0),
		ocrTid:        sch.SchInvalidTid,
		tmLastOCR:     make(map[SubNetworkID]map[PeerId]time.Time, 0),
		tmLastFNR:     make(map[SubNetworkID]time.Time, 0),
		reCfg: PeerReconfig{
			delList: make(map[config.SubNetworkID]interface{}, 0),
			addList: make(map[config.SubNetworkID]interface{}, 0),
		},
		reCfgTid:  sch.SchInvalidTid,
		inStartup: peMgrInNull,
		pasStatus: pwMgrPubAddrOutofSwitching,
	}
	peMgr.tep = peMgr.peerMgrProc
	return &peMgr
}

func (peMgr *PeerManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return peMgr.tep(ptn, msg)
}

func (peMgr *PeerManager) peerMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Debugf("peerMgrProc: name: %s, msg.Id: %d", peMgr.name, msg.Id)

	if peMgr.msgFilter(msg) != PeMgrEnoNone {
		log.Debugf("peerMgrProc: filtered out, id: %d", msg.Id)
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
		log.Debugf("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	log.Debugf("peerMgrProc: get out, name: %s, msg.Id: %d", peMgr.name, msg.Id)

	if eno != PeMgrEnoNone {
		schEno = sch.SchEnoUserTask
	}

	return schEno
}

func (peMgr *PeerManager) peMgrPoweron(ptn interface{}) PeMgrErrno {
	peMgr.ptnMe = ptn
	peMgr.sdl = sch.SchGetScheduler(ptn)
	_, peMgr.ptnLsn = peMgr.sdl.SchGetUserTaskNode(PeerLsnMgrName)


	log.Debugf("peMgrPoweron: inst: %s", peMgr.sdl.SchGetP2pCfgName())


	var cfg *config.Cfg4PeerManager
	if cfg = config.P2pConfig4PeerManager(peMgr.sdl.SchGetP2pCfgName()); cfg == nil {
		log.Debugf("peMgrPoweron: inited, inst: %s", peMgr.sdl.SchGetP2pCfgName())
		peMgr.inited <- PeMgrEnoConfig
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
		log.Debugf("peMgrPoweron: shell not found")
		return PeMgrEnoScheduler
	}

	peMgr.cfg = peMgrConfig{
		cfgName:       cfg.CfgName,
		chainId:	   cfg.ChainId,
		ip:            cfg.IP,
		port:          cfg.Port,
		udp:           cfg.UDP,
		noDial:        cfg.NoDial,
		noAccept:      cfg.NoAccept,
		bootstrapNode: cfg.BootstrapNode,
		defaultCto:    defaultConnectTimeout,
		defaultHto:    defaultHandshakeTimeout,
		defaultAto:    defaultActivePeerTimeout,
		maxMsgSize:    maxTcpmsgSize,
		protoNum:      cfg.ProtoNum,
		protocols:     make([]Protocol, 0),

		networkType:        cfg.NetworkType,
		staticMaxPeers:     cfg.StaticMaxPeers,
		staticMaxOutbounds: cfg.StaticMaxOutbounds,
		staticMaxInBounds:  cfg.StaticMaxInBounds,
		staticNodes:        cfg.StaticNodes,
		staticSubNetId:     cfg.StaticNetId,
		subNetMaxPeers:     cfg.SubNetMaxPeers,
		subNetMaxOutbounds: cfg.SubNetMaxOutbounds,
		subNetMaxInBounds:  cfg.SubNetMaxInBounds,
		subNetKeyList:      cfg.SubNetKeyList,
		subNetNodeList:     cfg.SubNetNodeList,
		subNetIdList:       cfg.SubNetIdList,
		ibpNumTotal:        0,
	}

	peMgr.cfg.ibpNumTotal = peMgr.cfg.staticMaxInBounds
	for _, ibpNum := range peMgr.cfg.subNetMaxInBounds {
		peMgr.cfg.ibpNumTotal += ibpNum
	}

	for _, p := range cfg.Protocols {
		peMgr.cfg.protocols = append(peMgr.cfg.protocols, Protocol{Pid: p.Pid, Ver: p.Ver})
	}

	// Notice: even the network type is P2pNetworkTypeDynamic, a static sub network can
	// still be exist, if static nodes are specified.
	for _, sn := range peMgr.cfg.staticNodes {
		idEx := PeerIdEx{Id: sn.ID, Dir: PeInstDirOutbound}
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
		log.Debugf("peMgrPoweron: invalid network type: %d", peMgr.cfg.networkType)
		return PeMgrEnoConfig
	}

	// tell initialization result, and EvPeMgrStartReq would be sent to us
	// some moment later.
	log.Debugf("peMgrPoweron: inited, inst: %s", peMgr.sdl.SchGetP2pCfgName())
	peMgr.isInited = true
	peMgr.inited <- PeMgrEnoNone

	return PeMgrEnoNone
}

func (peMgr *PeerManager) PeMgrInited() PeMgrErrno {
	return <-peMgr.inited
}

func (peMgr *PeerManager) PeMgrStart() PeMgrErrno {
	log.Debugf("PeMgrStart: EvPeMgrStartReq will be sent, target: %s", sch.PeerMgrName)
	msg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil)
	peMgr.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrPoweroff(ptn interface{}) PeMgrErrno {
	log.Debugf("peMgrPoweroff: task will be done, name: %s", sch.PeerMgrName)
	close(peMgr.indChan)
	for _, pi := range peMgr.peers {
		log.Debugf("peMgrPoweroff: send EvSchPoweroff to inst: %s, dir: %d, state: %d",
			pi.name, pi.dir, pi.state)
		if eno, ptn := peMgr.sdl.SchGetUserTaskNode(pi.name); eno != sch.SchEnoNone || ptn != pi.ptnMe {
			log.Debugf("peMgrPoweroff: not found, inst: %s, dir: %d, state: %d",
				pi.name, pi.dir, pi.state)
			continue
		}
		po := sch.SchMessage{
			Id:   sch.EvSchPoweroff,
		}
		peMgr.sdl.SchSetSender(&po, &sch.RawSchTask)
		peMgr.sdl.SchSetRecver(&po, pi.ptnMe)
		po.TgtName = pi.name
		peMgr.sdl.SchSendMessage(&po)
	}
	if peMgr.sdl.SchTaskDone(ptn, peMgr.name, sch.SchEnoKilled) != sch.SchEnoNone {
		log.Debugf("peMgrPoweroff: SchTaskDone faled")
		return PeMgrEnoScheduler
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrStartReq(_ interface{}) PeMgrErrno {
	peMgr.inStartup = peMgrInStartup
	if !peMgr.natResult {
		log.Debugf("peMgrStartReq: still not mapped by nat")
		return PeMgrEnoNone
	}
	log.Debugf("peMgrStartReq: task: %s", peMgr.name)
	return peMgr.start()
}

func (peMgr *PeerManager) ocrTimestampCleanup() {
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

func (peMgr *PeerManager) peMgrDcvFindNodeRsp(msg interface{}) PeMgrErrno {
	if peMgr.inStartup != peMgrInStartup || !peMgr.natResult {
		log.Debugf("peMgrDcvFindNodeRsp: not ready, inStartup: %d, natResult: %t",
			peMgr.inStartup, peMgr.natResult)
		return PeMgrEnoNone
	}

	var rsp, _ = msg.(*sch.MsgDcvFindNodeRsp)
	if rsp == nil {
		log.Debugf("peMgrDcvFindNodeRsp: invalid message")
		return PeMgrEnoParameter
	}

	if peerLog.debug__ {
		dbgStr := fmt.Sprintf("peMgrDcvFindNodeRsp: snid: %x, nodes: ", rsp.Snid)
		for idx := 0; idx < len(rsp.Nodes); idx++ {
			dbgStr = dbgStr + rsp.Nodes[idx].IP.String() + ","
		}
		log.Debugf(dbgStr)
	}

	if peMgr.dynamicSubNetIdExist(&rsp.Snid) != true {
		log.Debugf("peMgrDcvFindNodeRsp: subnet not exist")
		return PeMgrEnoNotfound
	}

	if _, ok := peMgr.reCfg.delList[rsp.Snid]; ok {
		log.Debugf("peMgrDcvFindNodeRsp: discarded for reconfiguration, snid: %x", rsp.Snid)
		return PeMgrEnoRecofig
	}

	var (
		snid     = rsp.Snid
		appended = make(map[SubNetworkID]int, 0)
		dup      bool
		idEx     PeerIdEx
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
		if dup {
			continue
		}

		dup = false
		for _, s := range peMgr.cfg.staticNodes {
			if s.ID == n.ID && snid == peMgr.cfg.staticSubNetId {
				dup = true
				break
			}
		}
		if dup {
			continue
		}

		localSubnetIp := peMgr.cfg.subNetNodeList[snid].IP
		localSubnetPort := peMgr.cfg.subNetNodeList[snid].TCP
		if n.IP.Equal(localSubnetIp) && n.TCP == localSubnetPort {
			continue
		}

		if len(peMgr.randoms[snid]) >= peMgr.cfg.subNetMaxPeers[snid] {
			log.Debugf("peMgrDcvFindNodeRsp: too much, some are truncated")
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

func (peMgr *PeerManager) peMgrDcvFindNodeTimerHandler(msg interface{}) PeMgrErrno {
	nwt := peMgr.cfg.networkType
	snid := msg.(*SubNetworkID)

	log.Debugf("peMgrDcvFindNodeTimerHandler: nwt: %d, snid: %x", nwt, *snid)

	if _, ok := peMgr.tidFindNode[*snid]; ok {
		peMgr.tidFindNode[*snid] = sch.SchInvalidTid
	} else {
		log.Debugf("peMgrDcvFindNodeTimerHandler: no timer for snid: %x", *snid)
	}

	if nwt == config.P2pNetworkTypeStatic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.staticMaxOutbounds {
			log.Debugf("peMgrDcvFindNodeTimerHandler: reach threshold: %d", peMgr.obpNum[*snid])
			return PeMgrEnoNone
		}
	} else if nwt == config.P2pNetworkTypeDynamic {
		if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
			log.Debugf("peMgrDcvFindNodeTimerHandler: reach threshold: %d", peMgr.obpNum[*snid])
			return PeMgrEnoNone
		}
	} else {
		log.Debugf("peMgrDcvFindNodeTimerHandler: invalid network type: %d", nwt)
		return PeMgrEnoParameter
	}

	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, snid)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrLsnConnAcceptedInd(msg interface{}) PeMgrErrno {
	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var ibInd, _ = msg.(*msgConnAcceptedInd)
	var peInst = new(PeerInstance)

	*peInst = peerInstDefault
	peInst.sdl = peMgr.sdl
	peInst.sdlName = peMgr.sdl.SchGetP2pCfgName()
	peInst.peMgr = peMgr
	peInst.tep = peInst.peerInstProc
	peInst.ptnMgr = peMgr.ptnMe
	peInst.state = peInstStateAccepted
	peInst.cto = peMgr.cfg.defaultCto
	peInst.hto = peMgr.cfg.defaultHto
	peInst.ato = peMgr.cfg.defaultAto
	peInst.maxPkgSize = peMgr.cfg.maxMsgSize
	peInst.dialer = nil
	peInst.conn = ibInd.conn
	peInst.laddr = ibInd.localAddr
	peInst.raddr = ibInd.remoteAddr
	peInst.dir = PeInstDirInbound

	peInst.txChan = make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.ppChan = make(chan *P2pPackage, PeInstMaxPings)
	peInst.rxChan = make(chan *P2pPackageRx, PeInstMaxP2packages)
	peInst.rxDone = make(chan PeMgrErrno)
	peInst.rxtxRuning = false

	peMgr.ibInstSeq++
	peInst.name = peInst.name + fmt.Sprintf("_inbound_%s",
		fmt.Sprintf("%d_", peMgr.ibInstSeq)+peInst.raddr.String())
	peInst.chainId = peMgr.cfg.chainId

	var tskDesc = sch.SchTaskDescription{
		Name:   peInst.name,
		MbSize: PeInstMailboxSize,
		Ep:     peInst,
		Wd:     &sch.SchWatchDog{HaveDog: false},
		Flag:   sch.SchCreatedGo,
		DieCb:  nil,
		UserDa: peInst,
	}

	log.Debugf("peMgrLsnConnAcceptedInd: inst: %s, peer: %s",
		peInst.name, peInst.raddr.String())

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc); eno != sch.SchEnoNone || ptnInst == nil {
		log.Debugf("peMgrLsnConnAcceptedInd: SchCreateTask failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	peInst.ptnMe = ptnInst
	peMgr.peers[peInst.ptnMe] = peInst

	// Send handshake request to the instance created aboved
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeHandshakeReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)

	// Pause inbound peer accepter if necessary:
	// we stop accepter simply, a duration of delay should be apply before pausing,
	// this should be improved later.
	if peMgr.ibpTotalNum++; peMgr.ibpTotalNum >= peMgr.cfg.ibpNumTotal {
		if !peMgr.cfg.noAccept {
			schMsg = sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStopReq, nil)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrOutboundReq(msg interface{}) PeMgrErrno {
	if peMgr.cfg.noDial || peMgr.cfg.bootstrapNode {
		log.Debugf("PeerManager: no outbound for noDial or boostrapNode: %t, %t")
		return PeMgrEnoNone
	}
	// if sub network identity is not specified, means all are wanted
	var snid *SubNetworkID
	if msg != nil {
		snid = msg.(*SubNetworkID)
	}
	if snid == nil {
		if eno := peMgr.peMgrStaticSubNetOutbound(); eno != PeMgrEnoNone {
			log.Debugf("peMgrOutboundReq: peMgrStaticSubNetOutbound failed, eno: %d", eno)
			return eno
		}
		if peMgr.cfg.networkType != config.P2pNetworkTypeStatic {
			for _, id := range peMgr.cfg.subNetIdList {
				if eno := peMgr.peMgrDynamicSubNetOutbound(&id); eno != PeMgrEnoNone {
					log.Debugf("peMgrOutboundReq: peMgrDynamicSubNetOutbound failed, eno: %d", eno)
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

func (peMgr *PeerManager) peMgrStaticSubNetOutbound() PeMgrErrno {
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
	var idEx = PeerIdEx{
		Id:  config.NodeID{},
		Dir: PeInstDirOutbound,
	}
	for _, n := range peMgr.cfg.staticNodes {
		idEx.Id = n.ID
		_, dup := peMgr.nodes[snid][idEx]
		if !dup && peMgr.staticsStatus[idEx] == peerIdle {
			candidates = append(candidates, n)
			count++
		}
	}

	var failed = 0
	var ok = 0
	idEx = PeerIdEx{Id: config.NodeID{}, Dir: PeInstDirOutbound}
	for cdNum := len(candidates); cdNum > 0; cdNum-- {
		idx := rand.Intn(cdNum)
		n := candidates[idx]
		idEx.Id = n.ID
		if idx != len(candidates)-1 {
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

	if peMgr.obpNum[snid] < peMgr.cfg.staticMaxOutbounds {
		if eno := peMgr.peMgrAsk4More(&snid); eno != PeMgrEnoNone {
			return eno
		}
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrDynamicSubNetOutbound(snid *SubNetworkID) PeMgrErrno {
	if peMgr.wrkNum[*snid] >= peMgr.cfg.subNetMaxPeers[*snid] {
		return PeMgrEnoResource
	}
	if peMgr.obpNum[*snid] >= peMgr.cfg.subNetMaxOutbounds[*snid] {
		return PeMgrEnoResource
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

	// Create outbound instances for candidates if any,
	// check OCR timer also.
	var failed = 0
	var ok = 0
	maxOutbound := peMgr.cfg.subNetMaxOutbounds[*snid]
	for _, n := range candidates {
		if tmPeers, exist := peMgr.tmLastOCR[*snid]; exist {
			if t, ok := tmPeers[n.ID]; ok {
				if time.Now().Sub(t) <= minDuration4OutboundConnectReq {
					log.Debugf("peMgrDynamicSubNetOutbound: too early, " +
						"snid: %x, peer: %x", *snid, n.IP.String())
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
func (peMgr *PeerManager) peMgrConnOutRsp(msg interface{}) PeMgrErrno {
	var rsp, _ = msg.(*msgConnOutRsp)
	if rsp.result != PeMgrEnoNone {
		if pi, lived := peMgr.peers[rsp.ptn]; lived {
			kip := kiParameters{
				ptn:   rsp.ptn,
				state: pi.state,
				node:  &pi.node,
				dir:   PeInstDirOutbound,
				name:  pi.name,
			}
			if eno := peMgr.peMgrKillInst(&kip, PKI_FOR_BOUNDOUT_FAILED); eno != PeMgrEnoNone {
				log.Debugf("peMgrConnOutRsp: peMgrKillInst failed, eno: %d", eno)
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

func (peMgr *PeerManager) peMgrHandshakeRsp(msg interface{}) PeMgrErrno {

	// This is an event from an instance task of outbound or inbound peer, telling
	// the result about the handshake procedure between a pair of peers.

	var rsp = msg.(*msgHandshakeRsp)
	var inst *PeerInstance
	var lived bool

	if inst, lived = peMgr.peers[rsp.ptn]; inst == nil || !lived {
		log.Debugf("peMgrHandshakeRsp: instance not found")
		return PeMgrEnoNotfound
	}

	if inst.ptnMe != rsp.ptn || inst.dir != rsp.dir {
		log.Debugf("peMgrHandshakeRsp: mismatched with instance, inst: %x, %d, %x",
			inst.name, inst.snid, inst.dir)
		return PeMgrEnoMismatched
	}

	if inst.dir == PeInstDirOutbound &&
		(inst.snid != rsp.snid || !bytes.Equal(inst.node.ID[0:], rsp.peNode.ID[0:])) {
		log.Debugf("peMgrHandshakeRsp: mismatched with instance, inst: %x, %d, %x",
			inst.name, inst.snid, inst.dir)
		return PeMgrEnoMismatched
	}

	if rsp.result != PeMgrEnoNone {
		if rsp.dir == PeInstDirInbound && !(rsp.peNode == nil && rsp.snid == config.AnySubNet) {
			// rsp.peNode must be nil, since the handshake procedure failed, see event
			// EvPeHandshakeReq handler for details pls.
			log.Debugf("peMgrHandshakeRsp: internal errors")
			return PeMgrEnoInternal
		}
		log.Debugf("peMgrHandshakeRsp: failed, inst: %s, snid: %x, dir: %d, result: %d",
			inst.name, inst.snid, inst.dir, rsp.result)
		kip := kiParameters{
			ptn:   inst.ptnMe,
			state: inst.state,
			node:  rsp.peNode,
			dir:   inst.dir,
			name:  inst.name,
		}
		peMgr.peMgrKillInst(&kip, PKI_FOR_HANDSHAKE_FAILED)
		if rsp.dir == PeInstDirOutbound {
			// notice: for outbound, rsp.peNode would not be nil; and since we kill this failed
			// outbound instance, we request outbound at once.
			idEx := PeerIdEx{Id: rsp.peNode.ID, Dir: rsp.dir}
			peMgr.updateStaticStatus(rsp.snid, idEx, peerKilling)
			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &inst.snid)
			peMgr.sdl.SchSendMessage(&schMsg)
		}
		return PeMgrEnoNone
	}

	// Check duplicated for inbound instance. Notice: only here the peer manager can known the
	// identity of peer to determine if it's duplicated to an outbound instance, which is an
	// instance connect from local to outside.

	if rsp.peNode == nil || rsp.snid != inst.snid {
		// must not fulfil since handshake ok
		log.Debugf("peMgrHandshakeRsp: internal errors")
		return PeMgrEnoInternal
	}

	var maxInbound = 0
	var maxOutbound = 0
	var maxPeers = 0
	var snid = rsp.snid

	kip := kiParameters{
		ptn:   inst.ptnMe,
		state: inst.state,
		node:  &inst.node,
		dir:   inst.dir,
		name:  inst.name,
	}
	idEx := PeerIdEx{Id: rsp.peNode.ID, Dir: rsp.dir}

	if rsp.dir == PeInstDirInbound {
		peMgr.ibpNum[rsp.snid] += 1
	}

	if _, ok := peMgr.reCfg.delList[rsp.snid]; ok {
		log.Debugf("peMgrHandshakeRsp: kill for reconfiguration, inst: %s, snid: %x, dir: %d",
			inst.name, inst.snid, inst.dir)
		peMgr.peMgrKillInst(&kip, PKI_FOR_RECONFIG)
		return PeMgrEnoRecofig
	}

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
		log.Debugf("peMgrHandshakeRsp: too much workers, inst: %s, snid: %x, dir: %d",
			inst.name, inst.snid, inst.dir)
		peMgr.updateStaticStatus(snid, idEx, peerKilling)
		peMgr.peMgrKillInst(&kip, PKI_FOR_TOOMUCH_WORKERS)
		return PeMgrEnoResource
	}

	idExTemp := idEx
	idExTemp.Dir = PeInstDirInbound
	if _, dup := peMgr.workers[snid][idExTemp]; dup {
		log.Debugf("peMgrHandshakeRsp: duplicated to inbound worker, " +
			"inst: %s, snid: %x, dir: %d",
			inst.name, inst.snid, inst.dir)
		peMgr.peMgrKillInst(&kip, PKI_FOR_IBW_DUPLICATED)
		return PeMgrEnoDuplicated
	}

	idExTemp.Dir = PeInstDirOutbound
	if _, dup := peMgr.workers[snid][idExTemp]; dup {
		log.Debugf("peMgrHandshakeRsp: duplicated to outbound worker, " +
			"inst: %s, snid: %x, dir: %d",
			inst.name, inst.snid, inst.dir)
		peMgr.peMgrKillInst(&kip, PKI_FOR_OBW_DUPLICATED)
		return PeMgrEnoDuplicated
	}

	if inst.dir == PeInstDirInbound {
		if peMgr.isStaticSubNetId(snid) {
			peMgr.workers[snid][idEx] = inst
		} else {
			if peMgr.ibpNum[snid] >= maxInbound {
				log.Debugf("peMgrHandshakeRsp: inbound too much, " +
					"inst: %s, snid: %x, dir: %d",
					inst.name, inst.snid, inst.dir)
				peMgr.peMgrKillInst(&kip, PKI_FOR_TOOMUCH_INBOUNDS)
				return PeMgrEnoResource
			}
			idExTemp.Dir = PeInstDirOutbound
			if _, dup := peMgr.nodes[snid][idExTemp]; dup {
				// this duplicated case, we kill one instance here, but the peer might kill
				// what he saw there also at the "same time", then two connections are lost,
				// protection needed for this case.
				log.Debugf("peMgrHandshakeRsp: inbound conflict to outbound, " +
					"inst: %s, snid: %x, dir: %d",
					inst.name, inst.snid, inst.dir)
				peMgr.peMgrKillInst(&kip, PKI_FOR_IB2OB_DUPLICATED)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)
				return PeMgrEnoDuplicated
			}
		}

		peMgr.nodes[snid][idEx] = inst

	} else if inst.dir == PeInstDirOutbound {
		if peMgr.isStaticSubNetId(snid) {
			peMgr.workers[snid][idEx] = inst
		} else {
			if peMgr.obpNum[snid] >= maxOutbound {
				log.Debugf("peMgrHandshakeRsp: outbound, too much workers, " +
					"inst: %s, snid: %x, dir: %d",
					inst.name, inst.snid, inst.dir)
				peMgr.peMgrKillInst(&kip, PKI_FOR_TOOMUCH_OUTBOUNDS)
				return PeMgrEnoResource
			}
			idExTemp.Dir = PeInstDirInbound
			if _, dup := peMgr.nodes[snid][idExTemp]; dup {
				// conflict
				log.Debugf("peMgrHandshakeRsp: outbound conflicts to inbound, " +
					"inst: %s, snid: %x, dir: %d",
					inst.name, inst.snid, inst.dir)
				peMgr.peMgrKillInst(&kip, PKI_FOR_OB2IB_DUPLICATED)
				peMgr.peMgrConflictAccessProtect(rsp.snid, rsp.peNode, rsp.dir)
				schMsg := sch.SchMessage{}
				peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &inst.snid)
				peMgr.sdl.SchSendMessage(&schMsg)
				return PeMgrEnoDuplicated
			}
		}
	}

	// bug: the following sch.EvPeEstablishedInd message contains a channel for the
	// receiver task to confirm us, might cause "deadlock" issues in "poweroff".
	cfmCh := make(chan int)
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeEstablishedInd, &cfmCh)
	peMgr.sdl.SchSendMessage(&schMsg)
	if eno, ok := <-cfmCh; eno != PeMgrEnoNone || !ok {
		log.Debugf("peMgrHandshakeRsp: confirm failed, " +
			"inst: %s, snid: %x, dir: %d, state: %d, eno: %d",
			inst.name, inst.snid, inst.dir, inst.state, eno)
		if ok {
			close(cfmCh)
		}
		kip := kiParameters{
			ptn:   rsp.ptn,
			state: inst.state,
			node:  &inst.node,
			dir:   inst.dir,
			name:  inst.name,
		}
		peMgr.peMgrKillInst(&kip, PKI_FOR_FAILED_INST_CFM)
		return PeMgrErrno(eno)
	}

	close(cfmCh)
	peMgr.workers[snid][idEx] = inst
	peMgr.wrkNum[snid]++
	peMgr.updateStaticStatus(snid, idEx, peerActivated)

	if inst.dir == PeInstDirInbound &&
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
			log.Debugf("peMgrHandshakeRsp: TabBucketAddNode failed, " +
				"inst: %s, snid: %x, dir: %d, state: %d, eno: %d",
				inst.name, inst.snid, inst.dir, inst.state, tabEno)
		}
		tabEno = peMgr.tabMgr.TabUpdateNode(snid, &n)
		if tabEno != tab.TabMgrEnoNone {
			log.Debugf("peMgrHandshakeRsp: TabUpdateNode failed, " +
				"inst: %s, snid: %x, dir: %d, state: %d, eno: %d",
				inst.name, inst.snid, inst.dir, inst.state, tabEno)
		}
	}

	// indicate activation of a peer instance to other modules:
	// if shell task present, send EvShellPeerActiveInd to it and then return;
	// else push the indication to queue(for callback method).
	i := P2pIndPeerActivatedPara{
		P2pInst: peMgr.sdl,
		RxChan:  inst.rxChan,
		PeerInfo: &Handshake{
			Snid:      inst.snid,
			Dir:       inst.dir,
			NodeId:    inst.node.ID,
			IP:        net.IP{},
			UDP:       uint32(inst.node.UDP),
			TCP:       uint32(inst.node.TCP),
			ProtoNum:  inst.protoNum,
			Protocols: inst.protocols,
		},
	}
	i.PeerInfo.IP = append(i.PeerInfo.IP, inst.node.IP...)
	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerActiveInd{
			TxChan:   inst.txChan,
			RxChan:   inst.rxChan,
			PeerInfo: i.PeerInfo,
			PeerInst: inst,
		}
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerActiveInd, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
		return PeMgrEnoNone
	}
	return peMgr.peMgrIndEnque(&i)
}

func (peMgr *PeerManager) peMgrCloseReq(msg *sch.SchMessage) PeMgrErrno {
	// here it's asked to close a peer instance, this might happen in following cases:
	// 1) the shell task ask to do this;
	// 2) a peer instance gone into bad status so it asks to be closed;
	// 3) function ClosePeer called from outside modules
	// with 2), if shell task is present, we should not send EvPeCloseReq to peer instance
	// since the shell might access to the peer (we had tell it with EvShellPeerActiveInd),
	// instead we should inform the shell task, so it should send us EvPeCloseReq, and then
	// we can send EvPeCloseReq to the peer instance.
	var req = msg.Body.(*sch.MsgPeCloseReq)
	var snid = req.Snid
	var idEx = PeerIdEx{Id: req.Node.ID, Dir: req.Dir}
	why, _ := req.Why.(string)
	inst := peMgr.getWorkerInst(snid, &idEx)
	if inst == nil {
		log.Debugf("peMgrCloseReq: worker not found")
		return PeMgrEnoNotfound
	}
	if inst.state < peInstStateNull {
		log.Debugf("peMgrCloseReq: already in killing or killed, state: %d", inst.state)
		return PeMgrEnoDuplicated
	}
	if ptnSender := peMgr.sdl.SchGetSender(msg); ptnSender != peMgr.ptnShell {
		// req.Ptn is nil, means the sender is peMgr.ptnShell, so need not to
		// send EvShellPeerAskToCloseInd to it again.
		if req.Ptn != nil {
			ind := sch.MsgShellPeerAskToCloseInd{
				Snid:   snid,
				PeerId: idEx.Id,
				Dir:    idEx.Dir,
				Why:    req.Why,
			}

			log.Debugf("peMgrCloseReq: why: %s, inst: %s, snid: %x, dir: %d, ip: %s, port: %d",
				why, inst.name, req.Snid, req.Dir, req.Node.IP.String(), req.Node.TCP)

			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerAskToCloseInd, &ind)
			peMgr.sdl.SchSendMessage(&schMsg)
			return PeMgrEnoNone
		}
	}

	log.Debugf("peMgrCloseReq: why: %s, inst: %s, snid: %x, dir: %d, ip: %s, port: %d",
		why, inst.name, inst.snid, inst.dir, inst.node.IP.String(), inst.node.TCP)

	peMgr.updateStaticStatus(snid, idEx, peerKilling)
	req.Node = inst.node
	req.Ptn = inst.ptnMe
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrConnCloseCfm(msg interface{}) PeMgrErrno {
	cfm, _ := msg.(*MsgCloseCfm)
	kip := kiParameters{
		name:  cfm.name,
		ptn:   cfm.ptn,
		state: cfm.state,
		node:  cfm.peNode,
		dir:   cfm.dir,
	}

	log.Debugf("peMgrConnCloseCfm: inst: %s, snid: %x, dir: %d, state: %d",
		cfm.name, cfm.snid, cfm.dir, cfm.state)

	if eno := peMgr.peMgrKillInst(&kip, PKI_FOR_CLOSE_CFM); eno != PeMgrEnoNone {
		log.Debugf("peMgrConnCloseCfm: peMgrKillInst failed, inst: %s, snid: %x, dir: %d, state: %d",
			cfm.name, cfm.snid, cfm.dir, cfm.state)
		return PeMgrEnoScheduler
	}
	i := P2pIndPeerClosedPara{
		P2pInst: peMgr.sdl,
		Snid:    cfm.snid,
		PeerId:  cfm.peNode.ID,
		Dir:     cfm.dir,
	}
	if peMgr.ptnShell != nil {
		ind2Sh := sch.MsgShellPeerCloseCfm{
			Result: int(cfm.result),
			Dir:    cfm.dir,
			Snid:   cfm.snid,
			PeerId: cfm.peNode.ID,
		}
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerCloseCfm, &ind2Sh)
		peMgr.sdl.SchSendMessage(&schMsg)
	} else {
		peMgr.peMgrIndEnque(&i)
	}

	if peMgr.inStartup == peMgrInStartup {

		log.Debugf("peMgrConnCloseCfm: send EvPeOutboundReq")
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, &cfm.snid)
		peMgr.sdl.SchSendMessage(&schMsg)

	} else if peMgr.inStartup == peMgrInStoping {

		if peMgr.isNoneOfInstances() {

			if peMgr.pasStatus == peMgrPubAddrInSwitching ||
				peMgr.pasStatus == peMgrPubAddrDelaySwitching {

				log.Debugf("peMgrConnCloseCfm: calll pubAddrSwitch")
				peMgr.pubAddrSwitch()

			} else {

				log.Debugf("peMgrConnCloseCfm: transfer to peMgrInStopped")
				peMgr.inStartup = peMgrInStopped
			}
		}

	} else if peMgr.inStartup == peMgrInStopped {

		log.Debugf("peMgrConnCloseCfm: confirmed while in peMgrInStopped")
		return PeMgrEnoMismatched
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrConnCloseInd(msg interface{}) PeMgrErrno {
	// this would never happen since a peer instance would never
	// kill himself in current implement.
	log.Errorf("peMgrConnCloseInd: should never come here!!!")
	return PeMgrEnoInternal
}

func (peMgr *PeerManager) natMgrReadyInd(msg *sch.MsgNatMgrReadyInd) PeMgrErrno {
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

func (peMgr *PeerManager) natMakeMapRsp(msg *sch.MsgNatMgrMakeMapRsp) PeMgrErrno {
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

func (peMgr *PeerManager) natPubAddrUpdateInd(msg *sch.MsgNatMgrPubAddrUpdateInd) PeMgrErrno {

	log.Debugf("natPubAddrUpdateInd: entered")

	oldNatResult := peMgr.natResult
	oldIp := peMgr.pubTcpIp
	oldTcp := peMgr.pubTcpPort

	natMapRecovered := func() {
		if !peMgr.pubTcpIp.Equal(oldIp) || peMgr.pubTcpPort != oldTcp {
			if peMgr.pasStatus == pwMgrPubAddrOutofSwitching {
				if peMgr.reCfgTid != sch.SchInvalidTid {
					log.Debugf("natMapRecovered: enter peMgrPubAddrDelaySwitching")
					peMgr.pasStatus = peMgrPubAddrDelaySwitching
				} else {
					log.Debugf("natMapRecovered: enter peMgrPubAddrInSwitching")
					peMgr.pasStatus = peMgrPubAddrInSwitching

					log.Debugf("natMapRecovered: call pubAddrSwitchPrepare")
					peMgr.pubAddrSwitchPrepare()

					log.Debugf("natMapRecovered: call stop")
					peMgr.stop(PEMGR_STOP4NAT)
				}
			}
		} else if peMgr.inStartup == peMgrInStartup {
			log.Debugf("natMapRecovered: call PeMgrStart")
			peMgr.PeMgrStart()
		}
	}

	natMapChanged := natMapRecovered

	natMapLost := func() {
		if oldNatResult {
			log.Debugf("natMapLost: call stop")
			peMgr.stop(PEMGR_STOP4NAT)
		}
	}

	if nat.NatIsStatusOk(msg.Status) {
		if msg.Proto == nat.NATP_TCP {
			peMgr.pubTcpIp = msg.PubIp
			peMgr.pubTcpPort = msg.PubPort
			if !oldNatResult {
				log.Debugf("natPubAddrUpdateInd: call natMapRecovered")
				natMapRecovered()
			} else if !msg.PubIp.Equal(oldIp) || oldTcp != msg.PubPort {
				log.Debugf("natPubAddrUpdateInd: call natMapChanged")
				natMapChanged()
			}
		}
	} else if msg.Proto == nat.NATP_TCP {
		peMgr.natResult = false
		peMgr.pubTcpIp = net.IPv4zero
		peMgr.pubTcpPort = 0
		if oldNatResult {
			log.Debugf("natPubAddrUpdateInd: call natMapLost")
			natMapLost()
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager) start() PeMgrErrno {
	if peMgr.cfg.noAccept == false {
		msg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&msg)
		log.Debugf("start: EvPeLsnStartReq sent")
	}

	tdOcr := sch.TimerDescription{
		Name:  "_pocrTimer",
		Utid:  sch.PeMinOcrCleanupTimerId,
		Tmt:   sch.SchTmTypePeriod,
		Dur:   minDuration4OutboundConnectReq,
		Extra: nil,
	}
	eno := sch.SchEnoNone
	eno, peMgr.ocrTid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &tdOcr)
	if eno != sch.SchEnoNone || peMgr.ocrTid == sch.SchInvalidTid {
		log.Debugf("start: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	log.Debugf("start: ocrTid start ok")

	msg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	peMgr.sdl.SchSendMessage(&msg)
	log.Debugf("start: EvPeOutboundReq sent")

	return PeMgrEnoNone
}

func (peMgr *PeerManager) stop(why interface{}) PeMgrErrno {
	if peMgr.reCfgTid != sch.SchInvalidTid {
		log.Debugf("stop: faied, in reconfiguring")
		return PeMgrEnoRecofig
	}

	log.Debugf("stop: kill caTids")
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
		log.Debugf("stop: EvPeLsnStopReq sent")
	}

	log.Debugf("stop: randoms cleared")
	peMgr.randoms = make(map[SubNetworkID][]*config.Node, 0)

	log.Debugf("stop: kill ocrTid")
	if peMgr.ocrTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, peMgr.ocrTid)
		peMgr.ocrTid = sch.SchInvalidTid
	}

	log.Debugf("stop: kill tidFindNode")
	for _, tid := range peMgr.tidFindNode {
		if tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
		}
	}
	peMgr.tidFindNode = make(map[SubNetworkID]int, 0)

	// try to close all peer instances: need to check the state of the instance to
	// be closed.
	log.Debugf("stop: try to kill all peer instances")
	for ptn, pi := range peMgr.peers {

		log.Debugf("stop: peer, inst: %s, snid: %x, dir: %d, ip: %s",
			pi.name, pi.snid, pi.dir, pi.node.IP.String())

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
				if pi.state == peInstStateActivated {

					log.Debugf("stop: send EvShellPeerAskToCloseInd, inst: %s, snid: %x, ip: %s, dir: %d",
						pi.name, snid, pi.node.IP.String(), pi.dir)

					ind := sch.MsgShellPeerAskToCloseInd{
						Snid:   snid,
						PeerId: idex.Id,
						Dir:    idex.Dir,
						Why:    why,
					}
					msg := sch.SchMessage{}
					peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellPeerAskToCloseInd, &ind)
					peMgr.sdl.SchSendMessage(&msg)
					continue
				}
			}
		}

		// else kill directly since these instances are not reported to other
		// modules at all, and piTx/piRx must not start for them.
		log.Debugf("stop: send EvPeCloseReq, inst: %s, snid: %x, ip: %s, dir: %d",
			pi.name, snid, pi.node.IP.String(), pi.dir)
		req := sch.MsgPeCloseReq{
			Ptn:  ptn,
			Snid: snid,
			Node: node,
			Dir:  dir,
			Why:  why,
		}
		msg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
		peMgr.sdl.SchSendMessage(&msg)
	}

	log.Debugf("stop: transfer to peMgrInStoping")
	peMgr.inStartup = peMgrInStoping
	return PeMgrEnoNone
}

func (peMgr *PeerManager) msgFilter(msg *sch.SchMessage) PeMgrErrno {
	eno := PeMgrErrno(PeMgrEnoNone)
	if peMgr.inStartup == peMgrInNull {
		switch msg.Id {
		case sch.EvSchPoweron:
		case sch.EvSchPoweroff:
		case sch.EvNatMgrReadyInd:
		case sch.EvNatMgrMakeMapRsp:
		case sch.EvPeMgrStartReq:
		default:
			log.Debugf("msgFilter: filtered out for peMgrInNull, msg.Id: %d", msg.Id)
			eno = PeMgrEnoMismatched
		}
	} else if peMgr.inStartup != peMgrInStartup {
		switch msg.Id {
		case sch.EvSchPoweroff:
		case sch.EvPeCloseReq:
		case sch.EvPeCloseCfm:
		case sch.EvPeCloseInd:
		default:
			log.Debugf("msgFilter: filtered out for inStartup: %d, msg.Id: %d", peMgr.inStartup, msg.Id)
			eno = PeMgrEnoMismatched
		}
	}
	return eno
}

func (peMgr *PeerManager) pubAddrSwitchPrepare() PeMgrErrno {
	peMgr.pasBackup = make([]pasBackupItem, 0)
	for _, piOfSubnet := range peMgr.workers {
		if piOfSubnet != nil && len(piOfSubnet) > 0 {
			for _, pi := range piOfSubnet {
				item := pasBackupItem{
					snid: pi.snid,
					node: pi.node,
				}
				log.Debugf("pubAddrSwitchPrepare: peer backup, name, snid: %x, ip: %s",
					pi.name, pi.snid, pi.node.IP.String())
				peMgr.pasBackup = append(peMgr.pasBackup, item)
			}
		}
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager) pubAddrSwitch() PeMgrErrno {
	log.Debugf("pubAddrSwitch: transfer to peMgrInStartup and pwMgrPubAddrOutofSwitching")
	peMgr.inStartup = peMgrInStartup
	peMgr.pasStatus = pwMgrPubAddrOutofSwitching

	log.Debugf("pubAddrSwitch: restore peers in bakup list")
	peMgr.randoms = make(map[SubNetworkID][]*config.Node, 0)
	for _, item := range peMgr.pasBackup {
		snid := item.snid
		if _, ok := peMgr.randoms[snid]; !ok {
			peMgr.randoms[snid] = make([]*config.Node, 0)
		}
		peMgr.randoms[snid] = append(peMgr.randoms[snid], &item.node)
		log.Debugf("pubAddrSwitch: backup one, snid: %x, ip: %s",
			item.snid, item.node.IP.String())
	}

	log.Debugf("pubAddrSwitch: switch to new ip: %s, port: %d",
		peMgr.pubTcpIp.String(), peMgr.pubTcpPort)

	peMgr.cfg.ip = peMgr.pubTcpIp
	peMgr.cfg.port = uint16(peMgr.pubTcpPort)
	peMgr.cfg.udp = uint16(peMgr.pubTcpPort)
	for k, old := range peMgr.cfg.subNetNodeList {
		n := config.Node{
			ID:  old.ID,
			IP:  peMgr.pubTcpIp,
			TCP: uint16(peMgr.pubTcpPort),
			UDP: uint16(peMgr.pubTcpPort),
		}
		peMgr.cfg.subNetNodeList[k] = n
	}

	log.Debugf("pubAddrSwitch: call start")
	return peMgr.start()
}

func (peMgr *PeerManager) isNoneOfInstances() bool {
	for _, piOfSubnet := range peMgr.workers {
		if piOfSubnet != nil && len(piOfSubnet) > 0 {
			return false
		}
	}
	for _, piOfSubnet := range peMgr.nodes {
		if piOfSubnet != nil && len(piOfSubnet) > 0 {
			return false
		}
	}
	return peMgr.peers == nil || len(peMgr.peers) == 0
}

func (peMgr *PeerManager) peMgrDataReq(msg interface{}) PeMgrErrno {
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

	if inst.state != peInstStateActivated {
		log.Debugf("peMgrDataReq: discarded, not activated, inst: %s, snid: %x, dir: %d, state: %d",
			inst.name, inst.snid, inst.dir, inst.state)
		return PeMgrEnoNotfound
	}

	if len(inst.txChan) >= cap(inst.txChan) {
		log.Debugf("peMgrDataReq: discarded, tx queue full, inst: %s, snid: %x, dir: %d,",
			inst.name, inst.snid, inst.dir)
		return PeMgrEnoResource
	}

	_pkg := req.Pkg.(*P2pPackage)
	inst.txChan <- _pkg
	inst.txPendNum += 1

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrCreateOutboundInst(snid *config.SubNetworkID, node *config.Node) PeMgrErrno {

	var eno = sch.SchEnoNone
	var ptnInst interface{} = nil
	var peInst = new(PeerInstance)

	*peInst = peerInstDefault
	peInst.sdl = peMgr.sdl
	peInst.sdlName = peMgr.sdl.SchGetP2pCfgName()
	peInst.peMgr = peMgr
	peInst.tep = peInst.peerInstProc
	peInst.ptnMgr = peMgr.ptnMe
	peInst.state = peInstStateConnOut
	peInst.cto = peMgr.cfg.defaultCto
	peInst.hto = peMgr.cfg.defaultHto
	peInst.ato = peMgr.cfg.defaultAto
	peInst.maxPkgSize = peMgr.cfg.maxMsgSize
	peInst.dialer = &net.Dialer{Timeout: peMgr.cfg.defaultCto}
	peInst.conn = nil
	peInst.laddr = nil
	peInst.raddr = nil
	peInst.dir = PeInstDirOutbound
	peInst.networkType = peMgr.cfg.networkType
	peInst.snid = *snid
	peInst.priKey = peMgr.cfg.subNetKeyList[*snid]
	peInst.localNode = peMgr.cfg.subNetNodeList[*snid]
	peInst.localProtoNum = peMgr.cfg.protoNum
	peInst.localProtocols = peMgr.cfg.protocols

	peInst.node = *node

	peInst.txChan = make(chan *P2pPackage, PeInstMaxP2packages)
	peInst.ppChan = make(chan *P2pPackage, PeInstMaxPings)
	peInst.rxChan = make(chan *P2pPackageRx, PeInstMaxP2packages)
	peInst.rxDone = make(chan PeMgrErrno)
	peInst.rxtxRuning = false

	peMgr.obInstSeq++
	peInst.name = peInst.name + fmt.Sprintf("_Outbound_%s", fmt.Sprintf("%d", peMgr.obInstSeq))
	peInst.chainId = peMgr.cfg.chainId
	tskDesc := sch.SchTaskDescription{
		Name:   peInst.name,
		MbSize: PeInstMailboxSize,
		Ep:     peInst,
		Wd:     &sch.SchWatchDog{HaveDog: false},
		Flag:   sch.SchCreatedGo,
		DieCb:  nil,
		UserDa: peInst,
	}

	log.Debugf("peMgrCreateOutboundInst: inst: %s, snid: %x, peer: %s",
		peInst.name, *snid, node.IP.String())

	if eno, ptnInst = peMgr.sdl.SchCreateTask(&tskDesc); eno != sch.SchEnoNone || ptnInst == nil {
		log.Debugf("peMgrCreateOutboundInst: SchCreateTask failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	peInst.ptnMe = ptnInst
	peMgr.peers[peInst.ptnMe] = peInst
	idEx := PeerIdEx{Id: peInst.node.ID, Dir: peInst.dir}
	peMgr.nodes[*snid][idEx] = peInst
	peMgr.obpNum[*snid]++

	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeConnOutReq, nil)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

const (
	PKI_FOR_CLOSE_CFM         = "peMgrConnCloseCfm"
	PKI_FOR_RECONFIG          = "reConfig"
	PKI_FOR_FAILED_INST_CFM   = "instCfmFailed"
	PKI_FOR_BOUNDOUT_FAILED   = "peMgrConnOutRsp"
	PKI_FOR_HANDSHAKE_FAILED  = "hsFailed"
	PKI_FOR_TOOMUCH_WORKERS   = "maxWorks"
	PKI_FOR_TOOMUCH_OUTBOUNDS = "maxOutbounds"
	PKI_FOR_TOOMUCH_INBOUNDS  = "maxInbounds"
	PKI_FOR_IBW_DUPLICATED    = "dup2InboundWorker"
	PKI_FOR_OBW_DUPLICATED    = "dup2OutboundWorker"
	PKI_FOR_IB2OB_DUPLICATED  = "inBoundDup2OutBound"
	PKI_FOR_OB2IB_DUPLICATED  = "outBoundDup2InBound"
)

type kiParameters struct {
	name  string        // instance name
	ptn   interface{}   // pointer to task instance node of sender
	state peerInstState // instance state while it's terminated
	node  *config.Node  // target node
	dir   int           // direction
}

func (peMgr *PeerManager) peMgrKillInst(kip *kiParameters, why interface{}) PeMgrErrno {

	//
	// notice: since this function will call SchStopTask to done the connection
	// instance directly, it must not be called while piTx/piRx are running. to
	// use this function to free an instance, one must be sure that the piTx/piRx
	// are never "go" or both had been stopped;
	// notice: kip.node might be nil, when killing an inbound instance which fail
	// to handshake with;
	// notice: when kill for PKI_FOR_HANDSHAKE_FAILED, the peer node identity and
	// subnet identity are all not unknown, since the handshake procedure failed;
	//

	ptn := kip.ptn
	dir := kip.dir
	why = why.(string)

	log.Debugf("peMgrKillInst: inst: %s, dir: %d, state: %d, why: %s",
		kip.name, kip.dir, kip.state, why)

	if why == PKI_FOR_FAILED_INST_CFM {
		// notice: in current implement, this kind of "why" must not come out,
		// see function peMgrHandshakeRsp and piEstablishedInd for details pls.
		log.Errorf("peMgrKillInst: impossible")
		return PeMgrEnoInternal
	}

	peInst := peMgr.peers[ptn]
	if peInst == nil {
		log.Debugf("peMgrKillInst: instance not found, inst: %s, kip: %s", peInst.name, kip.name)
		return PeMgrEnoInternal
	}

	if peInst.dir != dir {
		log.Debugf("peMgrKillInst: invalid parameters, inst: %s, kip: %s", peInst.name, kip.name)
		return PeMgrEnoInternal
	}

	if peInst.ppTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(ptn, peInst.ppTid)
		peInst.ppTid = sch.SchInvalidTid
	}

	if peInst.conn != nil {
		peInst.conn.Close()
	}

	if why == PKI_FOR_CLOSE_CFM || why == PKI_FOR_RECONFIG {
		snid := peInst.snid
		idEx := PeerIdEx{Id: peInst.node.ID, Dir: peInst.dir}
		if _, exist := peMgr.workers[snid][idEx]; exist {
			delete(peMgr.workers[snid], idEx)
			if peMgr.wrkNum[snid]--; peMgr.wrkNum[snid] < 0 {
				log.Debugf("peMgrKillInst: inst: %s, kip: %s", peInst.name, kip.name)
				return PeMgrEnoInternal
			}
		}
	}

	if peInst.dir == PeInstDirOutbound {
		snid := peInst.snid
		idEx := PeerIdEx{Id: peInst.node.ID, Dir: peInst.dir}
		delete(peMgr.nodes[snid], idEx)
		delete(peMgr.peers, ptn)
		if peMgr.obpNum[snid]--; peMgr.obpNum[snid] < 0 {
			log.Debugf("peMgrKillInst: inst: %s, kip: %s", peInst.name, kip.name)
			return PeMgrEnoInternal
		}
	} else if peInst.dir == PeInstDirInbound {
		delete(peMgr.peers, ptn)
		if peMgr.ibpTotalNum--; peMgr.ibpTotalNum < 0 {
			log.Debugf("peMgrKillInst: inst: %s, kip: %s", peInst.name, kip.name)
			return PeMgrEnoInternal
		}
		snid := peInst.snid
		idEx := PeerIdEx{Id: peInst.node.ID, Dir: peInst.dir}
		if why != PKI_FOR_HANDSHAKE_FAILED {
			if peMgr.ibpNum[snid]--; peMgr.ibpNum[snid] < 0 {
				log.Debugf("peMgrKillInst: inst: %s, kip: %s", peInst.name, kip.name)
				return PeMgrEnoInternal
			}
		}
		if why == PKI_FOR_CLOSE_CFM || why == PKI_FOR_RECONFIG {
			// notice: see function peMgrHandshakeRsp and some related functions for
			// details pls. in current implement, only these two "why" for which we
			// need to clean the peMgr.nodes.
			if _, exist := peMgr.nodes[snid][idEx]; exist {
				delete(peMgr.nodes[snid], idEx)
			}
		}
	} else {
		log.Debugf("peMgrKillInst: inst: %s, kip: %s", peInst.name, kip.name)
		return PeMgrEnoInternal
	}

	if why != PKI_FOR_HANDSHAKE_FAILED || peInst.dir == PeInstDirOutbound {
		snid := peInst.snid
		idEx := PeerIdEx{Id: peInst.node.ID, Dir: peInst.dir}
		peMgr.updateStaticStatus(snid, idEx, peerIdle)
	}

	if peMgr.cfg.noAccept == false &&
		peMgr.ibpTotalNum < peMgr.cfg.ibpNumTotal {
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
		peMgr.sdl.SchSendMessage(&schMsg)
	}

	peInst.state = peInstStateKilled
	peMgr.sdl.SchStopTask(ptn, kip.name)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrConflictAccessProtect(snid config.SubNetworkID, peer *config.Node, dir int) PeMgrErrno {
	delay := conflictAccessDelayLower + rand.Intn(conflictAccessDelayUpper-conflictAccessDelayLower)
	dur := time.Millisecond * time.Duration(delay)
	idexx := PeerIdExx{
		Snid: snid,
		Node: *peer,
		Dir:  dir,
	}

	td := sch.TimerDescription{
		Name:  "_conflictTimer",
		Utid:  sch.PeConflictAccessTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   dur,
		Extra: &idexx,
	}

	log.Debugf("peMgrConflictAccessProtect: set timer, utid: %d, dur: %d, idexx: %+v", td.Utid, td.Dur, idexx)

	eno, tid := peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		log.Debugf("peMgrConflictAccessProtect: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}
	idexx.toString()
	peMgr.caTids[idexx.toString()] = tid
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrCatHandler(msg interface{}) PeMgrErrno {
	idexx := msg.(*PeerIdExx)
	delete(peMgr.caTids, (*idexx).toString())
	r := sch.MsgDcvFindNodeRsp{
		Snid:  idexx.Snid,
		Nodes: []*config.Node{&idexx.Node},
	}
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvDcvFindNodeRsp, &r)
	peMgr.sdl.SchSendMessage(&schMsg)
	log.Debugf("peMgrCatHandler: EvDcvFindNodeRsp sent, peer: %s, idexx: %+v", idexx.Node.IP.String(), idexx)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) reconfigTimerHandler() PeMgrErrno {
	peMgr.reCfgTid = sch.SchInvalidTid
	for del := range peMgr.reCfg.delList {
		wks, ok := peMgr.workers[del]
		if !ok {
			continue
		}
		why := sch.PEC_FOR_RECONFIG
		for _, peerInst := range wks {

			log.Debugf("reconfigTimerHandler: send EvPeCloseReq, inst: %s, snid: %x, dir: %d, ip: %s",
				peerInst.name, peerInst.snid, peerInst.dir, peerInst.node.IP.String())

			req := sch.MsgPeCloseReq{
				Ptn:  peerInst.ptnMe,
				Snid: peerInst.snid,
				Node: peerInst.node,
				Dir:  peerInst.dir,
				Why:  why,
			}
			msg := sch.SchMessage{}
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

func (peMgr *PeerManager) shellReconfigReq(msg *sch.MsgShellReconfigReq) PeMgrErrno {
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
		log.Debugf("shellReconfigReq: previous reconfiguration not completed")
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
		for _, peerInst := range wks {
			if count++; count >= wkNum/2 {
				break
			}

			log.Debugf("shellReconfigReq: send EvPeCloseReq, inst: %s, snid: %x, dir: %d,  ip: %s",
				peerInst.name, peerInst.snid, peerInst.dir, peerInst.node.IP.String())

			why := sch.PEC_FOR_RECONFIG_REQ
			req := sch.MsgPeCloseReq{
				Ptn:  peerInst.ptnMe,
				Snid: peerInst.snid,
				Node: peerInst.node,
				Dir:  peerInst.dir,
				Why:  why,
			}
			schMsg := sch.SchMessage{}
			peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
			if eno := peMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
				log.Debug("shellReconfigReq: SchSendMessage failed, eno: %d", eno)
				return PeMgrEnoInternal
			}
		}

		if tid, ok := peMgr.tidFindNode[del]; ok && tid != sch.SchInvalidTid {
			peMgr.sdl.SchKillTimer(peMgr.ptnMe, tid)
			delete(peMgr.tidFindNode, del)
		}
	}

	// start timer for remain peer instances of deleting part
	td := sch.TimerDescription{
		Name:  "_recfgTimer",
		Utid:  sch.PeReconfigTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   reconfigDelay,
		Extra: nil,
	}
	if eno, tid := peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone {
		log.Debugf("shellReconfigReq: SchSetTimer failed, eno: %d", eno)
		return PeMgrEnoInternal
	} else {
		peMgr.reCfgTid = tid
	}

	// cleanup the randoms
	for _, del := range delList {
		if _, ok := peMgr.randoms[del]; ok {
			delete(peMgr.randoms, del)
		}
	}

	// tell discover manager that sub networks changed
	if eno := peMgr.peMgrRecfg2DcvMgr(); eno != PeMgrEnoNone {
		log.Debugf("shellReconfigReq: peMgrRecfg2DcvMgr failed, eno: %d", eno)
		return PeMgrEnoInternal
	}

	// tell shell manager to update subnet info
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnShell, sch.EvShellSubnetUpdateReq, nil)
	if eno := peMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debugf("shellReconfigReq: SchSendMessage failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrRecfg2DcvMgr() PeMgrErrno {
	req := sch.MsgDcvReconfigReq{
		DelList: make(map[config.SubNetworkID]interface{}, 0),
		AddList: make(map[config.SubNetworkID]interface{}, 0),
	}
	for del := range peMgr.reCfg.delList {
		req.DelList[del] = nil
	}
	for add := range peMgr.reCfg.addList {
		req.AddList[add] = nil
	}
	schMsg := sch.SchMessage{}
	peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvReconfigReq, &req)
	peMgr.sdl.SchSendMessage(&schMsg)
	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrAsk4More(snid *SubNetworkID) PeMgrErrno {
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
			log.Debugf("peMgrAsk4More: no more needed, obpNum: %d, max: %d",
				peMgr.obpNum[*snid],
				peMgr.cfg.subNetMaxOutbounds[*snid])
			return PeMgrEnoNone
		}

		req := sch.MsgDcvFindNodeReq{
			Snid:    *snid,
			More:    more,
			Include: nil,
			Exclude: nil,
		}
		schMsg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvFindNodeReq, &req)
		peMgr.sdl.SchSendMessage(&schMsg)
		timerName = fmt.Sprintf("%s%x", sch.PeerMgrName+"_DcvFindNodeTimer_", *snid)

		log.Debugf("peMgrAsk4More: " +
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
	var td = sch.TimerDescription{
		Name:  timerName,
		Utid:  sch.PeDcvFindNodeTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   dur,
		Extra: &extra,
	}

	if oldTid, ok := peMgr.tidFindNode[*snid]; ok && oldTid != sch.SchInvalidTid {
		peMgr.sdl.SchKillTimer(peMgr.ptnMe, oldTid)
		peMgr.tidFindNode[*snid] = sch.SchInvalidTid
	}

	if eno, tid = peMgr.sdl.SchSetTimer(peMgr.ptnMe, &td); eno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debugf("peMgrAsk4More: SchSetTimer failed, eno: %d", eno)
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
		log.Debugf(dbgStr)
	}

	return PeMgrEnoNone
}

func (peMgr *PeerManager) peMgrIndEnque(ind interface{}) PeMgrErrno {
	if len(peMgr.indChan) >= cap(peMgr.indChan) {
		log.Debugf("peMgrIndEnque: system overload")
		return PeMgrEnoResource
	}
	peMgr.indChan <- ind
	return PeMgrEnoNone
}

//
// Dynamic peer instance task
//
const peInstTaskName = "peInstTsk"
const (
	peInstStateNull            = iota // null
	peInstStateConnOut                // outbound connection inited
	peInstStateAccepted               // inbound accepted, need handshake
	peInstStateConnected              // outbound connected, need handshake
	peInstStateHandshakeFailed        // handshake failed
	peInstStateHandshook              // handshook
	peInstStateActivated              // actived in working
	peInstStateKilling         = -1   // in killing
	peInstStateKilled          = -2   // killed
)

type peerInstState int // instance state type

const PeInstDirNull = -1    // null, so connection should be nil
const PeInstDirInbound = 0  // inbound connection
const PeInstDirOutbound = 1 // outbound connection

const PeInstMailboxSize = 512                // mailbox size
const PeInstMaxP2packages = 1024              // max p2p packages pending to be sent
const PeInstMaxPings = 8                     // max pings pending to be sent
const PeInstMaxPingpongCnt = 4               // max pingpong counter value
const PeInstPingpongCycle = time.Second * 16 // pingpong period

type PeerInstance struct {
	sdl    *sch.Scheduler      // pointer to scheduler
	sdlName	string			   // scheduler instance name
	peMgr  *PeerManager        // pointer to peer manager
	name   string              // name
	chainId uint32			   // chain identity
	tep    sch.SchUserTaskEp   // entry
	ptnMe  interface{}         // the instance task node pointer
	ptnMgr interface{}         // the peer manager task node pointer
	state  peerInstState       // state
	cto    time.Duration       // connect timeout value
	hto    time.Duration       // handshake timeout value
	ato    time.Duration       // active peer connection read/write timeout value
	dialer *net.Dialer         // dialer to make outbound connection
	conn   net.Conn            // connection
	iow    ggio.WriteCloser    // IO writer
	ior    ggio.ReadCloser     // IO reader
	laddr  *net.TCPAddr        // local ip address
	raddr  *net.TCPAddr        // remote ip address
	dir    int                 // direction: outbound(+1) or inbound(-1)
	snid   config.SubNetworkID // sub network identity

	networkType    int              // network type
	priKey         ecdsa.PrivateKey // local node private key
	localNode      config.Node      // local "node" information
	localProtoNum  uint32           // local protocol number
	localProtocols []Protocol       // local protocol table

	node        config.Node        // peer "node" information
	protoNum    uint32             // peer protocol number
	protocols   []Protocol         // peer protocol table
	maxPkgSize  int                // max size of tcpmsg package
	ppTid       int                // pingpong timer identity
	rxChan      chan *P2pPackageRx // rx pending channel
	txChan      chan *P2pPackage   // tx pending channel
	ppChan      chan *P2pPackage   // ping channel
	txPendNum   int                // tx pending number
	txSeq       int64              // statistics sequence number
	txOkCnt     int64              // tx ok counter
	txFailedCnt int64              // tx failed counter
	rxDone      chan PeMgrErrno    // RX chan
	rxtxRuning  bool               // indicating that rx and tx routines are running
	ppSeq       uint64             // pingpong sequence no.
	ppCnt       int                // pingpong counter
	rxEno       PeMgrErrno         // rx errno
	txEno       PeMgrErrno         // tx errno
	ppEno       PeMgrErrno         // pingpong errno
	rxDiscard   int64              // number of rx messages discarded
	rxOkCnt     int64              // number of rx messages accepted
}

var peerInstDefault = PeerInstance{
	name:       peInstTaskName,
	state:      peInstStateNull,
	cto:        0,
	hto:        0,
	ato:        0,
	dir:        PeInstDirNull,
	node:       config.Node{},
	maxPkgSize: maxTcpmsgSize,
	protoNum:   0,
	protocols:  []Protocol{{}},
	ppTid:      sch.SchInvalidTid,
	ppSeq:      0,
	ppCnt:      0,
	rxEno:      PeMgrEnoNone,
	txEno:      PeMgrEnoNone,
	ppEno:      PeMgrEnoNone,
}

type msgConnOutRsp struct {
	result PeMgrErrno          // result of outbound connect action
	snid   config.SubNetworkID // sub network identity
	peNode *config.Node        // target node
	ptn    interface{}         // pointer to task instance node of sender
}

type msgHandshakeRsp struct {
	result PeMgrErrno          // result of handshake action
	dir    int                 // inbound or outbound
	snid   config.SubNetworkID // sub network identity
	peNode *config.Node        // target node
	ptn    interface{}         // pointer to task instance node of sender
}

type msgPingpongRsp struct {
	result PeMgrErrno   // result of pingpong action
	dir    int          // direction
	peNode *config.Node // target node
	ptn    interface{}  // pointer to task instance node of sender
}

type MsgCloseCfm struct {
	result PeMgrErrno          // result of pingpong action
	dir    int                 // direction
	snid   config.SubNetworkID // sub network identity
	peNode *config.Node        // target node
	ptn    interface{}         // pointer to task instance node of sender
	name   string              // instance name
	state  peerInstState       // instance state while it's terminated
}

type MsgCloseInd struct {
	cause  PeMgrErrno          // tell why it's closed
	dir    int                 // direction
	snid   config.SubNetworkID // sub network identity
	peNode *config.Node        // target node
	ptn    interface{}         // pointer to task instance node of sender
	name   string              // instance task name
}

type MsgPingpongReq struct {
	seq uint64 // init sequence no.
}

func (pi *PeerInstance) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return pi.tep(ptn, msg)
}

func (pi *PeerInstance) peerInstProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Debugf("peerInstProc: inst: %s, msg.Id: %d", pi.name, msg.Id)

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
		log.Debugf("PeerInstProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	log.Debugf("peerInstProc: get out, inst: %s, msg.Id: %d", pi.name, msg.Id)

	if eno != PeMgrEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (pi *PeerInstance) piPoweroff(ptn interface{}) PeMgrErrno {

	if pi.state < peInstStateNull {
		log.Debugf("piPoweroff: already in killing or killed, inst: %s, snid: %x, dir: %d, state: %d",
			pi.name, pi.snid, pi.dir, pi.state)
		if pi.sdl.SchTaskDone(pi.ptnMe, pi.name, sch.SchEnoKilled) != sch.SchEnoNone {
			return PeMgrEnoScheduler
		}
		return PeMgrEnoNone
	}

	log.Debugf("piPoweroff: task will be done, inst: %s, snid: %x, dir: %d, state: %d",
		pi.name, pi.snid, pi.dir, pi.state)

	pi.stopRxTx()

	if pi.sdl.SchTaskDone(pi.ptnMe, pi.name, sch.SchEnoKilled) != sch.SchEnoNone {
		return PeMgrEnoScheduler
	}
	return PeMgrEnoNone
}

func (pi *PeerInstance) piConnOutReq(_ interface{}) PeMgrErrno {
	if pi.dialer == nil ||
		pi.dir != PeInstDirOutbound ||
		pi.state != peInstStateConnOut {
		log.Debugf("piConnOutReq: instance mismatched, pi: %s", pi.name)
		return PeMgrEnoInternal
	}

	var (
		addr          = &net.TCPAddr{IP: pi.node.IP, Port: int(pi.node.TCP)}
		conn net.Conn = nil
		err  error
		eno  PeMgrErrno = PeMgrEnoNone
	)

	log.Debugf("piConnOutReq: outbound inst: %s, snid: %x, try to dial target: %s",
		pi.name, pi.snid, addr.String())

	pi.dialer.Timeout = pi.cto
	if conn, err = pi.dialer.Dial("tcp", addr.String()); err != nil {
		log.Debugf("piConnOutReq: dial failed, local: %s, to: %s, err: %s",
			fmt.Sprintf("%s:%d", pi.node.IP.String(), pi.node.TCP),
			addr.String(), err.Error())
		eno = PeMgrEnoOs
	} else {
		pi.conn = conn
		pi.laddr = conn.LocalAddr().(*net.TCPAddr)
		pi.raddr = conn.RemoteAddr().(*net.TCPAddr)
		pi.state = peInstStateConnected

		log.Debugf("piConnOutReq: dial ok, laddr: %s, raddr: %s",
			pi.laddr.String(),
			pi.raddr.String())
	}

	rsp := msgConnOutRsp{
		result: eno,
		snid:   pi.snid,
		peNode: &pi.node,
		ptn:    pi.ptnMe,
	}
	schMsg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeConnOutRsp, &rsp)
	pi.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *PeerInstance) piHandshakeReq(_ interface{}) PeMgrErrno {
	var eno PeMgrErrno
	if pi == nil {
		log.Debugf("piHandshakeReq: invalid instance")
		return PeMgrEnoInternal
	}

	log.Debugf("piHandshakeReq: inst: %s, dir: %d", pi.name, pi.dir)

	if pi.state != peInstStateConnected && pi.state != peInstStateAccepted {
		log.Debugf("piHandshakeReq: instance mismatched, inst: %s, dir: %d, state: %d",
			pi.name, pi.dir, pi.state)
		return PeMgrEnoInternal
	}

	if pi.conn == nil {
		log.Debugf("piHandshakeReq: invalid connection, inst: %s, dir: %d", pi.name, pi.dir)
		return PeMgrEnoInternal
	}

	if pi.dir == PeInstDirInbound {
		eno = pi.piHandshakeInbound(pi)
	} else if pi.dir == PeInstDirOutbound {
		eno = pi.piHandshakeOutbound(pi)
	} else {
		log.Debugf("piHandshakeReq: invalid instance direction: %d", pi.dir)
		return PeMgrEnoInternal
	}

	log.Debugf("piHandshakeReq: inst: %s, snid: %x, dir: %d, result: %d, laddr: %s, raddr: %s",
		pi.name,
		pi.snid,
		pi.dir,
		eno,
		pi.laddr.String(),
		pi.raddr.String())

	rsp := msgHandshakeRsp{
		result: eno,
		dir:    pi.dir,
		snid:   pi.snid,
		peNode: &pi.node,
		ptn:    pi.ptnMe,
	}
	if eno != PeMgrEnoNone {
		pi.state = peInstStateHandshakeFailed
		if pi.dir == PeInstDirInbound {
			rsp.peNode = nil
			rsp.snid = config.AnySubNet
		}
	} else {
		pi.state = peInstStateHandshook
	}
	msg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeHandshakeRsp, &rsp)
	pi.sdl.SchSendMessage(&msg)
	return eno
}

func (pi *PeerInstance) piPingpongReq(msg interface{}) PeMgrErrno {
	// notice: here in this function we must check the queue(channel) for pingpong
	// data to avoid be blocked.
	if pi.ppEno != PeMgrEnoNone || pi.state != peInstStateActivated {
		log.Debugf("piPingpongReq: discarded, inst: %s, ppEno: %d, state: %d",
			pi.name, pi.ppEno, pi.state)
		return PeMgrEnoResource
	}
	if pi.conn == nil {
		log.Debugf("piPingpongReq: connection had been closed, inst: %s", pi.name)
		return PeMgrEnoResource
	}
	if len(pi.ppChan) >= cap(pi.ppChan) {
		log.Debugf("piPingpongReq: queue full, inst: %s, dir: %d", pi.name, pi.dir)
		return PeMgrEnoResource
	}
	pi.ppSeq = msg.(*MsgPingpongReq).seq
	ping := Pingpong{
		Seq:   pi.ppSeq,
		Extra: nil,
	}
	upkg := new(P2pPackage)
	if eno := upkg.ping(pi, &ping, false); eno != PeMgrEnoNone {
		log.Debugf("piPingpongReq: ping failed, inst: %s, eno: %d", pi.name, eno)
		return eno
	}
	pi.ppChan <- upkg
	return PeMgrEnoNone
}

func (pi *PeerInstance) piCloseReq(_ interface{}) PeMgrErrno {
	if pi.state < peInstStateNull {
		log.Debugf("piCloseReq: already in killing, inst: %s, snid: %x, dir: %d, state: %d",
			pi.name, pi.snid, pi.dir, pi.state)
		return PeMgrEnoDuplicated
	}

	log.Debugf("piCloseReq: inst: %s, snid: %x, dir: %d, state: %d",
		pi.name, pi.snid, pi.dir, pi.state)

	pi.state = peInstStateKilling
	node := pi.node

	pi.stopRxTx()

	cfm := MsgCloseCfm{
		result: PeMgrEnoNone,
		dir:    pi.dir,
		snid:   pi.snid,
		peNode: &node,
		ptn:    pi.ptnMe,
		name:   pi.name,
		state:  pi.state,
	}
	schMsg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&schMsg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseCfm, &cfm)
	pi.sdl.SchSendMessage(&schMsg)

	return PeMgrEnoNone
}

func (pi *PeerInstance) piEstablishedInd(msg interface{}) PeMgrErrno {
	var schEno sch.SchErrno
	var tid int
	var tmDesc = sch.TimerDescription{
		Name:  sch.PeerMgrName + "_PePingpong",
		Utid:  sch.PePingpongTimerId,
		Tmt:   sch.SchTmTypePeriod,
		Dur:   PeInstPingpongCycle,
		Extra: nil,
	}
	cfmCh := *msg.(*chan int)

	log.Debugf("piEstablishedInd: enter, inst: %s, snid: %x, dir: %d, state: %d",
		pi.name, pi.snid, pi.dir, pi.state)

	if schEno, tid = pi.sdl.SchSetTimer(pi.ptnMe, &tmDesc); schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {
		log.Debugf("piEstablishedInd: SchSetTimer failed, pi: %s, eno: %d", pi.name, schEno)
		cfmCh <- PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	pi.ppTid = tid
	pi.txEno = PeMgrEnoNone
	pi.rxEno = PeMgrEnoNone
	pi.ppEno = PeMgrEnoNone

	if err := pi.conn.SetDeadline(time.Time{}); err != nil {
		log.Debugf("piEstablishedInd: SetDeadline failed, inst: %s, snid: %x, dir: %d,  ip: %s",
			pi.name, pi.snid, pi.dir, pi.node.IP.String())
		pi.sdl.SchKillTimer(pi.ptnMe, pi.ppTid)
		pi.ppTid = sch.SchInvalidTid
		cfmCh <- PeMgrEnoOs
		return PeMgrEnoOs
	}

	go piTx(pi)
	go piRx(pi)

	pi.state = peInstStateActivated
	pi.rxtxRuning = true
	cfmCh <- PeMgrEnoNone

	log.Debugf("piEstablishedInd: exit, inst: %s, snid: %x, dir: %d, state: %d",
		pi.name, pi.snid, pi.dir, pi.state)

	return PeMgrEnoNone
}

func (pi *PeerInstance) piPingpongTimerHandler() PeMgrErrno {
	if pi.ppCnt++; pi.ppCnt > PeInstMaxPingpongCnt {

		log.Debugf("piPingpongTimerHandler: send EvPeCloseReq, inst: %s, snid: %x, dir: %d,  ip: %s",
			pi.name, pi.snid, pi.dir, pi.node.IP.String())

		pi.ppEno = PeMgrEnoPingpongTh
		why := sch.PEC_FOR_PINGPONG
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
		return pi.ppEno
	}
	pr := MsgPingpongReq{
		seq: uint64(time.Now().UnixNano()),
	}
	msg := sch.SchMessage{}
	pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMe, sch.EvPePingpongReq, &pr)
	pi.sdl.SchSendMessage(&msg)
	return PeMgrEnoNone
}

func (pi *PeerInstance) piTxDataReq(_ interface{}) PeMgrErrno {
	// not applied
	return PeMgrEnoMismatched
}

func (pi *PeerInstance) piRxDataInd(msg interface{}) PeMgrErrno {
	return pi.piP2pPkgProc(msg.(*P2pPackage))
}

func (pi *PeerInstance) checkHandshakeInfo(hs *Handshake) bool {
	pass := false
	if pi.peMgr.dynamicSubNetIdExist(&hs.Snid) {
		pass = true
	} else if pi.peMgr.staticSubNetIdExist(&hs.Snid) {
		for _, sn := range pi.peMgr.cfg.staticNodes {
			if bytes.Compare(sn.ID[0:], hs.NodeId[0:]) == 0 {
				pass = true
				break
			}
		}
	}
	return pass
}

func (pi *PeerInstance) piHandshakeInbound(inst *PeerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs *Handshake

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		log.Debugf("piHandshakeInbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}

	log.Debugf("piHandshakeInbound: snid: %x, peer: %s, hs: %+v",
		hs.Snid, hs.IP.String(), *hs)

	if pi.checkHandshakeInfo(hs) != true {
		log.Debugf("piHandshakeInbound: checkHandshakeInfo failed, snid: %x, peer: %s, hs: %+v",
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
	hs2peer.ChainId = inst.chainId
	hs2peer.Snid = inst.snid
	hs2peer.NodeId = inst.localNode.ID
	hs2peer.IP = append(hs2peer.IP, inst.localNode.IP...)
	hs2peer.UDP = uint32(inst.localNode.UDP)
	hs2peer.TCP = uint32(inst.localNode.TCP)
	hs2peer.ProtoNum = inst.localProtoNum
	hs2peer.Protocols = inst.localProtocols

	if eno = pkg.putHandshakeOutbound(inst, &hs2peer); eno != PeMgrEnoNone {
		log.Debugf("piHandshakeInbound: write outbound Handshake message failed, eno: %d", eno)
		return eno
	}

	return PeMgrEnoNone
}

func (pi *PeerInstance) piHandshakeOutbound(inst *PeerInstance) PeMgrErrno {
	var eno PeMgrErrno = PeMgrEnoNone
	var pkg = new(P2pPackage)
	var hs = new(Handshake)

	// write outbound handshake to remote peer
	hs.ChainId = inst.chainId
	hs.Snid = pi.snid
	hs.NodeId = pi.localNode.ID
	hs.IP = append(hs.IP, pi.localNode.IP...)
	hs.UDP = uint32(pi.localNode.UDP)
	hs.TCP = uint32(pi.localNode.TCP)
	hs.ProtoNum = pi.localProtoNum
	hs.Protocols = append(hs.Protocols, pi.localProtocols...)

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {
		log.Debugf("piHandshakeOutbound: write outbound Handshake message failed, eno: %d", eno)
		return eno
	}

	// read inbound handshake from remote peer
	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {
		log.Debugf("piHandshakeOutbound: read inbound Handshake message failed, eno: %d", eno)
		return eno
	}

	// check handshake
	if pi.checkHandshakeInfo(hs) != true {
		log.Debugf("piHandshakeOutbound: checkHandshakeInfo failed, snid: %x, peer: %s, hs: %+v",
			hs.Snid, hs.IP.String(), *hs)
		return PeMgrEnoNotfound
	}

	// check sub network identity
	if hs.Snid != inst.snid {
		log.Debugf("piHandshakeOutbound: subnet identity mismathced")
		return PeMgrEnoMessage
	}

	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos.
	if hs.NodeId != inst.node.ID ||
		inst.node.TCP != uint16(hs.TCP) ||
		bytes.Compare(inst.node.IP, hs.IP) != 0 {
		log.Debugf("piHandshakeOutbound: handshake mismathced, ip: %s, port: %d, id: %x",
			hs.IP.String(), hs.TCP, hs.NodeId)
		return PeMgrEnoMessage
	}

	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	return PeMgrEnoNone
}

func SendPackage(pkg *P2pPackage2Peer) PeMgrErrno {
	// this function exported for user to send messages to specific peers, please
	// notice that it plays with the "messaging" based on scheduler. it's not the
	// only method to send messages, since active peers are backup in shell manager
	// of chain, see function ShellManager.broadcastReq for details please.
	if len(pkg.IdList) == 0 {
		log.Debugf("SendPackage: invalid parameter")
		return PeMgrEnoParameter
	}
	pem := pkg.P2pInst.SchGetTaskObject(sch.PeerMgrName)
	if pem == nil {
		log.Debugf("SendPackage: nil peer manager, might be in power off stage")
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
		req := sch.MsgPeDataReq{
			SubNetId: pkg.SubNetId,
			PeerId:   pid,
			Pkg:      _pkg,
		}
		msg := sch.SchMessage{}
		pkg.P2pInst.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeTxDataReq, &req)
		pkg.P2pInst.SchSendMessage(&msg)
	}
	return PeMgrEnoNone
}

func (peMgr *PeerManager) ClosePeer(snid *SubNetworkID, id *PeerId) PeMgrErrno {
	idExOut := PeerIdEx{Id: *id, Dir: PeInstDirOutbound}
	idExIn := PeerIdEx{Id: *id, Dir: PeInstDirInbound}
	idExList := []PeerIdEx{idExOut, idExIn}
	why := sch.PEC_FOR_COMMAND
	for _, idEx := range idExList {
		log.Debugf("ClosePeer: why: %d, snid: %x, dir: %d, id: %x",
			why, *snid, idEx.Dir, idEx.Id)
		var req = sch.MsgPeCloseReq{
			Ptn:  nil,
			Snid: *snid,
			Node: config.Node{
				ID: *id,
			},
			Dir: idEx.Dir,
			Why: why,
		}
		msg := sch.SchMessage{}
		peMgr.sdl.SchMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
		peMgr.sdl.SchSendMessage(&msg)
	}
	return PeMgrEnoNone
}

type txPkgKey struct {
	pid	uint32
	mid	uint32
}
type txStat struct {
	txTryCnt		int64
	txppOkCnt		int64
	txppFailedCnt	int64
	txdatOkCnt		int64
	txdatFailedCnt	int64
	txPkgCnt		map[txPkgKey]int64
}
func piTx(pi *PeerInstance) PeMgrErrno {
	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit for "txChan" closed.

	defer func() {
		if err := recover(); err != nil {
			log.Debugf("piTx: exception raised, wait done...")
		}
	}()

	ask4Close := func(eno PeMgrErrno) {

		// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning of
		// this instance, BUT at this moment, the message queue of peer manager might
		// be FULL, so the instance would be blocked while sending; AND the peer manager
		// might had fired pi.txDone and been blocked by pi.txExit. panic is called
		// for such a overload system, see scheduler please.

		log.Debugf("piTx: ask4Close, send EvPeCloseReq, " +
			"sdl: %s, inst: %s snid: %x, dir: %d, peer: %s",
			pi.sdlName, pi.name, pi.snid, pi.dir, pi.node.IP.String())

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
	}

	var (
		isPP   bool
		isData bool
		okPP   bool
		okData bool
		ppkg   *P2pPackage
		upkg   *P2pPackage
		stat	= txStat {
			txPkgCnt: make(map[txPkgKey]int64, 0),
		}
	)

_txLoop:
	for {

		isPP = false
		isData = false
		okPP = false
		okData = false
		ppkg = nil
		upkg = nil
		txpk := txPkgKey{}

		select {
		case ppkg, okPP = <-pi.ppChan:
			isPP = true
			isData = false
		case upkg, okData = <-pi.txChan:
			isPP = false
			isData = true
		}

		// check if any pendings or done
		stat.txTryCnt++
		if isPP {

			if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			if okPP && ppkg != nil {

				pi.txSeq += 1

				if eno := ppkg.SendPackage(pi); eno == PeMgrEnoNone {

					pi.txOkCnt++
					stat.txppOkCnt++

				} else {

					pi.txFailedCnt++
					stat.txppFailedCnt++
					ask4Close(eno)
				}

				txpk.pid = ppkg.Pid
				txpk.mid = ppkg.Mid
			}

		} else if isData {

			if !okData {
				log.Debugf("piTx: done, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d",
					pi.sdlName, pi.name, pi.snid, pi.dir)
				break _txLoop
			}

			if upkg == nil {
				log.Debugf("piTx: nil package, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d",
					pi.sdlName, pi.name, pi.snid, pi.dir)
				continue
			}

			if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			pi.txPendNum -= 1
			pi.txSeq += 1

			if eno := upkg.SendPackage(pi); eno == PeMgrEnoNone {

				log.Debugf("piTx: SendPackage ok, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, txSeq: %d, " +
					"key: %x",
					pi.sdlName, pi.name, pi.snid, pi.dir, pi.txSeq,
					upkg.Key)

				pi.txOkCnt++
				stat.txdatOkCnt++
			} else {

				log.Debugf("piTx: SendPackage failed, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, txSeq: %d, " +
					"key: %x",
					pi.sdlName, pi.name, pi.snid, pi.dir, pi.txSeq,
					upkg.Key)

				pi.txFailedCnt++
				stat.txdatFailedCnt++
				ask4Close(eno)
			}

			txpk.pid = upkg.Pid
			txpk.mid = upkg.Mid
		}

		if cnt, ok := stat.txPkgCnt[txpk]; ok {
			stat.txPkgCnt[txpk] = cnt + 1
		} else {
			stat.txPkgCnt[txpk] = 1
		}

		if stat.txTryCnt & 0x3ff == 0 {
			log.Debugf("piTx: stat, " +
				"sdl: %s, inst: %s, snid: %x, dir: %d, txSeq: %d, txOkCnt: %d, txFailedCnt: %d " +
				"stat: %+v",
				pi.sdlName, pi.name, pi.snid, pi.dir, pi.txSeq, pi.txOkCnt, pi.txFailedCnt,
				stat)
		}
	}

	log.Debugf("piTx: exit, " +
		"sdl: %s, inst: %s, snid: %x, dir: %d",
		pi.sdlName, pi.name, pi.snid, pi.dir)

	return PeMgrEnoNone
}

type rxPkgKey = txPkgKey
type rxStat struct {
	rxTryCnt		int64
	rxTmpErrCnt		int64
	rxErrCnt		int64
	rxOkCnt			int64
	rxDiscardCnt	int64
	rxPidErrCnt		int64
	rxChCnt			int64
	rxPkgCnt		map[rxPkgKey]int64
}
func piRx(pi *PeerInstance) PeMgrErrno {

	// This function is "go" when an instance of peer is activated to work,
	// inbound or outbound. When user try to close the peer, this routine
	// would then exit.

	defer func() {
		if err := recover(); err != nil {
			log.Debugf("piRx: exception raised, wait done...")
		}
	}()

	var done PeMgrErrno = PeMgrEnoNone
	var ok = true
	var stat = rxStat{
		rxPkgCnt: make(map[rxPkgKey]int64, 0),
	}

_rxLoop:
	for {
		select {
		case done, ok = <-pi.rxDone:
			log.Debugf("piRx: done, " +
				"sdl: %s, inst: %s, snid: %x, dir: %d, done with: %d",
				pi.sdlName, pi.name, pi.snid, pi.dir, done)
			if ok {
				close(pi.rxDone)
			}
			break _rxLoop
		default:
		}

		// if in errors, sleep then continue to check done
		if pi.rxEno != PeMgrEnoNone || pi.txEno != PeMgrEnoNone {
			time.Sleep(time.Microsecond * 100)
			continue
		}

		// try reading the peer
		if stat.rxTryCnt++; stat.rxTryCnt & 0x3ff == 0 {
			log.Debugf("piRx: stat, " +
				"sdl: %s, inst: %s, snid: %x, dir: %d, ip: %s, stat: %+v",
				pi.sdlName, pi.name, pi.snid, pi.dir, pi.node.IP.String(), stat)
		}

		upkg := new(P2pPackage)
		if eno := upkg.RecvPackage(pi); eno != PeMgrEnoNone {

			if eno == PeMgrEnoNetTemporary {

				log.Debugf("piRx: PeMgrEnoNetTemporary, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, ip: %s",
					pi.sdlName, pi.name, pi.snid, pi.dir, pi.node.IP.String())
				stat.rxTmpErrCnt++

			} else {
				// 1) if failed, ask the user to done, so he can close this peer seems in troubles,
				// and we will be done then;
				// 2) it is possible that, while we are blocked here in reading and the connection
				// is closed for some reasons(for example the user close the peer), in this case,
				// we would get an error;

				log.Debugf("piRx: error, send EvPeCloseReq, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, ip: %s",
					pi.sdlName, pi.name, pi.snid, pi.dir, pi.node.IP.String())

				why := sch.PEC_FOR_RXERROR
				pi.rxEno = eno
				req := sch.MsgPeCloseReq{
					Ptn:  pi.ptnMe,
					Snid: pi.snid,
					Node: pi.node,
					Dir:  pi.dir,
					Why:  why,
				}

				// Here we try to send EvPeCloseReq event to peer manager to ask for cleaning
				// this instance, BUT at this moment, the message queue of peer manager might
				// be FULL, so the instance would be blocked while sending; AND the peer manager
				// might had fired pi.txDone and been blocked by pi.txExit. panic is called
				// for such a overload system, see scheduler please.

				msg := sch.SchMessage{}
				pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMgr, sch.EvPeCloseReq, &req)
				pi.sdl.SchSendMessage(&msg)
				stat.rxErrCnt++
			}

			continue
		}

		upkg.DebugPeerPackage()

		rxpk := rxPkgKey{
			pid: upkg.Pid,
			mid: upkg.Mid,
		}
		stat.rxOkCnt++

		if upkg.Pid == uint32(PID_P2P) {

			msg := sch.SchMessage{}
			pi.sdl.SchMakeMessage(&msg, pi.ptnMe, pi.ptnMe, sch.EvPeRxDataInd, upkg)
			pi.sdl.SchSendMessage(&msg)

			if cnt, ok := stat.rxPkgCnt[rxpk]; ok {
				stat.rxPkgCnt[rxpk] = cnt + 1
			} else {
				stat.rxPkgCnt[rxpk] = 1
			}

		} else if upkg.Pid == uint32(PID_EXT) {

			if len(pi.rxChan) >= cap(pi.rxChan) {

				log.Debugf("piRx: queue full, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, key: %x",
					pi.sdlName, pi.name, pi.snid, pi.dir, upkg.Key)

				if pi.rxDiscard += 1; pi.rxDiscard & 0x1f == 0 {
					log.Debugf("piRx: stat, discarded, " +
						"sdl: %s, inst: %s, snid: %x, dir: %d, rxDiscard: %d, key: %x",
						pi.sdlName, pi.name, pi.snid, pi.dir, pi.rxDiscard, upkg.Key)
				}
				stat.rxDiscardCnt++

			} else {

				log.Debugf("piRx: put into channel, " +
					"sdl: %s, inst: %s, snid: %x, dir: %d, key: %x",
					pi.sdlName, pi.name, pi.snid, pi.dir, upkg.Key)

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
					log.Debugf("piRx: stat, " +
						"sdl: %s, inst: %s, snid: %x, dir: %d, rxOkCnt: %d",
						pi.sdlName, pi.name, pi.snid, pi.dir, pi.rxOkCnt)
				}

				stat.rxChCnt++
				if cnt, ok := stat.rxPkgCnt[rxpk]; ok {
					stat.rxPkgCnt[rxpk] = cnt + 1
				} else {
					stat.rxPkgCnt[rxpk] = 1
				}
			}
		} else {
			log.Debugf("piRx: invalid pid, " +
				"sdl: %s, inst: %s, snid: %x, dir: %d,  pid: %d",
				pi.sdlName, pi.name, pi.snid, pi.dir, upkg.Pid)
			stat.rxPidErrCnt++
		}
	}

	log.Debugf("piRx: exit, sdl: %s, inst: %s, snid: %x, dir: %d",
		pi.sdlName, pi.name, pi.snid, pi.dir)
	return done
}

func (pi *PeerInstance) piP2pPkgProc(upkg *P2pPackage) PeMgrErrno {
	if upkg.Pid != uint32(PID_P2P) {
		log.Debugf("piP2pPkgProc: not a p2p package, pid: %d", upkg.Pid)
		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {
		log.Debugf("piP2pPkgProc: invalid payload length: %d", upkg.PayloadLength)
		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {
		log.Debugf("piP2pPkgProc: payload length mismatched, PlLen: %d, real: %d",
			upkg.PayloadLength, len(upkg.Payload))
		return PeMgrEnoMessage
	}

	msg := P2pMessage{}
	if eno := upkg.GetMessage(&msg); eno != PeMgrEnoNone {
		log.Debugf("piP2pPkgProc: GetMessage failed, eno: %d", eno)
		return eno
	}

	// check message identity. we discard any handshake messages received here
	// since handshake procedure had been passed, and dynamic handshake is not
	// supported currently.

	switch msg.Mid {

	case uint32(MID_HANDSHAKE):
		log.Debugf("piP2pPkgProc: MID_HANDSHAKE, discarded")
		return PeMgrEnoMessage

	case uint32(MID_PING):
		return pi.piP2pPingProc(msg.Ping)

	case uint32(MID_PONG):
		return pi.piP2pPongProc(msg.Pong)

	default:
		log.Debugf("piP2pPkgProc: unknown mid: %d", msg.Mid)
		return PeMgrEnoMessage
	}

	return PeMgrEnoUnknown
}

func (pi *PeerInstance) piP2pPingProc(ping *Pingpong) PeMgrErrno {
	// notice: check the queue(channel) for pingpong data to avoid be blocked
	// while sending. this function is running in piRx context, so if it is
	// blocked here, means that piRx is blocked.
	if pi.ppEno != PeMgrEnoNone || pi.state != peInstStateActivated {
		log.Debugf("piP2pPingProc: discarded, inst: %s, ppEno: %d, state: %d",
			pi.name, pi.ppEno, pi.state)
		return PeMgrEnoResource
	}
	if pi.conn == nil {
		log.Debugf("piP2pPingProc: connection had been closed, inst: %s", pi.name)
		return PeMgrEnoResource
	}
	if len(pi.ppChan) >= cap(pi.ppChan) {
		log.Debugf("piP2pPingProc: queue full, inst: %s, dir: %d", pi.name, pi.dir)
		return PeMgrEnoResource
	}
	pong := Pingpong{
		Seq:   ping.Seq,
		Extra: nil,
	}
	pi.ppCnt = 0
	upkg := new(P2pPackage)
	if eno := upkg.pong(pi, &pong, false); eno != PeMgrEnoNone {
		log.Debugf("piP2pPingProc: pong failed, eno: %d, pi: %s",
			eno, fmt.Sprintf("%+v", *pi))
		return eno
	}
	pi.ppChan <- upkg
	return PeMgrEnoNone
}

func (pi *PeerInstance) piP2pPongProc(pong *Pingpong) PeMgrErrno {
	// Currently, the heartbeat checking does not apply pong messages from
	// peer, instead, a counter for ping messages and a timer are invoked,
	// see it pls.
	return PeMgrEnoNone
}

func (pis peerInstState) compare(s peerInstState) int {
	return int(pis - s)
}

func (peMgr *PeerManager) updateStaticStatus(snid SubNetworkID, idEx PeerIdEx, status int) {
	if snid == peMgr.cfg.staticSubNetId {
		if _, static := peMgr.staticsStatus[idEx]; static == true {
			peMgr.staticsStatus[idEx] = status
		}
	}
}

func (peMgr *PeerManager) dynamicSubNetIdExist(snid *SubNetworkID) bool {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		_, ok := peMgr.cfg.subNetNodeList[*snid]
		return ok
	}
	return false
}

func (peMgr *PeerManager) staticSubNetIdExist(snid *SubNetworkID) bool {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if peMgr.cfg.networkType == config.P2pNetworkTypeStatic {
		return peMgr.cfg.staticSubNetId == *snid
	} else if peMgr.cfg.networkType == config.P2pNetworkTypeDynamic {
		return len(peMgr.cfg.staticNodes) > 0 && peMgr.cfg.staticSubNetId == *snid
	}
	return false
}

func (peMgr *PeerManager) setHandshakeParameters(inst *PeerInstance, snid config.SubNetworkID) {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	inst.networkType = peMgr.cfg.networkType
	inst.priKey = peMgr.cfg.subNetKeyList[snid]
	inst.localNode = peMgr.cfg.subNetNodeList[snid]
	inst.localProtoNum = peMgr.cfg.protoNum
	inst.localProtocols = peMgr.cfg.protocols
}

func (peMgr *PeerManager) GetLocalSubnetInfo() ([]config.SubNetworkID, map[config.SubNetworkID]config.Node) {
	peMgr.lock.Lock()
	defer peMgr.lock.Unlock()
	if !peMgr.isInited {
		return nil, nil
	}
	return peMgr.cfg.subNetIdList, peMgr.cfg.subNetNodeList
}

func (peMgr *PeerManager) isStaticSubNetId(snid SubNetworkID) bool {
	return (peMgr.cfg.networkType == config.P2pNetworkTypeStatic &&
		peMgr.staticSubNetIdExist(&snid) == true) ||
		(peMgr.cfg.networkType == config.P2pNetworkTypeDynamic &&
			peMgr.staticSubNetIdExist(&snid) == true)
}

func (peMgr *PeerManager) getWorkerInst(snid SubNetworkID, idEx *PeerIdEx) *PeerInstance {
	return peMgr.workers[snid][*idEx]
}

func (peMgr *PeerManager) GetInstIndChannel() chan interface{} {
	// This function implements the "Channel" schema to hand up the indications
	// from peer instances to higher module. After this function called, the caller
	// can then go a routine to pull indications from the channel returned.
	return peMgr.indChan
}

func (peMgr *PeerManager) RegisterInstIndCallback(cb interface{}, userData interface{}) PeMgrErrno {
	// This function implements the "Callback" schema to hand up the indications
	// from peer instances to higher module. In this schema, a routine is started
	// in this function to pull indications, check what indication type it is and
	// call the function registered.
	if peMgr.ptnShell != nil {
		log.Debugf("RegisterInstIndCallback: register failed for shell task in running")
		return PeMgrEnoMismatched
	}
	if peMgr.indCb != nil {
		log.Debugf("RegisterInstIndCallback: callback duplicated")
		return PeMgrEnoDuplicated
	}
	if cb == nil {
		log.Debugf("RegisterInstIndCallback: try to register nil callback")
		return PeMgrEnoParameter
	}
	icb, ok := cb.(P2pIndCallback)
	if !ok {
		log.Debugf("RegisterInstIndCallback: invalid callback interface")
		return PeMgrEnoParameter
	}

	peMgr.indCb = icb
	peMgr.indCbUserData = userData

	go func() {
		for {
			select {
			case ind, ok := <-peMgr.indChan:
				if !ok {
					log.Debugf("P2pIndCallback: indication channel closed, done")
					return
				}
				indType := reflect.TypeOf(ind).Elem().Name()
				switch indType {
				case "P2pIndPeerActivatedPara":
					peMgr.indCb(P2pIndPeerActivated, ind, peMgr.indCbUserData)
				case "P2pIndPeerClosedPara":
					peMgr.indCb(P2pIndPeerClosed, ind, peMgr.indCbUserData)
				default:
					log.Debugf("P2pIndCallback: discard unknown indication type: %s", indType)
				}
			}
		}
	}()

	return PeMgrEnoNone
}

func (pi *PeerInstance) stopRxTx() {
	cleanIo := func() {
		if pi.ior != nil {
			pi.ior.Close()
		}
		if pi.iow != nil {
			pi.iow.Close()
		}
		pi.conn.Close()
		pi.conn = nil
	}
	cleanCh := func() {
		pi.rxtxRuning = false
		if pi.conn != nil {
			cleanIo()
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

	if pi.rxtxRuning {
		cleanCh()
	} else if pi.conn != nil {
		cleanIo()
	}
	pi.state = peInstStateKilled
}
