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

package table

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/yeeco/gyee/log"
	config "github.com/yeeco/gyee/p2p/config"
	um "github.com/yeeco/gyee/p2p/discover/udpmsg"
	nat "github.com/yeeco/gyee/p2p/nat"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


//
// errno
//
const (
	TabMgrEnoNone = iota
	TabMgrEnoConfig
	TabMgrEnoParameter
	TabMgrEnoScheduler
	TabMgrEnoDatabase
	TabMgrEnoNotFound
	TabMgrEnoDuplicated
	TabMgrEnoMismatched
	TabMgrEnoInternal
	TabMgrEnoFindNodeFailed
	TabMgrEnoPingpongFailed
	TabMgrEnoTimeout
	TabMgrEnoUdp
	TabMgrEnoResource
	TabMgrEnoRemove
	TabMgrEnoBootstrap
)

type TabMgrErrno int

//
// Hash type
//
const HashLength = 32           // 32 bytes(256 bits) hash applied
const HashBits = HashLength * 8 // bits number of hash
type Hash [HashLength]byte

//
// Some constants about database(levelDb)
//
const (
	ndbVersion = 1
)

//
// Some contants about NAT
//
const (
	natMapKeepTime    = time.Minute * 20
	natMapRefreshTime = natMapKeepTime - time.Minute*5
)

//
// Some constants about buckets, timers, ...
//
const (
	switch2Root         = true         // switch to root
	nodeId2SubnetId     = true         // subnet identity is masked from node identity
	bucketSize          = 256          // max nodes can be held for one bucket
	nBuckets            = HashBits + 1 // total number of buckets
	maxBonding          = 16           // max concurrency bondings
	maxFindnodeFailures = 3            // max FindNode failures to remove a node

	//
	// Since a bootstrap node would not dial outside, one could set a small value for the
	// following auto refresh timer cycle for it.
	//

	autoRefreshCycle    = 1 * time.Hour         // period to auto refresh
	autoBsnRefreshCycle = autoRefreshCycle / 60 // one minute

	findNodeMinInterval = 4 * time.Second    // min interval for two queries to same node
	findNodeExpiration  = 8 * time.Second    // should be (NgbProtoFindNodeResponseTimeout + delta)
	pingpongExpiration  = 8 * time.Second    // should be (NgbProtoPingResponseTimeout + delta)
	seedMaxCount        = 32                 // wanted number of seeds
	seedMaxAge          = 1 * 24 * time.Hour // max age can seeds be
	nodeReboundDuration = 1 * time.Minute    // duration for a node to be rebound
	nodeAutoCleanCycle  = time.Hour          // Time period for running the expiration task.
)

//
// Bucket entry
//
type NodeID = config.NodeID
type NodeIdEx [config.NodeIDBytes + config.SubNetIdBytes]byte

type Node struct {
	config.Node      // our Node type
	sha         Hash // hash from node identity
}

type bucketEntry struct {
	addTime     time.Time // time when node added
	lastQuery   time.Time // time when node latest queryed
	lastPing    time.Time // time when node latest pinged
	lastPong    time.Time // time when node pong latest received
	failCount   int       // fail to response find node request counter
	config.Node           // node
	sha         Hash      // hash of id
}

//
// bucket type
//
type bucket struct {
	nodes []*bucketEntry // node table for a bucket
}

//
// Table task configuration
//
const (
	p2pTypeDynamic = 0 // neighbor discovering needed
	p2pTypeStatic  = 1 // no discovering
)

type tabConfig struct {
	local          config.Node                  // local node identity
	networkType    int                          // p2p network type
	bootstrapNodes []*Node                      // bootstrap nodes
	dataDir        string                       // data directory
	name           string                       // node name
	nodeDb         string                       // node database
	noHistory      bool                         // no history node database
	bootstrapNode  bool                         // bootstrap flag of local node
	snidMaskBits   int                          // mask bits for subnet identity
	subNetNodeList map[SubNetworkID]config.Node // sub network node identities
	subNetIdList   []SubNetworkID               // sub network identity list. do not put the identity
	// of the local node in this list.
}

//
// Instance control block
//
const (
	TabInstStateNull     = iota // null instance state
	TabInstStateQuering         // FindNode sent but had not been responsed yet
	TabInstStateBonding         // Ping sent but hand not been responsed yet
	TabInstStateQTimeout        // query timeout
	TabInstStateBTimeout        // bound timeout
)

const (
	TabInstQPendingMax = 16  // max nodes in pending for quering
	TabInstBPendingMax = 128 // max nodes in pending for bounding
	TabInstQueringMax  = 8   // max concurrency quering instances
	TabInstBondingMax  = 64  // max concurrency bonding instances
)

type instCtrlBlock struct {
	snid  SubNetworkID // sub network identity
	state int          // instance state, see aboved consts about state pls
	req   interface{}  // request message pointer which inited this instance
	rsp   interface{}  // pointer to response message received
	tid   int          // identity of timer for response
	qrt   time.Time    // findnode sent time
	pit   time.Time    // ping sent time
	pot   time.Time    // pong received time
}

//
// FindNode pending item
//
type queryPendingEntry struct {
	node   *Node   // peer node to be queried
	target *NodeID // target looking for
}

//
// Table manager
//
const TabMgrName = sch.TabMgrName

type SubNetworkID = config.SubNetworkID

var ZeroSubNet = config.ZeroSubNet
var AnySubNet = config.AnySubNet

type TableManager struct {
	lock         *sync.Mutex          // lock for sync
	sdl          *sch.Scheduler       // scheduler pointer
	name         string               // name
	tep          sch.SchUserTaskEp    // entry
	cfg          tabConfig            // configuration
	root         *TableManager        // root manager pointer
	ptnMe        interface{}          // pointer to task node of myself
	ptnNgbMgr    interface{}          // pointer to neighbor manager task node
	ptnDcvMgr    interface{}          // pointer to discover manager task node
	ptnNatMgr    interface{}          // pointer to nat manager task node
	ptnPeerMgr   interface{}          // pointer to peer manager task node
	shaLocal     Hash                 // hash of local node identity
	buckets      [nBuckets]*bucket    // buckets
	queryIcb     []*instCtrlBlock     // active query instance table
	boundIcb     []*instCtrlBlock     // active bound instance table
	queryPending []*queryPendingEntry // pending to be queried
	boundPending []*Node              // pending to be bound
	dlkTab       []int                // log2 distance lookup table for a xor byte
	refreshing   bool                 // busy in refreshing now
	dataDir      string               // data directory
	arfTid       int                  // auto refresh timer identity

	//
	// Notice: currently Ethereum's database interface is introduced
	//see file nodedb.go for details please.
	//
	nodeDb *nodeDB // node database object pointer

	//
	// Notice: one node can attach to multiple sub networks, and we allocate
	// a table manager for each sub network. when network type is specified
	// as dynamic but no specific sub network identities provided, a table
	// manager with identity as AnySubNet would be allocated. There is always
	// one base table manager act as a task for dispatching messages to real
	// sub network table managers in subNetMgrList, and sending messages for
	// them when necessary.
	//
	networkType   int                            // network type
	snid          SubNetworkID                   // sub network identity
	subNetMgrList map[SubNetworkID]*TableManager // sub network manager

	//
	// public address from nat manager
	//
	natUdpResult bool   // result about nap mapping for udp
	pubUdpIp     net.IP // public ip from nat to be announced for udp
	pubUdpPort   int    // public port form nat to be announced for udp

	natTcpResult bool   // result about nap mapping for tcp
	pubTcpIp     net.IP // should be same as pubUdpIp
	pubTcpPort   int    // public port form nat to be announced for tcp
}

func NewTabMgr() *TableManager {
	tabMgr := newTabMgrWithoutLock()
	tabMgr.lock = new(sync.Mutex)
	tabMgr.tep = tabMgr.tabMgrProc
	return tabMgr
}

func (tabMgr *TableManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return tabMgr.tep(ptn, msg)
}

func (tabMgr *TableManager) tabMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Tracef("tabMgrProc: name: %s, msg.Id: %d", tabMgr.name, msg.Id)

	var eno TabMgrErrno = TabMgrEnoNone

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = tabMgr.tabMgrPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = tabMgr.tabMgrPoweroff(ptn)

	case sch.EvShellReconfigReq:
		eno = tabMgr.shellReconfigReq(msg.Body.(*sch.MsgShellReconfigReq))

	case sch.EvTabRefreshTimer:
		eno = tabMgr.tabMgrRefreshTimerHandler(msg.Body.(*SubNetworkID))

	case sch.EvTabPingpongTimer:
		eno = tabMgr.tabMgrPingpongTimerHandler(msg.Body.(*instCtrlBlock))

	case sch.EvTabFindNodeTimer:
		eno = tabMgr.tabMgrFindNodeTimerHandler(msg.Body.(*instCtrlBlock))

	case sch.EvTabRefreshReq:
		eno = tabMgr.tabMgrRefreshReq(msg.Body.(*sch.MsgTabRefreshReq))

	case sch.EvNblFindNodeRsp:
		eno = tabMgr.tabMgrFindNodeRsp(msg.Body.(*sch.NblFindNodeRsp))

	case sch.EvNblPingpongRsp:
		eno = tabMgr.tabMgrPingpongRsp(msg.Body.(*sch.NblPingRsp))

	case sch.EvNblPingedInd:
		eno = tabMgr.tabMgrPingedInd(msg.Body.(*um.Ping))

	case sch.EvNblPongedInd:
		eno = tabMgr.tabMgrPongedInd(msg.Body.(*um.Pong))

	case sch.EvNblQueriedInd:
		eno = tabMgr.tabMgrQueriedInd(msg.Body.(*um.FindNode))

	case sch.EvNatMgrReadyInd:
		eno = tabMgr.tabMgrNatReadyInd(msg.Body.(*sch.MsgNatMgrReadyInd))

	case sch.EvNatMgrMakeMapRsp:
		eno = tabMgr.tabMgrNatMakeMapRsp(msg.Body.(*sch.MsgNatMgrMakeMapRsp))

	case sch.EvNatMgrPubAddrUpdateInd:
		eno = tabMgr.tabMgrNatPubAddrUpdateInd(msg.Body.(*sch.MsgNatMgrPubAddrUpdateInd))

	default:
		log.Debugf("TabMgrProc: invalid message: %d", msg.Id)
		eno = TabMgrEnoParameter
	}

	log.Tracef("TabMgrProc: get out, name: %s, msg.Id: %d", tabMgr.name, msg.Id)

	if eno != TabMgrEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (tabMgr *TableManager) tabMgrPoweron(ptn interface{}) TabMgrErrno {
	var eno TabMgrErrno = TabMgrEnoNone
	tabMgr.ptnMe = ptn
	tabMgr.sdl = sch.SchGetScheduler(ptn)

	if eno = tabMgr.tabGetConfig(&tabMgr.cfg); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPoweron: tabGetConfig failed, eno: %d", eno)
		return eno
	}

	// if it's a static type, no table manager needed, just done the table
	// manager task and then return. so, in this case, any other must not
	// try to interact with table manager for it is not exist.

	if tabMgr.networkType == p2pTypeStatic {
		log.Debugf("tabMgrPoweron: static type, tabMgr is not needed")
		tabMgr.sdl.SchTaskDone(ptn, tabMgr.name, sch.SchEnoNone)
		return TabMgrEnoNone
	}

	if eno = tabMgr.tabNodeDbPrepare(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPoweron: tabNodeDbPrepare failed, eno: %d", eno)
		return eno
	}

	if eno = tabMgr.tabSetupLocalHashId(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPoweron: tabSetupLocalHash failed, eno: %d", eno)
		return eno
	}

	if eno = tabMgr.tabRelatedTaskPrepare(ptn); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPoweron: tabRelatedTaskPrepare failed, eno: %d", eno)
		return eno
	}

	if eno = tabSetupLog2DistanceLookupTable(tabMgr.dlkTab); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPoweron: tabSetupLog2DistanceLookupTable failed, eno: %d", eno)
		return eno
	}

	// Since the system is just powered on at this moment, we start table
	// refreshing bellow. Before doing this, we update the random seed for
	// the underlying.

	rand.Seed(time.Now().UnixNano())
	tabMgr.refreshing = false

	// setup table manager for AnySubNet type. at this moment, if this type
	// set, the network type must be P2pNetworkTypeDynamic, and none of sub
	// network identities are specified, see function tabGetConfig called
	// aboved for details. if it's not a AnySubNet, then some sub network
	// identities must be provided.

	if tabMgr.snid == config.AnySubNet {

		// we put object tabMgr into the sub network list, this case we get
		// a list with only one item. see comments bellow for more please.

		for loop := 0; loop < cap(tabMgr.buckets); loop++ {
			b := new(bucket)
			tabMgr.buckets[loop] = b
			b.nodes = make([]*bucketEntry, 0, bucketSize)
		}

		tabMgr.subNetMgrList[tabMgr.snid] = tabMgr

	} else if len(tabMgr.cfg.subNetIdList) > 0 {

		// here we must have some specific sub networks, we construct table
		// manager for each one basing on the tabMgr object we had worked on
		// it, see above codes, and then the base tabMgr would not response
		// to specific any sub network, instead, it plays the role as a task
		// scheduled by the scheduler, dispatching messages to those real
		// table managers according to sub network identities, and a task to
		// send messages to other tasks in system.

		if eno = tabMgr.setupSubNetTabMgr(); eno != TabMgrEnoNone {
			log.Debugf("tabMgrPoweron: SetSubNetTabMgr failed, eno: %d", eno)
			return eno
		}
	} else {
		log.Debugf("tabMgrPoweron: configuration mismatched")
		return TabMgrEnoInternal
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) stopSubnetRefresh() TabMgrErrno {
	if tabMgr.arfTid != sch.SchInvalidTid {
		tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, tabMgr.arfTid)
		tabMgr.arfTid = sch.SchInvalidTid
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) startSubnetRefresh(targetId *config.NodeID) TabMgrErrno {
	cycle := autoRefreshCycle
	if tabMgr.cfg.bootstrapNode {
		cycle = autoBsnRefreshCycle
	}

	if eno := tabMgr.tabStartTimer(nil, sch.TabRefreshTimerId, cycle); eno != TabMgrEnoNone {
		log.Debugf("startSubnetRefresh: tabStartTimer failed, eno: %d", eno)
		return eno
	}

	if eno := tabMgr.tabRefresh(&tabMgr.snid, targetId); eno != TabMgrEnoNone {
		log.Debugf("startSubnetRefresh: tabRefresh sub network failed, eno: %d, subnet: %x", eno, tabMgr.snid)
		return eno
	}

	return TabMgrEnoNone
}

func newTabMgrWithoutLock() *TableManager {

	var tabMgr = TableManager{
		lock:          nil,
		name:          TabMgrName,
		cfg:           tabConfig{},
		shaLocal:      Hash{},
		buckets:       [nBuckets]*bucket{},
		queryIcb:      make([]*instCtrlBlock, 0, TabInstQueringMax),
		boundIcb:      make([]*instCtrlBlock, 0, TabInstBondingMax),
		queryPending:  make([]*queryPendingEntry, 0, TabInstQPendingMax),
		boundPending:  make([]*Node, 0, TabInstBPendingMax),
		dlkTab:        make([]int, 256),
		refreshing:    false,
		dataDir:       "",
		arfTid:        sch.SchInvalidTid,
		networkType:   p2pTypeDynamic,
		snid:          AnySubNet,
		subNetMgrList: map[SubNetworkID]*TableManager{},
		natUdpResult:  false,
		pubUdpIp:      net.IPv4zero,
		pubUdpPort:    0,
		natTcpResult:  false,
		pubTcpIp:      net.IPv4zero,
		pubTcpPort:    0,
	}

	tabMgr.tep = tabMgr.tabMgrProc

	return &tabMgr
}

func (tabMgr *TableManager) mgr4Subnet(snid config.SubNetworkID) *TableManager {
	mgr := newTabMgrWithoutLock()
	*mgr = *tabMgr
	mgr.snid = snid
	mgr.cfg.local = tabMgr.cfg.subNetNodeList[snid]
	mgr.queryIcb = make([]*instCtrlBlock, 0, TabInstQueringMax)
	mgr.boundIcb = make([]*instCtrlBlock, 0, TabInstBondingMax)
	mgr.queryPending = make([]*queryPendingEntry, 0, TabInstQPendingMax)
	mgr.boundPending = make([]*Node, 0, TabInstBPendingMax)
	mgr.tabSetupLocalHashId()

	for loop := 0; loop < cap(mgr.buckets); loop++ {
		b := new(bucket)
		mgr.buckets[loop] = b
		b.nodes = make([]*bucketEntry, 0, bucketSize)
	}
	return mgr
}

func (tabMgr *TableManager) setupSubNetTabMgr() TabMgrErrno {
	for _, snid := range tabMgr.cfg.subNetIdList {
		mgr := tabMgr.mgr4Subnet(snid)
		mgr.root = tabMgr
		tabMgr.subNetMgrList[snid] = mgr
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrPoweroff(ptn interface{}) TabMgrErrno {

	log.Debugf("tabMgrPoweroff: task will be done, name: %s", tabMgr.sdl.SchGetTaskName(ptn))

	// close nodeDb and done the task: timers would be killed by scheduler when task done,
	// and one could also chose to kill them himself before done the task.
	if tabMgr.nodeDb != nil {
		tabMgr.nodeDb.close()
		tabMgr.nodeDb = nil
	}
	if tabMgr.sdl.SchTaskDone(ptn, tabMgr.name, sch.SchEnoKilled) != sch.SchEnoNone {
		return TabMgrEnoScheduler
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) shellReconfigReq(msg *sch.MsgShellReconfigReq) TabMgrErrno {
	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()

	delList := msg.SnidDel
	addList := msg.SnidAdd

	for _, del := range delList {
		if mgr, ok := tabMgr.subNetMgrList[del]; ok {
			if mgr.arfTid != sch.SchInvalidTid {
				tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, mgr.arfTid)
				mgr.arfTid = sch.SchInvalidTid
			}
			delete(tabMgr.cfg.subNetNodeList, del)
			delete(tabMgr.subNetMgrList, del)
			log.Debugf("shellReconfigReq: subnet deleted: %x", del)
		}
		for idx, snid := range tabMgr.cfg.subNetIdList {
			if snid == del {
				if idx != len(tabMgr.cfg.subNetIdList)-1 {
					tabMgr.cfg.subNetIdList = append(tabMgr.cfg.subNetIdList[0:idx],
						tabMgr.cfg.subNetIdList[idx+1:]...)
				} else {
					tabMgr.cfg.subNetIdList = tabMgr.cfg.subNetIdList[0:idx]
				}
				break
			}
		}
	}

	log.Debugf("shellReconfigReq: subnet mask bits, old: %d, %d, new: %d",
		tabMgr.cfg.snidMaskBits,
		tabMgr.sdl.SchGetP2pConfig().SnidMaskBits,
		msg.MaskBits)
	tabMgr.cfg.snidMaskBits = msg.MaskBits
	tabMgr.sdl.SchGetP2pConfig().SnidMaskBits = msg.MaskBits

	for _, add := range addList {
		if _, ok := tabMgr.subNetMgrList[add.SubNetId]; ok {
			log.Debugf("shellReconfigReq: duplicated for adding")
			continue
		}
		tabMgr.cfg.subNetNodeList[add.SubNetId] = add.SubNetNode
		tabMgr.cfg.subNetIdList = append(tabMgr.cfg.subNetIdList, add.SubNetId)
		log.Debugf("shellReconfigReq: subnet added: %x", add.SubNetId)
	}

	for _, add := range addList {
		if _, ok := tabMgr.subNetMgrList[add.SubNetId]; ok {
			log.Debugf("shellReconfigReq: duplicated for adding")
			continue
		}
		mgr := tabMgr.mgr4Subnet(add.SubNetId)
		tabMgr.subNetMgrList[add.SubNetId] = mgr
		log.Debugf("shellReconfigReq: subnet manager added: %x", add.SubNetId)

		if eno := mgr.startSubnetRefresh(nil); eno != TabMgrEnoNone {
			log.Debugf("shellReconfigReq: failed, eno: %d, snid: %x", eno, mgr.snid)
			return eno
		}
		log.Debugf("shellReconfigReq: refreshing started for subnet added: %x", add.SubNetId)
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrRefreshTimerHandler(snid *SubNetworkID) TabMgrErrno {
	if mgr, ok := tabMgr.subNetMgrList[*snid]; ok {
		if mgr.arfTid == sch.SchInvalidTid && !mgr.root.natUdpResult {
			log.Debugf("tabMgrRefreshTimerHandler: arfTid: %d, natUdpResult: %t",
				mgr.arfTid, mgr.root.natUdpResult)
			return TabMgrEnoMismatched
		}
		return mgr.tabRefresh(snid, nil)
	}
	log.Debugf("tabMgrRefreshTimerHandler: invalid subnet: %x", snid)
	return TabMgrEnoParameter
}

func (tabMgr *TableManager) tabMgrPingpongTimerHandler(inst *instCtrlBlock) TabMgrErrno {
	mgr, ok := tabMgr.subNetMgrList[inst.snid]
	if !ok {
		log.Debugf("tabMgrPingpongTimerHandler: invalid subnet: %x", inst.snid)
		return TabMgrEnoParameter
	}

	if eno := mgr.tabUpdateBucket(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongTimerHandler: tabUpdateBucket failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabDeleteActiveBoundInst(inst); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongTimerHandler: tabDeleteActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongTimerHandler: tabActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrFindNodeTimerHandler(inst *instCtrlBlock) TabMgrErrno {
	mgr, ok := tabMgr.subNetMgrList[inst.snid]
	if !ok {
		log.Debugf("tabMgrFindNodeTimerHandler: invalid subnet: %x", inst.snid)
		return TabMgrEnoNotFound
	}

	inst.state = TabInstStateQTimeout
	inst.rsp = nil
	if eno := mgr.tabUpdateNodeDb4Query(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.Debugf("tabMgrFindNodeTimerHandler: tabUpdateNodeDb4Query failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabUpdateBucket(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.Debugf("tabMgrFindNodeTimerHandler: tabUpdateBucket failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {
		log.Debugf("tabMgrFindNodeTimerHandler: tabDeleteActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabActiveQueryInst(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrFindNodeTimerHandler: tabActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrRefreshReq(msg *sch.MsgTabRefreshReq) TabMgrErrno {
	if !tabMgr.natUdpResult {
		log.Debugf("tabMgrRefreshReq: nat mapping not established")
		return TabMgrEnoUdp
	}
	return tabMgr.tabRefresh(&msg.Snid, nil)
}

func (tabMgr *TableManager) tabMgrFindNodeRsp(msg *sch.NblFindNodeRsp) TabMgrErrno {
	snid := msg.FindNode.SubNetId
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !ok {
		log.Debugf("tabMgrFindNodeRsp: invalid subnet: %x", snid)
		return TabMgrEnoNotFound
	}

	var inst *instCtrlBlock = nil
	inst = mgr.tabFindInst(&msg.FindNode.To, TabInstStateQuering)
	if inst == nil {
		log.Debugf("tabMgrFindNodeRsp: instance not found, subnet: %x, id: %X",
			snid, msg.FindNode.To.NodeId)
		return TabMgrEnoNotFound
	}

	inst.rsp = msg

	// Obtain result. notice: if the result responed is "duplicated", we just need
	// to delete the duplicated active query instance and try to activate more.
	var result = msg.Result & 0xffff
	if result == TabMgrEnoDuplicated {
		if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {
			log.Debugf("tabMgrFindNodeRsp: tabDeleteActiveQueryInst failed, " +
				"eno: %d, subnet: %x", eno, snid)
			return eno
		}
		if eno := mgr.tabActiveQueryInst(); eno != TabMgrEnoNone {
			log.Debugf("tabMgrFindNodeRsp: tabActiveQueryInst failed, " +
				"eno: %d, subnet: %x", eno, snid)
			return eno
		}
		return TabMgrEnoNone
	}

	// update database for the neighbor node.
	// DON'T care the result
	if eno := mgr.tabUpdateNodeDb4Query(inst, result); eno != TabMgrEnoNone {
		log.Debugf("tabMgrFindNodeRsp: tabUpdateNodeDb4Query failed, " +
			"eno: %d, subnet: %x", eno, snid)
	}

	// update buckets：DON'T care the result
	mgr.tabUpdateBucket(inst, result)

	// delete the active instance
	if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {
		return eno
	}

	// try to active more query instances
	mgr.tabActiveQueryInst()

	// check result reported, if it's failed, need not go further
	if result != 0 {
		return TabMgrEnoNone
	}

	// deal with the peer and those neighbors the peer reported, add them into the
	// BOUND pending queue for bounding, see bellow pls.
	mgr.tabAddPendingBoundInst(&msg.Neighbors.From)
	for _, node := range msg.Neighbors.Nodes {
		if eno := mgr.tabAddPendingBoundInst(node); eno != TabMgrEnoNone {
			break
		}
	}

	// try to active more BOUND instances
	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		return eno
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrPingpongRsp(msg *sch.NblPingRsp) TabMgrErrno {
	snid := msg.Ping.SubNetId
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !ok {
		log.Debugf("tabMgrFindNodeRsp: invalid subnet: %x", snid)
		return TabMgrEnoNotFound
	}

	var inst *instCtrlBlock = nil
	inst = mgr.tabFindInst(&msg.Ping.To, TabInstStateBonding)
	if inst == nil {
		if mgr.cfg.bootstrapNode == false {
			return TabMgrEnoNotFound
		}
		if msg.Result != 0 {
			return TabMgrEnoNone
		}
		if msg.Pong == nil {
			return TabMgrEnoInternal
		}
		return mgr.tabUpdateBootstarpNode(&msg.Pong.From)
	}

	inst.rsp = msg
	var result = msg.Result
	if result != 0 {
		result = TabMgrEnoPingpongFailed
	}

	// Update buckets
	mgr.tabUpdateBucket(inst, result)

	// delete the active instance
	if eno := mgr.tabDeleteActiveBoundInst(inst); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongRsp: tabDeleteActiveQueryInst failed, eno: %d, subnet: %x", eno, snid)
		return eno
	}

	// try to active more BOUND instances
	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongRsp: tabActiveBoundInst failed, eno: %d, subnet: %x", eno, snid)
		return eno
	}

	// Check result reported
	if msg.Result != 0 {
		return TabMgrEnoNone
	}

	// Update last pong time
	pot := time.Now()
	if eno := mgr.tabBucketUpdateBoundTime(NodeID(inst.req.(*um.Ping).To.NodeId), nil, &pot); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongRsp: tabBucketUpdateBoundTime failed, eno: %d, subnet: %x", eno, snid)
		return eno
	}

	// Update node database for pingpong related info
	n := Node{
		Node: config.Node{
			IP:  msg.Pong.From.IP,
			UDP: msg.Pong.From.UDP,
			TCP: msg.Pong.From.TCP,
			ID:  msg.Pong.From.NodeId,
		},
		sha: *TabNodeId2Hash(NodeID(msg.Pong.From.NodeId)),
	}
	if eno := mgr.tabUpdateNodeDb4Bounding(&n, nil, &pot); eno != TabMgrEnoNone {
		log.Debugf("tabMgrPingpongRsp: tabUpdateNodeDb4Bounding failed, eno: %d, subnet: %x", eno, snid)
		return eno
	}

	// response to the discover manager task
	if tabMgr.tabIsBootstrapNode(&n.ID) == false {
		if eno := mgr.tabDiscoverResp(&msg.Pong.From); eno != TabMgrEnoNone {
			log.Debugf("tabMgrPingpongRsp: tabDiscoverResp failed, eno: %d, subnet: %x", eno, snid)
			return eno
		}
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrPingedInd(ping *um.Ping) TabMgrErrno {
	snid := ping.SubNetId
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !tabMgr.cfg.bootstrapNode && !ok {
		log.Debugf("tabMgrPingedInd: invalid snid: %x", snid)
		return TabMgrEnoNotFound
	}

	if mgr == nil {
		if mgr = tabMgr.switch2RootInst(); mgr == nil {
			log.Debugf("tabMgrPingedInd: invalid snid: %x", snid)
			return TabMgrEnoNotFound
		}
	}

	if mgr.tabShouldBound(NodeID(ping.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	if eno := mgr.tabAddPendingBoundInst(&ping.From); eno != TabMgrEnoNone {
		return eno
	}

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		return eno
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrPongedInd(pong *um.Pong) TabMgrErrno {
	snid := pong.SubNetId
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !tabMgr.cfg.bootstrapNode && !ok {
		log.Debugf("tabMgrPongedInd: invalid snid: %x", snid)
		return TabMgrEnoNotFound
	}

	if mgr == nil {
		if mgr = tabMgr.switch2RootInst(); mgr == nil {
			log.Debugf("tabMgrPongedInd: invalid snid: %x", snid)
			return TabMgrEnoNotFound
		}
	}

	if mgr.tabShouldBound(NodeID(pong.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	if eno := mgr.tabAddPendingBoundInst(&pong.From); eno != TabMgrEnoNone {
		return eno
	}

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		return eno
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrQueriedInd(findNode *um.FindNode) TabMgrErrno {
	snid := findNode.SubNetId
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !tabMgr.cfg.bootstrapNode && !ok {
		log.Debugf("tabMgrQueriedInd: invalid snid: %x", snid)
		return TabMgrEnoNotFound
	}

	if mgr == nil {
		if mgr = tabMgr.switch2RootInst(); mgr == nil {
			log.Debugf("tabMgrQueriedInd: invalid snid: %x", snid)
			return TabMgrEnoNotFound
		}
	}

	// if the local is a bootstrap node and the peer reported itself with a private address, then
	// the "BOUNDING" procedure should not invoked. seems can just for debug or test.
	if mgr.cfg.bootstrapNode {
		if TabCheckPrivateIp(findNode.From.IP) {
			if eno := mgr.tabUpdateBootstarpNode(&findNode.From); eno != TabMgrEnoNone {
				log.Debugf("tabMgrQueriedInd: tabUpdateBootstarpNode failed, eno: %d", eno)
				return eno
			}
			return TabMgrEnoNone
		}
	}

	if mgr.tabShouldBound(NodeID(findNode.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	if eno := mgr.tabAddPendingBoundInst(&findNode.From); eno != TabMgrEnoNone {
		log.Debugf("tabMgrQueriedInd: tabAddPendingBoundInst failed, eno: %d", eno)
		return eno
	}

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		log.Debugf("tabMgrQueriedInd: tabActiveBoundInst failed, eno: %d", eno)
		return eno
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrNatReadyInd(msg *sch.MsgNatMgrReadyInd) TabMgrErrno {
	log.Debugf("tabMgrNatReadyInd: nat type: %s", msg.NatType)
	if msg.NatType == config.NATT_NONE {
		tabMgr.pubTcpIp = tabMgr.cfg.local.IP
		tabMgr.pubTcpPort = int(tabMgr.cfg.local.TCP)
		tabMgr.natTcpResult = true
		tabMgr.pubUdpIp = tabMgr.cfg.local.IP
		tabMgr.pubUdpPort = int(tabMgr.cfg.local.UDP)
		tabMgr.natUdpResult = true
	} else {
		reqUdp := sch.MsgNatMgrMakeMapReq{
			Proto:      "udp",
			FromPort:   int(tabMgr.cfg.local.UDP),
			ToPort:     int(tabMgr.cfg.local.UDP),
			DurKeep:    natMapKeepTime,
			DurRefresh: natMapRefreshTime,
		}
		schMsg := sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNatMgr, sch.EvNatMgrMakeMapReq, &reqUdp)
		if eno := tabMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Debugf("tabMgrNatReadyInd: SchSendMessage failed, eno: %d", eno)
			return TabMgrEnoScheduler
		}
		reqTcp := reqUdp
		reqTcp.Proto = "tcp"
		reqTcp.FromPort = int(tabMgr.cfg.local.TCP)
		reqTcp.ToPort = int(tabMgr.cfg.local.TCP)
		schMsg = sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNatMgr, sch.EvNatMgrMakeMapReq, &reqTcp)
		if eno := tabMgr.sdl.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			log.Debugf("tabMgrNatReadyInd: SchSendMessage failed, eno: %d", eno)
			return TabMgrEnoScheduler
		}
	}
	schMsg := sch.SchMessage{}
	tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnPeerMgr, sch.EvNatMgrReadyInd, msg)
	tabMgr.sdl.SchSendMessage(&schMsg)
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrNatMakeMapRsp(msg *sch.MsgNatMgrMakeMapRsp) TabMgrErrno {
	// notice0: the external public ip reported in msg.pubUdpIp and msg.pubTcpIp must be
	// same, tabMgr.pubTcpIp is copied twice with same value. a "status" is introduced
	// for future to deal with the case: the "result" is ok, but need to wait nat to do
	// more work for mapping.
	// notice1: this is happened while the p2p is powering on, just updating the public
	// address is needed, for rebuilding about address switching, see function
	// tabMgrNatPubAddrUpdateInd for details please.
	if !nat.NatIsResultOk(msg.Result) {
		log.Debugf("tabMgrNatMakeMapRsp: fail reported, msg: %+v", *msg)
	}
	log.Debugf("tabMgrNatMakeMapRsp: proto: %s, ip:port = %s:%d",
		msg.Proto, msg.PubIp.String(), msg.PubPort)

	proto := strings.ToLower(msg.Proto)
	if proto == "udp" {
		tabMgr.natUdpResult = nat.NatIsResultOk(msg.Result)
		if tabMgr.natUdpResult && nat.NatIsStatusOk(msg.Status) {
			tabMgr.pubUdpIp = msg.PubIp
			tabMgr.pubUdpPort = msg.PubPort
			log.Debugf("tabMgrNatMakeMapRsp: public chain udp addr: %s:%d",
				tabMgr.pubUdpIp.String(), tabMgr.pubUdpPort)
		} else {
			tabMgr.pubUdpIp = net.IPv4zero
			tabMgr.pubUdpPort = 0
		}
	} else if proto == "tcp" {
		tabMgr.natTcpResult = nat.NatIsResultOk(msg.Result)
		if tabMgr.natTcpResult && nat.NatIsStatusOk(msg.Status) {
			tabMgr.pubTcpIp = msg.PubIp
			tabMgr.pubTcpPort = msg.PubPort
			log.Debugf("tabMgrNatMakeMapRsp: public chain tcp addr: %s:%d",
				tabMgr.pubTcpIp.String(), tabMgr.pubTcpPort)
		} else {
			tabMgr.pubTcpIp = net.IPv4zero
			tabMgr.pubTcpPort = 0
		}
		schMsg := sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnPeerMgr, sch.EvNatMgrMakeMapRsp, msg)
		tabMgr.sdl.SchSendMessage(&schMsg)
	} else {
		log.Debugf("tabMgrNatMakeMapRsp: unknown protocol reported: %s", proto)
		return TabMgrEnoParameter
	}

	// refresh all possible sub networks in the list. since we had put all
	// into the list in any cases, we need just to loop the list, see codes
	// and comments above please. notice that, we do not take the old natUdpResult
	// into account here.

	if tabMgr.natUdpResult {
		if eno := tabMgr.switch2NatAddr(proto); eno != TabMgrEnoNone {
			log.Debugf("tabMgrNatMakeMapRsp: switch2NatAddr failed, eno: %d", eno)
			return eno
		} else if proto == "udp" {
			for _, mgr := range tabMgr.subNetMgrList {
				mgr.startSubnetRefresh(nil)
			}
		}
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabMgrNatPubAddrUpdateInd(msg *sch.MsgNatMgrPubAddrUpdateInd) TabMgrErrno {
	// see function nat.refreshInstance, the indication would be received after nat manager
	// refreshed itself ok, here the msg.Result must be good.
	if !nat.NatIsStatusOk(msg.Status) {
		log.Debugf("tabMgrNatPubAddrUpdateInd: fail reported, msg: %+v", *msg)
	}
	log.Debugf("tabMgrNatPubAddrUpdateInd: proto: %s, old: %s:%d; new: %s:%d",
		msg.Proto, tabMgr.pubTcpIp.String(), tabMgr.pubTcpPort, msg.PubIp.String(), msg.PubPort)

	old := tabMgr.natUdpResult
	oldUdpIp := tabMgr.pubUdpIp
	oldUdpPort := tabMgr.pubUdpPort
	proto := strings.ToLower(msg.Proto)
	if proto == "udp" {
		tabMgr.natUdpResult = nat.NatIsStatusOk(msg.Status)
		if tabMgr.natUdpResult {
			tabMgr.pubUdpIp = msg.PubIp
			tabMgr.pubUdpPort = msg.PubPort
		} else {
			tabMgr.pubUdpIp = net.IPv4zero
			tabMgr.pubUdpPort = 0
		}
	} else if proto == "tcp" {
		tabMgr.natTcpResult = nat.NatIsStatusOk(msg.Status)
		if tabMgr.natTcpResult {
			tabMgr.pubTcpIp = msg.PubIp
			tabMgr.pubTcpPort = msg.PubPort
		} else {
			tabMgr.pubTcpIp = net.IPv4zero
			tabMgr.pubTcpPort = 0
		}
		schMsg := sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnPeerMgr, sch.EvNatMgrPubAddrUpdateInd, msg)
		tabMgr.sdl.SchSendMessage(&schMsg)
	} else {
		log.Debugf("tabMgrNatPubAddrUpdateInd: unknown protocol reported: %s", proto)
		return TabMgrEnoParameter
	}

	// check to start or stop auto-refreshing when updated
	if tabMgr.natUdpResult {
		if !tabMgr.pubUdpIp.Equal(oldUdpIp) || oldUdpPort != tabMgr.pubUdpPort {
			log.Debugf("tabMgrNatPubAddrUpdateInd: call pubAddrSwitchPrepare")
			if eno := tabMgr.pubAddrSwitchPrepare(); eno != TabMgrEnoNone {
				log.Debugf("tabMgrNatPubAddrUpdateInd: pubAddrSwitchPrepare failed, eno: %d", eno)
				return eno
			}
		}
		log.Debugf("tabMgrNatPubAddrUpdateInd: call switch2NatAddr")
		if eno := tabMgr.switch2NatAddr(proto); eno != TabMgrEnoNone {
			log.Debugf("tabMgrNatPubAddrUpdateInd: switch2NatAddr failed, eno: %d", eno)
			return eno
		}
		if proto == "udp" {
			for _, mgr := range tabMgr.subNetMgrList {
				// notice: here we start refreshing with the local node identity, so, peer would update to our
				// new address with this identity(to overlap the old one), it can try to connect us also.
				log.Debugf("tabMgrNatPubAddrUpdateInd: call startSubnetRefresh, snid: %x", mgr.snid)
				mgr.startSubnetRefresh(&mgr.cfg.local.ID)
			}
		}
	} else if old {
		for _, mgr := range tabMgr.subNetMgrList {
			log.Debugf("tabMgrNatPubAddrUpdateInd: call stopSubnetRefresh, snid: %x", mgr.snid)
			mgr.stopSubnetRefresh()
		}
	}

	return TabMgrEnoNone
}

// Static task to keep the node database clean
const NdbcName = "ndbCleaner"

type NodeDbCleaner struct {
	sdl    *sch.Scheduler    // pointer to scheduler
	name   string            // name
	tep    sch.SchUserTaskEp // entry point
	tabMgr *TableManager     // pointer to table manager
	tid    int               // cleaner timer
}

func NewNdbCleaner() *NodeDbCleaner {
	var ndbCleaner = NodeDbCleaner{
		name: NdbcName,
		tep:  nil,
		tid:  sch.SchInvalidTid,
	}

	ndbCleaner.tep = ndbCleaner.ndbcProc
	return &ndbCleaner
}

func (ndbc *NodeDbCleaner) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return ndbc.tep(ptn, msg)
}

func (ndbc *NodeDbCleaner) ndbcProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	var eno TabMgrErrno
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = ndbc.ndbcPoweron(ptn)
	case sch.EvSchPoweroff:
		eno = ndbc.ndbcPoweroff(ptn)
	case sch.EvNdbCleanerTimer:
		eno = ndbc.ndbcAutoCleanTimerHandler()
	default:
		log.Debugf("NdbcProc: invalid message: %d", msg.Id)
		return sch.SchEnoInternal
	}
	log.Tracef("NdbcProc: get out, msg: %d, eno: %d", msg.Id, eno)
	return sch.SchEnoNone
}

func (ndbc *NodeDbCleaner) ndbcPoweron(ptn interface{}) TabMgrErrno {

	// if it's a static type, no database cleaner needed
	sdl := sch.SchGetScheduler(ptn)
	if sdl.SchGetP2pConfig().NetworkType == config.P2pNetworkTypeStatic {
		log.Debugf("ndbcPoweron: static subnet type, nodeDbCleaner will be done ...")
		sdl.SchTaskDone(ptn, ndbc.name, sch.SchEnoNone)
		return TabMgrEnoNone
	}

	ndbc.sdl = sdl
	ndbc.tabMgr = ndbc.sdl.SchGetTaskObject(TabMgrName).(*TableManager)
	var tmd = sch.TimerDescription{
		Name:  NdbcName + "_autoclean",
		Utid:  sch.NdbCleanerTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   nodeAutoCleanCycle,
		Extra: nil,
	}

	var (
		eno sch.SchErrno
		tid int
	)

	if eno, tid = ndbc.sdl.SchSetTimer(ptn, &tmd); eno != sch.SchEnoNone {
		log.Debugf("ndbcPoweron: SchSetTimer failed, eno: %d", eno)
		return TabMgrEnoScheduler
	}

	ndbc.tid = tid

	return TabMgrEnoNone
}

func (ndbc *NodeDbCleaner) ndbcPoweroff(ptn interface{}) TabMgrErrno {

	log.Debugf("ndbcPoweroff: task will be done")

	if ndbc.tid != sch.SchInvalidTid {
		ndbc.sdl.SchKillTimer(ptn, ndbc.tid)
		ndbc.tid = sch.SchInvalidTid
	}

	if ndbc.sdl.SchTaskDone(ptn, ndbc.name, sch.SchEnoKilled) != sch.SchEnoNone {
		return TabMgrEnoScheduler
	}

	return TabMgrEnoNone
}

func (ndbc *NodeDbCleaner) ndbcAutoCleanTimerHandler() TabMgrErrno {
	log.Debugf("ndbcAutoCleanTimerHandler: " +
		"auto cleanup timer expired, it's time to clean ...")

	err := ndbc.tabMgr.nodeDb.expireNodes()
	if err != nil {
		log.Debugf("ndbcAutoCleanTimerHandler: cleanup failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}

	log.Debugf("ndbcAutoCleanTimerHandler: cleanup ok")

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabGetConfig(tabCfg *tabConfig) TabMgrErrno {
	cfg := config.P2pConfig4TabManager(tabMgr.sdl.SchGetP2pCfgName())
	if cfg == nil {
		log.Debugf("tabGetConfig: P2pConfig4TabManager failed")
		return TabMgrEnoConfig
	}

	tabCfg.local = cfg.Local
	tabCfg.dataDir = cfg.DataDir
	tabCfg.name = cfg.Name
	tabCfg.nodeDb = cfg.NodeDB
	tabCfg.noHistory = cfg.NoHistory
	tabCfg.bootstrapNode = cfg.BootstrapNode
	tabCfg.snidMaskBits = cfg.SnidMaskBits
	tabCfg.subNetNodeList = cfg.SubNetNodeList
	tabCfg.subNetIdList = cfg.SubNetIdList

	tabCfg.bootstrapNodes = make([]*Node, len(cfg.BootstrapNodes))
	for idx, n := range cfg.BootstrapNodes {
		tabCfg.bootstrapNodes[idx] = new(Node)
		tabCfg.bootstrapNodes[idx].Node = *n
		tabCfg.bootstrapNodes[idx].sha = *TabNodeId2Hash(NodeID(n.ID))
	}

	// for static network type, table manager is not applied; a table manager with sub
	// network identity as config.ZeroSubNet means that this manager is not applied to
	// any real service activities.

	tabMgr.networkType = cfg.NetworkType

	if tabMgr.networkType == config.P2pNetworkTypeStatic {
		tabMgr.snid = config.ZeroSubNet
	} else if tabMgr.networkType == config.P2pNetworkTypeDynamic {

		if len(tabCfg.subNetIdList) == 0 {
			tabMgr.snid = AnySubNet
		} else {
			tabMgr.snid = config.ZeroSubNet
		}
	} else {
		log.Debugf("tabGetConfig: invalid network type: %d", tabMgr.networkType)
		return TabMgrEnoConfig
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabNodeDbPrepare() TabMgrErrno {
	if tabMgr.nodeDb != nil {
		log.Debugf("tabNodeDbPrepare: node database had been opened")
		return TabMgrEnoDatabase
	}

	dbPath := path.Join(tabMgr.cfg.dataDir, tabMgr.cfg.name, tabMgr.cfg.nodeDb)
	if runtime.GOOS == "windows" {
		log.Debugf("tabMgr: nodeDb will be created at: %s", strings.Replace(dbPath, "/", "\\", -1))
	} else {
		log.Debugf("tabMgr: nodeDb will be created at: %s", dbPath)
	}

	if tabMgr.cfg.noHistory {
		if _, err := os.Stat(dbPath); err == nil {
			os.RemoveAll(dbPath)
		}
	}

	db, err := newNodeDB(dbPath, ndbVersion, NodeID(tabMgr.cfg.local.ID))
	if err != nil {
		log.Debugf("tabNodeDbPrepare: newNodeDB failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}

	tabMgr.nodeDb = db
	return TabMgrEnoNone
}

func TabNodeId2Hash(id NodeID) *Hash {
	h := sha256.Sum256(id[:])
	return (*Hash)(&h)
}

func (tabMgr *TableManager) tabSetupLocalHashId() TabMgrErrno {
	if cap(tabMgr.shaLocal) != 32 {
		log.Debugf("tabSetupLocalHashId: hash identity should be 32 bytes")
		return TabMgrEnoParameter
	}

	h := TabNodeId2Hash(NodeID(tabMgr.cfg.local.ID))
	tabMgr.shaLocal = *h

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabRelatedTaskPrepare(ptnMe interface{}) TabMgrErrno {
	var eno = sch.SchEnoNone

	if eno, tabMgr.ptnNgbMgr = tabMgr.sdl.SchGetUserTaskNode(sch.NgbMgrName); eno != sch.SchEnoNone {
		log.Debugf("tabRelatedTaskPrepare: get task node failed, name: %s", sch.NgbMgrName)
		return TabMgrEnoScheduler
	}

	if eno, tabMgr.ptnDcvMgr = tabMgr.sdl.SchGetUserTaskNode(sch.DcvMgrName); eno != sch.SchEnoNone {
		log.Debugf("tabRelatedTaskPrepare: get task node failed, name: %s", sch.DcvMgrName)
		return TabMgrEnoScheduler
	}

	if eno, tabMgr.ptnNatMgr = tabMgr.sdl.SchGetUserTaskNode(sch.NatMgrName); eno != sch.SchEnoNone {
		log.Debugf("tabRelatedTaskPrepare: get task node failed, name: %s", sch.DcvMgrName)
		return TabMgrEnoScheduler
	}

	if eno, tabMgr.ptnPeerMgr = tabMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName); eno != sch.SchEnoNone {
		log.Debugf("tabRelatedTaskPrepare: get task node failed, name: %s", sch.PeerMgrName)
		return TabMgrEnoScheduler
	}

	if tabMgr.ptnMe == nil || tabMgr.ptnNgbMgr == nil || tabMgr.ptnDcvMgr == nil ||
		tabMgr.ptnNatMgr == nil || tabMgr.ptnPeerMgr == nil {
		log.Debugf("tabRelatedTaskPrepare: invaid task node pointer")
		return TabMgrEnoInternal
	}

	return TabMgrEnoNone
}

func tabSetupLog2DistanceLookupTable(lkt []int) TabMgrErrno {
	var n uint
	var b uint
	lkt[0] = 8
	for n = 0; n < 8; n++ {
		for b = 1 << n; b < 1<<(n+1); b++ {
			lkt[b] = int(8 - n - 1)
		}
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabRefresh(snid *SubNetworkID, tid *NodeID) TabMgrErrno {
	// If we are in refreshing, return at once. When the pending table for query
	// is empty, this flag is set to false;
	// If the "tid"(target identity) passed in is nil, we build a random one;
	if _, ok := tabMgr.subNetMgrList[*snid]; !ok {
		log.Debugf("tabRefresh: none of manager for subnet: %x", *snid)
		return TabMgrEnoNotFound
	}
	mgr := tabMgr.subNetMgrList[*snid]
	mgr.refreshing = len(mgr.queryIcb) >= TabInstQueringMax
	if mgr.refreshing == true {
		log.Debugf("tabRefresh: already in refreshing")
		return TabMgrEnoNone
	}

	var nodes []*Node
	var target NodeID

	// if target identity is nil, create randomly, and always force the target
	// identity to get a same subnet identity specified.
	if tid == nil {
		rand.Read(target[:])
	} else {
		target = *tid
	}
	target[config.NodeIDBytes-2] = snid[0]
	target[config.NodeIDBytes-1] = snid[1]
	if nodes = mgr.tabClosest(Closest4Querying, target, -1, TabInstQPendingMax); len(nodes) == 0 {

		log.Tracef("tabRefresh: snid: %x, seems all buckets are empty, " +
			"set local as target and try seeds from database and bootstrap nodes ...",
			*snid)

		target = NodeID(mgr.cfg.local.ID)
		seeds := mgr.tabSeedsFromDb(TabInstQPendingMax, seedMaxAge)
		seedsBackup := make([]*Node, 0)

		if len(seeds) == 0 {
			log.Debugf("tabRefresh: empty seeds set from nodes database")
		} else {
			// Check if seeds from database need to be bound, if false, means
			// that those seeds can be connected to without bounding procedure,
			// we report them to discover task to speed up our p2p network.
			for _, dbn := range seeds {
				if mgr.tabShouldBoundDbNode(NodeID(dbn.ID)) == false {
					umNode := um.Node{
						IP:     dbn.IP,
						UDP:    dbn.UDP,
						TCP:    dbn.TCP,
						NodeId: dbn.ID,
					}

					if eno := mgr.tabDiscoverResp(&umNode); eno != TabMgrEnoNone {
						log.Debugf("tabRefresh: tabDiscoverResp failed, eno: %d", eno)
					}
					continue
				}
				seedsBackup = append(seedsBackup, dbn)
			}
		}
		nodes = append(nodes, mgr.cfg.bootstrapNodes...)
		nodes = append(nodes, seedsBackup...)
		if len(nodes) == 0 {
			log.Debugf("tabRefresh: can't go refreshing without any seeds")
			return TabMgrEnoResource
		}
		if len(nodes) > TabInstQPendingMax {
			log.Debugf("tabRefresh: too much seeds, truncated: %d", len(nodes)-TabInstQPendingMax)
			nodes = nodes[:TabInstQPendingMax]
		}
	}

	if eno := mgr.tabQuery(&target, nodes); eno != TabMgrEnoNone {
		log.Debugf("tabRefresh: tabQuery failed, eno: %d", eno)
		return eno
	}
	mgr.refreshing = true
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabLog2Dist(h1 Hash, h2 Hash) int {
	// Caculate the distance between two nodes.
	// Notice: the return "d" more larger, it's more closer
	var d = 0
	for i, b := range h2 {
		delta := tabMgr.dlkTab[h1[i]^b]
		d += delta
		if delta != 8 {
			break
		}
	}
	return d
}

func (tabMgr *TableManager) tabClosest(forWhat int, target NodeID, mbs int, size int) []*Node {
	// Notice: in this function, we got []*Node with a approximate order,
	// since we do not sort the nodes in the first bank, see bellow pls.
	var closest = make([]*Node, 0, maxBonding)
	var count = 0

	if size == 0 || size > maxBonding {
		log.Debugf("tabClosest: invalid size: %d, min: 1, max: %d", size, maxBonding)
		return nil
	}
	if forWhat != Closest4Queried && forWhat != Closest4Querying {
		log.Debugf("tabClosest: don't know what it's for: %d", forWhat)
		return nil
	}

	ht := TabNodeId2Hash(target)
	dt := tabMgr.tabLog2Dist(tabMgr.shaLocal, *ht)

	// get target subnet identity, see bellow pls. this is only for Closest4Queried means
	// we are looking for nodes for queries from peers. in this case, if the local node is
	// bootstrap node and configured as "AnySubNet", we had to determine the target subnet
	// identity by mask bits and the target node identity, since the subnet identity is not
	// backup in the bucket. and please notice that: if it is for "Closest4Querying", then
	// when come the tabMgr instance must matched the target subnet identity.

	targetSnid := config.SubNetworkID{}
	if forWhat == Closest4Queried &&
		tabMgr.cfg.bootstrapNode &&
		tabMgr.snid == config.AnySubNet {
		err := error(nil)
		targetSnid, err = GetSubnetIdentity(target, mbs)
		if err != nil {
			log.Debugf("tabClosest: GetSubnetIdentity failed, err: %s", err.Error())
			return nil
		}
	}

	var addClosest = func(bk *bucket) int {
		count = len(closest)
		if bk != nil {
			for _, ne := range bk.nodes {

				// if we are fetching nodes to which we would query, we need to check the time
				// we had queried them last time to escape the case that query too frequency.

				if forWhat == Closest4Querying {
					if time.Now().Sub(ne.lastQuery) < findNodeMinInterval {
						continue
					}
				}

				// match the subnet identity to add closest set for bootstrap node when working
				// with subnet identity as "AnySubNet". so it can play without a reconfiguration
				// while others do.

				if forWhat == Closest4Queried &&
					tabMgr.cfg.bootstrapNode &&
					tabMgr.snid == config.AnySubNet {

					snid, err := GetSubnetIdentity(ne.ID, mbs)
					if err != nil {
						log.Debugf("tabClosest: GetSubnetIdentity failed, err: %s", err.Error())
						continue
					}

					if snid == targetSnid {
						closest = append(closest, &Node{
							Node: ne.Node,
							sha:  ne.sha,
						})
						if count++; count >= size {
							break
						}
					}

				} else {

					closest = append(closest, &Node{
						Node: ne.Node,
						sha:  ne.sha,
					})
					if count++; count >= size {
						break
					}
				}
			}
		}
		return count
	}

	// the most closest bank: one should sort nodes in this bank if accurate
	// order by log2 distance against the target node is expected, but we not.
	if bk := tabMgr.buckets[dt]; bk != nil && len(bk.nodes) > 0 {
		if addClosest(bk) >= size {
			return closest
		}
	}

	// the second closest bank
	for loop := dt + 1; loop < cap(tabMgr.buckets); loop++ {
		if bk := tabMgr.buckets[loop]; bk != nil && len(bk.nodes) > 0 {
			if addClosest(bk) >= size {
				return closest
			}
		}
	}

	if dt <= 0 {
		return closest
	}

	// the last bank
	for loop := dt - 1; loop >= 0; loop-- {
		if bk := tabMgr.buckets[loop]; bk != nil && len(bk.nodes) > 0 {
			if addClosest(bk) >= size {
				return closest
			}
		}
	}

	return closest
}

func (tabMgr *TableManager) tabSeedsFromDb(size int, age time.Duration) []*Node {
	if size == 0 {
		log.Debugf("tabSeedsFromDb: invalid zero size")
		return nil
	}

	if size > seedMaxCount {
		size = seedMaxCount
	}
	if age > seedMaxAge {
		age = seedMaxAge
	}

	nodes := tabMgr.nodeDb.querySeeds(tabMgr.snid, size, age)
	if nodes == nil {
		log.Debugf("tabSeedsFromDb: nil nodes")
		return nil
	}

	if len(nodes) == 0 {
		log.Debugf("tabSeedsFromDb: empty node table")
		return nil
	}

	if len(nodes) > size {
		nodes = nodes[0:size]
	}

	return nodes
}

func (tabMgr *TableManager) tabQuery(target *NodeID, nodes []*Node) TabMgrErrno {
	// check: since we apply doing best to active more, it's impossible that the active
	// table is not full while the pending table is not empty.
	remain := len(nodes)
	actNum := len(tabMgr.queryIcb)
	if remain == 0 {
		log.Debugf("tabQuery: invalid parameters, no node to be handled")
		return TabMgrEnoParameter
	}

	// create query instances
	var loop = 0
	var dup bool

	if actNum < TabInstQueringMax {
		for ; loop < remain && actNum < TabInstQueringMax; loop++ {
			// check not to query ourselves
			if nodes[loop].ID == tabMgr.cfg.local.ID {
				continue
			}
			// Check not to query duplicated
			dup = false
			for _, qi := range tabMgr.queryIcb {
				if qi.req.(*um.FindNode).To.NodeId == nodes[loop].ID {
					dup = true
					break
				}
			}

			if dup {
				continue
			}

			// do query
			msg := new(um.FindNode)
			icb := new(instCtrlBlock)

			icb.snid = tabMgr.snid
			icb.state = TabInstStateQuering
			icb.qrt = time.Now()
			icb.req = msg
			icb.rsp = nil
			icb.tid = sch.SchInvalidTid

			msg.From = um.Node{
				IP:     tabMgr.cfg.local.IP,
				UDP:    tabMgr.cfg.local.UDP,
				TCP:    tabMgr.cfg.local.TCP,
				NodeId: tabMgr.cfg.local.ID,
			}

			msg.To = um.Node{
				IP:     nodes[loop].IP,
				UDP:    nodes[loop].UDP,
				TCP:    nodes[loop].TCP,
				NodeId: nodes[loop].ID,
			}

			msg.FromSubNetId = tabMgr.cfg.subNetIdList
			msg.SubNetId = tabMgr.snid
			msg.MaskBits = tabMgr.cfg.snidMaskBits
			msg.Target = config.NodeID(*target)
			msg.Id = uint64(time.Now().UnixNano())
			msg.Expiration = 0
			msg.Extra = nil

			log.Tracef("tabQuery: EvNblFindNodeReq will be sent, ip: %s, udp: %d",
				msg.To.IP.String(), msg.To.UDP)

			var schMsg = sch.SchMessage{}
			tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNgbMgr, sch.EvNblFindNodeReq, msg)
			tabMgr.sdl.SchSendMessage(&schMsg)

			if eno := tabMgr.tabStartTimer(icb, sch.TabFindNodeTimerId, findNodeExpiration); eno != TabMgrEnoNone {
				log.Debugf("tabQuery: tabStartTimer failed, eno: %d", eno)
				return eno
			}

			tabMgr.queryIcb = append(tabMgr.queryIcb, icb)
			actNum++
		}
	}

	// append nodes to pending table if any
	for ; loop < remain; loop++ {
		if len(tabMgr.queryPending) >= TabInstQPendingMax {
			log.Debugf("tabQuery: pending query table full")
			break
		}

		// check not to query ourselves
		if nodes[loop].ID == tabMgr.cfg.local.ID {
			continue
		}

		// Check not to query duplicated
		dup = false

		for _, qp := range tabMgr.queryPending {
			if qp.node.ID == nodes[loop].ID {
				dup = true
				break
			}
		}

		if dup {
			continue
		}

		// Append to query pending
		tabMgr.queryPending = append(tabMgr.queryPending, &queryPendingEntry{
			node:   nodes[loop],
			target: target,
		})
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabFindInst(node *um.Node, state int) *instCtrlBlock {
	if node == nil || (state != TabInstStateQuering &&
		state != TabInstStateBonding &&
		state != TabInstStateQTimeout &&
		state != TabInstStateBTimeout) {
		log.Debugf("tabFindInst: invalid parameters")
		return nil
	}

	if state == TabInstStateQuering || state == TabInstStateQTimeout {
		for _, icb := range tabMgr.queryIcb {
			req := icb.req.(*um.FindNode)
			if req.To.CompareWith(node) == um.CmpNodeEqu {
				return icb
			}
		}
	} else {
		for _, icb := range tabMgr.boundIcb {
			req := icb.req.(*um.Ping)
			if req.To.CompareWith(node) == um.CmpNodeEqu {
				return icb
			}
		}
	}

	return nil
}

func (tabMgr *TableManager) tabUpdateNodeDb4Query(inst *instCtrlBlock, result int) TabMgrErrno {
	snid := tabMgr.snid
	var fnFailUpdate = func() TabMgrErrno {
		id := NodeID(inst.req.(*um.FindNode).To.NodeId)
		if node := tabMgr.nodeDb.node(snid, id); node == nil {
			log.Debugf("tabUpdateNodeDb4Query: fnFailUpdate: node not exist")
			return TabMgrEnoNone
		}
		fails := tabMgr.nodeDb.findFails(snid, id) + 1
		if err := tabMgr.nodeDb.updateFindFails(snid, id, fails); err != nil {
			log.Debugf("tabUpdateNodeDb4Query: fnFailUpdate: updateFindFails failed, err: %s", err.Error())
			return TabMgrEnoDatabase
		}
		return TabMgrEnoNone
	}

	switch {

	case inst.state == TabInstStateQuering && result == TabMgrEnoNone:
		return TabMgrEnoNone

	case inst.state == TabInstStateQuering && result == TabMgrEnoFindNodeFailed:
		return fnFailUpdate()

	case (inst.state == TabInstStateQuering || inst.state == TabInstStateQTimeout) &&
		result == TabMgrEnoTimeout:
		return fnFailUpdate()
	}

	log.Debugf("tabUpdateNodeDb4Query: invalid context, state: %d, result: %d",
		inst.state, result)
	return TabMgrEnoInternal
}

func (tabMgr *TableManager) tabUpdateNodeDb4Bounding(pn *Node, pit *time.Time, pot *time.Time) TabMgrErrno {
	snid := tabMgr.snid
	if node := tabMgr.nodeDb.node(snid, pn.ID); node == nil {
		if err := tabMgr.nodeDb.updateNode(snid, pn); err != nil {
			log.Debugf("tabUpdateNodeDb4Bounding: updateNode fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID))
			return TabMgrEnoDatabase
		}
	}

	if pit != nil {
		if err := tabMgr.nodeDb.updateLastPing(snid, pn.ID, *pit); err != nil {
			log.Debugf("tabUpdateNodeDb4Bounding: updateLastPing fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID))
			return TabMgrEnoDatabase
		}
	}

	if pot != nil {
		if err := tabMgr.nodeDb.updateLastPong(snid, pn.ID, *pot); err != nil {
			log.Debugf("tabUpdateNodeDb4Bounding: updateLastPong fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID))
			return TabMgrEnoDatabase
		}
	}

	fails := 0
	if err := tabMgr.nodeDb.updateFindFails(snid, pn.ID, fails); err != nil {
		log.Debugf("tabUpdateNodeDb4Bounding: fnFailUpdate: updateFindFails failed, err: %s",
			err.Error())
		return TabMgrEnoDatabase
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabUpdateBucket(inst *instCtrlBlock, result int) TabMgrErrno {
	// 1) When pingpong ok, add peer node to a bucket;
	// 2) When pingpong failed, add peer node to a bucket;
	// 3) When findnode failed counter reach the threshold, remove peer node from bucket;
	var eno TabMgrErrno

	switch {

	case inst.state == TabInstStateQuering && result == TabMgrEnoNone:
		return TabMgrEnoNone

	case (inst.state == TabInstStateQuering || inst.state == TabInstStateQTimeout) && result != TabMgrEnoNone:
		id := NodeID(inst.req.(*um.FindNode).To.NodeId)
		if eno = tabMgr.tabBucketUpdateFailCounter(id, +1); eno == TabMgrEnoRemove {
			return tabMgr.tabBucketRemoveNode(id)
		}
		return eno

	case inst.state == TabInstStateBonding && result == TabMgrEnoNone:
		node := &inst.req.(*um.Ping).To
		inst.pot = time.Now()
		return tabMgr.TabBucketAddNode(tabMgr.snid, node, &inst.qrt, &inst.pit, &inst.pot)

	case (inst.state == TabInstStateBonding || inst.state == TabInstStateBTimeout) && result != TabMgrEnoNone:
		node := &inst.req.(*um.Ping).To
		return tabMgr.TabBucketAddNode(tabMgr.snid, node, &inst.qrt, &inst.pit, nil)

	default:
		log.Debugf("tabUpdateBucket: invalid context, state: %d, result: %d",
			inst.state, result)
		return TabMgrEnoInternal
	}
}

func (tabMgr *TableManager) tabUpdateBootstarpNode(n *um.Node) TabMgrErrno {
	// Update node database while local node is a bootstrap node for an unexcepeted
	// bounding procedure: this procedure is inited by neighbor manager task when a
	// Ping or Pong message received without a responding neighbor instance can be mapped
	// to it. In this case, the neighbor manager would play the pingpong procedure (if Ping
	// received, a Pong sent firstly), and when Pong received, it is sent to here the
	// table manager task, see Ping, Pong handler in file neighbor.go for details pls.
	id := n.NodeId
	snid := tabMgr.snid
	node := Node{
		Node: config.Node{
			IP:  n.IP,
			UDP: n.UDP,
			TCP: n.TCP,
			ID:  n.NodeId,
		},
		sha: *TabNodeId2Hash(id),
	}

	if err := tabMgr.nodeDb.updateNode(snid, &node); err != nil {
		log.Debugf("tabUpdateBootstarpNode: updateNode failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}

	var now = time.Now()
	var umn = um.Node{
		IP:     node.IP,
		UDP:    node.UDP,
		TCP:    node.TCP,
		NodeId: node.ID,
	}
	return tabMgr.TabBucketAddNode(snid, &umn, &time.Time{}, &now, &now)
}

func (tabMgr *TableManager) tabStartTimer(inst *instCtrlBlock, tmt int, dur time.Duration) TabMgrErrno {
	if tmt != sch.TabRefreshTimerId && inst == nil {
		log.Debugf("tabStartTimer: invalid parameters")
		return TabMgrEnoParameter
	}

	var td = sch.TimerDescription{
		Name:  TabMgrName,
		Utid:  tmt,
		Dur:   dur,
		Extra: nil,
	}

	switch tmt {

	case sch.TabRefreshTimerId:
		td.Tmt = sch.SchTmTypePeriod
		td.Name = td.Name + "_AutoRefresh"
		td.Extra = &tabMgr.snid

	case sch.TabFindNodeTimerId:
		td.Tmt = sch.SchTmTypeAbsolute
		td.Name = td.Name + "_FindNode"
		td.Extra = inst

	case sch.TabPingpongTimerId:
		td.Tmt = sch.SchTmTypeAbsolute
		td.Name = td.Name + "_Pingpong"
		td.Extra = inst

	default:
		log.Debugf("tabStartTimer: invalid time type, type: %d", tmt)
		return TabMgrEnoParameter
	}

	var eno sch.SchErrno
	var tid int

	if tmt == sch.TabRefreshTimerId {
		if tabMgr.arfTid != sch.SchInvalidTid {
			tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, tabMgr.arfTid)
			tabMgr.arfTid = sch.SchInvalidTid
		}
	} else if inst.tid != sch.SchInvalidTid {
		tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, inst.tid)
		inst.tid = sch.SchInvalidTid
	}

	if eno, tid = tabMgr.sdl.SchSetTimer(tabMgr.ptnMe, &td); eno != sch.SchEnoNone {
		log.Debugf("tabStartTimer: SchSetTimer failed, eno: %d", eno)
		return TabMgrEnoScheduler
	}

	if tmt == sch.TabRefreshTimerId {
		tabMgr.arfTid = tid
	} else {
		inst.tid = tid
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabBucketFindNode(id NodeID) (int, int, TabMgrErrno) {
	h := TabNodeId2Hash(id)
	d := tabMgr.tabLog2Dist(tabMgr.shaLocal, *h)
	b := tabMgr.buckets[d]
	if nidx, eno := b.findNode(id); eno == TabMgrEnoNone {
		return d, nidx, TabMgrEnoNone
	}
	return -1, -1, TabMgrEnoNotFound
}

func (tabMgr *TableManager) tabBucketRemoveNode(id NodeID) TabMgrErrno {
	bidx, nidx, eno := tabMgr.tabBucketFindNode(id)
	if eno != TabMgrEnoNone {
		log.Debugf("tabBucketRemoveNode: not found, snid: %d, node: %x", tabMgr.snid, id)
		return eno
	}
	nodes := tabMgr.buckets[bidx].nodes
	log.Debugf("tabBucketRemoveNode: removed, sid: %d, ip: %s, port: %d, node: %x",
		tabMgr.snid, nodes[nidx].IP.String(), nodes[nidx].UDP, id)
	if nidx != len(nodes)-1 {
		nodes = append(nodes[0:nidx], nodes[nidx+1:]...)
	} else {
		nodes = nodes[0:nidx]
	}
	tabMgr.buckets[bidx].nodes = nodes
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabBucketUpdateFailCounter(id NodeID, delta int) TabMgrErrno {
	bidx, nidx, eno := tabMgr.tabBucketFindNode(id)
	if eno != TabMgrEnoNone {
		return eno
	}
	tabMgr.buckets[bidx].nodes[nidx].failCount += delta
	if tabMgr.buckets[bidx].nodes[nidx].failCount >= maxFindnodeFailures {
		log.Debugf("tabBucketUpdateFailCounter: threshold reached")
		return TabMgrEnoRemove
	}
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabBucketUpdateBoundTime(id NodeID, pit *time.Time, pot *time.Time) TabMgrErrno {
	bidx, nidx, eno := tabMgr.tabBucketFindNode(id)
	if eno != TabMgrEnoNone {
		return eno
	}

	if pit != nil {
		tabMgr.buckets[bidx].nodes[nidx].lastPing = *pit
	}

	if pot != nil {
		tabMgr.buckets[bidx].nodes[nidx].lastPong = *pot
	}

	return TabMgrEnoNone
}

func (b *bucket) maxFindNodeFailed(src []*bucketEntry) ([]*bucketEntry, []int) {
	if src == nil {
		return b.maxFindNodeFailed(b.nodes)
	}
	var max = 0
	var beMaxf = make([]*bucketEntry, 0)
	var idxMaxf = make([]int, 0)
	for idx, be := range src {
		if be.failCount > max {
			max = be.failCount
			beMaxf = []*bucketEntry{}
			idxMaxf = []int{}
			beMaxf = append(beMaxf, be)
			idxMaxf = append(idxMaxf, idx)
		} else if be.failCount == max {
			beMaxf = append(beMaxf, be)
			idxMaxf = append(idxMaxf, idx)
		}
	}
	return beMaxf, idxMaxf
}

func (b *bucket) findNode(id NodeID) (int, TabMgrErrno) {
	for idx, n := range b.nodes {
		if NodeID(n.ID) == id {
			return idx, TabMgrEnoNone
		}
	}
	return -1, TabMgrEnoNotFound
}

func (b *bucket) earlistAdd(src []*bucketEntry) ([]*bucketEntry, []int) {
	var earliest = time.Now()
	var beEarliest = make([]*bucketEntry, 0)
	var idxEarliest = make([]int, 0)
	for idx, be := range src {
		if be.addTime.Before(earliest) {
			earliest = be.addTime
			beEarliest = []*bucketEntry{}
			idxEarliest = []int{}
			beEarliest = append(beEarliest, be)
			idxEarliest = append(idxEarliest, idx)
		} else if be.addTime.Equal(earliest) {
			beEarliest = append(beEarliest, be)
			idxEarliest = append(idxEarliest, idx)
		}
	}
	return beEarliest, idxEarliest
}

func (b *bucket) latestAdd(src []*bucketEntry) []*bucketEntry {
	var latest = time.Time{}
	var beLatest = make([]*bucketEntry, 0)
	var idxLatest = make([]int, 0)
	for idx, be := range src {
		if be.addTime.After(latest) {
			latest = be.addTime
			beLatest = []*bucketEntry{}
			idxLatest = []int{}
			beLatest = append(beLatest, be)
			idxLatest = append(idxLatest, idx)
		} else if be.addTime.Equal(latest) {
			beLatest = append(beLatest, be)
			idxLatest = append(idxLatest, idx)
		}
	}
	return beLatest
}

func (b *bucket) eldestPong(src []*bucketEntry) ([]*bucketEntry, []int) {
	var eldest = time.Now()
	var beEldest = make([]*bucketEntry, 0)
	var idxEldest = make([]int, 0)
	for idx, be := range src {
		if be.lastPong.Before(eldest) {
			eldest = be.lastPong
			beEldest = []*bucketEntry{}
			beEldest = append(beEldest, be)
			idxEldest = append(idxEldest, idx)
		} else if be.lastPong.Equal(eldest) {
			beEldest = append(beEldest, be)
			idxEldest = append(idxEldest, idx)
		}
	}
	return beEldest, idxEldest
}

func (tabMgr *TableManager) tabBucketAddNode(n *um.Node, lastQuery *time.Time, lastPing *time.Time, lastPong *time.Time) TabMgrErrno {
	// node must be pinged can it be added into a bucket, if pong does not received
	// while adding, we set it a very old one.
	if n == nil || lastQuery == nil || lastPing == nil {
		log.Debugf("tabBucketAddNode: snid: %x, invalid parameters", tabMgr.snid)
		return TabMgrEnoParameter
	}

	log.Debugf("tabBucketAddNode: snid: %d, ip: %s, port: %d", tabMgr.snid, n.IP.String(), n.UDP)

	if lastPong == nil {
		var veryOld = time.Time{}
		lastPong = &veryOld
	}

	// locate bucket for node
	id := NodeID(n.NodeId)
	h := TabNodeId2Hash(id)
	d := tabMgr.tabLog2Dist(tabMgr.shaLocal, *h)
	b := tabMgr.buckets[d]

	// if node had been exist, update last pingpong time only
	if nidx, eno := b.findNode(id); eno == TabMgrEnoNone {
		b.nodes[nidx].lastQuery = *lastQuery
		b.nodes[nidx].lastPing = *lastPing
		b.nodes[nidx].lastPong = *lastPong
		return TabMgrEnoNone
	}

	var be = new(bucketEntry)

	// if bucket not full, insert node
	if len(b.nodes) < bucketSize {
		be.Node = config.Node{
			IP:  n.IP,
			UDP: n.UDP,
			TCP: n.TCP,
			ID:  n.NodeId,
		}

		be.sha = *TabNodeId2Hash(id)
		be.addTime = time.Now()
		be.lastPing = *lastQuery
		be.lastPing = *lastPing
		be.lastPong = *lastPong
		be.failCount = 0
		b.nodes = append([]*bucketEntry{be}, b.nodes...)

		return TabMgrEnoNone
	}

	// full, we had to kick another node out. the following order applied:
	// 1) the max find node failed
	// 2) the earliest added
	// 3) the eldest pong
	// if at last more than one nodes selected, we kick one randomly.
	var kicked []*bucketEntry = nil
	var beKickedIdx int

	var index []int
	var _index []int
	var tempIdx []int

	if kicked, index = b.maxFindNodeFailed(nil); len(kicked) == 1 {
		beKickedIdx = index[0]
		goto kickSelected
	}

	if kicked, _index = b.earlistAdd(kicked); len(kicked) == 1 {
		beKickedIdx = _index[0]
		goto kickSelected
	}
	for loop := 0; loop < len(_index); loop++ {
		tempIdx = append(tempIdx, index[loop])
	}
	index = tempIdx

	if kicked, _index = b.eldestPong(kicked); len(kicked) == 1 {
		beKickedIdx = _index[0]
		goto kickSelected
	}
	for loop := 0; loop < len(_index); loop++ {
		tempIdx = append(tempIdx, index[loop])
	}
	index = tempIdx

	beKickedIdx = rand.Int()%len(kicked)

kickSelected:

	be.Node = config.Node{
		IP:  n.IP,
		UDP: n.UDP,
		TCP: n.TCP,
		ID:  n.NodeId,
	}
	be.sha = *TabNodeId2Hash(id)
	be.addTime = time.Now()
	be.lastPing = *lastQuery
	be.lastPing = *lastPing
	be.lastPong = *lastPong
	be.failCount = 0

	if beKickedIdx == 0 {
		b.nodes[0] = be
	} else if beKickedIdx == len(b.nodes) - 1 {
		b.nodes = append([]*bucketEntry{be}, b.nodes[0:beKickedIdx]...)
	} else {
		b.nodes = append(append([]*bucketEntry{be}, b.nodes[0:beKickedIdx]...), b.nodes[beKickedIdx+1])
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabDeleteActiveQueryInst(inst *instCtrlBlock) TabMgrErrno {
	for idx, icb := range tabMgr.queryIcb {
		if icb == inst {
			if inst.tid != sch.SchInvalidTid {
				tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, inst.tid)
				inst.tid = sch.SchInvalidTid
			}
			if idx != len(tabMgr.queryIcb)-1 {
				tabMgr.queryIcb = append(tabMgr.queryIcb[0:idx], tabMgr.queryIcb[idx+1:]...)
			} else {
				tabMgr.queryIcb = tabMgr.queryIcb[0:idx]
			}
			return TabMgrEnoNone
		}
	}
	return TabMgrEnoNotFound
}

func (tabMgr *TableManager) tabActiveQueryInst() TabMgrErrno {
	if len(tabMgr.queryIcb) == TabInstQueringMax {
		log.Debugf("tabActiveQueryInst: active query table full")
		return TabMgrEnoNone
	}

	if len(tabMgr.queryPending) == 0 {
		return TabMgrEnoNone
	}

	for len(tabMgr.queryPending) > 0 && len(tabMgr.queryIcb) < TabInstQueringMax {
		p := tabMgr.queryPending[0]
		var nodes = []*Node{p.node}
		if p.node.ID == tabMgr.cfg.local.ID {
			if len(tabMgr.queryPending) > 1 {
				tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
			} else {
				tabMgr.queryPending = make([]*queryPendingEntry, 0)
			}
			continue
		}

		dup := false
		for _, qi := range tabMgr.queryIcb {
			if qi.req.(*um.FindNode).To.NodeId == p.node.ID {
				if len(tabMgr.queryPending) > 1 {
					tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
				} else {
					tabMgr.queryPending = make([]*queryPendingEntry, 0)
				}
				dup = true
				break
			}
		}

		if dup {
			continue
		}
		if eno := tabMgr.tabQuery(p.target, nodes); eno != TabMgrEnoNone {
			log.Debugf("tabActiveQueryInst: tabQuery failed, eno: %d", eno)
			return eno
		}
		if len(tabMgr.queryPending) > 1 {
			tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
		} else {
			tabMgr.queryPending = make([]*queryPendingEntry, 0)
		}
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabDeleteActiveBoundInst(inst *instCtrlBlock) TabMgrErrno {
	for idx, icb := range tabMgr.boundIcb {
		if icb == inst {
			if inst.tid != sch.SchInvalidTid {
				tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, inst.tid)
				inst.tid = sch.SchInvalidTid
			}
			if idx != len(tabMgr.boundIcb)-1 {
				tabMgr.boundIcb = append(tabMgr.boundIcb[0:idx], tabMgr.boundIcb[idx+1:]...)
			} else {
				tabMgr.boundIcb = tabMgr.boundIcb[0:idx]
			}
			return TabMgrEnoNone
		}
	}
	return TabMgrEnoNotFound
}

func (tabMgr *TableManager) tabAddPendingBoundInst(node *um.Node) TabMgrErrno {
	if len(tabMgr.boundPending) >= TabInstBPendingMax {
		log.Debugf("tabAddPendingBoundInst: pending table is full")
		return TabMgrEnoResource
	}

	for _, bp := range tabMgr.boundPending {
		if bp.ID == node.NodeId {
			return TabMgrEnoDuplicated
		}
	}

	var n = Node{
		Node: config.Node{
			IP:  node.IP,
			UDP: node.UDP,
			TCP: node.TCP,
			ID:  node.NodeId,
		},
		sha: *TabNodeId2Hash(NodeID(node.NodeId)),
	}
	tabMgr.boundPending = append(tabMgr.boundPending, &n)
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabActiveBoundInst() TabMgrErrno {
	if len(tabMgr.boundIcb) == TabInstBondingMax {
		return TabMgrEnoNone
	}
	if len(tabMgr.boundPending) == 0 {
		return TabMgrEnoNone
	}

	var dup bool
	for len(tabMgr.boundPending) > 0 && len(tabMgr.boundIcb) < TabInstBondingMax {
		var pn = tabMgr.boundPending[0]
		if pn.ID == tabMgr.cfg.local.ID {
			if len(tabMgr.boundPending) > 1 {
				tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
			} else {
				tabMgr.boundPending = make([]*Node, 0)
			}
			continue
		}

		dup = false
		for _, bi := range tabMgr.boundIcb {
			if bi.req.(*um.Ping).To.NodeId == pn.ID {
				if len(tabMgr.boundPending) > 1 {
					tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
				} else {
					tabMgr.boundPending = make([]*Node, 0)
				}
				dup = true
				break
			}
		}

		if dup {
			continue
		}

		if tabMgr.tabShouldBound(NodeID(pn.ID)) == false {
			if len(tabMgr.boundPending) > 1 {
				tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
			} else {
				tabMgr.boundPending = make([]*Node, 0)
			}
			// This neighbor is likely to be successfully connected to, see function
			// tabShouldBound for more about this pls. We report this to the discover
			// directly here and then continue.
			var umNode = um.Node{
				IP:     pn.IP,
				UDP:    pn.UDP,
				TCP:    pn.TCP,
				NodeId: pn.ID,
			}
			if eno := tabMgr.tabDiscoverResp(&umNode); eno != TabMgrEnoNone {
				log.Debugf("tabActiveBoundInst: tabDiscoverResp failed, eno: %d", eno)
			}
			continue
		}

		var req = um.Ping{
			From: um.Node{
				IP:     tabMgr.cfg.local.IP,
				UDP:    tabMgr.cfg.local.UDP,
				TCP:    tabMgr.cfg.local.TCP,
				NodeId: tabMgr.cfg.local.ID,
			},
			To: um.Node{
				IP:     pn.Node.IP,
				UDP:    pn.Node.UDP,
				TCP:    pn.Node.TCP,
				NodeId: pn.Node.ID,
			},
			FromSubNetId: tabMgr.cfg.subNetIdList,
			SubNetId:     tabMgr.snid,
			Id:           uint64(time.Now().UnixNano()),
			Expiration:   0,
			Extra:        nil,
		}

		var icb = new(instCtrlBlock)
		icb.snid = tabMgr.snid
		icb.state = TabInstStateBonding
		icb.req = &req
		icb.rsp = nil
		icb.tid = sch.SchInvalidTid
		icb.pit = time.Now()

		pot := time.Time{}
		pit := time.Now()
		if eno := tabMgr.tabBucketUpdateBoundTime(NodeID(pn.ID), &pit, &pot); eno != TabMgrEnoNone && eno != TabMgrEnoNotFound {
			log.Debugf("tabActiveBoundInst: tabBucketUpdateBoundTime failed, eno: %d", eno)
			return eno
		}

		if eno := tabMgr.tabUpdateNodeDb4Bounding(pn, &pit, &pot); eno != TabMgrEnoNone {
			log.Debugf("tabActiveBoundInst: tabUpdateNodeDb4Bounding failed, eno: %d", eno)
			return eno
		}

		schMsg := sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNgbMgr, sch.EvNblPingpongReq, &req)
		tabMgr.sdl.SchSendMessage(&schMsg)
		tabMgr.tabStartTimer(icb, sch.TabPingpongTimerId, pingpongExpiration)
		if len(tabMgr.boundPending) > 1 {
			tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
		} else {
			tabMgr.boundPending = make([]*Node, 0)
		}
		tabMgr.boundIcb = append(tabMgr.boundIcb, icb)
	}

	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabIsBootstrapNode(nodeId *config.NodeID) bool {
	for _, bn := range tabMgr.cfg.bootstrapNodes {
		if bytes.Compare(bn.ID[:], nodeId[:]) == 0 {
			return true
		}
	}
	return false
}

func (tabMgr *TableManager) tabDiscoverResp(node *um.Node) TabMgrErrno {
	if tabMgr.tabIsBootstrapNode(&node.NodeId) {
		return TabMgrEnoBootstrap
	}
	var rsp = sch.MsgTabRefreshRsp{
		Snid: tabMgr.snid,
		Nodes: []*config.Node{
			&config.Node{
				IP:  node.IP,
				UDP: node.UDP,
				TCP: node.TCP,
				ID:  node.NodeId,
			},
		},
	}
	schMsg := sch.SchMessage{}
	tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnDcvMgr, sch.EvTabRefreshRsp, &rsp)
	tabMgr.sdl.SchSendMessage(&schMsg)
	return TabMgrEnoNone
}

func (tabMgr *TableManager) tabShouldBound(id NodeID) bool {
	snid := tabMgr.snid
	if node := tabMgr.nodeDb.node(snid, id); node == nil {
		log.Tracef("tabShouldBound: not found, bounding needed")
		return true
	}
	failCnt := tabMgr.nodeDb.findFails(snid, id)
	agePong := time.Since(tabMgr.nodeDb.lastPong(snid, id))
	agePing := time.Since(tabMgr.nodeDb.lastPing(snid, id))
	needed := failCnt > 0 || agePong > nodeReboundDuration || agePing > nodeReboundDuration
	return needed
}

func (tabMgr *TableManager) tabShouldBoundDbNode(id NodeID) bool {
	return tabMgr.tabShouldBound(id)
}

func (tabMgr *TableManager) TabBucketAddNode(snid SubNetworkID, n *um.Node, lastQuery *time.Time, lastPing *time.Time, lastPong *time.Time) TabMgrErrno {
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !ok {
		log.Debugf("TabBucketAddNode: none of manager instance for subnet: %x", snid)
		return TabMgrEnoNotFound
	}
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	return mgr.tabBucketAddNode(n, lastQuery, lastPing, lastPong)
}

func (tabMgr *TableManager) TabUpdateNode(snid SubNetworkID, umn *um.Node) TabMgrErrno {
	mgr, ok := tabMgr.subNetMgrList[snid]
	if !ok {
		log.Debugf("TabUpdateNode: none of manager instance for subnet: %x", snid)
		return TabMgrEnoNotFound
	}

	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	n := Node{
		Node: config.Node{
			IP:  umn.IP,
			UDP: umn.UDP,
			TCP: umn.TCP,
			ID:  config.NodeID(umn.NodeId),
		},
		sha: *TabNodeId2Hash(NodeID(umn.NodeId)),
	}
	if err := mgr.nodeDb.updateNode(snid, &n); err != nil {
		log.Debugf("TabUpdateNode: update: updateNode failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}
	return TabMgrEnoNone
}

const Closest4Querying = 1
const Closest4Queried = 0

func (tabMgr *TableManager) TabClosest(forWhat int, target NodeID, mbs int, size int) []*Node {
	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()
	return tabMgr.tabClosest(forWhat, target, mbs, size)
}

func TabBuildNode(pn *config.Node) *Node {
	return &Node{
		Node: config.Node{
			IP:  pn.IP,
			UDP: pn.UDP,
			TCP: pn.TCP,
			ID:  config.NodeID(pn.ID),
		},
		sha: *TabNodeId2Hash(NodeID(pn.ID)),
	}
}

func (tabMgr *TableManager) TabGetSubNetId() *SubNetworkID {
	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()
	return &tabMgr.snid
}

func (tabMgr *TableManager) TabGetInstBySubNetId(snid *SubNetworkID) *TableManager {
	// should be called with the "root" manager
	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()
	if *snid != AnySubNet {
		mgr := tabMgr.subNetMgrList[*snid]
		if mgr == nil && tabMgr.cfg.bootstrapNode {
			mgr = tabMgr.subNetMgrList[AnySubNet]
		}
		return mgr
	}
	return tabMgr.subNetMgrList[AnySubNet]
}

func (tabMgr *TableManager) TabGetInstAll() *map[SubNetworkID]*TableManager {
	// should be called with the "root" manager
	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()
	return &tabMgr.subNetMgrList
}

func GetSubnetIdentity(id config.NodeID, maskBits int) (config.SubNetworkID, error) {

	//
	// caller should check the error returned, for empty {0x00,0x00} return
	// when errors happened.
	//

	if !nodeId2SubnetId {
		log.Debugf("GetSubnetIdentity: not supported")
		return SubNetworkID{}, errors.New("not supported")
	}

	const MaxSubNetMaskBits = config.SubNetIdBytes * 8
	if maskBits < 0 || maskBits > MaxSubNetMaskBits {
		return config.SubNetworkID{}, errors.New("invalid mask bits")
	} else if maskBits == 0 {
		return config.AnySubNet, nil
	}
	end := len(id) - 1
	snw := uint16((id[end-1] << 8) | id[end])
	snw = snw << uint(16-maskBits)
	snw = snw >> uint(16-maskBits)
	snid := config.SubNetworkID{
		byte((snw >> 8) & 0xff),
		byte(snw & 0xff),
	}
	return snid, nil
}

func (tabMgr *TableManager) switch2RootInst() *TableManager {

	//
	// notice: "tabMgr" must be the root instance to call this function. This is for
	// bootstrap node configed as "AnySubNet", while other nodes query/ping/pong with
	// their subnet identities. In this case, since the bootstrap node has only table
	// manager instance, it has to switch to the only one root instance. when subnets
	// are reconfigurated, the bootstarp node can ignor it and stay as an "AnySubNet".
	//

	if !switch2Root {
		log.Debugf("switch2RootInst: not supported")
		return nil
	}

	mgr := (*TableManager)(nil)
	if tabMgr.cfg.bootstrapNode {
		if tabMgr.snid == config.AnySubNet {
			mgr = tabMgr
		} else if tabMgr.snid == config.ZeroSubNet && len(tabMgr.subNetMgrList) == 1 {
			mgr = tabMgr.subNetMgrList[config.AnySubNet]
		}
	}
	return mgr
}

func (tabMgr *TableManager) switch2NatAddr(proto string) TabMgrErrno {
	// notice: in current design of p2p, same (ip, port) is applied for
	// udp and tcp, so tabMgr.pubUdpIp and tabMgr.pubTcpIp must be the
	// same.
	switch proto {
	case "udp":
		if bytes.Compare(tabMgr.cfg.local.IP, tabMgr.pubUdpIp) == 0 &&
			int(tabMgr.cfg.local.UDP) == tabMgr.pubUdpPort {
			return TabMgrEnoNone
		}
		tabMgr.cfg.local.IP = tabMgr.pubUdpIp
		tabMgr.cfg.local.UDP = uint16(tabMgr.pubUdpPort & 0xffff)
		for _, mgr := range tabMgr.subNetMgrList {
			mgr.cfg.local.IP = tabMgr.pubUdpIp
			mgr.cfg.local.UDP = uint16(tabMgr.pubUdpPort & 0xffff)
		}
		return TabMgrEnoNone
	case "tcp":
		if bytes.Compare(tabMgr.cfg.local.IP, tabMgr.pubTcpIp) == 0 &&
			int(tabMgr.cfg.local.TCP) == tabMgr.pubTcpPort {
			return TabMgrEnoNone
		}
		tabMgr.cfg.local.IP = tabMgr.pubUdpIp
		tabMgr.cfg.local.TCP = uint16(tabMgr.pubTcpPort & 0xffff)
		for _, mgr := range tabMgr.subNetMgrList {
			mgr.cfg.local.IP = mgr.cfg.local.IP
			mgr.cfg.local.UDP = uint16(tabMgr.pubUdpPort & 0xffff)
		}
		return TabMgrEnoNone
	default:
		log.Debugf("switch2NatAddr: invalid protocol: %s", proto)
	}
	return TabMgrEnoParameter
}

func (tabMgr *TableManager) pubAddrSwitchPrepare() TabMgrErrno {
	for snid, mgr := range tabMgr.subNetMgrList {
		log.Debugf("pubAddrSwitchPrepare: call subnetPubAddrSwitchPrepare, snid: %x", snid)
		if eno := mgr.subnetPubAddrSwitchPrepare(); eno != TabMgrEnoNone {
			log.Debugf("pubAddrSwitchPrepare: failed, subnet: %x", snid)
			return eno
		}
	}
	ind := sch.MsgNatPubAddrSwitchInd{
		Proto:    nat.NATP_UDP,
		FromPort: tabMgr.pubUdpPort,
		PubIp:    tabMgr.pubUdpIp,
		PubPort:  tabMgr.pubUdpPort,
	}
	msg := sch.SchMessage{}
	tabMgr.sdl.SchMakeMessage(&msg, tabMgr.ptnMe, tabMgr.ptnNgbMgr, sch.EvNatPubAddrSwitchInd, &ind)
	tabMgr.sdl.SchSendMessage(&msg)
	log.Debugf("pubAddrSwitchPrepare: EvNatPubAddrSwitchInd sent")
	return TabMgrEnoNone
}

func (tabMgr *TableManager) subnetPubAddrSwitchPrepare() TabMgrErrno {
	log.Debugf("subnetPubAddrSwitchPrepare: delete arfTid, snid: %x", tabMgr.snid)
	if tabMgr.arfTid != sch.SchInvalidTid {
		tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, tabMgr.arfTid)
		tabMgr.arfTid = sch.SchInvalidTid
	}
	tabMgr.queryPending = make([]*queryPendingEntry, 0)
	tabMgr.boundPending = make([]*Node, 0)
	for _, icb := range tabMgr.queryIcb {
		log.Debugf("subnetPubAddrSwitchPrepare: call tabDeleteActiveQueryInst, snid: %x", tabMgr.snid)
		if eno := tabMgr.tabDeleteActiveQueryInst(icb); eno != TabMgrEnoNone {
			log.Debugf("subnetPubAddrSwitchPrepare: tabDeleteActiveQueryInst failed, eno: %d", eno)
			return eno
		}
	}
	for _, icb := range tabMgr.boundIcb {
		log.Debugf("subnetPubAddrSwitchPrepare: call tabDeleteActiveBoundInst, snid: %x", tabMgr.snid)
		if eno := tabMgr.tabDeleteActiveBoundInst(icb); eno != TabMgrEnoNone {
			log.Debugf("subnetPubAddrSwitchPrepare: tabDeleteActiveBoundInst failed, eno: %d", eno)
			return eno
		}
	}
	return TabMgrEnoNone
}

/*
 * kinds of private ip address are listed as bellow. when nat type "pmp" is configured
 * but no gateway ip is set, we had to guess the gatway ip as: b1.b2.b3.1 or b1.b2.1.1
 * see bellow please.
 *
 *	type	IP								CIDR
 * ==========================================================
 * 	A		10.0.0.0~10.255.255.255			10.0.0.0/8
 * 	B		172.16.0.0~172.31.255.255		172.16.0.0/12
 * 	C		192.168.0.0~192.168.255.255		192.168.0.0/16
 */
var _, privateCidrA, _ = net.ParseCIDR("10.0.0.0/8")
var _, privateCidrB, _ = net.ParseCIDR("172.16.0.0/12")
var _, privateCidrC, _ = net.ParseCIDR("192.168.0.0/16")
func TabCheckPrivateIp(ip net.IP) bool {
	return privateCidrA.Contains(ip) || privateCidrB.Contains(ip) || privateCidrC.Contains(ip)
}