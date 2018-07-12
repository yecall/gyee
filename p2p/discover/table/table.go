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
	"time"
	"path"
	"math/rand"
	"fmt"
	"sync"
	"crypto/sha256"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	config	"github.com/yeeco/gyee/p2p/config"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	log		"github.com/yeeco/gyee/p2p/logger"
)


//
// errno
//
const (
	TabMgrEnoNone		= iota
	TabMgrEnoConfig
	TabMgrEnoParameter
	TabMgrEnoScheduler
	TabMgrEnoDatabase
	TabMgrEnoNotFound
	TabMgrEnoDuplicated
	TabMgrEnoInternal
	TabMgrEnoFindNodeFailed
	TabMgrEnoPingpongFailed
	TabMgrEnoTimeout
	TabMgrEnoUdp
	TabMgrEnoResource
	TabMgrEnoRemove
)

type TabMgrErrno int

//
// Hash type
//
const HashLength = 32				// 32 bytes(256 bits) hash applied
const HashBits = HashLength * 8		// bits number of hash
type Hash [HashLength]byte

//
// Some constants about database(levelDb)
//
const (
	ndbVersion = 4
)

//
// Some constants about buckets, timers, ...
//
const (
	bucketSize			= 32					// max nodes can be held for one bucket
	nBuckets			= HashBits + 1			// total number of buckets
	maxBonding			= 16					// max concurrency bondings
	maxFindnodeFailures	= 5						// max FindNode failures to remove a node

	//
	// Since a bootstrap node would not dial outside, one could set a small value for the
	// following auto refresh timer cycle for it.
	//

	autoRefreshCycle	= 1 * time.Hour			// period to auto refresh
	autoBsnRefreshCycle	= autoRefreshCycle / 60	// one minute

	findNodeMinInterval	= 4 * time.Second		// min interval for two queries to same node
	findNodeExpiration	= 21 * time.Second		// should be (NgbProtoFindNodeResponseTimeout + delta)
	pingpongExpiration	= 21 * time.Second		// should be (NgbProtoPingResponseTimeout + delta)
	seedMaxCount          = 32					// wanted number of seeds
	seedMaxAge          = 5 * 24 * time.Hour	// max age can seeds be
	nodeReboundDuration	= 1 * time.Minute		// duration for a node to be rebound
	nodeAutoCleanCycle	= time.Hour				// Time period for running the expiration task.

	//
	// See constant nodeDBNodeExpiration defined in file nodedb.go for details.
	// The following constant is an alias for that and is not used currently.
	//

	nodeExpiration		= 24 * time.Hour		// Time after which an unseen node should be dropped.

)

//
// Bucket entry
//
type NodeID = config.NodeID
type NodeIdEx [config.NodeIDBytes+config.SubNetIdBytes]byte

type Node struct {
	config.Node			// our Node type
	sha			Hash	// hash from node identity
}

type bucketEntry struct {
	addTime		time.Time	// time when node added
	lastQuery	time.Time	// time when node latest queryed
	lastPing	time.Time	// time when node latest pinged
	lastPong	time.Time	// time when node pong latest received
	failCount	int			// fail to response find node request counter
	config.Node				// node
	sha			Hash		// hash of id
}

//
// bucket type
//
type bucket struct {
	nodes []*bucketEntry	// node table for a bucket
}

//
// Table task configuration
//
const (
	p2pTypeDynamic	= 0		// neighbor discovering needed
	p2pTypeStatic	= 1		// no discovering
)

type tabConfig struct {
	local			config.Node		// local node identity
	networkType		int				// p2p network type
	bootstrapNodes	[]*Node			// bootstrap nodes
	dataDir			string			// data directory
	name			string			// node name
	nodeDb			string			// node database
	bootstrapNode	bool			// bootstrap flag of local node
	subNetIdList	[]SubNetworkID	// sub network identity list. do not put the identity
									// of the local node in this list.
}

//
// Instance control block
//
const (
	TabInstStateNull	= iota	// null instance state
	TabInstStateQuering			// FindNode sent but had not been responsed yet
	TabInstStateBonding			// Ping sent but hand not been responsed yet
	TabInstStateQTimeout		// query timeout
	TabInstStateBTimeout		// bound timeout
)

const (
	TabInstQPendingMax	= 16	// max nodes in pending for quering
	TabInstBPendingMax	= 128	// max nodes in pending for bounding
	TabInstQueringMax	= 8		// max concurrency quering instances
	TabInstBondingMax	= 64	// max concurrency bonding instances
)

type instCtrlBlock struct {
	snid	SubNetworkID		// sub network identity
	state	int					// instance state, see aboved consts about state pls
	req		interface{}			// request message pointer which inited this instance
	rsp		interface{}			// pointer to response message received
	tid		int					// identity of timer for response
	qrt		time.Time			// findnode sent time
	pit		time.Time			// ping sent time
	pot		time.Time			// pong received time
}

//
// FindNode pending item
//
type queryPendingEntry struct {
	node	*Node				// peer node to be queried
	target	*NodeID				// target looking for
}

//
// Table manager
//
const TabMgrName = sch.TabMgrName
type SubNetworkID = config.SubNetworkID
var ZeroSubNet = config.ZeroSubNet
var AnySubNet = config.AnySubNet

type TableManager struct {
	lock			sync.Mutex			// lock for sync
	sdl				*sch.Scheduler		// scheduler pointer
	name			string				// name
	tep				sch.SchUserTaskEp	// entry
	cfg				tabConfig			// configuration
	ptnMe			interface{}			// pointer to task node of myself
	ptnNgbMgr		interface{}			// pointer to neighbor manager task node
	ptnDcvMgr		interface{}			// pointer to discover manager task node
	shaLocal		Hash				// hash of local node identity
	buckets			[nBuckets]*bucket	// buckets
	queryIcb		[]*instCtrlBlock	// active query instance table
	boundIcb		[]*instCtrlBlock	// active bound instance table
	queryPending	[]*queryPendingEntry// pending to be queried
	boundPending	[]*Node				// pending to be bound
	dlkTab			[]int				// log2 distance lookup table for a xor byte
	refreshing		bool				// busy in refreshing now
	dataDir			string				// data directory
	arfTid			int					// auto refresh timer identity

	//
	// Notice: currently Ethereum's database interface is introduced, and
	// we had make some modification on it, see file nodedb.go for details
	// please.
	//

	nodeDb			*nodeDB				// node database object pointer

	//
	// Notice: one node can attach to multiple sub networks, and we allocate
	// a table manager for each sub network. A table manager is allocated to
	// handle the case which no sub networks are needed.
	//

	networkType		int								// network type
	snid			SubNetworkID					// sub network identity
	SubNetMgrList	map[SubNetworkID]*TableManager	// sub network manager
}

//
// Create table manager
//
func NewTabMgr() *TableManager {

	var tabMgr = TableManager {
		sdl:			nil,
		name:			TabMgrName,
		tep:			nil,
		cfg:			tabConfig{},
		ptnMe:			nil,
		ptnNgbMgr:		nil,
		ptnDcvMgr:		nil,
		shaLocal:		Hash{},
		buckets:		[nBuckets]*bucket{},
		queryIcb:		make([]*instCtrlBlock, 0, TabInstQueringMax),
		boundIcb:		make([]*instCtrlBlock, 0, TabInstBondingMax),
		queryPending:	make([]*queryPendingEntry, 0, TabInstQPendingMax),
		boundPending:	make([]*Node, 0, TabInstBPendingMax),
		dlkTab:			make([]int, 256),
		refreshing:		false,
		dataDir:		"",
		arfTid:			sch.SchInvalidTid,
		nodeDb:			nil,

		networkType:	p2pTypeDynamic,
		snid:			AnySubNet,
		SubNetMgrList:	map[SubNetworkID]*TableManager{},
	}

	tabMgr.tep = tabMgr.tabMgrProc

	return &tabMgr
}

//
// Entry point exported to shceduler
//
func (tabMgr *TableManager)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return tabMgr.tep(ptn, msg)
}

//
// Table manager entry
//
func (tabMgr *TableManager)tabMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil {
		log.LogCallerFileLine("TabMgrProc: invalid parameters")
		return TabMgrEnoParameter
	}

	var eno TabMgrErrno = TabMgrEnoNone

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = tabMgr.tabMgrPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = tabMgr.tabMgrPoweroff(ptn)

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

	default:
		log.LogCallerFileLine("TabMgrProc: invalid message: %d", msg.Id)
		return sch.SchEnoUserTask
	}

	if eno != TabMgrEnoNone {
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweron handler
//
func (tabMgr *TableManager)tabMgrPoweron(ptn interface{}) TabMgrErrno {

	var eno TabMgrErrno = TabMgrEnoNone

	if ptn == nil {
		log.LogCallerFileLine("tabMgrPoweron: invalid parameters")
		return TabMgrEnoParameter
	}

	//
	// save task node pointer, fetch scheduler pointer
	//

	tabMgr.ptnMe = ptn
	tabMgr.sdl = sch.SchGetScheduler(ptn)

	//
	// fetch configurations
	//

	if eno = tabMgr.tabGetConfig(&tabMgr.cfg); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPoweron: tabGetConfig failed, eno: %d", eno)
		return eno
	}

	//
	// if it's a static type, no table manager needed, just done the table
	// manager task and then return. so, in this case, any other must not
	// try to interact with table manager for it is not exist.
	//

	if tabMgr.networkType == p2pTypeStatic {

		log.LogCallerFileLine("tabMgrPoweron: static type, tabMgr is not needed")

		tabMgr.sdl.SchTaskDone(ptn, sch.SchEnoNone)
		return TabMgrEnoNone
	}

	//
	// prepare node database
	//

	if eno = tabMgr.tabNodeDbPrepare(); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPoweron: tabNodeDbPrepare failed, eno: %d", eno)
		return eno
	}

	//
	// build local node identity hash for neighbors finding
	//

	if eno = tabMgr.tabSetupLocalHashId(); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPoweron: tabSetupLocalHash failed, eno: %d", eno)
		return eno
	}

	//
	// preapare related task ponters
	//

	if eno = tabMgr.tabRelatedTaskPrepare(ptn); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPoweron: tabRelatedTaskPrepare failed, eno: %d", eno)
		return eno
	}

	//
	// setup the lookup table
	//

	if eno = tabSetupLog2DistanceLookupTable(tabMgr.dlkTab); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPoweron: tabSetupLog2DistanceLookupTable failed, eno: %d", eno)
		return eno
	}

	//
	// Since the system is just powered on at this moment, we start table
	// refreshing at once. Before doing this, we update the random seed for
	// the underlying.
	//

	rand.Seed(time.Now().UnixNano())
	tabMgr.refreshing = false

	//
	// setup table manager for sub networks
	//

	if tabMgr.snid != config.ZeroSubNet {
		for loop := 0; loop < cap(tabMgr.buckets); loop++ {
			b := new(bucket)
			tabMgr.buckets[loop] = b
			b.nodes = make([]*bucketEntry, 0, bucketSize)
		}
		tabMgr.SubNetMgrList[tabMgr.snid] = tabMgr
	}

	if len(tabMgr.cfg.subNetIdList) > 0 {
		if eno = tabMgr.setupSubNetTabMgr(); eno != TabMgrEnoNone {
			log.LogCallerFileLine("tabMgrPoweron: SetSubNetTabMgr failed, eno: %d", eno)
			return eno
		}
	}

	//
	// refresh all possible sub networks
	//

	var cycle = autoRefreshCycle
	if tabMgr.cfg.bootstrapNode {
		cycle = autoBsnRefreshCycle
	}

	for _, mgr := range tabMgr.SubNetMgrList {

		if eno = mgr.tabStartTimer(nil, sch.TabRefreshTimerId, cycle); eno != TabMgrEnoNone {
			log.LogCallerFileLine("tabMgrPoweron: tabStartTimer failed, eno: %d", eno)
			return eno
		}

		if eno = mgr.tabRefresh(&mgr.snid, nil); eno != TabMgrEnoNone {
			log.LogCallerFileLine("tabMgrPoweron: " +
				"tabRefresh sub network failed, eno: %d, subnet: %x",
				eno, mgr.snid)
			return eno
		}
	}

	return TabMgrEnoNone
}

//
// setup sub network managers
//
func (tabMgr *TableManager)setupSubNetTabMgr() TabMgrErrno {

	snl := tabMgr.cfg.subNetIdList

	for _, snid := range snl {

		mgr := NewTabMgr()
		*mgr = *tabMgr

		mgr.queryIcb		= make([]*instCtrlBlock, 0, TabInstQueringMax)
		mgr.boundIcb		= make([]*instCtrlBlock, 0, TabInstBondingMax)
		mgr.queryPending	= make([]*queryPendingEntry, 0, TabInstQPendingMax)
		mgr.boundPending	= make([]*Node, 0, TabInstBPendingMax)
		mgr.dlkTab			= make([]int, 256)

		mgr.snid = snid
		tabMgr.SubNetMgrList[snid] = mgr

		for loop := 0; loop < cap(mgr.buckets); loop++ {
			b := new(bucket)
			mgr.buckets[loop] = b
			b.nodes = make([]*bucketEntry, 0, bucketSize)
		}
	}

	return TabMgrEnoNone
}

//
// Poweroff handler
//
func (tabMgr *TableManager)tabMgrPoweroff(ptn interface{}) TabMgrErrno {

	if ptn == nil {
		log.LogCallerFileLine("tabMgrPoweroff: invalid parameters")
		return TabMgrEnoParameter
	}

	if tabMgr.nodeDb != nil {
		tabMgr.nodeDb.close()
		tabMgr.nodeDb = nil
	}

	tabMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)

	log.LogCallerFileLine("tabMgrPoweroff: task done")
	return TabMgrEnoScheduler
}

//
// Auto-Refresh timer handler
//
func (tabMgr *TableManager)tabMgrRefreshTimerHandler(snid *SubNetworkID)TabMgrErrno {

	if snid == nil {
		log.LogCallerFileLine("tabMgrRefreshTimerHandler: invalid parameters")
		return TabMgrEnoParameter
	}

	if mgr, ok := tabMgr.SubNetMgrList[*snid]; ok {
		return mgr.tabRefresh(snid, nil)
	}

	log.LogCallerFileLine("tabMgrRefreshTimerHandler: invalid subnet: %x", snid)
	return TabMgrEnoParameter
}

//
// Pingpong timer expired event handler
//
func (tabMgr *TableManager)tabMgrPingpongTimerHandler(inst *instCtrlBlock) TabMgrErrno {

	if inst == nil {
		log.LogCallerFileLine("tabMgrPingpongTimerHandler: invalid parameters")
		return TabMgrEnoParameter
	}

	mgr, ok := tabMgr.SubNetMgrList[inst.snid]

	if !ok {
		log.LogCallerFileLine("tabMgrPingpongTimerHandler: invalid subnet: %x", inst.snid)
		return TabMgrEnoParameter
	}

	//
	// update buckets
	//

	if eno := mgr.tabUpdateBucket(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPingpongTimerHandler: tabUpdateBucket failed, eno: %d", eno)
		return eno
	}

	//
	// delete the active instance
	//

	if eno := mgr.tabDeleteActiveBoundInst(inst); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPingpongTimerHandler: tabDeleteActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	//
	// try to active more query instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrPingpongTimerHandler: tabActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	return TabMgrEnoNone
}

//
// FindNode timer expired event handler
//
func (tabMgr *TableManager)tabMgrFindNodeTimerHandler(inst *instCtrlBlock) TabMgrErrno {

	if inst == nil {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: invalid parameters")
		return TabMgrEnoParameter
	}

	mgr, ok := tabMgr.SubNetMgrList[inst.snid]

	if !ok {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: invalid subnet: %x", inst.snid)
		return TabMgrEnoParameter
	}

	//
	// update database for the neighbor node
	//

	inst.state = TabInstStateQTimeout
	inst.rsp = nil
	
	if eno := mgr.tabUpdateNodeDb4Query(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: tabUpdateNodeDb4Query failed, eno: %d", eno)
		return eno
	}

	//
	// update buckets
	//

	if eno := mgr.tabUpdateBucket(inst, TabMgrEnoTimeout); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: tabUpdateBucket failed, eno: %d", eno)
		return eno
	}

	//
	// delete the active instance
	//

	if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: tabDeleteActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	//
	// try to active more query instances
	//

	if eno := mgr.tabActiveQueryInst(); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabMgrFindNodeTimerHandler: tabActiveQueryInst failed, eno: %d", eno)
		return eno
	}

	return TabMgrEnoNone
}

//
// Refresh request handler
//
func (tabMgr *TableManager)tabMgrRefreshReq(msg *sch.MsgTabRefreshReq)TabMgrErrno {
	log.LogCallerFileLine("tabMgrRefreshReq: requst to refresh table ...")
	return tabMgr.tabRefresh(&msg.Snid, nil)
}

//
// FindNode response handler
//
func (tabMgr *TableManager)tabMgrFindNodeRsp(msg *sch.NblFindNodeRsp)TabMgrErrno {

	if msg == nil {
		log.LogCallerFileLine("tabMgrFindNodeRsp: invalid parameters")
		return TabMgrEnoParameter
	}

	snid := msg.FindNode.SubNetId
	mgr, ok := tabMgr.SubNetMgrList[snid]

	if !ok {
		return TabMgrEnoNotFound
	}

	//
	// lookup active instance for the response
	//

	var inst *instCtrlBlock = nil

	inst = mgr.tabFindInst(&msg.FindNode.To, TabInstStateQuering)
	if inst == nil {
		log.LogCallerFileLine("tabMgrFindNodeRsp: instance not found, subnet: %x, id: %X",
			snid, msg.FindNode.To.NodeId)
		return TabMgrEnoNotFound
	}

	inst.rsp = msg

	//
	// Obtain result. notice: if the result responed is "duplicated", we just need
	// to delete the duplicated active query instance and try to activate more.
	//

	var result = msg.Result & 0xffff

	if result == TabMgrEnoDuplicated {

		//
		// delete the active instance
		//

		if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {

			log.LogCallerFileLine("tabMgrFindNodeRsp: " +
				"tabDeleteActiveQueryInst failed, eno: %d, subnet: %x",
				eno, snid)

			return eno
		}

		//
		// try to active more query instances
		//

		if eno := mgr.tabActiveQueryInst(); eno != TabMgrEnoNone {

			log.LogCallerFileLine("tabMgrFindNodeRsp: " +
				"tabActiveQueryInst failed, eno: %d, subnet: %x",
				eno, snid)

			return eno
		}

		return TabMgrEnoNone
	}

	//
	// update database for the neighbor node.
	// DON'T care the result, we must go ahead to remove the instance,
	// see bellow.
	//

	if eno := mgr.tabUpdateNodeDb4Query(inst, result); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrFindNodeRsp: " +
			"tabUpdateNodeDb4Query failed, eno: %d, subnet: %x",
			eno, snid)
	}

	//
	// update buckets：DON'T care the result, we must go ahead to remove the instance,
	// see bellow.
	//

	mgr.tabUpdateBucket(inst, result)

	//
	// delete the active instance
	//

	if eno := mgr.tabDeleteActiveQueryInst(inst); eno != TabMgrEnoNone {
		return eno
	}

	//
	// try to active more query instances
	//

	mgr.tabActiveQueryInst()

	//
	// check result reported, if it's failed, need not go further
	//

	if result != 0 {
		return TabMgrEnoNone
	}

	//
	// deal with the peer and those neighbors the peer reported, we add them into the
	// BOUND pending queue for bounding, see bellow pls.
	//

	mgr.tabAddPendingBoundInst(&msg.Neighbors.From)

	for _, node := range msg.Neighbors.Nodes {
		if eno := mgr.tabAddPendingBoundInst(node); eno != TabMgrEnoNone {
			break
		}
	}

	//
	// try to active more BOUND instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {
		return eno
	}

	return TabMgrEnoNone
}

//
// Pingpong respone handler
//
func (tabMgr *TableManager)tabMgrPingpongRsp(msg *sch.NblPingRsp) TabMgrErrno {

	//
	// Lookup active instance for the response. Notice: some respons without actived
	// instances might be sent here, see file neighbor.go please. To speed up the p2p
	// network, one might push those nodes into buckets and node database, but now in
	// current implement, except the case that the local node is a bootstrap node, we
	// discard all pong responses without an actived instance.
	//
	// Notice: we had modify the logic to accept all pong responses. If local instance
	// if not found, we act as we are a bootstrap node, see bellow pls.
	//

	if msg == nil {
		log.LogCallerFileLine("tabMgrPingpongRsp: invalid parameters")
		return TabMgrEnoParameter
	}

	snid := msg.Ping.SubNetId
	mgr, ok := tabMgr.SubNetMgrList[snid]
	if !ok {
		return TabMgrEnoNotFound
	}

	var inst *instCtrlBlock = nil
	inst = mgr.tabFindInst(&msg.Ping.To, TabInstStateBonding)

	if inst == nil {

		//
		// Instance not found, check if the local is a bootstarp node
		//

		if mgr.cfg.bootstrapNode == false {
			return TabMgrEnoNotFound
		}

		//
		// Check result response, if it's failed, do nothing
		//

		if msg.Result != 0 {
			return TabMgrEnoNone
		}

		if msg.Pong == nil {
			return TabMgrEnoInternal
		}

		return mgr.tabUpdateBootstarpNode(&msg.Pong.From)
	}

	inst.rsp = msg

	//
	// Obtain result
	//

	var result = msg.Result
	if result != 0 { result = TabMgrEnoPingpongFailed }

	//
	// Update buckets, we should not return when function tabUpdateBucket return
	// failed, for some nodes might not be added into any buckets.
	//

	_ = mgr.tabUpdateBucket(inst, result)

	//
	// delete the active instance
	//

	if eno := mgr.tabDeleteActiveBoundInst(inst); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingpongRsp: " +
			"tabDeleteActiveQueryInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// try to active more BOUND instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingpongRsp: " +
			"tabActiveBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// Check result reported
	//

	if msg.Result != 0 {
		return TabMgrEnoNone
	}

	//
	// Update last pong time
	//

	pot	:= time.Now()

	if eno := mgr.tabBucketUpdateBoundTime(NodeID(inst.req.(*um.Ping).To.NodeId), nil, &pot);
	eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingpongRsp: " +
			"tabBucketUpdateBoundTime failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// Update node database for pingpong related info
	//

	n := Node {
		Node: config.Node{
			IP:  msg.Pong.From.IP,
			UDP: msg.Pong.From.UDP,
			TCP: msg.Pong.From.TCP,
			ID:  msg.Pong.From.NodeId,
		},
		sha: *TabNodeId2Hash(NodeID(msg.Pong.From.NodeId)),
	}

	if eno := mgr.tabUpdateNodeDb4Bounding(&n, nil, &pot);
		eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingpongRsp: " +
			"tabUpdateNodeDb4Bounding failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// response to the discover manager task
	//

	if eno := mgr.tabDiscoverResp(&msg.Pong.From); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingpongRsp: " +
			"tabDiscoverResp failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	return TabMgrEnoNone
}

//
// Pinged indication handler
//
func (tabMgr *TableManager)tabMgrPingedInd(ping *um.Ping) TabMgrErrno {

	if ping == nil {
		log.LogCallerFileLine("tabMgrPingedInd: invalid parameter")
		return TabMgrEnoParameter
	}

	snid := ping.SubNetId
	mgr, ok := tabMgr.SubNetMgrList[snid]
	if !ok {
		return TabMgrEnoNotFound
	}

	//
	// check if remote node should be bound
	//

	if mgr.tabShouldBound(NodeID(ping.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	//
	// add node into pending queue for bounding
	//

	if eno := mgr.tabAddPendingBoundInst(&ping.From); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingedInd: " +
			"tabAddPendingBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// try to active more bounding instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPingedInd: " +
			"tabActiveBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	return TabMgrEnoNone
}

//
// Ponged indication handler
//
func (tabMgr *TableManager)tabMgrPongedInd(pong *um.Pong) TabMgrErrno {

	if pong == nil {
		log.LogCallerFileLine("tabMgrPongedInd: invalid parameter")
		return TabMgrEnoParameter
	}

	snid := pong.SubNetId
	mgr, ok := tabMgr.SubNetMgrList[snid]
	if !ok {
		return TabMgrEnoNotFound
	}

	//
	// check if remote node should be bound
	//

	if mgr.tabShouldBound(NodeID(pong.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	//
	// add node into pending queue for bounding
	//

	if eno := mgr.tabAddPendingBoundInst(&pong.From); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPongedInd: " +
			"tabAddPendingBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// try to active more bounding instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrPongedInd: " +
			"tabActiveBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	return TabMgrEnoNone
}

//
// Queried indication handler
//
func (tabMgr *TableManager)tabMgrQueriedInd(findNode *um.FindNode) TabMgrErrno {

	if findNode == nil {
		log.LogCallerFileLine("tabMgrQueriedInd: invalid parameter")
		return TabMgrEnoParameter
	}

	snid := findNode.SubNetId
	mgr, ok := tabMgr.SubNetMgrList[snid]
	if !ok {
		return TabMgrEnoNotFound
	}

	//
	// check if remote node should be bound
	//

	if mgr.tabShouldBound(NodeID(findNode.From.NodeId)) != true {
		return TabMgrEnoNone
	}

	//
	// add node into pending queue for bounding
	//

	if eno := mgr.tabAddPendingBoundInst(&findNode.From); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrQueriedInd: " +
			"tabAddPendingBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	//
	// try to active more bounding instances
	//

	if eno := mgr.tabActiveBoundInst(); eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabMgrQueriedInd: " +
			"tabActiveBoundInst failed, eno: %d, subnet: %x",
			eno, snid)

		return eno
	}

	return TabMgrEnoNone
}



//
// Static task to keep the node database clean
//
const NdbcName = "ndbCleaner"

type NodeDbCleaner struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// name
	tep		sch.SchUserTaskEp	// entry point
	tabMgr	*TableManager		// pointer to table manager
	tid		int					// cleaner timer
}


//
// Create node database cleaner
//
func NewNdbCleaner() *NodeDbCleaner {
	var ndbCleaner = NodeDbCleaner{
		name: NdbcName,
		tep:  nil,
		tid:  sch.SchInvalidTid,
	}

	ndbCleaner.tep = ndbCleaner.ndbcProc
	return &ndbCleaner
}

//
// Entry point exported to shceduler
//
func (ndbc *NodeDbCleaner)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return ndbc.tep(ptn, msg)
}

//
// NodeDb cleaner entry
//
func (ndbc *NodeDbCleaner)ndbcProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil {
		log.LogCallerFileLine("NdbcProc: invalid parameters")
		return TabMgrEnoParameter
	}

	var eno TabMgrErrno

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = ndbc.ndbcPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = ndbc.ndbcPoweroff(ptn)

	case sch.EvNdbCleanerTimer:
		eno = ndbc.ndbcAutoCleanTimerHandler()

	default:
		log.LogCallerFileLine("NdbcProc: invalid message: %d", msg.Id)
		return sch.SchEnoInternal
	}

	if eno != TabMgrEnoNone {
		log.LogCallerFileLine("NdbcProc: errors, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Pwoeron handler
//
func (ndbc *NodeDbCleaner)ndbcPoweron(ptn interface{}) TabMgrErrno {

	if ptn == nil {
		log.LogCallerFileLine("ndbcPoweron: invalid parameters")
		return TabMgrEnoParameter
	}

	ndbc.sdl = sch.SchGetScheduler(ptn)
	ndbc.tabMgr = ndbc.sdl.SchGetUserTaskIF(TabMgrName).(*TableManager)

	var tmd  = sch.TimerDescription {
		Name:	NdbcName + "_autoclean",
		Utid:	0,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	nodeAutoCleanCycle,
		Extra:	nil,
	}

	var (
		eno	sch.SchErrno
		tid int
	)

	if eno, tid = ndbc.sdl.SchSetTimer(ptn, &tmd); eno != sch.SchEnoNone {
		return TabMgrEnoScheduler
	}

	ndbc.tid = tid

	return TabMgrEnoNone
}

//
// Poweroff handler
//
func (ndbc *NodeDbCleaner)ndbcPoweroff(ptn interface{}) TabMgrErrno {

	if ptn == nil {
		log.LogCallerFileLine("ndbcPoweroff: invalid parameters")
		return TabMgrEnoParameter
	}

	if ndbc.tid != sch.SchInvalidTid {
		ndbc.sdl.SchKillTimer(ptn, ndbc.tid)
		ndbc.tid = sch.SchInvalidTid
	}

	ndbc.sdl.SchTaskDone(ptn, sch.SchEnoKilled)

	return TabMgrEnoNone
}

//
// Auto clean timer handler
//
func (ndbc *NodeDbCleaner)ndbcAutoCleanTimerHandler() TabMgrErrno {

	//
	// Carry out cleanup procedure
	//

	log.LogCallerFileLine("ndbcAutoCleanTimerHandler: " +
		"auto cleanup timer expired, it's time to clean ...")

	err := ndbc.tabMgr.nodeDb.expireNodes()

	if err != nil {

		log.LogCallerFileLine("ndbcAutoCleanTimerHandler: " +
			"cleanup failed, err: %s",
			err.Error())

		return TabMgrEnoDatabase
	}

	log.LogCallerFileLine("ndbcAutoCleanTimerHandler: cleanup ok")

	return TabMgrEnoNone
}

//
// Fetch configuration
//
func (tabMgr *TableManager)tabGetConfig(tabCfg *tabConfig) TabMgrErrno {

	if tabCfg == nil {
		log.LogCallerFileLine("tabGetConfig: invalid parameters")
		return TabMgrEnoParameter
	}

	if tabCfg == nil {
		log.LogCallerFileLine("tabGetConfig: invalid parameter(s)")
		return TabMgrEnoParameter
	}

	cfg := config.P2pConfig4TabManager(tabMgr.sdl.SchGetP2pCfgName())
	if cfg == nil {
		log.LogCallerFileLine("tabGetConfig: P2pConfig4TabManager failed")
		return TabMgrEnoConfig
	}

	tabCfg.local			= cfg.Local
	tabCfg.dataDir			= cfg.DataDir
	tabCfg.name				= cfg.Name
	tabCfg.nodeDb			= cfg.NodeDB
	tabCfg.bootstrapNode	= cfg.BootstrapNode
	tabCfg.subNetIdList		= cfg.SubNetIdList

	tabCfg.bootstrapNodes = make([]*Node, len(cfg.BootstrapNodes))
	for idx, n := range cfg.BootstrapNodes {
		tabCfg.bootstrapNodes[idx] = new(Node)
		tabCfg.bootstrapNodes[idx].Node = *n
		tabCfg.bootstrapNodes[idx].sha = *TabNodeId2Hash(NodeID(n.ID))
	}

	//
	// for static network type, table manager is not applied; a table manager with sub
	// network identity as config.ZeroSubNet means that this manager is not applied to
	// any real service activities.
	//

	tabMgr.networkType = cfg.NetworkType
	if tabMgr.networkType == config.P2pNewworkTypeStatic {
		tabMgr.snid = config.ZeroSubNet
	} else if tabMgr.networkType == config.P2pNewworkTypeDynamic && len(tabCfg.subNetIdList) == 0 {
		tabMgr.snid = AnySubNet
	} else {
		tabMgr.snid = config.ZeroSubNet
	}

	return TabMgrEnoNone
}

//
// Prepare node database when poweron
//
func (tabMgr *TableManager)tabNodeDbPrepare() TabMgrErrno {
	if tabMgr.nodeDb != nil {
		log.LogCallerFileLine("tabNodeDbPrepare: node database had been opened")
		return TabMgrEnoDatabase
	}

	dbPath := path.Join(tabMgr.cfg.dataDir, tabMgr.cfg.name, tabMgr.cfg.nodeDb)
	db, err := newNodeDB(dbPath, ndbVersion, NodeID(tabMgr.cfg.local.ID))
	if err != nil {
		log.LogCallerFileLine("tabNodeDbPrepare: newNodeDB failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}
	tabMgr.nodeDb = db

	return TabMgrEnoNone
}

//
// Node identity to sha
//
func TabNodeId2Hash(id NodeID) *Hash {
	h := sha256.Sum256(id[:])
	return (*Hash)(&h)
}

//
// Setup local node id hash
//
func (tabMgr *TableManager)tabSetupLocalHashId() TabMgrErrno {
	if cap(tabMgr.shaLocal) != 32 {
		log.LogCallerFileLine("tabSetupLocalHashId: hash identity should be 32 bytes")
		return TabMgrEnoParameter
	}
	var h = TabNodeId2Hash(NodeID(tabMgr.cfg.local.ID))
	tabMgr.shaLocal = *h
	return TabMgrEnoNone
}

//
// Prepare pointers to related tasks
//
func (tabMgr *TableManager)tabRelatedTaskPrepare(ptnMe interface{}) TabMgrErrno {

	if ptnMe == nil {
		log.LogCallerFileLine("tabRelatedTaskPrepare: invalid parameters")
		return TabMgrEnoParameter
	}

	var eno = sch.SchEnoNone

	if eno, tabMgr.ptnNgbMgr = tabMgr.sdl.SchGetTaskNodeByName(sch.NgbMgrName); eno != sch.SchEnoNone {
		log.LogCallerFileLine("tabRelatedTaskPrepare: " +
			"get task node failed, name: %s", sch.NgbMgrName)
		return TabMgrEnoScheduler
	}
	if eno, tabMgr.ptnDcvMgr = tabMgr.sdl.SchGetTaskNodeByName(sch.DcvMgrName); eno != sch.SchEnoNone {
		log.LogCallerFileLine("tabRelatedTaskPrepare: " +
			"get task node failed, name: %s", sch.DcvMgrName)
		return TabMgrEnoScheduler
	}
	if tabMgr.ptnMe == nil || tabMgr.ptnNgbMgr == nil || tabMgr.ptnDcvMgr == nil {
		log.LogCallerFileLine("tabRelatedTaskPrepare: invaid task node pointer")
		return TabMgrEnoInternal
	}
	return TabMgrEnoNone
}

//
// Setup lookup table for bytes
//
func tabSetupLog2DistanceLookupTable(lkt []int) TabMgrErrno {
	var n uint
	var b uint
	lkt[0] = 8
	for n = 0; n < 8; n++ {
		for b = 1<<n; b < 1<<(n+1); b++ {
			lkt[b] = int(8 - n - 1)
		}
	}
	return TabMgrEnoNone
}

//
// Init a refreshing procedure
//
func (tabMgr *TableManager)tabRefresh(snid *SubNetworkID, tid *NodeID) TabMgrErrno {

	//
	// If we are in refreshing, return at once. When the pending table for query
	// is empty, this flag is set to false;
	//
	// If the "tid"(target identity) passed in is nil, we get a random one;
	//

	//
	// Check if the active query instances table full. notice that if it's false,
	// then the pending table must be empty in current implement.
	//

	tabMgr.refreshing = len(tabMgr.queryIcb) >= TabInstQueringMax
	if tabMgr.refreshing == true {
		log.LogCallerFileLine("tabRefresh: already in refreshing")
		return TabMgrEnoNone
	}

	//
	// If nil target passed in, we get a random one
	//

	var nodes []*Node
	var target NodeID

	if tid == nil {
		rand.Read(target[:])
	} else {
		target = *tid
	}

	if nodes = tabMgr.tabClosest(Closest4Querying, target, TabInstQPendingMax); len(nodes) == 0 {

		//
		// Here all our buckets are empty, we then apply our local node as
		// the target.
		//

		log.LogCallerFileLine("tabRefresh: seems all buckets are empty, " +
			"set local as target and try seeds from database and bootstrap nodes ...")

		target = NodeID(tabMgr.cfg.local.ID)

		seeds := tabMgr.tabSeedsFromDb(TabInstQPendingMax, seedMaxAge)
		var seedsBackup = make([]*Node, 0)

		if len(seeds) == 0 {

			log.LogCallerFileLine("tabRefresh: empty seeds set from nodes database")

		} else {

			//
			// Check if seeds from database need to be bound, if false, means
			// that those seeds can be connected to without bounding procedure,
			// we report them to discover task to speed up our p2p network.
			//

			for _, dbn := range seeds {

				if tabMgr.tabShouldBoundDbNode(NodeID(dbn.ID)) == false {

					var umNode = um.Node {
						IP:		dbn.IP,
						UDP:	dbn.UDP,
						TCP:	dbn.TCP,
						NodeId:	dbn.ID,
					}

					if eno := tabMgr.tabDiscoverResp(&umNode); eno != TabMgrEnoNone {
						log.LogCallerFileLine("tabRefresh: " +
							"tabDiscoverResp failed, eno: %d",
							eno)
					}

					continue
				}

				seedsBackup = append(seedsBackup, dbn)
			}
		}

		nodes = append(nodes, tabMgr.cfg.bootstrapNodes...)
		nodes = append(nodes, seedsBackup...)

		if len(nodes) == 0 {
			log.LogCallerFileLine("tabRefresh: we can't do refreshing without any seeds")
			return TabMgrEnoResource
		}

		if len(nodes) > TabInstQPendingMax {

			log.LogCallerFileLine("tabRefresh: " +
				"too much seeds, truncated: %d",
				len(nodes) - TabInstQPendingMax)

			nodes = nodes[:TabInstQPendingMax]
		}
	}

	var eno TabMgrErrno

	if eno := tabMgr.tabQuery(&target, nodes); eno != TabMgrEnoNone {
		log.LogCallerFileLine("tabRefresh: tabQuery failed, eno: %d", eno)
	} else {
		tabMgr.refreshing = true
	}

	return eno
}

//
// Caculate the distance between two nodes.
// Notice: the return "d" more larger, it's more closer
//
func (tabMgr *TableManager)tabLog2Dist(h1 Hash, h2 Hash) int {
	var d = 0
	for i, b := range h2 {
		delta := tabMgr.dlkTab[h1[i] ^ b]
		d += delta
		if delta != 8 {
			break
		}
	}
	return d
}

//
// Get nodes closest to target
//
func (tabMgr *TableManager)tabClosest(forWhat int, target NodeID, size int) []*Node {

	//
	// Notice: in this function, we got []*Node with a approximate order,
	// since we do not sort the nodes in the first bank, see bellow pls.
	//

	var closest = make([]*Node, 0, maxBonding)
	var count = 0

	if size == 0 || size > maxBonding {

		log.LogCallerFileLine("tabClosest: " +
			"invalid size: %d, min: 1, max: %d",
			size, maxBonding)

		return nil
	}

	ht := TabNodeId2Hash(target)
	dt := tabMgr.tabLog2Dist(tabMgr.shaLocal, *ht)

	var addClosest = func (bk *bucket) int {

		count = len(closest)

		if bk != nil {

			for _, ne := range bk.nodes {

				//
				// if we are fetching nodes to which we would query, we need to check the time
				// we had queried them last time eo escape the case to query too frequency.
				//

				if forWhat == Closest4Querying {
					if time.Now().Sub(ne.lastQuery) < findNodeMinInterval {
						continue
					}
				}

				closest = append(closest, &Node{
					Node: ne.Node,
					sha:  ne.sha,
				})

				if count++; count >= size {
					break
				}
			}
		}

		return count
	}

	//
	// the most closest bank: one should sort nodes in this bank if accurate
	// order by log2 distance against the target node is expected, but we not.
	//

	if bk := tabMgr.buckets[dt]; bk != nil {
		if addClosest(bk) >= size {
			return closest
		}
	}

	//
	// the second closest bank
	//

	for loop := dt + 1; loop < cap(tabMgr.buckets); loop++ {
		if bk := tabMgr.buckets[loop]; bk != nil {
			if addClosest(bk) >= size {
				return closest
			}
		}
	}

	if dt <= 0 { return closest }

	//
	// the last bank
	//

	for loop := dt - 1; loop >= 0; loop-- {
		if bk := tabMgr.buckets[loop]; bk != nil {
			if addClosest(bk) >= size {
				return closest
			}
		}
	}

	return closest
}

//
// Fetch seeds from node database
//
func (tabMgr *TableManager)tabSeedsFromDb(size int, age time.Duration) []*Node {

	if size == 0 {
		log.LogCallerFileLine("tabSeedsFromDb: invalid zero size")
		return nil
	}

	if size > seedMaxCount { size = seedMaxCount }
	if age > seedMaxAge { age = seedMaxAge }
	nodes := tabMgr.nodeDb.querySeeds(tabMgr.snid, size, age)

	if nodes == nil {
		log.LogCallerFileLine("tabSeedsFromDb: nil nodes")
		return nil
	}

	if len(nodes) == 0 {
		log.LogCallerFileLine("tabSeedsFromDb: empty node table")
		return nil
	}

	if len(nodes) > size {
		nodes = nodes[0:size]
	}

	return nodes
}

//
// Query nodes
//
func (tabMgr *TableManager)tabQuery(target *NodeID, nodes []*Node) TabMgrErrno {

	//
	// check: since we apply doing best to active more, it's impossible that the active
	// table is not full while the pending table is not empty.
	//

	if target == nil || nodes == nil {
		log.LogCallerFileLine("tabQuery: invalid parameters")
		return TabMgrEnoParameter
	}

	remain := len(nodes)
	actNum := len(tabMgr.queryIcb)

	if remain == 0 {
		log.LogCallerFileLine("tabQuery: invalid parameters, no node to be handled")
		return TabMgrEnoParameter
	}

	//
	// create query instances
	//

	var schMsg = sch.SchMessage{}
	var loop = 0
	var dup bool

	if actNum < TabInstQueringMax {

		for ; loop < remain && actNum < TabInstQueringMax; loop++ {

			//
			// check not to query ourselves
			//

			if nodes[loop].ID == tabMgr.cfg.local.ID {
				continue
			}

			//
			// Check not to query duplicated
			//

			dup = false

			for _, qi := range tabMgr.queryIcb {
				if qi.req.(*um.FindNode).To.NodeId == nodes[loop].ID {
					dup = true
					break
				}
			}

			if dup { continue }

			//
			// do query
			//

			msg := new(um.FindNode)
			icb := new(instCtrlBlock)

			icb.snid	= tabMgr.snid
			icb.state	= TabInstStateQuering
			icb.qrt		= time.Now()
			icb.req		= msg
			icb.rsp		= nil
			icb.tid		= sch.SchInvalidTid

			msg.From = um.Node{
				IP:		tabMgr.cfg.local.IP,
				UDP:	tabMgr.cfg.local.UDP,
				TCP:	tabMgr.cfg.local.TCP,
				NodeId:	tabMgr.cfg.local.ID,
			}

			msg.To = um.Node{
				IP:     nodes[loop].IP,
				UDP:    nodes[loop].UDP,
				TCP:    nodes[loop].TCP,
				NodeId: nodes[loop].ID,
			}

			msg.FromSubNetId	= tabMgr.cfg.subNetIdList
			msg.SubNetId		= tabMgr.snid
			msg.Target			= config.NodeID(*target)
			msg.Id				= uint64(time.Now().UnixNano())
			msg.Expiration		= 0
			msg.Extra			= nil

			tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNgbMgr, sch.EvNblFindNodeReq, msg)
			tabMgr.sdl.SchSendMessage(&schMsg)

			if eno := tabMgr.tabStartTimer(icb, sch.TabFindNodeTimerId, findNodeExpiration); eno != TabMgrEnoNone {
				log.LogCallerFileLine("tabQuery: tabStartTimer failed, eno: %d", eno)
				return eno
			}

			tabMgr.queryIcb = append(tabMgr.queryIcb, icb)
			actNum++
		}
	}

	//
	// append nodes to pending table if any
	//

	for ; loop < remain; loop++ {

		if  len(tabMgr.queryPending) >= TabInstQPendingMax {
			log.LogCallerFileLine("tabQuery: pending query table full")
			break
		}

		//
		// check not to query ourselves
		//

		if nodes[loop].ID == tabMgr.cfg.local.ID {

			continue
		}

		//
		// Check not to query duplicated
		//

		dup = false

		for _, qp := range tabMgr.queryPending {

			if qp.node.ID == nodes[loop].ID {

				dup = true
				break
			}
		}

		if dup { continue }

		//
		// Append to query pending
		//

		tabMgr.queryPending = append(tabMgr.queryPending, &queryPendingEntry{
			node:nodes[loop],
			target: target,
		})
	}

	return TabMgrEnoNone
}

//
// Find active instance by node
//
func (tabMgr *TableManager)tabFindInst(node *um.Node, state int) *instCtrlBlock {

	if node == nil {
		log.LogCallerFileLine("tabFindInst: invalid parameters")
		return nil
	}

	if node == nil || (state != TabInstStateQuering &&
						state != TabInstStateBonding &&
						state != TabInstStateQTimeout &&
						state != TabInstStateBTimeout) {
		log.LogCallerFileLine("tabFindInst: invalid parameters")
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

//
// Update node database for FindNode procedure
//
func (tabMgr *TableManager)tabUpdateNodeDb4Query(inst *instCtrlBlock, result int) TabMgrErrno {

	//
	// The logic:
	//
	// 1) Update sending ping request time;
	// 2) Update receiving pong response time if we receive it indeed;
	// 3) Update find node failed counter;
	// 4) When pingpong ok, init the peer node record totally;
	// 5) When pingpong failed, if the node had been recorded in database, clear
	// the find node failed counter to be zero;
	//
	// Notice: in current implement, peer node records would be removed from node
	// database just by cleaner task, which checks if conditions are fullfilled to
	// do that, see it pls.
	//

	if inst == nil {
		log.LogCallerFileLine("tabUpdateNodeDb4Query: invalid parameters")
		return TabMgrEnoParameter
	}

	snid := tabMgr.snid

	var fnFailUpdate = func() TabMgrErrno {

		id := NodeID(inst.req.(*um.FindNode).To.NodeId)

		if node := tabMgr.nodeDb.node(snid, id); node == nil {

			log.LogCallerFileLine("tabUpdateNodeDb4Query: " +
				"fnFailUpdate: node not exist, do nothing")

			return TabMgrEnoNone
		}

		fails := tabMgr.nodeDb.findFails(snid, id) + 1

		if err := tabMgr.nodeDb.updateFindFails(snid, id, fails); err != nil {

			log.LogCallerFileLine("tabUpdateNodeDb4Query: " +
				"fnFailUpdate: updateFindFails failed, err: %s",
				err.Error())

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

	default:
		log.LogCallerFileLine("tabUpdateNodeDb4Query: " +
			"invalid context, update nothing, state: %d, result: %d",
			inst.state, result)

		return TabMgrEnoInternal
	}

	return TabMgrEnoInternal
}

//
// Update node database for Pingpong procedure
//
func (tabMgr *TableManager)tabUpdateNodeDb4Bounding(pn *Node, pit *time.Time, pot *time.Time) TabMgrErrno {

	snid := tabMgr.snid

	if node := tabMgr.nodeDb.node(snid, pn.ID); node == nil {

		if err := tabMgr.nodeDb.updateNode(snid, pn); err != nil {

			log.LogCallerFileLine("tabUpdateNodeDb4Bounding: " +
				"updateNode fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID)	)

			return TabMgrEnoDatabase
		}

	}

	if pit != nil {
		if err := tabMgr.nodeDb.updateLastPing(snid, pn.ID, *pit); err != nil {

			log.LogCallerFileLine("tabUpdateNodeDb4Bounding: " +
				"updateLastPing fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID)	)

			return TabMgrEnoDatabase
		}
	}

	if pot != nil {
		if err := tabMgr.nodeDb.updateLastPong(snid, pn.ID, *pot); err != nil {

			log.LogCallerFileLine("tabUpdateNodeDb4Bounding: " +
				"updateLastPong fialed, err: %s, node: %s",
				err.Error(), fmt.Sprintf("%X", pn.ID)	)

			return TabMgrEnoDatabase
		}
	}

	fails := 0
	if err := tabMgr.nodeDb.updateFindFails(snid, pn.ID, fails); err != nil {

		log.LogCallerFileLine("tabUpdateNodeDb4Bounding: " +
			"fnFailUpdate: updateFindFails failed, err: %s",
			err.Error())

		return TabMgrEnoDatabase
	}

	return TabMgrEnoNone
}


// Update buckets
//
func (tabMgr *TableManager)tabUpdateBucket(inst *instCtrlBlock, result int) TabMgrErrno {

	if inst == nil {
		log.LogCallerFileLine("tabUpdateBucket: invaliNd parameters")
		return TabMgrEnoParameter
	}

	//
	// The logic:
	// 1) When pingpong ok, add peer node to a bucket;
	// 2) When pingpong failed, add peer node to a bucket;
	// 3) When findnode failed counter reach the threshold, remove peer node from bucket;
	//

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
		return tabMgr.TabBucketAddNode(node, &inst.qrt, &inst.pit, &inst.pot)

	case (inst.state == TabInstStateBonding || inst.state == TabInstStateBTimeout) && result != TabMgrEnoNone:

		node := &inst.req.(*um.Ping).To
		return tabMgr.TabBucketAddNode(node, &inst.qrt, &inst.pit, nil)

	default:

		log.LogCallerFileLine("tabUpdateBucket: " +
			"invalid context, update nothing, state: %d, result: %d",
			inst.state, result)

		return TabMgrEnoInternal
	}
}

//
// Update node database while local node is a bootstrap node for an unexcepeted
// bounding procedure: this procedure is inited by neighbor manager task when a
// Ping or Pong message recived without an responding neighbor instance can be mapped
// it. In this case, the neighbor manager then carry the pingpong procedure (if Ping
// received, a Pong sent firstly), and when Pong recvied, it is sent to here the
// table manager task, see Ping, Pong handler in file neighbor.go for details pls.
//
func (tabMgr *TableManager)tabUpdateBootstarpNode(n *um.Node) TabMgrErrno {

	if n == nil {
		log.LogCallerFileLine("tabUpdateBootstarpNode: invalid parameters")
		return TabMgrEnoParameter
	}

	id := n.NodeId
	snid := tabMgr.snid

	//
	// update node database
	//

	node := Node {
		Node: config.Node {
			IP:  n.IP,
			UDP: n.UDP,
			TCP: n.TCP,
			ID:  n.NodeId,
		},
		sha: *TabNodeId2Hash(id),
	}

	if err := tabMgr.nodeDb.updateNode(snid, &node); err != nil {
		log.LogCallerFileLine("tabUpdateBootstarpNode: updateNode failed, err: %s", err.Error())
		return TabMgrEnoDatabase
	}

	//
	// add to bucket
	//

	var now = time.Now()

	var umn = um.Node{
		IP:		node.IP,
		UDP:	node.UDP,
		TCP:	node.TCP,
		NodeId:	node.ID,
	}

	return tabMgr.TabBucketAddNode(&umn, &time.Time{}, &now, &now)
}

//
// Start timer according instance, timer type, and duration
//
func (tabMgr *TableManager)tabStartTimer(inst *instCtrlBlock, tmt int, dur time.Duration) TabMgrErrno {

	if tmt != sch.TabRefreshTimerId && inst == nil {
		log.LogCallerFileLine("tabStartTimer: invalid parameters")
		return TabMgrEnoParameter
	}

	var td = sch.TimerDescription {
		Name:	TabMgrName,
		Utid:	tmt,
		Dur:	dur,
		Extra:	nil,
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
		log.LogCallerFileLine("tabStartTimer: invalid time type, type: %d", tmt)
		return TabMgrEnoParameter
	}

	var eno sch.SchErrno
	var tid int

	if eno, tid = tabMgr.sdl.SchSetTimer(tabMgr.ptnMe, &td); eno != sch.SchEnoNone {
		return TabMgrEnoScheduler
	}

	if tmt == sch.TabRefreshTimerId {
		tabMgr.arfTid = tid
	} else {
		inst.tid = tid
	}

	return TabMgrEnoNone
}

//
// Find node in buckets
//
func (tabMgr *TableManager)tabBucketFindNode(id NodeID) (int, int, TabMgrErrno) {

	h := TabNodeId2Hash(id)
	d := tabMgr.tabLog2Dist(tabMgr.shaLocal, *h)
	b := tabMgr.buckets[d]

	if nidx, eno := b.findNode(id); eno == TabMgrEnoNone {
		return d, nidx, TabMgrEnoNone
	}

	return -1, -1, TabMgrEnoNotFound
}

//
// Remove node from bucket
//
func (tabMgr *TableManager)tabBucketRemoveNode(id NodeID) TabMgrErrno {

	bidx, nidx, eno := tabMgr.tabBucketFindNode(id)
	if eno != TabMgrEnoNone {

		log.LogCallerFileLine("tabBucketRemoveNode: " +
			"not found, node: %s",
			config.P2pNodeId2HexString(config.NodeID(id)))

		return eno
	}

	nodes := tabMgr.buckets[bidx].nodes
	nodes = append(nodes[0:nidx], nodes[nidx+1:] ...)
	tabMgr.buckets[bidx].nodes = nodes

	return TabMgrEnoNone
}

//
// Update FindNode failed counter
//
func (tabMgr *TableManager)tabBucketUpdateFailCounter(id NodeID, delta int) TabMgrErrno {

	bidx, nidx, eno := tabMgr.tabBucketFindNode(id)
	if eno != TabMgrEnoNone {
		return eno
	}

	tabMgr.buckets[bidx].nodes[nidx].failCount += delta
	if tabMgr.buckets[bidx].nodes[nidx].failCount >= maxFindnodeFailures {
		log.LogCallerFileLine("tabBucketUpdateFailCounter: threshold reached")
		return TabMgrEnoRemove
	}

	return TabMgrEnoNone
}

//
// Update pingpong time
//
func (tabMgr *TableManager)tabBucketUpdateBoundTime(id NodeID, pit *time.Time, pot *time.Time) TabMgrErrno {

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

//
// Find max find node faile count
//
func (b *bucket) maxFindNodeFailed(src []*bucketEntry) ([]*bucketEntry) {

	//
	// if the source is nil, recursive then
	//

	if src == nil {
		return b.maxFindNodeFailed(b.nodes)
	}

	//
	// else, pick entries from source
	//

	var max = 0
	var beMaxf = make([]*bucketEntry, 0)

	for _, be := range src {

		if be.failCount > max {

			max = be.failCount
			beMaxf = []*bucketEntry{}

			beMaxf = append(beMaxf, be)

		} else if be.failCount == max {

			beMaxf = append(beMaxf, be)
		}
	}

	return beMaxf
}

//
// Find node in a specific bucket
//
func (b *bucket) findNode(id NodeID) (int, TabMgrErrno) {

	for idx, n := range b.nodes {

		if NodeID(n.ID) == id {

			return idx, TabMgrEnoNone
		}
	}

	return -1, TabMgrEnoNotFound
}

//
// Find latest added
//
func (b *bucket) latestAdd(src []*bucketEntry) ([]*bucketEntry) {

	//
	// if the source is nil, recursive then
	//

	if src == nil {
		return b.latestAdd(b.nodes)
	}

	//
	// else, pick entries from source
	//

	var latest = time.Time{}
	var beLatest = make([]*bucketEntry, 0)

	for _, be := range src {

		if be.addTime.After(latest) {

			latest = be.addTime
			beLatest = []*bucketEntry{}
			beLatest = append(beLatest, be)

		} else if be.addTime.Equal(latest) {

			beLatest = append(beLatest, be)
		}
	}

	return beLatest
}

//
// Find latest pong
//
func (b *bucket) eldestPong(src []*bucketEntry) ([]*bucketEntry) {

	//
	// if the source is nil, recursive then
	//

	if src == nil {
		return b.eldestPong(b.nodes)
	}

	//
	// else, pick entries from source
	//

	var eldest = time.Now()
	var beEldest = make([]*bucketEntry, 0)

	for _, be := range src {

		if be.lastPong.Before(eldest) {

			eldest = be.lastPong
			beEldest = []*bucketEntry{}
			beEldest = append(beEldest, be)

		} else if be.lastPong.Equal(eldest) {

			beEldest = append(beEldest, be)
		}
	}

	return beEldest
}

//
// Add node to bucket
//
func (tabMgr *TableManager)tabBucketAddNode(n *um.Node, lastQuery *time.Time, lastPing *time.Time, lastPong *time.Time) TabMgrErrno {

	//
	// node must be pinged can it be added into a bucket, if pong does not received
	// while adding, we set a very old one.
	//

	if n == nil || lastQuery == nil || lastPing == nil {
		log.LogCallerFileLine("tabBucketAddNode: invalid parameters")
		return TabMgrEnoParameter
	}

	if lastPong == nil {
		var veryOld = time.Time{}
		lastPong = &veryOld
	}

	//
	// locate bucket for node
	//

	id := NodeID(n.NodeId)
	h := TabNodeId2Hash(id)
	d := tabMgr.tabLog2Dist(tabMgr.shaLocal, *h)
	b := tabMgr.buckets[d]

	//
	// if node had been exist, update last pingpong time only
	//

	if nidx, eno := b.findNode(id); eno == TabMgrEnoNone {

		b.nodes[nidx].lastQuery = *lastQuery
		b.nodes[nidx].lastPing = *lastPing
		b.nodes[nidx].lastPong = *lastPong

		return TabMgrEnoNone
	}

	//
	// if bucket not full, append node
	//

	if len(b.nodes) < bucketSize {

		var be= new(bucketEntry)

		be.Node = config.Node{
			IP:		n.IP,
			UDP:	n.UDP,
			TCP:	n.TCP,
			ID:		n.NodeId,
		}

		be.sha = *TabNodeId2Hash(id)
		be.addTime = time.Now()
		be.lastPing = *lastQuery
		be.lastPing = *lastPing
		be.lastPong = *lastPong
		be.failCount = 0

		b.nodes = append(b.nodes, be)

		return TabMgrEnoNone
	}

	//
	// full, we had to kick another node out. the following order applied:
	//
	// 1) the max find node failed
	// 2) the youngest added
	// 3) the eldest pong
	//
	// if at last more than one nodes selected, we kick one randomly.
	//

	var kicked []*bucketEntry = nil
	var beKicked *bucketEntry = nil

	if kicked = b.maxFindNodeFailed(nil); len(kicked) == 1 {
		beKicked = kicked[0]
		goto kickSelected
	}

	if kicked := b.latestAdd(kicked); len(kicked) == 1 {
		beKicked = kicked[0]
		goto kickSelected
	}

	if kicked := b.eldestPong(kicked); len(kicked) == 1 {
		beKicked = kicked[0]
		goto kickSelected
	}

	beKicked = kicked[rand.Int() % len(kicked)]

kickSelected:

	beKicked.Node = config.Node {
		IP:		n.IP,
		UDP:	n.UDP,
		TCP:	n.TCP,
		ID:		n.NodeId,
	}

	beKicked.sha = *TabNodeId2Hash(id)
	beKicked.addTime = time.Now()
	beKicked.lastPing = *lastQuery
	beKicked.lastPing = *lastPing
	beKicked.lastPong = *lastPong
	beKicked.failCount = 0

	return TabMgrEnoNone
}

//
// Delete active query instance
//
func (tabMgr *TableManager)tabDeleteActiveQueryInst(inst *instCtrlBlock) TabMgrErrno {

	if inst == nil {
		log.LogCallerFileLine("tabDeleteActiveQueryInst: invalid parameters")
		return TabMgrEnoParameter
	}

	for idx, icb := range tabMgr.queryIcb {

		if icb == inst {

			if inst.tid != sch.SchInvalidTid {

				tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, inst.tid)
				inst.tid = sch.SchInvalidTid
			}

			tabMgr.queryIcb = append(tabMgr.queryIcb[0:idx], tabMgr.queryIcb[idx+1:]...)

			return TabMgrEnoNone
		}
	}

	return TabMgrEnoNotFound
}

//
// Active query instance
//
func (tabMgr *TableManager)tabActiveQueryInst() TabMgrErrno {

	//
	// check if we can activate more
	//

	if len(tabMgr.queryIcb) == TabInstQueringMax {
		log.LogCallerFileLine("tabActiveQueryInst: active query table full")
		return TabMgrEnoNone
	}

	//
	// check if any pending
	//

	if len(tabMgr.queryPending) == 0 {
		return TabMgrEnoNone
	}

	//
	// activate pendings
	//

	for ; len(tabMgr.queryPending) > 0 && len(tabMgr.queryIcb) < TabInstQueringMax; {

		//
		// Check not to query ourselves
		//

		p := tabMgr.queryPending[0]
		var nodes = []*Node{p.node}

		if p.node.ID == tabMgr.cfg.local.ID {
			tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
			continue
		}

		//
		// Check not to query duplicated
		//

		for _, qi := range tabMgr.queryIcb {

			if qi.req.(*um.FindNode).To.NodeId == p.node.ID {
				tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
				continue
			}
		}

		if len(tabMgr.queryPending) <= 0 {
			break;
		}

		//
		// Do query
		//

		if eno := tabMgr.tabQuery(p.target, nodes); eno != TabMgrEnoNone {

			log.LogCallerFileLine("tabActiveQueryInst: tabQuery failed, eno: %d", eno)
			return eno
		}

		tabMgr.queryPending = append(tabMgr.queryPending[:0], tabMgr.queryPending[1:]...)
	}

	return TabMgrEnoNone
}

//
// Delete active bound instance
//
func (tabMgr *TableManager)tabDeleteActiveBoundInst(inst *instCtrlBlock) TabMgrErrno {

	if inst == nil {
		log.LogCallerFileLine("tabDeleteActiveBoundInst: invalid parameters")
		return TabMgrEnoParameter
	}

	for idx, icb := range tabMgr.boundIcb {

		if icb == inst {

			if inst.tid != sch.SchInvalidTid {

				tabMgr.sdl.SchKillTimer(tabMgr.ptnMe, inst.tid)
				inst.tid = sch.SchInvalidTid
			}

			tabMgr.boundIcb = append(tabMgr.boundIcb[0:idx], tabMgr.boundIcb[idx+1:]...)

			return TabMgrEnoNone
		}
	}

	return TabMgrEnoNotFound
}

//
// Add pending bound instance for node
//
func (tabMgr *TableManager)tabAddPendingBoundInst(node *um.Node) TabMgrErrno {

	if node == nil {
		log.LogCallerFileLine("tabAddPendingBoundInst: invalid parameters")
		return TabMgrEnoParameter
	}

	if len(tabMgr.boundPending) >= TabInstBPendingMax {
		log.LogCallerFileLine("tabAddPendingBoundInst: pending table is full")
		return TabMgrEnoResource
	}

	for _, bp := range tabMgr.boundPending {

		if bp.ID == node.NodeId {

			return TabMgrEnoDuplicated
		}
	}

	var n = Node {
		Node: config.Node {
			IP:		node.IP,
			UDP:	node.UDP,
			TCP:	node.TCP,
			ID:		node.NodeId,
		},
		sha: *TabNodeId2Hash(NodeID(node.NodeId)),
	}

	tabMgr.boundPending = append(tabMgr.boundPending, &n)

	return TabMgrEnoNone
}

//
// Active bound instance
//
func (tabMgr *TableManager)tabActiveBoundInst() TabMgrErrno {

	if len(tabMgr.boundIcb) == TabInstBondingMax {
		return TabMgrEnoNone
	}

	if len(tabMgr.boundPending) == 0 {
		return TabMgrEnoNone
	}

	//
	// Try to activate as most as possible
	//

	var dup bool

	for ; len(tabMgr.boundPending) > 0 && len(tabMgr.boundIcb) < TabInstBondingMax; {

		var pn = tabMgr.boundPending[0]

		//
		// Check not to bound ourselves
		//

		if pn.ID == tabMgr.cfg.local.ID {
			tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
			continue
		}

		//
		// check duplicated
		//

		dup = false

		for _, bi := range tabMgr.boundIcb {

			if bi.req.(*um.Ping).To.NodeId == pn.ID {

				tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)
				dup = true
				break
			}
		}

		if dup { continue }

		//
		// Check if bounding needed
		//

		if tabMgr.tabShouldBound(NodeID(pn.ID)) == false {

			tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:]...)

			//
			// This neighbor is likely to be successfully connected to, see function
			// tabShouldBound for more about this pls. We report this to the discover
			// directly here and then continue.
			//

			var umNode = um.Node {
				IP:		pn.IP,
				UDP:	pn.UDP,
				TCP:	pn.TCP,
				NodeId:	pn.ID,
			}

			if eno := tabMgr.tabDiscoverResp(&umNode); eno != TabMgrEnoNone {

				log.LogCallerFileLine("tabActiveBoundInst: " +
					"tabDiscoverResp failed, eno: %d",
					eno)
			}

			continue
		}

		var req = um.Ping {
			From: um.Node {
				IP:		tabMgr.cfg.local.IP,
				UDP:	tabMgr.cfg.local.UDP,
				TCP:	tabMgr.cfg.local.TCP,
				NodeId:	tabMgr.cfg.local.ID,
			},
			To: um.Node {
				IP:		pn.Node.IP,
				UDP:	pn.Node.UDP,
				TCP:	pn.Node.TCP,
				NodeId:	pn.Node.ID,
			},
			FromSubNetId:	tabMgr.cfg.subNetIdList,
			SubNetId:		tabMgr.snid,
			Id: 			uint64(time.Now().UnixNano()),
			Expiration:		0,
			Extra:			nil,
		}

		var schMsg = sch.SchMessage{}
		tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnNgbMgr, sch.EvNblPingpongReq, &req)

		var icb = new(instCtrlBlock)

		icb.snid = tabMgr.snid
		icb.state = TabInstStateBonding
		icb.req = &req
		icb.rsp = nil
		icb.tid = sch.SchInvalidTid
		icb.pit = time.Now()

		//
		// Since we do not know what time we would be ponged, we set a very old time
		// for we believe it's more valuable at late as possible. please see function
		// tabBucketAddNode for more about this. Also notice that, at this moment,
		// the peer node might still not be add into the bucket, so calling to function
		// tabBucketUpdateBoundTime can get errno "not found", we check if it is
		// the case to ignore it.
		//

		pot	:= time.Time{}
		pit := time.Now()

		if eno := tabMgr.tabBucketUpdateBoundTime(NodeID(pn.ID), &pit, &pot);
		eno != TabMgrEnoNone && eno != TabMgrEnoNotFound {

			log.LogCallerFileLine("tabActiveBoundInst: " +
				"tabBucketUpdateBoundTime failed, eno: %d",
				eno)

			return eno
		}

		//
		// Update node database for pingpong related info
		//

		if eno := tabMgr.tabUpdateNodeDb4Bounding(pn, &pit, &pot);
		eno != TabMgrEnoNone {

			log.LogCallerFileLine("tabActiveBoundInst: " +
				"tabUpdateNodeDb4Bounding failed, eno: %d",
				eno)

			return eno
		}

		//
		// Send message to instance task to init the pingpong procedure
		//

		tabMgr.sdl.SchSendMessage(&schMsg)
		tabMgr.tabStartTimer(icb, sch.TabPingpongTimerId, pingpongExpiration)

		tabMgr.boundPending = append(tabMgr.boundPending[:0], tabMgr.boundPending[1:] ...)
		tabMgr.boundIcb = append(tabMgr.boundIcb, icb)
	}

	return TabMgrEnoNone
}

//
// Send respone to discover task for a bounded node
//
func (tabMgr *TableManager)tabDiscoverResp(node *um.Node) TabMgrErrno {

	if node == nil {
		log.LogCallerFileLine("tabDiscoverResp: invalid parameter")
		return TabMgrEnoParameter
	}

	var rsp = sch.MsgTabRefreshRsp {
		Snid:	tabMgr.snid,
		Nodes: []*config.Node {
			&config.Node {
				IP:		node.IP,
				UDP:	node.UDP,
				TCP:	node.TCP,
				ID:		node.NodeId,
			},
		},
	}

	var schMsg = sch.SchMessage{}
	tabMgr.sdl.SchMakeMessage(&schMsg, tabMgr.ptnMe, tabMgr.ptnDcvMgr, sch.EvTabRefreshRsp, &rsp)
	tabMgr.sdl.SchSendMessage(&schMsg)

	return TabMgrEnoNone
}

//
// Check should bounding procedure inited for a node
//
func (tabMgr *TableManager)tabShouldBound(id NodeID) bool {

	//
	// If node specified not found in database, bounding needed
	//

	snid := tabMgr.snid

	if node := tabMgr.nodeDb.node(snid, id); node == nil {
		log.LogCallerFileLine("tabShouldBound: not found, bounding needed")
		return true
	}

	//
	// If find node fail counter not be zero, bounding needed
	//

	failCnt := tabMgr.nodeDb.findFails(snid, id)
	agePong := time.Since(tabMgr.nodeDb.lastPong(snid, id))
	agePing := time.Since(tabMgr.nodeDb.lastPing(snid, id))

	needed := failCnt > 0 || agePong > nodeReboundDuration || agePing > nodeReboundDuration

	return needed
}

//
// Check if node from database needs to be bound
//
func (tabMgr *TableManager)tabShouldBoundDbNode(id NodeID) bool {
	return tabMgr.tabShouldBound(id)
}

//
// Upate node for the bucket
// Notice: inside the table manager task, this function MUST NOT be called,
// since we had obtain the lock at the entry of the task handler.
//
func (tabMgr *TableManager)TabBucketAddNode(n *um.Node, lastQuery *time.Time, lastPing *time.Time, lastPong *time.Time) TabMgrErrno {

	//
	// We would be called by other task, we need to lock and
	// defer unlock.
	//

	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()

	return tabMgr.tabBucketAddNode(n, lastQuery, lastPing, lastPong)
}


//
// Upate a node for node database.
// Notice: inside the table manager task, this function MUST NOT be called,
// since we had obtain the lock at the entry of the task handler.
//
func (tabMgr *TableManager)TabUpdateNode(umn *um.Node) TabMgrErrno {

	//
	// We would be called by other task, we need to lock and
	// defer unlock. Also notice that: calling this function
	// for a node would append new node or overwrite the exist
	// one, the FindNode fail counter would be set to zero.
	// See function tabMgr.nodeDb.updateNode for more please.
	//

	if umn == nil {
		log.LogCallerFileLine("TabUpdateNode: invalid parameter")
		return TabMgrEnoParameter
	}

	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()

	n := Node {
		Node: config.Node{
			IP:  umn.IP,
			UDP: umn.UDP,
			TCP: umn.TCP,
			ID:  config.NodeID(umn.NodeId),
		},
		sha: *TabNodeId2Hash(NodeID(umn.NodeId)),
	}

	if err := tabMgr.nodeDb.updateNode(tabMgr.snid, &n); err != nil {

		log.LogCallerFileLine("TabUpdateNode: " +
			"update: updateNode failed, err: %s",
				err.Error())

		return TabMgrEnoDatabase
	}

	return TabMgrEnoNone
}

//
// Fetch closest nodes for target
// Notice: inside the table manager task, this function MUST NOT be called,
// since we had obtain the lock at the entry of the task handler.
//
const Closest4Querying	= 1
const Closest4Queried	= 0

func (tabMgr *TableManager)TabClosest(forWhat int, target NodeID, size int) []*Node {

	//
	// We would be called by other task, we need to lock and
	// defer unlock.
	//

	tabMgr.lock.Lock()
	defer tabMgr.lock.Unlock()

	return tabMgr.tabClosest(forWhat, target, size)
}

//
// Build a table node for config.Node
//
func TabBuildNode(pn *config.Node) *Node {

	//
	// Need not to lock
	//

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

//
// Get sub network identity of manager instance
//
func (tabMgr *TableManager)TabGetSubNetId() *SubNetworkID {
	return &tabMgr.snid
}

//
// Get manager instance by sub network identity
//
func (tabMgr *TableManager)TabGetInstBySubNetId(snid *SubNetworkID) *TableManager {
	return tabMgr.SubNetMgrList[*snid]
}
