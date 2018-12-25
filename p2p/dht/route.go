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

package dht

import (
	"time"
	"bytes"
	"fmt"
	"crypto/rand"
	"container/list"
	"crypto/sha256"
	mrand	"math/rand"
	golog	"log"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type rutMgrLogger struct {
	debug__		bool
}

var rutLog = rutMgrLogger  {
	debug__:	false,
}

func (log rutMgrLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Constants
//
const (
	RutMgrName = sch.DhtRutMgrName			// Route manager name registered in scheduler
	rutMgrMaxNearest = 8					// Max nearest peers can be retrieved for a time
	rutMgrBucketSize = 32					// bucket size
	HashByteLength = config.DhtKeyLength	// 32 bytes(256 bits) hash applied
	HashBitLength = HashByteLength * 8		// hash bits
	rutMgrMaxLatency = time.Second * 60		// max latency in metric
	rutMgrMaxNofifee = 128					// max notifees could be
	rutMgrUpdate4Handshake = 0				// update for handshaking
	rutMgrUpdate4Closed = 1					// update for connection instance closed
	rutMgrUpdate4Query = 2					// update for query result
	rutMgrMaxFails2Del = 3					// max fails to be deleted
	rutMgrEwmaHisSize = 8					// history sample number
	rutMgrEwmaMF = 0.1						// memorize factor for EWMA filter
)

//
// Hash type
//
type Hash [HashByteLength]byte

//
// Latency measurement
//
type rutMgrPeerMetric struct {
	peerId		config.NodeID			// peer identity
	ltnSamples	[]time.Duration			// latency samples
	ewma		time.Duration			// exponentially-weighted moving avg
}

//
// Node in bucket
//
type rutMgrBucketNode struct {
	node	config.Node					// common node
	hash	Hash						// hash from node.ID
	dist	int							// distance between this node and local
	fails	int							// counter for peer fail to response our query
	pcs		int							// peer connection status
}

//
// Route table
//
type rutMgrRouteTable struct {
	shaLocal		Hash								// local node identity hash
	bucketSize		int									// max peers can be held in one list
	bucketTab		[]*list.List						// buckets
	metricTab		map[config.NodeID]*rutMgrPeerMetric	// metric table about peers
	maxLatency		time.Duration						// max latency
}

//
// Notifee
//

type rutMgrNotifeeId struct {
	task			interface{}			// destionation task
	target			config.DsKey		// target aimed at
}

type rutMgrNotifee struct {
	id				rutMgrNotifeeId		// notifee identity
	max				int					// max nearest asked for
	nearests		[]*rutMgrBucketNode	// nearest peers
	dists			[]int				// distances of nearest peers
}

//
// Route manager
//
type RutMgr struct {
	sdl				*sch.Scheduler							// pointer to scheduler
	name			string									// my name
	tep				sch.SchUserTaskEp						// task entry
	ptnMe			interface{}								// pointer to task node of myself
	ptnQryMgr		interface{}								// pointer to query manager task node
	ptnConMgr		interface{}								// pointer to connection manager task node
	bpCfg			bootstrapPolicy							// bootstrap policy configuration
	bpTid			int										// bootstrap policy timer identity
	bsTargets		map[config.DsKey] interface{}			// targets in bootstrapping
	distLookupTab	[]int									// log2 distance lookup table for a xor byte
	bootstrapNode	bool									// bootstrap node flag
	localNodeId		config.NodeID							// local node identity
	rutTab			rutMgrRouteTable						// route table
	ntfTab			map[rutMgrNotifeeId]*rutMgrNotifee		// notifee table
}

//
// Bootstrap policy configuration
//
type bootstrapPolicy struct {
	randomQryNum	int					// times to try query for a random peer identity
	period			time.Duration		// timer period to fire a bootstrap
}

var defautBspCfg = bootstrapPolicy {
	randomQryNum:	2,
	period:			time.Minute * 1,
}

//
// Reference to external bootstrap nodes
//
var bootstrapNodes []*config.Node = nil
func SetBootstrapNodes(bsn []*config.Node) {
	bootstrapNodes = bsn
}

//
// Create route manager
//
func NewRutMgr() *RutMgr {

	rutMgr := RutMgr {
		name:			RutMgrName,
		bpCfg:			defautBspCfg,
		bpTid:			sch.SchInvalidTid,
		bsTargets:		map[config.DsKey] interface{}{},
		distLookupTab:	make([]int, 256),
		localNodeId:	config.NodeID{},
		rutTab:			rutMgrRouteTable{},
		ntfTab:			make(map[rutMgrNotifeeId]*rutMgrNotifee, 0),
	}

	rutMgr.tep = rutMgr.rutMgrProc

	return &rutMgr
}

//
// Entry point exported to shceduler
//
func (rutMgr *RutMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return rutMgr.tep(ptn, msg)
}

//
// Route manager entry
//
func (rutMgr *RutMgr)rutMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	if ptn == nil || msg == nil {
		rutLog.Debug("rutMgrProc: " +
			"invalid parameters, ptn: %p, msg: %p",
			ptn, msg)
		return sch.SchEnoParameter
	}

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = rutMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = rutMgr.poweroff(ptn)

	case sch.EvDhtRutRefreshReq:
		eno = rutMgr.refreshReq()

	case sch.EvDhtRutBootstrapTimer:
		eno = rutMgr.bootstarpTimerHandler()

	case sch.EvDhtQryMgrQueryStartRsp:
		eno = rutMgr.queryStartRsp(msg.Body.(*sch.MsgDhtQryMgrQueryStartRsp))

	case sch.EvDhtQryMgrQueryResultInd:
		eno = rutMgr.queryResultInd(msg.Body.(*sch.MsgDhtQryMgrQueryResultInd))

	case sch.EvDhtRutMgrNearestReq:
		sender := rutMgr.sdl.SchGetSender(msg)
		eno = rutMgr.nearestReq(sender, msg.Body.(*sch.MsgDhtRutMgrNearestReq))

	case sch.EvDhtRutMgrUpdateReq:
		eno = rutMgr.updateReq(msg.Body.(*sch.MsgDhtRutMgrUpdateReq))

	case sch.EvDhtRutMgrStopNotifyReq:
		eno = rutMgr.stopNotifyReq(msg.Body.(*sch.MsgDhtRutMgrStopNofiyReq))

	case sch.EvDhtRutPingInd:
		rutLog.Debug("rutMgrProc: ping is not supported")
		eno = sch.SchEnoNotImpl

	case sch.EvDhtRutPongInd:
		rutLog.Debug("rutMgrProc: pong is not supported")
		eno = sch.SchEnoNotImpl

	default:
		rutLog.Debug("rutMgrProc: unknown message: %d", msg.Id)
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron signal handler
//
func (rutMgr *RutMgr)poweron(ptn interface{}) sch.SchErrno {

	if ptn == nil {
		rutLog.Debug("poweron: nil task node pointer")
		return sch.SchEnoParameter
	}

	var eno sch.SchErrno

	rutMgr.ptnMe = ptn
	rutMgr.sdl = sch.SchGetScheduler(ptn)

	eno, rutMgr.ptnQryMgr = rutMgr.sdl.SchGetUserTaskNode(QryMgrName)
	if eno != sch.SchEnoNone || rutMgr.ptnQryMgr == nil {
		rutLog.Debug("poweron: nil task node pointer, task: %s", QryMgrName)
		return eno
	}

	eno, rutMgr.ptnConMgr = rutMgr.sdl.SchGetUserTaskNode(ConMgrName)
	if eno != sch.SchEnoNone || rutMgr.ptnQryMgr == nil {
		rutLog.Debug("poweron: nil task node pointer, task: %s", ConMgrName)
		return eno
	}

	if dhtEno := rutMgr.rutMgrGetRouteConfig(); dhtEno != DhtEnoNone {
		rutLog.Debug("poweron: rutMgrGetRouteConfig failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if dhtEno := rutMgrSetupLog2DistLKT(rutMgr.distLookupTab); dhtEno != DhtEnoNone {
		rutLog.Debug("poweron: rutMgrSetupLog2DistLKT failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if dhtEno := rutMgr.rutMgrSetupRouteTable(); dhtEno != DhtEnoNone {
		rutLog.Debug("poweron: rutMgrSetupRouteTable failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	if dhtEno := rutMgr.rutMgrStartBspTimer(); dhtEno != DhtEnoNone {
		rutLog.Debug("poweron: rutMgrStartBspTimer failed, dhtEno: %d", dhtEno)
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

//
// Poweroff signal handler
//
func (rutMgr *RutMgr)poweroff(ptn interface{}) sch.SchErrno {
	rutLog.Debug("poweroff: task will be done ...")
	return rutMgr.sdl.SchTaskDone(ptn, sch.SchEnoKilled)
}

//
// Route table refresh handler
//
func (rutMgr *RutMgr)refreshReq() sch.SchErrno {
	return rutMgr.bootstarpTimerHandler()
}

//
// Bootstrap timer expired event handler
//
func (rutMgr *RutMgr)bootstarpTimerHandler() sch.SchErrno {

	rutLog.Debug("bootstarpTimerHandler: bootstrap will be carried out ...")

	if len(rutMgr.bsTargets) != 0 {
		rutLog.Debug("bootstarpTimerHandler: the previous is not completed")
		return sch.SchEnoNone
	}

	sdl := rutMgr.sdl

	for loop := 0; loop < rutMgr.bpCfg.randomQryNum; loop++ {
		msg := sch.SchMessage{}
		target := rutMgrRandomPeerId()
		key := (*DsKey)(rutMgrNodeId2Hash(target))
		req := sch.MsgDhtQryMgrQueryStartReq {
			Target:		*key,
			Msg:		nil,
			ForWhat:	MID_FINDNODE,
			Seq:		time.Now().UnixNano(),
		}

		rutMgr.bsTargets[*key] = target
		rutLog.Debug("bootstarpTimerHandler: query will be start, target: %x", req.Target)

		sdl.SchMakeMessage(&msg, rutMgr.ptnMe, rutMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &req)
		sdl.SchSendMessage(&msg)
	}

	return sch.SchEnoNone
}

//
// Query startup response(for bootstrap) handler
//
func (rutMgr *RutMgr)queryStartRsp(rsp *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {

	if rsp == nil {
		rutLog.Debug("queryStartRsp: invalid parameter")
		return sch.SchEnoParameter
	}

	rutLog.Debug("queryStartRsp: " +
		"bootstrap startup response, eno: %d, target: %x",
		rsp.Eno, rsp.Target)

	if _, exist := rutMgr.bsTargets[rsp.Target]; !exist {
		rutLog.Debug("queryStartRsp: not a bootstrap target: %x", rsp.Target)
		return sch.SchEnoMismatched
	}

	if rsp.Eno != DhtEnoNone.GetEno() {
		delete(rutMgr.bsTargets, rsp.Target)
	}

	return sch.SchEnoNone
}

//
// Query result indication(for bootstrap) handler
//
func (rutMgr *RutMgr)queryResultInd(ind *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {

	//
	// do not care the result, since the bootstrap procedure is just for keeping
	// the route table in a health status than finding out the target(which is not
	// a specific peer but just created randomly).
	//

	if ind == nil {
		rutLog.Debug("queryResultInd: invalid parameter")
		return sch.SchEnoParameter
	}

	rutLog.Debug("queryResultInd: " +
		"bootstrap result indication, eno: %d, target: %x",
		ind.Eno, ind.Target)

	ptrNodeId, ok := rutMgr.bsTargets[ind.Target]
	if !ok {
		rutLog.Debug("queryResultInd: not a bootstrap target: %x", ind.Target)
		return sch.SchEnoMismatched
	}
	nodeId, _ := ptrNodeId.(config.NodeID)

	//
	// when a bootstrap round is completed, we publish ourselves to outside world to
	// let others know us.
	//

	if delete(rutMgr.bsTargets, ind.Target); len(rutMgr.bsTargets) == 0 &&
		bytes.Compare(nodeId[0:], rutMgr.localNodeId[0:]) != 0 {

		msg := sch.SchMessage{}
		target := rutMgr.localNodeId
		key := (*DsKey)(rutMgrNodeId2Hash(target))
		req := sch.MsgDhtQryMgrQueryStartReq{
			Target:  *key,
			Msg:     nil,
			ForWhat: MID_FINDNODE,
			Seq:     time.Now().UnixNano(),
		}

		rutMgr.bsTargets[*key] = target

		rutMgr.sdl.SchMakeMessage(&msg, rutMgr.ptnMe, rutMgr.ptnQryMgr, sch.EvDhtQryMgrQueryStartReq, &req)
		rutMgr.sdl.SchSendMessage(&msg)
	}

	rutMgr.showRoute("bootstrap-round-completed")

	return sch.SchEnoNone
}

//
// Nearest peer request handler
//
func (rutMgr *RutMgr)nearestReq(tskSender interface{}, req *sch.MsgDhtRutMgrNearestReq) sch.SchErrno {

	if tskSender == nil || req == nil {
		rutLog.Debug("nearestReq: invalid parameters, tskSender: %p, req: %p", tskSender, req)
		return sch.SchEnoParameter
	}

	var rsp = sch.MsgDhtRutMgrNearestRsp {
		Eno:		int(DhtEnoUnknown),
		ForWhat:	req.ForWhat,
		Target:		req.Target,
		Peers:		nil,
		Dists:		nil,
		Pcs:		nil,
		Msg:		req.Msg,
	}
	var schMsg = sch.SchMessage{}

	dhtEno, nearest, nearestDist := rutMgr.rutMgrNearest(&req.Target, req.Max)
	if dhtEno != DhtEnoNone {
		rutLog.Debug("nearestReq: rutMgrNearest failed, eno: %d", dhtEno)
		rsp.Eno = int(dhtEno)
		rutMgr.sdl.SchMakeMessage(&schMsg, rutMgr.ptnMe, tskSender, sch.EvDhtRutMgrNearestRsp, &rsp)
		return rutMgr.sdl.SchSendMessage(&schMsg)
	}

	if dhtEno == DhtEnoNone && len(nearest) <= 0 {
		rutLog.Debug("nearestReq: empty nearest set from buckets, bootstrap node applied")
		if bootstrapNodes == nil {
			rsp.Eno = int(DhtEnoNotFound)
			rutMgr.sdl.SchMakeMessage(&schMsg, rutMgr.ptnMe, tskSender, sch.EvDhtRutMgrNearestRsp, &rsp)
			return rutMgr.sdl.SchSendMessage(&schMsg)
		}

		idx := mrand.Int31n(int32(len(bootstrapNodes)))
		node := bootstrapNodes[idx]
		hash := rutMgrNodeId2Hash(node.ID)
		bn := rutMgrBucketNode {
			node: *node,
			hash: *hash,
			dist: rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, hash),
			fails: 0,
			pcs: 0,
		}
		nearest = []*rutMgrBucketNode{&bn}
		nearestDist = []int{bn.dist}
	}

	pcsTab := []int{}
	rsp.Peers = nearest
	rsp.Dists = nearestDist
	for _, p := range nearest {
		pcsTab = append(pcsTab, p.pcs)
	}

	rsp.Eno = int(DhtEnoNone)
	rsp.Pcs = pcsTab
	rutMgr.sdl.SchMakeMessage(&schMsg, rutMgr.ptnMe, tskSender, sch.EvDhtRutMgrNearestRsp, &rsp)
	rutMgr.sdl.SchSendMessage(&schMsg)

	if req.NtfReq == true {
		if dhtEno = rutMgr.rutMgrNotifeeReg(tskSender, &req.Target, req.Max, nil, nil); dhtEno != DhtEnoNone {
			rutLog.Debug("nearestReq: rutMgrNotifeeReg failed, eno: %d", dhtEno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

//
// Update route table request handler
//
func (rutMgr *RutMgr)updateReq(req *sch.MsgDhtRutMgrUpdateReq) sch.SchErrno {

	dht := rutMgr.sdl.SchGetP2pCfgName()

	if req == nil || len(req.Seens) != len(req.Duras) || len(req.Seens) == 0 {
		rutLog.Debug("updateReq: invalid prameter, dht: %s", dht)
		return sch.SchEnoUserTask
	}

	why := req.Why
	eno := req.Eno

	//
	// check "why" and "eno"
	//

	if why == rutMgrUpdate4Handshake && eno == DhtEnoNone.GetEno() {

		rutLog.Debug("updateReq: dht: %s, why: rutMgrUpdate4Handshake, eno: DhtEnoNone", dht)

		//
		// new peer picked ok
		//

		rt := &rutMgr.rutTab

		for idx, n := range req.Seens {

			hash := rutMgrNodeId2Hash(n.ID)
			dist := rutMgr.rutMgrLog2Dist(&rt.shaLocal, hash)
			dur := req.Duras[idx]

			rutMgr.rutMgrMetricSample(n.ID, dur)

			bn := rutMgrBucketNode{
				node:	n,
				hash:	*hash,
				dist:	dist,
				pcs:	int(conInstStatus2PCS(CisHandshaked)),
			}

			rutMgr.update(&bn, dist)
			rutMgr.showRoute("handshake-update")
		}

		if dhtEno := rutMgr.rutMgrNotify(); dhtEno != DhtEnoNone {
			rutLog.Debug("updateReq: dht: %s, rutMgrNotify failed, eno: %d", dht, dhtEno)
			return sch.SchEnoUserTask
		}

	} else if why == rutMgrUpdate4Query && eno == DhtEnoTimeout.GetEno() {

		rutLog.Debug("updateReq: dht: %s, why: rutMgrUpdate4Query, eno: %d", dht, eno)

		//
		// query peer time out, check fail counter
		//

		p := req.Seens[0].ID
		h := rutMgrNodeId2Hash(p)
		d := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, h)

		eno, el := rutMgr.find(p, d)
		if eno != DhtEnoNone {
			rutLog.Debug("updateReq: not found, dht: %s, eno: DhtEnoTimeout", dht)
			return sch.SchEnoUserTask
		}

		bn := el.Value.(*rutMgrBucketNode)
		if bn.fails++; bn.fails >= rutMgrMaxFails2Del {

			if eno := rutMgr.delete(p); eno != DhtEnoNone {

				rutLog.Debug("updateReq: delete failed, dht: %s, eno: %d, id: %x", dht, eno, p)
				return sch.SchEnoUserTask
			}

			rutMgr.rutMgrRmvNotify(bn)
			rutMgr.showRoute("query-timeout-delete")
		}

	} else if why == rutMgrUpdate4Query && eno == DhtEnoNone.GetEno() {

		rutLog.Debug("updateReq: dht: %s, why: rutMgrUpdate4Query, eno: DhtEnoNone", dht)

		//
		// query ok, apply latency sample and clear fail counter
		//

		for idx, n := range req.Seens {
			dur := req.Duras[idx]
			rutMgr.rutMgrMetricSample(n.ID, dur)

			p := n.ID
			h := rutMgrNodeId2Hash(p)
			d := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, h)
			if eno, el := rutMgr.find(p, d); eno == DhtEnoNone {
				bn := el.Value.(*rutMgrBucketNode)
				bn.fails = 0
			}
		}

	} else if why == rutMgrUpdate4Closed {

		rutLog.Debug("updateReq: dht: %s, why: rutMgrUpdate4Closed", dht)

		//
		// update peer connection status to be CisClosed, but do not remove it from
		// bucket.
		//

		p := req.Seens[0].ID
		h := rutMgrNodeId2Hash(p)
		d := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, h)

		eno, el := rutMgr.find(p, d)
		if eno != DhtEnoNone {
			rutLog.Debug("updateReq: not found, dht: %s, eno: %d", dht, eno)
			return sch.SchEnoUserTask
		}

		el.Value.(*rutMgrBucketNode).pcs = int(conInstStatus2PCS(CisClosed))

	} else {

		rutLog.Debug("updateReq: dht: %s, invalid (why:%d, eno:%d)", dht, why, eno)
		return sch.SchEnoMismatched
	}


	return sch.SchEnoNone
}

//
// Stop notify request handler
//
func (rutMgr *RutMgr)stopNotifyReq(req *sch.MsgDhtRutMgrStopNofiyReq) sch.SchErrno {

	var nfi = rutMgrNotifeeId {
		task:	req.Task,
		target:	req.Target,
	}

	if _, exist := rutMgr.ntfTab[nfi]; exist == false {

		rutLog.Debug("stopNotifyReq: " +
			"notifee not found, task: %p, target: %x",
			nfi.task, nfi.target)

		return sch.SchEnoUserTask
	}

	delete(rutMgr.ntfTab, nfi)

	return sch.SchEnoNone
}

//
// Get route manager configuration
//
func (rutMgr *RutMgr)rutMgrGetRouteConfig() DhtErrno {

	cfgName := rutMgr.sdl.SchGetP2pCfgName()
	rutCfg := config.P2pConfig4DhtRouteManager(cfgName)
	rutMgr.bootstrapNode = rutCfg.BootstrapNode
	rutMgr.localNodeId = rutCfg.NodeId
	rutMgr.bpCfg.randomQryNum = rutCfg.RandomQryNum
	rutMgr.bpCfg.period = rutCfg.Period

	return DhtEnoNone
}

//
// Start bootstrap timer
//
func (rutMgr *RutMgr)rutMgrStartBspTimer() DhtErrno {

	if rutMgr.bootstrapNode {
		rutLog.Debug("rutMgrStartBspTimer: no bootstrap timer for a bot")
		return DhtEnoNone
	}

	var td = sch.TimerDescription {
		Name:	"dhtRutBspTimer",
		Utid:	sch.DhtRutBootstrapTimerId,
		Tmt:	sch.SchTmTypePeriod,
		Dur:	rutMgr.bpCfg.period,
		Extra:	nil,
	}

	if eno, tid := rutMgr.sdl.SchSetTimer(rutMgr.ptnMe, &td);
	eno != sch.SchEnoNone || tid == sch.SchInvalidTid {

		rutLog.Debug("rutMgrStartBspTimer: " +
			"SchSetTimer failed, eno: %d, tid: %d",
			eno, tid)

		return DhtEnoScheduler
	}

	return DhtEnoNone
}

//
// Stop bootstrap timer
//
func (rutMgr *RutMgr)rutMgrStopBspTimer() DhtErrno {

	var dhtEno = DhtEnoNone

	if rutMgr.bpTid != sch.SchInvalidTid {
		if eno := rutMgr.sdl.SchKillTimer(rutMgr.ptnMe, rutMgr.bpTid); eno != sch.SchEnoNone {
			dhtEno = DhtEnoScheduler
		}
	}
	rutMgr.bpTid = sch.SchInvalidTid

	return dhtEno
}

//
// Build random node identity
//
func rutMgrRandomPeerId() config.NodeID {
	var nid config.NodeID
	rand.Read(nid[:])
	return nid
}

//
// Build hash from random node identity
//
func rutMgrRandomHashPeerId() *Hash {
	return rutMgrNodeId2Hash(rutMgrRandomPeerId())
}

//
// Setup lookup table
//
func rutMgrSetupLog2DistLKT(lkt []int) DhtErrno {
	var n uint
	var b uint
	lkt[0] = 8
	for n = 0; n < 8; n++ {
		for b = 1<<n; b < 1<<(n+1); b++ {
			lkt[b] = int(8 - n - 1)
		}
	}
	return DhtEnoNone
}

//
// Caculate the distance between two nodes.
// Notice: the return "d" more larger, it's more closer
//
func (rutMgr *RutMgr)rutMgrLog2Dist(h1 *Hash, h2 *Hash) int {
	if h1 == nil {
		h1 = &rutMgr.rutTab.shaLocal
	}
	d := 0
	for i, b := range h2 {
		delta := rutMgr.distLookupTab[h1[i] ^ b]
		d += delta
		if delta != 8 {
			break
		}
	}
	return d
}

//
// Setup route table
//
func (rutMgr *RutMgr)rutMgrSetupRouteTable() DhtErrno {
	rt := &rutMgr.rutTab
	rt.shaLocal = *rutMgrNodeId2Hash(rutMgr.localNodeId)
	rt.bucketSize = rutMgrBucketSize
	rt.maxLatency = rutMgrMaxLatency
	rt.bucketTab = make([]*list.List, 0, HashBitLength + 1)
	rt.bucketTab = append(rt.bucketTab, list.New())
	rt.metricTab = make(map[config.NodeID]*rutMgrPeerMetric, 0)
	return DhtEnoNone
}

//
// Metric sample input
//
func (rutMgr *RutMgr) rutMgrMetricSample(id config.NodeID, latency time.Duration) DhtErrno {

	rt := &rutMgr.rutTab

	if m, dup := rt.metricTab[id]; dup {
		num := len(m.ltnSamples)
		next := (num + 1) & (rutMgrEwmaHisSize - 1)
		m.ltnSamples[next] = latency
		return rutMgr.rutMgrMetricUpdate(id)
	}

	if latency == -1 {
		return DhtEnoNone
	}

	rt.metricTab[id] = &rutMgrPeerMetric {
		peerId:		id,
		ltnSamples: make([]time.Duration, 8),
		ewma:		latency,
	}
	rt.metricTab[id].ltnSamples[0] = latency

	return DhtEnoNone
}

//
// Metric update EWMA about latency
//
func (rutMgr *RutMgr) rutMgrMetricUpdate(id config.NodeID) DhtErrno {

	rt := &rutMgr.rutTab
	m, exist := rt.metricTab[id]

	if !exist {
		rutLog.Debug("rutMgrMetricUpdate: not found: %x", id)
		return DhtEnoNotFound
	}

	sn := len(m.ltnSamples)

	if sn <= 0 {
		rutLog.Debug("rutMgrMetricUpdate: none of samples")
		return DhtEnoInternal
	}

	m.ewma = time.Duration((1.0 - rutMgrEwmaMF) * float64(m.ewma) + rutMgrEwmaMF * float64(m.ltnSamples[sn-1]))

	return DhtEnoNone
}

//
// Metric get EWMA latency of peer
//
func (rutMgr *RutMgr) rutMgrMetricGetEWMA(id config.NodeID) (DhtErrno, time.Duration){
	rt := &rutMgr.rutTab
	mt := rt.metricTab
	if m, ok := mt[id]; ok {
		return DhtEnoNone, m.ewma
	}
	return DhtEnoNotFound, -1
}

//
// Node identity to hash(sha)
//
func rutMgrNodeId2Hash(id config.NodeID) *Hash {
	h := sha256.Sum256(id[:])
	return (*Hash)(&h)
}

//
// Node identity to hash(sha) exported
//
func RutMgrNodeId2Hash(id config.NodeID) *Hash {
	return rutMgrNodeId2Hash(id)
}

//
// Sort peers with distance
//
func rutMgrSortPeer(ps []*rutMgrBucketNode, ds []int) {

	if len(ps) == 0 || len(ds) == 0 {
		return
	}

	li := list.New()

	for i, d := range ds {
		inserted := false
		for el := li.Front(); el != nil; el = el.Next() {
			if d < ds[el.Value.(int)] {
				li.InsertBefore(i, el)
				inserted = true
				break
			}
		}
		if !inserted {
			li.PushBack(i)
		}
	}

	i := 0
	for el := li.Front(); el != nil; el = el.Next() {
		pi := el.Value.(int)
		ps[i], ps[pi] = ps[pi], ps[i]
		ds[i], ds[pi] = ds[pi], ds[i]
		i++
	}
}

//
// Lookup node
//
func (rutMgr *RutMgr)rutMgrFind(id config.NodeID) (DhtErrno, *list.Element) {
	hash := rutMgrNodeId2Hash(id)
	dist := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, hash)
	return rutMgr.find(id, dist)
}

//
// Lookup node in buckets
//
func (rutMgr *RutMgr)find(id config.NodeID, dist int) (DhtErrno, *list.Element) {

	rt := &rutMgr.rutTab

	if dist >= len(rt.bucketTab) {
		dist = len(rt.bucketTab) - 1
	}

	li := rt.bucketTab[dist]
	for el := li.Front(); el != nil; el = el.Next() {
		if el.Value.(*rutMgrBucketNode).node.ID == id {
			return DhtEnoNone, el
		}
	}

	return DhtEnoNotFound, nil
}

//
// Delete peer from route table
//
func (rutMgr *RutMgr)delete(id config.NodeID) DhtErrno {

	rutLog.Debug("delete: id: %x", id)

	hash := rutMgrNodeId2Hash(id)
	dist := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, hash)

	if dist >= len(rutMgr.rutTab.bucketTab) {
		dist = len(rutMgr.rutTab.bucketTab) - 1
	}

	li := rutMgr.rutTab.bucketTab[dist]

	for el := li.Front(); el != nil; el = el.Next() {
		if el.Value.(*rutMgrBucketNode).node.ID == id {
			li.Remove(el)
			return DhtEnoNone
		}
	}

	return DhtEnoNotFound
}

//
// Update route table
//
func (rutMgr *RutMgr)update(bn *rutMgrBucketNode, dist int) DhtErrno {

	rt := &rutMgr.rutTab

	//
	// get tail index: if not initialized, append one
	//

	tail := len(rt.bucketTab)
	if tail == 0 {
		rt.bucketTab = append(rt.bucketTab, list.New())
	} else {
		tail--
	}

	//
	// if distance more closer to use than tail, set target bucket as tail:
	// all those peers with distance equal or greater than tail are there,
	// we lookup if "bn" is there.
	//

	if dist >= len(rt.bucketTab) {
		dist = tail
	}

	bucket := rt.bucketTab[dist]

	if eno, el := rutMgr.find(bn.node.ID, dist); eno == DhtEnoNone && el != nil {
		*el.Value.(*rutMgrBucketNode) = *bn
		bucket.MoveToFront(el)
		return DhtEnoNone
	}

	//
	// "bn" not found, new peer, check latency
	//

	eno, ewma := rutMgr.rutMgrMetricGetEWMA(bn.node.ID)
	if eno != DhtEnoNone && eno != DhtEnoNotFound {
		rutLog.Debug("update: " +
			"rutMgrMetricGetEWMA failed, eno: %d, ewma: %d",
			eno, ewma)
		return eno
	}

	if eno == DhtEnoNotFound {
		ewma = 0
	}

	if ewma > rt.maxLatency {
		rutLog.Debug("update: " +
			"discarded, ewma: %d,  maxLatency: %d",
			ewma, rt.maxLatency)
		return DhtEnoNone
	}

	//
	// push new peer as "bn" to target bucket, and, if it is the tail bucket
	// that the new peer pushed, we check if "split" needed; else we check if
	// the "Back" of the targt bucket should be removed.
	//

	bucket.PushFront(bn)

	if dist == tail {

		if  bucket.Len() > rt.bucketSize {

			rutMgr.split(bucket, tail)
		}

	} else {

		if bucket.Len() > rt.bucketSize {

			bucket.Remove(bucket.Back())
		}
	}

	return DhtEnoNone
}

//
// Just for debug to show the route table
//
func (rutMgr *RutMgr)showRoute(tag string) {
	if true {
		dht := rutMgr.sdl.SchGetP2pCfgName()
		routInfo := fmt.Sprintf("showRoute: dht: %s, rutTab: %+v\n", dht, rutMgr.rutTab)
		rt := rutMgr.rutTab
		for idx := 0; idx < len(rt.bucketTab); idx++ {
			routInfo = routInfo + fmt.Sprintf("showRoute: "+
				"=============================== tag: %s, dht: %s, bucket: %d ==============================\n",
				tag, dht, idx)
			li := rt.bucketTab[idx]
			count := 0
			for el := li.Front(); el != nil; el = el.Next() {
				bn, ok := el.Value.(*rutMgrBucketNode)
				if !ok {
					golog.Printf("showRoute: dht: %s, invalid bucket node found, idx: %d", dht, idx)
					continue
				}
				count++
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: count: %d >>>>>>>>>>>>>>>>>>>>>>>\n", dht, count)
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: node: %+v\n", dht, bn.node)
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: hash: %x\n", dht, bn.hash)
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: dist: %d\n", dht, bn.dist)
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: fails: %d\n", dht, bn.fails)
				routInfo = routInfo + fmt.Sprintf("dht: %s, showRoute: pcs: %d\n", dht, bn.pcs)
				golog.Printf("%s", routInfo)
			}
		}
	}
}

//
// Split the tail bucket
//
func (rutMgr *RutMgr)split(li *list.List, dist int) DhtErrno {

	//
	// notice: this function called recursively, this would make the buckets
	// in the bucket table must be continued from "0" to "tail".
	//

	rt := &rutMgr.rutTab

	if len(rt.bucketTab) - 1 != dist {
		rutLog.Debug("split: can only split the tail bucket")
		return DhtEnoParameter
	}

	if li.Len() == 0 {
		rutLog.Debug("split: can't split an empty bucket")
		return DhtEnoParameter
	}

	newLi := list.New()

	var el = li.Front()
	var elNext *list.Element = nil

	for {
		elNext = el.Next()
		bn := el.Value.(*rutMgrBucketNode)
		if bn.dist > dist {
			newLi.PushBack(bn)
			li.Remove(el)
		}
		if elNext == nil {
			break
		}
		el = elNext
	}

	//
	// if size exceeded, remove some
	//

	for li.Len() > rt.bucketSize {
		bn := li.Back().Value.(*rutMgrBucketNode)
		li.Remove(li.Back())
		rutMgr.rutMgrRmvNotify(bn)
	}

	//
	// append new bucket if necessary
	//

	if newLi.Len() != 0 {
		rt.bucketTab = append(rt.bucketTab, newLi)
	}

	//
	// recursive to split if necessary
	//

	if newLi.Len() > rt.bucketSize {
		rutMgr.split(newLi, dist + 1)
	}

	return DhtEnoNone
}

//
// Register notifee
//
func (rutMgr *RutMgr)rutMgrNotifeeReg(
	task	interface{},
	id		*config.DsKey,
	max		int,
	bns		[]*rutMgrBucketNode,
	ds		[]int) DhtErrno {

	if len(rutMgr.ntfTab) >= rutMgrMaxNofifee {
		rutLog.Debug("rutMgrNotifeeReg: too much notifees, max: %d", rutMgrMaxNofifee)
		return DhtEnoResource
	}

	nid := rutMgrNotifeeId {
		task:	task,
		target:	*id,
	}

	ntfe := rutMgrNotifee {
		id:			nid,
		max:		max,
		nearests:	bns,
		dists:		ds,
	}

	rutMgr.ntfTab[nid] = &ntfe

	return DhtEnoNone
}

//
// Notify those tasks whom registered with notifees
//
func (rutMgr *RutMgr)rutMgrNotify() DhtErrno {

	var ind = sch.MsgDhtRutMgrNotificationInd{}
	var msg = sch.SchMessage{}
	var failCnt = 0

	for key, ntf := range rutMgr.ntfTab {

		task := ntf.id.task
		target := &ntf.id.target
		size := ntf.max

		old := rutMgr.ntfTab[key].nearests
		eno, nearest, dist := rutMgr.rutMgrNearest(target, size)
		if eno != DhtEnoNone {
			rutLog.Debug("rutMgrNotify: rutMgrNearest failed, eno: %d", eno)
			failCnt++
			continue
		}

		doNotify := false

		if len(old) != len(nearest) {
			doNotify = true
		} else {
			for idx, n := range nearest {
				if bytes.Compare(old[idx].node.ID[0:], n.node.ID[0:]) != 0 {
					doNotify = true
				}
			}
		}

		if doNotify {

			rutMgr.ntfTab[key].nearests = nearest
			rutMgr.ntfTab[key].dists = dist

			ind.Target = *target
			ind.Peers = nearest
			ind.Dists = dist

			rutMgr.sdl.SchMakeMessage(&msg, rutMgr.ptnMe, task, sch.EvDhtRutMgrNotificationInd, &ind)
			rutMgr.sdl.SchSendMessage(&msg)
		}
	}

	return DhtEnoNone
}

//
// Get nearest peers for target
//
func (rutMgr *RutMgr)rutMgrNearest(target *config.DsKey, size int) (DhtErrno, []*rutMgrBucketNode, []int){

	var nearest = make([]*rutMgrBucketNode, 0, rutMgrMaxNearest)
	var nearestDist = make([]int, 0, rutMgrMaxNearest)

	var count = 0
	var dhtEno = DhtEnoNone

	//
	// please notice that distance from target to local might more closer than
	// the tail of the bucket table. if this is true, we set the distancet "dt"
	// to the tail. see function rutMgr.split pls.
	//

	ht := (*Hash)(target)
	dt := rutMgr.rutMgrLog2Dist(&rutMgr.rutTab.shaLocal, ht)
	if dt >= len(rutMgr.rutTab.bucketTab) {
		dt = len(rutMgr.rutTab.bucketTab) - 1
	}

	var addClosest = func (bk *list.List) int {
		count = len(nearest)
		if bk != nil {
			for el := bk.Front(); el != nil; el = el.Next() {
				peer := el.Value.(*rutMgrBucketNode)
				nearest = append(nearest, peer)
				dt := rutMgr.rutMgrLog2Dist(ht, &peer.hash)
				nearestDist = append(nearestDist, dt)
				if count++; count >= size {
					break
				}
			}
		}
		return count
	}

	if size <= 0 || size > rutMgrMaxNearest {
		rutLog.Debug("rutMgrNearest: " +
			"invalid size: %d, min: 1, max: %d",
			size, rutMgrMaxNearest)

		dhtEno = DhtEnoParameter
		goto _done
	}

	//
	// the most closest bank
	//

	if bk := rutMgr.rutTab.bucketTab[dt]; bk != nil {
		if addClosest(bk) >= size {
			goto _done
		}
	}

	//
	// the second closest bank
	//

	for loop := dt + 1; loop < len(rutMgr.rutTab.bucketTab); loop++ {
		if bk := rutMgr.rutTab.bucketTab[loop]; bk != nil {
			if addClosest(bk) >= size {
				goto _done
			}
		}
	}

	if dt <= 0 { goto _done }

	//
	// the last bank
	//

	for loop := dt - 1; loop >= 0; loop-- {
		if bk := rutMgr.rutTab.bucketTab[loop]; bk != nil {
			if addClosest(bk) >= size {
				goto _done
			}
		}
	}

	//
	// response to the sender
	//

_done:

	if dhtEno != DhtEnoNone  {
		return dhtEno, nil, nil
	}

	//
	// sort
	//

	if len(nearest) > 0 {
		rutMgrSortPeer(nearest, nearestDist)
	}

	return DhtEnoNone, nearest, nearestDist
}

//
// Tell connection manager that peer removed so it can close the connection
// with the peer if necessary.
//
func (rutMgr *RutMgr)rutMgrRmvNotify(bn *rutMgrBucketNode) DhtErrno {
	var msg = sch.SchMessage{}
	var ind = sch.MsgDhtRutPeerRemovedInd{
		Peer:	bn.node.ID,
	}
	rutMgr.sdl.SchMakeMessage(&msg, rutMgr.ptnMe, rutMgr.ptnConMgr, sch.EvDhtRutPeerRemovedInd, &ind)
	rutMgr.sdl.SchSendMessage(&msg)
	return DhtEnoNone
}

