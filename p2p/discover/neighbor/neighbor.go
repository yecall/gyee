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

package neighbor

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yeeco/gyee/log"
	config "github.com/yeeco/gyee/p2p/config"
	tab "github.com/yeeco/gyee/p2p/discover/table"
	um "github.com/yeeco/gyee/p2p/discover/udpmsg"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)


// errno
const (
	NgbMgrEnoNone = iota
	NgbMgrEnoParameter
	NgbMgrEnoTimeout
	NgbMgrEnoNotFound
	NgbMgrEnoEncode
	NgbMgrEnoUdp
	NgbMgrEnoDuplicated
	NgbMgrEnoMismatched
	NgbMgrEnoScheduler
)

type NgbMgrErrno int

const (
	NgbProcName        = "ngbproto" // Neighbor task name
	ngbProcMailboxSize = 512        // Mailbox size of a ngighbor instance
)

// The control block of neighbor task instance
type neighborInst struct {
	sdl     *sch.Scheduler    // pointer to scheduler
	ngbMgr  *NeighborManager  // pointer to neighbor manager
	ptn     interface{}       // task node pointer
	name    string            // instance name
	tskName	string			  // task name
	tep     sch.SchUserTaskEp // entry
	msgType um.UdpMsgType     // message type to inited this instance
	msgBody interface{}       // message body
	tidFN   int               // FindNode timer identity
	tidPP   int               // Pingpong timer identity
}

// Protocol handler errno
const (
	NgbProtoEnoNone = 0

	NgbProtoEnoParameter = iota + 100 // +100, an offset is necessary to distinct this errno from
	// those NgbMgrEnoxxx.

	NgbProtoEnoScheduler // scheduler
	NgbProtoEnoTimeout   // timeout
	NgbProtoEnoUdp       // udp
)

type NgbProtoErrno int

// Timeouts, zero value would be no timeout
const (
	NgbProtoWriteTimeout            = 8 * time.Second  // for underlying sending
	NgbProtoReadTimeout             = 0                // for underlying receiving
	NgbProtoPingResponseTimeout     = 20 * time.Second // for ping
	NgbProtoFindNodeResponseTimeout = 20 * time.Second // for find node
)

func (inst *neighborInst) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return inst.tep(ptn, msg)
}

func (inst *neighborInst) ngbProtoProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Tracef("ngbProtoProc: inst.name: %s, msg.Id: %d", inst.name, msg.Id)

	var protoEno NgbProtoErrno

	switch msg.Id {
	case sch.EvSchPoweroff:
		protoEno = inst.NgbProtoPoweroff(ptn)
	case sch.EvNblFindNodeReq:
		protoEno = inst.NgbProtoFindNodeReq(ptn, msg.Body.(*um.FindNode))
	case sch.EvNblPingpongReq:
		protoEno = inst.NgbProtoPingReq(ptn, msg.Body.(*um.Ping))
	case sch.EvNblPingpongRsp:
		protoEno = inst.NgbProtoPingRsp(msg.Body.(*um.Pong))
	case sch.EvNblFindNodeRsp:
		protoEno = inst.NgbProtoFindNodeRsp(msg.Body.(*um.Neighbors))
	case sch.EvNblFindNodeTimer:
		protoEno = inst.NgbProtoFindNodeTimeout()
	case sch.EvNblPingpongTimer:
		protoEno = inst.NgbProtoPingTimeout()
	default:
		log.Debugf("NgbProtoProc: invalid message, msg.Id: %d", msg.Id)
		protoEno = NgbProtoEnoParameter
	}

	log.Tracef("ngbProtoProc: get out, inst.name: %s, msg.Id: %d", inst.name, msg.Id)

	if protoEno != NgbProtoEnoNone {
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

func (inst *neighborInst) NgbProtoPoweroff(ptn interface{}) NgbProtoErrno {
	log.Debugf("NgbProtoPoweroff: task will be done, name: %s", inst.sdl.SchGetTaskName(inst.ptn))
	inst.sdl.SchTaskDone(inst.ptn, inst.tskName, sch.SchEnoKilled)
	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoFindNodeReq(ptn interface{}, fn *um.FindNode) NgbProtoErrno {
	var pum = new(um.UdpMsg)
	pum.Encode(um.UdpMsgTypeFindNode, fn)
	buf, _ := pum.GetRawMessage()

	var dst = net.UDPAddr{}
	dst.IP = append(dst.IP, fn.To.IP...)
	dst.Port = int(fn.To.UDP)

	pum.DebugMessageToPeer()

	if eno := sendUdpMsg(inst.sdl, inst.ngbMgr.ptnLsn, inst.ptn, buf, &dst); eno != sch.SchEnoNone {
		log.Debugf("NgbProtoFindNodeReq: failed to send, ip: %s, udp: %d", dst.IP.String(), dst.Port)
		rsp := sch.NblFindNodeRsp{}
		rsp.Result = (NgbProtoEnoUdp << 16) + tab.TabMgrEnoUdp
		rsp.FindNode = inst.msgBody.(*um.FindNode)
		schMsg := sch.SchMessage{}
		inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
		inst.sdl.SchSendMessage(&schMsg)
		inst.cleanMap(inst.name)
		return NgbProtoEnoUdp
	}

	var tmd = sch.TimerDescription{
		Name:  NgbProcName + "_timer_findnode",
		Utid:  sch.NblFindNodeTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   NgbProtoFindNodeResponseTimeout,
		Extra: nil,
	}
	_, inst.tidFN = inst.sdl.SchSetTimer(ptn, &tmd)

	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoPingReq(ptn interface{}, ping *um.Ping) NgbProtoErrno {
	pum := new(um.UdpMsg)
	pum.Encode(um.UdpMsgTypePing, ping)
	buf, _ := pum.GetRawMessage()

	dst := net.UDPAddr{}
	dst.IP = append(dst.IP, ping.To.IP...)
	dst.Port = int(ping.To.UDP)

	pum.DebugMessageToPeer()
	sendUdpMsg(inst.sdl, inst.ngbMgr.ptnLsn, inst.ptn, buf, &dst)

	var tmd = sch.TimerDescription{
		Name:  NgbProcName + "_timer_pingpong",
		Utid:  sch.NblPingpongTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   NgbProtoPingResponseTimeout,
		Extra: nil,
	}
	_, inst.tidPP = inst.sdl.SchSetTimer(ptn, &tmd)
	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoPingRsp(msg *um.Pong) NgbProtoErrno {
	if inst.msgType != um.UdpMsgTypePing || inst.msgBody == nil {
		return NgbProtoEnoParameter
	}

	ping := inst.msgBody.(*um.Ping)
	if equ := ping.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {
		return NgbProtoEnoParameter
	}

	if inst.tidPP != sch.SchInvalidTid {
		if eno := inst.sdl.SchKillTimer(inst.ptn, inst.tidPP); eno != sch.SchEnoNone {
			return NgbProtoEnoScheduler
		}
		inst.tidPP = sch.SchInvalidTid
	}

	rsp := sch.NblPingRsp{}
	rsp.Result = NgbProtoEnoNone
	rsp.Ping = inst.msgBody.(*um.Ping)
	rsp.Pong = msg
	schMsg := sch.SchMessage{}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	inst.cleanMap(inst.name)
	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoFindNodeRsp(msg *um.Neighbors) NgbProtoErrno {
	if inst.msgType != um.UdpMsgTypeFindNode || inst.msgBody == nil {
		return NgbProtoEnoParameter
	}

	findNode := inst.msgBody.(*um.FindNode)
	if equ := findNode.To.CompareWith(&msg.From); equ != um.CmpNodeEqu {
		return NgbProtoEnoParameter
	}

	if inst.tidFN != sch.SchInvalidTid {
		if eno := inst.sdl.SchKillTimer(inst.ptn, inst.tidFN); eno != sch.SchEnoNone {
			return NgbProtoEnoScheduler
		}
		inst.tidFN = sch.SchInvalidTid
	}

	rsp := sch.NblFindNodeRsp{}
	rsp.Result = (NgbProtoEnoNone << 16) + tab.TabMgrEnoNone
	rsp.FindNode = inst.msgBody.(*um.FindNode)
	rsp.Neighbors = msg
	schMsg := sch.SchMessage{}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	inst.cleanMap(inst.name)
	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoFindNodeTimeout() NgbProtoErrno {
	rsp := sch.NblFindNodeRsp{}
	rsp.Result = (NgbProtoEnoTimeout << 16) + tab.TabMgrEnoTimeout
	rsp.FindNode = inst.msgBody.(*um.FindNode)
	schMsg := sch.SchMessage{}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	inst.cleanMap(inst.name)
	return NgbProtoEnoNone
}

func (inst *neighborInst) NgbProtoPingTimeout() NgbProtoErrno {
	rsp := sch.NblPingRsp{}
	rsp.Result = NgbProtoEnoTimeout
	rsp.Ping = inst.msgBody.(*um.Ping)
	schMsg := sch.SchMessage{}
	inst.sdl.SchMakeMessage(&schMsg, inst.ptn, inst.ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
	inst.sdl.SchSendMessage(&schMsg)
	inst.cleanMap(inst.name)
	return NgbProtoEnoNone
}

func (inst *neighborInst) cleanMap(name string) NgbMgrErrno {
	msg := sch.SchMessage{}
	inst.sdl.SchMakeMessage(&msg, inst.ptn, inst.ngbMgr.ptnMe, sch.EvNblCleanMapReq, name)
	inst.sdl.SchSendMessage(&msg)
	return NgbMgrEnoNone
}

func (inst *neighborInst) NgbProtoDieCb(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

const (
	NgbMgrName = sch.NgbMgrName   // Neighbor manager task name
	expiration = 20 * time.Second // Timeouts
)

// Control block of neighbor manager task
type NeighborManager struct {
	cfg       config.Cfg4UdpNgbManager // configuration
	lock      sync.Mutex               // lock for protection
	sdl       *sch.Scheduler           // pointer to scheduler
	sdlName   string				   // scheduler name
	name      string                   // name
	tep       sch.SchUserTaskEp        // entry
	bootstrap bool                     // bootstrap node flag
	ptnMe     interface{}              // pointer to task node of myself
	ptnTab    interface{}              // pointer to task node of table task
	tabMgr    *tab.TableManager        // pointer to table manager
	ptnLsn    interface{}              // pointer to task node of listner
	ngbMap    map[string]*neighborInst // map neighbor node id to task node pointer
	fnInstSeq int                      // findnode instance sequence number
	ppInstSeq int                      // pingpong instance sequence number
	checkDestNodeStat map[string] int64 // checkDestNode failed count
}

func NewNgbMgr() *NeighborManager {
	var ngbMgr = NeighborManager{
		name:      NgbMgrName,
		ngbMap:    make(map[string]*neighborInst),
		fnInstSeq: 0,
		ppInstSeq: 0,
		checkDestNodeStat: make(map[string]int64, 0),
	}
	ngbMgr.tep = ngbMgr.ngbMgrProc
	return &ngbMgr
}

func (ngbMgr *NeighborManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return ngbMgr.tep(ptn, msg)
}

func (ngbMgr *NeighborManager) ngbMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	log.Tracef("ngbMgrProc: ngbMgr.name: %s, msg.Id: %d", ngbMgr.name, msg.Id)

	var eno NgbMgrErrno

	switch msg.Id {
	case sch.EvSchPoweron:
		return ngbMgr.PoweronHandler(ptn)
	case sch.EvSchPoweroff:
		return ngbMgr.PoweroffHandler(ptn)
	case sch.EvShellReconfigReq:
		eno = ngbMgr.shellReconfigReq(msg.Body.(*sch.MsgShellReconfigReq))
	case sch.EvNblMsgInd:
		eno = ngbMgr.UdpMsgInd(msg.Body.(*UdpMsgInd))
	case sch.EvNblFindNodeReq:
		eno = ngbMgr.FindNodeReq(msg.Body.(*um.FindNode))
	case sch.EvNblPingpongReq:
		eno = ngbMgr.PingpongReq(msg.Body.(*um.Ping))
	case sch.EvNblCleanMapReq:
		eno = ngbMgr.CleanMapReq(msg.Body.(string))
	case sch.EvNatPubAddrSwitchInd:
		eno = ngbMgr.natPubAddrSwitchInd(msg.Body.(*sch.MsgNatPubAddrSwitchInd))
	default:
		log.Debugf("NgbMgrProc:  invalid message id: %d", msg.Id)
		eno = NgbMgrEnoParameter
	}

	log.Tracef("ngbMgrProc: get out, ngbMgr.name: %s, msg.Id: %d", ngbMgr.name, msg.Id)

	if eno != NgbMgrEnoNone {
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

func (ngbMgr *NeighborManager) PoweronHandler(ptn interface{}) sch.SchErrno {
	var eno sch.SchErrno
	var ptnTab interface{}
	var ptnLsn interface{}

	ngbMgr.ptnMe = ptn
	ngbMgr.sdl = sch.SchGetScheduler(ptn)
	ngbMgr.sdlName = ngbMgr.sdl.SchGetP2pCfgName()
	ngbMgr.bootstrap = ngbMgr.sdl.SchGetP2pConfig().BootstrapNode
	if eno = ngbMgr.setupConfig(); eno != sch.SchEnoNone {
		log.Debugf("PoweronHandler: setupConfig failed, eno: %d", eno)
		return eno
	}

	if ngbMgr.cfg.NetworkType == config.P2pNetworkTypeStatic {
		log.Debugf("tabMgrPoweron: static subnet, tabMgr is not needed, done it ...")
		ngbMgr.sdl.SchTaskDone(ptn, ngbMgr.name, sch.SchEnoNone)
		return sch.SchEnoNone
	}

	ngbMgr.tabMgr = ngbMgr.sdl.SchGetTaskObject(sch.TabMgrName).(*tab.TableManager)
	eno, ptnTab = ngbMgr.sdl.SchGetUserTaskNode(sch.TabMgrName)
	eno, ptnLsn = ngbMgr.sdl.SchGetUserTaskNode(sch.NgbLsnName)
	ngbMgr.ptnMe = ptn
	ngbMgr.ptnTab = ptnTab
	ngbMgr.ptnLsn = ptnLsn

	return sch.SchEnoNone
}

func (ngbMgr *NeighborManager) PoweroffHandler(ptn interface{}) sch.SchErrno {
	log.Debugf("PoweroffHandler: task will be done, name: %s", ngbMgr.sdl.SchGetTaskName(ptn))
	ngbMgr.lock.Lock()
	for _, ngbInst := range ngbMgr.ngbMap {
		po := sch.SchMessage{
			Id:   sch.EvSchPoweroff,
		}
		ngbMgr.sdl.SchSetSender(&po, &sch.RawSchTask)
		ngbMgr.sdl.SchSetRecver(&po, ngbInst.ptn)
		po.TgtName = ngbInst.tskName
		ngbMgr.sdl.SchSendMessage(&po)
	}
	ngbMgr.lock.Unlock()
	return ngbMgr.sdl.SchTaskDone(ptn, ngbMgr.name, sch.SchEnoKilled)
}

func (ngbMgr *NeighborManager) shellReconfigReq(msg *sch.MsgShellReconfigReq) NgbMgrErrno {
	add := msg.SnidAdd
	del := msg.SnidDel

	for _, d := range del {
		for idx, id := range ngbMgr.cfg.SubNetIdList {
			if id == d {
				if idx != len(ngbMgr.cfg.SubNetIdList)-1 {
					ngbMgr.cfg.SubNetIdList = append(ngbMgr.cfg.SubNetIdList[0:idx], ngbMgr.cfg.SubNetIdList[idx+1:]...)
				} else {
					ngbMgr.cfg.SubNetIdList = ngbMgr.cfg.SubNetIdList[0:idx]
				}
				break
			}
		}
		delete(ngbMgr.cfg.SubNetNodeList, d)
	}

	for _, a := range add {
		ngbMgr.cfg.SubNetNodeList[a.SubNetId] = a.SubNetNode
	}

	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) UdpMsgInd(msg *UdpMsgInd) NgbMgrErrno {
	var eno NgbMgrErrno
	switch msg.msgType {
	case um.UdpMsgTypePing:
		eno = ngbMgr.PingHandler(msg.msgBody.(*um.Ping), msg.from)
	case um.UdpMsgTypePong:
		eno = ngbMgr.PongHandler(msg.msgBody.(*um.Pong), msg.from)
	case um.UdpMsgTypeFindNode:
		eno = ngbMgr.FindNodeHandler(msg.msgBody.(*um.FindNode), msg.from)
	case um.UdpMsgTypeNeighbors:
		eno = ngbMgr.NeighborsHandler(msg.msgBody.(*um.Neighbors), msg.from)
	default:
		log.Debugf("NgbMgrUdpMsgHandler: invalid udp message type: %d", msg.msgType)
		eno = NgbMgrEnoParameter
	}
	return eno
}

func (ngbMgr *NeighborManager) PingHandler(ping *um.Ping, from *net.UDPAddr) NgbMgrErrno {
	if ngbMgr.checkDestNode(&ping.To, ping.SubNetId, "ping") == false {
		log.Debugf("PingHandler: node identity mismatched")
		return NgbMgrEnoParameter
	}
	if expired(ping.Expiration) {
		log.Debugf("PingHandler: message expired")
		return NgbMgrEnoTimeout
	}

	matched := false
	for _, snid := range ngbMgr.cfg.SubNetIdList {
		if snid == ping.SubNetId {
			matched = true
			break
		}
	}

	if !matched {
		log.Debugf("PingHandler: subnet mismatched")
		return NgbMgrEnoMismatched
	}

	pong := um.Pong{
		From:         ping.To,
		To:           ping.From,
		FromSubNetId: ngbMgr.cfg.SubNetIdList,
		SubNetId:     ping.SubNetId,
		Id:           uint64(time.Now().UnixNano()),
		Expiration:   0,
		Extra:        nil,
	}

	var toAddr net.UDPAddr
	if from != nil {
		toAddr = *from
	} else {
		toAddr = net.UDPAddr{
			IP:   ping.From.IP,
			Port: int(ping.From.UDP),
			Zone: "",
		}
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypePong, &pong); eno != um.UdpMsgEnoNone {
		log.Debugf("PingHandler: Encode failed")
		return NgbMgrEnoEncode
	}
	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes <= 0 {
		log.Debugf("PingHandler: GetRawMessage failed")
		return NgbMgrEnoEncode
	}

	pum.DebugMessageToPeer()

	if eno := sendUdpMsg(ngbMgr.sdl, ngbMgr.ptnLsn, ngbMgr.ptnMe, buf, &toAddr); eno != sch.SchEnoNone {
		log.Debugf("PingHandler: sendUdpMsg failed")
		return NgbMgrEnoUdp
	}

	strPeerNodeId := config.P2pNodeId2HexString(ping.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(ping.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == true {
		log.Debugf("PingHandler: checkMap failed")
		return NgbMgrEnoNone
	}

	schMsg := sch.SchMessage{}
	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingedInd, ping)
	ngbMgr.sdl.SchSendMessage(&schMsg)
	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) PongHandler(pong *um.Pong, from *net.UDPAddr) NgbMgrErrno {
	_ = from
	if ngbMgr.checkDestNode(&pong.To, pong.SubNetId, "pong") == false {
		log.Debugf("PongHandler: node identity mismatched")
		return NgbMgrEnoParameter
	}
	if expired(pong.Expiration) {
		log.Debugf("PongHandler: message expired")
		return NgbMgrEnoTimeout
	}

	strPeerNodeId := config.P2pNodeId2HexString(pong.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(pong.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) == false {
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPongedInd, pong)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}

	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypePing) {
		ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblPingpongRsp, pong)
		ngbMgr.sdl.SchSendMessage(&schMsg)
	}

	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) FindNodeHandler(findNode *um.FindNode, from *net.UDPAddr) NgbMgrErrno {
	if ngbMgr.checkDestNode(&findNode.To, findNode.SubNetId, "findNode") == false {
		log.Debugf("FindNodeHandler: node identity mismatched")
		return NgbMgrEnoParameter
	}
	if expired(findNode.Expiration) {
		log.Debugf("FindNodeHandler: message expired")
		return NgbMgrEnoTimeout
	}

	nodes := make([]*tab.Node, 0)
	umNodes := make([]*um.Node, 0)
	if findNode.SubNetId != config.AnySubNet {
		mgr := ngbMgr.tabMgr.TabGetInstBySubNetId(&findNode.SubNetId)
		if mgr == nil {
			log.Debugf("FindNodeHandler: no manager for subnet: %x", findNode.SubNetId)
			return NgbMgrEnoNotFound
		}
		nodes = append(nodes,
			mgr.TabClosest(tab.Closest4Queried,
				tab.NodeID(findNode.Target),
				findNode.MaskBits,
				tab.TabInstQPendingMax)...)
	} else {

		mgrs := ngbMgr.tabMgr.TabGetInstAll()
		if mgrs == nil {
			log.Debugf("FindNodeHandler: none of table managers found")
			return NgbMgrEnoNotFound
		}
		for _, mgr := range *mgrs {
			if mgr == nil {
				continue
			}
			nodes = append(nodes,
				mgr.TabClosest(tab.Closest4Queried,
					tab.NodeID(findNode.Target),
					findNode.MaskBits,
					tab.TabInstQPendingMax)...)
		}
	}

	cfgNode := config.Node{
		IP:  ngbMgr.cfg.IP,
		UDP: ngbMgr.cfg.UDP,
		TCP: ngbMgr.cfg.TCP,
		ID:  ngbMgr.cfg.ID,
	}

	for idx, n := range nodes {
		if n.ID == findNode.From.NodeId {
			if idx != len(nodes)-1 {
				nodes = append(nodes[:idx], nodes[idx+1:]...)
			} else {
				nodes = nodes[0:idx]
			}
			break
		}
	}

	if len(nodes) == 0 && findNode.SubNetId == config.AnySubNet {
		if !cfgNode.IP.IsUnspecified() {
			nodes = append(nodes, tab.TabBuildNode(&cfgNode))
		}
	} else if findNode.From.NodeId == findNode.Target {
		if !cfgNode.IP.IsUnspecified() {
			num := len(nodes)
			if num < tab.TabInstQPendingMax {
				nodes = append(nodes, tab.TabBuildNode(&cfgNode))
			} else {
				nodes[num-1] = tab.TabBuildNode(&cfgNode)
			}
		}
	}

	for _, n := range nodes {
		umn := um.Node{
			IP:     n.IP,
			UDP:    n.UDP,
			TCP:    n.TCP,
			NodeId: n.ID,
		}
		umNodes = append(umNodes, &umn)
	}

	neighbors := um.Neighbors{
		From:         findNode.To,
		To:           findNode.From,
		FromSubNetId: ngbMgr.cfg.SubNetIdList,
		SubNetId:     findNode.SubNetId,
		Id:           uint64(time.Now().UnixNano()),
		Nodes:        umNodes,
		Expiration:   0,
		Extra:        nil,
	}

	var toAddr net.UDPAddr
	if from != nil {
		toAddr = *from
	} else {
		toAddr = net.UDPAddr{
			IP:   findNode.From.IP,
			Port: int(findNode.From.UDP),
			Zone: "",
		}
	}

	pum := new(um.UdpMsg)
	if eno := pum.Encode(um.UdpMsgTypeNeighbors, &neighbors); eno != um.UdpMsgEnoNone {
		log.Debugf("FindNodeHandler: Encode failed")
		return NgbMgrEnoEncode
	}

	buf, bytes := pum.GetRawMessage()
	if buf == nil || bytes <= 0 {
		log.Debugf("FindNodeHandler: GetRawMessage failed")
		return NgbMgrEnoEncode
	}

	pum.DebugMessageToPeer()

	if eno := sendUdpMsg(ngbMgr.sdl, ngbMgr.ptnLsn, ngbMgr.ptnMe, buf, &toAddr); eno != sch.SchEnoNone {
		log.Debugf("FindNodeHandler: sendUdpMsg failed")
		return NgbMgrEnoUdp
	}

	strPeerNodeId := config.P2pNodeId2HexString(findNode.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(findNode.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeFindNode) == false {
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblQueriedInd, findNode)
		ngbMgr.sdl.SchSendMessage(&schMsg)
	}

	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) NeighborsHandler(nbs *um.Neighbors, from *net.UDPAddr) NgbMgrErrno {
	_ = from
	if ngbMgr.checkDestNode(&nbs.To, nbs.SubNetId, "neighbors") == false {
		log.Debugf("NeighborsHandler: node identity mismatched")
		return NgbMgrEnoParameter
	}
	if expired(nbs.Expiration) {
		log.Debugf("NeighborsHandler: message expired")
		return NgbMgrEnoTimeout
	}

	strPeerNodeId := config.P2pNodeId2HexString(nbs.From.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(nbs.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeFindNode) == false {
		log.Debugf("NeighborsHandler: not found, id: %s", strPeerNodeId)
		return NgbMgrEnoNotFound
	}

	ptnNgb := ngbMgr.getMap(strPeerNodeId).ptn
	schMsg := sch.SchMessage{}
	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptnNgb, sch.EvNblFindNodeRsp, nbs)
	ngbMgr.sdl.SchSendMessage(&schMsg)

	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) FindNodeReq(findNode *um.FindNode) NgbMgrErrno {
	var rsp = sch.NblFindNodeRsp{}
	var funcRsp2Tab = func() NgbMgrErrno {
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblFindNodeRsp, &rsp)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}

	strPeerNodeId := config.P2pNodeId2HexString(findNode.To.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(findNode.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {
		rsp.Result = (NgbMgrEnoDuplicated << 16) + tab.TabMgrEnoDuplicated
		rsp.FindNode = findNode
		return funcRsp2Tab()
	}

	ngbMgr.fnInstSeq++

	var ngbInst = neighborInst{
		sdl:     ngbMgr.sdl,
		ngbMgr:  ngbMgr,
		ptn:     nil,
		name:    strPeerNodeId,
		tskName: fmt.Sprintf("FindNode:%d:%s", ngbMgr.fnInstSeq, strPeerNodeId),
		msgType: um.UdpMsgTypeFindNode,
		msgBody: findNode,
		tidFN:   sch.SchInvalidTid,
		tidPP:   sch.SchInvalidTid,
	}
	ngbInst.tep = ngbInst.ngbProtoProc

	var noDog = sch.SchWatchDog{
		HaveDog: false,
	}
	var dc = sch.SchTaskDescription{
		Name:   ngbInst.tskName,
		MbSize: ngbProcMailboxSize,
		Ep:     &ngbInst,
		Wd:     &noDog,
		Flag:   sch.SchCreatedSuspend,
		DieCb:  ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := ngbMgr.sdl.SchCreateTask(&dc)
	if eno != sch.SchEnoNone {
		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode
		return funcRsp2Tab()
	}
	ngbInst.ptn = ptn

	if eno := ngbMgr.sdl.SchStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {
		rsp.Result = (NgbMgrEnoScheduler << 16) + tab.TabMgrEnoScheduler
		rsp.FindNode = findNode
		return funcRsp2Tab()
	}

	rsp.Result = NgbMgrEnoNone
	rsp.FindNode = findNode
	schMsg := sch.SchMessage{}
	ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblFindNodeReq, findNode)
	if ngbMgr.sdl.SchSendMessage(&schMsg) != sch.SchEnoNone {
		if ngbMgr.sdl.SchTaskDone(ngbInst.ptn, ngbInst.tskName, sch.SchEnoKilled) != sch.SchEnoNone {
			log.Errorf("FindNodeReq: SchTaskDone failed, sdl: %s", ngbMgr.sdlName)
		}
	}
	ngbMgr.setupMap(strPeerNodeId, &ngbInst)

	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) PingpongReq(ping *um.Ping) NgbMgrErrno {
	var rsp = sch.NblPingRsp{}
	var funcRsp2Tab = func() NgbMgrErrno {
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ngbMgr.ptnTab, sch.EvNblPingpongRsp, &rsp)
		ngbMgr.sdl.SchSendMessage(&schMsg)
		return NgbMgrEnoNone
	}
	var funcReq2Inst = func(ptn interface{}) NgbMgrErrno {
		schMsg := sch.SchMessage{}
		ngbMgr.sdl.SchMakeMessage(&schMsg, ngbMgr.ptnMe, ptn, sch.EvNblPingpongReq, ping)
		if ngbMgr.sdl.SchSendMessage(&schMsg) != sch.SchEnoNone {
			return NgbMgrEnoScheduler
		}
		return NgbMgrEnoNone
	}

	strPeerNodeId := config.P2pNodeId2HexString(ping.To.NodeId)
	strSubNetId := config.P2pSubNetId2HexString(ping.SubNetId)
	strPeerNodeId = strSubNetId + strPeerNodeId
	if ngbMgr.checkMap(strPeerNodeId, um.UdpMsgTypeAny) {
		rsp.Result = NgbMgrEnoDuplicated
		rsp.Ping = ping
		return funcRsp2Tab()
	}

	ngbMgr.ppInstSeq++

	var ngbInst = neighborInst{
		sdl:     ngbMgr.sdl,
		ngbMgr:  ngbMgr,
		ptn:     nil,
		name:    strPeerNodeId,
		tskName: fmt.Sprintf("Ping:%d:%s", ngbMgr.ppInstSeq, strPeerNodeId),
		msgType: um.UdpMsgTypePing,
		msgBody: ping,
		tidFN:   sch.SchInvalidTid,
		tidPP:   sch.SchInvalidTid,
	}
	ngbInst.tep = ngbInst.ngbProtoProc

	var noDog = sch.SchWatchDog{
		HaveDog: false,
	}

	var dc = sch.SchTaskDescription{
		Name:   ngbInst.tskName,
		MbSize: ngbProcMailboxSize,
		Ep:     &ngbInst,
		Wd:     &noDog,
		Flag:   sch.SchCreatedSuspend,
		DieCb:  ngbInst.NgbProtoDieCb,
		UserDa: &ngbInst,
	}

	eno, ptn := ngbMgr.sdl.SchCreateTask(&dc)
	if eno != sch.SchEnoNone {
		rsp.Result = NgbMgrEnoScheduler
		rsp.Ping = ping
		return funcRsp2Tab()
	}
	ngbInst.ptn = ptn

	if eno := ngbMgr.sdl.SchStartTaskEx(ngbInst.ptn); eno != sch.SchEnoNone {
		rsp.Result = NgbMgrEnoScheduler
		rsp.Ping = ping
		return funcRsp2Tab()
	}

	if eno := funcReq2Inst(ngbInst.ptn); eno != NgbMgrEnoNone {
		if ngbMgr.sdl.SchTaskDone(ngbInst.ptn, ngbInst.tskName, sch.SchEnoKilled) != sch.SchEnoNone {
			log.Errorf("PingpongReq: SchTaskDone failed, sdl: %s", ngbMgr.sdlName)
		}
		rsp.Result = int(eno)
		rsp.Ping = ping
		return funcRsp2Tab()
	}

	ngbMgr.setupMap(strPeerNodeId, &ngbInst)
	return NgbMgrEnoNone
}

func (ngbMgr *NeighborManager) CleanMapReq(name string) NgbMgrErrno {
	return ngbMgr.cleanMap(name)
}

func (ngbMgr *NeighborManager) setupMap(name string, inst *neighborInst) {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	ngbMgr.ngbMap[name] = inst
}

func (ngbMgr *NeighborManager) cleanMap(name string) NgbMgrErrno {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	if inst, ok := ngbMgr.ngbMap[name]; ok {
		ngbMgr.sdl.SchTaskDone(inst.ptn, inst.tskName, sch.SchEnoKilled)
		delete(ngbMgr.ngbMap, name)
		return NgbMgrEnoNone
	}
	return NgbMgrEnoNotFound
}

func (ngbMgr *NeighborManager) checkMap(name string, umt um.UdpMsgType) bool {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	ngb, ok := ngbMgr.ngbMap[name]
	if umt == um.UdpMsgTypeAny {
		return ok
	}
	return ok && ngb.msgType == umt
}

func (ngbMgr *NeighborManager) getMap(name string) *neighborInst {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	return ngbMgr.ngbMap[name]
}

func (ngbMgr *NeighborManager) localEndpoint() *um.Endpoint {
	return &um.Endpoint{
		IP:  ngbMgr.cfg.IP,
		UDP: ngbMgr.cfg.UDP,
		TCP: ngbMgr.cfg.TCP,
	}
}

func (ngbMgr *NeighborManager) localNode() *um.Node {
	return &um.Node{
		IP:     ngbMgr.cfg.IP,
		UDP:    ngbMgr.cfg.UDP,
		TCP:    ngbMgr.cfg.TCP,
		NodeId: ngbMgr.cfg.ID,
	}
}

func (ngbMgr *NeighborManager) localSubNode(snid config.SubNetworkID) *um.Node {
	id := ngbMgr.getSubNodeId(snid)
	if id == nil {
		return nil
	}
	return &um.Node{
		IP:     ngbMgr.cfg.IP,
		UDP:    ngbMgr.cfg.UDP,
		TCP:    ngbMgr.cfg.TCP,
		NodeId: *id,
	}
}

func expired(ts uint64) bool {
	if ts == 0 {
		return false
	}
	return time.Unix(int64(ts), 0).Before(time.Now())
}

func (ngbMgr *NeighborManager) setupConfig() sch.SchErrno {
	var ptCfg *config.Cfg4UdpNgbManager = nil
	if ptCfg = config.P2pConfig4UdpNgbManager(ngbMgr.sdl.SchGetP2pCfgName()); ptCfg == nil {
		log.Debugf("setupConfig: P2pConfig4UdpNgbManager failed")
		return sch.SchEnoConfig
	}
	ngbMgr.cfg.IP = ptCfg.IP
	ngbMgr.cfg.UDP = ptCfg.UDP
	ngbMgr.cfg.TCP = ptCfg.TCP
	ngbMgr.cfg.ID = ptCfg.ID
	ngbMgr.cfg.NetworkType = ptCfg.NetworkType
	ngbMgr.cfg.SubNetNodeList = ptCfg.SubNetNodeList
	ngbMgr.cfg.SubNetIdList = ptCfg.SubNetIdList
	if len(ngbMgr.cfg.SubNetIdList) == 0 &&
		ngbMgr.cfg.NetworkType == config.P2pNetworkTypeDynamic {
		ngbMgr.cfg.SubNetIdList = append(ngbMgr.cfg.SubNetIdList, config.AnySubNet)
	}
	return sch.SchEnoNone
}

func (ngbMgr *NeighborManager) checkDestNode(dst *um.Node, snid config.SubNetworkID, tag string) bool {
	result := false
	if ngbMgr.bootstrap {
		result = ngbMgr.cfg.ID == dst.NodeId
	} else if snid == config.AnySubNet {
		result = true
	} else if me, ok := ngbMgr.cfg.SubNetNodeList[snid]; ok {
		result = me.ID == dst.NodeId
	}
	if !result {
		cnt := int64(1)
		if cnt, ok := ngbMgr.checkDestNodeStat[tag]; ok {
			cnt += 1
		}
		ngbMgr.checkDestNodeStat[tag] = cnt
		if cnt & 0x7f == 0 {
			log.Errorf("checkDestNode: sdl: %s, tag: %d, failed: %d",
				ngbMgr.sdlName, tag, cnt)
		}
	}
	return result
}

func (ngbMgr *NeighborManager) getSubNode(snid config.SubNetworkID) *config.Node {
	if me, ok := ngbMgr.cfg.SubNetNodeList[snid]; ok {
		return &me
	}
	return nil
}

func (ngbMgr *NeighborManager) getSubNodeId(snid config.SubNetworkID) *config.NodeID {
	if me, ok := ngbMgr.cfg.SubNetNodeList[snid]; ok {
		return &me.ID
	}
	return nil
}

func (ngbMgr *NeighborManager) natPubAddrSwitchInd(msg *sch.MsgNatPubAddrSwitchInd) NgbMgrErrno {
	ngbMgr.lock.Lock()
	defer ngbMgr.lock.Unlock()
	log.Debugf("natPubAddrSwitchInd: entered")
	for _, ngbInst := range ngbMgr.ngbMap {
		log.Debugf("natPubAddrSwitchInd: kill inst: %s", ngbInst.name)
		ngbMgr.sdl.SchTaskDone(ngbInst.ptn, ngbInst.tskName, sch.SchEnoKilled)
		delete(ngbMgr.ngbMap, ngbInst.name)
	}
	ngbMgr.cfg.IP = msg.PubIp
	ngbMgr.cfg.UDP = uint16(msg.PubPort)
	ngbMgr.cfg.TCP = uint16(msg.PubPort)
	return NgbMgrEnoNone
}

