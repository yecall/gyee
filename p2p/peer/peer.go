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
	ggio "github.com/gogo/protobuf/io"
	ycfg	"github.com/yeeco/gyee/p2p/config"
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
	tab		"github.com/yeeco/gyee/p2p/discover/table"
	um		"github.com/yeeco/gyee/p2p/discover/udpmsg"
	yclog	"github.com/yeeco/gyee/p2p/logger"
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
type PeerId ycfg.NodeID

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

const durDcvFindNodeTimer = time.Second * 22		// duration to wait for find node response from discover task,
													// should be (findNodeExpiration + delta).

type peMgrConfig struct {
	maxPeers		int					// max peers would be
	maxOutbounds	int					// max concurrency outbounds
	maxInBounds		int					// max concurrency inbounds
	ip				net.IP				// ip address
	port			uint16				// tcp port number
	udp				uint16				// udp port number, used with handshake procedure
	nodeId			ycfg.NodeID			// the node's public key
	statics			[]*ycfg.Node		// statics nodes
	noDial			bool				// do not dial outbound
	bootstrapNode	bool				// local is a bootstrap node
	defaultCto		time.Duration		// default connect outbound timeout
	defaultHto		time.Duration		// default handshake timeout
	defaultAto		time.Duration		// default active read/write timeout
	maxMsgSize		int					// max tcpmsg package size
	protoNum		uint32				// local protocol number
	protocols		[]Protocol			// local protocol table
}

//
// Statistics history
//
type peHistory struct {
	tmBegin		time.Time	// time begin to count
	cntOk		int			// counter for succeed to establish
	cntFailed	int			// counter for failed to establish
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
	tidFindNode		int								// find node timer identity
	ptnMe			interface{}						// pointer to myself(peer manager task node)
	ptnTab			interface{}						// pointer to table task node
	ptnLsn			interface{}						// pointer to peer listener manager task node
	ptnAcp			interface{}						// pointer to peer acceptor manager task node
	ptnDcv			interface{}						// pointer to discover task node
	ibInstSeq		int								// inbound instance seqence number
	obInstSeq		int								// outbound instance seqence number
	peers			map[interface{}]*peerInstance	// map peer instance's task node pointer to instance pointer
	nodes			map[ycfg.NodeID]*peerInstance	// map peer node identity to instance pointer
	workers			map[ycfg.NodeID]*peerInstance	// map peer node identity to pointer of instance in work
	wrkNum			int								// worker peer number
	ibpNum			int								// active inbound peer number
	obpNum			int								// active outbound peer number
	acceptPaused	bool							// if accept task paused
	randoms			[]*ycfg.Node					// random nodes found by discover
	stats			map[ycfg.NodeID]peHistory		// history for successful and failed

	txLock			sync.Mutex						// lock for data sending action from shell
	Lock4Cb			sync.Mutex						// lock for indication callback
	P2pIndHandler	P2pInfIndCallback				// indication callback installed by p2p user from shell
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
		tidFindNode:  	sch.SchInvalidTid,
		ptnMe:        	nil,
		ptnTab:       	nil,
		ptnLsn:       	nil,
		peers:        	map[interface{}]*peerInstance{},
		nodes:        	map[ycfg.NodeID]*peerInstance{},
		workers:      	map[ycfg.NodeID]*peerInstance{},
		wrkNum:       	0,
		ibpNum:       	0,
		obpNum:       	0,
		acceptPaused: 	false,
		randoms:      	[]*ycfg.Node{},
		stats:        	map[ycfg.NodeID]peHistory{},
		P2pIndHandler:	nil,
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

	yclog.LogCallerFileLine("PeerMgrProc: " +
		"scheduled, sender: %s, recver: %s, msg: %d",
		sch.SchinfGetMessageSender(msg), sch.SchinfGetMessageRecver(msg), msg.Id)

	var schEno = sch.SchEnoNone
	var eno PeMgrErrno = PeMgrEnoNone

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = peMgr.peMgrPoweron(ptn)

	case sch.EvSchPoweroff:
		eno = peMgr.peMgrPoweroff(ptn)

	case sch.EvPeMgrStartReq:
		eno = peMgr.peMgrStartReq(msg.Body)

	case sch.EvDcvFindNodeRsp:
		eno = peMgr.peMgrDcvFindNodeRsp(msg.Body)

	case sch.EvPeDcvFindNodeTimer:
		eno = peMgr.peMgrDcvFindNodeTimerHandler()

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
		yclog.LogCallerFileLine("PeerMgrProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if eno != PeMgrEnoNone {
		yclog.LogCallerFileLine("PeerMgrProc: errors, eno: %d", eno)
		schEno = sch.SchEnoUserTask
	}

	return schEno
}

//
// Poweron event handler
//
func (peMgr *PeerManager)peMgrPoweron(ptn interface{}) PeMgrErrno {

	var eno = sch.SchEnoNone

	//
	// backup pointers of related tasks
	//

	peMgr.ptnMe	= ptn
	peMgr.sdl = sch.SchinfGetScheduler(ptn)
	eno, peMgr.ptnTab = peMgr.sdl.SchinfGetTaskNodeByName(sch.TabMgrName)

	if eno != sch.SchEnoNone || peMgr.ptnTab == nil {

		yclog.LogCallerFileLine("peMgrPoweron: " +
			"SchinfGetTaskNodeByName failed, eno: %df, target: %s",
			eno, sch.TabMgrName)

		peMgr.inited<-PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	eno, peMgr.ptnLsn = peMgr.sdl.SchinfGetTaskNodeByName(PeerLsnMgrName)
	if eno != sch.SchEnoNone || peMgr.ptnTab == nil {

		yclog.LogCallerFileLine("peMgrPoweron: " +
			"SchinfGetTaskNodeByName failed, eno: %df, target: %s",
			eno, PeerLsnMgrName)

		peMgr.inited<-PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	eno, peMgr.ptnDcv = peMgr.sdl.SchinfGetTaskNodeByName(sch.DcvMgrName)
	if eno != sch.SchEnoNone || peMgr.ptnDcv == nil {

		yclog.LogCallerFileLine("peMgrPoweron: " +
			"SchinfGetTaskNodeByName failed, eno: %d, target: %s",
			eno, sch.DcvMgrName)

		peMgr.inited<-PeMgrEnoScheduler
		return PeMgrEnoScheduler
	}

	//
	// fetch configration
	//

	var cfg *ycfg.Cfg4PeerManager = nil

	if cfg = ycfg.P2pConfig4PeerManager(peMgr.sdl.SchinfGetP2pCfgName()); cfg == nil {

		yclog.LogCallerFileLine("peMgrPoweron: P2pConfig4PeerManager failed")

		peMgr.inited<-PeMgrEnoConfig
		return PeMgrEnoConfig
	}

	peMgr.cfg = peMgrConfig {
		maxPeers:		cfg.MaxPeers,
		maxOutbounds:	cfg.MaxOutbounds,
		maxInBounds:	cfg.MaxInBounds,
		ip:				cfg.IP,
		port:			cfg.Port,
		udp:			cfg.UDP,
		nodeId:			cfg.ID,
		statics:		cfg.Statics,
		noDial:			cfg.NoDial,
		bootstrapNode:	cfg.BootstrapNode,
		defaultCto:		defaultConnectTimeout,
		defaultHto:		defaultHandshakeTimeout,
		defaultAto:		defaultActivePeerTimeout,
		maxMsgSize:		maxTcpmsgSize,
		protoNum:		cfg.ProtoNum,
		protocols:		make([]Protocol, 0),
	}

	for _, p := range cfg.Protocols {
		peMgr.cfg.protocols = append(peMgr.cfg.protocols,
			Protocol{ Pid:p.Pid, Ver:p.Ver,},
		)
	}

	//
	// tell initialization result
	//

	peMgr.inited<-PeMgrEnoNone

	yclog.LogCallerFileLine("peMgrPoweron: " +
		"EvPeMgrStartReq send ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

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

	if eno := peMgr.sdl.SchinfMakeMessage(&msg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeMgrStartReq, nil);

		eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("PeMgrStart: " +
			"SchinfMakeMessage failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	if eno := peMgr.sdl.SchinfSendMessage(&msg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("PeMgrStart: " +
			"SchinfSendMessage failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("PeMgrStart: " +
		"EvPeMgrStartReq sent ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

	return PeMgrEnoNone
}


//
// Poweroff event handler
//
func (peMgr *PeerManager)peMgrPoweroff(ptn interface{}) PeMgrErrno {

	yclog.LogCallerFileLine("peMgrPoweroff: pwoeroff received, done the task")

	if peMgr.tidFindNode != sch.SchInvalidTid {

		if eno := peMgr.sdl.SchinfKillTimer(peMgr.ptnMe, peMgr.tidFindNode); eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("peMgrPoweroff: SchinfKillTimer failed, eno: %d", eno)
			return PeMgrEnoScheduler
		}

		peMgr.tidFindNode = sch.SchInvalidTid
	}

	if eno := peMgr.sdl.SchinfTaskDone(ptn, sch.SchEnoKilled); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrPoweroff: SchinfTaskDone failed, eno: %d", eno)
		return PeMgrEnoScheduler
	}

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

	yclog.LogCallerFileLine("peMgrStartReq: going to start both inbound and outbound procedures")

	_ = msg

	var schMsg = sch.SchMessage{}
	var eno = sch.SchEnoNone

	//
	// start peer listener
	//

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnLsn, sch.EvPeLsnStartReq, nil)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrStartReq: " +
			"SchinfMakeMessage for EvPeLsnStartReq failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	eno = peMgr.sdl.SchinfSendMessage(&schMsg)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrStartReq: " +
			"SchinfSendMessage for EvPeLsnConnAcceptedInd failed, target: %s",
			peMgr.sdl.SchinfGetTaskName(peMgr.ptnLsn))

		return PeMgrEnoScheduler
	}

	//
	// drive ourself to startup outbound
	//

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrStartReq: " +
			"SchinfMakeMessage for EvPeOutboundReq failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	eno = peMgr.sdl.SchinfSendMessage(&schMsg)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrStartReq: " +
			"SchinfSendMessage for EvPeOutboundReq failed, target: %s",
			peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

//
// FindNode response handler
//
func (peMgr *PeerManager)peMgrDcvFindNodeRsp(msg interface{}) PeMgrErrno {

	//
	// Here we got response about FindNode from discover task, which should contain
	// nodes could be try to connect to. We should check the number of the active
	// active outbound peer number currently to carry out action accordingly.
	//

	var rsp = msg.(*sch.MsgDcvFindNodeRsp)

	if rsp == nil {
		yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: invalid FindNode response")
		return PeMgrEnoParameter
	}

	//
	// Deal with each node responsed
	//

	var appended = 0
	var dup bool

	for _, n := range rsp.Nodes {

		//
		// Check if duplicated instances
		//

		if _, ok := peMgr.nodes[n.ID]; ok {

			yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: " +
				"duplicated(nodes): %s", fmt.Sprintf("%X", n.ID))

			continue
		}

		//
		// Check if duplicated randoms
		//

		dup = false

		for _, rn := range peMgr.randoms {

			if rn.ID == n.ID {

				yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: " +
					"duplicated(randoms): %s", fmt.Sprintf("%X", n.ID))

				dup = true
				break
			}
		}

		if dup { continue }

		//
		// Check if duplicated statics
		//

		dup = false

		for _, s := range peMgr.cfg.statics {

			if s.ID == n.ID {

				yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: " +
					"duplicated(statics): %s", fmt.Sprintf("%X", n.ID))

				dup = true
				break
			}
		}

		if dup { continue }

		//
		// backup node, max to the number of most peers can be
		//

		peMgr.randoms = append(peMgr.randoms, n)

		if appended++; len(peMgr.randoms) >= peMgr.cfg.maxPeers {

			yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: too much, some are truncated")
			break
		}
	}

	//
	// drive ourself to startup outbound if some nodes appended
	//

	yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: appended: %d", appended)

	if appended > 0 {

		var schMsg sch.SchMessage

		eno := peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
		if eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: " +
				"SchinfMakeMessage for EvPeOutboundReq failed, eno: %d",
				eno)

			return PeMgrEnoScheduler
		}

		eno = peMgr.sdl.SchinfSendMessage(&schMsg)
		if eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("peMgrDcvFindNodeRsp: " +
				"SchinfSendMessage for EvPeOutboundReq failed, target: %s",
				peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

			return PeMgrEnoScheduler
		}
	}

	return PeMgrEnoNone
}

//
// handler of timer for find node response expired
//
func (peMgr *PeerManager)peMgrDcvFindNodeTimerHandler() PeMgrErrno {

	//
	// This timer is set after a find node request is sent peer manager to discover task.
	// When find node response from discover is received, if the timer still not expired,
	// it then should be removed. Notice that this is an absolute timer than a cycly one,
	// and when it's expired, we try findnode, and set the timer again. This is done by
	// calling function peMgrAsk4More, see it for details pls.
	//

	yclog.LogCallerFileLine("peMgrDcvFindNodeTimerHandler: " +
		"find node expired, try it again")

	return peMgr.peMgrAsk4More()
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
	// Check if more inbound allowed
	//

	if peMgr.ibpNum >= peMgr.cfg.maxInBounds {

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"no more resources, ibpNum: %d, max: %d",
			peMgr.ibpNum, peMgr.cfg.maxInBounds)

		ibInd.conn.Close()
		return PeMgrEnoResource
	}

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

	if eno, ptnInst = peMgr.sdl.SchinfCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"SchinfCreateTask failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	peInst.ptnMe = ptnInst

	//
	// Send handshake request to the instance created aboved
	//

	var schMsg = sch.SchMessage{}
	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeHandshakeReq, nil)

	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"SchinfSendMessage EvPeHandshakeReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peInst.ptnMe))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
		"send EvPeHandshakeReq ok, laddr: %s, raddr: %s, peer: %s, target: %s",
		peInst.laddr.String(),
		peInst.raddr.String(),
		fmt.Sprintf("%+v", peInst.node),
		peMgr.sdl.SchinfGetTaskName(peInst.ptnMe))

	//
	// Map the instance, notice that we do not konw the node identity yet since
	// this is an inbound connection just accepted at this moment.
	//

	peMgr.peers[peInst.ptnMe] = peInst

	//
	// Check if the accept task needs to be paused
	//

	if peMgr.ibpNum += 1;  peMgr.ibpNum >= peMgr.cfg.maxInBounds {

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"maxInbounds reached, try to pause accept task ...")

		accetper := peMgr.sdl.SchinfGetUserTaskIF(PeerAccepterName).(*acceptTskCtrlBlock)
		peMgr.acceptPaused = accetper.PauseAccept()

		yclog.LogCallerFileLine("peMgrLsnConnAcceptedInd: " +
			"pause result: %d", peMgr.acceptPaused)
	}

	return PeMgrEnoNone
}

//
// Outbound request handler
//
func (peMgr *PeerManager)peMgrOutboundReq(msg interface{}) PeMgrErrno {

	//
	// Notice: the event sch.EvPeOutboundReq, which is designed to drive the manager
	// to carry out the outbound action, when received, the manager should do its best
	// to start as many as possible outbound tasks, if the possible nodes are not
	// enougth, it then ask the discover task to find more.
	//
	// When event sch.EvPeMgrStartReq received, the manager should send itself a message
	// with event sch.EvPeOutboundReq, and while some other events recevied, the manager
	// should also send itself event sch.EvPeOutboundReq too.
	//
	// When the local node is configurated as "NoDial" or "bootstrap", outbound would
	// not be inited.
	//

	_ = msg


	if peMgr.cfg.noDial {

		yclog.LogCallerFileLine("peMgrOutboundReq: " +
			"abandon for noDial flag set: %t",
			peMgr.cfg.noDial)

		return PeMgrEnoNone
	}

	if peMgr.cfg.bootstrapNode {

		yclog.LogCallerFileLine("peMgrOutboundReq: " +
			"abandon for bootstrapNode flag set: %t",
			peMgr.cfg.bootstrapNode)

		return PeMgrEnoNone
	}

	//
	// Check workers number
	//

	if peMgr.wrkNum >= peMgr.cfg.maxPeers {
		yclog.LogCallerFileLine("peMgrOutboundReq: it's good, peers full")
		return PeMgrEnoNone
	}

	//
	// Check outbounds number
	//

	if peMgr.obpNum >= peMgr.cfg.maxOutbounds {
		yclog.LogCallerFileLine("peMgrOutboundReq: it's good, outbounds full")
		return PeMgrEnoNone
	}

	//
	// Collect all possible candidates, duplicated nodes should be filtered out
	//

	var candidates = make([]*ycfg.Node, 0)
	var count = 0

	for _, n := range peMgr.cfg.statics {
		if _, ok := peMgr.nodes[n.ID]; !ok {
			candidates = append(candidates, n)
			count++
		}
	}

	var rdCnt = 0

	for _, n := range peMgr.randoms {
		if _, ok := peMgr.nodes[n.ID]; !ok {
			candidates = append(candidates, n)
			count++
		}
		rdCnt++
	}

	if rdCnt > 0 {
		peMgr.randoms = append(peMgr.randoms[:0], peMgr.randoms[rdCnt:]...)
	}

	yclog.LogCallerFileLine("peMgrOutboundReq: " +
		"total number of candidates: %d", len(candidates))

	//
	// Create outbound instances for candidates if any
	//

	var failed = 0
	var ok = 0
	var duped = 0

	for _, n := range candidates {

		//
		// Check duplicated: it's needed here, since candidate nodes might be duplicated
		//

		if _, dup := peMgr.nodes[n.ID]; dup {

			yclog.LogCallerFileLine("peMgrOutboundReq: " +
				"duplicated node: %s",
				fmt.Sprintf("%X", n.ID))

			duped++
			continue
		}

		//
		// Create instance
		//

		if eno := peMgr.peMgrCreateOutboundInst(n); eno != PeMgrEnoNone {

			yclog.LogCallerFileLine("peMgrOutboundReq: " +
				"create outbound instance failed, eno: %d", eno)

			failed++
			continue
		}

		ok++

		//
		// Break if full
		//

		if peMgr.obpNum >= peMgr.cfg.maxOutbounds {

			yclog.LogCallerFileLine("peMgrOutboundReq: " +
				"too much candidates, the remains are discarded")

			break
		}
	}

	yclog.LogCallerFileLine("peMgrOutboundReq: " +
		"create outbound intances end, duped: %d, failed: %d, ok: %d, discarded: %d",
		duped,
		failed,
		ok,
		len(candidates) - duped - failed - ok)

	//
	// If outbounds are not enougth, ask discover to find more
	//

	if peMgr.obpNum < peMgr.cfg.maxOutbounds {

		if eno := peMgr.peMgrAsk4More(); eno != PeMgrEnoNone {

			yclog.LogCallerFileLine("peMgrOutboundReq: " +
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

		yclog.LogCallerFileLine("peMgrConnOutRsp: " +
			"outbound failed, result: %d, node: %s",
			rsp.result, fmt.Sprintf("%+v", rsp.peNode.ID))

		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

			yclog.LogCallerFileLine("peMgrConnOutRsp: " +
				"peMgrKillInst failed, eno: %d",
				eno)

			return eno
		}

		return PeMgrEnoNone
	}

	//
	// Send EvPeHandshakeReq to instance
	//

	var schMsg = sch.SchMessage{}
	var eno sch.SchErrno

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeHandshakeReq, nil)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnOutRsp: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnOutRsp: " +
			"SchinfSendMessage EvPeHandshakeReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(rsp.ptn))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrConnOutRsp: " +
		"send EvPeHandshakeReq ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(rsp.ptn))

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

		yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
			"instance not found, rsp: %s",
			fmt.Sprintf("%+v", *rsp))

		return PeMgrEnoNotfound
	}

	yclog.LogCallerFileLine("peMgrHandshakeRsp:" +
		"response for handshake received: %s",
		fmt.Sprintf("%+v", rsp))

	//
	// Check result, if failed, kill the instance
	//

	if rsp.result != PeMgrEnoNone {

		yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
			"handshake failed, result: %d, node: %s",
			rsp.result,
			fmt.Sprintf("%X", rsp.peNode.ID))

		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

			yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
				"peMgrKillInst failed, node: %s",
				fmt.Sprintf("%X", rsp.peNode.ID))

			return eno
		}

		return PeMgrEnoNone
	}

	//
	// Check duplicated for inbound instance. Notice: only here the peer manager can known the
	// identity of peer to determine if it's duplicated to a outbound instance, which is an
	// instance connect from local to the same peer.
	//

	if inst.dir == PeInstDirInbound {

		if _, dup := peMgr.nodes[rsp.peNode.ID]; dup {

			yclog.LogCallerFileLine("peMgrHandshakeRsp: "+
				"duplicated, node: %s",
				fmt.Sprintf("%X", rsp.peNode.ID))

			//
			// Here we could not kill instance rudely, the instance state should be
			// compared with each other to determine whom would be killed. Since here
			// handshake response received, the duplicated inbound instance must be
			// in "handshook" state.
			//

			var ptn2Kill interface{} = nil
			var node2Kill *ycfg.Node = nil

			dupInst := peMgr.nodes[rsp.peNode.ID]
			cmp := inst.state.compare(dupInst.state)

			if cmp < 0 {
				ptn2Kill = rsp.ptn
				node2Kill = rsp.peNode
			} else if cmp > 0 {
				ptn2Kill = dupInst.ptnMe
				node2Kill = &dupInst.node
			} else {
				if rand.Int() & 0x01 == 0 {
					ptn2Kill = rsp.ptn
					node2Kill = rsp.peNode
				} else {
					ptn2Kill = dupInst.ptnMe
					node2Kill = &dupInst.node
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

			yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
				"node2Kill: %s",
				fmt.Sprintf("%X", *node2Kill))

			if eno := peMgr.peMgrKillInst(ptn2Kill, node2Kill); eno != PeMgrEnoNone {

				yclog.LogCallerFileLine("peMgrHandshakeRsp: "+
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
		}
	}

	//
	// Send EvPeEstablishedInd to instance
	//

	var schMsg = sch.SchMessage{}
	var eno sch.SchErrno

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, rsp.ptn, sch.EvPeEstablishedInd, nil)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
			"SchinfSendMessage EvPeHandshakeReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(rsp.ptn))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrHandshakeRsp: " +
		"event EvPeEstablishedInd sent ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(rsp.ptn))

	//
	// Map the instance, notice that, only at this moment we can know the node
	// identity of a inbound peer.
	//

	inst.state = peInstStateActivated
	peMgr.workers[rsp.peNode.ID] = inst
	peMgr.wrkNum++

	if inst.dir == PeInstDirInbound {
		peMgr.nodes[inst.node.ID] = inst
	}

	//
	// Since the peer node is accepted and handshake is passed here now,
	// we add this peer node to bucket. But notice that this operation
	// possible fail for some reasons such as it's a duplicated one. We
	// should not care the result returned from interface of table module.
	//

	if inst.dir == PeInstDirInbound {

		lastQuery := time.Time{}
		lastPing := time.Now()
		lastPong := time.Now()

		n := um.Node{
			IP:     rsp.peNode.IP,
			UDP:    rsp.peNode.UDP,
			TCP:    rsp.peNode.TCP,
			NodeId: rsp.peNode.ID,
		}

		tabMgr := inst.sdl.SchinfGetUserTaskIF(sch.TabMgrName).(*tab.TableManager)
		tabEno := tabMgr.TabBucketAddNode(&n, &lastQuery, &lastPing, &lastPong)
		if tabEno != tab.TabMgrEnoNone {

			yclog.LogCallerFileLine("peMgrHandshakeRsp: "+
				"TabBucketAddNode failed, node: %s",
				fmt.Sprintf("%+v", *rsp.peNode))
		}

		//
		// Backup peer node to node database. Notice that this operation
		// possible fail for some reasons such as it's a duplicated one. We
		// should not care the result returned from interface of table module.
		//

		tabEno = tabMgr.TabUpdateNode(&n)
		if tabEno != tab.TabMgrEnoNone {

			yclog.LogCallerFileLine("peMgrHandshakeRsp: "+
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

		yclog.LogCallerFileLine("peMgrPingpongRsp: " +
			"outbound failed, result: %d, node: %s",
			rsp.result, ycfg.P2pNodeId2HexString(rsp.peNode.ID))

		if eno := peMgr.peMgrKillInst(rsp.ptn, rsp.peNode); eno != PeMgrEnoNone {

			yclog.LogCallerFileLine("peMgrPingpongRsp: " +
				"kill instance failed, inst: %s, node: %s",
				peMgr.sdl.SchinfGetTaskName(rsp.ptn),
				ycfg.P2pNodeId2HexString(rsp.peNode.ID)	)
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

	inst := peMgr.nodes[req.Node.ID]
	if inst == nil {

		yclog.LogCallerFileLine("peMgrCloseReq: " +
			"instance not found, ID: %s, ptn: %p",
			fmt.Sprintf("%X", req.Node.ID),
			req.Ptn)

		return PeMgrEnoNotfound
	}

	if inst.killing {

		yclog.LogCallerFileLine("peMgrCloseReq: instance already in killing")
		return PeMgrEnoDuplicaated
	}

	//
	// Send close-request to instance
	//

	var schMsg = sch.SchMessage{}
	var eno sch.SchErrno

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, req.Ptn, sch.EvPeCloseReq, &req)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrCloseReq: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrCloseReq: " +
			"SchinfSendMessage EvPeCloseReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(req.Ptn))

		return PeMgrEnoScheduler
	}

	inst.killing = true

	yclog.LogCallerFileLine("peMgrCloseReq: " +
		"SchinfSendMessage EvPeCloseReq ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(req.Ptn))

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
	// Do not care the result, kill always
	//

	if cfm.result != PeMgrEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseCfm, " +
			"result: %d, node: %s",
			cfm.result, ycfg.P2pNodeId2HexString(cfm.peNode.ID))
	}

	if eno = peMgr.peMgrKillInst(cfm.ptn, cfm.peNode); eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseCfm: " +
			"kill instance failed, inst: %s, node: %s",
			peMgr.sdl.SchinfGetTaskName(cfm.ptn),
			ycfg.P2pNodeId2HexString(cfm.peNode.ID))

		return PeMgrEnoScheduler
	}

	//
	// callback to the user of p2p to tell peer closed
	//

	peMgr.Lock4Cb.Lock()

	if peMgr.P2pIndHandler != nil {

		para := P2pIndPeerClosedPara {
			Ptn:		peMgr.ptnMe,
			PeerId:		PeerId(cfm.peNode.ID),
		}

		peMgr.P2pIndHandler(P2pIndPeerClosed, &para)

	} else {
		yclog.LogCallerFileLine("peMgrConnCloseCfm: indication callback not installed yet")
	}

	peMgr.Lock4Cb.Unlock()


	//
	// since we had lost a peer, we need to drive ourself to startup outbound
	//

	var schEno sch.SchErrno
	var schMsg = sch.SchMessage{}

	schEno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseCfm: " +
			"SchinfMakeMessage for EvPeOutboundReq failed, eno: %d",
			schEno)

		return PeMgrEnoScheduler
	}

	schEno = peMgr.sdl.SchinfSendMessage(&schMsg)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseCfm: " +
			"SchinfSendMessage for EvPeOutboundReq failed, target: %s",
			peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

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

	var ind = msg.(*MsgCloseInd)

	//
	// Do not care the result, kill always
	//

	yclog.LogCallerFileLine("peMgrConnCloseInd, " +
		"cause: %d, node: %s",
		ind.cause, ycfg.P2pNodeId2HexString(ind.peNode.ID))

	if eno := peMgr.peMgrKillInst(ind.ptn, ind.peNode); eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseInd: " +
			"kill instance failed, inst: %s, node: %s",
			peMgr.sdl.SchinfGetTaskName(ind.ptn),
			ycfg.P2pNodeId2HexString(ind.peNode.ID))

		return PeMgrEnoScheduler
	}

	//
	// callback to the user of p2p to tell peer closed
	//

	peMgr.Lock4Cb.Lock()

	if peMgr.P2pIndHandler != nil {

		para := P2pIndPeerClosedPara {
			PeerId:		PeerId(ind.peNode.ID),
		}

		peMgr.P2pIndHandler(P2pIndPeerClosed, &para)

	} else {
		yclog.LogCallerFileLine("peMgrConnCloseInd: indication callback not installed yet")
	}

	peMgr.Lock4Cb.Unlock()

	//
	// since we had lost a peer, we need to drive ourself to startup outbound
	//

	var schEno sch.SchErrno
	var schMsg = sch.SchMessage{}

	schEno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeOutboundReq, nil)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseInd: " +
			"SchinfMakeMessage for EvPeOutboundReq failed, eno: %d",
			schEno)

		return PeMgrEnoScheduler
	}

	schEno = peMgr.sdl.SchinfSendMessage(&schMsg)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrConnCloseInd: " +
			"SchinfSendMessage for EvPeOutboundReq failed, target: %s",
			peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	return PeMgrEnoNone
}

//
// Create outbound instance
//
func (peMgr *PeerManager)peMgrCreateOutboundInst(node *ycfg.Node) PeMgrErrno {

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

	if eno, ptnInst = peMgr.sdl.SchinfCreateTask(&tskDesc);
	eno != sch.SchEnoNone || ptnInst == nil {

		yclog.LogCallerFileLine("peMgrCreateOutboundInst: " +
			"SchinfCreateTask failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	peInst.ptnMe = ptnInst

	//
	// Send EvPeConnOutReq request to the instance created aboved
	//

	var schMsg = sch.SchMessage{}

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peInst.ptnMe, sch.EvPeConnOutReq, nil)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrCreateOutboundInst: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrCreateOutboundInst: " +
			"SchinfSendMessage EvPeHandshakeReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peInst.ptnMe))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrCreateOutboundInst: " +
		"send EvPeConnOutReq ok, node: %s",
		fmt.Sprintf("%X", peInst.node	))

	//
	// Map the instance
	//

	peMgr.peers[peInst.ptnMe] = peInst
	peMgr.nodes[peInst.node.ID] = peInst
	peMgr.obpNum++

	return PeMgrEnoNone
}

//
// Kill specific instance
//
func (peMgr *PeerManager)peMgrKillInst(ptn interface{}, node *ycfg.Node) PeMgrErrno {

	//
	// Notice: when an instance is activated into state peInstStateActivated,
	// it then must not be killed by calling this function directly, instead,
	// peer.ClosePeer should be called, and this function would be invoked
	// later when evnet EvPeCloseCfm received.
	//

	//
	// Get task node pointer, if "ptn" is nil, we try to get it by "node"
	//

	if ptn == nil && node == nil {
		yclog.LogCallerFileLine("peMgrKillInst: invalid parameters")
		return PeMgrEnoParameter
	}

	if ptn == nil {

		if ptn = peMgr.nodes[node.ID].ptnMe; ptn == nil {

			yclog.LogCallerFileLine("peMgrKillInst: " +
				"instance not found, node: %s",
				ycfg.P2pNodeId2HexString(node.ID))

			return PeMgrEnoScheduler
		}
	}

	//
	// Get instance data area pointer, and if the connection is not nil
	// we close it so the instance would get out event it's blocked in
	// actions on its' connection.
	//
	// Notice: the possible pingpong timer should be closed before the
	// connection closing, since the timer handler would try to send ping
	// message on the connection. But since this function should be called
	// before peer activated, this seems not necessary, for pingpong timer
	// is still not be created for peer instance before its' activation.
	//

	var peInst = peMgr.peers[ptn]

	if peInst.ppTid != sch.SchInvalidTid {

		if eno := peMgr.sdl.SchinfKillTimer(ptn, peInst.ppTid); eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("peMgrKillInst: " +
				"SchinfKillTimer failed, eno: %d",
				eno)
		}

		peInst.ppTid = sch.SchInvalidTid
	}

	if peInst.conn != nil {

		peInst.conn.Close()
		peInst.conn = nil

		yclog.LogCallerFileLine("peMgrKillInst: " +
			"instance connection is closed and set to nil, peer: %s",
			fmt.Sprintf("%X", peInst.node.ID	))
	}

	//
	// Stop instance task
	//

	if eno := peMgr.sdl.SchinfStopTask(ptn); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrKillInst: " +
			"SchinfStopTask failed, eno: %d, task: %s",
			eno, peMgr.sdl.SchinfGetTaskName(ptn))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrKillInst: " +
		"done fired, peer: %s",
		fmt.Sprintf("%X", peInst.node.ID	))

	//
	// Remove maps for the node: we must check the instance state and connection
	// direction to step.
	//

	if peInst.state == peInstStateActivated {

		delete(peMgr.workers, peInst.node.ID)
		peMgr.wrkNum--
	}

	if peInst.dir == PeInstDirOutbound {

		delete(peMgr.nodes, peInst.node.ID)
		delete(peMgr.peers, ptn)

	} else if peInst.dir == PeInstDirInbound {

		delete(peMgr.peers, ptn)
		if peInst.state == peInstStateActivated {
			delete(peMgr.nodes, peInst.node.ID)
		}
	}

	if peInst.dir == PeInstDirOutbound {

		peMgr.obpNum--

	} else if peInst.dir == PeInstDirInbound {

		peMgr.ibpNum--

	} else {

		yclog.LogCallerFileLine("peMgrKillInst: " +
			"invalid peer instance direction: %d",
			peInst.dir)
	}

	yclog.LogCallerFileLine("peMgrKillInst: " +
		"map deleted, peer: %s",
		fmt.Sprintf("%X", peInst.node.ID	))

	//
	// Check if the accepter task paused, resume it if necessary
	//

	if peMgr.acceptPaused == true {
		accepter := peMgr.sdl.SchinfGetUserTaskIF(PeerAccepterName).(*acceptTskCtrlBlock)
		peMgr.acceptPaused = !accepter.ResumeAccept()
	}

	return PeMgrEnoNone
}

//
// Request the discover task to findout more node for outbound
//
func (peMgr *PeerManager)peMgrAsk4More() PeMgrErrno {

	//
	// Send EvDcvFindNodeReq to discover task. The filters “include" and
	// "exclude" are not applied currently.
	//

	more := peMgr.cfg.maxOutbounds - peMgr.obpNum

	if more <= 0 {

		yclog.LogCallerFileLine("peMgrAsk4More: " +
			"no more needed, obpNum: %d, max: %d",
			peMgr.obpNum,
			peMgr.cfg.maxOutbounds)

		return PeMgrEnoNone
	}

	var eno sch.SchErrno
	var schMsg = sch.SchMessage{}

	var req = sch.MsgDcvFindNodeReq {
		More:		more,
		Include:	nil,
		Exclude:	nil,
	}

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnDcv, sch.EvDcvFindNodeReq, &req)

	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrAsk4More: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("peMgrAsk4More: " +
			"SchinfSendMessage EvPeHandshakeReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peMgr.ptnDcv))

		return PeMgrEnoScheduler
	}

	var td = sch.TimerDescription {
		Name:	PeerMgrName + "_DcvFindNode",
		Utid:	sch.PeDcvFindNodeTimerId,
		Tmt:	sch.SchTmTypeAbsolute,
		Dur:	durDcvFindNodeTimer,
		Extra:	nil,
	}

	peMgr.tidFindNode = sch.SchInvalidTid

	//
	// if the findnode timer not still not expired, we kill it and set a new one,
	// but attention: if SchEnoNotFound returned while killing timer, we still go
	// ahead, see function sch.SchinfKillTimer for more please.
	//

	tid := peMgr.tidFindNode

	if tid != sch.SchInvalidTid {

		if eno = peMgr.sdl.SchinfKillTimer(peMgr.ptnMe, tid);
		eno != sch.SchEnoNone && eno != sch.SchEnoNotFound {

			yclog.LogCallerFileLine("peMgrAsk4More: " +
				"kill timer failed, eno: %d, tid: %d",
				eno, tid)

			return PeMgrEnoScheduler
		}

		if eno != sch.SchEnoNotFound {

			yclog.LogCallerFileLine("peMgrAsk4More: " +
				"timer not found, tid: %d",
				tid)
		}

		peMgr.tidFindNode = sch.SchInvalidTid
	}

	if eno, tid = peMgr.sdl.SchInfSetTimer(peMgr.ptnMe, &td);
	eno != sch.SchEnoNone || tid == sch.SchInvalidTid {

		yclog.LogCallerFileLine("peMgrAsk4More: " +
			"set timer sch.PeDcvFindNodeTimerId failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("peMgrAsk4More: " +
		"set timer sch.PeDcvFindNodeTimerId ok, tid: %d",
		tid)

	peMgr.tidFindNode = tid

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
const PeInstMaxP2packages	= 32				// max p2p packages pending to be sent
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
	node		ycfg.Node					// peer "node" information
	protoNum	uint32						// peer protocol number
	protocols	[]Protocol					// peer protocol table
	maxPkgSize	int							// max size of tcpmsg package
	ppTid		int							// pingpong timer identity
	p2pkgLock	sync.Mutex					// lock for p2p package tx-sync
	p2pkgRx		P2pInfPkgCallback			// incoming p2p package callback
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
	node:		ycfg.Node{},
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
	result	PeMgrErrno		// result of outbound connect action
	peNode 	*ycfg.Node		// target node
	ptn		interface{}		// pointer to task instance node of sender
}

//
// EvPeHandshakeRsp message
//
type msgHandshakeRsp struct {
	result	PeMgrErrno		// result of handshake action
	peNode 	*ycfg.Node		// target node
	ptn		interface{}		// pointer to task instance node of sender
}

//
// EvPePingpongRsp message
//
type msgPingpongRsp struct {
	result	PeMgrErrno		// result of pingpong action
	peNode 	*ycfg.Node		// target node
	ptn		interface{}		// pointer to task instance node of sender
}

//
// EvPeCloseCfm message
//
type MsgCloseCfm struct {
	result	PeMgrErrno		// result of pingpong action
	peNode 	*ycfg.Node		// target node
	ptn		interface{}		// pointer to task instance node of sender
}

//
// EvPeCloseInd message
//
type MsgCloseInd struct {
	cause	PeMgrErrno	// tell why it's closed
	peNode 	*ycfg.Node	// target node
	ptn		interface{}	// pointer to task instance node of sender
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

	yclog.LogCallerFileLine("PeerInstProc: " +
		"scheduled, sender: %s, recver: %s, msg: %d",
		sch.SchinfGetMessageSender(msg), sch.SchinfGetMessageRecver(msg), msg.Id)

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
		yclog.LogCallerFileLine("PeerInstProc: invalid message: %d", msg.Id)
		eno = PeMgrEnoParameter
	}

	if eno != PeMgrEnoNone {
		yclog.LogCallerFileLine("PeerInstProc: instance errors, eno: %d", eno)
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
		yclog.LogCallerFileLine("piConnOutReq: instance mismatched")
		return PeMgrEnoInternal
	}

	yclog.LogCallerFileLine("piConnOutReq: " +
		"try outbound connect to target: %s, dir: %d, state: %d",
		fmt.Sprintf("%+v", inst.node),
		inst.dir,
		inst.state)

	//
	// Dial to peer node
	//

	var addr = &net.TCPAddr{IP: inst.node.IP, Port: int(inst.node.TCP)}
	var conn net.Conn = nil
	var err error
	var eno PeMgrErrno = PeMgrEnoNone

	inst.dialer.Timeout = inst.cto

	if conn, err = inst.dialer.Dial("tcp", addr.String()); err != nil {

		yclog.LogCallerFileLine("piConnOutReq: " +
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

		yclog.LogCallerFileLine("piConnOutReq: " +
			"dial ok, laddr: %s, raddr: %s",
			inst.laddr.String(),
			inst.raddr.String())
	}

	//
	// Response to peer manager task
	//

	var schEno sch.SchErrno
	var schMsg = sch.SchMessage{}
	var rsp = msgConnOutRsp {
		result:eno,
		peNode:&inst.node,
		ptn: inst.ptnMe,
	}

	schEno = inst.sdl.SchinfMakeMessage(&schMsg, inst.ptnMe, inst.ptnMgr, sch.EvPeConnOutRsp, &rsp)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piConnOutReq: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if schEno = inst.sdl.SchinfSendMessage(&schMsg); schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piConnOutReq: " +
			"SchinfSendMessage EvPeConnOutRsp failed, eno: %d, target: %s",
			schEno, inst.sdl.SchinfGetTaskName(inst.ptnMgr))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("piConnOutReq: " +
		"send EvPeConnOutRsp ok, target: %s",
		inst.sdl.SchinfGetTaskName(inst.ptnMgr))

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
		yclog.LogCallerFileLine("piHandshakeReq: invalid instance")
		return PeMgrEnoParameter
	}

	if inst.state != peInstStateConnected && inst.state != peInstStateAccepted {
		yclog.LogCallerFileLine("piHandshakeReq: instance mismatched")
		return PeMgrEnoInternal
	}

	if inst.conn == nil {
		yclog.LogCallerFileLine("piHandshakeReq: invalid instance")
		return PeMgrEnoInternal
	}

	yclog.LogCallerFileLine("piHandshakeReq: " +
		"handshake request received, dir: %d. laddr: %s, raddr: %s",
		inst.dir,
		inst.laddr.String(),
		inst.raddr.String())

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

		yclog.LogCallerFileLine("piHandshakeReq: " +
			"invalid instance direction: %d",
			inst.dir)

		eno = PeMgrEnoInternal
	}

	yclog.LogCallerFileLine("piHandshakeReq: " +
			"handshake result: %d, dir: %d, laddr: %s, raddr: %s, peer: %s",
			eno,
			inst.dir,
			inst.laddr.String(),
			inst.raddr.String(),
			fmt.Sprintf("%+v", inst.node)	)

	//
	// response to peer manager with handshake result
	//

	var schEno sch.SchErrno
	var schMsg = sch.SchMessage{}

	var rsp = msgHandshakeRsp {
		result:	eno,
		peNode:	&inst.node,
		ptn:	inst.ptnMe,
	}

	schEno = inst.sdl.SchinfMakeMessage(&schMsg, inst.ptnMe, inst.ptnMgr, sch.EvPeHandshakeRsp, &rsp)
	if schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piHandshakeReq: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if schEno = inst.sdl.SchinfSendMessage(&schMsg); schEno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piHandshakeReq: " +
			"SchinfSendMessage EvPeConnOutRsp failed, eno: %d, target: %s",
			schEno, inst.sdl.SchinfGetTaskName(inst.ptnMgr))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("piHandshakeReq: " +
		"EvPeHandshakeRsp sent ok, target: %s, msg: %s",
		inst.sdl.SchinfGetTaskName(inst.ptnMgr),
		fmt.Sprintf("%+v", rsp))

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

		yclog.LogCallerFileLine("piPingpongReq: " +
			"nothing done, ppEno: %d",
			inst.ppEno)

		return PeMgrEnoResource
	}

	if inst.conn == nil {

		yclog.LogCallerFileLine("piPingpongReq: " +
			"connection had been closed")

		return PeMgrEnoResource
	}

	if msg != nil {

		yclog.LogCallerFileLine("piPingpongReq: " +
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

		yclog.LogCallerFileLine("piPingpongReq: " +
			"upkg.ping failed, eno: %d, peer: %s",
			eno,
			fmt.Sprintf("%X", inst.node.ID))

		inst.peMgr.Lock4Cb.Lock()

		inst.ppEno = eno

		if inst.peMgr.P2pIndHandler != nil {

			para := P2pIndConnStatusPara {
				Ptn:		inst.ptnMe,
				PeerInfo:	&Handshake {
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
			yclog.LogCallerFileLine("piPingpongReq: indication callback not installed yet")
		}

		inst.peMgr.Lock4Cb.Unlock()

		return eno
	}

	yclog.LogCallerFileLine("piPingpongReq: " +
		"ping sent ok: %s, peer: %s",
		fmt.Sprintf("%+v", ping),
		fmt.Sprintf("%X", inst.node.ID))

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
		yclog.LogCallerFileLine("piCloseReq: invalid parameters")
		return PeMgrEnoParameter
	}

	var eno = sch.SchEnoNone
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

		if eno = inst.sdl.SchinfKillTimer(inst.ptnMe, inst.ppTid); eno != sch.SchEnoNone {

			yclog.LogCallerFileLine("piCloseReq: " +
				"kill timer failed, task: %s, tid: %d, eno: %d",
				inst.sdl.SchinfGetTaskName(inst.ptnMe), inst.ppTid, eno)

			return PeMgrEnoScheduler
		}

		inst.ppTid = sch.SchInvalidTid
	}

	//
	// close connection
	//

	if inst.conn != nil {

		if err := inst.conn.Close(); err != nil {

			yclog.LogCallerFileLine("piCloseReq: " +
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
		peNode:	&node,
		ptn:	inst.ptnMe,
	}

	var schMsg = sch.SchMessage{}

	eno = inst.sdl.SchinfMakeMessage(&schMsg, inst.peMgr.ptnMe, inst.peMgr.ptnMe, sch.EvPeCloseCfm, &req)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piCloseReq: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = inst.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("piCloseReq: " +
			"SchinfSendMessage EvPeCloseCfm failed, eno: %d, target: %s",
			eno, inst.sdl.SchinfGetTaskName(inst.peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("piCloseReq: " +
		"EvPeCloseCfm sent ok, target: %s",
		inst.sdl.SchinfGetTaskName(inst.peMgr.ptnMe))

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

	yclog.LogCallerFileLine("piEstablishedInd: " +
		"instance will be activated, inst: %s",
		fmt.Sprintf("%+v", *inst))

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

	if schEno, tid = inst.sdl.SchInfSetTimer(inst.ptnMe, &tmDesc);
	schEno != sch.SchEnoNone || tid == sch.SchInvalidTid {

		yclog.LogCallerFileLine("piEstablishedInd: " +
			"set timer failed, eno: %d, tid: %d",
			schEno, tid)

		return PeMgrEnoScheduler
	}

	inst.ppTid = tid

	yclog.LogCallerFileLine("piEstablishedInd: " +
		"pingpong timer for heartbeat set ok, tid: %d",
		inst.ppTid)



	//
	// modify deadline of peer connection for we had set specific value while
	// handshake procedure. we set deadline to value 0, so action on connection
	// would be blocked until it's completed.
	//

	inst.txEno = PeMgrEnoNone
	inst.rxEno = PeMgrEnoNone
	inst.ppEno = PeMgrEnoNone

	//
	// setup IO writer and reader
	//
/*
	inst.conn.SetDeadline(time.Time{})

	w := inst.conn.(io.Writer)
	inst.iow = ggio.NewDelimitedWriter(w)

	r := inst.conn.(io.Reader)
	inst.ior = ggio.NewDelimitedReader(r, inst.maxPkgSize)
*/

	yclog.LogCallerFileLine("piEstablishedInd: " +
		"instance is in service now, inst: %s",
		fmt.Sprintf("%+v", *inst)	)

	//
	// callback to the user of p2p
	//

	inst.peMgr.Lock4Cb.Lock()

	if inst.peMgr.P2pIndHandler != nil {

		para := P2pIndPeerActivatedPara {
			Ptn: inst.ptnMe,
			PeerInfo: & Handshake {
				NodeId:		inst.node.ID,
				ProtoNum:	inst.protoNum,
				Protocols:	inst.protocols,
			},
		}

		inst.peMgr.P2pIndHandler(P2pIndPeerActivated, &para)

	} else {
		yclog.LogCallerFileLine("piEstablishedInd: indication callback not installed yet")
	}

	inst.peMgr.Lock4Cb.Unlock()

	//
	// :( here we go routines for tx/rx on the activated peer):
	//

	go piTx(inst)
	go piRx(inst)

	yclog.LogCallerFileLine("piEstablishedInd: " +
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

	yclog.LogCallerFileLine("piPingpongTimerHandler: " +
		"pingpong timer expired for inst: %s",
		fmt.Sprintf("%+v", *inst))

	//
	// Check the pingpong timer: when this expired event comes, the timer
	// might have been stop for instance currently in closing procedure.
	// We discard this event in this case.
	//

	if inst.ppTid == sch.SchInvalidTid {
		yclog.LogCallerFileLine("piPingpongTimerHandler: no timer, discarded")
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

		yclog.LogCallerFileLine("piPingpongTimerHandler: " +
			"call P2pIndHandler noping threshold reached, ppCnt: %d",
			inst.ppCnt)

		inst.peMgr.Lock4Cb.Lock()

		if inst.peMgr.P2pIndHandler != nil {

			para := P2pIndConnStatusPara {
				Ptn:		inst.ptnMe,
				PeerInfo:	&Handshake {
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
			yclog.LogCallerFileLine("piPingpongTimerHandler: indication callback not installed yet")
		}

		inst.peMgr.Lock4Cb.Unlock()

		//
		// close the peer instance
		//

		if eno := inst.sdl.SchinfMakeMessage(&schMsg, inst.ptnMe, inst.ptnMe, sch.EvPeCloseReq, nil);
		eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("piPingpongTimerHandler: " +
				"SchinfMakeMessage failed, eno: %d",
				eno)
			return PeMgrEnoScheduler
		}

		if eno := inst.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {
			yclog.LogCallerFileLine("piPingpongTimerHandler: " +
				"SchinfSendMessage failed, eno: %d, target: %s",
				eno,
				inst.sdl.SchinfGetTaskName(inst.ptnMe))
			return PeMgrEnoScheduler
		}

		yclog.LogCallerFileLine("piPingpongTimerHandler: " +
			"EvPeCloseReq sent ok, target: %s",
			inst.sdl.SchinfGetTaskName(inst.ptnMe))

		return PeMgrEnoNone
	}

	//
	// Send pingpong request
	//

	if eno := inst.sdl.SchinfMakeMessage(&schMsg, inst.ptnMe, inst.ptnMe, sch.EvPePingpongReq, nil);
	eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("piPingpongTimerHandler: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)
		return PeMgrEnoScheduler
	}

	if eno := inst.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {
		yclog.LogCallerFileLine("piPingpongTimerHandler: " +
			"SchinfSendMessage failed, eno: %d, target: %s",
			eno,
			inst.sdl.SchinfGetTaskName(inst.ptnMe))
		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("piPingpongTimerHandler: " +
		"EvPePingpongReq sent ok, target: %s",
		inst.sdl.SchinfGetTaskName(inst.ptnMe))

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

	yclog.LogCallerFileLine("piHandshakeInbound: " +
		"try to read the incoming Handshake from raddr: %s",
		inst.raddr.String())

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("piHandshakeInbound: " +
			"read inbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	yclog.LogCallerFileLine("piHandshakeInbound: " +
		"read handshake: %s, peer: %s",
		fmt.Sprintf("%+v", hs),
		inst.raddr.String())

	//
	// backup info about protocols supported by peer. notice that here we can
	// check against the ip and tcp port from handshake with that obtained from
	// underlying network, but we not now.
	//

	inst.protoNum = hs.ProtoNum
	inst.protocols = hs.Protocols
	inst.node.ID = hs.NodeId
	inst.node.IP = append(inst.node.IP, hs.IP...)
	inst.node.TCP = uint16(hs.TCP)
	inst.node.UDP = uint16(hs.UDP)

	//
	// write outbound handshake to remote peer
	//

	yclog.LogCallerFileLine("piHandshakeInbound: " +
		"write Handshake: %s, peer: %s",
		fmt.Sprintf("%+v", hs),
		inst.raddr.String())

	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = pi.peMgr.cfg.protocols

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("piHandshakeInbound: " +
			"write outbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	//
	// update instance state
	//

	inst.state = peInstStateHandshook

	yclog.LogCallerFileLine("piHandshakeInbound: " +
		"Handshake procedure completed, laddr: %s, raddr: %s, peer: %s",
		inst.laddr.String(),
		inst.raddr.String(),
		fmt.Sprintf("%+v", inst.node)	)

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

	hs.NodeId = pi.peMgr.cfg.nodeId
	hs.IP = append(hs.IP, pi.peMgr.cfg.ip ...)
	hs.UDP = uint32(pi.peMgr.cfg.udp)
	hs.TCP = uint32(pi.peMgr.cfg.port)
	hs.ProtoNum = pi.peMgr.cfg.protoNum
	hs.Protocols = append(hs.Protocols, pi.peMgr.cfg.protocols ...)

	yclog.LogCallerFileLine("piHandshakeOutbound: " +
		"write handshake: %s, peer: %s",
		fmt.Sprintf("%+v", hs),
		inst.raddr.String())

	if eno = pkg.putHandshakeOutbound(inst, hs); eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("piHandshakeOutbound: " +
			"write outbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	yclog.LogCallerFileLine("piHandshakeOutbound: " +
		"write outbound Handshake message ok, try to read the incoming Handshake ...")

	//
	// read inbound handshake from remote peer
	//

	if hs, eno = pkg.getHandshakeInbound(inst); hs == nil || eno != PeMgrEnoNone {

		yclog.LogCallerFileLine("piHandshakeOutbound: " +
			"read inbound Handshake message failed, eno: %d",
			eno)

		return eno
	}

	yclog.LogCallerFileLine("piHandshakeOutbound: " +
		"read handshake: %s, peer: %s",
		fmt.Sprintf("%+v", hs),
		inst.raddr.String())

	//
	// since it's an outbound peer, the peer node id is known before this
	// handshake procedure carried out, we can check against these twos,
	// and we update the remains.
	//

	if hs.NodeId != inst.node.ID {
		yclog.LogCallerFileLine("piHandshakeOutbound: node identity mismathced")
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

	yclog.LogCallerFileLine("piHandshakeOutbound: " +
		"the total Handshake procedure completed ok, laddr: %s, raddr: %s: peer: %s",
		inst.laddr.String(),
		inst.raddr.String(),
		fmt.Sprintf("%+v", inst.node)	)

	return PeMgrEnoNone
}

//
// Set callback for incoming packages
//
func SetP2pkgCallback(cb interface{}, ptn interface{}) PeMgrErrno {

	if ptn == nil {
		yclog.LogCallerFileLine("SetP2pkgCallback: invalid parameters")
		return PeMgrEnoParameter
	}

	sdl := sch.SchinfGetScheduler(ptn)
	inst := sdl.SchinfGetUserDataArea(ptn).(*peerInstance)

	if inst == nil {

		yclog.LogCallerFileLine("SetP2pkgCallback: " +
			"nil instance data area, task: %s",
			inst.sdl.SchinfGetTaskName(ptn))

		return PeMgrEnoUnknown
	}

	inst.p2pkgLock.Lock()
	defer inst.p2pkgLock.Unlock()

	if inst.p2pkgRx != nil {
		yclog.LogCallerFileLine("SetP2pkgCallback: old one will be overlapped")
	}
	inst.p2pkgRx = cb.(P2pInfPkgCallback)

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
		yclog.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter, nil
	}

	if len(pkg.IdList) == 0 {
		yclog.LogCallerFileLine("SendPackage: invalid parameter")
		return PeMgrEnoParameter, nil
	}

	peMgr := pkg.P2pInst.SchinfGetUserTaskIF(sch.PeerMgrName).(*PeerManager)
	peMgr.txLock.Lock()
	defer peMgr.txLock.Unlock()

	var failed = make([]*PeerId, 0)
	var inst *peerInstance = nil

	for _, pid := range pkg.IdList {

		if inst = peMgr.workers[ycfg.NodeID(pid)]; inst == nil {

			yclog.LogCallerFileLine("SendPackage: " +
				"instance not exist, id: %s",
				fmt.Sprintf("%X", pid))

			failed = append(failed, &pid)
			continue
		}

		inst.p2pkgLock.Lock()

		if len(inst.p2pkgTx) >= PeInstMaxP2packages {
			yclog.LogCallerFileLine("SendPackage: tx buffer full")
			failed = append(failed, &pid)
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

	yclog.LogCallerFileLine("SendPackage: seems failed to send packages to nodes, check it pls")

	return PeMgrEnoUnknown, failed
}

//
// Close connection to a peer
//
func (peMgr *PeerManager)ClosePeer(id *PeerId) PeMgrErrno {

	//
	// Notice: this function should only be called to kill instance when it
	// is in active state(peInstStateActivated), if it's not the case, one
	// should call peMgrKillInst to do that, see it pls.
	//

	//
	// get instance by its' identity passed in
	//

	var inst *peerInstance = nil

	if inst = peMgr.workers[ycfg.NodeID(*id)]; inst == nil {

		yclog.LogCallerFileLine("ClosePeer: " +
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
	var eno sch.SchErrno

	eno = peMgr.sdl.SchinfMakeMessage(&schMsg, peMgr.ptnMe, peMgr.ptnMe, sch.EvPeCloseReq, &req)
	if eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("ClosePeer: " +
			"SchinfMakeMessage failed, eno: %d",
			eno)

		return PeMgrEnoScheduler
	}

	if eno = peMgr.sdl.SchinfSendMessage(&schMsg); eno != sch.SchEnoNone {

		yclog.LogCallerFileLine("ClosePeer: " +
			"SchinfSendMessage EvPeCloseReq failed, eno: %d, target: %s",
			eno, peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

		return PeMgrEnoScheduler
	}

	yclog.LogCallerFileLine("ClosePeer: " +
		"EvPeCloseReq sent ok, target: %s",
		peMgr.sdl.SchinfGetTaskName(peMgr.ptnMe))

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

			yclog.LogCallerFileLine("piTx: done with: %d", done)

			inst.txExit<-done
			break txBreak

		default:
		}

		//
		// send user package, lock needed
		//

		if inst.txEno != PeMgrEnoNone {
			yclog.LogCallerFileLine("piTx: txEno: %d", inst.txEno)
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

			yclog.LogCallerFileLine("piTx: " +
				"send package, Pid: %d, PayloadLength: %d",
				upkg.Pid,
				upkg.PayloadLength)

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

				yclog.LogCallerFileLine("piTx: " +
					"call P2pIndHandler for SendPackage failed, eno: %d",
					eno)

				inst.txEno = eno

				inst.peMgr.Lock4Cb.Lock()

				if inst.peMgr.P2pIndHandler != nil {

					hs := Handshake {
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
					yclog.LogCallerFileLine("piTx: indication callback not installed yet")
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

			yclog.LogCallerFileLine("piRx: done with: %d", done)

			inst.rxExit<-done
			break rxBreak

		default:
		}

		//
		// try reading the peer
		//

		if inst.rxEno != PeMgrEnoNone {
			yclog.LogCallerFileLine("piRx: rxEno: %d", inst.rxEno)
			time.Sleep(time.Microsecond * 100)
			continue
		}

		yclog.LogCallerFileLine("piRx: try RecvPackage ...")

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

			yclog.LogCallerFileLine("piRx: " +
				"call P2pIndHandler for RecvPackage failed, eno: %d",
				eno)

			inst.peMgr.Lock4Cb.Lock()

			inst.rxEno = eno

			if inst.peMgr.P2pIndHandler != nil {

				hs := Handshake {
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
				yclog.LogCallerFileLine("piRx: indication callback not installed yet")
			}

			inst.peMgr.Lock4Cb.Unlock()

			continue
		}

		yclog.LogCallerFileLine("piRx: " +
			"package got, Pid: %d, PayloadLength: %d",
			upkg.Pid, upkg.PayloadLength)

		//
		// check the package received to filter out those not for p2p internal only
		//

		if upkg.Pid == uint32(PID_P2P) {

			if eno := inst.piP2pPkgProc(upkg); eno != PeMgrEnoNone {

				yclog.LogCallerFileLine("piRx: " +
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
				peerInfo.NodeId		= inst.node.ID
				peerInfo.ProtoNum	= inst.protoNum
				peerInfo.Protocols	= append(peerInfo.Protocols, inst.protocols...)

				pkgCb.Payload		= nil
				pkgCb.PeerInfo		= &peerInfo
				pkgCb.ProtoId		= int(upkg.Pid)
				pkgCb.PayloadLength	= int(upkg.PayloadLength)
				pkgCb.Payload		= append(pkgCb.Payload, upkg.Payload...)

				inst.p2pkgRx(&pkgCb)

			} else {
				yclog.LogCallerFileLine("piRx: package callback not installed yet")
			}

			inst.p2pkgLock.Unlock()

		} else {

			//
			// unknown protocol identity
			//

			yclog.LogCallerFileLine("piRx: " +
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
		yclog.LogCallerFileLine("piP2pPkgProc: invalid parameters")
		return PeMgrEnoParameter
	}

	if upkg.Pid != uint32(PID_P2P) {

		yclog.LogCallerFileLine("piP2pPkgProc: " +
			"not a p2p package, pid: %d",
			upkg.Pid)

		return PeMgrEnoMessage
	}

	if upkg.PayloadLength <= 0 {

		yclog.LogCallerFileLine("piP2pPkgProc: " +
			"invalid payload length: %d",
			upkg.PayloadLength)

		return PeMgrEnoMessage
	}

	if len(upkg.Payload) != int(upkg.PayloadLength) {

		yclog.LogCallerFileLine("piP2pPkgProc: " +
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

		yclog.LogCallerFileLine("piP2pPkgProc: " +
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

		yclog.LogCallerFileLine("piP2pPkgProc: MID_HANDSHAKE, discarded")
		return PeMgrEnoMessage

	case uint32(MID_PING):

		return pi.piP2pPingProc(msg.Ping)

	case uint32(MID_PONG):

		return pi.piP2pPongProc(msg.Pong)

	default:
		yclog.LogCallerFileLine("piP2pPkgProc: unknown mid: %d", msg.Mid)
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

		yclog.LogCallerFileLine("piP2pPingProc: " +
			"pong failed, eno: %d, pi: %s",
			eno,
			fmt.Sprintf("%+v", *pi))

		return eno
	}

	yclog.LogCallerFileLine("piP2pPingProc: " +
		"pong ok, ping: %s, pong: %s, inst: %s",
		fmt.Sprintf("%+v", *ping),
		fmt.Sprintf("%+v", pong),
		fmt.Sprintf("%+v", *pi))

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

	yclog.LogCallerFileLine("piP2pPongProc: " +
		"pong received: %s, pi: %s",
		fmt.Sprintf("%+v", pong),
		fmt.Sprintf("%+v", *pi))

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
