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

package shell

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	config "github.com/yeeco/gyee/p2p/config"
	dht "github.com/yeeco/gyee/p2p/dht"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	peer "github.com/yeeco/gyee/p2p/peer"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// debug
//
type chainShellLogger struct {
	debug__      bool
	debugForce__ bool
}

var chainLog = chainShellLogger{
	debug__:      false,
	debugForce__: false,
}

func (log chainShellLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

func (log chainShellLogger) ForceDebug(fmt string, args ...interface{}) {
	if log.debugForce__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// chain shell
//

const (
	ShMgrName  = sch.ShMgrName // name registered in scheduler
	rxChanSize = 128           // total rx chan capacity
)

type shellPeerID struct {
	snid   config.SubNetworkID // sub network identity
	dir    int                 // direct
	nodeId config.NodeID       // node identity
}

type shellPeerInst struct {
	shellPeerID                         // shell peer identity
	txChan      chan *peer.P2pPackage   // tx channel of peer instance
	rxChan      chan *peer.P2pPackageRx // rx channel of peer instance
	hsInfo      *peer.Handshake         // handshake info about peer
	pi          *peer.PeerInstance      // peer instance pointer
	status      int                     // active peer instance status
	txDiscrd    int64                   // number of messages discarded
}

const (
	pisActive  = iota // active status
	pisClosing        // in-closing status
)

type deDupKey struct {
	key  config.DsKey
	peer shellPeerID
}

type deDupVal struct {
	bcReq *sch.MsgShellBroadcastReq
	timer interface{}
}

const (
	chkkTime = time.Second * 8
	keyTime  = time.Second * 8
	MID_CHKK = peer.MID_CHKK
	MID_RPTK = peer.MID_RPTK
	MID_GCD  = peer.MID_GCD
	MID_PCD  = peer.MID_PCD
)

type ShellManager struct {
	sdl          *sch.Scheduler                      // pointer to scheduler
	name         string                              // my name
	tep          sch.SchUserTaskEp                   // task entry
	ptnMe        interface{}                         // pointer to task node of myself
	ptnPeMgr     interface{}                         // pointer to task node of peer manager
	ptnTabMgr    interface{}                         // pointer to task node of table manager
	ptnNgbMgr    interface{}                         // pointer to task node of neighbor manager
	ptrPeMgr     *peer.PeerManager                   // pointer to peer manager
	localSnid    []config.SubNetworkID               // local sub network identities
	localNode    map[config.SubNetworkID]config.Node // local sub nodes
	peerActived  map[shellPeerID]*shellPeerInst      // active peers
	peerLock     sync.Mutex                          // lock sync accessing to field "peerActived"
	rxChan       chan *peer.P2pPackageRx             // total rx channel, for rx packages from all instances
	deDup        bool                                // deduplication flag
	tmDedup      *dht.TimerManager                   // deduplication timer manager
	deDupKeyMap  map[config.DsKey]interface{}        // keys known in local node
	deDupMap     map[deDupKey]*deDupVal              // map for keys of messages had been sent
	deDupTiker   *time.Ticker                        // deduplication ticker
	deDupDone    chan bool                           // deduplication routine done channel
	deDupLock    sync.Mutex                          // deduplication lock
	deDupKeyLock sync.Mutex                          // deduplication key lock
}

//
// Create shell manager
//
func NewShellMgr() *ShellManager {
	shMgr := ShellManager{
		name:        ShMgrName,
		localSnid:   make([]config.SubNetworkID, 0),
		localNode:   make(map[config.SubNetworkID]config.Node, 0),
		peerActived: make(map[shellPeerID]*shellPeerInst, 0),
		rxChan:      make(chan *peer.P2pPackageRx, rxChanSize),
		deDup:       true,
	}

	if shMgr.deDup {
		shMgr.tmDedup = dht.NewTimerManager()
		shMgr.deDupKeyMap = make(map[config.DsKey]interface{}, 0)
		shMgr.deDupMap = make(map[deDupKey]*deDupVal, 0)
		shMgr.deDupTiker = time.NewTicker(dht.OneTick)
		shMgr.deDupDone = make(chan bool)
	}

	shMgr.tep = shMgr.shMgrProc
	return &shMgr
}

//
// Entry point exported to scheduler
//
func (shMgr *ShellManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return shMgr.tep(ptn, msg)
}

//
// Shell manager entry
//
func (shMgr *ShellManager) shMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	//chainLog.Debug("shMgrProc: name: %s, msg.Id: %d", shMgr.name, msg.Id)
	eno := sch.SchEnoUnknown
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = shMgr.powerOn(ptn)
	case sch.EvSchPoweroff:
		eno = shMgr.powerOff(ptn)
	case sch.EvShellPeerActiveInd:
		eno = shMgr.peerActiveInd(msg.Body.(*sch.MsgShellPeerActiveInd))
	case sch.EvShellPeerCloseCfm:
		eno = shMgr.peerCloseCfm(msg.Body.(*sch.MsgShellPeerCloseCfm))
	case sch.EvShellPeerCloseInd:
		eno = shMgr.peerCloseInd(msg.Body.(*sch.MsgShellPeerCloseInd))
	case sch.EvShellPeerAskToCloseInd:
		eno = shMgr.peerAskToCloseInd(msg.Body.(*sch.MsgShellPeerAskToCloseInd))
	case sch.EvShellReconfigReq:
		eno = shMgr.reconfigReq(msg.Body.(*sch.MsgShellReconfigReq))
	case sch.EvShellBroadcastReq:
		eno = shMgr.broadcastReq(msg.Body.(*sch.MsgShellBroadcastReq))
	case sch.EvShellSubnetUpdateReq:
		eno = shMgr.updateLocalSubnetInfo()
	case sch.EvShellGetChainInfoReq:
		eno = shMgr.getChainInfoReq(msg.Body.(*sch.MsgShellGetChainInfoReq))
	case sch.EvShellGetChainInfoRsp:
		eno = shMgr.getChainInfoRsp(msg.Body.(*sch.MsgShellGetChainInfoRsp))
	default:
		chainLog.Debug("shMgrProc: unknown event: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	//chainLog.Debug("shMgrProc: get out, name: %s, msg.Id: %d", shMgr.name, msg.Id)
	return eno
}

func (shMgr *ShellManager) powerOn(ptn interface{}) sch.SchErrno {
	shMgr.ptnMe = ptn
	shMgr.sdl = sch.SchGetScheduler(ptn)
	_, shMgr.ptnPeMgr = shMgr.sdl.SchGetUserTaskNode(sch.PeerMgrName)
	_, shMgr.ptnTabMgr = shMgr.sdl.SchGetUserTaskNode(sch.TabMgrName)
	_, shMgr.ptnNgbMgr = shMgr.sdl.SchGetUserTaskNode(sch.NgbLsnName)

	shMgr.ptrPeMgr = shMgr.sdl.SchGetTaskObject(sch.PeerMgrName).(*peer.PeerManager)
	shMgr.updateLocalSubnetInfo()

	if shMgr.deDup {
		if eno := shMgr.startDedup(); eno != sch.SchEnoNone {
			chainLog.Debug("powerOn: startDedup failed, eno: %d", eno)
			return eno
		}
	}
	return sch.SchEnoNone
}

func (shMgr *ShellManager) powerOff(ptn interface{}) sch.SchErrno {
	chainLog.Debug("powerOff: task will be done ...")
	close(shMgr.deDupDone)
	return shMgr.sdl.SchTaskDone(shMgr.ptnMe, shMgr.name, sch.SchEnoPowerOff)
}

func (shMgr *ShellManager) peerActiveInd(ind *sch.MsgShellPeerActiveInd) sch.SchErrno {
	txChan, _ := ind.TxChan.(chan *peer.P2pPackage)
	rxChan, _ := ind.RxChan.(chan *peer.P2pPackageRx)
	peerInfo, _ := ind.PeerInfo.(*peer.Handshake)
	pi, _ := ind.PeerInst.(*peer.PeerInstance)
	peerId := shellPeerID{
		snid:   peerInfo.Snid,
		nodeId: peerInfo.NodeId,
		dir:    peerInfo.Dir,
	}
	peerInst := shellPeerInst{
		shellPeerID: peerId,
		txChan:      txChan,
		rxChan:      rxChan,
		hsInfo:      peerInfo,
		pi:          pi,
		status:      pisActive,
	}

	shMgr.peerLock.Lock()
	if _, dup := shMgr.peerActived[peerId]; dup {
		chainLog.Debug("peerActiveInd: duplicated, peerId: %+v", peerId)
		shMgr.peerLock.Unlock()
		return sch.SchEnoUserTask
	}
	shMgr.peerActived[peerId] = &peerInst
	shMgr.peerLock.Unlock()

	chainLog.ForceDebug("peerActiveInd: snid: %x, dir: %d, peer ip: %s, port: %d",
		peerInfo.Snid, peerInfo.Dir, peerInfo.IP.String(), peerInfo.TCP)

	approc := func() {
		for {
			select {
			case rxPkg, ok := <-peerInst.rxChan:
				if !ok {
					chainLog.Debug("approc: exit for rxChan closed, peer info: %+v", *peerInfo)
					return
				}

				if shMgr.deDup == false {
					shMgr.rxChan <- rxPkg
					continue
				}

				if rxPkg.MsgId == int(MID_CHKK) {

					shMgr.checkKeyFromPeer(rxPkg)

				} else if rxPkg.MsgId == int(MID_RPTK) {

					shMgr.reportKeyFromPeer(rxPkg)

				} else if rxPkg.MsgId == int(MID_GCD) {

					if eno := shMgr.getChainDataFromPeer(rxPkg); eno != sch.SchEnoNone {
						chainLog.Debug("approc: GCD from peer discarded, eno: %d", eno)
					} else {
						shMgr.rxChan <- rxPkg
					}

				} else if rxPkg.MsgId == int(MID_PCD) {

					if eno := shMgr.putChainDataFromPeer(rxPkg); eno != sch.SchEnoNone {
						chainLog.Debug("approc: PCD from peer discarded, eno: %d", eno)
					} else {
						shMgr.rxChan <- rxPkg
					}

				} else {

					k := config.DsKey{}
					copy(k[0:], rxPkg.Key)
					skm := shMgr.setKeyMap(&k)

					if skm == SKM_OK {

						shMgr.rxChan <- rxPkg

					} else if skm == SKM_DUPLICATED {

						chainLog.Debug("approc: duplicated, key: %x", k)

					} else if skm == SKM_FAILED {

						chainLog.Debug("approc: setKeyMap failed")
					}
				}
			}
		}
	}

	go approc()

	return sch.SchEnoNone
}

func (shMgr *ShellManager) peerCloseCfm(cfm *sch.MsgShellPeerCloseCfm) sch.SchErrno {
	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()

	peerId := shellPeerID{
		snid:   cfm.Snid,
		nodeId: cfm.PeerId,
		dir:    cfm.Dir,
	}
	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		chainLog.ForceDebug("peerCloseCfm: peer not found, peerId: %+v", peerId)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisClosing {
		chainLog.ForceDebug("peerCloseCfm: status mismatched, status: %d, peerId: %+v", peerInst.status, peerId)
		return sch.SchEnoMismatched
	} else {
		hsInfo := peerInst.hsInfo
		chainLog.ForceDebug("peerCloseCfm: snid: %x, dir: %d, ip: %s", hsInfo.Snid, hsInfo.Dir, hsInfo.IP.String())
		delete(shMgr.peerActived, peerId)
		return sch.SchEnoNone
	}
}

func (shMgr *ShellManager) peerCloseInd(ind *sch.MsgShellPeerCloseInd) sch.SchErrno {
	// this would never happen since a peer instance would never kill himself in
	// current implement, instead, event EvShellPeerAskToCloseInd should be sent
	// to us to do this.
	panic("peerCloseInd: should never be called!!!")
}

func (shMgr *ShellManager) peerAskToCloseInd(ind *sch.MsgShellPeerAskToCloseInd) sch.SchErrno {
	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()

	why, _ := ind.Why.(string)
	peerId := shellPeerID{
		snid:   ind.Snid,
		nodeId: ind.PeerId,
		dir:    ind.Dir,
	}

	if peerInst, ok := shMgr.peerActived[peerId]; !ok {
		chainLog.ForceDebug("peerAskToCloseInd: not found, why: %s, snid: %x, dir: %d",
			why, ind.Snid, ind.Dir)
		return sch.SchEnoNotFound
	} else if peerInst.status != pisActive {
		chainLog.ForceDebug("peerAskToCloseInd: status mismatched, why: %s, snid: %x, dir: %d, status: %d",
			why, ind.Snid, ind.Dir, peerInst.status)
		return sch.SchEnoMismatched
	} else {
		peerInfo := peerInst.hsInfo
		chainLog.ForceDebug("peerAskToCloseInd: why: %s, snid: %x, dir: %d, peer ip: %s, port: %d",
			why, peerInfo.Snid, peerInfo.Dir, peerInfo.IP.String(), peerInfo.TCP)
		req := sch.MsgPeCloseReq{
			Ptn:  nil,
			Snid: peerId.snid,
			Node: config.Node{
				ID: peerId.nodeId,
			},
			Dir: peerId.dir,
			Why: sch.PEC_FOR_BEASKEDTO,
		}
		msg := sch.SchMessage{}
		shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnPeMgr, sch.EvPeCloseReq, &req)
		shMgr.sdl.SchSendMessage(&msg)
		peerInst.status = pisClosing
		return sch.SchEnoNone
	}
}

func (shMgr *ShellManager) GetRxChan() chan *peer.P2pPackageRx {
	return shMgr.rxChan
}

func (shMgr *ShellManager) reconfigReq(req *sch.MsgShellReconfigReq) sch.SchErrno {
	msg := sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnPeMgr, sch.EvShellReconfigReq, req)
	if eno := shMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		return eno
	}
	msg = sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnNgbMgr, sch.EvShellReconfigReq, req)
	if eno := shMgr.sdl.SchSendMessage(&msg); eno != sch.SchEnoNone {
		return eno
	}
	msg = sch.SchMessage{}
	shMgr.sdl.SchMakeMessage(&msg, shMgr.ptnMe, shMgr.ptnTabMgr, sch.EvShellReconfigReq, req)
	return shMgr.sdl.SchSendMessage(&msg)
}

func (shMgr *ShellManager) broadcastReq(req *sch.MsgShellBroadcastReq) sch.SchErrno {
	switch req.MsgType {
	case sch.MSBR_MT_EV, sch.MSBR_MT_TX, sch.MSBR_MT_BLKH, sch.MSBR_MT_BLK:
		if shMgr.deDup {
			key := config.DsKey{}
			copy(key[0:], req.Key)
			skm := shMgr.setKeyMap(&key)
			chainLog.Debug("broadcastReq: setKeyMap result skm: %d", skm)
			if skm == SKM_DUPLICATED || skm == SKM_FAILED {
				return sch.SchEnoUserTask
			}
		}

		for id, pe := range shMgr.peerActived {
			if pe.status != pisActive {
				chainLog.Debug("broadcastReq: not active, snid: %x, peer: %s", id.snid, pe.hsInfo.IP.String())
			} else {
				if req.Exclude == nil || (req.Exclude != nil && bytes.Compare(id.nodeId[0:], req.Exclude[0:]) != 0) {
					if shMgr.deDup == false {
						eno := shMgr.send2Peer(pe, req)
						chainLog.Debug("broadcastReq: send2Peer result eno: %d", eno)
					} else {
						eno := shMgr.checkKey(pe, id, req)
						chainLog.Debug("broadcastReq: checkKey result eno: %d", eno)
					}
				}
			}
		}
	default:
		chainLog.Debug("broadcastReq: invalid message type: %d", req.MsgType)
		return sch.SchEnoParameter
	}

	return sch.SchEnoNone
}

func (shMgr *ShellManager) bcr2Package(req *sch.MsgShellBroadcastReq) *peer.P2pPackage {
	pkg := new(peer.P2pPackage)
	pkg.Pid = uint32(peer.PID_EXT)
	pkg.Mid = uint32(req.MsgType)
	pkg.Key = req.Key
	pkg.PayloadLength = uint32(len(req.Data))
	pkg.Payload = req.Data
	return pkg
}

func (shMgr *ShellManager) send2Peer(spi *shellPeerInst, req *sch.MsgShellBroadcastReq) sch.SchErrno {
	if len(spi.txChan) >= cap(spi.txChan) {
		chainLog.Debug("send2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			spi.snid, spi.dir, spi.nodeId)
		if spi.txDiscrd += 1; spi.txDiscrd&0x1f == 0 {
			chainLog.Debug("send2Peer：sind: %x, dir: %d, txDiscrd: %d",
				spi.snid, spi.dir, spi.txDiscrd)
		}
		return sch.SchEnoResource
	}
	if pkg := shMgr.bcr2Package(req); pkg == nil {
		chainLog.Debug("send2Peer: bcr2Package failed")
		return sch.SchEnoUserTask
	} else {
		spi.txChan <- pkg
		return sch.SchEnoNone
	}
}

func (shMgr *ShellManager) startDedup() sch.SchErrno {
	go func() {
	_dedupLoop:
		for {
			select {
			case <-shMgr.deDupTiker.C:

				shMgr.deDupLock.Lock()
				shMgr.tmDedup.TickProc()
				shMgr.deDupLock.Unlock()

			case <-shMgr.deDupDone:

				shMgr.deDupTiker.Stop()
				break _dedupLoop
			}
		}
	}()
	return sch.SchEnoNone
}

func (shMgr *ShellManager) checkKey(pe *shellPeerInst, pid shellPeerID, req *sch.MsgShellBroadcastReq) sch.SchErrno {

	shMgr.deDupLock.Lock()
	defer shMgr.deDupLock.Unlock()

	ddk := deDupKey{}
	copy(ddk.key[0:], req.Key)
	ddk.peer = pid

	pai, ok := shMgr.peerActived[pid]
	if !ok {
		chainLog.Debug("checkKey: active peer not found, pid: %+v", pid)
		return sch.SchEnoNotFound
	}

	if _, dup := shMgr.deDupMap[ddk]; dup {
		chainLog.Debug("checkKey: duplicated, type: %d, ddk: %+v", req.MsgType, ddk)
		return sch.SchEnoDuplicated
	}

	ddv := deDupVal{
		bcReq: req,
		timer: nil,
	}

	if err := shMgr.checkKey2Peer(pai, &ddk); err != nil {
		chainLog.Debug("checkKey: checkKey2Peer failed")
		return sch.SchEnoUserTask
	}

	tm, err := shMgr.tmDedup.GetTimer(chkkTime, nil, shMgr.deDupTimerCb)
	if err != dht.TmEnoNone {
		chainLog.Debug("checkKey: GetTimer failed, error: %s", err.Error())
		return sch.SchEnoUserTask
	}

	shMgr.tmDedup.SetTimerData(tm, &ddk)
	ddv.timer = tm
	shMgr.deDupMap[ddk] = &ddv

	if err := shMgr.tmDedup.StartTimer(tm); err != dht.TmEnoNone {
		chainLog.Debug("checkKey: StartTimer failed, error: %s", err.Error())
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (shMgr *ShellManager) checkKeyFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	upkg := new(peer.P2pPackage)
	upkg.Pid = uint32(rxPkg.ProtoId)
	upkg.Mid = uint32(rxPkg.MsgId)
	upkg.Key = rxPkg.Key
	upkg.PayloadLength = uint32(rxPkg.PayloadLength)
	upkg.Payload = rxPkg.Payload

	msg := peer.ExtMessage{}
	if eno := upkg.GetExtMessage(&msg); eno != peer.PeMgrEnoNone {
		chainLog.Debug("checkKeyFromPeer: GetExtMessage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if msg.Mid != uint32(MID_CHKK) {
		chainLog.Debug("checkKeyFromPeer: message type mismatched, mid: %d", msg.Mid)
		return sch.SchEnoUserTask
	}

	chainLog.Debug("checkKeyFromPeer: %s", msg.Chkk.String())

	key := config.DsKey{}
	copy(key[0:], rxPkg.Key)

	spid := shellPeerID{
		snid:   rxPkg.PeerInfo.Snid,
		dir:    rxPkg.PeerInfo.Dir,
		nodeId: rxPkg.PeerInfo.NodeId,
	}

	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()

	pai, ok := shMgr.peerActived[spid]
	if !ok {
		chainLog.Debug("checkKeyFromPeer: active peer not found, spid: %+v", spid)
		return sch.SchEnoNotFound
	}

	if pai.status != pisActive {
		chainLog.Debug("checkKeyFromPeer: peer not active, spid: %+v", spid)
		return sch.SchEnoNotFound
	}

	shMgr.deDupKeyLock.Lock()
	status := int32(peer.KS_NOTEXIST)
	if _, dup := shMgr.deDupKeyMap[key]; dup {
		status = int32(peer.KS_EXIST)
	}
	shMgr.deDupKeyLock.Unlock()

	if err := shMgr.reportKey2Peer(pai, &key, status); err != nil {
		chainLog.Debug("checkKeyFromPeer: reportKey2Peer failed, err: %s", err.Error())
		return sch.SchEnoUserTask
	}

	return sch.SchEnoNone
}

func (shMgr *ShellManager) reportKeyFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	upkg := new(peer.P2pPackage)
	upkg.Pid = uint32(rxPkg.ProtoId)
	upkg.Mid = uint32(rxPkg.MsgId)
	upkg.Key = rxPkg.Key
	upkg.PayloadLength = uint32(rxPkg.PayloadLength)
	upkg.Payload = rxPkg.Payload

	msg := peer.ExtMessage{}
	if eno := upkg.GetExtMessage(&msg); eno != peer.PeMgrEnoNone {
		chainLog.Debug("reportKeyFromPeer: GetExtMessage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if msg.Mid != uint32(MID_RPTK) {
		chainLog.Debug("reportKeyFromPeer: message type mismatched, mid: %d", msg.Mid)
		return sch.SchEnoUserTask
	}

	chainLog.Debug("reportKeyFromPeer: %s", msg.Rptk.String())

	spid := shellPeerID{
		snid:   rxPkg.PeerInfo.Snid,
		dir:    rxPkg.PeerInfo.Dir,
		nodeId: rxPkg.PeerInfo.NodeId,
	}

	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()

	pai, ok := shMgr.peerActived[spid]
	if !ok {
		chainLog.Debug("reportKeyFromPeer: active peer not found, spid: %+v", spid)
		return sch.SchEnoNotFound
	}

	if pai.status != pisActive {
		chainLog.Debug("reportKeyFromPeer: peer not active, spid: %+v", spid)
		return sch.SchEnoNotFound
	}

	shMgr.deDupLock.Lock()
	ddk := deDupKey{}
	copy(ddk.key[0:], rxPkg.Key)
	ddk.peer = spid
	ddv, ok := shMgr.deDupMap[ddk]

	if !ok {
		chainLog.Debug("reportKeyFromPeer: not found, ddk: %+v", ddk)
		shMgr.deDupLock.Unlock()
		return sch.SchEnoNotFound
	}

	shMgr.tmDedup.KillTimer(ddv.timer)
	delete(shMgr.deDupMap, ddk)
	shMgr.deDupLock.Unlock()

	if msg.Rptk.Status == int32(peer.KS_NOTEXIST) {
		return shMgr.send2Peer(pai, ddv.bcReq)
	}

	return sch.SchEnoNone
}

func (shMgr *ShellManager) getChainDataFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()

	spid := shellPeerID{
		snid:   rxPkg.PeerInfo.Snid,
		dir:    rxPkg.PeerInfo.Dir,
		nodeId: rxPkg.PeerInfo.NodeId,
	}
	pai, ok := shMgr.peerActived[spid]
	if !ok {
		chainLog.Debug("getChainDataFromPeer: active peer not found, spid: %+v", spid)
		return sch.SchEnoNotFound
	}
	if pai.status != pisActive {
		chainLog.Debug("getChainDataFromPeer: peer not active, spid: %+v", spid)
		return sch.SchEnoNotFound
	}
	return sch.SchEnoNone
}

func (shMgr *ShellManager) putChainDataFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	return sch.SchEnoNone
}

func (shMgr *ShellManager) deDupTimerCb(el *list.Element, data interface{}) interface{} {
	// Notice: do not invoke Lock ... Unlock ... on shMgr.deDupLock here
	// please, since this function is called back within TickProc of timer
	// manager when any timer get expired. See function startDedup.
	ddk, ok := data.(*deDupKey)
	if !ok {
		chainLog.Debug("deDupTimerCb: invalid timer data")
		panic("deDupTimerCb: invalid data")
	}

	if ddv, ok := shMgr.deDupMap[*ddk]; ok {
		shMgr.tmDedup.KillTimer(ddv.timer)
		delete(shMgr.deDupMap, *ddk)
		return nil
	}

	return errors.New(fmt.Sprintf("deDupTimerCb: not found, ddk: %+v", *ddk))
}

func (shMgr *ShellManager) checkKey2Peer(spi *shellPeerInst, ddk *deDupKey) error {
	if len(spi.txChan) >= cap(spi.txChan) {
		chainLog.Debug("checkKey2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			spi.snid, spi.dir, spi.nodeId)
		if spi.txDiscrd += 1; spi.txDiscrd&0x1f == 0 {
			chainLog.Debug("checkKey2Peer：sind: %x, dir: %d, txDiscrd: %d",
				spi.snid, spi.dir, spi.txDiscrd)
		}
		return sch.SchEnoResource
	}
	chkk := peer.CheckKey{}
	chkk.Key = append(chkk.Key, ddk.key[0:]...)
	upkg := new(peer.P2pPackage)
	if eno := upkg.CheckKey(spi.pi, &chkk, false); eno != peer.PeMgrEnoNone {
		chainLog.Debug("checkKey2Peer: CheckKey failed, eno: %d", eno)
		return errors.New("checkKey2Peer: ReportKey failed")
	}
	spi.txChan <- upkg
	return nil
}

func (shMgr *ShellManager) reportKey2Peer(spi *shellPeerInst, key *config.DsKey, status int32) error {

	if len(spi.txChan) >= cap(spi.txChan) {
		chainLog.Debug("reportKey2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			spi.hsInfo.Snid, spi.hsInfo.Dir, spi.hsInfo.NodeId)
		if spi.txDiscrd += 1; spi.txDiscrd&0x1f == 0 {
			chainLog.Debug("reportKey2Peer：sind: %x, dir: %d, discardMessages: %d",
				spi.snid, spi.dir, spi.txDiscrd)
		}
		return sch.SchEnoResource
	}
	rptk := peer.ReportKey{}
	rptk.Key = append(rptk.Key, key[0:]...)
	rptk.Status = status
	upkg := new(peer.P2pPackage)
	if eno := upkg.ReportKey(spi.pi, &rptk, false); eno != peer.PeMgrEnoNone {
		chainLog.Debug("reportKey2Peer: ReportKey failed, eno: %d", eno)
		return errors.New("reportKey2Peer: ReportKey failed")
	}
	spi.txChan <- upkg
	return nil
}

func (shMgr *ShellManager) getChainData2Peer(spi *shellPeerInst, req *sch.MsgShellGetChainInfoReq) error {
	if len(spi.txChan) >= cap(spi.txChan) {
		chainLog.Debug("getChainData2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			spi.snid, spi.dir, spi.nodeId)
		if spi.txDiscrd += 1; spi.txDiscrd&0x1f == 0 {
			chainLog.Debug("getChainData2Peer：sind: %x, dir: %d, txDiscrd: %d",
				spi.snid, spi.dir, spi.txDiscrd)
		}
		return sch.SchEnoResource
	}
	gcd := peer.GetChainData {
		Seq: req.Seq,
		Name: req.Kind,
		Key: req.Key,
	}
	upkg := new(peer.P2pPackage)
	if eno := upkg.GetChainData(spi.pi, &gcd, false); eno != peer.PeMgrEnoNone {
		chainLog.Debug("getChainData2Peer: CheckKey failed, eno: %d", eno)
		return errors.New("getChainData2Peer: ReportKey failed")
	}
	spi.txChan <- upkg
	return nil
}

func (shMgr *ShellManager) putChainData2Peer(spi *shellPeerInst, rsp *sch.MsgShellGetChainInfoRsp) error {
	if len(spi.txChan) >= cap(spi.txChan) {
		chainLog.Debug("putChainData2Peer: discarded, tx queue full, snid: %x, dir: %d, peer: %x",
			spi.snid, spi.dir, spi.nodeId)
		if spi.txDiscrd += 1; spi.txDiscrd&0x1f == 0 {
			chainLog.Debug("putChainData2Peer：sind: %x, dir: %d, txDiscrd: %d",
				spi.snid, spi.dir, spi.txDiscrd)
		}
		return sch.SchEnoResource
	}
	pcd := peer.PutChainData {
		Seq: rsp.Seq,
		Name: rsp.Kind,
		Key: rsp.Key,
		Data: rsp.Data,
	}
	upkg := new(peer.P2pPackage)
	if eno := upkg.PutChainData(spi.pi, &pcd, false); eno != peer.PeMgrEnoNone {
		chainLog.Debug("putChainData2Peer: CheckKey failed, eno: %d", eno)
		return errors.New("putChainData2Peer: ReportKey failed")
	}
	spi.txChan <- upkg
	return nil
}

func (shMgr *ShellManager) updateLocalSubnetInfo() sch.SchErrno {
_update_again:
	snids, nodes := shMgr.ptrPeMgr.GetLocalSubnetInfo()
	if snids == nil || nodes == nil {
		chainLog.Debug("updateLocalSubnetInfo: peer manager had not be inited yet...")
		time.Sleep(time.Second)
		goto _update_again
	}
	shMgr.localSnid = snids
	shMgr.localNode = nodes
	return sch.SchEnoNone
}

func (shMgr *ShellManager)getChainInfoReq(msg *sch.MsgShellGetChainInfoReq) sch.SchErrno {
	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()
	failCount := 0
	for _, pe := range shMgr.peerActived {
		if pe.status == pisActive {
			if err := shMgr.getChainData2Peer(pe, msg); err != nil {
				chainLog.Debug("getChainInfoReq: getChainData2Peer failed, error: %s", err.Error())
				failCount += 1
			}
		}
	}
	if failCount == len(shMgr.peerActived) {
		return sch.SchEnoResource
	}
	return sch.SchEnoNone
}

func (shMgr *ShellManager)getChainInfoRsp(msg *sch.MsgShellGetChainInfoRsp) sch.SchErrno {
	peerInfo, ok := msg.Peer.(*peer.PeerInfo)
	if !ok {
		panic("getChainInfoRsp: invalid peer info pointer")
	}
	pid := shellPeerID {
		snid: peerInfo.Snid,
		dir: peerInfo.Dir,
		nodeId: peerInfo.NodeId,
	}
	shMgr.peerLock.Lock()
	defer shMgr.peerLock.Unlock()
	pai, ok := shMgr.peerActived[pid]
	if !ok || pai == nil {
		chainLog.Debug("getChainInfoRsp: peer not found: %+v", *peerInfo)
		return sch.SchEnoNotFound
	}
	if err := shMgr.putChainData2Peer(pai, msg); err != nil {
		chainLog.Debug("getChainInfoRsp: putChainData2Peer failed, error: %s", err.Error())
		return sch.SchEnoResource
	}
	return sch.SchEnoNone
}

const (
	SKM_OK = iota
	SKM_DUPLICATED
	SKM_FAILED
)

func (shMgr *ShellManager) setKeyMap(k *config.DsKey) int {
	shMgr.deDupKeyLock.Lock()
	if _, ok := shMgr.deDupKeyMap[*k]; ok {
		shMgr.deDupKeyLock.Unlock()
		return SKM_DUPLICATED
	}
	shMgr.deDupKeyLock.Unlock()

	// deDupLock.Lock !!!
	shMgr.deDupLock.Lock()

	tm, err := shMgr.tmDedup.GetTimer(keyTime, nil, shMgr.deDupKeyCb)
	if err != dht.TmEnoNone {
		chainLog.Debug("setKeyMap: GetTimer failed, error: %s", err.Error())
		shMgr.deDupLock.Unlock()
		return SKM_FAILED
	}
	shMgr.tmDedup.SetTimerData(tm, k)
	if shMgr.tmDedup.StartTimer(tm); err != dht.TmEnoNone {
		chainLog.Debug("setKeyMap: StartTimer failed, error: %s", err.Error())
		shMgr.deDupLock.Unlock()
		return SKM_FAILED
	}

	// deDupLock.Unlock !!!
	shMgr.deDupLock.Unlock()

	shMgr.deDupKeyLock.Lock()
	shMgr.deDupKeyMap[*k] = tm
	shMgr.deDupKeyLock.Unlock()

	return SKM_OK
}

func (shMgr *ShellManager) deDupKeyCb(el *list.Element, data interface{}) interface{} {
	shMgr.deDupKeyLock.Lock()
	defer shMgr.deDupKeyLock.Unlock()

	k, ok := data.(*config.DsKey)
	if !ok {
		chainLog.Debug("deDupKeyCb: invalid timer data")
		panic("deDupKeyCb: invalid timer data")
	}
	delete(shMgr.deDupKeyMap, *k)
	return nil
}
