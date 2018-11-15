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
	"sync"
	"time"
	"errors"
	"fmt"
	yep2p "github.com/yeeco/gyee/p2p"
	log "github.com/yeeco/gyee/p2p/logger"
	"github.com/yeeco/gyee/p2p/config"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	"github.com/yeeco/gyee/p2p/dht"
	"github.com/yeeco/gyee/p2p/peer"
	pepb "github.com/yeeco/gyee/p2p/peer/pb"
)

const (
	yesKeyBytes = 64											// key length in bytes
	yesMaxFindNode = 4											// max find node commands in pending
	yesMaxGetProvider = 4										// max get provider commands in pending
	yesMaxPutProvider = 4										// max put provider commands in pending
)

type yesKey [yesKeyBytes]byte									// key type

type yeShellManager struct {
	chainInst			*sch.Scheduler							// chain scheduler pointer
	ptnChainShell		interface{}								// chain shell manager task node pointer
	ptChainShMgr		*shellManager							// chain shell manager object
	dhtInst				*sch.Scheduler							// dht scheduler pointer
	ptnDhtShell			interface{}								// dht shell manager task node pointer
	ptDhtShMgr			*dhtShellManager						// dht shell manager object
	getValChan			chan []byte								// get value channel
	putValChan			chan bool								// put value channel
	findNodeMap			map[config.NodeID]chan interface{}		// find node command map to channel
	getProviderMap		map[yesKey]chan interface{}				// find node command map to channel
	putProviderMap		map[yesKey]chan interface{}				// find node command map to channel
	dhtEvChan			chan *sch.MsgDhtShEventInd				// dht event indication channel
	dhtCsChan			chan *sch.MsgDhtConInstStatusInd		// dht connection status indication channel
	subscribers      	*sync.Map								// subscribers for incoming messages
	chainRxChan			chan *peer.P2pPackageRx					// total rx channel for chain
}

func NewYeshellManager(cfg *config.Config) *yeShellManager {
	yeShMgr := yeShellManager{}
	var eno sch.SchErrno
	var ok bool

	if cfg == nil {
		log.Debug("NewYeshellManager: nil configuration")
		return nil
	}

	yeShMgr.chainInst, eno = P2pCreateInstance(cfg)
	if eno != sch.SchEnoNone || yeShMgr.chainInst == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	eno, yeShMgr.ptnChainShell = yeShMgr.chainInst.SchGetUserTaskNode(sch.ShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnChainShell == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptChainShMgr, ok = yeShMgr.chainInst.SchGetTaskObject(sch.ShMgrName).(*shellManager)
	if !ok || yeShMgr.ptChainShMgr == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.dhtInst, eno = P2pCreateInstance(cfg)
	if eno != sch.SchEnoNone || yeShMgr.dhtInst == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	eno, yeShMgr.ptnDhtShell = yeShMgr.dhtInst.SchGetUserTaskNode(sch.DhtShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnDhtShell == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptDhtShMgr, ok = yeShMgr.dhtInst.SchGetTaskObject(sch.DhtShMgrName).(*dhtShellManager)
	if !ok || yeShMgr.ptDhtShMgr == nil {
		log.Debug("NewYeshellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.getValChan = make(chan []byte, 0)
	yeShMgr.putValChan = make(chan bool, 0)
	yeShMgr.findNodeMap = make(map[config.NodeID]chan interface{}, yesMaxFindNode)
	yeShMgr.getProviderMap = make(map[yesKey]chan interface{}, yesMaxGetProvider)
	yeShMgr.putProviderMap = make(map[yesKey]chan interface{}, yesMaxPutProvider)
	yeShMgr.dhtEvChan = yeShMgr.ptDhtShMgr.GetEventChan()
	yeShMgr.dhtCsChan = yeShMgr.ptDhtShMgr.GetConnStatusChan()
	yeShMgr.subscribers = new(sync.Map)
	yeShMgr.chainRxChan = yeShMgr.ptChainShMgr.GetRxChan()

	go yeShMgr.dhtEvProc()
	go yeShMgr.dhtCsProc()
	go yeShMgr.chainRxProc()

	return &yeShMgr
}

func (yeShMgr *yeShellManager)Start() error {
	if eno := P2pStart(yeShMgr.dhtInst); eno != sch.SchEnoNone {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	if eno := P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)Stop() {
	stopCh := make(chan bool, 0)
	P2pStop(yeShMgr.dhtInst, stopCh)
	P2pStop(yeShMgr.chainInst, stopCh)
}

func (yeShMgr *yeShellManager)BroadcastMessage(message yep2p.Message) error {
	var err error = nil
	switch message.MsgType {
	case yep2p.MessageTypeTx:
		err = yeShMgr.broadcastTx(&message)
	case yep2p.MessageTypeEvent:
		err = yeShMgr.broadcastEv(&message)
	case yep2p.	MessageTypeBlockHeader:
		err = yeShMgr.broadcastBh(&message)
	case yep2p.	MessageTypeBlock:
		err = yeShMgr.broadcastBk(&message)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessage: invalid type: %d", message.MsgType))
	}
	return err
}

func (yeShMgr *yeShellManager)BroadcastMessageOsn(message yep2p.Message) error {
	var err error = nil
	switch message.MsgType {
	case yep2p.MessageTypeTx:
		err = yeShMgr.broadcastTxOsn(&message)
	case yep2p.MessageTypeEvent:
		err = yeShMgr.broadcastEvOsn(&message)
	case yep2p.	MessageTypeBlockHeader:
		err = yeShMgr.broadcastBhOsn(&message)
	case yep2p.	MessageTypeBlock:
		err = yeShMgr.broadcastBkOsn(&message)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessageOsn: invalid type: %d", message.MsgType))
	}
	return err
}

func (yeShMgr *yeShellManager)Register(subscriber *yep2p.Subscriber) {
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.LoadOrStore(t, new(sync.Map))
	m.(*sync.Map).Store(subscriber, true)
}

func (yeShMgr *yeShellManager)UnRegister(subscriber *yep2p.Subscriber) {
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.Load(t)
	if m == nil {
		return
	}
	m.(*sync.Map).Delete(subscriber)
}

func (yeShMgr *yeShellManager)DhtGetValue(key []byte) ([]byte, error) {
	if len(key) != yesKeyBytes {
		log.Debug("DhtGetValue: invalid key: %x", key)
		return nil, sch.SchEnoParameter
	}

	req := sch.MsgDhtMgrGetValueReq{
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtGetValue: failed, eno: %d, error: %s", eno, eno.Error())
		return nil, eno
	}

	if val, ok := <-yeShMgr.getValChan; ok {
		return val, nil
	}
	return nil, errors.New("DhtGetValue: failed, channel closed")
}

func (yeShMgr *yeShellManager)DhtSetValue(key []byte, value []byte) error {
	if len(key) != yesKeyBytes || len(value) == 0 {
		log.Debug("DhtSetValue: invalid (key, value) pair, key: %x, length of value: %d", key, len(value))
		return sch.SchEnoParameter
	}

	req := sch.MsgDhtMgrPutValueReq {
		Key: key,
		Val: value,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtSetValue: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	result, ok := <-yeShMgr.putValChan
	if !ok {
		return errors.New("DhtSetValue: failed, channel closed")
	}
	if result == false {
		return errors.New("DhtSetValue: failed")
	}
	return nil
}

func (yeShMgr *yeShellManager)DhtFindNode(target *config.NodeID, done chan interface{}) error {
	if target == nil || done == nil {
		log.Debug("DhtFindNode: invalid parameters")
		return sch.SchEnoParameter
	}

	if len(yeShMgr.findNodeMap) >= yesMaxFindNode {
		log.Debug("DhtFindNode: too much, max: %d", yesMaxFindNode)
		return sch.SchEnoResource
	}

	if _, ok := yeShMgr.findNodeMap[*target]; ok {
		log.Debug("DhtFindNode: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtQryMgrQueryStartReq {
		Target:		*target,
		Msg:		nil,
		ForWhat:	dht.MID_FINDNODE,
		Seq:		time.Now().Unix(),
	}

	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrFindPeerReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtFindNode: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yeShMgr.findNodeMap[*target] = done
	return nil
}

func (yeShMgr *yeShellManager)DhtGetProvider(key []byte, done chan interface{}) error {
	if len(key) != yesKeyBytes {
		log.Debug("DhtGetProvider: invalid key: %x", key)
		return sch.SchEnoParameter
	}
	if done == nil {
		log.Debug("DhtGetProvider: invalid done channel")
		return sch.SchEnoParameter
	}

	var yk yesKey
	copy(yk[0:], key)
	if _, ok := yeShMgr.getProviderMap[yk]; ok {
		log.Debug("DhtGetProvider: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtMgrGetValueReq{
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtGetProvider: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yeShMgr.getProviderMap[yk] = done
	return nil
}

func (yeShMgr *yeShellManager)DhtSetProvider(key []byte, provider *config.Node, done chan interface{}) error {
	if len(key) != yesKeyBytes {
		log.Debug("DhtSetProvider: invalid key: %x", key)
		return sch.SchEnoParameter
	}
	if provider == nil || done == nil {
		log.Debug("DhtSetProvider: invalid parameters")
		return sch.SchEnoParameter
	}

	var yk yesKey
	copy(yk[0:], key)
	if _, ok := yeShMgr.getProviderMap[yk]; ok {
		log.Debug("DhtSetProvider: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtPrdMgrAddProviderReq{
		Key: key,
		Prd: *provider,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutProviderReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtSetProvider: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yeShMgr.putProviderMap[yk] = done
	return nil
}

func (yeShMgr *yeShellManager)dhtEvProc() {
	evCh := yeShMgr.dhtEvChan
	evHandler := func(evi *sch.MsgDhtShEventInd) {
		switch evi.Evt {
		case  sch.EvDhtBlindConnectRsp:
		case  sch.EvDhtMgrFindPeerRsp:
		case  sch.EvDhtQryMgrQueryStartRsp:
		case  sch.EvDhtQryMgrQueryStopRsp:
		case  sch.EvDhtConMgrSendCfm:
		case  sch.EvDhtMgrPutProviderRsp:
		case  sch.EvDhtMgrGetProviderRsp:
		case  sch.EvDhtMgrPutValueRsp:
		case  sch.EvDhtMgrGetValueRsp:
		case  sch.EvDhtConMgrCloseRsp:
		default:
			log.Debug("evHandler: invalid event: %d", evi.Evt)
		}
	}

_evLoop:
	for {
		select {
		case evi, ok := <-evCh:
			if ok {
				evHandler(evi)
			} else {
				log.Debug("dhtEvProc: channel closed")
				break _evLoop
			}
		}
	}
	log.Debug("dhtEvProc: exit")
}

func (yeShMgr *yeShellManager)dhtCsProc() {
	csCh := yeShMgr.dhtCsChan
	csHandler := func(csi *sch.MsgDhtConInstStatusInd) {
		switch csi.Status {
		case dht.CisNull:
		case dht.CisConnecting:
		case dht.CisConnected:
		case dht.CisAccepted:
		case dht.CisInHandshaking:
		case dht.CisHandshaked:
		case dht.CisInService:
		case dht.CisClosed:
		default:
			log.Debug("csHandler: invalid connection status: %d", csi.Status)
		}
	}

_csLoop:
	for {
		select {
		case csi, ok := <-csCh:
			if ok {
				csHandler(csi)
			} else {
				log.Debug("dhtCsProc: channel closed")
				break _csLoop
			}
		}
	}
	log.Debug("dhtCsProc: exit")
}

func (yeShMgr *yeShellManager)chainRxProc() {

	mid2Str := map[int] string {
		int(pepb.MessageId_MID_TX): yep2p.MessageTypeTx,
		int(pepb.MessageId_MID_EVENT): yep2p.MessageTypeEvent,
		int(pepb.MessageId_MID_BLOCKHEADER): yep2p.MessageTypeBlockHeader,
		int(pepb.MessageId_MID_BLOCK): yep2p.MessageTypeBlock,
	}

_rxLoop:
	for {
		select {
		case pkg, ok := <-yeShMgr.chainRxChan:
			if !ok {
				log.Debug("chainRxProc: channel closed")
				break _rxLoop
			}
			if pkg.ProtoId != int(peer.PID_EXT) {
				log.Debug("chainRxProc: invalid protocol identity: %d", pkg.ProtoId)
				continue
			}
			msgType := mid2Str[pkg.MsgId]
			if subList, ok := yeShMgr.subscribers.Load(msgType); ok {
				subList.(*sync.Map).Range(func(key, value interface{}) bool {
					msg := yep2p.Message {
						MsgType: msgType,
						From: fmt.Sprintf("%x", pkg.PeerInfo.NodeId),
						Data: pkg.Payload,
					}
					sub, _ := key.(*yep2p.Subscriber)
					sub.MsgChan<-msg
					return true
				})
			}
		}
	}
	log.Debug("chainRxProc: exit")
}

func (yeShMgr *yeShellManager)broadcastTx(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastEv(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastBh(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastBk(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastTxOsn(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastEvOsn(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastBhOsn(msg *yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)broadcastBkOsn(msg *yep2p.Message) error {
	return nil
}

