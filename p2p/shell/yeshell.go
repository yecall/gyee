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
	"bytes"
	"container/list"
	yep2p "github.com/yeeco/gyee/p2p"
	log "github.com/yeeco/gyee/p2p/logger"
	"github.com/yeeco/gyee/p2p/config"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	"github.com/yeeco/gyee/p2p/dht"
	"github.com/yeeco/gyee/p2p/peer"
)

const (
	yesKeyBytes = config.DhtKeyLength											// key length in bytes
	yesMaxFindNode = 4											// max find node commands in pending
	yesMaxGetProvider = 4										// max get provider commands in pending
	yesMaxPutProvider = 4										// max put provider commands in pending
)

var yesMtAtoi = map[string]int{
	yep2p.MessageTypeTx:					sch.MSBR_MT_TX,
	yep2p.MessageTypeEvent:					sch.MSBR_MT_EV,
	yep2p.MessageTypeBlockHeader:			sch.MSBR_MT_BLKH,
	yep2p.MessageTypeBlock:					sch.MSBR_MT_BLK,
}

var yesMidItoa = map[int] string {
	int(sch.MSBR_MT_TX):					yep2p.MessageTypeTx,
	int(sch.MSBR_MT_EV):					yep2p.MessageTypeEvent,
	int(sch.MSBR_MT_BLKH):					yep2p.MessageTypeBlockHeader,
	int(sch.MSBR_MT_BLK):					yep2p.MessageTypeBlock,
}

type yesKey = config.DsKey										// key type

type yeShellManager struct {
	chainInst			*sch.Scheduler							// chain scheduler pointer
	ptnChainShell		interface{}								// chain shell manager task node pointer
	ptChainShMgr		*shellManager							// chain shell manager object
	dhtInst				*sch.Scheduler							// dht scheduler pointer
	ptnDhtShell			interface{}								// dht shell manager task node pointer
	ptDhtShMgr			*dhtShellManager						// dht shell manager object
	getValKey			yesKey									// key of value to get
	getValChan			chan []byte								// get value channel
	putValKey			yesKey									// key of value to put
	putValChan			chan bool								// put value channel
	findNodeMap			map[yesKey]chan interface{}				// find node command map to channel
	getProviderMap		map[yesKey]chan interface{}				// find node command map to channel
	putProviderMap		map[yesKey]chan interface{}				// find node command map to channel
	dhtEvChan			chan *sch.MsgDhtShEventInd				// dht event indication channel
	dhtCsChan			chan *sch.MsgDhtConInstStatusInd		// dht connection status indication channel
	subscribers      	*sync.Map								// subscribers for incoming messages
	chainRxChan			chan *peer.P2pPackageRx					// total rx channel for chain
	deDupLock			sync.Mutex								// lock for deduplication timer manager
	tmDedup				*dht.TimerManager						// deduplication timer manager
	deDupMap			map[[yesKeyBytes]byte]bool				// map for keys of messages had been sent
	deDupTiker			*time.Ticker							// deduplication ticker
	ddtChan				chan bool								// deduplication ticker channel
}

const MaxSubNetMaskBits	= 15	// max number of mask bits for sub network identity

type YeShellConfig struct {
	// Notice: in current stage, a simple configuration for p2p is applied, for total configuration
	// about p2p, see config.Config please.
	AppType				config.P2pAppType						// application type
	Name				string									// node name
	Validator			bool									// validator flag
	BootstrapNode		bool									// bootstrap node flag
	BootstrapNodes		[]string								// bootstrap nodes
	NodeDataDir			string									// node data directory
	NodeDatabase		string									// node database
	SubNetMaskBits		int										// mask bits for sub network identity
	EvKeepTime			time.Duration							// duration for events kept by dht
	DedupTime			time.Duration							// duration for deduplication cleanup timer
}

const (
	ChainCfgIdx = 0
	DhtCfgIdx = 1
)

// Default yee shell configuration for convenience
var DefaultYeShellConfig = YeShellConfig{
	AppType:		config.P2P_TYPE_ALL,
	Name:			"test",
	Validator:		true,
	BootstrapNode:	false,
	BootstrapNodes: []string {
		"4909CDF2A2C60BF1FE1E6BA849CC9297B06E00B54F0F8EB0F4B9A6AA688611FD7E43EDE402613761EC890AB46FE2218DC9B29FC47BE3AB8D1544B6C0559599AC@192.168.2.191:30303:30303",
	},
	NodeDataDir:	config.P2pDefaultDataDir(true),
	NodeDatabase:	"nodes",
	SubNetMaskBits:	0,
	EvKeepTime:		time.Minute * 8,
	DedupTime:		time.Second * 60,
}

// Global shell configuration: this var is set when function YeShellConfigToP2pCfg called,
// the p2p user should not change those fields other than "Validator" and "SubNetMaskBits"
// which can be reconfigurated, since YeShellConfigToP2pCfg should be called once only.
var YeShellCfg = YeShellConfig{}

func YeShellConfigToP2pCfg(yesCfg *YeShellConfig) []*config.Config {
	if yesCfg == nil {
		log.Debug("YeShellConfigToP2pCfg: nil configuration")
		return nil
	}

	if yesCfg.AppType != config.P2P_TYPE_ALL {
		log.Debug("YeShellConfigToP2pCfg: P2P_TYPE_ALL needed")
		return nil
	}

	YeShellCfg = *yesCfg

	cfg := []*config.Config{nil, nil}
	chainCfg := (*config.Config)(nil)
	dhtCfg := (*config.Config)(nil)

	if yesCfg.BootstrapNode {
		chainCfg = config.P2pDefaultBootstrapConfig(yesCfg.BootstrapNodes)
	} else {
		chainCfg = config.P2pDefaultConfig(yesCfg.BootstrapNodes)
	}

	if config.P2pSetupLocalNodeId(chainCfg) != config.PcfgEnoNone {
		log.Debug("YeShellConfigToP2pCfg: P2pSetupLocalNodeId failed")
		return nil
	}

	chainCfg.AppType = config.P2P_TYPE_CHAIN
	chainCfg.Name = yesCfg.Name
	chainCfg.NodeDataDir = yesCfg.NodeDataDir
	chainCfg.NodeDatabase = yesCfg.NodeDatabase

	if yesCfg.Validator {
		chainCfg.SubNetIdList = append(chainCfg.SubNetIdList, config.VSubNet)
		chainCfg.SubNetMaxPeers[config.VSubNet] = config.MaxPeers
		chainCfg.SubNetMaxOutbounds[config.VSubNet] = config.MaxOutbounds
		chainCfg.SubNetMaxInBounds[config.VSubNet] = config.MaxInbounds
	}

	if yesCfg.SubNetMaskBits > MaxSubNetMaskBits {
		log.Debug("YeShellConfigToP2pCfg: too big, SubNetMaskBits: %d", yesCfg.SubNetMaskBits)
		return nil
	}
	end := len(chainCfg.Local.ID) - 1
	snw := uint16((chainCfg.Local.ID[end-1] << 8) | chainCfg.Local.ID[end])
	snw = snw << uint(16 - yesCfg.SubNetMaskBits)
	snw = snw >> uint(16 - yesCfg.SubNetMaskBits)
	snid := config.SubNetworkID{
		byte((snw >> 8) & 0xff),
		byte(snw & 0xff),
	}
	chainCfg.SubNetIdList = append(chainCfg.SubNetIdList, snid)
	chainCfg.SubNetMaxPeers[snid] = config.MaxPeers
	chainCfg.SubNetMaxOutbounds[snid] = config.MaxOutbounds
	chainCfg.SubNetMaxInBounds[snid] = config.MaxInbounds

	chainCfg.SubNetIdList = append(chainCfg.SubNetIdList, config.AnySubNet)
	chainCfg.SubNetMaxPeers[config.AnySubNet] = config.MaxPeers
	chainCfg.SubNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
	chainCfg.SubNetMaxInBounds[config.AnySubNet] = config.MaxInbounds

	if chCfgName, eno := config.P2pSetConfig("chain", chainCfg); eno != config.PcfgEnoNone {
		log.Debug("YeShellConfigToP2pCfg: P2pSetConfig failed")
		return nil
	} else {
		chainCfg = config.P2pGetConfig(chCfgName)
	}

	dhtCfg = chainCfg
	dhtCfg.AppType = config.P2P_TYPE_DHT
	cfg[ChainCfgIdx] = chainCfg
	cfg[DhtCfgIdx] = dhtCfg

	return cfg
}

func NewYeShellManager(yesCfg *YeShellConfig) *yeShellManager {
	var eno sch.SchErrno
	yeShMgr := yeShellManager{}

	cfg := YeShellConfigToP2pCfg(yesCfg)
	if cfg == nil || len(cfg) != 2 {
		log.Debug("NewYeShellManager: YeShellConfigToP2pCfg failed")
		return nil
	}

	if cfg[0] == nil || cfg[1] == nil {
		log.Debug("NewYeShellManager: nil configuration")
		return nil
	}

	yeShMgr.chainInst, eno = P2pCreateInstance(cfg[ChainCfgIdx])
	if eno != sch.SchEnoNone || yeShMgr.chainInst == nil {
		log.Debug("NewYeShellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.dhtInst, eno = P2pCreateInstance(cfg[DhtCfgIdx])
	if eno != sch.SchEnoNone || yeShMgr.dhtInst == nil {
		log.Debug("NewYeShellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	return &yeShMgr
}

func (yeShMgr *yeShellManager)Start() error {
	var eno sch.SchErrno
	var ok bool

	if eno = P2pStart(yeShMgr.dhtInst); eno != sch.SchEnoNone {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	eno, yeShMgr.ptnChainShell = yeShMgr.chainInst.SchGetUserTaskNode(sch.ShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnChainShell == nil {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptChainShMgr, ok = yeShMgr.chainInst.SchGetTaskObject(sch.ShMgrName).(*shellManager)
	if !ok || yeShMgr.ptChainShMgr == nil {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	if eno := P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	eno, yeShMgr.ptnDhtShell = yeShMgr.dhtInst.SchGetUserTaskNode(sch.DhtShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnDhtShell == nil {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptDhtShMgr, ok = yeShMgr.dhtInst.SchGetTaskObject(sch.DhtShMgrName).(*dhtShellManager)
	if !ok || yeShMgr.ptDhtShMgr == nil {
		log.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.getValChan = make(chan []byte, 0)
	yeShMgr.putValChan = make(chan bool, 0)
	yeShMgr.findNodeMap = make(map[yesKey]chan interface{}, yesMaxFindNode)
	yeShMgr.getProviderMap = make(map[yesKey]chan interface{}, yesMaxGetProvider)
	yeShMgr.putProviderMap = make(map[yesKey]chan interface{}, yesMaxPutProvider)
	yeShMgr.dhtEvChan = yeShMgr.ptDhtShMgr.GetEventChan()
	yeShMgr.dhtCsChan = yeShMgr.ptDhtShMgr.GetConnStatusChan()
	yeShMgr.subscribers = new(sync.Map)
	yeShMgr.chainRxChan = yeShMgr.ptChainShMgr.GetRxChan()
	yeShMgr.deDupTiker = time.NewTicker(dht.OneTick)
	yeShMgr.ddtChan = make(chan bool, 1)

	go yeShMgr.dhtEvProc()
	go yeShMgr.dhtCsProc()
	go yeShMgr.chainRxProc()
	go yeShMgr.deDupTickerProc()

	return nil
}

func (yeShMgr *yeShellManager)Stop() {
	stopCh := make(chan bool, 1)

	log.Debug("Stop: close deduplication ticker")
	close(yeShMgr.ddtChan)

	log.Debug("Stop: stop chain")
	P2pStop(yeShMgr.dhtInst, stopCh)
	<-stopCh
	log.Debug("Stop: chain stopped")

	log.Debug("Stop: stop dht")
	P2pStop(yeShMgr.chainInst, stopCh)
	<-stopCh
	log.Debug("Stop: dht stopped")
}

func (yeShMgr *yeShellManager)Reconfig(reCfg *yep2p.RecfgCommand) error {
	if reCfg == nil {
		log.Debug("Reconfig: invalid parameter")
		return errors.New("nil reconfigurate command")
	}

	if reCfg.SubnetMaskBits <= 0 || reCfg.SubnetMaskBits > MaxSubNetMaskBits {
		log.Debug("")
		return errors.New(fmt.Sprintf("too much mask bits: %d", reCfg.SubnetMaskBits))
	}

	VSnidAdd := make([]config.SubNetworkID, 0)
	VSnidDel := make([]config.SubNetworkID, 0)
	SnidAdd := make([]config.SubNetworkID, 0)
	SnidDel := make([]config.SubNetworkID, 0)

	if reCfg.Validator && !YeShellCfg.Validator{
		VSnidAdd = append(VSnidAdd, config.VSubNet)
	} else if !reCfg.Validator && YeShellCfg.Validator {
		VSnidDel = append(VSnidDel, config.VSubNet)
	}

	if reCfg.SubnetMaskBits != YeShellCfg.SubNetMaskBits {
		oldSnid, err := yeShMgr.getSubnetIdentity(YeShellCfg.SubNetMaskBits)
		if err != nil {
			log.Debug("Reconfig: getSubnetIdentity failed, error: %s", err.Error())
			return err
		}
		newSnid, err := yeShMgr.getSubnetIdentity(reCfg.SubnetMaskBits)
		if err != nil {
			log.Debug("Reconfig: getSubnetIdentity failed, error: %s", err.Error())
			return err
		}
		SnidAdd = append(SnidAdd, newSnid)
		SnidDel = append(SnidDel, oldSnid)
	}

	req := sch.MsgShellReconfigReq {
		VSnidAdd: VSnidAdd,
		VSnidDel: VSnidDel,
		SnidAdd: SnidAdd,
		SnidDel: SnidDel,
	}

	msg := sch.SchMessage{}

	yeShMgr.chainInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellReconfigReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("Reconfig: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)BroadcastMessage(message yep2p.Message) error {
	// Notice: the function is not supported in fact, see handler function
	// in each case please.
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
	case yep2p.MessageTypeBlockHeader:
		err = yeShMgr.broadcastBhOsn(&message)
	case yep2p.MessageTypeBlock:
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

	copy(yeShMgr.getValKey[0:], key)
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
		KeepTime: time.Duration(0),
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Debug("DhtSetValue: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	copy(yeShMgr.putValKey[0:], key)
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

	key := *(*yesKey)(dht.RutMgrNodeId2Hash(*target))
	if _, ok := yeShMgr.findNodeMap[key]; ok {
		log.Debug("DhtFindNode: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtQryMgrQueryStartReq {
		Target:		key,
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

	yeShMgr.findNodeMap[key] = done
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

	req := sch.MsgDhtMgrGetProviderReq {
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetProviderReq, &req)
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

	req := sch.MsgDhtPrdMgrAddProviderReq {
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
	evHandler := func(evi *sch.MsgDhtShEventInd) error {
		eno := sch.SchEnoNone
		switch evi.Evt {

		case  sch.EvDhtBlindConnectRsp:
			eno = yeShMgr.dhtBlindConnectRsp(evi.Msg.(*sch.MsgDhtBlindConnectRsp))

		case  sch.EvDhtMgrFindPeerRsp:
			eno = yeShMgr.dhtMgrFindPeerRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryResultInd))

		case  sch.EvDhtQryMgrQueryStartRsp:
			eno = yeShMgr.dhtQryMgrQueryStartRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryStartRsp))

		case  sch.EvDhtQryMgrQueryStopRsp:
			eno = yeShMgr.dhtQryMgrQueryStopRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryStopRsp))

		case  sch.EvDhtConMgrSendCfm:
			eno = yeShMgr.dhtConMgrSendCfm(evi.Msg.(*sch.MsgDhtConMgrSendCfm))

		case  sch.EvDhtMgrPutProviderRsp:
			eno = yeShMgr.dhtMgrPutProviderRsp(evi.Msg.(*sch.MsgDhtPrdMgrAddProviderRsp))

		case  sch.EvDhtMgrGetProviderRsp:
			eno = yeShMgr.dhtMgrGetProviderRsp(evi.Msg.(*sch.MsgDhtMgrGetProviderRsp))

		case  sch.EvDhtMgrPutValueRsp:
			eno = yeShMgr.dhtMgrPutValueRsp(evi.Msg.(*sch.MsgDhtMgrPutValueRsp))

		case  sch.EvDhtMgrGetValueRsp:
			eno = yeShMgr.dhtMgrGetValueRsp(evi.Msg.(*sch.MsgDhtMgrGetValueRsp))

		case  sch.EvDhtConMgrCloseRsp:
			eno = yeShMgr.dhtConMgrCloseRsp(evi.Msg.(*sch.MsgDhtConMgrCloseRsp))

		default:
			log.Debug("evHandler: invalid event: %d", evi.Evt)
			eno = sch.SchEnoParameter
		}

		return eno
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
			log.Debug("dhtCsProc: CisNull, dir: %d, peer: %x", *csi.Peer)

		case dht.CisConnecting:
			log.Debug("dhtCsProc: CisConnecting, dir: %d, peer: %x", *csi.Peer)

		case dht.CisConnected:
			log.Debug("dhtCsProc: CisConnected, dir: %d, peer: %x", *csi.Peer)

		case dht.CisAccepted:
			log.Debug("dhtCsProc: CisAccepted, dir: %d, peer: %x", *csi.Peer)

		case dht.CisInHandshaking:
			log.Debug("dhtCsProc: CisInHandshaking, dir: %d, peer: %x", *csi.Peer)

		case dht.CisHandshaked:
			log.Debug("dhtCsProc: CisHandshaked, dir: %d, peer: %x", *csi.Peer)

		case dht.CisInService:
			log.Debug("dhtCsProc: CisInService, dir: %d, peer: %x", *csi.Peer)

		case dht.CisClosed:
			log.Debug("dhtCsProc: CisAccepted, dir: %d, peer: %x", *csi.Peer)

		default:
			log.Debug("dhtCsProc: invalid connection status: %d", csi.Status)
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
			msgType := yesMidItoa[pkg.MsgId]
			if subList, ok := yeShMgr.subscribers.Load(msgType); ok {
				subList.(*sync.Map).Range(func(key, value interface{}) bool {
					msg := yep2p.Message {
						MsgType: msgType,
						From: fmt.Sprintf("%x", pkg.PeerInfo.NodeId),
						Key: pkg.Key,
						Data: pkg.Payload,
					}

					err := error(nil)
					switch msg.MsgType {
					case yep2p.MessageTypeTx:
						err = yeShMgr.broadcastTxOsn(&msg)
					case yep2p.MessageTypeEvent:
						err = yeShMgr.broadcastEvOsn(&msg)
					case yep2p.MessageTypeBlockHeader:
						err = yeShMgr.broadcastBhOsn(&msg)
					case yep2p.MessageTypeBlock:
						err = yeShMgr.broadcastBkOsn(&msg)
					default:
						err = errors.New(fmt.Sprintf("chainRxProc: invalid message type: %s", msg.MsgType))
					}

					if err != nil {
						log.Debug("chainRxProc: error: %s", err.Error())
						return false
					}

					sub, _ := key.(*yep2p.Subscriber)
					sub.MsgChan <- msg
					return true
				})
			}
		}
	}
	log.Debug("chainRxProc: exit")
}

func (yeShMgr *yeShellManager)dhtBlindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	log.Debug("dhtBlindConnectRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrFindPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	log.Debug("dhtMgrFindPeerRsp: msg: %+v", *msg)
	if done, ok := yeShMgr.findNodeMap[msg.Target]; ok {
		done<-msg
		delete(yeShMgr.findNodeMap, msg.Target)
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtQryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	log.Debug("dhtQryMgrQueryStartRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtQryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	log.Debug("dhtQryMgrQueryStopRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtConMgrSendCfm(msg *sch.MsgDhtConMgrSendCfm) sch.SchErrno {
	log.Debug("dhtConMgrSendCfm: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrPutProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	log.Debug("dhtMgrPutProviderRsp: msg: %+v", *msg)
	if len(msg.Key) != yesKeyBytes {
		return sch.SchEnoParameter
	}
	yk := yesKey{}
	copy(yk[0:], msg.Key)
	if done, ok := yeShMgr.putProviderMap[yk]; ok {
		done<-msg
		delete(yeShMgr.putProviderMap, yk)
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrGetProviderRsp(msg *sch.MsgDhtMgrGetProviderRsp) sch.SchErrno {
	log.Debug("dhtMgrGetProviderRsp: msg: %+v", *msg)
	if len(msg.Key) != yesKeyBytes {
		return sch.SchEnoParameter
	}
	yk := yesKey{}
	copy(yk[0:], msg.Key)
	if done, ok := yeShMgr.getProviderMap[yk]; ok {
		done<-msg
		delete(yeShMgr.getProviderMap, yk)
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrPutValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	log.Debug("dhtMgrPutValueRsp: msg: %+v", *msg)
	if bytes.Compare(yeShMgr.putValKey[0:], msg.Key) != 0 {
		log.Debug("dhtMgrPutValueRsp: key mismatched")
		return sch.SchEnoMismatched
	}
	if msg.Eno == dht.DhtEnoNone.GetEno() {
		yeShMgr.putValChan<-true
	} else {
		yeShMgr.putValChan<-false
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrGetValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	log.Debug("dhtMgrGetValueRsp: msg: %+v", *msg)
	if bytes.Compare(yeShMgr.putValKey[0:], msg.Key) != 0 {
		log.Debug("dhtMgrGetValueRsp: key mismatched")
		return sch.SchEnoMismatched
	}
	if msg.Eno == dht.DhtEnoNone.GetEno() {
		yeShMgr.getValChan<-msg.Val
	} else {
		yeShMgr.getValChan<-[]byte{}
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtConMgrCloseRsp(msg *sch.MsgDhtConMgrCloseRsp) sch.SchErrno {
	log.Debug("dhtConMgrCloseRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)broadcastTx(msg *yep2p.Message) error {
	return errors.New("broadcastTx: not supported")
}

func (yeShMgr *yeShellManager)broadcastEv(msg *yep2p.Message) error {
	return errors.New("broadcastEv: not supported")
}

func (yeShMgr *yeShellManager)broadcastBh(msg *yep2p.Message) error {
	return errors.New("broadcastBh: not supported")
}

func (yeShMgr *yeShellManager)broadcastBk(msg *yep2p.Message) error {
	return errors.New("broadcastBk: not supported")
}

func (yeShMgr *yeShellManager)broadcastTxOsn(msg *yep2p.Message) error {
	// if local node is a validator, the Tx should be broadcast over the
	// validator-subnet; else the Tx should be broadcast over the dynamic
	// subnet. this is done in chain shell manager, and the message here
	// would be dispatched to chain shell manager.
	if err := yeShMgr.setDedupTimer(msg.Key); err != nil {
		log.Debug("broadcastTxOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Data: msg.Data,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debug("broadcastTxOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *yeShellManager)broadcastEvOsn(msg *yep2p.Message) error {
	// the local node must be a validator, and the Ev should be broadcast
	// over the validator-subnet. also, the Ev should be stored into DHT
	// with a duration to be expired. the message here would be dispatched
	// to chain shell manager and DHT shell manager.
	if err := yeShMgr.setDedupTimer(msg.Key); err != nil {
		log.Debug("broadcastEvOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Data: msg.Data,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	req2Dht := sch.MsgDhtMgrPutValueReq {
		Key: msg.Key,
		Val: msg.Data,
		KeepTime: YeShellCfg.EvKeepTime,
	}
	yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req2Dht)
	if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)broadcastBhOsn(msg *yep2p.Message) error {
	// the Bh should be broadcast over the any-subnet. the message here
	// would be dispatched to chain shell manager.
	if err := yeShMgr.setDedupTimer(msg.Key); err != nil {
		log.Debug("broadcastBhOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Data: msg.Data,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debug("broadcastBhOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *yeShellManager)broadcastBkOsn(msg *yep2p.Message) error {
	// the Bk should be stored by DHT and no broadcasting over any subnet.
	// the message here would be dispatched to DHT shell manager.
	if err := yeShMgr.setDedupTimer(msg.Key); err != nil {
		log.Debug("broadcastBkOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgDhtMgrPutValueReq {
		Key: msg.Key,
		Val: msg.Data,
		KeepTime: dht.DsMgrDurInf,
	}
	yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		log.Debug("broadcastBkOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *yeShellManager)getSubnetIdentity(maskBits int) (config.SubNetworkID, error) {
	if maskBits <= 0 || maskBits > MaxSubNetMaskBits {
		return config.SubNetworkID{}, errors.New("invalid mask bits")
	}
	localId := yeShMgr.chainInst.SchGetP2pConfig().Local.ID
	end := len(localId) - 1
	snw := uint16((localId[end-1] << 8) | localId[end])
	snw = snw << uint(16 - maskBits)
	snw = snw >> uint(16 - maskBits)
	snid := config.SubNetworkID {
		byte((snw >> 8) & 0xff),
		byte(snw & 0xff),
	}
	return snid, nil
}

func (yeShMgr *yeShellManager)deDupTimerCb(el *list.Element, data interface{}) interface{} {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()

	if key, ok := data.(*[yesKeyBytes]byte); !ok {
		return errors.New("deDupTimerCb: invalid key")
	} else {
		delete(yeShMgr.deDupMap, *key)
		return nil
	}
}

func (yeShMgr *yeShellManager)setDedupTimer(key []byte) error {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()

	if len(key) != yesKeyBytes {
		return errors.New("setDedupTimer: invalid key")
	}

	var k [yesKeyBytes]byte
	copy(k[0:], key)

	if _, ok := yeShMgr.deDupMap[k]; ok {
		log.Debug("setDedupTimer: duplicated")
		return errors.New("setDedupTimer: duplicated")
	}

	tm, err := yeShMgr.tmDedup.GetTimer(YeShellCfg.DedupTime, nil, yeShMgr.deDupTimerCb)
	if err != nil {
		log.Debug("setDedupTimer: GetTimer failed, error: %s", err.Error())
		return err
	}

	yeShMgr.tmDedup.SetTimerData(tm, &k)
	if err := yeShMgr.tmDedup.StartTimer(tm); err != nil {
		log.Debug("setDedupTimer: StartTimer failed, error: %s", err.Error())
		return err
	}

	yeShMgr.deDupMap[k] = true
	return nil
}

func (yeShMgr *yeShellManager)deDupTickerProc(){
	defer yeShMgr.deDupTiker.Stop()
	for {
		select {
		case <-yeShMgr.deDupTiker.C:

			yeShMgr.deDupLock.Lock()
			yeShMgr.tmDedup.TickProc()
			yeShMgr.deDupLock.Unlock()

		case <-yeShMgr.ddtChan:
			break
		}
	}
	log.Debug("deDupTickerProc: exit")
}

func (yeShMgr *yeShellManager)GetLocalNode() *config.Node {
	cfg := yeShMgr.chainInst.SchGetP2pConfig()
	return &cfg.Local
}