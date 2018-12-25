/*
 * Copyright (C) 2018 gyee authors
 *
 * This file is part of the gyee library.
 *
 * The gyee library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The gyee library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 */

package p2p

import (
	"sync"
	"time"
	"errors"
	"fmt"
	"bytes"
	"container/list"
	"math/rand"
	config	"github.com/yeeco/gyee/p2p/config"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	dht		"github.com/yeeco/gyee/p2p/dht"
	peer	"github.com/yeeco/gyee/p2p/peer"
	p2psh	"github.com/yeeco/gyee/p2p/shell"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
)


//
// debug
//
type yesLogger struct {
	debug__		bool
}

var yesLog = yesLogger {
	debug__:	true,
}

func (log yesLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// debug
//
type staticTaskLogger struct {
	debug__		bool
}

var stLog = staticTaskLogger {
	debug__:	false,
}

func (log staticTaskLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// yee shell (both for peer and dht)
//

const (
	yesKeyBytes = config.DhtKeyLength											// key length in bytes
	yesMaxFindNode = 4											// max find node commands in pending
	yesMaxGetProvider = 4										// max get provider commands in pending
	yesMaxPutProvider = 4										// max put provider commands in pending
)

var yesMtAtoi = map[string]int{
	MessageTypeTx:					sch.MSBR_MT_TX,
	MessageTypeEvent:				sch.MSBR_MT_EV,
	MessageTypeBlockHeader:			sch.MSBR_MT_BLKH,
	MessageTypeBlock:				sch.MSBR_MT_BLK,
}

var yesMidItoa = map[int] string {
	int(sch.MSBR_MT_TX):					MessageTypeTx,
	int(sch.MSBR_MT_EV):					MessageTypeEvent,
	int(sch.MSBR_MT_BLKH):					MessageTypeBlockHeader,
	int(sch.MSBR_MT_BLK):					MessageTypeBlock,
}

type yesKey = config.DsKey										// key type

type yeShellManager struct {
	chainInst			*sch.Scheduler							// chain scheduler pointer
	ptnChainShell		interface{}								// chain shell manager task node pointer
	ptChainShMgr		*p2psh.ShellManager							// chain shell manager object
	dhtInst				*sch.Scheduler							// dht scheduler pointer
	ptnDhtShell			interface{}								// dht shell manager task node pointer
	ptDhtShMgr			*p2psh.DhtShellManager						// dht shell manager object
	getValKey			yesKey									// key of value to get
	getValChan			chan []byte								// get value channel
	putValKey			[]byte									// key of value to put
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
	bsTicker			*time.Ticker							// bootstrap ticker
	dhtBsChan			chan bool								// bootstrap ticker channel
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
	DhtBootstrapNodes	[]string								// bootstrap nodes for dht
	NodeDataDir			string									// node data directory
	NodeDatabase		string									// node database
	SubNetMaskBits		int										// mask bits for sub network identity
	EvKeepTime			time.Duration							// duration for events kept by dht
	DedupTime			time.Duration							// duration for deduplication cleanup timer
	BootstrapTime		time.Duration							// duration for bootstrap blind connection
	localSnid			[]byte									// local sut network identity
	dhtBootstrapNodes	[]*config.Node							// dht bootstarp nodes
}

const (
	ChainCfgIdx = 0
	DhtCfgIdx = 1
)

// Default yee shell configuration for convenience
var DefaultYeShellConfig = YeShellConfig{
	AppType:			config.P2P_TYPE_ALL,
	Name:				"test",
	Validator:			true,
	BootstrapNode:		false,
	BootstrapNodes:		[]string {
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:30303:30303",
	},
	DhtBootstrapNodes:	[]string {
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:40404:40404",
	},
	NodeDataDir:		config.P2pDefaultDataDir(true),
	NodeDatabase:		"nodes",
	SubNetMaskBits:		0,
	EvKeepTime:			time.Minute * 8,
	DedupTime:			time.Second * 60,
	BootstrapTime:		time.Second * 4,
}

// Global shell configuration: this var is set when function YeShellConfigToP2pCfg called,
// the p2p user should not change those fields other than "Validator" and "SubNetMaskBits"
// which can be reconfigurated, since YeShellConfigToP2pCfg should be called once only.
var YeShellCfg = YeShellConfig{}

func YeShellConfigToP2pCfg(yesCfg *YeShellConfig) []*config.Config {
	if yesCfg == nil {
		yesLog.Debug("YeShellConfigToP2pCfg: nil configuration")
		return nil
	}

	if yesCfg.AppType != config.P2P_TYPE_ALL {
		yesLog.Debug("YeShellConfigToP2pCfg: P2P_TYPE_ALL needed")
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
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetupLocalNodeId failed")
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
		yesLog.Debug("YeShellConfigToP2pCfg: too big, SubNetMaskBits: %d", yesCfg.SubNetMaskBits)
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
	YeShellCfg.localSnid = append(YeShellCfg.localSnid, snid[0:]...)
	chainCfg.SubNetIdList = append(chainCfg.SubNetIdList, snid)
	chainCfg.SubNetMaxPeers[snid] = config.MaxPeers
	chainCfg.SubNetMaxOutbounds[snid] = config.MaxOutbounds
	chainCfg.SubNetMaxInBounds[snid] = config.MaxInbounds

	chainCfg.SubNetIdList = append(chainCfg.SubNetIdList, config.AnySubNet)
	chainCfg.SubNetMaxPeers[config.AnySubNet] = config.MaxPeers
	chainCfg.SubNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
	chainCfg.SubNetMaxInBounds[config.AnySubNet] = config.MaxInbounds

	if chCfgName, eno := config.P2pSetConfig("chain", chainCfg); eno != config.PcfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetConfig failed")
		return nil
	} else {
		chainCfg = config.P2pGetConfig(chCfgName)
	}

	dhtCfg = new(config.Config)
	*dhtCfg = *chainCfg
	YeShellCfg.dhtBootstrapNodes = config.P2pSetupBootstrapNodes(YeShellCfg.DhtBootstrapNodes)
	dht.SetBootstrapNodes(YeShellCfg.dhtBootstrapNodes)
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
		yesLog.Debug("NewYeShellManager: YeShellConfigToP2pCfg failed")
		return nil
	}

	if cfg[0] == nil || cfg[1] == nil {
		yesLog.Debug("NewYeShellManager: nil configuration")
		return nil
	}

	yeShMgr.chainInst, eno = p2psh.P2pCreateInstance(cfg[ChainCfgIdx])
	if eno != sch.SchEnoNone || yeShMgr.chainInst == nil {
		yesLog.Debug("NewYeShellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.dhtInst, eno = p2psh.P2pCreateInstance(cfg[DhtCfgIdx])
	if eno != sch.SchEnoNone || yeShMgr.dhtInst == nil {
		yesLog.Debug("NewYeShellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	return &yeShMgr
}

func (yeShMgr *yeShellManager)Start() error {
	var eno sch.SchErrno
	var ok bool

	if eno = p2psh.P2pStart(yeShMgr.dhtInst); eno != sch.SchEnoNone {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	eno, yeShMgr.ptnDhtShell = yeShMgr.dhtInst.SchGetUserTaskNode(sch.DhtShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnDhtShell == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptDhtShMgr, ok = yeShMgr.dhtInst.SchGetTaskObject(sch.DhtShMgrName).(*p2psh.DhtShellManager)
	if !ok || yeShMgr.ptDhtShMgr == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	if eno := p2psh.P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	eno, yeShMgr.ptnChainShell = yeShMgr.chainInst.SchGetUserTaskNode(sch.ShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnChainShell == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptChainShMgr, ok = yeShMgr.chainInst.SchGetTaskObject(sch.ShMgrName).(*p2psh.ShellManager)
	if !ok || yeShMgr.ptChainShMgr == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
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
	yeShMgr.tmDedup = dht.NewTimerManager()
	yeShMgr.deDupMap = make(map[[yesKeyBytes]byte]bool, 0)
	yeShMgr.deDupTiker = time.NewTicker(dht.OneTick)
	yeShMgr.ddtChan = make(chan bool, 1)

	go yeShMgr.dhtEvProc()
	go yeShMgr.dhtCsProc()

	if YeShellCfg.BootstrapNode == false {
		yeShMgr.bsTicker = time.NewTicker(YeShellCfg.BootstrapTime)
		yeShMgr.dhtBsChan = make(chan bool, 1)
		go yeShMgr.dhtBootstrapProc()
	}

	go yeShMgr.chainRxProc()
	go yeShMgr.deDupTickerProc()

	return nil
}

func (yeShMgr *yeShellManager)Stop() {
	if YeShellCfg.BootstrapNode == false {
		yesLog.Debug("Stop: close dht bootstrap timer")
		close(yeShMgr.dhtBsChan)
	}

	yesLog.Debug("Stop: close deduplication ticker")
	close(yeShMgr.ddtChan)

	stopCh := make(chan bool, 1)

	yesLog.Debug("Stop: stop dht")
	p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
	<-stopCh
	yesLog.Debug("Stop: dht stopped")

	yesLog.Debug("Stop: stop chain")
	p2psh.P2pStop(yeShMgr.chainInst, stopCh)
	<-stopCh
	yesLog.Debug("Stop: chain stopped")
}

func (yeShMgr *yeShellManager)Reconfig(reCfg *RecfgCommand) error {
	if reCfg == nil {
		yesLog.Debug("Reconfig: invalid parameter")
		return errors.New("nil reconfigurate command")
	}

	if reCfg.SubnetMaskBits <= 0 || reCfg.SubnetMaskBits > MaxSubNetMaskBits {
		yesLog.Debug("")
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
			yesLog.Debug("Reconfig: getSubnetIdentity failed, error: %s", err.Error())
			return err
		}
		newSnid, err := yeShMgr.getSubnetIdentity(reCfg.SubnetMaskBits)
		if err != nil {
			yesLog.Debug("Reconfig: getSubnetIdentity failed, error: %s", err.Error())
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
		yesLog.Debug("Reconfig: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)BroadcastMessage(message Message) error {
	// Notice: the function is not supported in fact, see handler function
	// in each case please.
	var err error = nil
	switch message.MsgType {
	case MessageTypeTx:
		err = yeShMgr.broadcastTx(&message)
	case MessageTypeEvent:
		err = yeShMgr.broadcastEv(&message)
	case 	MessageTypeBlockHeader:
		err = yeShMgr.broadcastBh(&message)
	case 	MessageTypeBlock:
		err = yeShMgr.broadcastBk(&message)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessage: invalid type: %d", message.MsgType))
	}
	return err
}

func (yeShMgr *yeShellManager)BroadcastMessageOsn(message Message) error {
	var err error = nil
	switch message.MsgType {
	case MessageTypeTx:
		err = yeShMgr.broadcastTxOsn(&message)
	case MessageTypeEvent:
		err = yeShMgr.broadcastEvOsn(&message)
	case MessageTypeBlockHeader:
		err = yeShMgr.broadcastBhOsn(&message)
	case MessageTypeBlock:
		err = yeShMgr.broadcastBkOsn(&message)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessageOsn: invalid type: %d", message.MsgType))
	}
	return err
}

func (yeShMgr *yeShellManager)Register(subscriber *Subscriber) {
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.LoadOrStore(t, new(sync.Map))
	m.(*sync.Map).Store(subscriber, true)
}

func (yeShMgr *yeShellManager)UnRegister(subscriber *Subscriber) {
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.Load(t)
	if m == nil {
		return
	}
	m.(*sync.Map).Delete(subscriber)
}

func (yeShMgr *yeShellManager)DhtGetValue(key []byte) ([]byte, error) {
	if len(key) != yesKeyBytes {
		yesLog.Debug("DhtGetValue: invalid key: %x", key)
		return nil, sch.SchEnoParameter
	}

	req := sch.MsgDhtMgrGetValueReq{
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("DhtGetValue: failed, eno: %d, error: %s", eno, eno.Error())
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
		yesLog.Debug("DhtSetValue: invalid (key, value) pair, key: %x, length of value: %d", key, len(value))
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
		yesLog.Debug("DhtSetValue: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	copy(yeShMgr.putValKey, key)
	result, ok := <-yeShMgr.putValChan
	yeShMgr.putValKey = nil
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
		yesLog.Debug("DhtFindNode: invalid parameters")
		return sch.SchEnoParameter
	}

	if len(yeShMgr.findNodeMap) >= yesMaxFindNode {
		yesLog.Debug("DhtFindNode: too much, max: %d", yesMaxFindNode)
		return sch.SchEnoResource
	}

	key := *(*yesKey)(dht.RutMgrNodeId2Hash(*target))
	if _, ok := yeShMgr.findNodeMap[key]; ok {
		yesLog.Debug("DhtFindNode: duplicated")
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
		yesLog.Debug("DhtFindNode: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yeShMgr.findNodeMap[key] = done
	return nil
}

func (yeShMgr *yeShellManager)DhtGetProvider(key []byte, done chan interface{}) error {
	if len(key) != yesKeyBytes {
		yesLog.Debug("DhtGetProvider: invalid key: %x", key)
		return sch.SchEnoParameter
	}
	if done == nil {
		yesLog.Debug("DhtGetProvider: invalid done channel")
		return sch.SchEnoParameter
	}

	var yk yesKey
	copy(yk[0:], key)
	if _, ok := yeShMgr.getProviderMap[yk]; ok {
		yesLog.Debug("DhtGetProvider: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtMgrGetProviderReq {
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetProviderReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("DhtGetProvider: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yeShMgr.getProviderMap[yk] = done
	return nil
}

func (yeShMgr *yeShellManager)DhtSetProvider(key []byte, provider *config.Node, done chan interface{}) error {
	if len(key) != yesKeyBytes {
		yesLog.Debug("DhtSetProvider: invalid key: %x", key)
		return sch.SchEnoParameter
	}
	if provider == nil || done == nil {
		yesLog.Debug("DhtSetProvider: invalid parameters")
		return sch.SchEnoParameter
	}

	var yk yesKey
	copy(yk[0:], key)
	if _, ok := yeShMgr.getProviderMap[yk]; ok {
		yesLog.Debug("DhtSetProvider: duplicated")
		return sch.SchEnoDuplicated
	}

	req := sch.MsgDhtPrdMgrAddProviderReq {
		Key: key,
		Prd: *provider,
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutProviderReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("DhtSetProvider: failed, eno: %d, error: %s", eno, eno.Error())
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
			yesLog.Debug("evHandler: invalid event: %d", evi.Evt)
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
				yesLog.Debug("dhtEvProc: channel closed")
				break _evLoop
			}
		}
	}
	yesLog.Debug("dhtEvProc: exit")
}

func (yeShMgr *yeShellManager)dhtCsProc() {
	csCh := yeShMgr.dhtCsChan
	csHandler := func(csi *sch.MsgDhtConInstStatusInd) {
		switch csi.Status {

		case dht.CisNull:
			yesLog.Debug("dhtCsProc: CisNull, dir: %d, peer: %x", *csi.Peer)

		case dht.CisConnecting:
			yesLog.Debug("dhtCsProc: CisConnecting, dir: %d, peer: %x", *csi.Peer)

		case dht.CisConnected:
			yesLog.Debug("dhtCsProc: CisConnected, dir: %d, peer: %x", *csi.Peer)

		case dht.CisAccepted:
			yesLog.Debug("dhtCsProc: CisAccepted, dir: %d, peer: %x", *csi.Peer)

		case dht.CisInHandshaking:
			yesLog.Debug("dhtCsProc: CisInHandshaking, dir: %d, peer: %x", *csi.Peer)

		case dht.CisHandshaked:
			yesLog.Debug("dhtCsProc: CisHandshaked, dir: %d, peer: %x", *csi.Peer)

		case dht.CisInService:
			yesLog.Debug("dhtCsProc: CisInService, dir: %d, peer: %x", *csi.Peer)

		case dht.CisClosed:
			yesLog.Debug("dhtCsProc: CisAccepted, dir: %d, peer: %x", *csi.Peer)

		default:
			yesLog.Debug("dhtCsProc: invalid connection status: %d", csi.Status)
		}
	}

_csLoop:
	for {
		select {
		case csi, ok := <-csCh:
			if ok {
				csHandler(csi)
			} else {
				yesLog.Debug("dhtCsProc: channel closed")
				break _csLoop
			}
		}
	}
	yesLog.Debug("dhtCsProc: exit")
}

func (yeShMgr *yeShellManager)chainRxProc() {

_rxLoop:
	for {
		select {
		case pkg, ok := <-yeShMgr.chainRxChan:
			if !ok {
				yesLog.Debug("chainRxProc: channel closed")
				break _rxLoop
			}
			if pkg.ProtoId != int(peer.PID_EXT) {
				yesLog.Debug("chainRxProc: invalid protocol identity: %d", pkg.ProtoId)
				continue
			}

			yesLog.Debug("chainRxProc: peer: %+v, packeage received: %+v", *pkg.PeerInfo, *pkg)

			msgType := yesMidItoa[pkg.MsgId]
			if subList, ok := yeShMgr.subscribers.Load(msgType); ok {
				subList.(*sync.Map).Range(func(key, value interface{}) bool {
					msg := Message {
						MsgType: msgType,
						From: fmt.Sprintf("%x", pkg.PeerInfo.NodeId),
						Key: pkg.Key,
						Data: pkg.Payload,
					}

					err := error(nil)
					switch msg.MsgType {
					case MessageTypeTx:
						err = yeShMgr.broadcastTxOsn(&msg)
					case MessageTypeEvent:
						err = yeShMgr.broadcastEvOsn(&msg)
					case MessageTypeBlockHeader:
						err = yeShMgr.broadcastBhOsn(&msg)
					case MessageTypeBlock:
						err = yeShMgr.broadcastBkOsn(&msg)
					default:
						err = errors.New(fmt.Sprintf("chainRxProc: invalid message type: %s", msg.MsgType))
					}

					if err != nil {
						yesLog.Debug("chainRxProc: MsgType: %s, error: %s", msg.MsgType, err.Error())
						return false
					}

					sub, _ := key.(*Subscriber)
					sub.MsgChan <- msg
					return true
				})
			}
		}
	}
	yesLog.Debug("chainRxProc: exit")
}

func (yeShMgr *yeShellManager)dhtBootstrapProc() {
	defer yeShMgr.bsTicker.Stop()
_bootstarp:
	for {
		select {
		case <-yeShMgr.bsTicker.C:
			if len(YeShellCfg.dhtBootstrapNodes) <= 0 {
				yesLog.Debug("dhtBootstrapProc: none of bootstarp nodes")
			} else {
				r := rand.Int31n(int32(len(YeShellCfg.dhtBootstrapNodes)))
				req := sch.MsgDhtBlindConnectReq{
					Peer: YeShellCfg.dhtBootstrapNodes[r],
				}
				msg := sch.SchMessage{}
				yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtBlindConnectReq, &req)
				yeShMgr.dhtInst.SchSendMessage(&msg)
			}
		case <-yeShMgr.dhtBsChan:
			break _bootstarp
		}
	}
	yesLog.Debug("dhtBootstrapProc: exit")
}

func (yeShMgr *yeShellManager)dhtBlindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	yesLog.Debug("dhtBlindConnectRsp: msg: %+v", *msg)
	for _, bsn := range YeShellCfg.dhtBootstrapNodes {
		if msg.Eno == dht.DhtEnoNone.GetEno() {
			if bytes.Compare(msg.Peer.ID[0:], bsn.ID[0:]) == 0 {
				yesLog.Debug("dhtBlindConnectRsp: bootstrap node connected, id: %x", msg.Peer.ID)
				yeShMgr.dhtBsChan<-true
				break
			}
		}
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrFindPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	yesLog.Debug("dhtMgrFindPeerRsp: msg: %+v", *msg)
	if done, ok := yeShMgr.findNodeMap[msg.Target]; ok {
		done<-msg
		delete(yeShMgr.findNodeMap, msg.Target)
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtQryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	yesLog.Debug("dhtQryMgrQueryStartRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtQryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	yesLog.Debug("dhtQryMgrQueryStopRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtConMgrSendCfm(msg *sch.MsgDhtConMgrSendCfm) sch.SchErrno {
	yesLog.Debug("dhtConMgrSendCfm: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrPutProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrPutProviderRsp: msg: %+v", *msg)
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
	yesLog.Debug("dhtMgrGetProviderRsp: msg: %+v", *msg)
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
	// Notice: only when function DhtSetValue called, put value key would be set
	// and called would be in a blocked mode, we check this case to feed the signal
	// into the channel which the caller is pending for, else nothing done.
	yesLog.Debug("dhtMgrPutValueRsp: msg: %+v", *msg)
	if yeShMgr.putValKey != nil && len(yeShMgr.putValKey) == yesKeyBytes {
		if bytes.Compare(yeShMgr.putValKey[0:], msg.Key) != 0 {
			yesLog.Debug("dhtMgrPutValueRsp: key mismatched")
			return sch.SchEnoMismatched
		}
		if msg.Eno == dht.DhtEnoNone.GetEno() {
			yeShMgr.putValChan <- true
		} else {
			yeShMgr.putValChan <- false
		}
	}
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)dhtMgrGetValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrGetValueRsp: msg: %+v", *msg)
	if bytes.Compare(yeShMgr.putValKey[0:], msg.Key) != 0 {
		yesLog.Debug("dhtMgrGetValueRsp: key mismatched")
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
	yesLog.Debug("dhtConMgrCloseRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *yeShellManager)broadcastTx(msg *Message) error {
	return errors.New("broadcastTx: not supported")
}

func (yeShMgr *yeShellManager)broadcastEv(msg *Message) error {
	return errors.New("broadcastEv: not supported")
}

func (yeShMgr *yeShellManager)broadcastBh(msg *Message) error {
	return errors.New("broadcastBh: not supported")
}

func (yeShMgr *yeShellManager)broadcastBk(msg *Message) error {
	return errors.New("broadcastBk: not supported")
}

func (yeShMgr *yeShellManager)broadcastTxOsn(msg *Message) error {
	// if local node is a validator, the Tx should be broadcast over the
	// validator-subnet; else the Tx should be broadcast over the dynamic
	// subnet. this is done in chain shell manager, and the message here
	// would be dispatched to chain shell manager.

	k := [yesKeyBytes]byte{}
	copy(k[0:], msg.Key)
	if _, ok := yeShMgr.deDupMap[k]; ok {
		return errors.New("broadcastTxOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastTxOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Key: msg.Key,
		Data: msg.Data,
		LocalSnid: YeShellCfg.localSnid,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastTxOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *yeShellManager)broadcastEvOsn(msg *Message) error {
	// the local node must be a validator, and the Ev should be broadcast
	// over the validator-subnet. also, the Ev should be stored into DHT
	// with a duration to be expired. the message here would be dispatched
	// to chain shell manager and DHT shell manager.
	k := [yesKeyBytes]byte{}
	copy(k[0:], msg.Key)
	if _, ok := yeShMgr.deDupMap[k]; ok {
		return errors.New("broadcastEvOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastEvOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Key: msg.Key,
		Data: msg.Data,
		LocalSnid: YeShellCfg.localSnid,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	req2Dht := sch.MsgDhtMgrPutValueReq {
		Key: msg.Key,
		Val: msg.Data,
		KeepTime: YeShellCfg.EvKeepTime,
	}
	yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req2Dht)
	if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	return nil
}

func (yeShMgr *yeShellManager)broadcastBhOsn(msg *Message) error {
	// the Bh should be broadcast over the any-subnet. the message here
	// would be dispatched to chain shell manager.
	k := [yesKeyBytes]byte{}
	copy(k[0:], msg.Key)
	if _, ok := yeShMgr.deDupMap[k]; ok {
		return errors.New("broadcastBhOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastBhOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq {
		MsgType: yesMtAtoi[msg.MsgType],
		From: msg.From,
		Key: msg.Key,
		Data: msg.Data,
		LocalSnid: YeShellCfg.localSnid,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastBhOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *yeShellManager)broadcastBkOsn(msg *Message) error {
	// the Bk should be stored by DHT and no broadcasting over any subnet.
	// the message here would be dispatched to DHT shell manager.
	k := [yesKeyBytes]byte{}
	copy(k[0:], msg.Key)
	if _, ok := yeShMgr.deDupMap[k]; ok {
		return errors.New("broadcastBkOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastBkOsn: error: %s", err.Error())
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
		yesLog.Debug("broadcastBkOsn: SchSendMessage failed, eno: %d", eno)
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

func (yeShMgr *yeShellManager)setDedupTimer(key [yesKeyBytes]byte) error {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()

	if len(key) != yesKeyBytes {
		return errors.New(fmt.Sprintf("setDedupTimer: invalid key length: %d", len(key)))
	}

	tm, err := yeShMgr.tmDedup.GetTimer(YeShellCfg.DedupTime, nil, yeShMgr.deDupTimerCb)
	if err != dht.TmEnoNone {
		yesLog.Debug("setDedupTimer: GetTimer failed, error: %s", err.Error())
		return err
	}

	yeShMgr.tmDedup.SetTimerData(tm, &key)
	if err := yeShMgr.tmDedup.StartTimer(tm); err != dht.TmEnoNone {
		yesLog.Debug("setDedupTimer: StartTimer failed, error: %s", err.Error())
		return err
	}

	yeShMgr.deDupMap[key] = true
	return nil
}

func (yeShMgr *yeShellManager)deDupTickerProc(){
	defer yeShMgr.deDupTiker.Stop()
_dedup:
	for {
		select {
		case <-yeShMgr.deDupTiker.C:
			yeShMgr.tmDedup.TickProc()
		case <-yeShMgr.ddtChan:
			break _dedup
		}
	}
	yesLog.Debug("deDupTickerProc: exit")
}

func (yeShMgr *yeShellManager)GetLocalNode() *config.Node {
	cfg := yeShMgr.chainInst.SchGetP2pConfig()
	return &cfg.Local
}