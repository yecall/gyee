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
	"bytes"
	"container/list"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"strings"

	"github.com/yeeco/gyee/p2p/config"
	"github.com/yeeco/gyee/p2p/dht"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	"github.com/yeeco/gyee/p2p/peer"
	sch "github.com/yeeco/gyee/p2p/scheduler"
	p2psh "github.com/yeeco/gyee/p2p/shell"
)

//
// debug
//
type yesLogger struct {
	debug__ bool
}

var yesLog = yesLogger{
	debug__: false,
}

func (log yesLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// yee shell (both for peer and dht)
//

const (
	yesKeyBytes       = config.DhtKeyLength // key length in bytes
	yesMaxFindNode    = 4                   // max find node commands in pending
	yesMaxGetProvider = 4                   // max get provider commands in pending
	yesMaxPutProvider = 4                   // max put provider commands in pending
	yesGetChainDataTm = time.Second * 8		// duration for get chain data
)

var yesMtAtoi = map[string]int{
	MessageTypeTx:          sch.MSBR_MT_TX,
	MessageTypeEvent:       sch.MSBR_MT_EV,
	MessageTypeBlockHeader: sch.MSBR_MT_BLKH,
	MessageTypeBlock:       sch.MSBR_MT_BLK,
}

var yesMidItoa = map[int]string{
	int(sch.MSBR_MT_TX):   MessageTypeTx,
	int(sch.MSBR_MT_EV):   MessageTypeEvent,
	int(sch.MSBR_MT_BLKH): MessageTypeBlockHeader,
	int(sch.MSBR_MT_BLK):  MessageTypeBlock,
}

type SubnetDescriptor struct {
	SubNetKeyList      map[config.SubNetworkID]ecdsa.PrivateKey // keys for sub-node
	SubNetNodeList     map[config.SubNetworkID]config.Node      // sub-node identities
	SubNetMaxPeers     map[config.SubNetworkID]int              // max peers would be
	SubNetMaxOutbounds map[config.SubNetworkID]int              // max concurrency outbounds
	SubNetMaxInBounds  map[config.SubNetworkID]int              // max concurrency inbounds
	SubNetIdList       []config.SubNetworkID                    // sub network identity list
}

type SingleSubnetDescriptor = sch.SingleSubnetDescriptor // single subnet descriptor

type yesKey = config.DsKey // key type

var yesInStopping = errors.New("yesmgr: in stopping")

type YeShellManager struct {
	name           string                           // unique name of the shell manager
	inStopping     bool                             // in stopping procedure
	chainInst      *sch.Scheduler                   // chain scheduler pointer
	ptnChainShell  interface{}                      // chain shell manager task node pointer
	ptChainShMgr   *p2psh.ShellManager              // chain shell manager object
	dhtInst        *sch.Scheduler                   // dht scheduler pointer
	ptnDhtShell    interface{}                      // dht shell manager task node pointer
	ptDhtShMgr     *p2psh.DhtShellManager           // dht shell manager object
	getValKey      yesKey                           // key of value to get
	getValChan     chan []byte                      // get value channel
	putValKey      []byte                           // key of value to put
	putValChan     chan bool                        // put value channel
	findNodeMap    map[yesKey]chan interface{}      // find node command map to channel
	getProviderMap map[yesKey]chan interface{}      // find node command map to channel
	putProviderMap map[yesKey]chan interface{}      // find node command map to channel
	dhtEvChan      chan *sch.MsgDhtShEventInd       // dht event indication channel
	dhtCsChan      chan *sch.MsgDhtConInstStatusInd // dht connection status indication channel
	subscribers    *sync.Map                        // subscribers for incoming messages
	chainRxChan    chan *peer.P2pPackageRx          // total rx channel for chain
	deDupLock      sync.Mutex                       // lock for deduplication timer manager
	tmDedup        *dht.TimerManager                // deduplication timer manager
	deDupMap       map[[yesKeyBytes]byte]bool       // map for keys of messages had been sent
	deDupTiker     *time.Ticker                     // deduplication ticker
	ddtChan        chan bool                        // deduplication ticker channel
	bsTicker       *time.Ticker                     // bootstrap ticker
	dhtBsChan      chan bool                        // bootstrap ticker channel
	cp             ChainProvider                    // interface registered to p2p for "get chain data" message
	gcdLock		   sync.Mutex						// get chain data lock
	gcdChan        chan []byte						// get chain data channel
	gcdTimer       *time.Timer						// get chain data timer
	gcdSeq         uint64							// get chain data sequence number
	gcdName        string							// get chain data name
	gcdKey         []byte							// get chain data key
}

const MaxSubNetMaskBits = 15 // max number of mask bits for sub network identity

type YeShellConfig struct {
	// Notice: in current stage, a simple configuration for p2p is applied, for total configuration
	// about p2p, see config.Config please.
	AppType           config.P2pAppType                   // application type
	Name              string                              // node name, should be unique
	Validator         bool                                // validator flag
	BootstrapNode     bool                                // bootstrap node flag
	BootstrapNodes    []string                            // bootstrap nodes
	DhtBootstrapNodes []string                            // bootstrap nodes for dht
	LocalNodeIp       string                              // local node ip for chain-peers
	LocalUdpPort      uint16                              // local node udp port
	LocalTcpPort      uint16                              // local node tcp port
	LocalDhtIp        string                              // local dht ip
	LocalDhtPort      uint16                              // local dht port
	NodeDataDir       string                              // node data directory
	NodeDatabase      string                              // node database
	SubNetMaskBits    int                                 // mask bits for sub network identity
	EvKeepTime        time.Duration                       // duration for events kept by dht
	DedupTime         time.Duration                       // duration for deduplication cleanup timer
	BootstrapTime     time.Duration                       // duration for bootstrap blind connection
	NatType           string                              // nat type, "none"/"pmp"/"upnp"
	GatewayIp         string                              // gateway ip when nat type is "pmp"
	localSnid         []config.SubNetworkID               // local sub network identities
	localNode         map[config.SubNetworkID]config.Node // local sub nodes
	dhtBootstrapNodes []*config.Node                      // dht bootstarp nodes
}

const (
	ChainCfgIdx      = 0
	DhtCfgIdx        = 1
	DftEvKeepTime    = time.Minute * 1
	DftDedupTime     = time.Second * 60
	DftBootstrapTime = time.Second * 4
	DftNatType       = config.NATT_NONE
	DftGatewayIp     = "0.0.0.0"
)

// Default yee shell configuration for convenience
var DefaultYeShellConfig = YeShellConfig{
	AppType:       config.P2P_TYPE_ALL,
	Name:          config.DefaultNodeName,
	Validator:     true,
	BootstrapNode: false,
	BootstrapNodes: []string{
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:30303:30303",
	},
	DhtBootstrapNodes: []string{
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:40404:40404",
	},
	LocalNodeIp:       config.P2pGetLocalIpAddr().String(),
	LocalUdpPort:      config.DftUdpPort,
	LocalTcpPort:      config.DftTcpPort,
	LocalDhtIp:        config.P2pGetLocalIpAddr().String(),
	LocalDhtPort:      config.DftDhtPort,
	NodeDataDir:       config.P2pDefaultDataDir(true),
	NodeDatabase:      config.DefaultNodeDatabase,
	SubNetMaskBits:    config.DftSnmBits,
	EvKeepTime:        DftEvKeepTime,
	DedupTime:         DftDedupTime,
	BootstrapTime:     DftBootstrapTime,
	NatType:           DftNatType,
	GatewayIp:         DftGatewayIp,
	localSnid:         make([]config.SubNetworkID, 0),
	localNode:         make(map[config.SubNetworkID]config.Node, 0),
	dhtBootstrapNodes: make([]*config.Node, 0),
}

// Global shell configuration: this var is set when function YeShellConfigToP2pCfg called,
// the p2p user should not change those fields other than "Validator" and "SubNetMaskBits"
// which can be reconfigurated, since YeShellConfigToP2pCfg should be called once only.
var YeShellCfg = make(map[string]YeShellConfig, 0)

func YeShellConfigToP2pCfg(yesCfg *YeShellConfig) []*config.Config {
	if yesCfg == nil {
		yesLog.Debug("YeShellConfigToP2pCfg: nil configuration")
		return nil
	}

	if yesCfg.AppType != config.P2P_TYPE_ALL {
		yesLog.Debug("YeShellConfigToP2pCfg: P2P_TYPE_ALL needed")
		return nil
	}

	thisCfg := *yesCfg
	copy(thisCfg.BootstrapNodes, yesCfg.BootstrapNodes)
	copy(thisCfg.DhtBootstrapNodes, yesCfg.DhtBootstrapNodes)

	cfg := []*config.Config{nil, nil}
	chainCfg := (*config.Config)(nil)
	dhtCfg := (*config.Config)(nil)

	if yesCfg.BootstrapNode {
		chainCfg = config.P2pDefaultBootstrapConfig(yesCfg.BootstrapNodes)
	} else {
		chainCfg = config.P2pDefaultConfig(yesCfg.BootstrapNodes)
	}

	chainCfg.AppType = config.P2P_TYPE_CHAIN
	chainCfg.Name = yesCfg.Name
	chainCfg.NodeDataDir = yesCfg.NodeDataDir
	chainCfg.DhtFdsCfg.Path = yesCfg.NodeDataDir
	if yesCfg.NodeDatabase != "" {
		chainCfg.NodeDatabase = yesCfg.NodeDatabase
	}

	p2plog.Debug("YeShellConfigToP2pCfg: local addr: chain[%s:%d:%d], dht[%s:%d]",
		yesCfg.LocalNodeIp, yesCfg.LocalUdpPort, yesCfg.LocalTcpPort,
		yesCfg.LocalDhtIp, yesCfg.LocalDhtPort)

	if config.P2pSetLocalIpAddr(chainCfg, yesCfg.LocalNodeIp, yesCfg.LocalUdpPort,
		yesCfg.LocalTcpPort) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetLocalIpAddr failed")
		return nil
	}
	if config.P2pSetupLocalNodeId(chainCfg) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetupLocalNodeId failed")
		return nil
	}
	if err := SetupSubNetwork(chainCfg, yesCfg.SubNetMaskBits, yesCfg.Validator); err != nil {
		yesLog.Debug("YeShellConfigToP2pCfg: SetupSubNetwork failed")
		return nil
	}
	thisCfg.localSnid = append(thisCfg.localSnid, chainCfg.SubNetIdList...)
	for _, snid := range thisCfg.localSnid {
		thisCfg.localNode[snid] = chainCfg.SubNetNodeList[snid]
	}

	p2plog.Debug("YeShellConfigToP2pCfg: NatType: %s, GatewayIp: %s", yesCfg.NatType, yesCfg.GatewayIp)
	config.P2pSetupNatType(chainCfg, yesCfg.NatType, yesCfg.GatewayIp)

	p2plog.Debug("YeShellConfigToP2pCfg: LocalDhtIp: %s, LocalDhtPort: %d",
		yesCfg.LocalDhtIp, yesCfg.LocalDhtPort)
	if config.P2pSetLocalDhtIpAddr(chainCfg, yesCfg.LocalDhtIp, yesCfg.LocalDhtPort) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetLocalDhtIpAddr failed")
		return nil
	}

	if chCfgName, eno := config.P2pSetConfig("chain", chainCfg); eno != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetConfig failed")
		return nil
	} else {
		chainCfg = config.P2pGetConfig(chCfgName)
	}

	dhtCfg = new(config.Config)
	*dhtCfg = *chainCfg

	bsn := config.P2pSetupBootstrapNodes(thisCfg.DhtBootstrapNodes)
	thisCfg.dhtBootstrapNodes = append(thisCfg.dhtBootstrapNodes, bsn...)
	dht.SetBootstrapNodes(thisCfg.dhtBootstrapNodes, thisCfg.Name)
	dhtCfg.AppType = config.P2P_TYPE_DHT
	cfg[ChainCfgIdx] = chainCfg
	cfg[DhtCfgIdx] = dhtCfg
	YeShellCfg[yesCfg.Name] = thisCfg

	return cfg
}

func NewYeShellManager(yesCfg *YeShellConfig) *YeShellManager {
	var eno sch.SchErrno
	yeShMgr := YeShellManager{
		name:           yesCfg.Name,
		inStopping:     false,
		putValKey:      make([]byte, 0),
		getValChan:     make(chan []byte, 0),
		putValChan:     make(chan bool, 0),
		findNodeMap:    make(map[yesKey]chan interface{}, yesMaxFindNode),
		getProviderMap: make(map[yesKey]chan interface{}, yesMaxGetProvider),
		putProviderMap: make(map[yesKey]chan interface{}, yesMaxPutProvider),
		subscribers:    new(sync.Map),
		deDupMap:       make(map[[yesKeyBytes]byte]bool, 0),
		ddtChan:        make(chan bool, 1),
	}

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

func (yeShMgr *YeShellManager) Start() error {
	var eno sch.SchErrno
	var ok bool

	yeShMgr.inStopping = false

	p2plog.Debug("yeShMgr: start...")

	dht.SetChConMgrReady(yeShMgr.dhtInst.SchGetP2pCfgName(), make(chan bool, 1))
	if eno = p2psh.P2pStart(yeShMgr.dhtInst); eno != sch.SchEnoNone {
		p2plog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	eno, yeShMgr.ptnDhtShell = yeShMgr.dhtInst.SchGetUserTaskNode(sch.DhtShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnDhtShell == nil {
		p2plog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptDhtShMgr, ok = yeShMgr.dhtInst.SchGetTaskObject(sch.DhtShMgrName).(*p2psh.DhtShellManager)
	if !ok || yeShMgr.ptDhtShMgr == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	if eno := p2psh.P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		p2plog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	eno, yeShMgr.ptnChainShell = yeShMgr.chainInst.SchGetUserTaskNode(sch.ShMgrName)
	if eno != sch.SchEnoNone || yeShMgr.ptnChainShell == nil {
		p2plog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	yeShMgr.ptChainShMgr, ok = yeShMgr.chainInst.SchGetTaskObject(sch.ShMgrName).(*p2psh.ShellManager)
	if !ok || yeShMgr.ptChainShMgr == nil {
		p2plog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	p2plog.Debug("Start: go shell routines...")

	yeShMgr.dhtEvChan = yeShMgr.ptDhtShMgr.GetEventChan()
	yeShMgr.dhtCsChan = yeShMgr.ptDhtShMgr.GetConnStatusChan()
	yeShMgr.chainRxChan = yeShMgr.ptChainShMgr.GetRxChan()
	yeShMgr.tmDedup = dht.NewTimerManager()
	yeShMgr.deDupTiker = time.NewTicker(dht.OneTick)

	go yeShMgr.dhtEvProc()
	go yeShMgr.dhtCsProc()

	thisCfg, _ := YeShellCfg[yeShMgr.name]
	if thisCfg.BootstrapNode == false {
		p2plog.Debug("Start: wait dht ready, inst: %s", yeShMgr.dhtInst.SchGetP2pCfgName())
		if dht.DhtReady(yeShMgr.dhtInst.SchGetP2pCfgName()) {
			yeShMgr.bsTicker = time.NewTicker(thisCfg.BootstrapTime)
			yeShMgr.dhtBsChan = make(chan bool, 1)
			go yeShMgr.dhtBootstrapProc()
		}
	}

	go yeShMgr.chainRxProc()
	go yeShMgr.deDupTickerProc()

	p2plog.Debug("Start: shell ok")

	return nil
}

func (yeShMgr *YeShellManager) Stop() {
	yesLog.Debug("Stop: close deduplication ticker")
	yeShMgr.inStopping = true
	close(yeShMgr.ddtChan)

	stopCh := make(chan bool, 1)
	yesLog.Debug("Stop: stop dht")
	p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
	<-stopCh
	yesLog.Debug("Stop: dht stopped")

	thisCfg, _ := YeShellCfg[yeShMgr.name]
	if thisCfg.BootstrapNode == false {
		yesLog.Debug("Stop: close dht bootstrap timer")
		close(yeShMgr.dhtBsChan)
	}

	yesLog.Debug("Stop: stop chain")
	p2psh.P2pStop(yeShMgr.chainInst, stopCh)
	<-stopCh
	yesLog.Debug("Stop: chain stopped")
}

func (yeShMgr *YeShellManager) Reconfig(reCfg *RecfgCommand) error {
	if yeShMgr.inStopping {
		return yesInStopping
	}
	if reCfg == nil {
		yesLog.Debug("Reconfig: invalid parameter")
		return errors.New("nil reconfigurate command")
	}
	if reCfg.SubnetMaskBits <= 0 || reCfg.SubnetMaskBits > MaxSubNetMaskBits {
		yesLog.Debug("Reconfig: invalid mask bits: %d", reCfg.SubnetMaskBits)
		return errors.New(fmt.Sprintf("invalid mask bits: %d", reCfg.SubnetMaskBits))
	}

	thisCfg, _ := YeShellCfg[yeShMgr.name]
	if reCfg.SubnetMaskBits == thisCfg.SubNetMaskBits &&
		reCfg.Validator == thisCfg.Validator {
		yesLog.Debug("Reconfig: no reconfiguration needed")
		return errors.New("no reconfiguration needed")
	}

	SnidAdd := make([]SingleSubnetDescriptor, 0)
	SnidDel := make([]config.SubNetworkID, 0)
	SnidDel = append(SnidDel, thisCfg.localSnid...)

	sd := &SubnetDescriptor{
		SubNetKeyList:      make(map[config.SubNetworkID]ecdsa.PrivateKey, 0),
		SubNetNodeList:     make(map[config.SubNetworkID]config.Node, 0),
		SubNetMaxPeers:     make(map[config.SubNetworkID]int, 0),
		SubNetMaxOutbounds: make(map[config.SubNetworkID]int, 0),
		SubNetMaxInBounds:  make(map[config.SubNetworkID]int, 0),
		SubNetIdList:       make([]config.SubNetworkID, 0),
	}
	local := yeShMgr.GetLocalNode()
	priKey := yeShMgr.GetLocalPrivateKey()

	sd.Setup(local, priKey, reCfg.SubnetMaskBits, reCfg.Validator)
	ssdl := sd.GetSubnetDescriptorList()
	SnidAdd = append(SnidAdd, *ssdl...)

	req := sch.MsgShellReconfigReq{
		SnidAdd:  SnidAdd,
		SnidDel:  SnidDel,
		MaskBits: reCfg.SubnetMaskBits,
	}

	msg := sch.SchMessage{}

	yeShMgr.chainInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellReconfigReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("Reconfig: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	for _, snid := range SnidDel {
		for idx := 0; idx < len(thisCfg.localSnid); idx++ {
			if thisCfg.localSnid[idx] == snid {
				if idx != len(thisCfg.localSnid)-1 {
					thisCfg.localSnid = append(thisCfg.localSnid[0:idx], thisCfg.localSnid[idx+1:]...)
				} else {
					thisCfg.localSnid = thisCfg.localSnid[0:idx]
				}
			}
		}
		delete(thisCfg.localNode, snid)
	}

	for _, ssd := range SnidAdd {
		thisCfg.localSnid = append(thisCfg.localSnid, ssd.SubNetId)
		thisCfg.localNode[ssd.SubNetId] = ssd.SubNetNode
	}

	return nil
}

func (yeShMgr *YeShellManager) BroadcastMessage(message Message) error {
	// 按字面的定义：“BroadcastMessage”是全网广播，“BroadcastMessageOsn”是子网结构下的广播。
	// 但是目前实际上这两个接口无法区分的：首先从输入的参数来看，P2P就无法区分这两个有何区别；其次
	// 这两个接口的定义不明确。本来是不打算支持这个接口的，但为以后的扩充方便（比如，如果在接口函数
	// BroadcastMessageOsn的输入参数中指定广播的子网号），暂时直接调用“OSN"接口的函数。
	if yeShMgr.inStopping {
		return yesInStopping
	}
	var err error = nil
	switch message.MsgType {
	case MessageTypeTx:
		err = yeShMgr.broadcastTx(&message)
	case MessageTypeEvent:
		err = yeShMgr.broadcastEv(&message)
	case MessageTypeBlockHeader:
		err = yeShMgr.broadcastBh(&message)
	case MessageTypeBlock:
		err = yeShMgr.broadcastBk(&message)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessage: invalid type: %v", message.MsgType))
	}
	return err
}

func (yeShMgr *YeShellManager) BroadcastMessageOsn(message Message) error {
	if yeShMgr.inStopping {
		return yesInStopping
	}
	var err error = nil
	switch message.MsgType {
	case MessageTypeTx:
		err = yeShMgr.broadcastTxOsn(&message, nil)
	case MessageTypeEvent:
		err = yeShMgr.broadcastEvOsn(&message, nil, false)
	case MessageTypeBlockHeader:
		err = yeShMgr.broadcastBhOsn(&message, nil)
	case MessageTypeBlock:
		err = yeShMgr.broadcastBkOsn(&message, nil)
	default:
		return errors.New(fmt.Sprintf("BroadcastMessageOsn: invalid type: %v", message.MsgType))
	}
	return err
}

func (yeShMgr *YeShellManager) Register(subscriber *Subscriber) {
	if yeShMgr.inStopping {
		return
	}
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.LoadOrStore(t, new(sync.Map))
	m.(*sync.Map).Store(subscriber, true)
}

func (yeShMgr *YeShellManager) UnRegister(subscriber *Subscriber) {
	if yeShMgr.inStopping {
		return
	}
	t := subscriber.MsgType
	m, _ := yeShMgr.subscribers.Load(t)
	if m == nil {
		return
	}
	m.(*sync.Map).Delete(subscriber)
}

func (yeShMgr *YeShellManager) DhtGetValue(key []byte) ([]byte, error) {
	if yeShMgr.inStopping {
		return nil, yesInStopping
	}

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

	yesLog.Debug("DhtGetValue: to pend on getValChan")

	copy(yeShMgr.getValKey[0:], key)
	val, ok := <-yeShMgr.getValChan
	yeShMgr.getValKey = yesKey{0x00, 0x00}

	yesLog.Debug("DhtGetValue: get out from getValChan, ok: %t, val: %x", ok, val)

	if !ok {
		return nil, errors.New("DhtGetValue: failed, channel closed")
	} else if len(val) <= 0 {
		return nil, errors.New("DhtGetValue: empty value")
	}

	return val, nil
}

func (yeShMgr *YeShellManager) DhtSetValue(key []byte, value []byte) error {
	if yeShMgr.inStopping {
		return yesInStopping
	}

	if len(key) != yesKeyBytes || len(value) == 0 {
		yesLog.Debug("DhtSetValue: invalid (key, value) pair, key: %x, length of value: %d", key, len(value))
		return sch.SchEnoParameter
	}

	req := sch.MsgDhtMgrPutValueReq{
		Key:      key,
		Val:      value,
		KeepTime: time.Duration(0),
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("DhtSetValue: failed, eno: %d, error: %s", eno, eno.Error())
		return eno
	}

	yesLog.Debug("DhtSetValue: to pend on putValChan")

	yeShMgr.putValKey = append(yeShMgr.putValKey, key...)
	result, ok := <-yeShMgr.putValChan
	yeShMgr.putValKey = make([]byte, 0)

	yesLog.Debug("DhtSetValue: get out from putValChan, result: %t, ok: %t", result, ok)

	if !ok {
		return errors.New("DhtSetValue: failed, channel closed")
	}
	if result == false {
		return errors.New("DhtSetValue: failed")
	}
	return nil
}

func (yeShMgr *YeShellManager) RegChainProvider(cp ChainProvider) {
	yeShMgr.cp = cp
}

func (yeShMgr *YeShellManager) GetChainInfo(kind string, key []byte) ([]byte, error) {
	if yeShMgr.gcdTimer != nil || yeShMgr.gcdChan != nil {
		return nil, errors.New("GetChainInfo: previous GCD exist")
	}
	yeShMgr.gcdChan = make(chan []byte, 0)
	yeShMgr.gcdTimer = time.NewTimer(yesGetChainDataTm)
	defer yeShMgr.gcdTimer.Stop()

	gcdCleanUp := func() {
		yeShMgr.gcdSeq = 0
		yeShMgr.gcdName = ""
		yeShMgr.gcdKey = make([]byte, 0)
	}
	defer gcdCleanUp()

	yeShMgr.gcdSeq = uint64(time.Now().UnixNano())
	yeShMgr.gcdName = kind
	yeShMgr.gcdKey = key

	msg := new(sch.SchMessage)
	req := sch.MsgShellGetChainInfoReq {
		Seq: yeShMgr.gcdSeq,
		Kind: yeShMgr.gcdName,
		Key: yeShMgr.gcdKey,
	}
	yeShMgr.chainInst.SchMakeMessage(msg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellGetChainInfoReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(msg); eno != sch.SchEnoNone {
		yesLog.Debug("GetChainInfo: SchSendMessage failed, eno: %d", eno)
		return nil, eno
	}

	chainData := ([]byte)(nil)
	gcdOk := false

	select {
	case <-yeShMgr.gcdTimer.C:

		yeShMgr.gcdLock.Lock()

		close(yeShMgr.gcdChan)
		yeShMgr.gcdChan = nil

		yeShMgr.gcdLock.Unlock()
		return nil, errors.New("GetChainInfo: timeout")

	case chainData, gcdOk = <-yeShMgr.gcdChan:

		yeShMgr.gcdLock.Lock()

		if gcdOk {
			close(yeShMgr.gcdChan)
		}
		yeShMgr.gcdChan = nil

		yeShMgr.gcdLock.Unlock()
	}

	if !gcdOk{
		return nil, errors.New("GetChainInfo: channel closed")
	}
	if  len(chainData) == 0 {
		return nil, errors.New("GetChainInfo: empty")
	}
	return chainData, nil
}

func (yeShMgr *YeShellManager) DhtFindNode(target *config.NodeID, done chan interface{}) error {
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

	req := sch.MsgDhtQryMgrQueryStartReq{
		Target:  key,
		Msg:     nil,
		ForWhat: dht.MID_FINDNODE,
		Seq:     dht.GetQuerySeqNo(yeShMgr.dhtInst.SchGetP2pCfgName()),
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

func (yeShMgr *YeShellManager) DhtGetProvider(key []byte, done chan interface{}) error {
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

	req := sch.MsgDhtMgrGetProviderReq{
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

func (yeShMgr *YeShellManager) DhtSetProvider(key []byte, provider *config.Node, done chan interface{}) error {
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

	req := sch.MsgDhtPrdMgrAddProviderReq{
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

func (yeShMgr *YeShellManager) dhtEvProc() {

	evCh := yeShMgr.dhtEvChan

	evHandler := func(evi *sch.MsgDhtShEventInd) error {

		yesLog.Debug("evHandler: event: %d", evi.Evt)

		eno := sch.SchEnoNone
		switch evi.Evt {

		case sch.EvDhtBlindConnectRsp:
			eno = yeShMgr.dhtBlindConnectRsp(evi.Msg.(*sch.MsgDhtBlindConnectRsp))

		case sch.EvDhtMgrFindPeerRsp:
			eno = yeShMgr.dhtMgrFindPeerRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryResultInd))

		case sch.EvDhtQryMgrQueryStartRsp:
			eno = yeShMgr.dhtQryMgrQueryStartRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryStartRsp))

		case sch.EvDhtQryMgrQueryStopRsp:
			eno = yeShMgr.dhtQryMgrQueryStopRsp(evi.Msg.(*sch.MsgDhtQryMgrQueryStopRsp))

		case sch.EvDhtConMgrSendCfm:
			eno = yeShMgr.dhtConMgrSendCfm(evi.Msg.(*sch.MsgDhtConMgrSendCfm))

		case sch.EvDhtMgrPutProviderRsp:
			eno = yeShMgr.dhtMgrPutProviderRsp(evi.Msg.(*sch.MsgDhtPrdMgrAddProviderRsp))

		case sch.EvDhtMgrGetProviderRsp:
			eno = yeShMgr.dhtMgrGetProviderRsp(evi.Msg.(*sch.MsgDhtMgrGetProviderRsp))

		case sch.EvDhtMgrPutValueLocalRsp:
			eno = yeShMgr.dhtMgrPutValueLocalRsp(evi.Msg.(*sch.MsgDhtMgrPutValueLocalRsp))

		case sch.EvDhtMgrPutValueRsp:
			eno = yeShMgr.dhtMgrPutValueRsp(evi.Msg.(*sch.MsgDhtMgrPutValueRsp))

		case sch.EvDhtMgrGetValueRsp:
			eno = yeShMgr.dhtMgrGetValueRsp(evi.Msg.(*sch.MsgDhtMgrGetValueRsp))

		case sch.EvDhtConMgrCloseRsp:
			eno = yeShMgr.dhtConMgrCloseRsp(evi.Msg.(*sch.MsgDhtConMgrCloseRsp))

		default:
			yesLog.Debug("evHandler: invalid event: %d", evi.Evt)
			eno = sch.SchEnoParameter
		}

		yesLog.Debug("evHandler: get out, event: %d", evi.Evt)

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

func (yeShMgr *YeShellManager) dhtCsProc() {
	csCh := yeShMgr.dhtCsChan
	csHandler := func(csi *sch.MsgDhtConInstStatusInd) {
		switch csi.Status {

		case dht.CisNull:
			yesLog.Debug("dhtCsProc: CisNull, peer: %x", *csi.Peer)

		case dht.CisConnecting:
			yesLog.Debug("dhtCsProc: CisConnecting, peer: %x", *csi.Peer)

		case dht.CisConnected:
			yesLog.Debug("dhtCsProc: CisConnected, peer: %x", *csi.Peer)

		case dht.CisAccepted:
			yesLog.Debug("dhtCsProc: CisAccepted, peer: %x", *csi.Peer)

		case dht.CisInHandshaking:
			yesLog.Debug("dhtCsProc: CisInHandshaking, peer: %x", *csi.Peer)

		case dht.CisHandshook:
			yesLog.Debug("dhtCsProc: CisHandshook, peer: %x", *csi.Peer)

		case dht.CisInService:
			yesLog.Debug("dhtCsProc: CisInService, peer: %x", *csi.Peer)

		case dht.CisOutOfService:
			yesLog.Debug("dhtCsProc: CisOutOfService, peer: %x", *csi.Peer)

		case dht.CisClosed:
			yesLog.Debug("dhtCsProc: CisClosed, peer: %x", *csi.Peer)

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

func (yeShMgr *YeShellManager) chainRxProc() {
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

			if pkg.MsgId == int(p2psh.MID_GCD) {

				yeShMgr.getChainDataFromPeer(pkg)

			} else if pkg.MsgId == int(p2psh.MID_PCD) {

				yeShMgr.putChainDataFromPeer(pkg)

			} else {

				k := [yesKeyBytes]byte{}
				copy(k[0:], pkg.Key)
				if yeShMgr.checkDupKey(k) {
					continue
				}

				msgType := yesMidItoa[pkg.MsgId]

				txCount := 0
				evCount := 0
				bhCount := 0
				xxCount := 0
				switch msgType {
				case MessageTypeTx:
					txCount++
				case MessageTypeEvent:
					evCount++
				case MessageTypeBlockHeader:
					bhCount++
				default:
					xxCount++
				}

				if subList, ok := yeShMgr.subscribers.Load(msgType); ok {
					subList.(*sync.Map).Range(func(key, value interface{}) bool {
						msg := Message{
							MsgType: msgType,
							From:    fmt.Sprintf("%x", pkg.PeerInfo.NodeId),
							Key:     pkg.Key,
							Data:    pkg.Payload,
						}

						sub, _ := key.(*Subscriber)
						sub.MsgChan <- msg

						exclude := pkg.PeerInfo.NodeId
						err := error(nil)
						switch msg.MsgType {
						case MessageTypeTx:
							err = yeShMgr.broadcastTxOsn(&msg, &exclude)
						case MessageTypeEvent:
							err = yeShMgr.broadcastEvOsn(&msg, &exclude, false)
						case MessageTypeBlockHeader:
							err = yeShMgr.broadcastBhOsn(&msg, &exclude)
						default:
							err = errors.New(fmt.Sprintf("chainRxProc: invalid message type: %s", msg.MsgType))
						}
						return err == nil
					})
				}
			}
		}
	}

	yesLog.Debug("chainRxProc: exit")
}

func (yeShMgr *YeShellManager) dhtBootstrapProc() {
	defer yeShMgr.bsTicker.Stop()
	thisCfg, _ := YeShellCfg[yeShMgr.name]

_bootstarp:
	for {
		select {
		case <-yeShMgr.bsTicker.C:

			if len(thisCfg.dhtBootstrapNodes) <= 0 {
				yesLog.Debug("dhtBootstrapProc: none of bootstarp nodes")
			} else {
				r := rand.Int31n(int32(len(thisCfg.dhtBootstrapNodes)))
				req := sch.MsgDhtBlindConnectReq{
					Peer: thisCfg.dhtBootstrapNodes[r],
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

func (yeShMgr *YeShellManager) dhtBlindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	yesLog.Debug("dhtBlindConnectRsp: msg: %+v", *msg)
	thisCfg, _ := YeShellCfg[yeShMgr.name]

	for _, bsn := range thisCfg.dhtBootstrapNodes {
		if msg.Eno == dht.DhtEnoNone.GetEno() || msg.Eno == dht.DhtEnoDuplicated.GetEno() {
			if bytes.Compare(msg.Peer.ID[0:], bsn.ID[0:]) == 0 {

				// done the blind-connect routine
				yesLog.Debug("dhtBlindConnectRsp: bootstrap node connected, id: %x", msg.Peer.ID)
				yeShMgr.dhtBsChan <- true

				// when coming here, we should have added the connected bootstarp node to our
				// dht route table, but it's the only node there, we need to start a bootstrap
				// procedure to fill our route now.
				time.Sleep(time.Second)
				schMsg := sch.SchMessage{}
				yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtRutRefreshReq, nil)
				yeShMgr.dhtInst.SchSendMessage(&schMsg)

				return sch.SchEnoNone
			}
		}
	}

	return sch.SchEnoMismatched
}

func (yeShMgr *YeShellManager) dhtMgrFindPeerRsp(msg *sch.MsgDhtQryMgrQueryResultInd) sch.SchErrno {
	yesLog.Debug("dhtMgrFindPeerRsp: msg: %+v", *msg)
	if done, ok := yeShMgr.findNodeMap[msg.Target]; ok {
		done <- msg
		delete(yeShMgr.findNodeMap, msg.Target)
	}
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtQryMgrQueryStartRsp(msg *sch.MsgDhtQryMgrQueryStartRsp) sch.SchErrno {
	yesLog.Debug("dhtQryMgrQueryStartRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtQryMgrQueryStopRsp(msg *sch.MsgDhtQryMgrQueryStopRsp) sch.SchErrno {
	yesLog.Debug("dhtQryMgrQueryStopRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtConMgrSendCfm(msg *sch.MsgDhtConMgrSendCfm) sch.SchErrno {
	yesLog.Debug("dhtConMgrSendCfm: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtMgrPutProviderRsp(msg *sch.MsgDhtPrdMgrAddProviderRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrPutProviderRsp: msg: %+v", *msg)
	if len(msg.Key) != yesKeyBytes {
		return sch.SchEnoParameter
	}
	yk := yesKey{}
	copy(yk[0:], msg.Key)
	if done, ok := yeShMgr.putProviderMap[yk]; ok {
		done <- msg
		delete(yeShMgr.putProviderMap, yk)
	}
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtMgrGetProviderRsp(msg *sch.MsgDhtMgrGetProviderRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrGetProviderRsp: msg: %+v", *msg)
	if len(msg.Key) != yesKeyBytes {
		return sch.SchEnoParameter
	}
	yk := yesKey{}
	copy(yk[0:], msg.Key)
	if done, ok := yeShMgr.getProviderMap[yk]; ok {
		done <- msg
		delete(yeShMgr.getProviderMap, yk)
	}
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager)dhtMgrPutValueLocalRsp(msg *sch.MsgDhtMgrPutValueLocalRsp) sch.SchErrno {
	// Notice: only when function DhtSetValue called, yeShMgr.putValKey would be set
	// and would play in a blocked mode, we check this case to feed the signal into
	// the channel which the caller is pending for, else nothing done.
	if yeShMgr.putValKey != nil && len(yeShMgr.putValKey) == yesKeyBytes {
		if bytes.Compare(yeShMgr.putValKey[0:], msg.Key) != 0 {
			yesLog.Debug("dhtMgrPutValueLocalRsp: key mismatched")
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

func (yeShMgr *YeShellManager) dhtMgrPutValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	// see function dhtMgrPutValueLocalRsp for more please, an event EvDhtMgrPutValueLocalRsp should
	// had received before this EvDhtMgrPutValueRsp.
	yesLog.Debug("dhtMgrPutValueRsp: eno: %d, key: %x, peers: %d", msg.Eno, msg.Key, len(msg.Peers))
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtMgrGetValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrGetValueRsp: msg: %+v", *msg)
	if bytes.Compare(yeShMgr.getValKey[0:], msg.Key) != 0 {
		yesLog.Debug("dhtMgrGetValueRsp: key mismatched")
		return sch.SchEnoMismatched
	}
	if msg.Eno == dht.DhtEnoNone.GetEno() {
		yeShMgr.getValChan <- msg.Val
	} else {
		yeShMgr.getValChan <- []byte{}
	}
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtConMgrCloseRsp(msg *sch.MsgDhtConMgrCloseRsp) sch.SchErrno {
	yesLog.Debug("dhtConMgrCloseRsp: msg: %+v", *msg)
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) broadcastTx(msg *Message) error {
	return yeShMgr.broadcastTxOsn(msg, nil)
}

func (yeShMgr *YeShellManager) broadcastEv(msg *Message) error {
	return yeShMgr.broadcastEvOsn(msg, nil, false)
}

func (yeShMgr *YeShellManager) broadcastBh(msg *Message) error {
	return yeShMgr.broadcastBhOsn(msg, nil)
}

func (yeShMgr *YeShellManager) broadcastBk(msg *Message) error {
	return yeShMgr.broadcastBkOsn(msg, nil)
}

func (yeShMgr *YeShellManager) broadcastTxOsn(msg *Message, exclude *config.NodeID) error {
	// if local node is a validator, the Tx should be broadcast over the
	// validator-subnet; else the Tx should be broadcast over the dynamic
	// subnet. this is done in chain shell manager, and the message here
	// would be dispatched to chain shell manager.
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if yeShMgr.checkDupKey(k) {
		return errors.New("broadcastTxOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastTxOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq{
		MsgType: yesMtAtoi[msg.MsgType],
		From:    msg.From,
		Key:     msg.Key,
		Data:    msg.Data,
		Exclude: exclude,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastTxOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *YeShellManager) broadcastEvOsn(msg *Message, exclude *config.NodeID, dht bool) error {
	// the local node must be a validator, and the Ev should be broadcast
	// over the validator-subnet. also, the Ev should be stored into DHT
	// with a duration to be expired. the message here would be dispatched
	// to chain shell manager and DHT shell manager.
	thisCfg, _ := YeShellCfg[yeShMgr.name]
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if yeShMgr.checkDupKey(k) {
		return errors.New("broadcastEvOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastEvOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq{
		MsgType: yesMtAtoi[msg.MsgType],
		From:    msg.From,
		Key:     msg.Key,
		Data:    msg.Data,
		Exclude: exclude,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	if dht {
		req2Dht := sch.MsgDhtMgrPutValueReq{
			Key:      msg.Key,
			Val:      msg.Data,
			KeepTime: thisCfg.EvKeepTime,
		}
		yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req2Dht)
		if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			yesLog.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
			return eno
		}
	}

	return nil
}

func (yeShMgr *YeShellManager) broadcastBhOsn(msg *Message, exclude *config.NodeID) error {
	// the Bh should be broadcast over the any-subnet. the message here
	// would be dispatched to chain shell manager.
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if yeShMgr.checkDupKey(k) {
		return errors.New("broadcastBhOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastBhOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgShellBroadcastReq{
		MsgType: yesMtAtoi[msg.MsgType],
		From:    msg.From,
		Key:     msg.Key,
		Data:    msg.Data,
		Exclude: exclude,
	}
	yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastBhOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}
	return nil
}

func (yeShMgr *YeShellManager) broadcastBkOsn(msg *Message, exclude *config.NodeID) error {
	// the Bk should be stored by DHT and no broadcasting over any subnet.
	// the message here would be dispatched to DHT shell manager.
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if yeShMgr.checkDupKey(k) {
		return errors.New("broadcastBkOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k); err != nil {
		yesLog.Debug("broadcastBkOsn: error: %s", err.Error())
		return err
	}

	schMsg := sch.SchMessage{}
	req := sch.MsgDhtMgrPutValueReq{
		Key:      msg.Key,
		Val:      msg.Data,
		KeepTime: dht.DsMgrDurInf,
	}

	yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
		yesLog.Debug("broadcastBkOsn: SchSendMessage failed, eno: %d", eno)
		return eno
	}

	return nil
}

func GetSubnetIdentity(id config.NodeID, maskBits int) (config.SubNetworkID, error) {
	if maskBits <= 0 || maskBits > MaxSubNetMaskBits {
		return config.SubNetworkID{}, errors.New("invalid mask bits")
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

func (yeShMgr *YeShellManager) deDupTimerCb(el *list.Element, data interface{}) interface{} {
	// Notice: do not invoke Lock ... Unlock ... on yeShMgr.deDupLock here
	// please, since this function is called back within TickProc of timer
	// manager when any timer get expired. See function deDupTickerProc.
	if key, ok := data.(*[yesKeyBytes]byte); !ok {
		return errors.New("deDupTimerCb: invalid key")
	} else {
		delete(yeShMgr.deDupMap, *key)
		return nil
	}
}

func (yeShMgr *YeShellManager) setDedupTimer(key [yesKeyBytes]byte) error {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()

	thisCfg, _ := YeShellCfg[yeShMgr.name]

	if len(key) != yesKeyBytes {
		return errors.New(fmt.Sprintf("setDedupTimer: invalid key length: %d", len(key)))
	}

	tm, err := yeShMgr.tmDedup.GetTimer(thisCfg.DedupTime, nil, yeShMgr.deDupTimerCb)
	if err != dht.TmEnoNone {
		yesLog.Debug("setDedupTimer: GetTimer failed, error: %s", err.Error())
		return err
	}

	key_ := key
	yeShMgr.tmDedup.SetTimerData(tm, &key_)
	if err := yeShMgr.tmDedup.StartTimer(tm); err != dht.TmEnoNone {
		yesLog.Debug("setDedupTimer: StartTimer failed, error: %s", err.Error())
		return err
	}

	yeShMgr.deDupMap[key] = true
	return nil
}

func (yeShMgr *YeShellManager) deDupTickerProc() {
	defer yeShMgr.deDupTiker.Stop()
_dedup:
	for {
		select {
		case <-yeShMgr.deDupTiker.C:

			yeShMgr.deDupLock.Lock()
			yeShMgr.tmDedup.TickProc()
			yeShMgr.deDupLock.Unlock()

		case <-yeShMgr.ddtChan:
			break _dedup
		}
	}
	yesLog.Debug("deDupTickerProc: exit")
}

func (yeShMgr *YeShellManager) GetLocalNode() *config.Node {
	cfg := yeShMgr.chainInst.SchGetP2pConfig()
	return &cfg.Local
}

func (yeShMgr *YeShellManager) GetLocalPrivateKey() *ecdsa.PrivateKey {
	cfg := yeShMgr.chainInst.SchGetP2pConfig()
	return cfg.PrivateKey
}

func (yeShMgr *YeShellManager) GetLocalDhtNode() *config.Node {
	cfg := yeShMgr.chainInst.SchGetP2pConfig()
	return &cfg.DhtLocal
}

func (yeShMgr *YeShellManager) checkDupKey(k yesKey) bool {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()
	_, dup := yeShMgr.deDupMap[k]
	return dup
}

func (yeShMgr *YeShellManager) getChainDataFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	upkg := new(peer.P2pPackage)
	upkg.Pid = uint32(rxPkg.ProtoId)
	upkg.Mid = uint32(rxPkg.MsgId)
	upkg.Key = rxPkg.Key
	upkg.PayloadLength = uint32(rxPkg.PayloadLength)
	upkg.Payload = rxPkg.Payload

	msg := peer.ExtMessage{}
	if eno := upkg.GetExtMessage(&msg); eno != peer.PeMgrEnoNone {
		yesLog.Debug("getChainDataFromPeer: GetExtMessage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if yeShMgr.cp != nil {
		data := yeShMgr.cp.GetChainData(msg.Gcd.Name, msg.Gcd.Key)
		if len(data) > 0 {
			rsp := sch.MsgShellGetChainInfoRsp {
				Peer: rxPkg.PeerInfo,
				Seq: msg.Gcd.Seq,
				Kind: msg.Gcd.Name,
				Key: msg.Gcd.Key,
			}
			schMsg := new(sch.SchMessage)
			yeShMgr.chainInst.SchMakeMessage(schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell,
				sch.EvShellGetChainInfoRsp, &rsp)
			yeShMgr.chainInst.SchSendMessage(schMsg)
			return sch.SchEnoNone
		}
	}
	return sch.SchEnoResource
}

func (yeShMgr *YeShellManager) putChainDataFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	yeShMgr.gcdLock.Lock()
	defer yeShMgr.gcdLock.Unlock()

	upkg := new(peer.P2pPackage)
	upkg.Pid = uint32(rxPkg.ProtoId)
	upkg.Mid = uint32(rxPkg.MsgId)
	upkg.Key = rxPkg.Key
	upkg.PayloadLength = uint32(rxPkg.PayloadLength)
	upkg.Payload = rxPkg.Payload

	msg := peer.ExtMessage{}
	if eno := upkg.GetExtMessage(&msg); eno != peer.PeMgrEnoNone {
		yesLog.Debug("putChainDataFromPeer: GetExtMessage failed, eno: %d", eno)
		return sch.SchEnoUserTask
	}

	if msg.Pcd.Seq == yeShMgr.gcdSeq &&
		bytes.Compare(msg.Pcd.Key, yeShMgr.gcdKey) == 0 &&
		strings.Compare(msg.Pcd.Name, yeShMgr.gcdName) == 0 {
		if yeShMgr.gcdChan != nil {
			yeShMgr.gcdChan <- msg.Pcd.Data
			return sch.SchEnoNone
		}
	}

	yesLog.Debug("putChainDataFromPeer: discarded, seq: %d, name: %s, key: %x",
		msg.Pcd.Seq, msg.Pcd.Name, msg.Pcd.Key)

	return sch.SchEnoNone
}

func Snid2Int(snid config.SubNetworkID) int {
	return (int(snid[0]) << 8) + int(snid[1])
}

func SetupSubNetwork(cfg *config.Config, mbs int, vdt bool) error {
	// Notice: this function should be called after config.Local is setup
	if mbs < 0 || mbs > MaxSubNetMaskBits {
		yesLog.Debug("setupSubNetwork: invalid subnet mask bits: %d", mbs)
		return errors.New("invalid subnet mask bits")
	} else if mbs == 0 {
		cfg.SubNetKeyList[config.AnySubNet] = *cfg.PrivateKey
		cfg.SubNetIdList = append(cfg.SubNetIdList, config.AnySubNet)
		cfg.SubNetNodeList[config.AnySubNet] = cfg.Local
		cfg.SubNetMaxPeers[config.AnySubNet] = config.MaxPeers
		cfg.SubNetMaxOutbounds[config.AnySubNet] = config.MaxOutbounds
		cfg.SubNetMaxInBounds[config.AnySubNet] = config.MaxInbounds
	} else if vdt == false {
		snid, err := GetSubnetIdentity(cfg.Local.ID, mbs)
		if err != nil {
			return errors.New("GetSubnetIdentity failed")
		}
		cfg.SubNetKeyList[snid] = *cfg.PrivateKey
		cfg.SubNetIdList = append(cfg.SubNetIdList, snid)
		cfg.SubNetNodeList[snid] = cfg.Local
		cfg.SubNetMaxPeers[snid] = config.MaxPeers
		cfg.SubNetMaxOutbounds[snid] = config.MaxOutbounds
		cfg.SubNetMaxInBounds[snid] = config.MaxInbounds
	} else {
		count := 1 << uint(mbs)
		cfg.SubNetIdList = make([]config.SubNetworkID, count)
		for count > 0 {
			prvKey, err := config.GenerateKey()
			if err != nil {
				continue
			}
			id := config.P2pPubkey2NodeId(&prvKey.PublicKey)
			snid, err := GetSubnetIdentity(*id, mbs)
			if err != nil {
				return errors.New("GetSubnetIdentity failed")
			}
			if _, dup := cfg.SubNetKeyList[snid]; dup {
				continue
			}
			cfg.SubNetKeyList[snid] = *prvKey
			cfg.SubNetIdList[Snid2Int(snid)] = snid
			cfg.SubNetNodeList[snid] = config.Node{
				IP:  cfg.Local.IP,
				UDP: cfg.Local.UDP,
				TCP: cfg.Local.TCP,
				ID:  *id,
			}
			cfg.SubNetMaxPeers[snid] = config.MaxPeers
			cfg.SubNetMaxOutbounds[snid] = config.MaxOutbounds
			cfg.SubNetMaxInBounds[snid] = config.MaxInbounds
			count--
		}
	}
	cfg.SnidMaskBits = mbs
	return nil
}

func (snd *SubnetDescriptor) Setup(node *config.Node, priKey *ecdsa.PrivateKey, mbs int, vdt bool) error {
	if mbs < 0 || mbs > MaxSubNetMaskBits {
		yesLog.Debug("Setup: invalid subnet mask bits: %d", mbs)
		return errors.New("invalid subnet mask bits")
	} else if mbs == 0 {
		snd.SubNetKeyList[config.ZeroSubNet] = *priKey
		snd.SubNetIdList = append(snd.SubNetIdList, config.ZeroSubNet)
		snd.SubNetNodeList[config.ZeroSubNet] = *node
		snd.SubNetMaxPeers[config.ZeroSubNet] = config.MaxPeers
		snd.SubNetMaxOutbounds[config.ZeroSubNet] = config.MaxOutbounds
		snd.SubNetMaxInBounds[config.ZeroSubNet] = config.MaxInbounds
	} else if vdt == false {
		snid, err := GetSubnetIdentity(node.ID, mbs)
		if err != nil {
			yesLog.Debug("Setup: GetSubnetIdentity failed")
			return errors.New("GetSubnetIdentity failed")
		}
		snd.SubNetKeyList[snid] = *priKey
		snd.SubNetIdList = append(snd.SubNetIdList, snid)
		snd.SubNetNodeList[snid] = *node
		snd.SubNetMaxPeers[snid] = config.MaxPeers
		snd.SubNetMaxOutbounds[snid] = config.MaxOutbounds
		snd.SubNetMaxInBounds[snid] = config.MaxInbounds
	} else {
		count := 1 << uint(mbs)
		snd.SubNetIdList = make([]config.SubNetworkID, count)
		for count > 0 {
			prvKey, err := config.GenerateKey()
			if err != nil {
				continue
			}
			id := config.P2pPubkey2NodeId(&prvKey.PublicKey)
			snid, err := GetSubnetIdentity(*id, mbs)
			if err != nil {
				yesLog.Debug("Setup: GetSubnetIdentity failed")
				return errors.New("GetSubnetIdentity failed")
			}
			if _, dup := snd.SubNetKeyList[snid]; dup {
				continue
			}
			snd.SubNetKeyList[snid] = *prvKey
			snd.SubNetIdList[Snid2Int(snid)] = snid
			snd.SubNetNodeList[snid] = config.Node{
				IP:  node.IP,
				UDP: node.UDP,
				TCP: node.TCP,
				ID:  *id,
			}
			snd.SubNetMaxPeers[snid] = config.MaxPeers
			snd.SubNetMaxOutbounds[snid] = config.MaxOutbounds
			snd.SubNetMaxInBounds[snid] = config.MaxInbounds
			count--
		}
	}
	return nil
}

func (snd *SubnetDescriptor) GetSubnetDescriptorList() *[]SingleSubnetDescriptor {
	ssdl := make([]SingleSubnetDescriptor, 0)
	for idx := 0; idx < len(snd.SubNetIdList); idx++ {
		snid := snd.SubNetIdList[idx]
		ssd := SingleSubnetDescriptor{
			SubNetKey:          snd.SubNetKeyList[snid],
			SubNetNode:         snd.SubNetNodeList[snid],
			SubNetMaxPeers:     snd.SubNetMaxPeers[snid],
			SubNetMaxOutbounds: snd.SubNetMaxOutbounds[snid],
			SubNetMaxInBounds:  snd.SubNetMaxInBounds[snid],
			SubNetId:           snid,
		}
		ssdl = append(ssdl, ssd)
	}
	return &ssdl
}

