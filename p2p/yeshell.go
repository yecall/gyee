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

	"github.com/yeeco/gyee/log"
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
	dht__ bool
	dhtCis__ bool
}

var yesLog = yesLogger{
	debug__: false,
	dht__: false,
	dhtCis__: false,
}

func (log yesLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

func (log yesLogger) DebugDht(fmt string, args ...interface{}) {
	if log.dht__ {
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

const (
	yesNull	= iota	// null
	yesDhtStart		// dht startup
	yesDhtReady		// dht ready
	yesChainReady	// all ready
)

const (
	GVTO = time.Second * 64	// get value timeout
	GVBS = 256				// get value buffer size
	PVTO = time.Second * 64	// put value timeout
	PVBS = 256				// put value buffer size
	GCITO = time.Second * 8	// duration for get chain information
	GCIBS = 64				// get chain formation buffer size
	gvkChBufSize = 64		// get value duplicated channel buffer size
)

type SingleSubnetDescriptor = sch.SingleSubnetDescriptor // single subnet descriptor

type yesKey = config.DsKey

type getValueResult struct {
	eno		int			// result
	key		[]byte		// key
	value	[]byte		// value
}

type putValueResult struct {
	eno		int			// result
	key		[]byte		// key
}

const GCIKEY_LEN = 32
type getChainInfoKeyEx struct {
	name	string				// name(kind)
	key		[GCIKEY_LEN]byte	// key, obtained from a slice, "0"s padding
	keyLen	int					// ken length
}

type getChainInfoValEx struct {
	gcdChan		chan []byte		// channel to sleep on
	gcdTimer	*time.Timer		// timer for expiration
	gcdSeq		uint64			// sequence
}

type YesErrno int
const (
	YesEnoNone			YesErrno = iota
	YesEnoInStopping
	YesEnoParameter
	YesEnoPutValFull
	YesEnoPutValDup
	YesEnoGetValFull
	YesEnoGetValDup
	YesEnoGetValDupFull
	YesEnoGcdFull
	YesEnoGcdDup
	YesEnoTimeout
	YesEnoResource
	YesEnoScheduler
	YesEnoChClosed
	YesEnoEmptyVal
	YesEnoDhtInteral
	YesEnoUnknown
)
var YesErrMsg = []string {
	"yesmgr: YesEnoNone",
	"yesmgr: YesEnoInStopping",
	"yesmgr: YesEnoParameter",
	"yesmgr: YesEnoPutValFull",
	"yesmgr: YesEnoPutValDup",
	"yesmgr: YesEnoGetValFull",
	"yesmgr: YesEnoGetValDup",
	"yesmgr: YesEnoGetValDupFull",
	"yesmgr: YesEnoGcdFull",
	"yesmgr: YesEnoGcdDup",
	"yesmgr: YesEnoTimeout",
	"yesmgr: YesEnoResource",
	"yesmgr: YesEnoScheduler",
	"yesmgr: YesEnoChClosed",
	"yesmgr: YesEnoEmptyVal",
	"yesmgr: YesEnoDhtInteral",
	"yesmgr: YesEnoUnknown",
}
func (eno YesErrno)Error() string {
	if eno > YesEnoUnknown {
		return "yesmgr: invalid eno"
	}
	return YesErrMsg[eno]
}

type YeShellManager struct {
	name           string                           // unique name of the shell manager
	config         *YeShellConfig					// configuration
	inStopping     bool                             // in stopping procedure
	status			int								// shell status
	readyCh        chan struct{}                    // close channel to notify service ready
	chainInst      *sch.Scheduler                   // chain scheduler pointer
	chainSdlName   string							// chain scheduler name
	ptnChainShell  interface{}                      // chain shell manager task node pointer
	ptChainShMgr   *p2psh.ShellManager              // chain shell manager object
	dhtInst        *sch.Scheduler                   // dht scheduler pointer
	dhtSdlName     string							// dht scheduler name
	ptnDhtShell    interface{}                      // dht shell manager task node pointer
	ptDhtShMgr     *p2psh.DhtShellManager           // dht shell manager object
	ptDhtConMgr    *dht.ConMgr					    // dht connection manager object
	gvk2DurMap     map[yesKey]time.Duration			// remain time to wait
	gvk2ChMap      map[yesKey][]chan *getValueResult // channel for get value
	getValChan     chan *getValueResult             // get value channel
	getValLock     sync.Mutex						// lock for get value
	pvk2DurMap     map[yesKey]time.Duration			// remain time to wait
	pvk2ChMap      map[yesKey]chan bool				// channel for put value
	putValChan     chan *putValueResult             // put value channel
	putValLock     sync.Mutex						// lock for put value
	findNodeMap    map[yesKey]chan interface{}      // find node command map to channel
	getProviderMap map[yesKey]chan interface{}      // find node command map to channel
	putProviderMap map[yesKey]chan interface{}      // find node command map to channel
	dhtEvChan      chan *sch.MsgDhtShEventInd       // dht event indication channel
	dhtCsChan      chan *sch.MsgDhtConInstStatusInd // dht connection status indication channel
	subscribers    *sync.Map                        // subscribers for incoming messages
	chainRxChan    chan *peer.P2pPackageRx          // total rx channel for chain
	deDupLock      sync.Mutex                       // lock for deduplication timer manager
	tmDedup        *dht.TimerManager                // deduplication timer manager
	deDupMap       map[yesKey][]byte		       // map for keys of messages had been sent
	deDupTiker     *time.Ticker                     // deduplication ticker
	ddtChan        chan bool                        // deduplication ticker channel
	bsTicker       *time.Ticker                     // bootstrap ticker
	dhtBsChan      chan bool                        // bootstrap ticker channel
	cp             ChainProvider                    // interface registered to p2p for "get chain data" message
	gciLock		   sync.Mutex						// get chain data lock
	gciMap         map[getChainInfoKeyEx]*getChainInfoValEx // map for get chain information
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
	chainId			  uint32							  // chain identity
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
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:30304:30304",
	},
	DhtBootstrapNodes: []string{
		"3CEF400192372CD94AAE8DCA465A4A48D4FFBF7E7364D5044CD003F07DCBB0D4EEA7E311D9ED0852890C2B72E79893F0CBA5238A09F7B441613218C3A0D4659B@192.168.1.109:40405:40405",
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

func YeShellConfigToP2pCfg(yesCfg *YeShellConfig) ([]*config.Config, *YeShellConfig) {
	if yesCfg == nil {
		yesLog.Debug("YeShellConfigToP2pCfg: nil configuration")
		return nil, nil
	}

	if yesCfg.AppType != config.P2P_TYPE_ALL {
		yesLog.Debug("YeShellConfigToP2pCfg: P2P_TYPE_ALL needed")
		return nil, nil
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
	chainCfg.ChainId = yesCfg.chainId
	chainCfg.NodeDataDir = yesCfg.NodeDataDir
	chainCfg.DhtFdsCfg.Path = yesCfg.NodeDataDir
	if yesCfg.NodeDatabase != "" {
		chainCfg.NodeDatabase = yesCfg.NodeDatabase
	}

	yesLog.Debug("YeShellConfigToP2pCfg: local addr: chain[%s:%d:%d], dht[%s:%d]",
		yesCfg.LocalNodeIp, yesCfg.LocalUdpPort, yesCfg.LocalTcpPort,
		yesCfg.LocalDhtIp, yesCfg.LocalDhtPort)

	if config.P2pSetLocalIpAddr(chainCfg, yesCfg.LocalNodeIp, yesCfg.LocalUdpPort,
		yesCfg.LocalTcpPort) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetLocalIpAddr failed")
		return nil, nil
	}
	if config.P2pSetupLocalNodeId(chainCfg) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetupLocalNodeId failed")
		return nil, nil
	}
	if err := SetupSubNetwork(chainCfg, yesCfg.SubNetMaskBits, yesCfg.Validator); err != nil {
		yesLog.Debug("YeShellConfigToP2pCfg: SetupSubNetwork failed")
		return nil, nil
	}
	thisCfg.localSnid = append(thisCfg.localSnid, chainCfg.SubNetIdList...)
	for _, snid := range thisCfg.localSnid {
		thisCfg.localNode[snid] = chainCfg.SubNetNodeList[snid]
	}

	yesLog.Debug("YeShellConfigToP2pCfg: NatType: %s, GatewayIp: %s", yesCfg.NatType, yesCfg.GatewayIp)
	config.P2pSetupNatType(chainCfg, yesCfg.NatType, yesCfg.GatewayIp)

	yesLog.Debug("YeShellConfigToP2pCfg: LocalDhtIp: %s, LocalDhtPort: %d",
		yesCfg.LocalDhtIp, yesCfg.LocalDhtPort)
	if config.P2pSetLocalDhtIpAddr(chainCfg, yesCfg.LocalDhtIp, yesCfg.LocalDhtPort) != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetLocalDhtIpAddr failed")
		return nil, nil
	}

	if chCfgName, eno := config.P2pSetConfig("chain", chainCfg); eno != config.P2pCfgEnoNone {
		yesLog.Debug("YeShellConfigToP2pCfg: P2pSetConfig failed")
		return nil, nil
	} else {
		chainCfg = config.P2pGetConfig(chCfgName)
	}

	dhtCfg = new(config.Config)
	*dhtCfg = *chainCfg

	bsn := config.P2pSetupBootstrapNodes(thisCfg.DhtBootstrapNodes)
	thisCfg.dhtBootstrapNodes = append(thisCfg.dhtBootstrapNodes, bsn...)
	dhtCfg.AppType = config.P2P_TYPE_DHT
	cfg[ChainCfgIdx] = chainCfg
	cfg[DhtCfgIdx] = dhtCfg
	YeShellCfg[yesCfg.Name] = thisCfg
	yesCfg.dhtBootstrapNodes = thisCfg.dhtBootstrapNodes

	return cfg, &thisCfg
}

func NewYeShellManager(yesCfg *YeShellConfig) *YeShellManager {
	var eno sch.SchErrno
	yeShMgr := YeShellManager{
		name:           yesCfg.Name,
		inStopping:     false,
		readyCh:		make(chan struct{}, 1),
		getValChan:     make(chan *getValueResult, 0),
		gvk2DurMap:		make(map[yesKey]time.Duration, 0),
		gvk2ChMap:		make(map[yesKey][]chan *getValueResult, 0),
		putValChan:     make(chan *putValueResult, 0),
		pvk2DurMap:		make(map[yesKey]time.Duration, 0),
		pvk2ChMap:		make(map[yesKey]chan bool, 0),
		findNodeMap:    make(map[yesKey]chan interface{}, yesMaxFindNode),
		getProviderMap: make(map[yesKey]chan interface{}, yesMaxGetProvider),
		putProviderMap: make(map[yesKey]chan interface{}, yesMaxPutProvider),
		subscribers:    new(sync.Map),
		deDupMap:       make(map[yesKey][]byte, 0),
		ddtChan:        make(chan bool, 1),
		gciMap:			make(map[getChainInfoKeyEx]*getChainInfoValEx, 0),
	}

	cfg, shellCfg := YeShellConfigToP2pCfg(yesCfg)
	yeShMgr.config = shellCfg
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
	yeShMgr.chainSdlName = yeShMgr.chainInst.SchGetP2pCfgName()

	yeShMgr.dhtInst, eno = p2psh.P2pCreateInstance(cfg[DhtCfgIdx])
	if eno != sch.SchEnoNone || yeShMgr.dhtInst == nil {
		yesLog.Debug("NewYeShellManager: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}
	yeShMgr.dhtSdlName = yeShMgr.dhtInst.SchGetP2pCfgName()
	dht.SetBootstrapNodes(yesCfg.dhtBootstrapNodes, yeShMgr.dhtSdlName)

	return &yeShMgr
}

func (yeShMgr *YeShellManager) Start() error {
	var eno sch.SchErrno
	var ok bool

	yeShMgr.inStopping = false
	yeShMgr.status = yesNull

	yesLog.Debug("yeShMgr: start...")

	dht.SetChConMgrReady(yeShMgr.dhtInst.SchGetP2pCfgName(), make(chan bool, 1))
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

	yeShMgr.ptDhtConMgr, ok = yeShMgr.dhtInst.SchGetTaskObject(sch.DhtConMgrName).(*dht.ConMgr)
	if !ok || yeShMgr.ptDhtConMgr == nil {
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		return nil
	}

	if eno := p2psh.P2pStart(yeShMgr.chainInst); eno != sch.SchEnoNone {
		stopCh := make(chan bool, 0)
		yesLog.Debug("Start: failed, eno: %d, error: %s", eno, eno.Error())
		p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
		return eno
	}

	yeShMgr.status = yesDhtStart

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

	yesLog.Debug("Start: go shell routines...")

	yeShMgr.dhtEvChan = yeShMgr.ptDhtShMgr.GetEventChan()
	yeShMgr.dhtCsChan = yeShMgr.ptDhtShMgr.GetConnStatusChan()
	yeShMgr.chainRxChan = yeShMgr.ptChainShMgr.GetRxChan()
	yeShMgr.tmDedup = dht.NewTimerManager()
	yeShMgr.deDupTiker = time.NewTicker(dht.OneTick)

	go yeShMgr.dhtEvProc()
	go yeShMgr.dhtCsProc()

	thisCfg := yeShMgr.config
	if thisCfg.BootstrapNode == false {
		yesLog.Debug("Start: wait dht ready, inst: %s", yeShMgr.dhtInst.SchGetP2pCfgName())
		if dht.DhtReady(yeShMgr.dhtInst.SchGetP2pCfgName()) {
			yeShMgr.bsTicker = time.NewTicker(thisCfg.BootstrapTime)
			yeShMgr.dhtBsChan = make(chan bool, 1)
			go yeShMgr.dhtBootstrapProc()
			go yeShMgr.dhtPutValProc()
			go yeShMgr.dhtGetValProc()
		}
	}

	yeShMgr.status = yesDhtReady
	go yeShMgr.chainReady4User()
	go yeShMgr.chainRxProc()
	go yeShMgr.deDupTickerProc()

	yeShMgr.status = yesChainReady

	yesLog.Debug("Start: shell ok")

	return nil
}

func (yeShMgr *YeShellManager)getStatus() int {
	return yeShMgr.status
}

func (yeShMgr *YeShellManager) Stop() {
	yesLog.Debug("Stop: close deduplication ticker")
	yeShMgr.inStopping = true
	close(yeShMgr.ddtChan)

	stopCh := make(chan bool, 1)
	yesLog.Debug("Stop: stop dht")
	p2psh.P2pStop(yeShMgr.dhtInst, stopCh)
	<-stopCh
	close(yeShMgr.getValChan)
	close(yeShMgr.putValChan)
	yesLog.Debug("Stop: dht stopped")

	log.Info("Stop: dht done", yeShMgr.dhtSdlName)

	thisCfg := yeShMgr.config
	if thisCfg.BootstrapNode == false {
		yesLog.Debug("Stop: close dht bootstrap timer")
		close(yeShMgr.dhtBsChan)
	}

	yesLog.Debug("Stop: stop chain")
	p2psh.P2pStop(yeShMgr.chainInst, stopCh)
	<-stopCh
	yesLog.Debug("Stop: chain stopped")

	log.Info("Stop: chain done", yeShMgr.chainSdlName)
}

func (yeShMgr *YeShellManager) Ready() {
	<-yeShMgr.readyCh
}

func (yeShMgr *YeShellManager) Reconfig(reCfg *RecfgCommand) error {
	if yeShMgr.inStopping {
		return YesEnoInStopping
	}
	if reCfg == nil {
		yesLog.Debug("Reconfig: invalid parameter")
		return errors.New("nil reconfigurate command")
	}
	if reCfg.SubnetMaskBits <= 0 || reCfg.SubnetMaskBits > MaxSubNetMaskBits {
		yesLog.Debug("Reconfig: invalid mask bits: %d", reCfg.SubnetMaskBits)
		return errors.New(fmt.Sprintf("invalid mask bits: %d", reCfg.SubnetMaskBits))
	}

	thisCfg := yeShMgr.config
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
		return YesEnoInStopping
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
		return YesEnoInStopping
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

// simple statistics for DhtGetValue
const _gvStatistics = false
var _gvStatLock sync.Mutex
var _gvFailedCount int64 = 0
var _gvOkCount int64 = 0
func _gvStat(result bool) {
	_gvStatLock.Lock()
	defer _gvStatLock.Unlock()
	if result {
		if _gvOkCount++; _gvOkCount & 0x7f == 0 {
			p2plog.Debug("_gvStat: ok: %d", _gvOkCount)
		}
	} else {
		if _gvFailedCount++; _gvFailedCount & 0x7f == 0 {
			p2plog.Debug("_gvStat: failed: %d", _gvFailedCount)
		}
	}
}

func (yeShMgr *YeShellManager) DhtGetValue(key []byte) ([]byte, error) {
	sdl := yeShMgr.dhtSdlName
	if yeShMgr.inStopping {
		log.Warnf("DhtGetValue: in stopping, sdl: %s", sdl)
		return nil, YesEnoInStopping
	}
	if yeShMgr.ptDhtConMgr.IsBusy(){
		log.Warnf("DhtGetValue: dht busy, sdl: %s", sdl)
		return nil, YesEnoResource
	}
	if len(key) != yesKeyBytes {
		log.Warnf("DhtGetValue: invalid key, sdl: %s, key: %x", sdl, key)
		return nil, YesEnoParameter
	}

	ch := make(chan *getValueResult, 1)
	err := yeShMgr.dhtGetValMapKey(key, GVTO, ch)
	if err != YesEnoNone && err != YesEnoGetValDup {
		log.Debugf("DhtGetValue: dhtGetValMapKey failed, sdl: %s, key: %x, err: %s",
			sdl, key, err.Error())
		return nil, err
	}
	if err == YesEnoNone {
		req := sch.MsgDhtMgrGetValueReq{
			Key: key,
		}
		msg := sch.SchMessage{}
		yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetValueReq, &req)
		if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
			log.Debugf("DhtGetValue: scheduler failed, sdl: %s, key: %x, err: %s",
				sdl, key, eno.Error())
			yk := yesKey{}
			copy(yk[0:], key)
			yeShMgr.getValLock.Lock()
			delete(yeShMgr.gvk2ChMap, yk)
			delete(yeShMgr.gvk2DurMap, yk)
			yeShMgr.getValLock.Unlock()
			return nil, YesEnoScheduler
		}
	}

	log.Debugf("DhtGetValue: pending, sdl: %s, key: %x", sdl, key)
	result, ok := <-ch
	if !ok {
		log.Debugf("DhtGetValue: timeout, sdl: %s, key: %x", sdl, key)
		if _gvStatistics {
			_gvStat(false)
		}
		return nil, YesEnoTimeout
	} else if result.eno != dht.DhtEnoNone.GetEno() {
		log.Debugf("DhtGetValue: dht failed, sdl: %s, eno: %d, key: %x", sdl, result.eno, key)
		if _gvStatistics {
			_gvStat(false)
		}
		return nil, YesEnoDhtInteral
	} else if len(result.value) <= 0 {
		log.Debugf("DhtGetValue: empty value, sdl: %s, key: %s", sdl, key)
		if _gvStatistics {
			_gvStat(false)
		}
		return nil, YesEnoEmptyVal
	}

	log.Debugf("DhtGetValue: ok, sdl: %s, key: %x", sdl, key)

	if _gvStatistics {
		_gvStat(true)
	}
	return result.value, nil
}

func (yeShMgr *YeShellManager) DhtGetValues(keys [][]byte, out chan<- []byte, timeout time.Duration) error {
	sdl := yeShMgr.dhtSdlName
	if cap(out) < len(keys) {
		log.Warnf("DhtGetValues: mismatched, sdl: %s, cap: %d, len: %d", sdl, cap(out), len(keys))
		close(out)
		return ErrInsufficientOutChanCapacity
	}
	if len(keys) > sch.MaxDhtGetBatchSize {
		log.Warnf("DhtGetValues: batch too big, sdl: %s, len: %d, max: %d",
			sdl, len(keys), sch.MaxDhtGetBatchSize)
		close(out)
		return ErrResourceLimited
	}
	dur := timeout
	if dur <= time.Duration(0) {
		dur = sch.DefaultDhtGetBatchTimeout
	}
	req := sch.MsgDhtMgrGetValueBatchReq {
		Keys: keys,
		ValCh: out,
		Timeout: dur,
	}
	msg := new(sch.SchMessage)
	yeShMgr.dhtInst.SchMakeMessage(msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrGetValueBatchReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(msg); eno != sch.SchEnoNone {
		log.Errorf("DhtGetValues: scheduler failed, sdl: %s, err: %s", sdl, eno.Error())
		close(out)
		return ErrDhtInternal
	}

	return nil
}

func (yeShMgr *YeShellManager) DhtSetValue(key []byte, value []byte) error {
	sdl := yeShMgr.dhtSdlName
	if yeShMgr.inStopping {
		log.Warnf("DhtSetValue: in stopping, sdl: %s", sdl)
		return YesEnoInStopping
	}
	if yeShMgr.ptDhtConMgr.IsBusy(){
		log.Warnf("DhtSetValue: dht busy, sdl: %s", sdl)
		return YesEnoResource
	}
	if len(key) != yesKeyBytes || len(value) == 0 {
		log.Debugf("DhtSetValue: invalid pair or value, sdl: %s, key: %x, value: %x", sdl, key, value)
		return YesEnoParameter
	}

	ch := make(chan bool, 1)
	if err := yeShMgr.dhtPutValMapKey(key, PVTO, ch); err != YesEnoNone {
		log.Debugf("DhtSetValue: dhtPutValMapKey failed, sdl: %s, key: %x, err: %s",
			sdl, key, err.Error())
		return err
	}

	req := sch.MsgDhtMgrPutValueReq{
		Key:      key,
		Val:      value,
		KeepTime: time.Duration(0),
	}
	msg := sch.SchMessage{}
	yeShMgr.dhtInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req)
	if eno := yeShMgr.dhtInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		log.Errorf("DhtSetValue: scheduler failed, sdl: %s, key: %x, err: %s",
			sdl, key, eno.Error())
		yk := yesKey{}
		copy(yk[0:], key)
		yeShMgr.putValLock.Lock()
		delete(yeShMgr.pvk2ChMap, yk)
		delete(yeShMgr.pvk2DurMap, yk)
		yeShMgr.putValLock.Unlock()
		return YesEnoScheduler
	}

	log.Debugf("DhtSetValue: pending, sdl: %s, key: %x", sdl, key)
	result, ok := <-ch
	if !ok {
		log.Debugf("DhtSetValue: timeout, sdl: %s, key: %x", sdl, key)
		return YesEnoTimeout
	}
	if result == false {
		log.Debugf("DhtSetValue: dht failed, sdl: %s, key: %x", sdl, key)
		return YesEnoDhtInteral
	}
	log.Debugf("DhtSetValue: ok, sdl: %s, key: %x", sdl, key)
	return nil
}

func (yeShMgr *YeShellManager) RegChainProvider(cp ChainProvider) {
	yeShMgr.cp = cp
}

func (yeShMgr *YeShellManager) GetChainInfo(kind string, key []byte) ([]byte, error) {
	if key == nil || len(key) > GCIKEY_LEN || len(kind) == 0 {
		yesLog.Debug("GetChainInfo: invalid invalid (kind,key) pair, sdl: %s, kind: %s, key: %x",
			yeShMgr.chainSdlName, kind, key)
		return nil, YesEnoParameter
	}
	kex := getChainInfoKeyEx {
		name: kind,
		keyLen: len(key),
	}
	copy(kex.key[0:], key)

	yeShMgr.gciLock.Lock()
	if _, dup := yeShMgr.gciMap[kex]; dup {
		yesLog.Debug("GetChainInfo: duplicated, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
		yeShMgr.gciLock.Unlock()
		return nil, YesEnoGcdDup
	}
	if len(yeShMgr.gciMap) > GCIBS {
		yesLog.Debug("GetChainInfo: too much, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
		yeShMgr.gciLock.Unlock()
		return nil, YesEnoGcdFull
	}
	vex := getChainInfoValEx{
		gcdChan: make(chan []byte, 1),
		gcdTimer: time.NewTimer(GCITO),
		gcdSeq: uint64(time.Now().UnixNano()),
	}
	yeShMgr.gciMap[kex] = &vex
	yeShMgr.gciLock.Unlock()
	defer vex.gcdTimer.Stop()

	// do not use kex.key[0:] for req.Key, since it's an array than a slice,
	// on which "0"s might have been padded after copy(...) called above.
	req := sch.MsgShellGetChainInfoReq {
		Seq: vex.gcdSeq,
		Kind: kex.name,
		Key: key,
	}
	msg := sch.SchMessage{}
	yeShMgr.chainInst.SchMakeMessage(&msg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellGetChainInfoReq, &req)
	if eno := yeShMgr.chainInst.SchSendMessage(&msg); eno != sch.SchEnoNone {
		yesLog.Debug("GetChainInfo: SchSendMessage failed, sdl: %s, kind: %s, key: %x, eno: %d",
			yeShMgr.chainSdlName, kind, key, eno)
		return nil, YesEnoScheduler
	}

	chainData := ([]byte)(nil)
	gcdOk := false

	_dbgch := make(chan bool, 1)
	go func() {
		_dbgtm := time.NewTimer(GCITO + time.Second * 4)
		defer _dbgtm.Stop()
		for {
			select {
			case <-_dbgtm.C:
				panic("why")
			case <-_dbgch:
				return
			}
		}
	}()


	select {
	case <-vex.gcdTimer.C:
		yeShMgr.gciLock.Lock()
		if _, ok := yeShMgr.gciMap[kex]; ok {
			delete(yeShMgr.gciMap, kex)
			close(vex.gcdChan)
		}
		yeShMgr.gciLock.Unlock()
		yesLog.Debug("GetChainInfo: timeout, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
		close(_dbgch)
		return nil, YesEnoTimeout
	case chainData, gcdOk = <-vex.gcdChan:
		close(_dbgch)
		yesLog.Debug("GetChainInfo: gcdChan got, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
	}

	if !gcdOk{
		yesLog.Debug("GetChainInfo: failed, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
		return nil, YesEnoChClosed
	}
	if  len(chainData) == 0 {
		yesLog.Debug("GetChainInfo: empty, sdl: %s, kind: %s, key: %x", yeShMgr.chainSdlName, kind, key)
		return nil, YesEnoEmptyVal
	}
	yesLog.Debug("GetChainInfo: ok, sdl: %s, kind: %s, key: %x, data: %x",
		yeShMgr.chainSdlName, kind, key, chainData)
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
	_dbgFunc := yesLog.Debug
	if yesLog.dhtCis__ {
		_dbgFunc = p2plog.Debug
	}

	csCh := yeShMgr.dhtCsChan
	csHandler := func(csi *sch.MsgDhtConInstStatusInd) {
		switch csi.Status {

		case dht.CisNull:
			_dbgFunc("dhtCsProc: CisNull, peer: %x", *csi.Peer)

		case dht.CisConnecting:
			_dbgFunc("dhtCsProc: CisConnecting, peer: %x", *csi.Peer)

		case dht.CisConnected:
			_dbgFunc("dhtCsProc: CisConnected, peer: %x", *csi.Peer)

		case dht.CisAccepted:
			_dbgFunc("dhtCsProc: CisAccepted, peer: %x", *csi.Peer)

		case dht.CisInHandshaking:
			_dbgFunc("dhtCsProc: CisInHandshaking, peer: %x", *csi.Peer)

		case dht.CisHandshook:
			_dbgFunc("dhtCsProc: CisHandshook, peer: %x", *csi.Peer)

		case dht.CisInService:
			_dbgFunc("dhtCsProc: CisInService, peer: %x", *csi.Peer)

		case dht.CisOutOfService:
			_dbgFunc("dhtCsProc: CisOutOfService, peer: %x", *csi.Peer)

		case dht.CisClosed:
			_dbgFunc("dhtCsProc: CisClosed, peer: %x", *csi.Peer)

		default:
			_dbgFunc("dhtCsProc: invalid connection status: %d", csi.Status)
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

func (yeShMgr *YeShellManager) chainReady4User() {
	const (
		hSize = 8 // history size, must be (2^n)
		hMask = hSize - 1 // mask for rounding
		hBackSize = 3 // look backward length
		apnTh = 1 // active peer number threshold when looking backward
	)
	if hBackSize > hSize {
		panic("chainReady4User: invalid configuration")
	}

	actHis := [hSize]int{}
	for loop := 0; !yeShMgr.inStopping ;loop++ {
		log.Debugf("chainReady4User: sdl: %s, actHis: %v", yeShMgr.chainSdlName, actHis)
		aps := yeShMgr.ptChainShMgr.GetActivePeerSnapshot()
		idx := loop & hMask
		for _, p := range(*aps) {
			if p.Status == p2psh.PisActive {
				actHis[idx] += 1
			}
		}
		time.Sleep(time.Second)
		idx = 0
		for ; idx < hBackSize; idx++ {
			hisIdx := (loop + hSize - idx) & hMask
			if actHis[hisIdx] < apnTh {
				break
			}
		}
		if idx >= hBackSize {
			log.Infof("chainReady4User: ok, sdl: %s, actHis: %v", yeShMgr.chainSdlName, actHis)
			close(yeShMgr.readyCh)
			break
		}
	}
	log.Infof("chainReady4User: it's over, sdl: %s", yeShMgr.chainSdlName)
}

func (yeShMgr *YeShellManager) chainRxProc() {
	// statistics
	txCount := 0
	evCount := 0
	bhCount := 0
	bkCount := 0
	xxCount := 0

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
				if dup, _ := yeShMgr.checkDupKey(k); dup {
					continue
				}

				msgType := yesMidItoa[pkg.MsgId]
				switch msgType {
				case MessageTypeTx:
					txCount++
				case MessageTypeEvent:
					evCount++
				case MessageTypeBlockHeader:
					bhCount++
				case MessageTypeBlock:
					bkCount++
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
						case MessageTypeBlock:
							err = yeShMgr.broadcastBkOsn(&msg, &exclude)
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
	thisCfg := yeShMgr.config

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

func (yeShMgr *YeShellManager)dhtPutValMapKey(key []byte, to time.Duration, ch chan bool) YesErrno {
	yeShMgr.putValLock.Lock()
	defer yeShMgr.putValLock.Unlock()
	if len(yeShMgr.pvk2ChMap) > PVBS {
		yesLog.Debug("dhtPutValMapKey: too much, max: %d", PVBS)
		return YesEnoPutValFull
	}
	yk := yesKey{}
	if len(key) != yesKeyBytes {
		yesLog.Debug("dhtPutValMapKey: invalid key")
		return YesEnoParameter
	}
	copy(yk[0:], key)
	if _, dup := yeShMgr.pvk2ChMap[yk]; dup {
		yesLog.Debug("dhtPutValMapKey: duplicated")
		return YesEnoPutValDup
	}
	yeShMgr.pvk2ChMap[yk] = ch
	yeShMgr.pvk2DurMap[yk] = to
	return YesEnoNone
}

func (yeShMgr *YeShellManager)dhtPutValProc() {
	sdl := yeShMgr.dhtSdlName
	const period = time.Second
	yk := yesKey{}
	tm := time.NewTicker(period)
	defer tm.Stop()

_pvpLoop:
	for {
		select {
		case <-tm.C:
			yeShMgr.putValLock.Lock()
			for key, dur := range yeShMgr.pvk2DurMap {
				dur = dur - period
				if dur <= 0 {
					close(yeShMgr.pvk2ChMap[key])
					delete(yeShMgr.pvk2ChMap, key)
					delete(yeShMgr.pvk2DurMap, key)
				} else {
					yeShMgr.pvk2DurMap[key] = dur
				}
			}
			yeShMgr.putValLock.Unlock()
		case result, ok := <-yeShMgr.putValChan:
			if !ok {
				break _pvpLoop
			}
			key := result.key
			if len(key) != yesKeyBytes {
				log.Warnf("dhtPutValProc: invalid key, sdl: %s")
			} else {
				log.Debugf("dhtPutValProc: got from channel, sdl: %s, eno: %d, key: %x",
					sdl, result.eno, result.key)
				copy(yk[0:], key)
				yeShMgr.putValLock.Lock()
				if ch, ok := yeShMgr.pvk2ChMap[yk]; ok {
					ch<-result.eno == dht.DhtEnoNone.GetEno()
					close(ch)
					delete(yeShMgr.pvk2ChMap, yk)
					delete(yeShMgr.pvk2DurMap, yk)
				}
				yeShMgr.putValLock.Unlock()
			}
		}
	}

	yeShMgr.putValLock.Lock()
	for k, ch := range yeShMgr.pvk2ChMap {
		close(ch)
		delete(yeShMgr.pvk2ChMap, k)
		delete(yeShMgr.pvk2DurMap, k)
	}
	yeShMgr.putValLock.Unlock()

	log.Warnf("dhtPutValProc: exit")
}

func (yeShMgr *YeShellManager)dhtGetValMapKey(key []byte, to time.Duration, ch chan *getValueResult) YesErrno {
	yeShMgr.getValLock.Lock()
	defer yeShMgr.getValLock.Unlock()
	if len(yeShMgr.gvk2ChMap) > GVBS {
		yesLog.DebugDht("dhtGetValMapKey: too much, max: %d", GVBS)
		return YesEnoGetValFull
	}
	yk := yesKey{}
	if len(key) != yesKeyBytes {
		yesLog.DebugDht("dhtGetValMapKey: invalid key")
		return YesEnoParameter
	}
	copy(yk[0:], key)
	if chList, ok := yeShMgr.gvk2ChMap[yk]; !ok {
		chList = make([]chan *getValueResult, 0, gvkChBufSize)
		chList = append(chList, ch)
		yeShMgr.gvk2ChMap[yk] = chList
		yeShMgr.gvk2DurMap[yk] = to
	} else if len(chList) < cap(chList) {
		yesLog.DebugDht("dhtGetValMapKey: duplicated")
		chList = append(chList, ch)
		yeShMgr.gvk2ChMap[yk] = chList
		return YesEnoGetValDup
	} else {
		yesLog.DebugDht("dhtGetValMapKey: duplicated full")
		return YesEnoGetValDupFull
	}
	return YesEnoNone
}

func (yeShMgr *YeShellManager)dhtGetValProc() {
	sdl := yeShMgr.dhtSdlName
	const period = time.Second
	yk := yesKey{}
	tm := time.NewTicker(period)
	defer tm.Stop()

_gvpLoop:
	for {
		select {
		case <-tm.C:
			yeShMgr.getValLock.Lock()
			for key, dur := range yeShMgr.gvk2DurMap {
				dur = dur - period
				if dur <= 0 {
					log.Debugf("dhtGetValProc: timeout, sdl: %s, key: %x", sdl, key)
					if chList, ok := yeShMgr.gvk2ChMap[key]; ok {
						for _, ch := range chList {
							close(ch)
						}
						delete(yeShMgr.gvk2ChMap, key)
						delete(yeShMgr.gvk2DurMap, key)
					}
				} else {
					yeShMgr.gvk2DurMap[key] = dur
				}
			}
			yeShMgr.getValLock.Unlock()
		case result, ok := <-yeShMgr.getValChan:
			if !ok {
				break _gvpLoop
			}
			key := result.key
			if len(key) != yesKeyBytes {
				log.Warnf("dhtGetValProc: invalid key, sdl: %s", sdl)
			} else {
				copy(yk[0:], key)
				yeShMgr.getValLock.Lock()
				if chList, ok := yeShMgr.gvk2ChMap[yk]; ok {
					log.Debugf("dhtGetValProc: got from channel, sdl: %s, eno: %d, key: %x",
						sdl, result.eno, result.key)
					for _, ch := range chList {
						if result.eno == dht.DhtEnoNone.GetEno() {
							ch <- result
						}
						close(ch)
					}
					delete(yeShMgr.gvk2ChMap, yk)
					delete(yeShMgr.gvk2DurMap, yk)
				}
				yeShMgr.getValLock.Unlock()
			}
		}
	}

	yeShMgr.getValLock.Lock()
	for k, chList := range yeShMgr.gvk2ChMap {
		for _, ch := range chList {
			close(ch)
		}
		delete(yeShMgr.gvk2ChMap, k)
		delete(yeShMgr.gvk2DurMap, k)
	}
	yeShMgr.getValLock.Unlock()

	log.Warnf("dhtGetValProc: exit")
}

func (yeShMgr *YeShellManager) dhtBlindConnectRsp(msg *sch.MsgDhtBlindConnectRsp) sch.SchErrno {
	yesLog.Debug("dhtBlindConnectRsp: msg: %+v", *msg)
	thisCfg := yeShMgr.config

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
	sdl := yeShMgr.dhtSdlName
	yesLog.Debug("dhtMgrPutValueLocalRsp: sdl: %s, msg: %+v", sdl, *msg)
	pvr := putValueResult {
		eno: msg.Eno,
		key: msg.Key,
	}
	yeShMgr.putValChan<-&pvr
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtMgrPutValueRsp(msg *sch.MsgDhtMgrPutValueRsp) sch.SchErrno {
	// see function dhtMgrPutValueLocalRsp for more please, an event EvDhtMgrPutValueLocalRsp should
	// had received before this EvDhtMgrPutValueRsp.
	sdl := yeShMgr.dhtSdlName
	yesLog.Debug("dhtMgrPutValueRsp: sdl: %s, eno: %d, key: %x, peers: %d",
		sdl, msg.Eno, msg.Key, len(msg.Peers))
	return sch.SchEnoNone
}

func (yeShMgr *YeShellManager) dhtMgrGetValueRsp(msg *sch.MsgDhtMgrGetValueRsp) sch.SchErrno {
	yesLog.Debug("dhtMgrGetValueRsp: msg: %+v", *msg)
	gvr := getValueResult{
		eno: msg.Eno,
		key: msg.Key,
		value: msg.Val,
	}
	yeShMgr.getValChan<-&gvr

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
	// broadcast the tx to peers connected currently;
	// need not to throw it into dht;
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if dup, old := yeShMgr.checkDupKey(k); dup {
		log.Warnf("broadcastTxOsn: duplicated, data: %x, old: %x", msg.Data, old)
		return errors.New("broadcastTxOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k, msg.Data); err != nil {
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
	// broadcast the event to peers connected currently;
	// and if needed, throw the event into dht also, determined by "dht" passed in;
	thisCfg := yeShMgr.config
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if dup, old := yeShMgr.checkDupKey(k); dup {
		log.Warnf("broadcastEvOsn: duplicated, data: %x, old: %x", msg.Data, old)
		return errors.New("broadcastEvOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k, msg.Data); err != nil {
		yesLog.Debug("broadcastEvOsn: error: %s", err.Error())
		return err
	}

	req := sch.MsgShellBroadcastReq{
		MsgType: yesMtAtoi[msg.MsgType],
		From:    msg.From,
		Key:     msg.Key,
		Data:    msg.Data,
		Exclude: exclude,
	}
	schMsg := sch.SchMessage{}
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
		schMsg := sch.SchMessage{}
		yeShMgr.dhtInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnDhtShell, sch.EvDhtMgrPutValueReq, &req2Dht)
		if eno := yeShMgr.dhtInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			yesLog.Debug("broadcastEvOsn: SchSendMessage failed, eno: %d", eno)
			return eno
		}
	}

	return nil
}

func (yeShMgr *YeShellManager) broadcastBhOsn(msg *Message, exclude *config.NodeID) error {
	// the Bh should be broadcast over the any-subnet;
	// need not to throw Bh into dht;
	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if dup, old := yeShMgr.checkDupKey(k); dup {
		log.Warnf("broadcastBhOsn: duplicated, data: %x, old: %x", msg.Data, old)
		return errors.New("broadcastBhOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k, msg.Data); err != nil {
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
	// the old design requires that:
	// 		the Bk should be stored by DHT and no broadcasting over any subnet.
	// 		the message here would be dispatched to DHT shell manager.
	// but it's changed to broadcast the blocks to peers instead of DHT now,
	// related functions need to be modified also at the same time, see them
	// for more please.

	k := yesKey{}
	if len(msg.Key) == 0 {
		k = sha256.Sum256(msg.Data)
		msg.Key = append(msg.Key, k[0:]...)
	} else {
		copy(k[0:], msg.Key)
	}

	if dup, old := yeShMgr.checkDupKey(k); dup {
		log.Warnf("broadcastBkOsn: duplicated, data: %x, old: %x", msg.Data, old)
		return errors.New("broadcastBkOsn: duplicated")
	}

	if err := yeShMgr.setDedupTimer(k, msg.Data); err != nil {
		yesLog.Debug("broadcastBkOsn: error: %s", err.Error())
		return err
	}

	// keep statements for old design, switch by a const, see comments above pls.
	// would remove unnecessary statements later.
	const bk2dht = false
	schMsg := sch.SchMessage{}
	if bk2dht {
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
	} else {
		req := sch.MsgShellBroadcastReq{
			MsgType: yesMtAtoi[msg.MsgType],
			From:    msg.From,
			Key:     msg.Key,
			Data:    msg.Data,
			Exclude: exclude,
		}
		yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell, sch.EvShellBroadcastReq, &req)
		if eno := yeShMgr.chainInst.SchSendMessage(&schMsg); eno != sch.SchEnoNone {
			yesLog.Debug("broadcastBkOsn: SchSendMessage failed, eno: %d", eno)
			return eno
		}
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

func (yeShMgr *YeShellManager) setDedupTimer(key yesKey, data []byte) error {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()

	thisCfg := yeShMgr.config

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

	yeShMgr.deDupMap[key] = data
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

func (yeShMgr *YeShellManager) checkDupKey(k yesKey) (bool, []byte) {
	yeShMgr.deDupLock.Lock()
	defer yeShMgr.deDupLock.Unlock()
	val, dup := yeShMgr.deDupMap[k]
	return dup, val
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

		yesLog.Debug("getChainDataFromPeer: cp: sdl: %s, kind: %s, key: %x, data: %x",
			yeShMgr.chainSdlName, msg.Gcd.Name, msg.Gcd.Key, data)

		if len(data) > 0 {
			rsp := sch.MsgShellGetChainInfoRsp {
				Peer: rxPkg.PeerInfo,
				Seq: msg.Gcd.Seq,
				Kind: msg.Gcd.Name,
				Key: msg.Gcd.Key,
				Data: data,
			}
			schMsg := sch.SchMessage{}
			yeShMgr.chainInst.SchMakeMessage(&schMsg, &sch.PseudoSchTsk, yeShMgr.ptnChainShell,
				sch.EvShellGetChainInfoRsp, &rsp)
			yeShMgr.chainInst.SchSendMessage(&schMsg)
			return sch.SchEnoNone
		}
	}
	return sch.SchEnoResource
}

func (yeShMgr *YeShellManager) putChainDataFromPeer(rxPkg *peer.P2pPackageRx) sch.SchErrno {
	yeShMgr.gciLock.Lock()
	defer yeShMgr.gciLock.Unlock()

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

	kex := getChainInfoKeyEx{
		name: msg.Pcd.Name,
		keyLen: len(msg.Pcd.Key),
	}
	copy(kex.key[0:], msg.Pcd.Key)

	vex, ok := yeShMgr.gciMap[kex]
	if !ok {
		yesLog.Debug("putChainDataFromPeer: not found, name: %s, key: %x", kex.key, kex.name)
		return sch.SchEnoNotFound
	}
	if vex.gcdSeq != msg.Pcd.Seq {
		yesLog.Debug("putChainDataFromPeer: sequence mismatch")
		return sch.SchEnoMismatched
	}

	// notice: when all matched, we should delete the key from the map at once
	// to discard the possible responses with the same key value, or in the worst
	// case, deadlock can happen. see function GetChainInfo also please.
	delete(yeShMgr.gciMap, kex)
	if vex.gcdChan != nil {
		vex.gcdChan <- msg.Pcd.Data
		return sch.SchEnoNone
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

