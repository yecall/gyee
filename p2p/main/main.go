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


package main


import (
	"os"
	"time"
	"fmt"
	"net"
	"sync"
	"math/rand"
	"os/signal"
	"bytes"
	"errors"
	golog	"log"
	"crypto/sha256"
	shell	"github.com/yeeco/gyee/p2p/shell"
	peer	"github.com/yeeco/gyee/p2p/peer"
	config	"github.com/yeeco/gyee/p2p/config"
	log		"github.com/yeeco/gyee/p2p/logger"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
	dht		"github.com/yeeco/gyee/p2p/dht"
)

//
// Configuration pointer
//
var p2pName2Cfg = make(map[string]*config.Config)
var p2pInst2Cfg = make(map[*sch.Scheduler]*config.Config)

var dhtName2Cfg = make(map[string]*config.Config)
var dhtInst2Cfg = make(map[*sch.Scheduler]*config.Config)

//
// Indication/Package handlers
//
var (
	p2pIndHandler peer.P2pIndCallback = p2pIndProc
	p2pPkgHandler peer.P2pPkgCallback = p2pPkgProc
)

//
// extend peer identity
//
type peerIdEx struct {
	subNetId	peer.SubNetworkID
	nodeId		peer.PeerId
	dir			int
}

//
// test statistics
//
type testCaseCtrlBlock struct {
	lock		sync.Mutex							// lock to sync action on tcbList
	tcbList		map[peerIdEx]*testCaseCtrlBlock		// test case control block list
	peerId		peerIdEx							// peer extend identity
	done 		chan bool							// done channel
	peerActInd	chan interface{}					// indication channel for peer activation
	peerClsInd	chan interface{}					// indication channel for peer closed
	txSeq		int64								// tx sequence number
	rxSeq		int64								// rx sequence number
	killing		bool								// is currently in killing
}

//
// Callback user data
//
type cbUserData struct {
	lock		sync.Mutex							// lock to sync action on tcbList
	tcbList		map[peerIdEx]*testCaseCtrlBlock		// test case control block list
}

//
// Done signal for Tx routines
//
var tcbListsLock sync.Mutex
var tcbLists map[*sch.Scheduler]map[peerIdEx]*testCaseCtrlBlock
var tcbLocks map[*sch.Scheduler]sync.Mutex

//
// test case
//
type testCase struct {
	name		string
	description	string
	entry		func(tc *testCase)
}

//
// test case table
//
var testCaseTable = []testCase{
	{
		name:			"testCase0",
		description:	"common case for AnySubNet",
		entry:			testCase0,
	},
	{
		name:			"testCase1",
		description:	"multiple p2p instances",
		entry:			testCase1,
	},
	{
		name:			"testCase2",
		description:	"static network without any dynamic sub networks",
		entry:			testCase2,
	},
	{
		name:			"testCase3",
		description:	"multiple sub networks without a static network",
		entry:			testCase3,
	},
	{
		name:			"testCase4",
		description:	"multiple sub networks with a static network",
		entry:			testCase4,
	},
	{
		name:			"testCase5",
		description:	"stop p2p instance",
		entry:			testCase5,
	},
	{
		name:			"testCase6",
		description:	"dht common test",
		entry:			testCase6,
	},
	{
		name:			"testCase7",
		description:	"stop dht instance",
		entry:			testCase7,
	},
}

//
// target case
//
var tgtCase = "testCase5"

//
// port base
//
const (
	portBase4P2p = 30303
	portBase4Dht = 40404
)

func newTcb(name string, idEx peerIdEx, tcbList map[peerIdEx]*testCaseCtrlBlock, lock sync.Mutex) *testCaseCtrlBlock {
	tcb := testCaseCtrlBlock {
		lock: lock,
		tcbList: tcbList,
		peerId: idEx,
		done: make(chan bool),
		peerActInd: make(chan interface{}, 128),
		peerClsInd: make(chan interface{}, 128),
		txSeq:	0,
		rxSeq:	0,
	}
	switch name {
	case "testCase0":
	case "testCase1":
	case "testCase2":
	case "testCase3":
	case "testCase4":
	case "testCase5":
	case "testCase6":
	case "testCase7":
	default:
		log.Debug("newTcb: undefined test: %s", name)
		return nil
	}
	return &tcb
}

func injectActPeer(tcbList map[peerIdEx]*testCaseCtrlBlock, acter *peerIdEx, lock sync.Mutex) {
	lock.Lock()
	defer lock.Unlock()
	for id, tcb := range tcbList {
		if id == *acter {
			log.Debug("injectActPeer: duplicated")
		}
		tcb.peerActInd<-acter
	}
}

func injectActPeers2Tcb(tcbList map[peerIdEx]*testCaseCtrlBlock, target *testCaseCtrlBlock, lock sync.Mutex) {
	lock.Lock()
	defer lock.Unlock()
	for id, _ := range tcbList {
		target.peerActInd<-&id
	}
}

func injectClsPeer(tcbList map[peerIdEx]*testCaseCtrlBlock, clser *peerIdEx, lock sync.Mutex) {
	lock.Lock()
	defer lock.Unlock()
	for id, tcb := range tcbList {
		if id != *clser {
			tcb.peerClsInd<-clser
		}
	}
}

var dataTxApply = true
var dataTxTmApply = true

func txrxStopAll() {
	tcbListsLock.Lock()
	defer tcbListsLock.Unlock()
	instKeys := make([]*sch.Scheduler, 0)
	for ik, tl := range tcbLists {
		instKeys = append(instKeys, ik)
		tcbKeys := make([]peerIdEx, 0)
		for tk, tcb := range tl {
			if !tcb.killing {
				tcb.killing = true
				tcb.done <- true
				<-tcb.done
				tcbKeys = append(tcbKeys, tk)
			}
		}
		for _, tk := range tcbKeys {
			delete(tl, tk)
		}
	}
	for _, ik := range instKeys {
		delete(tcbLists, ik)
	}
}

func isTxRxAllDone() bool {
	tcbListsLock.Lock()
	defer tcbListsLock.Unlock()
	return len(tcbLists) == 0
}

func createTcbLists(instList []*sch.Scheduler) {
	if tcbLists != nil {
		log.Debug("createTcbLists: duplicated")
		return
	}
	tcbLists = make(map[*sch.Scheduler]map[peerIdEx]*testCaseCtrlBlock, len(instList))
	tcbLocks = make(map[*sch.Scheduler]sync.Mutex, len(instList))
	for _, inst := range instList {
		tcbLists[inst] = make(map[peerIdEx]*testCaseCtrlBlock, 0)
		tcbLocks[inst] = sync.Mutex{}
	}
}

func appendTcb2List(instList []*sch.Scheduler) {
	if tcbLists == nil {
		tcbLists = make(map[*sch.Scheduler]map[peerIdEx]*testCaseCtrlBlock, len(instList))
		tcbLocks = make(map[*sch.Scheduler]sync.Mutex, len(instList))
	}
	for _, inst := range instList {
		if tcbLists[inst] != nil {
			log.Debug("appendTcb2List: duplicated")
			continue
		}
		tcbLists[inst] = make(map[peerIdEx]*testCaseCtrlBlock, 0)
		tcbLocks[inst] = sync.Mutex{}
	}
}

func txrxProc(p2pInst *sch.Scheduler, tcb *testCaseCtrlBlock, rxChan chan *peer.P2pPackageRx) {
	sdl := p2pInst.SchGetP2pCfgName()
	pkg := peer.P2pPackage2Peer {
		P2pInst:		p2pInst,
		IdList: 		make([]peer.PeerId, 0),
		ProtoId:		int(peer.PID_EXT),
		PayloadLength:	0,
		Payload:		make([]byte, 0, 512),
		Extra:			nil,
	}
	var peerList = make(map[peerIdEx]interface{}, 0)

	var setupPkg = func(id *peerIdEx) {
		txString := fmt.Sprintf(">>>>>> \nseq:%d\n"+
			"to: subnet: %s\n, id: %s\n",
			tcb.txSeq,
			fmt.Sprintf("%x", tcb.peerId.subNetId),
			fmt.Sprintf("%x", id))
		pkg.SubNetId = id.subNetId
		pkg.IdList = make([]peer.PeerId, 0)
		pkg.IdList = append(pkg.IdList, id.nodeId)
		pkg.Payload = []byte(txString)
		pkg.PayloadLength = len(pkg.Payload)
	}

	var tmHandler = func() {
		tcb.txSeq++
		if dataTxApply {
			for id := range peerList {
				setupPkg(&id)
				if eno := shell.P2pSendPackage(&pkg); eno != shell.P2pEnoNone {
					log.Debug("txrxProc: P2pSendPackage failed, eno: %d, sdl: %s, dir: %d, subnet: %s, id: %s",
						eno, sdl, id.dir,
						fmt.Sprintf("%x", id.subNetId),
						fmt.Sprintf("%x", id.nodeId))
				}
			}
		}
	}

	tm := time.NewTicker(time.Second * 1)
	defer tm.Stop()

	doneOk := false
	done4Done := false
	done4RxChan := false

txrxLoop:
	for {
		select {
		case ind := <-tcb.peerActInd:
			if peerActivated, ok := ind.(*peerIdEx); ok {
				peerList[*peerActivated] = nil
			}
		case ind := <-tcb.peerClsInd:
			if peerClosed, ok := ind.(*peerIdEx); ok {
				delete(peerList, *peerClosed)
			}
		case _, doneOk = <-tcb.done:
			log.Debug("txrxProc: it's done, sdl: %s, dir: %d, subnet: %s, id: %s",
				sdl, tcb.peerId.dir,
				fmt.Sprintf("%x", tcb.peerId.subNetId),
				fmt.Sprintf("%x", tcb.peerId.nodeId))
			done4Done = true
			break txrxLoop
		case <-tm.C:
			if dataTxTmApply {
				tmHandler()
			}
		case _, ok := <-rxChan:
			if !ok {
				log.Debug("txrxProc: rxChan closed, break loop, sdl: %s, dir: %d, subnet: %s, id: %s",
					sdl, tcb.peerId.dir,
					fmt.Sprintf("%x", tcb.peerId.subNetId),
					fmt.Sprintf("%x", tcb.peerId.nodeId))
				done4RxChan = true
				break txrxLoop
			}
			tcb.rxSeq += 1
			if tcb.rxSeq & 0x0f == 0 {
				log.Debug("txrxProc: rxSeq: %d, sdl: %s, dir: %d, subnet: %s, id: %s",
					tcb.rxSeq, sdl, tcb.peerId.dir,
					fmt.Sprintf("%x", tcb.peerId.subNetId),
					fmt.Sprintf("%x", tcb.peerId.nodeId))
			}
		}
	}

	if done4Done && doneOk {
		close(tcb.done)
	} else if done4RxChan {
		_, doneOk := <-tcb.done
		if doneOk {
			close(tcb.done)
		}
	} else {
		log.Debug("txrxProc: impossible loop broken")
	}

	log.Debug("txrxProc: exit, sdl: %s, dir: %d, subnet: %s, id: %s",
		sdl, tcb.peerId.dir,
		fmt.Sprintf("%x", tcb.peerId.subNetId),
		fmt.Sprintf("%x", tcb.peerId.nodeId))
}

func p2pIndProc(what int, para interface{}, userData interface{}) interface{} {
	switch what {
	case shell.P2pIndPeerActivated:
		pap := para.(*peer.P2pIndPeerActivatedPara)
		p2pInst := sch.SchGetScheduler(pap.Ptn)
		sdl := p2pInst.SchGetP2pCfgName()
		snid := pap.PeerInfo.Snid
		peerId := pap.PeerInfo.NodeId
		dir := pap.PeerInfo.Dir

		idEx := peerIdEx {
			subNetId:	snid,
			nodeId:		peerId,
			dir:		dir,
		}

		cud, ok := userData.(*cbUserData)
		if !ok {
			log.Debug("p2pIndProc: invalid user data, sdl: %s, dir: %d, snid: %x, peer: %x",
				sdl, dir, snid, peerId)
			return nil
		}
		tcbList := cud.tcbList
		if _, dup := tcbList[idEx]; dup {
			log.Debug("p2pIndProc: duplicated, sdl: %s, dir: %d, snid: %x, peer: %x",
				sdl, dir, snid, peerId)
			return nil
		}

		injectActPeer(tcbList, &idEx, cud.lock)
		tcb := newTcb(tgtCase, idEx, tcbList, cud.lock)
		injectActPeers2Tcb(tcbList, tcb, cud.lock)
		tcbList[idEx] = tcb

		log.Debug("p2pIndProc: start tx/rx, sdl: %s, dir: %d, snid: %x, peer: %x",
			sdl, dir, snid, peerId)

		go txrxProc(p2pInst, tcb, pap.RxChan)

	case shell.P2pIndPeerClosed:
		pcp := para.(*peer.P2pIndPeerClosedPara)
		p2pInst := sch.SchGetScheduler(pcp.Ptn)
		sdl := p2pInst.SchGetP2pCfgName()

		idEx := peerIdEx {
			subNetId: pcp.Snid,
			nodeId: pcp.PeerId,
			dir: pcp.Dir,
		}

		cud, ok := userData.(*cbUserData)
		if !ok {
			log.Debug("p2pIndProc: invalid user data, sdl: %s, dir: %d, snid: %x, peer: %x",
				sdl, idEx.dir, idEx.subNetId, idEx.nodeId)
			return nil
		}
		tcbList := cud.tcbList
		injectClsPeer(tcbList, &idEx, cud.lock)

		tcbListsLock.Lock()
		need2Kill := false
		tcb, exist := tcbList[idEx]
		if exist {
			if need2Kill = !tcb.killing; need2Kill {
				tcb.killing = true
			}
		}
		tcbListsLock.Unlock()

		if !need2Kill {
			log.Debug("p2pIndProc: already in killing, sdl: %s, dir: %d, snid: %x, peer: %x",
				sdl, idEx.dir, idEx.subNetId, idEx.nodeId)
		} else {
			log.Debug("p2pIndProc: try to kill, sdl: %s, dir: %d, snid: %x, peer: %x",
				sdl, idEx.dir, idEx.subNetId, idEx.nodeId)
			tcb.done<-true
			<-tcb.done
			tcbListsLock.Lock()
			delete(tcbList, idEx)
			tcbListsLock.Unlock()
		}

	default:
		log.Debug("p2pIndProc: inknown indication: %d", what)
	}
	return para
}

func p2pPkgProc(pkg *peer.P2pPackageRx, userData interface{}) interface{} {
	return nil
}

func waitInterrupt() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
}



//
// run target case
//
func main() {
	for _, tc := range testCaseTable {
		if tc.name == tgtCase {
			tc.entry(&tc)
			return
		}
	}
	log.Debug("main: target case not found: %s", tgtCase)
}

//
// testCase0
//
func testCase0(tc *testCase) {

	log.Debug("testCase0: going to start ycp2p...")

	// fetch default from underlying
	dftCfg := shell.ShellDefaultConfig()
	if dftCfg == nil {
		log.Debug("testCase0: ShellDefaultConfig failed")
		return
	}

	// one can then apply his configurations based on the default by calling
	// ShellSetConfig with a defferent configuration if he likes to. notice
	// that a configuration name also returned.
	myCfg := *dftCfg
	myCfg.AppType = config.P2P_TYPE_CHAIN
	cfgName := "p2pInst0"
	cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
	p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

	// init underlying p2p logic, an instance of p2p returned
	p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
	if eno != sch.SchEnoNone {
		log.Debug("testCase0: SchSchedulerInit failed, eno: %d", eno)
		return
	}
	p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

	// create test cast control block list
	createTcbLists([]*sch.Scheduler{p2pInst})

	// start p2p instance
	if eno = shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
		log.Debug("testCase0: P2pStart failed, eno: %d", eno)
		return
	}

	// register indication handler. notice that please, the indication handler is a
	// global object for all peers connected, while the incoming packages callback
	// handler is owned by every peer, and it can be installed while activation of
	// a peer is indicated. See demo indication handler p2pIndHandler and incoming
	// package handler p2pPkgHandler for more please.
	cud := cbUserData{
		lock: tcbLocks[p2pInst],
		tcbList: tcbLists[p2pInst],
	}
	if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
	eno != shell.P2pEnoNone {
		log.Debug("testCase0: P2pRegisterCallback failed, eno: %d", eno)
		return
	}
	log.Debug("testCase0: ycp2p started, cofig: %s", cfgName)

	// wait to be interrupted
	waitInterrupt()
}

//
// testCase1
//
func testCase1(tc *testCase) {
	log.Debug("testCase1: going to start ycp2p ...")

	var p2pInstNum = 16
	var bootstrapIp net.IP
	var bootstrapId = ""
	var bootstrapUdp uint16 = 0
	var bootstrapTcp uint16 = 0
	var bootstrapNodes = []*config.Node{}
	for loop := 0; loop < p2pInstNum; loop++ {
		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase1: handling configuration:%s ...", cfgName)
		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.Debug("testCase1: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.Local.UDP = uint16(portBase4P2p + loop)
		myCfg.Local.TCP = uint16(portBase4P2p + loop)
		if loop == 0 {
			myCfg.NoDial = true
			myCfg.BootstrapNode = true
		}

		myCfg.BootstrapNodes = nil
		if loop != 0 {
			myCfg.BootstrapNodes = append(myCfg.BootstrapNodes, bootstrapNodes...)
		}

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		if loop == 0 {
			bootstrapIp = p2pName2Cfg[cfgName].Local.IP
			bootstrapId = fmt.Sprintf("%x", p2pName2Cfg[cfgName].Local.ID)
			bootstrapUdp = p2pName2Cfg[cfgName].Local.UDP
			bootstrapTcp = p2pName2Cfg[cfgName].Local.TCP

			ipv4 := bootstrapIp.To4()
			url := []string {
				fmt.Sprintf("%s@%d.%d.%d.%d:%d:%d",
					bootstrapId,
					ipv4[0],ipv4[1],ipv4[2],ipv4[3],
					bootstrapUdp,
					bootstrapTcp),
			}
			bootstrapNodes = append(bootstrapNodes,config.P2pSetupBootstrapNodes(url)...)
		}

		p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase1: SchSchedulerInit failed, eno: %d", eno)
			return
		}
		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

		appendTcb2List([]*sch.Scheduler{p2pInst})

		if eno = shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.Debug("testCase1: P2pStart failed, eno: %d", eno)
			return
		}

		cud := cbUserData{
			lock: tcbLocks[p2pInst],
			tcbList: tcbLists[p2pInst],
		}
		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
			eno != shell.P2pEnoNone {
			log.Debug("testCase1: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		log.Debug("testCase1: ycp2p started, cofig: %s", cfgName)
	}

	waitInterrupt()
}

//
// testCase2
//
func testCase2(tc *testCase) {

	log.Debug("testCase2: going to start ycp2p ...")

	var p2pInstNum = 8
	var cfgName = ""

	var staticNodeIdList = []*config.Node{}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName = fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase2: prepare node identity: %s ...", cfgName)

		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.Debug("testCase2: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil

		if config.P2pSetupLocalNodeId(&myCfg) != config.PcfgEnoNone {
			log.Debug("testCase2: P2pSetupLocalNodeId failed")
			return
		}

		log.Debug("testCase2: cfgName: %s, id: %x",
			cfgName, myCfg.Local.ID)

		n := config.Node{
			UDP:	uint16(portBase4P2p + loop),
			TCP:	uint16(portBase4P2p + loop),
			ID:		myCfg.Local.ID,
		}

		staticNodeIdList = append(staticNodeIdList, &n)
	}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase2: handling configuration:%s ...", cfgName)

		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.Debug("testCase2: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.NetworkType = config.P2pNetworkTypeStatic
		myCfg.StaticNetId = config.ZeroSubNet
		myCfg.Local = *staticNodeIdList[loop]

		for idx, n := range staticNodeIdList {
			if idx != loop {
				myCfg.StaticNodes = append(myCfg.StaticNodes, n)
			}
		}

		myCfg.StaticMaxPeers = len(myCfg.StaticNodes) * 2
		myCfg.StaticMaxOutbounds = len(myCfg.StaticNodes)
		myCfg.StaticMaxInbounds = len(myCfg.StaticNodes)

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase2: SchSchedulerInit failed, eno: %d", eno)
			return
		}

		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]
	}

	var p2pInstList = []*sch.Scheduler{}
	for p2pInst := range p2pInst2Cfg {
		p2pInstList = append(p2pInstList, p2pInst)
	}

	for piNum := len(p2pInstList); piNum > 0; piNum-- {

		time.Sleep(time.Second * 2)

		pidx := rand.Intn(piNum)
		p2pInst := p2pInstList[pidx]
		p2pInstList = append(p2pInstList[0:pidx], p2pInstList[pidx+1:]...)

		appendTcb2List([]*sch.Scheduler{p2pInst})

		if eno := shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.Debug("testCase2: P2pStart failed, eno: %d", eno)
			return
		}

		cud := cbUserData{
			lock: tcbLocks[p2pInst],
			tcbList: tcbLists[p2pInst],
		}
		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
			eno != shell.P2pEnoNone {
			log.Debug("testCase2: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		log.Debug("testCase2: ycp2p started, cofig: %s", cfgName)
	}

	waitInterrupt()
}

//
// testCase3
//
func testCase3(tc *testCase) {

	log.Debug("testCase3: going to start ycp2p ...")

	var p2pInstNum = 16

	var bootstrapIp net.IP
	var bootstrapId = ""
	var bootstrapUdp uint16 = 0
	var bootstrapTcp uint16 = 0
	var bootstrapNodes = []*config.Node{}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase3: handling configuration:%s ...", cfgName)

		var dftCfg *config.Config = nil

		if loop == 0 {
			if dftCfg = shell.ShellDefaultBootstrapConfig(); dftCfg == nil {
				log.Debug("testCase3: ShellDefaultBootstrapConfig failed")
				return
			}
		} else {
			if dftCfg = shell.ShellDefaultConfig(); dftCfg == nil {
				log.Debug("testCase3: ShellDefaultConfig failed")
				return
			}
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.NetworkType = config.P2pNetworkTypeDynamic
		myCfg.Local.UDP = uint16(portBase4P2p + loop)
		myCfg.Local.TCP = uint16(portBase4P2p + loop)

		if loop == 0 {
			for idx := 0; idx < p2pInstNum; idx++ {
				snid0 := config.SubNetworkID{0xff, byte(idx & 0x0f)}
				myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
				myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
				myCfg.SubNetMaxInBounds[snid0] = config.MaxPeers
				myCfg.SubNetMaxOutbounds[snid0] = 0
			}
		} else {
			snid0 := config.SubNetworkID{0xff, byte(loop & 0x0f)}
			snid1 := config.SubNetworkID{0xff, byte((loop + 1) & 0x0f)}
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid1)
			myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid0] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid0] = config.MaxOutbounds
			myCfg.SubNetMaxPeers[snid1] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid1] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid1] = config.MaxOutbounds
		}

		if loop == 0 {
			myCfg.NoDial = true
			myCfg.NoAccept = true
			myCfg.BootstrapNode = true
		} else {
			myCfg.NoDial = false
			myCfg.NoAccept = false
			myCfg.BootstrapNode = false
		}

		myCfg.BootstrapNodes = nil
		if loop != 0 {
			myCfg.BootstrapNodes = append(myCfg.BootstrapNodes, bootstrapNodes...)
		}

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		if loop == 0 {

			bootstrapIp = p2pName2Cfg[cfgName].Local.IP
			bootstrapId = fmt.Sprintf("%x", p2pName2Cfg[cfgName].Local.ID)
			bootstrapUdp = p2pName2Cfg[cfgName].Local.UDP
			bootstrapTcp = p2pName2Cfg[cfgName].Local.TCP

			ipv4 := bootstrapIp.To4()
			url := []string {
				fmt.Sprintf("%s@%d.%d.%d.%d:%d:%d",
					bootstrapId,
					ipv4[0],ipv4[1],ipv4[2],ipv4[3],
					bootstrapUdp,
					bootstrapTcp),
			}
			bootstrapNodes = append(bootstrapNodes,config.P2pSetupBootstrapNodes(url)...)
		}

		p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase3: SchSchedulerInit failed, eno: %d", eno)
			return
		}
		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

		appendTcb2List([]*sch.Scheduler{p2pInst})

		if eno = shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.Debug("testCase3: P2pStart failed, eno: %d", eno)
			return
		}

		cud := cbUserData{
			lock: tcbLocks[p2pInst],
			tcbList: tcbLists[p2pInst],
		}
		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
			eno != shell.P2pEnoNone {
			log.Debug("testCase3: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		log.Debug("testCase3: ycp2p started, cofig: %s", cfgName)
	}

	waitInterrupt()
}

//
// testCase4
//
func testCase4(tc *testCase) {

	log.Debug("testCase4: going to start ycp2p ...")

	var p2pInstNum = 8

	var bootstrapIp net.IP
	var bootstrapId = ""
	var bootstrapUdp uint16 = 0
	var bootstrapTcp uint16 = 0
	var bootstrapNodes = []*config.Node{}
	var p2pInstBootstrap *sch.Scheduler = nil

	var staticNodeIdList = []*config.Node{}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase4: prepare node identity: %s ...", cfgName)

		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.Debug("testCase4: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil

		if config.P2pSetupLocalNodeId(&myCfg) != config.PcfgEnoNone {
			log.Debug("testCase4: P2pSetupLocalNodeId failed")
			return
		}

		log.Debug("testCase4: cfgName: %s, id: %x",
			cfgName, myCfg.Local.ID)

		n := config.Node{
			UDP:	uint16(portBase4P2p + loop),
			TCP:	uint16(portBase4P2p + loop),
			ID:		myCfg.Local.ID,
		}

		staticNodeIdList = append(staticNodeIdList, &n)
	}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase4: handling configuration:%s ...", cfgName)

		var dftCfg *config.Config = nil

		if loop == 0 {
			if dftCfg = shell.ShellDefaultBootstrapConfig(); dftCfg == nil {
				log.Debug("testCase4: ShellDefaultBootstrapConfig failed")
				return
			}
		} else {
			if dftCfg = shell.ShellDefaultConfig(); dftCfg == nil {
				log.Debug("testCase4: ShellDefaultConfig failed")
				return
			}
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.NetworkType = config.P2pNetworkTypeDynamic
		myCfg.StaticNetId = config.ZeroSubNet
		myCfg.Local.UDP = uint16(portBase4P2p + loop)
		myCfg.Local.TCP = uint16(portBase4P2p + loop)
		myCfg.Local.ID = (*staticNodeIdList[loop]).ID

		for idx, n := range staticNodeIdList {
			if idx != loop {
				myCfg.StaticNodes = append(myCfg.StaticNodes, n)
			}
		}

		myCfg.StaticMaxPeers = len(myCfg.StaticNodes) * 2	// config.MaxPeers
		myCfg.StaticMaxOutbounds = len(myCfg.StaticNodes)	// config.MaxOutbounds
		myCfg.StaticMaxInbounds = len(myCfg.StaticNodes) 	// config.MaxInbounds

		if loop == 0 {
			for idx := 0; idx < p2pInstNum; idx++ {
				snid0 := config.SubNetworkID{0xff, byte(idx & 0x0f)}
				myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
				myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
				myCfg.SubNetMaxInBounds[snid0] = config.MaxPeers
				myCfg.SubNetMaxOutbounds[snid0] = 0
			}
		} else {
			snid0 := config.SubNetworkID{0xff, byte(loop & 0x0f)}
			snid1 := config.SubNetworkID{0xff, byte((loop + 1) & 0x0f)}
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid1)
			myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid0] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid0] = config.MaxOutbounds
			myCfg.SubNetMaxPeers[snid1] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid1] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid1] = config.MaxOutbounds
		}

		if loop == 0 {
			myCfg.NoDial = true
			myCfg.NoAccept = true
			myCfg.BootstrapNode = true
		} else {
			myCfg.NoDial = false
			myCfg.NoAccept = false
			myCfg.BootstrapNode = false
		}

		myCfg.BootstrapNodes = nil
		if loop != 0 {
			myCfg.BootstrapNodes = append(myCfg.BootstrapNodes, bootstrapNodes...)
		}

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		if loop == 0 {

			bootstrapIp = append(bootstrapIp, p2pName2Cfg[cfgName].Local.IP[:]...)
			bootstrapId = fmt.Sprintf("%x", p2pName2Cfg[cfgName].Local.ID)
			bootstrapUdp = p2pName2Cfg[cfgName].Local.UDP
			bootstrapTcp = p2pName2Cfg[cfgName].Local.TCP

			ipv4 := bootstrapIp.To4()
			url := []string{
				fmt.Sprintf("%s@%d.%d.%d.%d:%d:%d",
					bootstrapId,
					ipv4[0], ipv4[1], ipv4[2], ipv4[3],
					bootstrapUdp,
					bootstrapTcp),
			}
			bootstrapNodes = append(bootstrapNodes, config.P2pSetupBootstrapNodes(url)...)
		}

		p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase4: SchSchedulerInit failed, eno: %d", eno)
			return
		}
		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

		if loop == 0 {
			p2pInstBootstrap = p2pInst
		}
	}

	var p2pInstList = []*sch.Scheduler{}
	for p2pInst := range p2pInst2Cfg {
		if p2pInst != p2pInstBootstrap {
			p2pInstList = append(p2pInstList, p2pInst)
		}
	}

	if eno := shell.P2pStart(p2pInstBootstrap); eno != sch.SchEnoNone {
		log.Debug("testCase4: P2pStart failed, eno: %d", eno)
		return
	}

	for piNum := len(p2pInstList); piNum > 0; piNum-- {

		time.Sleep(time.Second * 2)

		pidx := rand.Intn(piNum)
		p2pInst := p2pInstList[pidx]
		p2pInstList = append(p2pInstList[0:pidx], p2pInstList[pidx+1:]...)

		appendTcb2List([]*sch.Scheduler{p2pInst})

		if eno := shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.Debug("testCase4: P2pStart failed, eno: %d", eno)
			return
		}

		cud := cbUserData{
			lock: tcbLocks[p2pInst],
			tcbList: tcbLists[p2pInst],
		}
		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
			eno != shell.P2pEnoNone {
			log.Debug("testCase4: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		cfgName := p2pInst.SchGetP2pCfgName()
		log.Debug("testCase4: ycp2p started, cofig: %s", cfgName)
	}

	waitInterrupt()
}

//
// testCase5
//
func testCase5(tc *testCase) {

	log.Debug("testCase5: going to start ycp2p ...")

	var p2pInstNum = 16

	var bootstrapIp net.IP
	var bootstrapId = ""
	var bootstrapUdp uint16 = 0
	var bootstrapTcp uint16 = 0
	var bootstrapNodes = []*config.Node{}
	var p2pInstBootstrap *sch.Scheduler = nil

	var staticNodeIdList = []*config.Node{}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase5: prepare node identity: %s ...", cfgName)

		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.Debug("testCase5: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil

		if config.P2pSetupLocalNodeId(&myCfg) != config.PcfgEnoNone {
			log.Debug("testCase5: P2pSetupLocalNodeId failed")
			return
		}

		log.Debug("testCase5: cfgName: %s, id: %x",
			cfgName, myCfg.Local.ID)

		n := config.Node{
			IP:		dftCfg.Local.IP,
			UDP:	uint16(portBase4P2p + loop),
			TCP:	uint16(portBase4P2p + loop),
			ID:		myCfg.Local.ID,
		}

		staticNodeIdList = append(staticNodeIdList, &n)
	}

	for loop := 0; loop < p2pInstNum; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.Debug("testCase5: handling configuration:%s ...", cfgName)

		var dftCfg *config.Config = nil

		if loop == 0 {
			if dftCfg = shell.ShellDefaultBootstrapConfig(); dftCfg == nil {
				log.Debug("testCase5: ShellDefaultBootstrapConfig failed")
				return
			}
		} else {
			if dftCfg = shell.ShellDefaultConfig(); dftCfg == nil {
				log.Debug("testCase5: ShellDefaultConfig failed")
				return
			}
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_CHAIN
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.NetworkType = config.P2pNetworkTypeDynamic
		myCfg.StaticNetId = config.ZeroSubNet
		myCfg.Local.UDP = uint16(portBase4P2p + loop)
		myCfg.Local.TCP = uint16(portBase4P2p + loop)
		myCfg.Local.ID = (*staticNodeIdList[loop]).ID

		for idx, n := range staticNodeIdList {
			if idx != loop {
				myCfg.StaticNodes = append(myCfg.StaticNodes, n)
			}
		}

		myCfg.StaticMaxPeers = len(myCfg.StaticNodes) * 2	// config.MaxPeers
		myCfg.StaticMaxOutbounds = len(myCfg.StaticNodes)	// config.MaxOutbounds
		myCfg.StaticMaxInbounds = len(myCfg.StaticNodes) 	// config.MaxInbounds

		if loop == 0 {
			for idx := 0; idx < p2pInstNum; idx++ {
				snid0 := config.SubNetworkID{0xff, byte(idx & 0x3f)}
				myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
				myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
				myCfg.SubNetMaxInBounds[snid0] = config.MaxPeers
				myCfg.SubNetMaxOutbounds[snid0] = 0
			}
		} else {
			snid0 := config.SubNetworkID{0xff, byte(loop & 0x3f)}
			snid1 := config.SubNetworkID{0xff, byte((loop + 1) & 0x3f)}
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid0)
			myCfg.SubNetIdList = append(myCfg.SubNetIdList, snid1)
			myCfg.SubNetMaxPeers[snid0] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid0] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid0] = config.MaxOutbounds
			myCfg.SubNetMaxPeers[snid1] = config.MaxPeers
			myCfg.SubNetMaxInBounds[snid1] = config.MaxInbounds
			myCfg.SubNetMaxOutbounds[snid1] = config.MaxOutbounds
		}

		if loop == 0 {
			myCfg.NoDial = true
			myCfg.NoAccept = true
			myCfg.BootstrapNode = true
		} else {
			myCfg.NoDial = false
			myCfg.NoAccept = false
			myCfg.BootstrapNode = false
		}

		myCfg.BootstrapNodes = nil
		if loop != 0 {
			myCfg.BootstrapNodes = append(myCfg.BootstrapNodes, bootstrapNodes...)
		}

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		if loop == 0 {

			bootstrapIp = append(bootstrapIp, p2pName2Cfg[cfgName].Local.IP[:]...)
			bootstrapId = fmt.Sprintf("%x", p2pName2Cfg[cfgName].Local.ID)
			bootstrapUdp = p2pName2Cfg[cfgName].Local.UDP
			bootstrapTcp = p2pName2Cfg[cfgName].Local.TCP

			ipv4 := bootstrapIp.To4()
			url := []string{
				fmt.Sprintf("%s@%d.%d.%d.%d:%d:%d",
					bootstrapId,
					ipv4[0], ipv4[1], ipv4[2], ipv4[3],
					bootstrapUdp,
					bootstrapTcp),
			}
			bootstrapNodes = append(bootstrapNodes, config.P2pSetupBootstrapNodes(url)...)
		}

		p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase5: SchSchedulerInit failed, eno: %d", eno)
			return
		}
		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

		if loop == 0 {
			p2pInstBootstrap = p2pInst
		}
	}

	var p2pInstList = []*sch.Scheduler{}
	for p2pInst := range p2pInst2Cfg {
		if p2pInst != p2pInstBootstrap {
			p2pInstList = append(p2pInstList, p2pInst)
		}
	}

	if eno := shell.P2pStart(p2pInstBootstrap); eno != sch.SchEnoNone {
		log.Debug("testCase5: P2pStart failed, eno: %d", eno)
		return
	}

	for piNum := len(p2pInstList); piNum > 0; piNum-- {

		time.Sleep(time.Second * 2)

		pidx := rand.Intn(piNum)
		p2pInst := p2pInstList[pidx]
		p2pInstList = append(p2pInstList[0:pidx], p2pInstList[pidx+1:]...)

		appendTcb2List([]*sch.Scheduler{p2pInst})

		if eno := shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.Debug("testCase5: P2pStart failed, eno: %d", eno)
			return
		}

		cud := cbUserData{
			lock: tcbLocks[p2pInst],
			tcbList: tcbLists[p2pInst],
		}
		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, &cud, p2pInst);
			eno != shell.P2pEnoNone {
			log.Debug("testCase5: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		cfgName := p2pInst.SchGetP2pCfgName()
		log.Debug("testCase5: ycp2p started, cofig: %s", cfgName)
	}

	//
	// Sleep and then stop p2p instance
	//

	time.Sleep(time.Second * 64)
	golog.Printf("testCase5: going to stop p2p instances ...")

	stopCount := len(p2pInst2Cfg)
	stopChain := make(chan bool, stopCount)
	for p2pInst := range p2pInst2Cfg {
		go shell.P2pStop(p2pInst, stopChain)
	}

_waitStop:
	for {
		<-stopChain
		if stopCount--; stopCount == 0 {
			break _waitStop
		}
	}

	log.Debug("testCase5: wait all tx/rx instances to be done...")

	txrxStopAll()
	for !isTxRxAllDone() {
		log.Debug("testCase5: wait all tx/rx instances to be done...")
		time.Sleep(time.Second * 1)
	}
	golog.Printf("testCase5: it's the end")

	waitInterrupt()
}

//
// testCase6
//
func testCase6(tc *testCase) {
	
	log.Debug("testCase6: going to start ycDht ...")

	var dhtInstNum = 8
	var dhtInstList = []*sch.Scheduler{}

	for loop := 0; loop < dhtInstNum; loop++ {

		cfgName := fmt.Sprintf("dhtInst%d", loop)
		log.Debug("testCase6: handling configuration:%s ...", cfgName)

		var dftCfg *config.Config = nil

		if dftCfg = shell.ShellDefaultConfig(); dftCfg == nil {
			log.Debug("testCase6: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_DHT
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.Local.UDP = uint16(portBase4Dht + loop)
		myCfg.Local.TCP = uint16(portBase4Dht + loop)

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		dhtName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		dhtInst, eno := shell.P2pCreateInstance(dhtName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase6: SchSchedulerInit failed, eno: %d", eno)
			return
		}

		dhtInst2Cfg[dhtInst] = dhtName2Cfg[cfgName]
		dhtInstList = append(dhtInstList, dhtInst)
	}

	for _, dhtInst := range dhtInstList {
		time.Sleep(time.Second * 1)
		if eno := shell.P2pStart(dhtInst); eno != sch.SchEnoNone {
			log.Debug("testCase6: P2pStart failed, eno: %d", eno)
			return
		}
		dhtMgr := dhtInst.SchGetUserTaskIF(sch.DhtMgrName).(*dht.DhtMgr)
		if eno := dhtMgr.InstallEventCallback(dhtTestEventCallback); eno != dht.DhtEnoNone {
			log.Debug("testCase6: InstallEventCallback failed, eno: %d", eno)
			return
		}
	}

	cm := dhtTestBuildConnMatrix(dhtInstList)
	if cm == nil {
		log.Debug("testCase6: dhtBuildConnMatrix failed")
		return
	}

	log.Debug("testCase6: sleep a while for instances starup ...")
	time.Sleep(time.Second * 4)
	log.Debug("testCase6: going to apply connection matrix ...")

	if eno := dhtTestConnMatrixApply(dhtInstList, cm); eno != dht.DhtEnoNone {
		log.Debug("testCase6: dhtConnMatrixApply failed, eno: %d", eno)
		return
	}

	time.Sleep(time.Second*8)

	if eno := dhtTestBootstrapStart(dhtInstList); eno != dht.DhtEnoNone {
		log.Debug("testCase6: dhtTestBootstrapStart failed, eno: %d", eno)
		return
	}

	waitInterrupt()
}

//
// testCase7
//
func testCase7(tc *testCase) {

	log.Debug("testCase7: going to start ycDht ...")

	var dhtInstNum = 32
	var dhtInstList = []*sch.Scheduler{}

	for loop := 0; loop < dhtInstNum; loop++ {

		cfgName := fmt.Sprintf("dhtInst%d", loop)
		log.Debug("testCase7: handling configuration:%s ...", cfgName)

		var dftCfg *config.Config = nil

		if dftCfg = shell.ShellDefaultConfig(); dftCfg == nil {
			log.Debug("testCase7: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.AppType = config.P2P_TYPE_DHT
		myCfg.Name = cfgName
		myCfg.PrivateKey = nil
		myCfg.PublicKey = nil
		myCfg.Local.UDP = uint16(portBase4Dht + loop)
		myCfg.Local.TCP = uint16(portBase4Dht + loop)

		cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
		dhtName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

		dhtInst, eno := shell.P2pCreateInstance(dhtName2Cfg[cfgName])
		if eno != sch.SchEnoNone {
			log.Debug("testCase7: SchSchedulerInit failed, eno: %d", eno)
			return
		}

		dhtInst2Cfg[dhtInst] = dhtName2Cfg[cfgName]
		dhtInstList = append(dhtInstList, dhtInst)
	}

	for _, dhtInst := range dhtInstList {
		time.Sleep(time.Second * 1)
		if eno := shell.P2pStart(dhtInst); eno != sch.SchEnoNone {
			log.Debug("testCase7: P2pStart failed, eno: %d", eno)
			return
		}
		dhtMgr := dhtInst.SchGetUserTaskIF(sch.DhtMgrName).(*dht.DhtMgr)
		if eno := dhtMgr.InstallEventCallback(dhtTestEventCallback); eno != dht.DhtEnoNone {
			log.Debug("testCase7: InstallEventCallback failed, eno: %d", eno)
			return
		}
	}

	cm := dhtTestBuildConnMatrix(dhtInstList)
	if cm == nil {
		log.Debug("testCase7: dhtBuildConnMatrix failed")
		return
	}

	log.Debug("testCase7: sleep a while for instances starup ...")
	time.Sleep(time.Second * 4)
	log.Debug("testCase7: going to apply connection matrix ...")

	if eno := dhtTestConnMatrixApply(dhtInstList, cm); eno != dht.DhtEnoNone {
		log.Debug("testCase7: dhtConnMatrixApply failed, eno: %d", eno)
		return
	}

	time.Sleep(time.Second * 8)
	golog.Printf("testCase7: going to stop dht instances ...")

	stopCount := len(dhtInst2Cfg)
	stopChain := make(chan bool, stopCount)

	for dhtInst := range dhtInst2Cfg {
		go shell.P2pStop(dhtInst, stopChain)
	}

_waitStop:
	for {
		<-stopChain
		if stopCount--; stopCount == 0 {
			break _waitStop
		}
	}
	golog.Printf("testCase7: it's the end")
	waitInterrupt()
}

//
// build connection matrix for instance list
//
const (
	minInstNum		= 7
	instNeightbors	= 3

	lineMatrixType	= 0
	ringMatrixType	= 1
	starMatrixType	= 2
	randMatrixType	= 3
)

func dhtTestBuildConnMatrix(p2pInstList []*sch.Scheduler) [][]bool {
	// initialize specific connection relationship between dht instances, for example:
	// ring, star, line, random, ...
	var instNum int
	if instNum = len(p2pInstList); instNum <= minInstNum {
		log.Debug("dhtTestBuildConnMatrix: " +
			"instance not enougth, instNum: %d, should more than: %d",
			instNum, minInstNum)
		return nil
	}

	var m = make([][]bool, instNum)
	for idx := range m {
		row := make([]bool, instNum)
		m[idx] = row
		for idx := range row {
			row[idx] = false
		}
	}

	mt := lineMatrixType

	if mt == lineMatrixType {
		lineMatrix(m)
	} else if mt == ringMatrixType {
		ringMatrix(m)
	} else if mt == starMatrixType {
		starMatrix(m)
	} else if mt == randMatrixType{
		randMatrix(m)
	} else {
		lineMatrix(m)
	}
	return m
}

//
// setup line type connection matrix
//
func lineMatrix(m [][]bool) error {
	instNum := cap(m[0])
	if instNum <= minInstNum {
		return errors.New(fmt.Sprintf("min instances number: %d", minInstNum + 1))
	}
	for idx := 0; idx < instNum - 1; idx++ {
		m[idx][idx+1] = true
	}
	return nil
}

//
// setup ring type connection matrix
//
func ringMatrix(m [][]bool) error {
	instNum := cap(m[0])
	if instNum <= minInstNum {
		return errors.New(fmt.Sprintf("min instances number: %d", minInstNum + 1))
	}
	if err := lineMatrix(m); err != nil {
		return nil
	}
	m[instNum-1][0] = true
	return nil
}

//
// setup star type connection matrix
//
func starMatrix(m [][]bool) error {
	instNum := cap(m[0])
	if instNum <= minInstNum {
		return errors.New(fmt.Sprintf("min instances number: %d", minInstNum + 1))
	}
	for idx := 1; idx < instNum; idx++ {
		m[0][idx] = true
	}
	return nil
}

//
// setup a random connection matrix
//
func randMatrix(m [][]bool) error {
	rand.Seed(time.Now().Unix())
	instNum := cap(m[0])
	if instNum <= minInstNum {
		return errors.New(fmt.Sprintf("min instances number: %d", minInstNum + 1))
	}
	var neighbors = make([]int, instNum)
	for idx := 0; idx < instNum; idx++ {
		neighbors[idx] = 0
	}
	log.Debug("dhtTestBuildConnMatrix: total instance number: %d", instNum)
	for idx, row := range m {
		log.Debug("dhtTestBuildConnMatrix: instance index: %d", idx)
		count := neighbors[idx]
		if count >= instNeightbors {
			continue
		}
		for ; count < instNeightbors; {
			mask := bytes.Repeat([]byte{1}, instNum)
			randStop := false
			for !randStop {
				n := rand.Intn(instNum)
				mask[n] = 0
				allCovered := true
				for _, mk := range mask {
					allCovered = allCovered && (mk == 0)
				}
				randStop = allCovered
				if n == idx {
					continue
				}

				if row[n] == true {
					continue
				}

				if neighbors[n] >= instNeightbors {
					continue
				}

				row[n] = true
				m[n][idx] = true
				neighbors[n]++
				count++
				break
			}
			if randStop {
				break
			}
		}
		neighbors[idx] = count
	}
	return nil
}

//
// apply connection matrix for instance list
//
func dhtTestConnMatrixApply(p2pInstList []*sch.Scheduler, cm [][]bool) int {
	// setup connection between dht instance according to the connection matrix
	if len(p2pInstList) == 0 || cm == nil {
		log.Debug("dhtTestConnMatrixApply: invalid parameters")
		return -1
	}
	instNum := len(p2pInstList)
	cmBackup := make([][]bool, instNum)
	for idx := 0; idx < instNum; idx++ {
		cmBackup[idx] = append(cmBackup[idx], cm[idx]...)
	}
	conCount := 0
	for idx, row := range cmBackup {
		for n, connFlag := range row {
			if connFlag {
				dhtMgr := p2pInstList[idx].SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
				local := dhtInst2Cfg[p2pInstList[idx]].Local
				peerCfg := dhtInst2Cfg[p2pInstList[n]]
				req := sch.MsgDhtBlindConnectReq {
					Peer: &peerCfg.Local,
				}
				if eno := dhtMgr.DhtCommand(sch.EvDhtBlindConnectReq, &req); eno != sch.SchEnoNone {
					log.Debug("dhtTestConnMatrixApply: DhtCommand failed, eno: %d", eno)
					return -1
				}
				cmBackup[idx][n] = false
				cmBackup[n][idx] = false
				conCount++
				log.Debug("dhtTestConnMatrixApply: " +
					"blind connect request sent ok, from: %+v, to: %+v",
					local, peerCfg.Local)
			}
		}
	}
	log.Debug("dhtTestConnMatrixApply: applied, blind connection count: %d", conCount)
	return 0
}

//
// dht start route bootstrap(refreshing)
//
func dhtTestBootstrapStart(dhtInstList []*sch.Scheduler) int {
	for _, dhtInst := range dhtInstList {
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtRutRefreshReq, nil)
	}
	return dht.DhtEnoNone
}

//
// find node
//
func dhtTestFindNode(dhtInstList []*sch.Scheduler) int {
	rand.Seed(time.Now().Unix())
	req := sch.MsgDhtQryMgrQueryStartReq {
		Target:		config.NodeID{},
		Msg:		nil,
		ForWhat:	dht.MID_FINDNODE,
		Seq:		-1,
	}
	instNum := len(dhtInstList)
	for idx, dhtInst := range dhtInstList {
		targetIdx := rand.Intn(instNum)
		for ; targetIdx == idx; {
			targetIdx = rand.Intn(instNum)
		}
		req.Target = dhtInstList[targetIdx].SchGetP2pConfig().Local.ID
		req.Seq = time.Now().Unix()
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtMgrFindPeerReq, &req)
	}
	return dht.DhtEnoNone
}

//
// put value
//
type DhtTestKV struct {
	Key		[]byte
	Val		[]byte
}
func dhtTestPutValue(dhtInstList []*sch.Scheduler) (int, [] *DhtTestKV) {
	rand.Seed(time.Now().Unix())
	kvList := []*DhtTestKV{}
	req := sch.MsgDhtMgrPutValueReq{
		Key:	nil,
		Val:	nil,
	}
	for _, dhtInst := range dhtInstList {
		val := make([]byte, 128, 128)
		rand.Read(val)
		req.Val = []byte(fmt.Sprintf("%s:%x", dhtInst.SchGetP2pCfgName(), val))
		key := sha256.Sum256(req.Val)
		req.Key = key[0:]
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtMgrPutValueReq, &req)
		kv := DhtTestKV {
			Key:	req.Key,
			Val:	req.Val,
		}
		kvList = append(kvList, &kv)
	}
	return dht.DhtEnoNone, kvList
}

//
// get value
//
func dhtTestGetValue(dhtInstList []*sch.Scheduler, keys [][]byte) int {
	if len(dhtInstList) != len(keys) {
		return dht.DhtEnoParameter
	}
	req := sch.MsgDhtMgrGetValueReq{
		Key:	nil,
	}
	for idx, dhtInst := range dhtInstList {
		req.Key = keys[idx]
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtMgrGetValueReq, &req)
	}
	return dht.DhtEnoNone
}

//
// put provider
//
type DhtTestPrd struct {
	Key		[]byte
	Prd		config.Node
}
func dhtTestPutProvider(dhtInstList []*sch.Scheduler) (int, []*DhtTestPrd) {
	rand.Seed(time.Now().Unix())
	prdList := []*DhtTestPrd{}
	req := sch.MsgDhtPrdMgrAddProviderReq {
		Key:	nil,
		Prd:	config.Node{},
	}
	for _, dhtInst := range dhtInstList {
		val := make([]byte, 128, 128)
		rand.Read(val)
		key := sha256.Sum256([]byte(fmt.Sprintf("%s:%x", dhtInst.SchGetP2pCfgName(), val)))
		req.Key = key[0:]
		req.Prd = dhtInst.SchGetP2pConfig().Local
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtMgrPutProviderReq, &req)
		prd := DhtTestPrd {
			Key:	req.Key,
			Prd:	req.Prd,
		}
		prdList = append(prdList, &prd)
	}
	return dht.DhtEnoNone, prdList
}

//
// get provider
//
func dhtTestGetProvider(dhtInstList []*sch.Scheduler, keys [][]byte) int {
	if len(dhtInstList) != len(keys) {
		return dht.DhtEnoParameter
	}
	req := sch.MsgDhtMgrGetProviderReq {
		Key:	nil,
	}
	for idx, dhtInst := range dhtInstList {
		req.Key = keys[idx]
		dhtMgr := dhtInst.SchGetUserTaskIF(dht.DhtMgrName).(*dht.DhtMgr)
		dhtMgr.DhtCommand(sch.EvDhtMgrGetProviderReq, &req)
	}
	return dht.DhtEnoNone
}

//
// dht event callback
//
func dhtTestEventCallback(mgr interface{}, mid int, msg interface{}) int {
	eno := -1
	switch mid {
	case  sch.EvDhtBlindConnectRsp:
		eno = dhtTestBlindConnectRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtBlindConnectRsp))
	case  sch.EvDhtMgrFindPeerRsp:
		eno = dhtTestMgrFindPeerRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtQryMgrQueryResultInd))
	case  sch.EvDhtQryMgrQueryStartRsp:
		eno = dhtTestQryMgrQueryStartRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtQryMgrQueryStartRsp))
	case  sch.EvDhtQryMgrQueryStopRsp:
		eno = dhtTestQryMgrQueryStopRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtQryMgrQueryStopRsp))
	case  sch.EvDhtConMgrSendCfm:
		eno = dhtTestConMgrSendCfm(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtConMgrSendCfm))
	case  sch.EvDhtMgrPutProviderRsp:
		eno = dhtTestMgrPutProviderRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtPrdMgrAddProviderRsp))
	case  sch.EvDhtMgrGetProviderRsp:
		eno = dhtTestMgrGetProviderRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtMgrGetProviderRsp))
	case  sch.EvDhtMgrPutValueRsp:
		eno = dhtTestMgrPutValueRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtMgrPutValueRsp))
	case  sch.EvDhtMgrGetValueRsp:
		eno = dhtTestMgrGetValueRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtMgrGetValueRsp))
	case  sch.EvDhtConMgrCloseRsp:
		eno = dhtTestConMgrCloseRsp(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtConMgrCloseRsp))
	case  sch.EvDhtConInstStatusInd:
		eno = dhtTestConInstStatusInd(mgr.(*dht.DhtMgr), msg.(*sch.MsgDhtConInstStatusInd))
	default:
		log.Debug("dhtTestEventCallback: unknown event: %d", mid)
	}
	return eno
}

func dhtTestBlindConnectRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtBlindConnectRsp) int {
	if mgr == nil {
		log.Debug("dhtTestBlindConnectRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestBlindConnectRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestBlindConnectRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestMgrFindPeerRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtQryMgrQueryResultInd) int {
	if mgr == nil {
		log.Debug("dhtTestMgrFindPeerRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestMgrFindPeerRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestMgrFindPeerRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestQryMgrQueryStartRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtQryMgrQueryStartRsp) int {
	if mgr == nil {
		log.Debug("dhtTestQryMgrQueryStartRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestQryMgrQueryStartRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestQryMgrQueryStartRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestQryMgrQueryStopRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtQryMgrQueryStopRsp) int {
	if mgr == nil {
		log.Debug("dhtTestQryMgrQueryStopRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestQryMgrQueryStopRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestQryMgrQueryStopRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestConMgrSendCfm(mgr *dht.DhtMgr, msg *sch.MsgDhtConMgrSendCfm) int {
	if mgr == nil {
		log.Debug("dhtTestConMgrSendCfm: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestConMgrSendCfm: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestConMgrSendCfm: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestMgrPutProviderRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtPrdMgrAddProviderRsp) int {
	if mgr == nil {
		log.Debug("dhtTestMgrPutProviderRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestMgrPutProviderRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestMgrPutProviderRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestMgrGetProviderRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtMgrGetProviderRsp) int {
	if mgr == nil {
		log.Debug("dhtTestMgrGetProviderRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestMgrGetProviderRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestMgrGetProviderRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestMgrPutValueRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtMgrPutValueRsp) int {
	if mgr == nil {
		log.Debug("dhtTestMgrPutValueRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestMgrPutValueRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestMgrPutValueRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestMgrGetValueRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtMgrGetValueRsp) int {
	if mgr == nil {
		log.Debug("dhtTestMgrGetValueRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestMgrGetValueRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestMgrGetValueRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestConMgrCloseRsp(mgr *dht.DhtMgr, msg *sch.MsgDhtConMgrCloseRsp) int {
	if mgr == nil {
		log.Debug("dhtTestConMgrCloseRsp: nil manager")
		return -1
	}
	sdl := mgr.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestConMgrCloseRsp: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestConMgrCloseRsp: instance: %s, msg: %v", cfgName, *msg)
	return 0
}

func dhtTestConInstStatusInd(mgr *dht.DhtMgr, msg *sch.MsgDhtConInstStatusInd) int {
	switch msg.Status {
	case dht.CisNull:
		log.Debug("dhtTestConInstStatusInd: CisNull")
	case dht.CisConnecting:
		log.Debug("dhtTestConInstStatusInd: CisConnecting")
	case dht.CisConnected:
		log.Debug("dhtTestConInstStatusInd: CisConnected")
		if msg.Dir == dht.ConInstDirOutbound {
			if eno := mgr.InstallRxDataCallback(dhtTestConInstRxDataCallback,
				msg.Peer, dht.ConInstDir(msg.Dir)); eno != dht.DhtEnoNone {

				log.Debug("dhtTestConInstStatusInd: "+
					"InstallRxDataCallback failed, eno: %d, peer: %x",
					eno, *msg.Peer)

				return -1
			}
		}
	case dht.CisAccepted:
		log.Debug("dhtTestConInstStatusInd: CisAccepted")
	case dht.CisInHandshaking:
		log.Debug("dhtTestConInstStatusInd: CisInHandshaking")
	case dht.CisHandshaked:
		log.Debug("dhtTestConInstStatusInd: CisHandshaked")
		if msg.Dir == dht.ConInstDirInbound {
			if eno := mgr.InstallRxDataCallback(dhtTestConInstRxDataCallback,
				msg.Peer, dht.ConInstDir(msg.Dir)); eno != dht.DhtEnoNone {
				log.Debug("dhtTestConInstStatusInd: "+
					"InstallRxDataCallback failed, eno: %d, peer: %x",
					eno, *msg.Peer)
				return -1
			}
		}
	case dht.CisInService:
		log.Debug("dhtTestConInstStatusInd: CisInService")
	case dht.CisClosed:
		log.Debug("dhtTestConInstStatusInd: CisClosed")
	default:
		log.Debug("dhtTestConInstStatusInd: unknown status: %d", msg.Status)
	}
	return 0
}

//
// connetion instance rx-data callback
//
func dhtTestConInstRxDataCallback (conInst interface{}, pid uint32, msg interface{}) int {
	if conInst == nil || msg == nil {
		log.Debug("dhtTestConInstRxDataCallback:invalid parameters, " +
			"conInst: %p, pid: %d, msg: %p", conInst, pid, msg)
		return -1
	}
	ci, ok := conInst.(*dht.ConInst)
	if !ok {
		log.Debug("dhtTestConInstRxDataCallback: invalid connection instance, type: %T", conInst)
		return -1
	}
	data, ok := msg.([]byte)
	if !ok {
		log.Debug("dhtTestConInstRxDataCallback: invalid message, type: %T", msg)
		return -1
	}

	if pid != uint32(dht.PID_EXT) {
		log.Debug("dhtTestConInstRxDataCallback: " +
			"invalid pid: %d",
			pid)
		return -1
	}
	sdl := ci.GetScheduler()
	if sdl == nil {
		log.Debug("dhtTestConInstRxDataCallback: nil scheduler")
		return -1
	}
	cfgName := sdl.SchGetP2pCfgName()
	log.Debug("dhtTestConInstRxDataCallback: instance: %s, pid: %d, length: %d, data: %x",
		cfgName, pid, len(data), data)
	return 0
}
