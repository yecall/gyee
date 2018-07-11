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
	"os/signal"
	shell	"github.com/yeeco/gyee/p2p/shell"
	peer	"github.com/yeeco/gyee/p2p/peer"
	config	"github.com/yeeco/gyee/p2p/config"
	log		"github.com/yeeco/gyee/p2p/logger"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
)

//
// Configuration pointer
//
var p2pName2Cfg = make(map[string]*config.Config)
var p2pInst2Cfg = make(map[*sch.Scheduler]*config.Config)

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
}

//
// test statistics
//
type testCaseCtrlBlock struct {
	done 	chan bool
	txSeq	int64
	rxSeq	int64
}

//
// Done signal for Tx routines
//
var doneMap = make(map[*sch.Scheduler]map[peerIdEx]*testCaseCtrlBlock)

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
		description:	"common test case for AnySubNet",
		entry:			testCase0,
	},
	{
		name:			"testCase1",
		description:	"case of static sub network",
		entry:			testCase1,
	},
}

//
// target case
//
var tgtCase string = "testCase1"

//
// create test case control block by name
//
func newTcb(name string) *testCaseCtrlBlock {

	tcb := testCaseCtrlBlock {
		done:	make(chan bool, 1),
		txSeq:	0,
		rxSeq:	0,
	}

	switch name {
	case "testCase0":
	case "testCase1":
	default:
		log.LogCallerFileLine("newTcb: undefined test: %s", name)
		return nil
	}

	return &tcb
}

//
// Tx routine
//
func txProc(p2pInst *sch.Scheduler, snid peer.SubNetworkID, id peer.PeerId) {

	//
	// This demo simply apply timer with 1s cycle and then sends a string
	// again and again; The "done" signal is also checked to determine if
	// task is done. See bellow pls.
	//

	idEx := peerIdEx {
		subNetId:	snid,
		nodeId:		id,
	}

	if _, exist := doneMap[p2pInst]; exist == false {
		doneMap[p2pInst] = make(map[peerIdEx] *testCaseCtrlBlock, 0)
	}

	if _, dup := doneMap[p2pInst][idEx]; dup == true {

		log.LogCallerFileLine("txProc: " +
			"duplicated, subnet: %s, id: %s",
			fmt.Sprintf("%x", snid),
			fmt.Sprintf("%X", id))

		return
	}

	tcb := newTcb(tgtCase)
	doneMap[p2pInst][idEx] = tcb

	pkg := peer.P2pPackage2Peer {
		P2pInst:		p2pInst,
		IdList: 		make([]peer.PeerId, 0),
		ProtoId:		int(peer.PID_EXT),
		PayloadLength:	0,
		Payload:		make([]byte, 0, 512),
		Extra:			nil,
	}

	log.LogCallerFileLine("txProc: " +
		"entered, subnet: %s, id: %s",
		fmt.Sprintf("%x", snid),
		fmt.Sprintf("%X", id))


	var tmHandler = func() {

		tcb.txSeq++

		pkg.IdList = make([]peer.PeerId, 1)

		for id := range doneMap[p2pInst] {

			txString := fmt.Sprintf(">>>>>> \nseq:%d\n"+
				"to: subnet: %s\n, id: %s\n",
				tcb.txSeq,
				fmt.Sprintf("%x", snid),
				fmt.Sprintf("%X", id))

			pkg.SubNetId = id.subNetId
			pkg.IdList[0] = id.nodeId
			pkg.Payload = []byte(txString)
			pkg.PayloadLength = len(pkg.Payload)

			if eno := shell.P2pSendPackage(&pkg); eno != shell.P2pEnoNone {

				log.LogCallerFileLine("txProc: "+
					"send package failed, eno: %d, subnet: %s, id: %s",
					eno,
					fmt.Sprintf("%x", snid),
					fmt.Sprintf("%X", id))
			}
		}
	}

	tm := time.NewTicker(time.Second * 1)
	defer tm.Stop()

txLoop:

	for {

		select {

		case isDone := <-tcb.done:

			if isDone {
				log.LogCallerFileLine("txProc: "+
					"it's done, isDone: %s, subnet: %s, id: %s",
					fmt.Sprintf("%t", isDone),
					fmt.Sprintf("%x", snid),
					fmt.Sprintf("%X", id))
				break txLoop
			}

		case <-tm.C:

			tmHandler()

		default:
		}
	}

	close(tcb.done)
	delete(doneMap[p2pInst], idEx)
	if len(doneMap[p2pInst]) == 0 {
		delete(doneMap, p2pInst)
	}

	log.LogCallerFileLine("txProc: " +
		"exit, subnet: %s, id: %s",
		fmt.Sprintf("%x", snid),
		fmt.Sprintf("%X", id))
}


//
// Indication handler
//
func p2pIndProc(what int, para interface{}) interface{} {

	//
	// check what is indicated
	//

	switch what {

	case shell.P2pIndPeerActivated:

		//
		// a peer is activated to work, so one can install the incoming packages
		// handler.
		//

		pap := para.(*peer.P2pIndPeerActivatedPara)

		log.LogCallerFileLine("p2pIndProc: " +
			"P2pIndPeerActivated, para: %s",
			fmt.Sprintf("%+v", *pap.PeerInfo))

		if eno := shell.P2pRegisterCallback(shell.P2pPkgCb, p2pPkgHandler, pap.Ptn);
		eno != shell.P2pEnoNone {

			log.LogCallerFileLine("p2pIndProc: " +
				"P2pRegisterCallback failed, eno: %d",
				eno)
		}

		p2pInst := sch.SchGetScheduler(pap.Ptn)
		snid := pap.PeerInfo.Snid
		peerId := pap.PeerInfo.NodeId

		go txProc(p2pInst, snid, peerId)

	case shell.P2pIndConnStatus:

		//
		// Peer connection status report. in general, this report is resulted for
		// errors fired on the connection, one can check the "Flag" field in the
		// indication to know if p2p underlying would try to close the connection
		// itself, and one also can check the "Status" field to known what had
		// happened(the interface for this is not completed yet). Following demo
		// take a simple method: if connection is not closed by p2p itself, then
		// request p2p to close it here.
		//

		psp := para.(*peer.P2pIndConnStatusPara)
		p2pInst := sch.SchGetScheduler(psp.Ptn)

		log.LogCallerFileLine("p2pIndProc: " +
			"P2pIndConnStatus, para: %s",
			fmt.Sprintf("%+v", *psp))

		if psp.Status != 0 {

			log.LogCallerFileLine("p2pIndProc: " +
				"status: %d, close peer: %s",
				psp.Status,
				fmt.Sprintf("subnet:%x, id:%X", psp.PeerInfo.Snid, psp.PeerInfo.NodeId))

			if psp.Flag == false {

				log.LogCallerFileLine("p2pIndProc: " +
					"try to close the instance, peer: %s",
					fmt.Sprintf("subnet:%x, id:%X", psp.PeerInfo.Snid, psp.PeerInfo.NodeId))

				if eno := shell.P2pClosePeer(p2pInst, &psp.PeerInfo.Snid, &psp.PeerInfo.NodeId);
					eno != shell.P2pEnoNone {

					log.LogCallerFileLine("p2pIndProc: "+
						"P2pClosePeer failed, eno: %d, peer: %s",
						eno,
						fmt.Sprintf("subnet:%x, id:%X", psp.PeerInfo.Snid, psp.PeerInfo.NodeId))
				}
			}
		}

	case shell.P2pIndPeerClosed:

		//
		// Peer connection had been closed, one can clean his working context, see
		// bellow statements please.
		//

		pcp := para.(*peer.P2pIndPeerClosedPara)
		p2pInst := sch.SchGetScheduler(pcp.Ptn)

		log.LogCallerFileLine("p2pIndProc: " +
			"P2pIndPeerClosed, para: %s",
			fmt.Sprintf("%+v", *pcp))

		idEx := peerIdEx{subNetId:pcp.Snid, nodeId:pcp.PeerId}
		if tcb, ok := doneMap[p2pInst][idEx]; ok && tcb != nil {
			tcb.done<-true
			break
		}

		log.LogCallerFileLine("p2pIndProc: " +
			"done failed, subnet: %s, id: %s",
			fmt.Sprintf("%x", pcp.Snid),
			fmt.Sprintf("%X", pcp.PeerId))


	default:

		log.LogCallerFileLine("p2pIndProc: " +
			"inknown indication: %d",
				what)
	}

	return para
}

//
// Package handler
//
func p2pPkgProc(pkg *peer.P2pPackage4Callback) interface{} {

	p2pInst := sch.SchGetScheduler(pkg.Ptn)
	snid := pkg.PeerInfo.Snid
	peerId := pkg.PeerInfo.NodeId

	if _, exist := doneMap[p2pInst]; !exist {
		log.LogCallerFileLine("p2pPkgProc: " +
			"not activated, subnet: %s, id: %s",
			fmt.Sprintf("%x", snid),
			fmt.Sprintf("%X", peerId))
		return nil
	}

	idEx := peerIdEx{subNetId:snid, nodeId:peerId}
	tcb, exist := doneMap[p2pInst][idEx]
	if !exist {
		log.LogCallerFileLine("p2pPkgProc: " +
			"not activated, subnet: %s, id: %s",
			fmt.Sprintf("%x", snid),
			fmt.Sprintf("%X", peerId))
		return nil
	}

	tcb.rxSeq++

	return nil
}

//
// hook a system interrupt signal and wait on it
//
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
	log.LogCallerFileLine("main: target case not found: %s", tgtCase)
}

//
// testCase0
//
func testCase0(tc *testCase) {

	log.LogCallerFileLine("testCase0: going to start ycp2p ...")

	//
	// fetch default from underlying
	//

	dftCfg := shell.ShellDefaultConfig()
	if dftCfg == nil {
		log.LogCallerFileLine("testCase0: ShellDefaultConfig failed")
		return
	}

	//
	// one can then apply his configurations based on the default by calling
	// ShellSetConfig with a defferent configuration if he likes to. notice
	// that a configuration name also returned.
	//

	myCfg := *dftCfg
	cfgName := "myCfg"
	cfgName, _ = shell.ShellSetConfig(cfgName, &myCfg)
	p2pName2Cfg[cfgName] = shell.ShellGetConfig(cfgName)

	//
	// init underlying p2p logic, an instance of p2p returned
	//

	p2pInst, eno := shell.P2pCreateInstance(p2pName2Cfg[cfgName])
	if eno != sch.SchEnoNone {
		log.LogCallerFileLine("testCase0: SchSchedulerInit failed, eno: %d", eno)
		return
	}
	p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

	//
	// start p2p instance
	//

	if eno = shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
		log.LogCallerFileLine("testCase0: P2pStart failed, eno: %d", eno)
		return
	}

	//
	// register indication handler. notice that please, the indication handler is a
	// global object for all peers connected, while the incoming packages callback
	// handler is owned by every peer, and it can be installed while activation of
	// a peer is indicated. See demo indication handler p2pIndHandler and incoming
	// package handler p2pPkgHandler for more please.
	//

	if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, p2pInst);
	eno != shell.P2pEnoNone {
		log.LogCallerFileLine("testCase0: P2pRegisterCallback failed, eno: %d", eno)
		return
	}

	log.LogCallerFileLine("testCase0: ycp2p started, eno: %d", eno)

	//
	// wait os interrupt signal
	//

	waitInterrupt()
}

//
// testCase1
//
func testCase1(tc *testCase) {

	log.LogCallerFileLine("testCase1: going to start ycp2p ...")

	var bootstrapIp net.IP
	var bootstrapId string = ""
	var bootstrapUdp uint16 = 0
	var bootstrapTcp uint16 = 0
	var bootstrapNodes = []*config.Node{}

	for loop := 0; loop < 5; loop++ {

		cfgName := fmt.Sprintf("p2pInst%d", loop)
		log.LogCallerFileLine("testCase1: handling configuration:%s ...", cfgName)

		dftCfg := shell.ShellDefaultConfig()
		if dftCfg == nil {
			log.LogCallerFileLine("testCase1: ShellDefaultConfig failed")
			return
		}

		myCfg := *dftCfg
		myCfg.Name = cfgName
		myCfg.Local.IP = net.IP{127, 0, 0, 1}
		myCfg.Local.UDP = uint16(30303 + loop)
		myCfg.Local.TCP = uint16(30303 + loop)

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
			bootstrapId = fmt.Sprintf("%X", p2pName2Cfg[cfgName].Local.ID)
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
			log.LogCallerFileLine("testCase1: SchSchedulerInit failed, eno: %d", eno)
			return
		}
		p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

		if eno = shell.P2pStart(p2pInst); eno != sch.SchEnoNone {
			log.LogCallerFileLine("testCase0: P2pStart failed, eno: %d", eno)
			return
		}

		if eno := shell.P2pRegisterCallback(shell.P2pIndCb, p2pIndHandler, p2pInst);
			eno != shell.P2pEnoNone {
			log.LogCallerFileLine("testCase1: P2pRegisterCallback failed, eno: %d", eno)
			return
		}

		log.LogCallerFileLine("testCase1: ycp2p started, cofig: %s, eno: %d", cfgName, eno)
	}

	waitInterrupt()
}



