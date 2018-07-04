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
	"os/signal"
	"github.com/yeeco/gyee/p2p/shell"
	"github.com/yeeco/gyee/p2p/peer"
	ycfg	"github.com/yeeco/gyee/p2p/config"
	yclog	"github.com/yeeco/gyee/p2p/logger"
	sch		"github.com/yeeco/gyee/p2p/scheduler"
)


//
// Configuration pointer
//
var p2pName2Cfg = make(map[string]*ycfg.Config)
var p2pInst2Cfg = make(map[*sch.Scheduler]*ycfg.Config)

//
//
//


//
// Indication/Package handlers
//
var (
	p2pIndHandler peer.P2pInfIndCallback = p2pIndProc
	p2pPkgHandler peer.P2pInfPkgCallback = p2pPkgProc
)

//
// Done signal for Tx routines
//
var doneMap = make(map[*sch.Scheduler]map[peer.PeerId]chan bool)

//
// Tx routine
//
func txProc(p2pInst *sch.Scheduler, id peer.PeerId) {

	//
	// This demo simply apply timer with 1s cycle and then sends a string
	// again and again; The "done" signal is also checked to determine if
	// task is done. See bellow pls.
	//

	if _, dup := doneMap[p2pInst][id]; dup == true {

		yclog.LogCallerFileLine("txProc: " +
			"duplicated, id: %s",
			fmt.Sprintf("%X", id))

		return
	}

	seq := 0

	done := make(chan bool, 1)
	idMap := make(map[peer.PeerId]chan bool)
	idMap[id] = done
	doneMap[p2pInst] = idMap

	pkg := peer.P2pPackage2Peer {
		P2pInst:		p2pInst,
		IdList: 		make([]peer.PeerId, 0),
		ProtoId:		int(peer.PID_EXT),
		PayloadLength:	0,
		Payload:		make([]byte, 0, 512),
		Extra:			nil,
	}

	yclog.LogCallerFileLine("txProc: " +
		"entered, id: %s",
		fmt.Sprintf("%X", id))


	var tmHandler = func() {

		seq++

		pkg.IdList = make([]peer.PeerId, 1)

		for id := range doneMap[p2pInst] {

			txString := fmt.Sprintf("<<<<<<\nseq:%d\n"+
				"from: %s\n"+
				"to: %s\n"+
				">>>>>>",
				seq,
				fmt.Sprintf("%X", p2pInst2Cfg[p2pInst].Local.ID),
				fmt.Sprintf("%X", id))

			pkg.IdList[0] = id
			pkg.Payload = []byte(txString)
			pkg.PayloadLength = len(pkg.Payload)

			if eno := shell.P2pInfSendPackage(&pkg); eno != shell.P2pInfEnoNone {
				yclog.LogCallerFileLine("txProc: "+
					"send package failed, eno: %d, id: %s",
					eno,
					fmt.Sprintf("%X", p2pInst2Cfg[p2pInst].Local.ID))
			}

			yclog.LogCallerFileLine("txProc: %s", txString)
		}
	}

	tm := time.NewTicker(time.Second * 1)
	defer tm.Stop()

txLoop:

	for {

		select {

		case isDone := <-done:

			if isDone {
				yclog.LogCallerFileLine("txProc: "+
					"it's done, isDone: %s",
					fmt.Sprintf("%t", isDone))
				break txLoop
			}

		case <-tm.C:

			tmHandler()

		default:
		}
	}

	close(done)
	delete(doneMap[p2pInst], id)
	if len(doneMap[p2pInst]) == 0 {
		delete(doneMap, p2pInst)
	}

	yclog.LogCallerFileLine("txProc: " +
		"exit, id: %s",
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

		yclog.LogCallerFileLine("p2pIndProc: " +
			"P2pIndPeerActivated, para: %s",
			fmt.Sprintf("%+v", *pap))

		if eno := shell.P2pInfRegisterCallback(shell.P2pInfPkgCb, p2pPkgHandler, pap.Ptn);
		eno != shell.P2pInfEnoNone {

			yclog.LogCallerFileLine("p2pIndProc: " +
				"P2pInfRegisterCallback failed, eno: %d",
				eno)
		}

		p2pInst := sch.SchinfGetScheduler(pap.Ptn)
		go txProc(p2pInst, peer.PeerId(pap.PeerInfo.NodeId))

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
		p2pInst := sch.SchinfGetScheduler(psp.Ptn)

		yclog.LogCallerFileLine("p2pIndProc: " +
			"P2pIndConnStatus, para: %s",
			fmt.Sprintf("%+v", *psp))

		if psp.Status != 0 {

			yclog.LogCallerFileLine("p2pIndProc: " +
				"status: %d, close peer: %s",
				psp.Status,
				fmt.Sprintf("%X", psp.PeerInfo.NodeId	))

			if psp.Flag == false {

				yclog.LogCallerFileLine("p2pIndProc: " +
					"try to close the instance, peer: %s",
					fmt.Sprintf("%X", (*peer.PeerId)(&psp.PeerInfo.NodeId)))

				if eno := shell.P2pInfClosePeer(p2pInst, (*peer.PeerId)(&psp.PeerInfo.NodeId));
					eno != shell.P2pInfEnoNone {
					yclog.LogCallerFileLine("p2pIndProc: "+
						"P2pInfClosePeer failed, eno: %d, peer: %s",
						eno,
						fmt.Sprintf("%X", psp.PeerInfo.NodeId))
				}
			}
		}

	case shell.P2pIndPeerClosed:

		//
		// Peer connection had been closed, one can clean his working context, see
		// bellow statements please.
		//

		pcp := para.(*peer.P2pIndPeerClosedPara)
		p2pInst := sch.SchinfGetScheduler(pcp.Ptn)

		yclog.LogCallerFileLine("p2pIndProc: " +
			"P2pIndPeerClosed, para: %s",
			fmt.Sprintf("%+v", *pcp))


		if done, ok := doneMap[p2pInst][pcp.PeerId]; ok && done != nil {
			done<-true
			break
		}

		yclog.LogCallerFileLine("p2pIndProc: " +
			"done failed, id: %s",
			fmt.Sprintf("%X", pcp.PeerId))


	default:

		yclog.LogCallerFileLine("p2pIndProc: " +
			"inknown indication: %d",
				what)
	}

	return para
}

//
// Package handler
//
func p2pPkgProc(pkg *peer.P2pPackage4Callback) interface{} {

	//
	// the demo just print the payload, provided that it's a string,
	// see function txProc(in this file) to know what's sent pls.
	//

	yclog.LogCallerFileLine("p2pPkgProc: " +
		"paylod of package: %s",
		fmt.Sprintf("%s", pkg.Payload))

	return nil
}

func main() {

	yclog.LogCallerFileLine("main: going to start ycp2p ...")

	//
	// fetch default from underlying
	//

	dftCfg := shell.ShellDefaultConfig()
	if dftCfg == nil {
		yclog.LogCallerFileLine("main: ShellDefaultConfig failed")
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
		yclog.LogCallerFileLine("main: SchinfSchedulerInit failed, eno: %d", eno)
		return
	}
	p2pInst2Cfg[p2pInst] = p2pName2Cfg[cfgName]

	//
	// start p2p instance
	//

	eno, _ = shell.P2pStart(p2pInst)

	//
	// register indication handler. notice that please, the indication handler is a
	// global object for all peers connected, while the incoming packages callback
	// handler is owned by every peer, and it can be installed while activation of
	// a peer is indicated. See demo indication handler p2pIndHandler and incoming
	// package handler p2pPkgHandler for more please.
	//

	if eno := shell.P2pInfRegisterCallback(shell.P2pInfIndCb, p2pIndHandler, p2pInst);
	eno != shell.P2pInfEnoNone {
		yclog.LogCallerFileLine("main: P2pInfRegisterCallback failed, eno: %d", eno)
		return
	}

	yclog.LogCallerFileLine("main: ycp2p started, eno: %d", eno)

	//
	// hook a system interrupt signal and wait on it
	//

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
}

