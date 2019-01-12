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

//
// Notice: the test cases can work only those _TEST_ in peer manager and dht
// manager are set to "true", check them please.
//
package main

import (
	"time"
	"fmt"
	"os"
	"strconv"
	"runtime"
	"os/signal"
	"crypto/sha256"
	_ "net/http/pprof"
	"net/http"
	log		"github.com/yeeco/gyee/p2p/logger"
	yep2p	"github.com/yeeco/gyee/p2p"
	config	"github.com/yeeco/gyee/p2p/config"
)

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
		name:			"testCase5",
		description:	"yee chain start/stop test, SubNetMaskBits == 0",
		entry:			testCase5,
	},
	{
		name:			"testCase6",
		description:	"yee chain start/stop test, SubNetMaskBits != 0",
		entry:			testCase6,
	},
	{
		name:			"testCase7",
		description:	"yee chain test, SubNetMaskBits == 0",
		entry:			testCase7,
	},
	{
		name:			"testCase8",
		description:	"yee chain OSN test, SubNetMaskBits != 0",
		entry:			testCase8,
	},
	{
		name:			"testCase9",
		description:	"yee chain OSN test, SubNetMaskBits == 0",
		entry:			testCase9,
	},
	{
		name:			"testCase10",
		description:	"yee chain OSN test, SubNetMaskBits != 0",
		entry:			testCase10,
	},
	{
		name:			"testCase11",
		description:	"multiple yee chain OSN test on one host",
		entry:			testCase11,
	},
}

//
// target case
//
var tgtCase = "testCase7"

//
// switch for playing go-monitors, related commands:
// >> go tool pprof http://localhost:6060/debug/pprof/heap
// >> curl localhost:6060/goroutines
//
const goMonitors = true

//
// run target case
//
func main() {

	if goMonitors {
		startGoMemoryMonitor()
		startGoRoutineMonitor()
	}

	for _, tc := range testCaseTable {
		if tc.name == tgtCase {
			tc.entry(&tc)
			return
		}
	}
	log.Debug("main: target case not found: %s", tgtCase)
}

func startGoMemoryMonitor() {
	go func() {
		http.ListenAndServe("127.0.0.1:6060", nil)
	}()
}

func startGoRoutineMonitor() {
	go func() {
		http.HandleFunc("/goroutines", func(w http.ResponseWriter, r *http.Request) {
			num := strconv.FormatInt(int64(runtime.NumGoroutine()), 10)
			w.Write([]byte(num))
		});
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func waitInterrupt() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
}

func waitInterruptWithCallback(srv yep2p.Service, workp appWorkProc, stopp appStopProc) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
	workp(srv, ev, tx, bh, bk)
	stopp(srv, subEv, subTx, subBh)
}

var (
	ev = yep2p.Message{
		MsgType:	yep2p.MessageTypeEvent,
	}

	tx = yep2p.Message{
		MsgType:	yep2p.MessageTypeTx,
	}

	bh = yep2p.Message{
		MsgType:	yep2p.MessageTypeBlockHeader,
	}

	bk = yep2p.Message{
		MsgType:	yep2p.MessageTypeBlock,
	}

	subEv = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeEvent,
	}

	subTx = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeTx,
	}

	subBh = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeBlockHeader,
	}
)

type appMessages struct {
	ev	yep2p.Message
	tx	yep2p.Message
	bh	yep2p.Message
	bk	yep2p.Message
}

type appSubcribers struct {
	subEv	yep2p.Subscriber
	subTx	yep2p.Subscriber
	subBh	yep2p.Subscriber
}

type appWorkProc func(srv yep2p.Service, ev yep2p.Message, tx yep2p.Message, bh yep2p.Message, bk yep2p.Message)
type appStopProc func(srv yep2p.Service, subEv yep2p.Subscriber, subTx yep2p.Subscriber, subBh yep2p.Subscriber)

var subFunc = func(sub yep2p.Subscriber, tag string) {
	count := 0
_loop:
	for {
		select {
		case msg, ok := <-sub.MsgChan:
			if !ok {
				break _loop
			}
			if count++; count & 0x7f == 0 {
				log.Debug("subFunc: count: %d, %s: %x", count, tag, msg.Key)
			}
		}
	}
	log.Debug("subFunc: done, tag: %s", tag)
}

func (msgs *appMessages)setMessageFrom(n *config.Node) {
	from := fmt.Sprintf("%x", n.ID)
	msgs.ev.From = from
	msgs.tx.From = from
	msgs.bh.From = from
	msgs.bk.From = from
}

func (msgs *appMessages)init() {
	msgs.ev.MsgType = yep2p.MessageTypeEvent
	msgs.tx.MsgType = yep2p.MessageTypeTx
	msgs.bh.MsgType = yep2p.MessageTypeBlockHeader
	msgs.bk.MsgType = yep2p.MessageTypeBlock
}

func (msgs *appMessages)yeChainProc(srv yep2p.Service) {
	yeChainProc(srv, msgs.ev, msgs.tx, msgs.bh, msgs.bk)
}

func (subs *appSubcribers)Register(srv yep2p.Service) {
	srv.Register(&subs.subEv)
	srv.Register(&subs.subTx)
	srv.Register(&subs.subBh)
}

func (subs *appSubcribers)yeChainStop(srv yep2p.Service) {
	yeChainStop(srv, subs.subEv, subs.subTx, subs.subBh)
}

func (subs *appSubcribers)init() {
	subs.subEv = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeEvent,
	}
	subs.subTx = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeTx,
	}
	subs.subBh = yep2p.Subscriber {
		MsgChan:	make(chan yep2p.Message, 64),
		MsgType:	yep2p.MessageTypeBlockHeader,
	}
}

func (subs *appSubcribers)goSubFunc() {
	go subFunc(subs.subEv, "ev")
	go subFunc(subs.subTx, "tx")
	go subFunc(subs.subBh, "bh")
}

func setMessageFrom(n *config.Node) {
	// "From" set here might be overlapped later, for a sub node identity
	// is more suitable for this this field. see EvShellBroadcastReq event
	// handler in file chainshell.go please.
	from := fmt.Sprintf("%x", n.ID)
	ev.From = from
	tx.From = from
	bh.From = from
	bk.From = from
}

func yeChainProc(yeShMgr yep2p.Service, ev yep2p.Message, tx yep2p.Message, bh yep2p.Message, bk yep2p.Message) {
	cnt := 0
	cnt_max := 100 * 100 * 100

	for cnt < cnt_max {
		cnt++
		now := time.Now().UnixNano()

		data := []byte(fmt.Sprintf("ev: %d", now))
		key := sha256.Sum256(data)
		ev.Data = data
		ev.Key = key[0:]
		yeShMgr.BroadcastMessageOsn(ev)

		data = []byte(fmt.Sprintf("tx: %d", now))
		key = sha256.Sum256(data)
		tx.Data = data
		tx.Key = key[0:]
		yeShMgr.BroadcastMessageOsn(tx)

		data = []byte(fmt.Sprintf("bh: %d", now))
		key = sha256.Sum256(data)
		bh.Data = data
		bh.Key = key[0:]
		yeShMgr.BroadcastMessageOsn(bh)

		data = []byte(fmt.Sprintf("bk: %d", now))
		key = sha256.Sum256(data)
		bk.Data = data
		bk.Key = key[0:]
		yeShMgr.BroadcastMessageOsn(bk)

		if cnt & 0x7f == 0 {
			log.Debug("yeChainProc: cnt: %d, loop BroadcastMessageOsn", cnt)
		}

		time.Sleep(time.Millisecond * 20 /*1000*/)
	}
}

func yeChainStop(yeShMgr yep2p.Service, subEv yep2p.Subscriber, subTx yep2p.Subscriber, subBh yep2p.Subscriber) {
	close(subEv.MsgChan)
	yeShMgr.UnRegister(&subEv)
	close(subTx.MsgChan)
	yeShMgr.UnRegister(&subTx)
	close(subBh.MsgChan)
	yeShMgr.UnRegister(&subBh)
	yeShMgr.Stop()
}

//
// testCase5
//
func testCase5(tc *testCase) {
	for loop := 0; loop < 100; loop++ {
		yesCfg := yep2p.DefaultYeShellConfig
		//yesCfg.BootstrapNode = true
		yesCfg.SubNetMaskBits = 0
		yeShMgr := yep2p.NewYeShellManager(&yesCfg)
		yeShMgr.Start()
		time.Sleep(time.Second * 60)
		yeShMgr.Stop()
	}
}

//
// testCase6
//
func testCase6(tc *testCase) {
	for loop := 0; loop < 100; loop++ {
		yesCfg := yep2p.DefaultYeShellConfig
		//yesCfg.BootstrapNode = true
		yesCfg.SubNetMaskBits = 4
		yeShMgr := yep2p.NewYeShellManager(&yesCfg)
		yeShMgr.Start()
		time.Sleep(time.Second * 120)
		yeShMgr.Stop()
	}
}

//
// testCase7
//
func testCase7(tc *testCase) {

	yesCfg := yep2p.DefaultYeShellConfig
	//yesCfg.BootstrapNode = true
	yesCfg.SubNetMaskBits = 0
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()

	node := yeShMgr.GetLocalNode()
	setMessageFrom(node)

	yeShMgr.Register(&subEv)
	yeShMgr.Register(&subTx)
	yeShMgr.Register(&subBh)

	go subFunc(subEv, "ev")
	go subFunc(subTx, "tx")
	go subFunc(subBh, "bh")

	if true {
		waitInterruptWithCallback(yeShMgr, yeChainProc, yeChainStop)
	} else {
		time.Sleep(time.Second * 10)
		yeChainProc(yeShMgr, ev, tx, bh, bk)
		yeChainStop(yeShMgr, subEv, subTx, subBh)
	}
}

//
// testCase8
//
func testCase8(tc *testCase) {

	yesCfg := yep2p.DefaultYeShellConfig
	//yesCfg.BootstrapNode = true
	yesCfg.SubNetMaskBits = 4
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()

	node := yeShMgr.GetLocalNode()
	setMessageFrom(node)

	yeShMgr.Register(&subEv)
	yeShMgr.Register(&subTx)
	yeShMgr.Register(&subBh)

	go subFunc(subEv, "ev")
	go subFunc(subTx, "tx")
	go subFunc(subBh, "bh")

	if true {
		waitInterruptWithCallback(yeShMgr, yeChainProc, yeChainStop)
	} else {
		time.Sleep(time.Second * 10)
		yeChainProc(yeShMgr, ev, tx, bh, bk)
		yeChainStop(yeShMgr, subEv, subTx, subBh)
	}
}

//
// testCase9
//
func testCase9(tc *testCase) {

	osnCfg := yep2p.DefaultYeShellConfig
	//osnCfg.BootstrapNode = true
	osnCfg.SubNetMaskBits = 0
	osnSrv, _ := yep2p.NewOsnService(&osnCfg)
	osnSrv.Start()

	node := osnSrv.GetLocalNode()
	setMessageFrom(node)

	osnSrv.Register(&subEv)
	osnSrv.Register(&subTx)
	osnSrv.Register(&subBh)

	go subFunc(subEv, "ev")
	go subFunc(subTx, "tx")
	go subFunc(subBh, "bh")

	if true {
		waitInterruptWithCallback(osnSrv, yeChainProc, yeChainStop)
	} else {
		time.Sleep(time.Second * 10)
		yeChainProc(osnSrv, ev, tx, bh, bk)
		yeChainStop(osnSrv, subEv, subTx, subBh)
	}
}

//
// testCase10
//
func testCase10(tc *testCase) {

	osnCfg := yep2p.DefaultYeShellConfig
	//osnCfg.BootstrapNode = true
	osnCfg.SubNetMaskBits = 4
	osnSrv, _ := yep2p.NewOsnService(&osnCfg)
	osnSrv.Start()

	node := osnSrv.GetLocalNode()
	setMessageFrom(node)

	osnSrv.Register(&subEv)
	osnSrv.Register(&subTx)
	osnSrv.Register(&subBh)

	go subFunc(subEv, "ev")
	go subFunc(subTx, "tx")
	go subFunc(subBh, "bh")

	if true {
		waitInterruptWithCallback(osnSrv, yeChainProc, yeChainStop)
	} else {
		time.Sleep(time.Second * 10)
		yeChainProc(osnSrv, ev, tx, bh, bk)
		yeChainStop(osnSrv, subEv, subTx, subBh)
	}
}

//
// testCase11
//
func testCase11(tc *testCase) {
	appInstNum := 8
	appMsgs := make([]*appMessages, appInstNum)
	appSubs := make([]*appSubcribers, appInstNum)
	bsSrv := (*yep2p.OsnService)(nil)
	osnsTab := make([]*yep2p.OsnService, appInstNum)

	for idx := 0; idx < appInstNum; idx++ {
		osnCfg := yep2p.DefaultYeShellConfig
		osnCfg.Name = fmt.Sprintf("%s%d", "chain_", idx)
		osnCfg.LocalUdpPort += uint16(idx)
		osnCfg.LocalTcpPort += uint16(idx)
		osnCfg.LocalDhtPort += uint16(idx)
		osnCfg.BootstrapNode = idx == 0

		if idx == 0 {
			osns, err := yep2p.NewOsnService(&osnCfg)
			if err != nil || osns == nil {
				log.Debug("testCase10: NewOsnService failed, err: %s", err.Error())
				return
			}
			bsSrv = osns
			osnsTab[idx] = osns
		} else {
			bsn := bsSrv.GetLocalNode()
			bsnStr := fmt.Sprintf("%X@%s:%d:%d", bsn.ID, bsn.IP.String(), bsn.UDP, bsn.TCP)
			osnCfg.BootstrapNodes = make([]string, 0)
			osnCfg.BootstrapNodes = append(osnCfg.BootstrapNodes, bsnStr)
			dhtBsn := bsSrv.GetLocalDhtNode()
			dhtBsnStr := fmt.Sprintf("%X@%s:%d:%d", dhtBsn.ID, dhtBsn.IP.String(), dhtBsn.UDP, dhtBsn.TCP)
			osnCfg.DhtBootstrapNodes = make([]string, 0)
			osnCfg.DhtBootstrapNodes = append(osnCfg.DhtBootstrapNodes, dhtBsnStr)
			osns, err := yep2p.NewOsnService(&osnCfg)
			if err != nil || osns == nil {
				log.Debug("testCase10: NewOsnService failed, err: %s", err.Error())
				return
			}
			osnsTab[idx] = osns
		}

		osnsTab[idx].Start()
		appMsgs[idx] = new(appMessages)
		appMsgs[idx].init()
		appSubs[idx] = new(appSubcribers)
		appSubs[idx].init()
		appSubs[idx].Register(osnsTab[idx])
		appSubs[idx].goSubFunc()

		if false {
			go func() {
				appMsgs[idx].yeChainProc(osnsTab[idx])
				appSubs[idx].yeChainStop(osnsTab[idx])
			}()
		}
	}

	waitInterrupt()
}