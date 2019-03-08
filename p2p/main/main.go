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
	"syscall"
	"bytes"
	"strconv"
	"runtime"
	"os/signal"
	"crypto/sha256"
	_ "net/http/pprof"
	"net/http"
	config	"github.com/yeeco/gyee/p2p/config"
	log		"github.com/yeeco/gyee/p2p/logger"
	yep2p	"github.com/yeeco/gyee/p2p"
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
		name:			"testCase0",
		description:	"bootstrap node with SubNetMaskBits == 0",
		entry:			testCase0,
	},
	{
		name:			"testCase0Ex",
		description:	"bootstrap node with (SubNetMaskBits == 4, and Validator == true)",
		entry:			testCase0Ex,
	},
	{
		name:			"testCase1",
		description:	"little-white node, SubNetMaskBits == 4",
		entry:			testCase1,
	},
	{
		name:			"testCase2",
		description:	"little-white node, start/stop, SubNetMaskBits == 4",
		entry:			testCase2,
	},
	{
		name:			"testCase3",
		description:	"little-white node, broadcast, SubNetMaskBits == 4",
		entry:			testCase3,
	},
	{
		name:			"testCase4",
		description:	"little-white node, SubNetMaskBits == 0",
		entry:			testCase4,
	},
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
		description:	"yee chain test, SubNetMaskBits != 0",
		entry:			testCase8,
	},
	{
		name:			"testCase9",
		description:	"yee dht test",
		entry:			testCase9,
	},
	{
		name:			"testCase10",
		description:	"yee chain OSN test, SubNetMaskBits == 0",
		entry:			testCase10,
	},
	{
		name:			"testCase11",
		description:	"yee chain OSN test, SubNetMaskBits != 0",
		entry:			testCase11,
	},
	{
		name:			"testCase12",
		description:	"multiple yee chain OSN test on one host",
		entry:			testCase12,
	},
	{
		name:			"testCase13",
		description:	"reconfiguration: validator => little-white with same mask bits",
		entry:			testCase13,
	},
	{
		name:			"testCase14",
		description:	"reconfiguration: little-white => validator with same mask bits",
		entry:			testCase14,
	},
	{
		name:			"testCase15",
		description:	"reconfiguration: little-white => validator with defferent mask bits",
		entry:			testCase15,
	},
	{
		name:			"testCase16",
		description:	"reconfiguration: nat",
		entry:			testCase16,
	},
	{
		name:			"testCase17",
		description:	"reconfiguration: traffic with nat",
		entry:			testCase17,
	},
}

//
// target case
//
var targetCase = "testCase17"

//
// switch for playing go-monitors, related commands:
// >> go tool pprof http://localhost:6060/debug/pprof/heap
// >> curl localhost:6060/goroutines
// see comments in function startGoProfile for more pls.
//
const goMonitors = true

//
// run target case
//
func main() {
	if goMonitors {
		startGoProfile()
		startGoRoutinesMonitor()
	}
	for _, tc := range testCaseTable {
		if tc.name == targetCase {
			tc.entry(&tc)
			return
		}
	}
	log.Debug("main: target case not found: %s", targetCase)
}

func startGoProfile() {
	// profile all, such as memory, cpu, ... example client commands like:
	//	http://localhost:6060/debug/pprof/heap
	//	http://localhost:6060/debug/pprof/profile
	//	http://localhost:6060/debug/pprof/trace
	//	http://localhost:6060/debug/pprof/cmdline
	//	http://localhost:6060/debug/pprof/goroutine
	//	http://localhost:6060debug/pprof/symbol
	//	...
	// please query the web for more. and notice that "localhost:6060" is
	// shown just as an example.
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func startGoRoutinesMonitor() {
	// call runtime interface to get routine number when requested by http
	go func() {
		http.HandleFunc("/goroutines", func(w http.ResponseWriter, r *http.Request) {
			num := strconv.FormatInt(int64(runtime.NumGoroutine()), 10)
			w.Write([]byte(num))
		})
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func waitInterrupt() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigc)
	<-sigc
}

func waitInterrupt2Stop(mgr interface{}) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
	if yesMgr, ok := mgr.(*yep2p.YeShellManager); ok {
		yesMgr.Stop()
	}
}

func waitInterruptWithCallback(srv yep2p.Service, workp appWorkProc, stopp appStopProc) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	defer signal.Stop(sigc)
	<-sigc
	if workp != nil {
		workp(srv, ev, tx, bh, bk)
	}
	if stopp != nil {
		stopp(srv, subEv, subTx, subBh)
	}
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
		ev.Data = append(ev.Data[0:0], data...)
		key := sha256.Sum256(data)
		ev.Key = append(ev.Key[0:0], key[0:]...)
		yeShMgr.BroadcastMessageOsn(ev)

		data = []byte(fmt.Sprintf("tx: %d", now))
		tx.Data = append(tx.Data[0:0], data...)
		key = sha256.Sum256(data)
		tx.Key = append(tx.Key[0:0], key[0:]...)
		yeShMgr.BroadcastMessageOsn(tx)

		data = []byte(fmt.Sprintf("bh: %d", now))
		bh.Data = append(bh.Data[0:0], data...)
		key = sha256.Sum256(data)
		bh.Key = append(bh.Key[0:0], key[0:]...)
		yeShMgr.BroadcastMessageOsn(bh)

		data = []byte(fmt.Sprintf("bk: %d", now))
		bk.Data = append(bk.Data[0:0], data...)
		key = sha256.Sum256(data)
		bk.Key = append(bk.Key[0:0], key[0:]...)
		yeShMgr.BroadcastMessageOsn(bk)

		if cnt & 0x7f == 0 {
			log.Debug("yeChainProc: cnt: %d, loop BroadcastMessageOsn", cnt)
		}

		time.Sleep(time.Millisecond * 20)
		//time.Sleep(time.Millisecond * 50)
		//time.Sleep(time.Millisecond * 100)
		//time.Sleep(time.Millisecond * 1000)
	}
}

func yeChainProcEx(yeShMgr yep2p.Service, ev yep2p.Message, tx yep2p.Message, bh yep2p.Message, bk yep2p.Message, done chan bool) {
	if yeShMgr == nil {
		return
	}

	go func() {
		cnt := 0
		for {
			select {
			case <-done:
				return
			default:
			}

			cnt++
			now := time.Now().UnixNano()

			data := []byte(fmt.Sprintf("ev: %d", now))
			ev.Data = append(ev.Data[0:0], data...)
			key := sha256.Sum256(data)
			ev.Key = append(ev.Key[0:0], key[0:]...)
			yeShMgr.BroadcastMessageOsn(ev)

			data = []byte(fmt.Sprintf("tx: %d", now))
			tx.Data = append(tx.Data[0:0], data...)
			key = sha256.Sum256(data)
			tx.Key = append(tx.Key[0:0], key[0:]...)
			yeShMgr.BroadcastMessageOsn(tx)

			data = []byte(fmt.Sprintf("bh: %d", now))
			bh.Data = append(bh.Data[0:0], data...)
			key = sha256.Sum256(data)
			bh.Key = append(bh.Key[0:0], key[0:]...)
			yeShMgr.BroadcastMessageOsn(bh)

			data = []byte(fmt.Sprintf("bk: %d", now))
			bk.Data = append(bk.Data[0:0], data...)
			key = sha256.Sum256(data)
			bk.Key = append(bk.Key[0:0], key[0:]...)
			yeShMgr.BroadcastMessageOsn(bk)

			if cnt&0x7f == 0 {
				log.Debug("yeChainProc: cnt: %d, loop BroadcastMessageOsn", cnt)
			}

			time.Sleep(time.Millisecond * 20)
			//time.Sleep(time.Millisecond * 50)
			//time.Sleep(time.Millisecond * 100)
			//time.Sleep(time.Millisecond * 1000)
		}
	}()
}

func yeDhtProc(yeShMgr yep2p.Service, ev yep2p.Message, tx yep2p.Message, bh yep2p.Message, bk yep2p.Message) {
	cnt := 0
	cnt_max := 100 * 100 * 100

	for cnt < cnt_max {
		cnt++

		now := time.Now().UnixNano()
		data := []byte(fmt.Sprintf("bk: %d", now))
		bk.Data = append(bk.Data[0:0], data...)
		key := sha256.Sum256(data)
		bk.Key = append(bk.Key[0:0], key[0:]...)

		if err := yeShMgr.DhtSetValue(bk.Key, bk.Data); err != nil {

			log.Debug("yeDhtProc: DhtSetValue failed, err: %s", err.Error())

		} else {

			log.Debug("yeDhtProc: value put:\n\tkey: %x\n\tvalue: %x", bk.Key, bk.Data)
			time.Sleep(time.Millisecond * 1000)

			if val, err := yeShMgr.DhtGetValue(bk.Key); err != nil {

				log.Debug("yeDhtProc: DhtGetValue failed, err: %s", err.Error())

			} else {

				log.Debug("yeDhtProc: value got:\n\tkey: %x\n\tvalue: %x", bk.Key, val)

				if bytes.Compare(bk.Data, val) != 0 {
					log.Debug("yeDhtProc: value mismatched")
				}
			}

			time.Sleep(time.Millisecond * 2000)
		}

		if cnt & 0x7f == 0 {
			log.Debug("yeDhtProc: cnt: %d, loop BroadcastMessageOsn", cnt)
		}
	}
}

func yeChainStop(yeShMgr yep2p.Service, subEv yep2p.Subscriber, subTx yep2p.Subscriber, subBh yep2p.Subscriber) {
	yeShMgr.Stop()
	close(subEv.MsgChan)
	yeShMgr.UnRegister(&subEv)
	close(subTx.MsgChan)
	yeShMgr.UnRegister(&subTx)
	close(subBh.MsgChan)
	yeShMgr.UnRegister(&subBh)
}

func yeChainStopEx(yeShMgr yep2p.Service, subEv yep2p.Subscriber, subTx yep2p.Subscriber, subBh yep2p.Subscriber, done chan bool) {
	close(done)
	yeChainStop(yeShMgr, subEv, subTx, subBh )
}


//
// testCase0
//
func testCase0(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.BootstrapNode = true
	yesCfg.SubNetMaskBits = 0
	yesCfg.LocalNodeIp = "192.168.1.109"
	yesCfg.LocalDhtIp = "192.168.1.109"
	yesCfg.NatType = config.NATT_NONE
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()
	waitInterrupt()
	yeShMgr.Stop()
}

//
// testCase0Ex
//
func testCase0Ex(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.BootstrapNode = true
	yesCfg.SubNetMaskBits = 4
	yesCfg.LocalNodeIp = "192.168.1.109"
	yesCfg.LocalDhtIp = "192.168.1.109"
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()
	waitInterrupt()
	yeShMgr.Stop()
}

//
// testCase1
//
func testCase1(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.Validator = false
	yesCfg.BootstrapNode = false
	yesCfg.SubNetMaskBits = 4
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()
	waitInterrupt()
	yeShMgr.Stop()
}

//
// testCase2
//
func testCase2(tc *testCase) {
	for loop := 0; loop < 100; loop++ {
		yesCfg := yep2p.DefaultYeShellConfig
		yesCfg.Validator = false
		yesCfg.BootstrapNode = false
		yesCfg.SubNetMaskBits = 4
		yeShMgr := yep2p.NewYeShellManager(&yesCfg)
		yeShMgr.Start()
		time.Sleep(time.Second * 60)
		yeShMgr.Stop()
		log.Debug("\n\n\n testCase2: loop: %d", loop)
	}
}

//
// testCase3
//
func testCase3(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.Validator = false
	yesCfg.BootstrapNode = false
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
// testCase4
//
func testCase4(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.Validator = false
	yesCfg.BootstrapNode = false
	yesCfg.SubNetMaskBits = 0
	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()
	waitInterrupt()
	yeShMgr.Stop()
}

//
// testCase5
//
func testCase5(tc *testCase) {
	for loop := 0; loop < 100; loop++ {
		yesCfg := yep2p.DefaultYeShellConfig
		yesCfg.SubNetMaskBits = 0
		yeShMgr := yep2p.NewYeShellManager(&yesCfg)
		yeShMgr.Start()
		time.Sleep(time.Second * 60)
		yeShMgr.Stop()
		log.Debug("\n\n\n testCase5: loop: %d", loop)
	}
}

//
// testCase6
//
func testCase6(tc *testCase) {
	for loop := 0; loop < 100; loop++ {
		yesCfg := yep2p.DefaultYeShellConfig
		yesCfg.SubNetMaskBits = 4
		yeShMgr := yep2p.NewYeShellManager(&yesCfg)
		yeShMgr.Start()
		time.Sleep(time.Second * 120)
		yeShMgr.Stop()
		log.Debug("\n\n\n testCase6: loop: %d", loop)
	}
}

//
// testCase7
//
func testCase7(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
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

	if false {
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
	yesCfg := yep2p.DefaultYeShellConfig
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

	if false {
		waitInterruptWithCallback(yeShMgr, yeDhtProc, yeChainStop)
	} else {
		time.Sleep(time.Second * 10)
		yeDhtProc(yeShMgr, ev, tx, bh, bk)
		yeChainStop(yeShMgr, subEv, subTx, subBh)
	}
}

//
// testCase10
//
func testCase10(tc *testCase) {
	osnCfg := yep2p.DefaultYeShellConfig
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
// testCase11
//
func testCase11(tc *testCase) {
	osnCfg := yep2p.DefaultYeShellConfig
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
// testCase12
//
func testCase12(tc *testCase) {
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

//
// testCase13
//
func testCase13(tc *testCase) {
	osnCfg := yep2p.DefaultYeShellConfig
	osnCfg.Validator = true
	osnCfg.SubNetMaskBits = 4
	osnSrv, _ := yep2p.NewOsnService(&osnCfg)
	osnSrv.Start()

	if true {
		time.Sleep(time.Second * 20)
		reCfgCmd := yep2p.RecfgCommand{
			Validator:      false,
			SubnetMaskBits: 4,
		}
		if err := osnSrv.Reconfig(&reCfgCmd); err != nil {
			log.Debug("testCase13: Reconfig failed, err: %s", err.Error())
			return
		}
	}

	waitInterruptWithCallback(osnSrv, nil, yeChainStop)
}

//
// testCase14
//
func testCase14(tc *testCase) {
	osnCfg := yep2p.DefaultYeShellConfig
	osnCfg.Validator = false
	osnCfg.SubNetMaskBits = 4
	osnSrv, _ := yep2p.NewOsnService(&osnCfg)
	osnSrv.Start()

	if false {
		time.Sleep(time.Second * 10)
		reCfgCmd := yep2p.RecfgCommand{
			Validator:      true,
			SubnetMaskBits: 4,
		}
		if err := osnSrv.Reconfig(&reCfgCmd); err != nil {
			log.Debug("testCase14: Reconfig failed, err: %s", err.Error())
			return
		}
	}

	waitInterruptWithCallback(osnSrv, nil, yeChainStop)
}

//
// testCase15
//
func testCase15(tc *testCase) {
	osnCfg := yep2p.DefaultYeShellConfig
	osnCfg.Validator = false
	osnCfg.SubNetMaskBits = 2
	osnSrv, _ := yep2p.NewOsnService(&osnCfg)
	osnSrv.Start()

	if false {
		time.Sleep(time.Second * 10)
		reCfgCmd := yep2p.RecfgCommand{
			Validator:      true,
			SubnetMaskBits: 4,
		}
		if err := osnSrv.Reconfig(&reCfgCmd); err != nil {
			log.Debug("testCase15: Reconfig failed, err: %s", err.Error())
			return
		}
	}

	waitInterruptWithCallback(osnSrv, nil, yeChainStop)
}

//
// testCase16
//
func testCase16(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.Validator = true
	yesCfg.BootstrapNode = false
	yesCfg.SubNetMaskBits = 4

	//yesCfg.NatType = config.NATT_NONE
	//yesCfg.NatType = config.NATT_PMP
	//yesCfg.GatewayIp = "192.168.1.1"
	yesCfg.NatType = config.NATT_UPNP
	//yesCfg.NatType = config.NATT_ANY

	yeShMgr := yep2p.NewYeShellManager(&yesCfg)
	yeShMgr.Start()
	waitInterrupt()
	yeShMgr.Stop()
}

//
// testCase17
//
func testCase17(tc *testCase) {
	yesCfg := yep2p.DefaultYeShellConfig
	yesCfg.Validator = true
	yesCfg.BootstrapNode = false
	yesCfg.SubNetMaskBits = 4

	//yesCfg.NatType = config.NATT_NONE
	//yesCfg.NatType = config.NATT_PMP
	yesCfg.NatType = config.NATT_UPNP
	//yesCfg.NatType = config.NATT_ANY

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

	done := make(chan bool)
	if false {
		waitInterruptWithCallback(yeShMgr, yeChainProc, yeChainStop)
	} else if true {
		yeChainProcEx(yeShMgr, ev, tx, bh, bk, done)
		waitInterrupt()
		yeChainStopEx(yeShMgr, subEv, subTx, subBh, done)
	} else {
		go yeChainProc(yeShMgr, ev, tx, bh, bk)
		time.Sleep(time.Second * 60)
		yeChainStop(yeShMgr, subEv, subTx, subBh)
	}
}