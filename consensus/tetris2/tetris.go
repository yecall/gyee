// Copyright (C) 2018 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package tetris2

/*
9. 需解决的工程问题：1）点火问题。2）区块同步效率问题。3）成员替换时的know清单问题。4）内存释放问题。5）fork检测后操作问题。6)调速问题。
   7）metrics。8）块确认时中断问题。9）crash-recovery处理。10）上层协议member rotation。11）更换m的设计，直接用地址，hash用[]byte
   12) H变化时中断开始新一轮共识
*/

import (
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/utils"
	"github.com/yeeco/gyee/utils/logging"
)

var HASH0 = [common.HashLength]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type SyncRequest struct {
	count int
	time  time.Time
}

type Tetris struct {
	core   ICore
	signer crypto.Signer

	vid string //My vid，using the validator address string
	h   uint64 //Current Block Height
	n   uint64 //Current Sequence Number for processing

	validators       map[string]map[uint64]*Event //key: validator address, value map of event of n
	validatorsHeight map[string]uint64            //validator's current height
	witness          []map[string]*Event          //witness of each round.

	//Input Channel
	EventCh       chan []byte
	ParentEventCh chan []byte
	TxsCh         chan common.Hash

	//Output Channel
	OutputCh       chan *ConsensusOutput
	SendEventCh    chan []byte
	RequestEventCh chan common.Hash

	eventCache    *utils.LRU //收到过的events，用于去重
	eventAccepted []*Event   //本次已经accept的events列表，发送event的时候，作为parent列表
	eventPending  *utils.LRU //因parents未到还没有加入到tetris的events
	eventRequest  *utils.LRU //请求过的event
	//eventRequest  map[string]SyncRequest

	txsCache     *utils.LRU                        //收到过的transactions，用于去重
	txsAccepted  []common.Hash                     //本次收到的txs列表
	txsCommitted *utils.LRU                        //已经进入了共识的tranactions，用于去重
	txsPending   map[common.Hash]map[string]uint64 //交易在哪个member的哪个高度出现过, key1: hash of tx, key2:vid, value:height

	//currentEvent Event
	ticker    *time.Ticker
	heartBeat map[string]time.Time //从不同validators收到消息的最近时间，key:vid

	//params
	params *Params

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup

	//time
	lastSendTime time.Time

	//metrics or test
	requestCount   int
	parentCountRaw int
	parentCount    int
	eventCount     int
	txCount        int
	TrafficIn      int
	TrafficOut     int

	//request map[string]int
	level       int
	noRecvCount int
}

func NewTetris(core ICore, vid string, validatorList []string, blockHeight uint64) (*Tetris, error) {
	tetris := Tetris{
		core:             core,
		vid:              vid,
		h:                blockHeight,
		n:                blockHeight + 1,
		validators:       make(map[string]map[uint64]*Event, len(validatorList)),
		validatorsHeight: make(map[string]uint64, len(validatorList)),

		EventCh:       make(chan []byte, 100),
		ParentEventCh: make(chan []byte, 100),
		TxsCh:         make(chan common.Hash, 100000),

		OutputCh:       make(chan *ConsensusOutput, 10), //TODO：这种指针可能有问题吧？
		SendEventCh:    make(chan []byte, 100),
		RequestEventCh: make(chan common.Hash, 100),

		eventCache:    utils.NewLRU(10000, nil),
		eventAccepted: make([]*Event, 0),
		eventPending:  utils.NewLRU(10000, nil),
		eventRequest:  utils.NewLRU(10000, nil),

		txsCache:     utils.NewLRU(10000, nil),
		txsAccepted:  make([]common.Hash, 0),
		txsPending:   make(map[common.Hash]map[string]uint64),
		txsCommitted: utils.NewLRU(100000, nil),

		ticker:    time.NewTicker(1 * time.Second),
		heartBeat: make(map[string]time.Time),

		quitCh: make(chan struct{}),

		lastSendTime: time.Now(),
	}

	tetris.signer = core.GetSigner()
	pk, _ := core.GetPrivateKeyOfDefaultAccount()
	tetris.signer.InitSigner(pk) //TODO: 初始化要取缺省账户的私钥

	for _, value := range validatorList {
		tetris.validators[value] = make(map[uint64]*Event)
	}

	tetris.params = &Params{
		f:                 (len(validatorList) - 1) / 3,
		superMajority:     2*len(validatorList)/3 + 1,
		maxTxPerEvent:     2000,
		minTxPerEvent:     1,
		maxEventPerEvent:  len(validatorList),
		minEventPerEvent:  2,
		maxPeriodForEvent: 2000,
		minPeriodForEvent: 20,
		maxSunk:           0,
	}

	tetris.prepare()

	return &tetris, nil
}

func (t *Tetris) MajorityBeatTime() (ok bool, duration time.Duration) {
	if len(t.heartBeat) < t.params.superMajority-1 {
		return false, 0
	}

	var times []time.Time
	for _, time := range t.heartBeat {
		times = append(times, time)
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	now := time.Now()
	return true, now.Sub(times[t.params.superMajority-2])
}

func (t *Tetris) MemberRotate(joins []string, quits []string) {
	for _, vid := range quits {
		delete(t.validators, vid)
		delete(t.validatorsHeight, vid)
		delete(t.heartBeat, vid)
	}

	for _, vid := range joins {
		t.validators[vid] = make(map[uint64]*Event)
	}
}

func (t *Tetris) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	//logging.Logger.Info("Tetris Start...")
	go t.loop()
	return nil
}

func (t *Tetris) Stop() error { //TODO: stop是channel中还有消息怎么处理？需要关闭么？
	t.lock.Lock()
	defer t.lock.Unlock()
	//logging.Logger.Info("Tetris Stop...")
	close(t.quitCh)
	t.wg.Wait()
	return nil
}

func (t *Tetris) loop() {
	t.wg.Add(1)
	defer t.wg.Done()
	//logging.Logger.Info("Tetris loop...")
	for {
		select {
		case <-t.quitCh:
			//logging.Logger.Info("Tetris loop end.")
			return
		case eventMsg := <-t.EventCh:
			var event Event
			t.TrafficIn += len(eventMsg)
			event.Unmarshal(eventMsg)
			if t.checkEvent(&event) {
				t.heartBeat[event.vid] = time.Now()
				if event.Body.P { //如果是心跳，更新心跳时间表
					//logging.Logger.Info("receive heartbeat:", event.vid)
				} else {
					t.receiveEvent(&event)
				}
			}
		case eventMsg := <-t.ParentEventCh:
			var event Event
			t.TrafficIn += len(eventMsg)
			event.Unmarshal(eventMsg)
			if t.checkEvent(&event) {
				t.receiveParentEvent(&event)
			}
		case tx := <-t.TxsCh:
			t.TrafficIn += len(tx)
			t.receiveTx(tx)
		case time := <-t.ticker.C:
			t.receiveTicker(time)
		}
	}
}

//Receive transaction send to me
/*
发送速率控制，tx数到上限，间隔时间>min, event数目>min
没有tx的时候，如果有未确认的，继续低速。如果已全部确认，保持最低速10分钟出一块。
不同member的tx的同步问题，f+1个member同时存在，可以在当前h下面多深还有效？
*/

func (t *Tetris) sendPlaceholderEvent() {
	event := NewEvent(t.vid, t.h, t.n)
	event.AddSelfParent(t.validators[t.vid][t.n-1])
	event.ready = true
	event.Sign(t.signer)

	t.SendEventCh <- event.Marshal()
	t.validators[t.vid][t.n] = event
	t.validatorsHeight[t.vid] = t.n
	t.eventCache.Add(event.Hash(), event)

	t.lastSendTime = time.Now()

	t.n++

	t.update(event, false)
}

func (t *Tetris) sendEvent() {
	//logging.Logger.Info("sendEvent:", t.vid, "txs:",len(t.txsAccepted))
	event := NewEvent(t.vid, t.h, t.n)
	event.AddSelfParent(t.validators[t.vid][t.n-1])
	event.AddParents(t.eventAccepted)
	event.AddTransactions(t.txsAccepted)
	event.ready = true
	event.Sign(t.signer)
	eb := event.Marshal()
	t.TrafficOut += len(eb)
	t.SendEventCh <- eb
	t.validators[t.vid][t.n] = event
	t.validatorsHeight[t.vid] = t.n
	t.eventCache.Add(event.Hash(), event)

	t.lastSendTime = time.Now()
	t.eventAccepted = make([]*Event, 0)
	t.txsAccepted = make([]common.Hash, 0)

	t.n++

	t.update(event, false)
}

func (t *Tetris) sendHeartbeat() {
	pulse := NewPulse()
	pulse.Sign(t.signer)
	pb := pulse.Marshal()
	t.TrafficOut += len(pb)
	t.SendEventCh <- pb
}

func (t *Tetris) receiveTicker(ttime time.Time) {
	if ttime.Sub(t.lastSendTime) > 1*time.Second {
		t.sendHeartbeat()
	}
	//if strings.Contains(t.vid, "3038") || strings.Contains(t.vid, "3039") {
	//	fmt.Println()
	//	fmt.Println(t.vid[2:4], "------>")
	//	fmt.Println("sendEventCh:",len(t.SendEventCh), "requestEventCh:", len(t.RequestEventCh), "outputCh:", len(t.OutputCh))
	//	fmt.Println("EventCh:", len(t.EventCh), "TxsCh:", len(t.TxsCh), "ParentCh:",len(t.ParentEventCh))
	//}
}

func (t *Tetris) receiveTx(tx common.Hash) {
	if !t.txsCache.Contains(tx) {
		t.txCount++
		if t.txCount%30000 == 0 {
			logging.Logger.WithFields(logrus.Fields{
				"vid":     t.vid[0:4],
				"n":       t.n,
				"c":       t.txCount,
				"pending": t.eventPending.Len(),
			}).Info("Tx count")
		}
		t.txsCache.Add(tx, true)
		t.txsAccepted = append(t.txsAccepted, tx)

		if len(t.txsAccepted) > t.params.maxTxPerEvent {
			ok, _ := t.MajorityBeatTime()
			if ok {
				t.sendEvent()
			} else {
				t.txsAccepted = t.txsAccepted[10:]
			}
		}
	}
}

func (t *Tetris) checkEvent(event *Event) bool {
	pk, err := event.RecoverPublicKey(t.signer)
	if err != nil {
		logging.Logger.Warn("event check error.", err)
		return false
	}

	//TODO：从公钥计算address vid
	addr, err := t.core.AddressFromPublicKey(pk)
	event.vid = hex.EncodeToString(addr)
	//TODO: 检查是否属于当前validators

	ret := event.SignVerify(pk, t.signer)

	return ret
}

//Receive Event, if the parents not exists, send request
func (t *Tetris) receiveEvent(event *Event) {
	//event要来自合法的validator
	//比较event的h，及n。如果h比自己大，说明需要同步区块，如果n比自己大，需要发新event，
	//要检测是否恶意快速发送，需要核对min，max
	//t.currentEvent.appendEvent(event)
	//放入cache
	//放入unsettled，检查parent是否有到齐
	//放入currentEvent
	t.eventCount++
	//if t.eventCount % 100 == 0 {
	//	logging.Logger.WithFields(logrus.Fields{
	//		"tm":     t.m,
	//		"tn":     t.n,
	//		"eventCh": len(t.EventCh),
	//		"sendCh": len(t.SendEventCh),
	//		"txCh": len(t.TxsCh),
	//		"c":      t.eventCount,
	//		"pending": t.eventPending.Len(),
	//	}).Info("Event count")
	//}

	if t.eventCache.Contains(event.Hash()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hash(),
			"vid":   event.vid,
			"n":     event.Body.N,
		}).Debug("Recevie already existed event")

		return
	}

	//logging.Logger.WithFields(logrus.Fields{
	//	"m":  event.Body.M,
	//	"n": event.Body.N,
	//}).Info("receive event")

	if event.Body.H > t.h {
		//自己的区块高度已低于对方，需要同步区块
		//但如果这个地方自己总是慢而不采用共识输出，最终半数的签名数可能要不够？
		//return
	}
	if event.Body.N <= t.h {
		//过久以前的消息，可以丢弃
		return
	}

	t.eventPending.Add(event.Hash(), event)
	t.addReceivedEventToTetris(event)
}

func (t *Tetris) receiveParentEvent(event *Event) {
	t.parentCountRaw++

	if t.eventCache.Contains(event.Hash()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hash(),
			"vid":   event.vid,
			"n":     event.Body.N,
		}).Debug("Recevie already existed parent event")
		return
	}
	//logging.Logger.WithFields(logrus.Fields{
	//	"m":  event.Body.M,
	//	"n": event.Body.N,
	//}).Info("receive parent event")
	t.parentCount++
	//if t.parentCount % 300 == 0 {
	//	logging.Logger.WithFields(logrus.Fields{
	//		"em":     event.Body.M,
	//		"en":     event.Body.N,
	//		"tm":     t.m,
	//		"tn":     t.n,
	//		"c":      t.parentCount,
	//		"pending": t.eventPending.Len(),
	//	}).Info("Parent count")
	//}
	t.addReceivedEventToTetris(event)
}

func (t *Tetris) addReceivedEventToTetris(event *Event) {
	me := t.validators[event.vid][event.Body.N]
	if me != nil {
		//fmt.Println("*****")
		//fmt.Println(me.vid, me.Body.N, me.Hash(), t.vid)
		//fmt.Println(event.vid, event.Body.N, event.Hash())
		if me.Hash() != event.Hash() {
			logging.Logger.WithFields(logrus.Fields{
				"event": event.Hash(),
				"me":    me.Hash(),
			}).Warn("Receive event with different hash, fork detected.")
		}
		return
	}

	event.know = make(map[string]uint64)
	event.round = ROUND_UNDECIDED
	event.witness = false
	event.vote = 0
	event.committable = COMMITTABLE_UNDECIDED
	t.eventCache.Add(event.Hash(), event)
	/*
		新来一个event，可能是request来的，也可能是pending的
		检查event的parent是否都在，如果有不在的，发request
		宽度优先搜索parent的parent，如果有不在的，发request。这种网格结构深度优先算法效率低。
		直到event不但在，而且在member height之下。
		这个可以搞个时间控制限制执行频率
		这里有个问题是一个member一直没上，最后发一条event出来，其他member要等很长时间才能将它同步
		pending要搞个超时丢弃
	*/

	unpending := make([]*Event, 0)
	for _, key := range t.eventPending.Keys() {
		ei, ok := t.eventPending.Get(key)
		if ok {
			ev := ei.(*Event)

			bsl := make(map[common.Hash]*Event)
			bsl[ev.Hash()] = ev
			bs := append([]map[common.Hash]*Event{}, bsl)
			l := 0
			//广度优先搜索到level中所有的event或者不存在，或者都在member中为止
			for {
				currentL := bs[l]
				nextL := make(map[common.Hash]*Event)
				//当前level的events, 如果都在member中，结束。
				allReady := true
				for _, e := range currentL {
					//如果event都在member中，
					if !e.ready {
						allReady = false
						for _, peh := range e.Body.E {
							if peh == HASH0 {
								continue
							}
							//如果parent还没到，request，如果到了，加入下一level列表
							pei, ok := t.eventCache.Get(peh)
							if ok {
								pe := pei.(*Event)
								if pe.Body.N+uint64(t.params.maxSunk) > t.h {
									nextL[peh] = pe
								}
							} else {
								//request
								eri, ok := t.eventRequest.Get(peh)
								if ok {
									er := eri.(*SyncRequest)
									er.count++
								} else {
									t.RequestEventCh <- peh
									t.TrafficOut += 32
									t.eventRequest.Add(peh, &SyncRequest{count: 1, time: time.Now()})
								}
							}
						}
					}
				}
				if allReady || len(nextL) == 0 {
					break
				}

				//计算下一level events，如果空，结束。
				l++
				bs = append(bs, nextL)
			}

			for li := l; li >= 0; li-- {
				//l层如果ready了，
				currentL := bs[li]
				for _, e := range currentL {
					if !e.ready {
						pAllReady := true
						for _, peh := range e.Body.E {
							if peh == HASH0 {
								e.updateKnow(nil)
								continue
							}
							//如果parent还没到，request，如果到了，加入下一level列表
							pei, ok := t.eventCache.Get(peh)
							if ok {
								pe := pei.(*Event)
								if !pe.ready {
									pAllReady = false
								} else {
									e.updateKnow(pe)
								}

							} else {
								pAllReady = false
								//e.updateKnow(nil) //这个可能不一定需要
							}
						}
						if pAllReady {
							e.ready = true
							t.validators[e.vid][e.Body.N] = e
							t.validatorsHeight[e.vid] = e.Body.N
							//t.memberHeight[e.Body.M] = e.Body.N
							if t.eventPending.Contains(e.Hash()) {
								//if t.currentEvent.Body.N-e.Body.N < 20 { //试验
								if e.Body.N >= t.n {
									//对方的seq num超过本地，本地需要发出Event并生成新的currentEvent
									//
									//logging.Logger.WithFields(logrus.Fields{
									//	"t.m": t.m,
									//	"e.m": e.Body.M,
									//	"t.n": t.n,
									//	"e.n": e.Body.N,
									//}).Info("event.n >= t.n")

									for e.Body.N >= t.n {
										t.sendPlaceholderEvent()
									}
								}

								t.eventAccepted = append(t.eventAccepted, e)
								t.eventPending.Remove(e.Hash())
								//unpending = true
							}
							unpending = append(unpending, e)
						}
					}
				}
			}
		}
	}

	//如果有event已经可以被加入到tetris中，update一次状态
	if len(unpending) > 0 {
		sort.Slice(unpending, func(i, j int) bool {
			return unpending[i].Body.N < unpending[j].Body.N
		})

		newWitness := false
		for _, event := range unpending {
			//fmt.Print("    t:", t.m, " n:", event.Body.N, " m:", event.Body.M)
			if t.update(event, true) {
				newWitness = true
			}
		}

		if newWitness {
			t.consensusComputing()
		}
		//fmt.Println()
	}
}

////Process peer request for the parents for events
//func (t *Tetris) ReceiveSyncRequest() {
//
//}

//consensus computing
//每次启动的时候，和共识达成进入下一block的时候调用
func (t *Tetris) prepare() {
	for _, value := range t.validators {
		delete(value, t.h)
		for _, me := range value {
			me.round = ROUND_UNDECIDED
			me.witness = false
			me.vote = 0
			me.committable = COMMITTABLE_UNDECIDED
		}
	}
	t.witness = make([]map[string]*Event, 1)

	//todo:如果有member切换，这儿要重新update know
}

//有新event加入tetris的时候，

func (t *Tetris) update(me *Event, fromAll bool) (foundNew bool) {
	newWitness := false
	n := me.Body.N
	if n == t.h+1 {
		me.round = 0
		me.witness = true

		if t.witness[0] == nil {
			t.witness[0] = make(map[string]*Event)
		}
		t.witness[0][me.vid] = me
	} else {
		maxr := me.round
		for _, e := range me.Body.E {
			pe, ok := t.eventCache.Get(e)
			if ok {
				pme := t.validators[pe.(*Event).vid][pe.(*Event).Body.N]
				if pme != nil {
					if pme.round > maxr {
						maxr = pme.round
					}
				} else {
					//这儿是me的parent pme的n已经在base之下，在t.validators中已经delete。
					//因为me的self parent应该必然存在，所以round肯定可以得到
					//fmt.Println("t.vid=", t.vid[2:4], "me.vid=", me.vid[2:4], "pme==nil", "pe=", pe)
					//fmt.Println("n=", n, "pe.n=", pe.(*Event).Body.N, "h=", t.h)
				}
			} else {
				//缓存中已经没有这个event，
				fmt.Println("eventCache no existed:", e)
				continue
			}
		}
		me.round = maxr
		if me.round == ROUND_UNDECIDED {
			fmt.Println("t.vid:", t.vid[2:4], "me.vid", me.vid[2:4])
			fmt.Println(me.Body)
			fmt.Println(t.eventCache.Len())
			pe, _ := t.eventCache.Get(me.Body.E[0])
			pme := pe.(*Event)
			fmt.Println(pme.round)
			pme = t.validators[pme.vid][pme.Body.N]
			fmt.Println(pme.round)
			for k, v := range t.validators {
				fmt.Print(k[2:4], ": ")
				for nn := uint64(t.h); nn < t.n+10; nn++ { //怀疑可能是updateall时的高度不够，就是自身n不是最大的时候，会有event没update
					ee := v[nn]
					if ee != nil {
						fmt.Print(ee.round, " ")
					}
				}
				fmt.Println()
			}
		}
		if len(t.witness[me.round]) >= t.params.superMajority { //todo:这个地方有过一次panic，out of range, 原因是placeholder miss了update
			c := 0
			for _, v := range t.witness[me.round] {
				if t.knowWell(me, v) {
					c++
				}
			}
			if c >= t.params.superMajority {
				me.round++
			}
		}
		pme := t.validators[me.vid][n-1]
		if pme != nil {
			if me.round > pme.round {
				me.witness = true
				//if t.witness[me.round] == nil {
				//      t.witness[me.round] = make(map[uint32]*Event)
				//}
				if len(t.witness) <= me.round {
					t.witness = append(t.witness, make(map[string]*Event))
				}
				t.witness[me.round][me.vid] = me
				//if me.round == 0 {
				//	fmt.Println("*************", me.round, pme.round, pme.Body.N)
				//}
				newWitness = true
			}
		} else {
			fmt.Println("这儿应该不会跑到的。。。")
			fmt.Println("t.vid=", t.vid[2:4], "me.vid=", me.vid[2:4], n)
			//return
		}
	}

	//如果发现新的witness，就计算一次共识
	if newWitness && !fromAll {
		t.consensusComputing()
	}

	return newWitness
}

func (t *Tetris) updateAll() {
	//计算round， witness，
	newWitness := false
	maxh := uint64(0)
	for _, vh := range t.validatorsHeight {
		if maxh < vh {
			maxh = vh
		}
	}
	for n := t.h + 1; n <= maxh; n++ {
		for m, _ := range t.validators {
			me := t.validators[m][n]
			if me != nil {
				if t.update(me, true) {
					newWitness = true
				}
			}
		}
	}

	//如果发现新的witness，就计算一次共识
	if newWitness {
		t.consensusComputing()
	}
}

func (t *Tetris) consensusComputing() {
	if len(t.witness) <= 3 {
		return
	}

	for m, _ := range t.validators {
		me := t.validators[m][t.h+1]
		if me == nil {
			continue
		}
		if me.committable != COMMITTABLE_UNDECIDED {
			continue
		}

		for _, w := range t.witness[1] {
			if t.knowWell(w, me) {
				w.vote = 1
			} else {
				w.vote = 0
			}
		}
		for _, w := range t.witness[2] {
			c := 0
			w.vote = 0
			for _, pw := range t.witness[1] {
				if t.knowWell(w, pw) {
					if pw.vote == 1 {
						c++
					}
				}
			}
			if c >= t.params.f/2+1 {
				w.vote = 1
			}
		}
	loop:
		for i := 3; i < len(t.witness); i++ {
			if len(t.witness[i]) == 0 {
				break
			}
			for _, w := range t.witness[i] {
				c := 0
				nc := 0
				vv := 0
				vt := 0
				for _, pw := range t.witness[i-1] {
					if t.knowWell(w, pw) {
						if pw.vote == 1 {
							c++
						} else {
							nc++
						}
					}
				}

				if c >= nc {
					vv = 1
					vt = c
				} else {
					vv = 0
					vt = nc
				}

				if vt >= t.params.superMajority {
					me.committable = vv
					//if i > 3 {
					//	fmt.Println("committable at", i)
					//}
					break loop
				} else {
					w.vote = vv
				}
			}
		}
		if me.committable == COMMITTABLE_UNDECIDED {
			return
		}
	}

	//用txsPending来实现, txsPending的hash写到区块里面，新成员加入时可直接获取。另一个做法是有新成员时的stage清除pending检查
	//todo:pending也要有一个深度限制，防止恶意无效txs堆积.另、现在已确认交易的后续消息也会进入pending！已确认交易只有一个cache来去重。
	//todo:fair排序问题？

	c := make([]string, 0)
	cs := ""
	txc := make([]common.Hash, 0)

	for _, w := range t.witness[0] {
		if w.committable == 1 {
			c = append(c, w.vid[0:4])
			for _, tx := range w.Body.Tx {
				if t.txsCommitted.Contains(tx) {
					continue
				}
				txm := t.txsPending[tx]
				if txm == nil {
					txm = make(map[string]uint64)
					t.txsPending[tx] = txm
				}
				txm[w.vid] = w.Body.N
				if len(txm) > t.params.f {
					txc = append(txc, tx)
					t.txsCommitted.Add(tx, true)
					delete(t.txsPending, tx)
				}
			}
		}
	}

	//fmt.Println("pending tx:", len(t.PendingTxs))

	//if len(txc) == 0 {
	//	for _, w := range t.witness[0] {
	//		if w.committable == 1 {
	//			fmt.Println(w.vid, ":", len(w.Body.Tx))
	//		}
	//	}
	//}
	//fmt.Println()
	//fmt.Println("*****node:", t.vid[2:4])
	//for _, w := range t.witness[0] {
	//	fmt.Print(w.vid[2:4], "(", w.Body.N, ")", ", ")
	//}
	//fmt.Println()
	//for _, w := range t.witness[1] {
	//	fmt.Print(w.vid[2:4], "(", w.Body.N, ")", ",")
	//}
	//fmt.Println()

	//sort.Strings(txc)
	sort.Slice(txc, func(i, j int) bool {
		for k := 0; k < common.HashLength; k++ {
			if txc[i][k] < txc[j][k] {
				return true
			} else {
				if txc[i][k] > txc[j][k] {
					return false
				}
			}
		}
		return false
	})
	sort.Strings(c)
	for i := 0; i < len(c); i++ {
		cs = cs + c[i]
	}

	//if t.h < 10 {
	//	fmt.Println(t.vid[0:4], "'s consensus events:", cs)
	//}
	//fmt.Print("******************* consensus reached ******************* ")
	//fmt.Println("m=", t.m, " n=", t.h+1, ":", cs)

	o := &ConsensusOutput{h: t.h + 1, output: cs, Tx: txc}
	t.OutputCh <- o
	//deadlock的问题在这儿，当一个event触发大量的共识时，由于这儿是递归，一直没有返回，但在发output，导致外部程序的loop处理不了output而死锁！

	t.h++
	t.prepare() //开始下一轮
	t.updateAll()
}

func (t *Tetris) know(x, y *Event) bool {
	//if x.Body.N > y.Body.N+30 {
	//	fmt.Println(">>>>>>>", x.Body.N-y.Body.N, x.vid, y.vid, x.Body.N, y.Body.N, ">>>", t.vid)
	//}

	if x.know[y.vid] >= y.Body.N {
		return true
	} else {
		return false
	}
}

func (t *Tetris) knowWell(x, y *Event) bool {
	c := 0
	for id, _ := range t.validators {
		e, ok := t.validators[id][x.know[id]]

		if ok && t.know(e, y) {
			c++
		}
	}

	if c >= t.params.superMajority {
		return true
	}
	return false
}

func (t Tetris) DebugPrint() {
	fmt.Println()
	fmt.Println("t.vid:", t.vid[2:4], "t.h:", t.h, "t.n", t.n, "pendings:", t.eventPending.Len())
	//for _, key := range t.eventPending.Keys() {
	//	ei, ok := t.eventPending.Get(key)
	//	if ok {
	//		ev := ei.(*Event)
	//        fmt.Println(ev.vid[2:4], ev.Body.N, len(ev.Body.E), len(ev.Body.Tx))
	//	}
	//}
	allt := make(map[string]map[uint64]*Event)
	height := make(map[string]uint64)

	keys := []string{}
	for k := range t.validators {
		keys = append(keys, k)
		allt[k] = make(map[uint64]*Event)
	}
	sort.Strings(keys)


	for _, key := range t.eventPending.Keys() {
		ei, ok := t.eventPending.Get(key)
		if ok {
			ev := ei.(*Event)
			allt[ev.vid][ev.Body.N] = ev
			if ev.Body.N > height[ev.vid] {
				height[ev.vid] = ev.Body.N
			}
		}
	}

	for _, key := range keys {
		fmt.Print(key[2:4], ":")
		for h := t.h + 1; h <= t.validatorsHeight[key]; h++ {
			if t.validators[key][h] != nil {
                 fmt.Print("E")
			} else {
				fmt.Print("-")
			}
		}
		pstart := t.validatorsHeight[key] + 1
		if pstart < t.h + 1 {
			pstart = t.h+1
		}
		for p := pstart; p <= height[key]; p++ {
			if allt[key][p] != nil {
				fmt.Print("*")
			} else {
				fmt.Print(".")
			}
		}
		fmt.Print("(",height[key], t.validatorsHeight[key],")")
		fmt.Println()
	}
	fmt.Println()
}
