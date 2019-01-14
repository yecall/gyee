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
	"math"
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
	pendingHeight    map[string]uint64            //current pending event height
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
	//eventPending  *utils.LRU //因parents未到还没有加入到tetris的events
	eventRequest *utils.LRU //请求过的event
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
	requestCount int
	parentCount  int
	eventCount   int
	txCount      int
	TrafficIn    int
	TrafficOut   int

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
		pendingHeight:    make(map[string]uint64, len(validatorList)),

		EventCh:       make(chan []byte, 10),
		ParentEventCh: make(chan []byte, 10),
		TxsCh:         make(chan common.Hash, 2000),

		OutputCh:       make(chan *ConsensusOutput, 10),
		SendEventCh:    make(chan []byte, 10),
		RequestEventCh: make(chan common.Hash, 10),

		eventCache:    utils.NewLRU(10000, nil),
		eventAccepted: make([]*Event, 0),
		eventRequest:  utils.NewLRU(1000, nil),

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
	tetris.signer.InitSigner(pk) //TODO: get the key of default account

	for _, value := range validatorList {
		tetris.validators[value] = make(map[uint64]*Event)
		tetris.validatorsHeight[value] = 0
		tetris.pendingHeight[value] = 0
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
		delete(t.pendingHeight, vid)
		delete(t.heartBeat, vid)
	}

	for _, vid := range joins {
		t.validators[vid] = make(map[uint64]*Event)
		t.validatorsHeight[vid] = 0
		t.pendingHeight[vid] = 0
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
				if event.Body.P {
					//it is a heartbeat event
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
	event.Sign(t.signer)
	eb := event.Marshal()
	t.TrafficOut += len(eb)
	t.SendEventCh <- eb

	event.ready = true
	t.validators[t.vid][t.n] = event
	t.validatorsHeight[t.vid] = t.n
	t.pendingHeight[t.vid] = t.n
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
	event.Sign(t.signer)
	eb := event.Marshal()
	t.TrafficOut += len(eb)
	t.SendEventCh <- eb

	event.ready = true
	t.validators[t.vid][t.n] = event
	t.validatorsHeight[t.vid] = t.n
	t.pendingHeight[t.vid] = t.n
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
}

func (t *Tetris) receiveTx(tx common.Hash) {
	if !t.txsCache.Contains(tx) {
		t.txCount++
		if t.txCount%30000 == 0 {
			//logging.Logger.WithFields(logrus.Fields{
			//	"vid":     t.vid[0:4],
			//	"n":       t.n,
			//	"c":       t.txCount,
			//	"pending": t.eventPending.Len(),
			//}).Info("Tx count")
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

	addr, err := t.core.AddressFromPublicKey(pk)
	if err != nil {
		logging.Logger.Warn("can not get address from public key.", err)
		return false
	}

	event.vid = hex.EncodeToString(addr)

	if t.validators[event.vid] == nil {
		logging.Logger.Warn("the sender of event is not validators.", event.vid)
		return false
	}

	ok := event.SignVerify(pk, t.signer)

	if ok {
		event.know = make(map[string]uint64)
		event.fork = make([]common.Hash, 0)
		event.know[event.vid] = event.Body.N
		event.round = ROUND_UNDECIDED
		event.witness = false
		event.vote = 0
		event.committable = COMMITTABLE_UNDECIDED
	} else {
		logging.Logger.Warn("fail for sign verification", event.vid)
		return false
	}

	return ok
}

func (t *Tetris) receiveEvent(event *Event) {
	t.eventCount++

	if t.eventCache.Contains(event.Hash()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hash(),
			"vid":   event.vid,
			"n":     event.Body.N,
		}).Debug("Recevie already existed event")

		return
	}

	if event.Body.H > t.h {
		//自己的区块高度已低于对方，需要同步区块
		//但如果这个地方自己总是慢而不采用共识输出，最终半数的签名数可能要不够？
		//所以如果收到可以确认的区块签名，这儿可以跳过，到最新的块高度
		//return
	}
	if event.Body.N <= t.h {
		//discard old enough event
		logging.Logger.Debug("event too old, discard.", event.Body.N, t.h)
		return
	}

	t.addReceivedEventToTetris(event)
}

func (t *Tetris) receiveParentEvent(event *Event) {
	t.parentCount++

	if t.eventCache.Contains(event.Hash()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hash(),
			"vid":   event.vid,
			"n":     event.Body.N,
		}).Debug("Recevie already existed parent event")
		return
	}

	if event.Body.N <= t.h {
		//TODO: parent event can not discard even if it is too old. It is needed to check ready status of valid event.
		//logging.Logger.Info("old parent event:", event)
	}

	t.addReceivedEventToTetris(event)
}

func (t *Tetris) addReceivedEventToTetris(event *Event) {
	t.eventCache.Add(event.Hash(), event)
	me := t.validators[event.vid][event.Body.N]
	if me != nil {
		if me.Hash() != event.Hash() {
			logging.Logger.WithFields(logrus.Fields{
				"event": event.Hash(),
				"me":    me.Hash(),
			}).Warn("Receive event with different hash, fork detected.")
			//TODO: fork process
			me.fork = append(me.fork, event.Hash())
		}
		return
	}

	begin := time.Now()
	unpending := make([]*Event, 0)

	event.ready = false
	if event.Body.N > t.h { //valid event add to the tetris
		t.validators[event.vid][event.Body.N] = event
	}

	if t.pendingHeight[event.vid] < event.Body.N { //update the pending height
		t.pendingHeight[event.vid] = event.Body.N
	}

	//tag the base event as ready,
	if event.Body.N == t.h+1 {
		event.ready = true
		t.eventAccepted = append(t.eventAccepted, event)
		t.validatorsHeight[event.vid] = event.Body.N
		unpending = append(unpending, event)
	}

	//Here we check all the probabilities that an event could become ready
	//1. Parent event under base, should be used when check upper events' ready status.
	//2. Event is base event and has tagged as ready above.
	//3. Events just above the validators height
	//4. An base event has tagged as ready in t.prepare() after consensus got.
	if event.Body.N <= t.h ||
		event.ready ||
		event.Body.N == t.validatorsHeight[event.vid]+1 ||
		t.validatorsHeight[event.vid] == t.h+1 {
		searchHeight := make(map[string]uint64)
		newHeight := t.h + 1 //because the condition 4
		for k, v := range t.validatorsHeight {
			if v >= newHeight {
				searchHeight[k] = v + 1
			}
		}

	loop:
		for {
			newReadyThisRound := false
			minReadyHeight := uint64(math.MaxUint64)
			for k, v := range searchHeight {
				if t.validators[k][v] == nil {  //It is not possible for an event to be ready if it's parent not exist. so stop search any more.
					delete(searchHeight, k)
					continue
				}

				if v < newHeight {  //search height under the new ready event, it is impossible to be ready.
					delete(searchHeight, k)
					continue
				}
				ev := t.validators[k][v]
				//if all the parents of ev is under validators height, it will be ready.
				isReady := true
				for _, peh := range ev.Body.E {
					pei, ok := t.eventCache.Get(peh)
					if ok {
						pe := pei.(*Event)
						if pe.Body.N > t.validatorsHeight[pe.vid] {
							isReady = false
							break
						} else {
							ev.updateKnow(pe)
						}
					} else {
						isReady = false
						break
					}
				}

				if isReady {
					ev.ready = true
					for ev.Body.N >= t.n {
						t.sendPlaceholderEvent()
					}

					t.eventAccepted = append(t.eventAccepted, ev)
					t.validatorsHeight[ev.vid] = ev.Body.N
					searchHeight[ev.vid] = ev.Body.N + 1
					newReadyThisRound = true
					if ev.Body.N < minReadyHeight {
						minReadyHeight = ev.Body.N
					}
					unpending = append(unpending, ev)
				}
			}
			//break if no new ready events this round
			if !newReadyThisRound {
				break loop
			}
			newHeight = minReadyHeight
		}
	}

    if len(unpending) == 0 {
		for k, v := range t.validatorsHeight {
			if v <= t.h {
				v = t.h
			}

			for i := v + 1; i <= t.pendingHeight[k]; i++ {
				ev := t.validators[k][i]
				if ev == nil || ev.ready { //search to the first not ready event
					continue
				}
				if !ev.ready {
					for _, peh := range ev.Body.E {
						if peh == HASH0 {
							logging.Logger.Warn("It should not come here")
							continue
						}
						_, ok := t.eventCache.Get(peh)
						if ok {
							//parent existed
						} else {
							eri, ok := t.eventRequest.Get(peh)
							if ok {
								er := eri.(*SyncRequest)
								now := time.Now()
								if now.Sub(er.time) > time.Duration(er.count * 500) * time.Millisecond  && er.count < 10 {
									t.RequestEventCh <- peh
									t.TrafficOut += 32
									//logging.Logger.Debug("resend request for ", peh)
								}
								er.count++
							} else {
								t.RequestEventCh <- peh
								t.TrafficOut += 32
								t.eventRequest.Add(peh, &SyncRequest{count: 1, time: time.Now()})
							}
						}
					}
				}
				break
			}
		}
	}

	end := time.Now()
	duration := end.Sub(begin)
	if duration > 10*time.Millisecond {
		logging.Logger.Info(t.vid[2:4], "pending process:", duration.Nanoseconds(), " unpend:", len(unpending))
	}

	if len(unpending) > 0 {
		//sort.Slice(unpending, func(i, j int) bool {
		//	return unpending[i].Body.N < unpending[j].Body.N
		//})
		newWitness := false
		for _, event := range unpending {
			if t.update(event, true) {
				newWitness = true
			}
		}

		if newWitness {
			t.consensusComputing()
		}
	}

}

////Process peer request for the parents for events
//func (t *Tetris) ReceiveSyncRequest() {
//
//}

//consensus computing
//prepare for every stage
func (t *Tetris) prepare() {
	for _, value := range t.validators {
		delete(value, t.h)
		be := value[t.h+1]
		if be != nil && !be.ready {
			be.ready = true
			t.validatorsHeight[be.vid] = be.Body.N
			t.eventAccepted = append(t.eventAccepted, be)
		}

		for _, me := range value {
			me.round = ROUND_UNDECIDED
			me.witness = false
			me.vote = 0
			me.committable = COMMITTABLE_UNDECIDED
		}
	}
	t.witness = make([]map[string]*Event, 1)

	//todo: update know while members rotated

}


func (t *Tetris) update(me *Event, fromAll bool) (foundNew bool) {
	newWitness := false
	n := me.Body.N

	if !me.ready {
		//logging.Logger.Warn("update: event not ready", me.vid, me.Body.N)
		return false
	}

	if n <= t.h {
		logging.Logger.Info("update: event.n<t.n", me.vid, me.Body.N)
		return false
	}

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
					//It is ok and possible here, when pme is below base.
					//logging.Logger.Debug("parent not in tetris.")
				}
			} else {
				//event not existed, it is possible when eventCache overflow! It will cause problems!
				logging.Logger.Warn("eventCache no existed:", e)
				continue
			}
		}
		me.round = maxr
		if me.round == ROUND_UNDECIDED {
			logging.Logger.Warn("me.round undecided")
		}

		if len(t.witness[me.round]) >= t.params.superMajority {
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
			logging.Logger.Warn("pme==nil, It is impossible to here!")
		}
	}

	//consensus computing while new witness found.
	if newWitness && !fromAll {
		t.consensusComputing()
	}

	return newWitness
}

func (t *Tetris) updateAll() {
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

	//consensus computing while new witness found.
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

	//todo:pending也要有一个深度限制，防止恶意无效txs堆积.另、现在已确认交易的后续消息也会进入pending！已确认交易只有一个cache来去重。
	//todo:fair排序问题？

	begin := time.Now()

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

	end := time.Now()
	duration := end.Sub(begin)
	if duration > time.Millisecond {
		//logging.Logger.Info(t.vid[2:4], "tx order:", duration.Nanoseconds())
	}

	o := &ConsensusOutput{h: t.h + 1, output: cs, Tx: txc}
	t.OutputCh <- o

	t.h++
	t.prepare() //start next stage
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
	fmt.Println("t.vid:", t.vid[2:4], "t.h:", t.h, "t.n", t.n)
	fmt.Println("tx:", t.txCount, "event:", t.eventCount, "parent:", t.parentCount)
	fmt.Println("txCh:", len(t.TxsCh), "eventCh:", len(t.EventCh), "sendCh:", len(t.SendEventCh))

	keys := []string{}
	for k := range t.validators {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		fmt.Print(key[2:4], ":")
		for h := t.h + 1; h <= t.validatorsHeight[key]; h++ {
			if t.validators[key][h] != nil {
				ev := t.validators[key][h]

				if len(ev.Body.E) > 1 {
					if ev.witness {
						fmt.Print("W")
					} else {
						fmt.Print("m")
					}
				} else {
					if ev.witness {
						fmt.Print("$")
					} else if len(ev.Body.Tx) == 0 {
						fmt.Print("s")
					} else {
						fmt.Print("S")
					}
				}

			} else {
				fmt.Print("-")
			}
		}
		pstart := t.validatorsHeight[key] + 1
		if pstart < t.h+1 {
			pstart = t.h + 1
		}
		for p := pstart; p <= t.pendingHeight[key]; p++ {
			if t.validators[key][p] != nil {
				ev := t.validators[key][p]

				if len(ev.Body.E) > 1 {
					fmt.Print("X")
				} else {
					fmt.Print("x")
				}
			} else {
				fmt.Print(".")
			}
		}
		fmt.Print("(", t.pendingHeight[key], t.validatorsHeight[key], ")")
		fmt.Println()
	}
	fmt.Println()
}
