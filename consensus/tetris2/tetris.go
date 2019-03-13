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

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/consensus"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/utils"
	"github.com/yeeco/gyee/utils/logging"
)

const (
	VidStrStart = address.AddressContentIndex * 2
)

var HASH0 = [common.HashLength]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type SyncRequest struct {
	count int
	time  time.Time
}

type SealEvent struct {
	height uint64
	txs    []common.Hash
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
	witness          [][]*Event                   //witness of each round.

	//Input Channel
	EventCh       chan []byte
	ParentEventCh chan []byte
	TxsCh         chan common.Hash
	SealCh        chan *SealEvent

	//Output Channel
	OutputCh       chan *consensus.Output
	SendEventCh    chan []byte
	RequestEventCh chan common.Hash

	eventCache    *utils.LRU
	eventAccepted []*Event
	eventRequest  *utils.LRU

	txsCache     *utils.LRU                        //dedup for received txs
	txsAccepted  []common.Hash                     //for new event
	txsCommitted *utils.LRU                        //dedup for committed txs
	txsPending   map[common.Hash]map[string]uint64 //tx appear at which validator's which height, key1: hash of tx, key2:vid, value:height

	ticker    *time.Ticker
	heartBeat map[string]time.Time //time of receive event from every validators，key:vid

	//params
	params *Params

	//metrics or test
	Metrics *Metrics

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup

	//time
	lastSendTime     time.Time
	possibleNewReady bool
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
		witness:          make([][]*Event, 1),

		EventCh:       make(chan []byte, 10),
		ParentEventCh: make(chan []byte, 10),
		TxsCh:         make(chan common.Hash, 2000),
		SealCh:        make(chan *SealEvent, 100),

		OutputCh:       make(chan *consensus.Output, 10),
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
		Metrics:      NewMetrics(),
	}

	tetris.signer = core.GetSigner()
	pk, err := core.GetPrivateKeyOfDefaultAccount()
	if err != nil {
		return nil, err
	}
	if err := tetris.signer.InitSigner(pk); err != nil {
		return nil, err
	}

	for _, value := range validatorList {
		tetris.validators[value] = make(map[uint64]*Event)
		tetris.validatorsHeight[value] = blockHeight
		tetris.pendingHeight[value] = blockHeight
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
	}

	//tetris.prepare()

	return &tetris, nil
}

func (t *Tetris) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	//logging.Logger.Info("Tetris Start...")
	go t.loop()
	return nil
}

func (t *Tetris) Stop() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	//logging.Logger.Info("Tetris Stop...")
	close(t.quitCh)
	t.wg.Wait()
	return nil
}

func (t *Tetris) ChanEventSend() <-chan []byte {
	if t == nil {
		return nil
	}
	return t.SendEventCh
}

func (t *Tetris) ChanEventReq() <-chan common.Hash {
	if t == nil {
		return nil
	}
	return t.RequestEventCh
}

func (t *Tetris) Output() <-chan *consensus.Output {
	if t == nil {
		return nil
	}
	return t.OutputCh
}

func (t *Tetris) SendEvent(event []byte) {
	t.EventCh <- event
}

func (t *Tetris) SendParentEvent(event []byte) {
	t.ParentEventCh <- event
}

func (t *Tetris) SendTx(hash common.Hash) {
	t.TxsCh <- hash
}

func (t *Tetris) OnTxSealed(height uint64, txs []common.Hash) {
	t.SealCh <- &SealEvent{
		height: height,
		txs:    txs,
	}
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
			t.Metrics.AddTrafficIn(uint64(len(eventMsg)))
			event.Unmarshal(eventMsg)
			event.isParent = false
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
			t.Metrics.AddTrafficIn(uint64(len(eventMsg)))
			event.Unmarshal(eventMsg)
			event.isParent = true
			if t.checkEvent(&event) {
				t.receiveParentEvent(&event)
			}

		case tx := <-t.TxsCh:
			t.Metrics.AddTrafficIn(uint64(len(tx)))
			t.receiveTx(tx)

		case seal := <-t.SealCh:
			t.receiveSeal(seal)

		case time := <-t.ticker.C:
			t.receiveTicker(time)
		}
	}
}

func (t *Tetris) sendPlaceholderEvent() {
	if t.validators[t.vid] == nil { //self has quit the validators
		return
	}

	event := NewEvent(t.vid, t.h, t.n)
	event.AddSelfParent(t.validators[t.vid][t.n-1])
	event.Sign(t.signer)
	eb := event.Marshal()
	t.Metrics.AddTrafficOut(uint64(len(eb)))
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
	if t.validators[t.vid] == nil { //self has quit the validators
		return
	}

	//todo: eventAccepted should filter old parents?

	event := NewEvent(t.vid, t.h, t.n)
	event.AddSelfParent(t.validators[t.vid][t.n-1])
	event.AddParents(t.eventAccepted)
	event.AddTransactions(t.txsAccepted)
	event.Sign(t.signer)
	eb := event.Marshal()
	t.Metrics.AddTrafficOut(uint64(len(eb)))
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
	t.Metrics.AddTrafficOut(uint64(len(pb)))
	t.SendEventCh <- pb
}

func (t *Tetris) receiveTicker(ttime time.Time) {
	if ttime.Sub(t.lastSendTime) > 1*time.Second {
		//t.sendHeartbeat()
		t.sendEvent()
	}
}

func (t *Tetris) receiveTx(tx common.Hash) {
	if !t.txsCache.Contains(tx) {
		t.Metrics.AddTxIn(1)
		//if t.txCount%30000 == 0 {
		//	//logging.Logger.WithFields(logrus.Fields{
		//	//	"vid":     t.vid[0:4],
		//	//	"n":       t.n,
		//	//	"c":       t.txCount,
		//	//	"pending": t.eventPending.Len(),
		//	//}).Info("Tx count")
		//}
		t.txsCache.Add(tx, true)
		t.txsAccepted = append(t.txsAccepted, tx)

		//if len(t.txsAccepted) > t.params.maxTxPerEvent {
		ok, _ := t.MajorityBeatTime()
		if ok {
			t.sendEvent()
		} else {
			//t.txsAccepted = t.txsAccepted[10:]
		}
		//}
	}
}

func (t *Tetris) receiveSeal(event *SealEvent) {
	// TODO:
}

func (t *Tetris) checkEvent(event *Event) bool {
	if t.eventCache.Contains(event.Hash()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hash(),
			"vid":   event.vid,
			"n":     event.Body.N,
		}).Debug("Recevie already existed event")
		return false
	}

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
		if event.isParent {
			t.eventCache.Add(event.Hash(), event) //if it is parent, should cache for following task
			t.possibleNewReady = true
		}
		//logging.Logger.Warn("the sender of event is not validators.", event.vid[2:4])
		return false
	}

	ok := event.SignVerify(pk, t.signer)

	if ok {
		event.know = make(map[string]uint64)
		event.fork = make([]*Event, 0)
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
	t.Metrics.AddEventIn(1)

	if event.Body.H > t.h {
		//TODO: if the block has sealed, here can interrupt and jump to new height
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
	t.Metrics.AddParentEventIn(1)

	if event.Body.N <= t.h {
		//TODO: parent event can not discard even if it is too old. It is needed to check ready status of valid event.
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
			me.fork = append(me.fork, event)
		}
		return
	}

	begin := time.Now()
	newReady := make([]*Event, 0)

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
		if !event.isParent {
			t.eventAccepted = append(t.eventAccepted, event)
		}
		t.validatorsHeight[event.vid] = event.Body.N
		newReady = append(newReady, event)
	}

	//Here we check all the probabilities that an event could become ready
	//1. Parent event under base, should be used when check upper events' ready status.
	//2. Event is base event and has tagged as ready above.
	//3. Events just above the validators height
	//4. An base event has tagged as ready in t.prepare() after consensus got.
	//5. After t.prepare() or validator rotate, new ready is possible
	if event.Body.N <= t.h ||
		event.ready ||
		event.Body.N == t.validatorsHeight[event.vid]+1 ||
		t.possibleNewReady {
		t.possibleNewReady = false
		newReady = append(newReady, t.findNewReady()...)
	}

	if len(newReady) == 0 {
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
								if now.Sub(er.time) > time.Duration(er.count*500)*time.Millisecond && er.count < 10 {
									t.RequestEventCh <- peh
									t.Metrics.AddTrafficOut(32)
									t.Metrics.AddEventRequest(1)
									er.count++
									//logging.Logger.Info("resend request for ", er.count, er.time)
								}

							} else {
								t.RequestEventCh <- peh
								t.Metrics.AddTrafficOut(32)
								t.Metrics.AddEventRequest(1)
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
		logging.Logger.Info(t.vid[2:4], "pending process:", duration.Nanoseconds(), " unpend:", len(newReady))
	}

	if len(newReady) > 0 {
		newWitness := false
		for _, event := range newReady {
			if t.update(event, true) {
				newWitness = true
			}
		}

		if newWitness {
			t.consensusComputing()
		}
	}

}

func (t *Tetris) findNewReady() []*Event {
	var newReady []*Event
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
			//It is not possible for an event to be ready if it's parent not exist. so stop search any more.
			if t.validators[k][v] == nil {
				delete(searchHeight, k)
				continue
			}

			//search height under the new ready event, it is impossible to be ready. so stop search any more.
			if v <= newHeight {
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
					if t.validators[pe.vid] == nil { //ignore parent with validators has quitted
						continue
					}
					if pe.Body.N <= t.h {
						continue
					}
					if pe.Body.N > t.validatorsHeight[pe.vid] {
						isReady = false
						break
					} else {
						//here if pe under base, it's possible not ready
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

				if !ev.isParent {
					t.eventAccepted = append(t.eventAccepted, ev)
				}
				t.validatorsHeight[ev.vid] = ev.Body.N
				searchHeight[ev.vid] = ev.Body.N + 1
				newReadyThisRound = true
				if ev.Body.N < minReadyHeight {
					minReadyHeight = ev.Body.N
				}
				newReady = append(newReady, ev)
			}
		}
		//break if no new ready events this round
		if !newReadyThisRound {
			break loop
		}
		newHeight = minReadyHeight
	}

	return newReady
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
			if !be.isParent {
				t.eventAccepted = append(t.eventAccepted, be)
			}
			//t.findNewReady() //here cause a problem for reentry update
		}

		for _, me := range value {
			me.round = ROUND_UNDECIDED
			me.witness = false
			me.vote = 0
			me.committable = COMMITTABLE_UNDECIDED
		}
	}
	t.witness = make([][]*Event, 1)

	//test validator quit, this is controlled by consensus output in the future
	if t.h == 20 {
		//t.validatorRotate(nil, []string{"30373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037"})
		t.validatorRotate([]string{"30393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039303930393039"},
			[]string{"30373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037303730373037"})
		//todo: pending txs need clear then member rotate joins
		t.txsPending = make(map[common.Hash]map[string]uint64)
	}

	t.possibleNewReady = true

	//todo: remain issue: self parent of crash-recovery situation
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
			t.witness[0] = make([]*Event, 0)
		}
		t.witness[0] = append(t.witness[0], me)
	} else {
		maxr := me.round
		for _, e := range me.Body.E {
			pei, ok := t.eventCache.Get(e)
			if ok {
				pe := pei.(*Event)
				if t.validators[pe.vid] == nil {
					continue //ignore event for parents of quitted validators
				}
				pme := t.validators[pe.vid][pe.Body.N]
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
			logging.Logger.Warn("me.round undecided,", t.vid[2:4], me.vid[2:4], me.Body.N)
			//for _, e := range me.Body.E {
			//	pei, ok := t.eventCache.Get(e)
			//	if ok {
			//		pe := pei.(*Event)
			//		if t.validators[pe.vid] == nil {
			//			fmt.Println(pe.vid[2:4], " has quit")
			//			continue //ignore event for parents of quitted validators
			//		}
			//		pme := t.validators[pe.vid][pe.Body.N]
			//		fmt.Println("pme:", pe.vid[2:4], pe.Body.N, pme)
			//		if pme != nil {
			//			if pme.round > maxr {
			//				maxr = pme.round
			//			}
			//		} else {
			//			//It is ok and possible here, when pme is below base.
			//			//logging.Logger.Debug("parent not in tetris.")
			//		}
			//	} else {
			//		//event not existed, it is possible when eventCache overflow! It will cause problems!
			//		logging.Logger.Warn("eventCache no existed:", e)
			//		continue
			//	}
			//}
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
				if len(t.witness) <= me.round {
					t.witness = append(t.witness, make([]*Event, 0))
				}
				t.witness[me.round] = append(t.witness[me.round], me)
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

	//todo:fair order
	begin := time.Now()

	cs := make([]string, 0)
	txc := make([]common.Hash, 0)
	for _, w := range t.witness[0] {
		if w.committable == 1 {
			cs = append(cs, w.vid[VidStrStart:VidStrStart+3])
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

	sort.Strings(cs)
	css := strings.Join(cs, " ")
	end := time.Now()
	duration := end.Sub(begin)
	if duration > 10*time.Millisecond {
		//logging.Logger.Info(t.vid[2:4], "tx order:", duration.Nanoseconds())
	}

	o := &consensus.Output{H: t.h + 1, Output: css, Txs: txc}
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

//In release version, member rotate is fired by consensus output depend on the upper level protocol.
//That protocol need performance metrics of the current members and new request of candidates.
func (t *Tetris) validatorRotate(joins []string, quits []string) bool {
	if len(joins) == 0 && len(quits) == 0 {
		return false
	}

	for _, vid := range quits {
		delete(t.validators, vid)
		delete(t.validatorsHeight, vid)
		delete(t.pendingHeight, vid)
		delete(t.heartBeat, vid)
	}

	//todo: validatorHeight might change after delete some member? but it will be handled when addEventToTetris

	for _, vid := range joins {
		t.validators[vid] = make(map[uint64]*Event)
		t.validatorsHeight[vid] = t.h
		t.pendingHeight[vid] = t.h
	}

	maxh := uint64(0)
	for _, vh := range t.pendingHeight {
		if maxh < vh {
			maxh = vh
		}
	}
	for n := t.h + 1; n <= maxh; n++ {
		for m, _ := range t.validators {
			me := t.validators[m][n]
			if me != nil {
				//if me is base, then me know itself's height
				//else search for me's parents. if the parents is the quit one, then ignore it.
				me.know = make(map[string]uint64)
				me.know[me.vid] = me.Body.N

				if n == t.h+1 {
					continue
				}
			loop:
				for _, peh := range me.Body.E {
					pei, ok := t.eventCache.Get(peh)
					if ok {
						pe := pei.(*Event)
						for _, quit := range quits {
							if pe.vid == quit {
								continue loop
							}
						}
						me.updateKnow(pe)
					} else {
						//it is possible for pending event, ignore it safely.
						//logging.Logger.Info("not in eventcache:", peh, t.eventCache.Len(), t.vid[2:4])
					}
				}
			}
		}
	}

	//todo: adjust the params, the total member number is controled by the consensus protocol.
	t.params.f = (len(t.validators) - 1) / 3
	t.params.superMajority = 2*len(t.validators)/3 + 1
	t.params.maxEventPerEvent = len(t.validators)

	return true
}

//Print info for debug
func (t *Tetris) DebugPrint() {
	fmt.Println()
	fmt.Println("t.vid:", t.vid[2:4], "t.h:", t.h, "t.n", t.n)
	fmt.Println("tx:", t.Metrics.TxIn, "event:", t.Metrics.EventIn, "parent:", t.Metrics.ParentEventIn, "requst", t.Metrics.EventRequest)
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

func (t *Tetris) DebugPrintDetail() {
	keys := []string{}
	for k := range t.validators {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		fmt.Println(key[2:4], ": ")
		for h := t.h + 1; h <= t.validatorsHeight[key]+1; h++ {
			if t.validators[key][h] != nil {
				ev := t.validators[key][h]
				fmt.Print(ev.Body.N, "(")
				for _, peh := range ev.Body.E {
					pei, ok := t.eventCache.Get(peh)
					if ok {
						pe := pei.(*Event)
						fmt.Print(pe.vid[2:4], ":", pe.Body.N, ", ")
					} else {
						fmt.Print("*, ")
					}
				}
				fmt.Print(") ")

			} else {
				fmt.Print("-")
			}
		}
		fmt.Println()
	}
	fmt.Println()
}

/*
How to handle fork events
1. when forked event detected, if is parent, put it in the origin event's fork list. else discard
2. the forked event may be ready or not ready, may be all be witness, but will never committable.
3. the forked event is treated as normal event for: request parent, until it is ready, updateKnow as normal.
4. current event's F list, add the forked event when it is ready.
5. modify updateKnow function, know[vid of Fs]=-1, and this -1 is prior to others
6. if consensus base event include F, then all vid for F quit at next stage
*/
