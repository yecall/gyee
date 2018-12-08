/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package tetris

/*
   共识机制
   输入：tx的hash，Events
        读取，当前block checkpoint， core state，
        当前validate列表，
        validate列表更新操作
   输出：请求发送Events，达成共识的Event集合
        本届member评价生成tx
        下届validate列表
        统计metrics
   pow竞争：拿到全量state和checkpoint之后的block，做hash碰撞，成功的发送一个tx，谁的tx先到先得

0. 一个节点，申请成为candidate，同步区块stable state之后的所有内容，参与pow竞争，然后可能被选中为validator
   创世块中，有指定缺省的validator
   申请candidate的操作，可以在console中，也可以再wallet中发起操作
1. 创建新的Tetris
   时机：core模块启动，同步区块头完成，如果发现自己属于validator，则要同步区块内容，完成后启动Tetris
   参数：当前区块高度，validator清单，提交共识结果通道，
   缺省值：N=0, LN=0 等到接受到E刷新

2. 接收Tx，只需要交易的hash，
   本地从网络收交易有core负责
      core需要：验证交易格式，防止ddos攻击，内存池管理
   共识达成后的ordered tx，由core负责验证、执行、打包
   共识后的tx，有可能只有hash，内容未到，有core负责拉取。可在共识过程中由Tetris实时反馈给core去拉取刷新交易内容

3. 接收Event(h, m, n, t, {tx...}, {E...})
   Event的校验：

   如果toMe：Event存当前收到列表
   如果Event的parent还没有收全，放入unsettled列表，请求同步
   检查unsettleed列表，如果parent已收全，加入tetris

   拉取Event详情


4. 发送Events
   h = 当前区块高度
   m = member id
   n = sequence number, n>=parents.n && n > selfParent.n
   发送规则：num>max && time>=min || time > max && num >= min


5. 计算共识
   已确定的event，及其self-parent, 包含的所有tx，根据规则去重排序。规则还需要再设计
   tx被f+1个committable member收到过，才能加入共识，历史上收到的都算，如果是后续同步来的event，同步到哪个深度？

   计算member表现
   计算reward

6. 踢出老memeber，选择新member加入

7. Events和Tx何时可以销毁

8. 网络层面：group dht
   group包含成员validators，向group发送dht数据。搞一个group id？子网广播

9. 需解决的工程问题：1）点火问题。2）区块同步效率问题。3）成员替换时的know清单问题。4）内存释放问题。

*/

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yeeco/gyee/utils"
	"github.com/yeeco/gyee/utils/logging"
)

type SyncRequest struct {
	count int
	time  time.Time
}

type Tetris struct {
	core ICore

	h uint64 //Current Block Height
	m uint   //My MemberID
	n uint64 //Current Sequence Number for processing

	membersID   map[string]uint //[address] -> id
	membersAddr map[int]string  //[id] -> address

	memberEvents  map[uint]map[uint64]*Event
	memberHeight  map[uint]uint64
	pendingEvents *utils.LRU
	eventRequest  map[string]SyncRequest
	witness       []map[uint]*Event
	eventCache    *utils.LRU
	txsCache      *utils.LRU
	PendingTxs    map[string]map[uint]uint64 //交易在哪个member的哪个高度出现过
	CommittedTxs  *utils.LRU
	currentEvent Event

	EventCh       chan Event
	ParentEventCh chan Event
	TxsCh         chan string

	OutputCh       chan ConsensusOutput
	SendEventCh    chan Event
	RequestEventCh chan string

	//params
	params Params

	pendingSync bool

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup

	//time
	lastSendTime time.Time
	ticker       *time.Ticker

	//metrics or test
	requestCount   int
	parentCountRaw int
	parentCount    int
	eventCount     int
	txCount        int

	//request map[string]int
	level       int
	noRecvCount int
}

func NewTetris(core ICore, members map[string]uint, blockHeight uint64, mid string) (*Tetris, error) {
	//logging.Logger.Info("Create new Tetris")
	tetris := Tetris{
		core:      core,
		membersID: members,
		h:         blockHeight,
		m:         members[mid],
		n:         blockHeight + 1,
		//TODO: cache size and policy need to be decide
		eventCache: utils.NewLRU(10000, nil),
		txsCache:   utils.NewLRU(10000, nil),
		//knowWellCache: utils.NewLRU(1000, nil),
		memberEvents:  make(map[uint]map[uint64]*Event, len(members)),
		memberHeight:  make(map[uint]uint64, len(members)),
		pendingEvents: utils.NewLRU(10000, nil),
		eventRequest:  make(map[string]SyncRequest), //TODO:memroy consumption optimize
		PendingTxs:    make(map[string]map[uint]uint64),
		CommittedTxs:  utils.NewLRU(100000, nil),

		EventCh:       make(chan Event, 100),
		ParentEventCh: make(chan Event, 100),
		TxsCh:         make(chan string, 100000),

		OutputCh:       make(chan ConsensusOutput, 10),
		SendEventCh:    make(chan Event, 100),
		RequestEventCh: make(chan string, 100),
		quitCh:         make(chan struct{}),

		lastSendTime: time.Now(),
		ticker:       time.NewTicker(time.Second),
	}

	for _, value := range members {
		tetris.memberEvents[value] = make(map[uint64]*Event)
		tetris.memberHeight[value] = 0
	}

	tetris.params = Params{
		f:                 (len(members) - 1) / 3,
		superMajority:     2*len(members)/3 + 1,
		maxTxPerEvent:     2000,
		minTxPerEvent:     1,
		maxEventPerEvent:  len(members),
		minEventPerEvent:  2,
		maxPeriodForEvent: 2000,
		minPeriodForEvent: 20,
		maxSunk:           0,
	}

	//if tetris.m == 0 { //模拟不一样的发送频率影响
	//	tetris.params.maxTxPerEvent = 500
	//}

	tetris.pendingSync = true
	tetris.currentEvent = NewEvent(tetris.h, tetris.m, tetris.n)
	//tetris.currentEvent.appendEvent("")
	tetris.prepare()

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

func (t *Tetris) loop() {
	t.wg.Add(1)
	defer t.wg.Done()
	//logging.Logger.Info("Tetris loop...")
	for {
		select {
		case <-t.quitCh:
			//logging.Logger.Info("Tetris loop end.")
			return
		case event := <-t.EventCh:
			event.know = make(map[uint]uint64)
			event.parents = make(map[uint]*Event)
			event.round = ROUND_UNDECIDED
			event.witness = false
			event.vote = 0
			event.committable = COMMITTABLE_UNDECIDED
			t.receiveEvent(&event)
		case event := <-t.ParentEventCh:
			event.know = make(map[uint]uint64)
			event.parents = make(map[uint]*Event)
			event.round = ROUND_UNDECIDED
			event.witness = false
			event.vote = 0
			event.committable = COMMITTABLE_UNDECIDED
			t.receiveParentEvent(&event)
		case tx := <-t.TxsCh:
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

func (t *Tetris) sendEvent(event Event) {
	event.setAppendE()
	if event.totalEvent() == 1 {
		t.noRecvCount++
	} else {
		t.noRecvCount = 0
	}
	t.SendEventCh <- event
	t.lastSendTime = time.Now()
	event.ready = true
	t.memberEvents[t.m][t.n] = &event
	t.memberHeight[t.m] = t.n
	t.eventCache.Add(event.Hex(), &event)
	t.n += 1
	t.currentEvent = NewEvent(t.h, t.m, t.n)
	t.currentEvent.appendEvent(&event)
	t.update(&event)
}

func (t *Tetris) receiveTicker(time time.Time) {

}

func (t *Tetris) receiveTx(tx string) {
	if !t.txsCache.Contains(tx) {
		t.txCount++
		//if t.txCount % 30000 == 0 {
		//	logging.Logger.WithFields(logrus.Fields{
		//		"tm":     t.m,
		//		"tn":     t.n,
		//		"c":      t.txCount,
		//		"pending": t.pendingEvents.Len(),
		//	}).Info("Tx count")
		//}
		t.txsCache.Add(tx, true)
		t.currentEvent.appendTx(tx)
		//if t.noRecvCount > 5 {
		//	return
		//}
		if t.currentEvent.totalTx() > t.params.maxTxPerEvent {
			t.sendEvent(t.currentEvent)
		}
	}
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
	//		"pending": t.pendingEvents.Len(),
	//	}).Info("Event count")
	//}
	if t.eventCache.Contains(event.Hex()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hex(),
			"m":     event.Body.M,
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

	t.pendingEvents.Add(event.Hex(), event)
	t.addReceivedEventToTetris(event)
}

func (t *Tetris) receiveParentEvent(event *Event) {
	t.parentCountRaw++
	if t.eventCache.Contains(event.Hex()) {
		logging.Logger.WithFields(logrus.Fields{
			"event": event.Hex(),
			"m":     event.Body.M,
			"n":     event.Body.N,
		}).Debug("Recevie already existed event")
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
	//		"pending": t.pendingEvents.Len(),
	//	}).Info("Parent count")
	//}
	t.addReceivedEventToTetris(event)
}

func (t *Tetris) addReceivedEventToTetris(event *Event) {
	t.eventCache.Add(event.Hex(), event)
	me := t.memberEvents[event.Body.M][event.Body.N]
	if me != nil {
		if me.Hex() != event.Hex() {
			logging.Logger.WithFields(logrus.Fields{
				"event": event.Hex(),
				"me":    me.Hex(),
			}).Warn("Receive event with different hash, fork detected.")
		}
		return
	}

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
	for _, key := range t.pendingEvents.Keys() {
		ei, ok := t.pendingEvents.Get(key)
		if ok {
			ev := ei.(*Event)
			bsl := make(map[string]*Event)
			bsl[ev.Hex()] = ev
			bs := append([]map[string]*Event{}, bsl)
			l := 0
			//广度优先搜索到level中所有的event或者不存在，或者都在member中为止
			for {
				currentL := bs[l]
				nextL := make(map[string]*Event)
				//当前level的events, 如果都在member中，结束。
				allReady := true
				for _, e := range currentL {
					//如果event都在member中，
					if !e.ready {
						allReady = false
						for _, peh := range e.Body.E {
							if peh == "" {
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
								if t.eventRequest[peh].count < 1 {
									t.RequestEventCh <- peh
									t.requestCount++
									t.eventRequest[peh] = SyncRequest{count: t.eventRequest[peh].count + 1, time: time.Now()}
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
							if peh == "" {
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
							t.memberEvents[e.Body.M][e.Body.N] = e
							t.memberHeight[e.Body.M] = e.Body.N
							if t.pendingEvents.Contains(e.Hex()) {
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
										t.sendEvent(t.currentEvent)
									}
								}

								t.currentEvent.appendEvent(e)
								//}
								t.pendingEvents.Remove(e.Hex())
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

		for _, event := range unpending {
			//fmt.Print("    t:", t.m, " n:", event.Body.N, " m:", event.Body.M)
			t.update(event)
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
	for _, value := range t.memberEvents {
		delete(value, t.h)
		for _, me := range value {
			me.round = ROUND_UNDECIDED
			me.witness = false
			me.vote = 0
			me.committable = COMMITTABLE_UNDECIDED
		}
	}
	t.witness = make([]map[uint]*Event, 1)

	//todo:如果有member切换，这儿要重新update know
}

//有新event加入tetris的时候，

func (t *Tetris) update(me *Event) {
	newWitness := false
	m := me.Body.M
	n := me.Body.N
	if n == t.h+1 {
		me.round = 0
		me.witness = true

		if t.witness[0] == nil {
			t.witness[0] = make(map[uint]*Event)
		}
		t.witness[0][m] = me
	} else {
		maxr := me.round
		for _, e := range me.Body.E {
			pe, ok := t.eventCache.Get(e)
			if ok {
				pme := t.memberEvents[pe.(*Event).Body.M][pe.(*Event).Body.N]
				if pme != nil && pme.round > maxr {
					maxr = pme.round
				}
			} else {
				return
			}
		}
		me.round = maxr
		//fmt.Println(" round:",me.round, " ", n, " ", m, me.Body.E)
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
		pme := t.memberEvents[me.Body.M][me.Body.N-1]
		if pme != nil {
			if me.round > pme.round {
				me.witness = true
				//if t.witness[me.round] == nil {
				//      t.witness[me.round] = make(map[uint32]*Event)
				//}
				if len(t.witness) <= me.round {
					t.witness = append(t.witness, make(map[uint]*Event))
				}
				t.witness[me.round][m] = me
				newWitness = true
			}
		} else {
			fmt.Println("这儿应该不会跑到的。。。")
			return
		}
	}

	//如果发现新的witness，就计算一次共识
	if newWitness {
		t.consensusComputing()
	}
}

func (t *Tetris) updateAll() {
	//计算round， witness，
	newWitness := false
	for n := t.h + 1; n < t.n; n++ {
		for _, m := range t.membersID {
			me := t.memberEvents[m][n]

			if me != nil {
				if me.round != ROUND_UNDECIDED {
					continue
				}
				if n == t.h+1 {
					me.round = 0
					me.witness = true

					if t.witness[0] == nil {
						t.witness[0] = make(map[uint]*Event)
					}
					t.witness[0][m] = me
				} else {
					maxr := me.round
					for _, e := range me.Body.E {
						pe, ok := t.eventCache.Get(e)
						if ok {
							pme := t.memberEvents[pe.(*Event).Body.M][pe.(*Event).Body.N]
							if pme != nil && pme.round > maxr {
								maxr = pme.round
							}
						} else {
							return
						}
					}
					me.round = maxr

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
					pme := t.memberEvents[me.Body.M][me.Body.N-1]
					if pme != nil {
						if me.round > pme.round {
							me.witness = true
							//if t.witness[me.round] == nil {
							//	t.witness[me.round] = make(map[uint]*Event)
							//}
							if len(t.witness) <= me.round {
								t.witness = append(t.witness, make(map[uint]*Event))
							}
							t.witness[me.round][m] = me
							newWitness = true
						}
					} else {
						fmt.Println("这儿应该不会跑到的。。。")
						return
					}
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

	for _, m := range t.membersID {
		me := t.memberEvents[m][t.h+1]
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

	//用pendingTxs来实现, pendingTxs的hash写到区块里面，新成员加入时可直接获取
	//todo:pending也要有一个深度限制，防止恶意无效txs堆积.另、现在已确认交易的后续消息也会进入pending！已确认交易只有一个cache来去重。
	//todo:fair排序问题？

	c := make([]int, 0)
	cs := ""
	txc := make([]string, 0)
	for _, w := range t.witness[0] {
		if w.committable == 1 {
			c = append(c, int(w.Body.M))
			for _, tx := range w.Body.Tx {
				if t.CommittedTxs.Contains(tx) {
					continue
				}
				txm := t.PendingTxs[tx]
				if txm == nil {
					txm = make(map[uint]uint64)
					t.PendingTxs[tx] = txm
				}
				txm[w.Body.M] = w.Body.N
				if len(txm) > t.params.f {
					txc = append(txc, tx)
					t.CommittedTxs.Add(tx, true)
					delete(t.PendingTxs, tx)
				}
			}
		}
	}

    //fmt.Println("pending tx:", len(t.PendingTxs))

	if len(txc) == 0 {
		for _, w := range t.witness[0] {
			if w.committable == 1 {
				fmt.Println(w.Body.M, ":", len(w.Body.Tx))
			}
		}
	}

	sort.Strings(txc)
	sort.Ints(c)
	for i := 0; i < len(c); i++ {
		cs = cs + strconv.Itoa(c[i])
	}

	//fmt.Print("******************* consensus reached ******************* ")
	//fmt.Println("m=", t.m, " n=", t.h+1, ":", cs)

	o := ConsensusOutput{h: t.h + 1, output: cs, Tx: txc}
	t.OutputCh <- o

	t.h++
	t.prepare() //开始下一轮
	t.updateAll()
}

func (t *Tetris) know(x, y *Event) bool {
	//if x.Body.N > y.Body.N+30 {
	//	fmt.Println(">>>>>>>", x.Body.N-y.Body.N, x.Body.M, y.Body.M, x.Body.N, y.Body.N, ">>>", t.m)
	//}

	if x.know[y.Body.M] >= y.Body.N {
		return true
	} else {
		return false
	}

	//广度搜索
	/*
		bsl := make(map[string]*Event)
		bsl[x.Hex()] = x
		for {
			nextL := make(map[string]*Event)
			for h, e := range bsl {
				if h == y.Hex() || e.Body.M == y.Body.M {
					t.knowCache.Add(x.Hex()+y.Hex(), true)
					return true
				}
				for _, ne := range e.Body.E {
					if bsl[ne] != nil {
						continue
					}
					nevi, ok := t.eventCache.Get(ne)
					if ok {
						nev := nevi.(*Event)
						if nev.Body.N > y.Body.N {
							nextL[ne] = nev
						}
					}
				}
			}
			if len(nextL) == 0 {
				break
			} else {
				bsl = nextL
			}

		}
	*/
	//深度优先搜，貌似还是这个快一点，因为这个每个点都有cache
	/*
		for _, e := range x.Body.E {
			if e == y.Hex() {
				t.knowCache.Add(x.Hex()+y.Hex(), true)
				return true
			}
			ev, ok := t.eventCache.Get(e)
			if ok {
				evv := ev.(*Event)
				if evv.Body.N > y.Body.N {
					if evv.Body.M == y.Body.M {
						t.knowCache.Add(x.Hex()+y.Hex(), true)
						return true
					}
					ret := t.know(evv, y)
					if ret {
						t.knowCache.Add(x.Hex()+y.Hex(), true)
						return true
					}
				}
			}
		}
		t.knowCache.Add(x.Hex()+y.Hex(), false)
		return false
	*/
}

func (t *Tetris) knowWell(x, y *Event) bool {
	c := 0
	for _, id := range t.membersID {
		e, ok := t.memberEvents[id][x.know[id]]

		if ok && t.know(e, y) {
			c++
		}
	}

	if c >= t.params.superMajority {
		return true
	}
	return false
}

//func (t *Tetris) cacheKnowWell(x, y *Event) bool {
//    s, ok := t.knowWellCache.Get(x.Hex() + y.Hex())
//    if ok {
//    	    return s.(bool)
//	}
//
//	c := 0
//	for _, id := range t.membersID {
//		e, ok := t.memberEvents[id][x.know[id]]
//
//		if ok && t.know(e, y) {
//			c++
//		}
//	}
//
//	if c >= t.params.superMajority {
//		t.knowWellCache.Add(x.Hex() + y.Hex(), true)
//		return true
//	}
//	t.knowWellCache.Add(x.Hex() + y.Hex(), false)
//	return false
//}
