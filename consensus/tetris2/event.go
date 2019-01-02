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
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/utils/logging"
	"github.com/yeeco/gyee/crypto"
)

const ROUND_UNDECIDED = -1
const COMMITTABLE_UNDECIDED = -1

//TODO:这里用json做的序列化，以后要改成pb

type EventBody struct {
	H  uint64        //Block Height
	N  uint64        //Sequence Number, M和N唯一决定一个Event
	T  int64         //Timestamps, 用unixNano时间, 一秒等于10的9次方nano，只能表示到1678-2262年
	Tx []common.Hash //Transactions List
	E  []common.Hash //Parents Events, 0 for self parent
	P  bool          //HeartBeat Pulse
}

func (ev *EventBody) Hash() common.Hash {
	return sha256.Sum256(ev.Marshal())
}

func (ev *EventBody) Marshal() []byte {
	b, err := json.Marshal(ev)
	if err != nil {
		logging.Logger.Error("json encode error")
		return nil
	}
	return b
}

type EventMessage struct {
	Body      *EventBody
	signature *crypto.Signature
}

func (em *EventMessage) Marshal() []byte {
	b, err := json.Marshal(em)
	if err != nil {
		logging.Logger.Error("json encode error")
		return nil
	}
	return b
}

func (em *EventMessage) Unmarshal(data []byte) {
	json.Unmarshal(data, em)
}

type Event struct {
	Body *EventBody

	//fields for hash & signature
	signature *crypto.Signature
	hash      common.Hash //带上签名的hash，因为dht中保存的是要带签名的消息
	//hex       string
	vid string //这个从签名中恢复公钥，从公钥来计算出

	//fields for consensus computing
	know        map[string]uint64
	parents     map[string]*Event
	ready       bool //event is ready if all of it's self-parent events has existed and ready
	round       int  //event's round number
	witness     bool
	vote        int
	committable int

	fork []common.Hash //Fork Events, as Invalid
}

func NewEvent(vid string, height uint64, sequenceNumber uint64) *Event {
	body := &EventBody{
		H: height,
		N: sequenceNumber,
		T: time.Now().UnixNano(),
	}

	event := Event{
		Body: body,
		know: make(map[string]uint64),
		//parents:     make(map[string]*Event),
		ready:       false,
		round:       ROUND_UNDECIDED,
		witness:     false,
		vote:        0,
		committable: COMMITTABLE_UNDECIDED,
	}

	event.know[vid] = sequenceNumber
	return &event
}

func NewPulse() *Event {
	body := &EventBody{
		T: time.Now().UnixNano(),
		P: true,
	}

	event := Event{
		Body: body,
	}

	return &event
}

func (e *Event) AddSelfParent(selfParent *Event) {
	if selfParent == nil {
		e.Body.E = append([]common.Hash{HASH0}, e.Body.E...)
	} else {
		e.Body.E = append([]common.Hash{selfParent.Hash()}, e.Body.E...)
	}
}

func (e *Event) AddParents(parents []*Event) {
	for _, parent := range parents {
		e.Body.E = append(e.Body.E, parent.Hash())
	}
}

func (e *Event) AddTransactions(txs []common.Hash) {
	e.Body.Tx = append(e.Body.Tx, txs...)
}

//func (e *Event) Hex() string {
//	if e.hex == "" {
//		hash := e.Hash()
//		e.hex = fmt.Sprintf("0x%X", hash)
//	}
//	return e.hex
//}

func (e *Event) Hash() common.Hash {
	if e.hash == HASH0 {
		h := sha256.Sum256(e.Marshal())
		e.hash = h
	}

	return e.hash
}

func (e *Event) Marshal() []byte {
	em := &EventMessage{
		Body:      e.Body,
		signature: e.signature,
	}
	return em.Marshal()
}

func (e *Event) Unmarshal(data []byte) {
	em := &EventMessage{}
	em.Unmarshal(data)
	e.Body = em.Body
	e.signature = em.signature
}

func (e *Event) Sign(signer crypto.Signer) {
    sig, err := signer.Sign(e.Body.Hash()[:])
    if err != nil {

	}
	e.signature = sig
}

func (e *Event) SignVerify(signer crypto.Signer) bool {
    ret, err := signer.Verify(e.Body.Hash()[:], e.signature)
    if err != nil {

	}
	return ret
	//TODO: 恢复公钥并转换成地址
}

func (e *Event) totalTxAndEvent() int {
	return len(e.Body.Tx) + len(e.Body.E)
}

func (e *Event) totalTx() int {
	return len(e.Body.Tx)
}

func (e *Event) totalEvent() int {
	return len(e.Body.E)
}

//func (e *Event) appendTx(tx []byte) {
//	e.Body.Tx = append(e.Body.Tx, tx)
//}
//
//func (e *Event) appendEvent(event *Event) {
//	p := e.parents[event.address]
//	if p != nil {
//		if event.Body.N > p.Body.N {
//			e.parents[event.address] = event
//			e.updateKnow(event)
//		}
//	} else {
//		e.parents[event.address] = event
//		e.updateKnow(event)
//	}
//}
//
//func (e *Event) setAppendE() {
//	p := e.parents[e.address]
//	if p == nil {
//		e.Body.E = append(e.Body.E, "")
//	} else {
//		e.Body.E = append(e.Body.E, p.Hex())
//	}
//
//	for key, p := range e.parents {
//		if key != e.Body.M {
//			e.Body.E = append(e.Body.E, p.Hex())
//		}
//	}
//
//}

func (e *Event) updateKnow(event *Event) {
	e.know[e.vid] = e.Body.N
	if event != nil {
		for key, value := range event.know {
			if value > e.know[key] {
				e.know[key] = value
			}
		}
	}
}
