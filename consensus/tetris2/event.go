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
	//"encoding/json"
	"time"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/crypto"
	//"github.com/yeeco/gyee/utils/logging"
	"encoding/binary"
	"fmt"
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
	l := 25 + 32 * len(ev.Tx) + 32 * len(ev.E) + 4
	buf := make([]byte, l)
	binary.BigEndian.PutUint64(buf[0:8], ev.H)
	binary.BigEndian.PutUint64(buf[8:16], ev.N)
	binary.BigEndian.PutUint64(buf[16:24], uint64(ev.T))
	binary.BigEndian.PutUint16(buf[24:26], uint16(len(ev.Tx)))
	p := 26
	for i:=0; i<len(ev.Tx); i++ {
		copy(buf[p:p+32], ev.Tx[i][:])
		p += 32
	}
	binary.BigEndian.PutUint16(buf[p:p+2], uint16(len(ev.E)))
	p += 2
	for i:=0; i<len(ev.E); i++ {
		copy(buf[p:p+32], ev.E[i][:])
		p += 32
	}
	if ev.P {
		buf[p] = 1
	} else {
		buf[p] = 0
	}

	p++

	if p != l {
		fmt.Println("err:", p, l)
	}
	return buf
	/*
	b, err := json.Marshal(ev)
	if err != nil {
		logging.Logger.Error("json encode error")
		return nil
	}
	return b
	*/
}

func (ev *EventBody) Unmarshal(data []byte) {
	ev.H = binary.BigEndian.Uint64(data[0:8])
	ev.N = binary.BigEndian.Uint64(data[8:16])
	ev.T = int64(binary.BigEndian.Uint64(data[16:24]))
	p := 26
	l := binary.BigEndian.Uint16(data[24:26])
	ev.Tx = make([]common.Hash, l)
	for i:=uint16(0); i<l; i++ {
		copy(ev.Tx[i][:], data[p:p+32])
		p += 32
	}
	l = binary.BigEndian.Uint16(data[p:p+2])
	p += 2
	ev.E = make([]common.Hash, l)
	for i:=uint16(0); i<l; i++ {
		copy(ev.E[i][:], data[p:p+32])
		p += 32
	}

	if data[p] == 1 {
		ev.P = true
	} else {
		ev.P = false
	}
}

type EventMessage struct {
	Body      *EventBody
	Signature *crypto.Signature
}

func (em *EventMessage) Marshal() []byte {
	b := em.Body.Marshal()
    s := 1 + len(em.Signature.Signature)
    l := 8 + len(b) + s
	buf := make([]byte, l)
	p := 0
	binary.BigEndian.PutUint32(buf[p:p+4], uint32(len(b)))
	p += 4
	copy(buf[p:p+len(b)], b)
	p += len(b)
	binary.BigEndian.PutUint32(buf[p:p+4], uint32(s))
	p += 4
	buf[p] = byte(em.Signature.Algorithm)
	p += 1
	copy(buf[p:p+len(em.Signature.Signature)], em.Signature.Signature)
	return buf
	/*
	b, err := json.Marshal(em)
	if err != nil {
		logging.Logger.Error("json encode error")
		return nil
	}
	return b
	*/
}

func (em *EventMessage) Unmarshal(data []byte) {
	p := 0
    l := binary.BigEndian.Uint32(data[p:p+4])
    p += 4
    em.Body = &EventBody{}
    em.Body.Unmarshal(data[p:p+int(l)])
    p += int(l)
    l = binary.BigEndian.Uint32(data[p:p+4])
    p += 4
    em.Signature = &crypto.Signature{}
    em.Signature.Algorithm = crypto.Algorithm(data[p])
    p += 1
    em.Signature.Signature = data[p:p+int(l)-1]
	/*
	json.Unmarshal(data, em)
	*/
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
		Body:        body,
		vid:         vid,
		know:        make(map[string]uint64),
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
	e.updateKnow(selfParent)
}

func (e *Event) AddParents(parents []*Event) {
	for _, parent := range parents {
		e.Body.E = append(e.Body.E, parent.Hash())
		e.updateKnow(parent)
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
		Signature: e.signature,
	}
	return em.Marshal()
}

func (e *Event) Unmarshal(data []byte) {
	em := &EventMessage{}
	em.Unmarshal(data)
	e.Body = em.Body
	e.signature = em.Signature
	//fmt.Println("len:", len(data))
	//fmt.Println("raw:", len(e.Body.Tx)*32 + len(e.Body.E)*32 + 100)
}

func (e *Event) Sign(signer crypto.Signer) error {
	h :=  e.Body.Hash()
	sig, err := signer.Sign(h[:])
	if err != nil {
		return err
	}
	e.signature = sig
	return nil
}

func (e *Event) RecoverPublicKey(signer crypto.Signer) ([]byte, error) {
	h := e.Body.Hash()
	pk, err := signer.RecoverPublicKey(h[:], e.signature)
	return pk, err
}

func (e *Event) SignVerify(publicKey []byte, signer crypto.Signer) bool {
	h := e.Body.Hash()
	ret := signer.Verify(publicKey, h[:], e.signature)
	return ret
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
