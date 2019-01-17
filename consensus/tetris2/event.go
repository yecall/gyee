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
	"time"

	"encoding/binary"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/crypto"
	"errors"
	"github.com/yeeco/gyee/utils/logging"
)

const ROUND_UNDECIDED = -1
const COMMITTABLE_UNDECIDED = -1

type EventBody struct {
	H  uint64        //Block Height
	N  uint64        //Sequence Number
	T  int64         //Timestamps, unix nano, can only represent year 1678-2262
	Tx []common.Hash //Transactions List
	E  []common.Hash //Parents Events, 0 for self parent
	P  bool          //HeartBeat Pulse
}

func (eb *EventBody) Hash() common.Hash {
	m, err := eb.Marshal()
	if err != nil {
		logging.Logger.Warn("eventbody marshal error", err)
	}

	return sha256.Sum256(m)
}

func (eb *EventBody) EmptyLength() int {
	return 8 + 8 + 8 + 2 + 2 + 1
}

func (eb *EventBody) MarshalLength() int {
	return eb.EmptyLength() + 32*len(eb.Tx) + 32*len(eb.E)
}

func (eb *EventBody) MarshalTo(buf []byte) error{
	if eb.MarshalLength() != len(buf) {
		return errors.New("error buffer length for eventbody marshal")
	}

	binary.BigEndian.PutUint64(buf[0:8], eb.H)
	binary.BigEndian.PutUint64(buf[8:16], eb.N)
	binary.BigEndian.PutUint64(buf[16:24], uint64(eb.T))
	binary.BigEndian.PutUint16(buf[24:26], uint16(len(eb.Tx)))
	p := 26
	for i := 0; i < len(eb.Tx); i++ {
		copy(buf[p:p+32], eb.Tx[i][:])
		p += 32
	}
	binary.BigEndian.PutUint16(buf[p:p+2], uint16(len(eb.E)))
	p += 2
	for i := 0; i < len(eb.E); i++ {
		copy(buf[p:p+32], eb.E[i][:])
		p += 32
	}
	if eb.P {
		buf[p] = 1
	} else {
		buf[p] = 0
	}

	return nil
}

func (eb *EventBody) Marshal() ([]byte, error) {
	l := eb.MarshalLength()
	buf := make([]byte, l)

	err := eb.MarshalTo(buf)
	return buf, err
}

func (eb *EventBody) Unmarshal(data []byte) error {
	if len(data) < eb.EmptyLength() {
		return errors.New("error with data length")
	}

	eb.H = binary.BigEndian.Uint64(data[0:8])
	eb.N = binary.BigEndian.Uint64(data[8:16])
	eb.T = int64(binary.BigEndian.Uint64(data[16:24]))
	p := 26
	tl := binary.BigEndian.Uint16(data[24:26])

	if len(data) < eb.EmptyLength() + 32 * int(tl) {
		return errors.New("error with data length")
	}

	eb.Tx = make([]common.Hash, tl)
	for i := uint16(0); i < tl; i++ {
		copy(eb.Tx[i][:], data[p:p+32])
		p += 32
	}

	el := binary.BigEndian.Uint16(data[p : p+2])
	p += 2

	if len(data) < eb.EmptyLength() + 32 * int(tl) + 32 * int(el) {
		return errors.New("error with data length")
	}

	eb.E = make([]common.Hash, el)
	for i := uint16(0); i < el; i++ {
		copy(eb.E[i][:], data[p:p+32])
		p += 32
	}

	if data[p] == 1 {
		eb.P = true
	} else {
		eb.P = false
	}

	return nil
}

type EventMessage struct {
	Body      *EventBody
	Signature *crypto.Signature
}

func (em *EventMessage) Marshal() []byte {
	bl := em.Body.MarshalLength()

	sl := 1 + len(em.Signature.Signature)
	l := 8 + bl + sl
	buf := make([]byte, l)
	p := 0
	binary.BigEndian.PutUint32(buf[p:p+4], uint32(bl))
	p += 4
	em.Body.MarshalTo(buf[p : p+bl])
	p += bl
	binary.BigEndian.PutUint32(buf[p:p+4], uint32(sl))
	p += 4
	buf[p] = byte(em.Signature.Algorithm)
	p += 1
	copy(buf[p:p+len(em.Signature.Signature)], em.Signature.Signature)
	return buf
}

func (em *EventMessage) Unmarshal(data []byte) error {
	dl := len(data)

	p := 0

	if dl < 4 {
		return errors.New("error with data length")
	}

	ebl := binary.BigEndian.Uint32(data[p : p+4])
	p += 4

	if dl < 4 + int(ebl) {
		return errors.New("error with data length")
	}

	em.Body = &EventBody{}
	em.Body.Unmarshal(data[p : p+int(ebl)])
	p += int(ebl)

	if dl < 4 + int(ebl) + 4 {
		return errors.New("error with data length")
	}

	sl := binary.BigEndian.Uint32(data[p : p+4])
	p += 4


	if dl <  4 + int(ebl) + 4 + int(sl) {
		return errors.New("error with data length")
	}

	em.Signature = &crypto.Signature{}
	em.Signature.Algorithm = crypto.Algorithm(data[p])
	p += 1
	em.Signature.Signature = data[p : p+int(sl)-1]

	return nil
}

type Event struct {
	Body *EventBody

	//fields for hash & signature
	signature *crypto.Signature
	hash      common.Hash //hash for the body and signature, it is also the key of dht
	//hex       string
	vid string //It is derived from public key, public key is restored from signature

	//fields for consensus computing
	know        map[string]uint64
	ready       bool //event is ready if all of it's self-parent events has existed and ready
	round       int  //event's round number
	witness     bool
	vote        int
	committable int

	isParent    bool
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
		fork:        make([]common.Hash, 0),
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
	err := em.Unmarshal(data)
	if err != nil {
		logging.Logger.Warn("event unmarshal error.", err)
	}

	e.Body = em.Body
	e.signature = em.Signature
}

func (e *Event) Sign(signer crypto.Signer) error {
	h := e.Body.Hash()
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

func (e *Event) updateKnow(event *Event) {
	if event != nil {
		for key, value := range event.know {
			if value > e.know[key] {
				e.know[key] = value
			}
		}
	}
}
