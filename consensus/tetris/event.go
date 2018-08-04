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

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/yeeco/gyee/utils/logging"
	"time"
)

const ROUND_UNDECIDED = -1
const FAMOUS_UNDECIDED = -1

type EventBody struct {
	H  uint64   //Block Height
	M  uint     //MemberID
	N  uint64   //Sequence Number, M和N唯一决定一个Event
	T  int64    //Timestamps, 用unixNano时间, 一秒等于10的9次方nano，只能表示到1678-2262年
	Tx []string //Transactions List
	E  []string //Parents Events, 0 for self parent
	F  []string //Fork Events, as Invalid
}

type Event struct {
	Body EventBody

	//fields for hash & signature
	hash      []byte
	hex       string
	signature string

	//fields for consensus computing
	know    map[uint]uint64
	parents map[uint]*Event
	ready   bool //event is ready if all of it's self-parent events has existed and ready
	round   int  //event's round number
	witness bool
	vote    int
	famous  int
}

func NewEvent(height uint64, member uint, sequenceNumber uint64) Event {
	body := EventBody{
		H: height,
		M: member,
		N: sequenceNumber,
		T: time.Now().UnixNano(),
	}

	event := Event{
		Body:    body,
		know:    make(map[uint]uint64),
		parents: make(map[uint]*Event),
		ready:   false,
		round:   ROUND_UNDECIDED,
		witness: false,
		vote:    0,
		famous:  FAMOUS_UNDECIDED,
	}

	event.know[member] = sequenceNumber
	return event
}

func (e *Event) Hex() string {
	if e.hex == "" {
		hash := e.Hash()
		e.hex = fmt.Sprintf("0x%X", hash)
	}
	return e.hex
}

func (e *Event) Hash() []byte {
	if len(e.hash) == 0 {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		if err := enc.Encode(e.Body); err != nil {
			logging.Logger.Error("encode error")
			return nil
		}
		h := sha256.Sum256(b.Bytes())
		e.hash = h[:]
	}

	return e.hash
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

func (e *Event) appendTx(tx string) {
	e.Body.Tx = append(e.Body.Tx, tx)
}

func (e *Event) appendEvent(event *Event) {
	p := e.parents[event.Body.M]
	if p != nil {
		if event.Body.N > p.Body.N {
			e.parents[event.Body.M] = event
			e.updateKnow(event)
		}
	} else {
		e.parents[event.Body.M] = event
		e.updateKnow(event)
	}
	//e.Body.E = append(e.Body.E, event)
}

func (e *Event) setAppendE() {
	p := e.parents[e.Body.M]
	if p == nil {
		e.Body.E = append(e.Body.E, "")
	} else {
		e.Body.E = append(e.Body.E, p.Hex())
		//delete(e.parents, e.Body.M)
	}

	for key, p := range e.parents {
		if key != e.Body.M {
			e.Body.E = append(e.Body.E, p.Hex())
		}
	}

}

func (e *Event) updateKnow(event *Event) {
	e.know[e.Body.M] = e.Body.N
	if event != nil {
		for key, value := range event.know {
			if value > e.know[key] {
				e.know[key] = value
			}
		}
	}
}
