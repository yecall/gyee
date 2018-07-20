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

package p2p

import (
	"math/rand"
	"testing"
	//"os"
	//"os/signal"
	//"syscall"
	"fmt"
	"time"
)

func TestInmemService(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ia, _ := NewInmemService()
	ia.Start()
	ca := make(chan Message, 1)
	sa := NewSubscriber(nil, ca, MessageTypeTx)
	ia.Register(sa)

	ib, _ := NewInmemService()
	ib.Start()
	cb := make(chan Message, 1)
	sb := NewSubscriber(nil, cb, MessageTypeTx)
	ib.Register(sb)

	ic, _ := NewInmemService()
	ic.Start()
	cc := make(chan Message, 1)
	sc := NewSubscriber(nil, cc, MessageTypeTx)
	ic.Register(sc)

	id, _ := NewInmemService()
	id.Start()
	cd := make(chan Message, 1)
	sd := NewSubscriber(nil, cd, MessageTypeTx)
	sde := NewSubscriber(nil, cd, MessageTypeEvent)
	id.Register(sd)
	id.Register(sde)

	go func() {
		for {
			select {
			case message := <-ca:
				fmt.Printf("A receive %v from %v\n", message.MsgType, message.From)
			case message := <-cb:
				fmt.Printf("B receive %v from %v\n", message.MsgType, message.From)
			case message := <-cc:
				fmt.Printf("C receive %v from %v\n", message.MsgType, message.From)
			case message := <-cd:
				fmt.Printf("D receive %v from %v\n", message.MsgType, message.From)
			}
		}
	}()

	ia.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "A"})
	ia.BroadcastMessage(Message{MsgType: MessageTypeEvent, From: "A"})
	ib.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "B"})
	ic.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "C"})
	id.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "D"})

	time.Sleep(2 * time.Second)
	id.UnRegister(sd)
	fmt.Println()

	ia.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "A"})
	ia.BroadcastMessage(Message{MsgType: MessageTypeEvent, From: "A"})
	ib.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "B"})
	ic.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "C"})
	id.BroadcastMessage(Message{MsgType: MessageTypeTx, From: "D"})

	time.Sleep(2 * time.Second)

	ia.DhtSetValue([]byte("keyA"), []byte("valueA"))
	v, err := ib.DhtGetValue([]byte("keyA"))
	fmt.Println("dht:", string(v), err)

	n, err := ib.DhtGetValue([]byte("key not existed"))
	fmt.Println("dht:", string(n), err)

	//sigc := make(chan os.Signal, 1)
	//signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	//defer signal.Stop(sigc)
	//<-sigc

	time.Sleep(time.Second)

	ia.Stop()
	ib.Stop()
	ic.Stop()
	id.Stop()
}
