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
	"testing"
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

func TestInmemService(t *testing.T){
	ia, _ := NewInmemService()
	ib, _ := NewInmemService()
	ia.Start()
	ib.Start()
	c := make(chan Message, 1)
	sub := NewSubscriber(nil, c, MessageTypeTx )
    ib.Register(sub)

	ia.BroadcastMessage(Message{msgType:MessageTypeTx})
	msg := <-c
	fmt.Println(msg.msgType)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigc)
	<-sigc

	ia.Stop()
	ib.Stop()
}