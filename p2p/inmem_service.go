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
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
	"github.com/yeeco/gyee/utils/logging"
)

type InmemService struct {
	subscribers      *sync.Map
	hub              *InmemHub
	receiveMessageCh chan Message
	cp               ChainProvider

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup

	//用于模拟收发消息的延迟和丢失
	outDelay int //ms
	outMiss  int //0-100
	inDelay  int
	inMiss   int
}

func NewInmemService() (*InmemService, error) {
	is := &InmemService{
		subscribers:      new(sync.Map),
		hub:              GetInmemHub(),
		receiveMessageCh: make(chan Message),
		quitCh:           make(chan struct{}),
		outDelay:         1,
		outMiss:          0,
		inDelay:          1,
		inMiss:           0,
	}
	return is, nil
}

func (is *InmemService) Start() error {
	is.lock.Lock()
	defer is.lock.Unlock()
	is.hub.AddNode(is)
	go is.loop()
	return nil
}

func (is *InmemService) Stop() {
	is.lock.Lock()
	defer is.lock.Unlock()
	is.hub.RemoveNode(is)
	close(is.quitCh)
	is.wg.Wait()
	return
}

func (is *InmemService) loop() {
	is.wg.Add(1)
	defer is.wg.Done()
	//logging.Logger.Info("InmemService loop...")
	for {
		select {
		case <-is.quitCh:
			//logging.Logger.Info("InmemService loop end.")
			return
		case message := <-is.receiveMessageCh:
			t, _ := is.subscribers.Load(message.MsgType)
			if t != nil {
				s, _ := t.(*sync.Map)
				s.Range(func(key, value interface{}) bool {
					key.(*Subscriber).MsgChan <- message
					return true
				})
			}
		}
	}
}

func (is *InmemService) BroadcastMessage(message Message) error {
	return is.hub.Broadcast(is, message)
}

func (is *InmemService) BroadcastMessageOsn(message Message) error {
	is.BroadcastMessage(message)
	return nil
}

func (is *InmemService) Register(subscriber *Subscriber) {
	is.lock.Lock()
	defer is.lock.Unlock()
	t := subscriber.MsgType
	m, _ := is.subscribers.LoadOrStore(t, new(sync.Map))
	m.(*sync.Map).Store(subscriber, true)
}

func (is *InmemService) UnRegister(subscriber *Subscriber) {
	is.lock.Lock()
	defer is.lock.Unlock()
	t := subscriber.MsgType
	m, _ := is.subscribers.Load(t)
	if m == nil {
		return
	}
	m.(*sync.Map).Delete(subscriber)
}

func (is *InmemService) DhtGetValue(key []byte) ([]byte, error) {
	return is.hub.GetValue(key)
}

func (is *InmemService) DhtSetValue(key []byte, value []byte) error {
	return is.hub.SetValue(key, value)
}

func (is *InmemService) Reconfig(reCfg *RecfgCommand) error {
	return nil
}

func (is *InmemService) RegChainProvider(cp ChainProvider) {
	is.cp = cp
}

func (is *InmemService) GetChainInfo(kind string, key []byte) ([]byte, error) {
	// TODO:
	return nil, nil
}

//Inmem Hub for all InmemService
//模拟消息的延迟，丢失，dht检索
type InmemHub struct {
	nodes map[*InmemService]bool
	dht   map[string][]byte
	lock  sync.RWMutex
	ldb   *persistent.LevelStorage
}

var instance *InmemHub
var once sync.Once

func GetInmemHub() *InmemHub {
	once.Do(func() {
		instance = &InmemHub{
			nodes: make(map[*InmemService]bool),
			dht:   make(map[string][]byte),
		}
		dbPath, err := ioutil.TempDir("", "yee-inmemdb")
		if err != nil {
			log.Error("TempDir()", err)
		}
		instance.ldb, err = persistent.NewLevelStorage(dbPath)
		if err != nil {
			logging.Logger.Error(err)
		}
	})
	return instance
}

func (ih *InmemHub) AddNode(node *InmemService) {
	ih.lock.Lock()
	defer ih.lock.Unlock()
	ih.nodes[node] = true
}

func (ih *InmemHub) RemoveNode(node *InmemService) {
	ih.lock.Lock()
	defer ih.lock.Unlock()
	delete(ih.nodes, node)
}

func (ih *InmemHub) Broadcast(from *InmemService, message Message) error {
	ih.lock.RLock()
	defer ih.lock.RUnlock()
	for n, _ := range ih.nodes {
		if n != from {
			if rand.Intn(100) < from.outMiss {
				//fmt.Println("drop:", message.from, message.msgType)
				continue
			}
			go func(to *InmemService) {
				time.Sleep(time.Duration(rand.Intn(from.outDelay)+from.outDelay/2) * time.Millisecond)
				to.receiveMessageCh <- message
			}(n)
		}
	}

	return nil
}

func (ih *InmemHub) GetValue(key []byte) ([]byte, error) {
	ih.lock.RLock()
	defer ih.lock.RUnlock()
	v, err := ih.ldb.Get(key)
	//v, ok := ih.dht[string(key)]
	if err == nil {
		return v, nil
	}
	l := fmt.Sprintf("%d", len(ih.dht))
	return nil, errors.New("key not existed:" + l)
}

func (ih *InmemHub) SetValue(key []byte, value []byte) error {
	ih.lock.Lock()
	defer ih.lock.Unlock()

	ih.ldb.Put(key, value)
	//ih.dht[string(key)] = value
	return nil
}

func (ih *InmemHub) getChainInfo(node *InmemService, kind string, key []byte) ([]byte, error) {
	ih.lock.RLock()
	defer ih.lock.RUnlock()
	for n := range ih.nodes {
		if node == n {
			continue
		}
		value := n.cp.GetChainData(kind, key)
		if len(value) > 0 {
			return value, nil
		}
	}
	return nil, errors.New("not found")
}
