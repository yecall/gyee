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
	"github.com/yeeco/gyee/utils"
	"sync"
	"container/list"
)

type InmemService struct {
	subscribers  *sync.Map
	hub          *InmemHub
	element      *list.Element  //这个搞法有点丑陋的，不过反正是测试代码没关系
}

func NewInmemService() (*InmemService, error) {

	is := &InmemService{
		subscribers: new(sync.Map),
		hub:GetInmemHub(),
	}
	return is, nil
}

func (is *InmemService) Start() error {
	is.hub.AddNode(is)
	return nil
}

func (is *InmemService) Stop() {
    is.hub.RemoveNode(is)
    return
}

func (is *InmemService) BroadcastMessage(message Message) error {
	return nil
}

func (is *InmemService) BroadcastMessageOsn(message Message) error {
	return nil
}

func (is *InmemService) Register(subscriber *Subscriber) {

}

func (is *InmemService) UnRegister(subscriber *Subscriber) {

}

func (is *InmemService) dhtGetValue(key interface{}) []byte {
	return nil
}

func (is *InmemService) dhtSetValue(key interface{}, value []byte) {

}

//Inmem Hub for all InmemService
//模拟消息的延迟，丢失，dht检索
type InmemHub struct {
	nodes list.List
	dht   *utils.LRU
}

var instance *InmemHub
var once sync.Once

func GetInmemHub() *InmemHub {
	once.Do(func() {
		instance = &InmemHub{
			nodes: nil,
			dht: utils.NewLRU(10000, nil),
		}
	})
	return instance
}

func (ih *InmemHub) AddNode(node *InmemService) {
	node.element = ih.nodes.PushBack(node)
}

func (ih *InmemHub) RemoveNode(node *InmemService) {
	ih.nodes.Remove(node.element)
}
