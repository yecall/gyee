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
)

type InmemService struct {
}

func NewInmemService() (*InmemService, error) {

	is := &InmemService{}
	return is, nil
}

func (is *InmemService) Start() error {
	return nil
}

func (is *InmemService) Stop() {

}

func (is *InmemService) BroadcastMessage() error {
	return nil
}

func (is *InmemService) BroadcastMessageOsn() error {
	return nil
}

func (is *InmemService) Register() {

}

func (is *InmemService) UnRegister() {

}

//Inmem Hub for all InmemService
//模拟消息的延迟，丢失，dht检索
type InmemHub struct {
	nodes []*InmemService
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
	ih.nodes = append(ih.nodes, node)
}
