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

import "time"

const (
	DhtGetDftTimeout = 60 * time.Second
)

/*
inmem_service: 测试用inmem network
p2p_service: 全广播p2p network
osn_service: overlay sub-network
还有一个做法是hash全网广播，内容接收者自己去dht取？
消息类型：
1. tx，node发出，临时dht存储，发往validator group，发自己所在子网
2. block header：validator发出，发往全体。全体
3. block：dht存储
4. event：validator发出，临时dht存储，发往validator group，随机选取子网

注册消息处理handler，dispatch message
消息去重
消息格式：消息类型及payload？由应用层自己去解析
*/

type RecfgCommand struct {
	Validator      bool // is validator
	SubnetMaskBits int  // mask bits for sub network identity
}

type ChainProvider interface {
	GetChainData(kind string, key []byte) []byte
}

type Service interface {
	Start() error
	Stop()
	Ready()

	Reconfig(reCfg *RecfgCommand) error
	BroadcastMessage(message Message) error
	//如果在多个子网，随机选一个。如果单一子网，直接发布。
	BroadcastMessageOsn(message Message) error
	Register(subscriber *Subscriber)
	UnRegister(subscriber *Subscriber)

	DhtGetValue(key []byte) ([]byte, error)
	DhtGetValues(keys [][]byte, out chan<- []byte, timeout time.Duration) error
	DhtSetValue(key []byte, value []byte) error

	// p2p service get chain data from provider
	RegChainProvider(cp ChainProvider)

	// ask peer for chain info
	GetChainInfo(kind string, key []byte) ([]byte, error)
}
