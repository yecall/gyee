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

/*
 overlay sub-network supported p2p network
*/

package p2p

import (
	"github.com/pkg/errors"
	p2psh "github.com/yeeco/gyee/p2p/shell"
)

type OsnService struct {
	yeShCfg		p2psh.YeShellConfig
	yeShMgr		Service
}

func OsnServiceConfig(cfg *p2psh.YeShellConfig) error {
	return nil
}

func NewOsnService()(*OsnService, error) {
	osns := OsnService{
		yeShCfg: p2psh.DefaultYeShellConfig,
	}
	OsnServiceConfig(&osns.yeShCfg)
	if osns.yeShMgr = p2psh.NewYeShellManager(&osns.yeShCfg); osns.yeShMgr == nil {
		return nil, errors.New("NewOsnService: NewYeShellManager failed")
	}
	return &osns, nil
}

func (osns *OsnService)Start() error {
	return osns.yeShMgr.Start()
}

func (osns *OsnService)Stop() {
	osns.yeShMgr.Stop()
}

func (osns *OsnService)Reconfig(reCfg *RecfgCommand) error {
	return osns.yeShMgr.Reconfig(reCfg)
}

func (osns *OsnService)BroadcastMessage(message Message) error {
	return osns.BroadcastMessage(message)
}

func (osns *OsnService)BroadcastMessageOsn(message Message) error {
	return osns.BroadcastMessageOsn(message)
}

func (osns *OsnService)Register(subscriber *Subscriber) {
	osns.Register(subscriber)
}

func (osns *OsnService)UnRegister(subscriber *Subscriber) {
	osns.UnRegister(subscriber)
}

func (osns *OsnService)DhtGetValue(key []byte) ([]byte, error) {
	return osns.yeShMgr.DhtGetValue(key)
}

func (osns *OsnService)DhtSetValue(key []byte, value []byte) error {
	return osns.yeShMgr.DhtSetValue(key, value)
}


