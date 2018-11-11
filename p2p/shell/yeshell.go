/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  the gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  the gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package shell

import (
	yep2p "github.com/yeeco/gyee/p2p"
)

type yeService = yep2p.Service

type yeShellManager struct {}

func (yeShMgr *yeShellManager)Start() error {
	return nil
}

func (yeShMgr *yeShellManager)Stop() {
	return
}

func (yeShMgr *yeShellManager)BroadcastMessage(message yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)BroadcastMessageOsn(message yep2p.Message) error {
	return nil
}

func (yeShMgr *yeShellManager)Register(subscriber *yep2p.Subscriber) {
	return
}

func (yeShMgr *yeShellManager)UnRegister(subscriber *yep2p.Subscriber) {
	return
}

func (yeShMgr *yeShellManager)DhtGetValue(key []byte) ([]byte, error) {
	return nil, nil
}

func (yeShMgr *yeShellManager)DhtSetValue(key []byte, value []byte) error {
	return nil
}
