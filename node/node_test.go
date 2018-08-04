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

package node

import (
	"path/filepath"
	"testing"

	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/utils/logging"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const testNodeNumber = 4

var nodes [testNodeNumber]*Node

func TestNode(t *testing.T) {
	for i := 0; i < testNodeNumber; i++ {
		config := config.GetDefaultConfig()
		name := "node" + strconv.Itoa(i)
		config.DataDir = filepath.Join(config.DataDir, name)
		var err error
		nodes[i], err = NewNode(config)
		if err != nil {
			logging.Logger.Fatal(err)
		}
		nodes[i].name = name
		nodes[i].Start()
	}

	for i := 0; i < testNodeNumber; i++ {
		nodes[i].p2p.BroadcastMessage(p2p.Message{MsgType: p2p.MessageTypeTx, From: nodes[i].name})
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigc)
	<-sigc
	logging.Logger.Info("Got interrupt, shutting down...")
	for i := 0; i < testNodeNumber; i++ {
		nodes[i].Stop()
	}
}
