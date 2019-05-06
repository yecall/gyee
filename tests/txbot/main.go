// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"flag"

	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/node"
	p2pCfg "github.com/yeeco/gyee/p2p/config"
)

func main() {
	var (
		localIP    = flag.String("localIP", "0.0.0.0", "node local ip")
		localPort  = flag.Int("localPort", p2pCfg.DftUdpPort, "node local p2p port")
		dhtPort    = flag.Int("dhtPort", p2pCfg.DftDhtPort, "node local dht port")
		ksPassword = flag.String("password", "", "keystore password")
	)
	flag.Parse()

	cfg := config.GetDefaultConfig()
	cfg.P2p.LocalNodeIp = *localIP
	cfg.P2p.LocalTcpPort = (uint16)(*localPort & 0xffff)
	cfg.P2p.LocalUdpPort = (uint16)(*localPort & 0xffff)
	cfg.P2p.LocalDhtIp = *localIP
	cfg.P2p.LocalDhtPort = (uint16)(*dhtPort & 0xffff)

	n, err := node.NewNode(cfg)
	if err != nil {
		log.Crit("node create failure", "err", err)
	}
	if err := n.Start(); err != nil {
		log.Crit("node start failure", "err", err)
	}

	// TODO: send tx in routine
	_ = ksPassword

	n.WaitForShutdown()
}
