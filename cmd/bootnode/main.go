// Copyright (C) 2018 gyee authors
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
	"crypto/ecdsa"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
	p2pcfg "github.com/yeeco/gyee/p2p/config"
)

func main() {
	var (
		genKey      = flag.Bool("genkey", false, "generate node key to file")
		writeNodeID = flag.Bool("writenodeid", false, "write out the node's id and quit")
		nodeDataDir = flag.String("nodeDataDir", "", "node data directory")
		nodeName    = flag.String("nodeName", "", "node name")
		chainIp     = flag.String("cip", "", "chain ip(b1.b2.b3.b4)")
		chainPort   = flag.Int("cport", 0, "chain port")
		dhtIp       = flag.String("dip", "", "dht ip(b1.b2.b3.b4)")
		dhtPort     = flag.Int("dport", 0, "dht port")
		nodeKey     *ecdsa.PrivateKey
		err         error
	)
	flag.Parse()

	if *genKey {
		if *nodeDataDir == "" || *nodeName == "" {
			log.Crit("nodeDataDir and nodeName must not be empty", "err", err)
			os.Exit(-1)
		}
		kf := filepath.Join(*nodeDataDir, *nodeName, p2pcfg.KeyFileName)
		nodeKey, err = p2pcfg.GenerateKey()
		if err != nil {
			log.Crit("failed to generate nodekey", "err", err)
		}
		if err = p2pcfg.SaveECDSA(kf, nodeKey); err != nil {
			log.Crit("failed to save nodekey", "err", err)
			os.Exit(-1)
		}
		fmt.Printf("key saved ok to %s", kf)
		os.Exit(0)
	}

	if *writeNodeID {
		if *nodeDataDir == "" || *nodeName == "" {
			log.Crit("nodeDataDir and nodeName must not be empty")
			os.Exit(-1)
		}
		kf := filepath.Join(*nodeDataDir, *nodeName, p2pcfg.KeyFileName)
		nodeKey, err = p2pcfg.LoadECDSA(kf)
		if err != nil {
			log.Crit("failed to load nodekey", "err", err)
			os.Exit(-1)
		}
		nodeID := p2pcfg.P2pPubkey2NodeId(&nodeKey.PublicKey)
		if nodeID == nil {
			log.Crit("failed to parse nodeID")
			os.Exit(-1)
		}
		fmt.Printf("\n\t%x\n", *nodeID)
		os.Exit(0)
	}

	if (*nodeDataDir != "" && *nodeName == "") || (*nodeDataDir == "" && *nodeName != "") {
		log.Crit("nodeDataDir and nodeName must all be empty or all be not empty")
		os.Exit(-1)
	}

	nodeCfg := p2p.DefaultYeShellConfig
	if *nodeDataDir != "" && *nodeName != "" {
		nodeCfg.NodeDataDir = *nodeDataDir
		nodeCfg.Name = *nodeName
	}
	if *chainIp != "" {
		nodeCfg.LocalNodeIp = *chainIp
	}
	if *chainPort != 0 {
		nodeCfg.LocalTcpPort = (uint16)(*chainPort & 0xffff)
		nodeCfg.LocalUdpPort = (uint16)(*chainPort & 0xffff)
	}
	if *dhtIp != "" {
		nodeCfg.LocalDhtIp = *dhtIp
	}
	if *dhtPort != 0 {
		nodeCfg.LocalDhtPort = (uint16)(*dhtPort & 0xffff)
	}
	nodeCfg.BootstrapNode = true
	nodeCfg.Validator = false
	nodeCfg.SubNetMaskBits = 0
	nodeCfg.NatType = p2pcfg.NATT_NONE
	nodeCfg.BootstrapNodes = make([]string, 0)
	nodeCfg.DhtBootstrapNodes = make([]string, 0)
	bootNode, err := p2p.NewOsnService(&nodeCfg)
	if err != nil {
		log.Crit("failed to create bootnode")
		os.Exit(-2)
	} else if err := bootNode.Start(); err != nil {
		log.Crit("failed to start bootnode")
		os.Exit(-3)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	<-sig
	bootNode.Stop()
	os.Exit(0)
}
