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
	"os"
	"fmt"
	"flag"
	"os/signal"
	"crypto/ecdsa"

	"github.com/yeeco/gyee/log"
	p2pcfg	"github.com/yeeco/gyee/p2p/config"
	"github.com/yeeco/gyee/p2p"
)

func main() {
	var (
		genKey      = flag.String("genkey", "", "generate node key to file")
		writeNodeID = flag.Bool("writenodeid", false, "write out the node's id and quit")
		nodeKeyFile = flag.String("nodekey", "", "private key filename")

		nodeKey *ecdsa.PrivateKey
		err     error
	)
	flag.Parse()

	switch {
	case *genKey != "":
		nodeKey, err = p2pcfg.GenerateKey()
		if err != nil {
			log.Crit("failed to generate nodekey", "err", err)
		}
		if err = p2pcfg.SaveECDSA(*genKey, nodeKey); err != nil {
			log.Crit("failed to save nodekey", "err", err)
		}
		return
	case *nodeKeyFile != "":
		nodeKey, err = p2pcfg.LoadECDSA(*nodeKeyFile)
		if err != nil {
			log.Crit("failed to load nodekey", "err", err)
		}
	}

	if nodeKey == nil {
		log.Crit("nodeKey not provided")
	}

	if *writeNodeID {
		nodeID := p2pcfg.P2pPubkey2NodeId(&nodeKey.PublicKey)
		if nodeID == nil {
			log.Crit("failed to parse nodeID")
		}
		fmt.Printf("%x\n", *nodeID)
		os.Exit(0)
	}

	// 除非有特殊的需求，不需要上面的代码通过命令行进行配置，下面即启动了一个 bootstrap node。
	// 里面的本地节点IP地址和DHT的IP地址要设置一下，否则代码自己去决定的，得到的不一定是正确
	// 的结果。这两个IP地址可以相同。另bootnode不需要知道自己的外部地址和端口。

	nodeCfg := p2p.DefaultYeShellConfig
	nodeCfg.Name				= "bootnode"
	nodeCfg.BootstrapNode		= true
	nodeCfg.Validator			= false
	nodeCfg.SubNetMaskBits		= 0
	nodeCfg.NatType				= p2pcfg.NATT_NONE
	nodeCfg.LocalNodeIp			= "x1.x2.x3.x4"
	nodeCfg.LocalDhtIp			= "y1.y2.y3.y4"
	nodeCfg.BootstrapNodes		= make([]string, 0)
	nodeCfg.DhtBootstrapNodes	= make([]string, 0)
	bootNode, err := p2p.NewOsnService(&nodeCfg)
	if err != nil {
		log.Crit("failed to create bootnode")
		os.Exit(-1)
	} else if err := bootNode.Start(); err != nil {
		log.Crit("failed to start bootnode")
		os.Exit(-2)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	<-sig
	bootNode.Stop()

	os.Exit(0)
}
