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

	"github.com/yeeco/gyee/log"
	p2pcfg "github.com/yeeco/gyee/p2p/config"
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

	// TODO: start node with nodeKey
}
