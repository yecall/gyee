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
	"errors"
	"flag"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/keystore"
	"github.com/yeeco/gyee/crypto/secp256k1"
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

	//*ksPassword = strings.Split(*ksPassword, "\n")[0]
	if len(*ksPassword) == 0 {
		log.Crit("must provide keystore password")
	}

	cfg := config.GetDefaultConfig()
	cfg.P2p.LocalNodeIp = *localIP
	cfg.P2p.LocalTcpPort = (uint16)(*localPort & 0xffff)
	cfg.P2p.LocalUdpPort = (uint16)(*localPort & 0xffff)
	cfg.P2p.LocalDhtIp = *localIP
	cfg.P2p.LocalDhtPort = (uint16)(*dhtPort & 0xffff)

	// load accounts
	signers, addrs, err := loadAccounts(cfg, *ksPassword)
	if err != nil {
		log.Crit("account load failed", "err", err)
	}

	// create node
	n, err := node.NewNode(cfg)
	if err != nil {
		log.Crit("node create failure", "err", err)
	}

	// start node
	if err := n.Start(); err != nil {
		log.Crit("node start failure", "err", err)
	}

	// start tx generator
	var (
		quitCh = make(chan struct{})
		wg     sync.WaitGroup
	)
	go genTxs(n, signers, addrs, quitCh, wg)

	n.WaitForShutdown()

	// notify generator to stop
	close(quitCh)
	wg.Wait()
}

func loadAccounts(cfg *config.Config, password string) ([]crypto.Signer, []common.Address, error) {
	ks := keystore.NewKeystoreWithConfig(cfg)
	addrList := ks.List()
	if len(addrList) == 0 {
		return nil, nil, errors.New("no local account found")
	}

	signers := make([]crypto.Signer, 0)
	addrs := make([]common.Address, 0)
	for _, addr := range addrList {
		key, err := ks.GetKey(addr, []byte(password))
		if err != nil {
			log.Warn("failed to load account", "addr", addr, "err", err)
			continue
		}
		pub, err := secp256k1.GetPublicKey(key)
		if err != nil {
			log.Warn("failed to gen pubkey", "addr", addr, "err", err)
			continue
		}
		addr, err := address.NewAddressFromPublicKey(pub)
		signer := secp256k1.NewSecp256k1Signer()
		if err := signer.InitSigner(key); err != nil {
			log.Warn("failed to prepare signer", "addr", addr, "err", err)
			continue
		}
		signers = append(signers, signer)
		addrs = append(addrs, *addr.CommonAddress())
	}
	if len(signers) < 2 {
		return nil, nil, fmt.Errorf("at least two accounts needed for txBot, got %d", len(signers))
	}
	log.Info("accounts loaded for txBot", "cnt", len(signers))
	return signers, addrs, nil
}

func genTxs(n *node.Node, signers []crypto.Signer, addrs []common.Address, quitCh chan struct{}, wg sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	if len(signers) != len(addrs) {
		log.Crit("signer/addrs not match", "signers", signers, "addrs", addrs)
	}

	// wait before sending txs
	time.Sleep(20 * time.Second)

	c := n.Core()
	chainID := uint32(c.Chain().ChainID())
	nonces := make([]uint64, len(signers))

	// generator loop
	round := int(0)
	totalTxs := int(0)
	ticker := time.NewTicker(1000 * time.Millisecond)
Exit:
	for {
		select {
		case <-quitCh:
			ticker.Stop()
			break Exit
		case <-ticker.C:
			// send txs
		}
		// reset inMem nonce if needed
		if round%10000 == 0 {
			log.Info("nonce reset")
			if round > 0 {
				time.Sleep(10 * time.Second)
			}
			nonces = make([]uint64, len(signers))
			for i, addr := range addrs {
				account := c.Chain().LastBlock().GetAccount(addr)
				if account == nil {
					nonces[i] = 0
				} else {
					nonces[i] = account.Nonce()
				}
			}
		}
		// batch transfer
		for i, signer := range signers {
			for j, toAddr := range addrs {
				if j == i {
					continue
				}
				tx := core.NewTransaction(chainID, nonces[i], &toAddr, big.NewInt(100))
				if err := tx.Sign(signer); err != nil {
					log.Error("tx sign failed", "err", err)
					continue
				}
				if err := c.TxBroadcast(tx); err != nil {
					log.Error("tx broadcast failed", "err", err)
					continue
				}
				nonces[i]++
				totalTxs++
			}
		}
		log.Info("Total txs sent", totalTxs)
		round++
	}
	log.Info("Total txs sent", totalTxs)
}
