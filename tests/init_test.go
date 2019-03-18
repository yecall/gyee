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

package tests

import (
	"io/ioutil"
	"math/big"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/node"
	"github.com/yeeco/gyee/p2p"
)

const testChainID = uint32(1)

// build up a chain with random created validators
func TestInit(t *testing.T) {
	doTest(t, 16, 30*time.Second, nil)
}

func TestInitWithTx(t *testing.T) {
	numNodes := uint(16)
	doTest(t, numNodes, 300*time.Second, func(quitCh chan struct{}, wg sync.WaitGroup, nodes []*node.Node) {
		wg.Add(1)
		defer wg.Done()

		addrs := make([]common.Address, numNodes)
		signers := make([]crypto.Signer, numNodes)
		nonces := make([]uint64, numNodes)
		for i, n := range nodes {
			addrs[i] = *n.Core().MinerAddr().CommonAddress()
			if signer, err := n.Core().GetMinerSigner(); err != nil {
				t.Fatalf("signer failed %v", err)
			} else {
				signers[i] = signer
			}
		}
		time.Sleep(30 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			for i, fn := range nodes {
				select {
				case <-quitCh:
					ticker.Stop()
					return
				case <-ticker.C:
					// send txs
				}
				//state, err := fn.Core().Chain().State()
				//if err != nil {
				//	log.Error("get state failed", "err", err)
				//	t.Errorf("get state failed: %v", err)
				//	continue
				//}
				//fAddr := addrs[i]
				//fAccount := state.GetAccount(fAddr, false)
				//if fAccount == nil {
				//	t.Errorf("missing account %v", fAddr)
				//	continue
				//}
				for j, tn := range nodes {
					if j == i {
						continue
					}
					// transfer f => t
					tAddr := &addrs[j]
					tx := core.NewTransaction(testChainID, nonces[i], tAddr, big.NewInt(100))
					if err := tx.Sign(signers[i]); err != nil {
						t.Errorf("sign failed %v", err)
						continue
					}
					data, err := tx.Encode()
					if err != nil {
						t.Errorf("encode tx failed %v", err)
						continue
					}
					nonces[i]++
					msg := &p2p.Message{
						MsgType: p2p.MessageTypeTx,
						Data:    data,
					}
					_ = fn.P2pService().DhtSetValue(tx.Hash()[:], data)
					_ = fn.P2pService().BroadcastMessage(*msg)
					fn.Core().FakeP2pRecv(msg)
					tn.Core().FakeP2pRecv(msg)
				}
			}
		}
	})
}

func doTest(t *testing.T, numNodes uint, duration time.Duration,
	coroutine func(chan struct{}, sync.WaitGroup, []*node.Node)) {
	var (
		quitCh = make(chan struct{})
		wg     sync.WaitGroup
	)
	tmpDir, err := ioutil.TempDir("", "yee-test-")
	if err != nil {
		t.Fatalf("TempDir() %v", err)
	}
	keys := genKeys(numNodes)
	genesis := genGenesis(t, keys)
	nodes := make([]*node.Node, 0, numNodes)
	for i, key := range keys {
		cfg := dftConfig()
		cfg.NodeDir = filepath.Join(tmpDir, strconv.Itoa(i))
		cfg.Chain.Key = key

		n, err := node.NewNodeWithGenesis(cfg, genesis)
		if err != nil {
			t.Fatalf("newNode() %v", err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("node start %v", err)
		}
		nodes = append(nodes, n)
	}
	if coroutine != nil {
		go coroutine(quitCh, wg, nodes)
	}
	time.Sleep(duration)
	close(quitCh)
	wg.Wait()
	// check node chains
	for height := uint64(0); ; height++ {
		var (
			lastBlock *core.Block
			reached   = int(0)
			mismatch  = int(0)
		)
		for i, n := range nodes {
			if height > n.Core().Chain().CurrentBlockHeight() {
				continue
			}
			b := n.Core().Chain().GetBlockByNumber(height)
			if b == nil {
				log.Error("block not found", "idx", i, "node", n, "height", height)
				t.Errorf("block not found idx %d height %d", i, height)
				continue
			}
			if lastBlock == nil {
				lastBlock = b
				reached ++
			} else {
				if lastBlock.Hash() != b.Hash() {
					log.Error("block mismatch", "idx", i, "height", height)
					t.Errorf("block mismatch idx %d height %d", i, height)
					mismatch ++
				} else {
					reached ++
				}
			}
		}
		if lastBlock == nil {
			// no node reached
			break
		}
		log.Info("chain check", "height", height, "hash", lastBlock.Hash())
		log.Info("    stats", "reached", reached, "mismatch", mismatch)
	}
	// stop nodes
	for _, n := range nodes {
		_ = n.Stop()
	}
}

func genKeys(count uint) [][]byte {
	ret := make([][]byte, 0, count)
	for i := uint(0); i < count; i++ {
		key := secp256k1.NewPrivateKey()
		ret = append(ret, key)
	}
	return ret
}

func genGenesis(t *testing.T, keys [][]byte) *core.Genesis {
	count := len(keys)
	genesis := &core.Genesis{
		ChainID:     core.ChainID(testChainID),
		InitYeeDist: make([]core.InitYeeDist, 0, count),
	}
	validators := make([]string, 0, count)
	for _, key := range keys {
		pub, err := secp256k1.GetPublicKey(key)
		if err != nil {
			t.Fatalf("GetPublicKey() %v", err)
		}
		addr, err := address.NewAddressFromPublicKey(pub)
		if err != nil {
			t.Fatalf("NewAddressFromPublicKey() %v", err)
		}
		addrStr := addr.String()
		// setup init dist
		genesis.InitYeeDist = append(genesis.InitYeeDist, core.InitYeeDist{
			Address: addrStr, Value: "1000000000",
		})
		// setup validator
		validators = append(validators, addrStr)
	}
	genesis.Consensus.Tetris.Validators = validators
	return genesis
}

func dftConfig() *config.Config {
	cfg := &config.Config{
		Chain: &config.ChainConfig{
			ChainID: testChainID,
			Mine:    true,
		},
		Rpc: &config.RpcConfig{},
	}

	return cfg
}
