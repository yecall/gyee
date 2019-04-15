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
	p2pCfg "github.com/yeeco/gyee/p2p/config"
)

const testChainID = uint32(1)

func genTestTxs(t *testing.T,
	quitCh chan struct{}, wg sync.WaitGroup,
	nodes []*node.Node, numNodes uint) {
	wg.Add(1)
	defer wg.Done()

	totalTxs := int(0)
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
	ticker := time.NewTicker(100 * time.Millisecond)
	log.Info("send tx start")
Exit:
	for {
		for i, fn := range nodes {
			select {
			case <-quitCh:
				ticker.Stop()
				break Exit
			case <-ticker.C:
				// send txs
			}
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
				go func(hash *common.Hash, data []byte) {
					msg := &p2p.Message{
						MsgType: p2p.MessageTypeTx,
						Data:    data,
					}
					_ = fn.P2pService().DhtSetValue(hash[:], data)
					_ = fn.P2pService().BroadcastMessage(*msg)
					fn.Core().FakeP2pRecv(msg)
					tn.Core().FakeP2pRecv(msg)
				}(tx.Hash(), data)
				totalTxs++
			}
		}
		log.Info("Total txs sent", totalTxs)
	}
	log.Info("Total txs sent", totalTxs)
}

func doTest(t *testing.T, numNodes uint, duration time.Duration, viewerDelay time.Duration,
	genNode func(*config.Config, *core.Genesis) (*node.Node, error),
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
	genesis, err := genGenesis(keys)
	if err != nil {
		t.Fatalf("genGenesis() %v", err)
	}
	nodes := make([]*node.Node, 0, numNodes)
	for i, key := range keys {
		cfg := dftConfig(filepath.Join(tmpDir, strconv.Itoa(i)), uint16(i))
		cfg.Chain.Key = key

		n, err := genNode(cfg, genesis)
		if err != nil {
			t.Fatalf("genNode() %v", err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("node start %v", err)
		}
		nodes = append(nodes, n)
	}
	for i, n := range nodes {
		log.Info("validator", "index", i, "addr", n.Core().MinerAddr())
	}
	if coroutine != nil {
		go coroutine(quitCh, wg, nodes)
	}
	if viewerDelay > 0 {
		time.Sleep(viewerDelay)
	}
	const numViewers = 2
	viewers := make([]*node.Node, 0, numViewers)
	for i := uint(0); i < numViewers; i++ {
		cfg := dftConfig(filepath.Join(tmpDir, "v"+strconv.Itoa(int(i))), uint16(numNodes+i))
		cfg.Chain.Mine = false

		n, err := genNode(cfg, genesis)
		if err != nil {
			t.Fatalf("genNode() %v", err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("node start %v", err)
		}
		viewers = append(viewers, n)
	}
	time.Sleep(duration - viewerDelay)
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
				reached++
			} else {
				if lastBlock.Hash() != b.Hash() {
					log.Error("block mismatch", "idx", i, "height", height)
					t.Errorf("block mismatch idx %d height %d", i, height)
					mismatch++
				} else {
					reached++
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

func genDefaultNode(cfg *config.Config, genesis *core.Genesis) (*node.Node, error) {
	p2pSvc, err := p2p.NewOsnServiceWithCfg(cfg)
	if err != nil {
		return nil, err
	}
	return node.NewNodeWithGenesis(cfg, genesis, p2pSvc)
}

func genInMemNode(cfg *config.Config, genesis *core.Genesis) (*node.Node, error) {
	p2pSvc, err := p2p.NewInmemService()
	if err != nil {
		return nil, err
	}
	return node.NewNodeWithGenesis(cfg, genesis, p2pSvc)
}

func genKeys(count uint) [][]byte {
	ret := make([][]byte, 0, count)
	for i := uint(0); i < count; i++ {
		key := secp256k1.NewPrivateKey()
		ret = append(ret, key)
	}
	return ret
}

func genGenesis(keys [][]byte) (*core.Genesis, error) {
	count := len(keys)
	initDist := make(map[string]*big.Int)
	validators := make([]string, 0, count)
	for _, key := range keys {
		pub, err := secp256k1.GetPublicKey(key)
		if err != nil {
			return nil, err
		}
		addr, err := address.NewAddressFromPublicKey(pub)
		if err != nil {
			return nil, err
		}
		addrStr := addr.String()
		// setup init dist
		initDist[addrStr] = big.NewInt(1000000000)
		// setup validator
		validators = append(validators, addrStr)
	}
	genesis, err := core.NewGenesis(core.ChainID(testChainID), initDist, validators)
	if err != nil {
		return nil, err
	}
	return genesis, nil
}

func dftConfig(nodeDir string, portShift uint16) *config.Config {
	cfg := &config.Config{
		NodeDir: nodeDir,
		Chain: &config.ChainConfig{
			ChainID: testChainID,
			Mine:    true,
		},
		P2p: &config.P2pConfig{
			AppType: int(p2pCfg.P2P_TYPE_ALL),
			BootNode: []string{
				"E1E6B370C9BDA28A7420DD9BC577ACFDBB335EF7AA38CA43998C921AFBC13834AF1F809C43C524D13A6E7454AA97BADA72EE36A2389A1177630207F04C9B3F8B@13.230.176.195:30304:30304",
			},
			DhtBootstrapNodes: []string{
				"E1E6B370C9BDA28A7420DD9BC577ACFDBB335EF7AA38CA43998C921AFBC13834AF1F809C43C524D13A6E7454AA97BADA72EE36A2389A1177630207F04C9B3F8B@13.230.176.195:40405:40405",
			},
			NodeDataDir:  filepath.Join(nodeDir, "p2p"),
			LocalNodeIp:  "0.0.0.0",
			LocalDhtIp:   "0.0.0.0",
			LocalTcpPort: p2pCfg.DftTcpPort + portShift,
			LocalUdpPort: p2pCfg.DftUdpPort + portShift,
			LocalDhtPort: p2pCfg.DftDhtPort + portShift,
			NatType:      "none",
		},
		Rpc: &config.RpcConfig{},
	}

	return cfg
}
