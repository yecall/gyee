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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/node"
)

const numNodes = int(16)

// build up a chain with random created validators
func TestInit(t *testing.T) {
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
	time.Sleep(30 * time.Second)
	for _, n := range nodes {
		_ = n.Stop()
	}
}

func genKeys(count int) [][]byte {
	ret := make([][]byte, 0, count)
	for i := int(0); i < count; i++ {
		key := secp256k1.NewPrivateKey()
		ret = append(ret, key)
	}
	return ret
}

func genGenesis(t *testing.T, keys [][]byte) *core.Genesis {
	count := len(keys)
	genesis := &core.Genesis{
		ChainID:     1,
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
			ChainID: 1,
			Mine:    true,
		},
		Rpc: &config.RpcConfig{
		},
	}

	return cfg
}
