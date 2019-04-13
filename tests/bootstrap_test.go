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
	"sync"
	"testing"
	"time"

	"github.com/yeeco/gyee/node"
)

func TestLocalBootstrapNoTx(t *testing.T) {
	numNodes := uint(16)
	doTest(t, numNodes, 60*time.Second, genInMemNode, nil)
}

func TestLocalBootstrapWithTx(t *testing.T) {
	numNodes := uint(16)
	doTest(t, numNodes, 300*time.Second, genInMemNode,
		func(quitCh chan struct{}, wg sync.WaitGroup, nodes []*node.Node) {
			genTestTxs(t, quitCh, wg, nodes, numNodes)
		})
}

func TestBootstrapNoTx(t *testing.T) {
	numNodes := uint(16)
	doTest(t, numNodes, 60*time.Second, genDefaultNode, nil)
}

func TestBootstrapWithTx(t *testing.T) {
	numNodes := uint(16)
	doTest(t, numNodes, 300*time.Second, genDefaultNode,
		func(quitCh chan struct{}, wg sync.WaitGroup, nodes []*node.Node) {
			genTestTxs(t, quitCh, wg, nodes, numNodes)
		})
}
