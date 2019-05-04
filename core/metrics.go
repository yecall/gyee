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

package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/yeeco/gyee/log"
)

var (
	p2pDhtSetMeter  = metrics.NewRegisteredMeter("core/p2p/dht/set", nil)
	p2pDhtGetMeter  = metrics.NewRegisteredMeter("core/p2p/dht/get", nil)
	p2pDhtHitMeter  = metrics.NewRegisteredMeter("core/p2p/dht/hit", nil)
	p2pDhtMissMeter = metrics.NewRegisteredMeter("core/p2p/dht/miss", nil)

	p2pMsgSent     = metrics.NewRegisteredMeter("core/p2p/msg/sent", nil)
	p2pMsgSendFail = metrics.NewRegisteredMeter("core/p2p/msg/fail", nil)
	p2pMsgRecv     = metrics.NewRegisteredMeter("core/p2p/msg/recv", nil)

	p2pChainInfoGet    = metrics.NewRegisteredMeter("core/p2p/cInfo/get", nil)
	p2pChainInfoHit    = metrics.NewRegisteredMeter("core/p2p/cInfo/hit", nil)
	p2pChainInfoAnswer = metrics.NewRegisteredMeter("core/p2p/cInfo/answer", nil)
)

func printMetrics() {
	m := make(map[string]string)
	m["dhtSet"] = fmt.Sprintf("%d", p2pDhtSetMeter.Count())
	m["dhtGet"] = fmt.Sprintf("h%d m%d / total%d",
		p2pDhtHitMeter.Count(), p2pDhtMissMeter.Count(), p2pDhtGetMeter.Count())

	m["msgSend"] = fmt.Sprintf("f%d / total%d", p2pMsgSendFail.Count(), p2pMsgSent.Count())
	m["msgRecv"] = fmt.Sprintf("%d", p2pMsgRecv.Count())

	m["cInfoGet"] = fmt.Sprintf("%d / %d", p2pChainInfoHit.Count(), p2pChainInfoGet.Count())
	m["cInfoAns"] = fmt.Sprintf("%d", p2pChainInfoAnswer.Count())

	log.Info("core metrics", m)
}
