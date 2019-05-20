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
	"sort"
	"strings"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/log"
)

func organizeTxs(state state.AccountTrie, txs Transactions) (out Transactions, dropped Transactions) {
	txsRoot := DeriveHash(txs)
	log.Info("organizeTxs", "cnt", len(txs), "txsRoot", txsRoot)
	var (
		output   Transactions
		nonceMap = make(map[common.Address]uint64)
	)
	for {
		txCount := len(txs)
		var nextRound Transactions

		// sweep txs
		for _, tx := range txs {
			if tx.from == nil {
				// TODO: ignore for now
				log.Warn("tx ignored due to nil from")
				continue
			}
			from := *tx.from
			nonce, ok := nonceMap[from]
			if !ok {
				account := state.GetAccount(from, false)
				if account != nil {
					nonce = account.Nonce()
				} else {
					nonce = 0
				}
				nonceMap[from] = nonce
			}
			switch {
			case tx.nonce == nonce:
				output = append(output, tx)
				nonceMap[from]++
			case tx.nonce > nonce:
				nextRound = append(nextRound, tx)
			default:
				// TODO: ignore for now
				log.Warn("tx nonce too low", "nonce", nonce, "tx", tx)
			}
		}

		// check if we need another round
		txs = nextRound
		if len(txs) == 0 {
			break
		}
		if txCount == len(txs) {
			sort.Slice(txs, func(i, j int) bool {
				a := *txs[i].from
				b := *txs[j].from
				for k := 0; k < common.AddressLength; k++ {
					switch {
					case a[k] < b[k]:
						return true
					case a[k] > b[k]:
						return false
					}
				}
				return txs[i].Nonce() < txs[j].Nonce()
			})
			var sb = new(strings.Builder)
			for _, tx := range txs {
				f := *tx.from
				_, _ = fmt.Fprintf(sb, "\n%5d / %5d @%v %v", tx.Nonce(), nonceMap[f], f, tx.Hash().Hex())
			}
			log.Warn(fmt.Sprintf("output nonce not possible, remain[%d/%d]",
				len(txs), len(txs)+len(output)))
			log.Info("nonce not possible", sb.String())
			break
		}
	}
	return output, txs
}
