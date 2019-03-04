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

package consensus

import (
	"github.com/yeeco/gyee/common"
)

// output of consensus to generate a block at height H, with txs Txs
type Output struct {
	Txs    []common.Hash
	H      uint64
	Output string
}

// Engine defines what a consensus engine provides
type Engine interface {
	// lifecycle controls
	Start() error
	Stop() error

	// engine event output channel
	ChanEventSend() <-chan []byte
	ChanEventReq() <-chan common.Hash

	// engine output channel
	Output() <-chan *Output

	// send event raw bytes to engine
	SendEvent([]byte)
	SendParentEvent([]byte)

	// send tx hash to engine
	SendTx(common.Hash)

	// inform engine txs has been sealed in block
	OnTxSealed(uint64, []common.Hash)
}
