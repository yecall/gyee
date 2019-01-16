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
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
)

const (
	txTestAddress = "0011223344556677889900112233445566778899"
)

func TestNewTransaction(t *testing.T) {
	address := common.HexToAddress(txTestAddress)
	tx1 := NewTransaction(255, 128, &address, big.NewInt(10000))
	if tx1.ChainId() != 255 {
		t.Errorf("wrong chainId")
	}
	if tx1.Nonce() != 128 {
		t.Errorf("wrong nonce")
	}
	if tx1.Recipient().Hex() != txTestAddress {
		t.Errorf("wrong recipient")
	}
	if tx1.Amount().Cmp(big.NewInt(10000)) != 0 {
		t.Errorf("wrong amount")
	}
}

const txHex = "08ff011080011a14001122334455667788990011223344556677889922022710"

func TestTxDecode(t *testing.T) {
	pbTx := &corepb.Transaction{}
	if err := proto.Unmarshal(common.Hex2Bytes(txHex), pbTx); err != nil {
		t.Errorf("tx proto unmarshal failed %v", err)
	}
	tx, err := NewTransactionFromProto(pbTx)
	if err != nil {
		t.Errorf("tx FromProto failed %v", err)
	}
	match := (tx.chainId == 255) &&
		(tx.nonce == 128) &&
		(tx.to.Hex() == txTestAddress) &&
		(tx.amount.Cmp(big.NewInt(10000)) == 0)
	if !match {
		t.Errorf("decoded tx mismatch, got %v", tx)
	}
}

func TestTxEncode(t *testing.T) {
	address := common.HexToAddress(txTestAddress)
	tx := NewTransaction(255, 128, &address, big.NewInt(10000))
	pbTx, err := tx.ToProto()
	if err != nil {
		t.Errorf("tx ToProto failed %v", err)
	}
	enc, err := proto.Marshal(pbTx)
	if err != nil {
		t.Errorf("tx proto marshal failed %v", err)
	}
	if hexStr := hex.EncodeToString(enc); hexStr != txHex {
		t.Errorf("tx encoded hex mismatch, got %v", hexStr)
	}
}
