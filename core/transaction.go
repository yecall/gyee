/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package core

import (
	"math/big"

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
)

type Transaction struct {
	chainId uint32
	nonce   uint64
	to      *common.Address
	amount  *big.Int

	// caches
	from *common.Address
	hash *common.Hash
}

//最小transaction字节数？

func NewTransaction(chainId uint32, nonce uint64, recipient *common.Address, amount *big.Int) *Transaction {
	tx := &Transaction{
		chainId: chainId,
		nonce:   nonce,
		to:      recipient,
		amount:  new(big.Int),
	}
	if amount != nil {
		tx.amount.Set(amount)
	}
	return tx
}

func NewTransactionFromProto(msg proto.Message) (*Transaction, error) {
	tx := &Transaction{}
	err := tx.FromProto(msg)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (t *Transaction) ChainId() uint32 {
	return t.chainId
}

func (t *Transaction) Nonce() uint64 {
	return t.nonce
}

func (t *Transaction) Recipient() *common.Address {
	return t.to
}

func (t *Transaction) Amount() *big.Int {
	return t.amount
}

func (t *Transaction) ToProto() (*corepb.Transaction, error) {
	pbTx := &corepb.Transaction{
		ChainId: t.chainId,
		Nonce:   t.nonce,
	}
	if t.to != nil {
		pbTx.Recipient = common.CopyBytes(t.to[:])
	}
	if t.amount != nil {
		pbTx.Amount = t.amount.Bytes()
	}
	return pbTx, nil
}

func (t *Transaction) FromProto(msg proto.Message) error {
	pbt, ok := msg.(*corepb.Transaction)
	if !ok {
		return ErrInvalidProtoToTransaction
	}
	if pbt == nil {
		return ErrInvalidProtoToTransaction
	}
	// copy value
	t.chainId = pbt.ChainId
	t.nonce = pbt.Nonce
	if pbt.Recipient != nil {
		t.to = new(common.Address)
		t.to.SetBytes(pbt.Recipient)
	}
	t.amount = new(big.Int)
	if pbt.Amount != nil {
		t.amount.SetBytes(pbt.Amount)
	}

	return nil
}
