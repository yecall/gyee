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

package state

import (
	"errors"
	"math/big"

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/core/pb"
)

type accountObj struct {
	trie    *accountTrie
	dirty   bool
	deleted bool

	address common.Address

	nonce   uint64
	balance *big.Int

	//TODO: contract部分的数据
}

func newAccount(trie *accountTrie, address common.Address) *accountObj {
	return &accountObj{
		trie:    trie,
		address: address,
		balance: new(big.Int),
	}
}

func (acc *accountObj) Address() *common.Address {
	return &acc.address
}

func (acc *accountObj) Nonce() uint64 {
	return acc.nonce
}

func (acc *accountObj) Balance() *big.Int {
	return acc.balance
}

func (acc *accountObj) SetBalance(value *big.Int) {
	if acc.balance.Cmp(value) == 0 {
		return
	}
	acc.balance.Set(value)
	acc.dirty = true
}

func (acc *accountObj) ToBytes() ([]byte, error) {
	pbAcc := &corepb.Account{
		Nonce:   acc.nonce,
		Balance: acc.balance.Bytes(),
	}
	bytes, err := proto.Marshal(pbAcc)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (acc *accountObj) setBytes(bytes []byte) error {
	pbAcc := &corepb.Account{}
	if err := proto.Unmarshal(bytes, pbAcc); err != nil {
		return err
	}
	value := new(big.Int)
	value.SetBytes(bytes)
	if value.BitLen() > 256 {
		return errors.New("balance out of range")
	}
	acc.nonce = pbAcc.Nonce
	acc.balance.Set(value)
	return nil
}

func (acc *accountObj) NonceInc() {
	acc.nonce++
}
