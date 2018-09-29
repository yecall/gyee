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
	"math/big"

	"github.com/yeeco/gyee/common"
)

type account struct {
	address common.Hash
	balance *big.Int
	nonce   uint64
	//TODO: contract部分的数据
}

func (acc *account) ToBytes() ([]byte, error) {
	return nil, nil
}

func (acc *account) FromBytes(bytes []byte) error {
	return nil
}

func (acc *account) Balance() *big.Int {
	return acc.balance
}

func (acc *account) Address() common.Hash {
	return acc.address
}

func (acc *account) Nonce() uint64 {
	return acc.nonce
}

func (acc *account) NonceInc() {
	acc.nonce++
}
