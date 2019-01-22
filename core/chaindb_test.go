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
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestDBKeys(t *testing.T) {
	if string(keyChainID()) != KeyChainID {
		t.Errorf("wrong keyChainID()")
	}

	num := rand.Uint64()
	keyNum2Hash := keyBlockNum2Hash(num)
	if binary.BigEndian.Uint64(keyNum2Hash[len(keyNum2Hash)-8:]) != num {
		t.Errorf("wrong keyBlockNum2Hash()")
	}
}
