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

package qTESLA

import (
	"testing"
	"fmt"
)

func TestGenerateKeyPair(t *testing.T) {
	pk, sk := GenerateKeyPair()
	fmt.Printf("pk:%v\n", pk)
	fmt.Printf("sk:%v\n", sk)
}

func TestSign(t *testing.T) {
	pk, sk := GenerateKeyPair()
	msg := []byte("sign msg test")

	sm, err := Sign(msg, sk)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("sm:%v\n\n", sm)

	sm, err = Sign(msg, sk)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("sm:%v\n", sm)

	m, ret := Verify(sm, pk)
	if ret {
		fmt.Println("verify success", string(m))
	} else {
		fmt.Println("verify failed")
	}
}