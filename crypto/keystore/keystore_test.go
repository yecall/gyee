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

package keystore

import (
	"testing"
	"fmt"
)

func TestNewKeystore(t *testing.T) {
	ks := NewKeystore(".")

	fmt.Println("List:")
	list := ks.List()
	for _, item := range list {
		fmt.Println(item)
	}

	fmt.Println()
	fmt.Println("SetKey:")
	err := ks.SetKey("addr00001", []byte("private key1"), []byte("password1"))
	err = ks.SetKey("addr00002", []byte("private key2"), []byte("password2"))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println()
	fmt.Println("GetKey:")
	content, err := ks.GetKey("addr00001", []byte("password1"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(content))

	fmt.Println()
	fmt.Println("GetKey wrong passwrod")
	content, err = ks.GetKey("addr00002", []byte("password1"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(content))

	fmt.Println()
	fmt.Println("Contains:")
	ok, err := ks.Contains("addr00001")
	if ok {
		fmt.Println("addr00001 true")
	}

	ok, err = ks.Contains("addr00003")
	if ok {
		fmt.Println("addr00003 true")
	}

}
