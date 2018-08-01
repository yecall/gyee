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

package cipher

import (
	"testing"
	"reflect"
	"encoding/hex"
	"fmt"
)

func Test_Cipher_Scrypt(t *testing.T) {
	passphrase := []byte("passphrase")
	data, _ := hex.DecodeString("0eb3be2db3a534c192be5570c6c42f590eb3be2db3a534c192be5570c6c42f59")
	var cipher Cipher
	cipher = NewScrypt()
	got, err := cipher.EncryptKey("address00000",data, passphrase)
	if err != nil {
		t.Errorf("Encrypt() error, %v", err)
		return
	}
	fmt.Println(string(got))

	want, err := cipher.DecryptKey(got, passphrase)
	if err != nil {
		t.Errorf("Decrypt() error, %v", err)
		return
	}

	if !reflect.DeepEqual(data, want) {
		t.Errorf("Decrypt() = %v, data %v", want, data)
	}
}

func Test_Cipher_Argon2(t *testing.T) {
	passphrase := []byte("passphrase")
	data, _ := hex.DecodeString("0eb3be2db3a534c192be5570c6c42f590eb3be2db3a534c192be5570c6c42f59")
	var cipher Cipher
	cipher = NewArgon2()
	got, err := cipher.EncryptKey("address00000",data, passphrase)
	if err != nil {
		t.Errorf("Encrypt() error, %v", err)
		return
	}
	fmt.Println(string(got))

	want, err := cipher.DecryptKey(got, passphrase)
	if err != nil {
		t.Errorf("Decrypt() error, %v", err)
		return
	}

	if !reflect.DeepEqual(data, want) {
		t.Errorf("Decrypt() = %v, data %v", want, data)
	}
}

func Test_Cipher_Balloon(t *testing.T) {
	passphrase := []byte("passphrase")
	data, _ := hex.DecodeString("0eb3be2db3a534c192be5570c6c42f590eb3be2db3a534c192be5570c6c42f59")
	var cipher Cipher
	cipher = NewBalloon()
	got, err := cipher.EncryptKey("address00000",data, passphrase)
	if err != nil {
		t.Errorf("Encrypt() error, %v", err)
		return
	}
	fmt.Println(string(got))

	want, err := cipher.DecryptKey(got, passphrase)
	if err != nil {
		t.Errorf("Decrypt() error, %v", err)
		return
	}

	if !reflect.DeepEqual(data, want) {
		t.Errorf("Decrypt() = %v, data %v", want, data)
	}
}
