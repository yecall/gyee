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

import "errors"

type Cipher interface {
	Encrypt(data []byte, passphrase []byte) ([]byte, error)

	EncryptKey(address string, data []byte, passphrase []byte) ([]byte, error)

	Decrypt(data []byte, passphrase []byte) ([]byte, error)

	DecryptKey(keyjson []byte, passphrase []byte) ([]byte, error)
}

type cipherparamsJSON struct {
	IV string `json:"iv"`
}

type cryptoJSON struct {
	Cipher       string                 `json:"cipher"`
	CipherText   string                 `json:"ciphertext"`
	CipherParams cipherparamsJSON       `json:"cipherparams"`
	KDF          string                 `json:"kdf"`
	KDFParams    map[string]interface{} `json:"kdfparams"`
	MAC          string                 `json:"mac"`
	MACHash      string                 `json:"machash"`
}

type encryptedKeyJSON struct {
	Address string     `json:"address"`
	Crypto  cryptoJSON `json:"crypto"`
	ID      string     `json:"id"`
	Version int        `json:"version"`
}

const (
	//// cipher the name of cipher
	//cipherName = "aes-128-ctr"

	// mac calculate hash type
	macHash = "sha3256"
)

var (
	// ErrVersionInvalid version not supported
	ErrVersionInvalid = errors.New("version not supported")

	// ErrKDFInvalid cipher not supported
	ErrKDFInvalid = errors.New("kdf not supported")

	// ErrCipherInvalid cipher not supported
	ErrCipherInvalid = errors.New("cipher not supported")

	// ErrDecrypt decrypt failed
	ErrDecrypt = errors.New("could not decrypt key with given passphrase")
)

// because json.Unmarshal change int to float64, convert to int
func ensureInt(x interface{}) int {
	res, ok := x.(int)
	if !ok {
		res = int(x.(float64))
	}
	return res
}
