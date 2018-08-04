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

package secp256k1

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"

	"crypto/elliptic"

	"github.com/yeeco/gyee/crypto/util"
	"github.com/yeeco/gyee/utils/logging"
)

type Key struct {
	priKey []byte
	pubKey []byte
}

func NewKey(privateKey, publicKey []byte) *Key {
	return &Key{
		priKey: privateKey,
		pubKey: publicKey,
	}
}

//用ecdsa包，通过k1曲线产生私钥
func GenerateKey() *Key {
	key := NewKey(nil, nil)
	privateKeyECDSA, err := ecdsa.GenerateKey(S256(), rand.Reader)
	if err != nil {
		fmt.Print(err)
	}
    key.priKey = privateKeyFromECDSA(privateKeyECDSA)
    key.pubKey = publicKeyFromECDSA(privateKeyECDSA)
    return key
}

//直接用k1包来产生私钥, 与上应该一样的功能
func GenerateKeyK1() *Key {
	key := NewKey(nil, nil)
	key.priKey = NewPrivateKey()
	pk, err := GetPublicKey(key.priKey)
	if err != nil {
		logging.Logger.Panic("Error get public key:", err)
	}
	key.pubKey = pk
	return key
}

func (k *Key) PrivateKey() []byte {
	return k.priKey
}

func (k *Key) PublicKey() []byte {
	return k.pubKey
}

func (k *Key) Clear() {

}

func privateKeyFromECDSA(ecdsaKey *ecdsa.PrivateKey) []byte {
	return util.PaddedBigBytes(ecdsaKey.D, PrivateKeyLength)
}

func publicKeyFromECDSA(ecdsaKey *ecdsa.PrivateKey) []byte {
	return elliptic.Marshal(S256(), ecdsaKey.PublicKey.X, ecdsaKey.PublicKey.Y)
}
