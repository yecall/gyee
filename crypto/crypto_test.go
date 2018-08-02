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

package crypto

/*
test:
1. key generate via ecdsa vs secp256k1
2. sign&verify via ecdsa vs secp256k1
   这儿的注意点：
       ecdsa的签名，是有一个随机数k的，所以同样消息和私钥，每次签名的结果都是不一样的。如果随机数k重复使用，会泄露私钥
       secp256k1曲线在这儿的签名实现中，采用rfc6979，同样消息和私钥签名的结果是一样的。
3.
*/

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/crypto/util"
)

func TestSig(t *testing.T) {

}

func TestVerify(t *testing.T) {

}

func TestNewKey(t *testing.T) {
	privateKeyECDSA, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		fmt.Print(err)
	}

	pkey := util.PaddedBigBytes(privateKeyECDSA.D, 32)
	fmt.Println("private key:%v", pkey)

	pubkeyx := util.PaddedBigBytes(privateKeyECDSA.PublicKey.X, 32)
	fmt.Println("x:%v", pubkeyx)

	pubkeyy := util.PaddedBigBytes(privateKeyECDSA.PublicKey.Y, 32)
	fmt.Println("y:%v", pubkeyy)

	m := elliptic.Marshal(secp256k1.S256(), privateKeyECDSA.PublicKey.X, privateKeyECDSA.PublicKey.Y)
	fmt.Println("m:%v", m)

	pubkey, err := secp256k1.GetPublicKey(pkey)
	fmt.Println("public key:%v", pubkey)
}
