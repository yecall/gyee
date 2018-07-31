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

import (
	"crypto/ecdsa"
	"crypto/rand"
	"testing"

	"github.com/yeeco/gyee/crypto/secp256k1"

	"fmt"
	"math/big"
	"crypto/elliptic"
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

	pkey := paddedBigBytes(privateKeyECDSA.D, 32)
	fmt.Println("private key:%v", pkey)

	pubkeyx := paddedBigBytes(privateKeyECDSA.PublicKey.X, 32)
	fmt.Println("x:%v", pubkeyx)

	pubkeyy := paddedBigBytes(privateKeyECDSA.PublicKey.Y, 32)
	fmt.Println("y:%v", pubkeyy)

	m := elliptic.Marshal(secp256k1.S256(), privateKeyECDSA.PublicKey.X, privateKeyECDSA.PublicKey.Y)
	fmt.Println("m:%v", m)

	pubkey, err := secp256k1.GetPublicKey(pkey)
	fmt.Println( "public key:%v", pubkey)
}


// paddedBigBytes encodes a big integer as a big-endian byte slice.
func paddedBigBytes(bigint *big.Int, n int) []byte {
	if bigint.BitLen()/8 >= n {
		return bigint.Bytes()
	}
	ret := make([]byte, n)
	readBits(bigint, ret)
	return ret
}

const (
	// number of bits in a big.Word
	wordBits = 32 << (uint64(^big.Word(0)) >> 63)
	// number of bytes in a big.Word
	wordBytes = wordBits / 8
)

// readBits encodes the absolute value of bigint as big-endian bytes.
func readBits(bigint *big.Int, buf []byte) {
	i := len(buf)
	for _, d := range bigint.Bits() {
		for j := 0; j < wordBytes && i > 0; j++ {
			i--
			buf[i] = byte(d)
			d >>= 8
		}
	}
}