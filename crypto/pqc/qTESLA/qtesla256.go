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

/*
#cgo CFLAGS: -I./qTesla_256
#cgo CFLAGS: -I./qTesla_256/sha3
#cgo CFLAGS: -O3 -march=native -fomit-frame-pointer
#cgo LDFLAGS: -lm -L/usr/lib/ -lssl -lcrypto
#define USE_NUM_NONE
#define USE_FIELD_10X26
#define USE_FIELD_INV_BUILTIN
#define USE_SCALAR_8X32
#define USE_SCALAR_INV_BUILTIN
#define NDEBUG
#include "./qTesla_256/sign.c"
#include "./qTesla_256/sample.c"
#include "./qTesla_256/poly.c"
#include "./qTesla_256/consts.c"
#include "./qTesla_256/oracle.c"
#include "./qTesla_256/rng.c"
#include "./qTesla_256/cpucycles.c"
#include "./qTesla_256/sha3/fips202.c"
*/
import "C"
import "unsafe"

func init() {

}

func GenerateKeyPair() (pk, sk []byte) {
	pk = make([]byte, C.CRYPTO_PUBLICKEYBYTES)
	sk = make([]byte, C.CRYPTO_SECRETKEYBYTES)
    C.crypto_sign_keypair((*C.uchar)(unsafe.Pointer(&pk[0])), (*C.uchar)(unsafe.Pointer(&sk[0])))
    return pk, sk
}

func Signature() {

}

func Verify() {

}