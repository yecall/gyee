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
import (
	"unsafe"
	"errors"
)

func init() {

}

func GenerateKeyPair() (pk, sk []byte) {
	pk = make([]byte, C.CRYPTO_PUBLICKEYBYTES)
	sk = make([]byte, C.CRYPTO_SECRETKEYBYTES)
    C.crypto_sign_keypair(cBuf(pk), cBuf(sk))
    return pk, sk
}

func Sign(msg []byte, seckey []byte) ([]byte, error) {
	sm := make([]byte, len(msg)+C.CRYPTO_BYTES)
	var smlen C.ulonglong
	mlen := C.ulonglong(len(msg))

    ret := C.crypto_sign(cBuf(sm), &smlen, cBuf(msg), mlen, cBuf(seckey))

    if ret != 0 {
    	return nil, errors.New("sign error")
	}
    return sm, nil
}

func Verify(sm []byte, pubkey []byte) ([]byte, bool) {
	m := make([]byte, len(sm)+C.CRYPTO_BYTES)
	var mlen C.ulonglong
    smlen := C.ulonglong(len(sm))
	ret := C.crypto_sign_open(cBuf(m), &mlen, cBuf(sm), smlen, cBuf(pubkey))
	if ret == 0 {
		return m, true
	}
	return nil, false
}

func cBuf(goSlice []byte) *C.uchar {
	return (*C.uchar)(unsafe.Pointer(&goSlice[0]))
}

func goBytes(cSlice []C.uchar, size C.int) []byte {
	return C.GoBytes(unsafe.Pointer(&cSlice[0]), size)
}