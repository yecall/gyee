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


//TODO: config crypto params: hash, keystore.cipher, keystore.kdf, curve,
//TODO: 支持将来这个模块可以升级配置，算法内容的自描述

type Algorithm uint8

const (
	ALG_SECP256K1 Algorithm = 1
	ALG_QTESLA    Algorithm = 128
)

type Signature struct {
	Algorithm  Algorithm
	Signature  []byte
}

type Signer interface {
	Algorithm() Algorithm

	InitSigner(privateKey []byte) error

	Sign(data []byte) (signature *Signature, err error)

	RecoverPublicKey(data []byte, signature *Signature) (publicKey []byte, err error)

	Verify(publicKey []byte, data []byte, signature *Signature) bool
}