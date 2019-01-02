// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package secp256k1

import "github.com/yeeco/gyee/crypto"

type Signer struct {
	algrithm   crypto.Algorithm
	privateKey []byte
	publicKey  []byte
}

func NewSecp256k1Signer() *Signer {
	signer := &Signer{
		algrithm: crypto.ALG_SECP256K1,
	}
	return signer
}

func (s *Signer) Algorithm() crypto.Algorithm {
	return s.algrithm
}

func (s *Signer) InitSigner(privateKey []byte) error {
	s.privateKey = privateKey
	return nil
}

func (s *Signer) Sign(data []byte) (signature *crypto.Signature, err error) {
	if s.privateKey == nil {

	}
	sig, err := Sign(data, s.privateKey)
	signature = &crypto.Signature{
		Algorithm: s.Algorithm(),
		Signature: sig,
	}
	return
}

func (s *Signer) RecoverPublicKey(data []byte, signature *crypto.Signature) (publicKey []byte, err error) {
	pk, err := RecoverPubkey(data, signature.Signature)
	if err != nil {
        return nil, err
	}
	s.publicKey = pk
	return pk, nil
}

func (s *Signer) Verify(data []byte, signature *crypto.Signature) (bool, error) {
	_, err := s.RecoverPublicKey(data, signature)
	if err != nil {
		return false, err
	}

	ret := VerifySignature(s.publicKey, data, signature.Signature)
	return ret, nil
}
