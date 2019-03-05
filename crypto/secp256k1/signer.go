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

import (
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/utils/logging"
	"errors"
)

type Signer struct {
	algrithm   crypto.Algorithm
	privateKey []byte
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
        logging.Logger.Warn("privateKey has not setted!")
        return nil, errors.New("privateKey has not setted")
	}

	sig, err := Sign(data, s.privateKey)
	if err != nil {
		logging.Logger.Warn("signing error.", err)
		return nil, err
	}

	signature = &crypto.Signature{
		Algorithm: s.Algorithm(),
		Signature: sig,
	}
	return signature, nil
}

func (s *Signer) RecoverPublicKey(data []byte, signature *crypto.Signature) (publicKey []byte, err error) {
	pk, err := RecoverPubkey(data, signature.Signature)
	if err != nil {
		logging.Logger.Warn("recover public key error.", err)
        return nil, err
	}
	return pk, nil
}

func (s *Signer) Verify(publicKey []byte, data []byte, signature *crypto.Signature) bool {
	ret := VerifySignature(publicKey, data, signature.Signature[:64])
	return ret
}
