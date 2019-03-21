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

package core

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/core/pb"
	"github.com/yeeco/gyee/crypto"
	sha3 "github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/persistent"
)

var (
	ErrNoSignature       = errors.New("no signature with tx")
	ErrNoSigner          = errors.New("no signer found")
	ErrSignatureMismatch = errors.New("signature mismatch")
	ErrTxFromMismatch    = errors.New("tx sender mismatch")
)

type Transaction struct {
	chainID   uint32
	nonce     uint64
	to        *common.Address
	amount    *big.Int
	signature *crypto.Signature

	// caches
	from *common.Address
	hash *common.Hash
	raw  []byte
}

//最小transaction字节数？

func NewTransaction(chainID uint32, nonce uint64, recipient *common.Address, amount *big.Int) *Transaction {
	tx := &Transaction{
		chainID: chainID,
		nonce:   nonce,
		to:      recipient,
		amount:  new(big.Int),
	}
	if amount != nil {
		tx.amount.Set(amount)
	}
	return tx
}

func NewTransactionFromProto(msg proto.Message) (*Transaction, error) {
	tx := &Transaction{}
	err := tx.FromProto(msg)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (t *Transaction) ChainID() uint32 {
	return t.chainID
}

func (t *Transaction) Nonce() uint64 {
	return t.nonce
}

func (t *Transaction) Recipient() *common.Address {
	return t.to
}

func (t *Transaction) Amount() *big.Int {
	return t.amount
}

func (t *Transaction) Sign(signer crypto.Signer) error {
	sig, err := signer.Sign(t.Hash()[:])
	if err != nil {
		return err
	}
	t.signature = sig
	return nil
}

func (t *Transaction) Hash() *common.Hash {
	if t.hash == nil {
		enc, err := t.encode(true)
		if err != nil {
			log.Crit("wrong tx hash")
		}
		t.hash = new(common.Hash)
		t.hash.SetBytes(sha3.Sha3256(enc))
	}
	return t.hash
}

func (t *Transaction) ToProto() (*corepb.Transaction, error) {
	pbTx := &corepb.Transaction{
		ChainID: t.chainID,
		Nonce:   t.nonce,
	}
	if t.to != nil {
		pbTx.Recipient = common.CopyBytes(t.to[:])
	}
	if t.amount != nil {
		pbTx.Amount = t.amount.Bytes()
	}
	if t.signature != nil {
		pbTx.Signature = &corepb.Signature{
			SigAlgorithm: uint32(t.signature.Algorithm),
			Signature:    t.signature.Signature,
		}
	}
	return pbTx, nil
}

func (t *Transaction) FromProto(msg proto.Message) error {
	pbt, ok := msg.(*corepb.Transaction)
	if !ok {
		return ErrInvalidProtoToTransaction
	}
	if pbt == nil {
		return ErrInvalidProtoToTransaction
	}
	// copy value
	t.chainID = pbt.ChainID
	t.nonce = pbt.Nonce
	if pbt.Recipient != nil {
		t.to = new(common.Address)
		t.to.SetBytes(pbt.Recipient)
	}
	t.amount = new(big.Int)
	if pbt.Amount != nil {
		t.amount.SetBytes(pbt.Amount)
	}
	if pbt.Signature != nil {
		t.signature = &crypto.Signature{
			Algorithm: crypto.Algorithm(pbt.Signature.SigAlgorithm),
			Signature: pbt.Signature.Signature,
		}
	}

	return nil
}

func (t *Transaction) Encode() ([]byte, error) {
	return t.encode(false)
}

func (t *Transaction) encode(withoutSig bool) ([]byte, error) {
	pb, err := t.ToProto()
	if err != nil {
		return nil, err
	}
	if withoutSig {
		pb.Signature = nil
	}
	return proto.Marshal(pb)
}

func (t *Transaction) VerifySig() error {
	if t.signature == nil {
		return ErrNoSignature
	}
	signer := getSigner(t.signature.Algorithm)
	if signer == nil {
		return ErrNoSigner
	}
	txHash := t.Hash()[:]
	pubkey, err := signer.RecoverPublicKey(txHash, t.signature)
	if err != nil {
		return err
	}
	if !signer.Verify(pubkey, txHash, t.signature) {
		return ErrSignatureMismatch
	}
	addr, err := address.NewAddressFromPublicKey(pubkey)
	if err != nil {
		return err
	}
	if t.from == nil {
		t.from = addr.CommonAddress()
	} else {
		if *t.from != *addr.CommonAddress() {
			return ErrTxFromMismatch
		}
	}
	return nil
}

func (t *Transaction) Decode(enc []byte) error {
	pb := new(corepb.Transaction)
	if err := proto.Unmarshal(enc, pb); err != nil {
		return err
	}
	return t.FromProto(pb)
}

type Transactions []*Transaction

func (txs Transactions) Len() int { return len(txs) }

func (txs Transactions) GetEncoded(index int) []byte { return txs[index].raw }

func (txs Transactions) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprint(&sb, "[")
	for _, tx := range txs {
		_, _ = fmt.Fprint(&sb, tx.Hash(), " ")
	}
	_, _ = fmt.Fprint(&sb, "]")
	return sb.String()
}

func (txs Transactions) encode() error {
	for i := range txs {
		if txs[i].raw != nil {
			continue
		}
		enc, err := txs[i].Encode()
		if err != nil {
			return err
		}
		txs[i].raw = enc
	}
	return nil
}

func (txs Transactions) Write(putter persistent.Putter) error {
	for _, tx := range txs {
		pb, err := tx.ToProto()
		if err != nil {
			return err
		}
		putTransaction(putter, *tx.Hash(), pb)
	}
	return nil
}

func (txs Transactions) addToStorage(storage persistent.Storage) error {
	batch := storage.NewBatch()
	if err := txs.Write(batch); err != nil {
		return err
	}
	return batch.Write()
}
