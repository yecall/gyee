// Copyright (C) 2017 gyee authors
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

package address

/*
The account address calculation formula is as follows:
```
1.  content = ripemd160(sha3_256(public key))
    length: 20 bytes
                            +----------------+-------------+
2.  checksum = sha3_256(    |   network id   +   content   |   )[:4]
                            +----------------+-------------+
    length: 4 bytes

                        +----------------+-------------+------------+
3.  address = hex(      |   network id   |   content   |  checksum  |     ）
                        +----------------+-------------+------------+
    length: 50 chars
```
*/

import (
	"bytes"
	"encoding/hex"

	"github.com/pkg/errors"
	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/secp256k1"
)

type AddressType byte

const (
	AddressTypeAccount AddressType = 0x01 + iota
	AddressTypeContract
)

const (
	AddressTypeIndex       = 0
	AddressTypeLength      = 1
	AddressNetworkIdIndex  = 1
	AddressNetworkIdLength = 1
	AddressContentIndex    = 2
	AddressContentLength   = 20
	AddressChecksumIndex   = 22
	AddressChecksumLength  = 4
	AddressLength          = AddressTypeLength + AddressNetworkIdLength + AddressContentLength + AddressChecksumLength
	PublicKeyLength        = secp256k1.PublicKeyLength
	//TODO: 这个要从更通用的一个地方来定义

	AddressStringLength = 52
)

var (
	ErrInvalidAddress         = errors.New("address: invalid address")
	ErrInvalidAddressFormat   = errors.New("address: invalid address format")
	ErrInvalidAddressType     = errors.New("address: invalid address type")
	ErrInvalidAddressChecksum = errors.New("address: invalid address checksum")
)

type Address struct {
	Raw []byte
}

func NewAddressFromPublicKey(pubkey []byte) (*Address, error) {
	if len(pubkey) != PublicKeyLength {
		return nil, errors.New("error public key length")
	}
	return newAddressFromPublicKey(AddressTypeAccount, pubkey)
}

func NewAddressFromCommonAddress(addr common.Address) *Address {
	buffer := make([]byte, AddressLength)
	buffer[AddressTypeIndex] = byte(AddressTypeAccount)
	buffer[AddressNetworkIdIndex] = 0x05 //TODO：这个要从其他地方取
	copy(buffer[AddressContentIndex:AddressChecksumIndex], addr[:])
	cs := checkSum(buffer[:AddressChecksumIndex])
	copy(buffer[AddressChecksumIndex:], cs)
	return &Address{
		Raw: buffer,
	}
}

func NewContractAddressFromData() (*Address, error) {
	return nil, nil
}

// Bytes returns address bytes
func (a *Address) Bytes() []byte {
	return a.Raw
}

// String returns address string
func (a *Address) String() string {
	return hex.EncodeToString(a.Raw)
}

func (a *Address) CommonAddress() *common.Address {
	ret := new(common.Address)
	ret.SetBytes(a.Raw[AddressContentIndex:AddressChecksumIndex])
	return ret
}

func (a Address) Copy() *Address {
	addr := &Address{
		Raw: make([]byte, AddressLength),
	}
	copy(addr.Raw, a.Raw)
	return addr
}

// AddressParse parse address string.
func AddressParse(s string) (*Address, error) {
	if len(s) != AddressStringLength {
		return nil, ErrInvalidAddressFormat
	}
	ab, err := hex.DecodeString(s)
	if err != nil {
		return nil, ErrInvalidAddressFormat
	}
	return AddressParseFromBytes(ab)
}

// AddressParseFromBytes parse address from bytes.
func AddressParseFromBytes(b []byte) (*Address, error) {
	if len(b) != AddressLength || b[AddressNetworkIdIndex] != 0x05 { //TODO: network id
		return nil, ErrInvalidAddressFormat
	}

	switch AddressType(b[AddressTypeIndex]) {
	case AddressTypeAccount, AddressTypeContract:
	default:
		return nil, ErrInvalidAddressType
	}

	if !bytes.Equal(checkSum(b[:AddressChecksumIndex]), b[AddressChecksumIndex:]) {
		return nil, ErrInvalidAddressChecksum
	}

	return &Address{Raw: b}, nil
}

func newAddressFromPublicKey(t AddressType, pubkey []byte) (*Address, error) {
	buffer := make([]byte, AddressLength)
	buffer[AddressTypeIndex] = byte(t)
	buffer[AddressNetworkIdIndex] = 0x05 //TODO：这个要从其他地方取
	sha := hash.Sha3256(pubkey)
	content := hash.Ripemd160(sha)
	copy(buffer[AddressContentIndex:AddressChecksumIndex], content)
	cs := checkSum(buffer[:AddressChecksumIndex])
	copy(buffer[AddressChecksumIndex:], cs)
	return &Address{
		Raw: buffer,
	}, nil
}

func checkSum(data []byte) []byte {
	return hash.Sha3256(data)[:AddressChecksumLength]
}

//TODO: network id的处理
