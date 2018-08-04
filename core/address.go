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
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/pkg/errors"
	"github.com/yeeco/gyee/crypto/hash"
)

type AddressType byte

const (
	AddressTypeAccount AddressType = 0x01 + iota
	AddressTypeContract
)

const (
    AddressTypeIndex = 0
    AddressTypeLength = 1
    AddressNetworkIdIndex = 1
    AddressNetworkIdLength = 1
    AddressContentIndex = 2
    AddressContentLength = 20
    AddressChecksumIndex = 22
    AddressChecksumLength = 4
    AddressLength = AddressTypeLength + AddressNetworkIdLength + AddressContentLength + AddressChecksumLength
    PublicKeyLength = secp256k1.PublicKeyLength
    //TODO: 这个要从更通用的一个地方来定义
)

type Address struct {
	address crypto.Hash
}

func NewAddressFromPublicKey(pubkey []byte) (*Address, error) {
	if len(pubkey) != PublicKeyLength {
		return nil, errors.New("error public key length")
	}
	return newAddressFromPublicKey(AddressTypeAccount, pubkey)
	return nil,nil
}

func NewContractAddressFromData() (*Address, error) {
	return nil, nil
}

func (a *Address) String() string {
	return a.address.Hex()
}

func AddressParse(s string) (*Address, error) {
	return nil, nil
}

func AddressParseForBytes(b []byte) (*Address, error) {
	return nil, nil
}

func newAddressFromPublicKey(t AddressType, pubkey []byte) (*Address, error) {
	buffer := make([]byte, AddressLength)
	buffer[AddressTypeIndex] = byte(t)
	buffer[AddressNetworkIdIndex] = 0x05  //TODO：这个要从其他地方取
	sha := hash.Sha3256(pubkey)
	content := hash.Ripemd160(sha)
	copy(buffer[AddressContentIndex:AddressChecksumIndex], content)
	cs := checkSum(buffer[:AddressChecksumIndex])
	copy(buffer[AddressChecksumIndex:], cs)
	return &Address{
		address: buffer,
	}, nil
}

func checkSum(data []byte) []byte {
	return hash.Sha3256(data)[:AddressChecksumLength]
}

//TODO: network id的处理