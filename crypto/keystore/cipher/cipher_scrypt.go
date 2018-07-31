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

package cipher

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"


	"bytes"
	"encoding/json"

	uuid "github.com/satori/go.uuid"
	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/random"
	"golang.org/x/crypto/scrypt"
)

const (
	// ScryptKDF name
	ScryptKDF = "scrypt"

	//N: General work factor, iteration count.
	//r: blocksize in use for underlying hash; fine-tunes the relative memory-cost.
	//p: parallelization factor; fine-tunes the relative cpu-cost.

	// N:18, P:1 -> Using 256MB memory and taking approximately 1s CPU time on a modern processor.
	// N:12, P:6 -> Using 4MB memory and taking approximately 100ms CPU time on a modern processor.

	// StandardScryptN N parameter of Scrypt encryption algorithm
	StandardScryptN = 1 << 17

	// StandardScryptR r parameter of Scrypt encryption algorithm
	StandardScryptR = 8

	// StandardScryptP p parameter of Scrypt encryption algorithm
	StandardScryptP = 1

	// ScryptDKLen get derived key length
	ScryptDKLen = 32

	// version compatible with ethereum, the version start with 3
	version3       = 3
	currentVersion = 4
)

type Scrypt struct {
	N int
	R int
	P int
}

func NewScrypt() *Scrypt {
	s := &Scrypt{
		N:StandardScryptN,
		R:StandardScryptR,
		P:StandardScryptP,
	}
	return s
}

func (s *Scrypt) Encrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := s.scryptEncrypt(data, passphrase, s.N, s.R, s.P)
	if err != nil {
		return nil, err
	}
	return json.Marshal(crypto)
}

func (s *Scrypt) EncryptKey(address string, data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := s.scryptEncrypt(data, passphrase, s.N, s.R, s.P)
	if err != nil {
		return nil, err
	}
	uuid, _ := uuid.NewV4()
	encryptedKeyJSON := encryptedKeyJSON{
		address,
		*crypto,
		uuid.String(),
		currentVersion,
	}
	return json.Marshal(encryptedKeyJSON)
}

func (s *Scrypt) Decrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto := new(cryptoJSON)
	if err := json.Unmarshal(data, crypto); err != nil {
		return nil, err
	}
	return s.scryptDecrypt(crypto, passphrase, currentVersion)
}

func (s *Scrypt) DecryptKey(keyjson []byte, passphrase []byte) ([]byte, error) {
	keyJSON := new(encryptedKeyJSON)
	if err := json.Unmarshal(keyjson, keyJSON); err != nil {
		return nil, err
	}
	version := keyJSON.Version
	if version != currentVersion && version != version3 {
		return nil, ErrVersionInvalid
	}
	return s.scryptDecrypt(&keyJSON.Crypto, passphrase, version)
}

func (s *Scrypt) scryptEncrypt(data []byte, passphrase []byte, N, r, p int) (*cryptoJSON, error) {
	salt := random.GetEntropyCSPRNG(ScryptDKLen)
	derivedKey, err := scrypt.Key(passphrase, salt, N, r, p, ScryptDKLen)
	if err != nil {
		return nil, err
	}
	encryptKey := derivedKey[:16]

	iv := random.GetEntropyCSPRNG(aes.BlockSize) // 16
	cipherText, err := s.aesCTRXOR(encryptKey, data, iv)
	if err != nil {
		return nil, err
	}

	//mac := hash.Sha3256(derivedKey[16:32], cipherText) // version3: deprecated
	mac := hash.Sha3256(derivedKey[16:32], cipherText, iv, []byte(cipherName))

	scryptParamsJSON := make(map[string]interface{}, 5)
	scryptParamsJSON["n"] = N
	scryptParamsJSON["r"] = r
	scryptParamsJSON["p"] = p
	scryptParamsJSON["dklen"] = ScryptDKLen
	scryptParamsJSON["salt"] = hex.EncodeToString(salt)

	cipherParamsJSON := cipherparamsJSON{
		IV: hex.EncodeToString(iv),
	}

	crypto := &cryptoJSON{
		Cipher:       cipherName,
		CipherText:   hex.EncodeToString(cipherText),
		CipherParams: cipherParamsJSON,
		KDF:          ScryptKDF,
		KDFParams:    scryptParamsJSON,
		MAC:          hex.EncodeToString(mac),
		MACHash:      macHash,
	}
	return crypto, nil
}

func (s *Scrypt) aesCTRXOR(key, inText, iv []byte) ([]byte, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(aesBlock, iv)
	outText := make([]byte, len(inText))
	stream.XORKeyStream(outText, inText)
	return outText, err
}

func (s *Scrypt) scryptDecrypt(crypto *cryptoJSON, passphrase []byte, version int) ([]byte, error) {

	if crypto.Cipher != cipherName {
		return nil, ErrCipherInvalid
	}

	mac, err := hex.DecodeString(crypto.MAC)
	if err != nil {
		return nil, err
	}

	iv, err := hex.DecodeString(crypto.CipherParams.IV)
	if err != nil {
		return nil, err
	}

	cipherText, err := hex.DecodeString(crypto.CipherText)
	if err != nil {
		return nil, err
	}

	salt, err := hex.DecodeString(crypto.KDFParams["salt"].(string))
	if err != nil {
		return nil, err
	}

	dklen := ensureInt(crypto.KDFParams["dklen"])
	var derivedKey = []byte{}
	if crypto.KDF == ScryptKDF {
		n := ensureInt(crypto.KDFParams["n"])
		r := ensureInt(crypto.KDFParams["r"])
		p := ensureInt(crypto.KDFParams["p"])
		derivedKey, err = scrypt.Key(passphrase, salt, n, r, p, dklen)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrKDFInvalid
	}

	var calculatedMAC []byte

	if version == currentVersion {
		calculatedMAC = hash.Sha3256(derivedKey[16:32], cipherText, iv, []byte(crypto.Cipher))
	} else if version == version3 {
		calculatedMAC = hash.Sha3256(derivedKey[16:32], cipherText)
		if crypto.MACHash != macHash {
			// compatible ethereum keystore file,
			calculatedMAC = hash.Keccak256(derivedKey[16:32], cipherText)
		}
	} else {
		return nil, ErrVersionInvalid
	}

	if !bytes.Equal(calculatedMAC, mac) {
		return nil, ErrDecrypt
	}

	key, err := s.aesCTRXOR(derivedKey[:16], cipherText, iv)
	if err != nil {
		return nil, err
	}
	return key, nil
}
