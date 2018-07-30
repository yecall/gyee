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
	"errors"

	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/random"
	"golang.org/x/crypto/scrypt"
	"bytes"
	"encoding/json"
)

const (
	// ScryptKDF name
	ScryptKDF = "scrypt"

	// StandardScryptN N parameter of Scrypt encryption algorithm
	StandardScryptN = 1 << 12 //TODO: check 1<<12 vs 1<<18

	// StandardScryptR r parameter of Scrypt encryption algorithm
	StandardScryptR = 8

	// StandardScryptP p parameter of Scrypt encryption algorithm
	StandardScryptP = 1

	// ScryptDKLen get derived key length
	ScryptDKLen = 32

	// cipher the name of cipher
	cipherName = "aes-128-ctr"

	// version compatible with ethereum, the version start with 3
	version3       = 3
	currentVersion = 4

	// mac calculate hash type
	macHash = "sha3256"
)

var (
	// ErrVersionInvalid version not supported
	ErrVersionInvalid = errors.New("version not supported")

	// ErrKDFInvalid cipher not supported
	ErrKDFInvalid = errors.New("kdf not supported")

	// ErrCipherInvalid cipher not supported
	ErrCipherInvalid = errors.New("cipher not supported")

	// ErrDecrypt decrypt failed
	ErrDecrypt = errors.New("could not decrypt key with given passphrase")
)

type cipherparamsJSON struct {
	IV string `json:"iv"`
}

type cryptoJSON struct {
	Cipher       string                 `json:"cipher"`
	CipherText   string                 `json:"ciphertext"`
	CipherParams cipherparamsJSON       `json:"cipherparams"`
	KDF          string                 `json:"kdf"`
	KDFParams    map[string]interface{} `json:"kdfparams"`
	MAC          string                 `json:"mac"`
	MACHash      string                 `json:"machash"`
}

type encryptedKeyJSON struct {
	Address string     `json:"address"`
	Crypto  cryptoJSON `json:"crypto"`
	ID      string     `json:"id"`
	Version int        `json:"version"`
}

type Scrypt struct {
}

func NewScrypt() *Scrypt {
	return nil
}

func (s *Scrypt) Encrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := s.scryptEncrypt(data, passphrase, StandardScryptN, StandardScryptR, StandardScryptP)
	if err != nil {
		return nil, err
	}
    return json.Marshal(crypto)
}

func (s *Scrypt) EncryptKey(address string, data []byte, passphrase []byte) ([]byte, error) {
	return nil, nil
}

func (s *Scrypt) Decrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto := new(cryptoJSON)
	if err := json.Unmarshal(data, crypto); err != nil {
		return nil, err
	}
	return s.scryptDecrypt(crypto, passphrase, currentVersion)
}

func (s *Scrypt) DecryptKey(keyjson []byte, passphrase []byte) ([]byte, error) {
	return nil, nil
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

// because json.Unmarshal change int to float64, convert to int
func ensureInt(x interface{}) int {
	res, ok := x.(int)
	if !ok {
		res = int(x.(float64))
	}
	return res
}