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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"

	"github.com/satori/go.uuid"
	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/random"
	"golang.org/x/crypto/argon2"
)

const (
	// Argon2KDF name
	Argon2KDF = "argon2id"

	//N: General work factor, iteration count.
	//r: blocksize in use for underlying hash; fine-tunes the relative memory-cost.
	//p: parallelization factor; fine-tunes the relative cpu-cost.

	// N:18, P:1 -> Using 256MB memory and taking approximately 1s CPU time on a modern processor.
	// N:12, P:6 -> Using 4MB memory and taking approximately 100ms CPU time on a modern processor.

	StandardArgon2Time = 4

	StandardArgon2Memory = 256 * 1024

	StandardArgon2Threads = 4

	// Argon2DKLen get derived key length
	Argon2DKLen = 32
)

type Argon2 struct {
	Time    uint32
	Memory  uint32
	Threads uint8
}

func NewArgon2() *Argon2 {
	ar := &Argon2{
		Time:    StandardArgon2Time,
		Memory:  StandardArgon2Memory,
		Threads: StandardArgon2Threads,
	}
	return ar
}

func (a *Argon2) Encrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := a.scryptEncrypt(data, passphrase, a.Time, a.Memory, a.Threads)
	if err != nil {
		return nil, err
	}
	return json.Marshal(crypto)
}

func (a *Argon2) EncryptKey(address string, data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := a.scryptEncrypt(data, passphrase, a.Time, a.Memory, a.Threads)
	if err != nil {
		return nil, err
	}
	uuid, _ := uuid.NewV4()
	encryptedKeyJSON := encryptedKeyJSON{
		Address: address,
		Crypto:  *crypto,
		ID:      uuid.String(),
		Version: 1,
	}
	return json.Marshal(encryptedKeyJSON)
}

func (a *Argon2) Decrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto := new(cryptoJSON)
	if err := json.Unmarshal(data, crypto); err != nil {
		return nil, err
	}
	return a.scryptDecrypt(crypto, passphrase)
}

func (a *Argon2) DecryptKey(keyjson []byte, passphrase []byte) ([]byte, error) {
	keyJSON := new(encryptedKeyJSON)
	if err := json.Unmarshal(keyjson, keyJSON); err != nil {
		return nil, err
	}
	return a.scryptDecrypt(&keyJSON.Crypto, passphrase)
}

func (a *Argon2) scryptEncrypt(data []byte, passphrase []byte, t, m uint32, th uint8) (*cryptoJSON, error) {
	salt := random.GetEntropyCSPRNG(Argon2DKLen)
	derivedKey := argon2.IDKey(passphrase, salt, t, m, th, Argon2DKLen)
	//derivedKey, err := scrypt.Key(passphrase, salt, N, r, p, Argon2DKLen)
	//if err != nil {
	//	return nil, err
	//}
	encryptKey := derivedKey[:16]

	iv := random.GetEntropyCSPRNG(aes.BlockSize) // 16
	cipherText, err := a.aesCTRXOR(encryptKey, data, iv)
	if err != nil {
		return nil, err
	}

	//mac := hash.Sha3256(derivedKey[16:32], cipherText) // version3: deprecated
	mac := hash.Sha3256(derivedKey[16:32], cipherText, iv, []byte(cipherName))

	scryptParamsJSON := make(map[string]interface{}, 5)
	scryptParamsJSON["time"] = t
	scryptParamsJSON["memory"] = m
	scryptParamsJSON["threads"] = th
	scryptParamsJSON["dklen"] = Argon2DKLen
	scryptParamsJSON["salt"] = hex.EncodeToString(salt)

	cipherParamsJSON := cipherparamsJSON{
		IV: hex.EncodeToString(iv),
	}

	crypto := &cryptoJSON{
		Cipher:       cipherName,
		CipherText:   hex.EncodeToString(cipherText),
		CipherParams: cipherParamsJSON,
		KDF:          Argon2KDF,
		KDFParams:    scryptParamsJSON,
		MAC:          hex.EncodeToString(mac),
		MACHash:      macHash,
	}
	return crypto, nil
}

func (a *Argon2) aesCTRXOR(key, inText, iv []byte) ([]byte, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(aesBlock, iv)
	outText := make([]byte, len(inText))
	stream.XORKeyStream(outText, inText)
	return outText, err
}

func (a *Argon2) scryptDecrypt(crypto *cryptoJSON, passphrase []byte) ([]byte, error) {

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
	if crypto.KDF == Argon2KDF {
		t := ensureInt(crypto.KDFParams["time"])
		m := ensureInt(crypto.KDFParams["memory"])
		th := ensureInt(crypto.KDFParams["threads"])
		derivedKey = argon2.IDKey(passphrase, salt, uint32(t), uint32(m), uint8(th), uint32(dklen))
		//if err != nil {
		//	return nil, err
		//}
	} else {
		return nil, ErrKDFInvalid
	}

	calculatedMAC := hash.Sha3256(derivedKey[16:32], cipherText, iv, []byte(crypto.Cipher))

	if !bytes.Equal(calculatedMAC, mac) {
		return nil, ErrDecrypt
	}

	key, err := a.aesCTRXOR(derivedKey[:16], cipherText, iv)
	if err != nil {
		return nil, err
	}
	return key, nil
}
