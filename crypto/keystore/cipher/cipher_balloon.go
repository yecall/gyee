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

/*
reference from: https://github.com/nogoegst/balloon
这个不是官方实现，只能用于测试目的
*/

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"

	"github.com/nogoegst/balloon"
	"github.com/satori/go.uuid"
	"github.com/yeeco/gyee/crypto/hash"
	"github.com/yeeco/gyee/crypto/random"

	"golang.org/x/crypto/blake2b"
)

const (
	// BalloonKDF name
	BalloonKDF = "balloon"

	StandardBalloonTime = 16

	StandardBalloonSpace = 8 * 1024

	// BalloonDKLen get derived key length
	BalloonDKLen = 64

	BalloonCipherName = "aes-256-ctr"
)

type Balloon struct {
	Time  uint64
	Space uint64
}

func NewBalloon() *Balloon {
	b := &Balloon{
		Time:  StandardBalloonTime,
		Space: StandardBalloonSpace,
	}
	return b
}

func (b *Balloon) Encrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := b.scryptEncrypt(data, passphrase, b.Time, b.Space)
	if err != nil {
		return nil, err
	}
	return json.Marshal(crypto)
}

func (b *Balloon) EncryptKey(address string, data []byte, passphrase []byte) ([]byte, error) {
	crypto, err := b.scryptEncrypt(data, passphrase, b.Time, b.Space)
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

func (b *Balloon) Decrypt(data []byte, passphrase []byte) ([]byte, error) {
	crypto := new(cryptoJSON)
	if err := json.Unmarshal(data, crypto); err != nil {
		return nil, err
	}
	return b.scryptDecrypt(crypto, passphrase)
}

func (b *Balloon) DecryptKey(keyjson []byte, passphrase []byte) ([]byte, error) {
	keyJSON := new(encryptedKeyJSON)
	if err := json.Unmarshal(keyjson, keyJSON); err != nil {
		return nil, err
	}
	return b.scryptDecrypt(&keyJSON.Crypto, passphrase)
}

func (b *Balloon) scryptEncrypt(data []byte, passphrase []byte, t, s uint64) (*cryptoJSON, error) {
	salt := random.GetEntropyCSPRNG(BalloonDKLen)
	//derivedKey := argon2.IDKey(passphrase, salt, t, m, th, BalloonDKLen)
	//derivedKey, err := scrypt.Key(passphrase, salt, N, r, p, BalloonDKLen)
	h, _ := blake2b.New512(nil)
	derivedKey := balloon.Balloon(h, passphrase, salt, s, t)
	//if err != nil {
	//	return nil, err
	//}
	encryptKey := derivedKey[:32]

	iv := random.GetEntropyCSPRNG(aes.BlockSize) // 16
	cipherText, err := b.aesCTRXOR(encryptKey, data, iv)
	if err != nil {
		return nil, err
	}

	//mac := hash.Sha3256(derivedKey[16:32], cipherText) // version3: deprecated
	mac := hash.Sha3256(derivedKey[32:64], cipherText, iv, []byte(BalloonCipherName))

	scryptParamsJSON := make(map[string]interface{}, 5)
	scryptParamsJSON["time"] = t
	scryptParamsJSON["space"] = s
	scryptParamsJSON["dklen"] = BalloonDKLen
	scryptParamsJSON["salt"] = hex.EncodeToString(salt)

	cipherParamsJSON := cipherparamsJSON{
		IV: hex.EncodeToString(iv),
	}

	crypto := &cryptoJSON{
		Cipher:       BalloonCipherName,
		CipherText:   hex.EncodeToString(cipherText),
		CipherParams: cipherParamsJSON,
		KDF:          BalloonKDF,
		KDFParams:    scryptParamsJSON,
		MAC:          hex.EncodeToString(mac),
		MACHash:      macHash,
	}
	return crypto, nil
}

func (b *Balloon) aesCTRXOR(key, inText, iv []byte) ([]byte, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(aesBlock, iv)
	outText := make([]byte, len(inText))
	stream.XORKeyStream(outText, inText)
	return outText, err
}

func (b *Balloon) scryptDecrypt(crypto *cryptoJSON, passphrase []byte) ([]byte, error) {

	if crypto.Cipher != BalloonCipherName {
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

	//dklen := ensureInt(crypto.KDFParams["dklen"])
	var derivedKey = []byte{}
	if crypto.KDF == BalloonKDF {
		t := ensureInt(crypto.KDFParams["time"])
		s := ensureInt(crypto.KDFParams["space"])
		//derivedKey = argon2.IDKey(passphrase, salt, uint32(t), uint32(m), uint8(th), uint32(dklen))
		h, _ := blake2b.New512(nil)
		derivedKey = balloon.Balloon(h, passphrase, salt, uint64(s), uint64(t))
		//if err != nil {
		//	return nil, err
		//}
	} else {
		return nil, ErrKDFInvalid
	}

	calculatedMAC := hash.Sha3256(derivedKey[32:64], cipherText, iv, []byte(crypto.Cipher))

	if !bytes.Equal(calculatedMAC, mac) {
		return nil, ErrDecrypt
	}

	key, err := b.aesCTRXOR(derivedKey[:32], cipherText, iv)
	if err != nil {
		return nil, err
	}
	return key, nil
}
