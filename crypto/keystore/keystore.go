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

package keystore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/crypto/keystore/cipher"
	"github.com/yeeco/gyee/crypto/util"
	"github.com/yeeco/gyee/utils/logging"
)

//keystore package：
//1、set、get、delete、list
//2、lock、unlock、getunlock
//3、如何避免密钥解锁期间的rpc攻击问题？解锁请求都要有来源申请token？有session的概念
//4、如何让key在内存中及时的擦除？

var (
	ErrNeedAddress       = errors.New("need address")
	ErrNotFound          = errors.New("key not found")
	ErrNotUnlocked       = errors.New("key not unlocked")
	ErrInvalidPassphrase = errors.New("passphrase is invalid")
)

type Keystore struct {
	ksDirPath string
	cipher    cipher.Cipher
	entries   map[string][]byte

	mu sync.RWMutex
}

func NewKeystore(dirPath string) *Keystore {
	ks := &Keystore{
		ksDirPath: dirPath,
		cipher:    cipher.NewScrypt(),
		//entries:   make(map[string][]byte),
	}
	//load from file dir
	ks.loadKeyFiles()
	return ks
}

func (ks *Keystore) SetKey(address string, key []byte, passphrase []byte) error {
	if len(address) == 0 {
		return ErrNeedAddress
	}
	if len(passphrase) == 0 {
		return ErrInvalidPassphrase
	}

	keyjson, err := ks.cipher.EncryptKey(address, key, passphrase)
	if err != nil {
		return err
	}

	filename := filepath.Join(ks.ksDirPath, keyFileName(address))
	err = writeKeyFile(filename, keyjson)
	util.ZeroBytes(key)

	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.entries[address] = keyjson

	return err
}

func (ks *Keystore) GetKey(address string, passphrase []byte) ([]byte, error) {
	if len(address) == 0 {
		return nil, ErrNeedAddress
	}
	if len(passphrase) == 0 {
		return nil, ErrInvalidPassphrase
	}

	ks.mu.RLock()
	defer ks.mu.RUnlock()

	entry, ok := ks.entries[address]
	if !ok {
		return nil, ErrNotFound
	}
	data, err := ks.cipher.DecryptKey(entry, passphrase)
	if err != nil {
		return nil, err
	}
	//TODO:这儿要不要验证一下是否是合法的私钥？
	return data, nil
}

func (ks *Keystore) Delete(address string) error {
	if len(address) == 0 {
		return ErrNeedAddress
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()
	delete(ks.entries, address)
	//TODO:keystore file要不要也删除？
	return nil
}

func (ks *Keystore) List() []string {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	addresses := []string{}
	for address := range ks.entries {
		addresses = append(addresses, address)
	}
	return addresses
}

func (ks *Keystore) Contains(address string) (bool, error) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	if len(address) == 0 {
		return false, ErrNeedAddress
	}

	if _, ok := ks.entries[address]; ok {
		return true, nil
	}
	return false, ErrNotFound
}

func (ks *Keystore) Lock(address string) error {
	return nil
}

func (ks *Keystore) Unlock(address string, passphrase []byte, timeout time.Duration) error {
	return nil
}

func (ks *Keystore) GetUnlocked() {

}

func (ks *Keystore) loadKeyFiles() {
	var (
		keyJSON struct {
			Address string `json:"address"`
		}
	)

	files, err := ioutil.ReadDir(ks.ksDirPath)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"dir": ks.ksDirPath,
			"err": err,
		}).Panic("")
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.entries = make(map[string][]byte)

	for _, file := range files {
		filename := filepath.Join(ks.ksDirPath, file.Name())
		if file.IsDir() || strings.HasPrefix(file.Name(), ".") || strings.HasSuffix(file.Name(), "~") {
			logging.Logger.WithFields(logrus.Fields{
				"file": filename,
			}).Warn("Seems not key file, skipped")
			continue
		}

		content, err := ioutil.ReadFile(filename)
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"file": filename,
				"err":  err,
			}).Error("Failed to read the key file")
			continue
		}

		keyJSON.Address = ""
		err = json.Unmarshal(content, &keyJSON)
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"file": filename,
				"err":  err,
			}).Error("Not correct key file content")
			continue
		} else {
			_, err := core.AddressParse(keyJSON.Address)
			if err != nil {
				logging.Logger.WithFields(logrus.Fields{
					"address": keyJSON.Address,
					"err":     err,
				}).Error("Failed to parse the address")
				continue
			}
			ks.entries[keyJSON.Address] = content
		}
	}
}

func writeKeyFile(file string, content []byte) error {
	const dirPerm = 0700
	if err := os.MkdirAll(filepath.Dir(file), dirPerm); err != nil {
		return err
	}
	// Atomic write: create a temporary hidden file first
	// then move it into place. TempFile assigns mode 0600.
	f, err := ioutil.TempFile(filepath.Dir(file), "."+filepath.Base(file)+".tmp")
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	f.Close()
	return os.Rename(f.Name(), file)
}

func keyFileName(keyAddr string) string {
	ts := time.Now().UTC()
	return fmt.Sprintf("UTC--%s--%s", toISO8601(ts), keyAddr)
}

func toISO8601(t time.Time) string {
	var tz string
	name, offset := t.Zone()
	if name == "UTC" {
		tz = "Z"
	} else {
		tz = fmt.Sprintf("%03d00", offset/3600)
	}
	return fmt.Sprintf("%04d-%02d-%02dT%02d-%02d-%02d.%09d%s", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), tz)
}
