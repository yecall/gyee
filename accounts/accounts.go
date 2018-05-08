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
 
package accounts

import (
	"bytes"
	"crypto/elliptic"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

const keystoreFile = "keystore.dat"

type Accounts struct {
	Accounts map[string]*Account
}

func AccountsManager() *Accounts {
	accounts := Accounts{}
	accounts.Accounts = make(map[string]*Account)
	err := accounts.LoadFromFile()
	if err != nil {
		log.Println(err)
	}
	return &accounts
}

func (acc *Accounts) NewAccount() *Account {
	account := NewAccount()
	address := fmt.Sprintf("%s", account.GetAddress())
	acc.Accounts[address] = account
	acc.SaveToFile()
	return account
}

func (acc *Accounts) GetAccount(address string) *Account {
	return acc.Accounts[address]
}

func (acc *Accounts) GetAddresses() []string {
	var addresses []string
	for address := range acc.Accounts {
		addresses = append(addresses, address)
	}
	return addresses
}

func (acc *Accounts) LoadFromFile() error {
	if _, err := os.Stat(keystoreFile); os.IsNotExist(err) {
		return err
	}

	fileContent, err := ioutil.ReadFile(keystoreFile)
	if err != nil {
		log.Panic(err)
	}

	var accounts Accounts
	gob.Register(elliptic.P256())
	decoder := gob.NewDecoder(bytes.NewReader(fileContent))
	err = decoder.Decode(&accounts)
	if err != nil {
		log.Panic(err)
	}

	acc.Accounts = accounts.Accounts
	return nil
}

func (acc *Accounts) SaveToFile() {
	var content bytes.Buffer

	gob.Register(elliptic.P256())
	encoder := gob.NewEncoder(&content)
	err := encoder.Encode(acc)
	if err != nil {
		log.Panic(err)
	}

	err = ioutil.WriteFile(keystoreFile, content.Bytes(), 0644)
	if err != nil {
		log.Panic(err)
	}
}
