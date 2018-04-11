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
