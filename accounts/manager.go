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

import "github.com/yeeco/gyee/core"

type AccountManager struct {
	accounts map[string]*Account
}

func NewAccountManager() *AccountManager {
	//accounts := Accounts{}
	//accounts.Accounts = make(map[string]*Account)
	//err := accounts.LoadFromFile()
	//if err != nil {
	//	log.Println(err)
	//}
	//return &accounts
	return nil
}

func (am *AccountManager) CreateNewAccount(passphrase []byte) (*core.Address, error){
	return nil, nil
}

func (am *AccountManager) Accounts() []*core.Address{
	return nil
}

func (am *AccountManager) ResetPassword(address *core.Address, oldPass []byte, newPass []byte) error {
	return nil
}

func (am *AccountManager) Import(keyContent []byte, passphrase []byte) (*core.Address, error) {
	return nil, nil
}

//TODO：实现这几个func
//TODO：需要搞定keystore的问题