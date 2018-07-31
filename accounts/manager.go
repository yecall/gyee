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
/*
account manager的功能分类：
1、支持console和ipc的账户功能，create、list、reset、delete等
2、account的keystore文件的load、save等，import，export? 这部分还是放在keystore里？
3、account的lock、unlock
4、account来签名交易，签名block，签名hash等

unlock的时候，可不可以记录来源？比如console中，rpc中，wallet中等区分

keystore package：
1、set、get、delete、list
2、lock、unlock、getunlock

cipher：
1、scrypt
2、argon2
3、balloon hashing?
 */

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