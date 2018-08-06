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
	"fmt"
	"testing"

	"time"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
)

var (
	conf       = config.GetDefaultConfig()
	am, _      = NewAccountManager(conf)
	passphrase = "passphrase"
)

func Test_CreateNewAccount(t *testing.T) {
	address, err := am.CreateNewAccount([]byte(passphrase))
	if err != nil {
		fmt.Printf("failed create new account")
	}
	fmt.Printf("Account address: %s\n", address.String())
}

func Test_List(t *testing.T) {
	for i, addr := range am.Accounts() {
		fmt.Printf("Account #%d: %s\n", i, addr.String())
	}
}

func Test_Unlock(t *testing.T) {
	addr, _ := core.AddressParse("0105cfa04d12fb46fcea51d22cf1f340631bbe930dc0e026ba21")

	am.ks.Unlock("0105cfa04d12fb46fcea51d22cf1f340631bbe930dc0e026ba21", []byte(passphrase), time.Duration(time.Second))
	am.SignHash(addr, common.Hash("abc"))
	time.Sleep(time.Duration(500) * time.Millisecond)
	am.SignHash(addr, common.Hash("abc"))
	time.Sleep(time.Duration(2) * time.Second)
	am.SignHash(addr, common.Hash("cdf"))
}
