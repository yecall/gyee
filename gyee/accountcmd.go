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

package main

import (
	"fmt"

	"github.com/yeeco/gyee/gyee/console"
	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/node"
	"github.com/yeeco/gyee/utils/logging"
)

var (
	accountCommand = cli.Command{
		Name:        "account",
		Usage:       "Manage accounts",
		Category:    "ACCOUNT COMMANDS",
		Description: "Manage accounts, create, list, reset password or import",

		Subcommands: []cli.Command{
			{
				Name:        "new",
				Usage:       "Create new account",
				ArgsUsage:   "[passphrase]",
				Description: "",
				Action:      config.MergeFlags(accountCreate),
			},
			{
				Name:        "list",
				Usage:       "List all existing accounts",
				ArgsUsage:   "[passphrase]",
				Description: "",
				Action:      config.MergeFlags(accountList),
			},
			{
				Name:        "resetPassword",
				Usage:       "Reset account password",
				ArgsUsage:   "<address>",
				Description: "",
				Action:      config.MergeFlags(accountResetPassword),
			},
			{
				Name:        "import",
				Usage:       "Import account with private key",
				ArgsUsage:   "<file>",
				Description: "",
				Action:      config.MergeFlags(accountImport),
			},
		},
	}
)

func accountCreate(ctx *cli.Context) error {
	node := makeNode(ctx)

	passphrase := ctx.Args().First()
	if len(passphrase) == 0 {
		passphrase = ""
	}

	address, err := node.AccountManager().CreateNewAccount([]byte(passphrase))
	fmt.Printf("Account address: %s\n", address.String())

	return err
}

func accountList(ctx *cli.Context) error {
	node := makeNode(ctx)

	for i, addr := range node.AccountManager().Accounts() {
		fmt.Printf("Account #%d: %s\n", i, addr.String())
	}
	return nil
}

func accountResetPassword(ctx *cli.Context) error {
	//node := makeNode(ctx)
	return nil
}

func accountImport(ctx *cli.Context) error {
	//node := makeNode(ctx)

	return nil
}

func makeNode(ctx *cli.Context) *node.Node {
	config := config.GetConfig(ctx)
	node, err := node.NewNode(config)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	return node
}

func getPassPhrase(prompt string, confirmation bool) string {
	if prompt != "" {
		fmt.Println(prompt)
	}

	passphrase, err := console.Stdin.PromptPassphrase("Passphrase: ")
	if err != nil {
		logging.Logger.Fatalf("Failed to read passphrase: %v", err)
	}
	if confirmation {
		confirm, err := console.Stdin.PromptPassphrase("Repeat passphrase: ")
		if err != nil {
			logging.Logger.Fatalf("Failed to read passphrase confirmation: %v", err)
		}
		if passphrase != confirm {
			logging.Logger.Fatalf("Passphrases do not match")
		}
	}

	return passphrase
}

//多个不同的节点，account是共用的么？先按照不共用设计
//TODO: 确定keystore，完成accountManager的func
