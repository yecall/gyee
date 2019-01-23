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

	"github.com/urfave/cli"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/version"
)

var (
	versionCommand = cli.Command{
		Action:    config.MergeFlags(printVersion),
		Name:      "version",
		Usage:     "Print version numbers",
		ArgsUsage: " ",
		Category:  "MISC COMMANDS",
	}
	licenseCommand = cli.Command{
		Action:    config.MergeFlags(printLicense),
		Name:      "license",
		Usage:     "Display license information",
		ArgsUsage: " ",
		Category:  "MISC COMMANDS",
	}
)

func printVersion(ctx *cli.Context) error {
	version.PrintVersion()
	//node := makeNode(ctx)
	//
	//fmt.Println("Version:", version)
	//if commit != "" {
	//	fmt.Println("Git Commit:", commit)
	//}
	//fmt.Println("Protocol Versions:", node.NebProtocolID)
	//fmt.Println("Protocol ClientVersion:", net.ClientVersion)
	//fmt.Printf("Chain Id: %d\n", node.Config().Chain.ChainID)
	//fmt.Println("Go Version:", runtime.Version())
	//fmt.Println("Operating System:", runtime.GOOS)
	//fmt.Printf("GOPATH=%s\n", os.Getenv("GOPATH"))
	//fmt.Printf("GOROOT=%s\n", runtime.GOROOT())
	return nil
}

func printLicense(_ *cli.Context) error {
	fmt.Println("The preferred license for the Gyee Open Source Project is the GNU Lesser General Public License Version 3.0 (“LGPL v3”), which is commercial friendly, and encourage developers or companies modify and publish their changes.")

	return nil
}
