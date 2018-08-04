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

package console

import (
	"io"

	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterh/liner"
	"github.com/yeeco/gyee/res"
	"os"
	"os/signal"
	"strings"
)

const (
	defaultPrompt = "> "
	exitCommand   = "exit"
)

var (
	bignumberJS = res.MustAsset("library/bignumber.js")
)

type Console struct {
	prompter UserPrompter
	promptCh chan string
	history  []string
	bridge   *jsBridge
	jsre     *JSRE
	writer   io.Writer
}

// New a console by Config, neb.config params is need
func NewConsole() *Console {
	c := Console{
		prompter: Stdin,
		promptCh: make(chan string),
		writer:   os.Stdout,
	}

	c.bridge = newBridge("", c.prompter, c.writer)
	c.jsre = newJSRE()

	if err := c.loadLibraryScripts(); err != nil {
		fmt.Fprintln(c.writer, err)
	}

	if err := c.methodSwizzling(); err != nil {
		fmt.Fprintln(c.writer, err)
	}
	return &c
}

func (c *Console) loadLibraryScripts() error {
	if err := c.jsre.Compile("bignumber.js", bignumberJS); err != nil {
		return fmt.Errorf("bignumber.js: %v", err)
	}
	//TODO:添加node相关的js
	return nil
}

// Individual methods use go implementation
func (c *Console) methodSwizzling() error {

	// replace js console log & error with go impl
	jsconsole, _ := c.jsre.Get("console")
	jsconsole.Object().Set("log", c.bridge.output)
	jsconsole.Object().Set("error", c.bridge.output)

	// replace js xmlhttprequest to go implement
	c.jsre.Set("bridge", struct{}{})
	bridgeObj, _ := c.jsre.Get("bridge")
	bridgeObj.Object().Set("request", c.bridge.request)
	bridgeObj.Object().Set("asyncRequest", c.bridge.request)

	if _, err := c.jsre.Run("var Neb = require('neb');"); err != nil {
		return fmt.Errorf("neb require: %v", err)
	}
	if _, err := c.jsre.Run("var neb = new Neb(bridge);"); err != nil {
		return fmt.Errorf("neb create: %v", err)
	}
	jsAlias := "var api = neb.api; var admin = neb.admin; "
	if _, err := c.jsre.Run(jsAlias); err != nil {
		return fmt.Errorf("namespace: %v", err)
	}

	if c.prompter != nil {
		admin, err := c.jsre.Get("admin")
		if err != nil {
			return err
		}
		if obj := admin.Object(); obj != nil {
			bridgeRequest := `bridge._sendRequest = function (method, api, params, callback) {
				var action = "/admin" + api;
				return this.request(method, action, params);
			};`
			if _, err = c.jsre.Run(bridgeRequest); err != nil {
				return fmt.Errorf("bridge._sendRequest: %v", err)
			}

			if _, err = c.jsre.Run(`bridge.newAccount = admin.newAccount;`); err != nil {
				return fmt.Errorf("admin.newAccount: %v", err)
			}
			if _, err = c.jsre.Run(`bridge.unlockAccount = admin.unlockAccount;`); err != nil {
				return fmt.Errorf("admin.unlockAccount: %v", err)
			}
			if _, err = c.jsre.Run(`bridge.sendTransactionWithPassphrase = admin.sendTransactionWithPassphrase;`); err != nil {
				return fmt.Errorf("admin.sendTransactionWithPassphrase: %v", err)
			}
			if _, err = c.jsre.Run(`bridge.signTransactionWithPassphrase = admin.signTransactionWithPassphrase;`); err != nil {
				return fmt.Errorf("admin.signTransactionWithPassphrase: %v", err)
			}
			obj.Set("setHost", c.bridge.setHost)
			obj.Set("newAccount", c.bridge.newAccount)
			obj.Set("unlockAccount", c.bridge.unlockAccount)
			obj.Set("sendTransactionWithPassphrase", c.bridge.sendTransactionWithPassphrase)
			obj.Set("signTransactionWithPassphrase", c.bridge.signTransactionWithPassphrase)
		}
	}
	return nil
}

// AutoComplete console auto complete input
func (c *Console) AutoComplete(line string, pos int) (string, []string, string) {
	// No completions can be provided for empty inputs
	if len(line) == 0 || pos == 0 {
		return "", nil, ""
	}
	start := pos - 1
	for ; start > 0; start-- {
		// Skip all methods and namespaces
		if line[start] == '.' || (line[start] >= 'a' && line[start] <= 'z') || (line[start] >= 'A' && line[start] <= 'Z') {
			continue
		}
		start++
		break
	}
	if start == pos {
		return "", nil, ""
	}
	return line[:start], c.jsre.CompleteKeywords(line[start:pos]), line[pos:]
}

// Setup setup console
func (c *Console) Setup() {
	if c.prompter != nil {
		c.prompter.SetWordCompleter(c.AutoComplete)
	}
	fmt.Fprint(c.writer, "Welcome to the Gyee JavaScript console!\n")
}

// Interactive starts an interactive user session.
func (c *Console) Interactive() {
	// Start a goroutine to listen for promt requests and send back inputs
	go func() {
		for {
			// Read the next user input
			line, err := c.prompter.Prompt(<-c.promptCh)
			if err != nil {
				if err == liner.ErrPromptAborted { // ctrl-C
					c.promptCh <- exitCommand
					continue
				}
				close(c.promptCh)
				return
			}
			c.promptCh <- line
		}
	}()
	// Monitor Ctrl-C
	abort := make(chan os.Signal, 1)
	signal.Notify(abort, os.Interrupt, os.Kill)

	// Start sending prompts to the user and reading back inputs
	for {
		// Send the next prompt, triggering an input read and process the result
		c.promptCh <- defaultPrompt
		select {
		case <-abort:
			fmt.Fprint(c.writer, "exiting...")
			return
		case line, ok := <-c.promptCh:
			// User exit
			if !ok || strings.ToLower(line) == exitCommand {
				return
			}
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}

			if command := strings.TrimSpace(line); len(c.history) == 0 || command != c.history[len(c.history)-1] {
				c.history = append(c.history, command)
				if c.prompter != nil {
					c.prompter.AppendHistory(command)
				}
			}
			c.Evaluate(line)
		}
	}
}

// Evaluate executes code and pretty prints the result
func (c *Console) Evaluate(code string) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(c.writer, "[native] error: %v\n", r)
		}
	}()
	v, err := c.jsre.Run(code)
	if err != nil {
		fmt.Fprintln(c.writer, err)
		return err
	}
	if v.IsObject() {
		result, err := c.jsre.JSONString(v)
		if err != nil {
			fmt.Fprintln(c.writer, err)
			return err
		}
		var buf bytes.Buffer
		err = json.Indent(&buf, []byte(result), "", "    ")
		if err != nil {
			fmt.Fprintln(c.writer, err)
			return err
		}
		fmt.Fprintln(c.writer, buf.String())
	} else if v.IsString() {
		fmt.Fprintln(c.writer, v.String())
	}
	return nil
}

// Stop stop js console
func (c *Console) Stop() error {
	return nil
}
