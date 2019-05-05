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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/peterh/liner"
	"github.com/yeeco/gyee/res"
	"google.golang.org/grpc"
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
func NewConsole(conn *grpc.ClientConn) *Console {
	c := Console{
		prompter: Stdin,
		promptCh: make(chan string),
		writer:   os.Stdout,
	}

	c.bridge = newBridge(conn, c.prompter, c.writer)
	c.jsre = newJSRE(c.writer)

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
	if err := c.setupBridge(); err != nil {
		return err
	}

	return nil
}

func (c *Console) setupBridge() error {
	bridgeObj, err := c.jsre.Get("bridge")
	if err != nil {
		return err
	}
	obj := bridgeObj.Object()
	_ = obj.Set("nodeInfo", c.bridge.nodeInfo)

	_ = obj.Set("request", c.bridge.request)
	_ = obj.Set("asyncRequest", c.bridge.request)

	_ = obj.Set("Accounts", c.bridge.getAccounts)
	_ = obj.Set("newAccount", c.bridge.newAccount)
	_ = obj.Set("unlockAccount", c.bridge.unlockAccount)
	_ = obj.Set("lockAccount", c.bridge.lockAccount)

	_ = obj.Set("sendTransaction", c.bridge.sendTransaction)

	// temporary bridge api, should switch to js binding later
	if true {
		_ = obj.Set("getBlockByHash", c.bridge.getBlockByHash)
		_ = obj.Set("getBlockByHeight", c.bridge.getBlockByHeight)
		_ = obj.Set("getLastBlock", c.bridge.getLastBlock)
		_ = obj.Set("getTxByHash", c.bridge.getTxByHash)
		_ = obj.Set("getAccountState", c.bridge.getAccountState)

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
