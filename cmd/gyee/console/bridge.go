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
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/robertkrimen/otto"
	"github.com/yeeco/gyee/log"
	rpcpb "github.com/yeeco/gyee/rpc/pb"
	"google.golang.org/grpc"
)

const (
	//APIVersion rpc http version
	APIVersion = "v1"
)

type jsBridge struct {
	// js request host
	//TODO:这儿改为rpc.client
	host string

	conn     *grpc.ClientConn
	svcAdmin rpcpb.AdminServiceClient
	svcApi   rpcpb.ApiServiceClient

	ctx context.Context

	// terminal input prompter
	prompter UserPrompter

	writer io.Writer
}

// newBirdge create a new jsbridge with given prompter and writer
func newBridge(conn *grpc.ClientConn, prompter UserPrompter, writer io.Writer) *jsBridge {
	bridge := &jsBridge{
		conn:     conn,
		svcAdmin: rpcpb.NewAdminServiceClient(conn),
		svcApi:   rpcpb.NewApiServiceClient(conn),
		ctx:      context.Background(),
		prompter: prompter,
		writer:   writer,
	}
	bridge.host = "http://localhost:8685"
	return bridge
}

// output handle the error & log in js runtime
func (b *jsBridge) output(call otto.FunctionCall) otto.Value {
	output := []string{}
	for _, argument := range call.ArgumentList {
		output = append(output, fmt.Sprintf("%v", argument))
	}
	fmt.Fprintln(b.writer, strings.Join(output, " "))
	return otto.NullValue()
}

func (b *jsBridge) nodeInfo(call otto.FunctionCall) otto.Value {
	response, err := b.svcApi.NodeInfo(b.ctx,
		&rpcpb.NonParamsRequest{})
	if err != nil {
		log.Error("nodeInfo()", "err", err)
		return otto.NullValue()
	}
	result, _ := otto.ToValue(fmt.Sprintf("nodeInfo: %v", response))
	return result
}

func (b *jsBridge) getAccounts(call otto.FunctionCall) otto.Value {
	response, err := b.svcAdmin.Accounts(b.ctx, &rpcpb.NonParamsRequest{})
	if err != nil {
		return jsError(call.Otto, err)
	}
	result, _ := otto.ToValue(response.String())
	return result
}

func (b *jsBridge) getBlockByHash(call otto.FunctionCall) otto.Value {
	hash := call.Argument(0)
	if !hash.IsString() {
		return jsError(call.Otto, errors.New("not hash hex str"))
	}
	response, err := b.svcApi.GetBlockByHash(b.ctx,
		&rpcpb.GetBlockByHashRequest{Hash: hash.String()})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

func (b *jsBridge) getBlockByHeight(call otto.FunctionCall) otto.Value {
	hObj := call.Argument(0)
	if !hObj.IsNumber() {
		return jsError(call.Otto, errors.New("not height number"))
	}
	h, _ := hObj.ToInteger()
	response, err := b.svcApi.GetBlockByHeight(b.ctx,
		&rpcpb.GetBlockByHeightRequest{Height: uint64(h)})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

func (b *jsBridge) getLastBlock(call otto.FunctionCall) otto.Value {
	response, err := b.svcApi.GetLastBlock(b.ctx,
		&rpcpb.GetLastBlockRequest{})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

func (b *jsBridge) getTxByHash(call otto.FunctionCall) otto.Value {
	hash := call.Argument(0)
	if !hash.IsString() {
		return jsError(call.Otto, errors.New("not hash hex str"))
	}
	response, err := b.svcApi.GetTxByHash(b.ctx,
		&rpcpb.GetTxByHashRequest{Hash: hash.String()})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

func (b *jsBridge) getAccountState(call otto.FunctionCall) otto.Value {
	addr := call.Argument(0)
	if !addr.IsString() {
		return jsError(call.Otto, errors.New("not addr str"))
	}
	response, err := b.svcApi.GetAccountState(b.ctx,
		&rpcpb.GetAccountStateRequest{Address: addr.String()})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

// request handle http request
func (b *jsBridge) request(call otto.FunctionCall) otto.Value {
	method := call.Argument(0)
	api := call.Argument(1)
	if method.IsNull() || api.IsNull() {
		return jsError(call.Otto, errors.New("request method/api is null"))
	}

	// convert args to string
	JSON, _ := call.Otto.Object("JSON")
	args := ""
	if !call.Argument(2).IsNull() {
		argsVal, err := JSON.Call("stringify", call.Argument(2))
		if err != nil {
			return jsError(call.Otto, err)
		}
		if argsVal.IsString() {
			args = argsVal.String()
		}
	}

	url := b.host + "/" + APIVersion + api.String()
	//fmt.Fprintln(b.writer, "request", url, method.String(), args)
	// method only support upper case.
	req, err := http.NewRequest(strings.ToUpper(method.String()), url, bytes.NewBuffer([]byte(args)))
	if err != nil {
		return jsError(call.Otto, err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return jsError(call.Otto, err)
	}

	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return jsError(call.Otto, err)
	}
	//fmt.Fprintln(b.writer, "result:", result)
	response, err := JSON.Call("parse", string(result))
	if err != nil {
		// if result can't be parse to json obj ,return origin string
		response, _ = otto.ToValue(string(result))
	}

	if fn := call.Argument(3); fn.Class() == "Function" {
		fn.Call(otto.NullValue(), response)
		return otto.UndefinedValue()
	}
	return response
}

// newAccount handle the account generate with passphrase input
func (b *jsBridge) newAccount(call otto.FunctionCall) otto.Value {
	var (
		password string
		err      error
	)
	switch {
	// No password was specified, prompt the user for it
	case len(call.ArgumentList) == 0:
		if password, err = b.prompter.PromptPassphrase("Passphrase: "); err != nil {
			fmt.Fprintln(b.writer, err)
			return otto.NullValue()
		}
		var confirm string
		if confirm, err = b.prompter.PromptPassphrase("Repeat passphrase: "); err != nil {
			fmt.Fprintln(b.writer, err)
			return otto.NullValue()
		}
		if password != confirm {
			fmt.Fprintln(b.writer, errors.New("passphrase don't match"))
			return otto.NullValue()
		}
	case len(call.ArgumentList) == 1 && call.Argument(0).IsString():
		password, _ = call.Argument(0).ToString()
	default:
		fmt.Fprintln(b.writer, errors.New("unexpected argument count"))
		return otto.NullValue()
	}
	// TODO: switch to js wrapping
	//ret, err := call.Otto.Call("bridge.newAccount", nil, password)
	//if err != nil {
	//	fmt.Fprintln(b.writer, err)
	//	return otto.NullValue()
	//}
	response, err := b.svcAdmin.NewAccount(b.ctx,
		&rpcpb.NewAccountRequest{Passphrase: password})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.Address)
	return value
}

// signTransaction handle the account unlock with passphrase input
func (b *jsBridge) unlockAccount(call otto.FunctionCall) otto.Value {
	if !call.Argument(0).IsString() {
		fmt.Fprintln(b.writer, errors.New("address arg must be string"))
		return otto.NullValue()
	}
	address := call.Argument(0)

	var passphrase otto.Value

	if call.Argument(1).IsUndefined() || call.Argument(1).IsNull() {
		fmt.Fprintf(b.writer, "Unlock account %s\n", address)
		var (
			input string
			err   error
		)
		if input, err = b.prompter.PromptPassphrase("Passphrase: "); err != nil {
			fmt.Fprintln(b.writer, err)
			return otto.NullValue()
		}
		passphrase, _ = otto.ToValue(input)
	} else {
		if !call.Argument(1).IsString() {
			fmt.Fprintln(b.writer, errors.New("password must be a string"))
			return otto.NullValue()
		}
		passphrase = call.Argument(1)
	}
	// TODO: switch to js wrapping
	// Send the request to the backend and return
	//val, err := call.Otto.Call("bridge.unlockAccount", nil, address, passphrase)
	//if err != nil {
	//	fmt.Fprintln(b.writer, err)
	//	return otto.NullValue()
	//}
	response, err := b.svcAdmin.UnlockAccount(b.ctx,
		&rpcpb.UnlockAccountRequest{
			Address:    address.String(),
			Passphrase: passphrase.String(),
			Duration:   300,
		})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.Result)
	return value
}

func (b *jsBridge) lockAccount(call otto.FunctionCall) otto.Value {
	response, err := b.svcAdmin.LockAccount(b.ctx,
		&rpcpb.LockAccountRequest{
			Address: call.Argument(0).String(),
		})
	if err != nil {
		return jsError(call.Otto, err)
	}
	value, _ := otto.ToValue(response.String())
	return value
}

// sendTransactionWithPassphrase handle the transaction send with passphrase input
func (b *jsBridge) sendTransactionWithPassphrase(call otto.FunctionCall) otto.Value {
	if !call.Argument(0).IsString() || !call.Argument(1).IsString() {
		fmt.Fprintln(b.writer, errors.New("from/to address arg must be string"))
		return otto.NullValue()
	}
	var passphrase otto.Value
	if call.Argument(8).IsUndefined() || call.Argument(8).IsNull() {
		var (
			input string
			err   error
		)
		if input, err = b.prompter.PromptPassphrase("Passphrase: "); err != nil {
			fmt.Fprintln(b.writer, err)
			return otto.NullValue()
		}
		passphrase, _ = otto.ToValue(input)
	} else {
		if !call.Argument(8).IsString() {
			fmt.Fprintln(b.writer, errors.New("password must be a string"))
			return otto.NullValue()
		}
		passphrase = call.Argument(1)
	}
	// Send the request to the backend and return
	val, err := call.Otto.Call("bridge.sendTransactionWithPassphrase", nil,
		call.Argument(0), call.Argument(1), call.Argument(2),
		call.Argument(3), call.Argument(4), call.Argument(5),
		call.Argument(6), call.Argument(7), passphrase)
	if err != nil {
		fmt.Fprintln(b.writer, err)
		return otto.NullValue()
	}
	return val
}

// signTransactionWithPassphrase handle the transaction sign with passphrase input
func (b *jsBridge) signTransactionWithPassphrase(call otto.FunctionCall) otto.Value {
	if !call.Argument(0).IsString() || !call.Argument(1).IsString() {
		fmt.Fprintln(b.writer, errors.New("from/to address arg must be string"))
		return otto.NullValue()
	}
	var passphrase otto.Value
	if call.Argument(8).IsUndefined() || call.Argument(8).IsNull() {
		var (
			input string
			err   error
		)
		if input, err = b.prompter.PromptPassphrase("Passphrase: "); err != nil {
			fmt.Fprintln(b.writer, err)
			return otto.NullValue()
		}
		passphrase, _ = otto.ToValue(input)
	} else {
		if !call.Argument(8).IsString() {
			fmt.Fprintln(b.writer, errors.New("password must be a string"))
			return otto.NullValue()
		}
		passphrase = call.Argument(1)
	}
	// Send the request to the backend and return
	val, err := call.Otto.Call("bridge.signTransactionWithPassphrase", nil,
		call.Argument(0), call.Argument(1), call.Argument(2),
		call.Argument(3), call.Argument(4), call.Argument(5),
		call.Argument(6), call.Argument(7), passphrase)
	if err != nil {
		fmt.Fprintln(b.writer, err)
		return otto.NullValue()
	}
	return val
}

func jsError(otto *otto.Otto, err error) otto.Value {
	resp, _ := otto.Object(`({})`)
	resp.Set("error", map[string]interface{}{"code": -1, "message": err.Error()})
	return resp.Value()
}
