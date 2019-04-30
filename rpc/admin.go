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

package rpc

import (
	"context"
	"time"

	"github.com/yeeco/gyee/accounts"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/rpc/pb"
)

type AdminService struct {
	server RPCServer
	am     *accounts.AccountManager
}

func newAdminService(server RPCServer) *AdminService {
	return &AdminService{
		server: server,
		am:     server.Node().AccountManager(),
	}
}

func (s *AdminService) Accounts(ctx context.Context, req *rpcpb.NonParamsRequest) (*rpcpb.AccountsResponse, error) {
	addrList := s.am.Accounts()
	strList := make([]string, 0, len(addrList))
	for _, addr := range addrList {
		strList = append(strList, addr.String())
	}
	return &rpcpb.AccountsResponse{Addresses: strList}, nil
}

func (s *AdminService) NewAccount(ctx context.Context, req *rpcpb.NewAccountRequest) (*rpcpb.NewAccountResponse, error) {
	addr, err := s.am.CreateNewAccount([]byte(req.Passphrase))
	if err != nil {
		return nil, err
	}
	return &rpcpb.NewAccountResponse{Address: addr.String()}, nil
}

func (s *AdminService) UnlockAccount(ctx context.Context, req *rpcpb.UnlockAccountRequest) (*rpcpb.UnlockAccountResponse, error) {
	addr, err := address.AddressParse(req.Address)
	if err != nil {
		return nil, err
	}
	err = s.am.Unlock(addr, []byte(req.Passphrase), time.Duration(req.Duration))
	return &rpcpb.UnlockAccountResponse{Result: err == nil}, err
}

func (s *AdminService) LockAccount(ctx context.Context, req *rpcpb.LockAccountRequest) (*rpcpb.LockAccountResponse, error) {
	addr, err := address.AddressParse(req.Address)
	if err != nil {
		return nil, err
	}
	err = s.am.Lock(addr)
	return &rpcpb.LockAccountResponse{Result: err == nil}, err
}
