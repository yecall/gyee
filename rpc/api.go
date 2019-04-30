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
	"errors"

	"github.com/yeeco/gyee/common"
	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/core/state"
	"github.com/yeeco/gyee/rpc/pb"
)

type APIService struct {
	server RPCServer
	chain  *core.BlockChain
}

func newAPIService(server RPCServer) *APIService {
	return &APIService{
		server: server,
		chain:  server.Core().Chain(),
	}
}

func (s *APIService) NodeInfo(ctx context.Context, req *rpcpb.NonParamsRequest) (*rpcpb.NodeInfoResponse, error) {
	nodeId := s.server.Node().NodeID()
	return &rpcpb.NodeInfoResponse{Id: nodeId, Version: 1}, nil
}

func (s *APIService) GetBlockByHash(ctx context.Context, req *rpcpb.GetBlockByHashRequest) (*rpcpb.BlockResponse, error) {
	bhash := common.HexToHash(req.Hash)
	b := s.server.Core().Chain().GetBlockByHash(bhash)
	return blockResponse(b)
}

func (s *APIService) GetBlockByHeight(ctx context.Context, req *rpcpb.GetBlockByHeightRequest) (*rpcpb.BlockResponse, error) {
	b := s.server.Core().Chain().GetBlockByNumber(req.Height)
	return blockResponse(b)
}

func (s *APIService) GetLastBlock(ctx context.Context, req *rpcpb.GetLastBlockRequest) (*rpcpb.GetLastBlockResponse, error) {
	b := s.server.Core().Chain().LastBlock()
	return lastBlockResponse(b)
}

func (s *APIService) GetTxByHash(ctx context.Context, req *rpcpb.GetTxByHashRequest) (*rpcpb.TransactionResponse, error) {
	txHash := common.HexToHash(req.Hash)
	tx := s.server.Core().Chain().GetTxByHash(txHash)
	return txResponse(tx)
}

func (s *APIService) GetAccountState(ctx context.Context, req *rpcpb.GetAccountStateRequest) (*rpcpb.GetAccountStateResponse, error) {
	addr, err := address.AddressParse(req.Address)
	if err != nil {
		return nil, err
	}
	account := s.server.Core().Chain().LastBlock().GetAccount(*addr.CommonAddress())
	return accountStateResponse(account)
}

func blockResponse(b *core.Block) (*rpcpb.BlockResponse, error) {
	if b == nil {
		return nil, errors.New("block not found")
	}
	return &rpcpb.BlockResponse{
		Hash:       b.Hash().Hex(),
		ParentHash: b.ParentHash().Hex(),
		Height:     b.Number(),
		Timestamp:  b.Time(),
		ChainId:    b.ChainID(),

		ConsensusRoot: b.ConsensusRoot().Hex(),
		StateRoot:     b.StateRoot().Hex(),
		TxsRoot:       b.TxsRoot().Hex(),
		ReceiptsRoot:  b.ReceiptsRoot().Hex(),
	}, nil
}

func lastBlockResponse(b *core.Block) (*rpcpb.GetLastBlockResponse, error) {
	br, err := blockResponse(b)
	if err != nil {
		return nil, err
	}
	return &rpcpb.GetLastBlockResponse{
		Hash:  b.Hash().Hex(),
		Block: br,
	}, nil
}

func txResponse(tx *core.Transaction) (*rpcpb.TransactionResponse, error) {
	if tx == nil {
		return nil, errors.New("tx not found")
	}
	return &rpcpb.TransactionResponse{
		Hash:      tx.Hash().Hex(),
		Nonce:     tx.Nonce(),
		From:      tx.From().Hex(),
		Recipient: tx.Recipient().Hex(),
		Amount:    tx.Amount().String(),
	}, nil
}

func accountStateResponse(account state.Account) (*rpcpb.GetAccountStateResponse, error) {
	if account == nil {
		return nil, errors.New("account not found")
	}
	return &rpcpb.GetAccountStateResponse{
		Address: account.Address().String(),
		Nonce:   account.Nonce(),
		Balance: account.Balance().String(),
	}, nil
}
