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
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/rpc/pb"
)

type APIService struct {
	server RPCServer
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
