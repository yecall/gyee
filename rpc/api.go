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

	"github.com/yeeco/gyee/rpc/pb"
)

type APIService struct {
	server RPCServer
}

func (s *APIService) NodeInfo(ctx context.Context, req *rpcpb.NonParamsRequest) (*rpcpb.NodeInfoResponse, error) {
	nodeId := s.server.Node().NodeID()
	return &rpcpb.NodeInfoResponse{Id: nodeId, Version: 1}, nil
}
