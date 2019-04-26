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
	"net"
	"sync"

	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/rpc/pb"
	"google.golang.org/grpc"
)

//rpc include services module such as admin, api, etc. and listen&accept on IPC, tcp, http-json with the different access right.
//Admin service can only access via IPC on console, admin can config the access right of other services.
//All the service function related to other YeeChain modules, using Yeelet to organize.

type Server struct {
	conf      *config.Config
	node      core.INode
	core      *core.Core
	rpcServer *grpc.Server

	lock sync.RWMutex
}

func NewServer(conf *config.Config, node core.INode) *Server {
	rpc := grpc.NewServer()
	srv := &Server{
		conf:      conf,
		node:      node,
		core:      node.Core(),
		rpcServer: rpc,
	}
	admin := &AdminService{server: srv}
	api := &APIService{server: srv}
	rpcpb.RegisterAdminServiceServer(rpc, admin)
	rpcpb.RegisterApiServiceServer(rpc, api)

	return srv
}

func (s *Server) Node() core.INode {
	return s.node
}

func (s *Server) Core() *core.Core {
	return s.core
}

func (s *Server) Serve(lis net.Listener) error {
	return s.rpcServer.Serve(lis)
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Info("RPC start...")

	return nil
}

func (s *Server) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Info("RPC stop...")

	s.rpcServer.Stop()
}
