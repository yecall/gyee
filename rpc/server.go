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
	"github.com/yeeco/gyee/rpc/pb"
	"google.golang.org/grpc"
)

//rpc include services module such as admin, api, etc. and listen&accept on IPC, tcp, http-json with the different access right.
//Admin service can only access via IPC on console, admin can config the access right of other services.
//All the service function related to other YeeChain modules, using Yeelet to organize.



type Server struct {
	rpcServer *grpc.Server
}

func NewServer() *Server {
	rpc := grpc.NewServer()
	srv := &Server{rpcServer: rpc}
	admin := &AdminService{server: srv}
	api := &APIService{server: srv}
	rpcpb.RegisterAdminServiceServer(rpc, admin)
	rpcpb.RegisterApiServiceServer(rpc, api)

	return srv
}

func (s *Server) Start() error {

	return nil
}

func (s *Server) Stop() {

}
