// Copyright (C) 2019 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

// +build windows

package node

import (
	"context"
	"net"
	"time"

	"github.com/Microsoft/go-winio"
)

func ipcListen(endpoint string) (net.Listener, error) {
	l, err := winio.ListenPipe(endpoint, nil)
	if err != nil {
		return  nil, err
	}

	return l, nil
}

func NewIPCConn(ctx context.Context, endpoint string) (conn net.Conn, e error) {
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = deadline.Sub(time.Now())
		if timeout < 0 {
			timeout = 0
		}
	}
	return winio.DialPipe(endpoint, &timeout)
}
