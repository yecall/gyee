/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  the gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  the gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package node

import (
	"errors"
	"github.com/theckman/go-flock"
	"github.com/yeeco/gyee/config"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

type Node struct {
	config      *config.Config
	lock        sync.RWMutex
	filelock    *flock.Flock
	stop        chan struct{}
	ipcEndpoint string
}

func New(conf *config.Config) (*Node, error) {
	if conf.DataDir != "" {
		absdatadir, err := filepath.Abs(conf.DataDir)
		if err != nil {
			log.Panic(err)
			return nil, err
		}
		conf.DataDir = absdatadir
	}

	return &Node{
		config: conf,
	}, nil
}

func (n *Node) Start() error {
	n.lock.Lock()
	if err := n.lockDataDir(); err != nil {
		log.Println(err)
		return err
	}

	n.startIPC()

	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		log.Println("Got interrupt, shutting down...")
		go n.Stop()
	}()

	n.stop = make(chan struct{})
	n.lock.Unlock()
	<-n.stop
	return nil
}

func (n *Node) Stop() error {
	n.lock.Lock()
	defer n.lock.Unlock()

	if err := n.unlockDataDir(); err != nil {
		log.Println(err)
	}
	close(n.stop)
	return nil
}

func (n *Node) lockDataDir() error {
	instDir := filepath.Join(n.config.DataDir, n.config.Name)
	if err := os.MkdirAll(instDir, 0700); err != nil {
		return err
	}

	filelock := flock.NewFlock(filepath.Join(instDir, "LOCK"))
	locked, err := filelock.TryLock()
	if err != nil {
		log.Println(err)
	}
	if locked {
		log.Println("locked")
	} else {
		log.Println("can not lock")
		return errors.New("can not lock")
	}
	return nil
}

func (n *Node) unlockDataDir() error {
	if n.filelock != nil {
		if err := n.filelock.Unlock(); err != nil {
			log.Println(err)
		}
		n.filelock = nil
	}
	return nil
}

func (n *Node) startIPC() error {
	if n.config.IPCEndpoint() == "" {
		return nil
	}

	handler := rpc.NewServer()
	jsService := new(JSService)
	handler.Register(jsService)

	endpoint := n.config.IPCEndpoint()
	if err := os.MkdirAll(filepath.Dir(endpoint), 0751); err != nil {
		return err
	}
	os.Remove(endpoint)
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		log.Println(err)
		return err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			go handler.ServeConn(conn)
		}
	}()

	return nil
}

//TODO:for test, remove later
type JSService int
type Args struct {
	S string
}

func (js *JSService) Hello(args *Args, reply *string) error {
	log.Println(args.S)
	*reply = args.S + "how are you"
	return nil
}
