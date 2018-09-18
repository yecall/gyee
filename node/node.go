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

package node

/*
   节点主要功能
   第一次启动的话，初始化node：生成node id，
   启停p2p
   启停core，控制core的状态
   启停rpc接口，http json api
   启停console ipc接口， js解释器
   node的状态：启动，同步区块，各类服务状态，账户状态：候选，validator
1. 不同节点以datadir来区分，缺省./Library/YeeChain (Mac OS)
   datadir下存放：链数据，文件锁，keystore，node id，ipc通道文件等
2. 启动时，如果节点还没有创建，创建一个node id，
3. 启动core，core加载本地数据，如果本地没有数据，创建创始块
4. 启动p2p，进入区块同步状态
5. 启动rpc， ipc接口

*/

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/theckman/go-flock"
	"github.com/yeeco/gyee/accounts"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/p2p"
	grpc "github.com/yeeco/gyee/rpc"
	"github.com/yeeco/gyee/utils/logging"
)

type Node struct {
	name           string //for test purpose
	config         *config.Config
	core           *core.Core
	accountManager *accounts.AccountManager
	p2p            p2p.Service
	rpc            grpc.RPCServer

	lock        sync.RWMutex
	filelock    *flock.Flock
	stop        chan struct{}
	ipcEndpoint string
}

func NewNode(conf *config.Config) (*Node, error) {
	logging.Logger.Info("Create new node")
	if conf.NodeDir != "" {
		absdatadir, err := filepath.Abs(conf.NodeDir)
		if err != nil {
			logging.Logger.Panic(err)
		}
		conf.NodeDir = absdatadir
	}

	node := &Node{
		config: conf,
	}

	am, err := accounts.NewAccountManager(conf)
	if err != nil {
		logging.Logger.Panic(err)
	}
	node.accountManager = am

	p2p, err := p2p.NewInmemService()
	if err != nil {
		logging.Logger.Panic(err)
	}
	node.p2p = p2p

	core, err := core.NewCore(node, conf)
	if err != nil {
		logging.Logger.Panic(err)
	}
	node.core = core

	node.stop = make(chan struct{})
	return node, nil
}

func (n *Node) Start() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	logging.Logger.Info("Node Start...")

	if err := n.lockDataDir(); err != nil {
		logging.Logger.Fatal(err)
	}

	//依次启动p2p，rpc, ipc, blockchain, sync service, consensus
	n.p2p.Start()
	n.core.Start()

	n.startIPC()

	return nil
}

func (n *Node) Stop() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	logging.Logger.Info("Node Stop...")
	n.core.Stop()
	n.p2p.Stop()

	if err := n.unlockDataDir(); err != nil {
		logging.Logger.Println(err)
		return err
	}
	close(n.stop)
	return nil
}

func (n *Node) WaitForShutdown() {
	logging.Logger.Info("Node Wait for shutdown...")
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		logging.Logger.Info("Got interrupt, shutting down...")
		n.Stop()
	}()

	<-n.stop
	return
}

func (n *Node) lockDataDir() error {
	instDir := filepath.Join(n.config.NodeDir, n.config.Name)
	if err := os.MkdirAll(instDir, 0700); err != nil {
		return err
	}

	filelock := flock.NewFlock(filepath.Join(instDir, "LOCK"))
	locked, err := filelock.TryLock()
	if err != nil {
		logging.Logger.Println(err)
	}
	if locked {
		logging.Logger.Println("filelock locked")
	} else {
		logging.Logger.Println("filelock can not lock")
		return errors.New("filelock can not lock")
	}
	return nil
}

func (n *Node) unlockDataDir() error {
	if n.filelock != nil {
		if err := n.filelock.Unlock(); err != nil {
			logging.Logger.Println(err)
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
	os.Remove(endpoint) //TODO:?为什么？确定目录在，但把文件删掉
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		logging.Logger.Println(err)
		return err
	}

	os.Chmod(endpoint, 0600)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				logging.Logger.Println(err)
				continue
			}
			go handler.ServeConn(conn)
		}
	}()

	return nil
}

//get the node id of self
func (n *Node) NodeID() string {
	return "aaaa"
}

func (n *Node) NodeName() string {
	return n.name
}

func (n *Node) Config() *config.Config {
	return n.config
}

func (n *Node) AccountManager() *accounts.AccountManager {
	return n.accountManager
}

func (n *Node) P2pService() p2p.Service {
	return n.p2p
}

//TODO:for test, remove later
type JSService int
type Args struct {
	S string
}

func (js *JSService) Hello(args *Args, reply *string) error {
	logging.Logger.Println(args.S)
	*reply = args.S + "how are you"
	return nil
}
