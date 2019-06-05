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
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/gofrs/flock"
	"github.com/yeeco/gyee/accounts"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/core"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/rpc"
)

type Node struct {
	name           string //for test purpose
	config         *config.Config
	core           *core.Core
	accountManager *accounts.AccountManager
	p2p            p2p.Service
	ipc            rpc.RPCServer
	rpc            rpc.RPCServer

	lock        sync.RWMutex
	filelock    *flock.Flock
	stop        chan struct{}
	ipcEndpoint string
}

func NewNode(conf *config.Config) (*Node, error) {
	return NewNodeWithGenesis(conf, nil, nil)
}

func NewNodeWithGenesis(conf *config.Config, genesis *core.Genesis, p2pSvc p2p.Service) (*Node, error) {
	log.Info("Create new node")
	if conf.NodeDir != "" {
		absdatadir, err := filepath.Abs(conf.NodeDir)
		if err != nil {
			log.Crit("node: config path: ", err)
		}
		conf.NodeDir = absdatadir
		conf.P2p.NodeDataDir = filepath.Join(absdatadir, "p2p")
	}
	err := os.MkdirAll(conf.NodeDir, 0755)
	if err != nil {
		return nil, err
	}

	node := &Node{
		config:   conf,
		filelock: flock.New(filepath.Join(conf.NodeDir, "LOCK")),
	}

	node.accountManager, err = accounts.NewAccountManager(conf)
	if err != nil {
		log.Crit("node: accountMgr: ", err)
	}

	node.core, err = core.NewCoreWithGenesis(node, conf, genesis)
	if err != nil {
		log.Crit("node: core: ", err)
	}

	if p2pSvc == nil {
		if p2pSvc, err = p2p.NewOsnServiceWithCfg(conf); err != nil {
			log.Crit("node: p2p: ", err)
		}
	}
	node.p2p = p2pSvc

	node.stop = make(chan struct{})
	return node, nil
}

func (n *Node) Start() (err error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Info("Node Start...")

	if err = n.lockDataDir(); err != nil {
		log.Error("node: lockDataDir(): ", err)
		return err
	}

	//依次启动p2p，rpc, ipc, blockchain, sync service, consensus
	if err = n.core.Start(); err != nil {
		return err
	}
	log.Info("Node Started")

	if err = n.p2p.Start(); err != nil {
		return err
	}
	log.Info("p2p Started")

	if err = n.startIPC(); err != nil {
		return err
	}
	log.Info("IPC Started")

	if err = n.startRPC(); err != nil {
		return err
	}
	log.Info("RPC Started")

	return nil
}

func (n *Node) Stop() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Info("Node Stop...")

	n.p2p.Stop()
	if err := n.core.Stop(); err != nil {
		return err
	}

	if err := n.unlockDataDir(); err != nil {
		log.Error("node: unlockDataDir():", err)
		return err
	}
	close(n.stop)
	return nil
}

func (n *Node) WaitForShutdown() {
	log.Info("Node Wait for shutdown...")
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc

		log.Info("Got interrupt, shutting down...")
		if err := n.Stop(); err != nil {
			log.Error("node: Stop(): ", err)
		}
	}()

	<-n.stop
	return
}

func (n *Node) lockDataDir() error {
	locked, err := n.filelock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("node: failed to acquire node file lock")
	}
	return nil
}

func (n *Node) unlockDataDir() error {
	if n.filelock != nil {
		if err := n.filelock.Unlock(); err != nil {
			return err
		}
		n.filelock = nil
	}
	return nil
}

func (n *Node) startIPC() error {
	if n.config.IPCEndpoint() == "" {
		return nil
	}

	endpoint := n.config.IPCEndpoint()
	listener, err := ipcListen(endpoint)
	if err != nil {
		return err
	}

	n.ipc = rpc.NewServer(n.config, n)

	go func() {
		if err := n.ipc.Serve(listener); err != nil {
			log.Error("IPC exited", "err", err)
		}
	}()

	return nil
}

func (n *Node) startRPC() error {
	// TODO: remove hardcoded listen param after connection security handled
	rpcListen := "127.0.0.1:7353"

	listener, err := net.Listen("tcp", rpcListen)
	if err != nil {
		return err
	}

	n.rpc = rpc.NewServer(n.config, n)
	go func() {
		if err := n.rpc.Serve(listener); err != nil {
			log.Error("RPC exited", err)
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

func (n *Node) Core() *core.Core {
	return n.core
}

func (n *Node) P2pService() p2p.Service {
	return n.p2p
}
