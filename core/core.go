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

package core

/*
   blockchain的主要内容
   创世块
   数据同步
   交易验证
   block验证，blockchain维护
   db管理
   state？
   VM

1. 创建的时候，如果本地没有数据，先创建创世块
2. 启动先进入同步状态，同步区块高度
3. 进入到正常状态后，收取区块数据及验证
4. 如果开启了挖矿：
   如果进入到candidate状态，需要同步上一个state及之后所有的区块内容。
   如果进入到validator状态，开启tetris
5.

*/
import (
	"path/filepath"
	"sync"

	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/consensus"
	"github.com/yeeco/gyee/consensus/tetris2"
	"github.com/yeeco/gyee/core/yvm"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/persistent"
)

type Core struct {
	node           INode
	config         *config.Config
	engine         consensus.Engine
	engineOutputCh chan *consensus.Output
	storage        persistent.Storage
	blockChain     *BlockChain
	blockPool      *BlockPool
	yvm            yvm.YVM
	subscriber     *p2p.Subscriber

	lock    sync.RWMutex
	running bool
	quitCh  chan struct{}
	wg      sync.WaitGroup
}

func NewCore(node INode, conf *config.Config) (*Core, error) {
	log.Info("Create new core")

	// prepare chain db
	dbPath := filepath.Join(conf.NodeDir, "chaindata")
	storage, err := persistent.NewLevelStorage(dbPath)
	if err != nil {
		return nil, err
	}

	core := &Core{
		node:    node,
		config:  conf,
		storage: storage,
		quitCh:  make(chan struct{}),
	}
	core.blockChain, err = NewBlockChainWithCore(core)
	if err != nil {
		return nil, err
	}
	core.blockPool, err = NewBlockPool(core)
	if err != nil {
		return nil, err
	}

	return core, nil
}

func (c *Core) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.running {
		return nil
	}
	log.Info("Core Start...")

	c.blockPool.Start()

	//如果开启挖矿
	if c.config.Chain.Mine {
		members := c.blockChain.GetValidators()
		blockHeight := c.blockChain.CurrentBlockHeight()
		mid := c.node.NodeID()
		// TODO: vid?mid?
		tetris, err := tetris2.NewTetris(c, mid, members, blockHeight)
		if err != nil {
			return err
		}
		c.engine = tetris
		c.engineOutputCh = tetris.Output()
		if err := c.engine.Start(); err != nil {
			return err
		}

		c.subscriber = p2p.NewSubscriber(c, make(chan p2p.Message), p2p.MessageTypeEvent)
		p2p := c.node.P2pService()
		p2p.Register(c.subscriber)
	}

	go c.loop()

	c.running = true
	return nil
}

func (c *Core) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.running {
		return nil
	}
	log.Info("Core Stop...")

	// unsubscribe from p2p net
	c.node.P2pService().UnRegister(c.subscriber)

	// stop block pool and wait
	c.blockPool.Stop()

	// stop chain also wait for cache flush
	c.blockChain.Stop()
	if err := c.storage.Close(); err != nil {
		log.Error("core: storage.Close():", err)
	}

	// stop tetris
	err := c.engine.Stop()

	// notify loop and wait
	close(c.quitCh)
	c.wg.Wait()
	return err
}

func (c *Core) loop() {
	c.wg.Add(1)
	defer c.wg.Done()
	log.Info("Core loop...")
	for {
		select {
		case <-c.quitCh:
			log.Info("Core loop end.")
			return
		case output := <-c.engineOutputCh:
			log.Info("core receive engine output", "output", output)
			// TODO: build block
		case msg := <-c.subscriber.MsgChan:
			log.Info("core receive ", msg.MsgType, " ", msg.From)
			// TODO: send to consensus
		}

	}
}

// implements of interface

//ICORE
func (c *Core) GetSigner() crypto.Signer {
	return secp256k1.NewSecp256k1Signer()
}

func (c *Core) GetPrivateKeyOfDefaultAccount() ([]byte, error) { //从node的accountManager取
	return nil, nil
}

func (c *Core) AddressFromPublicKey(publicKey []byte) ([]byte, error) {
	ad, err := NewAddressFromPublicKey(publicKey)
	if err != nil {
		log.Warn("New address from public key error", err)
		return nil, err
	}
	return ad.address, nil
}
