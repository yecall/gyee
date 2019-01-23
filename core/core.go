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
	"github.com/yeeco/gyee/consensus/tetris"
	"github.com/yeeco/gyee/core/yvm"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/persistent"
	"github.com/yeeco/gyee/utils/logging"
)

type Core struct {
	node           INode
	config         *config.Config
	tetris         *tetris.Tetris
	tetrisOutputCh chan tetris.ConsensusOutput
	storage        persistent.Storage
	blockChain     *BlockChain
	yvm            yvm.YVM
	subscriber     *p2p.Subscriber

	lock   sync.RWMutex
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewCore(node INode, conf *config.Config) (*Core, error) {
	logging.Logger.Info("Create new core")

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

	return core, nil
}

func (c *Core) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	logging.Logger.Info("Core Start...")
	c.blockChain.Start()

	//如果开启挖矿
	if c.config.Chain.Mine {
		members := c.blockChain.GetValidators()
		blockHeight := c.blockChain.CurrentBlockHeight()
		mid := c.node.NodeID()
		tetris, err := tetris.NewTetris(c, members, blockHeight, mid)
		if err != nil {
			return err
		}
		c.tetris = tetris
		c.tetrisOutputCh = tetris.OutputCh
		c.tetris.Start()

		c.subscriber = p2p.NewSubscriber(c, make(chan p2p.Message), p2p.MessageTypeEvent)
		p2p := c.node.P2pService()
		p2p.Register(c.subscriber)
	}

	go c.loop()
	return nil
}

func (c *Core) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	logging.Logger.Info("Core Stop...")

	p2p := c.node.P2pService()
	p2p.UnRegister(c.subscriber)

	c.tetris.Stop()
	c.blockChain.Stop()
	//c.quitCh <- struct{}{}
	close(c.quitCh)
	c.wg.Wait()
	return nil
}

func (c *Core) loop() {
	c.wg.Add(1)
	defer c.wg.Done()
	logging.Logger.Info("Core loop...")
	for {
		select {
		case <-c.quitCh:
			logging.Logger.Info("Core loop end.")
			return
		case <-c.tetrisOutputCh:
		case msg := <-c.subscriber.MsgChan:
			logging.Logger.Info("core receive ", msg.MsgType, " ", msg.From)
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
		logging.Logger.Warn("New address from public key error", err)
		return nil, err
	}
	return ad.address, nil
}
