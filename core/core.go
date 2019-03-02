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
	"errors"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yeeco/gyee/common/address"
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/consensus"
	"github.com/yeeco/gyee/consensus/tetris2"
	"github.com/yeeco/gyee/core/yvm"
	"github.com/yeeco/gyee/crypto"
	"github.com/yeeco/gyee/crypto/keystore"
	"github.com/yeeco/gyee/crypto/secp256k1"
	"github.com/yeeco/gyee/log"
	"github.com/yeeco/gyee/p2p"
	"github.com/yeeco/gyee/persistent"
)

var (
	ErrNoCoinbase          = errors.New("coinbase not provided")
	ErrNoCoinbasePwdFile   = errors.New("coinbase keystore password file not provided")
	ErrCoinbaseKeyNotFound = errors.New("coinbase not found in keystore")
)

type Core struct {
	node           INode
	config         *config.Config
	engine         consensus.Engine
	engineOutputCh chan *consensus.Output
	storage        persistent.Storage
	blockChain     *BlockChain
	blockPool      *BlockPool
	txPool         *TransactionPool
	yvm            yvm.YVM
	subscriber     *p2p.Subscriber

	// miner
	keystore *keystore.Keystore
	minerKey []byte

	lock    sync.RWMutex
	running bool
	quitCh  chan struct{}
	wg      sync.WaitGroup
}

func NewCore(node INode, conf *config.Config) (*Core, error) {
	return NewCoreWithGenesis(node, conf, nil)
}

func NewCoreWithGenesis(node INode, conf *config.Config, genesis *Genesis) (*Core, error) {
	log.Info("Create new core")

	// prepare chain db
	dbPath := filepath.Join(conf.NodeDir, "chaindata")
	storage, err := persistent.NewLevelStorage(dbPath)
	if err != nil {
		return nil, err
	}

	// prepare storage with genesis
	// for unit tests only
	if genesis != nil {
		stateDB := GetStateDB(storage)
		if _, err := genesis.Commit(stateDB, storage); err != nil {
			return nil, err
		}
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
	core.txPool, err = NewTransactionPool(core)
	if err != nil {
		return nil, err
	}
	if conf.Chain.Mine {
		if err := core.loadCoinbaseKey(); err != nil {
			return nil, err
		}
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
	c.txPool.Start()

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

	// stop tx pool and wait
	c.txPool.Stop()

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
		var (
			outputChan = c.engineOutputCh
			msgChan    chan p2p.Message
		)

		if c.subscriber != nil {
			msgChan = c.subscriber.MsgChan
		}

		select {
		case <-c.quitCh:
			log.Info("Core loop end.")
			return
		case output := <-outputChan:
			log.Info("core receive engine output", "output", output)
			// TODO: build block
		case msg := <-msgChan:
			log.Info("core receive ", msg.MsgType, " ", msg.From)
			switch msg.MsgType {
			case p2p.MessageTypeEvent:
				c.engine.SendEvent(msg.Data)
			default:
				log.Crit("wrong msg", "msg", msg)
			}
		}

	}
}

func (c *Core) loadCoinbaseKey() error {
	conf := c.config

	if key := conf.Chain.Key; len(key) > 0 {
		// private key provided in config
		c.minerKey = key
		return nil
	}

	coinbase := conf.Chain.Coinbase
	if len(coinbase) == 0 {
		return ErrNoCoinbase
	}
	if len(conf.Chain.PwdFile) == 0 {
		return ErrNoCoinbasePwdFile
	}
	c.keystore = keystore.NewKeystoreWithConfig(conf)
	if contains, _ := c.keystore.Contains(coinbase); !contains {
		return ErrCoinbaseKeyNotFound
	}
	pwdContent, err := ioutil.ReadFile(conf.Chain.PwdFile)
	if err != nil {
		return err
	}
	pwd := []byte(strings.Split(string(pwdContent), "\n")[0])
	key, err := c.keystore.GetKey(coinbase, pwd)
	if err != nil {
		return err
	}
	c.minerKey = key
	return nil
}

// implements of interface

//ICORE
func (c *Core) GetSigner() crypto.Signer {
	signer := secp256k1.NewSecp256k1Signer()
	_ = signer.InitSigner(c.minerKey)
	return signer
}

func (c *Core) GetPrivateKeyOfDefaultAccount() ([]byte, error) {
	//从node的accountManager取
	if c.minerKey == nil {
		return nil, ErrCoinbaseKeyNotFound
	}
	return c.minerKey, nil
}

func (c *Core) AddressFromPublicKey(publicKey []byte) ([]byte, error) {
	ad, err := address.NewAddressFromPublicKey(publicKey)
	if err != nil {
		log.Warn("New address from public key error", err)
		return nil, err
	}
	return ad.Raw, nil
}
