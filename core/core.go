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
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yeeco/gyee/common"
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
	node    INode
	config  *config.Config
	engine  consensus.Engine
	storage persistent.Storage

	blockChain *BlockChain
	blockPool  *BlockPool
	txPool     *TransactionPool

	yvm        yvm.YVM
	subscriber *p2p.Subscriber
	subsChan   chan p2p.Message

	// miner
	keystore  *keystore.Keystore
	minerKey  []byte
	minerAddr *address.Address

	metrics *coreMetrics

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
		metrics: newCoreMetrics(),
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
	c.node.P2pService().RegChainProvider(c)

	//如果开启挖矿
	if c.config.Chain.Mine {
		if err := c.prepareCoinbase(); err != nil {
			return err
		}

		members := c.blockChain.GetValidators()
		blockHeight := c.blockChain.CurrentBlockHeight()
		tetris, err := tetris2.NewTetris(c, c.minerAddr.String(), members, blockHeight)
		if err != nil {
			return err
		}
		c.engine = tetris
		if err := c.engine.Start(); err != nil {
			return err
		}

		c.subscriber = p2p.NewSubscriber(c, make(chan p2p.Message), p2p.MessageTypeEvent)
		p2p := c.node.P2pService()
		p2p.Register(c.subscriber)
		c.subsChan = c.subscriber.MsgChan
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

	// output metrics
	c.metrics.printMetrics()

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
	if c.engine != nil {
		if err := c.engine.Stop(); err != nil {
			log.Error("core: engine.Stop", "err", err)
		}
	}

	// notify loop and wait
	close(c.quitCh)
	c.wg.Wait()
	return nil
}

func (c *Core) loop() {
	c.wg.Add(1)
	defer c.wg.Done()
	defer func() { c.running = false }()

	log.Trace("Core loop...")
	for {
		var (
			chanEventSend <-chan []byte
			chanEventReq  <-chan common.Hash
			outputChan    <-chan *consensus.Output
		)
		if c.engine != nil {
			chanEventSend = c.engine.ChanEventSend()
			chanEventReq = c.engine.ChanEventReq()
			outputChan = c.engine.Output()
		}

		select {
		case <-c.quitCh:
			log.Info("Core loop end.")
			return
		case event := <-chanEventSend:
			log.Trace("engine send event")
			go c.handleEngineEventSend(event)
		case req := <-chanEventReq:
			log.Trace("engine req event", "hash", req)
			go c.handleEngineEventReq(req)
		case output := <-outputChan:
			log.Info("core receive engine output", "output", output)
			c.handleEngineOutput(output)
		case msg := <-c.subsChan:
			log.Trace("core receive ", msg.MsgType, " ", msg.From)
			c.metrics.p2pMsgRecv.Mark(1)
			switch msg.MsgType {
			case p2p.MessageTypeEvent:
				c.metrics.p2pMsgRecvEv.Mark(1)
				go c.engine.SendEvent(msg.Data)
			default:
				log.Crit("wrong msg", "msg", msg)
			}
		}
	}
}

func (c *Core) handleEngineEventSend(event []byte) {
	c.wg.Add(1)
	defer c.wg.Done()

	if !c.running {
		return
	}
	h := sha256.Sum256(event)
	c.metrics.p2pDhtSetMeter.Mark(1)
	err := c.node.P2pService().DhtSetValue(h[:], event)
	if err != nil {
		log.Warn("engine send event to dht failed", "err", err)
	}
	c.metrics.p2pMsgSent.Mark(1)
	err = c.node.P2pService().BroadcastMessage(p2p.Message{
		MsgType: p2p.MessageTypeEvent,
		From:    c.node.NodeID(),
		Data:    event,
	})
	if err != nil {
		log.Warn("engine send event failed", "err", err)
		c.metrics.p2pMsgSendFail.Mark(1)
	}
}

func (c *Core) handleEngineEventReq(hash common.Hash) {
	c.wg.Add(1)
	defer c.wg.Done()
	retry := 60
	for {
		if !c.running {
			return
		}

		c.metrics.p2pDhtGetMeter.Mark(1)
		data, err := c.node.P2pService().DhtGetValue(hash[:])
		if err == nil {
			c.metrics.p2pDhtHitMeter.Mark(1)
			c.engine.SendParentEvent(data)
			return
		}
		c.metrics.p2pDhtMissMeter.Mark(1)
		retry--
		if retry <= 0 {
			log.Warn("engine req event failed", "hash", hash, "err", err)
			break
		}
		time.Sleep(time.Second)
	}
}

func (c *Core) handleEngineOutput(o *consensus.Output) {
	currentHeight := c.blockChain.CurrentBlockHeight()
	if currentHeight >= o.H {
		log.Warn("engine height lower than blockchain", "engineH", o.H, "chainH", currentHeight)
		// TODO: block already in chain, check if output matches with block
		return
	}
	if currentHeight+TooFarBlocks < o.H {
		log.Warn("engine height too high", "engineH", o.H, "chainH", currentHeight)
		// TODO: block chain too far behind, should reSync
	}
	go func() {
		c.wg.Add(1)
		defer c.wg.Done()

		txs := make(Transactions, 0, len(o.Txs))
		for _, hash := range o.Txs {
			c.metrics.p2pDhtGetMeter.Mark(1)
			enc, err := c.node.P2pService().DhtGetValue(hash[:])
			if err != nil {
				log.Error("failed to get tx", "hash", hash, "err", err)
				c.metrics.p2pDhtMissMeter.Mark(1)
				return
			}
			c.metrics.p2pDhtHitMeter.Mark(1)
			tx := &Transaction{}
			if err := tx.Decode(enc); err != nil {
				log.Error("failed to decode tx", "hash", hash, "err", err)
				return
			}
			if err := tx.VerifySig(); err != nil {
				log.Error("failed to verify tx", "hash", hash, "err", err)
				return
			}
			tx.raw = enc
			txs = append(txs, tx)
		}
		c.blockPool.AddSealRequest(o.H,
			uint64(o.T.UTC().UnixNano()/int64(time.Millisecond/time.Nanosecond)),
			txs)
	}()
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

func (c *Core) prepareCoinbase() error {
	if err := c.loadCoinbaseKey(); err != nil {
		return err
	}
	pub, err := secp256k1.GetPublicKey(c.minerKey)
	if err != nil {
		return err
	}
	c.minerAddr, err = address.NewAddressFromPublicKey(pub)
	if err != nil {
		return err
	}
	return nil
}

func (c *Core) Chain() *BlockChain {
	return c.blockChain
}

func (c *Core) MinerAddr() *address.Address {
	return c.minerAddr.Copy()
}

func (c *Core) TxBroadcast(tx *Transaction) error {
	return c.txPool.TxBroadcast(tx)
}

// as if msg was received from p2p module
func (c *Core) FakeP2pRecv(msg *p2p.Message) {
	c.txPool.subscriber.MsgChan <- *msg
}

func (c *Core) signBlock(b *Block) error {
	if c.engine == nil {
		log.Crit("not in miner mode")
	}
	signer, err := c.GetMinerSigner()
	if err != nil {
		return err
	}
	return b.Sign(signer)
}

// implements of interface

//ICORE
func (c *Core) GetSigner() crypto.Signer {
	signer := secp256k1.NewSecp256k1Signer()
	return signer
}

func (c *Core) GetMinerSigner() (crypto.Signer, error) {
	key, err := c.GetPrivateKeyOfDefaultAccount()
	if err != nil {
		log.Warn("failed to get miner key", "err", err)
		return nil, err
	}
	signer := c.GetSigner()
	if err := signer.InitSigner(key); err != nil {
		log.Warn("failed to init signer", "err", err)
		return nil, err
	}
	return signer, nil
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

func getSigner(algorithm crypto.Algorithm) crypto.Signer {
	switch algorithm {
	case crypto.ALG_SECP256K1:
		return secp256k1.NewSecp256k1Signer()
	default:
		log.Warn("wrong crypto algorithm", "algorithm", algorithm)
		return nil
	}
}

func (c *Core) GetChainData(kind string, key []byte) []byte {
	c.metrics.p2pChainInfoAnswer.Mark(1)
	switch kind {
	case ChainDataTypeLatestH:
		return c.blockChain.LastBlock().Hash().Bytes()
	case ChainDataTypeLatestN:
		return new(big.Int).SetUint64(c.blockChain.CurrentBlockHeight()).Bytes()
	case ChainDataTypeBlockH:
		b := c.blockPool.GetBlockByHash(common.BytesToHash(key))
		if b != nil {
			enc, err := b.ToBytes()
			if err != nil {
				log.Warn("block encode failed", "blk", b, "err", err)
			}
			return enc
		}
	case ChainDataTypeBlockN:
		n := new(big.Int).SetBytes(key).Uint64()
		b := c.blockPool.GetBlockByNumber(n)
		if b != nil {
			enc, err := b.ToBytes()
			if err != nil {
				log.Warn("block encode failed", "blk", b, "err", err)
			}
			return enc
		}
	}
	return nil
}

func (c *Core) GetRemoteLatestHash() (*common.Hash, error) {
	c.metrics.p2pChainInfoGet.Mark(1)
	encoded, err := c.node.P2pService().GetChainInfo(ChainDataTypeLatestH, nil)
	if err != nil {
		return nil, err
	}
	if len(encoded) != common.HashLength {
		return nil, errors.New(fmt.Sprintf("latest hash length mismatch, got %v", encoded))
	}
	c.metrics.p2pChainInfoHit.Mark(1)
	return new(common.Hash).SetBytes(encoded), nil
}

func (c *Core) GetRemoteLatestNumber() (uint64, error) {
	c.metrics.p2pChainInfoGet.Mark(1)
	encoded, err := c.node.P2pService().GetChainInfo(ChainDataTypeLatestN, nil)
	if err != nil {
		return 0, err
	}
	c.metrics.p2pChainInfoHit.Mark(1)
	n := new(big.Int).SetBytes(encoded).Uint64()
	return n, nil
}

func (c *Core) GetRemoteBlockByHash(hash common.Hash) (*Block, error) {
	c.metrics.p2pChainInfoGet.Mark(1)
	encoded, err := c.node.P2pService().GetChainInfo(ChainDataTypeBlockH, hash[:])
	if err != nil {
		return nil, err
	}
	c.metrics.p2pChainInfoHit.Mark(1)
	return ParseBlock(encoded)
}

func (c *Core) GetRemoteBlockByNumber(n uint64) (*Block, error) {
	key := new(big.Int).SetUint64(n).Bytes()
	c.metrics.p2pChainInfoGet.Mark(1)
	encoded, err := c.node.P2pService().GetChainInfo(ChainDataTypeBlockN, key)
	if err != nil {
		return nil, err
	}
	c.metrics.p2pChainInfoHit.Mark(1)
	return ParseBlock(encoded)
}
