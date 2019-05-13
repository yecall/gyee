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

/*
 overlay sub-network supported p2p network
*/

package p2p

import (
	"time"

	"github.com/pkg/errors"
	yeeCfg "github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/p2p/config"
	"github.com/yeeco/gyee/p2p/shell"
	yeelog "github.com/yeeco/gyee/utils/logging"
)

type OsnService struct {
	yeShCfg YeShellConfig
	yeShMgr Service
}

func OsnServiceConfig(cfg *YeShellConfig, cfgFromFie interface{}) error {
	//
	// 在本函数进行服务参数配置。一般而言，可先从配置文件中得到P2P的各项配置，具体的结构由应用决定并以
	// "cfgFromFile"为参数；其次取P2P中缺省的配置DefaultYeShellConfig之后进行适当的修改，以参数
	// "cfg"指定，调用本函数。后面是对目标配置中各个参数的说明，本函数由应用根据具体情况（cfgFromFie
	// 的结构设计）实现。完成对目标配置的设置之后，即可调用NewOsnService生成一个服务实例。
	//
	// 名称					类型					描述
	//-------------------------------------------------------------------------------------
	// AppType				config.P2pAppType	P2P包含两大部分，peer和dht，是可以单独
	//											使用的。而对我们项目的具体应用，是必须两个
	//											一起使用；
	//
	// Name					string				本次运行的P2P实例，即由函数NewOsnService
	//											生成的对象的名称；
	//
	// Validator			bool				是否为验证器；
	//
	// BootstrapNode		bool				是否为bootstrap节点，目前peer和dht的bootstrap
	//											节点身份不加区分，即一个节点要么同时是这两部分的
	//											bootstrap节点，要么都不是；
	//
	// BootstrapNodes		[]string			peer部分的bootstrap节点列表；
	//
	// DhtBootstrapNodes	[]string			dht部分的bootstrap节点列表；
	//
	// LocalNodeIp			string				本地peer部分的IP地址
	//
	// LocalUdpPort			uint16				本地peer部分的UDP端口
	//
	// LocalTcpPort			uint16				本地peer部分的TCP端口
	//
	// LocalDhtIp			string				本地dht部分的IP地址
	//
	// LocalDhtPort			uint16				本地dht部分的TCP端口
	//
	// NodeDataDir			string				本次实例的数据目录；
	//
	// NodeDatabase			string				本次实例的leveldb数据库名称（数据库所在目录）；
	//
	// SubNetMaskBits		int					子网所使用的掩码的比特数，0-15；
	//
	// EvKeepTime			time.Duration		event在dht中保留的时长；
	//
	// DedupTime			time.Duration		去重时钟管理器进行清理的周期时长；
	//
	// BootstrapTime		time.Duration		dht盲连接周期。在本实例不是bootstrap节点的情况下，
	//											周期性的从BootstrapNodes中随机挑选节点进行连接，
	//											连接成功之后，该时钟停止。
	//
	// NatType				string				nat类型，目前支持三种："none", "pmp", "upnp"
	//
	// GatewayIp			string				当nat类型配置为"pmp"的时候相应的网关IP地址
	//
	// 注：如前所述，本函数应由应用根据具体情况（cfgFromFie的结构设计）实现并调用，但这不是必须的，应用
	// 可以用任何方法构造合理的YeShellConfig结构，然后调用NewOsnService得到服务实例。
	//

	// TODO: 对参数进行检查
	yc, ok := cfgFromFie.(*yeeCfg.Config)
	if !ok {
		panic("OsnServiceConfig: invalid configuration type")
	}

	p2p := yc.P2p
	if p2p.AppType != int(config.P2P_TYPE_ALL) {
		panic("OsnServiceConfig: must be P2P_TYPE_ALL")
	}
	cfg.AppType = config.P2pAppType(p2p.AppType)

	if len(p2p.Name) != 0 {
		cfg.Name = p2p.Name
	} else {
		yeelog.Logger.Infof("OsnServiceConfig: default Name: %s", cfg.Name)
	}

	cfg.Validator = p2p.Validator
	cfg.BootstrapNode = p2p.BootstrapNode
	cfg.BootstrapNodes = make([]string, 0)
	cfg.BootstrapNodes = append(cfg.BootstrapNodes, p2p.BootstrapNodes...)
	cfg.DhtBootstrapNodes = make([]string, 0)
	cfg.DhtBootstrapNodes = append(cfg.DhtBootstrapNodes, p2p.DhtBootstrapNodes...)

	if len(p2p.LocalNodeIp) > 0 {
		cfg.LocalNodeIp = p2p.LocalNodeIp
	}

	if p2p.LocalUdpPort > 0 {
		cfg.LocalUdpPort = p2p.LocalUdpPort
	}

	if p2p.LocalTcpPort > 0 {
		cfg.LocalTcpPort = p2p.LocalTcpPort
	}

	if len(p2p.LocalDhtIp) > 0 {
		cfg.LocalDhtIp = p2p.LocalDhtIp
	}

	if p2p.LocalDhtPort > 0 {
		cfg.LocalDhtPort = p2p.LocalDhtPort
	}

	if len(p2p.NodeDataDir) == 0 {
		yeelog.Logger.Infof("OsnServiceConfig: default NodeDataDir: %s", cfg.NodeDataDir)
	} else {
		cfg.NodeDataDir = p2p.NodeDataDir
	}

	if len(p2p.NodeDatabase) == 0 {
		yeelog.Logger.Infof("OsnServiceConfig: default NodeDatabase: %s", cfg.NodeDatabase)
	} else {
		cfg.NodeDatabase = p2p.NodeDatabase
	}

	yeelog.Logger.Infof("OsnServiceConfig: node[%s:%d:%d]", cfg.LocalNodeIp, cfg.LocalUdpPort, cfg.LocalTcpPort)
	yeelog.Logger.Infof("OsnServiceConfig: dht[%s:%d]", cfg.LocalDhtIp, cfg.LocalDhtPort)

	cfg.SubNetMaskBits = p2p.SubNetMaskBits
	if cfg.SubNetMaskBits < 0 {
		cfg.SubNetMaskBits = 0
	}

	factor := int64(time.Second / time.Nanosecond)
	if p2p.EvKeepTime <= 0 {
		yeelog.Logger.Infof("OsnServiceConfig: default EvKeepTime: %d(s)", int64(cfg.EvKeepTime)/factor)
	} else {
		cfg.EvKeepTime = time.Duration(int64(p2p.EvKeepTime) * factor)
	}

	if p2p.DedupTime <= 0 {
		yeelog.Logger.Infof("OsnServiceConfig: default DedupTime: %d(s)", int64(cfg.DedupTime)/factor)
	} else {
		cfg.DedupTime = time.Duration(int64(p2p.DedupTime) * factor)
	}

	if p2p.BootstrapTime <= 0 {
		yeelog.Logger.Infof("OsnServiceConfig: default BootstrapTime: %d(s)", int64(cfg.BootstrapTime)/factor)
	} else {
		cfg.BootstrapTime = time.Duration(int64(p2p.BootstrapTime) * factor)
	}

	cfg.NatType = p2p.NatType
	cfg.GatewayIp = p2p.GatewayIp

	return nil
}

func NewOsnServiceWithCfg(cfg *yeeCfg.Config) (*OsnService, error) {
	yeShellCfg := DefaultYeShellConfig
	if err := OsnServiceConfig(&yeShellCfg, cfg); err != nil {
		return nil, err
	}
	return NewOsnService(&yeShellCfg)
}

func NewOsnService(cfg *YeShellConfig) (*OsnService, error) {
	osns := OsnService{
		yeShCfg: *cfg,
	}
	config.SwitchConfigDebugFlag(cfg.BootstrapNode)
	shell.SwitchStaticDebugFlag(cfg.BootstrapNode)
	if osns.yeShMgr = NewYeShellManager(&osns.yeShCfg); osns.yeShMgr == nil {
		return nil, errors.New("NewOsnService: NewYeShellManager failed")
	}
	return &osns, nil
}

func (osns *OsnService) Start() error {
	return osns.yeShMgr.Start()
}

func (osns *OsnService) Stop() {
	osns.yeShMgr.Stop()
}

func (osns *OsnService) Reconfig(reCfg *RecfgCommand) error {
	return osns.yeShMgr.Reconfig(reCfg)
}

func (osns *OsnService) BroadcastMessage(message Message) error {
	return osns.yeShMgr.BroadcastMessage(message)
}

func (osns *OsnService) BroadcastMessageOsn(message Message) error {
	return osns.yeShMgr.BroadcastMessageOsn(message)
}

func (osns *OsnService) Register(subscriber *Subscriber) {
	osns.yeShMgr.Register(subscriber)
}

func (osns *OsnService) UnRegister(subscriber *Subscriber) {
	osns.yeShMgr.UnRegister(subscriber)
}

func (osns *OsnService) DhtGetValue(key []byte) ([]byte, error) {
	return osns.yeShMgr.DhtGetValue(key)
}

func (osns *OsnService) DhtGetValues(keys [][]byte, out chan<- []byte) error {
	return osns.yeShMgr.DhtGetValues(keys, out)
}

func (osns *OsnService) DhtSetValue(key []byte, value []byte) error {
	return osns.yeShMgr.DhtSetValue(key, value)
}

func (osns *OsnService) RegChainProvider(cp ChainProvider) {
	osns.yeShMgr.RegChainProvider(cp)
}

func (osns *OsnService) GetChainInfo(kind string, key []byte) ([]byte, error) {
	return osns.yeShMgr.GetChainInfo(kind, key)
}

func (osns *OsnService) GetLocalNode() *config.Node {
	return osns.yeShMgr.(*YeShellManager).GetLocalNode()
}

func (osns *OsnService) GetLocalDhtNode() *config.Node {
	return osns.yeShMgr.(*YeShellManager).GetLocalDhtNode()
}
