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


package config

import (
	"strings"
	"strconv"
	"net"
	"crypto/ecdsa"
	"crypto/elliptic"
	"path/filepath"
	"os"
	"os/user"
	"runtime"
	"fmt"

	"crypto/rand"
	"encoding/hex"
	"io"
	"io/ioutil"
	"math/big"
	"errors"

	log "github.com/yeeco/gyee/p2p/logger"
	"time"
)


//
// errno
//
type P2pCfgErrno int

const (
	PcfgEnoNone			= iota
	PcfgEnoParameter
	PcfgEnoPublicKye
	PcfgEnoPrivateKye
	PcfgEnoDataDir
	PcfgEnoDatabase
	PcfgEnoIpAddr
	PcfgEnoNodeId
)

//
// Some paths
//
const (
	PcfgEnoIpAddrivateKey	= "nodekey"		// Path within the datadir to the node's private key
	datadirNodeDatabase		= "nodes"		// Path within the datadir to store the node infos
)

//
// Bootstrap nodes, in a format like:
//
//	node-identity-hex-string@ip:udp-port:tcp-port
//
const P2pMaxBootstrapNodes = 32

var BootstrapNodeUrl = []string {
	"4909CDF2A2C60BF1FE1E6BA849CC9297B06E00B54F0F8EB0F4B9A6AA688611FD7E43EDE402613761EC890AB46FE2218DC9B29FC47BE3AB8D1544B6C0559599AC@192.168.2.190:30303:30303",
	//"8FAEAD1F4B57494B6596B4320A9AA5B083486513222B8735E487A59F4C9CC59D2155B3668B1BAA34A7D07FC2F12384CE44EA50746BCA262B66B2F9E89F47D8E6@192.168.2.144:30303:30303",
}

//
// Build "Node" struct for bootstraps
//
var BootstrapNodes = P2pSetupDefaultBootstrapNodes()

//
// Node ID length in bits
//
const NodeIDBits	= 512
const NodeIDBytes	= NodeIDBits / 8

//
// Node identity
//
type NodeID [NodeIDBytes]byte

//
// Max protocols
//
const MaxProtocols = 32

//
// Max peers
//
const MaxPeers = 16

//
// Max concurrecny inboudn and outbound
//
const MaxInbounds	= MaxPeers / 2 // +2
const MaxOutbounds	= MaxPeers / 2 // +2



//
// Node
//

type SubNetworkID [2]byte					// sbu network identity
const MaxSubNetworks = 16					// max sub networks can a node attached to
const SubNetIdBytes = 2						// 2 bytes for sub network identity
var ZeroSubNet = SubNetworkID{0,0}			// zero sub network
var AnySubNet = SubNetworkID{0xff, 0xff}	// any sub network

type Node struct {
	IP				net.IP			// ip address
	UDP, TCP		uint16			// port numbers
	ID				NodeID			// the node's public key
}

type Protocol struct {
	Pid		uint32					// protocol identity
	Ver		[4]byte					// protocol version: M.m0.m1.m2
}

//
// Node static Configuration parameters
//

const (
	P2pNetworkTypeDynamic	= 0			// neighbor discovering needed
	P2pNetworkTypeStatic	= 1			// no discovering
)

type Config struct {

	//
	// Chain application part
	//

	CfgName				string					// configureation name
	Version				string					// p2p version
	Name				string					// node name
	PrivateKey			*ecdsa.PrivateKey		// node private key
	PublicKey			*ecdsa.PublicKey		// node public key
	NetworkType			int						// p2p network type
	BootstrapNodes		[]*Node					// bootstrap nodes
	StaticMaxPeers		int						// max peers would be
	StaticMaxOutbounds	int						// max concurrency outbounds
	StaticMaxInbounds	int						// max concurrency inbounds
	StaticNetId			SubNetworkID			// static network identity
	StaticNodes			[]*Node					// static nodes
	NodeDataDir			string					// node data directory
	NodeDatabase		string					// node database
	NoDial				bool					// outboundless flag
	NoAccept			bool					// inboundless flag
	BootstrapNode		bool					// bootstrap node flag
	Local				Node					// myself
	ProtoNum			uint32					// local protocol number
	Protocols			[]Protocol				// local protocol table
	SubNetMaxPeers		map[SubNetworkID]int	// max peers would be
	SubNetMaxOutbounds	map[SubNetworkID]int	// max concurrency outbounds
	SubNetMaxInBounds	map[SubNetworkID]int	// max concurrency inbounds
	SubNetIdList		[]SubNetworkID			// sub network identity list. do not put the identity
												// of the local node in this list.

	//
	// DHT application part
	//

	dhtRutCfg			Cfg4DhtRouteManager		// for dht route manager
}

//
// Configuration about neighbor manager on UDP
//
type Cfg4UdpNgbManager struct {
	IP				net.IP			// ip address
	UDP				uint16			// udp port numbers
	TCP				uint16			// tcp port numbers
	ID				NodeID			// the node's public key
	NetworkType		int				// network type
	SubNetIdList	[]SubNetworkID	// sub network identity list. do not put the identity
}

//
// Configuration about neighbor listener on UDP
//
type Cfg4UdpNgbListener struct {
	IP		net.IP		// ip address
	UDP		uint16		// udp port numbers
	TCP		uint16		// tcp port numbers
	ID		NodeID		// the node's public key
}

//
// Configuration about peer listener on TCP
//
type Cfg4PeerListener struct {
	IP			net.IP	// ip address
	Port		uint16	// port numbers
	ID			NodeID	// the node's public key
	MaxInBounds	int		// max concurrency inbounds
}

//
// Configuration about peer manager
//

type Cfg4PeerManager struct {
	CfgName				string					// p2p configuration name
	NetworkType			int						// p2p network type
	IP					net.IP					// ip address
	Port				uint16					// tcp port number
	UDP					uint16					// udp port number, used with handshake procedure
	ID					NodeID					// the node's public key
	StaticMaxPeers		int						// max peers would be
	StaticMaxOutbounds	int						// max concurrency outbounds
	StaticMaxInBounds	int						// max concurrency inbounds
	StaticNodes			[]*Node					// static nodes
	StaticNetId			SubNetworkID			// static network identity
	SubNetMaxPeers		map[SubNetworkID]int	// max peers would be
	SubNetMaxOutbounds	map[SubNetworkID]int	// max concurrency outbounds
	SubNetMaxInBounds	map[SubNetworkID]int	// max concurrency inbounds
	SubNetIdList		[]SubNetworkID			// sub network identity list. do not put the identity
												// of the local node in this list.
	NoDial				bool					// do not dial outbound
	NoAccept			bool					// do not accept inbound
	BootstrapNode		bool					// local is a bootstrap node
	ProtoNum			uint32					// local protocol number
	Protocols			[]Protocol				// local protocol table
}

//
// Configuration about table manager
//
type Cfg4TabManager struct {
	NetworkType		int				// Network type
	Local			Node			// local node
	BootstrapNodes	[]*Node			// bootstrap nodes
	DataDir			string			// data directory
	Name			string			// node name
	NodeDB			string			// node database
	BootstrapNode	bool			// bootstrap node flag
	SubNetIdList	[]SubNetworkID	// sub network identity list. do not put the identity
									// of the local node in this list.
}

//
// Configuration about protocols supported
//
type Cfg4Protocols struct {
	ProtoNum  uint32     	// local protocol number
	Protocols []Protocol	// local protocol table
}

//
// Configuration about dht route manager
//
type Cfg4DhtRouteManager struct {
	RandomQryNum	int				// times to try query for a random peer identity
	Period			time.Duration	// timer period to fire a bootstrap
}

//
// Default version string, formated as "M.m0.m1.m2"
//
const dftVersion = "0.1.0.0"

//
// Default p2p instance name
//
const dftName = "test"

//
// Default configuration(notice that it's not a validated configuration and
// could never be applied), most of those defaults must be overided by higher
// lever module of system.
//
const (
	dftUdpPort = 30303
	dftTcpPort = 30303
)

var dftLocal = Node {
	IP:		net.IPv4(192,168,2, 123),
	UDP:	dftUdpPort,
	TCP:	dftTcpPort,
	ID:		NodeID{0},
}


//
// Multiple configurations each identified by its' name
//
var config = make(map[string] *Config)

//
// Get default non-bootstrap node config
//
func P2pDefaultConfig() *Config {

	var defaultConfig = Config {

		//
		// Chain application part
		//

		NetworkType:			P2pNetworkTypeDynamic,
		Name:					dftName,
		Version:				dftVersion,
		PrivateKey:				nil,
		PublicKey:				nil,
		StaticMaxPeers:			MaxPeers,
		StaticMaxInbounds:		MaxInbounds,
		StaticMaxOutbounds:		MaxOutbounds,
		BootstrapNodes:			BootstrapNodes,
		StaticNodes:			nil,
		StaticNetId:			ZeroSubNet,
		NodeDataDir:			P2pDefaultDataDir(true),
		NodeDatabase:			datadirNodeDatabase,
		NoDial:					false,
		NoAccept:				false,
		BootstrapNode:			false,
		Local:					dftLocal,
		ProtoNum:				1,
		Protocols:				[]Protocol {{Pid:0,Ver:[4]byte{0,1,0,0},}},
		SubNetMaxPeers:			map[SubNetworkID]int{},
		SubNetMaxOutbounds:		map[SubNetworkID]int{},
		SubNetMaxInBounds:		map[SubNetworkID]int{},
		SubNetIdList:			[]SubNetworkID{},

		//
		// DHT application part
		//

		dhtRutCfg: Cfg4DhtRouteManager {
			RandomQryNum:	1,
			Period:			time.Minute * 1,
		},
	}

	return &defaultConfig
}

//
// Get default bootstrap node config
//
func P2pDefaultBootstrapConfig() *Config {

	var defaultConfig = Config {
		NetworkType:			P2pNetworkTypeDynamic,
		Name:					dftName,
		Version:				dftVersion,
		PrivateKey:				nil,
		PublicKey:				nil,
		StaticMaxPeers:			0,
		StaticMaxInbounds:		0,
		StaticMaxOutbounds:		0,
		BootstrapNodes:			BootstrapNodes,
		StaticNodes:			nil,
		StaticNetId:			ZeroSubNet,
		NodeDataDir:			P2pDefaultDataDir(true),
		NodeDatabase:			datadirNodeDatabase,
		NoDial:					true,
		NoAccept:				true,
		BootstrapNode:			true,
		Local:					dftLocal,
		ProtoNum:				1,
		Protocols:				[]Protocol {{Pid:0,Ver:[4]byte{0,1,0,0},}},
		SubNetMaxPeers:			map[SubNetworkID]int{},
		SubNetMaxOutbounds:		map[SubNetworkID]int{},
		SubNetMaxInBounds:		map[SubNetworkID]int{},
		SubNetIdList:			[]SubNetworkID{},
	}

	return &defaultConfig
}

//
// P2pSetConfig
//
func P2pSetConfig(name string, cfg *Config) (string, P2pCfgErrno) {

	//
	// Update, one SHOULD first call P2pDefaultConfig to get default value, modify
	// some fields if necessary, and then call this function, since the configuration
	// is overlapped directly here in this function.
	//

	if cfg == nil {
		log.LogCallerFileLine("P2pSetConfig: invalid configuration")
		return name, PcfgEnoParameter
	}

	//
	// Check configuration. Notice that we do not need a private key in current
	// implement, only public key needed to build the local node identity, so one
	// can leave private to be nil while give a not nil public key. If both are
	// nils, key pair will be built, see bellow pls.
	//

	if cfg.PrivateKey == nil {
		log.LogCallerFileLine("P2pSetConfig: private key is empty")
	}

	if cfg.PublicKey == nil {
		log.LogCallerFileLine("P2pSetConfig: public key is empty")
	}

	if m1, m2, m3 := len(cfg.SubNetIdList) == len(cfg.SubNetMaxPeers),
		len(cfg.SubNetIdList) == len(cfg.SubNetMaxInBounds),
		len(cfg.SubNetIdList) == len(cfg.SubNetMaxOutbounds); !(m1 && m2 && m3) {

		log.LogCallerFileLine("P2pSetConfig: invalid sub network configuration")
		return name, PcfgEnoParameter
	}

	for key, maxPeers := range cfg.SubNetMaxPeers {
		if maxPeers < cfg.SubNetMaxOutbounds[key] + cfg.SubNetMaxInBounds[key] {
			log.LogCallerFileLine("P2pSetConfig: invalid sub network configuration")
			return name, PcfgEnoParameter
		}
	}

	if len(cfg.Name) == 0 {
		log.LogCallerFileLine("P2pSetConfig: node name is empty")
	}

	if cap(cfg.BootstrapNodes) == 0 {
		log.LogCallerFileLine("P2pSetConfig: BootstrapNodes is empty")
	}

	if cap(cfg.StaticNodes) == 0 {
		log.LogCallerFileLine("P2pSetConfig: StaticNodes is empty")
	}

	//
	// Seems path.IsAbs does not work under Windows
	//

	if len(cfg.NodeDataDir) == 0 /*|| path.IsAbs(cfg.NodeDataDir) == false*/ {
		log.LogCallerFileLine("P2pSetConfig: invaid data directory")
		return name, PcfgEnoDataDir
	}

	if len(cfg.NodeDatabase) == 0 {
		log.LogCallerFileLine("P2pSetConfig: invalid database name")
		return name, PcfgEnoDatabase
	}

	if cfg.Local.IP == nil {
		log.LogCallerFileLine("P2pSetConfig: invalid ip address")
		return name, PcfgEnoIpAddr
	}

	name = strings.Trim(name, " ")
	if len(name) == 0 {
		if len(cfg.CfgName) == 0 {
			log.LogCallerFileLine("P2pSetConfig: empty configuration name")
			return name, PcfgEnoParameter
		}
		name = cfg.CfgName
	}
	cfg.CfgName = name

	if _, dup := config[name]; dup {
		log.LogCallerFileLine("P2pSetConfig: duplicated configuration name: %s", name)
		log.LogCallerFileLine("P2pSetConfig: old configuration: %+v", *config[name])
		log.LogCallerFileLine("P2pSetConfig: overlapped by new configuration: %+v", *cfg)
	}
	config[name] = cfg

	//
	// setup local node identity from key
	//

	if p2pSetupLocalNodeId(cfg) != PcfgEnoNone {
		log.LogCallerFileLine("P2pSetConfig: invalid ip address")
		return name, PcfgEnoNodeId
	}

	return name, PcfgEnoNone
}

//
// Get global configuration pointer
//
func P2pGetConfig(name string) *Config {
	return config[name]
}

//
// Node identity to hex string
//
func P2pNodeId2HexString(id NodeID) string {
	return fmt.Sprintf("%X", id[:])
}

//
// Sub network identity to hex string
//
func P2pSubNetId2HexString(id SubNetworkID) string {
	return fmt.Sprintf("%X", id[:])
}

//
// Hex-string to node identity
//
func P2pHexString2NodeId(hex string) *NodeID {

	var nid = NodeID{byte(0)}

	if len(hex) != NodeIDBytes * 2 {
		log.LogCallerFileLine("P2pHexString2NodeId: " +
			"invalid length: %d",
			len(hex))
		return nil
	}

	for cidx, c := range hex {

		if c >= '0' && c <= '9' {
			c = c - '0'
		} else if c >= 'a' && c <= 'f' {
			c = c - 'a' + 10
		} else if c >= 'A' && c <= 'F' {
			c = c - 'A' + 10
		} else {
			log.LogCallerFileLine("P2pHexString2NodeId: " +
				"invalid string: %s",
				hex)
			return nil
		}

		bidx := cidx >> 1
		if cidx & 0x01 == 0 {
			nid[bidx] = byte(c << 4)
		} else {
			nid[bidx] += byte(c)
		}
	}

	return &nid
}

//
// Get default data directory
//
func P2pDefaultDataDir(flag bool) string {

	//
	// get home and setup default dir
	//

	home := P2pGetUserHomeDir()

	if home != "" {
		if runtime.GOOS == "darwin" {
			home = filepath.Join(home, "Library", "yee")
		} else if runtime.GOOS == "windows" {
			home = filepath.Join(home, "AppData", "Roaming", "Yee")
		} else {
			home = filepath.Join(home, ".yee")
		}
	}

	//
	// check flag to create default directory if it's not exit
	//

	if flag {
		_, err := os.Stat(home)
		if err == nil {
			return home
		}
		if os.IsNotExist(err) {
			err := os.MkdirAll(home, 0700)
			if err != nil {
				return ""
			}
		}
	}

	return home
}

//
// Get user directory
//
func P2pGetUserHomeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}

//
// Build private key
//
func p2pBuildPrivateKey(cfg *Config) *ecdsa.PrivateKey {

	//
	// Here we apply the Ethereum crypto package to build node private key:
	//
	// 1) if no data directory specified, try to generate key, but do no save to file;
	// 2) if data directory presented, try to load key from file;
	// 3) if load failed, try to generate key and the save it to file;
	//
	// See bellow please, also see Ethereum function node.NodeKey pls.
	//

	if cfg.NodeDataDir == "" {
		//key, err := ethereum.GenerateKey()
		key, err := GenerateKey()
		if err != nil {
			log.LogCallerFileLine("p2pBuildPrivateKey: " +
				"GenerateKey failed, err: %s",
				err.Error())
			return nil
		}
		return key
	}

	keyfile := filepath.Join(cfg.NodeDataDir, cfg.Name, PcfgEnoIpAddrivateKey)
	//if key, err := ethereum.LoadECDSA(keyfile); err == nil {
	if key, err := LoadECDSA(keyfile); err == nil {
		log.LogCallerFileLine("p2pBuildPrivateKey: " +
			"private key loaded ok from file: %s",
			keyfile)
		return key
	}

	//key, err := ethereum.GenerateKey()
	key, err := GenerateKey()
	if err != nil {
		log.LogCallerFileLine("p2pBuildPrivateKey: " +
			"GenerateKey failed, err: %s",
			err.Error())
		return nil
	}

	instanceDir := filepath.Join(cfg.NodeDataDir, cfg.Name)
	if _, err := os.Stat(instanceDir); os.IsNotExist(err) {
		if err := os.MkdirAll(instanceDir, 0700); err != nil {
			log.LogCallerFileLine("p2pBuildPrivateKey: " +
				"MkdirAll failed, err: %s, path: %s",
				err.Error(), instanceDir)
			return key
		}
	}

	//if err := ethereum.SaveECDSA(keyfile, key); err != nil {
	if err := SaveECDSA(keyfile, key); err != nil {
		log.LogCallerFileLine("p2pBuildPrivateKey: " +
			"SaveECDSA failed, err: %s",
			err.Error())
	}

	log.LogCallerFileLine("p2pBuildPrivateKey: " +
		"key save ok to file: %s",
		keyfile)

	return key
}

//
// Trans public key to node identity
//
func p2pPubkey2NodeId(pub *ecdsa.PublicKey) *NodeID {

	var id NodeID

	pbytes := elliptic.Marshal(pub.Curve, pub.X, pub.Y)

	if len(pbytes)-1 != len(id) {
		log.LogCallerFileLine("p2pPubkey2NodeId: " +
			"invalid public key for node identity")
		return nil
	}
	copy(id[:], pbytes[1:])

	return &id
}

//
// Setup local node identity
//
func P2pSetupLocalNodeId(cfg *Config) P2pCfgErrno {
	return p2pSetupLocalNodeId(cfg)
}

func p2pSetupLocalNodeId(cfg *Config) P2pCfgErrno {

	if cfg.PrivateKey != nil {

		cfg.PublicKey = &cfg.PrivateKey.PublicKey

	} else if cfg.PublicKey == nil {

		cfg.PrivateKey = p2pBuildPrivateKey(cfg)

		if cfg.PrivateKey == nil {
			log.LogCallerFileLine("p2pSetupLocalNodeId: " +
				"p2pBuildPrivateKey failed")
			return PcfgEnoPrivateKye
		}

		cfg.PublicKey = &cfg.PrivateKey.PublicKey
	}

	pnid := p2pPubkey2NodeId(cfg.PublicKey)

	if pnid == nil {

		log.LogCallerFileLine("p2pSetupLocalNodeId: " +
			"p2pPubkey2NodeId failed")
		return PcfgEnoPublicKye
	}
	cfg.Local.ID = *pnid

	log.LogCallerFileLine("p2pSetupLocalNodeId: " +
		"local node identity: %s",
		P2pNodeId2HexString(cfg.Local.ID))

	return PcfgEnoNone
}

//
// Setup default bootstrap nodes
//
func P2pSetupDefaultBootstrapNodes() []*Node {
	return P2pSetupBootstrapNodes(BootstrapNodeUrl)
}

//
// Setup bootstrap nodes
//
func P2pSetupBootstrapNodes(urls []string) []*Node {

	var bsn = make([]*Node, 0, P2pMaxBootstrapNodes)

	for idx, url := range urls {

		strs := strings.Split(url,"@")
		if len(strs) != 2 {
			log.LogCallerFileLine("P2pSetupBootstrapNodes: " +
				"invalid bootstrap url: %s",
				url)
			return nil
		}

		strNodeId := strs[0]
		strs = strings.Split(strs[1],":")
		if len(strs) != 3 {
			log.LogCallerFileLine("P2pSetupBootstrapNodes: " +
				"invalid bootstrap url: %s",
				url)
			return nil
		}

		strIp := strs[0]
		strUdpPort := strs[1]
		strTcpPort := strs[2]

		pid := P2pHexString2NodeId(strNodeId)
		if pid == nil {
			log.LogCallerFileLine("P2pSetupBootstrapNodes: " +
				"P2pHexString2NodeId failed, strNodeId: %s",
				strNodeId)
			return nil
		}

		bsn = append(bsn, new(Node))
		copy(bsn[idx].ID[:], (*pid)[:])
		bsn[idx].IP = net.ParseIP(strIp)

		if port, err := strconv.Atoi(strUdpPort); err != nil {
			log.LogCallerFileLine("P2pSetupBootstrapNodes: " +
				"Atoi for UDP port failed, err: %s",
				err.Error())
			return nil
		} else {
			bsn[idx].UDP = uint16(port)
		}

		if port, err := strconv.Atoi(strTcpPort); err != nil {
			log.LogCallerFileLine("P2pSetupBootstrapNodes: " +
				"Atoi for TCP port failed, err: %s",
				err.Error())
			return nil
		} else {
			bsn[idx].TCP = uint16(port)
		}
	}

	return  bsn
}

//
// Get configuration of neighbor discovering manager
//
func P2pConfig4UdpNgbManager(name string) *Cfg4UdpNgbManager {
	return &Cfg4UdpNgbManager {
		IP:				config[name].Local.IP,
		UDP:			config[name].Local.UDP,
		TCP:			config[name].Local.TCP,
		ID:				config[name].Local.ID,
		NetworkType:	config[name].NetworkType,
		SubNetIdList:	config[name].SubNetIdList,
	}
}

//
// Get configuration of neighbor discovering listener
//
func P2pConfig4UdpNgbListener(name string) *Cfg4UdpNgbListener {
	return &Cfg4UdpNgbListener {
		IP:		config[name].Local.IP,
		UDP:	config[name].Local.UDP,
		TCP:	config[name].Local.TCP,
		ID:		config[name].Local.ID,
	}
}

//
// Get configuration of peer listener
//
func P2pConfig4PeerListener(name string) *Cfg4PeerListener {
	return &Cfg4PeerListener {
		IP:			config[name].Local.IP,
		Port:		config[name].Local.TCP,
		ID:			config[name].Local.ID,
	}
}

//
// Get configuration of peer manager
//
func P2pConfig4PeerManager(name string) *Cfg4PeerManager {
	return &Cfg4PeerManager {
		CfgName:			name, // config[name].CfgName
		NetworkType:		config[name].NetworkType,
		IP:					config[name].Local.IP,
		Port:				config[name].Local.TCP,
		UDP:				config[name].Local.UDP,
		ID:					config[name].Local.ID,
		StaticMaxPeers:		config[name].StaticMaxPeers,
		StaticMaxOutbounds:	config[name].StaticMaxOutbounds,
		StaticMaxInBounds:	config[name].StaticMaxInbounds,
		StaticNodes:		config[name].StaticNodes,
		StaticNetId:		config[name].StaticNetId,
		NoDial:				config[name].NoDial,
		NoAccept:			config[name].NoAccept,
		ProtoNum:			config[name].ProtoNum,
		Protocols:			config[name].Protocols,
		SubNetMaxPeers:		config[name].SubNetMaxPeers,
		SubNetMaxOutbounds:	config[name].SubNetMaxOutbounds,
		SubNetMaxInBounds:	config[name].SubNetMaxInBounds,
		SubNetIdList:		config[name].SubNetIdList,
	}
}

//
// Get configuration of table manager
//
func P2pConfig4TabManager(name string) *Cfg4TabManager {
	return &Cfg4TabManager {
		Local:			config[name].Local,
		BootstrapNodes:	config[name].BootstrapNodes,
		DataDir:		config[name].NodeDataDir,
		Name:			config[name].Name,
		NodeDB:			config[name].NodeDatabase,
		BootstrapNode:	config[name].BootstrapNode,
		NetworkType:	config[name].NetworkType,
		SubNetIdList:	config[name].SubNetIdList,
	}
}

//
// Get protocols
//
func P2pConfig4Protocols(name string) *Cfg4Protocols {
	return &Cfg4Protocols {
		ProtoNum: config[name].ProtoNum,
		Protocols: config[name].Protocols,
	}
}

//
// Get configuration for dht route manager
//
func P2pConfig4DhtRouteManager(name string) *Cfg4DhtRouteManager {
	return &config[name].dhtRutCfg
}

//
// by yeeco, alias Ethereum's S256 to elliptic.P256
//
func S256() elliptic.Curve {
	return elliptic.P256()
}


// LoadECDSA loads a secp256k1 private key from the given file.
func LoadECDSA(file string) (*ecdsa.PrivateKey, error) {
	buf := make([]byte, 64)
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	if _, err := io.ReadFull(fd, buf); err != nil {
		return nil, err
	}

	key, err := hex.DecodeString(string(buf))
	if err != nil {
		return nil, err
	}
	return ToECDSA(key)
}

// SaveECDSA saves a secp256k1 private key to the given file with
// restrictive permissions. The key data is saved hex-encoded.
func SaveECDSA(file string, key *ecdsa.PrivateKey) error {
	k := hex.EncodeToString(FromECDSA(key))
	return ioutil.WriteFile(file, []byte(k), 0600)
}

func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(S256(), rand.Reader)
}

// ToECDSA creates a private key with the given D value.
func ToECDSA(d []byte) (*ecdsa.PrivateKey, error) {
	return toECDSA(d, true)
}

// toECDSA creates a private key with the given D value. The strict parameter
// controls whether the key's length should be enforced at the curve size or
// it can also accept legacy encodings (0 prefixes).
func toECDSA(d []byte, strict bool) (*ecdsa.PrivateKey, error) {
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = S256()
	if strict && 8*len(d) != priv.Params().BitSize {
		return nil, fmt.Errorf("invalid length, need %d bits", priv.Params().BitSize)
	}
	priv.D = new(big.Int).SetBytes(d)


	//
	// Notice: since S256 aliased to P256, we do not apply the following checks under original
	// S256, it's not sure to be suitable. --- yeeco, 20180612
	//
	//

	// The priv.D must < N
	//if priv.D.Cmp(secp256k1_N) >= 0 {
	//	return nil, fmt.Errorf("invalid private key, >=N")
	//}

	// The priv.D must not be zero or negative.
	//if priv.D.Sign() <= 0 {
	//	return nil, fmt.Errorf("invalid private key, zero or negative")
	//}

	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(d)
	if priv.PublicKey.X == nil {
		return nil, errors.New("invalid private key")
	}
	return priv, nil
}

// FromECDSA exports a private key into a binary dump.
func FromECDSA(priv *ecdsa.PrivateKey) []byte {
	if priv == nil {
		return nil
	}
	//
	// Modified by yeeco, remove reference to Ethereum's math
	//
	//return math.PaddedBigBytes(priv.D, priv.Params().BitSize/8)
	return priv.D.Bytes()
}

