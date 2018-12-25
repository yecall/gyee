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

package table

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"os"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"

	p2plog "github.com/yeeco/gyee/p2p/logger"
	config "github.com/yeeco/gyee/p2p/config"
)


//
// debug
//
type ndbLogger struct {
	debug__		bool
}

var ndbLog = ndbLogger {
	debug__:	false,
}

func (log ndbLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// node database
//

var (
	nodeDBNodeExpiration = 24 * time.Hour // Time for node to be expired
)

type nodeDB struct {
	lvl    *leveldb.DB   // Pointer to level database
	self   NodeID        // Identity of the owner node of this database
}

var (
	// Since it's possible an old database might be present and not suitable fro application,
	// a "version" field is applied to keep this case off: When a node database is opened, it
	// is checked against the preferred version, if mismatched, truncate to empty. The key for
	// version looks like: nodeId + versionKey.
	versionKey = "version"
	// In current implement, the application operates the database often with the identity of
	// the peer node as the parameter, and the operating context indicated by the interface
	// function name, like "updateLastPing", "updateLastPong". But in the database, a context
	// should be recognized to construct unique key. Fot this, we apply follwing key layout:
	// for node:
	//		key=nodeId + root
	// for node with context:
	//		key=nodeId + root + context
	// See function makeKey for more.
	rootKey			= "dcvMgr"				// root
	pingKey			= rootKey + ":lpi"		// last ping
	pongKey			= rootKey + ":lpo"		// last pong
	findfailKey		= rootKey + ":ffa"		// find fail
)

func newNodeDB(path string, version int, self NodeID) (*nodeDB, error) {
	if path == "" {
		return newMemoryNodeDB(self)
	}
	return newPersistentNodeDB(path, version, self)
}

func newMemoryNodeDB(self NodeID) (*nodeDB, error) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}
	return &nodeDB{
		lvl:  db,
		self: self,
	}, nil
}

func newPersistentNodeDB(path string, version int, self NodeID) (*nodeDB, error) {
	opts := &opt.Options{OpenFilesCacheCapacity: 5}
	db, err := leveldb.OpenFile(path, opts)
	if _, iscorrupted := err.(*errors.ErrCorrupted); iscorrupted {
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}
	var idEx = make([]byte, 0)
	idEx = append(idEx, self[:]...)
	idEx = append(idEx, ZeroSubNet[:]...)
	verKey := makeKey(idEx, versionKey)
	currentVer := make([]byte, binary.MaxVarintLen64)
	currentVer = currentVer[:binary.PutVarint(currentVer, int64(version))]
	blob, err := db.Get(verKey, nil)
	switch err {
	case leveldb.ErrNotFound:
		if err := db.Put(verKey, currentVer, nil); err != nil {
			db.Close()
			return nil, err
		}
	case nil:
		if !bytes.Equal(blob, currentVer) {
			db.Close()
			if err = os.RemoveAll(path); err != nil {
				return nil, err
			}
			return newPersistentNodeDB(path, version, self)
		}
	}
	return &nodeDB{
		lvl:  db,
		self: self,
	}, nil
}

func makeKey(id []byte, field string) []byte {
	return append(id[:], field...)
}

func splitKey(key []byte) (id NodeIdEx, field string) {
	copy(id[:], key[:len(id)])
	field = string(key[len(id):])
	return id, field
}

func (db *nodeDB) fetchInt64(key []byte) int64 {
	// in our application, we need to fetch timestamp value which stored as int64 type,
	// see function storeInt64 please.
	blob, err := db.lvl.Get(key, nil)
	if err != nil {
		return 0
	}
	val, read := binary.Varint(blob)
	if read <= 0 {
		return 0
	}
	return val
}

func (db *nodeDB) storeInt64(key []byte, n int64) error {
	// the main purpose of this function is to store timestamp vaule as type int64.
	// in our application, timestamps about ping/pong/update are applied.
	blob := make([]byte, binary.MaxVarintLen64)
	blob = blob[:binary.PutVarint(blob, n)]
	return db.lvl.Put(key, blob, nil)
}

func (db *nodeDB) node(snid SubNetworkID, id NodeID) *Node {
	idEx := make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	blob, err := db.lvl.Get(makeKey(idEx, rootKey), nil)
	if err != nil {
		return nil
	}
	node := new(Node)
	if err := DecodeBytes(blob, node, nil); err != nil {
		ndbLog.Debug("node: DecodeBytes failed")
		return nil
	}
	node.sha = *TabNodeId2Hash(id)
	return node
}

func (db *nodeDB) updateNode(snid SubNetworkID, node *Node) error {
	blob, err := EncodeToBytes(snid, node)
	if err != nil {
		return err
	}
	var idEx = make([]byte, 0)
	idEx = append(idEx, node.ID[:]...)
	idEx = append(idEx, snid[:]...)
	return db.lvl.Put(makeKey(idEx, rootKey), blob, nil)
}

func (db *nodeDB) deleteNode(snid SubNetworkID, id NodeID) error {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	deleter := db.lvl.NewIterator(util.BytesPrefix(makeKey(idEx, "")), nil)
	for deleter.Next() {
		if err := db.lvl.Delete(deleter.Key(), nil); err != nil {
			return err
		}
	}
	return nil
}

func (db *nodeDB) expireNodes() error {
	threshold := time.Now().Add(-nodeDBNodeExpiration)
	it := db.lvl.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		id, field := splitKey(it.Key())
		if field != rootKey {
			continue
		}
		var nodeId NodeID
		var snid SubNetworkID
		snid = SubNetworkID{id[config.NodeIDBytes], id[config.NodeIDBytes+1]}
		copy(nodeId[0:], id[:config.NodeIDBytes])
		if !bytes.Equal(id[2:], db.self[:]) {
			if seen := db.lastPong(snid, nodeId); seen.After(threshold) {
				continue
			}
		}
		db.deleteNode(snid, nodeId)
	}
	return nil
}

func (db *nodeDB) lastPing(snid SubNetworkID, id NodeID) time.Time {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return time.Unix(db.fetchInt64(makeKey(idEx, pingKey)), 0)
}

func (db *nodeDB) updateLastPing(snid SubNetworkID, id NodeID, instance time.Time) error {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return db.storeInt64(makeKey(idEx, pingKey), instance.Unix())
}

func (db *nodeDB) lastPong(snid SubNetworkID, id NodeID) time.Time {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return time.Unix(db.fetchInt64(makeKey(idEx, pongKey)), 0)
}

func (db *nodeDB) updateLastPong(snid SubNetworkID, id NodeID, instance time.Time) error {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return db.storeInt64(makeKey(idEx, pongKey), instance.Unix())
}

func (db *nodeDB) findFails(snid SubNetworkID, id NodeID) int {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return int(db.fetchInt64(makeKey(idEx, findfailKey)))
}

func (db *nodeDB) updateFindFails(snid SubNetworkID, id NodeID, fails int) error {
	var idEx = make([]byte, 0)
	idEx = append(idEx, id[:]...)
	idEx = append(idEx, snid[:]...)
	return db.storeInt64(makeKey(idEx, findfailKey), int64(fails))
}

func (db *nodeDB) querySeeds(snid SubNetworkID, n int, maxAge time.Duration) []*Node {
	// this function called to find out nodes with age less than maxAge, and the max
	// number of these nodes should not exceed n passed in.
	// in our application, since we are looking for nodes to flood information, we should
	// not "Likes Or Dislikes" this or that.
	var (
		now   = time.Now()
		nodes = make([]*Node, 0, n)
		it    = db.lvl.NewIterator(nil, nil)
		id    NodeID
	)
	defer it.Release()

seek:

	// max try: n*5
	for seeks := 0; len(nodes) < n && seeks < n*5; seeks++ {

		// segment nodes by the first byte, see comments about function it.Seek
		// for more please:
		ctr := id[0]
		rand.Read(id[:])
		id[0] = ctr + id[0]%16

		// construct the key
		var idEx = make([]byte, 0)
		idEx = append(idEx, id[:]...)
		idEx = append(idEx, snid[:]...)

		// seek to first, comment segment about it.Seek:
		// "Seek moves the iterator to the first key/value pair whose key is greater
		// than or equal to the given key"
		if it.Seek(makeKey(idEx, rootKey)) != true {
			continue seek
		}

		// check if we got some
		n, nSnid := nextNode(it)
		if n == nil {
			continue seek
		}

		// check sub network identity
		if snid != AnySubNet {
			if *nSnid != snid {
				continue seek
			}
		}

		// check if just node of ourselves local
		if n.ID == db.self {
			continue seek
		}

		// check age
		if now.Sub(db.lastPong(snid, n.ID)) > maxAge {
			continue seek
		}

		// check if duplicated
		for i := range nodes {
			if nodes[i].ID == n.ID {
				continue seek
			}
		}

		// got
		nodes = append(nodes, n)
	}

	return nodes
}

func nextNode(it iterator.Iterator) (*Node, *SubNetworkID) {
	for end := false; !end; end = !it.Next() {
		_, field := splitKey(it.Key())
		if field != rootKey {
			continue
		}
		var n Node
		var snid = AnySubNet
		if err := DecodeBytes(it.Value(), &n, &snid); err != nil {
			ndbLog.Debug("nextNode: DecodeBytes failed")
			continue
		}
		return &n, &snid
	}
	return nil, nil
}

func (db *nodeDB) close() {
	db.lvl.Close()
}

func EncodeToBytes(snid SubNetworkID, node *Node) ([]byte, error) {
	// Test version, only IPV4 supported
	blob := make([]byte, 0)
	blob = append(blob, node.IP...)

	udp := node.UDP
	hb := byte((udp >> 8) & 0xff)
	lb := byte(udp & 0xff)
	blob = append(blob, []byte{hb,lb}...)

	tcp := node.TCP
	hb = byte((tcp >> 8) & 0xff)
	lb = byte(tcp & 0xff)
	blob = append(blob, []byte{hb,lb}...)

	blob = append(blob, node.ID[:]...)
	blob = append(blob, snid[:]...)
	blob = append(blob, node.sha[:]...)

	return blob, nil
}

func DecodeBytes(blob []byte, node *Node, snid *SubNetworkID) error {
	// Test version, only IPV4 supported
	node.IP = append(node.IP, blob[0:4]...)
	node.UDP = (uint16(blob[4]) << 8) +uint16(blob[5])
	node.TCP = (uint16(blob[6]) << 8) +uint16(blob[7])
	copy(node.ID[0:], blob[8:8+cap(node.ID)])
	if snid != nil {
		(*snid)[0] = blob[8+cap(node.ID)]
		(*snid)[1] = blob[8+cap(node.ID)+1]
	}
	copy(node.sha[0:], blob[8+cap(node.ID)+2:])
	return nil
}

