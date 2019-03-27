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

package peer

import (
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// Package passed into user's callback
//
type P2pPackageRx struct {
	Ptn           interface{} // instance task node pointer
	PeerInfo      *PeerInfo   // peer information
	ProtoId       int         // protocol identity
	MsgId         int         // message identity
	Key           []byte      // message key
	PayloadLength int         // bytes in payload buffer
	Payload       []byte      // payload buffer
}

//
// Message from user
//
type P2pPackage2Peer struct {
	P2pInst       *sch.Scheduler // p2p network instance
	SubNetId      SubNetworkID   // sub network identity
	IdList        []PeerId       // peer identity list
	ProtoId       int            // protocol identity
	Mid           int            // message identity
	Key           []byte         // message key
	PayloadLength int            // payload length
	Payload       []byte         // payload
	Extra         interface{}    // extra info: user this field to tell p2p more about this message,
	// for example, if broadcasting is wanted, then set IdList to nil
	// and setup thie extra info field.
}

//
// callback type
//
const (
	P2pIndCb = iota
	P2pPkgCb
)

//
// P2p peer status indication callback type
//
const (
	P2pIndPeerActivated = iota // peer activated
	P2pIndPeerClosed           // connection closed
)

type P2pIndPeerActivatedPara struct {
	P2pInst  *sch.Scheduler     // p2p instance pointer
	RxChan   chan *P2pPackageRx // channel for packages received
	PeerInfo *Handshake         // handshake info
}

type P2pIndConnStatusPara struct {
	Ptn      interface{} // task node pointer
	PeerInfo *Handshake  // handshake info
	Status   int         // status code

	//
	// Indicate that if the instance would be closed by underlying of p2p.
	// If true, user should not try to close the instance; else, user should
	// determine basing on the status code to call shell.P2pClosePeer to
	// close the instance if necessary.
	//

	Flag bool // flag

	Description string // description
}

type P2pIndPeerClosedPara struct {
	P2pInst *sch.Scheduler // p2p instance pointer
	Snid    SubNetworkID   // sub network identity
	PeerId  PeerId         // peer identity
	Dir     int            // direction
}

type P2pIndCallback func(what int, para interface{}, userData interface{}) interface{}

//
// P2p callback function type for package incoming
//
type P2pPkgCallback func(msg *P2pPackageRx, userData interface{}) interface{}
