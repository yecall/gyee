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
import "sync"



//
// Package passed into user's callback
//
type P2pPackage4Callback struct {
	PeerInfo		*PeerInfo	// peer information
	ProtoId			int				// protocol identity
	PayloadLength	int				// bytes in payload buffer
	Payload			[]byte			// payload buffer
}

//
// Message from user
//
type P2pPackage2Peer struct {
	IdList			[]PeerId		// peer identity list
	ProtoId			int				// protocol identity
	PayloadLength	int				// payload length
	Payload			[]byte			// payload
	Extra			interface{}		// extra info: user this field to tell p2p more about this message,
									// for example, if broadcasting is wanted, then set IdList to nil
									// and setup thie extra info field.
}

//
// callback type
//
const (
	P2pInfIndCb	= iota
	P2pInfPkgCb
)

//
// P2p peer status indication callback type
//
const (
	P2pIndPeerActivated	= iota		// peer activated
	P2pIndConnStatus				// connection status changed
	P2pIndPeerClosed				// connection closed
)

type P2pIndPeerActivatedPara struct {
	Ptn			interface{}			// task node pointer
	PeerInfo	*Handshake			// handshake info
}

type P2pIndConnStatusPara struct {
	Ptn			interface{}			// task node pointer
	PeerInfo	*Handshake			// handshake info
	Status		int					// status code

	//
	// Indicate that if the instance would be closed by underlying of p2p.
	// If true, user should not try to close the instance; else, user should
	// determine basing on the status code to call shell.P2pInfClosePeer to
	// close the instance if necessary.
	//

	Flag		bool				// flag

	Description	string				// description
}

type P2pIndPeerClosedPara struct {
	PeerId		PeerId				// peer identity
}

type P2pInfIndCallback func(what int, para interface{}) interface{}

//
// P2p callback function type for package incoming
//
type P2pInfPkgCallback func(msg *P2pPackage4Callback) interface{}

//
// Indication handler
//
var P2pIndHandler P2pInfIndCallback = nil

//
// Lock for syncing callbacks
//
var Lock4Cb sync.Mutex


