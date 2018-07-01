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


package shell


//
// DHT errno constants
//
const (
	DHTINF_ENO_NONE		= iota
	DHTINF_ENO_PARA
	DHTINF_ENO_UNKNOWN
	DHTINF_ENO_MAX
)

//
// DHT errno type
//
type DhtErrno int

//
// Command type constants
//
const (
	DHTINF_CMD_NULL			= 0		// null
	DHTINF_CMD_STORE_REQ	= 1		// request to store a chunk
	DHTINF_CMD_STORE_CFM	= 2		// confirm to store chunk request
	DHTINF_CMD_RETRIVE_REQ	= 3		// request to retrive a chunk
	DHTINF_CMD_RETRIVE_RSP	= 4		// confirm to retrive chunk request
	DHTINF_CMD_MAX			= 5		// max, just for bound checking
)

//
// Command type
//
type DhtCommandType	int

//
// Key
//
type DhtinfKey 		[]byte

//
// Chunk
//
type DhtinfChunk	[]byte

//
// Id
//
type DhtinfId		uint64

//
// Request to store a chunk
//
type DhtinfStoreChunkReq struct {
	Key		DhtinfKey		// key for the chunk data
	Chunk	DhtinfChunk		// the chunk data
	Id		DhtinfId		// identity for this request
}

//
// Confirm to sotre a chunk
//
type DhtinfStoreChunkCfm struct {
	Key		DhtinfKey		// key for the chunk data
	Id		DhtinfId		// identity obtained by DhtinfStoreChunkReq.Id
}

//
// Request to retrive a chunk
//
type DhtinfRetriveChunkReq struct {
	Key		DhtinfKey		// key for chunk wanted
	Id		DhtinfId		// identity for this request
}

//
// Confirm to retrive a chunk
//
type DhtinfRetriveChunkCfm struct {
	Key		DhtinfKey		// key for chunk wanted
	Chunk	DhtinfChunk		// the chunk data
	Id		DhtinfId		// identity obtained by DhtinfRetriveChunkReq.Id
}

//
// Confirm handler interface
//
type DhtinfConfirmHandler interface {

	//
	// determine what "msg" is by "ty": a pointer to DhtinfRetriveChunkCfm
	// or DhtinfStoreChunkCfm
	//

	DhtCfmCb(ty DhtCommandType, msg interface{}) interface{}
}

//
// Request to store a chunk
//
func DhtinfStoreChunk(req *DhtinfStoreChunkReq) DhtErrno {
	return DHTINF_ENO_NONE
}

//
// Request to retrive a chunk
//
func DhtinfRetriveChunk(req *DhtinfRetriveChunkReq) DhtErrno {
	return DHTINF_ENO_NONE
}

//
// Register confirm handler
//
func DhtinfRegisterConfirmHandler(h DhtinfConfirmHandler) DhtErrno{
	return DHTINF_ENO_NONE
}
