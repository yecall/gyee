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

package scheduler

import (
	"sync"
	"time"

	"github.com/yeeco/gyee/p2p/config"
)

//
// message
//
type schMessage = SchMessage

//
// User task entry point
//
type schUserTaskProc = SchUserTaskInterface

//
// max mail box size
//
const schDftMbSize = SchDftMbSize
const schMaxMbSize = SchMaxMbSize

//
// Watch dog
//
type schWatchDog = SchWatchDog

//
// Mail box
//
type schMailBox struct {
	qtm  *chan *schMessage	// channel for timer
	que  *chan *schMessage	// channel for message
	size int              	// number of messages buffered
}

//
// Timer type
//
const (
	schTmTypeNull     = -1                // null
	schTmTypePeriod   = SchTmTypePeriod   // cycle timer
	schTmTypeAbsolute = SchTmTypeAbsolute // absolute timer
)

type schTimerType SchTimerType

//
// User task timer description
//
type timerDescription TimerDescription

//
// Timer control block
//
type schTimerCtrlBlock struct {
	name     string        // timer name
	utid     int           // user timer identity
	tmt      schTimerType  // timer type, see aboved
	dur      time.Duration // duration: a period value or duration from now
	stop     chan bool     // should be stop
	stopped  chan bool     // had been stopped
	taskNode *schTaskNode  // pointer to owner task node
	extra    interface{}   // extra data return to timer owner when expired
}

//
// Timer control block node
//
type schTmcbNode struct {
	tmcb schTimerCtrlBlock // timer control block
	last *schTmcbNode      // pointer to last node
	next *schTmcbNode      // pointer to next node
}

//
// Timer node pool
//
const schTimerNodePoolSize = 1024 * 16 // timer node pool size, must be (2^n)

//
// Task struct
//
const schMaxTaskTimer = SchMaxTaskTimer // max timers can be held by one user task
const schTmqSize = 1024					// timer message queue size
const schTmqFork = true					// do not send timer message to common queue if true
const schInvalidTid = SchInvalidTid     // invalid timer identity
const evHistorySize = 64                // round buffer size fo event history
type schTask struct {
	lock            sync.Mutex                    // lock to protect task control block
	sdl             *scheduler                    // pointer to scheduler
	name            string                        // task name, should be unique in system
	utep            schUserTaskProc               // user task entry point
	mailbox         schMailBox                    // mail box
	killing			bool						  // in killing
	killed			bool						  // killed
	scheduling		bool						  // in scheduling
	doneGot			bool						  // if EvSchDone got
	done            chan SchErrno                 // done with errno
	stopped         chan bool                     // stopped signal
	tmTab           [schMaxTaskTimer]*schTmcbNode // timer node table
	tmIdxTab        map[*schTmcbNode]int          // map time node pointer to its' index in tmTab
	dog             schWatchDog                   // wathch dog
	dieCb           func(interface{}) SchErrno    // callbacked when going to die
	goStatus        int                           // in going or suspended
	evHistory       [evHistorySize]schMessage     // event history
	evhIndex        int                           // event history index
	evTotal         int64                         // total event number
	userData        interface{}                   // data area pointer of user task
	isStatic        bool                          // is static task
	isPoweron       bool                          // if EvSchPoweron sent to task
	delayMessages   []*schMessage                 // messages before EvSchPoweron
	discardMessages int64                         // messages discarded
}

//
// Task node
//
type schTaskNode struct {
	task schTask      // this task node
	last *schTaskNode // pointing to the last node
	next *schTaskNode // pointing to the next node
}

//
// The ycp2p scheduler struct: since it's only planed to invoke the scheduler
// internal mode ycp2p, this struct is not exported, any interface to created
// such a scheduler object is not provided, see it pls.
//
const schTaskNodePoolSize = 1024 * 16 // task node pool size, must be (2^n)

type scheduler struct {

	//
	// Notice: SYNC-IPC liked mode is not supported now, a task can not send a message to other
	// and then blocked until the message receiver task ACK it. If the SYNC-IPC is necessary in
	// practice really, we might take it into account future ...
	//

	lock             sync.Mutex                        // lock to protect the scheduler
	appType          int                               // application type
	p2pCfg           *config.Config                    // p2p network configuration
	tkFree           *schTaskNode                      // free task queue
	freeSize         int                               // number of nodes in free
	tkBusy           *schTaskNode                      // busy task queue in scheduling
	tkMap            map[string]*schTaskNode           // map task name to pointer of running task node
	tnMap			 map[*schTaskNode]string		   // map task node pointer to task name
	busySize         int                               // number of nodes in busy
	tmFree           *schTmcbNode                      // free timer node queue
	tmFreeSize       int                               // free timer node queue size
	schTaskNodePool  [schTaskNodePoolSize]schTaskNode  // task node pool
	schTimerNodePool [schTimerNodePoolSize]schTmcbNode // timer node pool
	powerOff         bool                              // power off stage flag
}

//
// static tasks name
//
const (

	//
	// shell manager name
	//

	ShMgrName    = "ShMgr"    // chain shell manager
	DhtShMgrName = "DhtShMgr" // dht shell manager

	//
	// followings are for chain application
	//

	DcvMgrName       = "DcvMgr"       // disccover manager
	TabMgrName       = "TabMgr"       // table
	NgbLsnName       = "NgbLsn"       // udp neighbor listener
	NgbMgrName       = "NgbMgr"       // udp neighbor manager
	NgbReaderName    = "UdpReader"    // udp reader
	PeerLsnMgrName   = "PeerLsnMgr"   // tcp peer listener
	PeerAccepterName = "peerAccepter" // tcp accepter
	PeerMgrName      = "PeerMgr"      // tcp peer manager

	//
	// followings are for DHT application
	//

	DhtMgrName    = "DhtMgr"    // dht manager
	DhtLsnMgrName = "DhtLsnMgr" // dht listner manager
	DhtPrdMgrName = "DhtPrdMgr" // dht provider manager
	DhtQryMgrName = "DhtQryMgr" // dht query manager
	DhtRutMgrName = "DhtRutMgr" // dht route manager
	DhtConMgrName = "DhtConMgr" // dht connection manager
	DhtDsMgrName  = "DhtDsMgr"  // dht data store manager

	// NAT
	NatMgrName = "NatMgr" // nat manager
)
