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
)

//
// message
//
type schMessage SchMessage

//
// User task entry point
//
type schUserTaskEp SchUserTaskEp

//
// max mail box size
//
const schMaxMbSize	 = SchMaxMbSize

//
// Watch dog for a user task
//
const (
	schDeaultWatchCycle			= SchDeaultWatchCycle
	schDefaultDogCycle			= SchDefaultDogCycle
	schDefaultDogDieThresold		= SchDefaultDogDieThresold
)

type schWatchDog SchWatchDog

//
// Mail box
//
type schMailBox struct {
	que		*chan schMessage	// channel for message
	size	int					// number of messages buffered
}

//
// Timer type
//
const (
	schTmTypeNull		= -1					// null
	schTmTypePeriod		= SchTmTypePeriod	// cycle timer
	schTmTypeAbsolute	= SchTmTypeAbsolute	// absolute timer
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
	name		string			// timer name
	utid		int				// user timer identity
	tmt			schTimerType	// timer type, see aboved
	dur			time.Duration	// duration: a period value or duration from now
	stop		chan bool		// should be stop
	stopped		chan bool		// had been stopped
	taskNode	*schTaskNode	// pointer to owner task node
	extra		interface{}		// extra data return to timer owner when expired
}

//
// Timer control block node
//
type schTmcbNode struct {
	tmcb	schTimerCtrlBlock	// timer control block
	last	*schTmcbNode		// pointer to last node
	next	*schTmcbNode		// pointer to next node
}

//
// Timer node pool
//
const schTimerNodePoolSize	= 2048						// timer node pool size, must be (2^n)
var schTimerNodePool [schTimerNodePoolSize]schTmcbNode	// timer node pool

//
// Task struct
//
const schMaxTaskTimer	= SchMaxTaskTimer		// max timers can be held by one user task
const schInvalidTid		= SchInvalidTid			// invalid timer identity

type schTask struct {
	lock		sync.Mutex						// lock to protect task control block
	name		string							// task name
	utep		schUserTaskEp					// user task entry point
	mailbox		schMailBox						// mail box
	done		chan SchErrno					// done with errno
	stopped		chan bool						// stopped signal
	tmTab		[schMaxTaskTimer]*schTmcbNode	// timer node table
	tmIdxTab	map[*schTmcbNode] int			// map time node pointer to its' index in tmTab
	dog			schWatchDog						// wathch dog
	dieCb		func(interface{}) SchErrno		// callbacked when going to die
	goStatus	int								// in going or suspended
	userData	interface{}						// data area pointer of user task
}

//
// Task node
//
type schTaskNode struct {
	task	schTask			// this task node
	last	*schTaskNode	// pointing to the last node
	next	*schTaskNode	// pointing to the next node
}

//
// Task group
//
const schMaxGroupSize = SchMaxGroupSize				// max number of group members
type schTaskGroupName	string							// group name as string
type schTaskGroup map[schTaskGroupName][]*schTaskNode	// group map group-name to task node array

//
// The ycp2p scheduler struct: since it's only planed to invoke the scheduler
// internal modle ycp2p, this struct is not exported, any interface to created
// such a scheduler object is not provided, see it pls.
//
const schInvalidTaskIndex		= -1					// invalid index
const schTaskNodePoolSize		= 1024					// task node pool size, must be (2^n)
type schTaskName 	string								// task name
type schTaskIndex	int									// index of task node in pool
var schTaskNodePool	[schTaskNodePoolSize]schTaskNode	// task node pool

type scheduler struct {

	//
	// Notice: SYNC-IPC liked mode is not supported now, a task can not send a message to other
	// and then blocked until the message receiver task ACK it. If the SYNC-IPC is necessary in
	// practice really, we might take it into account future ...
	//

	lock 		sync.Mutex						// lock to protect the scheduler
	tkFree		*schTaskNode					// free task queue
	freeSize	int								// number of nodes in free
	tkBusy		*schTaskNode					// busy task queue in scheduling
	tkMap		map[schTaskName] *schTaskNode	// map task name to pointer of running task node
	busySize	int								// number of nodes in busy
	tmFree		*schTmcbNode					// free timer node queue
	tmFreeSize	int								// free timer node queue size
	tmMap		map[*schTmcbNode] *schTaskNode	// map busy timer node pointer to its' owner task node pointer
	grpMap		schTaskGroup					// group name to group member map
	grpCnt		int								// group counter
}

//
// Scheduler task entry point
//
type schCommonTaskEp func(ptn *schTaskNode) SchErrno


//
// static tasks name
//
const (
	DcvMgrName			= "DcvMgr"			// disccover manager
	TabMgrName			= "TabMgr"			// table
	NgbLsnName			= "NgbLsn"			// udp neighbor listener
	NgbMgrName			= "NgbMgr"			// udp neighbor manager
	NgbReaderName		= "UdpReader"		// udp reader
	PeerLsnMgrName		= "PeerLsnMgr"		// tcp peer listener
	PeerAccepterName	= "peerAccepter"	// tcp accepter
	PeerMgrName			= "PeerMgr"			// tcp peer manager
)
