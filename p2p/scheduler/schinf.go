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

import(
	"fmt"
	"time"
	yclog "github.com/yeeco/p2p/logger"
	"sync"
)

//
// Scheduler interface errnos
//
type SchErrno int

const (
	SchEnoNone			SchErrno = 0	// none of errors
	SchEnoParameter		SchErrno = 1	// invalid parameters
	SchEnoResource		SchErrno = 2	// no resources
	SchEnoWatchDog		SchErrno = 3	// watch dog
	SchEnoNotFound		SchErrno = 4	// not found
	SchEnoInternal		SchErrno = 5	// internal errors
	SchEnoMismatched	SchErrno = 7	// mismatched
	SchEnoOS			SchErrno = 8	// operating system
	SchEnoConfig		SchErrno = 9	// configuration
	SchEnoKilled		SchErrno = 10	// task killed
	SchEnoNotImpl		SchErrno = 11	// not implemented
	SchEnoUserTask		SchErrno = 12	// internal user task application
	SchEnoDuplicated	SchErrno = 13	// duplicated
	SchEnoSuspended		SchErrno = 14	// user task is suspended for some reasons
	SchEnoUnknown		SchErrno = 15	// unknowns
	SchEnoMax			SchErrno = 16	// just for bound checking
)

var SchErrnoDescription = []string {
	"none of errors",
	"invalid parameters",
	"no resources",
	"watch dog",
	"not found",
	"internal errors",
	"mismathced",
	"unknowns",
}

//
// Stringz an errno with itself
//
func (eno SchErrno) SchErrnoString() string {
	if eno < SchEnoNone || eno >= SchEnoMax {
		return fmt.Sprintf("Can't be stringzed, invalid eno:%d", eno)
	}
	return SchErrnoDescription[eno]
}

//
// Stringz an errno with an eno parameter
//
func SchErrnoString(eno SchErrno) string {
	return eno.SchErrnoString()
}

//
// User task entry point: notice, parameter ptn would be type of pointer to schTaskNode,
// the user task should never try to access the field directly, instead, interface func
// provide by scheduler module should by applied. for example, when user task try to set
// a timer, it should then pass this ptn to function SchInfSetTimer, see it pls. Also,
// user task should try to interpret the msg.body by msg.id, which is defined by user
// task than scheduler itself, of course, timer event is an exception.
//
type SchUserTaskEp func(ptn interface{}, msg *SchMessage) SchErrno

//
// message type for scheduling between user tasks
//
type SchMessage struct {
	sender 	*schTaskNode	// sender task node pointer
	recver	*schTaskNode	// receiver task node pointer
	Id		int				// message identity
	Body	interface{}	// message body
}

//
// Watch dog for a user task
//
const (
	SchDeaultWatchCycle			= time.Second
	SchDefaultDogCycle			= time.Second
	SchDefaultDogDieThresold	= 2
)

type SchWatchDog struct {
	lock			sync.Mutex
	HaveDog			bool				// if dog would come out
	Inited			bool				// watching inited
	Cycle			time.Duration		// feed cycle expected, must be times of second
	BiteCounter		int					// counter of user task bited by dog
	DieThreshold	int					// threshold counter of dog-bited to die
}

//
// Flag for user just be created
//
const (
	SchCreatedGo		= iota			// go at once
	SchCreatedSuspend					// suspended
)

//
// Descriptor for a user tack to be created
//
const SchMaxGroupSize = 64			// max number of group members

//
// max mail box size
//
const SchMaxMbSize	 = 256

type SchTaskDescription struct {
	Name	string						// user task name
	MbSize	int							// mailbox size
	Ep		SchUserTaskEp				// user task entry point
	Wd		*SchWatchDog				// watchdog
	Flag	int							// flag: start at once or to be suspended
	DieCb	func(interface{}) SchErrno	// callbacked when going to die
	UserDa	interface{}				// user data area pointer
}

//
// Timer type
//
const (
	SchTmTypePeriod	= 0		// cycle timer
	SchTmTypeAbsolute	= 1	// absolute timer
)

type SchTimerType int

//
// Timer description
//
const SchMaxTaskTimer 	= 128	// max timers can be held by one user task
const SchInvalidTid		= -1	// invalid timer identity

type TimerDescription struct {
	Name	string			// timer name
	Utid	int				// user timer identity
	Tmt		SchTimerType	// timer type, see aboved
	Dur		time.Duration	// duration: a period value or duration from now
	Extra	interface{}		// extra data return to timer owner when expired
}

//
// Static user task description
//
type TaskStaticDescription struct {
	Name	string								// task name
	Tep		SchUserTaskEp						// task entry point
	MbSize	int									// mailbox size, if less than zero, default value applied
	Wd		SchWatchDog							// watchdog
	DieCb	func(task interface{}) SchErrno		// callbacked when going to die
	Flag	int									// flag: start at once or to be suspended
}

//
// Scheduler initilization
//
func SchinfSchedulerInit() SchErrno {
	return schimplSchedulerInit()
}

//
// Start scheduler
//
func SchinfSchedulerStart(tsd []TaskStaticDescription, tpo []string) (SchErrno, *map[string]interface{}){
	return schimplSchedulerStart(tsd, tpo)
}

//
// Create a single task
//
func SchinfCreateTask(taskDesc *SchTaskDescription)(SchErrno, interface{}) {
	return schimplCreateTask((*schTaskDescription)(taskDesc))
}

//
// Start a single task
//
func SchinfStartTask(name string) SchErrno {
	return schimplStartTask(name)
}

//
// Start task by task node pointer
//
func SchinfStartTaskEx(ptn interface{}) SchErrno {
	return schimplStartTaskEx(ptn.(*schTaskNode))
}

//
// Stop a single task by name
//
func SchinfStopTaskByName(name string) SchErrno {
	return schimplStopTask(name)
}

//
// Stop a single task by task node pointer
//
func SchinfStopTask(ptn interface{}) SchErrno {
	if eno := SchinfTaskDone(ptn.(*schTaskNode), SchEnoKilled); eno != SchEnoNone {
		yclog.LogCallerFileLine("SchinfStopTask: SchinfTaskDone failed, eno: %d", eno)
		return eno
	}
	return SchEnoNone
}

//
// Delete a single task
//
func SchinfDeleteTask(name string) SchErrno {
	return schimplDeleteTask(name)
}

//
// Get user task node pointer: the return type is a pointer a user task named by parameter
// "name" passed in. but the user caller need not to know details about the task node pointed
// by that pointer returned. any actions to the task should be carried out by calling funcs
// exported here in this file, which would call into the core of the scheduler.
//
func SchinfGetTaskNodeByName(name string) (eno SchErrno, task interface{}) {
	return schimplGetTaskNodeByName(name)
}

//
// Send message to a specific task
//
func SchinfSendMessageByName(dstTask string, srcTask string, msg *SchMessage) SchErrno {

	eno, src := SchinfGetTaskNodeByName(srcTask)
	if eno != SchEnoNone || src == nil {
		yclog.LogCallerFileLine("SchinfSendMessageByName: " +
			"SchinfGetTaskNodeByName failed, name: %s, eno: %d", srcTask, eno)
		return eno
	}

	eno, dst := SchinfGetTaskNodeByName(dstTask)
	if eno != SchEnoNone || dst == nil {
		yclog.LogCallerFileLine("SchinfSendMessageByName: " +
			"SchinfGetTaskNodeByName failed, name: %s, eno: %d", dstTask, eno)
		return eno
	}

	msg.sender = src.(*schTaskNode)
	msg.recver = dst.(*schTaskNode)

	return SchinfSendMessage(msg)
}

func SchinfSendMessage(msg *SchMessage) SchErrno {
	return schimplSendMsg((*schMessage)(msg))
}

//
// Make scheduling message
//
func SchinfMakeMessage(msg *SchMessage, s, r interface{}, id int, body interface{}) SchErrno {
	if msg == nil || s == nil || r == nil {
		yclog.LogCallerFileLine("SchinfMakeMessage: invalid parameter(s)")
		return SchEnoParameter
	}
	msg.sender = s.(*schTaskNode)
	msg.recver = r.(*schTaskNode)
	msg.Id = id
	msg.Body = body
	return SchEnoNone
}

//
// Send message to a specific task group
//
func SchinfSendMessageGroup(grp string, msg *SchMessage) (eno SchErrno, failedCount int) {
	return schimplSendMsg2TaskGroup(grp, (*schMessage)(msg))
}

//
// Set a timer
//
func SchInfSetTimer(ptn interface{}, tdc *TimerDescription) (eno SchErrno, tid int) {
	return schimplSetTimer(ptn.(*schTaskNode), (*timerDescription)(tdc))
}

//
// Kill a timer
//
func SchinfKillTimer(ptn interface{}, tid int) SchErrno {
	return schimplKillTimer(ptn.(*schTaskNode), tid)
}

//
// Done a task
//
func SchinfTaskDone(ptn interface{}, eno SchErrno) SchErrno {
	return schimplTaskDone(ptn.(*schTaskNode), eno)
}

//
// Get message sender
//
func SchinfGetMessageSender(msg *SchMessage) string {
	if msg == nil {
		return ""
	}
	return msg.sender.task.name
}

//
// Get message recevier
//
func SchinfGetMessageRecver(msg *SchMessage) string {
	if msg == nil {
		return ""
	}
	return msg.recver.task.name
}

//
// Get user data area pointer
//
func SchinfGetUserDataArea(ptn interface{}) interface{} {
	return schimplGetUserDataArea(ptn.(*schTaskNode))
}

//
// Set user data area pointer
//
func SchinfSetUserDataArea(ptn interface{}, uda interface{}) SchErrno {
	return schimplSetUserDataArea(ptn.(*schTaskNode), uda)
}

//
// Remove user data area pointer
//
func SchinfDelUserDataArea(ptn interface{}) SchErrno {
	return SchinfSetUserDataArea(ptn.(*schTaskNode), nil)
}

//
// Get task name
//
func SchinfGetTaskName(ptn interface{}) string {
	return schimplGetTaskName(ptn.(*schTaskNode))
}
