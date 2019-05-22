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
	"fmt"
	"sync"
	"time"

	config "github.com/yeeco/gyee/p2p/config"
	"github.com/yeeco/gyee/log"
)

// Scheduler interface errnos
type SchErrno int

const (
	SchEnoNone	SchErrno = iota  // none of errors
	SchEnoParameter // invalid parameters
	SchEnoResource // no resources
	SchEnoWatchDog // watch dog
	SchEnoNotFound // not found
	SchEnoInternal // internal errors
	SchEnoMismatched // mismatched
	SchEnoOS // operating system
	SchEnoConfig // configuration
	SchEnoKilled  // task killed
	SchEnoNotImpl // not implemented
	SchEnoUserTask // internal user task application
	SchEnoDuplicated // duplicated
	SchEnoSuspended // user task is suspended for some reasons
	SchEnoPowerOff // in power off stage
	SchEnoDone // done
	SchEnoTimeout // timeout
	SchEnoUnknown // unknowns
	SchEnoInvalid // just for bound checking
)

var SchErrnoDescription = []string{
	"SchEnoNone",
	"SchEnoParameter",
	"SchEnoResource",
	"SchEnoWatchDog",
	"SchEnoNotFound",
	"SchEnoInternal",
	"SchEnoMismatched",
	"SchEnoOS",
	"SchEnoConfig",
	"SchEnoKilled",
	"SchEnoNotImpl",
	"SchEnoUserTask",
	"SchEnoDuplicated",
	"SchEnoSuspended",
	"SchEnoPowerOff",
	"SchEnoDone",
	"SchEnoTimeout",
	"SchEnoUnknown",
	"SchEnoInvalid",
}

// Errno string
func (eno SchErrno) SchErrnoString() string {
	if eno < SchEnoNone || eno >= SchEnoInvalid {
		return fmt.Sprintf("invalid scheduler errno: %d", eno)
	}
	return SchErrnoDescription[eno]
}

// error interface
func (eno SchErrno) Error() string {
	return eno.SchErrnoString()
}

// Export scheduler type
type Scheduler = scheduler

// User task entry point: notice, parameter ptn would be type of pointer to schTaskNode,
// the user task should never try to access the field directly, instead, interface func
// provide by scheduler module should by applied. for example, when user task try to set
// a timer, it should then pass this ptn to function SchSetTimer, see it pls. Also,
// user task should try to interpret the msg.body by msg.id, which is defined by user
// task than scheduler itself, of course, timer event is an exception.
type SchUserTaskEp = func(ptn interface{}, msg *SchMessage) SchErrno

// User task inteface for scheduler
type SchUserTaskInterface interface {
	TaskProc4Scheduler(ptn interface{}, msg *SchMessage) SchErrno
}

// message type for scheduling between user tasks
type SchMsgSendCallback func(errno SchErrno)
const (
	SchMsgKeepFromNone = 0
	SchMsgKeepFromPoweroff	= 1
	SchMsgKeepFromDone = 2
)
type SchMessage struct {
	sender  *schTaskNode		// sender task node pointer
	recver  *schTaskNode		// receiver task node pointer
	Id      int          		// message identity
	Body    interface{}  		// message body

	// callback for sending result.
	// notice: the user task should try to complete its' work
	// for message sending result as fast as it could.
	Mscb    SchMsgSendCallback
	TgtName	string				// target receiver task name
	Keep	int					// keep even in power off stage
}

// Watch dog for a user task
const (
	SchDeaultWatchCycle      = time.Second
	SchDefaultDogCycle       = time.Second
	SchDefaultDogDieThresold = 2
)

type SchWatchDog struct {
	lock         sync.Mutex
	HaveDog      bool          // if dog would come out
	Inited       bool          // dog initialized
	Cycle        time.Duration // feed cycle expected, must be times of second
	BiteCounter  int           // counter for a user task to be bitten by dog
	DieThreshold int           // threshold counter of dog-bited to die
}

// Flag for user just be created
const (
	SchCreatedNull = iota	 // not created
	SchCreatedGo			 // go at once
	SchCreatedSuspend        // suspended
)

// Max mail box size: notice that it's the real depth of queue implemented as a channel,
// when MbSize is set to (-1) for a task. In extreme case when system load is very heavy,
// the queue might be full so tasks can be blocked in sending messages.
const SchDftMbSize = 1024 * (1)
const SchMaxMbSize = 1024 * (256)

type SchTaskDescription struct {
	Name   string                     // user task name
	MbSize int                        // mailbox size
	Ep     SchUserTaskInterface       // user task entry point
	Wd     *SchWatchDog               // watchdog
	Flag   int                        // flag: start at once or to be suspended
	DieCb  func(interface{}) SchErrno // callbacked when going to die
	UserDa interface{}                // user data area pointer
}

// Timer type
const (
	SchTmTypePeriod   = 0 // cycle timer
	SchTmTypeAbsolute = 1 // absolute timer
)

type SchTimerType int

// Timer description
const SchMaxTaskTimer = 1024 // max timers can be held by one user task
const SchInvalidTid = -1    // invalid timer identity
type TimerDescription struct {
	Name  string        // timer name
	Utid  int           // user timer identity
	Tmt   SchTimerType  // timer type, see aboved
	Dur   time.Duration // duration: a period value or duration from now
	Extra interface{}   // extra data return to timer owner when expired
}

// Static user task description
type TaskStaticDescription struct {
	Name string               // task name
	Tep  SchUserTaskInterface // task inteface, it's the user control block which
	// exports its' entry point
	MbSize int                             // mailbox size, if less than zero, default value applied
	Wd     SchWatchDog                     // watchdog
	DieCb  func(task interface{}) SchErrno // callbacked when going to die
	Flag   int                             // flag: start at once or to be suspended
}

// Scheduler init
func SchSchedulerInit(cfg *config.Config) (*Scheduler, SchErrno) {
	return schSchedulerInit(cfg)
}

// Start scheduler
func (sdl *Scheduler) SchSchedulerStart(
	tsd []TaskStaticDescription,
	tpo []string) (SchErrno, *map[string]interface{}) {
	return sdl.schSchedulerStart(tsd, tpo)
}

// Create a single task
func (sdl *Scheduler) SchCreateTask(taskDesc *SchTaskDescription) (SchErrno, interface{}) {
	return sdl.schCreateTask((*schTaskDescription)(taskDesc))
}

// Start task by task node pointer
func (sdl *Scheduler) SchStartTaskEx(ptn interface{}) SchErrno {
	return sdl.schStartTaskEx(ptn.(*schTaskNode))
}

// Stop caller task or other task in async
func (sdl *Scheduler) SchStopTask(ptn interface{}, name string) SchErrno {
	if eno := sdl.SchTaskDone(ptn.(*schTaskNode), name, SchEnoKilled); eno != SchEnoNone {
		log.Debugf("SchStopTask: SchTaskDone failed, eno: %d", eno)
		return eno
	}
	return SchEnoNone
}

// Stop other task than the caller in sync
func (sdl *Scheduler) SchStopTaskSync(ptn interface{}, name string) SchErrno {
	return sdl.schStopTask(ptn.(*schTaskNode), name)
}

// Get user task node pointer
func (sdl *Scheduler) SchGetUserTaskNode(name string) (eno SchErrno, task interface{}) {
	return sdl.schGetTaskNodeByName(name)
}

// Send message to a specific task
func (sdl *Scheduler) SchSendMessageByName(dstTask string, srcTask string, msg *SchMessage) SchErrno {
	result := SchEnoNone
	eno := SchEnoNone
	src := interface{}(nil)
	dst := interface{}(nil)

	if eno, src = sdl.SchGetUserTaskNode(srcTask); eno != SchEnoNone {
		result = eno
		goto _failed
	}
	if src == nil {
		result = SchEnoInternal
		goto _failed
	}
	if eno, dst = sdl.SchGetUserTaskNode(dstTask); eno != SchEnoNone {
		result = eno
		goto _failed
	}
	if dst == nil {
		result = SchEnoInternal
		goto _failed
	}

	msg.sender = src.(*schTaskNode)
	msg.recver = dst.(*schTaskNode)
	result = sdl.SchSendMessage(msg)

_failed:
	if msg.Mscb != nil {
		msg.Mscb(result)
	}
	return result
}

func (sdl *Scheduler) SchSendMessage(msg *SchMessage) SchErrno {
	// to ensure the message not be modified by the caller after calling
	// this function, we make a copy.
	_msg := *msg
	return sdl.schSendMsg((*schMessage)(&_msg))
}

// Set sender of message
func (sdl *Scheduler) SchSetSender(msg *SchMessage, sender interface{}) SchErrno {
	msg.sender = sender.(*schTaskNode)
	return SchEnoNone
}

// Get sender of message
func (sdl *Scheduler) SchGetSender(msg *SchMessage) interface{} {
	return msg.sender
}

// Set receiver of message
func (sdl *Scheduler) SchSetRecver(msg *SchMessage, recver interface{}) SchErrno {
	msg.recver = recver.(*schTaskNode)
	return SchEnoNone
}

// Get receiver of message
func (sdl *Scheduler) SchGetRecver(msg *SchMessage) interface{} {
	return msg.recver
}

// Make scheduling message
func (sdl *Scheduler) SchMakeMessage(msg *SchMessage, s, r interface{}, id int, body interface{}) SchErrno {
	// notice: the "body" is a reference to message body, so one should not try
	// to access the body again after this function called, or the body will be
	// modified(corrupted).
	if msg == nil || s == nil || r == nil {
		log.Debugf("SchMakeMessage: invalid message, sdl: %s", sdl.p2pCfg.CfgName)
		return SchEnoParameter
	}
	msg.sender = s.(*schTaskNode)
	msg.recver = r.(*schTaskNode)
	msg.Id = id
	msg.Body = body
	return SchEnoNone
}

// Set timer
func (sdl *Scheduler) SchSetTimer(ptn interface{}, tdc *TimerDescription) (eno SchErrno, tid int) {
	return sdl.schSetTimer(ptn.(*schTaskNode), (*timerDescription)(tdc))
}

// Kill timer
func (sdl *Scheduler) SchKillTimer(ptn interface{}, tid int) SchErrno {
	return sdl.schKillTimer(ptn.(*schTaskNode), tid)
}

// Done caller task or other task in async
func (sdl *Scheduler) SchTaskDone(ptn interface{}, name string, eno SchErrno) SchErrno {
	return sdl.schTaskDone(ptn.(*schTaskNode), name, eno)
}

// Get scheduler by task node
func SchGetScheduler(ptn interface{}) *Scheduler {
	return ptn.(*schTaskNode).task.sdl
}

// Get user task interface exported to scheduler
func (sdl *Scheduler) SchGetTaskObject(tn string) interface{} {
	eno, ptn := sdl.SchGetUserTaskNode(tn)
	if eno != SchEnoNone {
		return nil
	}
	return ptn.(*schTaskNode).task.utep
}

// Get user data area pointer
func (sdl *Scheduler) SchGetUserDataArea(ptn interface{}) interface{} {
	return sdl.schGetUserDataArea(ptn.(*schTaskNode))
}

// Set user data area pointer
func (sdl *Scheduler) SchSetUserDataArea(ptn interface{}, uda interface{}) SchErrno {
	return sdl.schSetUserDataArea(ptn.(*schTaskNode), uda)
}

// Set the power off stage flag to tell the scheduler it's going to be turn off
func (sdl *Scheduler) SchSetPoweroffStage() SchErrno {
	log.Debugf("SchSetPoweroffStage: prepare to power off, sdl: %s", sdl.p2pCfg.Name)
	return sdl.schSetPoweroffStage()
}

// get the power off stage flag
func (sdl *Scheduler) SchGetPoweroffStage() bool {
	return sdl.schGetPoweroffStage()
}

// Get task name
func (sdl *Scheduler) SchGetTaskName(ptn interface{}) string {
	return sdl.schGetTaskName(ptn.(*schTaskNode))
}

// Get task number
func (sdl *Scheduler) SchGetTaskNumber() int {
	return sdl.schGetTaskNumber()
}

// Show names about alived tasks
func (sdl *Scheduler) SchShowTaskName() []string {
	return sdl.schShowTaskName()
}

// Test if task exist with specific name
func (sdl *Scheduler) SchTaskExist(name string) bool {
	eno, ptn := sdl.SchGetUserTaskNode(name)
	return eno == SchEnoNone && ptn != nil
}

// Get p2p network instance node name
func (sdl *Scheduler) SchGetP2pNodeName() string {
	return sdl.p2pCfg.Name
}

// Get p2p network configuration name
func (sdl *Scheduler) SchGetP2pCfgName() string {
	return sdl.p2pCfg.CfgName
}

// Get p2p configuration
func (sdl *Scheduler) SchGetP2pConfig() *config.Config {
	return sdl.p2pCfg
}

// Set application type
func (sdl *Scheduler) SchSetAppType(appType int) SchErrno {
	sdl.appType = appType
	return SchEnoNone
}

// Get application type
func (sdl *Scheduler) SchGetAppType() int {
	return sdl.appType
}

//
// Get task mailbox capacity
//
func (sdl *scheduler) SchGetTaskMailboxCapacity(ptn interface{}) int {
	return sdl.schGetTaskMailboxCapicity(ptn.(*schTaskNode))
}

//
// Get task mailbox space
//
func (sdl *scheduler) SchGetTaskMailboxSpace(ptn interface{}) int {
	return sdl.schGetTaskMailboxSpace(ptn.(*schTaskNode))
}
