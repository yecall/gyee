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
	"sync"
	config	"github.com/yeeco/gyee/p2p/config"
	log		"github.com/yeeco/gyee/p2p/logger"
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
	"resources",
	"watch dog",
	"not found",
	"internal",
	"mismathced",
	"os",
	"configuration",
	"killed",
	"not implemented",
	"user task",
	"duplicated",
	"suspended",
	"not supported",
	"unknowns",
	"max value errno can be",
}

//
// Errno string
//
func (eno SchErrno) SchErrnoString() string {
	if eno < SchEnoNone || eno >= SchEnoMax {
		return fmt.Sprintf("Can't be stringzed, invalid eno:%d", eno)
	}
	return SchErrnoDescription[eno]
}

//
// error interface
//
func (eno SchErrno) Error() string {
	return eno.SchErrnoString()
}

//
// Export scheduler type
//
type Scheduler = scheduler

//
// User task entry point: notice, parameter ptn would be type of pointer to schTaskNode,
// the user task should never try to access the field directly, instead, interface func
// provide by scheduler module should by applied. for example, when user task try to set
// a timer, it should then pass this ptn to function SchSetTimer, see it pls. Also,
// user task should try to interpret the msg.body by msg.id, which is defined by user
// task than scheduler itself, of course, timer event is an exception.
//
type SchUserTaskEp = func(ptn interface{}, msg *SchMessage) SchErrno

//
// User task inteface for scheduler
//
type SchUserTaskInf interface {
	TaskProc4Scheduler(ptn interface{}, msg *SchMessage) SchErrno
}

//
// message type for scheduling between user tasks
//
type SchMessage struct {
	sender 	*schTaskNode	// sender task node pointer
	recver	*schTaskNode	// receiver task node pointer
	Id		int				// message identity
	Body	interface{}		// message body
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
// max mail box size
//
const SchMaxMbSize	 = 256

type SchTaskDescription struct {
	Name	string						// user task name
	MbSize	int							// mailbox size
	Ep		SchUserTaskInf				// user task entry point
	Wd		*SchWatchDog				// watchdog
	Flag	int							// flag: start at once or to be suspended
	DieCb	func(interface{}) SchErrno	// callbacked when going to die
	UserDa	interface{}					// user data area pointer
}

//
// Timer type
//
const (
	SchTmTypePeriod		= 0		// cycle timer
	SchTmTypeAbsolute	= 1		// absolute timer
)

type SchTimerType int

//
// Timer description
//
const SchMaxTaskTimer 	= 128	// max timers can be held by one user task
const SchInvalidTid		= -1	// invalid timer identity

type TimerDescription struct {
	Name	string				// timer name
	Utid	int					// user timer identity
	Tmt		SchTimerType		// timer type, see aboved
	Dur		time.Duration		// duration: a period value or duration from now
	Extra	interface{}			// extra data return to timer owner when expired
}

//
// Static user task description
//
type TaskStaticDescription struct {
	Name	string								// task name
	Tep		SchUserTaskInf						// task inteface, it's the user control block which exports its' entry point
	MbSize	int									// mailbox size, if less than zero, default value applied
	Wd		SchWatchDog							// watchdog
	DieCb	func(task interface{}) SchErrno		// callbacked when going to die
	Flag	int									// flag: start at once or to be suspended
}

//
// Scheduler init
//
func SchSchedulerInit(cfg *config.Config) (*Scheduler, SchErrno) {
	return schSchedulerInit(cfg)
}

//
// Start scheduler
//
func (sdl *Scheduler)SchSchedulerStart(tsd []TaskStaticDescription, tpo []string) (SchErrno, *map[string]interface{}){
	return sdl.schSchedulerStart(tsd, tpo)
}

//
// Create a single task
//
func (sdl *Scheduler)SchCreateTask(taskDesc *SchTaskDescription)(SchErrno, interface{}) {
	return sdl.schCreateTask((*schTaskDescription)(taskDesc))
}

//
// Start task by task node pointer
//
func (sdl *Scheduler)SchStartTaskEx(ptn interface{}) SchErrno {
	return sdl.schStartTaskEx(ptn.(*schTaskNode))
}

//
// Stop a single task by task node pointer
//
func (sdl *Scheduler)SchStopTask(ptn interface{}) SchErrno {
	if eno := sdl.SchTaskDone(ptn.(*schTaskNode), SchEnoKilled); eno != SchEnoNone {
		log.LogCallerFileLine("SchStopTask: SchTaskDone failed, eno: %d", eno)
		return eno
	}
	return SchEnoNone
}

//
// Get user task node pointer: the return type is a pointer a user task named by parameter
// "name" passed in. but the user caller need not to know details about the task node pointed
// by that pointer returned. any actions to the task should be carried out by calling funcs
// exported here in this file, which would call into the core of the scheduler.
//
func (sdl *Scheduler)SchGetTaskNodeByName(name string) (eno SchErrno, task interface{}) {
	return sdl.schGetTaskNodeByName(name)
}

//
// Send message to a specific task
//
func (sdl *Scheduler)SchSendMessageByName(dstTask string, srcTask string, msg *SchMessage) SchErrno {

	eno, src := sdl.SchGetTaskNodeByName(srcTask)
	if eno != SchEnoNone || src == nil {
		return eno
	}

	eno, dst := sdl.SchGetTaskNodeByName(dstTask)
	if eno != SchEnoNone || dst == nil {
		return eno
	}

	msg.sender = src.(*schTaskNode)
	msg.recver = dst.(*schTaskNode)

	return sdl.SchSendMessage(msg)
}

func (sdl *Scheduler)SchSendMessage(msg *SchMessage) SchErrno {
	return sdl.schSendMsg((*schMessage)(msg))
}

//
// Make scheduling message
//
func (sdl *Scheduler)SchMakeMessage(msg *SchMessage, s, r interface{}, id int, body interface{}) SchErrno {

	if msg == nil || s == nil || r == nil {
		return SchEnoParameter
	}

	msg.sender = s.(*schTaskNode)
	msg.recver = r.(*schTaskNode)
	msg.Id = id
	msg.Body = body

	return SchEnoNone
}

//
// Set a timer
//
func (sdl *Scheduler)SchSetTimer(ptn interface{}, tdc *TimerDescription) (eno SchErrno, tid int) {
	return sdl.schSetTimer(ptn.(*schTaskNode), (*timerDescription)(tdc))
}

//
// Kill a timer
//
func (sdl *Scheduler)SchKillTimer(ptn interface{}, tid int) SchErrno {
	return sdl.schKillTimer(ptn.(*schTaskNode), tid)
}

//
// Done a task
//
func (sdl *Scheduler)SchTaskDone(ptn interface{}, eno SchErrno) SchErrno {
	return sdl.schTaskDone(ptn.(*schTaskNode), eno)
}

//
// Get scheduler by task node
//
func SchGetScheduler(ptn interface{}) *Scheduler {
	return ptn.(*schTaskNode).task.sdl
}

//
// Get user task interface exported to scheduler
//
func (sdl *Scheduler)SchGetUserTaskIF(tn string) interface{} {
	eno, ptn := sdl.SchGetTaskNodeByName(tn)
	if eno != SchEnoNone {
		return nil
	}
	return ptn.(*schTaskNode).task.utep
}

//
// Get user data area pointer
//
func (sdl *Scheduler)SchGetUserDataArea(ptn interface{}) interface{} {
	return sdl.schGetUserDataArea(ptn.(*schTaskNode))
}

//
// Set user data area pointer
//
func (sdl *Scheduler)SchSetUserDataArea(ptn interface{}, uda interface{}) SchErrno {
	return sdl.schSetUserDataArea(ptn.(*schTaskNode), uda)
}

//
// Get task name
//
func (sdl *Scheduler)SchGetTaskName(ptn interface{}) string {
	return sdl.schGetTaskName(ptn.(*schTaskNode))
}

//
// Get p2p network configuration name
//
func (sdl *Scheduler)SchGetP2pCfgName() string {
	return sdl.p2pCfg.CfgName
}
