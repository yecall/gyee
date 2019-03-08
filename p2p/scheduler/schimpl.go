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
	"time"
	"strings"
	"runtime"
	"fmt"
	config	"github.com/yeeco/gyee/p2p/config"
	p2plog	"github.com/yeeco/gyee/p2p/logger"
	"sync"
)


//
// debug
//
type schLogger struct {
	debug__		bool
}

var schLog = schLogger  {
	debug__:	false,
}

func (log schLogger)Debug(fmt string, args ... interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args ...)
	}
}

//
// Pseudo task node for external module to send event
//
var PseudoSchTsk = schTaskNode {
	task: schTask{name:"pseudoSchTsk"},
	last: nil,
	next: nil,
}

//
// Default task node for scheduler to send event
//
const rawSchTaskName = "schTsk"
const RawSchTaskName = rawSchTaskName
const mbReserved = 10

var rawSchTsk = schTaskNode {
	task: schTask{name:rawSchTaskName,},
	last: nil,
	next: nil,
}

var RawSchTask = rawSchTsk

//
// Default task node for scheduler to send timer event
//
const rawTmTaskName = "tmTsk"

var rawTmTsk = schTaskNode {
	task: schTask{name:rawTmTaskName,},
	last: nil,
	next: nil,
}

//
// Scheduler initialization
//
func schSchedulerInit(cfg *config.Config) (*scheduler, SchErrno) {

	var sdl = new(scheduler)

	//
	// backup p2p network configuration
	//

	sdl.p2pCfg = cfg
	sdl.powerOff = false
	sdl.appType = int(cfg.AppType)

	//
	// make maps
	//

	sdl.tkMap = make(map[string] *schTaskNode)

	//
	// setup free task node queue
	//

	for loop := 0; loop < schTaskNodePoolSize; loop++ {
		sdl.schTaskNodePool[loop].last = &sdl.schTaskNodePool[(loop - 1 + schTaskNodePoolSize) & (schTaskNodePoolSize - 1)]
		sdl.schTaskNodePool[loop].next = &sdl.schTaskNodePool[(loop + 1) & (schTaskNodePoolSize - 1)]
		sdl.schTaskNodePool[loop].task.tmIdxTab = make(map[*schTmcbNode] int)
	}

	sdl.freeSize = schTaskNodePoolSize
	sdl.tkFree = &sdl.schTaskNodePool[0]

	//
	// setup free timer node queue
	//

	for loop := 0; loop < schTimerNodePoolSize; loop++ {
		sdl.schTimerNodePool[loop].last = &sdl.schTimerNodePool[(loop - 1 + schTimerNodePoolSize) & (schTimerNodePoolSize - 1)]
		sdl.schTimerNodePool[loop].next = &sdl.schTimerNodePool[(loop + 1) & (schTimerNodePoolSize - 1)]
		sdl.schTimerNodePool[loop].tmcb.stop = make(chan bool, 1)
		sdl.schTimerNodePool[loop].tmcb.stopped = make(chan bool)
	}

	sdl.tmFreeSize = schTimerNodePoolSize
	sdl.tmFree = &sdl.schTimerNodePool[0]

	return sdl, SchEnoNone
}

//
// the common entry point for a scheduler task
//
func (sdl *scheduler)schCommonTask(ptn *schTaskNode) SchErrno {

	//
	// check pointer to task node
	//

	if ptn == nil {
		schLog.Debug("schCommonTask: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// check user task more
	//

	if ptn.task.utep == nil || ptn.task.mailbox.que == nil || ptn.task.done == nil {

		schLog.Debug("schCommonTask: " +
			"invalid user task: %s",
			ptn.task.name)

		return SchEnoParameter
	}

	if len(ptn.task.name) == 0 {
		panic("schCommonTask: task must have name")
		return SchEnoParameter
	}

	task := ptn.task
	mailbox := ptn.task.mailbox
	queMsg := ptn.task.mailbox.que
	done := &ptn.task.done
	proc := ptn.task.utep.TaskProc4Scheduler

	//
	// loop to schedule, until done(or something else happened).
	// notice: if the message queue size of one user task is zero, then this means
	// the user task would be a longlong loop. We simply start a routine for this
	// case and wait it's done.
	//

	if cap(*mailbox.que) <= 0 {

		if schLog.debug__ {
			schLog.Debug("schCommonTask: longlong loop user task: %s",
				task.name)
		}

		go proc(ptn, nil)

		why := <-*done

		if schLog.debug__ {
			schLog.Debug("schCommonTask: sdl: %s, done with: %d, task: %s",
				sdl.p2pCfg.CfgName, why, ptn.task.name)
		}

		goto taskDone
	}

	//
	// go a routine to check "done" signal
	//

	go func () {

		why := <-*done

		if schLog.debug__ {
			schLog.Debug("schCommonTask: sdl: %s, done with: %d, task: %s",
				sdl.p2pCfg.CfgName, why, ptn.task.name)
		}

		//
		// drain possible pending messages and send EvSchDone if the task done
		// with a mailbox.
		//

		if cap(*mailbox.que) > 0 {

			doneInd := MsgTaskDone{
				why: why,
			}

			var msg = schMessage{
				sender: &rawSchTsk,
				recver: ptn,
				Id:     EvSchDone,
				Body:   &doneInd,
			}

drainLoop1:

			for {
				select {
				case <-*queMsg:
				default:
					break drainLoop1
				}
			}

			sdl.schSendMsg(&msg)
		}
	}()

	//
	// loop task messages until done
	//

taskLoop:

	for {

		//
		// check power off stage
		//

		var msg schMessage

		if sdl.powerOff == true {

			//
			// drain until EvSchDone or EvSchPoweroff event met
			//

drainLoop2:

			for {

				msg = <-*queMsg

				if msg.Id == EvSchDone || msg.Id == EvSchPoweroff {
					break drainLoop2
				}
			}

		} else {

			//
			// get one message
			//

			msg = <-*queMsg
		}

		//
		// check "done" to break task loop
		//

		if msg.Id == EvSchDone {

			doneInd := msg.Body.(*MsgTaskDone)

			if schLog.debug__ {
				schLog.Debug("schCommonTask: sdl: %s, done with eno: %d, task: %s",
					sdl.p2pCfg.CfgName, doneInd.why, ptn.task.name)
			}

			break taskLoop
		}

		//
		// call handler, but if in power off stage, we discard all except EvSchPoweroff
		//

		if (sdl.powerOff == false) || (sdl.powerOff == true && msg.Id == EvSchPoweroff) {

			proc(ptn, (*SchMessage)(&msg))

		}
	}

	//
	// see function: schTaskDone, schStopTask... for more pls
	//

taskDone:
	ptn.task.stopped<-true

	//
	// remove the name to task node pointer map, so this task would not be found in
	// the owner scheduler scope from now on.
	//

	schLog.Debug("schCommonTask: clean task map, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno := sdl.schTaskDemap(ptn.task.name); eno != SchEnoNone {

		panic(fmt.Sprintf("schCommonTasks: " +
			"schTaskDemap failed, eno: %d, task: %s",
			eno, ptn.task.name))

		return eno
	}

	//
	// dequeue form busy queue
	//

	schLog.Debug("schCommonTask: deque task from busy queue, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno := sdl.schTaskBusyDeque(ptn); eno != SchEnoNone {

		panic(fmt.Sprintf("schCommonTasks: " +
			"schTaskBusyDeque failed, eno: %d, task: %s",
			eno, ptn.task.name))
	}

	//
	// remove user task resources
	//

	schLog.Debug("schCommonTask: post done process, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno := sdl.schStopTaskEx(ptn); eno != SchEnoNone {

		panic(fmt.Sprintf("schCommonTasks: " +
			"schStopTaskEx failed, eno: %d, task: %s",
			eno, ptn.task.name))
	}

	//
	// free task node
	//

	schLog.Debug("schCommonTask: free task node, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno := sdl.schRetTaskNode(ptn); eno != SchEnoNone {

		panic(fmt.Sprintf("schCommonTask: " +
			"schRetTaskNode failed, eno: %d, task: %s",
			eno, ptn.task.name))
	}

	return SchEnoNone
}

//
// the common entry point for timer task
//
func (sdl *scheduler)schTimerCommonTask(ptm *schTmcbNode) SchErrno {

	var tk *time.Ticker
	var tm *time.Timer
	var killed = false
	var task = &ptm.tmcb.taskNode.task

	//
	// get timer identity
	//

	task.lock.Lock()
	var tid = task.tmIdxTab[ptm]
	task.lock.Unlock()

	//
	// cleaner for absolute timer when it expired
	//

	var absTimerClean = func (tn *schTmcbNode) {

		//
		// clear timer control block and remove it from maps, notice that the task
		// node should not be released here, it's accessed later after this function
		// called; and do not ret the timer control block node here.
		//

		delete(task.tmIdxTab, tn)
		task.tmTab[tid] = nil

		tn.tmcb.name	= ""
		tn.tmcb.tmt		= schTmTypeNull
		tn.tmcb.dur		= 0
		tn.tmcb.extra	= nil
	}

	//
	// cleaning job for cyclic timers are the same as those absolute ones
	//

	var cycTimerClean = absTimerClean

	//
	// check timer type to deal with it
	//

	if ptm.tmcb.tmt == schTmTypePeriod {

		tk = time.NewTicker(ptm.tmcb.dur)

		//
		// go routine to check timer killed
		//

		var to = make(chan int)

		go func() {
			_check_loop_p:
			for {
				select {
				case stop := <-ptm.tmcb.stop:
					if stop {
						to <- EvSchDone
						break _check_loop_p
					}
				case <-tk.C:
					to<-EvTimerBase
				}
			}
		}()


		//
		// loop for ever until killed
		//

timerLoop:

		for {

			event := <-to

			//
			// check if timer killed
			//

			if event == EvSchDone {

				schLog.Debug("schTimerCommonTask: EvSchDone, timer: %s, task: %s",
					ptm.tmcb.name,
					task.name)

				task.lock.Lock()

				killed = true
				tk.Stop()

				cycTimerClean(ptm)

				break timerLoop
			}

			//
			// must be timer expired
			//

			if event == EvTimerBase {

				task.lock.Lock()

				if eno := sdl.schSendTimerEvent(ptm); eno != SchEnoNone  && eno != SchEnoPowerOff {

					schLog.Debug("schTimerCommonTask: " +
						"send timer event failed, eno: %d, task: %s",
						eno,
						ptm.tmcb.taskNode.task.name)
				}

				task.lock.Unlock()
				continue
			}

			panic(fmt.Sprintf("schTimerCommonTask: internal errors, event: %d", event))
		}

	} else if ptm.tmcb.tmt == schTmTypeAbsolute {

		//
		// absolute, check duration
		//

		dur := ptm.tmcb.dur
		if dur <= time.Duration(0) {

			schLog.Debug("schTimerCommonTask: " +
				"invalid absolute timer duration:%d",
				ptm.tmcb.dur)

			return SchEnoParameter
		}

		//
		// send timer event after duration specified. we could not call time.After
		// directly, or we will blocked until timer expired, go a routine instead.
		//

		var to = make(chan int)
		tm = time.NewTimer(dur)

		go func() {
			_check_loop_a:
			for {
				select {
				case stop := <-ptm.tmcb.stop:
					if stop {
						to<-EvSchDone
						break _check_loop_a
					}
				case <-tm.C:
					to<-EvTimerBase
					break _check_loop_a
				}
			}
			tm.Stop()
		}()

		//
		// handle timer events or done
		//

absTimerLoop:

		for {

			event := <-to

			if event == EvTimerBase {

				task.lock.Lock()

				if eno := sdl.schSendTimerEvent(ptm); eno != SchEnoNone {

					schLog.Debug("schTimerCommonTask: "+
						"send timer event failed, eno: %d, task: %s",
						eno,
						ptm.tmcb.taskNode.task.name)
				}

				absTimerClean(ptm)

				break absTimerLoop

			} else if event == EvSchDone {

				schLog.Debug("schTimerCommonTask: EvSchDone, timer: %s, task: %s",
					ptm.tmcb.name,
					task.name)

				task.lock.Lock()

				absTimerClean(ptm)
				killed = true

				break absTimerLoop
			}

			panic(fmt.Sprintf("schTimerCommonTask: internal errors, event: %d", event))
		}

	} else {

		//
		// unknown
		//

		schLog.Debug("schTimerCommonTask: " +
			"invalid timer type: %d",
			ptm.tmcb.tmt)

		return SchEnoParameter
	}

	//
	// exit, notice that here task is still locked, and only when killed we
	// need to feed the "stopped"
	//

	if killed {

		ptm.tmcb.stopped<-true
	}

	// notice: here the timer owner task might be blocked in function schKillTimer
	// (if it's called), for waiting "stopped": the action "kill" and the event
	// "expirted" happened at the "same" time, but "expired" is selected, so "close"
	// for "stopped" is needed.

	close(ptm.tmcb.stop)
	close(ptm.tmcb.stopped)

	ptm.tmcb.taskNode = nil
	task.lock.Unlock()

	if eno := sdl.schRetTimerNode(ptm); eno != SchEnoNone {
		panic(fmt.Sprintf("schTimerCommonTask: schRetTimerNode failed, eno: %d", eno))
	}

	return SchEnoNone
}

//
// Get timer node
//
func (sdl *scheduler)schGetTimerNode() (SchErrno, *schTmcbNode) {

	var tmn *schTmcbNode = nil

	sdl.lock.Lock()
	defer sdl.lock.Unlock()

	if sdl.tmFree == nil {
		schLog.Debug("schGetTimerNode: free queue is empty")
		return SchEnoResource, nil
	}

	tmn = sdl.tmFree

	if tmn.last == tmn && tmn.next == tmn {

		if sdl.tmFreeSize - 1 != 0 {

			schLog.Debug("schGetTimerNode: " +
				"internal errors, should be 0, but free size: %d",
				sdl.tmFreeSize)

			return SchEnoInternal, nil
		}

		sdl.tmFreeSize--
		sdl.tmFree = nil

	} else {

		if sdl.tmFreeSize - 1 <= 0 {

			schLog.Debug("schGetTimerNode: " +
				"internal errors, should equal or less than 0, but free size: %d",
				sdl.tmFreeSize)

			return SchEnoInternal, nil
		}

		last := tmn.last
		next := tmn.next
		next.last = last
		last.next = next

		sdl.tmFree = next
		sdl.tmFreeSize--
	}

	tmn.next = tmn
	tmn.last = tmn

	return SchEnoNone, tmn
}

//
// Ret timer node to free queue
//
func (sdl *scheduler)schRetTimerNode(ptm *schTmcbNode) SchErrno {

	sdl.lock.Lock()
	defer  sdl.lock.Unlock()

	if sdl.tmFree == nil {

		ptm.last = ptm
		ptm.next = ptm

	} else {

		last := sdl.tmFree.last
		ptm.last = last
		last.next = ptm
		ptm.next = sdl.tmFree
		sdl.tmFree.last = ptm

	}

	sdl.tmFree = ptm
	sdl.tmFreeSize++

	return SchEnoNone
}

//
// Get task node
//
func (sdl *scheduler)schGetTaskNode() (SchErrno, *schTaskNode) {

	var tkn *schTaskNode = nil

	sdl.lock.Lock()
	defer sdl.lock.Unlock()

	if sdl.tkFree== nil {
		schLog.Debug("schGetTaskNode: free queue is empty")
		return SchEnoResource, nil
	}

	tkn = sdl.tkFree

	if tkn.last == tkn && tkn.next == tkn {

		sdl.tkFree = nil

		if sdl.freeSize--; sdl.freeSize != 0 {

			schLog.Debug("schGetTaskNode: internal errors")

			return SchEnoInternal, nil
		}

	} else {

		last := tkn.last
		next := tkn.next
		next.last = last
		last.next = next
		sdl.tkFree = next

		if sdl.freeSize--; sdl.freeSize <= 0 {

			schLog.Debug("schGetTaskNode: internal errors")
			return SchEnoInternal, nil
		}
	}

	tkn.next = tkn
	tkn.last = tkn

	return SchEnoNone, tkn
}

//
// Ret task node
//
func (sdl *scheduler)schRetTaskNode(ptn *schTaskNode) SchErrno {

	sdl.lock.Lock()
	defer  sdl.lock.Unlock()

	if sdl.tkFree == nil {

		ptn.last = ptn
		ptn.next = ptn

	} else {

		last := sdl.tkFree.last
		ptn.last = last
		last.next = ptn
		ptn.next = sdl.tkFree
		sdl.tkFree.last = ptn

	}

	sdl.tkFree = ptn
	sdl.freeSize++

	return SchEnoNone
}

//
// Task node enter the busy queue
//
func (sdl *scheduler)schTaskBusyEnque(ptn *schTaskNode) SchErrno {
	
	sdl.lock.Lock()
	defer  sdl.lock.Unlock()

	if sdl.tkBusy == nil {

		ptn.last = ptn
		ptn.next = ptn

	} else {

		last := sdl.tkBusy.last
		ptn.last = last
		last.next = ptn
		ptn.next = sdl.tkBusy
		sdl.tkBusy.last = ptn

	}

	sdl.tkBusy = ptn
	sdl.busySize++

	return SchEnoNone
}

//
// Task node dequeue from the busy queue
//
func (sdl *scheduler)schTaskBusyDeque(ptn *schTaskNode) SchErrno {

	sdl.lock.Lock()
	defer sdl.lock.Unlock()

	if sdl.busySize <= 0 {

		schLog.Debug("schTaskBusyDeque: invalid parameter")
		return SchEnoInternal

	} else if sdl.busySize == 1 {

		if sdl.tkBusy != ptn {

			schLog.Debug("schTaskBusyDeque: invalid parameter")
			return SchEnoInternal

		} else {

			sdl.tkBusy = nil
			sdl.busySize = 0
			return SchEnoNone
		}
	}

	if ptn.last == ptn && ptn.next == ptn {

		if ptn == sdl.tkBusy {

			sdl.tkBusy = nil

		} else {

			schLog.Debug("schTaskBusyDeque: internal errors")

			return SchEnoInternal
		}

		if sdl.busySize--; sdl.busySize != 0 {

			schLog.Debug("schTaskBusyDeque: internal errors")

			return SchEnoInternal
		}
	} else {

		last := ptn.last
		next := ptn.next
		last.next = next
		next.last = last

		if sdl.tkBusy == ptn {

			sdl.tkBusy = next
		}

		if sdl.busySize--; sdl.busySize <= 0 {

			schLog.Debug("schTaskBusyDeque: internal errors")

			return SchEnoInternal
		}
	}

	return SchEnoNone
}

//
// Remove task map entry
//
func (sdl *scheduler)schTaskDemap(name string) SchErrno {
	sdl.lock.Lock()
	defer sdl.lock.Unlock()
	delete(sdl.tkMap, name)
	return SchEnoNone
}

//
// Send timer event to user task when timer expired
//
func (sdl *scheduler)schSendTimerEvent(ptm *schTmcbNode) SchErrno {

	//
	// if in power off stage, discard the message
	//

	if sdl.powerOff == true {
		if schLog.debug__ {
			schLog.Debug("schSendTimerEvent: in power off stage")
		}
		return SchEnoPowerOff
	}

	//
	// put timer expired event into target task mail box
	//

	var task = &ptm.tmcb.taskNode.task
	var msg = schMessage{
		sender:	&rawTmTsk,
		recver:	ptm.tmcb.taskNode,
		Id:		EvTimerBase + ptm.tmcb.utid,
		Body:	ptm.tmcb.extra,
	}

	if len(*task.mailbox.que) + mbReserved >= cap(*task.mailbox.que) {
		schLog.Debug("schSendTimerEvent: mailbox of target is full, sdl: %s, task: %s", sdl.p2pCfg.CfgName, task.name)
		panic(fmt.Sprintf("system overload, task: %s", task.name))
	}

	task.evTotal += 1
	task.evHistory[task.evhIndex] = msg
	task.evhIndex = (task.evhIndex + 1) & (evHistorySize - 1)

	*task.mailbox.que<-msg

	return SchEnoNone
}

//
// Create a single task
//
type schTaskDescription SchTaskDescription

func (sdl *scheduler)schCreateTask(taskDesc *schTaskDescription) (SchErrno, interface{}){

	//
	// failed if in power off stage
	//

	if sdl.powerOff == true {
		schLog.Debug("schCreateTask: in power off stage")
		return SchEnoPowerOff, nil
	}

	var eno SchErrno
	var ptn *schTaskNode

	if taskDesc == nil {
		schLog.Debug("schCreateTask: invalid user task description")
		return SchEnoParameter, nil
	}

	//
	// get node and check it
	//

	if eno, ptn = sdl.schGetTaskNode(); eno != SchEnoNone || ptn == nil {

		schLog.Debug("schCreateTask: " +
			"schGetTaskNode failed, eno: %d",
			eno)

		return eno, nil
	}

	if ptn.task.mailbox.que != nil {
		close(*ptn.task.mailbox.que)
		ptn.task.mailbox.que = nil
		ptn.task.mailbox.size = 0
	}

	if ptn.task.done != nil {
		close(ptn.task.done)
		ptn.task.done = nil
	}

	if ptn.task.stopped != nil {
		close(ptn.task.stopped)
		ptn.task.stopped = nil
	}

	//
	// setup user task
	//

	ptn.task.sdl			= sdl
	ptn.task.name			= strings.TrimSpace(taskDesc.Name)
	ptn.task.utep			= taskDesc.Ep
	mq 						:= make(chan schMessage, taskDesc.MbSize)
	ptn.task.mailbox.que	= &mq
	ptn.task.mailbox.size	= taskDesc.MbSize
	ptn.task.done			= make(chan SchErrno, 1)
	ptn.task.stopped		= make(chan bool, 1)
	ptn.task.dog			= *taskDesc.Wd
	ptn.task.dieCb			= taskDesc.DieCb
	ptn.task.userData		= taskDesc.UserDa

	//
	// task timer table
	//

	for idx, ptm := range ptn.task.tmTab {
		if ptm != nil {
			ptn.task.tmTab[idx] = nil
		}
	}

	for k := range ptn.task.tmIdxTab {
		delete(ptn.task.tmIdxTab, k)
	}

	//
	// map task name to task node pointer. some dynamic tasks might have empty
	// task name, in this case, the task node pointer would not be mapped in
	// table, and this task could not be found by function schGetTaskNodeByName
	//

	sdl.lock.Lock()

	if len(ptn.task.name) <= 0 {

		schLog.Debug("schCreateTask: task with empty name")

		sdl.schRetTaskNode(ptn)
		sdl.lock.Unlock()

		return SchEnoParameter, nil

	} else if _, dup := sdl.tkMap[ptn.task.name]; dup == true {

		_, file, line, _ := runtime.Caller(2)
		schLog.Debug("schCreateTask: " +
			"sdl: %s, duplicated task: %s, fbt2: %s, lbt2: %d",
			sdl.p2pCfg.Name, ptn.task.name, file, line)

		sdl.schRetTaskNode(ptn)
		sdl.lock.Unlock()

		return SchEnoDuplicated, nil

	} else {

		sdl.tkMap[ptn.task.name] = ptn
	}

	sdl.lock.Unlock()

	//
	// put task node to busy queue
	//

	if eno := sdl.schTaskBusyEnque(ptn); eno != SchEnoNone {

		schLog.Debug("schCreateTask: " +
			"schTaskBusyEnque failed, rc: %d",
			eno)

		sdl.schRetTaskNode(ptn)

		return eno, nil
	}

	//
	// start task to work according to the flag, if the flag is invalid, we suspend
	// the user task and inform caller with SchEnoSuspended returned.
	//

	eno = SchEnoNone

	if taskDesc.Flag == SchCreatedGo {

		ptn.task.goStatus = SchCreatedGo
		go sdl.schCommonTask(ptn)

	} else if taskDesc.Flag == SchCreatedSuspend {

		ptn.task.goStatus = SchCreatedSuspend

	} else {

		schLog.Debug("schCreateTask: " +
			"suspended for invalid goStatus flag: %d",
			taskDesc.Flag)

		ptn.task.goStatus = SchCreatedSuspend
		eno = SchEnoSuspended
	}

	return eno, ptn
}

//
// Start a single task by task name
//
func (sdl *scheduler)schStartTask(name string) SchErrno {

	//
	// Notice: only those suspended user task can be started, so this function does
	// not create new user task, instead, it try to find the suspended task and then
	// start it.
	//

	if sdl.powerOff == true {
		schLog.Debug("schStartTask: in power off stage")
		return SchEnoPowerOff
	}

	eno, ptn := sdl.schGetTaskNodeByName(name)

	if eno != SchEnoNone || ptn == nil {

		schLog.Debug("schStartTask: " +
			"schGetTaskNodeByName failed, name: %s, eno: %d, ptn: %X",
			name, eno, ptn)

		return eno
	}

	//
	// can only a suspended task be started
	//

	if ptn.task.goStatus != SchCreatedSuspend {

		schLog.Debug("schStartTask: " +
			"invalid user task status: %d",
			ptn.task.goStatus)

		return SchEnoMismatched
	}

	//
	// go the user task
	//

	ptn.task.goStatus = SchCreatedGo
	go sdl.schCommonTask(ptn)

	return SchEnoNone
}

//
// Start task by task node pointer
//
func (sdl *scheduler)schStartTaskEx(ptn *schTaskNode) SchErrno {

	if sdl.powerOff == true {
		schLog.Debug("schStartTaskEx: in power off stage")
		return SchEnoPowerOff
	}

	if ptn == nil {
		schLog.Debug("schStartTaskEx: invalid pointer to task node")
		return SchEnoParameter
	}

	//
	// can only a suspended task be started
	//

	if ptn.task.goStatus != SchCreatedSuspend {

		schLog.Debug("schStartTaskEx: " +
			"invalid user task status: %d",
			ptn.task.goStatus)

		return SchEnoMismatched
	}

	//
	// go the user task
	//

	ptn.task.goStatus = SchCreatedGo
	go sdl.schCommonTask(ptn)

	return SchEnoNone
}

//
// Stop a single task by task name
//
func (sdl *scheduler)schStopTask(name string) SchErrno {

	//
	// Attention: this function MUST only be called to kill a task than the
	// caller itself, or will get a deadlock.
	//

	eno, ptn := sdl.schGetTaskNodeByName(name)
	if eno != SchEnoNone || ptn == nil {
		schLog.Debug("schStopTask: " +
			"schGetTaskNodeByName failed, name: %s, eno: %d, ptn: %X",
			name, eno, ptn)
		return eno
	}

	//
	// done with "killed" signal and then wait "stopped"
	//

	ptn.task.done<-SchEnoKilled
	<-ptn.task.stopped

	return SchEnoNone
}

//
// Stop a single task by task pointer
//
func (sdl *scheduler)schStopTaskEx(ptn *schTaskNode) SchErrno {

	//
	// Seems need not to lock the scheduler control block ?! for functions
	// called here had applied the lock if necessary when they called in.
	//

	var eno SchErrno

	if ptn == nil {
		panic("schStopTaskEx: invalid task node pointer")
	}

	//
	// callback user task to die.
	//

	if ptn.task.dieCb != nil {

		if eno = ptn.task.dieCb(ptn); eno != SchEnoNone {

			//
			// Notice: here dieCb return failed, but we SHOULD NOT care it since
			// this function is called into a user context, we can not return here
			// or the resources allocated to this task will leak. We had to go
			// ahead.
			//

			schLog.Debug("schStopTaskEx: "+
				"dieCb failed, eno: %d, task: %s",
				eno, ptn.task.name)
		}
	}

	//
	// stop user timers
	//

	schLog.Debug("schStopTaskEx: clean task timers, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno = sdl.schKillTaskTimers(&ptn.task); eno != SchEnoNone {
		panic(fmt.Sprintf("schStopTaskEx: " +
			"schKillTaskTimers faild, eno: %d， task: %s",
			eno, ptn.task.name))
	}

	//
	// clean the user task control block
	//

	schLog.Debug("schStopTaskEx: clean task control block, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	if eno = sdl.schTcbClean(&ptn.task); eno != SchEnoNone {
		panic(fmt.Sprintf("schStopTaskEx: " +
			"schTcbClean faild, eno: %d, task: %s",
			eno, ptn.task.name))
	}

	schLog.Debug("schStopTaskEx: all ok, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, ptn.task.name)

	return SchEnoNone
}

//
// Make user task control block clean
//
func (sdl *scheduler)schTcbClean(tcb *schTask) SchErrno {

	tcb.lock.Lock()
	defer tcb.lock.Unlock()

	if tcb == nil {
		schLog.Debug("schTcbClean: invalid task control block pointer")
		return SchEnoParameter
	}

	tcb.name				= ""
	tcb.utep				= nil
	tcb.dog.Cycle			= SchDefaultDogCycle
	tcb.dog.BiteCounter		= 0
	tcb.dog.DieThreshold	= SchDefaultDogDieThresold
	tcb.dieCb				= nil
	tcb.goStatus			= SchCreatedSuspend

	close(*tcb.mailbox.que)
	tcb.mailbox.que = nil
	tcb.mailbox.size = 0

	close(tcb.done)
	tcb.done = nil
	close(tcb.stopped)
	tcb.stopped = nil

	for loop := 0; loop < schMaxTaskTimer; loop++ {

		if tcb.tmTab[loop] != nil {

			delete(tcb.tmIdxTab, tcb.tmTab[loop])
			tcb.tmTab[loop] = nil
		}
	}

	return SchEnoNone
}

//
// Delete a single task by task name
//
func (sdl *scheduler)schDeleteTask(name string) SchErrno {

	//
	// Currently, "Delete" implented as "Stop"
	//

	return sdl.schStopTask(name)
}

//
// Send message to a specific task
//
func (sdl *scheduler)schSendMsg(msg *schMessage) (eno SchErrno) {

	//
	// check the message to be sent
	//

	if msg == nil {
		schLog.Debug("schSendMsg: invalid message")
		return SchEnoParameter
	}

	//
	// failed if in power off stage
	//

	if sdl.powerOff == true {
		switch msg.Id {
		case EvSchPoweroff:
		case EvSchDone:
		default:
			if schLog.debug__ {
				schLog.Debug("schSendMsg: in power off stage")
			}
			return SchEnoPowerOff
		}
	}

	if msg.sender == nil {
		schLog.Debug("schSendMsg: invalid sender")
		return SchEnoParameter
	}

	if msg.recver == nil {
		schLog.Debug("schSendMsg: invalid receiver")
		return SchEnoParameter
	}

	//
	// for debug to back trace
	//

	if schLog.debug__ && false {
		_, file, line, _ := runtime.Caller(2)
		schLog.Debug("schSendMsg: sdl: %s, from: %s, to: %s, mid: %d, fbt2: %s, lbt2: %d",
			sdl.p2pCfg.CfgName,
			sdl.schGetTaskName(msg.sender),
			sdl.schGetTaskName(msg.recver),
			msg.Id,
			file,
			line)
	}

	//
	// put message to receiver mailbox.
	// notice: here we do not invoke any "lock" to ensure the integrity of the target task
	// pointer, this require the following conditions fulfilled: 1) after task is created,
	// the task pointer is always effective until it's done; 2) after task is done, any other
	// tasks should never reference to this stale pointer, this means that any other tasks
	// should remove the pointer(if then have)to a task will be done.
	//

	target := &msg.recver.task
	target.lock.Lock()
	defer target.lock.Unlock()

	msg2MailBox := func(msg *schMessage) SchErrno {
		if target.mailbox.que == nil {
			// not found, target had been killed
			schLog.Debug("schSendMsg: mailbox of target is empty, sdl: %s, task: %s", sdl.p2pCfg.CfgName, target.name)
			return SchEnoInternal
		}

		if len(*target.mailbox.que) + mbReserved >= cap(*target.mailbox.que) {

			schLog.Debug("schSendMsg: mailbox of target is full, sdl: %s, task: %s", sdl.p2pCfg.CfgName, target.name)
			target.discardMessages += 1

			if target.discardMessages & (0x1f) == 0 {
				p2plog.Debug("schSendMsg: sdl: %s, task: %s, discardMessages: %d",
					sdl.p2pCfg.CfgName, target.name, target.discardMessages)
			}

		} else {

			*target.mailbox.que <- *msg
		}

		target.evTotal += 1
		target.evHistory[target.evhIndex] = *msg
		target.evhIndex = (target.evhIndex + 1) & (evHistorySize - 1)

		return SchEnoNone
	}

	if target.isStatic {
		if !target.isPoweron {

			if msg.Id != EvSchPoweron {

				target.delayMessages = append(target.delayMessages, msg)
				return SchEnoNone

			} else {

				if eno := msg2MailBox(msg); eno != SchEnoNone {
					return eno
				}

				target.isPoweron = true
				for idx := 0; idx < len(target.delayMessages); idx++ {
					if eno := msg2MailBox(target.delayMessages[idx]); eno != SchEnoNone {
						return eno
					}
				}

				target.delayMessages = nil
				return SchEnoNone
			}
		} else {

			return msg2MailBox(msg)
		}
	} else {

		return msg2MailBox(msg)
	}

	panic("schSendMsg: would never come here!!!")
}

//
// Set a timer: extra passed in, which would ret to timer owner when
// timer expired; and timer identity returned to caller. So when timer
// event received, one can determine which timer it is, and extract
// those extra put into timer when it's created.
//
func (sdl *scheduler)schSetTimer(ptn *schTaskNode, tdc *timerDescription) (SchErrno, int) {

	//
	// failed if in power off stage
	//

	if sdl.powerOff == true {
		schLog.Debug("schSetTimer: in power off stage")
		return SchEnoPowerOff, SchInvalidTid
	}

	//
	// Here we had got a task node, since the timer is still not in running,
	// seems we had no need to do anything to protect the task. Notice that
	// function schGetTimerNode would get the scheduler lock internal
	// itself, see it pls.
	//

	var tid int
	var eno SchErrno
	var ptm *schTmcbNode

	if ptn == nil || tdc == nil {
		schLog.Debug("schSetTimer: invalid parameter(s)")
		return SchEnoParameter, schInvalidTid
	}

	ptn.task.lock.Lock()
	defer ptn.task.lock.Unlock()

	//
	// check if some user task timers are free
	//

	for tid = 0; tid < schMaxTaskTimer; tid++ {
		if ptn.task.tmTab[tid] == nil {
			break
		}
	}

	if tid >= schMaxTaskTimer {
		schLog.Debug("schSetTimer: too much, timer table is full")
		return SchEnoResource, schInvalidTid
	}

	//
	// get a timer node
	//

	if eno, ptm = sdl.schGetTimerNode(); eno != SchEnoNone || ptm == nil {

		schLog.Debug("schSetTimer: " +
			"schGetTimerNode failed, eno: %d",
			eno)

		return eno, schInvalidTid
	}

	ptm.tmcb.stopped = make(chan  bool)
	ptm.tmcb.stop = make(chan bool, 1)

	//
	// backup timer node
	//

	ptn.task.tmTab[tid] = ptm
	ptn.task.tmIdxTab[ptm] = tid

	//
	// setup timer control block
	//

	tcb 			:= &ptm.tmcb
	tcb.name		= tdc.Name
	tcb.utid		= tdc.Utid
	tcb.tmt			= schTimerType(tdc.Tmt)
	tcb.dur			= tdc.Dur
	tcb.taskNode	= ptn
	tcb.extra		= tdc.Extra

	//
	// go timer common task for timer
	//

	go sdl.schTimerCommonTask(ptm)

	return SchEnoNone, tid
}

//
// Kill a timer
//
func (sdl *scheduler)schKillTimer(ptn *schTaskNode, tid int) SchErrno {

	//
	// para check
	//

	if ptn == nil || tid < 0 || tid > schMaxTaskTimer {
		schLog.Debug("schKillTimer: invalid parameter(s)")
		return SchEnoParameter
	}

	//
	// lock the task, we can't use defer here. see function schTimerCommonTask
	// for more about sync please. we would unlock bellow, see it.
	//

	ptn.task.lock.Lock()

	//
	// Notice: when try to kill a timer, the timer might have been expired, but
	// the message sent about this is still not received by user task. In this
	// case, user task would get "SchEnoNotFound", and this is not fault of the
	// scheduler really, but the user task would be confuse with the event about
	// a timer killed failed(SchEnoNotFound) received later. We still not solve
	// this issue now.
	//

	if ptn.task.tmTab[tid] == nil {
		ptn.task.lock.Unlock()
		return SchEnoNone
	}

	//
	// emit stop signal and wait stopped signal
	//

	tcb := &ptn.task.tmTab[tid].tmcb
	tcb.stop<-true

	ptn.task.lock.Unlock()

	if stopped := <-tcb.stopped; stopped {
		return SchEnoNone
	}

	return SchEnoInternal
}

//
// Kill all active timers owned by a task
//
func (sdl *scheduler)schKillTaskTimers(task *schTask) SchErrno {

	task.lock.Lock()
	stopped := make([]chan bool, 0)
	for tm := range task.tmIdxTab {
		tm.tmcb.stop<-true
		stopped = append(stopped, tm.tmcb.stopped)
	}
	task.lock.Unlock()

	count := len(stopped)
	if count == 0 {
		schLog.Debug("schKillTaskTimers: none of timers, sdl: %s, task: %s",
			sdl.p2pCfg.CfgName, task.name)
		return SchEnoNone
	}

	lock := sync.Mutex{}
	allDone:= make(chan bool)
	deCount := func () {
		lock.Lock()
		if count--; count == 0 {
			allDone<-true
		}
		lock.Unlock()
	}
	for _, ch := range stopped {
		go func() {
			<-ch
			deCount()
		}()
	}

	<-allDone

	schLog.Debug("schKillTaskTimers: all killed, sdl: %s, task: %s",
		sdl.p2pCfg.CfgName, task.name)

	return SchEnoNone
}


//
// Get task node pointer by task name
//
func (sdl *scheduler)schGetTaskNodeByName(name string) (SchErrno, *schTaskNode) {

	sdl.lock.Lock()
	defer sdl.lock.Unlock()

	if name == RawSchTaskName {
		return SchEnoNone, &rawSchTsk
	}

	if ptn, err := sdl.tkMap[name]; ptn == nil || !err {
		return SchEnoNotFound, nil
	}

	return SchEnoNone, sdl.tkMap[name]
}

//
// Done a task
//
func (sdl *scheduler)schTaskDone(ptn *schTaskNode, eno SchErrno) SchErrno {

	//
	// Notice: this function "should" be called inside a task to kill itself, so
	// do not poll the "stopped" signal, for this signal is fired by itself, this
	// will result in a deadlock.
	//

	if ptn == nil {
		schLog.Debug("schTaskDone: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// some tasks might have empty names
	//

	var tskName = ""
	if tskName = sdl.schGetTaskName(ptn); len(tskName) == 0 {
		schLog.Debug("schTaskDone: done task without name")
		return SchEnoParameter
	}

	//
	// see function schCommonTask for more please
	//

	ptn.task.done<-eno

	//
	// when coming here, it just the "done" fired, it's still not killed really,
	// see function schCommonTask for more pls. we could not try to poll the
	// "stopped" signal here, since we need our task to try the "done" we fired
	// above, there the "stopped" would be fired, but no one would care it.
	//
	return SchEnoNone
}

//
// Get user data area pointer
//
func (sdl *scheduler)schGetUserDataArea(ptn *schTaskNode) interface{} {
	if ptn == nil {
		return nil
	}
	return ptn.task.userData
}

//
// Set user data area pointer
//
func (sdl *scheduler)schSetUserDataArea(ptn *schTaskNode, uda interface{}) SchErrno {
	if ptn == nil {
		schLog.Debug("schSetUserDataArea: invalid task node pointer")
		return SchEnoParameter
	}
	ptn.task.userData = uda
	return SchEnoNone
}

//
// Set the power off stage flag to tell the scheduler it's going to be turn off
//
func (sdl *scheduler)schSetPoweroffStage() SchErrno {
	//sdl.lock.Lock()
	//defer sdl.lock.Unlock()
	sdl.powerOff = true
	return SchEnoNone
}

//
// Get the power off stage flag
//
func (sdl *scheduler)schGetPoweroffStage() bool {
	//sdl.lock.Lock()
	//defer sdl.lock.Unlock()
	return sdl.powerOff
}

//
// Get task name
//
func (sdl *scheduler)schGetTaskName(ptn *schTaskNode) string {
	if ptn == nil {
		return ""
	}
	return ptn.task.name
}

//
// Get task number
//
func (sdl *scheduler)schGetTaskNumber() int {
	sdl.lock.Lock()
	tn := len(sdl.tkMap)
	sdl.lock.Unlock()
	return tn
}

//
// Show task names
//
func (sdl *scheduler)schShowTaskName() []string {
	sdl.lock.Lock()
	defer sdl.lock.Unlock()
	var names []string = nil
	for n := range sdl.tkMap {
		names = append(names, n)
	}
	return names
}

//
// Start scheduler
//
func (sdl *scheduler)schSchedulerStart(tsd []TaskStaticDescription, tpo []string) (eno SchErrno, name2Ptn *map[string]interface{}){

	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart: going to start ycp2p scheduler ...")
	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart:")

	if len(tsd) <= 0 {
		schLog.Debug("schSchedulerStart: static task table is empty")
		return SchEnoParameter, nil
	}

	var (
		po = schMessage {
			sender:	&rawSchTsk,
			recver: nil,
			Id:		EvSchPoweron,
			Body:	nil,
		}

		ptn interface{} = nil

		// watch dog is not implemented, ignored
		tkd  = schTaskDescription {
			MbSize:	schMaxMbSize,
			Wd:		&SchWatchDog {HaveDog: false},
			Flag:	SchCreatedGo,
		}

		name2PtnMap = make(map[string] interface{})
	)

	//
	// loop the static table
	//

	for loop := 0; loop < len(tsd); loop++ {

		desc := tsd[loop]
		schLog.Debug("schSchedulerStart: " +
			"start a static task, idx: %d, name: %s",
			loop,
			desc.Name)

		//
		// setup task description. notice here the "Flag" always set to SchCreatedGo,
		// so task routine always goes when schCreateTask called, and later we would
		// not send poweron to an user task if it's flag (tsd[loop].Flag) is not
		// SchCreatedGo(SchCreatedSuspend), see bellow pls.
		//

		tkd.Name	= tsd[loop].Name
		tkd.DieCb	= tsd[loop].DieCb
		tkd.Ep		= tsd[loop].Tep
		tkd.Flag	= SchCreatedGo

		if tsd[loop].MbSize < 0 {
			tkd.MbSize = schMaxMbSize
		} else {
			tkd.MbSize = tsd[loop].MbSize
		}

		//
		// create task
		//

		if eno, ptn = sdl.schCreateTask(&tkd); eno != SchEnoNone {
			schLog.Debug("schSchedulerStart: " +
				"schCreateTask failed, task: %s",
				tkd.Name)
			return SchEnoParameter, nil
		}

		//
		// backup task node pointer by name
		//

		name2PtnMap[tkd.Name] = ptn

		//
		// all tasks created from register table are static, and those created by calling
		// function SchCreateTask would be dynamic tasks.
		//

		ptn.(*schTaskNode).task.isStatic = true

		//
		// send poweron event to task created aboved if it is required to be shceduled
		// at once; if the flag is SchCreatedSuspend, NO poweron sending, for this task
		// routine is not sleeping on the mailbox, BUT those who had set SchCreatedGo
		// do sleeping on their mailboxes. and please notice that: those tasks who had
		// been sent poweron signal to, must not try to interact with those tasks still
		// not be created.
		//

		if tsd[loop].Flag == SchCreatedGo {
			po.recver = ptn.(*schTaskNode)
			if eno = sdl.schSendMsg(&po); eno != SchEnoNone {
				schLog.Debug("schSchedulerStart: "+
					"schSendMsg failed, event: EvSchPoweron, eno: %d, task: %s",
					eno,
					tkd.Name)
				return eno, nil
			}
		}
	}

	//
	// send poweron event for those taskes registed in table "tpo" passed in, notice
	// that, if one task had set the SchCreatedGo flag, it then should not be present
	// in this "tpo" table, see above pls.
	//

	for loop := 0; loop < len(tpo); loop++ {

		name := tpo[loop]
		schLog.Debug("schSchedulerStart: send poweron to task: %s", name)
		eno, tsk := sdl.schGetTaskNodeByName(name)
		if eno != SchEnoNone {
			schLog.Debug("schSchedulerStart: " +
				"schGetTaskNodeByName failed, eno: %d, name: %s",
				eno,
				name)
			continue
		}

		if tsk == nil {
			schLog.Debug("schSchedulerStart: " +
				"nil task node pointer, eno: %d, name: %s",
				eno,
				name)
			continue
		}

		po.recver = tsk

		if eno = sdl.schSendMsg(&po); eno != SchEnoNone {
			schLog.Debug("schSchedulerStart: "+
				"schSendMsg failed, event: EvSchPoweron, eno: %d, task: %s",
				eno,
				name)
			return eno, nil
		}
	}

	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart: it's ok, ycp2p in running")
	schLog.Debug("schSchedulerStart:")
	schLog.Debug("schSchedulerStart:")

	return SchEnoNone, &name2PtnMap
}
