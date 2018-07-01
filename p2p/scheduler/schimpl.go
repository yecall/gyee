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
	golog	"log"
	yclog	"github.com/yeeco/p2p/logger"
)

//
// The scheduler for p2p module, and its' pointer. Notice: we do not plan to
// export any scheduler logic to other modules in any system, so we prefert
// a static var here than creating a shceduler objcet on demand, see it pls.
//
var p2pSDL = scheduler{}

//
// Default task node for shceduler to send event
//
const rawSchTaskName = "schTsk"

var rawSchTsk = schTaskNode {
	task: schTask{name:rawSchTaskName,},
	last: nil,
	next: nil,
}

//
// Default task node for shceduler to send event
//
const rawTmTaskName = "tmTsk"

var rawTmTsk = schTaskNode {
	task: schTask{name:rawTmTaskName,},
	last: nil,
	next: nil,
}

//
// Scheduler initilization
//
func schimplSchedulerInit() SchErrno {

	//
	// make maps
	//

	p2pSDL.tkMap = make(map[schTaskName] *schTaskNode)
	p2pSDL.tmMap = make(map[*schTmcbNode] *schTaskNode)
	p2pSDL.grpMap = make(map[schTaskGroupName][]*schTaskNode)

	//
	// setup free task node queue
	//

	for loop := 0; loop < schTaskNodePoolSize; loop++ {
		schTaskNodePool[loop].last = &schTaskNodePool[(loop - 1 + schTaskNodePoolSize) & (schTaskNodePoolSize - 1)]
		schTaskNodePool[loop].next = &schTaskNodePool[(loop + 1) & (schTaskNodePoolSize - 1)]
		schTaskNodePool[loop].task.tmIdxTab = make(map[*schTmcbNode] int)
	}

	p2pSDL.freeSize = schTaskNodePoolSize
	p2pSDL.tkFree = &schTaskNodePool[0]

	//
	// setup free timer node queue
	//

	for loop := 0; loop < schTimerNodePoolSize; loop++ {
		schTimerNodePool[loop].last = &schTimerNodePool[(loop - 1 + schTimerNodePoolSize) & (schTimerNodePoolSize - 1)]
		schTimerNodePool[loop].next = &schTimerNodePool[(loop + 1) & (schTimerNodePoolSize - 1)]
		schTimerNodePool[loop].tmcb.stop = make(chan bool, 1)
		schTimerNodePool[loop].tmcb.stopped = make(chan bool)
	}

	p2pSDL.tmFreeSize = schTimerNodePoolSize
	p2pSDL.tmFree = &schTimerNodePool[0]

	return SchEnoNone
}

//
// the common entry point for a scheduler task
//
func schimplCommonTask(ptn *schTaskNode) SchErrno {

	var queMsg	*chan schMessage
	var done 	*chan SchErrno
	var eno		SchErrno

	//
	// check pointer to task node
	//

	if ptn == nil {
		yclog.LogCallerFileLine("schimplCommonTask: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// check user task more
	//

	if ptn.task.utep == nil || ptn.task.mailbox.que == nil || ptn.task.done == nil {

		yclog.LogCallerFileLine("schimplCommonTask: " +
			"invalid user task: %s",
			ptn.task.name)

		return SchEnoParameter
	}

	//
	// get chans
	//

	queMsg = ptn.task.mailbox.que
	done = &ptn.task.done

	//
	// loop to schedule, until done(or something else happened).
	//
	// Notice: if the message queue size of one user task is zero, then this means
	// the user task would be a longlong loop, which need not to be shceduled by
	// messages. In this case, we go a routine for the loop first, then we check
	// the task until it done.
	//

	if ptn.task.mailbox.size == 0 ||
		ptn.task.mailbox.que == nil ||
		cap(*ptn.task.mailbox.que) == 0 {

		yclog.LogCallerFileLine("schimplCommonTask: " +
			"dead loop user task: %s",
			ptn.task.name)

		go ptn.task.utep(ptn, nil)
	}

taskLoop:

	for {
		select {

		case msg := <-*queMsg:

			//
			// dog wakes up
			//

			ptn.task.dog.lock.Lock()
			ptn.task.dog.Inited = ptn.task.dog.HaveDog
			ptn.task.dog.lock.Unlock()

			//
			// call handler
			//

			ptn.task.utep(ptn, (*SchMessage)(&msg))

			//
			// dog sleeps
			//

			ptn.task.dog.lock.Lock()
			ptn.task.dog.Inited = false
			ptn.task.dog.lock.Unlock()

		case eno = <-*done:

			if eno != SchEnoNone {

				yclog.LogCallerFileLine("schimplCommonTask: done with eno: %d", eno)
			}

			break taskLoop
		}
	}

	//
	// see function:
	//
	// 	schimplTaskDone
	//	schimplStopTask
	//
	// for more pls
	//

	ptn.task.stopped<-true

	//
	// exit, remove user task
	//

	return schimplStopTaskEx(ptn)
}

//
// the common entry point for timer task
//
func schimplTimerCommonTask(ptm *schTmcbNode) SchErrno {

	//
	// backup for debug output
	//

	ptm.tmcb.taskNode.task.lock.Lock()
	var tid = ptm.tmcb.taskNode.task.tmIdxTab[ptm]
	ptm.tmcb.taskNode.task.lock.Unlock()

	var killed = false

	//
	// cleaner for absolute timer when it expired
	//

	var funcAbsTimerClean = func (tn *schTmcbNode) {

		//
		// clear timer control block and remove it from maps, notice that the task
		// node should not be released here, it's accessed later after this function
		// called; and do not ret the timer control block node here.
		//

		delete(tn.tmcb.taskNode.task.tmIdxTab, tn)
		delete(p2pSDL.tmMap, tn)
		tn.tmcb.taskNode.task.tmTab[tid] = nil

		tn.tmcb.name	= ""
		tn.tmcb.tmt		= schTmTypeNull
		tn.tmcb.dur		= 0
		tn.tmcb.extra	= nil
	}

	//
	// cleaning job for cyclic timers are same as those absolute ones
	//

	var funcCycTimerClean = funcAbsTimerClean

	var tm *time.Ticker

	//
	// check pointer to timer node
	//

	if ptm == nil {
		yclog.LogCallerFileLine("schimplTimerCommonTask: invalid timer pointer")
		return SchEnoParameter
	}

	//
	// check timer type to deal with it
	//

	if ptm.tmcb.tmt == schTmTypePeriod {

		//
		// period, we loop for ever until killed
		//

		tm = time.NewTicker(ptm.tmcb.dur)

timerLoop:

		for {

			select {

			case <-tm.C:

				ptm.tmcb.taskNode.task.lock.Lock()

				if eno := schimplSendTimerEvent(ptm); eno != SchEnoNone {

					yclog.LogCallerFileLine("schimplTimerCommonTask: " +
						"send timer event failed, eno: %d, task: %s",
						eno,
						ptm.tmcb.taskNode.task.name)
				}

			case stop := <-ptm.tmcb.stop:

				ptm.tmcb.taskNode.task.lock.Lock()

				if stop == true {

					tm.Stop()
					killed = true
					funcCycTimerClean(ptm)

					break timerLoop
				}
			}

			ptm.tmcb.taskNode.task.lock.Unlock()
		}
	} else if ptm.tmcb.tmt == schTmTypeAbsolute {

		//
		// absolute, check duration
		//

		ptm.tmcb.taskNode.task.lock.Lock()

		dur := ptm.tmcb.dur

		if dur <= 0 {

			yclog.LogCallerFileLine("schimplTimerCommonTask: " +
				"invalid absolute timer duration:%d",
				ptm.tmcb.dur)

			ptm.tmcb.taskNode.task.lock.Unlock()

			return SchEnoParameter
		}

		ptm.tmcb.taskNode.task.lock.Unlock()

		//
		// send timer event after duration specified. we could not call time.After
		// directly, or we will blocked until timer expired, go a routine instead.
		//

		var to = make(chan bool)
		go func() {
			<-time.After(dur)
			to<-true
		}()

absTimerLoop:

		for {

			select {

			case <-to:

				//
				// Notice: here we must try to obtain the task first, since function
				// schimplRetTimerNode would try to get the lock of the shceduler,
				// see function schimplKillTimer for details please.
				//

				ptm.tmcb.taskNode.task.lock.Lock()
				p2pSDL.lock.Lock()

				if eno := schimplSendTimerEvent(ptm); eno != SchEnoNone {

					yclog.LogCallerFileLine("schimplTimerCommonTask: "+
						"send timer event failed, eno: %d, task: %s",
						eno,
						ptm.tmcb.taskNode.task.name)
				}

				funcAbsTimerClean(ptm)
				p2pSDL.lock.Unlock()

				break absTimerLoop

			case stop := <-ptm.tmcb.stop:

				ptm.tmcb.taskNode.task.lock.Lock()
				p2pSDL.lock.Lock()

				if stop == true {

					funcAbsTimerClean(ptm)
					killed = true
				}

				p2pSDL.lock.Unlock()

				break absTimerLoop
			}
		}
	} else {

		//
		// unknown
		//

		yclog.LogCallerFileLine("schimplTimerCommonTask: " +
			"invalid timer type: %d",
			ptm.tmcb.tmt)

		return SchEnoParameter
	}

	//
	// exit, notice that here task is still locked, and only when killed we
	// need more cleaning job.
	//

	if killed {

		ptm.tmcb.stopped<-true

		close(ptm.tmcb.stop)
		close(ptm.tmcb.stopped)

		ptm.tmcb.stop = nil
		ptm.tmcb.stopped = nil
	}

	ptm.tmcb.taskNode.task.lock.Unlock()
	ptm.tmcb.taskNode = nil

	if eno := schimplRetTimerNode(ptm); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplTimerCommonTask: " +
			"schimplRetTimerNode failed, eno: %d",
			eno)

		return eno
	}

	return SchEnoNone
}

//
// Get timer node
//
func schimplGetTimerNode() (SchErrno, *schTmcbNode) {

	var tmn *schTmcbNode = nil

	p2pSDL.lock.Lock()
	defer p2pSDL.lock.Unlock()

	//
	// if empty
	//

	if p2pSDL.tmFree == nil {
		yclog.LogCallerFileLine("schimplGetTimerNode: free queue is empty")
		return SchEnoResource, nil
	}

	//
	// dequeue one node
	//

	tmn = p2pSDL.tmFree

	if tmn.last == tmn && tmn.next == tmn {

		if p2pSDL.tmFreeSize - 1 != 0 {

			yclog.LogCallerFileLine("schimplGetTimerNode: " +
				"internal errors, should be 0, but free size: %d",
				p2pSDL.tmFreeSize)

			return SchEnoInternal, nil
		}

		p2pSDL.tmFreeSize--
		p2pSDL.tmFree = nil

	} else {

		if p2pSDL.tmFreeSize - 1 <= 0 {

			yclog.LogCallerFileLine("schimplGetTimerNode: " +
				"internal errors, should equal or less than 0, but free size: %d",
				p2pSDL.tmFreeSize)

			return SchEnoInternal, nil
		}

		last := tmn.last
		next := tmn.next
		next.last = last
		last.next = next

		p2pSDL.tmFree = next
		p2pSDL.tmFreeSize--
	}

	tmn.next = tmn
	tmn.last = tmn

	return SchEnoNone, tmn
}

//
// Ret timer node to free queue
//
func schimplRetTimerNode(ptm *schTmcbNode) SchErrno {

	if ptm == nil {
		yclog.LogCallerFileLine("schimplRetTimerNode: invalid timer node pointer")
		return SchEnoParameter
	}

	p2pSDL.lock.Lock()
	defer  p2pSDL.lock.Unlock()

	//
	// enqueue a node
	//

	if p2pSDL.tmFree == nil {

		ptm.last = ptm
		ptm.next = ptm

	} else {

		last := p2pSDL.tmFree.last
		ptm.last = last
		last.next = ptm
		ptm.next = p2pSDL.tmFree
		p2pSDL.tmFree.last = ptm

	}

	p2pSDL.tmFree = ptm
	p2pSDL.tmFreeSize++

	return SchEnoNone
}

//
// Get task node
//
func schimplGetTaskNode() (SchErrno, *schTaskNode) {

	var tkn *schTaskNode = nil

	p2pSDL.lock.Lock()
	defer p2pSDL.lock.Unlock()

	//
	// if free node queue empty
	//

	if p2pSDL.tkFree== nil {
		yclog.LogCallerFileLine("schimplGetTaskNode: free queue is empty")
		return SchEnoResource, nil
	}

	//
	// dequeue one node
	//

	tkn = p2pSDL.tkFree

	if tkn.last == tkn && tkn.next == tkn {

		p2pSDL.tkFree = nil

		if p2pSDL.freeSize--; p2pSDL.freeSize != 0 {

			yclog.LogCallerFileLine("schimplGetTaskNode: internal errors")

			return SchEnoInternal, nil
		}

	} else {

		last := tkn.last
		next := tkn.next
		next.last = last
		last.next = next
		p2pSDL.tkFree = next

		if p2pSDL.freeSize--; p2pSDL.freeSize <= 0 {

			yclog.LogCallerFileLine("schimplGetTaskNode: internal errors")
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
func schimplRetTaskNode(ptn *schTaskNode) SchErrno {

	if ptn == nil {
		yclog.LogCallerFileLine("schimplRetTaskNode: invalid task node pointer")
		return SchEnoParameter
	}

	p2pSDL.lock.Lock()
	defer  p2pSDL.lock.Unlock()

	//
	// enqueue a node
	//

	if p2pSDL.tkFree == nil {

		ptn.last = ptn
		ptn.next = ptn

	} else {

		last := p2pSDL.tkFree.last
		ptn.last = last
		last.next = ptn
		ptn.next = p2pSDL.tkFree
		p2pSDL.tkFree.last = ptn

	}

	p2pSDL.tkFree = ptn
	p2pSDL.freeSize++

	return SchEnoNone
}

//
// Task node enter the busy queue
//
func schimplTaskBusyEnque(ptn *schTaskNode) SchErrno {
	
	if ptn == nil {
		yclog.LogCallerFileLine("schimplTaskBusyEnque: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// lock/unlock scheduler control block
	//

	p2pSDL.lock.Lock()
	defer  p2pSDL.lock.Unlock()

	//
	// enqueue a node
	//

	if p2pSDL.tkBusy == nil {

		ptn.last = ptn
		ptn.next = ptn

	} else {

		last := p2pSDL.tkBusy.last
		ptn.last = last
		last.next = ptn
		ptn.next = p2pSDL.tkBusy
		p2pSDL.tkBusy.last = ptn

	}

	p2pSDL.tkBusy = ptn
	p2pSDL.busySize++

	return SchEnoNone
}

//
// Task node dequeue from the busy queue
//
func schimplTaskBusyDeque(ptn *schTaskNode) SchErrno {

	if ptn == nil {
		yclog.LogCallerFileLine("schimplTaskBusyDeque: invalid parameter")
		return SchEnoParameter
	}

	//
	// lock/unlock schduler control block
	//

	p2pSDL.lock.Lock()
	defer p2pSDL.lock.Unlock()

	//
	// remove the busy node
	//

	if p2pSDL.busySize <= 0 {

		yclog.LogCallerFileLine("schimplTaskBusyDeque: invalid parameter")
		return SchEnoInternal

	} else if p2pSDL.busySize == 1 {

		if p2pSDL.tkBusy != ptn {

			yclog.LogCallerFileLine("schimplTaskBusyDeque: invalid parameter")
			return SchEnoInternal

		} else {

			p2pSDL.tkBusy = nil
			p2pSDL.busySize = 0
			return SchEnoNone
		}
	}

	if ptn.last == ptn && ptn.next == ptn {

		if ptn == p2pSDL.tkBusy {

			p2pSDL.tkBusy = nil

		} else {

			yclog.LogCallerFileLine("schimplTaskBusyDeque: internal errors")

			return SchEnoInternal
		}
		if p2pSDL.busySize--; p2pSDL.busySize != 0 {

			yclog.LogCallerFileLine("schimplTaskBusyDeque: internal errors")

			return SchEnoInternal
		}
	} else {

		last := ptn.last
		next := ptn.next
		last.next = next
		next.last = last

		if p2pSDL.tkBusy == ptn {

			p2pSDL.tkBusy = next
		}

		if p2pSDL.busySize--; p2pSDL.busySize <= 0 {

			yclog.LogCallerFileLine("schimplTaskBusyDeque: internal errors")

			return SchEnoInternal
		}
	}

	return SchEnoNone
}

//
// Send timer event to user task when timer expired
//
func schimplSendTimerEvent(ptm *schTmcbNode) SchErrno {

	//
	// get owner task
	//

	var task = &ptm.tmcb.taskNode.task

	//
	// setup timer event message. notice that the sender is the scheduler indeed,
	// we set sender pointer to raw timer task in this case; and the extra set when
	// timer crated is also return to timer owner.
	//

	var msg = schMessage{
		sender:	&rawTmTsk,
		recver:	ptm.tmcb.taskNode,
		Id:		EvSchNull,
		Body:	ptm.tmcb.extra,
	}

	msg.Id = EvTimerBase + ptm.tmcb.utid

	//
	// put message to task mailbox
	//

	*task.mailbox.que<-msg

	return SchEnoNone
}

//
// Create a single task
//
type schTaskDescription SchTaskDescription

func schimplCreateTask(taskDesc *schTaskDescription) (SchErrno, interface{}){

	var eno SchErrno
	var ptn *schTaskNode

	if taskDesc == nil {
		yclog.LogCallerFileLine("schimplCreateTask: invalid user task description")
		return SchEnoParameter, nil
	}

	//
	// get task node
	//

	if eno, ptn = schimplGetTaskNode(); eno != SchEnoNone || ptn == nil {

		yclog.LogCallerFileLine("schimplCreateTask: " +
			"schimplGetTaskNode failed, eno: %d",
			eno)

		return eno, nil
	}

	//
	// check if a nil mailbox
	//

	if ptn.task.mailbox.que != nil {

		close(*ptn.task.mailbox.que)
		ptn.task.mailbox.que = nil
		ptn.task.mailbox.size = 0
	}

	//
	// check if a nil done channel
	//

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

	ptn.task.name			= strings.TrimSpace(taskDesc.Name)
	ptn.task.utep			= schUserTaskEp(taskDesc.Ep)
	mq 						:= make(chan schMessage, taskDesc.MbSize)
	ptn.task.mailbox.que	= &mq
	ptn.task.mailbox.size	= taskDesc.MbSize
	ptn.task.done			= make(chan SchErrno, 1)
	ptn.task.stopped		= make(chan bool, 1)
	ptn.task.dog			= schWatchDog(*taskDesc.Wd)
	ptn.task.dieCb			= taskDesc.DieCb
	ptn.task.userData		= taskDesc.UserDa

	//
	// make timer table
	//

	for idx, ptm := range ptn.task.tmTab {

		if ptm != nil {

			ptn.task.tmTab[idx] = nil
		}
	}

	//
	// make timer map clean
	//

	for k := range ptn.task.tmIdxTab {

		delete(ptn.task.tmIdxTab, k)
	}

	//
	// map task name to task node pointer. some dynamic tasks might have empty
	// task name, in this case, the task node pointer would not be mapped in
	// table, and this task could not be found by function schimplGetTaskNodeByName
	//

	p2pSDL.lock.Lock()

	if len(ptn.task.name) <= 0 {

		yclog.LogCallerFileLine("schimplCreateTask: task with empty name")

	} else if _, dup := p2pSDL.tkMap[schTaskName(ptn.task.name)]; dup == true {

		yclog.LogCallerFileLine("schimplCreateTask: " +
			"duplicated task name: %s",
			ptn.task.name)

		p2pSDL.lock.Unlock()

		return SchEnoDuplicated, nil

	} else {

		p2pSDL.tkMap[schTaskName(ptn.task.name)] = ptn
	}

	p2pSDL.lock.Unlock()

	//
	// put task node to busy queue
	//

	if eno := schimplTaskBusyEnque(ptn); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplCreateTask: " +
			"schimplTaskBusyEnque failed, rc: %d",
			eno)

		return eno, nil
	}

	//
	// start task to work according the flag, if the flag is invalid, we suspend
	// the user task and inform caller with SchEnoSuspended returned.
	//

	eno = SchEnoNone

	if taskDesc.Flag == SchCreatedGo {

		ptn.task.goStatus = SchCreatedGo
		go schimplCommonTask(ptn)

	} else if taskDesc.Flag == SchCreatedSuspend {

		ptn.task.goStatus = SchCreatedSuspend

	} else {

		yclog.LogCallerFileLine("schimplCreateTask: " +
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
func schimplStartTask(name string) SchErrno {

	//
	// Notice: only those suspended user task can be started, so this function does
	// not create new user task, instead, it try to find the suspended task and then
	// start it.
	//

	//
	// get task node pointer by name
	//

	eno, ptn := schimplGetTaskNodeByName(name)

	if eno != SchEnoNone || ptn == nil {

		yclog.LogCallerFileLine("schimplStartTask: " +
			"schimplGetTaskNodeByName failed, name: %s, eno: %d, ptn: %X",
			name, eno, ptn)

		return eno
	}

	//
	// can only a suspended task be started
	//

	if ptn.task.goStatus != SchCreatedSuspend {

		yclog.LogCallerFileLine("schimplStartTask: " +
			"invalid user task status: %d",
			ptn.task.goStatus)

		return SchEnoMismatched
	}

	//
	// go the user task
	//

	ptn.task.goStatus = SchCreatedGo
	go schimplCommonTask(ptn)

	yclog.LogCallerFileLine("schimplStartTask:" +
		"start ok, task: %s",
		name)

	return SchEnoNone
}

//
// Start task by task node pointer
//
func schimplStartTaskEx(ptn *schTaskNode) SchErrno {

	if ptn == nil {
		yclog.LogCallerFileLine("schimplStartTaskEx: invalid pointer to task node")
		return SchEnoParameter
	}

	//
	// can only a suspended task be started
	//

	if ptn.task.goStatus != SchCreatedSuspend {

		yclog.LogCallerFileLine("schimplStartTaskEx: " +
			"invalid user task status: %d",
			ptn.task.goStatus)

		return SchEnoMismatched
	}

	//
	// go the user task
	//

	ptn.task.goStatus = SchCreatedGo
	go schimplCommonTask(ptn)

	return SchEnoNone
}

//
// Stop a single task by task name
//
func schimplStopTask(name string) SchErrno {

	//
	// Attention: this function MUST only be called to kill a task than the
	// caller itself, or will get a deadlock.
	//

	//
	// get task node pointer by name
	//

	eno, ptn := schimplGetTaskNodeByName(name)

	if eno != SchEnoNone || ptn == nil {

		yclog.LogCallerFileLine("schimplStopTask: " +
			"schimplGetTaskNodeByName failed, name: %s, eno: %d, ptn: %X",
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
func schimplStopTaskEx(ptn *schTaskNode) SchErrno {

	//
	// Seems need not to lock the scheduler control block ?! for functions
	// called here had applied the lock if necessary when they called in.
	//

	var eno SchErrno

	if ptn == nil {
		yclog.LogCallerFileLine("schimplStopTaskEx: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// dequeue form busy queue
	//

	if eno := schimplTaskBusyDeque(ptn); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplStopTaskEx: " +
			"schimplTaskBusyDeque failed, eno: %d",
			eno)

		return eno
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

			yclog.LogCallerFileLine("schimplStopTaskEx: "+
				"dieCb failed, task: %s, eno: %d",
				ptn.task.name,
				eno)
		}
	}

	//
	// stop user timers
	//

	if eno = schimplKillTaskTimers(&ptn.task); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplStopTaskEx: " +
			"schimplKillTaskTimers faild, eno: %d",
			eno)

		return eno
	}

	//
	// clean the user task control block
	//

	if eno = schimplTcbClean(&ptn.task); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplStopTaskEx: " +
			"schimplTcbClean faild, eno: %d",
			eno)

		return eno
	}

	//
	// free task node
	//

	if eno = schimplRetTaskNode(ptn); eno != SchEnoNone {

		yclog.LogCallerFileLine("schimplStopTaskEx: " +
			"schimplRetTimerNode failed, task: %s, eno: %d",
			ptn.task.name,
			eno)

		return  eno
	}

	//
	// remove name to task node pointer map
	//

	if len(ptn.task.name) > 0 {

		p2pSDL.lock.Lock()
		delete(p2pSDL.tkMap, schTaskName(ptn.task.name))
		p2pSDL.lock.Unlock()
	}

	yclog.LogCallerFileLine("schimplStopTaskEx: task stopped, it's cleaned ok")

	return SchEnoNone
}

//
// Make user task control block clean
//
func schimplTcbClean(tcb *schTask) SchErrno {

	tcb.lock.Lock()
	defer tcb.lock.Unlock()

	if tcb == nil {
		yclog.LogCallerFileLine("schimplTcbClean: invalid task control block pointer")
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
			delete(p2pSDL.tmMap, tcb.tmTab[loop])

			tcb.tmTab[loop] = nil
		}
	}

	return SchEnoNone
}

//
// Delete a single task by task name
//
func schimplDeleteTask(name string) SchErrno {

	//
	// Currently, "Delete" implented as "Stop"
	//

	return schimplStopTask(name)
}

//
// Send message to a specific task
//
func schimplSendMsg(msg *schMessage) (eno SchErrno) {

	//
	// check the message to be sent
	//

	if msg == nil {
		yclog.LogCallerFileLine("schimplSendMsg: invalid message")
		return SchEnoParameter
	}

	if msg.sender == nil {
		yclog.LogCallerFileLine("schimplSendMsg: invalid sender")
		return SchEnoParameter
	}

	if msg.recver == nil {
		yclog.LogCallerFileLine("schimplSendMsg: invalid receiver")
		return SchEnoParameter
	}

	//
	// put message to receiver mailbox. More work might be needed, such as
	// checking against the sender and receiver name to see if they are in
	// busy queue; checking go status of both to see if they are matched,
	// and so on.
	//

	if msg.recver.task.mailbox.que == nil {
		yclog.LogCallerFileLine("schimplSendMsg: mailbox of target is empty")
		return SchEnoInternal
	}

	*msg.recver.task.mailbox.que<-*msg

	return SchEnoNone
}

//
// Send message to a specific task group
//
func schimplSendMsg2TaskGroup(grp string, msg *schMessage) (SchErrno, int) {

	var mtl []*schTaskNode = nil
	var found bool
	var failedCount = 0

	//
	// check message to be sent
	//

	if msg == nil {
		yclog.LogCallerFileLine("schimplSendMsg2TaskGroup: invalid message")
		return SchEnoParameter, -1
	}

	//
	// check group
	//

	if mtl, found = p2pSDL.grpMap[schTaskGroupName(grp)]; found != true {

		yclog.LogCallerFileLine("schimplSendMsg2TaskGroup: " +
			"not exist, group: %s",
			grp)

		return SchEnoParameter, -1
	}

	//
	// send message to each group member
	//

	for _, ptn := range mtl {

		msg.recver = ptn

		if eno := schimplSendMsg(msg); eno != SchEnoNone {

			yclog.LogCallerFileLine("schimplSendMsg2TaskGroup: " +
				"send failed, group: %s, member: %s",
				grp,
				ptn.task.name)

			failedCount++
		}
	}

	//
	// always SchEnoNone, caller should check failedCount
	//

	return SchEnoNone, failedCount
}

//
// Set a timer: extra passed in, which would ret to timer owner when
// timer expired; and timer identity returned to caller. So when timer
// event received, one can determine which timer it is, and extract
// those extra put into timer when it's created.
//
func schimplSetTimer(ptn *schTaskNode, tdc *timerDescription) (SchErrno, int) {

	//
	// Here we had got a task node, since the timer is still not in running,
	// seems we had no need to do anything to protect the task. Notice that
	// function schimplGetTimerNode would get the scheduler lock internal
	// itself, see it pls.
	//

	var tid int
	var eno SchErrno
	var ptm *schTmcbNode

	if ptn == nil || tdc == nil {
		yclog.LogCallerFileLine("schimplSetTimer: invalid parameter(s)")
		return SchEnoParameter, schInvalidTid
	}

	//
	// check if some user task timers are free
	//

	for tid = 0; tid < schMaxTaskTimer; tid++ {
		if ptn.task.tmTab[tid] == nil {
			break
		}
	}

	if tid >= schMaxTaskTimer {
		yclog.LogCallerFileLine("schimplSetTimer: too much, timer table is full")
		return SchEnoResource, schInvalidTid
	}

	//
	// get a timer node
	//

	if eno, ptm = schimplGetTimerNode(); eno != SchEnoNone || ptm == nil {

		yclog.LogCallerFileLine("schimplSetTimer: " +
			"schimplGetTimerNode failed, eno: %d",
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

	p2pSDL.lock.Lock()
	p2pSDL.tmMap[ptm] = ptn
	p2pSDL.lock.Unlock()

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

	go schimplTimerCommonTask(ptm)

	return SchEnoNone, tid
}

//
// Kill a timer
//
func schimplKillTimer(ptn *schTaskNode, tid int) SchErrno {

	//
	// para check
	//

	if ptn == nil || tid < 0 || tid > schMaxTaskTimer {
		yclog.LogCallerFileLine("schimplKillTimer: invalid parameter(s)")
		return SchEnoParameter
	}

	//
	// lock the task, we can't use defer here. see function schimplTimerCommonTask
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

		yclog.LogCallerFileLine("schimplKillTimer: try to kill a null timer")

		ptn.task.lock.Unlock()
		return SchEnoNotFound
	}

	//
	// emit stop signal and wait stopped signal
	//

	var tcb = &ptn.task.tmTab[tid].tmcb
	tcb.stop<-true

	ptn.task.lock.Unlock()

	if stopped := <-tcb.stopped; stopped {

		return SchEnoNone
	}

	yclog.LogCallerFileLine("schimplKillTimer: " +
		"timer killed failed, tid: %d, task: %s",
		tid,
		ptn.task.name)

	return SchEnoInternal
}

//
// Kill all active timers owned by a task
//
func schimplKillTaskTimers(task *schTask) SchErrno {

	if task == nil {
		yclog.LogCallerFileLine("schimplKillTaskTimers: nil task pointer")
		return SchEnoParameter
	}

	task.lock.Lock()
	defer task.lock.Unlock()

	for tm, idx := range task.tmIdxTab {

		//
		// check
		//

		if tm != task.tmTab[idx] {

			yclog.LogCallerFileLine("schimplKillTaskTimers: " +
				"timer node pointer mismatched, tm: %p, idx: %d, tmTab: %p",
				tm,
				idx,
				task.tmTab[idx])

			return SchEnoInternal
		}

		//
		// done the timer task
		//

		tm.tmcb.stop<-true

		//
		// wait until done
		//

		if stopped := <-tm.tmcb.stopped; stopped != true {

			yclog.LogCallerFileLine("schimplKillTaskTimers: "+
				"timer stopped with: %t",
				stopped)
		}
	}

	return SchEnoNone
}


//
// Get task node pointer by task name
//
func schimplGetTaskNodeByName(name string) (SchErrno, *schTaskNode) {

	// if exist
	if _, err := p2pSDL.tkMap[schTaskName(name)]; !err {
		return SchEnoNotFound, nil
	}

	// yes
	return SchEnoNone, p2pSDL.tkMap[schTaskName(name)]
}

//
// Done a task
//
func schimplTaskDone(ptn *schTaskNode, eno SchErrno) SchErrno {

	//
	// Notice: this function should be called inside a task to kill itself, so it
	// could not to poll the "stopped" signal, for this signal is fired by itself,
	// to do this will result in a deadlock.
	//

	if ptn == nil {
		yclog.LogCallerFileLine("schimplTaskDone: invalid task node pointer")
		return SchEnoParameter
	}

	//
	// some tasks might have empty names
	//

	var tskName = ""

	if tskName = schimplGetTaskName(ptn); len(tskName) == 0 {

		yclog.LogCallerFileLine("schimplTaskDone: done task without name")

	}

	//
	// see function schimplCommonTask for more please
	//

	ptn.task.done<-eno

	//
	// when coming here, it just the "done" fired, it's still not killed really,
	// see function schimplCommonTask for more pls. we could not try to poll the
	// "stopped" signal here, since we need our task to try the "done" we fired
	// above, there the "stopped" would be fired, but no one would care it is the
	// case.
	//

	return SchEnoNone
}

//
// Get user data area pointer
//
func schimplGetUserDataArea(ptn *schTaskNode) interface{} {
	if ptn == nil {
		return nil
	}
	return ptn.task.userData
}

//
// Set user data area pointer
//
func schimplSetUserDataArea(ptn *schTaskNode, uda interface{}) SchErrno {
	if ptn == nil {
		yclog.LogCallerFileLine("schimplSetUserDataArea: invalid task node pointer")
		return SchEnoParameter
	}
	ptn.task.userData = uda
	return SchEnoNone
}

//
// Remove user data area pointer
//
func schimplDelUserDataArea(ptn *schTaskNode) SchErrno {
	return schimplSetUserDataArea(ptn, nil)
}

//
// Get task name
//
func schimplGetTaskName(ptn *schTaskNode) string {
	if ptn == nil {
		return ""
	}
	return ptn.task.name
}

//
// Start scheduler
//
func schimplSchedulerStart(tsd []TaskStaticDescription, tpo []string) (eno SchErrno, name2Ptn *map[string]interface{}){

	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart: going to start ycp2p scheduler ...")
	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart:")

	var po = schMessage {
		sender:	&rawSchTsk,
		recver: nil,
		Id:		EvSchPoweron,
		Body:	nil,
	}

	var ptn interface{} = nil

	var tkd  = schTaskDescription {
		MbSize:	schMaxMbSize,
		Wd:		&SchWatchDog {Cycle:SchDefaultDogCycle, DieThreshold:SchDefaultDogDieThresold},
		Flag:	SchCreatedGo,
	}

	var name2PtnMap = make(map[string] interface{})

	if len(tsd) <= 0 {
		yclog.LogCallerFileLine("schimplSchedulerStart: static task table is empty")
		return SchEnoParameter, nil
	}

	//
	// loop the static table table
	//

	for loop, desc := range tsd {

		yclog.LogCallerFileLine("schimplSchedulerStart: " +
			"start a static task, idx: %d, name: %s",
			loop,
			desc.Name)

		//
		// setup task description. notice here the "Flag" always set to SchCreatedGo,
		// so task routine always goes when schimplCreateTask called, and later we
		// would not send poweron to an user task if it's flag (tsd[loop].Flag) is not
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

		if eno, ptn = schimplCreateTask(&tkd); eno != SchEnoNone {

			yclog.LogCallerFileLine("schimplSchedulerStart: " +
				"schimplCreateTask failed, task: %s",
				tkd.Name)

			return SchEnoParameter, nil
		}

		//
		// backup task node pointer by name
		//

		name2PtnMap[tkd.Name] = ptn

		//
		// send poweron event to task created aboved if it is required to be shceduled
		// at once; if the flag is SchCreatedSuspend, here JUST no poweron sending, BUT
		// the routine is going! see aboved pls.
		//

		if tsd[loop].Flag == SchCreatedGo {

			po.recver = ptn.(*schTaskNode)

			if eno = schimplSendMsg(&po); eno != SchEnoNone {

				yclog.LogCallerFileLine("schimplSchedulerStart: "+
					"schimplSendMsg failed, event: EvSchPoweron, eno: %d, task: %s",
					eno,
					tkd.Name)

				return eno, nil
			}
		}
	}

	//
	// send poweron event for those taskes registed in table "tpo" passed in
	//

	for _, name := range tpo {

		yclog.LogCallerFileLine("schimplSchedulerStart: send poweron to task: %s", name)

		eno, tsk := schimplGetTaskNodeByName(name)

		if eno != SchEnoNone {

			yclog.LogCallerFileLine("schimplSchedulerStart: " +
				"schimplGetTaskNodeByName failed, eno: %d, name: %s",
				eno,
				name)

			continue
		}

		if tsk == nil {

			yclog.LogCallerFileLine("schimplSchedulerStart: " +
				"nil task node pointer, eno: %d, name: %s",
				eno,
				name)

			continue
		}

		po.recver = tsk

		if eno = schimplSendMsg(&po); eno != SchEnoNone {

			yclog.LogCallerFileLine("schimplSchedulerStart: "+
				"schimplSendMsg failed, event: EvSchPoweron, eno: %d, task: %s",
				eno,
				name)

			return eno, nil
		}
	}

	//
	// run the dog
	//

	yclog.LogCallerFileLine("schimplSchedulerStart: go the watchdog")

	go wdCB.watchDogProc()

	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart: it's ok, ycp2p in running")
	golog.Printf("schimplSchedulerStart:")
	golog.Printf("schimplSchedulerStart:")

	return SchEnoNone, &name2PtnMap
}

//
// Watchdog task
//
type watchDogCtrlBlock struct {
	scb		*scheduler		// secheduler control block
	dogKill chan SchErrno	// dog killed signal
}

var wdCB = watchDogCtrlBlock {
	scb:		&p2pSDL,
	dogKill:	make(chan SchErrno, 1),
}

func (wd watchDogCtrlBlock)watchDogProc() SchErrno {

	var wdt	*time.Ticker
	var why = SchEnoNone

	wdt = time.NewTicker(schDeaultWatchCycle)
	defer wdt.Stop()

dogKilled:

	for {
		select {
		case <-wdt.C:
			//yclog.LogCallerFileLine("watchDogProc: dog time to watch")
			wd.dogWatch()

		case why = <-wd.dogKill:
			break dogKilled
		}
	}

	yclog.LogCallerFileLine("watchDogProc: " +
		"dog killed, why: %d",
		why)

	return SchEnoKilled
}

//
// Guard the user tasks going to "fly"
//
func (wd watchDogCtrlBlock) dogWatch() SchErrno {

	if wd.scb == nil {
		yclog.LogCallerFileLine("dogWatch: nil scheduler control block pointer")
		return SchEnoInternal
	}

	wd.scb.lock.Lock()
	defer wd.scb.lock.Unlock()

	var ptn *schTaskNode

	if ptn = wd.scb.tkBusy; ptn == nil {
		yclog.LogCallerFileLine("dogWatch: none of user tasks in scheduling")
		return SchEnoNone
	}

	//
	// 1) Scan the actived task link. one can improve this by link those user tasks have
	// a dog only to be scaned;
	// 2) Do not kill, debug out only even threshold reached;
	// 3) It's guarded per each event(message), means when an user task is shceduled to
	// deal with an event, the dog is feeded, the bited counter is cleaned to restart.
	//

	for {
		if ptn.task.dog.HaveDog == true && ptn.task.dog.Inited == true{

			ptn.task.dog.lock.Lock()

			if ptn.task.dog.BiteCounter++; ptn.task.dog.BiteCounter >= ptn.task.dog.DieThreshold {

				yclog.LogCallerFileLine("dogWatch: "+
					"in flying? task: %s, BiteCounter: %d",
					ptn.task.name,
					ptn.task.dog.BiteCounter)
			}

			ptn.task.dog.BiteCounter = 0

			ptn.task.dog.lock.Unlock()
		}

		if ptn = ptn.next; ptn == wd.scb.tkBusy {
			break
		}
	}

	return SchEnoNone
}
