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

package dht

import (
	"time"
	"fmt"
	"container/list"
)

const (
	oneTick			= time.Second					// unit tick to driver the timer manager
	xsecondBits		= 6
	xsecondCycle	= 1 << xsecondBits				// x-second cycle in tick
	xminuteBits		= 6
	xminuteCycle	= 1 << xminuteBits				// x-minute cycle in x-second
	xhourBits		= 6
	xhourCycle		= 1 << xhourBits				// x-hour cycle in x-minute
	xdayBits		= 6
	xdayCycle		= 1 << xdayBits					// x-day cycle in x-hour

	// min and max duration
	minDur			= oneTick * xsecondCycle
	maxDur			= (1 << (xsecondBits + xminuteBits + xhourBits + xdayBits)) * time.Second
)

type timerEno int

const (
	TmEnoNone			timerEno = iota		// none of errors
	TmEnoPara								// invalid parameters
	TmEnoDurTooBig							// duration too big
	TmEnoDurTooSmall						// duration too small
	TmEnoInternal							// internal errors
	TmEnoNotsupport							// not supported
	TmEnoBadTimer							// bad timer parameters
)

func (eno timerEno)Error() string {
	return fmt.Sprintf("%d", eno)
}

type timerCallback = func(el *list.Element, data interface{})interface{}

type timer struct {
	s			int							// seconds remain
	m			int							// minutes remain
	h			int							// hours remain
	d			int							// day remain
	data		interface{}					// pointer passed to callback
	tcb			timerCallback				// callback when timer expired
	li			*list.List					// list pointer
	el			*list.Element				// element pointer
	to			time.Time					// absolute time moment to be expired
	k			[]byte						// key attached to this timer
}

type timerManager struct {
	sp			int							// second pointer
	mp			int							// minute pointer
	hp			int							// hour pointer
	dp			int							// day pointer
	sTmList		[xsecondCycle]*list.List	// second timer list
	mTmList		[xminuteCycle]*list.List	// minute timer list
	hTmList		[xhourCycle]*list.List		// hour timer list
	dTmList		[xdayCycle]*list.List		// day timer list
}

func NewTimerManager() *timerManager {
	return &timerManager{}
}

func (mgr *timerManager)getTimer(dur time.Duration, dat interface{}, tcb timerCallback) (*timer, error) {
	if dur < minDur {
		return nil, TmEnoDurTooSmall
	}
	if dur > maxDur {
		return nil, TmEnoDurTooBig
	}
	if tcb == nil {
		return nil, TmEnoPara
	}

	ss := int64(dur.Seconds())
	xs := ss & (xsecondCycle - 1)
	ss = ss >> xsecondBits
	xm := ss & (xminuteCycle - 1)
	ss = ss >> xminuteBits
	xh := ss & (xhourCycle - 1)
	ss = ss >> xhourBits
	xd := ss & (xdayCycle - 1)

	tm := timer {
		s:		int(xs),
		m:		int(xm),
		h:		int(xh),
		d:		int(xd),
		data:	dat,
		tcb:	tcb,
		li:		nil,
		el:		nil,
	}

	return &tm, TmEnoNone
}

func (mgr *timerManager)setTimerHandler(tm *timer, tcb timerCallback) error {
	tm.tcb = tcb
	return TmEnoNone
}

func (mgr *timerManager)setTimerData(tm *timer, data interface{}) error {
	tm.data = data
	return TmEnoNone
}

func (mgr *timerManager)startTimer(tm *timer) error {
	if tm == nil {
		return TmEnoPara
	}

	targetLi := (*list.List)(nil)

	if tm.s > 0 {
		sp := (mgr.sp + tm.s) & (xsecondCycle - 1)
		if mgr.sTmList[sp] == nil {
			mgr.sTmList[sp] = list.New()
		}
		targetLi = mgr.sTmList[sp]
	} else if tm.m > 0 {
		mp := (mgr.mp + tm.s) & (xminuteCycle- 1)
		if mgr.mTmList[mp] == nil {
			mgr.mTmList[mp] = list.New()
		}
		targetLi = mgr.mTmList[mp]
	} else if tm.h > 0 {
		hp := (mgr.hp + tm.s) & (xhourCycle - 1)
		if mgr.hTmList[hp] == nil {
			mgr.hTmList[hp] = list.New()
		}
		targetLi = mgr.hTmList[hp]
	} else if tm.d > 0 {
		dp := (mgr.dp + tm.s) & (xdayCycle - 1)
		if mgr.dTmList[dp] == nil {
			mgr.dTmList[dp] = list.New()
		}
		targetLi = mgr.dTmList[dp]
	} else {
		return TmEnoBadTimer
	}

	targetEl := targetLi.PushBack(tm)
	tm.li = targetLi
	tm.el = targetEl

	return TmEnoNone
}

func (mgr *timerManager)killTimer(tm *timer) error {
	if tm == nil || tm.li == nil || tm.el == nil {
		return TmEnoPara
	}
	tm.li.Remove(tm.el)
	return TmEnoNone
}

func (mgr *timerManager)tickProc() error {
	if mgr.sp = (mgr.sp + 1) & (xsecondCycle - 1); mgr.sp == 0 {
		if mgr.mp = (mgr.mp + 1) & (xminuteCycle - 1); mgr.mp == 0 {
			if mgr.hp = (mgr.hp + 1) & (xhourCycle - 1); mgr.hp == 0 {
				mgr.dp = (mgr.dp + 1) & (xdayCycle - 1)
			}
		}
	}
	serr := mgr.spHandler(mgr.sTmList[mgr.sp])
	merr := mgr.mpHandler(mgr.mTmList[mgr.mp])
	herr := mgr.hpHandler(mgr.hTmList[mgr.hp])
	derr := mgr.dpHandler(mgr.dTmList[mgr.dp])

	if serr, merr, herr, derr != nil, nil, nil, nil {
		return TmEnoInternal
	}

	return TmEnoNone
}

func (mgr *timerManager)spHandler(li *list.List) error {
	if li != nil {
		for {
			if el := li.Front(); el == nil {
				break
			} else {
				tm, _ := el.Value.(*timer)
				if tm.m > 0 {
					mp := (mgr.mp + tm.m) & (xminuteCycle - 1)
					if mgr.mTmList[mp] == nil {
						mgr.mTmList[mp] = list.New()
					}
					mgr.mTmList[mp].PushBack(tm)
				} else if tm.h > 0 {
					hp := (mgr.hp + tm.h) & (xhourCycle - 1)
					if mgr.hTmList[hp] == nil {
						mgr.hTmList[hp] = list.New()
					}
					mgr.hTmList[hp].PushBack(tm)
				} else if tm.d > 0 {
					dp := (mgr.dp + tm.h) & (xdayCycle - 1)
					if mgr.dTmList[dp] == nil {
						mgr.dTmList[dp] = list.New()
					}
					mgr.dTmList[dp].PushBack(tm)
				} else {
					tm.tcb(el, tm.data)
				}
				li.Remove(el)
			}
		}
	}
	return TmEnoNone
}

func (mgr *timerManager)mpHandler(li *list.List) error {
	if li != nil {
		for {
			if el := li.Front(); el == nil {
				break
			} else {
				tm, _ := el.Value.(*timer)
				if tm.h > 0 {
					hp := (mgr.hp + tm.h) & (xhourCycle - 1)
					if mgr.hTmList[hp] == nil {
						mgr.hTmList[hp] = list.New()
					}
					mgr.hTmList[hp].PushBack(tm)
				} else if tm.d > 0 {
					dp := (mgr.dp + tm.h) & (xdayCycle - 1)
					if mgr.dTmList[dp] == nil {
						mgr.dTmList[dp] = list.New()
					}
					mgr.dTmList[dp].PushBack(tm)
				} else {
					tm.tcb(el, tm.data)
				}
				li.Remove(el)
			}
		}
	}
	return TmEnoNone
}

func (mgr *timerManager)hpHandler(li *list.List) error {
	if li != nil {
		for {
			if el := li.Front(); el == nil {
				break
			} else {
				tm, _ := el.Value.(*timer)
				if tm.d > 0 {
					dp := (mgr.dp + tm.h) & (xdayCycle - 1)
					if mgr.dTmList[dp] == nil {
						mgr.dTmList[dp] = list.New()
					}
					mgr.dTmList[dp].PushBack(tm)
				} else {
					tm.tcb(el, tm.data)
				}
				li.Remove(el)
			}
		}
	}
	return TmEnoNone
}

func (mgr *timerManager)dpHandler(li *list.List) error {
	if li != nil {
		for {
			if el := li.Front(); el == nil {
				break
			} else {
				tm, _ := el.Value.(*timer)
				tm.tcb(el, tm.data)
				li.Remove(el)
			}
		}
	}
	return TmEnoNone
}


