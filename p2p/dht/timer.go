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
	"container/list"
	"fmt"
	"time"

	"github.com/yeeco/gyee/log"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	config "github.com/yeeco/gyee/p2p/config"
)

//
// debug
//
const (
	yeShellManagerTag = "yeShMgr"
)

type tmMgrLogger struct {
	debug__ bool
}

var tmLog = tmMgrLogger{
	debug__: false,
}

func (log tmMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

const (
	oneTick      = time.Second // unit tick to driver the timer manager
	OneTick      = oneTick
	xsecondBits  = 5
	xsecondCycle = 1 << xsecondBits // x-second cycle in tick
	xminuteBits  = 5
	xminuteCycle = 1 << xminuteBits // x-minute cycle in x-second
	xhourBits    = 5
	xhourCycle   = 1 << xhourBits // x-hour cycle in x-minute
	xdayBits     = 5
	xdayCycle    = 1 << xdayBits // x-day cycle in x-hour

	// min and max duration
	minDur = oneTick
	maxDur = (1 << (xsecondBits + xminuteBits + xhourBits + xdayBits)) * time.Second
	xsTickMaskBits = 0
	xmTickMaskBits = xsecondBits
	xhTickMaskBits = xsecondBits + xminuteBits
	xdTickMaskBits = xsecondBits + xminuteBits + xhourBits
	maxTicks = 1 << (xsecondBits + xminuteBits + xhourBits + xdayBits)
)

type timerEno int

const (
	TmEnoNone        timerEno = iota // none of errors
	TmEnoPara                        // invalid parameters
	TmEnoDurTooBig                   // duration too big
	TmEnoDurTooSmall                 // duration too small
	TmEnoInternal                    // internal errors
	TmEnoBadTimer                    // bad timer parameters
)

func (eno timerEno) Error() string {
	return fmt.Sprintf("%d", eno)
}

type TimerCallback = func(el *list.Element, data interface{}) interface{}

type timer struct {
	s		int				// seconds remain
	m		int				// minutes remain
	h		int				// hours remain
	d		int				// day remain
	t		int				// ticks
	r		int				// remain
	data	interface{}	// pointer passed to callback
	tcb		TimerCallback	// callback when timer expired
	li		*list.List		// list pointer for second
	el		*list.Element	// element pointer
	to		time.Time		// absolute time moment to be expired
	k		[]byte			// key attached to this timer
}

type TimerManager struct {
	sdl		string						// scheduler instance name
	tag		string						// tag(name) of manager
	sp      int							// second pointer
	mp      int							// minute pointer
	hp      int							// hour pointer
	dp      int							// day pointer
	sTmList [xsecondCycle]*list.List	// second timer list
	mTmList [xminuteCycle]*list.List	// minute timer list
	hTmList [xhourCycle]*list.List		// hour timer list
	dTmList [xdayCycle]*list.List		// day timer list
}

func NewTimerManager(sdl string, tag string) *TimerManager {
	return &TimerManager{
		sdl: sdl,
		tag: tag,
	}
}

func (mgr *TimerManager) GetTimer(dur time.Duration, dat interface{}, tcb TimerCallback) (interface{}, error) {
	if dur < minDur {
		log.Debugf("GetTimer: too small, dur: %f", dur.Seconds())
		return nil, TmEnoDurTooSmall
	}
	if dur > maxDur {
		log.Debugf("GetTimer: too big, dur: %f", dur.Seconds())
		return nil, TmEnoDurTooBig
	}
	if tcb == nil {
		log.Debugf("GetTimer: nil callback")
		return nil, TmEnoPara
	}

	ticks := int(dur.Seconds()/oneTick.Seconds())
	tm := timer{
		t: ticks,
		r: -1,
		data: dat,
		tcb:  tcb,
	}

	if mgr.tag == yeShellManagerTag {
		log.Debugf("GetTimer: sdl: %s, " +
			"t: %d, r: %d, s: %d, m: %d, h: %d, d: %d, sp: %d, mp: %d, hp: %d, dp: %d",
			mgr.sdl, tm.r, tm.t, tm.s, tm.m, tm.h, tm.d, mgr.sp, mgr.mp, mgr.hp, mgr.dp, )
	}

	return &tm, TmEnoNone
}

func (mgr *TimerManager) SetTimerHandler(ptm interface{}, tcb TimerCallback) error {
	tm := ptm.(*timer)
	tm.tcb = tcb
	return TmEnoNone
}

func (mgr *TimerManager) SetTimerData(ptm interface{}, data interface{}) error {
	tm := ptm.(*timer)
	tm.data = data
	return TmEnoNone
}

func (mgr *TimerManager) SetTimerKey(ptm interface{}, key []byte) error {
	tm := ptm.(*timer)
	tm.k = append(tm.k, key...)
	return TmEnoNone
}

func (mgr *TimerManager) StartTimer(ptm interface{}) error {
	tm, ok := ptm.(*timer)
	if tm == nil || !ok {
		return TmEnoPara
	}

	if mgr.tag == yeShellManagerTag {
		key := tm.data.(*config.DsKey)
		log.Debugf("StartTimer: sdl: %s, " +
			"t: %d, r: %d, s: %d, m: %d, h: %d, d: %d, sp: %d, mp: %d, hp: %d, dp: %d, key: %x",
			mgr.sdl, tm.t, tm.r, tm.s, tm.m, tm.h, tm.d, mgr.sp, mgr.mp, mgr.hp, mgr.dp, *key)
	}

	targetLi := (*list.List)(nil)
	targetEl := (*list.Element)(nil)
	compensate :=	mgr.sp +
					(mgr.mp << xsecondBits) +
					(mgr.hp << (xsecondBits + xminuteBits)) +
					(mgr.dp << (xsecondBits + xminuteBits + xhourBits))
	absTicks := (tm.t + compensate) & (maxTicks - 1)
	xd := -1
	xh := -1
	xm := -1
	xs := -1
	r := -1
	sp := -1
	mp := -1
	hp := -1
	dp := -1
	if d := tm.t >> xdTickMaskBits; d > 0 {
		xd = absTicks >> xdTickMaskBits
		r = absTicks & ((1 << xdTickMaskBits) - 1)
		dp = (mgr.dp + xd) & (xdayCycle - 1)
		if mgr.dTmList[dp] == nil {
			mgr.dTmList[dp] = list.New()
		}
		targetLi = mgr.dTmList[dp]
		targetEl = targetLi.PushBack(tm)
	} else if h := tm.t >> xhTickMaskBits; h > 0 {
		xh = absTicks >> xhTickMaskBits
		r = absTicks & ((1 << xhTickMaskBits) - 1)
		hp = (mgr.hp + xh) & (xhourCycle - 1)
		if mgr.hTmList[hp] == nil {
			mgr.hTmList[hp] = list.New()
		}
		targetLi = mgr.hTmList[hp]
		targetEl = targetLi.PushBack(tm)
	} else if m := tm.t >> xmTickMaskBits; m > 0 {
		xm = absTicks >> xmTickMaskBits
		r = absTicks & ((1 << xmTickMaskBits) - 1)
		mp = (mgr.mp + xm) & (xminuteCycle - 1)
		if mgr.mTmList[mp] == nil {
			mgr.mTmList[mp] = list.New()
		}
		targetLi = mgr.mTmList[mp]
		targetEl = targetLi.PushBack(tm)
	} else {
		xs = tm.t
		r = 0
		sp = (mgr.sp + xs) & (xsecondCycle - 1)
		if mgr.sTmList[sp] == nil {
			mgr.sTmList[sp] = list.New()
		}
		targetLi = mgr.sTmList[sp]
		targetEl = targetLi.PushBack(tm)
	}
	tm.s = xs
	tm.m = xm
	tm.h = xh
	tm.d = xd
	tm.r = r
	tm.li = targetLi
	tm.el = targetEl

	if mgr.tag == yeShellManagerTag {
		key := tm.data.(*config.DsKey)
		log.Debugf("StartTimer: sdl: %s, " +
			"t: %d, r: %d, s: %d, m: %d, h: %d, d: %d, _sp: %d, _mp: %d, _hp: %d, _dp: %d, key: %x",
			mgr.sdl, tm.r, tm.t, tm.s, tm.m, tm.h, tm.d, sp, mp, hp, dp, *key)
	}

	return TmEnoNone
}

func (mgr *TimerManager) KillTimer(ptm interface{}) error {
	if ptm == nil {
		log.Debugf("KillTimer: invalid timer pointer")
		return TmEnoPara
	}
	tm := ptm.(*timer)
	if tm.li == nil || tm.el == nil {
		log.Debugf("KillTimer: internal errors")
		return TmEnoBadTimer
	}
	tm.li.Remove(tm.el)
	return TmEnoNone
}

func (mgr *TimerManager) TickProc() error {
	if mgr.sp = (mgr.sp + 1) & (xsecondCycle - 1); mgr.sp == 0 {
		if mgr.mp = (mgr.mp + 1) & (xminuteCycle - 1); mgr.mp == 0 {
			if mgr.hp = (mgr.hp + 1) & (xhourCycle - 1); mgr.hp == 0 {
				mgr.dp = (mgr.dp + 1) & (xdayCycle - 1)
			}
		}
	}
	derr := mgr.dpHandler(mgr.dTmList[mgr.dp])
	herr := mgr.hpHandler(mgr.hTmList[mgr.hp])
	merr := mgr.mpHandler(mgr.mTmList[mgr.mp])
	serr := mgr.spHandler(mgr.sTmList[mgr.sp])

	if serr != TmEnoNone || merr != TmEnoNone || herr != TmEnoNone || derr != TmEnoNone {
		log.Debugf("TickProc: internal errors, serr: %s, merr: %s, herr: %s, derr: %s",
			serr.Error(), merr.Error(), herr.Error(), derr.Error())
		return TmEnoInternal
	}
	return TmEnoNone
}

func (mgr *TimerManager) spHandler(li *list.List) error {
	if eno := mgr.handler(li); eno != TmEnoNone {
		log.Debugf("spHandler: handler failed, eno: %d", eno)
		return eno
	}
	return TmEnoNone
}

func (mgr *TimerManager) mpHandler(li *list.List) error {
	if eno := mgr.handler(li); eno != TmEnoNone {
		log.Debugf("mpHandler: handler failed, eno: %d", eno)
		return eno
	}
	return TmEnoNone
}

func (mgr *TimerManager) hpHandler(li *list.List) error {
	if eno := mgr.handler(li); eno != TmEnoNone {
		log.Debugf("hpHandler: handler failed, eno: %d", eno)
		return eno
	}
	return TmEnoNone
}

func (mgr *TimerManager) dpHandler(li *list.List) error {
	if eno := mgr.handler(li); eno != TmEnoNone {
		log.Debugf("dpHandler: handler failed, eno: %d", eno)
		return eno
	}
	return TmEnoNone
}

func (mgr *TimerManager) handler(li *list.List) error {
	if li != nil {
		for ; li.Len() > 0; {
			el := li.Front()
			if eno := mgr.hit(li, el); eno != TmEnoNone {
				log.Debugf("handler: hit failed, eno: %d", eno)
				return eno
			}
		}
	}
	return TmEnoNone
}

func (mgr *TimerManager) hit(li *list.List, el *list.Element) error {
	tm, _ := el.Value.(*timer)
	li.Remove(el)
	tm.li = nil
	tm.el = nil
	tm.d = -1
	if tm.r == 0 {
		tm.tcb(el, tm.data)
	} else {
		tm.t = tm.r
		tm.r = -1
		if eno := mgr.StartTimer(tm); eno != TmEnoNone {
			log.Debugf("hit: start timer failed, eno: %d", eno)
			return eno
		}
	}
	return TmEnoNone
}