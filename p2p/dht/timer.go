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
	"container/list"
)

const (
	secondCycle		= 64			// cycle in seconds
	minuteCycle		= 64			// cycle in minutes
	hourCycle		= 64			// cycle in hours
	dayCycle		= 64			// cycle in days
	oneTick			= time.Second	// unit tick to driver the timer manager
)

type timerCallback = func(el *list.Element, para interface{})interface{}

type timer struct {
	s			int				// seconds remain
	m			int				// minutes remain
	h			int				// hours remain
	d			int				// day remain
	data		interface{}		// pointer passed to callback
	tcb			timerCallback	// callback when timer expired
}

type timerManager struct {
	sp			int						// second pointer
	mp			int						// minute pointer
	hp			int						// hour pointer
	dp			int						// day pointer
	sTmList		[secondCycle]*list.List	// second timer list
	mTmList		[minuteCycle]*list.List	// minute timer list
	hTmList		[hourCycle]*list.List	// hour timer list
	dTmList		[dayCycle]*list.List	// day timer list
}

func getTimer(dur time.Duration, dat interface{}, tcb timerCallback) (*timer, error) {
	return nil, nil
}

func setTimerHandler(tm *timer, tcb timerCallback) error {
	return nil
}

func setTimerData(tm *timer, data interface{}) error {
	return nil
}

func setTimer(tm *timer) (*list.Element, error) {
	return nil, nil
}

func killTimer(el *list.Element) error {
	return nil
}

func tickProc() error {
	return nil
}



