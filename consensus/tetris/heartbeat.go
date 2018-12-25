/*
 *  Copyright (C) 2017 gyee authors
 *
 *  This file is part of the gyee library.
 *
 *  The gyee library is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  The gyee library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package tetris

import "time"

//每个节点都检查2t+1个member有消息的时间，根据这个时间启动fire，调整event发送间隔。

type HeartBeat struct {
    m  map[uint]time.Time
}

func NewHeartBeat() *HeartBeat {
	h := &HeartBeat{
        m: make(map[uint]time.Time),
	}

	return h
}

func (hb *HeartBeat) MajorityBeatTime() (ok bool, duration time.Duration) {

	return true, 0
}

func (hb *HeartBeat) MemberRotate(joins []uint, quits []uint) {

}

