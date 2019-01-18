// Copyright (C) 2018 gyee authors
//
// This file is part of the gyee library.
//
// The gyee library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gyee library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with the gyee library.  If not, see <http://www.gnu.org/licenses/>.

package tetris2

import "time"

type Metrics struct {
	startTime     time.Time
	TrafficIn     uint64
	TrafficOut    uint64
	EventIn       uint64
	ParentEventIn uint64
	EventOut      uint64
	EventRequest  uint64
	TxIn          uint64
}

func NewMetrics() *Metrics {
	metrics := &Metrics{}
	metrics.startTime = time.Now()
	return metrics
}

func (m *Metrics) AddTrafficIn(traffic uint64) {
	m.TrafficIn += traffic
}

func (m *Metrics) AddTrafficOut(traffic uint64) {
	m.TrafficOut += traffic
}

func (m *Metrics) AddEventIn(num uint64) {
	m.EventIn += num
}

func (m *Metrics) AddParentEventIn(num uint64) {
	m.ParentEventIn += num
}

func (m *Metrics) AddEventOut(num uint64) {
	m.EventOut += num
}

func (m *Metrics) AddEventRequest(num uint64) {
	m.EventRequest += num
}

func (m *Metrics) AddTxIn(num uint64) {
	m.TxIn += num
}