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
	sch	"github.com/yeeco/gyee/p2p/scheduler"
)

const QryMgrName = sch.DhtQryMgrName

type QryMgr struct {
	sdl		*sch.Scheduler		// pointer to scheduler
	name	string				// my name
	tep		sch.SchUserTaskEp	// task entry
	ptnMe	interface{}			// pointer to task node of myself
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qryMgr := QryMgr{
		sdl:	nil,
		name:	QryMgrName,
		tep:	nil,
		ptnMe:	nil,
	}

	qryMgr.tep = qryMgr.qryMgrProc

	return &qryMgr
}

//
// Entry point exported to shceduler
//
func (qryMgr *QryMgr)TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return qryMgr.tep(ptn, msg)
}

//
// Discover manager entry
//
func (qryMgr *QryMgr)qryMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {

	eno := sch.SchEnoUnknown

	switch msg.Id {

	case sch.EvSchPoweron:
		eno = qryMgr.poweron(ptn)

	case sch.EvSchPoweroff:
		eno = qryMgr.poweroff(ptn)

	case sch.EvDhtQryMgrQueryStartReq:
		eno = qryMgr.queryStartReq(msg.Body.(*sch.MsgDhtQryMgrQueryStartReq))

	case sch.EvDhtQryMgrQueryStopReq:
		eno = qryMgr.queryStopReq(msg.Body.(*sch.MsgDhtQryMgrQueryStopReq))

	case sch.EvDhtRutMgrNotificationInd:
		eno = qryMgr.rutNotificationInd(msg.Body.(*sch.MsgDhtRutMgrNotificationInd))

	case sch.EvDhtQryInstResultInd:
		eno = qryMgr.instResultInd(msg.Body.(*sch.MsgDhtQryInstResultInd))

	case sch.EvDhtQryInstStopRsp:
		eno = qryMgr.instStopRsp(msg.Body.(*sch.MsgDhtQryInstStopRsp))

	default:
		eno = sch.SchEnoParameter
	}

	return eno
}

//
// Poweron handler
//
func (qryMgr *QryMgr)poweron(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Poweroff handler
//
func (qryMgr *QryMgr)poweroff(ptn interface{}) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Query start request handler
//
func (qryMgr *QryMgr)queryStartReq(msg *sch.MsgDhtQryMgrQueryStartReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Query stop request handler
//
func (qryMgr *QryMgr)queryStopReq(msg *sch.MsgDhtQryMgrQueryStopReq) sch.SchErrno {
	return sch.SchEnoNone
}

//
//Route notification handler
//
func (qryMgr *QryMgr)rutNotificationInd(msg *sch.MsgDhtRutMgrNotificationInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance query result indication handler
//
func (qryMgr *QryMgr)instResultInd(msg *sch.MsgDhtQryInstResultInd) sch.SchErrno {
	return sch.SchEnoNone
}

//
// Instance stop response handler
//
func (qryMgr *QryMgr)instStopRsp(msg *sch.MsgDhtQryInstStopRsp) sch.SchErrno {
	return sch.SchEnoNone
}
