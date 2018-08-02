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
	sch	"github.com/yeeco/gyee/p2p/scheduler"
	config "github.com/yeeco/gyee/p2p/config"
	log "github.com/yeeco/gyee/p2p/logger"
)

//
// Constants
//
const (
	QryMgrName = sch.DhtQryMgrName		// query manage name registered in shceduler
	qryMgrMaxActInsts = 4				// max concurrent actived instances for one query
	qryMgrQryExpired = time.Second * 16	// duration to get expired for a query
)


//
// Query control block
//
type qryCtrlBlock struct {
	target		config.NodeID							// target is looking up
	qryHist		map[config.NodeID]*rutMgrBucketNode		// history peers had been queried
	qryPending	map[config.NodeID]*rutMgrBucketNode		// pending peers to be queried
	qryActived	map[config.NodeID]*qryInstCtrlBlock		// queries activated
}

//
// Query instance control block
//
type qryInstCtrlBlock struct {
	sdl			*sch.Scheduler		// pointer to scheduler
	name		string				// instance name
	ptnInst		interface{}			// pointer to query instance task node
	target		config.NodeID		// target is looking up
	to			rutMgrBucketNode	// to whom the query message sent
	qTid		int					// query timer identity
	begTime		time.Time			// query begin time
	endTime		time.Time			// query end time
}

//
// Query manager
//
type QryMgr struct {
	sdl			*sch.Scheduler					// pointer to scheduler
	name		string							// query manager name
	tep			sch.SchUserTaskEp				// task entry
	ptnMe		interface{}						// pointer to task node of myself
	ptnRutMgr	interface{}						// pointer to task node of route manager
	ptnDhtMgr	interface{}						// pointer to task node of dht manager
	instSeq		int								// query instance sequence number
	qcbTab		map[config.NodeID]*qryCtrlBlock	// query control blocks
}

//
// Create query manager
//
func NewQryMgr() *QryMgr {

	qryMgr := QryMgr{
		sdl:		nil,
		name:		QryMgrName,
		tep:		nil,
		ptnMe:		nil,
		ptnRutMgr:	nil,
		ptnDhtMgr:	nil,
		instSeq:	0,
		qcbTab:		map[config.NodeID]*qryCtrlBlock{},
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
// Query manager entry
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
		log.LogCallerFileLine("qryMgrProc: unknown event: %d", msg.Id)
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
