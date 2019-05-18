// Copyright (C) 2019 gyee authors
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

package nat

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/yeeco/gyee/log"
	config "github.com/yeeco/gyee/p2p/config"
	p2plog "github.com/yeeco/gyee/p2p/logger"
	sch "github.com/yeeco/gyee/p2p/scheduler"
)

//
// debug
//
type natMgrLogger struct {
	debug__ bool
}

var natLog = natMgrLogger{
	debug__: false,
}

func (log natMgrLogger) Debug(fmt string, args ...interface{}) {
	if log.debug__ {
		p2plog.Debug(fmt, args...)
	}
}

//
// errno
//
type NatEno int

const (
	NatEnoNone = NatEno(iota)
	NatEnoParameter
	NatEnoNotFound
	NatEnoDuplicated
	NatEnoMismatched
	NatEnoScheduler
	NatEnoFromPmpLib
	NatEnoFromUpnpLib
	NatEnoFromSystem
	NatEnoNoNat
	NatEnoNullNat
	NatEnoUnknown
)

func (ne NatEno) Error() string {
	return fmt.Sprintf("NatEno: %d", ne)
}

func (ne NatEno) Errno() int {
	return int(ne)
}

//
// configuration
//
const (
	NATT_NONE = config.NATT_NONE
	NATT_PMP  = config.NATT_PMP
	NATT_UPNP = config.NATT_UPNP
	NATT_ANY  = config.NATT_ANY
)

type natConfig struct {
	natType string // "pmp", "upnp", "none"
	gwIp    net.IP // gateway ip address when "pmp" specified
}

//
// refresh the mapping before it's expired
//
const (
	MinKeepDuration = time.Minute * 20
	MinRefreshDelta = time.Minute * 5
	MaxRefreshDelta = time.Minute * 10
)

//
// interface for nat
//

var natLock sync.Mutex

type natInterface interface {

	// make map between local address to public address
	makeMap(name string, proto string, locPort int, pubPort int, durKeep time.Duration) NatEno

	// remove map make by makeMap
	removeMap(proto string, locPort int, pubPort int) NatEno

	// get public address
	getPublicIpAddr() (net.IP, NatEno)
}

//
// map instance
//
const (
	NATP_TCP = "tcp"
	NATP_UDP = "udp"
)

type NatMapInstID struct {
	proto    string // the prototcol, "tcp" or "udp"
	fromPort int    // local port number be mapped
}

func (id NatMapInstID) toString() string {
	return fmt.Sprintf("%s:%d", id.proto, id.fromPort)
}

//
// map instance
//
type NatMapInstance struct {
	owner      interface{}   // owner task pointer
	id         NatMapInstID  // map item identity
	toPort     int           // target port number requested
	durKeep    time.Duration // duration for map to be kept
	durRefresh time.Duration // interval duration to refresh the map
	tidRefresh int           // timer identity of refresh timer
	status     NatEno        // map status
	pubIp      net.IP        // public address
	pubPort    int           // public port
}

//
// nat manager
//
const NatMgrName = sch.NatMgrName
const debugPubAddrSwitch = false

type NatManager struct {
	sdl       *sch.Scheduler                   // pointer to scheduler
	name      string                           // name
	ptnMe     interface{}                      // myself task node pointer
	ptnDhtMgr interface{}                      // pointer to table manager
	ptnTabMgr interface{}                      // pointer to dht manager
	tep       sch.SchUserTaskEp                // entry
	cfg       natConfig                        // configuration
	nat       natInterface                     // nil or pointer to pmpCtrlBlock or upnpCtrlBlock
	instTab   map[NatMapInstID]*NatMapInstance // instance table
}

func NewNatMgr() *NatManager {
	var natMgr = NatManager{
		name:    NatMgrName,
		instTab: make(map[NatMapInstID]*NatMapInstance, 0),
	}
	natMgr.tep = natMgr.natMgrProc
	natMgr.nat = nil
	return &natMgr
}

func (natMgr *NatManager) TaskProc4Scheduler(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return natMgr.tep(ptn, msg)
}

func (natMgr *NatManager) natMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	var eno sch.SchErrno
	switch msg.Id {
	case sch.EvSchPoweron:
		eno = natMgr.poweron(ptn)
	case sch.EvSchPoweroff:
		eno = natMgr.poweroff(msg)
	case sch.EvNatMgrDiscoverReq:
		eno = natMgr.discoverReq(msg)
	case sch.EvNatRefreshTimer:
		eno = natMgr.refreshTimerHandler(msg)
	case sch.EvNatMgrMakeMapReq:
		eno = natMgr.makeMapReq(msg)
	case sch.EvNatMgrRemoveMapReq:
		eno = natMgr.removeMapReq(msg)
	case sch.EvNatMgrGetPublicAddrReq:
		eno = natMgr.getPubAddrReq(msg)
	case sch.EvNatDebugTimer:
		eno = natMgr.debugTimer()
	default:
		log.Debugf("natMgrProc: unknown message: %d", msg.Id)
		eno = sch.SchEnoParameter
	}
	return eno
}

func (natMgr *NatManager) poweron(ptn interface{}) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	natMgr.ptnMe = ptn
	natMgr.sdl = sch.SchGetScheduler(ptn)

	appt := natMgr.sdl.SchGetAppType()
	if appt == int(config.P2P_TYPE_CHAIN) {
		if _, natMgr.ptnTabMgr = natMgr.sdl.SchGetUserTaskNode(sch.TabMgrName); natMgr.ptnTabMgr == nil {
			log.Debugf("poweron: SchGetUserTaskNode failed with task name: %s", sch.TabMgrName)
		}
	} else if appt == int(config.P2P_TYPE_DHT) {
		if _, natMgr.ptnDhtMgr = natMgr.sdl.SchGetUserTaskNode(sch.DhtMgrName); natMgr.ptnDhtMgr == nil {
			log.Debugf("poweron: SchGetUserTaskNode failed with task name: %s", sch.DhtMgrName)
		}
	} else {
		log.Debugf("poweron: unknown application type: %d", appt)
	}

	if eno := natMgr.getConfig(); eno != NatEnoNone {
		log.Debugf("poweron: getConfig failed, error: %s", eno.Error())
		return sch.SchEnoUserTask
	}

	if eno := natMgr.setupNatInterface(); eno != NatEnoNone {
		log.Debugf("poweron: setupNatInterface failed, error: %s", eno.Error())
		return sch.SchEnoUserTask
	}

	ind := sch.MsgNatMgrReadyInd{
		NatType: natMgr.cfg.natType,
	}

	if natMgr.ptnTabMgr != nil {
		msg2Tab := sch.SchMessage{}
		if natMgr.ptnTabMgr != nil {
			natMgr.sdl.SchMakeMessage(&msg2Tab, natMgr.ptnMe, natMgr.ptnTabMgr, sch.EvNatMgrReadyInd, &ind)
			natMgr.sdl.SchSendMessage(&msg2Tab)
		}
	}

	if natMgr.ptnDhtMgr != nil {
		msg2Dht := sch.SchMessage{}
		if natMgr.ptnDhtMgr != nil {
			natMgr.sdl.SchMakeMessage(&msg2Dht, natMgr.ptnMe, natMgr.ptnDhtMgr, sch.EvNatMgrReadyInd, &ind)
			natMgr.sdl.SchSendMessage(&msg2Dht)
		}
	}

	if debugPubAddrSwitch {
		td := sch.TimerDescription{
			Name:  "natDebugTimer",
			Utid:  sch.NatMgrDebugTimerId,
			Tmt:   sch.SchTmTypeAbsolute,
			Dur:   time.Second * 60,
			Extra: nil,
		}
		eno, _ := natMgr.sdl.SchSetTimer(natMgr.ptnMe, &td)
		if eno != sch.SchEnoNone {
			log.Debugf("startRefreshTimer: SchSetTimer NatMgrDebugTimerId failed, eno: %d", eno)
			return sch.SchEnoUserTask
		}
	}

	return sch.SchEnoNone
}

func (natMgr *NatManager) poweroff(msg *sch.SchMessage) sch.SchErrno {
	log.Debugf("lsnMgrPoweroff: task will be done, name: %s", natMgr.name)
	natMgr.stop()
	return natMgr.sdl.SchTaskDone(natMgr.ptnMe, natMgr.name, sch.SchEnoKilled)
}

func (natMgr *NatManager) discoverReq(msg *sch.SchMessage) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	// notice: "ANY" type is not supported by reconfiguration, so it's not
	// supported by EvNatMgrDiscoverReq.
	var eno = NatEnoNone
	sender := natMgr.sdl.SchGetSender(msg)
	dcvReq, _ := msg.Body.(*sch.MsgNatMgrDiscoverReq)
	if strings.Compare(natMgr.cfg.natType, dcvReq.NatType) != 0 {
		eno = natMgr.reconfig(dcvReq)
	} else if natMgr.cfg.natType == NATT_PMP && bytes.Compare(natMgr.cfg.gwIp, dcvReq.GwIp) != 0 {
		eno = natMgr.reconfig(dcvReq)
	}
	rsp := sch.MsgNatMgrDiscoverRsp{
		Result: eno.Errno(),
	}
	schMsg := sch.SchMessage{}
	natMgr.sdl.SchMakeMessage(&schMsg, natMgr.ptnMe, sender, sch.EvNatMgrDiscoverRsp, &rsp)
	return natMgr.sdl.SchSendMessage(&schMsg)
}

func (natMgr *NatManager) refreshTimerHandler(msg *sch.SchMessage) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	inst, _ := msg.Body.(*NatMapInstance)
	if eno := natMgr.refreshInstance(inst); eno != NatEnoNone {
		log.Debugf("refreshTimerHandler: refreshInstance failed, error: %s", eno.Error())
		return sch.SchEnoUserTask
	}
	return sch.SchEnoNone
}

func (natMgr *NatManager) makeMapReq(msg *sch.SchMessage) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	var (
		eno      = NatEnoNone
		status   = NatEnoUnknown
		proto    = ""
		fromPort = -1
		pubIp    = net.IPv4zero
		pubPort  = -1
		id       NatMapInstID
		inst     NatMapInstance
	)

	var ip net.IP
	var s NatEno

	sender := natMgr.sdl.SchGetSender(msg)
	mmr, _ := msg.Body.(*sch.MsgNatMgrMakeMapReq)

	if natMgr.cfg.natType == NATT_NONE {
		status = NatEnoNoNat
		proto = fmt.Sprintf("%s", mmr.Proto)
		fromPort = mmr.FromPort
		pubPort = mmr.FromPort
		goto _rsp2sender
	}

	if eno = natMgr.checkMakeMapReq(mmr); eno != NatEnoNone {
		log.Debugf("makeMapReq: checkMakeMapReq failed, error: %s", eno.Error())
		goto _rsp2sender
	}

	id = NatMapInstID{
		proto:    mmr.Proto,
		fromPort: mmr.FromPort,
	}

	if _, ok := natMgr.instTab[id]; ok {
		log.Debugf("makeMapReq: duplicated, id: %+v", id)
		eno = NatEnoDuplicated
		goto _rsp2sender
	}

	inst = NatMapInstance{
		owner:      sender,
		id:         id,
		toPort:     mmr.ToPort,
		durKeep:    mmr.DurKeep,
		durRefresh: mmr.DurRefresh,
		tidRefresh: sch.SchInvalidTid,
		status:     NatEnoUnknown,
		pubIp:      net.IPv4zero,
		pubPort:    mmr.ToPort,
	}

	if eno := natMgr.nat.makeMap(inst.id.toString(), inst.id.proto, inst.id.fromPort, inst.toPort, inst.durKeep); eno != NatEnoNone {
		log.Debugf("makeMapReq: makeMap failed, error: %s", eno.Error())
		goto _rsp2sender
	}
	if eno := natMgr.startRefreshTimer(&inst); eno != NatEnoNone {
		log.Debugf("makeMapReq: makeMap failed, error: %s", eno.Error())
		goto _rsp2sender

	}
	ip, s = natMgr.nat.getPublicIpAddr()
	natMgr.instTab[id] = &inst
	inst.pubIp = ip
	inst.status = s

	proto = fmt.Sprintf("%s", inst.id.proto)
	fromPort = mmr.FromPort
	pubIp = inst.pubIp
	pubPort = inst.pubPort
	status = inst.status

_rsp2sender:

	rsp := sch.MsgNatMgrMakeMapRsp{
		Result:   eno.Errno(),
		Status:   status.Errno(),
		Proto:    proto,
		FromPort: fromPort,
		PubIp:    pubIp,
		PubPort:  pubPort,
	}
	log.Debugf("makeMapReq: respone to sender %s with rsp: %+v",
		natMgr.sdl.SchGetTaskName(sender), rsp)
	schMsg := sch.SchMessage{}
	natMgr.sdl.SchMakeMessage(&schMsg, natMgr.ptnMe, sender, sch.EvNatMgrMakeMapRsp, &rsp)
	return natMgr.sdl.SchSendMessage(&schMsg)
}

func (natMgr *NatManager) removeMapReq(msg *sch.SchMessage) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	eno := NatEnoUnknown
	rmr, _ := msg.Body.(*sch.MsgNatMgrRemoveMapReq)
	sender := natMgr.sdl.SchGetSender(msg)

	id := NatMapInstID{
		proto:    rmr.Proto,
		fromPort: rmr.FromPort,
	}
	inst, ok := natMgr.instTab[id]
	if !ok {
		eno = NatEnoMismatched
		goto _rsp2sender
	}

	if natMgr.cfg.natType != NATT_NONE {
		if inst.status == NatEnoNone {
			if eno = natMgr.nat.removeMap(inst.id.proto, inst.id.fromPort, inst.pubPort); eno != NatEnoNone {
				log.Debugf("removeMapReq: removeMap failed, error: %s", eno.Error())
				goto _rsp2sender
			}
		}
		if inst.tidRefresh != sch.SchInvalidTid {
			if schEno := natMgr.sdl.SchKillTimer(natMgr.ptnMe, inst.tidRefresh); schEno != sch.SchEnoNone {
				log.Debugf("removeMapReq: SchKillTimer failed, eno: %d", schEno)
				eno = NatEnoScheduler
				goto _rsp2sender
			}
		}
	}

	delete(natMgr.instTab, id)
	eno = NatEnoNone

_rsp2sender:
	rsp := sch.MsgNatMgrRemoveMapRsp{
		Result: eno.Errno(),
	}
	schMsg := sch.SchMessage{}
	natMgr.sdl.SchMakeMessage(&schMsg, natMgr.ptnMe, sender, sch.EvNatMgrRemoveMapRsp, &rsp)
	return natMgr.sdl.SchSendMessage(&schMsg)
}

func (natMgr *NatManager) getPubAddrReq(msg *sch.SchMessage) sch.SchErrno {
	natLock.Lock()
	defer natLock.Unlock()

	if natMgr.cfg.natType == NATT_NONE {
		log.Debugf("getPubAddrReq: type mismatche, current: %s", natMgr.cfg.natType)
		return sch.SchEnoUserTask
	}

	var (
		eno     = NatEnoUnknown
		status  = NatEnoUnknown
		pubIp   = net.IPv4zero
		pubPort = -1
		id      NatMapInstID
		inst    *NatMapInstance
	)
	var ip net.IP
	var s NatEno

	gar, _ := msg.Body.(*sch.MsgNatMgrGetPublicAddrReq)
	sender := natMgr.sdl.SchGetSender(msg)
	id = NatMapInstID{
		proto:    gar.Proto,
		fromPort: gar.FromPort,
	}
	_, ok := natMgr.instTab[id]
	if !ok {
		eno = NatEnoMismatched
		goto _rsp2sender
	}

	if natMgr.nat == nil {
		status = NatEnoNullNat
	} else {
		inst = natMgr.instTab[id]
		ip, s = natMgr.nat.getPublicIpAddr()
		inst.pubIp = ip
		inst.status = s
		eno = NatEnoNone
		pubIp = inst.pubIp
		pubPort = inst.pubPort
	}

_rsp2sender:
	rsp := sch.MsgNatMgrGetPublicAddrRsp{
		Result:  eno.Errno(),
		Status:  status.Errno(),
		PubIp:   pubIp,
		PubPort: pubPort,
	}
	schMsg := sch.SchMessage{}
	natMgr.sdl.SchMakeMessage(&schMsg, natMgr.ptnMe, sender, sch.EvNatMgrGetPublicAddrRsp, &rsp)
	return natMgr.sdl.SchSendMessage(&schMsg)
}

func (natMgr *NatManager) getConfig() NatEno {
	cfg := config.P2pConfig4NatManager(natMgr.sdl.SchGetP2pCfgName())
	natMgr.cfg.natType = fmt.Sprintf("%s", cfg.NatType)
	natMgr.cfg.gwIp = append(natMgr.cfg.gwIp, cfg.GwIp...)
	return NatEnoNone
}

func (natMgr *NatManager) setupNatInterface() NatEno {
	if natMgr.cfg.natType == NATT_NONE {
		natMgr.nat = nil
	} else if natMgr.cfg.natType == NATT_PMP {
		natMgr.nat = NewPmpInterface(natMgr.cfg.gwIp)
	} else if natMgr.cfg.natType == NATT_UPNP {
		natMgr.nat = NewUpnpInterface()
	} else if natMgr.cfg.natType == NATT_ANY {
		if natMgr.cfg.gwIp != nil && !natMgr.cfg.gwIp.Equal(net.IPv4zero) {
			if natMgr.nat = NewPmpInterface(natMgr.cfg.gwIp); natMgr.nat != nil {
				if !reflect.ValueOf(natMgr.nat).IsNil() {
					if _, eno := natMgr.nat.getPublicIpAddr(); eno != NatEnoNone {
						natMgr.nat = nil
					} else {
						natMgr.cfg.natType = NATT_PMP
					}
				}
			}
		} else {
			if gws, eno := guessPossibleGateways(); eno == NatEnoNone {
				for _, gwIp := range gws {
					if natMgr.nat = NewPmpInterface(gwIp); natMgr.nat != nil {
						if !reflect.ValueOf(natMgr.nat).IsNil() {
							if _, eno := natMgr.nat.getPublicIpAddr(); eno != NatEnoNone {
								natMgr.nat = nil
							} else {
								natMgr.cfg.natType = NATT_PMP
								natMgr.cfg.gwIp = gwIp
								break
							}
						}
					}
				}
			}
		}

		if natMgr.nat == nil || reflect.ValueOf(natMgr.nat).IsNil() {
			if natMgr.nat = NewUpnpInterface(); natMgr.nat != nil {
				natMgr.cfg.natType = NATT_UPNP
			}
		}
	} else {
		log.Debugf("setupNatInterface: invalid nat type: %s", natMgr.cfg.natType)
		return NatEnoParameter
	}

	if natMgr.cfg.natType != NATT_NONE && reflect.ValueOf(natMgr.nat).IsNil() {
		log.Debugf("setupNatInterface: null nat, natType: %s", natMgr.cfg.natType)
		return NatEnoNullNat
	}

	return NatEnoNone
}

func (natMgr *NatManager) stop() {
	for _, inst := range natMgr.instTab {
		if eno := natMgr.deleteInstance(inst); eno != NatEnoNone {
			log.Debugf("stopInstance: failed, id: %+v", inst.id)
		}
	}
	natMgr.nat = nil
}

func (natMgr *NatManager) deleteInstance(inst *NatMapInstance) NatEno {
	if inst == nil {
		log.Debugf("deleteInstance: invalid instance")
		return NatEnoParameter
	}
	if natMgr.cfg.natType != NATT_NONE {
		if inst.tidRefresh == sch.SchInvalidTid {
			log.Debugf("deleteInstance: invalid timer")
			return NatEnoParameter
		}
		if eno := natMgr.sdl.SchKillTimer(natMgr.ptnMe, inst.tidRefresh); eno != sch.SchEnoNone {
			log.Debugf("deleteInstance: SchKillTimer failed, eno: %d", eno)
			return NatEnoScheduler
		}
		inst.tidRefresh = sch.SchInvalidTid
	}
	delete(natMgr.instTab, inst.id)
	return NatEnoNone
}

func (natMgr *NatManager) reconfig(dcvReq *sch.MsgNatMgrDiscoverReq) NatEno {
	// notice: "ANY" type is not supported by reconfiguration
	if dcvReq == nil {
		log.Debugf("reconfig: invalid parameters")
		return NatEnoParameter
	}
	switch dcvReq.NatType {
	case NATT_NONE, NATT_UPNP:
	case NATT_PMP:
		if dcvReq.GwIp == nil {
			log.Debugf("reconfig: invalid GwIp for type: %s", NATT_PMP)
			return NatEnoParameter
		}
	default:
		log.Debugf("reconfig: invalid type: %s", dcvReq.NatType)
		return NatEnoParameter
	}
	natMgr.stop()
	natMgr.cfg.natType = dcvReq.NatType
	if dcvReq.NatType == NATT_PMP {
		natMgr.cfg.gwIp = dcvReq.GwIp
	}
	return natMgr.setupNatInterface()
}

func (natMgr *NatManager) refreshInstance(inst *NatMapInstance) NatEno {
	if _, ok := natMgr.instTab[inst.id]; !ok {
		log.Debugf("refreshInstance: instance not exist, id: %+v", inst.id)
		return NatEnoMismatched
	}
	eno := natMgr.nat.makeMap(inst.id.toString(), inst.id.proto, inst.id.fromPort, inst.toPort, inst.durKeep)
	if eno != NatEnoNone {
		log.Debugf("refreshInstance: makeMap failed, inst: %+v", *inst)
		return eno
	}

	// when failed to get public address, we do not send indication, so nat client will
	// keep the old public address version and go on.
	if curIp, eno := natMgr.nat.getPublicIpAddr(); eno != NatEnoNone {
		log.Debugf("refreshInstance: getPublicIpAddr failed, error: %s", eno.Error())
	} else {
		if bytes.Compare(inst.pubIp, curIp) != 0 {
			inst.pubIp = curIp
			ind := sch.MsgNatMgrPubAddrUpdateInd{
				Status:   NatEnoNone.Errno(),
				Proto:    inst.id.proto,
				FromPort: inst.id.fromPort,
				PubIp:    inst.pubIp,
				PubPort:  inst.pubPort,
			}
			log.Debugf("refreshInstance: send to %s with ind: %v", natMgr.sdl.SchGetTaskName(inst.owner), ind)
			schMsg := sch.SchMessage{}
			natMgr.sdl.SchMakeMessage(&schMsg, natMgr.ptnMe, inst.owner, sch.EvNatMgrPubAddrUpdateInd, &ind)
			natMgr.sdl.SchSendMessage(&schMsg)
		}
	}

	return natMgr.startRefreshTimer(inst)
}

func (natMgr *NatManager) checkMakeMapReq(mmr *sch.MsgNatMgrMakeMapReq) NatEno {
	if mmr == nil {
		log.Debugf("checkMakeMapReq: invalid prameters")
		return NatEnoParameter
	}
	if strings.Compare(strings.ToLower(mmr.Proto), NATP_UDP) != 0 &&
		strings.Compare(strings.ToLower(mmr.Proto), NATP_TCP) != 0 {
		log.Debugf("checkMakeMapReq: invalid protocol: %s", mmr.Proto)
		return NatEnoParameter
	}
	if mmr.DurKeep < MinKeepDuration {
		log.Debugf("checkMakeMapReq: invalid DurKeep: %d, min: %d", mmr.DurKeep, MinKeepDuration)
		return NatEnoParameter
	}
	if mmr.DurRefresh != time.Duration(0) {
		if !(mmr.DurRefresh >= mmr.DurKeep-MaxRefreshDelta &&
			mmr.DurRefresh <= mmr.DurKeep-MinRefreshDelta) {
			log.Debugf("checkMakeMapReq: invalid [keep, refesh] pair: [%d,%d]", mmr.DurKeep, mmr.DurRefresh)
			return NatEnoParameter
		}
	} else {
		mmr.DurRefresh = mmr.DurKeep - MinRefreshDelta
	}
	return NatEnoNone
}

func (natMgr *NatManager) startRefreshTimer(inst *NatMapInstance) NatEno {
	// notice: we start an "Absolute" timer after map made than a "Cycle" timer
	if inst.tidRefresh != sch.SchInvalidTid {
		if eno := natMgr.sdl.SchKillTimer(natMgr.ptnMe, inst.tidRefresh); eno != sch.SchEnoNone {
			log.Debugf("startRefreshTimer: SchKillTimer failed, tid: %d, eno: %d", inst.tidRefresh, eno)
			return NatEnoScheduler
		}
	}
	td := sch.TimerDescription{
		Name:  "natInstRefreshingTimer",
		Utid:  sch.NatMgrRefreshTimerId,
		Tmt:   sch.SchTmTypeAbsolute,
		Dur:   inst.durRefresh,
		Extra: inst,
	}
	eno, tid := natMgr.sdl.SchSetTimer(natMgr.ptnMe, &td)
	if eno != sch.SchEnoNone {
		log.Debugf("startRefreshTimer: SchSetTimer failed, eno: %d", eno)
		inst.tidRefresh = sch.SchInvalidTid
		return NatEnoScheduler
	}
	inst.tidRefresh = tid
	return NatEnoNone
}

func NatIsResultOk(eno int) bool {
	return eno == NatEnoNone.Errno()
}

func NatIsStatusOk(status int) bool {
	return status == NatEnoNone.Errno()
}

func (natMgr *NatManager) debugTimer() sch.SchErrno {
	// for debug only: this timer event would be fired only once since we set a
	// "ABS" timer when nat manager is powered on.
	ip := config.P2pGetLocalIpAddr()
	for _, inst := range natMgr.instTab {
		ind := sch.MsgNatMgrPubAddrUpdateInd{
			Status:   NatEnoNone.Errno(),
			Proto:    inst.id.proto,
			FromPort: inst.id.fromPort,
			PubIp:    ip,
			PubPort:  inst.pubPort,
		}
		log.Debugf("debugTimer: send to %s with ind: %v", natMgr.sdl.SchGetTaskName(inst.owner), ind)
		msg := sch.SchMessage{}
		natMgr.sdl.SchMakeMessage(&msg, natMgr.ptnMe, inst.owner, sch.EvNatMgrPubAddrUpdateInd, &ind)
		natMgr.sdl.SchSendMessage(&msg)
	}
	return sch.SchEnoNone
}
