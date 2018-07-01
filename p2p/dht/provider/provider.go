//
// This file implements the DHT Provider module: a static task for provider manager
// is needed.
//
// liyy, 20180408
//

package provider

import (
	sch 	"github.com/yeeco/p2p/scheduler"
	yclog	"github.com/yeeco/p2p/logger"
)

//
// Provider manager
//
const DhtpMgrName = "DhtpMgr"

type dhtProviderManager struct {
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

var dhtpMgr = dhtProviderManager{
	name:	DhtpMgrName,
	tep:	DhtpMgrProc,
}

//
// Table manager entry
//
func DhtpMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	yclog.LogCallerFileLine("DhtpMgrProc: scheduled, msg: %d", msg.Id)
	return sch.SchEnoNone
}

