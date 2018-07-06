//
// This file implements the DHT Route module: a static task for route manager
// is needed.
//
// liyy, 20180408
//

package route

import (
	sch 	"github.com/yeeco/gyee/p2p/scheduler"
)

//
// Route manager
//
const DhtrMgrName = "DhtpMgr"

type dhtRouteManager struct {
	name	string				// name
	tep		sch.SchUserTaskEp	// entry
}

var dhtpMgr = dhtRouteManager{
	name:	DhtrMgrName,
	tep:	DhtrMgrProc,
}

//
// Table manager entry
//
func DhtrMgrProc(ptn interface{}, msg *sch.SchMessage) sch.SchErrno {
	return sch.SchEnoNone
}

