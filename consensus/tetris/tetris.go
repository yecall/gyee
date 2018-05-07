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

package tetris
/*
   共识机制
   输入：tx的hash，Events
        读取，当前block checkpoint， core state，
        当前validate列表，
        validate列表更新操作
   输出：请求发送Events，达成共识的Event集合
        本届member评价生成tx
        下届validate列表
        统计metrics
   pow竞争：拿到全量state和checkpoint之后的block，做hash碰撞，成功的发送一个tx，谁的tx先到先得

0. 一个节点，申请成为candidate，同步区块stable state之后的所有内容，参与pow竞争，然后可能被选中为validator
   创世块中，有指定缺省的validator
   申请candidate的操作，可以在console中，也可以再wallet中发起操作
1. 创建新的Tetris
   时机：core模块启动，同步区块头完成，如果发现自己属于validator，则要同步区块内容，完成后启动Tetris
   参数：当前区块高度，validator清单，提交共识结果通道，
   缺省值：N=0, LN=0 等到接受到E刷新

2. 接收Tx，只需要交易的hash，
   本地从网络收交易有core负责
      core需要：验证交易格式，防止ddos攻击，内存池管理
   共识达成后的ordered tx，由core负责验证、执行、打包
   共识后的tx，有可能只有hash，内容未到，有core负责拉取。可在共识过程中由Tetris实时反馈给core去拉取刷新交易内容

3. 接收Event(h, m, n, t, {tx...}, {E...})
   Event的校验：

   如果toMe：Event存当前收到列表
   如果Event的parent还没有收全，放入unsettled列表，请求同步
   检查unsettleed列表，如果parent已收全，加入tetris

   拉取Event详情


4. 发送Events
   h = 当前区块高度
   m = member id
   n = sequence number, n>=parents.n && n > selfParent.n
   发送规则：num>max && time>=min || time > max && num >= min


5. 计算共识
   已确定的event，及其self-parent, 包含的所有tx，根据规则去重排序。规则还需要再设计
   tx被f+1个famous member收到过，才能加入共识，历史上收到的都算

   计算member表现
   计算reward

6. 踢出老memeber，选择新member加入

7. Events和Tx何时可以销毁

8. 网络层面：group dht
   group包含成员validators，向group发送dht数据。搞一个group id？

*/

import (
	"container/list"
	"github.com/yeeco/gyee/utils"
)

type Tetris struct {
	H uint64 //Current Block Height
	M uint  //My MemberID
	N uint64 //Current Sequence Number for processing
	LN uint64  //Latest Sequence Number of Event

	MembersID  map[string] int  //[address] -> id
	MembersAddr map[int] string  //[id] -> address
    MemberEvents []list.List
    EventCache  *utils.LRU

    UnsettledEvents map[string] string  //Events with their parents has not arrived

    superMajority  int
    maxTxPerEvent  int
    minTxPerEvent  int
    maxPeriodForEvent  int
    minPeriodForEvent  int
}

func NewTetris() (*Tetris, error) {

	tetris := Tetris{

	}

	return &tetris, nil
}

//Receive transaction send to me
func (t *Tetris) ReceiveTx(tx []byte){

}

//Receive Event, if the parents not exists, send request
func (t *Tetris) ReceiveEvent(event Event){

}

//Process peer request for the parents for events
func(t *Tetris) ReceiveSyncRequest(){

}

