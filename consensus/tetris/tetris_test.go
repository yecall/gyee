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

/*
 测试项目：
 1、发送tx的延时，tx的速率波动
 2、发送和event的延迟，发送范围，丢失
 3、request的延迟
 4、最大支持的member数
 5、单一member算共识，其他member仅转发event情况下的最大支持member数，这个测试单机性能。
 6、member crash，重启，新加入，退出等情况测试，快速重启和慢速重启测试
 7、byzantine行为测试，member hold tx&event，fork，协议内ddos，长程延迟，伪造h，m，n
 8、三分之一以上crash一段时间后再恢复能否恢复算法，这种情况应该需要丢弃部分时间段交易才可行，否则积累起来无法处理。
 9、高负载测试，持续超负载，然后放缓
 10、中等时间断网测试
 11、网络分区测试，分区后再并上，数据同步的压力可能很大，有丢弃策略么？
 12、tetris多节点的时候，启动过程。节点启动是不同步的，少量节点启动时也是无共识的，时间间隔可能也会比较长。

*/

package tetris

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/yeeco/gyee/utils"
	"github.com/yeeco/gyee/utils/logging"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
)

const TNUM = 10

var tetrises [TNUM]*Tetris
var wg sync.WaitGroup
var eventCache *utils.LRU
var co []map[uint]string

func TestTetris(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	eventCache = utils.NewLRU(1000000, nil)

	members := map[string]uint{}
	memberAddr := map[uint]string{}
	for i := uint(0); i < TNUM; i++ {
		members[strconv.Itoa(int(i))] = i
		memberAddr[i] = strconv.Itoa(int(i))
	}

	for i := uint(0); i < TNUM; i++ {
		var err error
		tetrises[i], err = NewTetris(nil, members, 0, memberAddr[i])
		if err != nil {
			logging.Logger.Fatal("create tetris err ", err)
		}
		tetrises[i].Start()
	}
	co = make([]map[uint]string, 1)

	//随机生成tx，向各个tetris发送
	wg.Add(1)
	go sendRandomTx()
	go loop()
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		logging.Logger.Info("Got interrupt, shutting down...")
		for t := 0; t < TNUM; t++ {
			fmt.Println("-------", t, tetrises[t].n, tetrises[t].h)
			//tetrises[t].UpdateGraph()
			for i := tetrises[t].n; i > 0; i-- {
				for j := uint(0); j < TNUM; j++ {
					me := tetrises[t].memberEvents[j][i]
					if me != nil {
						fmt.Printf("m%d_n%d_r%d(", me.Body.M, me.Body.N, me.round)
						ll := 5
						for _, e := range me.Body.E {
							if e == "" {
								continue
							}
							ll--
							ev, _ := eventCache.Get(e)
							fmt.Printf("%d %2d, ", ev.(Event).Body.M, ev.(Event).Body.N)
						}
						fmt.Printf(")        ")
						for ; ll > 0; ll-- {
							fmt.Printf("      ")
						}
					}
				}
				fmt.Println("")
			}
			fmt.Println("")
		}
		logging.Logger.Fatal("")
	}()
	wg.Wait()

	//time.Sleep(1 * time.Second)
	//fmt.Println(tetrises[0].MemberEvents)
	for t := 0; t < TNUM; t++ {
		fmt.Println("-------", t, tetrises[t].n, tetrises[t].h)
		//tetrises[t].UpdateGraph()
		for i := tetrises[t].n; i > tetrises[t].h; i-- {
			for j := uint(0); j < TNUM; j++ {
				me := tetrises[t].memberEvents[j][i]
				if me != nil {
					fmt.Printf("m%d_n%d_r%d(", me.Body.M, me.Body.N, me.round)
					ll := 5
					for _, e := range me.Body.E {
						if e == "" {
							continue
						}
						ll--
						ev, _ := eventCache.Get(e)
						fmt.Printf("%d %2d, ", ev.(Event).Body.M, ev.(Event).Body.N)
					}
					fmt.Printf(")        ")
					for ; ll > 0; ll-- {
						fmt.Printf("      ")
					}
				}
			}
			fmt.Println("")
		}
		fmt.Println("")
	}

	for t := 0; t < TNUM; t++ {
		fmt.Println("request count", tetrises[t].requestCount, "parent raw", tetrises[t].parentCountRaw, "parent", tetrises[t].parentCount)

	}

	for i := 0; i < len(co); i++ {
		c := 0
		fmt.Print(i, ": ")
		for j := uint(0); j < TNUM; j++ {
			if len(co[i][j]) > 0 {
				fmt.Print(co[i][j], " ")
			} else {
				fmt.Print("     ")
				c++
			}
		}
		fmt.Println()
		f := co[i][0]
		for j := uint(1); j < TNUM; j++ {
			if f != co[i][j] && co[i][j] != "" && f != "" {
				fmt.Println("error error error error!!!!!!!!!!!!")
			}
		}
		if c == TNUM && i > 1 {
			break
		}
	}
	fmt.Println()
	fmt.Println("node:", TNUM, "      total tx:", totalConsensusTx)
}

func sendRandomTx() {
	num := 100000
	for i := 0; i < num; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint32(buf, uint32(i))
		hash := sha256.Sum256(buf)
		hex := fmt.Sprintf("0x%X", hash)
		go func(hex string) {
			wg.Add(1)
			for i := 0; i < TNUM; i++ {
				t := rand.Intn(TNUM - 1) //最后一个不发tx
				tetrises[t].TxsCh <- hex
				time.Sleep(time.Duration(rand.Intn(1)) * time.Millisecond)
			}
			wg.Done()
		}(hex)
		time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
	}
	wg.Done()
}

func loop() {
	logging.Logger.Info("Test loop...")

	cases := make([]reflect.SelectCase, 3*TNUM)
	for i := 0; i < TNUM; i++ {
		cases[3*i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tetrises[i].SendEventCh)}
		cases[3*i+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tetrises[i].RequestEventCh)}
		cases[3*i+2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tetrises[i].OutputCh)}
	}

	remaining := len(cases)
	for remaining > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}
		if chosen%3 == 0 {
			sendEvent(uint(chosen/3), value.Interface().(Event))
		} else if chosen%3 == 1 {
			requestEvent(uint(chosen/3), value.Interface().(string))
		} else {
			consensusOutput(uint(chosen/3), value.Interface().(ConsensusOutput))
		}
	}
}

func sendEvent(from uint, event Event) {
	//需要模拟丢失和延迟
	event.know = nil
	event.parents = nil
	eventCache.Add(event.Hex(), event)

	for i := 0; i < TNUM-1; i++ {
		t := rand.Intn(TNUM)
		if uint(t) != from {
			go func(from uint, to uint, event Event) {
				//wg.Add(1)
				//defer wg.Done()
				if from == 9 && event.Body.N > 40 {
					return
				}
				if from == 8 && event.Body.N > 80 {
					return
				}
				if from == 3 && event.Body.N > 5 && event.Body.N < 15 {
					return
					//time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				}

				if to == 3 && event.Body.N > 1 && event.Body.N < 10 {
					return
				}
				//if to > TNUM/3*2 + 1 {
				//	return
				//}
				if from == 1 && event.Body.N > 20 && event.Body.N < 30 ||
					from == 2 && event.Body.N > 30 && event.Body.N < 40 {
					return
				}
				time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
				if from == 0 {
					time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
				}
				tetrises[to].EventCh <- event

			}(from, uint(t), event)
		}
	}
}

func requestEvent(from uint, eventHex string) {
	//模拟丢失和延迟
	//fmt.Println("request parent", from)
	event, ok := eventCache.Get(eventHex)
	if ok {
		go func(to uint, event Event) {
			//wg.Add(1)
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			tetrises[to].ParentEventCh <- event
			//wg.Done()
		}(from, event.(Event))
	} else {
		fmt.Println("*********************** request event error ********************", from, eventHex)
	}
}

var totalConsensusTx int

func consensusOutput(from uint, output ConsensusOutput) {
	outputHash := sha256.Sum256([]byte(strings.Join(output.Tx, " ")))
	fmt.Printf("***** Consensus output: %d  %d  %d \n", from, output.h, len(output.Tx))
	//fmt.Println(from, string(output))
	if int(output.h) >= len(co) {
		co = append(co, make(map[uint]string))
	}
	m := co[output.h]
	if m == nil {
		co[output.h] = make(map[uint]string)
	}
	co[output.h][from] = fmt.Sprintf("(%v)%X", output.output, outputHash[0:2]) //output.output
	if from == 0 {
		totalConsensusTx += len(output.Tx)
	}
}
