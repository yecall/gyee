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

package core

/*
   blockchain的主要内容
   创世块
   数据同步
   交易验证
   block验证，blockchain维护
   db管理
   state？
   VM

1. 创建的时候，如果本地没有数据，先创建创世块
2. 启动先进入同步状态，同步区块高度
3. 进入到正常状态后，收取区块数据及验证
4. 如果开启了挖矿：
   如果进入到candidate状态，需要同步上一个state及之后所有的区块内容。
   如果进入到validator状态，开启tetris
5.

*/
import (
	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/consensus/tetris"
)

type Core struct {
	node  INode
	config *config.Config
	tetris *tetris.Tetris
	tetrisOutputCh chan tetris.ConsensusOutput

}

func NewCore(node INode, conf *config.Config) (*Core, error) {

    core := Core{
    	node: node,
    	config: conf,
	}

	return &core, nil
}

func (c *Core) Start() error {
    //

    //如果开启挖矿
    if true {
		tetris, err := tetris.NewTetris(c)
		if err != nil {
			return err
		}
		c.tetris = tetris
		c.tetrisOutputCh = tetris.OutputCh

	}
	return nil
}

func (c *Core) Stop() error {

	return nil
}
