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

package node

import (
	"testing"

	"github.com/yeeco/gyee/config"
	"github.com/yeeco/gyee/utils/logging"
	"path/filepath"
)

func TestNode(t *testing.T) {
	config1 := config.GetDefaultConfig()
	config1.DataDir = filepath.Join(config1.DataDir, "node1")
	node1, err := NewNode(config1)
	if err != nil {
		logging.Logger.Fatal(err)
	}

	config2 := config.GetDefaultConfig()
	config2.DataDir = filepath.Join(config2.DataDir, "node2")
	node2, err := NewNode(config2)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	node1.Start()
	node2.Start()

	node1.WaitForShutdown()
}
