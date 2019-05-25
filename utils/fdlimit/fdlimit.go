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

package fdlimit

import (
	ethfdlimit "github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/yeeco/gyee/log"
)

func FixFdLimit() error {
	curr, err := Current()
	if err != nil {
		return err
	}
	max, err := Maximum()
	if err != nil {
		return err
	}
	result, err := Raise(uint64(max))
	if err != nil {
		return err
	}
	log.Infof("fdLimit raise %d -> %d, max %d", curr, result, max)
	return nil
}

func Current() (int, error) {
	return ethfdlimit.Current()
}

func Raise(max uint64) (uint64, error) {
	return ethfdlimit.Raise(max)
}

func Maximum() (int, error) {
	return ethfdlimit.Maximum()
}
