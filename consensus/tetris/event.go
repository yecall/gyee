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

type EventBody struct {
	H uint64 //Block Height
	M uint  //MemberID
	N uint64 //Sequence Number, M和N唯一决定一个Event
	T int64 //Timestamps, 用unixNano时间, 一秒等于10的9次方nano，只能表示到1678-2262年
    Tx [][]byte //Transactions List
    E [][]byte //Parents Events, 0 for self parent
    F [][]byte //Fork Events, as Invalid
}

type Event struct {
    Body EventBody
    Hash [][]byte
    Signature string

}

func NewEvent() Event {
	body := EventBody{

	}

	return Event{
		Body: body,
	}
}
