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

package random

import (
	"crypto/rand"
	"io"

	"github.com/yeeco/gyee/utils/logging"
)

func GetEntropyCSPRNG(n int) []byte {
	buff := make([]byte, n)
	_, err := io.ReadFull(rand.Reader, buff)
	if err != nil {
		logging.Logger.Panic("reading from crypto/rand failed: " + err.Error())
	}
	return buff
}


/*
！！！好的随机数是一切安全算法的根本！！！

Reference: https://leanpub.com/gocrypto/read

Randomness

Cryptographic systems rely on sources of sufficiently random data. We want the data from these sources to be indistinguishable from ideally random data (a uniform distribution over the range of possible values). There has been historically a lot of confusion between the options available on Unix platforms, but the right answer (e.g. [6]) is to use /dev/urandom. Fortunately, crypto/rand.Reader in the Go standard library uses this on Unix systems.

Ensuring the platform has sufficient randomness is another problem, which mainly comes down to ensuring that the kernel’s PRNG is properly seeded before being used for cryptographic purposes. This is a problem particularly with virtual machines, which may be duplicated elsewhere or start from a known or common seed. In this case, it might be useful to include additional sources of entropy in the kernel’s PRNG, such as a hardware RNG that writes to the kernel’s PRNG. The host machine may also have access to the PRNG via disk or memory allowing its observation by the host, which must be considered as well.
*/