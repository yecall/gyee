CC = /usr/bin/gcc
CFLAGS = -O3 -march=native -fomit-frame-pointer #-Wall -Wextra 
LDFLAGS=-lm -L/usr/lib/ -lssl -lcrypto 

SOURCE = sign.c cpucycles.c rng.c consts.c sample.c oracle.c poly.c 
SOURCE += sha3/fips202.c

HEADER = api.h rng.h params.h sample.h oracle.h sha3/fips202.h


all: test_qtesla PQCgenKAT_sign 


test_qtesla: $(SOURCE) $(HEADER) test_qtesla.c rng.c
	$(CC) $(CFLAGS) $(SOURCE) test_qtesla.c  $(LDFLAGS) -o $@

PQCgenKAT_sign: $(SOURCE) $(HEADER) PQCgenKAT_sign.c rng.c 
	$(CC) $(CFLAGS) $(SOURCE) PQCgenKAT_sign.c $(LDFLAGS) -o $@

PQCtestKAT_sign: $(SOURCE) $(HEADER) PQCtestKAT_sign.c rng.c 
	$(CC) $(CFLAGS) $(SOURCE) PQCtestKAT_sign.c $(LDFLAGS) -o $@

testKATs: PQCtestKAT_sign ../../KAT/PQCsignKAT_qTesla-256.rsp
	./PQCtestKAT_sign


.PHONY: clean

clean:
	-rm test_qtesla
	-rm PQCgenKAT_sign
	-rm PQCsignKAT*
	-rm PQCtestKAT_sign
