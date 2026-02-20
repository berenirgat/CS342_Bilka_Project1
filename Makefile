CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c11

all: findlwp findlwt

findlwp: findlwp.c common.h
	$(CC) $(CFLAGS) -o findlwp findlwp.c

findlwt: findlwt.c common.h
	$(CC) $(CFLAGS) -pthread -o findlwt findlwt.c

clean:
	rm -f findlwp findlwt out.txt