CC = gcc
CFLAGS = -W -Wall -std=c11 -pedantic -pthread -Wno-unused-parameter -Wno-unused-variable -g -g3 -fsanitize=address
.PHONY : clean

all : main generation print

main: main.c Makefile
	$(CC) $(CFLAGS) main.c -o main

generation: generation.c Makefile
	$(CC) $(CFLAGS) generation.c -o generation

print: print.c Makefile
	$(CC) $(CFLAGS) print.c -o print

clean:
	rm -f main generation print