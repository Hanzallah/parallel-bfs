#!/bin/bash
gcc -o serial_bfs serial_bfs.c util.c queue.c
mpicc parallel_bfs.c -o parallel_bfs util.c queue.c