OPT ?= -g -O2 -fno-omit-frame-pointer -lprofiler -ltcmalloc
CC = gcc
CXX = g++ -std=c++17 $(OPT)

