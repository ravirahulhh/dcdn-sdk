include ../config.mk

INCFLAGS = -I.. -I../deps/include 

LIBFLAGS = \
		   -L../deps/lib -lcurl -ldatachannel -lusrsctp -ljuice -lsqlite3 -lz \

PLT ?= $(shell uname)
ifeq ($(PLT), Darwin)
	OpenSslDir=$(shell brew --prefix openssl@3)
	INCFLAGS += -I$(OpenSslDir)/include
    LIBFLAGS += -L$(OpenSslDir)/lib -lssl -lcrypto
	LIBFLAGS += -framework CoreFoundation -framework SystemConfiguration
	LIBFLAGS += -L/opt/homebrew/opt/gperftools/lib -lprofiler -ltcmalloc
else
    LIBFLAGS += -lssl -lcrypto -lprofiler -ltcmalloc
endif	

libTargets = \
			 libdcdn.a

libdcdnObjs = \
			   Cert.o \
			   Config.o \
			   EventLoop.o \
			   WebSocketManager.o \
			   WebRtcManager.o \
			   FileManager.o \
			   UploadManager.o \
			   DownloadManager.o \
			   DownloadTask.o \
			   MainManager.o \
			   dcdn.o \
			   DownloadManager.o \
			   DeployManager.o \
			   P2PDownloader.o \
			   P2PSingleTask.o \

testTargets = \
			  MainTest \
			  dcdnTest \

MainTestObjs = \
			   MainManagerTest.o \

dcdnTestObjs = \
			   dcdnTest.o

all: $(libTargets)
	@echo Done

libdcdn.a: $(libdcdnObjs)
	ar rcs $@ $^

test: $(testTargets)
	@echo Done

MainTest: $(MainTestObjs) libdcdn.a
	$(CXX) -o $@ $^ $(LIBFLAGS)

dcdnTest: $(dcdnTestObjs) libdcdn.a
	$(CXX) -o $@ $^ $(LIBFLAGS)

%.o : %.cpp
	$(CXX) -c $(INCFLAGS) $^

%.o : %.c
	$(CC) -c $(INCFLAGS) $^

clean:
	rm -rf $(libTargets) $(testTargets) *.o
