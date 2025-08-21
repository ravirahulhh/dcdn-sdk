include ../config.mk

INCFLAGS = -I.. -I../deps/include 
CPPFLAGS = -DHAVE_GPERFTOOLS -ggdb3 -fno-omit-frame-pointer

LIBFLAGS = \
		   -L../deps/lib -lcurl -ldatachannel -lusrsctp -ljuice -lsqlite3 -lz \

PLT ?= $(shell uname)
ifeq ($(PLT), Darwin)
	OpenSslDir=$(shell brew --prefix openssl@3)
	INCFLAGS += -I$(OpenSslDir)/include
	INCFLAGS += -I/opt/homebrew/opt/gperftools/include
    LIBFLAGS += -L$(OpenSslDir)/lib -lssl -lcrypto
	LIBFLAGS += -framework CoreFoundation -framework SystemConfiguration
	LIBFLAGS += -L/opt/homebrew/opt/gperftools/lib 
else
    LIBFLAGS += -lssl -lcrypto
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
	$(CXX) -c $(INCFLAGS) $(CPPFLAGS) $^

%.o : %.c
	$(CC) -c $(INCFLAGS) $(CPPFLAGS) $^

clean:
	rm -rf $(libTargets) $(testTargets) *.o

# Profiling targets
profile: MainTest
	@echo "Running profiling..."
	@rm -f cpu.prof
	@./MainTest
	@if [ -f cpu.prof ]; then \
		echo "Profile generated: cpu.prof"; \
		ls -la cpu.prof; \
	else \
		echo "No profile generated"; \
	fi

# Try to analyze profile with different tools
analyze-profile: cpu.prof
	@echo "Trying to analyze profile..."
	@echo "=== Using pprof text output ==="
	@pprof --text ./MainTest cpu.prof 2>/dev/null || echo "pprof failed"
	@echo "=== Using Google pprof if available ==="
	@$(HOME)/go/bin/pprof --text ./MainTest cpu.prof 2>/dev/null || echo "Google pprof not available"
	@echo "=== Using gperftools pprof if available ==="
	@/opt/homebrew/bin/pprof --text ./MainTest cpu.prof 2>/dev/null || echo "gperftools pprof not available"

# Generate flame graph using alternative method  
flamegraph: cpu.prof
	@echo "Generating flame graph..."
	@if command -v stackcollapse-pprof.pl >/dev/null 2>&1 && command -v flamegraph.pl >/dev/null 2>&1; then \
		pprof --text ./MainTest cpu.prof | stackcollapse-pprof.pl | flamegraph.pl > flamegraph.svg; \
		echo "Flame graph generated: flamegraph.svg"; \
	else \
		echo "FlameGraph tools not available. Install from: https://github.com/brendangregg/FlameGraph"; \
	fi
