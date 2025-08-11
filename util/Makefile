include ../config.mk

INCFLAGS = -I.. -I../deps/include

LIBFLAGS = \
		   -L../deps/lib -lcurl -lz \

PLT ?= $(shell uname)
ifeq ($(PLT), Darwin)
	OpenSslDir=$(shell brew --prefix openssl@3)
	INCFLAGS += -I$(OpenSslDir)/include
    LIBFLAGS += -L$(OpenSslDir)/lib -lssl -lcrypto
	LIBFLAGS += -framework CoreFoundation -framework SystemConfiguration
else
    LIBFLAGS += -lssl -lcrypto
endif	

testTargets = \
			  HttpClientTest \
			  HttpDownloaderTest \
			  Secp256k1PubKeyTest \

test: $(testTargets)

HttpClientTest: HttpClientTest.o
	$(CXX) -o $@ $^ $(LIBFLAGS)

HttpDownloaderTest: HttpDownloaderTest.o
	$(CXX) -o $@ $^ $(LIBFLAGS)

Secp256k1PubKeyTest: Secp256k1PubKeyTest.o
	$(CXX) -o $@ $^ $(LIBFLAGS)

%.o : %.cpp
	$(CXX) -c $(INCFLAGS) $^

clean:
	rm -rf $(testTargets) *.o
