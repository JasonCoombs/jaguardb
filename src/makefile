all: lib prog createKeyPair glibcversion
progonly: prog
test: testjdbserv 

BASICFLAGS=-I. -IWeb/GeographicLib-1.49/include -IWeb/libcds/include \
		  -IWeb/Tencent/rapidjson/include -IWeb/TomCrypt/include \
		  -IWeb/Snappy -IWeb/UTF -IWeb/Boost/boost_1_68_0/include \
		  -IWeb/Google/Btree/include \
		  -IWeb/CGAL/CGAL-4.13/include -IWeb/ConcaveHull \
	      -D_REENTRANT -std=c++11 -Wno-register -Wunused-variable \
		  -DCHECK_LATENCY1 -DDEVDEBUG1 -DTFM_DESC -DDEBUG_STACK1 $(USER_DEFINE)

CCFLAGS_STATIC=$(BASICFLAGS)  -static-libgcc -static-libstdc++ -static  
CCFLAGS=$(CCFLAGS_STATIC)

STDLIBS_STATIC =  Web/GeographicLib-1.49/lib/libGeographic.a  \
                  Web/CGAL/CGAL-4.13/lib64/libCGAL.a Web/Boost/boost_1_68_0/lib/libboost_system.a \
				  Web/Boost/boost_1_68_0/lib/libboost_thread.a Web/Boost/boost_1_68_0/lib/libboost_system.a \
				  /usr/lib64/libpthread.a 

STDLIBS=$(STDLIBS_STATIC)

GCC=gcc
GPP=g++
    
CPP=$(GPP)  -pg -DJAG_DEBUGCHECK
CPP=$(GPP)  -g3 -DJAG_DEBUGCHECK
CPP=$(GPP)  -O3 -g -DJAG_DEBUGCHECK
CPP=$(GPP)  -O3 -DJAG_DEBUGCHECK
CPP=$(GPP)  -ggdb
CPP=$(GPP)  -pg
CPP=$(GPP)  -g
CPP=$(GPP) 
CPP=$(GPP)  -g
CPP=$(GPP)  -O3 -g
CPP=$(GPP)  -O3

#### makefile.obj has $(OBJS)
include makefile.obj

EXTOBJS=Web/TomCrypt/static/lib/*.o Web/Snappy/static/*.o 
ALLCLIENTOBJS=$(CLIENTOBJS) JaguarCPPClient_static.obj
ALLEXTOBJS=$(EXTOBJS) JaguarCPPClient_static.obj

JagParseExpr.o: JagExprStack.h

.cc.o: 
	date
	$(CPP) $(CCFLAGS) -c -o $@ $<  
	date

JagParseExprClient.o: JagParseExpr.cc
JagParseExprServer.o: JagParseExpr.cc

clean:
	/bin/rm -f *.class core.* jaguar jql.bin ~/jaguar/lib/jagserv.o ~/jaguar/lib/jql.o

glibcversion: glibcversion.o
	$(CPP) $(CCFLAGS) -o glibcversion glibcversion.o


lib: $(ALLCLIENTOBJS) $(EXTOBJS)
	ar cr libJaguarClient.a $(ALLCLIENTOBJS) $(EXTOBJS) 


jql:  $(ALLCLIENTOBJS) $(SERVEROBJS) jagserv.o jql.o
	$(CPP) $(CCFLAGS) -o jql.bin jql.o $(ALLCLIENTOBJS) $(EXTOBJS) /usr/lib64/libpthread.a
	/bin/cp -f jql.bin ~/jbin

prog:  $(ALLCLIENTOBJS) $(SERVEROBJS) jagserv.o jql.o
	$(CPP) $(CCFLAGS) -o jql.bin jql.o $(ALLCLIENTOBJS) $(EXTOBJS) /usr/lib64/libpthread.a
	$(CPP) $(CCFLAGS) -o jaguar.bin jagserv.o $(SERVEROBJS) $(ALLEXTOBJS) $(STDLIBS) 

createKeyPair: createKeyPair.o
	$(CPP) $(CCFLAGS) -o createKeyPair.bin  createKeyPair.o $(SERVEROBJS) $(ALLEXTOBJS) $(STDLIBS)

testjdbserv: testjdbserv.o $(OBJS) 
	$(CPP) $(CCFLAGS) -o testjdbserv testjdbserv.o $(SERVEROBJS) $(ALLEXTOBJS) /usr/local/lib64/libcds-s.a $(STDLIBS) 

