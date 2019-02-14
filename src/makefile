jaguarprog: lib createKeyPair
all: clean lib 
test: testjdbserv 

BASICFLAGS=-I. -IWeb/GeographicLib-1.49/include \
		  -IWeb/Tencent/rapidjson/include -IWeb/TomCrypt/include \
		  -IWeb/Snappy -IWeb/UTF -IWeb/Boost/boost_1_68_0/include \
		  -IWeb/CGAL/CGAL-4.13/include \
	      -D_REENTRANT -std=c++11 -Wno-register \
		  -DCHECK_LATENCY1 -DJAG_DEBUGCHECK1 -DDEVDEBUG1 -DTFM_DESC $(USER_DEFINE)
         

CCFLAGS_STATIC=$(BASICFLAGS)  -fpermissive -static-libgcc -static-libstdc++ -static  
CCFLAGS_DYNAMIC=$(BASICFLAGS) -fPIC
CCFLAGS=$(CCFLAGS_STATIC)
CCFLAGS=$(CCFLAGS_DYNAMIC)

STDLIBS_STATIC =  Web/GeographicLib-1.49/lib/libGeographic.a /usr/lib64/libpthread.a
STDLIBS_DYNAMIC = -LWeb/GeographicLib-1.49/lib -LWeb/CGAL/CGAL-4.13/lib64 -LWeb/Boost/boost_1_68_0/lib \
                  -lGeographic -lCGAL -lboost_thread -lboost_system 


STDLIBS=$(STDLIBS_STATIC)
STDLIBS=$(STDLIBS_DYNAMIC)

JAVA_HOME:=$(shell jrunscript -e "java.lang.System.out.println(new java.io.File(java.lang.System.getProperty(\"java.home\")));")
JAVAINC=$(JAVA_HOME)/../include
DRIZZLE=Web/DrizzleJDBC
DRIZZLEINC=$(DRIZZLE)/src/main/java

GCC=gcc
GPP=g++
    
CPP=$(GPP)  -pg -DJAG_DEBUGCHECK
CPP=$(GPP)  -g3 -DJAG_DEBUGCHECK
CPP=$(GPP)  -O3 -g -DJAG_DEBUGCHECK
CPP=$(GPP)  -O3 -DJAG_DEBUGCHECK
CPP=$(GPP) 
CPP=$(GPP)  -O3
CPP=$(GPP)  -ggdb

#### makefile.obj has $(OBJS)
include makefile.obj


JagParseExpr.o: JagExprStack.h

.cc.o: 
	date
	$(CPP) $(CCFLAGS) -c -o $@ $<  
	date

clean:
	/bin/rm -f *.o *.class *.gz core.* jaguar jql ~/jaguar/lib/jagserv.o ~/jaguar/lib/jql.o

Jaguar.o: Jaguar.cc
	$(CPP) $(CCFLAGS) -I. -I$(JAVAINC) -I$(JAVAINC)/linux -I$(DRIZZLEINC)  -c Jaguar.cc

javahdr:
	/bin/cp -f Jaguar.java $(DRIZZLE)/src/main/java/com/jaguar/jdbc/internal/jaguar/
	cd $(DRIZZLE)/src/main/java; javah -jni com.jaguar.jdbc.internal.jaguar.Jaguar

javalib: Jaguar.o  $(OBJS) 
	$(CPP) $(CCFLAGS) -shared $(STDLIBS_DYNAMIC) -Wl,-soname,libJaguarClient.so -o libJaguarClient.so \
	Jaguar.o $(OBJS) Web/TomCrypt/lib/*.o Web/Snappy/*.o

glibcversion: glibcversion.o
	$(CPP) -o glibcversion glibcversion.o

lib:  $(OBJS) jagserv.o jql.o javahdr javalib
	$(CPP) -o jaguar.bin jagserv.o -L. -lJaguarClient $(STDLIBS)
	$(CPP) -o jql.bin jql.o -L. -lJaguarClient $(STDLIBS)


createKeyPair: createKeyPair.o
	$(CPP) -o createKeyPair.bin  createKeyPair.o libJaguarClient.so $(STDLIBS)

testjdbserv: testjdbserv.o $(OBJS) 
	$(GPP) -o testjdbserv testjdbserv.o libJaguarClient.so $(STDLIBS) -lpthread

