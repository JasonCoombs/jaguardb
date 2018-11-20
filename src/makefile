jaguarprog: lib createKeyPair
all: clean lib 
test: testjdbserv 

BASICFLAGS=-I. -IWeb/GeographicLib-1.49/include \
		  -IWeb/Tencent/rapidjson/include -IWeb/TomCrypt/include \
		  -IWeb/Snappy -IWeb/UTF  -D_REENTRANT -std=c++0x \
		 -DCHECK_LATENCY1 -DJAG_DEBUGCHECK1 -DDEVDEBUG1 -DTFM_DESC

CCFLAGS_STATIC=$(BASICFLAGS)  -fpermissive -static-libgcc -static-libstdc++ -static  
CCFLAGS_DYNAMIC=$(BASICFLAGS) -fPIC
CCFLAGS=$(CCFLAGS_STATIC)
CCFLAGS=$(CCFLAGS_DYNAMIC)

STDLIBS_STATIC =   Web/GeographicLib-1.49/lib/libGeographic.a /usr/lib64/libpthread.a
STDLIBS_DYNAMIC =  -LWeb/GeographicLib-1.49/lib -lGeographic  -lpthread
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
CPP=$(GPP)  -ggdb
CPP=$(GPP)  -O3

#### makefile.obj has $(OBJS)
include makefile.obj


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
	$(CPP) $(CCFLAGS) -shared -LWeb/GeographicLib-1.49/lib -lGeographic -Wl,-soname,libJaguarClient.so -o libJaguarClient.so \
	Jaguar.o $(OBJS) Web/TomCrypt/lib/*.o Web/Snappy/*.o

glibcversion: glibcversion.o
	$(CPP) -o glibcversion glibcversion.o

lib:  $(OBJS) jagserv.o jql.o javahdr javalib
	$(CPP) -o jaguar.bin jagserv.o -L. -LWeb/GeographicLib-1.49/lib  -lJaguarClient -lGeographic -lpthread
	$(CPP) -o jql.bin jql.o -L. -LWeb/GeographicLib-1.49/lib -lJaguarClient -lGeographic  -lpthread


createKeyPair: createKeyPair.o
	$(CPP) -o createKeyPair.bin  createKeyPair.o libJaguarClient.so $(STDLIBS_DYNAMIC)

testjdbserv: testjdbserv.o $(OBJS) 
	$(GPP) $(CCFLAGS_DYNAMIC) -o testjdbserv testjdbserv.o libJaguarClient.so $(STDLIBS_DYNAMIC)

