/*
 * Copyright (C) 2018 DataJaguar, Inc.
 *
 * This file is part of JaguarDB.
 *
 * JaguarDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JaguarDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JaguarDB (LICENSE.txt). If not, see <http://www.gnu.org/licenses/>.
 */
#include <JagGlobalDef.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <sys/xattr.h>
#include <atomic>
#include <map>
#include <vector>
#include <unordered_map>
#include <fcntl.h>

#include <boost/array.hpp>

#define CGAL_HEADER_ONLY 1
//#define CGAL_GMPQ_TYPE_H 1

#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/convex_hull_2.h>

#include <abax.h>
#include <JagCfg.h>
#include <JagTableSchema.h>
#include <JagIndexSchema.h>
#include <JagDiskArrayServer.h>
#include <JagFileMgr.h>
#include <JagNet.h>
#include <JagNode.h>
#include <JagStrSplit.h>
#include <JagMD5lib.h>
#include <JagUUID.h>
#include <JagParseExpr.h>
#include <JagTime.h>
#include <JagVector.h>
#include <JagParseParam.h>
#include <JagStrSplitWithQuote.h>
#include <JagBlockLock.h>
#include <JagStack.h>
#include <JagThreadPool.h>
#include <JagMutex.h>
#include <JDFS.h>
#include <JagBlock.h>
#include <JagFixBlock.h>
#include <JagFastCompress.h>
#include <JagHashLock.h>
#include <JagBoundFile.h>
#include <JagLocalDiskHash.h>
#include <JagSchemaRecord.h>
#include <JagDiskArrayClient.h>
#include <JagSimpleBoundedArray.h>
#include <JagSQLMergeReader.h>
#include <JagDBMap.h>
#include <JagFixHashArray.h>
#include <JagDiskKeyChecker.h>
#include <AbaxCStr.h>
#include <base64.h>
#include <JagGeom.h>
//#include <JagBtree.h>
//#include <btree_map.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"
#include <JagLineFile.h>
#include <JagParser.h>


using namespace std;

pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

struct TPass {
	int fd;
	int num;
	void *ptr1;
	void *ptr2;
	void *ptr3;
};

class Counter
{
		public:
        std::atomic<abaxint> value;
    
        void increment(){
			if ( value.is_lock_free() ) {
				printf(" std::atomic<int> is lock free\n");
			}
            ++value;
        }
    
        void decrement(){
            --value;
        }
    
        int get(){
            return value.load();
        }
};
Counter g_cnt;
abaxint g_num;

void writefloat( const char *str, abaxint len, FILE *outf );
void initCheckLicense();
void testjson();
void testrayrecord();
void testwheretree();
void testtime();
time_t my_convert_utc_tm_to_time_t (struct tm *tm);
void testlaststr();
void testjaguarreader();
void testjaguarreader2();
void testUUID();
void testStrSplitWQuote();
void testAtomic( int N);
int test_schema( const char *tabname );
int test_obj( int N);
int test_fixstring( int N);
int test_time();
int test_misc();
int test_vec( int max );
void *useMutex( void *ptr );
void *useNolock( void *ptr );
void *useAtomic( void *ptr );
void *lockFD( void *ptr );
void *regionLock( void *ptr );
//void *condLock( void *ptr );
int test_threads( int N );
void test_stack();

void *task1( void *p );
void *task2( void *p );
void test_threadpool();
void test_fdread();
void test_jdfs();
void test_attr();
void test_blockindex();
void test_fixblockindex();
void test_compress();
void test_hashlock();
void test_size();
void test_parse();
void test_boundfile();
void test_localdiskhash( int N );
void test_diskkeychecker( int N );
// void test_diskarrayclient( int N );
void test_jagarray( int N );
void test_simpleboundedarray( int N );
void test_malloc( int N );
void test_filefamily( );
void test_sqlmerge_reader();
void test_escape();
//void test_keychecker( int N );
void test_map ( int N );
void test_mapvec ( int N );
void test_veconly ( int N );
void test_array ( int N );
void test_lineseg ( int N );
void test_stdhashmap ( int N );
void test_stdmap ( int N );
void test_jaghashmap ( int N );
void test_jaghashmap_nofix ( int N );
void test_jagunordmap ( int N );
void test_jagunordmap_nofix ( int N );

void test_jagfixhasharray ( int N );
void test_sys_getline();
void test_jag_getline();
void test_reply ( const char *inputFile );
void test_str( int N );
void test_geo( int n );
void test_sqrt( int n );
void test_split( int n );
void test_btree( int n );
void test_json( int n );
void test_linefile( int n );
void test_distance();
void test_cgal();
void test_new( long n);

int main(int argc, char *argv[] )
{
	int N = 3;
	if ( argc >= 2 ) {
		N = atoi( argv[1] );
	} 

	// test_misc();
	//testjson();
	//testrayrecord();
	//testwheretree();
	//testtime();
	//testlaststr();
	//testjaguarreader();
	//testjaguarreader2();
	// testUUID();
	// testStrSplitWQuote();
	// testAtomic( N );

	// test_schema( argv[2] );
	// test_obj( N );
	// test_fixstring( N );
	// test_time( );
	// test_vec( N );
	//test_threads( N );

	// test_stack();
	// test_threadpool();
	// test_fdread();
	// test_jdfs();
	// test_attr();
	// test_blockindex();
	// test_fixblockindex();
	// test_compress();
	// test_hashlock();
	// test_size();
	//test_parse();
	// test_boundfile();
	// test_localdiskhash( N );
	// test_diskkeychecker( N );
	// test_diskarrayclient( N );
	// test_jagarray( N );
	// test_simpleboundedarray( N );
	// test_malloc( N );
	// test_filefamily();
	// test_sqlmerge_reader();
	// test_escape();
	// test_keychecker( N );

	//test_array( N );
	// test_lineseg( N );
	// test_map( N );
	// test_mapvec( N );
	// test_veconly( N );

	// test_jaghashmap( N );
	// test_jaghashmap_nofix( N );
	// test_jagunordmap( N );
	// test_jagunordmap_nofix( N );
	// test_stdhashmap( N );
	// test_jagfixhasharray( N );
	// test_sys_getline();
	// test_jag_getline();
	// test_reply( argv[1] );
	//test_str( N );
	//test_geo( N );
	//test_sqrt( N );
	// test_split( N );
	//test_stdmap( N );
	//test_btree( N );
	//test_linefile( N );
	//test_json( N );
	//test_distance();
	test_cgal();
	//test_new( N );
}


#if 0
void testjson()
{
	int max = 200000;

	time_t t1 = time(NULL);
	for ( int i = 0; i < max ; ++i ) {
		Json::Value root;  
		root["uid"] = "u849494944949494";
		root["key"] = "kdk38393jffjfrj";
		root["addr"] = "123 djkdfd  blvd, cr kdk38393jffjfrj";
		root["age"] = "40";
		root["score"] = "90";
	}
	time_t t2 = time(NULL);
	printf("write json %d rows used %d seconds\n", max, t2-t1 );

	/*********
	char *start, *end;
	Json::Value root;  
	root["uid"] = "u849494944949494";
	root["key"] = "kdk38393jffjfrj";
	root["addr"] = "123 djkdfd  blvd, cr kdk38393jffjfrj";
	root["age"] = "40";
	root["score"] = "90";
	// cout << "json root as string " << root.asString() << endl;
	bool rc = root.getString(&start, &end );
	if ( rc ) {
		int len = end - start;
		printf("json start=[%s] length=%d\n", start, len );
	} else {
		printf("root.getString failed\n");
	}

	t1 = time(NULL);
	for ( int i = 0; i < max ; ++i ) {
		root["uid"].asString();
		root["key"].asString();
		root["addr"].asString();
		root["age"].asString();
		root["score"].asString();
		root.getString(&start, &end );
		int len = end - start;
	}
	t2 = time(NULL);
	printf("read json %d rows used %d seconds\n", max, t2-t1 );
	********/

}
#endif

void testrayrecord()
{
	int max = 200000;

	time_t t1 = time(NULL);
	for ( int i = 0; i < max ; ++i ) {
		JagRecord root;  
		root.addNameValue("uid", "u849494944949494");
		root.addNameValue("key", "kdk38393jffjfrj");
		root.addNameValue("addr", "123 djkdfd  blvd, cr kdk38393jffjfrj");
		root.addNameValue("age", "40");
		root.addNameValue("score", "90");
	}
	time_t t2 = time(NULL);
	printf("write rayrecord %d rows used %d seconds\n", max, t2-t1 );

		JagRecord root;  
		root.addNameValue("uid", "u849494944949494");
		root.addNameValue("key", "kdk38393jffjfrj");
		root.addNameValue("addr", "123 djkdfd  blvd, cr kdk38393jffjfrj");
		root.addNameValue("age", "40");
		root.addNameValue("score", "90");
	t1 = time(NULL);
	char *p;
	for ( int i = 0; i < max ; ++i ) {
		p = root.getValue("uid");
		free( p );
		p = root.getValue("key");
		free( p );
		p = root.getValue("addr");
		free( p );
		p = root.getValue("age");
		free( p );
		p = root.getValue("score");
		free( p );
	}
	t2 = time(NULL);
	printf("read rayrecord %d rows used %d seconds\n", max, t2-t1 );

	using namespace rapidjson;
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();   
	writer.Key("key1");  
	writer.String("val1");
	writer.Key("key2");  
	writer.String("val2");
	writer.Key("x1");  
	writer.Double(12.42);
	writer.Key("Null1");  
	writer.Null();
	writer.Key("y1");  
	writer.Double(19.42);
	writer.Double( jagatof("23.434") );
	writer.Key("Null2");  
	writer.Null();

	writer.Key("points");  
	writer.StartArray();   
	for (unsigned i = 0; i < 4; i++) {
		writer.Double(i);
	}
	writer.EndArray();   

	//cout << s.GetString() << endl;

	/**
	JagRecord rec2;
	rec2.fromJSON( s.GetString() );
	rec2.print();

	rec2.toJSON();
	rec2.print();
	**/

}

void testwheretree()
{
	/**
    BinaryOpNode *root;
    BinaryExpressionBuilder b;
    AbaxDataString expression = "1=1 and f=2 or ( 2=2 and f=3) or ( b in ( 1,2) )";
	cout << expression << endl;
    try {
		JagVector<OneNameAttribute> onetable;
        JagVector<OneNameAttribute> oneindex;
        root = b.parse(expression, onetable, oneindex);
        root->print();
        printf("\n");
        //cout << " result = " << root->getValue();
        cout << endl;
    } catch ( const NotWellFormed &e ) {
        printf("exception BinaryExpressionBuilder::NotWellFormed %s \n", e.what());
    } catch ( const char *s ) {
        printf("exception %s\n", s );
    } catch ( ... ) {
        printf("unknown exception\n");
    }
	root->clear();
	delete root;
	***/
}

void testtime()
{
	time_t nowt = time(NULL);
	printf("nowt=%lld\n", nowt );

	struct tm * gmt_tms = gmtime( &nowt );
	printf(" gmtime hour=%d min=%d\n", gmt_tms->tm_hour, gmt_tms->tm_min );

	struct tm * local_tms = localtime( &nowt );
	printf(" local  hour=%d min=%d\n", local_tms->tm_hour, local_tms->tm_min );
	// same !!!

	time_t t1 =  my_convert_utc_tm_to_time_t ( gmt_tms );
	time_t t2 =  my_convert_utc_tm_to_time_t ( local_tms );

	printf(" gmt t1=%lld  diff=%d\n", t1, t1 - nowt );
	printf(" lcoal t2=%lld diff=%d\n", t2, t2-nowt );

#if 0
          struct tm {
               int tm_sec;         /* seconds */
               int tm_min;         /* minutes */
               int tm_hour;        /* hours */
               int tm_mday;        /* day of the month */
               int tm_mon;         /* month */
               int tm_year;        /* year */
               int tm_wday;        /* day of the week */
               int tm_yday;        /* day in the year */
               int tm_isdst;       /* daylight saving time */
           };
#endif

}


time_t my_convert_utc_tm_to_time_t (struct tm *tm)
{
	int tdiff = JagTime::getTimeZoneDiff();
	printf("tdiff = %d min \n", tdiff/60 );
	
	struct timeval tval;
	struct timezone tzone;

    int rc =  gettimeofday( &tval, &tzone );
	printf(" tz_minuteswest=%d tz_dsttime=%d \n", tzone.tz_minuteswest, tzone.tz_dsttime );

	/********
           struct timeval {
               time_t      tv_sec;   
               suseconds_t tv_usec;  
           };

       and gives the number of seconds and microseconds since the Epoch (see time(2)).  The tz argument is a struct timezone:

           struct timezone {
               int tz_minuteswest;     ///  minutes west of Greenwich 
               int tz_dsttime;         // type of DST correction 
           };

	***/

    return tval.tv_sec;
}

void testlaststr()
{
	bool rc;
	AbaxDataString s1 = "hello world";
	AbaxDataString s2 = "world";
	AbaxDataString s3 = "World";

	rc = lastStrEqual(s1.c_str(), s2.c_str(), 12, 4 );
	if ( rc ) {
		printf("s1=%s  last equal s2=%s\n", s1.c_str(), s2.c_str() );
	} else {
		printf("s1=%s  NOT last equal s2=%s\n", s1.c_str(), s2.c_str() );
	}
}

#if 0
void testjaguarreader()
{
    AbaxDataString getstr = "";
    int getint = 0;
    abaxint getlong = 0;
    float getfloat = 0.0;
    double getdouble = 0.0;
    int gettype = 0;

    JaguarReader testjagreader( "test", "u1", "k1 > Apple01234567890 and k1 < Epple01234567890 or k1 = 4ESjbmBauozwu5Rz" );
    JaguarReader testjagreader2( "test", "u2", "uid < 100000" );

    printf("testjagreader 1 begin\n");
    testjagreader.begin();
    while ( testjagreader.next() ) {
        getstr = testjagreader.getString( "k1" );
        gettype = testjagreader.getType( "v1" );
        printf("jagreader 1 string is [%s]\n", getstr.c_str());
    }
    printf("testjagreader 1 end\n");
    printf("testjagreader 2 begin\n");
    testjagreader2.begin();
    while ( testjagreader2.next() ) {
        getint = testjagreader2.getInt( "uid" );
        getlong = testjagreader2.getLong( "uid" );
        getfloat = testjagreader2.getFloat( "amt" );
        getdouble = testjagreader2.getDouble( "unit" );
        getstr = testjagreader2.getTimeString( "daytime" );
        gettype = testjagreader2.getType( "amt" );
        printf("jagreader 2 int [%d] [%d] abaxint [%lld] [%lld] float [%f] [%f] double [%f] [%f] timestring [%s] type [%d]\n", getint, getint*2, getlong, getlong*2, getfloat, getfloat*2, getdouble, getdouble*2, getstr.c_str(), gettype);
    }
    printf("testjagreader 2 end\n");
}


void testjaguarreader2()
{
    AbaxDataString getstr;
    int getint = 0;
    abaxint getlong = 0;
    float getfloat = 0.0;
    double getdouble = 0.0;
    int gettype = 0;

    JaguarReader reader( "test", "jbench" );

    reader.begin();
	int cnt = 0;
    while ( reader.next() ) {
        getstr = reader.getString( "uid" );
		++cnt;
    }

	printf("testjaguarreader2 cnt=%d\n", cnt );
	sleep ( 10 );
}
#endif

void testUUID()
{
	JagUUID uuid;
	AbaxDataString getstr = uuid.getString();
	printf("get uuid string [%s] length [%d]\n", getstr.c_str(), strlen(getstr.c_str()));
}

void testStrSplitWQuote()
{
	AbaxDataString teststr("select * from u1 where (uid = 10 or uid=20); insert into u1 ( uid, addr ) values ( '12435', 'hello;world' ) ; select * from u1 where uid > 100;");
	printf("str [%s]\n", teststr.c_str());
	JagStrSplitWithQuote splitwq(teststr.c_str(), ';');
	splitwq.print();
	printf("len [%d]\n\n", splitwq.length());

	AbaxDataString teststr2("   select * from u1 where uid = 10; ; select * from u1 where uid > 100 or ( uid < 20)");
	printf("str [%s]\n", teststr2.c_str());
	JagStrSplitWithQuote splitwq2(teststr2.c_str(), ';');
	splitwq2.print();
	printf("len [%d]\n\n", splitwq2.length());

	AbaxDataString teststr3(" create table t123 (key: a int, value: b int, c linestring(43), d polygon, e enum('a', 'b', 'c'), f point3d, ); ");
	printf("str [%s]\n", teststr3.c_str());
	JagStrSplitWithQuote splitwq3(teststr3.c_str(), ',');
	splitwq3.print();
	printf("len [%d]\n", splitwq3.length());
	
	const char *p;
	p = strchr( teststr3.c_str(), '(');
	JagStrSplitWithQuote splitwq4(p+1, ',');
	splitwq4.print();
	printf("len [%d]\n", splitwq4.length());

}

void testAtomic( int N )
{
	g_cnt.value = 0;
	JagClock clock;
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		g_cnt.increment();
	}
	clock.stop();
	printf("%d atomic increment %d usec\n", N, clock.elapsedusec() );
	cout << g_cnt.value << endl;
}


int test_obj( int N)
{
	printf("test_obj ...\n");

	char *p;
	char buf[301];
	int i;
	for ( i = 0; i < 300; ++i ) { buf[i] = 'a'; } buf[i] = '\0';

	JagClock clock;

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		p = (char*)malloc(300);
		free(p );
	}
	clock.stop();
	printf("%d  malloc-free(300)=%d usec \n", N,  clock.elapsedusec() );

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		AbaxDataString s(buf);
	}
	clock.stop();
	printf("%d  AbaxDataString=%d usec \n", N,  clock.elapsedusec() );


	srand(10);
	clock.start();
	JagHashMap<AbaxString,abaxint> *map = new JagHashMap<AbaxString,abaxint>();
	AbaxString rs;
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(16);
		map->addKeyValue(rs, i );
	}
	clock.stop();
	printf("%d  JagHashMap(addKeyValue)=%d usec \n", N,  clock.elapsedusec() );

	srand(10);
	clock.start();
	abaxint v;
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(16);
		map->getValue(rs, v );
	}
	clock.stop();
	printf("%d  JagHashMap(getValue)=%d usec \n", N,  clock.elapsedusec() );
	delete map;



	srand(10);
	clock.start();
	jag_hash_t hashtab;
	jag_hash_init ( &hashtab, 10 );
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(16);
		jag_hash_insert_str_int( &hashtab, rs.c_str(), i );
	}
	clock.stop();
	printf("%d  jag_hash_t(jag_hash_insert_str_int)=%d usec \n", N,  clock.elapsedusec() );

	srand(10);
	clock.start();
	int vv;
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(16);
		jag_hash_lookup_str_int( &hashtab, rs.c_str(), &vv );
	}
	clock.stop();
	printf("%d  jag_hash_t(jag_hash_lookup_str_int)=%d usec \n", N,  clock.elapsedusec() );
	jag_hash_destroy(  &hashtab );


	clock.start();
	char buf2[300];
	for ( int i = 0; i < N; ++i ) {
		memset( buf2, 0, 300 );
	}
	clock.stop();
	printf("%d  memset(300)=%d usec \n", N,  clock.elapsedusec() );

	char fpath[] = "/tmp/test.txt";
	FILE *fp;
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		fp = fopen( fpath, "w" );
		fclose( fp );
	}
	clock.stop();
	printf("%d  fopen/fclose=%d usec \n", N,  clock.elapsedusec() );

	int ofd;
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		ofd =  open("/tm/testobj.txt", O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
		close( ofd );
	}
	clock.stop();
	printf("%d  open/close=%d usec \n", N,  clock.elapsedusec() );

	JagReadWriteLock    *_lock = new JagReadWriteLock();
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	}
	clock.stop();
	printf("%d  JagReadWriteMutex::WRITE_LOCK=%d usec \n", N,  clock.elapsedusec() );


	clock.start();
	for ( int i = 0; i < N; ++i ) {
		JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	}
	clock.stop();
	printf("%d  JagReadWriteMutex::READ_LOCK=%d usec \n", N,  clock.elapsedusec() );

	ofd =  open("/tm/testobj.txt", O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
	JagBlockLock                    *_blockLock;
	_blockLock = new JagBlockLock();
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		_blockLock->writeLock( -1 );
		_blockLock->writeUnlock( -1 );
	}
	clock.stop();
	printf("%d  _blockLock->writeLock/unLock(-1)=%d usec \n", N,  clock.elapsedusec() );

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		_blockLock->readLock( -1 );
		_blockLock->readUnlock( -1 );
	}
	clock.stop();
	printf("%d  _blockLock->writeLock/unLock(-1)=%d usec \n", N,  clock.elapsedusec() );


	clock.start();
	for ( int i = 0; i < N; ++i ) {
		_blockLock->writeLock( 0 );
		_blockLock->writeUnlock( 0 );
	}
	clock.stop();
	printf("%d  _blockLock->writeLock/unLock(0)=%d usec \n", N,  clock.elapsedusec() );

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		_blockLock->readLock( 0 );
		_blockLock->readUnlock( 0 );
	}
	clock.stop();
	printf("%d  _blockLock->writeLock/unLock(0)=%d usec \n", N,  clock.elapsedusec() );

	close( ofd );


	fp = fopen( fpath, "w" );
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		fprintf(fp, "%s\n", buf);
	}
	clock.stop();
	printf("%d  fprintf=%d usec \n", N,  clock.elapsedusec() );
	fclose( fp );

	fp = fopen( fpath, "r" );
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		fgets( buf, 299, fp);
	}
	clock.stop();
	printf("%d  fgets=%d usec \n", N,  clock.elapsedusec() );
	fclose( fp );

	int fd;
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		fd = open((char *)fpath, O_RDONLY|JAG_NOATIME, S_IRWXU);
		close(fd );
	}
	clock.stop();
	printf("%d  open-close=%d usec \n", N,  clock.elapsedusec() );

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		{
			JagReadWriteMutex mutex( _lock,  JagReadWriteMutex::WRITE_LOCK );
		}
	}
	clock.stop();
	printf("%d  mutex-write-lock=%d usec \n", N,  clock.elapsedusec() );

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		{
			JagReadWriteMutex mutex( _lock,  JagReadWriteMutex::READ_LOCK );
		}
	}
	clock.stop();
	printf("%d  mutex-read-lock=%d usec \n", N,  clock.elapsedusec() );
	delete _lock;

	/****
	clock.start();
	for ( i = 0; i < 300; ++i ) { buf[i] = 'a'; } buf[i] = '\0';
	for ( int i = 0; i < N; ++i ) {
		JagFileMgr::writeTextFile( "/tmp/aaa.txt", buf );
	}
	clock.stop();
	printf("%d  JagFileMgr::writeTextFile( /tmp/aaa.txt 300)=%d usec \n", N,  clock.elapsedusec() );
	*****/

	pthread_mutex_init( &g_mutex, NULL );

	int a = 0;
	clock.start();
	for ( int i = 0; i < N; ++i ) {
		pthread_mutex_lock ( &g_mutex );
		++a;
		pthread_mutex_unlock ( &g_mutex );
	}
	clock.stop();
	printf("%d  mutext lock/unlock=%d usec \n", N,  clock.elapsedusec() );
	pthread_mutex_destroy( &g_mutex );

}


int test_fixstring( int N)
{
	JagClock clock;

	clock.start();
	for ( int i = 0; i < N; ++i ) {
		AbaxFixString str("  0123456789  ", 14 );
		str.ltrim();
		// printf("ltrim=[%s]\n", str.c_str() );
	}
	clock.stop();
	printf("%d  ltrim=%d usec \n", N,  clock.elapsedusec() );


	clock.start();
	for ( int i = 0; i < N; ++i ) {
		AbaxFixString str("  0123456789  ", 14 );
		str.rtrim();
		// printf("rtrim=[%s]\n", str.c_str() );
	}
	clock.stop();
	printf("%d  rtrim=%d usec \n", N,  clock.elapsedusec() );


	clock.start();
	for ( int i = 0; i < N; ++i ) {
		AbaxFixString str("  0123456789  ", 14 );
		str.trim();
		// printf("trim=[%s]\n", str.c_str() );
	}
	clock.stop();
	printf("%d  trim=%d usec \n", N,  clock.elapsedusec() );

	AbaxFixString str("  0123456789  ", 14 );
	str.substr(2);
	printf("subdstr(2)=[%s]\n", str.c_str() );

	str.substr(3, 5);
	printf("subdstr(3,5)=[%s]\n", str.c_str() );
}

int test_time( )
{
	int tt = 100;
	abaxint sec = JagTime::getTimeZoneDiff();
	printf("timezone diff %d\n", sec );

	int aa = atoi("2013-23-23");
	printf("aa=%d\n", aa );

	aa = atoi("23-13");
	printf("aa=%d\n", aa );

	sec = tt/1000000;
	abaxint usec = tt % 1000000;
	struct tm *tmptr = localtime( (time_t *)&sec );
	char tmstr[48];
	strftime( tmstr, sizeof(tmstr), "%Y-%m-%d %H:%M:%S", tmptr );
	printf("%s\n", tmstr );

	memset( tmstr, 0, 48 );
	bool rc = JagTime::getDateFromStr("2012-12-23", tmstr );
	printf("tmstr=[%s] rc=%d\n", tmstr, rc );

	char buf[8];
	memset( buf, 0, 8 );
	time_t now = time(NULL);
	struct tm *ptm;
	ptm = localtime( &now );
	strftime( buf, 8,  "%z", ptm );

    int hrmin =  atoi( buf );
    int hour = hrmin/100;
    int min = hrmin % 100;
    int totmin;
	if ( hour < 0 ) {
		totmin = hour * 60 - min;
	} else {
		totmin = hour * 60 + min;
	}

	printf("tzone diff=%s   hr=%d min=%d  totmin=%d\n", buf, hour, min, totmin );

	return 0;
}


int test_vec( int max )
{
	for ( int i = 0; i < max; ++i ) {
		JagVector<AbaxDataString> vec;
		vec.append( "jkjdkjdjkdjfdf d" );
	}
}

int test_threads( int N )
{
	pthread_t  thread[200];
	TPass pass[200];

	g_num = 0;
	JagClock  clock;

	int fd =  open("testthread.txt", O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
	if ( fd < 0 ) {
		printf("Error open testthread.txt for O_CREAT|O_RDWR\n");
	}
	write(fd, "OKOKOKOKOK", 10 );

	/*****
	clock.start();
	for ( int i =0; i < N; ++i ) {
		pass[i].fd = fd;
		pthread_create( &thread[i], NULL, lockFD, &(pass[i]) );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useNolock=%d usec\n", N, clock.elapsedusec() );
	****/

	clock.start();
	for ( int i =0; i < N; ++i ) {
		pthread_create( &thread[i], NULL, useNolock, NULL );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useNolock=%d usec\n", N, clock.elapsedusec() );

	clock.start();
	for ( int i =0; i < N; ++i ) {
		pthread_create( &thread[i], NULL, useMutex, NULL );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useMutex=%d usec\n", N, clock.elapsedusec() );

	clock.start();
	for ( int i =0; i < N; ++i ) {
		pthread_create( &thread[i], NULL, useAtomic, NULL );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useAtomic=%d usec\n", N, clock.elapsedusec() );

	clock.start();
	for ( int i =0; i < N; ++i ) {
		pthread_create( &thread[i], NULL, useNolock, NULL );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useNolock=%d usec\n", N, clock.elapsedusec() );

	clock.start();
	for ( int i =0; i < N; ++i ) {
		pthread_create( &thread[i], NULL, useMutex, NULL );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads useMutex=%d usec\n", N, clock.elapsedusec() );

	clock.start();
	JagBlockLock *blk = new JagBlockLock();
	TPass tp[N];
	for ( int i =0; i < N; ++i ) {
		tp[i].ptr1 = (void*)blk;
		tp[i].num = i;
		pthread_create( &thread[i], NULL, regionLock, &tp[i] );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads regionLock=%d usec\n", N, clock.elapsedusec() );

	/******
	JagCondLock *cond = new JagCondLock();
	clock.start();
	for ( int i =0; i < N; ++i ) {
		tp[i].ptr1 = (void*)cond;
		tp[i].num = i;
		pthread_create( &thread[i], NULL, condLock, &tp[i] );
	}
	for ( int i =0; i < N; ++i ) {
    	pthread_join(thread[i], NULL );
	}
	clock.stop();
	printf("%d threads condLock=%d usec\n", N, clock.elapsedusec() );
	******/

	close( fd );

}

void *useMutex( void *ptr )
{
	pthread_mutex_lock ( &g_mutex );
	++ g_num;
	pthread_mutex_unlock ( &g_mutex );
}

void *useAtomic( void *ptr )
{
	g_cnt.increment();
}

void *useNolock( void *ptr )
{
	++ g_num;
}

void *lockFD( void *ptr )
{
	int rc;
	TPass *p = (TPass*) ptr;
	int fd1 = p->fd;  // same fd cannot cause multi-threads block on lock
	struct flock fl;

	// only when each thread opens the same file can each thread be blocked
	// on the F_OFD_SETLKW
	int fd =  open("testthread.txt", O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
	if ( fd < 0 ) {
		printf("Error open testthread.txt for O_CREAT|O_RDWR\n");
	}
	// int fd = dup( fd1 );  // dup cannot cause lock

	fl.l_type = F_WRLCK;
	fl.l_start = 0;
	fl.l_whence = SEEK_SET;
	fl.l_len = 5;
	fl.l_pid = 0;
	//rc = fcntl(fd, F_OFD_SETLKW, &fl);
	printf("thread=%lld fcntl F_OFD_SETLKW rc=%d errno=%d  err=[%s]\n", pthread_self(), rc, errno, strerror( errno ) );

	rc = write( fd, "11", 2 );
	printf("thread=%lld wrote %d bytes to fd=%d\n", pthread_self(), rc, fd );

	close( fd );
}


void *regionLock( void *ptr )
{
	int i;

	TPass *p = (TPass*) ptr;

	JagBlockLock *blockLock = ( JagBlockLock*) p->ptr1;
	int num = p->num;

	i =  rand()%3;
	if ( num == 14 ) {
		printf("%lld %d ****** blockLock->writeLock(-1)...\n", pthread_self(), num );
		fflush( stdout );
		blockLock->writeLock( -1 );
		printf("%lld %d blockLock->writeLock(-1) done\n", pthread_self(), num );
		fflush( stdout );
		printf("%lld %d write sleep 2 (-1) ...\n", pthread_self(), num );
		sleep(2);
		printf("%lld %d write sleep 2 (-1) done\n", pthread_self(), num );
		blockLock->writeUnlock( -1 );
		printf("%lld %d ####### blockLock->writeUnLock(-1) done\n", pthread_self(), num );
		fflush( stdout );
	} else if ( num == 15 ) {
		printf("%lld %d <<<<<< blockLock->readLock(-1)...\n", pthread_self(), num );
		fflush( stdout );
		blockLock->readLock( -1 );
		printf("%lld %d blockLock->readLock(-1) done\n", pthread_self(), num );
		fflush( stdout );
		printf("%lld %d read sleep 2 (-1) ...\n", pthread_self(), num );
		sleep(2);
		printf("%lld %d read sleep 2 (-1) done\n", pthread_self(), num );
		blockLock->readUnlock( -1 );
		printf("%lld %d >>>>>>> blockLock->readUnLock(-1) done\n", pthread_self(), num );
		fflush( stdout );
	} else if ( ( num%3) == 0 ) {
		printf("%lld %d blockLock->writeLock(%d)...\n", pthread_self(), num, i );
		fflush( stdout );
		blockLock->writeLock( i );
		printf("%lld %d blockLock->writeLock(%d) done\n", pthread_self(), num, i );
		fflush( stdout );
		printf("%lld %d sleep 1 ...\n", pthread_self(), num );
		sleep(1);
		printf("%lld %d sleep 1 done\n", pthread_self(), num );
		blockLock->writeUnlock( i );
		printf("%lld %d blockLock->writeUnLock(%d) done\n", pthread_self(), num, i );
		fflush( stdout );
	} else {
		printf("%lld %d blockLock->readLock(%d)...\n",  pthread_self(), num, i );
		fflush( stdout );
		blockLock->readLock( i );
		printf("%lld %d blockLock->readLock(%d) done\n", pthread_self(), num, i );
		fflush( stdout );
		blockLock->readUnlock( i );
		printf("%lld %d blockLock->readUnLock(%d) done\n", pthread_self(), num, i );
		fflush( stdout );
	}
}

/***
void *condLock( void *ptr )
{
	int i;

	TPass *p = (TPass*) ptr;
	JagCondLock *condLock = ( JagCondLock*) p->ptr1;
	int num = p->num;

	if ( (num%5) == 0 ) {
		printf("%lld condLock->setLock()...\n", pthread_self() );
		fflush( stdout );
		condLock->setLock();
		printf("%lld condLock->setLock() done\n", pthread_self() );
		fflush( stdout );
		sleep(3);
		condLock->unlock();
		printf("%lld condLock->unlock() done\n", pthread_self() );
		fflush( stdout );
	} else {
		printf("%lld condLock->checkLock()...\n", pthread_self() );
		fflush( stdout );
		condLock->checkLock();
		printf("%lld condLock->checkLock() done\n", pthread_self() );
		fflush( stdout );
	}
}
***/

int test_misc()
{
	char buf[234];

	char onemax = 255;
	printf("%c %d  %d  %d  %d %d\n", *buf, *buf, *buf == 255, *buf == '\255', *buf == onemax , *buf == -1 );
	AbaxDataString s = "\"thisifdkfd\"";
	AbaxDataString s2 = trimHeadChar(s, '"');
	AbaxDataString s1 = trimTailChar(s, '"');
	AbaxDataString s3 = trimChar(s, '"');
	printf("s=[%s]  nohead=[%s]  notail=[%s]  noheadtail=[%s]\n", s.c_str(), s2.c_str(), s1.c_str(), s3.c_str() );

	s = "\"\"\"";
	s2 = trimHeadChar(s, '"');
	s1 = trimTailChar(s, '"');
	s3 = trimChar(s, '"');
	printf("s=[%s]  nohead=[%s]  notail=[%s]  noheadtail=[%s]\n", s.c_str(), s2.c_str(), s1.c_str(), s3.c_str() );

	JagUUID uuid;
	AbaxDataString str1 = uuid.getString();
	printf("uuid=%s\n", str1.c_str() );


	AbaxDataString str = "||a||b|c||";
	JagStrSplit split( str, '|', true );
	for ( int i =0 ; i < split.length(); ++i ) {
		printf("i=%d  strplit=[%s]\n", i, split[i].c_str() );
	}

	JagClock clock;
	clock.start();
	for ( int i = 0; i < 10000; ++i ) {
		char *md5 = MDString("hellojdjdjd fjfjfjf9393irjfNfne jejejiejeje jckdjffeje jejre ejr ejeiiuer jfdkjfkdj jfd");
	}
	clock.stop();
	printf("s1809 MDString %d times took %d ms\n", 10000, clock.elapsed() );

	char *md5 = MDString("hello");
	printf("hello ==> md5:  [%s]\n", md5 );
	free( md5 );

	md5 = MDString("hello");
	printf("hello ==> md5:  [%s]\n", md5 );
	free( md5 );

	md5 = MDString("helloworld");
	printf("helloworld ==> md5:  [%s]\n", md5 );
	free( md5 );


	AbaxDataString fdir = "/tmp/a/b/c/d";
	JagFileMgr::makedirPath ( fdir );
	printf("mkdir %s \n", fdir.c_str() );

}

void test_stack()
{
	JagStack<int>  ints;

	ints.push(11);
	ints.push(19);
	ints.push(21);
	ints.push(29);
	ints.print();
	ints.pop();
	ints.print();

	struct A { int a; };
}





void *task1( void *p){
	printf("Thread #%u working on task1\n", (int)pthread_self());
	fflush( stdout );
	return NULL;
}


void *task2( void *p ){
	printf("Thread #%u working on task2\n", (int)pthread_self());
	fflush( stdout );
	return NULL;
}


void test_threadpool()
{
	puts("Making threadpool with 4 threads");
	threadpool thpool = thpool_init(4);

	puts("Adding 40 tasks to threadpool");
	int i;
	for (i=0; i<20; i++){
		thpool_add_work(thpool, task1, NULL);
		thpool_add_work(thpool, task2, NULL);
	};

	sleep (2);
	puts("Killing threadpool");
	thpool_destroy(thpool);
	
	return;
}

void test_attr()
{
	char buf[32];
	memset( buf, 0, 32 );

	// removexattr ( "/tmp/a.txt", "user.stripe", );
	setxattr ( "/tmp/a.txt", "user.stripe", "38", 2, 0 );
	getxattr ( "/tmp/a.txt", "user.stripe", buf, 32 );

	printf("buf=[%s]\n", buf );

}


void test_blockindex()
{
	printf("new bi\n");
	JagBlock<JagDBPair> *_blockIndex = new JagBlock<JagDBPair>();
	printf("done new bi\n");

	JagDBPair pair;
	AbaxFixString k;
	abaxint idx;
	
	k = "k5";
	pair = k;
	_blockIndex->updateIndex( pair,  5);
	printf("done k5\n");

	k = "k4";
	pair = k;
	_blockIndex->updateIndex( pair, 4 );
	printf("done k4\n");

	k = "k61";
	pair = k;
	_blockIndex->updateIndex( pair, 21 );
	printf("done k61\n");

	k = "k1";
	pair = k;
	_blockIndex->updateIndex( pair, 1 );
	printf("done k1\n");

	k = "k2";
	pair = k;
	_blockIndex->updateIndex( pair, 2 );
	printf("done k2\n");


	AbaxFixString min = _blockIndex->getMinKey();
	AbaxFixString max = _blockIndex->getMaxKey();
	_blockIndex->print();

	printf("min=[%s]  max=[%s]\n", min.c_str(), max.c_str() );
}

void test_fixblockindex()
{
	printf("new bi\n");
	JagFixBlock *_blockIndex = new JagFixBlock( 16 );
	printf("done new bi\n");

	JagDBPair pair;
	AbaxFixString k;
	abaxint idx;
	
	k = "k5";
	pair = k;
	_blockIndex->updateIndex( pair,  5);
	printf("done k5\n");

	k = "k4";
	pair = k;
	_blockIndex->updateIndex( pair, 4 );
	printf("done k4\n");

	k = "k61";
	pair = k;
	_blockIndex->updateIndex( pair, 21 );
	printf("done k61\n");

	k = "k1";
	pair = k;
	_blockIndex->updateIndex( pair, 1 );
	printf("done k1\n");

	k = "k2";
	pair = k;
	_blockIndex->updateIndex( pair, 2 );
	printf("done k2\n");


	AbaxFixString min = _blockIndex->getMinKey();
	AbaxFixString max = _blockIndex->getMaxKey();
	_blockIndex->print();

	printf("min=[%s]  max=[%s]\n", min.c_str(), max.c_str() );

}

void test_compress()
{
	AbaxDataString src = "insert into jbench values ( kkkkk1, 2vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv2 )";
	// AbaxDataString src = "desc t1";

	JagClock clock;
	clock.start();
	int n = 10000;

	AbaxDataString comp, dest;
	for ( int i = 0; i < n; ++i ) {
		JagFastCompress::compress( src, comp );
    	JagFastCompress::uncompress( comp, dest );
	}
	clock.stop();
	printf(" comp/uncomp %d strings took %d msec\n", n, clock.elapsed() );
	printf("srclen=%d   --> compressed len=%d\n", src.size(), comp.size() );

	src = "thsi ishf inf from fjre wheee jefjfj en = jkdi2848jfdf ";
	JagFastCompress::compress( src, comp );
	printf("srclen=%d   --> compressed len=%d\n", src.size(), comp.size() );

	src = "thsi ishf inf from fjre whee";
	JagFastCompress::compress( src, comp );
	printf("srclen=%d   --> compressed len=%d\n", src.size(), comp.size() );

	src = "thsi ishf inf from fjre whee in th fjdf difufjf  nvnerh fhfeurfhg fdfjdjif djgh";
	JagFastCompress::compress( src, comp );
	printf("srclen=%d   --> compressed len=%d\n", src.size(), comp.size() );

	src = "dfkfj fjgkg gjgthsi ishf inf from fjre whee in";
	JagFastCompress::compress( src, comp );
	printf("srclen=%d   --> compressed len=%d\n", src.size(), comp.size() );

	src="PYOrg4qTQIbymMSjmQDB4hMRcNQqRwxG,\0sl7GqwxecGMzsCQM,\0stnxK8oVTcjKGQo6,cuMA0hMcXyJn8z9A";
	JagFastCompress::compress( src, comp );
	AbaxDataString uncomp;
	JagFastCompress::uncompress( comp, uncomp );
	printf(" src=[%s] uncomp=[%s]\n", src.c_str(), uncomp.c_str() );
	printf("srclen=%d   --> compressed len=%d  ucomlen=%d\n", src.size(), comp.size(), uncomp.size() );

	src=AbaxDataString("PYOrg4qTQIbymMSjmQDB4hMRcNQqRwxG,NULL...\0sl7GqwxecGMzsCQM,\0stnxK8oVTcjKGQo6,cuMA0hMcXyJn8z9A", 80 );
	JagFastCompress::compress( src, comp );
	JagFastCompress::uncompress( comp, uncomp );
	printf(" src=[%s] uncomp=[%s]\n", src.c_str(), uncomp.c_str() );
	printf("srclen=%d   --> compressed len=%d  ucomlen=%d\n", src.size(), comp.size(), uncomp.size() );



	src="shortstr";
	JagFastCompress::compress( src, comp );
	JagFastCompress::uncompress( comp, uncomp );
	printf(" src=[%s] uncomp=[%s]\n", src.c_str(), uncomp.c_str() );
	printf("srclen=%d   --> compressed len=%d  ucomlen=%d\n", src.size(), comp.size(), uncomp.size() );


	src="s";
	JagFastCompress::compress( src, comp );
	JagFastCompress::uncompress( comp, uncomp );
	printf(" src=[%s] uncomp=[%s]\n", src.c_str(), uncomp.c_str() );
	printf("srclen=%d   --> compressed len=%d  ucomlen=%d\n", src.size(), comp.size(), uncomp.size() );

	src="stu";
	JagFastCompress::compress( src, comp );
	JagFastCompress::uncompress( comp, uncomp );
	printf(" src=[%s] uncomp=[%s]\n", src.c_str(), uncomp.c_str() );
	printf("srclen=%d   --> compressed len=%d  ucomlen=%d\n", src.size(), comp.size(), uncomp.size() );


	AbaxDataString instr;
	AbaxDataString outstr;

	AbaxString rd;
	int len = 10000;
	rd = rd.randomValue( len );
	instr = rd.c_str();
	JagFastCompress::compress( instr, outstr );
	prt(("%d random bytes compressed to %d byte\n", len, outstr.size() ));

}

void test_hashlock()
{
	abaxint ps = sysconf( _SC_PAGESIZE );
	abaxint np = sysconf( _SC_AVPHYS_PAGES );
	printf("_SC_PAGESIZE=%lld  _SC_AVPHYS_PAGES=%lld\n", ps, np );

	JagHashLock lock;

	JagClock clock;
	clock.start();
	int n = 10000;
	for ( int i =0; i < n; ++i ) {
		lock.writeLock( "a1");
		lock.writeUnlock( "a1");
	}
	clock.stop();
	printf("JagHashLock %d times used %d millisec\n", n, clock.elapsed() );


	lock.writeLock( "a.a");
	printf("a.a lock obtained\n");

	lock.writeLock( "a.b");
	printf("a.b lock obtained\n");

	lock.readLock( "c.c");
	printf("c.c read lock obtained\n");

	lock.readLock( "-1");
	printf("-1 read lock obtained\n");

	lock.readLock( "a.b");
	printf("a.b read lock obtained\n");

}

void test_size()
{
	JagDBPair pair;

	JagVector<JagDBPair> dbvec;
	dbvec.append(pair);
	int vectot =  sizeof(dbvec) + dbvec.size() * sizeof(JagDBPair);

	printf("size of JagDBPair=%d  pair.size=%d sizeof(dbvec)=%d\n", sizeof(JagDBPair), sizeof(pair), sizeof(dbvec) );
	printf("vectotsize=%d\n", vectot );

	AbaxFixString key("01234567890123456789", 20 );
	AbaxFixString val("01234567890123456789", 20 );
	JagDBPair pair2(key, val);
	dbvec.append(pair2);
	vectot =  sizeof(dbvec) + dbvec.size() * sizeof(JagDBPair);
	printf("pair2 size=%d\n", sizeof( pair2) );

	printf("size of JagDBPair=%d  pair.size=%d sizeof(dbvec)=%d\n", sizeof(JagDBPair), sizeof(pair), sizeof(dbvec) );
	printf("vectotsize=%d\n", vectot );

}


void test_parse()
{
	char cmd[256];
	strcpy( cmd, "this is a [ fjdkjfd ] (dddd,dddd, kdkdd ) sign =; \"jdjdjdjd;ddd[odod]=ddddd\" jfd hhahaha 'hahahahah[dddd] ddd' end" );
	//strcpy( cmd, "this is a (dddd,dddd, kdkdd ) sign ");
	//printf("cmd=[%s]\n", cmd );
    char *saveptr, *gettok;

	/***
    gettok = jag_strtok_r(cmd, " ;", &saveptr );
	printf("111 cmd=[%s]\n", cmd );
	printf("111 tok=[%s] saveptr=[%s]\n", gettok, saveptr );
    while ( gettok ) {
       	gettok = jag_strtok_r(NULL, " ;,[]=", &saveptr );
		printf("tok=[%s] saveptr=[%s]\n", gettok, saveptr );
    }
	***/

	strcpy( cmd, " key: uid int, value: b int default '1', c char(32) default 'SFO', d enum('a  ','b') default 'a' " );
    gettok = jag_strtok_r_bracket(cmd, ",", &saveptr );
	printf("222 cmd=[%s]\n", cmd );
	printf("tok=[%s] saveptr=[%s]\n", gettok, saveptr );
    while ( gettok ) {
       	gettok = jag_strtok_r_bracket(NULL, ",", &saveptr );
		printf("tok=[%s] saveptr=[%s]\n", gettok, saveptr );
    }

	JagVector<JagPolygon> pgvec;
	const char *p = "( ( (0 0, 3 3, 2 2, 9 8, 0 0)), ( (10 0, 38 39, 22 2, 91 87, 10 0)) ) "; 
	JagParser::addMultiPolygonData( pgvec, p, true, false, false );
	pgvec.print();

}
   

void test_boundfile()
{
	JagBoundFile bf ( "/tmp/bound.txt", 5 );
	bf.openAppend();
	bf.appendLine( "line 1");
	bf.appendLine( "line 2");
	bf.appendLine( "line 3");
	bf.appendLine( "line 4");
	bf.appendLine( "line 5");
	bf.appendLine( "line 6");
	bf.appendLine( "line 7");
	bf.appendLine( "line 8");
	bf.close();
	
	bf.openRead();
	JagVector<AbaxDataString> vec;
	bf.readLines( 20, vec );
	for ( int i = 0; i < vec.length(); ++ i ) {
		printf("s1922 line=[%s]\n", vec[i].c_str() );
	}

}

void test_localdiskhash( int N )
{
	int klen = 10;
	int vlen = 20;
	JagLocalDiskHash dh( "test", klen, vlen );

	AbaxString k, v;
	AbaxFixString  key, val;
	int rc;
	srand( 0 );
	int num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		// printf("insert key=[%s]\n", k.c_str() );
		v = AbaxString::randomValue( vlen );
		AbaxFixString key( k.c_str(), klen );
		AbaxFixString val( v.c_str(), vlen );
		JagDBPair pair( key, val);

		rc = dh.insert( pair );
		if ( rc ) {
			++num;
			// dh.print();
		}
	}
	printf("inserted num=%d\n", num );
	// dh.print();

	srand( 0 );
	num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		//printf("read key=[%s]\n", k.c_str() );
		v = AbaxString::randomValue( vlen );
		AbaxFixString key( k.c_str(), klen );
		JagDBPair pair( key );
		rc = dh.get( pair );
		if ( rc ) {
			++num;
		} else {
			printf("s3848 *** key=[%s] not found\n", k.c_str() );
			// dh.print();
		}
	}
	printf("read num=%d\n", num );

	srand( 0 );
	num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		v = AbaxString::randomValue( vlen );
		v = "00000000000000000000009292929999****************929293939339";
		AbaxFixString key( k.c_str(), klen );
		AbaxFixString val( v.c_str(), vlen );
		JagDBPair pair( key, val);
		rc = dh.set( pair );
		if ( rc ) {
			++num;
		} else {
			printf("s3848 *** key=[%s] not set\n", k.c_str() );
			// dh.print();
		}
	}
	printf("update num=%d\n", num );
	// dh.print();

	printf("Start remove ...****\n");
	srand( 0 );
	num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		v = AbaxString::randomValue( vlen );
		AbaxFixString key( k.c_str(), klen );
		AbaxFixString val( v.c_str(), vlen );
		JagDBPair pair( key, val);
		// printf("s2239 remove key=[%s] ...\n", k.c_str() );
		rc = dh.remove( pair );
		if ( rc ) {
			++num;
		} else {
			printf("s3848 *** key=[%s] not del\n", k.c_str() );
		}
		// dh.print();
	}
	printf("del num=%d \n", num );
	dh.print();

}

/*****
void test_diskarrayclient( int N )
{
	AbaxDataString fpath = AbaxDataString(getenv("HOME")) + "/commit/DiskArrayClient";
	JagSchemaRecord record;
	int KLEN = 16;
	int VLEN = 32;
	AbaxDataString s = record.makeSimpleSchema( "uid", 16, "addr", 32 );
	record.parseRecord( s.c_str() );
	JagDiskArrayClient *darr = new JagDiskArrayClient( fpath, &record);

	JagDBPair retpair;
	char buf[KLEN+VLEN+1];
	memset( buf, 0, KLEN+VLEN+1 );
	int insertCode;
	AbaxString k, v;

	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( KLEN );
		v = AbaxString::randomValue( VLEN );
		memcpy( buf, k.c_str(), KLEN );
		memcpy( buf+KLEN, v.c_str(), VLEN );
		JagDBPair tpair(buf, KLEN, buf+KLEN, VLEN, false );
		darr->insertSync(tpair);
	}

	darr->flushInsertBufferSync();
	prt(("Done insert %d rows\n", N ));

    JagSingleBuffReader navig( darr->getFD(), darr->_arrlen, KLEN, VLEN );
	abaxint i;
	abaxint cnt=0;
    while ( navig.getNext( buf, KLEN+VLEN, i ) ) {
		++cnt;
	}
	prt(("Done read %d rows\n", cnt ));

}
****/


void test_jagarray( int N )
{
	AbaxDataString s1, s2;

	printf("test_jagarray N=%d\n", N );
	JagArray<AbaxPair<AbaxInt, AbaxInt> > jar;
	AbaxInt k,v;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxInt::randomValue();
		// k = i;
		v =  100;
		AbaxPair<AbaxInt, AbaxInt> pair(k, v );
		jar.insert( pair );
	}
	printf("JagArray has %d elements   %d cells\n", jar.elements(), jar.size() );

	int cnt = 0;
	for ( int i = 0; i < N; ++i ) {
		k = i;
		v = 100;
		AbaxPair<AbaxInt, AbaxInt> pair(k, v );
		if ( jar.exist( pair ) ) {
			++cnt;
		}
	}
	printf("JagArray found %d elements\n", cnt );
}

void test_simpleboundedarray( int N )
{
	JagSimpleBoundedArray<AbaxPair<AbaxInt, AbaxInt> > jar(10);

	AbaxInt k,v;
	for ( int i = 0; i < N; ++i ) {
		k = i;
		v =  999;
		AbaxPair<AbaxInt, AbaxInt> pair(k, v );
		jar.insert( pair );
	}

	jar.begin();
	AbaxPair<AbaxInt, AbaxInt> pair;
	while ( jar.next( pair ) ) {
		printf("*** key=%d  val=%d\n", pair.key.value(), pair.value.value() );
	}

}

void test_malloc( int N )
{
	prt(("t3829 test_malloc ...\n"));
	abaxint len = 2000000000;
	char *p = (char*)malloc( len );
	memset( p, 0, len );
	free( p );
	prt(("t3829 test_malloc done len=%lld \n", len ));
}


void test_filefamily()
{
	AbaxDataString fullpath = "/tmp";
	AbaxDataString fam = JagFileMgr::getFileFamily( fullpath, "t123.jdb" );
	prt(("fam=[%s]\n", fam.c_str() ));

	fam = JagFileMgr::getFileFamily( fullpath, "t123.idx.jdb" );
	prt(("fam=[%s]\n", fam.c_str() ));

}

void test_sqlmerge_reader()
{
	AbaxDataString files = "a1.sql|a2.sql|a3.sql";
	JagSQLMergeReader mrdr( files );
	AbaxDataString sql;

	while ( mrdr.getNextSQL( sql ) ) {
		prt(("sql=[%s]\n", sql.c_str() ));
	}
}

void test_escape()
{
	AbaxDataString instr = "hihi\n2222\n333\n\n";
	AbaxDataString out;
	escapeNewline( instr, out );
	printf("instr=[%s]\n", instr.c_str() );
	printf("out=[%s]\n", out.c_str() );
}

/***
void test_keychecker( int N )
{
	printf("Begin test_keychecker: N=%d\n", N );
	char vbuf[3]; 
	vbuf[0] = '2';
	vbuf[1] = '3';
	vbuf[2] = '\0';
	AbaxFixString vstr(  vbuf, 2, 1 ); // point to vbuf

   	JagKeyChecker * _keyChecker = new JagKeyChecker();
   	for ( int i = 0; i < N; ++i ) {
   		AbaxString str  = AbaxString::randomValue(30);
   		AbaxFixString k  = AbaxDataString( str.c_str() );
   		_keyChecker->addKeyValue( k, vstr );
   	}

	printf("Done test_keychecker elems=%d.\n", _keyChecker->size() );
	fflush( stdout );
	sleep(20);

	delete _keyChecker;
	jagmalloc_trim( 0 );
	printf("delete test_keychecker\n" );
	fflush( stdout );
	sleep(20);
}
***/

void test_map ( int N )
{
	JagDBMap *jmap = new JagDBMap();

	AbaxString k, v;
	AbaxFixString key("k1");
	AbaxFixString val("v1");
	AbaxFixString fk, fv;

	time_t t1 = time(NULL);
	v = AbaxString::randomValue(3000);
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue(48);
		fk = k.c_str(); fv = v.c_str();
		JagDBPair pair(fk, fv );
		jmap->insert(pair);
	}
	time_t t2 = time(NULL);
	printf("Insert map %d done in %d secs\n", N, t2-t1 );
	sleep( 10 );

}

void test_veconly ( int N )
{
	AbaxString k, v;
	AbaxFixString key("k1");
	AbaxFixString val("v1");
	AbaxFixString fk, fv;
	char buf[12];
	std::vector<AbaxFixString> *vec = new std::vector<AbaxFixString>(20000);

	time_t t1 = time(NULL);
	v = AbaxString::randomValue(3000);
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue(48);
		fk = k.c_str(); fv = v.c_str();

		vec->push_back( fv );
		sprintf( buf, "%lld", i );
		fv = buf;
	}
	time_t t2 = time(NULL);
	printf("Insert vec %d done in %d secs\n", N, t2-t1 );
	sleep( 10 );

}
void test_mapvec ( int N )
{
	JagDBMap *jmap = new JagDBMap();

	AbaxString k, v;
	AbaxFixString key("k1");
	AbaxFixString val("v1");
	AbaxFixString fk, fv;
	char buf[12];
	std::vector<AbaxFixString> *vec = new std::vector<AbaxFixString>(20000);


	time_t t1 = time(NULL);
	v = AbaxString::randomValue(3000);
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue(48);
		fk = k.c_str(); fv = v.c_str();
		vec->push_back( fv );

		sprintf( buf, "%lld", i );
		fv = buf;

		JagDBPair pair(fk, fv );
		jmap->insert(pair);
	}
	time_t t2 = time(NULL);
	printf("Insert mapvec %d done in %d secs\n", N, t2-t1 );
	sleep( 10 );

}

void test_lineseg ( int N )
{
	JagArray<JagLineSeg2DPair> *arr = new JagArray<JagLineSeg2DPair>(32 );
	AbaxString k, v;
	double x1,y1,x2,y2;
	JagSortPoint2D *points = new JagSortPoint2D[2*N];
	JagClock clock;
	int cnt = 0;
	for  ( int i=0; i < 2*N; ++i ) {
		 x1 = rand() % 1000;
		 y1 = rand() % 1000;
		 x2 = x1 + 100;
		 y2 = y1 + 100;
		 points[i].x1 = x1; points[i].y1 = y1;
		 points[i].x2 = x2; points[i].y2 = y2;
	}

	JagLineSeg2DPair seg; 
	seg.value = '1';
	for ( int i=0; i < N; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		arr->insert(seg);
	}

	//arr->printKey();

	const JagLineSeg2DPair *pp;
	const JagLineSeg2DPair *pp2;
	clock.start();
	cnt = 0;
	int cp = 0;
	for ( int i = 0; i < N; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		pp = arr->getPred( seg );
		if (! pp ) {
			prt(("s3098 seg has no pred\n"  ));
			seg.println();
			continue;
		}
		++cp;
		pp2 = arr->getSucc( *pp );
		if (  pp2 && ( seg == *pp2 ) ) {
			++cnt;
		}
	}
	clock.stop();
	printf("Get array predfound=%d  pred->succ=self=%d  done in %d millissecs\n", cp, cnt, clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		pp = arr->getSucc( seg );
		if (! pp ) {
			prt(("s3498 seg has no succ\n"  ));
			seg.println();
			continue;
		}
		++cp;
		pp2 = arr->getPred( *pp );
		if (  pp2 && ( seg == *pp2 ) ) {
			++cnt;
		}
	}
	clock.stop();
	printf("Get array succfound=%d  succ->pred=self=%d  done in %d millissecs\n", cp, cnt, clock.elapsed() );



	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		arr->remove( seg );
	}
	clock.stop();
	printf("Remove done in %d millissecs\n", clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		if ( arr->exist( seg ) ) ++cnt;
	}
	clock.stop();
	printf("Exist done remain=%d in %d millissecs\n", cnt, clock.elapsed() );

	printf("\n");
	delete arr;
	delete [] points;

}
void test_array ( int N )
{
	//JagArray<JagDBPair> *arr = new JagArray<JagDBPair>(32, 1);
	JagArray<JagDBPair> *arr = new JagArray<JagDBPair>(32, 0);
	AbaxString k, v;
	AbaxFixString fk, fv;

	std::vector<AbaxString> *vec = new std::vector<AbaxString>(N);
	JagClock clock;
	int cnt = 0;
	for  ( int i=0; i < N; ++i ) {
		(*vec)[i] = AbaxString::randomValue(32);
	}

	clock.start();
	JagDBPair pair;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		fv = fk;
		pair.key = fk;
		pair.value = fv;
		arr->insert(pair);
	}
	clock.stop();
	printf("Insert array %d done in %d millissecs\n", N, clock.elapsed() );
	//arr->print( false );

	clock.start();
	cnt = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		fv = fk;
		pair.key = fk;
		pair.value = fv;
		if ( arr->exist( pair ) ) ++cnt;
	}
	clock.stop();
	printf("Seach array found=%d done in %d millissecs\n", cnt, clock.elapsed() );

	clock.start();
	cnt = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		if ( arr->get( pair ) ) ++cnt;
	}
	clock.stop();
	printf("Get array found=%d done in %d millissecs\n", cnt, clock.elapsed() );


	const JagDBPair *pp;
	const JagDBPair *pp2;
	clock.start();
	cnt = 0;
	int cp = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		pp = arr->getPred( pair );
		if (! pp ) {
			prt(("s3098 pair.key=%s has no pred\n", pair.key.c_str() ));
			continue;
		}
		++cp;
		//prt(("pred=[%s] <--- pair=[%s]\n", pp->key.c_str(), pair.key.c_str() ));
		pp2 = arr->getSucc( *pp );
		if (  pp2 && ( pair == *pp2) ) {
			++cnt;
		}
	}
	clock.stop();
	printf("Get array predfound=%d  pred->succ=self=%d  done in %d millissecs\n", cp, cnt, clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		pp = arr->getSucc( pair );
		if (! pp ) {
			prt(("s3038 pair.key=%s has no succ\n", pair.key.c_str() ));
			continue;
		}
		++cp;
		//prt(("pair=[%s] --> succ=[%s]\n", pair.key.c_str(), pp->key.c_str() ));

		pp2 = arr->getPred( *pp );
		if (  pp2 && ( pair == *pp2) ) {
			++cnt;
		}
	}
	clock.stop();
	printf("Get array succfound=%d  succ->pred=self=%d  done in %d millissecs\n", cp, cnt, clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		if ( arr->exist( pair ) ) ++cnt;
	}
	clock.stop();
	printf("Exist done remain=%d in %d millissecs\n", cnt, clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		arr->remove( pair );
	}
	clock.stop();
	printf("Remove done in %d millissecs\n", clock.elapsed() );

	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i = 0; i < N; ++i ) {
		fk = (*vec)[i].c_str(); 
		pair.key = fk;
		pair.value = fv;
		if ( arr->exist( pair ) ) ++cnt;
	}
	clock.stop();
	printf("Exist done remain=%d in %d millissecs\n", cnt, clock.elapsed() );

	printf("\n");


	delete arr;
}

void test_jaghashmap ( int N )
{
	JagClock clock;
	clock.start();
	JagHashMap<AbaxString, AbaxFixString> *map = new JagHashMap<AbaxString,AbaxFixString>();
	AbaxString rs;
	AbaxFixString v("aa", 2, 1 );
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		map->addKeyValue(rs, v );
	}
	clock.stop();
	printf("%d  JagHashMap(addKeyValue)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);
}

void test_jaghashmap_nofix ( int N )
{
	JagClock clock;
	clock.start();
	JagHashMap<AbaxString, AbaxDataString> *map = new JagHashMap<AbaxString,AbaxDataString>();
	AbaxString rs;
	AbaxDataString v("aa");
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		map->addKeyValue(rs, v );
	}
	clock.stop();
	printf("%d  JagHashMap_nofix(addKeyValue)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);
}

/*******
void test_jagunordmap ( int N )
{
	JagClock clock;
	clock.start();
	JagUnordMap<AbaxDataString, AbaxFixString> *map = new JagUnordMap<AbaxDataString, AbaxFixString>();
	AbaxString rs;
	AbaxFixString v("aa", 2, 1 );
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		map->addKeyValue( rs.c_str(), v );
	}
	clock.stop();
	printf("%d  JagUnordMap(addKeyValue)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);
}

void test_jagunordmap_nofix ( int N )
{
	JagClock clock;
	clock.start();
	JagUnordMap<AbaxDataString, AbaxDataString> *map = new JagUnordMap<AbaxDataString, AbaxDataString>();
	AbaxString rs;
	AbaxDataString v("aa");
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		map->addKeyValue( rs.c_str(), v );
	}
	clock.stop();
	printf("%d  JagUnordMap_nofix(addKeyValue)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);
}
*****/


/*****
void test_stdhashmap ( int N )
{
	JagClock clock;
	clock.start();
	std::unordered_map<AbaxDataString,AbaxFixString> *map = new std::unordered_map< AbaxDataString,AbaxFixString>();
	AbaxString rs;
	AbaxFixString v("aa", 2, 1 );
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		map->insert( std::make_pair<AbaxDataString,AbaxFixString>( AbaxDataString(rs.c_str()), v) );
	}
	clock.stop();
	printf("%d  StdHashMap(addKeyValue)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);
}
*****/

void test_stdmap ( int N )
{
	printf("\n");
	JagClock clock;
	std::map<std::string,std::string> *map = new std::map< std::string,std::string>();
	std::string rs;
	clock.start();
	std::vector<std::string> *vec = new std::vector<std::string>(N);
	for ( int i = 0; i < N; ++i ) {
		(*vec)[i] = rs.c_str();
		rs = AbaxString::randomValue(20).c_str();
		map->insert( std::pair<std::string,std::string>( rs.c_str(), rs.c_str() ) );
	}
	clock.stop();
	printf("%d  StdMap(insert)  %d usedmsec \n", N,  clock.elapsed() );


	clock.start();
	int cnt = 0;
	for ( int i = 0; i < N; ++i ) {
		if ( map->find( (*vec)[i] ) != map->end() ) ++cnt;
	}
	clock.stop();
	printf("%d  StdMap(find) used msec %d \n", cnt,  clock.elapsed() );

	delete map;
	delete vec;
	jagmalloc_trim( 0 );
	printf("done delete\n");
}

void test_jagfixhasharray ( int N )
{
	JagFixHashArray *map = new JagFixHashArray(20, 2);
	AbaxDataString save[N];

	JagClock clock;
	clock.start();
	AbaxString rs;
	char rec[22];
	for ( int i = 0; i < N; ++i ) {
		rs = rs.randomValue(20);
		// map->addKeyValue( rs.c_str(), v );
		memcpy( rec, rs.c_str(), 20 );
		memcpy( rec+20, "aa", 2 );
		map->insert( rec );
		save[i] =  rs.c_str();
	}
	clock.stop();
	printf("%d  fixhasharray(insert)=%d msec \n", N,  clock.elapsed() );
	sleep(10);

	abaxint idx;
	abaxint cnt = 0;
	for ( int i = 0; i < N; ++i ) {
		if ( map->exist( save[i].c_str(), &idx ) ) {
			++cnt;
		}
	}
	printf("Found %d elements\n", cnt );

	rs = rs.randomValue(20);
	if (  map->exist( rs.c_str(), &idx ) ) {
		printf("found rnd str\n");
	} else {
		printf("not found rnd str\n");
	}

	rs = rs.randomValue(20);
	if (  map->exist( rs.c_str(), &idx ) ) {
		printf("found rnd str\n");
	} else {
		printf("not found rnd str\n");
	}

	char buf[23];
	memcpy(buf, save[2].c_str(), 20 );
	buf[22] = '\0';
	if (  map->get( buf ) ) {
		printf("found save2 str [%s]\n", buf );
	} else {
		printf("not found save2 str [%s]\n", buf);
	}

	delete map;
	jagmalloc_trim( 0 );
	printf("done delete\n");
	sleep(10);

}

void test_diskkeychecker( int N )
{
	int klen = 10;
	int vlen = 20;
	JagDiskKeyChecker dh( "test", klen, vlen );

	char kv[klen+JAG_KEYCHECKER_VLEN+1];
	char value[JAG_KEYCHECKER_VLEN+1];

	AbaxString k, v;
	AbaxFixString  key, val;
	int rc;
	srand( 0 );
	int num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		v = "12";
		memset( kv, 0, klen+JAG_KEYCHECKER_VLEN+1 );
		sprintf(kv, k.c_str(), klen );
		sprintf(kv+JAG_KEYCHECKER_KLEN, v.c_str(), JAG_KEYCHECKER_VLEN );

		rc = dh.addKeyValue(kv);
		if ( rc ) {
			++num;
		}
	}
	printf("inserted num=%d\n", num );

	srand( 0 );
	num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		v = "12";
		memset( kv, 0, klen+JAG_KEYCHECKER_VLEN+1 );
		sprintf(kv, k.c_str(), klen );
		sprintf(kv+JAG_KEYCHECKER_KLEN, v.c_str(), JAG_KEYCHECKER_VLEN );
		rc = dh.getValue( k.c_str(), value );
		if ( rc ) {
			++num;
		} else {
			printf("s3848 *** key=[%s] not found\n", k.c_str() );
			// dh.print();
		}
	}
	printf("read num=%d\n", num );

	printf("Start remove ...****\n");
	srand( 0 );
	num = 0;
	for ( int i = 0; i < N; ++i ) {
		k = AbaxString::randomValue( klen );
		rc = dh.removeKey( k.c_str() );
		if ( rc ) {
			++num;
		} else {
			printf("s3848 *** key=[%s] not del\n", k.c_str() );
		}
		// dh.print();
	}
	printf("del num=%d \n", num );

}


void test_sys_getline()
{
	FILE *fp = fopen("getline.txt", "r" );
	if ( ! fp ) {
		printf("error open getline.txt\n" );
		return;
	}

	char *cmdline = NULL;
	size_t  sz=0;

	while ( 1 ) {
        if ( getline( (char**)&cmdline, &sz, fp ) == -1 ) {
            break;
        }
		printf("sz=%d %s", sz, cmdline );
		if ( cmdline ) free( cmdline );
		cmdline = NULL;
	}

	printf("cmdline=[%s]\n", cmdline );
	if ( cmdline ) {
		printf("free cmdline\n");
		free( cmdline );
	}
	fclose( fp );
}

void test_jag_getline()
{
	FILE *fp = fopen("getline.txt", "r" );
	if ( ! fp ) {
		printf("error open getline.txt\n" );
		return;
	}

	char *cmdline = NULL;
	size_t  sz=0;
	while ( 1 ) {
        if ( jaggetline( (char**)&cmdline, &sz, fp ) == -1 ) {
            break;
        }
		printf("sz=%d %s", sz, cmdline );
		free( cmdline );
		cmdline = NULL;
	}

	printf("cmdline=[%s]\n", cmdline );

	if ( cmdline ) {
		printf("free cmdline\n");
		free( cmdline );
	}
	fclose( fp );
}

void test_reply ( const char *inputFile )
{
	/***
	JagVector<AbaxDataString> xvec, yvec;
	AbaxDataString xname, yname;
	bool xisvalue;
	readInputFile( inputFile, xname, yname, xvec, yvec, xisvalue );
	for ( int i = 0; i < xvec.length(); ++i ) {
		printf("xvec i=%d  xvec[i]=[%s]\n", i, xvec[i].c_str() );
	}
	***/
}

void test_str( int N ) 
{
	unsigned char s[4]; s[0] = 23; s[1] = 12; s[2] = 32; s[3] = 99;
	std::string s1 = "0123456";
	size_t f1 = s1.find(s[3]);
	printf("std::string find(2) ==> [%d]\n", f1 );
	for ( int i=0; i < s1.size(); ++i ) {
		printf("%c ", s1[i] );
	}
	printf("\n" );

	//std::string ns(NULL);
	//printf("null=[%s]", ns.c_str() );
	std::string ns2;
	printf("ns2 null=[%s]\n", ns2.c_str() );


	AbaxCStr s2 = "0123456";
	size_t f2 = s2.find(s[3]);
	printf("AbaxCStr find(2) ==> [%d]\n", f2 );
	for ( int i=0; i < s2.size(); ++i ) {
		printf("%c ", s2[i] );
	}
	printf("\n" );

	AbaxDataString ss = "hello worldidifjdjdjfjd 0jjfdjfd hahah";
	AbaxDataString enc1 =  abaxEncodeBase64( ss );
	AbaxDataString ss2 =  abaxDecodeBase64( enc1 );
	prt(("ss=[%s]\n", ss.c_str() ));
	prt(("base64 enc-dec: ss2=[%s]\n", ss2.c_str() ));

	char buf[100];
	strcpy(buf, "1.0" );
	printf("buf=[%s]\n", buf );
	stripTailZeros( buf, 3 );
	printf("buf=[%s]\n", buf );

	//jagfwritefloat(buf, 3, stdout );
	writefloat(buf, 3, stdout );
	printf("done\n" );


	const char *str1 = "hello world, welcome to baidu, ffd 3939 kdkd";
	const char *p = KMPstrstr(str1, "baidu" );
	prt(("baidu=[%s]\n", p ));

	p = KMPstrstr(str1, "" );
	prt(("baidu=[%s]\n", p ));

	p = KMPstrstr(str1, "come" );
	prt(("come=[%s]\n", p ));

	p = KMPstrstr("aa", "come" );
	prt(("come=[%s]\n", p ));

	p = KMPstrstr("come", "come" );
	prt(("come=[%s]\n", p ));

	AbaxDataString h1= "jaaj ";
	AbaxDataString h2= "xxxxx ";
	AbaxDataString h3 = h1 + "|" + h2;
	prt(("h3=[%s]\n", h3.c_str() ));

	JagClock clock;
	clock.start();
	for ( int i=0; i < N; ++i ) {
		AbaxDataString h = "903jjfkdjf kfdjfjd fdjf94jff jjfk djf df fjdjfdjf djf dj  fjd fjdkkfjdjf9rjfkdjfdk fjdkfjdf df d";
		AbaxDataString d = "jdjdjfkd fjdkfjdjfdkfjdkjfd jkfdfjdjfdjfkdjfk djfdkjfkdjkfdjkfjdkjf djfkdjkf dkfjdk fkdjfkdj fdj fd";
		AbaxDataString s = h + d;
		AbaxDataString g;
		g = h;
	}
	clock.stop();
	prt(("AbaxDataString init + = took %d millisec\n", clock.elapsed() ));

	clock.start();
	for ( int i=0; i < N; ++i ) {
		std::string h = "903jjfkdjf kfdjfjd fdjf94jff jjfk djf df fjdjfdjf djf dj  fjd fjdkkfjdjf9rjfkdjfdk fjdkfjdf df d";
		std::string d = "jdjdjfkd fjdkfjdjfdkfjdkjfd jkfdfjdjfdjfkdjfk djfdkjfkdjkfdjkfjdkjf djfkdjkf dkfjdk fkdjfkdj fdj fd";
		std::string s = h + d;
		std::string g;
		g = h;
	}
	clock.stop();
	prt(("std::string init + = took %d millisec\n", clock.elapsed() ));


	AbaxDataString  a1 = "0000000000003.45000000000";
	AbaxDataString  a2 = "3.0";
	AbaxDataString  a3 = "4";
	prt(("a1=[%s] a1len=%d a2=[%s] a3=[%s]\n", a1.c_str(), a1.size(), a2.c_str(), a3.c_str() ));
	a1.trimEndZeros();
	a2.trimEndZeros();
	a3.trimEndZeros();
	prt(("after trimend0 a1=[%s] a1len=%d a2=[%s] a3=[%s]\n", a1.c_str(), a1.size(), a2.c_str(), a3.c_str() ));

	AbaxDataString q1 = "all(ls )";
	AbaxDataString nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = "all(ls";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = "all( ls)";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = " ls)";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = "(";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = ")";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	q1 = "()";
	nm = q1.substr('(', ')');
	nm = trimChar( nm, ' ' );
	prt(("q1=[%s]  (*)=[%s]\n", q1.c_str(), nm.c_str() ));

	const char *see = "hi there ins it is me tom bay";
	prt(("see=[%s]\n", see ));
	const char *ps = jagstrrstr(see, "me" );
	if ( ps ) {
		prt(("me=[%s]\n", ps ));
	}

	ps = jagstrrstr(see, "mehtere" );
	if ( ps ) {
		prt(("mehtere=[%s]\n", ps ));
	}

	ps = jagstrrstr(see, "hi" );
	if ( ps ) {
		prt(("hi=[%s]\n", ps ));
	}

	AbaxDataString s3[3];
	s3[0] = "OJAG=0=test.pol2.ls=LS 0.0:0.0:500.0:600.0 ";
	prt(("before s3=[%s]\n", s3[0].c_str() ));
	char bf[234];
	sprintf(bf, " 500.0:600.0" );
	//s3 += bf; 
	s3[0]  += AbaxDataString(bf);
	prt(("after s3=[%s]\n", s3[0].c_str() ));

}

void test_geo( int N )
{
	JagClock clock;

	clock.start();
	bool rc;
	int cnt = 0;
	for ( int i=0; i < N; ++i ) {
		// 22.2, 232.2, 22.2, 32.3, 0.3
		// 29.2, 230.1, 84.5, 95.3, 0.6
		// rc = JagGeo::ellipseWithinEllipse( 22.2, 232.2, 22.2, 32.3, 0.3, 1.2, 3.1, 34.5, 55.3, 0.6, true );
		rc = JagGeo::ellipseWithinEllipse( 22.2, 232.2, 22.2, 32.3, 0.3,
											29.2, 230.1, 84.5, 95.3, 0.3,
											true );
		if ( rc ) { ++cnt; }
	}
	clock.stop();
	printf("test_geo %d computes ellipseWithinEllipse used %d millisecs true=%d\n", N, clock.elapsed(), cnt );

	clock.start();
	cnt = 0;
	for ( int i=0; i < N; ++i ) {
		// 22.2, 232.2, 22.2, 32.3, 0.3
		// 29.2, 230.1, 84.5, 95.3, 0.6
		// rc = JagGeo::ellipseWithinEllipse( 22.2, 232.2, 22.2, 32.3, 0.3, 1.2, 3.1, 34.5, 55.3, 0.6, true );
		rc = JagGeo::coneWithinCube_test( 12.3, 23.1, 32.1, 23.5, 43.1, 0.1, 0.2,
											13.3, 20.3, 30.2, 32.0, 0.3, 0.4, 
											true );
		if ( rc ) { ++cnt; }
	}
	clock.stop();
	printf("test_geo %d computes coneWithinCube_test used %d millisecs true=%d\n", N, clock.elapsed(), cnt );

	clock.start();
	cnt = 0;
	for ( int i=0; i < N; ++i ) {
		// 22.2, 232.2, 22.2, 32.3, 0.3
		// 29.2, 230.1, 84.5, 95.3, 0.6
		// rc = JagGeo::ellipseWithinEllipse( 22.2, 232.2, 22.2, 32.3, 0.3, 1.2, 3.1, 34.5, 55.3, 0.6, true );
		rc = JagGeo::coneWithinCube( 12.3, 23.1, 32.1, 23.5, 43.1, 0.1, 0.2,
											13.3, 20.3, 30.2, 32.0, 0.3, 0.4, 
											true );
		if ( rc ) { ++cnt; }
	}
	clock.stop();
	printf("test_geo %d computes coneWithinCube used %d millisecs true=%d\n", N, clock.elapsed(), cnt );


	clock.start();
	cnt = 0;
	JagSortPoint2D *points = new JagSortPoint2D[N];
	for ( int i=0; i < N; ++i ) {
		points[i].x1 = (rand() %10000) * 1.23;
		points[i].y1 = (rand() %10000) * 1.23;
		points[i].x2 = (rand() %100000) * 1.43;
		points[i].y2 = (rand() %100000) * 1.63;
		if ( (i%2) == 0 ) {
			points[i].end = JAG_LEFT;
		} else {
			points[i].end = JAG_RIGHT;
		}
		points[i].color = JAG_RED;
	}

	//JagGeo::_instantiate_sortLinePoints();

	int nrc = JagGeo::sortLinePoints( points, N );
	clock.stop();
	printf("test_geo %d computes sortIntersectLinePoints used %d millisecs nrc=%d\n", N, clock.elapsed(), nrc );
	delete [] points;

}

void test_sqrt( int N )
{
	JagClock clock;
	clock.start();
	bool rc;
	int cnt = 0;
	double f;
	for ( int i=0; i < N; ++i ) {
		f = sqrt( (double)N*23.3 );
	}
	clock.stop();
	printf("test_sqrt %d used %d millisecs \n", N, clock.elapsed() );
}

void writefloat( const char *str, abaxint len, FILE *outf )
{
	if ( NULL == str || '\0' == *str || len <1 ) return;

	bool leadzero = false;
	if ( str[0] == '0' && str[1] != '\0' ) leadzero = true;
	prt(("leadzero=%d\n", leadzero ));
	for ( int i = 0; i < len; ++i ) {
		if ( str[i] == '\0' ) continue;

		if ( str[i] != '0' ) leadzero = false;
		prt(("leadzero=%d\n", leadzero ));
		if ( ! leadzero ) {
			fputc( str[i], outf );
		}
	}
}

void test_split( int n )
{
	AbaxDataString s= "1234:93939";
	double x,y;
	char *p;
	const char *str;

	JagClock clock;
	clock.start();
	for ( int i=0; i < n; ++i ) {
		JagStrSplit sp(s, ':');
		x = jagatof(sp[0].c_str() );
		y = jagatof(sp[1].c_str() );
	}
	clock.stop();
	prt(("JagStrSplit %d used %d millisecs\n", n, clock.elapsed() ));

	clock.start();
	for ( int i=0; i < n; ++i ) {
		str = s.c_str();
		get2double( str, p, ':',  x, y );
		// prt(("x=%f y=%f\n", x, y ));
	}
	clock.stop();
	prt(("get2double %d used %d millisecs\n", n, clock.elapsed() ));
}

#if 0
void test_btree( int N )
{
	// google btree is slow in search
	std::vector<AbaxString> *vec = new std::vector<AbaxString>(N);
	typedef std::pair<AbaxString, AbaxString> Pair;

	//JagBtree<AbaxString, AbaxString> btree;
	btree::btree_map<AbaxString, AbaxString> btree;

	JagClock clock;
	clock.start();
	int cnt = 0;
	for  ( int i=0; i < N; ++i ) {
		//(*vec)[i] = AbaxString::randomValue(32);
		(*vec)[i] = intToStr(i).c_str();
		btree.insert( Pair( (*vec)[i], (*vec)[i]) );
	}
	clock.stop();
	prt(("btree insert %d took %d millisecs\n", N, clock.elapsed() ));

	clock.start();
	cnt = 0;
	for  ( int i=0; i < N; ++i ) {
		if ( btree.find( (*vec)[i] ) != btree.end() ) ++cnt;
	}
	clock.stop();
	prt(("btree exist found=%d took %d millisecs\n", cnt, clock.elapsed() ));


	clock.start();
	cnt = 0;
	const AbaxString *p;
	const AbaxString *pp;
	int cp = 0;
	btree::btree_map<AbaxString, AbaxString>::iterator it;
	btree::btree_map<AbaxString, AbaxString>::iterator itpred;
	btree::btree_map<AbaxString, AbaxString>::iterator itsucc;

	for  ( int i=0; i < N; ++i ) {
		it = btree.find( (*vec)[i] );
		if ( it == btree.end() ) continue; 
		++cp;
		if ( it->first == (*vec)[i] ) ++cnt;
		//prt(("i=%d key=[%s] *p=[%s]\n", i, (*vec)[i].c_str(), p->c_str() ));
	}
	clock.stop();
	prt(("btree getValue pointerOK=%d valuefound=%d took %d millisecs\n", cp, cnt, clock.elapsed() ));

	clock.start();
	cnt = 0;
	cp = 0;
	AbaxString succ, pred;
	int cnt1 = 0, cnt2 = 0;
	for  ( int i=0; i < N; ++i ) {
		//if ( btree.exist( (*vec)[i] ) ) ++cnt;
		it = btree.lower_bound( (*vec)[i] );
		if ( it == btree.end() ) continue;
		//prt((" lowerbound of %s is %s\n", (*vec)[i].c_str(), it->first.c_str() ));
		++cp;
		if ( it->first == (*vec)[i] ) {
			--it;
			if ( it == btree.begin() ) continue;
			if ( it == btree.end() ) continue;
			pred = it->first; 
			itpred = it;
			prt((" pred of %s is %s\n", (*vec)[i].c_str(), it->first.c_str() ));
			++it;
			++it;
			if ( it == btree.end() ) continue;
			succ = it->first;
			itsucc = it;
			prt((" succ of %s is %s\n", (*vec)[i].c_str(), it->first.c_str() ));

		} else {
			succ = it->first; 
			itsucc = it;
			prt((" succ of %s is %s\n", (*vec)[i].c_str(), it->first.c_str() ));
			--it;
			if ( it == btree.begin() ) continue;
			if ( it == btree.end() ) continue;
			pred = it->first;
			itpred = it;
			prt((" pred of %s is %s\n", (*vec)[i].c_str(), it->first.c_str() ));
		}

		if ( itpred != btree.end() && itpred != btree.begin() && itsucc != btree.end() ) {
			++itpred;
			if ( itpred !=  btree.end() && itpred->first == itsucc->first ) {
				++cnt1;
			}

			/***
			--itpred;
			--itsucc;
			if ( itsucc !=  btree.end() && itsucc->first == itpred->first && itpred != btree.begin() ) {
				++cnt2;
			}
			***/
		}

		if ( pred == succ ) ++cnt;
		// qwer
	}
	clock.stop();
	prt(("btree getPredSucc pointerOK=%d cnt1=%d cnt2=%d took %d millisecs\n", cp, cnt1, cnt2, clock.elapsed() ));


	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i=0; i < N; ++i ) {
		btree.erase( (*vec)[i] );
	}
	clock.stop();
	prt(("btree remove took %d millisecs\n", clock.elapsed() ));

	clock.start();
	cnt = 0;
	cp = 0;
	for  ( int i=0; i < N; ++i ) {
		if ( btree.find( (*vec)[i] ) != btree.end() ) ++cnt;
	}
	clock.stop();
	prt(("btree exist remaining %d took %d millisecs\n", cnt, clock.elapsed() ));

}
#endif

#if 0
void test_btree( int N )
{
	// delete removed do not work
	std::vector<AbaxString> *vec = new std::vector<AbaxString>(N);
	JagBtree<AbaxString, AbaxString> btree;
	JagClock clock;
	clock.start();
	int cnt = 0;
	for  ( int i=0; i < N; ++i ) {
		(*vec)[i] = AbaxString::randomValue(32);
		btree.addValue( (*vec)[i], (*vec)[i] );
	}
	clock.stop();
	prt(("btree insert %d took %d millisecs\n", N, clock.elapsed() ));

	clock.start();
	cnt = 0;
	for  ( int i=0; i < N; ++i ) {
		if ( btree.exist( (*vec)[i] ) ) ++cnt;
	}
	clock.stop();
	prt(("btree exist found=%d took %d millisecs\n", cnt, clock.elapsed() ));


	clock.start();
	cnt = 0;
	const AbaxString *p;
	const AbaxString *pp;
	int cp = 0;
	for  ( int i=0; i < N; ++i ) {
		//if ( btree.exist( (*vec)[i] ) ) ++cnt;
		p = btree.getValue( (*vec)[i] );
		if ( ! p ) continue;
		if ( p ) ++cp;
		if ( p && *p == (*vec)[i] ) ++cnt;
		//prt(("i=%d key=[%s] *p=[%s]\n", i, (*vec)[i].c_str(), p->c_str() ));
	}
	clock.stop();
	prt(("btree getValue pointerOK=%d valuefound=%d took %d millisecs\n", cp, cnt, clock.elapsed() ));

	//btree.print();

	clock.start();
	cnt = 0;
	cp = 0;
	for  ( int i=0; i < N; ++i ) {
		//if ( btree.exist( (*vec)[i] ) ) ++cnt;
		p = btree.getPred( (*vec)[i] );
		if ( ! p ) continue;
		++cp;
		//prt(("i=%d key=[%s] *p=[%s]\n", i, (*vec)[i].c_str(), p->c_str()  ));

		pp = btree.getSucc( *p );
		if ( ! pp ) continue;
		if ( *pp == (*vec)[i] ) ++cnt;
		else {
			prt(("error i=%d key=[%s] *p=[%s] *pp=[%s]\n", i, (*vec)[i].c_str(), p->c_str(), pp->c_str() ));
		}
	}
	clock.stop();
	prt(("btree getPredSucc pointerOK=%d valuefound=%d took %d millisecs\n", cp, cnt, clock.elapsed() ));


	clock.start();
	cnt = 0;
	cp = 0;
	for ( int i=0; i < N; ++i ) {
		if ( btree.remove( (*vec)[i] ) ) ++cnt;
	}
	clock.stop();
	prt(("btree remove %d OK took %d millisecs\n", cnt, clock.elapsed() ));

	clock.start();
	cnt = 0;
	cp = 0;
	for  ( int i=0; i < N; ++i ) {
		if ( btree.exist( (*vec)[i] ) ) ++cnt;
	}
	clock.stop();
	prt(("btree exist %d took %d millisecs\n", cnt, clock.elapsed() ));

}
#endif


void test_json( int N ) 
{
	// write
	using namespace rapidjson;
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();   
	writer.Key("key1");  
	writer.String("val1");
	writer.Key("key2");  
	writer.String("val2");
	writer.Key("x1");  
	writer.Double(12.42);
	writer.Key("Null1");  
	writer.Null();
	writer.Key("y1");  
	writer.Double(19.42);
	writer.Double( jagatof("23.434") );
	writer.Key("Null2");  
	writer.Null();

	writer.Key("points");  
	writer.StartArray();   
	for (unsigned i = 0; i < 4; i++) {
		writer.Double(i);
	}
	writer.EndArray();   
	//writer.EndObject();   

    writer.Key("geometry");
	writer.StartObject();   
    writer.Key("type");
    writer.String("point");             // follow by a value.
    writer.Key("cooirdinates");
    writer.StartArray();                // Between StartArray()/EndArray(),
    for (unsigned i = 0; i < 4; i++)
        writer.Uint(i);                 // all values are elements of the array.
    writer.EndArray();
	writer.EndObject();   


	//cout << s.GetString() << endl;
	prt(("json output string: [%s]\n", s.GetString() ));

	rapidjson::Document dom;
	for ( int i=0; i < N; ++i ) {
		if ( dom.Size() > 0 ) {
			dom.RemoveAllMembers( );
		}
		dom.Parse( s.GetString() );
	}

	const Value& n1 = dom["Null1"];
	if ( n1.IsNull() ) {
		prt(("Null1 is null\n" ));
	}

	const Value& n2 = dom["LEN123"];
	if ( n2.IsNull() ) {
		prt(("LEN123 is null\n" ));
	}

	AbaxDataString obj="OJAG=0=test.pol2.po2";
	JagStrSplit sp(obj, '=');

	//const char *sstr = "0.0:0.0:500.0:600.0 0.0:0.0 20.0:0.0 8.0:9.0 0.0:0.0| 1.0:2.0 2.0:3.0 1.0:2.0";
	//char *str = strdup( sstr );

	const char *str = "0.0:0.0:500.0:600.0 0.0:0.0 20.0:0.0 8.0:9.0 0.0:0.0| 1.0:2.0 2.0:3.0 1.0:2.0|2:3 9:0 2:4 20:399";

	//AbaxDataString json= JagGeo::makeJsonPolygon("Polygon", sp, str, true );
	AbaxDataString json= JagGeo::makeJsonPolygon("Polygon", sp, str, false );
	prt(("json=[%s]\n", json.c_str() ));

	const char *js = "{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[\"20.0\",\"0.0\"],[\"8.0\",\"9.0\"],[\"0.0\",\"0.0\"]],[[\"1.0\",\"2.0\"],[\"2.0\",\"3.0\"],[\"1.0\",\"2.0\"]],[[\"2\",\"3\"],[\"9\",\"0\"],[\"2\",\"4\"],[\"20\",\"399\"]]]}";

	prt(("js=[%s]\n", js ));
	rapidjson::Document d1;
	d1.Parse( js );
	if ( d1.HasParseError() ) {
		prt(("parse error\n" ));
		ParseErrorCode errcode = d1.GetParseError(); 
		const char *err = GetParseError_En( errcode );
		prt(("error=[%s]\n", err ));
	}



	const Value& tp = d1["type"];
	prt(("tp=[%s]\n", tp.GetString() ));

	const Value& cd = d1["coordinates"];
	prt(("cd=[%s]\n", cd.GetString() ));
	if ( ! cd.IsArray() ) {
		prt(("error, coordinates is not array\n" ));
	}

	for (SizeType i = 0; i < cd.Size(); i++) {
		if (  cd[i].IsDouble() ) {
			prt(("i=%d %f\n", i, cd[i].GetDouble() ));
		} else if ( cd[i].IsArray() ) {
			const Value &arr = cd[i];
			for (SizeType j = 0; j < arr.Size(); j++) {
				if ( arr[j].IsDouble() ) {
					prt(("j=%d %f\n", j, arr[j].GetDouble() ));
				} else if ( arr[j].IsArray() ) {
					const Value &arr2 = arr[j];
					for (SizeType k = 0; k < arr2.Size(); k++) {
						if ( arr2[k].IsNumber() ) {
							prt(("number k=%d %f\n", k, arr2[k].GetDouble() ));
							//prt(("number->string k=%s %f\n", k, arr2[k].GetString() ));
						} else if ( arr2[k].IsString() ) {
							prt(("string k=%d %s\n", k, arr2[k].GetString() ));
						} else {
							prt(("no double i=%d j=%d k=%d\n", i, j, k ));
						}
					}
				}
			}
		}
	}

}

void test_linefile( int n )
{
	JagLineFile ff;
	ff.append("1111" );
	ff.append("2111" );
	ff.append("3111" );
	ff.append("4111" );
	ff.append("5111" );
	ff.append("6111" );
	ff.append("7111" );

	ff.startRead();

	int cnt = 0;
	AbaxDataString line;
	while ( ff.getLine( line ) ) {
		prt(("line=[%s]\n", line.c_str() ));
		++ cnt;
		if ( cnt > 100 ) break;
	}
}

void test_distance()
{
	double lata1, lona1, lata2, lona2, latb1, lonb1;
	lata1 = 52.10000;
	lona1 = 5.50000;

	lata2 = 52.26;
	lona2 = 5.450000;

	latb1 = 52.2;
	lonb1 = 5.39;

	double dist = JagGeo::pointToLineDistance( lata1, lona1, lata2, lona2, latb1, lonb1 );
	prt(("dist=%.3f meters\n", dist ));
}


#if 1
typedef CGAL::Exact_predicates_inexact_constructions_kernel CGALKernel;
typedef CGALKernel::Point_2 Point_2;
typedef std::vector<Point_2> Points;

typedef CGAL::Simple_cartesian<double> CartKernel;
typedef CartKernel::Point_2 Point2;
typedef CartKernel::Segment_2 Segment2;
void test_cgal()
{
    // convex_hull_2
    Point_2 points[5] = { Point_2(0,0), Point_2(10,0), Point_2(10,10), Point_2(6,5), Point_2(4,1) };
    Point_2 result[5];
    Point_2 *ptr = CGAL::convex_hull_2( points, points+5, result );
    int len  = ptr - result;
    std::cout <<  len << " points on the convex hull:" << std::endl;
    for(int i = 0; i < len; i++){
      std::cout << result[i] << std::endl;
    }

    // convex_hull_2 of vector of points
    Points vpoints, vresult;
    vpoints.push_back(Point_2(0,0));
    vpoints.push_back(Point_2(10,0));
    vpoints.push_back(Point_2(10,10));
    vpoints.push_back(Point_2(6,5));
    vpoints.push_back(Point_2(2,1));
    vpoints.push_back(Point_2(4,8));
    CGAL::convex_hull_2( vpoints.begin(), vpoints.end(), std::back_inserter(vresult) );
    std::cout << vresult.size() << " points on the convex hull:" << std::endl;
    for(int i = 0; i < vresult.size(); i++){
      std::cout << vresult[i] << std::endl;
      std::cout << "x: " << vresult[i].x() << std::endl;
      std::cout << "y: " << vresult[i].y() << std::endl;
    }



    /////// orientation ///////////////
	/***
    Point2 p(1,1), q(10,10);
    std::cout << "p = " << p << std::endl;
    std::cout << "q = " << q.x() << " " << q.y() << std::endl;
    std::cout << "sqdist(p,q) = " << CGAL::squared_distance(p,q) << std::endl;

    Segment2 s(p,q);
    Point2 m(5, 9);
    std::cout << "m = " << m << std::endl;
    std::cout << "sqdist(Segment2(p,q), m) = " << CGAL::squared_distance(s,m) << std::endl;

    std::cout << "p, q, and m ";
    switch (CGAL::orientation(p,q,m))
    {
      case CGAL::COLLINEAR:
          std::cout << "are collinear\n";
          break;
      case CGAL::LEFT_TURN:
          std::cout << "make a left turn\n";
          break;
      case CGAL::RIGHT_TURN:
          std::cout << "make a right turn\n";
          break;
    }
	***/
}
#endif

class TestN
{
  public:
     TestN() { p = new char[100*1000]; }
     TestN( long n) { p = new char[n*1000]; }
	 ~TestN() { delete p; }
	 char *p;
};

void test_new( long n )
{
	TestN *t = newObject<TestN>( true );
	prt(("t=%0x n=%d\n", t, n ));
}

