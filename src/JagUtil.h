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
#ifndef _jag_string_util_h_ 
#define _jag_string_util_h_ 

#include <pthread.h>
#include <stdarg.h>
#include <abax.h>
#include <JagNet.h>
#include <JagDef.h>
#include <JagClock.h>
#include <JagTableUtil.h>
#include <JaguarCPPClient.h>
#include <time.h>


#ifdef JAGPRT
#define prt(x) printf x; fflush(stdout)
#else
#define prt(x) 1  
#endif


#ifdef _WINDOWS64_
typedef double abaxdouble;
#else
typedef long double abaxdouble;
// typedef double abaxdouble;
#endif

class JDFS;
class OtherAttribute;
class JagParseParam;
template <class K> class JagVector;
template <class K, class V> class JagHashMap;
class JagStrSplit;

int  str_str(const char *longstr, const char *shortstr );
int  str_str_ch(const char *longstr, char ch, const char *shortstr );
int  str_print( const char *ptr, int len );

char * jag_strtok_r(char *s, const char *delim, char **lasts );
char * jag_strtok_r_bracket(char *s, const char *delim, char **lasts );
const char *strrchrWithQuote(const char *str, int c, bool processParenthese=true);
short memreversecmp( const char *buf1, const char *buf2, int len );
int reversestrlen( const char *str, int maxlen );
// char * jag_strtok_r(char *s, const char *delim, char **lasts, char *lastc );

void raydebug( FILE *outf, int level, const char *fmt, ... );
const char *jagstrrstr(const char *haystack, const char *needle);

// pread/pwrite style, but safe
ssize_t raysafepread( int fd, char *buf, abaxint len, abaxint startpos );
ssize_t raysafepwrite( int fd, const char *buf, abaxint len, abaxint startpos );

ssize_t jagpwrite( int fd, const char *buf, abaxint len, abaxint startpos );
ssize_t jagpread( int fd, char *buf, abaxint len, abaxint startpos );

// fd read/write style, but safe
ssize_t raysaferead( int fd, char *buf, abaxint len );
ssize_t raysaferead( int fd, unsigned char *buf, abaxint len );
ssize_t raysafewrite( int fd, const char *buf, abaxint len );

ssize_t jdfpread( const JDFS *jdfs, char *buf, abaxint len, abaxint startpos );
ssize_t jdfpwrite( JDFS *jdfs, const char *buf, abaxint len, abaxint startpos );

short getSimpleEscapeSequenceIndex( const char p );
int rayatoi( const char *buf, int length );
abaxint rayatol( const char *buf, int length );
double rayatof( const char *buf, int length );
abaxdouble raystrtold( const char *buf, int length );

char *itostr( int i, char *buf );
char *ltostr( abaxint i, char *buf );
AbaxDataString intToStr( int i );
AbaxDataString longToStr( abaxint i );
AbaxDataString doubleToStr( double f );
AbaxDataString d2s( double f );
AbaxDataString doubleToStr( double f, int maxlen, int sig );
AbaxDataString longDoubleToStr( abaxdouble f );
int jagsprintfLongDouble( int mode, bool fill, char *buf, abaxdouble i, abaxint maxlen );
abaxint getNearestBlockMultiple( abaxint value );
abaxint getBuffReaderWriterMemorySize( abaxint value ); // max as 1024 ( MB ), value and return in MB

bool formatOneCol( int tzdiff, int servtzdiff, char *outbuf, const char *inbuf, AbaxDataString &errmsg, const AbaxDataString &name, 
	const int offset, const int length, const int sig, const AbaxDataString &type );

void dbNaturalFormatExchange( char *buffer, int numKeys, const JagSchemaAttribute *attrs=NULL, 
	int offset=0, int length=0, const AbaxDataString &type=" " );

void dbNaturalFormatExchange( char *buffers[], int num, int numKeys[], const JagSchemaAttribute *attrs[] );
void rwnegConvertion( char *outbuf, int checkNum, const JagSchemaAttribute *schAttr, 
	int offset, int length, const AbaxDataString &type );

AbaxDataString filePathFromFD( int fd );
AbaxDataString makeUpperString( const AbaxDataString &str );
AbaxDataString makeLowerString( const AbaxDataString &str );
AbaxDataString removeCharFromString( const AbaxDataString &str, char dropc );
AbaxFixString makeUpperOrLowerFixString( const AbaxFixString &str, bool isUpper );
AbaxDataString trimChar( const AbaxDataString &str, char c );
AbaxDataString trimHeadChar( const AbaxDataString &str, char c );
AbaxDataString trimTailChar( const AbaxDataString &str, char c='\n' );
AbaxDataString trimTailLF( const AbaxDataString &str ); 
AbaxDataString strRemoveQuote( const char *p );
bool beginWith( const AbaxDataString &str, char c );
bool beginWith( const AbaxString &str, char c );
bool endWith( const AbaxDataString &str, char c );
bool endWithStr( const AbaxDataString &str, const AbaxDataString &end );
bool endWhiteWith( const AbaxDataString &str, char c );
bool endWith( const AbaxString &str, char c );
bool endWhiteWith( const AbaxString &str, char c );
bool startWith(  const AbaxDataString &str, char a );
char *jumptoEndQuote(const char *p);
char *jumptoEndBracket(const char *p);
int stripStrEnd( char *msg, int len );
void randDataStringSort( AbaxDataString *vec, int maxlen );

void fsetXattr( int fd, const char* path, const char *name, abaxint value );
abaxint fgetXattr( int fd, const char *path, const char *name );

FILE *loopOpen( const char *path, const char *mode );
FILE *jagfopen( const char *path, const char *mode );
int jagfclose( FILE *fp );
int jagopen( const char *path, int flags );
int jagopen( const char *path, int flags, mode_t mode );
int jagclose( int fd );
int jagaccess( const char *path, int mode );
int jagunlink( const char *path );
int jagrename( const char *path, const char *newpath );
#ifdef _WINDOWS64_
int jagftruncate( int fd, __int64 size );
const char *strcasestr(const char *s1, const char *s2);
#else
int jagftruncate( int fd, off_t length );
#endif
void jagsleep( abaxint time, int mode );
bool lastStrEqual( const char *bigstr, const char *smallstr, int lenbig, int lensmall );
bool isInteger( const AbaxDataString &dtype );
bool isDateTime( const AbaxDataString &dtype );
bool isJoin( int dtype );
AbaxDataString intToString( int i ) ;
AbaxDataString longToString( abaxint i ) ;
AbaxDataString ulongToString( uabaxint i ) ;
abaxint  strchrnum( const char *str, char ch );
char *strnchr(const char *s, int c, int n);
int  strInStr( const char *str, int len, const char *str2 );
void splitFilePath( const char *fpath, AbaxDataString &first, AbaxDataString &last );
void stripeFilePath( const AbaxDataString &fpath, abaxint stripe, AbaxDataString &stripePath );
AbaxDataString makeDBObjName( JAGSOCK sock, const AbaxDataString &dbname, const AbaxDataString &objname );
AbaxDataString makeDBObjName( JAGSOCK sock, const AbaxDataString &dbdotname );
AbaxDataString jaguarHome();
AbaxDataString renameFilePath( const AbaxDataString& fpath, const AbaxDataString &newLast );
int selectServer( const AbaxFixString &min, const AbaxFixString &max, const AbaxFixString &inkey );
int trimEndWithChar ( char *msg, int len, char c );
int trimEndToChar ( char *msg, int len, char stopc );
int trimEndWithCharKeepNewline ( char *msg, int len, char c );
abaxint availableMemory( abaxint &callCount, abaxint lastBytes );
int checkReadOrWriteCommand( const char *pmesg );
int checkColumnTypeMode( const AbaxDataString &type );
AbaxDataString formOneColumnNaturalData( const char *buf, abaxint offset, abaxint length, const AbaxDataString &type );
void printParseParam( JagParseParam *parseParam );
int rearrangeHdr( int num, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], JagParseParam *parseParam,
	const JagVector<SetHdrAttr> &spa, AbaxDataString &newhdr, AbaxDataString &gbvhdr,
	abaxint &finalsendlen, abaxint &gbvsendlen, bool needGbvs=true );
int checkGroupByValidation( const JagParseParam *parseParam );
int checkAndReplaceGroupByAlias( JagParseParam *parseParam );
void convertToHashMap( const AbaxDataString &kvstr, char sep, JagHashMap<AbaxString, AbaxString> &hashmap );
void changeHome( AbaxDataString &fpath );
int jaguar_mutex_lock(pthread_mutex_t *mutex);
int jaguar_mutex_unlock(pthread_mutex_t *mutex);
int jaguar_cond_broadcast( pthread_cond_t *cond);
int jaguar_cond_signal( pthread_cond_t *cond);
int jaguar_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
int jaguar_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, const struct timespec *abstime);
int getPassword( AbaxDataString &outPassword );
void getWinPass( char *pass );
const char *strcasestrskipquote( const char *str, const char *token );
const char *strcasestrskipspacequote( const char *str, const char *token );
void trimLenEndColonWhite( char *str, int len );
void trimEndColonWhite( char *str, int len );
void escapeNewline( const AbaxDataString &instr, AbaxDataString &outstr );
char *jagmalloc( abaxint sz );
int jagpthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
int jagpthread_join(pthread_t thread, void **retval);
abaxint sendData( JAGSOCK sock, const char *buf, abaxint len );
abaxint recvData( JAGSOCK sock, char *hdr, char *&buf );
abaxint recvData( JAGSOCK sock, char *buf, abaxint len );
abaxint _raysend( JAGSOCK sock, const char *hdr, abaxint N );
abaxint _rayrecv( JAGSOCK sock, char *hdr, abaxint N );

#ifdef DEVDEBUG
#define dbg(x) printf x; fflush(stdout)
#else
#define dbg(x) ;
#endif

int jagmkdir(const char *path, mode_t mode);
int jagfdatasync( int fd ); 
int jagsync( ); 
int jagfsync( int fd ); 
abaxint jagsendfile( JAGSOCK sock, int fd, abaxint size );

#ifdef DEVDEBUG
#define JAG_BLURT \
  raydebug( stdout, JAG_LOG_HIGH, "BLURT pid=%d file=%s func=%s line=%d\n", getpid(),  __FILE__, __func__, __LINE__ );

#define JAG_BLURT_UN \
  raydebug( stdout, JAG_LOG_HIGH, "BLURT_UN pid=%d file=%s func=%s line=%d\n", getpid(), __FILE__, __func__, __LINE__ );

#define JAG_OVER \
  raydebug( stdout, JAG_LOG_HIGH, "OVER pid=%d file=%s func=%s line=%d\n", getpid(), __FILE__, __func__, __LINE__ );

#else
#define JAG_BLURT ;
#define JAG_OVER ;
#define JAG_BLURT_UN ;
#endif

#ifndef _WINDOWS64_
#include <sys/sysinfo.h>
#endif

int jagmalloc_trim( abaxint n );
AbaxDataString psystem( const char *cmd );
bool checkCmdTimeout( abaxint startTime, abaxint timeoutLimit );
char *getNameValueFromStr( const char *content, const char *name );
ssize_t jaggetline(char **lineptr, size_t *n, FILE *stream);
AbaxDataString expandEnvPath( const AbaxDataString &path );
struct tm *jag_localtime_r(const time_t *timep, struct tm *result);
char *jag_ctime_r(const time_t *timep, char *result);
int formatInsertSelectCmdHeader( const JagParseParam *parseParam, AbaxDataString &str );
bool isValidVar( const char *name );
bool isValidNameChar( char c );
void stripEndSpace( char *qstr, char endc );
abaxint _getFieldInt( const char * rowstr, char fieldToken );
void makeMapFromOpt( const char *options, JagHashMap<AbaxString, AbaxString> &omap );
AbaxDataString makeStringFromOneVec( const JagVector<AbaxDataString> &vec, int dquote );
AbaxDataString makeStringFromTwoVec( const JagVector<AbaxDataString> &xvec, const JagVector<AbaxDataString> &yvec );
int oneFileSender( JAGSOCK sock, const AbaxDataString &inpath );
int oneFileReceiver( JAGSOCK sock, const AbaxDataString &outpath, bool isDirPath=true );
abaxint sendDirectToSock( JAGSOCK sock, const char *mesg, abaxint len, bool nohdr=false );
abaxint recvDirectFromSock( JAGSOCK sock, char *&buf, char *hdr );
abaxint sendDirectToSockWithHdr( JAGSOCK sock, const char *hdr, const char *mesg, abaxint len );
int isValidSciNotation(const char *str );
AbaxDataString fileHashDir( const AbaxFixString &fstr );
char lastChar( const AbaxFixString &str );

#define jagfree(x) free(x); x=NULL
#define jagiffree(x) if (x) {free(x); x=NULL;}
#define jagdelete(x) delete x; x=NULL 

void jagfwrite( const char *str, abaxint len, FILE *outf );
void jagfwritefloat( const char *str, abaxint len, FILE *outf );
void charFromStr( char *dest, const AbaxDataString &src );
bool streq( const char *s1, const char *s2 );
long long jagatoll(const char *nptr);
long jagatol(const char *nptr);
long double jagstrtold(const char *nptr, char **endptr=NULL);
double jagatof(const char *nptr );
double jagatof(const AbaxDataString &str );
int jagatoi(const char *nptr );
int jagatoi(char *nptr, int len );
void appendOther( JagVector<OtherAttribute> &otherVec,  int n, bool isSub=true);
void stripTailZeros( char *buf, int len );
bool jagisspace( char c);
AbaxDataString trimEndZeros( const AbaxDataString& str );
bool likeMatch( const AbaxDataString& str, const AbaxDataString& like );
int levenshtein(const char *s1, const char *s2);

#define get2double( str, p, sep, d1, d2 ) \
    p=(char*)str;\
	while (*p != sep ) ++p; \
	*p = '\0';\
	d1 = jagatof(str);\
	*p = sep;\
	++p;\
	d2 = jagatof(p)

#define get3double( str, p, sep, d1, d2, d3 ) \
    p=(char*)str;\
	while (*p != sep ) ++p; \
	*p = '\0';\
	d1 = jagatof(str);\
	*p = sep;\
	++p;\
	str = p;\
	while (*p != sep ) ++p; \
	*p = '\0';\
	d2 = jagatof(str);\
	*p = sep;\
	++p;\
	d3 = jagatof(p)

void dumpmem( const char *buf, int len );

const char *KMPstrstr(const char *text, const char *pat);
void prepareKMP(const char *pat, int M, int *lps);

AbaxDataString replaceChar( const AbaxDataString& str, char oldc, char newc );
void printStr( const AbaxDataString &str );
char *secondTokenStart( const char *str, char sep=' ' );
abaxint convertToSecond( const char *str);
abaxint convertToMicroSecond( const char *str);

template <class T> T* newObject( bool doPrint = true )
{
	T* o = NULL;
	bool rc = true;
	while ( 1 ) {
		rc = true;
		try { 
   			o  =  new T();
		} catch ( std::bad_alloc &ex ) {
			 if ( doPrint ) {
			 	prt(("s4136 ******** new oject error [%s], retry ...\n", ex.what() ));
			 }
			 o = NULL;
			 rc = false;
		} catch ( ... ) {
			 if ( doPrint ) {
			 	prt(("s4138 ******** new oject error, retry ...\n" ));
			 }
			 o = NULL;
			 rc = false;
		}

		if ( rc ) break;
		sleep(5);
	}

	return o;
}

template <class T> T* newObjectArg( bool doPrint, ... )
{
	T* o = NULL;
	va_list args;
	va_start(args, doPrint);
	bool rc = true;
	while ( 1 ) {
		rc = true;
		try { 
   			//o  =  new (std::nothrow) T( args );
   			o  =  new T( args );
		} catch ( std::bad_alloc &ex ) {
			 if ( doPrint ) {
			 	prt(("s4236 ******** new oject error [%s], retry ...\n", ex.what() ));
			 }
			 o = NULL;
			 rc = false;
		} catch ( ... ) {
			 if ( doPrint ) {
			 	prt(("s4238 ******** new oject error, retry ...\n" ));
			 }
			 o = NULL;
			 rc = false;
		}

		if ( rc ) break;
		sleep(5);
	}

	va_end( args);
	return o;
}


#endif
