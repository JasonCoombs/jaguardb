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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <signal.h>
#include <dirent.h>
#include <math.h>
#include <malloc.h>

#ifndef _WINDOWS64_
#include <termios.h>
#else
#include <direct.h>
#include <io.h>
#include <conio.h>
#endif

#include <abax.h>
#include <JagLogLevel.h>
#include <JDFS.h>
#include <JagStrSplit.h>
#include <JagParser.h>
#include <JagParseParam.h>
#include <JagVector.h>
#include <JagHashMap.h>
#include <JagTime.h>
#include <JagNet.h>
#include <JagSchemaRecord.h>
#include <JagFastCompress.h>
#include <JagUtil.h>


/********************************************************************************
** Internal helper function
**
** str_match function matches string bigstr with smallstr up to the length of 
** smallstr from the beginning of the two strings
********************************************************************************/
static int str_match_ch( const char *s1, char ch, const char *s2, size_t *step);

/*******************************************************
same as above except terminator in str1 is ch
*******************************************************/
int str_str_ch(const char *str1, char ch, const char *str2 )
{
    const char  *p1 = NULL; 
    size_t      index = -1, step =1;
    int         diff = 1;

    if ( NULL == str2 || *str2 == '\0' ) { return 0; }  // special case
	if ( *str1 == ch ) { return -1; }


    const char *start = str1;
    while(1)
    {
        for ( p1=start; (*p1 != '\0' && *p1 != ch ) && *p1 != *str2; p1++ );
        if ( *p1 == '\0') break;
        if ( *p1 == ch ) break;

        if ( ( diff = str_match_ch (p1, ch, str2, &step ) ) > 0 ) 
        {
            start = p1 + step; // jump to new location
        }
        else
        {
            if ( diff == 0 ) { index = p1 - str1; }
            break;
        }
    }

    return index;
}


/********************************************************************************
Same as str_match except terminater char in s1 is ch
********************************************************************************/
int str_match_ch( const char *s1, char ch, const char *s2, size_t *step)
{
    const char *p, *q;

    for (p = s1+1, q=s2+1; (*p != ch && *p != '\0') && *q != '\0' && *p == *q; p++, q++ );

    if ( *q == '\0' ) { return 0; }
    if ( *p == '\0' || *p == ch ) { return  -1; }

    *step = q - s2;
    return 1;
}

int str_print( const char *ptr, int len )
{
	for(int i = 0; i < len; i++) {
		if (ptr[i] == '\0') {
			printf("@");
		}
		else {
			printf("%c", ptr[i]);
		}
	}
	printf("\n");
	fflush(stdout);
}

// jag_strtok_r, get next token ending with one of delim; ignore strings in '', "" and ``
char * jag_strtok_r(char *s, const char *delim, char **lasts)
{
	const char *spanp; char *tok, *r;
	int c, sc;

	// if input is NULL, change input position to lasts
	if ( s == NULL ) {
		s = *lasts;
		// if lasts is NULL, return NULL
		if ( s == NULL ) {
			return NULL;
		}
	}
	// printf("begin s=[%s]\n", s );
	
	// first, skip ( span ) any leading delimiters(s += strspn(s, delim), sort of)
	cont:
	c = *s++;   // c in "jkfdjkfjd,fjdkfjdk='fjdkfjkdfd'  fjdkf d
	for (spanp = delim; (sc = *spanp++) != 0;) {   // sc: "delimieter , "
		if (c == sc && sc != '\'' && sc != '"' && sc != '`' ) {
			goto cont;
		}
	}	
	if (c == 0) {           /* no non-delimiter characters */
		*lasts = NULL;
		return NULL;
	}
	// then, put back s to the starting position
	tok = --s;
		
	// Scan token (scan for delimiters: s += strcspn(s, delim), sort of).
	// Note that delim must have one NULL; we stop if we see that, too.
	// printf("scan token s=[%s]\n", s );
	for (;;) {
		r = s++;
		if ( *r == '\0' ) break;
		// see any quote, scan to enclosing single quote or till end
		if ( *r == '\'' || *r == '"' || *r == '`' ) {
			r = jumptoEndQuote( r );
			if ( *r == '\0' ) {
				*lasts = r;
				return tok;
			} else {
				s = ++r;
			}
			continue;
		}
		c = *r;
		
		for (spanp = delim; (sc = *spanp++) != 0;) {
			if (c == sc && sc != '\'' && sc != '"' && sc != '`' ) {
				if (c == 0) {
					s = NULL;
				} else {
					s[-1] = 0;
				}
				*lasts = s;
				// printf("reg sep tok=[%s]  s=[%s]\n", tok, s );
				return tok;
			}
		}
	}
	*lasts = r;
	return tok;
}


// jag_strtok_r, get next token ending with one of delim; ignore strings in '', "" and ``
char * jag_strtok_r_bracket(char *s, const char *delim, char **lasts )
{
	if ( s == NULL ) {
		s = *lasts;
		if ( *s == '\0' ) {
			//prt(("111 NULL\n" ));
			return NULL;
		}
	} else {
		*lasts = NULL;
	}

	while ( strchr(delim, *s) ) { ++s; }
	if ( *s == '\0' ) {
		*lasts = s;
		//prt(("112 NULL\n" ));
		return NULL;
	}

	char *r = s;
	if (*s == '(' ) {
		while ( *s != ')' ) ++s;
		if ( *s == '\0' ) {
			*lasts = s;
			return r;
		}
		++s;
	}

	//prt(("s6001 r=[%s] s=[%s] lasts=[%s]\n", r, s, *lasts ));

	while ( *s ) {
		if ( *s == '(' ) {
			while ( *s != ')' ) ++s;
			if ( *s == '\0' ) {
				*lasts = s;
				return r;
			}
		}
		
		if ( strchr(delim, *s) ) {
				*s = '\0';
				*lasts = s+1;
				//prt(("s6002 r=[%s] lasts=[%s]\n", r, *lasts ));
				return r;
		} else if ( *s == '"' || *s == '\'' ) {
			s = jumptoEndQuote( s );
			if ( *s == '\0' ) {
				*lasts = s;
				//prt(("s6003 r=[%s] lasts=[%s]\n", r, *lasts ));
				return r;
			} 
		}

		++s;
	}
	
	//prt(("s6003 r=[%s] s=[%s]\n", r, s ));
	if ( *s == '\0' ) {
		*lasts = s;
	} else {
		*lasts = s+1;
	}
	//prt(("s6004 r=[%s] lasts=[%s]\n", r, *lasts ));
	return r;
}


short memreversecmp( const char *buf1, const char *buf2, int len )
{
	for ( int i = len-1; i >= 0; --i ) {
		if ( *(buf1+i) != *(buf2+i) ) return *(buf1+i) - *(buf2+i);	
	}
	return 0;
}

int reversestrlen( const char *str, int maxlen )
{
	if ( 1 == maxlen ) return 1;

	prt(("u4401 reversestrlen  str=[%s] maxlen=[%d]\n", str, maxlen ));
	const char *p = str+maxlen-1;
	while( *p == '\0' && p-str > 0 ) --p;
	if ( p-str == 0 ) return 0;
	else return p-str+1;
}

const char *strrchrWithQuote(const char *str, int c, bool processParenthese) 
{
	const char *result = NULL;
	const char *p = str;
	int parencnt = 0;

	while ( 1 ) {
		if ( *p == '\0' ) break;
		else if ( *p == '\'' && (p = jumptoEndQuote(p)) && *p != '\0' ) ++p;
		else if ( *p == '"' && (p = jumptoEndQuote(p)) && *p != '\0' ) ++p;
		else if ( *p == '(' && processParenthese ) {
			++parencnt;
			++p;
			while ( 1 ) {
				if ( *p == '\0' ) break;
				else if ( *p == ')' ) {
					--parencnt;
					if ( !parencnt ) break;
					else ++p;
				} else if ( *p == '\'' && (p = jumptoEndQuote(p)) && *p != '\0' ) ++p;
				else if ( *p == '"' && (p = jumptoEndQuote(p)) && *p != '\0' ) ++p;
				else if ( *p == '(' ) {
					++parencnt;
					++p;
				} else if ( *p != ')' && *p != '\0' ) ++p;
			}
			if ( *p == ')' ) ++p;			
		} else if ( *p == c ) { 
			result = p; 
			++p; 
		} else ++p;
	}
	return result;
}

// method to jump to the end quote with the separator passed as the first byte of buffer parameter
// returned *q must be the same as *p ( true ) or '\0' ( false ) 
// p = 'adofia'
// p = "apiodf"
char *jumptoEndQuote(const char *p) 
{
	char *q = (char*)p + 1;
	while( 1 ) {
		if ( *q == '\0' || (*q == *p && *(q-1) != '\\') ) break;
		++q;
	}
	return q;
}

// Method to jump to the end bracket with the separator passed as the first byte 
// returned *q must be the same as *p ( true ) or '\0' ( false ) 
// p = (adofia)
// if not found end ), return *q='\0'
char *jumptoEndBracket(const char *p) 
{
	char *q = (char*)p + 1;
	while( 1 ) {
		if ( *q == '\0' || *q == ')' ) break;
		++q;
	}
	return q;
}

// method to remove quote from given string; e.g. 'apple' -> apple ; "apple" -> apple ;
// instr beginning with quote which you want to remove; e.g. ' or "
AbaxDataString strRemoveQuote( const char *p )
{
	AbaxDataString str = p;
	if ( p == NULL || *p == '\0' ) {
	} else if ( *p != '\'' && *p != '"' && *p != '`' ) {
		str = p;
	} else {
		char *q = jumptoEndQuote( p ); ++p;
		if ( *p == '\0' || *q == '\0' || q-p <= 0 ) {
		} else {
			str = AbaxDataString( p, q-p );
		}
	}
	return str;
}	

//static char *itostr( int i, char *buf );
//static char *ltostr( abaxint i, char *buf );
void raydebug( FILE *outf, int level, const char *fmt, ... )
{
    const char *p;
    va_list 	argp;
    abaxint 		i;
    char 		*s;
    char 		fmtbuf[256];
	char        tstr[22];

	if ( JAG_LOG_LEVEL < 1 ) JAG_LOG_LEVEL = 1;

	if ( level > JAG_LOG_LEVEL ) {
		// printf("raydebug level=%d  > JAG_LOG_LEVEL=%d  skip logging\n", level, JAG_LOG_LEVEL );
		// fflush( stdout );
		return;
	}

	memset(tstr, 0, 22);
	time_t  now = time(NULL);
	struct tm  *tmp;
	struct tm  result;
	// tmp = localtime( &now );
	tmp = jag_localtime_r( &now, &result );
	strftime( tstr, 22, "%Y-%m-%d %H:%M:%S", tmp );
	fprintf( outf, "%s %lld ", tstr, pthread_self() ); 
    
    va_start(argp, fmt);
    
    for(p = fmt; *p != '\0'; p++) {
    	if(*p != '%') {
    		fputc(*p, outf);
    		continue;
   		}
    
    	switch(*++p) {
    		case 'c':
    			i = va_arg(argp, int);
    			fputc(i, outf);
    			break;
    
    		case 'd':
    			i = va_arg(argp, int);
    			s = itostr(i, fmtbuf );
    			fputs(s, outf);
    			break;
    		case 'l':
    			i = va_arg(argp, abaxint);
    			s = ltostr(i, fmtbuf );
    			fputs(s, outf);
    			break;
    		case 'u':
    			i = va_arg(argp, abaxint);
    			s = itostr(i, fmtbuf );
    			fputs(s, outf);
    			break;
    		case 's':
    			s = va_arg(argp, char *);
				if ( s ) {
    				fputs(s, outf);
				} 
    			break;
    
    		case '%':
    			fputc('%', outf );
    			break;

			default:
    			i = va_arg(argp, abaxint);
    			s = ltostr(i, fmtbuf );
    			fputs(s, outf);
				++p;
    			break;
   		}
   	}
    
   	va_end(argp);
	fflush( outf );
}

char *itostr( int i, char *buf )
{
	sprintf( buf, "%d", i );
	return buf;
}
char *ltostr( abaxint i, char *buf )
{
	sprintf( buf, "%lld", i );
	return buf;
}

// use jdfs to read data
ssize_t jdfpread( const JDFS *jdfs, char *buf, abaxint len, abaxint startpos )
{
	ssize_t s;

	#ifdef CHECK_LATENCY
		JagClock clock;
		clock.start();
	#endif

	s = jdfs->pread( buf, len, startpos );
	#ifdef CHECK_LATENCY
		clock.stop();
		printf("s5524 jdfpread(len=%d startpos=%d) took %d microsecs\n", len, startpos, clock.elapsedusec() );
	#endif

	return s;
}

// use jdfs to write data
ssize_t jdfpwrite( JDFS *jdfs, const char *buf, abaxint len, abaxint startpos )
{
	ssize_t s;

	#ifdef CHECK_LATENCY
		JagClock clock;
		clock.start();
	#endif

	s = jdfs->pwrite( buf, len, startpos );
	#ifdef CHECK_LATENCY
		clock.stop();
		printf("s5524 jdfpwrite(len=%d startpos=%d) took %d microsecs\n", len, startpos, clock.elapsedusec() );
	#endif
	return s;
}

// safe pread data from a file
ssize_t raysafepread( int fd, char *buf, abaxint length, abaxint startpos )
{
    register ssize_t bytes = 0;
    register ssize_t len;

   	len = jagpread( fd, buf, length, startpos );
	if ( len < 0 && errno == EINTR ) {
    	len = jagpread( fd, buf, length, startpos );
	}

	if ( len == 0 ) {
		return 0;
	} else if ( len < 0 ) {
		printf("E6287 raysafepread error fd=%d length=%lld startpos=%lld [%s]\n",
				fd, length, startpos, strerror(errno) ); 
		fflush( stdout );
		return -1;
	}

    bytes += len;
    while ( bytes < length ) {
       	len = jagpread( fd, buf+bytes, length-bytes, startpos+bytes );
		if ( len < 0 && errno == EINTR ) {
        	len = jagpread( fd, buf+bytes, length-bytes, startpos+bytes );
		}

		if ( len < 0 ) {
			printf("E6288 raysafepread error fd=%d length-bytes=%lld startpos+bytes=%lld [%s]\n",
				fd, length-bytes, startpos+bytes, strerror(errno) ); 
			fflush( stdout );
		}

        if ( len <= 0 ) {
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}

// safe pwrite data to a file
ssize_t raysafepwrite( int fd, const char *buf, abaxint len, abaxint startpos )
{
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    ptr = buf;
    nleft = len;
    while (nleft > 0) {
        nwritten = jagpwrite(fd, ptr, nleft, startpos );
        if ( nwritten <= 0 ) {
            if (nwritten < 0 && errno == EINTR) {
                nwritten = 0;   /* and call write() again */
			} else {
				printf("raysafepwrite error fd=%d len=%lld startpos=%lld [%s]\n", fd, len, startpos, strerror(errno) );
				fflush( stdout );
                return (-1);    /* error */
			}
        }

        nleft -= nwritten;
        ptr += nwritten;
		startpos += nwritten;
    }
    return (len);
}

int rayatoi( const char *buf, int length )
{
	char *p = (char*)buf;
	char savebyte = *(p+length);
	*(p+length) = '\0';
	int value = atoi(p);
	*(p+length) = savebyte;
	return value;
}

abaxint rayatol( const char *buf, int length )
{
	char *p = (char*)buf;
	char savebyte = *(p+length);
	*(p+length) = '\0';
	abaxint value = jagatoll(p);
	*(p+length) = savebyte;
	return value;
}

double rayatof( const char *buf, int length )
{
	char *p = (char*)buf;
	char savebyte = *(p+length);
	*(p+length) = '\0';
	double value = atof(p);
	*(p+length) = savebyte;
	return value;	
}

abaxdouble raystrtold( const char *buf, int length )
{
	char *p = (char*)buf;
	char savebyte = *(p+length);
	*(p+length) = '\0';
	abaxdouble value = jagstrtold(p, NULL);
	*(p+length) = savebyte;
	return value;
}


//errcode:  0 success; 1 input format invalid; 2 input exceed length; 
bool formatOneCol( int tzdiff, int servtzdiff, char *outbuf, const char *inbuf, 
				   AbaxDataString &errmsg, const AbaxDataString &name, 
				   const int offset, const int length, const int sig, const AbaxDataString &type )
{	
	// prt(("s3660 formatOneCol name=[%s] offset=%d length=%d type=[%s] inbuf=[%s]\n", name.c_str(), offset, length, type.c_str(), inbuf ));
	if ( length < 1 ) {
		// including type == "PT", type == "LN", etc
		return 1;
	}

	if ( *inbuf == '\0' ) return 1;
	int errcode = 0;
	char savebyte = *(outbuf+offset+length);
	abaxint actwlen = 0;
	int writelen = 0;

	JagParseAttribute jpa( NULL, tzdiff, servtzdiff );
	//prt(("s8282 formatOneCol type=[%s]  inbuf=[%s] offset=%d length=%d\n", type.c_str(), inbuf, offset, length ));

	if ( type == JAG_C_COL_TYPE_DATETIME ) { // for datetime
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length );
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) { // for datetime == timestamp
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length );
	} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) { // for datetime in nano sec
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length, true );
	} else if ( type == JAG_C_COL_TYPE_DATE ) { // for date
		errcode = JagTime::convertDateFormat( outbuf, inbuf, offset, length );
	} else if ( type == JAG_C_COL_TYPE_TIME ) { // for time
		errcode = JagTime::convertTimeFormat( outbuf, inbuf, offset, length );
	} else if ( type == JAG_C_COL_TYPE_TIMENANO ) { // for time in nano sec
		errcode = JagTime::convertTimeFormat( outbuf, inbuf, offset, length, true );
	} else if ( type == JAG_C_COL_TYPE_DBOOLEAN || type == JAG_C_COL_TYPE_DBIT ) { // for boolean
		if ( atoi(inbuf) == 0 ) { outbuf[offset] = '0'; } else { outbuf[offset] = '1'; }
	} else if ( type == JAG_C_COL_TYPE_STR ) { // for string
		actwlen = snprintf(outbuf+offset, length+1, "%s", inbuf);
		/***
		if ( ( actwlen = snprintf(outbuf+offset, length+1, "%s", inbuf) ) > length ) {
			errcode = 1;
		}
		***/
	} else { // for int/abaxint, float/double
		abaxint lonnum = 0;
		bool dofl = false;
		char *trackpos;

		if ( *inbuf == JAG_C_NEG_SIGN ) {
			outbuf[offset] = JAG_C_NEG_SIGN;
			trackpos = (char*)inbuf+1;
		} else {
			outbuf[offset] = JAG_C_POS_SIGN;
			trackpos = (char*)inbuf;
		}
		// before get lonnum, change scientific notation ( if has ) to regular data string
		// using atof and/or strtold to achieve
		AbaxDataString actdata = trackpos;
		if ( type == JAG_C_COL_TYPE_DOUBLE || type == JAG_C_COL_TYPE_FLOAT  ) {
			writelen = isValidSciNotation(inbuf); // borrow writelen
			//prt(("u2001 inbuf=[%s] writelen=%d\n", inbuf, writelen ));
			if ( 2 == writelen ) {
				actdata = longDoubleToStr( jagstrtold(trackpos, NULL) );
				trackpos = (char*)actdata.c_str();
			} else if ( 0 == writelen ) {
				actdata = trackpos = (char*)"0";
			}
			//prt(("u2230 actdata=[%s]\n", actdata.c_str() ));
		}

		lonnum = jagatoll(trackpos);  // the digits before . decimal point
		//prt(("u2210 trackpos=[%s] lonnum=%d\n", trackpos, lonnum ));

		if ( isInteger( type ) ) { // for int
			writelen = length; 
			if ( strlen(trackpos) > JAG_DBIGINT_FIELD_LEN ) errcode = 2;
		} else if ( type == JAG_C_COL_TYPE_DOUBLE || type == JAG_C_COL_TYPE_FLOAT ) { 
			// for double and float whole part
			writelen = length-sig-1;		
			dofl = true;
			//prt(("u0128 writelen=%d\n", writelen ));
		}

		if ( !errcode && ( actwlen = snprintf(outbuf+offset+1, writelen, "%0*lld", writelen-1, lonnum) ) > writelen-1 ) {
			//prt(("u2039 errcode=%d actwlen=%d writelen=%d lonnum=%d\n", errcode, actwlen, writelen, lonnum ));
			errcode = 2;
		}
		//prt(("u3001 actwlen=%d writelen-1=%d lonnum=%d\n", actwlen, writelen-1, lonnum ));

		if ( !errcode && dofl ) { // for double and float decimal part
			outbuf[offset+writelen] = '.';
			memset(outbuf+offset+writelen+1, JAG_C_POS_SIGN, sig);
			if ( (trackpos = strchr((char*)actdata.c_str(), '.')) != NULL ) {
				errcode = snprintf(outbuf+offset+writelen+1, sig+1, "%s", trackpos+1);
				//prt(("u2012 sig=%d errcode=%d trackpos+1=[%s]\n", sig, errcode, trackpos+1 ));
				if ( errcode > sig ) errcode = sig;
				outbuf[offset+writelen+errcode+1] = JAG_C_POS_SIGN;
				errcode = 0;
			}
		}
	}
	
	if ( errcode == 0 ) {
		*(outbuf+offset+length) = savebyte;
		return 1;
	} 
	
	if ( errcode == 1 ) {
		if ( type == JAG_C_COL_TYPE_DATE ) {
			errmsg = "E6200 Error date format. Please check your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIME ) {
			errmsg = "E6201 Error datetime format. Please check your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
			errmsg = "E6203 Error timestamp format. Please check your input.";
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			errmsg = "E6204 Error time format. Please check your input.";
		} else if ( type == JAG_C_COL_TYPE_STR ) {
			errmsg = "E6205 Length of string " + longToStr(actwlen) + " exceeded limit " + 
					intToStr(length) + " for column " + name + ". Please check your input.";
		} else {
			errmsg = "E6206 Error input. Please check your input.";
		}
	} else if ( errcode == 2 ) {
		errmsg = "E6208 Length of input " + longToStr(actwlen) + " exceeded limit " + 
				intToStr(writelen-1) + " for column " + name + ". Please check your input.";
	} else {
		errmsg = "E6210 Error error code";
	}
	
	return 0;
}


// dbformat <-> natural true format
void dbNaturalFormatExchange( char *buffer, int numKeys, const JagSchemaAttribute *attrs, int offset, int length, 
							  const AbaxDataString &type )
{
	rwnegConvertion( buffer, numKeys, attrs, offset, length, type );
}

void dbNaturalFormatExchange( char *buffers[], int num, int numKeys[], const JagSchemaAttribute *attrs[] )
{
	for ( int i = 0; i < num; ++i ) {
		rwnegConvertion( buffers[i], numKeys[i], attrs[i], 0, 0, " " );
	}
}

void rwnegConvertion( char *outbuf, int checkNum, const JagSchemaAttribute *schAttr, int offset, int length, 
					  const AbaxDataString &type )
{	
	if ( checkNum == 0 ) {
        if ( ( isInteger(type) || type == JAG_C_COL_TYPE_FLOAT
            || type == JAG_C_COL_TYPE_DOUBLE) && *(outbuf+offset) == JAG_C_NEG_SIGN ) {
            for ( int j = offset+1; j < offset+length; ++j ) {
                if ( *(outbuf+j) != '.' ) *(outbuf+j) = '9' - *(outbuf+j) + '0';
            }
        }
	}
	for ( int i = 0; i < checkNum; ++i ) {
		if ( ( isInteger(schAttr[i].type) || schAttr[i].type == JAG_C_COL_TYPE_FLOAT 
			|| schAttr[i].type == JAG_C_COL_TYPE_DOUBLE) && *(outbuf+schAttr[i].offset) == JAG_C_NEG_SIGN ) {
			for ( int j = schAttr[i].offset+1; j < schAttr[i].offset+schAttr[i].length; ++j ) {
				if ( *(outbuf+j) != '.' ) *(outbuf+j) = '9' - *(outbuf+j) + '0';
			}
		}
	}
}

#ifndef _WINDOWS64_
 AbaxDataString filePathFromFD( int fd )
 {
	char tmp[256];
	char file[256];
	sprintf(tmp, "/proc/self/fd/%d", fd );
	memset( file, 0, 256 );
	readlink( tmp, file, 256 );
	return file;
}
#endif


AbaxDataString removeCharFromString( const AbaxDataString &str, char dropc )
{
	if ( str.length()<1) return "";

	char *p = (char*)jagmalloc( str.length() + 1 );
	memset(p, 0, str.length() + 1 );
	int cnt = 0;
	for ( int i = 0; i < str.length(); ++i ) {
		if ( p[i] != dropc ) {
			p[cnt++] = toupper( p[i] );
		}
	}
	AbaxDataString res( p );
	if ( p ) free( p );
	p = NULL;
	return res;
}



AbaxDataString makeUpperString( const AbaxDataString &str )
{
	if ( str.length()<1) return "";

	char * p = (char*)jagmalloc( str.length() + 1 );
	p[str.length()] = '\0';
	for ( int i = 0; i < str.length(); ++i ) {
		p[i] = toupper( *(str.c_str()+i) );
	}
	AbaxDataString res( p );
	if ( p ) free( p );
	p = NULL;
	return res;
}

AbaxDataString makeLowerString( const AbaxDataString &str )
{
	if ( str.length()<1) return "";

	char *p = (char*)jagmalloc( str.length() + 1 );
	p[str.length()] = '\0';
	for ( int i = 0; i < str.length(); ++i ) {
		p[i] = tolower( *(str.c_str()+i) );
	}
	AbaxDataString res( p );
	if ( p ) free( p );
	p = NULL;
	return res;
}

AbaxFixString makeUpperOrLowerFixString( const AbaxFixString &str, bool isUpper )
{
	AbaxFixString res;
	if ( str.length() < 1 ) return res;
	char * p = (char*)jagmalloc( str.length() + 1 );
	p[str.length()] = '\0';
	for ( int i = 0; i < str.length(); ++i ) {
		if ( isUpper ) {
			p[i] = toupper( *(str.c_str()+i) );
		} else {
			p[i] = tolower( *(str.c_str()+i) );
		}
	}
	res = AbaxFixString( p, str.length() );
	if ( p ) free( p );
	p = NULL;
	return res;
}

AbaxDataString trimChar( const AbaxDataString &str, char c )
{
	AbaxDataString s1 = trimHeadChar( str, c);
	AbaxDataString s2 = trimTailChar( s1, c);
	return s2;
}

// input: "ccxjfjdkjfkd"  out: "xjfjdkjfkd"
AbaxDataString trimHeadChar( const AbaxDataString &str, char c )
{
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str();
	if ( *p != c ) return str; 
	while ( *p == c ) ++p;
	if ( *p == '\0' ) return "";
	return AbaxDataString(p);
}

// input: "xjfjdkjfkdcc"  out: "xjfjdkjfkd"
AbaxDataString trimTailChar( const AbaxDataString &str, char c )
{
	char r;
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p != c ) return str; 
	while ( *p == c && p >= str.c_str() ) --p;
    if ( p < str.c_str() ) return ""; 

	r = *(p+1);
	*(p+1) = '\0';
	AbaxDataString newstr = AbaxDataString( str.c_str() );
	*(p+1) = r;
	return newstr;
}

AbaxDataString trimTailLF( const AbaxDataString &str )
{
	char r;
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p != '\r' && *p != '\n' ) return str; 
	while ( (*p =='\r' || *p=='\n' ) && p >= str.c_str() ) --p;
    if ( p < str.c_str() ) return ""; 

	r = *(p+1);
	*(p+1) = '\0';
	AbaxDataString newstr = AbaxDataString( str.c_str() );
	*(p+1) = r;
	return newstr;
}

bool beginWith( const AbaxDataString &str, char c )
{
	char *p = (char*)str.c_str();
	if ( *p == c ) return true;
	return false;
}

bool beginWith( const AbaxString &str, char c )
{
	char *p = (char*)str.c_str();
	if ( *p == c ) return true;
	return false;
}

bool endWith( const AbaxDataString &str, char c )
{
	if ( str.size() < 1 ) return false;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p == c ) return true; 
	return false;
}

bool endWithStr( const AbaxDataString &str, const AbaxDataString &end )
{
	const char *p = jagstrrstr( str.c_str(), end.c_str() );
	if ( ! p ) return false;
	if ( strlen(p) != end.size() ) {
		return false;
	}

	return true;
}

bool endWith( const AbaxString &str, char c )
{
	if ( str.size() < 1 ) return false;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p == c ) return true; 
	return false;
}

// skip white spaces at end and check
bool endWhiteWith( const AbaxString &str, char c )
{
	if ( str.size() < 1 ) return false;
	char *p = (char*)str.c_str() + str.size()-1;
	while ( p != (char*)str.c_str() ) {
		if ( *p == c ) return true; 

		if ( isspace(*p) ) { --p; }
		else break;
	}
	return false;
}

// skip white spaces at end and check
bool endWhiteWith( const AbaxDataString &str, char c )
{
	if ( str.size() < 1 ) return false;
	char *p = (char*)str.c_str() + str.size()-1;
	while ( p != (char*)str.c_str() ) {
		if ( *p == c ) return true; 

		if ( isspace(*p) ) { --p; }
		else break;
	}
	return false;
}


#ifdef _WINDOWS64_
const char *strcasestr(const char *s1, const char *s2)
{
    if (s1 == 0 || s2 == 0) return 0;
    size_t n = strlen(s2);
   
    while(*s1) {
    	// if(!strncmpi(s1++,s2,n))
    	if(!strncasecmp(s1++,s2,n))
		{
     		return (s1-1);
		}
    }
    return 0;
}
#endif


AbaxDataString intToStr( int i )
{
	char buf[16];
	memset(buf, 0, 16 );
	sprintf(buf, "%d", i );
	return AbaxDataString(buf);
}

AbaxDataString longToStr( abaxint i )
{
	char buf[32];
	memset(buf, 0, 32 );
	sprintf(buf, "%lld", i );
	return AbaxDataString(buf);
}

AbaxDataString doubleToStr( double f )
{
	char buf[48];
	memset(buf, 0, 48 );
	sprintf(buf, "%.10f", f );
	return AbaxDataString(buf);
}

AbaxDataString doubleToStr( double f, int maxlen, int sig )
{
	char buf[64];
	memset(buf, 0, 64 );
	snprintf( buf, 64, "%0*.*f", maxlen, sig, f);
	return AbaxDataString(buf);
}


AbaxDataString longDoubleToStr( abaxdouble f )
{
	char buf[64];
	memset(buf, 0, 64 );
	#ifdef _WINDOWS64_
	sprintf(buf, "%f", f );
	#else
	sprintf(buf, "%Lf", f );
	#endif
	return AbaxDataString(buf);
}

// mode 0: use sprintf, 1 use snprintf
// fill 0: directly fill with Lf or f; 1: fill with *.*Lf or *.*f, where fill length are JAG_MAX_INT_LEN and JAG_MAX_SIG_LEN
int jagsprintfLongDouble( int mode, bool fill, char *buf, abaxdouble i, abaxint maxlen )
{
	int rc;
	#ifdef _WINDOWS64_
	if ( 0 == mode ) {
		if ( !fill ) rc = sprintf( buf, "%f", i );
		else rc = sprintf( buf, "%0*.*f", JAG_MAX_INT_LEN, JAG_MAX_SIG_LEN, i);
	} else {
		if ( !fill ) rc = snprintf( buf, maxlen+1, "%f", i );
		else rc = snprintf( buf, maxlen+1, "%0*.*f", JAG_MAX_INT_LEN, JAG_MAX_SIG_LEN, i);
	}
	#else
	if ( 0 == mode ) {
		if ( !fill ) rc = sprintf( buf, "%Lf", i );
		else rc = sprintf( buf, "%0*.*Lf", JAG_MAX_INT_LEN, JAG_MAX_SIG_LEN, i);
	} else {
		if ( !fill ) rc = snprintf( buf, maxlen+1, "%Lf", i );
		else rc = snprintf( buf, maxlen+1, "%0*.*Lf", JAG_MAX_INT_LEN, JAG_MAX_SIG_LEN, i);
	}
	#endif
	return rc;
}

bool lastStrEqual( const char *bigstr, const char *smallstr, int lenbig, int lensmall )
{
	//prt(("u2230 bigstr=[%s] smallstr=[%s] lenbig=%d lensmall=%d\n", bigstr, smallstr, lenbig, lensmall ));
	lensmall = reversestrlen( smallstr, lensmall );
	//prt(("u1293 lensmall=%d\n", lensmall ));
	if ( lensmall < 1 ) return false;

	lenbig = reversestrlen( bigstr, lenbig );
	//prt(("u1294 lenbig=%d\n", lenbig ));
	if ( lensmall > lenbig ) {
		return false;
	}

	if ( 0 == memcmp(bigstr+lenbig-lensmall, smallstr, lensmall) ) {
		//prt(("u1102 true\n" ));
		return true;
	} else {
		//prt(("u1103 false\n" ));
		return false;
	}
}

bool isInteger( const AbaxDataString &dtype )
{
	if ( dtype == JAG_C_COL_TYPE_DINT  ||
		dtype == JAG_C_COL_TYPE_DBIGINT   ||
		dtype == JAG_C_COL_TYPE_DBOOLEAN  ||
		dtype == JAG_C_COL_TYPE_DBIT  ||
		dtype == JAG_C_COL_TYPE_DTINYINT  ||
		dtype == JAG_C_COL_TYPE_DSMALLINT  ||
		dtype == JAG_C_COL_TYPE_DMEDINT )
	 {
	 	return true;
	 }
	 return false;
}

bool isDateTime( const AbaxDataString &dtype ) 
{
	if ( dtype == JAG_C_COL_TYPE_DATETIME  ||
		 dtype == JAG_C_COL_TYPE_TIMESTAMP  ||
		dtype == JAG_C_COL_TYPE_DATETIMENANO  ||
		dtype == JAG_C_COL_TYPE_DATE  ||
		dtype == JAG_C_COL_TYPE_TIME  ||
		dtype == JAG_C_COL_TYPE_TIMENANO )
	{
		return true;
	}
	return false;
}

bool isJoin( int dtype ) {
	if (JAG_INNERJOIN_OP == dtype ||
		JAG_LEFTJOIN_OP == dtype ||
		JAG_RIGHTJOIN_OP == dtype ||
		JAG_FULLJOIN_OP == dtype)
	{
		return true;
	}
	return false;
}

short getSimpleEscapeSequenceIndex( char p )
{
	if ( p == 'a' ) return 7;
	else if ( p == 'b' ) return 8;
	else if ( p == 't' ) return 9;
	else if ( p == 'n' ) return 10;
	else if ( p == 'v' ) return 11;
	else if ( p == 'f' ) return 12;
	else if ( p == 'r' ) return 13;
	else if ( p == '"' ) return 34;
	else if ( p == '\'' ) return 39;
	else if ( p == '?' ) return 63;
	else if ( p == '\\' ) return 92;
	else return 0;
}

// strip ending \n and \r
int stripStrEnd( char *msg, int len )
{
    int i;
	int t = len;
    for ( i=len-1; i>=0; --i ) {
        if ( msg[i] == '\n' ) {  msg[i] = '\0'; --t; }
        else if ( msg[i] == '\r' ) { msg[i] = '\0'; --t; }
    }

    return t;
}

// if c==';'
// "sssss;    "  : return 1  --- saw last line wth ;
// "sssss;hhh    "  :0       --- not the end line with ;
int trimEndWithChar ( char *msg, int len, char c )
{
	if ( ! msg ) return 0;

	char *p = msg + len-1;
	while ( p != msg ) {
		if ( isspace(*p) ) { *p = '\0'; --p; }
		else break;
	} 

	if ( *p == c ) {
		return 1;
	} else {
		return 0;
	}
}


// begin msg: "jdkjfkdjkfdjkfdkfd}ffff"
// end msg: "jdkjfkdjkfdjkfdkfd}"
// stopc='}'
int trimEndToChar ( char *msg, int len, char stopc )
{
	if ( ! msg ) return 0;
	if ( *msg == '\0' ) return 0;

	char *p = msg + len-1;
	while ( p != msg ) {
		if ( *p == stopc ) break;
		*p = '\0'; --p; 
	} 

	return 1;
}



// 1: last line with c;
// 0: not-last line with c;
int trimEndWithCharKeepNewline ( char *msg, int len, char c )
{
	if ( ! msg ) return 0;
	char *p = msg + len-1;
	int hasEndC = 0;
	while ( p != msg ) {
		if ( isspace(*p) ) { --p; }
		else {
			if ( *p == c ) { hasEndC = 1; }
			break;
		}
	} 

	p = msg + len-1;
	if ( hasEndC ) {
		while ( p != msg ) {
			if ( isspace(*p) ) { *p = '\0'; --p; }
			else break;
		}
	}

	return hasEndC;
}

AbaxDataString intToString( int i ) 
{
	char buf[16];
	sprintf(buf, "%d", i );
	return AbaxDataString( buf );
}
AbaxDataString longToString( abaxint i ) 
{
	char buf[32];
	sprintf(buf, "%lld", i );
	return AbaxDataString( buf );
}
AbaxDataString ulongToString( uabaxint i ) 
{
	char buf[32];
	sprintf(buf, "%lu", i );
	return AbaxDataString( buf );
}

abaxint strchrnum( const char *str, char ch )
{
    if ( ! str || *str == '\0' ) return 0;

    abaxint cnt = 0;
    const char *p = str;
    const char *q;
    while ( 1 ) {
        q=strchr(p, ch);
        if ( ! q ) break;
        ++cnt;
        p = q+1;
    }

    return cnt;
}

void escapeNewline( const AbaxDataString &instr, AbaxDataString &outstr )
{
	if ( instr.size() < 1 ) return;

	abaxint n = strchrnum( instr.c_str(), '\n' );
	char *str = (char*)jagmalloc( instr.size() + 2*n + 1 );
	const char *p = instr.c_str();
	char *q = str;
	while ( *p != '\0' ) {
		if ( *p == '\n' ) {
			*q++ = '\\';
			*q++ = 'n';
		} else {
			*q++ = *p;
		}
		++p;
	}
	*q++ = '\0';
	outstr = AbaxDataString(str);
}



// str: pointer to string, int len:  str2: "str1 str2"
// str in any tokens contained in str2
// 0: no   1: found true
int strInStr( const char *str, int len, const char *str2 )
{
    const char *p = str2;
    if ( !p ) return 0;
    if ( *p == '\0' ) return 0;
    if ( !str ) return 0;
    if ( *str == '\0' ) return 0;

    int i;
    int found = 0;
    const char *q;
    while ( *p != '\0' ) {
        found = 1;
        q = p;
        for ( i = 0; i < len; ++i ) {
            if ( str[i] != *q++ ) {
                found = 0;
                break;
            }
        }

        if ( found ) { break; }
        ++p;
    }

    return found;
}

// input fpath  /tmp/abc/fff.jdb
// output: first=/tmp/abc/fff  last=jdb
void splitFilePath( const char *fpath, AbaxDataString &first, AbaxDataString &last )
{
	char *p = (char*)strrchr( fpath, '.' );
	if ( !p ) {
		first = fpath;
		return;
	}

	*p = '\0';
	first = fpath;
	last = p+1;
	*p = '.';
}

// input fpath:  /tmp/abc/fff.jdb  newLast: "bid"
// output: first=/tmp/abc/fff.bid
AbaxDataString renameFilePath( const AbaxDataString& fpath, const AbaxDataString &newLast )
{
	char *p = (char*)strrchr( fpath.c_str(), '.' );
	if ( !p ) {
		return fpath;
	}
	*p = '\0';
	
	AbaxDataString newPath = AbaxDataString(fpath.c_str()) + "." + newLast;
	*p = '.';
	return newPath;
}

void stripeFilePath( const AbaxDataString &fpath, abaxint stripe, AbaxDataString &stripePath )
{
	AbaxDataString first, last;
	splitFilePath( fpath.c_str(), first, last );
	if ( last.size() < 1 ) {
		printf("s5721 error in stripeFilePath fpath=[%s]\n", fpath.c_str() );
		return;
	}

	stripePath = first + "." + longToStr(stripe) + "." + last;
}

AbaxDataString makeDBObjName( JAGSOCK sock, const AbaxDataString &dbname, const AbaxDataString &objname )
{
	AbaxDataString dbobj;
	dbobj = dbname + "." + objname;
	return dbobj;
}

AbaxDataString makeDBObjName( JAGSOCK sock, const AbaxDataString &dbdotobj )
{
	JagStrSplit split( dbdotobj, '.' );
	return makeDBObjName( sock, split[0], split[1] );
}

// /home/user1/jaguar  or /home/user2/brandx
AbaxDataString jaguarHome()
{
	AbaxDataString jaghm;
	const char *p = getenv("JAGUAR_HOME");
	if (  ! p ) {
		p = getenv("HOME");
		if ( p ) {
			jaghm = AbaxDataString(p) + "/" + AbaxDataString(JAG_BRAND);
		} else {
			#ifdef _WINDOWS64_
		       p = "/c/jaguar";
		       _mkdir( p );
			#else
		  	  p = "/tmp/jaguar";
		      ::mkdir( p, 0777 );
		    #endif
			jaghm = AbaxDataString(p);
		}
	} else {
		jaghm = AbaxDataString(p);
	}
	
	// return AbaxDataString(p) + "/" + AbaxDataString(JAG_BRAND);
	return jaghm;
}

// Used for  yes or nor tests.  a should be lowercase
bool startWith( const AbaxDataString &str, char a )
{
	if ( str.size() < 1 ) return false;
	if ( *(str.c_str()) == a ) return true;
	if ( *(str.c_str()) == toupper(a) ) return true;
	return false;

}

// safe read data from a file
ssize_t raysaferead( int fd, char *buf, abaxint len )
{
     size_t  nleft;
     ssize_t nread;
     char   *ptr;

     ptr = buf;
     nleft = len;
     while (nleft > 0) {
         if ( (nread = ::read(fd, ptr, nleft)) < 0) {
             if (errno == EINTR)
                 nread = 0;      /* and call read() again */
             else
                 return (-1);
         } else if (nread == 0)
             break;              /* EOF */

         nleft -= nread;
         ptr += nread;
     }
     return (len - nleft);         /* return >= 0 */
}

ssize_t raysaferead( int fd, unsigned char *buf, abaxint len )
{
     size_t  nleft;
     ssize_t nread;
     unsigned char   *ptr;

     ptr = buf;
     nleft = len;
     while (nleft > 0) {
         if ( (nread = ::read(fd, ptr, nleft)) < 0) {
             if (errno == EINTR)
                 nread = 0;      /* and call read() again */
             else
                 return (-1);
         } else if (nread == 0)
             break;              /* EOF */

         nleft -= nread;
         ptr += nread;
     }
     return (len - nleft);         /* return >= 0 */
}


// safe write data to a file
ssize_t raysafewrite( int fd, const char *buf, abaxint len )
{
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    ptr = buf;
    nleft = len;
    while (nleft > 0) {
        if ( (nwritten = ::write(fd, ptr, nleft)) <= 0) {
            if (nwritten < 0 && errno == EINTR)
                nwritten = 0;   /* and call write() again */
            else
                return (-1);    /* error */
        }

        nleft -= nwritten;
        ptr += nwritten;
    }
    return (len);
}

// find mid point of min-max, check if inkey is > midpoint
// 0: use the min-side server
// 1: use the max-side server
int selectServer( const AbaxFixString &min, const AbaxFixString &max, const AbaxFixString &inkey )
{
	const char *p1, *p2, *t;
	int i, mid;

	p1 = min.c_str();
	p2 = max.c_str();
	t = inkey.c_str();

	for ( i = 0; i < min.size(); ++i ) {
		if ( *p1 == *p2 ) {
			if ( *t > *p1 ) {
				return 1;
			} else { 
				return 0;
			}
			++p1; ++p2; ++t;
			continue;
		}

		mid = ( *p1 + *p2 )/2;
		if ( *t > mid ) {
			return 1;
		} else {
			return 0;
		}
	}

	return 0;
}


// Available Memory to use in bytes
abaxint availableMemory( abaxint &callCount, abaxint lastBytes )
{
	abaxint bytes;
	#ifndef _WINDOWS64_
		bytes = sysconf( _SC_PAGESIZE ) * sysconf( _SC_AVPHYS_PAGES );
    #else
        MEMORYSTATUSEX memInfo;
		memInfo.dwLength = sizeof( memInfo );
        GlobalMemoryStatusEx (&memInfo);
        bytes = memInfo.ullAvailPhys;
	#endif

	if ( callCount < 0 ) {
		return bytes;
	} else if ( callCount == 0 ) {
		++callCount;
		return bytes;
	} else if ( callCount >= 100000 ) {
		callCount = 0;
		return bytes;
	} else {
		if ( lastBytes <= 0 ) {
			callCount = 0;
			return bytes;
		} else {
			++callCount;
			return lastBytes;
		}
	}
}

// return 1: write related, insert, cinsert, dinsert, create, alter, update, delete, drop, truncate, import
//  1: broadcast command to all clusters and all servers
// return 0: read related, select, show, desc and/or other commands
// 0: sent to maybe onnly 1 server
int checkReadOrWriteCommand( const char *pmesg )
{
	if ( 0 == strncasecmp( pmesg, "insert", 6 ) || 0 == strncasecmp( pmesg, "sinsert", 7 ) ||
		 0 == strncasecmp( pmesg, "cinsert", 7 ) || 0 == strncasecmp( pmesg, "dinsert", 7 ) ||
		 0 == strncasecmp( pmesg, "create", 6 ) || 0 == strncasecmp( pmesg, "alter", 5 ) ||
		 0 == strncasecmp( pmesg, "update", 6 ) || 0 == strncasecmp( pmesg, "delete", 6 ) ||
		 0 == strncasecmp( pmesg, "use ", 4 ) || 0 == strncmp( pmesg, "import", 6 ) ||
		 0 == strncasecmp( pmesg, "drop", 4 ) || 0 == strncasecmp( pmesg, "truncate", 8 ) ||
		 0 == strncasecmp( pmesg, "createdb", 8 ) || 0 == strncasecmp( pmesg, "dropdb", 6 ) ||
		 0 == strncasecmp( pmesg, "createuser", 10 ) || 0 == strncasecmp( pmesg, "dropuser", 8 ) ||
		 0 == strncasecmp( pmesg, "grant", 5 ) || 0 == strncasecmp( pmesg, "revoke", 6 ) ||
		 0 == strncasecmp( pmesg, "changepass", 10 ) || 0 == strncasecmp( pmesg, "changedb", 8 ) ) {
		 return 1;
	}
	return 0;
}

// return 1: string type
// return 2: bool type
// return 3: int type
// return 4: abaxint type
// return 5: float/double type
// return 6: date/time type
int checkColumnTypeMode( const AbaxDataString &type )
{
	if ( type == JAG_C_COL_TYPE_STR ) return 1;
	else if ( type == JAG_C_COL_TYPE_DBOOLEAN || type == JAG_C_COL_TYPE_DBIT ) return 2;
	else if ( type == JAG_C_COL_TYPE_DINT  || type == JAG_C_COL_TYPE_DTINYINT  || 
				type == JAG_C_COL_TYPE_DSMALLINT  || type == JAG_C_COL_TYPE_DMEDINT  ) return 3;
	else if ( type == JAG_C_COL_TYPE_DBIGINT  ) return 4;
	else if ( type == JAG_C_COL_TYPE_FLOAT  || type == JAG_C_COL_TYPE_DOUBLE  ) return 5;
	else if ( type == JAG_C_COL_TYPE_DATETIME || type == JAG_C_COL_TYPE_TIMESTAMP  || type == JAG_C_COL_TYPE_TIME  || 
				type==JAG_C_COL_TYPE_DATETIMENANO  || type==JAG_C_COL_TYPE_TIMENANO || type==JAG_C_COL_TYPE_DATE  ) return 6;
	return 0;
}

AbaxDataString formOneColumnNaturalData( const char *buf, abaxint offset, abaxint length, const AbaxDataString &type )
{
	AbaxDataString instr, outstr;
	int rc = checkColumnTypeMode( type );
	if ( 6 == rc ) {
		if ( type == JAG_C_COL_TYPE_DATETIME || type == JAG_C_COL_TYPE_TIMESTAMP ) {
			instr = AbaxDataString( buf+offset, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr );
		} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
			instr = AbaxDataString( buf+offset, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr, true );
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			instr = AbaxDataString( buf+offset, length );
			JagTime::convertTimeToStr( instr, outstr );
		} else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
			instr = AbaxDataString( buf+offset, length );
			JagTime::convertTimeToStr( instr, outstr, 2 );
		} else if ( type == JAG_C_COL_TYPE_DATE ) {
			instr = AbaxDataString( buf+offset, length );
			JagTime::convertDateToStr( instr, outstr );
		}
	} else if ( 5 == rc ) {
		outstr = longDoubleToStr( raystrtold(buf+offset, length) );
	} else if ( 4 == rc ) {
		outstr = longToStr( rayatol(buf+offset, length) );
	} else if ( 3 == rc ) {
		outstr = intToStr( rayatoi(buf+offset, length) );
	} else if ( 2 == rc ) {
		outstr = intToStr( rayatoi(buf+offset, length) != 0 );
	} else if ( 1 == rc ) {
		/**
		AbaxDataString tmpstr = AbaxDataString(buf+offset, length);
		outstr = tmpstr.c_str();
		**/
		outstr = AbaxDataString(buf+offset, length);
	}
	return outstr;
}

/***
AbaxDataString printCharWithEscapeSequence( const char p )
{
	AbaxDataString str;
	if ( p == '\a' ) str = "\\a";
	else if ( p == '\b' ) str = "\\b"; 
	else if ( p == '\t' ) str = "\\t";
	else if ( p == '\n' ) str = "\\n";
	else if ( p == '\v' ) str = "\\v";
	else if ( p == '\f' ) str = "\\f";
	else if ( p == '\r' ) str = "\\r";
	else if ( p == '"' ) str = "\"";
	else if ( p == '\'' ) str = "'";
	else if ( p == '\?' ) str = "\\?";
	else if ( p == '\\' ) str = "\\";
	else str = p;
	return str;
}
****/


// rewrite for group by 1-24-2017
// gdvhdr is for group by use only
// return 0: error
// return 1: success
// num is # of tables involved. normally num=1 for one table
int rearrangeHdr( int num, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
					JagParseParam *parseParam, const JagVector<SetHdrAttr> &spa, AbaxDataString &newhdr, AbaxDataString &gbvhdr,
					abaxint &finalsendlen, abaxint &gbvsendlen, bool needGbvs )
{
	//prt(("u2033 rearrangeHdr num=%d parseParam->hasColumn=%d ...\n", num, parseParam->hasColumn ));
	// qwer
	gbvsendlen = 0;
	int rc, collen, siglen, constMode = 0, typeMode = 0;
	bool isAggregate;
	abaxint offset = 0;
	ExpressionElementNode *root;
	const JagSchemaRecord *records[num];
	AbaxDataString type, hdr, tname;
	int groupnum = parseParam->groupVec.size();

	if ( !parseParam->hasColumn && num == 1 ) {
		// select * from t123 or getfile  from t123
		//prt(("u6521 in rearrangehdr \n" ));
		newhdr = spa[0].sstring;
		finalsendlen = spa[0].record->keyLength + spa[0].record->valueLength;
		if ( parseParam->opcode == JAG_GETFILE_OP ) {
			JagSchemaRecord record;
			newhdr = record.formatHeadRecord();
			for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
				type = JAG_C_COL_TYPE_STR;
				collen = JAG_FILE_FIELD_LEN;
				siglen = 0;
				tname = parseParam->selColVec[i].getfileCol;
				if ( JAG_GETFILE_SIZE == parseParam->selColVec[i].getfileType ) {
					tname += ".size";
				} else if ( JAG_GETFILE_TIME == parseParam->selColVec[i].getfileType ) {
					tname += ".time";
				} else if ( JAG_GETFILE_MD5SUM == parseParam->selColVec[i].getfileType ) {
					tname += ".md5";
				}

				newhdr += record.formatColumnRecord( tname.c_str(), type.c_str(), offset, collen, siglen );

				parseParam->selColVec[i].offset = offset;
				parseParam->selColVec[i].length = collen;
				parseParam->selColVec[i].sig = siglen;
				parseParam->selColVec[i].type = type;
				offset += collen;
			}
			newhdr += record.formatTailRecord();
			finalsendlen = offset;
		}
		// else the header is formed in following
	} else {
		//prt(("u1043 in rearrangehdr \n" ));
		for ( int i = 0; i < num; ++i ) {
			records[i] = spa[i].record;
		}	

		newhdr = records[0]->formatHeadRecord();
		if ( !parseParam->hasColumn ) {	
			// select * from t123 ...
			for ( int i = 0; i < num; ++i ) {
				for ( int j = 0; j < records[i]->columnVector->size(); ++j ) {
					if ( num == 1 ) {
						hdr = (*(records[i]->columnVector))[j].name.c_str();
					} else {	
						hdr = spa[i].dbobj + "." + AbaxDataString((*(records[i]->columnVector))[j].name.c_str());
					}

					newhdr += records[i]->formatColumnRecord( hdr.c_str(), (*(records[i]->columnVector))[j].type.c_str(), offset, 
								(*(records[i]->columnVector))[j].length, (*(records[i]->columnVector))[j].sig );
					offset += (*(records[i]->columnVector))[j].length;
				}
			}	
		} else {
			// selecg c1, c2, c3 from t123 ...
			//prt(("u1045 in rearrangehdr selecg c1, c2, c3 from t123... \n" ));
			for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
				isAggregate = false;
				root = parseParam->selColVec[i].tree->getRoot();
				// get collen, siglen etc
				rc = root->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, type, collen, siglen );
				if ( 0 == rc ) {
					printf("s3004 Error setFuncAttribute\n");
					return 0;
				}

				newhdr += records[0]->formatColumnRecord( parseParam->selColVec[i].asName.c_str(), 
														  type.c_str(), offset, collen, siglen );

				#ifdef DEVDEBUG
				prt(("u2046 i=%d name=[%s] asName=[%s] type=[%s] offset=%d collen=%d isAggregate=%d\n",
					i, parseParam->selColVec[i].name.c_str(), parseParam->selColVec[i].asName.c_str(), 
					type.c_str(), offset, collen, isAggregate ));
				#endif

				parseParam->selColVec[i].offset = offset;
				parseParam->selColVec[i].length = collen;
				parseParam->selColVec[i].sig = siglen;
				parseParam->selColVec[i].type = type;
				parseParam->selColVec[i].isAggregate = isAggregate;
				offset += collen;
			}
		}
		newhdr += records[0]->formatTailRecord();
		finalsendlen = offset;
	}
	
	if ( !parseParam->hasGroup || !parseParam->hasColumn || !needGbvs ) {
		// prt(("u1130 in rearrangehdr return 1\n" ));
		return 1;
	}

	// else, is group by ( both key and value ), need to form gbvhdr to use later ( must has columns, used by server )
	for ( int i = 0; i < num; ++i ) {
		records[i] = spa[i].record;
	}	

	offset = 0;
	gbvhdr = records[0]->formatHeadRecord( );

	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		isAggregate = false;
		root = parseParam->selColVec[i].tree->getRoot();
		rc = root->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, type, collen, siglen );
		if ( 0 == rc ) {
			printf("s3005 Error setFuncAttribute\n");
			return 0;
		}
		if ( i < groupnum ) {
			gbvhdr += records[0]->formatColumnRecord( parseParam->selColVec[i].asName.c_str(), 
													  type.c_str(), offset, collen, siglen, true );
		} else {
			gbvhdr += records[0]->formatColumnRecord( parseParam->selColVec[i].asName.c_str(), 
													  type.c_str(), offset, collen, siglen, false );
		}
		offset += collen;
	}

	gbvhdr += records[0]->formatTailRecord( );
	gbvsendlen = offset;
	
	return 2;
}

// check if GroupVec has correct order with select column orders, must be called after select tree
int checkGroupByValidation( const JagParseParam *parseParam )
{
	int grouplen = 0;
	if ( parseParam->selColVec.size() < parseParam->groupVec.size() ) {
		return 0;
	}
	for ( int i = 0; i < parseParam->groupVec.size(); ++i ) {
		if ( parseParam->groupVec[i].name != parseParam->selColVec[i].asName ) {
			return 0;
		}
		grouplen += parseParam->selColVec[i].length;
	}
	return grouplen;
}


int checkAndReplaceGroupByAlias( JagParseParam *parseParam )
{
	bool found;
	AbaxDataString gstr;
	for ( int i = 0; i < parseParam->groupVec.size(); ++i ) { 
		found = false;
		for ( int j = 0; j < parseParam->selColVec.size(); ++j ) {
			if ( parseParam->groupVec[i].name == parseParam->selColVec[j].asName ) {
				if ( parseParam->selColVec[j].givenAsName ) {
					gstr += parseParam->selColVec[j].origFuncStr;
					parseParam->groupVec[i].name = parseParam->selColVec[j].origFuncStr;
				} else {
					gstr += parseParam->selColVec[j].asName;
				}
				found = true;
				break;
			}
		}
		if ( !found ) {
			return 0;
		}
		if ( i != parseParam->groupVec.size()-1 ) {
			gstr += ",";
		}
	}
	parseParam->selectGroupClause = gstr;
	return 1;
}

// str: "k=v/k=v/k=v"  ch:  '/'
// output: hashmap
void convertToHashMap( const AbaxDataString &kvstr, char sep,  JagHashMap<AbaxString, AbaxString> &hashmap )
{
	if ( kvstr.size()<1) return;
	JagStrSplit sp( kvstr, sep, true );
	if ( sp.length()<1) return;
	AbaxDataString kv;
	for ( int i=0; i < sp.length(); ++i ) {
		kv = sp[i];
		JagStrSplit p( kv, '=');
		if ( p.length()>1 ) {
			hashmap.addKeyValue( p[0], p[1] );
		}
	}
}
// /home/dev2/jaguar/data..  --> /home/dev3/jaguar/data...
// if fpath=="/home/sssss/ffff", convert to /home/$HOME/ffff
void changeHome( AbaxDataString &fpath )
{
	//prt(("s5880 changeHome in=[%s]\n", fpath.c_str() ));
	JagStrSplit oldsp( fpath, '/', true );
	char *home = getenv("JAGUAR_HOME");
	if ( ! home ) home = getenv("HOME");
	int idx = 0;
	for ( int i = oldsp.length()-1; i>=0; --i ) {
		if ( oldsp[i] == JAG_BRAND ) {
			idx = i;
			break; // found last "jaguar"
		}
	}

	if ( 0 == idx ) {
		printf("Fatal error: %s path is wrong\n", fpath.c_str() );
		return;
	}

	fpath = home;
	for ( int i = idx; i < oldsp.length(); ++i ) {
		fpath += AbaxDataString("/") + oldsp[i];
	}
	//prt(("s5881 changeHome out=[%s]\n", fpath.c_str() ));

}

int jaguar_mutex_lock(pthread_mutex_t *mutex)
{
	int rc = pthread_mutex_lock( mutex );
	if ( 0 != rc ) {
		prt(("s6803 error pthread_mutex_lock(%0x) [%s]\n", mutex, strerror( rc ) ));
	}
	return rc;
}

int jaguar_mutex_unlock(pthread_mutex_t *mutex)
{
	int rc = pthread_mutex_unlock( mutex );
	if ( 0 != rc ) {
		prt(("s6804 error pthread_mutex_unlock(%0x) [%s]\n", mutex, strerror( rc ) ));
	}
	return rc;
}

int jaguar_cond_broadcast( pthread_cond_t *cond)
{
	int rc = pthread_cond_broadcast( cond );
	if ( 0 != rc ) {
		prt(("s6805 error pthread_cond_broadcast(%0x) [%s]\n", cond, strerror( rc ) ));
	}
	return rc;
}

int jaguar_cond_signal( pthread_cond_t *cond)
{
	int rc = pthread_cond_signal( cond );
	if ( 0 != rc ) {
		prt(("s6806 error pthread_cond_signal(%0x) [%s]\n", cond, strerror( rc ) ));
	}
	return rc;
}

int jaguar_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	int rc = pthread_cond_wait(cond, mutex);
	if ( 0 != rc ) {
		prt(("s6807 error pthread_cond_wait(%0x %0x) [%s]\n", cond, mutex, strerror( rc ) ));
	}
	return rc;
}

int jaguar_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, const struct timespec *abstime)
{
	int rc = pthread_cond_timedwait(cond, mutex, abstime);
	if ( ETIMEDOUT == rc ) {
	} else if ( 0 != rc ) {
		prt(("s6808 error pthread_cond_timedwait(%0x %0x : %ld-%ld)  [%s]\n", cond, mutex, abstime->tv_sec, abstime->tv_nsec, strerror( rc ) ));
	}
	return rc;
}

// 0: failure
// 1: OK
int getPassword( AbaxDataString &outPassword )
{
	outPassword = "";
    char password[128];
	#ifndef _WINDOWS64_
    struct termios oflags, nflags;
    /* disabling echo */
    tcgetattr(fileno(stdin), &oflags);
    nflags = oflags;
    nflags.c_lflag &= ~ECHO;
    nflags.c_lflag |= ECHONL;
    if (tcsetattr(fileno(stdin), TCSANOW, &nflags) != 0) {
        perror("tcsetattr");
        return 0;;
    }

    fgets(password, sizeof(password), stdin);
    tcsetattr(fileno(stdin), TCSANOW, &oflags);
	#else
	memset( password, 0, 12 );
	getWinPass( password );
	#endif

    password[strlen(password) - 1] = 0;
	outPassword = password;

    return 1;
}

int jagmkdir(const char *path, mode_t mode)
{
	#ifdef _WINDOWS64_
		::mkdir( path );
	#else
		::mkdir( path, mode );
	#endif
}


ssize_t jagpread( int fd, char *buf, abaxint length, abaxint startpos )
{
	ssize_t len;
	#ifdef _WINDOWS64_
		lseek(fd, startpos, SEEK_SET );
    	len = ::read( fd, buf, length );
	#else
    	len = ::pread( fd, buf, length, startpos );
	#endif

	return len;
}

ssize_t jagpwrite( int fd, const char *buf, abaxint length, abaxint startpos )
{
	ssize_t len;
	#ifdef _WINDOWS64_
		lseek(fd, startpos, SEEK_SET );
    	len = ::write( fd, buf, length );
	#else
    	len = ::pwrite( fd, buf, length, startpos );
	#endif

	return len;
}

// find macting string not inside '...' "..."
// str should be moving pointer
const char *strcasestrskipquote( const char *str, const char *token )
{
    int toklen = strlen( token );
    while ( *str != '\0' ) {
        if ( *str == '\'' || *str == '\"' ) {
            str = jumptoEndQuote( str );
            if ( ! str ) return NULL;
            ++str;
            if ( *str == '\0' ) break;
        }

        if ( 0 == strncasecmp( str, token, toklen) ) {
            // str += toklen;
            // return str-toklen;
            return str;
        }
        ++str;
    }

    return NULL;
}

// find macting string not inside '...' "..."
// str should be moving pointer
const char *strcasestrskipspacequote( const char *str, const char *token )
{
	prt(("s0928 strcasestrskipspacequote str=[%s] token=[%s]\n", str, token ));
	while ( isspace(*str) ) ++str;
    int toklen = strlen( token );
    while ( *str != '\0' ) {
        if ( *str == '\'' || *str == '\"' ) {
            str = jumptoEndQuote( str );
			prt(("s0938 str=[%s]\n", str ));
            if ( ! str ) return NULL;
            ++str;
			prt(("s0934 str=[%s]\n", str ));
			while ( isspace(*str) ) ++str;
			prt(("s0932 str=[%s]\n", str ));
            if ( *str == '\0' ) break;
        }

		prt(("s3932 str=[%s]\n", str ));
		while ( isspace(*str) ) ++str;
		prt(("s3912 str=[%s]\n", str ));
        if ( 0 == strncasecmp( str, token, toklen) ) {
            // str += toklen;
            // return str-toklen;
            return str;
        }
        ++str;
		prt(("s8912 str=[%s]\n", str ));
    }

    return NULL;
}

int jagsync( )
{
	#ifndef _WINDOWS64_
		sync();
		return 1;
	#else
		return 1;
	#endif
}

int jagfdatasync( int fd )
{
	#ifndef _WINDOWS64_
		return fdatasync( fd );
	#else
		return 1;
	#endif
}

int jagfsync( int fd )
{
	#ifndef _WINDOWS64_
		return fsync( fd );
	#else
		return 1;
	#endif
}

void trimLenEndColonWhite( char *str, int len )
{
	char *p = str + len -1;
	while ( p >= str && isspace(*p) ) { *p = '\0'; --p; }
	if ( p >= str && *p == ';' ) { *p = '\0'; }
}

void trimEndColonWhite( char *str )
{
	int len = strlen( str );
	trimLenEndColonWhite( str, len );
}


void randDataStringSort( AbaxDataString *vec, int maxlen )
{
	struct timeval now;
	gettimeofday( &now, NULL );
	abaxint microsec = now.tv_sec * (abaxint)1000000 + now.tv_usec;
	srand( microsec );
	AbaxDataString tmp;
	int i = maxlen-1, pivot = maxlen-1, radidx;
	while ( i > 0 ) {
		tmp = vec[pivot];
		radidx = rand() % i;
		vec[pivot] = vec[radidx];
		vec[radidx] = tmp;
		--i;
		--pivot;
	}
}

abaxint getNearestBlockMultiple( abaxint value )
{
	if ( value < JAG_BLOCK_SIZE ) value = JAG_BLOCK_SIZE;
	abaxint a, b;
	double c, d;
	d = (double)value;
	c = log2 ( d );
	b = (abaxint) c;
	++b;
	a = exp2( b );
	return a;
}

abaxint getBuffReaderWriterMemorySize( abaxint value ) // max as 1024 ( MB ), value and return in MB
{
	if ( value <= 0 ) value = 0;
	value = getNearestBlockMultiple( value ) / 2;
	if ( value > 1024 ) value = 1024;
	return value;
}

char *jagmalloc( abaxint sz )
{
	// if ( sz > 1000000000 ) abort();
	//todo tmpdebug

	char *p = NULL;
	while ( NULL == p ) {
		p = (char*)malloc(sz);
		if ( NULL == p ) {
			printf("ERR0300 Error malloc %lld bytes memory, not enough memory. Please free up some memory. Retry in 10 seconds...\n", sz );
			fflush( stdout );
			sleep(10);
		}
	}

	return p;
}

// 0: OK;  else: cannot create
int jagpthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine) (void *), void *arg)
{
	int rc = 1;
	while ( rc != 0 ) {
		rc = pthread_create( thread, attr, (*start_routine), arg );
		if ( rc != 0 ) {
			printf("ERR0400 Error creating new thread, errno=%d. Please clean up some unused processes. Retry in 10 seconds...\n", rc );
			fflush( stdout );
			sleep(10);
		}
	}

	return 0;
}

int jagpthread_join(pthread_t thread, void **retval)
{
	int rc = pthread_join( thread, retval );
	if ( rc != 0 ) {
		printf("Error pthread_join , errno=%d\n", rc );
		fflush( stdout );
	}
	return rc;
}

// send data to socket
// return -1: error; bytes received
// buf has space to hold len bytes
abaxint sendData( JAGSOCK sock, const char *buf, abaxint len )
{
	#if 0
		// 21 is HB heartbeat
		if ( 21 != len ) {
			prt(("s2625 sendData sock=%d buf=[%s] len=%d dumpmem:\n", sock, buf, len ));
			dumpmem(buf, len );
			prt(("\n"));
			//if ( 106 == len ) abort();
		}
	#endif
	abaxint slen = _raysend( sock, buf, len );

	/**
	if ( slen > JAG_SOCK_MSG_HDR_LEN && *(buf+JAG_SOCK_MSG_HDR_LEN-3) == 'H' && *(buf+JAG_SOCK_MSG_HDR_LEN-2) == 'B' ) {
		// no print HB
	} else {
		prt(("s7889 socket=%d len=%lld slen=%lld sendData=[%.200s]\n", sock, len, slen, buf));
	}
	**/

	if ( slen < len ) { 
		// prt(("sendData error 1=%d %d %d\n", sock, slen, len)); 
		return -1; 
	}
	else return slen;
}

// receive data from socket, with hdr
// return -1: error; bytes received
// hdr is a buffer to hold hdr, buf is a pointer to hold len bytes
// with jagmalloc inside
abaxint recvData( JAGSOCK sock, char *hdr, char *&buf )
{
	abaxint slen, slen2, len;
	memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
	slen2 = slen = _rayrecv( sock, hdr, JAG_SOCK_MSG_HDR_LEN ); 
	if ( slen < JAG_SOCK_MSG_HDR_LEN ) { 
		// prt(("recvData error 1=%d %d\n", sock, slen)); 
		return -1; 
	}
	len = jagatoll(hdr);
	if ( len < 0 ) { 
		// prt(("recvData error 1=%d hdr=%s\n", sock, hdr)); 
		return -1; 
	}
	else if ( len == 0 ) { return 0; }
	if ( buf ) { free( buf ); }
	buf = (char*)jagmalloc( len+1 );
	memset( buf, 0, len+1 );
	slen = _rayrecv( sock, buf, len );

	/**
	if ( *(hdr+JAG_SOCK_MSG_HDR_LEN-3) == 'H' && *(hdr+JAG_SOCK_MSG_HDR_LEN-2) == 'B' ) {
		// no print HB
	} else {
		prt(("s7890 socket=%d len=%lld slen=%lld recvDataAndHdr=[%s%.200s]\n", sock, len+JAG_SOCK_MSG_HDR_LEN, slen+slen2, hdr, buf));
	}
	**/

	if ( slen < len ) { 
		// prt(("recvData error 2=%d %d %d\n", sock, slen, len));
		free( buf ); buf = NULL; 
		return -1; 
	}

	return slen;
}

// receive data from socket, data only
// return -1: error; bytes received
abaxint recvData( JAGSOCK sock, char *buf, abaxint len )
{
	abaxint slen = _rayrecv( sock, buf, len );

	/**
	if ( slen > JAG_SOCK_MSG_HDR_LEN && *(buf+JAG_SOCK_MSG_HDR_LEN-3) == 'H' && *(buf+JAG_SOCK_MSG_HDR_LEN-2) == 'B' ) {
		// no print HB
	} else {
		prt(("s7891 socket=%d len=%lld slen=%lld recvDataAndHdr=[%.200s]\n", sock, len, slen, buf));
	}
	**/

	if ( slen < len ) { 
		return -1; 
	}

	return slen;
}

#ifdef _WINDOWS64_
// windows code
abaxint _raysend( JAGSOCK sock, const char *hdr, abaxint N )
{
    register abaxint bytes = 0;
    register abaxint len;
	int errcnt = 0;
	
	if ( N <= 0 ) { return 0; }
	if ( socket_bad( sock ) ) { return -1; } 
	len = ::send( sock, hdr, N, 0 );
	if ( len < 0 && WSAGetLastError() == WSAEINTR ) {
    	len = ::send( sock, hdr, N, 0 );
	}
	if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) { // send timeout
		++errcnt;
		while ( 1 ) {
    		jagsleep( 1, JAG_MSEC );
    		len = ::send( sock, hdr, N, 0 );
			if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
				++errcnt;
			} else break;
			if ( errcnt >= 3 ) break;
		}
		if ( errcnt >= 3 ) {
			// prt(("2000 _raysend Timeout or error() %d %d - %s\n",
			// sock, WSAGetLastError(), strerror(WSAGetLastError())));
			rayclose( sock );
			len = -1;
		}
	}

	if ( len < 0 ) {
		// prt(("2004 _raysend len is %d, sock %d, hdr=[%.100s], N %d MSG_NOS %d, errno=[%d]\n", 
		// len, sock, hdr, N, 0, WSAGetLastError()));
		if ( WSAGetLastError() == WSAENETRESET || WSAGetLastError() == WSAECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}

    bytes += len;
    while ( bytes < N ) {
        len = ::send( sock, hdr+bytes, N-bytes, 0 );
		if ( len < 0 && WSAGetLastError() == WSAEINTR ) {
        	len = ::send( sock, hdr+bytes, N-bytes, 0 );
		}

		errcnt = 0;
		if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) { // send timeout
			++errcnt;
			while ( 1 ) {
    			jagsleep( 1, JAG_MSEC );
        		len = ::send( sock, hdr+bytes, N-bytes, 0 );
				if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
					++errcnt;
				} else break;
				if ( errcnt >= 3 ) break;
			}
			if ( errcnt >= 3 ) {
				// prt(("2006 _raysend Timeout or error() %d %d\n",
				// sock, WSAGetLastError()));
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len < 0 ) {
            // prt(("2008 _raysend len is %d, sock %d, hdr=[%.100s], N %d MSG_NOS %d, errno=[%d]\n", 
			// len, sock, hdr, N, 0, WSAGetLastError()));
            if ( WSAGetLastError() == WSAENETRESET || WSAGetLastError() == WSAECONNRESET ) {
                rayclose( sock );
            }
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}

// windows code
abaxint _rayrecv( JAGSOCK sock, char *hdr, abaxint N )
{
    register abaxint bytes = 0;
    register abaxint len;
	int errcnt = 0;

	if ( N <= 0 ) { return 0; }
	if ( socket_bad( sock ) ) { return -1; } 
	len = ::recv( sock, hdr, N, 0 );
	if ( len < 0 && WSAGetLastError() == WSAEINTR ) {
    	len = ::recv( sock, hdr, N, 0 );
	}
	if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) { // recv timeout
		++errcnt;
		while ( 1 ) {
    		jagsleep( 1, JAG_MSEC );
    		len = ::recv( sock, hdr, N, 0 );
			if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
				++errcnt;
			} else break;
			if ( errcnt >= 3 ) break;
		}
		if ( errcnt >= 3 ) {
			// prt(("2100 _rayrecv Timeout or error() %d %d\n",
			// sock, WSAGetLastError()));
			rayclose( sock );
			len = -1;
		}
	}	

	// if len==0; remote end had a orderly shudown
	if ( len == 0 ) {
		// prt(("2102 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d DONE 0\n", 
		// len, sock, hdr, N));
		return 0;
	} else if ( len < 0 ) {
		// prt(("2104 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d errno=[%d] DONE -1\n", 
		// len, sock, hdr, N, WSAGetLastError()));
		if ( WSAGetLastError() == WSAENETRESET || WSAGetLastError() == WSAECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}

    bytes += len;
    while ( bytes < N ) {
        len = ::recv( sock, hdr+bytes, N-bytes, 0 );
		if ( len < 0 && WSAGetLastError() == WSAEINTR ) {
        	len = ::recv( sock, hdr+bytes, N-bytes, 0 );
		}
	
		errcnt = 0;
		if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) { // recv timeout
			++errcnt;
			while ( 1 ) {
    			jagsleep( 1, JAG_MSEC );
        		len = ::recv( sock, hdr+bytes, N-bytes, 0 );
				if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
					++errcnt;
				} else break;
				if ( errcnt >= 3 ) break;
			}
			if ( errcnt >= 3 ) {
				// prt(("2106 _rayrecv Timeout or error() %d %d\n",
				// sock, WSAGetLastError()));
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len <= 0 ) {
            // prt(("2108 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d errno=[%d] DONE -1\n", 
			// len, sock, hdr, N, WSAGetLastError()));
            if ( WSAGetLastError() == WSAENETRESET || WSAGetLastError() == WSAECONNRESET ) {
                rayclose( sock );
            }
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}
#else
// linux code
abaxint _raysend( JAGSOCK sock, const char *hdr, abaxint N )
{
    register abaxint bytes = 0;
    register abaxint len;
	int errcnt = 0;

	if ( N <= 0 ) { return 0; }
	if ( socket_bad( sock ) ) { return -1; } 
	len = ::send( sock, hdr, N, MSG_NOSIGNAL );
	if ( len < 0 && errno == EINTR ) {
    	len = ::send( sock, hdr, N, MSG_NOSIGNAL );
	}
	if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // send timeout
		++errcnt;
		while ( 1 ) {
    		jagsleep( 1, JAG_MSEC );
    		len = ::send( sock, hdr, N, MSG_NOSIGNAL );
			if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
				++errcnt;
			} else break;
			if ( errcnt >= 3 ) break;
		}
		if ( errcnt >= 3 ) {
			// prt(("2200 _raysend Timeout or error() %d %d - %s\n",
			// sock, errno, strerror(errno)));
			rayclose( sock );
			len = -1;
		}
	}	

	if ( len < 0 ) {
		// prt(("2204 _raysend len is %d, sock %d, hdr=[%.100s], N %d MSG_NOS %d, errnomsg=[%s] DONE -1\n", 
		// len, sock, hdr, N, MSG_NOSIGNAL, strerror(errno)));
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}

    bytes += len;
    while ( bytes < N ) {
        len = ::send( sock, hdr+bytes, N-bytes, MSG_NOSIGNAL );
		if ( len < 0 && errno == EINTR ) {
        	len = ::send( sock, hdr+bytes, N-bytes, MSG_NOSIGNAL );
		}

		errcnt = 0;
		if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // send timeout
			++errcnt;
			while ( 1 ) {
    			jagsleep( 1, JAG_MSEC );
        		len = ::send( sock, hdr+bytes, N-bytes, MSG_NOSIGNAL );
				if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
					++errcnt;
				} else break;
				if ( errcnt >= 3 ) break;
			}
			if ( errcnt >= 3 ) {
				// prt(("2206 _raysend Timeout or error() %d %d - %s\n",
				// sock, errno, strerror(errno)));
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len < 0 ) {
            // prt(("2208 _raysend len is %d, sock %d, hdr=[%.100s], N %d MSG_NOS %d, errnomsg=[%s]\n", 
			// len, sock, hdr, N, MSG_NOSIGNAL, strerror(errno)));
            if ( errno == ENETRESET || errno == ECONNRESET ) {
                rayclose( sock );
            }
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}

// linux code
abaxint _rayrecv( JAGSOCK sock, char *hdr, abaxint N )
{
    register abaxint bytes = 0;
    register abaxint len;
	int errcnt = 0;

	if ( N <= 0 ) { return 0; }
	if ( socket_bad( sock ) ) { return -1; } 
	len = ::recv( sock, hdr, N, 0 );
	if ( len < 0 && errno == EINTR ) {
    	len = ::recv( sock, hdr, N, 0 );
	}
	if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // recv timeout
		++errcnt;
		while ( 1 ) {
    		jagsleep( 1, JAG_MSEC );
    		len = ::recv( sock, hdr, N, 0 );
			if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
				++errcnt;
			} else break;
			if ( errcnt >= 3 ) break;
		}
		if ( errcnt >= 3 ) {
			// prt(("2300 _rayrecv Timeout or error() %d %d - %s\n",
			// sock, errno, strerror(errno)));
			rayclose( sock );
			len = -1;
		}
	}	

	// if len==0; remote end had a orderly shudown
	if ( len == 0 ) {
		// prt(("2302 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d DONE 0\n", 
		// len, sock, hdr, N));
		return 0;
	} else if ( len < 0 ) {
		// prt(("2304 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d errnomsg=[%s] DONE -1\n", 
		// len, sock, hdr, N, strerror(errno)));
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}

    bytes += len;
    while ( bytes < N ) {
        len = ::recv( sock, hdr+bytes, N-bytes, 0 );
		if ( len < 0 && errno == EINTR ) {
        	len = ::recv( sock, hdr+bytes, N-bytes, 0 );
		}
	
		errcnt = 0;
		if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // recv timeout
			++errcnt;
			while ( 1 ) {
    			jagsleep( 1, JAG_MSEC );
        		len = ::recv( sock, hdr+bytes, N-bytes, 0 );
				if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
					++errcnt;
				} else break;
				if ( errcnt >= 3 ) break;
			}
			if ( errcnt >= 3 ) {
				// prt(("2306 _rayrecv Timeout or error() %d %d - %s\n",
				// sock, errno, strerror(errno)));
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len <= 0 ) {
            // prt(("2308 _rayrecv len is %d, sock %d, hdr=[%.100s], N %d errnomsg=[%s] DONE -1\n", 
			// len, sock, hdr, N, strerror(errno)));
            if ( errno == ENETRESET || errno == ECONNRESET ) {
                rayclose( sock );
            }
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}
#endif


int jagmalloc_trim( abaxint n ) 
{
	#ifdef _WINDOWS64_
    return 1;
	#else
	int rc = malloc_trim( n );
    return rc; 
	#endif
}

FILE *loopOpen( const char *path, const char *mode )
{
	FILE *fp = NULL;
	while ( 1 ) {
		fp = ::fopen( path, mode );
		if ( fp ) {
			// prt(("s3370 fopen(%s)\n", path));
			return fp;
		}

		prt(("E0110 fopen fpath=[%s] mode=[%s] error=[%s], retry ...\n", path, mode, strerror(errno)));
		jagsleep(10, JAG_SEC);
	}

	return fp;
}

FILE *jagfopen( const char *path, const char *mode )
{
	FILE *fp = ::fopen( path, mode );
	if ( !fp ) {
		// prt(("E0111 fopen single fpath=[%s] mode=[%s] error=[%s]\n", path, mode, strerror(errno)));
	} else {
		// prt(("s3370 fopen(%s)\n", path));
	}
	return fp;
}

int jagfclose( FILE *fp )
{
	int rc = ::fclose( fp );
	if ( rc != 0 ) {
		// prt(("E0112 fclose error=[%s]\n", strerror(errno)));
	} else {
		// prt(("s3370 fclose()\n"));
	}
	return rc;
}

int jagopen( const char *path, int flags )
{
	#ifdef _WINDOWS64_
	flags = flags | O_BINARY;
	#endif
	int rc = ::open( path, flags );
	if ( rc < 0 ) {
		// prt(("s3371 open(%s) error [%s]\n", path, strerror( errno )));
	} else {
		// prt(("s3372 open(%s)\n", path));
	}
	
	return rc;
}

int jagopen( const char *path, int flags, mode_t mode )
{
	int rc = ::open( path, flags, mode );
	if ( rc < 0 ) {
		// prt(("s3371 open(%s) error [%s]\n", path, strerror( errno )));
	} else {
		// prt(("s3372 open(%s)\n", path));
	}
	
	return rc;
}


int jagclose( int fd )
{
	int rc = ::close( fd );
	if ( rc < 0 ) {
		// prt(("s3373 close(%d) error [%s]\n", fd, strerror( errno )));
	} else {
		// prt(("s3374 close(%d)\n", fd));
	}
	return rc;
}

int jagaccess( const char *path, int mode )
{
	int rc = ::access( path, mode );
	if ( rc < 0 ) {
		// prt(("s3375 access(%s) error [%s]\n", path, strerror( errno )));
	} else {
		// prt(("s3376 access(%s)\n", path));
	}
	return rc;
}

int jagunlink( const char *path )
{
	int rc = ::unlink( path );
	if ( rc < 0 ) {
		// prt(("s3377 unlink(%s) error [%s]\n", path, strerror( errno )));
	} else {
		// prt(("s3378 unlink(%s)\n", path));
	}

	return rc;
}

int jagrename( const char *path, const char *newpath )
{
#ifdef _WINDOWS64_
	jagunlink( newpath );
#endif
	int rc = ::rename( path, newpath );
	if ( rc < 0 ) {
		prt(("s3379 rename(%s   ->   %s) error [%s]\n", path, newpath, strerror( errno )));
	} else {
		// prt(("s3380 rename(%s   ->   %s)\n", path, newpath));
	}
	return rc;
}

#ifdef _WINDOWS64_
int jagftruncate( int fd, __int64 size )
{
	errno_t rc = _chsize( fd, size );
	if ( rc != 0 ) {
		prt(("s3381 _chsize(%d) error [%s]\n", fd, strerror( rc )));
	} else {
		// prt(("s3382 _chsize(%d)\n", fd));
	}
	return rc;
}
#else 
int jagftruncate( int fd, off_t length )
{
	int rc = ::ftruncate( fd, length );
	if ( rc < 0 ) {
		prt(("s3383 ftruncate(%d) error [%s]\n", fd, strerror( errno )));
	} else {
		// prt(("s3384 ftruncate(%d)\n", fd));
	}
	return rc;
}
#endif

// mode ( input type ): 0: seconds; 1: milliseconds, 2 or else: microseconds
void jagsleep( abaxint time, int mode )
{
	#ifdef _WINDOWS64_
	if ( JAG_SEC == mode ) Sleep( time*1000 );
	else if ( JAG_MSEC == mode ) Sleep( time );
	else if ( JAG_USEC == mode ) {
		abaxint t = time/1000;
		if ( t < 1 ) t = 1;
		Sleep( t );
	}
	#else
	if ( JAG_SEC == mode ) sleep( time );
	else if ( JAG_MSEC == mode ) usleep( time*1000 );
	else if ( JAG_USEC == mode ) usleep( time );
	#endif
}

const char *jagstrrstr(const char *haystack, const char *needle)
{
	if ( ! needle ) return haystack;

	const char *p;
	const char *r = NULL;
	if (!needle[0]) return (char*)haystack + strlen(haystack);

	while (1) {
		p = strstr(haystack, needle);
		if (!p) return r;
		r = p;
		haystack = p + 1;
	}
	return NULL;
}

#ifdef _WINDOWS64_
abaxint jagsendfile( JAGSOCK sock, int fd, abaxint size )
{
	char buf[1024*1024];
	lseek(fd, 0, SEEK_SET );
	abaxint sz = 0;
	abaxint n = 0;
	abaxint snd;
	while (  ( n= ::read(fd, buf, 1024*1024 ) ) > 0 ) {
		snd = raysafewrite( sock, buf, n );
		if ( snd < n ) {
			lseek(fd, 0, SEEK_SET );
			return -1;
		}

		sz += snd;
	}
	return sz;
}
#else
#include <sys/sendfile.h>
abaxint jagsendfile( JAGSOCK sock, int fd, abaxint size )
{
	abaxint sz = ::sendfile( sock, fd, NULL, size );
	if ( sz < 0 ) {
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}
	else if ( sz < size ) {
		abaxint sz2;
		while ( 1 ) {
			if ( size-sz <= 0 ) break;
			sz2 = ::sendfile( sock, fd, NULL, size-sz );
			if ( sz2 < 0 ) {
				prt((" jagsendfile error tid=%lld sz=%lld size=%lld errno=%d [%s]\n", THREADID, sz, size, errno, strerror(errno)));
				if ( errno == ENETRESET || errno == ECONNRESET ) {
					rayclose( sock );
				}
				return -1;
			}
			sz += sz2;
		}
	}
	return sz;
}
#endif


AbaxDataString psystem( const char *command )
{
	AbaxDataString res;
	FILE *fp = popen(command, "r" );
	if ( ! fp ) {
		res = AbaxDataString("Error execute ") + command; 
		return res;
	}

	char buf[1024];
	while ( NULL != fgets(buf, 1024, fp ) ) {
		res += buf;
	}

	pclose( fp );
	return res;
}

// both arguments are in seconds
bool checkCmdTimeout( abaxint startTime, abaxint timeoutLimit )
{
	if ( timeoutLimit < 0 ) return false;
	struct timeval now;
	gettimeofday( &now, NULL ); 
	if ( now.tv_sec - startTime >= timeoutLimit ) return true;
	return false;
}


// Given name, output value after the '=' sign
// content NULL, or "", or "name=value" or "name1=value1/name2=value2/"
// caller must free strdup string
char *getNameValueFromStr( const char *content, const char *name )
{
	if ( ! content ) return NULL;
	if ( *content == '\0' ) return NULL;
	const char *p = strstr( content, name );
	if ( ! p ) return NULL;
	while ( *p != '\0' && *p != '=' ) ++p;
	if ( *p == '\0' ) return NULL;
	++p; // pass '='
	const char *start = p;
	while ( *p != '\0' && *p != '/' ) ++p;
	#ifndef _WINDOWS64_
	return strndup( start, p-start );
	#else
		int len =  p-start;
		char *p2 = jagmalloc( len+1);
		memcpy(p2, start, len );
		p2[len] = '\0';
		return p2;
	#endif
}

/**
On success, getline() return the number of characters read, but not  including
the terminating null byte.  This value can NOT be used to handle embedded null bytes in the line read.
Functions return -1  on failure to read a line (including end-of-file condition).
***/
ssize_t jaggetline(char **lineptr, size_t *n, FILE *stream)
{
	#ifndef _WINDOWS64_
		return getline( lineptr, n, stream );
	#else
    ssize_t count = 0;
    char c;
	AbaxDataString out;
    while ( 1) {
    	c = (char)getc(stream);
    	// if ( '\n' == c || EOF == c ) break;
    	if ( EOF == c ) break;
		out += c;
		++count;
		if ( '\n' == c ) { break;}
    }

	*n = count;
	if ( count < 1 && EOF == c ) {
		*lineptr = strdup("");
		return -1;
	}

	*lineptr = strdup( out.c_str() );
    return  count;
	#endif
}


/**
int daemonInit()
{
    pid_t  pid;
    if ( (pid=fork()) < 0 ) {
        return -1;
    } else if ( pid != 0 ) {
        exit(0);  // parent bye-bye
    }

    setsid();
    umask(0);
    return 0;
}
**/


AbaxDataString expandEnvPath( const AbaxDataString &path )
{
	// prt(("s29393 expandEnvPath input=[%s]\n", path.c_str() ));
    if ( '$' != *(path.c_str()) ) return path;

    JagStrSplit sp( path, '/' );
    AbaxDataString fs = sp[0];
	const char *p = fs.c_str() + 1;
	// prt(("s2238 fs=[%s] p=[%s]\n", fs.c_str(), p ));
	if ( getenv(p) ) {
		return AbaxDataString(getenv(p)) + AbaxDataString(path.c_str() + fs.size());
	} else {
		return AbaxDataString(".") + AbaxDataString(path.c_str() + fs.size());
	}
}

#ifdef _WINDOWS64_
struct tm *jag_localtime_r(const time_t *timep, struct tm *result)
{
	localtime_s (result, timep);
	return result;
}
char *jag_ctime_r(const time_t *timep, char *result)
{
	ctime_s(result, JAG_CTIME_LEN, timep);
	return result;
}

#else
struct tm *jag_localtime_r(const time_t *timep, struct tm *result)
{
	return localtime_r( timep, result );
}
char *jag_ctime_r(const time_t *timep, char *result)
{
	return ctime_r( timep, result );
}

#endif

int formatInsertSelectCmdHeader( const JagParseParam *parseParam, AbaxDataString &str )
{
	if ( JAG_INSERTSELECT_OP == parseParam->opcode ) {
		str = "insert into " + parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
		if ( parseParam->otherVec.size() > 0 ) {
			str += " (";
			for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
				if ( 0 == i ) {
					str += parseParam->otherVec[i].objName.colName;
				} else {
					str += ", " + parseParam->otherVec[i].objName.colName;
				}
			}
			str += " ) ";
		} else {
			str += " ";
		}
		return 1;
	}
	return 0;
}

// true: name is "wjdjdAJ71828_jdjd"
bool isValidVar( const char *name )
{
	while ( *name ) {
		//if ( ! isalnum( *name ) ) {
		if ( ! isValidNameChar( *name ) ) {
			return false;
		}
		++name;
	}
	return true;
}

void stripEndSpace( char *qstr, char endch )
{
	if ( NULL == qstr || *qstr == '\0' ) return;
	char *start = qstr;
	while ( *qstr ) ++qstr;
	--qstr;

	// qstr is at E\0
	while ( qstr != start ) {
		if ( isspace(*qstr) || *qstr == endch ) {
			*qstr = '\0';
		} else {
			break;
		}
		--qstr;
	}
}

#ifdef _WINDOWS64_
void getWinPass( char *pass )
{
    char c = 0;
    int i = 0;
    while ( c != 13 && i < 20 ) {
        c = _getch();
        pass[i++] = c;
    }
}
#endif

// Parse error msg from _END_[T=ddd|E=Error 1002 Syntax error in ...|X=...]
// NULL: if no error
// jagmalloced string containing error message, must tbe freeed.
abaxint _getFieldInt( const char * rowstr, char fieldToken )
{
	char newbuf[30];
	const char *p; 
	const char *start, *end;
	int len;
	char  feq[3];

	sprintf(feq, "%c=", fieldToken );  // fieldToken: E   "E=" in _row
	p = strstr( rowstr, feq );
	if ( ! p ) {
		return 0;
	}

	start = p +2;
	end = strchr( start, '|' );
	if ( ! end ) {
		end = strchr( start, ']' );
	}

	if ( ! end ) return 0;

	len = end - start;
	if ( len < 1 ) {
		return 0;
	}

	memcpy( newbuf, start, len );
	newbuf[len] = '\0';

	return jagatoll(newbuf);
}

// Input: options: "a=v  b=3  c=44"
// Output: hash map omap
void makeMapFromOpt( const char *options, JagHashMap<AbaxString, AbaxString> &omap )
{
	if ( NULL == options ) return;
	if ( '\0' == *options ) return;

	JagStrSplit sp( options, ' ', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		JagStrSplit sp2( sp[i], '=' );
		if ( sp2.length() < 2 ) continue;
		omap.addKeyValue ( sp2[0], sp2[1] );
	}
}


// if dquote is 1: return "a", "v", "xx"
// else   1, 2, 3, 92
AbaxDataString makeStringFromOneVec( const JagVector<AbaxDataString> &vec, int dquote )
{
	AbaxDataString res;
	int len = vec.length();
	for ( int i =0; i < len; ++i ) {
		if ( dquote ) {
			res += AbaxDataString("\"") + vec[i] + "\"";
		} else {
			res += vec[i];
		}

		if ( i < len-1 ) {
			res += ",";
		} 
	}
	return res;
}


// "[1.2], [2, 3] ..."
AbaxDataString makeStringFromTwoVec( const JagVector<AbaxDataString> &xvec, const JagVector<AbaxDataString> &yvec )
{
	AbaxDataString res;
	int len = xvec.length();
	if ( len != yvec.length() ) return "";
	for ( int i =0; i < len; ++i ) {
		res += AbaxDataString("[") + xvec[i] + "," + yvec[i] + "]";
		if ( i < len-1 ) { res += ","; } 
	}
	return res;
}

int oneFileSender( JAGSOCK sock, const AbaxDataString &inpath )
{
	//prt(("\ns4009 oneFileSender sock=%d THRD=%lld inpath=[%s] ...\n", sock, THREADID, inpath.c_str() ));
	int fileSuccess = 0; 
	AbaxDataString filename;
	char *newbuf = NULL; char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	ssize_t rlen = 0; struct stat sbuf; AbaxDataString cmd; int fd = -1;
	abaxint filelen;

	//prt(("s2838 oneFileSender inpath=[%s]\n", inpath.c_str() ));
    const char *p = strrchr( inpath.c_str(), '/' );
    if ( p == NULL ) {
		p = inpath.c_str();
		filename = p;
    } else {
		filename = p+1;
	}

	//prt(("s0384 filename=[%s] inpath=[%s]\n", filename.c_str(), inpath.c_str() ));
	rlen = 0;
	if ( inpath == "." || inpath.size() < 1 || filename.size() < 1 ) {
		rlen = -1;
	} else {
    	if ( 0 == stat(inpath.c_str(), &sbuf) && sbuf.st_size > 0 ) {
    		fd = jagopen( inpath.c_str(), O_RDONLY, S_IRWXU);
    		if ( fd < 0 ) { rlen = -2; }
    	} else { rlen = -3; }
	}

	int sendFakeData;
	if ( rlen < 0 ) {
		sendFakeData = 1;
		filelen = 0;
		//prt(("s0283 sendFakeData = 1\n" ));
		filename = ".";
	} else {
		sendFakeData = 0;
		filelen = sbuf.st_size;
		//prt(("s0283 sendFakeData = 0\n" ));
	}

	cmd = "_onefile|" + filename + "|" + longToStr( filelen ) + "|" + longToStr(THREADID);
	char cmdbuf[cmd.size()+JAG_SOCK_MSG_HDR_LEN+1]; memset( cmdbuf, 0, cmd.size()+JAG_SOCK_MSG_HDR_LEN+1);
	memcpy( cmdbuf+JAG_SOCK_MSG_HDR_LEN, cmd.c_str(), cmd.size() );
	sprintf( cmdbuf, "%0*lld", JAG_SOCK_MSG_HDR_LEN-4, cmd.size() );
	memcpy( cmdbuf+JAG_SOCK_MSG_HDR_LEN-4, "ATFC", 4 );

	//prt(("s2309 sendData cmdbuf=[%s]\n", cmdbuf ));
	rlen = sendData( sock, cmdbuf, JAG_SOCK_MSG_HDR_LEN+cmd.size() ); // 016ABCDmessage client query mode
	if ( rlen < JAG_SOCK_MSG_HDR_LEN+cmd.size() ) {
		prt(("s0822 sendData error\n" ));
		sendFakeData = 1;
	}

	if ( sendFakeData ) {
		// send 1 byte to server
		//raysafewrite( sock, "0", 1 );
	} else {
		//prt(("s2293 jagsendfile ...\n" ));
		beginBulkSend( sock );
		rlen = jagsendfile( sock, fd, sbuf.st_size );
		endBulkSend( sock );
		//prt(("s2293 jagsendfile done\n" ));
	}

	if ( fd >= 0 ) jagclose( fd );
	if ( newbuf ) free( newbuf );
	return 1;
}


// return 1: OK
//   -1: error not _onefile command
//   -2: error invalid _onefile command
//   -3: fake data  -4: file open error
int oneFileReceiver( JAGSOCK sock, const AbaxDataString &outpath, bool isDirPath )
{	
	// prt(("c0293 oneFileReceiver outpath=[%s]\n", outpath.c_str() ));
	int fd = -1; 
	abaxint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT;
	AbaxDataString filename, senderID, recvpath;
	char *newbuf = NULL; 
	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	ssize_t rlen = 0;

	// prt(("s3028 recvData() ...\n" ));
	while ( 1 ) {
		memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
    	rlen = recvData( sock, hdr, newbuf );
    	// prt(("s3028 recvData() done hdr=[%s] newbuf=[%s] ...\n", hdr, newbuf ));
		if ( 0 == rlen ) {
			continue;
		}

		if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) {
			continue;
		}

    	if ( 0 != strncmp( newbuf, "_onefile|", 9 ) ) {
    		prt(("s0293 _onefile error newbuf=[%s]\n", newbuf ));
    		if ( newbuf ) free(newbuf );
    		return -1;
    	} else {
			break; // got _onefile
		}
	}

	// if _onefile recved, parse and check the outpath is valid to recv
	JagStrSplit sp( newbuf, '|', true );
	filename = sp[1];
	// get fake data
	if ( filename == "." ) {
		char c;
		// recvData( sock, &c, 1 );
		return -3;
	}

	fsize = jagatoll(sp[2].c_str());
	senderID = sp[3];
	if ( isDirPath ) recvpath = outpath + "/" + filename;
	else recvpath = outpath;
	fd = jagopen( recvpath.c_str(), O_CREAT|O_RDWR|O_TRUNC, S_IRUSR|S_IWUSR );
	if ( fd < 0 ) {
		prt(("s0394 jagopen(recvpath=[%s]) error \n",  recvpath.c_str() ));
		if ( newbuf ) free(newbuf );
		// return -4;
	}

	// else, begin to recv actual file materials
	char *mbuf =(char*)jagmalloc(memsize);
	while( 1 ) {
		if ( totlen >= fsize ) break;
		if ( fsize-totlen < memsize ) {
			recvlen = fsize-totlen;
		} else {
			recvlen = memsize;
		}
		rlen = recvData( sock, mbuf, recvlen );
		if ( rlen < recvlen ) {
			rlen = -1;
			break;
		}
		// rlen = raysafewrite( fd, mbuf, rlen );
		raysafewrite( fd, mbuf, rlen );
		totlen += rlen;
	}
	// prt(("s1239 end recv file totlen=%d\n", totlen ));

	jagfdatasync( fd ); jagclose( fd );
	/***
	prt(("s1239 end recv file totlen=%d\n", totlen ));
	prt(("s2736 send _filedone to client ...\n" ));
	sendDirectToSock(sock, "_filedone", 0);
	prt(("s2736 send _filedone to client done\n" ));
	***/

	if ( mbuf ) free( mbuf );
	if ( newbuf ) free( newbuf );
	return 1;
}

// method to send data directly to sock
abaxint sendDirectToSock( JAGSOCK sock, const char *mesg, abaxint len, bool nohdr )
{
	abaxint rlen;
	if ( nohdr ) {
		rlen = sendData( sock, mesg, len ); // 016ABCDmessage client query mode
	} else {
		char *buf =(char*)jagmalloc(len+JAG_SOCK_MSG_HDR_LEN+1);
		memset( buf, 0, len+JAG_SOCK_MSG_HDR_LEN+1 );
		memcpy( buf+JAG_SOCK_MSG_HDR_LEN, mesg, len );
		sprintf( buf, "%0*lld", JAG_SOCK_MSG_HDR_LEN-4, len );
		memcpy( buf+JAG_SOCK_MSG_HDR_LEN-4, "ATFC", 4 );
		rlen = sendData( sock, buf, len+JAG_SOCK_MSG_HDR_LEN ); // 016ABCDmessage client query mode
		// prt(("c9910 sendDirectToSock sendData buf=[%s] len=%d rlen=%d\n", buf, len, rlen ));
		if ( buf ) free( buf );
	}
	return rlen;
}

// method to recv data directly from sock
abaxint recvDirectFromSock( JAGSOCK sock, char *&buf, char *hdr )
{
	abaxint rlen = 0;
	while ( 1 ) {
		rlen = recvData( sock, hdr, buf );
		// prt(("c2039 hdr=[%s] buf=[%s]\n", hdr, buf ));
		if ( rlen > 0 && (hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B') ) { 
		    // heartbit alive, ignore
			continue;
		}
		break;
	}
	return rlen;
}

// method to send data directly to sock
// len is length of mesg, not including hdr
// if original data is compressed ( checked from orig hdr ), then rebuild mesg with compressed data
abaxint sendDirectToSockWithHdr( JAGSOCK sock, const char *hdr, const char *mesg, abaxint len )
{
	const char *p = mesg;
	abaxint rlen;
	AbaxDataString comp;
	if ( hdr[JAG_SOCK_MSG_HDR_LEN-4] == 'Z' ) {
		JagFastCompress::compress( mesg, len, comp );
		p = comp.c_str();
		len = comp.size();
    }

	rlen = sendData( sock, hdr, JAG_SOCK_MSG_HDR_LEN );
	rlen = sendData( sock, p, len );
	return rlen;
}

// returns 2: if there is E e in string
// 1: if there is no E or E
// 0: if none of above
int isValidSciNotation(const char *str )
{
	int rc = 1;
	while ( *str != '\0' ) {
		if ( isdigit(*str) || '.' == *str || 'e' == *str || 'E' == *str 
		     || '-' == *str || '+' == *str ) {
		 	++str;
			if ( 'e' == *str || 'E' == *str ) { rc = 2; }
		 } else {
		 	return 0;
		 }
	}
	return rc;
}

// hash fstr and return  "ddd/ddd"
AbaxDataString fileHashDir( const AbaxFixString &fstr )
{
	abaxint hcode = fstr.hashCode();
	char buf[8];
	sprintf( buf, "%d/%d", hcode % 1000, (hcode/1000) % 1000 ); 
	return buf;
}

char lastChar( const AbaxFixString &str )
{
	return *( str.c_str() + str.size()-1);
}

// ignore NULL in str
void jagfwrite( const char *str, abaxint len, FILE *outf )
{
	for ( int i = 0; i < len; ++i ) {
		if ( str[i] != '\0' ) {
			fputc( str[i], outf );
		}
	}
}

// ignore leading zeros and  NULL in str
void jagfwritefloat( const char *str, abaxint len, FILE *outf )
{
	if ( NULL == str || '\0' == *str || len <1 ) return;
	bool leadzero = false;
	int start = 0;
	if ( str[0] == '+' || str[0] == '-' ) {
		start = 1;
		fputc( str[0], outf );
	}

	if ( str[start] == '0' && str[start+1] != '\0' ) leadzero = true;
	for ( int i = start; i < len; ++i ) {
		if ( str[i] == '\0' ) continue;
		if ( str[i] != '0' || ( str[i] == '0' && str[i+1] == '.' ) ) leadzero = false;
		if ( ! leadzero ) {
			fputc( str[i], outf );
		}
	}
}

void charFromStr( char *dest, const AbaxDataString &src )
{
	strcpy( dest, src.c_str() );
}

bool streq( const char *s1, const char *s2 )
{
	if ( 0 == strcmp( s1, s2 ) ) {
		return true;
	}

	return false;
}

bool isValidNameChar( char c )
{
    if ( c == '+' ) return false;
    if ( c == '-' ) return false;
    if ( c == '*' ) return false;
    if ( c == ' ' ) return false;
    if ( c == '\t' ) return false;
    if ( c == '/' ) return false;
    if ( c == '\\' ) return false;
    if ( c == '\n' ) return false;
    if ( c == '\0' ) return false;
    if ( c == '=' ) return false;
    if ( c == '!' ) return false;
    if ( c == '(' ) return false;
    if ( c == ')' ) return false;
    if ( c == '|' ) return false;
    if ( c == '[' ) return false;
    if ( c == ']' ) return false;
    if ( c == '{' ) return false;
    if ( c == '}' ) return false;
    if ( c == ';' ) return false;
    if ( c == ',' ) return false;
    if ( c == ':' ) return false;

    return true;
}

long jagatol(const char *nptr)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0;
	return atol( nptr );
}

long long jagatoll(const char *nptr)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0;
	return atoll( nptr );
}

long double jagstrtold(const char *nptr, char **endptr)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0.0;
	return strtold( nptr, endptr );
}

double jagatof(const char *nptr)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0.0;
	return atof( nptr);
}

double jagatof(const AbaxDataString &str )
{
	return jagatof( str.c_str() );
}


int jagatoi(const char *nptr)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0;
	return atoi( nptr);
}

int jagatoi(char *nptr, int len)
{
	if ( NULL == nptr || '\0' == *nptr ) return 0;
	if ( len < 0 ) return 0;
	char save = nptr[len];
	nptr[len] = 0;
	int n = atoi( nptr);
	nptr[len] = save;
	return n;
}

void appendOther( JagVector<OtherAttribute> &otherVec,  int n, bool isSub )
{
	OtherAttribute other; 
	other.issubcol = isSub;
	for ( int i=0; i < n; ++i ) {
		otherVec.append( other ); 
	}
}

// trim tailing zeroes but leave first zero untouched
void stripTailZeros( char *buf, int len )
{
	if ( len < 1 ) return;
	if ( NULL == buf || *buf == '\0' ) return;
	char *p = buf+len-1;
	//prt(("c1000 begin stripTailZeros:\n"));
	while ( p >= buf+1 ) {
		//prt(("c4101 lookat p=[%s] buf=[%s] *buf=[%c]\n", p, buf, *buf ));
		if ( *(p-1) != '.' && *p == '0' ) {
		//if ( *p == '0' || *p == '.' ) {
			*p = '\0';
			//prt(("c2003 set *p to null\n" ));
		} else {
			//prt(("c2004 *p=[%c] is no 0 or . break\n", *p ));
			break;
		}
		--p;
	}
	//prt((":c1001 end stripTailZeros\n"));

	if ( buf[0] == '.' && buf[1] == '\0' ) buf[0] = '0';
}

bool jagisspace( char c)
{
	// if ( ' ' == c  || '\t' == c  || '\r' == c || '\f' == c || '\v' == c ) return true;
	if ( ' ' == c  || '\t' == c  || '\r' == c  ) return true;
	return false;
}

#if 1
// input: str: "00000034.200000"   "00003445.0000"
// output:  "34.2"  "3445"
AbaxDataString trimEndZeros( const AbaxDataString& str )
{
	if ( ! strchr( str.c_str(), '.') ) return str;
	AbaxDataString res;

	// skip leading zeros
	int len = str.size();
	if ( len < 1 ) return "";
	int start=0;
	bool leadzero = false;
	if ( str[0] == '+' || str[0] == '-' ) {
		res += str[0];
		start = 1;
	}

	if ( str[start] == '0' && str[start+1] != '\0' ) leadzero = true;
	for ( int i = start; i < len; ++i ) {
		if ( str[i] != '0' || ( str[i] == '0' && str[i+1] == '.' ) ) leadzero = false;
		if ( ! leadzero ) {
			res += str[i];
		}
	}
	//prt(("u1200 trimEndZeros res=[%s]\n", res.c_str() ));

	//  trim tail zeros
	len = res.size();
	if ( len < 1 ) return "";
	char *buf = strndup( res.c_str(), len );
	//prt(("u12001 trimEndZeros buf=[%s]\n", buf ));
	char *p = buf+len-1;
	while ( p >= buf+1 ) {
		if ( *(p-1) != '.' && *p == '0' ) {
			*p = '\0';
		} else {
			break;
		}
		--p;
	}
	if ( buf[0] == '.' && buf[1] == '\0' ) buf[0] = '0';
	//prt(("u2231 trimEndZeros buf=[%s]\n", buf ));
	res = buf;
	free( buf );
	return res;
}
#endif

void dumpmem( const char *buf, int len )
{
	for ( int i=0; i < len; ++i ) {
		if ( buf[i] == '\0' ) {
			printf("@");
		} else {
			printf("%c", buf[i]);
		}
	}
	printf("\n");
	fflush(stdout);
}

// for KMPstrstr
// int M = strlen(pat);
void prepareKMP(const char *pat, int M, int *lps)
{
    int len = 0;
    lps[0] = 0; // lps[0] is always 0
    int i = 1;
    while (i < M) {
        if (pat[i] == pat[len]) {
            len++;
            lps[i] = len;
            i++;
        } else {
            if (len != 0) {
                len = lps[len-1];
            } else {
                lps[i] = 0;
                i++;
            }
        }
    }
}

const char *KMPstrstr(const char *txt, const char *pat)
{
	if ( ! txt || ! pat ) return NULL;
	if ( 0 == *txt ) return NULL;
	if ( 0 == *pat  ) return txt;
    int N = strlen(txt);
    int M = strlen(pat);
	if ( M > N ) return NULL;
    int lps[M];
    prepareKMP(pat, M, lps);
    int i = 0, j=0;
    while (i < N) {
        if (pat[j] == txt[i]) {
            j++; i++;
        }
 
        if (j == M) {
            //printf(&quot;Found pattern at index %d n&quot;, i-j);
            //j = lps[j-1];
			return &(txt[i-j]);
        }
 
        // mismatch after j matches
        if (i < N  &&  pat[j] != txt[i] ) {
            if (j != 0) j = lps[j-1];
            else i = i+1;
        }
    }

	return NULL;
}

AbaxDataString replaceChar( const AbaxDataString& str, char oldc, char newc )
{
	AbaxDataString res;
	for ( int i=0; i < str.size(); ++i ) {
		if ( str[i] == oldc ) {
			res += newc;
		} else {
			res += str[i];
		}
	}
}

void printStr( const AbaxDataString &str )
{
	printf("printStr size=%d: \n[", str.size() );
	for ( int i=0; i < str.size(); ++i ) {
		if ( str[i] == '\0' ) {
			putchar('@');
		} else {
			putchar( str[i] );
		}
	}
	printf("]\n\n");
}

char *secondTokenStart( const char *str, char sep )
{
	if ( NULL == str || *str == 0 ) return NULL;
	char *p = (char*) str;
	while ( *p == sep ) ++p;  // "  abc  cdef"  p is at a
	while ( *p != sep && *p != '\0' ) ++p; // p is at pos after c
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;  // p is at c
	return p;
}

//  xm, xh, xd, xw
abaxint convertToSecond( const char *p )
{
	abaxint num, n = jagatol(p);
    if ( strchr(p, 'm') || strchr(p, 'M')  ) {
          num = n * 60; // tosecond
    } else if (  strchr(p, 'h') || strchr(p, 'H') ) {
          num = n * 360; // tosecond
    } else if (  strchr(p, 'd') || strchr(p, 'D') ) {
          num = n * 86400; // tosecond
    } else if (  strchr(p, 'w') || strchr(p, 'W') ) {
          num = n * 86400 *7; // tosecond
    } else {
          num = -1;
    }
	return num;
}

//  xm, xh, xd, xs, xw
abaxint convertToMicroSecond( const char *p )
{
	abaxint num, n = jagatol(p);
    if ( strchr(p, 'm') || strchr(p, 'M')  ) {
          num = n * 60000000; 
    } else if (  strchr(p, 'h') || strchr(p, 'H') ) {
          num = n * 360000000; 
    } else if (  strchr(p, 'd') || strchr(p, 'D') ) {
          num = n * 86400000000; 
    } else if (  strchr(p, 's') || strchr(p, 'S') ) {
          num = n * 1000000; 
    } else if (  strchr(p, 'w') || strchr(p, 'W') ) {
          num = n * 86400000000 *7; 
    } else {
          num = -1;
    }
	return num;
}

// str: "jfkd jkfjdkjf djfkd"
// like:  "pat"  "%pat"  "pat%"  "%pat%"
bool likeMatch( const AbaxDataString& str, const AbaxDataString& like )
{
	//prt(("u2208 likeMatch str=[%s] like=[%s]\n", str.c_str(), like.c_str() ));
	if ( like.size() < 1 ) {
		return false;
	}

	char firstc = *(like.c_str());
	char endc = *(like.c_str()+ like.size()-1);
	//prt(("u1102 like firsc=[%c]  endc=[%c]\n", firstc, endc ));
	if ( firstc == '%' && endc == '%' ) {
		char *p =  (char*)(like.c_str() + like.size() - 1);
		*p = '\0';
		if ( strstr( str.c_str(), like.c_str()+1 ) ) {
			*p = endc;
			return true;
		}
		*p = endc;
	} else if ( firstc == '%' ) {
		if ( lastStrEqual(str.c_str(), like.c_str()+1, str.size(), like.size()-1 ) ) {
			return true;
		} 
	} else if ( endc == '%' ) {
		if ( 0 == strncmp( str.c_str(), like.c_str(), like.size()-1 ) ) {
			return true;
		}
	} else {
		if ( str == like ) return true;
	}

	return false;
}


#define MINVAL3(a, b, c) ((a) < (b) ? ((a) < (c) ? (a) : (c)) : ((b) < (c) ? (b) : (c)))
int levenshtein(const char *s1, const char *s2) 
{
    unsigned int s1len, s2len, x, y, lastdiag, olddiag;
    s1len = strlen(s1);
    s2len = strlen(s2);
    unsigned int column[s1len+1];
    for (y = 1; y <= s1len; y++) column[y] = y;
    for (x = 1; x <= s2len; x++) {
        column[0] = x;
        for (y = 1, lastdiag = x-1; y <= s1len; y++) {
            olddiag = column[y];
            column[y] = MINVAL3(column[y] + 1, column[y-1] + 1, lastdiag + (s1[y-1] == s2[x-1] ? 0 : 1));
            lastdiag = olddiag;
        }
    }
    return(column[s1len]);
}
