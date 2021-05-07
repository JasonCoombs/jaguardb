/*
 * Copyright (C) 2018,2019,2020,2021 DataJaguar, Inc.
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
#include <math.h>
#include <malloc.h>
#include <libgen.h>

#ifndef _WINDOWS64_
#include <termios.h>
#else
#include <direct.h>
#include <io.h>
#include <conio.h>
#endif

#include <abax.h>
#include <JagLogLevel.h>
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
/***
char *jumptoEndBracket(const char *p) 
{
	char *q = (char*)p + 1;
	while( 1 ) {
		if ( *q == '\0' || *q == ')' ) break;
		++q;
	}
	return q;
}
***/

// method to remove quote from given string; e.g. 'apple' -> apple ; "apple" -> apple ;
// instr beginning with quote which you want to remove; e.g. ' or "
Jstr strRemoveQuote( const char *p )
{
	Jstr str = p;
	if ( p == NULL || *p == '\0' ) {
	} else if ( *p != '\'' && *p != '"' && *p != '`' ) {
		str = p;
	} else {
		char *q = jumptoEndQuote( p ); ++p;
		if ( *p == '\0' || *q == '\0' || q-p <= 0 ) {
		} else {
			str = Jstr( p, q-p, q-p );
			//str = Jstr( p, q-p );
		}
	}
	return str;
}	

//static char *itostr( int i, char *buf );
//static char *ltostr( jagint i, char *buf );
void raydebug( FILE *outf, int level, const char *fmt, ... )
{
    const char *p;
    va_list 	argp;
    jagint 	i;
    char 		*s;
    char 		fmtbuf[256];
	char        tstr[22];

	if ( JAG_LOG_LEVEL < 1 ) JAG_LOG_LEVEL = 1;
	if ( level > JAG_LOG_LEVEL ) {
		return;
	}

	memset(tstr, 0, 22);
	time_t  now = time(NULL);
	struct tm  *tmp;
	struct tm  result;
	tmp = jag_localtime_r( &now, &result );
	strftime( tstr, 22, "%Y-%m-%d %H:%M:%S", tmp );
	pthread_t tid = pthread_self();
	fprintf( outf, "%s 0x%0x ", tstr, tid ); 
    
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
    			i = va_arg(argp, jagint);
    			s = ltostr(i, fmtbuf );
    			fputs(s, outf);
    			break;
    		case 'x':
    			i = va_arg(argp, jagint);
    			s = xtostr(i, fmtbuf );
    			fputs(s, outf);
    			break;
    		case 'u':
    			i = va_arg(argp, jagint);
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
    			i = va_arg(argp, jagint);
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

char *ltostr( jagint i, char *buf )
{
	sprintf( buf, "%lld", i );
	return buf;
}

char *xtostr( jagint i, char *buf )
{
	sprintf( buf, "%0x", i );
	return buf;
}

// safe pread data from a file
// 0: no more data   -1: error
ssize_t raysafepread( int fd, char *buf, jagint length, jagint startpos )
{
    ssize_t bytes = 0;
    ssize_t len;

   	len = jagpread( fd, buf, length, startpos );
	if ( len < 0 && errno == EINTR ) {
    	len = jagpread( fd, buf, length, startpos );
	}

	if ( len == length ) {
		return len;
	}

	if ( len == 0 ) {
		prt(("E62816 raysafepread error len==0  fd=%d length=%lld startpos=%lld [%s] return 0\n",
				 fd, length, startpos, strerror(errno) )); 
		return 0;
	} else if ( len < 0 ) {
		prt(("E628117 raysafepread error len=%d  fd=%d length=%lld startpos=%lld [%s] return -1\n",
				len, fd, length, startpos, strerror(errno) )); 
		return -1;
	}

    bytes += len;
    while ( bytes < length ) {
       	len = jagpread( fd, buf+bytes, length-bytes, startpos+bytes );
		if ( len < 0 && errno == EINTR ) {
        	len = jagpread( fd, buf+bytes, length-bytes, startpos+bytes );
		}

		if ( len < 0 ) {
			prt(("E62808 raysafepread error fd=%d length-bytes=%lld startpos+bytes=%lld [%s]\n",
				     fd, length-bytes, startpos+bytes, strerror(errno) )); 
		}

        if ( len <= 0 ) {
            return bytes;
        }
        bytes += len;
    }

    return bytes;
}

// safe pwrite data to a file
ssize_t raysafepwrite( int fd, const char *buf, jagint len, jagint startpos )
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
				prt(("E290260 raysafepwrite error fd=%d len=%lld startpos=%lld [%s]\n", fd, len, startpos, strerror(errno) ));
                return (-1);    /* error */
			}
        }

        nleft -= nwritten;
        ptr += nwritten;
		startpos += nwritten;
    }
    return len;
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

jagint rayatol( const char *buf, int length )
{
	char *p = (char*)buf;
	char savebyte = *(p+length);
	*(p+length) = '\0';
	jagint value = jagatoll(p);
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


// input: inbuf can be string "234.5"  "2938381" "2020-12-25 10:33:11"  "some string" 
//errcode:  1 success; 0: error
bool formatOneCol( int tzdiff, int servtzdiff, char *outbuf, const char *inbuf, 
				   Jstr &errmsg, const Jstr &name, 
				   int offset, int length, int sig, const Jstr &type )
{
	prt(("s36602 formatOneCol name=[%s] offset=%d length=%d type=[%s] inbuf=[%s]\n", name.c_str(), offset, length, type.c_str(), inbuf ));
	if ( length < 1 ) {
		// including type == "PT", type == "LN", etc
		return 1;
	}

	if ( *inbuf == '\0' ) return 1;
	int errcode = 0;
	char savebyte = *(outbuf+offset+length);
	jagint actwlen = 0;
	int writelen = 0;

	if ( type == JAG_C_COL_TYPE_DATETIME || type == JAG_C_COL_TYPE_TIMESTAMP ) { // for datetime microsecs
		JagParseAttribute jpa( NULL, tzdiff, servtzdiff );
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length, 1 );
	} else if ( type == JAG_C_COL_TYPE_DATETIMENANO || type == JAG_C_COL_TYPE_TIMESTAMPNANO ) { // for datetime in nano sec 
		JagParseAttribute jpa( NULL, tzdiff, servtzdiff );
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length, 2 );
	} else if ( type == JAG_C_COL_TYPE_DATETIMESEC || type == JAG_C_COL_TYPE_TIMESTAMPSEC ) { // for datetime in sec 
		JagParseAttribute jpa( NULL, tzdiff, servtzdiff );
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length, 3 );
	} else if ( type == JAG_C_COL_TYPE_DATETIMEMILL || type == JAG_C_COL_TYPE_TIMESTAMPMILL ) { // for datetime in millisec 
		JagParseAttribute jpa( NULL, tzdiff, servtzdiff );
		errcode = JagTime::convertDateTimeFormat( jpa, outbuf, inbuf, offset, length, 4 );
	} else if ( type == JAG_C_COL_TYPE_DATE ) { // for date
		errcode = JagTime::convertDateFormat( outbuf, inbuf, offset, length );
	} else if ( type == JAG_C_COL_TYPE_TIME ) { // for time
		errcode = JagTime::convertTimeFormat( outbuf, inbuf, offset, length, 1 );
	} else if ( type == JAG_C_COL_TYPE_TIMENANO ) { // for time in nano sec
		errcode = JagTime::convertTimeFormat( outbuf, inbuf, offset, length, 2 );
	} else if ( type == JAG_C_COL_TYPE_DBOOLEAN || type == JAG_C_COL_TYPE_DBIT ) { // for boolean
		if ( atoi(inbuf) == 0 ) { outbuf[offset] = '0'; } else { outbuf[offset] = '1'; }
	} else if ( type == JAG_C_COL_TYPE_STR ) { // for string
		actwlen = snprintf(outbuf+offset, length+1, "%s", inbuf);
		/***
		if ( ( actwlen = snprintf(outbuf+offset, length+1, "%s", inbuf) ) > length ) {
			errcode = 1;
		}
		***/
	} else { // for int/jagint, float/double
		if ( inbuf[0] == '*' ) {
			prt(("u20287 int/float see *\n"));
			memset( outbuf+offset, 0, length );
			*(outbuf+offset) = '*';
			return 1;
		}

		jagint lonnum = 0;
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
		Jstr actdata = trackpos;
		if ( type == JAG_C_COL_TYPE_DOUBLE || type == JAG_C_COL_TYPE_FLOAT || type == JAG_C_COL_TYPE_LONGDOUBLE  ) {
			writelen = isValidSciNotation(inbuf); // borrow writelen
			//prt(("u2001 inbuf=[%s] writelen=%d\n", inbuf, writelen ));
			if ( 2 == writelen ) {
				actdata = longDoubleToStr( jagstrtold(trackpos, NULL) );
				trackpos = (char*)actdata.c_str();
			} else if ( 0 == writelen ) {
				actdata = trackpos = (char*)"0";
			}
			prt(("u2230 actdata=[%s]\n", actdata.c_str() ));
		}

		lonnum = jagatoll(trackpos);  // the digits before . decimal point
		prt(("u2210 trackpos=[%s] lonnum=%d\n", trackpos, lonnum ));

		if ( isInteger( type ) ) { // for int
			writelen = length; 
			if ( strlen(trackpos) > JAG_DBIGINT_FIELD_LEN ) errcode = 2;
		} else if ( type == JAG_C_COL_TYPE_DOUBLE || type == JAG_C_COL_TYPE_FLOAT || type == JAG_C_COL_TYPE_LONGDOUBLE ) { 
			// for double and float whole part
			writelen = length-sig-1;		
			dofl = true;
			//prt(("u0128 writelen=%d\n", writelen ));
		}

		if ( !errcode && ( actwlen = snprintf(outbuf+offset+1, writelen, "%0*lld", writelen-1, lonnum) ) > writelen-1 ) {
			prt(("u2039 errcode=%d actwlen=%d writelen=%d lonnum=%d\n", errcode, actwlen, writelen, lonnum ));
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
	
	// OK
	if ( errcode == 0 ) {
		*(outbuf+offset+length) = savebyte;
		return 1;
	} 
	
	if ( errcode == 1 ) {
		if ( type == JAG_C_COL_TYPE_DATE ) {
			errmsg = "E6200 Error date format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIME ) {
			errmsg = "E6202 Error datetime format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIMESEC ) {
			errmsg = "E6203 Error datetimesec format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
			errmsg = "E6204 Error datetimenano format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
			errmsg = "E6208 Error timestampsec format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
			errmsg = "E6210 Error timestamp format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
			errmsg = "E6212 Error timestampsec format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIMESEC ) {
			errmsg = "E6212 Error datetimesec format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
			errmsg = "E6214 Error timestampnano format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
			errmsg = "E6218 Error timestampmill format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_DATETIMEMILL ) {
			errmsg = "E6220 Error datetimemill format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			errmsg = "E6223 Error time format. Please correct your input.";
		} else if ( type == JAG_C_COL_TYPE_STR ) {
			errmsg = "E6227 Length of string " + longToStr(actwlen) + " exceeded limit " + 
					intToStr(length) + " for column " + name + ". Please correct your input.";
		} else {
			errmsg = "E6232 Error input. Please correct your input.";
		}
	} else if ( errcode == 2 ) {
		errmsg = "E16208 Length of input " + longToStr(actwlen) + " exceeded limit " + 
				intToStr(writelen-1) + " for column " + name + ". Please correct your input.";
	} else {
		errmsg = "E16210 Error error code";
	}
	
	return 0; // error
}

void MultiDbNaturalFormatExchange( char *buffers[], int num, int numKeys[], const JagSchemaAttribute *attrs[] )
{
	for ( int i = 0; i < num; ++i ) {
		rwnegConvertionCols( buffers[i], numKeys[i], attrs[i] );
	}
}

// convert negative numbers for multi columns
// For negative number comparison    "-000248" ---> "-999752"
// It called again: "-999752" -->  "-000248"
void dbNaturalFormatExchange( char *buffer, int numKeys, const JagSchemaAttribute *schAttr, int offset, int length, const Jstr &type )
{
	if ( numKeys == 0 ) {
		rwnegConvertionBuf( buffer, offset, length, type );
	} else {
		rwnegConvertionCols( buffer, numKeys, schAttr );
	}
}

// convert negative numbers for multi columns
// For negative number comparison    "-000248" ---> "-999752"
// It called again: "-999752" -->  "-000248"
// convert negative numbers for a column
void rwnegConvertionBuf( char *buffer, int offset, int length, const Jstr &type )
{	
    if ( *(buffer+offset) == JAG_C_NEG_SIGN && ( isInteger(type) || isFloat(type) ) ) {
        for ( int j = offset+1; j < offset+length; ++j ) {
            if ( *(buffer+j) != '.' ) {
				*(buffer+j) = '9' - *(buffer+j) + '0';
			}
        }
   }
}

// convert negative numbers for multi columns
// For negative number comparison    "-000248" ---> "-999752"
// It called again: "-999752" -->  "-000248"
void rwnegConvertionCols( char *buffer, int numKeys, const JagSchemaAttribute *schAttr )
{	
	for ( int i = 0; i < numKeys; ++i ) {
		if ( *(buffer+schAttr[i].offset) == JAG_C_NEG_SIGN 
			 && ( isInteger(schAttr[i].type) || isFloat( schAttr[i].type ) ) ) {

			for ( int j = schAttr[i].offset+1; j < schAttr[i].offset+schAttr[i].length; ++j ) {
				if ( *(buffer+j) != '.' ) {
					*(buffer+j) = '9' - *(buffer+j) + '0';
				}
			}
		}
	}
}

/***
#ifndef _WINDOWS64_
 Jstr filePathFromFD( int fd )
 {
	char tmp[256];
	char file[256];
	sprintf(tmp, "/proc/self/fd/%d", fd );
	memset( file, 0, 256 );
	readlink( tmp, file, 256 );
	return file;
}
#endif
***/


/***
Jstr removeCharFromString( const Jstr &str, char dropc )
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
	Jstr res( p );
	if ( p ) free( p );
	return res;
}
**/

Jstr makeUpperString( const Jstr &str )
{
	if ( str.length()<1) return "";

	char *p = (char*)jagmalloc( str.length() + 1 );
	p[str.length()] = '\0';
	for ( int i = 0; i < str.length(); ++i ) {
		p[i] = toupper( *(str.c_str()+i) );
	}
	Jstr res( p );
	if ( p ) free( p );
	return res;
}

Jstr makeLowerString( const Jstr &str )
{
	if ( str.length()<1) return "";

	char *p = (char*)jagmalloc( str.length() + 1 );
	p[str.length()] = '\0';
	for ( int i = 0; i < str.length(); ++i ) {
		p[i] = tolower( *(str.c_str()+i) );
	}
	Jstr res( p );
	if ( p ) free( p );
	return res;
}

JagFixString makeUpperOrLowerFixString( const JagFixString &str, bool isUpper )
{
	JagFixString res;
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
	res = JagFixString( p, str.length(), str.length() );
	if ( p ) free( p );
	return res;
}

Jstr trimChar( const Jstr &str, char c )
{
	//prt(("s22092 in trimChar str=[%s] c=[%c]\n", str.s(), c ));
	Jstr s1 = trimHeadChar( str, c);
	//prt(("s22093 in trimChar str=[%s] c=[%c] s1=[%s]\n", str.s(), c, s1.s() ));
	Jstr s2 = trimTailChar( s1, c);
	//prt(("s22094 in trimChar str=[%s] c=[%c] s2=[%s]\n", str.s(), c, s2.s() ));
	return s2;
}

// input: "ccxjfjdkjfkd"  out: "xjfjdkjfkd"
Jstr trimHeadChar( const Jstr &str, char c )
{
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str();
	if ( *p != c ) return str; 
	while ( *p == c ) ++p;
	if ( *p == '\0' ) return "";
	//prt(("s222018 in trimHeadChar p=[%s]\n", p ));
	return Jstr(p);
}

// input: "xjfjdkjfkdcc"  out: "xjfjdkjfkd"
Jstr trimTailChar( const Jstr &str, char c )
{
	char v;
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p != c ) return str; 
	while ( *p == c && p >= str.c_str() ) --p;
    if ( p < str.c_str() ) return ""; 

	v = *(p+1);
	*(p+1) = '\0';
	//Jstr newstr = Jstr( str.c_str(), str.size(), str.size() );
	Jstr newstr = Jstr( str.c_str() );
	*(p+1) = v;
	//prt(("s302838 in trimTailChar str=[%s]  newstr=[%s]\n", str.c_str(), newstr.s() ));
	return newstr;
}

Jstr trimTailLF( const Jstr &str )
{
	char v;
	if ( str.size() < 1 ) return str;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p != '\r' && *p != '\n' ) return str; 
	while ( (*p =='\r' || *p=='\n' ) && p >= str.c_str() ) --p;
    if ( p < str.c_str() ) return ""; 

	v = *(p+1);
	*(p+1) = '\0';
	//Jstr newstr = Jstr( str.c_str(), str.size(), str.size() );
	Jstr newstr = Jstr( str.c_str() );
	*(p+1) = v;
	return newstr;
}

/***
bool beginWith( const Jstr &str, char c )
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
***/

bool endWith( const Jstr &str, char c )
{
	if ( str.size() < 1 ) return false;
	char *p = (char*)str.c_str() + str.size()-1;
	if ( *p == c ) return true; 
	return false;
}

bool endWithStr( const Jstr &str, const Jstr &end )
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
bool endWhiteWith( const Jstr &str, char c )
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


Jstr intToStr( int i )
{
	char buf[16];
	memset(buf, 0, 16 );
	sprintf(buf, "%d", i );
	return Jstr(buf);
}

Jstr longToStr( jagint i )
{
	char buf[32];
	memset(buf, 0, 32 );
	sprintf(buf, "%lld", i );
	return Jstr(buf);
}

Jstr doubleToStr( double f )
{
	char buf[48];
	memset(buf, 0, 48 );
	sprintf(buf, "%.10f", f );
	return Jstr(buf).trim0();
}

Jstr d2s( double f )
{
	char buf[48];
	memset(buf, 0, 48 );
	sprintf(buf, "%.10f", f );
	return Jstr(buf).trim0();
}

Jstr doubleToStr( double f, int maxlen, int sig )
{
	char buf[64];
	memset(buf, 0, 64 );
	snprintf( buf, 64, "%0*.*f", maxlen, sig, f);
	return Jstr(buf).trim0();
}


Jstr longDoubleToStr( abaxdouble f )
{
	char buf[64];
	memset(buf, 0, 64 );
	#ifdef _WINDOWS64_
	sprintf(buf, "%f", f );
	#else
	sprintf(buf, "%Lf", f );
	#endif
	return Jstr(buf).trim0();
}

// mode 0: use sprintf, 1 use snprintf
// fill 0: directly fill with Lf or f; 1: fill with *.*Lf or *.*f, where fill length are JAG_MAX_INT_LEN and JAG_MAX_SIG_LEN
int jagsprintfLongDouble( int mode, bool fill, char *buf, abaxdouble i, jagint maxlen )
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

bool isInteger( const Jstr &dtype )
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

bool isFloat( const Jstr &colType )
{
	 if ( colType == JAG_C_COL_TYPE_DOUBLE || colType == JAG_C_COL_TYPE_FLOAT || colType == JAG_C_COL_TYPE_LONGDOUBLE ) {
	 	return true;
	 }

	 return false;
}


bool isDateTime( const Jstr &dtype ) 
{
	if ( isDateAndTime( dtype ) ) return true;

	if ( dtype == JAG_C_COL_TYPE_TIMENANO ||
		dtype == JAG_C_COL_TYPE_DATE  ||
		dtype == JAG_C_COL_TYPE_TIME ) 
	{
		return true;
	}
	return false;
}

bool isDateAndTime( const Jstr &dtype ) 
{
	if ( dtype == JAG_C_COL_TYPE_DATETIME  ||
		 dtype == JAG_C_COL_TYPE_TIMESTAMP  ||
		 dtype == JAG_C_COL_TYPE_TIMESTAMPSEC  ||
		 dtype == JAG_C_COL_TYPE_DATETIMESEC  ||
		 dtype == JAG_C_COL_TYPE_DATETIMEMILL  ||
		 dtype == JAG_C_COL_TYPE_TIMESTAMPMILL  ||
		 dtype == JAG_C_COL_TYPE_DATETIMENANO  ||
		 dtype == JAG_C_COL_TYPE_TIMESTAMPNANO )
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

// replace ending \n and \r  --> ' '
void replaceStrEnd( char *msg, int len )
{
    int i;
    for ( i=len-1; i>=0; --i ) {
        if ( msg[i] == '\n' ) {  msg[i] = ' '; }
        else if ( msg[i] == '\r' ) { msg[i] = ' '; }
    }

	// leave only one end ' '
    for ( i=len-1; i>=1; --i ) {
		if ( msg[i-1] == ' ' && msg[i] == ' ' ) {
			 msg[i] = '\0';
		} else {
			break;
		}
	}
}

// if c==';'
// "sssss;    "   return 1  --- saw last line wth ;
// "sssss;hhh    "  return 0   --- not the end line with ;
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

Jstr intToString( int i ) 
{
	char buf[16];
	sprintf(buf, "%d", i );
	return Jstr( buf );
}
Jstr longToString( jagint i ) 
{
	char buf[32];
	sprintf(buf, "%lld", i );
	return Jstr( buf );
}
Jstr ulongToString( jaguint i ) 
{
	char buf[32];
	sprintf(buf, "%lu", i );
	return Jstr( buf );
}

jagint strchrnum( const char *str, char ch )
{
    if ( ! str || *str == '\0' ) return 0;

    jagint cnt = 0;
    const char *q;
    while ( 1 ) {
        q=strchr(str, ch);
        if ( ! q ) break;
        ++cnt;
        str = q+1;
    }

    return cnt;
}

// "aabbb" --> 2, skip consecutive chars
// also skip 'ijjj'  "xxx"
jagint strchrnumskip( const char *str, char ch )
{
    if ( ! str || *str == '\0' ) return 0;
    jagint cnt = 0;
    while ( *str != '\0' ) {
		if ( *str == '\'' ) { while ( *str != '\'' && *str != '\0' ) ++str; ++str; }
		else if ( *str == '"' ) { while ( *str != '"' && *str != '\0' ) ++str; ++str; }
		else if ( *str == ch ) { while ( *str == ch ) ++str; ++cnt; }
		else ++str;
    }

    return cnt;
}

void escapeNewline( const Jstr &instr, Jstr &outstr )
{
	if ( instr.size() < 1 ) return;

	jagint n = strchrnum( instr.c_str(), '\n' );
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
	outstr = Jstr(str);
	jagfree( str );
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
void splitFilePath( const char *fpath, Jstr &first, Jstr &last )
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

/***
// input fpath:  /tmp/abc/fff.jdb  newLast: "bid"
// output: first=/tmp/abc/fff.bid
Jstr renameFilePath( const Jstr& fpath, const Jstr &newLast )
{
	prt(("s33278 renameFilePath fpath=%s newLast=%s\n", fpath.c_str(), newLast.c_str() ));
	char *p = (char*)strrchr( fpath.c_str(), '.' );
	if ( !p ) {
		return fpath;
	}
	*p = '\0';
	
	Jstr newPath = Jstr(fpath.c_str()) + "." + newLast;
	*p = '.';
	return newPath;
}

void stripeFilePath( const Jstr &fpath, jagint stripe, Jstr &stripePath )
{
	Jstr first, last;
	splitFilePath( fpath.c_str(), first, last );
	if ( last.size() < 1 ) {
		printf("s5721 error in stripeFilePath fpath=[%s]\n", fpath.c_str() );
		return;
	}

	stripePath = first + "." + longToStr(stripe) + "." + last;
}
***/

Jstr makeDBObjName( JAGSOCK sock, const Jstr &dbname, const Jstr &objname )
{
	Jstr dbobj;
	dbobj = dbname + "." + objname;
	return dbobj;
}

Jstr makeDBObjName( JAGSOCK sock, const Jstr &dbdotobj )
{
	JagStrSplit split( dbdotobj, '.' );
	return makeDBObjName( sock, split[0], split[1] );
}

// /home/user1/jaguar  or /home/user2/brandx
Jstr jaguarHome()
{
	Jstr jaghm;
	const char *p = getenv("JAGUAR_HOME");
	if (  ! p ) {
		p = getenv("HOME");
		if ( p ) {
			jaghm = Jstr(p) + "/jaguar";
		} else {
			#ifdef _WINDOWS64_
		       p = "/c/jaguar";
		       _mkdir( p );
			#else
		  	  p = "/tmp/jaguar";
		      ::mkdir( p, 0777 );
		    #endif
			jaghm = Jstr(p);
		}
	} else {
		jaghm = Jstr(p);
	}
	
	return jaghm;
}

// Used for  yes or nor tests.  a should be lowercase
bool startWith( const Jstr &str, char a )
{
	if ( str.size() < 1 ) return false;
	if ( *(str.c_str()) == a ) return true;
	if ( *(str.c_str()) == toupper(a) ) return true;
	return false;

}

// safe read data from a file
ssize_t raysaferead( int fd, char *buf, jagint len )
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

ssize_t raysaferead( int fd, unsigned char *buf, jagint len )
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
ssize_t raysafewrite( int fd, const char *buf, jagint len )
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
int selectServer( const JagFixString &min, const JagFixString &max, const JagFixString &inkey )
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
jagint availableMemory( jagint &callCount, jagint lastBytes )
{
	jagint bytes;
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

// return JAG_WRITE_SQL(==1): write related, insert, cinsert, dinsert, create, alter, update, delete, drop, truncate, import
//                        broadcast command to all clusters and all servers
// return JAG_READ_SQL(==0): read related, select, show, desc and/or other commands, sent to maybe only 1 server
int checkReadOrWriteCommand( const char *pmesg )
{
	if ( 0 == strncasecmp( pmesg, "insert", 6 ) || 0 == strncasecmp( pmesg, "finsert", 7 ) ||
		 0 == strncasecmp( pmesg, "cinsert", 7 ) || 0 == strncasecmp( pmesg, "dinsert", 7 ) ||
		 0 == strncasecmp( pmesg, "create", 6 ) || 0 == strncasecmp( pmesg, "alter", 5 ) ||
		 0 == strncasecmp( pmesg, "update", 6 ) || 0 == strncasecmp( pmesg, "delete", 6 ) ||
		 0 == strncasecmp( pmesg, "use ", 4 ) || 0 == strncmp( pmesg, "import", 6 ) ||
		 0 == strncasecmp( pmesg, "drop", 4 ) || 0 == strncasecmp( pmesg, "truncate", 8 ) ||
		 0 == strncasecmp( pmesg, "createdb", 8 ) || 0 == strncasecmp( pmesg, "dropdb", 6 ) ||
		 0 == strncasecmp( pmesg, "createuser", 10 ) || 0 == strncasecmp( pmesg, "dropuser", 8 ) ||
		 0 == strncasecmp( pmesg, "grant", 5 ) || 0 == strncasecmp( pmesg, "revoke", 6 ) ||
		 0 == strncasecmp( pmesg, "changepass", 10 ) || 0 == strncasecmp( pmesg, "changedb", 8 ) ) {
		 return JAG_WRITE_SQL;
	}
	return JAG_READ_SQL;
}

// return JAG_WRITE_SQL(==1): write related, insert, cinsert, dinsert, create, alter, update, delete, drop, truncate, import
//                        broadcast command to all clusters and all servers
// return JAG_READ_SQL(==0): read related, select, show, desc and/or other commands, sent to maybe only 1 server
int checkReadOrWriteCommand( int qmode )
{
	if ( 1 == qmode || 7 == qmode || 3 == qmode ) {
		 return JAG_WRITE_SQL;
	} else {
		return JAG_READ_SQL;
	}
}

// return 1: string type
// return 2: bool type
// return 3: int type
// return 4: jagint type
// return 5: float/double type
// return 6: date/time type
int checkColumnTypeMode( const Jstr &type )
{
	if ( type == JAG_C_COL_TYPE_STR ) return 1;
	else if ( type == JAG_C_COL_TYPE_DBOOLEAN || type == JAG_C_COL_TYPE_DBIT ) return 2;
	else if ( type == JAG_C_COL_TYPE_DINT  || type == JAG_C_COL_TYPE_DTINYINT  || 
				type == JAG_C_COL_TYPE_DSMALLINT  || type == JAG_C_COL_TYPE_DMEDINT  ) return 3;
	else if ( type == JAG_C_COL_TYPE_DBIGINT  ) return 4;
	else if ( type == JAG_C_COL_TYPE_FLOAT  || type == JAG_C_COL_TYPE_DOUBLE || type == JAG_C_COL_TYPE_LONGDOUBLE  ) return 5;
	else if ( isDateTime(type) ) return 6;
	return 0;
}

Jstr formOneColumnNaturalData( const char *buf, jagint offset, jagint length, const Jstr &type )
{
	Jstr instr, outstr;
	int rc = checkColumnTypeMode( type );
	if ( 6 == rc ) {
		if ( type == JAG_C_COL_TYPE_DATETIME || type == JAG_C_COL_TYPE_TIMESTAMP ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr, 1 );
		} else if ( type == JAG_C_COL_TYPE_DATETIMENANO || type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr, 2 );
		} else if ( type == JAG_C_COL_TYPE_DATETIMESEC || type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr, 3 );
		} else if ( type == JAG_C_COL_TYPE_DATETIMEMILL || type == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertDateTimeToLocalStr( instr, outstr, 4 );
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertTimeToStr( instr, outstr );
		} else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
			instr = Jstr( buf+offset, length, length );
			JagTime::convertTimeToStr( instr, outstr, 2 );
		} else if ( type == JAG_C_COL_TYPE_DATE ) {
			instr = Jstr( buf+offset, length, length );
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
		// string
		/**
		Jstr tmpstr = Jstr(buf+offset, length);
		outstr = tmpstr.c_str();
		**/
		//outstr = Jstr(buf+offset, length, length);
		prt(("u2029280 formOneColumnNaturalData buf=[%s] offset=%d length=%d\n", buf, offset, length ));
		char *pbuf = (char*)buf;
		char v = *(pbuf+offset+length);
		*(pbuf+offset+length) = NBT;
		outstr = Jstr(pbuf+offset);
		*(pbuf+offset+length) = v;
		prt(("u222208 outstr=[%s]\n", outstr.s() ));
	}

	return outstr;
}


// rewrite for group by 1-24-2017
// gdvhdr is for group by use only
// return 0: error
// return 1: success
// num is # of tables involved. normally num=1 for one table
int rearrangeHdr( int num, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
					JagParseParam *parseParam, const JagVector<SetHdrAttr> &spa, Jstr &newhdr, Jstr &gbvhdr,
					jagint &finalsendlen, jagint &gbvsendlen, bool needGbvs )
{
	//prt(("u2033 rearrangeHdr num=%d parseParam->hasColumn=%d ...\n", num, parseParam->hasColumn ));
	gbvsendlen = 0;
	int rc, collen, siglen, constMode = 0, typeMode = 0;
	bool isAggregate;
	jagint offset = 0;
	ExprElementNode *root;
	const JagSchemaRecord *records[num];
	Jstr type, hdr, tname;
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
					tname += "_size";
				} else if ( JAG_GETFILE_TIME == parseParam->selColVec[i].getfileType ) {
					tname += "_time";
				} else if ( JAG_GETFILE_MD5SUM == parseParam->selColVec[i].getfileType ) {
					tname += "_md5";
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
						hdr = spa[i].dbobj + "." + Jstr((*(records[i]->columnVector))[j].name.c_str());
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
					printf("s39004 Error setFuncAttribute\n");
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
	prt(("u12340 gbvhdr=[%s] ...\n", gbvhdr.s() ));

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

		prt(("u12342 gbvhdr=[%s] ...\n", gbvhdr.s() ));
	}

	gbvhdr += records[0]->formatTailRecord( );
	gbvsendlen = offset;

	prt(("u12342 gbvhdr=[%s] ... gbvsendlen=%d\n", gbvhdr.s(), gbvsendlen ));
	
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
	Jstr gstr;
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
void convertToHashMap( const Jstr &kvstr, char sep,  JagHashMap<AbaxString, AbaxString> &hashmap )
{
	if ( kvstr.size()<1) return;
	JagStrSplit sp( kvstr, sep, true );
	if ( sp.length()<1) return;
	Jstr kv;
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
void changeHome( Jstr &fpath )
{
	//prt(("s5880 changeHome in=[%s]\n", fpath.c_str() ));
	JagStrSplit oldsp( fpath, '/', true );
	char *home = getenv("JAGUAR_HOME");
	if ( ! home ) home = getenv("HOME");
	int idx = 0;
	for ( int i = oldsp.length()-1; i>=0; --i ) {
		if ( oldsp[i] == "jaguar" ) {
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
		fpath += Jstr("/") + oldsp[i];
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

/***
int jaguar_cond_signal( pthread_cond_t *cond)
{
	int rc = pthread_cond_signal( cond );
	if ( 0 != rc ) {
		prt(("s6806 error pthread_cond_signal(%0x) [%s]\n", cond, strerror( rc ) ));
	}
	return rc;
}
***/

int jaguar_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	int rc = pthread_cond_wait(cond, mutex);
	if ( 0 != rc ) {
		prt(("s6807 error pthread_cond_wait(%0x %0x) [%s]\n", cond, mutex, strerror( rc ) ));
	}
	return rc;
}

// 0: failure
// 1: OK
int getPassword( Jstr &outPassword )
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


ssize_t jagpread( int fd, char *buf, jagint length, jagint startpos )
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

ssize_t jagpwrite( int fd, const char *buf, jagint length, jagint startpos )
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


void randDataStringSort( Jstr *vec, int maxlen )
{
	struct timeval now;
	gettimeofday( &now, NULL );
	jagint microsec = now.tv_sec * (jagint)1000000 + now.tv_usec;
	srand( microsec );
	Jstr tmp;
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

jagint getNearestBlockMultiple( jagint value )
{
	if ( value < JAG_BLOCK_SIZE ) value = JAG_BLOCK_SIZE;
	jagint a, b;
	double c, d;
	d = (double)value;
	c = log2 ( d );
	b = (jagint) c;
	++b;
	a = exp2( b );
	return a;
}

jagint getBuffReaderWriterMemorySize( jagint value ) // max as 1024 ( MB ), value and return in MB
{
	if ( value <= 0 ) value = 0;
	value = getNearestBlockMultiple( value ) / 2;
	if ( value > 1024 ) value = 1024;
	return value;
}

char *jagmalloc( jagint sz )
{
	char *p = NULL;
	while ( NULL == p ) {
		p = (char*)malloc(sz);
		if ( NULL == p ) {
			prt(("ERR0300 Error malloc %lld bytes memory, not enough memory. Please free up some memory. Retry in 10 seconds...\n", sz ));
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
			prt(("ERR0400 Error pthread_create, errno=%d. Please clean up some unused processes. Retry in 10 seconds...\n", rc ));
			sleep(10);
		}
	}

	return 0;
}

int jagpthread_join(pthread_t thread, void **retval)
{
	int rc = pthread_join( thread, retval );
	if ( rc != 0 ) {
		prt(("Error pthread_join , errno=%d\n", rc ));
	}
	return rc;
}

// Send data to socket, no header added
// return -1: error; bytes received
// buf has space to hold len bytes
jagint sendRawData( JAGSOCK sock, const char *buf, jagint len )
{
	jagint slen = _raysend( sock, buf, len );
	if ( slen < len ) { 
		return -1; 
	} else {
		return slen;
	}
}

// Receive message data from socket, with sqlhdr
// return -1: error; bytes received
// hdr is a buffer to hold hdr, buf is a pointer to hold len bytes
// with jagmalloc inside, buf is payload
jagint recvMessage( JAGSOCK sock, char *hdr, char *&buf )
{
	jagint slen, len;
	memset( hdr, 0, JAG_SOCK_TOTAL_HDR_LEN+1);
	slen = _rayrecv( sock, hdr, JAG_SOCK_TOTAL_HDR_LEN); 
	if ( slen < JAG_SOCK_TOTAL_HDR_LEN) { 
		prt(("u2220221 _rayrecv slen=%d < JAG_SOCK_TOTAL_HDR_LEN return -1\n", slen ));
		return -1; 
	}
	len = getXmitMsgLen( hdr );
	if ( len <= 0 ) { 
		prt(("u82038 getXmitMsgLen hdr=[%s] slen=%d len=%d return 0\n", hdr, slen, len ));
		return 0; 
	} 
	prt(("u82038 getXmitMsgLen hdr=[%s] slen=%d len=%d\n", hdr, slen, len ));

	if ( buf ) { free( buf ); }
	buf = (char*)jagmalloc( len+1 );
	memset( buf, 0, len+1 );
	slen = _rayrecv( sock, buf, len );
	if ( slen < len ) { 
		prt(("recvMessage error 2=%d %d %d\n", sock, slen, len));
		free( buf ); buf = NULL; 
		return -1; 
	}

	//prt(("c387440 recvMessage got hdr=[%s] buf=[%s] slen=%d\n", hdr, buf, slen ));
	return slen;
}

// Receive data from socket, with sqlhdr
// return -1: error; bytes received
// hdr is a buffer to hold hdr
// If buf is not NULL, it is a pointer to hold len bytes with jagmalloc
// if buf is NULL, sbuf will hold data. sbuf is allocated from caller
jagint recvMessageInBuf( JAGSOCK sock, char *hdr, char *&buf, char *sbuf, int sbuflen )
{
	jagint slen, len;
	memset( hdr, 0, JAG_SOCK_TOTAL_HDR_LEN+1);

	slen = _rayrecv( sock, hdr, JAG_SOCK_TOTAL_HDR_LEN); 
	if ( slen < JAG_SOCK_TOTAL_HDR_LEN) { 
		prt(("u332092 slen=%d < JAG_SOCK_TOTAL_HDR_LEN got hdr=[%s] return -1\n", slen, hdr ));
		return -1; 
	}

	len = getXmitMsgLen( hdr );
	if ( len <= 0 ) { 
		prt(("u2020281 getXmitMsgLen len=%d <=0 hdr=[%s]\n", len, hdr ));
		return 0; 
	}

	if ( len < sbuflen ) {
    	if ( buf ) { free( buf ); buf=NULL; }
		memset( sbuf, 0, sbuflen+1);
    	slen = _rayrecv( sock, sbuf, len );
    	if ( slen < len ) { 
			prt(("s0292811 _rayrecv len=%d  got slen=%d recved sbuf=[%s]\n", len, slen, sbuf ));
			return -1;
		}
		//sbuf[ len ] = '\0' ;
		sbuf[ slen ] = '\0' ;
	} else {
    	if ( buf ) { free( buf ); }
    	buf = (char*)jagmalloc( len+1 );
		memset( buf, 0,  len+1 );
    	slen = _rayrecv( sock, buf, len );
    	if ( slen < len ) { 
			prt(("s000288 error slen=%d < len=%d buf=[%s]\n", slen, len, buf ));
    		free( buf ); buf = NULL; 
    		return -1; 
    	}
		//buf[ len ] = '\0' ;
		buf[ slen ] = '\0' ;
		sbuf[0] = '\0';
		//memset( sbuf, 0, sbuflen+1);
	}

	return slen;
}

// receive data from socket, data only
// return -1: error; bytes received
jagint recvRawData( JAGSOCK sock, char *buf, jagint len )
{
	jagint slen = _rayrecv( sock, buf, len );
	if ( slen < len ) { 
		return -1; 
	}
	return slen;
}

#ifdef _WINDOWS64_
// windows code
jagint _raysend( JAGSOCK sock, const char *hdr, jagint N )
{
    register jagint bytes = 0;
    register jagint len;
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
    		len = ::send( sock, hdr, N, 0 );
			if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
				++errcnt;
			} else break;
			if ( errcnt >= 5 ) break;
		}
		if ( errcnt >= 5 ) {
			rayclose( sock );
			len = -1;
		}
	}

	if ( len < 0 ) {
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
        		len = ::send( sock, hdr+bytes, N-bytes, 0 );
				if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
					++errcnt;
				} else break;
				if ( errcnt >= 5 ) break;
			}
			if ( errcnt >= 5 ) {
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len < 0 ) {
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
jagint _rayrecv( JAGSOCK sock, char *hdr, jagint N )
{
    register jagint bytes = 0;
    register jagint len;
	int errcnt = 0;

	if ( N <= 0 ) { 
		prt(("s4429 _rayrecv  N <= 0 return 0\n" ));
		return 0; 
	}

	if ( socket_bad( sock ) ) { 
		return -1; 
	}

	len = ::recv( sock, hdr, N, 0 );
	if ( len < 0 && WSAGetLastError() == WSAEINTR ) {
    	len = ::recv( sock, hdr, N, 0 );
	}
	if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) { // recv timeout
		++errcnt;
		while ( 1 ) {
    		len = ::recv( sock, hdr, N, 0 );
			if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
				++errcnt;
			} else break;
			if ( errcnt >= 5 ) break;
		}
		if ( errcnt >= 5 ) {
			rayclose( sock );
			len = -1;
		}
	}	

	// if len==0; remote end had a orderly shudown
	if ( len == 0 ) {
		return 0;
	} else if ( len < 0 ) {
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
        		len = ::recv( sock, hdr+bytes, N-bytes, 0 );
				if ( len < 0 && WSAGetLastError() == WSAEWOULDBLOCK ) {
					++errcnt;
				} else break;
				if ( errcnt >= 5 ) break;
			}
			if ( errcnt >= 5 ) {
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len <= 0 ) {
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
jagint _raysend( JAGSOCK sock, const char *hdr, jagint N )
{
    jagint bytes = 0;
    jagint len;
	int errcnt;

	/***
	// debug, no print HB 
	if ( ! strstr( hdr, "1CHBCY" ) ) {
		prt(("    u2220 _raysend hdr=[%s] N=%d\n", hdr, N ));
	}
	prt(("s2220 _raysend hdr=[%s] N=%d\n", hdr, N ));
	dumpmem( hdr, N );
	prt(("s2220\n" ));
	***/

	if ( N <= 0 ) { 
		prt(("    u2083 raysend N<=0 return 0\n"));
		return 0;
	}

	if ( socket_bad( sock ) ) { 
		prt(("    u2083 raysend sock %d bad\n", sock ));
		//abort();
		return -1; 
	}

	len = ::send( sock, hdr, N, MSG_NOSIGNAL );
	if ( len < 0 && errno == EINTR ) {
    	len = ::send( sock, hdr, N, MSG_NOSIGNAL );
	}

	errcnt = 0;
	if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // send timeout
		++errcnt;
		while ( 1 ) {
    		len = ::send( sock, hdr, N, MSG_NOSIGNAL );
			if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
				++errcnt;
			} else break;
			if ( errcnt >= 5 ) break;
		}
		if ( errcnt >= 5 ) {
			rayclose( sock );
			len = -1;
		}
	}	

	if ( len < 0 ) {
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		prt(("    u20922 raysend hdr=[%s] N=%d  len < 0 ret -1\n", hdr, N ));
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
        		len = ::send( sock, hdr+bytes, N-bytes, MSG_NOSIGNAL );
				if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
					++errcnt;
				} else break;
				if ( errcnt >= 5 ) break;
			}
			if ( errcnt >= 5 ) {
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len < 0 ) {
            if ( errno == ENETRESET || errno == ECONNRESET ) {
                rayclose( sock );
            }
			prt(("    u20922 raysend len < 0 ret bytes=%d\n", bytes));
            return bytes;
        }
        bytes += len;
    }

	#if 0
	if ( ! strstr( hdr, "1CHBCY" ) ) {
		prt(("    u20922 raysend final ret bytes=%d\n", bytes));
	}
	#endif

    return bytes;
}

// linux code
jagint _rayrecv( JAGSOCK sock, char *buf, jagint N )
{
    register jagint bytes = 0;
    register jagint len;
	int errcnt = 0;

	//prt(("    u200727 _rayrecv expect N=%d ...\n", N));

	if ( N <= 0 ) { 
		prt(("    s4429 _rayrecv  N <= 0 return 0\n" ));
		return 0;
	}

	if ( socket_bad( sock ) ) { 
		prt(("    u82882 socket_bad return -1\n"));
		return -1; 
	}

	len = ::recv( sock, buf, N, 0 );
	//prt(("    s4424 _rayrecv N=%d len=%d buf=[%s] done\n", N, len, buf ));
	if ( len < 0 && errno == EINTR ) {
    	len = ::recv( sock, buf, N, 0 );
	}

	if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // recv timeout
		++errcnt;
		while ( 1 ) {
    		len = ::recv( sock, buf, N, 0 );
			if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
				++errcnt;
			} else break;
			if ( errcnt >= 5 ) break;
		}
		if ( errcnt >= 5 ) {
			rayclose( sock );
			len = -1;
		}
	}	

	// if len==0; remote end had a orderly shudown
	if ( len == 0 ) {
		//prt(("    u2302 _rayrecv len is %d, sock %d, buf=[%.100s], N %d DONE ret 0\n", len, sock, buf, N));
		return 0;
	} else if ( len < 0 ) {
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		//prt(("    u2866 _rayrecv len < 0 return -1\n"));
		return -1;
	}

    bytes += len;
    while ( bytes < N ) {
        len = ::recv( sock, buf+bytes, N-bytes, 0 );
		if ( len < 0 && errno == EINTR ) {
        	len = ::recv( sock, buf+bytes, N-bytes, 0 );
		}
	
		errcnt = 0;
		if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) { // recv timeout
			++errcnt;
			while ( 1 ) {
        		len = ::recv( sock, buf+bytes, N-bytes, 0 );
				if ( len < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK ) ) {
					++errcnt;
				} else break;
				if ( errcnt >= 5 ) break;
			}
			if ( errcnt >= 5 ) {
				rayclose( sock );
				len = -1;
			}
		}	

        if ( len <= 0 ) {
            if ( errno == ENETRESET || errno == ECONNRESET ) {
                rayclose( sock );
            }
			//prt(("    u667807 _rayrecv return bytes=%d\n", bytes ));
            return bytes;
        }
        bytes += len;
    }

	//prt(("c50287 _rayrecv buf=[%s] bytes=%d N=%d\n", buf, bytes, N ));
    return bytes;
}
#endif


int jagmalloc_trim( jagint n ) 
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
			//prt(("s3370 loopOpen fopen(%s) mode=[%s]\n", path, mode ));
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
		prt(("s3377 ::unlink(%s) error [%s]\n", path, strerror( errno )));
	} else {
		prt(("s3378 ::unlink(%s) OK\n", path));
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
		//prt(("s3380 rename(%s   ->   %s)\n", path, newpath));
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
void jagsleep( useconds_t time, int mode )
{
	#ifdef _WINDOWS64_
	if ( JAG_SEC == mode ) Sleep( time*1000 );
	else if ( JAG_MSEC == mode ) Sleep( time );
	else if ( JAG_USEC == mode ) {
		jagint t = time/1000;
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
jagint jagsendfile( JAGSOCK sock, int fd, jagint size )
{
	char buf[1024*1024];
	lseek(fd, 0, SEEK_SET );
	jagint sz = 0;
	jagint n = 0;
	jagint snd;
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
jagint jagsendfile( JAGSOCK sock, int fd, jagint size )
{
	/***
	jagint sz = ::sendfile( sock, fd, NULL, size );
	if ( sz < 0 ) {
		if ( errno == ENETRESET || errno == ECONNRESET ) {
			rayclose( sock );
		}
		return -1;
	}
	else if ( sz < size ) {
		jagint sz2;
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
	***/

    jagint BATCHSIZE = 2000000000;
    jagint batches = size/BATCHSIZE;
    jagint remain = size % BATCHSIZE;
    jagint tot, rc;
    tot = 0;
    for ( int i = 0; i < batches; ++i ) {
        rc = sendOneBatch( sock, fd, BATCHSIZE );
        if ( rc < 0 ) {
            break;
        }
        tot += rc;
    }

    if ( remain > 0 ) {
        rc = sendOneBatch( sock, fd, remain );
    }
    if ( rc > 0 ) {
        tot += rc;
    }

    return tot;

}
#endif

// -1: error
jagint sendOneBatch( int sock, int fd, jagint size )
{
	jagint remain = size;
	jagint onesendbytes;
	while ( remain > 0 ) {
		prt(("u022294 try sendfile remain=%lld ...\n", remain ));
		onesendbytes = ::sendfile( sock, fd, NULL, remain );
		if ( onesendbytes < 0 ) {
			prt(("u022294 sendfile got %d return -1\n", onesendbytes ));
			return -1;
		}
		prt(("u022295 sendfile onesendbytes=%lld\n", onesendbytes ));
		remain = remain - onesendbytes;
	}
	prt(("u022298 remain=%lld done\n", remain ));
	return size;
}

Jstr psystem( const char *command )
{
	Jstr res;
	FILE *fp = popen(command, "r" );
	if ( ! fp ) {
		res = Jstr("Error execute ") + command; 
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
bool checkCmdTimeout( jagint startTime, jagint timeoutLimit )
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
	Jstr out;
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

Jstr expandEnvPath( const Jstr &path )
{
	// prt(("s29393 expandEnvPath input=[%s]\n", path.c_str() ));
    if ( '$' != *(path.c_str()) ) return path;

    JagStrSplit sp( path, '/' );
    Jstr fs = sp[0];
	const char *p = fs.c_str() + 1;
	// prt(("s2238 fs=[%s] p=[%s]\n", fs.c_str(), p ));
	if ( getenv(p) ) {
		return Jstr(getenv(p)) + Jstr(path.c_str() + fs.size());
	} else {
		return Jstr(".") + Jstr(path.c_str() + fs.size());
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

int formatInsertSelectCmdHeader( const JagParseParam *parseParam, Jstr &str )
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
		if ( ! isValidNameChar( *name ) ) {
			return false;
		}
		++name;
	}
	return true;
}

// true: name is "wjdjdAJ71828_jdjd"
bool isValidCol( const char *name )
{
	while ( *name ) {
		if ( ! isValidColChar( *name ) ) {
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
jagint _getFieldInt( const char * rowstr, char fieldToken )
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
Jstr makeStringFromOneVec( const JagVector<Jstr> &vec, int dquote )
{
	Jstr res;
	int len = vec.length();
	for ( int i =0; i < len; ++i ) {
		if ( dquote ) {
			res += Jstr("\"") + vec[i] + "\"";
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
Jstr makeStringFromTwoVec( const JagVector<Jstr> &xvec, const JagVector<Jstr> &yvec )
{
	Jstr res;
	int len = xvec.length();
	if ( len != yvec.length() ) return "";
	for ( int i =0; i < len; ++i ) {
		res += Jstr("[") + xvec[i] + "," + yvec[i] + "]";
		if ( i < len-1 ) { res += ","; } 
	}
	return res;
}

int oneFileSender( JAGSOCK sock, const Jstr &inpath )
{
	prt(("\ns4009 oneFileSender sock=%d THRD=%lld inpath=[%s] ...\n", sock, THREADID, inpath.c_str() ));
	Jstr filename;
	char *newbuf = NULL; 
	ssize_t rlen = 0; struct stat sbuf; Jstr cmd; int fd = -1;
	jagint filelen;

	prt(("s2838 oneFileSender inpath=[%s]\n", inpath.c_str() ));
    const char *p = strrchr( inpath.c_str(), '/' );
    if ( p == NULL ) {
		p = inpath.c_str();
		filename = p;
    } else {
		filename = p+1;
	}

	prt(("s0384 filename=[%s] inpath=[%s]\n", filename.c_str(), inpath.c_str() ));
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
		prt(("s0283 sendFakeData = 1\n" ));
		filename = ".";  // means no data
	} else {
		sendFakeData = 0;
		filelen = sbuf.st_size;
		prt(("s0283 sendFakeData = 0\n" ));
	}

	cmd = "_onefile|" + filename + "|" + longToStr( filelen ) + "|" + longToStr(THREADID);
	char cmdbuf[JAG_SOCK_TOTAL_HDR_LEN+cmd.size()+1]; 
	memset( cmdbuf, ' ', JAG_SOCK_TOTAL_HDR_LEN+cmd.size()+1);
	char sqlhdr[8]; makeSQLHeader( sqlhdr );
	putXmitHdrAndData( cmdbuf, sqlhdr, cmd.c_str(), cmd.size(), "ATFC" );

	prt(("s2309 sendRawData cmdbuf=[%s]\n", cmdbuf ));
	rlen = sendRawData( sock, cmdbuf, JAG_SOCK_TOTAL_HDR_LEN+cmd.size() ); // 016ABCDmessage client query mode
	if ( rlen < JAG_SOCK_TOTAL_HDR_LEN+cmd.size() ) {
		prt(("s0822 sendRawData error\n" ));
		sendFakeData = 1;
	} else {
		prt(("c00271 sendRawData rlen=%d OK  cmdbuf=[%s]\n", rlen, cmdbuf ));
	}

	if ( ! sendFakeData ) {
		prt(("s2293 jagsendfile fd=%d sbuf.st_size=%lld ...\n", fd, sbuf.st_size ));
		beginBulkSend( sock );
		rlen = jagsendfile( sock, fd, sbuf.st_size );
		endBulkSend( sock );
		prt(("s2293 jagsendfile done rlen=%lld\n", rlen ));
	}

	if ( fd >= 0 ) jagclose( fd );
	if ( newbuf ) free( newbuf );
	return 1;
}

// return 1: OK
//   -1: error not _onefile command
//   -2: error invalid _onefile command
//   -3: fake data  -4: file open error
int oneFileReceiver( JAGSOCK sock, const Jstr &outpath, bool isDirPath )
{	
	if ( 0 == sock ) {
		return 0;
	}

	// prt(("c0293 oneFileReceiver outpath=[%s]\n", outpath.c_str() ));
	int fd = -1; 
	jagint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT;
	Jstr filename, senderID, recvpath;
	char *newbuf = NULL; 
	char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
	ssize_t rlen = 0;

	while ( 1 ) {
    	rlen = recvMessage( sock, hdr, newbuf ); // "_onefile|...."
		if ( 0 == rlen ) {
			continue;
		}
		if ( rlen < 0 ) {
			return 0;
		}

		if ( hdr[JAG_SOCK_TOTAL_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_TOTAL_HDR_LEN-2] == 'B' ) {
			continue;
		}

    	if ( 0 != strncmp( newbuf, "_onefile|", 9 ) ) {
    		prt(("s0293 _onefile error newbuf=[%s]\n", newbuf ));
    		if ( newbuf ) free(newbuf );
    		return -1;
    	} 

		break; // got _onefile command
	}

	// if _onefile recved, parse and check the outpath is valid to recv
	// cmd = "_onefile|" + filename + "|" + longToStr( filelen ) + "|" + longToStr(THREADID);
	JagStrSplit sp( newbuf, '|', true );
	if ( sp.length() <  4 ) return -100;
	filename = sp[1];
	// get fake data
	if ( filename == "." ) {
    	if ( newbuf ) free(newbuf );
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
		rlen = recvRawData( sock, mbuf, recvlen );
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

	prt(("s1239 end recv file %s totlen=%lld\n", recvpath.s(), totlen ));
	/**
	prt(("s2736 send _filedone to client ...\n" ));
	sendDirectToSock(sock, "_filedone", 0);
	prt(("s2736 send _filedone to client done\n" ));
	**/

	if ( mbuf ) free( mbuf );
	if ( newbuf ) free( newbuf );
	return 1;
}

// method to send data directly to sock
jagint sendDirectToSock( JAGSOCK sock, const char *mesg, jagint len, bool nohdr )
{
	jagint rlen;
	if ( nohdr ) {
		rlen = sendRawData( sock, mesg, len ); // 016ABCDmessage client query mode
	} else {
		char *buf =(char*)jagmalloc(len+JAG_SOCK_TOTAL_HDR_LEN+1);
		char sqlhdr[8];  makeSQLHeader( sqlhdr );
		putXmitHdrAndData( buf, sqlhdr, mesg, len, "ATFC" );
		rlen = sendRawData( sock, buf, len+JAG_SOCK_TOTAL_HDR_LEN ); // 016ABCDmessage client query mode
		if ( buf ) free( buf );
	}
	return rlen;
}

// method to recv data directly from sock
jagint recvDirectFromSock( JAGSOCK sock, char *&buf, char *hdr )
{
	jagint rlen = 0;
	while ( 1 ) {
		prt(("u208737 util recvMesage ...\n"));
		// if ( buf ) { buf[0] = '\0'; }
		rlen = recvMessage( sock, hdr, buf );
		//prt(("c201139 got hdr=[%s] buf=[%s] rlen=%lld\n", hdr, buf, rlen ));
		if ( rlen > 0 && (hdr[JAG_SOCK_TOTAL_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_TOTAL_HDR_LEN-2] == 'B') ) { 
		    // heartbit alive, ignore
			prt(("c201134 HB \n" ));
			continue;
		}
		break;
	}
	prt(("u208737 util recvMesage done rlen=%lld\n", rlen));
	return rlen;
}

// method to send data directly to sock
// len is length of mesg, not including hdr
// if original data is compressed ( checked from orig hdr ), then rebuild mesg with compressed data
jagint sendDirectToSockWithHdr( JAGSOCK sock, const Jstr &shdr, const Jstr &mesg )
{
	jagint len = mesg.size();
	const char *p = mesg.c_str();
	const char *hdr = shdr.c_str();
	jagint rlen;
	Jstr comp;
	if ( hdr[JAG_SOCK_TOTAL_HDR_LEN-4] == 'Z' ) {
		JagFastCompress::compress( mesg.c_str(), len, comp );
		p = comp.c_str();
		len = comp.size();
    }

	rlen = sendRawData( sock, hdr, JAG_SOCK_TOTAL_HDR_LEN );
	rlen = sendRawData( sock, p, len );
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
Jstr fileHashDir( const JagFixString &fstr )
{
	jagint hcode = fstr.hashCode();
	char buf[8];
	sprintf( buf, "%d/%d", hcode % 1000, (hcode/1000) % 1000 ); 
	return buf;
}

char lastChar( const JagFixString &str )
{
	return *( str.c_str() + str.size()-1);
}

// ignore NULL in str
void jagfwrite( const char *str, jagint len, FILE *outf )
{
	for ( int i = 0; i < len; ++i ) {
		if ( str[i] != '\0' ) {
			fputc( str[i], outf );
		}
	}
}

// ignore leading zeros and  NULL in str
void jagfwritefloat( const char *str, jagint len, FILE *outf )
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

void charFromStr( char *dest, const Jstr &src )
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
    if ( c == '<' ) return false;
    if ( c == '>' ) return false;
    if ( c == '`' ) return false;
    if ( c == '~' ) return false;
    if ( c == '&' ) return false;

	// supports UTF chars
    return true;
}

bool isValidColChar( char c )
{
    if ( c == ':' ) return true;
	return isValidNameChar( c );
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

long long jagatoll(const Jstr &str )
{
	return jagatoll( str.c_str() );
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

double jagatof(const Jstr &str )
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

#if 0
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
#endif


// trim tailing zeroes "33.000" --> "33"   "222.020000" --> "222.02"
// "-2233.001000" --> "-2233.001"
void stripTailZeros( char *buf, int len )
{
    if ( NULL == buf || *buf == '\0' ) return;
    if ( len < 2 ) return;

    char *p = buf+len-1;
    while ( p >= buf+1 ) {
        if ( *p == '0' || *p == '.' ) {
            *p = '\0';
            --p;
            continue;
        } else {
            break;
        }
    }

    if ( buf[1] == '\0' ) {
        if ( buf[0] == '.' || buf[0] == '+' || buf[0] == '-' ) {
            buf[0] = '0';
        }
    } else {
        if ( *p == '.' ) {
            *p = '\0';
        }
	}
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
Jstr trimEndZeros( const Jstr& str )
{
	if ( ! strchr( str.c_str(), '.') ) return str;
	Jstr res;

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

Jstr replaceChar( const Jstr& str, char oldc, char newc )
{
	Jstr res;
	for ( int i=0; i < str.size(); ++i ) {
		if ( str[i] == oldc ) {
			res += newc;
		} else {
			res += str[i];
		}
	}
}

void prttr( const Jstr &str )
{
	printf("prttr size=%d: \n[", str.size() );
	for ( int i=0; i < str.size(); ++i ) {
		if ( str[i] == '\0' ) {
			putchar('@');
		} else {
			putchar( str[i] );
		}
	}
	printf("]\n\n");
}

// str: "tok1  tok2 ..."  return pointer to "tok2 .."
char *secondTokenStart( const char *str, char sep )
{
	if ( NULL == str || *str == 0 ) return NULL;
	char *p = (char*) str;
	while ( *p == sep ) ++p;  // "  abc  mdef"  p is at a
	while ( *p != sep && *p != '\0' ) ++p; // p is at pos after c
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;  // p is at m
	return p;
}

// str: "tok1  tok2 tok3 ..."  return pointer to "tok3 .."
char *thirdTokenStart( const char *str, char sep )
{
	if ( NULL == str || *str == 0 ) return NULL;
	char *p = (char*) str;
	while ( *p == sep ) ++p;  // "  abc  mdef nfe"  p is at a
	while ( *p != sep && *p != '\0' ) ++p; // p is at pos after c
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;  // p is at m

	while ( *p != sep && *p != '\0' ) ++p; // p is at pos after c
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;  // p is at n

	return p;
}

char *secondTokenStartEnd( const char *str, char *&pend, char sep )
{
	if ( NULL == str || *str == 0 ) return NULL;
	char *p = (char*) str;
	while ( *p == sep ) ++p;  // "  abc  mdef"  p is at a
	while ( *p != sep && *p != '\0' ) ++p; // p is at pos after c
	if ( *p == '\0' ) return NULL;
	while ( *p == sep ) ++p;  // p is at m
	pend = p; 
	while ( *pend != sep && *pend != '\0' ) ++pend;
	return p;
}


//  xm, xh, xd, xw
jagint convertToSecond( const char *p )
{
	jagint num, n = jagatol(p);
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
jagint convertToMicroSecond( const char *p )
{
	jagint num, n = jagatol(p);
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
bool likeMatch( const Jstr& str, const Jstr& like )
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

// check first n chars in s for c
char *strnchr(const char *s, int c, int n)
{
	if ( ! s || *s=='\0' ) return 0;
	char *p = (char*)s;
	while ( *p != '\0' ) {
		if ( p-s >= n ) break;
		if ( *p == c ) return p;
		++p;
	}
	return NULL;
}


bool isNumeric( const char *str) 
{
	while ( *str != '\0' ) {
        if ( ! isdigit(*str) && *str != '.' ) {
            return false;
        } else {
			++str;
		}
    }
    return true;
}

// rotate counter-clock-wise of point oldx,oldy around point x0,y0
// alpha is radians
void rotateat( double oldx, double oldy, double alpha, double x0, double y0, double &x, double &y )
{
	x = x0 + (oldx-x0)*cos(alpha) - (oldy-y0)*sin(alpha);
	y = y0 + (oldy-y0)*cos(alpha) + (oldx-x0)*sin(alpha);
}

void rotatenx( double oldnx, double alpha, double &nx )
{
	if ( fabs(oldnx) > 1.0 || fabs( fabs(oldnx) - 1.0 ) < 0.00000001 ) {
		nx = 1.0;
		return;
	}
	double oldny = sqrt(1.0 - oldnx*oldnx);
	nx = oldnx*cos(alpha) - oldny*sin(alpha);
}

void affine2d( double x1, double y1, double a, double b, double d, double e,
                double dx, double dy, double &x, double &y )
{
	x = a*x1 + b*y1 + dx;
	y = d*x1 + e*y1 + dy;
}

void affine3d( double x1, double y1, double z1, double a, double b, double c, double d, double e,
                double f, double g, double h, double i, double dx, double dy, double dz,
                double &x, double &y, double &z )
{
	x = a*x1 + b*y1 + c*z1 + dx;
	y = d*x1 + e*y1 + f*z1 + dy;
	z = g*x1 + h*y1 + i*z1 + dz;
}

void ellipseBoundBox( double x0, double y0, double a, double b, double nx, 
					  double &xmin, double &xmax, double &ymin, double &ymax )
{
	// nx is sin(theta) -- theta is angle of rotating shape clock-wise
	// ny=sqrt(1.0-nx*nx) and == cos(theta)=ny
	// xmin = - sqrt( a^2 * ny^2 + b^2 * nx^2 )
	// xmax = + sqrt( a^2 * ny^2 + b^2 * nx^2 )
	// ymin = - sqrt( a^2 * nx^2 + b^2 * ny^2 )
	// ymax = + sqrt( a^2 * nx^2 + b^2 * ny^2 )

	if ( nx > 1.0 ) nx = 1.0;
	if ( nx < -1.0 ) nx = -1.0;

	if ( jagEQ(nx, 0.0) ) {
		xmin = x0-a;
		xmax = x0+a;
		ymin = y0-b;
		ymax = y0+b;
		return;
	}

	if ( jagEQ(fabs(nx), 1.0) ) {
		xmin = x0-b;
		xmax = x0+b;
		ymin = y0-a;
		ymax = y0+a;
		return;
	}

	double nx2 = nx * nx;
	double ny2 = 1.0-nx2;
	double sqrt1 = sqrt( a*a*ny2 + b*b*nx2 );
	double sqrt2 = sqrt( a*a*nx2 + b*b*ny2 );

	xmin = x0 - sqrt1;
	xmax = x0 + sqrt1;
	ymin = y0 - sqrt2;
	ymax = y0 + sqrt2;
}


void ellipseMinMax(int op, double x0, double y0, double a, double b, double nx, 
					  double &xmin, double &xmax, double &ymin, double &ymax )
{
	// nx is sin(theta) -- theta is angle of rotating shape clock-wise
	// ny=sqrt(1.0-nx*nx) and == cos(theta)=ny
	// newx = xcos(theta) + ysin(theta)
	// newy = ycos(theta) - xsin(theta)
	// xmin = - sqrt( a^2 * ny^2 + b^2 * nx^2 )
	// xmax = + sqrt( a^2 * ny^2 + b^2 * nx^2 )
	// ymin = - sqrt( a^2 * nx^2 + b^2 * ny^2 )
	// ymax = + sqrt( a^2 * nx^2 + b^2 * ny^2 )
	xmin = ymin = xmax = ymax = 0.0;
	if ( jagEQ(a, 0.0) && jagEQ( b, 0.0) ) {
		return;
	}

	if ( nx > 1.0 ) nx = 1.0;
	if ( nx < -1.0 ) nx = -1.0;

	if ( jagEQ(nx, 0.0) ) {
		if ( JAG_FUNC_XMINPOINT == op ) {
			xmin = x0 - a;
			ymin = y0;
		} else if ( JAG_FUNC_YMINPOINT == op ) {
			xmin = x0;
			ymin = y0 - b;
		} else if ( JAG_FUNC_XMAXPOINT == op ) {
			xmax = x0 + a;
			ymax = y0;
		} else if ( JAG_FUNC_YMAXPOINT == op ) {
			xmax = x0;
			ymax = y0 + b;
		}
		return;
	}

	double nx2 = nx * nx;
	double ny2 = 1.0-nx2;
	double ny = sqrt(ny2);

	double A, B;
	if ( JAG_FUNC_XMINPOINT == op ) {
		double sqrt1 = sqrt( a*a*ny2 + b*b*nx2 );
		A = b*b*nx2 + a*a * ny2;
		B = 2.0*(b*b - a*a)*sqrt1*nx*ny;
		xmin = x0 - sqrt1;
		ymin = y0 - 0.5*B/A;
	} else if ( JAG_FUNC_XMAXPOINT == op ) {
		double sqrt1 = sqrt( a*a*ny2 + b*b*nx2 );
		A = b*b*nx2 + a*a * ny2;
		B = 2.0*(a*a - b*b)*sqrt1*nx*ny;
		xmax = x0 + sqrt1;
		ymax = y0 - 0.5*B/A;
	} else if ( JAG_FUNC_YMINPOINT == op ) {
		double sqrt2 = sqrt( a*a*nx2 + b*b*ny2 );
		A = b*b*ny2 + a*a * nx2;
		B = 2.0*(b*b-a*a)*sqrt2*nx*ny;
		ymin = y0 - sqrt2;
		xmin = x0 - 0.5*B/A;
	} else if ( JAG_FUNC_YMAXPOINT == op ) {
		double sqrt2 = sqrt( a*a*nx2 + b*b*ny2 );
		A = b*b*ny2 + a*a * nx2;
		B = 2.0*(a*a - b*b)*sqrt2*nx*ny;
		ymax = y0 + sqrt2;
		xmax = x0 - 0.5*B/A;
	}
}

#if 0
void ellipseBoundBox( double x0, double y0, double a, double b, double nx, 
					  double &xmin, double &xmax, double &ymin, double &ymax )
{
	// nx is sin(phi) -- phi is angle of rotating shape clock-wise
	// sqrt(1.0-nx*nx) is cos(phi)=ny
	// x = x0+a*cos(t)*cos(phi) + b*sin(t)*sin(phi)  
	// y = y0 + b*sin(t)*cos(phi) - a*cos(t)*sin(phi)
	// 0 == dx/dt = -asin(t)*cos(phi) +bcos(t)sin(phi)  --> tan(t) = (b/a)*sin(phi)/cos(phi)=v
	// t1 = atan(v)  t2=JAG_PI + t;
	// x1 = .. y1 == use t1 
	// x2 = .. y2 == use t2 
	if ( nx > 1.0 ) nx = 1.0;
	if ( nx < -1.0 ) nx = -1.0;

	if ( jagEQ(nx, 0.0) ) {
		xmin = x0-a;
		xmax = x0+a;
		ymin = y0-b;
		ymax = y0+b;
		return;
	}

	if ( jagEQ(fabs(nx), 1.0) ) {
		xmin = x0-b;
		xmax = x0+b;
		ymin = y0-a;
		ymax = y0+a;
		return;
	}

	double ny = sqrt(1.0-nx*nx);
	double v = (b/a)*(nx/ny);
	double t1 = atan(v);
	double t2 = t1 + JAG_PI;
	double x1 = x0 + a*cos(t1)*ny + b*sin(t1)*nx;
	double y1 = y0 + b*sin(t1)*ny - b*cos(t1)*nx;
	double x2 = x0 + a*cos(t2)*ny + b*sin(t2)*nx;
	double y2 = y0 + b*sin(t2)*ny - b*cos(t2)*nx;
	xmin = jagmin(x1,x2);
	xmax = jagmax(x1,x2);
	ymin = jagmin(y1,y2);
	ymax = jagmax(y1,y2);
}
#endif



double dotProduct( double x1, double y1, double x2, double y2 )
{
    return ( x1*x2 + y1*y2 );
}
double dotProduct( double x1, double y1, double z1, double x2, double y2, double z2 )
{
    return ( x1*x2 + y1*y2 + z1*z2 );
}

void crossProduct( double x1, double y1, double z1, double x2, double y2, double z2,
                           double &x, double &y, double &z )
{
    x = y1*z2 - z1*y2;
    y = z1*x2 - x1*z2;
    z = x1*y2 - y1*x2;
}


bool jagLE (double f1, double f2 )
{
    if ( f1 < f2 ) return true;
    if ( fabs(f1-f2) < JAG_ZERO ) return true;
    return false;
}

bool jagGE (double f1, double f2 )
{
    if ( f1 > f2 ) return true;
    if ( fabs(f1-f2) < JAG_ZERO ) return true;
    return false;
}

bool jagEQ (double f1, double f2 )
{
    if ( fabs(f1-f2) < JAG_ZERO ) return true;
    return false;
}


// strlen(code)  must be 4 code=XXXX (ie. ACCC)
// [              sqlhdr][                        ]msgdata
// [JAG_SOCK_SQL_HDR_LEN][JAG_SOCK_MSG_HDR_LENXXXX]msgdata
// [    3 bytes         ][   13 bytes             ]msgdata
// [               JAG_SOCK_TOTAL_HDR_LEN         ]msgdata
// sqlhdr shoule be 2 bytes
void putXmitHdr( char *buf, const char *sqlhdr, int msglen, const char *code )
{
	int blanksz = JAG_SOCK_SQL_HDR_LEN-strlen(sqlhdr);
    memset(buf, '#', blanksz );
    sprintf( buf+blanksz, "%s", sqlhdr );
    sprintf( buf+JAG_SOCK_SQL_HDR_LEN, "%0*lld%s", JAG_SOCK_MSG_HDR_LEN-4, msglen, code );
}

// strlen(code)  must be 4 code=XXXX
// [               qlhdr][                        ]msgdata
// [JAG_SOCK_SQL_HDR_LEN][JAG_SOCK_MSG_HDR_LENXXXX]msgdata
// [               JAG_SOCK_TOTAL_HDR_LEN         ]msgdata
void putXmitHdrAndData( char *buf, const char *sqlhdr, const char *msg, int msglen, const char *code )
{
	int blanksz = JAG_SOCK_SQL_HDR_LEN-strlen(sqlhdr);
    memset(buf, '#', blanksz );
    sprintf( buf+blanksz, "%s", sqlhdr );
    sprintf( buf+JAG_SOCK_SQL_HDR_LEN, "%0*lld%s", JAG_SOCK_MSG_HDR_LEN-4, msglen, code );
    memcpy( buf+JAG_SOCK_TOTAL_HDR_LEN, msg, msglen );
    buf[JAG_SOCK_TOTAL_HDR_LEN+msglen] = '\0';
}

void getXmitSQLHdr( char *buf, char *sqlhdr )
{
	char *p = buf;
	int blanklen;
	while ( *p == '#' ) ++p;  // assume front is '#'
	blanklen = p-buf;
	for ( int i=0; i < JAG_SOCK_SQL_HDR_LEN-blanklen; ++i ) {
		*sqlhdr = *p;
		++p;
		++sqlhdr;
	}
	*sqlhdr = '\0';
}

void getXmitCode( char *buf, char *code )
{
	memcpy( code, buf+JAG_SOCK_TOTAL_HDR_LEN-4, 4 );
}

long long getXmitMsgLen( char *buf )
{
	char c = buf[JAG_SOCK_TOTAL_HDR_LEN-4];
	buf[JAG_SOCK_TOTAL_HDR_LEN-4]='\0';
	long long n = atoll( buf+JAG_SOCK_SQL_HDR_LEN );
	//prt(("s2088893 getXmitMsgLen() dumpmem : n=%lld\n", n ));
	//dumpmem( buf+JAG_SOCK_SQL_HDR_LEN, JAG_SOCK_MSG_HDR_LEN ); // todel
	buf[JAG_SOCK_TOTAL_HDR_LEN-4]=c;
	return n;
}

// Hope to have 2 bytes
void makeSQLHeader( char *sqlhdr )
{
	for ( int i=0; i < JAG_SOCK_SQL_HDR_LEN; ++i ) {
		sqlhdr[i] = '9';
	}
	sqlhdr[JAG_SOCK_SQL_HDR_LEN] = '\0';
}

// tothdr must have tothdr[JAG_SOCK_TOTAL_HDR_LEN+1] enough length
// payloadLen is payload length in bytes, code is 4-letter code "ACCC" is non compressed
void makeTotalHeader( char *tothdr, jagint payloadLen, const char *code4 )
{
    char sqlhdr[8]; makeSQLHeader( sqlhdr );
    int blanksz = JAG_SOCK_SQL_HDR_LEN-strlen(sqlhdr);
    memset(tothdr, '#', blanksz );
    sprintf( tothdr+blanksz, "%s", sqlhdr );
    sprintf( tothdr+JAG_SOCK_SQL_HDR_LEN, "%0*lld%s", JAG_SOCK_MSG_HDR_LEN-4, payloadLen, code4 );
}

// Send data with sqlhdr
jagint sendShortMessageToSock( JAGSOCK sock, const char *msg, jagint msglen, const char *code4 )
{
	char buf[JAG_SOCK_TOTAL_HDR_LEN+msglen+1];
	makeTotalHeader( buf, msglen, code4 );
	memcpy( buf + JAG_SOCK_TOTAL_HDR_LEN, msg, msglen );
	return sendRawData( sock, buf, JAG_SOCK_TOTAL_HDR_LEN + msglen );
}

// sp: OJAG=0=test.lstr.ls=LS guarantee 3 '=' signs
// str: "x:y x:y x:y ..." or "x:y:z x:y:z x:y:z ..."
Jstr makeGeoJson( const JagStrSplit &sp, const char *str )
{
	//prt(("s3391 makeGeoJson sp[3]=[%s]\n", sp[3].c_str() ));
	//prt(("s3392 sp.print: \n" ));
	//sp.print();

	if ( sp[3] == JAG_C_COL_TYPE_LINESTRING ) {
		return makeJsonLineString("LineString", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_LINESTRING3D ) {
		return makeJsonLineString3D( "LineString", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOINT ) {
		return makeJsonLineString("MultiPoint", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return makeJsonLineString3D("MultiPoint", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_POLYGON ) {
		return makeJsonPolygon( "Polygon", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_POLYGON3D ) {
		return makeJsonPolygon( "Polygon", sp, str, true );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return makeJsonPolygon("MultiLineString", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return makeJsonPolygon( "MultiLineString", sp, str, true );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return makeJsonMultiPolygon( "MultiPolygon", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return makeJsonMultiPolygon( "MultiPolygon", sp, str, true );
	} else {
		return makeJsonDefault( sp, str) ;
	}
}

/******************************************************************
** GeoJSON supports the following geometry types: 
** Point, LineString, Polygon, MultiPoint, MultiLineString, and MultiPolygon. 
** Geometric objects with additional properties are Feature objects. 
** Sets of features are contained by FeatureCollection objects.
** https://tools.ietf.org/html/rfc7946
*******************************************************************/

// sp: OJAG=0=test.lstr.ls=LS guarantee 3 '=' signs
// str: "xmin:ymin:xmax:ymax x:y x:y x:y ..." 
/*********************
    {
       "type": "Feature",
       "bbox": [-10.0, -10.0, 10.0, 10.0],
       "geometry": {
           "type": "LineString",
           "coordinates": [
                   [-10.0, -10.0],
                   [10.0, -10.0],
                   [10.0, 10.0],
                   [-10.0, -10.0]
           ]
       }
       //...
    }

    {
       "type": "Feature",
       "bbox": [-10.0, -10.0, 10.0, 10.0],
       "geometry": {
           "type": "Polygon",
           "coordinates": [
               [
                   [-10.0, -10.0],
                   [10.0, -10.0],
                   [10.0, 10.0],
                   [-10.0, -10.0]
               ]
           ]
       }
       //...
    }
****************/
Jstr makeJsonLineString( const Jstr &title, const JagStrSplit &sp, const char *str )
{
	//prt(("s2980 makeJsonLineString str=[%s]\n", str ));
	const char *p = str;
	//while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	Jstr s;
	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	JagStrSplit bsp( Jstr(str, p-str, p-str), ':' );
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;
	prt(("s1038 p=[%s]\n", p ));

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		while( *q != '\0' ) {
			//prt(("s2029 q=[%s]\n", q ));
			writer.StartArray(); 
			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			s = Jstr(p, q-p, q-p);
			prt(("s3941 s=[%s]\n", s.s() ));
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s2039 q=[%s]\n", q ));
			p = q;
			//while (*q != ' ' && *q != '\0' ) ++q;
			while ( *q != ' ' && *q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				//writer.String( p );
				writer.Double( jagatof(p) );
				writer.EndArray(); 
				break;
			}
			// *q == ':' or ' '

			s = Jstr(p, q-p, q-p);
			prt(("s3942 s=[%s]\n", s.s() ));
			//prt(("s2339 q=[%s]\n", q ));
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			writer.EndArray(); 
			//*q = ' ';

			while (*q != ' ' && *q != '\0' ) ++q;
			while (*q == ' ' ) ++q;
			p = q;
			//prt(("s1336 q=[%s]\n", q ));
		}
		writer.EndArray(); 
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
	    writer.String( "2" );
	writer.EndObject();

	writer.EndObject();

	//prt(("s0301 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

Jstr makeJsonLineString3D( const Jstr &title, const JagStrSplit &sp, const char *str )
{
	//prt(("s0823 makeJsonLineString3D str=[%s]\n", str ));

	const char *p = str;
	//while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	Jstr s;
	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	JagStrSplit bsp( Jstr(str, p-str, p-str), ':' );
	if ( bsp.length() >= 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p;  // "x:y:z x:y:z x:y:z ..."
	char *q = (char*)p;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    // writer.String("LineString");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		while( *q != '\0' ) {
			//prt(("s8102 q=[%s]\n", q ));
			writer.StartArray(); 

			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			//*q = '\0';
			s = Jstr(p, q-p, q-p);
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s8103 q=[%s]\n", q ));
			p = q;
			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			//*q = '\0';
			s = Jstr(p, q-p, q-p);
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s8104 q=[%s]\n", q ));
			p = q;
			//while (*q != ' ' && *q != '\0' ) ++q;
			while ( *q != ' ' && *q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				//writer.String( p );
				writer.Double( jagatof(p) );
				writer.EndArray(); 
				break;
			}

			//*q = '\0';
			s = Jstr(p, q-p, q-p);
			// writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			writer.EndArray(); 
			//*q = ' ';

			while (*q != ' ' && *q != '\0' ) ++q;
			while (*q == ' ' ) ++q;
			//prt(("s8105 q=[%s]\n", q ));
			p = q;
		}
		writer.EndArray(); 
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
	    writer.String( "3" );
	writer.EndObject();

	writer.EndObject();

	return (char*)bs.GetString();
}

/********************************************************
{
   "type": "Polygon",
   "coordinates": [
       [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],
       [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]
   ]
}
********************************************************/
Jstr makeJsonPolygon( const Jstr &title,  const JagStrSplit &sp, const char *str, bool is3D )
{
	//prt(("s7081 makeJsonPolygon str=[%s] is3D=%d\n", str, is3D ));

	const char *p = str;
	//while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	Jstr bbox(str, p-str, p-str);
	//prt(("s5640 bbox=[%s]\n", bbox.c_str() ));
	JagStrSplit bsp( bbox, ':' );
	//prt(("s5732 bsp.len=%d\n", bsp.length() ));
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;
	Jstr s;
	//int level = 0;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    //writer.String("Polygon");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		//++level;
		bool startRing = true;
		while( *q != '\0' ) {
			while (*q == ' ' ) ++q; 
			p = q;
			//prt(("s2029 q=[%s] level=%d p=[%s]\n", q, level, p ));
			if ( startRing ) {
				writer.StartArray(); 
				//++level;
				startRing = false;
			}

			while (*q != ':' && *q != '\0' && *q != '|' ) ++q;
			//prt(("s2132 q=[%s] level=%d\n", q, level ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s3362 level=%d break\n", level ));
				break;
			}
			//*q = '\0';
			//s = Jstr(p, q-p);

			if ( *q == '|' ) {
				writer.EndArray(); // outeraray
				startRing = true;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			s = Jstr(p, q-p, q-p);

			writer.StartArray(); 
			//++level;
			//prt(("s6301 write xcoord s=[%s]\n", s.c_str() ));
			//writer.String( p );   // x-coord
			//writer.String( s.c_str(), s.size() );   // x-coord
			writer.Double( jagatof(s.c_str()) );
			//*q = save;

			++q;
			p = q;
			//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
			if ( is3D ) {
				while ( *q != ':' && *q != '\0' && *q != '|' ) ++q;
			} else {
				//while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
				while ( *q != ' ' && *q != ':' && *q != '\0' && *q != '|' ) ++q;
			}

			s = Jstr(p, q-p, q-p);
			//prt(("s6302 write ycoord s=[%s]\n", s.c_str() ));
			//writer.String( s.c_str(), s.size() );   // y-coord
			writer.Double( jagatof(s.c_str()) );

			if ( is3D && *q != '\0' ) {
				++q;
				p = q;
				//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
				//while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
				while ( *q != ' ' && *q != ':' && *q != '\0' && *q != '|' ) ++q;
				//prt(("s6303 write zcoord s=[%s]\n", s.c_str() ));
				s = Jstr(p, q-p, q-p);
				//writer.String( s.c_str(), s.size() );   // z-coord
				writer.Double( jagatof(s.c_str()) );
			}

			writer.EndArray(); // inner raray
			//--level;

			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s3162 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outer raray
				startRing = true;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			while (*q != ' ' && *q != '\0' ) ++q;  // goto next x:y coord
			while (*q == ' ' ) ++q;  // goto next x:y coord
			//prt(("s2339 q=[%s]\n", q ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s5862 level=%d break \n", level ));
				break;
			}

			p = q;
		}

		writer.EndArray(); 
		//--level;
		//prt(("s5869 level=%d outside loop \n", level ));
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
		if ( is3D ) {
	    	writer.String( "3" );
		} else {
	    	writer.String( "2" );
		}
	writer.EndObject();

	writer.EndObject();

	//prt(("s0303 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

/***********************************************************************************
{
   "type": "MultiPolygon",
   "coordinates": [
       [
           [ [102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0] ]
       ],
       [
           [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],
           [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]
       ]
   ]
}
***********************************************************************************/
Jstr makeJsonMultiPolygon( const Jstr &title,  const JagStrSplit &sp, const char *str, bool is3D )
{
	//prt(("s7084 makeJsonMultiPolygon str=[%s] is3D=%d\n", str, is3D ));

	const char *p = str;
	//while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	Jstr bbox(str, p-str, p-str);
	//prt(("s5640 bbox=[%s]\n", bbox.c_str() ));
	JagStrSplit bsp( bbox, ':' );
	//prt(("s5732 bsp.len=%d\n", bsp.length() ));
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;
	Jstr s;
	//int level = 0;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		//++level;
		bool startPolygon = true;
		bool startRing = true;
		while( *q != '\0' ) {
			while (*q == ' ' ) ++q; 
			p = q;
			//prt(("s2029 q=[%s] level=%d p=[%s]\n", q, level, p ));

			if ( startPolygon ) {
				writer.StartArray(); 
				//++level;
				startPolygon = false;
			}

			if ( startRing ) {
				writer.StartArray(); 
				//++level;
				startRing = false;
			}

			while (*q != ':' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			//prt(("s2132 q=[%s] level=%d\n", q, level ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s3362 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outeraray
				startRing = true;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			if ( *q == '!' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				startPolygon = true;
				startRing = true;
				//--level;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			s = Jstr(p, q-p, q-p);

			writer.StartArray(); 
			//++level;
			//prt(("s6301 write xcoord p=[%s]\n", p ));
			//writer.String( s.c_str(), s.size() );   // x-coord
			writer.Double( jagatof(s.c_str()) );

			++q;
			p = q;
			//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
			if ( is3D ) {
				while ( *q != ':' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			} else {
				while ( *q != ' ' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			}

			s = Jstr(p, q-p, q-p);
			//writer.String( s.c_str(), s.size() );   // y-coord
			writer.Double( jagatof(s.c_str()) );

			if ( is3D && *q != '\0' ) {
				++q;
				p = q;
				//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
				while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
				s = Jstr(p, q-p, q-p);
				//writer.String( s.c_str(), s.size() );   // z-coord
				writer.Double( jagatof(s.c_str()) );
			}

			writer.EndArray(); // inner raray
			//--level;

			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s3162 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outer raray
				startRing = true;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			if ( *q == '!' ) {
				writer.EndArray(); // outer raray
				writer.EndArray(); // outer raray
				startPolygon = true;
				startRing = true;
				//--level;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			while (*q == ' ' ) ++q;  // goto next x:y coord
			//prt(("s2339 q=[%s]\n", q ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s5862 level=%d break \n", level ));
				break;
			}

			p = q;
		}

		writer.EndArray(); 
		//--level;
		//prt(("s5869 level=%d outside loop \n", level ));
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
		if ( is3D ) {
	    	writer.String( "3" );
		} else {
	    	writer.String( "2" );
		}
	writer.EndObject();

	writer.EndObject();

	//prt(("s0303 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

Jstr makeJsonDefault( const JagStrSplit &sp, const char *str )
{
	return "";
}

int getDimension( const Jstr& colType )
{
	if ( colType == JAG_C_COL_TYPE_POINT 
		|| colType == JAG_C_COL_TYPE_LINE 
		|| colType == JAG_C_COL_TYPE_LINESTRING
		|| colType == JAG_C_COL_TYPE_MULTILINESTRING
		|| colType == JAG_C_COL_TYPE_MULTIPOLYGON
		|| colType == JAG_C_COL_TYPE_MULTIPOINT
		|| colType == JAG_C_COL_TYPE_POLYGON
		|| colType == JAG_C_COL_TYPE_CIRCLE
		|| colType == JAG_C_COL_TYPE_SQUARE
		|| colType == JAG_C_COL_TYPE_RECTANGLE
		|| colType == JAG_C_COL_TYPE_TRIANGLE
		|| colType == JAG_C_COL_TYPE_ELLIPSE
		 ) {
		 return 2;
	 } else if (  colType == JAG_C_COL_TYPE_POINT3D
	 			 ||  colType == JAG_C_COL_TYPE_LINE3D
	 			 ||  colType == JAG_C_COL_TYPE_LINESTRING3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTILINESTRING3D
				 || colType == JAG_C_COL_TYPE_MULTIPOINT3D
	 			 ||  colType == JAG_C_COL_TYPE_POLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_CIRCLE3D
	 			 ||  colType == JAG_C_COL_TYPE_SPHERE
	 			 ||  colType == JAG_C_COL_TYPE_SQUARE3D
	 			 ||  colType == JAG_C_COL_TYPE_CUBE
	 			 ||  colType == JAG_C_COL_TYPE_RECTANGLE3D
	 			 ||  colType == JAG_C_COL_TYPE_BOX
	 			 ||  colType == JAG_C_COL_TYPE_TRIANGLE3D
	 			 ||  colType == JAG_C_COL_TYPE_CYLINDER
	 			 ||  colType == JAG_C_COL_TYPE_CONE
	 			 ||  colType == JAG_C_COL_TYPE_ELLIPSE3D
	 			 ||  colType == JAG_C_COL_TYPE_ELLIPSOID
				 ) {
		 return 3;
	 } else {
	 	return 0;
	 }
}


Jstr getTypeStr( const Jstr& colType )
{
	Jstr t;
	if ( colType == JAG_C_COL_TYPE_POINT ) {
		t = "Point";
	} else if ( colType == JAG_C_COL_TYPE_LINE ) {
		t = "Line";
	} else if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		t = "LineString";
	} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		t = "MultiLineString";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		t = "MultiPolygon";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOINT ) {
		t = "MultiPoint";
	} else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		t = "Polygon";
	} else if ( colType == JAG_C_COL_TYPE_CIRCLE ) {
		t = "Circle";
	} else if ( colType == JAG_C_COL_TYPE_SQUARE ) {
		t = "Square";
	} else if ( colType == JAG_C_COL_TYPE_RECTANGLE ) {
		t = "Rectangle";
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		t = "Triangle";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSE ) {
		t = "Ellipse";
	} else if ( colType == JAG_C_COL_TYPE_POINT3D ) {
		t = "Point3D";
	} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
		t = "Line3D";
	} else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		t = "LineString3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		t = "MultiLineString3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		t = "MultiPoint3D";
	} else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		t = "Polygon3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		t = "MultiPolygon3D";
	} else if ( colType == JAG_C_COL_TYPE_CIRCLE3D ) {
		t = "Circle3D";
	} else if ( colType == JAG_C_COL_TYPE_SPHERE ) {
		t = "Sphere";
	} else if ( colType == JAG_C_COL_TYPE_SQUARE3D ) {
		t = "Square3D";
	} else if ( colType == JAG_C_COL_TYPE_CUBE ) {
		t = "Cube";
	} else if ( colType == JAG_C_COL_TYPE_RECTANGLE3D ) {
		t = "Rectangle3D";
	} else if ( colType == JAG_C_COL_TYPE_BOX  ) {
		t = "Box";
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		t = "Triangle3D";
	} else if ( colType == JAG_C_COL_TYPE_CYLINDER  ) {
		t = "Cylinder";
	} else if ( colType == JAG_C_COL_TYPE_CONE   ) {
		t = "Cone";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSE3D   ) {
		t = "Ellipse3D";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSOID ) {
		t = "Ellipsoid";
	} else if ( colType == JAG_C_COL_TYPE_DBIGINT ) {
		t = "bigint";
	} else if ( colType == JAG_C_COL_TYPE_DINT ) {
		t = "int";
	} else if ( colType == JAG_C_COL_TYPE_DSMALLINT ) {
		t = "smallint";
	} else if ( colType == JAG_C_COL_TYPE_DMEDINT ) {
		t = "mediumint";
	} else if ( colType == JAG_C_COL_TYPE_FLOAT ) {
		t = "float";
	} else if ( colType == JAG_C_COL_TYPE_LONGDOUBLE ) {
		t = "longdouble";
	} else if ( colType == JAG_C_COL_TYPE_DOUBLE ) {
		t = "double";
	} else if ( colType == JAG_C_COL_TYPE_UUID ) {
		t = "uuid";
	} else if ( colType == JAG_C_COL_TYPE_FILE ) {
		t = "file";
	} else if ( colType == JAG_C_COL_TYPE_ENUM ) {
		t = "enum";
	} else {
		t = "Unknown";
	}
	return t;
}

int getPolyDimension( const Jstr& colType )
{
	if ( colType == JAG_C_COL_TYPE_LINESTRING
	     || colType == JAG_C_COL_TYPE_MULTILINESTRING
		 || colType == JAG_C_COL_TYPE_POLYGON
		 || colType == JAG_C_COL_TYPE_MULTIPOLYGON
		 || colType == JAG_C_COL_TYPE_MULTIPOINT
		 ) {
		 return 2;
	 } else if (  colType == JAG_C_COL_TYPE_LINESTRING3D
	     		 || colType == JAG_C_COL_TYPE_MULTILINESTRING3D
	 			 ||  colType == JAG_C_COL_TYPE_POLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOINT3D
				 ) {
		 return 3;
	 } else {
	 	return 0;
	 }
}

// send to socket with header
jagint sendMessage( const JagRequest &req, const char *mesg, const char *type )
{
    jagint len = strlen( mesg );
	return sendMessageLength2( req.session, mesg, len, type );
}

// send to socket with header
jagint sendMessageLength( const JagRequest &req, const char *mesg, jagint msglen, const char *type )
{
    return sendMessageLength2( req.session, mesg, msglen, type );
}

// send to socket with header
jagint sendMessageLength2( JagSession *session, const char *mesg, jagint msglen, const char *type )
{
	char *buf = NULL;
	jagint rc = 0;
    if ( strlen( type ) < 2 ) { 
		prt(("s202928 type.size < 2 return -200\n"));
		return -200; 
	}

	// check if message is last one, e.g. "_END_" for mesg or "X1" for type
	/**
	int lastone = false;
	if ( strncmp( mesg, "_END_", 5 ) == 0 || (type[0] == 'X' && type[1] == '1') || (type[0] == 'S' && type[1] == 'S') ) {
		lastone = true;
	}
	***/

	bool isHB;
	if ( type[0] == 'H' && type[1] == 'B' ) { isHB = true; } else { isHB = false; }

	// compress or no: if no, len is original; if yes, use new length
	char code4[5];
	char sqlhdr[8]; makeSQLHeader( sqlhdr );
	if ( isHB ) {
		sqlhdr[0] = 'H'; sqlhdr[1] = 'B'; sqlhdr[2] = 'B'; // "HBB"
	}

	if ( msglen >= JAG_SOCK_COMPRSS_MIN ) {
		Jstr comp;
		JagFastCompress::compress( mesg, msglen, comp );
		msglen = comp.size();
		buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+comp.size()+1+64 );
		sprintf( code4, "Z%c%cC", type[0], type[1] );
		putXmitHdrAndData( buf, sqlhdr, comp.c_str(), msglen, code4 );
		//prt(("s2039383 compress\n"));
	} else {
 		buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+msglen+1+64 );
		sprintf( code4, "C%c%cC", type[0], type[1] );
		putXmitHdrAndData( buf, sqlhdr, mesg, msglen, code4 );
		//prt(("s2039384 no compress\n"));
	}

	// check if message is heart beat
	/***
    if ( !isHB ) { 
		prt(("s2800 THREADID=%ld sock=%d SENDMEGLEN mesg=[%s], msglen=%lld\n", THREADID, session->sock, mesg, msglen));
		prt(("s2800 THREADID=%ld buf=[%s]\n", THREADID, buf ));
	}
	***/

	if ( !isHB ) {
		rc = sendRawData( session->sock, buf, JAG_SOCK_TOTAL_HDR_LEN+msglen );
	} else if ( session->hasTimer ) {
		rc = sendRawData( session->sock, buf, JAG_SOCK_TOTAL_HDR_LEN+msglen );
	}

	if ( rc < 0 ) session->sessionBroken = 1; // session send error, broken 

	if ( rc < JAG_SOCK_TOTAL_HDR_LEN+msglen ) {
		prt(("s20298 sendMLen rc = %d < %d(HLN=%d) mesg=[%s] msglen=%d -1\n", 
			  rc, JAG_SOCK_TOTAL_HDR_LEN+msglen , JAG_SOCK_TOTAL_HDR_LEN, mesg, msglen ));
		rc = -1;
	}

	if ( buf ) free( buf );
    return rc;
}

Jstr convertToStr( const Jstr  &pm )
{
    Jstr str;
    if ( pm == JAG_ROLE_SELECT  ) {
        str = "select";
    } else if ( pm == JAG_ROLE_INSERT ) {
        str = "insert";
    } else if ( pm == JAG_ROLE_UPDATE ) {
        str = "update";
    } else if ( pm == JAG_ROLE_DELETE ) {
        str = "delete";
    } else if ( pm == JAG_ROLE_CREATE ) {
        str = "create";
    } else if ( pm == JAG_ROLE_DROP ) {
        str = "drop";
    } else if ( pm == JAG_ROLE_ALTER ) {
        str = "alter";
    } else if ( pm == JAG_ROLE_ALL  ) {
        str = "all";
    }

    return str;
}

// pms "A,U,D"
Jstr convertManyToStr( const Jstr &pms )
{
    Jstr str;
    JagStrSplit sp( pms, ',', true );
    for ( int i=0; i < sp.length(); ++i ) {
        str += convertToStr( sp[i] ) + ",";
    }
    return trimTailChar(str, ',' );
}

Jstr convertType2Short( const Jstr &geotypeLong )
{
	const char *p = geotypeLong.c_str();
    if ( 0==strcasecmp(p, "point" ) ) {
		return JAG_C_COL_TYPE_POINT;
	} else if ( 0==strcasecmp(p, "point3d" ) ) {
		return JAG_C_COL_TYPE_POINT3D;
	} else if ( 0==strcasecmp(p, "line" ) ) {
		return JAG_C_COL_TYPE_LINE;
	} else if ( 0==strcasecmp(p, "line3d" ) ) {
		return JAG_C_COL_TYPE_LINE3D;
	} else if ( 0==strcasecmp(p, "circle" ) ) {
		return JAG_C_COL_TYPE_CIRCLE;
	} else if ( 0==strcasecmp(p, "circle3d" ) ) {
		return JAG_C_COL_TYPE_CIRCLE3D;
	} else if ( 0==strcasecmp(p, "sphere" ) ) {
		return JAG_C_COL_TYPE_SPHERE;
	} else if ( 0==strcasecmp(p, "square" ) ) {
		return JAG_C_COL_TYPE_SQUARE;
	} else if ( 0==strcasecmp(p, "square3d" ) ) {
		return JAG_C_COL_TYPE_SQUARE3D;
	} else if ( 0==strcasecmp(p, "cube" ) ) {
		return JAG_C_COL_TYPE_CUBE;
	} else if ( 0==strcasecmp(p, "rectangle" ) ) {
		return JAG_C_COL_TYPE_RECTANGLE;
	} else if ( 0==strcasecmp(p, "rectangle3d" ) ) {
		return JAG_C_COL_TYPE_RECTANGLE3D;
	} else if ( 0==strcasecmp(p, "bbox" ) ) {
		return JAG_C_COL_TYPE_BBOX;
	} else if ( 0==strcasecmp(p, "box" ) ) {
		return JAG_C_COL_TYPE_BOX;
	} else if ( 0==strcasecmp(p, "cone" ) ) {
		return JAG_C_COL_TYPE_CONE;
	} else if ( 0==strcasecmp(p, "triangle" ) ) {
		return JAG_C_COL_TYPE_TRIANGLE;
	} else if ( 0==strcasecmp(p, "triangle3d" ) ) {
		return JAG_C_COL_TYPE_TRIANGLE3D;
	} else if ( 0==strcasecmp(p, "cylinder" ) ) {
		return JAG_C_COL_TYPE_CYLINDER;
	} else if ( 0==strcasecmp(p, "ellipse" ) ) {
		return JAG_C_COL_TYPE_ELLIPSE;
	} else if ( 0==strcasecmp(p, "ellipse3d" ) ) {
		return JAG_C_COL_TYPE_ELLIPSE3D;
	} else if ( 0==strcasecmp(p, "ellipsoid" ) ) {
		return JAG_C_COL_TYPE_ELLIPSOID;
	} else if ( 0==strcasecmp(p, "polygon" ) ) {
		return JAG_C_COL_TYPE_POLYGON;
	} else if ( 0==strcasecmp(p, "polygon3d" ) ) {
		return JAG_C_COL_TYPE_POLYGON3D;
	} else if ( 0==strcasecmp(p, "linestring" ) ) {
		return JAG_C_COL_TYPE_LINESTRING;
	} else if ( 0==strcasecmp(p, "linestring3d" ) ) {
		return JAG_C_COL_TYPE_LINESTRING3D;
	} else if ( 0==strcasecmp(p, "multipoint" ) ) {
		return JAG_C_COL_TYPE_MULTIPOINT;
	} else if ( 0==strcasecmp(p, "multipoint3d" ) ) {
		return JAG_C_COL_TYPE_MULTIPOINT3D;
	} else if ( 0==strcasecmp(p, "multilinestring" ) ) {
		return JAG_C_COL_TYPE_MULTILINESTRING;
	} else if ( 0==strcasecmp(p, "multilinestring3d" ) ) {
		return JAG_C_COL_TYPE_MULTILINESTRING3D;
	} else if ( 0==strcasecmp(p, "multipolygon" ) ) {
		return JAG_C_COL_TYPE_MULTIPOLYGON;
	} else if ( 0==strcasecmp(p, "multipolygon3d" ) ) {
		return JAG_C_COL_TYPE_MULTIPOLYGON3D;
	} else if ( 0==strcasecmp(p, "range" ) ) {
		return JAG_C_COL_TYPE_RANGE;
	} else {
		return "UNKNOWN";
	}
}

// input str:  "aaaaa is not bbb"
// returns aaaaa if isSpace is true
// input str:  "aaaaa b|is not bbb"
// returns "aaaaa b" if isSpace is false, and sep='|'
Jstr firstToken( const char *str, char sep )
{
    if ( NULL == str || *str == NBT ) {
		prt(("u127608 firstToken str NULL or NBT\n" ));
		return "";
	}
	char *p = (char*)str;
	if ( sep != NBT ) {
    	while ( ! isspace(*p) && *p != sep && *p != '\0' ) ++p;
		// str points to first non-space char
	} else {
    	while ( *p != sep && *p != '\0' ) ++p;
		// p points to sep or end NBT
	}
	prt(("s230837 firstToken str=[%s] p=[%s] p-str=%d\n", str, p,  p-str ));
	return Jstr(str, p-str);
}

Jstr jagerr( int errcode )
{
	Jstr err;
	if ( -30144 == errcode ) {
		err = "Key column must not be a roll-up column";
	} else if ( -30145 == errcode )  {
		err = "Spare column must be a char column";
	} else if ( -10511 == errcode )  {
		err = "dropdb force must have a database name";
	} else if ( -12823 == errcode )  {
		err = "timeseries(TIMESERIES|RETAINPERIOD) is required";
	} else if ( -12820 == errcode || -12821 == errcode )  {
		err = "timeseries syntax is incorrect";
	} else if ( -15315 == errcode )  {
		err = "A value column must be given";
	} else if ( -13052 == errcode )  {
		err = "A timeseries table must have a timestamp(nano) or datetime(nano) KEY column";
	} else if ( -13050 == errcode )  {
		err = "A table cannot have duplicated columns";
	} else if ( -19000 == errcode )  {
		err = "Column name too long";
	} else if ( -19001 == errcode )  {
		err = "Column name cannot have . character";
	} else if ( -19013 == errcode )  {
		err = "Syntax error near rollup";
	} else if ( -90030 == errcode )  {
		err = "Column type error";
	} else if ( -18000 == errcode || -18010 == errcode || -18030 == errcode )  {
		err = "enum syntax is incorrect";
	} else if ( -19042 == errcode )  {
		err = "srid is too large ( must be <= 2000000000 )";
	} else if ( -19060 == errcode )  {
		err = "Syntax error near default";
	} else if ( -19150 == errcode )  {
		err = "Default value not enclosed with single-quotes or double-quotes";
	} else if ( -12800 == errcode )  {
		err = "Inserting duplicate columns";
	} else if ( -11010 == errcode )  {
		err = "Empty command";
	} else if ( -13822 == errcode )  {
		err = "Wrong rention unit";
	} else if ( -19004 == errcode )  {
		err = "Column type is missing";
	} else if ( -19003 == errcode )  {
		err = "Column name is invalid";
	} else if ( -13821 == errcode )  {
		err = "TimeSeries clause is invalid";
	}

	return err;
}

bool hasDefaultValue( char spare4 )
{
    if ( spare4 == JAG_CREATE_DEFINSERTVALUE ) return true;
    return hasDefaultDateTimeValue( spare4 );
}

bool hasDefaultDateTimeValue( char spare4 )
{
    if ( spare4 == JAG_CREATE_DEFDATE || spare4 == JAG_CREATE_DEFUPDATE_DATE ) return true;
    if ( spare4 == JAG_CREATE_DEFDATETIMESEC || spare4 == JAG_CREATE_DEFUPDATE_DATETIMESEC ) return true;
    if ( spare4 == JAG_CREATE_DEFDATETIME || spare4 == JAG_CREATE_DEFUPDATE_DATETIME ) return true;
    if ( spare4 == JAG_CREATE_DEFDATETIMENANO || spare4 == JAG_CREATE_DEFUPDATE_DATETIMENANO ) return true;
    if ( spare4 == JAG_CREATE_DEFDATETIMEMILL || spare4 == JAG_CREATE_DEFUPDATE_DATETIMEMILL ) return true;
    return false;
}

bool hasDefaultUpdateDateTime( char spare4 )
{
    if ( spare4 == JAG_CREATE_UPDATE_DATE || spare4 == JAG_CREATE_DEFUPDATE_DATE ) return true;
    if ( spare4 == JAG_CREATE_UPDATE_DATETIMESEC || spare4 == JAG_CREATE_DEFUPDATE_DATETIMESEC ) return true;
    if ( spare4 == JAG_CREATE_UPDATE_DATETIME || spare4 == JAG_CREATE_DEFUPDATE_DATETIME ) return true;
    if ( spare4 == JAG_CREATE_UPDATE_DATETIMENANO || spare4 == JAG_CREATE_DEFUPDATE_DATETIMENANO ) return true;
    if ( spare4 == JAG_CREATE_UPDATE_DATETIMEMILL || spare4 == JAG_CREATE_DEFUPDATE_DATETIMEMILL ) return true;
    return false;
}

Jstr charToStr(char c)
{
	char p[2];
	p[0] = c;
	p[1] = '\0';
	return p;
}

// "ldkfkfdfd)  ; "  true
// "ldkfkfdfd)  ;"  true
// "ldkfkfdfd); "  true
// "ldkfkfdfd) ; "  true
// "ldkfkfdfd)  "  true
// "ldkfkfdfd)"  true
// "ldkfkfdfd)\t ;"  true
bool endWithSQLRightBra( const char *sql )
{
	if ( !sql ) return false;
	if ( *sql == '\0' ) return false;
	int len  = strlen(sql);
	const char *r = sql + len-1;
	bool seebra = false;
	while ( r != sql ) {
		if ( isspace(*r ) || *r == ';' ) {
			--r;
		} else {
			if ( *r == ')' ) {
				seebra = true;
			}
			break;
		}
	}

	if ( seebra ) {
		return true;
	} else {
		return false;
	}

}


