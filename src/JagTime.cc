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

#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <JagTime.h>
#include <JagUtil.h>

int JagTime::getTimeZoneDiff()
{
    char buf[8];
    memset( buf, 0, 8 );
    time_t now = time(NULL);
    struct tm result;
	jag_localtime_r ( &now, &result );

    strftime( buf, 8,  "%z", &result );  // -0500
	int hrmin =  atoi( buf );
	int hour = hrmin/100;
	int min = hrmin % 100; 
	if ( hour < 0 ) {
		min = 60 * hour - min;
	} else {
		min = 60 * hour + min;
	}

	if ( result.tm_isdst > 0 ) {
		min -= 60;
	}

	return min;
}

jaguint JagTime::mtime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	return (1000*(jaguint)now.tv_sec + now.tv_usec/1000);
}

jaguint JagTime::utime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	return (1000000*(jaguint)now.tv_sec + now.tv_usec);
}

Jstr JagTime::makeRandDateTimeString( int N )
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm  result;
  	char buffer [80];
	jagint yearSecs = 365 * 86400;
	jagint rnd = rand() % (N*yearSecs);

  	time (&rawtime);
	rawtime -=  rnd;
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer,80,"%Y-%m-%d %H:%M:%S", timeinfo);
	return buffer;
}

Jstr JagTime::makeRandTimeString()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm  result;
  	char buffer [80];

  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer,80,"%H:%M:%S", timeinfo);
	return buffer;
}

Jstr JagTime::YYYYMMDDHHMM()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm  result;
	char  buffer[64];

  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime( buffer, 64, "%Y-%m-%d-%H-%M", timeinfo);
	return buffer;
}

Jstr JagTime::YYYYMMDD()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
	char  buffer[64];
  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime( buffer, 64, "%Y%m%d", timeinfo);
	return buffer;
}

Jstr JagTime::makeNowTimeStringSeconds()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
	char  buffer[80];
  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	return buffer;
}

Jstr JagTime::makeNowTimeStringMilliSeconds()
{
	char  buffer[80];
	char  endbuf[8];
    struct timeval now;
    gettimeofday( &now, NULL );
    int usec = now.tv_usec;
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	sprintf( endbuf, ".%d", usec/1000 );
	strcat( buffer, endbuf );
	return buffer;
}

Jstr JagTime::makeNowTimeStringMicroSeconds()
{
	char  buffer[80];
	char  endbuf[8];
    struct timeval now;
    gettimeofday( &now, NULL );
    int usec = now.tv_usec;
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
  	time (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	sprintf( endbuf, ".%d", usec );
	strcat( buffer, endbuf );
	return buffer;
}

void JagTime::setTimeInfo( const JagParseAttribute &jpa , const char *str, struct tm &timeinfo, int isTime ) 
{
	const char *ttime1, *tdate2;
	jagint timeval = -1;
	
	ttime1 = strchr(str, ':'); 
	tdate2 = strchr(str, '-');

	if ( ttime1 && tdate2 ) {
		// "2016-02-13 12:32:22"
		timeval = getDateTimeFromStr( jpa, str, isTime );
	} else if ( ttime1 ) {
		// "12:23:00"
		timeval = getTimeFromStr( str, isTime );
	} else if ( tdate2 ) {
		// "yyyy-mm-dd" only
		timeinfo.tm_year = rayatoi(str, 4) - 1900;
		timeinfo.tm_mon = rayatoi(str+5, 2) - 1;
		timeinfo.tm_mday = rayatoi(str+8, 2);
		timeinfo.tm_hour = 0;
		timeinfo.tm_min = 0;
		timeinfo.tm_sec = 0;
	} else {
		if ( isTime ) {
			// "19293944"  microsec, nanosec, or secs, or milliseconds
			timeval = jagatoll( str );
		} else {
			// "20130212"
			timeinfo.tm_year = rayatoi(str, 4) - 1900;
			timeinfo.tm_mon = rayatoi(str+4, 2) - 1;
			timeinfo.tm_mday = rayatoi(str+6, 2);
			timeinfo.tm_hour = 0;
			timeinfo.tm_min = 0;
			timeinfo.tm_sec = 0;			
		}
	}

	if ( timeval >= 0 ) {
		time_t secs = timeval;
		if ( 1 == isTime ) secs = timeval / 1000000;
		else if ( 3 == isTime ) secs = timeval;
		else if ( 2 == isTime ) secs = timeval / 1000000000;
		else if ( 4 == isTime ) secs = timeval / 1000;

		// gmtime_r( &secs, &timeinfo );
		jagint  defTZDiffMin = jpa.timediff;
		jagint servTZDiffMin = jpa.servtimediff;
		jagint minSrvCli = servTZDiffMin - defTZDiffMin;
		// prt(("s2394 secs=%lld\n", secs ));

		secs -=  minSrvCli * 60;
		jag_localtime_r( &secs, &timeinfo );

	}
}

JagFixString JagTime::getValueFromTimeOrDate( const JagParseAttribute &jpa, const JagFixString &str, 
									const JagFixString &str2, int op, const Jstr& ddiff ) 
{
	prt(("s28336 getValueFromTimeOrDate str=[%s] str2=[%s] op=%d ddiff=%d\n", str.c_str(), str2.c_str(), op, ddiff ));
	prt(("s28336 getValueFromTimeOrDate str.len=[%d]\n", str.length() ));
	char *p;
	char savebyte;
	int timelen, isTime;
	struct tm timeinfo;
	timelen = JAG_DATETIMENANO_FIELD_LEN-6;
	char buf[timelen+1];
	memset(buf, 0, timelen+1);
	
	isTime = 0;
	if ( str.length() == JAG_DATETIME_FIELD_LEN || str.length() == JAG_TIME_FIELD_LEN 
		 || str.length() == JAG_TIMESTAMP_FIELD_LEN ) isTime = 1; // microseconds
	else if ( str.length() == JAG_DATETIMENANO_FIELD_LEN 
		 || str.length() == JAG_TIMENANO_FIELD_LEN ) isTime = 2; // nanosecs
	else if ( str.length() == JAG_DATETIMESEC_FIELD_LEN ) isTime = 3; // seconds
	else if ( str.length() == JAG_DATETIMEMILL_FIELD_LEN ) isTime = 4; // milliseconds

	p = (char*)str.c_str();
	savebyte = *(p+str.length());
	*(p+str.length()) = '\0';

	setTimeInfo( jpa, p, timeinfo, isTime );
	*(p+str.length()) = savebyte;	

	if ( op == JAG_FUNC_SECOND ) {
		sprintf(buf, "%d", timeinfo.tm_sec);
	} else if ( op == JAG_FUNC_MINUTE ) {
		sprintf(buf, "%d", timeinfo.tm_min);
	} else if ( op == JAG_FUNC_HOUR ) {
		sprintf(buf, "%d", timeinfo.tm_hour);
	} else if ( op == JAG_FUNC_DAY ) {
		sprintf(buf, "%d", timeinfo.tm_mday);
	} else if ( op == JAG_FUNC_MONTH ) {
		sprintf(buf, "%d", timeinfo.tm_mon + 1);
	} else if ( op == JAG_FUNC_YEAR ) {
		sprintf(buf, "%d", timeinfo.tm_year + 1900);
	} else if ( op == JAG_FUNC_DATE ) {
		sprintf(buf, "%d-%02d-%02d", timeinfo.tm_year + 1900, timeinfo.tm_mon+1, timeinfo.tm_mday );
	} else if ( op == JAG_FUNC_DAYOFMONTH ) {
		if ( timeinfo.tm_mday <= 1 || timeinfo.tm_mday > 31 ) {
			buf[0] = '0';
		} else {
			sprintf(buf, "%d", timeinfo.tm_mday);
		}
	} else if ( op == JAG_FUNC_DAYOFWEEK ) {
		sprintf(buf, "%d", timeinfo.tm_wday);
	} else if ( op == JAG_FUNC_DAYOFYEAR ) {
		sprintf(buf, "%d", timeinfo.tm_yday + 1);
	} else if ( op == JAG_FUNC_DATEDIFF ) {
		struct tm timeinfo2;
		isTime = 0;
		if ( str2.length() == JAG_DATETIME_FIELD_LEN || str2.length() == JAG_TIME_FIELD_LEN 
			 || str2.length() == JAG_TIMESTAMP_FIELD_LEN ) isTime = 1;
		else if ( str2.length() == JAG_DATETIMENANO_FIELD_LEN || str2.length() == JAG_TIMENANO_FIELD_LEN ) isTime = 2;
		else if ( str2.length() == JAG_DATETIMESEC_FIELD_LEN ) isTime = 3;
		else if ( str2.length() == JAG_DATETIMEMILL_FIELD_LEN ) isTime = 4;

		p = (char*)str2.c_str();
		savebyte = *(p+str2.length());
		*(p+str2.length()) = '\0';
		setTimeInfo( jpa, p, timeinfo2, isTime );		
		*(p+str2.length()) = savebyte;

		time_t endt = mktime( &timeinfo2 );
		time_t begint = mktime( &timeinfo );
		jagint diff = 0;
		if ( endt > -1 && begint > -1 ) {
			diff = endt - begint;
    		if ( ddiff == "m" ) {
    			diff = diff / 60;
    		} else if ( ddiff == "h" ) {
    			diff = diff / 3600;
    		} else if ( ddiff == "D" ) {
    			diff = diff / 86400;
    		} else if ( ddiff == "M" ) {
    			diff = (timeinfo2.tm_year-timeinfo.tm_year)*12 + (timeinfo2.tm_mon-timeinfo.tm_mon);
    		} else if ( ddiff == "Y" ) {
    			diff = timeinfo2.tm_year - timeinfo.tm_year;
    		}
		} 

		snprintf(buf, timelen+1, "%lld", diff);
	}

	prt(("s400124 buf=[%s] timelen=%d\n", buf, timelen ));
	
	JagFixString res(buf, timelen, timelen);
	return res;	
}

void JagTime::convertDateTimeToLocalStr( const Jstr& instr, Jstr& outstr, int isnano )
{
	char tmstr[48];
	char utime[12];
	jaguint timestamp;
	jaguint  sec;
	jaguint  usec;
	struct tm *tmptr;
	struct tm result;

	timestamp = jagatoll( instr.c_str() );
	if ( 0 == isnano || 1 == isnano ) {
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	} else if ( 2 == isnano ) {
		sec = timestamp/1000000000; 
		usec = timestamp%1000000000;
	} else if ( 3 == isnano ) {
		sec = timestamp;
		usec = 0;
	} else if ( 4 == isnano ) {
		sec = timestamp/1000; 
		usec = timestamp%1000;
	}

	time_t tsec = sec;
	tmptr = jag_localtime_r( &tsec, &result );  // client calls this
	strftime( tmstr, sizeof(tmstr), "%Y-%m-%d %H:%M:%S", tmptr ); 

	if ( 0 == isnano || 1 == isnano ) {
		sprintf( utime, ".%06d", usec );
		strcat( tmstr, utime );
		outstr = tmstr;
	} else if ( 2 == isnano ) {
		sprintf( utime, ".%09d", usec );
		strcat( tmstr, utime );
		outstr = tmstr;
	} else if ( 4 == isnano ) {
		sprintf( utime, ".%03d", usec );
		strcat( tmstr, utime );
		outstr = tmstr;
	} else if ( 3 == isnano ) {
		outstr = tmstr;
	}

}

void JagTime::convertTimeToStr( const Jstr& instr, Jstr& outstr, int tmtype )
{
	char tmstr[24];
	char utime[8];
	jagint timestamp;
	time_t  sec;
	jagint  usec;
	struct tm *tmptr;

	timestamp = jagatoll( instr.c_str() );
	if ( 1 == tmtype ) {
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	} else if ( 2 == tmtype ) {
		sec = timestamp/1000000000; 
		usec = timestamp%1000000000;
	} else if ( 3 == tmtype ) {
		sec = timestamp;
		usec = 0;
	} else if ( 4 == tmtype ) {
		sec = timestamp/1000; 
		usec = timestamp%1000;
	} else { 
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	}

	struct tm result;
	tmptr = gmtime_r( &sec, &result );
	strftime( tmstr, sizeof(tmstr), "%H:%M:%S", tmptr ); 

	if ( 1 == tmtype ) {
	    sprintf( utime, ".%06d", usec );
		strcat( tmstr, utime );
	} else if ( 2 == tmtype ) {
		sprintf( utime, ".%09d", usec );
		strcat( tmstr, utime );
	} else if ( 4 == tmtype ) {
		sprintf( utime, ".%03d", usec );
		strcat( tmstr, utime );
	} else if ( 3 == tmtype ) {
	} 

	outstr = tmstr;
}

void JagTime::convertDateToStr( const Jstr& instr, Jstr& outstr )
{
	char  buf[11];
	memset( buf, 0, 11 );
	buf[0] = instr[0];
	buf[1] = instr[1];
	buf[2] = instr[2];
	buf[3] = instr[3];
	buf[4] = '-';
	buf[5] = instr[4];
	buf[6] = instr[5];
	buf[7] = '-';
	buf[8] = instr[6];
	buf[9] = instr[7];
	outstr = buf;
}

int JagTime::convertDateTimeFormat( const JagParseAttribute &jpa, char *outbuf, const char *inbuf, 
								    int offset, int length, int isnano )
{
	prt(("s31661 convertDateTimeFormat inbuf=[%s] isnano=%d\n", inbuf, isnano ));
    jagint lonnum = 0;

	if ( ( NULL == strchr(inbuf, '-') && NULL == strchr(inbuf, ':') ) ) {
		// inbuf:  "16263838380"
		if ( 2 == isnano ) {
			lonnum = jagatoll( inbuf ); 
			prt(("s400123 2 == isnano lonnum=%lld\n", lonnum ));
		} else if ( 3 == isnano ) {
			lonnum = jagatoll( inbuf );
			if ( lonnum > jagint(1000000000) * JAG_BILLION ) {
			   lonnum = lonnum/JAG_BILLION;
			} else if ( lonnum > jagint(1000000000) * JAG_MILLION ) {
			   lonnum = lonnum/JAG_MILLION;
			}

		} else if ( 4 == isnano ) {
			// to milliseconds
			lonnum = jagatoll( inbuf );
			if ( lonnum > jagint(1000000000) * JAG_BILLION ) {
			   lonnum = lonnum/JAG_MILLION;
			} else if ( lonnum > jagint(1000000000) * JAG_MILLION ) {
			   lonnum = lonnum/1000;
			}

			prt(("s401124 4 == isnano lonnum=%lld\n", lonnum ));
		} else {
			// to microsecs
			lonnum = jagatoll( inbuf );
			if ( lonnum > jagint(1000000000) * JAG_BILLION ) {
			   lonnum = lonnum/1000;
			}
			prt(("s401125 microsecs 1 == isnano lonnum=%lld\n", lonnum ));
		}
		// prt(("s2883 pure num in datestr inbuf lonnum=%lld\n", lonnum  ));
	} else {
		// inbuf:  "2020-21-21 12:13:21.000000" or datetimesec or datatimenano format
		lonnum = getDateTimeFromStr( jpa, inbuf, isnano);
		if ( lonnum == -1 ) {
			return 1; // error
		}
	}

    if ( snprintf(outbuf+offset, length+1, "%0*lld", length, lonnum) > length ) {
		prt(("s1884 convertDateTimeFormat return 2\n" ));
		return 2; // error
	}

	return 0; // OK
}

int JagTime::convertTimeFormat( char *outbuf, const char *inbuf, const int offset, const int length, int isnano )
{
    jagint lonnum = 0;
	if ( strchr((char*)inbuf, ':') ) {
		lonnum = getTimeFromStr(inbuf, isnano);
		if ( -1 == lonnum ) return 1;
	} else {
		lonnum = jagatoll( inbuf );
	}

    if ( snprintf(outbuf+offset, length+1, "%0*lld", length, lonnum) > length ) {
		return 2;
	}

	return 0;
}


int JagTime::convertDateFormat( char *outbuf, const char *inbuf, int offset, int length )
{
	char buf[JAG_DATE_FIELD_LEN+1];
	const char *ubuf;
	memset(buf, 0, JAG_DATE_FIELD_LEN+1);
		
	if ( NULL != strchr((char*)inbuf, '-') && !getDateFromStr(inbuf, buf) ) {
		return 1;
	}

	if ( *buf != '\0' ) ubuf = buf;
	else ubuf = inbuf;

	if ( snprintf(outbuf+offset, length+1, "%s", ubuf) > length ) return 2;

	return 0;
}

jaguint JagTime::getDateTimeFromStr( const JagParseAttribute &jpa, const char *str, int isnano )
{
	jagint  defTZDiffMin = jpa.timediff;
	jagint  servTZDiffMin = jpa.servtimediff;
	jagint  minSrvCli = servTZDiffMin - defTZDiffMin;

	int c;
	char buf10[10];
    memset(buf10, 0, 10);
	struct tm  ttime;
	char *s = (char*) str;
	char *p;
	char v;
	jaguint res;

	ttime.tm_isdst = -1;

	// get yyyy
	p = s;
	while ( *p != '-' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return 0;

	*p = '\0';
	ttime.tm_year = atoi(s) - 1900;
	if ( ttime.tm_year >= 10000 ) return 0;
	*p = '-';
	s = p+1;
	if ( *s == '\0' ) return 0;

	// get mm
	p = s;
	while ( *p != '-' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return 0;
	*p = '\0';
	ttime.tm_mon = atoi(s) - 1;
	// if ( ttime.tm_mon < 0 || ttime.tm_mon > 11 ) return -1;
	*p = '-';

	// get dd
	s = p+1;
	if ( *s == '\0' ) return 0;

	p = s;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return 0;
	*p = '\0';
	ttime.tm_mday = atoi(s);
	// if ( ttime.tm_mday < 1 || ttime.tm_mday > 31 ) return -1;
	*p = ' ';

	while ( *p == ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return 0;

	// hh:mm:ss[.ffffff]
	// get hh
	s = p;
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return 0;
	*p = '\0';
	ttime.tm_hour = atoi(s);
	// if ( ttime.tm_hour > 23 ) return -1;
	*p = ':';

	// get mm
	++p;
	s = p;
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return 0;
	*p = '\0';
	ttime.tm_min = atoi(s);
	// if ( ttime.tm_min > 59 ) return -1;
	*p = ':';

	// get ss
	++p;
	s = p;
	while ( *p != '.' && *p != ' ' && *p != '\0' ) ++p;
	v = *p;
	*p = '\0';
	ttime.tm_sec = atoi(s);
	// if ( ttime.tm_sec > 59 ) return -1;
	*p = v;


   	// no .ffffff section
   	// res = 1000000* mktime( &ttime );
   	res = mktime( &ttime );
   	// adjust server
   	res += minSrvCli * 60;

	if ( 2 == isnano ) {
		res *= (jaguint)1000000000;
	} else if ( 1 == isnano ) {
		res *= (jaguint)1000000;
	} else if ( 3 == isnano ) {
		// seconds
	} else if ( 4 == isnano ) {
		res *= (jaguint)1000;
	} else {
		res *= (jaguint)1000000;
	}

   	jagint zsecdiff;
	jagint tzhour;

	while ( 1 ) {
    	if ( *p == '\0' ) { break; }
    
    	// saw .fff or .ffffff or .fffffffff
    	if ( *p == '.' ) {
    		if ( 2 == isnano ) {
        		memset(buf10, '0', 9);
			} else if ( 1 == isnano ) {
    			memset(buf10, '0', 6);
			} else if ( 3 == isnano ) {
    			memset(buf10, 0, 10);  // seconds
			} else if ( 4 == isnano ) {
    			memset(buf10, '0', 3);
    		} else {
    			memset(buf10, 0, 10);
    		}

        	++p;
        	s = p;
        	c = 0;
    		while ( *p != '\0' && *p != '+' && *p != '-' && *p != ' ' ) {
    			if ( (1 == isnano) && c < 6 ) {
    				buf10[c] = *p;
					++c;
    			} else if ( ( 2 == isnano ) && c < 9 ) {
    				buf10[c] = *p;
					++c;
    			} else if ( ( 4 == isnano ) && c < 3 ) {
    				buf10[c] = *p;
					++c;
    			} else {
    				while ( *p != '\0' && *p != ' ' ) ++p;
    				break;
    			}

    			++p;
    		}
    
        	res += jagatoll(buf10);
    	}
    
    	if ( *p == '\0' ) { break; }
    
    	while ( *p == ' ' ) ++p;  // skip blanks
    	if ( *p == '\0' ) {
			break;
    	}
    
		// -08:00 or +09:30 part
    	if ( *p == '-' ) {
    		c = -1; ++p;
    	} else if ( *p == '+' )  {
    		c = 1; ++p;
    	} else {
    		c = 1;
    	}
    
    	s = p;
    	// printf("c100 s=[%s]\n", s );
    	if ( *p == '\0' ) {
    		// no time zone info is given
			break;
    	}
    
    	while ( *p != ':' && *p != '\0' ) { ++p; } // goto : in 08:30
    
    	v = *p;
    	*p = '\0';
    	// -06:30
    	// printf("c110 s=[%s] defTZDiffMin=%d atoll(s)*60=%d\n", s, defTZDiffMin, atoll(s)*60 );
    	zsecdiff;
		tzhour = jagatoll(s);
		// prt(("s6612 before hour adjust tzhour=%d res=%lld\n", tzhour, res ));
    	if ( c > 0 ) {
    		zsecdiff =  tzhour *3600 - defTZDiffMin*60;  // adjust to original clien time zone
			if ( 1 == isnano ) {
				// micro
				res -=  zsecdiff * (jaguint)1000000;
			} else if ( 2 == isnano ) {
				// nano
				res -=  zsecdiff * (jaguint)1000000000;
			} else if ( 4 == isnano ) {
				// milli
				res -=  zsecdiff * (jaguint)1000;
			} else if ( 3 == isnano ) {
				res -=  zsecdiff;
			} else {
			}

    	} else {
    		zsecdiff =  defTZDiffMin*60 + tzhour*3600;
			if ( 1 == isnano ) {
				// micro
				res +=  zsecdiff * (jaguint)1000000;
			} else if ( 2 == isnano ) {
				// nano
				res +=  zsecdiff * (jaguint)1000000000;
			} else if ( 4 == isnano ) {
				// milli
				res +=  zsecdiff * (jaguint)1000;
			} else if ( 3 == isnano ) {
				res +=  zsecdiff;
			} else {
			}
    	}

    	*p = v;
    	if ( *p == '\0' ) { break; }

    	++p; // past :   :30 minute part
    	if ( *p == '\0' ) { break; }

    	s = p;
		// prt(("s8309 s=[%s] res=%lld\n", s, res ));
    	if ( c > 0 ) {
			if ( 1 == isnano ) {
				// micro
				res = res - jagatoll(s) * 60 * (jaguint)1000000;  // res is GMT time
			} else if ( 2 == isnano ) {
				// nano
				res = res - jagatoll(s) * 60 * (jaguint)1000000000;  // res is GMT time
			} else if ( 4 == isnano ) {
				// milli
				res = res - jagatoll(s) * 60 * (jaguint)1000;  // res is GMT time
			} else if ( 3 == isnano ) {
				// secs
				res = res - jagatoll(s) * 60;
			} else {
			}
    	} else {
			if ( 1 == isnano ) {
				// micro
				res = res + jagatoll(s) * 60 * (jaguint)1000000;  // res is GMT time
			} else if ( 2 == isnano ) {
				// nano
				res = res + jagatoll(s) * 60 * (jaguint)1000000000;  // res is GMT time
			} else if ( 4 == isnano ) {
				// milli
				res = res + jagatoll(s) * 60 * (jaguint)1000;  // res is GMT time
			} else if ( 3 == isnano ) {
				// secs
				res = res + jagatoll(s) * 60;
			} else {
			}
    	}

		break;  // must be here to exit the loop
	}

	// prt(("s4309 res=%lld\n", res ));
	return res;
}

jagint JagTime::getTimeFromStr( const char *str, int isnano )
{
	int c;
	char buf10[10];
    memset(buf10, 0, 10);
	struct tm  ttime;
	char *s = (char*) str;
	char *p;
	char v;
	jagint res;

	ttime.tm_isdst = -1;
	ttime.tm_year = 0;
	ttime.tm_mon = 0;
	ttime.tm_mday = 1;

	p = s;
	// hh:mm:ss[.ffffff]
	// get hh
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return -1;
	*p = '\0';
	ttime.tm_hour = atoi(s);
	if ( ttime.tm_hour > 23 ) return -1;
	*p = ':';

	// get mm
	++p;
	s = p;
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return -1;
	*p = '\0';
	ttime.tm_min = atoi(s);
	if ( ttime.tm_min > 59 ) return -1;
	*p = ':';

	// get ss
	++p;
	s = p;
	while ( *p != '.' && *p != ' ' && *p != '\0' ) ++p;
	v = *p;
	*p = '\0';
	ttime.tm_sec = atoi(s);
	if ( ttime.tm_sec > 59 ) return -1;
	*p = v;

	// no .ffffff section
	// res = 1000000* mktime( &ttime );
	// res = 1000000* timegm( &ttime );
	res = 3600*ttime.tm_hour + 60*ttime.tm_min + ttime.tm_sec;

	if ( 1 == isnano ) {
		res *= (jagint)1000000;
	} else if ( 2 == isnano ) {
		res *= (jagint)1000000000;
	} else if ( 4 == isnano ) {
		res *= (jagint)1000;
	} else if ( 3 == isnano ) {
	} else {
	}

	// prt(("s8907 time str=[%s] ===> res=[%lld]\n", str, res));
	if ( *p == '\0' ) {
		// printf("c111\n");
		return res;
	}

	// saw .fffffff or .fffffffff
	if ( *p == '.' ) {
    	if ( 2 == isnano ) {
       		memset(buf10, '0', 9);
		} else if ( 1 == isnano ) {
    		memset(buf10, '0', 6);
		} else if ( 4 == isnano ) {
    		memset(buf10, '0', 3);
		} else if ( 3 == isnano ) {
    		memset(buf10, 0, 10);
    	} else {
    		memset(buf10, 0, 10);
    	}

    	++p;
    	s = p;
    	c = 0;
    	while ( *p != '\0' && *p != ' ' ) {
    		if ( (1 == isnano) && c < 6 ) {
    			buf10[c] = *p;
				++c;
    		} else if ( ( 2 == isnano ) && c < 9 ) {
    			buf10[c] = *p;
				++c;
    		} else if ( ( 4 == isnano ) && c < 3 ) {
    			buf10[c] = *p;
				++c;
    		} else {
    			while ( *p != '\0' && *p != ' ' ) ++p;
    			break;
    		}
    
    		++p;
    	}
    
    	res += jagatoll(buf10);
		// printf("c112 buf10=%d  s=[%s] res=%lld\n", atoll(buf10), buf10, res );
	}

	return res;
}


bool JagTime::getDateFromStr( const char *instr, char *outstr )
{
	int yyyy, mm, dd;
	char *p = (char*) instr;
	bool res = false;

	// get yyyy
	yyyy =  atoi(p);
	if ( yyyy >= 10000 ) return res;

	// get mm
	while ( *p != '-' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return res;
	++p;
	if ( *p == '\0' ) return res;
	mm =  atoi(p);
	if ( mm < 0 || mm > 12 ) return res;

	// get dd
	while ( *p != '-' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return res;
	++p;
	if ( *p == '\0' ) return res;
	dd =  atoi(p);
	if ( dd < 1 || dd > 31 ) return res;

	// get result
	sprintf( outstr, "%04d%02d%02d", yyyy, mm, dd );

	// prt(("s7383 getdatestr outstr=[%s]\n", outstr ));

	res = true;
	return res;
}

// cycle: 1,2,3,4,6,8,12
jagint JagTime::getStartTimeSecOfSecond( time_t tsec, int cycle )
{
	struct tm result;
	gmtime_r( &tsec, &result );
	int startsec = (result.tm_sec/cycle) * cycle;

	result.tm_sec = startsec;
	time_t  t = mktime( &result );
	return t;
}

// cycle: 1,2,3,4,6,8,12
jagint JagTime::getStartTimeSecOfMinute( time_t tsec, int cycle )
{
	struct tm result;
	
	gmtime_r( &tsec, &result );
	int startmin = (result.tm_min/cycle) * cycle; 
	prt(("s22220 tm_isdst=%d\n", result.tm_isdst ));

	result.tm_sec = 0;
	result.tm_min = startmin;

	time_t  t = mktime( &result );
	return t;
}

jagint JagTime::getStartTimeSecOfHour( time_t tsec, int cycle )
{
	struct tm result;
	
	gmtime_r( &tsec, &result );
	int starthour = (result.tm_hour/cycle) * cycle;  // 0 --30
	prt(("s22220 tm_isdst=%d\n", result.tm_isdst ));

	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = starthour;

	time_t  t = mktime( &result );
	return t;
}

jagint JagTime::getStartTimeSecOfDay( time_t tsec, int cycle )
{
	struct tm result;
	gmtime_r( &tsec, &result );

	int startday = ((result.tm_mday-1)/cycle) * cycle;  // 0 --30

	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;
	result.tm_mday = startday + 1;  // 1--31

	time_t  t = mktime( &result );
	return t;
}

// cycle: 1 only
jagint JagTime::getStartTimeSecOfWeek( time_t tsec )
{
	struct tm result;
	
	gmtime_r( &tsec, &result );
	tsec = tsec - jagint(result.tm_wday) * 86400;  // back to sunday

	gmtime_r( &tsec, &result );
	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;

	time_t  t = mktime( &result );
	return t;
}

// cycle: 1,2,3,4,6
jagint JagTime::getStartTimeSecOfMonth( time_t tsec, int cycle )
{
	struct tm result;
	
	gmtime_r( &tsec, &result );
	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;
	result.tm_mday = 1;
	// result.tm_mon  0 --11
	result.tm_mon = (result.tm_mon/cycle) * cycle; 

	time_t  t = mktime( &result ); // ignores tm_wday and tm_yday
	return t;
}


// cycle: 1, 2
jagint JagTime::getStartTimeSecOfQuarter( time_t tsec, int cycle )
{
	struct tm result;
	gmtime_r( &tsec, &result );
	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;
	result.tm_mday = 1;
	// result.tm_mon  0-11
	result.tm_mon = (result.tm_mon/(3*cycle)) * 3*cycle;
	time_t  t = mktime( &result );
	return t;
}

// cycle: any
jagint JagTime::getStartTimeSecOfYear( time_t tsec, int cycle )
{
	struct tm result;
	
	gmtime_r( &tsec, &result );
	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;
	result.tm_mday = 1;
	result.tm_mon = 0;
	result.tm_year = (result.tm_year/cycle) * cycle;
	time_t  t = mktime( &result );
	return t;
}

// cycle: any
jagint JagTime::getStartTimeSecOfDecade( time_t tsec, int cycle )
{
	struct tm result;
	gmtime_r( &tsec, &result );
	result.tm_sec = 0;
	result.tm_min = 0;
	result.tm_hour = 0;
	result.tm_mday = 1;
	result.tm_mon = 0;
	result.tm_year = (result.tm_year/(cycle*10)) * cycle*10;
	time_t  t = mktime( &result );
	return t;
}

void JagTime::print( struct tm &t )
{
	printf("JagTime::print:\n");
	printf(" tm.tm_year=%d\n", t.tm_year );
	printf(" tm.tm_mon=%d\n", t.tm_mon );
	printf(" tm.tm_mday=%d\n", t.tm_mday );
	printf(" tm.tm_hour=%d\n", t.tm_hour );
	printf(" tm.tm_min=%d\n", t.tm_min );
	printf(" tm.tm_sec=%d\n", t.tm_sec );
	printf(" tm.tm_wday=%d\n", t.tm_wday );
	printf(" tm.tm_yday=%d\n", t.tm_yday );
	printf(" tm.tm_isdst=%d\n", t.tm_isdst );
}

// return buf
// return 1: for success;   0 for error
int JagTime::fillTimeBuffer ( time_t tsec, const Jstr &colType, char *buf )
{
	if ( colType == JAG_C_COL_TYPE_DATETIMENANO || colType == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
		sprintf( buf, "%lld", tsec * 1000000000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIME || colType == JAG_C_COL_TYPE_TIMESTAMP ) {
		sprintf( buf, "%lld", tsec * 1000000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIMEMILL || colType == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
		sprintf( buf, "%lld", tsec * 1000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIMESEC || colType == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
		sprintf( buf, "%lld", tsec );
	} else {
		return 0;
	}

	return 1;
}

// returns seconds, microseconds, or nanoseconds
// return 0 for error
time_t JagTime::getTypeTime( time_t tsec, const Jstr &colType )
{
	if ( colType == JAG_C_COL_TYPE_DATETIMENANO || colType == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
		return (tsec * 1000000000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIME || colType == JAG_C_COL_TYPE_TIMESTAMP ) {
		return (tsec * 1000000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIMEMILL || colType == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
		return (tsec * 1000 );
	} else if ( colType == JAG_C_COL_TYPE_DATETIMESEC || colType == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
		return tsec;
	} else {
		return 0;
	}
}


Jstr JagTime::getLocalTime( time_t  tsec )
{
	struct tm result;
	char tmstr[48];
	jag_localtime_r( &tsec, &result );  // client calls this
	strftime( tmstr, sizeof(tmstr), "%Y-%m-%d %H:%M:%S", &result ); 
	return tmstr;
}

jagint JagTime::nowMilliSeconds()
{
    struct timeval now;
    gettimeofday( &now, NULL );
    return (jagint)now.tv_usec / 1000 + now.tv_sec*1000;
}

