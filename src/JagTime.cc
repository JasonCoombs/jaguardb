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
    // ptm = localtime( &now );
	jag_localtime_r ( &now, &result );

    strftime( buf, 8,  "%z", &result );  // -0500
	// printf("c3784 getTimeZoneDiff buf=[%s]\n", buf );
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

// return milliseconds timestamp
abaxint JagTime::mtime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	return (1000*(abaxint)now.tv_sec + now.tv_usec/1000);
}

// return microseconds timestamp
abaxint JagTime::utime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	return (1000000*(abaxint)now.tv_sec + now.tv_usec);
}

//  N: years back
AbaxDataString JagTime::makeRandDateTimeString( int N )
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm  result;
  	char buffer [80];
	abaxint yearSecs = 365 * 86400;
	abaxint rnd = rand() % (N*yearSecs);

  	time (&rawtime);
	rawtime -=  rnd;
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer,80,"%Y-%m-%d %H:%M:%S", timeinfo);
	return buffer;
}

AbaxDataString JagTime::makeRandTimeString()
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

AbaxDataString JagTime::YYYYMMDDHHMM()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm  result;
	char  buffer[64];

  	time (&rawtime);
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime( buffer, 64, "%Y-%m-%d-%H-%M", timeinfo);
	return buffer;
}

AbaxDataString JagTime::YYYYMMDD()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
	char  buffer[64];
  	time (&rawtime);
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime( buffer, 64, "%Y%m%d", timeinfo);
	return buffer;
}

AbaxDataString JagTime::makeNowTimeStringSeconds()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
	char  buffer[80];
  	time (&rawtime);
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	return buffer;
}

AbaxDataString JagTime::makeNowTimeStringMilliSeconds()
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
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	sprintf( endbuf, ".%d", usec/1000 );
	strcat( buffer, endbuf );
	return buffer;
}

AbaxDataString JagTime::makeNowTimeStringMicroSeconds()
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
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
	sprintf( endbuf, ".%d", usec );
	strcat( buffer, endbuf );
	return buffer;
}

abaxint JagTime::nowMilliSeconds()
{
    struct timeval now;
    gettimeofday( &now, NULL );
	return (abaxint)now.tv_usec * 1000 + now.tv_sec/1000;
}

AbaxDataString JagTime::nowYear()
{
	time_t rawtime;
  	struct tm * timeinfo;
  	struct tm result;
	char  buffer[8];
  	time (&rawtime);
  	// timeinfo = localtime (&rawtime);
	timeinfo = jag_localtime_r ( &rawtime, &result );
  	strftime (buffer, 8, "%Y", timeinfo);
	return buffer;
}

#if 0
// input: rawSeconds "1538282828"
AbaxDataString JagTime::rawToClientString( const JagParseAttribute &jpa, time_t seconds, const AbaxDataString &ttype )
{
	//abaxint  defTZDiffMin = jpa.timediff;
	//abaxint servTZDiffMin = jpa.servtimediff;
	//abaxint minSrvCli = servTZDiffMin - defTZDiffMin;
	//int minSrvCli = jpa.servtimediff - jpa.timediff;
	int minSrvCli = jpa.timediff;
	prt(("s2096 rawToString jpa.servtimediff=%d jpa.timediff=%d minSrvCli=%d\n", jpa.servtimediff, jpa.timediff, minSrvCli ));

	//seconds -=  minSrvCli * 60;
	seconds +=  minSrvCli * 60;

	struct tm timeinfo;
	//jag_localtime_r( &seconds, &timeinfo );
	gmtime_r( &seconds, &timeinfo );
	char buf[64];
	memset(buf, 0, 64 );
	if (  ttype == JAG_C_COL_TYPE_DATETIME ) {
  		strftime (buf, 64, "%Y-%m-%d %H:%M:%S", &timeinfo);
	} else if (  ttype == JAG_C_COL_TYPE_TIME ) {
  		strftime (buf, 64, "%H:%M:%S", &timeinfo);
	} else if (  ttype == JAG_C_COL_TYPE_DATE ) {
  		strftime (buf, 64, "%Y-%m-%d", &timeinfo);
	} 
	prt(("s2228 buf=[%s]\n", buf ));
	return buf; 
}
#endif

#if 0
// input: rawSeconds "1538282828"
AbaxDataString JagTime::rawToServerString( const JagParseAttribute &jpa, time_t seconds, const AbaxDataString &ttype )
{
	//abaxint  defTZDiffMin = jpa.timediff;
	//abaxint servTZDiffMin = jpa.servtimediff;
	//abaxint minSrvCli = servTZDiffMin - defTZDiffMin;
	//int minSrvCli = jpa.servtimediff - jpa.timediff;
	//seconds +=  minSrvCli * 60;

	struct tm timeinfo;
	jag_localtime_r( &seconds, &timeinfo ); // server localtime
	//gmtime_r( &seconds, &timeinfo );
	char buf[64];
	memset(buf, 0, 64 );
	if (  ttype == JAG_C_COL_TYPE_DATETIME ) {
  		strftime (buf, 64, "%Y-%m-%d %H:%M:%S", &timeinfo);
	} else if (  ttype == JAG_C_COL_TYPE_TIME ) {
  		strftime (buf, 64, "%H:%M:%S", &timeinfo);
	} else if (  ttype == JAG_C_COL_TYPE_DATE ) {
  		strftime (buf, 64, "%Y-%m-%d", &timeinfo);
	} 
	prt(("s2228 buf=[%s]\n", buf ));
	return buf; 
}
#endif


// str: "2016-02-13 12:32:22"   "12:23:00" "yyyy-mm-dd" "152939440000"
void JagTime::setTimeInfo( const JagParseAttribute &jpa , const char *str, struct tm &timeinfo, int isTime ) 
{
	// printf("s2838 setTimeInfo str=[%s]  isTime=%d\n", str, isTime );
	const char *ttime1, *tdate2;
	abaxint microsec = -1;
	
	ttime1 = strchr(str, ':'); 
	tdate2 = strchr(str, '-');

	if ( ttime1 && tdate2 ) {
		// "2016-02-13 12:32:22"
		if ( 1 == isTime ) microsec = getDateTimeFromStr( jpa, str );
		else microsec = getDateTimeFromStr( jpa, str, true );
	} else if ( ttime1 ) {
		// "12:23:00"
		if ( 1 == isTime ) microsec = getTimeFromStr( str );
		else microsec = getTimeFromStr( str, true );
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
			// "19293944"
			microsec = jagatoll( str );
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

	if ( microsec >= 0 ) {
		time_t secs;
		if ( 1 == isTime ) secs = microsec / 1000000;
		else secs = microsec / 1000000000;

		// gmtime_r( &secs, &timeinfo );
		abaxint  defTZDiffMin = jpa.timediff;
		abaxint servTZDiffMin = jpa.servtimediff;
		abaxint minSrvCli = servTZDiffMin - defTZDiffMin;
		// prt(("s2394 secs=%lld\n", secs ));

		secs -=  minSrvCli * 60;

		/***
		#ifndef _WINDOWS64_
		localtime_r( &secs, &timeinfo );
		#else
		struct tm *result = localtime( &secs );
		timeinfo = *result;
		#endif
		***/
		jag_localtime_r( &secs, &timeinfo );

	}
}


AbaxFixString JagTime::getValueFromTimeOrDate( const JagParseAttribute &jpa, const AbaxFixString &str, 
									const AbaxFixString &str2, int op, const AbaxDataString& ddiff ) 
{
	// prt(("s2839 getValueFromTimeOrDate str=[%s] str2=[%s] op=%d ddiff=%d\n", str.c_str(), str2.c_str(), op, ddiff ));
	char *p;
	char savebyte;
	int timelen, isTime;
	struct tm timeinfo, timeinfo2;
	time_t rawtime;

	time( &rawtime );
	timelen = JAG_DATETIMENANO_FIELD_LEN-6;
	char buf[timelen+1];
	memset(buf, 0, timelen+1);
	
	// first timeinfo
	/**
	timeinfo.tm_isdst = -1;
	gmtime_r( &rawtime, &timeinfo );
	**/
	isTime = 0;

	if ( str.length() == JAG_DATETIME_FIELD_LEN || str.length() == JAG_TIME_FIELD_LEN 
		 || str.length() == JAG_TIMESTAMP_FIELD_LEN ) isTime = 1;
	else if ( str.length() == JAG_DATETIMENANO_FIELD_LEN 
		 || str.length() == JAG_TIMENANO_FIELD_LEN ) isTime = 2;

	p = (char*)str.c_str();
	savebyte = *(p+str.length());
	*(p+str.length()) = '\0';

	setTimeInfo( jpa, p, timeinfo, isTime );
	*(p+str.length()) = savebyte;	

	// printf("s3994 op=[%d] str=[%s]  str2=[%s]  ddif=[%d]\n", op, str.c_str(), str2.c_str(), ddiff );
	
	if ( op == JAG_FUNC_SECOND ) {
		sprintf(buf, "%d", timeinfo.tm_sec);
	} else if ( op == JAG_FUNC_MINUTE ) {
		sprintf(buf, "%d", timeinfo.tm_min);
	} else if ( op == JAG_FUNC_HOUR ) {
		sprintf(buf, "%d", timeinfo.tm_hour);
	} else if ( op == JAG_FUNC_DATE ) {
		sprintf(buf, "%d", timeinfo.tm_mday);
	} else if ( op == JAG_FUNC_MONTH ) {
		sprintf(buf, "%d", timeinfo.tm_mon + 1);
	} else if ( op == JAG_FUNC_YEAR ) {
		sprintf(buf, "%d", timeinfo.tm_year + 1900);
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

		// second timeinfo
		// gmtime_r( &rawtime, &timeinfo2 );
		isTime = 0;
		if ( str2.length() == JAG_DATETIME_FIELD_LEN || str2.length() == JAG_TIME_FIELD_LEN 
			 || str2.length() == JAG_TIMESTAMP_FIELD_LEN ) isTime = 1;
		else if ( str2.length() == JAG_DATETIMENANO_FIELD_LEN || str2.length() == JAG_TIMENANO_FIELD_LEN ) isTime = 2;

		p = (char*)str2.c_str();
		savebyte = *(p+str2.length());
		*(p+str2.length()) = '\0';
		setTimeInfo( jpa, p, timeinfo2, isTime );		
		*(p+str2.length()) = savebyte;

		time_t endt = mktime( &timeinfo2 );
		time_t begint = mktime( &timeinfo );
		abaxint diff = 0;
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
	
	AbaxFixString res(buf, timelen);
	return res;	
}

 
 // tzdiff in minutes
// convert GMT time microsecs or nanosecs to caller Local time (usually client)
// void convertDateTimeToLocalStr( int tzdiff, const AbaxDataString& instr, AbaxDataString& outstr, bool isnano )
void JagTime::convertDateTimeToLocalStr( const AbaxDataString& instr, AbaxDataString& outstr, bool isnano )
{
	// print keys
	char tmstr[48];
	char utime[8];
	abaxint timestamp;
	time_t  sec;
	abaxint  usec;
	struct tm *tmptr;
	struct tm result;

	//prt(("c4828 convertDateTimeToLocalStr instr=[%s]\n", instr.c_str() ));

	timestamp = jagatoll( instr.c_str() );
	if ( !isnano ) {
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	} else {
		sec = timestamp/1000000000; 
		usec = timestamp%1000000000;
	}

	// tmptr = localtime( &sec );  // client calls this
	tmptr = jag_localtime_r( &sec, &result );  // client calls this
	// tmptr = gmtime( &sec );

	strftime( tmstr, sizeof(tmstr), "%Y-%m-%d %H:%M:%S", tmptr ); 

	if ( !isnano ) sprintf( utime, ".%06d", usec );
	else sprintf( utime, ".%09d", usec );

	strcat( tmstr, utime );
	outstr = tmstr;
	//prt(("c1183 cnvtDatTimToStr ins=[%s] outs=[%s] isnano=%d\n", instr.c_str(), outstr.c_str(), isnano ));
}

// input: instr "1928282292929"
// output new Time format  "HH:MM:SS"
// tmtype=0: no micro/o/nano sub-seconds
// tmtype=1: micro subseconds
// tmtype=2: nano subseconds
void JagTime::convertTimeToStr( const AbaxDataString& instr, AbaxDataString& outstr, int tmtype )
{
	// print keys
	char tmstr[24];
	char utime[8];
	abaxint timestamp;
	time_t  sec;
	abaxint  usec;
	struct tm *tmptr;


	timestamp = jagatoll( instr.c_str() );
	if ( 1 == tmtype ) {
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	} else if ( 2 == tmtype ) {
		sec = timestamp/1000000000; 
		usec = timestamp%1000000000;
	} else { 
		sec = timestamp/1000000; 
		usec = timestamp%1000000;
	}

	struct tm result;
	tmptr = gmtime_r( &sec, &result );
	strftime( tmstr, sizeof(tmstr), "%H:%M:%S", tmptr ); 

	/**
	if ( !isnano ) sprintf( utime, ".%06d", usec );
	else sprintf( utime, ".%09d", usec );
	**/
	if ( 1 == tmtype ) {
	    sprintf( utime, ".%06d", usec );
		strcat( tmstr, utime );
	} else if ( 2 == tmtype ) {
		sprintf( utime, ".%09d", usec );
		strcat( tmstr, utime );
	} 

	outstr = tmstr;
	//prt(("tm8208 convertTimeToStr instr=[%s] tmtype=%d outstr=[%s]\n", instr.c_str(), tmtype, outstr.c_str() ));

	//prt(("s3881 convertTimeToStr instr=[%s] outstr=[%s]\n", instr.c_str(), outstr.c_str() ));
}

// instr: "yyyymmdd"
// outstr: "yyyy-mm-dd"
void JagTime::convertDateToStr( const AbaxDataString& instr, AbaxDataString& outstr )
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

// tzdiff: client tz difff in min
int JagTime::convertDateTimeFormat( const JagParseAttribute &jpa, char *outbuf, const char *inbuf, 
							const int offset, const int length, bool isnano )
{
	// prt(("s3661 convertDateTimeFormat inbuf=[%s] isnano=%d\n", inbuf, isnano ));
    abaxint lonnum = 0;

	// allow "microseconds" or "nanoseconds" since epoch digits input UTC time
	// timestamp is microseconds since epoch, same as datetime
	if ( ( NULL == strchr((char*)inbuf, '-') && NULL == strchr((char*)inbuf, ':') ) ) {
		if ( isnano ) {
			lonnum = jagatoll( inbuf ) * 1000;
		} else {
			lonnum = jagatoll( inbuf );
		}
		// prt(("s2883 pure num in datestr inbuf lonnum=%lld\n", lonnum  ));
	} else {
		// lonnum = getDateTimeFromStr( tzdiff, inbuf, isnano);
		lonnum = getDateTimeFromStr( jpa, inbuf, isnano);
		if ( lonnum == -1 ) {
			return 1;
		}
	}

    if ( snprintf(outbuf+offset, length+1, "%0*lld", length, lonnum) > length ) {
		// prt(("s1884 convertDateTimeFormat return 2\n" ));
		return 2;
	}

	// prt(("s1885 convertDateTimeFormat return 0  lonnum=%lld  outbuf+offset=[%s]\n", lonnum, outbuf+offset ));
	return 0;
}

int JagTime::convertTimeFormat( char *outbuf, const char *inbuf, const int offset, const int length, bool isnano )
{
	//prt(("s4774 convertTimeFormat inbuf=[%s] offset=%d length=%d isnano=%d\n", inbuf, offset, length, isnano ));
    abaxint lonnum = 0;

	/***
	if ( ( NULL == strchr((char*)inbuf, ':') ) || (lonnum = getTimeFromStr(inbuf, isnano)) == -1 )  {
		prt(("s5980 return 1 lonnum=%d\n", lonnum ));
		return 1;
	}
	***/
	if ( strchr((char*)inbuf, ':') ) {
		lonnum = getTimeFromStr(inbuf, isnano);
		if ( -1 == lonnum ) return 1;
	} else {
		lonnum = jagatoll( inbuf );
	}

    if ( snprintf(outbuf+offset, length+1, "%0*lld", length, lonnum) > length ) {
		//prt(("s5981 return 2\n" ));
		return 2;
	}

	return 0;
}


int JagTime::convertDateFormat( char *outbuf, const char *inbuf, const int offset, const int length )
{
	char buf[JAG_DATE_FIELD_LEN+1];
	const char *ubuf;
	memset(buf, 0, JAG_DATE_FIELD_LEN+1);
		
	if ( NULL != strchr((char*)inbuf, '-') && !getDateFromStr(inbuf, buf) ) {
		// printf("s6338 here convertDateFormat return 1 inbuf=[%s]   buf=[%s]\n", inbuf, buf );
		return 1;
	}

	if ( *buf != '\0' ) ubuf = buf;
	else ubuf = inbuf;
	// printf("s6348 here convertDateFormat return 1 inbuf=[%s]   buf=[%s]\n", inbuf, buf );

	if ( snprintf(outbuf+offset, length+1, "%s", ubuf) > length ) return 2;

	return 0;
}


// get GMT time in microseconds, or nanoseconds (if isnano is true)
// str:   yyyy-mm-dd hh:mm:ss[.ffffff]
// str:   yyyy-mm-dd hh:mm:ss[.ffffff] +HH:MM  (timezone offset)
// str:   yyyy-mm-dd hh:mm:ss[.ffffff] -HH:MM  (timezone offset)
// defTZDiffMin: client's default timezone diff in minutes (-8 or +10)
// if isnano, str has [.fffffffff] 
// -1: error
// abaxint getDateTimeFromStr( int defTZDiffMin, const char *str, bool isnano )
abaxint JagTime::getDateTimeFromStr( const JagParseAttribute &jpa, const char *str, bool isnano )
{
	abaxint  defTZDiffMin = jpa.timediff;
	abaxint servTZDiffMin = jpa.servtimediff;
	abaxint minSrvCli = servTZDiffMin - defTZDiffMin;

	// prt(("s1828 getDateTimeFromStr str=[%s] defTZDfMin=%d srvTZMin=%d minSrvCli=%d\n", str, defTZDiffMin, servTZDiffMin, minSrvCli ));

	int c;
	char buf10[10];
    memset(buf10, 0, 10);
	struct tm  ttime;
	char *s = (char*) str;
	char *p;
	char v;
	abaxint res;

	ttime.tm_isdst = -1;

	// get yyyy
	p = s;
	while ( *p != '-' && *p != '\0' ) ++p;
	// if ( *p == '\0' ) return -1;
	if ( *p == '\0' ) return 0;

	*p = '\0';
	ttime.tm_year = atoi(s) - 1900;
	if ( ttime.tm_year >= 10000 ) return -1;
	*p = '-';
	s = p+1;
	if ( *s == '\0' ) return -1;

	// get mm
	p = s;
	while ( *p != '-' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return -1;
	*p = '\0';
	ttime.tm_mon = atoi(s) - 1;
	// if ( ttime.tm_mon < 0 || ttime.tm_mon > 11 ) return -1;
	*p = '-';

	// get dd
	s = p+1;
	if ( *s == '\0' ) return -1;

	p = s;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return -1;
	*p = '\0';
	ttime.tm_mday = atoi(s);
	// if ( ttime.tm_mday < 1 || ttime.tm_mday > 31 ) return -1;
	*p = ' ';

	while ( *p == ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return -1;

	// hh:mm:ss[.ffffff]
	// get hh
	s = p;
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return -1;
	*p = '\0';
	ttime.tm_hour = atoi(s);
	// if ( ttime.tm_hour > 23 ) return -1;
	*p = ':';

	// get mm
	++p;
	s = p;
	while ( *p != ':' && *p != '\0' ) ++p;
	if ( *s == '\0' ) return -1;
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

   	if ( isnano ) res *= (abaxint)1000000000;
   	else res *= (abaxint)1000000;

	while ( 1 ) {
    	// prt(("s8907 time str=[%s] ===> res=[%lld]\n", str, res));
    	if ( *p == '\0' ) {
			break;
    	}
    
    	// saw .ffffff or .fffffffff
    	if ( *p == '.' ) {
    		if ( isnano ) {
        		memset(buf10, '0', 9);
    		} else {
    			memset(buf10, '0', 6);
    		}
        	++p;
        	s = p;
        	c = 0;
    		while ( *p != '\0' && *p != '+' && *p != '-' && *p != ' ' ) {
    			if ( !isnano && c < 6 ) {
    				buf10[c++] = *p;
    			} else if ( isnano && c < 9 ) {
    				buf10[c++] = *p;
    			} else {
    				while ( *p != '\0' && *p != ' ' ) ++p;
    				break;
    			}
    			++p;
    		}
    
        	res += jagatoll(buf10);
    	}
    
    	if ( *p == '\0' ) {
			break;
    	}
    
    	while ( *p == ' ' ) ++p;  // skip blanks
    	if ( *p == '\0' ) {
    		// prt(("c8133 getdatetimefromstr return res=[%lld]\n", res ));
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
    	abaxint zsecdiff;
		abaxint tzhour = jagatoll(s);
		// prt(("s6612 before hour adjust tzhour=%d res=%lld\n", tzhour, res ));
    	if ( c > 0 ) {
    		zsecdiff =  tzhour *3600 - defTZDiffMin*60;  // adjust to original clien time zone
    		if ( !isnano ) res -=  zsecdiff * (abaxint)1000000;
    		else res -=  zsecdiff * (abaxint)1000000000;
    	} else {
    		zsecdiff =  defTZDiffMin*60 + tzhour*3600;
    		if ( !isnano ) res +=  zsecdiff * (abaxint)1000000;
    		else res +=  zsecdiff * (abaxint)1000000000;
    	}

    	*p = v;
    	if ( *p == '\0' ) {
			break;
    	}

    	++p; // past :   :30 minute part
    	if ( *p == '\0' ) {
			break;
		}

    	s = p;
		// prt(("s8309 s=[%s] res=%lld\n", s, res ));
    	if ( c > 0 ) {
    		if ( !isnano ) res = res - jagatoll(s) * 60 * (abaxint)1000000;  // res is GMT time
    		else res = res - jagatoll(s) * 60 * (abaxint)1000000000;  // res is GMT time
    	} else {
    		if ( !isnano ) res = res + jagatoll(s) * 60 * (abaxint)1000000;
    		else res = res + jagatoll(s) * 60 * (abaxint)1000000000;
    	}

		break;  // must be here to exit the loop
	}

	// prt(("s4309 res=%lld\n", res ));
	return res;
}


// return time in microseconds
// str:   hh:mm:ss[.ffffff]
// -1: error
abaxint JagTime::getTimeFromStr( const char *str, bool isnano )
{
	//prt(("s2039 getTimeFromStr str=[%s] isnano=%d\n", str, isnano ));
	int c;
	char buf10[10];
    memset(buf10, 0, 10);
	struct tm  ttime;
	char *s = (char*) str;
	char *p;
	char v;
	abaxint res;

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
	if ( !isnano ) res *= (abaxint)1000000;
	else res *= (abaxint)1000000000;
	// prt(("s8907 time str=[%s] ===> res=[%lld]\n", str, res));
	if ( *p == '\0' ) {
		// printf("c111\n");
		return res;
	}

	// saw .fffffff or .fffffffff
	if ( *p == '.' ) {
		if ( !isnano ) {
			memset(buf10, '0', 6);
		} else {
    		memset(buf10, '0', 9);
		}
    	++p;
    	s = p;
    	c = 0;
    	while ( *p != '\0' && *p != ' ' ) {
    		// printf("c=%d  p=[%s]\n", c, p );
			if ( !isnano && c < 6 ) {
				buf10[c++] = *p;
			} else if ( isnano && c < 9 ) {
				buf10[c++] = *p;
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


// instr:  2013-2-14  2015-03-12  2013-3-1
// outstr: "yyyymmdd"
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
