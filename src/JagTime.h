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
#ifndef _jag_time_h_
#define _jag_time_h_

#include <time.h>
#include <abax.h>
#include <JagParseAttribute.h>

class JagTime
{
	public:
		static int getTimeZoneDiff();  //  minutes
		static jaguint mtime();  //  time(NULL) + milliseconds
		static jaguint utime();  //  time(NULL) + microseconds
		static Jstr makeRandDateTimeString( int N);
		static Jstr makeRandTimeString();
		static Jstr YYYYMMDDHHMM();
		static Jstr YYYYMMDD();
		static jagint nowMilliSeconds();


		static Jstr makeNowTimeStringSeconds();
		static Jstr makeNowTimeStringMilliSeconds();
		static Jstr makeNowTimeStringMicroSeconds();
		static void setTimeInfo( const JagParseAttribute &jpa , const char *str, struct tm &timeinfo, int isTime );

		static JagFixString getValueFromTimeOrDate( const JagParseAttribute &jpa, const JagFixString &str,
 													 const JagFixString &str2, int op, const Jstr &ddif );

		static void convertDateTimeToLocalStr( const Jstr& instr, Jstr& outstr, int isnano=0 );
		static void convertTimeToStr( const Jstr& instr, Jstr& outstr, int tmtype=2 );
		static void convertDateToStr( const Jstr& instr, Jstr& outstr );
		static int convertDateTimeFormat( const JagParseAttribute &jpa, char *outbuf, const char *inbuf, 
										  int offset, int length, int isnano=0 );

		static int convertTimeFormat( char *outbuf, const char *inbuf, int offset, int length, int isnano = 0 );
		static int convertDateFormat( char *outbuf, const char *inbuf, int offset, int length );
		static jaguint getDateTimeFromStr( const JagParseAttribute &jpa, const char *str, int isnano=0 );
		static jagint getTimeFromStr( const char *str, int isnano=0 );
		static bool getDateFromStr( const char *instr, char *outstr );

		static jagint getStartTimeSecOfSecond( time_t tsec, int cycle );
		static jagint getStartTimeSecOfMinute( time_t tsec, int cycle );
		static jagint getStartTimeSecOfHour( time_t tsec, int cycle );
		static jagint getStartTimeSecOfDay( time_t tsec, int cycle );
		static jagint getStartTimeSecOfWeek( time_t tsec );
		static jagint getStartTimeSecOfMonth( time_t tsec, int cycle );
		static jagint getStartTimeSecOfQuarter( time_t tsec, int cycle );
		static jagint getStartTimeSecOfYear( time_t tsec, int cycle );
		static jagint getStartTimeSecOfDecade( time_t tsec, int cycle );
		static int fillTimeBuffer ( time_t tsec, const Jstr &colType, char *buf ); 
		static time_t getTypeTime( time_t tsec, const Jstr &colType ); 
		static Jstr getLocalTime( time_t  tsec );

		static void print( struct tm &t );


};

#endif
