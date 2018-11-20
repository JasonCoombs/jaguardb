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
#ifndef _jag_session_h_
#define _jag_session_h_

#include <pthread.h>
#include <sys/types.h>
#include <atomic>
#include <abax.h>
#include <JagNet.h>

class JagDBServer;

class JagSession 
{
   public:
    JagSession( int timeout=2 );
	~JagSession();
	void createTimer();
	static void *sessionTimer( void *ptr );

   	JAGSOCK sock;
	int active;
	int done;
   	int timediff;  // client's timezone diff in minutes from UTC (+ or -)
	int fromShell;
	int exclusiveLogin;
	int	origserv;
	int replicateType;
	int drecoverConn;
	int samePID;
	int dcfrom;
	int dcto;
	abaxint connectionTime;
	bool spCommandReject;
	bool datacenter;
	JagDBServer *servobj;
	AbaxDataString ip;
	AbaxDataString unixSocket;
	AbaxDataString uid;
	AbaxDataString passwd;
	AbaxDataString dbname;
	AbaxDataString cliPID;
	AbaxDataString loadlocalbpath;

	// data member for separate thread timer
	pthread_t threadTimer;
	pthread_mutex_t timerMutex;
	pthread_mutex_t dataMutex;
	bool hasTimer;
	std::atomic<int8_t> sessionBroken;

	int dtimeout;
};

#endif
