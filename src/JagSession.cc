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

#include <JagSession.h>
//#include <JagTable.h>
#include <JagUtil.h>

// ctor
JagSession::JagSession( int timeout )
{
	dtimeout = timeout;
	sock = active = done = timediff = connectionTime = 0;
	origserv = 0;
	exclusiveLogin = 0;
	fromShell = 0;
	servobj = NULL;
	replicateType = 0;
	drecoverConn = 0;
	spCommandReject = 0;
	hasTimer = 0;
	sessionBroken = 0;
	samePID = 0;
	dcfrom = 0;
	dcto = 0;
	pthread_mutex_init( &dataMutex, NULL );
}

// dtor
JagSession::~JagSession() 
{
	// if has timer, join thread
	sessionBroken = 1;
	if ( hasTimer ) {
		pthread_join( threadTimer, NULL );
	}
	pthread_mutex_destroy( &dataMutex );
}

// create separate thread for timer
void JagSession::createTimer()
{
	if ( samePID ) return;
	hasTimer = 1;
	jagpthread_create( &threadTimer, NULL, sessionTimer, (void*)this );
	jaguar_mutex_lock ( &dataMutex );
	jaguar_mutex_unlock ( &dataMutex );
}

// separate thread to set timer and send doing processing cmd per 5 secs
void *JagSession::sessionTimer( void *ptr )
{
	JagSession *sn = (JagSession*)ptr;
	while ( !sn->sessionBroken ) {
		jagsleep(3, JAG_SEC);
		sendMessageLength2( sn, "Y", 1, "HB" );
		// heartbeat from server to client in session
	}

	return NULL;
}
