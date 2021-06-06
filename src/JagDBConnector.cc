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

#include <JagDBConnector.h>
#include <JagCfg.h>
#include <JagNet.h>
#include <JaguarCPPClient.h>
#include <JagStrSplit.h>
#include <JagUtil.h>
#include <JagParseParam.h>

JagDBConnector::JagDBConnector( )
{
	_lock = newJagReadWriteLock();
	_passwd = "dummy";
	_parentCliNonRecover = NULL;
	_parentCli = NULL;
	_broadcastCli = NULL;
	
	JagCfg cfg;
	AbaxString port = cfg.getValue("PORT");
	if ( port.size() < 1 ) {
		port = "8888";
	}
	_port = atoi( port.c_str() );
	_listenIP = cfg.getValue("LISTEN_IP", "");
	_servToken = cfg.getValue("SERVER_TOKEN", "wvcYrfYdVagqXQ4s3eTFKyvNFxV" );
	_nodeMgr = new JagNodeMgr( _listenIP );
}

JagDBConnector::~JagDBConnector()
{
	if ( _lock ) {
		deleteJagReadWriteLock( _lock );
	}
	if ( _nodeMgr ) {
		delete _nodeMgr;
		_nodeMgr = NULL;
	}
	if ( _parentCliNonRecover ) {
		delete _parentCliNonRecover;
		_parentCliNonRecover = NULL;
	}
	if ( _parentCli ) {
		delete _parentCli;
		_parentCli = NULL;
	}
	if ( _broadcastCli ) {
		delete _broadcastCli;
		_broadcastCli = NULL;
	}
}

void JagDBConnector::makeInitConnection( bool debugClient )
{
	raydebug( stdout, JAG_LOG_LOW, "makeInitConnection debugClient=%d\n", debugClient );
	Jstr username, dbname;
	username = "admin";
	dbname = "test";

	if ( _parentCliNonRecover ) {
		delete _parentCliNonRecover;
		_parentCliNonRecover = NULL;
	}

	if ( _parentCli ) {
		delete _parentCli;
		_parentCli = NULL;
	}

	if ( _broadcastCli ) {
		delete _broadcastCli;
		_broadcastCli = NULL;
	}

	_parentCliNonRecover = newObject<JaguarCPPClient>();
	_parentCliNonRecover->setDebug( debugClient );

	_parentCli = newObject<JaguarCPPClient>();
	_parentCli->_deltaRecoverConnection = 1;
	_parentCli->setDebug( debugClient );

	_broadcastCli = newObject<JaguarCPPClient>();
	_broadcastCli->setDebug( debugClient );

	Jstr unixSocket = Jstr("/TOKEN=") + _servToken;

	raydebug( stdout, JAG_LOG_LOW, "Init Connection Localhost [%s:%d]\n", _nodeMgr->_selfIP.c_str(), _port );
	while ( !_parentCliNonRecover->connect( _nodeMgr->_selfIP.c_str(), _port, username.c_str(), _passwd.c_str(), 
								  dbname.c_str(), unixSocket.c_str(), JAG_SERV_PARENT ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4122 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  username.c_str(), _passwd.c_str(), _nodeMgr->_selfIP.c_str(), _port, _parentCliNonRecover->error() );
		jagsleep(5, JAG_SEC);
	}

	while ( !_parentCli->connect( _nodeMgr->_selfIP.c_str(), _port, username.c_str(), _passwd.c_str(), 
								  dbname.c_str(), unixSocket.c_str(), JAG_SERV_PARENT ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4123 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  username.c_str(), _passwd.c_str(), _nodeMgr->_selfIP.c_str(), _port, _parentCli->error() );
		jagsleep(5, JAG_SEC);
	}

	while ( !_broadcastCli->connect( _nodeMgr->_selfIP.c_str(), _port, username.c_str(), _passwd.c_str(), 
								  dbname.c_str(), unixSocket.c_str(), JAG_SERV_PARENT ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4124 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  username.c_str(), _passwd.c_str(), _nodeMgr->_selfIP.c_str(), _port, _broadcastCli->error() );
		jagsleep(5, JAG_SEC);
	}
	raydebug( stdout, JAG_LOG_LOW, "Connection to self %s:%d OK\n", _nodeMgr->_selfIP.c_str(), _port );
}

bool JagDBConnector::broadcastSignal( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli, bool toAll )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	bool rc = true;
	JagStrSplit split(_nodeMgr->_allNodes, '|', true);
	int n = 0, len = split.length();
	if ( !toAll && len <= 1 ) {
		return false;
	}
	JaguarCPPClient *tcli = _broadcastCli;
	if ( ucli ) tcli = ucli;
	
    pthread_t thrd[len];
	BroadCastAttr pass[len];
	n = 0;
	if ( hostlist.size() < 1 ) { 
	    for ( int i = 0; i < len; ++i ) {
			if ( toAll || split[i] != _nodeMgr->_selfIP ) {
	   			pass[n].usecli = tcli;
   				pass[n].host = split[i];
   				pass[n].cmd = signal;
				pass[n].getStr = 0;
				pass[n].result = 1;
	   			jagpthread_create( &thrd[n], NULL, queryDBServerStatic, (void*)&pass[n] );
	   			++n;
	   		}
	   	}
	} else { 
		JagStrSplit sp( hostlist, '|' );
		for ( int i = 0; i < sp.length(); ++i ) {
			if ( toAll || sp[i] != _nodeMgr->_selfIP ) {
	   			pass[n].usecli = tcli;
   				pass[n].host = sp[i];
   				pass[n].cmd = signal;
				pass[n].getStr = 0;
				pass[n].result = 1;
	   			jagpthread_create( &thrd[n], NULL, queryDBServerStatic, (void*)&pass[n] );
	   			++n;
			}
	   	}
	}
    
    for ( int i = 0; i < n; ++i ) {
    	pthread_join(thrd[i], NULL);
		if ( pass[i].result == 0 ) rc = false;
    }

	return rc;
}

Jstr JagDBConnector::broadcastGet( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	Jstr res;
	JagStrSplit split(_nodeMgr->_allNodes, '|', true);
	int n = 0, len = split.length();
	if ( len <= 1 ) { return ""; }
	JaguarCPPClient *tcli = _broadcastCli;
	if ( ucli ) tcli = ucli;

	pthread_t thrd[len];
	BroadCastAttr pass[len];

	if ( hostlist.size() < 1 ) { 
	    for ( int i = 0; i < len; ++i ) {
			if ( split[i] != _nodeMgr->_selfIP ) {
	   			pass[n].usecli = tcli;
   				pass[n].host = split[i];
   				pass[n].cmd = signal;
				pass[n].getStr = 1;
				pass[n].result = 1;
	   			jagpthread_create( &thrd[n], NULL, queryDBServerStatic, (void*)&pass[n] );
	   			++n;
	   		}
	   	}
	} else { 
		JagStrSplit sp( hostlist, '|' );
		for ( int i = 0; i < sp.length(); ++i ) {
			if ( sp[i] != _nodeMgr->_selfIP ) {
	   			pass[n].usecli = tcli;
   				pass[n].host = sp[i];
   				pass[n].cmd = signal;
				pass[n].getStr = 1;
				pass[n].result = 1;
	   			jagpthread_create( &thrd[n], NULL, queryDBServerStatic, (void*)&pass[n] );
	   			++n;
			}
	   	}
	}

	for ( int i = 0; i < n; ++i ) {
		pthread_join(thrd[i], NULL);
		res += pass[i].value + "\n";
	}

	return res;
}

void *JagDBConnector::queryDBServerStatic( void *ptr )
{
	BroadCastAttr *pass = (BroadCastAttr*)ptr;

	const char *p;
	pass->value = "";
	JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->usecli->_connMap, pass->host.c_str() );
	if ( ! jcli ) {
		abort();
		return NULL;
	}

	if ( jcli->queryDirect( 0, pass->cmd.c_str(), pass->cmd.size(), true, false, true ) == 0 ) {
		pass->result = 0;
	} else {
		while ( jcli->reply() > 0 ) {
			p = jcli->getMessage();
			if ( ! p ) continue;
			if ( pass->getStr ) pass->value += p;
		}
		if ( jcli->getSocket() == -1 ) {
			pass->result = 0;
		}
	}

	return NULL;
}

