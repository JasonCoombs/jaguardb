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

// ctor
JagDBConnector::JagDBConnector( )
{
	_lock = newJagReadWriteLock();
	_passwd = "dummy";
	_parentCliNonRecover = NULL;
	_parentCli = NULL;
	_broadcastCli = NULL;
	
	// get port
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

// dtor
JagDBConnector::~JagDBConnector()
{
	if ( _lock ) {
		/**
		delete _lock;
		_lock = NULL;
		**/
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

// public methods
// static connect JDB server, make init connection
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
	//	raydebug( stdout, JAG_LOG_LOW, "_parentCli NULL\n" );

	if ( _broadcastCli ) {
		delete _broadcastCli;
		_broadcastCli = NULL;
	}
	//raydebug( stdout, JAG_LOG_LOW, "_broadcastCli NULL\n" );

	_parentCliNonRecover = newObject<JaguarCPPClient>();
	_parentCliNonRecover->setDebug( debugClient );
	//raydebug( stdout, JAG_LOG_LOW, "new JaguarCPPClient\n" );

	_parentCli = newObject<JaguarCPPClient>();
	_parentCli->_deltaRecoverConnection = 1;
	_parentCli->setDebug( debugClient );

	_broadcastCli = newObject<JaguarCPPClient>();
	_broadcastCli->setDebug( debugClient );
	//raydebug( stdout, JAG_LOG_LOW, "new _broadcastCli\n" );

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

// send signal to all JDB servers
// return false: no server list in cluster.conf, or some connection failure during broadcast
// otherwise, if all broadcast successfully finished, return true
// if toAll==false, exclude self
bool JagDBConnector::broadcastSignal( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli, bool toAll )
{
	prt(("s200987 broadcastSignal signal=[%s] hostlist=[%s] toAll=%d mutex ...\n", signal.s(), hostlist.s(), toAll ));
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	prt(("s200987 broadcastSignal signal=[%s] hostlist=[%s] toAll=%d mutex done\n", signal.s(), hostlist.s(), toAll ));

	// get a list of all server IPs
	// if not in localIP send command
	bool rc = true;
	JagStrSplit split(_nodeMgr->_allNodes, '|', true);
	prt(("s532383 broadcastSignal _allNodes=[%s]\n", _nodeMgr->_allNodes.s() ));
	int n = 0, len = split.length();
	if ( !toAll && len <= 1 ) {
		prt(("s5003 broadcastSignal allnodes.size()=%d return\n", len ));
		return false;
	}
	JaguarCPPClient *tcli = _broadcastCli;
	if ( ucli ) tcli = ucli;
	
    pthread_t thrd[len];
	BroadCastAttr pass[len];
	n = 0;
	if ( hostlist.size() < 1 ) { // no hostlist, broadcast all
	    for ( int i = 0; i < len; ++i ) {
			if ( toAll || split[i] != _nodeMgr->_selfIP ) {
	   			pass[n].usecli = tcli;
   				pass[n].host = split[i];
   				pass[n].cmd = signal;
				pass[n].getStr = 0;
				pass[n].result = 1;
				prt(("s333887 jagpthread_create n=%d host=%s\n", n, split[i].s() ));
	   			jagpthread_create( &thrd[n], NULL, queryDBServerStatic, (void*)&pass[n] );
				prt(("s333887 jagpthread_create done n=%d\n", n ));
	   			++n;
	   		}
	   	}
	} else { 
		// has hostlist: broadcast to len hosts ( except self )
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
    
	prt(("s222088 pthread_join queryDBServerStatic len=%d n=%d ...\n", len, n ));
	prt(("s2020229 join threads %s ....\n", signal.s() ));
    for ( int i = 0; i < n; ++i ) {
    	pthread_join(thrd[i], NULL);
		if ( pass[i].result == 0 ) rc = false;
    }

	prt(("s122288 done bcastsignal joined threds n=%d rc=%d signal=%s\n\n", n, rc, signal.s() ));

	return rc;
}

// send signal(command) to all OTHER servers
// get all their replies separated by \n
//   host1_result1\n
//   host2_result1\n
//   host3_result1\n
//   host4_result1\n
Jstr JagDBConnector::broadcastGet( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli )
{
	// get a list of all server IPs
	// if not in localIP send command
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	Jstr res;
	JagStrSplit split(_nodeMgr->_allNodes, '|', true);
	int n = 0, len = split.length();
	if ( len <= 1 ) { return ""; }
	JaguarCPPClient *tcli = _broadcastCli;
	if ( ucli ) tcli = ucli;

	pthread_t thrd[len];
	BroadCastAttr pass[len];

	if ( hostlist.size() < 1 ) { // no hostlist, broadcast all
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
		// has hostlist: broadcast to len hosts ( except self )
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

// static
// send cmd to JDB server
// Return value as whole data string, if needed ( for broadCastGet )
// init result is 1, any types of error ( connection error, "ER" type ) result 0
void *JagDBConnector::queryDBServerStatic( void *ptr )
{
	BroadCastAttr *pass = (BroadCastAttr*)ptr;

	// prt(("s0321 thrd=%lld queryDBServer host=[%s] cmd=[%s]\n", THREADID, pass->host.c_str(), pass->cmd.c_str() ));
	// prt(("s0321 thrd=%lld queryDBServer host=[%s]\n", THREADID, pass->host.c_str() ));
	const char *p;
	pass->value = "";
	JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->usecli->_connMap, pass->host.c_str() );
	if ( ! jcli ) {
		prt(("E0283 jag_hash_lookup(%s) got NULL cli\n", pass->host.c_str() ));
		abort();
		return NULL;
	}

	// direct send, no need to send backup serv message
	if ( jcli->queryDirect( 0, pass->cmd.c_str(), pass->cmd.size(), true, false, true ) == 0 ) {
		// send failure
		pass->result = 0;
		prt(("s344828 in queryDBServerStatic [%s] error\n",  pass->cmd.c_str() ));
	} else {
		// else, try reply: if broadCastGet, append message, otherwise ignore
		while ( jcli->reply() > 0 ) {
			p = jcli->getMessage();
			if ( ! p ) continue;
			prt(("s344824 in queryDBServerStatic [req=%s][resp=%s]\n",  pass->cmd.c_str(), p ));
			if ( pass->getStr ) pass->value += p;
		}
		if ( jcli->getSocket() == -1 ) {
			// reply connection failure, no finish receving _END_
			pass->result = 0;
			prt(("s344824 in queryDBServerStatic [req=%s] getSocket==-1\n",  pass->cmd.c_str() ));
		}
	}

	prt(("s344826 queryDBServerStatic done\n"));
	return NULL;
}

