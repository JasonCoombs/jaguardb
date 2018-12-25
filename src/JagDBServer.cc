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
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <strings.h>
#include <string.h>
#include <signal.h>
#include <iostream>
#include <pthread.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <malloc.h>

#include <JagUtil.h>
#include <JagUserID.h>
#include <JagUserRole.h>
#include <JagPass.h>
#include <JagTableSchema.h>
#include <JagIndexSchema.h>
#include <JagSchemaRecord.h>
#include <JagDBServer.h>
#include <JagNet.h>
#include <JagMD5lib.h>
#include <JagStrSplitWithQuote.h>
#include <JagParseExpr.h>
#include <JagSession.h>
#include <JagRequest.h>
#include <JagMutex.h>
#include <JagSystem.h>
#include <JDFSMgr.h>
#include <JagFastCompress.h>
#include <JagTime.h>
#include <JagHashLock.h>
#include <JagServerObjectLock.h>
#include <JagDataAggregate.h>
#include <JagUUID.h>
#include <JagTableUtil.h>
#include <JagParallelParse.h>
#include <JagProduct.h>
#include <JagTable.h>
#include <JagIndex.h>
#include <JagBoundFile.h>
#include <JagDBConnector.h>
#include <JagDiskArrayBase.h>
#include <JagADB.h>
#include <JagIPACL.h>
#include <JagSingleMergeReader.h>
#include <JagDBLogger.h>
#include <JaguarAPI.h>
#include <JagParser.h>
#include <JagLineFile.h>
#include <JagCrypt.h>

extern int JAG_LOG_LEVEL;
int JagDBServer::g_dtimeout = 0;
int JagDBServer::g_receivedSignal = 0;
abaxint JagDBServer::g_lastSchemaTime = 0;
abaxint JagDBServer::g_lastHostTime = 0;

// data member for testing only
abaxint JagDBServer::g_icnt1 = 0;
abaxint JagDBServer::g_icnt2 = 0;
abaxint JagDBServer::g_icnt3 = 0;
abaxint JagDBServer::g_icnt4 = 0;
abaxint JagDBServer::g_icnt5 = 0;
abaxint JagDBServer::g_icnt6 = 0;
abaxint JagDBServer::g_icnt7 = 0;
abaxint JagDBServer::g_icnt8 = 0;
abaxint JagDBServer::g_icnt9 = 0;


pthread_mutex_t JagDBServer::g_dbmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_flagmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_logmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_dlogmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_dinsertmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_datacentermutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_selectmutex = PTHREAD_MUTEX_INITIALIZER;
AbaxDataString  JagDBServer::_allLockToken = "-1";

// ctor
JagDBServer::JagDBServer() 
{
	AbaxDataString fpath = jaguarHome() + "/conf/host.conf";
	AbaxDataString fpathnew = jaguarHome() + "/conf/cluster.conf";
	if ( JagFileMgr::exist( fpath ) && ( ! JagFileMgr::exist( fpathnew ) )  ) {
		jagrename( fpath.c_str(), fpathnew.c_str() );
	}

	_whiteIPList = _blackIPList = NULL;
	_serverReady = false;


	// data member for testing only
	g_icnt1 = g_icnt2 = g_icnt3 = g_icnt4 = g_icnt5 = g_icnt6 = g_icnt7 = g_icnt8 = g_icnt9 = 0;

	g_receivedSignal = g_lastSchemaTime = g_lastHostTime = 0;
	_taskID = _numPrimaryServers = 1;
	_nthServer = 0;
	_xferInsert = 0;
	_isSSD = 0;
	_memoryMode = JAG_MEM_HIGH;
	_shutDownInProgress = 0;
	numInInsertBuffers = numInserts = numSelects = numUpdates = numDeletes = 0;
	_connections = 0;
	
	_hashRebalanceLen = 0;
	_hashRebalanceFD = -1;
	_msyncCount = 0;
	
	// in order to use JDFSMgr later in other classes, JDFSMgr must be create here	
	jdfsMgr = newObject<JDFSMgr>();
	servtimediff = JagTime::getTimeZoneDiff();

	// build _cfg first
	_cfg = newObject<JagCfg>();
	g_dtimeout = atoi(_cfg->getValue("TRANSMIT_TIMEOUT", "3").c_str());
	_userDB = NULL;
	_prevuserDB = NULL;
	_nextuserDB = NULL;
	_userRole = NULL;
	_prevuserRole = NULL;
	_nextuserRole = NULL;
	_tableschema = NULL;
	_indexschema = NULL;
	_prevtableschema = NULL;
	_previndexschema = NULL;
	_nexttableschema = NULL;
	_nextindexschema = NULL;

	_fpCommand = NULL;
	_dinsertCommand = NULL;
	_delPrevOriCommand = NULL;
	_delPrevRepCommand = NULL;
	_delPrevOriRepCommand = NULL;
	_delNextOriCommand = NULL;
	_delNextRepCommand = NULL;
	_delNextOriRepCommand = NULL;
	_recoveryRegCommand = NULL;
	_recoverySpCommand = NULL;

	_objectLock = new JagServerObjectLock( this );
	_createIndexLock = newObject<JagHashLock>();
	_tableUsingLock = newObject<JagHashLock>();
	_jagUUID = new JagUUID();
	pthread_rwlock_init(&_aclrwlock, NULL);

	_internalHostNum = new JagHashMap<AbaxString, abaxint>();

	// also, get other databases name
	AbaxDataString dblist, dbpath;
	dbpath = _cfg->getJDBDataHOME( 0 );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, 0 );
	dbpath = _cfg->getJDBDataHOME( 1 );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, 1 );
	dbpath = _cfg->getJDBDataHOME( 2 );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, 2 );

	_beginAddServer = 0;
	_exclusiveAdmins = 0;
	_doingRemoteBackup = 0;
	_doingRestoreRemote = 0;
	_restartRecover = 0;
	_addClusterFlag = 0;
	_newdcTrasmittingFin = 0;

	init( _cfg );
	_dbConnector = newObject<JagDBConnector>( );
	_clusterMode = true;
	_faultToleranceCopy = atoi(_cfg->getValue("REPLICATION", "3").c_str());
	if ( 0==strcasecmp(PRODUCT_VERSION, "FREETRIAL" ) ) {
		_faultToleranceCopy = 1;
	}

	if ( _faultToleranceCopy < 1 ) _faultToleranceCopy = 1;
	else if ( _faultToleranceCopy > 3 ) _faultToleranceCopy = 3;

	loadACL(); // load self acl, no broadcast

	_sea_records_limit = jagatoll(_cfg->getValue("SEA_RECORDS_LIMIT", "1000000").c_str());
	_jdbMonitorTimedoutPeriod = jagatol(_cfg->getValue("JDB_MONITOR_TIMEDOUT", "600").c_str()); 
	// jdb file monitor timed out period ( in seconds ), default is 600s
	raydebug( stdout, JAG_LOG_LOW, "JdbMonitorTimedoutPeriod = [%d]\n", _jdbMonitorTimedoutPeriod );

	_localInternalIP = _dbConnector->_nodeMgr->_selfIP;
	if ( _localInternalIP.size() > 0 ) {
		// raydebug( stdout, JAG_LOG_LOW, "Host internal IP = [%s]\n", _localInternalIP.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "Host IP = [%s]\n", _localInternalIP.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "Servers [%s]\n", _dbConnector->_nodeMgr->_allNodes.c_str() );
	}

	_totalClusterNumber = _dbConnector->_nodeMgr->_totalClusterNumber;
	_curClusterNumber = _dbConnector->_nodeMgr->_curClusterNumber;
	raydebug( stdout, JAG_LOG_LOW, "Clusters [%d]\n", _totalClusterNumber );
	raydebug( stdout, JAG_LOG_LOW, "Cluster  [%d]\n", _curClusterNumber );

	// server token for authorization
	_servToken = _cfg->getValue("SERVER_TOKEN", "wvcYrfYdVagqXQ4s3eTFKyvNFxV");

	// figure out n-th server of myself
	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	_numPrimaryServers = sp.length();
	for ( int i=0; i < sp.length(); ++i ) {
		if ( sp[i] == _localInternalIP ) {
			_nthServer = i;
			break;
		}
	}

	
	if ( _numPrimaryServers == 1 ) {
		if ( _faultToleranceCopy >= 2 ) {
			_faultToleranceCopy = 1;
		}
	} else if ( _numPrimaryServers == 2 ) {
		if ( _faultToleranceCopy >= 3 ) {
			_faultToleranceCopy = 2;
		}
	}

	// set hash map for host to num 
	for ( int i=0; i < sp.length(); ++i ) {
		_internalHostNum->addKeyValue(AbaxString(sp[i]), i);
	}

	raydebug( stdout, JAG_LOG_LOW, "This is %d-th server in the cluster\n", _nthServer );
	raydebug( stdout, JAG_LOG_LOW, "Fault tolerance copy is %d\n", _faultToleranceCopy );

	_perfFile = jaguarHome() + "/log/perflog.txt";
	JagBoundFile bf( _perfFile.c_str(), 96 );
	bf.openAppend();
	bf.close();

	AbaxDataString dologmsg = makeLowerString(_cfg->getValue("DO_DBLOG_MSG", "no"));
	AbaxDataString dologerr = makeLowerString(_cfg->getValue("DO_DBLOG_ERR", "yes"));
	int logdays = atoi(_cfg->getValue("DBLOG_DAYS", "3").c_str());
	int logmsg=0, logerr=0;
	if ( dologmsg == "yes" ) { logmsg = 1; raydebug( stdout, JAG_LOG_LOW, "DB logging message is enabled.\n" ); } 
	if ( dologerr == "yes" ) { logerr = 1; raydebug( stdout, JAG_LOG_LOW, "DB logging error logger is enabled.\n" ); } 
	if ( logmsg || logerr ) {
		raydebug( stdout, JAG_LOG_LOW, "DB log %d days\n", logdays );
	}
	_dbLogger = new JagDBLogger( logmsg, logerr, logdays );

	_numCPUs = _jagSystem.getNumCPUs();
	raydebug( stdout, JAG_LOG_LOW, "Number of cores %d\n", _numCPUs );
}

// dtor
JagDBServer::~JagDBServer()
{
    destroy();
	jagclose( _dumfd );
}

void JagDBServer::destroy()
{
	if ( _cfg ) {
		delete _cfg;
		_cfg = NULL;
	}

	if ( _userDB ) {
		delete _userDB;
		_userDB = NULL;
	}

	if ( _prevuserDB ) {
		delete _prevuserDB;
		_prevuserDB = NULL;
	}

	if ( _nextuserDB ) {
		delete _nextuserDB;
		_nextuserDB = NULL;
	}

	if ( _userRole ) {
		delete _userRole;
		_userRole = NULL;
	}

	if ( _prevuserRole ) {
		delete _prevuserRole;
		_prevuserRole = NULL;
	}

	if ( _nextuserRole ) {
		delete _nextuserRole;
		_nextuserRole = NULL;
	}

	if ( _tableschema ) {
		delete _tableschema;
		_tableschema = NULL;
	}

	if ( _indexschema ) {
		delete _indexschema;
		_indexschema = NULL;
	}

	if ( _prevtableschema ) {
		delete _prevtableschema;
		_prevtableschema = NULL;
	}

	if ( _previndexschema ) {
		delete _previndexschema;
		_previndexschema = NULL;
	}

	if ( _nexttableschema ) {
		delete _nexttableschema;
		_nexttableschema = NULL;
	}

	if ( _nextindexschema ) {
		delete _nextindexschema;
		_nextindexschema = NULL;
	}

	if ( _dbConnector ) {
		delete _dbConnector;
		_dbConnector = NULL;
	}

	if ( _taskMap ) {
		delete _taskMap;
		_taskMap = NULL;
	}
	
	if ( _joinMap ) {
		delete _joinMap;
		_joinMap = NULL;
	}

	if ( _scMap ) {
		delete _scMap;
		_scMap = NULL;
	}

	if ( _objectLock ) {
		delete _objectLock;
		_objectLock = NULL;
	}

	if ( _createIndexLock ) {
		delete _createIndexLock;
		_createIndexLock = NULL;
	}

	if ( _tableUsingLock ) {
		delete _tableUsingLock;
		_tableUsingLock = NULL;
	}

	if ( _jagUUID ) {
		delete _jagUUID;
		_jagUUID = NULL;
	}

	if ( _internalHostNum ) {
		delete _internalHostNum;
		_internalHostNum = NULL;
	}

	if ( _delPrevOriCommand ) {
		jagfclose( _delPrevOriCommand );
		_delPrevOriCommand = NULL;
	}

	if ( _delPrevRepCommand ) {
		jagfclose( _delPrevRepCommand );
		_delPrevRepCommand = NULL;
	}

	if ( _delPrevOriRepCommand ) {
		jagfclose( _delPrevOriRepCommand );
		_delPrevOriRepCommand = NULL;
	}

	if ( _delNextOriCommand ) {
		jagfclose( _delNextOriCommand );
		_delNextOriCommand = NULL;
	}

	if ( _delNextRepCommand ) {
		jagfclose( _delNextRepCommand );
		_delNextRepCommand = NULL;
	}

	if ( _delNextOriRepCommand ) {
		jagfclose( _delNextOriRepCommand );
		_delNextOriRepCommand = NULL;
	}

	if ( _recoveryRegCommand ) {
		jagfclose( _recoveryRegCommand );
		_recoveryRegCommand = NULL;
	}

	if ( _recoverySpCommand ) {
		jagfclose( _recoverySpCommand );
		_recoverySpCommand = NULL;
	}

	delete _parsePool;
	delete _selectPool;

	if ( _blackIPList )  delete _blackIPList;
	if ( _whiteIPList )  delete _whiteIPList;
	pthread_rwlock_destroy(&_aclrwlock);

	delete _dbLogger;

	closeDataCenterConnection();

}


// dedicated thread
// version4
int JagDBServer::main(int argc, char**argv)
{
	abaxint callCounts = -1, lastBytes = 0;
	raydebug(stdout, JAG_LOG_LOW, "s1101 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	JagNet::socketStartup();

	umask(077);
	initDirs();

	initConfigs();
	raydebug(stdout, JAG_LOG_LOW, "Initialized config data\n" );

	_jagSystem.initLoad(); // for load stats

	pthread_mutex_init( &g_dbmutex, NULL );
	pthread_mutex_init( &g_flagmutex, NULL );
	pthread_mutex_init( &g_logmutex, NULL );
	pthread_mutex_init( &g_dlogmutex, NULL );
	pthread_mutex_init( &g_dinsertmutex, NULL );
	pthread_mutex_init( &g_datacentermutex, NULL );
	pthread_mutex_init( &g_selectmutex, NULL );

	// take acceptions
	createSocket( argc, argv );
	raydebug(stdout, JAG_LOG_LOW, "Initialized listening socket\n" );
	_activeThreadGroups = 0;
	_activeClients = 0;
	_threadGroupSeq = 0;
	makeThreadGroups( _threadGroups + _initExtraThreads, _threadGroupSeq++ );
	raydebug(stdout, JAG_LOG_LOW, "Created socket thread groups\n" );
	raydebug(stdout, JAG_LOG_LOW, "s1105 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );


	initObjects();
	raydebug(stdout, JAG_LOG_LOW, "s1102 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );
	createAdmin();
	makeTableObjects( (void*)this );

	raydebug(stdout, JAG_LOG_LOW, "s1103 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	// setup recover flag when start the server
	JAG_BLURT jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;
	_restartRecover = 1;
	jaguar_mutex_unlock ( &g_flagmutex );

	resetDeltaLog();
	resetRegSpLog();
	raydebug(stdout, JAG_LOG_LOW, "s1104 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	mainInit( argc, argv );
	raydebug(stdout, JAG_LOG_LOW, "s1106 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	printResources();
	_serverReady = true;

	openDataCenterConnection();
	raydebug(stdout, JAG_LOG_LOW, "Jaguar server ready to process commands from clients\n" );

	uabaxint seq = 1;
	
	while ( true ) {
		jagsleep(60, JAG_SEC);

		doBackup( seq );
		writeLoad( seq );
		rotateWalLog();
		rotateDinsertLog();
		refreshDataCenterConnections( seq );

		if ( 0 == (seq%15) ) {
			refreshACL( 1 );
		}

		jaguar_mutex_lock ( &g_dlogmutex );
		if ( checkDeltaFileStatus() && !_restartRecover ) {
			onlineRecoverDeltaLog();
		}
		jaguar_mutex_unlock ( &g_dlogmutex );

		++seq;
		if ( seq >= ULLONG_MAX ) { seq = 0; }
		jagmalloc_trim(0);

		// dynamically increase thread groups
		checkAndCreateThreadGroups();

		// refresh user uid and password
		refreshUserDB( seq );
		refreshUserRole( seq );
	}

	mainClose( );
	JagNet::socketCleanup();
}

// static
void JagDBServer::addTask(  uabaxint taskID, JagSession *session, const char *mesg )
{
	char buf[256];
	const int LEN=64;
	char sbuf[LEN+1];
	memset( sbuf, 0, LEN+1 );
	strncpy( sbuf, mesg, LEN );

	// "threadID|userid|dbname|timestamp|query"
	sprintf( buf, "%s|%s|%s|%lld|%s", 
			ulongToString( pthread_self() ).c_str(), session->uid.c_str(), session->dbname.c_str(), time(NULL), sbuf );

	session->servobj->_taskMap->addKeyValue( taskID, AbaxString(buf) );
}

// static
// Process commands in one thread
int JagDBServer::processMultiCmd( JagRequest &req, const char *mesg, abaxint msglen, JagDBServer *servobj, 
	abaxint &threadSchemaTime, abaxint &threadHostTime, abaxint threadQueryTime, bool redoOnly, int isReadOrWriteCommand )
{
	//prt(("s0922 processMultiCmd mesg=[%s]\n", mesg ));
	if ( servobj->_shutDownInProgress > 0 ) return 0;	

	// add tasks	
	uabaxint taskID;
	++ ( servobj->_taskID );
	taskID =  servobj->_taskID;
	addTask( taskID, req.session, mesg );

	if ( servobj->_shutDownInProgress > 0 ) {
		servobj->_taskMap->removeKey( taskID );
		return 0;
	}	

	// local variables
	JagClock clock;
	int rc;
	bool sucsync = true;
	JagParseAttribute jpa( servobj, req.session->timediff, servobj->servtimediff, req.session->dbname, servobj->_cfg );
	AbaxDataString reterr, rowFilter;
		
	if ( req.batchReply ) {
        abaxint callCounts = -1, lastBytes = 0;
        raydebug(stdout, JAG_LOG_HIGH, "s5033 newbatch availmem=%l M\n", 
				 availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES);
		clock.start();
		JagStrSplitWithQuote split( mesg, ';' );
		int len = split.length();
		if ( len >= 10000 ) {
			clock.stop();
			prt(("s5034 %lld splt %d type=%d batchReply=%d %d ms\n", 
				THREADID, len, req.session->replicateType, req.batchReply, clock.elapsed() )); 
		}

		if ( !redoOnly ) { // write related commands, store in wallog
			if ( req.batchReply ) {
				clock.start();
				JagDBServer::logCommand( req.session, mesg, msglen, 2 );
				clock.stop();
				servobj->_msyncCount += len;
				raydebug( stdout, JAG_LOG_HIGH, "msynch %d/%l %s in %l msecs\n",
					len, servobj->_msyncCount, servobj->_activeWalFpath.c_str(), clock.elapsed() );
			}
		}
		JagParseParam *pparam[len];
		for ( int i = 0; i < len; ++i ) {
			pparam[i] = newObject<JagParseParam>();
		}
		// batch insert, put in server _insertBuffer, and return ED without update map
		// for insert, lock fschema locks
		int numCPUs = servobj->_numCPUs;
		JagParallelParse pparser( numCPUs,  servobj->_parsePool );
		pparser.parse( jpa, split, pparam );
		if ( ! servobj->_isGate ) {
    		for ( int i = 0; i < len; ++i ) {
    			if ( pparam[i]->parseOK ) {
					// before do insert, need to check permission of this user for insert
					rc = checkUserCommandPermission( servobj, NULL, req, *(pparam[i]), 0, rowFilter, reterr );
					if ( rc ) {
						rc = doInsert( servobj, req, *(pparam[i]), reterr, split[i] );
    					if ( ! rc ) {
    						prt(("s3620 doInsert rc=%d reterr=[%s]\n", rc, reterr.c_str() ));
    					}
					} else {
						prt(("no permission for user [%s] to do insert on current table\n", req.session->uid.c_str()));
					}
    			} else {
    				prt(("s6349 parse error=%d i=%d split[i]=[%s]\n", pparam[i]->parseError, i, split[i].c_str() ));
    			}
    		}
		} else {
			// if is gate, do not store data locally
		}

		// sync command to other data-centers
		if ( req.dorep ) {
			JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
			for ( int i = 0; i < len; ++i ) {
				if ( JAG_SINSERT_OP == pparam[i]->opcode ) {
					servobj->synchToOtherDataCenters( split[i].c_str(), sucsync, req );
					// sinsert to insert
				} else {
					servobj->synchToOtherDataCenters( split[i].c_str(), sucsync, req );
				}	
			}
			jaguar_mutex_unlock ( &g_datacentermutex );
		}

		// check timestamp to see if need to update schema for client SC
		if ( !req.session->origserv && threadSchemaTime < g_lastSchemaTime ) {
			servobj->sendMapInfo( "_cschema", req );
			//servobj->sendMapInfo( "_cdefval", req );
			threadSchemaTime = g_lastSchemaTime;
		}

		// check timestamp to see if need to update host list for client HL
		if ( !req.session->origserv && threadHostTime < g_lastHostTime ) {
			prt(("s4083 sendHostInfo _chost\n" ));
			servobj->sendHostInfo( "_chost", req );
			threadHostTime = g_lastHostTime;
		}

		AbaxDataString endmsg = AbaxDataString("_END_[T=20|E=|]");
		if ( req.hasReply && !redoOnly ) {
			// prt(("s7736 sendMessageLength ED endmsg=[%s]\n", endmsg.c_str() ));
			JagTable::sendMessageLength( req, endmsg.c_str(), endmsg.length(), "ED" );
		} else {
			prt(("s7738 *** not sent sendMessageLength ED endmsg=[%s] req.hasReply=%d redoOnly=%d\n", 
				endmsg.c_str(), req.hasReply, redoOnly ));
		}

		for ( int i = 0; i < len; ++i ) {
			delete pparam[i];
		}
	} else {
		// other single cmd
		if ( ! servobj->_isGate ) {
    		if ( !redoOnly && isReadOrWriteCommand > 0 ) { // write related commands, store in wallog
    			if ( 0 == strncasecmp( mesg, "update", 6 ) || 0 == strncasecmp( mesg, "delete", 6 ) ) {
    				JagDBServer::logCommand( req.session, mesg, msglen, 1 );
    			} else if ( 0 == strncasecmp( mesg, "insert", 6 ) ) {
    				JagDBServer::logCommand( req.session, mesg, msglen, 2 );
    			} else {
    				JagDBServer::logCommand( req.session, mesg, msglen, 0 );
    			}
    
    		}
		}

		JagParser parser(servobj, NULL );
		JagParseParam pparam( &parser );
		// prt(("s4072 parseCommand mesg=[%s]\n", mesg ));
		if ( parser.parseCommand( jpa, mesg, &pparam, reterr ) ) {
			if ( JAG_SHOWSVER_OP == pparam.opcode ) {
				// show version cmd
				char brand[32];
				char hellobuf[128];
				sprintf(brand, JAG_BRAND );
				brand[0] = toupper( brand[0] );
				sprintf( hellobuf, "%s Server %s", brand, servobj->_version.c_str() );
				rc = JagTable::sendMessage( req, hellobuf, "OK");  // SG: Server Greeting
				servobj->_taskMap->removeKey( taskID );
				JagTable::sendMessage( req, "_END_[T=20|E=]", "ED");  // SG: Server Greeting
				return 1;
			} else {
				// other cmds
				// rc = JagDBServer::processCmd( req, servobj, mesg, pparam, reterr, threadQueryTime, threadSchemaTime );
				// prt(("s9304 done processCmd mesg=[%s] rc=%d\n", mesg, rc ));
			}

			// prt(("s02939 _isGate=%d\n", servobj->_isGate ));
			if ( servobj->_isGate ) {
					// prt(("pparam.opcode=%d\n", pparam.opcode ));
					if ( JAG_SELECT_OP == pparam.opcode || JAG_GETFILE_OP == pparam.opcode || JAG_COUNT_OP == pparam.opcode ||
						 JAG_INNERJOIN_OP == pparam.opcode || JAG_LEFTJOIN_OP == pparam.opcode || JAG_RIGHTJOIN_OP == pparam.opcode ||
						 JAG_FULLJOIN_OP == pparam.opcode ) {
						//prt(("s0281 jaguar_mutex_lock g_datacentermutex\n" ));
    					JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
						//prt(("s0282 synchFromOtherDataCenters mesg=[%s]\n", mesg ));
    					rc = servobj->synchFromOtherDataCenters( req, mesg, pparam );
						//prt(("s0282 synchFromOtherDataCenters mesg=[%s] done rc=%d\n", mesg, rc ));
    					jaguar_mutex_unlock ( &g_datacentermutex );						
						// JagTable::sendMessageLength( req, err.c_str(), err.length(), "OK" );
					} else {
						//prt(("s9302 doing processCmd mesg=[%s] ... \n", mesg ));
						rc = JagDBServer::processCmd( req, servobj, mesg, pparam, reterr, threadQueryTime, threadSchemaTime );
						//prt(("s9304 done processCmd mesg=[%s] rc=%d reterr=[%s]\n", mesg, rc, reterr.c_str() ));
						//prt(("s7391 reterr.size()=%d req.dorep=%d req.syncDataCenter=%d req.session->replicateType=%d\n",
								 // reterr.size(), req.dorep, req.syncDataCenter, req.session->replicateType ));
    					if ( reterr.size() < 1 && req.dorep && req.syncDataCenter && 0 == req.session->replicateType ) {
							//prt(("s0395 gate non-select op [%s] ...\n", mesg ));
    						JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
    						servobj->synchToOtherDataCenters( mesg, sucsync, req );
    						jaguar_mutex_unlock ( &g_datacentermutex );
    					}
					}
			} else {
				// prt(("s4081 processCmd mesg=[%s]\n", mesg ));
				rc = JagDBServer::processCmd( req, servobj, mesg, pparam, reterr, threadQueryTime, threadSchemaTime );
				raydebug( stdout, JAG_LOG_HIGH, "s9304 done [%s] rc=%d\n", mesg, rc );
    			if ( reterr.size()< 1 && req.dorep && req.syncDataCenter && 0 == req.session->replicateType ) {
					//raydebug( stdout, JAG_LOG_HIGH, "s0395 NON-gate non-select op ...\n" );
    				JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
    				servobj->synchToOtherDataCenters( mesg, sucsync, req );
    				jaguar_mutex_unlock ( &g_datacentermutex );
    			}
			}


			// send endmsg: -111 is X1, when select sent one record data, no more endmsg
			// send endmsg if not X1 ( one line select )
			if ( -111 != rc && req.hasReply && !redoOnly ) { 
				// prt(("s5029 rc=%d req.hasReply=%d redoOnly=%d mesg=[%s]\n", rc, req.hasReply, redoOnly, mesg ));
				
				// check timestamp to see if need to update schema for client SC
				if ( !req.session->origserv && threadSchemaTime < g_lastSchemaTime ) {
					// prt(("s0393 sendMapInfo _cschema ...\n" ));
					servobj->sendMapInfo( "_cschema", req );
					//servobj->sendMapInfo( "_cdefval", req );
					// prt(("s0393 sendMapInfo _cschema done ...\n" ));
					threadSchemaTime = g_lastSchemaTime;
				}
				// check timestamp to see if need to update host list for client HL
				if ( !req.session->origserv && threadHostTime < g_lastHostTime ) {
					// prt(("s0393 sendHostInfo _chost ...\n" ));
					servobj->sendHostInfo( "_chost", req );
					// prt(("s0393 sendHostInfo _chost done\n" ));
					threadHostTime = g_lastHostTime;
				}
				
				AbaxDataString resstr = "ED", endmsg = AbaxDataString("_END_[T=20|E=");
				if ( sucsync ) endmsg = AbaxDataString("_END_[T=777|E=");
				// prt(("s7721 reterr=[%s]\n", reterr.c_str() ));
				if ( reterr.length() > 0 ) {
					endmsg += reterr;
					resstr = "ER";
				}
				endmsg += AbaxDataString("|]");
				// prt(("s0293 sendMessageLength endmsg=[%s] ...\n",  endmsg.c_str() ));
				JagTable::sendMessageLength( req, endmsg.c_str(), endmsg.length(), resstr.c_str() );
				// prt(("s0239 sendMessageLength endmsg=[%s] done\n",  endmsg.c_str() ));
			} else {
				// prt(("s7378 no sendMessageLength req.hasReply=%d redoOnly=%d ************** \n", req.hasReply, redoOnly ));
				// AbaxDataString resstr = "ED", endmsg = AbaxDataString("_END_[T=20|E=");
				// JagTable::sendMessageLength( req, endmsg.c_str(), endmsg.length(), resstr.c_str() );
			}
		} else {
			//prt(("E8383 parse error mesg=[%s] reterr=[%s]\n", mesg, reterr.c_str() ));
			if ( req.hasReply && !redoOnly ) {
				// AbaxDataString resstr = "ER", endmsg = AbaxDataString("_END_[T=20|E=E3018 Syntax error: ");
				AbaxDataString resstr = "ER", endmsg = AbaxDataString("_END_[T=20|E=");
				// endmsg += AbaxDataString(mesg) + " ";
				endmsg += reterr + AbaxDataString("|]");
				// prt(("s02939 sendMessageLength endmsg=[%s] ...\n",  endmsg.c_str() ));
				JagTable::sendMessageLength( req, endmsg.c_str(), endmsg.length(), resstr.c_str() );
				// prt(("s02939 sendMessageLength endmsg=[%s] done\n",  endmsg.c_str() ));
			}
		}
	}
	
	// removeTask
	servobj->_taskMap->removeKey( taskID );
	
	return 1;
}

// static
// Process commands in one thread
abaxint JagDBServer::processCmd( JagRequest &req, JagDBServer *servobj, const char *cmd, 
								 JagParseParam &parseParam, AbaxDataString &reterr, abaxint threadQueryTime, 
								 abaxint &threadSchemaTime )
{
	// prt(("s4839 processCmd cmd=[%s] reterr=[%s] parseParam.opcode=%d ...\n", cmd, reterr.c_str(), parseParam.opcode ));
	reterr = "";
	if ( JAG_SELECT_OP != parseParam.opcode ) {
		servobj->_dbLogger->logmsg( req, "DML", cmd );
	}

	bool rc;	
	abaxint cnt = 0;
	int insertCode = 0;
	JagCfg *cfg = servobj->_cfg;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	// prt(("s3392 got getTableIndexSchema\n" ));
	AbaxDataString errmsg, dbtable, rowFilter; 
	AbaxDataString dbname = makeLowerString(req.session->dbname);
	JagIndex *pindex = NULL;
	JagTable *ptab = NULL;

	// prt(("s3801 parseParam.opcode=[%d]\n", parseParam.opcode ));
	// change default dbname for single cmd
	if ( parseParam.objectVec.size() == 1 ) { dbname = parseParam.objectVec[0].dbName; }
	req.opcode = parseParam.opcode;

	// debug only
	/***
	for ( int i=0; i < parseParam.objectVec.size() ; ++i ) {
		prt(("s3029 i=%d dbname=[%s] tableName=[%s] indexName=[%s] colName=[%s]\n", i,
			parseParam.objectVec[i].dbName.c_str(),
			parseParam.objectVec[i].tableName.c_str(),
			parseParam.objectVec[i].indexName.c_str(),
			parseParam.objectVec[i].colName.c_str()
			));
	}
	***/
	
	// methods of frequent use
	if ( JAG_SELECT_OP == parseParam.opcode || JAG_INSERTSELECT_OP == parseParam.opcode 
		 || JAG_GETFILE_OP == parseParam.opcode ) {
		AbaxDataString dbidx, tabName, idxName; 
		JagDataAggregate *jda = NULL; int pos = 0;
			if ( JAG_INSERTSELECT_OP == parseParam.opcode && parseParam.objectVec.size() > 1 ) {
				// insert into ... select ... from syntax, select part as objectVec[1]
				pos = 1;
			}

			if ( parseParam.objectVec[pos].indexName.length() > 0 ) {
				// known index
				pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].dbName, 
							tabName, parseParam.objectVec[pos].indexName,
					tableschema, indexschema, req.session->replicateType, 0 );
					dbidx = parseParam.objectVec[pos].dbName;
					idxName = parseParam.objectVec[pos].indexName;
			} else {
				// table object or index object
				ptab = servobj->_objectLock->readLockTable( parseParam.opcode, parseParam.objectVec[pos].dbName, 
							parseParam.objectVec[pos].tableName, req.session->replicateType, 0 );
				// prt(("8829 readLockTable ptab=%0x\n", ptab ));
				if ( !ptab ) {
					pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].dbName, 
								tabName, parseParam.objectVec[pos].tableName,
						tableschema, indexschema, req.session->replicateType, 0 );
						dbidx = parseParam.objectVec[pos].dbName;
						idxName = parseParam.objectVec[pos].tableName;
					if ( !pindex ) {
						pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].colName, 
									tabName, parseParam.objectVec[pos].tableName,
							tableschema, indexschema, req.session->replicateType, 0 );
						dbidx = parseParam.objectVec[pos].colName;
						idxName = parseParam.objectVec[pos].tableName;
					}
				}
			}
			
			if ( !ptab && !pindex ) {
				if ( parseParam.objectVec[pos].colName.length() > 0 ) {
					reterr = AbaxDataString("E1120 Unable to select for ") + 
								parseParam.objectVec[pos].colName + "." + parseParam.objectVec[pos].tableName +
								" session.dbname=[" + req.session->dbname + "]"; 
				} else {
					reterr = AbaxDataString("E1121 Unable to select for ") + 
							parseParam.objectVec[pos].dbName + "." + parseParam.objectVec[pos].tableName;
				}
				if ( parseParam.objectVec[pos].indexName.size() > 0 ) {
					reterr += AbaxDataString(".") + parseParam.objectVec[pos].indexName;
				}
				//prt(("s3009 reterr=[%s]\n", reterr.c_str() ));
				return 0;
			}
		
			if ( ptab ) {
				// table to select ...
				rc = checkUserCommandPermission( servobj, &ptab->_tableRecord, req, parseParam, 0, rowFilter, errmsg );
				//prt(("s3044 checkUserCommandPermission parseParam.opcode=%d rc=%d\n", parseParam.opcode, rc ));
				if ( rc ) {
					AbaxDataString newcmd;
					if ( rowFilter.size() > 0 ) {
						parseParam.resetSelectWhere( rowFilter );
						//prt(("s4031 parseParam.selectWhereCluase=[%s]\n", parseParam.selectWhereClause.c_str() ));
						parseParam.setSelectWhere();
						newcmd = parseParam.formSelectSQL();
						cmd = newcmd.c_str();
						//prt(("s4708 formSelectSQL cmd=[%s]\n", cmd ));
					} else {
						//prt(("s3208 rowFilter.size <= 0 \n" ));
					}
					//prt(("s4820 ptab->select cmd=[%s]\n", cmd ));
					cnt = ptab->select( jda, cmd, req, &parseParam, errmsg, true, pos );
					//prt(("s2873 ptab->select cnt=%d\n", cnt ));
				} else {
					cnt = -1;
				}

				servobj->_objectLock->readUnlockTable( parseParam.opcode, parseParam.objectVec[pos].dbName, 
					parseParam.objectVec[pos].tableName, req.session->replicateType, 0 );
					// prt(("select cnt=%d cmd=[%s]\n", cnt, cmd ));
			} else if ( pindex ) {
				rc = checkUserCommandPermission( servobj, &pindex->_indexRecord, req, parseParam, 0, rowFilter, errmsg );
				//prt(("s3044 checkUserCommandPermission parseParam.opcode=%d rc=%d\n", parseParam.opcode, rc ));
				if ( rc ) {
					cnt = pindex->select( jda, cmd, req, &parseParam, errmsg, true, pos );
				} else {
					cnt = -1;
				}

				servobj->_objectLock->readUnlockIndex( parseParam.opcode, dbidx, tabName, idxName,
					tableschema, indexschema, req.session->replicateType, 0 );
			}

			if ( cnt == -1 ) {
				reterr = errmsg;
			}

			if ( -111 == cnt || cnt > 0 ) {
				++ servobj->numSelects;
			}

			raydebug( stdout, JAG_LOG_HIGH, "s2239 cnt=%d req.session->sessionBroken=%d\n", cnt, (int)req.session->sessionBroken );
			//raydebug( stdout, JAG_LOG_LOW, "s2239 cnt=%d req.session->sessionBroken=%d\n", cnt, (int)req.session->sessionBroken );
			//prt(("s3744 parseParam->opcode=%d\n", parseParam.opcode ));
			// prt(("s3744 parseParam->hasCountAll=%d cnt=%d\n", parseParam.hasCountAll, cnt ));
			if (  cnt > 0 && !req.session->sessionBroken ) {
				if ( jda ) {
					// in processcmd send data back to client
					//prt(("s8284 send regular data ...\n" ));
					jda->sendDataToClient( cnt, req );
				}
			}	

			if ( !req.session->sessionBroken && parseParam._lineFile && parseParam._lineFile->size() > 0 ) {
				prt(("s8283 send linefile string ...\n" ));
				sendValueData( parseParam, req  );
			} 

			if ( parseParam.exportType == JAG_EXPORT ) req.syncDataCenter = true;
			if ( jda ) { delete jda; jda = NULL; }

	} else if ( JAG_UPDATE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			ptab = servobj->_objectLock->readLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
				req.session->replicateType, 0 );
			if ( !ptab ) {
				reterr = "E4283 Update can only been applied to tables";
			} else {
				AbaxDataString dbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
				servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
				cnt = ptab->update( req, &parseParam, errmsg, insertCode );
				servobj->_objectLock->readUnlockTable( parseParam.opcode, dbname, 
					parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
				servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
			}
	
			// prt(("s3389 JAG_UPDATE_OP cnt=%d errmsg=[%s]\n", cnt, errmsg.c_str() ));
			if ( ! servobj->_isGate ) {
    			if ( 0 == cnt && reterr.size() < 1 ) {
    				if ( servobj->_totalClusterNumber > 1 && servobj->_curClusterNumber < (servobj->_totalClusterNumber -1) ) {
    					reterr = "E3028 update error";
    				} else {
    					reterr = "E3208 update error";
    					// prt(("s0393 logerr [%s]\n", reterr.c_str() ));
    					servobj->_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else if ( cnt < 0 ) {
    				if ( servobj->_totalClusterNumber > 1 && servobj->_curClusterNumber < (servobj->_totalClusterNumber -1) ) {
    					reterr = errmsg;
    				} else {
    					reterr = errmsg;
    					// prt(("s0394 logerr [%s]\n", reterr.c_str() ));
    					servobj->_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else {
    				reterr= "";
    				++ servobj->numUpdates;
    				// prt(("s2229 numUpdates=%d\n", (abaxint)servobj->numUpdates ));
    			}
			} else {
    			reterr= "";
			}
			cnt = 100;  // temp fix
			req.syncDataCenter = true;
		} else {
			// no permission for update
		}
	} else if ( JAG_DELETE_OP == parseParam.opcode ) {
		// prt(("s3088 JAG_DELETE_OP checkUserCommandPermission ...\n" ));
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		// prt(("s3088 JAG_DELETE_OP checkUserCommandPermission rc=%d\n", rc ));
		if ( rc ) {
			ptab = servobj->_objectLock->readLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
				req.session->replicateType, 0 );
			if ( !ptab ) {
				reterr = "E0238 Delete can only be applied to tables";
			} else {
                AbaxDataString dbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
                servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
                cnt = ptab->remove( req, &parseParam, errmsg );
                servobj->_objectLock->readUnlockTable( parseParam.opcode, dbname, 
					parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
				servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
			}

			if ( ! servobj->_isGate ) {
    			if ( 0 == cnt && reterr.size() < 1 ) {
    				if ( servobj->_totalClusterNumber > 1 && servobj->_curClusterNumber < (servobj->_totalClusterNumber -1) ) {
    					reterr = "E3772 delete error";
    				} else {
    					reterr = "E3773 delete error";
    					servobj->_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else if ( cnt < 0 ) {
    				if ( servobj->_totalClusterNumber > 1 && servobj->_curClusterNumber < (servobj->_totalClusterNumber -1) ) {
    					reterr = errmsg;
    				} else {
    					reterr = errmsg;
    					servobj->_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else {
    				reterr= "";
    				++ servobj->numDeletes;
    			}
			} else {
    			reterr= "";
			}
			cnt = 100;  // temp fix
			req.syncDataCenter = true;
		} else {
			// no permission for delete
			// prt(("s7333 invalid update\n" ));
			cnt = 100;  // temp fix
		}
	} else if ( JAG_COUNT_OP == parseParam.opcode ) {
		AbaxDataString dbidx, tabName, idxName; abaxint keyCheckerCnt = 0;
		if ( parseParam.objectVec[0].indexName.length() > 0 ) {
			// known index
			pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, dbname, tabName, parseParam.objectVec[0].indexName,
					tableschema, indexschema, req.session->replicateType, 0 );
			dbidx = dbname;
			idxName = parseParam.objectVec[0].indexName;
		} else {
				// table object or index object
				ptab = servobj->_objectLock->readLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
					req.session->replicateType, 0 );
				if ( !ptab ) {
					pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, dbname, tabName, parseParam.objectVec[0].tableName,
						tableschema, indexschema, req.session->replicateType, 0 );
					dbidx = dbname;
					idxName = parseParam.objectVec[0].tableName;
					if ( !pindex ) {
						pindex = servobj->_objectLock->readLockIndex( parseParam.opcode, 
									parseParam.objectVec[0].colName, tabName, parseParam.objectVec[0].tableName,
							tableschema, indexschema, req.session->replicateType, 0 );
						dbidx = parseParam.objectVec[0].colName;
						idxName = parseParam.objectVec[0].tableName;
					}
				}
		}
			
		if ( !ptab && !pindex ) {
				if ( parseParam.objectVec[0].colName.length() > 0 ) {
					reterr = AbaxDataString("E1023 Unable to select count(*) for ") + 
							 parseParam.objectVec[0].colName + "." + parseParam.objectVec[0].tableName;
				} else {
					reterr = AbaxDataString("E1024  Unable to select count(*) for ") + 
							 parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
				}
				if ( parseParam.objectVec[0].indexName.size() > 0 ) {
					reterr += AbaxDataString(".") + parseParam.objectVec[0].indexName;
				}
				return 0;
		}
		
		if ( ptab ) {
				rc = checkUserCommandPermission( servobj, &ptab->_tableRecord, req, parseParam, 0, rowFilter, errmsg );
				if ( rc ) {
					cnt = ptab->count( cmd, req, &parseParam, errmsg, keyCheckerCnt );
				} else {
					cnt = -1;
				}
				servobj->_objectLock->readUnlockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName, 
														req.session->replicateType, 0 );
		} else if ( pindex ) {
				rc = checkUserCommandPermission( servobj, &pindex->_indexRecord, req, parseParam, 0, rowFilter, errmsg );
				if ( rc ) {
					cnt = pindex->count( cmd, req, &parseParam, errmsg, keyCheckerCnt );
				} else {
					cnt = -1;
				}
				servobj->_objectLock->readUnlockIndex( parseParam.opcode, dbidx, tabName, idxName,
					tableschema, indexschema, req.session->replicateType, 0 );
		}

		if ( cnt == -1 ) {
			reterr = errmsg;
		} else {
			char cntbuf[30];
			memset( cntbuf, 0, 30 );
			sprintf( cntbuf, "%lld", cnt );
			JagTable::sendMessage( req, cntbuf, "OK" );
		}
		
		if ( cnt >= 0 ) {
			prt(("selectcount type=%d %s keychecker=%lld diskarray=%lld\n", 
				req.session->replicateType, parseParam.objectVec[0].tableName.c_str(), keyCheckerCnt, cnt));
		}
			
		// for testing use only, no need for production
		servobj->objectCountTesting( parseParam );
	} else if ( isJoin( parseParam.opcode ) ) {
		if ( !strcasestrskipquote( cmd, "count(*)" ) ) {
			rc = joinObjects( req, servobj, &parseParam, reterr );
		}
	// methods to change schema or userid
   	} else if ( JAG_IMPORT_OP == parseParam.opcode ) {
		importTable( req, servobj, dbname, &parseParam, reterr );
	} else if ( JAG_CREATEUSER_OP == parseParam.opcode ) {
		createUser( req, servobj, parseParam, threadQueryTime );
	} else if ( JAG_DROPUSER_OP == parseParam.opcode ) {
		dropUser( req, servobj, parseParam, threadQueryTime );
	} else if ( JAG_CHANGEPASS_OP == parseParam.opcode ) {
		changePass( req, servobj, parseParam, threadQueryTime );		
	} else if ( JAG_CHANGEDB_OP == parseParam.opcode ) {
		changeDB( req, servobj, parseParam, threadQueryTime );
	} else if ( JAG_CREATEDB_OP == parseParam.opcode ) {
		createDB( req, servobj, parseParam, threadQueryTime );
	} else if ( JAG_DROPDB_OP == parseParam.opcode ) {
		dropDB( req, servobj, parseParam, threadQueryTime );
	} else if ( JAG_CREATETABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = createTable( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
		else noGood( req, servobj, parseParam );
	} else if ( JAG_CREATECHAIN_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = createTable( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
		else noGood( req, servobj, parseParam );
	} else if ( JAG_CREATEMEMTABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = createMemTable( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
		else noGood( req, servobj, parseParam );
	} else if ( JAG_CREATEINDEX_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = createIndex( req, servobj, dbname, &parseParam, ptab, pindex, reterr, threadQueryTime );		
		else noGood( req, servobj, parseParam );
	} else if ( JAG_ALTER_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = renameColumn( req, servobj, dbname, &parseParam, reterr, threadQueryTime, threadSchemaTime );	
		else noGood( req, servobj, parseParam );
   	} else if ( JAG_DROPTABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = dropTable( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
		else noGood( req, servobj, parseParam );
   	} else if ( JAG_DROPINDEX_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = dropIndex( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
   	} else if ( JAG_TRUNCATE_OP == parseParam.opcode ) {
		//prt(("s3938 JAG_TRUNCATE_OP\n" ));
		rc = checkUserCommandPermission( servobj, NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = truncateTable( req, servobj, dbname, &parseParam, reterr, threadQueryTime );
		else noGood( req, servobj, parseParam );
   	} else if ( JAG_GRANT_OP == parseParam.opcode ) {
		/**
		prt(("s3044 JAG_GRANT_OP perm=[%s] obj=[%s] user=[%s] where=[%s] ..\n", 
				parseParam.grantPerm.c_str(), parseParam.grantObj.c_str(), parseParam.grantUser.c_str(), parseParam.grantWhere.c_str()  ));
		**/
		grantPerm( req, servobj, parseParam, threadQueryTime );
   	} else if ( JAG_REVOKE_OP == parseParam.opcode ) {
		// prt(("s3049 JAG_REVOKE_OP ..\n" ));
		revokePerm( req, servobj, parseParam, threadQueryTime );
   	} else if ( JAG_SHOWGRANT_OP == parseParam.opcode ) {
		// prt(("s3059 JAG_SHOWGRANT_OP ..\n" ));
		showPerm( req, servobj, parseParam, threadQueryTime );
		// methods to describe/show info
	} else if ( JAG_DESCRIBE_OP == parseParam.opcode ) {
		/***
		prt(("s2291 JAG_DESCRIBE_OP parseParam.objectVec[0].indexName=[%s]\n",
			 parseParam.objectVec[0].indexName.c_str() ));
		 ***/

		if ( parseParam.objectVec[0].indexName.length() > 0 ) {
			describeIndex( parseParam, req, servobj, indexschema, parseParam.objectVec[0].dbName, 
							parseParam.objectVec[0].indexName, reterr );
		} else {
			AbaxDataString dbtable = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
			// prt(("s0293 dbtable=[%s]\n", dbtable.c_str() ));
			if ( tableschema->existAttr ( dbtable ) ) {
				describeTable( JAG_ANY_TYPE, req, servobj, ptab, tableschema, dbtable, parseParam );
			} else {
				/***
				prt(("s2200 parseParam.objectVec[0].dbName=[%s]\n", parseParam.objectVec[0].dbName.c_str() ));
				prt(("s2200 parseParam.objectVec[0].colName=[%s]\n", parseParam.objectVec[0].colName.c_str() ));
				prt(("s2201 parseParam.objectVec[0].tableName=[%s]\n", parseParam.objectVec[0].tableName.c_str() ));
				***/
				if ( parseParam.objectVec[0].colName.length() > 0 ) {
					// prt(("s2029 desc index [%s]\n", parseParam.objectVec[0].colName.c_str() ));
					describeIndex( parseParam, req, servobj, indexschema, parseParam.objectVec[0].colName, 
								   parseParam.objectVec[0].tableName, reterr );
				} else {
					// prt(("s2029 desc index [%s]\n", parseParam.objectVec[0].tableName.c_str() ));
					describeIndex( parseParam, req, servobj, indexschema, parseParam.objectVec[0].dbName, 
								   parseParam.objectVec[0].tableName, reterr );
			    }
			}
		}
	} else if ( JAG_SHOW_CREATE_TABLE_OP == parseParam.opcode ) {
		AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			describeTable( JAG_TABLE_TYPE, req, servobj, ptab, tableschema, dbtable, parseParam, 1 );
		} else {
			reterr = "Table " + dbtable + " does not exist";
		}
	} else if ( JAG_SHOW_CREATE_CHAIN_OP == parseParam.opcode ) {
		AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			describeTable( JAG_CHAINTABLE_TYPE, req, servobj, ptab, tableschema, dbtable, parseParam, 1 );
		} else {
			reterr = "Chain " + dbtable + " does not exist";
		}
	} else if ( JAG_EXEC_DESC_OP == parseParam.opcode ) {
		AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			_describeTable( req, ptab, servobj, dbtable, 0 );
		} else {
			reterr = "Table " + dbtable + " does not exist";
		}
	} else if ( JAG_EXEC_PKEY_OP == parseParam.opcode ) {
		AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			_describeTable( req, ptab, servobj, dbtable, 1 );
		} else {
			reterr = "Table " + dbtable + " does not exist";
		}
	} else if ( JAG_SHOWUSER_OP == parseParam.opcode ) {
		showUsers( req, servobj );
	} else if ( JAG_SHOWDB_OP == parseParam.opcode ) {
		showDatabases( cfg, req );
	} else if ( JAG_CURRENTDB_OP == parseParam.opcode ) {
		showCurrentDatabase( cfg, req, dbname );
	} else if ( JAG_SHOWSTATUS_OP == parseParam.opcode ) {
		showClusterStatus( req, servobj );
	} else if ( JAG_SHOWDATACENTER_OP == parseParam.opcode ) {
		showDatacenter( req, servobj );
	} else if ( JAG_SHOWTOOLS_OP == parseParam.opcode ) {
		showTools( req, servobj );
	} else if ( JAG_CURRENTUSER_OP == parseParam.opcode ) {
		showCurrentUser( cfg, req );
	} else if ( JAG_SHOWTABLE_OP == parseParam.opcode ) {
		showTables( req, parseParam, tableschema, dbname, JAG_TABLE_TYPE );
	} else if ( JAG_SHOWCHAIN_OP == parseParam.opcode ) {
		showTables( req, parseParam, tableschema, dbname, JAG_CHAINTABLE_TYPE );
	} else if ( JAG_SHOWINDEX_OP == parseParam.opcode ) {
		if ( parseParam.objectVec.size() < 1 ) {
			showAllIndexes( req, parseParam, indexschema, dbname );
		} else {
			AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
			showIndexes( req, parseParam, indexschema, dbtable );
		}
	} else if ( JAG_SHOWTASK_OP == parseParam.opcode ) {
		showTask( req, servobj );
	} else if ( JAG_EXEC_SHOWDB_OP == parseParam.opcode ) {
		_showDatabases( cfg, req );
	} else if ( JAG_EXEC_SHOWTABLE_OP == parseParam.opcode ) {
		_showTables( req, tableschema, dbname );
	} else if ( JAG_EXEC_SHOWINDEX_OP == parseParam.opcode ) {
		AbaxDataString dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		_showIndexes( req, indexschema, dbtable );
	}

	// printf("s3930 return cnt=%d in processCmd\n", cnt );
	parseParam.clearRowHash();
	return cnt;
}


#ifndef SIGHUP
#define SIGHUP 1
#endif
void JagDBServer::sig_hup(int sig)
{
	// printf("sig_hup called, should reread cfg...\n");
	// raydebug( stdout, JAG_LOG_LOW, "Server received SIGHUP, cfg and userdb refresh ...\n");
	// JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	g_receivedSignal = SIGHUP;
	/***
	if ( _cfg ) {
		_cfg->refresh();
	}

	if ( _userDB ) {
		_userDB->refresh();
	}
	if ( _prevuserDB ) {
		_prevuserDB->refresh();
	}
	if ( _nextuserDB ) {
		_nextuserDB->refresh();
	}
	***/
	// raydebug( stdout, JAG_LOG_LOW, "Server received SIGHUP, cfg/userDB refresh done\n");
	raydebug( stdout, JAG_LOG_LOW, "Server received SIGHUP, ignored.\n");
	// jaguar_mutex_unlock ( &g_dbmutex );
}

// method to check non standard command is valid or not
// may append more commands later
// return 0: for error
int JagDBServer::checkNonStandardCommandValidation( const char *mesg )
{	
	if ( 0 == strncmp( mesg, "_noop", 5 ) ) return JAG_SCMD_NOOP;
	else if ( 0 == strncmp( mesg, "_cschema", 8 ) ) return JAG_SCMD_CSCHEMA;
	//else if ( 0 == strncmp( mesg, "_cdefval", 8 ) ) return JAG_SCMD_CDEFVAL;
	else if ( 0 == strncmp( mesg, "_chost", 6 ) ) return JAG_SCMD_CHOST;
	else if ( 0 == strncmp( mesg, "_serv_crecover", 14 ) ) return JAG_SCMD_CRECOVER;
	else if ( 0 == strncmp( mesg, "_serv_checkdelta", 16 ) ) return JAG_SCMD_CHECKDELTA;
	else if ( 0 == strncmp( mesg, "_serv_beginftransfer", 20) ) return JAG_SCMD_BFILETRANSFER;
	else if ( 0 == strncmp( mesg, "_serv_addbeginftransfer", 23) ) return JAG_SCMD_ABFILETRANSFER;
	else if ( 0 == strncmp( mesg, "_serv_reqdatajoin", 17 ) ) return JAG_SCMD_JOINDATAREQUEST;
	else if ( 0 == strncmp( mesg, "_serv_opinfo", 12 ) ) return JAG_SCMD_OPINFO;
	else if ( 0 == strncmp( mesg, "_serv_copydata", 14 ) ) return JAG_SCMD_COPYDATA;
	else if ( 0 == strncmp( mesg, "_serv_dolocalbackup", 19 ) ) return JAG_SCMD_DOLOCALBACKUP;
	else if ( 0 == strncmp( mesg, "_serv_doremotebackup", 20 ) ) return JAG_SCMD_DOREMOTEBACKUP;
	else if ( 0 == strncmp( mesg, "_serv_dorestoreremote", 21 ) ) return JAG_SCMD_DORESTOREREMOTE;
	else if ( 0 == strncmp( mesg, "_serv_refreshacl", 14 ) ) return JAG_SCMD_REFRESHACL;
	else if ( 0 == strncmp( mesg, "_serv_reqschemafromdc", 21 ) ) return JAG_SCMD_REQSCHEMAFROMDC;
	else if ( 0 == strncmp( mesg, "_serv_unpackschinfo", 19 ) ) return JAG_SCMD_UNPACKSCHINFO;
	else if ( 0 == strncmp( mesg, "_serv_askdatafromdc", 19 ) ) return JAG_SCMD_ASKDATAFROMDC;
	else if ( 0 == strncmp( mesg, "_serv_preparedatafromdc", 23 ) ) return JAG_SCMD_PREPAREDATAFROMDC;
	else if ( 0 == strncmp( mesg, "_mon_dbtab", 10 ) ) return JAG_SCMD_MONDBTAB;
	else if ( 0 == strncmp( mesg, "_mon_info", 9 ) ) return JAG_SCMD_MONINFO;
	else if ( 0 == strncmp( mesg, "_mon_rsinfo", 11 ) ) return JAG_SCMD_MONRSINFO;
	else if ( 0 == strncmp( mesg, "_mon_clusteropinfo", 18 ) ) return JAG_SCMD_MONCLUSTERINFO;
	else if ( 0 == strncmp( mesg, "_mon_hosts", 10 ) ) return JAG_SCMD_MONHOSTS;
	else if ( 0 == strncmp( mesg, "_mon_remote_backuphosts", 23 ) ) return JAG_SCMD_MONBACKUPHOSTS;
	else if ( 0 == strncmp( mesg, "_mon_local_stat6", 16 ) ) return JAG_SCMD_MONLOCALSTAT;
	else if ( 0 == strncmp( mesg, "_mon_cluster_stat6", 18 ) ) return JAG_SCMD_MONCLUSTERSTAT;
	else if ( 0 == strncmp( mesg, "_ex_proclocalbackup", 19 ) ) return JAG_SCMD_EXPROCLOCALBACKUP;
	else if ( 0 == strncmp( mesg, "_ex_procremotebackup", 20 ) ) return JAG_SCMD_EXPROCREMOTEBACKUP;
	else if ( 0 == strncmp( mesg, "_ex_restorefromremote", 21 ) ) return JAG_SCMD_EXRESTOREFROMREMOTE;
	else if ( 0 == strncmp( mesg, "_ex_repairocheck", 16 ) ) return JAG_SCMD_EXREPAIRCHECK;
	else if ( 0 == strncmp( mesg, "_ex_repairobject", 16 ) ) return JAG_SCMD_EXREPAIROBJECT;
	else if ( 0 == strncmp( mesg, "_ex_addcluster", 14 ) ) return JAG_SCMD_EXADDCLUSTER;
	else if ( 0 == strncmp( mesg, "_ex_shutdown", 12 ) ) return JAG_SCMD_EXSHUTDOWN;
	else if ( 0 == strncmp( mesg, "_getpubkey", 10 ) ) return JAG_SCMD_GETPUBKEY;
	// more commands to be added
	else return 0;
}

// method to check is simple command or not
// may append more commands later
// returns 0 for false (not simple commands)
int JagDBServer::checkSimpleCommandValidation( const char *mesg )
{	
	if ( 0 == strncmp( mesg, "?", 1 ) || 0 == strncmp( mesg, "help", 4 )  ) return JAG_RCMD_HELP;
	else if ( 0 == strncmp( mesg, "use ", 4 ) ) return JAG_RCMD_USE;
	else if ( 0 == strncmp( mesg, "auth", 4 ) ) return JAG_RCMD_AUTH;
	else if ( 0 == strncmp( mesg, "quit", 4 ) ) return JAG_RCMD_QUIT;
	else if ( 0 == strncmp( mesg, "hello", 5 ) ) return JAG_RCMD_HELLO;
	// more commands to be added
	else return 0;
}

// static Task for a client/thread
void *JagDBServer::oneClientThreadTask( void *passptr )
{
	pthread_detach ( pthread_self() );
 	JagPass *ptr = (JagPass*)passptr;
	JagSession session( g_dtimeout );
 	JAGSOCK sock = ptr->sock;
	JagDBServer  *servobj = (JagDBServer*)ptr->servobj;
	session.sock = sock;
	session.servobj = servobj;
	session.ip = ptr->ip;

	raydebug( stdout, JAG_LOG_HIGH, "Client IP: %s\n", session.ip.c_str() );

 	int 	rc, simplerc, breakcode;
 	char  	save, *pmesg, *saveptr, *tok, *p;
	char    brand[32];
	char	hellobuf[128];
	abaxint 	threadSchemaTime = 0, threadHostTime = g_lastHostTime, threadQueryTime = 0, len, len2, clen;
	abaxint cnt = 1;

 	int  authed = 0;
 
	sprintf(brand, JAG_BRAND );
	brand[0] = toupper( brand[0] );

	// signal( SIGHUP, SIG_IGN ); 
	// signal( SIGUSR1, catch_sig_usr1 );

	// prt(("s9359 %lld server: in thread sock=%d passptr=%0x ptr=%0x session=%0x...\n", pthread_self(), sock, passptr, ptr, &session ));
	#ifdef CHECK_LATENCY
		JagClock clock; 
	#endif

   	// pthread_t  threadmo;
	session.active = 1;
	++ servobj->_activeClients; 
	// prt(("s4480 client enters, _activeClients=%d\n", (abaxint)servobj->_activeClients ));

	char rephdr[4];
	rephdr[3] = '\0';
 	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
 	char hdr2[JAG_SOCK_MSG_HDR_LEN+1];
	char *newbuf = NULL;
	char *newbuf2 = NULL;
    for(;;)
    {
		if ( ( cnt % 100000 ) == 0 ) { jagmalloc_trim( 0 ); }

		// get new command and uncompress it if needed
		JagRequest req;
		req.session = &session;
		memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
		
		// prt(("s3288 recvData from %s ...\n", session.ip.c_str() ));
		len = recvData( sock, hdr, newbuf );
		//prt(("s9310 recv() pid=%lld sock=%d len=%d hdr=[%s] newbuf=[%.200s] ...\n", pthread_self(), sock, len, hdr, newbuf ));
		if ( len <= 0 ) {
			if ( session.uid == "admin" ) {
				if ( session.exclusiveLogin ) {
					servobj->_exclusiveAdmins = 0;
					raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", session.ip.c_str() );
				}
			}

			if ( newbuf ) { free( newbuf ); }
			if ( newbuf2 ) { free( newbuf2 ); }
			breakcode = 1;
			-- servobj->_connections;

			if ( ! session.origserv && 0 == session.replicateType ) {
				raydebug( stdout, JAG_LOG_LOW, "user %s disconnected from %s\n", session.uid.c_str(), session.ip.c_str() );
				if ( session.uid == "admin" && session.datacenter ) {
					raydebug( stdout, JAG_LOG_LOW, "datacenter admin disconnected from %s\n", session.ip.c_str() );
					// servobj->reconnectDataCenter( session.ip );
				}
			}
			break;
		}
		++cnt;
		struct timeval now;
		gettimeofday( &now, NULL );
		threadQueryTime = now.tv_sec * (abaxint)1000000 + now.tv_usec;

		// if recv hard bit, ignore
		if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) {
			continue;
		}

		if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'N' ) {
			req.hasReply = false;
			req.batchReply = false;
		} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'B' ) {
			req.hasReply = true;
			req.batchReply = true;
		} else {
			req.hasReply = true;
			req.batchReply = false;
		}

		if ( hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'Z' ) {
			req.doCompress = true;
		} else {
			req.doCompress = false;
		}

		/***
		if ( hdr[JAG_SOCK_MSG_HDR_LEN-1] == 'N' ) {
			req.norep = true;
			// prt(("s7738 hdr=[%s] req.norep = true\n", hdr ));
		} else {
			req.norep = false;
			// prt(("s7739 hdr=[%s] req.norep = false\n", hdr ));
		}
		***/
		// if from GATE to HOST, then req.norep = true; ie, req.dorep = false
		// if ( JAG_DATACENTER_GATE == req.session->dcfrom && JAG_DATACENTER_HOST == JAG_DATACENTER_GATE == req.session->dcto ) {
		// prt(("s2038 req.session->dcfrom=%d req.session->dcto=%d\n", req.session->dcfrom, req.session->dcto ));
		if ( JAG_DATACENTER_GATE == req.session->dcfrom && JAG_DATACENTER_HOST == req.session->dcto ) {
			req.dorep = false;
			// prt(("s7736 hdr=[%s] req.dorep = false GATE->HOST\n", hdr ));
		} else if ( JAG_DATACENTER_GATE == req.session->dcfrom && JAG_DATACENTER_PGATE == req.session->dcto ) {
			req.dorep = false;
			// prt(("s7738 hdr=[%s] req.dorep = false GATE->PGATE\n", hdr ));
		} else {
			req.dorep = true;
			// prt(("s7739 hdr=[%s] req.dorep = true GATE->GATE/HOST->*\n", hdr ));
		}

		pmesg = newbuf;
		AbaxDataString us;
		if ( req.doCompress ) {
			JagFastCompress::uncompress( newbuf, len, us );
			pmesg = (char*)us.c_str();
			len = us.length();
		}
	
		if ( 0 ) {
		    prt(("s3828 %lld server: fromip=[%s] recover=%d replicateType=%d sock=%d msg=(%.200s) batchReply=%d len=%d\n", 
	  	        THREADID, session.ip.c_str(), session.drecoverConn, session.replicateType, sock, pmesg, req.batchReply, len ));
		}

		while ( *pmesg == ' ' ) ++pmesg;
		if ( strncmp(pmesg, "auth", 4 ) && *pmesg != '_' ) {
			raydebug( stdout, JAG_LOG_HIGH, "%s\n", pmesg );
		}

		simplerc = checkSimpleCommandValidation( pmesg );
		
		// begin process message
		// first, check and process non standard server command
		if ( *pmesg == '_' && 0 != strncmp( pmesg, "_show", 5 ) 
		     && 0 != strncmp( pmesg, "_desc", 5 ) 
		     && 0 != strncmp( pmesg, "_pkey", 5 ) 
			 ) {
			// non standard message, generated by program
			rc = checkNonStandardCommandValidation( pmesg );
			if ( !rc ) {
				prt(("E7845 invalid non standard server command [%s]\n", pmesg));
				JagTable::sendMessage( req, "_END_[T=10|E=Error non standard server command]", "ER" );			
			} else {
				if ( ! authed && JAG_SCMD_GETPUBKEY != rc ) {
					// printf("s8003 %lld user not authed close\n", pthread_self() );
					JagTable::sendMessage( req, "_END_[T=20|E=Not authed before requesting query]", "ER" );
					breakcode = 5;
					break;
				}
				if ( JAG_SCMD_NOOP == rc ) {
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_CSCHEMA == rc ) {
					servobj->sendMapInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
					/***
				} else if ( JAG_SCMD_CDEFVAL == rc ) {
					servobj->sendMapInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
					***/
				} else if ( JAG_SCMD_CHOST == rc ) {
					servobj->sendHostInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_CRECOVER == rc ) {
					servobj->cleanRecovery( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_CHECKDELTA == rc ) {
					servobj->checkDeltaFiles( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_BFILETRANSFER == rc ) {
					JagStrSplit sp( pmesg, '|', true );
					if ( sp.length() >= 4 ) {
						int fpos = atoi(sp[1].c_str());
						if ( fpos < 10 ) {
							servobj->recoveryFileReceiver( pmesg, req );
						} else if ( fpos >= 10 && fpos < 20 ) {
							servobj->walFileReceiver( pmesg, req );
						}
					}
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_ABFILETRANSFER == rc ) {
					servobj->recoveryFileReceiver( pmesg, req );
					servobj->_addClusterFlag = 1;
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_JOINDATAREQUEST == rc ) {
					servobj->joinRequestSend( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );					
				} else if ( JAG_SCMD_OPINFO == rc ) {
					servobj->sendOpInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_COPYDATA == rc ) {
					servobj->doCopyData( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_REFRESHACL == rc ) {
					servobj->doRefreshACL( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_REQSCHEMAFROMDC == rc ) {
					servobj->sendSchemaToDataCenter( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_UNPACKSCHINFO == rc ) {
					servobj->unpackSchemaInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_ASKDATAFROMDC == rc ) {
					servobj->askDataFromDC( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_PREPAREDATAFROMDC == rc ) {
					servobj->prepareDataFromDC( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_DOLOCALBACKUP == rc ) {
					servobj->doLocalBackup( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_DOREMOTEBACKUP == rc ) {
					servobj->doRemoteBackup( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_DORESTOREREMOTE == rc ) {				
					servobj->doRestoreRemote( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONDBTAB == rc ) {
					servobj->dbtabInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONINFO == rc ) {
					servobj->sendInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONRSINFO == rc ) {
					servobj->sendResourceInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONCLUSTERINFO == rc ) {
					servobj->sendClusterOpInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONHOSTS == rc ) {
					servobj->sendHostsInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONBACKUPHOSTS == rc ) {
					servobj->sendRemoteHostsInfo( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONLOCALSTAT == rc ) {
					servobj->sendLocalStat6( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_MONCLUSTERSTAT == rc ) {
					servobj->sendClusterStat6( pmesg, req );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				} else if ( JAG_SCMD_EXPROCLOCALBACKUP == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->processLocalBackup( pmesg, req );
				} else if ( JAG_SCMD_EXPROCREMOTEBACKUP == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->processRemoteBackup( pmesg, req );
				} else if ( JAG_SCMD_EXRESTOREFROMREMOTE == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->processRestoreRemote( pmesg, req );
				} else if ( JAG_SCMD_EXREPAIRCHECK == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->repairCheck( pmesg, req );
				} else if ( JAG_SCMD_EXREPAIROBJECT == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->repairCheck( pmesg, req );
				} else if ( JAG_SCMD_EXADDCLUSTER == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->addCluster( pmesg, req );
				} else if ( JAG_SCMD_EXSHUTDOWN == rc ) {
					// no _END_ ED sent, already sent in method
					servobj->shutDown( pmesg, req );
				} else if ( JAG_SCMD_GETPUBKEY == rc ) {
					// no _END_ ED sent, already sent in method
					JagTable::sendMessage( req, servobj->_publicKey.c_str(), "DS" );
					JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
				}				
			}
			continue;
		}
		
		// for simple request and new connection requests
		if ( simplerc > 0 ) {
			if ( ! authed && JAG_RCMD_AUTH != simplerc ) {
				// printf("s8003 %lld user not authed close\n", pthread_self() );
				JagTable::sendMessage( req, "_END_[T=20|E=Not authed before requesting query]", "ER" );
				breakcode = 5;
				break;
			}
			if ( JAG_RCMD_HELP == simplerc ) {
				tok = strtok_r( pmesg, " ;", &saveptr );
				tok = strtok_r( NULL, " ;", &saveptr );
				if ( tok ) { helpTopic( req, tok ); } 
				else { helpPrintTopic( req ); }
				JagTable::sendMessage( req, "_END_[T=10|E=]", "ED" );				
			} else if ( JAG_RCMD_USE == simplerc ) {
				// no _END_ ED sent, already sent in method
				useDB( req, servobj, pmesg, saveptr );
			} else if ( JAG_RCMD_AUTH == simplerc ) {
				if ( doAuth( req, servobj, pmesg, saveptr, session ) ) {
					// serv->serv still needs the insert pool when built init index from table
					authed = 1; 
					struct timeval now;
					gettimeofday( &now, NULL );
					session.connectionTime = now.tv_sec * (abaxint)1000000 + now.tv_usec;
				} else {
					req.session->active = 0;
					breakcode = 2;
					break;
				}		
			} else if ( JAG_RCMD_QUIT == simplerc ) {
				noLinger( req );
				breakcode = 3;
				if ( session.exclusiveLogin ) {
					servobj->_exclusiveAdmins = 0;
				}
				break;
			} else if ( JAG_RCMD_HELLO == simplerc ) {
				// pthread_t sessid = pthread_self();
				// sprintf( pmesg, "%s Server %s %lld\r\n", brand, servobj->_version.c_str(), sessid );
				// sprintf( hellobuf, "%s Server %s %lld", brand, servobj->_version.c_str(), sessid );
				sprintf( hellobuf, "%s Server %s", brand, servobj->_version.c_str() );
				// rc = JagTable::sendMessage( req, pmesg, "OK");  // SG: Server Greeting
				rc = JagTable::sendMessage( req, hellobuf, "OK");  // SG: Server Greeting
				JagTable::sendMessage( req, "_END_[T=20|E=]", "ED" );
				session.fromShell = 1;
			}
			continue;
		}
		
		// check session dbname and session authed, must be valid for both, otherwise, break connection
		if ( req.session->dbname.size() < 1 ) {
			int ok = 0;
			if ( 0 == strncmp( pmesg, "show", 3 ) ) {
				p = pmesg;
				while ( *p != ' ' && *p != '\0' ) ++p;
				if ( *p != '\0' ) {
					while ( *p == ' ' ) ++p;
					if ( tok && strncmp( p, "database", 8 ) == 0 ) {
						ok = 1;
					}
				}
			}
			
			if ( ! ok ) {
				JagTable::sendMessage( req, "_END_[T=20|E=No database selected]", "ER" );
				breakcode = 4;
				break;
			}
		}

		if ( ! authed ) {
			// printf("s8003 %lld user not authed close\n", pthread_self() );
			JagTable::sendMessage( req, "_END_[T=20|E=Not authed before requesting query]", "ER" );
			breakcode = 5;
			break;
		}
		/**
		if ( ! servobj->_serverReady && ! simplerc && *pmesg != '_' ) {
			JagTable::sendMessage( req, "_END_[T=10|E=Server not ready, please try later]", "ER" );			
			continue;
		}
		**/
		if ( ! servobj->_newdcTrasmittingFin && ! req.session->datacenter && ! req.session->samePID ) {
			JagTable::sendMessage( req, "_END_[T=20|E=Server not ready for datacenter to accept regular commands, please try later]", "ER" );
			continue;
		}


		// if session.drecoverConn is 1 ( for delta recover commands )
		// need to split first and second several bytes for timediff and batchReply
		if ( 1 == session.drecoverConn ) {
			char *p, *q;
			int tdlen = 0;
			p = q = pmesg;
			while ( *q != ';' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				prt(("ERROR tdiff hdr for delta recover\n"));
				abort();
			}
			tdlen = q-p;
			session.timediff = rayatoi( p, tdlen );
			pmesg = q+1;
			len -= ( tdlen+1 );
			++q;
			p = q;
			while ( *q != ';' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				prt(("ERROR isBatch hdr for delta recover\n"));
				abort();
			}
			tdlen = q-p;
			req.batchReply = rayatoi( p, tdlen );
			pmesg = q+1;
			len -= ( tdlen+1 );
		}
		
		/***
		if ( req.batchReply && session.drecoverConn == 0 && !session.origserv ) {
			JagStrSplitWithQuote sspp( pmesg, ';' );
			if ( session.replicateType == 0 ) g_icnt1 += sspp.length();
			else if ( session.replicateType == 1 ) g_icnt2 += sspp.length();
			else if ( session.replicateType == 2 ) g_icnt3 += sspp.length();
		}
		***/
		
		// get recovery flag
		// if yes, for non recover connections, store cmd to regSp log files for write cmd, otherwise continue to do cmd
		// for recover connections, use key checker to shuffle repeated commands
		// if no, for non recover connections, continue regular process
		// for recover connections, ignore and reject commands
		int isReadOrWriteCommand = checkReadOrWriteCommand( pmesg );
		if ( isReadOrWriteCommand > 0 ) {
			JAG_BLURT jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;
			if ( servobj->_restartRecover ) {
				int rspec = 0;
				if ( req.batchReply ) {
					JagDBServer::regSplogCommand( req.session, pmesg, len, 2 );
				} else if ( 0 == strncasecmp( pmesg, "update", 4 ) || 0 == strncasecmp( pmesg, "delete", 6 ) ) {
					JagDBServer::regSplogCommand( req.session, pmesg, len, 1 );
				} else {
					JagDBServer::regSplogCommand( req.session, pmesg, len, 0 );
					rspec = 1;
				}
				jaguar_mutex_unlock ( &g_flagmutex );

				int sprc = 1;
				char cond[3] = { 'O', 'K', '\0' };
				if ( 0 == session.drecoverConn && rspec == 1 ) {
					if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
						sprc = 0;
					}
	
					if ( sprc == 1 && recvData( session.sock, hdr2, newbuf2 ) < 0 ) {
						sprc = 0;
					}
				}

				JagTable::sendMessage( req, "_END_[T=779|E=]", "ED" );
				
				// receive three bytes of signal
				if ( sprc == 1 && servobj->_faultToleranceCopy > 1 && session.drecoverConn == 0 ) {
					// receive status bytes
					int rsmode = 0;
					rephdr[0] = rephdr[1] = rephdr[2] = 'N';		
					if ( recvData( sock, hdr2, newbuf2 ) < 0 ) {
						rephdr[session.replicateType] = 'Y';
						rsmode = servobj->getReplicateStatusMode( rephdr, session.replicateType );
						if ( !session.spCommandReject ) servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
						if ( session.uid == "admin" ) {
							if ( session.exclusiveLogin ) {
								servobj->_exclusiveAdmins = 0;
								raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", session.ip.c_str() );
							}
						}
						if ( newbuf ) { free( newbuf ); }
						if ( newbuf2 ) { free( newbuf2 ); }
						breakcode = 6;
						-- servobj->_connections;
						break;
					}
					rephdr[0] = *newbuf2; rephdr[1] = *(newbuf2+1); rephdr[2] = *(newbuf2+2);
					rsmode = servobj->getReplicateStatusMode( rephdr );
					if ( !session.spCommandReject ) servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
					JagTable::sendMessage( req, "_END_[T=7799|E=]", "ED" );
				}
				continue;
			}
			jaguar_mutex_unlock ( &g_flagmutex );
		}

		// prt(("processMultiCmd: %lld %.200s ...\n", THREADID, pmesg));
		try {
			processMultiCmd( req, pmesg, len, servobj, threadSchemaTime, threadHostTime, threadQueryTime, false, isReadOrWriteCommand );
		} catch ( const char *e ) {
			raydebug( stdout, JAG_LOG_LOW, "processMultiCmd [%s] caught exception [%s]\n", pmesg, e );
		} catch ( ... ) {
			raydebug( stdout, JAG_LOG_LOW, "processMultiCmd [%s] caught unknown exception\n", pmesg );
		}
		// prt(("after processMultiCmd %lld\n", THREADID));

		// receive replicate infomation again, to make sure if delta command needs to be stored or not
		// write related cmd need to be stored
		if ( servobj->_faultToleranceCopy > 1 && isReadOrWriteCommand > 0 && session.drecoverConn == 0 ) {
			// receive status bytes
			int rsmode = 0;
			rephdr[0] = rephdr[1] = rephdr[2] = 'N';
			if ( recvData( sock, hdr2, newbuf2 ) < 0 ) {
				rephdr[session.replicateType] = 'Y';
				rsmode = servobj->getReplicateStatusMode( rephdr, session.replicateType );
				if ( !session.spCommandReject ) servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
				if ( session.uid == "admin" ) {
					if ( session.exclusiveLogin ) {
						servobj->_exclusiveAdmins = 0;
						raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", session.ip.c_str() );
					}
				}

				if ( newbuf ) { free( newbuf ); }
				if ( newbuf2 ) { free( newbuf2 ); }
				breakcode = 7;
				-- servobj->_connections;
				break;
			}
			rephdr[0] = *newbuf2; rephdr[1] = *(newbuf2+1); rephdr[2] = *(newbuf2+2);
			rsmode = servobj->getReplicateStatusMode( rephdr );
			if ( !session.spCommandReject ) servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
			JagTable::sendMessage( req, "_END_[T=77999|E=]", "ED" );
		}
    }

	// servobj->_threadMap->removeKey( AbaxLong( pthread_self() ) );
	session.active = 0;
	--  servobj->_activeClients; 
	// prt(("s4481 client quit, _activeClients=%d\n", (abaxint)servobj->_activeClients ));

   	jagclose(sock);
	// prt(("s9999 closed sock ptr=%0x\n", ptr));
	delete ptr;
	// prt(("s9999 deleted ptr\n"));
	jagmalloc_trim(0);
	// prt(("s9999 malloc_trim(0)\n"));
   	return NULL;
}

// ask servers to confirm DOWN 'N' status
// 0: YYY; 1: YYN; 2: YNY; 3: YNN; 4: NYY; 5: NYN; 6: NNY;
// if three bytes of receiving is broken ( client is down, and replicateType >=0 ), ask servers to make sure each server is up or Down
int JagDBServer::getReplicateStatusMode( char *pmesg, int replicateType )
{
	if ( replicateType >= 0 && _faultToleranceCopy > 2 ) {
		// if client is down, ask servers status using _noop only for 3 replications
		int pos1, pos2, rc1, rc2;
		JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
		if ( 0 == replicateType ) {
			// use left one and right one
			if ( _nthServer == 0 ) {
				pos1 = _nthServer+1;
				pos2 = sp.length()-1;
			} else if ( _nthServer == sp.length()-1 ) {
				pos1 = 0;
				pos2 = _nthServer-1;
			} else {
				pos1 = _nthServer+1;
				pos2 = _nthServer-1;
			}
		} else if ( 1 == replicateType ) {
			// use left two hosts;
			if ( _nthServer == 0 ) {
				pos1 = sp.length()-1;
				pos2 = sp.length()-2;
			} else if ( _nthServer == 1 ) {
				pos1 = 0;
				pos2 = sp.length()-1;
			} else {
				pos1 = _nthServer-1;
				pos2 = _nthServer-2;
			}
		} else if ( 2 == replicateType ) {
			// use right two hosts
			if ( _nthServer == sp.length()-1 ) {
				pos1 = 0;
				pos2 = 1;
			} else if ( _nthServer == sp.length()-2 ) {
				pos1 = sp.length()-1;
				pos2 = 0;
			} else {
				pos1 = _nthServer+1;
				pos2 = _nthServer+2;
			}
		}
		// ask each server of pos1 and pos2 to see if them is up
		rc1 = _dbConnector->broadCastSignal( "_noop", sp[pos1] );
		rc2 = _dbConnector->broadCastSignal( "_noop", sp[pos2] );
		// set correct bytes for pmesg
		if ( 0 == replicateType ) {
			if ( !rc1 ) *(pmesg+1) = 'N';
			else *(pmesg+1) = 'Y';
			if ( !rc2 ) *(pmesg+2) = 'N';
			else *(pmesg+2) = 'Y';
		} else if ( 1 == replicateType ) {
			if ( !rc1 ) *(pmesg) = 'N';
			else *(pmesg) = 'Y';
			if ( !rc2 ) *(pmesg+2) = 'N';
			else *(pmesg+2) = 'Y';
		} else if ( 2 == replicateType ) {
			if ( !rc1 ) *(pmesg) = 'N';
			else *(pmesg) = 'Y';
			if ( !rc2 ) *(pmesg+1) = 'N';
			else *(pmesg+1) = 'Y';
		}
	}

	if ( 0 == strncmp( pmesg, "YYN", 3 ) ) return 1;
	else if ( 0 == strncmp( pmesg, "YNY", 3 ) ) return 2;
	else if ( 0 == strncmp( pmesg, "YNN", 3 ) ) return 3;
	else if ( 0 == strncmp( pmesg, "NYY", 3 ) ) return 4;
	else if ( 0 == strncmp( pmesg, "NYN", 3 ) ) return 5;
	else if ( 0 == strncmp( pmesg, "NNY", 3 ) ) return 6;
	return 0;

}

// return 0: error with reterr
//        1: success
int JagDBServer::createIndexSchema( const JagRequest &req, JagDBServer *servobj, JagTable* ptab, 
									const AbaxDataString &dbname, JagParseParam *parseParam, AbaxDataString &reterr )
{	
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	bool found = false, found2 = false;
	AbaxDataString dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	AbaxDataString dbindex = dbname + "." + parseParam->objectVec[0].tableName + 
							 "." + parseParam->objectVec[1].indexName;
	parseParam->isMemTable = tableschema->isMemTable( dbtable );
	found = indexschema->indexExist( dbname, parseParam );
	found2 = indexschema->existAttr( dbindex );
	if ( found || found2 ) {
		return 0;
	}

	abaxint getpos;
	AbaxDataString errmsg;
	AbaxDataString dbcol;
	AbaxDataString defvalStr;
	int keylen = 0, vallen = 0;
	CreateAttribute createTemp;
	createTemp.init();
	createTemp.objName.dbName = parseParam->objectVec[1].dbName;
	createTemp.objName.tableName = "";
	createTemp.objName.indexName = parseParam->objectVec[1].indexName;
	*(createTemp.spare) = JAG_C_COL_KEY;
	*(createTemp.spare+2) = JAG_ASC;
	const JagSchemaRecord *trecord = tableschema->getAttr( dbtable );
	if ( ! trecord ) {
		return 0;
	}
    
	JagHashStrInt checkmap;
	JagHashStrInt schmap;

	for ( int i = 0; i < trecord->columnVector->size(); ++i ) {
		schmap.addKeyValue((*(trecord->columnVector))[i].name.c_str(), i);
	}
	
	// get key part of create index
	bool hrc;
	for ( int i = 0; i < parseParam->limit; ++i ) {
		checkmap.addKeyValue(parseParam->otherVec[i].objName.colName.c_str(), i);
		getpos = schmap.getValue(parseParam->otherVec[i].objName.colName.c_str(), hrc );
		if ( ! hrc ) {
			prt(("s2024****** kip\n" ));
			continue;
		}
		createTemp.objName.colName = (*(trecord->columnVector))[getpos].name.c_str();
		createTemp.type = (*(trecord->columnVector))[getpos].type;
		createTemp.offset = keylen;
		createTemp.length = (*(trecord->columnVector))[getpos].length;
		createTemp.sig = (*(trecord->columnVector))[getpos].sig;
		createTemp.srid = (*(trecord->columnVector))[getpos].srid;
		*(createTemp.spare+1) = (*(trecord->columnVector))[getpos].spare[1];
		*(createTemp.spare+4) = (*(trecord->columnVector))[getpos].spare[4];
		*(createTemp.spare+5) = (*(trecord->columnVector))[getpos].spare[5]; // mute
		*(createTemp.spare+6) = (*(trecord->columnVector))[getpos].spare[6]; // subcol
		dbcol = dbtable + "." + createTemp.objName.colName;
		defvalStr = "";
		tableschema->getAttrDefVal( dbcol, defvalStr );
		createTemp.defValues = defvalStr.c_str();
		keylen += createTemp.length;
		parseParam->createAttrVec.append( createTemp );
	}
	for (int i = 0; i < trecord->columnVector->size(); i++) {
		if ( (*(trecord->columnVector))[i].iskey && !checkmap.keyExist((*(trecord->columnVector))[i].name.c_str() ) ) {
			createTemp.objName.colName = (*(trecord->columnVector))[i].name.c_str();
			createTemp.type = (*(trecord->columnVector))[i].type;
			createTemp.offset = keylen;
			createTemp.length = (*(trecord->columnVector))[i].length;
			createTemp.sig = (*(trecord->columnVector))[i].sig;
			createTemp.srid = (*(trecord->columnVector))[i].srid;
			*(createTemp.spare+1) = (*(trecord->columnVector))[i].spare[1];
			*(createTemp.spare+4) = (*(trecord->columnVector))[i].spare[4];
			*(createTemp.spare+5) = (*(trecord->columnVector))[i].spare[5];
			*(createTemp.spare+6) = (*(trecord->columnVector))[i].spare[6];
			dbcol = dbtable + "." + createTemp.objName.colName;
			defvalStr = "";
			tableschema->getAttrDefVal( dbcol, defvalStr );
			createTemp.defValues = defvalStr.c_str();
			keylen += createTemp.length;
			parseParam->createAttrVec.append( createTemp );
		}
	}
	parseParam->keyLength = keylen;
	
	// get value part of create index
	*(createTemp.spare) = JAG_C_COL_VALUE;
	for ( int i = parseParam->limit; i < parseParam->otherVec.size(); ++i ) {
		getpos = schmap.getValue(parseParam->otherVec[i].objName.colName.c_str(), hrc);
		if ( ! hrc ) {
			prt(("s2028****** kip\n" ));
			continue;
		}
		createTemp.objName.colName = (*(trecord->columnVector))[getpos].name.c_str();
		createTemp.type = (*(trecord->columnVector))[getpos].type;
		createTemp.offset = keylen;
		createTemp.length = (*(trecord->columnVector))[getpos].length;
		createTemp.sig = (*(trecord->columnVector))[getpos].sig;
		createTemp.srid = (*(trecord->columnVector))[getpos].srid;
		*(createTemp.spare+1) = (*(trecord->columnVector))[getpos].spare[1];
		*(createTemp.spare+4) = (*(trecord->columnVector))[getpos].spare[4];
		*(createTemp.spare+5) = (*(trecord->columnVector))[getpos].spare[5];
		*(createTemp.spare+6) = (*(trecord->columnVector))[getpos].spare[6];
		dbcol = dbtable + "." + createTemp.objName.colName;
		defvalStr = "";
		tableschema->getAttrDefVal( dbcol, defvalStr );
		createTemp.defValues = defvalStr.c_str();
		keylen += createTemp.length;
		vallen += createTemp.length;
		parseParam->createAttrVec.append( createTemp );
	}
	parseParam->valueLength = vallen;
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
    int rc = indexschema->insert( parseParam, false );
	jaguar_mutex_unlock ( &g_dbmutex );
    if ( rc ) raydebug(stdout, JAG_LOG_LOW, "user [%s] create index [%s]\n", req.session->uid.c_str(), dbindex.c_str() );
	//if ( schmap ) delete schmap;
	//if ( checkmap ) delete checkmap;
	// schmap = checkmap = NULL;
	return rc;
}

int JagDBServer::importTable( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname,
	JagParseParam *parseParam, AbaxDataString &reterr )
{
	JagTable *ptab = NULL;
	char cond[3] = { 'O', 'K', '\0' };
	if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
		// prt(("s3828 return 0\n"));
		return 0;
	}
	// waiting for signal; if NG, reject and return
 	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *newbuf = NULL;
	if ( recvData( req.session->sock, hdr, newbuf ) < 0 || 
					*(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
		if ( newbuf ) free( newbuf );
		// connection abort or not OK signal, reject
		return 0;
	} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
		req.syncDataCenter = true;
	}
	if ( newbuf ) free( newbuf );

	AbaxDataString dbtab = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbtab + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	AbaxDataString dirpath = jaguarHome() + "/export/" + dbtab;
	if ( parseParam->impComplete ) {
		JagFileMgr::rmdir( dirpath );
		// prt(("s3418 return 1\n"));
		// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
		servobj->schemaChangeCommandSyncRemove( scdbobj );
		return 1;
	}
	ptab = servobj->_objectLock->readLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
	AbaxDataString host = "localhost", objname = dbtab + ".sql", fpath;
	fpath = JagFileMgr::getFileFamily( dirpath, objname );
	if ( servobj->_listenIP.size() > 0 ) { host = servobj->_listenIP; }
	JaguarCPPClient pcli;
	AbaxDataString unixSocket = AbaxDataString("/TOKEN=") + servobj->_servToken;
	while ( !pcli.connect( host.c_str(), servobj->_port, "admin", "dummy", "test", unixSocket.c_str(), 0 ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4022 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  "admin", "dummy", host.c_str(), servobj->_port, pcli.error() );
		jagsleep(5, JAG_SEC);
	}

	// prt(("s5292 pcli.importLocalFileFamily(%s) ...\n", fpath.c_str() ));
	servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbtab, req.session->replicateType, 1, ptab, NULL );
	if ( ptab ) {	
		servobj->_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
			parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
	}
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );

	int rc = pcli.importLocalFileFamily( fpath );
	// prt(("s5292 pcli.importLocalFileFamily(%s) done rc=%d ...\n", fpath.c_str(), rc ));
	pcli.close();
	if ( rc < 0 ) {
		reterr = "Import file does not exist on server";
		prt(("s4418 Import file does not exist on server 1\n"));
	}
	return (rc>=0);
}

void JagDBServer::init( JagCfg *cfg )
{

	AbaxDataString jagdatahome = cfg->getJDBDataHOME( 0 );

    AbaxDataString sysdir = jagdatahome + "/system";
    AbaxDataString newdir;
   	jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jagdatahome + "/test";
   	jagmkdir( sysdir.c_str(), 0700 );
	
	jagdatahome = cfg->getJDBDataHOME( 1 );
	sysdir = jagdatahome + "/system";
    jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jagdatahome + "/test";
    jagmkdir( sysdir.c_str(), 0700 );
	
	jagdatahome = cfg->getJDBDataHOME( 2 );
	sysdir = jagdatahome + "/system";
    jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jagdatahome + "/test";
    jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jaguarHome() + "/backup";
    jagmkdir( sysdir.c_str(), 0700 );

    newdir = sysdir + "/15min";
    jagmkdir( newdir.c_str(), 0700 );

    newdir = sysdir + "/hourly";
    jagmkdir( newdir.c_str(), 0700 );

    newdir = sysdir + "/daily";
    jagmkdir( newdir.c_str(), 0700 );

    newdir = sysdir + "/weekly";
    jagmkdir( newdir.c_str(), 0700 );

    newdir = sysdir + "/monthly";
    jagmkdir( newdir.c_str(), 0700 );

	AbaxDataString cs;
	/***
	cs = cfg->getValue("STORAGE_TYPE", "HDD");
	if ( cs == "SSD" ) {
		_isSSD = 1;
		raydebug( stdout, JAG_LOG_LOW, "STORAGE_TYPE is SSD\n" );
	} else {
		_isSSD = 0;
		raydebug( stdout, JAG_LOG_LOW, "STORAGE_TYPE is HDD\n" );
	}
	***/

	raydebug( stdout, JAG_LOG_LOW, "Jaguar start\n" );
	cs = cfg->getValue("MEMORY_MODE", "high");
	if ( cs == "high" || cs == "HIGH" ) {
		_memoryMode = JAG_MEM_HIGH;
		raydebug( stdout, JAG_LOG_LOW, "MEMORY_MODE is high\n" );
	} else {
		_memoryMode = JAG_MEM_LOW;
		raydebug( stdout, JAG_LOG_LOW, "MEMORY_MODE is low\n" );
	}

}

void JagDBServer::helpPrintTopic( const JagRequest &req )
{
	AbaxDataString str;
	str += "You can enter the following commands:\n\n";


	str += "help use;           (how to use databases)\n";
	str += "help desc;          (how to describe tables)\n";
	str += "help show;          (how to show tables)\n";
	str += "help create;        (how to create tables)\n";
	str += "help insert;        (how to insert data)\n";
	str += "help load;          (how to load data from client host)\n";
	str += "help select;        (how to select data)\n";
	str += "help getfile;       (how to get file data)\n";
	str += "help update;        (how to update data)\n";
	str += "help delete;        (how to delete data)\n";
	str += "help join;          (how to join two tables)\n";
	str += "help drop;          (how to drop a table completely)\n";
	// str += "help rename        (how to rename a table)\n";
	str += "help alter;         (how to alter a table and rename a key column)\n";
	str += "help truncate;      (how to truncate a table)\n";
	str += "help func;          (how to call functions in select)\n";
	str += "help spool;         (how to write output data to a file)\n";
	str += "help password;      (how to change the password of current user)\n";
	str += "help source;        (how to exeucte commands from a file)\n";
	str += "help shell;         (how to exeucte shell commands)\n";
	str += "\n";
	if ( req.session->uid == "admin" ) {
		str += "help admin;         (how to execute commands for admin account)\n";
		str += "help shutdown;      (how to shutdown servers)\n";
		str += "help createdb;      (how to create a new database)\n";
		str += "help dropdb;        (how to drop a database)\n";
		str += "help createuser;    (how to create a new user account)\n";
		str += "help dropuser;      (how to drop a user account)\n";
		str += "help showusers;     (how to view all user accounts)\n";
		str += "help addcluster;    (how to add a new cluster)\n";
		str += "help grant;         (how to grant permission to a user)\n";
		str += "help revoke;        (how to revoke permission to a user)\n";
	}
	str += "\n";
	str += "\n";

	JagTable::sendMessageLength( req, str.c_str(), str.size(), "I_" );
}

void JagDBServer::helpTopic( const JagRequest &req, const char *cmd )
{
	// prt(("s3384 cmd=[%s]\n", cmd ));

	AbaxDataString str;
	str += "\n";

	if ( 0 == strncasecmp( cmd, "use", 3 ) ) {
		str += "use DATABASE;\n";
		str += "\n";
		str += "Example:\n";
		str += "use userdb;\n";
	} else if ( 0 == strncasecmp( cmd, "admin", 5 ) ) {
    		str += "createdb DBNAME;\n"; 
    		str += "dropdb [force] DBNAME;\n"; 
    		str += "createuser UID;\n"; 
    		str += "createuser UID:PASSWORD;\n"; 
    		str += "dropuser UID;\n"; 
    		str += "showusers;\n"; 
    		str += "grant PERM1, PERM2, ... PERM on DB.TAB.COL to USER [where ...];\n"; 
    		str += "revoke PERM1, PERM2, ... PERM on DB.TAB.COL from USER;\n"; 
    		str += "addcluster;  (add new servers into existing cluster)\n"; 
    		str += "\n"; 
    		str += "Example:\n";
    		str += "createdb mydb;\n";
    		str += "dropdb mydb;\n";
    		str += "dropdb force mydb123;\n";
    		str += "createuser test;\n"; 
    		str += "  New user password: ******\n"; 
    		str += "  New user password again: ******\n"; 
    		str += "createuser test:mypassword888;\n"; 
    		str += "(Maximum length of user ID and password is 32 characters.\n";
    		str += "dropuser test;\n"; 
    		str += "grant all on all to test;\n";
    		str += "grant select, update, delete on all to test;\n";
    		str += "grant select on mydb.tab123.col3 to test;\n";
    		str += "grant select on mydb.tab123.* to test1 where tab123.col4 >= 100 or tab123.col4 <= 800;\n";
    		str += "grant update, delete on mydb.tab123 to test;\n";
    		str += "revoke update on mydb.tab123 from test;\n";
   	} else if ( 0 == strncasecmp( cmd, "createdb", 8 ) ) {
    		str += "jaguar> createdb DBNAME;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to create a new database.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> createdb mydb123;\n";
   	} else if (  0 == strncasecmp( cmd, "shutdown", 8 ) ) {
    		str += "Shutdown jaguar server process\n";
    		str += "jaguar> shutdown SERVER_IP;\n"; 
    		str += "jaguar> shutdown all;\n"; 
    		str += "\n";
    		str += "Only the admin account logging with exclusive mode can issue this command.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "$ jag -u admin -p -h localhost:8888 -x yes\n";
    		str += "jaguar> shutdown 192.168.7.201;\n";
    		str += "jaguar> shutdown all;\n";
   	} else if ( 0 == strncasecmp( cmd, "dropdb", 6 ) ) {
    		str += "jaguar> dropdb DBNAME;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to drop a new database.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> dropdb mydb123;\n";
   	} else if ( 0 == strncasecmp( cmd, "createuser", 9 ) ) {
    		str += "jaguar> createuser UID;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to create a new user account.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> createuser johndoe;\n";
   	} else if ( 0 == strncasecmp( cmd, "dropuser", 8 ) ) {
    		str += "jaguar> dropuser UID;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to delete a user account.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> dropuser johndoe;\n";
   	} else if (  0 == strncasecmp( cmd, "showusers", 8 ) ) {
    		str += "jaguar> showusers;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to display a list of current user accounts.\n"; 
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> showusers;\n";
   	} else if ( 0 == strncasecmp( cmd, "addcluster", 10 ) ) {
    		str += "jaguar> addcluster;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command to add new servers.\n"; 
    		str += "The admin account must login with exclusive mode, i.e., using \"-x yes\" in jag command.\n"; 
    		str += "Before starting this operation, on the admin client host there must exist \n";
    		str += "$JAGUAR_HOME/jaguar/conf/newcluster.conf which contains the IP addresses of new hosts on each line\n";
    		str += "in the file. Once this file is ready and correct, the addcluster command will include and activate \n";
    		str += "the new servers.\n";
   	} else if (  0 == strncasecmp( cmd, "grant", 5 ) ) {
    		str += "jaguar> grant all on all to user;\n"; 
    		str += "jaguar> grant PERM1, PERM2, ... PERM on DB.TAB.COL to user;\n"; 
    		str += "jaguar> grant PERM on DB.TAB.* to user;\n"; 
    		str += "jaguar> grant PERM on DB.TAB  to user;\n"; 
    		str += "jaguar> grant PERM on DB  to user;\n"; 
    		str += "jaguar> grant PERM on all  to user;\n"; 
    		str += "jaguar> grant select on DB.TAB.COL to user [where TAB.COL1 > NNN and TAB.COL2 < MMM;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command.\n"; 
    		str += "PERM is one of: all/create/insert/select/update/delete/alter/truncate \n"; 
    		str += "All means all permissions.\n";
    		str += "The where statement, if provided, will be used to filter rows in select and join.\n";
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> grant all on all to user123;\n";
    		str += "jaguar> grant all on mydb.tab123 to user123;\n";
    		str += "jaguar> grant select on mydb.tab123.* to user123;\n";
    		str += "jaguar> grant select on mydb.tab123.col2 to user3 where tab123.col4>100;\n";
    		str += "jaguar> grant delete, update on mydb.tab123.col4 to user1;\n";
   	} else if (  0 == strncasecmp( cmd, "revoke", 6 ) ) {
    		str += "jaguar> revoke all on all from user;\n"; 
    		str += "jaguar> revoke PERM1, PERM2, ... PERM on DB.TAB.COL from user;\n"; 
    		str += "jaguar> revoke PERM on DB.TAB.* from user;\n"; 
    		str += "jaguar> revoke PERM on DB.TAB  from user;\n"; 
    		str += "jaguar> revoke PERM on DB  from user;\n"; 
    		str += "jaguar> revoke PERM on all  from user;\n"; 
    		str += "\n";
    		str += "Only the admin account can issue this command.\n"; 
    		str += "PERM is one of: all/create/insert/select/update/delete/alter/truncate \n"; 
    		str += "All means all permissions. The permission to be revoked must exist already.\n";
    		str += "\n";
    		str += "Example:\n";
    		str += "jaguar> revoke all on all from user123;\n";
    		str += "jaguar> revoke all on mydb.tab123 from user123;\n";
    		str += "jaguar> revoke select on mydb.tab123.* from user123;\n";
    		str += "jaguar> revoke select, update on mydb.tab123.col2 from user3;\n";
    		str += "jaguar> revoke update, delete on mydb.tab123.col4 from user1;\n";
	} else if ( 0 == strncasecmp( cmd, "desc", 4 ) ) {
		str += "desc TABLE [detail];\n"; 
		str += "desc INDEX [detail];\n"; 
		str += "(If detail is provided, it will display internal fields for complex columns)\n";
		str += "\n";
		str += "Example:\n";
		str += "desc usertab;\n";
		str += "desc addr_index;\n";
		str += "desc geotab detail;\n";
	} else if ( 0 == strncasecmp( cmd, "password", 8 ) ) {
		str += "changepass;\n"; 
		str += "changepass NEWPASSWORD;\n"; 
		str += "changepass uid:NEWPASSWORD; -- for admin only\n"; 
		str += "\n";
		str += "Example:\n";
		str += "changepass;\n"; 
		str += "   New password: ******\n"; 
		str += "   New password again: ******\n"; 
		str += "changepass mynewpassword888;\n"; 
	} else if ( 0 == strncasecmp( cmd, "show", 3 ) ) {
		str += "show databases          (display all databases in the system)\n"; 
		str += "show tables [LIKE PAT]  (display all tables in current database. PAT: '%pat' or '%pat%' or 'pat%' )\n"; 
		str += "show                    indexes     (display all indexes in current database)\n"; 
		str += "show currentdb          (display current database being used)\n"; 
		str += "show task               (display all active tasks)\n"; 
		str += "show indexes from/in table      (display all indexes of a table in currently selected database)\n"; 
		str += "show indexes [LIKE PAT] (display all indexes matching a pattern. PAT: '%pat' or '%pat%' or 'pat%' )\n"; 
		str += "show server version     (display Jaguar server version)\n"; 
		str += "show client version     display Jaguar client version)\n"; 
		str += "show user               (display username of current session)\n"; 
		str += "show status             (display statistics of server cluster)\n";
		str += "show create TABLE       (display statement of creating a table)\n";
		str += "show datacenter         (display data center info)\n";
		str += "show tools              (display bin/tools command scripts)\n";
		str += "show grants [for uid]   (display grants for user. Default is current user)\n";
		str += "\n";
		str += "Example:\n";
		str += "show databases;\n"; 
		str += "show tables;\n"; 
		str += "show tables like '%mytab%';\n"; 
		str += "show tables like 'mytab%';\n"; 
		str += "show indexes from mytable;\n"; 
		str += "show indexes like 'jb%';\n"; 
		str += "show indexes;\n"; 
		str += "show status;\n";
		str += "show task;\n";
	} else if ( 0 == strncasecmp( cmd, "create", 3 ) ) {
		str += "create table [if not exists] TABLE ( key: KEY TYPE(size), ..., value: VALUE TYPE(size), ...  );\n";
		str += "create index INDEXNAEME on TABLE(COL1, COL2, ...[, value: COL,COL]);\n";
		str += "create index INDEXNAEME on TABLE(key: COL1, COL2, ...[, value: COL,COL]);\n";
		str += "\n";
		str += "Example:\n";
		str += "create table dept ( key: name char(32),  value: age int, b bit default b'0' );\n";
		str += "create table log ( key: id bitint,  value: tm timestamp default current_timestmap on update current_timestmap );\n";
		str += "create table user ( key: name char(32),  value: age int, address char(128), rdate date );\n";
		str += "create table sales ( key: name varchar(32), stime datetime, value: author varchar(32), id uuid );\n";
		str += "create table message ( key: name varchar(32), value: msg text );\n";
		str += "create table media ( key: name varchar(32), value: img1 file, img2 file );\n";
		str += "create table if not exists sales ( key: id bigint, stime datetime, value: member char(32) );\n";
		str += "create table geo ( key: id bigint, value: ls linestring, sq square );\n";
		str += "create table park ( key: id bigint, value: lake polygon(srid:wgs84), farm rectangle(srid:wgs84) );\n";
		str += "create table tm ( key: id bigint, value: name char(32), range(datetime) );\n";
		str += "create index addr_index on user( address );\n";
		str += "create index addr_index on user( address, value: zipcode );\n";
		str += "create index addr_index on user( key: address, value: zipcode, city );\n";
	} else if ( 0 == strncasecmp( cmd, "insert", 3 ) ) {
		str += "insert into TABLE (col1, col2, col3, ...) values ( 'va1', 'val2', intval, ... );\n"; 
		str += "insert into TABLE values ( 'k1', 'k2', 'va1', 'val2', intval, ... );\n"; 
		str += "insert into TABLE values ( 'k1', 'k2', 'va1', square(0 0 100), ... );\n"; 
		str += "insert into TABLE values ( 'k1', 'k2', 'va1', json({...}), ... );\n"; 
		str += "insert into TAB1 select TAB2.col1, TAB2.col2, ... from TAB2 [WHERE] [LIMIT];\n";
		str += "insert into TAB1 (TAB1.col1, TAB1.col2, ...) select TAB2.col1, TAB2.col2, ... from TAB2 [WHERE] [LIMIT];\n";
		str += "insert into TAB1 (TAB1.col1, TAB1.col2, ...) values ( 'k1', 'k2', load_file(/path/to/file), 'v4' );\n";
		str += "insert into TAB1 (TAB1.col1, TAB1.col2, ...) values ( 'k1', now(), 'v3', 'v4' );\n";
		str += "insert into TAB1 (TAB1.col1, TAB1.col2, ...) values ( 'k1', time(), 'v3', 'v4' );\n";
		str += "\n";
		str += "Example:\n";
		str += "insert into user values ( 'John S.', 'Doe' );\n";
		str += "insert into user ( fname, lname ) values ( 'John S.', 'Doe' );\n";
		str += "insert into user ( fname, lname, age ) values ( 'David', 'Doe', 30 );\n";
		str += "insert into user ( fname, lname, age, addr ) values ( 'Larry', 'Lee', 40, '123 North Ave., CA' );\n";
		str += "insert into member ( name, datecol ) values ( 'LarryK', '2015-03-21' );\n";
		str += "insert into member ( name, timecol ) values ( 'DennyC', '2015-12-23 12:32:30.022012 +08:30' );\n";
		str += "insert into geo values ( 'DennyC', linestring(1 2, 2 3, 3 4, 4 5), '123' );\n";
		str += "insert into geo2 values ( '123', json({\"type\":\"Point\", \"coordinates\": [2,3] }) );\n";
		str += "insert into ev values ( '123', range(10, 20) );\n";
		str += "insert into tm values ( '123', range('2018-09-01 11:12:10', '2019-09-01 11:12:10' ) );\n";
		str += "For datetime, datetimenano, timestamp columns, if no time zone is provided (as in +HH:MM),\n";
		str += "then the provided time is taken as the client's local time.\n";
		str += "insert into t1 select * from t2 where t2.key1=1000;\n";
		str += "insert into t1 (t1.k1, t1.k2, t1.c2) select t2.k1, t2.c2, t2.c4 from t2 where t2.k1=1000;\n";
		str += "The columns in t1 and selected columns from t2 must be compatible (same type)\n";
		str += "insert into t1 values ( 'k1', 'k2', load_file($HOME/path/to/girl.jpg), 'v4' );\n";
		str += "The data of $HOME/path/to/girl.jpg will be first base64-encoded and then loaded into table t1.\n";
		str += "now() inserts datetime string \"YYYY-MM-DD HH:MM:SS.dddddd\" into a table.\n";
		str += "time() inserts the number of seconds since the Epoch.\n";
	} else if ( 0 == strncasecmp( cmd, "getfile", 7 ) ) {
		str += "getfile FILECOL1 into LOCALFILE, FILECOL2 into LOCALFILE2, ... from TABLE where ...;\n";
		str += "    (The above command downloads data of column(s) of type file and save to client side files)\n";
		str += "getfile FILECOL1 time, FILECOL2 size, FILECOL3 md5 from TABLE where ...;\n";
		str += "getfile FILECOL1 time, FILECOL1 size, FILECOL1 md5 from TABLE where ...;\n";
		str += "    (The above command displays modification time, size (in bytes), and md5sum of file column)\n";
		str += "\n";
		str += "Example:\n";
		str += "getfile img1 into myimg1.jpg, img2 into myimg2.jog from media where uid='100';\n"; 
		str += "    (assume img1 and img2 are two file type columns in table media)\n";
		str += "getfile img time, img size, img md5 from TABLE where ...;\n";
	} else if ( 0 == strncasecmp( cmd, "select", 3 ) ) {
		str += "(SELECT CLAUSE) from TABLE [WHERE CLAUSE] [GROUP BY CLAUSE] [ORDER BY] [LIMIT CLAUSE] [exportsql] [TIMEOUT N];\n";
		str += "(SELECT CLAUSE) from TABLE [WHERE CLAUSE] [GROUP BY CLAUSE] [ORDER BY] [LIMIT CLAUSE] [exportcsv] [TIMEOUT N];\n";
		str += "(SELECT CLAUSE) from TABLE [WHERE CLAUSE] [GROUP BY CLAUSE] [ORDER BY] [LIMIT CLAUSE] [export] [TIMEOUT N];\n";
		str += "(SELECT CLAUSE) from INDEX [WHERE CLAUSE] [GROUP BY CLAUSE] [ORDER BY] [LIMIT CLAUSE] [TIMEOUT N];\n";
		str += "select * from TABLE;\n"; 
		str += "select * from TABLE limit N;\n"; 
		str += "select * from TABLE limit S,N;\n"; 
		str += "select COL1, COL2, ... from TABLE;\n"; 
		str += "select COL1, COL2, ... from TABLE limit N;\n"; 
		str += "select COL1, COL2, ... from TABLE limit N;\n"; 
		str += "select COL1, COL2, ... from TABLE where KEY='...' or KEY='...' and ( ... ) ;\n"; 
		str += "select COL1, COL2, ... from TABLE where KEY <='...' and KEY >='...' or ( ... and ... );\n"; 
		str += "select COL1, COL2, ... from TABLE where KEY between 'abc' and 'bcd' and KEY2 between e and f;\n";
		str += "select COL1, COL2, ... from TABLE where KEY between 'abc' and 'bcd' and VAL1 between m and n;\n";
		str += "select COL1 as col1label, COL2 col2label, ... from TABLE;\n";
		str += "select count(*) from TABLE;\n"; 
		str += "select min(COL1), max(COL2) max2, avg(COL3) as avg3, sum(COL4) sum4, count(1) from TABLE;\n"; 
		str += "select FUNC(COL1), FUNC(COL2) as x from TABLE;\n"; 
		str += "\n";
		str += "Example:\n";
		str += "select * from user;\n"; 
		str += "select * from user export;\n"; 
		str += "select * from user exportcsv;\n"; 
		str += "select * from user exportsql timeout 500;\n"; 
		str += "select * from user limit 100;\n"; 
		str += "select * from user limit 1000,100;\n"; 
		str += "select fname, lname, address from user;\n"; 
		str += "select fname, lname, address, age  from user limit 10;\n"; 
		str += "select fname, lname, address  from user where fname='Sam' and lname='Walter';\n"; 
		str += "select * from user where fname='Sam' and lname='Walter';\n"; 
		str += "select * from user where fname='Sam' or ( fname='Ted' and lname like 'Ben%');\n"; 
		str += "select * from user where fname='Sam' and match 'Ben.*s');\n"; 
		str += "select * from user where fname >= 'Sam';\n"; 
		str += "select * from user where fname >= 'Sam' and fname < 'Zack';\n"; 
		str += "select * from user where fname >= 'Sam' and fname < 'Zack' and ( zipcode = 94506 or zipcode = 94507);\n"; 
		str += "select * from user where fname match 'Sam.*' and fname < 'Zack' and zipcode in ( 94506, 94582 );\n"; 
		str += "select * from t1_index  where uid='frank380' or uid='davidz';\n";
		str += "select * from sales  where stime between '2014-12-01 00:00:00 -08:00' and '2014-12-31 23:59:59 -08:00';\n";
		str += "select avg(amt) as amt_avg from sales;\n";
		str += "select sum(amt) amt_sum from sales where ...;\n";
		str += "select sum(amt) amt_sum from sales group by key1, key2 limit 10;\n";
		str += "select k1, sum(amt) amt_sum from sales group by val1, key1 limit 10;\n";
		str += "select sum(amt+fee) as amt_sum from sales where ...;\n";
		str += "select c1, c4, count(1) as cnt from tab123 where k1 like 'abc%' group by v1, k2 order by cnt desc limit 100; \n";
		str += "select c1, c4, count(1) as cnt from tab123 where group by v1, k2 order by cnt desc limit 1000 timeout 180; \n";
	} else if ( 0 == strncasecmp( cmd, "func", 4 ) ) {
		str += "SELECT FUNC( EXPR(COL) ) from TABLE [WHERE CLAUSE] [GROUP BY CLAUSE] [LIMIT CLAUSE];\n";
		str += "  EXPR(COL): \n";
		str += "    Numeric columns:  columns with arithmetic operation\n";
		str += "                      +  addition\n";
		str += "                      -  subtraction)\n";
		str += "                      *  multiplication)\n";
		str += "                      /  division\n";
		str += "                      %  modulo\n";
		str += "                      ^  power\n";
		str += "    String columns: Concatenation of columns or  string constants\n";
		str += "                      string column + string column\n";
		str += "                      string column + string constant\n";
		str += "                      string constant + string column\n";
		str += "                      string constant + string constant\n";
		str += "                      string constant:  'some string'\n";
		str += "  FUNC( EXPR(COL) ): \n";
		str += "    min( EXPR(COL) )    -- minimum value of column expression\n";
		str += "    max( EXPR(COL) )    -- maximum value of column expression\n";
		str += "    avg( EXPR(COL) )    -- average value of column expression\n";
		str += "    sum( EXPR(COL) )    -- sum of column expression\n";
		str += "    stddev( EXPR(COL) ) -- standard deviation of column expression\n";
		str += "    first( EXPR(COL) )  -- first value of column expression\n";
		str += "    last( EXPR(COL) )   -- last value of column expression\n";
		str += "    abs( EXPR(COL) )    -- absolute value of column expression\n";
		str += "    acos( EXPR(COL) )   -- arc cosine function of column expression\n";
		str += "    asin( EXPR(COL) )   -- arc sine function of column expression\n";
		str += "    ceil( EXPR(COL) )   -- smallest integral value not less than column expression\n";
		str += "    cos( EXPR(COL) )    -- cosine value of column expression\n";
		str += "    cot( EXPR(COL) )    -- inverse of tangent value of column expression\n";
		str += "    floor( EXPR(COL) )  -- largest integral value not greater than column expression\n";
		str += "    log2( EXPR(COL) )   -- base-2 logarithmic function of column expression\n";
		str += "    log10( EXPR(COL) )  -- base-10 logarithmic function of column expression\n";
		str += "    log( EXPR(COL) )    -- natural logarithmic function of column expression\n";
		str += "    ln( EXPR(COL) )     -- natural logarithmic function of column expression\n";
		str += "    mod( EXPR(COL), EXPR(COL) )  -- modulo value of first over second column expression\n";
		str += "    pow( EXPR(COL), EXPR(COL) )  -- power function of first to second column expression\n";
		str += "    radians( EXPR(COL) )   -- convert degrees to radian\n";
		str += "    degrees( EXPR(COL) )   -- convert radians to degrees\n";
		str += "    sin( EXPR(COL) )    -- sine function of column expression\n";
		str += "    sqrt( EXPR(COL) )   -- square root function of column expression\n";
		str += "    tan( EXPR(COL) )    -- tangent function of column expression\n";
		str += "    substr( EXPR(COL), start, length )  -- sub string of column expression\n";
		str += "    substr( EXPR(COL), start, length, ENCODING )  -- sub string of international language\n";
		str += "                                      ENCODING: 'UTF8', 'GBK', 'GB2312', 'GB10830'\n";
		str += "    substring( EXPR(COL), start, length)             -- same as substr() above\n";
		str += "    substring( EXPR(COL), start, length, ENCODING )  -- same as substr() with encoding\n";
		str += "    diff( EXPR(COL), EXPR(COL) )  -- Levenshtein (or edit) distance between two strings (or columns)\n";
		str += "    upper( EXPR(COL) )  -- upper case string of column expression\n";
		str += "    lower( EXPR(COL) )  -- lower case string of column expression\n";
		str += "    ltrim( EXPR(COL) )  -- remove leading white spaces of string column expression\n";
		str += "    rtrim( EXPR(COL) )  -- remove trailing white spaces of string column expression\n";
		str += "    trim( EXPR(COL) )   -- remove leading and trailing white spaces of string column expression\n";
		str += "    length( EXPR(COL) ) -- length of string column expression\n";
		str += "    second( TIMECOL )   -- value of second in a time column\n";
		str += "    minute( TIMECOL )   -- value of minute in a time column\n";
		str += "    hour( TIMECOL )     -- value of hour in a time column\n";
		str += "    date( TIMECOL )     -- value of date in a time column\n";
		str += "    month( TIMECOL )    -- value of month in a time column\n";
		str += "    year( TIMECOL )     -- value of year in a time column\n";
		str += "    datediff(type, BEGIN_TIMECOL, END_TIMECOL )  -- difference of two time columns\n";
		str += "             type: second  (difference in seconds)\n";
		str += "             type: minute  (difference in minutes)\n";
		str += "             type: hour    (difference in hours)\n";
		str += "             type: day     (difference in days)\n";
		str += "             type: month   (difference in months)\n";
		str += "             type: year    (difference in years)\n";
		str += "             The result is the value of (END_TIMECOL - BEGIN_TIMECOL) \n";
		str += "    dayofmonth( TIMECOL )  -- the day of the month in a time column (1-31)\n";
		str += "    dayofweek( TIMECOL )   -- the day of the week in a time column (0-6)\n";
		str += "    dayofyear( TIMECOL )   -- day of the year in a time column (1-366)\n";
		str += "    curdate()              -- current date (yyyy-mm-dd) in client time zone\n";
		str += "    curtime()              -- current time (hh:mm:ss) in client time zone\n";
		str += "    now()                  -- current date and time (yyyy-dd-dd hh:mm:ss) in client time zone\n";
		str += "    time()                 -- current time in seconds since the Epoch (1970-01-01 00:00:00)\n";
		str += "    tosecond('PATTERN')    -- convert PATTERN to seconds. PATTERN: xM, xH, xD, xW. x is a number.\n";
		str += "    tomicrosecond('PATTERN')  -- convert PATTERN to microseconds. PATTERN: xS, xM, xH, xD, xW. x is a number.\n";
		str += "    miletometer(MILES)     -- convert miles to meters\n";
		str += "    within(col1, col2)           -- check if shape col1 is within another shape col2\n";
		str += "    nearby(col1, col2, radius)   -- check if shape col1 is close to another shape col2 by distance radius\n";
		str += "    intersect(col1, col2 )       -- check if shape col1 intersects another shape col2\n";
		str += "    coveredby(col1, col2 )       -- check if shape col1 is covered by another shape col2\n";
		str += "    cover(col1, col2 )           -- check if shape col1 covers another shape col2\n";
		str += "    contain(col1, col2 )         -- check if shape col1 contains another shape col2\n";
		str += "    disjoint(col1, col2 )        -- check if shape col1 and col2 are disjoint\n";
		str += "    distance(col1, col2 )        -- compute distance between shape col1 and col2\n";
		str += "    area(col)                    -- compute area or surface area of 2D or 3D objects\n";
		str += "    all(col)                     -- get GeoJSON data of 2D or 3D objects\n";
		str += "    dimension(col)         -- get dimension as integer of a shape column \n";
		str += "    geotype(col)           -- get type as string of a shape column\n";
		str += "    pointn(col,n)          -- get n-th point (1-based) of a shape column. (x y [z])\n";
		str += "    bbox(col)              -- get bounding box of a shape column. (xmin ymin [zmin] xmax ymax [zmax])\n";
		str += "    startpoint(col)        -- get start point of a line string column. (x y [z])\n";
		str += "    endtpoint(col)         -- get end point of a line string column. (x y [z])\n";
		str += "    isclosed(col)          -- check if points of a line string column is closed. (0 or 1)\n";
		str += "    numpoints(col)         -- get total number of points of a line string or polygon\n";
		str += "    numrings(col)          -- get total number of rings of a polygon or multipolygon\n";
		str += "    srid(col)              -- get SRID of a shape column\n";
		str += "    summary(col)           -- get a text summary of a shape column\n";
		str += "    xmin(col)              -- get the minimum x-coordinate of a shape with raster data\n";
		str += "    ymin(col)              -- get the minimum y-coordinate of a shape with raster data\n";
		str += "    zmin(col)              -- get the minimum z-coordinate of a shape with raster data\n";
		str += "    xmax(col)              -- get the maximum x-coordinate of a shape with raster data\n";
		str += "    ymax(col)              -- get the maximum y-coordinate of a shape with raster data\n";
		str += "    zmax(col)              -- get the maximum z-coordinate of a shape with raster data\n";
		str += "    convexhull(col)        -- get the convex hull of a shape with raster data\n";
		str += "    centroid(col)          -- get the centroid coordinates of a vector or raster shape\n";
		str += "    volume(col)            -- get the volume of a 3D shape\n";
		str += "    closestpoint(point(x y), col)   -- get the closest point on col from point(x y)\n";
		str += "    angle(line(x y), col)  -- get the closest point on geom from point(x y)\n";
		str += "\n";
		str += "Example:\n";
		str += "select sum(amt) as amt_sum from sales limit 3;\n";
		str += "select cos(lat), sin(lon) from map limit 3;\n";
		str += "select tan(lat+sin(lon)), cot(lat^2+lon^2) from map limit 3;\n";
		str += "select uid, uid+addr, length(uid+addr)  from user limit 3;\n";
		str += "select price/2.0 + 1.25 as newprice, lead*1.25 - 0.3 as newlead from plan limit 3;\n";
		str += "select * from tm where dt < time() - tomicrosecond('1D');\n";
	} else if ( 0 == strncasecmp( cmd, "update", 6 ) ) {
		str += "update TABLE set VALUE='...', VALUE='...', ... where KEY1='...' and KEY2='...', ... ;\n";
		str += "update TABLE set VALUE='...', VALUE='...', ... where KEY1>='...' and KEY2>='...', ...;\n";
		str += "update TABLE set VALUE='...', ... where KEY='...' and VALUE='...', ...;\n";
		str += "update TABLE set VALUE=VALUE+N where KEY='...';\n";
		str += "\n";
		str += "Example:\n";
		str += "update user set address='200 Main St., SR, CA 94506' where fname='Sam' and lname='Walter';\n";
		str += "update user set fname='Tim', address='201 Main St., SR, CA 94506' where fname='Sam' and lname='Walter';\n";
	/***
	} else if ( 0 == strncasecmp( cmd, "upsert", 6 ) ) {
		str += "upsert is same as insert except that when the key exists in the table,\n";
		str += "the old record is replaced with the new record.\n";
	***/
	} else if ( 0 == strncasecmp( cmd, "delete", 3 ) ) {
		str += "delete from TABLE;\n";
		str += "delete from TABLE where KEY='...' and KEY='...' and ... ;\n";
		str += "delete from TABLE where KEY>='...' and KEY<='...' and ... ;\n";
		str += "\n";
		str += "Example:\n";
		str += "delete from junktable;\n";
		str += "delete from user where fname='Sam' and lname='Walter';\n";
	} else if ( 0 == strncasecmp( cmd, "drop", 4 ) ) {
		str += "drop table [if exists] [force] TABLENAME; \n"; 
		str += "drop index [if exists] INDEXNAME on TABLENAME; \n"; 
		str += "\n";
		str += "Example:\n";
		str += "drop table user; \n";
		str += "drop table if exists user; \n";
		str += "drop table force user123; \n";
		str += "drop index user_idx9 on user; \n";
		str += "drop index if exists user_idx9 on user; \n";
	} else if ( 0 == strncasecmp( cmd, "truncate", 3 ) ) {
		str += "truncate table TABLE; \n"; 
		str += "\n";
		str += "Data in table will be deleted, but the table tableschema still exists.\n";
		str += "\n";
		str += "Example:\n";
		str += "truncate table user; \n";
	/***
	} else if ( 0 == strncasecmp( cmd, "rename", 3 ) ) {
		str += "rename table OLDTABLE to NEWTABLE; \n"; 
		str += "\n";
		str += "Rename old table OLDTABLE to new table NEWTABLE.\n";
		str += "\n";
		str += "Example:\n";
		str += "rename table myoldtable to newtable;\n";
	***/
	} else if ( 0 == strncasecmp( cmd, "alter", 3 ) ) {
		str += "alter table TABLE add COLNAME TYPE;\n";
		str += "alter table TABLE rename OLDKEY to NEWKEY;\n";
		str += "\n";
		str += "Add a new column in table TABLE.\n";
		str += "Rename a key name in table TABLE.\n";
		str += "\n";
		str += "Example:\n";
		str += "alter table mytable add zipcode char(6);\n";
		str += "alter table mytable rename mykey1 to userid;\n";
		/**************
	} else if ( 0 == strncasecmp( cmd, "join", 4 ) ) {
		str += "Join tables that have partial keys or the same set of keys as the first table.\n";
		str += "(SELECT CLAUSE) join ( TABLE, TABLE1, TABLE2, ...)  [WHERE CLAUSE] [GROUP BY] [LIMIT CLAUSE];\n";
		str += "select * join ( TABLE, TABLE1, TABLE2, ...);\n";
		str += "select * join ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select * join ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  join ( TABLE, TABLE1, TABLE2, ...);\n";
		str += "select TAB.COL, TAB.COL, ...  join ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  join ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select * join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...);\n";
		str += "select * join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) where KEY between A and B;\n";
		str += "select * join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) where KEY between A and B and DB.TABLE.VAL >= nnn;\n";
		str += "select * join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select * join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...);\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  join ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "\n";
		str += "Example:\n";
		str += "select * join ( user, clubmember );\n";
		str += "select * join ( user, clubmember ) limit 100;\n";
		str += "select * join ( user, clubmember ) limit 100;\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level join ( user, clubmember );\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level join ( user, clubmember ) limit 30;\n";
		str += "select mdb.user.fname, mdb.user.lname, storedb.customer.credit join ( mdb.user, storedb.customer );\n";
		str += "select * join ( user, clubmember ) where user.uid=12345; \n";
		str += "select * join ( user, clubmember ) where user.age>50 and clubmemer.zipcode=94506 limit 100;\n";
	} else if ( 0 == strncasecmp( cmd, "starjoin", 8 ) ) {
		str += "Join tables that have keys appearing as key or value in the first table.\n";
		str += "(SELECT CLAUSE) starjoin ( TABLE, TABLE1, TABLE2, ...)  [WHERE CLAUSE] [GROUP BY] [LIMIT CLAUSE];\n";
		str += "select * starjoin ( TABLE, TABLE1, TABLE2, ...);\n";
		str += "select * starjoin ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select * starjoin ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  starjoin ( TABLE, TABLE1, TABLE2, ...);\n";
		str += "select TAB.COL, TAB.COL, ...  starjoin ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  starjoin ( TABLE, TABLE1, TABLE2, ...) limit N;\n";
		str += "select * starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...);\n";
		str += "select * starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) where KEY between A and B;\n";
		str += "select * starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) where KEY between A and B and DB.TABLE.VAL >= nnn;\n";
		str += "select * starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select * starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...);\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  starjoin ( DB.TABLE, DB.TABLE1, DB.TABLE2, ...) limit N;\n";
		str += "\n";
		str += "Example:\n";
		str += "select * starjoin ( user, clubmember );\n";
		str += "select * starjoin ( user, clubmember ) limit 100;\n";
		str += "select * starjoin ( user, clubmember ) limit 100;\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level starjoin ( user, clubmember );\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level starjoin ( user, clubmember ) limit 30;\n";
		str += "select mdb.user.fname, mdb.user.lname, storedb.customer.credit starjoin ( mdb.user, storedb.customer );\n";
		str += "select * starjoin ( user, clubmember ) where user.uid=12345; \n";
		str += "select * starjoin ( user, clubmember ) where user.age>50 and clubmemer.zipcode=94506 limit 100;\n";
	} else if ( 0 == strncasecmp( cmd, "indexjoin", 9 ) ) {
		str += "Join an index and tables that have the same set of keys or partial keys of the index.\n";
		str += "(SELECT CLAUSE) indexjoin ( index(INDEX), TABLE1, TABLE2 ...)  [WHERE CLAUSE] [GROUP BY CLAUSE] [LIMIT CLAUSE];\n";
		str += "select * indexjoin ( index(INDEX), TABLE1, TABLE2 ...);\n";
		str += "select * indexjoin ( index(INDEX), TABLE1, TABLE2 ...) limit N;\n";
		str += "select * indexjoin ( index(INDEX), TABLE1, TABLE2 ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  indexjoin ( index(INDEX), TABLE1, TABLE2 ...);\n";
		str += "select TAB.COL, TAB.COL, ...  indexjoin ( index(INDEX), TABLE1, TABLE2 ...) limit N;\n";
		str += "select TAB.COL, TAB.COL, ...  indexjoin ( index(INDEX), TABLE1, TABLE2 ...) limit N;\n";
		str += "select * indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...);\n";
		str += "select * indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) where KEY between A and B;\n";
		str += "select * indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) where KEY between A and B and DB.TABLE.VAL >= nnn;\n";
		str += "select * indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) limit N;\n";
		str += "select * indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...);\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) limit N;\n";
		str += "select DB.TAB.COL, DB.TAB.COL, ...  indexjoin ( index(DB.INDEX), DB.TABLE1, DB.TABLE2 ...) limit N;\n";
		str += "\n";
		str += "Example:\n";
		str += "select * indexjoin ( index(addr_index), user, clubmember );\n";
		str += "select * indexjoin ( index(addr_index), user, clubmember ) limit 100;\n";
		str += "select * indexjoin ( index(addr_index), user, clubmember ) limit 100;\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level indexjoin ( index(addr_index), user, clubmember );\n";
		str += "select user.fname, user.lname, clubmember.clubname, clubmember.level indexjoin ( index(addr_index), user, clubmember ) limit 30;\n";
		str += "select mdb.user.fname, mdb.user.lname, storedb.customer.credit indexjoin ( index(cdb.addr_index), mdb.user, storedb.customer );\n";
		str += "select * indexjoin ( index(addr_index), user, clubmember ) where user.uid=12345; \n";
		str += "select * indexjoin ( index(addr_index), user, clubmember ) where user.age>50 and clubmemer.zipcode=94506 limit 100;\n";
		******/
	} else if ( 0 == strncasecmp( cmd, "join", 4 ) ) {
		str += "Join two tables by any column, either key or value.\n";
		str += "(SELECT ) from TABLE1 [inner] join TABLE2 on TABLE1.COL1=TABLE2.COL2 [WHERE CLAUSE] [GROUPBY] [ORDERBY] [LIMIT] [TIMEOUT];\n";
		str += "(SELECT ) from TABLE1, TABLE1 where TABLE1.COL1=TABLE2.COL2 [MORE WHERE CLAUSE] [GROUPBY] [ORDERBY] [LIMIT] [TIMEOUT];\n";
		str += "\n";
		str += "Example:\n";
		str += "select tab1.name, tab2.id from tab1 join tab2 on tab1.id=tab2.uid where tab2.zip=230210;\n"; 
		str += "select tab1.name, tab2.id from tab1, tab2 where tab1.id=tab2.uid and tab2.city=123 order by tab1.indate;\n";
	} else if ( 0 == strcasecmp( cmd, "load" ) ) {
		str += "load /path/input.txt into TABLE [columns terminated by C] [lines terminated by N] [quote terminated by Q];\n"; 
		str += "(Instructions inside [ ] are optional. /path/input.txt is located on client host.)\n";
		str += "Default values: \n";
		str += "   columns terminated by: ','\n";
		str += "   lines terminated by: '\\n'\n";
		str += "   columns quoted by singe quote (') character.\n";
		str += "\n";
		str += "Example:\n";
		str += "load /tmp/input.txt into user columns terminated by ',';\n"; 
		str += "load /tmp/input.txt into user quote terminated by '\\'';\n"; 
		str += "load /tmp/input.txt into user quote terminated by '\"';\n"; 
		str += "\n";
	/***
	} else if ( 0 == strncasecmp( cmd, "copy", 4 ) ) {
		str += "copy /path/input.txt into TABLE [columns terminated by C] [lines terminated by N] [quote terminated by Q];\n"; 
		str += "(Instructions inside [ ] are optional. /path/input.txt is located on single server host.)\n";
		str += "Default values: \n";
		str += "   columns terminated by: ','\n";
		str += "   lines terminated by: '\\n'\n";
		str += "   columns can be quoted by singe quote (') character.\n";
		str += "\n";
		str += "Example:\n";
		str += "copy /tmp/input.txt into user columns terminated by ',';\n"; 
		str += "\n";
	***/
	} else if ( 0 == strncasecmp( cmd, "spool", 5 ) ) {
		str += "spool LOCALFILE;\n";
		str += "spool off;\n";
		str += "\n";
		str += "Example:\n";
		str += "spool /tmp/myout.txt;\n";
		str += "(The above command will make the output data to be written to file /tmp/myout.txt)\n";
		str += "\n";
		str += "spool off;\n";
		str += "(The above command will stop writing output data to any file)\n";
		str += "\n";
	} else if ( 0 == strncasecmp( cmd, "source", 6 ) ) {
		str += "source SQLFILE;\n";
		str += "@SQLFILE;\n";
		str += "Execute commands from file SQLFILE";
		str += "\n";
		str += "Example:\n";
		str += "source /tmp/myout.sql;\n";
		str += "@/tmp/myout.sql;\n";
		str += "(The above commands will execute the commands  in /tmp/myout.txt)\n";
		str += "\n";
	} else if ( 0 == strncasecmp( cmd, "shell", 5 ) ) {
		str += "!command;\n";
		str += "Execute shell command";
		str += "\n";
		str += "Example:\n";
		str += "!cat my.sql;\n";
		str += "(The above command will cat my.sql file and display its content.)\n";
		str += "\n";
	} else {
		str += AbaxDataString("Command not supported [") + cmd + "]\n";
	}

	// str += "\n";

	JagTable::sendMessageLength( req, str.c_str(), str.size(), "I_" );
}

void JagDBServer::showCurrentDatabase( JagCfg *cfg, const JagRequest &req, const AbaxDataString &dbname )
{
	AbaxDataString res = dbname;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showDatabases( JagCfg *cfg, const JagRequest &req )
{
	AbaxDataString res;
	res = JagSchema::getDatabases( cfg, req.session->replicateType );
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showCurrentUser( JagCfg *cfg, const JagRequest &req )
{
	AbaxDataString res = req.session->uid;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showClusterStatus( const JagRequest &req, JagDBServer *servobj )
{
	// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
	char buf[1024];
	AbaxDataString res = getClusterOpInfo( req, servobj);
	JagStrSplit sp( res, '|' );
	res = "Servers   Databases   Tables  Connections      Selects      Inserts      Updates      Deletes\n";
	res += "---------------------------------------------------------------------------------------------\n";
	sprintf( buf, "%7ld %11d %8ld %12ld %12ld %12ld %12ld %12ld\n", 
				jagatoll( sp[0].c_str() ),
				jagatoll( sp[1].c_str() ),
				jagatoll( sp[2].c_str() ),
				jagatoll( sp[7].c_str() ),
				jagatoll( sp[3].c_str() ),
				jagatoll( sp[4].c_str() ),
				jagatoll( sp[5].c_str() ),
				jagatoll( sp[6].c_str() )  );

	res += buf;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showTools( const JagRequest &req, JagDBServer *servobj )
{
	AbaxDataString res;
	res += "Scripts in $JAGUAR_HOME/bin/tools/: \n";
	res += "\n";
	res += "check_servers_on_all_hosts.sh     -- check if server.conf is same on all hosts\n";
	res += "check_binaries_on_all_hosts.sh    -- check jaguar server program on all hosts\n";
	res += "check_config_on_all_hosts.sh      -- check each config parameter in server.conf on all hosts\n";
	res += "check_datacenter_on_all_hosts.sh  -- check if datacenter.conf is same on all hosts\n";
	res += "check_log_on_all_hosts.sh [N]     -- display the last N (default 10) lines of jaguar.log on all hosts\n";
	res += "change_config_on_all_hosts.sh NAME VALUE  -- change the value of config paramter NAME  to VALUE on all hosts\n";
	res += "delete_config_on_all_hosts.sh NAME        -- delete the config paramter NAME on all hosts\n";
	res += "remove_config_on_all_hosts.sh NAME        -- comment out the config paramter NAME on all hosts\n";
	res += "restore_config_on_all_hosts.sh NAME       -- uncomment the config paramter NAME on all hosts\n";
	res += "list_configs_on_all_hosts.sh       -- list all config parameters in server.conf on all hosts\n";
	res += "disable_datacenter_on_all_hosts.sh -- disable datacenter on all hosts\n";
	res += "enable_datacenter_on_all_hosts.sh  -- enable datacenter on all hosts\n";
	res += "sshall \"command\"                   -- execute command on all hosts\n";
	res += "scpall LOCALFILE DESTDIR           -- copy LOCALFILE to destination directory on all hosts\n";
	res += "setupsshkeys                       -- setup account public keys on all hosts\n";
	res += "createKeyPair.[bin|exe]            -- generate server public and private keys on current host\n";
	res += "\n";

	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showDatacenter( const JagRequest &req, JagDBServer *servobj )
{
	// prt(("s7374 showDatacenter ...\n" ));
	AbaxDataString fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
		return;
	}

	AbaxDataString res, cont;
	JagFileMgr::readTextFile( fpath, cont );
	/*******
	AbaxDataString line, line2, uphost, host, port, destType;
	int rc;
	res = "Data Center Queries\n";
	res += "--------------------------------------------------\n";
	JagStrSplit sp( cont, '\n', true );
	long cnt = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	for ( int i = 0; i < sp.length(); ++i ) {
		if ( strchr( sp[i].c_str(), '#' ) ) continue;
		line2 = trimTailChar( sp[i], '\r' );
		line = trimTailChar( line2, ' ');
		if ( line.length() < 2 ) continue;
		uphost = makeUpperString( line );
		servobj->getDestHostPortType( uphost, host, port, destType );
		cnt = servobj->getDataCenterQueryCount( host, false );
		res += line + "    " + longToStr(cnt) + "\n";
	}
	****/
	res = cont;

	JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showDatabases( JagCfg *cfg, const JagRequest &req )
{
	AbaxDataString res;
	res = JagSchema::getDatabases( cfg, req.session->replicateType );
	JagStrSplit sp(res, '\n', true);
	if ( sp.length() < 1 ) {
		JagTable::sendMessageLength( req, " ", 1, "JV" );
		return;
	}

	for ( int i = 0; i < sp.length(); ++i ) {
		JagRecord rec;
		rec.addNameValue("TABLE_CAT", sp[i].c_str() );
		JagTable::sendMessageLength( req, rec.getSource(), rec.getLength(), "JV" );
    }
}

void JagDBServer::showTables( const JagRequest &req, const JagParseParam &pparam, 
							  const JagTableSchema *tableschema, 
							  const AbaxDataString &dbname, int objType )
{
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexesLabel( objType, dbname, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;
	vec = NULL;

	// printf("s7382 showTables %s\n", res.c_str() );

	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showTables( const JagRequest &req, const JagTableSchema *tableschema, const AbaxDataString &dbname )
{
	/***
	TABLE_CAT String => table catalog (may be null)
	TABLE_SCHEM String => table schema (may be null)
	TABLE_NAME String => table name
	TABLE_TYPE String => table type. 
         Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	***/
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( dbname, "" );
	int i, len;
	AbaxString res;

	JagRecord rrec;
	rrec.addNameValue("ISMETA", "Y");
	rrec.addNameValue("CMD", "SHOWTABLES");
	rrec.addNameValue("COLUMNCOUNT", "4");
	JagRecord colrec;
	colrec.addNameValue("TABLE_CAT", "0");
	colrec.addNameValue("TABLE_SCHEM", "0");
	colrec.addNameValue("TABLE_NAME", "0");
	colrec.addNameValue("TABLE_TYPE", "0");
	rrec.addNameValue("schema.table", colrec.getSource() );

	len = rrec.getLength();
	char buf[ 5+len];
	memcpy( buf, "m   |", 5 ); // meta data
	memcpy( buf+5, rrec.getSource(), rrec.getLength() );
	JagTable::sendMessageLength( req, buf, 5+len, "FH" );

	for ( i = 0; i < vec->size(); ++i ) {
		res = (*vec)[i];
		JagStrSplit split( res.c_str(), '.' );
		JagRecord rec;
		rec.addNameValue( "TABLE_CAT", split[0].c_str() );
		rec.addNameValue( "TABLE_SCHEM", split[0].c_str() );
		rec.addNameValue( "TABLE_NAME", split[1].c_str() );
		rec.addNameValue( "TABLE_TYPE", "TABLE" );

		JagTable::sendMessageLength( req, rec.getSource(), rec.getLength(), "JV" );
		// JV records
	}
	if ( vec ) delete vec;
	vec = NULL;

}

void JagDBServer::showIndexes( const JagRequest &req, const JagParseParam &pparam, 
							   const JagIndexSchema *indexschema, const AbaxDataString &dbtable )
{
	JagVector<AbaxString> *vec = indexschema->getAllTablesOrIndexes( dbtable, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;
	vec = NULL;

	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showAllIndexes( const JagRequest &req,  const JagParseParam &pparam, const JagIndexSchema *indexschema, 
								const AbaxDataString &dbname )
{
	JagVector<AbaxString> *vec = indexschema->getAllIndexes( dbname, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;
	vec = NULL;

	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showIndexes( const JagRequest &req, const JagIndexSchema *indexschema, const AbaxDataString &dbtable )
{
	JagTable::sendMessageLength( req, " ", 1, "JV" );
}

void JagDBServer::showTask( const JagRequest &req, JagDBServer *servobj )
{
	AbaxDataString str;
	char buf[1024];
	const AbaxPair<AbaxLong,AbaxString> *arr = servobj->_taskMap->array();
	abaxint len = servobj->_taskMap->arrayLength();
	sprintf( buf, "%14s  %20s  %16s  %16s  %16s %s\n", "TaskID", "ThreadID", "User", "Database", "StartTime", "Command");
	str += AbaxDataString( buf );
	sprintf( buf, "------------------------------------------------------------------------------------------------------------\n");
	str += AbaxDataString( buf );

	for ( abaxint i = 0; i < len; ++i ) {
		if ( ! servobj->_taskMap->isNull( i ) ) {
			const AbaxPair<AbaxLong,AbaxString> &pair = arr[i];
			JagStrSplit sp( pair.value.c_str(), '|' );
			// "threadID|userid|dbname|timestamp|query"
			sprintf( buf, "%14lu  %20s  %16s  %16s  %16s %s\n", 
				pair.key, sp[0].c_str(), sp[1].c_str(), sp[2].c_str(), sp[3].c_str(), sp[4].c_str() );
			str += AbaxDataString( buf );
		}
	}

	JagTable::sendMessageLength( req, str.c_str(), str.size(), "I_" );
}


// 0: error
// 1: OK
int JagDBServer::describeTable( int inObjType, const JagRequest &req, const JagDBServer *servobj, JagTable *ptab, 
								const JagTableSchema *tableschema, const AbaxDataString &dbtable, 
								const JagParseParam &pparam, bool showCreate )
{
	const JagSchemaRecord *record = tableschema->getAttr( dbtable );
	if ( ! record ) return 0;

	prt(("s5080 describeTable dbtable=[%s]\n", dbtable.c_str() ));
	AbaxString res;
	AbaxDataString type, dbcol, defvalStr;
	char buf[16];
	char spare4;
	int offset, len, sig, srid;
	bool enterValue = false;

	int objtype = tableschema->objectType ( dbtable );
	if ( JAG_TABLE_TYPE == inObjType ) {
		if ( objtype != JAG_TABLE_TYPE && objtype != JAG_MEMTABLE_TYPE ) {
			return 0;
		}
	} else if ( JAG_CHAINTABLE_TYPE == inObjType ) {
		if ( objtype != JAG_CHAINTABLE_TYPE ) {
			return 0;
		}
	} 

	if ( showCreate ) { res = AbaxString("create "); } 
	if ( JAG_MEMTABLE_TYPE == objtype ) {
		res += AbaxString("memtable "); 
	} else if ( JAG_CHAINTABLE_TYPE == objtype ) {
		res += AbaxString("chain "); 
	} else {
		res += AbaxString("table "); 
	}

	res += AbaxString(dbtable) + "\n";
	res += AbaxString("(\n");
	int keyMode = record->getKeyMode();
	if ( keyMode == JAG_ASC ) {
		// res += AbaxString("  key asc:\n");
		res += AbaxString("  key:\n");
	} else {
		// res += AbaxString("  key:\n");
		res += AbaxString("  key rand:\n");
	}

	int sz = record->columnVector->size();
	//prt(("s4408 record->columnVector->size=%d\n", sz ));
	for (int i = 0; i < sz; i++) {
		if ( ! pparam.detail ) {
			if ( *((*(record->columnVector))[i].spare+6) == JAG_SUB_COL ) {
				continue;
			}

			if ( 0==strncmp( (*(record->columnVector))[i].name.c_str(), "geo:", 4 ) ) {
				// geo:*** ommmit
				continue;
			}

		} else {

			if ( *((*(record->columnVector))[i].spare+6) == JAG_SUB_COL ) {
				res += AbaxString("  ");
			}
		}

		if ( showCreate && (*(record->columnVector))[i].name == "spare_" ) {
			continue;
		}

		if ( !enterValue && *((*(record->columnVector))[i].spare) == JAG_C_COL_VALUE ) {
			enterValue = true;
			res += AbaxString("  value:\n");
		}
		res += AbaxString("    ");
		res += (*(record->columnVector))[i].name + " ";
		//memset( buf, 0, 128 );
		type = (*(record->columnVector))[i].type;
		offset = (*(record->columnVector))[i].offset;
		len = (*(record->columnVector))[i].length;
		sig = (*(record->columnVector))[i].sig;
		srid = (*(record->columnVector))[i].srid;
		dbcol = dbtable + "." + (*(record->columnVector))[i].name.c_str();

		if ( *((*(record->columnVector))[i].spare+5) == JAG_KEY_MUTE ) {
			res += "mute ";
		}

		res += servobj->fillDescBuf( tableschema, (*(record->columnVector))[i], dbtable );
		spare4 =  *((*(record->columnVector))[i].spare+4);
		//prt(("\ns5623 i=%d spare+4=[%c]\n", i, spare4 ));

		if ( JAG_CREATE_DEFDATETIME == spare4 ||
			 JAG_CREATE_DEFDATE == spare4 ) {
			res += " DEFAULT CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_DEFUPDATETIME == spare4 || JAG_CREATE_DEFUPDATE == spare4 ) {
			res += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_UPDATETIME == spare4 ||
			*((*(record->columnVector))[i].spare+4) == JAG_CREATE_UPDDATE ) {
			res += " ON UPDATE CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_DEFINSERTVALUE == spare4 ) {
			defvalStr = "";
			tableschema->getAttrDefVal( dbcol, defvalStr );
			//prt(("s5301 dbcol=[%s] defvalStr=[%s]\n", dbcol.c_str(), defvalStr.c_str() ));
			if ( type == JAG_C_COL_TYPE_DBIT ) {
				res += AbaxDataString(" DEFAULT b") + defvalStr.c_str();
			} else {
				if ( *((*(record->columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM_CHAR ) {
					JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
					res += AbaxDataString(" DEFAULT ") + sp[sp.length()-1].c_str();
				} else {
					res += AbaxDataString(" DEFAULT ") + defvalStr.c_str();
				}
			}
		}

		if ( pparam.detail ) {
			res += AbaxDataString(" ") + intToStr(offset) + ":" + intToStr(len);
			#if 1
			sprintf(buf, "  %03d", i+1 );
			//res += AbaxString(buf) + " ";
			res += AbaxString(buf);
			#endif
		}
		if ( i < sz-1 ) {
			res += ",\n";
		} else {
			res += "\n";
		}
	}

	res += AbaxString(");\n");
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
	return 1;
}

void JagDBServer::sendValueData( const JagParseParam &pparm, const JagRequest &req )
{
	if ( ! pparm._lineFile ) {
		prt(("s7308 sendValueData pparm._rowHash is NULL, no data to be sent\n" ));
		return;
	}

	// get colnames
	//AbaxDataString dbtabname, value, res, json, colobj;
	AbaxDataString line;
	pparm._lineFile->startRead();

	while ( pparm._lineFile->getLine( line ) ) {
		//prt(("s7230 sendMessageLength line=%s ...\n", line.c_str() ));
		JagTable::sendMessageLength( req, line.c_str(), line.size(), "JS" );
	}
}

void JagDBServer::_describeTable( const JagRequest &req, JagTable *ptab, const JagDBServer *servobj, 
								  const AbaxDataString &dbtable, int keyOnly )
{
	/******
	TABLE_CAT String => table catalog (may be null)
	TABLE_SCHEM String => table schema (may be null)
	TABLE_NAME String => table name
	COLUMN_NAME String => column name
	DATA_TYPE int => SQL type from java.sql.Types
	TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
	COLUMN_SIZE int => column size.
	DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
	NULLABLE int => is NULL allowed.
	KEY_SEQ // 
	********/
	JagTableSchema *tableschema =  servobj->getTableSchema( req.session->replicateType );
	AbaxDataString cols = "TABLE_CAT|TABLE_SCHEM|TABLE_NAME|COLUMN_NAME|COLUMN_NAME|DATA_TYPE|TYPE_NAME|COLUMN_SIZE|DECIMAL_DIGITS|NULLABLE|KEY_SEQ";
	JagStrSplit sp( cols, '|' );

	JagRecord rrec;
	rrec.addNameValue("ISMETA", "Y");
	rrec.addNameValue("CMD", "DESCRIBETABLE");
	rrec.addNameValue("COLUMNCOUNT", intToString(sp.length()).c_str() );
	JagRecord colrec;
	for ( int i = 0; i < sp.length(); ++i ) {
		colrec.addNameValue( sp[i].c_str(), "0");
	}
	rrec.addNameValue("schema.table", colrec.getSource() );

	int len = rrec.getLength();
	char buf5[ 5+len];
	memcpy( buf5, "m   |", 5 ); // meta data
	memcpy( buf5+5, rrec.getSource(), rrec.getLength() );
	JagTable::sendMessageLength( req, buf5, 5+len, "FH" );


	const JagSchemaRecord *record = tableschema->getAttr( dbtable );
	if ( ! record ) return;
	char buf[128];
	AbaxDataString  type;
	JagStrSplit split( dbtable, '.' );
	AbaxDataString dbname = split[0];
	AbaxDataString tabname = split[1];
	int seq = 1;
	int iskey;
	char sig;

	for (int i = 0; i < record->columnVector->size(); i++) {
		JagRecord rec;
		iskey = (*(record->columnVector))[i].iskey;

		if ( iskey && (*(record->columnVector))[i].name == "_id" && 0 == i ) {
			continue;
		}

		if ( keyOnly && ! iskey ) {
			continue;
		}

		rec.addNameValue("TABLE_CAT", dbname.c_str() );
		rec.addNameValue("TABLE_SCHEM", dbname.c_str() );
		rec.addNameValue("TABLE_NAME", tabname.c_str() );
		rec.addNameValue("COLUMN_NAME", (*(record->columnVector))[i].name.c_str() );

		type = (*(record->columnVector))[i].type;
		len = (*(record->columnVector))[i].length;
		sig = (*(record->columnVector))[i].sig;
		iskey = (*(record->columnVector))[i].iskey;
		//printf("sig=[%c] %d\n", sig, sig );

		if ( type == JAG_C_COL_TYPE_STR ) {
			rec.addNameValue("DATA_TYPE", "1"); // char
			rec.addNameValue("TYPE_NAME", "CHAR"); // char
		} else if ( type == JAG_C_COL_TYPE_FLOAT ) {
			rec.addNameValue("DATA_TYPE", "6"); // float
			rec.addNameValue("TYPE_NAME", "FLOAT");
		} else if ( type == JAG_C_COL_TYPE_DOUBLE ) {
			rec.addNameValue("DATA_TYPE", "8"); // double
			rec.addNameValue("TYPE_NAME", "DOUBLE");
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			rec.addNameValue("DATA_TYPE", "92"); // time
			rec.addNameValue("TYPE_NAME", "TIME");
		} else if ( type == JAG_C_COL_TYPE_DATETIME ) {
			rec.addNameValue("DATA_TYPE", "93"); // time
			rec.addNameValue("TYPE_NAME", "DATETIME");
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
			rec.addNameValue("DATA_TYPE", "94"); // time
			rec.addNameValue("TYPE_NAME", "TIMESTAMP");
		} else if ( type == JAG_C_COL_TYPE_DINT ) {
			rec.addNameValue("DATA_TYPE", "4"); 
			rec.addNameValue("TYPE_NAME", "INTEGER");
		} else if ( type == JAG_C_COL_TYPE_DBOOLEAN ) {
			rec.addNameValue("DATA_TYPE", "-2"); 
			rec.addNameValue("TYPE_NAME", "BINARY");
		} else if ( type == JAG_C_COL_TYPE_DBIT) {
			rec.addNameValue("DATA_TYPE", "-3"); 
			rec.addNameValue("TYPE_NAME", "BIT");
		} else if ( type == JAG_C_COL_TYPE_DTINYINT ) {
			rec.addNameValue("DATA_TYPE", "-6"); 
			rec.addNameValue("TYPE_NAME", "TINYINT");
		} else if ( type == JAG_C_COL_TYPE_DSMALLINT ) {
			rec.addNameValue("DATA_TYPE", "5"); 
			rec.addNameValue("TYPE_NAME", "TINYINT");
		} else if ( type == JAG_C_COL_TYPE_DBIGINT ) {
			rec.addNameValue("DATA_TYPE", "5"); 
			rec.addNameValue("TYPE_NAME", "BIGINT");
		} else if ( type == JAG_C_COL_TYPE_DMEDINT ) {
			rec.addNameValue("DATA_TYPE", "4"); 
			rec.addNameValue("TYPE_NAME", "INTEGER");
		} else {
			rec.addNameValue("DATA_TYPE", "12"); // varchar
			rec.addNameValue("TYPE_NAME", "VARCHAR");
		}

		memset(buf, 0, 128 );
		sprintf( buf, "%d", len );
		rec.addNameValue("COLUMN_SIZE", buf ); // bytes

		sprintf( buf, "%d", sig );
		rec.addNameValue("DECIMAL_DIGITS", buf ); 
		rec.addNameValue("NULLABLE", "0" ); // 

		if ( iskey ) {
			sprintf( buf, "%d", seq++ );
			rec.addNameValue("KEY_SEQ", buf ); // 
		}

		JagTable::sendMessageLength( req, rec.getSource(), rec.getLength(), "JV" );
		// JV records
	}
}

void JagDBServer::describeIndex( const JagParseParam &pparam, const JagRequest &req, const JagDBServer *servobj, 
							     const JagIndexSchema *indexschema, 
								 const AbaxDataString &dbname, const AbaxDataString &indexName, AbaxDataString &reterr )
{
	AbaxDataString tab = indexschema->getTableName( dbname, indexName );
	if ( tab.size() < 1 ) {
		reterr = AbaxDataString("E2380 Index ") + indexName + " does not exist";
		return;
	}

	AbaxDataString dbtabidx = dbname + "." + tab + "." + indexName, dbcol;
	prt(("s2098 describeIndex dbtabidx=[%s]\n", dbtabidx.c_str() ));
	AbaxDataString schemaText = indexschema->readSchemaText( dbtabidx );
	AbaxDataString defvalStr;

	// prt(("s2838 describeIndex dbtabidx=[%s]\n", dbtabidx.c_str() ));
	if ( schemaText.size() < 1 ) {
		reterr = AbaxDataString("E2080 Index ") + indexName + " does not exist";
		return;
	}

	JagSchemaRecord record;
	record.parseRecord( schemaText.c_str() );

	AbaxString res;
	AbaxDataString type;
	int len, sig, srid, offset;
	bool enterValue = false;

	int isMemIndex = indexschema->isMemTable( dbtabidx );
	if ( isMemIndex ) {
		res = AbaxString("memindex ") + AbaxString(dbtabidx) + "\n";
	} else {
		res = AbaxString(dbtabidx) + "\n";
	}

	res += AbaxString("(\n");
	res += AbaxString("  key:\n");
	int sz = record.columnVector->size();
	for (int i = 0; i < sz; i++) {
		if ( ! pparam.detail ) {
			if ( *((*(record.columnVector))[i].spare+6) == JAG_SUB_COL ) {
				continue;
			}
		} else {
			if ( *((*(record.columnVector))[i].spare+6) == JAG_SUB_COL ) {
				res += AbaxString("  ");
			}
		}

		if ( !enterValue && *((*(record.columnVector))[i].spare) == JAG_C_COL_VALUE ) {
			enterValue = true;
			res += AbaxString("  value:\n");
		}
		dbcol = dbtabidx + "." + (*(record.columnVector))[i].name.c_str();
		res += AbaxString("    ");
		res += (*(record.columnVector))[i].name + " ";
		type = (*(record.columnVector))[i].type;
		offset = (*(record.columnVector))[i].offset;
		len = (*(record.columnVector))[i].length;
		sig = (*(record.columnVector))[i].sig;
		srid = (*(record.columnVector))[i].srid;

		res += servobj->fillDescBuf( indexschema, (*(record.columnVector))[i], dbtabidx );

		if ( *((*(record.columnVector))[i].spare+4) == JAG_CREATE_DEFDATETIME ||
			 *((*(record.columnVector))[i].spare+4) == JAG_CREATE_DEFDATE ) {
			res += " DEFAULT CURRENT_TIMESTAMP";
		} else if ( *((*(record.columnVector))[i].spare+4) == JAG_CREATE_DEFUPDATETIME ||
					*((*(record.columnVector))[i].spare+4) == JAG_CREATE_DEFUPDATE ) {
			res += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
		} else if ( *((*(record.columnVector))[i].spare+4) == JAG_CREATE_UPDATETIME ||
					*((*(record.columnVector))[i].spare+4) == JAG_CREATE_UPDDATE ) {
			res += " ON UPDATE CURRENT_TIMESTAMP";
		} else if ( *((*(record.columnVector))[i].spare+4) == JAG_CREATE_DEFINSERTVALUE ) {
			defvalStr = "";
			indexschema->getAttrDefVal( dbcol, defvalStr );
			if ( type == JAG_C_COL_TYPE_DBIT ) {
				res += AbaxDataString(" DEFAULT b") + defvalStr.c_str();
			} else {
				if ( *((*(record.columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM_CHAR ) {
					JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
					res += AbaxDataString(" DEFAULT ") + sp[sp.length()-1].c_str();
				} else {
					res += AbaxDataString(" DEFAULT ") + defvalStr.c_str();
				}
			}
		}

		if ( pparam.detail ) {
			res += AbaxDataString(" ") + intToStr(offset) + ":" + intToStr(len);
		}
		
		if ( i < sz-1 ) {
			res += ",\n";
		} else {
			res += "\n";
		}
	}

	res += AbaxString(");\n");
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

// static Task for a thread
// sequential
void *JagDBServer::makeTableObjects( void *servptr )
{
	JagDBServer *servobj = (JagDBServer*)servptr;

	AbaxString dbtab;
	JagRequest req;
	JagSession session( g_dtimeout );
	req.session = &session;
	session.servobj = servobj;
	time_t t1 = time(NULL);
	raydebug( stdout, JAG_LOG_HIGH, "Initialing all tables ...\n");

	JagParseParam pparam;
	ObjectNameAttribute objNameTemp;
	objNameTemp.init();
	pparam.objectVec.append(objNameTemp);
	pparam.optype = 'C';
	pparam.opcode = JAG_CREATETABLE_OP;
	
	JagTable *ptab;
	JagTableSchema *tableschema;
	req.session->replicateType = 0;
	tableschema = servobj->_tableschema;
	JagVector<AbaxString> *vec = servobj->_tableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = servobj->_objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
			pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) ptab->buildInitIndexlist();
		if ( ptab ) servobj->_objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
						pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	}
	delete vec;

	req.session->replicateType = 1;
	tableschema = servobj->_prevtableschema;
	vec = servobj->_prevtableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = servobj->_objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
			pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) ptab->buildInitIndexlist();
		if ( ptab ) servobj->_objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
						pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	}
	delete vec;

	req.session->replicateType = 2;
	tableschema = servobj->_nexttableschema;
	vec = servobj->_nexttableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = servobj->_objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
			pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) ptab->buildInitIndexlist();
		if ( ptab ) servobj->_objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
			pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	}
	delete vec;

	time_t  t2 = time(NULL);
	int min = (t2-t1)/60;
	int sec = (t2-t1) % 60;
	raydebug( stdout, JAG_LOG_LOW, "Initialized all %d tables in %d minutes %d seconds\n", vec->size(), min, sec );

   	return NULL;
}

// statis
void *JagDBServer::monitorRemoteBackup( void *ptr )
{
	JagPass *jp = (JagPass*)ptr;
	int period;
	AbaxDataString bcastCmd;
	AbaxFixString data;
	JagRequest req;

	while ( 1 ) {
		period = atoi(jp->servobj->_cfg->getValue("REMOTE_BACKUP_INTERVAL", "30").c_str());
		jagsleep(period, JAG_SEC);
		jp->servobj->processRemoteBackup( "_ex_procremotebackup", req );
	}

	delete jp;
   	return NULL;
}

// thread for local doRemoteBackup on host0
// static
void * JagDBServer::threadRemoteBackup( void *ptr )
{
	JagPass *jp = (JagPass*)ptr;
	AbaxDataString rmsg = AbaxDataString("_serv_doremotebackup|") + jp->ip + "|" + jp->passwd;
	JagRequest req;
	jp->servobj->doRemoteBackup( rmsg.c_str(), req );
	delete jp;
	return NULL;
}

// static
// Get localhost IP address. In case there are N IP for this host (many ether cards)
// it matches the right one in the primary host list
AbaxDataString JagDBServer::getLocalHostIP( JagDBServer *servobj, const AbaxDataString &hostips )
{
	JagVector<AbaxDataString> vec;
	JagNet::getLocalIPs( vec );
	
	//printf("s4931 local host IPs:\n");
	//vec.printString();
	raydebug( stdout, JAG_LOG_HIGH, "Local Interface IPs [%s]\n", vec.asString().c_str() );

	JagStrSplit sp( hostips, '|' );
	for ( int i = 0; i < sp.length(); ++i ) {
		if ( servobj->_listenIP.size() > 0 ) {
			if ( servobj->_listenIP == sp[i] ) {
				raydebug( stdout, JAG_LOG_HIGH, "Local Listen IP [%s]\n", sp[i].c_str() );
				return sp[i];
			}
		} else {
			if ( vec.exist( sp[i] ) ) {
				raydebug( stdout, JAG_LOG_HIGH, "Local IP [%s]\n", sp[i].c_str() );
				return sp[i];
			}
		}
	}

	return "";
}

AbaxDataString JagDBServer::getBroadCastRecoverHosts( int replicateCopy )
{
	AbaxDataString hosts;
	int pos1, pos2, pos3, pos4;
	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	if ( _nthServer == 0 ) {
		pos1 = _nthServer+1;
		pos2 = sp.length()-1;
	} else if ( _nthServer == sp.length()-1 ) {
		pos1 = 0;
		pos2 = _nthServer-1;
	} else {
		pos1 = _nthServer+1;
		pos2 = _nthServer-1;
	}
	if ( pos1 == sp.length()-1 ) {
		pos3 = 0;
	} else {
		pos3 = pos1+1;
	}
	if ( pos2 == 0 ) {
		pos4 = sp.length()-1;
	} else {
		pos4 = pos2-1;
	}

	// check for repeat pos, set to -1
	if ( pos1 == _nthServer ) pos1 = -1;
	if ( pos2 == _nthServer || pos2 == pos1 ) pos2 = -1;
	if ( pos3 == _nthServer || pos3 == pos1 || pos3 == pos2 ) pos3 = -1;
	if ( pos4 == _nthServer || pos4 == pos1 || pos4 == pos2 || pos4 == pos3 ) pos4 = -1;

	hosts = sp[_nthServer];
	if ( 2 == replicateCopy ) { // one replicate copy, broadcast to self and right one server
		if ( pos1 >= 0 ) hosts += AbaxDataString("|") + sp[pos1];
	} else if ( 3 == replicateCopy ) { 
		if ( pos1 >= 0 ) hosts += AbaxDataString("|") + sp[pos1];
		if ( pos2 >= 0 ) hosts += AbaxDataString("|") + sp[pos2];
		if ( pos3 >= 0 ) hosts += AbaxDataString("|") + sp[pos3];
		if ( pos4 >= 0 ) hosts += AbaxDataString("|") + sp[pos4];
	}

	raydebug(stdout, JAG_LOG_LOW, "BroadCastRecoverHosts [%s]\n", hosts.c_str() );
	return hosts;
}	

// static
void  JagDBServer::noLinger( const JagRequest &req )
{
	JAGSOCK sock = req.session->sock;
	struct linger so_linger;
	so_linger.l_onoff = 1;
	so_linger.l_linger = 0;
	setsockopt( sock , SOL_SOCKET, SO_LINGER, (CHARPTR)&so_linger, sizeof(so_linger) );
}


// object method: do initialization for server
int JagDBServer::mainInit( int argc, char *argv[] )
{
    int rc;
    int len;
	AbaxString  ports;
	AbaxString  cs;

	raydebug( stdout, JAG_LOG_LOW, "Initializing main ...\n");
	
	// make dbconnector connections first
	raydebug( stdout, JAG_LOG_LOW, "Make init connection from %s ...\n", _localInternalIP.c_str() ); 
	_dbConnector->makeInitConnection( _debugClient );
	raydebug( stdout, JAG_LOG_LOW, "Done init connection from %s\n", _localInternalIP.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "Debug dbConnector %d\n", _debugClient );

	// remove the blockindex-disk files
	// removeAllBlockIndexInDisk();
	JagRequest req;
	JagSession session( g_dtimeout );
	req.session = &session;
	session.replicateType = 0;
	session.servobj = this;
	refreshSchemaInfo( req, this, g_lastSchemaTime );
	char *rp = getenv("DO_RECOVER");
	char *rp2 = getenv("NEW_DATACENTER");

	// recover un-flushed insert data
	if ( rp ) {
		raydebug(stdout, JAG_LOG_LOW, "begin recover wallog ...\n");
		AbaxDataString walpath = JagFileMgr::getLocalLogDir("cmd") + "/walactive.log";
		recoverWalLog( walpath );
		walpath = JagFileMgr::getLocalLogDir("cmd") + "/walbackup.log";
		recoverWalLog( walpath );
		raydebug(stdout, JAG_LOG_LOW, "end recover wallog\n");
	}
	resetWalLog();

	// recover un-flushed dinsert data
	if ( rp ) {
		raydebug(stdout, JAG_LOG_LOW, "begin recover dinsertlog ...\n");
		AbaxDataString dinpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertactive.log";
		recoverDinsertLog( dinpath );
		dinpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertbackup.log";
		recoverDinsertLog( dinpath );
		raydebug(stdout, JAG_LOG_LOW, "end recover dinsertlog\n");
	}
	resetDinsertLog();

	// set recover flag, check to see if need to do drecover ( delta log recovery, if hard disk is ok )
	// or do crecover ( tar copy file, if hard disk is brand new )
	AbaxDataString recoverCmd, bcasthosts = getBroadCastRecoverHosts( _faultToleranceCopy );
	_crecoverFpath = "";
	_prevcrecoverFpath = "";
	_nextcrecoverFpath = "";
	_walrecoverFpath = "";
	_prevwalrecoverFpath = "";
	_nextwalrecoverFpath = "";

	int isCrecover = 0;

	// first broadcast crecover to get package
	if ( rp ) {
		// ask to do crecover
		_dbConnector->broadCastSignal( "_serv_crecover", bcasthosts );
		recoveryFromTransferredFile();
		// wait for this socket to be ready to recv data from client
		// then request delta data from neighbor
		raydebug( stdout, JAG_LOG_LOW, "end broadcast\n" );
	}

	recoverRegSpLog(); // regular data and metadata redo
	// prt(("s7677 after recover reg\n"));
	
	// next, if is new datacenter, ask original datacenter to broadcast old data to here
	if ( rp2 ) {
		_newdcTrasmittingFin = 0;
		if ( _isGate ) requestSchemaFromDataCenter();
		raydebug( stdout, JAG_LOG_LOW, "end request from datacenter\n" );
	} else {
		_newdcTrasmittingFin = 1;
	}

	/***
	// remove the blockindex-disk files
	// removeAllBlockIndexInDisk();
	JagRequest req;
	JagSession session;
	req.session = &session;
	session.replicateType = 0;
	session.servobj = this;
	refreshSchemaInfo( req, this, g_lastSchemaTime );
	// recover un-flushed insert data
	recoverLog();
	***/
	
	// create thread to monitor logged incoming insert/update/delete commands
	// also create and open delta logs for replicate
	JagFileMgr::makeLocalLogDir("cmd");
	JagFileMgr::makeLocalLogDir("delta");

	/***
	if ( _walLog ) {
		resetLog();
    	pthread_t  threadmo;
		JagSession *session = new JagSession();
		session->servobj = this;
		session->uid = "sys";
		session->dbname = "log";
		raydebug( stdout, JAG_LOG_LOW, "Initializing thread to flush DML commands\n");
    	jagpthread_create( &threadmo, NULL, monitorLog, (void*)session );
    	pthread_detach( threadmo );
	}
	***/

	AbaxDataString cs1 = _cfg->getValue("REMOTE_BACKUP_SERVER", "" );
	AbaxDataString cs2 = _cfg->getValue("REMOTE_BACKUP_INTERVAL", "0" );
	int interval = atoi( cs2.c_str() );
	if ( interval > 0 && interval < 10 ) interval = 10;
	int ishost0 = _dbConnector->_nodeMgr->_isHost0OfCluster0;
	if ( ishost0 && cs1.length()>0 && interval > 0 ) {
    	pthread_t  threadmo;
		JagPass *jp = new JagPass();
		jp->servobj = this;
		raydebug( stdout, JAG_LOG_LOW, "Initializing thread for remote backup\n");
    	jagpthread_create( &threadmo, NULL, monitorRemoteBackup, (void*)jp );
    	pthread_detach( threadmo );
	}

	return 1;
}

int JagDBServer::mainClose()
{
	pthread_mutex_destroy( &g_dbmutex );
	pthread_mutex_destroy( &g_flagmutex );
	pthread_mutex_destroy( &g_logmutex );
	pthread_mutex_destroy( &g_dlogmutex );
	pthread_mutex_destroy( &g_dinsertmutex );
	pthread_mutex_destroy( &g_datacentermutex );
	pthread_mutex_destroy( &g_selectmutex );

	// thpool_destroy( _threadPool );
}

// object method: make thread groups
int JagDBServer::makeThreadGroups( int threadGroups, int threadGroupNum )
{
	prt(("s4912 Begin makeThreadGroups grpnum=%lld thrdgrps=%lld ...\n", threadGroupNum, _threadGroups ));
	this->_threadGroupNum = threadGroupNum;
	pthread_t thr[threadGroups];
	for ( int i = 0; i < threadGroups; ++i ) {
		this->_groupSeq = i;
    	jagpthread_create( &thr[i], NULL, threadGroupTask, (void*)this );
    	pthread_detach( thr[i] );
	}

	// printf("s4913 done makeThreadGroups\n");
}

// static method
void* JagDBServer::threadGroupTask( void *servptr )
{
	JagDBServer *servobj = (JagDBServer*)servptr;
	abaxint threadGroupNum = (abaxint)servobj->_threadGroupNum;
	int grpSeq = (abaxint)servobj->_groupSeq;
	JAGSOCK sock = servobj->_sock;
    struct sockaddr_in cliaddr;
    socklen_t clilen;
	JAGSOCK connfd;
	int  	rc;
	int     timeout = 600;
	int     lasterrno = -1;
	abaxint     toterrs = 0;
	
	++ servobj->_activeThreadGroups;

	// threadpool thpool = thpool_init( servobj->_threadGroupSize );
    listen( servobj->_sock, 30);

    for(;;) {
		servobj->_dumfd = dup2( jagopen("/dev/null", O_RDONLY ), 0 );
        clilen=sizeof(cliaddr);

		memset(&cliaddr, 0, clilen );
        connfd = accept( servobj->_sock, (struct sockaddr *)&cliaddr, ( socklen_t* )&clilen);
		timeout = 30;
		#if 0
		prt(("s9989 accept connfd=%d ip=[%s] errno=%d thrdGrpN=%d grpSeq=%d\n", 
			connfd, inet_ntoa( cliaddr.sin_addr ), errno, threadGroupNum, grpSeq ));
		#endif
		if ( connfd < 0 ) {
			if ( errno == lasterrno ) {
				if ( toterrs < 10 ) {
					raydebug( stdout, JAG_LOG_LOW, "E3234 accept error connfd=%d errno=%d (%s)\n", connfd, errno, strerror(errno) );
				} else if ( toterrs > 1000000 ) {
					toterrs = 0;
					raydebug( stdout, JAG_LOG_LOW, "E3258 accept error connfd=%d errno=%d (%s)\n", connfd, errno, strerror(errno) );
				}
			} else {
				raydebug( stdout, JAG_LOG_LOW, "E3334 accept error connfd=%d errno=%d (%s)\n", connfd, errno, strerror(errno) );
				toterrs = 0;
			}

			lasterrno = errno;
			++ toterrs;
			jagsleep(3, JAG_SEC);
			continue;
		}
		// prt(("s8042 accept connfd=%d client=[%s]\n", connfd, inet_ntoa( cliaddr.sin_addr ) ));

		if ( 0 == connfd ) {
			jagclose( connfd );
			prt(("s5081 accepted connfd=0, ignore\n"));
			jagsleep(1, JAG_SEC);
			continue;
		}

		++ servobj->_connections;

		//JagPass *passmo = new JagPass();
		JagPass *passmo = newObject<JagPass>();
		passmo->sock = connfd;
		passmo->servobj = servobj;
		passmo->ip = inet_ntoa( cliaddr.sin_addr );

		// dbg(("s3301 accepetd sock=%d add work threadTask lastThread=%d ...\n", connfd, lastThread ));
		// prt(("s9999 threadTask passmo=%0x\n", passmo));
		// rc = thpool_add_work(thpool, oneClientThreadTask, (void*)passmo );
		oneClientThreadTask( (void*)passmo );  // loop inside

		// prt(("s5088 threadGroupNum=%d _activeClients=%d\n", threadGroupNum, (abaxint)servobj->_activeClients ));
		if ( threadGroupNum > 0 && abaxint(servobj->_activeClients) * 100 <= servobj->_activeThreadGroups * 40  ) {
			// raydebug( stdout, JAG_LOG_LOW, "c9499 threadGroup=%l idling quit\n", threadGroupNum );
			break;
		}

	}
	
	/***
	prt(("s0293 thpool_wait ...\n" ));
	thpool_wait( thpool );
	prt(("s0293 thpool_wait done\n" ));
	thpool_destroy( thpool );
	***/

	-- servobj->_activeThreadGroups;
   	raydebug( stdout, JAG_LOG_LOW, "s2894 thread group=%l done, activeThreadGroups=%l\n", 
				threadGroupNum, (abaxint)servobj->_activeThreadGroups );

	return NULL;
}


// auth user
// 0: error
// 1: OK
// static
bool JagDBServer::doAuth( const JagRequest &req, JagDBServer *servobj, char *pmesg, char *&saveptr, 
							JagSession &session )
{
	// prt(("s3032 doauth pmesg=[%s]\n", pmesg ));
	// check client IP address
	int ipOK = 0;
	pthread_rwlock_rdlock( & servobj->_aclrwlock );
	if ( servobj->_whiteIPList->size() < 1 && servobj->_blackIPList->size() < 1 ) {
		// no witelist and blacklist setup, pass
		ipOK = 1;
	} else {
		if ( servobj->_whiteIPList->match( req.session->ip ) ) {
			if ( ! servobj->_blackIPList->match(  req.session->ip ) ) {
				ipOK = 1;
			}
		}
	}

	pthread_rwlock_unlock( &servobj->_aclrwlock );
	if ( ! ipOK ) {
    	JagTable::sendMessage( req, "_END_[T=579|E=Error client IP address]", "ER" );
		raydebug( stdout, JAG_LOG_HIGH, "client IP (%s) is blocked\n", req.session->ip.c_str() );
		return false;
	}

	// prt(("s2601 doAuth pmesg=[%s]\n", pmesg ));

	JagStrSplit sp(pmesg, '|' );
	if ( sp.length() < 9 ) {
		JagTable::sendMessage( req, "_END_[T=20|E=Error Auth]", "ER" );
		// prt(("s5403 pmesg=[%s] erroorauth\n", pmesg ));
		return false;
	}

	AbaxDataString uid = sp[1];
	AbaxDataString encpass = sp[2];
	AbaxDataString pass = JagDecryptStr( servobj->_privateKey, encpass );
	session.timediff = atoi( sp[3].c_str() );
	session.origserv = atoi( sp[4].c_str() );
	session.replicateType = atoi( sp[5].c_str() );
	session.drecoverConn = atoi( sp[6].c_str() );
	session.unixSocket = sp[7];
	session.samePID = atoi( sp[8].c_str() );
	session.cliPID = sp[8];
	session.uid = uid;
	session.passwd = pass;
	if ( getpid() == session.samePID ) session.samePID = 1;
	else session.samePID = 0;

	if ( ! servobj->_serverReady && ! session.origserv ) {
		JagTable::sendMessage( req, "_END_[T=20|E=Server not ready, please try later]", "ER" );
		return false;
	}
	
	JagHashMap<AbaxString, AbaxString> hashmap;
	convertToHashMap( sp[7], '/', hashmap);
	AbaxString exclusive, clear, checkip;
	session.exclusiveLogin = 0;

	if ( uid == "admin" ) {
    	if ( hashmap.getValue("clearex", clear ) ) {
    		if ( clear == "yes" || clear == "true" || clear == "1" ) {
        		servobj->_exclusiveAdmins = 0;
        		raydebug( stdout, JAG_LOG_LOW, "admin cleared exclusive from %s\n", session.ip.c_str() );
			}
		}

		session.datacenter = false;
    	if ( hashmap.getValue("DATACENTER", clear ) ) {
    		if ( clear == "1" ) {
				session.datacenter = true;
        		// raydebug( stdout, JAG_LOG_LOW, "admin from datacenter\n" );
			}
		}

		session.dcfrom = 0;
		if ( hashmap.getValue("DATC_FROM", clear ) ) {
			if ( clear == "GATE" ) {
				session.dcfrom = JAG_DATACENTER_GATE;
			} else if ( clear == "HOST" ) {
				session.dcfrom = JAG_DATACENTER_HOST;
			}
		}

		session.dcto = 0;
		if ( hashmap.getValue("DATC_TO", clear ) ) {
			if ( clear == "GATE" ) {
				session.dcto = JAG_DATACENTER_GATE;
			} else if ( clear == "HOST" ) {
				session.dcto = JAG_DATACENTER_HOST;
			} else if ( clear == "PGATE" ) {
				session.dcto = JAG_DATACENTER_PGATE;
			}
		}

    	if ( hashmap.getValue("exclusive", exclusive ) ) {
    		if ( exclusive == "yes" || exclusive == "true" || exclusive == "1" ) {
				session.exclusiveLogin = 1;
				if ( session.replicateType == 0 ) {
					// main connection accept exclusive login, other backup connection ignore it
					if ( servobj->_exclusiveAdmins > 0 ) {
						JagTable::sendMessage( req, "_END_[T=120|E=Failed to login in exclusive mode]", "ER" );
						return false;
					}
				}
        	}
    	}  
	}

	/***
	if ( ! servobj->_newdcTrasmittingFin && ! session.datacenter && ! session.samePID ) {
		JagTable::sendMessage( req, "_END_[T=20|E=Server not ready for datacenter, please try later]", "ER" );
		return false;
	}
	***/

	// prt(("s4418 exclusiveAdmins=%d\n", (int)servobj->_exclusiveAdmins ));
	// dbg(("s9403 uid=[%s]  pass=[%s]\n", uid.c_str(), pass.c_str() ));

 	JagUserID *userDB;
	if ( session.replicateType == 0 ) userDB = servobj->_userDB;
	else if ( session.replicateType == 1 ) userDB = servobj->_prevuserDB;
	else if ( session.replicateType == 2 ) userDB = servobj->_nextuserDB;
	// check password first
	bool isGood = false;
	if ( userDB ) {
		AbaxString dbpass = userDB->getValue( uid, JAG_PASS );
		char *md5 = MDString( pass.c_str() );
		AbaxString md5pass = md5;
		if ( md5 ) free( md5 );
		if ( dbpass == md5pass && dbpass.size() > 0 ) {
			isGood = true;
		}
	}
	
	if ( ! isGood ) {
		AbaxString servToken;
    	if ( hashmap.getValue("TOKEN", servToken ) ) {
			if ( servToken == servobj->_servToken ) {
				// prt(("s7263 servToken match\n"));
				isGood = true;
			}
		} 
	}

	// prt(("s8822 isGood=%d\n", isGood ));
	if ( ! isGood ) {
       	JagTable::sendMessage( req, "_END_[T=20|E=Error password or token]", "ER" );
       	raydebug( stdout, JAG_LOG_LOW, "Connection from %s, Error password or TOKEN\n", session.ip.c_str() );
       	return false;
	}

	if ( uid == "admin" ) {
		if ( session.exclusiveLogin ) {
        	raydebug( stdout, JAG_LOG_LOW, "Exclusive admin connected from %s\n", session.ip.c_str() );
        	servobj->_exclusiveAdmins = 1;
		}
	}
	
	if ( ! session.origserv && 0 == session.replicateType ) {
    	raydebug( stdout, JAG_LOG_LOW, "user %s connected from %s\n", uid.c_str(), session.ip.c_str() );
	}


	// prt(("s5091 sending client OK ...\n"));
	AbaxDataString oksid = "OK ";
	oksid += longToStr(  pthread_self() );
	oksid += AbaxDataString(" 1 ") + servobj->_dbConnector->_nodeMgr->_sendNodes + " ";
   	// raydebug( stdout, JAG_LOG_LOW, "sendNodes(%s)\n", servobj->_dbConnector->_nodeMgr->_sendNodes.c_str() );
	oksid += longToStr( servobj->_nthServer ) + " " + longToStr( servobj->_faultToleranceCopy );
	if ( session.samePID ) oksid += AbaxDataString(" 1 ");
	else oksid += AbaxDataString(" 0 ");
	oksid += longToStr( session.exclusiveLogin );
	oksid += AbaxDataString(" ") + servobj->_cfg->getValue("HASHJOIN_TABLE_MAX_RECORDS", "500000");
	oksid += AbaxDataString(" ") + intToStr( servobj->_isGate );

	JagTable::sendMessageLength( req, oksid.c_str(), oksid.size(), "CD" );
	JagTable::sendMessage( req, "_END_[T=20|E=]", "ED" );
	session.uid = uid;

	// set timer for session if recover connection is not 2 ( clean recover )
	if ( session.drecoverConn != 2 && !session.samePID ) session.createTimer();
	// prt(("s9999 fin doauth\n"));

	// prt(("s7734 _threadMap->addKeyValue( %lld ) \n", AbaxLong(THREADID) ));
	// servobj->_threadMap->addKeyValue( AbaxLong(THREADID), time(NULL) );

	return true;
}

// static
// method for make connection use only
bool JagDBServer::useDB( const JagRequest &req, JagDBServer *servobj, char *pmesg, char *&saveptr )
{
	raydebug( stdout, JAG_LOG_HIGH, "%s\n", pmesg );

	char *tok;
	char dbname[JAG_MAX_DBNAME+1];

	// prt(("s9999 usedb1\n"));
	tok = strtok_r( pmesg, " ", &saveptr );
	tok = strtok_r( NULL, " ;", &saveptr );
	if ( ! tok ) {
		// prt(("s9999 usedb2\n"));
		JagTable::sendMessage( req, "_END_[T=20|E=Database empty]", "ER" );
	} else {
		// prt(("s9999 usedb3\n"));
		if ( strlen( tok ) > JAG_MAX_DBNAME ) {
			// prt(("s9999 usedb4\n"));
			JagTable::sendMessage( req, "_END_[T=20|E=Database too long]", "ER" );
		} else {
			// prt(("s9999 usedb5\n"));
    		strcpy( dbname, tok );
			tok = strtok_r( NULL, " ;", &saveptr );
			if ( tok ) {
				// prt(("s9999 usedb6\n"));
				// JagTable::sendMessage( req, "_END_[T=20|E=Use database syntax error]", "ED" );
				JagTable::sendMessage( req, "_END_[T=20|E=Use database syntax error]", "ER" );
			} else {
				// prt(("s9999 usedb7\n"));
				if ( 0 == strcmp( dbname, "test" ) ) {
					req.session->dbname = dbname;
					JagTable::sendMessage( req, "Database changed", "OK" );
					JagTable::sendMessage( req, "_END_[T=20|E=]", "ED" );
					return true;
				}
    			AbaxDataString jagdatahome = servobj->_cfg->getJDBDataHOME( req.session->replicateType );
    			AbaxDataString fpath = jagdatahome + "/" + dbname;
				// prt(("s8189 usedb(%s) fpath=[%s]\n", dbname, fpath.c_str() ));
    			if ( 0 == jagaccess( fpath.c_str(), X_OK )  ) {
					// prt(("s9999 usedb9\n"));
    				req.session->dbname = dbname;
    				JagTable::sendMessage( req, "Database changed", "OK" );
    				JagTable::sendMessage( req, "_END_[T=20|E=]", "ED" );
    			} else {
					// prt(("s9999 usedb10\n"));
    				//JagTable::sendMessage( req, "Database does not exist", "OK" );
    				//JagTable::sendMessage( req, "_END_[T=40|E=Database does not exist]", "ED" );
    				// JagTable::sendMessage( req, "_END_[T=40|E=Database does not exist]", "EE" );
    				JagTable::sendMessage( req, "_END_[T=20|E=Database does not exist]", "ER" );
    			}
			}
		}
	}

	return true;
}

// method to refresh schemaMap and schematime for dbConnector
void JagDBServer::refreshSchemaInfo( const JagRequest &req, JagDBServer *servobj, abaxint &schtime )
{
	AbaxDataString schemaInfo;

	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	AbaxDataString obj;
	AbaxString recstr;

	// tables
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( "", "");
	if ( vec ) {
    	for ( int i = 0; i < vec->size(); ++i ) {
    		obj = (*vec)[i].c_str();
    		if ( tableschema->getAttr( obj, recstr ) ) {
    			schemaInfo += AbaxDataString( obj.c_str() ) + ":" + recstr.c_str() + "\n";
				// prt(("s3901 tableschema  %s:%s\n",  obj.c_str(), recstr.c_str() ));
    		} 
    	}
    	if ( vec ) delete vec;
    	vec = NULL;
	}

	// indexes
	vec = indexschema->getAllTablesOrIndexes( "", "" );
	if ( vec ) {
    	for ( int i = 0; i < vec->size(); ++i ) {
    		obj = (*vec)[i].c_str();
    		if ( indexschema->getAttr( obj, recstr ) ) {
    			schemaInfo += AbaxDataString( obj.c_str() ) + ":" + recstr.c_str() + "\n"; 
				// prt(("s3902 indexschema  %s:%s\n",  obj.c_str(), recstr.c_str() ));
    		}
    	}
    	if ( vec ) delete vec;
    	vec = NULL;
	}

	// prt(("s8393 schemaInfo=[%s]\n", schemaInfo.c_str() ));
	servobj->_dbConnector->_parentCliNonRecover->_mapLock->writeLock( -1 );
	servobj->_dbConnector->_parentCliNonRecover->rebuildSchemaMap();
	servobj->_dbConnector->_parentCliNonRecover->updateSchemaMap( schemaInfo.c_str() );
	servobj->_dbConnector->_parentCliNonRecover->_mapLock->writeUnlock( -1 );

	servobj->_dbConnector->_parentCli->_mapLock->writeLock( -1 );
	servobj->_dbConnector->_parentCli->rebuildSchemaMap();
	servobj->_dbConnector->_parentCli->updateSchemaMap( schemaInfo.c_str() );
	servobj->_dbConnector->_parentCli->_mapLock->writeUnlock( -1 );

	servobj->_dbConnector->_broadCastCli->_mapLock->writeLock( -1 );
	servobj->_dbConnector->_broadCastCli->rebuildSchemaMap();
	servobj->_dbConnector->_broadCastCli->updateSchemaMap( schemaInfo.c_str() );
	servobj->_dbConnector->_broadCastCli->_mapLock->writeUnlock( -1 );

	struct timeval now;
	gettimeofday( &now, NULL );
	schtime = now.tv_sec * (abaxint)1000000 + now.tv_usec;
}

// Reload ACL list
void JagDBServer::refreshACL( int bcast )
{
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		return;
	}

	loadACL();

	if ( bcast ) {
		AbaxDataString bcastCmd = "_serv_refreshacl|" + _whiteIPList->_data+ "|" + _blackIPList->_data;
		_dbConnector->broadCastSignal( bcastCmd, "" );
	}
}

// Load ACL list
void JagDBServer::loadACL()
{
	AbaxDataString fpath = jaguarHome() + "/conf/whitelist.conf";
	//raydebug( stdout, JAG_LOG_LOW, "Check %s\n", fpath.c_str() );
	pthread_rwlock_wrlock(&_aclrwlock);
	if ( _whiteIPList ) delete _whiteIPList;
	_whiteIPList = new JagIPACL( fpath );

	fpath = jaguarHome() + "/conf/blacklist.conf";
	//raydebug( stdout, JAG_LOG_LOW, "Check %s\n", fpath.c_str() );
	if ( _blackIPList ) delete _blackIPList;
	_blackIPList = new JagIPACL( fpath );
	pthread_rwlock_unlock(&_aclrwlock);

}

// back up schema and other meta data
// seq should be every minute
void JagDBServer::doBackup( uabaxint seq )
{
	// use _cfg
	AbaxDataString cs = _cfg->getValue("LOCAL_BACKUP_PLAN", "" );
	// prt(("s3748 _cfg=%0x BACKUP_PLAN=[%s] seq=%lld\n", _cfg, cs.c_str(), seq ));
	if ( cs.length() < 1 ) {
		return;
	}

	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		return;
	}

	if ( _restartRecover ) {
		return;
	}

	JagStrSplit sp( cs, '|' );
	AbaxDataString  rec;
	AbaxDataString  bcastCmd, bcasthosts;

	// copy meta data to 15min directory
	if ( ( seq%15 ) == 0 && sp.contains( "15MIN", rec ) ) {
		copyData( rec, false );
		bcastCmd = "_serv_copydata|" + rec + "|0";
		_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to hourly directory
	if ( ( seq % 60 ) == 0 && sp.contains( "HOURLY", rec ) ) {
		copyData( rec, false );
		bcastCmd = "_serv_copydata|" + rec + "|0";
		_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to daily directory
	if ( ( seq % 1440 ) == 0 && sp.contains( "DAILY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to weekly directory
	if ( ( seq % 10080 ) == 0 && sp.contains( "WEEKLY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to monthly directory
	if ( ( seq % 43200 ) == 0 && sp.contains( "MONTHLY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 
	}
}

// back up schema and other meta data
#ifndef _WINDOWS64_
#include <sys/sysinfo.h>
void JagDBServer::writeLoad( uabaxint seq )
{
	if ( ( seq%15) != 0 ) {
		return;
	}

	struct sysinfo info;
    sysinfo( &info );

	abaxint usercpu, syscpu, idle;
	_jagSystem.getCPUStat( usercpu, syscpu, idle );

	AbaxDataString userCPU = ulongToString( usercpu );
	AbaxDataString sysCPU = ulongToString( syscpu );

	AbaxDataString totrams = ulongToString( info.totalram / ONE_GIGA_BYTES );
	AbaxDataString freerams = ulongToString( info.freeram / ONE_GIGA_BYTES );
	AbaxDataString totswaps = ulongToString( info.totalswap / ONE_GIGA_BYTES );
	AbaxDataString freeswaps = ulongToString( info.freeswap / ONE_GIGA_BYTES );
	AbaxDataString procs = intToString( info.procs );

	uabaxint diskread, diskwrite, netread, netwrite;
	JagNet::getNetStat( netread, netwrite );
	JagFileMgr::getIOStat( diskread, diskwrite );
	AbaxDataString netReads = ulongToString( netread /ONE_GIGA_BYTES );
	AbaxDataString netWrites = ulongToString( netwrite /ONE_GIGA_BYTES );
	AbaxDataString diskReads = ulongToString( diskread  );
	AbaxDataString diskWrites = ulongToString( diskwrite  );

	AbaxDataString nselects = ulongToString( numSelects );
	AbaxDataString ninserts = ulongToString( numInserts );
	AbaxDataString nupdates = ulongToString( numUpdates );
	AbaxDataString ndeletes = ulongToString( numDeletes );

	// printf("s3104 diskread=%lu diskwrite=%lu netread=%lu netwrite=%lu\n", diskread, diskwrite, netread, netwrite );

	AbaxDataString rec = ulongToString( time(NULL) ) + "|" + AbaxDataString(userCPU) + "|" + sysCPU;
	rec += AbaxDataString("|") + totrams + "|" + freerams + "|" + totswaps + "|" + freeswaps + "|" + procs; 
	rec += AbaxDataString("|") + diskReads + "|" + diskWrites + "|" + netReads + "|" + netWrites;
	rec += AbaxDataString("|") + nselects + "|" + ninserts + "|" + nupdates + "|" + ndeletes;
	JagBoundFile bf( _perfFile.c_str(), 96 ); // 24 hours
	bf.openAppend();
	bf.appendLine( rec.c_str() );
	bf.close();
}
#else
// Windows
void JagDBServer::writeLoad( uabaxint seq )
{
	if ( ( seq%15) != 0 ) {
		return;
	}

	abaxint usercpu, syscpu, idlecpu;
	_jagSystem.getCPUStat( usercpu, syscpu, idlecpu );
	AbaxDataString userCPU = ulongToString( usercpu );
	AbaxDataString sysCPU = ulongToString( syscpu );

	abaxint totm, freem, used; //GB
	JagSystem::getMemInfo( totm, freem, used );
	AbaxDataString totrams = ulongToString( totm );
	AbaxDataString freerams = ulongToString( freem );

	MEMORYSTATUSEX statex;
	AbaxDataString totswaps = intToString( statex.ullTotalPageFile/ ONE_GIGA_BYTES );
	AbaxDataString freeswaps = intToString( statex.ullAvailPageFile / ONE_GIGA_BYTES );
	AbaxDataString procs = intToString( _jagSystem.getNumProcs() );

	AbaxDataString nselects = ulongToString( numSelects );
	AbaxDataString ninserts = ulongToString( numInserts );
	AbaxDataString nupdates = ulongToString( numUpdates );
	AbaxDataString ndeletes = ulongToString( numDeletes );

	uabaxint diskread, diskwrite, netread, netwrite;
	JagNet::getNetStat( netread, netwrite );
	JagFileMgr::getIOStat( diskread, diskwrite );
	AbaxDataString netReads = ulongToString( netread / ONE_GIGA_BYTES );
	AbaxDataString netWrites = ulongToString( netwrite / ONE_GIGA_BYTES );
	AbaxDataString diskReads = ulongToString( diskread  );
	AbaxDataString diskWrites = ulongToString( diskwrite  );

	AbaxDataString rec = ulongToString( time(NULL) ) + "|" + AbaxDataString(userCPU) + "|" + sysCPU;
	rec += AbaxDataString("|") + totrams + "|" + freerams + "|" + totswaps + "|" + freeswaps + "|" + procs; 
	rec += AbaxDataString("|") + diskReads + "|" + diskWrites + "|" + netReads + "|" + netWrites;
	rec += AbaxDataString("|") + nselects + "|" + ninserts + "|" + nupdates + "|" + ndeletes;
	JagBoundFile bf( _perfFile.c_str(), 96 ); // 24 hours
	bf.openAppend();
	bf.appendLine( rec.c_str() );
	bf.close();

}

#endif

// rec:  15MIN:OVERWRITE
void JagDBServer::copyData( const AbaxDataString &rec, bool show )
{
	if ( rec.length() < 1 ) {
		return;
	}
	
	JagStrSplit sp( rec, ':' );
	if ( sp.length() < 1 ) {
		return;
	}

	AbaxDataString dirname = makeLowerString( sp[0] );
	AbaxDataString policy;
	policy = sp[1];

	AbaxDataString tmstr = JagTime::YYYYMMDDHHMM();
	int rc = copyLocalData( dirname, policy, tmstr, show );
	if ( rc < 0 ) {
		// do not comment out
		prt(("E8305 copyData error\n"));
	}

	/***
	if ( policy != "OVERWRITE" && policy != "SNAPSHOT" ) {
		return;
	}

    AbaxDataString srcdir = jaguarHome() + "/data";
	if ( JagFileMgr::dirEmpty( srcdir ) ) {
		prt(("s7372 srcdir=[%s] is empty. copyData returns\n", srcdir.c_str() ));
		return;
	}

	char hname[80];
	gethostname( hname, 80 );

    AbaxDataString fileName = JagTime::YYYYMMDDHHMM() + "-" + hname;
    AbaxDataString destdir = jaguarHome() + "/backup";
	destdir += AbaxDataString("/") + dirname + "/" + fileName;

	JagFileMgr::makedirPath( destdir, 0700 );
	
	char cmd[2048];
	sprintf( cmd, "/bin/cp -rf %s/*  %s", srcdir.c_str(), destdir.c_str() );
	system( cmd );
	if ( show ) {
		raydebug( stdout, JAG_LOG_LOW, "Backup metadata to %s\n", destdir.c_str() );
	}

	// prt(("s3081 policy=[%s]\n", policy.c_str() ));
	AbaxDataString lastPath = jaguarHome() + "/backup/" + dirname + "/.lastFileName.txt";
	if ( policy == "OVERWRITE" ) {
		AbaxDataString lastFileName;
		JagFileMgr::readTextFile( lastPath, lastFileName );
		if (  lastFileName.length() > 0 ) {
    		AbaxDataString destdir = jaguarHome() + "/backup";
			destdir += AbaxDataString("/") + dirname + "/" + lastFileName;
			sprintf( cmd, "/bin/rm -rf %s", destdir.c_str() );
			system( cmd );
		}
	}

	JagFileMgr::writeTextFile( lastPath, fileName );
	// prt(("s3825 writeTextFile lastPath=[%s] fileName=[%s]\n", lastPath.c_str(), fileName.c_str() ));

	// prt(( "Backup data from [%s] to [%s]\n", srcdir.c_str(), destdir.c_str() ));
	*********/

}

// < 0: error
// 0: OK
// dirname: 15min/hourly/daily/weekly/monthly or last   policy: "OVERWRITE" or "SNAPSHOT"
int JagDBServer::copyLocalData( const AbaxDataString &dirname, const AbaxDataString &policy, const AbaxDataString &tmstr, bool show )
{
	if ( policy != "OVERWRITE" && policy != "SNAPSHOT" ) {
		return -1;
	}

    AbaxDataString srcdir = jaguarHome() + "/data";
	if ( JagFileMgr::dirEmpty( srcdir ) ) {
		prt(("s7372 srcdir=[%s] is empty. copyData returns\n", srcdir.c_str() ));
		return -2;
	}

	char hname[80];
	gethostname( hname, 80 );

    // AbaxDataString fileName = JagTime::YYYYMMDDHHMM() + "-" + hname;
    AbaxDataString fileName = tmstr + "-" + hname;
    AbaxDataString destdir = jaguarHome() + "/backup";
	destdir += AbaxDataString("/") + dirname + "/" + fileName;

	JagFileMgr::makedirPath( destdir, 0700 );
	
	char cmd[2048];
	sprintf( cmd, "/bin/cp -rf %s/*  %s", srcdir.c_str(), destdir.c_str() );
	system( cmd );
	if ( show ) {
		raydebug( stdout, JAG_LOG_LOW, "Backup data from %s to %s\n", srcdir.c_str(), destdir.c_str() );
	}

	AbaxDataString lastPath = jaguarHome() + "/backup/" + dirname + "/.lastFileName.txt";
	if ( policy == "OVERWRITE" ) {
		AbaxDataString lastFileName;
		JagFileMgr::readTextFile( lastPath, lastFileName );
		if (  lastFileName.length() > 0 ) {
    		AbaxDataString destdir = jaguarHome() + "/backup";
			destdir += AbaxDataString("/") + dirname + "/" + lastFileName;
			sprintf( cmd, "/bin/rm -rf %s", destdir.c_str() );
			system( cmd );
		}
	}

	JagFileMgr::writeTextFile( lastPath, fileName );
	return 0;
}

void JagDBServer::doCreateIndex( JagTable *ptab, JagIndex *pindex, JagDBServer *servobj )
{
	abaxint keyCheckerCnt = 0;
	ptab->_indexlist.append( pindex->getIndexName() );
	if ( ptab->_darrFamily->getElements( keyCheckerCnt ) < 1 ) {
		// prt(("s3023 getElements < 1 return. No formatCreateIndex\n" ));
		return;
	}
	//prt(("s5401 ptab->formatCreateIndex(pindex=%0x)\n", pindex ));
	ptab->formatCreateIndex( pindex );
}

// method to drop all tables and indexs under database "dbname"
void JagDBServer::dropAllTablesAndIndexUnderDatabase( const JagRequest &req, JagDBServer *servobj, 
	JagTableSchema *schema, const AbaxDataString &dbname )
{	
	JagTable *ptab;
	AbaxDataString dbobj, errmsg;
	JagVector<AbaxString> *vec = schema->getAllTablesOrIndexes( dbname, "" );
	if ( vec ) {
    	for ( int i = 0; i < vec->size(); ++i ) {
			ptab = NULL;
    		dbobj = (*vec)[i].c_str();
			JagStrSplit sp( dbobj, '.' );
			if ( sp.length() != 2 ) continue;

			JagParseParam pparam;
			ObjectNameAttribute objNameTemp;
			objNameTemp.init();
			objNameTemp.dbName = sp[0];
			objNameTemp.tableName = sp[1];
			pparam.objectVec.append(objNameTemp);
			pparam.optype = 'C';
			pparam.opcode = JAG_DROPTABLE_OP;
			
			ptab = servobj->_objectLock->writeLockTable( pparam.opcode, pparam.objectVec[0].dbName,
				pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			if ( ptab ) {
				servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
        		ptab->drop( errmsg );  //_darr is gone, raystat has cleaned up this darr
    			schema->remove( dbobj );
        		delete ptab; ptab = NULL;
				servobj->_objectLock->writeUnlockTable( pparam.opcode, pparam.objectVec[0].dbName,
					pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			}
    	}
		delete vec;
		vec = NULL;
	}
}

// method to drop all tables and indexs under all databases
void JagDBServer::dropAllTablesAndIndex( const JagRequest &req, JagDBServer *servobj, 
	JagTableSchema *schema )
{	
	JagTable *ptab;
	AbaxDataString dbobj, errmsg;
	JagVector<AbaxString> *vec = schema->getAllTablesOrIndexes( "", "" );
	if ( vec ) {
    	for ( int i = 0; i < vec->size(); ++i ) {
			ptab = NULL;
    		dbobj = (*vec)[i].c_str();
			JagStrSplit sp( dbobj, '.' );
			if ( sp.length() != 2 ) continue;

			JagParseParam pparam;
			ObjectNameAttribute objNameTemp;
			objNameTemp.init();
			objNameTemp.dbName = sp[0];
			objNameTemp.tableName = sp[1];
			pparam.objectVec.append(objNameTemp);
			pparam.optype = 'C';
			pparam.opcode = JAG_DROPTABLE_OP;
			
			ptab = servobj->_objectLock->writeLockTable( pparam.opcode, pparam.objectVec[0].dbName,
				pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			if ( ptab ) {
        		ptab->drop( errmsg );  //_darr is gone, raystat has cleaned up this darr
    			schema->remove( dbobj );
        		delete ptab; ptab = NULL;
				servobj->_objectLock->writeUnlockTable( pparam.opcode, pparam.objectVec[0].dbName,
					pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			}
    	}
		delete vec;
		vec = NULL;
	}
}

// static  
// showusers
void JagDBServer::showUsers( const JagRequest &req, JagDBServer *servobj )
{
	if ( req.session->uid != "admin" ) {
		JagTable::sendMessage( req, "Only admin can get a list of user accounts", "OK" );
		return;
	}

	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;
	AbaxDataString users;
	if ( uiddb ) {
		users = uiddb->getListUsers();
	}
	JagTable::sendMessageLength( req, users.c_str(), users.length(), "OK" );
}

// ensure admin account is created
void JagDBServer::createAdmin()
{
	// default admin password
	_dbConnector->_passwd = "dummy";
	JagUserID *uiddb = _userDB;
	makeAdmin( uiddb );
	uiddb = _prevuserDB;
	makeAdmin( uiddb );
	uiddb = _nextuserDB;
	makeAdmin( uiddb );
}

void JagDBServer::makeAdmin( JagUserID *uiddb )
{
	// default admin password
	AbaxDataString uid = "admin";
	AbaxDataString pass = "jaguarjaguarjaguar";
	AbaxString dbpass = uiddb->getValue(uid, JAG_PASS );
	if ( dbpass.size() > 0 ) {
		return;
	}

	char *md5 = MDString( pass.c_str() );
	AbaxDataString mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;

	bool rc = uiddb->addUser(uid, mdpass, JAG_USER, JAG_WRITE );
	// printf("s8339 createAdmin uid=[%s] mdpass=[%s] rc=%d\n", uid.c_str(), mdpass.c_str(), rc );
}

// static  
// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
AbaxDataString JagDBServer::getClusterOpInfo( const JagRequest &req, JagDBServer *servobj)
{
	JagStrSplit srv( servobj->_dbConnector->_nodeMgr->_allNodes, '|' );
	int nsrv = srv.length(); 
	int dbs, tabs;
	servobj->numDBTables( dbs, tabs );

	// get opinfo on this server
	AbaxDataString res;
	char buf[1024];
	sprintf( buf, "%d|%d|%d|%lld|%lld|%lld|%lld|%lld", nsrv, dbs, tabs, 
			(abaxint)servobj->numSelects, (abaxint)servobj->numInserts, 
			(abaxint)servobj->numUpdates, (abaxint)servobj->numDeletes, 
			servobj->_taskMap->size() );
	res = buf;

	// broadcast other server request info
	AbaxDataString resp, bcasthosts;
	resp = servobj->_dbConnector->broadCastGet( "_serv_opinfo", bcasthosts ); 
	resp += res + "\n";
	//printf("s4820 broadCastGet(_serv_opinfo)  resp=[%s]\n", resp.c_str() );
	//fflush( stdout );

	JagStrSplit sp( resp, '\n', true );
	abaxint sel, ins, upd, del, sess;
	sel = ins = upd = del = sess =  0;

	for ( int i = 0 ; i < sp.length(); ++i ) {
		JagStrSplit ss( sp[i], '|' );
		sel += jagatoll( ss[3].c_str() );
		ins += jagatoll( ss[4].c_str() );
		upd += jagatoll( ss[5].c_str() );
		del += jagatoll( ss[6].c_str() );
		sess += jagatoll( ss[7].c_str() );
	}

	memset(buf, 0, 1024);
	sprintf( buf, "%d|%d|%d|%lld|%lld|%lld|%lld|%lld", nsrv, dbs, tabs, sel, ins, upd, del, sess ); 
	return buf;
}

// object method
// get number of databases, and tables
void JagDBServer::numDBTables( int &databases, int &tables )
{
	databases = tables = 0;
	AbaxDataString dbs = JagSchema::getDatabases( _cfg, 0 );
	JagStrSplit sp(dbs, '\n', true );
	databases = sp.length();
	for ( int i = 0; i < sp.length(); ++i ) {
		JagVector<AbaxString> *vec = _tableschema->getAllTablesOrIndexes( sp[i], "" );
		tables += vec->size();
		if ( vec ) delete vec;
		vec = NULL;
	}
}

// object method
int JagDBServer::initObjects()
{
	//printf("s4200 initObjects() ...\n");
	raydebug( stdout, JAG_LOG_LOW, "begin initObjects ...\n" );
	
	_tableschema = new JagTableSchema( this, 0 );
	//prt(("s4201 _tableschema done\n"));
	_prevtableschema = new JagTableSchema( this, 1 );
	// printf("s4202 _tableschema done\n"); fflush( stdout );
	_nexttableschema = new JagTableSchema( this, 2 );
	// printf("s4203 _tableschema done\n"); fflush( stdout );

	_indexschema = new JagIndexSchema( this, 0 );
	// printf("s4204 _indexschema done\n"); fflush( stdout );
	_previndexschema = new JagIndexSchema( this, 1 );
	// printf("s4205 _indexschema done\n"); fflush( stdout );
	_nextindexschema = new JagIndexSchema( this, 2 );
	// printf("s4206 _indexschema done\n"); fflush( stdout );
	
	_userDB = new JagUserID( this, 0 );
	// printf("s4207 _userDB done\n"); fflush( stdout );
	_prevuserDB = new JagUserID( this, 1 );
	// printf("s4208 _userDB done\n"); fflush( stdout );
	_nextuserDB = new JagUserID( this, 2 );
	// printf("s4209 _userDB done\n"); fflush( stdout );

	_userRole = new JagUserRole( this, 0 );
	// printf("s4210 _userRole done\n"); fflush( stdout );
	_prevuserRole = new JagUserRole( this, 1 );
	// printf("s4211 _userRole done\n"); fflush( stdout );
	_nextuserRole = new JagUserRole( this, 2 );
	// printf("s4212 _userRole done\n"); fflush( stdout );

	raydebug( stdout, JAG_LOG_LOW, "end initObjects\n" );
	raydebug( stdout, JAG_LOG_HIGH, "Initialized schema objects\n" );
	// printf("s4210 initObjects() done \n");
	return 1;
}

int JagDBServer::createSocket( int argc, char *argv[] )
{
	AbaxString ports;
    if ( argc > 1 ) {
   	   _port = atoi( argv[1] );
    } else {
		ports = _cfg->getValue("PORT", "8888");
		_port = atoi( ports.c_str() );
		_listenIP = _cfg->getValue("LISTEN_IP", "");
	}
	_sock = JagNet::createIPV4Socket( _listenIP.c_str(), _port );
	if ( _sock < 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "Failed to create server socket, exit\n" );
		exit( 1 );
	}

	if ( _listenIP.size() > 1 ) {
		raydebug( stdout, JAG_LOG_LOW, "Listen connections at %s:%d (socket=%d)\n", _listenIP.c_str(), _port, _sock );
	} else {
		raydebug( stdout, JAG_LOG_LOW, "Listen connections at port %d (socket=%d)\n", _port, _sock );
	}
	return 1;
}

int JagDBServer::initConfigs()
{
	AbaxDataString cs;
	cs = _cfg->getValue("IS_GATE", "no");
	if ( cs == "yes" || cs == "YES" || cs == "Yes" || cs == "Y" || cs == "true" ) {
		_isGate = 1;
		raydebug( stdout, JAG_LOG_LOW, "isGate is yes\n" );
	} else {
		_isGate = 0;
		raydebug( stdout, JAG_LOG_LOW, "isGate is no\n" );
	}

	int threads = _numCPUs*atoi(_cfg->getValue("CPU_SELECT_FACTOR", "4").c_str());
	_selectPool = new JaguarThreadPool( threads );
	raydebug( stdout, JAG_LOG_LOW, "Select thread pool %d\n", threads );

	threads = _numCPUs*atoi(_cfg->getValue("CPU_PARSE_FACTOR", "2").c_str());
	_parsePool = new JaguarThreadPool( threads );
	raydebug( stdout, JAG_LOG_LOW, "Parser thread pool %d\n", threads );

	cs = _cfg->getValue("JAG_LOG_LEVEL", "0");
	JAG_LOG_LEVEL = atoi( cs.c_str() );
	if ( JAG_LOG_LEVEL <= 0 ) JAG_LOG_LEVEL = 1;
	raydebug( stdout, JAG_LOG_LOW, "JAG_LOG_LEVEL=%d\n", JAG_LOG_LEVEL );

	_version = AbaxDataString(JAG_VERSION);
	raydebug( stdout, JAG_LOG_LOW, "Server version is %s\n", _version.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "Server license is %s\n", PRODUCT_VERSION );

	// int trg = 8*_numCPUs;
	// int trg = _dbConnector->_nodeMgr->_numNodes * 30;
	// cs = _cfg->getValue("THREAD_GROUPS", intToStr(trg) );
	// _threadGroups = atoi( cs.c_str() );
	_threadGroups = _faultToleranceCopy * _dbConnector->_nodeMgr->_numAllNodes * 10;
	raydebug( stdout, JAG_LOG_LOW, "Thread Groups = %d\n", _threadGroups );
	if ( _isGate ) {
		cs = _cfg->getValue("INIT_EXTRA_THREADS", "100" );
	} else {
		cs = _cfg->getValue("INIT_EXTRA_THREADS", "30" );
	}
	_initExtraThreads = atoi( cs.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "Init Extra Threads = %d\n", _initExtraThreads );

	/***
	AbaxDataString schmpath = jaguarHome() + "/data/system/schema";
	abaxint numtabs = JagFileMgr::numObjects( schmpath );
	if ( numtabs < 1 ) numtabs = 4;
	abaxint grpsz = _dbConnector->_nodeMgr->_numNodes * numtabs;
	***/
	/***
	abaxint grpsz = 8 * _dbConnector->_nodeMgr->_numNodes;
	cs = _cfg->getValue("THREAD_GROUP_SIZE", longToStr(grpsz) );
	_threadGroupSize = atoi( cs.c_str() );
	**/
	// raydebug( stdout, JAG_LOG_LOW, "Group Size = %d\n", _threadGroupSize );
	// _connMax = _threadGroups * _threadGroupSize;

	// write process ID
	AbaxDataString logpath = jaguarHome() + "/log/" + JAG_BRAND + ".pid";
	FILE *pidf = loopOpen( logpath.c_str(), "wb" );
	fprintf( pidf, "%d\n", getpid() );
	fflush( pidf ); jagfclose( pidf );

	raydebug( stdout, JAG_LOG_LOW, "Process ID %d in %s\n", getpid(), logpath.c_str() );

	_walLog = 0;
	cs = _cfg->getValue("WAL_LOG", "yes");
	if ( startWith( cs, 'y' ) ) {
		_walLog =  1;
		raydebug( stdout, JAG_LOG_LOW, "WAL Log %s\n", cs.c_str() );
	}

	cs = _cfg->getValue("FLUSH_WAIT", "1");
	_flushWait = atoi( cs.c_str() );
	// raydebug( stdout, JAG_LOG_LOW, "Wait Count %d\n", _flushWait );

	_debugClient = 0;
	cs = _cfg->getValue("DEBUG_CLIENT", "no");
	if ( startWith( cs, 'y' ) ) {
		_debugClient =  1;
	} 
	raydebug( stdout, JAG_LOG_LOW, "DEBUG_CLIENT %s\n", cs.c_str() );

	// cs = _cfg->getValue("INSERT_SYNC_PERIOD", "20");
	// _insertSyncPeriod = atoi( cs.c_str() );
	// raydebug( stdout, JAG_LOG_LOW, "Wait Count %d\n", _flushWait );

	//_threadMap = new JagHashMap<AbaxLong,AbaxLong>();
	_taskMap = new JagHashMap<AbaxLong,AbaxString>();
	_joinMap = new JagHashMap<AbaxString, AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>>>();
	_scMap = new JagHashMap<AbaxString, AbaxInt>();

	// read public and private keys
	int keyExists = 0;
	AbaxDataString keyFile = jaguarHome() + "/conf/public.key";
	while ( ! keyExists ) {
		JagFileMgr::readTextFile( keyFile, _publicKey );
		if ( _publicKey.size() < 1 ) {
			raydebug( stdout, JAG_LOG_LOW, "Key file %s not found, wait ...\n", keyFile.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "Please execute createKeyPair program to generate it.\n");
			raydebug( stdout, JAG_LOG_LOW, "Once conf/public.key and conf/private.key are generated, server will use them.\n");
			sleep(10);
		} else {
			keyExists = 1;
		}
	}

	keyFile = jaguarHome() + "/conf/private.key";
	keyExists = 0;
	while ( ! keyExists ) {
		JagFileMgr::readTextFile( keyFile, _privateKey );
		if ( _privateKey.size() < 1 ) {
			raydebug( stdout, JAG_LOG_LOW, "Key file %s not found, wait ...\n", keyFile.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "Please execute createKeyPair program to generate it.\n");
			raydebug( stdout, JAG_LOG_LOW, "Once conf/public.key and conf/private.key are generated, server will use them.\n");
			sleep(10);
		} else {
			keyExists = 1;
		}
	}



}

// dinsert log command to the command log file
void JagDBServer::dinsertlogCommand( JagSession *session, const char *mesg, abaxint len )
{
	JAG_BLURT jaguar_mutex_lock ( &g_dinsertmutex ); JAG_OVER;
	if ( session->servobj->_dinsertCommand ) {
		fprintf( session->servobj->_dinsertCommand, "%d;%016ld%s",
			session->timediff, len, mesg );
	}
	jaguar_mutex_unlock ( &g_dinsertmutex );
}

// log command to the command log file
// spMode = 0 : special cmds, create/drop etc. spMode = 1 : single regular cmds, update/delete etc.
// spMode = 2 : batch regular cmds, insert/cinsert/dinsert etc. 
void JagDBServer::logCommand( JagSession *session, const char *mesg, abaxint len, int spMode )
{
	JAG_BLURT jaguar_mutex_lock ( &g_logmutex ); JAG_OVER;
	if ( session->servobj->_fpCommand ) {
		fprintf( session->servobj->_fpCommand, "%d;%d;%d;%016ld%s",
			session->replicateType, session->timediff, 2==spMode, len, mesg );
		if ( 0 == spMode || 2 == spMode ) {
			// fflush( session->servobj->_fpCommand );
			jagfdatasync( fileno(session->servobj->_fpCommand ) );
		}
	}
	jaguar_mutex_unlock ( &g_logmutex );
}

// log command to the recovery log file
// spMode = 0 : special cmds, create/drop etc. spMode = 1 : single regular cmds, update/delete etc.
// spMode = 2 : batch regular cmds, insert/cinsert/dinsert etc. 
void JagDBServer::regSplogCommand( JagSession *session, const char *mesg, abaxint len, int spMode )
{
	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	if ( 0 == spMode && session->servobj->_recoverySpCommand ) {
		fprintf( session->servobj->_recoverySpCommand, "%d;%d;0;%016ld%s",
			session->replicateType, session->timediff, len, mesg );
		// fflush( session->servobj->_recoverySpCommand );
		jagfdatasync( fileno(session->servobj->_recoverySpCommand ) );
	} else if ( 0 != spMode && session->servobj->_recoveryRegCommand ) {
		fprintf( session->servobj->_recoverySpCommand, "%d;%d;%d;%016ld%s",
			session->replicateType, session->timediff, 2==spMode, len, mesg );
		if ( 2 == spMode ) {
			// fflush ( session->servobj->_recoveryRegCommand );
			jagfdatasync( fileno(session->servobj->_recoveryRegCommand ) );
	 		// fflush ( session->servobj->_recoveryRegCommand );
			// jagfdatasync( fileno(session->servobj->_recoveryRegCommand ) );
		}
	}
	jaguar_mutex_unlock ( &g_dlogmutex );
}

// mode: replicateType 012
// 0: YYY; 1: YYN; 2: YNY; 3: YNN; 4: NYY; 5: NYN; 6: NNY;
void JagDBServer::deltalogCommand( int mode, JagSession *session, const char *mesg, bool isBatch )
{	
	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	int rc, needSync = 0;
	AbaxDataString cmd;
	FILE *f1 = NULL;
	FILE *f2 = NULL;
	
	if ( !isBatch ) { // not batch command, parse and rebuild cmd with dbname inside
		AbaxDataString reterr;
		JagParseAttribute jpa( session->servobj, session->timediff, session->servobj->servtimediff, 
							   session->dbname, session->servobj->_cfg );
		// prt(("s2230 parseCommand...\n" ));
		JagParser parser( this, NULL );
		JagParseParam parseParam(&parser);
		rc = parser.parseCommand( jpa, mesg, &parseParam, reterr );
		if ( rc ) {
			cmd = parseParam.dbNameCmd;
			// rebuildCommandWithDBName( mesg, parseParam, cmd );
		}
		if ( parseParam.opcode != JAG_UPDATE_OP && parseParam.opcode != JAG_DELETE_OP ) {
			needSync = 1;
		}
	} else { // batch command, insert/cinsert/dinsert
		cmd = mesg;
		needSync = 1;
	}
	
	if ( session->servobj->_faultToleranceCopy == 2 ) {
		if ( 1 == mode ) mode = 0;
		else if ( 3 == mode ) mode = 2;
		else if ( 5 == mode ) mode = 4;
	}

	// store to correct delta files
	if ( 1 == mode ) {
		if ( 0 == session->replicateType ) {
			f1 = session->servobj->_delPrevRepCommand;
		} else if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriRepCommand;
		} 
	} else if ( 2 == mode ) {
		if ( 0 == session->replicateType ) {
			f1 = session->servobj->_delNextRepCommand;
		} else if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriRepCommand;
		} 
	} else if ( 3 == mode ) {
		if ( 0 == session->replicateType ) {
			f1 = session->servobj->_delPrevRepCommand;
			f2 = session->servobj->_delNextRepCommand;
		} 
	} else if ( 4 == mode ) {
		if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriCommand;
		} else if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriCommand;		
		} 
	} else if ( 5 == mode ) {
		if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriCommand;
			f2 = session->servobj->_delPrevOriRepCommand;
		}
	} else if ( 6 == mode ) {
		if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriCommand;
			f2 = session->servobj->_delNextOriRepCommand;
		}
	}
	
	if ( f1 ) {
		fprintf( f1, "%d;%d;%s\n", session->timediff, isBatch, cmd.c_str() );
	}
	if ( f2 ) {
		fprintf( f2, "%d;%d;%s\n", session->timediff, isBatch, cmd.c_str() );
	}
	if ( needSync ) {
		if ( f1 ) {
			// fflush( f1 );
			jagfdatasync( fileno( f1 ) );
		}
		if ( f2 ) {
			// fflush( f2 );
			jagfdatasync( fileno( f2 ) );
		}
		if ( 0 == session->replicateType && ! session->origserv ) {
			// if ( f1 || f2 ) JagStrSplitWithQuote split( mesg, ';' );
		}
	}

	// prt(("s9999 deltalogCommand type=%d mode=%d\n", session->replicateType, mode));
	jaguar_mutex_unlock ( &g_dlogmutex );
}

// method to recover delta log immediately after delta log written, if connection has already been recovered
void JagDBServer::onlineRecoverDeltaLog()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate
	AbaxDataString fpaths;
	// use client to recover delta log
	if ( _delPrevOriCommand && JagFileMgr::fileSize(_actdelPOpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelPOpath;
		else fpaths += AbaxDataString("|") + _actdelPOpath;
		// fflush( _delPrevOriCommand );
		// jagfdatasync( fileno( _delPrevOriCommand ) );
		jagfclose( _delPrevOriCommand );
		_delPrevOriCommand = NULL;
	}
	if ( _delPrevRepCommand && JagFileMgr::fileSize(_actdelPRpath) > 0 ) { 
		if ( fpaths.size() < 1 ) fpaths = _actdelPRpath;
		else fpaths += AbaxDataString("|") + _actdelPRpath;
		// fflush( _delPrevRepCommand );
		// jagfdatasync( fileno( _delPrevRepCommand ) );
		jagfclose( _delPrevRepCommand );
		_delPrevRepCommand = NULL;
	}
	if ( _delPrevOriRepCommand && JagFileMgr::fileSize(_actdelPORpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelPORpath;
		else fpaths += AbaxDataString("|") + _actdelPORpath;
		// fflush( _delPrevOriRepCommand );
		// jagfdatasync( fileno( _delPrevOriRepCommand ) );
		jagfclose( _delPrevOriRepCommand );
		_delPrevOriRepCommand = NULL;
	}
	if ( _delNextOriCommand && JagFileMgr::fileSize(_actdelNOpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNOpath;
		else fpaths += AbaxDataString("|") + _actdelNOpath;
		// fflush( _delNextOriCommand );
		// jagfdatasync( fileno( _delNextOriCommand ) );
		jagfclose( _delNextOriCommand );
		_delNextOriCommand = NULL;
	}
	if ( _delNextRepCommand && JagFileMgr::fileSize(_actdelNRpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNRpath;
		else fpaths += AbaxDataString("|") + _actdelNRpath;
		// fflush( _delNextRepCommand );
		// jagfdatasync( fileno( _delNextRepCommand ) );
		jagfclose( _delNextRepCommand );
		_delNextRepCommand = NULL;
	}
	if ( _delNextOriRepCommand && JagFileMgr::fileSize(_actdelNORpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNORpath;
		else fpaths += AbaxDataString("|") + _actdelNORpath;
		// fflush( _delNextOriRepCommand );
		// jagfdatasync( fileno( _delNextOriRepCommand ) );
		jagfclose( _delNextOriRepCommand );
		_delNextOriRepCommand = NULL;
	}

	if ( fpaths.size() > 0 ) { // has delta files, recover using delta
		raydebug( stdout, JAG_LOG_LOW, "begin online recoverDeltaLog %s ...\n", fpaths.c_str() );
		_dbConnector->_parentCli->recoverDeltaLog( fpaths );
		raydebug( stdout, JAG_LOG_LOW, "end online recoverDeltaLog\n");
	}

	if ( !_delPrevOriCommand ) {
		_delPrevOriCommand = loopOpen( _actdelPOpath.c_str(), "ab" );
	}
	if ( !_delNextRepCommand ) {
		_delNextRepCommand = loopOpen( _actdelNRpath.c_str(), "ab" );
	}
	if ( 3 == _faultToleranceCopy ) {
		if ( !_delPrevRepCommand ) {
			_delPrevRepCommand = loopOpen( _actdelPRpath.c_str(), "ab" );
		}
		if ( !_delPrevOriRepCommand ) {
			_delPrevOriRepCommand = loopOpen( _actdelPORpath.c_str(), "ab" );
		}
		if ( !_delNextOriCommand ) {
			_delNextOriCommand = loopOpen( _actdelNOpath.c_str(), "ab" );
		}
		if ( !_delNextOriRepCommand ) {
			_delNextOriRepCommand = loopOpen( _actdelNORpath.c_str(), "ab" );
		}
	}
}

// close and reopen delta log file, for replicate use only
void JagDBServer::resetDeltaLog()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate

	raydebug( stdout, JAG_LOG_LOW, "begin resetDeltaLog ...\n");

	AbaxDataString deltahome = JagFileMgr::getLocalLogDir("delta"), fpath, host0, host1, host2, host3, host4;
	int pos1, pos2, pos3, pos4;
	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	// set pos1: _nthServer+1; pos2: _nthServer-1
	if ( _nthServer == 0 ) {
		pos1 = _nthServer+1;
		pos2 = sp.length()-1;
	} else if ( _nthServer == sp.length()-1 ) {
		pos1 = 0;
		pos2 = _nthServer-1;
	} else {
		pos1 = _nthServer+1;
		pos2 = _nthServer-1;
	}
	// set pos3: pos1+1; pos4: pos2-1;
	if ( pos1 == sp.length()-1 ) {
		pos3 = 0;
	} else {
		pos3 = pos1+1;
	}
	if ( pos2 == 0 ) {
		pos4 = sp.length()-1;
	} else {
		pos4 = pos2-1;
	}
	// set host0, host1, host2, host3, host4
	host0 = sp[_nthServer];
	host1 = sp[pos1];
	host2 = sp[pos2];
	host3 = sp[pos3];
	host4 = sp[pos4];
	
	if ( _faultToleranceCopy == 2 ) {
		if ( _delPrevOriCommand ) {
			jagfclose( _delPrevOriCommand );
		}
		if ( _delNextRepCommand ) {
			jagfclose( _delNextRepCommand );
		}
		
		fpath = deltahome + "/" + host2 + "_0";
		_delPrevOriCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelPOpath = fpath;
		_actdelPOhost = host2;
		
		fpath = deltahome + "/" + host0 + "_1";	
		_delNextRepCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelNRpath = fpath;
		_actdelNRhost = host1;
	} else if ( _faultToleranceCopy == 3 ) {
		if ( _delPrevOriCommand ) {
			jagfclose( _delPrevOriCommand );
		}
		if ( _delPrevRepCommand ) {
			jagfclose( _delPrevRepCommand );
		}
		if ( _delPrevOriRepCommand ) {
			jagfclose( _delPrevOriRepCommand );
		}
		if ( _delNextOriCommand ) {
			jagfclose( _delNextOriCommand );
		}
		if ( _delNextRepCommand ) {
			jagfclose( _delNextRepCommand );
		}
		if ( _delNextOriRepCommand ) {
			jagfclose( _delNextOriRepCommand );
		}
		
		fpath = deltahome + "/" + host2 + "_0";
		_delPrevOriCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelPOpath = fpath;
		_actdelPOhost = host2;
		fpath = deltahome + "/" + host0 + "_2";
		_delPrevRepCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelPRpath = fpath;
		_actdelPRhost = host2;
		fpath = deltahome + "/" + host2 + "_2";
		_delPrevOriRepCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelPORpath = fpath;
		_actdelPORhost = host4; 
		fpath = deltahome + "/" + host1 + "_0";
		_delNextOriCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelNOpath = fpath;
		_actdelNOhost = host1;
		fpath = deltahome + "/" + host0 + "_1";
		_delNextRepCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelNRpath = fpath;
		_actdelNRhost = host1;
		fpath = deltahome + "/" + host1 + "_1";
		_delNextOriRepCommand = loopOpen( fpath.c_str(), "ab" );
		_actdelNORpath = fpath;
		_actdelNORhost = host3;
	}
	raydebug( stdout, JAG_LOG_LOW, "end resetDeltaLog\n");
}

int JagDBServer::checkDeltaFileStatus()
{
	if ( JagFileMgr::fileSize(_actdelPOpath) > 0 || JagFileMgr::fileSize(_actdelPRpath) > 0 ||
		 JagFileMgr::fileSize(_actdelPORpath) > 0 || JagFileMgr::fileSize(_actdelNOpath) > 0 ||
		 JagFileMgr::fileSize(_actdelNRpath) > 0 || JagFileMgr::fileSize(_actdelNORpath) > 0 ) {
		// has delta content
		return 1;
	} else {
		return 0;
	}
}

// orginize and tar dir to be transmitted later
// mode 0: data; mode 1: pdata; mode 2: ndata
void JagDBServer::organizeCompressDir( int mode, AbaxDataString &fpath )
{
	AbaxDataString spath = _cfg->getJDBDataHOME( mode );
	fpath = _cfg->getTEMPDataHOME( mode ) + "/" + longToStr(THREADID) + ".tar.gz";
	// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/tid.tar.gz *
	AbaxDataString cmd = AbaxDataString("cd ") + spath + "; " + "tar -zcf " + fpath + " *";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6107 [%s]\n", cmd.c_str() );
}

// file open a.tar.gz and send to another server
// mode 0: data; mode 1: pdata; mode 2: ndata  
// mode 10: transmit wallog file
int JagDBServer::fileTransmit( const AbaxDataString &host, unsigned int port, const AbaxDataString &passwd, 
	const AbaxDataString &unixSocket, int mode, const AbaxDataString &fpath, int isAddCluster )
{
	prt(("s3082 fileTransmit host=[%s] mode=%d fath=[%s] isAddCluster=%d\n", host.c_str(), mode, fpath.c_str(), isAddCluster ));
	if ( fpath.size() < 1 ) {
		return 0; // no fpath, no need to transfer
	}
	raydebug( stdout, JAG_LOG_LOW, "begin fileTransmit [%s]\n", fpath.c_str() );
	int pkgsuccess = false;
	
	while ( !pkgsuccess ) {
		JaguarCPPClient pcli;
		pcli._deltaRecoverConnection = 2;
		if ( !pcli.connect( host.c_str(), port, "admin", passwd.c_str(), "test", unixSocket.c_str(), JAG_CONNECT_ONE ) ) {
			raydebug( stdout, JAG_LOG_LOW, "s4058 recover failure, unable to make connection %s:%d ...\n", host.c_str(), _port );
			if ( !isAddCluster ) jagunlink( fpath.c_str() );
			return 0;
		}

		// open binary tar.gz file to send, malloc 128MB buffer to use
		abaxint rc;
		ssize_t rlen;
		struct stat sbuf;
		AbaxDataString cmd;
		if ( 0 == stat(fpath.c_str(), &sbuf) && sbuf.st_size > 0 ) {
			int fd = jagopen( fpath.c_str(), O_RDONLY, S_IRWXU);
			if ( fd < 0 ) {
				if ( !isAddCluster ) jagunlink( fpath.c_str() );
				pcli.close();
				raydebug( stdout, JAG_LOG_LOW, "E0838 end fileTransmit [%s] open error\n", fpath.c_str() );
				return 0;
			}
			char buf[100];
			memset( buf, 0, 100 );
			if ( !isAddCluster ) {
				cmd = "_serv_beginftransfer|" + intToStr( mode ) + "|" + longToStr(sbuf.st_size) + "|" + longToStr(THREADID);
			} else {
				cmd = "_serv_addbeginftransfer|" + intToStr( mode ) + "|" + longToStr(sbuf.st_size) + "|" + longToStr(THREADID);
			}
			memcpy( buf+JAG_SOCK_MSG_HDR_LEN, cmd.c_str(), cmd.size() );
			sprintf( buf, "%0*lld", JAG_SOCK_MSG_HDR_LEN-4, cmd.size() );
			memcpy( buf+JAG_SOCK_MSG_HDR_LEN-4, "ANCC", 4 );
			rc = sendData( pcli.getSocket(), buf, JAG_SOCK_MSG_HDR_LEN+cmd.size() ); // 016ABCmessage client query mode
			if ( rc < 0 ) {
				jagclose( fd );
				pcli.close();
				raydebug( stdout, JAG_LOG_LOW, "E0831 retry fileTransmit [%s] senddata error\n", fpath.c_str() );
				continue;
			}
			
			beginBulkSend( pcli.getSocket() );
			rlen = jagsendfile( pcli.getSocket(), fd, sbuf.st_size );
			endBulkSend( pcli.getSocket() );
			if ( rlen < sbuf.st_size ) {
				jagclose( fd );
				pcli.close();
				raydebug( stdout, JAG_LOG_LOW, "E8371 retry fileTransmit [%s] sendfile error\n", fpath.c_str() );
				continue;
			}
			raydebug( stdout, JAG_LOG_LOW, "fileTransmit [%s] sendfile %l bytes\n", fpath.c_str(), rlen );
			rlen = sendData( pcli.getSocket(), "try", 3 );
			if ( rlen < 3 ) {
				jagclose( fd );
				pcli.close();
				raydebug( stdout, JAG_LOG_LOW, "E7723 retry fileTransmit [%s] try error\n", fpath.c_str() );
				continue;
			}
			rlen = recvData( pcli.getSocket(), buf, 3 );
			if ( rlen < 3 || memcmp( buf, "end", 3 ) != 0 ) {
				jagclose( fd );
				pcli.close();
				raydebug( stdout, JAG_LOG_LOW, "retry fileTransmit [%s] package integrity check failure\n", fpath.c_str() );
				continue;
			}
			jagclose( fd );
			if ( !isAddCluster ) jagunlink( fpath.c_str() );
			pkgsuccess = true;
			pcli.close();
			raydebug( stdout, JAG_LOG_LOW, "end fileTransmit [%s] OK\n", fpath.c_str() );
		}
	}
	return 1;
}

// method to unzip tar.gz file and rebuild necessary memory objects
void JagDBServer::recoveryFromTransferredFile()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate
	raydebug( stdout, JAG_LOG_LOW, "begin redo xfer data ...\n" );
	AbaxDataString cmd, datapath, tmppath, fpath;
	bool isdo = false;

	// first, need to drop all current tables and indexs if untar needed
	JagRequest req;
	JagSession session( g_dtimeout );
	req.session = &session;
	session.servobj = this;

	// process original data first
	tmppath = _cfg->getTEMPDataHOME( 0 );
	datapath = _cfg->getJDBDataHOME( 0 );
	JagStrSplit sp( _crecoverFpath.c_str(), '|', true );
	if ( sp.length() > 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "cleanredo file [%s]\n", _crecoverFpath.c_str() );
		isdo = true;
		// if more than one file has received, need to consider which file to be used
		// since all packages sent are non-recover server, use first package is enough
		
		session.replicateType = 0;
		dropAllTablesAndIndex( req, this, _tableschema );
		JagFileMgr::rmdir( datapath, false );
		fpath = tmppath + "/" + sp[0];
		cmd = AbaxDataString("tar -zxf ") + fpath + " --directory=" + datapath;
		system( cmd.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "s6100 crecover system cmd[%s]\n", cmd.c_str() );

		// remove tar.gz files
		for ( int i = 0; i < sp.length(); ++i ) {
			jagunlink((tmppath+"/"+sp[i]).c_str());
			raydebug( stdout, JAG_LOG_LOW, "delete %s\n", sp[i].c_str() );
		}
	}
	if ( _faultToleranceCopy >= 2 ) {
		// process prev data dir
		tmppath = _cfg->getTEMPDataHOME( 1 );
		datapath = _cfg->getJDBDataHOME( 1 );
		sp.init( _prevcrecoverFpath.c_str(), '|', true );
		if ( sp.length() > 0 ) {
			raydebug( stdout, JAG_LOG_LOW, "prevcleanredo file [%s]\n", _prevcrecoverFpath.c_str() );
			isdo = true;
			// if more than one file has received, need to consider which file to be used
			// since all packages sent are non-recover server, use first package is enough
	
			session.replicateType = 1;
			dropAllTablesAndIndex( req, this, _prevtableschema );
			JagFileMgr::rmdir( datapath, false );
			fpath = tmppath + "/" + sp[0];
			cmd = AbaxDataString("tar -zxf ") + fpath + " --directory=" + datapath;
			system( cmd.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "s6102 crecover system cmd[%s]\n", cmd.c_str() );

			// remove tar.gz files
			for ( int i = 0; i < sp.length(); ++i ) {
				jagunlink((tmppath+"/"+sp[i]).c_str());
				raydebug( stdout, JAG_LOG_LOW, "delete %s\n", sp[i].c_str() );
			}
		}
	}
	if ( _faultToleranceCopy >= 3 ) {
		// process next data dir
		tmppath = _cfg->getTEMPDataHOME( 2 );
		datapath = _cfg->getJDBDataHOME( 2 );
		sp.init( _nextcrecoverFpath.c_str(), '|', true );
		if ( sp.length() > 0 ) {
			raydebug( stdout, JAG_LOG_LOW, "nextcleanredo file [%s]\n", _nextcrecoverFpath.c_str() );
			isdo = true;
			// if more than one file has received, need to consider which file to be used
			// since all packages sent are non-recover server, use first package is enough

			session.replicateType = 2;
			dropAllTablesAndIndex( req, this, _nexttableschema );
			JagFileMgr::rmdir( datapath, false );
			fpath = tmppath + "/" + sp[0];
			cmd = AbaxDataString("tar -zxf ") + fpath + " --directory=" + datapath;
			system( cmd.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "s6104 crecover system cmd[%s]\n", cmd.c_str() );

			// remove tar.gz files
			for ( int i = 0; i < sp.length(); ++i ) {
				jagunlink((tmppath+"/"+sp[i]).c_str());
				raydebug( stdout, JAG_LOG_LOW, "delete %s\n", sp[i].c_str() );
			}
		}		
	}

	raydebug( stdout, JAG_LOG_LOW, "end redo xfer data\n" );
	
	// after finish copying files, refresh some related memory objects
	if ( isdo ) crecoverRefreshSchema( 0 );
	_crecoverFpath = "";
	_prevcrecoverFpath = "";
	_nextcrecoverFpath = "";
}

// mode 0: rebuild all objects and remake connections
// mode 1: rebuild all objects only
// mode 2: remake connections only
void JagDBServer::crecoverRefreshSchema( int mode )
{
	// destory schema related objects and rebuild them
	raydebug( stdout, JAG_LOG_LOW, "begin redo schema...\n" );

	if ( 0 == mode || 1 == mode ) {
		if ( _userDB ) {
			delete _userDB;
			_userDB = NULL;
		}

		if ( _prevuserDB ) {
			delete _prevuserDB;
			_prevuserDB = NULL;
		}

		if ( _nextuserDB ) {
			delete _nextuserDB;
			_nextuserDB = NULL;
		}

		if ( _userRole ) {
			delete _userRole;
			_userRole = NULL;
		}

		if ( _prevuserRole ) {
			delete _prevuserRole;
			_prevuserRole = NULL;
		}

		if ( _nextuserRole ) {
			delete _nextuserRole;
			_nextuserRole = NULL;
		}

		if ( _tableschema ) {
			delete _tableschema;
			_tableschema = NULL;
		}

		if ( _indexschema ) {
			delete _indexschema;
			_indexschema = NULL;
		}

		if ( _prevtableschema ) {
			delete _prevtableschema;
			_prevtableschema = NULL;
		}

		if ( _previndexschema ) {
			delete _previndexschema;
			_previndexschema = NULL;
		}
	
		if ( _nexttableschema ) {
			delete _nexttableschema;
			_nexttableschema = NULL;
		}

		if ( _nextindexschema ) {
			delete _nextindexschema;
			_nextindexschema = NULL;
		}

		_objectLock->rebuildObjects();

		// also, get other databases name
		AbaxDataString dblist, dbpath;
		dbpath = _cfg->getJDBDataHOME( 0 );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, 0 );
		dbpath = _cfg->getJDBDataHOME( 1 );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, 1 );
		dbpath = _cfg->getJDBDataHOME( 2 );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, 2 );

		initObjects();
		makeTableObjects( (void*)this );
	}

	if ( 0 == mode || 2 == mode ) {
		_dbConnector->makeInitConnection( _debugClient );
		JagRequest req;
		JagSession session( g_dtimeout );
		req.session = &session;
		session.servobj = this;
		session.replicateType = 0;
		refreshSchemaInfo( req, this, g_lastSchemaTime );
	}
	raydebug( stdout, JAG_LOG_LOW, "end redo schema\n" );
}

// close and reopen temp log file to store temporary write related commands while recovery
void JagDBServer::resetRegSpLog()
{
	raydebug( stdout, JAG_LOG_LOW, "begin reset data/schema log\n" );
	
    AbaxDataString deltahome = JagFileMgr::getLocalLogDir("delta");
    AbaxDataString fpath = deltahome + "/redodata.log";
    if ( _recoveryRegCommand ) {
        jagfclose( _recoveryRegCommand );
    }
    if ( _recoverySpCommand ) {
        jagfclose( _recoverySpCommand );
    }
	
	_recoveryRegCmdPath = fpath;
    _recoveryRegCommand = loopOpen( fpath.c_str(), "ab" );
	
	fpath = deltahome + "/redometa.log";
	_recoverySpCmdPath = fpath;
    _recoverySpCommand = loopOpen( fpath.c_str(), "ab" );
	raydebug( stdout, JAG_LOG_LOW, "end reset data/schema log\n" );
}

void JagDBServer::recoverRegSpLog()
{
	raydebug( stdout, JAG_LOG_LOW, "begin redo data and schema ...\n" );
	abaxint cnt;

	// global lock
	JAG_BLURT jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;

	// check sp command first
	if ( _recoverySpCommand && JagFileMgr::fileSize(_recoverySpCmdPath) > 0 ) {
		// fflush( _recoverySpCommand );
		// jagfdatasync( fileno( _recoverySpCommand ) );
		jagfclose( _recoverySpCommand );
		_recoverySpCommand = NULL;
		raydebug( stdout, JAG_LOG_LOW, "begin redo metadata [%s] ...\n", _recoverySpCmdPath.c_str() );
		cnt = redoLog( _recoverySpCmdPath );
		raydebug( stdout, JAG_LOG_LOW, "end redo metadata [%s] count=%l\n", _recoverySpCmdPath.c_str(), cnt );
		jagunlink( _recoverySpCmdPath.c_str() );
		_recoverySpCommand = loopOpen( _recoverySpCmdPath.c_str(), "ab" );
	}

	// unlock global lock
	_restartRecover = 0;
	jaguar_mutex_unlock ( &g_flagmutex );

	// check reg command to redo log
	if ( _recoveryRegCommand && JagFileMgr::fileSize(_recoveryRegCmdPath) > 0 ) {
		// fflush( _recoveryRegCommand );
		// jagfdatasync( fileno( _recoveryRegCommand ) );
		jagfclose( _recoveryRegCommand );
		_recoveryRegCommand = NULL;
		raydebug( stdout, JAG_LOG_LOW, "begin redo data [%s] ...\n", _recoveryRegCmdPath.c_str() );
		cnt = redoLog( _recoveryRegCmdPath );
		raydebug( stdout, JAG_LOG_LOW, "end redo data [%s] count=%l\n", _recoveryRegCmdPath.c_str(), cnt );
		jagunlink( _recoveryRegCmdPath.c_str() );
		_recoveryRegCommand = loopOpen( _recoveryRegCmdPath.c_str(), "ab" );
	}

	raydebug( stdout, JAG_LOG_LOW, "end redo data and schema\n" );
}

// open new dinsert active log
void JagDBServer::resetDinsertLog()
{
	AbaxDataString fpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertactive.log";
	if ( _dinsertCommand ) {
		jagfclose( _dinsertCommand );
	}
	raydebug( stdout, JAG_LOG_LOW, "open new dinsert %s\n", fpath.c_str() );
	_dinsertCommand = loopOpen( fpath.c_str(), "ab" );
	_activeDinFpath = fpath;
}

// do dinsertbackup if not empty, and then rename dinsertactive to dinsertbackup
void JagDBServer::rotateDinsertLog()
{
	AbaxDataString backupDinPath = JagFileMgr::getLocalLogDir("delta") + "/dinsertbackup.log";
	recoverDinsertLog( backupDinPath );
	// JAG_BLURT jaguar_mutex_lock ( &g_dinsertmutex ); JAG_OVER;
	jaguar_mutex_lock ( &g_dinsertmutex ); 
	if ( _dinsertCommand ) {
		// fflush( _dinsertCommand );
		// jagfdatasync( fileno( _dinsertCommand ) );
		jagfclose( _dinsertCommand );
	}
	jagrename( _activeDinFpath.c_str(), backupDinPath.c_str() );
	_dinsertCommand = loopOpen( _activeDinFpath.c_str(), "ab" );
	jaguar_mutex_unlock ( &g_dinsertmutex );
}

// read uncommited logs in dinsert/ dir and execute them
void JagDBServer::recoverDinsertLog( const AbaxDataString &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) < 1 ) return;
	raydebug( stdout, JAG_LOG_LOW, "begin redo %s ...\n",  fpath.c_str() );
	abaxint cnt = redoDinsertLog( fpath );
	raydebug( stdout, JAG_LOG_LOW, "end redo %s cnt=%l\n",  fpath.c_str(), cnt );
	jagunlink( fpath.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "unlink %s\n",  fpath.c_str() );
}

// process commands in fpath one by one and send them to corresponding servers
// client_timediff;ddddddddddddddddstrclient_timediff;ddddddddddddddddstr
abaxint JagDBServer::redoDinsertLog( const AbaxDataString &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) <= 0 ) {
		return 0;
	}

	int i, fd = jagopen( fpath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) return 0;
	char buf16[17];
	abaxint len;
	char c;
	char *buf = NULL;

	prt(("s3950 redoDinsertLog [%s]\n", fpath.c_str() ));

	abaxint cnt = 0, onecnt = 0;
	AbaxDataString cmd;
	_dbConnector->_parentCliNonRecover->_servCurrentCluster = _dbConnector->_nodeMgr->_curClusterNumber;
	JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> qmap;
	while ( 1 ) {
		// get timediff
		i = 0;
		memset( buf16, 0, 17 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 15 ) {
					jagclose( fd );
					return cnt;
				}
			}
			++i;
		}
		if ( buf16[0] == '\0' ) {
			printf("s3398 end timediff 0 is 0\n");
			break;
		}
		_dbConnector->_parentCliNonRecover->_tdiff = atoi( buf16 );

		memset( buf16, 0, 17 );
		read( fd, buf16, 16 );
		len = jagatoll( buf16 );
		buf = (char*)jagmalloc(len+1);
		memset(buf, 0, len+1);
		read( fd, buf, len );
		cmd = AbaxDataString( buf, len );
		onecnt += _dbConnector->_parentCliNonRecover->oneCmdInsertPool( qmap, cmd );
		if ( onecnt > 99999 ) {
			_dbConnector->_parentCliNonRecover->flushQMap( qmap );
			cnt += onecnt;
			onecnt = 0;
		}
		free( buf );
		buf = NULL;
	}
	_dbConnector->_parentCliNonRecover->flushQMap( qmap );
	cnt += onecnt;
	jagclose( fd );
	return cnt;
}

// open new walactive log
void JagDBServer::resetWalLog()
{
	if ( ! _walLog ) return;
	
    AbaxDataString fpath = JagFileMgr::getLocalLogDir("cmd") + "/walactive.log";
    if ( _fpCommand ) {
        jagfclose( _fpCommand );
    }
	raydebug( stdout, JAG_LOG_LOW, "open new WAL %s\n", fpath.c_str() );

    _fpCommand = loopOpen( fpath.c_str(), "wb" );
	_activeWalFpath = fpath;
	// prt(("s8018 open [%s] _fpCommand=%0x\n", fpath.c_str(), _fpCommand ));
}

// rename walactive to walbackup
void JagDBServer::rotateWalLog()
{
	if ( ! _walLog ) return;
	AbaxDataString backupWalPath = JagFileMgr::getLocalLogDir("cmd") + "/walbackup.log";
	// JAG_BLURT jaguar_mutex_lock ( &g_logmutex ); JAG_OVER;
	jaguar_mutex_lock ( &g_logmutex ); 
    if ( _fpCommand ) {
        jagfclose( _fpCommand );
    }
	// jagsync();

	// find a remote Data-center and fileTransmit to a random server in that data-center
	// AbaxDataString host = findHostInOtherDataCenter();
	// if ( host.size() > 0 ) {
		// fileTransmit( host, 10,  _activeWalFpath, 0 );
	// }

	jagrename( _activeWalFpath.c_str(), backupWalPath.c_str() );
	_fpCommand = loopOpen( _activeWalFpath.c_str(), "wb" );
	jaguar_mutex_unlock ( &g_logmutex );
}


// read uncommited logs in cmd/ dir and execute them
void JagDBServer::recoverWalLog( const AbaxDataString &fpath )
{
	if ( ! _walLog ) return;
	
	if ( JagFileMgr::fileSize( fpath ) < 1 ) return;
	raydebug( stdout, JAG_LOG_LOW, "begin redo %s ...\n",  fpath.c_str() );
	abaxint cnt = redoLog( fpath );
	raydebug( stdout, JAG_LOG_LOW, "end redo %s cnt=%l\n",  fpath.c_str(), cnt );
	jagunlink( fpath.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "unlink %s\n",  fpath.c_str() );
}

// fsync log files
void JagDBServer::syncLogs()
{
	if ( ! _walLog ) return;

    AbaxDataString fullpath = JagFileMgr::getLocalLogDir("cmd");

	struct stat   	statbuf;
	DIR 			*dp;
	struct dirent  	*dirp;
	AbaxDataString  fpath;
	abaxint cnt = 0;
	int  fd;

	if ( stat( fullpath.c_str(), &statbuf) < 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "Error stat [%s], recovery not done\n", fullpath.c_str() );
		return;
	}

	if ( NULL == (dp=opendir(fullpath.c_str())) ) {
		raydebug( stdout, JAG_LOG_LOW, "Error opendir [%s], recovery not done\n", fullpath.c_str() );
		return;
	}

	// raydebug( stdout, JAG_LOG_LOW, "Recover log...\n"); 
	while( NULL != (dirp=readdir(dp)) ) {
		if ( 0==strcmp(dirp->d_name, ".") || 0==strcmp(dirp->d_name, "..") ) {
			// prt(("s6307 dirp->d_name=[%s] skip\n", dirp->d_name ));
			continue;
		}

		fpath = fullpath + "/" + dirp->d_name;
		if ( 0 != strncmp( dirp->d_name, "command-", 8 ) ) { 
			continue; 
		}

		if ( JagFileMgr::fileSize( fpath ) > 0 ) {
			fd = jagopen((char *)fpath.c_str(), O_RDWR|JAG_NOATIME );
			if ( fd >= 0 ) {
				jagfsync( fd );
				raydebug( stdout, JAG_LOG_LOW, "synch %s\n", fpath.c_str() );
				jagclose( fd );
			}
		} else {
			if ( _activeWalFpath != fpath ) {
		    	prt(("s6208 filesize=0  delete %s\n", fpath.c_str() ));
				jagunlink( fpath.c_str() );
			}
		}
	}

	closedir(dp);
}

// execute commands in fpath one by one
// replicateType;client_timediff;isBatch;ddddddddddddddddqstrreplicateType;client_timediff;isBatch;ddddddddddddddddqstr
abaxint JagDBServer::redoLog( const AbaxDataString &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) <= 0 ) {
		return 0;
	}

	int i, fd = jagopen( fpath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) return 0;
	char buf16[17];
	abaxint len;
	char c;
	char *buf = NULL;
	JagRequest req;
	JagSession session( 0 );
	req.hasReply = false;
	req.session = &session;
	session.servobj = this;
	session.dbname = "test";
	session.uid = "admin";
	session.origserv = 1;
	session.drecoverConn = 3;

	prt(("s3949 redoLog [%s]\n", fpath.c_str() ));

	abaxint cnt = 0;
	while ( 1 ) {
		// get replicateType
		i = 0;
		memset( buf16, 0, 17 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 0 ) {
					jagclose( fd );
					return cnt;
				}
			}
			++i;
		}
		if ( buf16[0] == '\0' ) {
			printf("s3398 end replicateType 0 is 0\n");
			break;
		}
		session.replicateType = atoi( buf16 );

		// get timediff
		i = 0;
		memset( buf16, 0, 17 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 15 ) {
					jagclose( fd );
					return cnt;
				}
			}
			++i;
		}
		if ( buf16[0] == '\0' ) {
			printf("s3398 end timediff 0 is 0\n");
			break;
		}
		session.timediff = atoi( buf16 );

		// get isBatch
		i = 0;
		memset( buf16, 0, 17 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 0 ) {
					jagclose( fd );
					return cnt;
				}
			}
			++i;
		}
		if ( buf16[0] == '\0' ) {
			printf("s3398 end isBatch 0 is 0\n");
			break;
		}
		req.batchReply = atoi( buf16 );

		memset( buf16, 0, 17 );
		read( fd, buf16, 16 );
		len = jagatoll( buf16 );
		buf = (char*)jagmalloc(len+1);
		memset(buf, 0, len+1);
		read( fd, buf, len );
		try {
			processMultiCmd( req, buf, len, this, g_lastSchemaTime, g_lastHostTime, 0, true, 1 );
		} catch ( const char *e ) {
			raydebug( stdout, JAG_LOG_LOW, "redo log processMultiCmd [%s] caught exception [%s]\n", buf, e );
		} catch ( ... ) {
			raydebug( stdout, JAG_LOG_LOW, "redo log processMultiCmd [%s] caught unknown exception\n", buf );
		}

		free( buf );
		buf = NULL;
		if ( req.session->replicateType == 0 ) ++cnt;
	}

	jagclose( fd );
	return cnt;
}

// object method
// goto to each table/index, write the bottom level to a disk file
void JagDBServer::flushAllBlockIndexToDisk()
{
	// raydebug( stdout, JAG_LOG_LOW, "Flush block index to disk ...\n" );
	// indexs will be flushed inside table object
	JagTable *ptab; AbaxDataString str;
	for ( int i = 0; i < _faultToleranceCopy; ++i ) {
		raydebug( stdout, JAG_LOG_LOW, "Copy %d/%d:\n", i, _faultToleranceCopy );
		str = _objectLock->getAllTableNames( i );
		JagStrSplit sp( str, '|', true );
		for ( int j = 0; j < sp.length(); ++j ) {
			JagStrSplit sp2( sp[j], '.', true );
			raydebug( stdout, JAG_LOG_LOW, "  Table %s\n", sp[j].c_str() );
			ptab = _objectLock->readLockTable( JAG_INSERT_OP, sp2[0], sp2[1], i, 1 );
			if ( ptab ) {
				ptab->flushBlockIndexToDisk();
				_objectLock->readUnlockTable( JAG_INSERT_OP, sp2[0], sp2[1], i, 1 );
			}
		}
	}
}

// object method
// goto to each table/index, remove the bottom level idx file
void JagDBServer::removeAllBlockIndexInDisk()
{
	removeAllBlockIndexInDiskAll( _tableschema, "/data/" );
	removeAllBlockIndexInDiskAll( _prevtableschema, "/pdata/" );
	removeAllBlockIndexInDiskAll( _nexttableschema, "/ndata/" );
}

void JagDBServer::removeAllBlockIndexInDiskAll( const JagTableSchema *tableschema, const char *datapath )
{
	if ( ! tableschema ) return;
	AbaxDataString dbtab, db, tab, idx, fpath;
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i].c_str();
		//printf("s4490 dbtab=[%s]\n", dbtab.c_str() );
		JagStrSplit sp(dbtab, '.');
		if ( sp.length() >= 2 ) {
			db = sp[0];
			tab = sp[1];
			fpath = jaguarHome() + datapath + db + "/" + tab  + ".bid";
			jagunlink( fpath.c_str() );

			fpath = jaguarHome() + datapath + db + "/" + tab +"/" + tab + ".bid";
			jagunlink( fpath.c_str() );
			//printf("s4401 unlink [%s]\n", fpath.c_str() );
		}
	}
	if ( vec ) delete vec;
}

// obj method
// look at _taskMap and get all TaskIDs with the same threadID
AbaxDataString JagDBServer::getTaskIDsByThreadID( abaxint threadID )
{
	AbaxDataString taskIDs;
	const AbaxPair<AbaxLong,AbaxString> *arr = _taskMap->array();
	abaxint len = _taskMap->arrayLength();
	// sprintf( buf, "%14s  %20s  %16s  %16s  %16s %s\n", "TaskID", "ThreadID", "User", "Database", "StartTime", "Command");
	for ( abaxint i = 0; i < len; ++i ) {
		if ( ! _taskMap->isNull( i ) ) {
			const AbaxPair<AbaxLong,AbaxString> &pair = arr[i];
			if ( pair.key != AbaxLong(threadID) ) { continue; }
			JagStrSplit sp( pair.value.c_str(), '|' );
			// "threadID|userid|dbname|timestamp|query"
			if ( taskIDs.size() < 1 ) {
				taskIDs = sp[0];
			} else {
				taskIDs += AbaxDataString("|") + sp[0];
			}
		}
	}
	return taskIDs;
}

int JagDBServer::schemaChangeCommandSyncRemove( const AbaxDataString & dbobj )
{
	_scMap->removeKey( dbobj );
	return 1;
}

// method to make sure schema change command, such as drop/create table etc, is synchronized among servers using hashmap before actually lock it
// isRemove 0: addKeyValue; isRemove 1: removeKey from hashMap
int JagDBServer::schemaChangeCommandSyncCheck( const JagRequest &req, const AbaxDataString &dbobj, abaxint opcode, int isRemove )
{
	// disable for now
	// return 1;

	if ( isRemove ) {
		_scMap->removeKey( dbobj );
		return 1;
	}

	bool setmap = 0;
	char cond[3] = { 'N', 'G', '\0' };
	if ( _scMap->addKeyValue( dbobj, opcode ) ) {
		cond[0] = 'O'; cond[1] = 'K';
		setmap = 1;
		//prt(("s0393 schemaChangeCommandSyncCheck() addKeyValue true  OK\n" ));
	} else {
		//prt(("s0394 schemaChangeCommandSyncCheck() addKeyValue false NG\n" ));
	}

	//prt(("s4402 schemaChangeCommandSyncCheck() sendMessageLength(cond=%s) ...\n", cond ));
	if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
		if ( setmap > 0 ) _scMap->removeKey( dbobj );
		raydebug( stdout, JAG_LOG_LOW, "s6027 send SS < 0, return 0\n" );
		return 0;
	}
	//prt(("s4402 sendMessageLength() done ...\n" ));

	// waiting for signal; if NG, reject and return
	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *newbuf = NULL;
	// prt(("s5002 schemaChangeCommandSyncCheck() recvData ...\n" ));
	// if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || *(newbuf+1) != 'K' ) {
	if ( recvData( req.session->sock, hdr, newbuf ) >=0 && ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) ) {
		// prt(("s3039 recvData got OK from %s\n", req.session->ip.c_str() ));
		// JagTable::sendMessageLength( req, "OK", 2, "SS" );
		// DONOT send OK to client; if recv OK, do nothing
	} else {
		// connection abort or not OK signal, reject
		if ( setmap > 0 ) _scMap->removeKey( dbobj );
		if ( newbuf ) {
			raydebug( stdout, JAG_LOG_LOW, "s6328 recvData < 0 or not OK, buf=[%s] return 0\n", newbuf );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "s6328 recvData < 0 or not OK, newbuf==NULL return 0\n" );
		}
		if ( newbuf ) free( newbuf );
		return 0;
	}

	if ( newbuf ) free( newbuf );
	// prt(("s5002 schemaChangeCommandSyncCheck() recvData done\n" ));
	return 1;
}

// only flush one table ( and related indexs ) insert buffer or one index insert buffer
// dbobj must be db.tab or db.idx
void JagDBServer::flushOneTableAndRelatedIndexsInsertBuffer( const AbaxDataString &dbobj, int replicateType, int isTable, 
	JagTable *iptab, JagIndex *ipindex )
{
	// prt(("s6630 JagDBServer::flushOneTableAndRelatedIndexsInsertBuffer() _faultToleranceCopy=%d ...\n", _faultToleranceCopy ));
	// indexs will be flushed inside table object
	JagTable *ptab; JagIndex *pindex; AbaxDataString tabName;
	JagTableSchema *tableschema; JagIndexSchema *indexschema;
	getTableIndexSchema( replicateType, tableschema, indexschema );
	JagStrSplit sp( dbobj, '.', true );
	if ( isTable ) {
		if ( !iptab ) {
			ptab = _objectLock->readLockTable( JAG_INSERT_OP, sp[0], sp[1], replicateType, 1 );
		} else {
			ptab = iptab;
		}
		if ( ptab ) {
			ptab->copyAndInsertBufferAndClean( ipindex );
			if ( !iptab ) {
				_objectLock->readUnlockTable( JAG_INSERT_OP, sp[0], sp[1], replicateType, 1 );
			}
		}
	} else {
		if ( !ipindex ) {
			pindex = _objectLock->readLockIndex( JAG_INSERT_OP, sp[0], tabName, sp[1], tableschema, indexschema, replicateType, 1 );
		} else {
			pindex = ipindex;
		}
		if ( pindex ) {
			pindex->copyAndInsertBufferAndClean();
			if ( !ipindex ) {
				_objectLock->readUnlockIndex( JAG_INSERT_OP, sp[0], tabName, sp[1], tableschema, indexschema, replicateType, 1 );
			}
		}
	}
}

// only applied if global locked all 
void JagDBServer::flushAllTableAndRelatedIndexsInsertBuffer()
{
	// prt(("s6630 JagDBServer::flushAllTableAndRelatedIndexsInsertBuffer() _faultToleranceCopy=%d ...\n", _faultToleranceCopy ));
	// indexs will be flushed inside table object
	JagTable *ptab; AbaxDataString str;
	for ( int i = 0; i < _faultToleranceCopy; ++i ) {
		str = _objectLock->getAllTableNames( i );
		JagStrSplit sp( str, '|', true );
		for ( int j = 0; j < sp.length(); ++j ) {
			JagStrSplit sp2( sp[j], '.', true );
			ptab = _objectLock->readLockTable( JAG_INSERT_OP, sp2[0], sp2[1], i, 1 );
			if ( ptab ) {
				ptab->copyAndInsertBufferAndClean();
				_objectLock->readUnlockTable( JAG_INSERT_OP, sp2[0], sp2[1], i, 1 );
			}
		}
	}
}

int JagDBServer::doInsert( JagDBServer *servobj, JagRequest &req, JagParseParam &parseParam, 
							AbaxDataString &reterr, const AbaxDataString &oricmd )
{
	abaxint cnt = 0;
	int insertCode = 0, rc;
	JagTableSchema *tableschema = servobj->getTableSchema( req.session->replicateType );
	AbaxDataString dbobj;
	JagTable *ptab = NULL;
	if ( parseParam.objectVec.size() > 0 ) {
		ptab = servobj->_objectLock->readLockTable( parseParam.opcode, parseParam.objectVec[0].dbName, 
			parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
	}

	if ( ptab ) {
		if ( JAG_INSERT_OP == parseParam.opcode ) {
			cnt = ptab->insert( req, &parseParam, reterr, insertCode, false );
			// prt(("s9383 ptab->insert cnt=%d reterr=[%s]\n", cnt, reterr.c_str() ));
			++ servobj->numInserts;
			if ( 1 == cnt ) {
				servobj->_dbLogger->logmsg( req, "INS", oricmd );
			} else {
				servobj->_dbLogger->logerr( req, reterr, oricmd );
			}
		} else if ( JAG_SINSERT_OP == parseParam.opcode ) {
			cnt = ptab->sinsert( req, &parseParam, reterr, insertCode, false );
			++ servobj->numInserts;
			if ( 1 == cnt ) {
				servobj->_dbLogger->logmsg( req, "INS", oricmd );
			} else {
				servobj->_dbLogger->logerr( req, reterr, oricmd );
			}
		} else if ( JAG_CINSERT_OP == parseParam.opcode ) {
			cnt = ptab->cinsert( req, &parseParam, reterr, insertCode, false );
			if ( cnt > 0 ) {
				JagDBServer::dinsertlogCommand( req.session, oricmd.c_str(), oricmd.length() );
			}
		} else if ( JAG_DINSERT_OP == parseParam.opcode ) {
			cnt = ptab->dinsert( req, &parseParam, reterr, insertCode, false );
		}
		servobj->_objectLock->readUnlockTable( parseParam.opcode, parseParam.objectVec[0].dbName, 
			parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
	} else {
		dbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
		raydebug( stdout, JAG_LOG_HIGH, "s4304 table %s does not exist\n", dbobj.c_str() );
		reterr = "Table " + dbobj + " does not exist";
		return 0;
	}
	
	return 1;
}

// handle signals
#ifndef _WINDOWS64_
int JagDBServer::processSignal( int sig )
{
	if ( sig == SIGHUP ) {
		raydebug( stdout, JAG_LOG_LOW, "Processing SIGHUP ...\n" );
		sig_hup( sig );
	} else if ( sig == SIGINT || sig == SIGTERM ) {
		JagRequest req;
		JagSession session;
		session.uid = "admin";
		session.exclusiveLogin = 1;
		session.servobj = this;
		req.session = &session;
		raydebug( stdout, JAG_LOG_LOW, "Processing [%s(%d)] ...\n", strsignal(sig), sig );
		shutDown( "_ex_shutdown", req );
	} else {
		if ( sig != SIGCHLD && sig != SIGWINCH ) {
			raydebug( stdout, JAG_LOG_LOW, "Unknown signal [%s(%d)] ignored.\n", strsignal(sig), sig );
		}
	}
	return 1;
}
#else
// WINDOWS
int JagDBServer::processSignal( int sig )
{
	if ( sig == JAG_CTRL_HUP ) {
		raydebug( stdout, JAG_LOG_LOW, "Processing SIGHUP ...\n" );
		sig_hup( sig );
	} else if ( sig == JAG_CTRL_CLOSE ) {
		JagRequest req;
		JagSession session;
		session.uid = "admin";
		session.exclusiveLogin = 1;
		session.servobj = this;
		req.session = &session;
		raydebug( stdout, JAG_LOG_LOW, "Processing [%d] ...\n", sig );
		shutDown( "_ex_shutdown", req );
	} else {
		raydebug( stdout, JAG_LOG_LOW, "Unknown signal [%d] ignored.\n", sig );
	}
	return 1;
}

#endif


bool JagDBServer::isInteralIP( const AbaxDataString &ip )
{
	bool rc = false;
	JagStrSplit spp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	for ( int i = 0; i < spp.length(); ++i ) {
		if ( spp[i] == ip ) {
			rc = true;
			break;
		}
	}
	return rc;
}

bool JagDBServer::existInAllHosts( const AbaxDataString &ip )
{
	return isInteralIP( ip );
}

void JagDBServer::initDirs()
{
	AbaxDataString fpath = jaguarHome() + "/data/system/schema";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/pdata/system/schema";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/ndata/system/schema";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/log/cmd";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/log/delta";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/export";
	JagFileMgr::makedirPath( fpath, 0700 );

	fpath = jaguarHome() + "/tmp/*";
	AbaxDataString cmd = AbaxDataString("/bin/rm -rf ") + fpath;
	system( cmd.c_str() );

	fpath = jaguarHome() + "/tmp/data";
	// JagFileMgr::rmdir( fpath, false );
	JagFileMgr::makedirPath( fpath );

	fpath = jaguarHome() + "/tmp/pdata";
	// JagFileMgr::rmdir( fpath, false );
	JagFileMgr::makedirPath( fpath );

	fpath = jaguarHome() + "/tmp/ndata";
	// JagFileMgr::rmdir( fpath, false );
	JagFileMgr::makedirPath( fpath );

	fpath = jaguarHome() + "/tmp/join";
	// JagFileMgr::rmdir( fpath, false );
	JagFileMgr::makedirPath( fpath );

	fpath = jaguarHome() + "/log/delta";
	JagFileMgr::rmdir( fpath, false );
	JagFileMgr::makedirPath( fpath );

}

// negative reply to client
void JagDBServer::noGood( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam )
{
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;

	AbaxDataString uid = parseParam.uid;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		prt(("s1004 return\n" ));
		return;
	}
	servobj->_objectLock->writeLockSchema( req.session->replicateType );

	if ( 0 == req.session->drecoverConn ) {
		prt(("s1208 sendMessageLength cond=[%s] SS\n", cond ));
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			// req.syncDataCenter = true;
			req.syncDataCenter = false;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	servobj->schemaChangeCommandSyncRemove( scdbobj ); 
}

// methods will affect userid or schema
// static  
// createuser uid password
void JagDBServer::createUser( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString uid = parseParam.uid;
	AbaxDataString pass = parseParam.passwd;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		prt(("s4004 return\n" ));
		return;
	}
	servobj->_objectLock->writeLockSchema( req.session->replicateType );
	AbaxString dbpass = uiddb->getValue(uid, JAG_PASS );
	if ( dbpass.size() < 1 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		// prt(("s3508 sendMessageLength cond=[%s] SS\n", cond ));
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	char *md5 = MDString( pass.c_str() );
	AbaxDataString mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->addUser(uid, mdpass, JAG_USER, JAG_WRITE );
	}
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj ); 
}

// static  
// dropuser uid
void JagDBServer::dropUser( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString uid = parseParam.uid;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	servobj->_objectLock->writeLockSchema( req.session->replicateType );
	AbaxString dbpass = uiddb->getValue( uid, JAG_PASS );
	if ( dbpass.size() > 0 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) 
	{
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->dropUser( uid );
	}
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
}

// static  
// changepass uid password
void JagDBServer::changePass( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString uid = parseParam.uid;
	AbaxDataString pass = parseParam.passwd;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		prt(("s3257 changePass return\n"));
		return;
	}
	servobj->_objectLock->writeLockSchema( req.session->replicateType );
	AbaxString dbpass = uiddb->getValue( uid, JAG_PASS );
	if ( dbpass.size() > 0 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			prt(("s0239 changepass return\n" ));
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			prt(("s3239 changepass return\n" ));
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	char *md5 = MDString( pass.c_str() );
	AbaxDataString mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->setValue( uid, JAG_PASS, mdpass );
	}
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] changepass uid=[%s]\n", req.session->uid.c_str(),  parseParam.uid.c_str() );

}

// static
// method to change dfdb ( use db command input by user, not via connection )
// changedb dbname
void JagDBServer::changeDB( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{
	// prt(("s4408 changeDB ...\n" ));
	abaxint len, rc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString jagdatahome = servobj->_cfg->getJDBDataHOME( req.session->replicateType );
    AbaxDataString sysdir = jagdatahome + "/" + parseParam.dbName;
	AbaxDataString scdbobj = sysdir + "." + intToStr( req.session->replicateType );
	prt(("s2238 schemaChangeCommandSyncCheck scdbobj=[%s] parseParam.opcode=%d ...\n", scdbobj.c_str(), parseParam.opcode ));
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	prt(("s2238 schemaChangeCommandSyncCheck scdbobj=[%s] parseParam.opcode=%d done ...\n", scdbobj.c_str(), parseParam.opcode ));

	prt(("s3081 readLockDatabase %s ...\n", parseParam.dbName.c_str()  ));
	rc = servobj->_objectLock->readLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	prt(("s3081 readLockDatabase %s done ...\n", parseParam.dbName.c_str()  ));
	// if ( 0 == strcmp( parseParam.dbName.c_str(), "test" ) || 0 == jagaccess( fpath.c_str(), X_OK ) ) {
	if ( 0 == strcmp( parseParam.dbName.c_str(), "test" ) || 0 == jagaccess( sysdir.c_str(), X_OK ) ) {
		if ( rc ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		// prt(("s2209 sendMessageLength ...\n" ));
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			prt(("s4407 sendMessageLength < 0 \n" ));
			if ( rc ) servobj->_objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
		// prt(("s2209 sendMessageLength done \n" ));
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		prt(("s2204 recvData ...\n" ));
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			prt(("s2407 recvData < 0 or not OK \n" ));
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) servobj->_objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		prt(("s2204 recvData done ...\n" ));
		if ( newbuf ) free( newbuf );
	}
	req.session->dbname = parseParam.dbName;
	servobj->_objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	// prt(("s4402 schemaChangeCommandSyncCheck ...\n" ));
	// servobj->schemaChangeCommandSyncCheck( req, fpath, parseParam.opcode, 1 );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	prt(("s4402 schemaChangeCommandSyncCheck done ...\n" ));
}

// static 
// createdb dbname
void JagDBServer::createDB( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{
	abaxint len, rc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString jagdatahome = servobj->_cfg->getJDBDataHOME( req.session->replicateType );
    AbaxDataString sysdir = jagdatahome + "/" + parseParam.dbName;
	AbaxDataString scdbobj = sysdir + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	rc = servobj->_objectLock->writeLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	if ( JagFileMgr::isDir( sysdir ) < 1 && rc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
    jagmkdir( sysdir.c_str(), 0700 );
	servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
}

// static  
// dropdb dbname
void JagDBServer::dropDB( JagRequest &req, JagDBServer *servobj, 
	JagParseParam &parseParam, abaxint threadQueryTime )
{

	abaxint len, rc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString jagdatahome = servobj->_cfg->getJDBDataHOME( req.session->replicateType );
    AbaxDataString sysdir = jagdatahome + "/" + parseParam.dbName;
	AbaxDataString scdbobj = sysdir + "." + intToStr( req.session->replicateType );

	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	rc = servobj->_objectLock->writeLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	if ( JagFileMgr::isDir( sysdir ) > 0 && rc ) {
		cond[0] = 'O'; cond[1] = 'K';
	} 
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( parseParam.hasForce ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	// when doing dropDB, createIndexLock && tableUsingLock needs to be locked to protect tables
	req.session->spCommandReject = 0;
	JagTableSchema *tableschema = servobj->getTableSchema( req.session->replicateType );
	// drop all tables and indexes under this database
	dropAllTablesAndIndexUnderDatabase( req, servobj, tableschema, parseParam.dbName );
    JagFileMgr::rmdir( sysdir );
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	servobj->_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	if ( parseParam.hasForce ) {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] force dropped database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
	} else {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] dropped database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
	}
}

// create table schema
int JagDBServer::createTable( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, 
		JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime )
{
	bool found = false, found2 = false;
	int repType =  req.session->replicateType;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	if ( JAG_CREATECHAIN_OP == parseParam->opcode ) {
		parseParam->isChainTable = 1;
	}  else {
		parseParam->isChainTable = 0;
	}
	// prt(("s9330 createTable isChainTable=%d\n", parseParam->isChainTable ));

	abaxint len, len2, rc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbtable + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		prt(("s4982 createTable return 0 %s reptype=%d\n", scdbobj.c_str(), repType ));
		return 0;
	}
	// for createtable, write lock db first, insert schema then lock table
	prt(("s7723 writeLockDatabase %s reptype=%d ...\n", dbname.c_str(), repType )); 
	rc = servobj->_objectLock->writeLockDatabase( parseParam->opcode, dbname, repType );
	prt(("s7723 writeLockDatabase %s is locked reptype=%d rc=%d\n", dbname.c_str(), repType, rc )); 
	found = indexschema->tableExist( dbname, parseParam );
	found2 = tableschema->existAttr( dbtable );
	//prt(("s5221 indexschema->tableExist(%s) found=%d\n", dbname.c_str(), found ));
	//prt(("s5222 tableschema->existAttr(%s) found2=%d\n", dbtable.c_str(), found2 ));
	if ( !found && !found2 && rc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	prt(("s5839 createTable cond=[%s] reptype=%d\n", cond, repType ));
	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			// prt(("s5003 return 0\n"));
			prt(("s3203 writeUnlockDatabase return 0\n" ));
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			prt(("s5004 createTable %s return 0\n", scdbobj.c_str()));
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
			// prt(("s2039 syncDataCenter = true\n" ));
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	//prt(("s4029 jaguar_mutex_lock insert schema ...\n" ));
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	rc = tableschema->insert( parseParam );
	//prt(("s4029 jaguar_mutex_lock insert schema done rc=%d\n", rc ));
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	prt(("s4025 refreshSchemaInfo done.\n" ));
	//prt(("s4026 Thrd=%lld lccking %s reptype=%d ...\n", THREADID, parseParam->objectVec[0].tableName.c_str(), repType ));

	if ( servobj->_objectLock->writeLockTable( parseParam->opcode, dbname, 
			parseParam->objectVec[0].tableName, tableschema, repType, 1 ) ) {
		//prt(("s8273 Thrd=%lld locked %s reptype=%d \n", THREADID, parseParam->objectVec[0].tableName.c_str(), repType ));
		servobj->_objectLock->writeUnlockTable( parseParam->opcode, dbname, 
			parseParam->objectVec[0].tableName, tableschema, repType, 1 );
		//prt(("s8273 Thrd=%lld unlocked %s reptype=%d \n", THREADID, parseParam->objectVec[0].tableName.c_str(), repType ));
	}
	servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	prt(("s7523 writeUnlockDatabase %s is unlocked reptype=%d\n", dbname.c_str(), repType )); 
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	if ( parseParam->isChainTable ) {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create chain [%s] done reptype=%d\n", 
				req.session->uid.c_str(), dbtable.c_str(), repType );
	} else {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create table [%s] done reptype=%d\n", 
				req.session->uid.c_str(), dbtable.c_str(), repType );
	}

	return 1;
}

// create memtable schema
int JagDBServer::createMemTable( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, 
							     JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime )
{
	bool found = false, found2 = false;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	abaxint len, rc;
	int repType = req.session->replicateType;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbtable + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	parseParam->isMemTable = 1;
	prt(("s3523 writeLockDatabase %s is being locked reptype=%da ...\n", dbname.c_str(), repType )); 
	rc = servobj->_objectLock->writeLockDatabase( parseParam->opcode, dbname, repType );
	prt(("s3523 writeLockDatabase %s is locked reptype=%d\n", dbname.c_str(), repType )); 
	found = indexschema->tableExist( dbname, parseParam );
	found2 = tableschema->existAttr( dbtable );
	if ( !found && !found2 && rc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) 
	{
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}
	
	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			prt(("s3523 writeUnlockDatabase %s is unlocked reptype=%d\n", dbname.c_str(), repType )); 
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	tableschema->insert( parseParam );
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );

	if ( servobj->_objectLock->writeLockTable( parseParam->opcode, dbname, 
			parseParam->objectVec[0].tableName, tableschema, repType, 1 ) ) {
		servobj->_objectLock->writeUnlockTable( parseParam->opcode, dbname, 
			parseParam->objectVec[0].tableName, tableschema, repType, 1 );
	}
	servobj->_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create memtable [%s]\n", req.session->uid.c_str(), dbtable.c_str() );
	return 1;
}

// static method
// new method for createIndex to deal with asc index keys
//  return 0: error with reterr;  1: success
int JagDBServer::createIndex( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, JagParseParam *parseParam,
							   JagTable *&ptab, JagIndex *&pindex, AbaxDataString &reterr, abaxint threadQueryTime )
{
	// create index on schema or get errmsg
	// prt(("s1206 create index on schema\n"));
	JagIndexSchema *indexschema; JagTableSchema *tableschema;
	servobj->getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	AbaxDataString dbindex = dbname + "." + parseParam->objectVec[0].tableName + 
							 "." + parseParam->objectVec[1].indexName;

	AbaxDataString tgttab = indexschema->getTableNameScan( dbname, parseParam->objectVec[1].indexName );
	if ( tgttab.size() > 0 ) {
		servobj->noGood( req, servobj, *parseParam );
		prt(("E2033 index [%s] already exists in database [%s]\n", parseParam->objectVec[1].indexName.c_str(), dbname.c_str() ));
		return 0;
	}

	int rc = 0;
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbobj + "." + intToStr( req.session->replicateType );

	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		prt(("s4983 createIndex return 0 scdbobj=[%s]\n", scdbobj.c_str() ));
		return 0;
	}

	//prt(("s2263 createIndex scdbobj=[%s] writeLockTable(%s) ...\n", scdbobj.c_str(), parseParam->objectVec[0].tableName.c_str() ));
	ptab = servobj->_objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	if ( ptab ) {
		// prt(("s0921 got ptab createIndexSchema() %s ...\n", scdbobj.c_str() ));
		rc = createIndexSchema( req, servobj, ptab, dbname, parseParam, reterr );
		// prt(("s0921 got ptab createIndexSchema() rc=%d ...\n", rc ));
		if ( rc ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	} else {
		prt(("s0921 got NO ptab %s\n", scdbobj.c_str() ));
	}
	//prt(("s5603 ptab=%0x\n", ptab ));
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) {
				// rollback index schema
				prt(("E3481 SS sent error remove dbindex ...\n" ));
				JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	    		indexschema->remove( dbindex );
				jaguar_mutex_unlock ( &g_dbmutex );
				prt(("E3481 SS sent error remove dbindex done\n" ));
			}

			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
							parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			//prt(("s3012 return 0 %s\n", scdbobj.c_str() ));
			return 0;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		// prt(("s3052 recvData %s ...\n", scdbobj.c_str() ));
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) {
				// rollback index schema
				// prt(("s4401 remove dbindex %s ...\n", scdbobj.c_str() ));
				JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	    		indexschema->remove( dbindex );
				jaguar_mutex_unlock ( &g_dbmutex );
				// prt(("s4401 remove dbindex done\n" ));
			}
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
							parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			//prt(("s3081 return 0 %s\n", scdbobj.c_str()  ));
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
			// prt(("s3481 syncDataCenter = true\n" ));
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	// prt(("s5092 refreshSchemaInfo...\n" ));
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	// prt(("s5092 refreshSchemaInfo done\n" ));

	pindex = servobj->_objectLock->writeLockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
					parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
					tableschema, indexschema, req.session->replicateType, 1 );
	if ( ptab && pindex ) {
		// if successfully create index, begin process table's data
		//prt(("s6021 flushOneTableAndRelatedIndexsInsertBuffer %s ...\n", dbobj.c_str() ));
		servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, pindex );
		//prt(("s6021 flushOneTableAndRelatedIndexsInsertBuffer %s done\n", dbobj.c_str() ));
		doCreateIndex( ptab, pindex, servobj );
		//prt(("s6022 doCreateIndex %s done\n", dbobj.c_str() ));
		//after create index, flush this index's data
		servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 0, ptab, pindex );
		//prt(("s6023 flushOneTableAndRelatedIndexsInsertBuffer %s done\n", dbobj.c_str() ));
		servobj->_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
			parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
			tableschema, indexschema, req.session->replicateType, 1 );		
		rc = 1;
	} else {
		// rollback index schema
		// prt(("s6086 remove dbindex %s ...\n", dbindex.c_str() ));
		JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	    indexschema->remove( dbindex );
		jaguar_mutex_unlock ( &g_dbmutex );
		//prt(("s6086 remove dbindex %s done pindex=%0x ...\n", dbindex.c_str(), pindex ));
		rc = 0;
	}

	if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
					parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create index [%s]\n", req.session->uid.c_str(), scdbobj.c_str() );
	return rc;
}

int JagDBServer::renameColumn( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname,
							   const JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime, 
							   abaxint &threadSchemaTime )
{
	// prt(("s6028 renameColumn...\n"));

	JagTableSchema *tableschema = servobj->getTableSchema( req.session->replicateType );
	JagTable *ptab = NULL;
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString dbtable = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbtable + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	ptab = servobj->_objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	if ( ptab && tableschema->existAttr( dbtable ) ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) 
	{
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	// for add column command, if new column's length is larger than spare_ remains length, reject
	if ( parseParam->createAttrVec.size() > 0 ) {
		if ( !tableschema->checkSpareRemains( dbtable, parseParam ) ) {
			cond[0] = 'N'; cond[1] = 'G';
		}
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
							parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbtable, req.session->replicateType, 1, ptab, NULL );
	req.session->spCommandReject = 0;
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;	
	// prt(("s0938 ptab->refreshSchema();...\n"));
	if ( ptab ) {
		tableschema->renameColumn( dbtable, parseParam );
		ptab->refreshSchema();
	}

	if ( parseParam->createAttrVec.size() < 1 ) {
		// if rename column
		if ( ptab ) {
			ptab->renameIndexColumn( parseParam, reterr );
		}
	}
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );

	// bcast schema change
	servobj->sendMapInfo( "_cschema", req );
	//servobj->sendMapInfo( "_cdefval", req );
	threadSchemaTime = g_lastSchemaTime;
	// prt(("s6029 done\n"));
	return 1;
}

// return 1: OK  0: error
int JagDBServer::dropTable( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, 
							JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime )
{
	AbaxDataString dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbobj + "." + intToStr( req.session->replicateType );

	prt(("s0236 dropTable dbobj=[%s] scdbobj=[%s] ...\n", dbobj.c_str(), scdbobj.c_str() ));
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		prt(("s7303 error dropTable SyncCheck return 0\n"));
		return 0;
	}
	prt(("s6263 dropTable continue ...\n" ));

	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = servobj->_objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );	
	if ( ptab ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	prt(("s8379 dropTable got lock ptab=%0x cond=[%s]\n", ptab, cond ));
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( parseParam->hasForce ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	// if not from dead server (self or other). 0 is from client
	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			// client conn disconnected
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	AbaxDataString dbtabidx;
	if ( ptab ) {
		servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
		// prt(("s4941 ptab->drop() ...\n"));
		ptab->drop( reterr );
	}
	dbtabidx = dbname + "." + parseParam->objectVec[0].tableName; 

	prt(("s4950 mutext lock ...\n" ));
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	prt(("s4951 tableschema->remove(%s) ...\n", dbtabidx.c_str() ));
	tableschema->remove( dbtabidx );
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	
	// drop table and related indexs
	if ( ptab ) {
		prt(("s49403 delete ptab ...\n"));
		delete ptab; 
	}
	ptab = NULL;
	servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	prt(("s0283 droptable done\n"));
	if ( parseParam->hasForce ) {
		raydebug( stdout, JAG_LOG_LOW, "user [%s] force drop table [%s]\n", req.session->uid.c_str(), dbtabidx.c_str() );		
	} else {
		raydebug( stdout, JAG_LOG_LOW, "user [%s] drop table [%s]\n", req.session->uid.c_str(), dbtabidx.c_str() );		
	}
	return 1;
}

// return 1: OK  0: error
int JagDBServer::dropIndex( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, 
							JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime )
{
	AbaxDataString dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbobj + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = servobj->_objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );	

	pindex = servobj->_objectLock->writeLockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
			parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
			tableschema, indexschema, req.session->replicateType, 1 );

	if ( ptab ) {
		if ( pindex ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( pindex ) servobj->_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
				parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
				tableschema, indexschema, req.session->replicateType, 1 );
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			return 0;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( pindex ) servobj->_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
				parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
				tableschema, indexschema, req.session->replicateType, 1 );
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	if ( ! pindex ) {
		// prt(("s5930 dropIndex pindex is NULL\n" ));
		// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
		servobj->schemaChangeCommandSyncRemove( scdbobj );
		return 0;
	}
	
	if ( ! ptab ) {
		// prt(("s5931 dropIndex ptab is NULL\n" ));
		// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
		servobj->schemaChangeCommandSyncRemove( scdbobj );
		return 0;
	}

	req.session->spCommandReject = 0;
	AbaxDataString dbtabidx;
	servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 0, ptab, pindex );
	pindex->drop();
	dbtabidx = dbname + "." + parseParam->objectVec[0].tableName + "." + parseParam->objectVec[1].indexName;

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	indexschema->remove( dbtabidx );
	ptab->dropFromIndexList( parseParam->objectVec[1].indexName );	
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	
	// drop one index
	delete pindex; pindex = NULL;
	servobj->_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
		parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
		tableschema, indexschema, req.session->replicateType, 1 );
	servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	raydebug( stdout, JAG_LOG_LOW, "user [%s] drop index [%s]\n", req.session->uid.c_str(), dbtabidx.c_str() );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
	return 1;
}

int JagDBServer::truncateTable( JagRequest &req, JagDBServer *servobj, const AbaxDataString &dbname, 
							JagParseParam *parseParam, AbaxDataString &reterr, abaxint threadQueryTime )
{
	AbaxDataString dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	AbaxDataString scdbobj = dbobj + "." + intToStr( req.session->replicateType );
	AbaxDataString indexNames;
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = servobj->_objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );

	if ( ptab ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {		
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( ptab ) servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	if ( ptab ) {
		indexNames = ptab->drop( reterr, true );
	}
	refreshSchemaInfo( req, servobj, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbmutex );
	
	if ( ptab ) {
		delete ptab; ptab = NULL;
		// rebuild ptab, and possible related indexs
		ptab = servobj->_objectLock->writeTruncateTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
			parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) {
			JagStrSplit sp( indexNames, '|', true );
			for ( int i = 0; i < sp.length(); ++i ) {
				pindex = servobj->_objectLock->writeLockIndex( JAG_CREATEINDEX_OP, parseParam->objectVec[0].dbName,
					parseParam->objectVec[0].tableName, sp[i],
					tableschema, indexschema, req.session->replicateType, 1 );
				if ( pindex ) {
					ptab->_indexlist.append( pindex->getIndexName() );
				}
				if ( pindex ) servobj->_objectLock->writeUnlockIndex( JAG_CREATEINDEX_OP, parseParam->objectVec[0].dbName,
								parseParam->objectVec[0].tableName, sp[i],
								tableschema, indexschema, req.session->replicateType, 1 );
			}
			servobj->_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
				parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		}
		AbaxDataString dbtab = dbname + "." + parseParam->objectVec[0].tableName;
		raydebug( stdout, JAG_LOG_LOW, "user [%s] truncate table [%s]\n", req.session->uid.c_str(), dbtab.c_str() );
	}
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	servobj->schemaChangeCommandSyncRemove( scdbobj );

	return 1;
}


// "_" cmds, server processing object methods

// send map info to client if requested
// pmesg: "_cschema"
void JagDBServer::sendMapInfo( const char *mesg, const JagRequest &req )
{	
	//if ( 0==strcmp(mesg, "_cschema") ) {
		if ( !req.session->origserv && !_restartRecover ) {	
			AbaxDataString schemaInfo;
			_dbConnector->_broadCastCli->getSchemaMapInfo( schemaInfo );
			// prt(("send map info=[%s]\n", schemaInfo.c_str()));
			if ( schemaInfo.size() > 0 ) {
				// prt(("s3330 sendMapInfo schemaInfo=[%s]\n", schemaInfo.c_str() ));
				JagTable::sendMessageLength( req, schemaInfo.c_str(), schemaInfo.size(), "SC" );
			}
		}
		return;
	//}

	/***
	if ( 0==strcmp(mesg, "_cdefval") ) {
		if ( !req.session->origserv && !_restartRecover ) {	
			AbaxDataString info = _tableschema->getAllDefVals(); 
			// "db.tab.col1=kkdkddk|db.tab.col2=kdkfkdfkd|..."
			// prt(("send map info=[%s]\n", schemaInfo.c_str()));
			if ( info.size() > 0 ) {
				// prt(("s3330 sendMapInfo schemaInfo=[%s]\n", schemaInfo.c_str() ));
				JagTable::sendMessageLength( req, info.c_str(), info.size(), "DF" );
			}
		}
		return;
	}
	***/

}

// send host info to client if requested
// pmesg: "_chost"
void JagDBServer::sendHostInfo( const char *mesg, const JagRequest &req )
{	
	if ( !req.session->origserv && !_restartRecover ) {	
		AbaxDataString snodes = _dbConnector->_nodeMgr->_sendNodes;
		JagTable::sendMessageLength( req, snodes.c_str(), snodes.size(), "HL" );
	}
}

// checkdelta, check if current server has non-empty delta file for session->ip server
// pmesg: "_serv_checkdelta"
void JagDBServer::checkDeltaFiles( const char *mesg, const JagRequest &req )
{
	AbaxDataString str;
	if ( _actdelPOhost == req.session->ip && JagFileMgr::fileSize(_actdelPOpath) > 0 ) {
		// prt(("checkdelta error 1=[%s]\n", req.session->ip.c_str()));
		str = _actdelPOpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelPRhost == req.session->ip && JagFileMgr::fileSize(_actdelPRpath) > 0 ) {
		// prt(("checkdelta error 2=[%s]\n", req.session->ip.c_str()));
		str = _actdelPRpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelPORhost == req.session->ip && JagFileMgr::fileSize(_actdelPORpath) > 0 ) {
		// prt(("checkdelta error 3=[%s]\n", req.session->ip.c_str()));
		str = _actdelPORpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNOhost == req.session->ip && JagFileMgr::fileSize(_actdelNOpath) > 0 ) {
		// prt(("checkdelta error 4=[%s]\n", req.session->ip.c_str()));
		str = _actdelNOpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNRhost == req.session->ip && JagFileMgr::fileSize(_actdelNRpath) > 0 ) {
		// prt(("checkdelta error 5=[%s]\n", req.session->ip.c_str()));
		str = _actdelNRpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNORhost == req.session->ip && JagFileMgr::fileSize(_actdelNORpath) > 0 ) { 
		// prt(("checkdelta error 6=[%s]\n", req.session->ip.c_str()));
		str = _actdelNORpath + " not empty";
		JagTable::sendMessage( req, str.c_str(), "OK" );
	}
}

// crecover, clean recover server(s)
// pmesg: "_serv_crecover"
void JagDBServer::cleanRecovery( const char *mesg, const JagRequest &req )
{
	if ( req.session->servobj->_restartRecover ) return;					
	if ( _faultToleranceCopy <= 1 ) return; // no replicate

	// first, ask other servers to see if current server has delta recover file; if yes, return ( not up-to-date files )
	AbaxDataString unixSocket = AbaxDataString("/TOKEN=") + _servToken;
	JaguarCPPClient reqcli;
	if ( !reqcli.connect( _dbConnector->_nodeMgr->_selfIP.c_str(), _port, "admin", "dummy", "test", unixSocket.c_str(), JAG_SERV_PARENT ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4058 crecover check failure, unable to make connection ...\n" );
		reqcli.close();
		return;
	}

	AbaxDataString bcasthosts = getBroadCastRecoverHosts( _faultToleranceCopy );
	prt(("s8120 bcasthosts=[%s]\n", bcasthosts.c_str() ));
	AbaxDataString resp = _dbConnector->broadCastGet( "_serv_checkdelta", bcasthosts, &reqcli );
	JagStrSplit checksp( resp, '\n', true );
	if ( checksp.length() > 1 || checksp[0].length() > 0 ) {
		// prt(("request clean recover, but not done=[%s]\n", req.session->ip.c_str()));
		reqcli.close();
		return;
	}
	reqcli.close();

	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	raydebug( stdout, JAG_LOG_LOW, "begin proc request of clean redo from %s ...\n", req.session->ip.c_str() );
	_objectLock->writeLockSchema( -1 );
	flushAllTableAndRelatedIndexsInsertBuffer();
	_objectLock->writeUnlockSchema( -1 );
	jagsync();
	raydebug( stdout, JAG_LOG_LOW, "begin cleanRecovery ...\n");
	abaxint reqservi;
	req.session->servobj->_internalHostNum->getValue(req.session->ip, reqservi);
	int pos1, pos2, pos3, pos4, rc;
	AbaxDataString filePath, passwd = "dummy";
	unsigned int uport = _port;
	if ( _faultToleranceCopy == 2 ) {
		if ( reqservi == 0 ) {
			pos1 = reqservi+1;
			pos2 = req.session->servobj->_numPrimaryServers-1;
		} else if ( reqservi == req.session->servobj->_numPrimaryServers-1 ) {
			pos1 = 0;
			pos2 = reqservi-1;
		} else {
			pos1 = reqservi+1;
			pos2 = reqservi-1;
		}

		if ( pos1 == _nthServer ) {
			// perpare to copy prev dir to original data dir
			if ( _objectLock->getnumObjects( 1, 1 ) > 0 ) {	
				organizeCompressDir( 1, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 0, filePath, 0 );
			}
		} 
		if ( pos2 == _nthServer ) {
			// prepare to copy original data dir to prev dir
			if ( _objectLock->getnumObjects( 1, 0 ) > 0 ) {	
				organizeCompressDir( 0, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 1, filePath, 0 );
			}
		}
		// else ignore
	} else if ( _faultToleranceCopy == 3 ) {
		if ( reqservi == 0 ) {
			pos1 = reqservi+1;
			pos2 = req.session->servobj->_numPrimaryServers-1;
		} else if ( reqservi == req.session->servobj->_numPrimaryServers-1 ) {
			pos1 = 0;
			pos2 = reqservi-1;
		} else {
			pos1 = reqservi+1;
			pos2 = reqservi-1;
		}
		if ( pos1 == sp.length()-1 ) {
			pos3 = 0;
		} else {
			pos3 = pos1+1;
		}
		if ( pos2 == 0 ) {
			pos4 = sp.length()-1;
		} else {
			pos4 = pos2-1;
		}

		if ( pos1 == _nthServer ) {
			// perpare to copy prev dir to original data dir
			if ( _objectLock->getnumObjects( 1, 1 ) > 0 ) {	
				organizeCompressDir( 1, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 0, filePath, 0 );
			}

			// prepare to copy original data dir to next dir
			if ( _objectLock->getnumObjects( 1, 0 ) > 0 ) {	
				organizeCompressDir( 0, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 2, filePath, 0 );
			}
		}
		if ( pos2 == _nthServer ) {
			// perpare to copy next dir to original data dir
			if ( _objectLock->getnumObjects( 1, 2 ) > 0 ) {	
				organizeCompressDir( 2, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 0, filePath, 0 );
			}

			// prepare to copy original data dir to prev dir
			if ( _objectLock->getnumObjects( 1, 0 ) > 0 ) {	
				organizeCompressDir( 0, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 1, filePath, 0 );
			}
		}
		if ( pos3 == _nthServer ) {
			// perpare to copy prev dir to next dir
			if ( _objectLock->getnumObjects( 1, 1 ) > 0 ) {	
				organizeCompressDir( 1, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 2, filePath, 0 );
			}
		}
		if ( pos4 == _nthServer ) {
			// perpare to copy next dir to prev dir
			if ( _objectLock->getnumObjects( 1, 2 ) > 0 ) {	
				organizeCompressDir( 2, filePath );
				rc = fileTransmit( req.session->ip, uport, passwd, unixSocket, 1, filePath, 0 );
			}
		}
	}

	raydebug( stdout, JAG_LOG_LOW, "end cleanRecovery\n");
	raydebug( stdout, JAG_LOG_LOW, "end proc request of clean redo from %s\n", req.session->ip.c_str() );
}

// method to receive tar.gz recovery file
// pmesg: "_serv_beginftransfer|0|123456|sender_tid" or "_serv_addbeginftransfer|0|123456|sender_tid"
void JagDBServer::recoveryFileReceiver( const char *mesg, const JagRequest &req )
{
	if ( req.session->drecoverConn != 2 ) {
		prt(("s4209 recoveryFileReceiver drecoverConn=%d not 2 return\n", req.session->drecoverConn ));
		return;
	}
	raydebug( stdout, JAG_LOG_LOW, "begin proc request of xfer from %s ...\n", req.session->ip.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "begin recoveryFileReceiver ...\n" );
	
	JagStrSplit sp( mesg, '|', true );
	if ( sp.length() < 4 ) return;
	int fpos = atoi(sp[1].c_str());
	size_t rlen;
	abaxint fsize = jagatoll(sp[2].c_str());
	abaxint memsize = 128*1024*1024;
	abaxint totlen = 0;
	abaxint recvlen = 0;
	AbaxDataString recvpath = req.session->servobj->_cfg->getTEMPDataHOME( fpos ) + "/" + req.session->ip + "_" + sp[3] + ".tar.gz";
	int fd = jagopen( recvpath.c_str(), O_CREAT|O_RDWR|O_TRUNC, S_IRWXU);
	raydebug( stdout, JAG_LOG_LOW, "s6207 open recvpath=[%s]\n", recvpath.c_str() );
	if ( fd < 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "s6208 error open recvpath=[%s]\n", recvpath.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error open\n" );
		return;
	}

	raydebug( stdout, JAG_LOG_LOW, "expect to recv %l bytes \n", fsize );
	char *buf =(char*)jagmalloc(memsize);
	
	while( 1 ) {
		if ( totlen >= fsize ) break;
		if ( fsize-totlen < memsize ) {
			recvlen = fsize-totlen;
		} else {
			recvlen = memsize;
		}
		rlen = recvData( req.session->sock, buf, recvlen );
		if ( rlen < recvlen ) {
			if ( buf ) free ( buf );
			jagclose( fd );
			jagunlink( recvpath.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error recvdata\n" );
			return;
		}
		rlen = raysafewrite( fd, buf, recvlen );
		if ( rlen < recvlen ) {
			if ( buf ) free ( buf );
			jagclose( fd );
			jagunlink( recvpath.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error savedata\n" );
			return;
		}
		totlen += rlen;
	}
	jagfdatasync( fd );
	jagclose( fd );
	raydebug( stdout, JAG_LOG_LOW, "recved %l bytes\n", totlen );
	
	rlen = recvData( req.session->sock, buf, 3 );
	raydebug( stdout, JAG_LOG_LOW, "recved %d try bytes b0=[%c] b1=[%c] b2=[%c]\n", rlen, buf[0], buf[1], buf[2] );

	// check number of bytes
	struct stat sbuf;
	if ( 0 != stat(recvpath.c_str(), &sbuf) || sbuf.st_size != fsize || totlen != fsize ||
		rlen < 3 || memcmp( buf, "try", 3 ) != 0 ) {
		// incorrect number of bytes of file, remove file
		jagunlink( recvpath.c_str() );
	} else {
		JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
		if ( 0 == fpos ) {
			if ( _crecoverFpath.size() < 1 ) {
				_crecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_crecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		} else if ( 1 == fpos ) {
			if ( _prevcrecoverFpath.size() < 1 ) {
				_prevcrecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_prevcrecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		} else if ( 2 == fpos ) {
			if ( _nextcrecoverFpath.size() < 1 ) {
				_nextcrecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_nextcrecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		}
		jaguar_mutex_unlock ( &g_dlogmutex );
		sendData( req.session->sock, "end", 3 );
		raydebug( stdout, JAG_LOG_LOW, "sent 3 end bytes\n" );
		raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver OK\n" );
	}
	if ( buf ) free ( buf );
	raydebug( stdout, JAG_LOG_LOW, "end proc request of xfer from %s\n", req.session->ip.c_str() );
}

// method to receive tar.gz recovery file
// pmesg: "_serv_beginftransfer|10|123456|sender_tid" 
void JagDBServer::walFileReceiver( const char *mesg, const JagRequest &req )
{
	#if 0
	if ( req.session->drecoverConn != 2 ) return;
	raydebug( stdout, JAG_LOG_LOW, "begin proc request of xfer from %s ...\n", req.session->ip.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "begin walFileReceiver ...\n" );
	
	JagStrSplit sp( mesg, '|', true );
	if ( sp.length() < 4 ) return;
	int fpos = atoi(sp[1].c_str());
	size_t rlen;
	abaxint fsize = jagatoll(sp[2].c_str());
	abaxint memsize = 128*1024*1024;
	abaxint totlen = 0;
	abaxint recvlen = 0;
	AbaxDataString recvpath = req.session->servobj->_cfg->getTEMPDataHOME( fpos ) + "/" + req.session->ip + "_" + sp[3] + ".wal";
	int fd = jagopen( recvpath.c_str(), O_CREAT|O_RDWR|O_TRUNC, S_IRWXU);
	raydebug( stdout, JAG_LOG_LOW, "s6217 open recvpath=[%s]\n", recvpath.c_str() );
	if ( fd < 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "s6218 error open recvpath=[%s]\n", recvpath.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "end walFileReceiver error open\n" );
		return;
	}

	raydebug( stdout, JAG_LOG_LOW, "expect to recv %l bytes \n", fsize );
	char *buf =(char*)jagmalloc(memsize);
	
	while( 1 ) {
		if ( totlen >= fsize ) break;
		if ( fsize-totlen < memsize ) {
			recvlen = fsize-totlen;
		} else {
			recvlen = memsize;
		}
		rlen = recvData( req.session->sock, buf, recvlen );
		if ( rlen < recvlen ) {
			if ( buf ) free ( buf );
			jagclose( fd );
			jagunlink( recvpath.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "end walFileReceiver error recvdata\n" );
			return;
		}
		rlen = raysafewrite( fd, buf, recvlen );
		if ( rlen < recvlen ) {
			if ( buf ) free ( buf );
			jagclose( fd );
			jagunlink( recvpath.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "end walFileReceiver error savedata\n" );
			return;
		}
		totlen += rlen;
	}
	jagfdatasync( fd );
	jagclose( fd );
	raydebug( stdout, JAG_LOG_LOW, "recved %l bytes\n", totlen );
	
	rlen = recvData( req.session->sock, buf, 3 );
	raydebug( stdout, JAG_LOG_LOW, "recved %d try bytes b0=[%c] b1=[%c] b2=[%c]\n", rlen, buf[0], buf[1], buf[2] );

	// check number of bytes
	struct stat sbuf;
	if ( 0 != stat(recvpath.c_str(), &sbuf) || sbuf.st_size != fsize || totlen != fsize ||
		rlen < 3 || memcmp( buf, "try", 3 ) != 0 ) {
		// incorrect number of bytes of file, remove file
		jagunlink( recvpath.c_str() );
	} else {
		// JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
		if ( 10 == fpos ) {
			if ( _walrecoverFpath.size() < 1 ) {
				_walrecoverFpath = req.session->ip + "_" + sp[3] + ".wal";
			} else {
				_walrecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".wal";
			}
		} else if ( 11 == fpos ) {
			if ( _prevwalrecoverFpath.size() < 1 ) {
				_prevwalrecoverFpath = req.session->ip + "_" + sp[3] + ".wal";
			} else {
				_prevwalrecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".wal";
			}
		} else if ( 12 == fpos ) {
			if ( _nextwalrecoverFpath.size() < 1 ) {
				_nextwalrecoverFpath = req.session->ip + "_" + sp[3] + ".wal";
			} else {
				_nextwalrecoverFpath += AbaxDataString("|") + req.session->ip + "_" + sp[3] + ".wal";
			}
		}
		// jaguar_mutex_unlock ( &g_dlogmutex );
		sendData( req.session->sock, "end", 3 );
		raydebug( stdout, JAG_LOG_LOW, "sent 3 end bytes\n" );
		raydebug( stdout, JAG_LOG_LOW, "end walFileReceiver OK\n" );
	}
	if ( buf ) free ( buf );
	raydebug( stdout, JAG_LOG_LOW, "end proc request of xfer from %s\n", req.session->ip.c_str() );

	raydebug( stdout, JAG_LOG_LOW, "begin do %s ...\n",  recvpath.c_str() );
	abaxint cnt = redoLog( recvpath );
	raydebug( stdout, JAG_LOG_LOW, "end do %s cnt=%l\n",  recvpath.c_str(), cnt );
	jagunlink( recvpath.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "unlink %s\n",  recvpath.c_str() );
	#endif
}

// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
// pmesg: "_serv_opinfo"
void JagDBServer::sendOpInfo( const char *mesg, const JagRequest &req )
{
	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	int nsrv = sp.length(); 

	int dbs, tabs;
	numDBTables( dbs, tabs );

	AbaxDataString res;
	char buf[1024];
	sprintf( buf, "%d|%d|%d|%lld|%lld|%lld|%lld|%lld", nsrv, dbs, tabs, 
			(abaxint)numSelects, (abaxint)numInserts, 
			(abaxint)numUpdates, (abaxint)numDeletes, 
			_connections );

	res = buf;
	// printf("s4910 sendOpInfo [%s]\n", res.c_str() );
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// do local data backup
// pmesg: "_serv_copydata|15MIN:OVERWRITE|0"
void JagDBServer::doCopyData( const char *mesg, const JagRequest &req )
{
	JagStrSplit sp( mesg, '|', true );
	if ( sp.length() < 3 ) {
		raydebug( stdout, JAG_LOG_LOW, "s8035 doCopyData error [%s]\n", mesg );
		return;
	}
	AbaxDataString rec = sp[1];
	int show = atoi(sp[2].c_str());
	copyData( rec, show );
}

// do local backup method
// pmesg: "_serv_dolocalbackup"
void JagDBServer::doLocalBackup( const char *mesg, const JagRequest &req )
{
	raydebug( stdout, JAG_LOG_LOW, "dolocalbackup ...\n" );
	JagStrSplit sp( mesg, '|', true );
	copyLocalData("last", "OVERWRITE", sp[1], true );
	raydebug( stdout, JAG_LOG_LOW, "dolocalbackup done\n" );
}

// do remote backup method
// pmesg: "_serv_doremotebackup"
void JagDBServer::doRemoteBackup( const char *mesg, const JagRequest &req )
{
	if ( _doingRemoteBackup ) {
		// do not comment out
		prt(("s1192 _doingRemoteBackup active, doRemoteBackup skip\n"));
		return;
	}

	JagStrSplit sp( mesg, '|', true ); // sp[1] is rserv, sp[2] is passwd
	if ( sp[1].length() < 1 || sp[2].length() < 1 ) {
		// do not comment out
		prt(("s1193 rserv or passwd empty, skip doRemoteBackup\n" ));
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "doremotebackup ...\n" );	
	_doingRemoteBackup = 1;

	AbaxDataString passwdfile = _cfg->getConfHOME() + "/tmpsyncpass.txt";
	JagFileMgr::writeTextFile( passwdfile, sp[2] );
	AbaxDataString cmd;
	chmod( passwdfile.c_str(), 0600 );

	AbaxDataString intip = _localInternalIP;
	AbaxDataString jagdatahome = _cfg->getJDBDataHOME( 0 ); // /home/jaguar/data
	AbaxDataString backupdir = jaguarHome() + "/tmp/remotebackup";
	JagFileMgr::rmdir( backupdir );
	JagFileMgr::makedirPath( backupdir );
	char buf[2048];
	sprintf( buf, "rsync -r %s/ %s", jagdatahome.c_str(), backupdir.c_str() );
	system( buf );  // first local copy /home/jaguar/data/* to /home/jaguar/tmp/remotebackup

	cmd = "rsync -q --contimeout=10 --password-file=" + passwdfile + " -az " + backupdir + "/ " + sp[1] + "::jaguardata/" + intip;
	// then copy from /home/jaguar/data/remotebackup/* to remotehost::jaguardata/192.183.2.120
	// do not comment out
	prt(("s1829 [%s]\n", cmd.c_str() ));
	AbaxDataString res = psystem( cmd.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "doRemoteBackup done %s\n", res.c_str() );
	_doingRemoteBackup = 0;
	// jagunlink( passwdfile.c_str() );
	JagFileMgr::rmdir( backupdir, false );
	raydebug( stdout, JAG_LOG_LOW, "doremotebackup done\n" );
}

// do restore remote backup method
// pmesg: "_serv_dorestoreremote"
void JagDBServer::doRestoreRemote( const char *mesg, const JagRequest &req )
{
	if ( _doingRestoreRemote || _doingRemoteBackup ) {
		// do not comment out
		prt(("s1192 _doingRestoreRemote or _doingRemoteBackup active, doRestoreRemote skip\n"));
		return;
	}

	JagStrSplit sp( mesg, '|', true );	// sp[1] is rserv, sp[2] is passwd
	// if ( rserv.length() < 1 || passwd.length() < 1 ) {
	if ( sp[1].length() < 1 ) {
		// do not comment out
		prt(("s1193 rserv empty, skip doRestoreRemote\n" ));
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "dorestoreremote ...\n" );
	_doingRestoreRemote = 1;

	char buf[2048];
    AbaxDataString cmd = jaguarHome() + "/bin/restorefromremote.sh";
	AbaxDataString logf = jaguarHome() + "/log/restorefromremote.log";
	sprintf( buf, "%s %s %s > %s 2>&1", cmd.c_str(), sp[1].c_str(), sp[2].c_str(), logf.c_str() );

	AbaxDataString res = psystem( buf ); 
	raydebug( stdout, JAG_LOG_LOW, "doRestoreRemote %s\n", res.c_str() );
	_doingRestoreRemote = 0;
	raydebug( stdout, JAG_LOG_LOW, "exit. Please restart jaguar after restore completes\n", res.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "dorestoreremote done\n" );
	exit(0);
}

// refresh local server whitelist and blacklist
// pmesg: "_serv_refreshacl|whitelistIPS|blacklistIPS"
void JagDBServer::doRefreshACL( const char *mesg, const JagRequest &req )
{
	JagStrSplit sp( mesg, '|' );
	if ( sp.length() < 3 ) {
		raydebug( stdout, JAG_LOG_LOW, "s8315 doRefreshACL error [%s]\n", mesg );
		return;
	}

	AbaxDataString whitelist = sp[1];  // ip1\nip2\nip3\n
	AbaxDataString blacklist = sp[2];  // ip4\nip5\ip6\n
	// prt(("s1209 doRefreshACL whitelist=[%s] blacklist=[%s]\n", whitelist.c_str(), blacklist.c_str() ));
	pthread_rwlock_wrlock( &_aclrwlock);
	_whiteIPList->refresh( whitelist );
	_blackIPList->refresh( blacklist );
	pthread_rwlock_unlock( &_aclrwlock );

}

// dbtabInfo 
// pmesg: "_mon_dbtab" 
void JagDBServer::dbtabInfo( const char *mesg, const JagRequest &req )
{
	// "db1:t1:t2|db2:t1:t2|db3:t1:t3"
	JagTableSchema *tableschema = getTableSchema( req.session->replicateType );
	AbaxDataString res;
	AbaxDataString dbs = JagSchema::getDatabases( _cfg, req.session->replicateType );
	JagStrSplit sp(dbs, '\n', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( sp[i], "" );
		res += sp[i];
		for ( int j =0; j < vec->size(); ++j ) {
			res += AbaxDataString(":") + (*vec)[j].c_str();
		}
		if ( vec ) delete vec;
		vec = NULL;
		res += AbaxDataString("|");
	}
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// send info 
// pmesg: "_mon_info"
void JagDBServer::sendInfo( const char *mesg, const JagRequest &req )
{
	AbaxDataString res;
	JagVector<AbaxDataString> vec;
	JagBoundFile bf( _perfFile.c_str(), 96 );
	bf.openRead();
	bf.readLines( 96, vec );
	bf.close();
	int len = vec.length();
	for ( int i = 0; i < len; ++i ) {
		res += vec[i]  + "\n";
	}

	if ( res.length() < 1 ) {
		res = "0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0";
	}
	// prt(("s3228 res=[%s] len=%d done\n", res.c_str(), len ));
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "disk:123:322|mem:234:123|cpu:23:12";  in GB  used:free
// pmesg: "_mon_rsinfo"
void JagDBServer::sendResourceInfo( const char *mesg, const JagRequest &req )
{
	int rc = 0;
	AbaxDataString res;
	abaxint usedDisk, freeDisk;
	abaxint usercpu, syscpu, idle;
	_jagSystem.getCPUStat( usercpu, syscpu, idle );
	abaxint totm, freem, used; //GB
	rc = _jagSystem.getMemInfo( totm, freem, used );
	// prt(("s0394 _jagSystem.getMemInfo rc=%d totm=%d freem=%d used=%d\n", rc, totm, freem, used ));

	AbaxDataString jaghome= jaguarHome();
	JagFileMgr::getPathUsage( jaghome.c_str(), usedDisk, freeDisk );

	char buf[1024];
	sprintf( buf, "disk:%lld:%lld|mem:%lld:%lld|cpu:%lld:%lld", 
			 usedDisk, freeDisk, totm-freem, freem, usercpu+syscpu, 100-usercpu-syscpu );
	res = buf;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
	// prt(("s3349 res=[%s]\n", res.c_str() ));
}

// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
// pmesg: "_mon_clusteropinfo"
void JagDBServer::sendClusterOpInfo( const char *mesg, const JagRequest &req )
{
	AbaxDataString res = getClusterOpInfo( req, this );
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "disk:123:322|mem:234:123|cpu:23:12";  in GB  used:free
// pmesg: "_mon_hosts"
void JagDBServer::sendHostsInfo( const char *mesg, const JagRequest &req )
{
	AbaxDataString res;
	res = _dbConnector->_nodeMgr->_allNodes;
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "ip1|ip2|ip3";  in GB  used:free
// pmesg: "_mon_remote_backuphosts"
void JagDBServer::sendRemoteHostsInfo( const char *mesg, const JagRequest &req )
{
	AbaxDataString res = _cfg->getValue("REMOTE_BACKUP_SERVER", "0" );
	JagTable::sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp
// pmesg: "_mon_local_stat6"
void JagDBServer::sendLocalStat6( const char *mesg, const JagRequest &req )
{
	abaxint totalDiskGB, usedDiskGB, freeDiskGB, nproc, tcp;
	float loadvg;
	_jagSystem.getStat6( totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	char line[256];
	sprintf(line, "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	JagTable::sendMessageLength( req, line, strlen(line), "OK" );
}

// client expects: "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp
// loadvg is avg, others are accumulative from all nodes
// pmesg: "_mon_cluster_stat6"
void JagDBServer::sendClusterStat6( const char *mesg, const JagRequest &req )
{
	abaxint totalDiskGB=0, usedDiskGB=0, freeDiskGB=0, nproc=0, tcp=0;
	float loadvg;

	_jagSystem.getStat6( totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	char line[256];
	sprintf(line, "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	AbaxDataString self = line;

	AbaxDataString resp, bcasthosts;
	resp = _dbConnector->broadCastGet( "_mon_local_stat6", bcasthosts ); 
	// \n separated data from all nodes

	resp += self + "\n";

	JagStrSplit sp( resp, '\n', true );

	totalDiskGB = usedDiskGB = freeDiskGB = nproc = tcp = 0;
	loadvg = 0.0;
	int splen = sp.length();
	for ( int i = 0 ; i < splen; ++i ) {
		JagStrSplit ds( sp[i], '|' );
		totalDiskGB += jagatoll( ds[0].c_str() ); 
		usedDiskGB += jagatoll( ds[1].c_str() ); 
		freeDiskGB += jagatoll( ds[2].c_str() ); 
		nproc += jagatoll( ds[3].c_str() ); 
		loadvg += atof( ds[4].c_str() ); 
		tcp += jagatoll( ds[5].c_str() ); 
	}

	loadvg = loadvg/(float)splen;

	sprintf(line, "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	JagTable::sendMessageLength( req, line, strlen(line), "OK" );
}

// 0: not done; 1: done
// pmesg: "_ex_proclocalbackup"
void JagDBServer::processLocalBackup( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "localbackup rejected. admin exclusive login is required\n" );
		JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		prt(("s2930 processLocalBackup not ishost0Cluster0 skip\n"));
		raydebug( stdout, JAG_LOG_LOW, "localbackup not processed\n" );
		JagTable::sendMessage( req, "localbackup is not setup and not processed", "ER" );
		JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "localbackup started\n" );
	JagTable::sendMessage( req, "localbackup started...", "OK" );
	
	// copy local data
	AbaxDataString tmstr = JagTime::YYYYMMDDHHMM();
	copyLocalData("last", "OVERWRITE", tmstr, true );

	// bcast to other servers
	AbaxDataString bcastCmd, bcasthosts;
	bcastCmd = AbaxDataString("_serv_dolocalbackup|") + tmstr;
	raydebug( stdout, JAG_LOG_LOW, "broadcast localbackup to all servers ...\n" ); 
	_dbConnector->broadCastSignal( bcastCmd, bcasthosts );
	raydebug( stdout, JAG_LOG_LOW, "localbackup finished\n" );
	JagTable::sendMessage( req, "localbackup finished", "OK" );
	JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
}

// 0: not done; 1: done
// pmesg: "_ex_procremotebackup"
void JagDBServer::processRemoteBackup( const char *mesg, const JagRequest &req )
{
	// if _END_ leads, then it is final message to client.
	// "_END_[]", "ED"  means end of message, no error
	// "_END_[]", "ER"  means end of message, with error
	// "msg", "OK"  just an message
	
	if ( req.session && ( req.session->uid!="admin" || !req.session->exclusiveLogin ) ) {
		raydebug( stdout, JAG_LOG_LOW, "remotebackup rejected. admin exclusive login is required\n" );
		if ( req.session ) JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	// if i am host0, do remotebackup
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		prt(("s2930 processRemoteBackup not _isHost0OfCluster0 skip\n"));
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) JagTable::sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	if ( _restartRecover ) {
		prt(("s2931 in recovery, skip processRemoteBackup\n"));
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) JagTable::sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	AbaxDataString rs = _cfg->getValue("REMOTE_BACKUP_SERVER", "" );
	AbaxDataString passwdfile = _cfg->getConfHOME() + "/syncpass.txt";
	chmod( passwdfile.c_str(), 0600 );
	AbaxDataString passwd;
	JagFileMgr::readTextFile( passwdfile, passwd );
	passwd = trimChar( passwd, '\n');
	if ( rs.size() < 1 || passwd.size() < 1 ) {
		// do not comment out
		raydebug( stdout, JAG_LOG_LOW, "REMOTE_BACKUP_SERVER/syncpass.txt empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) JagTable::sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "remotebackup started\n" );
	if ( req.session ) JagTable::sendMessage( req, "remotebackup started...", "OK" );
	
	JagStrSplit sp (rs, '|');
	AbaxDataString remthost;
	for ( int i = 0; i < sp.length(); ++i ) {
		// multiple remote backup servers
		remthost = sp[i];
    	// self thread starts
       	pthread_t  threadmo;
    	//JagPass *jp = new JagPass();
    	JagPass *jp = newObject<JagPass>();
    	jp->servobj = this;
    	jp->ip = remthost;
    	jp->passwd = passwd;
       	jagpthread_create( &threadmo, NULL, threadRemoteBackup, (void*)jp );
       	pthread_detach( threadmo );
    
    	// bcast to other servers
    	AbaxDataString bcastCmd, bcasthosts;
    	bcastCmd = AbaxDataString("_serv_doremotebackup|") + remthost +"|" + passwd;
    	raydebug( stdout, JAG_LOG_LOW, "broadcast remotebackup to all servers ...\n" ); 
    	_dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 

		jagsleep(10, JAG_SEC);
	}
	raydebug( stdout, JAG_LOG_LOW, "remotebackup finished\n" );
	if ( req.session ) JagTable::sendMessage( req, "remotebackup finished", "OK" );
	if ( req.session ) JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
}

// 0: not done; 1: done
// pmesg: "_ex_restorefromremote"
void JagDBServer::processRestoreRemote( const char *mesg, const JagRequest &req )
{
	// if _END_ leads, then it is final message to client.
	// "_END_[]", "ED"  means end of message, no error
	// "_END_[]", "ER"  means end of message, with error
	// "msg", "OK"  just an message
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote rejected. admin exclusive login is required\n" );
		JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	// if i am host0, do remotebackup
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		prt(("s2930 processRestoreRemote not _isHost0OfCluster0 skip\n"));
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		JagTable::sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	if ( _restartRecover ) {
		prt(("s2931 in recovery, skip processRestoreRemote\n"));
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		JagTable::sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	JagStrSplit sp( mesg, '|' );
	if ( sp.length() < 3 ) {
		raydebug( stdout, JAG_LOG_LOW, "processRestoreRemote pmsg empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		JagTable::sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	AbaxDataString remthost  = sp[1];
	AbaxDataString passwd  = sp[2];
	if ( remthost.size() < 1 ) {
		raydebug( stdout, JAG_LOG_LOW, "processRestoreRemote IP empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		JagTable::sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	prt(("s6370 _restorefromremote mesg=[%s]\n", mesg ));
	raydebug( stdout, JAG_LOG_LOW, "restorefromremote started\n" );
	JagTable::sendMessage( req, "restorefromremote started... please restart jaguar after completion", "OK" );

    // bcast to other servers
    AbaxDataString bcastCmd, bcasthosts;
    bcastCmd = AbaxDataString("_serv_dorestoreremote|") + remthost +"|" + passwd;
    raydebug( stdout, JAG_LOG_LOW, "broadcast restoreremote to all servers ...\n" ); 
    _dbConnector->broadCastSignal( bcastCmd, bcasthosts ); 

	jagsleep(3, JAG_SEC);

	AbaxDataString rmsg = AbaxDataString("_serv_dorestoreremote|") + remthost + "|" + passwd;
	doRestoreRemote( rmsg.c_str(), req );
	// doRestoreRemote( remthost, passwd );
	raydebug( stdout, JAG_LOG_LOW, "restorefromremote finished\n" );
	JagTable::sendMessage( req, "restorefromremote finished", "OK" );
	JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
}

// method to check and repair given object data
// pmesg: "_ex_repairocheck" or "_ex_repairobject"
void JagDBServer::repairCheck( const char *mesg, const JagRequest &req )
{
	int isCheck = 0;
	if ( 0 == strncmp( mesg, "_ex_repairocheck", 16 ) ) isCheck = 1;
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "check/repair object rejected. admin exclusive login is required\n" );
		JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	AbaxDataString dbobj = mesg+17, gettab;
	abaxint rc = 0, elem = 1;
	AbaxDataString msg;
	JagParseParam pparam;
	ObjectNameAttribute objNameTemp;
	JagStrSplit sp( dbobj.c_str(), '.' );
	objNameTemp.init();
	objNameTemp.dbName = sp[0];
	objNameTemp.tableName = sp[1];
	pparam.objectVec.append(objNameTemp);
	pparam.optype = 'R';
	pparam.opcode = JAG_REPAIRCHECK_OP;
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;

	ptab = _objectLock->readLockTable( pparam.opcode, pparam.objectVec[0].dbName, 
		pparam.objectVec[0].tableName, req.session->replicateType, 0 );
	if ( !ptab ) {
		pindex = _objectLock->readLockIndex( pparam.opcode, pparam.objectVec[0].dbName,
			gettab, pparam.objectVec[0].tableName,
			tableschema, indexschema, req.session->replicateType, 0 );
	}
	
	rc = 0;
	abaxint keyCheckerCnt = 0;
	if ( ptab ) {
		flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
		if ( isCheck ) {
			rc = ptab->_darrFamily->checkFileOrder( req );
		} else {
			rc = ptab->_darrFamily->orderRepair( req );
			elem = ptab->_darrFamily->getElements( keyCheckerCnt );
		}
		_objectLock->readUnlockTable( pparam.opcode, pparam.objectVec[0].dbName, 
			pparam.objectVec[0].tableName, req.session->replicateType, 0 );
	} else if ( pindex ) {
		flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 0, NULL, pindex );
		if ( isCheck ) {
			rc = pindex->_darrFamily->checkFileOrder( req );
		} else {
			rc = pindex->_darrFamily->orderRepair( req );
			elem = pindex->_darrFamily->getElements( keyCheckerCnt );
		}
		_objectLock->readUnlockIndex( pparam.opcode, pparam.objectVec[0].dbName,
			gettab, pparam.objectVec[0].tableName,
			tableschema, indexschema, req.session->replicateType, 0 );
	} else {
		msg = dbobj + " does not exist. Please check the name";
		JagTable::sendMessage( req, msg.c_str(), "ER" );
		return;
	}

	if ( isCheck ) {
		if ( rc > 0 ) {
			msg = dbobj + " is in right order. No need to repair";
		} else {
			msg = dbobj + " has misplaced records. Please use repair command to fix it";
		}
	} else {
		if ( rc > 0 ) {
			msg = dbobj + " has been corrected";
			prt(("s8180 %s repair wrong records=%lld elements=%lld\n", dbobj.c_str(), rc, elem ));
		} else {
			msg = dbobj + " all records are in right order";
		}
	}
	prt(("s7459 %s\n", msg.c_str()));
	JagTable::sendMessage( req, msg.c_str(), "OK" );
	JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );
}

// method to add new cluster
// pmesg: "_ex_addcluster|#ip1|ip2#ip3|ip4!ip5|ip6"
void JagDBServer::addCluster( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "add cluster rejected. admin exclusive login is required\n" );
		JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	JagHashMap<AbaxString, AbaxInt> ipmap;

	
	const char *end = strchr( mesg+16, '|' );
	if ( !end ) end = strchr( mesg+16, '!' );
	if ( !end ) end = mesg+16;
	AbaxDataString hstr = mesg+15, absfirst( mesg+16, end-mesg-16 );
	// split to get original cluster(s) and new added cluster
	JagStrSplit sp( hstr, '!', true );
	JagStrSplit sp2( sp[1], '|', true );
	_objectLock->writeLockSchema( -1 );
	flushAllTableAndRelatedIndexsInsertBuffer();
	
	// form new cluster.conf string as the form of: #\nip1\nip2\n#\nip3\nip4...
	int clusternum = 1;
	AbaxDataString nhstr, ip, err, clustname;
	JagStrSplit sp3( sp[0], '#', true );
	for ( int i = 0; i < sp3.length(); ++i ) {
		clustname = AbaxDataString("# Subcluster ") + intToStr(clusternum) + " (Do not delete this line)";
		nhstr += clustname + "\n";
		++ clusternum;
		JagStrSplit sp4( sp3[i], '|', true );
		for ( int j = 0; j < sp4.length(); ++j ) {
			ip = JagNet::getIPFromHostName( sp4[j] );
			// nhstr += sp4[j] + "\n";
			if ( ip.length() < 2 ) {
				err = AbaxDataString( "_END_[T=130|E=Command Failed. Unable to resolve IP address of " ) +  sp4[j] ;
				raydebug( stdout, JAG_LOG_LOW, "E1300 addcluster error %s \n", err.c_str() );
				JagTable::sendMessage( req, err.c_str(), "ER" );
				return;
			}

			if ( ! ipmap.keyExist( ip.c_str() ) ) {
				ipmap.addKeyValue( ip.c_str(), 1 );
				nhstr += ip + "\n";
			}
		}
	}

	// nhstr += "# Do not delete this line\n";
	clustname = AbaxDataString("# Subcluster ") + intToStr(clusternum) + " (New. Do not delete this line)";
	nhstr += clustname + "\n";
	for ( int i = 0; i < sp2.length(); ++i ) {
		// nhstr += sp2[i] + "\n";
		ip = JagNet::getIPFromHostName( sp2[i] );
		if ( ip.length() < 2 ) {
			err = AbaxDataString( "_END_[T=130|E=Command Failed. Unable to resolve IP address of newhost " ) +  sp2[i] ;
			raydebug( stdout, JAG_LOG_LOW, "E1301 addcluster error %s \n", err.c_str() );
			JagTable::sendMessage( req, err.c_str(), "ER" );
			return;
		}

		if ( ! ipmap.keyExist( ip.c_str() ) ) {
			ipmap.addKeyValue( ip.c_str(), 1 );
			nhstr += ip + "\n";
		}
	}
	raydebug( stdout, JAG_LOG_LOW, "addcluster newhosts:\n%s\n", nhstr.c_str() );

	AbaxDataString fpath, cmd, dirpath, tmppath, passwd = "dummy", unixSocket = AbaxDataString("/TOKEN=") + _servToken;
	unsigned int uport = _port;
	// first, let host0 of cluster0 send schema info to new server(s)
	if ( _dbConnector->_nodeMgr->_selfIP == absfirst ) {
		dirpath = _cfg->getJDBDataHOME( 0 );
		tmppath = _cfg->getTEMPDataHOME( 0 );
		// make schema package -- empty database dirs and full system dir
		// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/a.tar.gz --no-recursion *;
		// tar -zcf /home/$USER/$BRAND/tmp/$DATA/b.tar.gz system
		cmd = AbaxDataString("cd ") + dirpath + "; tar -zcf " + tmppath + "/a.tar.gz --no-recursion *; tar -zcf ";
		cmd += tmppath + "/b.tar.gz system";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6300 [%s]\n", cmd.c_str() );
		
		// untar the above two tar.gzs, remove them and remake a new tar.gz
		// cd /home/$USER/$BRAND/tmp/$DATA; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm a.tar.gz; b.tar.gz; tar -zcf a.tar.gz *
		cmd = AbaxDataString("cd ") + tmppath + "; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm -f a.tar.gz b.tar.gz; tar -zcf c.tar.gz *";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6302 [%s]\n", cmd.c_str() );
		fpath = tmppath + "/c.tar.gz";
		// make connection and transfer package to each server
		for ( int i = 0; i < sp2.length(); ++i ) {
			fileTransmit( sp2[i], uport, passwd, unixSocket, 0, fpath, 1 );
		}
		//unlink tar.gz file
		jagunlink(fpath.c_str());
		// clean up tmp dir
		JagFileMgr::rmdir( tmppath, false );
	}
	
	// for new added servers, wait for package receive, and then format schema
	for ( int i = 0; i < sp2.length(); ++i ) {
		if ( _dbConnector->_nodeMgr->_selfIP == sp2[i] ) {
			// is new server, waiting for package to receive
			while ( _addClusterFlag < 1 ) {
				jagsleep(1, JAG_SEC);
			}
			// received new schema package
			// 1. cp tar.gz to pdata and ndata
			// 1. drop old tables, untar packages, cp -rf of data to pdata and ndata and rebuild new table objects
			// 3. init clean other dirs ( /tmp etc. )
			// 1.
			JagRequest req;
			JagSession session( g_dtimeout );
			req.session = &session;
			session.servobj = this;

			// for data dir
			dirpath = _cfg->getJDBDataHOME( 0 );
			session.replicateType = 0;
			dropAllTablesAndIndex( req, this, _tableschema );
			JagFileMgr::rmdir( dirpath, false );
			fpath = _cfg->getTEMPDataHOME( 0 ) + "/" + _crecoverFpath;
			cmd = AbaxDataString("tar -zxf ") + fpath + " --directory=" + dirpath;
			system( cmd.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "s6304 [%s]\n", cmd.c_str() );
			jagunlink(fpath.c_str());
			raydebug( stdout, JAG_LOG_LOW, "delete %s\n", fpath.c_str() );
			
			// for pdata dir
			if ( _faultToleranceCopy >= 2 ) {
				dirpath = _cfg->getJDBDataHOME( 1 );
				session.replicateType = 1;
				dropAllTablesAndIndex( req, this, _prevtableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = AbaxDataString("cp -rf ") + _cfg->getJDBDataHOME( 0 ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6307 [%s]\n", cmd.c_str() );
			}

			// for ndata dir
			if ( _faultToleranceCopy >= 3 ) {
				dirpath = _cfg->getJDBDataHOME( 2 );
				session.replicateType = 2;
				dropAllTablesAndIndex( req, this, _nexttableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = AbaxDataString("cp -rf ") + _cfg->getJDBDataHOME( 0 ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6308 [%s]\n", cmd.c_str() );
			}
			_objectLock->writeUnlockSchema( -1 );
			crecoverRefreshSchema( 1 );
			_objectLock->writeLockSchema( -1 );
			_crecoverFpath = "";

			// 2.
			fpath = jaguarHome() + "/tmp";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/tmp/data";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/tmp/pdata";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/tmp/ndata";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/tmp/join";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/export";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			fpath = jaguarHome() + "/backup";
			JagFileMgr::rmdir( fpath );
			JagFileMgr::makedirPath( fpath );
			JagFileMgr::makedirPath( fpath+"/15min" );
			JagFileMgr::makedirPath( fpath+"/hourly" );
			JagFileMgr::makedirPath( fpath+"/daily" );
			JagFileMgr::makedirPath( fpath+"/weekly" );
			JagFileMgr::makedirPath( fpath+"/monthly" );

			_addClusterFlag = 0;
			break;
		}
	}

	// then, for all servers, refresh cluster.conf and remake connections
	_dbConnector->_nodeMgr->refreshFile( nhstr );
	crecoverRefreshSchema( 2 );
	_localInternalIP = _dbConnector->_nodeMgr->_selfIP;
	JagTable::sendMessage( req, "_END_[T=30|E=]", "ED" );

	struct timeval now;
	gettimeofday( &now, NULL );
	g_lastHostTime = now.tv_sec * (abaxint)1000000 + now.tv_usec;
	_objectLock->writeUnlockSchema( -1 );

	// remove $HOME/.jagsetupssh
	AbaxDataString setup = AbaxDataString( getenv("HOME") ) + "/.jagsetupssh";
	jagunlink( setup.c_str() );
}

// method to request schema info from old datacenter to current one
void JagDBServer::requestSchemaFromDataCenter()
{
	AbaxDataString fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) || !_isGate ) {
		// no datacenter file exists, or requested server is not GATE, return
		return;
	}
	
	int rc = 0, succ = 0, MAXTRY = 12, trynum;
	AbaxDataString adminpass = "dummy";
	JagVector<AbaxDataString> vec;
	JagVector<AbaxDataString> vecip;
	JagVector<AbaxDataString> vecdc; // vector of GATE hosts
	JagVector<AbaxDataString> dcport; // vector of GATE hosts' port
	JagVector<AbaxDataString> vecinnerdc; // vector of HOST hosts
	JagVector<AbaxDataString> innerdcport; // vector of HOST hosts' port
	AbaxDataString ip;
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );
		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.length() > 0 ) {
			vecip.append( ip );
		}
	}
		
	AbaxDataString seeds, host, port, dcmd = "_serv_reqschemafromdc|", dcmd2 = "_serv_askdatafromdc|";
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	AbaxDataString bcasthosts, destType, uphost;
	
	for ( int i = 0; i < sp.length(); ++i ) {
		if ( sp[i].size()< 2 ) continue;
		if ( strchr( sp[i].c_str(), '#' ) ) continue;
		uphost = makeUpperString( sp[i] );
		getDestHostPortType( uphost, host, port, destType );
		if ( vec.exist( host ) ) { 
			succ = 1; 
			dcmd += host + "|" + port.c_str();
			dcmd2 += host + "|" + port.c_str();
			continue; 
		}
		if ( vec.length() > 0 && ( vec.length() == vecip.length() ) ) {
			ip = JagNet::getIPFromHostName( host );
			if ( ip.size() > 1 && vecip.exist( ip ) ) { 
				succ = 1; 
				dcmd += host + "|" + port.c_str();
				dcmd2 += host + "|" + port.c_str();
				continue; 
			}
		}

		if ( destType == "GATE" ) { 
			vecdc.append( host );
			dcport.append( port );
		} else { 
			vecinnerdc.append( host );
			innerdcport.append( port );
		}
	}
	
	if ( !succ ) {
		return;
	}
	succ = 0;
	destType = "GATE";
	AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + srcDestType( _isGate, destType ) +
	      						AbaxDataString("/TOKEN=") + _servToken;

	// make connection to first not self datacenter, and request schema
	//JaguarCPPClient *pcli = new JaguarCPPClient();
	JaguarCPPClient *pcli = newObject<JaguarCPPClient>();
	for ( int i = 0; i < vecdc.length(); ++i ) {
		pcli->_datcSrcType = JAG_DATACENTER_GATE;
		pcli->_datcDestType = JAG_DATACENTER_GATE;
		rc = 0; trynum = 0;
		while ( ! rc ) {
			if ( 0 == rc && trynum > MAXTRY ) { break; }
			rc = pcli->connect( vecdc[i].c_str(), atoi( dcport[i].c_str() ), 
								"admin", adminpass.c_str(), "test", unixSocket.c_str(), JAG_CONNECT_ONE );
			if ( 0 == rc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
					vecdc[i].c_str(), pcli->error(), trynum, MAXTRY );
				sleep(10);
			}
			++trynum;
		}

		if ( 0 == rc && trynum > MAXTRY ) {
			raydebug( stdout, JAG_LOG_LOW, "Failed connect to datacenter=%s retry next datacenter\n", vecdc[i].c_str() );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "Connected to datacenter=%s, begin to request schema ...\n", vecdc[i].c_str() );
			succ = 1;
			break;
		}
	}
	
	if ( !succ ) {
		// no valid connection with one of old datacenters, return
		delete pcli;
		return;
	}
	
	if ( pcli->queryDirect( dcmd.c_str(), true, false, true ) == 0 ) {
		delete pcli;
        return;
	}
	
	// wait for schema trasmit package, regard as add cluster
	_objectLock->writeLockSchema( -1 );
	while ( _addClusterFlag < 1 ) {
		jagsleep(1, JAG_SEC);
	}
	destType = "HOST";
	unixSocket = AbaxDataString("/DATACENTER=1") + srcDestType( _isGate, destType ) +
	 				AbaxDataString("/TOKEN=") + _servToken;
	// then, send package to all other servers -- inner datacenter
	AbaxDataString bcmd = "_serv_unpackschinfo"; JagRequest req;
	fpath = _cfg->getTEMPDataHOME( 0 ) + "/" + _crecoverFpath;
	for ( int i = 0; i < vecinnerdc.length(); ++i ) {
		JaguarCPPClient pcli2;
		pcli2._datcSrcType = JAG_DATACENTER_GATE;
		pcli2._datcDestType = JAG_DATACENTER_GATE;
		rc = 0; trynum = 0;
		while ( ! rc ) {
			if ( 0 == rc && trynum > MAXTRY ) { break; }
			rc = pcli2.connect( vecinnerdc[i].c_str(), atoi( innerdcport[i].c_str() ), "admin", "dummy", "test", unixSocket.c_str(), 0 );
			if ( 0 == rc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
					vecinnerdc[i].c_str(), pcli2.error(), trynum, MAXTRY );
				sleep(10);
			}
			++trynum;
		}

		if ( 0 == rc && trynum > MAXTRY ) {
			raydebug( stdout, JAG_LOG_LOW, "failed connect to datacenter=%s retry with next datacenter\n", vecinnerdc[i].c_str() );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "connected to datacenter=%s, beginning to request schema ...\n", vecinnerdc[i].c_str() );
			// when connect success, get the host lists and send schema info to all inner datacenter hosts
			JagStrSplit sp2( pcli2._primaryHostString, '#', true );			
			for ( int i = 0; i < sp2.length(); ++i ) {
				JagVector<AbaxDataString> chosts;
				JagStrSplit sp3( sp2[i], '|', true );
				for ( int j = 0; j < sp3.length(); ++j ) {
					fileTransmit( sp3[j], atoi( innerdcport[i].c_str() ), "dummy", unixSocket, 0, fpath, 1 );
					// then, ask each server to unpack schema info
					JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pcli2._connMap, sp3[j].c_str() );
					if ( jcli->queryDirect( bcmd.c_str(), true, false, true ) != 0 ) {
						while ( jcli->reply() > 0 ) {}
					}
				}
			}
		}
	}
	_objectLock->writeUnlockSchema( -1 );
	unpackSchemaInfo( bcmd.c_str(), req );
	pcli->replyAll();

	// then, send cmd to let old datacenter prepare to export and import data by separate thread
	pthread_t  nthread;
	//JagCliCmdPass *jp = new JagCliCmdPass();
	JagCliCmdPass *jp = newObject<JagCliCmdPass>();
	jp->servobj = this;
	jp->cli = pcli;
	jp->cmd = dcmd2;
	raydebug( stdout, JAG_LOG_LOW, "Initializing thread for data copy to new datacenter\n");
	jagpthread_create( &nthread, NULL, copyDataToNewDCStatic, (void*)jp );
	pthread_detach( nthread );
}

void *JagDBServer::copyDataToNewDCStatic( void *ptr )
{
	JagCliCmdPass *pass = (JagCliCmdPass*)ptr;
	if ( pass->cli->queryDirect( pass->cmd.c_str(), true, false, true ) == 0 ) {
		delete pass->cli;
		return NULL;
	}
	prt(("before copyDataToNewDC\n"));
	pass->cli->replyAll();
	prt(("done copyDataToNewDC\n"));
	// pass->servobj->_newdcTrasmittingFin = 1;
	delete pass->cli;
	return NULL;
}

// method to send schema info to new datacenter's gate, as requested
// pmesg: "_serv_reqschemafromdc|newdc_host|newdc_port"
void JagDBServer::sendSchemaToDataCenter( const char *mesg, const JagRequest &req )
{
	AbaxDataString hstr = mesg+22;
	// split to get original cluster(s) and new added cluster
	JagStrSplit sp( hstr, '|', true );
	if ( sp.length() < 2 ) {
		return;
	}
	AbaxDataString host = sp[0];
	unsigned int port = atoi ( sp[1].c_str() );
	AbaxDataString adminpass = "dummy";
	AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + AbaxDataString("/TOKEN=") + _servToken;
	AbaxDataString fpath, cmd, dirpath, tmppath;
	_objectLock->writeLockSchema( -1 );
	flushAllTableAndRelatedIndexsInsertBuffer();	

	dirpath = _cfg->getJDBDataHOME( 0 );
	tmppath = _cfg->getTEMPDataHOME( 0 );
	// make schema package -- empty database dirs and full system dir
	// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/a.tar.gz --no-recursion *;
	// tar -zcf /home/$USER/$BRAND/tmp/$DATA/b.tar.gz system
	cmd = AbaxDataString("cd ") + dirpath + "; tar -zcf " + tmppath + "/a.tar.gz --no-recursion *; tar -zcf ";
	cmd += tmppath + "/b.tar.gz system";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6300 [%s]\n", cmd.c_str() );
	
	// untar the above two tar.gzs, remove them and remake a new tar.gz
	// cd /home/$USER/$BRAND/tmp/$DATA; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm a.tar.gz; b.tar.gz; tar -zcf a.tar.gz *
	cmd = AbaxDataString("cd ") + tmppath + "; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm -f a.tar.gz b.tar.gz; tar -zcf c.tar.gz *";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6302 [%s]\n", cmd.c_str() );
	fpath = tmppath + "/c.tar.gz";
	// make connection and transfer package to new server
	fileTransmit( host, port, adminpass, unixSocket, 0, fpath, 1 );
	//unlink tar.gz file
	jagunlink(fpath.c_str());
	// clean up tmp dir
	JagFileMgr::rmdir( tmppath, false );
	_objectLock->writeUnlockSchema( -1 );
}

// method to unpack schema info after recv tar packages
// pmesg: "_serv_unpackschinfo"
void JagDBServer::unpackSchemaInfo( const char *mesg, const JagRequest &req )
{
	if ( _addClusterFlag < 1 ) {
		return;
	}
	_objectLock->writeLockSchema( -1 );
	// received new schema package
	// 1. cp tar.gz to pdata and ndata
	// 1. drop old tables, untar packages, cp -rf of data to pdata and ndata and rebuild new table objects
	// 3. init clean other dirs ( /tmp etc. )
	// 1.
	JagRequest req2;
	JagSession session( g_dtimeout );
	req2.session = &session;
	session.servobj = this;

	// for data dir
	AbaxDataString dirpath = _cfg->getJDBDataHOME( 0 );
	session.replicateType = 0;
	dropAllTablesAndIndex( req2, this, _tableschema );
	JagFileMgr::rmdir( dirpath, false );
	AbaxDataString fpath = _cfg->getTEMPDataHOME( 0 ) + "/" + _crecoverFpath;
	AbaxDataString cmd = AbaxDataString("tar -zxf ") + fpath + " --directory=" + dirpath;
	system( cmd.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "s6304 [%s]\n", cmd.c_str() );
	jagunlink(fpath.c_str());
	raydebug( stdout, JAG_LOG_LOW, "delete %s\n", fpath.c_str() );
	
	// for pdata dir
	if ( _faultToleranceCopy >= 2 ) {
		dirpath = _cfg->getJDBDataHOME( 1 );
		session.replicateType = 1;
		dropAllTablesAndIndex( req2, this, _prevtableschema );
		JagFileMgr::rmdir( dirpath, false );
		cmd = AbaxDataString("cp -rf ") + _cfg->getJDBDataHOME( 0 ) + "/* " + dirpath;
		system( cmd.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "s6307 [%s]\n", cmd.c_str() );
	}

	// for ndata dir
	if ( _faultToleranceCopy >= 3 ) {
		dirpath = _cfg->getJDBDataHOME( 2 );
		session.replicateType = 2;
		dropAllTablesAndIndex( req2, this, _nexttableschema );
		JagFileMgr::rmdir( dirpath, false );
		cmd = AbaxDataString("cp -rf ") + _cfg->getJDBDataHOME( 0 ) + "/* " + dirpath;
		system( cmd.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "s6308 [%s]\n", cmd.c_str() );
	}
	_objectLock->writeUnlockSchema( -1 );
	crecoverRefreshSchema( 1 );
	_objectLock->writeLockSchema( -1 );
	_crecoverFpath = "";

	// 2.
	fpath = jaguarHome() + "/tmp";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/tmp/data";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/tmp/pdata";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/tmp/ndata";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/tmp/join";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/export";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	fpath = jaguarHome() + "/backup";
	JagFileMgr::rmdir( fpath );
	JagFileMgr::makedirPath( fpath );
	JagFileMgr::makedirPath( fpath+"/15min" );
	JagFileMgr::makedirPath( fpath+"/hourly" );
	JagFileMgr::makedirPath( fpath+"/daily" );
	JagFileMgr::makedirPath( fpath+"/weekly" );
	JagFileMgr::makedirPath( fpath+"/monthly" );
	crecoverRefreshSchema( 2 );

	_addClusterFlag = 0;
	_newdcTrasmittingFin = 1;
	_objectLock->writeUnlockSchema( -1 );
}

// method to ask data export and import from old datacenter to new datacenter
// called by GATE
// by separate thread
// pmesg: "_serv_askdatafromdc|newdc_host|newdc_port"
void JagDBServer::askDataFromDC( const char *mesg, const JagRequest &req )
{
	AbaxDataString bcmd = AbaxDataString("_serv_preparedatafromdc|") + _dbConnector->_nodeMgr->_selfIP.c_str() + "|" 
		+ intToStr(_port) + "|" + (mesg+20);
	
	AbaxDataString fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) || !_isGate ) {
		// no datacenter file exists, or is not GATE, return
		return;
	}
	
	int rc = 0, succ = 0, MAXTRY = 12, trynum;
	AbaxDataString adminpass = "dummy";
		
	AbaxDataString uphost, seeds, host, port, destType;
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	
	for ( int i = 0; i < sp.length(); ++i ) {
		if ( sp[i].size()< 2 ) continue;
		if ( strchr( sp[i].c_str(), '#' ) ) continue;
		uphost = makeUpperString( sp[i] );
		getDestHostPortType( uphost, host, port, destType );

		if ( destType == "HOST" ) { 
			// first inner datacenter, break
			break;
		} 
	}
	
	JaguarCPPClient pcli;
	AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + srcDestType( _isGate, destType ) + 
								AbaxDataString("/TOKEN=") + _servToken;	
	pcli._datcSrcType = JAG_DATACENTER_GATE;
	pcli._datcDestType = JAG_DATACENTER_HOST;
	rc = 0; trynum = 0;
	while ( ! rc ) {
		if ( 0 == rc && trynum > MAXTRY ) { break; }
		rc = pcli.connect( host.c_str(), atoi( port.c_str() ), "admin", adminpass.c_str(), "test", unixSocket.c_str(), 0 );
		if ( 0 == rc ) {
			raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
				host.c_str(), pcli.error(), trynum, MAXTRY );
			sleep(10);
		}
		++trynum;
	}

	if ( 0 == rc && trynum > MAXTRY ) {
		raydebug( stdout, JAG_LOG_LOW, "failed connect to datacenter=%s retry with next datacenter\n", host.c_str() );
	} else {
		raydebug( stdout, JAG_LOG_LOW, "connected to datacenter=%s, beginning to request schema ...\n", host.c_str() );
		JagVector<JaguarCPPClient*> jclilist;
		JagStrSplit sp2( pcli._primaryHostString, '#', true );			
		// send query to each server
		for ( int i = 0; i < sp2.length(); ++i ) {
			JagVector<AbaxDataString> chosts;
			JagStrSplit sp3( sp2[i], '|', true );
			for ( int j = 0; j < sp3.length(); ++j ) {
				JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pcli._connMap, sp3[j].c_str() );
				if ( jcli->queryDirect( bcmd.c_str(), true, false, true ) != 0 ) {
					jclilist.append( jcli );
					// while ( jcli->reply() > 0 ) {}
				}
			}
		}
		// recv reply from each server
		for ( int i = 0; i < jclilist.length(); ++i ) {
			while ( jclilist[i]->reply() > 0 ) {}
		}
	}
}

// method to ask data export and import from old datacenter to new datacenter
// called by host server
// pmesg: "_serv_preparedatafromdc|selfproxy_host|selfproxy_port|newdc_host|newdc_port"
void JagDBServer::prepareDataFromDC( const char *mesg, const JagRequest &req )
{
	// make connection to new datacenter
	JagStrSplit sp( mesg+24, '|', true );
	if ( sp.length() < 4 ) {
		prt(("prepareDataFromDC return 0\n"));
		return;
	}
	AbaxDataString host = sp[0], destType = "GATE", spstr = "insertsyncdconly|" + sp[2] + "|" + sp[3] + " ";
	unsigned int port = atoi ( sp[1].c_str() );
	AbaxDataString adminpass =  "dummy";
	AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + srcDestType( _isGate, destType ) +
					AbaxDataString("/TOKEN=") + _servToken;
	_objectLock->writeLockSchema( -1 );
	flushAllTableAndRelatedIndexsInsertBuffer();	
	_objectLock->writeUnlockSchema( -1 );

	int rc = 0, trynum = 0, MAXTRY = 12;
	JaguarCPPClient pcli;
	while ( ! rc ) {
		if ( 0 == rc && trynum > MAXTRY ) { break; }
		rc = pcli.connect( host.c_str(), port, "admin", adminpass.c_str(), "test", unixSocket.c_str(), 0 );
		if ( 0 == rc ) {
			raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
				host.c_str(), pcli.error(), trynum, MAXTRY );
			sleep(10);
		}
		++trynum;
	}

	if ( ! rc ) {
		prt(("prepareDataFromDC return 1\n"));
		return;
	}
	pcli._datcSrcType = JAG_DATACENTER_HOST;
	pcli._datcDestType = JAG_DATACENTER_GATE;
	// for all ptab, export one by one, and import one by one
	JagRequest req2; JagSession session;
	session.dbname = "test";
	session.timediff = servtimediff;
	session.servobj = this;
	req2.session = &session;
	AbaxDataString str = _objectLock->getAllTableNames( 0 ), cmd, reterr, dbtab, dirpath, objname, fpath;
	JagStrSplit sp2( str, '|', true );
	abaxint threadSchemaTime = 0, threadQueryTime = 0;
	for ( int i = 0; i < sp2.length(); ++i ) {
		cmd = AbaxDataString("select * from ") + sp2[i] + " export;";
		JagParseAttribute jpa( this, servtimediff, servtimediff, req2.session->dbname, _cfg );
		prt(("s4071 parseCommand cmd=[%s]\n", cmd.c_str() ));
		JagParser parser( this, NULL );
		JagParseParam pparam( &parser );
		if ( parser.parseCommand( jpa, cmd.c_str(), &pparam, reterr ) ) {
			// select table export;
			rc = JagDBServer::processCmd( req2, this, cmd.c_str(), pparam, reterr, threadQueryTime, threadSchemaTime );
			// then, import to another datacenter
			dirpath = jaguarHome() + "/export/" + sp2[i];
			objname = sp2[i] + ".sql";
			fpath = JagFileMgr::getFileFamily( dirpath, objname );
			rc = pcli.importLocalFileFamily( fpath, spstr.c_str() );
			JagFileMgr::rmdir( dirpath );
		}
	}
	pcli.close();
}

// method to shut down servers
// pmesg: "_ex_shutdown"
void JagDBServer::shutDown( const char *mesg, const JagRequest &req )
{
	if ( req.session && (req.session->uid!="admin" || !req.session->exclusiveLogin ) ) {
		raydebug( stdout, JAG_LOG_LOW, "shut down rejected. admin exclusive login is required\n" );
		JagTable::sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	if ( _shutDownInProgress ) {
		raydebug( stdout, JAG_LOG_LOW, "Shutdown is already in progress. return\n");
		return;
	}
	
	AbaxDataString stopPath = jaguarHome() + "/log/shutdown.cmd";
	JagFileMgr::writeTextFile(stopPath, "WIP");
	_shutDownInProgress = 1;
	
	_objectLock->writeLockSchema( -1 );
	flushAllTableAndRelatedIndexsInsertBuffer();

	while ( _taskMap->size() > 0 ) {
		jagsleep(1, JAG_SEC);
		raydebug( stdout, JAG_LOG_LOW, "Shutdown waits other tasks to finish ...\n");
	}

	JAG_BLURT jaguar_mutex_lock ( &g_logmutex ); JAG_OVER;
	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	if ( _fpCommand ) jagfsync( fileno( _fpCommand ) );
	
	if ( _delPrevOriCommand ) jagfsync( fileno( _delPrevOriCommand ) );
	if ( _delPrevRepCommand ) jagfsync( fileno( _delPrevRepCommand ) );
	if ( _delPrevOriRepCommand ) jagfsync( fileno( _delPrevOriRepCommand ) );
	if ( _delNextOriCommand ) jagfsync( fileno( _delNextOriCommand ) );
	if ( _delNextRepCommand ) jagfsync( fileno( _delNextRepCommand ) );
	if ( _delNextOriRepCommand ) jagfsync( fileno( _delNextOriRepCommand ) );
	if ( _recoveryRegCommand ) jagfsync( fileno( _recoveryRegCommand ) );
	if ( _recoverySpCommand ) jagfsync( fileno( _recoverySpCommand ) );

	/***
	// remove all WAL logs
	AbaxDataString cmdpath = JagFileMgr::getLocalLogDir("cmd");
	raydebug( stdout, JAG_LOG_LOW, "Remove %s ...\n", cmdpath.c_str() );
	JagFileMgr::rmdir( cmdpath, false );
	***/

	// resetLog();
	jaguar_mutex_unlock ( &g_logmutex );
	jaguar_mutex_unlock ( &g_dlogmutex );

	raydebug( stdout, JAG_LOG_LOW, "Shutdown: Flushing block index to disk ...\n");
	flushAllBlockIndexToDisk();
	raydebug( stdout, JAG_LOG_LOW, "Shutdown: Completed\n");

	// _shutDownInProgress = 0;
	JagFileMgr::writeTextFile(stopPath, "DONE");
	jagsync();
	_objectLock->writeUnlockSchema( -1 );
	exit(0);
}

// for testing use only, no need for production
void JagDBServer::objectCountTesting( JagParseParam &parseParam )
{
	const char *cmd = NULL;
	AbaxDataString errmsg, dbidx, tabName, idxName;
	abaxint cnt, keyCheckerCnt;
	JagTable *ptab; JagIndex *pindex;
	JagRequest req; JagSession session;
	req.session = &session; session.servobj = this;
	JagTableSchema *tableschema; JagIndexSchema *indexschema;
	
	for ( int i = 0; i < _faultToleranceCopy; ++i ) {
		ptab = NULL; pindex = NULL; cnt = -1;
		session.replicateType = i;
		getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
		if ( parseParam.objectVec[0].indexName.length() > 0 ) {
			// known index
			pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[0].dbName, tabName, parseParam.objectVec[0].indexName,
				tableschema, indexschema, req.session->replicateType, 0 );
				dbidx = parseParam.objectVec[0].dbName;
				idxName = parseParam.objectVec[0].indexName;
		} else {
			// table object or index object
			ptab = _objectLock->readLockTable( parseParam.opcode, parseParam.objectVec[0].dbName, parseParam.objectVec[0].tableName,
				req.session->replicateType, 0 );
			if ( !ptab ) {
				pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[0].dbName, tabName, parseParam.objectVec[0].tableName,
					tableschema, indexschema, req.session->replicateType, 0 );
					dbidx = parseParam.objectVec[0].dbName;
					idxName = parseParam.objectVec[0].tableName;
				if ( !pindex ) {
					pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[0].colName, tabName, parseParam.objectVec[0].tableName,
						tableschema, indexschema, req.session->replicateType, 0 );
					dbidx = parseParam.objectVec[0].colName;
					idxName = parseParam.objectVec[0].tableName;
				}
			}
		}

		if ( ptab ) {
			cnt = ptab->count( cmd, req, &parseParam, errmsg, keyCheckerCnt );
			_objectLock->readUnlockTable( parseParam.opcode, parseParam.objectVec[0].dbName, 
				parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
		} else if ( pindex ) {
			cnt = pindex->count( cmd, req, &parseParam, errmsg, keyCheckerCnt );
			_objectLock->readUnlockIndex( parseParam.opcode, dbidx, tabName, idxName,
				tableschema, indexschema, req.session->replicateType, 0 );
		}

		if ( cnt >= 0 ) {
			prt(("selectcount type=%d %s keychecker=%lld diskarray=%lld\n", 
				req.session->replicateType, parseParam.objectVec[0].tableName.c_str(), keyCheckerCnt, cnt));
			// prt(("cnt1=%lld cnt2=%lld cnt3=%lld cnt4=%lld cnt5=%lld cnt6=%lld cnt7=%lld cnt8=%lld cnt9=%lld\n",	
			//	g_icnt1, g_icnt2, g_icnt3, g_icnt4, g_icnt5, g_icnt6, g_icnt7, g_icnt8, g_icnt9));
		}
	}
}

// method to do inner/outer join for multiple tables/indexs
int JagDBServer::joinObjects( const JagRequest &req, JagDBServer *servobj, JagParseParam *parseParam, AbaxDataString &reterr )
{
	//prt(("s2051 joinObjects ...\n" ));

	// first, only accept two table inner join
	if ( parseParam->objectVec.length() != 2 || parseParam->opcode != JAG_INNERJOIN_OP ) {
		reterr = "E2202 Only accept two tables/indexs innerjoin";
		return 0;
	}

	JagTableSchema *tableschema; JagIndexSchema *indexschema;
	servobj->getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	
	// init variables
	struct timeval now;
	gettimeofday( &now, NULL );
	int num = 2;  // 2 obects join only
	int rc, tmode = 0, tnum = 0, tlength = 0, st, nst;
	int collen, siglen, cmode = 0, useHash = -1;
	int numCPUs = servobj->_numCPUs; 
	abaxint i, j, stime = now.tv_sec, totlen = 0, kcheckerCnt = 0, finalsendlen = 0, gbvsendlen = 0;
	int pos;
	abaxint offset = 0, objelem[num], jlen[num], sklen[num];
	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	AbaxDataString ttype = " ";
	char *newbuf = NULL;
	bool hasValCol = 0, needInit = 1, timeout = 0, isagg, hasagg = 0, dfSorted[num];
	AbaxFixString rstr, tstr, fkey, fval;
	AbaxDataString jname, jpath, jdapath, jdapath2, sigpath, sigpath2, sigpath3, newhdr, gbvhdr;
	AbaxDataString unixSocket = AbaxDataString("/TOKEN=") + servobj->_servToken;
	ExpressionElementNode *root = NULL;
	JagDataAggregate *jda = NULL;
	JagVector<abaxint> jsvec[num];
	JaguarCPPClient hcli;
	hcli._sleepTime = 1; // reduce regular client object monitor sleep time to 1*1000
	
	// get all ptabs/pindexs info
	JagTable *ptab[num]; JagIndex *pindex[num];
	int klen[num], vlen[num], kvlen[num], numKeys[num], numCols[num];
	const JagHashStrInt *maps[num]; 
	const JagSchemaAttribute *attrs[num]; 
	const JagSchemaRecord *rec[num];
	JagDiskArrayFamily *df[num]; JagMinMax minmax[num];
	AbaxDataString dbidx[num], tabName[num], idxName[num];
	AbaxDataString dbName, colName, tableName, oneFilter, rowFilter;

	// prt(("s4202 selectWhereClause=[%s]\n", parseParam->selectWhereClause.c_str() ));

	for ( i = 0; i < num; ++i ) {
		colName = parseParam->objectVec[i].colName;
		tableName = parseParam->objectVec[i].tableName;
		dbName = parseParam->objectVec[i].dbName;

		// first, get ptab/pindex for each object
		jlen[i] = 0;
		sklen[i] = 0;
		dfSorted[i] = 0;
		ptab[i] = NULL; pindex[i] = NULL;
		if ( parseParam->objectVec[i].indexName.length() > 0 ) {
			// known index
			pindex[i] = servobj->_objectLock->readLockIndex( parseParam->opcode, dbName, 
								tabName[i], parseParam->objectVec[i].indexName, tableschema, indexschema, 
								req.session->replicateType, 0 );
			dbidx[i] = dbName;
			idxName[i] = parseParam->objectVec[i].indexName;
		} else {
			// table object or index object
			ptab[i] = servobj->_objectLock->readLockTable( parseParam->opcode, dbName, tableName, 
														   req.session->replicateType, 0 );
			if ( !ptab[i] ) {
				pindex[i] = servobj->_objectLock->readLockIndex( parseParam->opcode, dbName, 
								tabName[i], tableName, tableschema, indexschema, req.session->replicateType, 0 );

				dbidx[i] = dbName;
				idxName[i] = tableName;
				if ( !pindex[i] ) {
					pindex[i] = servobj->_objectLock->readLockIndex( parseParam->opcode, colName, 
									tabName[i], tableName, tableschema, indexschema, req.session->replicateType, 0 );
					dbidx[i] = colName;
					idxName[i] = tableName;
				}
			}
		}
		
		if ( !ptab[i] && !pindex[i] ) {
			if ( colName.length() > 0 ) {
				reterr = AbaxDataString("E4023 Unable to select for ") + tableName + "." + colName;
			} else {
				reterr = AbaxDataString("E4024 Unable to select for ") + dbName + "." + tableName;
			}

			if ( parseParam->objectVec[i].indexName.size() > 0 ) {
				reterr += AbaxDataString(".") + parseParam->objectVec[i].indexName;
			}
			break;
		}
		
		// otherwise, set the variables
		if ( ptab[i] ) {
			rc = checkUserCommandPermission( servobj, &ptab[i]->_tableRecord, req, *parseParam, i, oneFilter, reterr );
			if ( ! rc ) {
				reterr = AbaxDataString("E4524 No permission for table");
				return 0;
			}
			// prt(("s2039 tab i=%d oneFilter=[%s]\n", i, oneFilter.c_str() ));
			if ( oneFilter.size() > 0 ) { rowFilter += oneFilter + "|"; }

			totlen += ptab[i]->KEYVALLEN;
			klen[i] = ptab[i]->KEYLEN;
			vlen[i] = ptab[i]->VALLEN;
			kvlen[i] = ptab[i]->KEYVALLEN;
			numKeys[i] = ptab[i]->_numKeys;
			numCols[i] = ptab[i]->_numCols;
			maps[i] = ptab[i]->_tablemap;
			attrs[i] = ptab[i]->_schAttr;
			minmax[i].setbuflen( klen[i] );
			df[i] = ptab[i]->_darrFamily;
			rec[i] = &ptab[i]->_tableRecord;
			objelem[i] = df[i]->getElements( kcheckerCnt );
		} else {
			rc = checkUserCommandPermission( servobj, &pindex[i]->_indexRecord, req, *parseParam, i, oneFilter, reterr );
			if ( ! rc ) {
				reterr = AbaxDataString("E4525 No permission for index");
				return 0;
			}

			// prt(("s2034 idx i=%d oneFilter=[%s]\n", i, oneFilter.c_str() ));
			if ( oneFilter.size() > 0 ) { rowFilter += oneFilter + "|"; }

			totlen += pindex[i]->KEYVALLEN;
			klen[i] = pindex[i]->KEYLEN;
			vlen[i] = pindex[i]->VALLEN;
			kvlen[i] = pindex[i]->KEYVALLEN;
			numKeys[i] = pindex[i]->_numKeys;
			numCols[i] = pindex[i]->_numCols;
			maps[i] = pindex[i]->_indexmap;
			attrs[i] = pindex[i]->_schAttr;
			minmax[i].setbuflen( klen[i] );
			df[i] = pindex[i]->_darrFamily;
			rec[i] = &pindex[i]->_indexRecord;
			objelem[i] = df[i]->getElements( kcheckerCnt );			
		}
	}

	// send all table elements info to client
	AbaxDataString elemInfo = longToStr( objelem[0] ) + "|" + longToStr( objelem[1] );

	JagTable::sendMessage( req, elemInfo.c_str(), "OK" );

 	char ehdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *enewbuf = NULL;

	abaxint clen = recvData( req.session->sock, ehdr, enewbuf );

	if ( clen > 0 ) {
		useHash = jagatoll( enewbuf );
	}
	if ( enewbuf ) jagfree ( enewbuf );

	// reparse where from rowFilter
	if ( reterr.length() < 1 && rowFilter.size() > 0  ) {
		//prt(("s2231 rowFilter=[%s]\n", rowFilter.c_str() ));
		parseParam->resetSelectWhere( rowFilter );
		//prt(("s4231 parseParam.selectWhereCluase=[%s]\n", parseParam->selectWhereClause.c_str() ));
		parseParam->setSelectWhere( );
	}

	// if has join_on part, check its validation
	// select * from t join t2 on t.a=t2.a  joinOnVec.size=1
	// selecct * from t1, t2 where t1.a=t2.a; joinOnVec.size=0
	if ( reterr.length() < 1 && parseParam->joinOnVec.size() > 0 ) {
		root = parseParam->joinOnVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, klen, numKeys, num, hasValCol, minmax, tstr, tmode, tnum );
		if ( 0 == rc ) {
			for ( i = 0; i < num; ++i ) {
				memset( minmax[i].minbuf, 0, klen[i]+1 );
				memset( minmax[i].maxbuf, 255, klen[i] );
				(minmax[i].maxbuf)[klen[i]] = '\0';
			}
		} else if (  rc < 0 ) {
			reterr = "E2091 Error join on part";
		}
	}
	
	// if has where, set range
	if ( reterr.length() < 1 && parseParam->hasWhere ) {
		root = parseParam->whereVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, klen, numKeys, num, hasValCol, minmax, tstr, tmode, tnum );
		if ( 0 == rc ) {
			for ( i = 0; i < num; ++i ) {
				memset( minmax[i].minbuf, 0, klen[i]+1 );
				memset( minmax[i].maxbuf, 255, klen[i] );
				(minmax[i].maxbuf)[klen[i]] = '\0';
			}
		} else if ( rc < 0 ) {
			reterr = "E4042 Error select where part";
		}
	}

	// if has column calculations and/or group by, reformat data
	if ( reterr.length() < 1 && ( parseParam->hasGroup || parseParam->hasColumn ) ) {
		// first, arrange hdr
		JagVector<SetHdrAttr> spa; SetHdrAttr onespa;
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) onespa.setattr( numKeys[i], false, ptab[i]->_dbtable, rec[i] );
			else onespa.setattr( numKeys[i], false, pindex[i]->_dbobj, rec[i] );

			spa.append( onespa );
		}

		rc = rearrangeHdr( num, maps, attrs, parseParam, spa, newhdr, gbvhdr, finalsendlen, gbvsendlen );
		if ( !rc ) {
			reterr = "E5004 Error header for join";
		} else if ( gbvsendlen <= 0 && parseParam->hasGroup ) {
			reterr = "E5005 Error group by for join";
		} else {
			// set param select tree and where tree, if needed
			if ( parseParam->hasColumn ) {
				for ( i = 0; i < parseParam->selColVec.size(); ++i ) {
					isagg = 0;
					root = parseParam->selColVec[i].tree->getRoot();
					rc = root->setFuncAttribute( maps, attrs, cmode, tmode, isagg, ttype, collen, siglen );
					if ( 0 == rc ) {
						reterr = "E7041 Error select " + parseParam->selColVec[i].origFuncStr;
						break;
					}

					parseParam->selColVec[i].offset = offset;
					parseParam->selColVec[i].length = collen;
					parseParam->selColVec[i].sig = siglen;
					parseParam->selColVec[i].type = ttype;
					parseParam->selColVec[i].isAggregate = isagg;
					if ( isagg ) hasagg = 1;
					offset += collen;
				}
			}
		}
	}	

	// error, clean up users
	if ( reterr.length() > 0 ) {
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) {
				servobj->_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
					parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
			} else if ( pindex[i] ) {
				servobj->_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], idxName[i],
					tableschema, indexschema, req.session->replicateType, 0 );				
			}
		}
		return 0;
	}
	
	// make /home/tmp/join/cliPID_cliIP dir for use
	jname = req.session->cliPID + "_" + req.session->ip;
	jpath = servobj->_cfg->getTEMPJoinHOME() + "/" + jname;
	jdapath = jpath + "/alldata";
	jdapath2 = jpath + "/finaldata";
	sigpath = jpath + "/fprep";
	JagFileMgr::makedirPath( jpath );
	JagFileMgr::makedirPath( sigpath );
	//jda = new JagDataAggregate();
	jda = newObject<JagDataAggregate>();
	jda->setwrite( jdapath, jdapath, 2 );
	jda->setMemoryLimit( jagatoll(servobj->_cfg->getValue("JOIN_MEMLINE", "100000").c_str())*totlen );	
	
	// for each table, get join len based on columns
	// AbaxPair is column number and table number for each memory array
	/******
	JagArray<AbaxPair<AbaxLong, abaxint>> *sarr[num];
	const AbaxPair<AbaxString, abaxint> *jmarr = parseParam->joincolmap.array();
	for ( i = 0; i < num; ++i ) { sarr[i] = NULL; }
	for ( i = 0; i < parseParam->joincolmap.arrayLength(); ++i ) {
		if ( jmarr[i].key.size() > 0 ) {
			// put column name into sort arr
			if ( maps[jmarr[i].value]->getValue( jmarr[i].key, pos ) ) {
				AbaxPair<AbaxLong, abaxint> pair( AbaxLong( pos ), jmarr[i].value );
				if ( !sarr[jmarr[i].value] ) {
					sarr[jmarr[i].value] = new JagArray<AbaxPair<AbaxLong, abaxint>>();
				}
				sarr[jmarr[i].value]->insert( pair );
			}
		}
	}
	*****/
	JagArray<AbaxPair<AbaxLong, abaxint>> *sarr[num];
	for ( i = 0; i < num; ++i ) { sarr[i] = NULL; }

	JagVector<AbaxPair<AbaxDataString,abaxint>> jmarr = parseParam->joincolmap.getStrIntVector();
	AbaxDataString kstr;
	for ( i = 0; i < jmarr.length(); ++i ) {
		if( jmarr[i].key.size() > 0 ) {
			kstr = jmarr[i].key;
			if ( maps[jmarr[i].value]->getValue( kstr, pos ) ) {
				AbaxPair<AbaxLong, abaxint> pair( AbaxLong( pos ), jmarr[i].value );
				if ( !sarr[jmarr[i].value] ) {
					sarr[jmarr[i].value] = new JagArray<AbaxPair<AbaxLong, abaxint>>();
				}
				sarr[jmarr[i].value]->insert( pair );
			}
		}
	}



	for ( i = 0; i < num; ++i ) {
		if ( sarr[i] ) {
			abaxint hval = 0, hcont = 1, hall = 0, lpos = -1, upos = 0;
			AbaxPair<AbaxLong, abaxint> pair;
			sarr[i]->setGetpos( 0 );
			while ( sarr[i]->getNext( pair ) ) {
				upos = pair.key.value();
				jsvec[i].append( upos );
				jlen[i] += attrs[i][upos].length;
				if ( upos < numKeys[i] ) {
					++hall;
					if ( upos == lpos+1 ) {
						lpos = upos;
					} else {
						hcont = 0;
					}
				} else {
					hval = 1;
				}
			}

			if ( hall >= numKeys[i] ) hall = 1;
			else hall = 0;

			if ( !hcont || ( !hall && hval ) ) {
				if ( useHash < 0 ) dfSorted[i] = 1;
				sklen[i] = jlen[i] + JAG_UUID_FIELD_LEN;
			}

			delete sarr[i];
		}
	}

	if ( !hcli.connect( servobj->_dbConnector->_nodeMgr->_selfIP.c_str(), servobj->_port, 
					    "admin", "dummy", "test", unixSocket.c_str(), JAG_SERV_PARENT ) ) 
	{
		raydebug( stdout, JAG_LOG_LOW, "s4058 join connection, unable to make connection, retry ...\n" );
		if ( jda ) delete jda;
		if ( newbuf ) free( newbuf );
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) {
				servobj->_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
					parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
			} else if ( pindex[i] ) {
				servobj->_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], idxName[i],
					tableschema, indexschema, req.session->replicateType, 0 );				
			}
		}
		hcli.close();
		JagFileMgr::rmdir( jpath, true );
		return 0;
	}
	
	// separate hashjoin and regular join here
	if ( useHash >= 0 ) {
		//prt(("s5338 use hash join useHash=%d ...\n", useHash ));
		// use hash join
		JagStrSplit srv( servobj->_dbConnector->_nodeMgr->_allNodes, '|' );	
		if ( srv.length() < numCPUs ) numCPUs = srv.length();
		JagHashMap<AbaxFixString, AbaxFixString> hmaps[numCPUs];
		if ( 0 == useHash ) {
			st = 1;
			nst = 0;
			//prt(("s4023 st = 1; nst = 0;\n" ));
		} else {
			st = 0;
			nst = 1;
			//prt(("s4024 st = 0; nst = 1;\n" ));
		}
		AbaxString jmkey = jname + "_" + intToStr( nst );
		
		// store joinmap object attributes
		// pair order is: <sortlen, <kvlen, numKeys>>, <<minbuf, maxbuf>, <diskfamily, <attrs, parseParam>>
		AbaxPair< AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, 
				  AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>> pair;	
		pair.key.key = 0; // no sorted
		pair.key.value.key = kvlen[nst];
		pair.key.value.value = numKeys[nst];
		pair.value.key = JagDBPair( minmax[nst].minbuf, klen[nst], minmax[nst].maxbuf, klen[nst] );
		pair.value.value.key = (void*)df[nst];
		pair.value.value.value.key = (void*)attrs[nst];
		pair.value.value.value.value = (void*)parseParam;
		servobj->_joinMap->addKeyValue( jmkey, pair );
		/***
		prt(("s2836 sz=%lld k=[%s] sortklen=%lld, kvlen=%lld, numKs=%lld, minbf=[%s] maxbf=[%s] dfam=[%0x] atrs=[%0x] pprm=[%0x]\n", 
			  servobj->_joinMap->size(), jmkey.c_str(), sklen[nst], kvlen[nst], numKeys[nst], minmax[nst].minbuf,
			  minmax[nst].maxbuf, df[nst], attrs[nst], parseParam));
			  ***/
		sigpath2 = sigpath + "/" + intToStr( nst );
		JagFileMgr::makedirPath( sigpath2 );
		
		// for hash join, first ask each servers to send small table's data		
		pthread_t thrd[srv.length()];  // sr is all servers splitvec
		ParallelJoinPass pjp[srv.length()];
		for ( i = 0; i < srv.length(); ++i ) {
			pjp[i].ptab = ptab[st];
			pjp[i].ptab2 = ptab[nst];
			pjp[i].pindex = pindex[st];
			pjp[i].pindex2 = pindex[nst];
			pjp[i].klen = klen[st];
			pjp[i].klen2 = klen[nst];
			pjp[i].vlen = vlen[st];
			pjp[i].vlen2 = vlen[nst];
			pjp[i].kvlen = kvlen[st];
			pjp[i].kvlen2 = kvlen[nst];
			pjp[i].numKeys = numKeys[st];
			pjp[i].numKeys2 = numKeys[nst];
			pjp[i].numCols = numCols[st];
			pjp[i].numCols2 = numCols[nst];
			pjp[i].maps = maps[st];
			pjp[i].maps2 = maps[nst];
			pjp[i].attrs = attrs[st];
			pjp[i].attrs2 = attrs[nst];
			pjp[i].df = df[st];
			pjp[i].df2 = df[nst];
			pjp[i].rec = rec[st];
			pjp[i].rec2 = rec[nst];
			pjp[i].minbuf = minmax[st].minbuf;
			pjp[i].minbuf2 = minmax[nst].minbuf;
			pjp[i].maxbuf = minmax[st].maxbuf;
			pjp[i].maxbuf2 = minmax[nst].maxbuf;
			pjp[i].dfSorted = dfSorted[st];
			pjp[i].dfSorted2 = dfSorted[nst];
			pjp[i].objelem = objelem[st];
			pjp[i].objelem2 = objelem[nst];
			pjp[i].jlen = jlen[st];
			pjp[i].jlen2 = jlen[nst];
			pjp[i].sklen = sklen[st];
			pjp[i].sklen2 = sklen[nst];
			pjp[i].jsvec = &jsvec[st];
			pjp[i].jsvec2 = &jsvec[nst];
			pjp[i].tabnum = st;
			pjp[i].tabnum2 = nst;
			pjp[i].pos = i;
			pjp[i].totlen = totlen;
			pjp[i].jpath = jpath;
			pjp[i].jname = jname;			
			pjp[i].req = req;
			pjp[i].parseParam = parseParam;
			pjp[i].hcli = &hcli;
			pjp[i].jda = jda;
			pjp[i].stime = stime;
			pjp[i].uhost = srv[i];
			pjp[i].hmaps = hmaps;
			pjp[i].numCPUs = numCPUs;
			jagpthread_create( &thrd[i], NULL, joinRequestStatic, (void*)&pjp[i] );
		}

		//prt(("s5081 pthread_join %d threads ...\n", srv.length() ));
		for ( i = 0; i < srv.length(); ++i ) {
			pthread_join(thrd[i], NULL);
			if ( pjp[i].timeout ) timeout = 1;
		}
		//prt(("s5081 pthread_join %d threads done\n", srv.length() ));

		if ( !timeout ) {
			// then, after get all data and successfully create hashmaps, separate files by diskarray and join by piece
			pthread_t thrd2[df[st]->_darrlistlen];
			ParallelJoinPass pjp2[df[st]->_darrlistlen];
			for ( i = 0; i < df[st]->_darrlistlen; ++i ) {
				pjp2[i].ptab = ptab[st];
				pjp2[i].ptab2 = ptab[nst];
				pjp2[i].pindex = pindex[st];
				pjp2[i].pindex2 = pindex[nst];
				pjp2[i].klen = klen[st];
				pjp2[i].klen2 = klen[nst];
				pjp2[i].vlen = vlen[st];
				pjp2[i].vlen2 = vlen[nst];
				pjp2[i].kvlen = kvlen[st];
				pjp2[i].kvlen2 = kvlen[nst];
				pjp2[i].numKeys = numKeys[st];
				pjp2[i].numKeys2 = numKeys[nst];
				pjp2[i].numCols = numCols[st];
				pjp2[i].numCols2 = numCols[nst];
				pjp2[i].maps = maps[st];
				pjp2[i].maps2 = maps[nst];
				pjp2[i].attrs = attrs[st];
				pjp2[i].attrs2 = attrs[nst];
				pjp2[i].df = df[st];
				pjp2[i].df2 = df[nst];
				pjp2[i].rec = rec[st];
				pjp2[i].rec2 = rec[nst];
				pjp2[i].minbuf = minmax[st].minbuf;
				pjp2[i].minbuf2 = minmax[nst].minbuf;
				pjp2[i].maxbuf = minmax[st].maxbuf;
				pjp2[i].maxbuf2 = minmax[nst].maxbuf;
				pjp2[i].dfSorted = dfSorted[st];
				pjp2[i].dfSorted2 = dfSorted[nst];
				pjp2[i].objelem = objelem[st];
				pjp2[i].objelem2 = objelem[nst];
				pjp2[i].jlen = jlen[st];
				//prt(("s2004 i=%d pjp2[%d].jlen=%d st=%d\n", i, i, pjp2[i].jlen, st ));
				pjp2[i].jlen2 = jlen[nst];
				//prt(("s2005 i=%d pjp2[%d].jlen2=%d st=%d\n", i, i, pjp2[i].jlen2, st ));
				pjp2[i].sklen = sklen[st];
				pjp2[i].sklen2 = sklen[nst];
				pjp2[i].jsvec = &jsvec[st];
				pjp2[i].jsvec2 = &jsvec[nst];
				pjp2[i].tabnum = st;
				pjp2[i].tabnum2 = nst;
				pjp2[i].pos = i;
				pjp2[i].totlen = totlen;
				pjp2[i].jpath = jpath;
				pjp2[i].jname = jname;
				pjp2[i].req = req;
				pjp2[i].parseParam = parseParam;
				pjp2[i].hcli = &hcli;
				pjp2[i].jda = jda;
				pjp2[i].stime = stime;
				pjp2[i].hmaps = hmaps;
				pjp2[i].numCPUs = numCPUs;
				jagpthread_create( &thrd2[i], NULL, joinHashStatic, (void*)&pjp2[i] );
			}

			//prt(("s4008 pthread_join ...\n" ));
			for ( i = 0; i < df[st]->_darrlistlen; ++i ) {
				pthread_join(thrd2[i], NULL);
				if ( pjp2[i].timeout ) timeout = 1;
			}
			//prt(("s4008 pthread_join done \n" ));
		}
	} else {
		//prt(("s5838 use regular join ...\n" ));
		// use regular join
		// for regular join, ask each table to sort if needed and then begin join with data request
		pthread_t thrd[num];
		ParallelJoinPass pjp[num];
		for ( i = 0; i < num; ++i ) {
			if ( 0 == i ) {
				st = 0; nst = 1;
			} else if ( 1 == i ) {
				st = 1; nst = 0;
			}
			pjp[i].ptab = ptab[st];
			pjp[i].ptab2 = ptab[nst];
			pjp[i].pindex = pindex[st];
			pjp[i].pindex2 = pindex[nst];
			pjp[i].klen = klen[st];
			pjp[i].klen2 = klen[nst];
			pjp[i].vlen = vlen[st];
			pjp[i].vlen2 = vlen[nst];
			pjp[i].kvlen = kvlen[st];
			pjp[i].kvlen2 = kvlen[nst];
			pjp[i].numKeys = numKeys[st];
			pjp[i].numKeys2 = numKeys[nst];
			pjp[i].numCols = numCols[st];
			pjp[i].numCols2 = numCols[nst];
			pjp[i].maps = maps[st];
			pjp[i].maps2 = maps[nst];
			pjp[i].attrs = attrs[st];
			pjp[i].attrs2 = attrs[nst];
			pjp[i].df = df[st];
			pjp[i].df2 = df[nst];
			pjp[i].rec = rec[st];
			pjp[i].rec2 = rec[nst];
			pjp[i].minbuf = minmax[st].minbuf;
			pjp[i].minbuf2 = minmax[nst].minbuf;
			pjp[i].maxbuf = minmax[st].maxbuf;
			pjp[i].maxbuf2 = minmax[nst].maxbuf;
			pjp[i].dfSorted = dfSorted[st];
			pjp[i].dfSorted2 = dfSorted[nst];
			pjp[i].objelem = objelem[st];
			pjp[i].objelem2 = objelem[nst];
			pjp[i].jlen = jlen[st];
			//prt(("s5201 i=%d pjp[%d].jlen=%d st=%d\n", i, i, pjp[i].jlen, st ));
			pjp[i].jlen2 = jlen[nst];
			//prt(("s5202 i=%d pjp[%d].jlen2=%d st=%d\n", i, i, pjp[i].jlen2, st ));
			pjp[i].sklen = sklen[st];
			pjp[i].sklen2 = sklen[nst];
			pjp[i].jsvec = &jsvec[st];
			pjp[i].jsvec2 = &jsvec[nst];
			pjp[i].tabnum = st;
			pjp[i].tabnum2 = nst;
			pjp[i].totlen = totlen;
			pjp[i].jpath = jpath;
			pjp[i].jname = jname;			
			pjp[i].req = req;
			pjp[i].parseParam = parseParam;
			pjp[i].hcli = &hcli;
			pjp[i].jda = jda;
			pjp[i].stime = stime;
			pjp[i].numCPUs = numCPUs;
			jagpthread_create( &thrd[i], NULL, joinSortStatic, (void*)&pjp[i] );
		}
		
		// concurrently, do peices of join for current servers
		// first, wait for needed sort first, need to have two items ( dirs ) to make sure each table has finished sort procedure
		// currently, only has 2 tables to be join, use hardcode 2; later need to change to num of tables
		//prt(("s6002 JagFileMgr::numObjects( sigpath )=%d wait ...\n", JagFileMgr::numObjects( sigpath ) ));
		while ( JagFileMgr::numObjects( sigpath ) < 2 ) {		
			jagsleep( 100000, JAG_USEC );
			if ( req.session->sessionBroken ) { 
				timeout = 1;
				break; 
			}
		}
		//prt(("s6002 JagFileMgr::numObjects( sigpath )=%d done \n", JagFileMgr::numObjects( sigpath ) ));

		// then, reset diskarray family pointer to possible value-sorted diskarray family object
		for ( i = 0; i < num; ++i ) {
			df[i] = pjp[i].df;
			dfSorted[i] = pjp[i].dfSorted;
			if ( pjp[i].timeout ) timeout = 1;
		}
		
		if ( !timeout ) {
			// after sorted, join self pieces for current servers
			int wrc = 1, goNext[num];
			char *jbuf[num];
			const char *buffers[num], *buffers2[num];
			char *buf = (char*)jagmalloc( totlen+1 );
			char *cmpbuf = (char*)jagmalloc( jlen[0]+1 );
			JagMergeReader *jntr[num];

			//prt(("s4023 cmpbuf:jlen[0]=%d buf:totlen=%d\n", jlen[0], totlen ));

			memset( buf, 0, totlen+1 );
			memset( cmpbuf, 0, jlen[0]+1 );
			j = 0;

			for ( i = 0; i < num; ++i ) {
				goNext[i] = 1;
				jbuf[i] = (char*)jagmalloc( kvlen[i]+sklen[i]+1 );
				//prt(("s0203 i=%d jbuf kvlen[i]+sklen[i]+1=%d\n", i, kvlen[i]+sklen[i]+1 ));
				memset( jbuf[i], 0, kvlen[i]+sklen[i]+1 );
				jntr[i] = NULL;
				if ( !dfSorted[i] ) {
					df[i]->setFamilyRead( jntr[i], minmax[i].minbuf, minmax[i].maxbuf );
				} else {
					df[i]->setFamilyRead( jntr[i] );
				}
				buffers[i] = jbuf[i]+sklen[i];
				//prt(("s5984 i=%d sklen[%d]=%d buffers[%d]=[%s]\n", i, i, sklen[i], i, buffers[i] ));
				buffers2[i] = buf+j;
				j += kvlen[i];
				if ( !jntr[i] ) wrc = 0;
			}
			
			// begin pieces merge join
			// however, if any of merge reader is empty, ignore join
			if ( wrc ) {
				while ( 1 ) {
					if ( checkCmdTimeout( stime, parseParam->timeout ) ) {
						timeout = 1;
						break;
					}

					if ( req.session->sessionBroken ) { 
						timeout = 1;	
						break; 
					}
					
					// read data from file
					if ( goNext[0] > 0 ) {
						// get next line of data ( filter by parseParam if needed )
						while ( 1 ) {
							rc = jntr[0]->getNext(jbuf[0]);
							if ( !rc ) {
								// table 0 reaches to the end, break
								goNext[0] = -1;
								break;
							}

							if ( dfSorted[0] ) {
								goNext[0] = 0;
								break;
							}

							// read, check and send				
							dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0,0, " " ); // db format -> natural format
							if ( parseParam->hasWhere ) {
								root = parseParam->whereVec[0].tree->getRoot();
								buffers[1] = NULL;
								//prt(("s2939 in jagdbserver.cc calling root->checkFuncValid() bufers[0]=[%s]\n",  buffers[0] ));
								rc = root->checkFuncValid( jntr[0], maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
								buffers[1] = jbuf[1]+sklen[1];
							} else {
								rc = 1;
							}

							dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0, 0, " " ); // natural format -> db format
							if ( rc > 0 ) { // rc may be 1 or 2
								goNext[0] = 0;
								break;
							}
						}

						if ( goNext[0] < 0 ) break; // reaches to the end
						// after get jbuf[0], check if it's joinlen jbuf is same to the previous one
						if ( cmpbuf[0] != '\0' && memcmp( cmpbuf, jbuf[0], jlen[0] ) == 0 ) {
							// next data joinlen of buf is same as previous one, move back buffreader for second table
							cmpbuf[0] = '\0';
							jntr[1]->getRestartPos();
							goNext[1] = 1;
						} else if ( cmpbuf[0] != '\0' && memcmp( cmpbuf, jbuf[0], jlen[0] ) != 0 ) {
							// next data joinlen of buf is different from previous one, clean flag
							cmpbuf[0] = '\0';
							jntr[1]->setClearRestartPosFlag();
						}
					}

					if ( goNext[1] > 0 ) {
						// get next line of data ( filter by parseParam if needed )
						while ( 1 ) {
							rc = jntr[1]->getNext(jbuf[1]);
							if ( !rc ) {
								// table 1 reaches to the end, get next table 0 data ( filter by parseParam is needed )
								while ( 1 ) {
									rc = jntr[0]->getNext(jbuf[0]);
									if ( !rc ) {
										// table 0 reaches to the end, break
										goNext[0] = -1;
										break;
									}

									if ( dfSorted[0] ) {
										goNext[0] = 0;
										break;
									}

									// read, check and send				
									dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0,0, " " ); // db format -> natural format
									if ( parseParam->hasWhere ) {
										root = parseParam->whereVec[0].tree->getRoot();
										buffers[1] = NULL;
										//prt(("s2949 in jagdbserver.cc calling root->checkFuncValid() bufers[0]=[%s]\n",  buffers[0] ));
										rc = root->checkFuncValid( jntr[0], maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
										buffers[1] = jbuf[1]+sklen[1];
									} else {
										rc = 1;
									}
									dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0,0, " " ); // natural format -> db format
									if ( rc > 0 ) { // rc may be 1 or 2
										goNext[0] = 0;
										break;
									}
								}

								if ( goNext[0] < 0 ) break; // reaches to the end
								// if table 0 next buf is different from previous one for joinlen, end and break
								// otherwise, move back mergereader pointer for table 1 and get next buf for table 1
								if ( cmpbuf[0] == '\0' || memcmp( cmpbuf, jbuf[0], jlen[0] ) != 0 ) {
									goNext[0] = -1;
									break;
								}

								cmpbuf[0] = '\0';
								jntr[1]->getRestartPos();
								while ( 1 ) {
									rc = jntr[1]->getNext(jbuf[1]);
									if ( !rc ) {
										// table 1 reaches to the end, break for table 0 next boolean
										goNext[0] = -1;
										break;
									}
									if ( dfSorted[1] ) {
										goNext[1] = 0;
										break;
									}
									// read, check and send				
									dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0,0, " " ); // db format -> natural format
									if ( parseParam->hasWhere ) {
										root = parseParam->whereVec[0].tree->getRoot();
										buffers[0] = NULL;
										//prt(("s2959 in jagdbserver.cc calling root->checkFuncValid() bufers[1]=[%s]\n",  buffers[1] ));
										rc = root->checkFuncValid( jntr[1], maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
										buffers[0] = jbuf[0]+sklen[0];
									} else {
										rc = 1;
									}

									dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0,0, " " ); // natural format -> db format
									if ( rc > 0 ) { // rc may be 1 or 2
										goNext[1] = 0;
										break;
									}
								}
								break;
							}

							if ( dfSorted[1] ) {
								goNext[1] = 0;
								break;
							}

							// read, check and send				
							dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0, 0, " " ); // db format -> natural format
							if ( parseParam->hasWhere ) {
								root = parseParam->whereVec[0].tree->getRoot();
								buffers[0] = NULL;
								//prt(("s2969 in jagdbserver.cc calling root->checkFuncValid() bufers[1]=[%s]\n",  buffers[1] ));
								rc = root->checkFuncValid(jntr[1], maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
								buffers[0] = jbuf[0]+sklen[0];
							} else {
								rc = 1;
							}

							dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0,0, " " ); // natural format -> db format
							if ( rc > 0 ) { // rc may be 1 or 2
								goNext[1] = 0;
								break;
							}						
						} // end while
						if ( goNext[0] < 0 ) break; // reaches to the end
					}
					
					wrc = 0;
					// compare two buffers and set for innerjoin/leftjoin/rightjoin/fulljoin
					rc = memcmp( jbuf[0], jbuf[1], jlen[0] );
					if ( rc == 0 ) {
						// left and right table has the same join key
						memcpy( buf, jbuf[0]+sklen[0], kvlen[0] );
						memcpy( buf+kvlen[0], jbuf[1]+sklen[1], kvlen[1] );
						wrc = 1;
						// only move one of the key, use 2nd table for now
						if ( goNext[1] == 0 ) goNext[1] = 1;
						if ( jntr[1]->setRestartPos() ) {
							memcpy( cmpbuf, jbuf[0], jlen[0] ); // store current joinlen data, and also set buffreader position
						}
					} else if ( rc < 0 ) {
						// left data is smaller than right data
						if ( JAG_LEFTJOIN_OP == parseParam->opcode && goNext[0] >= 0 ) {
							memcpy( buf, jbuf[0]+sklen[0], kvlen[0] );
							memset( buf+kvlen[0], 0, kvlen[1] );
							wrc = 1;
						} else if ( JAG_FULLJOIN_OP == parseParam->opcode ) {
							memcpy( buf, jbuf[0]+sklen[0], kvlen[0] );
							memcpy( buf+kvlen[0], jbuf[1]+sklen[1], kvlen[1] );
							wrc = 1;
						}
						if ( goNext[0] == 0 ) goNext[0] = 1;
					} else {
						// left data is larger that right data
						if ( JAG_RIGHTJOIN_OP == parseParam->opcode ) {
							memset( buf, 0, kvlen[0] );
							memcpy( buf+kvlen[0], jbuf[1]+sklen[1], kvlen[1] );
							wrc = 1;	
						} else if ( JAG_FULLJOIN_OP == parseParam->opcode ) {			
							memcpy( buf, jbuf[0]+sklen[0], kvlen[0] );
							memcpy( buf+kvlen[0], jbuf[1]+sklen[1], kvlen[1] );
							wrc = 1;
						}
						if ( goNext[1] == 0 ) goNext[1] = 1;
					}
					
					dbNaturalFormatExchange( (char**)buffers2, num, numKeys, attrs ); // db format -> natural format
					if ( parseParam->hasWhere ) {
						root = parseParam->whereVec[0].tree->getRoot();
						//prt(("s2999 345 in jagdbserver.cc calling root->checkFuncValid() bufers2[0]=[%s]\n",  buffers2[0] ));
						rc = root->checkFuncValid( NULL, maps, attrs, buffers2, rstr, tmode, ttype, tlength, needInit, 0, 0 );
					} else {
						rc = 1;
					}
					if ( wrc && rc ) jda->writeit( jdapath, buf, totlen, NULL, true );
				}
			}

			//prt(("s3722 pthread_join num=%d ...\n", num ));
			for ( i = 0; i < num; ++i ) {
				pthread_join(thrd[i], NULL);
				if ( pjp[i].timeout ) timeout = 1;
				if ( jbuf[i] ) free( jbuf[i] );
				if ( jntr[i] ) delete jntr[i];
			}
			//prt(("s3722 pthread_join num=%d done\n", num ));

			if ( buf ) free ( buf );
			if ( cmpbuf ) free ( cmpbuf );
		} else {
			for ( i = 0; i < num; ++i ) {
				pthread_join(thrd[i], NULL);
			}
		}
	}

	jda->flushwrite();
	
	// if has column calculations and/or group by, reformat data
	if ( (parseParam->hasGroup || parseParam->hasColumn) && !timeout ) {
		// then, setup finalbuf and gbvbuf if needed
		// finalbuf, hasColumn len or KEYVALLEN*n if !hasColumn
		// gbvbuf, if has group by
		bool hasFirst = 0; std::atomic<abaxint> cnt; cnt = 0; AbaxFixString data;
		char *finalbuf = (char*)jagmalloc(finalsendlen+1);
		char *gbvbuf = (char*)jagmalloc(gbvsendlen+1);
		const char *buffers[num];
		JagParseParam *pparam[1]; pparam[0] = parseParam;
		memset(finalbuf, 0, finalsendlen+1);
		memset(gbvbuf, 0, gbvsendlen+1);
		JagMemDiskSortArray *gmdarr = NULL;
		if ( parseParam->hasGroup ) {
			//gmdarr = new JagMemDiskSortArray();
			gmdarr = newObject<JagMemDiskSortArray>();
			gmdarr->init( atoi((servobj->_cfg->getValue("GROUPBY_SORT_SIZE_MB", "1024")).c_str()), gbvhdr.c_str(), "GroupByValue" );
			gmdarr->beginWrite();
		}
		// setup jda2 to do group by and/or column calculation
		//JagDataAggregate *jda2 = new JagDataAggregate();
		JagDataAggregate *jda2 = newObject<JagDataAggregate>();
		jda2->setwrite( jdapath2, jdapath2, 2 );
		jda2->setMemoryLimit( jda->elements()*jda->getdatalen()*2 );
		
		// inside jda, data should be natural format
		while ( jda->readit( data ) ) {
			if ( req.session->sessionBroken ) break;
			// set buffers
			buffers[0] = (char*)data.c_str();
			buffers[1] = (char*)data.c_str()+kvlen[0];
			if ( gmdarr ) { // has group by
				rc = JagTable::buildDiskArrayForGroupBy( NULL, maps, attrs, req, buffers, parseParam, gmdarr, gbvbuf );
				if ( 0 == rc ) break;
			} else { // no group by
				if ( hasagg ) { // has aggregate functions
					JagTable::aggregateDataFormat( NULL, maps, attrs, req, buffers, parseParam, !hasFirst );
				} else { // non aggregate functions
					JagTable::nonAggregateFinalbuf( NULL, maps, attrs, req, buffers, parseParam, finalbuf, finalsendlen, jda2, 
						jdapath2, cnt, true, NULL, false );
					if ( parseParam->hasLimit && cnt > parseParam->limit ) break;
				}
			}
			hasFirst = 1;
		}

		if ( gmdarr ) {
			JagTable::groupByFinalCalculation( gbvbuf, true, finalsendlen, cnt, parseParam->limit, req, 
				jdapath2, parseParam, jda2, gmdarr, NULL );
		} else if ( hasagg ) {
			JagTable::aggregateFinalbuf( req, newhdr, 1, pparam, finalbuf, finalsendlen, jda2, jdapath2, cnt, true, NULL );
		}

		jda2->flushwrite();
		// after finish format group by and/or calculations, replace jda pointer to jda2
		if ( jda ) delete jda;
		jda = jda2;

		if ( finalbuf ) free( finalbuf );
		if ( gbvbuf ) free( gbvbuf );
		if ( gmdarr ) free( gmdarr );		
	}

	if ( timeout ) {
		JagTable::sendMessage( req, "E8012 Join command has timed out. Results have been truncated;", "ER" );
	}
	// send data to client if not 0
	if ( jda->elements() > 0 && !req.session->sessionBroken && jda ) {
		// in join
		// dataAggregateSendData( jda->elements(), req, jda );
		jda->sendDataToClient( jda->elements(), req );
	}
	// send "JOINEND" to client and wait for signal to free all memory ( make sure all servers finish join job )
	hcli.close();
	JagTable::sendMessage( req, "JOINEND", "OK" );
	recvData( req.session->sock, hdr, newbuf );
	
	// free all memory, and also remove all files and dirs under clipid_ip
	if ( jda ) delete jda;
	if ( newbuf ) free( newbuf );
	for ( i = 0; i < num; ++i ) {
		if ( dfSorted[i] ) {
			// use new create diskarray family, delete it as well as its record
			if ( df[i] ) {
				if ( df[i]->_schemaRecord ) delete ( df[i]->_schemaRecord );
				delete df[i];
			}
		}
		AbaxString jmkey = jname + "_" + intToStr( i );
		servobj->_joinMap->removeKey( jmkey );
		// prt(("s2837 remove joinmap elem=%d name=[%s]\n", servobj->_joinMap->size(), jmkey.c_str()));
		if ( ptab[i] ) {
			servobj->_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
				parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
		} else if ( pindex[i] ) {
			servobj->_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], idxName[i],
				tableschema, indexschema, req.session->replicateType, 0 );				
		}
	}

	JagFileMgr::rmdir( jpath, true );
	return 1;
}

// method to sort by columns if needed for each table
void *JagDBServer::joinSortStatic( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	AbaxDataString fpath; 
	abaxint i;
	
	if ( pass->ptab ) {
		fpath = pass->jpath + "/" + pass->ptab->getdbName() + "_" + pass->ptab->getTableName();
	} else {
		fpath = pass->jpath + "/" + pass->pindex->getdbName() + "_" + pass->pindex->getIndexName();	
	}
	JagFileMgr::makedirPath( fpath );
	// need to check if sort needed
	if ( pass->dfSorted ) {
		// first, use numCPUs to get num threads to sort concurrently
		abaxint len = pass->df->_darrlistlen;
		if ( len >= pass->numCPUs ) len = pass->numCPUs;
		
		pthread_t thrd[len];
		ParallelJoinPass pjp[len];
		for ( i = 0; i < len; ++i ) {
			pjp[i].ptab = pass->ptab;
			pjp[i].ptab2 = pass->ptab2;
			pjp[i].pindex = pass->pindex;
			pjp[i].pindex2 = pass->pindex2;
			pjp[i].klen = pass->klen;
			pjp[i].klen2 = pass->klen2;
			pjp[i].vlen = pass->vlen;
			pjp[i].vlen2 = pass->vlen2;
			pjp[i].kvlen = pass->kvlen;
			pjp[i].kvlen2 = pass->kvlen2;
			pjp[i].numKeys = pass->numKeys;
			pjp[i].numKeys2 = pass->numKeys2;
			pjp[i].numCols = pass->numCols;
			pjp[i].numCols2 = pass->numCols2;
			pjp[i].maps = pass->maps;
			pjp[i].maps2 = pass->maps2;
			pjp[i].attrs = pass->attrs;
			pjp[i].attrs2 = pass->attrs2;
			pjp[i].df = pass->df;
			pjp[i].df2 = pass->df2;
			pjp[i].rec = pass->rec;
			pjp[i].rec2 = pass->rec2;
			pjp[i].minbuf = pass->minbuf;
			pjp[i].minbuf2 = pass->minbuf2;
			pjp[i].maxbuf = pass->maxbuf;
			pjp[i].maxbuf2 = pass->maxbuf2;
			pjp[i].dfSorted = pass->dfSorted;
			pjp[i].dfSorted2 = pass->dfSorted2;
			pjp[i].objelem = pass->objelem;
			pjp[i].objelem2 = pass->objelem2;
			pjp[i].jlen = pass->jlen;
			pjp[i].jlen2 = pass->jlen2;
			pjp[i].sklen = pass->sklen;
			pjp[i].sklen2 = pass->sklen2;
			pjp[i].jsvec = pass->jsvec;
			pjp[i].jsvec2 = pass->jsvec2;
			
			pjp[i].totlen = pass->totlen;
			pjp[i].jpath = fpath;
			pjp[i].jname = pass->jname;			
			pjp[i].req = pass->req;
			pjp[i].parseParam = pass->parseParam;
			pjp[i].hcli = pass->hcli;
			pjp[i].jda = pass->jda;
			pjp[i].stime = pass->stime;
			pjp[i].tabnum = i;
			pjp[i].tabnum2 = pass->tabnum2;
			pjp[i].numCPUs = len;
			jagpthread_create( &thrd[i], NULL, joinSortStatic2, (void*)&pjp[i] );
		}
		
		for ( i = 0; i < len; ++i ) {
			pthread_join(thrd[i], NULL);
			if ( pjp[i].timeout ) pass->timeout = 1;
		}
		if ( pass->timeout ) {
			AbaxDataString sigpath = pass->jpath + "/fprep/" + intToStr( pass->tabnum );
			JagFileMgr::makedirPath( sigpath );
			pass->dfSorted = 0;
			return NULL;
		}
		
		// after get each file, make new darrfamily to use later
		abaxint aoffset = 0;
		AbaxDataString nhdr;
		if ( pass->ptab ) {
			fpath += AbaxDataString("/") + pass->ptab->getTableName();
		} else {
			fpath += AbaxDataString("/") + pass->pindex->getTableName() + "." + pass->pindex->getIndexName();
		}

		// nhdr = AbaxDataString("NN|") + longToStr( pass->sklen ) + "|" + longToStr( pass->kvlen ) + "|{";
		nhdr = fpath + AbaxDataString(":NN|") + longToStr( pass->sklen ) + "|" + longToStr( pass->kvlen ) + "|{";
		for ( i = 0; i < (*pass->jsvec).length(); ++i ) {
			nhdr += pass->rec->formatColumnRecord( pass->attrs[(*pass->jsvec)[i]].colname.c_str(), 
												   pass->attrs[(*pass->jsvec)[i]].type.c_str(), aoffset,
				pass->attrs[(*pass->jsvec)[i]].length, pass->attrs[(*pass->jsvec)[i]].sig, true );
			aoffset += pass->attrs[(*pass->jsvec)[i]].length;
		}
		// UUID column
		nhdr += pass->rec->formatColumnRecord( "UUID", JAG_C_COL_TYPE_UUID, aoffset, JAG_UUID_FIELD_LEN, 0, true );
		aoffset += JAG_UUID_FIELD_LEN;
		// original data as value part
		nhdr += pass->rec->formatColumnRecord( "ORIG", JAG_C_COL_TYPE_STR, aoffset, pass->kvlen, 0, false );
		aoffset += pass->kvlen;
		nhdr += "}";
		
		//JagSchemaRecord *rec =  new JagSchemaRecord();
		JagSchemaRecord *rec = newObject<JagSchemaRecord>();
		rec->parseRecord( nhdr.c_str() );
		/***
		pass->rec = new JagSchemaRecord();
		pass->rec->parseRecord( nhdr.c_str() );
		***/
		pass->rec = rec;
		pass->df = new JagDiskArrayFamily( pass->req.session->servobj, fpath, pass->rec, false, 32, true );
	} else {
		pass->dfSorted = 0;
	}
	
	// store joinmap object attributes
	// pair order is: <sortlen, <kvlen, numKeys>>, <<minbuf, maxbuf>, <diskfamily, <attrs, parseParam>>
	AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>> pair;	
	pair.key.key = pass->sklen;
	pair.key.value.key = pass->kvlen;
	pair.key.value.value = pass->numKeys;
	pair.value.key = JagDBPair( pass->minbuf, pass->klen, pass->maxbuf, pass->klen );
	pair.value.value.key = (void*)pass->df;
	pair.value.value.value.key = (void*)pass->attrs;
	pair.value.value.value.value = (void*)pass->parseParam;
	AbaxString jmkey = pass->jname + "_" + intToStr( pass->tabnum );
	pass->req.session->servobj->_joinMap->addKeyValue( jmkey, pair );
	// prt(("s2850 insert joinmap elem=%lld name=[%s] sortkeylen=%lld, kvlen=%lld, numKeys=%lld, minbuf=[%s] maxbuf=[%s] dfamily=[%0x] attrs=[%0x] pparam=[%0x]\n", 
	// pass->req.session->servobj->_joinMap->size(), jmkey.c_str(), pass->sklen, pass->kvlen, pass->numKeys, 
	// pass->minbuf, pass->maxbuf, pass->df, pass->attrs, pass->parseParam));
	AbaxDataString sigpath = pass->jpath + "/fprep/" + intToStr( pass->tabnum );
	JagFileMgr::makedirPath( sigpath );
	
	
	/*** for even and odd: e.g. ( both started with 0 )
	host_num  host_list  table0  table1  isOddTable(based on table number)  selfIsOdd(based on server list number)  svec[0]  svec[1]
		0		ip0			Y		N			F									F								  ip0	  N/A
		1		ip1			Y		N			F									T								  N/A	  ip1
		2		ip2			Y		N			F									F								  ip2	  N/A
		3		ip3			Y		N			F									T								  N/A	  ip3
		4		ip4			Y		N			F									F								  ip4	  N/A
		
		
		0		ip0			N		Y			T									F								  ip0	  N/A
		1		ip1			N		Y			T									T								  N/A	  ip1
		2		ip2			N		Y			T									F								  ip2	  N/A
		3		ip3			N		Y			T									T								  N/A	  ip3
		4		ip4			N		Y			T									F								  ip4	  N/A
	***/
	
	// after prepare data, parallel send data to each other server for join
	// for each table, get how many servers to be sent for data
	JagStrSplit srv( pass->req.session->servobj->_dbConnector->_nodeMgr->_allNodes, '|' );
	JagVector<AbaxDataString> svec[2];
	int totnum = srv.length(), isOdd = totnum%2, isOddTable = pass->tabnum%2, selfIsOdd = 0, snum = 0, pos = 0;
	for ( i = 0; i < totnum; ++i ) {
		if ( srv[i] == pass->req.session->servobj->_dbConnector->_nodeMgr->_selfIP ) {
			selfIsOdd = i%2;
		} else {
			if ( i%2 == 0 ) svec[0].append( srv[i] );
			else svec[1].append( srv[i] );
		}
	}

	if ( !selfIsOdd ) {
		if ( !isOddTable ) {
			if ( isOdd ) snum = totnum/2;
			else snum = totnum/2-1;
		} else {
			snum = totnum/2;
		}
	} else {
		if ( !isOddTable ) {
			snum = totnum/2-1;
		} else {
			if ( isOdd ) snum = totnum/2+1;
			else snum = totnum/2;
		}
	}
	
	pthread_t thrd[snum];
	ParallelJoinPass pjp[snum];
	for ( i = 0; i < snum; ++i ) {
		pjp[i].ptab = pass->ptab;
		pjp[i].ptab2 = pass->ptab2;
		pjp[i].pindex = pass->pindex;
		pjp[i].pindex2 = pass->pindex2;
		pjp[i].klen = pass->klen;
		pjp[i].klen2 = pass->klen2;
		pjp[i].vlen = pass->vlen;
		pjp[i].vlen2 = pass->vlen2;
		pjp[i].kvlen = pass->kvlen;
		pjp[i].kvlen2 = pass->kvlen2;
		pjp[i].numKeys = pass->numKeys;
		pjp[i].numKeys2 = pass->numKeys2;
		pjp[i].numCols = pass->numCols;
		pjp[i].numCols2 = pass->numCols2;
		pjp[i].maps = pass->maps;
		pjp[i].maps2 = pass->maps2;
		pjp[i].attrs = pass->attrs;
		pjp[i].attrs2 = pass->attrs2;
		pjp[i].df = pass->df;
		pjp[i].df2 = pass->df2;
		pjp[i].rec = pass->rec;
		pjp[i].rec2 = pass->rec2;
		pjp[i].minbuf = pass->minbuf;
		pjp[i].minbuf2 = pass->minbuf2;
		pjp[i].maxbuf = pass->maxbuf;
		pjp[i].maxbuf2 = pass->maxbuf2;
		pjp[i].dfSorted = pass->dfSorted;
		pjp[i].dfSorted2 = pass->dfSorted2;
		pjp[i].objelem = pass->objelem;
		pjp[i].objelem2 = pass->objelem2;
		pjp[i].jlen = pass->jlen;
		pjp[i].jlen2 = pass->jlen2;
		pjp[i].sklen = pass->sklen;
		pjp[i].sklen2 = pass->sklen2;
		pjp[i].jsvec = pass->jsvec;
		pjp[i].jsvec2 = pass->jsvec2;
		
		pjp[i].totlen = pass->totlen;
		pjp[i].jpath = pass->jpath;
		pjp[i].jname = pass->jname;			
		pjp[i].req = pass->req;
		pjp[i].parseParam = pass->parseParam;
		pjp[i].hcli = pass->hcli;
		pjp[i].jda = pass->jda;
		pjp[i].stime = pass->stime;
		pjp[i].tabnum = pass->tabnum;
		pjp[i].tabnum2 = pass->tabnum2;
		
		if ( !selfIsOdd ) {
			if ( !isOddTable ) {
				pjp[i].uhost = svec[0][pos++];
			} else {
				pjp[i].uhost = svec[1][pos++];
			}
		} else {
			if ( !isOddTable ) {
				pjp[i].uhost = svec[1][pos++];
			} else {
				pjp[i].uhost = svec[0][pos++];
			}
		}
		jagpthread_create( &thrd[i], NULL, joinRequestStatic, (void*)&pjp[i] );
	}

	for ( i = 0; i < snum; ++i ) {
		pthread_join(thrd[i], NULL);
		if ( pjp[i].timeout ) pass->timeout = 1;
	}
}

// method to parallel sort table for join
void *JagDBServer::joinSortStatic2( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	int rc, tmode = 0, tlength = 0;
	bool needInit = 1;
	AbaxDataString ttype = " ";
	abaxint i, j, k, aoffset, deach = pass->df->_darrlistlen/pass->numCPUs+1;
	abaxint dstart = deach*pass->tabnum;
	abaxint dend = dstart+deach;
	abaxint index = 0, slimit, rlimit, callCounts = -1, lastBytes = 0, tmpcnt;
	abaxint mlimit = availableMemory( callCounts, lastBytes )/8/pass->df->_darrlistlen/1024/1024;
	abaxint pairmapMemLimit = jagatoll(pass->req.session->servobj->_cfg->getValue("JOIN_BUFFER_SIZE_MB", "100").c_str())*1024*1024;
	if ( mlimit <= 0 ) mlimit = 1;
	if ( dend > pass->df->_darrlistlen ) dend = pass->df->_darrlistlen;
	AbaxDataString fpath, tpath; 
	AbaxFixString sres, value;
	ExpressionElementNode *root;
	JagDataAggregate jda;
	char *buf = (char*)jagmalloc(pass->kvlen+1);
	char *sbuf = (char*)jagmalloc(pass->sklen+1);
	char *tbuf = (char*)jagmalloc(pass->sklen+pass->kvlen+1);
	memset( buf, 0, pass->kvlen+1 );
	memset( sbuf, 0, pass->sklen+1 );
	memset( tbuf, 0, pass->sklen+pass->kvlen+1 );
	const char *buffers[2];
	JagDBMap pairmap;
	JagDBPair retpair;
	if ( pass->tabnum ) {
		buffers[0] = NULL;
		buffers[1] = buf;
	} else {
		buffers[0] = buf;
		buffers[1] = NULL;
	}
	
	for ( i = dstart; i < dend; ++i ) {
		tmpcnt = 0;
		JagDiskArrayServer *darr = pass->df->_darrlist[i];
		JagDBPair minpair( AbaxFixString( pass->minbuf, darr->KEYLEN ), value );
		JagDBPair maxpair( AbaxFixString( pass->maxbuf, darr->KEYLEN ), value );
		rc = darr->exist( minpair, &index, retpair );
		if ( !rc ) ++index;
		slimit = index;
		if ( slimit < 0 ) slimit = 0;
		rc = darr->exist( maxpair, &index, retpair );
		if ( !rc ) ++index;
		rlimit = index;
		rlimit = rlimit - slimit + 1;
		JagBuffReader br( darr, rlimit, darr->KEYLEN, darr->VALLEN, slimit, 0, mlimit );
		fpath = pass->jpath + "/" + longToStr( THREADID ) + "_" + longToStr( tmpcnt );
		tpath = fpath;
		jda.setwrite( fpath, fpath, 3 );
		
		while ( br.getNext( buf ) ) {
			if ( checkCmdTimeout( pass->stime, pass->parseParam->timeout ) ) {
				pass->timeout = 1;
				break;
			}
			if ( pass->req.session->sessionBroken ) { 
				pass->timeout = 1;	
				break; 
			}
			dbNaturalFormatExchange( buf, pass->numKeys, pass->attrs, 0,0, " " ); // db format -> natural format
			if ( pass->parseParam->hasWhere ) {
				root = pass->parseParam->whereVec[0].tree->getRoot();
				rc = root->checkFuncValid( NULL, &pass->maps, &pass->attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
			} else {
				rc = 1;
			}
			dbNaturalFormatExchange( buf, pass->numKeys, pass->attrs, 0,0, " " ); // natural format -> db format
			if ( rc > 0 ) { // rc may be 1 or 2
				aoffset = 0;
				for ( j = 0; j < pass->jsvec->length(); ++j ) {
					memcpy(sbuf+aoffset, buf+pass->attrs[(*(pass->jsvec))[j]].offset, pass->attrs[(*(pass->jsvec))[j]].length);
					aoffset += pass->attrs[(*(pass->jsvec))[j]].length;
				}
				memcpy(sbuf+aoffset, pass->req.session->servobj->_jagUUID->getString().c_str(), JAG_UUID_FIELD_LEN);
				JagDBPair pair( sbuf, pass->sklen, buf, pass->kvlen );				
				pairmap.insert( pair );
				if ( pairmap.elements()*(pass->sklen+pass->kvlen) > pairmapMemLimit ) {
					FixMap::iterator it;
					for ( it = pairmap._map->begin(); it != pairmap._map->end(); ++it ) {
						memcpy( tbuf, it->first.c_str(), pass->sklen );
						memcpy( tbuf+pass->sklen, it->second.first.c_str(), pass->kvlen );
						jda.writeit( fpath, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
					}
					jda.flushwrite();
					++tmpcnt;
					fpath = pass->jpath + "/" + longToStr( THREADID ) + "_" + longToStr( tmpcnt );
					tpath += AbaxDataString("|") + fpath;
					jda.setwrite( fpath, fpath, 3 );
					pairmap.clean();
				}
			}
		}
		if ( 1 == pass->timeout ) break;
		if ( pairmap.elements() > 0 ) {
			FixMap::iterator it;
			for ( it = pairmap._map->begin(); it != pairmap._map->end(); ++it ) {
				memcpy( tbuf, it->first.c_str(), pass->sklen );
				memcpy( tbuf+pass->sklen, it->second.first.c_str(), pass->kvlen );
				jda.writeit( fpath, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
			}
			jda.flushwrite();
			pairmap.clean();
		}
		// after fliter all data in one jdb file, merge tmp files to a single big file
		JagStrSplit sp( tpath.c_str(), '|' );
		int fd[sp.length()];
		for ( j = 0; j < sp.length(); ++j ) {
			fd[j] = jagopen( sp[j].c_str(), O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
		}
		JagVector<OnefileRangeFD> fRange(8);
		OnefileRangeFD tempRange;
		for ( j = 0; j < sp.length(); ++j ) {
			tempRange.fd = fd[j];
			tempRange.startpos = 0;
			tempRange.readlen = -1;
			tempRange.memmax = mlimit;
			fRange.append( tempRange );
		}
		JagSingleMergeReader nts( fRange, fRange.size(), pass->sklen, pass->kvlen );
		if ( 0 == i ) {
			if ( pass->ptab ) {
				fpath = pass->jpath + "/" + pass->ptab->getTableName() + ".jdb";
			} else {
				fpath = pass->jpath + "/" + pass->pindex->getTableName() + "." + pass->pindex->getIndexName() + ".jdb";
			}
		} else {
			if ( pass->ptab ) {
				fpath = pass->jpath + "/" + pass->ptab->getTableName() + "." + intToStr( i ) + ".jdb";
			} else {
				fpath = pass->jpath + "/" + pass->pindex->getTableName() + "." + pass->pindex->getIndexName() + "." + intToStr( i ) + ".jdb";
			}
		}
		jda.setwrite( fpath, fpath, 3 );
		while ( nts.getNext( tbuf ) ) {
			if ( checkCmdTimeout( pass->stime, pass->parseParam->timeout ) ) {
				pass->timeout = 1;
				break;
			}
			if ( pass->req.session->sessionBroken ) { 
				pass->timeout = 1;	
				break; 
			}
			jda.writeit( fpath, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
		}
		jda.flushwrite();
		
		for ( j = 0; j < sp.length(); ++j ) {
			jagclose( fd[j] );
			jagunlink( sp[j].c_str() );
		}
		if ( 1 == pass->timeout ) break;
	}
	free( buf );
	free( sbuf );
	free( tbuf );
}

// method to join main table with multiple hash tables
void *JagDBServer::joinHashStatic( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	// for each of the family file, move one by one and check each hash maps for join part	
    JagDiskArrayServer *darr;
    if ( pass->ptab ) darr = pass->ptab->_darrFamily->_darrlist[pass->pos];
    else darr = pass->pindex->_darrFamily->_darrlist[pass->pos];
	
    JagDBPair retpair;
    AbaxFixString key, value;
	abaxint i, aoffset = 0, alen = 0, callCounts = -1, lastBytes = 0, mlimit, slimit = 0, rlimit = darr->_arrlen;
	ExpressionElementNode *root;
	AbaxFixString sres;
	AbaxDataString jdapath = pass->jpath + "/alldata";
	int rc, tmode = 0, tlength = 0, insertCode;
	bool needInit = 1;
	const char *buffers[2], *buffers2[2];
	AbaxDataString ttype = " ";
    char jbuf[pass->jlen+1], *buf = (char*)jagmalloc(pass->kvlen+1), *tbuf = (char*)jagmalloc(pass->totlen+1);
    memset(buf, 0, pass->kvlen+1);
	memset(jbuf, 0, pass->jlen+1);
	memset(tbuf, 0, pass->totlen+1);
	mlimit = availableMemory( callCounts, lastBytes )/8/pass->df->_darrlistlen/1024/1024;
	if ( mlimit <= 0 ) mlimit = 1;
	key.point( jbuf, pass->jlen );
	int vnumKeys[2]; const JagSchemaAttribute *vattrs[2];

	if ( pass->tabnum ) {
		buffers[0] = NULL;
		buffers[1] = buf;
		buffers2[0] = tbuf;
		buffers2[1] = tbuf+pass->kvlen2;
		vnumKeys[0] = pass->numKeys2;
		vnumKeys[1] = pass->numKeys;
		vattrs[0] = pass->attrs2;
		vattrs[1] = pass->attrs;
	} else {
		buffers[0] = buf;
		buffers[1] = NULL;
		buffers2[0] = tbuf;
		buffers2[1] = tbuf+pass->kvlen;
		vnumKeys[0] = pass->numKeys;
		vnumKeys[1] = pass->numKeys2;
		vattrs[0] = pass->attrs;
		vattrs[1] = pass->attrs2;
	}

    if ( pass->parseParam->hasWhere ) {
        AbaxFixString minkey( pass->minbuf, darr->KEYLEN );
        JagDBPair minpair( minkey, value );
        AbaxFixString maxkey( pass->maxbuf, darr->KEYLEN );
        JagDBPair maxpair( maxkey, value );
        darr->exist( minpair, &slimit, retpair );
        if ( slimit < 0 ) slimit = 0;
        darr->exist( maxpair, &rlimit, retpair );
        if ( rlimit >= darr->_arrlen ) rlimit = darr->_arrlen;
        rlimit = rlimit - slimit + 1;
    }
    JagBuffReader br( darr, rlimit, darr->KEYLEN, darr->VALLEN, slimit, 0, mlimit );

    while ( br.getNext( buf ) ) {
		if ( checkCmdTimeout( pass->stime, pass->parseParam->timeout ) ) {
			pass->timeout = 1;
			break;
		}
		if ( pass->req.session->sessionBroken ) { 
			pass->timeout = 1;	
			break; 
		}

		dbNaturalFormatExchange( buf, pass->numKeys, pass->attrs, 0,0, " " ); // db format -> natural format
		if ( pass->parseParam->hasWhere ) {
			root = pass->parseParam->whereVec[0].tree->getRoot();
			rc = root->checkFuncValid( NULL, &pass->maps, &pass->attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
		} else {
			rc = 1;
		}
		dbNaturalFormatExchange( buf, pass->numKeys, pass->attrs, 0,0, " " ); // natural format -> db format	
		if ( rc > 0 ) { // rc may be 1 or 2
			aoffset = 0;
			for ( i = 0; i < pass->jsvec->length(); ++i ) {
				memcpy(jbuf+aoffset, buf+pass->attrs[(*(pass->jsvec))[i]].offset, pass->attrs[(*(pass->jsvec))[i]].length);
				aoffset += pass->attrs[(*(pass->jsvec))[i]].length;
			}
			// then, go through each of the hashmap to see if current line of data is exist or not, and insert to jda if exists
			for ( i = 0; i < pass->numCPUs; ++i ) {	
				if ( (pass->hmaps)[i].getValue( key, value ) ) {
					aoffset = 0;
					alen = value.length();
					while ( aoffset < alen ) {
						// exist, store to jda
						if ( pass->tabnum ) {
							memcpy( tbuf, value.c_str()+aoffset, pass->kvlen2 );
							memcpy( tbuf+pass->kvlen2, buf, pass->kvlen );
						} else {
							memcpy( tbuf, buf, pass->kvlen );
							memcpy( tbuf+pass->kvlen, value.c_str()+aoffset, pass->kvlen2 );
						}
						dbNaturalFormatExchange( (char**)buffers2, 2, vnumKeys, vattrs ); // db format -> natural format
						// at last, need to make sure data is valid with where before store it
						if ( pass->parseParam->hasWhere ) {
							root = pass->parseParam->whereVec[0].tree->getRoot();
							rc = root->checkFuncValid( NULL, &pass->maps, &pass->attrs, buffers2, sres, tmode, ttype, tlength, needInit, 0, 0 );
						} else {
							rc = 1;
						}
						if ( rc ) pass->jda->writeit( jdapath, tbuf, pass->totlen, NULL, true );
						aoffset += pass->kvlen2;
					}
				}
			}
		}
	}
    free( buf );
    free( tbuf );
}

// for each table, begin requesting data from other servers to join
// there are two ways to request; one is store data directly to hashmap ( for hash join )
// and the other is request with join together
void *JagDBServer::joinRequestStatic( void * ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->hcli->_connMap, pass->uhost.c_str() );
	int useHash = 0;
	if ( pass->hmaps ) useHash = 1;
	AbaxDataString rcmd = AbaxDataString("_serv_reqdatajoin|") + intToStr( useHash ) + "|" + intToStr( pass->tabnum2 ) + "|" + pass->jname;
	char jbuf[pass->jlen+1];
	const char *p = NULL;
	memset( jbuf, 0, pass->jlen+1 );
	abaxint i, offset, aoffset, len = 0;
	
	if ( useHash ) {
		// hash join, get data and store to hashmap
		abaxint hpos = pass->pos % pass->numCPUs;
		AbaxFixString key, val, val2, val3;
		key.point( jbuf, pass->jlen );
		// first, check if request host is same as self host, if not, send request to other servers
		// otherwise, read current server's small table and store them to hashmap
		if ( pass->req.session->servobj->_localInternalIP == pass->uhost ) {
			JagMergeReader *ntr = NULL;
			pass->df2->setFamilyRead( ntr, pass->minbuf2, pass->maxbuf2 );

			if ( ntr ) {
				ExpressionElementNode *root;
				AbaxFixString sres;
				int rc, tmode = 0, tlength = 0;
				bool needInit = 1;
				char *buf = (char*)jagmalloc( pass->kvlen2+1 );
				const char *buffers[2];
				AbaxDataString ttype= " ";
				memset( buf, 0, pass->kvlen2+1 );
				// prt(("s5982 pass->tabnum2=%d\n", pass->tabnum2 ));
				if ( pass->tabnum2 ) {
					buffers[0] = NULL;
					buffers[1] = buf;
				} else {
					buffers[0] = buf;
					buffers[1] = NULL;
				}
	
				while ( ntr->getNext( buf ) ) {
					// read, check and send	
					//prt(("s0302  buffers[0]=%0x  &buf=%0x buf=[%s]\n", buffers[0], buf, buf ));
					dbNaturalFormatExchange( buf, pass->numKeys2, pass->attrs2, 0,0, " " ); // db format -> natural format
					//prt(("s0303  buffers[0]=%0x  &buf=%0x buf=[%s]\n", buffers[0], buf, buf ));
					if ( pass->parseParam->hasWhere ) {
						root = pass->parseParam->whereVec[0].tree->getRoot();
						rc = root->checkFuncValid( ntr, &pass->maps, &pass->attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
					} else {
						rc = 1;
					}

					dbNaturalFormatExchange( buf, pass->numKeys2, pass->attrs2, 0,0, " " ); // natural format -> db format
					if ( rc > 0 ) { // rc may be 1 or 2
						aoffset = 0;
						for ( i = 0; i < pass->jsvec2->length(); ++i ) {
							memcpy(jbuf+aoffset, buf+pass->attrs2[(*(pass->jsvec2))[i]].offset, pass->attrs2[(*(pass->jsvec2))[i]].length);
							aoffset += pass->attrs2[(*(pass->jsvec2))[i]].length;
						}
						// then, append joinkey, original buffers into hashmap
						val.point( buf, pass->kvlen2 );
						rc = (pass->hmaps)[hpos].appendValue( key, val );
						(pass->hmaps)[hpos].getValue( key, val2 );
					}
				}
				if ( buf ) free( buf );
				if ( ntr ) delete ntr;
			}
		} else {
			if ( jcli->queryDirect( rcmd.c_str(), true, false, false ) == 0 ) {
				return NULL;
			}
			while ( jcli->reply() > 0 ) {
				if ( checkCmdTimeout( pass->stime, pass->parseParam->timeout ) ) {
					pass->timeout = 1;
					while( jcli->reply() > 0 ) {}
					break;
				}
				if ( pass->req.session->sessionBroken ) {
					pass->timeout = 1;
					while( jcli->reply() > 0 ) {}
					break;
				}
				offset = 0;
				p = jcli->getMessage();
				len = jcli->getMessageLen();
				while ( offset < len ) {
					aoffset = 0;
					for ( i = 0; i < pass->jsvec2->length(); ++i ) {
						memcpy(jbuf+aoffset, p+offset+pass->attrs2[(*(pass->jsvec2))[i]].offset, pass->attrs2[(*(pass->jsvec2))[i]].length);
						aoffset += pass->attrs2[(*(pass->jsvec2))[i]].length;
					}
					// then, append joinkey, original buffers into hashmap
					val.point( p+offset, pass->kvlen2 );
					int rc = (pass->hmaps)[hpos].appendValue( key, val );
					(pass->hmaps)[hpos].getValue( key, val2 );
					offset += pass->kvlen2;
				}
			}
		}
	} else {
		if ( jcli->queryDirect( rcmd.c_str(), true, false, false ) == 0 ) {
			return NULL;
		}
		// regular join, get data and join to get result together ( store in jda )
		ExpressionElementNode *root;
		JagMergeReader *jntr = NULL;
		if ( pass->dfSorted ) {
			pass->df->setFamilyRead( jntr );
		} else {
			pass->df->setFamilyRead( jntr, pass->minbuf, pass->maxbuf );
		}
		if ( jntr ) {
			AbaxDataString jdapath = pass->jpath + "/alldata"; AbaxFixString sres;
			int wrc, rc, tmode = 0, tlength = 0, hostLeft = pass->tabnum, vnumKeys[2];
			bool needInit = 1;
			const char *buffers[2], *buffers2[2];
			char cmpbuf[pass->jlen+1];
			AbaxDataString ttype = " ";
			const JagSchemaAttribute *vattrs[2];
			int goNext[2] = {1, 1};
			char *jbuf = (char*)jagmalloc( pass->kvlen+pass->sklen+1 ), *buf = (char*)jagmalloc( pass->totlen+1 );	
			memset( buf, 0, pass->totlen+1 );
			memset( jbuf, 0, pass->kvlen+pass->sklen+1 );
			memset( cmpbuf, 0, pass->jlen+1 );
		
			if ( pass->tabnum ) {
				vnumKeys[0] = pass->numKeys2;
				vnumKeys[1] = pass->numKeys;
				vattrs[0] = pass->attrs2;
				vattrs[1] = pass->attrs;
				buffers[0] = NULL;
				buffers[1] = jbuf+pass->sklen;
				buffers2[0] = buf;
				buffers2[1] = buf+pass->kvlen2;
			} else {
				vnumKeys[0] = pass->numKeys;
				vnumKeys[1] = pass->numKeys2;
				vattrs[0] = pass->attrs;
				vattrs[1] = pass->attrs2;
				buffers[0] = jbuf+pass->sklen;
				buffers[1] = NULL;
				buffers2[0] = buf;
				buffers2[1] = buf+pass->kvlen;
			}
			
			while ( 1 ) {
				if ( checkCmdTimeout( pass->stime, pass->parseParam->timeout ) ) {
					pass->timeout = 1;
					break;
				}
				if ( pass->req.session->sessionBroken ) { 
					pass->timeout = 1;
					break; 
				}
				
				// read data from socket buffer and local merge reader
				// pay attention, goNext[0] is always from socket buffer
				if ( goNext[0] > 0 ) {
					// get next data
					offset += pass->kvlen2+pass->sklen2;
					while ( 1 ) {
						if ( offset >= len ) {
							if ( jcli->reply() <= 0 ) {
								goNext[0] = -1;
								break;
							} else {
								offset = 0;
								p = jcli->getMessage();
								len = jcli->getMessageLen();
							}
						} else {
							goNext[0] = 0;
							break;
						}
					}
					if ( goNext[0] < 0 || !p ) break; // reach to the end
					// after get next position of p, check if it's joinlen jbuf is same to the previous one
					if ( cmpbuf[0] != '\0' && memcmp( cmpbuf, p+offset, pass->jlen ) == 0 ) {
						// next data joinlen of buf is same as previous one, move back buffreader for second table
						cmpbuf[0] = '\0';
						jntr->getRestartPos();
						goNext[1] = 1;
					} else if ( cmpbuf[0] != '\0' && memcmp( cmpbuf, p+offset, pass->jlen ) != 0 ) {
						// next data joinlen of buf is different from previous one, clean flag
						cmpbuf[0] = '\0';
						jntr->setClearRestartPosFlag();
					}
				}
				if ( goNext[1] > 0 ) {
					// get next line of data ( filter by parseParam if needed )				
					while ( 1 ) {
						rc = jntr->getNext(jbuf);
						if ( !rc ) {
							// table 1 reaches to the end, get next table 0 data from socket buffer
							offset += pass->kvlen2+pass->sklen2;
							while ( 1 ) {
								if ( offset >= len ) {
									if ( jcli->reply() <= 0 ) {
										goNext[0] = -1;
										break;
									} else {
										offset = 0;
										p = jcli->getMessage();
										len = jcli->getMessageLen();
									}
								} else {
									goNext[0] = 0;
									break;
								}
							}
							if ( goNext[0] < 0 ) break; // reaches to the end
							// if table 0 next buf is different from previous one for joinlen, end and break
							// otherwise, move back mergereader pointer for table 1 and get next buf for table 1
							if ( cmpbuf[0] == '\0' || memcmp( cmpbuf, p+offset, pass->jlen ) != 0 ) {
								goNext[0] = -1;
								break;
							}
							cmpbuf[0] = '\0';
							jntr->getRestartPos();
							while ( 1 ) {
								rc = jntr->getNext(jbuf);
								if ( !rc ) {
									// table 1 reaches to the end, break for table 0 next boolean
									goNext[0] = -1;
									break;
								}
								if ( pass->dfSorted ) {
									goNext[1] = 0;
									break;
								}
								// read, check and send				
								dbNaturalFormatExchange( jbuf, pass->numKeys, pass->attrs, 0,0, " " ); // db format -> natural format
								if ( pass->parseParam->hasWhere ) {
									root = pass->parseParam->whereVec[0].tree->getRoot();
									rc = root->checkFuncValid( jntr, &pass->maps, &pass->attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
								} else {
									rc = 1;
								}
								dbNaturalFormatExchange( jbuf, pass->numKeys, pass->attrs, 0,0, " " ); // natural format -> db format
								if ( rc > 0 ) { // rc may be 1 or 2
									goNext[1] = 0;
									break;
								}
							}
							break;
						}
						if ( pass->dfSorted ) {
							goNext[1] = 0;
							break;
						}
						// read, check and send				
						dbNaturalFormatExchange( jbuf, pass->numKeys, pass->attrs, 0,0, " " ); // db format -> natural format
						if ( pass->parseParam->hasWhere ) {
							root = pass->parseParam->whereVec[0].tree->getRoot();
							rc = root->checkFuncValid( jntr, &pass->maps, &pass->attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
						} else {
							rc = 1;
						}
						dbNaturalFormatExchange( jbuf, pass->numKeys, pass->attrs, 0,0, " " ); // natural format -> db format
						if ( rc > 0 ) { // rc may be 1 or 2
							goNext[1] = 0;
							break;
						}						
					}
					if ( goNext[0] < 0 ) break; // reaches to the end
				}
				
				wrc = 0;
				// compare two buffers and set for innerjoin/leftjoin/rightjoin/fulljoin
				if ( pass->tabnum ) rc = memcmp( p+offset, jbuf, pass->jlen );		
				else rc = memcmp( jbuf, p+offset, pass->jlen );
				if ( rc == 0 ) {
					// left and right table has the same join key
					if ( pass->tabnum ) {
						memcpy( buf, p+offset+pass->sklen2, pass->kvlen2 );
						memcpy( buf+pass->kvlen2, jbuf+pass->sklen, pass->kvlen );
					} else {
						memcpy( buf, jbuf+pass->sklen, pass->kvlen );
						memcpy( buf+pass->kvlen, p+offset+pass->sklen2, pass->kvlen2 );
					}
					wrc = 1;
					// move next for the non socket-buffer one, goNext[1]
					if ( goNext[1] == 0 ) goNext[1] = 1;
					if ( jntr->setRestartPos() ) {
						memcpy( cmpbuf, p+offset, pass->jlen ); // store current joinlen data, and also set buffreader position
					}
				} else if ( rc < 0 ) {
					// left data is smaller than right data
					if ( JAG_LEFTJOIN_OP == pass->parseParam->opcode ) {
						if ( pass->tabnum ) {
							memcpy( buf, p+offset+pass->sklen2, pass->kvlen2 );
							memset( buf+pass->kvlen2, 0, pass->kvlen );
						} else {
							memcpy( buf, jbuf+pass->sklen, pass->kvlen );
							memset( buf+pass->kvlen, 0, pass->kvlen2 );
						}
						wrc = 1;
					} else if ( JAG_FULLJOIN_OP == pass->parseParam->opcode ) {
						if ( pass->tabnum ) {
							memcpy( buf, p+offset+pass->sklen2, pass->kvlen2 );
							memcpy( buf+pass->kvlen2, jbuf+pass->sklen, pass->kvlen );
						} else {
							memcpy( buf, jbuf+pass->sklen, pass->kvlen );
							memcpy( buf+pass->kvlen, p+offset+pass->sklen2, pass->kvlen2 );
						}
						wrc = 1;
					}
					if ( pass->tabnum && 0 == goNext[0] ) {
						goNext[0] = 1;
					} else if ( !pass->tabnum && 0 == goNext[1] ) {
						goNext[1] = 1;
					}
				} else {
					// left data is larger that right data
					if ( JAG_RIGHTJOIN_OP == pass->parseParam->opcode ) {
						if ( pass->tabnum ) {
							memset( buf, 0, pass->kvlen2 );
							memcpy( buf+pass->kvlen2, jbuf+pass->sklen, pass->kvlen );
						} else {
							memset( buf, 0, pass->kvlen );
							memcpy( buf+pass->kvlen, p+offset+pass->sklen2, pass->kvlen2 );
						}
						wrc = 1;	
					} else if ( JAG_FULLJOIN_OP == pass->parseParam->opcode ) {			
						if ( pass->tabnum ) {
							memcpy( buf, p+offset+pass->sklen2, pass->kvlen2 );
							memcpy( buf+pass->kvlen2, jbuf+pass->sklen, pass->kvlen );
						} else {
							memcpy( buf, jbuf+pass->sklen, pass->kvlen );
							memcpy( buf+pass->kvlen, p+offset+pass->sklen2, pass->kvlen2 );
						}
						wrc = 1;
					}
					if ( pass->tabnum && 0 == goNext[1] ) {
						goNext[1] = 1;
					} else if ( !pass->tabnum && 0 == goNext[0] ) {
						goNext[0] = 1;
					}
				}
				
				dbNaturalFormatExchange( (char**)buffers2, 2, vnumKeys, vattrs ); // db format -> natural format
				// at last, need to make sure data is valid with where before store it
				if ( pass->parseParam->hasWhere ) {
					root = pass->parseParam->whereVec[0].tree->getRoot();
					rc = root->checkFuncValid( jntr, &pass->maps, &pass->attrs, buffers2, sres, tmode, ttype, tlength, needInit, 0, 0 );
				} else {
					rc = 1;
				}
				if ( wrc && rc ) pass->jda->writeit( jdapath, buf, pass->totlen, NULL, true );
			}
			if ( buf ) free( buf );
			if ( jbuf ) free( jbuf );
			if ( jntr ) delete jntr;
			if ( goNext[0] >= 0 ) {
				while ( jcli->reply() > 0 ) {}
			}
		}
	}
}

// for each join request, begin processing and send requested data block batch
// pmesg: "_serv_reqdatajoin|useHash|reqtabnum|jname"
void JagDBServer::joinRequestSend( const char *mesg, const JagRequest &req )
{
	JagStrSplitWithQuote sp( mesg, '|' );
	int useHash = atoi( sp[1].c_str() ), rtabnum = atoi( sp[2].c_str() );
	AbaxDataString jpath = _cfg->getTEMPJoinHOME() + "/" + sp[3], cpath = jpath + "/fprep";
	// check fprep file exist, if use hash, check only 1, otherwise check 2
	if ( useHash ) {
		while ( JagFileMgr::numObjects( cpath ) < 1 ) {
			jagsleep( 100000, JAG_USEC );
			if ( req.session->sessionBroken ) { 
				return;
			}
		}
	} else {		
		while ( JagFileMgr::numObjects( cpath ) < 2 ) {
			jagsleep( 100000, JAG_USEC );
			if ( req.session->sessionBroken ) { 
				return;
			}
		}
	}
	
	AbaxString jmkey = sp[3] + "_" + sp[2];
	// pair order is: <sortlen, <kvlen, numKeys>>, <<minbuf, maxbuf>, <diskfamily, <attrs, parseParam>>
	AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>> pair;
	if ( !_joinMap->getValue( jmkey, pair ) ) {
		return;
	}
	
	abaxint sklen = pair.key.key.value(), kvlen = pair.key.value.key.value(), numKeys = pair.key.value.value.value();
	const char *minbuf = pair.value.key.key.c_str(), *maxbuf = pair.value.key.value.c_str();
	JagDiskArrayFamily *df = (JagDiskArrayFamily*)pair.value.value.key.value();
	const JagSchemaAttribute *attrs = (const JagSchemaAttribute*)pair.value.value.value.key.value();
	JagParseParam *pparam = (JagParseParam*)pair.value.value.value.value.value();
	// prt(("s2870 elem=%lld name=[%s] sortklen=%lld, kvln=%lld, numKs=%lld, minbuf=[%s] maxbuf=[%s] dfamily=[%0x] attrs=[%0x] pparam=[%0x]\n", 
	// _joinMap->size(), jmkey.c_str(), sklen, kvlen, numKeys, minbuf, maxbuf, df, attrs, pparam));
	JagMergeReader *ntr = NULL;
	if ( sklen > 0 ) { // already sorted
		df->setFamilyRead( ntr );
	} else {
		df->setFamilyRead( ntr, minbuf, maxbuf );
	}

	if ( ntr ) {
		// malloc a large block of memory to store batch block
		ExpressionElementNode *root;
		AbaxFixString sres;
		int rc, tmode = 0, tlength = 0;
		abaxint boffset = 0, totlen = sklen+kvlen, totbytes = jagatoll(_cfg->getValue("JOIN_BATCH_LINE", "100000").c_str())*totlen;
		bool needInit = 1;
		char *bbuf = (char*)jagmalloc( totbytes+1 ), *buf = (char*)jagmalloc( totlen+1 );
		const char *buffers[2];
		AbaxDataString ttype = " ";
		memset( bbuf, 0, totbytes+1 );
		memset( buf, 0, totlen+1 );
		if ( rtabnum ) {
			buffers[0] = NULL;
			buffers[1] = buf;
		} else {
			buffers[0] = buf;
			buffers[1] = NULL;
		}

		while ( ntr->getNext( buf ) ) {
			if ( boffset < totbytes ) {
				if ( sklen > 0 ) {
					// sorted by columns, read and send
					memcpy( bbuf+boffset, buf, totlen );
					boffset += totlen;
				} else {
					// read, check and send				
					dbNaturalFormatExchange( buf, numKeys, attrs, 0,0, " " ); // db format -> natural format
					if ( pparam->hasWhere ) {
						root = pparam->whereVec[0].tree->getRoot();
						rc = root->checkFuncValid(ntr, NULL, &attrs, buffers, sres, tmode, ttype, tlength, needInit, 0, 0 );
					} else {
						rc = 1;
					}
					dbNaturalFormatExchange( buf, numKeys, attrs, 0,0, " " ); // natural format -> db format
					if ( rc > 0 ) { // rc may be 1 or 2
						memcpy( bbuf+boffset, buf, totlen );
						boffset += totlen;
					}
				}
			} else {
				bbuf[boffset] = '\0';
				// if one batch if full, flush the whole batch to another server
				rc = JagTable::sendMessageLength( req, bbuf, boffset, "XX" );
				boffset = 0;
				if ( rc < 0 ) { break; }
			}
		}
		if ( boffset > 0 ) {
			// flush last time
			bbuf[boffset] = '\0';
			rc = JagTable::sendMessageLength( req, bbuf, boffset, "XX" );
			boffset = 0;
		}
		if ( bbuf ) free( bbuf );
		if ( buf ) free( buf );
		if ( ntr ) delete ntr;
	}
}

#ifdef _WINDOWS64_
void JagDBServer::printResources() 
{
	abaxint totm, freem, used; //GB
	JagSystem::getMemInfo( totm, freem, used );
	int maxopen = _getmaxstdio();
	_setmaxstdio(2048);
	int maxopennew = _getmaxstdio();
    raydebug(stdout, JAG_LOG_LOW, "totrams=%l freerams=%l usedram=%l maxopenfiles=%d ==> %d\n",
				totm, freem, used, maxopen, maxopennew );
}
#else
#include <sys/resource.h>
void JagDBServer::printResources()
{
    struct rlimit rlim;
    getrlimit(RLIMIT_AS,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1107 limit virtual memory soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_CORE,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1108 limit core file soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_CPU,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1109 limit cpu time soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_DATA,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1110 limit data segment soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_FSIZE,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1111 limit size of files soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_NOFILE,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1112 limit number of fd soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_NPROC,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1113 limit number of threads soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_RSS,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1114 limit resident memory soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
    getrlimit(RLIMIT_STACK,&rlim);
    raydebug(stdout, JAG_LOG_LOW, "s1115 limit stack soft=%l hard=%l\n", rlim.rlim_cur, rlim.rlim_max);
}
#endif

// return table schema according to replicateType
// return NULL for error
JagTableSchema *JagDBServer::getTableSchema( int replicateType ) const
{
	JagTableSchema *tableschema = NULL;
	if ( replicateType == 0 ) {
		tableschema = this->_tableschema;
	} else if ( replicateType == 1 ) {
		tableschema = this->_prevtableschema;
	} else if ( replicateType == 2 ) {
		tableschema = this->_nexttableschema;
	}

	return tableschema;
}

void JagDBServer::getTableIndexSchema( int replicateType, JagTableSchema *& tableschema, JagIndexSchema *&indexschema )
{
	if ( replicateType == 0 ) {
		tableschema = this->_tableschema;
		indexschema = this->_indexschema;
	} else if ( replicateType == 1 ) {
		tableschema = this->_prevtableschema;
		indexschema = this->_previndexschema;
	} else if ( replicateType == 2 ) {
		tableschema = this->_nexttableschema;
		indexschema = this->_nextindexschema;
	} else {
		tableschema = NULL;
		indexschema = NULL;
	}
}

// object method
// msg:  "create table ..."  "update ..."  "insert ..." "delete ..."
// "alter ... "
int JagDBServer::synchToOtherDataCenters( const char *mesg, bool &sucsync, const JagRequest &req )
{
	abaxint rc = 0, cnt = 0, onedc = 0;
	sucsync = true;
	// prt(("s6380 synchToOtherDataCenters begin _numDataCenter=%d mesg=[%s]\n", _numDataCenter, mesg ));
	// need to check if mesg is insertsyncdconly, if yes, sync only to requested datacenter; otherwise, sync all
	const char *pp, *qq;
	AbaxDataString host;
    if ( 0 == strncasecmp( mesg, "insertsyncdconly|", 17 ) ) {
		pp = mesg+17;
		qq = strchr( pp, '|' );
		host = AbaxDataString( pp, qq-pp );
		pp = strchr( pp, ' ' );
		while ( *pp == ' ' ) ++pp;
		host = JagNet::getIPFromHostName( host );
		onedc = 1;
	} else {
		pp = mesg;
	}
	JagVector<int> useindex;
	
	for ( int i = 0; i < _numDataCenter; ++i ) {
		if ( ! _dataCenter[i] ) {
			// prt(("s0992 i=%d datc is NULL, skip\n", i ));
			continue;
		}
		if ( req.session->dcfrom == JAG_DATACENTER_HOST && _dataCenter[i]->datcDestType() == JAG_DATACENTER_HOST ) {
			// from data server, sync to gate, otherwise ignore
			// prt(("s1098 session->dcfrom=HOST  _dataCenter[i]->datcDest=HOST=[%s]  skip synch\n", _dataCenter[i]->getHost() ));
			continue;
		}
		if ( req.session->dcfrom == JAG_DATACENTER_GATE && _dataCenter[i]->datcDestType() == JAG_DATACENTER_GATE ) {
			// from gate, sync to data server, otherwise ignore
			// prt(("s1098 session->dcfrom=GATE  _dataCenter[i]->datcDest=GATE=[%s]  skip synch\n", _dataCenter[i]->getHost() ));
			continue;
		}
		if ( onedc && host != _dataCenter[i]->getHost() ) {
			// if send to one dc, and requested dc host is not the same as _dataCenter host, ignore
			// prt(("s2838 onedc true and host=[%s] NEQ [%s]  skip synch\n", host.c_str(), _dataCenter[i]->getHost() ));
			continue;
		}
		useindex.append( i );
		// prt(("s2338 useindex.append(%d)\n", i ));
	}
	
	if ( 0 == strncasecmp( pp, "sinsert", 7 ) && _isGate ) {
		if ( req.session->replicateType != 0 ) {
			// if not main server ( not replicate server ), do not sync
			// prt(("s6381 synchToOtherDataCenters done cnt=0\n" ));
			return 0;
		}
		++pp;
		// single insert, need to send file piece to other datacenter if needed
		// first, send cmd to all datacenter, but not reply
		for ( int i = 0; i < useindex.size(); ++i ) {
			// prt(("s7732 query(%s) host=[%s] ...\n",  pp, _dataCenter[useindex[i]]->getHost() ));
			_dataCenter[useindex[i]]->setDataCenterSync();
			rc = _dataCenter[useindex[i]]->query( pp );
			// prt(("s7733 query(%s) done rc=%d\n", pp, rc ));
			if ( 0 == rc ) {
				// prt(("s6638 sync (%s) error _dataCenter[i]->error=[%s]\n", q.c_str(), _dataCenter[i]->error() ));
				sucsync = false;
				continue;
			}
		}

		abaxint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT; ssize_t rlen = 0;
		char *newbuf = NULL, *nbuf = NULL; char hdr[JAG_SOCK_MSG_HDR_LEN+1], ebuf[JAG_ONEFILESIGNAL+1];
		while ( 1 ) {
			// recv one peice from client, send to another server, then waiting for server's reply, send back to client;
			// only repeat recv peices from client and send one by one to another server during file transfer
			memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
			// prt(("s2402 recvData ...\n" ));
			rlen = recvData( req.session->sock, hdr, newbuf );
			// prt(("s7734 recvData newbuf=[%s]\n", newbuf ));
			if ( rlen < 0 ) {
				break;
			} else if ( rlen == 0 || ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) ) {
				// invalid info for file header, ignore
				continue;
			} else if ( 0 == strncmp( newbuf, "_onefile|", 9 ) ) {
				// if _onefile recved, parse and check the outpath is valid to recv
				// prt(("s0223 _onefile newbuf=[%s]\n", newbuf ));
				JagStrSplit sp( newbuf, '|', true );
				if ( sp.length() < 4 ) {
					fsize = -1;
					prt(("E2039 error newbuf=[%s] wrong format\n", newbuf ));
				} else {
					fsize = jagatoll(sp[2].c_str());
				}
				if ( nbuf ) { free( nbuf ); nbuf = NULL; }
				nbuf = (char*)jagmalloc( rlen+1 );
				memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
				for ( int i = 0; i < useindex.size(); ++i ) {
					// prt(("s2939 sendDirectToSockAll nbuf=[%s]...\n", nbuf ));
					rc = _dataCenter[useindex[i]]->sendDirectToSockAll( nbuf, rlen );
					// prt(("s2939 sendDirectToSockAll done nbuf=[%s]\n", nbuf ));
					// rc = _dataCenter[useindex[i]]->recvDirectFromSockAll( rbuf, hdr );
					// prt(("s7737 recvData rbuf=[%s]\n", rbuf ));
				}

				// else, while to recv pieces
				totlen = 0;
				// prt(("s7474 recv data...\n" ));
				char *mbuf =(char*)jagmalloc(memsize);
				while( 1 ) {
					if ( totlen >= fsize ) break;
					if ( fsize-totlen < memsize ) {
						recvlen = fsize-totlen;
					} else {
						recvlen = memsize;
					}
					// prt(("s3039 recvData ...\n" ));
					rlen = recvData( req.session->sock, mbuf, recvlen );
					// prt(("s7739 recvData mbuf=[%s]\n", mbuf ));
					if ( rlen >= recvlen ) {
						for ( int i = 0; i < useindex.size(); ++i ) {
							// prt(("s9344 sendDirectToSockAll mbuf=[%s]\n", mbuf ));
							rlen = _dataCenter[useindex[i]]->sendDirectToSockAll( mbuf, recvlen, true );
							// prt(("s7740 sendData mbuf=[%s]\n", mbuf ));
						}		
					} else {
						break;
					}
					totlen += rlen;
				}
				if ( mbuf ) { free( mbuf ); mbuf = NULL; }
				// prt(("s7474 recv data done totlen=%d\n", totlen ));
			} else {
				if ( 0 == strncmp( newbuf, "_BEGINFILEUPLOAD_", 17 ) ) { rc = 10; }
				else if ( 0 == strncmp( newbuf, "_ENDFILEUPLOAD_", 15 ) ) { rc = 20; }
				else { rc = 30; }
				// prt(("s0384 got newbuf=[%s]\n", newbuf ));

				nbuf = (char*)jagmalloc( rlen+1 );
				memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
				for ( int i = 0; i < useindex.size(); ++i ) {
					// prt(("s7745 sendDirectToSockAll nbuf=[%s] rlen=%d ...\n", nbuf, rlen ));
					rlen = _dataCenter[useindex[i]]->sendDirectToSockAll( nbuf, rlen );
					// prt(("s7745 sendDirectToSockAll nbuf=[%s] rlen=%d done\n", nbuf, rlen ));
				}
				if ( nbuf ) { free( nbuf ); nbuf = NULL; }

				if ( 10 != rc && 20 != rc )  {
					/***
					if ( !rbuf ) {
						prt(("s2039 break\n"));
						break;
					}
					prt(("s2358 sendDirectToSock req.session->sock=%d rbuf=[%s]\n", req.session->sock, rbuf ));
					sendDirectToSock( req.session->sock, rbuf, rlen ); // 016ABCDmessage client query mode
					prt(("s7747 sendDirectToSock done rbuf=[%s]\n", rbuf ));
					***/
					prt(("s0294 ********* error\n" ));
				}

				if ( 20 == rc ) {
					// end, break while
					break;
				}
			}
		}
		if ( newbuf ) { free( newbuf ); newbuf = NULL; }
		if ( nbuf ) { free( nbuf ); nbuf = NULL; }

		for ( int i = 0; i < useindex.size(); ++i ) {
			// prt(("s7748 replyall ...\n"));
			_dataCenter[useindex[i]]->replyAll();
			// prt(("s7748 replyall\n"));

			if ( _dataCenter[useindex[i]]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex[i]], ':');
				prt(("s5129 _allSocketsBad, reconnect [%s] ...\n", sp[0].c_str() ));
				reconnectDataCenter( sp[0], false );
			}
			++cnt;
		}
	} else {
		// sinsert not gate
		// prt(("s7308 pp=[%s]\n", pp ));
		if ( 0 == strncasecmp( pp, "sinsert", 7 ) ) {
			++pp;
			if ( req.session->replicateType != 0 ) {
				// if not main server ( not replicate server, do sync
				// prt(("s6381 synchToOtherDataCenters done cnt=0\n" ));
				return 0;
			}
		}
		for ( int i = 0; i < useindex.size(); ++i ) {
			// q = AbaxDataString("[N] ") + pp; 
			// prt(("s7752 query(%s) host=[%s] ...\n",  pp, _dataCenter[useindex[i]]->getHost() ));
			_dataCenter[useindex[i]]->setDataCenterSync();
			// prt(("s7753 datc query(%s) ...\n", pp ));
			rc = _dataCenter[useindex[i]]->query( pp );
			// prt(("s7753 query(%s) done rc=%d\n", pp, rc ));
			if ( 0 == rc ) {
				// prt(("s6638 sync (%s) error _dataCenter[i]->error=[%s]\n", pp, _dataCenter[i]->error() ));
				sucsync = false;
				continue;
			}
			// prt(("s5034 sync (%s) replyAll...n", pp ));
			_dataCenter[useindex[i]]->replyAll();
			// prt(("s5035 sync (%s) query OK error=[%s]\n", pp, _dataCenter[i]->error() ));

			if ( _dataCenter[useindex[i]]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex[i]], ':');
				prt(("s5129 _allSocketsBad, reconnect [%s] ...\n", sp[0].c_str() ));
				reconnectDataCenter( sp[0], false );
				rc = _dataCenter[useindex[i]]->query( pp );
				if ( 0 == rc ) {
					sucsync = false;
					continue;
				}
				_dataCenter[useindex[i]]->replyAll();
			}

			// prt(("s4004 replyAll done\n" ));
			++ cnt;
		}
	}
	// prt(("s6381 synchToOtherDataCenters done cnt=%d\n", cnt ));

	return cnt;
}

// object method
// msg:  "select ... from ..."  "desc t1; " ... readonly
// "alter ... " "getfile ..."
// usually, synch from data center which is HOST from self GATE
int JagDBServer::synchFromOtherDataCenters( const JagRequest &req, const char *mesg, const JagParseParam &pparam )
{
	const char *pmsg;
	abaxint msglen;
	JagDataAggregate *jda = NULL;
	AbaxDataString dbobj = longToStr( THREADID ) + ".tmp";
	
	abaxint rc = 0, cnt = 0, useindex = -1, hasX1 = false;
	raydebug( stdout, JAG_LOG_HIGH, "s6380 synchFromOtherDataCenters begin _numDataCenter=%d mesg=[%s]\n", _numDataCenter, mesg );
	
	for ( int i = 0; i < _numDataCenter; ++i ) {
		if ( ! _dataCenter[i] ) continue;
		if ( _dataCenter[i]->datcDestType() != JAG_DATACENTER_HOST ) {
			// not request from self HOST, ignore
			// prt(("s1098 session->dcfrom=HOST  _dataCenter[i]->datcDest=HOST=[%s] skip synch\n", _dataCenter[i]->getHost() ));
			continue;
		}
		useindex = i;
		break;
		// only one HOST
	}

	if ( useindex  < 0 ) {
		return 0;
	}
	
	if ( pparam.getfileActualData && _isGate ) {
		// if getfile actual data download, reverse the procedure of synchToOtherDataCenters	
		 //prt(("s7732 getfileActualData && _isGate: query(%s) host=[%s] ...\n",  mesg, _dataCenter[useindex]->getHost() ));
		_dataCenter[useindex]->setDataCenterSync();
		rc = _dataCenter[useindex]->query( mesg );
		// prt(("s7733 query(%s) done rc=%d\n",  mesg, rc ));
		if ( rc > 0 ) {
			abaxint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT; ssize_t rlen = 0;
			char *newbuf = NULL, *nbuf = NULL;  char hdr[JAG_SOCK_MSG_HDR_LEN+1], ebuf[JAG_ONEFILESIGNAL+1];
			while ( 1 ) {
				// recv one peice from another HOST, send to client, then waiting for client's reply, send back to another HOST;
				// only repeat recv peices from HOST and send one by one to client during file transfer
				memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
				//prt(("s3039 _dataCenter useindex=%d recvDirectFromSockAll ....\n", useindex ));
				rlen = _dataCenter[useindex]->recvDirectFromSockAll( newbuf, hdr );
				// prt(("s6122 _dataCenter recvDirectFromSockAll hdr=[%s]  newbuf=[%s]\n", hdr, newbuf ));
				if ( rlen < 0 ) {
					// prt(("s4028 recvDirectFromSockAll rlen<0 break\n" ));
					break;
				} if ( rlen == 0 || ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) ) {
					// invalid info for file header, ignore
					continue;
				} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'X' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == '1' ) {
					// "X1" message, send to client and continue to recv another buf
					nbuf = (char*)jagmalloc( JAG_SOCK_MSG_HDR_LEN+rlen+1 );
					memcpy( nbuf, hdr, JAG_SOCK_MSG_HDR_LEN );
					memcpy( nbuf+JAG_SOCK_MSG_HDR_LEN, newbuf, rlen ); nbuf[rlen] = '\0';
					//prt(("s5003 sendDirectToSock session.sock nbuf=[%s] ren=%d\n", nbuf, rlen ));
					rc = sendDirectToSock( req.session->sock, nbuf, JAG_SOCK_MSG_HDR_LEN+rlen, true ); 
					//prt(("s5003 sendDirectToSock session.sock nbuf=[%s] done\n", nbuf ));
					hasX1 = true;
					//prt(("s3691 hasX1=true\n" ));
					if ( nbuf ) { free( nbuf ); nbuf = NULL; }
					continue;
				} else if ( 0 == strncmp( newbuf, "_onefile|", 9 ) ) {
					// if _onefile recved, parse and check the outpath is valid to recv
					//prt(("s5043 received from datac _onefile newbuf=[%s]\n", newbuf ));
					JagStrSplit sp( newbuf, '|', true );
					if ( sp.length() < 4 ) {
						fsize = -1;
					} else {
						fsize = jagatoll(sp[2].c_str());
					}
					nbuf = (char*)jagmalloc( rlen+1 );
					memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';

					//prt(("s3949 sendDirectToSock to session.sock nbuf=[%s]\n", nbuf ));
					rc = sendDirectToSock( req.session->sock, nbuf, rlen ); 
					//prt(("s3949 sendDirectToSock to session.sock nbuf=[%s] done\n", nbuf ));
					if ( nbuf ) { free( nbuf ); nbuf = NULL; }
					totlen = 0;
					char *mbuf =(char*)jagmalloc(memsize);
					while( 1 ) {
						if ( totlen >= fsize ) break;
						if ( fsize-totlen < memsize ) {
							recvlen = fsize-totlen;
						} else {
							recvlen = memsize;
						}
						rlen = _dataCenter[useindex]->recvDirectFromSockAll( mbuf, recvlen );
						if ( rlen >= recvlen ) {
							rlen = sendDirectToSock( req.session->sock, mbuf, recvlen, true ); 
						} else {
							break;
						}
						totlen += rlen;
					}
					if ( mbuf ) { free( mbuf ); mbuf = NULL; }
					//prt(("s6291 reved data done totlen=%d\n", totlen ));
				} else {
					if ( 0 == strncmp( newbuf, "_BEGINFILEUPLOAD_", 17 ) ) { rc = 10; }
					else if ( 0 == strncmp( newbuf, "_ENDFILEUPLOAD_", 15 ) ) { rc = 20; }
					else { rc = 30; }
					//prt(("s2839 recved newbuf=[%s]\n", newbuf ));
					nbuf = (char*)jagmalloc( rlen+1 );
					memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
					//prt(("s5892 sendDirectToSock nbuf=[%s] rlen=%d\n", nbuf, rlen ));
					rlen = sendDirectToSock( req.session->sock, nbuf, rlen ); 
					//prt(("s5892 sendDirectToSock nbuf=[%s] rlen=%d done\n", nbuf, rlen ));
					if ( nbuf ) { free( nbuf ); nbuf = NULL; }
					if ( 10 != rc && 20 != rc ) {
						/***
						rlen = recvDirectFromSock( req.session->sock, rbuf, hdr );
						if ( rbuf ) _dataCenter[useindex]->sendDirectToSockAll( rbuf, rlen );
						***/
						/***
						prt(("s0938************ unexpected hdr=[%s] newbuf=[%s]\n", hdr, newbuf ));
						if  ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'E' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'D' &&
						      0==strncmp(newbuf, "_END", 4 ) ) {
								// JagTable::sendMessage( req, nbuf, "ED" );
							  // continue;
							  // break;
					    }
						***/
					}

					if ( 20 == rc ) {
						// end, break while
						// prt(("s5233 got _ENDFILEUPLOAD_ break\n" ));
						break;
					}
				}
			}
			if ( newbuf ) { free( newbuf ); newbuf = NULL; }
			if ( nbuf ) { free( nbuf ); nbuf = NULL; }

			// prt(("s7383 _dataCenter replyAll...\n"));
			_dataCenter[useindex]->replyAll();
			// prt(("s7383 _dataCenter replyAll done\n"));

			if ( _dataCenter[useindex]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex], ':');
				prt(("s5129 _allSocketsBad, reconnect [%s] ...\n", sp[0].c_str() ));
				reconnectDataCenter( sp[0], false );
			}
			++cnt;
		}
	} else {
		// select command
		// prt(("s7755 notgetfiledata or not isgate: query(%s) host=[%s] ...\n", mesg, _dataCenter[useindex]->getHost() ));
		_dataCenter[useindex]->setDataCenterSync();
		_dataCenter[useindex]->setRedirectSock( (void*)&req.session->sock );
		rc = _dataCenter[useindex]->query( mesg );
		// prt(("s7733 query(%s) done rc=%d\n",  mesg, rc ));
		if ( rc > 0 ) {
			while ( _dataCenter[useindex]->reply() ) {
				// must be X1, or select count(*) result
				pmsg = _dataCenter[useindex]->getMessage();
				msglen = _dataCenter[useindex]->getMessageLen();
				//JagTable::sendMessageLength( req, pmsg, msglen, "OK" );
				if ( pparam.opcode == JAG_COUNT_OP ) {
					pmsg = strcasestrskipquote( pmsg, " contains " );
					AbaxDataString cntstr = longToStr(jagatoll(pmsg+10));
					// prt(("sendback to origclient select count() [%s] ...\n", cntstr.c_str() ));
					JagTable::sendMessageLength( req, cntstr.c_str(), cntstr.size(), "OK" );
				} else {
					// prt(("sendback to origclient select point query [%s] ...\n", pmsg ));
					if ( !hasX1 && pmsg ) {
						JagTable::sendMessageLength( req, pmsg, msglen, "X1" );
						hasX1 = true;
						// prt(("s7028 hasX1=true pmsg=[%s]\n", pmsg ));
					}
				}
			}

			if ( _dataCenter[useindex]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex], ':');
				prt(("s5529 _allSocketsBad, reconnect [%s] ...\n", sp[0].c_str() ));
				reconnectDataCenter( sp[0], false );
				_dataCenter[useindex]->setRedirectSock( (void*)&req.session->sock );
				rc = _dataCenter[useindex]->query( mesg );
				if ( rc > 0 ) {
					while ( _dataCenter[useindex]->reply() ) {
						// must be X1
						pmsg = _dataCenter[useindex]->getMessage();
						msglen = _dataCenter[useindex]->getMessageLen();
						// prt(("sendbak to origclient [%s] ...\n", pmsg ));
						if ( !hasX1 && pmsg ) {
							// "select * from tab export" gives NULL pmsg
							JagTable::sendMessageLength( req, pmsg, msglen, "X1" );
							hasX1 = true;
							// prt(("s5028 hasX1=true pmsg=[%s]\n", pmsg ));
						}
					}
				}
			}
		}
		// prt(("s4004 replyAll done\n" ));
		++cnt;
	}

	// prt(("s6381 synchFromOtherDataCenters done  cnt=%d\n", cnt ));
	if ( hasX1 ) cnt = -111;
	// prt(("s6382 synchFromOtherDataCenters done  cnt=%d\n", cnt ));
	return cnt;
}

void JagDBServer::openDataCenterConnection()
{
	AbaxDataString fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
	    _numDataCenter = 0;
		return;
	}
	int trynum;
	int  MAXTRY = 12;

	AbaxDataString adminpass = "dummy";
	JagVector<AbaxDataString> vec;
	JagVector<AbaxDataString> vecip;
	AbaxDataString ip;
	// JagNet::getLocalIPs( vec );
	// _dbConnector->_nodeMgr->_allNodes "ip1|ip2|ip3" of current cluster
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );

		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.length() > 0 ) {
			vecip.append( ip );
		}
	}

	AbaxDataString uphost, line, line2, seeds, host, port, destType;
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	int rc = 0;
	int stype, dtype;
	// AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + AbaxDataString("/TOKEN=") + _servToken;
	JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	_numDataCenter = 0;
	for ( int i = 0; i < JAG_DATACENTER_MAX; ++i ) {
		_dataCenter[i] = NULL;
	}
	// prt(("s2939 unixSocket=[%s]\n", unixSocket.c_str() ));

	for ( int i =0; i < sp.length() && i < JAG_DATACENTER_MAX; ++i ) {
		if ( strchr( sp[i].c_str(), '#' ) ) continue;
		line = trimTailChar( sp[i], '\r' );
		line2 = trimTailChar( line, ' ' );
		if ( line2.size()< 2 ) continue;
		uphost = makeUpperString( line2 );
		getDestHostPortType( uphost, host, port, destType );
		if ( vec.exist( host ) ) continue;
		if ( vec.length() > 0 && (vec.length() == vecip.length() ) ) {
			ip = JagNet::getIPFromHostName( host );
			if ( ip.size() > 3 && vecip.exist( ip ) ) continue;
		}

		raydebug( stdout, JAG_LOG_LOW, "Connecting to datacenter [%s]\n", host.c_str() );
		_centerHostPort[_numDataCenter] = uphost;
		// prt(("s9393 uphost=[%s]\n", uphost.c_str() ));
		//_dataCenter[_numDataCenter] = new JaguarAPI();
		_dataCenter[_numDataCenter] = newObject<JaguarAPI>();
		if ( JAG_LOG_LEVEL == JAG_LOG_HIGH ) {
			prt(("s4807 datac setDebug(true)\n" ));
			_dataCenter[_numDataCenter]->setDebug( true );
		}
		if ( destType == "GATE" ) { dtype = JAG_DATACENTER_GATE; } 
		else if ( destType == "PGATE" ) { dtype = JAG_DATACENTER_PGATE; }
		else { dtype = JAG_DATACENTER_HOST; }
		if ( _isGate ) { stype = JAG_DATACENTER_GATE; } else { stype = JAG_DATACENTER_HOST; }
		// prt(("s9494 isGate=%d dt=[%s] stype=%d dtype=%d\n", _isGate, destType.c_str(), stype, dtype));
		
		_dataCenter[_numDataCenter]->setDatcType( stype, dtype );
		AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + AbaxDataString("/TOKEN=") + _servToken;
		unixSocket += srcDestType( _isGate, destType );
		rc = 0;
		trynum = 0;
		while ( ! rc ) {
			if ( 0 == rc && trynum > MAXTRY ) { break; }
			rc = _dataCenter[_numDataCenter]->connect( host.c_str(), atoi( port.c_str() ), 
							  "admin", adminpass.c_str(), "test", unixSocket.c_str() );
			if ( 0 == rc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
					uphost.c_str(),  _dataCenter[_numDataCenter]->error(), trynum, MAXTRY );
				sleep(10);
			}
			++trynum;
		}

		if ( 0 == rc && trynum > MAXTRY ) {
			delete ( _dataCenter[_numDataCenter] );
			_dataCenter[_numDataCenter] = NULL;
			raydebug( stdout, JAG_LOG_LOW, "failed connect to datacenter=%s _numDataCenter=%d\n", host.c_str(), _numDataCenter );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "connected to datacenter=%s _numDataCenter=%d\n", host.c_str(), _numDataCenter );
		}
		_numDataCenter++;
	}
	JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;

}

void JagDBServer::closeDataCenterConnection()
{
	JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	for ( int i = 0; i < _numDataCenter; ++i ) {
		if ( _dataCenter[i] ) {
			_dataCenter[i]->close();
			delete  _dataCenter[i];
			_dataCenter[i] = NULL;
		}
	}
	JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
}

long JagDBServer::getDataCenterQueryCount( const AbaxDataString &ip, bool doLock )
{
	if ( ip.size() < 1 ) return 0;

	if ( doLock ) {
		JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	}
	long cnt = 0;
	AbaxDataString host, port, destType;
	prt(("s8383 _numDataCenter=%d\n", _numDataCenter ));
	for ( int i = 0; i < _numDataCenter; ++i ) {
		getDestHostPortType( _centerHostPort[i], host, port, destType );
		prt(("s9393 _centerHostPort[%d]=[%s] host=[%s] port=[%s]\n", i,  _centerHostPort[i].c_str(), host.c_str(), port.c_str() ));
		if ( ip != host ) {
			continue; 
		}

		if ( ! _dataCenter[i] ) {
			break;
		}

		cnt = _dataCenter[i]->getQueryCount();
		break;
	}

	prt(("s939339 \n" ));
	if ( doLock ) {
		JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
	}

	prt(("s939339 cnt=%d\n", cnt ));
	return cnt;
}


// reconnect to one data center ip : is one of peerip from peer datacenter
// is from datacenter.conf one line host
void JagDBServer::reconnectDataCenter( const AbaxDataString &ip, bool doLock )
{
	int rc = 0;
	int stype, dtype, trynum;
	int  MAXTRY = 12;
	AbaxDataString host, port, destType;
	AbaxDataString adminpass = "dummy";
	if ( doLock ) {
		JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	}

	for ( int i = 0; i < _numDataCenter; ++i ) {
		getDestHostPortType( _centerHostPort[i], host, port, destType );
		if ( ip != host ) {
			continue; 
		}

		if ( _dataCenter[i] ) {
			_dataCenter[i]->close();
			delete  _dataCenter[i];
		}
		//_dataCenter[i] = new JaguarAPI();
		_dataCenter[i] = newObject<JaguarAPI>();
		if ( JAG_LOG_LEVEL == JAG_LOG_HIGH ) {
			prt(("s4021 datc setDebug(true)\n" ));
			 _dataCenter[i]->setDebug( true );
		}
		if ( destType == "GATE" ) { dtype = JAG_DATACENTER_GATE; } 
		else if ( destType == "PGATE" ) { dtype = JAG_DATACENTER_PGATE; }
		else { dtype = JAG_DATACENTER_HOST; }

		if ( _isGate ) { stype = JAG_DATACENTER_GATE; } else { stype = JAG_DATACENTER_HOST; }
		_dataCenter[i]->setDatcType( stype, dtype );
		AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + AbaxDataString("/TOKEN=") + _servToken;
		unixSocket += srcDestType( _isGate, destType );
		rc = 0;
		trynum = 0;
		while ( ! rc ) {
			if ( 0 == rc && trynum > MAXTRY ) { break; }
			// prt(("s5671 _dataCenter[i]->connect(%s) ...\n", host.c_str() ));
			rc = _dataCenter[i]->connect( host.c_str(), atoi( port.c_str() ), "admin", adminpass.c_str(), "test", unixSocket.c_str() );
			if ( 0 == rc ) {
				raydebug( stdout, JAG_LOG_LOW, 
						  "Error connect to datacenter [%s], %s, retry %d/%d ...\n", host.c_str(), _dataCenter[i]->error(), trynum, MAXTRY );
				sleep(10);
			}
			++trynum;
		}

		if ( 0==rc && trynum > MAXTRY ) {
			raydebug( stdout, JAG_LOG_LOW, "Reconnect failed to datacenter [%s] after %d retries\n", ip.c_str(), MAXTRY );
			if ( _dataCenter[i] ) {
				delete  _dataCenter[i];
				_dataCenter[i] = NULL;
			}
		} else {
			raydebug( stdout, JAG_LOG_LOW, "Reconnected to datacenter [%s]\n", ip.c_str() );
		}

	}

	if ( doLock ) {
		JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
	}
}

int JagDBServer::countOtherDataCenters()
{
	AbaxDataString fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
		return 0;
	}

	JagVector<AbaxDataString> vec;
	JagVector<AbaxDataString> vecip;
	AbaxDataString ip;
	// prt(("s2039 countOtherDataCenters _allNodes=[%s]\n", _dbConnector->_nodeMgr->_allNodes.c_str() ));
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );

		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.size() > 4 ) {
			vecip.append( ip ); 
		}
	}

	AbaxDataString seeds, hostport, upphost;
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	int cnt = 0;
	for ( int i =0; i < sp.length() && i < JAG_DATACENTER_MAX; ++i ) {
		// prt(("s2373 %s\n", sp[i].c_str() ));
		if ( strchr( sp[i].c_str(), '#' ) ) continue;
		hostport = trimChar( sp[i], ' ' );
		JagStrSplit hp( hostport, ':' );
		upphost = makeUpperString( hp[0] );
		ip = JagNet::getIPFromHostName( hp[0] );
		if ( upphost.size()< 2 ) continue;
		if ( vec.exist( upphost ) ) continue;
		if ( ip.size() > 2 && vec.length() == vecip.length() ) {
			if ( vecip.exist( ip ) ) continue;
		}

		++cnt;
		// prt(("s2382 cnt=%d\n", cnt ));
	}

	return cnt;
}

void JagDBServer::refreshDataCenterConnections( abaxint seq )
{
	int rc;
	int newCnt = countOtherDataCenters();
	AbaxDataString h, p, destType;
	AbaxDataString adminpass = "dummy";
	if ( newCnt == _numDataCenter ) {
		// prt(("s2396 refreshDataCenterConnections newCnt=%d _numDataCenter=%d same, noop\n", newCnt, _numDataCenter ));
		if ( ( seq % 5 ) == 0 ) {
			for ( int i = 0; i < _numDataCenter; ++i ) {
				if ( _dataCenter[i] ) continue;
				//_dataCenter[i] = new JaguarAPI();
				_dataCenter[i] = newObject<JaguarAPI>();
				if ( JAG_LOG_LEVEL == JAG_LOG_HIGH ) {
					prt(("s2066 datc setDebug(true) \n" ));
				 	_dataCenter[i]->setDebug( true );
				 }
				getDestHostPortType( _centerHostPort[i], h, p, destType );
				AbaxDataString unixSocket = AbaxDataString("/DATACENTER=1") + AbaxDataString("/TOKEN=") + _servToken;
				unixSocket += srcDestType( _isGate, destType );
				rc = _dataCenter[i]->connect( h.c_str(), atoi( p.c_str() ), 
					  "admin", adminpass.c_str(), "test", unixSocket.c_str() );
				if ( ! rc ) {
					delete _dataCenter[i];
					_dataCenter[i] = NULL;
					raydebug( stdout, JAG_LOG_LOW, "E2865 reconnect datacenter [%s] error\n", _centerHostPort[i].c_str() );
				} else {
					raydebug( stdout, JAG_LOG_LOW, "reconnect datacenter [%s] OK\n", _centerHostPort[i].c_str() );
				}
			}
		}

		return;
	}

	raydebug( stdout, JAG_LOG_LOW, "existingdatacenter=%d  newdatacenter=%d, refresh connections...\n", _numDataCenter, newCnt );
	closeDataCenterConnection();
	openDataCenterConnection();
}

// object method
void JagDBServer::checkAndCreateThreadGroups()
{
	#ifdef DEVDEBUG
	raydebug( stdout, JAG_LOG_HIGH, "s5328 activeClients=%l activeThreads=%l\n", 
			  (abaxint)_activeClients, (abaxint)_activeThreadGroups );
    #endif

	abaxint percent = 70;
	if ( abaxint(_activeClients) * 100 >= percent * (abaxint)_activeThreadGroups ) {
		raydebug( stdout, JAG_LOG_LOW, "s3380 create new threads activeClients=%l activeGrps=%l makeThreadGroups(%l) ...\n",
				(abaxint) _activeClients, (abaxint)_activeThreadGroups, _threadGroupSeq );
		makeThreadGroups( _threadGroups, _threadGroupSeq++ );
	}
}

// line: "ip:port:HOST|ip:host:GATE|ip:port|ip"
// return host, port, type parts
void JagDBServer::getDestHostPortType( const AbaxDataString &inLine, 
			AbaxDataString& host, AbaxDataString& port, AbaxDataString& destType )
{
	AbaxDataString line = trimTailChar( inLine, '\r' );
	JagStrSplit sp(line, '|', true );
	int len = sp.length();
	int i = rand() % len;
	AbaxDataString seg = sp[i];
	JagStrSplit sp2( seg, ':', true );
	if ( sp2.length() > 2 ) {
		host = sp2[0];
		port = sp2[1];
		destType = sp2[2];
	} else if ( sp2.length() > 1 ) {
		host = sp2[0];
		port = sp2[1];
		destType = "HOST";
	} else {
		host = sp2[0];
		port = "8888";
		destType = "HOST";
	}
}

AbaxDataString JagDBServer::srcDestType( int isGate, const AbaxDataString &destType )
{
	AbaxDataString conn = "/";
	if ( isGate ) {
		conn += "DATC_FROM=GATE";
	} else {
		conn += "DATC_FROM=HOST";
	}

	conn += "/";
	if ( destType == "GATE" ) {
		conn += "DATC_TO=GATE";
	} else if ( destType == "PGATE" ) {
		conn += "DATC_TO=PGATE";
	} else {
		conn += "DATC_TO=HOST";
	}

	return conn;
}

void JagDBServer::refreshUserDB( abaxint seq )
{
	if ( ( seq % 10 ) != 0 ) return;

	if ( _userDB ) {
		_userDB->refresh();
	}

	if ( _prevuserDB ) {
		_prevuserDB->refresh();
	}

	if ( _nextuserDB ) {
		_nextuserDB->refresh();
	}
}

void JagDBServer::refreshUserRole( abaxint seq )
{
	if ( ( seq % 10 ) != 0 ) return;

	if ( _userRole ) {
		_userRole->refresh();
	}

	if ( _prevuserRole ) {
		_prevuserRole->refresh();
	}

	if ( _nextuserRole ) {
		_nextuserRole->refresh();
	}
}


// methods will affect userrole or schema
// static  
void JagDBServer::grantPerm( JagRequest &req, JagDBServer *servobj, 
	const JagParseParam &parseParam, abaxint threadQueryTime )
{
	// prt(("s3722 grantPerm obj=[%s] ...\n", parseParam.grantObj.c_str() ));
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString uid  = parseParam.grantUser;
	JagStrSplit sp( parseParam.grantObj, '.' );
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = servobj->_userRole;
	else if ( req.session->replicateType == 1 ) uidrole = servobj->_prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = servobj->_nextuserRole;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		 prt(("s5002 grantPerm return\n"));
	    return;
	}

	// prt(("s4812 schemaChangeCommandSyncCheck done \n" ));
	servobj->_objectLock->writeLockSchema( req.session->replicateType );
	if ( sp.length() == 3 ) {
		cond[0] = 'O'; cond[1] = 'K';
		if ( sp[0] != "*" ) {
			// db.tab.col  check DB
			if ( ! servobj->dbExist( sp[0], req.session->replicateType ) ) {
				cond[0] = 'N'; cond[1] = 'G';
				// prt(("s0391 db=[%s] does not exist\n", sp[0].c_str() ));
			} else if (  sp[1] != "*" ) {
				if ( ! servobj->objExist( sp[0], sp[1], req.session->replicateType ) ) {
					cond[0] = 'N'; cond[1] = 'G';
					// prt(("s4123 obj=[%s] ot found\n", sp[1].c_str() )); 
				}
			}
		}
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;

	// check if uid exists
	if ( uiddb ) {
		if ( ! uiddb->exist( uid ) ) {
			cond[0] = 'N'; cond[1] = 'G';
			// prt(("s0394 user does not exist\n" ));
		} 
	} else {
		cond[0] = 'N'; cond[1] = 'G';
	}


	if ( 0 == req.session->drecoverConn ) {
		// prt(("s2220 sending cond=[%s] SS ..\n", cond ));
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			prt(("s3928 return\n"));
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			// prt(("s6834 return\n"));
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	if ( uidrole ) {
		uidrole->addRole( uid, sp[0], sp[1], sp[2], parseParam.grantPerm, parseParam.grantWhere );
	} else {
		// prt(("s5835 no addrole\n"));
	}
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	servobj->schemaChangeCommandSyncRemove( scdbobj ); 
}

// static  
void JagDBServer::revokePerm( JagRequest &req, JagDBServer *servobj, 
	const JagParseParam &parseParam, abaxint threadQueryTime )
{
	abaxint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	AbaxDataString uid = parseParam.grantUser;
	JagStrSplit sp( parseParam.grantObj, '.' );
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = servobj->_userRole;
	else if ( req.session->replicateType == 1 ) uidrole = servobj->_prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = servobj->_nextuserRole;
	AbaxDataString scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		prt(("s3108 revokePerm return\n" ));
		return;
	}
	servobj->_objectLock->writeLockSchema( req.session->replicateType );
	if ( sp.length() == 3 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = servobj->_userDB;
	else if ( req.session->replicateType == 1 ) uiddb = servobj->_prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = servobj->_nextuserDB;

	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	bool rc = false;
	if ( uiddb ) {
		rc = uiddb->exist( uid );
	}
	// prt(("s3123 exist rc=%d\n", rc ));
	jaguar_mutex_unlock ( &g_dbmutex );
	if ( ! rc ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( JagTable::sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_MSG_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvData( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
			servobj->schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	
	JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	rc = false;
	if ( uidrole ) {
		rc = uidrole->dropRole( uid, sp[0], sp[1], sp[2], parseParam.grantPerm );
	}
	// prt(("s3123 dropRole rc=%d\n", rc ));
	jaguar_mutex_unlock ( &g_dbmutex );
	servobj->_objectLock->writeUnlockSchema( req.session->replicateType );
	servobj->schemaChangeCommandSyncRemove( scdbobj );
}

// method read affect userrole or schema
// static  
void JagDBServer::showPerm( JagRequest &req, JagDBServer *servobj, 
	const JagParseParam &parseParam, abaxint threadQueryTime )
{
	//prt(("s3722 showPerm ...\n" ));
	AbaxDataString uid  = parseParam.grantUser;
	if ( uid.size() < 1 ) {
		uid = req.session->uid;
	}

	if ( req.session->uid != "admin" && uid != req.session->uid ) {
		AbaxDataString err = AbaxDataString("E5082 error show grants for user ") + uid;
		JagTable::sendMessageLength( req, err.c_str(), err.length(), "OK" );
		return;
	}

	//prt(("s4423 showPerm uid=[%s]\n", uid.c_str() ));

	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = servobj->_userRole;
	else if ( req.session->replicateType == 1 ) uidrole = servobj->_prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = servobj->_nextuserRole;


	// JAG_BLURT jaguar_mutex_lock ( &g_dbmutex ); JAG_OVER;
	AbaxDataString perms;
	if ( uidrole ) {
		perms = uidrole->showRole( uid );
	} else {
		perms = AbaxDataString("E4024 error show grants for ") + uid;
	}

	// prt(("s2473 perms=[%s] send to cleint\n", perms.c_str() ));
	JagTable::sendMessageLength( req, perms.c_str(), perms.length(), "DS" );
	// jaguar_mutex_unlock ( &g_dbmutex );
}

bool JagDBServer::checkUserCommandPermission( JagDBServer *servobj, const JagSchemaRecord *srec, const JagRequest &req, 
	const JagParseParam &parseParam, int i, AbaxDataString &rowFilter, AbaxDataString &reterr )
{
	// For admin user, all true
	if ( req.session->uid == "admin" ) return true;
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = servobj->_userRole;
	else if ( req.session->replicateType == 1 ) uidrole = servobj->_prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = servobj->_nextuserRole;
	return uidrole->checkUserCommandPermission( servobj, srec, req, parseParam, i, rowFilter, reterr );
}

// object method
// if dbExist  
bool JagDBServer::dbExist( const AbaxDataString &dbName, int replicateType )
{
	AbaxDataString jagdatahome = _cfg->getJDBDataHOME( replicateType );
    AbaxDataString sysdir = jagdatahome + "/" + dbName;
	if ( JagFileMgr::isDir( sysdir ) > 0 ) {
		return true;
	} else {
		return false;
	}
}

// obj: table name or index name
bool JagDBServer::objExist( const AbaxDataString &dbname, const AbaxDataString &objName, int replicateType )
{
	JagTableSchema *tableschema = NULL;
	JagIndexSchema *indexschema = NULL;
	bool rc;
	getTableIndexSchema( replicateType, tableschema, indexschema );
	if (  tableschema ) {
		rc = tableschema->dbTableExist( dbname, objName );
		if ( rc ) return true;
	}

	if (  indexschema ) {
		rc = indexschema->indexExist( dbname, objName );
		if ( rc ) return true;
	}

	return false;

}

AbaxDataString JagDBServer::fillDescBuf( const JagSchema *schema, const JagColumn &column, 
										 const AbaxDataString &dbtable ) const
{
	char buf[512];
	AbaxDataString type = column.type;
	int  offset = column.length;
	int  len = column.length;
	int  sig = column.sig;
	int  srid = column.srid;
	AbaxDataString dbcol = dbtable + "." + column.name.c_str();
	//prt(("s5041 fillDescBuf type=[%s]\n", type.c_str() ));

	AbaxDataString res;
		if ( type == JAG_C_COL_TYPE_DBOOLEAN ) {
			sprintf( buf, "boolean" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DBIT ) {
			sprintf( buf, "bit" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DINT ) {
			sprintf( buf, "int" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DTINYINT ) {
			sprintf( buf, "tinyint" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DSMALLINT ) {
			sprintf( buf, "smallint" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DMEDINT ) {
			sprintf( buf, "mediumint" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DBIGINT ) {
			sprintf( buf, "bigint" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_FLOAT ) {
			sprintf( buf, "float(%d.%d)", len-2, sig );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DOUBLE ) {
			sprintf( buf, "double(%d.%d)", len-2, sig );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DATETIME ) {
			sprintf( buf, "datetime" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
			sprintf( buf, "timestamp" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			sprintf( buf, "time" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
			sprintf( buf, "datetimenano" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
			sprintf( buf, "timenano" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_DATE ) {
			sprintf( buf, "date" );
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_POINT ) {
			if ( srid > 0 ) {
				sprintf( buf, "point(srid:%d)", srid );
			} else {
				sprintf( buf, "point" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_POINT3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "point3d(srid:%d)", srid );
			} else {
				sprintf( buf, "point3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE ) {
			if ( srid > 0 ) {
				sprintf( buf, "circle(srid:%d)", srid );
			} else {
				sprintf( buf, "circle" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "circle3d(srid:%d)", srid );
			} else {
				sprintf( buf, "circle3d", srid );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE ) {
			if ( srid > 0 ) {
				sprintf( buf, "triangle(srid:%d)", srid );
			} else {
				sprintf( buf, "triangle" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "triangle3d(srid:%d)", srid );
			} else {
				sprintf( buf, "triangle3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_SPHERE ) {
			if ( srid > 0 ) {
				sprintf( buf, "sphere(srid:%d)", srid );
			} else {
				sprintf( buf, "sphere" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_SQUARE ) {
			if ( srid > 0 ) {
				sprintf( buf, "square(srid:%d)", srid );
			} else {
				sprintf( buf, "square" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_SQUARE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "square3d(srid:%d)", srid );
			} else {
				sprintf( buf, "square3d)" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_CUBE ) {
			if ( srid > 0 ) {
				sprintf( buf, "cube(srid:%d)", srid );
			} else {
				sprintf( buf, "cube" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE ) {
			if ( srid > 0 ) {
				sprintf( buf, "rectangle(srid:%d)", srid );
			} else {
				sprintf( buf, "rectangle" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "rectangle3d(srid:%d)", srid );
			} else {
				sprintf( buf, "rectangle3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_BOX ) {
			if ( srid > 0 ) {
				sprintf( buf, "box(srid:%d)", srid );
			} else {
				sprintf( buf, "box" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_CYLINDER ) {
			if ( srid > 0 ) {
				sprintf( buf, "cylinder(srid:%d)", srid );
			} else {
				sprintf( buf, "cylinder" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_CONE ) {
			if ( srid > 0 ) {
				sprintf( buf, "cone(srid:%d)", srid );
			} else {
				sprintf( buf, "cone" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_LINE ) {
			if ( srid > 0 ) {
				sprintf( buf, "line(srid:%d)", srid );
			} else {
				sprintf( buf, "line" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_LINE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "line3d(srid:%d)", srid );
			} else {
				sprintf( buf, "line3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING ) {
			if ( srid > 0 ) {
				sprintf( buf, "linestring(srid:%d)", srid );
			} else {
				sprintf( buf, "linestring" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "linestring3d(srid:%d)", srid );
			} else {
				sprintf( buf, "linestring3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT ) {
			if ( srid > 0 ) {
				sprintf( buf, "multipoint(srid:%d)", srid );
			} else {
				sprintf( buf, "multipoint" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "multipoint3d(srid:%d)", srid );
			} else {
				sprintf( buf, "multipoint3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING ) {
			if ( srid > 0 ) {
				sprintf( buf, "multilinestring(srid:%d)", srid );
			} else {
				sprintf( buf, "linestring" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "multilinestring3d(srid:%d)", srid );
			} else {
				sprintf( buf, "multilinestring3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_POLYGON ) {
			if ( srid > 0 ) {
				sprintf( buf, "polygon(srid:%d)", srid );
			} else {
				sprintf( buf, "polygon" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_POLYGON3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "polygon3d(srid:%d)", srid );
			} else {
				sprintf( buf, "polygon3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			if ( srid > 0 ) {
				sprintf( buf, "multipolygon(srid:%d)", srid );
			} else {
				sprintf( buf, "multipolygon" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "multipolygon3d(srid:%d)", srid );
			} else {
				sprintf( buf, "multipolygon3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE ) {
			if ( srid > 0 ) {
				sprintf( buf, "ellipse(srid:%d)", srid );
			} else {
				sprintf( buf, "ellipse" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE3D ) {
			if ( srid > 0 ) {
				sprintf( buf, "ellipse3d(srid:%d)", srid );
			} else {
				sprintf( buf, "ellipse3d" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSOID ) {
			if ( srid > 0 ) {
				sprintf( buf, "ellipsoid(srid:%d)", srid );
			} else {
				sprintf( buf, "ellipsoid" );
			}
			res += buf;
		} else if ( type == JAG_C_COL_TYPE_RANGE ) {
			if ( srid > 0 ) {
				AbaxDataString s = JagParser::getFieldTypeString( srid );
				sprintf( buf, "range(%s)", s.c_str() );
			} else {
				sprintf( buf, "range" );
			}
			res += buf;
		} else if ( column.spare[1] == JAG_C_COL_TYPE_UUID_CHAR ) {
			sprintf( buf, "uuid" );
			res += buf;
		} else if ( column.spare[1] == JAG_C_COL_TYPE_FILE_CHAR ) {
			sprintf( buf, "file" );
			res += buf;
		} else if ( column.spare[1] == JAG_C_COL_TYPE_ENUM_CHAR ) {
			AbaxDataString defvalStr;
			AbaxDataString enumstr =  "enum(";
			schema->getAttrDefVal( dbcol, defvalStr );
			JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
			for ( int k = 0; k < sp.length()-1; ++k ) {
				if ( 0 == k ) {
					enumstr += sp[k];
				} else {
					enumstr += AbaxDataString(",") + sp[k];
				}
			}
			if ( *(column.spare+4) == JAG_CREATE_DEFINSERTVALUE ) {
				enumstr += ")";
			} else {
				enumstr += AbaxDataString(",") + sp[sp.length()-1] + ")";;
			}
			res += enumstr;
		} else if ( type == JAG_C_COL_TYPE_STR ) {
			sprintf( buf, "char(%d)", len );
			res += buf;
		} else {
			sprintf( buf, "[%s] Error", type.c_str() );
			res += buf;
		}

	return res;
}
