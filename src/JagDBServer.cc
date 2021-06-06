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

#undef JAG_CLIENT_SIDE
#define JAG_SERVER_SIDE 1

#include <JagDef.h>
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
#include <JagFSMgr.h>
#include <JagFastCompress.h>
#include <JagTime.h>
#include <JagHashLock.h>
#include <JagServerObjectLock.h>
#include <JagDataAggregate.h>
#include <JagUUID.h>
#include <JagTableUtil.h>
#include <JagProduct.h>
#include <JagTable.h>
#include <JagIndex.h>
#include <JagBoundFile.h>
#include <JagDBConnector.h>
#include <JagDiskArrayBase.h>
#include <JagIPACL.h>
#include <JagSingleMergeReader.h>
#include <JagDBLogger.h>
#include <JaguarAPI.h>
#include <JagParser.h>
#include <JagLineFile.h>
#include <JagCrypt.h>
#include <JagBlockLock.h>
#include <JagFamilyKeyChecker.h>


extern int JAG_LOG_LEVEL;
int JagDBServer::g_receivedSignal = 0;
jagint JagDBServer::g_lastSchemaTime = 0;
jagint JagDBServer::g_lastHostTime = 0;

pthread_mutex_t JagDBServer::g_dbschemamutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_flagmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_wallogmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_dlogmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_dinsertmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t JagDBServer::g_datacentermutex = PTHREAD_MUTEX_INITIALIZER;

JagDBServer::JagDBServer() 
{
	Jstr fpath = jaguarHome() + "/conf/host.conf";
	Jstr fpathnew = jaguarHome() + "/conf/cluster.conf";
	if ( JagFileMgr::exist( fpath ) && ( ! JagFileMgr::exist( fpathnew ) )  ) {
		jagrename( fpath.c_str(), fpathnew.c_str() );
	}

	_allowIPList = _blockIPList = NULL;
	_serverReady = false;

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
	
	jdfsMgr = newObject<JagFSMgr>();
	servtimediff = JagTime::getTimeZoneDiff();

	_cfg = newObject<JagCfg>();
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

	_dinsertCommandFile = NULL;
	_delPrevOriCommandFile = NULL;
	_delPrevRepCommandFile = NULL;
	_delPrevOriRepCommandFile = NULL;
	_delNextOriCommandFile = NULL;
	_delNextRepCommandFile = NULL;
	_delNextOriRepCommandFile = NULL;
	_recoveryRegCommandFile = NULL;
	_recoverySpCommandFile = NULL;

	_objectLock = new JagServerObjectLock( this );
	_jagUUID = new JagUUID();
	pthread_rwlock_init(&_aclrwlock, NULL);

	_internalHostNum = new JagHashMap<AbaxString, jagint>();

	Jstr dblist, dbpath;
	dbpath = _cfg->getJDBDataHOME( JAG_MAIN );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, JAG_MAIN );

	dbpath = _cfg->getJDBDataHOME( JAG_PREV );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, JAG_PREV );

	dbpath = _cfg->getJDBDataHOME( JAG_NEXT );
	dblist = JagFileMgr::listObjects( dbpath );
	_objectLock->setInitDatabases( dblist, JAG_NEXT );

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
	_faultToleranceCopy = _cfg->getIntValue("REPLICATION", 1);
	if ( 0==strcasecmp(PRODUCT_VERSION, "FREETRIAL" ) ) {
		_faultToleranceCopy = 1;
	}

	if ( _faultToleranceCopy < 1 ) _faultToleranceCopy = 1;
	else if ( _faultToleranceCopy > 3 ) _faultToleranceCopy = 3;

	loadACL(); 

	_jdbMonitorTimedoutPeriod = _cfg->getLongValue("JDB_MONITOR_TIMEDOUT", 600);
	raydebug( stdout, JAG_LOG_LOW, "JdbMonitorTimedoutPeriod = [%d]\n", _jdbMonitorTimedoutPeriod );

	_localInternalIP = _dbConnector->_nodeMgr->_selfIP;
	if ( _localInternalIP.size() > 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "Host IP = [%s]\n", _localInternalIP.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "Servers [%s]\n", _dbConnector->_nodeMgr->_allNodes.c_str() );
	}

	_totalClusterNumber = _dbConnector->_nodeMgr->_totalClusterNumber;
	_curClusterNumber = _dbConnector->_nodeMgr->_curClusterNumber;
	raydebug( stdout, JAG_LOG_LOW, "Clusters [%d]\n", _totalClusterNumber );
	raydebug( stdout, JAG_LOG_LOW, "Cluster  [%d]\n", _curClusterNumber );

	_servToken = _cfg->getValue("SERVER_TOKEN", "wvcYrfYdVagqXQ4s3eTFKyvNFxV");

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

	for ( int i=0; i < sp.length(); ++i ) {
		_internalHostNum->addKeyValue(AbaxString(sp[i]), i);
	}

	raydebug( stdout, JAG_LOG_LOW, "This is %d-th server in the cluster\n", _nthServer );
	raydebug( stdout, JAG_LOG_LOW, "Fault tolerance copy is %d\n", _faultToleranceCopy );

	_perfFile = jaguarHome() + "/log/perflog.txt";
	JagBoundFile bf( _perfFile.c_str(), 96 );
	bf.openAppend();
	bf.close();

	Jstr dologmsg = makeLowerString(_cfg->getValue("DO_DBLOG_MSG", "no"));
	Jstr dologerr = makeLowerString(_cfg->getValue("DO_DBLOG_ERR", "yes"));
	int logdays = _cfg->getIntValue("DBLOG_DAYS", 3);
	int logmsg=0, logerr=0;
	if ( dologmsg == "yes" ) { logmsg = 1; raydebug( stdout, JAG_LOG_LOW, "DB logging message is enabled.\n" ); } 
	if ( dologerr == "yes" ) { logerr = 1; raydebug( stdout, JAG_LOG_LOW, "DB logging error logger is enabled.\n" ); } 
	if ( logmsg || logerr ) {
		raydebug( stdout, JAG_LOG_LOW, "DB log %d days\n", logdays );
	}
	_dbLogger = new JagDBLogger( logmsg, logerr, logdays );

	_numCPUs = _jagSystem.getNumCPUs();
	raydebug( stdout, JAG_LOG_LOW, "Number of cores %d\n", _numCPUs );

	Jstr scaleMode = makeLowerString(_cfg->getValue("SCALE_MODE", "static"));
	if  ( scaleMode == "static" ) {
		_scaleMode = JAG_SCALE_STATIC;
	} else {
		_scaleMode = JAG_SCALE_GOLAST;
	}

	pthread_mutex_init( &g_dbconnectormutex, NULL );
}

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

	if ( _jagUUID ) {
		delete _jagUUID;
		_jagUUID = NULL;
	}

	if ( _internalHostNum ) {
		delete _internalHostNum;
		_internalHostNum = NULL;
	}

	if ( _delPrevOriCommandFile ) {
		jagfclose( _delPrevOriCommandFile );
		_delPrevOriCommandFile = NULL;
	}

	if ( _delPrevRepCommandFile ) {
		jagfclose( _delPrevRepCommandFile );
		_delPrevRepCommandFile = NULL;
	}

	if ( _delPrevOriRepCommandFile ) {
		jagfclose( _delPrevOriRepCommandFile );
		_delPrevOriRepCommandFile = NULL;
	}

	if ( _delNextOriCommandFile ) {
		jagfclose( _delNextOriCommandFile );
		_delNextOriCommandFile = NULL;
	}

	if ( _delNextRepCommandFile ) {
		jagfclose( _delNextRepCommandFile );
		_delNextRepCommandFile = NULL;
	}

	if ( _delNextOriRepCommandFile ) {
		jagfclose( _delNextOriRepCommandFile );
		_delNextOriRepCommandFile = NULL;
	}

	if ( _recoveryRegCommandFile ) {
		jagfclose( _recoveryRegCommandFile );
		_recoveryRegCommandFile = NULL;
	}

	if ( _recoverySpCommandFile ) {
		jagfclose( _recoverySpCommandFile );
		_recoverySpCommandFile = NULL;
	}


	if ( _blockIPList )  delete _blockIPList;
	if ( _allowIPList )  delete _allowIPList;
	pthread_rwlock_destroy(&_aclrwlock);

	delete _dbLogger;

	pthread_mutex_destroy( &g_dbschemamutex );
	pthread_mutex_destroy( &g_flagmutex );
	pthread_mutex_destroy( &g_wallogmutex );
	pthread_mutex_destroy( &g_dlogmutex );
	pthread_mutex_destroy( &g_dinsertmutex );
	pthread_mutex_destroy( &g_datacentermutex );

	pthread_mutex_destroy( &g_dbconnectormutex );

	closeDataCenterConnection();

}


int JagDBServer::main(int argc, char*argv[])
{
	jagint callCounts = -1, lastBytes = 0;
	raydebug(stdout, JAG_LOG_LOW, "s1101 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	JagNet::socketStartup();

	umask(077);
	initDirs();

	initConfigs();
	raydebug(stdout, JAG_LOG_LOW, "Initialized config data\n" );

	_jagSystem.initLoad(); // for load stats

	pthread_mutex_init( &g_dbschemamutex, NULL );
	pthread_mutex_init( &g_flagmutex, NULL );
	pthread_mutex_init( &g_wallogmutex, NULL );
	pthread_mutex_init( &g_dlogmutex, NULL );
	pthread_mutex_init( &g_dinsertmutex, NULL );
	pthread_mutex_init( &g_datacentermutex, NULL );

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
	makeTableObjects( );

	raydebug(stdout, JAG_LOG_LOW, "s1103 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	JAG_BLURT jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;
	_restartRecover = 1;
	jaguar_mutex_unlock ( &g_flagmutex );

	resetDeltaLog();
	resetRegSpLog();
	raydebug(stdout, JAG_LOG_LOW, "s1104 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	mainInit();
	raydebug(stdout, JAG_LOG_LOW, "s1106 availmem=%l M\n", availableMemory( callCounts, lastBytes)/ONE_MEGA_BYTES );

	printResources();
	_serverReady = true;

	openDataCenterConnection();
	raydebug(stdout, JAG_LOG_LOW, "Jaguar server ready to process commands from clients\n" );

	jaguint seq = 1;
	
	while ( true ) {
		sleep(60);

		doBackup( seq );
		writeLoad( seq );

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
void JagDBServer::addTask(  jaguint taskID, JagSession *session, const char *mesg )
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

// Process commands in one thread
int JagDBServer::processMultiSingleCmd( JagRequest &req, const char *mesg, jagint msglen, 
										jagint &threadSchemaTime, jagint &threadHostTime, jagint threadQueryTime, 
										bool redoOnly, int isReadOrWriteCommand )
{
	if ( _shutDownInProgress > 0 ) {
		return 0;
	}	

	int rc;
	bool sucsync = true;
	JagParseAttribute jpa( this, req.session->timediff, servtimediff, req.session->dbname, _cfg );
	Jstr reterr, rowFilter;
		
	if ( req.batchReply ) {
		JagStrSplitWithQuote split( mesg, ';' );
		int msgnum = split.length();
		if ( ! _isGate ) {
			JagParser parser((void*)this);
			JagParseParam pparam( &parser ); 
    		for ( int i = 0; i < msgnum; ++i ) {
				bool brc = parser.parseCommand( jpa, split[i], &pparam, reterr );
    			if ( brc ) {
					// before do insert, need to check permission of this user for insert
					rc = checkUserCommandPermission( NULL, req, pparam, 0, rowFilter, reterr );
					if ( rc ) {
						if ( ! redoOnly && req.batchReply ) {
							logCommand( &pparam, req.session, split[i].c_str(), split[i].size(), 2 );
						}
						
						rc = doInsert( req, pparam, reterr, split[i] );
    					if ( ! rc ) {
    					} 
					} else {
					}
    			} else {
    			}
    		}
		} else {
			// if is gate, do not store data locally
		}

		//raydebug( stdout, JAG_LOG_LOW, "s22389 done %d batch insert\n", msgnum );
		// sync command to other data-centers
		if ( req.dorep ) {
			JagParser parser((void*)this);
			JagParseParam pparam( &parser );
			JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
			for ( int i = 0; i < msgnum; ++i ) {
				if ( ! parser.parseCommand( jpa, split[i], &pparam, reterr ) ) {
					continue; 
				}

				if ( JAG_FINSERT_OP == pparam.opcode ) {
					synchToOtherDataCenters( split[i].c_str(), sucsync, req );
					// finsert to insert
				} else {
					synchToOtherDataCenters( split[i].c_str(), sucsync, req );
				}	
			}
			jaguar_mutex_unlock ( &g_datacentermutex );
		}

		// check timestamp to see if need to update schema for client SC
		if ( !req.session->origserv && threadSchemaTime < g_lastSchemaTime ) {
			sendMapInfo( "_cschema", req );
			threadSchemaTime = g_lastSchemaTime;
		}

		// check timestamp to see if need to update host list for client HL
		if ( !req.session->origserv && threadHostTime < g_lastHostTime ) {
			sendHostInfo( "_chost", req );
			threadHostTime = g_lastHostTime;
		}

		Jstr endmsg = Jstr("_END_[T=20|E=|]");
		if ( req.hasReply && !redoOnly ) {
			sendMessageLength( req, endmsg.c_str(), endmsg.length(), "ED" );
		} else {
			//	endmsg.c_str(), req.hasReply, redoOnly ));
		}
	} else {
		// single cmd (not batch insert)
		JagParser parser( (void*)this );
		JagParseParam pparam( &parser );
		if ( parser.parseCommand( jpa, mesg, &pparam, reterr ) ) {
			if ( JAG_SHOWSVER_OP == pparam.opcode ) {
				char brand[32];
				char hellobuf[128];
				sprintf(brand, "jaguar" );
				brand[0] = toupper( brand[0] );
				sprintf( hellobuf, "%s Server %s", brand, _version.c_str() );
				rc = sendMessage( req, hellobuf, "OK");  // SG: Server Greeting
				sendMessage( req, "_END_[T=20|E=]", "ED");  // SG: Server Greeting
				return 1;
			} 

    		if ( ! _isGate ) {
        		if ( !redoOnly && isReadOrWriteCommand == JAG_WRITE_SQL ) { // write related commands, store in wallog
					if ( JAG_INSERT_OP == pparam.opcode ) {
        				logCommand(&pparam, req.session, mesg, msglen, 2 );
					} else if ( JAG_UPDATE_OP == pparam.opcode || JAG_DELETE_OP == pparam.opcode ) {
        				logCommand(&pparam, req.session, mesg, msglen, 1 );
					} else if ( JAG_FINSERT_OP == pparam.opcode || JAG_CINSERT_OP == pparam.opcode ) {
        				logCommand(&pparam, req.session, mesg, msglen, 2 );
					} else {
						// other commands (e.g. scheme changes) have no logs
					}
        		}

				rc = processCmd( jpa, req, mesg, pparam, reterr, threadQueryTime, threadSchemaTime );
    			if ( reterr.size()< 1 && req.dorep && req.syncDataCenter && 0 == req.session->replicateType ) {
    				JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
    				synchToOtherDataCenters( mesg, sucsync, req );
    				jaguar_mutex_unlock ( &g_datacentermutex );
    			}

    		} else {
			        // _isGate 
					if ( JAG_SELECT_OP == pparam.opcode || JAG_GETFILE_OP == pparam.opcode || JAG_COUNT_OP == pparam.opcode ||
						 JAG_INNERJOIN_OP == pparam.opcode || JAG_LEFTJOIN_OP == pparam.opcode || JAG_RIGHTJOIN_OP == pparam.opcode ||
						 JAG_FULLJOIN_OP == pparam.opcode ) {

    					JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
    					rc = synchFromOtherDataCenters( req, mesg, pparam );
    					jaguar_mutex_unlock ( &g_datacentermutex );						
					} else {
						rc = processCmd( jpa, req, mesg, pparam, reterr, threadQueryTime, threadSchemaTime );
    					if ( reterr.size() < 1 && req.dorep && req.syncDataCenter && 0 == req.session->replicateType ) {
    						JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
    						synchToOtherDataCenters( mesg, sucsync, req );
    						jaguar_mutex_unlock ( &g_datacentermutex );
    					}
					}
			} 
			
			// send endmsg: -111 is X1, when select sent one record data, no more endmsg
			// send endmsg if not X1 ( one line select )
			if ( -111 != rc && req.hasReply && !redoOnly ) { 
				// check timestamp to see if need to update schema for client SC
				if ( !req.session->origserv && threadSchemaTime < g_lastSchemaTime ) {
					sendMapInfo( "_cschema", req );
					threadSchemaTime = g_lastSchemaTime;
				}

				// check timestamp to see if need to update host list for client HL
				if ( !req.session->origserv && threadHostTime < g_lastHostTime ) {
					sendHostInfo( "_chost", req );
					threadHostTime = g_lastHostTime;
				}
				
				Jstr resstr, endmsg;
				if ( sucsync ) endmsg = Jstr("_END_[T=777|E=");
				else endmsg = Jstr("_END_[T=20|E=");

				if ( reterr.length() > 0 ) {
					endmsg += reterr;
					resstr = "ER";
				} else {
					resstr = "ED";
				}

				endmsg += Jstr("|]");
				sendMessageLength( req, endmsg.c_str(), endmsg.length(), resstr.c_str() );
			} else {
			}
		} else {
			// parsing param got error
			if ( req.hasReply && !redoOnly )  {
				Jstr resstr = "ER", endmsg = Jstr("_END_[T=20|E=");
				endmsg += reterr + Jstr("|]");
				sendMessageLength( req, endmsg.c_str(), endmsg.length(), resstr.c_str() );
			}
		}

	}
	
	return 1;
}

// Process commands in one thread
jagint JagDBServer::processCmd(  const JagParseAttribute &jpa, JagRequest &req, const char *cmd, 
								 JagParseParam &parseParam, Jstr &reterr, jagint threadQueryTime, 
								 jagint &threadSchemaTime )
{

	reterr = "";
	if ( JAG_SELECT_OP != parseParam.opcode ) {
		_dbLogger->logmsg( req, "DML", cmd );
	}

	bool rc;	
	jagint cnt = 0;
	JagCfg *cfg = _cfg;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	Jstr errmsg, dbtable, rowFilter; 
	Jstr dbname = makeLowerString(req.session->dbname);
	JagIndex *pindex = NULL;
	JagTable *ptab = NULL;

	// change default dbname for single cmd
	if ( parseParam.objectVec.size() == 1 ) { dbname = parseParam.objectVec[0].dbName; }
	req.opcode = parseParam.opcode;

	// methods of frequent use
	if ( JAG_SELECT_OP == parseParam.opcode || JAG_INSERTSELECT_OP == parseParam.opcode 
		 || JAG_GETFILE_OP == parseParam.opcode ) {

		if (  parseParam.isSelectConst() ) {
			 processSelectConstData( req, &parseParam );
			 return 1;
		}


		Jstr dbidx, tabName, idxName; 
		JagDataAggregate *jda = NULL; int pos = 0;

		if ( JAG_INSERTSELECT_OP == parseParam.opcode && parseParam.objectVec.size() > 1 ) {
			// insert into ... select ... from syntax, select part as objectVec[1]
				pos = 1;
		}

		if ( parseParam.objectVec[pos].indexName.length() > 0 ) {
			// known index
			pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].dbName, 
												  tabName, parseParam.objectVec[pos].indexName,
												  req.session->replicateType, 0 );
			dbidx = parseParam.objectVec[pos].dbName;
			idxName = parseParam.objectVec[pos].indexName;
		} else {
			// table object or index object
			ptab = _objectLock->readLockTable( parseParam.opcode, parseParam.objectVec[pos].dbName, 
												parseParam.objectVec[pos].tableName, req.session->replicateType, 0 );
			if ( !ptab ) {
				pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].dbName, 
											      tabName, parseParam.objectVec[pos].tableName,
													  req.session->replicateType, 0 );
					dbidx = parseParam.objectVec[pos].dbName;
					idxName = parseParam.objectVec[pos].tableName;
				if ( !pindex ) {
					pindex = _objectLock->readLockIndex( parseParam.opcode, parseParam.objectVec[pos].colName, 
														  tabName, parseParam.objectVec[pos].tableName,
														  req.session->replicateType, 0 );
					dbidx = parseParam.objectVec[pos].colName;
					idxName = parseParam.objectVec[pos].tableName;
				}
			}
		}
			
		if ( !ptab && !pindex ) {
			if ( parseParam.objectVec[pos].colName.length() > 0 ) {
				reterr = Jstr("E1120 Unable to select for ") + 
							parseParam.objectVec[pos].colName + "." + parseParam.objectVec[pos].tableName +
							" session.dbname=[" + req.session->dbname + "]"; 
			} else {
				reterr = Jstr("E1121 Unable to select for ") + 
						parseParam.objectVec[pos].dbName + "." + parseParam.objectVec[pos].tableName;
			}
			if ( parseParam.objectVec[pos].indexName.size() > 0 ) {
				reterr += Jstr(".") + parseParam.objectVec[pos].indexName;
			}
			return 0;
		}
		
		if ( ptab ) {
			// table to select ...
			rc = checkUserCommandPermission( &ptab->_tableRecord, req, parseParam, 0, rowFilter, errmsg );
			if ( rc ) {
				if ( rowFilter.size() > 0 ) {
					parseParam.resetSelectWhere( rowFilter );
					parseParam.setSelectWhere();
					Jstr newcmd = parseParam.formSelectSQL();
					cmd = newcmd.c_str();
				} else {
				}

				if ( parseParam.exportType == JAG_EXPORT ) {
					Jstr dbtab = dbname + "." + parseParam.objectVec[pos].tableName;
					Jstr dirpath = jaguarHome() + "/export/" + dbtab;
					JagFileMgr::rmdir( dirpath );
					raydebug( stdout, JAG_LOG_LOW, "s22028 rmdir %s\n", dirpath.s() );
				}

				cnt = ptab->select( jda, cmd, req, &parseParam, errmsg, true, pos );
				// export is processed in select
			} else {
				cnt = -1;
			}

			_objectLock->readUnlockTable( parseParam.opcode, parseParam.objectVec[pos].dbName, 
										   parseParam.objectVec[pos].tableName, req.session->replicateType, 0 );
		} else if ( pindex ) {
			rc = checkUserCommandPermission( &pindex->_indexRecord, req, parseParam, 0, rowFilter, errmsg );
			if ( rc ) {
				cnt = pindex->select( jda, cmd, req, &parseParam, errmsg, true, pos );
			} else {
				cnt = -1;
			}

			_objectLock->readUnlockIndex( parseParam.opcode, dbidx, tabName, idxName, req.session->replicateType, 0 );
		}

		if ( cnt == -1 ) {
			reterr = errmsg;
		}

		if ( -111 == cnt || cnt > 0 ) {
			++ numSelects;
		}

		//raydebug( stdout, JAG_LOG_LOW, "s2239 cnt=%d req.session->sessionBroken=%d\n", cnt, (int)req.session->sessionBroken );
		if (  cnt > 0 && !req.session->sessionBroken ) {
			if ( jda ) {
				jda->sendDataToClient( cnt, req );
			}
		} else {
		}

		if ( !req.session->sessionBroken && parseParam._lineFile && parseParam._lineFile->size() > 0 ) {
			sendValueData( parseParam, req  );
		} 

		if ( parseParam.exportType == JAG_EXPORT ) req.syncDataCenter = true;
		if ( jda ) { delete jda; jda = NULL; }

	} else if ( JAG_UPDATE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			ptab = _objectLock->writeLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
												tableschema, req.session->replicateType, 0 );
			if ( !ptab ) {
				reterr = "E4283 Update can only been applied to tables";
			} else {
				Jstr dbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
				cnt = ptab->update( req, &parseParam, false, errmsg );

				_objectLock->writeUnlockTable( parseParam.opcode, dbname, 
												parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
			}
	
			if ( ! _isGate ) {
    			if ( 0 == cnt && reterr.size() < 1 ) {
    				if ( _totalClusterNumber > 1 && _curClusterNumber < (_totalClusterNumber -1) ) {
    					reterr = "E13028 server update error";
    				} else {
    					reterr = "E13209 server update error";
    					_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else if ( cnt < 0 ) {
    				if ( _totalClusterNumber > 1 && _curClusterNumber < (_totalClusterNumber -1) ) {
    					reterr = errmsg;
    				} else {
    					reterr = errmsg;
    					_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else {
    				reterr= "";
    				++ numUpdates;
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
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			ptab = _objectLock->writeLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
												tableschema, req.session->replicateType, 0 );
			if ( ptab ) {
                Jstr dbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
                cnt = ptab->remove( req, &parseParam, errmsg );
				jagint trimLimit = JAG_WALLOG_TRIM_RATIO * (JAG_SIMPFILE_LIMIT_BYTES/ptab->KEYVALLEN );
				if ( 1 || cnt > trimLimit ) {
					trimWalLogFile( ptab, dbname, parseParam.objectVec[0].tableName, 
								    ptab->_darrFamily->_insertBufferMap, ptab->_darrFamily->_keyChecker );
				}

                _objectLock->writeUnlockTable( parseParam.opcode, dbname, 
											   parseParam.objectVec[0].tableName, req.session->replicateType, 0 );
			} else {
    			reterr = "E10238 Delete can only be applied to tables";
			}

			if ( ! _isGate ) {
    			if ( 0 == cnt && reterr.size() < 1 ) {
    				if ( _totalClusterNumber > 1 && _curClusterNumber < (_totalClusterNumber -1) ) {
    					reterr = "E37702 server delete error";
    				} else {
    					reterr = "E37703 server delete error";
    					_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else if ( cnt < 0 ) {
    				if ( _totalClusterNumber > 1 && _curClusterNumber < (_totalClusterNumber -1) ) {
    					reterr = errmsg;
    				} else {
    					reterr = errmsg;
    					_dbLogger->logerr( req, reterr, cmd );
    				}
    			} else {
    				reterr= "";
    				++ numDeletes;
    			}
			} else {
    			reterr= "";
			}
			cnt = 100;  // temp fix
			req.syncDataCenter = true;
		} else {
			// no permission for delete
			cnt = 100;  // temp fix
		}
	} else if ( JAG_COUNT_OP == parseParam.opcode ) {
		Jstr dbidx, tabName, idxName; 
		if ( parseParam.objectVec[0].indexName.length() > 0 ) {
			// known index
			pindex = _objectLock->readLockIndex( parseParam.opcode, dbname, tabName, parseParam.objectVec[0].indexName,
												  req.session->replicateType, 0 );
			dbidx = dbname;
			idxName = parseParam.objectVec[0].indexName;
		} else {
				// table object or index object
				ptab = _objectLock->readLockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName,
													req.session->replicateType, 0 );
				if ( !ptab ) {
					pindex = _objectLock->readLockIndex( parseParam.opcode, dbname, tabName, parseParam.objectVec[0].tableName,
														  req.session->replicateType, 0 );
					dbidx = dbname;
					idxName = parseParam.objectVec[0].tableName;
					if ( !pindex ) {
						pindex = _objectLock->readLockIndex( parseParam.opcode, 
															  parseParam.objectVec[0].colName, tabName, parseParam.objectVec[0].tableName,
															  req.session->replicateType, 0 );
						dbidx = parseParam.objectVec[0].colName;
						idxName = parseParam.objectVec[0].tableName;
					}
				}
		}
			
		if ( !ptab && !pindex ) {
				if ( parseParam.objectVec[0].colName.length() > 0 ) {
					reterr = Jstr("E1023 Unable to select count(*) for ") + 
							 parseParam.objectVec[0].colName + "." + parseParam.objectVec[0].tableName;
				} else {
					reterr = Jstr("E1024  Unable to select count(*) for ") + 
							 parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
				}
				if ( parseParam.objectVec[0].indexName.size() > 0 ) {
					reterr += Jstr(".") + parseParam.objectVec[0].indexName;
				}
				return 0;
		}
		
		if ( ptab ) {
				rc = checkUserCommandPermission( &ptab->_tableRecord, req, parseParam, 0, rowFilter, errmsg );
				if ( rc ) {
					cnt = ptab->getCount( cmd, req, &parseParam, errmsg );
				} else {
					cnt = -1;
				}
				_objectLock->readUnlockTable( parseParam.opcode, dbname, parseParam.objectVec[0].tableName, 
												req.session->replicateType, 0 );
		} else if ( pindex ) {
				rc = checkUserCommandPermission( &pindex->_indexRecord, req, parseParam, 0, rowFilter, errmsg );
				if ( rc ) {
					cnt = pindex->getCount( cmd, req, &parseParam, errmsg );
				} else {
					cnt = -1;
				}
				_objectLock->readUnlockIndex( parseParam.opcode, dbidx, tabName, idxName, req.session->replicateType, 0 );
		}

		if ( cnt == -1 ) {
			reterr = errmsg;
		} else {
			char cntbuf[30];
			memset( cntbuf, 0, 30 );
			sprintf( cntbuf, "%lld", cnt );
			sendMessage( req, cntbuf, "OK" );
		}
		
		if ( cnt >= 0 ) {
			prt(("selectcount type=%d %s cnt=%lld \n", req.session->replicateType, parseParam.objectVec[0].tableName.c_str(), cnt));
		}
			
		// for testing use only, no need for production
	} else if ( parseParam.isJoin() ) {
		if ( !strcasestrskipquote( cmd, "count(*)" ) ) {
			rc = joinObjects( req, &parseParam, reterr );
		}
	// methods to change schema or userid
   	} else if ( JAG_IMPORT_OP == parseParam.opcode ) {
		importTable( req, dbname, &parseParam, reterr );
	} else if ( JAG_CREATEUSER_OP == parseParam.opcode ) {
		createUser( req, parseParam, threadQueryTime );
	} else if ( JAG_DROPUSER_OP == parseParam.opcode ) {
		dropUser( req, parseParam, threadQueryTime );
	} else if ( JAG_CHANGEPASS_OP == parseParam.opcode ) {
		changePass( req, parseParam, threadQueryTime );		
	} else if ( JAG_CHANGEDB_OP == parseParam.opcode ) {
		changeDB( req, parseParam, threadQueryTime );
	} else if ( JAG_CREATEDB_OP == parseParam.opcode ) {
		createDB( req, parseParam, threadQueryTime );
	} else if ( JAG_DROPDB_OP == parseParam.opcode ) {
		dropDB( req, parseParam, threadQueryTime );
	} else if ( JAG_CREATETABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			rc = createTable( req, dbname, &parseParam, reterr, threadQueryTime );
			if ( rc && parseParam.timeSeries.size() > 0 ) {
				Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
				createTimeSeriesTables( req, parseParam.timeSeries, dbname, dbtable, jpa, reterr );
			}
		} else {
			noGood( req, parseParam );
		}
	} else if ( JAG_CREATECHAIN_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			rc = createTable( req, dbname, &parseParam, reterr, threadQueryTime );
			if ( rc && parseParam.timeSeries.size() > 0 ) {
				Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
				createTimeSeriesTables( req, parseParam.timeSeries, dbname, dbtable, jpa, reterr );
			}
		} else {
			noGood( req, parseParam );
		}
	} else if ( JAG_CREATEMEMTABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			rc = createMemTable( req, dbname, &parseParam, reterr, threadQueryTime );
		} else {
			noGood( req, parseParam );
		}
	} else if ( JAG_CREATEINDEX_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) { 
			rc = createIndex( req, dbname, &parseParam, ptab, pindex, reterr, threadQueryTime );
			Jstr tSer;
			if ( rc && ptab->hasTimeSeries( tSer ) && parseParam.value=="ticks" ) {
				createTimeSeriesIndexes( jpa, req, parseParam, tSer, reterr );
			}
		} else noGood( req, parseParam ); // if checkUserCommandPermission fails
	} else if ( JAG_ALTER_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( ! rc ) {
			noGood( req, parseParam );
		} else {
			rc = alterTable( jpa, req, dbname, &parseParam, reterr, threadQueryTime, threadSchemaTime );	
		}
   	} else if ( JAG_DROPTABLE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			Jstr timeSeries;
			rc = dropTable( req, &parseParam, reterr, threadQueryTime, timeSeries );
			if ( rc && timeSeries.size() > 0 ) {
				Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
				dropTimeSeriesTables( req, timeSeries, dbname, dbtable, jpa, reterr );
			}
		} else noGood( req, parseParam );
   	} else if ( JAG_DROPINDEX_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) {
			Jstr timeSeries;
			rc = dropIndex( req, dbname, &parseParam, reterr, threadQueryTime, timeSeries );
			if ( rc && timeSeries.size() > 0 ) {
				dropTimeSeriesIndexes( req, jpa, parseParam.objectVec[0].tableName, parseParam.objectVec[1].indexName, timeSeries );
			}
		}
   	} else if ( JAG_TRUNCATE_OP == parseParam.opcode ) {
		rc = checkUserCommandPermission( NULL, req, parseParam, 0, rowFilter, reterr );
		if ( rc ) rc = truncateTable( req, &parseParam, reterr, threadQueryTime );
		else noGood( req, parseParam );
   	} else if ( JAG_GRANT_OP == parseParam.opcode ) {
		grantPerm( req, parseParam, threadQueryTime );
   	} else if ( JAG_REVOKE_OP == parseParam.opcode ) {
		revokePerm( req, parseParam, threadQueryTime );
   	} else if ( JAG_SHOWGRANT_OP == parseParam.opcode ) {
		showPerm( req, parseParam, threadQueryTime );
	} else if ( JAG_DESCRIBE_OP == parseParam.opcode ) {
		if ( parseParam.objectVec[0].indexName.length() > 0 ) {
			Jstr res = describeIndex( parseParam.detail, req, indexschema, parseParam.objectVec[0].dbName, 
									  parseParam.objectVec[0].indexName, reterr, false, false, "" );
			if ( res.size() > 0 ) { sendMessageLength( req, res.c_str(), res.size(), "DS" ); }
		} else {
			Jstr dbtable = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
			Jstr res;
			if ( tableschema->existAttr ( dbtable ) ) {
				res = describeTable( JAG_ANY_TYPE, req, tableschema, dbtable, parseParam.detail, false, false, "" );
			} else {
				if ( parseParam.objectVec[0].colName.length() > 0 ) {
					res = describeIndex( parseParam.detail, req, indexschema, parseParam.objectVec[0].colName, 
							   			 parseParam.objectVec[0].tableName, reterr, false, false, "" );
				} else {
					res = describeIndex( parseParam.detail, req, indexschema, parseParam.objectVec[0].dbName, 
								   		 parseParam.objectVec[0].tableName, reterr, false, false, "" );
			    }
			}
			if ( res.size() > 0 ) { sendMessageLength( req, res.c_str(), res.size(), "DS" ); }
		}
	} else if ( JAG_SHOW_CREATE_TABLE_OP == parseParam.opcode ) {
		Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			Jstr res = describeTable( JAG_TABLE_TYPE, req, tableschema, dbtable, parseParam.detail, true, false, "" );
			if ( res.size() > 0 ) { sendMessageLength( req, res.c_str(), res.size(), "DS" ); }
		} else {
			reterr = "Table " + dbtable + " not found";
		}
	} else if ( JAG_SHOW_CREATE_CHAIN_OP == parseParam.opcode ) {
		Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			Jstr res = describeTable( JAG_CHAINTABLE_TYPE, req, tableschema, dbtable, parseParam.detail, true, false, "" );
			if ( res.size() > 0 ) { sendMessageLength( req, res.c_str(), res.size(), "DS" ); }
		} else {
			reterr = "Chain " + dbtable + " not found";
		}
	} else if ( JAG_EXEC_DESC_OP == parseParam.opcode ) {
		Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		if ( tableschema->existAttr ( dbtable ) ) {
			_describeTable( req, dbtable, 0 );
		} else {
			reterr = "Table " + dbtable + " not found";
		}
	} else if ( JAG_SHOWUSER_OP == parseParam.opcode ) {
		showUsers( req );
	} else if ( JAG_SHOWDB_OP == parseParam.opcode ) {
		showDatabases( cfg, req );
	} else if ( JAG_CURRENTDB_OP == parseParam.opcode ) {
		showCurrentDatabase( cfg, req, dbname );
	} else if ( JAG_SHOWSTATUS_OP == parseParam.opcode ) {
		showClusterStatus( req );
	} else if ( JAG_SHOWDATACENTER_OP == parseParam.opcode ) {
		showDatacenter( req );
	} else if ( JAG_SHOWTOOLS_OP == parseParam.opcode ) {
		showTools( req );
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
			Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
			showIndexes( req, parseParam, indexschema, dbtable );
		}
	} else if ( JAG_SHOWTASK_OP == parseParam.opcode ) {
		showTask( req );
	} else if ( JAG_EXEC_SHOWDB_OP == parseParam.opcode ) {
		_showDatabases( cfg, req );
	} else if ( JAG_EXEC_SHOWTABLE_OP == parseParam.opcode ) {
		_showTables( req, tableschema, dbname );
	} else if ( JAG_EXEC_SHOWINDEX_OP == parseParam.opcode ) {
		Jstr dbtable = dbname + "." + parseParam.objectVec[0].tableName;
		_showIndexes( req, indexschema, dbtable );
	}

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
	// JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
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
	// jaguar_mutex_unlock ( &g_dbschemamutex );
}

// method to check non standard command is valid or not
// may append more commands later
// return 0: for error
int JagDBServer::isValidInternalCommand( const char *mesg )
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
	else if ( 0 == strncmp( mesg, "_ex_addclust_migrate", 20 ) ) return JAG_SCMD_EXADDCLUSTER_MIGRATE;
	else if ( 0 == strncmp( mesg, "_ex_addclust_migrcontinue", 25 ) ) return JAG_SCMD_EXADDCLUSTER_MIGRATE_CONTINUE;
	else if ( 0 == strncmp( mesg, "_ex_addclustr_mig_complete", 26 ) ) return JAG_SCMD_EXADDCLUSTER_MIGRATE_COMPLETE;
	else if ( 0 == strncmp( mesg, "_ex_addcluster", 14 ) ) return JAG_SCMD_EXADDCLUSTER;
	else if ( 0 == strncmp( mesg, "_ex_importtable", 15 ) ) return JAG_SCMD_IMPORTTABLE;
	else if ( 0 == strncmp( mesg, "_ex_truncatetable", 17 ) ) return JAG_SCMD_TRUNCATETABLE;
	else if ( 0 == strncmp( mesg, "_exe_shutdown", 13 ) ) return JAG_SCMD_EXSHUTDOWN;
	else if ( 0 == strncmp( mesg, "_getpubkey", 10 ) ) return JAG_SCMD_GETPUBKEY;
	// more commands to be added
	else return 0;
}

// method to check is simple command or not
// may append more commands later
// returns 0 for false (not simple commands)
int JagDBServer::isSimpleCommand( const char *mesg )
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
 	JAGSOCK sock = ptr->sock;

 	int 	rc, simplerc;
 	char  	*pmesg, *p;
	jagint 	threadSchemaTime = 0, threadHostTime = g_lastHostTime, threadQueryTime = 0, len;
	jagint cnt = 1;

 	int  authed = 0;
 
	JagSession session;
	session.sock = sock;
	session.servobj = (JagDBServer*)ptr->servobj;
	session.ip = ptr->ip;
	session.active = 0;
	raydebug( stdout, JAG_LOG_HIGH, "Client IP: %s\n", session.ip.c_str() );

	JagDBServer  *servobj = (JagDBServer*)ptr->servobj;
	++ servobj->_activeClients; 

	char *sbuf = (char*)jagmalloc(SERVER_SOCKET_BUFFER_BYTES+1);
	char 	rephdr[4]; rephdr[3] = '\0';
	int 	hdrsz = JAG_SOCK_TOTAL_HDR_LEN;
 	char 	hdr[hdrsz+1];
 	char 	hdr2[hdrsz+1];
	char 	*newbuf = NULL;
	char 	*newbuf2 = NULL;
	char 	sqlhdr[JAG_SOCK_SQL_HDR_LEN+1];

    for(;;)
    {
		if ( ( cnt % 100000 ) == 0 ) { jagmalloc_trim( 0 ); }

		JagRequest req;
		req.session = &session;

		session.active = 0;
		sbuf[0] = '\0';
		len = recvMessageInBuf( sock, hdr, newbuf, sbuf, SERVER_SOCKET_BUFFER_BYTES );
		session.active = 1;
		if ( len <= 0 ) {
			if ( session.uid == "admin" ) {
				if ( session.exclusiveLogin ) {
					servobj->_exclusiveAdmins = 0;
					raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", session.ip.c_str() );
				}
			}

			// disconnecting ...
			if ( newbuf ) { free( newbuf ); }
			if ( newbuf2 ) { free( newbuf2 ); }
			-- servobj->_connections;

			if ( ! session.origserv && 0 == session.replicateType ) {
				raydebug( stdout, JAG_LOG_LOW, "user %s disconnected from %s\n", session.uid.c_str(), session.ip.c_str() );
				if ( session.uid == "admin" && session.datacenter ) {
					raydebug( stdout, JAG_LOG_LOW, "datacenter admin disconnected from %s\n", session.ip.c_str() );
					// servobj->reconnectDataCenter( session.ip );
				}
			}

			if ( ! session.origserv ) {
				servobj->_clientSocketMap.removeKey( sock );
			}
			break;
		}

		++cnt;
		getXmitSQLHdr( hdr, sqlhdr );

		struct timeval now;
		gettimeofday( &now, NULL );
		threadQueryTime = now.tv_sec * (jagint)1000000 + now.tv_usec;

		// if recv heartbeat, ignore
		if ( hdr[hdrsz-3] == 'H' && hdr[hdrsz-2] == 'B' ) {
			continue;
		}

		if ( hdr[hdrsz-3] == 'N' ) {
			req.hasReply = false;
			req.batchReply = false;
		} else if ( hdr[hdrsz-3] == 'B' ) {
			req.hasReply = true;
			req.batchReply = true;
		} else {
			req.hasReply = true;
			req.batchReply = false;
		}

		if ( hdr[hdrsz-2] == 'Z' ) {
			req.doCompress = true;
		} else {
			req.doCompress = false;
		}

		if ( JAG_DATACENTER_GATE == req.session->dcfrom && JAG_DATACENTER_HOST == req.session->dcto ) {
			req.dorep = false;
		} else if ( JAG_DATACENTER_GATE == req.session->dcfrom && JAG_DATACENTER_PGATE == req.session->dcto ) {
			req.dorep = false;
		} else {
			if ( servobj->_isGate ) {
				req.dorep = true;
			} else {
				req.dorep = false;
			}
		}

		if ( newbuf ) { pmesg = newbuf; } else { pmesg = sbuf; }

		Jstr us;
		if ( req.doCompress ) {
			if ( newbuf ) {
				JagFastCompress::uncompress( newbuf, len, us );
			} else {
				JagFastCompress::uncompress( sbuf, len, us );
			}
			pmesg = (char*)us.c_str();
			len = us.length();
		}
	
		while ( *pmesg == ' ' || *pmesg == '\t' ) ++pmesg;
		
		if ( *pmesg == '_' && 0 != strncmp( pmesg, "_show", 5 ) 
			  && 0 != strncmp( pmesg, "_desc", 5 )
			  && 0 != strncmp( pmesg, "_send", 5) && 0!=strncmp( pmesg, "_disc", 5 ) ) {
			rc = isValidInternalCommand( pmesg ); // "_xxx" commands
			if ( !rc ) {
				Jstr errmsg = Jstr("_END_[T=10|E=Error non standard server command]") + "[" + pmesg + "]";
				sendMessageLength( req, errmsg.s(), errmsg.size(), "ER" );
			} else {
				if ( ! authed && JAG_SCMD_GETPUBKEY != rc ) {
					sendMessage( req, "_END_[T=20|E=Not authed before query]", "ER" );
					break;
				}
				servobj->processInternalCommands( rc, req, pmesg );
			}
			continue;
		}

		simplerc = isSimpleCommand( pmesg ); // help helo auth use etc
		if ( simplerc > 0 ) {
			int prc = servobj->processSimpleCommand( simplerc, req, pmesg, authed );
			if ( prc < 0 ) {
				break;
			} else {
				continue;
			}
		}

		if ( 0 == strcmp(pmesg, "YYY") || 0 == strcmp(sqlhdr, "SIG") ) {
			continue;
		}
		
		if ( req.session->dbname.size() < 1 ) {
			int ok = 0;
			if ( 0 == strncmp( pmesg, "show", 4 ) ) {
				p = pmesg;
				while ( *p != ' ' && *p != '\t' && *p != '\0' ) ++p;
				if ( *p != '\0' ) {
					while ( *p == ' ' || *p == '\t' ) ++p;
					if ( strncmp( p, "database", 8 ) == 0 ) { ok = 1; }
				}
			}
			
			if ( ! ok ) {
				sendMessage( req, "_END_[T=20|E=No database selected]", "ER" );
				break;
			}
		}

		if ( ! authed ) {
			sendMessage( req, "_END_[T=20|E=Not authed before requesting query]", "ER" );
			break;
		}

		if ( ! servobj->_newdcTrasmittingFin && ! req.session->datacenter && ! req.session->samePID ) {
			sendMessage( req, "_END_[T=20|E=Server not ready for datacenter to accept regular commands, please try later]", "ER" );
			continue;
		}

		if ( 1 == session.drecoverConn ) {
			rePositionMessage( req, pmesg, len );
		}
		
		int isReadOrWriteCommand = checkReadOrWriteCommand( pmesg );
		if ( isReadOrWriteCommand == JAG_WRITE_SQL ) {
			if ( servobj->_restartRecover ) {
				jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;
				int recovrc = servobj->handleRestartRecover( req, pmesg, len, hdr2, newbuf, newbuf2 );
				if ( 0 == recovrc ) {
					continue;
				} else {
					break;
				}
			} else {
			}
		}  
		else {
		}


		// add tasks	
		jaguint taskID;
		++ ( servobj->_taskID );
		taskID =  servobj->_taskID;
		addTask( taskID, req.session, pmesg );

		try {
			servobj->processMultiSingleCmd( req, pmesg, len, threadSchemaTime, threadHostTime, 
								   		   threadQueryTime, false, isReadOrWriteCommand );
		} catch ( const char *e ) {
			raydebug( stdout, JAG_LOG_LOW, "processMultiSingleCmd [%s] caught exception [%s]\n", pmesg, e );
		} catch ( ... ) {
			raydebug( stdout, JAG_LOG_LOW, "processMultiSingleCmd [%s] caught unknown exception\n", pmesg );
		}

		servobj->_taskMap->removeKey( taskID );

		if ( servobj->_faultToleranceCopy > 1 && isReadOrWriteCommand == JAG_WRITE_SQL && session.drecoverConn == 0 ) {
			rephdr[0] = rephdr[1] = rephdr[2] = 'N';
			int rsmode = 0;

			int rcr = recvMessage( sock, hdr2, newbuf2 );

			if ( rcr < 0 ) {
				rephdr[session.replicateType] = 'Y';
				rsmode = servobj->getReplicateStatusMode( rephdr, session.replicateType );

				if ( !session.spCommandReject && rsmode > 0 ) {
					servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
				}

				if ( session.uid == "admin" ) {
					if ( session.exclusiveLogin ) {
						servobj->_exclusiveAdmins = 0;
						raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", session.ip.c_str() );
					}
				}

				if ( newbuf ) { free( newbuf ); }
				if ( newbuf2 ) { free( newbuf2 ); }
				-- servobj->_connections;
				break;
			}

			memcpy( rephdr, newbuf2, 3 );
			rsmode = servobj->getReplicateStatusMode( rephdr );
			if ( !session.spCommandReject && rsmode >0 ) {
				servobj->deltalogCommand( rsmode, &session, pmesg, req.batchReply );
			} else {
			}
		}
    } // for ( ; ; )

	session.active = 0;
	-- servobj->_activeClients; 

   	jagclose(sock);
	delete ptr;
	jagmalloc_trim(0);
	free(sbuf);
   	return NULL;
}

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
		rc1 = _dbConnector->broadcastSignal( "_noop", sp[pos1] );
		rc2 = _dbConnector->broadcastSignal( "_noop", sp[pos2] );
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

int JagDBServer::createIndexSchema( const JagRequest &req, 
									const Jstr &dbname, JagParseParam *parseParam, Jstr &reterr, bool lockSchema )
{	
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	bool found = false;
	Jstr dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	Jstr dbindex = dbtable + "." + parseParam->objectVec[1].indexName;
	//parseParam->isMemTable = tableschema->isMemTable( dbtable );
	found = indexschema->indexExist( dbname, parseParam );
	if ( found ) {
		reterr = "E32016 Index already exists";
		return 0;
	}

	found = indexschema->existAttr( dbindex );
	if ( found ) { 
		reterr = "E32017 Index already exists in schama";
		return 0; 
	}

	const JagSchemaRecord *trecord = tableschema->getAttr( dbtable );
	if ( ! trecord ) {
		reterr = "E32018 Cannot find table of the index";
		return 0;
	}

	jagint getpos;
	Jstr errmsg;
	Jstr dbcol;
	Jstr defvalStr;
	int keylen = 0, vallen = 0;
	CreateAttribute createTemp;
	JagHashStrInt checkmap;
	JagHashStrInt schmap;

	createTemp.init();
	createTemp.objName.dbName = parseParam->objectVec[1].dbName;
	createTemp.objName.tableName = "";
	createTemp.objName.indexName = parseParam->objectVec[1].indexName;

    
	// map for each column
	for ( int i = 0; i < trecord->columnVector->size(); ++i ) {
		schmap.addKeyValue((*(trecord->columnVector))[i].name.c_str(), i);
	}
	
	// get key part of create index
	*(createTemp.spare) = JAG_C_COL_KEY;
	*(createTemp.spare+2) = JAG_ASC;
	bool hrc;
	for ( int i = 0; i < parseParam->limit; ++i ) {
		// save for checking duplicate keys
		checkmap.addKeyValue(parseParam->otherVec[i].objName.colName.c_str(), i);

		getpos = schmap.getValue(parseParam->otherVec[i].objName.colName.c_str(), hrc );
		if ( ! hrc ) {
			continue;
		}

		createTemp.objName.colName = (*(trecord->columnVector))[getpos].name.c_str();

		createTemp.type = (*(trecord->columnVector))[getpos].type;
		createTemp.offset = keylen;
		createTemp.length = (*(trecord->columnVector))[getpos].length;
		createTemp.sig = (*(trecord->columnVector))[getpos].sig;
		createTemp.srid = (*(trecord->columnVector))[getpos].srid;
		createTemp.metrics = (*(trecord->columnVector))[getpos].metrics;
		*(createTemp.spare+1) = (*(trecord->columnVector))[getpos].spare[1];
		*(createTemp.spare+4) = (*(trecord->columnVector))[getpos].spare[4]; // default datetime value patterns
		*(createTemp.spare+5) = (*(trecord->columnVector))[getpos].spare[5]; // mute
		*(createTemp.spare+6) = (*(trecord->columnVector))[getpos].spare[6]; // subcol
		*(createTemp.spare+7) = (*(trecord->columnVector))[getpos].spare[7]; // rollup
		*(createTemp.spare+8) = (*(trecord->columnVector))[getpos].spare[8]; // rollup-type
		dbcol = dbtable + "." + createTemp.objName.colName;
		defvalStr = "";
		tableschema->getAttrDefVal( dbcol, defvalStr );
		createTemp.defValues = defvalStr.c_str();
		keylen += createTemp.length;
		parseParam->createAttrVec.append( createTemp );
	}

	// add existing keys in table, following specified index-keys in above
	for (int i = 0; i < trecord->columnVector->size(); i++) {
		if ( (*(trecord->columnVector))[i].iskey && !checkmap.keyExist((*(trecord->columnVector))[i].name.c_str() ) ) {
			createTemp.objName.colName = (*(trecord->columnVector))[i].name.c_str();
			createTemp.type = (*(trecord->columnVector))[i].type;
			createTemp.offset = keylen;
			createTemp.length = (*(trecord->columnVector))[i].length;
			createTemp.sig = (*(trecord->columnVector))[i].sig;
			createTemp.srid = (*(trecord->columnVector))[i].srid;
			createTemp.metrics = (*(trecord->columnVector))[i].metrics;
			*(createTemp.spare+1) = (*(trecord->columnVector))[i].spare[1];
			*(createTemp.spare+4) = (*(trecord->columnVector))[i].spare[4];
			*(createTemp.spare+5) = (*(trecord->columnVector))[i].spare[5];
			*(createTemp.spare+6) = (*(trecord->columnVector))[i].spare[6];
			*(createTemp.spare+7) = (*(trecord->columnVector))[i].spare[7];
			*(createTemp.spare+8) = (*(trecord->columnVector))[i].spare[8];
			dbcol = dbtable + "." + createTemp.objName.colName;
			defvalStr = "";
			tableschema->getAttrDefVal( dbcol, defvalStr );
			createTemp.defValues = defvalStr.c_str();
			keylen += createTemp.length;
			parseParam->createAttrVec.append( createTemp );
		}
	}
	parseParam->keyLength = keylen;
	
	// get value part of create index. Index can attach value columns.  "on tab1(key: v1, v4, k3, value: v8, v10, v12 )"
	*(createTemp.spare) = JAG_C_COL_VALUE;
	for ( int i = parseParam->limit; i < parseParam->otherVec.size(); ++i ) {
		getpos = schmap.getValue(parseParam->otherVec[i].objName.colName.c_str(), hrc);
		if ( ! hrc ) {
			continue;
		}

		createTemp.objName.colName = (*(trecord->columnVector))[getpos].name.c_str();
		createTemp.type = (*(trecord->columnVector))[getpos].type;
		createTemp.offset = keylen;
		createTemp.length = (*(trecord->columnVector))[getpos].length;
		createTemp.sig = (*(trecord->columnVector))[getpos].sig;
		createTemp.srid = (*(trecord->columnVector))[getpos].srid;
		createTemp.metrics = (*(trecord->columnVector))[getpos].metrics;
		*(createTemp.spare+1) = (*(trecord->columnVector))[getpos].spare[1];
		*(createTemp.spare+4) = (*(trecord->columnVector))[getpos].spare[4];
		*(createTemp.spare+5) = (*(trecord->columnVector))[getpos].spare[5];
		*(createTemp.spare+6) = (*(trecord->columnVector))[getpos].spare[6];
		*(createTemp.spare+7) = (*(trecord->columnVector))[getpos].spare[7];

		*(createTemp.spare+8) = (*(trecord->columnVector))[getpos].spare[8];
		dbcol = dbtable + "." + createTemp.objName.colName;
		defvalStr = "";
		tableschema->getAttrDefVal( dbcol, defvalStr );
		createTemp.defValues = defvalStr.c_str();
		keylen += createTemp.length;
		vallen += createTemp.length;
		parseParam->createAttrVec.append( createTemp );
	}

	parseParam->valueLength = vallen;
	if ( lockSchema ) {
		JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	}
    int rc = indexschema->insert( parseParam, false );
	if ( lockSchema ) {
		jaguar_mutex_unlock ( &g_dbschemamutex );
	}
    if ( rc ) raydebug(stdout, JAG_LOG_LOW, "user [%s] create index [%s]\n", req.session->uid.c_str(), dbindex.c_str() );
	return rc;
}

int JagDBServer::importTable( JagRequest &req, const Jstr &dbname,
							  JagParseParam *parseParam, Jstr &reterr )
{
	JagTable *ptab = NULL;
	char cond[3] = { 'O', 'K', '\0' };
	if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
		return 0;
	}
	// waiting for signal; if NG, reject and return
 	char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
	char *newbuf = NULL;
	if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || 
					*(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
		if ( newbuf ) free( newbuf );
		// connection abort or not OK signal, reject
		return 0;
	} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
		req.syncDataCenter = true;
	}
	if ( newbuf ) free( newbuf );

	Jstr dbtab = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbtab + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		return 0;
	}

	Jstr dirpath = jaguarHome() + "/export/" + dbtab;
	if ( parseParam->impComplete ) {
		JagFileMgr::rmdir( dirpath );
		schemaChangeCommandSyncRemove( scdbobj );
		return 1;
	}

	ptab = _objectLock->readLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
		parseParam->objectVec[0].tableName, req.session->replicateType, 0 );

	Jstr host = "localhost", objname = dbtab + ".sql";
	if ( _listenIP.size() > 0 ) { host = _listenIP; }
	JaguarCPPClient pcli;
	Jstr unixSocket = Jstr("/TOKEN=") + _servToken;
	while ( !pcli.connect( host.c_str(), _port, "admin", "dummy", "test", unixSocket.c_str(), 0 ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4022 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  "admin", "dummy", host.c_str(), _port, pcli.error() );
		jagsleep(5, JAG_SEC);
	}

	if ( ptab ) {	
		_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
									   parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
	}
	schemaChangeCommandSyncRemove( scdbobj );

	Jstr fpath = JagFileMgr::getFileFamily( dirpath, objname );
	int rc = pcli.importLocalFileFamily( fpath );
	pcli.close();
	if ( rc < 0 ) {
		reterr = "Import file not found on server";
	}
	return (rc>=0);
}

void JagDBServer::init( JagCfg *cfg )
{
	Jstr jagdatahome = cfg->getJDBDataHOME( JAG_MAIN );

    Jstr sysdir = jagdatahome + "/system";
    Jstr newdir;
   	jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jagdatahome + "/test";
   	jagmkdir( sysdir.c_str(), 0700 );
	
	jagdatahome = cfg->getJDBDataHOME( JAG_PREV );
	sysdir = jagdatahome + "/system";
    jagmkdir( sysdir.c_str(), 0700 );

    sysdir = jagdatahome + "/test";
    jagmkdir( sysdir.c_str(), 0700 );
	
	jagdatahome = cfg->getJDBDataHOME( JAG_NEXT );
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

	Jstr cs;
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
	Jstr str;
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

	sendMessageLength( req, str.c_str(), str.size(), "I_" );
}

void JagDBServer::helpTopic( const JagRequest &req, const char *cmd )
{
	Jstr str;
	str += "\n";

	if ( 0 == strncasecmp( cmd, "use", 3 ) ) {
		str += "use DATABASE;\n";
		str += "\n";
		str += "Example:\n";
		str += "use userdb;\n";
	} else if ( 0 == strncasecmp( cmd, "admin", 5 ) ) {
    		str += "createdb DBNAME;\n"; 
    		str += "create database DBNAME;\n"; 
    		str += "dropdb [force] DBNAME;\n"; 
    		str += "createuser UID;\n"; 
    		str += "create user UID;\n"; 
    		str += "createuser UID:PASSWORD;\n"; 
    		str += "create user UID:PASSWORD;\n"; 
    		str += "dropuser UID;\n"; 
    		str += "showusers;\n"; 
    		str += "grant PERM1, PERM2, ... PERM on DB.TAB.COL to USER [where ...];\n"; 
    		str += "revoke PERM1, PERM2, ... PERM on DB.TAB.COL from USER;\n"; 
    		str += "addcluster;  (add new servers into existing cluster)\n"; 
    		str += "\n"; 
    		str += "Example:\n";
    		str += "createdb mydb;\n";
    		str += "create database mydb2;\n";
    		str += "dropdb mydb;\n";
    		str += "dropdb force mydb123;\n";
    		str += "createuser test;\n"; 
    		str += "  New user password: ******\n"; 
    		str += "  New user password again: ******\n"; 
    		str += "createuser test:mypassword888000;\n"; 
    		str += "create user test2:mypassword888888;\n"; 
    		str += "(Maximum length of user ID and password is 32 characters.)\n";
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
    		str += "The admin user must login to an existing host, connect to jaguardb with exclusive mode, i.e., \n";
    		str += " using \"-x yes\" option in the jag command.\n"; 
    		str += "Before starting this operation, on the host that the admin user is on, there must exist a file \n";
    		str += "$JAGUAR_HOME/conf/newcluster.conf that contains the IP addresses of new hosts on each line\n";
    		str += "in the file. DO NOT inclue existing hosts in the file newcluster.conf. Once this file is ready and correct, \n";
    		str += " execute the addcluster command which will include and activate the new servers.\n";
    		str += "\n";
    		str += "Example:\n";
    		str += "\n";
    		str += "Suppose you have 192.168.1.10, 192.168.1.11, 192.168.1.12, 192.168.1.13 on your current cluster.\n";
    		str += "You want to add a new cluster with new hosts: 192.168.1.14, 192.168.1.15, 192.168.1.16, 192.168.1.17 to the system.\n";
    		str += "The following steps demonstrate the process to add the new cluster into the system:\n";
    		str += "\n";
    		str += "1. Provision the new hosts 192.168.1.14, 192.168.1.15, 192.168.1.16, 192.168.1.17 and install jaguardb on these hosts\n";
    		str += "2. Configure the new cluster with new hosts 192.168.1.14, 192.168.1.15, 192.168.1.16, 192.168.1.17\n";
    		str += "3. The new cluster is a blank cluster, with no database schema and table data\n";
    		str += "4. Admin user should log in (or ssh) to a host in EXISTING cluster, e.g., 192.168.1.10\n";
    		str += "5. Prepare the newcluster.conf file, with the IP addresses of the hosts on each line separately\n";
    		str += "   In newcluster.conf file:\n";
    		str += "   192.168.1.14\n";
    		str += "   192.168.1.15\n";
    		str += "   192.168.1.16\n";
    		str += "   192.168.1.17\n";
    		str += "\n";
    		str += "6. Save the newcluster.conf file in $JAGUAR_HOME/conf/ directory\n";
    		str += "7. Connect to local jaguardb server from the host that contains the newcluster.conf file\n";
    		str += "       $JAGUAR_HOME/bin/jag -u admin -p <adminpassword> -h 192.168.1.10:8888 -x yes\n";
    		str += "8. While connected to the jagaurdb, execute the addcluster command:\n";
    		str += "       jaguardb> addcluster; \n";
    		str += "\n";
    		str += "Note:\n";
    		str += "\n";
    		str += "1. Never directly add new hosts in the file $JAGUAR_HOME/conf/cluster.conf manually\n";
    		str += "2. Any plan to add a new cluster of hosts must implement the addcluster process described here\n";
    		str += "3. Execute addcluster in the existing cluster, NOT in the new cluster\n";
    		str += "4. It is recommended that existing clusters and new cluster contain large number of hosts. (dozens or hundreds)\n";
    		str += "   For example, the existing cluster can have 30 hosts, and the new cluster can have 100 hosts.\n";
    		str += "5. Make sure jaguardb is installed on all the hosts of the new cluster, and connectivity is good among all the hosts\n";
    		str += "6. The server and client must have the same version, on all the hosts of existing clusters and the new cluster\n";
    		str += "7. If the new cluster is setup with a new version of jaguardb, the old clusters most likely will need an upgrade\n";
    		str += "8. After adding a new cluster, all hosts will have the same cluster.conf file\n";
    		str += "9. Make sure REPLICATION factor is the same on all the hosts\n";
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
		// str += "create table [if not exists] TABLE ( key: TS TIMESTAMP, KEY TYPE(size), ..., value: VALUE ROLLUP(TYPE|WHERE) TYPE, ...  );\n";
		str += "create table [if not exists] TABLE ( key: TS TIMESTAMP, KEY TYPE(size), ..., value: VALUE ROLLUP(TYPE) TYPE, ...  );\n";
		str += "create table [if not exists] TABLE ( key: KEY TYPE(size), ..., value: VALUE TYPE(srid:ID,metrics:M), ...  );\n";
		str += "create index [if not exists] INDEXNAEME [ticks] on TABLE(COL1, COL2, ...[, value: COL,COL]);\n";
		str += "create index [if not exists] INDEXNAEME [ticks] on TABLE(key: COL1, COL2, ...[, value: COL,COL]);\n";
		str += "\n";
		str += "Example:\n";
		str += "create table dept ( key: name char(32),  value: age int, b bit default b'0' );\n";
		str += "create table dept ( key: name char(32),  value: age int default '10', b bit default b'0' );\n";
		str += "create table log ( key: id bigint,  value: tm timestamp default current_timestamp);\n";
		str += "create table log ( key: id bigint,  value: tm timestamp default current_timestamp on update current_timestamap );\n";
		str += "create table log ( key: id bigint,  value: tn timestampnano default current_timestamp );\n";
		str += "create table log ( key: id bigint,  value: tn timestampnano default current_timestamp on update current_timestamp );\n";
		str += "create table timeseries(5m,30m) ts2 ( key: k1 int, ts timestamp, value: v1 rollup int, v2 int);\n";
		str += "create index ts2idx1 on ts2(v1, k1);\n";
		str += "create index ts2idx2 ticks on ts2(v1);\n";

		str += "create table timeseries(15m:3d,1h:24h,1d:30d,1y:10y|3M) log1 (key: ts timestamp, id bigint, value: a rollup(sum) bigint, b int);\n";
		str += "create table timeseries(15s:1m,5m:1d|3d) log2 (key: ts timestamp, id bigint, value: a rollup(sum) bigint, b int);\n";
		str += "create table timeseries(15s:3m,5m:3d) log3 (key: ts timestamp, id bigint, value: a rollup(sum) bigint, b int);\n";
		str += "create table user ( key: name char(32),  value: age int, address char(128), rdate date );\n";
		str += "create table sp ( key: name varchar(32), stime datetime, value: bus varchar(32), id uuid );\n";
		str += "create table tr ( key: name varchar(32), value: tp char(2) enum('A2', 'B3'), driver varchar(32), id uuid );\n";
		str += "create table mt ( key: name varchar(32), value: tp char(2) enum('A2', 'B3') default 'A2', zip varchar(32) );\n";
		str += "create table message ( key: name varchar(32), value: msg text );\n";
		str += "create table media ( key: name varchar(32), value: img1 file, img2 file );\n";
		str += "create table if not exists sales ( key: id bigint, stime datetime, value: member char(32) );\n";
		str += "create table geo ( key: id bigint, value: ls linestring, sq square );\n";
		str += "create table park ( key: id bigint, value: lake polygon(srid:wgs84), farm rectangle(srid:wgs84) );\n";
		str += "create table tm ( key: id bigint, value: name char(32), range(datetime) );\n";
		str += "create table pt ( key: id int, value: s linestring(srid:4326,metrics:5) );\n";
		str += "create index if not exists addr_index on user( address );\n";
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
		str += "getfile FILECOL1 sizemb from TABLE where ...;\n";
		str += "    (The above command displays size (in megabytes) of file column)\n";
		str += "getfile FILECOL1 sizegb from TABLE where ...;\n";
		str += "    (The above command displays size (in gigabytes) of file column)\n";
		str += "getfile FILECOL1 fpath from TABLE where ...;\n";
		str += "    (The above command displays the fullpath name of a file column)\n";
		str += "getfile FILECOL1 host from TABLE where ...;\n";
		str += "    (The above command displays the host where the file is stored)\n";
		str += "getfile FILECOL1 hostfpath from TABLE where ...;\n";
		str += "    (The above command displays the host and full path of the file)\n";
		str += "\n";
		str += "Example:\n";
		str += "getfile img1 into myimg1.jpg, img2 into myimg2.jog from media where uid='100';\n"; 
		str += "    (assume img1 and img2 are two file type columns in table media)\n";
		str += "getfile img time, img size, img md5 from TABLE where ...;\n";
		str += "getfile img fpath from TABLE where ...;\n";
		str += "getfile img hostfpath from TABLE where ...;\n";
		str += "getfile img fpath from INDEX where ...;\n";
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
		str += "    strdiff( EXPR(COL), EXPR(COL) )  -- Levenshtein (or edit) distance between two strings\n";
		str += "    upper( EXPR(COL) )  -- upper case string of column expression\n";
		str += "    lower( EXPR(COL) )  -- lower case string of column expression\n";
		str += "    ltrim( EXPR(COL) )  -- remove leading white spaces of string column expression\n";
		str += "    rtrim( EXPR(COL) )  -- remove trailing white spaces of string column expression\n";
		str += "    trim( EXPR(COL) )   -- remove leading and trailing white spaces of string column expression\n";
		str += "    length( EXPR(COL) ) -- length of string column expression\n";
		str += "\n";

		str += "    second( TIMECOL )   -- value of second in a time column, 0-59\n";
		str += "    minute( TIMECOL )   -- value of minute in a time column, 0-59\n";
		str += "    hour( TIMECOL )     -- value of hour in a time column, 0-23\n";
		str += "    day( TIMECOL )      -- value of day in a time column, 1-31\n";
		str += "    month( TIMECOL )    -- value of month in a time column, 1-12\n";
		str += "    year( TIMECOL )     -- value of year in a time column\n";
		str += "    date( TIMECOL )     -- date in YYYY-MM-DD in a time column\n";
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
		str += "    window(PATTERN, TIMECOL)  -- devide timecol by windows. PATTERN: xs, xm, xh, xd, xw, xM, xy, xD. x is a number.\n";
		str += "\n";

		str += "    metertomile(METERS)    -- convert meters to miles\n";
		str += "    miletometer(MILES)     -- convert miles to meters\n";
		str += "    kilometertomile(KILOMETERS)    -- convert kilometers to miles\n";
		str += "    miletokilometer(MILES     )    -- convert miles to kilometers\n";
		str += "    within(geom1, geom2)           -- check if shape geom1 is within another shape geom2\n";
		str += "    nearby(geom1, geom2, radius)   -- check if shape geom1 is close to another shape geom2 by distance radius\n";
		str += "    intersect(geom1, geom2 )       -- check if shape geom1 intersects another shape geom2\n";
		str += "    coveredby(geom1, geom2 )       -- check if shape geom1 is covered by another shape geom2\n";
		str += "    cover(geom1, geom2 )           -- check if shape geom1 covers another shape geom2\n";
		str += "    contain(geom1, geom2 )         -- check if shape geom1 contains another shape geom2\n";
		str += "    disjoint(geom1, geom2 )        -- check if shape geom1 and geom2 are disjoint\n";
		str += "    distance(geom1, geom2, 'TYPE') -- compute distance between shape geom1 and geom2. TYPE: min, max, center\n";
		str += "    distance(geom1, geom2 )        -- compute distance between shape geom1 and geom2. TYPE: center\n";
		str += "    area(geom)                     -- compute area or surface area of 2D or 3D objects\n";
		str += "    all(geom)                      -- get GeoJSON data of 2D or 3D objects\n";
		str += "    dimension(geom)         -- get dimension as integer of a shape geom \n";
		str += "    geotype(geom)           -- get type as string of a shape geom\n";
		str += "    pointn(geom,n)          -- get n-th point (1-based) of a shape geom. (x y [z])\n";
		str += "    extent(geom)            -- get bounding box of a shape geom, resulting rectangle or box\n";
		str += "    startpoint(geom)        -- get start point of a line string geom. (x y [z])\n";
		str += "    endpoint(geom)          -- get end point of a line string geom. (x y [z])\n";
		str += "    isclosed(geom)          -- check if points of a line string geom is closed. (0 or 1)\n";
		str += "    numpoints(geom)         -- get total number of points of a line string or polygon\n";
		str += "    numsegments(geom)       -- get total number of line segments of linestring or polygon\n";
		str += "    numrings(geom)          -- get total number of rings of a polygon or multipolygon\n";
		str += "    numpolygons(geom)       -- get total number of polygons of a multipolygon\n";
		str += "    srid(geom)              -- get SRID of a shape geom\n";
		str += "    summary(geom)           -- get a text summary of a shape geom\n";
		str += "    xmin(geom)              -- get the minimum x-coordinate of a shape with raster data\n";
		str += "    ymin(geom)              -- get the minimum y-coordinate of a shape with raster data\n";
		str += "    zmin(geom)              -- get the minimum z-coordinate of a shape with raster data\n";
		str += "    xmax(geom)              -- get the maximum x-coordinate of a shape with raster data\n";
		str += "    ymax(geom)              -- get the maximum y-coordinate of a shape with raster data\n";
		str += "    zmax(geom)              -- get the maximum z-coordinate of a shape with raster data\n";
		str += "    xminpoint(geom)         -- get the coordinates of minimum x-coordinate of a shape\n";
		str += "    yminpoint(geom)         -- get the coordinates of minimum y-coordinate of a shape\n";
		str += "    zminpoint(geom)         -- get the coordinates of minimum z-coordinate of a 3D shape\n";
		str += "    xmaxpoint(geom)         -- get the coordinates of maximum x-coordinate of a shape\n";
		str += "    ymaxpoint(geom)         -- get the coordinates of maximum y-coordinate of a shape\n";
		str += "    zmaxpoint(geom)         -- get the coordinates of maximum z-coordinate of a 3D shape\n";
		str += "    convexhull(geom)        -- get the convex hull of a shape with raster data\n";
		str += "    centroid(geom)          -- get the centroid coordinates of a vector or raster shape\n";
		str += "    volume(geom)            -- get the volume of a 3D shape\n";
		str += "    closestpoint(point(x y), geom)   -- get the closest point on geom from point(x y)\n";
		str += "    angle(line(x y), geom)    -- get the angle in degrees between two lines\n";
		str += "    buffer(geom, 'STRATEGY')  -- get polygon buffer of a shape. The STRATEGY is:\n";
		str += "                 distance=symmetric/asymmetric:RADIUS,join=round/miter:N,end=round/flat,point=circle/square:N\n";
		str += "    linelength(geom)        -- get length of line/3d, linestring/3d, multilinestring/3d\n";
		str += "    perimeter(geom)         -- get perimeter length of a closed shape (vector or raster)\n";
		str += "    equal(geom1,geom2)      -- check if shape geom1 is exactly the same as shape geom2\n";
		str += "    issimple(geom)          -- check if shape geom has no self-intersecting or tangent points\n";
		str += "    isvalid(geom )          -- check if multipoint, linestring, polygon, multilinestring, multipolygon is valid\n";
		str += "    isring(geom )           -- check if linestring is a ring\n";
		str += "    ispolygonccw(geom )     -- check if the outer ring is counter-clock-wise, inner rings clock-wise\n";
		str += "    ispolygoncw(geom  )     -- check if the outer ring is clock-wise, inner rings couter-clock-wise\n";
		str += "    outerring(polygon)      -- the outer ring as linestring of a polygon\n";
		str += "    outerrings(mpolygon)    -- the outer rings as multilinestring of a multipolygon\n";
		str += "    innerrings(polygon)     -- the inner rings as multilinestring of a polygon or multipolygon\n";
		str += "    ringn(polygon,n)        -- the n-th ring as linestring of a polygon. n is 1-based\n";
		str += "    innerringn(polygon,n)   -- the n-th inner ring as linestring of a polygon. n is 1-based\n";
		str += "    polygonn(multipgon,n)   -- the n-th polygon of a multipolygon. n is 1-based\n";
		str += "    unique(geom)            -- geom with consecutive duplicate points removed\n";
		str += "    union(geom1,geom2)      -- union of two geoms. Polygon outer ring should be counter-clock-wise\n";
		str += "    collect(geom1,geom2)    -- collection of two geoms\n";
		str += "    topolygon(geom)         -- converting square, rectangle, circle, ellipse, triangle to polygon\n";
		str += "    topolygon(geom,N)       -- converting circle, ellipse to polygon with N points\n";
		str += "    text(geom)              -- text string of a geometry shape\n";
		str += "    difference(g1,g2)       -- g1 minus the common part of g1 and g2\n";
		str += "    symdifference(g1,g2)    -- g1+g2 minus the common part of g1 and g2\n";
		str += "    isconvex(pgon)          -- check if the outer ring of a polygon is convex\n";
		str += "    interpolate(lstr,frac)  -- the point on linestring where linelength is at a fraction (0.0-1.0)\n";
		str += "    linesubstring(lstr,startfrac,endfrac)  -- substring of linestring between start fraction and end fraction\n";
		str += "    locatepoint(lstr,point) -- fraction where on linestring a point is closest to a given point\n";
		str += "    addpoint(lstr,point)    -- add a point to end of a linestring\n";
		str += "    addpoint(lstr,point,position)  -- add a point to the position of a linestring\n";
		str += "    setpoint(lstr,point,position)  -- change a point at position of a linestring\n";
		str += "    removepoint(lstr,position)     -- remove a point at position of a linestring\n";
		str += "    reverse(geom)                  -- reverse the order of points on a line, linestring, polygon, and multipolygon\n";
		str += "    scale(geom,factor)             -- scale the coordinates of geom by a factor\n";
		str += "    scale(geom,xfac,yfac)          -- scale the x-y coordinates of geom by factors\n";
		str += "    scale(geom,xfac,yfac,zfac)     -- scale the x-y-z coordinates of geom by factors\n";
		str += "    scaleat(geom,point,factor)     -- scale the coordinates of geom relative to a point by a factor\n";
		str += "    scaleat(geom,point,xfac,yfac)  -- scale the coordinates of geom relative to a point by a factor\n";
		str += "    scaleat(geom,point,xfac,yfac,zfac) -- scale the coordinates of geom relative to a point by a factor\n";
		str += "    scalesize(geom,factor)         -- scale the size of vector shapes by a factor\n";
		str += "    scalesize(geom,xfac,yfac)      -- scale the size of vector shapes by diffeent factors\n";
		str += "    scalesize(geom,xfac,yfac,zfac) -- scale the size of vector shapes by diffeent factors\n";
		str += "    translate(geom,dx,dy)          -- translate the location of 2D geom by dx,dy\n";
		str += "    translate(geom,dx,dy,dz)       -- translate the location of 3D geom by dx,dy,dz\n";
		str += "    transscale(geom,dx,dy,fac)     -- translate and then scale 2D geom\n";
		str += "    transscale(geom,dx,dy,xfac,yfac)  -- translate and then scale 2D geom with xfac, yfac factors\n";
		str += "    transscale(geom,dx,dy,dz,xfac,yfac,zfac)  -- translate and then scale 3D geom\n";
		str += "    rotate(geom,N)                 -- rotate 2D geom by N degrees counter-clock-wise with respect to point(0,0)\n";
		str += "    rotate(geom,N,'radian')        -- rotate 2D geom by N radians counter-clock-wise with respect to point(0,0)\n";
		str += "    rotate(geom,N,'degree')        -- rotate 2D geom by N degrees counter-clock-wise with respect to point(0,0)\n";
		str += "    rotateself(geom,N)             -- rotate 2D geom by N degrees counter-clock-wise with respect to self-center\n";
		str += "    rotateself(geom,N,'radian')    -- rotate 2D geom by N radians counter-clock-wise with respect to self-center\n";
		str += "    rotateself(geom,N,'degree')    -- rotate 2D geom by N degrees counter-clock-wise with respect to self-center\n";
		str += "    rotateat(geom,N,'radian',x,y)  -- rotate 2D geom by N radians counter-clock-wise with respect to point(x,y)\n";
		str += "    rotateat(geom,N,'degree',x,y)  -- rotate 2D geom by N degrees counter-clock-wise with respect to point(y,y)\n";
		str += "    affine(geom,a,b,d,e,dx,dy)     -- affine transformation on 2D geom\n";
		str += "    affine(geom,a,b,c,d,e,f,g,h,i,dx,dy,dz) -- affine transformation on 3D geom\n";
		str += "    voronoipolygons(mpoint)                 -- find Voronoi polygons from multipoints\n";
		str += "    voronoipolygons(mpoint,tolerance)       -- find Voronoi polygons from multipoints with tolerance\n";
		str += "    voronoipolygons(mpoint,tolerance,bbox)  -- find Voronoi polygons from multipoints with tolerance and bounding box\n";
		str += "    voronoilines(mpoint)                    -- find Voronoi lines from multipoints\n";
		str += "    voronoilines(mpoint,tolerance)          -- find Voronoi lines from multipoints with tolerance\n";
		str += "    voronoilines(mpoint,tolerance,bbox)     -- find Voronoi lines from multipoints with tolerance and bounding box\n";
		str += "    delaunaytriangles(mpoint)               -- find Delaunay triangles from multipoints\n";
		str += "    delaunaytriangles(mpoint,tolerance)     -- find Delaunay triangles from multipoints with tolerance\n";
		str += "    geojson(geom)                  -- GeoJSON string of geom\n";
		str += "    geojson(geom,N)                -- GeoJSON string of geom, receiving maximum of N points (default 3000)\n";
		str += "    geojson(geom,N,n)              -- GeoJSON string, receiving maximum of N points, n samples on 2D vector shapes\n";
		str += "    tomultipoint(geom)             -- converting geom to multipoint\n";
		str += "    tomultipoint(geom,N)           -- converting geom to multipoint. N is number of points for vector shapes\n";
		str += "    wkt(geom)                      -- Well Known Text of geom\n";
		str += "    minimumboundingcircle(geom)    -- minimum bounding circle of 2D geom\n";
		str += "    minimumboundingsphere(geom)    -- minimum bounding sphere of 3D geom\n";
		str += "    isonleft(geom1,geom2)          -- check if geom1 is on the left of geom2 (point and linear objects)\n";
		str += "    isonright(geom1,geom2)         -- check if geom1 is on the right of geom2 (point and linear objects)\n";
		str += "    leftratio(geom1,geom2)         -- ratio of geom1 on the left of geom2 (point and linear objects)\n";
		str += "    rightratio(geom1,geom2)        -- ratio of geom1 on the right of geom2 (point and linear objects)\n";
		str += "    knn(geom,point,K)              -- K-nearest neighbors in geom of point\n";
		str += "    knn(geom,point,K,min,max)      -- K-nearest neighbors in geom of point within maximum and mininum distance\n";
		str += "    metricn(geom)                  -- metrics of vector shapes\n";
		str += "    metricn(geom,N)                -- metric of N-th point. If vector shape, N-th metric\n";
		str += "    metricn(geom,N,m)              -- metric of N-th point, m-th metric. 1-based\n";
		str += "\n";
		str += "Example:\n";
		str += "select sum(amt) as amt_sum from sales limit 3;\n";
		str += "select cos(lat), sin(lon) from map limit 3;\n";
		str += "select tan(lat+sin(lon)), cot(lat^2+lon^2) from map limit 3;\n";
		str += "select uid, uid+addr, length(uid+addr)  from user limit 3;\n";
		str += "select price/2.0 + 1.25 as newprice, lead*1.25 - 0.3 as newlead from plan limit 3;\n";
		str += "select * from tm where dt < time() - tomicrosecond('1D');\n";
		str += "select angle(c1,c2) from g3 where id < 100;\n";
		str += "select buffer(col2, 'distance=symmetric:20,join=round:20,end=round') as buf from g2;\n";
		str += "select perimeter(squares) as perim from myshape;\n";
		str += "select ringn(poly, 2) as ring2 from myshape;\n";
		str += "select interpolate(lstr, 0.5) as midpoint from myline;\n";
		str += "select linesubstring(lstr, 0.2, 0.8) as midseg from myline where a=100;\n";
		str += "select rotate(lstr, 30, 'degree' ) as rot from myline where a=100;\n";
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
		str += "alter table TABLE rename OLDCOL to NEWCOL;\n";
		str += "alter table TABLE set COL:ATTR to VALUE;\n";
		str += "alter table TABLE add tick(TICK:RERENTION);\n";
		str += "alter table TABLE drop tick(TICK);\n";
		str += "alter table TABLE retention RERENTION;\n";
		str += "\n";
		str += "Add a new column in table TABLE.\n";
		str += "Rename a key column name in table TABLE.\n";
		str += "Rename a value column name in table TABLE.\n";
		str += "Set an attribute ATTR of column COL to value VALUE.\n";
		str += "Add a new tick to a timeseries table.\n";
		str += "Drop a tick from a timeseries table.\n";
		str += "Change the retention of a timeseries table.\n";
		str += "\n";
		str += "Example:\n";
		str += "alter table mytable add zipcode char(6);\n";
		str += "alter table mytable rename mykey to userid;\n";
		str += "alter table mytable rename col2 to col3;\n";
		str += "alter table mytable set sq:srid to 4326;\n";
		str += "alter table traffic add tick(3M:24M);\n";
		str += "alter table traffic drop tick(3M);\n";
		str += "alter table traffic retention 12M;\n";
		str += "alter table traffic retention 0;\n";
		str += "alter table traffic@1h retention 24h;\n";
		str += "alter table traffic@1M retention 24M;\n";
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
		str += "shell command;\n";
		str += "Execute shell command";
		str += "\n";
		str += "Example:\n";
		str += "!cat my.sql;\n";
		str += "shell ls -l /tmp;\n";
		str += "(The above shell commands are executed.)\n";
		str += "\n";
	} else {
		str += Jstr("Command not supported [") + cmd + "]\n";
	}

	// str += "\n";

	sendMessageLength( req, str.c_str(), str.size(), "I_" );
}

void JagDBServer::showCurrentDatabase( JagCfg *cfg, const JagRequest &req, const Jstr &dbname )
{
	Jstr res = dbname;
	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showDatabases( JagCfg *cfg, const JagRequest &req )
{
	Jstr res;
	res = JagSchema::getDatabases( cfg, req.session->replicateType );
	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showCurrentUser( JagCfg *cfg, const JagRequest &req )
{
	Jstr res = req.session->uid;
	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showClusterStatus( const JagRequest &req )
{
	// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
	char buf[1024];
	Jstr res = getClusterOpInfo( req );
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
	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showTools( const JagRequest &req )
{
	Jstr res;
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

	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showDatacenter( const JagRequest &req )
{
	Jstr fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
		return;
	}

	Jstr res, cont;
	JagFileMgr::readTextFile( fpath, cont );
	res = cont;

	JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showDatabases( JagCfg *cfg, const JagRequest &req )
{
	Jstr res;
	res = JagSchema::getDatabases( cfg, req.session->replicateType );
	JagStrSplit sp(res, '\n', true);
	if ( sp.length() < 1 ) {
		sendMessageLength( req, " ", 1, "KV" );
		return;
	}

	for ( int i = 0; i < sp.length(); ++i ) {
		JagRecord rec;
		rec.addNameValue("TABLE_CAT", sp[i].c_str() );
		sendMessageLength( req, rec.getSource(), rec.getLength(), "KV" );
    }
}

void JagDBServer::showTables( const JagRequest &req, const JagParseParam &pparam, 
							  const JagTableSchema *tableschema, 
							  const Jstr &dbname, int objType )
{
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexesLabel( objType, dbname, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;

	// printf("s7382 showTables %s\n", res.c_str() );

	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showTables( const JagRequest &req, const JagTableSchema *tableschema, const Jstr &dbname )
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
	sendMessageLength( req, buf, 5+len, "FH" );

	for ( i = 0; i < vec->size(); ++i ) {
		res = (*vec)[i];
		JagStrSplit split( res.c_str(), '.' );
		JagRecord rec;
		rec.addNameValue( "TABLE_CAT", split[0].c_str() );
		rec.addNameValue( "TABLE_SCHEM", split[0].c_str() );
		rec.addNameValue( "TABLE_NAME", split[1].c_str() );
		rec.addNameValue( "TABLE_TYPE", "TABLE" );

		sendMessageLength( req, rec.getSource(), rec.getLength(), "KV" );
		// KV records
	}
	if ( vec ) delete vec;
	vec = NULL;

}

void JagDBServer::showIndexes( const JagRequest &req, const JagParseParam &pparam, 
							   const JagIndexSchema *indexschema, const Jstr &dbtable )
{
	JagVector<AbaxString> *vec = indexschema->getAllTablesOrIndexes( dbtable, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;
	vec = NULL;

	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::showAllIndexes( const JagRequest &req,  const JagParseParam &pparam, const JagIndexSchema *indexschema, 
								const Jstr &dbname )
{
	JagVector<AbaxString> *vec = indexschema->getAllIndexes( dbname, pparam.like );
	int i;
	AbaxString res;
	for ( i = 0; i < vec->size(); ++i ) {
		res += (*vec)[i] + "\n";
	}
	if ( vec ) delete vec;
	vec = NULL;

	sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

void JagDBServer::_showIndexes( const JagRequest &req, const JagIndexSchema *indexschema, const Jstr &dbtable )
{
	sendMessageLength( req, " ", 1, "KV" );
}

void JagDBServer::showTask( const JagRequest &req )
{
	Jstr str;
	char buf[1024];
	const AbaxPair<AbaxLong,AbaxString> *arr = _taskMap->array();
	jagint len = _taskMap->arrayLength();
	sprintf( buf, "%14s  %20s  %16s  %16s  %16s %s\n", "TaskID", "ThreadID", "User", "Database", "StartTime", "Command");
	str += Jstr( buf );
	sprintf( buf, "------------------------------------------------------------------------------------------------------------\n");
	str += Jstr( buf );

	for ( jagint i = 0; i < len; ++i ) {
		if ( ! _taskMap->isNull( i ) ) {
			const AbaxPair<AbaxLong,AbaxString> &pair = arr[i];
			JagStrSplit sp( pair.value.c_str(), '|' );
			// "threadID|userid|dbname|timestamp|query"
			sprintf( buf, "%14lu  %20s  %16s  %16s  %16s %s\n", 
				pair.key, sp[0].c_str(), sp[1].c_str(), sp[2].c_str(), sp[3].c_str(), sp[4].c_str() );
			str += Jstr( buf );
		}
	}

	sendMessageLength( req, str.c_str(), str.size(), "I_" );
}

Jstr JagDBServer::describeTable( int inObjType, const JagRequest &req, 
								const JagTableSchema *tableschema, const Jstr &dbtable, 
								bool showDetail, bool showCreate, bool forRollup, const Jstr &tserRetain )
{
	const JagSchemaRecord *record = tableschema->getAttr( dbtable );
	if ( ! record ) return "";

	Jstr tname;
	Jstr retain;
	if ( forRollup ) {
		if ( tserRetain.containsChar('_') ) {
			JagStrSplit sp2( tserRetain, '_');
			tname = sp2[0];
			retain = sp2[1];
		} else {
			tname = tserRetain;
			retain = "0";
		}
	}

	Jstr res;
	Jstr type, dbcol, defvalStr;
	char buf[16];
	char spare4;
	int  offset, len;
	bool enteredValue = false;

	int objtype = tableschema->objectType ( dbtable );
	if ( JAG_TABLE_TYPE == inObjType ) {
		if ( objtype != JAG_TABLE_TYPE && objtype != JAG_MEMTABLE_TYPE ) {
			return "";
		}
	} else if ( JAG_CHAINTABLE_TYPE == inObjType ) {
		if ( objtype != JAG_CHAINTABLE_TYPE ) {
			return "";
		}
	} 

	if ( showCreate ) { res = Jstr("create "); } 
	if ( JAG_MEMTABLE_TYPE == objtype ) {
		res += Jstr("memtable "); 
	} else if ( JAG_CHAINTABLE_TYPE == objtype ) {
		res += Jstr("chain "); 
	} else {
		res += Jstr("table "); 
	}

	Jstr systser;
	if ( record->hasTimeSeries( systser ) ) {
		if ( ! forRollup ) {
			Jstr inputTser = record->translateTimeSeriesBack( systser );
			Jstr retention = record->timeSeriesRentention();
			if ( retention == "0" ) {
				res += Jstr("timeseries(") + inputTser + ") " + dbtable+"\n";
			} else {
				res += Jstr("timeseries(") + inputTser + "|" + retention + ") " + dbtable+"\n";
			}
		} else {
			res += Jstr("timeseries(0|") + retain + ") " + dbtable + "@" + tname + "\n";
		}
	} else {
		if ( strchr( dbtable.s(), '@' ) ) {
			Jstr retention = record->timeSeriesRentention();
			if ( retention == "0" ) {
				res += dbtable + "\n";
			} else {
				res += dbtable + "|" + retention + "\n";
			}
		} else {
			res += dbtable + "\n";
		}
	}

	res += Jstr("(\n");
	res += Jstr("  key:\n");

	int sz = record->columnVector->size();
	bool isRollupCol;
	bool doConvertSec = false;
	if ( forRollup ) { doConvertSec = true; } // first time do conversion
	bool  doneConvertSec;
	Jstr colStr;
	Jstr colName;
	Jstr colType;
	for (int i = 0; i < sz; i++) {
		colName = (*(record->columnVector))[i].name.c_str();
		colType = (*(record->columnVector))[i].type;

		colStr = "";
		if ( ! showDetail ) {
			if ( *((*(record->columnVector))[i].spare+6) == JAG_SUB_COL ) {
				continue;
			}

			if ( 0==strncmp( (*(record->columnVector))[i].name.c_str(), "geo:", 4 ) ) {
				// geo:*** ommit
				continue;
			}

		} else {
			if ( *((*(record->columnVector))[i].spare+6) == JAG_SUB_COL ) {
				//res += Jstr("  ");
				colStr += Jstr("  ");
			}
		}

		if ( showCreate && (*(record->columnVector))[i].name == "spare_" ) {
			continue;
		}

		if ( *((*(record->columnVector))[i].spare+7) == JAG_ROLL_UP ) {
			isRollupCol = true;
		} else {
			isRollupCol = false;
		}


		if ( !enteredValue && *((*(record->columnVector))[i].spare) == JAG_C_COL_VALUE ) {
			enteredValue = true;
			res += Jstr("  value:\n");
		}

		// if forRollup, skip the non-rollup columns
		if ( enteredValue && forRollup && ! isRollupCol ) {
			continue;
		}

		colStr += Jstr("    ");
		colStr += colName + " ";

		offset = (*(record->columnVector))[i].offset;
		len = (*(record->columnVector))[i].length;
		dbcol = dbtable + "." + (*(record->columnVector))[i].name.c_str();

		if ( *((*(record->columnVector))[i].spare+5) == JAG_KEY_MUTE ) {
			colStr += "mute ";
		}

		if ( isRollupCol && ! dbtable.containsChar('@') ) {
			colStr += "rollup ";
		}

		colStr += fillDescBuf( tableschema, (*(record->columnVector))[i], dbtable, doConvertSec, doneConvertSec );
		spare4 =  *((*(record->columnVector))[i].spare+4);

		if ( JAG_CREATE_DEFDATE == spare4 || JAG_CREATE_DEFDATETIMESEC == spare4 ||
			 JAG_CREATE_DEFDATETIMEMILL == spare4 ||
		     JAG_CREATE_DEFDATETIME == spare4 || JAG_CREATE_DEFDATETIMENANO == spare4 ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_DEFUPDATE_DATE == spare4 || JAG_CREATE_DEFUPDATE_DATETIMESEC == spare4 || 
					JAG_CREATE_DEFUPDATE_DATETIMEMILL == spare4 ||
				    JAG_CREATE_DEFUPDATE_DATETIME == spare4 || JAG_CREATE_DEFUPDATE_DATETIMENANO == spare4 ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_UPDATE_DATE == spare4 || JAG_CREATE_UPDATE_DATETIMESEC == spare4 || 
					JAG_CREATE_UPDATE_DATETIMEMILL == spare4 ||
		            JAG_CREATE_UPDATE_DATETIME == spare4 || JAG_CREATE_UPDATE_DATETIMENANO == spare4  ) {
			colStr += " ON UPDATE CURRENT_TIMESTAMP";
		} else if ( JAG_CREATE_DEFINSERTVALUE == spare4 ) {
			defvalStr = "";
			tableschema->getAttrDefVal( dbcol, defvalStr );
			if ( type == JAG_C_COL_TYPE_DBIT ) {
				colStr += Jstr(" DEFAULT b") + defvalStr.c_str();
			} else {
				if ( *((*(record->columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM[0] ) {
					JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
					colStr += Jstr(" DEFAULT ") + sp[sp.length()-1].c_str();
				} else {
					colStr += Jstr(" DEFAULT ") + defvalStr.c_str();
				}
			}
		}

		if ( showDetail ) {
			colStr += Jstr(" ") + intToStr(offset) + ":" + intToStr(len);
			#if 1
			sprintf(buf, "  %03d", i+1 );
			colStr += Jstr(buf);
			#endif
		}

		colStr += ",\n";
		res += colStr;

		// append stat cols
		/////////////////////////////////////////////////////
    	if ( forRollup && ! dbtable.containsChar('@') ) {
			if ( (*(record->columnVector))[i].iskey ) {
				continue;
			}

    		if ( ! isInteger( colType ) && ! isFloat(colType ) ) {
				continue;
			}

			colStr = "";
    		if ( isInteger( colType ) ) {
    			colStr += Jstr("    ") + colName + "::sum bigint,\n";
			} else {
    			colStr += Jstr("    ") + colName + "::sum double(20.3),\n";
			}
			Jstr ct = getTypeStr( colType ); 
    		colStr += Jstr("    ") + colName + "::min " + ct + ",\n";
    		colStr += Jstr("    ") + colName + "::max " + ct + ",\n";
    		colStr += Jstr("    ") + colName + "::avg " + ct + ",\n";
    		colStr += Jstr("    ") + colName + "::var " + ct + ",\n";

			res += colStr;
    	}
	} // end of record->columnVector

	if ( forRollup ) {
		res += "    counter bigint default '1' ";
	}

	res += Jstr(");\n");
	return res;
}

void JagDBServer::sendValueData( const JagParseParam &pparm, const JagRequest &req )
{
	if ( ! pparm._lineFile ) {
		return;
	}

	Jstr line;
	pparm._lineFile->startRead();
	while ( pparm._lineFile->getLine( line ) ) {
		sendMessageLength( req, line.c_str(), line.size(), "JS" );
	}
}

void JagDBServer::_describeTable( const JagRequest &req, const Jstr &dbtable, int keyOnly )
{
	JagTableSchema *tableschema =  getTableSchema( req.session->replicateType );
	Jstr cols = "TABLE_CAT|TABLE_SCHEM|TABLE_NAME|COLUMN_NAME|COLUMN_NAME|DATA_TYPE|TYPE_NAME|COLUMN_SIZE|DECIMAL_DIGITS|NULLABLE|KEY_SEQ";
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
	sendMessageLength( req, buf5, 5+len, "FH" );


	const JagSchemaRecord *record = tableschema->getAttr( dbtable );
	if ( ! record ) return;
	char buf[128];
	Jstr  type;
	JagStrSplit split( dbtable, '.' );
	Jstr dbname = split[0];
	Jstr tabname = split[1];
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
		} else if ( type == JAG_C_COL_TYPE_LONGDOUBLE ) {
			rec.addNameValue("DATA_TYPE", "9"); // longdouble
			rec.addNameValue("TYPE_NAME", "LONGDOUBLE");
		} else if ( type == JAG_C_COL_TYPE_TIME ) {
			rec.addNameValue("DATA_TYPE", "92"); // time
			rec.addNameValue("TYPE_NAME", "TIME");
		} else if ( type == JAG_C_COL_TYPE_DATETIMESEC ||  type == JAG_C_COL_TYPE_DATETIME
		            || type == JAG_C_COL_TYPE_DATETIMEMILL || type == JAG_C_COL_TYPE_DATETIMENANO ) {
			rec.addNameValue("DATA_TYPE", "93"); // time
			rec.addNameValue("TYPE_NAME", "DATETIME");
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMP || type == JAG_C_COL_TYPE_TIMESTAMPSEC
		            || type == JAG_C_COL_TYPE_TIMESTAMPMILL || type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
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
		} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
			rec.addNameValue("DATA_TYPE", "98"); // time
			rec.addNameValue("TYPE_NAME", "DATETIMENANO");
		} else if ( type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
			rec.addNameValue("DATA_TYPE", "99"); // time
			rec.addNameValue("TYPE_NAME", "TIMESTAMPNANO");
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

		sendMessageLength( req, rec.getSource(), rec.getLength(), "KV" );
	}
}

Jstr JagDBServer::describeIndex( bool showDetail, const JagRequest &req, 
							     const JagIndexSchema *indexschema, 
								 const Jstr &dbname, const Jstr &indexName, Jstr &reterr,
								 bool showCreate, bool forRollup, const Jstr &tserRetain )
{
	Jstr tab = indexschema->getTableName( dbname, indexName );
	if ( tab.size() < 1 ) {
		reterr = Jstr("E12380 table or index [") + indexName + "] not found";
		return "";
	}

	Jstr tname;
	Jstr retain;
	if ( forRollup ) {
		JagStrSplit sp2( tserRetain, '_');
		tname = sp2[0];
		retain = sp2[1];
	}

	Jstr dbtabidx = dbname + "." + tab + "." + indexName, dbcol;
	Jstr schemaText = indexschema->readSchemaText( dbtabidx );
	Jstr defvalStr;

	if ( schemaText.size() < 1 ) {
		reterr = Jstr("E12080 Index ") + indexName + " not found";
		return "";
	}

	JagSchemaRecord record;
	record.parseRecord( schemaText.c_str() );

	Jstr res;
	Jstr type;
	int len, offset;
	bool enteredValue = false;
	bool doneConvertSec;

	if  ( showCreate ) {
		res = "create ";
	}

	int isMemIndex = indexschema->isMemTable( dbtabidx );
	if ( isMemIndex ) {
		res += Jstr("memindex ") + dbtabidx + "\n";
	} else {
		if ( forRollup ) {
			res += Jstr("index ") + dbtabidx + "@" + tname + "\n";
		} else {
			res += Jstr("index " ) + dbtabidx + "\n";
		}
	}

	if ( forRollup ) {
		res += Jstr(" on ") + tab + "@" + tname + " ";
	}

	res += Jstr("(\n");
	res += Jstr("  key:\n");
	int sz = record.columnVector->size();
	char spare4, spare7;
	Jstr colName, colType, colStr;
	for (int i = 0; i < sz; i++) {
		colStr = "";
        colName = (*(record.columnVector))[i].name.c_str();
        colType = (*(record.columnVector))[i].type;

		if ( ! showDetail ) {
			if ( *((*(record.columnVector))[i].spare+6) == JAG_SUB_COL ) {
				continue;
			}
		} else {
			if ( *((*(record.columnVector))[i].spare+6) == JAG_SUB_COL ) {
				colStr += Jstr("  ");
			}
		}

		if ( !enteredValue && *((*(record.columnVector))[i].spare) == JAG_C_COL_VALUE ) {
			enteredValue = true;
			colStr += Jstr("  value:\n");
		}


		spare7 = *((*(record.columnVector))[i].spare+7);
		if ( ( ! (*(record.columnVector))[i].iskey ) && forRollup && ( JAG_ROLL_UP != spare7 ) ) {
			continue;
		}

		colStr += Jstr("    ");
		colStr += colName + " ";
		offset = (*(record.columnVector))[i].offset;
		len = (*(record.columnVector))[i].length;

		if ( forRollup ) {
			colStr += fillDescBuf( indexschema, (*(record.columnVector))[i], dbtabidx, true, doneConvertSec );
		} else {
			Jstr descbuf = fillDescBuf( indexschema, (*(record.columnVector))[i], dbtabidx, false, doneConvertSec );
			colStr += descbuf;
		}


		dbcol = dbtabidx + "." + colName;
		spare4 = *((*(record.columnVector))[i].spare+4);
		if ( spare4 == JAG_CREATE_DEFDATETIME || spare4 == JAG_CREATE_DEFDATE ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_DEFDATETIMESEC || spare4 == JAG_CREATE_DEFDATETIMENANO || spare4 == JAG_CREATE_DEFDATETIMEMILL ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_DEFUPDATE_DATETIME || spare4 == JAG_CREATE_DEFUPDATE_DATE ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_DEFUPDATE_DATETIMENANO || spare4 == JAG_CREATE_DEFUPDATE_DATETIMESEC 
		            || spare4 == JAG_CREATE_DEFUPDATE_DATETIMEMILL ) {
			colStr += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_UPDATE_DATETIME || spare4 == JAG_CREATE_UPDATE_DATE ) {
			colStr += " ON UPDATE CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_UPDATE_DATETIMESEC || spare4 == JAG_CREATE_UPDATE_DATETIMENANO 
					|| spare4 == JAG_CREATE_UPDATE_DATETIMEMILL ) {
			colStr += " ON UPDATE CURRENT_TIMESTAMP";
		} else if ( spare4 == JAG_CREATE_DEFINSERTVALUE ) {
			defvalStr = "";
			indexschema->getAttrDefVal( dbcol, defvalStr );
			if ( type == JAG_C_COL_TYPE_DBIT ) {
				colStr += Jstr(" DEFAULT b") + defvalStr.c_str();
			} else {
				if ( *((*(record.columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM[0]  ) {
					JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
					colStr += Jstr(" DEFAULT ") + sp[sp.length()-1].c_str();
				} else {
					colStr += Jstr(" DEFAULT ") + defvalStr.c_str();
				}
			}
		}

		if ( showDetail ) {
			colStr += Jstr(" ") + intToStr(offset) + ":" + intToStr(len);
		}
		
		//if ( i < sz-1 ) { colStr += ",\n"; } else { colStr += "\n"; }
		colStr += ",\n";
		res += colStr;

		if ( forRollup ) {
			if ( (*(record.columnVector))[i].iskey ) {
				continue;
			}
            if ( ! isInteger( colType ) && ! isFloat( colType ) ) {
                continue;
            }

            colStr = "";
			if ( isInteger( colType ) ) {
            	colStr += Jstr("    ") + colName + "::sum bigint,\n";
			} else {
            	colStr += Jstr("    ") + colName + "::sum longdouble(20.3),\n";
			}

			Jstr ct = getTypeStr( colType ); 
            colStr += Jstr("    ") + colName + "::min " + ct + ",\n";
            colStr += Jstr("    ") + colName + "::max " + ct + ",\n";
            colStr += Jstr("    ") + colName + "::avg " + ct + ",\n";
            colStr += Jstr("    ") + colName + "::var " + ct + ",\n";
            res += colStr;
		}
	}

	res += Jstr(");\n");
	return res;
	// sendMessageLength( req, res.c_str(), res.size(), "DS" );
}

// sequential
void JagDBServer::makeTableObjects( bool doRestoreInserBufferMap )
{
	AbaxString dbtab;
	JagRequest req;
	JagSession session;
	req.session = &session;
	session.servobj = this;
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
	tableschema = _tableschema;
	JagVector<AbaxString> *vec = _tableschema->getAllTablesOrIndexes( "", "" );

	req.session->replicateType = 0;
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = _objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
											 pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) {
			ptab->buildInitIndexlist();
		}

		if ( ptab ) _objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
												   pparam.objectVec[0].tableName, req.session->replicateType, 0 );
	}
	if ( vec ) delete vec;

	///////////////  1
	req.session->replicateType = 1;
	tableschema = _prevtableschema;
	vec = _prevtableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = _objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
											 pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) {
			ptab->buildInitIndexlist();
		}
		if ( ptab ) _objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
												   pparam.objectVec[0].tableName, req.session->replicateType, 0 );
	}
	delete vec;

	///////////////   2
	req.session->replicateType = 2;
	tableschema = _nexttableschema;
	vec = _nexttableschema->getAllTablesOrIndexes( "", "" );
	for ( int i = 0; i < vec->size(); ++i ) {
		dbtab = (*vec)[i];
		JagStrSplit split(  dbtab.c_str(), '.' );
		pparam.objectVec[0].dbName = split[0];
		pparam.objectVec[0].tableName = split[1];
		ptab = _objectLock->writeLockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
													 pparam.objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
		if ( ptab ) {
			ptab->buildInitIndexlist();
		}
		if ( ptab ) _objectLock->writeUnlockTable( JAG_CREATETABLE_OP, pparam.objectVec[0].dbName, 
												   pparam.objectVec[0].tableName, req.session->replicateType, 0 );
	}

	time_t  t2 = time(NULL);
	int min = (t2-t1)/60;
	int sec = (t2-t1) % 60;
	raydebug( stdout, JAG_LOG_LOW, "Initialized all %d tables in %d minutes %d seconds\n", vec->size(), min, sec );
	delete vec;

}

// static
void *JagDBServer::monitorRemoteBackup( void *ptr )
{
	JagPass *jp = (JagPass*)ptr;
	int period;
	Jstr bcastCmd;
	JagFixString data;
	JagRequest req;

	while ( 1 ) {
		period = jp->servobj->_cfg->getIntValue("REMOTE_BACKUP_INTERVAL", 30);
		jagsleep(period, JAG_SEC);
		jp->servobj->processRemoteBackup( "_ex_procremotebackup", req );
	}

	delete jp;
   	return NULL;
}

// static
void *JagDBServer::monitorTimeSeries( void *ptr )
{
	JagPass *jp = (JagPass*)ptr;
	int period;

	while ( 1 ) {
		period = jp->servobj->_cfg->getIntValue("TIMESERIES_CLEANUP_INTERVAL", 127);
		jagsleep(period, JAG_SEC);
		jp->servobj->trimTimeSeries();
	}

	delete jp;
   	return NULL;
}

// thread for local doRemoteBackup on host0
// static
void * JagDBServer::threadRemoteBackup( void *ptr )
{
	JagPass *jp = (JagPass*)ptr;
	Jstr rmsg = Jstr("_serv_doremotebackup|") + jp->ip + "|" + jp->passwd;
	JagRequest req;
	jp->servobj->doRemoteBackup( rmsg.c_str(), req );
	delete jp;
	return NULL;
}

// Get localhost IP address. In case there are N IP for this host (many ether cards)
// it matches the right one in the primary host list
Jstr JagDBServer::getLocalHostIP( const Jstr &hostips )
{
	JagVector<Jstr> vec;
	JagNet::getLocalIPs( vec );
	
	raydebug( stdout, JAG_LOG_HIGH, "Local Interface IPs [%s]\n", vec.asString().c_str() );

	JagStrSplit sp( hostips, '|' );
	for ( int i = 0; i < sp.length(); ++i ) {
		if ( _listenIP.size() > 0 ) {
			if ( _listenIP == sp[i] ) {
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

Jstr JagDBServer::getBroadcastRecoverHosts( int replicateCopy )
{
	Jstr hosts;
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
		if ( pos1 >= 0 ) hosts += Jstr("|") + sp[pos1];
	} else if ( 3 == replicateCopy ) { 
		if ( pos1 >= 0 ) hosts += Jstr("|") + sp[pos1];
		if ( pos2 >= 0 ) hosts += Jstr("|") + sp[pos2];
		if ( pos3 >= 0 ) hosts += Jstr("|") + sp[pos3];
		if ( pos4 >= 0 ) hosts += Jstr("|") + sp[pos4];
	}

	raydebug(stdout, JAG_LOG_LOW, "broadcastRecoverHosts [%s]\n", hosts.c_str() );
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
int JagDBServer::mainInit()
{
    //int rc;
    //int len;
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
	JagSession session;
	req.session = &session;
	session.replicateType = 0;
	session.servobj = this;
	refreshSchemaInfo( session.replicateType, g_lastSchemaTime );
	//char *rp = getenv("DO_RECOVER");
	char *rp2 = getenv("NEW_DATACENTER");

	// recover un-flushed insert data
	if ( 1 ) {
		raydebug(stdout, JAG_LOG_LOW, "begin recover wallog ...\n");
		recoverWalLog();
		raydebug(stdout, JAG_LOG_LOW, "done recover wallog\n");
	}
	// resetWalLog(); // reopen

	// recover un-flushed dinsert data
	if ( 1 ) {
		raydebug(stdout, JAG_LOG_LOW, "begin recover dinsertlog ...\n");
		Jstr dinpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertactive.log";
		recoverDinsertLog( dinpath );
		dinpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertbackup.log";
		recoverDinsertLog( dinpath );
		raydebug(stdout, JAG_LOG_LOW, "end recover dinsertlog\n");
	}
	resetDinsertLog();

	// set recover flag, check to see if need to do drecover ( delta log recovery, if hard disk is ok )
	// or do crecover ( tar copy file, if hard disk is brand new )
	Jstr recoverCmd, bcasthosts = getBroadcastRecoverHosts( _faultToleranceCopy );
	_crecoverFpath = "";
	_prevcrecoverFpath = "";
	_nextcrecoverFpath = "";
	_walrecoverFpath = "";
	_prevwalrecoverFpath = "";
	_nextwalrecoverFpath = "";

	//int isCrecover = 0;

	// first broadcast crecover to get package
	if ( 1 ) {
		// ask to do crecover
		_dbConnector->broadcastSignal( "_serv_crecover", bcasthosts );
		recoveryFromTransferredFile();
		// wait for this socket to be ready to recv data from client
		// then request delta data from neighbor
		raydebug( stdout, JAG_LOG_LOW, "end broadcast\n" );
	}

	recoverRegSpLog(); // regular data and metadata redo
	
	// next, if is new datacenter, ask original datacenter to broadcast old data to here
	if ( rp2 ) {
		_newdcTrasmittingFin = 0;
		if ( _isGate ) requestSchemaFromDataCenter();
		raydebug( stdout, JAG_LOG_LOW, "end request from datacenter\n" );
	} else {
		_newdcTrasmittingFin = 1;
	}

	// create thread to monitor logged incoming insert/update/delete commands
	// also create and open delta logs for replicate
	JagFileMgr::makeLocalLogDir("cmd");
	JagFileMgr::makeLocalLogDir("delta");

	Jstr cs1 = _cfg->getValue("REMOTE_BACKUP_SERVER", "" );
	Jstr cs2 = _cfg->getValue("REMOTE_BACKUP_INTERVAL", "0" );
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

	if ( 1 ) {
    	pthread_t  threadmo;
		JagPass *jp = new JagPass();
		jp->servobj = this;
		raydebug( stdout, JAG_LOG_LOW, "Initializing thread for timeseries\n");
    	jagpthread_create( &threadmo, NULL, monitorTimeSeries, (void*)jp );
    	pthread_detach( threadmo );
	}


	return 1;
}

int JagDBServer::mainClose()
{
	pthread_mutex_destroy( &g_dbschemamutex );
	pthread_mutex_destroy( &g_flagmutex );
	pthread_mutex_destroy( &g_wallogmutex );
	pthread_mutex_destroy( &g_dlogmutex );
	pthread_mutex_destroy( &g_dinsertmutex );
	pthread_mutex_destroy( &g_datacentermutex );
}

// object method: make thread groups
int JagDBServer::makeThreadGroups( int threadGroups, int threadGroupNum )
{
	this->_threadGroupNum = threadGroupNum;
	pthread_t thr[threadGroups];
	for ( int i = 0; i < threadGroups; ++i ) {
		this->_groupSeq = i;
    	jagpthread_create( &thr[i], NULL, oneThreadGroupTask, (void*)this );
    	pthread_detach( thr[i] );
	}

	// printf("s4913 done makeThreadGroups\n");
}

// static method
void* JagDBServer::oneThreadGroupTask( void *servptr )
{
	JagDBServer *servobj = (JagDBServer*)servptr;
	jagint 		threadGroupNum = (jagint)servobj->_threadGroupNum;
    struct sockaddr_in cliaddr;
    socklen_t 	clilen;
	JAGSOCK 	connfd;
	//int     	timeout = 600;
	int     	lasterrno = -1;
	jagint     	toterrs = 0;
	
	++ servobj->_activeThreadGroups;

    listen( servobj->_sock, 300);

    for(;;) {
		servobj->_dumfd = dup2( jagopen("/dev/null", O_RDONLY ), 0 );
        clilen = sizeof(cliaddr);

		memset(&cliaddr, 0, clilen );
        connfd = accept( servobj->_sock, (struct sockaddr *)&cliaddr, ( socklen_t* )&clilen);

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

		if ( 0 == connfd ) {
			jagclose( connfd );
			jagsleep(1, JAG_SEC);
			continue;
		}

		//servobj->_clientSocketMap.addKeyValue( connfd, 1 );

		++ servobj->_connections;

		JagPass *passmo = newObject<JagPass>();
		passmo->sock = connfd;
		passmo->servobj = servobj;
		passmo->ip = inet_ntoa( cliaddr.sin_addr );

		oneClientThreadTask( (void*)passmo );  // loop inside

		if ( threadGroupNum > 0 && jagint(servobj->_activeClients) * 100 <= servobj->_activeThreadGroups * 40  ) {
			raydebug( stdout, JAG_LOG_LOW, "c9499 threadGroup=%l idle quit\n", threadGroupNum );
			break;
		}
	}
	
	-- servobj->_activeThreadGroups;
   	raydebug( stdout, JAG_LOG_LOW, "s2894 thread group=%l done, activeThreadGroups=%l\n", 
				threadGroupNum, (jagint)servobj->_activeThreadGroups );

	return NULL;
}


// auth user
// 0: error
// 1: OK
bool JagDBServer::doAuth( JagRequest &req, char *pmesg )
{
	int ipOK = 0;
	pthread_rwlock_rdlock( & _aclrwlock );
	if ( _allowIPList->size() < 1 && _blockIPList->size() < 1 ) {
		// no witelist and blocklist setup, pass
		ipOK = 1;
	} else {
		if ( _allowIPList->match( req.session->ip ) ) {
			if ( ! _blockIPList->match(  req.session->ip ) ) {
				ipOK = 1;
			}
		}
	}

	pthread_rwlock_unlock( &_aclrwlock );
	if ( ! ipOK ) {
    	sendMessage( req, "_END_[T=579|E=Error client IP address]", "ER" );
		raydebug( stdout, JAG_LOG_HIGH, "client IP (%s) is blocked\n", req.session->ip.c_str() );
		return false;
	}

	JagStrSplit sp(pmesg, '|' );
	if ( sp.length() < 9 ) {
		sendMessage( req, "_END_[T=20|E=Error Auth]", "ER" );
		return false;
	}

	Jstr uid = sp[1];
	Jstr encpass = sp[2];
	Jstr pass = JagDecryptStr( _privateKey, encpass );
	req.session->timediff = atoi( sp[3].c_str() );
	req.session->origserv = atoi( sp[4].c_str() );
	req.session->replicateType = atoi( sp[5].c_str() );
	req.session->drecoverConn = atoi( sp[6].c_str() );
	req.session->unixSocket = sp[7];
	req.session->samePID = atoi( sp[8].c_str() );
	req.session->cliPID = sp[8];
	req.session->uid = uid;
	req.session->passwd = pass;
	if ( getpid() == req.session->samePID ) req.session->samePID = 1;
	else req.session->samePID = 0;

	if ( ! _serverReady && ! req.session->origserv ) {
		sendMessage( req, "_END_[T=20|E=Server not ready, please try later]", "ER" );
		return false;
	}
	
	JagHashMap<AbaxString, AbaxString> hashmap;
	convertToHashMap( sp[7], '/', hashmap);
	AbaxString exclusive, clear, checkip;
	req.session->exclusiveLogin = 0;

	if ( uid == "admin" ) {
    	if ( hashmap.getValue("clearex", clear ) ) {
    		if ( clear == "yes" || clear == "true" || clear == "1" ) {
        		_exclusiveAdmins = 0;
        		raydebug( stdout, JAG_LOG_LOW, "admin cleared exclusive from %s\n", req.session->ip.c_str() );
			}
		}

		req.session->datacenter = false;
    	if ( hashmap.getValue("DATACENTER", clear ) ) {
    		if ( clear == "1" ) {
				req.session->datacenter = true;
        		// raydebug( stdout, JAG_LOG_LOW, "admin from datacenter\n" );
			}
		}

		req.session->dcfrom = 0;
		if ( hashmap.getValue("DATC_FROM", clear ) ) {
			if ( clear == "GATE" ) {
				req.session->dcfrom = JAG_DATACENTER_GATE;
			} else if ( clear == "HOST" ) {
				req.session->dcfrom = JAG_DATACENTER_HOST;
			}
		}

		req.session->dcto = 0;
		if ( hashmap.getValue("DATC_TO", clear ) ) {
			if ( clear == "GATE" ) {
				req.session->dcto = JAG_DATACENTER_GATE;
			} else if ( clear == "HOST" ) {
				req.session->dcto = JAG_DATACENTER_HOST;
			} else if ( clear == "PGATE" ) {
				req.session->dcto = JAG_DATACENTER_PGATE;
			}
		}

    	if ( hashmap.getValue("exclusive", exclusive ) ) {
    		if ( exclusive == "yes" || exclusive == "true" || exclusive == "1" ) {
				req.session->exclusiveLogin = 1;
				if ( req.session->replicateType == 0 ) {
					// main connection accept exclusive login, other backup connection ignore it
					if ( _exclusiveAdmins > 0 ) {
						sendMessage( req, "_END_[T=120|E=Failed to login in exclusive mode]", "ER" );
						return false;
					}
				}
        	}
    	}  
	}

 	JagUserID *userDB;
	if ( req.session->replicateType == 0 ) userDB = _userDB;
	else if ( req.session->replicateType == 1 ) userDB = _prevuserDB;
	else if ( req.session->replicateType == 2 ) userDB = _nextuserDB;
	// check password first
	bool isGood = false;
	int passOK = 0;
	if ( userDB ) {
		AbaxString dbpass = userDB->getValue( uid, JAG_PASS );
		char *md5 = MDString( pass.c_str() );
		AbaxString md5pass = md5;
		if ( md5 ) free( md5 );
		if ( dbpass == md5pass && dbpass.size() > 0 ) {
			isGood = true;
			passOK = 1;
		} else {
			passOK = -1;
       		//raydebug( stdout, JAG_LOG_HIGH, "dbpass=[%s] inmd5pass=[%s]\n", dbpass.c_str(), md5pass.c_str() );
		}
	} else {
		passOK = -2;
	}
	
	int tokenOK = 0;
	if ( ! isGood ) {
		AbaxString servToken;
    	if ( hashmap.getValue("TOKEN", servToken ) ) {
			if ( servToken == _servToken ) {
				isGood = true;
				tokenOK = 1;
			} else {
				tokenOK = -1;
       			raydebug( stdout, JAG_LOG_HIGH, "inservToken=[%s] _servToken=[%s]\n", 
						  servToken.c_str(), _servToken.c_str() );
			}
		}  else {
			tokenOK = -2;
		}
	}

	if ( ! isGood ) {
       	sendMessage( req, "_END_[T=20|E=Error password or token]", "ER" );
       	raydebug( stdout, JAG_LOG_LOW, "Connection from %s, Error password or TOKEN\n", req.session->ip.c_str() );
       	raydebug( stdout, JAG_LOG_LOW, "%s passOK=%d tokenOK=%d\n", req.session->ip.c_str(), passOK, tokenOK );
       	return false;
	}

	if ( uid == "admin" ) {
		if ( req.session->exclusiveLogin ) {
        	raydebug( stdout, JAG_LOG_LOW, "Exclusive admin connected from %s\n", req.session->ip.c_str() );
        	_exclusiveAdmins = 1;
		}
	}
	
	if ( ! req.session->origserv && 0 == req.session->replicateType ) {
    	raydebug( stdout, JAG_LOG_LOW, "user %s connected from %s\n", uid.c_str(), req.session->ip.c_str() );
	}


	Jstr oksid = "OK ";
	oksid += longToStr(  pthread_self() );
	oksid += Jstr(" 1 ") + _dbConnector->_nodeMgr->_sendAllNodes + " ";
	oksid += longToStr( _nthServer ) + " " + longToStr( _faultToleranceCopy );
	if ( req.session->samePID ) oksid += Jstr(" 1 ");
	else oksid += Jstr(" 0 ");
	oksid += longToStr( req.session->exclusiveLogin );
	oksid += Jstr(" ") + _cfg->getValue("HASHJOIN_TABLE_MAX_RECORDS", "100000");
	oksid += Jstr(" ") + intToStr( _isGate );

	sendMessageLength( req, oksid.c_str(), oksid.size(), "CD" );
	sendMessage( req, "_END_[T=20|E=]", "ED" );
	req.session->uid = uid;

	// set timer for session if recover connection is not 2 ( clean recover )
	if ( req.session->drecoverConn != 2 && !req.session->samePID ) {
		req.session->createTimer();
	}

	return true;
}

// method for make connection use only
bool JagDBServer::useDB( JagRequest &req, char *pmesg  )
{
	raydebug( stdout, JAG_LOG_HIGH, "useDB %s\n", pmesg );

	char *tok;
	char dbname[JAG_MAX_DBNAME+1];
	char *saveptr;

	tok = strtok_r( pmesg, " ", &saveptr );  // pmesg is modified!!
	tok = strtok_r( NULL, " ;", &saveptr );
	if ( ! tok ) {
		sendMessage( req, "_END_[T=20|E=Database empty]", "ER" );
	} else {
		if ( strlen( tok ) > JAG_MAX_DBNAME ) {
			sendMessage( req, "_END_[T=20|E=Database too long]", "ER" );
		} else {
    		strcpy( dbname, tok );
			tok = strtok_r( NULL, " ;", &saveptr );
			if ( tok ) {
				sendMessage( req, "_END_[T=20|E=Use database syntax error]", "ER" );
			} else {
				if ( 0 == strcmp( dbname, "test" ) ) {
					req.session->dbname = dbname;
					sendMessage( req, "Database changed", "OK" );
					sendMessage( req, "_END_[T=20|E=]", "ED" );
					return true;
				}
    			Jstr jagdatahome = _cfg->getJDBDataHOME( req.session->replicateType );
    			Jstr fpath = jagdatahome + "/" + dbname;
    			if ( 0 == jagaccess( fpath.c_str(), X_OK )  ) {
    				req.session->dbname = dbname;
    				sendMessage( req, "Database changed", "OK" );
    				sendMessage( req, "_END_[T=20|E=]", "ED" );
    			} else {
    				//sendMessage( req, "Database not found", "OK" );
    				//sendMessage( req, "_END_[T=40|E=Database not found]", "ED" );
    				// sendMessage( req, "_END_[T=40|E=Database not found]", "EE" );
    				sendMessage( req, "_END_[T=20|E=Database not found]", "ER" );
    			}
			}
		}
	}

	return true;
}

// method to refresh schemaMap and schematime for dbConnector
void JagDBServer::refreshSchemaInfo( int replicateType, jagint &schtime )
{
	Jstr schemaInfo;

	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;

	getTableIndexSchema( replicateType, tableschema, indexschema );

	Jstr obj;
	AbaxString recstr;

	// tables
	JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( "", "");
	if ( vec ) {
    	for ( int i = 0; i < vec->size(); ++i ) {
    		obj = (*vec)[i].c_str();
    		if ( tableschema->getAttr( obj, recstr ) ) {
    			schemaInfo += Jstr( obj.c_str() ) + ":" + recstr.c_str() + "\n";
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
    			schemaInfo += Jstr( obj.c_str() ) + ":" + recstr.c_str() + "\n"; 
    		}
    	}
    	if ( vec ) delete vec;
    	vec = NULL;
	}

	_dbConnector->_parentCliNonRecover->_schemaMapLock->writeLock( -1 );
	_dbConnector->_parentCliNonRecover->rebuildSchemaMap();
	_dbConnector->_parentCliNonRecover->updateSchemaMap( schemaInfo.c_str() );
	_dbConnector->_parentCliNonRecover->_schemaMapLock->writeUnlock( -1 );

	_dbConnector->_parentCli->_schemaMapLock->writeLock( -1 );
	_dbConnector->_parentCli->rebuildSchemaMap();
	_dbConnector->_parentCli->updateSchemaMap( schemaInfo.c_str() );
	_dbConnector->_parentCli->_schemaMapLock->writeUnlock( -1 );

	_dbConnector->_broadcastCli->_schemaMapLock->writeLock( -1 );
	_dbConnector->_broadcastCli->rebuildSchemaMap();
	_dbConnector->_broadcastCli->updateSchemaMap( schemaInfo.c_str() );
	_dbConnector->_broadcastCli->_schemaMapLock->writeUnlock( -1 );

	struct timeval now;
	gettimeofday( &now, NULL );
	schtime = now.tv_sec * (jagint)1000000 + now.tv_usec;
}

// Reload ACL list
void JagDBServer::refreshACL( int bcast )
{
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		return;
	}

	loadACL();

	if ( bcast ) {
		Jstr bcastCmd = "_serv_refreshacl|" + _allowIPList->_data+ "|" + _blockIPList->_data;
		_dbConnector->broadcastSignal( bcastCmd, "" );
	}
}

// Load ACL list
void JagDBServer::loadACL()
{
	Jstr fpath = jaguarHome() + "/conf/allowlist.conf";
	pthread_rwlock_wrlock(&_aclrwlock);
	if ( _allowIPList ) delete _allowIPList;
	_allowIPList = new JagIPACL( fpath );

	fpath = jaguarHome() + "/conf/blocklist.conf";
	if ( _blockIPList ) delete _blockIPList;
	_blockIPList = new JagIPACL( fpath );
	pthread_rwlock_unlock(&_aclrwlock);

}

// back up schema and other meta data
// seq should be every minute
void JagDBServer::doBackup( jaguint seq )
{
	// use _cfg
	Jstr cs = _cfg->getValue("LOCAL_BACKUP_PLAN", "" );
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
	Jstr  rec;
	Jstr  bcastCmd, bcasthosts;

	// copy meta data to 15min directory
	if ( ( seq%15 ) == 0 && sp.contains( "15MIN", rec ) ) {
		copyData( rec, false );
		bcastCmd = "_serv_copydata|" + rec + "|0";
		_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to hourly directory
	if ( ( seq % 60 ) == 0 && sp.contains( "HOURLY", rec ) ) {
		copyData( rec, false );
		bcastCmd = "_serv_copydata|" + rec + "|0";
		_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to daily directory
	if ( ( seq % 1440 ) == 0 && sp.contains( "DAILY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to weekly directory
	if ( ( seq % 10080 ) == 0 && sp.contains( "WEEKLY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 
	}

	// copy meta data to monthly directory
	if ( ( seq % 43200 ) == 0 && sp.contains( "MONTHLY", rec ) ) {
		copyData( rec );
		bcastCmd = "_serv_copydata|" + rec + "|1";
		_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 
	}
}

// back up schema and other meta data
#ifndef _WINDOWS64_
#include <sys/sysinfo.h>
void JagDBServer::writeLoad( jaguint seq )
{
	if ( ( seq%15) != 0 ) {
		return;
	}

	struct sysinfo info;
    sysinfo( &info );

	jagint usercpu, syscpu, idle;
	_jagSystem.getCPUStat( usercpu, syscpu, idle );

	Jstr userCPU = ulongToString( usercpu );
	Jstr sysCPU = ulongToString( syscpu );

	Jstr totrams = ulongToString( info.totalram / ONE_GIGA_BYTES );
	Jstr freerams = ulongToString( info.freeram / ONE_GIGA_BYTES );
	Jstr totswaps = ulongToString( info.totalswap / ONE_GIGA_BYTES );
	Jstr freeswaps = ulongToString( info.freeswap / ONE_GIGA_BYTES );
	Jstr procs = intToString( info.procs );

	jaguint diskread, diskwrite, netread, netwrite;
	JagNet::getNetStat( netread, netwrite );
	JagFileMgr::getIOStat( diskread, diskwrite );
	Jstr netReads = ulongToString( netread /ONE_GIGA_BYTES );
	Jstr netWrites = ulongToString( netwrite /ONE_GIGA_BYTES );
	Jstr diskReads = ulongToString( diskread  );
	Jstr diskWrites = ulongToString( diskwrite  );

	Jstr nselects = ulongToString( numSelects );
	Jstr ninserts = ulongToString( numInserts );
	Jstr nupdates = ulongToString( numUpdates );
	Jstr ndeletes = ulongToString( numDeletes );

	// printf("s3104 diskread=%lu diskwrite=%lu netread=%lu netwrite=%lu\n", diskread, diskwrite, netread, netwrite );

	Jstr rec = ulongToString( time(NULL) ) + "|" + Jstr(userCPU) + "|" + sysCPU;
	rec += Jstr("|") + totrams + "|" + freerams + "|" + totswaps + "|" + freeswaps + "|" + procs; 
	rec += Jstr("|") + diskReads + "|" + diskWrites + "|" + netReads + "|" + netWrites;
	rec += Jstr("|") + nselects + "|" + ninserts + "|" + nupdates + "|" + ndeletes;
	JagBoundFile bf( _perfFile.c_str(), 96 ); // 24 hours
	bf.openAppend();
	bf.appendLine( rec.c_str() );
	bf.close();
}
#else
// Windows
void JagDBServer::writeLoad( jaguint seq )
{
	if ( ( seq%15) != 0 ) {
		return;
	}

	jagint usercpu, syscpu, idlecpu;
	_jagSystem.getCPUStat( usercpu, syscpu, idlecpu );
	Jstr userCPU = ulongToString( usercpu );
	Jstr sysCPU = ulongToString( syscpu );

	jagint totm, freem, used; //GB
	JagSystem::getMemInfo( totm, freem, used );
	Jstr totrams = ulongToString( totm );
	Jstr freerams = ulongToString( freem );

	MEMORYSTATUSEX statex;
	Jstr totswaps = intToString( statex.ullTotalPageFile/ ONE_GIGA_BYTES );
	Jstr freeswaps = intToString( statex.ullAvailPageFile / ONE_GIGA_BYTES );
	Jstr procs = intToString( _jagSystem.getNumProcs() );

	Jstr nselects = ulongToString( numSelects );
	Jstr ninserts = ulongToString( numInserts );
	Jstr nupdates = ulongToString( numUpdates );
	Jstr ndeletes = ulongToString( numDeletes );

	jaguint diskread, diskwrite, netread, netwrite;
	JagNet::getNetStat( netread, netwrite );
	JagFileMgr::getIOStat( diskread, diskwrite );
	Jstr netReads = ulongToString( netread / ONE_GIGA_BYTES );
	Jstr netWrites = ulongToString( netwrite / ONE_GIGA_BYTES );
	Jstr diskReads = ulongToString( diskread  );
	Jstr diskWrites = ulongToString( diskwrite  );

	Jstr rec = ulongToString( time(NULL) ) + "|" + Jstr(userCPU) + "|" + sysCPU;
	rec += Jstr("|") + totrams + "|" + freerams + "|" + totswaps + "|" + freeswaps + "|" + procs; 
	rec += Jstr("|") + diskReads + "|" + diskWrites + "|" + netReads + "|" + netWrites;
	rec += Jstr("|") + nselects + "|" + ninserts + "|" + nupdates + "|" + ndeletes;
	JagBoundFile bf( _perfFile.c_str(), 96 ); // 24 hours
	bf.openAppend();
	bf.appendLine( rec.c_str() );
	bf.close();

}

#endif

// rec:  15MIN:OVERWRITE
void JagDBServer::copyData( const Jstr &rec, bool show )
{
	if ( rec.length() < 1 ) {
		return;
	}
	
	JagStrSplit sp( rec, ':' );
	if ( sp.length() < 1 ) {
		return;
	}

	Jstr dirname = makeLowerString( sp[0] );
	Jstr policy;
	policy = sp[1];

	Jstr tmstr = JagTime::YYYYMMDDHHMM();
	copyLocalData( dirname, policy, tmstr, show );
}

// < 0: error
// 0: OK
// dirname: 15min/hourly/daily/weekly/monthly or last   policy: "OVERWRITE" or "SNAPSHOT"
int JagDBServer::copyLocalData( const Jstr &dirname, const Jstr &policy, const Jstr &tmstr, bool show )
{
	if ( policy != "OVERWRITE" && policy != "SNAPSHOT" ) {
		return -1;
	}

    Jstr srcdir = jaguarHome() + "/data";
	if ( JagFileMgr::dirEmpty( srcdir ) ) {
		return -2;
	}

	char hname[80];
	gethostname( hname, 80 );

    // Jstr fileName = JagTime::YYYYMMDDHHMM() + "-" + hname;
    Jstr fileName = tmstr + "-" + hname;
    Jstr destdir = jaguarHome() + "/backup";
	destdir += Jstr("/") + dirname + "/" + fileName;

	JagFileMgr::makedirPath( destdir, 0700 );
	
	char cmd[2048];
	sprintf( cmd, "/bin/cp -rf %s/*  %s", srcdir.c_str(), destdir.c_str() );
	system( cmd );
	if ( show ) {
		raydebug( stdout, JAG_LOG_LOW, "Backup data from %s to %s\n", srcdir.c_str(), destdir.c_str() );
	}

	Jstr lastPath = jaguarHome() + "/backup/" + dirname + "/.lastFileName.txt";
	if ( policy == "OVERWRITE" ) {
		Jstr lastFileName;
		JagFileMgr::readTextFile( lastPath, lastFileName );
		if (  lastFileName.length() > 0 ) {
    		Jstr destdir = jaguarHome() + "/backup";
			destdir += Jstr("/") + dirname + "/" + lastFileName;
			sprintf( cmd, "/bin/rm -rf %s", destdir.c_str() );
			system( cmd );
		}
	}

	JagFileMgr::writeTextFile( lastPath, fileName );
	return 0;
}

void JagDBServer::doCreateIndex( JagTable *ptab, JagIndex *pindex )
{
	ptab->_indexlist.append( pindex->getIndexName() );
	if ( ptab->_darrFamily->getCount( ) < 1 ) {
		return;
	}

	ptab->formatCreateIndex( pindex );
}

// method to drop all tables and indexs under database "dbname"
void JagDBServer::dropAllTablesAndIndexUnderDatabase( const JagRequest &req, JagTableSchema *schema, const Jstr &dbname )
{	
	JagTable *ptab;
	Jstr dbobj, errmsg;
	JagVector<AbaxString> *vec = schema->getAllTablesOrIndexes( dbname, "" );
	if ( NULL == vec ) return;

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
			
			ptab = _objectLock->writeLockTable( pparam.opcode, pparam.objectVec[0].dbName,
												 pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			if ( ptab ) {
				//flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );
        		ptab->drop( errmsg );  //_darr is gone, raystat has cleaned up this darr
    			schema->remove( dbobj );
        		delete ptab; ptab = NULL;
				_objectLock->writeUnlockTable( pparam.opcode, pparam.objectVec[0].dbName,
												pparam.objectVec[0].tableName, req.session->replicateType, 1 );
			}
    	}

		delete vec;
		vec = NULL;
}

// method to drop all tables and indexs under all databases
void JagDBServer::dropAllTablesAndIndex( const JagRequest &req, JagTableSchema *schema )
{	
	JagTable *ptab;
	Jstr dbobj, errmsg;
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
			
			ptab = _objectLock->writeLockTable( pparam.opcode, pparam.objectVec[0].dbName,
												 pparam.objectVec[0].tableName, schema, req.session->replicateType, 1 );
			if ( ptab ) {
        		ptab->drop( errmsg );  //_darr is gone, raystat has cleaned up this darr
    			schema->remove( dbobj );
        		delete ptab; ptab = NULL;
				_objectLock->writeUnlockTable( pparam.opcode, pparam.objectVec[0].dbName,
												pparam.objectVec[0].tableName, req.session->replicateType, 1 );
			}
    	}
		delete vec;
		vec = NULL;
	}
}

// showusers
void JagDBServer::showUsers( const JagRequest &req )
{
	if ( req.session->uid != "admin" ) {
		sendMessage( req, "Only admin can get a list of user accounts", "OK" );
		return;
	}

	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;
	Jstr users;
	if ( uiddb ) {
		users = uiddb->getListUsers();
	}
	sendMessageLength( req, users.c_str(), users.length(), "OK" );
}

// ensure admin account is created
void JagDBServer::createAdmin()
{
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
	Jstr uid = "admin";
	Jstr pass = "jaguarjaguarjaguar";
	AbaxString dbpass = uiddb->getValue(uid, JAG_PASS );
	if ( dbpass.size() > 0 ) {
		return;
	}

	char *md5 = MDString( pass.c_str() );
	Jstr mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;

	uiddb->addUser(uid, mdpass, JAG_USER, JAG_WRITE );
	// printf("s8339 createAdmin uid=[%s] mdpass=[%s] rc=%d\n", uid.c_str(), mdpass.c_str(), rc );
}

// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
Jstr JagDBServer::getClusterOpInfo( const JagRequest &req )
{
	JagStrSplit srv( _dbConnector->_nodeMgr->_allNodes, '|' );
	int nsrv = srv.length(); 
	int dbs, tabs;
	numDBTables( dbs, tabs );

	// get opinfo on this server
	Jstr res;
	char buf[1024];
	sprintf( buf, "%d|%d|%d|%lld|%lld|%lld|%lld|%lld", nsrv, dbs, tabs, 
			(jagint)numSelects, (jagint)numInserts, 
			(jagint)numUpdates, (jagint)numDeletes, 
			_taskMap->size() );
	res = buf;

	// broadcast other server request info
	Jstr resp, bcasthosts;
	resp = _dbConnector->broadcastGet( "_serv_opinfo", bcasthosts ); 
	resp += res + "\n";
	//printf("s4820 broadcastGet(_serv_opinfo)  resp=[%s]\n", resp.c_str() );
	//fflush( stdout );

	JagStrSplit sp( resp, '\n', true );
	jagint sel, ins, upd, del, sess;
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
	Jstr dbs = JagSchema::getDatabases( _cfg, 0 );
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
	raydebug( stdout, JAG_LOG_LOW, "begin initObjects ...\n" );
	
	_tableschema = new JagTableSchema( this, 0 );
	_prevtableschema = new JagTableSchema( this, 1 );
	_nexttableschema = new JagTableSchema( this, 2 );

	_indexschema = new JagIndexSchema( this, 0 );
	_previndexschema = new JagIndexSchema( this, 1 );
	_nextindexschema = new JagIndexSchema( this, 2 );
	
	_userDB = new JagUserID( 0 );
	_prevuserDB = new JagUserID( 1 );
	_nextuserDB = new JagUserID( 2 );

	_userRole = new JagUserRole( 0 );
	_prevuserRole = new JagUserRole( 1 );
	_nextuserRole = new JagUserRole( 2 );

	raydebug( stdout, JAG_LOG_LOW, "end initObjects\n" );
	raydebug( stdout, JAG_LOG_HIGH, "Initialized schema objects\n" );
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
		exit( 51 );
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
	Jstr cs;
	cs = _cfg->getValue("IS_GATE", "no");
	if ( cs == "yes" || cs == "YES" || cs == "Yes" || cs == "Y" || cs == "true" ) {
		_isGate = 1;
		raydebug( stdout, JAG_LOG_LOW, "isGate is yes\n" );
	} else {
		_isGate = 0;
		raydebug( stdout, JAG_LOG_LOW, "isGate is no\n" );
	}

	int threads = _numCPUs*_cfg->getIntValue("CPU_SELECT_FACTOR", 4);
	// _selectPool = new JaguarThreadPool( threads );
	raydebug( stdout, JAG_LOG_LOW, "Select thread pool %d\n", threads );

	threads = _numCPUs*_cfg->getIntValue("CPU_PARSE_FACTOR", 2);
	// _parsePool = new JaguarThreadPool( threads );
	raydebug( stdout, JAG_LOG_LOW, "Parser thread pool %d\n", threads );

	cs = _cfg->getValue("JAG_LOG_LEVEL", "0");
	JAG_LOG_LEVEL = atoi( cs.c_str() );
	if ( JAG_LOG_LEVEL <= 0 ) JAG_LOG_LEVEL = 1;
	raydebug( stdout, JAG_LOG_LOW, "JAG_LOG_LEVEL=%d\n", JAG_LOG_LEVEL );

	_version = Jstr(JAG_VERSION);
	raydebug( stdout, JAG_LOG_LOW, "Server version is %s\n", _version.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "Server license is %s\n", PRODUCT_VERSION );

	_threadGroups = _faultToleranceCopy * _dbConnector->_nodeMgr->_numAllNodes * 15;
	if ( _isGate ) {
		cs = _cfg->getValue("INIT_EXTRA_THREADS", "100" );
	} else {
		cs = _cfg->getValue("INIT_EXTRA_THREADS", "50" );
	}
	_initExtraThreads = atoi( cs.c_str() );


	raydebug( stdout, JAG_LOG_LOW, "Thread Groups = %d\n", _threadGroups );
	raydebug( stdout, JAG_LOG_LOW, "Init Extra Threads = %d\n", _initExtraThreads );

	// write process ID
	Jstr logpath = jaguarHome() + "/log/jaguar.pid";
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

	_debugClient = 0;
	cs = _cfg->getValue("DEBUG_CLIENT", "no");
	if ( startWith( cs, 'y' ) ) {
		_debugClient =  1;
	} 
	raydebug( stdout, JAG_LOG_LOW, "DEBUG_CLIENT %s\n", cs.c_str() );

	_taskMap = new JagHashMap<AbaxLong,AbaxString>();
	_joinMap = new JagHashMap<AbaxString, AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>>>();
	_scMap = new JagHashMap<AbaxString, AbaxInt>();

	// read public and private keys
	int keyExists = 0;
	Jstr keyFile = jaguarHome() + "/conf/public.key";
	while ( ! keyExists ) {
		JagFileMgr::readTextFile( keyFile, _publicKey );
		if ( _publicKey.size() < 1 ) {
			raydebug( stdout, JAG_LOG_LOW, "Key file %s not found, wait ...\n", keyFile.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "Please execute createKeyPair program to generate it.\n");
			raydebug( stdout, JAG_LOG_LOW, "Once conf/public.key and conf/private.key are generated, server will use them.\n");
			jagsleep(10, JAG_SEC);
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
			jagsleep(10, JAG_SEC);
		} else {
			keyExists = 1;
		}
	}
}

// dinsert log command to the command log file
void JagDBServer::dinsertlogCommand( JagSession *session, const char *mesg, jagint len )
{
	// JAG_REDO_MSGLEN is 10, %010ld--> prepend 0, max 10 long digits 
	JAG_BLURT jaguar_mutex_lock ( &g_dinsertmutex ); JAG_OVER;
	if ( session->servobj->_dinsertCommandFile ) {
		fprintf( session->servobj->_dinsertCommandFile, "%d;%010ld%s", session->timediff, len, mesg );
	}
	jaguar_mutex_unlock ( &g_dinsertmutex );
}

// log command to the command wallog file
// spMode = 0 : special cmds, create/drop etc. 
// spMode = 1 : single regular cmds, update/delete etc.
// spMode = 2 : batch regular cmds, insert/cinsert/dinsert etc. 
void JagDBServer::logCommand( const JagParseParam *pparam, JagSession *session, const char *mesg, jagint msglen, int spMode )
{
	Jstr db = pparam->objectVec[0].dbName;
	Jstr tab = pparam->objectVec[0].tableName;
	if ( db.size() < 1 || tab.size() <1 ) { return; }

	JAG_BLURT jaguar_mutex_lock ( &g_wallogmutex ); JAG_OVER;

	Jstr fpath = _cfg->getWalLogHOME() + "/" + db + "." + tab + ".wallog";

	FILE *walFile = _walLogMap.ensureFile( fpath );
	if ( NULL == walFile ) {
		raydebug( stdout, JAG_LOG_LOW, "error open wallog %s\n", fpath.c_str() );
	} else {
		int isInsert;
		if ( 2==spMode ) isInsert = 1; else isInsert = 0;
		fprintf( walFile, "%d;%d;%d;%010ld%s",
		         session->replicateType, session->timediff, isInsert, msglen, mesg );
	    fflush( walFile );
	}
	JAG_BLURT jaguar_mutex_unlock ( &g_wallogmutex ); 
}

// log command to the recovery log file
// spMode = 0 : special cmds, create/drop etc. spMode = 1 : single regular cmds, update/delete etc.
// spMode = 2 : batch regular cmds, insert/cinsert/dinsert etc. 
void JagDBServer::regSplogCommand( JagSession *session, const char *mesg, jagint len, int spMode )
{
	// logformat
	// JAG_REDO_MSGLEN is 10, %010ld--> prepend 0, max 10 long digits 
	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	if ( 0 == spMode && session->servobj->_recoverySpCommandFile ) {
		fprintf( session->servobj->_recoverySpCommandFile, "%d;%d;0;%010ld%s",
			     session->replicateType, session->timediff, len, mesg );
		jagfdatasync( fileno(session->servobj->_recoverySpCommandFile ) );
	} else if ( 0 != spMode && session->servobj->_recoveryRegCommandFile ) {
		fprintf( session->servobj->_recoverySpCommandFile, "%d;%d;%d;%010ld%s",
			     session->replicateType, session->timediff, 2==spMode, len, mesg );
		if ( 2 == spMode ) {
			jagfdatasync( fileno(session->servobj->_recoveryRegCommandFile ) );
		}
	}
	jaguar_mutex_unlock ( &g_dlogmutex );
}

// mode: replicateType 012 0: YYY; 1: YYN; 2: YNY; 3: YNN; 4: NYY; 5: NYN; 6: NNY;
void JagDBServer::deltalogCommand( int mode, JagSession *session, const char *mesg, bool isBatch )
{	
	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	int rc, needSync = 0;
	Jstr cmd;
	FILE *f1 = NULL;
	FILE *f2 = NULL;
	
	if ( !isBatch ) { // not batch command, parse and rebuild cmd with dbname inside
		Jstr reterr;
		JagParseAttribute jpa( session->servobj, session->timediff, session->servobj->servtimediff, 
							   session->dbname, session->servobj->_cfg );
		JagParser parser( (void*)this );
		JagParseParam parseParam(&parser);
		rc = parser.parseCommand( jpa, mesg, &parseParam, reterr );
		if ( rc ) {
			cmd = parseParam.dbNameCmd;
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
			f1 = session->servobj->_delPrevRepCommandFile;
		} else if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriRepCommandFile;
		} 
	} else if ( 2 == mode ) {
		if ( 0 == session->replicateType ) {
			f1 = session->servobj->_delNextRepCommandFile;
		} else if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriRepCommandFile;
		} 
	} else if ( 3 == mode ) {
		if ( 0 == session->replicateType ) {
			f1 = session->servobj->_delPrevRepCommandFile;
			f2 = session->servobj->_delNextRepCommandFile;
		} 
	} else if ( 4 == mode ) {
		if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriCommandFile;
		} else if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriCommandFile;		
		} 
	} else if ( 5 == mode ) {
		if ( 1 == session->replicateType ) {
			f1 = session->servobj->_delPrevOriCommandFile;
			f2 = session->servobj->_delPrevOriRepCommandFile;
		}
	} else if ( 6 == mode ) {
		if ( 2 == session->replicateType ) {
			f1 = session->servobj->_delNextOriCommandFile;
			f2 = session->servobj->_delNextOriRepCommandFile;
		}
	}
	
	// deltalogformat
	if ( f1 ) {
		fprintf( f1, "%d;%d;%s\n", session->timediff, isBatch, cmd.c_str() );
	}
	if ( f2 ) {
		fprintf( f2, "%d;%d;%s\n", session->timediff, isBatch, cmd.c_str() );
	}
	if ( needSync ) {
		if ( f1 ) {
			jagfdatasync( fileno( f1 ) );
		}
		if ( f2 ) {
			jagfdatasync( fileno( f2 ) );
		}
		if ( 0 == session->replicateType && ! session->origserv ) {
			// if ( f1 || f2 ) JagStrSplitWithQuote split( mesg, ';' );
		}
	}

	jaguar_mutex_unlock ( &g_dlogmutex );
}

// method to recover delta log immediately after delta log written, if connection has already been recovered
void JagDBServer::onlineRecoverDeltaLog()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate
	Jstr fpaths;
	// use client to recover delta log
	if ( _delPrevOriCommandFile && JagFileMgr::fileSize(_actdelPOpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelPOpath;
		else fpaths += Jstr("|") + _actdelPOpath;
		jagfclose( _delPrevOriCommandFile );
		_delPrevOriCommandFile = NULL;
	}
	if ( _delPrevRepCommandFile && JagFileMgr::fileSize(_actdelPRpath) > 0 ) { 
		if ( fpaths.size() < 1 ) fpaths = _actdelPRpath;
		else fpaths += Jstr("|") + _actdelPRpath;
		jagfclose( _delPrevRepCommandFile );
		_delPrevRepCommandFile = NULL;
	}
	if ( _delPrevOriRepCommandFile && JagFileMgr::fileSize(_actdelPORpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelPORpath;
		else fpaths += Jstr("|") + _actdelPORpath;
		jagfclose( _delPrevOriRepCommandFile );
		_delPrevOriRepCommandFile = NULL;
	}
	if ( _delNextOriCommandFile && JagFileMgr::fileSize(_actdelNOpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNOpath;
		else fpaths += Jstr("|") + _actdelNOpath;
		jagfclose( _delNextOriCommandFile );
		_delNextOriCommandFile = NULL;
	}
	if ( _delNextRepCommandFile && JagFileMgr::fileSize(_actdelNRpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNRpath;
		else fpaths += Jstr("|") + _actdelNRpath;
		jagfclose( _delNextRepCommandFile );
		_delNextRepCommandFile = NULL;
	}
	if ( _delNextOriRepCommandFile && JagFileMgr::fileSize(_actdelNORpath) > 0 ) {
		if ( fpaths.size() < 1 ) fpaths = _actdelNORpath;
		else fpaths += Jstr("|") + _actdelNORpath;
		jagfclose( _delNextOriRepCommandFile );
		_delNextOriRepCommandFile = NULL;
	}

	if ( fpaths.size() > 0 ) { // has delta files, recover using delta
		raydebug( stdout, JAG_LOG_LOW, "begin online recoverDeltaLog %s ...\n", fpaths.c_str() );
		_dbConnector->_parentCli->recoverDeltaLog( fpaths );
		raydebug( stdout, JAG_LOG_LOW, "end online recoverDeltaLog\n");
	}

	if ( !_delPrevOriCommandFile ) {
		_delPrevOriCommandFile = loopOpen( _actdelPOpath.c_str(), "ab" );
	}
	if ( !_delNextRepCommandFile ) {
		_delNextRepCommandFile = loopOpen( _actdelNRpath.c_str(), "ab" );
	}
	if ( 3 == _faultToleranceCopy ) {
		if ( !_delPrevRepCommandFile ) {
			_delPrevRepCommandFile = loopOpen( _actdelPRpath.c_str(), "ab" );
		}
		if ( !_delPrevOriRepCommandFile ) {
			_delPrevOriRepCommandFile = loopOpen( _actdelPORpath.c_str(), "ab" );
		}
		if ( !_delNextOriCommandFile ) {
			_delNextOriCommandFile = loopOpen( _actdelNOpath.c_str(), "ab" );
		}
		if ( !_delNextOriRepCommandFile ) {
			_delNextOriRepCommandFile = loopOpen( _actdelNORpath.c_str(), "ab" );
		}
	}
}

// close and reopen delta log file, for replicate use only
void JagDBServer::resetDeltaLog()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate

	raydebug( stdout, JAG_LOG_LOW, "begin resetDeltaLog ...\n");

	Jstr deltahome = JagFileMgr::getLocalLogDir("delta"), fpath, host0, host1, host2, host3, host4;
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
		if ( _delPrevOriCommandFile ) {
			jagfclose( _delPrevOriCommandFile );
		}
		if ( _delNextRepCommandFile ) {
			jagfclose( _delNextRepCommandFile );
		}
		
		fpath = deltahome + "/" + host2 + "_0";
		_delPrevOriCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelPOpath = fpath;
		_actdelPOhost = host2;
		
		fpath = deltahome + "/" + host0 + "_1";	
		_delNextRepCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelNRpath = fpath;
		_actdelNRhost = host1;
	} else if ( _faultToleranceCopy == 3 ) {
		if ( _delPrevOriCommandFile ) {
			jagfclose( _delPrevOriCommandFile );
		}
		if ( _delPrevRepCommandFile ) {
			jagfclose( _delPrevRepCommandFile );
		}
		if ( _delPrevOriRepCommandFile ) {
			jagfclose( _delPrevOriRepCommandFile );
		}
		if ( _delNextOriCommandFile ) {
			jagfclose( _delNextOriCommandFile );
		}
		if ( _delNextRepCommandFile ) {
			jagfclose( _delNextRepCommandFile );
		}
		if ( _delNextOriRepCommandFile ) {
			jagfclose( _delNextOriRepCommandFile );
		}
		
		fpath = deltahome + "/" + host2 + "_0";
		_delPrevOriCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelPOpath = fpath;
		_actdelPOhost = host2;
		fpath = deltahome + "/" + host0 + "_2";
		_delPrevRepCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelPRpath = fpath;
		_actdelPRhost = host2;
		fpath = deltahome + "/" + host2 + "_2";
		_delPrevOriRepCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelPORpath = fpath;
		_actdelPORhost = host4; 
		fpath = deltahome + "/" + host1 + "_0";
		_delNextOriCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelNOpath = fpath;
		_actdelNOhost = host1;
		fpath = deltahome + "/" + host0 + "_1";
		_delNextRepCommandFile = loopOpen( fpath.c_str(), "ab" );
		_actdelNRpath = fpath;
		_actdelNRhost = host1;
		fpath = deltahome + "/" + host1 + "_1";
		_delNextOriRepCommandFile = loopOpen( fpath.c_str(), "ab" );
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
void JagDBServer::organizeCompressDir( int mode, Jstr &fpath )
{
	Jstr spath = _cfg->getJDBDataHOME( mode );
	fpath = _cfg->getTEMPDataHOME( mode ) + "/" + longToStr(THREADID) + ".tar.gz";
	// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/tid.tar.gz *
	Jstr cmd = Jstr("cd ") + spath + "; " + "tar -zcf " + fpath + " *";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6107 [%s]\n", cmd.c_str() );
}

// file open a.tar.gz and send to another server
// mode=0: main data; mode=1: pdata; mode=2: ndata  
// mode=10: transmit wallog file
// return 0: error; 1: OK
int JagDBServer::fileTransmit( const Jstr &host, unsigned int port, const Jstr &passwd, 
								const Jstr &unixSocket, int mode, const Jstr &fpath, int isAddCluster )
{
	if ( fpath.size() < 1 ) {
		return 0; // no fpath, no need to transfer
	}
	raydebug( stdout, JAG_LOG_LOW, "begin fileTransmit [%s]\n", fpath.c_str() );
	int pkgsuccess = false;
	
	int hdrsz = JAG_SOCK_TOTAL_HDR_LEN;
	jagint rc;
	ssize_t rlen;
	struct stat sbuf;
	if ( 0 != stat(fpath.c_str(), &sbuf) || sbuf.st_size <= 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "E12026 error stat [%s], return\n", fpath.c_str() );
		return 0;
	}

	//char tothdr[ hdrsz +1 ];
	char sqlhdr[ 8 ];
	while ( !pkgsuccess ) {
		JaguarCPPClient pcli;
		pcli._deltaRecoverConnection = 2;
		if ( !pcli.connect( host.c_str(), port, "admin", passwd.c_str(), "test", unixSocket.c_str(), JAG_CONNECT_ONE ) ) {
			raydebug( stdout, JAG_LOG_LOW, "s4058 recover failure, unable to connect %s:%d ...\n", host.c_str(), _port );
			if ( !isAddCluster ) jagunlink( fpath.c_str() );
			return 0;
		}

		Jstr cmd;
		int fd = jagopen( fpath.c_str(), O_RDONLY, S_IRWXU);
		if ( fd < 0 ) {
			if ( !isAddCluster ) jagunlink( fpath.c_str() );
			pcli.close();
			raydebug( stdout, JAG_LOG_LOW, "E0838 end fileTransmit [%s] open error\n", fpath.c_str() );
			return 0;
		}

		if ( isAddCluster ) {
			cmd = "_serv_addbeginftransfer|";
		} else {
			cmd = "_serv_beginftransfer|";
		}
		cmd += intToStr( mode ) + "|" + longToStr(sbuf.st_size) + "|" + longToStr(THREADID);
		char cmdbuf[JAG_SOCK_TOTAL_HDR_LEN+cmd.size()+1];
		makeSQLHeader( sqlhdr );
		putXmitHdrAndData( cmdbuf, sqlhdr, cmd.c_str(), cmd.size(), "ANCC" );

		rc = sendRawData( pcli.getSocket(), cmdbuf, hdrsz+cmd.size() ); // xxx00000000168ANCCmessage client query mode
		if ( rc < 0 ) {
			jagclose( fd );
			pcli.close();
			raydebug( stdout, JAG_LOG_LOW, "E0831 retry fileTransmit [%s] sendrawdata error\n", fpath.c_str() );
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

		jagclose( fd );
		if ( !isAddCluster ) jagunlink( fpath.c_str() );
		pkgsuccess = true;
		pcli.close();
		//if ( nbuf ) free(nbuf);
		raydebug( stdout, JAG_LOG_LOW, "end fileTransmit [%s] OK\n", fpath.c_str() );
	}

	return 1;
}

// method to unzip tar.gz file and rebuild necessary memory objects
void JagDBServer::recoveryFromTransferredFile()
{
	if ( _faultToleranceCopy <= 1 ) return; // no replicate
	raydebug( stdout, JAG_LOG_LOW, "begin redo xfer data ...\n" );
	Jstr cmd, datapath, tmppath, fpath;
	bool isdo = false;

	// first, need to drop all current tables and indexs if untar needed
	JagRequest req;
	JagSession session;
	req.session = &session;
	session.servobj = this;

	// process original data first
	tmppath = _cfg->getTEMPDataHOME( JAG_MAIN );
	datapath = _cfg->getJDBDataHOME( JAG_MAIN );
	JagStrSplit sp( _crecoverFpath.c_str(), '|', true );
	if ( sp.length() > 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "cleanredo file [%s]\n", _crecoverFpath.c_str() );
		isdo = true;
		// if more than one file has received, need to consider which file to be used
		// since all packages sent are non-recover server, use first package is enough
		
		session.replicateType = 0;
		dropAllTablesAndIndex( req, _tableschema );
		JagFileMgr::rmdir( datapath, false );
		fpath = tmppath + "/" + sp[0];
		cmd = Jstr("tar -zxf ") + fpath + " --directory=" + datapath;
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
		tmppath = _cfg->getTEMPDataHOME( JAG_PREV );
		datapath = _cfg->getJDBDataHOME( JAG_PREV );
		sp.init( _prevcrecoverFpath.c_str(), '|', true );
		if ( sp.length() > 0 ) {
			raydebug( stdout, JAG_LOG_LOW, "prevcleanredo file [%s]\n", _prevcrecoverFpath.c_str() );
			isdo = true;
			// if more than one file has received, need to consider which file to be used
			// since all packages sent are non-recover server, use first package is enough
	
			session.replicateType = 1;
			dropAllTablesAndIndex( req, _prevtableschema );
			JagFileMgr::rmdir( datapath, false );
			fpath = tmppath + "/" + sp[0];
			cmd = Jstr("tar -zxf ") + fpath + " --directory=" + datapath;
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
		tmppath = _cfg->getTEMPDataHOME( JAG_NEXT );
		datapath = _cfg->getJDBDataHOME( JAG_NEXT );
		sp.init( _nextcrecoverFpath.c_str(), '|', true );
		if ( sp.length() > 0 ) {
			raydebug( stdout, JAG_LOG_LOW, "nextcleanredo file [%s]\n", _nextcrecoverFpath.c_str() );
			isdo = true;
			// if more than one file has received, need to consider which file to be used
			// since all packages sent are non-recover server, use first package is enough

			session.replicateType = 2;
			dropAllTablesAndIndex( req, _nexttableschema );
			JagFileMgr::rmdir( datapath, false );
			fpath = tmppath + "/" + sp[0];
			cmd = Jstr("tar -zxf ") + fpath + " --directory=" + datapath;
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
	if ( isdo ) crecoverRefreshSchema( JAG_MAKE_OBJECTS_CONNECTIONS );
	_crecoverFpath = "";
	_prevcrecoverFpath = "";
	_nextcrecoverFpath = "";
}

// mode JAG_MAKE_OBJECTS_CONNECTIONS: rebuild all objects and remake connections
// mode JAG_MAKE_OBJECTS_ONLY: rebuild all objects only
// mode JAG_MAKE_CONNECTIONS_ONLY: remake connections only
void JagDBServer::crecoverRefreshSchema( int mode, bool doRestoreInsertBufferMap )
{
	// destory schema related objects and rebuild them
	raydebug( stdout, JAG_LOG_LOW, "begin redo schema mode=%d ...\n", mode );

	if ( JAG_MAKE_OBJECTS_CONNECTIONS == mode || JAG_MAKE_OBJECTS_ONLY == mode ) {
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

		Jstr dblist, dbpath;
		dbpath = _cfg->getJDBDataHOME( JAG_MAIN );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, JAG_MAIN );

		dbpath = _cfg->getJDBDataHOME( JAG_PREV );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, JAG_PREV );

		dbpath = _cfg->getJDBDataHOME( JAG_NEXT );
		dblist = JagFileMgr::listObjects( dbpath );
		_objectLock->setInitDatabases( dblist, JAG_NEXT );

		initObjects();

		makeTableObjects( doRestoreInsertBufferMap );
	}

	if ( JAG_MAKE_OBJECTS_CONNECTIONS == mode || JAG_MAKE_CONNECTIONS_ONLY == mode ) {
		_dbConnector->makeInitConnection( _debugClient );
		JagRequest req;
		JagSession session;
		req.session = &session;
		session.servobj = this;
		session.replicateType = 0;
		refreshSchemaInfo( session.replicateType, g_lastSchemaTime );
	}

	raydebug( stdout, JAG_LOG_LOW, "end redo schema\n" );
}

// close and reopen temp log file to store temporary write related commands while recovery
void JagDBServer::resetRegSpLog()
{
	raydebug( stdout, JAG_LOG_LOW, "begin reset data/schema log\n" );
	
    Jstr deltahome = JagFileMgr::getLocalLogDir("delta");
    Jstr fpath = deltahome + "/redodata.log";
    if ( _recoveryRegCommandFile ) {
        jagfclose( _recoveryRegCommandFile );
    }
    if ( _recoverySpCommandFile ) {
        jagfclose( _recoverySpCommandFile );
    }
	
	_recoveryRegCmdPath = fpath;
    _recoveryRegCommandFile = loopOpen( fpath.c_str(), "ab" );
	
	fpath = deltahome + "/redometa.log";
	_recoverySpCmdPath = fpath;
    _recoverySpCommandFile = loopOpen( fpath.c_str(), "ab" );
	raydebug( stdout, JAG_LOG_LOW, "end reset data/schema log\n" );
}

void JagDBServer::recoverRegSpLog()
{
	raydebug( stdout, JAG_LOG_LOW, "begin redo data and schema ...\n" );
	jagint cnt;

	// global lock
	JAG_BLURT jaguar_mutex_lock ( &g_flagmutex ); JAG_OVER;

	// check sp command first
	if ( _recoverySpCommandFile && JagFileMgr::fileSize(_recoverySpCmdPath) > 0 ) {
		jagfclose( _recoverySpCommandFile );
		_recoverySpCommandFile = NULL;
		raydebug( stdout, JAG_LOG_LOW, "begin redo metadata [%s] ...\n", _recoverySpCmdPath.c_str() );
		cnt = redoWalLog( _recoverySpCmdPath );
		raydebug( stdout, JAG_LOG_LOW, "end redo metadata [%s] count=%l\n", _recoverySpCmdPath.c_str(), cnt );
		jagunlink( _recoverySpCmdPath.c_str() );
		_recoverySpCommandFile = loopOpen( _recoverySpCmdPath.c_str(), "ab" );
	}

	// unlock global lock
	_restartRecover = 0;
	jaguar_mutex_unlock ( &g_flagmutex );

	// check reg command to redo log
	if ( _recoveryRegCommandFile && JagFileMgr::fileSize(_recoveryRegCmdPath) > 0 ) {
		jagfclose( _recoveryRegCommandFile );
		_recoveryRegCommandFile = NULL;
		raydebug( stdout, JAG_LOG_LOW, "begin redo data [%s] ...\n", _recoveryRegCmdPath.c_str() );
		cnt = redoWalLog( _recoveryRegCmdPath );
		raydebug( stdout, JAG_LOG_LOW, "end redo data [%s] count=%l\n", _recoveryRegCmdPath.c_str(), cnt );
		jagunlink( _recoveryRegCmdPath.c_str() );
		_recoveryRegCommandFile = loopOpen( _recoveryRegCmdPath.c_str(), "ab" );
	}

	raydebug( stdout, JAG_LOG_LOW, "end redo data and schema\n" );
}

// open new dinsert active log
void JagDBServer::resetDinsertLog()
{
	Jstr fpath = JagFileMgr::getLocalLogDir("delta") + "/dinsertactive.log";
	if ( _dinsertCommandFile ) {
		jagfclose( _dinsertCommandFile );
	}
	raydebug( stdout, JAG_LOG_LOW, "open new dinsert %s\n", fpath.c_str() );
	_dinsertCommandFile = loopOpen( fpath.c_str(), "ab" );
	_activeDinFpath = fpath;
}

// This is called periodically
// do dinsertbackup if not empty, and then rename dinsertactive to dinsertbackup
void JagDBServer::rotateDinsertLog()
{
	Jstr backupDinPath = JagFileMgr::getLocalLogDir("delta") + "/dinsertbackup.log";

	recoverDinsertLog( backupDinPath );

	jaguar_mutex_lock ( &g_dinsertmutex ); 
	if ( _dinsertCommandFile ) {
		jagfclose( _dinsertCommandFile );
	}
	jagrename( _activeDinFpath.c_str(), backupDinPath.c_str() );
	_dinsertCommandFile = loopOpen( _activeDinFpath.c_str(), "ab" );
	jaguar_mutex_unlock ( &g_dinsertmutex );
}

// read uncommited logs in dinsert/ dir and execute them
void JagDBServer::recoverDinsertLog( const Jstr &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) < 1 ) return;
	raydebug( stdout, JAG_LOG_LOW, "begin redo %s ...\n",  fpath.c_str() );

	jagint cnt = redoDinsertLog( fpath );

	raydebug( stdout, JAG_LOG_LOW, "end redo %s cnt=%l\n",  fpath.c_str(), cnt );
	jagunlink( fpath.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "jagunlink %s\n",  fpath.c_str() );
}

// process commands in fpath one by one and send them to corresponding servers
// client_timediff;ddddddddddddddddstrclient_timediff;ddddddddddddddddstr
jagint JagDBServer::redoDinsertLog( const Jstr &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) <= 0 ) {
		return 0;
	}

	int i, fd = jagopen( fpath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) return 0;
	char buf16[17];
	jagint len;
	char c;
	char *buf = NULL;

	jagint cnt = 0, onecnt = 0;
	Jstr cmd;
	_dbConnector->_parentCliNonRecover->_servCurrentCluster = _dbConnector->_nodeMgr->_curClusterNumber;
	JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> qmap;

	JagParser parser( (void*)this );
	JagParseAttribute jpa;
	jpa.dfdbname = _dbConnector->_parentCliNonRecover->_dbname;
	jpa.servobj = this;

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
		_dbConnector->_parentCliNonRecover->_tdiff = jpa.timediff = jpa.servtimediff = atoi( buf16 );

		// read msglen
		memset( buf16, 0, JAG_REDO_MSGLEN+1 );
		raysaferead( fd, buf16, JAG_REDO_MSGLEN );
		len = jagatoll( buf16 );

		// read msg
		buf = (char*)jagmalloc(len+1);
		memset(buf, 0, len+1);
		raysaferead( fd, buf, len );
		cmd = Jstr( buf, len );

		onecnt += _dbConnector->_parentCliNonRecover->oneCmdInsertPool( jpa, parser, qmap, cmd );

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

// read uncommited logs in cmd/ dir and execute them
void JagDBServer::recoverWalLog( )
{
	if ( ! _walLog ) return;
	
	Jstr walpath = _cfg->getWalLogHOME();
	Jstr fpath;
	Jstr fileNames = JagFileMgr::listObjects( walpath, ".wallog" );
	JagStrSplit sp( fileNames, '|');
	for ( int i=0; i < sp.size(); ++i ) {
		fpath = walpath + "/" + sp[i];
		raydebug( stdout, JAG_LOG_LOW, "begin redoWalLog %s ...\n",  fpath.c_str() );

		jagint cnt = redoWalLog( fpath );

		raydebug( stdout, JAG_LOG_LOW, "end redoWalLog %s cnt=%l\n",  fpath.c_str(), cnt );
	}
}

// execute commands in fpath one by one
// replicateType;client_timediff;isInsert;ddddddddddddddddqstzreplicateType;client_timediff;isBatch;ddddddddddddddddqstr
jagint JagDBServer::redoWalLog( const Jstr &fpath )
{
	if ( JagFileMgr::fileSize( fpath ) <= 0 ) {
		return 0;
	}

	int i, fd = jagopen( fpath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) return 0;

	char buf16[17];
	jagint len;
	char c;
	char *buf = NULL;
	JagRequest req;
	JagSession session;
	req.hasReply = false;
	req.session = &session;
	session.servobj = this;
	session.dbname = "test";
	session.uid = "admin";
	session.origserv = 1;
	session.drecoverConn = 3;

	jagint cnt = 0;
	while ( 1 ) {
		i = 0;
		memset( buf16, 0, 4 );
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
			break;
		}
		session.replicateType = atoi( buf16 );
		if ( buf16[0] != '0' && buf16[0] != '1' && buf16[0] != '2' ) {
			raydebug( stdout, JAG_LOG_LOW, "Error in wal file [%s]. Please fix it and then restart jaguardb. cnt=%lld\n", fpath.c_str(), cnt );
			break;
		}

		// get time zone diff
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
		memset( buf16, 0, 4 );
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

		// get mesg len
		memset( buf16, 0, JAG_REDO_MSGLEN+1 );
		raysaferead( fd, buf16, JAG_REDO_MSGLEN );
		len = jagatoll( buf16 );

		buf = (char*)jagmalloc(len+1);
		memset(buf, 0, len+1);
		raysaferead( fd, buf, len );
		try {
			processMultiSingleCmd( req, buf, len, g_lastSchemaTime, g_lastHostTime, 0, true, 1 );
		} catch ( const char *e ) {
			raydebug( stdout, JAG_LOG_LOW, "redo log processMultiSingleCmd [%s] caught exception [%s]\n", buf, e );
		} catch ( ... ) {
			raydebug( stdout, JAG_LOG_LOW, "redo log processMultiSingleCmd [%s] caught unknown exception\n", buf );
		}

		free( buf );
		buf = NULL;
		if ( req.session->replicateType == 0 ) ++cnt;

		if ( (cnt%50000) == 0 ) {
			raydebug( stdout, JAG_LOG_LOW, "redo count=%d\n", cnt );
		}
	}

	jagclose( fd );
	raydebug( stdout, JAG_LOG_LOW, "redoWalLog done count=%d\n", cnt );
	return cnt;
}

// object method
// goto to each table/index, write the bottom level to a disk file
void JagDBServer::flushAllBlockIndexToDisk()
{
	JagTable *ptab; Jstr str;
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
	Jstr dbtab, db, tab, idx, fpath;
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
			//printf("s4401 jagunlink [%s]\n", fpath.c_str() );
		}
	}
	if ( vec ) delete vec;
}

// obj method
// look at _taskMap and get all TaskIDs with the same threadID
Jstr JagDBServer::getTaskIDsByThreadID( jagint threadID )
{
	Jstr taskIDs;
	const AbaxPair<AbaxLong,AbaxString> *arr = _taskMap->array();
	jagint len = _taskMap->arrayLength();
	// sprintf( buf, "%14s  %20s  %16s  %16s  %16s %s\n", "TaskID", "ThreadID", "User", "Database", "StartTime", "Command");
	for ( jagint i = 0; i < len; ++i ) {
		if ( ! _taskMap->isNull( i ) ) {
			const AbaxPair<AbaxLong,AbaxString> &pair = arr[i];
			if ( pair.key != AbaxLong(threadID) ) { continue; }
			JagStrSplit sp( pair.value.c_str(), '|' );
			// "threadID|userid|dbname|timestamp|query"
			if ( taskIDs.size() < 1 ) {
				taskIDs = sp[0];
			} else {
				taskIDs += Jstr("|") + sp[0];
			}
		}
	}
	return taskIDs;
}

int JagDBServer::schemaChangeCommandSyncRemove( const Jstr & dbobj )
{
	_scMap->removeKey( dbobj );
	return 1;
}

// method to make sure schema change command, such as drop/create table etc, 
// is synchronized among servers using hashmap before actually lock it
// isRemove 0: addKeyValue; isRemove 1: removeKey from hashMap
int JagDBServer::schemaChangeCommandSyncCheck( const JagRequest &req, const Jstr &dbobj, jagint opcode, int isRemove )
{
	if ( isRemove ) {
		_scMap->removeKey( dbobj );
		return 1;
	}

	bool setmap = 0;
	char cond[3] = { 'N', 'G', '\0' };
	if ( _scMap->addKeyValue( dbobj, opcode ) ) {
		cond[0] = 'O'; cond[1] = 'K';
		setmap = 1;
	} else {
	}

	int rc;
	if ( ( rc = sendMessageLength( req, cond, 2, "SS" ) ) < 0 ) {
		if ( setmap > 0 ) _scMap->removeKey( dbobj );
		raydebug( stdout, JAG_LOG_LOW, "s6027 sendMessageLength(%s) SS < 0 (%d), return 0\n", cond, rc  );
		return 0;
	}

	// waiting for signal; if NG, reject and return
	int hdrsz = JAG_SOCK_TOTAL_HDR_LEN; 
	char hdr[hdrsz+1];
	char *newbuf = NULL;
	if ( recvMessage( req.session->sock, hdr, newbuf ) >=0 && ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) ) {
	} else {
		if ( setmap > 0 ) _scMap->removeKey( dbobj );
		if ( newbuf ) {
			raydebug( stdout, JAG_LOG_LOW, "s6328 recvMessage < 0 or not OK, buf=[%s] return 0\n", newbuf );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "s6328 recvMessage < 0 or not OK, newbuf==NULL return 0\n" );
		}
		if ( newbuf ) free( newbuf );
		return 0;
	}

	if ( newbuf ) free( newbuf );
	return 1;
}

// only flush one table ( and related indexs ) insert buffer or one index insert buffer
// dbobj must be db.tab or db.idx
void JagDBServer::flushOneTableAndRelatedIndexsInsertBuffer( const Jstr &dbobj, int replicateType, int isTable, 
	JagTable *iptab, JagIndex *ipindex )
{
	return;  // new method of compf

}

// insert one record
// return 0: error   1: OK
int JagDBServer::doInsert( JagRequest &req, JagParseParam &parseParam, 
						   Jstr &reterr, const Jstr &oricmd )
{
	jagint cnt = 0;
	Jstr dbobj;
	JagTable *ptab = NULL;
	JagTableSchema *tableschema = this->getTableSchema( req.session->replicateType );
	Jstr dbName = parseParam.objectVec[0].dbName;
	Jstr tableName = parseParam.objectVec[0].tableName;

	if ( parseParam.objectVec.size() > 0 ) {
		ptab = _objectLock->writeLockTable( parseParam.opcode, dbName, tableName, 
											tableschema, req.session->replicateType, 0 );
	}

	if ( ! ptab ) {
		dbobj = dbName + "." + tableName;
		raydebug( stdout, JAG_LOG_HIGH, "s4304 table %s not found\n", dbobj.c_str() );
		reterr = "Table " + dbobj + " not found";
		return 0;
	}

	if ( JAG_INSERT_OP == parseParam.opcode ) {
		// cnt = ptab->insert( req, &parseParam, reterr, insertCode, false );
		cnt = ptab->insert( req, &parseParam, reterr );
		++ numInserts;
		if ( 1 == cnt ) {
			this->_dbLogger->logmsg( req, "INS", oricmd );
			Jstr tser;
			if ( ptab->hasTimeSeries( tser ) ) {
				insertToTimeSeries( ptab->_tableRecord, req, parseParam, tser, dbName, tableName, tableschema, req.session->replicateType, oricmd );
			}
		} else {
			_dbLogger->logerr( req, reterr, oricmd );
		}

	} else if ( JAG_FINSERT_OP == parseParam.opcode ) {
		cnt = ptab->finsert( req, &parseParam, reterr );
		++ numInserts;
		if ( 1 == cnt ) {
			_dbLogger->logmsg( req, "INS", oricmd );
		} else {
			_dbLogger->logerr( req, reterr, oricmd );
		}
	} else if ( JAG_CINSERT_OP == parseParam.opcode ) {
		// I am on a old cluster, so I can receive this cinsert command
		if ( JAG_SCALE_GOLAST == _scaleMode ) {
			// I should delete this record in cinsert
			cnt = ptab->dinsert( req, &parseParam, reterr );
		} else {
			cnt = ptab->cinsert( req, &parseParam, reterr );
			if ( cnt > 0 ) {
				// if data exists on this node
				// log them, and later to be sent to last two clusters for deletion
				JagDBServer::dinsertlogCommand( req.session, oricmd.c_str(), oricmd.length() );
			}
		}
	} else if ( JAG_DINSERT_OP == parseParam.opcode ) {
		cnt = ptab->dinsert( req, &parseParam, reterr );
	}

	_objectLock->writeUnlockTable( parseParam.opcode, dbName, tableName, req.session->replicateType, 0 );
	
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
		shutDown( "_exe_shutdown", req );
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
		shutDown( "_exe_shutdown", req );
	} else {
		raydebug( stdout, JAG_LOG_LOW, "Unknown signal [%d] ignored.\n", sig );
	}
	return 1;
}

#endif


bool JagDBServer::isInteralIP( const Jstr &ip )
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

bool JagDBServer::existInAllHosts( const Jstr &ip )
{
	return isInteralIP( ip );
}

void JagDBServer::initDirs()
{
	Jstr fpath = jaguarHome() + "/data/system/schema";
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
	Jstr cmd = Jstr("/bin/rm -rf ") + fpath;
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
void JagDBServer::noGood( JagRequest &req, JagParseParam &parseParam )
{
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;

	Jstr uid = parseParam.uid;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		return;
	}
	_objectLock->writeLockSchema( req.session->replicateType );

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
		int hdrsz = JAG_SOCK_TOTAL_HDR_LEN;
 		char hdr[hdrsz+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			// req.syncDataCenter = true;
			req.syncDataCenter = false;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	_objectLock->writeUnlockSchema( req.session->replicateType );
	schemaChangeCommandSyncRemove( scdbobj ); 
}

void JagDBServer::createUser( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr uid = parseParam.uid;
	Jstr pass = parseParam.passwd;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		return;
	}
	_objectLock->writeLockSchema( req.session->replicateType );
	AbaxString dbpass = uiddb->getValue(uid, JAG_PASS );
	if ( dbpass.size() < 1 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			_objectLock->writeUnlockSchema( req.session->replicateType );
			// schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	char *md5 = MDString( pass.c_str() );
	Jstr mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->addUser(uid, mdpass, JAG_USER, JAG_WRITE );
	}
	jaguar_mutex_unlock ( &g_dbschemamutex );

	_objectLock->writeUnlockSchema( req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	schemaChangeCommandSyncRemove( scdbobj ); 
}

// dropuser uid
void JagDBServer::dropUser( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{
	//jagint len;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr uid = parseParam.uid;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	_objectLock->writeLockSchema( req.session->replicateType );
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
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			_objectLock->writeUnlockSchema( req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->dropUser( uid );
	}
	jaguar_mutex_unlock ( &g_dbschemamutex );

	_objectLock->writeUnlockSchema( req.session->replicateType );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
	schemaChangeCommandSyncRemove( scdbobj );
}

// changepass uid password
void JagDBServer::changePass( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr uid = parseParam.uid;
	Jstr pass = parseParam.passwd;
	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		return;
	}
	_objectLock->writeLockSchema( req.session->replicateType );
	AbaxString dbpass = uiddb->getValue( uid, JAG_PASS );
	if ( dbpass.size() > 0 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	char *md5 = MDString( pass.c_str() );
	Jstr mdpass = md5;
	if ( md5 ) free( md5 );
	md5 = NULL;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	if ( uiddb ) {
		uiddb->setValue( uid, JAG_PASS, mdpass );
	}
	jaguar_mutex_unlock ( &g_dbschemamutex );
	_objectLock->writeUnlockSchema( req.session->replicateType );
	schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] changepass uid=[%s]\n", req.session->uid.c_str(),  parseParam.uid.c_str() );

}

// static
// method to change dfdb ( use db command input by user, not via connection )
// changedb dbname
void JagDBServer::changeDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{
	jagint lockrc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr jagdatahome = _cfg->getJDBDataHOME( req.session->replicateType );
    Jstr sysdir = jagdatahome + "/" + parseParam.dbName;
	Jstr scdbobj = sysdir + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;

	lockrc = _objectLock->readLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	if ( 0 == strcmp( parseParam.dbName.c_str(), "test" ) || 0 == jagaccess( sysdir.c_str(), X_OK ) ) {
		if ( lockrc ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( lockrc ) _objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( lockrc ) _objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			// schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 1 );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->dbname = parseParam.dbName;
	if ( lockrc )  {
		_objectLock->readUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	}
	schemaChangeCommandSyncRemove( scdbobj );
}

// static 
// createdb dbname
void JagDBServer::createDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{
	jagint lockrc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr jagdatahome = _cfg->getJDBDataHOME( req.session->replicateType );
    Jstr sysdir = jagdatahome + "/" + parseParam.dbName;
	Jstr scdbobj = sysdir + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	lockrc = _objectLock->writeLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	if ( JagFileMgr::isDir( sysdir ) < 1 && lockrc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
    jagmkdir( sysdir.c_str(), 0700 );
	_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
}

// dropdb dbname
void JagDBServer::dropDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime )
{

	jagint lockrc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr jagdatahome = _cfg->getJDBDataHOME( req.session->replicateType );
    Jstr sysdir = jagdatahome + "/" + parseParam.dbName;
	Jstr scdbobj = sysdir + "." + intToStr( req.session->replicateType );

	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) return;
	lockrc = _objectLock->writeLockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	if ( JagFileMgr::isDir( sysdir ) > 0 && lockrc ) {
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
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	// when doing dropDB, createIndexLock && tableUsingLock needs to be locked to protect tables
	req.session->spCommandReject = 0;
	JagTableSchema *tableschema = getTableSchema( req.session->replicateType );
	// drop all tables and indexes under this database
	dropAllTablesAndIndexUnderDatabase( req, tableschema, parseParam.dbName );
    JagFileMgr::rmdir( sysdir );
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );
	if ( lockrc ) {
		_objectLock->writeUnlockDatabase( parseParam.opcode, parseParam.dbName, req.session->replicateType );
	}
	schemaChangeCommandSyncRemove( scdbobj );
	if ( parseParam.hasForce ) {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] force dropped database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
	} else {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] dropped database [%s]\n", req.session->uid.c_str(),  parseParam.dbName.c_str() );
	}
}

// create table schema
// return 1: OK   0: error
int JagDBServer::createTable( JagRequest &req, const Jstr &dbname, 
							  JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime )
{
	bool found = false, found2 = false;
	int repType =  req.session->replicateType;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	if ( JAG_CREATECHAIN_OP == parseParam->opcode ) {
		parseParam->isChainTable = 1;
	}  else {
		parseParam->isChainTable = 0;
	}
	jagint lockrc;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbtable + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		return 0;
	}
	// for createtable, write lock db first, insert schema then lock table
	lockrc = _objectLock->writeLockDatabase( parseParam->opcode, dbname, repType );
	found = indexschema->tableExist( dbname, parseParam );
	found2 = tableschema->existAttr( dbtable );
	if ( !found && !found2 && lockrc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	tableschema->insert( parseParam );
	refreshSchemaInfo( repType, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbschemamutex );


	// create table object 
	if ( _objectLock->writeLockTable( parseParam->opcode, dbname, 
									   parseParam->objectVec[0].tableName, tableschema, repType, 1 ) ) {
		_objectLock->writeUnlockTable( parseParam->opcode, dbname, 
										parseParam->objectVec[0].tableName, repType, 1 );
	}

	if ( lockrc ) {
		_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
	}
	schemaChangeCommandSyncRemove( scdbobj );
	if ( parseParam->isChainTable ) {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create chain [%s] reptype=%d\n", 
				req.session->uid.c_str(), dbtable.c_str(), repType );
	} else {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create table [%s] reptype=%d\n", 
				req.session->uid.c_str(), dbtable.c_str(), repType );
	}

	return 1;
}

int JagDBServer::createSimpleTable( const JagRequest &req, const Jstr &dbname, const JagParseParam *parseParam )
{
	Jstr tableName = parseParam->objectVec[0].tableName;
	Jstr dbtable = dbname + "." + tableName;
	int  replicateType = req.session->replicateType;
	int opcode = parseParam->opcode;

	bool found = false, found2 = false;
	int repType =  req.session->replicateType;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( replicateType, tableschema, indexschema );

	found = indexschema->tableExist( dbname, parseParam );
	found2 = tableschema->existAttr( dbtable );

	if ( found || found2 ) {
		return 0;
	}
	
    tableschema->insert( parseParam );
    refreshSchemaInfo( repType, g_lastSchemaTime );
    _objectLock->getTable( opcode, dbname, tableName, tableschema, repType );

	if ( parseParam->isChainTable ) {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create simple chain [%s] reptype=%d\n", 
				 req.session->uid.c_str(), dbtable.c_str(), repType );
	} else {
		raydebug(stdout, JAG_LOG_LOW, "user [%s] create simple table [%s] reptype=%d\n", 
				 req.session->uid.c_str(), dbtable.c_str(), repType );
	}

	return 1;
}

// create memtable schema
int JagDBServer::createMemTable( JagRequest &req, const Jstr &dbname, 
							     JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime )
{
	bool found = false, found2 = false;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	jagint lockrc;
	int repType = req.session->replicateType;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr dbtable = dbname + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbtable + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	parseParam->isMemTable = 1;
	lockrc = _objectLock->writeLockDatabase( parseParam->opcode, dbname, repType );
	found = indexschema->tableExist( dbname, parseParam );
	found2 = tableschema->existAttr( dbtable );
	if ( !found && !found2 && lockrc ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) 
	{
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}
	
	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( lockrc ) _objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	tableschema->insert( parseParam );
	refreshSchemaInfo( repType, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbschemamutex );

	/***
	if ( _objectLock->writeLockTable(  parseParam->opcode, dbname, 
												parseParam->objectVec[0].tableName, tableschema, repType, 1 ) ) {
		_objectLock->writeUnlockTable( parseParam->opcode, dbname, 
												parseParam->objectVec[0].tableName, repType, 1 );
	}
	***/

	if ( lockrc ) {
		_objectLock->writeUnlockDatabase( parseParam->opcode, dbname, repType );
	}
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create memtable [%s]\n", req.session->uid.c_str(), dbtable.c_str() );
	return 1;
}

// new method for createIndex to deal with asc index keys
//  return 0: error with reterr;  1: success
int JagDBServer::createIndex( JagRequest &req, const Jstr &dbname, JagParseParam *parseParam,
							  JagTable *&ptab, JagIndex *&pindex, Jstr &reterr, jagint threadQueryTime )
{
	JagIndexSchema *indexschema; 
	JagTableSchema *tableschema;
	getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	Jstr dbindex = dbname + "." + parseParam->objectVec[0].tableName + 
							 "." + parseParam->objectVec[1].indexName;

	Jstr tgttab = indexschema->getTableNameScan( dbname, parseParam->objectVec[1].indexName );
	if ( tgttab.size() > 0 ) {
		noGood( req, *parseParam );
		return 0;
	}

	int rc = 0;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbobj + "." + intToStr( req.session->replicateType );

	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		return 0;
	}

	ptab = _objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
										 parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );
	if ( ptab ) {
		rc = createIndexSchema( req, dbname, parseParam, reterr, true );
		if ( rc ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	} else {
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( rc ) {
				JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	    		indexschema->remove( dbindex );
				jaguar_mutex_unlock ( &g_dbschemamutex );
			}

			if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
													   parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( rc ) {
				JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	    		indexschema->remove( dbindex );
				jaguar_mutex_unlock ( &g_dbschemamutex );
			}
			if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
														parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( newbuf[0] == 'O' && newbuf[1] == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbschemamutex );

	pindex = _objectLock->writeLockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
										   parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										   tableschema, indexschema, req.session->replicateType, 1 );
	if ( ptab && pindex ) {
		// if successfully create index, begin process table's data
		doCreateIndex( ptab, pindex );
		rc = 1;
	} else {
		JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	    indexschema->remove( dbindex );
		jaguar_mutex_unlock ( &g_dbschemamutex );
		rc = 0;
	}

	if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
												parseParam->objectVec[0].tableName, req.session->replicateType, 0 );

	if ( pindex ) {
		_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
										parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										req.session->replicateType, 1 );		
	}

	schemaChangeCommandSyncRemove( scdbobj );
	raydebug(stdout, JAG_LOG_LOW, "user [%s] create index [%s]\n", req.session->uid.c_str(), scdbobj.c_str() );
	return rc;
}

int JagDBServer
::alterTable( const JagParseAttribute &jpa, JagRequest &req, const Jstr &dbname,
			   const JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, 
			   jagint &threadSchemaTime )
{
	Jstr dbName = parseParam->objectVec[0].dbName;
	Jstr tableName = parseParam->objectVec[0].tableName;
	int replicateType = req.session->replicateType;
	int opcode =  parseParam->opcode;
	bool brc;

	JagTableSchema *tableschema = getTableSchema( replicateType );
	JagTable *ptab = NULL;
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr dbtable = dbName + "." + tableName;
	Jstr scdbobj = dbtable + "." + intToStr( replicateType );

	if ( !schemaChangeCommandSyncCheck( req, scdbobj, opcode, 0 ) ) {
		return 0;
	}
	ptab = _objectLock->writeLockTable( opcode, dbName, tableName, tableschema, replicateType, 0 );
	if ( ptab && tableschema->existAttr( dbtable ) ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
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
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( ptab ) _objectLock->writeUnlockTable( opcode, dbName, tableName, replicateType, 0 );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( ptab ) _objectLock->writeUnlockTable( opcode, dbName, tableName, replicateType, 0 );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( newbuf[0] == 'O' && newbuf[1] == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;	

	bool hasChange = false;
	Jstr sql, normalizedTser;
	if ( ptab ) {
		if ( parseParam->cmd == JAG_SCHEMA_ADD_COLUMN || parseParam->cmd == JAG_SCHEMA_RENAME_COLUMN ) {
			brc = tableschema->addOrRenameColumn( dbtable, parseParam );
			if ( brc ) {
				hasChange = true;
			} else {
				reterr = "E12302 error add or rename table column"; 
			}
		} else if ( parseParam->cmd == JAG_SCHEMA_SET ) {
			brc = tableschema->setColumn( dbtable, parseParam );
			if ( brc ) {
				hasChange = true;
			} else {
				reterr = "E12303 error setting table column property"; 
			}
		} else if ( parseParam->cmd == JAG_SCHEMA_ADD_TICK && ptab->hasTimeSeries() ) {
			normalizedTser = JagSchemaRecord::translateTimeSeries( parseParam->value );
			normalizedTser = JagSchemaRecord::makeTickPair( normalizedTser );
			sql = describeTable( JAG_TABLE_TYPE, req, _tableschema, dbtable, false, true, true, normalizedTser );
			JagParser parser((void*)this);
			JagParseParam pparam2( &parser );
			if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
				bool crc = createSimpleTable( req, dbname, &pparam2 );
				if ( crc ) {
					bool arc = tableschema->addTick( dbtable, normalizedTser );
					if ( arc ) {
						hasChange = true;
					} else {
						raydebug( stdout, JAG_LOG_LOW, "E20221 Error: [%s] addTick[%s]\n", sql.s(), normalizedTser.s() );
						reterr = "E13214 error adding tick";
					}
				} else {
					reterr = "E13215 error adding tick table";
				}
			} else {
				raydebug( stdout, JAG_LOG_LOW, "E20121 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
				reterr = Jstr("E13216 error parsing command ") + sql + " " + reterr;
			}
		} else if ( parseParam->cmd == JAG_SCHEMA_DROP_TICK && ptab->hasTimeSeries() ) {
			normalizedTser = JagSchemaRecord::translateTimeSeries( parseParam->value );
			JagStrSplit ss(normalizedTser, '_');
			Jstr rollTab = ss[0];
			Jstr sql = Jstr("drop table ") + dbtable + "@" + rollTab;
			JagParser parser((void*)this);
			JagParseParam pparam2( &parser );
			if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
				bool crc = dropSimpleTable( req, &pparam2, reterr, false );
				if ( crc ) {
					bool arc = tableschema->dropTick( dbtable, normalizedTser );
					if ( arc ) {
						hasChange = true;
					} else {
						raydebug( stdout, JAG_LOG_LOW, "E20133 Error: [%s] dropTick(%s)\n", sql.s(), normalizedTser );
						reterr = "E13217 error dropping tick";
					}
				} else {
					reterr = "E13218 error dropping tick table";
				}
			} else {
				raydebug( stdout, JAG_LOG_LOW, "E20123 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
				reterr = "E13219 error parsing command";
			}
		} else if ( parseParam->cmd == JAG_SCHEMA_CHANGE_RETENTION ) {
			bool arc = tableschema->changeRetention( dbtable, parseParam->value );
			if ( arc ) {
				hasChange = true;

				if ( strchr( dbtable.s(), '@' ) ) {
					JagStrSplit sp( dbtable, '@');
					Jstr parentTable = sp[0];
					Jstr tick =  sp[1];
					tableschema->changeTickRetention( parentTable, tick, parseParam->value );
				}

			} else {
				raydebug( stdout, JAG_LOG_LOW, "E20153 Error: change retention(%s)\n", parseParam->value.s() );
				reterr = "E13220 error change retention";
			}

		}

		if ( hasChange ) {
			ptab->refreshSchema();  // including tableRecord is changed
		} 
	}

	if ( parseParam->createAttrVec.size() < 1 && hasChange ) {
		if ( ptab ) {
			if ( parseParam->cmd == JAG_SCHEMA_ADD_COLUMN || parseParam->cmd == JAG_SCHEMA_RENAME_COLUMN ) {
				ptab->renameIndexColumn( parseParam, reterr );
			} else if (  parseParam->cmd == JAG_SCHEMA_SET ) {
				ptab->setIndexColumn( parseParam, reterr );
			} else if ( parseParam->cmd == JAG_SCHEMA_ADD_TICK && ptab->hasTimeSeries() ) {
				JagTable *newPtab; JagIndex *newPindex;
				const JagVector<Jstr> &indexVec = ptab->getIndexes();
				Jstr  sql, indexName;
				for ( int i = 0; i < indexVec.size(); ++i ) {
					indexName = indexVec[i];
               		sql = describeIndex( false, req, _indexschema, dbname, indexName, reterr, true, true, normalizedTser );
               		sql.replace('\n', ' ');
               		JagParser parser((void*)this);
               		JagParseParam pparam2( &parser );
               		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
               			bool crc = createSimpleIndex( req, &pparam2, newPtab, newPindex, reterr );
               			if ( ! crc ) {
               				raydebug( stdout, JAG_LOG_LOW, "E13271 Error: creating timeseries index [%s]\n", sql.s() );
               			} else {
               				raydebug( stdout, JAG_LOG_LOW, "OK13213 OK: creating timeseries index [%s]\n", sql.s() );
               			}
               		} else {
               			raydebug( stdout, JAG_LOG_LOW, "E12211 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
               		}
        
        		}
			} else if ( parseParam->cmd == JAG_SCHEMA_DROP_TICK && ptab->hasTimeSeries() ) {
				JagStrSplit ss(normalizedTser, '_');
				Jstr rollTab = ss[0];
				Jstr parentTableName = tableName;
				const JagVector<Jstr> &indexVec = ptab->getIndexes();
				Jstr  sql, parentIndexName;
				for ( int i = 0; i < indexVec.size(); ++i ) {
					parentIndexName = indexVec[i];
					sql = Jstr("drop index ") + parentIndexName + "@" + rollTab + " on " + parentTableName + "@" + rollTab;

               		sql.replace('\n', ' ');
               		JagParser parser((void*)this);
               		JagParseParam pparam2( &parser );
               		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
						bool crc = dropSimpleIndex( req, &pparam2, reterr, false );
               			if ( ! crc ) {
               				raydebug( stdout, JAG_LOG_LOW, "E13281 Error: dropSimpleIndex [%s]\n", sql.s() );
               			} else {
               				raydebug( stdout, JAG_LOG_LOW, "OK13413 OK: dropSimpleIndex [%s]\n", sql.s() );
               			}
               		} else {
               			raydebug( stdout, JAG_LOG_LOW, "E12241 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
               		}
        
        		}
			}
		}
	}

	if ( hasChange ) {
		refreshSchemaInfo( replicateType, g_lastSchemaTime );
	}

	jaguar_mutex_unlock ( &g_dbschemamutex );
	if ( ptab ) {
		_objectLock->writeUnlockTable( opcode, dbName, tableName, replicateType, 0 );
	}
	schemaChangeCommandSyncRemove( scdbobj );

	if ( hasChange ) {
		if ( !req.session->origserv && !_restartRecover ) {
			broadcastSchemaToClients();
		}

		threadSchemaTime = g_lastSchemaTime;
	} 

	if ( hasChange ) {
		return 1;
	} else {
		return 0;
	}
}

// return 1: OK  0: error
int JagDBServer::dropTable( JagRequest &req, JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, Jstr &timeSeries )
{
	Jstr dbname =  parseParam->objectVec[0].dbName;
	Jstr tabname =  parseParam->objectVec[0].tableName;
	Jstr dbobj = dbname + "." + tabname;
	Jstr scdbobj = dbobj + "." + intToStr( req.session->replicateType );

	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		return 0;
	}

	JagTable *ptab = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = _objectLock->writeLockTable( parseParam->opcode, dbname, tabname, tableschema, req.session->replicateType, 0 );	
	if ( ptab ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( parseParam->hasForce ) {
		cond[0] = 'O'; cond[1] = 'K';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			// client conn disconnected
			if ( ptab ) {
				_objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 );
			}
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}

 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			if ( ptab ) {
				_objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 ); 
			}
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	req.session->spCommandReject = 0;
	if ( ptab ) {
		ptab->hasTimeSeries( timeSeries );
		ptab->drop( reterr ); 
	}

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	tableschema->remove( dbobj );
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbschemamutex );
	
	if ( ptab ) {
		_objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 );
		delete ptab; 
		ptab = NULL;
	}
	schemaChangeCommandSyncRemove( scdbobj );
	JAG_BLURT jaguar_mutex_lock ( &g_wallogmutex ); JAG_OVER;
	Jstr fpath = _cfg->getWalLogHOME() + "/" + dbname + "." + tabname + ".wallog";
	jagunlink( fpath.s() );
	JAG_BLURT jaguar_mutex_unlock ( &g_wallogmutex ); 

	if ( parseParam->hasForce ) {
		raydebug( stdout, JAG_LOG_LOW, "user [%s] force drop table [%s]\n", req.session->uid.c_str(), dbobj.c_str() );		
	} else {
		raydebug( stdout, JAG_LOG_LOW, "user [%s] drop table [%s]\n", req.session->uid.c_str(), dbobj.c_str() );		
	}
	return 1;
}

// return 1: OK  0: error
int JagDBServer::dropSimpleTable( const JagRequest &req, const JagParseParam *parseParam, Jstr &reterr, bool lockSchema )
{
	Jstr dbname = parseParam->objectVec[0].dbName;
	Jstr tabname = parseParam->objectVec[0].tableName;

	Jstr dbobj = dbname + "." + tabname;

	JagTable *ptab = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	ptab = _objectLock->writeLockTable( parseParam->opcode, dbname, tabname, tableschema, req.session->replicateType, 0 );

	if ( ptab ) {
		ptab->drop( reterr ); 
	}

	if ( lockSchema ) {
		JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	}
	tableschema->remove( dbobj );
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );

	if ( lockSchema ) {
		jaguar_mutex_unlock ( &g_dbschemamutex );
	}

	// remove wallog
	JAG_BLURT jaguar_mutex_lock ( &g_wallogmutex ); JAG_OVER;
	Jstr fpath = _cfg->getWalLogHOME() + "/" + dbname + "." + tabname + ".wallog";
	jagunlink( fpath.s() );
	JAG_BLURT jaguar_mutex_unlock ( &g_wallogmutex ); 
	
	// drop table and related indexs
	if ( ptab ) {
		_objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 ); 
		delete ptab; 
		return 1;
	} else {
		reterr = Jstr("E33301 cannot get ") + dbobj;
		return 0;
	}
}

// return 1: OK  0: error
int JagDBServer::dropIndex( JagRequest &req, const Jstr &dbname, 
							JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, Jstr &timeSer )
{
	Jstr dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbobj + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) return 0;
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = _objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
										 parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );	

	pindex = _objectLock->writeLockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
										parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										tableschema, indexschema, req.session->replicateType, 1 );

	if ( ptab ) {
		ptab->hasTimeSeries( timeSer );
		if ( pindex ) {
			cond[0] = 'O'; cond[1] = 'K';
		}
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			if ( pindex ) {
				_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
											    parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
												req.session->replicateType, 1 );
			}

			if ( ptab ) {
				_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
												parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
			}
			schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			return 0;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( pindex ) _objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
														parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
														req.session->replicateType, 1 );
			if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
														parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
			// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}

	if ( ! pindex ) {
		if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
													parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
		schemaChangeCommandSyncRemove( scdbobj );
		return 0;
	}
	
	if ( ! ptab ) {
		_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
										parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										req.session->replicateType, 1 );
		schemaChangeCommandSyncRemove( scdbobj );
		return 0;
	}

	req.session->spCommandReject = 0;
	Jstr dbtabidx;
	pindex->drop();
	dbtabidx = dbname + "." + parseParam->objectVec[0].tableName + "." + parseParam->objectVec[1].indexName;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	indexschema->remove( dbtabidx );
	ptab->dropFromIndexList( parseParam->objectVec[1].indexName );	
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );
	jaguar_mutex_unlock ( &g_dbschemamutex );
	
	// drop one index
	delete pindex; pindex = NULL;
	_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
									parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
									req.session->replicateType, 1 );

	_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
									parseParam->objectVec[0].tableName, req.session->replicateType, 0 );

	raydebug( stdout, JAG_LOG_LOW, "user [%s] drop index [%s]\n", req.session->uid.c_str(), dbtabidx.c_str() );
	// servobj->schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 1 );
	schemaChangeCommandSyncRemove( scdbobj );
	return 1;
}

int JagDBServer::truncateTable( JagRequest &req, JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime )
{
	Jstr dbname = parseParam->objectVec[0].dbName;
	Jstr tabname = parseParam->objectVec[0].tableName;
	Jstr dbobj = dbname + "." + tabname;
	Jstr scdbobj = dbobj + "." + intToStr( req.session->replicateType );
	Jstr indexNames;
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam->opcode, 0 ) ) {
		return 0;
	}
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	ptab = _objectLock->writeLockTable( parseParam->opcode, dbname, tabname, tableschema, req.session->replicateType, 0 ); 

	if ( ptab ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		// command comes at an inappropriate time, reject
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {		
			if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 ); 
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			// connection abort or not OK signal, reject
			if ( ptab ) _objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 ); 
			schemaChangeCommandSyncRemove( scdbobj );
			return 0;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	//servobj->flushOneTableAndRelatedIndexsInsertBuffer( dbobj, req.session->replicateType, 1, ptab, NULL );

	if ( ptab ) {
		indexNames = ptab->drop( reterr, true );
	}
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );
	
	if ( ptab ) {
		delete ptab; ptab = NULL;
		// rebuild ptab, and possible related indexs
		ptab = _objectLock->writeTruncateTable( parseParam->opcode, dbname, tabname, tableschema, req.session->replicateType, 0 ); 
		if ( ptab ) {
			JagStrSplit sp( indexNames, '|', true );
			for ( int i = 0; i < sp.length(); ++i ) {
				pindex = _objectLock->writeLockIndex( JAG_CREATEINDEX_OP, dbname, tabname, sp[i],
														tableschema, indexschema, req.session->replicateType, 1 );
				if ( pindex ) {
					ptab->_indexlist.append( pindex->getIndexName() );
				    _objectLock->writeUnlockIndex( JAG_CREATEINDEX_OP, dbname, tabname, sp[i], req.session->replicateType, 1 );
				}
			}
			_objectLock->writeUnlockTable( parseParam->opcode, dbname, tabname, req.session->replicateType, 0 );
		}
		//Jstr dbtab = dbname + "." + tabname;
		raydebug( stdout, JAG_LOG_LOW, "user [%s] truncate table [%s]\n", req.session->uid.c_str(), dbobj.c_str() );

		// remove wallog
		JAG_BLURT jaguar_mutex_lock ( &g_wallogmutex ); JAG_OVER;
		Jstr fpath = _cfg->getWalLogHOME() + "/" + dbname + "." + tabname + ".wallog";
		jagunlink( fpath.s() );
		JAG_BLURT jaguar_mutex_unlock ( &g_wallogmutex ); 
	}

	schemaChangeCommandSyncRemove( scdbobj );
	return 1;
}

void JagDBServer::sendMapInfo( const char *mesg, const JagRequest &req )
{	
	if ( !req.session->origserv && !_restartRecover ) {	
		Jstr schemaInfo;
		_dbConnector->_broadcastCli->getSchemaMapInfo( schemaInfo );

		if ( schemaInfo.size() > 0 ) {
			sendMessageLength( req, schemaInfo.c_str(), schemaInfo.size(), "SC" );
		}
	}
	return;
}

void JagDBServer::sendHostInfo( const char *mesg, const JagRequest &req )
{	
	if ( !req.session->origserv && !_restartRecover ) {	
		jaguar_mutex_lock ( &g_dbconnectormutex );
		Jstr snodes = _dbConnector->_nodeMgr->_sendAllNodes;
		jaguar_mutex_unlock ( &g_dbconnectormutex );
		sendMessageLength( req, snodes.c_str(), snodes.size(), "HL" );
	}
}

void JagDBServer::checkDeltaFiles( const char *mesg, const JagRequest &req )
{
	Jstr str;
	if ( _actdelPOhost == req.session->ip && JagFileMgr::fileSize(_actdelPOpath) > 0 ) {
		str = _actdelPOpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelPRhost == req.session->ip && JagFileMgr::fileSize(_actdelPRpath) > 0 ) {
		str = _actdelPRpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelPORhost == req.session->ip && JagFileMgr::fileSize(_actdelPORpath) > 0 ) {
		str = _actdelPORpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNOhost == req.session->ip && JagFileMgr::fileSize(_actdelNOpath) > 0 ) {
		str = _actdelNOpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNRhost == req.session->ip && JagFileMgr::fileSize(_actdelNRpath) > 0 ) {
		str = _actdelNRpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
	if ( _actdelNORhost == req.session->ip && JagFileMgr::fileSize(_actdelNORpath) > 0 ) { 
		str = _actdelNORpath + " not empty";
		sendMessage( req, str.c_str(), "OK" );
	}
}

// crecover, clean recover server(s)
// pmesg: "_serv_crecover"
void JagDBServer::cleanRecovery( const char *mesg, const JagRequest &req )
{
	if ( req.session->servobj->_restartRecover ) return;					
	if ( _faultToleranceCopy <= 1 ) return; // no replicate

	// first, ask other servers to see if current server has delta recover file; if yes, return ( not up-to-date files )
	Jstr unixSocket = Jstr("/TOKEN=") + _servToken;
	JaguarCPPClient reqcli;
	if ( !reqcli.connect( _dbConnector->_nodeMgr->_selfIP.c_str(), _port, "admin", "dummy", "test", unixSocket.c_str(), JAG_SERV_PARENT ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4058 crecover check failure, unable to make connection ...\n" );
		reqcli.close();
		return;
	}

	Jstr bcasthosts = getBroadcastRecoverHosts( _faultToleranceCopy );
	Jstr resp = _dbConnector->broadcastGet( "_serv_checkdelta", bcasthosts, &reqcli );
	JagStrSplit checksp( resp, '\n', true );
	if ( checksp.length() > 1 || checksp[0].length() > 0 ) {
		reqcli.close();
		return;
	}
	reqcli.close();

	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	raydebug( stdout, JAG_LOG_LOW, "begin proc request of clean redo from %s ...\n", req.session->ip.c_str() );
	jagsync();
	raydebug( stdout, JAG_LOG_LOW, "begin cleanRecovery ...\n");
	jagint reqservi;
	req.session->servobj->_internalHostNum->getValue(req.session->ip, reqservi);
	int pos1, pos2, pos3, pos4, rc;
	Jstr filePath, passwd = "dummy";
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
		return;
	}
	raydebug( stdout, JAG_LOG_LOW, "begin proc request of xfer from %s ...\n", req.session->ip.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "begin recoveryFileReceiver ...\n" );
	
	JagStrSplit sp( mesg, '|', true );
	if ( sp.length() < 4 ) return;
	int fpos = atoi(sp[1].c_str());
	size_t rlen;
	jagint fsize = jagatoll(sp[2].c_str());
	jagint memsize = 128*1024*1024;
	jagint totlen = 0;
	jagint recvlen = 0;
	Jstr recvpath = req.session->servobj->_cfg->getTEMPDataHOME( fpos ) + "/" + req.session->ip + "_" + sp[3] + ".tar.gz";
	int fd = jagopen( recvpath.c_str(), O_CREAT|O_RDWR|O_TRUNC, S_IRWXU);
	raydebug( stdout, JAG_LOG_LOW, "s6207 open recvpath=[%s] for recoveryFile\n", recvpath.c_str() );
	if ( fd < 0 ) {
		raydebug( stdout, JAG_LOG_LOW, "s6208 error open recvpath=[%s]\n", recvpath.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error open\n" );
		//return;
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

		// even if fd < 0; still recv data
		rlen = recvRawData( req.session->sock, buf, recvlen );
		if ( rlen < recvlen ) {
			if ( buf ) free ( buf );
			jagclose( fd );
			jagunlink( recvpath.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error recvrawdata\n" );
			return;
		}

		if ( fd > 0 ) {
			rlen = raysafewrite( fd, buf, recvlen );
			if ( rlen < recvlen ) {
				if ( buf ) free ( buf );
				jagclose( fd );
				jagunlink( recvpath.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver error savedata\n" );
				return;
			}
		}

		totlen += rlen;
	}

	if ( fd > 0 ) {
		jagfdatasync( fd );
		jagclose( fd );
		raydebug( stdout, JAG_LOG_LOW, "saved %l bytes\n", totlen );
	}
	raydebug( stdout, JAG_LOG_LOW, "recved %l bytes\n", totlen );
	
	// check number of bytes
	struct stat sbuf;
	if ( 0 != stat(recvpath.c_str(), &sbuf) || sbuf.st_size != fsize || totlen != fsize ) {
		// incorrect number of bytes of file, remove file
		jagunlink( recvpath.c_str() );
	} else {
		JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
		if ( 0 == fpos ) {
			if ( _crecoverFpath.size() < 1 ) {
				_crecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_crecoverFpath += Jstr("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		} else if ( 1 == fpos ) {
			if ( _prevcrecoverFpath.size() < 1 ) {
				_prevcrecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_prevcrecoverFpath += Jstr("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		} else if ( 2 == fpos ) {
			if ( _nextcrecoverFpath.size() < 1 ) {
				_nextcrecoverFpath = req.session->ip + "_" + sp[3] + ".tar.gz";
			} else {
				_nextcrecoverFpath += Jstr("|") + req.session->ip + "_" + sp[3] + ".tar.gz";
			}
		}
		jaguar_mutex_unlock ( &g_dlogmutex );
		raydebug( stdout, JAG_LOG_LOW, "end recoveryFileReceiver OK\n" );
	}

	if ( buf ) free ( buf );
	raydebug( stdout, JAG_LOG_LOW, "end proc request of xfer from %s\n", req.session->ip.c_str() );
}


// method to receive tar.gz recovery file
// pmesg: "_serv_beginftransfer|10|123456|sender_tid" 
void JagDBServer::walFileReceiver( const char *mesg, const JagRequest &req )
{
}

// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
// pmesg: "_serv_opinfo"
void JagDBServer::sendOpInfo( const char *mesg, const JagRequest &req )
{
	JagStrSplit sp( _dbConnector->_nodeMgr->_curClusterNodes, '|' );
	int nsrv = sp.length(); 

	int dbs, tabs;
	numDBTables( dbs, tabs );

	Jstr res;
	char buf[1024];
	sprintf( buf, "%d|%d|%d|%lld|%lld|%lld|%lld|%lld", nsrv, dbs, tabs, 
			(jagint)numSelects, (jagint)numInserts, 
			(jagint)numUpdates, (jagint)numDeletes, 
			_connections );

	res = buf;
	// printf("s4910 sendOpInfo [%s]\n", res.c_str() );
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
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
	Jstr rec = sp[1];
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

	Jstr passwdfile = _cfg->getConfHOME() + "/tmpsyncpass.txt";
	JagFileMgr::writeTextFile( passwdfile, sp[2] );
	Jstr cmd;
	chmod( passwdfile.c_str(), 0600 );

	Jstr intip = _localInternalIP;
	Jstr jagdatahome = _cfg->getJDBDataHOME( JAG_MAIN ); // /home/jaguar/data
	Jstr backupdir = jaguarHome() + "/tmp/remotebackup";
	JagFileMgr::rmdir( backupdir );
	JagFileMgr::makedirPath( backupdir );
	char buf[2048];
	sprintf( buf, "rsync -r %s/ %s", jagdatahome.c_str(), backupdir.c_str() );
	system( buf );  // first local copy /home/jaguar/data/* to /home/jaguar/tmp/remotebackup

	cmd = "rsync -q --contimeout=10 --password-file=" + passwdfile + " -az " + backupdir + "/ " + sp[1] + "::jaguardata/" + intip;
	// then copy from /home/jaguar/data/remotebackup/* to remotehost::jaguardata/192.183.2.120
	// do not comment out
	prt(("s1829 [%s]\n", cmd.c_str() ));
	Jstr res = psystem( cmd.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "doRemoteBackup done %s\n", res.c_str() );
	_doingRemoteBackup = 0;
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
	if ( sp[1].length() < 1 ) {
		// do not comment out
		prt(("s1193 rserv empty, skip doRestoreRemote\n" ));
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "dorestoreremote ...\n" );
	_doingRestoreRemote = 1;

	char buf[2048];
    Jstr cmd = jaguarHome() + "/bin/restorefromremote.sh";
	Jstr logf = jaguarHome() + "/log/restorefromremote.log";
	sprintf( buf, "%s %s %s > %s 2>&1", cmd.c_str(), sp[1].c_str(), sp[2].c_str(), logf.c_str() );

	Jstr res = psystem( buf ); 
	raydebug( stdout, JAG_LOG_LOW, "doRestoreRemote %s\n", res.c_str() );
	_doingRestoreRemote = 0;
	raydebug( stdout, JAG_LOG_LOW, "exit. Please restart jaguar after restore completes\n", res.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "dorestoreremote done\n" );
	exit(39);
}

// refresh local server allowlist and blocklist
// pmesg: "_serv_refreshacl|allowlistIPS|blocklistIPS"
void JagDBServer::doRefreshACL( const char *mesg, const JagRequest &req )
{
	JagStrSplit sp( mesg, '|' );
	if ( sp.length() < 3 ) {
		raydebug( stdout, JAG_LOG_LOW, "s8315 doRefreshACL error [%s]\n", mesg );
		return;
	}

	Jstr allowlist = sp[1];  // ip1\nip2\nip3\n
	Jstr blocklist = sp[2];  // ip4\nip5\ip6\n
	pthread_rwlock_wrlock( &_aclrwlock);
	_allowIPList->refresh( allowlist );
	_blockIPList->refresh( blocklist );
	pthread_rwlock_unlock( &_aclrwlock );

}

// dbtabInfo 
// pmesg: "_mon_dbtab" 
void JagDBServer::dbtabInfo( const char *mesg, const JagRequest &req )
{
	// "db1:t1:t2|db2:t1:t2|db3:t1:t3"
	JagTableSchema *tableschema = getTableSchema( req.session->replicateType );
	Jstr res;
	Jstr dbs = JagSchema::getDatabases( _cfg, req.session->replicateType );
	JagStrSplit sp(dbs, '\n', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		JagVector<AbaxString> *vec = tableschema->getAllTablesOrIndexes( sp[i], "" );
		res += sp[i];
		for ( int j =0; j < vec->size(); ++j ) {
			res += Jstr(":") + (*vec)[j].c_str();
		}
		if ( vec ) delete vec;
		vec = NULL;
		res += Jstr("|");
	}
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// send info 
// pmesg: "_mon_info"
void JagDBServer::sendInfo( const char *mesg, const JagRequest &req )
{
	Jstr res;
	JagVector<Jstr> vec;
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
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "disk:123:322|mem:234:123|cpu:23:12";  in GB  used:free
// pmesg: "_mon_rsinfo"
void JagDBServer::sendResourceInfo( const char *mesg, const JagRequest &req )
{
	int rc = 0;
	Jstr res;
	jagint usedDisk, freeDisk;
	jagint usercpu, syscpu, idle;
	_jagSystem.getCPUStat( usercpu, syscpu, idle );
	jagint totm, freem, used; //GB
	rc = _jagSystem.getMemInfo( totm, freem, used );

	Jstr jaghome= jaguarHome();
	JagFileMgr::getPathUsage( jaghome.c_str(), usedDisk, freeDisk );

	char buf[1024];
	sprintf( buf, "disk:%lld:%lld|mem:%lld:%lld|cpu:%lld:%lld", 
			 usedDisk, freeDisk, totm-freem, freem, usercpu+syscpu, 100-usercpu-syscpu );
	res = buf;
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "numservs|numDBs|numTables|selects|inserts|updates|deletes|usersessions"
// pmesg: "_mon_clusteropinfo"
void JagDBServer::sendClusterOpInfo( const char *mesg, const JagRequest &req )
{
	Jstr res = getClusterOpInfo( req );
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "disk:123:322|mem:234:123|cpu:23:12";  in GB  used:free
// pmesg: "_mon_hosts"
void JagDBServer::sendHostsInfo( const char *mesg, const JagRequest &req )
{
	Jstr res;
	res = _dbConnector->_nodeMgr->_allNodes;
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "ip1|ip2|ip3";  in GB  used:free
// pmesg: "_mon_remote_backuphosts"
void JagDBServer::sendRemoteHostsInfo( const char *mesg, const JagRequest &req )
{
	Jstr res = _cfg->getValue("REMOTE_BACKUP_SERVER", "0" );
	sendMessageLength( req, res.c_str(), res.size(), "OK" );
}

// client expects: "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp
// pmesg: "_mon_local_stat6"
void JagDBServer::sendLocalStat6( const char *mesg, const JagRequest &req )
{
	jagint totalDiskGB, usedDiskGB, freeDiskGB, nproc, tcp;
	float loadvg;
	_jagSystem.getStat6( totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	char line[256];
	sprintf(line, "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	sendMessageLength( req, line, strlen(line), "OK" );
}

// client expects: "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp
// loadvg is avg, others are accumulative from all nodes
// pmesg: "_mon_cluster_stat6"
void JagDBServer::sendClusterStat6( const char *mesg, const JagRequest &req )
{
	jagint totalDiskGB=0, usedDiskGB=0, freeDiskGB=0, nproc=0, tcp=0;
	float loadvg;

	_jagSystem.getStat6( totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	char line[256];
	sprintf(line, "%lld|%lld|%lld|%lld|%.2f|%lld", totalDiskGB, usedDiskGB, freeDiskGB, nproc, loadvg, tcp );
	Jstr self = line;

	Jstr resp, bcasthosts;
	resp = _dbConnector->broadcastGet( "_mon_local_stat6", bcasthosts ); 
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
	sendMessageLength( req, line, strlen(line), "OK" );
}

// 0: not done; 1: done
// pmesg: "_ex_proclocalbackup"
void JagDBServer::processLocalBackup( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "localbackup rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		raydebug( stdout, JAG_LOG_LOW, "localbackup not processed\n" );
		sendMessage( req, "localbackup is not setup and not processed", "ER" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "localbackup started\n" );
	sendMessage( req, "localbackup started...", "OK" );
	
	// copy local data
	Jstr tmstr = JagTime::YYYYMMDDHHMM();
	copyLocalData("last", "OVERWRITE", tmstr, true );

	// bcast to other servers
	Jstr bcastCmd, bcasthosts;
	bcastCmd = Jstr("_serv_dolocalbackup|") + tmstr;
	raydebug( stdout, JAG_LOG_LOW, "broadcast localbackup to all servers ...\n" ); 
	_dbConnector->broadcastSignal( bcastCmd, bcasthosts );
	raydebug( stdout, JAG_LOG_LOW, "localbackup finished\n" );
	sendMessage( req, "localbackup finished", "OK" );
	sendMessage( req, "_END_[T=30|E=]", "ED" );
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
		if ( req.session ) sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	// if i am host0, do remotebackup
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		prt(("s2930 processRemoteBackup not _isHost0OfCluster0 skip\n"));
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	if ( _restartRecover ) {
		prt(("s2931 in recovery, skip processRemoteBackup\n"));
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	Jstr rs = _cfg->getValue("REMOTE_BACKUP_SERVER", "" );
	Jstr passwdfile = _cfg->getConfHOME() + "/syncpass.txt";
	chmod( passwdfile.c_str(), 0600 );
	Jstr passwd;
	JagFileMgr::readTextFile( passwdfile, passwd );
	passwd = trimChar( passwd, '\n');
	if ( rs.size() < 1 || passwd.size() < 1 ) {
		// do not comment out
		raydebug( stdout, JAG_LOG_LOW, "REMOTE_BACKUP_SERVER/syncpass.txt empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "remotebackup not processed\n" );
		if ( req.session ) sendMessage( req, "remotebackup is not setup and not processed", "ER" );
		if ( req.session ) sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	raydebug( stdout, JAG_LOG_LOW, "remotebackup started\n" );
	if ( req.session ) sendMessage( req, "remotebackup started...", "OK" );
	
	JagStrSplit sp (rs, '|');
	Jstr remthost;
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
    	Jstr bcastCmd, bcasthosts;
    	bcastCmd = Jstr("_serv_doremotebackup|") + remthost +"|" + passwd;
    	raydebug( stdout, JAG_LOG_LOW, "broadcast remotebackup to all servers ...\n" ); 
    	_dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 

		jagsleep(10, JAG_SEC);
	}
	raydebug( stdout, JAG_LOG_LOW, "remotebackup finished\n" );
	if ( req.session ) sendMessage( req, "remotebackup finished", "OK" );
	if ( req.session ) sendMessage( req, "_END_[T=30|E=]", "ED" );
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
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	// if i am host0, do remotebackup
	if ( ! _dbConnector->_nodeMgr->_isHost0OfCluster0 ) {
		prt(("s2930 processRestoreRemote not _isHost0OfCluster0 skip\n"));
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	if ( _restartRecover ) {
		prt(("s2931 in recovery, skip processRestoreRemote\n"));
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	JagStrSplit sp( mesg, '|' );
	if ( sp.length() < 3 ) {
		raydebug( stdout, JAG_LOG_LOW, "processRestoreRemote pmsg empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	Jstr remthost  = sp[1];
	Jstr passwd  = sp[2];
	if ( remthost.size() < 1 ) {
		raydebug( stdout, JAG_LOG_LOW, "processRestoreRemote IP empty, skip\n" ); 
		raydebug( stdout, JAG_LOG_LOW, "restorefromremote not processed\n" );
		sendMessage( req, "restorefromremote is not setup and not processed", "ER" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}
	
	prt(("s6370 _restorefromremote mesg=[%s]\n", mesg ));
	raydebug( stdout, JAG_LOG_LOW, "restorefromremote started\n" );
	sendMessage( req, "restorefromremote started... please restart jaguar after completion", "OK" );

    // bcast to other servers
    Jstr bcastCmd, bcasthosts;
    bcastCmd = Jstr("_serv_dorestoreremote|") + remthost +"|" + passwd;
    raydebug( stdout, JAG_LOG_LOW, "broadcast restoreremote to all servers ...\n" ); 
    _dbConnector->broadcastSignal( bcastCmd, bcasthosts ); 

	jagsleep(3, JAG_SEC);

	Jstr rmsg = Jstr("_serv_dorestoreremote|") + remthost + "|" + passwd;
	doRestoreRemote( rmsg.c_str(), req );
	// doRestoreRemote( remthost, passwd );
	raydebug( stdout, JAG_LOG_LOW, "restorefromremote finished\n" );
	sendMessage( req, "restorefromremote finished", "OK" );
	sendMessage( req, "_END_[T=30|E=]", "ED" );
}

void JagDBServer::addClusterMigrate( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "add cluster rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	Jstr oldHosts = _dbConnector->_nodeMgr->_allNodes.s(); // existing hosts "ip1|ip2|ip3"
	this->_migrateOldHosts = oldHosts;
	JagHashMap<AbaxString, AbaxInt> ipmap;

	int elen = strlen("_ex_addclust_migrate") + 2; 
	
	const char *end = strchr( mesg+elen, '|' ); // skip _ex_addcluster_migrate|#
	if ( !end ) end = strchr( mesg+elen, '!' );
	if ( !end ) end = mesg+elen;
	Jstr hstr = mesg+elen-1, absfirst( mesg+elen, end-mesg-elen );
	// split to get original cluster(s) and new added cluster
	JagStrSplit sp( hstr, '!', true );
	JagStrSplit sp2( sp[1], '|', true ); // new nodes
	_objectLock->writeLockSchema( -1 );
	
	// form new cluster.conf string as the form of: #\nip1\nip2\n#\nip3\nip4...
	int clusternum = 1;
	Jstr nhstr, ip, err, clustname;
	JagStrSplit sp3( sp[0], '#', true );
	for ( int i = 0; i < sp3.length(); ++i ) {
		++ clusternum;
		JagStrSplit sp4( sp3[i], '|', true );
		for ( int j = 0; j < sp4.length(); ++j ) {
			ip = JagNet::getIPFromHostName( sp4[j] );
			if ( ip.length() < 2 ) {
				err = Jstr( "_END_[T=130|E=Command Failed. Unable to resolve IP address of " ) +  sp4[j] ;
				raydebug( stdout, JAG_LOG_LOW, "E1300 addcluster error %s \n", err.c_str() );
				sendMessage( req, err.c_str(), "ER" );
				_objectLock->writeUnlockSchema( -1 );
				return;
			}

			if ( ! ipmap.keyExist( ip.c_str() ) ) {
				ipmap.addKeyValue( ip.c_str(), 1 );
				nhstr += ip + "\n";
			}
		}
	}

	// nhstr += "# Do not delete this line\n";
	//clustname = Jstr("# Subcluster ") + intToStr(clusternum) + " (New. Do not delete this line)";
	//nhstr += clustname + "\n";
	for ( int i = 0; i < sp2.length(); ++i ) { // new hosts
		// nhstr += sp2[i] + "\n";
		ip = JagNet::getIPFromHostName( sp2[i] );
		if ( ip.length() < 2 ) {
			err = Jstr( "_END_[T=130|E=Command Failed. Unable to resolve IP address of newhost " ) +  sp2[i] ;
			raydebug( stdout, JAG_LOG_LOW, "E1301 addcluster error %s \n", err.c_str() );
			sendMessage( req, err.c_str(), "ER" );
			_objectLock->writeUnlockSchema( -1 );
			return;
		}

		if ( ! ipmap.keyExist( ip.c_str() ) ) {
			ipmap.addKeyValue( ip.c_str(), 1 );
			nhstr += ip + "\n";
		}
	}
	// nhstr has all hosts now ( old + new )
	raydebug( stdout, JAG_LOG_LOW, "addcluster migrate updated set of hosts:\n%s\n", nhstr.c_str() );

	Jstr fpath, cmd, dirpath, tmppath, passwd = "dummy", unixSocket = Jstr("/TOKEN=") + _servToken;
	unsigned int uport = _port;
	// first, let host0 of cluster0 send schema info to new server(s)
	bool isDirector = false;
	if ( _dbConnector->_nodeMgr->_selfIP == absfirst ) {
		isDirector = true; // the main old host
		dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
		tmppath = _cfg->getTEMPDataHOME( JAG_MAIN );
		// make schema package -- empty database dirs and full system dir
		// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/a.tar.gz --no-recursion *;
		// tar -zcf /home/$USER/$BRAND/tmp/$DATA/b.tar.gz system
		cmd = Jstr("cd ") + dirpath + "; tar -zcf " + tmppath + "/a.tar.gz --no-recursion *; tar -zcf ";
		cmd += tmppath + "/b.tar.gz system";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6300 [%s]\n", cmd.c_str() );
		
		// untar the above two tar.gzs, remove them and remake a new tar.gz
		// cd /home/$USER/$BRAND/tmp/$DATA; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm a.tar.gz; b.tar.gz; tar -zcf a.tar.gz *
		cmd = Jstr("cd ") + tmppath + "; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm -f a.tar.gz b.tar.gz; tar -zcf c.tar.gz *";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6302 [%s]\n", cmd.c_str() );
		fpath = tmppath + "/c.tar.gz";
		// make connection and transfer package to each server
		for ( int i = 0; i < sp2.length(); ++i ) {
			fileTransmit( sp2[i], uport, passwd, unixSocket, 0, fpath, 1 );
		}
		jagunlink(fpath.c_str());
		// clean up tmp dir
		JagFileMgr::rmdir( tmppath, false );
	} else {
		prt(("s2767172 i am not host0 of cluster, no tar file and fileTransmit\n"));
	}
	
	// for new added servers, wait for package receive, and then format schema
	bool amNewNode = false;
	for ( int i = 0; i < sp2.length(); ++i ) {
		if ( _dbConnector->_nodeMgr->_selfIP == sp2[i] ) {
			amNewNode = true;
			prt(("s307371 I am a new server %s , wait for schema files to arrive ...\n", sp2[i].s() ));
			// is new server, waiting for package to receive
			while ( _addClusterFlag < 1 ) {
				jagsleep(1, JAG_SEC);
				prt(("s402981 sleep 1 sec ...\n"));
			}
			prt(("s307371 i am a new server %s , _addClusterFlag=%d schema files arrived\n", sp2[i].s(), (int)_addClusterFlag ));
			// received new schema package
			// 1. cp tar.gz to pdata and ndata
			// 2. drop old tables, untar packages, cp -rf of data to pdata and ndata and rebuild new table objects
			// 3. init clean other dirs ( /tmp etc. )
			JagRequest req;
			JagSession session;
			req.session = &session;
			session.servobj = this;

			// for data dir
			dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
			session.replicateType = 0;

			dropAllTablesAndIndex( req, _tableschema );

			JagFileMgr::rmdir( dirpath, false );
			fpath = _cfg->getTEMPDataHOME( JAG_MAIN ) + "/" + _crecoverFpath;
			cmd = Jstr("tar -zxf ") + fpath + " --directory=" + dirpath;
			system( cmd.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "s6304 [%s]\n", cmd.c_str() );
			jagunlink(fpath.c_str());
			raydebug( stdout, JAG_LOG_LOW, "delete %s\n", fpath.c_str() );
			
			// for pdata dir
			if ( _faultToleranceCopy >= 2 ) {
				dirpath = _cfg->getJDBDataHOME( JAG_PREV );
				session.replicateType = 1;
				dropAllTablesAndIndex( req, _prevtableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = Jstr("/bin/cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6307 [%s]\n", cmd.c_str() );
			}

			// for ndata dir
			if ( _faultToleranceCopy >= 3 ) {
				dirpath = _cfg->getJDBDataHOME( JAG_NEXT );
				session.replicateType = 2;
				dropAllTablesAndIndex( req, _nexttableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = Jstr("/bin/cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6308 [%s]\n", cmd.c_str() );
			}

			_objectLock->writeUnlockSchema( -1 );
			_objectLock->writeLockSchema( -1 );
			_crecoverFpath = "";

			// 2.
			makeNeededDirectories();

			_addClusterFlag = 0;
			break;
		} else {
		}
	}

	// then, for all servers, refresh cluster.conf and remake connections
	raydebug( stdout, JAG_LOG_LOW, "s11 existing allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );

	if ( ! isDirector || amNewNode ) {
		this->_isDirectorNode = false;
		this->_migrateOldHosts = "";
		_dbConnector->_nodeMgr->refreshClusterFile( nhstr );

		struct timeval now;
		gettimeofday( &now, NULL );
		g_lastHostTime = now.tv_sec * (jagint)1000000 + now.tv_usec;
		_objectLock->writeUnlockSchema( -1 );

		raydebug( stdout, JAG_LOG_LOW, "new node or follower, existing allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
		raydebug( stdout, JAG_LOG_LOW, "_sendAllNodes: [%s]\n", _dbConnector->_nodeMgr->_sendAllNodes.s() );

		jaguar_mutex_lock ( &g_dbconnectormutex );
		if ( _dbConnector ) {
			delete _dbConnector;
			raydebug( stdout, JAG_LOG_LOW, "done delete _dbConnector, new JagDBConnector...\n" );
			_dbConnector = newObject<JagDBConnector>( );
			raydebug( stdout, JAG_LOG_LOW, "done new JagDBConnector\n" );
		}
		jaguar_mutex_unlock ( &g_dbconnectormutex );

		raydebug( stdout, JAG_LOG_LOW, "crecoverRefreshSchema(0)... \n" );
		if ( amNewNode ) {
			crecoverRefreshSchema( JAG_MAKE_OBJECTS_CONNECTIONS, true );
		} else {
			crecoverRefreshSchema( JAG_MAKE_CONNECTIONS_ONLY, true );
		}
		raydebug( stdout, JAG_LOG_LOW, "crecoverRefreshSchema(0) done \n" );


		sendMessage( req, "_END_[T=30|E=]", "ED" );
		raydebug( stdout, JAG_LOG_LOW, "done new node or old non-main node, allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
		raydebug( stdout, JAG_LOG_LOW, "_sendAllNodes: [%s]\n", _dbConnector->_nodeMgr->_sendAllNodes.s() );

		return;

	}

	_objectLock->writeUnlockSchema( -1 );
	this->_isDirectorNode = true;


	struct timeval now;
	gettimeofday( &now, NULL );
	g_lastHostTime = now.tv_sec * (jagint)1000000 + now.tv_usec;

	raydebug( stdout, JAG_LOG_LOW, "s1927 existing allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );

	_dbConnector->_nodeMgr->refreshClusterFile( nhstr );
	jaguar_mutex_lock ( &g_dbconnectormutex );
	if ( _dbConnector ) {
		delete _dbConnector;
		_dbConnector = newObject<JagDBConnector>( );
	}
	jaguar_mutex_unlock ( &g_dbconnectormutex );
	raydebug( stdout, JAG_LOG_LOW, "s13 new allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
	raydebug( stdout, JAG_LOG_LOW, "s132 _sendAllNodes: [%s]\n", _dbConnector->_nodeMgr->_sendAllNodes.s() );

	crecoverRefreshSchema( JAG_MAKE_CONNECTIONS_ONLY, true );
	raydebug( stdout, JAG_LOG_LOW, "s23 new allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );

	sendMessage( req, "_END_[T=30|E=]", "ED" );

	raydebug( stdout, JAG_LOG_LOW, "s14 new allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
	raydebug( stdout, JAG_LOG_LOW, "s30087 addclustermigrate() begin is done\n" );
}


// after  preparing hosts, files, etc. now do broadcast to other servers
// method to add new servers in a cluster, do full data migration
// pmesg: "_ex_addclust_migrcontinue"
void JagDBServer::addClusterMigrateContinue( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "add cluster rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	Jstr oldHosts = this->_migrateOldHosts;

	if ( ! this->_isDirectorNode ) {
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;

	}

	JagVector<AbaxString> *vec = _tableschema->getAllTablesOrIndexesLabel( JAG_TABLE_OR_CHAIN_TYPE, "", "" );
	Jstr sql;
	int bad = 0;
	bool erc;
	Jstr dbtab, db, tab;
	for ( int j=0; j < vec->length(); ++j ) {
		dbtab = (*vec)[j].s();  // "db.tab123"
		sql = Jstr("select * from ") + dbtab + " export;";
		prt(("s2220833 dbtab=[%s] sql=[%s] broadcastSignal ...\n", dbtab.s(),  sql.s() ));
		erc = _dbConnector->broadcastSignal( sql, oldHosts, NULL, true ); // all hosts including self
		prt(("s2220834 dbtab=[%s] sql=[%s] broadcastSignal done erc=%d\n", dbtab.s(),  sql.s(), erc ));
		if ( erc ) {
			raydebug( stdout, JAG_LOG_LOW, "requested exporting %s\n", dbtab.s() );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "requesting exporting %s has error\n", dbtab.s() );
			bad ++;
			break;
		}
	}

	Jstr resp;
	if ( bad > 0 ) {
		for ( int j=0; j < vec->length(); ++j ) {
			dbtab = (*vec)[j].s();
			JagStrSplit sp( dbtab, '.' );
			db = sp[0];
			tab = sp[1];
			sql= Jstr("_ex_importtable|") + db + "|" + tab + "|YES";
			_dbConnector->broadcastSignal( sql, oldHosts, NULL, true ); 
			raydebug( stdout, JAG_LOG_LOW, "s387 broadcastSignal %s to hosts=[%s]\n", sql.s(), oldHosts.s() );
		}
	} else {
		for ( int j=0; j < vec->length(); ++j ) {
			dbtab = (*vec)[j].s();
			JagStrSplit sp( dbtab, '.' );
			db = sp[0];
			tab = sp[1];

			for ( int r = 0; r < _faultToleranceCopy; ++r ) {
				sql= Jstr("_ex_truncatetable|") + intToStr(r) + "|" + db + "|" + tab;
				_dbConnector->broadcastSignal( sql, oldHosts, NULL, true ); 
			}

			raydebug( stdout, JAG_LOG_LOW, "s387 broadcastSignal %s to hosts=[%s]\n", sql.s(), oldHosts.s() );
		}
	}

	if ( bad == 0 ) {
		for ( int j=0; j < vec->length(); ++j ) {
			dbtab = (*vec)[j].s();
			JagStrSplit sp( dbtab, '.' );
			db = sp[0];
			tab = sp[1];
			sql= Jstr("_ex_importtable|") + db + "|" + tab + "|NO";
			_dbConnector->broadcastSignal( sql, oldHosts, NULL, true ); 
			raydebug( stdout, JAG_LOG_LOW, "s387 broadcastSignal %s to hosts=[%s]\n", sql.s(), oldHosts.s() );
		}

		//sendMessageLength( req, resp.s(), resp.size(), "OK");  
	}

	delete vec;
	sendMessage( req, "_END_[T=30|E=]", "ED" );

	raydebug( stdout, JAG_LOG_LOW, "s14 new allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
	raydebug( stdout, JAG_LOG_LOW, "s30087 addclustermigratecontnue() broadcast is done bad=%d\n", bad );
}


// method to add new cluster
// pmesg: "_ex_addcluster|#ip1|ip2#ip3|ip4!ip5|ip6"
void JagDBServer::addCluster( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "add cluster rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	JagHashMap<AbaxString, AbaxInt> ipmap;

	int elen = strlen("_ex_addcluster") + 2; 
	const char *end = strchr( mesg+elen, '|' ); // skip _ex_addcluster|#
	if ( !end ) end = strchr( mesg+elen, '!' );
	if ( !end ) end = mesg+elen;
	Jstr hstr = mesg+elen-1, absfirst( mesg+elen, end-mesg-elen );
	JagStrSplit sp( hstr, '!', true );
	JagStrSplit sp2( sp[1], '|', true );
	_objectLock->writeLockSchema( -1 );
	
	int clusternum = 1;
	Jstr nhstr, ip, err, clustname;
	JagStrSplit sp3( sp[0], '#', true );
	for ( int i = 0; i < sp3.length(); ++i ) {
		clustname = Jstr("# Subcluster ") + intToStr(clusternum) + " (Do not delete this line)";
		nhstr += clustname + "\n";
		++ clusternum;
		JagStrSplit sp4( sp3[i], '|', true );
		for ( int j = 0; j < sp4.length(); ++j ) {
			ip = JagNet::getIPFromHostName( sp4[j] );
			// nhstr += sp4[j] + "\n";
			if ( ip.length() < 2 ) {
				err = Jstr( "_END_[T=130|E=Command Failed. Unable to resolve IP address of " ) +  sp4[j] ;
				raydebug( stdout, JAG_LOG_LOW, "E1300 addcluster error %s \n", err.c_str() );
				sendMessage( req, err.c_str(), "ER" );
				_objectLock->writeUnlockSchema( -1 );
				return;
			}

			if ( ! ipmap.keyExist( ip.c_str() ) ) {
				ipmap.addKeyValue( ip.c_str(), 1 );
				nhstr += ip + "\n";
			}
		}
	}

	// nhstr += "# Do not delete this line\n";
	clustname = Jstr("# Subcluster ") + intToStr(clusternum) + " (New hosts. Do not delete this line)";
	nhstr += clustname + "\n";
	for ( int i = 0; i < sp2.length(); ++i ) {
		// nhstr += sp2[i] + "\n";
		ip = JagNet::getIPFromHostName( sp2[i] );
		if ( ip.length() < 2 ) {
			err = Jstr( "_END_[T=130|E=Command Failed. Unable to resolve IP address of newhost " ) +  sp2[i] ;
			raydebug( stdout, JAG_LOG_LOW, "E1301 addcluster error %s \n", err.c_str() );
			sendMessage( req, err.c_str(), "ER" );
			_objectLock->writeUnlockSchema( -1 );
			return;
		}

		if ( ! ipmap.keyExist( ip.c_str() ) ) {
			ipmap.addKeyValue( ip.c_str(), 1 );
			nhstr += ip + "\n";
		}
	}
	raydebug( stdout, JAG_LOG_LOW, "addcluster newhosts:\n%s\n", nhstr.c_str() );

	Jstr fpath, cmd, dirpath, tmppath, passwd = "dummy", unixSocket = Jstr("/TOKEN=") + _servToken;
	unsigned int uport = _port;
	// first, let host0 of cluster0 send schema info to new server(s)
	if ( _dbConnector->_nodeMgr->_selfIP == absfirst ) {
		dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
		tmppath = _cfg->getTEMPDataHOME( JAG_MAIN );
		// make schema package -- empty database dirs and full system dir
		// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/a.tar.gz --no-recursion *;
		// tar -zcf /home/$USER/$BRAND/tmp/$DATA/b.tar.gz system
		cmd = Jstr("cd ") + dirpath + "; tar -zcf " + tmppath + "/a.tar.gz --no-recursion *; tar -zcf ";
		cmd += tmppath + "/b.tar.gz system";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6300 [%s]\n", cmd.c_str() );
		
		// untar the above two tar.gzs, remove them and remake a new tar.gz
		// cd /home/$USER/$BRAND/tmp/$DATA; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm a.tar.gz; b.tar.gz; tar -zcf a.tar.gz *
		cmd = Jstr("cd ") + tmppath + "; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm -f a.tar.gz b.tar.gz; tar -zcf c.tar.gz *";
		system(cmd.c_str());
		raydebug( stdout, JAG_LOG_LOW, "s6302 [%s]\n", cmd.c_str() );
		fpath = tmppath + "/c.tar.gz";
		// make connection and transfer package to each server
		for ( int i = 0; i < sp2.length(); ++i ) {
			fileTransmit( sp2[i], uport, passwd, unixSocket, 0, fpath, 1 );
		}
		jagunlink(fpath.c_str());
		// clean up tmp dir
		JagFileMgr::rmdir( tmppath, false );
	} else {
		prt(("s276717 i am not host0 of cluster, no tar file and fileTransmit\n"));
	}
	
	// for new added servers, wait for package receive, and then format schema
	//bool amNewNode = false;
	for ( int i = 0; i < sp2.length(); ++i ) {
		if ( _dbConnector->_nodeMgr->_selfIP == sp2[i] ) {
			while ( _addClusterFlag < 1 ) {
				jagsleep(1, JAG_SEC);
				prt(("s402981 sleep 1 sec ...\n"));
			}
			// received new schema package
			// 1. cp tar.gz to pdata and ndata
			// 1. drop old tables, untar packages, cp -rf of data to pdata and ndata and rebuild new table objects
			// 3. init clean other dirs ( /tmp etc. )
			// 1.
			JagRequest req;
			JagSession session;
			req.session = &session;
			session.servobj = this;

			// for data dir
			dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
			session.replicateType = 0;
			dropAllTablesAndIndex( req, _tableschema );
			JagFileMgr::rmdir( dirpath, false );
			fpath = _cfg->getTEMPDataHOME( JAG_MAIN ) + "/" + _crecoverFpath;
			cmd = Jstr("tar -zxf ") + fpath + " --directory=" + dirpath;
			system( cmd.c_str() );
			raydebug( stdout, JAG_LOG_LOW, "s6304 [%s]\n", cmd.c_str() );
			jagunlink(fpath.c_str());
			raydebug( stdout, JAG_LOG_LOW, "delete %s\n", fpath.c_str() );
			
			// for pdata dir
			if ( _faultToleranceCopy >= 2 ) {
				dirpath = _cfg->getJDBDataHOME( JAG_PREV );
				session.replicateType = 1;
				dropAllTablesAndIndex( req, _prevtableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = Jstr("/bin/cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6307 [%s]\n", cmd.c_str() );
			}

			// for ndata dir
			if ( _faultToleranceCopy >= 3 ) {
				dirpath = _cfg->getJDBDataHOME( JAG_NEXT );
				session.replicateType = 2;
				dropAllTablesAndIndex( req, _nexttableschema );
				JagFileMgr::rmdir( dirpath, false );
				cmd = Jstr("/bin/cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
				system( cmd.c_str() );
				raydebug( stdout, JAG_LOG_LOW, "s6308 [%s]\n", cmd.c_str() );
			}

			_objectLock->writeUnlockSchema( -1 );
			crecoverRefreshSchema( JAG_MAKE_OBJECTS_ONLY );
			_objectLock->writeLockSchema( -1 );
			_crecoverFpath = "";

			// 2.
			makeNeededDirectories();

			_addClusterFlag = 0;
			break;
		}
	}

	// then, for all servers, refresh cluster.conf and remake connections
	_dbConnector->_nodeMgr->refreshClusterFile( nhstr );
	crecoverRefreshSchema( JAG_MAKE_CONNECTIONS_ONLY );
	_localInternalIP = _dbConnector->_nodeMgr->_selfIP;
	sendMessage( req, "_END_[T=30|E=]", "ED" );

	struct timeval now;
	gettimeofday( &now, NULL );
	g_lastHostTime = now.tv_sec * (jagint)1000000 + now.tv_usec;
	_objectLock->writeUnlockSchema( -1 );

	raydebug( stdout, JAG_LOG_LOW, "s30087 addcluster() is done\n" );
}



// method to cleanup saved sql files for import
// pmesg: "_ex_addclustr_mig_complete|#ip1|ip2#ip3|ip4!ip5|ip6"
void JagDBServer::addClusterMigrateComplete( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" || !req.session->exclusiveLogin ) {
		raydebug( stdout, JAG_LOG_LOW, "add cluster rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	Jstr oldHosts = this->_migrateOldHosts;
	if ( oldHosts.size() < 1 ) {
		raydebug( stdout, JAG_LOG_LOW, "I am a new host or non-main host. Do not do broadcast\n" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	JagVector<AbaxString> *vec = _tableschema->getAllTablesOrIndexesLabel( JAG_TABLE_OR_CHAIN_TYPE, "", "" );
	Jstr sql;
	int bad = 0;
	Jstr dbtab, db, tab;

	for ( int j=0; j < vec->length(); ++j ) {
		dbtab = (*vec)[j].s();
		JagStrSplit sp( dbtab, '.' );
		db = sp[0];
		tab = sp[1];
		sql= Jstr("_ex_importtable|") + db + "|" + tab + "|YES";
		_dbConnector->broadcastSignal( sql, oldHosts, NULL, true ); 
		raydebug( stdout, JAG_LOG_LOW, "s387 broadcastSignal %s to hosts=[%s]\n", sql.s(), oldHosts.s() );
	}

	if ( vec ) delete vec;
	sendMessage( req, "_END_[T=30|E=]", "ED" );

	raydebug( stdout, JAG_LOG_LOW, "s14 new allnodes: [%s]\n", _dbConnector->_nodeMgr->_allNodes.s() );
	raydebug( stdout, JAG_LOG_LOW, "s30087 addclustermigratecomplete() is done bad=%d\n", bad );

}

// method to request schema info from old datacenter to current one
void JagDBServer::requestSchemaFromDataCenter()
{
	Jstr fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) || !_isGate ) {
		// no datacenter file exists, or requested server is not GATE, return
		return;
	}
	
	int rc = 0, succ = 0, MAXTRY = 12, trynum;
	Jstr adminpass = "dummy";
	JagVector<Jstr> vec;
	JagVector<Jstr> vecip;
	JagVector<Jstr> vecdc; // vector of GATE hosts
	JagVector<Jstr> dcport; // vector of GATE hosts' port
	JagVector<Jstr> vecinnerdc; // vector of HOST hosts
	JagVector<Jstr> innerdcport; // vector of HOST hosts' port
	Jstr ip;
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );
		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.length() > 0 ) {
			vecip.append( ip );
		}
	}
		
	Jstr seeds, host, port, dcmd = "_serv_reqschemafromdc|", dcmd2 = "_serv_askdatafromdc|";
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	Jstr bcasthosts, destType, uphost;
	
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
	Jstr unixSocket = Jstr("/DATACENTER=1") + srcDestType( _isGate, destType ) +
	      						Jstr("/TOKEN=") + _servToken;

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
				jagsleep(10, JAG_SEC );
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
	
	if ( pcli->queryDirect( 0, dcmd.c_str(), dcmd.size(), true, false, true ) == 0 ) {
		delete pcli;
        return;
	}
	
	// wait for schema trasmit package, regard as add cluster
	_objectLock->writeLockSchema( -1 );
	while ( _addClusterFlag < 1 ) {
		jagsleep(1, JAG_SEC);
	}
	destType = "HOST";
	unixSocket = Jstr("/DATACENTER=1") + srcDestType( _isGate, destType ) +
	 				Jstr("/TOKEN=") + _servToken;
	// then, send package to all other servers -- inner datacenter
	Jstr bcmd = "_serv_unpackschinfo"; JagRequest req;
	fpath = _cfg->getTEMPDataHOME( JAG_MAIN ) + "/" + _crecoverFpath;
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
				jagsleep(10, JAG_SEC );
			}
			++trynum;
		}

		if ( 0 == rc && trynum > MAXTRY ) {
			raydebug( stdout, JAG_LOG_LOW, "failed connect to datacenter=%s retry with next datacenter\n", vecinnerdc[i].c_str() );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "connected to datacenter=%s, beginning to request schema ...\n", vecinnerdc[i].c_str() );
			// when connect success, get the host lists and send schema info to all inner datacenter hosts
			JagStrSplit sp2( pcli2._allHostsString, '#', true );			
			for ( int i = 0; i < sp2.length(); ++i ) {
				JagVector<Jstr> chosts;
				JagStrSplit sp3( sp2[i], '|', true );
				for ( int j = 0; j < sp3.length(); ++j ) {
					fileTransmit( sp3[j], atoi( innerdcport[i].c_str() ), "dummy", unixSocket, 0, fpath, 1 );
					// then, ask each server to unpack schema info
					JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pcli2._connMap, sp3[j].c_str() );
					if ( jcli->queryDirect( 0, bcmd.c_str(), bcmd.size(), true, false, true ) != 0 ) {
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
	if ( pass->cli->queryDirect( 0, pass->cmd.c_str(), pass->cmd.size(), true, false, true ) == 0 ) {
		delete pass->cli;
		return NULL;
	}
	pass->cli->replyAll();
	delete pass->cli;
	return NULL;
}

// method to send schema info to new datacenter's gate, as requested
// pmesg: "_serv_reqschemafromdc|newdc_host|newdc_port"
void JagDBServer::sendSchemaToDataCenter( const char *mesg, const JagRequest &req )
{
	Jstr hstr = mesg+22;
	// split to get original cluster(s) and new added cluster
	JagStrSplit sp( hstr, '|', true );
	if ( sp.length() < 2 ) {
		return;
	}
	Jstr host = sp[0];
	unsigned int port = atoi ( sp[1].c_str() );
	Jstr adminpass = "dummy";
	Jstr unixSocket = Jstr("/DATACENTER=1") + Jstr("/TOKEN=") + _servToken;
	Jstr fpath, cmd, dirpath, tmppath;
	_objectLock->writeLockSchema( -1 );

	dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
	tmppath = _cfg->getTEMPDataHOME( JAG_MAIN );
	// make schema package -- empty database dirs and full system dir
	// cd /home/$USER/$BRAND/$DATA; tar -zcf /home/$USER/$BRAND/tmp/$DATA/a.tar.gz --no-recursion *;
	// tar -zcf /home/$USER/$BRAND/tmp/$DATA/b.tar.gz system
	cmd = Jstr("cd ") + dirpath + "; tar -zcf " + tmppath + "/a.tar.gz --no-recursion *; tar -zcf ";
	cmd += tmppath + "/b.tar.gz system";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6300 [%s]\n", cmd.c_str() );
	
	// untar the above two tar.gzs, remove them and remake a new tar.gz
	// cd /home/$USER/$BRAND/tmp/$DATA; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm a.tar.gz; b.tar.gz; tar -zcf a.tar.gz *
	cmd = Jstr("cd ") + tmppath + "; tar -zxf a.tar.gz; tar -zxf b.tar.gz; rm -f a.tar.gz b.tar.gz; tar -zcf c.tar.gz *";
	system(cmd.c_str());
	raydebug( stdout, JAG_LOG_LOW, "s6302 [%s]\n", cmd.c_str() );
	fpath = tmppath + "/c.tar.gz";
	// make connection and transfer package to new server
	fileTransmit( host, port, adminpass, unixSocket, 0, fpath, 1 );
	//jagunlink tar.gz file
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
	JagSession session;
	req2.session = &session;
	session.servobj = this;

	// for data dir
	Jstr dirpath = _cfg->getJDBDataHOME( JAG_MAIN );
	session.replicateType = 0;
	dropAllTablesAndIndex( req2, _tableschema );
	JagFileMgr::rmdir( dirpath, false );
	Jstr fpath = _cfg->getTEMPDataHOME( JAG_MAIN ) + "/" + _crecoverFpath;
	Jstr cmd = Jstr("tar -zxf ") + fpath + " --directory=" + dirpath;
	system( cmd.c_str() );
	raydebug( stdout, JAG_LOG_LOW, "s6304 [%s]\n", cmd.c_str() );
	jagunlink(fpath.c_str());
	raydebug( stdout, JAG_LOG_LOW, "delete %s\n", fpath.c_str() );
	
	// for pdata dir
	if ( _faultToleranceCopy >= 2 ) {
		dirpath = _cfg->getJDBDataHOME( JAG_PREV );
		session.replicateType = 1;
		dropAllTablesAndIndex( req2, _prevtableschema );
		JagFileMgr::rmdir( dirpath, false );
		cmd = Jstr("cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
		system( cmd.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "s6307 [%s]\n", cmd.c_str() );
	}

	// for ndata dir
	if ( _faultToleranceCopy >= 3 ) {
		dirpath = _cfg->getJDBDataHOME( JAG_NEXT );
		session.replicateType = 2;
		dropAllTablesAndIndex( req2, _nexttableschema );
		JagFileMgr::rmdir( dirpath, false );
		cmd = Jstr("cp -rf ") + _cfg->getJDBDataHOME( JAG_MAIN ) + "/* " + dirpath;
		system( cmd.c_str() );
		raydebug( stdout, JAG_LOG_LOW, "s6308 [%s]\n", cmd.c_str() );
	}
	_objectLock->writeUnlockSchema( -1 );
	crecoverRefreshSchema( JAG_MAKE_OBJECTS_ONLY );
	_objectLock->writeLockSchema( -1 );
	_crecoverFpath = "";

	// 2.
	makeNeededDirectories();

	crecoverRefreshSchema( JAG_MAKE_CONNECTIONS_ONLY );

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
	Jstr bcmd = Jstr("_serv_preparedatafromdc|") + _dbConnector->_nodeMgr->_selfIP.c_str() + "|" 
				+ intToStr(_port) + "|" + (mesg+20);
	
	Jstr fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) || !_isGate ) {
		// no datacenter file exists, or is not GATE, return
		return;
	}
	
	int rc = 0, MAXTRY = 12, trynum;
	Jstr adminpass = "dummy";
		
	Jstr uphost, seeds, host, port, destType;
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
	Jstr unixSocket = Jstr("/DATACENTER=1") + srcDestType( _isGate, destType ) + Jstr("/TOKEN=") + _servToken;	
	pcli._datcSrcType = JAG_DATACENTER_GATE;
	pcli._datcDestType = JAG_DATACENTER_HOST;
	rc = 0; trynum = 0;
	while ( ! rc ) {
		if ( 0 == rc && trynum > MAXTRY ) { break; }
		rc = pcli.connect( host.c_str(), atoi( port.c_str() ), "admin", adminpass.c_str(), "test", unixSocket.c_str(), 0 );
		if ( 0 == rc ) {
			raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
					  host.c_str(), pcli.error(), trynum, MAXTRY );
			jagsleep(10, JAG_SEC );
		}
		++trynum;
	}

	if ( 0 == rc && trynum > MAXTRY ) {
		raydebug( stdout, JAG_LOG_LOW, "failed connect to datacenter=%s retry with next datacenter\n", host.c_str() );
	} else {
		raydebug( stdout, JAG_LOG_LOW, "connected to datacenter=%s, beginning to request schema ...\n", host.c_str() );
		JagVector<JaguarCPPClient*> jclilist;
		JagStrSplit sp2( pcli._allHostsString, '#', true );			
		// send query to each server
		for ( int i = 0; i < sp2.length(); ++i ) {
			JagVector<Jstr> chosts;
			JagStrSplit sp3( sp2[i], '|', true );
			for ( int j = 0; j < sp3.length(); ++j ) {
				JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pcli._connMap, sp3[j].c_str() );
				if ( jcli->queryDirect( 0, bcmd.c_str(), bcmd.size(), true, false, true ) != 0 ) {
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
		return;
	}
	Jstr host = sp[0], destType = "GATE", spstr = "insertsyncdconly|" + sp[2] + "|" + sp[3] + " ";
	unsigned int port = atoi ( sp[1].c_str() );
	Jstr adminpass =  "dummy";
	Jstr unixSocket = Jstr("/DATACENTER=1") + srcDestType( _isGate, destType ) +
					  Jstr("/TOKEN=") + _servToken;
	int rc = 0, trynum = 0, MAXTRY = 12;
	JaguarCPPClient pcli;
	while ( ! rc ) {
		if ( 0 == rc && trynum > MAXTRY ) { break; }
		rc = pcli.connect( host.c_str(), port, "admin", adminpass.c_str(), "test", unixSocket.c_str(), 0 );
		if ( 0 == rc ) {
			raydebug( stdout, JAG_LOG_LOW, "Error connect to datacenter [%s], %s, retry %d/%d ...\n", 
					  host.c_str(), pcli.error(), trynum, MAXTRY );
			jagsleep(10, JAG_SEC );
		}
		++trynum;
	}

	if ( ! rc ) {
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
	Jstr str = _objectLock->getAllTableNames( 0 ), cmd, reterr, dbtab, dirpath, objname, fpath;
	JagStrSplit sp2( str, '|', true );
	jagint threadSchemaTime = 0, threadQueryTime = 0;
	for ( int i = 0; i < sp2.length(); ++i ) {
		cmd = Jstr("select * from ") + sp2[i] + " export;";
		JagParseAttribute jpa( this, servtimediff, servtimediff, req2.session->dbname, _cfg );
		JagParser parser( (void*)this );
		JagParseParam pparam( &parser );
		if ( parser.parseCommand( jpa, cmd.c_str(), &pparam, reterr ) ) {
			// select table export;
			rc = processCmd( jpa, req2, cmd.c_str(), pparam, reterr, threadQueryTime, threadSchemaTime );
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
// pmesg: "_exe_shutdown"
void JagDBServer::shutDown( const char *mesg, const JagRequest &req )
{
	if ( req.session && (req.session->uid!="admin" || !req.session->exclusiveLogin ) ) {
		raydebug( stdout, JAG_LOG_LOW, "shut down rejected. admin exclusive login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}
	
	if ( _shutDownInProgress ) {
		raydebug( stdout, JAG_LOG_LOW, "Shutdown is already in progress. return\n");
		return;
	}
	
	Jstr stopPath = jaguarHome() + "/log/shutdown.cmd";
	JagFileMgr::writeTextFile(stopPath, "WIP");
	_shutDownInProgress = 1;
	
	_objectLock->writeLockSchema( -1 );

	while ( _taskMap->size() > 0 ) {
		jagsleep(1, JAG_SEC);
		raydebug( stdout, JAG_LOG_LOW, "Shutdown waits other tasks to finish ...\n");
	}

	JAG_BLURT jaguar_mutex_lock ( &g_dlogmutex ); JAG_OVER;
	
	if ( _delPrevOriCommandFile ) jagfsync( fileno( _delPrevOriCommandFile ) );
	if ( _delPrevRepCommandFile ) jagfsync( fileno( _delPrevRepCommandFile ) );
	if ( _delPrevOriRepCommandFile ) jagfsync( fileno( _delPrevOriRepCommandFile ) );
	if ( _delNextOriCommandFile ) jagfsync( fileno( _delNextOriCommandFile ) );
	if ( _delNextRepCommandFile ) jagfsync( fileno( _delNextRepCommandFile ) );
	if ( _delNextOriRepCommandFile ) jagfsync( fileno( _delNextOriRepCommandFile ) );
	if ( _recoveryRegCommandFile ) jagfsync( fileno( _recoveryRegCommandFile ) );
	if ( _recoverySpCommandFile ) jagfsync( fileno( _recoverySpCommandFile ) );

	jaguar_mutex_unlock ( &g_dlogmutex );

	raydebug( stdout, JAG_LOG_LOW, "Shutdown: Flushing block index to disk ...\n");
	flushAllBlockIndexToDisk();
	raydebug( stdout, JAG_LOG_LOW, "Shutdown: Completed\n");

	JagFileMgr::writeTextFile(stopPath, "DONE");
	jagsync();
	_objectLock->writeUnlockSchema( -1 );
	exit(0);
}

// method to do inner/outer join for multiple tables/indexs
int JagDBServer::joinObjects( const JagRequest &req, JagParseParam *parseParam, Jstr &reterr )
{
	// first, only accept two table inner join
	if ( parseParam->objectVec.length() != 2 || parseParam->opcode != JAG_INNERJOIN_OP ) {
		reterr = "E2202 Only accept two tables/indexs innerjoin";
		return 0;
	}

	// disable join for multi-host cluster
	if ( _dbConnector->_nodeMgr->_numAllNodes > 1 ) {
		reterr = "E2203 Join is not supported on a multi-node database system";
		return 0;
	}

	parseParam->initJoinColMap();

	JagTableSchema *tableschema; 
	JagIndexSchema *indexschema;
	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );
	
	// init variables
	struct timeval now;
	gettimeofday( &now, NULL );
	int num = 2;  // 2 obects join only
	int rc, tmode = 0, tnum = 0, st, nst;
	int collen, siglen, cmode = 0, useHash = -1;
	int numCPUs = _numCPUs; 
	jagint i, j, stime = now.tv_sec, totlen = 0, finalsendlen = 0, gbvsendlen = 0;
	int pos;
	jagint offset = 0, objelem[num], jlen[num], sklen[num];
	char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
	Jstr ttype = " ";
	char *newbuf = NULL;
	bool hasValCol = 0, timeout = 0, isagg, hasagg = 0, dfSorted[num];
	JagFixString rstr, tstr, fkey, fval;
	Jstr jname, jpath, jdapath, jdapath2, sigpath, sigpath2, sigpath3, newhdr, gbvhdr;
	Jstr unixSocket = Jstr("/TOKEN=") + _servToken;
	ExprElementNode *root = NULL;
	JagDataAggregate *jda = NULL;
	JagVector<jagint> jsvec[num];
	JaguarCPPClient hcli;
	hcli._sleepTime = 1; // reduce regular client object monitor sleep time to 1*1000
	
	// get all ptabs/pindexs info
	JagTable *ptab[num]; 
	JagIndex *pindex[num];

	int klen[num], vlen[num], kvlen[num], numKeys[num], numCols[num];
	const JagHashStrInt *maps[num]; 
	const JagSchemaAttribute *attrs[num]; 
	const JagSchemaRecord *rec[num];
	JagDiskArrayFamily *df[num]; JagMinMax minmax[num];
	Jstr dbidx[num], tabName[num], idxName[num];
	Jstr dbName, colName, tableName, oneFilter, rowFilter;

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
			pindex[i] = _objectLock->readLockIndex( parseParam->opcode, dbName, 
								tabName[i], parseParam->objectVec[i].indexName,
								req.session->replicateType, 0 );
			dbidx[i] = dbName;
			idxName[i] = parseParam->objectVec[i].indexName;
		} else {
			// table object or index object
			ptab[i] = _objectLock->readLockTable( parseParam->opcode, dbName, tableName, 
														   req.session->replicateType, 0 );
			if ( !ptab[i] ) {
				pindex[i] = _objectLock->readLockIndex( parseParam->opcode, dbName, 
								tabName[i], tableName, req.session->replicateType, 0 );

				dbidx[i] = dbName;
				idxName[i] = tableName;
				if ( !pindex[i] ) {
					pindex[i] = _objectLock->readLockIndex( parseParam->opcode, colName, 
									tabName[i], tableName, req.session->replicateType, 0 );
					dbidx[i] = colName;
					idxName[i] = tableName;
				}
			} else {
		    	prt(("s310087 got ptab[i=%d]\n", i ));
			}
		}
		
		if ( !ptab[i] && !pindex[i] ) {
			if ( colName.length() > 0 ) {
				reterr = Jstr("E4023 unable to select ") + tableName + "." + colName;
			} else {
				reterr = Jstr("E4024 unable to select ") + dbName + "." + tableName;
			}

			if ( parseParam->objectVec[i].indexName.size() > 0 ) {
				reterr += Jstr(".") + parseParam->objectVec[i].indexName;
			}
			break;
		}
		
		// otherwise, set the variables
		if ( ptab[i] ) {
			rc = checkUserCommandPermission( &ptab[i]->_tableRecord, req, *parseParam, i, oneFilter, reterr );
			if ( ! rc ) {
				reterr = Jstr("E4524 no permission for table");
				return 0;
			}
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
			objelem[i] = df[i]->getElements( );
		} else {
			rc = checkUserCommandPermission( &pindex[i]->_indexRecord, req, *parseParam, i, oneFilter, reterr );
			if ( ! rc ) {
				reterr = Jstr("E4525 No permission for index");
				return 0;
			}

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
			objelem[i] = df[i]->getElements( );			
		}
	}

	// send all table elements info to client
	Jstr elemInfo = Jstr("usehash|") + longToStr( objelem[0] ) + "|" + longToStr( objelem[1] );

	sendMessage( req, elemInfo.c_str(), "OK" );

 	char ehdr[JAG_SOCK_TOTAL_HDR_LEN+1];
	char *enewbuf = NULL;

	jagint clen = recvMessage( req.session->sock, ehdr, enewbuf );

	if ( clen > 0 ) {
		JagStrSplit sp( enewbuf, '|');
		//useHash = jagatoll( enewbuf );
		//sp[0] == "usehash";
		if ( 2 == sp.size() ) {
			useHash = jagatoll( sp[1].c_str() );
		}
	}
	if ( enewbuf ) jagfree ( enewbuf );
	// usehash: -1 for mergejoin; 0: left small, right big; 1: left big; right small

	// error, clean up users
	if ( reterr.length() > 0 ) {
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) {
				_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
														parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
			} else if ( pindex[i] ) {
				_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], 
														idxName[i], req.session->replicateType, 0 );				
			}
		}
		return 0;
	}

	// reparse where from rowFilter
	if ( reterr.length() < 1 && rowFilter.size() > 0  ) {
		parseParam->resetSelectWhere( rowFilter );
		parseParam->setSelectWhere( );
	}

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
		JagVector<SetHdrAttr> spa; 
		SetHdrAttr onespa;
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
	} // end if	

	jname = req.session->cliPID + "_" + req.session->ip;
	jpath = _cfg->getTEMPJoinHOME() + "/" + jname;
	jdapath = jpath + "/alldata";
	jdapath2 = jpath + "/finaldata";
	sigpath = jpath + "/fprep";
	JagFileMgr::makedirPath( jpath );
	JagFileMgr::makedirPath( sigpath );
	jda = newObject<JagDataAggregate>();
	jda->setwrite( jdapath, jdapath, 2 );
	jda->setMemoryLimit( _cfg->getLongValue("JOIN_MEMLINE", 100000) *totlen );	

	JagArray<AbaxPair<AbaxLong, jagint>> *sarr[num];
	for ( i = 0; i < num; ++i ) { sarr[i] = NULL; }

	JagVector<AbaxPair<Jstr,jagint>> jmarr = parseParam->joinColMap->getStrIntVector();
	Jstr kstr;
	for ( i = 0; i < jmarr.length(); ++i ) {
		if( jmarr[i].key.size() > 0 ) {
			kstr = jmarr[i].key;
			if ( maps[jmarr[i].value]->getValue( kstr, pos ) ) {
				AbaxPair<AbaxLong, jagint> pair( AbaxLong( pos ), jmarr[i].value );
				if ( !sarr[jmarr[i].value] ) {
					sarr[jmarr[i].value] = new JagArray<AbaxPair<AbaxLong, jagint>>();
				}
				sarr[jmarr[i].value]->insert( pair );
			}
		}
	}

	bool needSort = false;
	for ( i = 0; i < num; ++i ) {
		if ( ! sarr[i] ) continue;

		int hasval = 0, colContiguous = 1, keyCols = 0, hasAllKeyCols = 0;
		int lpos = -1, upos = 0; // track join-column positions
		AbaxPair<AbaxLong, jagint> pair;
		sarr[i]->initGetPosition( 0 );
		while ( sarr[i]->getNext( pair ) ) {
			upos = pair.key.value();
			jsvec[i].append( upos );
			jlen[i] += attrs[i][upos].length;
			if ( upos < numKeys[i] ) {
				++keyCols;
				if ( upos == lpos+1 ) {
					lpos = upos;
				} else {
					colContiguous = 0;
				}
			} else {
				hasval = 1; // has value column
			}
		}

		if ( keyCols >= numKeys[i] ) hasAllKeyCols = 1;
		else hasAllKeyCols = 0;


		// !colContiguous: keys are not contiguous in join columns
		// !hasAllKeyCols: not having all keys   in join columns
		// hasval: has a non-key column in join columns
		if ( !colContiguous || ( !hasAllKeyCols && hasval ) ) {
			if ( useHash < 0 ) {
				dfSorted[i] = 1;
				needSort = true;
			}
			sklen[i] = jlen[i] + JAG_UUID_FIELD_LEN;
		} else {
		}

		delete sarr[i];
	} // end for loop

	if ( needSort ) {
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) {
				_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
														parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
			} else if ( pindex[i] ) {
				_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], 
														idxName[i], req.session->replicateType, 0 );				
			}
		}

		sendMessage( req, "E80158 Join operation would take too long, aborted", "ER" );
		JagFileMgr::rmdir( jpath, true );
		delete jda;
		if ( newbuf ) free( newbuf );
		if ( enewbuf ) free( enewbuf );
		return 0;
	}


	if ( !hcli.connect( _dbConnector->_nodeMgr->_selfIP.c_str(), _port, 
					    "admin", "dummy", "test", unixSocket.c_str(), JAG_SERV_PARENT ) ) 
	{
		raydebug( stdout, JAG_LOG_LOW, "s4058 join connection, unable to make connection, retry ...\n" );
		if ( jda ) delete jda;
		if ( newbuf ) free( newbuf );
		if ( enewbuf ) free( enewbuf );
		for ( i = 0; i < num; ++i ) {
			if ( ptab[i] ) {
				_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
													   parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
			} else if ( pindex[i] ) {
				_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], idxName[i], req.session->replicateType, 0 );				
			}
		}
		hcli.close();
		JagFileMgr::rmdir( jpath, true );
		return 0;
	}
	
	if ( useHash == 0 || useHash > 0 ) {
		// use hash join
		JagStrSplit srv( _dbConnector->_nodeMgr->_allNodes, '|' );	
		if ( srv.length() < numCPUs ) numCPUs = srv.length();
		JagHashMap<JagFixString, JagFixString> hmaps[numCPUs];
		if ( 0 == useHash ) {
			st = 1; // bigger table
			nst = 0; // smaller table
		} else {
			st = 0; // bigger table
			nst = 1; // smaller table
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
		_joinMap->addKeyValue( jmkey, pair );
		prt(("s283766 sz=%lld k=[%s] sortklen=%lld, kvlen=%lld, numKs=%lld, minbf=[%s] maxbf=[%s] dfam=[%0x] atrs=[%0x] pprm=[%0x]\n", 
			  _joinMap->size(), jmkey.c_str(), sklen[nst], kvlen[nst], numKeys[nst], minmax[nst].minbuf,
			  minmax[nst].maxbuf, df[nst], attrs[nst], parseParam));
		sigpath2 = sigpath + "/" + intToStr( nst );
		JagFileMgr::makedirPath( sigpath2 );
		
		// for hash join, first ask each servers to send small table's data		
		pthread_t thrd[srv.length()];  // sr is all servers splitvec
		ParallelJoinPass pjp[srv.length()];
		for ( i = 0; i < srv.length(); ++i ) {
			pjp[i].ptab = ptab[st]; 	// bigger table
			pjp[i].ptab2 = ptab[nst]; 	// smaller table
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
			jagpthread_create( &thrd[i], NULL, joinRequestStatic, (void*)&(pjp[i]) );
		}

		for ( i = 0; i < srv.length(); ++i ) {
			pthread_join(thrd[i], NULL);
			if ( pjp[i].timeout ) timeout = 1;
		}

		if ( !timeout ) {
			int stnum = df[st]->_darrlist.size();
			if ( stnum < 1 ) stnum = 1;
			pthread_t thrd2[ stnum ];

			ParallelJoinPass pjp2[ stnum ] ;

			for ( i = 0; i < stnum; ++i ) {
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
				pjp2[i].jlen2 = jlen[nst];
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
				jagpthread_create( &thrd2[i], NULL, joinHashMapData, (void*)&(pjp2[i]) );
			}

			for ( i = 0; i < stnum; ++i ) {
				pthread_join(thrd2[i], NULL);
				if ( pjp2[i].timeout ) timeout = 1;
			}
		}
	} else {
		pthread_t thrd[num]; // num=2
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
			pjp[i].jlen2 = jlen[nst];
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
			jagpthread_create( &thrd[i], NULL, doMergeJoinStatic, (void*)&pjp[i] );
		}
		
		while ( JagFileMgr::numObjects( sigpath ) < 2 ) {		
			jagsleep( 100000, JAG_USEC );
			if ( req.session->sessionBroken ) { 
				timeout = 1;
				break; 
			}
		}

		for ( i = 0; i < num; ++i ) {
			df[i] = pjp[i].df;
			dfSorted[i] = pjp[i].dfSorted;
			if ( pjp[i].timeout ) timeout = 1;
		}
		
		if ( !timeout ) {
			int 	wrc = 1; 
			int 	goNext[num];
			char 	*jbuf[num];
			const char *buffers[num], *buffers2[num];
			char 	*buf = (char*)jagmalloc( totlen+1 );
			JagMergeReader *jntr[num];

			memset( buf, 0, totlen+1 );
			j = 0;

			for ( i = 0; i < num; ++i ) {
				goNext[i] = 1; // both can go down
				jbuf[i] = (char*)jagmalloc( kvlen[i]+sklen[i]+1 );
				memset( jbuf[i], 0, kvlen[i]+sklen[i]+1 );
				jntr[i] = NULL;
				if ( !dfSorted[i] ) {
					df[i]->setFamilyRead( jntr[i], minmax[i].minbuf, minmax[i].maxbuf );
				} else {
					df[i]->setFamilyRead( jntr[i] );
				}
				buffers[i] = jbuf[i]+sklen[i];
				buffers2[i] = buf+j;
				j += kvlen[i];
				if ( !jntr[i] ) wrc = 0;
			}

			if ( wrc ) {
				jntr[1]->unsetMark();
				rc = advanceLeftTable( jntr[0], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
				if ( rc < 0 ) { goto outofloop; }

				rc = advanceRightTable( jntr[1], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
				if ( rc < 0 ) { goto outofloop; }

				while ( 1 ) {
					if ( checkCmdTimeout( stime, parseParam->timeout ) || req.session->sessionBroken ) {
						timeout = 1; break;
					}

					if ( ! jntr[1]->isMarked() ) {
						while ( memcmp( jbuf[0], jbuf[1], jlen[0] ) < 0 ) {
								// left < right
								rc = advanceLeftTable( jntr[0], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
								if ( rc < 0 ) { prt(("s8888203 left got < 0 \n")); goto outofloop; }
						}

						while ( memcmp( jbuf[0], jbuf[1], jlen[0] ) > 0 ) {
								// left > right
								rc = advanceRightTable( jntr[1], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
								if ( rc < 0 ) { prt(("s8888209 righ got < 0 \n")); goto outofloop; }
						}

						// left == right
						jntr[1]->setRestartPos();
					} // end if ! isMarkSet 

					if ( 0 == memcmp( jbuf[0], jbuf[1], jlen[0] ) ) {
						memcpy( buf, jbuf[0]+sklen[0], kvlen[0] );
						memcpy( buf+kvlen[0], jbuf[1]+sklen[1], kvlen[1] );
						jda->writeit( 0, buf, totlen, NULL, true );
						rc = advanceRightTable( jntr[1], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
						if ( rc < 0 ) {
							//goto outofloop;
							jbuf[1][0] = '\0';
							if ( ! jntr[1]->isMarked() ) {
								goto outofloop;
							}
						}
						continue;
					}

					jntr[1]->moveToRestartPos();
					// pop to jbuf[1]
					advanceRightTable( jntr[1], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);

					rc = advanceLeftTable( jntr[0], maps, jbuf, numKeys, sklen, attrs, parseParam, buffers);
					if ( rc < 0 ) { prt(("s11112301 left < 0\n")); goto outofloop; }
					jntr[1]->unsetMark();
				}  // end while(1)
			} // end if wrc (ntrs are all good)
			outofloop:

			for ( i = 0; i < num; ++i ) {
				pthread_join(thrd[i], NULL);
				if ( pjp[i].timeout ) timeout = 1;
				if ( jbuf[i] ) free( jbuf[i] );
				if ( jntr[i] ) delete jntr[i];
			}

			if ( buf ) free ( buf );
			//if ( cmpbuf ) free ( cmpbuf );
			// end of not timedout
		} else {
			// time out happened
			for ( i = 0; i < num; ++i ) {
				pthread_join(thrd[i], NULL);
			}
		}
	} // end regular merge join

	jda->flushwrite();
	
	// if has column calculations and/or group by, reformat data
	if (  !timeout && (parseParam->hasGroup || parseParam->hasColumn) ) {
		// then, setup finalbuf and gbvbuf if needed
		// finalbuf, hasColumn len or KEYVALLEN*n if !hasColumn
		// gbvbuf, if has group by
		bool hasFirst = 0; 
		std::atomic<jagint> cnt; cnt = 0; 
		JagFixString data;
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
			gmdarr->init( _cfg->getIntValue("GROUPBY_SORT_SIZE_MB", 1024), gbvhdr.c_str(), "GroupByValue" );
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
				rc = JagTable::buildDiskArrayForGroupBy( NULL, maps, attrs, &req, buffers, parseParam, gmdarr, gbvbuf );
				if ( 0 == rc ) break;
			} else { // no group by
				if ( hasagg ) { // has aggregate functions
					JagTable::aggregateDataFormat( NULL, maps, attrs, &req, buffers, parseParam, !hasFirst );
				} else { // non aggregate functions
					JagTable::nonAggregateFinalbuf( NULL, maps, attrs, &req, buffers, parseParam, finalbuf, finalsendlen, jda2, 
						jdapath2, cnt, true, NULL, false );
					if ( parseParam->hasLimit && cnt > parseParam->limit ) break;
				}
			}
			hasFirst = 1;
		}

		if ( gmdarr ) {
			JagTable::groupByFinalCalculation( gbvbuf, true, finalsendlen, cnt, parseParam->limit, 
												jdapath2, parseParam, jda2, gmdarr, NULL );
		} else if ( hasagg ) {
			JagTable::aggregateFinalbuf( &req, newhdr, 1, pparam, finalbuf, finalsendlen, jda2, jdapath2, cnt, true, NULL );
		}

		jda2->flushwrite();
		prt(("s2862635 jda2->flushwrite() done\n"));
		// after finish format group by and/or calculations, replace jda pointer to jda2
		if ( jda ) delete jda;
		jda = jda2;

		if ( finalbuf ) free( finalbuf );
		if ( gbvbuf ) free( gbvbuf );
		if ( gmdarr ) free( gmdarr );		
	} // end if !timeout && ( hasgroup || hascolumns )

	if ( timeout ) {
		sendMessage( req, "E8012 Join command has timed out. Results have been truncated;", "ER" );
	}
	// send data to client if not 0
	if ( jda->elements() > 0 && !req.session->sessionBroken && jda ) {
		jda->sendDataToClient( jda->elements(), req );
	}
	hcli.close();

	sendMessage( req, "JOINEND", "OK" );
	// with sqlhdr
	recvMessage( req.session->sock, hdr, newbuf );
	
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
		_joinMap->removeKey( jmkey );
		if ( ptab[i] ) {
			_objectLock->readUnlockTable( parseParam->opcode, parseParam->objectVec[i].dbName, 
											parseParam->objectVec[i].tableName, req.session->replicateType, 0 );
		} else if ( pindex[i] ) {
			_objectLock->readUnlockIndex( parseParam->opcode, dbidx[i], tabName[i], idxName[i], req.session->replicateType, 0 );				
		}
	}

	JagFileMgr::rmdir( jpath, true );
	return 1;
}

// method to sort by columns if needed for each table
void *JagDBServer::doMergeJoinStatic( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	Jstr fpath; 
	jagint i;
	
	if ( pass->ptab ) {
		fpath = pass->jpath + "/" + pass->ptab->getdbName() + "_" + pass->ptab->getTableName();
	} else {
		fpath = pass->jpath + "/" + pass->pindex->getdbName() + "_" + pass->pindex->getIndexName();	
	}
	
	JagFileMgr::makedirPath( fpath );
	// need to check if is sort needed
	if ( ! pass->dfSorted ) {
		pass->dfSorted = 0;
	} else {
		// first, use numCPUs to get num threads to sort concurrently
		jagint len = pass->df->_darrlist.size();
		if ( len < 1 ) len = 1;
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
			Jstr sigpath = pass->jpath + "/fprep/" + intToStr( pass->tabnum );
			JagFileMgr::makedirPath( sigpath );
			pass->dfSorted = 0;
			return NULL;
		}
		
		// after get each file, make new darrfamily to use later
		jagint aoffset = 0;
		Jstr nhdr;
		if ( pass->ptab ) {
			fpath += Jstr("/") + pass->ptab->getTableName();
		} else {
			fpath += Jstr("/") + pass->pindex->getTableName() + "." + pass->pindex->getIndexName();
		}

		// nhdr = Jstr("NN|") + longToStr( pass->sklen ) + "|" + longToStr( pass->kvlen ) + "|{";
		nhdr = fpath + Jstr(":NN|") + longToStr( pass->sklen ) + "|" + longToStr( pass->kvlen ) + "|{";
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
		pass->df = new JagDiskArrayFamily( pass->req.session->servobj, fpath, pass->rec, 0, false );
	} // end else pass->dfSorted == true 
	
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
	// pass->req.session->servobj->_joinMap->size(), jmkey.c_str(), pass->sklen, pass->kvlen, pass->numKeys, 
	// pass->minbuf, pass->maxbuf, pass->df, pass->attrs, pass->parseParam));
	Jstr sigpath = pass->jpath + "/fprep/" + intToStr( pass->tabnum );
	JagFileMgr::makedirPath( sigpath );
	prt(("s33462 makedirPath sigpath=%s\n", sigpath.s() ));
	
	
	/*** for even and odd: e.g. ( both starts from 0 )
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
	
	// after preparing data, parallelly send data to each other server for join
	// for each table, get how many servers to be sent for data
	JagStrSplit srv( pass->req.session->servobj->_dbConnector->_nodeMgr->_allNodes, '|' );
	JagVector<Jstr> svec[2];
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
	
	// if only onnserver, snum is 0
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

// method to parallel mergesort table for join
void *JagDBServer::joinSortStatic2( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	int rc, tmode = 0, tlength = 0;
	bool needInit = 1;
	Jstr ttype = " ";
	int dlen = pass->df->_darrlist.size();
	if ( dlen < 1 ) dlen = 1;
	jagint i, j, aoffset;

	jagint deach = pass->df->_darrlist.size()/pass->numCPUs+1;
	//jagint i, j, aoffset, deach = dlen/pass->numCPUs+1;
	jagint dstart = deach * pass->tabnum;
	jagint dend = dstart+deach;
	//jagint index = 0, slimit, rlimit, callCounts = -1, lastBytes = 0, tmpcnt;
	jagint callCounts = -1, lastBytes = 0, tmpcnt;
	jagint mlimit = availableMemory( callCounts, lastBytes )/8/ dlen/1024/1024;
	jagint pairmapMemLimit = pass->req.session->servobj->_cfg->getLongValue("JOIN_BUFFER_SIZE_MB", 100)*1024*1024;
	if ( mlimit <= 0 ) mlimit = 1;
	//if ( dend > pass->df->_darrlist.size() ) dend = pass->df->_darrlist.size();
	if ( dend > dlen ) dend = dlen;

	Jstr fpath, tpath; 
	JagFixString sres, value;
	ExprElementNode *root;
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
	
	while ( 1 ) {
		i = 0;
		tmpcnt = 0;

		// new method
		JagMergeReader *ntr = NULL;
		pass->df->setFamilyRead( ntr, pass->minbuf, pass->maxbuf );

		fpath = pass->jpath + "/" + longToStr( THREADID ) + "_" + longToStr( tmpcnt );
		tpath = fpath;
		jda.setwrite( fpath, fpath, 3 );
		
		while ( ntr && ntr->getNext( buf ) ) {
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
					// flush pairmap to disk
					JagFixMapIterator it;
					for ( it = pairmap._map->begin(); it != pairmap._map->end(); ++it ) {
						memcpy( tbuf, it->first.c_str(), pass->sklen );
						//memcpy( tbuf+pass->sklen, it->second.first.c_str(), pass->kvlen );
						memcpy( tbuf+pass->sklen, it->second.c_str(), pass->kvlen );
						//jda.writeit( fpath, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
						jda.writeit( 0, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
					}
					jda.flushwrite();
					++tmpcnt;
					fpath = pass->jpath + "/" + longToStr( THREADID ) + "_" + longToStr( tmpcnt );
					tpath += Jstr("|") + fpath;
					jda.setwrite( fpath, fpath, 3 );
					pairmap.clear();
				}
			}
		} // end of ntr getNext()

		if ( ntr ) delete ntr;

		if ( 1 == pass->timeout ) break;
		if ( pairmap.elements() > 0 ) {
			// flush to disk
			JagFixMapIterator it;
			for ( it = pairmap._map->begin(); it != pairmap._map->end(); ++it ) {
				memcpy( tbuf, it->first.c_str(), pass->sklen );
				memcpy( tbuf+pass->sklen, it->second.c_str(), pass->kvlen );
				jda.writeit( 0, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
			}
			jda.flushwrite();
			pairmap.clear();
		}

		// after fliter all data in one jdb file, merge tmp files to a single big file
		JagStrSplit sp( tpath.c_str(), '|' );
		int fd[sp.length()];
		//JagCompFile* fd[sp.length()];
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
			jda.writeit( 0, tbuf, pass->sklen+pass->kvlen, NULL, false, mlimit );
		}
		jda.flushwrite();
		
		for ( j = 0; j < sp.length(); ++j ) {
			jagclose( fd[j] );
			jagunlink( sp[j].c_str() );
		}

		if ( 1 == pass->timeout ) break;

		break;

	} //end for each darr

	free( buf );
	free( sbuf );
	free( tbuf );
}


// method to join main(bigger) table with hashmap data (second smaller table)
void *JagDBServer::joinHashMapData( void *ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr; // second table
	// for each of the family file, move one by one and check each hash maps for join part	
	JagDiskArrayFamily *dfam = NULL;
    //JagDiskArrayServer *darr;
	// second table
    if ( pass->ptab ) {
		dfam = pass->ptab->_darrFamily;
	} else {
		dfam = pass->pindex->_darrFamily;
	}

    JagDBPair retpair;
    JagFixString key, value;
	jagint i, aoffset = 0, alen = 0;

	ExprElementNode *root;
	JagFixString sres;
	Jstr jpath = pass->jpath;
	Jstr 	jdapath = jpath + "/alldata";
	int 	rc, tmode = 0, tlength = 0;
	bool 	needInit = 1;
	const char *buffers[2], *buffers2[2];
	Jstr 	ttype = " ";
    char 	jbuf[pass->jlen+1];
	char 	*buf = (char*)jagmalloc(pass->kvlen+1);
	char 	*tbuf = (char*)jagmalloc(pass->totlen+1);

    memset(buf, 0, pass->kvlen+1);
	memset(jbuf, 0, pass->jlen+1);
	memset(tbuf, 0, pass->totlen+1);

	key.point( jbuf, pass->jlen );
	int vnumKeys[2]; 
	const JagSchemaAttribute *vattrs[2];

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

	JagMergeReader *ntr = NULL;
	dfam->setFamilyRead( ntr, pass->minbuf, pass->maxbuf );

    while ( ntr->getNext( buf ) ) {
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
			// then, go through each of the hashmap to see if current line of data exist or not, and insert to jda if exists
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
						MultiDbNaturalFormatExchange( (char**)buffers2, 2, vnumKeys, vattrs ); // db format -> natural format
						// at last, need to make sure data is valid with where before store it
						if ( pass->parseParam->hasWhere ) {
							root = pass->parseParam->whereVec[0].tree->getRoot();
							rc = root->checkFuncValid( NULL, &pass->maps, &pass->attrs, buffers2, sres, tmode, ttype, 
													   tlength, needInit, 0, 0 );
						} else {
							rc = 1;
						}

						if ( rc ) {
							pass->jda->writeit( 0, tbuf, pass->totlen, NULL, true );
						}
						aoffset += pass->kvlen2;
					}
				}
			}
		}
	}

	if ( ntr ) delete ntr;

    free( buf );
    free( tbuf );
}

// for each table, begin requesting data from other servers to join
// there are two ways to request; one is store data directly to hashmap ( for hash join )
// and the other is request with join together
void *JagDBServer::joinRequestStatic( void * ptr )
{
	ParallelJoinPass *pass = (ParallelJoinPass*)ptr;
	int useHash = 0;
	if ( pass->hmaps ) useHash = 1;
	
	if ( useHash ) {
		prt(("s333776 hashJoinStatic hashJoinStaticGetMap() ...\n"));
		// get data in hashmap
		hashJoinStaticGetMap( pass );
	} else {
		mergeJoinGetData( pass );
	}
}

// read one table, read another into hashmap
void JagDBServer::hashJoinStaticGetMap( ParallelJoinPass *pass ) 
{
	int useHash = 1;
	char jbuf[pass->jlen+1];
	memset( jbuf, 0, pass->jlen+1 );
	Jstr rcmd = Jstr("_serv_reqdatajoin|") + intToStr( useHash ) + "|" + intToStr( pass->tabnum2 ) + "|" + pass->jname;
	jagint i, offset, aoffset, len = 0;
	JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->hcli->_connMap, pass->uhost.c_str() );
	const char *p = NULL;

	// hash join, get data and store to hashmap
	jagint hpos = pass->pos % pass->numCPUs;
	JagFixString key, val;
	key.point( jbuf, pass->jlen );

	// first, check if request host is same as self host. 
	// If yes, read current server's small table and store them to hashmap.
	// if not, send request to other servers
	if ( pass->req.session->servobj->_localInternalIP == pass->uhost ) {
		JagMergeReader *ntr = NULL;
		pass->df2->setFamilyRead( ntr, pass->minbuf2, pass->maxbuf2 );

		if ( ntr ) {
			ExprElementNode *root;
			JagFixString 	sres;
			int 			rc, tmode = 0, tlength = 0;
			bool 			needInit = 1;
			char 			*buf = (char*)jagmalloc( pass->kvlen2+1 );
			const char 		*buffers[2];
			Jstr 			ttype= " ";

			memset( buf, 0, pass->kvlen2+1 );
			if ( pass->tabnum2 ) {
				buffers[0] = NULL;
				buffers[1] = buf;
			} else {
				buffers[0] = buf;
				buffers[1] = NULL;
			}

			while ( ntr->getNext( buf ) ) {
				// read, check and send	
				dbNaturalFormatExchange( buf, pass->numKeys2, pass->attrs2, 0,0, " " ); // db format -> natural format
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
				}
			}
			if ( buf ) free( buf );
			if ( ntr ) delete ntr;
		}
	} else {
		if ( jcli->queryDirect( 0, rcmd.c_str(), rcmd.size(), true, false, false ) == 0 ) {
			return;
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
				(pass->hmaps)[hpos].appendValue( key, val );
				//(pass->hmaps)[hpos].getValue( key, val2 ); 
				offset += pass->kvlen2;
			}
		}
	}
}

// merge two tables
void JagDBServer::mergeJoinGetData( ParallelJoinPass *pass )
{
	JaguarCPPClient* jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->hcli->_connMap, pass->uhost.c_str() );
	int useHash = 0;
	Jstr rcmd = Jstr("_serv_reqdatajoin|") + intToStr( useHash ) + "|" + intToStr( pass->tabnum2 ) + "|" + pass->jname;
	jagint offset, len = 0;
	const char *p = NULL;

	if ( jcli->queryDirect( 0, rcmd.c_str(), rcmd.size(), true, false, false ) == 0 ) {
		return;
	}
	// regular join, get data and join to get result together ( store in jda )
	ExprElementNode *root;
	JagMergeReader *jntr = NULL;
	if ( pass->dfSorted ) {
		pass->df->setFamilyRead( jntr );
	} else {
		pass->df->setFamilyRead( jntr, pass->minbuf, pass->maxbuf );
	}

	if ( jntr ) {
		Jstr jdapath = pass->jpath + "/alldata"; 
		JagFixString sres;
		int 		wrc, rc, tmode = 0, tlength = 0, vnumKeys[2];
		bool 		needInit = 1;
		const char *buffers[2], *buffers2[2];
		char 		cmpbuf[pass->jlen+1];
		Jstr 		ttype = " ";
		const 		JagSchemaAttribute *vattrs[2];
		int 		goNext[2] = {1, 1};

		char 	*jbuf = (char*)jagmalloc( pass->kvlen+pass->sklen+1 );
		char 	*buf = (char*)jagmalloc( pass->totlen+1 );	
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
			// consider _insertBufferMap
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
					jntr->moveToRestartPos();
					goNext[1] = 1;
				} else if ( cmpbuf[0] != '\0' && memcmp( cmpbuf, p+offset, pass->jlen ) != 0 ) {
					// next data joinlen of buf is different from previous one, clean flag
					cmpbuf[0] = '\0';
					//jntr->setClearRestartPosFlag();
					jntr->unsetMark();
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
						jntr->moveToRestartPos();
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
			} // end if ( goNext[1] > 0 )
			
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
				if ( ! jntr->isMarked() ) {
					jntr->setRestartPos();
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
			
			MultiDbNaturalFormatExchange( (char**)buffers2, 2, vnumKeys, vattrs ); // db format -> natural format
			// at last, need to make sure data is valid with where before store it
			if ( pass->parseParam->hasWhere ) {
				root = pass->parseParam->whereVec[0].tree->getRoot();
				rc = root->checkFuncValid( jntr, &pass->maps, &pass->attrs, buffers2, sres, tmode, ttype, tlength, needInit, 0, 0 );
			} else {
				rc = 1;
			}
			//if ( wrc && rc ) pass->jda->writeit( jdapath, buf, pass->totlen, NULL, true );
			if ( wrc && rc ) pass->jda->writeit( 0, buf, pass->totlen, NULL, true );
		}

		if ( buf ) free( buf );
		if ( jbuf ) free( jbuf );
		if ( jntr ) delete jntr;
		if ( goNext[0] >= 0 ) {
			while ( jcli->reply() > 0 ) {} //
		}
	} else {
	}
}


// for each join request, begin processing and send requested data block batch
// pmesg: "_serv_reqdatajoin|useHash|reqtabnum|jname"
void JagDBServer::joinRequestSend( const char *mesg, const JagRequest &req )
{
	JagStrSplitWithQuote sp( mesg, '|' );
	int useHash = atoi( sp[1].c_str() ), rtabnum = atoi( sp[2].c_str() );
	Jstr jpath = _cfg->getTEMPJoinHOME() + "/" + sp[3], cpath = jpath + "/fprep";
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
	
	jagint sklen = pair.key.key.value(), kvlen = pair.key.value.key.value(), numKeys = pair.key.value.value.value();
	const char *minbuf = pair.value.key.key.c_str(), *maxbuf = pair.value.key.value.c_str();
	JagDiskArrayFamily *df = (JagDiskArrayFamily*)pair.value.value.key.value();
	const JagSchemaAttribute *attrs = (const JagSchemaAttribute*)pair.value.value.value.key.value();
	JagParseParam *pparam = (JagParseParam*)pair.value.value.value.value.value();
	JagMergeReader *ntr = NULL;
	if ( sklen > 0 ) { // already sorted
		df->setFamilyRead( ntr );
	} else {
		df->setFamilyRead( ntr, minbuf, maxbuf );
	}

	if ( ntr ) {
		// malloc a large block of memory to store batch block
		ExprElementNode *root;
		JagFixString sres;
		int rc, tmode = 0, tlength = 0;
		jagint boffset = 0, totlen = sklen+kvlen;
		jagint totbytes = _cfg->getLongValue("JOIN_BATCH_LINE", 100000)*totlen;
		bool needInit = 1;
		char *bbuf = (char*)jagmalloc( totbytes+1 ), *buf = (char*)jagmalloc( totlen+1 );
		const char *buffers[2];
		Jstr ttype = " ";
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
				rc = sendMessageLength( req, bbuf, boffset, "XX" );
				boffset = 0;
				if ( rc < 0 ) { break; }
			}
		}
		if ( boffset > 0 ) {
			// flush last time
			bbuf[boffset] = '\0';
			rc = sendMessageLength( req, bbuf, boffset, "XX" );
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
	jagint totm, freem, used; //GB
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

int JagDBServer::synchToOtherDataCenters( const char *mesg, bool &sucsync, const JagRequest &req )
{
	jagint rc = 0, cnt = 0, onedc = 0;
	sucsync = true;
	const char *pp, *qq;
	Jstr host;
    if ( 0 == strncasecmp( mesg, "insertsyncdconly|", 17 ) ) {
		pp = mesg+17;
		qq = strchr( pp, '|' );
		host = Jstr( pp, qq-pp );
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
			continue;
		}
		if ( req.session->dcfrom == JAG_DATACENTER_HOST && _dataCenter[i]->datcDestType() == JAG_DATACENTER_HOST ) {
			// from data server, sync to gate, otherwise ignore
			continue;
		}
		if ( req.session->dcfrom == JAG_DATACENTER_GATE && _dataCenter[i]->datcDestType() == JAG_DATACENTER_GATE ) {
			// from gate, sync to data server, otherwise ignore
			continue;
		}
		if ( onedc && host != _dataCenter[i]->getHost() ) {
			// if send to one dc, and requested dc host is not the same as _dataCenter host, ignore
			continue;
		}
		useindex.append( i );
	}
	
	if ( 0 == strncasecmp( pp, "finsert", 7 ) && _isGate ) {
		if ( req.session->replicateType != 0 ) {
			// if not main server ( not replicate server ), do not sync
			return 0;
		}
		++pp;
		// single insert, need to send file piece to other datacenter if needed
		// first, send cmd to all datacenter, but not reply
		for ( int i = 0; i < useindex.size(); ++i ) {
			_dataCenter[useindex[i]]->setDataCenterSync();
			rc = _dataCenter[useindex[i]]->query( pp );
			if ( 0 == rc ) {
				sucsync = false;
				continue;
			}
		}

		jagint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT; ssize_t rlen = 0;
		char *newbuf = NULL, *nbuf = NULL; 
		int hdrsz = JAG_SOCK_TOTAL_HDR_LEN;
		char hdr[hdrsz+1];
		while ( 1 ) {
			// recv one peice from client, send to another server, then waiting for server's reply, send back to client;
			// only repeat recv peices from client and send one by one to another server during file transfer
			rlen = recvMessage( req.session->sock, hdr, newbuf );
			if ( rlen < 0 ) {
				break;
			} else if ( rlen == 0 || ( hdr[hdrsz-3] == 'H' && hdr[hdrsz-2] == 'B' ) ) {
				// invalid info for file header, ignore
				continue;
			} else if ( 0 == strncmp( newbuf, "_onefile|", 9 ) ) {
				// if _onefile recved, parse and check the outpath is valid to recv
				JagStrSplit sp( newbuf, '|', true );
				if ( sp.length() < 4 ) {
					fsize = -1;
				} else {
					fsize = jagatoll(sp[2].c_str());
				}
				if ( nbuf ) { free( nbuf ); nbuf = NULL; }
				nbuf = (char*)jagmalloc( rlen+1 );
				memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
				for ( int i = 0; i < useindex.size(); ++i ) {
					rc = _dataCenter[useindex[i]]->sendDirectToSockAll( nbuf, rlen );
				}

				// else, while to recv pieces
				totlen = 0;
				char *mbuf =(char*)jagmalloc(memsize);
				while( 1 ) {
					if ( totlen >= fsize ) break;
					if ( fsize-totlen < memsize ) {
						recvlen = fsize-totlen;
					} else {
						recvlen = memsize;
					}
					rlen = recvRawData( req.session->sock, mbuf, recvlen );
					if ( rlen >= recvlen ) {
						for ( int i = 0; i < useindex.size(); ++i ) {
							rlen = _dataCenter[useindex[i]]->sendDirectToSockAll( mbuf, recvlen, true );
							// nohdr is true, send raw data without sqlhdr
						}		
					} else {
						break;
					}
					totlen += rlen;
				}
				if ( mbuf ) { free( mbuf ); mbuf = NULL; }
			} else {
				if ( 0 == strncmp( newbuf, "_BEGINFILEUPLOAD_", 17 ) ) { rc = 10; }
				else if ( 0 == strncmp( newbuf, "_ENDFILEUPLOAD_", 15 ) ) { rc = 20; }
				else { rc = 30; }

				nbuf = (char*)jagmalloc( rlen+1 );
				memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
				for ( int i = 0; i < useindex.size(); ++i ) {
					rlen = _dataCenter[useindex[i]]->sendRawDirectToSockAll( nbuf, rlen );
				}
				if ( nbuf ) { free( nbuf ); nbuf = NULL; }

				if ( 10 != rc && 20 != rc )  {
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
			_dataCenter[useindex[i]]->replyAll();

			if ( _dataCenter[useindex[i]]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex[i]], ':');
				reconnectDataCenter( sp[0], false );
			}
			++cnt;
		}
	} else {
		// finsert not gate
		if ( 0 == strncasecmp( pp, "finsert", 7 ) ) {
			++pp;
			if ( req.session->replicateType != 0 ) {
				// if not main server ( not replicate server, do sync
				return 0;
			}
		}
		for ( int i = 0; i < useindex.size(); ++i ) {
			// q = Jstr("[N] ") + pp; 
			_dataCenter[useindex[i]]->setDataCenterSync();
			rc = _dataCenter[useindex[i]]->query( pp );
			if ( 0 == rc ) {
				sucsync = false;
				continue;
			}
			_dataCenter[useindex[i]]->replyAll();

			if ( _dataCenter[useindex[i]]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex[i]], ':');
				reconnectDataCenter( sp[0], false );
				rc = _dataCenter[useindex[i]]->query( pp );
				if ( 0 == rc ) {
					sucsync = false;
					continue;
				}
				_dataCenter[useindex[i]]->replyAll();
			}

			++ cnt;
		}
	}

	return cnt;
}

int JagDBServer::synchFromOtherDataCenters( const JagRequest &req, const char *mesg, const JagParseParam &pparam )
{
	const char *pmsg;
	jagint msglen;
	Jstr dbobj = longToStr( THREADID ) + ".tmp";
	
	jagint rc = 0, cnt = 0, useindex = -1, hasX1 = false;
	raydebug( stdout, JAG_LOG_HIGH, "s6380 synchFromOtherDataCenters begin _numDataCenter=%d mesg=[%s]\n", _numDataCenter, mesg );
	
	for ( int i = 0; i < _numDataCenter; ++i ) {
		if ( ! _dataCenter[i] ) continue;
		if ( _dataCenter[i]->datcDestType() != JAG_DATACENTER_HOST ) {
			// not request from self HOST, ignore
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
		_dataCenter[useindex]->setDataCenterSync();
		rc = _dataCenter[useindex]->query( mesg );
		if ( rc > 0 ) {
			jagint fsize = 0, totlen = 0, recvlen = 0, memsize = 128*JAG_MEGABYTEUNIT; ssize_t rlen = 0;
			int hdrsz = JAG_SOCK_TOTAL_HDR_LEN;
			char *newbuf = NULL, *nbuf = NULL;  char hdr[hdrsz+1];
			while ( 1 ) {
				// recv one peice from another HOST, send to client, then waiting for client's reply, send back to another HOST;
				// only repeat recv peices from HOST and send one by one to client during file transfer
				memset( hdr, 0, hdrsz+1);
				rlen = _dataCenter[useindex]->recvDirectFromSockAll( newbuf, hdr );
				if ( rlen < 0 ) {
					break;
				} if ( rlen == 0 || ( hdr[hdrsz-3] == 'H' && hdr[hdrsz-2] == 'B' ) ) {
					// invalid info for file header, ignore
					continue;
				} else if ( hdr[hdrsz-3] == 'X' && hdr[hdrsz-2] == '1' ) {
					// "X1" message, send to client and continue to recv another buf
					nbuf = (char*)jagmalloc( hdrsz+rlen+1 );
					memcpy( nbuf, hdr, hdrsz );
					memcpy( nbuf+hdrsz, newbuf, rlen ); nbuf[rlen] = '\0';
					rc = sendDirectToSock( req.session->sock, nbuf, hdrsz+rlen, true ); 
					hasX1 = true;
					if ( nbuf ) { free( nbuf ); nbuf = NULL; }
					continue;
				} else if ( 0 == strncmp( newbuf, "_onefile|", 9 ) ) {
					// if _onefile recved, parse and check the outpath is valid to recv
					JagStrSplit sp( newbuf, '|', true );
					if ( sp.length() < 4 ) {
						fsize = -1;
					} else {
						fsize = jagatoll(sp[2].c_str());
					}
					nbuf = (char*)jagmalloc( rlen+1 );
					memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';

					rc = sendDirectToSock( req.session->sock, nbuf, rlen ); 
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
						rlen = _dataCenter[useindex]->recvRawDirectFromSockAll( mbuf, recvlen );
						if ( rlen >= recvlen ) {
							rlen = sendDirectToSock( req.session->sock, mbuf, recvlen, true ); 
						} else {
							break;
						}
						totlen += rlen;
					}
					if ( mbuf ) { free( mbuf ); mbuf = NULL; }
				} else {
					if ( 0 == strncmp( newbuf, "_BEGINFILEUPLOAD_", 17 ) ) { rc = 10; }
					else if ( 0 == strncmp( newbuf, "_ENDFILEUPLOAD_", 15 ) ) { rc = 20; }
					else { rc = 30; }
					nbuf = (char*)jagmalloc( rlen+1 );
					memcpy( nbuf, newbuf, rlen ); nbuf[rlen] = '\0';
					rlen = sendDirectToSock( req.session->sock, nbuf, rlen ); 
					if ( nbuf ) { free( nbuf ); nbuf = NULL; }
					if ( 10 != rc && 20 != rc ) {
					}

					if ( 20 == rc ) {
						// end, break while
						// prt(("s105233 got _ENDFILEUPLOAD_ break\n" ));
						break;
					}
				}
			}

			if ( newbuf ) { free( newbuf ); newbuf = NULL; }
			if ( nbuf ) { free( nbuf ); nbuf = NULL; }

			// prt(("s73383 _dataCenter replyAll...\n"));
			_dataCenter[useindex]->replyAll();
			// prt(("s73383 _dataCenter replyAll done\n"));

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
		if ( rc > 0 ) {
			while ( _dataCenter[useindex]->reply() ) {
				// must be X1, or select count(*) result
				pmsg = _dataCenter[useindex]->getMessage();
				msglen = _dataCenter[useindex]->getMessageLen();
				if ( pparam.opcode == JAG_COUNT_OP ) {
					pmsg = strcasestrskipquote( pmsg, " contains " );
					Jstr cntstr = longToStr(jagatoll(pmsg+10));
					sendMessageLength( req, cntstr.c_str(), cntstr.size(), "OK" );
				} else {
					if ( !hasX1 && pmsg ) {
						sendMessageLength( req, pmsg, msglen, "X1" );
						hasX1 = true;
					}
				}
			}

			if ( _dataCenter[useindex]->allSocketsBad() ) {
				JagStrSplit sp( _centerHostPort[useindex], ':');
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
							sendMessageLength( req, pmsg, msglen, "X1" );
							hasX1 = true;
							// prt(("s5028 hasX1=true pmsg=[%s]\n", pmsg ));
						}
					}
				}
			}
		}
		++cnt;
	}

	if ( hasX1 ) cnt = -111;
	return cnt;
}

void JagDBServer::openDataCenterConnection()
{
	Jstr fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
	    _numDataCenter = 0;
		return;
	}
	int trynum;
	int  MAXTRY = 12;

	Jstr adminpass = "dummy";
	JagVector<Jstr> vec;
	JagVector<Jstr> vecip;
	Jstr ip;
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );

		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.length() > 0 ) {
			vecip.append( ip );
		}
	}

	Jstr uphost, line, line2, seeds, host, port, destType;
	JagFileMgr::readTextFile( fpath, seeds );
	JagStrSplit sp( seeds, '\n', true );
	int rc = 0;
	int stype, dtype;
	JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	_numDataCenter = 0;
	for ( int i = 0; i < JAG_DATACENTER_MAX; ++i ) {
		_dataCenter[i] = NULL;
	}

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
		_dataCenter[_numDataCenter] = newObject<JaguarAPI>();
		if ( JAG_LOG_LEVEL == JAG_LOG_HIGH ) {
			prt(("s4807 datac setDebug(true)\n" ));
			_dataCenter[_numDataCenter]->setDebug( true );
		}
		if ( destType == "GATE" ) { dtype = JAG_DATACENTER_GATE; } 
		else if ( destType == "PGATE" ) { dtype = JAG_DATACENTER_PGATE; }
		else { dtype = JAG_DATACENTER_HOST; }
		if ( _isGate ) { stype = JAG_DATACENTER_GATE; } else { stype = JAG_DATACENTER_HOST; }
		
		_dataCenter[_numDataCenter]->setDatcType( stype, dtype );
		Jstr unixSocket = Jstr("/DATACENTER=1") + Jstr("/TOKEN=") + _servToken;
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
				jagsleep(10, JAG_SEC );
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

long JagDBServer::getDataCenterQueryCount( const Jstr &ip, bool doLock )
{
	if ( ip.size() < 1 ) return 0;

	if ( doLock ) {
		JAG_BLURT jaguar_mutex_lock ( &g_datacentermutex ); JAG_OVER;
	}
	long cnt = 0;
	Jstr host, port, destType;
	for ( int i = 0; i < _numDataCenter; ++i ) {
		getDestHostPortType( _centerHostPort[i], host, port, destType );
		if ( ip != host ) {
			continue; 
		}

		if ( ! _dataCenter[i] ) {
			break;
		}

		cnt = _dataCenter[i]->getQueryCount();
		break;
	}

	if ( doLock ) {
		JAG_BLURT jaguar_mutex_unlock ( &g_datacentermutex ); JAG_OVER;
	}

	return cnt;
}

// reconnect to one data center ip : is one of peerip from peer datacenter
// is from datacenter.conf one line host
void JagDBServer::reconnectDataCenter( const Jstr &ip, bool doLock )
{
	int rc = 0;
	int stype, dtype, trynum;
	int  MAXTRY = 12;
	Jstr host, port, destType;
	Jstr adminpass = "dummy";
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
		Jstr unixSocket = Jstr("/DATACENTER=1") + Jstr("/TOKEN=") + _servToken;
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
				jagsleep(10, JAG_SEC );
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
	Jstr fpath = jaguarHome() + "/conf/datacenter.conf";
	if ( ! JagFileMgr::exist( fpath ) ) {
		return 0;
	}

	JagVector<Jstr> vec;
	JagVector<Jstr> vecip;
	Jstr ip;
	// prt(("s2039 countOtherDataCenters _allNodes=[%s]\n", _dbConnector->_nodeMgr->_allNodes.c_str() ));
	JagStrSplit ipsp( _dbConnector->_nodeMgr->_allNodes, '|' );
	for ( int i=0; i < ipsp.length(); ++i ) {
		vec.append( makeUpperString( ipsp[i] ) );

		ip = JagNet::getIPFromHostName( ipsp[i] );
		if ( ip.size() > 4 ) {
			vecip.append( ip ); 
		}
	}

	Jstr seeds, hostport, upphost;
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

void JagDBServer::refreshDataCenterConnections( jagint seq )
{
	int rc;
	int newCnt = countOtherDataCenters();
	Jstr h, p, destType;
	Jstr adminpass = "dummy";
	if ( newCnt == _numDataCenter ) {
		if ( ( seq % 5 ) == 0 ) {
			for ( int i = 0; i < _numDataCenter; ++i ) {
				if ( _dataCenter[i] ) continue;
				_dataCenter[i] = newObject<JaguarAPI>();
				if ( JAG_LOG_LEVEL == JAG_LOG_HIGH ) {
					prt(("s2066 datc setDebug(true) \n" ));
				 	_dataCenter[i]->setDebug( true );
				 }
				getDestHostPortType( _centerHostPort[i], h, p, destType );
				Jstr unixSocket = Jstr("/DATACENTER=1") + Jstr("/TOKEN=") + _servToken;
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
			  (jagint)_activeClients, (jagint)_activeThreadGroups );
    #endif

	jagint percent = 70;
	if ( jagint(_activeClients) * 100 >= percent * (jagint)_activeThreadGroups ) {
		raydebug( stdout, JAG_LOG_LOW, "s3380 create new threads activeClients=%l activeGrps=%l makeThreadGroups(%l) ...\n",
				(jagint) _activeClients, (jagint)_activeThreadGroups, _threadGroupSeq );
		makeThreadGroups( _threadGroups, _threadGroupSeq++ );
	}
}

// line: "ip:port:HOST|ip:host:GATE|ip:port|ip"
// return host, port, type parts
void JagDBServer::getDestHostPortType( const Jstr &inLine, 
			Jstr& host, Jstr& port, Jstr& destType )
{
	Jstr line = trimTailChar( inLine, '\r' );
	JagStrSplit sp(line, '|', true );
	int len = sp.length();
	int i = rand() % len;
	Jstr seg = sp[i];
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

Jstr JagDBServer::srcDestType( int isGate, const Jstr &destType )
{
	Jstr conn = "/";
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

void JagDBServer::refreshUserDB( jagint seq )
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

void JagDBServer::refreshUserRole( jagint seq )
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
void JagDBServer::grantPerm( JagRequest &req, const JagParseParam &parseParam, jagint threadQueryTime )
{
	// prt(("s3722 grantPerm obj=[%s] ...\n", parseParam.grantObj.c_str() ));
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr uid  = parseParam.grantUser;
	JagStrSplit sp( parseParam.grantObj, '.' );
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = _userRole;
	else if ( req.session->replicateType == 1 ) uidrole = _prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = _nextuserRole;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		 prt(("s5002 grantPerm return\n"));
	    return;
	}

	// prt(("s4812 schemaChangeCommandSyncCheck done \n" ));
	_objectLock->writeLockSchema( req.session->replicateType );
	if ( sp.length() == 3 ) {
		cond[0] = 'O'; cond[1] = 'K';
		if ( sp[0] != "*" ) {
			// db.tab.col  check DB
			if ( ! dbExist( sp[0], req.session->replicateType ) ) {
				cond[0] = 'N'; cond[1] = 'G';
				// prt(("s0391 db=[%s] not found\n", sp[0].c_str() ));
			} else if (  sp[1] != "*" ) {
				if ( ! objExist( sp[0], sp[1], req.session->replicateType ) ) {
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
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;

	// check if uid exists
	if ( uiddb ) {
		if ( ! uiddb->exist( uid ) ) {
			cond[0] = 'N'; cond[1] = 'G';
			// prt(("s0394 user not found\n" ));
		} 
	} else {
		cond[0] = 'N'; cond[1] = 'G';
	}


	if ( 0 == req.session->drecoverConn ) {
		// prt(("s2220 sending cond=[%s] SS ..\n", cond ));
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			prt(("s3928 return\n"));
			return;
		}

		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			// prt(("s6834 return\n"));
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	if ( uidrole ) {
		uidrole->addRole( uid, sp[0], sp[1], sp[2], parseParam.grantPerm, parseParam.grantWhere );
	} else {
		// prt(("s5835 no addrole\n"));
	}
	jaguar_mutex_unlock ( &g_dbschemamutex );
	_objectLock->writeUnlockSchema( req.session->replicateType );
	schemaChangeCommandSyncRemove( scdbobj ); 
}

void JagDBServer::revokePerm( JagRequest &req, const JagParseParam &parseParam, jagint threadQueryTime )
{
	char cond[3] = { 'N', 'G', '\0' };
	req.session->spCommandReject = 1;
	Jstr uid = parseParam.grantUser;
	JagStrSplit sp( parseParam.grantObj, '.' );
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = _userRole;
	else if ( req.session->replicateType == 1 ) uidrole = _prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = _nextuserRole;
	Jstr scdbobj = uid + "." + intToStr( req.session->replicateType );
	if ( !schemaChangeCommandSyncCheck( req, scdbobj, parseParam.opcode, 0 ) ) {
		prt(("s3108 revokePerm return\n" ));
		return;
	}
	_objectLock->writeLockSchema( req.session->replicateType );
	if ( sp.length() == 3 ) {
		cond[0] = 'O'; cond[1] = 'K';
	}
	
	if ( threadQueryTime > 0 && threadQueryTime < g_lastHostTime ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	JagUserID *uiddb = NULL;
	if ( req.session->replicateType == 0 ) uiddb = _userDB;
	else if ( req.session->replicateType == 1 ) uiddb = _prevuserDB;
	else if ( req.session->replicateType == 2 ) uiddb = _nextuserDB;

	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	bool rc = false;
	if ( uiddb ) {
		rc = uiddb->exist( uid );
	}
	// prt(("s3123 exist rc=%d\n", rc ));
	jaguar_mutex_unlock ( &g_dbschemamutex );
	if ( ! rc ) {
		cond[0] = 'N'; cond[1] = 'G';
	}

	if ( 0 == req.session->drecoverConn ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		}
	
		// waiting for signal; if NG, reject and return
 		char hdr[JAG_SOCK_TOTAL_HDR_LEN+1];
		char *newbuf = NULL;
		if ( recvMessage( req.session->sock, hdr, newbuf ) < 0 || *(newbuf) != 'O' || ( *(newbuf+1) != 'K' && *(newbuf+1) != 'N' ) ) {
			if ( newbuf ) free( newbuf );
			_objectLock->writeUnlockSchema( req.session->replicateType );
			schemaChangeCommandSyncRemove( scdbobj );
			return;
		} else if ( *(newbuf) == 'O' && *(newbuf+1) == 'K' ) {
			req.syncDataCenter = true;
		}
		if ( newbuf ) free( newbuf );
	}
	req.session->spCommandReject = 0;
	
	JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	rc = false;
	if ( uidrole ) {
		rc = uidrole->dropRole( uid, sp[0], sp[1], sp[2], parseParam.grantPerm );
	}
	// prt(("s3123 dropRole rc=%d\n", rc ));
	jaguar_mutex_unlock ( &g_dbschemamutex );
	_objectLock->writeUnlockSchema( req.session->replicateType );
	schemaChangeCommandSyncRemove( scdbobj );
}

// method read affect userrole or schema
void JagDBServer::showPerm( JagRequest &req, const JagParseParam &parseParam, jagint threadQueryTime )
{
	//prt(("s3722 showPerm ...\n" ));
	Jstr uid  = parseParam.grantUser;
	if ( uid.size() < 1 ) {
		uid = req.session->uid;
	}

	if ( req.session->uid != "admin" && uid != req.session->uid ) {
		Jstr err = Jstr("E5082 error show grants for user ") + uid;
		sendMessageLength( req, err.c_str(), err.length(), "OK" );
		return;
	}

	//prt(("s4423 showPerm uid=[%s]\n", uid.c_str() ));

	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = _userRole;
	else if ( req.session->replicateType == 1 ) uidrole = _prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = _nextuserRole;


	// JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	Jstr perms;
	if ( uidrole ) {
		perms = uidrole->showRole( uid );
	} else {
		perms = Jstr("E4024 error show grants for ") + uid;
	}

	// prt(("s2473 perms=[%s] send to cleint\n", perms.c_str() ));
	sendMessageLength( req, perms.c_str(), perms.length(), "DS" );
	// jaguar_mutex_unlock ( &g_dbschemamutex );
}

bool JagDBServer::checkUserCommandPermission( const JagSchemaRecord *srec, const JagRequest &req, 
	const JagParseParam &parseParam, int i, Jstr &rowFilter, Jstr &reterr )
{
	// For admin user, all true
	if ( req.session->uid == "admin" ) return true;
	JagUserRole *uidrole = NULL;
	if ( req.session->replicateType == 0 ) uidrole = _userRole;
	else if ( req.session->replicateType == 1 ) uidrole = _prevuserRole;
	else if ( req.session->replicateType == 2 ) uidrole = _nextuserRole;
	return uidrole->checkUserCommandPermission( srec, req, parseParam, i, rowFilter, reterr );
}

// object method
// if dbExist  
bool JagDBServer::dbExist( const Jstr &dbName, int replicateType )
{
	Jstr jagdatahome = _cfg->getJDBDataHOME( replicateType );
    Jstr sysdir = jagdatahome + "/" + dbName;
	if ( JagFileMgr::isDir( sysdir ) > 0 ) {
		return true;
	} else {
		return false;
	}
}

// obj: table name or index name
bool JagDBServer::objExist( const Jstr &dbname, const Jstr &objName, int replicateType )
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

Jstr JagDBServer::fillDescBuf( const JagSchema *schema, const JagColumn &column, const Jstr &dbtable, bool convertToSec, bool &doneConvert ) const
{
	char buf[512];
	Jstr type = column.type;
	int  len = column.length;
	int  sig = column.sig;
	int  srid = column.srid;
	int  metrics = column.metrics;
	Jstr dbcol = dbtable + "." + column.name.c_str();
	//prt(("s5041 fillDescBuf type=[%s]\n", type.c_str() ));

	Jstr res;
	doneConvert = false;
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
	} else if ( type == JAG_C_COL_TYPE_LONGDOUBLE ) {
		sprintf( buf, "longdouble(%d.%d)", len-2, sig );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_DATETIME ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "datetime" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_DATETIMESEC ) {
		sprintf( buf, "datetimesec" );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMPSEC ) {
		sprintf( buf, "timestampsec" );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "timestamp" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIME ) {
		sprintf( buf, "time" );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "datetimenano" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMPNANO ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "timestampnano" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_DATETIMEMILL ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "datetimemill" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMPMILL ) {
		if ( convertToSec ) {
			sprintf( buf, "datetimesec" );
			doneConvert = true;
		} else {
			sprintf( buf, "timestampmill" );
		}
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
		sprintf( buf, "timenano" );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_DATE ) {
		sprintf( buf, "date" );
		res += buf;
	} else if ( type == JAG_C_COL_TYPE_POINT ) {
		res += columnProperty("point", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_POINT3D ) {
		res += columnProperty("point3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_CIRCLE ) {
		res += columnProperty("circle", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_CIRCLE3D ) {
		res += columnProperty("circle3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_TRIANGLE ) {
		res += columnProperty("triangle", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_TRIANGLE3D ) {
		res += columnProperty("triangle3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_SPHERE ) {
		res += columnProperty("sphere", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_SQUARE ) {
		res += columnProperty("square", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_SQUARE3D ) {
		res += columnProperty("square3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_CUBE ) {
		res += columnProperty("cube", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_RECTANGLE ) {
		res += columnProperty("rectangle", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_RECTANGLE3D ) {
		res += columnProperty("rectangle3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_BOX ) {
		res += columnProperty("box", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_CYLINDER ) {
		res += columnProperty("cylinder", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_CONE ) {
		res += columnProperty("cone", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_LINE ) {
		res += columnProperty("line", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_LINE3D ) {
		res += columnProperty("line3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_LINESTRING ) {
		res += columnProperty("linestring", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_LINESTRING3D ) {
		res += columnProperty("linestring3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTIPOINT ) {
		res += columnProperty("multipoint", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		res += columnProperty("multipoint3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING ) {
		res += columnProperty("multilinestring", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		res += columnProperty("multilinestring3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_POLYGON ) {
		res += columnProperty("polygon", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_POLYGON3D ) {
		res += columnProperty("polygon3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		res += columnProperty("multipolygon", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		res += columnProperty("multipolygon3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_ELLIPSE ) {
		res += columnProperty("ellipse", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_ELLIPSE3D ) {
		res += columnProperty("ellipse3d", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_ELLIPSOID ) {
		res += columnProperty("ellipsoid", srid, metrics );
	} else if ( type == JAG_C_COL_TYPE_RANGE ) {
		if ( srid > 0 ) {
			Jstr s = JagParser::getFieldTypeString( srid );
			sprintf( buf, "range(%s)", s.c_str() );
		} else {
			sprintf( buf, "range" );
		}
		res += buf;
	} else if ( column.spare[1] == JAG_C_COL_TYPE_UUID[0] ) {
		sprintf( buf, "uuid" );
		res += buf;
	} else if ( column.spare[1] == JAG_C_COL_TYPE_FILE[0] ) {
		sprintf( buf, "file" );
		res += buf;
	} else if ( column.spare[1] == JAG_C_COL_TYPE_ENUM[0] ) {
		Jstr defvalStr;
		Jstr enumstr =  "enum(";
		schema->getAttrDefVal( dbcol, defvalStr );
		JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
		for ( int k = 0; k < sp.length()-1; ++k ) {
			if ( 0 == k ) {
				enumstr += sp[k];
			} else {
				enumstr += Jstr(",") + sp[k];
			}
		}
		if ( *(column.spare+4) == JAG_CREATE_DEFINSERTVALUE ) {
			enumstr += ")";
		} else {
			enumstr += Jstr(",") + sp[sp.length()-1] + ")";;
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

Jstr JagDBServer::columnProperty(const char *ctype, int srid, int metrics ) const
{
	char buf[64];
	/***
	if ( srid > 0 ) {
		sprintf( buf, "%s(srid:%d,metrics:%d)", ctype, srid, metrics );
	} else if ( metrics > 0 ) {
		sprintf( buf, "%s(metrics:%d)", ctype, metrics );
	} else {
		sprintf( buf, "%s", ctype );
	}
	***/
	if ( srid > 0 || metrics > 0 ) {
		sprintf( buf, "%s(srid:%d,metrics:%d)", ctype, srid, metrics );
	} else {
		sprintf( buf, "%s", ctype );
	}

	return buf;
}

int JagDBServer::processSelectConstData( const JagRequest &req, const JagParseParam *parseParam )
{
        JagRecord rec;
		Jstr asName;
    	int  typeMode;
    	Jstr type;
    	int length;
    	ExprElementNode *root; 
		int cnt = 0;
		//prt(("s1283 in processSelectConstData parseParam->selColVec.size()=%d\n", parseParam->selColVec.size() ));
		for ( int i=0; i < parseParam->selColVec.size(); ++i ) {
    	    root = parseParam->selColVec[i].tree->getRoot();
    		JagFixString str;
    		root->checkFuncValidConstantOnly( str, typeMode, type, length );
    		prt(("s7372 checkFuncValidConstantOnly str=[%s] typeMode=%d type=[%s] length=%d name=%s asname=%s\n",
    			 str.c_str(), typeMode, type.c_str(), length, 
    			 parseParam->selColVec[i].name.c_str(), parseParam->selColVec[i].asName.c_str() ));
			if ( str.size() > 0 ) {
    			asName = parseParam->selColVec[i].asName;
            	rec.addNameValue( asName.c_str(), str.c_str() );
				++cnt;
			}
		}

		if ( cnt > 0 ) {
        	int rc = sendMessageLength( req, rec.getSource(), rec.getLength(), "KV" );
			//prt(("s1128 sendMessageLength msg=[%s] rc=%d\n", rec.getSource(), rc  ));
			return rc;
		} else {
			return 0;
		}
}

void JagDBServer::processInternalCommands( int rc, const JagRequest &req, const char *pmesg )
{
	prt(("s50047 processInternalCommands pmesg=[%s]\n", pmesg ));
	if ( JAG_SCMD_NOOP == rc ) {
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_CSCHEMA == rc ) {
		sendMapInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		/***
	} else if ( JAG_SCMD_CDEFVAL == rc ) {
		servobj->sendMapInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		***/
	} else if ( JAG_SCMD_CHOST == rc ) {
		sendHostInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_CRECOVER == rc ) {
		cleanRecovery( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_CHECKDELTA == rc ) {
		checkDeltaFiles( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_BFILETRANSFER == rc ) {
		JagStrSplit sp( pmesg, '|', true );
		if ( sp.length() >= 4 ) {
			int fpos = atoi(sp[1].c_str());
			if ( fpos < 10 ) {
				recoveryFileReceiver( pmesg, req );
			} else if ( fpos >= 10 && fpos < 20 ) {
				// should not get this
				walFileReceiver( pmesg, req );
			}
		}
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_ABFILETRANSFER == rc ) {
		recoveryFileReceiver( pmesg, req );
		_addClusterFlag = 1;
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_JOINDATAREQUEST == rc ) {
		joinRequestSend( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );					
	} else if ( JAG_SCMD_OPINFO == rc ) {
		sendOpInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_COPYDATA == rc ) {
		doCopyData( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_REFRESHACL == rc ) {
		doRefreshACL( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_REQSCHEMAFROMDC == rc ) {
		sendSchemaToDataCenter( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_UNPACKSCHINFO == rc ) {
		unpackSchemaInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_ASKDATAFROMDC == rc ) {
		askDataFromDC( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_PREPAREDATAFROMDC == rc ) {
		prepareDataFromDC( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_DOLOCALBACKUP == rc ) {
		doLocalBackup( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_DOREMOTEBACKUP == rc ) {
		doRemoteBackup( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_DORESTOREREMOTE == rc ) {				
		doRestoreRemote( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONDBTAB == rc ) {
		dbtabInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONINFO == rc ) {
		sendInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONRSINFO == rc ) {
		sendResourceInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONCLUSTERINFO == rc ) {
		sendClusterOpInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONHOSTS == rc ) {
		sendHostsInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONBACKUPHOSTS == rc ) {
		sendRemoteHostsInfo( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONLOCALSTAT == rc ) {
		sendLocalStat6( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_MONCLUSTERSTAT == rc ) {
		sendClusterStat6( pmesg, req );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	} else if ( JAG_SCMD_EXPROCLOCALBACKUP == rc ) {
		// no _END_ ED sent, already sent in method
		processLocalBackup( pmesg, req );
	} else if ( JAG_SCMD_EXPROCREMOTEBACKUP == rc ) {
		// no _END_ ED sent, already sent in method
		processRemoteBackup( pmesg, req );
	} else if ( JAG_SCMD_EXRESTOREFROMREMOTE == rc ) {
		// no _END_ ED sent, already sent in method
		processRestoreRemote( pmesg, req );
	} else if ( JAG_SCMD_EXADDCLUSTER == rc ) {
		// no _END_ ED sent, already sent in method
		addCluster( pmesg, req );
		if ( !req.session->origserv && !_restartRecover ) {
			broadcastHostsToClients();
		}
	} else if ( JAG_SCMD_EXADDCLUSTER_MIGRATE == rc ) {
		addClusterMigrate( pmesg, req );
	} else if ( JAG_SCMD_EXADDCLUSTER_MIGRATE_CONTINUE == rc ) {
		addClusterMigrateContinue( pmesg, req );
	} else if ( JAG_SCMD_EXADDCLUSTER_MIGRATE_COMPLETE == rc ) {
		addClusterMigrateComplete( pmesg, req );
	} else if ( JAG_SCMD_IMPORTTABLE == rc ) {
		importTableDirect( pmesg, req );
	} else if ( JAG_SCMD_TRUNCATETABLE == rc ) {
		truncateTableDirect( pmesg, req );
	} else if ( JAG_SCMD_EXSHUTDOWN == rc ) {
		// no _END_ ED sent, already sent in method
		shutDown( pmesg, req );
	} else if ( JAG_SCMD_GETPUBKEY == rc ) {
		// no _END_ ED sent, already sent in method
		prt(("s112923 JAG_SCMD_GETPUBKEY==rc send pubkey=[%s]\n", _publicKey.c_str() ));
		sendMessage( req, _publicKey.c_str(), "DS" );
		sendMessage( req, "_END_[T=30|E=]", "ED" );
	}				

}

// return < 0 for break; else for continue
int JagDBServer::processSimpleCommand( int simplerc, JagRequest &req, char *pmesg, int &authed )
{
	prt(("s59881 processSimpleCommand pmesg=[%s]\n", pmesg ));
	int rc = 0;
	while ( 1 ) {
    	if ( ! authed && JAG_RCMD_AUTH != simplerc ) {
    		sendMessage( req, "_END_[T=20|E=Not authed before requesting query]", "ER" );
			rc = -10;
    		break;
    	}

		char *tok;
		char *saveptr;
    	if ( JAG_RCMD_HELP == simplerc ) {
    		tok = strtok_r( pmesg, " ;", &saveptr );
    		tok = strtok_r( NULL, " ;", &saveptr );
    		if ( tok ) { helpTopic( req, tok ); } 
    		else { helpPrintTopic( req ); }
    		sendMessage( req, "_END_[T=10|E=]", "ED" );				
    	} else if ( JAG_RCMD_USE == simplerc ) {
    		// no _END_ ED sent, already sent in method
    		//useDB( req, servobj, pmesg, saveptr );
    		useDB( req, pmesg );
    	} else if ( JAG_RCMD_AUTH == simplerc ) {
    		if ( doAuth( req, pmesg ) ) {
    			// serv->serv still needs the insert pool when built init index from table
    			authed = 1; 
    			struct timeval now;
    			gettimeofday( &now, NULL );
    			req.session->connectionTime = now.tv_sec * (jagint)1000000 + now.tv_usec;

				if ( !req.session->origserv && !_restartRecover ) {
					_clientSocketMap.addKeyValue( req.session->sock, 1 );
				}

    		} else {
    			req.session->active = 0;
				rc = -20;
    			break;
    		}		
    	} else if ( JAG_RCMD_QUIT == simplerc ) {
    		noLinger( req );
    		if ( req.session->exclusiveLogin ) {
    			_exclusiveAdmins = 0;
    		}
			rc = -30;
    		break;
    	} else if ( JAG_RCMD_HELLO == simplerc ) {
			char hellobuf[128];
			char brand[32];
			sprintf(brand, "jaguar" );
			brand[0] = toupper( brand[0] );
    		sprintf( hellobuf, "%s Server %s", brand, _version.c_str() );
    		rc = sendMessage( req, hellobuf, "OK");  // SG: Server Greeting
    		sendMessage( req, "_END_[T=20|E=]", "ED" );
    		req.session->fromShell = 1;
    	}

		rc = 0;
		break;
	}

	return rc;
}


// returns -1 for end of file, 
// returns 1  for one qualified record
int JagDBServer::advanceLeftTable( JagMergeReader *jntr, const JagHashStrInt **maps, char *jbuf[], int numKeys[], jagint sklen[],
								   const JagSchemaAttribute *attrs[], JagParseParam *parseParam, const char *buffers[] )
{
	bool grc;
	ExprElementNode *root;
	int rc;
	JagFixString rstr;
	Jstr ttype = " ";
	int tlength = 0;
	bool needInit = true;
	int tmode = 0;

	while ( 1 ) {
		grc = jntr->getNext(jbuf[0] );
		if ( ! grc ) {
			// left has no more data
			prt(("s90134 left no more data\n"));
			return -1;
		}

		dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0,0, " " ); 
		if ( parseParam->hasWhere ) {
		 	root = parseParam->whereVec[0].tree->getRoot();
			buffers[1] = NULL;
			rc = root->checkFuncValid( jntr, maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
			buffers[1] = jbuf[1]+sklen[1];
		} else {
		 	rc = 1;
		}
		dbNaturalFormatExchange( jbuf[0], numKeys[0], attrs[0], 0,0, " " ); 

		if ( rc > 0 ) {
			prt(("s90122 got left qualified in advance left jbuf[0]=[%s]\n", jbuf[0] ));
			return 1;
		} else {
			prt(("901321 left not qualified, read next rec\n"));
		 	continue; // look for next qualified record
		}
	} // end while(1)
}

// returns -1 for end of file, 
// returns 1  for one qualified record
int JagDBServer::advanceRightTable( JagMergeReader *jntr, const JagHashStrInt **maps, char *jbuf[], int numKeys[], jagint sklen[], 
								   const JagSchemaAttribute *attrs[], JagParseParam *parseParam, const char *buffers[] )
{
	bool grc;
	ExprElementNode *root;
	int rc;
	JagFixString rstr;
	Jstr ttype = " ";
	int tlength = 0;
	bool needInit = true;
	int tmode = 0;

	while ( 1 ) {
		grc = jntr->getNext(jbuf[1] );
		if ( ! grc ) {
			// left has no more data
			prt(("s90138 right no more data\n"));
			return -1;
		}

		dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0,0, " " ); 
		if ( parseParam->hasWhere ) {
		 	root = parseParam->whereVec[0].tree->getRoot();
			buffers[0] = NULL;
			rc = root->checkFuncValid( jntr, maps, attrs, buffers, rstr, tmode, ttype, tlength, needInit, 0, 0 );
			buffers[0] = jbuf[1]+sklen[1];
		} else {
		 	rc = 1;
		}
		dbNaturalFormatExchange( jbuf[1], numKeys[1], attrs[1], 0,0, " " ); 

		if ( rc > 0 ) {
			prt(("s90124 got right qualified in advance right jbuf[1]=[%s]\n", jbuf[1] ));
			return 1;
		} else {
			prt(("s901321 right not qualified, read next rec\n"));
		 	continue; // look for next qualified record
		}
	} // end while(1)
}


// return < 0 : for error; 0 for OK
int JagDBServer::handleRestartRecover( const JagRequest &req, 
									   const char *pmesg, jagint len,
									   char *hdr2, char *&newbuf, char *&newbuf2 )
{
	//prt(("s550283 handleRestartRecover ...\n"));
	int rspec = 0;
	if ( req.batchReply ) {
		JagDBServer::regSplogCommand( req.session, pmesg, len, 2 );
	} else if ( 0 == strncasecmp( pmesg, "update", 6 ) || 0 == strncasecmp( pmesg, "delete", 6 ) ) {
		//prt(("s43083 update or delete regSplogComman ...\n"));
		JagDBServer::regSplogCommand( req.session, pmesg, len, 1 );
	} else {
		JagDBServer::regSplogCommand( req.session, pmesg, len, 0 );
		rspec = 1;
	}
	jaguar_mutex_unlock ( &g_flagmutex );

	int sprc = 1;
	char cond[3] = { 'O', 'K', '\0' };
	if ( 0 == req.session->drecoverConn && rspec == 1 ) {
		if ( sendMessageLength( req, cond, 2, "SS" ) < 0 ) {
			sprc = 0;
		}

		if ( sprc == 1 && recvMessage( req.session->sock, hdr2, newbuf2 ) < 0 ) {
			sprc = 0;
		}
	}

	sendMessage( req, "_END_[T=779|E=]", "ED" );
				
	// receive three bytes of signal  3byte
	if ( sprc == 1 && _faultToleranceCopy > 1 && req.session->drecoverConn == 0 ) {
		// receive status bytes
		char rephdr[4];
		rephdr[3] = '\0';
		rephdr[0] = rephdr[1] = rephdr[2] = 'N';		

		int rsmode = 0;
		// "YYY" "NNN" "YNY" etc

		if ( recvMessage( req.session->sock, hdr2, newbuf2 ) < 0 ) {
			rephdr[req.session->replicateType] = 'Y';
			rsmode = getReplicateStatusMode( rephdr, req.session->replicateType );
			if ( !req.session->spCommandReject && rsmode >0 ) deltalogCommand( rsmode, req.session, pmesg, req.batchReply );
			if ( req.session->uid == "admin" ) {
				if ( req.session->exclusiveLogin ) {
					_exclusiveAdmins = 0;
					raydebug( stdout, JAG_LOG_LOW, "Exclusive admin disconnected from %s\n", req.session->ip.c_str() );
				}
			}
			if ( newbuf ) { free( newbuf ); }
			if ( newbuf2 ) { free( newbuf2 ); }
			-- _connections;

			return -1;
		}

		rephdr[0] = *newbuf2; rephdr[1] = *(newbuf2+1); rephdr[2] = *(newbuf2+2);
		rsmode = getReplicateStatusMode( rephdr );
		if ( !req.session->spCommandReject && rsmode >0 ) {
			deltalogCommand( rsmode, req.session, pmesg, req.batchReply );
		}

		sendMessage( req, "_END_[T=7799|E=]", "ED" );
	}

	return 0;
}

// shift pmesage from beginning special header bytes
void JagDBServer::rePositionMessage( JagRequest &req, char *&pmesg, jagint &len )
{
			char *p, *q;
			int tdlen = 0;

			p = q = pmesg;
			while ( *q != ';' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				prt(("ERROR tdiff hdr for delta recover\n"));
				abort();
			}
			tdlen = q-p;
			req.session->timediff = rayatoi( p, tdlen );

			pmesg = q+1; // ;pmesg
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

int JagDBServer::broadcastSchemaToClients()
{
	Jstr schemaInfo;
	_dbConnector->_broadcastCli->getSchemaMapInfo( schemaInfo );
	//prt(("bcast map info=[%s]\n", schemaInfo.c_str()));
	if ( schemaInfo.size() < 1 ) {
		return 0;
	}

    char code4[5];
	char *buf = NULL;
    char sqlhdr[8]; makeSQLHeader( sqlhdr );
	sqlhdr[0] = 'S'; sqlhdr[1] = 'C'; sqlhdr[2] = 'H'; // "SCH" -- schema change

	jagint msglen = schemaInfo.size();
	const char *mesg = schemaInfo.c_str();
    if ( msglen >= JAG_SOCK_COMPRSS_MIN ) {
        Jstr comp;
        JagFastCompress::compress( mesg, msglen, comp );
        msglen = comp.size();
        buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+comp.size()+1+64 );
        strcpy( code4, "ZSCC" );
        putXmitHdrAndData( buf, sqlhdr, comp.c_str(), msglen, code4 );
    } else {
        buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+msglen+1+64 );
        strcpy( code4, "CSCC" );
        putXmitHdrAndData( buf, sqlhdr, mesg, msglen, code4 );
    }

	JagVector<int> vec = _clientSocketMap.getIntVector();
	int cnt = 0;
	int clientsock;
	jagint rc;
	for ( int i=0; i < vec.size(); ++i ) {
		clientsock = vec[i];
		rc = sendRawData( clientsock, buf, JAG_SOCK_TOTAL_HDR_LEN+msglen ); 
		if ( rc == JAG_SOCK_TOTAL_HDR_LEN+msglen ) {
			++ cnt;
		}
	}

	if ( buf ) free( buf );
	prt(("s39388 broadast schema to %d clients\n", cnt ));
	return cnt;
}


int JagDBServer::broadcastHostsToClients()
{
    Jstr snodes = _dbConnector->_nodeMgr->_sendAllNodes;
	if ( snodes.size() < 1 ) {
		return 0;
	}
	prt(("s39388 broadast hosts to %d clients ...\n", snodes.size() ));

    char code4[5];
	char *buf = NULL;
    char sqlhdr[8]; makeSQLHeader( sqlhdr );
	sqlhdr[0] = 'H'; sqlhdr[1] = 'O'; sqlhdr[2] = 'S'; // "HOS" -- hosts change

	jagint msglen = snodes.size();
	const char *mesg = snodes.c_str();
    if ( msglen >= JAG_SOCK_COMPRSS_MIN ) {
        Jstr comp;
        JagFastCompress::compress( mesg, msglen, comp );
        msglen = comp.size();
        buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+comp.size()+1+64 );
        strcpy( code4, "ZHLC" );
        putXmitHdrAndData( buf, sqlhdr, comp.c_str(), msglen, code4 );
    } else {
        buf = (char*)jagmalloc( JAG_SOCK_TOTAL_HDR_LEN+msglen+1+64 );
        strcpy( code4, "CHLC" );
        putXmitHdrAndData( buf, sqlhdr, mesg, msglen, code4 );
    }

	JagVector<int> vec = _clientSocketMap.getIntVector();
	int cnt = 0;
	int clientsock;
	jagint rc;
	for ( int i=0; i < vec.size(); ++i ) {
		clientsock = vec[i];
		rc = sendRawData( clientsock, buf, JAG_SOCK_TOTAL_HDR_LEN+msglen ); 
		if ( rc == JAG_SOCK_TOTAL_HDR_LEN+msglen ) {
			++ cnt;
		}
	}

	if ( buf ) free( buf );
	prt(("s39388 broadast hosts to %d clients\n", cnt ));
	return cnt;
}

void JagDBServer::makeNeededDirectories()
{
	Jstr fpath;

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

}

// method to import local cached records and insert to a table
// pmesg: "_ex_importtable|db|table|finish(YES/NO)"
void JagDBServer::importTableDirect( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" ) {
		raydebug( stdout, JAG_LOG_LOW, "importTableDirect rejected. admin login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin login is required]", "ER" );
		return;
	}

	JagStrSplit sp( mesg, '|');
	if ( sp.length() < 4 ) {
		raydebug( stdout, JAG_LOG_LOW, "importTableDirect rejected. wrong command [%s]\n", mesg );
		sendMessage( req, "_END_[T=130|E=Command Failed. importTableDirect rejected. wrong command]", "ER" );
		return;
	}

	Jstr db = sp[1];
	Jstr tab = sp[2];
	Jstr doFinishUp = sp[3]; // YES/NO

	Jstr dbtab = db + "." + tab;
	Jstr dirpath = jaguarHome() + "/export/" + dbtab;
	if ( doFinishUp == "YES" || doFinishUp == "Y" ) {
		JagFileMgr::rmdir( dirpath );
		prt(("s22029288 importTableDirect doFinishUp yes, rmdir %s\n", dirpath.s() ));
		sendMessage( req, "_END_[T=30|E=]", "ED" );
		return;
	}

	Jstr host = "localhost"; 
	Jstr objname = dbtab + ".sql";
	if ( this->_listenIP.size() > 0 ) { host = this->_listenIP; }
	Jstr unixSocket = Jstr("/TOKEN=") + this->_servToken;
	JaguarCPPClient pcli;
	// pcli.setDebug( true ); 
	while ( !pcli.connect( host.c_str(), this->_port, "admin", "dummy", "test", unixSocket.c_str(), 0 ) ) {
		raydebug( stdout, JAG_LOG_LOW, "s4022 Connect (%s:%s) (%s:%d) error [%s], retry ...\n", 
				  "admin", "dummy", host.c_str(), this->_port, pcli.error() );
		jagsleep(5, JAG_SEC);
	}

	prt(("s344877 connect to localhost got pcli._allHostsString=[%s]\n", pcli._allHostsString.s() ));

	Jstr fpath = JagFileMgr::getFileFamily( dirpath, objname );
	prt(("s220291 fpath=[%s]\n", fpath.s() ));
	int rc = pcli.importLocalFileFamily( fpath );
	pcli.close();
	if ( rc < 0 ) {
		prt(("s4418 Import file not found on server %s\n", fpath.s() ));
	}
	prt(("s1111028 importTable done rc=%d (<0 is error)\n", rc ));

	sendMessage( req, "_END_[T=30|E=]", "ED" );

	raydebug( stdout, JAG_LOG_LOW, "s300873 importTableDirect() is done\n" );
}

// method to truncate a table
// pmesg: "_ex_truncatetable|replicate_type(0/1/2)|db|table"
void JagDBServer::truncateTableDirect( const char *mesg, const JagRequest &req )
{
	if ( req.session->uid!="admin" ) {
		raydebug( stdout, JAG_LOG_LOW, "truncateTableDirect rejected. admin login is required\n" );
		sendMessage( req, "_END_[T=130|E=Command Failed. admin exclusive login is required]", "ER" );
		return;
	}

	JagStrSplit sp( mesg, '|');
	if ( sp.length() < 4 ) {
		raydebug( stdout, JAG_LOG_LOW, "truncateTableDirect rejected. wrong command [%s]\n", mesg );
		sendMessage( req, "_END_[T=130|E=Command Failed. truncateTableDirect rejected. wrong command]", "ER" );
		return;
	}

	Jstr reterr, indexNames;
	Jstr replicateTypeStr = sp[1];
	int replicateType = replicateTypeStr.toInt();
	Jstr db = sp[2];
	Jstr tab = sp[3];

	Jstr dbobj = db + "." + tab;
	prt(("s22208 truncateTableDirect dbobj=%s replicateType=%d\n", dbobj.s(), replicateType ));

	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;
	getTableIndexSchema( replicateType, tableschema, indexschema );

	ptab = this->_objectLock->writeLockTable( JAG_TRUNCATE_OP, db, tab, tableschema, replicateType, 0 );

	if ( ptab ) {
		indexNames = ptab->drop( reterr, true );
		prt(("s222029  ptab->drop() indexNames=%s\n", indexNames.s() ));
	}

	refreshSchemaInfo( replicateType, g_lastSchemaTime );
	
	if ( ptab ) {
		delete ptab; 
		// rebuild ptab, and possible related indexs

		ptab = _objectLock->writeTruncateTable( JAG_TRUNCATE_OP, db, tab, tableschema, replicateType, 0 );
		if ( ptab ) {
			JagStrSplit sp( indexNames, '|', true );
			for ( int i = 0; i < sp.length(); ++i ) {
				pindex = _objectLock->writeLockIndex( JAG_CREATEINDEX_OP, db, tab, sp[i],
														tableschema, indexschema, replicateType, 1 );
				if ( pindex ) {
					ptab->_indexlist.append( pindex->getIndexName() );
				    _objectLock->writeUnlockIndex( JAG_CREATEINDEX_OP, db, tab, sp[i], replicateType, 1 );
				}
			}
			this->_objectLock->writeUnlockTable( JAG_TRUNCATE_OP, db, tab, replicateType, 0 );
		}
		raydebug( stdout, JAG_LOG_LOW, "user [%s] truncate table [%s]\n", req.session->uid.c_str(), dbobj.c_str() );
	} else {
		prt(("s2200112 no ptab\n"));
	}

	prt(("s51128 truncateTableDirect() dbobj=%s done replicateType=%d\n", dbobj.s(), replicateType  ));

	sendMessage( req, "_END_[T=30|E=]", "ED" );

	raydebug( stdout, JAG_LOG_LOW, "s300873 truncateTableDirect() replicateType=%d is done\n", replicateType );
}

void JagDBServer::insertToTimeSeries( const JagSchemaRecord &parentSrec, const JagRequest &req, JagParseParam &parseParam, 
									  const Jstr &tser, const Jstr &dbName, const Jstr &tableName, 
									  const JagTableSchema *tableschema, int replicateType, const Jstr &oricmd )
{
	if ( parseParam.insColMap ) {
		parseParam.insColMap->print();
	} else {
	}

	JagVector<OtherAttribute> rollupOtherVec;
	for ( int i = 0; i < parentSrec.columnVector->size(); ++i ) {
		const JagColumn &jcol = (*parentSrec.columnVector)[i];
		if ( parseParam.otherVec[i].issubcol ) { 
			continue; 
		}

		if ( jcol.iskey ) {
			rollupOtherVec.append( parseParam.otherVec[i] );
			continue;
		}

		if ( jcol.isrollup ) {
			rollupOtherVec.append( parseParam.otherVec[i] );
			// 5 extra  col:sum col::min col::max  col::avg col::var
			rollupOtherVec.append( parseParam.otherVec[i] );
			rollupOtherVec.append( parseParam.otherVec[i] );
			rollupOtherVec.append( parseParam.otherVec[i] );
			rollupOtherVec.append( parseParam.otherVec[i] );
			parseParam.otherVec[i].valueData = "0";
			rollupOtherVec.append( parseParam.otherVec[i] );
		}
	}

	OtherAttribute ca;
	ca.valueData = "1";
	rollupOtherVec.append( ca );

	JagStrSplit sp( tser, ','); 
	Jstr tsTable, ts;
	int rc;
	for ( int i =0; i < sp.length(); ++ i ) {
		ts = sp[i];
		tsTable = tableName + "@" + ts;
		JagTable *rtab = _objectLock->writeLockTable( parseParam.opcode, dbName, tsTable, tableschema, replicateType, 0 ); 
		if ( rtab ) {
			Jstr errmsg;
			JagVector<JagDBPair> retpairVec;
			parseParam.objectVec[0].tableName = tsTable;
			parseParam.otherVec = rollupOtherVec;

			rc = rtab->parsePair( req.session->timediff, &parseParam, retpairVec, errmsg  ); 

			JagDBPair &insPair = retpairVec[0];
			if ( rc ) {
				rc = rtab->rollupPair( req, insPair, rollupOtherVec );
				if ( rc ) {
				} else {
					_dbLogger->logerr( req, errmsg, oricmd + "@" + ts );
				}
			} else {
				_dbLogger->logerr( req, errmsg, oricmd + "@" + ts );
			}

			_objectLock->writeUnlockTable( parseParam.opcode, dbName, tsTable, replicateType, 0 ); 

		} else {
			raydebug( stdout, JAG_LOG_LOW, "E32038 Error: timeseries table [%s] not found\n", tsTable.s() );
		}
	}
}

int JagDBServer::createTimeSeriesTables( const JagRequest &req, const Jstr &timeSeries, const Jstr &dbname, const Jstr &dbtable, 
										 const JagParseAttribute &jpa, Jstr &reterr )
{
	JagStrSplit sp( timeSeries, ':');
	int crc;
	Jstr sql;
	int  cnt = 0;
	for ( int i = 0; i < sp.length(); ++i ) {
		sql = describeTable( JAG_TABLE_TYPE, req, _tableschema, dbtable, false, true, true, sp[i] ); 
		sql.replace('\n', ' ');
		JagParser parser((void*)this);
		JagParseParam pparam2( &parser );
		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
			crc = createSimpleTable( req, dbname, &pparam2 );
			if ( ! crc ) {
				raydebug( stdout, JAG_LOG_LOW, "E20320 Error: creating timeseries table [%s]\n", sql.s() );
			} else {
				raydebug( stdout, JAG_LOG_LOW, "OK: creating timeseries rollup table [%s]\n", sql.s() );
				++ cnt;
			}
		} else {
			raydebug( stdout, JAG_LOG_LOW, "E20321 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
		}
	}

	return cnt;
}

void JagDBServer::dropTimeSeriesTables( const JagRequest &req, const Jstr &timeSeries, const Jstr &dbname, const Jstr &dbtable, 
									    const JagParseAttribute &jpa, Jstr &reterr )
{
	JagStrSplit sp( timeSeries, ',');
	int crc;
	Jstr sql, rollTab;
	for ( int i = 0; i < sp.length(); ++i ) {
		JagStrSplit ss(sp[i], '_');
		rollTab = ss[0];

		sql = Jstr("drop table ") + dbtable + "@" + rollTab; 
		JagParser parser((void*)this);
		JagParseParam pparam2( &parser );
		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
			crc = dropSimpleTable( req, &pparam2, reterr, true );
			if ( ! crc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error: drop timeseries table [%s][%s]\n", sql.s(), reterr.s() );
			} else {
				raydebug( stdout, JAG_LOG_LOW, "OK: drop timeseries rollup table [%s]\n", sql.s() );
			}
		} else {
			raydebug( stdout, JAG_LOG_LOW, "Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
		}
	}
}

void JagDBServer::createTimeSeriesIndexes( const JagParseAttribute &jpa, const JagRequest &req, 
										   const JagParseParam &parseParam, const Jstr &timeSeries, Jstr &reterr )
{
	Jstr dbname =  parseParam.objectVec[0].dbName;
	JagStrSplit sp( timeSeries, ',');
	int crc;
	Jstr sql;
	JagTable *newPtab;
	JagIndex *newPindex;
	JagRequest req2( req );

	for ( int i = 0; i < sp.length(); ++i ) {
		sql = describeIndex( false, req, _indexschema, dbname, 
							 parseParam.objectVec[1].indexName, reterr, true, true, sp[i] );
		sql.replace('\n', ' ');
		JagParser parser((void*)this);
		JagParseParam pparam2( &parser );
		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
			crc = createSimpleIndex( req, &pparam2, newPtab, newPindex, reterr );
			if ( ! crc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error: creating timeseries rollup index [%s]\n", sql.s() );
			} else {
				raydebug( stdout, JAG_LOG_LOW, "OK: creating timeseries rollup index [%s]\n", sql.s() );
			}
		} else {
			raydebug( stdout, JAG_LOG_LOW, "E21013 Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
		}
	}
}

int JagDBServer::createSimpleIndex( const JagRequest &req, JagParseParam *parseParam,
							        JagTable *&ptab, JagIndex *&pindex, Jstr &reterr )
{
	JagIndexSchema *indexschema; 
	JagTableSchema *tableschema;
	getTableIndexSchema(  req.session->replicateType, tableschema, indexschema );
	Jstr dbname = parseParam->objectVec[0].dbName;
	int replicateType = req.session->replicateType;
	int opcode = parseParam->opcode;

	Jstr dbindex = dbname + "." + parseParam->objectVec[0].tableName + 
							 "." + parseParam->objectVec[1].indexName;

	Jstr tgttab = indexschema->getTableNameScan( dbname, parseParam->objectVec[1].indexName );
	if ( tgttab.size() > 0 ) {
		reterr = "E20373 index already exists in database";
		return 0;
	}

	int rc = 0;
	Jstr dbobj = dbname + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbobj + "." + intToStr( replicateType );

	ptab = _objectLock->writeLockTable( opcode, parseParam->objectVec[0].dbName, 
										 parseParam->objectVec[0].tableName, tableschema, replicateType, 0 );
	if ( ptab ) {
		rc = createIndexSchema( req, dbname, parseParam, reterr, false );
	} else {
		reterr = Jstr("E22208 unable to find table ") + parseParam->objectVec[0].tableName ;
		return 0;
	}
	
	refreshSchemaInfo( replicateType, g_lastSchemaTime );
	pindex = _objectLock->writeLockIndex( opcode, parseParam->objectVec[1].dbName,
										   parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										   tableschema, indexschema, replicateType, 1 );
	if ( ptab && pindex ) {
		doCreateIndex( ptab, pindex );
		rc = 1;
	} else {
	    indexschema->remove( dbindex );
		rc = 0;
	}

	if ( pindex ) {
		_objectLock->writeUnlockIndex(  opcode, parseParam->objectVec[1].dbName,
										parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										replicateType, 1 );		
	}

	if ( ptab ) {
		_objectLock->writeUnlockTable( opcode, parseParam->objectVec[0].dbName, 
										parseParam->objectVec[0].tableName, replicateType, 0 );
	}

	raydebug(stdout, JAG_LOG_LOW, "user [%s] create index [%s]\n", req.session->uid.c_str(), scdbobj.c_str() );
	return rc;
}

void JagDBServer::dropTimeSeriesIndexes( const JagRequest &req, const JagParseAttribute &jpa, 
										 const Jstr &parentTableName,
									     const Jstr &parentIndexName, const Jstr &timeSeries )
{
	JagStrSplit sp( timeSeries, ',');
	int crc;
	Jstr sql, rollTab, reterr;
	for ( int i = 0; i < sp.length(); ++i ) {
		JagStrSplit ss(sp[i], '_');
		rollTab = ss[0];

		sql = Jstr("drop index ") + parentIndexName + "@" + rollTab + " on " + parentTableName + "@" + rollTab; 
		JagParser parser((void*)this);
		JagParseParam pparam2( &parser );
		if ( parser.parseCommand( jpa, sql, &pparam2, reterr ) ) {
			crc = dropSimpleIndex( req, &pparam2, reterr, true );
			if ( ! crc ) {
				raydebug( stdout, JAG_LOG_LOW, "Error: drop timeseries index [%s][%s]\n", sql.s(), reterr.s() );
			} else {
				raydebug( stdout, JAG_LOG_LOW, "OK: drop timeseries rollup index [%s]\n", sql.s() );
			}
		} else {
			raydebug( stdout, JAG_LOG_LOW, "Error: parse [%s] [%s]\n", sql.s(), reterr.s() );
		}
	}
}

int JagDBServer::dropSimpleIndex( const JagRequest &req, const JagParseParam *parseParam, Jstr &reterr, bool lockSchema )
{
	Jstr dbname = parseParam->objectVec[0].dbName;
	Jstr dbobj = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	Jstr scdbobj = dbobj + "." + intToStr( req.session->replicateType );
	JagTable *ptab = NULL;
	JagIndex *pindex = NULL;
	JagTableSchema *tableschema;
	JagIndexSchema *indexschema;

	getTableIndexSchema( req.session->replicateType, tableschema, indexschema );

	ptab = _objectLock->writeLockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
										 parseParam->objectVec[0].tableName, tableschema, req.session->replicateType, 0 );	

	pindex = _objectLock->writeLockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
										  parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
										  tableschema, indexschema, req.session->replicateType, 1 );

	if ( ! pindex ) {
		if ( ptab ) {
			_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
								   		   parseParam->objectVec[0].tableName, req.session->replicateType, 0 );
		}
		return 0;
	}
	
	if ( ! ptab ) {
		_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
								   parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
								   req.session->replicateType, 1 );
		return 0;
	}

	Jstr dbtabidx;
	pindex->drop();
	dbtabidx = dbname + "." + parseParam->objectVec[0].tableName + "." + parseParam->objectVec[1].indexName;

	if ( lockSchema ) {
		JAG_BLURT jaguar_mutex_lock ( &g_dbschemamutex ); JAG_OVER;
	}

	indexschema->remove( dbtabidx );
	ptab->dropFromIndexList( parseParam->objectVec[1].indexName );	
	refreshSchemaInfo( req.session->replicateType, g_lastSchemaTime );

	if ( lockSchema ) {
		jaguar_mutex_unlock ( &g_dbschemamutex );
	}
	
	delete pindex; 
	_objectLock->writeUnlockIndex( parseParam->opcode, parseParam->objectVec[1].dbName,
								   parseParam->objectVec[0].tableName, parseParam->objectVec[1].indexName,
								   req.session->replicateType, 1 );

	_objectLock->writeUnlockTable( parseParam->opcode, parseParam->objectVec[0].dbName, 
								   parseParam->objectVec[0].tableName, req.session->replicateType, 0 );

	raydebug( stdout, JAG_LOG_LOW, "user [%s] drop simple index [%s]\n", req.session->uid.c_str(), dbtabidx.c_str() );
	return 1;
}

void JagDBServer::trimTimeSeries()
{
	const int replicateType[] = { 0, 1, 2 };
	JagTable *ptab = NULL;
	JagTableSchema *tableschema;

	JagVector<AbaxString> *vec;
	Jstr dbname, dbtab, tableName, allDBs;
	int repType;
	bool hasTser;
	jagint  cnt = 0;
	time_t  windowlen;

	for ( int i = 0; i < 3; ++i ) {
		repType = replicateType[i];
		tableschema = this->getTableSchema( repType );
		allDBs = JagSchema::getDatabases( _cfg, repType );
		JagStrSplit sp(allDBs, '\n', true );
		for ( int j=0; j < sp.size(); ++j ) {
			dbname = sp[j];
			if ( dbname == "system" ) continue;
			vec = tableschema->getAllTablesOrIndexesLabel( JAG_TABLE_TYPE, dbname, "" );
			for ( int k = 0; k < vec->size(); ++k ) {
				dbtab = (*vec)[k].s();
				JagStrSplit ss(dbtab, '.');
				if ( ss.size() == 2 ) {
					tableName = ss[1];
				} else {
					tableName = dbtab;
				}

				ptab = _objectLock->writeLockTable( JAG_DELETE_OP, dbname, tableName, tableschema, repType, 0 );
				if ( ! ptab ) { 
					continue; 
				}

				Jstr retention = ptab->timeSeriesRentention();
				Jstr timeSer;
				hasTser =  ptab->hasTimeSeries( timeSer );
				bool needProcess = false;
				if ( hasTser ) {
					if ( retention != "0" ) {
						needProcess = true;
					}
				} else {
					if ( ptab->hasRollupColumn() ) {
						if ( retention != "0" ) {
							needProcess = true;
						}
					} else {
					}

					if ( needProcess ) {
					} else {
						_objectLock->writeUnlockTable( JAG_DELETE_OP, dbname, tableName, repType, 0 );
						continue; 
					}
				}

				windowlen = JagSchemaRecord::getRetentionSeconds( retention );
				cnt = -1;
				if ( windowlen > 0 ) {
					cnt = ptab->cleanupOldRecords( time(NULL) - windowlen );
					trimWalLogFile( ptab, dbname, tableName, ptab->_darrFamily->_insertBufferMap, ptab->_darrFamily->_keyChecker );
				}

				_objectLock->writeUnlockTable( JAG_DELETE_OP, dbname, tableName, repType, 0 );
			}
			if ( vec ) delete vec;
		}
	}
}

jagint JagDBServer::trimWalLogFile( const JagTable *ptab, const Jstr &db, const Jstr &tab, 
									const JagDBMap *insertBufferMap, const JagFamilyKeyChecker *keyChecker )
{
	if ( db.size() < 1 || tab.size() <1 ) { return 0; }

	JAG_BLURT jaguar_mutex_lock ( &g_wallogmutex ); JAG_OVER;

	Jstr fpath = _cfg->getWalLogHOME() + "/" + db + "." + tab + ".wallog";
	jagint cnt = doTrimWalLogFile( ptab, fpath, db, insertBufferMap, keyChecker );

	JAG_BLURT jaguar_mutex_unlock ( &g_wallogmutex ); 
	return cnt;
}

jagint JagDBServer::doTrimWalLogFile( const JagTable *ptab, const Jstr &fpath, const Jstr &dbname, 
								      const JagDBMap *insertBufferMap, const JagFamilyKeyChecker *keyChecker )
{
	if ( JagFileMgr::fileSize( fpath ) <= 0 ) {
		return 0;
	}

	int i, fd = jagopen( fpath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) return 0;

	Jstr newLogPath = fpath + ".trimmedXYZ";
	FILE *newLogFP = fopen( newLogPath.s(), "w" );
	if ( ! newLogFP ) {
		close(fd);
		raydebug( stdout, JAG_LOG_LOW, "E20281 Error open write [%s]\n", newLogPath.s() );
		return 0;
	}

	char buf16[17];
	jagint msglen;
	char c;
	char *msgbuf = NULL;
	int replicateType;
	int timediff;
	int batchReply;
	Jstr reterr;

	jagint cntwrite = 0;
	jagint cntdel = 0;
	while ( 1 ) {
		i = 0;
		memset( buf16, 0, 4 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 0 ) {
					jagclose( fd );
					return cntwrite;
				}
			}
			++i;
		}

		if ( buf16[0] == '\0' ) {
			break;
		}

		replicateType = atoi( buf16 );

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
					return cntwrite;
				}
			}
			++i;
		}
		if ( buf16[0] == '\0' ) {
			break;
		}
		timediff = atoi( buf16 );

		i = 0;
		memset( buf16, 0, 4 );
		while( 1==read(fd, &c, 1) ) {
			buf16[i] = c;
			if ( c == ';' ) {
				buf16[i] =  '\0';
				break;
			} else {
				if ( i > 0 ) {
					jagclose( fd );
					return cntwrite;
				}
			}
			++i;
		}

		if ( buf16[0] == '\0' ) {
			printf("s3398 end isBatch 0 is 0\n");
			break;
		}

		batchReply = atoi( buf16 );

		memset( buf16, 0, JAG_REDO_MSGLEN+1 );
		raysaferead( fd, buf16, JAG_REDO_MSGLEN );
		msglen = jagatoll( buf16 );

		msgbuf = (char*)jagmalloc(msglen+1);
		memset(msgbuf, 0, msglen+1);
		raysaferead( fd, msgbuf, msglen );

		JagParseAttribute jpa( this, timediff, servtimediff, dbname, _cfg );
		JagParser parser((void*)this);
		JagParseParam pparam( &parser );
		JagVector<JagDBPair> pairVec;
		bool exist = false;
		bool brc = parser.parseCommand( jpa, msgbuf, &pparam, reterr );
		if ( brc && pparam.opcode == JAG_INSERT_OP ) {
			int prc = ptab->parsePair( timediff, &pparam, pairVec, reterr );
			if ( prc ) {
				if ( insertBufferMap->exist( pairVec[0] ) ) {
					exist = true;
				} else {
					char kbuf[ptab->KEYLEN+1];
					memset( kbuf, 0, ptab->KEYLEN+1);
					memcpy( kbuf, pairVec[0].key.c_str(), ptab->KEYLEN );
					if ( keyChecker->exist( kbuf )  ) {
						exist = true;
					}
				}

				if ( exist ) {
					fprintf( newLogFP, "%d;%d;%d;%010ld%s", replicateType, timediff, batchReply, msglen, msgbuf );
					++cntwrite;
				} else {
					++cntdel;
				}

			} else {
			}
		} else {
		}

		free( msgbuf );
		msgbuf = NULL;
	}

	jagclose( fd );
	fclose( newLogFP  );

	jagunlink( fpath.s() );
	jagrename( newLogPath.s(), fpath.s() );

	_walLogMap.removeKey( fpath );

	_walLogMap.ensureFile( fpath );

	raydebug( stdout, JAG_LOG_LOW, "trimWalLogFile %s write=%d deleted=%d\n", fpath.s(),  cntwrite, cntdel );
	return cntwrite;
}
