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
#include <sys/time.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <malloc.h>
#include <signal.h>
#include <dirent.h>
#include <math.h>

#include <abax.h>
#include <JagCfg.h>
#include <JagDef.h>
#include <jaghashtable.h>
#include <JagRecord.h>
#include <JagUtil.h>
#include <JagStrSplit.h>
#include <JaguarCPPClient.h>
#include <JagSchemaRecord.h>
#include <JagNet.h>
#include <JagTime.h>
#include <JagFileMgr.h>
#include <JagDBPair.h>
#include <JagArray.h>
#include <JagTableOrIndexAttrs.h>
#include <JagTextFileBuffReader.h>
#include <JagUUID.h>
#include <JagADB.h>
//#include <HostMinMaxKey.h>
#include <JagFastCompress.h>
#include <JagThreadPool.h>
#include <JagVector.h>
#include <JagMutex.h>
#include <JagIndexString.h>
#include <JagBlockLock.h>
#include <JagDataAggregate.h>
#include <JagSingleBuffReader.h>
#include <JagBuffReader.h>
#include <JagBuffBackReader.h>
#include <JagDiskArrayClient.h>
#include <JagMemDiskSortArray.h>
#include <JagSQLMergeReader.h>
#include <JagReplicateBackup.h>
#include <JagParser.h>
#include <JagUserRole.h>
#include <JagGeom.h>
#include <JagLineFile.h>
#include <JagCrypt.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

// refer http://www.binarytides.com/winsock-socket-programming-tutorial/
// for winsock2.h 

/********************************************************************************************
**
**  JaguarCPPClient.cc
**
*********************************************************************************************/

JaguarCPPClient::JaguarCPPClient( )
{
	setWalLog( false );
	_init();
}

JaguarCPPClient::~JaguarCPPClient()
{
	if ( ! _destroyed ) {
	  destroy();
	}
}

// ctor all data members for JaguarCPPClient
void JaguarCPPClient::_init()
{
	_row = (ADBROW*)jagmalloc( sizeof( ADBROW ) );
	memset( _row, 0,  sizeof( ADBROW ) );
	initRow();
	_destroyed = false;
	char brand[64];
	sprintf( brand, JAG_BRAND );
	brand[0] = toupper( brand[0] );
	_brand = Jstr( brand );
	_version = Jstr(JAG_VERSION);

	// time zone diff from GMT in minutes
	_tdiff = JagTime::getTimeZoneDiff();

	// _testinsertcnt = 0;
	_sleepTime = 0;
	_isExclusive = 0;
	_cliservSameProcess = 0;
	_servCurrentCluster = 0;
	_lastConnectionBrokenTime = 0;	
	_multiThreadMidPointStart = 0;
	_multiThreadMidPointStop = 0;
	_useJPB = 0;
	_fromServ = 0;
	_connInit = 0;
	_oneConnect = 0;
	_connMapDone = 0;
	_sock = INVALID_SOCKET;
	_faultToleranceCopy = 1; // faultToleranceCode
	_end = 0;
	_qMutexThreadID = 0;
	_fromShell = 0;
	_forExport = 0;
	_isToGate = 0;
	_maxQueryNum = 1;
	_redirectSock = INVALID_SOCKET;
	_outf = NULL;
	_isCluster = false;
	_hostIdxMap = NULL;
	_clusterIdxMap = NULL;
	_thrdCliMap = NULL;
	_schemaMap = new JagHashMap<AbaxString, JagTableOrIndexAttrs>();
	_insertBuffer = NULL;
	_insertBufferCopy = NULL;
	_allHosts = NULL;
	_allHostsByCluster = NULL;
	_selectCountBuffer = NULL;
	_lock = NULL;
	_loadlock = NULL;
	_qrlock = NULL;
	_parentCli = NULL;
	_jda = NULL;
	_lineFile = NULL;
	_cfg = new JagCfg( JAG_CLIENT );
	_maxQueryNum = jagatoll(_cfg->getValue("MAX_QUERY_NUMBER", "30000").c_str());
	_dtimeout = atoi(_cfg->getValue("TRANSMIT_TIMEOUT", "3").c_str());
	_connRetryLimit = atoi(_cfg->getValue("CONNECTION_RETRY", "30").c_str());

	_queryMutex = PTHREAD_MUTEX_INITIALIZER;
	 pthread_mutex_init( &_queryMutex, NULL );

	_lineFileMutex = PTHREAD_MUTEX_INITIALIZER;
	 pthread_mutex_init( &_lineFileMutex, NULL );

	 pthread_cond_init( &_insertBufferLessThanMaxCond, NULL);
	 pthread_cond_init( &_insertBufferMoreThanMinCond, NULL);
	 pthread_mutex_init( &_insertBufferMutex, NULL );

	_mapLock = NULL;
	_qtype = 0;
	_tcode = 0;
	_fullConnectionArg = 1;  // connect to all server hosts?
	_fullConnectionOK = 0;  
	_debug = 0;  
	_spCommandErrorCnt = 0;

	_jagUUID = new JagUUID();

	_isparent = 1;
	_connectOK = false;
	_insertLog = NULL;
	_lastHasSemicolon = true;
	_numHosts = 0;
	_numClusters = 0;
	_serverReplicateMode = 0;
	_passmo = new CliPass();
	_passflush = new CliPass();
	_jpb = new JagReplicateBackup( _dtimeout, _connRetryLimit );

	_unixSocket = "_=0";
	_queryCode = 0;
	_dataSortArr = NULL;
	
	// may be removed later
	_orderByReadFrom = 0;
	_orderByKEYLEN = 0;
	_orderByVALLEN = 0;
	_orderByIsAsc = 0;
	_orderByLimit = 0;
	_orderByLimitCnt = 0;
	_orderByLimitStart = 0;
	_orderByMemArr = NULL;
	_orderByRecord = NULL;
	_orderByDiskArr = NULL;
	_orderByBuffReader = NULL;
	_orderByBuffBackReader = NULL;

	_lastHasGroupBy = 0;
	_lastHasOrderBy = 0;
	_deltaRecoverConnection = 0;
	_isFlushingInsertBuffer = 0;
	_lastQueryConnError = 0;

	_hashJoinLimit = 500000; // default hashjoin is 500000

	_makeSingleReconnect = 0;
	_qCount = 0;
	_compressFlag[0] =  _compressFlag[1] =  _compressFlag[2] = _compressFlag[3] = 0;
	_isDataCenterSync = 0;
	_datcSrcType = 0;
	_datcDestType = 0;
	_insertPool = NULL;
	_monitorDone = true;
}

// close and free up memory
void JaguarCPPClient::close()
{
	if ( ! _destroyed ) {
		destroy();
	}
}

void JaguarCPPClient::destroy()
{
	/**
	prt(("c4930 JaguarCPPClient::destroy() thrd=%lld this=%0x _isparent=%d _coectOK=%d _monitorDone=%d _oneConnect=%d...\n", 
			THREADID, this, _isparent, _connectOK, (int)_monitorDone, _oneConnect ));
	***/

	_threadend = 1;
	if ( _isparent && _connectOK && !_oneConnect ) {
		// wait for monitor done
		while ( 1 ) {
			if ( _monitorDone ) break;
    		jagsleep( 50, JAG_MSEC );
		}
		pthread_join( _threadmo, NULL );
		thpool_wait( _insertPool );
	} 

	// clean up possible _connMap
	if ( _isparent && _isCluster && _connMap.entries && !_oneConnect ) {
		JaguarCPPClient *jcli;
		HashNodeT *node, *last;
        for ( int i = 0; i < _connMap.size; ++i ) {
			node = _connMap.bucket[i];
            while (node != NULL) {
              last = node;
              node = node->next;
              if ( last->value ) {
                jcli = (JaguarCPPClient*) last->value;
				if ( ! jcli->_isparent ) {
					delete jcli;
				}
			    last->value = NULL;
			  }
            }
        }

		jag_hash_destroy( &_connMap, false );
		//printf("c2787 pthread_join _threadmo thrd=%lld  this=%0x delete jcli in connmap done\n", THREADID, this );
		//fflush( stdout );
	}

	// prt(("s7777 rayclose 4 sock=%d\n", _sock));
	if ( !socket_bad(_sock) ) rayclose( _sock );

	if ( _row ) {
		doFreeRow();
		// printf("c0140 tid=%lld freeRow..\n", THREADID ); fflush( stdout );
		free (_row);
		_row = NULL;
	}

	if ( _outf ) {
		jagfclose( _outf );
		_outf = NULL;
	}

	if ( _hostIdxMap ) {
		jagdelete( _hostIdxMap);
	}

	if ( _clusterIdxMap ) {
		jagdelete( _clusterIdxMap);
	}
	
	if ( _thrdCliMap ) {
		jagdelete( _thrdCliMap);
	}

	if ( _schemaMap ) {
		cleanUpSchemaMap( 1 );
	} 

	if ( _selectCountBuffer ) {
		jagdelete( _selectCountBuffer);
	}	

	if ( _jda ) {
		jagdelete( _jda);
	}

	if ( _lineFile ) {
		jagdelete( _lineFile);
	}
	
	if ( _qrlock ) {
		jagdelete( _qrlock);
	}

	if ( _passmo ) {
		jagdelete( _passmo);
		jagdelete( _passflush);
	}

	if ( _cfg ) {
		jagdelete( _cfg );
	}

	if ( _jagUUID ) {
		jagdelete( _jagUUID);
	}
	
	if ( _jpb ) {
		jagdelete( _jpb);
	}

	if ( _allHosts ) {
		jagdelete( _allHosts);
	}

	if ( _allHostsByCluster ) {
		jagdelete( _allHostsByCluster);
	}
	
	if ( _dataSortArr ) {
		jagdelete( _dataSortArr);
	}

	// may be removed later
	if ( _orderByMemArr ) {
		jagdelete( _orderByMemArr);
	}

	if ( _orderByRecord ) {
		jagdelete( _orderByRecord);
	}

	if ( _orderByDiskArr ) {
		_orderByDiskArr->drop();
		jagdelete( _orderByDiskArr);
	}
	
	if ( _orderByBuffReader ) {
		jagdelete( _orderByBuffReader);
	}
	
	if ( _orderByBuffBackReader ) {
		jagdelete( _orderByBuffBackReader);
	}
	
	pthread_mutex_destroy( &_queryMutex );
	pthread_cond_destroy( &_insertBufferLessThanMaxCond );
	pthread_cond_destroy( &_insertBufferMoreThanMinCond );
	pthread_mutex_destroy( &_insertBufferMutex );

	pthread_mutex_destroy( &_lineFileMutex );

	if ( _insertPool ) {
		thpool_destroy( _insertPool );
		if ( _lock ) {
			jagdelete( _lock);
		}

		if ( _loadlock ) {
			jagdelete( _loadlock );
		}
		
		if ( _insertBuffer ) {
			jagdelete( _insertBuffer );
		}
		
		if ( _insertBufferCopy ) {
			jagdelete( _insertBufferCopy);
		}

		Jstr fpath = JagFileMgr::getLocalLogDir("cmd/") + intToString( _sock );
		JagFileMgr::rmdir( fpath );
		// printf("c3939 rmdir %s\n", fpath.c_str() );

		_connectOK = false;
	}

	if ( _insertLog ) { jagfclose( _insertLog ); }

	if ( _mapLock ) {
		jagdelete ( _mapLock);
		// prt(("c2930 _mapLock deleted\n" ));
	}

	_destroyed = true;
	// printf("c8383 tid=%lld _END=1\n", THREADID );
	_end = 1;
	// prt(("s9999 testinsertcnt=%lld\n", _testinsertcnt));
}

// get session string
const char * JaguarCPPClient::getSession()
{
	return _session.c_str();
}

// get dbname
const char * JaguarCPPClient::getDatabase()
{
	return _dbname.c_str();
}

// get socket
JAGSOCK JaguarCPPClient::getSocket() const
{
	if ( _useJPB ) {
		if ( _jpb->_replypos < 0 ) return _sock;
		return _jpb->_conn[_jpb->_replypos]._sock;
	}
	return _sock;
}

// get host
Jstr JaguarCPPClient::getHost() const
{
	return _host;
}

// update connection broken time
void JaguarCPPClient::setConnectionBrokenTime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	_lastConnectionBrokenTime = now.tv_sec;
}

// check if connection broken time is exceed retry limit
int JaguarCPPClient::checkConnectionRetry()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	if ( now.tv_sec - _lastConnectionBrokenTime > _connRetryLimit ) return true;
	else return false;
}

// 0: error  1: OK   adb->socket = socket() ....
// send username and password to socket. Server verifies and sends authentication
// info back from server. OK: 1  error: 0
// unixSocket: ""/NULL, "exclusive=yes/clearex=yes/flag2=v/flag3=v"
int JaguarCPPClient::connect( const char *host, unsigned int port, 
				const char *username, const char *passwd,
				const char *dbname, const char *unixSocket,
				uabaxint clientFlag )
{
	char querys[512]; // need to be large enough after encrypt the password; at least 300 bytes for now
	int  rc, len;
	int connTimeout = 5;

	if ( _debug ) {
		prt(("c3780 JaguarCPPClient::connect( host=[%s] port=%d uid=[%s] db=[%s] us=[%s] cliflag=%lld\n",
			host, port, username, dbname, unixSocket, clientFlag ));
		prt(("c3082 datcSrcType=%d datcDestType=%d\n", _datcSrcType, _datcDestType ));
	}

	// init data members, not to do when reconnect
	if ( !_connInit ) {
		if ( strlen( username ) + strlen(passwd) > 120 ) {
			_replyerrmsg = "C1000 username or passwd too long";
			_row->type = 'E';
			return 0;
		}

		if ( 0 == strcmp( host, "localhost" ) ) {
			host = "127.0.0.1";
		}

		char hbuf[64];
		if ( 0==strcmp( host, "127.0.0.1") ) {
			JagVector<Jstr> hvec;
    		JagNet::getLocalIPs( hvec );
			for ( int i=0; i < hvec.size(); ++i ) {
				if ( hvec[i] != "127.0.0.1" ) {
					memset(hbuf, 0, 64 );
					strcpy(hbuf, hvec[i].c_str() );
					host = hbuf;
				}
			}
		}
	
		// _host = host;
		_host = JagNet::getIPFromHostName( host );
		if ( _host.length() < 2 ) {
			_replyerrmsg = "C1005 unable to resolve IP address of server";
			_row->type = 'E';
			return 0;
		}

		_port = port;
		_username = username;
		_password = passwd;
		_dbname = dbname;
		// prt(("c3384 _dbname=[%s] _host=[%s] _port=[%d]\n", _dbname.c_str(), _host.c_str(), _port ));
	
		if ( unixSocket ) _unixSocket = unixSocket;
		if ( NULL==unixSocket || strlen(unixSocket) < 1 ) _unixSocket="_=0";
		char *pval = getNameValueFromStr( unixSocket, "timeout" );
		if ( pval ) {
			connTimeout = atoi( pval);
			free( pval );
		}

		_clientFlag = clientFlag;
		if ( _clientFlag & JAG_CHILD ) {  // connection may has _connMap, _schemaMap
			_isparent = 0;
		} else {
			_isparent = 1;
		}

		if ( _clientFlag & JAG_SERV ) {
			_fromServ = 1;
		} else {
			_fromServ = 0;
		}

		if ( _clientFlag & JAG_CONNECT_ONE ) { // direct connect to only one server, used by monitor
			_oneConnect = 1;
		} else {
			_oneConnect = 0;
		}

		// printf("c3093 ************** host=[%s] _clientFlag=%d\n", _host.c_str(), _clientFlag );
	}

	if ( _isparent && !_thrdCliMap && !_oneConnect ) {
		_thrdCliMap = new JagHashMap<AbaxInt, AbaxBuffer>;
	}
	
	// make connection
	if ( 1 ) {
		// prt(("c3903 connect(%s:%d) uid=[%s] pass=[%s] ...\n", host, port, username, passwd ));
		if ( !socket_bad(_sock) ) rayclose( _sock );
		bool netErrorPrint = ( _faultToleranceCopy < 2 );
		if ( _parentCli && _parentCli->_faultToleranceCopy > 1 ) netErrorPrint = false;
		_debug && prt(("c3900 connect(%s:%d) uid=[%s] ...\n", host, port, username ));
		_sock = rayconnect( host, port, connTimeout, netErrorPrint );
		if ( socket_bad(_sock) ) {
			_replyerrmsg = "C1003 Connect to Socket Error";
			_row->type = 'E';
			_sock = INVALID_SOCKET;
			return 0;
		}

		_debug && prt(("c3904 connect(%s:%d) uid=[%s] OK  _sock=%d\n", host, port, username, _sock ));

		//////////////////  authenticate user
		int drc = _deltaRecoverConnection;
		int fromsrv = _fromServ;
		if ( !fromsrv ) fromsrv = _oneConnect;
		int rpt = 0;
		if ( _parentCli ) {
			drc = _parentCli->_deltaRecoverConnection;
			fromsrv = _parentCli->_fromServ;
			if ( !fromsrv ) fromsrv = _parentCli->_oneConnect;
		}

		// get server's public key
		sprintf( querys, "_getpubkey" );
		rc = queryDirect( querys, true, false, true ); 
		if ( ! rc ) {
			_replyerrmsg = "C0010 PubKey Error";
			_queryerrmsg = "C0010 PubKey Error";
			_row->type = 'E';
			return 0;
		}
		rc = reply();
		Jstr pubkey;
		if ( ! rc || _row->data == NULL ) { 
			_replyerrmsg = "C0010 PubKey Error";
			_queryerrmsg = "C0010 PubKey Error";
			_row->type = 'E';
			return 0; 
		}
		if ( _row->data ) {
			pubkey =  _row->data;
		} 
		replyAll();

		if ( pubkey.size() < 1 ) {
			_replyerrmsg = "C0011 PubKey Error";
			_queryerrmsg = "C0011 PubKey Error";
			_row->type = 'E';
			return 0;
		}

		Jstr encPass;
		encPass = JagEncryptStr( pubkey, passwd );
		sprintf( querys, "auth|%s|%s|%d|%d|%d|%d|%s|%d", 
				 username, encPass.c_str(), _tdiff, fromsrv, rpt, drc, _unixSocket.c_str(), getpid() );

		rc = queryDirect( querys, true, false, true ); 
		if ( ! rc ) {
			_replyerrmsg = "C1010 Auth Error";
			_queryerrmsg = "C1010 Auth Error";
			_row->type = 'E';
			// prt(("44 error auth query error\n"));
			return 0;
		}

		// prt(("c3012 queryDirect(%s) sent, reply()...\n", querys ));
		int authed = 0;
		Jstr reterr;
		while ( reply( ) ) {
			// prt(("c4802 inside reply() data=[%s]\n", _row->data ));
			if ( _row->data && _row->data[0] == 'O' && _row->data[1] == 'K' ) {
				// // reply: "OK threadid 0 h1|h2|h3"   clustermode: 0
				// prt(("c8329  _row->data=[%s]\n",  _row->data ));
				authed = 1;
				JagStrSplit sp(  _row->data, ' ', true );
				if ( sp.length() >= 2 ) {
					_session = sp[1];
				}
	
				if ( sp.length() >= 3 ) {
					_isCluster = atoi( sp[2].c_str() );
					if ( _debug ) { prt(("c3001 _isCluster=%d\n", _isCluster )); }
				}
				
				if ( sp.length() >= 4 ) {
					// #ip1|ip2#ip3|ip4..
					_primaryHostString = sp[3];
					if ( _debug ) { prt(("c3002 _primaryHostString=%s\n", _primaryHostString.c_str() )); }

					if ( _isparent ) {
						if ( _hostIdxMap ) {
							jagdelete( _hostIdxMap);
						}
						_hostIdxMap = new JagHashMap<AbaxString, abaxint>();

						if ( _clusterIdxMap ) {
							jagdelete( _clusterIdxMap);
						}
						_clusterIdxMap = new JagHashMap<AbaxString, abaxint>();

						if ( _allHosts ) {
							jagdelete( _allHosts);
						}
						_allHosts = new JagVector<Jstr>();

						if ( _allHostsByCluster ) {
							jagdelete( _allHostsByCluster);
						}
						_allHostsByCluster = new JagVector<JagVector<Jstr>>();
						_parentCli = this;
					}

					JagStrSplit sp2( sp[3], '#', true );
					for ( int i = 0; i < sp2.length(); ++i ) {
						JagVector<Jstr> chosts;
						JagStrSplit sp3( sp2[i], '|', true );
						for ( int j = 0; j < sp3.length(); ++j ) {
							++_numHosts;
							if ( _isparent ) {
								if ( _debug ) { prt(("c2283 chosts.append(%s)\n", sp3[j].c_str() )); }
								chosts.append( sp3[j] );
								_allHosts->append( sp3[j] );
								_hostIdxMap->addKeyValue( AbaxString(sp3[j]), j );
								_clusterIdxMap->addKeyValue( AbaxString(sp3[j]), i );
							}
						}
						if ( _isparent ) {
							_allHostsByCluster->append( chosts );
						}
					}
					if ( _parentCli ) _numClusters = _parentCli->_allHostsByCluster->size();
					else _numClusters = _allHostsByCluster->size();
					if ( _debug ) { prt(("c3102 _numClusters=%d\n", _numClusters )); }
				}

				if ( sp.length() >= 5 ) {
					_nthHost = jagatoll( sp[4].c_str() );
					_debug && prt(("From host number: %d\n", _nthHost ));
				}
			
				if ( sp.length() >= 6 ) {
					_faultToleranceCopy = atoi( sp[5].c_str() );
				}

				if ( sp.length() >= 7 ) {
					_cliservSameProcess = atoi( sp[6].c_str() );
					_debug && prt(("c3142 _cliservSameProcess=%d\n", _cliservSameProcess ));
				}

				if ( sp.length() >= 8 ) {
					_isExclusive = atoi( sp[7].c_str() );
				}

				if ( sp.length() >= 9 ) {
					_hashJoinLimit = jagatoll( sp[8].c_str() );
				}

				if ( sp.length() >= 10 ) {
					_isToGate = jagatoll( sp[9].c_str() );
				}

			} else {
				reterr = _replyerrmsg;
			}
		}

		// freeRow();
		// prt(("c3003 reply done\n"));
		if ( ! authed ) {
			if ( 579 == _tcode && _parentCli ) {
				_parentCli->_tcode = _tcode;
			}

			if ( reterr.size() < 1 ) {
				// _replyerrmsg = "C1013 User Auth Error";
				_replyerrmsg = _queryerrmsg = "C1013 User Authentication Error";
				// prt(("c8830 _queryerrmsg=[%s]\n", _queryerrmsg.c_str() ));
			} else {
				_replyerrmsg = _queryerrmsg = reterr;
			}

			_row->type = 'E';
			// prt(("c3004 User Auth Error\n"));
			return 0;
		}
		// printf("auth OK\n");

		///////////////////  use dbname
		len = 0;
		if ( dbname ) {
			len = strlen( dbname );
		}
		if ( len > 0 && len < 63 ) {
			_dbname = dbname;
			sprintf( querys, "use %s;", dbname );
			// prt(("c4003 sending queryDirect(%s)...\n", querys ));
			rc = queryDirect( querys, true, false, true ); 
			if ( ! rc ) {
				_replyerrmsg = "C1012 User Query Error";
				_row->type = 'E';
				// prt(("c4634 queryDirect() error\n"));
				return 0;
			}

			int usedbok = 0;
			// prt(("c4004 reply()  querys was [%s]...\n", querys ));
			while ( reply() ) {
				if ( _row->data && 0==strncmp( _row->data, "Database", 8 ) ) {
					usedbok = 1;
				}
			}
			// freeRow();
			// prt(("c4005 reply() done\n" ));

			if ( ! usedbok ) {
				_replyerrmsg =  Jstr("C1014 error: database [") + dbname  + "] does not exist";
				_row->type = 'E';
				// printf("99 error\n");
				return 0;
			}
		}
		// printf("c8403 connect ok\n");
		
		// after make successful connection, set and make replicate connection for _jpb
		if ( !_oneConnect && !_isExclusive ) {
			JagVector<Jstr> hostlist;
			getReplicateHostList( hostlist );
			_jpb->setConnAttributes( _faultToleranceCopy, drc, _port, _clientFlag, fromsrv,
				hostlist, _username, _password, _dbname, _unixSocket );
			// also set _sock to _jpb sock0
			_jpb->_conn[0]._sock = _sock;
		}
	
		// set timeout socket for server connection
		if ( 1 ) {
			JagNet::setRecvSndTimeOut( _sock, _dtimeout, _cliservSameProcess );
		}
	}

	// build connection map when not done
	if ( !_connMapDone && !_oneConnect ) {
		if ( !_isparent ) {
			_connMapDone = 1;
		} else {
			// prt(("c4006 buildConnMap()...\n"));
			if ( !buildConnMap() ) {
				_replyerrmsg = "C1099 error: failed to connect to other servers";
				_row->type = 'E';
				return 0;
			}
			_connMapDone = 1;
			// prt(("c4016 buildConnMap() done\n"));
		}
	}

	// if got IP blocked by server, close
	if ( 579 == _tcode ) {
		close();
		_init();
		return 0;
	}

	_mapLock = new JagBlockLock();
	if ( _isparent && !_mapLock && !_oneConnect ) {
		prt(("c0023 _mapLock=%0x\n", _mapLock ));
	} 
	
	if ( _isparent && !_jda && !_oneConnect ) {
		_jda = new JagDataAggregate( false );
	}

	if ( _isparent && !_selectCountBuffer && !_oneConnect ) {
		_selectCountBuffer = new JagVector<abaxint>();
	}

	if ( _isparent && !_oneConnect ) {
		// prt(("s9999 before request info\n"));
		requestInitMapInfo();
		// prt(("s9999 after request info\n"));
	}

	if ( _isparent && !_oneConnect ) {
		_qrlock = new JagReadWriteLock();
		// prt(("c9391 _qrlock=%0x\n", _qrlock ));
	}	
	
	// separate thread for monitor insertbuffer if hasmap
	if ( _isparent && !_connectOK && !_oneConnect ) {
		_threadend = 0;
		_lock = new JagReadWriteLock();
		// prt(("c2938 _lock=%0x\n", _lock ));
		_loadlock = new JagReadWriteLock();
		// prt(("c2438 _loadlock=%0x\n", _loadlock ));
		_insertBuffer = new JagVector<Jstr>();
		_insertBufferCopy = new JagVector<Jstr>();
		_passmo->cli = this;

		_insertPool = thpool_init( _numHosts );
		jagpthread_create( &_threadmo, NULL, monitorInserts, (void*)_passmo );
		// prt(("c2401 jagpthread_create monitorInserts _numHosts=%d\n", _numHosts ));
		_connectOK = true;

	}
	
	// prt(("s9999 finish connect\n"));
	return 1;
}

// method to get replicate host lists
void JaguarCPPClient::getReplicateHostList( JagVector<Jstr> &hostlist )
{
	abaxint i = 0, j = 0, i1 = 0, i2 = 0;
	
	// get current host list from parent cli
	_parentCli->_clusterIdxMap->getValue( AbaxString(_host), i );
	_parentCli->_hostIdxMap->getValue( AbaxString(_host), j );
	if ( (*(_parentCli->_allHostsByCluster))[i].length() < 2 ) {
		hostlist.append( _host );
		hostlist.append( _host );
		hostlist.append( _host );
	}

	i1 = j+1;
	i2 = j-1;
	if ( i1 >= (*(_parentCli->_allHostsByCluster))[i].length() ) i1 = 0;
	if ( i2 < 0 ) i2 = (*(_parentCli->_allHostsByCluster))[i].length()-1;

	hostlist.append( _host );
	hostlist.append( (*(_parentCli->_allHostsByCluster))[i][i1] );
	hostlist.append( (*(_parentCli->_allHostsByCluster))[i][i2] );
}

// method to update _dbname after successfully "use db"
void JaguarCPPClient::updateDBName( const Jstr &dbname )
{
	_dbname = dbname;
	// if has replicate copy, update _jpb _dbname
	if ( _jpb ) _jpb->updateDBName( _dbname );
}

// method to update _password after successfully "changepass uid password"
void JaguarCPPClient::updatePassword( const Jstr &password )
{
	_password = password;
	// if has replicate copy, update _jpb _password
	if ( _jpb ) _jpb->updatePassword( _password );
}

// method called by parentCli to make connections to each of childCli
bool JaguarCPPClient::buildConnMap()
{
	if ( _primaryHostString.length() < 1 || _allHosts->length() < 1 ) {
		return 0;
	}

	_parentCli = this;
	jag_hash_init( &_connMap, 10 );
	JaguarCPPClient *jcli;
    int rc, len = _numHosts, goodConnections = 0, connsuccess, flag = JAG_CLI_CHILD;
	if ( _fromServ ) flag = JAG_SERV_CHILD;

	for ( int i = 0; i < _allHosts->length(); ++i ) {
		if ( _host == (*(_allHosts))[i] ) {
			// parentCli connection
			_debug && prt(("c4093 jag_hash_insert_str_void(%s) ...\n", _host.c_str() ));
			jag_hash_insert_str_void ( &_connMap, _host.c_str(), (void*)this );
			++ goodConnections;
		} else {
			connsuccess = false;
			jcli = new JaguarCPPClient();
			jcli->setDebug( _debug );
			jcli->_parentCli = this;
			connsuccess = jcli->connect( (*(_allHosts))[i].c_str(), _port, _username.c_str(), _password.c_str(), _dbname.c_str(),
				_unixSocket.c_str(), flag );
			if ( !connsuccess && _faultToleranceCopy > 1 && !_parentCli->_isExclusive ) {
				// connection failure, main connection is unreachable, try to connect backup servers if has replications
				jcli->_faultToleranceCopy = _faultToleranceCopy;
				JagVector<Jstr> hostlist;
				jcli->getReplicateHostList( hostlist );
				if ( jcli->_jpb->setConnAttributes( _faultToleranceCopy, _deltaRecoverConnection, _port, _clientFlag, _fromServ,
					hostlist, _username, _password, _dbname, _unixSocket ) ) {
					jcli->_jpb->_conn[0]._sock = INVALID_SOCKET;
					connsuccess = true;
				}
			}
				
			if ( !connsuccess ) {
				jagdelete( jcli );
				if ( _fullConnectionArg ) {
					// need full connection, if one connection is broken, error return
					prt(("c3434 Error connection to [%s:%d]\n", (*(_allHosts))[i].c_str(), _port));
					return 0;
				}
			} else {
				_debug && prt(("c3328 jag_hash_insert_str_void(%s) ...\n", (*(_allHosts))[i].c_str() ));
				jag_hash_insert_str_void ( &_connMap, (*(_allHosts))[i].c_str(), (void*)jcli );
				++ goodConnections;
			}
		}
	}

	if ( goodConnections == len ) {
		_fullConnectionOK = 1;
	} else {
		_fullConnectionOK = 0;
	}

	return 1;
}

void JaguarCPPClient::requestInitMapInfo()
{
	Jstr sqlcmd("_cschema");
	queryDirect( sqlcmd.c_str() );
	while ( reply() ) {}
}

// static Task for a thread
// check _insertBuffer
// every 3 seconds passed, flush the records
// to server
void *JaguarCPPClient::monitorInserts( void *ptr )
{
	CliPass *pass = (CliPass*)ptr;
	JaguarCPPClient *cli = pass->cli;
	time_t  nowt, lastFlush, t1;
	bool  doFlush;
	abaxint   FLUSH_LIMIT = cli->_maxQueryNum * cli->_numHosts;  // records
	abaxint   bfsize = 0;
	JagClock clock;
	JagCfg cfg( JAG_CLIENT );
	int FLUSH_INTERVAL = atoi(cfg.getValue("FLUSH_INTERVAL", "500").c_str());

	// JagClock clock;
	// printf("c7373 monitorInserts tid=%lld this=%0x ...\n", THREADID, cli );
	lastFlush = JagTime::mtime(); // milliseconds
	cli->_monitorDone = false;
	//prt(("c2039 enter monitorInserts ...\n" ));
	while ( true ) {
		{
			JagReadWriteMutex loadmutex( cli->_loadlock, JagReadWriteMutex::READ_LOCK );
			bfsize = cli->_insertBuffer->size();
			if ( cli->_threadend && bfsize < 1 ) break;
		}

		nowt = JagTime::mtime();
		if ( bfsize >0 && (( nowt-lastFlush ) >= FLUSH_INTERVAL || bfsize > FLUSH_LIMIT ) ) { 
			// clock.start();
			JagReadWriteMutex loadmutex( cli->_loadlock, JagReadWriteMutex::WRITE_LOCK );
			cli->flushInsertBuffer();
			lastFlush = JagTime::mtime();
			// clock.stop();
			// t1 = clock.elapsed();
			// prt(("s6028 client flib(%d) %d ms\n", bfsize, t1));
		} else {
			if ( !cli->_threadend ) {
				//prt(("c3828 cli->_sleepTime=%d FLUSH_INTERVAL=%d\n", cli->_sleepTime, FLUSH_INTERVAL ));
				if ( cli->_sleepTime > 0 ) {
    				jagsleep( cli->_sleepTime, JAG_MSEC );
				} else {
    				jagsleep( FLUSH_INTERVAL, JAG_MSEC );
				}
			}
			continue;
		}
	}

	cli->_monitorDone = true;
	//prt(("c3828 monitrtor done\n"));
	return NULL;
}

// send DML command to socket to server
// 1: OK   0: error
int JaguarCPPClient::execute( const char *querys )
{
	int rc = query( querys, true );
	if ( ! rc ) return 0;

	while ( reply() ) {}
	if ( hasError() ) {
		return 0;
	}
	return 1;
}

// check commands type --
// return 0: single server command ( show table, desc table etc )
// return 1: insert/upsert/cinsert/dinsert command
// return 2: load command
// return 3: multiserver command ( create/drop/truncate/alter/update/delete etc. )
// return 4: select command, getfile command
// return 5: client self command ( spool, kvlen )
// return 6: exclusive login commands ( remotebackup, localbackup etc )
// return 7: insert ... select commands
int JaguarCPPClient::checkRegularCommands( const char *querys )
{
	// if ( strncasecmp( querys, "insert", 6 ) == 0 || strncasecmp( querys, "upsert", 6 ) == 0 
	if ( strncasecmp( querys, "insert", 6 ) == 0 
		|| strncasecmp( querys, "cinsert", 6 ) == 0 || strncasecmp( querys, "dinsert", 6 ) == 0 ) {
		// if ( strcasestr( querys, " select " ) ) {
		if ( strcasestrskipquote( querys+6, " select " ) ) {
			return 7;
		}
		return 1;
	} else if ( strncasecmp( querys, "load", 4 ) == 0 ) {
		return 2;
	} else if ( strncasecmp( querys, "create", 6) == 0 || strncasecmp( querys, "drop", 4) == 0 ||
		strncasecmp( querys, "truncate", 8) == 0 || strncasecmp( querys, "alter", 5) == 0 ||
		strncasecmp( querys, "changepass", 10) == 0 ||
		strncasecmp( querys, "update", 6) == 0 || strncasecmp( querys, "delete", 6) == 0 ||
		strncasecmp( querys, "import", 6) == 0 || strncasecmp( querys, "use", 3) == 0 || 
		strncasecmp( querys, "changedb", 8) == 0 || 1 == _fromServ  ||
		strncasecmp( querys, "grant", 5) == 0 || strncasecmp( querys, "revoke", 6) == 0) {
		return 3;
	} else if ( strncasecmp( querys, "select", 6 ) == 0 || strncasecmp( querys, "getfile", 7 ) == 0 ) {
		return 4;
	} else if ( strncasecmp( querys, "spool", 5) == 0 || strncasecmp( querys, "kvlen", 5 ) == 0 ||
		strncasecmp( querys, "commit", 6 ) == 0 ) {
		return 5;
	} else if ( strncasecmp( querys, "remotebackup", 12 ) == 0 || strncasecmp( querys, "localbackup", 11 ) == 0 ||
		strncasecmp( querys, "restorefromremote", 17 ) == 0 || strncasecmp( querys, "shutdown", 8 ) == 0 ||
		strncasecmp( querys, "check", 5 ) == 0 || strncasecmp( querys, "repair", 6 ) == 0 ||
		strncasecmp( querys, "addcluster", 10 ) == 0 ) {
		return 6;
	} else if ( strncasecmp( querys, "hello", 5 ) == 0 ) {
		_fromShell = 1;
	}
	return 0;
}

// method to process concurrent direct insert cmds
// currently, only called by server insert into ... select ... cmd
int JaguarCPPClient::concurrentDirectInsert( const char *querys )
{
	if ( _destroyed ) return 0;
	
	if ( _schemaUpdateString.size() > 0 ) {
		JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );

		if ( _schemaUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			rebuildSchemaMap();
			updateSchemaMap( _schemaUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}
		/***
		if ( _defvalUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			updateDefvalMap( _defvalUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_defvalUpdateString = "";
		}
		***/

	}

	// insert, invalid upsert/cinsert/dinsert
	if ( *(querys) != 'i' && *(querys) != 'I' ) {
		return 0;
	}
	// CLIENT_SEND_BATCH_RECORDS is 200000
	while ( _insertBuffer->size() > CLIENT_SEND_BATCH_RECORDS-1 || _insertBuffer->bytes() > ONE_GIGA_BYTES ) {
		jagsleep(1, JAG_MSEC);
	}
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	_insertBuffer->append( Jstr(querys), strlen( querys ) );
	mutex.writeUnlock();
	return 1;
}

int JaguarCPPClient::apiquery( const char *querys )
{
	// debug
	int rc;
	if ( 0 == strncasecmp( querys, "doshowx", 7 ) ) {
		rc = query("show tables" );
		if ( rc ) {
			while ( reply() ) {
				printRow();
			}
			return 1;
		} else {
			return 0;
		}
	} else if ( 0 == strncasecmp( querys, "doselectx", 9 ) ) {
		rc = query("select * from jbench" );
		if ( rc ) {
			while ( reply() ) {
				printRow();
			}
			return 1;
		} else {
			return 0;
		}
	}
}

// send command to socket to server
// 1: OK   0: error
// server sends number of columns in the result over socket
// send query command over sock to server 
// must be called by parent
// querys: "select ...."  "insert ...."  "_xyz ..."  "[control] select ..."
int JaguarCPPClient::query( const char *querys, bool replyFlag )
{
	_debug && prt(("c8118 query() thrd=%lld this=%0x [%s]\n", THREADID, this, querys ));

	_allSocketsBad = 0;
	if ( _destroyed ) return 0;
	//prt(("c8119 thrd=%lld this=%0x query=[%s]\n", THREADID, this, querys ));
	//_row->jsData = "";

	jaguar_mutex_lock ( &_lineFileMutex );
	if (  _parentCli->_lineFile ) {
		_debug && prt(("c2088 jagdelete  _parentCli->_lineFile _parentCli=%0x\n", _parentCli ));
		jagdelete(  _parentCli->_lineFile);
		 _parentCli->_lineFile = NULL;
	}
	jaguar_mutex_unlock ( &_lineFileMutex );

	
	JagParseParam pparam;
	Jstr retmsg, oldquerys = querys;
	// prt(("c2020 querys=[%s]\n", querys ));
	int rc = getParseInfo( oldquerys, pparam, retmsg );
	if ( rc > 0 && pparam.dbNameCmd.size() > 0 && pparam.opcode != JAG_USEDB_OP ) {
		querys = pparam.dbNameCmd.c_str();
		// prt(("c1129 dbNameCmd=[%s]\n", querys ));
	}
	// prt(("c2021 querys=[%s]\n", querys ));

	if ( _oneConnect ) {
		// prt(("c2029 queryDirect(%s) ...\n" ));
		int rc =  queryDirect( querys, replyFlag, false, true );
		// prt(("c2439 queryDirect(%s) done rc=%d\n", rc ));
		return rc;
	}
	
	// prt(("c3391 this=%0x  thrd=%lld query(%s) reply=%d...\n", this, THREADID, querys, reply ));
	_queryerrmsg = "";
	if ( *querys == ';' || *querys == '\n' ) {
		_queryerrmsg = "No query command.";
		return 0;
	}
	
	// may be removed later
	_lastHasGroupBy = 0;
	_lastHasOrderBy = 0;

	_queryCode = 0;
	if ( _jda ) _jda->clean();
	
	_selectTruncateMsg = "";
	_aggregateData = "";
	_pointQueryString = _dataFileHeader = _dataSelectCount = _lastQuery = _randfilepath = "";
	_orderByKEYLEN = _orderByVALLEN = _orderByReadFrom = _orderByIsAsc = 0;
	_queryCode = _orderByKEYLEN = _orderByVALLEN = 0;
	_orderByReadFrom = _orderByIsAsc = 0;
	_orderByLimit = _orderByLimitCnt = _orderByLimitStart = 0;
	_orderByReadPos = _orderByWritePos = 0;
	_forExport = 0;
	_exportObj = "";
	if ( _orderByMemArr ) {
		jagdelete( _orderByMemArr );
	}
	if ( _orderByRecord ) {
		jagdelete( _orderByRecord );
	}
	if ( _orderByDiskArr ) {
		_orderByDiskArr->drop();
		jagdelete( _orderByDiskArr );
	}
	if ( _orderByBuffReader ) {
		jagdelete( _orderByBuffReader );
	}
	if ( _orderByBuffBackReader ) {
		jagdelete( _orderByBuffBackReader);
	}

	// before new query, check if needs to update host list ( after add cluster happens )
	if ( _hostUpdateString.size() > 0 ) {
		commit();
		rebuildHostsConnections( _hostUpdateString.c_str(), _hostUpdateString.length() );
	}
	// also check if needs to update schema map
	if ( _schemaUpdateString.size() > 0 ) {
		JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );
		if ( _schemaUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			rebuildSchemaMap();
			updateSchemaMap( _schemaUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}
		/***
		if ( _defvalUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			updateDefvalMap( _defvalUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_defvalUpdateString = "";
		}
		***/
	}
	
	abaxint len = strlen( querys );
	JaguarCPPClient *tcli = this;
	int setEnd = JAG_END_BEGIN;
	int qmode = checkRegularCommands( querys );
	Jstr newquery = querys;
	JagParseParam parseParam;
	bool doneParse = false;

	if ( 4 == qmode ) {
		// qwer
		rc = getParseInfo( newquery, parseParam, retmsg );
		if ( ! rc ) {
			_queryerrmsg = retmsg;
			_debug && prt(("c2043 getParseInfo rc=%d return 0 [%s]\n", rc, retmsg.c_str() ));
			return 0;
		}

		if ( parseParam.isSelectConst() ) {
			qmode = 0;
		}
		doneParse = true;
		_lastOpCode = JAG_SELECT_OP;
		_queryCode = parseParam.opcode;
	}

	if ( _isExclusive && 6 != qmode && strncasecmp( querys, "hello", 5 ) != 0 ) {
		prt(("Regular command is not accpeted by exclusive admin user\n"));
		return 0;
	}

	if ( !_isExclusive && 6 == qmode ) {
		prt(("Maintenance command is only accpeted by exclusive admin user\n"));
		return 0;
	}
	
	if ( 1 == qmode ) {
		// insert, invalid upsert/cinsert/dinsert
		if ( *(querys) != 'i' && *(querys) != 'I' ) {
			_queryerrmsg = "E5851 Error query command, syntax error";
			// prt(("c2481\n"));
			return 0;
		}
		// now, need to parse the command, and check with schema to see if the table contain file part
		// if true, use single insert
		// otherwise, use batch insert
		rc = getParseInfo( newquery, parseParam, retmsg );
		// tclock.stop(); prt(("parser parse time is=%lld micro\n", tclock.elapsedusec()));
		if ( ! rc ) {
			_queryerrmsg = retmsg;
			// prt(("c2281\n"));
			return 0;
		}

		AbaxString hdbobj;
		const JagTableOrIndexAttrs *objAttr = NULL;
		_mapLock->readLock( -1 );
		if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, retmsg ) ) {
			// table does not exist, retry
			_mapLock->readUnlock( -1 );
			_parentCli->queryDirect( "_cschema" );
			while ( _parentCli->reply() ) {};
			if ( _schemaUpdateString.size() > 0 ) {
				_mapLock->writeLock( -1 );
				rebuildSchemaMap();
				updateSchemaMap( _schemaUpdateString.c_str() );
				_mapLock->writeUnlock( -1 );
				_schemaUpdateString = "";
			}
			/***
			if ( _defvalUpdateString.size() > 0 ) {
				_mapLock->writeLock( -1 );
				updateDefvalMap( _defvalUpdateString.c_str() );
				_mapLock->writeUnlock( -1 );
				_defvalUpdateString = "";
			}
			***/
			_mapLock->readLock( -1 );
			if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, retmsg ) ) {
				_mapLock->readUnlock( -1 );
				// prt(("c2439 %s\n", retmsg.c_str() ));
				return 0;
			}
		}

		prt(("c1282 objAttr=%0x\n", objAttr ));

		bool hasFile = objAttr->hasFile, noQueryButReply = false;
		JagVector<Jstr> *files = new JagVector<Jstr>();
		_mapLock->readUnlock( -1 );
			
		if ( hasFile ) {
			// single insert
			pthread_t tid = THREADID;
			JaguarCPPClient *tcli;
			JagVector<JagDBPair> cmdhosts;
			int errorrc;
			rc = processInsertCommands( cmdhosts, parseParam, retmsg, files );
			if ( ! rc || cmdhosts.size() != 1 ) {
				_queryerrmsg = retmsg;
				if ( files ) delete files;
				return 0;
			}
			if ( _thrdCliMap->keyExist( tid ) ) {
				_queryerrmsg = "Command has been sent to server. Please reply() before sending new query.";
				if ( files ) delete files;
				return 0;
			}
			if ( _debug ) { prt(("c1188 jag_hash_lookup(%s) ...\n", cmdhosts[0].value.c_str() )); }
			tcli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, cmdhosts[0].value.c_str() );
			if ( ! tcli ) {
				if ( files ) delete files;
				return 0;
			}

			JagReadWriteMutex loadmutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );
			errorrc = 1;
			while ( errorrc > 0 ) {
				// prt(("c4293 host=[%s] tcli->queryDirect(%s) ...\n", tcli->_host.c_str(), cmdhosts[0].key.c_str() ));
				tcli->queryDirect( cmdhosts[0].key.c_str(), true, true, false, true );
				_thrdCliMap->addKeyValue( tid, AbaxBuffer((void*)tcli) );
				errorrc = tcli->_lastQueryConnError; // 1 represent error, and 0 is ok
				//_debug && prt(("c1949 tcli->_lastQueryConnError=%d \n", tcli->_lastQueryConnError ));
				if ( !tcli->_lastQueryConnError ) {
					if ( _datcSrcType != JAG_DATACENTER_GATE ) {
						if ( *(cmdhosts[0].key.c_str()) == 's' ) {
							rc = tcli->sendFilesToServer( *files );
						}
						//_debug && prt(("c0939 reply() _lastQueryConnError=%d rc=%d ...\n", tcli->_lastQueryConnError, rc ));
						//_debug && prt(("c0914 not do tcli->reply() \n" ));
						while ( tcli->reply() ) {
							_debug && prt(("c5029 data=[%s]\n", tcli->getMessage() ));
						}
						//_debug && prt(("c0949 reply() done _lastQueryConnError=%d ...\n", tcli->_lastQueryConnError ));
						//_debug && prt(("c03849 tcli->_lastQueryConnError=%d\n", tcli->_lastQueryConnError ));
						errorrc = 0;
					} else {
						noQueryButReply = true;
						errorrc = tcli->_lastQueryConnError; // 1 represent error, and 0 is ok
					}
					// errorrc = tcli->_lastQueryConnError; // 1 represent error, and 0 is ok
				}
				if ( !errorrc ) break;
				if ( tcli->_parentCli && tcli->_parentCli->_fromShell ) {
					prt(("E2018 insert to host [%s] and backup hosts failed, retry in 10 seconds ...\n", tcli->_host.c_str()));
				}
				jagsleep( 10, JAG_SEC );
			}
			if ( noQueryButReply ) {
				setEnd = JAG_END_NOQUERY_BUT_REPLY;
				tcli->_end = JAG_END_NOQUERY_BUT_REPLY;
			} else {
				tcli->_end = JAG_END_NORMAL;
			}
			// prt(("c5028 query=[%s] rc=%d\n", cmdhosts[0].key.c_str(), rc ));
			if ( files ) delete files;
			return 1;
		} else {
			// batch insert
			// CLIENT_SEND_BATCH_RECORDS is 200000
			while ( _insertBuffer->size() > CLIENT_SEND_BATCH_RECORDS-1 || _insertBuffer->bytes() > ONE_GIGA_BYTES ) {
				jagsleep(1, JAG_MSEC);
			}
			JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
			_insertBuffer->append( Jstr(querys), len );
			mutex.writeUnlock();
			setEnd = JAG_END_NORMAL;	
		}
		delete files;
		setEnd = JAG_END_NORMAL;	
	} else if ( 2 == qmode ) {
		// load 
		abaxint loadcnt = loadFile( querys );
		setEnd = JAG_END_NORMAL;
		_queryStatMsg = longToStr(loadcnt) + " rows loaded";
		if ( ! loadcnt ) return 0;
	} else if ( 3 == qmode || 4 == qmode || 7 == qmode ) {
		// create/drop/truncate/alter/select etc
		if ( 4 == qmode ) {
			_lastOpCode = JAG_SELECT_OP;
		} else if ( 7 == qmode ) {
			_lastOpCode = JAG_INSERTSELECT_OP;
		} else {
			_lastOpCode = JAG_NOOP;
		}

		_debug && prt(("c5320 _lastOpCode=%d\n", _lastOpCode ));
		if ( _lastOpCode != JAG_SELECT_OP ) {
			_debug && prt(("c4004 commit()...\n" ));
			commit();
			_debug && prt(("c4004 commit() done qdirect _chost...\n" ));
			_parentCli->queryDirect( Jstr("_chost").c_str() );
			_debug && prt(("c4005 queryDirect( _chost ) done\n" ));
			while ( _parentCli->reply() ) {}
			_debug && prt(("c4006 reply( ) done\n" ));
			if ( _hostUpdateString.size() > 0 ) {
				_debug && prt(("c4056 rebuildHostsConnections...\n" ));
				rebuildHostsConnections( _hostUpdateString.c_str(), _hostUpdateString.length() );
				_debug && prt(("c4056 rebuildHostsConnections done\n" ));
			}
		}

		if ( ! doneParse ) {
			// JagClock tclock; tclock.start();
			rc = getParseInfo( newquery, parseParam, retmsg );
			// tclock.stop(); prt(("parser parse time is=%lld micro\n", tclock.elapsedusec()));
			if ( ! rc ) {
				_queryerrmsg = retmsg;
				_debug && prt(("c2093 getParseInfo rc=%d return 0 [%s]\n", rc, retmsg.c_str() ));
				return 0;
			}
		}

		_debug && prt(("c5023 processMultiServerCommands qmode=%d (%s)\n", qmode, querys ));
		rc = processMultiServerCommands( querys, parseParam, retmsg );
		_debug && prt(("c0984 processMultiServerCommands qs=[%s] done rc=%d\n", querys, rc ));
		if ( ! rc ) {
			_queryerrmsg = retmsg;
			return 0;
		}
		setEnd = rc;
		_queryCode = parseParam.opcode;
	} else if ( 5 == qmode ) {
		// spool/kvlen etc spool /path/log.file; spool off;
		if ( strncasecmp( querys, "spool", 5) == 0 ) {
			rc = processSpool( (char*)querys );
			if ( !rc ) {
				_queryerrmsg = _replyerrmsg;
				return 0;
			}
			setEnd = JAG_END_NORMAL;
		} else if ( strncasecmp( querys, "kvlen", 5 ) == 0 ) {
			rc = doKVLen( querys, len, retmsg );
			if ( rc < 0 ) return 0;
			setEnd = JAG_END_RECVONE_THEN_DONE;
		} else if ( strncasecmp( querys, "commit", 6 ) == 0 ) {
			commit();
			setEnd = JAG_END_NORMAL;
			return 1;
		}
	} else if ( 6 == qmode ) {
		// exclusive admin commands, remotebackup/localbackup etc.
		commit();
		rc = processMaintenanceCommand( querys );
		if ( ! rc ) return 0;
		setEnd = JAG_END_NORMAL;
	}

	// prt(("c1803 setEnd=%d _isCluster=%d query=[%s]\n", setEnd, _isCluster, querys ));

	if ( JAG_END_BEGIN == setEnd && _isCluster ) {
		// desc/show/help/hello
		/***
		rc = getParseInfo( newquery, parseParam, retmsg );
		if ( *querys == '_' && _username =="admin" ) rc = true;
		if ( ! rc ) {
			_queryerrmsg = retmsg;
			return 0;
		}
		***/
		if ( ! doneParse ) {
			rc = getParseInfo( newquery, parseParam, retmsg );
			if ( ! rc ) { _queryerrmsg = retmsg; return 0; }
			if ( *querys == '_' && _username =="admin" ) rc = true;
		}

		if ( JAG_SHOWCVER_OP == parseParam.opcode ) {
			retmsg = _brand + " Client " + _version;
 			setEnd = JAG_END_RECVONE_THEN_DONE;
		}
	}
	
	pthread_t tid = THREADID;
	if ( _thrdCliMap->keyExist( tid ) ) {
		// _queryerrmsg = "Command has already been sent to server. Please reply first before sending new query.";
		AbaxBuffer bfr; JaguarCPPClient *tcli = this;
		if ( setEnd = JAG_END_NOQUERY_BUT_REPLY && _thrdCliMap->getValue( tid, bfr ) ) {
			//  do not  do doquery anymore, do reply()
			// already done query
			tcli = (JaguarCPPClient*) bfr.value();
			tcli->_end = JAG_END_NOQUERY_BUT_REPLY;
			return 1; // do not do query
		} else {
			// normal query, reply()
			_qMutexThreadID = 0;
			jaguar_mutex_unlock ( &_queryMutex );
			//prt(("c1238 cli=%0x host=[%s] jaguar_mutex_unlock _queryMutex done\n", this, _host.c_str() ));
		}
		// return 0;
	} else {
		_thrdCliMap->addKeyValue( tid, AbaxBuffer((void*)tcli) );
	}

	// parameters: querys, reply, compress, batchReply, dirConn, setEnd, msg
	rc = tcli->doquery( newquery.c_str(), replyFlag, true, false, false, setEnd, retmsg.c_str() );
	if ( 0 == rc || ! replyFlag ) _thrdCliMap->removeKey( tid );
	if ( ! rc ) {
		_queryerrmsg = retmsg;
	}

	//prt(("c5029 query=[%s] rc=%d\n", newquery.c_str(), rc ));
	return rc;
}

int JaguarCPPClient::queryDirect( const char *querys, bool reply, bool batchReply, bool dirConn, bool forceConnection )
{
	if ( _oneConnect ) {
		return doquery( querys, reply, true, batchReply, dirConn, 0, "", forceConnection );
	}

	pthread_t tid = THREADID;
	if ( _isparent ) {
		if ( _thrdCliMap->keyExist( tid ) ) {
			if ( _makeSingleReconnect > 0 && (strncmp( querys, "auth|", 5 ) == 0 || strncmp( querys, "use ", 4 ) == 0 ||
				strncmp( querys, "_cschema", 8 ) == 0 || strncmp( querys, "_getpubkey", 10 ) == 0) ) {
				return doquery( querys, reply, true, batchReply, dirConn, 0, "", forceConnection );
			} else {
				return 0;
			}
		} else {
			_thrdCliMap->addKeyValue( tid, AbaxBuffer((void*)this) );
		}
	}
	int rc = doquery( querys, reply, true, batchReply, dirConn, 0, "", forceConnection );
	if ( ( 0 == rc || ! reply ) && _isparent ) {
		_thrdCliMap->removeKey( tid );
		// prt(("c5054 this=%0x  _thrdCliMap->removeKey(%lld)\n", this, tid ));
	}

	_debug && prt(("c4551 this=%0x thrd=%lld host=[%s] doquery( %s ) rc=%d...\n", this, THREADID, _host.c_str(), querys, rc ));
	return rc;
}
	
// 0: error   1: OK
int JaguarCPPClient::doquery( const char *querys, bool reply, bool compress, bool batchReply, bool dirConn,
							  int setEnd, const char *msg, bool forceConnection )
{
	_debug && prt(("c4561 doquery this=%0x thrd=%lld host=[%s] [%.200s] setEnd=%d ...\n", this, THREADID, _host.c_str(), querys, setEnd ));

	if ( !_oneConnect ) {
		// if forceConnection, retry
		if ( forceConnection && (_faultToleranceCopy < 2 || dirConn) && socket_bad(_sock) ) {
			// retry connection
			_makeSingleReconnect = 1;
			if ( !connect( _host.c_str(), _port, _username.c_str(), _password.c_str(), 
							_dbname.c_str(), _unixSocket.c_str(), _clientFlag ) ) {
				_makeSingleReconnect = 0;
				_debug && prt(("c3094 connect error, return 0 host=[%s]\n",  _host.c_str() ));
				return 0;
			}
			_makeSingleReconnect = 0;
		} else {
			// for replicate copy == 1 and from server, if connection broken, retry
			int fsf = _fromServ;
			if ( _parentCli ) fsf = _parentCli->_fromServ;
			if ( (_faultToleranceCopy < 2 || dirConn) && fsf && socket_bad(_sock) ) {
				// retry connection
				if ( _faultToleranceCopy < 2 || checkConnectionRetry() ) {
					_makeSingleReconnect = 1;
					if ( !connect( _host.c_str(), _port, _username.c_str(), _password.c_str(), 
								   _dbname.c_str(), _unixSocket.c_str(), _clientFlag ) ) {
						_makeSingleReconnect = 0;
						// prt(("c3095 connect error, return 0 host=[%s]\n",  _host.c_str() ));
						return 0;
					}
					_makeSingleReconnect = 0;
				}
			}
		}
	}
	
	const char *p;
	int forceflush = 0;
	abaxint rc = 0, len = strlen( querys );
	abaxint modv;
	while ( isspace(*querys) ) ++querys;

	JagReadWriteLock *ll = NULL;
	JagReadWriteMutex mutex( ll );
	if ( !_oneConnect && _parentCli && _parentCli->_loadlock && _host == _parentCli->_host && 
		( setEnd == JAG_END_GOT_DBPAIR_AND_ORDERBY || setEnd == JAG_END_GOT_DBPAIR || setEnd == JAG_END_SENDQUERY_LOADLOCK ) ) {
		ll = _parentCli->_loadlock;
		mutex.writeLock();
		forceflush = 1;
	}

	if ( !_oneConnect ) {
		// prt(("c0333 cli=%0x host=[%s] jaguar_mutex_lock _queryMutex ...\n", this, _host.c_str() ));
		JAG_BLURT jaguar_mutex_lock ( &(this->_queryMutex) ); JAG_OVER;
		// prt(("c0333 cli=%0x host=[%s] jaguar_mutex_lock _queryMutex done\n", this, _host.c_str() ));
		_qMutexThreadID = THREADID;
	}

	if ( forceflush ) {
		mutex.writeUnlock();
	}
	
	_lastQueryConnError = 0;
	if ( strncasecmp( querys, "sel", 3) == 0 || strncasecmp( querys, "getfile", 7) == 0 ) {
		_qtype = 1;
	} else {
		_qtype = 0;
	}
	freeRow();
	initRow();
	
	if ( JAG_END_NORMAL == setEnd ) {
		_end = JAG_END_NORMAL;
	} else if ( JAG_END_NOQUERY_BUT_REPLY == setEnd ) {
		_end = JAG_END_NOQUERY_BUT_REPLY;
		// do not do doQuery(), but doReply()
	} else if ( JAG_END_RECVONE_THEN_DONE == setEnd ) {
		_row->type = 'I';
		int msglen = strlen(msg);
		char *buf = (char*)jagmalloc( msglen+1 );
		memcpy( buf, msg, msglen );
		buf[ msglen ] = '\0';
		_row->data = buf;
		_end = JAG_END_RECVONE_THEN_DONE;
	} else if ( JAG_END_RECVONE_THEN_DONE_ERR == setEnd ) {
		_row->type = 'E';
		_queryerrmsg = msg;
		_end = JAG_END_RECVONE_THEN_DONE;
	} else if ( JAG_END_GOT_DBPAIR_AND_ORDERBY == setEnd ) {
		_end = JAG_END_GOT_DBPAIR_AND_ORDERBY;
	} else if ( JAG_END_GOT_DBPAIR == setEnd ) {
		_end = JAG_END_GOT_DBPAIR;
	} else {
		// small strings need not compress
		if ( len < JAG_SOCK_COMPRSS_MIN ) {
			compress = false;
		} else {
			++ _qCount;
			// do compress sampling
			Jstr compStr;
			modv =  _qCount%1000;
			if (  0 <= modv && modv <= 3 ) {
				JagFastCompress::compress( querys, compStr );
				if (  compStr.size() < len ) { _compressFlag[modv] = 1; } else { _compressFlag[modv] = 0; }
			}

			// if 3/4 sampling say yes, then do compress
			if ( ( _compressFlag[0] + _compressFlag[1] +  _compressFlag[2] + _compressFlag[3] ) >= 3  ) {
				compress = true;
				// prt(("c1838 compress true\n"));
			} else {
				compress = false;
			}
		}

		// prt(("c4488 thrd=%lld this=%0x parent=%0x _control=[%s] doQuery(%s)\n", 
		// 		THREADID, this, _parentCli, _control.c_str(), querys ));
		/**
		if ( _parentCli ) {
			prt(("c3387 _parentCli->control=[%s]\n", _parentCli->_control.c_str() ));
		}
		**/

		char cbyte = 'C';
		/***
		if ( _parentCli && _parentCli->_control == "N" ) {
			cbyte = 'N';
		}
		***/
		
		// send data part header
		Jstr compressedStr;
		if ( compress ) {
			if ( reply ) {
				JagFastCompress::compress( querys, compressedStr );
				if ( batchReply ) {
					sprintf( sendhdr, "%0*lldABZ%c", JAG_SOCK_MSG_HDR_LEN-4, compressedStr.size(), cbyte ); 
					// ABZC  C is control byte
				} else {
					sprintf( sendhdr, "%0*lldACZ%c", JAG_SOCK_MSG_HDR_LEN-4, compressedStr.size(), cbyte ); 
				}
				_hasReply = true;
			} else {
				JagFastCompress::compress( querys, compressedStr );
				sprintf( sendhdr, "%0*lldANZ%c", JAG_SOCK_MSG_HDR_LEN-4, compressedStr.size(), cbyte ); 
				_hasReply = false;
			}
			p = compressedStr.c_str();
			len = compressedStr.size();
		} else {
			if ( reply ) {
				if ( batchReply ) {
					sprintf( sendhdr, "%0*lldABC%c", JAG_SOCK_MSG_HDR_LEN-4, len, cbyte );   
				} else {
					sprintf( sendhdr, "%0*lldACC%c", JAG_SOCK_MSG_HDR_LEN-4, len, cbyte );   
				}
				_hasReply = true;
			} else {
				sprintf( sendhdr, "%0*lldANC%c", JAG_SOCK_MSG_HDR_LEN-4, len, cbyte );  
				_hasReply = false;
			}
			p = querys;
		}

		int ftc = 1;
		if ( _parentCli ) ftc = _parentCli->_faultToleranceCopy;
		// prt(("c4562 ftc=%d host=[%s] sendhdr=[%s] query=[%s] dirConn=%d exc=%d ...\n", ftc, _host.c_str(), sendhdr, p, dirConn, _isExclusive ));
		// prt(("c4563 compress=%d, bclen=%d, aflen=%d, bcquery=[%s] afquery=[%s]\n", compress, strlen( querys ), len, querys, p));
		// _lastquery = querys;
		if ( dirConn || ftc < 2 || _isExclusive ) { // no replicate
			_useJPB = 0;
			if ( JAG_SOCK_MSG_HDR_LEN + len <= 2048 ) {
				memset( _qbuf, 0, 2048 );
				memcpy( _qbuf, sendhdr, JAG_SOCK_MSG_HDR_LEN);
				memcpy( _qbuf+JAG_SOCK_MSG_HDR_LEN, p, len );
				rc = sendData( _sock, _qbuf, JAG_SOCK_MSG_HDR_LEN + len );
			} else {
				beginBulkSend( _sock );
				rc = sendData( _sock, sendhdr, JAG_SOCK_MSG_HDR_LEN );
				rc = sendData( _sock, p, len );
				endBulkSend( _sock );
			}

			if ( rc < 0 ) {
				_allSocketsBad = 1;
			}

		} else {
			_useJPB = 1;
			bool isWrite = checkReadOrWriteCommand( querys );
			if ( JAG_SOCK_MSG_HDR_LEN + len <= 2048 ) {
				memset( _qbuf, 0, 2048 );
				memcpy( _qbuf, sendhdr, JAG_SOCK_MSG_HDR_LEN);
				memcpy( _qbuf+JAG_SOCK_MSG_HDR_LEN, p, len );
				rc = _jpb->sendQuery( _qbuf, JAG_SOCK_MSG_HDR_LEN+len, _hasReply, isWrite, false, forceConnection );
			} else {
				// since sendQuery need a whole buffer to process, malloc now
				char *allbuf = (char*)jagmalloc(JAG_SOCK_MSG_HDR_LEN + len);
				memcpy( allbuf, sendhdr, JAG_SOCK_MSG_HDR_LEN);
				memcpy( allbuf+JAG_SOCK_MSG_HDR_LEN, p, len );
				rc = _jpb->sendQuery( allbuf, JAG_SOCK_MSG_HDR_LEN+len, _hasReply, isWrite, true, forceConnection );
				free( allbuf );
			}
		}
		
		// if query failed, stop connection ( too many connections broken, no more fault tolerance )
		// prt(("c0938 send rc=%d\n", rc ));

		if ( rc < 0 ) {
			_replyerrmsg = "C1007 error sending message";
			_debug && prt(("c3847 _lastQueryConnError=1\n" ));
			_lastQueryConnError = 1;
			// prt(("ERROR, send all timeout host0=[%s]\n", _host.c_str()));
			if ( !_oneConnect ) {
				_qMutexThreadID = 0;
				JAG_BLURT_UN jaguar_mutex_unlock ( &_queryMutex );
				// prt(("c2238 cli=%0x jaguar_mutex_unlock _queryMutex done\n", this, _host.c_str() ));
			}
			if ( !_useJPB ) {
				_sock = INVALID_SOCKET;
				setConnectionBrokenTime();
			}
			// prt(("c3097 error sending msg, return 0\n" ));
			return 0;
		}
		
		if ( ! reply ) {
			if ( !_oneConnect ) {
				_qMutexThreadID = 0;
				JAG_BLURT_UN jaguar_mutex_unlock ( &_queryMutex );
				// prt(("c2268 cli=%0x host=[%s] jaguar_mutex_unlock _queryMutex done\n", this, _host.c_str() ));
			}
		}
	}

	_debug && prt(("c3390 doquery done, return 1\n" ));
	return 1;
}

// method to send directly to socket 
abaxint JaguarCPPClient::sendDirectToSockAll( const char *mesg, abaxint len, bool nohdr )
{
	// prt(("s3740 sendDirectToSockAll mesg=[%s]\n", mesg ));
	AbaxBuffer bfr;
	JaguarCPPClient *cli = this;
	pthread_t tid = THREADID;
	abaxint rc = 1;

	// prt(("\n\n_isparent=%d\n", _isparent ));
	if ( _isparent ) {
		if ( _thrdCliMap->getValue( tid, bfr ) ) {
			cli = (JaguarCPPClient*) bfr.value();
		} else {
			rc = 0;
			cli = this;
		}
	} else {
		cli = this;
	}

	// prt(("c6881 in sendDirectToSockAll isparent=%d _debug=%d  cli->_debug=%d rc=%d this=%0x cli=%0x _jpb=%0x  thrd=%lld _useJPB=%d\n", 
	// _isparent, _debug, cli->_debug, rc, this, cli, _jpb, THREADID, _useJPB ));
	if ( rc ) {
		if ( cli->_debug ) { prt(("c0394 cli=%0x -> doSendDirectToSockAll(%s)\n", cli, mesg )); }
		rc = cli->doSendDirectToSockAll( mesg, len, nohdr );
		if ( cli->_debug ) { prt(("c0394 cli=%0x -> doSendDirectToSockAll(%s) done rc=%lld\n", cli, mesg, rc )); }
	} else {
		if ( cli->_debug ) { prt(("c0395 NOT doSendDirectToSockAll(%s)\n", mesg )); }
	}

	_debug && prt(("c9222 sendDirectToSockAll return this=%0x cli=%0x mesg=[%s] rc=%lld\n", this, cli, mesg, rc ));
	return rc;
}

abaxint JaguarCPPClient::doSendDirectToSockAll( const char *mesg, abaxint len, bool nohdr )
{
	abaxint rlen =0;
	_debug && prt(("c2002 doSendDirectToSockAll mesg=[%s] ...\n", mesg ));
	_debug && prt(("c0822 _useJPB=%d doSendDirectToSockAll(%s)\n", _useJPB, mesg ));
	if ( !_useJPB || _datcDestType == JAG_DATACENTER_GATE ) {
		_debug && prt(("c4822 ******* sendDirectToSock _sock=%d [%s] ...\n", _sock, mesg ));
		rlen = sendDirectToSock( _sock, mesg, len, nohdr );
	} else {
		_debug && prt(("c4003 _jpb=%0x->sendDirectToSockAll(%s)\n", _jpb, mesg ));
		rlen = _jpb->sendDirectToSockAll( mesg, len, nohdr );
		_debug && prt(("c4003 _jpb=%0x->sendDirectToSockAll(%s) done rlen=%lld\n", _jpb, mesg, rlen ));
	}
	return rlen;
}

// method to recv directly from socket
abaxint JaguarCPPClient::recvDirectFromSockAll( char *&buf, char *hdr )
{
	AbaxBuffer bfr;
	JaguarCPPClient *cli = this;
	pthread_t tid = THREADID;
	abaxint rc = 1;

	// prt(("\n\n_isparent=%d\n", _isparent ));
	if ( _isparent ) {
		if ( _thrdCliMap->getValue( tid, bfr ) ) {
			cli = (JaguarCPPClient*) bfr.value();
		} else {
			rc = 0;
			// prt(("c7374 not parent rc= 0 \n"));
		}
	}
	if ( rc ) rc = cli->doRecvDirectFromSockAll( buf, hdr );
	return rc;
}

abaxint JaguarCPPClient::doRecvDirectFromSockAll( char *&buf, char *hdr )
{
	abaxint rlen;
	if ( !_useJPB || _datcDestType == JAG_DATACENTER_GATE ) {
		rlen = recvDirectFromSock( _sock, buf, hdr );
	} else {
		rlen = _jpb->recvDirectFromSockAll( buf, hdr );
	}
	// process special case: if hdr has 'X1', set _end
	if ( 'X' == hdr[JAG_SOCK_MSG_HDR_LEN-3] && '1' == hdr[JAG_SOCK_MSG_HDR_LEN-2] ) {
		_end = 1;
	}
	return rlen;
}

abaxint JaguarCPPClient::recvDirectFromSockAll( char *&buf, abaxint len )
{
	AbaxBuffer bfr;
	JaguarCPPClient *cli = this;
	pthread_t tid = THREADID;
	abaxint rc = 1;

	// prt(("\n\n_isparent=%d\n", _isparent ));
	if ( _isparent ) {
		if ( _thrdCliMap->getValue( tid, bfr ) ) {
			cli = (JaguarCPPClient*) bfr.value();
		} else {
			rc = 0;
			// prt(("c7374 not parent rc= 0 \n"));
		}
	}
	if ( rc ) rc = cli->doRecvDirectFromSockAll( buf, len );
	return rc;
}

abaxint JaguarCPPClient::doRecvDirectFromSockAll( char *&buf, abaxint len )
{
	abaxint rlen;
	if ( !_useJPB || _datcDestType == JAG_DATACENTER_GATE ) {
		rlen = recvData( _sock, buf, len );
	} else {
		rlen = _jpb->recvDirectFromSockAll( buf, len );
	}
	return rlen;
}

// method to send files to client one by one, only called by insert command with files
// return number of files sent
int JaguarCPPClient::sendFilesToServer( const JagVector<Jstr> &files )
{	
	// first need to send _BEGINFILEUPLOAD_ to server
	_debug && prt(("c3394 this=%0x sendFilesToServer files.size=%d _debug=%d _useJPB=%d ...\n", this, files.size(), _debug, _useJPB ));

	int cnt = 0;
	if ( _debug ) { prt(("c5509 files.size=%d\n", files.size() )); }

	if ( _debug ) { prt(("c5032 sending _BEGINFILEUPLOAD_ to server ...\n" )); }
	int rc = sendDirectToSockAll( "_BEGINFILEUPLOAD_", 17 );
	if ( _debug ) { prt(("c5032 sending _BEGINFILEUPLOAD_ to server done rc=%d ...\n", rc )); }

	for ( int i = 0; i < files.size(); ++i ) {
		if ( !_useJPB ) {
			if ( _debug ) { prt(("c7092 oneFileSender() i=%d thread=%lld\n", i, THREADID )); }
			cnt += oneFileSender( _sock, files[i] );
		} else {
			if ( _debug ) { prt(("c7093 _jpb->sendFilesToServer() i=%d thread=%lld\n", i, THREADID )); }
			cnt += _jpb->sendFilesToServer( files[i] );
		}
	}	

	if ( _debug ) { prt(("c5343 sending _ENDFILEUPLOAD_ to server ...\n" )); }
	rc = sendDirectToSockAll( "_ENDFILEUPLOAD_", 15 );
	if ( _debug ) { prt(("c5343 sending _ENDFILEUPLOAD_ to serverd done rc=%d\n", rc )); }
	
	if ( _debug ) prt(("c6383 this=%0x cnt=%d\n\n", this, cnt ));
	return cnt;
}

// method to recv files from client one by one, only called by getfile command
// return 0: send error; return 1: send ok
int JaguarCPPClient::recvFilesFromServer( const JagParseParam *parseParam )
{	
	int rc = 0;
	char *rbuf = NULL; char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1 );
	// first need to send _BEGINFILEUPLOAD_ to server
	recvDirectFromSockAll( rbuf, hdr );
	// if ( rbuf ) { free( rbuf ); rbuf = NULL; }

	if ( rbuf && 0 == strncmp( rbuf, "_BEGINFILEUPLOAD_", 17 ) ) {
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			if ( !_useJPB ) {
				rc += oneFileReceiver( _sock, parseParam->selColVec[i].getfilePath, false );
			} else {
				rc += _jpb->recvFilesFromServer( parseParam->selColVec[i].getfilePath );
			}
		}

		memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1 );
		// last need to send _ENDFILEUPLOAD_ to server
		if ( _debug ) { prt(("c9393 recvDirectFromSockAll ...\n" )); }
		recvDirectFromSockAll( rbuf, hdr );
		if ( _debug ) { prt(("c9393 recvDirectFromSockAll done ...\n" )); }
		if ( rc != parseParam->selColVec.size() || 0 != strncmp( rbuf, "_ENDFILEUPLOAD_", 15 ) ) {
			rc = 0;
		} else {
			rc = 1;
		}
		if ( rbuf ) { free( rbuf ); rbuf = NULL; }
	} else {
		if ( _debug ) { prt(("c9421 here\n" )); }
	}
	return rc;
}

// get all reply()
int JaguarCPPClient::replyAll( bool headerOnly )
{
	AbaxBuffer bfr;
	JaguarCPPClient *cli = this;
	pthread_t tid = THREADID;
	int rc = 1;

	// prt(("\n\n_isparent=%d thrdCliMap=%d\n", _isparent, _thrdCliMap ));
	if ( _isparent && _thrdCliMap ) {
		if ( _thrdCliMap->getValue( tid, bfr ) ) {
			cli = (JaguarCPPClient*) bfr.value();
		} else {
			rc = 0;
			// prt(("c7374 not parent rc= 0 \n"));
		}
	}
	if ( rc ) rc = doreplyAll( headerOnly );
	return rc;
}
int JaguarCPPClient::doreplyAll( bool headerOnly )
{
	while ( reply( headerOnly) ) {}
	return 1;
}

// may be removed later
// server sends number of columns in the result over socket
// client receives a row
// 0: error or reaching the last record
// 1: successful and having more records
int JaguarCPPClient::reply( bool headerOnly ) 
{
	//prt(("c4053 JaguarCPPClient::reply headerOnly=%d\n", headerOnly ));

	if ( _destroyed ) return 0;

	if ( _oneConnect ) {
		//prt(("c5003 _oneConnect doreply() ...\n" ));
		return doreply( headerOnly );
	}

	// prt(("c4562 this=%0x  thrd=%lld reply( %d ) ...\n", this, THREADID, headerOnly ));
	// _replyerrmsg = "";
	int rc = 1, rc2, rc4;
	AbaxBuffer bfr;
	JaguarCPPClient *cli = this;
	JaguarCPPClient *prcli;
	pthread_t tid = THREADID;

	// prt(("\n\n_isparent=%d\n", _isparent ));
	if ( _isparent ) {
		if ( _thrdCliMap->getValue( tid, bfr ) ) {
			cli = (JaguarCPPClient*) bfr.value();
		} else {
			rc = 0;
			// prt(("c7374 not parent rc= 0 \n"));
		}
	}

	prcli = cli->_parentCli;

	if ( rc ) {
		// check if has reply and _end
		// prt(("c5523 cli->_hasReply=%d cli->_end=%d cli->host=[%s]\n", cli->_hasReply, cli->_end, cli->_host.c_str() ));
		if ( ! cli->_hasReply ) {
			_debug && prt(("c4001 ! _hasReply\n" ));
			rc = 0;
		} else if ( JAG_END_NORMAL == cli->_end ) {
			_debug && prt(("c4002  JAG_END_NORMAL\n" ));
			rc = 0;
		} else if ( JAG_END_RECVONE_THEN_DONE == cli->_end ) {
			cli->_end = JAG_END_NORMAL;
			rc = 1;
		} else if ( JAG_END_GOT_DBPAIR == cli->_end || JAG_END_GOT_DBPAIR_AND_ORDERBY == cli->_end ) {
			rc2 = cli->_end;
			cli->freeRow( 1 );
			char *buf = NULL;
			if ( prcli->_dataSelectCount.size() ) { // reply select count(*)
				buf = (char*)jagmalloc( prcli->_dataSelectCount.size()+1 );
				memcpy( buf, prcli->_dataSelectCount.c_str(), prcli->_dataSelectCount.size() );
				buf[prcli->_dataSelectCount.size()] = '\0';
				
				cli->_row->type = 'I';
				if (  cli->_row->data != NULL ) { prt(("c9182 memleak on cli->_row->data\n")); }
				cli->_row->data = buf;
				cli->_row->datalen = prcli->_dataSelectCount.size();	
				prcli->_dataSelectCount = "";
				cli->_end = JAG_END_NORMAL;
				rc = 1;
				rc2 = 1;
			} else if ( prcli->_dataFileHeader.size() ) { // reply FH
				buf = (char*)jagmalloc( prcli->_dataFileHeader.size()+1 );
				memcpy( buf, prcli->_dataFileHeader.c_str(), prcli->_dataFileHeader.size() );
				buf[prcli->_dataFileHeader.size()] = '\0';
				//prt(("c3802 FH hdr buf=[%s]\n", buf )); 
				
				cli->_row->type = 'H';
				if (  cli->_row->data != NULL ) { prt(("c9142 memleak on cli->_row->data\n")); }
				cli->_row->data = buf;
				cli->_row->datalen = prcli->_dataFileHeader.size();

				//prt(("c303999999999999 FH called rowdata=[%s]\n", buf ));

				if ( *(cli->_row->data) == 'm' ) { // meta flag
					cli->_row->isMeta = 1;
					cli->_row->colCount = 0;
					cli->_row->hasSchema = false;
					JagRecord rrec;
					rrec.readSource( (cli->_row->data)+5 );
					char *p2 = rrec.getValue("COLUMNCOUNT");
					if ( p2 ) {
						cli->_row->colCount = atoi(p2);
						if ( p2 ) free( p2 );
						p2 = NULL;
					}
					cli->findAllMetaKeyValueProperty( rrec );
					/***
				} else if ( *(cli->_row->data) == 'n' ) {
					cli->_row->isMeta = 1;
					cli->_row->colCount = 1;
					cli->_row->hasSchema = false;
					JagRecord rrec;
					rrec.readSource( cli->_row->data + 5 );
					cli->findAllNameValueProperty( rrec );
					prt(("c2033 findAllNameValueProperty \n" ));
					***/
				} else {
					cli->_row->hasSchema = true;
					//prt(("c4401 _parseSchema %s ...\n", cli->_row->data ));
					cli->_parseSchema( cli->_row->data );
				}

				prcli->_dataFileHeader = "";
				
				if ( headerOnly ) {
					rc = 1;
					return 1;
				}
			} 
			if ( prcli->_pointQueryString.size() ) { // point query
				buf = (char*)jagmalloc( prcli->_pointQueryString.size()+1 );				
				memcpy( buf, prcli->_pointQueryString.c_str(), prcli->_pointQueryString.size() );
				buf[prcli->_pointQueryString.size()] = '\0';

				cli->_row->type = 'D';
				cli->_row->data = buf;
				cli->_row->datalen = prcli->_pointQueryString.size();
				prcli->_pointQueryString = "";
				cli->_end = JAG_END_NORMAL;
				rc = 1;
				rc2 = 1;
			}

			// prt(("c6730 rc2=%d\n", rc2 ));	
			if ( JAG_END_GOT_DBPAIR == rc2 ) {
				cli->freeRow( 1 );
				if ( prcli->_aggregateData.size() ) { // for aggregate data
					buf = (char*)jagmalloc( prcli->_aggregateData.size()+1 );
					memcpy( buf, prcli->_aggregateData.c_str(), prcli->_aggregateData.size() );
					buf[prcli->_aggregateData.size()] = '\0';
							
					cli->_row->type = 'D';
					cli->_row->data = buf;
					cli->_row->datalen = prcli->_aggregateData.size();
					prcli->_aggregateData = "";
					cli->_end = JAG_END_NORMAL;
				} else {
					JagFixString rdata;							
					if ( prcli->_orderByLimitStart > 1 ) {
						// ignore limitstart-1 lines of data
						for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
							rc = prcli->_jda->readit( rdata );
							if ( !rc ) {
								_debug && prt(("c5033 readit rc=%d\n", rc ));
								break;
							} else {
								// prt(("c6730 rc=%d\n", rc ));
							}
						}
						prcli->_orderByLimitStart = 0;
					}

					if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {
						rc = prcli->_jda->readit( rdata );
						// prt(("c8831 readit rc=%d rdata.size()=%d %s\n", rc, rdata.size(), rdata.c_str() ));
						if ( rc ) {
							buf = (char*)jagmalloc( rdata.size()+1 );
							memcpy( buf, rdata.c_str(), rdata.size() );
							buf[rdata.size()] = '\0';
										
							cli->_row->type = 'D';
							cli->_row->data = buf;
							cli->_row->datalen = rdata.size();
							++prcli->_orderByLimitCnt;
							// prt(("c8023 readit rc=%d\n", rc ));
						} else {
							cli->_end = JAG_END_NORMAL;
							// prt(("c8033 readit rc=%d\n", rc ));
						}								
					} else {
						cli->_end = 1;
						prcli->_jda->clean();
						rc = 0;
					}
				}
			} else if ( JAG_END_GOT_DBPAIR_AND_ORDERBY == rc2 ) {
				cli->freeRow( 1 );
				if ( prcli->_aggregateData.size() ) { // for aggregate data
					buf = (char*)jagmalloc( prcli->_aggregateData.size()+1 );
					memcpy( buf, prcli->_aggregateData.c_str(), prcli->_aggregateData.size() );
					buf[prcli->_aggregateData.size()] = '\0';
							
					cli->_row->type = 'D';
					// prt(("c4301 headerOnly rc set to 1\n"));
					cli->_row->data = buf;
					cli->_row->datalen = prcli->_aggregateData.size();
					prcli->_aggregateData = "";
					cli->_end = 1;
				} else { // actually order by results, read one by one
					if ( prcli->_orderByReadFrom == JAG_ORDERBY_READFROM_JDA ) {
						if ( prcli->_orderByIsAsc ) {
							JagFixString rdata;							
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									rc = prcli->_jda->readit( rdata );
									if ( !rc ) {
										_debug && prt(("c5433 readit rc=%d\n", rc ));
										break;
									} else {
										// prt(("c5463 readit rc=%d\n", rc ));
									}
								}
								prcli->_orderByLimitStart = 0;
							}
							
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {								
								rc = prcli->_jda->readit( rdata );
								if ( rc ) {
									buf = (char*)jagmalloc( rdata.size()+1 );
									memcpy( buf, rdata.c_str(), rdata.size() );
									buf[rdata.size()] = '\0';
										
									cli->_row->type = 'D';
									cli->_row->data = buf;
									cli->_row->datalen = rdata.size();
									++prcli->_orderByLimitCnt;
									// prt(("c3463 readit rc=%d\n", rc ));
								} else {
									cli->_end = 1;
									// prt(("c5435 readit rc=%d\n", rc ));
								}								
							} else {
								cli->_end = JAG_END_NORMAL;
								prcli->_jda->clean();
								rc = 0;
							}
						} else {							
							JagFixString rdata;							
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									rc = prcli->_jda->backreadit( rdata );
									if ( !rc ) {
										// prt(("c1435 readit rc=%d\n", rc ));
										break;
									} else {
										// prt(("c1432 readit rc=%d\n", rc ));
									}
								}
								prcli->_orderByLimitStart = 0;
							}
							
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {								
								rc = prcli->_jda->backreadit( rdata );
								if ( rc ) {
									buf = (char*)jagmalloc( rdata.size()+1 );
									memcpy( buf, rdata.c_str(), rdata.size() );
									buf[rdata.size()] = '\0';
										
									cli->_row->type = 'D';
									cli->_row->data = buf;
									cli->_row->datalen = rdata.size();
									++prcli->_orderByLimitCnt;
									// prt(("c1532 readit rc=%d\n", rc ));
								} else {
									cli->_end = 1;
									// prt(("c1436 readit rc=%d\n", rc ));
								}								
							} else {
								cli->_end = JAG_END_NORMAL;
								prcli->_jda->clean();
								rc = 0; 
							}
						}
					} else if ( prcli->_orderByReadFrom == JAG_ORDERBY_READFROM_MEMARR ) {
						if ( prcli->_orderByIsAsc ) {
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									if ( prcli->_orderByReadPos >= prcli->_orderByMemArr->size() ) { rc = 0; break; }
									prcli->_orderByWritePos = prcli->_orderByMemArr->nextNonNull( prcli->_orderByReadPos );
									if ( prcli->_orderByWritePos >= prcli->_orderByMemArr->size() ) { rc = 0; break; }
									prcli->_orderByReadPos = prcli->_orderByWritePos + 1;
								}
								prcli->_orderByLimitStart = 0;
							}
							
							rc4 = 0;
							JagDBPair rpair;
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {
								if ( prcli->_orderByReadPos >= prcli->_orderByMemArr->size() ) { rc = 0; }
								else {
									prcli->_orderByWritePos = prcli->_orderByMemArr->nextNonNull( prcli->_orderByReadPos );
									if ( prcli->_orderByWritePos >= prcli->_orderByMemArr->size() ) { rc = 0; }
									else {
										rpair = prcli->_orderByMemArr->exactAt(prcli->_orderByWritePos);
										prcli->_orderByReadPos = prcli->_orderByWritePos + 1;
										buf = (char*)jagmalloc( rpair.value.size()+1 );
										memcpy( buf, rpair.value.c_str(), rpair.value.size() );
										buf[rpair.value.size()] = '\0';
											
										cli->_row->type = 'D';
										cli->_row->data = buf;
										cli->_row->datalen = rpair.value.size();
										++prcli->_orderByLimitCnt;
										rc4 = 1;
									}
								}
							}
							
							if ( !rc || prcli->_orderByLimitCnt >= prcli->_orderByLimit ) {
								if ( prcli->_orderByMemArr ) {
									delete prcli->_orderByMemArr;
									prcli->_orderByMemArr = NULL;
								}
								cli->_end = JAG_END_NORMAL;
								if ( 1 == rc4 ) rc = 1;
								else rc = 0;
							}
						} else {							
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									if ( prcli->_orderByReadPos < 0 ) { rc = 0; break; }
									prcli->_orderByWritePos = prcli->_orderByMemArr->prevNonNull( prcli->_orderByReadPos );
									if ( prcli->_orderByWritePos < 0 ) { rc = 0; break; }
									prcli->_orderByReadPos = prcli->_orderByWritePos - 1;
								}
								prcli->_orderByLimitStart = 0;
							}
							
							rc4 = 0;
							JagDBPair rpair;
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {
								if ( prcli->_orderByReadPos < 0 ) { rc = 0; }
								else {
									prcli->_orderByWritePos = prcli->_orderByMemArr->prevNonNull( prcli->_orderByReadPos );
									if ( prcli->_orderByWritePos < 0 ) { rc = 0; }
									else {
										rpair = prcli->_orderByMemArr->exactAt(prcli->_orderByWritePos);
										prcli->_orderByReadPos = prcli->_orderByWritePos - 1;
										buf = (char*)jagmalloc( rpair.value.size()+1 );
										memcpy( buf, rpair.value.c_str(), rpair.value.size() );
										buf[rpair.value.size()] = '\0';
									
										cli->_row->type = 'D';
										cli->_row->data = buf;
										cli->_row->datalen = rpair.value.size();
										++prcli->_orderByLimitCnt;
										rc4 = 1;
									}
								}
							}
							
							if ( !rc || prcli->_orderByLimitCnt >= prcli->_orderByLimit ) {
								if ( prcli->_orderByMemArr ) {
									delete prcli->_orderByMemArr;
									prcli->_orderByMemArr = NULL;
								}
								cli->_end = JAG_END_NORMAL;
								if ( 1 == rc4 ) rc = 1;
								else rc = 0;
							}
						}						
					} else if ( prcli->_orderByReadFrom == JAG_ORDERBY_READFROM_DISKARR ) {
						char *readbuf = (char*)jagmalloc( prcli->_orderByKEYLEN+prcli->_orderByVALLEN+1 );
						memset(readbuf, 0, prcli->_orderByKEYLEN+prcli->_orderByVALLEN+1);
						if ( prcli->_orderByIsAsc ) {
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									rc = prcli->_orderByBuffReader->getNext(readbuf);
									if ( !rc ) { break; }
								}
								prcli->_orderByLimitStart = 0;
							}
							
							rc4 = 0;
							JagDBPair rpair;
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {
								rc = prcli->_orderByBuffReader->getNext(readbuf);
								if ( rc ) {
									buf = (char*)jagmalloc( prcli->_orderByVALLEN+1 );
									memcpy( buf, readbuf+prcli->_orderByKEYLEN, prcli->_orderByVALLEN );
									buf[prcli->_orderByVALLEN] = '\0';
									
									cli->_row->type = 'D';
									cli->_row->data = buf;
									cli->_row->datalen = rpair.value.size();
									++prcli->_orderByLimitCnt;
									rc4 = 1;
								}
							}
							
							if ( !rc || prcli->_orderByLimitCnt >= prcli->_orderByLimit ) {
								if ( prcli->_orderByRecord ) {
									delete prcli->_orderByRecord;
									prcli->_orderByRecord = NULL;
								}
								if ( prcli->_orderByDiskArr ) {
									prcli->_orderByDiskArr->drop();
									delete prcli->_orderByDiskArr;
									prcli->_orderByDiskArr = NULL;
								}
								if ( prcli->_orderByBuffReader ) {
									delete prcli->_orderByBuffReader;
									prcli->_orderByBuffReader = NULL;
								}								
								cli->_end = 1;
								if ( 1 == rc4 ) rc = 1;
								else rc = 0;
							}
						} else {							
							if ( prcli->_orderByLimitStart > 1 ) {
								// ignore limitstart-1 lines of data
								for ( abaxint i = 1; i < prcli->_orderByLimitStart; ++i ) {
									rc = prcli->_orderByBuffBackReader->getNext(readbuf);
									if ( !rc ) { break; }
								}
								prcli->_orderByLimitStart = 0;
							}
							rc4 = 0;
							JagDBPair rpair;
							if ( rc && prcli->_orderByLimitCnt < prcli->_orderByLimit ) {
								rc = prcli->_orderByBuffBackReader->getNext(readbuf);
								if ( rc ) { 
									buf = (char*)jagmalloc( prcli->_orderByVALLEN+1 );
									memcpy( buf, readbuf+prcli->_orderByKEYLEN, prcli->_orderByVALLEN );
									buf[prcli->_orderByVALLEN] = '\0';
									
									cli->_row->type = 'D';
									cli->_row->data = buf;
									cli->_row->datalen = rpair.value.size();
									++prcli->_orderByLimitCnt;
									rc4 = 1;
								}
							}
							
							if ( !rc || prcli->_orderByLimitCnt >= prcli->_orderByLimit ) {
								if ( prcli->_orderByRecord ) {
									delete prcli->_orderByRecord;
									prcli->_orderByRecord = NULL;
								}
								if ( prcli->_orderByDiskArr ) {
									prcli->_orderByDiskArr->drop();
									delete prcli->_orderByDiskArr;
									prcli->_orderByDiskArr = NULL;
								}
								if ( prcli->_orderByBuffBackReader ) {
									delete prcli->_orderByBuffBackReader;
									prcli->_orderByBuffBackReader = NULL;
								}								
								cli->_end = JAG_END_NORMAL;
								if ( 1 == rc4 ) rc = 1;
								else rc = 0;
							}
						}
						if ( readbuf ) {
							jagfree( readbuf );
						}
					}
				}				
			}
			if ( rc == 0 && cli && prcli && prcli->_selectTruncateMsg.size() ) {
				cli->_row->type = 'E';
				cli->_replyerrmsg = prcli->_selectTruncateMsg;
				prcli->_selectTruncateMsg = "";
				rc = 1;
			} 
		} else {
			rc = cli->doreply( headerOnly );
			//prt(("c0778 cli->doreply( headerOnly=%d ) rc=%d\n", headerOnly, rc ));
		}
	}

	if ( 0 == rc ) {
		if ( tid == cli->_qMutexThreadID ) {
			cli->_qMutexThreadID = 0;
			JAG_BLURT_UN jaguar_mutex_unlock ( &cli->_queryMutex );
			// prt(( "c9999 unlock cli=%0x, _parentCli=%0x, this=%0x, thrd=%0x, mutex=%0x, _host=[%s]\n", 
			//       cli, _parentCli, this, THREADID, &cli->_queryMutex, cli->_host.c_str()));
			// prt(("c2138 cli=%0x host=[%s] jaguar_mutex_unlock _queryMutex done\n", cli, cli->_host.c_str() ));
		}

		if ( _isparent ) {
			_thrdCliMap->removeKey( tid );
			// prt(("c3182 this=%0x _thrdCliMap->removeKey(%lld)\n", this, tid ));
		}

		if ( 50 == cli->_tcode ) {
			if ( prcli && prcli->_fromShell ) {
				prt(("%s\n", cli->_replyerrmsg.c_str()));
			}
			prt(("c7902 error code 50, exit\n"));
			close();
			exit(1);
		} 
		else if ( 120 == cli->_tcode ) {
			// prt(("c3381 120 tcode err=[%s]\n", cli->_replyerrmsg.c_str() ));
		}
	}

	_debug && prt(("s2030 prcli=%0x headerOnly=%d\n", prcli, headerOnly ));
	if ( prcli && ! headerOnly ) {
		if ( 0 == rc ) {
			jaguar_mutex_lock ( &_lineFileMutex );
			if ( prcli && prcli->_lineFile && prcli->_lineFile->hasData() ) {
				prt(("c3939 _lineFile->hasData() true\n" ));
				rc = 1;
			}
			jaguar_mutex_unlock ( &_lineFileMutex );
		}
	}

	_debug && prt(("c4082 end reply rc=%d\n", rc ));
	return rc;
}

void JaguarCPPClient::cleanForNewQuery()
{
	_forExport = 0;
	_exportObj = "";
	_lastHasGroupBy = _lastHasOrderBy = _queryCode = 0;
	_jda->clean();
	_dataSortArr->clean();
}

void JaguarCPPClient::formReplyDataFromTempFile( const char *str, abaxint len, char type )
{
	char *buf = (char*)jagmalloc( len+1 );
	memcpy( buf, str, len );
	buf[len] = '\0';
	_row->type = type;
	_row->data = buf;
	_row->datalen = len;	
}

// client receives a _row
// -1: error on socket
int JaguarCPPClient::doreply( bool headerOnly )
{
	_debug && prt(("c4562 doreply this=%0x thrd=%lld hdronly=%d ...\n", this, THREADID, headerOnly ));
	// prt(("c8832 this=%0x _row=%0x _row->data=%0x\n", this, _row, _row->data ));
	// recv new hdr and data
	abaxint len = 0;
	char h1, h2;
	char *buf = NULL;
	abaxint heartbeat = 0;

	while ( true ) {
		buf = NULL;
		//if ( buf ) { jagfree( buf ); }
    	if ( _useJPB ) {
    		// use replicate object to send data, receive from replicate object
			_debug && prt(( "c9032 this=%0x parent=%0x _jpb=%0x _jpb->recvReply ...\n", this, _parentCli, _jpb ));
    		len = _jpb->recvReply( recvhdr, buf );
			_debug && prt(( "c9032 _jpb->recvReply done len=%d buf=[%s]\n", len, buf ));
    		if ( len < 0 ) {
    			// error or no more data
    			// prt(("ERROR, recv all timeout host0=[%s]\n", _host.c_str()));
    			if ( buf ) jagfree( buf );
    			_lastQueryConnError = 1;
    			return 0;
    		}
    	} else {
    		memset( recvhdr, 0, JAG_SOCK_MSG_HDR_LEN+1 );
			//if ( _debug ) { prt(( "c9132 recvData recvhdr=[%s] ...\n", recvhdr )); }
    		// len = recvData( _sock, recvhdr, buf );
			_debug && prt(( "c9034 recvDirectFromSock ...\n" ));
			len = recvDirectFromSock( _sock, buf, recvhdr );
			_debug && prt(( "c9132 recvDirectFromSock done len=%d rcvhdr=[%s] buf=[%s]\n", len, recvhdr, buf ));
    		if ( len < 0 ) {
    			// prt(("ERROR, recv all timeout host0=[%s]\n", _host.c_str()));
    			if ( buf ) jagfree( buf );
    			_sock = INVALID_SOCKET;
    			setConnectionBrokenTime();
				_debug && prt(("c3447 _lastQueryConnError=1\n" ));
    			_lastQueryConnError = 1;
    			return 0;
    		} else if ( len == 0 ) {
    			if ( buf ) jagfree( buf );
    			// return doreply( headerOnly );
				continue;
    		}
    		
        	if ( recvhdr[JAG_SOCK_MSG_HDR_LEN-4] == 'Z' ) { 
        		Jstr compressed( buf, len );
        		Jstr unCompressed;
        		JagFastCompress::uncompress( compressed, unCompressed );
        		if ( buf ) jagfree( buf );
        		// since strdup cannot process \0 byte, we use jagmalloc instead
        		buf = (char*)jagmalloc( unCompressed.size()+1 );
        		memcpy( buf, unCompressed.c_str(), unCompressed.size() );
        		buf[unCompressed.size()] = '\0';
        		len = unCompressed.size();
        	}
       	}
    
    	// free row data only
		// _debug && prt(("c8832 this=%0x _row=%0x _row->data=%0x freeRow()...\n", this, _row, _row->data ));
   		freeRow( 1 );
    
		#if 1
		if ( 'Y' != *buf && _debug ) {
    		prt(("c4930 recv sock=%d, thrd=%lld hdr=[%s] buf=[%s] type=[%c]\n", _sock, THREADID, recvhdr, buf, _row->type ));
		}
		#endif

    	h1 = recvhdr[JAG_SOCK_MSG_HDR_LEN-3];
    	h2 = recvhdr[JAG_SOCK_MSG_HDR_LEN-2];
    	char *recvhdr2 = (char*)jagmalloc(JAG_SOCK_MSG_HDR_LEN+1);
    	memcpy( recvhdr2, recvhdr, JAG_SOCK_MSG_HDR_LEN ); recvhdr2[JAG_SOCK_MSG_HDR_LEN] = '\0';
    	_row->hdr = recvhdr2;
    	// if _END_
		// prt(("c2135 buf=[%s] h1=[%c] h2=[%c]\n", buf, h1, h2 ));

    	if ( buf[0] == '_' && buf[1] == 'E' && buf[2] == 'N' && buf[3] == 'D' && buf[4] == '_' ) {
			_replyerrmsg = "";
    		_row->type = ' ';
    		_tcode = _getFieldInt( buf, 'T' );
    		if ( ('E' == h1 && 'D' == h2) ) {
    			// "ED" type is really end
    		 	_end = 1;
    			if ( _useJPB ) {
    				if ( 777 == _jpb->_tcode ) {
    					_tcode = _jpb->_tcode;
    				}
    			}
				if ( buf ) jagfree( buf );
				// prt(("c1220 got _END_ _end=1 return 0\n" ));
    		 	return 0;
    		} else if ( h1 == 'E' && h2 == 'R' ) {
    			// "ER" got error when _END_, get _row->data for this call and end next time
    			// are there errors, should check for ER type
    			char *perr = _getField( buf, 'E' );
    			if ( perr ) {
    				_row->type = 'E';
    				_replyerrmsg = perr;
    				jagfree( perr );
    				// prt(("s3931 errmsg=[%s]\n", _replyerrmsg.c_str() ));
    			} 
    		 	_end = 1;
    		}
    
    		if ( _useJPB ) {
    			if ( 777 == _jpb->_tcode ) {
    				_tcode = _jpb->_tcode;
    			}
    		}
    		// prt(("c5766  _row->type=[%c]\n",  _row->type ));
			if ( buf ) jagfree( buf );
    		return 1;
    	}  else {
			_replyerrmsg = "";
		}
	
		if (  'S' == h1 && 'C' == h2 ) {
			// "SC" schema update for client
			// prt(("c3001 THRD=%lld received schema=[%s] headerOnly=%d _host=[%s]\n", THREADID, buf, headerOnly, _host.c_str()));
			_row->type = 'C';
			_row->data = buf;
			_row->datalen = len;
			_parentCli->_mapLock->writeLock( -1 );
			_parentCli->_schemaUpdateString = _row->data;
			_parentCli->_mapLock->writeUnlock( -1 );
			// return doreply( false );
			headerOnly = false;
			/***
		} else if (  'D' == h1 && 'F' == h2 ) {
			// "DF" default value schema update for client
			// prt(("c3001 THRD=%lld received schema=[%s] headerOnly=%d _host=[%s]\n", THREADID, buf, headerOnly, _host.c_str()));
			_row->type = 'C';
			_row->data = buf;
			_row->datalen = len;
			_parentCli->_mapLock->writeLock( -1 );
			_parentCli->_defvalUpdateString = _row->data;
			_parentCli->_mapLock->writeUnlock( -1 );
			// return doreply( false );
			headerOnly = false;
			***/
		} else if ( 'H' == h1 && 'L' == h2 ) {
			// "HL" host list to be updated for client if exclusive admin does addCluster command
			// prt(("c3002 THRD=%lld received hostlist=[%s] headerOnly=%d _host=[%s]\n", THREADID, buf, headerOnly, _host.c_str()));
			_row->type = 'C';
			_row->data = buf;
			_row->datalen = len;
			_parentCli->_mapLock->writeLock( -1 );
			_parentCli->_hostUpdateString = Jstr(_row->data);
			_parentCli->_mapLock->writeUnlock( -1 );
			// return doreply( false );
			headerOnly = false;
		} else if ( 'F' == h1 && 'H' == h2 ) {
			// check for hdr record  "FH" field header record
			prt(("c2339 FH headerOnly=%d\n", headerOnly ));
			_row->type = 'H';
			_row->data = buf;
			_row->datalen = len;
			// prt(("c4782 buf=[%s]\n", buf ));
			// setPossibleMetaStr(); append later
			// may be removed later
			if ( *(_row->data) == 'm' ) { // meta flag
				_row->isMeta = 1;
				_row->colCount = 0;
				_row->hasSchema = false;
				JagRecord rrec;
				rrec.readSource( (_row->data)+5 );
				char *p2 = rrec.getValue("COLUMNCOUNT");
				if ( p2 ) {
					_row->colCount = atoi(p2);
					if ( p2 ) jagfree( p2 );
				}
				findAllMetaKeyValueProperty( rrec );
			} else if ( *(_row->data) == 'n' ) {
				_row->isMeta = 1;
				_row->colCount = 1;
				_row->hasSchema = false;
				JagRecord rrec;
				rrec.readSource( _row->data + 5 );
				findAllNameValueProperty( rrec );
				prt(("c2812 n findAllNameValueProperty \n" ));
			}

			if ( ! headerOnly ) {
				// return doreply( false );
				// continue receive, with false flag
			} else {
				break; // stop receive
			}
		} else if ( 'O' == h1 && 'K' == h2 ) {
			// "OK"
			_row->type = 'I';
			_row->data = buf;
			_row->datalen = len;
			break;
		} else if ( 'E' == h1 && 'R' == h2 ) {
			// "ER"
			_row->type = 'E';
			_replyerrmsg = buf;
			break;
		} else if ( 'K' == h1 && 'V' == h2 ) {
			// "KV"  old was "JV"
			_row->type = 'V';
			_row->data = buf;
			//prt(("c3130 got KV data=[%s]\n", buf ));
			break;
		} else if ( 'J' == h1 && 'S' == h2 ) {
			// "JS"
			_row->type = 'J';
			_debug && prt(("c3430 this=%0x _parentCli=%0x buf=[%s]\n", this, _parentCli, buf ));
			_debug && prt(("c3430 got JS data=[%s] parentCli->appendJSData()...\n", buf ));
			Jstr jsData = convertToJson(buf);
			_debug && prt(("c7203 jsData=[%s] to be appended to parent ...\n", jsData.c_str() ));
			_parentCli->appendJSData( jsData );

			break;
		} else if ( 'X' == h1 && '1' == h2 ) {
			// "X1" only one line of data, not _END_ after
			_row->type = '1';
			_row->data = buf;
			_row->datalen = len;
			_end = 1;
			break;
		} else if ( 'S' == h1 && 'S' == h2 ) {
			// "SS" special signal, as "OK" or "NG" for create/drop table/index/users etc. commands
			_row->type = 'D';
			_row->data = buf;
			_row->datalen = len;
			break;
		} else if ( 'H' == h1 && 'B' == h2 ) {
			// "HB" heartbeatt signal from server, to make sure server is still up and alive; prevent client timeout
			// prt(("c9999 HB recved, continue loop(%d)...\n", headerOnly ));
			if ( _debug && ( ++heartbeat % 100 ) == 0 ) {
				prt(("heartbeat %lld\n", heartbeat ));
			}
			_debug && prt(("c6380 recvhdr=[%s]\n", recvhdr ));
			if ( buf ) jagfree( buf );
		} else {
			// currently, "XX" and "DS" will be here
			_row->type = 'D';
			_row->data = buf;
			_row->datalen = len;
			break;
		}
	} // end of while(true) loop
	
	if ( _useJPB ) {
		if ( 777 == _jpb->_tcode ) {
			_tcode = _jpb->_tcode;
		}
	}
	// prt(("c5766  _row->type=[%c]\n",  _row->type ));
	return 1;
}

// static
void *JaguarCPPClient::batchStatic( void * ptr )
{
	// for batch insert, if any send or reply is failure, retry last batch
	// JagClock clock; clock.start();
	CliPass *pass = (CliPass*)ptr;
	int errorrc = 1;
	while ( errorrc > 0 ) {
		pass->cli->queryDirect( pass->cmd.c_str(), true, true, false, true );
		errorrc = pass->cli->_lastQueryConnError; // 1 represent error, and 0 is ok
		// clock.stop();
		// prt(("c4000 batch send to %s takes %d millisecs\n", pass->cli->_host.c_str(), clock.elapsed()));
		// clock.start();
		if ( !errorrc ) {
			while ( pass->cli->reply() ) {}
			errorrc = pass->cli->_lastQueryConnError; // 1 represent error, and 0 is ok
			// clock.stop();
			// prt(("c4001 batch reply from %s takes %d millisecs\n", pass->cli->_host.c_str(), clock.elapsed()));
		}
		if ( !errorrc ) break;
		prt(("E1032 last insert to host [%s] and backup hosts failed, retry in 10 seconds ...\n", pass->cli->_host.c_str()));
		jagsleep( 10, JAG_SEC );
	}	
	jagdelete( pass );
}

// client initializes the row
int JaguarCPPClient::initRow()
{
	// prt(("c4818 initRow() called\n"));
	_row->hasSchema = false;
	_row->data = NULL;
	_row->hdr = NULL;
	_row->type = ' ';
	_row->numKeyVals = 0; 
	_row->isMeta = 0; 
	_row->colCount = 0; 
	_end = 0; 
	if ( _qtype ) {
		jag_hash_init ( & (_row->colHash), 16 ); 
	}
	return 1;
}

// row hasError?
// 1: yes  0: no
int JaguarCPPClient::hasError( )
{
	if ( _oneConnect ) {
		return doHasError();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doHasError( );
		} else {
			if ( _queryerrmsg.size() ) return 1;
			if ( _replyerrmsg.size() ) return 1;
			else return 0;
		}
	} else {
		return doHasError( );
	}
}

int JaguarCPPClient::doHasError( )
{
	// dbg(("c4088 doHasError _row=%0x\n", _row ));
	// if ( ! _row ) return 0;
	// dbg(("c4088 doHasError _row->type=[%c]\n", _row->type ));
	if ( _queryerrmsg.size() > 0 || _replyerrmsg.size() > 0 ) return 1;
	return 0;
}

const char *JaguarCPPClient::status( )
{
	return _queryStatMsg.c_str();
}

// get error string from row
const char *JaguarCPPClient::error( )
{
	if ( _oneConnect ) {
		return doError();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			// prt(("c2258\n"));
			return tcli->doError( );
		} else {
			// prt(("c2218 _queryerrmsg.size()=%d _replyerrmsg=[%s]\n", _queryerrmsg.size(), _replyerrmsg.c_str() ));
			if ( _queryerrmsg.size() > 0 ) return _queryerrmsg.c_str();
			else if ( _replyerrmsg.size()>0 ) return _replyerrmsg.c_str();
			else return NULL;
		}
	} else {
		// prt(("c2288\n"));
		return doError( );
	}
}

// get error message
const char *JaguarCPPClient::doError( )
{
	// prt(("c3329 doError\n"));
	// prt(("c3349 [%s]\n", _replyerrmsg.c_str() ));
	if ( _queryerrmsg.size() > 0 ) return _queryerrmsg.c_str();
	else return _replyerrmsg.c_str();
}

int JaguarCPPClient::errorCode( )
{
	if ( _oneConnect ) {
		return _tcode;
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->_tcode;
		} else {
			return _tcode;
		}
	} else {
		return _tcode;
	}
}

char *JaguarCPPClient::getValue( const char *longName )
{
	char *p = _getValue( longName );
	if ( p ) {
		return p;
	} else {
		return getAllByName( longName );;
	}
}

// open API
// returns a pointer to char string as value for name
// The buffer needs to be freed after use
char *JaguarCPPClient::_getValue( const char *longName )
{
	if ( _destroyed ) return NULL;

	if ( _oneConnect ) {
		return doGetValue( longName );
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetValue( longName );
		} else {
			return NULL;
		}
	} else {
		return doGetValue( longName );
	}
}

// returns a pointer to char string as value for name
// The buffer needs to be freed after use if it returns not NULL
char *JaguarCPPClient::doGetValue( const char *longName )
{
	int keyLength, i, rrlen; 
	char  save;

	// prt(("c8380 JaguarCPPClient::getValue longname=[%s] _row->type=[%c]\n", longName, _row->type ));
	if ( _row->type == 'V' ) {
		JagRecord rayrec;
		rayrec.readSource( _row->data );
		rayrec.getAllNameValues( (char**)_row->g_names,  (char**)_row->g_values, &rrlen );
		char *p= NULL;
		for ( i = 0; i < rrlen; ++i ) {
			if ( 0 == strcmp( _row->g_names[i], longName ) ) {
				p = strdup( _row->g_values[i] );
				break;
			}
		}
		for ( i = 0; i < rrlen; ++i ) {
			if ( _row->g_names[i] ) free( _row->g_names[i] );
			if ( _row->g_values[i] ) free( _row->g_values[i] );
			_row->g_names[i] = _row->g_values[i] = NULL;
		}

		if ( p ) return p;
		return NULL;
	}

	if ( _row->type != 'D' && _row->type != '1' ) {
		return NULL;
	}

	// longName can be:  uid  
	// longName can be:  tab1.uid 
	// longName can be:  mydb.tab1.uid 

	// longname must be mydb1.tab1.col2  or tab2.col3
	char fullname[128];
	memset( fullname, 0, 128 ); 
	int  items = strchrnum( longName, '.' );
	if ( items == 1 ) {
		sprintf( fullname, "%s.%s", _dbname.c_str(), longName ); // append db before longName
	} else {
		strcpy( fullname, longName ); // longName itself
	}

	// get offset and length
	Jstr strValue, outstr;
	Jstr ktype;
	int rc = _getKeyOrValue( fullname, strValue, ktype );
	if ( ! rc ) {
		// prt(("c3819 return NULL; fullname=[%s] rc=%d\n", fullname, rc ));
		return NULL; 
	}
	if ( ktype == JAG_C_COL_TYPE_DATETIME || ktype == JAG_C_COL_TYPE_TIMESTAMP ) {
		JagTime::convertDateTimeToLocalStr( strValue, outstr );
		return strdup( outstr.c_str() );
	} else if ( ktype == JAG_C_COL_TYPE_DATETIMENANO ) {
		JagTime::convertDateTimeToLocalStr( strValue, outstr, true );
		return strdup( outstr.c_str() );
	} else if ( ktype == JAG_C_COL_TYPE_TIME ) {
		JagTime::convertTimeToStr( strValue, outstr, 1 );
		return strdup( outstr.c_str() );
	} else if ( ktype == JAG_C_COL_TYPE_TIMENANO ) {
		JagTime::convertTimeToStr( strValue, outstr, 2 );
		return strdup( outstr.c_str() );
	} else if ( ktype == JAG_C_COL_TYPE_DATE ) {
		JagTime::convertDateToStr( strValue, outstr );
		return strdup( outstr.c_str() );
	} else if ( isInteger(ktype) ) {
		if ( *(strValue.c_str()) == '\0' ) return strdup( strValue.c_str() );
		return strdup( longToStr(jagatoll(strValue.c_str())).c_str() );
	} else if ( ktype == JAG_C_COL_TYPE_FLOAT || ktype == JAG_C_COL_TYPE_DOUBLE ) {
		if ( *(strValue.c_str()) == '\0' ) return strdup( strValue.c_str() );
		return strdup( longDoubleToStr(jagstrtold(strValue.c_str(), NULL)).c_str() );
	} else {
		return strdup( strValue.c_str() );
	}
}

// free all memory of row fields, if true, free _row->data only
int JaguarCPPClient::freeRow( int type )
{
	if ( _oneConnect ) {
		return doFreeRow( type );
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if (  _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doFreeRow( type );
		} else {
			return 0;
		}
	} else {
		return doFreeRow( type );
	}
}

// cleantype: 0: delete data and colhhash
// cleantype: 1: delete data 
// cleantype: 2: delete colhhash
int JaguarCPPClient::doFreeRow( int cleantype )
{
	#if 0
	prt(("c8383 thread=%lld JaguarCPPClient::freeRow(this=%0x) row=%0x ctype=%d\n", THREADID, this, _row, cleantype ));
	#endif

	if ( 0 == cleantype ) {
		if ( _row->data ) {
			// printf("c8811 tid=%lld free _row->data this=%0x  _host=[%s]\n", THREADID, this, _host.c_str() ); fflush( stdout );
			jagfree ( _row->data );
		}
		if ( _row->hdr ) {
			free ( _row->hdr );
			_row->hdr = NULL;
		}

		if (  _row->colHash.entries > 0 ) {
			jag_hash_destroy( & _row->colHash );
		}
	} else if ( 1 == cleantype ) {
		if ( _row->data ) {
			// printf("c8814 tid=%lld free _row->data this=%0x  _host=[%s]\n", THREADID, this, _host.c_str() ); fflush( stdout );
			jagfree ( _row->data );
		}
		if ( _row->hdr ) {
			free ( _row->hdr );
			_row->hdr = NULL;
		}
	} else if ( 2 == cleantype ) {
		if (  _row->colHash.entries > 0 ) {
			jag_hash_destroy( & _row->colHash );
		}
	}

	return 1;
}

int JaguarCPPClient::printRow()
{
	_debug && prt(("c6010 printRow() ...\n" ));
	Jstr  rowStr;
	if ( _oneConnect ) {
		_debug && prt(("c6011 doPrintRow() ...\n" ));
		return doPrintRow( false, rowStr);
	}

	if ( _isparent ) {
		AbaxBuffer bfr;
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			_debug && prt(("c6012 tcli->doPrintRow() ...\n" ));
			return tcli->doPrintRow( false, rowStr );
		} else {
			return 0;
		}
	} else {
		_debug && prt(("c6013 doPrintRow() ...\n" ));
		return doPrintRow( false, rowStr );
	}
}

// caller must free memory
char *JaguarCPPClient::getAllByName( const char *name )
{
	char *all = getAll();
	if ( ! all ) return NULL;
	Jstr names( name );

	// all: "db.tab.col=....\ndb.tab.col=....\ndb.tab.col=...."
	char *p = all;
	char *q;
	while ( *p != '\0' ) {
		q = p;
		while ( *q != '=' && *q != '\0' ) ++q;
		if ( *q == '\0' ) { break; }

		Jstr s(p, q-p);
		JagStrSplit sp(s, '.' );
		if ( sp.length() < 3 ) { break; }

		++q;
		p = q;  // jsondata.....|
		while ( *q != '\n' && *q != '\0' ) ++q;

		if ( names == sp[2] || names == s || names == sp[1] + "." + sp[2] ) {
			char *res = strndup(p, q-p);
			free( all );
			return res;
		} 

		if ( *q == '\0' ) break;
		p = q+1;
	}

	// not found
	free( all );
	return NULL;
}

// caller must free the memory
// 1--N
char *JaguarCPPClient::getAllByIndex( int nth )
{
	char *all = getAll();
	if ( ! all ) return NULL;
	// all: "db.tab.col=....#db.tab.col=....#db.tab.col=...."
	char *p = all;
	char *q;
	int cnt = 1;
	while ( *p != '\0' ) {
		q = p;
		while ( *q != '=' && *q != '\0' ) ++q;
		if ( *q == '\0' ) { break; }

		Jstr s(p, q-p);
		JagStrSplit sp(s, '.' );
		if ( sp.length() < 3 ) { break; }

		++q;
		p = q;  // jsondata.....|
		while ( *q != '\n' && *q != '\0' ) ++q;

		if ( cnt == nth ) {
			char *res = strndup(p, q-p);
			free( all );
			return res;
		} 

		if ( *q == '\0' ) break;
		p = q+1;
		++cnt;
	}

	// not found
	free( all );
	return NULL;
}

// strndup string, must be freed by caller
char *JaguarCPPClient::getAll()
{
	Jstr  rowStr;
	if ( _oneConnect ) {
		doPrintAll( true, rowStr);
		return strndup(rowStr.c_str(), rowStr.size() );
	}
	_parentCli->doPrintAll( true, rowStr );
	return strndup(rowStr.c_str(), rowStr.size() );
}


bool JaguarCPPClient::printAll()
{
	_debug && prt(("c6010 printAll() ...\n" ));
	Jstr  rowStr;
	if ( _oneConnect ) {
		_debug && prt(("c6011 doPrintAll() ...\n" ));
		return doPrintAll( false, rowStr);
	}
	_debug && prt(("c2088 _parentCli->doPrintAll...\n" ));
	return _parentCli->doPrintAll( false, rowStr );
}

// get row data into rowStr
char *JaguarCPPClient::getRow()
{
	Jstr  rowStr;
	if ( _oneConnect ) {
		doPrintRow( true, rowStr);
		return strdup( rowStr.c_str() );
	}

	if ( _isparent ) {
		AbaxBuffer bfr;
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			tcli->doPrintRow( true, rowStr );
			return strdup( rowStr.c_str() );
		} else {
			return NULL;
		}
	} else {
		doPrintRow( true, rowStr );
		return strdup( rowStr.c_str() );
	}
}

int JaguarCPPClient::doPrintRow( bool retRow, Jstr &rowStr )
{
	if ( retRow ) {
		return _printRow( stdout, 0, true, rowStr, _parentCli->_forExport, _parentCli->_exportObj.c_str() );
	} else if ( _parentCli->_outf ) {
		return _printRow( _parentCli->_outf, 0, false, rowStr, _parentCli->_forExport, _parentCli->_exportObj.c_str() );
	} else {
		return _printRow( stdout, 0, false, rowStr, _parentCli->_forExport, _parentCli->_exportObj.c_str() );
	}
}


int JaguarCPPClient::doPrintAll( bool retRow, Jstr &rowStr )
{
	//prt(("c4081 this=%0x doPrintRow retRow=%d _row->jsData.size=%d \n", this, retRow, _row->jsData.size()  ));
	jaguar_mutex_lock ( &_lineFileMutex );
	if ( _parentCli && _parentCli->_lineFile ) {
		if ( ! _parentCli->_lineFile->_hasStartedRead ) { _parentCli->_lineFile->startRead(); }
	}

	Jstr jsData;
	int rc = 0;
	if ( _row && _parentCli && _parentCli->_lineFile && _parentCli->_lineFile->getLine( jsData  ) ) {
		_debug && prt(("c4720 this=%0x jsData=[%s] _parentCli=%0x _parentCli->_lineFile=%0x\n", 
						this, jsData.c_str(), _parentCli, _parentCli->_lineFile ));
		if ( retRow ) {
			rowStr =  jsData;
		}  else if ( _parentCli->_outf ) {
			fprintf( _parentCli->_outf, "%s\n", jsData.c_str() );
		} else {
			printf( "%s\n", jsData.c_str() );
		}
		//prt(("c1203 this=%0x set  _row->jsData = empty\n", this ));
		//prt(("c4082 this=%0x doPrintRow retRow=%d _row->jsData.size=%d \n", this, retRow, _row->jsData.size()  ));
		rc = 1;
	}

	jaguar_mutex_unlock ( &_lineFileMutex );
	return rc;
}

void JaguarCPPClient::flush()
{
	if ( _oneConnect ) {
		doFlush();
		return;
	}

	if ( _isparent ) {
		AbaxBuffer bfr;
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			tcli->doFlush();
			return;
		} else {
			return;
		}
	} else {
		doFlush();
		return;
	}
}

void JaguarCPPClient::doFlush()
{
	if ( _parentCli->_outf ) {
		fflush( _parentCli->_outf );
	} else {
		fflush( stdout );
	}
}

// client receives a row with all columns
// int JaguarCPPClient::_printRow( FILE *outf, char **retstr, int nth, int forExport, const char *dbobj )
int JaguarCPPClient::_printRow( FILE *outf, int nth, bool retRow, Jstr &rowStr, 
								int forExport, const char *dbobj )
{
	char *p, v;
	int  vlen, i, rc, len;
	bool foundnth = 0;
	//prt(("c3339 _printRow nth=%d retRow=%d forExport=%d dbobj=[%s]\n", nth, retRow, forExport, dbobj ));
	if ( doHasError() ) {
		if ( retRow ) {
			rowStr = doError();
		} else {
			if ( outf ) {
				fprintf( outf, "%s\n", doError() );
			}
		}
		return 1;
	}

	if ( _row->type == ' ' || !_row->data ) {
		return 0;
	}

	if ( _row->type == 'I' ) {
		if ( retRow ) {
			rowStr = _row->data;
		} else {
			if ( outf ) {
				fprintf( outf, "%s\n", _row->data );
			}
		}
		return 1;
	}
	
	if ( ! _row->hasSchema ) {
		_debug && prt(("\n no schema\n" ));
		if ( _row->type == 'D' || _row->type == '1' ) {
			if ( retRow ) {
				rowStr = _row->data;
			} else {
				if ( outf ) {
					fprintf( outf, "%s\n", _row->data );
				}
			}
		} else if ( _row->type == 'V' ) {
			len = 0;
			JagRecord rec;
			rec.readSource( _row->data ); 
			rc = rec.getAllNameValues(  _row->g_names, _row->g_values, &len );
			foundnth =  0;
			for ( i = 0; i < len; ++i ) {
				if ( retRow ) {
					 rowStr += Jstr(_row->g_names[i]) + "=[" + Jstr( _row->g_values[i] ) + "]    ";
				} else if ( outf ) {
					fprintf( outf, "%s=[%s]    ", _row->g_names[i], _row->g_values[i] );
				} else { 
					if ( nth == (i+1) ) {
						foundnth = 1;
					}
				}
			}

			if ( retRow ) {
			    rowStr += "\n";
			} else if ( outf ) { 
				fprintf( outf, "\n" ); 
			} else {
				if ( foundnth ) {
					rowStr = _row->g_values[nth-1];
				}
			}

			for ( i = 0; i < len; ++i ) {
				if ( _row->g_names[i] ) free( _row->g_names[i] );
				if ( _row->g_values[i] ) free( _row->g_values[i] );
				_row->g_names[i] = _row->g_values[i] = NULL;
			}
		}
		return 1;
	}

	// print keys & values
	// change format from "col=[data]" to "insert into db.table ( col ) values ( data )" format if export
	int cnt = 1, typerc = 0;
	Jstr instr, outstr;
	//prt(("c29293 forExport=%d outf=%0x dbobj=%0x\n", forExport, outf, dbobj ));
	if ( ! forExport || ! outf || ! dbobj ) {
		//prt(("s3349 numKeyVals=%d\n", _row->numKeyVals ));
		for ( int i = 0; i < _row->numKeyVals; ++i ) {
			//prt(("s22393 i=%d dbname=[%s]\n", i, _row->prop[i].dbname ));
			//prt(("s22393 tabname=[%s]\n", _row->prop[i].tabname ));
			//prt(("s22393 colname=[%s]\n", _row->prop[i].colname ));
			if ( 0==strcmp(_row->prop[i].colname, "spare_" ) ) continue;
			if ( strstr(_row->prop[i].colname, ".spare_" ) ) continue;
			if ( strstr(_row->prop[i].colname, "all(" ) ) continue;

			if ( retRow ) {
				// copy of if ( outf )  but replace with rowStr
				if ( *(_row->prop[i].dbname) != '\0' ) {
					rowStr += Jstr(_row->prop[i].dbname) + "." + _row->prop[i].tabname 
							  + "." + _row->prop[i].colname + "=[";
				} else if ( *(_row->prop[i].tabname) != '\0' ) {
					rowStr += Jstr(_row->prop[i].tabname) + "." + _row->prop[i].colname + "=[";
				} else {
					rowStr += Jstr(_row->prop[i].colname) + "=[";
				}

				if (  streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIME) 
					  || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMESTAMP) ) {
					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					JagTime::convertDateTimeToLocalStr( instr, outstr );
					rowStr += outstr;
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIMENANO) ) {
					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					JagTime::convertDateTimeToLocalStr( instr, outstr, true );
					rowStr += outstr;
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIME) ) {
					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					JagTime::convertTimeToStr( instr, outstr, 1 );
					rowStr += outstr;
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMENANO) ) {
					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					JagTime::convertTimeToStr( instr, outstr, 2 );
					rowStr += outstr;
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATE) ) {
					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					JagTime::convertDateToStr( instr, outstr );
					rowStr += outstr;
				} else if ( isInteger(_row->prop[i].type) ) {
					if ( *(_row->data+_row->prop[i].offset) != '\0' ) {
						char dbuf[48];
						sprintf( dbuf, "%lld", rayatol(_row->data+_row->prop[i].offset, _row->prop[i].length) );
						rowStr += dbuf;
					}
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_FLOAT) || streq(_row->prop[i].type, JAG_C_COL_TYPE_DOUBLE) ) {
					if ( *(_row->data+_row->prop[i].offset) != '\0' ) {
						char dbuf[64];
						memset(dbuf, 0, 64 );
						memcpy( dbuf, _row->data+_row->prop[i].offset, _row->prop[i].length );
						rowStr += dbuf;
					}
				} else {
					v = *(_row->data+_row->prop[i].offset + _row->prop[i].length);
					*(_row->data+_row->prop[i].offset + _row->prop[i].length) = '\0';
					rowStr += _row->data+_row->prop[i].offset;
					*(_row->data+_row->prop[i].offset + _row->prop[i].length) = v;
				}
				rowStr += "] ";
			} else if ( outf ) {
				// skip empty columns 
				if ( _row->prop[i].length < 1 ) continue;
				_debug && prt(("\n c3382 db=[%s] tab=[%s] col=[%s]\n", 
								_row->prop[i].dbname, _row->prop[i].tabname, _row->prop[i].colname ));

				if (  1 ) {
    				if ( *(_row->prop[i].dbname) != '\0' ) {
    					fprintf( outf, "%s.%s.%s=[",  _row->prop[i].dbname, _row->prop[i].tabname, _row->prop[i].colname );
    				} else if ( *(_row->prop[i].tabname) != '\0' ) {
    					fprintf( outf, "%s.%s=[", _row->prop[i].tabname, _row->prop[i].colname );
    				} else {
    					fprintf( outf, "%s=[", _row->prop[i].colname );
    				}

					//prt(("\nc3292 i=%d _row->prop[i].type=[%s]\n\n", i,  _row->prop[i].type ));
    
    				if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIME) || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMESTAMP) ) {
    					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
    					JagTime::convertDateTimeToLocalStr( instr, outstr );
    					fprintf( outf, "%s", outstr.c_str() );
    				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIMENANO) ) {
    					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
    					JagTime::convertDateTimeToLocalStr( instr, outstr, true );
    					fprintf( outf, "%s", outstr.c_str() );
    				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIME) ) {
    					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
    					JagTime::convertTimeToStr( instr, outstr, 1 );
    					fprintf( outf, "%s", outstr.c_str() );
    				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMENANO) ) {
    					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
    					JagTime::convertTimeToStr( instr, outstr, 2 );
    					fprintf( outf, "%s", outstr.c_str() );
    				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATE) ) {
    					instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
    					JagTime::convertDateToStr( instr, outstr );
    					fprintf( outf, "%s", outstr.c_str() );
    				} else if ( isInteger(_row->prop[i].type) ) {
    					if ( *(_row->data+_row->prop[i].offset) == '\0' ) fprintf( outf, "" );
    					else fprintf( outf, "%lld", rayatol(_row->data+_row->prop[i].offset, _row->prop[i].length) );
    				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_FLOAT) || streq(_row->prop[i].type, JAG_C_COL_TYPE_DOUBLE) ) {
    					if ( *(_row->data+_row->prop[i].offset) == '\0' ) fprintf( outf, "" );
    					else {
							// print data here in jql
							stripTailZeros(  _row->data+_row->prop[i].offset, _row->prop[i].length );
    						jagfwritefloat( _row->data+_row->prop[i].offset, _row->prop[i].length, outf );
    					}
    				} else {
    					v = *(_row->data+_row->prop[i].offset + _row->prop[i].length);
    					*(_row->data+_row->prop[i].offset + _row->prop[i].length) = '\0';
    					fprintf( outf, "%s", _row->data+_row->prop[i].offset );
    					*(_row->data+_row->prop[i].offset + _row->prop[i].length) = v;
    				}
    				fprintf( outf, "] " ); 
				}

			} else {
				// prt(("c2203 nth=%d  cnt=%d\n", nth, cnt ));
				if ( nth == cnt ) {
					if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIME) || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMESTAMP) ) {
						instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
						JagTime::convertDateTimeToLocalStr( instr, rowStr );
					} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIMENANO) ) {
						instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
						JagTime::convertDateTimeToLocalStr( instr, rowStr, true );
					} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIME) ) {
						instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
						JagTime::convertTimeToStr( instr, rowStr, 1 );
					} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMENANO) ) {
						instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
						JagTime::convertTimeToStr( instr, rowStr, 2 );
					} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATE) ) {
						instr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
						JagTime::convertDateToStr( instr, rowStr );
					} else if ( isInteger(_row->prop[i].type) ) {
						if ( *(_row->data+_row->prop[i].offset) == '\0' ) rowStr = "";
						else rowStr = longToStr(rayatol(_row->data+_row->prop[i].offset, _row->prop[i].length));
					} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_FLOAT) || streq(_row->prop[i].type, JAG_C_COL_TYPE_DOUBLE) ) {
						if ( *(_row->data+_row->prop[i].offset) == '\0' ) rowStr = "";
						else rowStr = longDoubleToStr(raystrtold(_row->data+_row->prop[i].offset, _row->prop[i].length));
					} else {
						rowStr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
					}
					//rowStr = outstr;
					return 1;
				}
				++cnt;
			}
		}
	} else {
		// export format, export sql / export csv
		if ( forExport == JAG_EXPORT_SQL ) {
			bool isLast = 0;
			fprintf( outf, "insert into %s (", dbobj );
			for ( int i = 0; i < _row->numKeyVals; ++i ) {
				if ( i==_row->numKeyVals-2) {
					if ( 0==strcmp(_row->prop[i+1].colname, "spare_" ) ) 
					{ isLast = 1; }
				} else if ( i==_row->numKeyVals-1 ) { isLast = 1; }

				if ( ! isLast ) {
					fprintf( outf, " %s,", _row->prop[i].colname );
				} else {
					fprintf( outf, " %s ) values (", _row->prop[i].colname );
				}
				if ( isLast ) break;
			}
			
			isLast = 0;
			for ( int i = 0; i < _row->numKeyVals; ++i ) {
				if ( i==_row->numKeyVals-2) {
					if ( 0==strcmp(_row->prop[i+1].colname, "spare_" ) ) 
					{ isLast = 1; }
				} else if ( i==_row->numKeyVals-1 ) { isLast = 1; }


				outstr = formOneColumnNaturalData( _row->data, _row->prop[i].offset, _row->prop[i].length, _row->prop[i].type );
				if ( ! isLast ) {
					fprintf( outf, " '" ); 
					jagfwrite( outstr.c_str(), outstr.length(), outf );
					fprintf( outf, "'," ); 
				} else {
					fprintf( outf, " '" ); 
					jagfwrite( outstr.c_str(), outstr.length(), outf );
					fprintf( outf, "' );" ); 
				}
				if ( isLast ) break;
			}
		} else if ( forExport == JAG_EXPORT_CSV ) {
			Jstr nlstr;
			bool isLast = 0;
			for ( int i = 0; i < _row->numKeyVals; ++i ) {
				if ( i==_row->numKeyVals-2) {
					if ( 0==strcmp(_row->prop[i+1].colname, "spare_" ) ) 
					{ isLast = 1; }
				} else if ( i==_row->numKeyVals-1 ) { isLast = 1; }

				fprintf( outf, "\"" );
				outstr = formOneColumnNaturalData( _row->data, _row->prop[i].offset, _row->prop[i].length, _row->prop[i].type );
				// check to see if need escape byte before new line data
				if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_STR) ) {
					escapeNewline( outstr, nlstr );
					if ( ! isLast ) {
						jagfwrite( nlstr.c_str(), nlstr.length(), outf );
						fprintf( outf, "\"," );
					} else {
						jagfwrite( outstr.c_str(), outstr.length(), outf );
						fprintf( outf, "\"" ); 
					}
				} else {
					if (  ! isLast ) {
						jagfwrite( outstr.c_str(), outstr.length(), outf );
						fprintf( outf, "\"," );
					} else {
						jagfwrite( outstr.c_str(), outstr.length(), outf );
						fprintf( outf, "\"" );
					}
				}

				if ( isLast ) break;
			}
		}
	}
	
	if ( outf ) {
		fprintf( outf, "\n" ); 
	}

	return 1;
}

// key: db.tab.colname
bool JaguarCPPClient::_getKeyOrValue( const char *key, Jstr &strValue, Jstr &ktype )
{
	char *pval = jag_hash_lookup( &(_row->colHash ), key );
	if ( ! pval ) {
		// printf("c1033 false key=[%s]\n", key );
		return false;
	}

	JagStrSplit sp ( pval, '|' );
	if ( sp.length() < 4 ) {
		// printf("c2033 false pval=[%s] length=%d\n", pval, sp.length() );
		return false;
	}

	if ( !  _row->data ) {
		return false;
	}

	// db.tab.colname --> "type|offset|length|sig|iskey"
	char v;
	int offset, len;

	// ktype = sp[0].c_str()[0];
	ktype = sp[0];
	offset = atoi(sp[1].c_str());
	len = atoi(sp[2].c_str());

	v = *(_row->data+offset+len);
	*(_row->data+offset+len) = '\0';
	strValue = _row->data+ offset;
	*(_row->data+offset+len) = v;

	return true;
}

// return data string in JSON format
const char *JaguarCPPClient::jsonString()
{
	if ( _oneConnect ) {
		return doJsonString();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doJsonString();
		} else {
			return NULL;
		}
	} else {
		return doJsonString();
	}
}

// return JSON string, use _printRow and JagRecord toJSON for reference
const char *JaguarCPPClient::doJsonString()
{
	_jsonString = "";
	//Json::Value root;
	rapidjson::StringBuffer sbuf;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sbuf);
	writer.StartObject();

	char v;
	int i, rc, len;

	if ( hasError() ) {
	} else if ( _row->type == ' ' || !_row->data ) {
	} else if ( _row->type == 'I' ) {
	} else if ( ! _row->hasSchema ) {
		if ( _row->type == 'D' || _row->type == '1' ) {
		} else if ( _row->type == 'V' ) {
			len = 0;
			JagRecord rec;
			rec.readSource( _row->data ); 
			rc = rec.getAllNameValues(  _row->g_names, _row->g_values, &len );
			for ( i = 0; i < len; ++i ) {

				//root[ std::string(_row->g_names[i]) ] = std::string(_row->g_values[i]);
				if ( 0!=strcmp( _row->g_names[i], "spare_" ) ) { 
					writer.Key( _row->g_names[i] );
					writer.String( _row->g_values[i] );
				}

				if ( _row->g_names[i] ) free( _row->g_names[i] );
				if ( _row->g_values[i] ) free( _row->g_values[i] );
				_row->g_names[i] = _row->g_values[i] = NULL;
			}
		}
	} else {
		Jstr instr, outstr;
		for ( i = 0; i < _row->numKeyVals; ++i ) {
			if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIME) 
			     || streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIMENANO)
				|| streq(_row->prop[i].type, JAG_C_COL_TYPE_TIME) || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMENANO)
				|| streq(_row->prop[i].type, JAG_C_COL_TYPE_DATE) || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMESTAMP) ) {
    			v = *(_row->data+_row->prop[i].offset+ _row->prop[i].length);
    			*(_row->data+ _row->prop[i].offset+ _row->prop[i].length) = '\0';
    			instr =  _row->data+ _row->prop[i].offset;
    			*(_row->data+ _row->prop[i].offset+ _row->prop[i].length) = v;
				if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIME) || streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMESTAMP) ) {
					JagTime::convertDateTimeToLocalStr( instr, outstr );
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATETIMENANO) ) {
					JagTime::convertDateTimeToLocalStr( instr, outstr, true );
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIME) ) {
					JagTime::convertTimeToStr( instr, outstr, 1 );
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_TIMENANO) ) {
					JagTime::convertTimeToStr( instr, outstr, 2 );
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_DATE) ) {
					JagTime::convertDateToStr( instr, outstr );
				}
			} else {
				if ( isInteger(_row->prop[i].type) ) {
					if ( *(_row->data+_row->prop[i].offset) == '\0' ) outstr = "";
					outstr = longToStr(rayatol(_row->data+_row->prop[i].offset, _row->prop[i].length));
				} else if ( streq(_row->prop[i].type, JAG_C_COL_TYPE_FLOAT) || streq(_row->prop[i].type, JAG_C_COL_TYPE_DOUBLE) ) {
					if ( *(_row->data+_row->prop[i].offset) == '\0' ) outstr = "";
					outstr = longDoubleToStr(raystrtold(_row->data+_row->prop[i].offset, _row->prop[i].length));
				} else {
					outstr = Jstr( _row->data+_row->prop[i].offset, _row->prop[i].length );
				}
			}

			if ( *(_row->prop[i].dbname) != '\0' ) {
					instr = Jstr(_row->prop[i].dbname) + "." + _row->prop[i].tabname + "." + _row->prop[i].colname;
			} else if ( *(_row->prop[i].tabname) != '\0' ) {
					instr = Jstr(_row->prop[i].tabname) + "." + _row->prop[i].colname;
			} else {
					instr = _row->prop[i].colname;
			}

			if ( 0!=strcmp( instr.c_str(), "spare_" ) ) { 
				writer.Key( instr.c_str() );
				writer.String( outstr.c_str() );
			}

    	}
	}

	writer.EndObject();
	_jsonString = sbuf.GetString();
	return _jsonString.c_str();
}

// return data buffer in row
const char *JaguarCPPClient::getMessage( )
{
	if ( _oneConnect ) {
		return doGetMessage();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetMessage( );
		} else {
			return NULL;
		}
	} else {
		return doGetMessage( );
	}
}

// return data buffer
const char *JaguarCPPClient::doGetMessage( )
{
	return _row->data;
}

// return data buffer len in row
abaxint JaguarCPPClient::getMessageLen( )
{
	if ( _oneConnect ) {
		return doGetMessageLen();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetMessageLen( );
		} else {
			return 0;
		}
	} else {
		return doGetMessageLen( );
	}
}

// return data buffer len
abaxint JaguarCPPClient::doGetMessageLen( )
{
	return _row->datalen;
}

char JaguarCPPClient::getMessageType( )
{
	if ( _oneConnect ) {
		return doGetMessageType();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetMessageType( );
		} else {
			return ' ';
		}
	} else {
		return doGetMessageType( );
	}
}

char JaguarCPPClient::doGetMessageType( )
{
	return _row->type;
}

// returns a integer
// 1: if name exists in row; 0: if name does not exists in row
int JaguarCPPClient::getInt(  const char * name, int *value )
{
	*value = 0;
	char *p = getValue( name );
	if ( ! p ) return 0;

	*value = atoi(p);
	if ( p ) free( p );
	return 1;
}

// returns a long
// 1: if name exists in row; 0: if name does not exists in row
int JaguarCPPClient::getLong( const char * name, abaxint *value )
{
	*value = 0;
	char *p = getValue( name );
	if ( ! p ) return 0;

	*value = jagatoll(p);
	if ( p ) free( p );
	return 1;
}

// returns a float value
// 1: if name exists in row; 0: if name does not exists in row
int JaguarCPPClient::getFloat( const char * name, float *value )
{
	*value = 0.0;
	char *p = getValue( name );
	if ( ! p ) return 0;

	*value = atof(p);
	if ( p ) free( p );
	return 1;
}

// returns a float value
// 1: if name exists in row; 0: if name does not exists in row
int JaguarCPPClient::getDouble(  const char * name, double *value )
{
	*value = 0.0;
	char *p = getValue( name );
	if ( ! p ) return 0;

	*value = atof(p);
	if ( p ) free( p );
	return 1;
}


// Parse error msg from _END_[T=ddd|E=Error 1002 Syntax error in ...|X=...]
// NULL: if no error
// jagmalloced string containing error message, must be freed.
char *JaguarCPPClient::_getField( const char * rowstr, char fieldToken )
{
	char *newbuf = NULL;
	const char *p; 
	const char *start, *end;
	int len;
	char  feq[3];

	sprintf(feq, "%c=", fieldToken );  // fieldToken: E   "E=" in _row
	p = strstr( rowstr, feq );
	if ( ! p ) {
		return NULL;
	}

	start = p +2;
	end = strchr( start, '|' );
	if ( ! end ) {
		end = strchr( start, ']' );
	}

	if ( ! end ) return NULL;

	len = end - start;
	if ( len < 1 ) {
		return NULL;
	}

	newbuf = (char*) jagmalloc ( len+1);
	//printf("c4002 newbuf jagmalloced %d bytes\n", len+1 );
	memcpy( newbuf, start, len );
	newbuf[len] = '\0';

	return newbuf;
}

char *JaguarCPPClient::getNthValue( int nth )
{
	char *p = _getNthValue( nth );
	if ( p ) return p;
	p = getAllByIndex( nth );
	return p;
}

// get n-th column in the _row
// NULL if not found; jagmalloced char* if found, must be freed later
char *JaguarCPPClient::_getNthValue( int nth )
{
	if ( _oneConnect ) {
		return doGetNthValue( nth );
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetNthValue( nth );
		} else {
			return NULL;
		}
	} else {
		return doGetNthValue( nth );
	}
}

// get n-th column in the _row
// NULL if not found; jagmalloced char* if found, must be freed later
char *JaguarCPPClient::doGetNthValue( int nth )
{
	if ( nth < 1 ) { return NULL; }
	Jstr rowStr;
    _printRow( NULL, nth, false, rowStr, 0, NULL );
    return strdup(rowStr.c_str());
}

int JaguarCPPClient::getColumnCount()
{
	if ( _oneConnect ) {
		return doGetColumnCount();
	}

	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnCount();
		} else {
			return 0;
		}
	} else {
		return doGetColumnCount();
	}
}

int JaguarCPPClient::doGetColumnCount()
{
    // printf("c7384 JaguarCPPClient::getColumnCount() isMeta=%d\n", _row->isMeta );
    int cnt = 0;
    if ( _row->isMeta ) {
        return _row->colCount;
    }

    cnt =  _row->numKeyVals;
    // printf("c3002 getColumnCount cnt=%d\n", cnt );
    return cnt;
}


// get selected kv or select *  columns property
int JaguarCPPClient::findAllMetaKeyValueProperty( JagRecord &rrec )
{
	Jstr defdb, deftab, defcol;
	char *p = rrec.getValue("schema.table");
	if ( ! p ) return 0;

	JagRecord srec;
	srec.readSource( p );

	int len = 0;
	srec.getAllNameValues(  _row->g_names, _row->g_values, &len );
	for ( int i =0 ; i < len; ++i ) {
		strcpy( _row->prop[i].dbname, "schema");
		strcpy( _row->prop[i].tabname, "table");
		strcpy( _row->prop[i].colname, _row->g_names[i] );
		_row->prop[i].length = 64;
		strcpy(_row->prop[i].type, JAG_C_COL_TYPE_STR );
		_row->prop[i].sig = 0;
		if ( _row->g_names[i] ) free(  _row->g_names[i] );
		if ( _row->g_values[i] ) free(  _row->g_values[i] );
		_row->g_names[i] = _row->g_values[i] = NULL;
	}
	_row->numKeyVals = len;

	if ( p ) free(p);
	return 1;
}

// get selected kv or select *  columns property
int JaguarCPPClient::findAllNameValueProperty( JagRecord &srec )
{
	int len = 0;
	srec.getAllNameValues(  _row->g_names, _row->g_values, &len );
	for ( int i =0 ; i < len; ++i ) {
		strcpy( _row->prop[i].dbname, "name");
		strcpy( _row->prop[i].tabname, "value");
		//prt(("c3302 colname=[%s]  value=[%s]\n", _row->g_names[i], _row->g_values[i] ));
		strcpy( _row->prop[i].colname, _row->g_names[i] );
		_row->prop[i].length = jagatoi(_row->g_values[i]);
		strcpy(_row->prop[i].type, JAG_C_COL_TYPE_STR );
		_row->prop[i].sig = 0;
		if ( _row->g_names[i] ) free(  _row->g_names[i] );
		if ( _row->g_values[i] ) free(  _row->g_values[i] );
		_row->g_names[i] = _row->g_values[i] = NULL;
	}
	_row->numKeyVals = len;

	return 1;
}

// col: 1 - N
char* JaguarCPPClient::getCatalogName( int col )
{
	if ( _oneConnect ) {
		return doGetCatalogName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetCatalogName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetCatalogName( col );
	}
}

char* JaguarCPPClient::doGetCatalogName( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return NULL; }
	return strdup( _row->prop[col-1].dbname );
}

// col: 1 - N
char* JaguarCPPClient::getColumnClassName( int col )
{
	if ( _oneConnect ) {
		return doGetColumnClassName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnClassName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetColumnClassName( col );
	}
}

char* JaguarCPPClient::doGetColumnClassName( int col )
{
	return strdup("Unknown");
}

int JaguarCPPClient::getColumnDisplaySize( int col )
{
	if ( _oneConnect ) {
		return doGetColumnDisplaySize( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnDisplaySize( col );
		} else {
			return 0;
		}
	} else {
		return doGetColumnDisplaySize( col );
	}
}

int JaguarCPPClient::doGetColumnDisplaySize( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return 0; }
	return _row->prop[col-1].length + 10;
}

char * JaguarCPPClient::getColumnLabel( int col )
{
	if ( _oneConnect ) {
		return doGetColumnLabel( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnLabel( col );
		} else {
			return NULL;
		}
	} else {
		return doGetColumnLabel( col );
	}
}

char * JaguarCPPClient::doGetColumnLabel( int col )
{
	return doGetColumnName( col );
}

char * JaguarCPPClient::getColumnName( int col )
{
	if ( _oneConnect ) {
		return doGetColumnName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetColumnName( col );
	}
}

char * JaguarCPPClient::doGetColumnName( int col )
{
	// printf("c8384 JaguarCPPClient::getColumnName col=%d  _row->numKeyVals=%d\n", col, _row->numKeyVals );
	
	if ( col > _row->numKeyVals || col < 1 ) { return NULL; }
	return strdup( _row->prop[col-1].colname );
}

int JaguarCPPClient::getColumnType( int col )
{
	if ( _oneConnect ) {
		return doGetColumnType( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnType( col );
		} else {
			return 0;
		}
	} else {
		return doGetColumnType( col );
	}
}

int JaguarCPPClient::doGetColumnType( int col )
{
	// printf("c3002 getColumnType col=%d\n", col );
	if ( col > _row->numKeyVals || col < 1 ) { return 0; }
	Jstr type = _row->prop[col-1].type;

    if ( type == JAG_C_COL_TYPE_STR ) {
    	return 1;
    } else if ( type == JAG_C_COL_TYPE_DINT ) {
    	return 4;
    } else if ( type == JAG_C_COL_TYPE_FLOAT ) {
    	return 6;
    } else if ( type == JAG_C_COL_TYPE_DOUBLE ) {
    	return 8;
    } else if ( type == JAG_C_COL_TYPE_DATETIME ) {
    	return 91;
    } else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
    	return 92;
    } else if ( type == JAG_C_COL_TYPE_DATE ) {
    	return 90;
    } else if ( type == JAG_C_COL_TYPE_TIME ) {
    	return 93;
    } else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
    	return 94;
    } else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
    	return 95;
    } else if ( type == JAG_C_COL_TYPE_DTINYINT ) {
    	return -6;
    } else if ( type == JAG_C_COL_TYPE_DBIGINT ) {
    	return -5;
    } else if ( type == JAG_C_COL_TYPE_DSMALLINT ) {
    	return 5;
    } else if ( type == JAG_C_COL_TYPE_DMEDINT ) {
    	return 5;
    } else if ( type == JAG_C_COL_TYPE_DBOOLEAN ) {
    	return -2;
    } else if ( type == JAG_C_COL_TYPE_DBIT ) {
    	return -3;
    } else {
    	return 12;
	}
}

char *JaguarCPPClient::getColumnTypeName( int col )
{
	if ( _oneConnect ) {
		return doGetColumnTypeName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetColumnTypeName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetColumnTypeName( col );
	}
}

char *JaguarCPPClient::doGetColumnTypeName( int col )
{
	// printf("c3002 getColumnTypeName col=%d\n", col );

	if ( col > _row->numKeyVals || col < 1 ) { return NULL; }
	Jstr type =  _row->prop[col-1].type;

    if ( type == JAG_C_COL_TYPE_STR ) {
    	return strdup("CHAR");
    } else if ( type == JAG_C_COL_TYPE_DBOOLEAN ) {
    	return strdup("BINARY");
    } else if ( type == JAG_C_COL_TYPE_DBIT ) {
    	return strdup("BIT");
    } else if ( type == JAG_C_COL_TYPE_DINT ) {
    	return strdup("INTEGER");
    } else if ( type == JAG_C_COL_TYPE_DTINYINT ) {
    	return strdup("TINYINT");
    } else if ( type == JAG_C_COL_TYPE_DBIGINT ) {
    	return strdup("BIGINT");
    } else if ( type == JAG_C_COL_TYPE_DSMALLINT ) {
    	return strdup("SMALLINT");
    } else if ( type == JAG_C_COL_TYPE_DMEDINT ) {
    	return strdup("SMALLINT");
    } else if ( type == JAG_C_COL_TYPE_FLOAT ) {
    	return strdup("FLOAT");
    } else if ( type == JAG_C_COL_TYPE_DOUBLE ) {
    	return strdup("DOUBLE");
    } else if ( type == JAG_C_COL_TYPE_DATETIME ) {
    	return strdup("DATETIME");
	} else if ( type == JAG_C_COL_TYPE_TIMESTAMP ) {
		return strdup("TIMESTAMP");
    } else if ( type == JAG_C_COL_TYPE_DATETIMENANO ) {
    	return strdup("DATETIMENANO");
    } else if ( type == JAG_C_COL_TYPE_TIME ) {
    	return strdup("TIME");
    } else if ( type == JAG_C_COL_TYPE_TIMENANO ) {
    	return strdup("TIMENANO");
    } else {
    	return strdup("VARCHAR");
	}
}

int JaguarCPPClient::getScale( int col )
{
	if ( _oneConnect ) {
		return doGetScale( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetScale( col );
		} else {
			return 0;
		}
	} else {
		return doGetScale( col );
	}
}

int JaguarCPPClient::doGetScale( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return 0; }
	return  _row->prop[col-1].sig;
}

char * JaguarCPPClient::getSchemaName( int col )
{
	if ( _oneConnect ) {
		return doGetSchemaName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetSchemaName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetSchemaName( col );
	}
}

char * JaguarCPPClient::doGetSchemaName( int col )
{
	if ( col >= _row->numKeyVals ) { return NULL; }
	return doGetCatalogName( col );
}

char * JaguarCPPClient::getTableName( int col )
{
	if ( _oneConnect ) {
		return doGetTableName( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doGetTableName( col );
		} else {
			return NULL;
		}
	} else {
		return doGetTableName( col );
	}
}

char * JaguarCPPClient::doGetTableName( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return NULL; }
	return  strdup( _row->prop[col-1].tabname );
}


bool JaguarCPPClient::isAutoIncrement( int col )
{
	if ( _oneConnect ) {
		return doIsAutoIncrement( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsAutoIncrement( col );
		} else {
			return 0;
		}
	} else {
		return doIsAutoIncrement( col );
	}
}

bool JaguarCPPClient::doIsAutoIncrement( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return false;
}

bool JaguarCPPClient::isCaseSensitive( int col )
{
	if ( _oneConnect ) {
		return doIsCaseSensitive( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsCaseSensitive( col );
		} else {
			return 0;
		}
	} else {
		return doIsCaseSensitive( col );
	}
}

bool JaguarCPPClient::doIsCaseSensitive( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return true;
}

bool JaguarCPPClient::isCurrency( int col )
{
	if ( _oneConnect ) {
		return doIsCurrency( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsCurrency( col );
		} else {
			return 0;
		}
	} else {
		return doIsCurrency( col );
	}
}

bool JaguarCPPClient::doIsCurrency( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return false;
}

bool JaguarCPPClient::isDefinitelyWritable( int col )
{
	if ( _oneConnect ) {
		return doIsDefinitelyWritable( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsDefinitelyWritable( col );
		} else {
			return 0;
		}
	} else {
		return doIsDefinitelyWritable( col );
	}
}

bool JaguarCPPClient::doIsDefinitelyWritable( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return false;
}

int JaguarCPPClient::isNullable( int col )
{
	if ( _oneConnect ) {
		return doIsNullable( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsNullable( col );
		} else {
			return 0;
		}
	} else {
		return doIsNullable( col );
	}
}

int JaguarCPPClient::doIsNullable( int col )
{
	return 0;
}

bool JaguarCPPClient::isReadOnly( int col )
{
	if ( _oneConnect ) {
		return doIsReadOnly( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsReadOnly( col );
		} else {
			return 0;
		}
	} else {
		return doIsReadOnly( col );
	}
}

bool JaguarCPPClient::doIsReadOnly( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return false;
}

bool JaguarCPPClient::isSearchable( int col )
{
	if ( _oneConnect ) {
		return doIsSearchable( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsSearchable( col );
		} else {
			return 0;
		}
	} else {
		return doIsSearchable( col );
	}
}

bool JaguarCPPClient::doIsSearchable( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	return true;
}

bool JaguarCPPClient::isSigned( int col )
{
	if ( _oneConnect ) {
		return doIsSigned( col );
	}
	AbaxBuffer bfr;
	if ( _isparent ) {
		if ( _thrdCliMap && _thrdCliMap->getValue( THREADID, bfr ) ) {
			JaguarCPPClient *tcli = (JaguarCPPClient*) bfr.value();
			return tcli->doIsSigned( col );
		} else {
			return 0;
		}
	} else {
		return doIsSigned( col );
	}
}

bool JaguarCPPClient::doIsSigned( int col )
{
	if ( col > _row->numKeyVals || col < 1 ) { return false; }
	Jstr type =  _row->prop[col-1].type;

    if ( type == JAG_C_COL_TYPE_STR ) {
    	return false;
    } else if ( type == JAG_C_COL_TYPE_DINT ) {
    	return true;
    } else if ( type == JAG_C_COL_TYPE_FLOAT ) {
    	return true;
    } else if ( type == JAG_C_COL_TYPE_DOUBLE ) {
    	return true;
	} else if ( type == JAG_C_COL_TYPE_DATETIME 
		|| type == JAG_C_COL_TYPE_DATETIMENANO
		|| type == JAG_C_COL_TYPE_TIME || type == JAG_C_COL_TYPE_TIMENANO || type == JAG_C_COL_TYPE_TIMESTAMP ) {
    	return false;
    } else {
    	return false;
	}
}

// NN,3,4,4[....]
// add to _colHash
// return offset of this table
// return -1 or error; else offset of this table
int JaguarCPPClient::_parseSchema( const char *keyschema )
{
	// db.tab.colname --> "type|size|sig"
	// printf("c2030 _parseSchema keyschema=[%s]\n", keyschema );
	JagSchemaRecord krec;
	int rc = krec.parseRecord( keyschema );
	if (  rc < 0 ) {
		// printf("c3529 error parseRecord(%s)\n", keyschema );
		return -1;
	}

	JagStrSplit namesp;
	int len = krec.columnVector->size();
	Jstr key;
	char buf[128];
	Jstr type;
	int offset, length, sig;
	bool iskey;
	const char *p;
	// printf("c301 krec.columnVector->size()=%d\n", len );

	for ( int i = 0; i < len; ++i ) {
		key = Jstr( (*krec.columnVector)[i].name.c_str() );
		type = (*krec.columnVector)[i].type;
		offset = (*krec.columnVector)[i].offset;
		length = (*krec.columnVector)[i].length;
		sig = 	 (*krec.columnVector)[i].sig;
		iskey =  (*krec.columnVector)[i].iskey;

		// printf("c8391 key=[%s] i=%d  iskey=%d\n", key.c_str(), i, iskey ); fflush( stdout );
		if ( key == "_id" && 0 == i && iskey ) {
			continue;
		}

		memset( buf, 0, 128 );
		sprintf( buf, "%s|%d|%d|%d|%d", type.c_str(), offset, length, sig, iskey );
		jag_hash_insert( &( _row->colHash), key.c_str(), buf );
		//prt(("c6210 jag_hash_insert  key=[%s]   val=[%s]\n", key.c_str(), buf ));
		
		p = key.c_str();
		//_debug && prt(("\nc2901 p=key=[%s]\n", p ));
		while( isValidNameChar( *p ) || *p == '_' || *p == ':' ) ++p;
		if ( *p != '\0' ) {
			strcpy( _row->prop[ _row->numKeyVals].colname, key.c_str() );
		} else {
			namesp.init( key.c_str(), '.' );
			if ( namesp.length() >= 3 ) {
				strcpy( _row->prop[ _row->numKeyVals].dbname, namesp[0].c_str() );
				strcpy( _row->prop[ _row->numKeyVals].tabname, namesp[1].c_str() );
				strcpy( _row->prop[ _row->numKeyVals].colname, namesp[2].c_str() );			
				//_debug && prt(("c5841 namesp.length()=%d >= 3 tab=[%s] \n", namesp.length(), namesp[1].c_str() ));
			} else if ( namesp.length() == 1 ) {
				strcpy( _row->prop[ _row->numKeyVals].dbname, "");
				strcpy( _row->prop[ _row->numKeyVals].tabname, "");
				strcpy( _row->prop[ _row->numKeyVals].colname, namesp[0].c_str() );
				/***
				_debug && prt(("c5842 namesp.length()=%d < 3 db=[%s], tab=[%s] col=[%s]\n", namesp.length(),
								_row->prop[ _row->numKeyVals].dbname, _row->prop[ _row->numKeyVals].tabname, 
								_row->prop[ _row->numKeyVals].colname )); 
				***/
			}
		}

		_row->prop[ _row->numKeyVals].offset = offset;
		_row->prop[ _row->numKeyVals].length = length;
		_row->prop[ _row->numKeyVals].sig = sig;
		//_row->prop[ _row->numKeyVals].type = Jstr(type.c_str(), type.size() );
		charFromStr(_row->prop[ _row->numKeyVals].type, type );
		_row->prop[ _row->numKeyVals].iskey = iskey;

		// printf("c1900 parseSchemaData [%s] [%s] [%s] [%d] [%d] [%d] [%c] [%d]\n",  
		//       _row->prop[ _row->numKeyVals].dbname, _row->prop[ _row->numKeyVals].tabname, 
		//         _row->prop[ _row->numKeyVals].colname, offset, length, sig, type, iskey);
		// printf("c1902 append to _row->prop numKeyVals=%d\n", _row->numKeyVals );

		++ _row->numKeyVals;
		// printf("c5293 _row->numKeyVals=%d\n", _row->numKeyVals );
	}
	
	// printf("c2031 _parseSchema done\n\n");

}

// load file by thread-pool
// loadCommand: "load localhost:/path/file into TAB123 ....."
// loadCommand: "load host123:/path/file into TAB123 ....."
abaxint JaguarCPPClient::loadFile( const char *loadCommand )
{
	abaxint loadCount = 0;
	if ( !_isparent ) { return 0; }

	// mutual exclusive with monitorInsert flushInsertBuffer
	JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );

	AbaxString dbobj;
	Jstr cmd, cmdhdr, dbname, tabname, errpath, coldata, reterr;
	JagParseParam parseParam;
	JagParseAttribute jpa;
	jpa.dfdbname = _dbname;
	jpa.timediff = _tdiff;
	JagParser parser( NULL, this );
	int rc = parser.parseCommand( jpa, loadCommand, &parseParam, reterr );
	
	if ( ! rc ) {
		_queryerrmsg = Jstr("E6001 Error command [") + loadCommand + "]" ;
		return 0;
	}

	if ( ! JagFileMgr::exist( parseParam.batchFileName ) || ! rc ) {
		_queryerrmsg = Jstr("Input file [") + parseParam.batchFileName + "] does not exist.";
		return 0;
	}

	_parentCli->queryDirect( "_cschema" );
	while ( _parentCli->reply() ) {}

	dbname = parseParam.objectVec[0].dbName;
	tabname = parseParam.objectVec[0].tableName;
	dbobj = AbaxString( dbname ) + "." + tabname; 
	const JagTableOrIndexAttrs *objAttr = NULL;
	_mapLock->readLock( -1 );
	objAttr = _schemaMap->getValue( dbobj );
	if ( !_isparent || !_schemaMap || ! objAttr ) {
		_mapLock->readUnlock( -1 );		
		_queryerrmsg = "Object info does not exist. Please check if table/index is created.";
		return 0;
	}

	errpath = jaguarHome() + "/log";
	JagFileMgr::makedirPath( errpath, 0700);
	errpath += "/load.err";
	cmdhdr = Jstr("insert into ") + dbname + "." + tabname + " values (";
	
	int KEYVALLEN = objAttr->keylen + objAttr->vallen;
	int i, fd, len, linelen = KEYVALLEN*3;
	char *p, *q;
	char linesep = parseParam.lineSep;
	char fieldsep = parseParam.fieldSep;
	char quotesep = parseParam.quoteSep;
	char quotesep_doublequote = '"';
	char *line = (char*)jagmalloc(linelen);
	memset(line, 0, linelen);
	fd = jagopen((char *)parseParam.batchFileName.c_str(), O_RDONLY );
	if ( fd < 0 ) {
		_mapLock->readUnlock( -1 );
		_queryerrmsg = Jstr("E3220 Error open file ") + parseParam.batchFileName + " for read.";
		if ( line ) free ( line );
		return 0;		
	}
	_mapLock->readUnlock( -1 );

	JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> qmap;
	bool gotonecol;
	int errLimit = 20;
	FILE *fperr = jagfopen( errpath.c_str(), "w");
	JagTextFileBuffReader br(fd, linesep );
	br.connectNextlineDQ();
	
	abaxint onecnt = 0;
	bool firstKeyEmpty = false;
	
	while ( br.getLine( line, linelen, &len ) ) {
		if ( len <=3 ) continue;
		cmd = cmdhdr;
		if ( rc > errLimit ) break;
		if ( len == linelen && line[linelen-1] != linesep ) {
			if ( fperr) {
				jagfwrite( line, linelen, fperr );
				fprintf(fperr, "ERR1 len=%d line[linelen-1]=[%c] linesep=[%c]\n", len, line[linelen-1], linesep );
			}
			while( br.getLine( line, linelen, &len ) ) {
				if ( len == linelen && line[linelen-1] != linesep ) {
					if ( fperr ) {
						jagfwrite( line, linelen, fperr );
						fprintf(fperr, "ERR2 len=%d line[linelen-1]=[%c] linesep=[%c]\n", len, line[linelen-1], linesep );
					}
					++rc;
				} else {
					if ( fperr ) {
						jagfwrite( line, len, fperr );
						fprintf(fperr, "ERR3 len=%d line[linelen-1]=[%c] linesep=[%c]\n", len, line[linelen-1], linesep );
					}
					break;
				}
				if ( rc > errLimit ) break;
			}			
			++rc;
			if ( rc > errLimit ) break;
		}
		
		p = line;
		gotonecol = false;
		// prt(("c2004 fieldsep=[%c]  quotesep=[%c] linesep=[%c]\n", fieldsep, quotesep, linesep ));
		//prt(("s5142 line=p=[%s]\n", line ));
		firstKeyEmpty = false;
		// for ( i = 0; i < objAttr->numCols; ++i ) 
		for ( i = 0; i < objAttr->numCols-1; ++i ) {
			coldata = "";
			_mapLock->readLock( -1 );
			// if ( !_isparent || !_schemaMap || !_schemaMap->getValue( dbobj, objAttr ) ) 
			objAttr = _schemaMap->getValue( dbobj );
			if ( !_isparent || !_schemaMap || ! objAttr ) {
				_mapLock->readUnlock( -1 );
				_queryerrmsg = "Object seems to be dropped. Load process is stopped.";
				if ( line ) free ( line );
				if ( fperr ) fclose(fperr);
				return 0;
			}
			_mapLock->readUnlock( -1 );

			//{
				while ( jagisspace(*p) ) ++p;
				if ( *p == '\0' || *p == linesep ) {
					// prt(("c3100 break here sptr=[%s] i=%d\n", sptr, i ));
					break;
				} 
				
				if ( *p == fieldsep ) {
					++p;
				} else if ( *p == quotesep || *p == quotesep_doublequote ) {
					q = jumptoEndQuote(p);
					if ( *q != '\0' ) {
						*q = '\0';
						if ( '\0' == *(p+1) ) {
							coldata = "";
						} else {
							coldata = Jstr("'") + Jstr(p+1) + "'";
						}
						p = q + 1;
						while ( jagisspace(*p) ) ++p;
						if ( *p == fieldsep ) ++p; // pass its own ,
					}
				} else {
					if ( 0 == strncasecmp(p, "point(", 6 ) 
					     || 0 == strncasecmp(p, "point3d(", 8 )
					     || 0 == strncasecmp(p, "multipoint(", 11 )
					     || 0 == strncasecmp(p, "multipoint3d(", 13 )
					     || 0 == strncasecmp(p, "line(", 5 )
					     || 0 == strncasecmp(p, "line3d(", 7 )
					     || 0 == strncasecmp(p, "linestring(", 11 )
					     || 0 == strncasecmp(p, "linestring3d(", 13 )
					     || 0 == strncasecmp(p, "polygon(", 8 )
					     || 0 == strncasecmp(p, "polygon3d(", 10 )
					     || 0 == strncasecmp(p, "multilinestring(", 16 )
					     || 0 == strncasecmp(p, "multilinestring3d(", 18 )
					     || 0 == strncasecmp(p, "multipolygon(", 13 )
					     || 0 == strncasecmp(p, "multipolygon3d(", 15 )
					     || 0 == strncasecmp(p, "square(", 7 )
					     || 0 == strncasecmp(p, "square3d(", 9 )
					     || 0 == strncasecmp(p, "circle(", 7 )
					     || 0 == strncasecmp(p, "circle3d(", 9 )
					     || 0 == strncasecmp(p, "sphere(", 7 )
					     || 0 == strncasecmp(p, "cube(", 5 )
					     || 0 == strncasecmp(p, "rectangle(", 10 )
					     || 0 == strncasecmp(p, "rectangle3d(", 12 )
					     || 0 == strncasecmp(p, "box(", 4 )
					     || 0 == strncasecmp(p, "cone(", 5 )
					     || 0 == strncasecmp(p, "triangle(", 9 )
					     || 0 == strncasecmp(p, "triangle3d(", 11 )
					     || 0 == strncasecmp(p, "cylinder(", 9 )
					     || 0 == strncasecmp(p, "ellipse(", 8 )
					     || 0 == strncasecmp(p, "ellipse3d(", 10 )
					     || 0 == strncasecmp(p, "ellipsoid(", 10 )
					     || 0 == strncasecmp(p, "json(", 5 )
					     || 0 == strncasecmp(p, "range(", 6 )
					 )
					 {
					 	// jump q to the ending ')'
						int brackets = 0;
						for ( q=p; *q != '\0'; ++q ) {
							if ( *q == '(' ) {
								++brackets;
							} else if ( *q == ')' ) {
								--brackets;
								if ( 0 == brackets ) {
									break;
								}
							}
						}
						if ( 0 != brackets ) {
							coldata = "";
						} else {
							++q;
    						coldata = Jstr(p, q-p);
							prt(("c2093 coldata=[%s]\n", coldata.c_str() ));
						}
						p = q;
    					while ( jagisspace(*p) ) ++p;
    					if ( *p == fieldsep ) ++p;	 // pass its own ,
					 } else {
    					q = p;
    					while( *q != '\0' && *q != linesep && *q != fieldsep ) ++q;
    					--q;
    					while ( jagisspace(*q) ) --q;
    					++q;
    					coldata = Jstr("'") + Jstr(p, q-p) + "'";
    					p = q;
    					while ( jagisspace(*p) ) ++p;
    					if ( *p == fieldsep ) ++p;	 // pass its own ,
					}
				}

				if ( gotonecol ) cmd += ",";
				if ( coldata.size() <1 ) {
					coldata = "''";
					cmd += coldata;
				} else {
					cmd += coldata;
				}
				gotonecol = true;
				prt(("c5034 running cmd=[%s] coldata=[%s]\n", cmd.c_str(), coldata.c_str() ));
				if (  0 == i && coldata == "''" ) {
					firstKeyEmpty = true;
					//prt(("c6201 firstKeyEmpty is true\n" ));
				}
		}
		cmd += ")";
		//prt(("c0182 firstKeyEmpty=%d\n", firstKeyEmpty ));
		prt(("c0183 cmd=[%s]\n", cmd.c_str() ));
		if ( ! firstKeyEmpty ) {
			//prt(("c4032 cmd=[%s]\n", cmd.c_str() ));
			++ loadCount;
			onecnt += oneCmdInsertPool( qmap, cmd );
			if ( onecnt > CLIENT_SEND_BATCH_RECORDS-1 ) {
				flushQMap( qmap );
				onecnt = 0;
			}
		} 
	}

	if ( onecnt > 0 ) flushQMap( qmap );

	if ( rc > errLimit ) rc = 0;
	else rc = 1;
	
	if ( fperr ) jagfclose( fperr );
	jagclose( fd );

	if ( rc == 0 ) _queryerrmsg = "E6004 Too many invalid lines in load input file.";
	if ( line ) free ( line );
	return loadCount;
}

void JaguarCPPClient::commit()
{
	if ( ! _isparent ) {
		return;
	}

	// prt(("c2338 JaguarCPPClient::commit() ...\n"));
	if ( _isFlushingInsertBuffer ) {
		while ( _isFlushingInsertBuffer ) {
			// prt(("c7223 _isFlushingInsertBuffer wait 1 sec this=%0x thrd=%lld ...\n", this, THREADID));
			jagsleep(2, JAG_SEC);
		}
		// prt(("c7223 _isFlushingInsertBuffer FALSE, return\n"));
		return;
	}

	JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );
	// prt(("c2338 JaguarCPPClient::commit() flushInsertBuffer ...\n"));
	flushInsertBuffer();
	thpool_wait( _insertPool );
	// prt(("c2338 JaguarCPPClient::commit() done\n"));

}

bool JaguarCPPClient::processSpool( char *querys )
{
	bool rc = 1;
	querys += 6;
	while ( isspace(*querys) ) ++querys;
	if ( *querys == ';' || *querys == '\0' || *querys == '\n' || *querys == '\r' ) {
		rc = 0;
	} else {
		char *path = querys;
		while ( *querys != ' ' && *querys != ';' && *querys != '\0' ) ++querys;
		*querys =  '\0';
		if ( strncasecmp( path, "off;", 4 ) ==0 || strncasecmp( path, "off ", 4 ) ==0 ||
		     0 == strcasecmp( path, "off" ) ) {
			 if ( _outf ) {
			 	jagfclose( _outf );
			 	_outf = NULL;
			 }
		} else {
			if ( _outf ) { jagfclose( _outf ); }
			Jstr fpenv = expandEnvPath(path);
			_outf = jagfopen( fpenv.c_str(), "wb" );
			if ( ! _outf ) {
				rc = 0;
			}
		}
	}

	if ( ! rc ) {
		_replyerrmsg = "C1235 spool command error";
		return 0;
	}

	return 1;
}

// rebuild schema map -- cleanup and rebuild new schema, must be called by parent
void JaguarCPPClient::rebuildSchemaMap()
{
	if ( !_isparent ) return;
	if ( _schemaMap ) {
		// printf("c7890 begin cleanup schema\n");
		cleanUpSchemaMap( 0 );
	}
	_schemaMap = new JagHashMap<AbaxString, JagTableOrIndexAttrs>();
}

/**
** Delete the schema map and the object in value part as JagTableOrIndexAttrs, must be called by parent
**/
void JaguarCPPClient::cleanUpSchemaMap( bool dolock )
{
	if ( !_isparent ) return;
	if ( dolock && _mapLock ) { _mapLock->writeLock( -1 ); }

	if ( _schemaMap ) {
		delete _schemaMap;
		_schemaMap = NULL;
	}
	if ( dolock && _mapLock ) { _mapLock->writeUnlock( -1 ); }
}

// update schema map without cleanup, must be called by parent
void JaguarCPPClient::updateSchemaMap( const char *msg )
{
	// prt(("c9149 this=%0x update schema map _isparent=%d, _schemaMap=%d msg=[%s]\n", this, _isparent, _schemaMap==NULL, msg));
	if ( !_isparent || !_schemaMap || !msg ) {
		prt(("c8823 _schemaMap not built this=%0x _isparent=%d msg=[%s]\n", this, _isparent, msg ));
		return;
	}
	// info:   db.tab:........\n
	// info:   db.tab:........\n
	// info:   db.tab:........\n
	// info:   db.tab.idx1:........\n
	// info:   db.tab.idx2:........\n
	// info:   db.tab.idx3:........\n
	
	char save;
	char *s, *p;
	AbaxString dbidx;
	Jstr dbobj, objonly, dbcolumn;
	PosOffsetLength onearr;
	abaxint tabpos;

	JagStrSplit sp( msg, '\n', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		s = p = (char*)sp[i].c_str();
		while ( *p != ':' && *p != '\0' ) ++p;
		if ( *p == '\0' ) continue;
		*p = '\0';
		++p;
		JagStrSplit sp2(s, '.' );
		
		JagTableOrIndexAttrs tobj, tobj2;

		tobj.schemaRecord.parseRecord(p);
		tobj2.schemaRecord.parseRecord(p);
		tobj.keylen = tobj.schemaRecord.keyLength;
		tobj2.keylen = tobj2.schemaRecord.keyLength;
		tobj.vallen = tobj.schemaRecord.valueLength;
		tobj2.vallen = tobj2.schemaRecord.valueLength;
		tobj.numCols = tobj.schemaRecord.columnVector->size();
		tobj2.numCols = tobj2.schemaRecord.columnVector->size();
		tobj.schemaString = p;
		tobj2.schemaString = p;
		
		if ( sp2.length() == 2 ) {
			tobj.dbName = sp2[0];
			tobj2.dbName = sp2[0];
			tobj.tableName = sp2[1];
			tobj2.tableName = sp2[1];
			dbobj = s;
			objonly = tobj.tableName;
		} else if ( sp2.length() == 3 ) {
			tobj.dbName = sp2[0];
			tobj2.dbName = sp2[0];
			tobj.tableName = sp2[1];
			tobj2.tableName = sp2[1];
			tobj.indexName = sp2[2];
			tobj2.indexName = sp2[2];
			dbobj = sp2[0] + "." + sp2[2];
			objonly = tobj.indexName;
		} 
		
		tobj.makeSchemaAttrArray( tobj.numCols );
		tobj2.makeSchemaAttrArray( tobj2.numCols );
		// prt(("c6031 tobj.numCols=%d tobj2.numCols=%d\n", tobj.numCols, tobj2.numCols ));
		
		for ( int j = 0; j < tobj.numCols; ++j ) {
			dbcolumn = dbobj + "." + (*(tobj.schemaRecord.columnVector))[j].name.c_str();
			tobj.schmap.addKeyValue(dbcolumn, j);
			tobj2.schmap.addKeyValue(dbcolumn, j);
			//prt(("c1028  tobj.schmap addKeyValue dbcolumn=[%s] j=%d\n", dbcolumn.c_str(), j ));
			//prt(("c1029 tobj2.schmap addKeyValue dbcolumn=[%s] j=%d\n", dbcolumn.c_str(), j ));

			tobj.schAttr[j].dbcol = dbcolumn;
			tobj2.schAttr[j].dbcol = dbcolumn;
			tobj.schAttr[j].objcol = objonly + "." + (*(tobj.schemaRecord.columnVector))[j].name.c_str();
			tobj2.schAttr[j].objcol = objonly + "." + (*(tobj.schemaRecord.columnVector))[j].name.c_str();
			tobj.schAttr[j].colname = (*(tobj.schemaRecord.columnVector))[j].name.c_str();
			tobj2.schAttr[j].colname = (*(tobj.schemaRecord.columnVector))[j].name.c_str();
			tobj.schAttr[j].isKey = (*(tobj.schemaRecord.columnVector))[j].iskey;
			tobj2.schAttr[j].isKey = (*(tobj2.schemaRecord.columnVector))[j].iskey;
			tobj.schAttr[j].offset = (*(tobj.schemaRecord.columnVector))[j].offset;
			tobj2.schAttr[j].offset = (*(tobj2.schemaRecord.columnVector))[j].offset;
			tobj.schAttr[j].length = (*(tobj.schemaRecord.columnVector))[j].length;
			tobj2.schAttr[j].length = (*(tobj2.schemaRecord.columnVector))[j].length;
			tobj.schAttr[j].sig = (*(tobj.schemaRecord.columnVector))[j].sig;
			tobj2.schAttr[j].sig = (*(tobj2.schemaRecord.columnVector))[j].sig;
			tobj.schAttr[j].type = (*(tobj.schemaRecord.columnVector))[j].type;
			tobj2.schAttr[j].type = (*(tobj2.schemaRecord.columnVector))[j].type;
			tobj.schAttr[j].isUUID = false;
			tobj2.schAttr[j].isUUID = false;
			tobj.schAttr[j].isFILE = false;
			tobj2.schAttr[j].isFILE = false;
			tobj.schAttr[j].isAscending = false;
			tobj2.schAttr[j].isAscending = false;
			
			if ( tobj.numKeys == 0 && !tobj.schAttr[j].isKey ) {
				tobj.numKeys = j;
				tobj2.numKeys = j;
			}
			
			if ( *((*(tobj.schemaRecord.columnVector))[j].spare+1) == JAG_C_COL_TYPE_UUID_CHAR ) {
				tobj.schAttr[j].isUUID = true;			
				tobj2.schAttr[j].isUUID = true;			
				onearr.pos = j;
				onearr.offset = (*(tobj.schemaRecord.columnVector))[j].offset;
				onearr.length = (*(tobj.schemaRecord.columnVector))[j].length;
				tobj.uuidarr.append(onearr);
				tobj2.uuidarr.append(onearr);
			} else if ( *((*(tobj.schemaRecord.columnVector))[j].spare+1) == JAG_C_COL_TYPE_FILE_CHAR ) {
				tobj.schAttr[j].isFILE = true;	
				tobj2.schAttr[j].isFILE = true;		
				tobj.hasFile = true;
				tobj2.hasFile = true;
				onearr.offset = (*(tobj.schemaRecord.columnVector))[j].offset;
				onearr.length = (*(tobj.schemaRecord.columnVector))[j].length;
			}

			if ( *((*(tobj.schemaRecord.columnVector))[j].spare+2) == JAG_ASC ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+2) == JAG_DESC ) {
				tobj.schAttr[j].isAscending = true;
				tobj2.schAttr[j].isAscending = true;
			}
			
			if ( *((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFUPDATETIME ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFUPDATE ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFDATETIME ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFDATE ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_UPDATETIME ||
				*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_UPDDATE ) {
				onearr.pos = j;
				onearr.offset = (*(tobj.schemaRecord.columnVector))[j].offset;
				onearr.length = (*(tobj.schemaRecord.columnVector))[j].length;
				tobj.deftimestamparr.append(onearr);
				tobj2.deftimestamparr.append(onearr);
				if ( *((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFUPDATETIME ||
					 *((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFUPDATE ) {
					tobj.defUpdDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
					tobj2.defUpdDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
				} else if ( *((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFDATETIME ||
							*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_DEFDATE ) {
					tobj.defDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
					tobj2.defDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
				} else if ( *((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_UPDATETIME ||
							*((*(tobj.schemaRecord.columnVector))[j].spare+4) == JAG_CREATE_UPDDATE ) {
					tobj.updDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
					tobj2.updDatetime = *((*(tobj.schemaRecord.columnVector))[j].spare+4);
				}
			}	
		}
		
		if ( tobj.numKeys == 0 ) {
			tobj.numKeys = tobj.numCols;
			tobj2.numKeys = tobj2.numCols;
		}

		if ( sp2.length() == 2 ) {
			tobj.dbobjnum = 2;
			_schemaMap->addKeyValue( s, tobj );
			// prt(("c2390 sp2.length() == 2 addKeyValue s=[%s]  tobj=[%d]\n", s, tobj.numCols ));
		} else if ( sp2.length() == 3 ) {
			tobj2.dbobjnum = 3;
			_schemaMap->addKeyValue( s, tobj2 );
			// prt(("c2391 sp2.length() == 2 addKeyValue s=[%s]  tobj2=[%d]\n", s, tobj2.numCols ));

			dbidx = AbaxString(sp2[0]) + "." + sp2[2];
			tobj.dbobjnum = 2;
			_schemaMap->addKeyValue( dbidx, tobj );
			// prt(("c2392 sp2.length() == 2 addKeyValue dbidx=[%s]  tobj=[%d]\n", dbidx.c_str(), tobj.numCols ));
		}

	}
	// _schemaMap->printKeyStringOnly();
}

// serialize Jstr from schemaMap, only called by servers
// only get db.tab parts and db.tab.idx parts, ignore db.idx parts ( same as db.tab.idx parts )
void JaguarCPPClient::getSchemaMapInfo( Jstr &schemaInfo )
{
	_mapLock->readLock( -1 );
	if ( _isparent && _schemaMap ) {
		const AbaxPair<AbaxString, JagTableOrIndexAttrs> *arr = _schemaMap->array();
		abaxint len = _schemaMap->arrayLength();
		if ( len >= 1 ) {
			for ( int i = 0; i < len; ++i ) {
				if ( arr[i].value.schemaString.size() > 0 ) {
					if ( ( 2 == arr[i].value.dbobjnum && arr[i].value.indexName.size() < 1 ) ||
						 ( 3 == arr[i].value.dbobjnum && arr[i].value.indexName.size() > 0 ) ) {
						schemaInfo += Jstr( arr[i].key.c_str() ) + ":" + arr[i].value.schemaString.c_str() + "\n";
						// prt(("c3039 key=[%s]\n", arr[i].key.c_str() ));
					}
				}
			}
		}
	}
	_mapLock->readUnlock( -1 );
	// prt(("c0393 getSchemaMapInfo schemaInfo=[%s]\n", schemaInfo.c_str() ));
}

// rebuild host connections --  if exclusive admin applies addCluster command, must be called by parent
void JaguarCPPClient::rebuildHostsConnections( const char *msg, abaxint len )
{
	_mapLock->writeLock( -1 );
	if ( !_isparent || _isExclusive ) {
		_hostUpdateString = "";
		_mapLock->writeUnlock( -1 );
		return;
	}
	// if len < primaryHostString.length()
	// else if first primaryHostString.length() bytes of msg is not the same as primaryHostString, invalid
	if ( len < _primaryHostString.length() || 0 != strncmp( _primaryHostString.c_str(), msg, _primaryHostString.length() ) ) {
		// prt(("E6110 Error newhost string, ignore [%s] len=%d, length=%d msg=[%s] %d\n", _primaryHostString.c_str(), len, _primaryHostString.length(), msg, strncmp( _primaryHostString.c_str(), msg, _primaryHostString.length() )));
		_hostUpdateString = "";
		_mapLock->writeUnlock( -1 );
		return;
	}
	JagStrSplit sp( msg+_primaryHostString.length(), '#', true );
	JagVector<Jstr> newhosts;
	for ( int i = 0; i < sp.length(); ++i ) {
		JagVector<Jstr> chosts;
		JagStrSplit sp2( sp[i], '|', true );
		for ( int j = 0; j < sp2.length(); ++j ) {
			++_numHosts;
			chosts.append( sp2[j] );
			newhosts.append( sp2[j] );
			_allHosts->append( sp2[j] );
			_hostIdxMap->addKeyValue( AbaxString(sp2[j]), j );
			_clusterIdxMap->addKeyValue( AbaxString(sp2[j]), _numClusters+i );
		}
		_allHostsByCluster->append( chosts );
	}
	_numClusters = _allHostsByCluster->size();
	_primaryHostString = msg;
	
	// then, try to make new connections with newly added hosts
	
	JaguarCPPClient *jcli;
	int connsuccess;
	for ( int i = 0; i < newhosts.size(); ++i ) {
		connsuccess = false;
		jcli = new JaguarCPPClient();
		jcli->setDebug( _debug );
		jcli->_parentCli = this;
		connsuccess = jcli->connect( newhosts[i].c_str(), _port, _username.c_str(), _password.c_str(), _dbname.c_str(),
			_unixSocket.c_str(), JAG_CLI_CHILD );
		if ( !connsuccess && _faultToleranceCopy > 1 && !_isExclusive ) {
			// connection failure, main connection is unreachable, try to connect backup servers if has replications
			JagVector<Jstr> hostlist;
			jcli->getReplicateHostList( hostlist );
			if ( jcli->_jpb->setConnAttributes( _faultToleranceCopy, _deltaRecoverConnection, _port, _clientFlag, _fromServ,
				hostlist, _username, _password, _dbname, _unixSocket ) ) {
				jcli->_jpb->_conn[0]._sock = INVALID_SOCKET;
				connsuccess = true;
			}
		}
		
		if ( !connsuccess ) {
			delete jcli;
			jcli = NULL;
			if ( _fullConnectionArg ) {
				// need full connection, if one connection is broken, error return
				// prt(("c3939 Error connection to [%s:%d] during rebuild hostlist\n", newhosts[i].c_str(), _port));
				_hostUpdateString = "";
				_mapLock->writeUnlock( -1 );
				return;
			}
		} else {
			if ( _debug ) {
				prt(("c1129 jag_hash_insert_str_void(%s) ...\n", newhosts[i].c_str() ));
			}
			jag_hash_insert_str_void ( &_connMap, newhosts[i].c_str(), (void*)jcli );
		}
	}
	_hostUpdateString = "";
	_mapLock->writeUnlock( -1 );
}

// object method to find server using given key
// if isWrite, use last cluster to hash and find server
// otherwise, hash and find one server in each cluster
// called by parentCli
// mode 0: one node of each cluster; 
// mode 1: one node of each first n-1 cluster; mode 2: one node of cluster n; mode 3: one node of cluster n-1 and n
int JaguarCPPClient::getUsingHosts( JagVector<Jstr> &hosts, const JagFixString &kstr, int mode ) 
{
	abaxint hashCode = kstr.hashCode();
	abaxint 	   idx, len;
	Jstr host;

	if ( 0 && _debug ) { 
		prt(("c0253 kstr:\n" ));
		kstr.dump();
		prt(("c0294 kstr hashCode=%lld\n", hashCode ));
		prt(("c1090 getUsingHosts _allHostsByCluster->length=%d  _numClusters=%d _servCurrentCluster=%d\n",
				    _allHostsByCluster->length(), _numClusters, _servCurrentCluster )); 
	}

	if ( 0 == mode ) {
		// isRead command ( point query ), hash and find one server from each cluster
		for ( int i = 0; i < _allHostsByCluster->length(); ++i ) {
			len = (*(_allHostsByCluster))[i].length();
			idx = hashCode%len;
			host = (*(_allHostsByCluster))[i][idx];
			hosts.append( host );
			if ( _debug ) { prt(("c2000 getUsingHosts mode=0 i=%d len=%d idx=%d host=[%s]\n", i, len, idx, host.c_str() )); }
		}
	} else if ( 1 == mode ) {
		// isWrite command ( cinsert check insert ), hash and find one server from each first n-1 clusters
		for ( int i = 0; i < _allHostsByCluster->length()-1; ++i ) {
			len = (*(_allHostsByCluster))[i].length();
			idx = hashCode%len;
			host = (*(_allHostsByCluster))[i][idx];
			hosts.append( host );
			if ( _debug ) { prt(("c2001 getUsingHosts mode=1 i=%d len=%d idx=%d host=[%s]\n", i, len, idx, host.c_str() )); }
		}
	} else if ( 2 == mode ) {
		// isWrite command ( insert ), hash and find one server from last cluster n
		len = (*(_allHostsByCluster))[_numClusters-1].length();
		idx = hashCode%len;
		host = (*(_allHostsByCluster))[_numClusters-1][idx];
		hosts.append( host );
		if ( _debug ) { prt(("c2002 getUsingHosts mode=2 len=%d idx=%d host=[%s]\n", len, idx, host.c_str() )); }
	} else if ( 3 == mode ) {
		// isWrite command ( dinsert delete insert ), hash and find one server from each last two n-1 and n clusters
		if ( _numClusters-_servCurrentCluster-1 >= 2 ) {
			len = (*(_allHostsByCluster))[_numClusters-2].length();
			idx = hashCode%len;
			host = (*(_allHostsByCluster))[_numClusters-2][idx];
			hosts.append( host );
			if ( _debug ) { prt(("c2003-1 getUsingHosts mode=3 len=%d idx=%d host=[%s]\n", len, idx, host.c_str() )); }
		}
		len = (*(_allHostsByCluster))[_numClusters-1].length();
		idx = hashCode%len;
		host = (*(_allHostsByCluster))[_numClusters-1][idx];
		hosts.append( host );
		if ( _debug ) { prt(("c2003-2 getUsingHosts mode=3 len=%d idx=%d host=[%s]\n", len, idx, host.c_str() )); }
	} else {
		if ( _debug ) { prt(("c4084 else\n" )); }
	}
	// prt(("c2939 kstr=[%s] hashed to server=%d(%s)\n", kstr.c_str(), servnum, host.c_str() ));
	return 1;
}

bool JaguarCPPClient::getSQLCommand( Jstr &sqlcmd, int echo, FILE *fp, int saveNewline )
{
	sqlcmd = "";
	char *cmdline = NULL; 
	char *p;
	size_t  sz, len;
	bool first = 1;
	int  newlen;

	while ( 1 ) {
		if ( jaggetline( (char**)&cmdline, &sz, fp ) == -1 ) {
			return false;
		}

		//prt(("c7762 cmdline=[%s]\n", cmdline ));
		p = cmdline;
		if ( p ) { while ( isspace(*p) ) ++p; }
		if ( 0==strncasecmp( p, "quit", 4 ) ||
		     0==strncasecmp( p, "exit", 4 ) ) {
			 stripStrEnd( (char*)p, strlen(p) );
    		 sqlcmd += p;
			 jagfree( cmdline );
			 return true;
		}

		if ( echo ) { printf("%s", cmdline ); }
		if ( '#' == cmdline[0] ) {
			if ( cmdline ) free( cmdline );
			cmdline = NULL;
			continue;
		}

		len = strlen( cmdline );
		if ( saveNewline ) {
			// prt(("c3312 cmdline=[%s] strlen=%d\n", cmdline, strlen( cmdline)  ));
			if ( ';' == cmdline[0] ) {
				if ( 2 == len && ( '\n' == cmdline[1] || '\r' == cmdline[1] ) ) {
					if ( cmdline ) free( cmdline );
					return true;
				} else if ( 3 == len && '\r' == cmdline[1] && '\n' == cmdline[2] ) {
					if ( cmdline ) free( cmdline );
					return true;
				}
			}

			if ( first ) {
				if ( '\r' == cmdline[0] || '\n' ==  cmdline[0] ) {
					if ( cmdline ) free( cmdline );
					return true;
				}
			}
		}

		
		if ( saveNewline ) {
			newlen = len;
		} else {
			// strip \r \n at end of cmdline
			newlen = stripStrEnd( cmdline, len );
		}

		if ( first ) {
			first = false;
			if ( newlen < 1 ) {
				if ( cmdline ) free( cmdline );
				return true;
			}
		}

		if ( len < 1 ) {
			if ( cmdline ) free( cmdline );
			cmdline = NULL;
			continue;
		}


		len = strlen( cmdline );
		if ( ! saveNewline ) {
    		if (  trimEndWithChar ( cmdline, len, ';' ) ) {
    			sqlcmd += cmdline;
    			if ( cmdline ) free( cmdline );
    			return true;
    		}
		} else {
    		if (  trimEndWithCharKeepNewline ( cmdline, len, ';' ) ) {
    			sqlcmd += cmdline;
				// prt(("s1169 sqlcmd=[%s]\n", sqlcmd.c_str() ));
    			if ( cmdline ) free( cmdline );
    			return true;
    		}
		}

		if ( sqlcmd.size() < 1 && *cmdline=='\n' ) {
			// skip first \n
		} else {
			sqlcmd += cmdline;
		}

		if ( cmdline ) free( cmdline );
		cmdline = NULL;
	}
}

// static
// send cmd to server and wait for reply
void *JaguarCPPClient::pingServer( void *ptr )
{
	CliPass *pass = (CliPass*)ptr;
	JaguarCPPClient *cli = pass->cli;
	cli->queryDirect(pass->cmd.c_str(), true, false, true );
	while ( cli->reply() ) {}
	delete pass;
	return NULL;
}

// object method
void JaguarCPPClient::setWalLog( bool flag )
{
	_walLog = flag;
}

// append insert to log _inserLog
void JaguarCPPClient::appendToLog( const Jstr &querys )
{
	if ( ! _walLog ) return;

	if ( ! _insertLog ) {
    	Jstr fpath = JagFileMgr::makeLocalLogDir("cmd/") + intToString( _sock );
		jagmkdir( fpath.c_str(), 0700 );
		fpath += "/clientinsert-"  + longToStr( abaxint(this) ) + "-" + longToString( JagTime::mtime() ) + ".log";
		_logFPath = fpath;
		_insertLog = jagfopen( fpath.c_str(), "wb" );
		if ( ! _insertLog ) {
			printf("Error open [%s] for write log. No logging is done\n",  fpath.c_str() );
			return;
		}
	}

	if ( _lastHasSemicolon ) {
		fprintf( _insertLog, "%s", querys.c_str() );
	} else {
		fprintf( _insertLog, ";%s", querys.c_str() );
	}

	if ( endWhiteWith( querys, ';' ) ) {
		_lastHasSemicolon = true;
	} else {
		_lastHasSemicolon = false;
	}
}

void JaguarCPPClient::resetLog()
{
	if ( ! _walLog ) return;

	if ( _insertLog ) { jagfclose( _insertLog ); }
	// printf("c7309 unlink [%s]\n",  _logFPath.c_str() );
	jagunlink( _logFPath.c_str() );

   	Jstr fpath = JagFileMgr::makeLocalLogDir("cmd/") + intToString( _sock );
	jagmkdir( fpath.c_str(), 0700 );
	fpath += "/clientinsert-"  + longToStr( abaxint(this) ) + "-" + longToString( JagTime::mtime() ) + ".log";
	_logFPath = fpath;
	_insertLog = jagfopen( fpath.c_str(), "wb" );
}

// load commands in log file
// fpath is full path file name of log file
// "/home/jaguar/jaguar/log/cmd/3/clientinsert-140733714918848-1466193562011.log"
abaxint JaguarCPPClient::redoLog( const Jstr &fpath )
{
	// printf("c0032 redoLog [%s]\n", fpath.c_str() );

	if ( ! _walLog ) { return 0; }
	FILE *insertLog = jagfopen( fpath.c_str(), "rb" );
	if ( ! insertLog ) { return 0; }

	JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> qmap;

	Jstr sqlcmd;
	abaxint cnt = 0;
	abaxint onecnt = 0;
	// printf("c3049 getSQLCommand ...\n");
	while ( getSQLCommand( sqlcmd, false, insertLog ) ) {
		onecnt += oneCmdInsertPool( qmap, sqlcmd ); 
		++cnt;
		if ( onecnt > CLIENT_SEND_BATCH_RECORDS-1 ) {
			flushQMap( qmap );
			onecnt = 0;
		}
	}
	if ( onecnt > 0 ) cnt += flushQMap( qmap );

	jagfclose( insertLog );
	// printf("c3839 unlink [%s]\n", fpath.c_str() );
	jagunlink( fpath.c_str() );
	return cnt;
}

void JaguarCPPClient::flushInsertBuffer()
{
	// update schema map if available
	if ( _schemaUpdateString.size() > 0 ) {
		_mapLock->writeLock( -1 );
		rebuildSchemaMap();
		updateSchemaMap( _schemaUpdateString.c_str() );
		_mapLock->writeUnlock( -1 );
		_schemaUpdateString = "";
	}

	/**
	if ( _defvalUpdateString.size() > 0 ) {
		_mapLock->writeLock( -1 );
		updateDefvalMap( _defvalUpdateString.c_str() );
		_mapLock->writeUnlock( -1 );
		_defvalUpdateString = "";
	}
	***/


	// prt(("c3000 flushinsertbuffer this=%0x thrd=%lld...\n", this, THREADID ));
	_isFlushingInsertBuffer = 1;
	JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> qmap;

	// JagClock clock;
	// clock.start();
 	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	if ( _insertBufferCopy ) { delete _insertBufferCopy; _insertBufferCopy = NULL; }
	_insertBufferCopy = _insertBuffer;
	_insertBuffer = new JagVector<Jstr>();
	mutex.writeUnlock();
	// clock.stop();
	// prt(("c3001 %lld copy/clean insertBuffer %lld recs took %d millisecs\n", THREADID, _insertBufferCopy->size(), clock.elapsed() ));

	if ( _insertBufferCopy->size() < 1 ) {
		delete _insertBufferCopy; _insertBufferCopy = NULL;
		_isFlushingInsertBuffer = 0;
		return;
	}

	// clock.start();
	abaxint onecnt = 0;
	for ( int i = 0; i < _insertBufferCopy->size(); ++i ) {
		onecnt += oneCmdInsertPool( qmap, (*(_insertBufferCopy))[i] );
		if ( onecnt > CLIENT_SEND_BATCH_RECORDS-1 ) {
			flushQMap( qmap );
			onecnt = 0;
		}
	}
	// clock.stop();
	// prt(("c3002 %lld insert %d recs to pool took %d millisecs\n", THREADID, _insertBufferCopy->size(), clock.elapsed() ));

	// clock.start();
	if ( onecnt > 0 ) flushQMap( qmap );
	// clock.stop();
	// prt(("c3003 %lld flushQMap %lld cmds took %d millisecs\n", THREADID, cnt, clock.elapsed() ));
	_isFlushingInsertBuffer = 0;
	return;
}

// format insert cmd and cinsert cmd to qmap
// return 0: error; return 1: success
int JaguarCPPClient::oneCmdInsertPool( JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> &qmap, const Jstr &cmd )
{
	int rc;
	bool exist;
	JagParseParam pparam;
	Jstr errmsg;
	JaguarCPPClient *tcli;
	JagVector<JagDBPair> cmdhosts;
	//JagClock clock; clock.start();	
	rc = getParseInfo( cmd, pparam, errmsg );
	if ( ! rc ) {
		prt(("Parse error: [%s] [%s]\n", cmd.c_str(), errmsg.c_str() ));
		return 0;
	}	
	rc = processInsertCommands( cmdhosts, pparam, errmsg );
	if ( ! rc ) {
		prt(("Process insert error: [%s] [%s]\n", cmd.c_str(), errmsg.c_str() ));
		return 0;
	}
	//clock.stop(); prt(("c8381 processInsertCommands took %d usecs\n", clock.elapsedusec()));	
	
	rc = 0;
	for ( int i = 0; i < cmdhosts.size(); ++i ) {
		if ( _debug ) { prt(("c1138 jag_hash_lookup(%s) ...\n", cmdhosts[i].value.c_str() )); }
		tcli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, cmdhosts[i].value.c_str() );
		AbaxPair<AbaxString,AbaxBuffer> &pair = qmap.getValue( tcli->_host, exist );
		if ( exist ) {
			pair.key += cmdhosts[i].key.c_str();
			pair.value = (void*)tcli;
		} else {
			AbaxPair<AbaxString,AbaxBuffer> emptyPair;
			emptyPair.key = cmdhosts[i].key.c_str();
			emptyPair.value = (void*)tcli;
			qmap.addKeyValue( tcli->_host, emptyPair );
		}
		++rc;
	}
	return rc;
}

// object method, flush qmap
abaxint JaguarCPPClient::flushQMap( JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer>> &qmap )
{
	thpool_wait( _insertPool );
	abaxint len = qmap.arrayLength();
	const AbaxPair<AbaxString, AbaxPair<AbaxString, AbaxBuffer> > *arr = qmap.array();
	abaxint cnt = 0;
	for ( abaxint i = 0; i < len; ++i ) {
		if ( qmap.isNull(i) ) continue;
		const AbaxPair<AbaxString, AbaxPair<AbaxString, AbaxBuffer> > &pair = arr[i];
		CliPass *bp = new CliPass();
		bp->cli = (JaguarCPPClient*)pair.value.value.value();
		bp->cmd = pair.value.key.c_str();
		thpool_add_work( _insertPool , batchStatic, (void*)bp );
		++cnt;
    }

	if ( cnt > 0 ) {
    	thpool_wait( _insertPool );
	}

	qmap.removeAllKey();
	return cnt;
}

// must have sent query before, send signal bytes, e.g. "OK" || "ON" || "NG"
int JaguarCPPClient::sendTwoBytes( const char *condition )
{
	int rc;
	if ( _useJPB ) {
		rc = _jpb->sendTwoBytes( condition );
	} else {
		char twobuf[JAG_SOCK_MSG_HDR_LEN+3];
		// sprintf( twobuf, "%0*lldACC", JAG_SOCK_MSG_HDR_LEN-3, 2 );
		sprintf( twobuf, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, 2 );
		twobuf[JAG_SOCK_MSG_HDR_LEN] = *condition;
		twobuf[JAG_SOCK_MSG_HDR_LEN+1] = *(condition+1);
		twobuf[JAG_SOCK_MSG_HDR_LEN+2] = '\0';
		rc = sendData( _sock, twobuf, JAG_SOCK_MSG_HDR_LEN+2 );
	}

	return rc;
}

int JaguarCPPClient::recvTwoBytes( char *condition )
{
	int rc;
	_debug && prt(("c7301 recvTwoBytes ...\n" ));

	if ( _useJPB ) {
		rc = _jpb->recvTwoBytes( condition );
		return rc;
	} 
	
	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *buf = NULL;
	condition[0] = 'N'; condition[1] = 'G';
	abaxint hbs = 0;
	while ( 1 ) {
		_debug && prt(("c2038 recvData ...\n" ));
		rc = recvData( _sock, hdr, buf );
		_debug && prt(("c2839 hdr=[%s] buf=[%s]\n", hdr, buf ));
		if ( rc < 0 ) {
			break;
		} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) {
			// HB ignore
			++hbs; if ( hbs > 1000000000 ) hbs = 0;
			if ( _debug && ( (hbs%100) == 0 ) ) {
				prt(("c1280 JaguarCPPClient::recvTwoBytes() recevd too many HB\n" ));
			}
			continue;
		} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'S' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'S' && *buf == 'O' && *(buf+1) == 'K' ) {
			condition[0] = 'O'; condition[1] = 'K';
			break;
		} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'S' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'S' && *buf == 'N' && *(buf+1) == 'G' ) {
			condition[0] = 'N'; condition[1] = 'G';
			break;
		}
	}
	if ( buf ) { free( buf ); buf = NULL; }
	_debug && prt(("c7301 recvTwoBytes done rc=%d\n", rc ));
	return rc;
}

void *JaguarCPPClient::broadcastAllRegularStatic( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	Jstr  rowStr;
	
	pass->cli->queryDirect( pass->cmd.c_str(), true, false );
	while ( pass->cli->reply() ) {
		if ( pass->passMode % 2 == 1 && pass->cli == pass->cli->_parentCli ) {
			pass->cli->doPrintRow( false, rowStr );
		}
	}
	return NULL;
}

// static method to broadcast special commands to all server, and waiting for all servers' reply 
// if any of them is failure, rollback and reject what has been down on successful server(s)
// passMode: 0 no reply; 1 one reply; 2 handshake no reply; 3 handshake one reply
// send query to client, and receiving sync schema check result
void *JaguarCPPClient::broadcastAllRejectFailureStatic1( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	pass->cli->_debug &&prt(("c0188 broadcastAllRejectFailureStatic1() queryDirect(%s) ...\n", pass->cmd.c_str() ));
	int rc = pass->cli->queryDirect( pass->cmd.c_str(), true, false );
	pass->cli->_debug && prt(("c0188 broadcastAllRejectFailureStatic1() queryDirect(%s) done rc=%d\n", pass->cmd.c_str(), rc ));
	char condition[3] = {'N', 'G', '\0'};
	pass->cli->_debug && prt(("c0273 broadcastAllRejectFailureStatic1 cmd=[%s] recvTwoBytes ...\n", pass->cmd.c_str() ));
	rc = pass->cli->recvTwoBytes( condition );
	pass->cli->_debug && prt(("c0213 broadcastAllRejectFailureStatic1 recvTwoBytes condition=[%s] rc=%d\n", condition, rc ));
	if ( condition[0] != 'O' || condition[1] != 'K' ) {
		++pass->cli->_parentCli->_spCommandErrorCnt;
	    // prt(("c5081 _spCommandErrorCnt=%d cond=[%s]\n", (int)pass->cli->_parentCli->_spCommandErrorCnt, condition ));
		// pass->isContinue = false;

		// jaguar_mutex_unlock ( &pass->cli->_queryMutex );
		// prt(("c1438 cli=%0x jaguar_mutex_unlock _queryMutex done\n", pass->cli ));
	}
	return NULL;
}
// send condition to server, for all servers' sync schema check result
void *JaguarCPPClient::broadcastAllRejectFailureStatic2( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	if ( pass->cli->_parentCli->_spCommandErrorCnt > 0 ) {
		pass->cli->sendTwoBytes( "NG" );
		pass->isContinue = false;
	} else {
		pass->cli->sendTwoBytes( "OK" );
		pass->isContinue = true;
	}
	return NULL;
}
// receiving query condition check on each server's result
void *JaguarCPPClient::broadcastAllRejectFailureStatic3( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	// again, repeat the above procedure to make sure the only one command can be done ( or error out )
	char condition[3] = {'N', 'G', '\0'};
	if ( pass->isContinue ) {
		// prt(("c0693 isContinue recvTwoBytes() ...\n" ));
		int rc = pass->cli->recvTwoBytes( condition );
		// prt(("c0593 isContinue recvTwoBytes() done condition=[%s]\n", condition ));
		// prt(("c3448 rc=%d condition=[%s]\n", rc, condition ));
		if ( condition[0] != 'O' || condition[1] != 'K' ) {
			++pass->cli->_parentCli->_spCommandErrorCnt;
			// prt(("c1418 cli=%0x jaguar_mutex_unlock _queryMutex done\n", pass->cli ));
		}
	} else {
		// must drain socket, server should send OK
		// prt(("c0493 in broadcastAllRejectFailureStatic3 NOT isContinue recvTwoBytes() done condition=[%s]\n", condition ));
		// prt(("c1413 cli=%0x jaguar_mutex_unlock _queryMutex done\n", pass->cli ));
	}
	return NULL;


}
// send condition to server, for all servers' query condition check result
// also receive _ENDMSG_ from server to check if need to request one server to do dataCenter or not
// send "OK", do query and dataCenter; send "ON", do query but no dataCenter; send "NG", do nothing
void *JaguarCPPClient::broadcastAllRejectFailureStatic4( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	int rc = 0;
	Jstr rowStr;
	if ( pass->isContinue ) {
		if ( pass->cli->_parentCli->_spCommandErrorCnt > 0 ) {
			rc = pass->cli->sendTwoBytes( "NG" );
		} else {
			if ( pass->syncDataCenter ) {
				rc = pass->cli->sendTwoBytes( "OK" );
			} else {
				rc = pass->cli->sendTwoBytes( "ON" );
			}
		}

		// prt(("c1029 after sendTwoBytes, send2B rc=%d replyAll() ...\n", rc ));
		while ( pass->cli->reply() ) {
			if ( pass->passMode % 2 == 1 && pass->cli == pass->cli->_parentCli ) {
				pass->cli->doPrintRow( false, rowStr );
			}
		}

		// prt(("c1029 after replyAll() _tcode=%d\n", pass->cli->_tcode ));
		if ( 777 == pass->cli->_tcode ) {
			// not to do data center for next server
			pass->syncDataCenter = false;			
		} // else no change
	} else {
		// prt(("c0238 sendTwoBytes NG ..\n"));
		pass->cli->replyAll();
	}

	return NULL;

}

// static method to broadcast select commands to all servers, and get data back
void *JaguarCPPClient::broadcastAllSelectStatic( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	pass->cli->_debug && prt(("c0382 broadcastAllSelectStatic pass->isContinue=%d \n", pass->isContinue ));

	if ( pass->isContinue ) {
		const char *p;
		abaxint rc, datalen;
		pthread_t tid = THREADID;
		pass->cli->queryDirect( pass->cmd.c_str(), true, false );
		if ( pass->parseParam->opcode == JAG_GETFILE_OP && pass->parseParam->getfileActualData &&
			pass->cli->_parentCli->_datcSrcType == JAG_DATACENTER_GATE ) {
			if ( !pass->cli->_parentCli->_thrdCliMap->keyExist( tid ) ) {
				pass->cli->_parentCli->_thrdCliMap->addKeyValue( tid, AbaxBuffer((void*)pass->cli) );
			}
			pass->noQueryButReply = true;
			pass->isContinue = false;
		} else {
			rc = pass->cli->reply( true );
			if ( ! rc || ! pass->cli->_row ) {
				if ( pass->cli->_debug ) { prt(("c9013 rc=%d pass->cli->_row=%0x\n", rc, pass->cli->_row )); }
				pass->isContinue = false;
			} else {
				if ( pass->cli->_debug )  { prt(("c2349 pass->cli->_row->type=[%c]\n", pass->cli->_row->type )); }
				if ( pass->cli->_row->type == '1' ) {
					// point query data, or getfile data ( must be point query )
					pass->cli->_pointQueryString = JagFixString(pass->cli->_row->data, pass->cli->_row->datalen);
					pass->cli->_debug && prt(("c3849 pass->cli->_pointQueryString=[%s]\n", pass->cli->_pointQueryString.c_str() )); 
					if ( pass->parseParam->opcode == JAG_GETFILE_OP && pass->parseParam->getfileActualData ) {
						/***
						if ( !pass->cli->_parentCli->_thrdCliMap->keyExist( tid ) ) {
							pass->cli->_parentCli->_thrdCliMap->addKeyValue( tid, AbaxBuffer((void*)pass->cli) );
						}
						***/
						pass->cli->_debug && prt(("c8132 recvFilesFromServer ...\n" ));
						pass->cli->recvFilesFromServer( pass->parseParam );
						pass->cli->_debug && prt(("c8132 recvFilesFromServer done ...\n" ));
						pass->cli->_pointQueryString = "";
					}
					if ( pass->cli->_debug ) { prt(("c83830 replyall...\n" )); }
					while ( pass->cli->reply( true ) ) { 
						if ( pass->cli->getMessage() ) {
							prt(("c5802 getmsg=[%s]\n", pass->cli->getMessage() ));
						} 
					}
					if ( pass->cli->_debug ) { prt(("c83830 replyall done...\n" )); }
					pass->isContinue = false;
				} else if ( pass->cli->_row->type == 'E' ) {
					// select range query is timeout, error message
					JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
					if ( pass->cli->_parentCli->_selectTruncateMsg.size() < 1 ) {
						pass->cli->_parentCli->_selectTruncateMsg = pass->cli->_replyerrmsg;
						// pass->cli->_parentCli->_selectTruncateMsg += pass->cli->_replyerrmsg + "\n";
					}
					mutex.writeUnlock();
					if ( ! pass->cli->reply( true ) ) {
						pass->isContinue = false;
					}
				} else if ( pass->cli->_row->type != 'I' ) {
					while ( pass->cli->reply( true ) ) { }
					pass->isContinue = false;
				} else {
					// if ( pass->cli->_debug ) prt(("c3384 else pass->cli->_row->type=[%c] \n", pass->cli->_row->type ));
				}

				if ( pass->isContinue ) {
					// for select count(*) or select range dataAggregate count
					if ( pass->cli->_debug ) { prt(("c9313 isContinue selectMode=%d\n", pass->selectMode )); }
					p = pass->cli->_row->data;
					if ( 3 == pass->selectMode ) {
						// for select count(*)
						pass->cnt = jagatoll( p );
						while ( pass->cli->reply( true ) ) {};
						pass->isContinue = false;
					}

					if ( pass->isContinue ) {
						// if join cmd, get each table elements number and calculate total num to see if need hash or not
						if ( pass->joinEachCnt && !pass->isToGate ) {
							JagStrSplit jsp( p, '|', true );
							JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
							for ( abaxint i = 0; i < jsp.length(); ++i ) {
								(pass->joinEachCnt)[i] += jagatoll( jsp[i].c_str() );
							}
							mutex.writeUnlock();
						}
					}
				}
			}
		}
	}

	if ( pass->cli->_debug ) { prt(("c7002  begin static 2...\n" )); }
	// check to begin static 2
	if ( pass->totnum > 1 ) {
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		++pass->cli->_parentCli->_multiThreadMidPointStop;
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 1 ) {
			jagsleep( 1, JAG_MSEC );
		}
		--pass->cli->_parentCli->_multiThreadMidPointStop;
	}
	broadcastAllSelectStatic2( ptr );
	if ( pass->cli->_debug ) { prt(("c7002  begin static 3...\n" )); }
	// check to begin static 3
	if ( pass->totnum > 1 ) {
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		++pass->cli->_parentCli->_multiThreadMidPointStop;
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 1 ) {
			jagsleep( 1, JAG_MSEC );
		}
		--pass->cli->_parentCli->_multiThreadMidPointStop;
	}
	broadcastAllSelectStatic3( ptr );
	if ( pass->cli->_debug ) { prt(("c7002  begin static 4...\n" )); }
	// check to begin static 4
	if ( pass->totnum > 1 ) {
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		++pass->cli->_parentCli->_multiThreadMidPointStop;
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 1 ) {
			jagsleep( 1, JAG_MSEC );
		}
		--pass->cli->_parentCli->_multiThreadMidPointStop;
	}
	broadcastAllSelectStatic4( ptr );
	// check to begin static 5
	if ( pass->cli->_debug ) { prt(("c7002  begin static 5...\n" )); }
	if ( pass->totnum > 1 ) {
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		++pass->cli->_parentCli->_multiThreadMidPointStop;
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 1 ) {
			jagsleep( 1, JAG_MSEC );
		}
		--pass->cli->_parentCli->_multiThreadMidPointStop;
	}
	broadcastAllSelectStatic5( ptr );
	// check to begin static 6
	if ( pass->cli->_debug ) { prt(("c7002  begin static 6...\n" )); }
	if ( pass->totnum > 1 ) {
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		++pass->cli->_parentCli->_multiThreadMidPointStop;
		while ( pass->cli->_parentCli->_multiThreadMidPointStart != 1 ) {
			jagsleep( 1, JAG_MSEC );
		}
		--pass->cli->_parentCli->_multiThreadMidPointStop;
	}
	broadcastAllSelectStatic6( ptr );
	if ( pass->cli->_debug ) { prt(("c7002  end\n" )); }

	return NULL;
}
void *JaguarCPPClient::broadcastAllSelectStatic2( void * ptr ) 
{
	CliPass *pass = (CliPass*)ptr;
	if ( pass->isContinue && pass->joinEachCnt && !pass->isToGate ) {
		const char *p;
		abaxint rc, clen;
		// send request buf and receive data
		Jstr tcmd; abaxint hjlimit = pass->cli->_parentCli->_hashJoinLimit;
		if ( (pass->joinEachCnt)[0] > hjlimit && (pass->joinEachCnt)[1] > hjlimit ) {
			tcmd = "-1";
		} else {
			if ( (pass->joinEachCnt)[0] < hjlimit && (pass->joinEachCnt)[1] > hjlimit ) {
				tcmd = "0";
			} else if ( (pass->joinEachCnt)[0] > hjlimit && (pass->joinEachCnt)[1] < hjlimit ) {
				tcmd = "1";
			} else {
				if ( (pass->joinEachCnt)[0] < (pass->joinEachCnt)[1] ) tcmd = "0";
				else tcmd = "1";
			}
		}

		char sbuf2[JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN];
		sprintf( sbuf2, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, SELECT_DATA_REQUEST_LEN );
		memcpy(sbuf2+JAG_SOCK_MSG_HDR_LEN, tcmd.c_str(), SELECT_DATA_REQUEST_LEN);
		clen = sendData( pass->cli->getSocket(), sbuf2, JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN );
		if ( clen < SELECT_DATA_REQUEST_LEN ) {
			pass->isContinue = false;
			return NULL;
		}

		rc = pass->cli->reply( true );
		if ( pass->cli->_row->type == 'E' ) {
			// select range query is timeout, error message
			JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
			if ( pass->cli->_parentCli->_selectTruncateMsg.size() < 1 ) {
				pass->cli->_parentCli->_selectTruncateMsg = pass->cli->_replyerrmsg;
				// pass->cli->_parentCli->_selectTruncateMsg += pass->cli->_replyerrmsg + "\n";
			}
			mutex.writeUnlock();
			if ( ! pass->cli->reply( true ) ) {
				pass->isContinue = false;
				return NULL;
			}
		} else if ( pass->cli->_row->type != 'I' ) {
			// invalid select count(*) or select range query first message, ingore the whole request reply
			while ( pass->cli->reply( true ) ) {};
			pass->isContinue = false;
			return NULL;
		}
		p = pass->cli->_row->data;
		if ( !p ) {
			if ( rc ) while ( pass->cli->reply( true ) ) {};
			pass->isContinue = false;
			return NULL;
		}

		// also, for join, if received "JOINEND", return end with no data
		if ( p && strlen( p ) >= 7 && memcmp( p, "JOINEND", 7 ) == 0 ) {
			pass->recvJoinEnd = true;
		}
	}
	return NULL;
}
void *JaguarCPPClient::broadcastAllSelectStatic3( void * ptr ) 
{
	CliPass *pass = (CliPass*)ptr;
	if ( pass->isContinue ) {
		char spcmdbuf[SELECT_DATA_REQUEST_LEN];
		abaxint clen;
		if ( pass->joinEachCnt && pass->recvJoinEnd && !pass->isToGate ) {
			memset( spcmdbuf, 0, SELECT_DATA_REQUEST_LEN );
			memcpy( spcmdbuf, "JOINEND", 7 );
			char sbuf[JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN];
			sprintf( sbuf, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, SELECT_DATA_REQUEST_LEN );
			memcpy(sbuf+JAG_SOCK_MSG_HDR_LEN, spcmdbuf, SELECT_DATA_REQUEST_LEN);
			clen = sendData( pass->cli->getSocket(), sbuf, JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN );
			while ( pass->cli->reply( true ) ) {};
			pass->isContinue = false;
			return NULL;
		}
	}
	return NULL;
}
void *JaguarCPPClient::broadcastAllSelectStatic4( void * ptr ) 
{
	CliPass *pass = (CliPass*)ptr;
	if ( pass->isContinue ) {
		const char *p;
		abaxint len, datalen;
		p = pass->cli->_row->data;	
		// for select range query dataAggregate count
		if ( pass->cli->_parentCli->_datcSrcType != JAG_DATACENTER_GATE ) {
			// if self is not gate, use and parse the attributes
			JagStrSplit sp( p, '|', true );
			len = jagatoll( sp[0].c_str() ); // # of elements
			datalen = jagatoll(sp[1].c_str());
			// prt(("s9999 lencnt=%lld\n", len ));
			pass->cli->_parentCli->_jda->setdatalen( datalen );  // length of each row
			JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
			for ( abaxint i = pass->idx + 1; i < pass->totnum; ++i ) {
				(*(pass->cli->_parentCli->_selectCountBuffer))[i] += len;
			}
			mutex.writeUnlock();
		} else {
			// otherwise, send direct to client
			JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
			sendDirectToSockWithHdr( pass->redirectSock, pass->cli->_row->hdr, p, pass->cli->_row->datalen );
			mutex.writeUnlock();
		}
	}
	return NULL;
}
void *JaguarCPPClient::broadcastAllSelectStatic5( void * ptr ) 
{
	CliPass *pass = (CliPass*)ptr;
	//prt(("c7393 broadcastAllSelectStatic5 pass->isContinue=%d\n", pass->isContinue ));

	if ( pass->isContinue ) {
		const char *p;
		abaxint clen, len, datalen, prevcnt, slimit, totcnt = 0, totbytes = 0;
		//prt(("c0233 isToGate=%d needAll=%d hasLimi=%d\n", pass->isToGate, pass->needAll, pass->hasLimit ));
		if ( !pass->isToGate ) {
			char spcmdbuf[SELECT_DATA_REQUEST_LEN];
			p = pass->cli->_row->data;
			//prt(("c2031 pass->cli->_row->data=p=[%s]\n", p ));
			// for select range query dataAggregate count
			JagStrSplit sp( p, '|', true );
			len = jagatoll( sp[0].c_str() ); // # of elements
			datalen = jagatoll(sp[1].c_str());
			// totbytes = len * jagatoll(sp[1].c_str());
			totbytes = len * datalen;
			totcnt = (*(pass->cli->_parentCli->_selectCountBuffer))[pass->totnum-1]*datalen;
			// set request buf
			if ( ! pass->hasLimit || pass->needAll ) {
				sprintf( spcmdbuf, "_sendall" );
				// prt(("c0122 spcmdbuf=[%s]\n", spcmdbuf ));
			} else {
				prevcnt = (*(pass->cli->_parentCli->_selectCountBuffer))[pass->idx];
				// prt(("s9999 prevcnt=%lld\n", prevcnt ));
				if ( pass->start == 0 ) { // not has limit start
					if ( prevcnt >= pass->cnt ) {
						sprintf( spcmdbuf, "_discard" );
					} else {
						if ( pass->cnt - prevcnt <= len ) {
							sprintf( spcmdbuf, "_send|0|%lld", pass->cnt - prevcnt );
						} else {
							sprintf( spcmdbuf, "_send|0|%lld", len );
						}
					}
					//prt(("c4119 spcmdbuf=[%s]\n", spcmdbuf ));
				} else { // has limit start
					slimit = pass->start - 1;
					// prt(("s9999 prevcnt=%lld slimit=%lld, limit=%lld, len=%lld\n", prevcnt, slimit, pass->cnt, len ));
					if ( prevcnt <= slimit - len || prevcnt >= slimit + pass->cnt ) {
						sprintf( spcmdbuf, "_discard" );
					} else {
						if ( prevcnt < slimit ) {
							if ( slimit + pass->cnt - prevcnt < len ) {
								sprintf( spcmdbuf, "_send|%lld|%lld", slimit-prevcnt, pass->cnt );
							} else {
								sprintf( spcmdbuf, "_send|%lld|%lld", slimit-prevcnt, prevcnt+len-slimit );
							}
						} else {
							if ( slimit + pass->cnt - prevcnt < len ) {
								sprintf( spcmdbuf, "_send|0|%lld", slimit + pass->cnt - prevcnt );
							} else {
								sprintf( spcmdbuf, "_send|0|%lld", len );
							}
						}
					}
				}
			}
			
			// send request buf and receive data
			char sbuf[JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN];
			sprintf( sbuf, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, SELECT_DATA_REQUEST_LEN );
			memcpy(sbuf+JAG_SOCK_MSG_HDR_LEN, spcmdbuf, SELECT_DATA_REQUEST_LEN);
			// prt(("c8229 sendData sbuf=[%s]\n", sbuf ));
			clen = sendData( pass->cli->getSocket(), sbuf, JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN );
			if ( clen < SELECT_DATA_REQUEST_LEN ) {
				pass->isContinue = false;
				return NULL;
			}
			
			if ( pass->cli->_parentCli->_datcSrcType != JAG_DATACENTER_GATE ) {
				// if self not gate, store data
				abaxint bytesrecived = 0;
				int freeGB, needGB;
				pass->hasError = 0;
				while ( pass->cli->reply( true ) ) {
					if ( pass->cli->_row->type == 'D' ) { // for actual data, write to _jda
						p = pass->cli->_row->data;
						len = pass->cli->_row->datalen;

						bytesrecived += len;
						// check diskspace usage every 100MB is received
						if ( bytesrecived > 100000000 ) { 
							bytesrecived = 0;
							if ( ! pass->cli->hasEnoughDiskSpace( pass->totnum, totbytes, needGB, freeGB ) ) {
								pass->hasError = 1;
								pass->errmsg = "Error: not enough space for the query. Needed: " + 
											   intToStr(needGB) + "G Available: " + intToStr( freeGB ) + "G";
								continue;
							}
						}
						pass->cli->_parentCli->_jda->writeit( pass->cli->_host, p, len, NULL, false, totcnt );
					} else if ( pass->cli->_row->type == 'I' && memcmp( pass->cli->_row->data, "JOINEND", 7 ) == 0 ) {
						pass->recvJoinEnd = true;
						break;
					} else {
						//prt(("c0938 pass->cli->_row->type=[%c]\n", pass->cli->_row->type ));
					}
				}
			} else {			
				// if is GATE, send piece directly to client
				pass->hasError = 0;
				while ( pass->cli->reply( true ) ) {
					if ( pass->cli->_row->type == 'D' || 
					     (pass->cli->_row->type == 'I' && memcmp( pass->cli->_row->data, "JOINEND", 7 ) == 0) ) { 
						p = pass->cli->_row->data;
						len = pass->cli->_row->datalen;
						JagReadWriteMutex mutex( pass->cli->_parentCli->_qrlock, JagReadWriteMutex::WRITE_LOCK );
						sendDirectToSockWithHdr( pass->redirectSock, pass->cli->_row->hdr, p, len );
						mutex.writeUnlock();
						if ( pass->cli->_row->type == 'I' && memcmp( pass->cli->_row->data, "JOINEND", 7 ) == 0 ) {
							pass->recvJoinEnd = true;
							break;
						}
					}
				}
			}
		} else {
			// if connect to GATE, receive only data
			abaxint bytesrecived = 0;
			int freeGB, needGB;
			pass->hasError = 0;
			while ( pass->cli->reply( true ) ) {
				if ( pass->cli->_row->type == 'D' ) { // for actual data, write to _jda
					p = pass->cli->_row->data;
					len = pass->cli->_row->datalen;

					bytesrecived += len;
					// check diskspace usage every 100MB is received
					if ( bytesrecived > 100000000 ) { 
						bytesrecived = 0;
						if ( ! pass->cli->hasEnoughDiskSpace( pass->totnum, totbytes, needGB, freeGB ) ) {
							pass->hasError = 1;
							pass->errmsg = "Error: not enough space for the query. Needed: " + 
										   intToStr(needGB) + "G Available: " + intToStr( freeGB ) + "G";
							continue;
						}
					}
					pass->cli->_parentCli->_jda->writeit( pass->cli->_host, p, len, NULL, false, totcnt );
				} else if ( pass->cli->_row->type == 'I' && memcmp( pass->cli->_row->data, "JOINEND", 7 ) == 0 ) {
					pass->recvJoinEnd = true;
					break;
				}
			}
		}
	}
	return NULL;
}

void *JaguarCPPClient::broadcastAllSelectStatic6( void * ptr )
{
	CliPass *pass = (CliPass*)ptr;
	if ( pass->isContinue && pass->recvJoinEnd && !pass->isToGate ) {
		char spcmdbuf[SELECT_DATA_REQUEST_LEN];
		char sbuf[JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN];
		abaxint clen;
		sprintf( sbuf, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, SELECT_DATA_REQUEST_LEN );
		memset( spcmdbuf, 0, SELECT_DATA_REQUEST_LEN );
		memcpy( spcmdbuf, "JOINEND", 7 );
		memcpy(sbuf+JAG_SOCK_MSG_HDR_LEN, spcmdbuf, SELECT_DATA_REQUEST_LEN);
		clen = sendData( pass->cli->getSocket(), sbuf, JAG_SOCK_MSG_HDR_LEN+SELECT_DATA_REQUEST_LEN );
		while ( pass->cli->reply( true ) ) {}
	} else if ( pass->isToGate ) {
		while ( pass->cli->reply( true ) ) {}
	}
	pass->isContinue = false;
	return NULL;
}

void JaguarCPPClient::setFullConnection( bool flag )
{
	if ( flag ) {
		_fullConnectionArg = 1;
	} else {
		_fullConnectionArg = 0;
	}
}

void JaguarCPPClient::setDebug( bool flag )
{
	if ( flag ) {
		_debug = 1;
	} else {
		_debug = 0;
	}

	if ( _jpb ) {
		if ( _debug ) {
			// prt(("c0233 set debug in _jpb to true\n" ));
			_jpb->_debug = 1;
		}
	}
}

// method to recover delta log
// method for server use only
int JaguarCPPClient::recoverDeltaLog( const Jstr &inpath )
{
	if ( !_isparent ) { return 0; }
	
	JagStrSplit sp( inpath.c_str(), '|' );
	int numfiles = sp.length();
	JagVector<Jstr> fpaths;
	struct stat sbuf;
	for ( int i = 0; i < numfiles; ++i ) {
		if ( JagFileMgr::exist( sp[i].c_str() ) ) {
			stat(sp[i].c_str(), &sbuf);
			if ( sbuf.st_size > 0 ) {
				fpaths.append( sp[i] );
			}
		}
	}
	
	pthread_t thrd[fpaths.size()];
	RecoverPass pass[fpaths.size()];
	for ( int i = 0; i < fpaths.size(); ++i ) {
		pass[i].fpath = fpaths[i];
		pass[i].pcli = this;
		pass[i].result = 0;
		// prt(("c5860 actual doing drecover ... fpath=%s\n", fpaths[i].c_str()));
		jagpthread_create( &thrd[i], NULL, recoverDeltaLogStatic, (void*)&pass[i] );
	}

	for ( int i = 0; i < fpaths.size(); ++i ) {
		pthread_join(thrd[i], NULL);
		if ( pass[i].result == 1 ) jagunlink( fpaths[i].c_str() );
	}
	
	return 1;
}

// method for server use only
void *JaguarCPPClient::recoverDeltaLogStatic( void *ptr )
{
	RecoverPass *pass = (RecoverPass*)ptr;
	JaguarCPPClient *pcli = (JaguarCPPClient*)pass->pcli;
	JagStrSplit sp( pass->fpath.c_str(), '_' );
	if ( sp.length() != 2 ) return NULL;
	
	int useidx = atoi( sp[1].c_str() );
	abaxint rc;
	Jstr cmd;
	char *buf = NULL;
	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	memset(hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);

	// parse file path to get host name
	JagStrSplit spp( sp[0].c_str(), '/' );

	if ( pcli->_debug ) { prt(("c1158 jag_hash_lookup(%s) ...\n", spp[spp.length()-1].c_str() )); }
	JaguarCPPClient *jcli = (JaguarCPPClient*) jag_hash_lookup ( &pass->pcli->_connMap, spp[spp.length()-1].c_str() );
	JagSQLMergeReader mr( pass->fpath );
	
	while ( true ) {
		rc = mr.getNextSQL( cmd );
		if ( rc ) {
			rc = jcli->_jpb->simpleQuery( useidx, cmd.c_str(), true );
			if ( rc < 0 ) {
				pass->result = 0;
				break;
			}
			while ( 1 ) {
				rc = jcli->_jpb->simpleReply( useidx, hdr, buf );
				if ( rc == -1 ) {
					pass->result = 0;
					break;
				} else if ( rc == 0 || rc == -2 ) {
					break;
				}
			} 
			if ( rc == -1 ) break;
		} else break;
	}
	if ( rc >= 0 ) pass->result = 1;
	if ( buf ) free ( buf );
	return NULL;
}

// method for server use only
// return -1: export file does not exist
// inpath: aaa/t1.1.sql|aaa/t1.5.sql|aaa/t1.3.sql
int JaguarCPPClient::importLocalFileFamily( const Jstr &inpath, const char *spstr )
{
	if ( !_isparent ) { return 0; }
	JagStrSplit sp( inpath.c_str(), '|' );

	int numfiles = sp.length();
	Jstr paths[numfiles];
	
	for ( int i = 0; i < numfiles; ++i ) {
		if ( ! JagFileMgr::exist( sp[i].c_str() ) ) {
			return -1;
		}
		paths[i] = sp[i];
	}

	Jstr fullpath;
	for ( int i = 0; i < numfiles; ++i ) {
		if ( i < numfiles-1 ) fullpath += paths[i] + "|";
		else fullpath += paths[i];
	}
	JagSQLMergeReader mr( fullpath );
	int rc;
	Jstr cmd, cmd2;
	if ( spstr ) cmd2 = spstr;
	
	while ( true ) {
		rc = mr.getNextSQL( cmd );
		if ( rc ) {
			cmd = cmd2 + cmd;
			rc = query( cmd.c_str() );
			if ( rc ) {
				while ( reply() ) {}
			}
		} else break;
	}
	
	return 1;
}

// method to check validation for some special commands, e.g. create/drop/changepass/alter
// return 0: error; return 1: success
int JaguarCPPClient::checkSpecialCommandValidation( const char *querys, Jstr &newquery, 
	const JagParseParam &parseParam, Jstr &errmsg, AbaxString &hdbobj, const JagTableOrIndexAttrs *objAttr )
{
	// prt(("c4812 checkSpecialCommandValidation querys=[%s] parseParam.opcode=%d\n", querys, parseParam.opcode ));
	stripEndSpace( (char*)querys, ';' );

	errmsg = "";
 	if ( parseParam.opcode == JAG_CREATEUSER_OP ) {
		if ( _username != "admin" ) {
			errmsg = "Only admin can create user account";
			return 0;
		}

		// prt(("c3849 querys=[%s]\n", querys ));
		JagStrSplit sp( querys, ' ', true );
		if ( sp.length() == 2 && strchr( sp[1].c_str(), ':' ) ) {
			newquery = Jstr("createuser ") + parseParam.uid + ":" + parseParam.passwd;
			if ( parseParam.passwd.length() < 12 ) {
				errmsg = "E6110 Error length for password. It must have at least 12 letters.";
				return 0;
			}
		} else {
			errmsg = "E4063 Error format for createuser user:password [";
			errmsg += Jstr(querys) + "]";
			return 0;
		}
	} else if ( parseParam.opcode == JAG_DROPUSER_OP ) {
		if ( _username != "admin" ) {
			errmsg = "E4081 Only admin can drop user account";
			return 0;
		}
		newquery = Jstr("dropuser ") + parseParam.uid;
	} else if ( parseParam.opcode == JAG_CHANGEPASS_OP ) {
		JagStrSplit sp( querys, ' ', true );
		if ( sp.length() == 2 && strchr( sp[1].c_str(), ':' ) ) {
			if ( _username != "admin" &&_username != parseParam.uid ) {
				errmsg = "E3082 Password can only be changed for current user";
				return 0;
			}

			newquery = Jstr("changepass ") + parseParam.uid + ":" + parseParam.passwd;
			if ( parseParam.passwd.length() < 12 ) {
				errmsg = "E3088 Error length for password. It must have at least 12 letters.";
				return 0;
			}
		} else {
			errmsg = "E4064 Error format for 'changepass user:password'";
			return 0;
		}
	} else if ( parseParam.opcode == JAG_USEDB_OP || ( 1 == _fromServ && parseParam.opcode == JAG_CHANGEDB_OP ) ) {
		JagStrSplit sp( querys, ' ', true );
		if ( sp.length() == 2 && isValidVar( sp[1].c_str() ) &&
			sp[1].length() <= JAG_MAX_DBNAME && 0 != strcmp( sp[1].c_str(), JAG_BRAND ) ) {
			newquery = Jstr("changedb ") + parseParam.dbName;
		} else {
			errmsg = Jstr("E2837 Error name [") + sp[1] + "] for database";
			return 0;
		}
	} else if ( parseParam.opcode == JAG_CREATEDB_OP ) {
		if ( _username != "admin" ) {
			errmsg = "Only admin can create database";
			return 0;
		}
		JagStrSplit sp( querys, ' ', true );
		//prt(("c0092 querys=[%s] isValid=%d\n", querys, isValidVar( sp[1].c_str()) ));
		if ( sp.length() == 2 && isValidVar( sp[1].c_str() ) &&
			sp[1].length() <= JAG_MAX_DBNAME && 0 != strcmp( sp[1].c_str(), JAG_BRAND ) ) {
			newquery = Jstr("createdb ") + parseParam.dbName;
		} else {
			//prt(("c2739 spleng=%d sp[1] = [%s] JAG_BRAND=[%s]\n", sp.length(), sp[1].c_str(), JAG_BRAND ));
			errmsg = Jstr("E3088 Error name [") + sp[1] + "] for database";
			return 0;
		}
	} else if ( parseParam.opcode == JAG_DROPDB_OP ) {
		if ( _username != "admin" ) {
			errmsg = "Only admin can drop database";
			return 0;
		}
		JagStrSplit sp( querys, ' ', true );
		if ( sp.length() == 2 && isValidVar( sp[1].c_str() ) && 
			sp[1].length() <= JAG_MAX_DBNAME && 0 != strcmp( sp[1].c_str(), JAG_BRAND ) ) {
			if ( parseParam.dbName == "system" ) {
				errmsg = "System database cannot be dropped";
				return 0;
			} else if ( parseParam.dbName == "test" ) {
				errmsg = "Test database cannot be dropped";
				return 0;
			} else if ( parseParam.dbName == _dbname ) {
				errmsg = "Current database cannot be dropped";
				return 0;
			}
			if ( parseParam.hasForce ) {
				newquery = Jstr("dropdb force ") + parseParam.dbName;
			} else {
				newquery = Jstr("dropdb ") + parseParam.dbName;
			}
		} else if ( sp.length() == 3 && isValidVar( sp[2].c_str() ) &&
		         sp[2].length() <= JAG_MAX_DBNAME  ) {
			if ( parseParam.dbName == "system" ) {
				errmsg = "System database cannot be dropped";
				return 0;
			} else if ( parseParam.dbName == "test" ) {
				errmsg = "Test database cannot be dropped";
				return 0;
			} else if ( parseParam.dbName == _dbname ) {
				errmsg = "Current database cannot be dropped";
				return 0;
			}
			newquery = Jstr("dropdb force ") + parseParam.dbName;
		} else {
			errmsg = "E2408 Error name for database";
			return 0;
		}
	} else if ( parseParam.opcode == JAG_CREATEINDEX_OP ) {
		// check if create index column names are in the range of table's column names
		for ( int i = 0; i < parseParam.otherVec.size(); ++i ) {
			AbaxString ss = hdbobj + "." + parseParam.otherVec[i].objName.colName;
			// prt(("c3008 check ss=[%s] in objAttr.schmap ...\n", ss.c_str() ));
			if ( !objAttr->schmap.keyExist( ss.c_str() ) ) {
				errmsg = Jstr("E2008 Index column ") + parseParam.otherVec[i].objName.colName + 
						" is not a member of table columns";
				return 0;
			}
		}
		newquery = querys;
	} else if ( parseParam.opcode == JAG_ALTER_OP ) {
		if ( parseParam.createAttrVec.size() < 1 ) {
			// for rename column, check if old column name is a member of table's column names
			AbaxString ss = hdbobj + "." + parseParam.objectVec[0].colName;
			if ( !objAttr->schmap.keyExist( ss.c_str() ) && parseParam.cmd != JAG_SCHEMA_SET ) {
				errmsg = Jstr("E1028 Column ") + parseParam.objectVec[0].colName + 
						" is not a member of table columns";
				return 0;
			}
		} else {
			// for add column, check if new column name is a member of table's column names
			AbaxString ss = hdbobj + "." + parseParam.createAttrVec[0].objName.colName;
			if ( objAttr->schmap.keyExist( ss.c_str() ) ) {
				errmsg = Jstr("Column ") + parseParam.createAttrVec[0].objName.colName + 
						" is already a member of table columns";
				return 0;
			}
		}
		newquery = querys;
	} else if ( parseParam.opcode == JAG_CREATETABLE_OP ) {
		// need to check if default value length is larger than 10240
		abaxint totlen = 0;
		for ( int i = 0; i < parseParam.createAttrVec.size(); ++i ) {
			if ( parseParam.createAttrVec[i].defValues.size() > 0 ) {
				totlen += 2 + parseParam.createAttrVec[i].objName.colName.size() + parseParam.createAttrVec[i].defValues.size();
			}
		}
		if ( totlen > JAG_SCHEMA_VALLEN ) {
			errmsg = Jstr("E3088 Error creating table ") + 
							parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName + 
					". Default values are too long.";
			return 0;
		}
		newquery = querys;
	} else if ( JAG_GRANT_OP == parseParam.opcode || JAG_REVOKE_OP == parseParam.opcode ) {
		if ( _username != "admin" ) {
			errmsg = "Only admin can grant and revoke permissions for a user.";
			return 0;
		}
		newquery = querys;
		// prt(("c4771 newquery=[%s]\n", newquery.c_str() ));
	} else {
		newquery = querys;
		// prt(("c4772 newquery=[%s]\n", newquery.c_str() ));
	}

	return 1;
}

int JaguarCPPClient::formatReturnMessageForSpecialCommand( const JagParseParam &parseParam, Jstr &retmsg )
{
	retmsg = "";
	bool rc = 1; // success commit
	if ( _spCommandErrorCnt > 0 ) rc = 0; // abort commit

	if ( parseParam.opcode == JAG_CREATEUSER_OP ) {
		retmsg = Jstr("User ") + parseParam.uid;
		if ( rc ) retmsg += " is created successfully. Please use grant command to give permissions to the user.";
		else retmsg += " create error";
	} else if ( parseParam.opcode == JAG_DROPUSER_OP ) {
		retmsg = Jstr("User ") + parseParam.uid;
		if ( rc ) retmsg += " is dropped successfully";
		else retmsg += " drop error";
	} else if ( parseParam.opcode == JAG_CHANGEPASS_OP ) {
		if ( rc ) {
			retmsg = Jstr("Password is changed successfully for ") + parseParam.uid;
			// update _password for all client objects
			JaguarCPPClient *jcli;
			for ( int i = 0; i < _parentCli->_allHosts->size(); ++i ) {
				if ( _debug ) { prt(("c0881 jag_hash_lookup(%s) ...\n", (*(_parentCli->_allHosts))[i].c_str() )); }
				jcli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(_parentCli->_allHosts))[i].c_str() );
				jcli->updatePassword( parseParam.passwd );
			}
		} else retmsg = "Password is NOT changed for current user";
	} else if ( parseParam.opcode == JAG_USEDB_OP || ( 1 == _fromServ && parseParam.opcode == JAG_CHANGEDB_OP ) ) {
		if ( rc ) {
			retmsg = "Database is changed successfully";
			JaguarCPPClient *jcli;
			for ( int i = 0; i < _parentCli->_allHosts->size(); ++i ) {
				if ( _debug ) { prt(("c0884 jag_hash_lookup(%s) ...\n", (*(_parentCli->_allHosts))[i].c_str() )); }
				jcli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(_parentCli->_allHosts))[i].c_str() );
				jcli->updateDBName( parseParam.dbName );
			}
		} else retmsg = "Database is NOT changed";
	} else if ( parseParam.opcode == JAG_CREATEDB_OP ) {
		retmsg = Jstr("Database ") + parseParam.dbName;
		if ( rc ) retmsg += " is created successfully";
		else retmsg += " create error";
	} else if ( parseParam.opcode == JAG_DROPDB_OP ) {
		retmsg = Jstr("Database ") + parseParam.dbName;
		if ( rc ) retmsg += " is dropped successfully";
		else retmsg += " drop error";
	} else if ( parseParam.opcode == JAG_CREATETABLE_OP || parseParam.opcode == JAG_CREATEMEMTABLE_OP ) {
		// prt(("c5092 create table parseParam.hasExist=%d\n", parseParam.hasExist ));
		retmsg = Jstr("Table ") + parseParam.objectVec[0].tableName;
		if ( rc ) retmsg += " is created successfully";
		else if ( !parseParam.hasExist ) retmsg += " create error";
		else retmsg = "";
	} else if ( parseParam.opcode == JAG_CREATECHAIN_OP ) {
		retmsg = Jstr("Chain ") + parseParam.objectVec[0].tableName;
		if ( rc ) retmsg += " is created successfully";
		else if ( !parseParam.hasExist ) retmsg += " create error";
		else retmsg = "";
	} else if ( parseParam.opcode == JAG_CREATEINDEX_OP ) {
		retmsg = Jstr("Index ") + parseParam.objectVec[1].indexName;
		if ( rc ) retmsg += " is created successfully";
		else retmsg += " create error";
	} else if ( parseParam.opcode == JAG_DROPTABLE_OP ) {
		retmsg = Jstr("Table ") + parseParam.objectVec[0].tableName;
		if ( rc ) retmsg += " is dropped successfully";
		else if ( !parseParam.hasExist ) retmsg += " drop error";
		else retmsg = "";
	} else if ( parseParam.opcode == JAG_DROPINDEX_OP ) {
		retmsg = Jstr("Index ") + parseParam.objectVec[1].indexName;
		if ( rc ) retmsg += " is dropped successfully";
		else if ( !parseParam.hasExist ) retmsg += " drop error";
		else retmsg = "";
	} else if ( parseParam.opcode == JAG_TRUNCATE_OP ) {
		retmsg = Jstr("Table ") + parseParam.objectVec[0].tableName;
		if ( rc ) retmsg += " been truncated successfully";
		else retmsg += " truncate error";
	} else if ( parseParam.opcode == JAG_ALTER_OP ) {
		if ( rc ) retmsg = Jstr("Table column ") + parseParam.objectVec[0].colName +
					" is renamed/added successfully";
		else retmsg = Jstr("Table ") + parseParam.objectVec[0].tableName + " alter error";
	} else if ( JAG_GRANT_OP == parseParam.opcode ) {
		retmsg = Jstr("Permission [") + JagUserRole::convertManyToStr(parseParam.grantPerm) + 
				"] for " + parseParam.grantUser;
		if ( rc ) retmsg += " is granted successfully";
		else retmsg += " is not granted";
	} else if ( JAG_REVOKE_OP == parseParam.opcode ) {
		retmsg = Jstr("Permission [") + JagUserRole::convertManyToStr(parseParam.grantPerm) + 
				 "] for " + parseParam.grantUser;
		if ( rc ) retmsg += " is revoked successfully";
		else retmsg += " is not revoked";
	} else {
		if ( rc ) retmsg = "Command executed successfully";
		else retmsg = "Command error";
	}

	// prt(("c3221 rc=%d\n", rc ));
	return rc;
}

// first, parse to see if cmd is valid or not
// 0: fail
// rc: parse OK, return and prepare to find related server return value is checkCmdMode
int JaguarCPPClient::getParseInfo( const Jstr &cmd, JagParseParam &parseParam, Jstr &errmsg )
{
	JagParseAttribute jpa( NULL, _tdiff, _tdiff, _dbname );
	JagParser parser( NULL, this );
	if ( !parser.parseCommand( jpa, cmd, &parseParam, errmsg ) ) {
		return 0;
	}

	// return checkCmdMode( parseParam.opcode, parseParam.optype );
	return parseParam.checkCmdMode();
}

// method to check all tables/indexs to make sure they exist before doing process cmd
// if any of them does not exist, retry with schema update
// return 1: all tables/indexs exist, or 'C' related cmds except create index and alter table
// return 0: any one of tables/index does not exist
int JaguarCPPClient::checkCmdTableIndexExist( const JagParseParam &parseParam, AbaxString &hdbobj, 
										const JagTableOrIndexAttrs *&objAttr, Jstr &errmsg )
{
	if ( parseParam.optype == 'C' && parseParam.opcode != JAG_CREATEINDEX_OP && parseParam.opcode != JAG_ALTER_OP ) return 1;
	if ( !_schemaMap ) {
		hdbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
		errmsg = Jstr(hdbobj.c_str()) + " does not exist [0]";
		return 0;
	}

	bool coltab = 0;
	// for create index and alter cmd, table must exist
	if ( parseParam.opcode == JAG_CREATEINDEX_OP || parseParam.opcode == JAG_ALTER_OP ) {
		hdbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
		// if ( !_schemaMap->getValue( hdbobj, objAttr ) ) 
		objAttr = _schemaMap->getValue( hdbobj );
		if ( ! objAttr ) {
			errmsg = Jstr(hdbobj.c_str()) + " does not exist [100]";
			return 0;
		}
	} else {
		// for all other cmds, all tables/indexs in cmd must exist
		for ( int i = 0; i < parseParam.objectVec.size(); ++i ) {
			if ( parseParam.objectVec[i].indexName.length() > 0 ) {
				hdbobj = parseParam.objectVec[i].dbName + "." + parseParam.objectVec[i].indexName;
				// if ( !_schemaMap->getValue( hdbobj, objAttr ) ) 
				objAttr = _schemaMap->getValue( hdbobj );
				if ( ! objAttr ) {
					errmsg = Jstr(hdbobj.c_str()) + " does not exist [200]";
					return 0;
				}
			} else {
				hdbobj = parseParam.objectVec[i].dbName + "." + parseParam.objectVec[i].tableName;
				// if ( !_schemaMap->getValue( hdbobj, objAttr ) ) {
				objAttr = _schemaMap->getValue( hdbobj );
				if ( ! objAttr ) { 
					if ( parseParam.objectVec[i].colName.length() > 0 ) {
						// try dfdbname + tableName 					
						hdbobj = parseParam.objectVec[i].colName + "." + parseParam.objectVec[i].tableName;
						// if ( !_schemaMap->getValue( hdbobj, objAttr ) ) 
						objAttr = _schemaMap->getValue( hdbobj );
						if ( ! objAttr ) {
							errmsg = Jstr(hdbobj.c_str()) + " does not exist [3030]";
							return 0;
						} else {
							if ( 0 == i ) coltab = 1;					
						}
					} else {
						errmsg = Jstr(hdbobj.c_str()) + " does not exist [3095]";
						return 0;
					}
				}
			}
			if ( objAttr->indexName.size() > 0 ) {
				const JagTableOrIndexAttrs *objAttr2 = NULL;
				AbaxString hdbobj2 = objAttr->dbName + "." + objAttr->tableName;
				// if ( !_schemaMap->getValue( hdbobj, objAttr2 ) ) 
				objAttr2 = _schemaMap->getValue( hdbobj );
				if ( ! objAttr2 ) {
					errmsg = Jstr("E2033 No table ") + hdbobj2.c_str() + " for index " + hdbobj.c_str();
					return 0;
				}
			}
		}
	}

	if ( parseParam.objectVec[0].indexName.length() > 0 ) {
		hdbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].indexName;
	} else {
		if ( coltab ) hdbobj = parseParam.objectVec[0].colName + "." + parseParam.objectVec[0].tableName;
		else hdbobj = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
	}

	// bool rc = _schemaMap->getValue( hdbobj, objAttr );
	objAttr = _schemaMap->getValue( hdbobj );
	// prt(("s2037 hdbobj=[%s] rc=%d\n", hdbobj.c_str(), rc ));
	return 1;
}
	

// method to process schema change related 'C' type, read related 'R' type and update/delete cmds
// returns:
//     1: success
//     0: error
int JaguarCPPClient::processInsertCommands( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, 
										    Jstr &errmsg, JagVector<Jstr> *filevec )
{
	int numNames = parseParam.inscolmap.size();
	if ( numNames > 0 ) {
		return processInsertCommandsWithNames( cmdhosts, parseParam, errmsg, filevec );
	} else {
		return processInsertCommandsWithoutNames( cmdhosts, parseParam, errmsg, filevec );
	}
}


// method to process schema change related 'C' type, read related 'R' type and update/delete cmds
// returns:
//     1: success
//     0: error
int JaguarCPPClient::processInsertCommandsWithNames( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, 
										    Jstr &errmsg, JagVector<Jstr> *filevec )
{
	int rc;
	AbaxString hdbobj;
	Jstr newquery;
	const JagTableOrIndexAttrs *objAttr = NULL;

	_mapLock->readLock( -1 );
	if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
		// table does not exist, retry
		_mapLock->readUnlock( -1 );
		_parentCli->queryDirect( "_cschema" ); _parentCli->replyAll();
		if ( _schemaUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			rebuildSchemaMap();
			updateSchemaMap( _schemaUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}

		/***
		_parentCli->queryDirect( "_cdefval" ); _parentCli->replyAll();
		if ( _defvalUpdateString.size() > 0 ) {
			prt(("c8191 _schemaUpdateString=[%s]\n", _schemaUpdateString.c_str() ));
			_mapLock->writeLock( -1 );
			updateDefvalMap( _defvalUpdateString );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}
		**/

		_mapLock->readLock( -1 );
		if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}	
	
	if ( parseParam.objectVec[0].indexName.length() > 0 || objAttr->indexName.size() > 0 ) { 
	    // insert to index, invalid command
		errmsg = Jstr("E4003 Cannot insert directly into index ") + hdbobj.c_str();
		_mapLock->readUnlock( -1 );
		return 0;
	}
	
	errmsg = "";
	cmdhosts.clean();
	int numNames = parseParam.inscolmap.size();
	int numCols = objAttr->numCols;  // all columns, complex, details, including spare_ column
	int numInValues = parseParam.otherVec.size(); // # of data in vaues ()
	_debug && prt(("c6003 numNames=%d otherVec.size=numInValues=%d numCols=%d\n", numNames, numInValues, objAttr->numCols ));
	int getpos = 0;
	Jstr colName;
	Jstr colName2;
	Jstr colData;
	JagFixString kstr;
	JagVector<Jstr> hosts;
	int offset, length, sig;
	Jstr type;
	char spare1, spare4;
	char kbuf[objAttr->keylen+1];
	memset(kbuf, 0, objAttr->keylen+1);
	int fromsrv = _fromServ;
	if ( _parentCli ) fromsrv = _parentCli->_fromServ;

	bool hasAllCols = false;
	if ( numInValues == objAttr->numCols -1 ) {
		hasAllCols = true;
	}

	if ( _debug ) {
		prt(("c6013 numCols=%d hasAllCols=%d\n", objAttr->numCols, hasAllCols ));
	}

	// check some error conditions of cmd
	// not from server, from client
	if ( ! fromsrv ) {
		if ( numInValues >= objAttr->numCols ) {
			// prt(("c3005 numInValues=%d objAttr->numCols=%d\n", numInValues, objAttr->numCols ));
			errmsg = Jstr("E0382 Too many columns in name list for ") + hdbobj.c_str();
			errmsg += Jstr("Names: ") + intToStr( numInValues ) + " Columns: " + intToStr(objAttr->numCols);
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}

	// disallow uuid column in names list
	Jstr uuidCols;
	int pos;
	int otherSize = parseParam.otherVec.size();
	JagNameIndex sortedName[otherSize];
	for ( int i = 0; i < otherSize; ++i ) {
		colName2 = parseParam.otherVec[i].objName.colName;
		getpos =  objAttr->schemaRecord.getPosition( colName2 );

		if ( getpos < 0 ) {
			errmsg = Jstr("E1382 Error column in insert [") + colName2 + "]";
			_mapLock->readUnlock( -1 );
			return 0;
		}

		spare1 = (*objAttr->schemaRecord.columnVector)[getpos].spare[1];
		if ( spare1 == JAG_C_COL_TYPE_UUID_CHAR )  {
			// this column is uuid, not allowed in columns list
			errmsg = Jstr("E1383 Error uuid column in insert [") + colName2 + "]";
			_mapLock->readUnlock( -1 );
			return 0;
		}

		// OK
		sortedName[i].i = getpos;
		sortedName[i].name = colName2;
		//prt(("s2727 sortedName i=%d name=[%s]\n", pos, colName2.c_str() ));
	}

	JagGeo::sortLinePoints<JagNameIndex>( sortedName, otherSize );
	//prt(("s871 done sortLinePoints otherSize=%d\n", otherSize ));
	
	// insert into table related cmds, need to reformat
	newquery = Jstr("insert into ") + parseParam.objectVec[0].dbName + "." 
			   + parseParam.objectVec[0].tableName + " values ('";
	if ( JAG_CINSERT_OP == parseParam.opcode ) {
		// for dinsert cmd, change and format to dinsert _tdiff into ... for server processing
		newquery = Jstr("dinsert ") + intToStr( _tdiff ) + " into ";
	} else {
		newquery = "insert into ";
	}
	newquery += parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName + " values (";
		
	
	bool hquote, useValueData;
	int issubcol, begincol, endcol;
	Jstr colType;

	// debug only
	#if 0
	prt(("\n"));
	for ( int i = 0; i < parseParam.otherVec.size(); ++i ) {
		prt(("c5409 otherVec i=%d  other.x=%s other.y=%s other.a=%s type=%s value=[%s] \n", 
				i, parseParam.otherVec[i].point.x, parseParam.otherVec[i].point.y, parseParam.otherVec[i].point.a,
				objAttr->schAttr[i].type.c_str(), parseParam.otherVec[i].valueData.c_str() ));
	}
	prt(("\n"));
	#endif


	int j = 0;  // scan sortedNames[]
	// for ( int i = 0; i < parseParam.otherVec.size(); ++i ) 
	bool colInOther;
	int otherPos;
	bool first = true;
	for ( int i = 0; i < objAttr->numCols-1; ++i ) {
		colName2 = (*objAttr->schemaRecord.columnVector)[i].name.c_str();
		if ( strchr( colName2.c_str(), ':') ) continue;  // skip bbox and :x :y :z columns
		colInOther = false;
		if ( j < otherSize && colName2 == sortedName[j].name ) {
			colInOther = true;
			//prt(("s7160 i=%d colName2=[%s] j=%d colInOther true\n", i, colName2.c_str(), j ));
			rc = parseParam.inscolmap.getValue(colName2.c_str(), otherPos );
			//prt(("s2073  colName2=[%s] j=%d otherPos=%d colInOther true rc=%d\n", colName2.c_str(), j, otherPos, rc ));
			++j;
		} else {
			//prt(("s7161 i=%d colName2=[%s] sortedName[j=%d] colInOther false\n", i, colName2.c_str(), j  ));
		}

		offset = objAttr->schAttr[i].offset;
		length = objAttr->schAttr[i].length;
		type = objAttr->schAttr[i].type;
		issubcol = (*objAttr->schemaRecord.columnVector)[i].issubcol;
		begincol = (*objAttr->schemaRecord.columnVector)[i].begincol;
		endcol = (*objAttr->schemaRecord.columnVector)[i].endcol;
		spare1 = (*objAttr->schemaRecord.columnVector)[i].spare[1];

		//prt(("s6591 i=%d numCols=%d colName2=[%s] type=[%s]\n", i, numCols, colName2.c_str(), type.c_str() ));
		prt(("s1727 first=%d\n", first ));
		if ( first ) {
			first = false;
		} else {
			newquery += Jstr(",");
			prt(("s1290 append , to newquery\n" ));
		}

		if ( type == JAG_C_COL_TYPE_POINT ) {
			if ( colInOther ) {
				newquery += getCoordStr( "point", parseParam, otherPos, 1, 1, 0, 0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_POINT3D ) {
			if ( colInOther ) {
				newquery += getCoordStr( "point3d", parseParam, otherPos, 1, 1, 1, 0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE ) {
			if ( colInOther ) {
				newquery += getCoordStr( "circle", parseParam, otherPos, 1, 1, 0, 1 );
			} else {
				newquery += "'',";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE3D ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "circle3d", parseParam, otherPos, 1, 1, 1,  1,0,0, 1, 1, 1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_SPHERE ) {
			if ( colInOther ) {
				newquery += getCoordStr( "sphere", parseParam, otherPos, 1, 1, 1, 1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_SQUARE ) {
			//prt(("s3380 JAG_C_COL_TYPE_SQUARE colInOther=%d\n", colInOther ));
			if ( colInOther ) {
				newquery += getSquareCoordStr( "square", parseParam, otherPos );
				//prt(("s1094 square getSquareCoordStr newquery=[%s]\n", newquery.c_str() ));
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_SQUARE3D ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "square3d", parseParam, otherPos, 1, 1, 1, 1,0,0,1,1,0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_CUBE ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "cube", parseParam, otherPos, 1,1,1, 1,0,0, 1,1,0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "rectangle", parseParam, otherPos, 1,1,0, 1,1,0, 1,0,0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE3D ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "rectangle3d", parseParam, otherPos, 1, 1, 1, 1,1,0, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE3D ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "ellipse3d", parseParam, otherPos, 1,1,1, 1,1,0, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_BOX ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "box", parseParam, otherPos, 1, 1, 1, 1, 1, 1, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_CYLINDER ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "cylinder", parseParam, otherPos, 1, 1, 1, 1, 0, 1, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_CONE ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "cone", parseParam, otherPos, 1, 1, 1, 1, 0, 1, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "ellipse", parseParam, otherPos, 1,1,0,  1,0,1, 1,0,0 );
			} else {
				newquery += "''";
			}
			// prt(("c3931 colName2=[%s] JAG_C_COL_TYPE_ELLIPSE newquery=[%s]\n", colName2.c_str(), newquery.c_str() ));
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSOID ) {
			if ( colInOther ) {
				newquery += get3DPlaneCoordStr( "ellipsoid", parseParam, otherPos, 1, 1, 1, 1, 1, 1, 1,1,1 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINE ) {
			prt(("c1028 newquery=[%s]\n", newquery.c_str() ));
			if ( colInOther ) {
				newquery += getLineCoordStr( "line", parseParam, otherPos, 1, 1, 0, 1, 1, 0 );
			} else {
				newquery += "''";
			}
			prt(("c1029 newquery=[%s]\n", newquery.c_str() ));
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINE3D ) {
			prt(("c1528 newquery=[%s]\n", newquery.c_str() ));
			if ( colInOther ) {
				newquery += getLineCoordStr( "line3d", parseParam, otherPos, 1, 1, 1, 1, 1, 1 );
			} else {
				newquery += "''";
			}
			prt(("c1528 newquery=[%s]\n", newquery.c_str() ));
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING ) {
			if ( colInOther ) {
				newquery += Jstr("linestring(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING3D ) {
			if ( colInOther ) {
				newquery += Jstr("linestring3d(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT ) {
			if ( colInOther ) {
				newquery += Jstr("multipoint(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			if ( colInOther ) {
				newquery += Jstr("multipoint3d(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_POLYGON ) {
			if ( colInOther ) {
				newquery += Jstr("polygon(") + parseParam.otherVec[otherPos].valueData  + ")";
				prt(("s0291 POLYGON valueData=[%s]\n", parseParam.otherVec[otherPos].valueData.c_str() ));
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_POLYGON3D ) {
			if ( colInOther ) {
				newquery += Jstr("polygon3d(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			if ( colInOther ) {
				newquery += Jstr("multipolygon") + parseParam.otherVec[otherPos].valueData;
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			if ( colInOther ) {
				newquery += Jstr("multipolygon3d") + parseParam.otherVec[otherPos].valueData;
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING ) {
			if ( colInOther ) {
				newquery += Jstr("multilinestring(") + parseParam.otherVec[otherPos].valueData  + ")";
				prt(("s0295 MULTILINESTRING valueData=[%s]\n", parseParam.otherVec[otherPos].valueData.c_str() ));
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			if ( colInOther ) {
				newquery += Jstr("multilinestring3d(") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_RANGE ) {
			if ( colInOther ) {
				newquery += Jstr("range") + parseParam.otherVec[otherPos].valueData  + ")";
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE ) {
			if ( colInOther ) {
				newquery += getTriangleCoordStr( "triangle", parseParam, otherPos, 1, 1, 0, 1, 1, 0, 1, 1, 0 );
			} else {
				newquery += "''";
			}
			continue;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			if ( colInOther ) {
				newquery += getTriangleCoordStr( "triangle3d", parseParam, otherPos, 1, 1, 1, 1, 1, 1, 1, 1, 1 );
			} else {
				newquery += "''";
			}
			continue;
		}

		// regular columns
		hquote = 0;
		useValueData = 0;
		spare1 = (*objAttr->schemaRecord.columnVector)[i].spare[1];
		spare4 = (*objAttr->schemaRecord.columnVector)[i].spare[4];

		if ( spare1 == JAG_C_COL_TYPE_UUID_CHAR )  {
			colData = _jagUUID->getString();
		} else if ( spare4 == JAG_CREATE_DEFUPDATETIME || spare4 == JAG_CREATE_DEFUPDATE ||
					spare4 == JAG_CREATE_DEFDATETIME || spare4 == JAG_CREATE_DEFDATE ) {
			// this column is timestamp/datetime, with default as current time
			sig = 0;
			if (  colInOther && parseParam.otherVec[otherPos].valueData.size() > 0 ) {
				colData = parseParam.otherVec[otherPos].valueData;
				hquote = parseParam.otherVec[otherPos].hasQuote;
				useValueData = 1;
			} else {
				char timebuf[JAG_FUNC_NOW_LEN_MICRO+1];
				char timebuf2[JAG_FUNC_CURDATE_LEN+1];
				getTimebuf( objAttr, timebuf, timebuf2 );
				if ( spare4 == JAG_CREATE_DEFUPDATE || spare4  == JAG_CREATE_DEFDATE ) {
					colData = timebuf2;
				} else {
					colData = timebuf;
				}
			}
		} else {
			// prt(("c0172 ...\n" ));
			sig = objAttr->schAttr[i].sig;
			type = objAttr->schAttr[i].type;
			//colName = (*objAttr->schemaRecord.columnVector)[getpos].name;
			if ( colInOther ) {
				//colData = parseParam.otherVec[j].valueData;
				//hquote = parseParam.otherVec[j].hasQuote;
				colData = parseParam.otherVec[otherPos].valueData;
				hquote = parseParam.otherVec[otherPos].hasQuote;
				useValueData = 1;
				//prt(("s2031 i=%d colData=[%s] j=%d useValueData=1 otherPos=%d\n", i, colData.c_str(), j, otherPos ));
			} else {
				colData = "";
				hquote = 0;
				useValueData = 0;
				//prt(("s2091 i=%d colData empty useValueData=0\n", i ));
			}

			if ( colInOther && spare1 == JAG_C_COL_TYPE_FILE_CHAR && filevec ) {
				if ( _debug ) { prt(("c63830 JAG_C_COL_TYPE_FILE ... getpos=%d colData=[%s]\n", getpos, colData.c_str() )); }
				// for file object, parse and get the filename only 
				Jstr oripath = colData;
				const char *lp = strrchr( colData.c_str(), '/' );
				if ( lp ) { colData = lp+1; }
				if ( _datcSrcType != JAG_DATACENTER_GATE ) {
					// rewrite filepath if called by datacenter client in jaguarserver
					if ( _isDataCenterSync ) {
						Jstr hdir = fileHashDir( kstr );
						oripath = jaguarHome() + "/data/" + parseParam.objectVec[0].dbName + "/" 
								  + parseParam.objectVec[0].tableName + "/files/" + hdir + "/" + colData;
					}

					// if ( oripath.size() <= 0 ) { colData = ""; }
					if ( oripath.size() > 0 ) {
						oripath = expandEnvPath( oripath );
						filevec->append( oripath );
					} else {
						colData = "";
						filevec->append( "." );
					}

					if ( oripath.size() > 0 ) {
						if ( 0 !=  access( oripath.c_str(), R_OK ) ) {
    						_mapLock->readUnlock( -1 );
    						errmsg = Jstr("E7102 File ") + oripath + " cannot be open";
							if ( _debug ) { prt(("c5202 errmsg=[%s]\n", errmsg.c_str() )); }
    						return 0;
						}
					} 
				}
			}
		}

		// check if colData is "NULL", regard as empty
		if ( useValueData && !hquote && strcasecmp( colData.c_str(), "NULL" ) == 0 ) {
			colData = "";
		}

		if ( i < objAttr->numKeys && colData.size() > 0 ) {
			rc = formatOneCol( _tdiff, _tdiff, kbuf, colData.c_str(), errmsg, colName.c_str(), 
								offset, length, sig, type );
			if ( !rc ) {
				_mapLock->readUnlock( -1 );
				_debug && prt(("c5039 return 0 i=%d objAttr->numKeys=%d\n", i, objAttr->numKeys, objAttr->numKeys ));
				errmsg = Jstr("E0801 Error insert for ") + colName.c_str() + " formatOneCol error";
				return 0;
			}
		}

		// check first column value
		if ( ! parseParam.hasPoly && (0 == i ) && (colData.size() < 1) ) {
			errmsg = Jstr("E0811 Error insert for ") + hdbobj.c_str() + 
					 ". First column data " + colName.c_str() + " must be provided.";
			_mapLock->readUnlock( -1 );
			return 0;
		}

		//newquery += Jstr ( "'" ) + colData + "',";
		newquery += Jstr ( "'" ) + colData + "'";
		//prt(("c3934 added getpos=%d colData=[%s] newquery=[%s]\n", getpos, colData.c_str(), newquery.c_str() ));

		//if ( colInOther ) { ++j; }

	}  // end for loop ( int i = 0; i < objAttr->numCols; ++i  )


	newquery += Jstr(");");
	//prt(("c3091 newquery=[%s]\n", newquery.c_str() ));

	dbNaturalFormatExchange( kbuf, objAttr->numKeys, objAttr->schAttr, 0, 0, " " ); // natural format -> db format
	kstr = JagFixString( kbuf, objAttr->keylen );
	// if ( _debug ) { prt(("c5038 kstr:\n" )); kstr.dump(); }
	// kstr.dump();
	JagFixString hostkstr;
	if ( objAttr->schemaRecord.hasMute ) {
		getHostKeyStr( kbuf, objAttr, hostkstr );
	} else {
		hostkstr.point( kstr );
	}

	//prt(("c2009 addQueryToCmdhosts parseParam.opcode=%d\n", parseParam.opcode ));
    addQueryToCmdhosts( parseParam, hostkstr, objAttr, hosts, cmdhosts, newquery );

	_mapLock->readUnlock( -1 );
	return 1;
}

// method to process schema change related 'C' type, read related 'R' type and update/delete cmds
// returns:
//     1: success
//     0: error
int JaguarCPPClient::processInsertCommandsWithoutNames( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, 
										    Jstr &errmsg, JagVector<Jstr> *filevec )
{
	int rc;
	AbaxString hdbobj;
	Jstr newquery;
	const JagTableOrIndexAttrs *objAttr = NULL;
	_mapLock->readLock( -1 );
	if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
		// table does not exist, retry
		_mapLock->readUnlock( -1 );
		//_parentCli->queryDirect( Jstr("_cschema").c_str() );

		_parentCli->queryDirect( "_cschema" ); _parentCli->replyAll();
		if ( _schemaUpdateString.size() > 0 ) {
			// prt(("c8192 _schemaUpdateString=[%s]\n", _schemaUpdateString.c_str() ));
			_mapLock->writeLock( -1 );
			rebuildSchemaMap();
			updateSchemaMap( _schemaUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}

		/***
		_parentCli->queryDirect( "_cdefval" ); _parentCli->replyAll();
		prt(("s0283 _defvalUpdateString=[%s]\n", _defvalUpdateString.c_str() ));
		if ( _defvalUpdateString.size() > 0 ) {
			prt(("c8192 _schemaUpdateString=[%s]\n", _schemaUpdateString.c_str() ));
			_mapLock->writeLock( -1 );
			updateDefvalMap( _defvalUpdateString );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}
		***/

		_mapLock->readLock( -1 );
		if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}	
	
	if ( parseParam.objectVec[0].indexName.length() > 0 || objAttr->indexName.size() > 0 ) { 
	    // insert to index, invalid command
		errmsg = Jstr("E4003 Cannot insert directly into index ") + hdbobj.c_str();
		_mapLock->readUnlock( -1 );
		return 0;
	}
	
	errmsg = "";
	cmdhosts.clean();
	int numNames = parseParam.inscolmap.size();
	int numCols = objAttr->numCols;  // all columns, complex, details, including spare_ column
	int numInValues = parseParam.otherVec.size(); // # of data in vaues ()
	_debug && prt(("c6003 numNames=%d otherVec.size=numInValues=%d numCols=%d\n", numNames, numInValues, objAttr->numCols ));
	int getpos = 0;
	//Jstr colName;
	Jstr colName2;
	Jstr colData;
	JagFixString kstr;
	JagVector<Jstr> hosts;
	int offset, length, sig;
	Jstr type;
	char kbuf[objAttr->keylen+1];
	memset(kbuf, 0, objAttr->keylen+1);
	int fromsrv = _fromServ;
	if ( _parentCli ) fromsrv = _parentCli->_fromServ;

	bool hasAllCols = false;
	if ( numInValues == objAttr->numCols -1 ) {
		hasAllCols = true;
	}
	//prt(("c1238 numInValues=%d objAttr->numCols=%d hasAllCols=%d\n", numInValues, objAttr->numCols, hasAllCols ));

	if ( _debug ) {
		prt(("c6013 numCols=%d hasAllCols=%d\n", objAttr->numCols, hasAllCols ));
	}
	
	// insert into table related cmds, need to reformat
	newquery = Jstr("insert into ") + parseParam.objectVec[0].dbName + "." 
				+ parseParam.objectVec[0].tableName + " values ('";
	if ( JAG_CINSERT_OP == parseParam.opcode ) {
		// for dinsert cmd, change and format to dinsert _tdiff into ... for server processing
		newquery = Jstr("dinsert ") + intToStr( _tdiff ) + " into ";
	} else {
		newquery = "insert into ";
	}
	newquery += parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName + " values (";
		
	// check some error conditions of cmd
	// not from server, from client
	if ( ! fromsrv ) {
		if ( hasAllCols ) {
			// just values insert and has all columns (except last spare_ col)
		} else {
			if ( numInValues + objAttr->uuidarr.size() + objAttr->deftimestamparr.size() > objAttr->numCols ) {
					errmsg = Jstr("E0237 Error columns in value insert for ") + hdbobj.c_str();
					errmsg += Jstr(" numInValues=") + intToStr(numInValues) 
					          + " uuidnum=" + intToStr( objAttr->uuidarr.size() ) 
							  + " deftime=" + intToStr( objAttr->deftimestamparr.size() )
							  + " objAttr->numCols=" + intToStr( objAttr->numCols );
					_mapLock->readUnlock( -1 );
					return 0;
			}
		}
	}
	
	// num is # of columns in table schema
	//getTimebuf( objAttr, timebuf, timebuf2 );

	bool hquote, useValueData;
	int issubcol, begincol, endcol;
	Jstr colType;
	bool lastIsGeo  = false;

	int ii = -1;
	char spare1, spare4;
	bool first = true;
	for ( int i = 0; i < numCols-1; ++i ) {
		offset = objAttr->schAttr[i].offset;
		length = objAttr->schAttr[i].length;
		type = objAttr->schAttr[i].type;
		colName2 = (*objAttr->schemaRecord.columnVector)[i].name.c_str();
		issubcol = (*objAttr->schemaRecord.columnVector)[i].issubcol;
		begincol = (*objAttr->schemaRecord.columnVector)[i].begincol;
		endcol = (*objAttr->schemaRecord.columnVector)[i].endcol;

		if ( NULL != strchr( colName2.c_str(), ':' ) ) {
			continue;  // skip all bbox and :x :y :z columns
		}
		//if ( colName2 == "spare_" ) { continue;  // skip spare_ }

		//++ii; // index to other vector
		spare1 = (*objAttr->schemaRecord.columnVector)[i].spare[1];
		spare4 = (*objAttr->schemaRecord.columnVector)[i].spare[4];
		if ( spare1 == JAG_C_COL_TYPE_UUID_CHAR )  {
			// this column is uuid
			if ( hasAllCols ) { ++ii; } 
		} else if ( hasDefaultValue(spare4 ) ) {
			if ( hasAllCols ) { ++ii; } 
		} else {
			++ii;
		}

		if ( first ) {
			first = false;
		} else {
			newquery += Jstr(",");
		}

		if ( type == JAG_C_COL_TYPE_POINT ) {
			newquery += getCoordStr( "point", parseParam, ii, 1, 1, 0, 0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_POINT3D ) {
			newquery += getCoordStr( "point3d", parseParam, ii, 1, 1, 1, 0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE ) {
			newquery += getCoordStr( "circle", parseParam, ii, 1, 1, 0, 1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_CIRCLE3D ) {
			newquery += get3DPlaneCoordStr( "circle3d", parseParam, ii, 1, 1, 1, 1,0,0, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_SPHERE ) {
			newquery += getCoordStr( "sphere", parseParam, ii, 1, 1, 1, 1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_SQUARE ) {
			newquery += getSquareCoordStr( "square", parseParam, ii );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_SQUARE3D ) {
			newquery += get3DPlaneCoordStr( "square3d", parseParam, ii, 1, 1, 1, 1,0,0,1,1,0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_CUBE ) {
			newquery += get3DPlaneCoordStr( "cube", parseParam, ii, 1,1,1, 1,0,0, 1,1,0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE ) {
			newquery += get3DPlaneCoordStr( "rectangle", parseParam, ii, 1,1,0, 1,0,1, 1,0,0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_RECTANGLE3D ) {
			newquery += get3DPlaneCoordStr( "rectangle3d", parseParam, ii, 1, 1, 1, 1,0,1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE3D ) {
			newquery += get3DPlaneCoordStr( "ellipse3d", parseParam, ii, 1,1,1, 1,0,1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_BOX ) {
			newquery += get3DPlaneCoordStr( "box", parseParam, ii, 1, 1, 1, 1, 1, 1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_CYLINDER ) {
			newquery += get3DPlaneCoordStr( "cylinder", parseParam, ii, 1, 1, 1, 1, 0, 1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_CONE ) {
			newquery += get3DPlaneCoordStr( "cone", parseParam, ii, 1, 1, 1,  1, 0, 1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSE ) {
			newquery += get3DPlaneCoordStr( "ellipse", parseParam, ii, 1,1,0,  1,0,1, 1,0,0 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_ELLIPSOID ) {
			newquery += get3DPlaneCoordStr( "ellipsoid", parseParam, ii, 1, 1, 1, 1, 1, 1, 1,1,1 );
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINE ) {
			prt(("c1128 newquery=[%s]\n", newquery.c_str() ));
			newquery += getLineCoordStr( "line", parseParam, ii, 1, 1, 0, 1, 1, 0 );
			prt(("c1128 newquery=[%s]\n", newquery.c_str() ));
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINE3D ) {
			prt(("c1148 newquery=[%s]\n", newquery.c_str() ));
			newquery += getLineCoordStr( "line3d", parseParam, ii, 1, 1, 1, 1, 1, 1 );
			prt(("c1148 newquery=[%s]\n", newquery.c_str() ));
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("linestring(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_LINESTRING3D ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("linestring3d(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("multipoint(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("multipoint3d(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_POLYGON ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("polygon(") + parseParam.otherVec[ii].valueData  + ")";
				prt(("s2093 POLYGON valuedata=[%s]\n", parseParam.otherVec[ii].valueData.c_str() ));
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_POLYGON3D ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("polygon3d(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("multipolygon") + parseParam.otherVec[ii].valueData;
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			if ( parseParam.otherVec[ii].valueData.size() > 0 ) {
				newquery += Jstr("multipolygon3d") + parseParam.otherVec[ii].valueData;
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("multilinestring(") + parseParam.otherVec[ii].valueData  + ")";
				//prt(("s2053 MULTILINESTRING valuedata=[%s]\n", parseParam.otherVec[ii].valueData.c_str() ));
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("multilinestring3d(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_RANGE ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += Jstr("range(") + parseParam.otherVec[ii].valueData  + ")";
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += getTriangleCoordStr( "triangle", parseParam, ii, 1, 1, 0, 1, 1, 0, 1, 1, 0 );
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else if ( type == JAG_C_COL_TYPE_TRIANGLE3D ) {
			if ( parseParam.otherVec[ii].valueData.size()>0 ) {
				newquery += getTriangleCoordStr( "triangle3d", parseParam, ii, 1, 1, 1, 1, 1, 1, 1, 1, 1 );
			} else {
				newquery += Jstr("''");
			}
			lastIsGeo = true;
			continue;
		} else {
			//newquery += Jstr("''");
			lastIsGeo = false;
		}

		if ( issubcol ) { 
			//prt(("c0392 name=[%s] type=[%s] issubcol=%d true, skip add newquery\n", colName2.c_str(), type.c_str(), issubcol ));
			continue; 
		}
		//prt(("c0393 name=[%s] type=[%s] issubcol=%d\n", colName2.c_str(), type.c_str(), issubcol ));

		hquote = 0;
		useValueData = 0;
		// colName2 = (*objAttr->schemaRecord.columnVector)[i].name;
		if ( spare1 == JAG_C_COL_TYPE_UUID_CHAR )  {
			// this column is uuid
			sig = 0;
			type = JAG_C_COL_TYPE_STR;
			//colName = "UUID";
			colName2 = "UUID";
			if ( hasAllCols ) {
				colData = parseParam.otherVec[ii].valueData;
				hquote = parseParam.otherVec[ii].hasQuote;
				useValueData = 1;
			} else {
				colData = _jagUUID->getString();
			}
		} else if ( spare4 == JAG_CREATE_DEFUPDATETIME || spare4 == JAG_CREATE_DEFUPDATE ||
					spare4 == JAG_CREATE_DEFDATETIME || spare4 == JAG_CREATE_DEFDATE ) {
			// this column is timestamp/datetime, with default as current time
			sig = 0;
			type = (*objAttr->schemaRecord.columnVector)[i].type;
			if ( hasAllCols ) {
					colData = parseParam.otherVec[ii].valueData;
					hquote = parseParam.otherVec[ii].hasQuote;
					useValueData = 1;
			} else {
					char timebuf[JAG_FUNC_NOW_LEN_MICRO+1];
					char timebuf2[JAG_FUNC_CURDATE_LEN+1];
					getTimebuf( objAttr, timebuf, timebuf2 );
					if ( spare4  == JAG_CREATE_DEFUPDATE || spare4 == JAG_CREATE_DEFDATE ) {
						colData = timebuf2;
					} else {
						colData = timebuf;
					}
			}
		} else {
			// prt(("c0172 ...\n" ));
			sig = objAttr->schAttr[i].sig;
			type = objAttr->schAttr[i].type;
			//colName = (*objAttr->schemaRecord.columnVector)[i].name.c_str();
			//colName = colName2;

			colData = parseParam.otherVec[ii].valueData;
			hquote = parseParam.otherVec[ii].hasQuote;
			useValueData = 1;
			//prt(("c5432 ii=%d colData=[%s]\n", ii, colData.c_str() ));
			//prt(("c5433 colData=[%s]\n", parseParam.otherVec[i].valueData.c_str() ));

			if ( spare1 == JAG_C_COL_TYPE_FILE_CHAR && filevec ) {
				if ( _debug ) { prt(("c63830 JAG_C_COL_TYPE_FILE ... i=%d colData=[%s]\n", i, colData.c_str() )); }
				// for file object, parse and get the filename only 
				Jstr oripath = colData;
				const char *lp = strrchr( colData.c_str(), '/' );
				if ( lp ) { colData = lp+1; }
				if ( _datcSrcType != JAG_DATACENTER_GATE ) {
					// rewrite filepath if called by datacenter client in jaguarserver
					if ( _isDataCenterSync ) {
						Jstr hdir = fileHashDir( kstr );
						oripath = jaguarHome() + "/data/" + parseParam.objectVec[0].dbName + "/" 
								  + parseParam.objectVec[0].tableName + "/files/" + hdir + "/" + colData;
					}

					if ( oripath.size() <= 0 ) { colData = ""; }
				}
			}
		}

		// check if colData is "NULL", regard as empty
		if ( useValueData && !hquote && strcasecmp( colData.c_str(), "NULL" ) == 0 ) {
			colData = "";
		}

		if ( i < objAttr->numKeys && colData.size() > 0 ) {
			//rc = formatOneCol( _tdiff, _tdiff, kbuf, colData.c_str(), errmsg, colName.c_str(), offset, length, sig, type );
			rc = formatOneCol( _tdiff, _tdiff, kbuf, colData.c_str(), errmsg, colName2.c_str(), offset, length, sig, type );
			if ( !rc ) {
				_mapLock->readUnlock( -1 );
				_debug && prt(("c8039 return 0 i=%d objAttr->numKeys=%d\n", i, objAttr->numKeys, objAttr->numKeys ));
				errmsg = Jstr("E0801 Error insert for ") + colName2.c_str() + " formatOneCol error";
				return 0;
			}
		}

		// check first column value
		if ( ! parseParam.hasPoly && (0 == i) && (colData.size() < 1) ) {
			errmsg = Jstr("E0811 Error insert for ") + hdbobj.c_str() 
					 				+ ". Data of first column " + colName2.c_str() + " must be provided.";
			_mapLock->readUnlock( -1 );
			return 0;
		}

		newquery += Jstr ( "'" ) + colData + "'";
		//prt(("c3935 added i=%d colData=[%s]\n", i, colData.c_str() ));
		//prt(("c3048 newquery=[%s]\n", newquery.c_str() ));

	}  // end for loop ( int i = 0; i < numCols; ++i  )
	// end of all cols i


	newquery += Jstr(");");  // must end with ;
	//prt(("c3091 newquery=[%s]\n", newquery.c_str() ));

	dbNaturalFormatExchange( kbuf, objAttr->numKeys, objAttr->schAttr, 0, 0, " " ); // natural format -> db format
	kstr = JagFixString( kbuf, objAttr->keylen );
	// if ( _debug ) { prt(("c5038 kstr:\n" )); kstr.dump(); }
	// kstr.dump();
	JagFixString hostkstr;
	if ( objAttr->schemaRecord.hasMute ) {
		getHostKeyStr( kbuf, objAttr, hostkstr );
	} else {
		hostkstr.point( kstr );
	}

	/////////////////////////////////////////////////////////////
	// filevec
	Jstr hdir, oripath;
	const char *lp;
	ii = -1;
	int otherSize = parseParam.otherVec.size();
	for ( int i = 0; i < numCols-1; ++i ) {
		colName2 = (*objAttr->schemaRecord.columnVector)[i].name.c_str();
		if ( strchr( colName2.c_str(), ':' ) ) {
			continue;
		}
		++ii;
		if ( ii < otherSize ) {
			colData = parseParam.otherVec[ii].valueData;
		} else {
			colData = "";
		}

		if ( spare1  == JAG_C_COL_TYPE_FILE_CHAR && filevec ) {
				// for file object, parse and get the filename only 
				if ( _debug ) { prt(("c7202 colData=[%s]\n", colData.c_str() )); }
				oripath = colData;
				lp = strrchr( colData.c_str(), '/' );
				if ( lp ) { colData = lp+1; }
				if ( _datcSrcType != JAG_DATACENTER_GATE ) {
					// rewrite filepath if called by datacenter client in jaguarserver
					if ( _isDataCenterSync ) {
						hdir = fileHashDir( kstr );
						oripath = jaguarHome() + "/data/" + parseParam.objectVec[0].dbName + "/" 
							    + parseParam.objectVec[0].tableName + "/files/" + hdir + "/" + colData;
							// prt(("c2031 modified oripath=[%s]\n", oripath.c_str() ));
					}

					if ( oripath.size() > 0 ) {
						oripath = expandEnvPath( oripath );
						filevec->append( oripath );
					} else {
						filevec->append( "." );
					}

					if ( oripath.size() > 0 ) {
						if ( 0 !=  access( oripath.c_str(), R_OK ) ) {
    						_mapLock->readUnlock( -1 );
    						errmsg = Jstr("E7102 File ") + colData + " cannot be open";
							if ( _debug ) { prt(("c5202 errmsg=[%s]\n", errmsg.c_str() )); }
    						return 0;
						}
					} 
				}
		}
	}
	////////////////////////////////////////////////////////////

    addQueryToCmdhosts( parseParam, hostkstr, objAttr, hosts, cmdhosts, newquery );
	//prt(("c8271 after addQueryToCmdhosts newquery=[%s]\n", newquery.c_str() ));

	_mapLock->readUnlock( -1 );
	return 1;
}

// method to process schema change related 'C' type, read related 'R' type and update/delete cmds
// returns:
//     JAG_END_RECVONE_THEN_DONE_ERR: abort/rollback last commit, error
//     JAG_END_RECVONE_THEN_DONE: commit finish for special command
//     JAG_END_NORMAL: update/delete
//     JAG_END_GOT_DBPAIR_AND_ORDERBY: done order by
//     JAG_END_GOT_DBPAIR: regular non orderby was done
//     0: error
int JaguarCPPClient::processMultiServerCommands( const char *qstr, JagParseParam &parseParam, Jstr &errmsg )
{	
	_debug && prt(("c8823 processMultiServerCommands [%s]\n", qstr ));

	int rc, isInsertSelect = 0;
	AbaxString hdbobj;
	Jstr newquery;
	const JagTableOrIndexAttrs *objAttr = NULL;
	_mapLock->readLock( -1 );
	if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
		if ( _debug ) { prt(("c4029 checkCmdTableIndexExist false\n")); }
		// table does not exist, retry
		_mapLock->readUnlock( -1 );

		_parentCli->queryDirect( "_cschema" ); _parentCli->replyAll();
		if ( _schemaUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			rebuildSchemaMap();
			updateSchemaMap( _schemaUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_schemaUpdateString = "";
		}

		/***
		_parentCli->queryDirect( "_cdefval" ); _parentCli->replyAll();
		if ( _defvalUpdateString.size() > 0 ) {
			_mapLock->writeLock( -1 );
			//rebuildSchemaMap();
			updateDefvalMap( _defvalUpdateString.c_str() );
			_mapLock->writeUnlock( -1 );
			_defvalUpdateString = "";
		}
		***/

		_mapLock->readLock( -1 );
		if ( !checkCmdTableIndexExist( parseParam, hdbobj, objAttr, errmsg ) ) {
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}

	// update/delete from index invalid
	if ( (JAG_UPDATE_OP == parseParam.opcode || JAG_DELETE_OP == parseParam.opcode) && objAttr->indexName.size() > 0 ) {
		errmsg = Jstr("E3028 error update/delete directly from index ") + hdbobj.c_str();
		_mapLock->readUnlock( -1 );
		return 0;
	}
	
	// special commands, create/drop db/user/table/index, truncate, alter, changepass, use db
	// prt(("c3748 parseParam.optype=[%c] qstr=[%s]\n", parseParam.optype, qstr ));
	if ( parseParam.optype == 'C' ) { 
		// commands which may affect schema or userid, e.g. create ( table/index/user/db ), drop ( table/index/user/db )
		// changepass, truncate, alter
		if ( !checkSpecialCommandValidation( qstr, newquery, parseParam, errmsg, hdbobj, objAttr ) ) {
			_mapLock->readUnlock( -1 );
			_debug && prt(("c2033 checkSpecialCommandValidation false return 0\n" ));
			return 0;
		}
		_mapLock->readUnlock( -1 );
		// format query command to get query with dbname
		// prt(("c2035 rebuildCommandWithDBName oldquery=[%s]\n", newquery.c_str() ));
		// rebuildCommandWithDBName( newquery.c_str(), parseParam, dbquery );
		if (  parseParam.dbNameCmd.size() > 0 ) {
			newquery = parseParam.dbNameCmd;
		} else {
			parseParam.dbNameCmd = newquery;
		}

		// prt(("c2037 rebuildCommandWithDBName newquery=[%s]\n", newquery.c_str() ));
		_debug && prt(("c5082 newquery=[%s]\n", newquery.c_str() ));

		JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );
		pthread_t thrd[_numHosts];
		CliPass pass[_numHosts];
		_spCommandErrorCnt = 0;

		/*** parallel, disabled for now
		for ( int i = 0; i < _numHosts; ++i ) {
			// prt(("c4825 broadcastAllRejectFailureStatic _host=[%s] host=[%s] ...\n", 
					_host.c_str(), (*(_parentCli->_allHosts))[i].c_str() ));
			pass[i].cli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(_parentCli->_allHosts))[i].c_str() );
			if ( ! pass[i].cli ) {
				// prt(("c9384 pass[%d].cli is NULL, skip\n", i ));
				continue;
			}
			pass[i].cmd = newquery;
			pass[i].passMode = 2;
			jagpthread_create( &thrd[i], NULL, broadcastAllRejectFailureStatic, (void*)&pass[i] );
			// prt(("c6402 jagpthread_create i=%d done\n", i ));
		}

		for ( int i = 0; i < _numHosts; ++i ) {
			if ( pass[i].cli ) {
				prt(("c3841 pthread_join i=%d/%d ...\n", i, _numHosts ));
				pthread_join(thrd[i], NULL);
				prt(("c3841 pthread_join i=%d/%d done\n", i, _numHosts ));
			}
		}
		***/

		// sequential
		bool syncDataCenter = true;
		for ( int i = 0; i < _numHosts; ++i ) {
			if ( _debug ) { prt(("c5881 jag_hash_lookup(%s) ...\n", (*(_parentCli->_allHosts))[i].c_str() )); }
			pass[i].cli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(_parentCli->_allHosts))[i].c_str() );
			if ( ! pass[i].cli ) { continue; }
			pass[i].cmd = newquery;
			pass[i].passMode = 2;
			_debug && prt(("c2034 broadcastAllRejectFailureStatic1...\n" ));
			broadcastAllRejectFailureStatic1( (void*)&pass[i] );
		}

		for ( int i = 0; i < _numHosts; ++i ) {
			if ( ! pass[i].cli ) { continue; }
			_debug && prt(("c2034 broadcastAllRejectFailureStatic2...\n" ));
			broadcastAllRejectFailureStatic2( (void*)&pass[i] );
		}

		for ( int i = 0; i < _numHosts; ++i ) {
			if ( ! pass[i].cli ) { continue; }
			_debug && prt(("c2034 broadcastAllRejectFailureStatic3...\n" ));
			broadcastAllRejectFailureStatic3( (void*)&pass[i] );
		}

		for ( int i = 0; i < _numHosts; ++i ) {
			if ( ! pass[i].cli ) { continue; }
			pass[i].syncDataCenter = syncDataCenter;
			_debug && prt(("c2034 broadcastAllRejectFailureStatic4...\n" ));
			broadcastAllRejectFailureStatic4( (void*)&pass[i] );
			syncDataCenter = pass[i].syncDataCenter;
		}

		rc = formatReturnMessageForSpecialCommand( parseParam, errmsg );
		if ( hasError() ) {
			errmsg += Jstr(" [") + error() + "]";
		}

		if ( rc ) {
			_debug && prt(("c0391 JAG_END_RECVONE_THEN_DONE\n" ));
			return JAG_END_RECVONE_THEN_DONE; // commit finish for special command
		} else {
			_debug && prt(("c0392 JAG_END_RECVONE_THEN_DONE_ERR\n" ));
			return JAG_END_RECVONE_THEN_DONE_ERR; // abort/rollback last commit, error
		}
	}
	
	if ( JAG_INSERTSELECT_OP == parseParam.opcode ) {
		isInsertSelect = 1;
	}
	_lastOpCode = parseParam.opcode;
	_lastHasGroupBy = parseParam.hasGroup; 
	_lastHasOrderBy = parseParam.hasOrder; 
	// prt(("c3829 _lastOpCode=%d _lastHasGroupBy=%d _lastHasOrderBy=%d\n", _lastOpCode, _lastHasGroupBy, _lastHasOrderBy ));
	errmsg = "";
	JagFixString kstr, treestr;
	int num, beginnum, typeMode = 0, tabnum = 0;
	if ( !isInsertSelect ) {
		num = parseParam.objectVec.size();
		beginnum = 0;
	} else {
		num = parseParam.objectVec.size()-1;
		beginnum = 1;
	}
	if ( num <= 0 ) {
		num = 1;
		beginnum = 0;
	}
	bool uniqueAndHasValueCol = 0, hasAggregate = 0;
	int numCol, grouplen, keylen[num], numKeys[num], numCols[num];
	Jstr dbobjs[num];
	Jstr schStrings[num];
	JagSchemaRecord records[num];
	const JagHashStrInt *maps[num];
	const JagSchemaAttribute *attrs[num];
	JagVector<int> selectPartsOpcode;
	JagVector<int> selColSetAggParts;
	JagHashMap<AbaxInt, AbaxInt> selColToselParts;
	JagMinMax minmax[num];
	keylen[0] = 0;
	Jstr sendquery;
	bool isPointQuery = false;
	int nqHasLimit = 0;
	abaxint limitNum = 0;
	JagParseParam parseParam2;
	JagVector<Jstr> *uhosts;
	JagVector<Jstr> phosts;
	bool rc2;
	
	// setup local variables
	for ( int i = 0; i < num; ++i ) {
		// db.tab or db.idx
		if ( parseParam.objectVec[i+beginnum].indexName.length() > 0 ) {
			dbobjs[i] = parseParam.objectVec[i+beginnum].dbName + "." + parseParam.objectVec[i+beginnum].indexName;
			// prt(("c0838 index dbobjs[%d]=[%s]\n", i, dbobjs[i].c_str() ));
		} else  {
			dbobjs[i] = parseParam.objectVec[i+beginnum].dbName + "." + parseParam.objectVec[i+beginnum].tableName;
			// prt(("c0838 table dbobjs[%d]=[%s]\n", i, dbobjs[i].c_str() ));
		}

		// rc2 = _schemaMap->getValue( AbaxString(dbobjs[i]), *objAttr );
		objAttr = _schemaMap->getValue( AbaxString(dbobjs[i]) );
		if ( ! objAttr ) {
			/***
			prt(("s7339 error getValue for _schemaMap=%0x  dbobj=[%s] skip: printKeyStringOnly:\n", 
					_schemaMap,  dbobjs[i].c_str()  ));
			_schemaMap->printKeyStringOnly();
			prt(("\n.\n"));
			***/
			continue;
		}
		// prt(("c8330 _schemaMap->getValue i=%d dbobjs[i]=[%s] getValue rc2=%d\n", i, dbobjs[i].c_str(), rc2 ));
		keylen[i] = objAttr->keylen;
		numKeys[i] = objAttr->numKeys;
		numCols[i] = objAttr->numCols;
		records[i] = objAttr->schemaRecord;
		schStrings[i] = objAttr->schemaString;
		maps[i] = &(objAttr->schmap);
		attrs[i] = objAttr->schAttr;
		/***
		prt(("s0393 i=%d maps[i]=%0x attrs[i]=%0x  schmap.size=%d numschAttr=%d\n", 
				maps[i], attrs[i], objAttr->schmap.size(), objAttr->numschAttr ));
		***/
		minmax[i].setbuflen( keylen[i] );
		//prt(("c2228 i=%d minmax[i].setbuflen (keylen=%d)\n", i, keylen[i]  ));
	}

	// if has update set part, check its validation
	if ( parseParam.updSetVec.size() > 0 ) {
		Jstr udbcol;
		int udbpos;
		for ( int i = 0; i < parseParam.updSetVec.size(); ++i ) {
			udbcol = dbobjs[0] + "." + parseParam.updSetVec[i].colName;
			if ( !maps[0]->getValue(udbcol, udbpos) ) {
				errmsg = "E3218 Error Update [";
				errmsg += udbcol + ":" + parseParam.origCmd + "]";
				_mapLock->readUnlock( -1 );
				return 0;
			}
		}
	}
	
	// if getfile, check columns validation
	if ( parseParam.opcode == JAG_GETFILE_OP ) {
		if ( _debug ) { prt(("c4582 JAG_GETFILE_OP ...\n" )); }
		Jstr udbcol;
		int udbpos;
		for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
			udbcol = dbobjs[0] + "." + parseParam.selColVec[i].getfileCol;
			// prt(("c2093 getfile=[%s] \n", parseParam.selColVec[i].getfilePath.c_str() ));
			if ( parseParam.selColVec[i].getfileType == JAG_GETFILE_ACTDATA  && 
			     ! JagFileMgr::pathWritable( parseParam.selColVec[i].getfilePath ) ) {
				errmsg = "E3021 Path of getfile column not writable";
				_mapLock->readUnlock( -1 );
				if ( _debug ) { prt(("c4482 JAG_GETFILE_OP %s ...\n", errmsg.c_str() )); }
				return 0;
			}

			if ( !maps[0]->getValue(udbcol, udbpos) || !attrs[0][udbpos].isFILE ) {
				errmsg = "E9208 Error getfile columns";
				_mapLock->readUnlock( -1 );
				if ( _debug ) { prt(("c4485 JAG_GETFILE_OP %s ...\n", errmsg.c_str() )); }
				return 0;
			}
		}
	}

	// if has join_on part, check its validation
	if ( parseParam.joinOnVec.size() > 0 ) {
		ExprElementNode *root = parseParam.joinOnVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, num, uniqueAndHasValueCol, minmax, 
								  treestr, typeMode, tabnum );
		if ( 0 == rc ) {
			for ( int i = 0; i < num; ++i ) {
				memset( minmax[i].minbuf, 0, keylen[i]+1 );
				memset( minmax[i].maxbuf, 255, keylen[i] );
				(minmax[i].maxbuf)[keylen[i]] = '\0';
			}
		} else if ( rc < 0 ) {
			errmsg = "E5303 Error JOIN ON clause";
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}
	
	// process where part
	if ( parseParam.hasWhere ) {
		ExprElementNode *root = parseParam.whereVec[0].tree->getRoot();
		//prt(("c2033 root->setWhereRange maps...\n" ));
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, num, uniqueAndHasValueCol, 
								  minmax, treestr, typeMode, tabnum );
	    //prt(("c20229 setWhereRange rc=%d\n", rc ));
		if ( 0 == rc ) {
			for ( int i = 0; i < num; ++i ) {
				memset( minmax[i].minbuf, 0, keylen[i]+1 );
				memset( minmax[i].maxbuf, 255, keylen[i] );
				(minmax[i].maxbuf)[keylen[i]] = '\0';
				//prt(("s10000 memset .minbuf .maxbuf \n" ));
			}
		} else if (  rc < 0 ) {
			errmsg = "E5104 Error where clause [";
			errmsg += parseParam.origCmd + "] " + intToStr(rc);
			_mapLock->readUnlock( -1 );
			return 0;
		}
	}

	if ( JAG_UPDATE_OP == parseParam.opcode || JAG_DELETE_OP == parseParam.opcode ) {
		// format query command to get query with dbname, for update and delete cmd
		// rebuildCommandWithDBName( qstr, parseParam, newquery );
		newquery = parseParam.dbNameCmd;
		if ( JAG_UPDATE_OP == parseParam.opcode && 
			( JAG_CREATE_DEFUPDATETIME == objAttr->defUpdDatetime || JAG_CREATE_UPDATETIME == objAttr->updDatetime ||
			  JAG_CREATE_DEFUPDATE == objAttr->defUpdDatetime || JAG_CREATE_UPDDATE == objAttr->updDatetime ) ) {
			// has update current_timestamp, rebuild update query
			struct tm res; 
			struct timeval now; 
			gettimeofday( &now, NULL );
			time_t snow = now.tv_sec; 
			jag_localtime_r( &snow, &res );
			char timebuf[JAG_FUNC_NOW_LEN_MICRO+1];
			memset( timebuf, 0, JAG_FUNC_NOW_LEN_MICRO+1 );
			char timebuf2[JAG_FUNC_CURDATE_LEN+1];
			memset( timebuf2, 0, JAG_FUNC_CURDATE_LEN+1 );
			sprintf( timebuf, "%4d-%02d-%02d %02d:%02d:%02d.%06d",
				res.tm_year+1900, res.tm_mon+1, res.tm_mday, res.tm_hour, res.tm_min, res.tm_sec, now.tv_usec );
			sprintf( timebuf2, "%4d-%02d-%02d",
				res.tm_year+1900, res.tm_mon+1, res.tm_mday );

			// const char *start = newquery.c_str(), *wstart = strcasestr( start, " where " );
			const char *start = newquery.c_str(), *wstart = strcasestrskipquote( start, " where " );
			sendquery = Jstr( start, wstart-start );
			// go through all columns, and set all default timestamp accordingly
			for ( int i = 0; i < numCols[0]; ++i ) {
				if ( *((*(objAttr->schemaRecord.columnVector))[i].spare+4) == JAG_CREATE_DEFUPDATE ||
					*((*(objAttr->schemaRecord.columnVector))[i].spare+4) == JAG_CREATE_UPDDATE ) {
					sendquery += Jstr(", ") + (*(objAttr->schemaRecord.columnVector))[i].name.c_str() +
						"='" + timebuf2 + "' ";
				} else if ( *((*(objAttr->schemaRecord.columnVector))[i].spare+4) == JAG_CREATE_DEFUPDATETIME ||
					*((*(objAttr->schemaRecord.columnVector))[i].spare+4) == JAG_CREATE_UPDATETIME ) {
					sendquery += Jstr(", ") + (*(objAttr->schemaRecord.columnVector))[i].name.c_str() +
						"='" + timebuf + "' ";
				}
			}
			sendquery += wstart;
		} else {
			sendquery = newquery;
		}
		newquery = "";
		rc = 1;
	} else if ( JAG_GETFILE_OP == parseParam.opcode ) { 
		// rebuildCommandWithDBName( qstr, parseParam, newquery );
		newquery = parseParam.dbNameCmd;
		sendquery = newquery;
		newquery = "";
		rc = 1;
		if ( _debug ) { prt(("c4185 JAG_GETFILE_OP sendquery%s ...\n", sendquery.c_str() )); }
	} else {
		rc = formatSendQuery( parseParam, parseParam2, maps, attrs, 
			selectPartsOpcode, selColSetAggParts, selColToselParts, nqHasLimit, hasAggregate,
			limitNum, grouplen, numCols, num, sendquery, errmsg );
		// prt(("c0057 formatSendQuery rc=%d\n", rc ));
		if ( !rc ) {
			_mapLock->readUnlock( -1 );
			return 0;
		}
		// if send query to GATE, use original query
		if ( _isToGate ) {
			sendquery = qstr;
		}
	}

	if ( _debug ) { prt(("s9999 sendQuery=[%s]\n", sendquery.c_str())); }
	//_debug && prt(("s2060 minmax[0].minbuf=[%s] minmax[0].maxbuf=[%s] keylen=%d\n",  minmax[0].minbuf, minmax[0].maxbuf , keylen[0] ));

	// begin process querys
	// for point query

	if ( !isJoin(parseParam.opcode) && memcmp( minmax[0].minbuf, minmax[0].maxbuf, keylen[0] ) == 0 ) { 
		//prt(("s0283 point query\n" ));
		// first, need to see if request object is table or index
		// if is index, get table kstr from index object
		// otherwise, format kstr using minbuf
		JagFixString hostkstr;
		if ( ! objAttr ) {
			_mapLock->readUnlock( -1 );
			errmsg = Jstr("E5054 error objAttr, no table found.");
			return 0;
		}

		if ( objAttr->indexName.size() > 0 ) { // is index
			const JagTableOrIndexAttrs *objAttr2 = NULL;
			AbaxString idxdbtab = objAttr->dbName + "." + objAttr->tableName;
			objAttr2 = _schemaMap->getValue( idxdbtab );
			if ( ! objAttr2 ) {
				_mapLock->readUnlock( -1 );
				errmsg = Jstr("E5100 error TableOrIndexAttrs for ") + idxdbtab.c_str();
				return 0;
			}
			// use get related table keystr from index buffer
			// objAttr is index's object pointer, and objAttr2 is related table's object
			AbaxString dbcol;
			int gpos;
			char tabbuf[objAttr2->keylen+1];
			memset( tabbuf, 0, objAttr2->keylen+1 );
			for ( int i = 0; i < objAttr2->numKeys; ++i ) {
				dbcol = hdbobj + "." + objAttr2->schAttr[i].colname;
				if ( objAttr->schmap.getValue( dbcol.c_str(), gpos ) ) {
					memcpy( tabbuf+objAttr2->schAttr[i].offset, minmax[0].minbuf+objAttr->schAttr[gpos].offset,
							objAttr->schAttr[gpos].length );
				}
			}
			kstr = JagFixString( tabbuf, objAttr2->keylen );
			if ( objAttr2->schemaRecord.hasMute ) {
				getHostKeyStr( tabbuf, objAttr2, hostkstr );
			} else {
				hostkstr.point( kstr );
			}
		} else {
			kstr = JagFixString( minmax[0].minbuf, keylen[0] );
			if ( objAttr->schemaRecord.hasMute ) {
				getHostKeyStr( minmax[0].minbuf, objAttr, hostkstr );
			} else {
				hostkstr.point( kstr );
			}
		}
		getUsingHosts( phosts, hostkstr, 0 );
		isPointQuery = true;
		//prt(("actual hash kstr=[%s], minbuf=[%s]\n", kstr.c_str(), minmax[0].minbuf ));
	}

	if ( phosts.size() > 0 ) uhosts = &phosts;
	else uhosts = _parentCli->_allHosts;
	_mapLock->readUnlock( -1 );
	
	if ( parseParam.opcode == JAG_GETFILE_OP && !isPointQuery ) {
		errmsg = "E2580 getfile command must be point query";
		if ( _debug ) { prt(("c5003 errmsg=%s\n", errmsg.c_str() )); }
		return 0;
	}
	
	bool noQueryButReply = false;
	int hasError = 0, isSelectMode = 0, joinEachCnt[2] = {0};
	abaxint finalsendlen, gbvsendlen, selcount = 0;
	Jstr finalFileHeader, gbvFileHeader;
	if ( parseParam.optype == 'R' && isPointQuery ) isSelectMode = 1;
	else if ( parseParam.optype == 'R' && !isPointQuery ) isSelectMode = 2;
	if ( JAG_COUNT_OP == parseParam.opcode ) isSelectMode = 3;
	
	// select range command, set jda and related members
	if ( 2 == isSelectMode ) {
		_jda->setwrite( (*(uhosts)) );
		_selectCountBuffer->clean();
		for ( int i = 0; i < uhosts->size(); ++i ) {
			_selectCountBuffer->append( 0 );
		}
	}

	_multiThreadMidPointStart = 0;
	_multiThreadMidPointStop = 0;
	// begin parallel requests for select/update/delete
	pthread_t thrd[uhosts->size()];
	CliPass pass[uhosts->size()];
	for ( int i = 0; i < uhosts->size(); ++i ) {
		if ( _debug ) { prt(("c8821 jag_hash_lookup(%s) ...\n", (*(uhosts))[i].c_str() )); }
		pass[i].cli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(uhosts))[i].c_str() );
		pass[i].idx = i;
		pass[i].totnum = uhosts->size();
		pass[i].start = parseParam.limitStart;
		pass[i].cmd = sendquery;
		pass[i].hasLimit = nqHasLimit;
		pass[i].cnt = limitNum;
		pass[i].passMode = 0;
		pass[i].selectMode = isSelectMode;
		pass[i].parseParam = &parseParam;
		pass[i].isToGate = _isToGate;
		pass[i].redirectSock = _redirectSock;
		if ( parseParam.hasOrder ) pass[i].needAll = 1;
		if ( isJoin(parseParam.opcode) ) pass[i].joinEachCnt = joinEachCnt;

		pass[i].cli->_lastOpCode = this->_lastOpCode;
		pass[i].cli->_lastHasGroupBy = this->_lastHasGroupBy;
		pass[i].cli->_lastHasOrderBy = this->_lastHasOrderBy;

		if ( uhosts->size() > 1 ) {
			if ( parseParam.optype == 'R' ) {
				if ( _debug )  { prt(("c1908 uhosts->size()=%d R\n", uhosts->size() )); }
				jagpthread_create( &thrd[i], NULL, broadcastAllSelectStatic, (void*)&pass[i] );
			} else {
				if ( _debug )  { prt(("c1908 uhosts->size()=%d !R\n", uhosts->size() )); }
				jagpthread_create( &thrd[i], NULL, broadcastAllRegularStatic, (void*)&pass[i] );
			}
		} else {
			if ( parseParam.optype == 'R' ) {
				if ( _debug )  { prt(("c1909 uhosts->size() <=1 R\n" )); }
				broadcastAllSelectStatic( (void*)&pass[i] );
			} else {
				if ( _debug )  { prt(("c1909 uhosts->size() <=1 !R\n" )); }
				broadcastAllRegularStatic( (void*)&pass[i] );
			}
		}
	}

	if ( _debug )  { prt(("c2008 begin static 2 ...\n" )); }

	// begin static 2
	if ( uhosts->size() > 1 && parseParam.optype == 'R' ) {
		while ( _multiThreadMidPointStop != uhosts->size() ) {
			jagsleep( 1, JAG_MSEC );
		}
		_multiThreadMidPointStart = 1;
		while ( _multiThreadMidPointStop != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		_multiThreadMidPointStart = 0;
	}

	if ( _debug )  { prt(("c2008 begin static 3 ...\n" )); }
	// begin static 3
	if ( uhosts->size() > 1 && parseParam.optype == 'R' ) {
		while ( _multiThreadMidPointStop != uhosts->size() ) {
			jagsleep( 1, JAG_MSEC );
		}
		_multiThreadMidPointStart = 1;
		while ( _multiThreadMidPointStop != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		_multiThreadMidPointStart = 0;

	}

	if ( _debug )  { prt(("c2008 begin static 4 ...\n" )); }
	// begin static 4
	if ( uhosts->size() > 1 && parseParam.optype == 'R' ) {
		while ( _multiThreadMidPointStop != uhosts->size() ) {
			jagsleep( 1, JAG_MSEC );
		}
		_multiThreadMidPointStart = 1;
		while ( _multiThreadMidPointStop != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		_multiThreadMidPointStart = 0;
	}

	if ( _debug )  { prt(("c2008 begin static 5 ...\n" )); }
	// begin static 5
	if ( uhosts->size() > 1 && parseParam.optype == 'R' ) {
		while ( _multiThreadMidPointStop != uhosts->size() ) {
			jagsleep( 1, JAG_MSEC );
		}
		_multiThreadMidPointStart = 1;
		while ( _multiThreadMidPointStop != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		_multiThreadMidPointStart = 0;
	}

	if ( _debug )  { prt(("c2008 begin static 6 ...\n" )); }
	// begin static 6
	if ( uhosts->size() > 1 && parseParam.optype == 'R' ) {
		while ( _multiThreadMidPointStop != uhosts->size() ) {
			jagsleep( 1, JAG_MSEC );
		}
		_multiThreadMidPointStart = 1;
		while ( _multiThreadMidPointStop != 0 ) {
			jagsleep( 10, JAG_USEC );
		}
		_multiThreadMidPointStart = 0;
	}

	// for select point and range query command, format FH on client side
	if ( ( 1 == isSelectMode || 2 == isSelectMode ) ) {
		JagVector<SetHdrAttr> hspa;
		SetHdrAttr honespa;
		for ( int i = 0; i < num; ++i ) {
			honespa.setattr( numKeys[i], false, dbobjs[i], &records[i], schStrings[0].c_str() );
			hspa.append( honespa );
		}
		// format original query FH
		// prt(("c6350 rearrangeHdr original query FH\n"));
		rearrangeHdr( num, maps, attrs, &parseParam, hspa, finalFileHeader, gbvFileHeader, finalsendlen, gbvsendlen, false );
		// format sent query FH
		//prt(("c6351 rearrangeHdr sent query FH\n"));
		rearrangeHdr( num, maps, attrs, &parseParam2, hspa, _dataFileHeader, gbvFileHeader, finalsendlen, gbvsendlen, false );
		// if ( parseParam.opcode == JAG_GETFILE_OP || _isToGate ) 
		if ( parseParam.opcode == JAG_GETFILE_OP || ( _isToGate && !hasAggregate ) ) {
			if ( _debug ) { prt(("c5013 isSelectMode=%d _isToGate=%d\n", isSelectMode, _isToGate )); }
			_dataFileHeader = finalFileHeader;
		}
		//prt(("c6352 rearrangeHdr sent query FH\n"));
	}
	// if ( uhosts->size() > 1 ) thpool_wait( _insertPool );
	if ( uhosts->size() > 1 ) {
		if ( _debug )  { prt(("c2018 join threads ...\n" )); }
		for ( int i = 0; i < uhosts->size(); ++i ) {
			pthread_join(thrd[i], NULL);
		}
	}

	// then, pthread_join concurrent jobs
	for ( int i = 0; i < uhosts->size(); ++i ) {
		if ( 0 == isSelectMode ) {
			if ( pass[i].cli->_replyerrmsg.size() > 0 ) {
				errmsg = pass[i].cli->_replyerrmsg;
				if ( _debug )  { prt(("c2048 errmsg=%s ...\n", errmsg.c_str() )); }
				return 0;
			}
		} else {
			if ( 3 == isSelectMode ) selcount += pass[i].cnt;
			if ( 1 == isSelectMode && pass[i].cli->_pointQueryString.size() ) {
				_pointQueryString = pass[i].cli->_pointQueryString;
				if ( pass[i].cli != _parentCli ) {
					pass[i].cli->_pointQueryString = "";
				}
			}
			// check if there error from thread result of too large tmp files on client side
			if ( 2 == isSelectMode && pass[i].hasError ) {
				hasError = 1;
				errmsg = pass[i].errmsg;
			}
		}
		if ( pass->noQueryButReply ) noQueryButReply = true;
	}

	if ( hasError || errmsg.size() > 0 ) {
		if ( _debug )  { prt(("c2044 hasError=%d errmsg=%s ...\n", hasError, errmsg.c_str() )); }
		return 0;
	}

	if ( 0 == isSelectMode ) {
		return JAG_END_NORMAL; // update/delete, no need to check methods below
	} else if ( 3 == isSelectMode ) {
		// select count(*)
		_dataSelectCount = Jstr( hdbobj.c_str() ) + " contains " + longToStr( selcount ) + " rows";
		return JAG_END_GOT_DBPAIR;
	} else if ( 2 == isSelectMode ) _jda->flushwrite();
	
	if ( noQueryButReply ) {
		// getfile with GATE, return JAG_END_NOQUERY_BUT_REPLY
		return JAG_END_NOQUERY_BUT_REPLY;
	}

	if ( _debug ) { 
		prt(("c4021 _isToGate=%d hasAggregate=%d _jda->getdatalen()=%d\n", _isToGate, hasAggregate, _jda->getdatalen() )); 
	}
	
	// process aggregation functions, also applied for order by simple object
	if ( hasAggregate && _jda->getdatalen() > 0 ) {
		JagSchemaRecord aggrec;
		rc = aggrec.parseRecord( _dataFileHeader.c_str() );
		if ( rc < 0 ) { prt(("c9473 should not happen\n")); abort(); }
		if ( aggrec.columnVector->size() != selectPartsOpcode.size() ) { prt(("c9473 should not happen\n")); abort(); }
		JagFixString aggstr;
		int datalen = _jda->getdatalen();
		int dcountbegin = parseParam.groupVec.size();
		JagVector<JagFixString> oneSetData;
		char *aggbuf = (char*)jagmalloc(datalen+1);
		char *finalbuf = (char*)jagmalloc(finalsendlen+1);
		memset(aggbuf, 0, datalen+1);
		memset(finalbuf, 0, finalsendlen+1);
		_dataFileHeader = finalFileHeader;

		if ( 1 == _jda->elements( datalen ) ) { // if only one line of data
			while ( _jda->readit( aggstr ) ) {
				memcpy( aggbuf, aggstr.c_str(), datalen );
			}
			reformOriginalAggQuery( aggrec, selectPartsOpcode, selColSetAggParts, selColToselParts, aggbuf, finalbuf, parseParam );
			_aggregateData = JagFixString( finalbuf, finalsendlen );
		} else if ( !parseParam.hasGroup ) { // not group by sql command, one set of data
			while ( _jda->readit( aggstr ) ) {
				oneSetData.append( aggstr );
			}
			if ( oneSetData.length() > 0 ) {
				doAggregateCalculation( aggrec, selectPartsOpcode, oneSetData, aggbuf, datalen, dcountbegin );
				reformOriginalAggQuery( aggrec, selectPartsOpcode, selColSetAggParts, selColToselParts, aggbuf, finalbuf, parseParam );
				oneSetData.clean();
				_aggregateData = JagFixString( finalbuf, finalsendlen );	
			}
		} else { // group by, multiple sets of data
			JagDataAggregate *jda2 = new JagDataAggregate( false );
			jda2->setwrite( "aggGroup", "aggGroup" );
			_jda->beginJoinRead( grouplen );
			while ( _jda->joinReadNext( oneSetData ) ) {
				if ( oneSetData.length() > 0 ) {
					doAggregateCalculation( aggrec, selectPartsOpcode, oneSetData, aggbuf, datalen, dcountbegin );
					reformOriginalAggQuery( aggrec, selectPartsOpcode, selColSetAggParts, selColToselParts, aggbuf, finalbuf, parseParam );
					jda2->writeit( "aggGroup", finalbuf, finalsendlen );
					oneSetData.clean();
				}
			}
			_jda->endJoinRead();
			jda2->flushwrite();
			if ( _jda ) delete _jda;
			_jda = jda2;
		}
		if ( aggbuf ) free( aggbuf );
		if ( finalbuf ) free ( finalbuf );
	}


	// also reform no aggregate and has group by
	// if ( !_isToGate && !hasAggregate && parseParam.hasGroup && _jda->getdatalen() > 0 ) 
	if ( !hasAggregate && parseParam.hasGroup && _jda->getdatalen() > 0 ) {
		JagFixString aggstr;
		JagVector<JagFixString> oneSetData;
		_dataFileHeader = finalFileHeader;

		if ( 1 == _jda->elements( _jda->getdatalen() ) ) { // if only one line of data
			while ( _jda->readit( aggstr ) ) {
				_aggregateData = JagFixString( aggstr.c_str()+grouplen, finalsendlen );
			}
		} else { // group by, multiple sets of data
			JagDataAggregate *jda2 = new JagDataAggregate( false );
			jda2->setwrite( "aggGroup", "aggGroup" );
			_jda->beginJoinRead( grouplen );
			while ( _jda->joinReadNext( oneSetData ) ) {
				if ( oneSetData.length() > 0 ) {
					jda2->writeit( "aggGroup", oneSetData[0].c_str()+grouplen, finalsendlen );
					oneSetData.clean();
				}
			}
			_jda->endJoinRead();
			jda2->flushwrite();
			if ( _jda ) delete _jda;
			_jda = jda2;
		}
	}

	// processDataSortArray( parseParam, _dataSelectCount, _dataFileHeader, _aggregateData ); append later
	// prt(("c4029 parseParam.hasOrder=%d hasExport=%d\n", parseParam.hasOrder, parseParam.hasExport ));
	// if ( !parseParam.hasLimit && _fromShell && !parseParam.hasExport ) 
	if ( !parseParam.hasLimit ) {
		_orderByLimitStart = 0;
		if ( _fromShell ) {
			_orderByLimit = jagatoll((_cfg->getValue("DEFAULT_LIMIT_SIZE", "10000")).c_str());
		} else {
			_orderByLimit = _jda->elements( _jda->getdatalen() );
		}
	} else {
		_orderByLimitStart = parseParam.limitStart;
		_orderByLimit = parseParam.limit;			
	}

	if ( ! _isToGate ) {
		if ( 1 == nqHasLimit ) {
			_orderByLimitStart = 0;
		}
	}
	
	_orderByLimitCnt = 0;
	_orderByIsAsc = parseParam.orderVec[0].isAsc;
	
	// process order by, if _jda has multiple lines of data
	if ( parseParam.hasOrder && _jda->elements( _jda->getdatalen() ) > 1 ) {
		rc = processOrderByQuery( parseParam );
		if ( rc ) rc = 2;
	} else {
		rc = 1;
	}

	if ( rc > 0 && parseParam.opcode == JAG_SELECT_OP && ( parseParam.exportType == JAG_EXPORT_SQL 
		                                                   || parseParam.exportType == JAG_EXPORT_CSV ) ) {
		_forExport = parseParam.exportType;
		_exportObj = hdbobj.c_str();
	}

	// if ( _debug ) { prt(("c5029 rc=%d nearend of processMultiServerCommands\n", rc )); }
	if ( rc ) {
		if ( 1 == rc ) return JAG_END_GOT_DBPAIR;
		return JAG_END_GOT_DBPAIR_AND_ORDERBY;
	} else {
		errmsg = "E3502 Error command [";
		errmsg += parseParam.origCmd + "]";
		return 0;
	}
}   // end processMultiServerCommands()


// method to reformat query which to be sent to server(s), only for querys begin with word select
// return 0: error
// return 1: OK
int JaguarCPPClient::formatSendQuery( JagParseParam &parseParam, JagParseParam &parseParam2, const JagHashStrInt *maps[], 
									const JagSchemaAttribute *attrs[], JagVector<int> &selectPartsOpcode, 
									JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
									int &nqHasLimit, bool &hasAggregate,
									abaxint &limitNum, int &grouplen,
									int numCols[], int num, Jstr &newquery, Jstr &errmsg )
{
	// prt(("c3093 formatSendQuery ...\n"));
	bool isAggregate;
 	hasAggregate = 0;
	Jstr type;
	const char *ppos, *qpos;
	int rc, collen, siglen, constMode = 0, typeMode = 0, nodenum = 0;
	abaxint offset = 0, offset2 = 0;
	ExprElementNode *root;
	Jstr columnlist, aggQuery;
	JagVector<Jstr> selectParts;
	grouplen = 0;

	// if has group by, need to check if each column in group by is alias of db column; if true, replace the original query
	// to the dbcolumn one
	if ( parseParam.hasGroup && parseParam.hasColumn ) {
		checkAndReplaceGroupByAlias( &parseParam );
	}
	
	// if insert into ... select ... syntax, need to check if each column from select part has the same name and/or type with insert into columns;
	// if no insert into columns are provided, all columns from select table must have the same type with insert into table
	if ( JAG_INSERTSELECT_OP == parseParam.opcode && !checkInsertSelectColumnValidation( parseParam ) ) {
		errmsg = "E4021 Error insert into select [";
		errmsg += parseParam.origCmd + "]";
		return 0;
	}
	
	// if has select column, check the validation of those columns, also format possible aggregate strings
	if ( parseParam.hasColumn ) {
		for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
			isAggregate = false;
			root = parseParam.selColVec[i].tree->getRoot();
			rc = root->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, type, collen, siglen );
			if ( 0 == rc ) {
				errmsg = "E3481 Error column function for calculation [";
				errmsg += parseParam.origCmd + "]";
				return 0;
			}
			parseParam.selColVec[i].offset = offset;
			parseParam.selColVec[i].length = collen;
			parseParam.selColVec[i].sig = siglen;
			parseParam.selColVec[i].type = type;
			parseParam.selColVec[i].isAggregate = isAggregate;
			if ( isAggregate ) hasAggregate = 1;
			offset += collen;
		}
		
		if ( parseParam.hasGroup ) { // if has group by, add group by cols to the front of this list
			for ( int i = 0; i < parseParam.groupVec.size(); ++i ) {
				selectParts.append( parseParam.groupVec[i].name );
				selectPartsOpcode.append( 0 );
				selColSetAggParts.append( -1 );
				selColToselParts.setValue( selectParts.length()-1, -1, true );
			}
		}
		
		// process aggregate funcs in column part
		if ( hasAggregate ) {
			if ( JAG_INSERTSELECT_OP == parseParam.opcode ) {
				errmsg = "E4308 Error insert into select [";
				errmsg += parseParam.origCmd + "]";
				return 0;
			}
			int endcount = 0, spopcode = 0;
			//prt(("c9023 selectParts.append( count(1) ) \n" ));
			selectParts.append( "count(1)" );
			selectPartsOpcode.append( JAG_FUNC_COUNT );
			selColSetAggParts.append( -1 );
			selColToselParts.setValue( selectParts.length()-1, -1, true );
			for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
				if ( parseParam.selColVec[i].isAggregate ) {
					nodenum = 0;
					root = parseParam.selColVec[i].tree->getRoot();
					rc = root->getFuncAggregate( selectParts, selectPartsOpcode, selColSetAggParts, selColToselParts, nodenum, i );
				} else {
					selectParts.append( parseParam.selColVec[i].origFuncStr );
					selectPartsOpcode.append( 0 );
					selColSetAggParts.append( -1 );
					selColToselParts.setValue( selectParts.length()-1, i, true );
				}
			}
		} else {
			for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
				// non aggregate funcs, need to add asName to the end of original select part
				if ( parseParam.selColVec[i].givenAsName ) {
					selectParts.append( parseParam.selColVec[i].origFuncStr + " as " + parseParam.selColVec[i].asName );
				} else {
					// no given separate as name
					selectParts.append( parseParam.selColVec[i].origFuncStr );
				}
				selectPartsOpcode.append( 0 );
				selColSetAggParts.append( -1 );
				selColToselParts.setValue( selectParts.length()-1, i, true );
			}
		}
	}
	
	// format columnlist for new querys
	if ( parseParam.opcode == JAG_COUNT_OP ) {
		columnlist = "count(*)";
	} else {
		if ( parseParam.hasColumn ) {
			columnlist = selectParts[0];
			for ( int i = 1; i < selectParts.size(); ++i ) {
				columnlist += Jstr(", ") + selectParts[i];
			}
		} else {
			if ( parseParam.hasGroup ) {
				columnlist = parseParam.groupVec[0].name;
				for ( int i = 1; i < parseParam.groupVec.size(); ++i ) {
					columnlist += Jstr(", ") + parseParam.groupVec[i].name;
				}
				if ( num == 1 ) {
					for ( int i = 0; i < num; ++i ) {
						for ( int j = 0; j < numCols[i]; ++j ) {
							columnlist += Jstr(", ") + attrs[i][j].colname;
						}
					}
				} else {
					for ( int i = 0; i < num; ++i ) {
						for ( int j = 0; j < numCols[i]; ++j ) {
							columnlist += Jstr(", ") + attrs[i][j].dbcol;
						}
					}
				}
			} else {
				columnlist = "*";
			}
		}
	}
	
	Jstr checkquery;
	formatInsertSelectCmdHeader( &parseParam, checkquery );
	checkquery += "select " + columnlist + " from " + parseParam.selectTablistClause + " ";
	if ( parseParam.hasGroup ) {
		checkquery += Jstr(" group by ") + parseParam.selectGroupClause + " ";
	}

	//prt(("c1028 parseParam.hasOrder=%d selectOrderClause=[%s]\n", parseParam.hasOrder, parseParam.selectOrderClause.c_str() ));
	if ( parseParam.hasOrder && parseParam.selectOrderClause.size() > 0 ) {
		checkquery += Jstr(" order by ") + parseParam.selectOrderClause + " ";
	}

	checkquery += " ;";
	Jstr errmsg2;
	JagParseAttribute jpa;
	jpa.dfdbname = _dbname;
	jpa.timediff = _tdiff;
	JagParser parser(NULL, this);
	rc = parser.parseCommand( jpa, checkquery.c_str(), &parseParam2, errmsg2 );
	if ( !rc ) {
		errmsg = "E5230 Error select column [";
		errmsg += parseParam2.origCmd + "] " + errmsg2;
		return 0;
	}
		
	// if hasGroupBy, check the validation of select column parts 
	// and check the validation of those group by columns
	if ( parseParam.hasGroup ) {
		for ( int i = 0; i < parseParam2.selColVec.size(); ++i ) {
			isAggregate = false;
			root = parseParam2.selColVec[i].tree->getRoot();
			rc = root->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, type, collen, siglen );
			if ( 0 == rc || ( isAggregate && i < parseParam2.groupVec.size() ) ) {
				errmsg = "E4031 Error group function for calculation [";
				errmsg += parseParam2.origCmd + "]";
				return 0;
			} else if ( rc > 0 ) {
				parseParam2.selColVec[i].offset = offset2;
				parseParam2.selColVec[i].length = collen;
				parseParam2.selColVec[i].sig = siglen;
				parseParam2.selColVec[i].type = type;
				offset2 += collen;
			}
		}

		// if has group by, need to check if each column in group by is alias of db column; if true, replace the original query
		// to the dbcolumn one
		if ( parseParam.hasGroup && parseParam.hasColumn ) {
			rc = checkAndReplaceGroupByAlias( &parseParam2 );
			if ( 0 == rc ) {
				errmsg = "E3821 Error group by alias name [";
				errmsg += parseParam2.origCmd + "]";
				return 0;
			}
		}

		grouplen = checkGroupByValidation( &parseParam2 );
		if ( 0 == grouplen ) {
			errmsg = "E4003 Error group by [";
			errmsg += parseParam2.origCmd + "]";
			return 0;
		}
	}
	
	// if hasOrderBy, check the validation of those order by columns, original query check
	int orc = -1;
	if ( parseParam.hasOrder && ( orc = checkOrderByValidation( parseParam, attrs, numCols, num ) ) == 0 ) {
		errmsg = "E4302 Error order by [";
		errmsg += parseParam.origCmd + "]";
		return 0;
	}
	
	// after checking all valid conditions, form the final query which needs to be sent to server(s)
	abaxint limitsize = jagatoll((_cfg->getValue("DEFAULT_LIMIT_SIZE", "10000")).c_str());
	formatInsertSelectCmdHeader( &parseParam, newquery );
	newquery += Jstr("select ") + columnlist + " from " + parseParam.selectTablistClause;
	if ( parseParam.hasWhere ) {
		newquery += Jstr(" where ") + parseParam.selectWhereClause + " ";
	}
	if ( parseParam.hasGroup ) {
		newquery += Jstr(" group by ") + parseParam.selectGroupClause + " ";
	}
	if ( 2 == orc && parseParam.selectOrderClause.size() > 0 ) {
		// if order by first key asc or desc, add order by part to server
		newquery += Jstr(" order by ") + parseParam.selectOrderClause + " ";
	}
	// + parseParam.selectHavingClause + " " later
	// also add limit condition if needed, need to change later
	// if has order by, or has aggregate functions, or count(*) no limit
	// if not has Aggregate, and not has order by, and not count(*) with shell or haslimit, use limit
	//
	abaxint dfTimeout = jagatoll((_cfg->getValue("SELECT_TIMEOUT", "60")).c_str());
	bool check1, check2, check3;
	check1 = !hasAggregate && (JAG_COUNT_OP != parseParam.opcode);
	check2 = _fromShell || parseParam.hasLimit;
	check3 = !parseParam.hasOrder || orc == 2;
	if ( check1 && check2 && check3 ) {
		nqHasLimit = 1;
		if ( parseParam.hasLimit ) {
			limitNum = parseParam.limit;
		} else if ( _fromShell ) {
			limitNum = limitsize;
		}
		
		if ( !parseParam.hasExport || parseParam.hasLimit ) {
			newquery += Jstr (" limit ") + longToStr( parseParam.limitStart+limitNum );
			if ( JAG_INSERTSELECT_OP != parseParam.opcode ) {
				if ( ! parseParam.hasTimeout ) {
					newquery += Jstr (" timeout ") + longToStr( dfTimeout );
				} else {
					newquery += Jstr (" timeout ") + longToStr( parseParam.timeout );
				}
			}
			//newquery += ";";
		} else {
			nqHasLimit = 0;
			limitNum = 0;

			if ( parseParam.exportType == JAG_EXPORT ) {
				newquery += " export;";
			} else {
				if ( JAG_INSERTSELECT_OP != parseParam.opcode ) {
					if ( ! parseParam.hasTimeout ) {
						newquery += Jstr (" timeout ") + longToStr( dfTimeout );
					} else {
						newquery += Jstr (" timeout ") + longToStr( parseParam.timeout );
					}
				}
				//newquery += " ;";
			}
		}
		if ( parseParam.hasGroup ) {
			// client has no limit; server uses limit
			nqHasLimit = 0;
		}
		if ( 1 == nqHasLimit && 2 == orc ) {
			nqHasLimit = 2;
		}
	} else {
		nqHasLimit = 0;
		limitNum = 0;
		if ( parseParam.exportType == JAG_EXPORT ) {
			newquery += " export;";
		} else {
			if ( JAG_INSERTSELECT_OP != parseParam.opcode ) {
				if ( ! parseParam.hasTimeout ) {
					newquery += Jstr (" timeout ") + longToStr( dfTimeout );
				} else {
					newquery += Jstr (" timeout ") + longToStr( parseParam.timeout );
				}
			}
			//newquery += ";";
		}
	}
	return 1;	
}

int JaguarCPPClient::checkOrderByValidation( JagParseParam &parseParam, const JagSchemaAttribute *attr[], int numCols[], int num )
{
	int rc = 1;

	// check if order list is sublist of select part
	JagHashMap<AbaxString, Jstr> colchecklist;
	if ( !parseParam.hasColumn ) {
		if ( 1 == num ) {
			for ( int i = 0; i < numCols[0]; ++i ) {
				colchecklist.addKeyValue( AbaxString(attr[0][i].colname), attr[0][i].colname );
			}
		} else if ( num > 1 ) {
			for ( int i = 0; i < num; ++i ) {
				for ( int j = 0; j < numCols[i]; ++j ) {
					// db.tab.col -> db.tab.col
					colchecklist.addKeyValue( AbaxString(attr[i][j].dbcol), attr[i][j].dbcol );
					// tab.col -> db.tab.col
					colchecklist.addKeyValue( AbaxString(attr[i][j].objcol), attr[i][j].dbcol );
				}
			}
		}
	} else {
		for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
			colchecklist.addKeyValue( AbaxString(parseParam.selColVec[i].asName), parseParam.selColVec[i].asName );
		}
	}
	// check if subset list
	for ( int i = 0; i < parseParam.orderVec.size(); ++i ) {
		if ( ! colchecklist.keyExist( AbaxString(parseParam.orderVec[i].name) ) ) {
			rc = 0;
			break;
		} else {
			if ( !parseParam.hasColumn && num > 1 ) {
				Jstr fname;
				if ( !colchecklist.getValue( AbaxString(parseParam.orderVec[i].name), fname ) ) {
					rc = 0;
				} else {
					parseParam.orderVec[i].name = fname;
				}
			}
		}
	}	

	if ( rc > 0 ) {
		// special: if first item of order by vector is the first key of table, rc=2
		if ( 1 == num && parseParam.orderVec[0].name == attr[0][0].colname ) rc = 2;
	}
	return rc;
}

// must be called when _mapLock is already locked
// objectVec[0]: insert into TABLE; objectVec[1]: select from TABLE/INDEX;
// return 0: error;
// return 1: OK;
int JaguarCPPClient::checkInsertSelectColumnValidation( const JagParseParam &parseParam )
{
	Jstr dbobj1, dbobj2;
	const JagTableOrIndexAttrs *oattr1 = NULL, *oattr2 = NULL;
	if ( parseParam.objectVec.size() < 2 ) {
		return 0;
	}
	
	// get oattr1 and oattr2, dbobj1 can only be table; dbobj2 can be either table/index
	dbobj1 = parseParam.objectVec[0].dbName + "." + parseParam.objectVec[0].tableName;
	// if ( !_schemaMap->getValue( dbobj1, oattr1 ) ) 
	oattr1 = _schemaMap->getValue( dbobj1 );
	if ( ! oattr1 ) {
		if ( parseParam.objectVec[0].colName.length() > 0 ) {
			// try dfdbname + tableName 
			dbobj1 = parseParam.objectVec[0].colName + "." + parseParam.objectVec[0].tableName;
			// if ( !_schemaMap->getValue( dbobj1, oattr1 ) ) {
			oattr1 = _schemaMap->getValue( dbobj1 );
			if ( ! oattr1 ) {
				return 0;
			}
		} else {
			return 0;
		}
	}

	if ( parseParam.objectVec[1].indexName.length() > 0 ) {
		dbobj2 = parseParam.objectVec[1].dbName + "." + parseParam.objectVec[1].indexName;
		// if ( !_schemaMap->getValue( dbobj2, oattr2 ) ) {
		oattr2 = _schemaMap->getValue( dbobj2 );
		if ( ! oattr2 ) {
			return 0;
		}
	} else {
		dbobj2 = parseParam.objectVec[1].dbName + "." + parseParam.objectVec[1].tableName;
		// if ( !_schemaMap->getValue( dbobj2, oattr2 ) ) 
		oattr2 = _schemaMap->getValue( dbobj2 );
		if ( ! oattr2 ) {
			if ( parseParam.objectVec[1].colName.length() > 0 ) {
				// try dfdbname + tableName 					
				dbobj2 = parseParam.objectVec[1].colName + "." + parseParam.objectVec[1].tableName;
				oattr2 = _schemaMap->getValue( dbobj2 );
				if ( ! oattr2 ) {
					return 0;
				}
			} else {
				return 0;
			}
		}
	}
	
	if ( parseParam.objectVec[0].indexName.size() > 0 || oattr1->indexName.size() > 0 ) {
		// insert into index, invalid
		return 0;
	}
	
	int check = 0, rc1, rc2; 
	int pos1, pos2;
	rc1 = parseParam.otherVec.size(); // num of insert into cols, 0 for all
	rc2 = parseParam.selColVec.size(); // num of select from cols, 0 for select *
	
	if ( !rc1 && !rc2 && oattr1->numCols == oattr2->numCols ) {
		check = 1;
	} else if ( rc1 && rc2 && rc1 == rc2 ) {
		check = 2;
	}
	
	if ( !check ) {
		return 0;
	}
	
	// at last, check column type as well as length to make sure their column's attributes are the same
	if ( 1 == check ) {
		// check all columns of table
		for ( int i = 0; i < oattr1->numCols; ++i ) {
			if ( oattr1->schAttr[i].type == JAG_C_COL_TYPE_STR && oattr2->schAttr[i].type == JAG_C_COL_TYPE_STR ) {
				continue;
			}

			if ( oattr1->schAttr[i].type != oattr2->schAttr[i].type || 
				 oattr1->schAttr[i].length < oattr2->schAttr[i].length ) {
				 // oattr1.schAttr[i].sig != oattr2.schAttr[i].sig ) {
				return 0;
			}
		}
	} else if ( 2 == check ) {
		// check only given columns
		Jstr dbcol1, dbcol2;
		for ( int i = 0; i < parseParam.otherVec.size(); ++i ) {
			dbcol1 = dbobj1 + "." + parseParam.otherVec[i].objName.colName;
			pos2 = strchrnum(parseParam.selColVec[i].asName.c_str(), '.');
			if ( 0 == pos2 ) {
				// only colName, add db+tab
				dbcol2 = dbobj2 + "." + parseParam.selColVec[i].asName;
			} else if ( 1 == pos2 ) {
				// tab+col, add db
				dbcol2 = parseParam.objectVec[1].dbName + "." + parseParam.selColVec[i].asName;
			} else {
				dbcol2 = parseParam.selColVec[i].asName;
			}

			if ( !oattr1->schmap.getValue(dbcol1, pos1) || !oattr2->schmap.getValue(dbcol2, pos2) ) {
				// db.tab.col is either not among the schema of table1 or table2, error
				return 0;
			}

			if ( oattr1->schAttr[i].type == JAG_C_COL_TYPE_STR && oattr2->schAttr[i].type == JAG_C_COL_TYPE_STR ) {
				continue;
			}

			if ( oattr1->schAttr[pos1].type != oattr2->schAttr[pos2].type || 
				 oattr1->schAttr[pos1].length < oattr2->schAttr[pos2].length ) {
				 // oattr1.schAttr[pos1].sig != oattr2.schAttr[pos2].sig ) {
				return 0;
			}			
		}
	}
	return 1;
}

// calculate each separate aggregate function value between servers
int JaguarCPPClient::doAggregateCalculation( const JagSchemaRecord &aggrec, const JagVector<int> &selectPartsOpcode,
											const JagVector<JagFixString> &oneSetData, char *aggbuf, int datalen, int countBegin ) 
{
	char save;
	Jstr type;
	abaxdouble ld, ld1, ld2, ld3, ld4, ld5;
	abaxint ns, ns1, ns2, offset, length, rc;

	char *getbuf1 = (char*)jagmalloc(datalen+1);
	char *getbuf2 = (char*)jagmalloc(datalen+1);
	memset( getbuf1, 0, datalen+1 );
	memset( getbuf2, 0, datalen+1 );
	memcpy( aggbuf, oneSetData[0].c_str(), datalen );

	for ( int j = 1; j < oneSetData.size(); ++j ) {
		memcpy( getbuf1, aggbuf, datalen );
		memcpy( getbuf2, oneSetData[j].c_str(), datalen );
		for ( int i = 0; i < aggrec.columnVector->size(); ++i ) {
			type = (*aggrec.columnVector)[i].type;
			offset = (*aggrec.columnVector)[i].offset;
			length = (*aggrec.columnVector)[i].length;
			prt(("s2737 i=%d type=[%s] offset=%d length=%d\n", i, type.c_str(), offset, length ));

			if ( selectPartsOpcode[i] == JAG_FUNC_COUNT ) {
				// ns1 = n1||s1, ns2 = n2||s2;
				ns1 = rayatol( getbuf1+offset, length );
				ns2 = rayatol( getbuf2+offset, length );
				ns = ns1 + ns2;
				ld = (abaxdouble)ns;			
				memset( aggbuf+offset, 0, length );
				save = *(aggbuf+offset+length);
				jagsprintfLongDouble( 1, 1, aggbuf+offset, ld, length );
				*(aggbuf+offset+length) = save;				
			} else if ( selectPartsOpcode[i] == JAG_FUNC_SUM ) {
				ld1 = raystrtold( getbuf1+offset, length );
				ld2 = raystrtold( getbuf2+offset, length );
				ld = ld1 + ld2;
				memset( aggbuf+offset, 0, length );
				save = *(aggbuf+offset+length);
				jagsprintfLongDouble( 1, 1, aggbuf+offset, ld, length );
				*(aggbuf+offset+length) = save;
			} else if ( selectPartsOpcode[i] == JAG_FUNC_MIN ) {
				rc = checkColumnTypeMode( type );
				if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, abaxint
					ns1 = rayatol( getbuf1+offset, length );
					ns2 = rayatol( getbuf2+offset, length );
					if ( ns1 > ns2 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				} else if ( rc == 5 ) { // float, double
					ld1 = rayatol( getbuf1+offset, length );
					ld2 = rayatol( getbuf2+offset, length );
					if ( ld1 > ld2 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				} else { // other types, string
					if ( memcmp( getbuf1+offset, getbuf2+offset, length ) > 0 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				}
			} else if ( selectPartsOpcode[i] == JAG_FUNC_MAX ) {
				rc = checkColumnTypeMode( type );
				if ( rc == 2 || rc == 3 || rc == 4 ) { // bool, int, abaxint
					ns1 = rayatol( getbuf1+offset, length );
					ns2 = rayatol( getbuf2+offset, length );
					if ( ns1 < ns2 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				} else if ( rc == 5 ) { // float, double
					ld1 = rayatol( getbuf1+offset, length );
					ld2 = rayatol( getbuf2+offset, length );
					if ( ld1 < ld2 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				} else { // other types, string
					if ( memcmp( getbuf1+offset, getbuf2+offset, length ) < 0 ) {
						memcpy( aggbuf+offset, getbuf2+offset, length );
					}
				}
			} else if ( selectPartsOpcode[i] == JAG_FUNC_FIRST ) {
				memcpy( aggbuf+offset, getbuf1+offset, length );
			} else if ( selectPartsOpcode[i] == JAG_FUNC_LAST ) {
				memcpy( aggbuf+offset, getbuf2+offset, length );
			} else if ( selectPartsOpcode[i] == JAG_FUNC_AVG ) {
				// ld1 = avg(1), ld2 = avg(2);
				// ns1 = n1, ns2 = n2;
				ld1 = raystrtold( getbuf1+offset, length );
				ld2 = raystrtold( getbuf2+offset, length );
				ns1 = rayatol( getbuf1+(*aggrec.columnVector)[countBegin].offset, (*aggrec.columnVector)[countBegin].length );
				ns2 = rayatol( getbuf2+(*aggrec.columnVector)[countBegin].offset, (*aggrec.columnVector)[countBegin].length );
				ld = ( ld1*ns1 + ld2*ns2 ) / ( ns1 + ns2 );
				memset( aggbuf+offset, 0, length );
				save = *(aggbuf+offset+length);
				jagsprintfLongDouble( 1, 1, aggbuf+offset, ld, length );
				*(aggbuf+offset+length) = save;
			} else if ( selectPartsOpcode[i] == JAG_FUNC_STDDEV ) {
				// ld1 = sd1, ld2 = sd2;
				// ld3 = avg(1), ld4 = avg(2), ld5 = avg(1, 2);
				// ns1 = n1, ns2 = n2, ns = n1+n2;
				ld1 = raystrtold( getbuf1+offset, length );
				ld2 = raystrtold( getbuf2+offset, length );
				ns1 = rayatol( getbuf1+(*aggrec.columnVector)[countBegin].offset, (*aggrec.columnVector)[countBegin].length );
				ns2 = rayatol( getbuf2+(*aggrec.columnVector)[countBegin].offset, (*aggrec.columnVector)[countBegin].length );
				ns = rayatol( aggbuf+(*aggrec.columnVector)[countBegin].offset, (*aggrec.columnVector)[countBegin].length );
				ld3 = raystrtold( getbuf1+(*aggrec.columnVector)[i-1].offset, (*aggrec.columnVector)[i-1].length );
				ld4 = raystrtold( getbuf2+(*aggrec.columnVector)[i-1].offset, (*aggrec.columnVector)[i-1].length );
				ld5 = raystrtold( aggbuf+(*aggrec.columnVector)[i-1].offset, (*aggrec.columnVector)[i-1].length );
				ld = ns1*(ld1*ld1) + ns2*(ld2*ld2) + ns1*((ld3-ld5)*(ld3-ld5)) + ns2*((ld4-ld5)*(ld4-ld5));
				ld = sqrt( ld / ns );
				memset( aggbuf+offset, 0, length );
				save = *(aggbuf+offset+length);
				jagsprintfLongDouble( 1, 1, aggbuf+offset, ld, length );
				*(aggbuf+offset+length) = save;
			} else {
				memcpy( aggbuf+offset, getbuf2+offset, length );
			}
		}
	}
	if ( getbuf1 ) free ( getbuf1 );
	if ( getbuf2 ) free ( getbuf2 );
	return 1;
}

// calculate separate aggregate functions to original combined function
int JaguarCPPClient::reformOriginalAggQuery( const JagSchemaRecord &aggrec, const JagVector<int> &selectPartsOpcode,  
	const JagVector<int> &selColSetAggParts, const JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
	const char *aggbuf, char *finalbuf, JagParseParam &parseParam ) 
{
	// put aggregated values back to original query
	char save;
	Jstr treetype = " ";
	int typeMode = 0, treelength = 0;
	abaxint offset, length, sig;
	AbaxInt vpos;
	ExprElementNode *root;
	Jstr errmsg;
	for ( int i = 0; i < aggrec.columnVector->size(); ++i ) {
		offset = (*aggrec.columnVector)[i].offset;
		length = (*aggrec.columnVector)[i].length;
		selColToselParts.getValue( AbaxInt(i), vpos );
		if ( vpos.value() >= 0 ) {
			if ( selectPartsOpcode[i] > 0 ) {
				root = parseParam.selColVec[vpos.value()].tree->getRoot();
				root->setAggregateValue( selColSetAggParts[i], aggbuf+offset, length );
			} else {
				if ( 1 ) {
					memcpy(finalbuf+parseParam.selColVec[vpos.value()].offset, aggbuf+offset, length);
				} else {
					save = *(finalbuf+parseParam.selColVec[vpos.value()].offset+length);
					jagsprintfLongDouble( 1, 0, finalbuf+parseParam.selColVec[vpos.value()].offset, raystrtold(aggbuf+offset+30, length-30), length );
					*(finalbuf+parseParam.selColVec[vpos.value()].offset+length) = save;
				}

			}
		}
	}

	for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
		if ( parseParam.selColVec[i].isAggregate ) {
			root = parseParam.selColVec[i].tree->getRoot();
			root->checkFuncValidConstantOnly( parseParam.selColVec[i].strResult, typeMode, treetype, treelength );
		}
	}

	for ( int i = 0; i < parseParam.selColVec.size(); ++i ) {
		if ( parseParam.selColVec[i].isAggregate ) {
			offset = parseParam.selColVec[i].offset;
			length = parseParam.selColVec[i].length;
			sig = parseParam.selColVec[i].sig;
			treetype = parseParam.selColVec[i].type;
			formatOneCol( _tdiff, _tdiff, finalbuf, parseParam.selColVec[i].strResult.c_str(), errmsg, 
				"GARBAGE", offset, length, sig, treetype );
		}
	}
	return 1;
}

// method to process data sort array
// pay attention to return values
// return 0: no order by, direct read from jda
int JaguarCPPClient::processDataSortArray( const JagParseParam &parseParam, 
	const Jstr &selcnt, const Jstr &selhdr, const JagFixString &aggstr ) 
{
	abaxint skeylen = 0, svallen = 0, lstart = 0, limit = 0, isdesc = 0, elem = 0, jlen = 0;
	if ( !parseParam.hasLimit ) {
		lstart = 0;
		if ( _fromShell ) limit = jagatoll((_cfg->getValue("DEFAULT_LIMIT_SIZE", "10000")).c_str());
		else limit = _jda->elements( _jda->getdatalen() );
	} else {
		lstart = parseParam.limitStart-1;
		limit = parseParam.limit;
	}
	isdesc = !parseParam.orderVec[0].isAsc;
	jlen = _jda->getdatalen();
	elem = _jda->elements( jlen );
	
	// if no order by, or one has one line of data, or order by non hash object keys, use jda
	if ( !parseParam.hasOrder || elem <= 1 ) {
		_dataSortArr->init();
		_dataSortArr->setJDA( _jda, isdesc );
		skeylen = 0;
		svallen = jlen;
	} else { // process order by sort, if _jda has multiple lines of data
		// get sort lengths
		JagSchemaRecord rec;
		JagFixString str;
		int num = parseParam.orderVec.size();
		abaxint offsets[num], lengths[num];
		abaxint pos = 0, offset = 0;						
		
		rec.parseRecord( selhdr.c_str() );
		JagHashMap<AbaxString, abaxint> slist;
		for ( int i = 0; i < rec.columnVector->size(); ++i ) {
			slist.addKeyValue( (*rec.columnVector)[i].name, i );
		}

		for ( int i = 0; i < num; ++i ) {
			if ( slist.getValue( AbaxString(parseParam.orderVec[i].name), pos ) ) {
				offsets[i] = (*rec.columnVector)[pos].offset;
				lengths[i] = (*rec.columnVector)[pos].length;
				skeylen += lengths[i];
			}
		}
		skeylen += JAG_UUID_FIELD_LEN;
		svallen = jlen;
	
		// format client diskarray hdr
		char hbuf[JAG_SCHEMA_SPARE_LEN+1];
		hbuf[JAG_SCHEMA_SPARE_LEN] = '\0';
		memset(hbuf, ' ', JAG_SCHEMA_SPARE_LEN );					
		Jstr hstr = Jstr("NN|") + longToStr(skeylen) + "|" + longToStr(svallen) + "|0|{";
		hbuf[0] = JAG_C_COL_KEY;
		hbuf[2] = JAG_RAND;
		for ( int i = 0; i < parseParam.orderVec.size(); ++i ) {
			hstr += Jstr("!") + "key_" + parseParam.orderVec[i].name + "!s!" + longToStr(offset) + "!" + 
				longToStr(lengths[i]) + "!0!" + hbuf + "|";
			offset += lengths[i];
		}
		hbuf[1] = JAG_C_COL_TYPE_UUID_CHAR;
		hstr += Jstr("!") + "key_" + longToStr( THREADID ) + "_uuid" + "!s!" + longToStr(offset) + "!" + 
			longToStr(JAG_UUID_FIELD_LEN) + "!0!" + hbuf + "|";
		offset += JAG_UUID_FIELD_LEN;
		memset( hbuf, ' ', JAG_SCHEMA_SPARE_LEN );
		hbuf[0] = JAG_C_COL_VALUE;
		hbuf[2] = JAG_RAND;
		for ( int i = 0; i < rec.columnVector->size(); ++i ) {
			hstr += Jstr("!") + (*(rec.columnVector))[i].name.c_str() + "!s!" + longToStr(offset) + "!" + 
				longToStr((*(rec.columnVector))[i].length) + "!0!" + hbuf + "|";
			offset += (*(rec.columnVector))[i].length;
		}
		hstr += "}";
		
		// sort data in memory / disk array
		_dataSortArr->init( 300, hstr.c_str(), "GroupByValue" );
		_dataSortArr->beginWrite();
		char *buf = (char*)jagmalloc(skeylen+svallen+1);
		memset(buf, 0, skeylen+svallen+1);
		while( _jda->readit(str) ) {
			offset = 0;
			for ( int i = 0; i < num; ++i ) {
				memcpy(buf+offset, str.c_str()+offsets[i], lengths[i]);
				offset += lengths[i];
			}
			memcpy(buf+offset, _jagUUID->getString().c_str(), JAG_UUID_FIELD_LEN);
			offset += JAG_UUID_FIELD_LEN;
			memcpy(buf+offset, str.c_str(), str.length());
			JagDBPair pair(buf, skeylen, buf+skeylen, svallen, true);
			_dataSortArr->insert( pair );
		}
		if ( buf ) free ( buf );
		_dataSortArr->endWrite();
		_dataSortArr->beginRead( isdesc );
	}
	
	// set possible select count(*), FH, or aggregate data string
	_dataSortArr->setClientUseStr( selcnt, selhdr, aggstr );
	// begin process read range for data
	_dataSortArr->ignoreNumData( lstart );
	_dataSortArr->setDataLimit( limit );
	return 1;
}

// may be removed later
int JaguarCPPClient::processOrderByQuery( const JagParseParam &parseParam ) 
{	
	// process order by and possible limit condition
	_debug && prt(("c1038 processOrderByQuery ...\n" ));
	JagFixString sortstr;
	Jstr tmpuuid;
	JagSchemaRecord orderrec(true);
	int ordernum = parseParam.orderVec.size();
	abaxint orderoffset[ordernum], orderlength[ordernum];
	abaxint orderpos, sortkeylen = 0, sortvallen = 0, sortkeyvallen = 0;						
	abaxint writeoff = 0, totbytes = _jda->elements( 1 );
	bool isMem = true;
	abaxint orderByMemLimit = jagatoll((_cfg->getValue("ORDERBY_MEM_LIMIT", "100")).c_str());
	abaxint orderByLimit = jagatoll((_cfg->getValue("ORDERBY_LIMIT", "100000")).c_str());
	
	if ( totbytes > orderByMemLimit*1024*1024 ) { // order by sorted in memory array
		isMem = false;
	}	
	_debug && prt(("c3831 isMem=%d\n", isMem ));
	
	int rc = orderrec.parseRecord( _dataFileHeader.c_str() );
	JagHashMap<AbaxString, abaxint> sentDataList;
	for ( int i = 0; i < orderrec.columnVector->size(); ++i ) {
		sentDataList.addKeyValue( (*orderrec.columnVector)[i].name, i );
	}

	for ( int i = 0; i < ordernum; ++i ) {
		if ( sentDataList.getValue( AbaxString(parseParam.orderVec[i].name), orderpos ) ) {
			orderoffset[i] = (*orderrec.columnVector)[orderpos].offset;
			orderlength[i] = (*orderrec.columnVector)[orderpos].length;
			sortkeylen += orderlength[i];
		} else {
			return 0;
		}
	}
	sortkeylen += JAG_UUID_FIELD_LEN;
	sortvallen = _jda->getdatalen();					
	char *sortbuf = (char*)jagmalloc(sortkeylen+sortvallen+1);
	memset(sortbuf, 0, sortkeylen+sortvallen+1);
	
	_orderByKEYLEN = sortkeylen;
	_orderByVALLEN = sortvallen;
	int entries = 0;
	if ( isMem ) {
		_orderByReadFrom = JAG_ORDERBY_READFROM_MEMARR;
		if ( _orderByMemArr ) {
			delete _orderByMemArr;
		}
		_orderByMemArr = new PairArray();
		_debug && prt(("c3371 isMem readit ...\n" ));
		while( _jda->readit(sortstr) ) {
			writeoff = 0;
			for ( int i = 0; i < ordernum; ++i ) {
				memcpy(sortbuf+writeoff, sortstr.c_str()+orderoffset[i], orderlength[i]);
				writeoff += orderlength[i];
			}
			tmpuuid = _jagUUID->getString();
			memcpy(sortbuf+writeoff, tmpuuid.c_str(), JAG_UUID_FIELD_LEN);
			writeoff += JAG_UUID_FIELD_LEN;
			memcpy(sortbuf+writeoff, sortstr.c_str(), sortstr.length());
			JagDBPair tpair(sortbuf, sortkeylen, sortbuf+sortkeylen, sortstr.length());
			_orderByMemArr->insert( tpair );
			++ entries;
			if ( entries > orderByLimit ) {
				_debug && prt(("c3372 mem orderByLimit=%d reached, break\n", orderByLimit ));
				break;
			}
		}
		_debug && prt(("c3371 isMem readit done\n" ));
		
		if ( _orderByIsAsc ) {
			_orderByReadPos = -1;
		} else {
			_orderByReadPos = _orderByMemArr->size();
		}
	} else {
		_orderByReadFrom = JAG_ORDERBY_READFROM_DISKARR;
		// format client diskarray schema record and filepath
		int insertCode;
		abaxint cntoffset = 0;
		char cschebuf[JAG_SCHEMA_SPARE_LEN+1];
		cschebuf[JAG_SCHEMA_SPARE_LEN] = '\0';
		memset(cschebuf, ' ', JAG_SCHEMA_SPARE_LEN );
		Jstr clifpath = jaguarHome() + "/tmp/" + longToStr( THREADID ) + "_" + "orderBySortFile";					
		Jstr clischema = Jstr("NN|") + longToStr(sortkeylen) + "|" + longToStr(sortvallen) + "|0|{";
		// format client key schema
		cschebuf[0] = JAG_C_COL_KEY;
		cschebuf[2] = JAG_RAND;
		for ( int i = 0; i < parseParam.orderVec.size(); ++i ) {
			clischema += Jstr("!") + "key_" + parseParam.orderVec[i].name + "!s!" + longToStr(cntoffset) + "!" + 
						longToStr(orderlength[i]) + "!0!" + cschebuf + "|";
			cntoffset += orderlength[i];
		}
		cschebuf[1] = JAG_C_COL_TYPE_UUID_CHAR;
		clischema += Jstr("!") + "key_" + longToStr( THREADID ) + "_uuid" + "!s!" + longToStr(cntoffset) + "!" + 
					longToStr(JAG_UUID_FIELD_LEN) + "!0!" + cschebuf + "|";
		cntoffset += JAG_UUID_FIELD_LEN;
		// format client value schema
		memset(cschebuf, ' ', JAG_SCHEMA_SPARE_LEN );
		cschebuf[0] = JAG_C_COL_VALUE;
		cschebuf[2] = JAG_RAND;
		for ( int i = 0; i < orderrec.columnVector->size(); ++i ) {
			clischema += Jstr("!") + (*(orderrec.columnVector))[i].name.c_str() + "!s!" + longToStr(cntoffset) + "!" + 
						longToStr((*(orderrec.columnVector))[i].length) + "!0!" + cschebuf + "|";
			cntoffset += (*(orderrec.columnVector))[i].length;
		}
		clischema += "}";
		
		if ( _orderByRecord ) {
			delete _orderByRecord;
		}
		_orderByRecord = new JagSchemaRecord(true);
		_orderByRecord->parseRecord( clischema.c_str() );
		
		if ( _orderByDiskArr ) {
			_orderByDiskArr->drop();
			delete _orderByDiskArr;
		}
		// _orderByDiskArr = new JagDiskArrayClient(clifpath, _orderByRecord);
		_orderByDiskArr = new JagDiskArrayClient(clifpath, _orderByRecord);
		JagDBPair retpair;
		
		_debug && prt(("c0018 isdisk start readit ...\n" ));
		entries = 0;
		while( _jda->readit(sortstr) ) {
			writeoff = 0;
			for ( int i = 0; i < ordernum; ++i ) {
				memcpy(sortbuf+writeoff, sortstr.c_str()+orderoffset[i], orderlength[i]);
				writeoff += orderlength[i];
			}
			tmpuuid = _jagUUID->getString();
			memcpy(sortbuf+writeoff, tmpuuid.c_str(), JAG_UUID_FIELD_LEN);
			writeoff += JAG_UUID_FIELD_LEN;
			memcpy(sortbuf+writeoff, sortstr.c_str(), sortstr.length());
			JagDBPair tpair(sortbuf, sortkeylen, sortbuf+sortkeylen, sortstr.length(), true);
			_orderByDiskArr->insertSync( tpair );
			// _orderByDiskArr->insert(tpair, insertCode, true, false, retpair);
			++ entries;
			if ( entries > orderByLimit ) {
				_debug && prt(("c3374 disk orderByLimit=%d reached, break\n", orderByLimit ));
				break;
			}
		}
		_debug && prt(("c0018 isdisk end readit\n" ));
		_orderByDiskArr->flushInsertBufferSync();
		_debug && prt(("c0018 isdisk done flushInsertBufferSync \n" ));
		// _orderByDiskArr->copyInsertBuffer();
		// _orderByDiskArr->flushInsertBuffer2();
		// _orderByDiskArr->cleanInsertBufferCopy();
		
		// clean up possible buffreaders
		if ( _orderByBuffReader ) {
			delete _orderByBuffReader;
			_orderByBuffReader = NULL;
		}
		if ( _orderByBuffBackReader ) {
			delete _orderByBuffBackReader;
			_orderByBuffBackReader = NULL;
		}
		
		if ( _orderByIsAsc ) {
			_orderByBuffReader = new JagBuffReader( (JagDiskArrayBase*)_orderByDiskArr, _orderByDiskArr->_arrlen,
				_orderByDiskArr->KEYLEN, _orderByDiskArr->VALLEN );
		} else {
			_orderByBuffBackReader = new JagBuffBackReader( (JagDiskArrayBase*)_orderByDiskArr, _orderByDiskArr->_arrlen, 
				_orderByDiskArr->KEYLEN, _orderByDiskArr->VALLEN, _orderByDiskArr->_arrlen );
		}
	}
	free( sortbuf );
	return 1;
}

// Detect if client side will enough space
// 1: has enough space   0: not enough space
bool  JaguarCPPClient::hasEnoughDiskSpace( abaxint numServers, abaxint totbytes, int &requiredGB, int &availableGB )
{
	requiredGB = availableGB = 0;
	// prt(("this=%0x hasEnoughDiskSpace _lastOpCode=%d JAG_SELECT_OP=%d\n", this, _lastOpCode, JAG_SELECT_OP ));
	if ( _lastOpCode != JAG_SELECT_OP ) {
		return 1;
	}

	abaxint totneeded = totbytes;
	if ( _lastHasGroupBy ) {
		totneeded += numServers * totbytes; // merge files from all servers
	}

	if ( _lastHasOrderBy ) {
		totneeded += 4 * numServers * totbytes; // order diskarryclient
	}

	abaxint usedGB, freeGB;
	char *fpath = getenv("JAGUAR_HOME" );
	if ( ! fpath ) fpath = getenv("HOME" );
	int rc = JagFileMgr::getPathUsage( (const char *)fpath , usedGB, freeGB );
	if ( ! rc ) return 1;

	abaxint MB = totneeded/1000000;
	abaxint needGB = MB/1000;  // half needed

	// prt(("c7710 numServers=%d totbytes=%d needGB=%d freeGB=%d\n", numServers, totbytes, needGB, freeGB ));
	if ( freeGB < needGB - 30 ) {
		availableGB = freeGB;
		requiredGB = needGB;
		return 0;
	}

	return 1;
}

// return < 0 for error;  1: OK
int JaguarCPPClient::doKVLen( const char *querys, int len, Jstr &retmsg  )
{
		trimLenEndColonWhite( (char*)querys, len );
		JagStrSplit sp(querys, ' ', true );
		if ( sp.length() != 2 ) {
			_queryerrmsg = "E4042 Error command. Use \"kvlen DB.TABLE; or kvlen DB.INDEX;\" command";
			return -1;
		}

		Jstr dbtab = sp[1];
		JagStrSplit spo( dbtab, '.' );
		if ( spo.length() != 2 ) {
			_queryerrmsg = "E4043 Error command. Use \"kvlen DB.TABLE; or kvlen DB.INDEX;\" command";
			return -2;
		}

		_mapLock->readLock( -1 );
		const JagTableOrIndexAttrs *objAttr = _schemaMap->getValue( dbtab );
		if ( _schemaMap && objAttr ) {
			retmsg = Jstr("KLEN=") + intToStr(objAttr->keylen) + " VLEN=" + intToStr(objAttr->vallen);
			// setEnd = JAG_END_RECVONE_THEN_DONE;
		} else {
			_queryerrmsg = "E4045 Error command. Table or Index does not exist";
			_mapLock->readUnlock( -1 );
			return -3;
		}
		_mapLock->readUnlock( -1 );

	return 1;
}

int JaguarCPPClient::processMaintenanceCommand( const char *querys )
{
	int rc, isShutDown = 0, shellReply = 0;
	Jstr newquery, cmd;
	if ( strncasecmp( querys, "localbackup", 11 ) == 0 ) {
		newquery = "_ex_proclocalbackup";
		cmd = "localbackup";
	} else if ( strncasecmp( querys, "remotebackup", 12 ) == 0 ) {
		newquery = "_ex_procremotebackup";
		cmd = "remotebackup";
	} else if ( strncasecmp( querys, "restorefromremote", 17 ) == 0 ) {
		newquery = "_ex_restorefromremote";
		cmd = "restorefromremote";
	} else if ( strncasecmp( querys, "check", 5 ) == 0 ) {
		trimLenEndColonWhite( (char*)querys, strlen(querys) );
		JagStrSplit sp(querys, ' ', true );
		if ( sp.length() != 2 ) {
			_queryerrmsg = "E4123 Error command. Use \"check DB.TABLE; or check DB.INDEX;\" command";
			return 0;
		}
		Jstr dbtab = sp[1];
		JagStrSplit spo( dbtab, '.' );
		if ( spo.length() != 2 ) {
			_queryerrmsg = "E4124 Error command. Use \"check DB.TABLE; or check DB.INDEX;\" command";
			return 0;
		}
		newquery = Jstr("_ex_repairocheck|") + dbtab;
		shellReply = 1;
		cmd = "checktable";
	} else if ( strncasecmp( querys, "repair", 6 ) == 0 ) {
		trimLenEndColonWhite( (char*)querys, strlen(querys) );
		JagStrSplit sp(querys, ' ', true );
		if ( sp.length() != 2 ) {
			_queryerrmsg = "E4421 Error command. Use \"repair DB.TABLE; or repair DB.INDEX;\" command";
			return 0;
		}
		Jstr dbtab = sp[1];
		JagStrSplit spo( dbtab, '.' );
		if ( spo.length() != 2 ) {
			_queryerrmsg = "E4129 Error command. Use \"repair DB.TABLE; or repair DB.INDEX;\" command";
			return 0;
		}
		newquery = Jstr("_ex_repairobject|") + dbtab;
		shellReply = 1;
		cmd = "repairtable";
	} else if ( strncasecmp( querys, "addcluster", 10 ) == 0 ) {
		if ( ! getnewClusterString( newquery, _queryerrmsg ) ) {
			return 0;
		}
		cmd = "addcluster";
	} else if ( strncasecmp( querys, "shutdown", 8 ) == 0 ) {
		JagStrSplit sp(querys, ' ', true );
		if ( sp.length() < 2 ) { 
			_queryerrmsg = "E5120 Error command. Use \"shutdown <SERVER_IP>\" or \"shutdown all;\"";
			return 0;
		}
		newquery = "_ex_shutdown";
		Jstr targetHost = trimTailChar(sp[1], ';');
		if ( targetHost != "all" ) {
			int found = 0;
    		for ( int i = 0; i < _numHosts; ++i ) {
				if ( (*(_allHosts))[i] == targetHost ) {
					found = 1;
					break;
				}
			}

			if ( ! found ) {
				_queryerrmsg = "Host IP address " + targetHost;
				_queryerrmsg += " not recognized. Please enter the correct IP address of the server to be shutdown.";
				return 0;
			}

			if ( _debug ) { prt(("c8821 jag_hash_lookup(%s) ...\n", targetHost.c_str() )); }
    		JaguarCPPClient *cli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, targetHost.c_str() );
			if ( ! cli ) {
				_queryerrmsg = "Host connection not found.";
				return 0;
			}

			cli->queryDirect( newquery.c_str(), true, false );
			while( cli->reply() ) { }
    		prt(("Jaguar server on %s has been shut down.\n", targetHost.c_str() ));
    		close();
    		exit(1);
		}
		isShutDown = 1;
		cmd = "shutdown";
	}
	
	if ( ! _fullConnectionArg && ! _fullConnectionOK ) {
		_queryerrmsg = "E4081 Error command, not allowed in partial connection";
		return 0;
	}
	
	JagReadWriteMutex mutex( _loadlock, JagReadWriteMutex::WRITE_LOCK );
	pthread_t thrd[_numHosts];
	CliPass pass[_numHosts];
	_spCommandErrorCnt = 0;
	for ( int i = 0; i < _numHosts; ++i ) {
		//prt(("c4829 broadcastAllRegularStatic _host=[%s] host=[%s] ...\n", _host.c_str(), (*(_parentCli->_allHosts))[i].c_str() ));
		if ( _debug ) { prt(("c4221 jag_hash_lookup(%s) ...\n", (*(_parentCli->_allHosts))[i].c_str() )); }
		pass[i].cli = (JaguarCPPClient*) jag_hash_lookup ( &_connMap, (*(_parentCli->_allHosts))[i].c_str() );
		if ( !  pass[i].cli ) {
			continue;
		}
		pass[i].cmd = newquery;
		pass[i].passMode = shellReply;
		// jagpthread_create( &thrd[i], NULL, broadcastAllRegularStatic, (void*)&pass[i] );
		broadcastAllRegularStatic( (void*)&pass[i] );
	}

	/***
	for ( int i = 0; i < _numHosts; ++i ) {
		if ( pass[i].cli ) {
			pthread_join(thrd[i], NULL);
		}
	}
	***/
	
	if ( isShutDown ) {
		prt(("Servers have been shut down. Please restart all servers to continue.\n"));
		close();
		exit(1);
	}
	
	if ( _spCommandErrorCnt > 0 ) {
		_queryerrmsg = "Unable to perform exclusive admin command. Please try again later";
		return 0;
	}
	prt(("%s finished successfully\n", cmd.c_str()));
	return 1;	

}

// form the "_ex_addcluster|#ip1|ip2#ip3|ip4!ip5|ip6" command, where ip5|ip6 are new added servers' ips
// also, make connections with new added servers
int JaguarCPPClient::getnewClusterString( Jstr &newquery, Jstr &queryerrmsg )
{
	/**************************************
	** conf/newcluster.conf
	** ip1
	** ip2
	** ip3
	**************************************/
	Jstr vfpath = jaguarHome() + "/conf/newcluster.conf";
	FILE *fv = jagfopen( vfpath.c_str(), "rb" );
	if ( ! fv ) { 
		queryerrmsg = "No conf/newcluster.conf file found. Operation is not performed.";
		return 0;
	}
	
	newquery = Jstr("_ex_addcluster|") + _primaryHostString + "!";
	char line[2048];
	int len, first = true, cnt = 0;
	Jstr hn, sline;
	abaxint pos;
	JagVector<Jstr> newhosts;
	while ( NULL != (fgets( line, 2048, fv ) ) ) {
		if ( strchr( line, '#' ) ) continue;
		len = strlen( line );
		if ( len < 2 ) { continue; }
		hn = trimTailChar( line );
		hn = trimChar( hn, ' ' );
		sline = JagNet::getIPFromHostName( hn );
		if ( _debug ) {
			prt(("c2939 newhost [%s] ==> [%s]\n", hn.c_str(), sline.c_str() ));
		}
		if ( sline.length() < 2 ) {
			queryerrmsg = Jstr("E8003 Host ") + hn + " cannot resolve to IP address. Operation not performed";
			jagfclose( fv );
			return 0;
		}

		if ( _hostIdxMap->getValue( AbaxString(sline), pos ) ) {
			// if any of the new node is part of original nodes, ignore add cluster
			queryerrmsg = Jstr("E4008 Host ") + hn + "(" + sline + ") already exists in cluster. Operation not performed.";
			jagfclose( fv );
			return 0;
		}

		if ( first ) {
			newquery += sline;
			first = false;
		} else newquery += Jstr("|") + sline;
		newhosts.append( sline );
		++cnt;
	}
   	jagfclose( fv );
	if ( cnt < _faultToleranceCopy ) {
		queryerrmsg = Jstr("E6208 Not enough servers to add. Number of new servers must be at least ") + 
					  intToStr( _faultToleranceCopy );
		return 0;
	}
	
	JaguarCPPClient *jcli[newhosts.size()];
	int errornum = -1;
	for ( int i = 0; i < newhosts.size(); ++i ) {
		jcli[i] = new JaguarCPPClient();
		jcli[i]->setDebug( _debug );
		jcli[i]->_parentCli = this;
		if ( ! jcli[i]->connect( newhosts[i].c_str(), _port, _username.c_str(), _password.c_str(), _dbname.c_str(),
			_unixSocket.c_str(), JAG_CLI_CHILD ) ) {
			errornum = i;
			break;
		}
	}
	
	if ( errornum >= 0 ) {
		for ( int i = 0; i <= errornum; ++i ) {
			delete jcli[i];
			jcli[i] = NULL;
		}
		queryerrmsg = Jstr("E4098 Unable connect to new hosts, addcluster not done");
		return 0; 
	}
	
	for ( int i = 0 ; i < newhosts.size(); ++i ) {
		if ( _debug ) {
			prt(("c5033 jag_hash_insert_str_void(%s) ...\n", newhosts[i].c_str() ));
		}
		jag_hash_insert_str_void ( &_connMap, newhosts[i].c_str(), (void*)jcli[i] );
		++_numHosts;
		if ( _isparent && !_oneConnect ) {
			_allHosts->append( newhosts[i] );
			_hostIdxMap->addKeyValue( AbaxString(newhosts[i]), i );
			_clusterIdxMap->addKeyValue( AbaxString(newhosts[i]), _numClusters );
		}
	}
	_allHostsByCluster->append( newhosts );
	++_numClusters;	
	return 1;
}

// Pick non-mute key cols
void JaguarCPPClient::getHostKeyStr( const char *kbuf, const JagTableOrIndexAttrs *objAttr, JagFixString &hostKeyStr )
{
	char *nbuf = jagmalloc( objAttr->keylen );
	memset( nbuf, 0, objAttr->keylen );
	abaxint pos = 0;
	int offset, klen;
	for ( int i = 0; i < objAttr->numCols; ++i ) {
		if ( ! (*(objAttr->schemaRecord.columnVector))[i].iskey ) continue;
		if ( *((*(objAttr->schemaRecord.columnVector))[i].spare+5) != JAG_KEY_MUTE ) {
			offset = (*(objAttr->schemaRecord.columnVector))[i].offset;
			klen = (*(objAttr->schemaRecord.columnVector))[i].length;
			memcpy( nbuf+pos, kbuf+offset, klen );
			pos += klen;
		}
	}

	hostKeyStr = JagFixString(nbuf, pos);
	// prt(("c4029 hostKeyStr:\n" ));
	// hostKeyStr.dump();
	jagfree( nbuf );
}

Jstr JaguarCPPClient::getSquareCoordStr( const Jstr &shape, const JagParseParam &parseParam, int pos )
{
	if ( strlen( parseParam.otherVec[pos].point.x ) < 1 ) { return "''"; }

	Jstr res = shape + "(";
	res += trimEndZeros(parseParam.otherVec[pos].point.x);
	res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.y);
	res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.a);
	res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.b);
	res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.nx);
	res +=  ")";
	return res;
}

Jstr JaguarCPPClient::getCoordStr( const Jstr &shape, const JagParseParam &parseParam, int pos, 
										     bool hasX, bool hasY, bool hasZ, bool hasWidth, bool hasDepth, bool hasHeight )
{
	if ( strlen( parseParam.otherVec[pos].point.x ) < 1 ) { return "''"; }

	Jstr res = shape + "(";
	if ( hasX ) {
		res += trimEndZeros(parseParam.otherVec[pos].point.x);
		//prt(("s4501 pos=%d res=[%s]\n", pos, res.c_str() ));
	} 

	if ( hasY ) {
		res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.y);
		//prt(("s4402 pos=%d res=[%s]\n", pos, res.c_str() ));
	} 

	if ( hasZ ) {
		res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.z);
		//prt(("s4403 pos=%d res=[%s]\n", pos, res.c_str() ));
	} 

	if ( hasWidth ) {
		res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.a);
	} 

	if ( hasDepth ) {
		res += Jstr(" ") + trimEndZeros(parseParam.otherVec[pos].point.b);
	} 

	if ( hasHeight ) {
		if ( hasDepth ) {
			res += Jstr(" ") + parseParam.otherVec[pos].point.c;
		} else {
			res += Jstr(" ") + parseParam.otherVec[pos].point.b;
		}
	} 
	
	res +=  ")";
	return res;
}

Jstr JaguarCPPClient::getLineCoordStr( const Jstr &shape, const JagParseParam &parseParam,
    											 int pos, bool hasX1, bool hasY1, bool hasZ1, bool hasX2, bool hasY2, bool hasZ2 )
{
	if ( strlen( parseParam.otherVec[pos].linestr.point[0].x ) < 1 ) { return "''"; }

	Jstr res = shape + "(";
	if ( hasX1 ) {
		res += parseParam.otherVec[pos].linestr.point[0].x;
	} 

	if ( hasY1 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[0].y;
	} 

	if ( hasZ1 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[0].z;
	} 

	if ( hasX2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].x;
	} 

	if ( hasY2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].y;
	} 

	if ( hasZ2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].z;
	} 

	res +=  ")";

	prt(("s5041 getLineCoordStr(): res=[%s]\n", res.c_str() ));
	return res;
}

Jstr JaguarCPPClient::getTriangleCoordStr( const Jstr &shape, const JagParseParam &parseParam,
    											 int pos, bool hasX1, bool hasY1, bool hasZ1, 
												 bool hasX2, bool hasY2, bool hasZ2,
												 bool hasX3, bool hasY3, bool hasZ3 )
{
	if ( strlen(parseParam.otherVec[pos].linestr.point[0].x ) < 1 ) {
		return "''";
	}

	Jstr res = shape + "(";
	if ( hasX1 ) {
		res += parseParam.otherVec[pos].linestr.point[0].x;
	} 

	if ( hasY1 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[0].y;
	} 

	if ( hasZ1 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[0].z;
	} 

	if ( hasX2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].x;
	} 

	if ( hasY2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].y;
	} 

	if ( hasZ2 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[1].z;
	} 

	if ( hasX3 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[2].x;
	} 

	if ( hasY3 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[2].y;
	} 

	if ( hasZ3 ) {
		res += Jstr(" ") + parseParam.otherVec[pos].linestr.point[2].z;
	} 

	res +=  ")";

	// prt(("s5041 getTriCoordStr(): res=[%s]\n", res.c_str() ));
	return res;
}

Jstr JaguarCPPClient::get3DPlaneCoordStr( const Jstr &shape, const JagParseParam &parseParam,
                                       int pos, bool hasX, bool hasY, bool hasZ,
                                         bool hasWidth, bool hasDepth, bool hasHeight,
                                         bool hasNX, bool hasNY, bool hasNZ )

{
	if ( strlen( parseParam.otherVec[pos].point.x ) < 1 ) {
		return "''";
	}

	Jstr res = shape + "(";
	if ( hasX ) {
		res += parseParam.otherVec[pos].point.x;
	} 

	if ( hasY ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.y;
	} 

	if ( hasZ ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.z;
	} 

	if ( hasWidth ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.a;
	} 

	if ( hasDepth ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.b;
	} 

	if ( hasHeight ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.c;
	} 

	if ( hasNX ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.nx;
	} 
	if ( hasNY ) {
		res += Jstr(" ") + parseParam.otherVec[pos].point.ny;
	} 

	res +=  ")";
	return res;
}

// buf:  "db.tab.ls=....!db.tab.ls=.....!...
// buf:  "db.tab.ls=....#db.tab.ls=.....#...
Jstr JaguarCPPClient::convertToJson(const char *buf)
{
	if ( ! buf || *buf == '\0' ) {
		// prt(("c2280 convertToJson buf is NULL, return empty str\n" ));
		return "";
	}
	Jstr res, colobj, json;
	//prt(("c2064 convertToJS buf=[%s]\n", buf ));

	const char *p = buf;
	const char *q;
	while ( *p != '\0' ) {
		q = p;
		while ( *q != '=' && *q != '\0' ) ++q;
		if ( *q == '\0' ) {
			//prt(("c5304 break here\n" ));
			break; 
		}

		Jstr dbtabcol(p, q-p);
		//prt(("s2120 dbtabcol=[%s]\n", dbtabcol.c_str() ));
		JagStrSplit sp(dbtabcol, '.' );
		if ( sp.length() < 3 ) { 
			//prt(("s2220 break here dbtabcol=[%s]\n", dbtabcol.c_str() ));
			break; 
		}

		++q;
		p = q;  // jsondata.....!jsondata..
		//while ( *q != '!' && *q != '\0' ) ++q;
		while ( *q != '#' && *q != '\0' ) ++q;
		// p-q is "OJAG=srid=db.tab.col=LS x:y x:y ...."
		Jstr value(p, q-p);
		//prt(("c8282 value=[%s]\n", value.c_str() ));
		colobj = value.firstToken(' ');
		//prt(("c8283 colobj=[%s]\n", colobj.c_str() ));
		JagStrSplit csp(colobj, '=');
		if ( csp.length() < 4 ) break;
		p = value.secondTokenStart(' ');
		//prt(("c3380 secondTokenStart p=[%s]\n", p ));
		json = JagGeo::makeGeoJson(csp, value.secondTokenStart(' ') );
		//prt(("c3318 json=[%s]\n", json.c_str() ));
		if ( res.size() < 1 ) {
			res = dbtabcol + "=" + json;
		} else {
			res += Jstr("\n") + dbtabcol + "=" + json;
		}
		if ( *q == '\0' ) break;
		p = q+1;
	}

	// prt(("c6230 res=[%s]\n", res.c_str() ));
	return res;
}

void JaguarCPPClient::getTimebuf( const JagTableOrIndexAttrs *objAttr, char *timebuf, char *timebuf2 )
{
	if ( JAG_CREATE_DEFUPDATETIME == objAttr->defUpdDatetime || 
		JAG_CREATE_DEFUPDATE == objAttr->defUpdDatetime ||
		JAG_CREATE_DEFDATETIME == objAttr->defDatetime ||
		JAG_CREATE_DEFDATE == objAttr->defDatetime ) {
		struct tm res; struct timeval now; 
		gettimeofday( &now, NULL );
		time_t snow = now.tv_sec;
		jag_localtime_r( &snow, &res );
		memset( timebuf, 0, JAG_FUNC_NOW_LEN_MICRO+1 );
		memset( timebuf2, 0, JAG_FUNC_CURDATE_LEN+1 );
		sprintf( timebuf, "%4d-%02d-%02d %02d:%02d:%02d.%06d",
				 res.tm_year+1900, res.tm_mon+1, res.tm_mday, res.tm_hour, res.tm_min, res.tm_sec, now.tv_usec );
		sprintf( timebuf2, "%4d-%02d-%02d",
				 res.tm_year+1900, res.tm_mon+1, res.tm_mday );
	}
}

void JaguarCPPClient::addQueryToCmdhosts( const JagParseParam &parseParam, const JagFixString &hostkstr, 
										  const JagTableOrIndexAttrs *objAttr, 
										  JagVector<Jstr> &hosts,
										  JagVector<JagDBPair> &cmdhosts, Jstr &newquery )
{

	if ( JAG_CINSERT_OP == parseParam.opcode || JAG_SINSERT_OP == parseParam.opcode ) {
		// for cinsert-dinsert cmds, need to find all servers after this current cluster one
		getUsingHosts( hosts, hostkstr, 3 );
		for ( int i = 0; i < hosts.length(); ++i ) {
			if ( parseParam.insertDCSyncHost.size() > 0 ) {
				newquery = parseParam.insertDCSyncHost + " " + newquery;
			}
			cmdhosts.append( JagDBPair(newquery, hosts[i]) );
		}
	} else {
		// first, get write commands
		getUsingHosts( hosts, hostkstr, 2 );
		if ( objAttr->hasFile ) {
			Jstr snewquery = Jstr("s") + newquery;
			if ( parseParam.insertDCSyncHost.size() > 0 ) {
				snewquery = parseParam.insertDCSyncHost + " " + snewquery;
			}
			cmdhosts.append( JagDBPair(snewquery, hosts[0]) );
			_debug && prt(("c0384 hasFile cmdhosts.append snewquery=[%s] hosts[0]=[%s]\n", snewquery.c_str(),  hosts[0].c_str() ));
		} else {
			if ( parseParam.insertDCSyncHost.size() > 0 ) {
				newquery = parseParam.insertDCSyncHost + " " + newquery;
			}
			cmdhosts.append( JagDBPair(newquery, hosts[0]) );
			if ( 0 && _debug ) {
				prt(("c0385 !hasFile cmdhosts.append newquery=[%s] hosts[0]=[%s]\n", newquery.c_str(),  hosts[0].c_str() ));
			}
		}
		// if not allow duplicates, send 'c' + insertcmd to old clusters to check if exists
		if ( _numClusters >= 2 ) {
			Jstr cnewquery = Jstr("c") + newquery;
			if ( parseParam.insertDCSyncHost.size() > 0 ) {
				cnewquery = parseParam.insertDCSyncHost + " " + cnewquery;
			}
			hosts.clean();
			getUsingHosts( hosts, hostkstr, 1 );
			for ( int i = 0; i < hosts.length(); ++i ) {
				cmdhosts.append( JagDBPair(cnewquery, hosts[i]) );
			}
		}
	}
}

JagNameIndex::JagNameIndex()
{
}

JagNameIndex::JagNameIndex( const JagNameIndex &o)
{
	name = o.name;
	i = o.i;
}

JagNameIndex& JagNameIndex::operator=( const JagNameIndex& o )
{
	if ( this == &o ) return *this;
	name = o.name;
	i = o.i;
	return *this;
}

bool JagNameIndex::operator==(const JagNameIndex& o )
{
	return ( i == o.i );
}

bool JagNameIndex::operator<=(const JagNameIndex& o )
{
	return ( i <= o.i );
}
bool JagNameIndex::operator<(const JagNameIndex& o )
{
	return ( i < o.i );
}
bool JagNameIndex::operator>(const JagNameIndex& o )
{
	return ( i > o.i );
}
bool JagNameIndex::operator>=(const JagNameIndex& o )
{
	return ( i >= o.i );
}


bool JaguarCPPClient::hasDefaultValue( char spare4 )
{
	if ( spare4 == JAG_CREATE_DEFUPDATETIME || spare4 == JAG_CREATE_DEFUPDATE 
	     || JAG_CREATE_DEFINSERTVALUE == spare4 
		 || spare4 == JAG_CREATE_DEFDATETIME || spare4 == JAG_CREATE_DEFDATE ) {
		 return true;
	}
	return false;
}

void JaguarCPPClient::appendJSData( const Jstr &line )
{
	if ( line.size() < 1 ) return;
	jaguar_mutex_lock ( &_lineFileMutex );

	if ( ! _lineFile ) {
		_lineFile = new JagLineFile();
		_debug && prt(("c9301 appendJSData new JagLineFile() line=[%s]\n", line.c_str() ));
	}

	_debug && prt(("c9304 appendJSData line=[%s]\n", line.c_str() ));
	_lineFile->append( line );
	_debug && prt(("c2331 this=%0x parent=%0x doappendJSData line=[%s] _lineFile=%0x\n", this, _parentCli, line.c_str(), _lineFile ));
	_debug && _lineFile->print();
	jaguar_mutex_unlock ( &_lineFileMutex );

}

