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

#include <JagReplicateBackup.h>
#include <JagUtil.h>
#include <JagTime.h>
#include <JagStrSplit.h>
#include <JagFastCompress.h>
#include <jagcrypt.h>

JagReplicateBackup::JagReplicateBackup( int timeout, int retrylimit )
{ 
	memset(_servsignal, 0, 4 );
	_dtimeout = timeout;
	_connRetryLimit = retrylimit;
	_lastConnectionBrokenTime = 0;
	_tdiff = JagTime::getTimeZoneDiff();
	_replicateCopy = 1;
	_deltaRecoverConnection = 0;
	_replypos = -1;
	_isWrite = 1;
	_tcode = 0;
	_allSocketsBad = 0;
	// _sendPool = thpool_init( JAG_REPLICATE_MAX );
	for ( int i = 0; i < JAG_REPLICATE_MAX; ++i ) {
		_conn[i].init();
	}

	_connectTimeout = 5;  // secs
	_debug = 0;
}

JagReplicateBackup::~JagReplicateBackup()
{
	/***
	thpool_wait( _sendPool );
	thpool_destroy( _sendPool );
	***/
	
	for ( int i = 0; i < JAG_REPLICATE_MAX; ++i ) {
		if ( _conn[i]._dbuf ) free( _conn[i]._dbuf );
	}
}

bool JagReplicateBackup::setConnAttributes( int replicateCopy, int deltaRecoverConnection,
	unsigned int port, uabaxint clientFlag, bool fromServ,
	JagVector<AbaxDataString> &hostlist, AbaxDataString &username, AbaxDataString &password,
	AbaxDataString &dbname, AbaxDataString &unixSocket )
{
	int success = 0;
	_replicateCopy = replicateCopy;
	_deltaRecoverConnection = deltaRecoverConnection;
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._port = port;
		_conn[i]._host = hostlist[i];
		_conn[i]._username = username;
		_conn[i]._password = password;
		_conn[i]._dbname = dbname;
		_conn[i]._unixSocket = unixSocket;
		_conn[i]._clientFlag = clientFlag;
		_conn[i]._fromServ = fromServ;
		if ( i != 0 ) success += makeConnection( i ); // make other connections beside main connection
	}
	return success;
}

// method to update dbname after sucssfully "use db"
void JagReplicateBackup::updateDBName( AbaxDataString &dbname )
{
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._dbname = dbname;
	}
}

// method to update password after sucssfully "changepass uid password"
void JagReplicateBackup::updatePassword( AbaxDataString &password )
{
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._password = password;
	}
}

// similar method to CPPClient connect method, make connection to server
// these connection only apply for replicate connections, major connection still applied in client cpp
int JagReplicateBackup::makeConnection( int i )
{	
	int rc, authed = 0, usedbok = 0;
	abaxint len = 0;
	char querys[512]; // need to be large enough after encrypt the password; at least 300 bytes for now 
	// try socket connection
	_conn[i]._sock = rayconnect( _conn[i]._host.c_str(), _conn[i]._port, _connectTimeout, false );
	if ( socket_bad(_conn[i]._sock) ) return 0;
	
	// get server's public key
	sprintf( querys, "_getpubkey" );
	rc = simpleQuery( i, querys, false ); 
	if ( rc < 0 ) { return 0; }
	len = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
	if ( len <= 0 || _conn[i]._dbuf == NULL ) { return 0; }
	AbaxDataString pubkey =  _conn[i]._dbuf;
	// prt(("c4203 replicate reply() rc=%d  got data=[%s]\n", rc, _conn[i]._dbuf ));
	while( simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf ) > 0 ) {}

    AbaxDataString encPass;
    encPass = JagEncryptStr( pubkey, _conn[i]._password.c_str() );

	// do authenticate user
	sprintf( querys, "auth|%s|%s|%d|%d|%d|%d|%s|%d", _conn[i]._username.c_str(), encPass.c_str(),
		_tdiff, _conn[i]._fromServ, i, _deltaRecoverConnection, _conn[i]._unixSocket.c_str(), getpid() );
	rc = simpleQuery( i, querys, false ); // todo
	if ( rc < 0 ) return 0;
	
	// recv auth info
	while ( 1 ) {
		len = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
		if ( len <= 0 ) break;
		if ( _conn[i]._dbuf && _conn[i]._dbuf[0] == 'O' && _conn[i]._dbuf[1] == 'K' ) {
			authed = 1;
			JagStrSplit sp( _conn[i]._dbuf, ' ' );
			if ( sp.length() >= 2 ) {
				_conn[i]._session = sp[1];
			}
			if ( sp.length() >= 7 ) {
				_conn[i]._cliservSameProcess = atoi( sp[6].c_str() );
			}
		}
	}	
	if ( authed == 0 ) return 0;

	// do use db
	sprintf( querys, "use %s;", _conn[i]._dbname.c_str() );
	rc = simpleQuery( i, querys, false ); // todo
	if ( rc < 0 ) return 0;
	while ( 1 ) {
		len = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
		if ( len <= 0 ) break;
		if ( _conn[i]._dbuf && 0==strncmp( _conn[i]._dbuf, "Database", 8 ) ) { usedbok = 1; }
	}
	if ( usedbok == 0 ) return 0;

	// set timeout socket for server connection
	if ( 1 ) {
		/***
		struct timeval tv; 
		struct linger so_linger;
		so_linger.l_onoff = 1;
		so_linger.l_linger = _dtimeout;
		tv.tv_sec = _dtimeout;
		if ( _conn[i]._cliservSameProcess > 0 ) tv.tv_sec = 0;
		tv.tv_usec = 0;
		setsockopt(_conn[i]._sock, SOL_SOCKET, SO_SNDTIMEO, (CHARPTR)&tv, sizeof(tv));
		setsockopt(_conn[i]._sock, SOL_SOCKET, SO_RCVTIMEO, (CHARPTR)&tv, sizeof(tv));
		setsockopt(_conn[i]._sock, SOL_SOCKET, SO_LINGER, (CHARPTR)&so_linger, sizeof(so_linger) );
		***/
		JagNet::setRecvSndTimeOut( _conn[i]._sock, _dtimeout, _conn[i]._cliservSameProcess );
	}
	
	return 1;
}

// for each sendQuery, if some connection is broken, try reconnect again
abaxint JagReplicateBackup::sendQuery( const char *querys, abaxint len, bool hasReply, bool isWrite, bool bulkSend, bool forceConnection )
{
	// check reconnection, if all failure connection, return -1;
	_allSocketsBad = 0;
	int connCheck = 0, numCheck = 0;
	for ( int i = 0; i < _replicateCopy; ++i ) {
		if ( forceConnection ) {
			if ( socket_bad(_conn[i]._sock) ) {
				connCheck += makeConnection( i );
				++numCheck;
			}
		} else {
			if ( socket_bad(_conn[i]._sock) && checkConnectionRetry() ) {
				connCheck += makeConnection( i );
				++numCheck;
			}
		}
	}
	if ( connCheck <= 0 && numCheck >= _replicateCopy ) { return -1; }
	
	abaxint rc = -1;
	_replypos = -1;
	_tcode = 0;
	_isWrite = isWrite;
	// read related querys, e.g. select, show, desc
	if ( !isWrite ) {
		for ( int i = 0; i < _replicateCopy; ++i ) {
			_conn[i]._hasReply = hasReply;
			_conn[i]._bulkSend = bulkSend;
			rc = simpleSend( i, querys, len );
			if ( rc >= 0 ) {
				_replypos = i;
				break;
			}
		}
		return rc;
	}
	
	// write related querys, including but not limited to insert, cinsert, dinsert, create, alter, update, delete, drop, truncate, import
	memset( _servsignal, 'N', _replicateCopy );
	memset( _servsignal+_replicateCopy, 'Y', JAG_REPLICATE_MAX-_replicateCopy );
	/***
	thpool_wait( _sendPool );
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._querys = AbaxDataString(querys, len);
		_conn[i]._len = len;
		_conn[i]._hasReply = hasReply;
		_conn[i]._bulkSend = bulkSend;
		thpool_add_work( _sendPool , simpleSendStatic, (void*)&_conn[i] );
    }
	thpool_wait( _sendPool );
	***/
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._querys = AbaxDataString(querys, len);
		_conn[i]._len = len;
		_conn[i]._hasReply = hasReply;
		_conn[i]._bulkSend = bulkSend;
		simpleSendStatic( (void*)&_conn[i] );
    }

	
	for ( int i = 0; i < _replicateCopy; ++i ) {
		if ( _conn[i]._result > 0 ) {
			rc = _conn[i]._len;
			break;
		} else {
			setConnectionBrokenTime();
		}
	}

	if ( rc < 0 ) return -1;
	return rc;
}

// method to send mesg directly to sockets
abaxint JagReplicateBackup::sendDirectToSockAll( const char *mesg, abaxint len, bool nohdr )
{
	abaxint rlen;
	if ( !_isWrite ) {
		// read related query, simple reply from replypos socket
		if ( _replypos < 0 ) return -1;
		// if ( _debug ) { prt(("c0431 JagReplicateBackup::simpleReply ...\n" )); }
		rlen = sendDirectToSock( _conn[_replypos]._sock, mesg, len, nohdr );
	} else {
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( _conn[i]._result == 1 ) {
				rlen = sendDirectToSock( _conn[i]._sock, mesg, len, nohdr );
			}
    	}
	}
	return rlen;
}

// method to recv mesg directly from sockets
abaxint JagReplicateBackup::recvDirectFromSockAll( char *&buf, char *hdr )
{
	abaxint rlen;
	if ( !_isWrite ) {
		// read related query, simple reply from replypos socket
		if ( _replypos < 0 ) return -1;
		// if ( _debug ) { prt(("c0431 JagReplicateBackup::simpleReply ...\n" )); }
		rlen = recvDirectFromSock( _conn[_replypos]._sock, buf, hdr );
	} else {
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( _conn[i]._result == 1 ) {
				rlen = recvDirectFromSock( _conn[i]._sock, buf, hdr );
			}
    	}
	}
	return rlen;
}
abaxint JagReplicateBackup::recvDirectFromSockAll( char *&buf, abaxint len )
{
	abaxint rlen;
	if ( !_isWrite ) {
		// read related query, simple reply from replypos socket
		if ( _replypos < 0 ) return -1;
		// if ( _debug ) { prt(("c0431 JagReplicateBackup::simpleReply ...\n" )); }
		rlen = recvData( _conn[_replypos]._sock, buf, len );
	} else {
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( _conn[i]._result == 1 ) {
				rlen = recvData( _conn[i]._sock, buf, len );
			}
    	}
	}
	return rlen;
}

// method only called by client "insert" command
// return 0: all server send error; return 1: else
bool JagReplicateBackup::sendFilesToServer( const AbaxDataString &inpath )
{
	int rc = 0;
	
	/***
	thpool_wait( _sendPool );
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._querys = inpath;
		thpool_add_work( _sendPool , sendFilesToServerStatic, (void*)&_conn[i] );
    }
	thpool_wait( _sendPool );
	***/
	for ( int i = 0; i < _replicateCopy; ++i ) {
		_conn[i]._querys = inpath;
		sendFilesToServerStatic( (void*)&_conn[i] );
    }
	
	for ( int i = 0; i < _replicateCopy; ++i ) {
		rc += _conn[i]._result;
	}

	return rc;
}

void *JagReplicateBackup::simpleSendStatic( void *conn )
{
	JagReplicateConnAttr *ptr = (JagReplicateConnAttr*)conn;
	if ( ptr->_bulkSend ) beginBulkSend( ptr->_sock );
	abaxint rc = sendData( ptr->_sock, ptr->_querys.c_str(), ptr->_len ); // 016ABCmessage format
	if ( ptr->_bulkSend ) endBulkSend( ptr->_sock );
	if ( rc < 0 ) {
		ptr->_sock = INVALID_SOCKET;
		ptr->_result = 0;
		ptr->_len = rc;
	} else {
		ptr->_result = 1;
		ptr->_len = rc;
	}
	return NULL;
}

void *JagReplicateBackup::sendFilesToServerStatic( void *conn )
{
	JagReplicateConnAttr *ptr = (JagReplicateConnAttr*)conn;
	oneFileSender( ptr->_sock, ptr->_querys );
	return NULL;
}

// simple send, send data to server without locks and other unnecessary stuffs, no retry inside
abaxint JagReplicateBackup::simpleSend( int i, const char *querys, abaxint len )
{
	abaxint rc = sendData( _conn[i]._sock, querys, len ); // 016ABCmessage format
	_replypos = i; 
	if ( rc < 0 ) {
		_conn[i]._sock = INVALID_SOCKET;
		_replypos = -1;
		setConnectionBrokenTime();
		return -1;
	}
	return rc;
}

// simple send two signal bytes to all connections, "OK" || "ON" || "NG"
int JagReplicateBackup::sendTwoBytes( const char *condition )
{
	char twobuf[JAG_SOCK_MSG_HDR_LEN+3];
	sprintf( twobuf, "%0*ldACCC", JAG_SOCK_MSG_HDR_LEN-4, 2 );
	twobuf[JAG_SOCK_MSG_HDR_LEN] = *condition; 
	twobuf[JAG_SOCK_MSG_HDR_LEN+1] = *(condition+1); 
	twobuf[JAG_SOCK_MSG_HDR_LEN+2] = '\0';
	int rc, rc2 = 0;
	for ( int i = 0; i < _replicateCopy; ++i ) {
		if ( !socket_bad(_conn[i]._sock) ) {
			rc = sendData( _conn[i]._sock, twobuf, JAG_SOCK_MSG_HDR_LEN+2 ); // 016ABCmessage format
			if ( rc < 0 ) {
				_conn[i]._sock = INVALID_SOCKET;
				setConnectionBrokenTime();
			}
			rc2 += rc;
		}
	}
	return rc2;
}

// simple receive two signal bytes from all connections, "NG" || "OK"
int JagReplicateBackup::recvTwoBytes( char *condition )
{
	if ( _debug ) { prt(("c2201 JagReplicateBackup::recvTwoBytes _replicateCopy=%d...\n", _replicateCopy )); }
 	char hdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *buf = NULL;
	int rc, result = 0;
	abaxint hbs = 0;
	for ( int i = 0; i < _replicateCopy; ++i ) {
		if ( !socket_bad(_conn[i]._sock) ) {
			while ( 1 ) {
				_debug && prt(("c2034 JagReplicateBackup recvData()...\n"));
				rc = recvData( _conn[i]._sock, hdr, buf );
				_debug && prt(("c2035 hdr=[%s] buf=[%s] rc=%d\n", hdr, buf, rc ));
				if ( rc < 0 ) {
					_conn[i]._sock = INVALID_SOCKET;
					setConnectionBrokenTime();
					// result = 0;
					break;
				} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) { 
				    // HB alive, ignore
					++hbs; if ( hbs > 1000000000 ) hbs = 0;
					if ( _debug && ( (hbs%100) == 0 ) ) {
						prt(("r1280 JagReplicateBackup::recvTwoBytes() recevd too many HB\n" ));
					}
					continue;
				} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'S' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'S' && *buf == 'O' && *(buf+1) == 'K' ) {
					// special signal bytes and OK
					// prt(("s3723 got SS OK break\n"));
					result = 1;
					break;
				} else if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'S' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'S' && *buf == 'N' && *(buf+1) == 'G' ) {
					// special signal bytes and NG
					break;
				}

				if ( buf ) { free(buf); buf = NULL; }
			}
			// prt(("s9383 while done\n"));
		}
	}

	if ( 1 == result ) {
		condition[0] = 'O'; condition[1] = 'K';
	} else {
		condition[0] = 'N'; condition[1] = 'G';
	}

	if ( buf ) free( buf );
	if ( _debug ) { prt(("c2201 JagReplicateBackup::recvTwoBytes done\n" )); }

	return rc;
}

// simple query, send data to server without locks and other unnecessary stuffs, no retry inside
// used by self class methods and delta recover method
abaxint JagReplicateBackup::simpleQuery( int i, const char *querys, bool checkConnection )
{
	_debug && prt(("c2501 JagReplicateBackup::simpleQuery (%s) %d ...\n", querys, checkConnection )); 
	// check reconnection
	if ( checkConnection ) {
		abaxint connCheck = 1;
		if ( socket_bad(_conn[i]._sock) && checkConnectionRetry() ) {
			connCheck = makeConnection( i );
		}
		if ( socket_bad(_conn[i]._sock) || !connCheck ) { return -1; }
	}

	const char *p;
	bool compress = false, batchReply = false;
	abaxint rc = 0, len = strlen( querys );
	while ( *querys == ' ' ) ++querys;
	if ( len >= JAG_SOCK_COMPRSS_MIN ) {
		compress = true;
	}
	if ( strncasecmp( querys, "insert", 6 ) == 0 || 
		strncasecmp( querys, "cinsert", 7 ) == 0 || strncasecmp( querys, "dinsert", 7 ) == 0 ) {
		batchReply = true;
	}
	
	AbaxDataString compressedStr;
	if ( compress ) {
		JagFastCompress::compress( querys, compressedStr );
		if ( batchReply ) {
			sprintf( _conn[i]._hdr, "%0*lldABZC", JAG_SOCK_MSG_HDR_LEN-4, compressedStr.size() ); 
		} else {
			sprintf( _conn[i]._hdr, "%0*lldACZC", JAG_SOCK_MSG_HDR_LEN-4, compressedStr.size() ); 
		}
		p = compressedStr.c_str();
		len = compressedStr.size();
	} else {
		if ( batchReply ) {
			sprintf( _conn[i]._hdr, "%0*lldABCC", JAG_SOCK_MSG_HDR_LEN-4, len );
		} else {
			sprintf( _conn[i]._hdr, "%0*lldACCC", JAG_SOCK_MSG_HDR_LEN-4, len );   
		}
		p = querys;
	}

	if ( JAG_SOCK_MSG_HDR_LEN + len <= 2048 ) {
		memset( _conn[i]._qbuf, 0, 2048 );
		memcpy( _conn[i]._qbuf, _conn[i]._hdr, JAG_SOCK_MSG_HDR_LEN );
		memcpy( _conn[i]._qbuf+JAG_SOCK_MSG_HDR_LEN, p, len );
		rc = simpleSend( i, _conn[i]._qbuf, JAG_SOCK_MSG_HDR_LEN+len );
		if ( _debug ) { prt(("c2401 JagReplicateBackup::simpleSend rc=%d\n", rc )); }
	} else {
		beginBulkSend( _conn[i]._sock );
		rc = simpleSend( i, _conn[i]._hdr, JAG_SOCK_MSG_HDR_LEN );
		if ( rc > 0 ) rc = simpleSend( i, p, len );
		endBulkSend( _conn[i]._sock );
		if ( _debug ) { prt(("c2404 JagReplicateBackup::simpleSend rc=%d\n", rc )); }
	}
	_conn[i]._hasReply = 1;
	
	_debug && prt(("c2404 JagReplicateBackup::simpleQuery rc=%d\n", rc ));
	return rc;
}

// simple reply, which is simliar to do reply in client cpp, used by replicate connection only ( receive reply but not process )
// however, need to ignore "HB"
// return -1: error; return 0: _END_ received; return -2: no more data; else return length of data
abaxint JagReplicateBackup::simpleReply( int i, char *hdr, char *&buf )
{
	_debug && prt(("c3804 JagReplicateBackup::simpleReply ...\n" ));

	abaxint len = 0;
	abaxint hbs = 0;  // HB numbers received
	while ( 1 ) {
		if ( _conn[i]._hasReply == 0 ) return -2; 
		if ( buf ) {
			free( buf );
			buf = NULL;
		}
		memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN );
		_debug && prt(("c2038 JagReplicateBackup recvData ...\n" ));
		len = recvData( _conn[i]._sock, hdr, buf );
		_debug && prt(("c2038 JagReplicateBackup recvData done len=%d\n", len ));
		_debug && prt(("c2038 JagReplicateBackup recvData hdr=[%s] buf=[%s]\n", hdr, buf ));
		if ( len < 0 ) {
			_conn[i]._sock = INVALID_SOCKET;
			_conn[i]._hasReply == 0;
			setConnectionBrokenTime();
			return -1;
		} else if ( len == 0 ) {
			continue;
		}
		
		// if compressed, uncompress data
		if ( hdr[JAG_SOCK_MSG_HDR_LEN-4] == 'Z' ) { 
			AbaxDataString compressed( buf, len );
			AbaxDataString unCompressed;
			JagFastCompress::uncompress( compressed, unCompressed );
			free( buf );
			buf = (char*)jagmalloc( unCompressed.size()+1 );
			memcpy( buf, unCompressed.c_str(), unCompressed.size() );
			buf[unCompressed.size()] = '\0';
			len = unCompressed.size();
		}

		// if receive 'HB', ignore and continue
		if ( hdr[JAG_SOCK_MSG_HDR_LEN-3] == 'H' && hdr[JAG_SOCK_MSG_HDR_LEN-2] == 'B' ) {
			++hbs;
			if ( _debug && ( (hbs%100) == 0 ) ) {
				prt(("r1029 JagReplicateBackup simpleReply() recved too many HB\n" ));
				if ( hbs > 100000000 ) hbs = 0;
			}
			continue;
		}

		// if _END_ , no more data to receive
		if ( buf[0] == '_' && buf[1] == 'E' && buf[2] == 'N' && buf[3] == 'D' && buf[4] == '_' ) {
			_conn[i]._hasReply = 0;
			if ( _tcode != 777 ) {
				// not to do data center
				_tcode = _getFieldInt( buf, 'T' );
			}
			return 0;
		}
		break;
	}

	return len;
}

// method called by doreply in cpp client
abaxint JagReplicateBackup::recvReply( char *hdr, char *&buf )
{
	if ( _debug ) { prt(("c0931 JagReplicateBackup::recvReply ...\n" )); }
	if ( !_isWrite ) {
		// read related query, simple reply from replypos socket
		if ( _replypos < 0 ) return -1;
		//if ( _debug ) { prt(("c0431 JagReplicateBackup::simpleReply ...\n" )); }
		return simpleReply( _replypos, hdr, buf );
	}
	
	abaxint len = -1, len2 = -1, rc;
	// else, write related query, need to format server status signal after receive all _END_ message
	// receive available reply if needed
	for ( int i = 0; i < _replicateCopy; ++i ) {
		if ( !socket_bad(_conn[i]._sock) && _conn[i]._result > 0 ) { 
		    // connection is valid before reply, and send query success
			if ( _conn[i]._hasReply != 0 ) {
				if ( len < 0 ) {
					// major reply, give back to cpp client class
					len = simpleReply( i, hdr, buf );
					if ( len == -1 ) { // error reply, set servsignal to 'N' and continue
						_conn[i]._result = 0;
					} else if ( len == 0 || len == -2 ) {
						// receive _END_ message, represent last query has successfully processed by server, set 'Y'
						_servsignal[i] = 'Y';
					}
				} else {
					// other reply, just check to see server signal, no data back to cpp client, after major reply get its message
					len2 = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
					if ( len2 == -1 ) { // error reply, leave servsignal to 'N' and continue
						_conn[i]._result = 0;
					} else if ( len2 == 0 || len2 == -2 ) {
						_servsignal[i] = 'Y';
					}
				}
			}
		}
	}

	_debug && prt(("c0431 JagReplicateBackup::len=%d ...\n", len ));
	
	// if major reply has already reached to the end, make sure all other socket replys have also reached to the end
	// else, if no more major replys, which means all related servers ( main server and backup servers ) are all dead, return -1
	if ( len == 0 || len == -2 ) {
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( !socket_bad(_conn[i]._sock) && _conn[i]._result > 0 ) { // connection is valid before reply
				if ( _conn[i]._hasReply != 0 ) {
					while ( 1 ) {
						len2 = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
						if ( len2 <= 0 ) {
							if ( len2 == -1 ) {
								_conn[i]._result = 0;
							} else {
								_servsignal[i] = 'Y';
							}
							break;
						}
					}
				}
			}
		}

		// after receive all replys from servers, send serv signal to them to store/ignore delta information
		char threebuf[JAG_SOCK_MSG_HDR_LEN+4];
		sprintf( threebuf, "%0*ldACCC", JAG_SOCK_MSG_HDR_LEN-4, 3 );
		threebuf[JAG_SOCK_MSG_HDR_LEN] = *_servsignal; 
		threebuf[JAG_SOCK_MSG_HDR_LEN+1] = *(_servsignal+1); 
		threebuf[JAG_SOCK_MSG_HDR_LEN+2] = *(_servsignal+2); 
		threebuf[JAG_SOCK_MSG_HDR_LEN+3] = '\0';
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( !socket_bad(_conn[i]._sock) && _conn[i]._result > 0 ) {
				rc = sendData( _conn[i]._sock, threebuf, JAG_SOCK_MSG_HDR_LEN+3 ); // 016ABCCmessage format
				if ( rc < 0 ) {
					_conn[i]._result = 0;
					_conn[i]._sock = INVALID_SOCKET;
					setConnectionBrokenTime();
				} else {
					_conn[i]._hasReply = 1;
				}
			}
		}

		// then, wait for all servers to finish processing delta information
		for ( int i = 0; i < _replicateCopy; ++i ) {
			if ( !socket_bad(_conn[i]._sock) && _conn[i]._result ) { // connection is valid before reply
				if ( _conn[i]._hasReply != 0 ) {
					while ( 1 ) {
						len2 = simpleReply( i, _conn[i]._hdr, _conn[i]._dbuf );
						if ( len2 <= 0 ) {
							if ( len2 == -1 ) {
								_conn[i]._result = 0;
								_conn[i]._sock = INVALID_SOCKET;
								setConnectionBrokenTime();
							}
							break;
						}
					}
				}
			}
		}
	}
	
	if ( _debug ) { prt(("c5431 JagReplicateBackup::recvReply done len=%d ...\n", len )); }
	return len;
}

// receive one file
bool JagReplicateBackup::recvFilesFromServer( const AbaxDataString &outpath )
{
	// read related query, simple reply from replypos socket
	if ( _replypos < 0 ) return 0;
	return oneFileReceiver( _conn[_replypos]._sock, outpath, false );
};	

// update connection broken time
void JagReplicateBackup::setConnectionBrokenTime()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	_lastConnectionBrokenTime = now.tv_sec;
}

// check if connection broken time is exceed retry limit
int JagReplicateBackup::checkConnectionRetry()
{
	struct timeval now;
	gettimeofday( &now, NULL );
	if ( now.tv_sec - _lastConnectionBrokenTime > _connRetryLimit ) return true;
	else return false;
}

