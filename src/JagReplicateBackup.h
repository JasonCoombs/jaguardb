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
#ifndef _jag_replicate_backup_h_
#define _jag_replicate_backup_h_

#include <atomic>
#include <abax.h>
#include <JagDef.h>
#include <JagNet.h>
#include <JagThreadPool.h>
#include <JaguarCPPClient.h>

class JagReplicateConnAttr
{
  public:
	JagReplicateConnAttr() { init(); }
	~JagReplicateConnAttr() { destroy(); }
	
	void init() { 
		_sock = INVALID_SOCKET; 
		_port = 0; _clientFlag = 0;
		_fromServ = 0;
		_cliservSameProcess = 0;
		_bulkSend = _hasReply = true;
		_result = 0; _len = 0;
		memset(_hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
		memset(_qbuf, 0, 2048);
		_dbuf = NULL;
	}
	void destroy() { if ( !socket_bad(_sock) ) rayclose( _sock ); }
	
	bool _bulkSend;
	bool _hasReply;
	bool _fromServ;
	int _result;
    JAGSOCK _sock;
	int _port;
	int _cliservSameProcess;
	abaxint _len;
	Jstr _host;
	Jstr _username;
	Jstr _password;
	Jstr _dbname;
	Jstr _unixSocket;
	Jstr _session;
	Jstr _querys;
	uabaxint _clientFlag;
	char _qbuf[2048];
	char _hdr[JAG_SOCK_MSG_HDR_LEN+1];
	char *_dbuf;
};

class JagReplicateBackup
{
  public:
	JagReplicateBackup( int timeout=2, int retrylimit=30 );
	~JagReplicateBackup();
	
	bool setConnAttributes( int replicateCopy, int deltaRecoverConnection,
		unsigned int port, uabaxint clientFlag, bool fromServ,
		JagVector<Jstr> &hostlist, Jstr &username, Jstr &passwd,
		Jstr &dbname, Jstr &unixSocket );

	void updateDBName( Jstr &dbname );
	void updatePassword( Jstr &password );

	int makeConnection( int i );

	// query method
	abaxint simpleQuery( int i, const char *querys, bool checkConnection );
	abaxint sendQuery( const char *querys, abaxint len, bool hasReply, bool isWrite, bool bulkSend, bool forceConnection );
	abaxint simpleSend( int i, const char *querys, abaxint len );
	static void *simpleSendStatic( void *conn );

	abaxint sendDirectToSockAll( const char *mesg, abaxint len, bool nohdr=false );
	abaxint recvDirectFromSockAll( char *&buf, char *hdr );
	abaxint recvDirectFromSockAll( char *&buf, abaxint len );
	bool sendFilesToServer( const Jstr &inpath );
	static void *sendFilesToServerStatic( void *conn );
	bool recvFilesFromServer( const Jstr &outpath );

	// reply method
	// simpleQuery --> simpleReply
	// sendQuery --> recvReply
	abaxint simpleReply( int i, char *hdr, char *&buf );
	abaxint recvReply( char *hdr, char *&buf );

	// special signal
	int sendTwoBytes( const char *condition );
	int recvTwoBytes( char *condition );

	void setConnectionBrokenTime();
	int checkConnectionRetry();

	int _tdiff;
	int _replicateCopy;
	int _deltaRecoverConnection;
	int	_replypos; // only used by read commands
	int _isWrite;
	int _tcode;
	// thpool_ *_sendPool;
	JagReplicateConnAttr _conn[JAG_REPLICATE_MAX];
	char _servsignal[4];

	int _dtimeout;
	int _connRetryLimit;
	std::atomic<abaxint>	_lastConnectionBrokenTime;
	int  _connectTimeout;
	int   _allSocketsBad;
	int   _debug;
};

#endif
