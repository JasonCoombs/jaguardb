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
#ifndef _jaguar_cpp_client_h_
#define _jaguar_cpp_client_h_

#include <atomic>
#include "JagNet.h"

#include <abax.h>
#include <jaghashtable.h>
#include <JagKeyValProp.h>
#include <JagThreadPool.h>
#include <JagTableOrIndexAttrs.h>
#include <JagHashStrStr.h>

#define JAG_SOCK_MSG_HDR_LEN       20

class JagCfg;
class ADBROW;
class CliPass;
class JagRecord;
class JagDBPair;
class JagDBServer;
class JagBlockLock;
class HostMinMaxKey;
class JagParseParam;
class JagIndexString;
class JagDataAggregate;
class JagReadWriteLock;
class JagSchemaAttribute;
class JagReplicateBackup;
class OtherAttribute;
template <class Pair> class JagVector;
template <class K, class V> class JagHashMap;
template <class Pair> class JagArray;

class JagUUID;
class JagSchemaRecord;
class JagDiskArrayClient;
class JagBuffReader;
class JagBuffBackReader;
class JagMemDiskSortArray;
class JagLineFile;

struct JagNameIndex
{
	int i;
	AbaxDataString name;
	JagNameIndex();
	JagNameIndex( const JagNameIndex &o);
	JagNameIndex& operator=( const JagNameIndex& o );
	bool operator==(const JagNameIndex& o );
	bool operator<=(const JagNameIndex& o );
	bool operator<(const JagNameIndex& o );
	bool operator>(const JagNameIndex& o );
	bool operator>=(const JagNameIndex& o );
};

class JaguarCPPClient 
{
  public:
    //////////// Business API calls, such as from JDBC or C++, Java client
  	JaguarCPPClient( );
	~JaguarCPPClient();
	
	int connect( const char *ipaddress, unsigned int port, const char *username, const char *passwd,
		const char *dbname, const char *unixSocket, uabaxint clientFlag );
	int execute( const char *query );
    int query( const char *query, bool reply=true ); 
    int apiquery( const char *query ); 
    int queryDirect( const char *query, bool reply=true, bool batchReply=false, bool dirConn=false, bool forceConnection=false ); 
    int reply( bool headerOnly=false );
    int replyAll( bool headerOnly=false );
    int doreplyAll( bool headerOnly=false );
	int freeResult() { return freeRow(); } // alias of freeRow
	void close();

    int getInt(  const char *name, int *value );  
    int getLong(  const char *name, abaxint *value );
    int getFloat(  const char *name, float *value );
    int getDouble(  const char *name, double *value );
	
	// method which needs a do method
	int 	printRow();
	bool 	printAll();
	char   *getAll();
	char   *getAllByName( const char *name );
	char   *getAllByIndex( int i ); // 1---->len
	char    *getRow();
	int 	hasError( );
	int 	freeRow( int type=0 );
	const 	char *error();
	const 	char *status();
	int   	errorCode();
    const 	char *getMessage();
	const 	char *jsonString();

	char 	*getValue( const char *name );
	char 	*getNthValue( int nth );
	int  	getColumnCount();

	char 	*getCatalogName( int col );
	char 	*getColumnClassName( int col );	
	int  	getColumnDisplaySize( int col );
	char 	*getColumnLabel( int col );
	char 	*getColumnName( int col );	
	int  	getColumnType( int col );
	char 	*getColumnTypeName( int col );		
	int  	getScale( int col ); 
	char 	*getSchemaName( int col );
	char 	*getTableName( int col );
	bool 	isAutoIncrement( int col );
	bool 	isCaseSensitive( int col );
	bool 	isCurrency( int col );
	bool 	isDefinitelyWritable( int col );	
	int  	isNullable( int col );
	bool 	isReadOnly( int col );
	bool 	isSearchable( int col );
	bool 	isSigned( int col );


	///////// Below are public, called by Jaguar programs, but not public APIs
	static bool getSQLCommand( AbaxDataString &sqlcmd, int echo, FILE *fp, int saveNewline=0 );
	static int doAggregateCalculation( const JagSchemaRecord &aggrec, const JagVector<int> &selectPartsOpcode,
						const JagVector<AbaxFixString> &oneSetData, char *aggbuf, int datalen, int countBegin );

	int concurrentDirectInsert( const char *querys );
	JAGSOCK getSocket() const;
	const char *getSession();
	const char *getDatabase();
	AbaxDataString getHost() const;
	abaxint getMessageLen();
	char getMessageType();
	void destroy();
	void flush();
	
	// update schema and/or host list
	void rebuildSchemaMap();
	void updateSchemaMap( const char *schstr );
	// void updateDefvalMap( const AbaxDataString &schstr );

	void setWalLog( bool flag = true );
	void getReplicateHostList( JagVector<AbaxDataString> &hostlist );
	void getSchemaMapInfo( AbaxDataString &schemaInfo );
	int oneCmdInsertPool( JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer> > &qmap, const AbaxDataString &cmd );
	abaxint flushQMap( JagHashMap<AbaxString, AbaxPair<AbaxString,AbaxBuffer> > &qmap );
	void updateDBName( const AbaxDataString &dbname );
	void updatePassword( const AbaxDataString &password );
	void setFullConnection( bool flag );
	void setDebug( bool flag );
	int recoverDeltaLog( const AbaxDataString &inpath );
	int importLocalFileFamily( const AbaxDataString &inpath, const char *spstr=NULL );

	abaxint sendDirectToSockAll( const char *mesg, abaxint len, bool nohdr=false );
	abaxint recvDirectFromSockAll( char *&buf, char *hdr );
	abaxint doSendDirectToSockAll( const char *mesg, abaxint len, bool nohdr=false );
	abaxint doRecvDirectFromSockAll( char *&buf, char *hdr );
	abaxint recvDirectFromSockAll( char *&buf, abaxint len );
	abaxint doRecvDirectFromSockAll( char *&buf, abaxint len );
	void appendJSData( const AbaxDataString &line );


    // public data members
	AbaxDataString _version;	
	AbaxDataString _dbname;
	short 			_queryCode;
	JagBlockLock *_mapLock;
	int 		_deltaRecoverConnection;
	int 		_servCurrentCluster;
	int 		_tdiff;
	int 		_sleepTime;
	jag_hash_t _connMap;
	int		   _allSocketsBad;
	int      _isDataCenterSync;
	int      _datcSrcType;   // connecting from to HOST or GATE JAG_DATACENTER_HOST/JAG_DATACENTER_GATE
	int      _datcDestType;  // connecting to HOST or GATE JAG_DATACENTER_HOST/JAG_DATACENTER_GATE
	int      _isToGate;  // connecting to GATE
	AbaxDataString _primaryHostString;
	abaxint  _qCount;
	FILE *_outf;

	JAGSOCK		_redirectSock;
	JagHashMap<AbaxString, JagTableOrIndexAttrs> *_schemaMap;
	//JagHashStrStr         _defvalMap;
	JagLineFile *_lineFile;


	///////////////////// protected
  protected:
	int checkSpecialCommandValidation( const char *querys, AbaxDataString &newquery, 
		const JagParseParam &parseParam, AbaxDataString &errmsg, AbaxString &hdbobj, const JagTableOrIndexAttrs *objAttr );
	int formatReturnMessageForSpecialCommand( const JagParseParam &parseParam, AbaxDataString &retmsg );
	int getParseInfo( const AbaxDataString &cmd, JagParseParam &parseParam, AbaxDataString &errmsg );
	int checkCmdTableIndexExist( const JagParseParam &parseParam, AbaxString &hdbobj, 
								 const JagTableOrIndexAttrs *&objAttr, AbaxDataString &errmsg );
	int processInsertCommands( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, AbaxDataString &errmsg, 
							   JagVector<AbaxDataString> *filevec=NULL );
	int processInsertCommandsWithNames( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, AbaxDataString &errmsg, 
							   JagVector<AbaxDataString> *filevec=NULL );
	int processInsertCommandsWithoutNames( JagVector<JagDBPair> &cmdhosts, JagParseParam &parseParam, AbaxDataString &errmsg, 
							   JagVector<AbaxDataString> *filevec=NULL );
	int processMultiServerCommands( const char *qstr, JagParseParam &parseParam, AbaxDataString &errmsg );
	int reformOriginalAggQuery( const JagSchemaRecord &aggrec, const JagVector<int> &selectPartsOpcode, 
							const JagVector<int> &selColSetAggParts, const JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
							const char *aggbuf, char *finalbuf, JagParseParam &parseParam );
	int processDataSortArray( const JagParseParam &parseParam, const AbaxDataString &selcnt, const AbaxDataString &selhdr, 
								const AbaxFixString &aggstr );
	int processOrderByQuery( const JagParseParam &parseParam );
	int formatSendQuery( JagParseParam &parseParam, JagParseParam &parseParam2, const JagHashStrInt *maps[], 
		const JagSchemaAttribute *attr[], JagVector<int> &selectPartsOpcode, JagVector<int> &selColSetAggParts, 
		JagHashMap<AbaxInt, AbaxInt> &selColToselParts, int &nqHasLimit, bool &hasAggregate, 
		abaxint &limitNum, int &grouplen, int numCols[], int num, AbaxDataString &newquery, AbaxDataString &errmsg );
	int getUsingHosts( JagVector<AbaxDataString> &hosts, const AbaxFixString &kstr, int isWrite );

	int sendFilesToServer( const JagVector<AbaxDataString> &files );
	int recvFilesFromServer( const JagParseParam *parseParam );

	int checkRegularCommands( const char *querys );
	int getnewClusterString( AbaxDataString &newquery, AbaxDataString &queryerrmsg );
	void setConnectionBrokenTime();
	int checkConnectionRetry();
	int processMaintenanceCommand( const char *querys );
	bool buildConnMap();
	abaxint loadFile( const char *loadCommand );
	int initRow();
	void cleanUpSchemaMap( bool dolock );
	void rebuildHostsConnections( const char *msg, abaxint len );
	abaxint redoLog( const AbaxDataString &fpath );
  	void _init();
	void resetLog();
	void flushInsertBuffer();
	void _printSelectedCols();
	void requestInitMapInfo();
	void appendToLog( const AbaxDataString &querys );
	void checkPoint( const AbaxDataString &infpath );
	bool processSpool( char *querys );
	bool _getKeyOrValue( const char *key, AbaxDataString & strValue, AbaxDataString &type );
	int  setOneCmdProp( int pos );
	int _parseSchema( const char *schema );
	int findAllMetaKeyValueProperty( JagRecord &rrec );
	int findAllNameValueProperty( JagRecord &rrec );
	// int _printRow( FILE *outf, char **retstr, int nth, bool retRow, AbaxDataString &rowStr, int forExport, const char *dbobj );
	int _printRow( FILE *outf, int nth, bool retRow, AbaxDataString &rowStr, int forExport, const char *dbobj );
    char *_getField( const char * rowstr, char fieldToken );
    
    // methods with do at beginning
    int doquery( const char *querys, bool reply=true, bool compress=true, bool batchReply=false, bool dirConn=false, 
				 int setEnd=0, const char *msg="", bool forceConnection=false ); 
    int doreply( bool headerOnly );
    int doPrintRow( bool retRow, AbaxDataString &rowStr );
    int doPrintAll( bool retRow, AbaxDataString &rowStr );
    void doFlush();
    int doHasError();
    int doFreeRow( int type=0 );
	const char *doError();
    const char *doGetMessage();
	const char *doJsonString();
	abaxint doGetMessageLen();
	char doGetMessageType();
    char *doGetValue( const char *name );    
	char *doGetNthValue( int nth );
	int doGetColumnCount();	
	char *doGetCatalogName( int col );
	char *doGetColumnClassName( int col );	
	int doGetColumnDisplaySize( int col );
	char *doGetColumnLabel( int col );
	char *doGetColumnName( int col );	
	int doGetColumnType( int col );
	char *doGetColumnTypeName( int col );		
	int doGetScale( int col ); 
	char *doGetSchemaName( int col );
	char *doGetTableName( int col );
	bool doIsAutoIncrement( int col );
	bool doIsCaseSensitive( int col );
	bool doIsCurrency( int col );
	bool doIsDefinitelyWritable( int col );	
	int doIsNullable( int col );
	bool doIsReadOnly( int col );
	bool doIsSearchable( int col );
	bool doIsSigned( int col );
	char 	*_getValue( const char *name );
	char 	*_getNthValue( int nth );
    
	// static methods
	static void *recoverDeltaLogStatic( void *ptr );
	static void *monitorInserts( void *ptr );
	static void *monitorFlush( void *ptr );
	static void *batchStatic( void * ptr );    
	static void *pingServer( void *ptr );
	static void *broadcastAllRegularStatic( void *ptr );
	static void *broadcastAllRejectFailureStatic1( void *ptr );
	static void *broadcastAllRejectFailureStatic2( void *ptr );
	static void *broadcastAllRejectFailureStatic3( void *ptr );
	static void *broadcastAllRejectFailureStatic4( void *ptr );
	static void *broadcastAllRejectFailureStatic5( void *ptr );
	static void *broadcastAllSelectStatic( void *ptr );
	static void *broadcastAllSelectStatic2( void *ptr );
	static void *broadcastAllSelectStatic3( void *ptr );
	static void *broadcastAllSelectStatic4( void *ptr );
	static void *broadcastAllSelectStatic5( void *ptr );
	static void *broadcastAllSelectStatic6( void *ptr );
	
	// other methods
	int sendTwoBytes( const char *condition );
	int recvTwoBytes( char *condition );

	int getRebalanceString( AbaxDataString &newquery, AbaxDataString &queryerrmsg );
	int doShutDown( const char *querys );

	int checkOrderByValidation( JagParseParam &parseParam, const JagSchemaAttribute *attr[], int numCols[], int num );
	int checkInsertSelectColumnValidation( const JagParseParam &parseParam );
	bool  hasEnoughDiskSpace( abaxint numServers, abaxint totbytes, int &requiredGB, int &availableGB );

	void cleanForNewQuery();
	void formReplyDataFromTempFile( const char *str, abaxint len, char type );
	void setPossibleMetaStr();
	void commit(); // flush insert buffer
	int  doDebugCheck( const char *querys, int len, JagParseParam &parseParam );
	int  doRepairCheck( const char *querys, int len );
	int  doRepairObject( const char *querys, int len );
	int  doKVLen( const char *querys, int len, AbaxDataString &retmsg );
	void getHostKeyStr( const char *kbuf, const JagTableOrIndexAttrs *objAttr, AbaxFixString &hostKeyStr );
	AbaxDataString getCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam, 
								int pos, bool hasX, bool hasY, bool hasZ, bool hasWidth, 
								bool hasDepth=false, bool hasHeight=false ); 
	AbaxDataString getLineCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam, 
								int pos, bool hasX1, bool hasY1, bool hasZ1, 
								         bool hasX2, bool hasY2, bool hasZ2 );
	AbaxDataString getTriangleCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam, 
								int pos, bool hasX1, bool hasY1, bool hasZ1, 
								         bool hasX2, bool hasY2, bool hasZ2,
										 bool hasX3, bool hasY3, bool hasZ3 );

	AbaxDataString get3DPlaneCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam,  int pos,
								       bool hasX,  bool hasY,  bool hasZ, 
								       bool hasW,  bool hasD,  bool hasH,
									   bool hasNX, bool hasNY, bool hasNZ );
    /***
	AbaxDataString getLineStringCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam, 
								       int pos, bool hasX1, bool hasY1, bool hasZ1 );
	AbaxDataString getPolygonCoordStr( const AbaxDataString &shape, const JagParseParam &parseParam, 
								       int pos, bool hasX1, bool hasY1, bool hasZ1 );
	***/

	//bool matchFillOtherCols( const AbaxDataString &colType, JagVector<OtherAttribute>&otherVec, int &i );
	AbaxDataString convertToJson(const char *buf);
	void  getTimebuf( const JagTableOrIndexAttrs *objAttr, char *timebuf, char *timebuf2 );
	void addQueryToCmdhosts( const JagParseParam &parseParam, const AbaxFixString &hostkstr,
	                         const JagTableOrIndexAttrs *objAttr,
		                     JagVector<AbaxDataString> &hosts, JagVector<JagDBPair> &cmdhosts, 
							 AbaxDataString &newquery );

	bool hasDefaultValue( char spare4 );


	// protected data members
	JagCfg *_cfg;
	JagHashMap<AbaxString, abaxint> *_hostIdxMap;
	JagHashMap<AbaxString, abaxint> *_clusterIdxMap;
	JagHashMap<AbaxInt, AbaxBuffer>	*_thrdCliMap;
	JagVector<AbaxDataString> *_insertBuffer;
	JagVector<AbaxDataString> *_insertBufferCopy;
	JagVector<abaxint> *_selectCountBuffer;
	std::atomic<int>   _spCommandErrorCnt;
	std::atomic<abaxint>	_lastConnectionBrokenTime;
	std::atomic<abaxint>	_multiThreadMidPointStart;
	std::atomic<abaxint>	_multiThreadMidPointStop;
	int _end;
	int _serverReplicateMode;
	abaxint _qMutexThreadID;
	//bool _threadend;
	std::atomic<bool> _threadend;
	//bool _hasReply;
	std::atomic<bool> _hasReply;
	bool _fromShell;
	int _forExport;
	AbaxDataString _exportObj;
	abaxint _nthHost;
	abaxint _numHosts;
	abaxint _numClusters;
	abaxint _maxQueryNum;
	ADBROW *_row;
	thpool_ *_insertPool;
	pthread_t _threadmo;
	pthread_t _threadflush;	
	pthread_mutex_t _insertBufferMutex;
	pthread_cond_t  _insertBufferLessThanMaxCond;
	pthread_cond_t  _insertBufferMoreThanMinCond;	
	JagReadWriteLock *_qrlock;
	AbaxDataString _brand;
	AbaxDataString _newdbname;
	AbaxDataString _queryerrmsg;
	AbaxDataString _queryStatMsg;
	JaguarCPPClient *_parentCli;
	short			 _lastOpCode;
	bool			 _lastHasGroupBy;
	bool			 _lastHasOrderBy;
	int _fullConnectionArg;
	int _fullConnectionOK;
	int _fromServ;
	int _connInit;
	int _connMapDone;
	int _useJPB;
	int _makeSingleReconnect;
	JagDataAggregate *_jda;
	AbaxFixString _aggregateData;
	AbaxFixString _pointQueryString;
	AbaxDataString _dataFileHeader;
	AbaxDataString _dataSelectCount;
	AbaxDataString _lastQuery;
	AbaxDataString _randfilepath;
	AbaxDataString _selectTruncateMsg;

	// for host update string and schema update string
	AbaxDataString _hostUpdateString;
	AbaxDataString _schemaUpdateString;
	//AbaxDataString _defvalUpdateString;
	JagMemDiskSortArray *_dataSortArr;
	int _oneConnect;
	// may be removed later
	int _orderByReadFrom;
	abaxint _orderByKEYLEN;
	abaxint _orderByVALLEN;
	bool _orderByIsAsc;
	abaxint _orderByReadPos;
	abaxint _orderByWritePos;
	abaxint _orderByLimit;
	abaxint _orderByLimitCnt;
	abaxint _orderByLimitStart;
	JagArray<JagDBPair> *_orderByMemArr;
	JagSchemaRecord *_orderByRecord;
	JagDiskArrayClient *_orderByDiskArr;
	JagBuffReader *_orderByBuffReader;
	JagBuffBackReader *_orderByBuffBackReader;
	JagReplicateBackup *_jpb;
	int _dtimeout;
	int _connRetryLimit;
	int _cliservSameProcess;
	abaxint _hashJoinLimit;
	int _lastQueryConnError;

	// for testing use only
	// abaxint _testinsertcnt;
	// int   _maintBegin;
	// int	 _exclusiveLogin;
	
  
	JagReadWriteLock *_lock;
	JagReadWriteLock *_loadlock;
	pthread_mutex_t _queryMutex;
	pthread_mutex_t _lineFileMutex;
	
	int _port;
	JAGSOCK _sock;
	int _usedSockNum;
	int _qtype;
	int _numCPU;
	int _faultToleranceCopy;
	bool _isCluster;
	bool _shareSock;
	bool _isparent;
	std::atomic<bool> _destroyed;
	bool _connectOK;
	bool _walLog;
	bool _lastHasSemicolon;
	bool _isExclusive;
	uabaxint _clientFlag;
	FILE *_insertLog;
	AbaxDataString _host;
	AbaxDataString _lefthost;
	AbaxDataString _righthost;
	AbaxDataString _username;
	AbaxDataString _password;
	AbaxDataString _unixSocket;

	JagVector<AbaxDataString> *_allHosts;
	JagVector<JagVector<AbaxDataString> > *_allHostsByCluster;
	AbaxDataString _replyerrmsg;
	AbaxDataString _session;
	AbaxDataString _logFPath;
	AbaxDataString _jsonString;
	JagUUID    *_jagUUID;
	int        _tcode;
	
	CliPass *_passmo;
	CliPass *_passflush;
	char _qbuf[2048];
	char sendhdr[JAG_SOCK_MSG_HDR_LEN];
	char recvhdr[JAG_SOCK_MSG_HDR_LEN+1];	
	int   _debug;
	std::atomic<int>   _isFlushingInsertBuffer;
	int  _compressFlag[4];
	// AbaxDataString _control;
	std::atomic<bool>   _monitorDone;

};


#endif

