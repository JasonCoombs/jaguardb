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
#ifndef _raydb_server_h_
#define _raydb_server_h_

#include <atomic>
#include <abax.h>
#include <JagHashMap.h>
#include <JagThreadPool.h>
#include <JagArray.h>
#include <JagSystem.h>

class JagKeyChecker;
class JagTable;
class JagIndex;
class JagParseParam;
class JagSchema;
class JagSchemaRecord;
class JagTableSchema;
class JagTableValueSchema;
class JagIndexSchema;
class JagCfg;
class JagUserID;
class JagUserRole;
class JagReadWriteLock;
class JagBlockLock;
class JagHashLock;
class JagServerObjectLock;
class JagRequest;
class JagSession;
class JDFSMgr;
class JaguarAPI;
class JagDataAggregate;
class JagUUID;
class JagParseAttribute;
class JagDBConnector;
class JagStrSplitWithQuote;
class JagIPACL;
class JagDBLogger;

template <class Pair> class JagVector;

class JagDBServer
{
  public:

	void objectCountTesting( JagParseParam &parseParam );
  
	JagDBServer();
	~JagDBServer();
	void init( JagCfg *cfg );
	void destroy();
    int main(int argc, char**argv);
    void sig_int(int sig);
    void sig_hup(int sig);

	int processSignal( int signal );
	int getReplicateStatusMode( char *pmesg, int replicateType=-1 );
	int fileTransmit( const Jstr &host, unsigned int port, const Jstr &passwd, 
		const Jstr &unixSocket, int mode, const Jstr &fpath, int isAddCluster );
	int checkDeltaFileStatus();	
	void organizeCompressDir( int mode, Jstr &fpath );
	void crecoverRefreshSchema( int mode );
	void recoveryFromTransferredFile();
	void onlineRecoverDeltaLog();
	void requestSchemaFromDataCenter();
	Jstr getBroadCastRecoverHosts( int replicateCopy );

	
	// "_" server cmds
	void sendMapInfo( const char *pmesg, const JagRequest &req );
	void sendHostInfo( const char *pmesg, const JagRequest &req );
	void checkDeltaFiles( const char *mesg, const JagRequest &req );
	void cleanRecovery( const char *pmesg, const JagRequest &req );
	void recoveryFileReceiver( const char *pmesg, const JagRequest &req );
	void walFileReceiver( const char *pmesg, const JagRequest &req );
	void sendOpInfo( const char *pmesg, const JagRequest &req );
	void doCopyData( const char *pmesg, const JagRequest &req );
	void doRefreshACL( const char *pmesg, const JagRequest &req );
	void doLocalBackup( const char *pmesg, const JagRequest &req );
	void doRemoteBackup( const char *pmesg, const JagRequest &req );
	void doRestoreRemote( const char *pmesg, const JagRequest &req );
	void dbtabInfo( const char *pmesg, const JagRequest &req );
	void sendInfo( const char *pmesg, const JagRequest &req );
	void sendResourceInfo( const char *pmesg, const JagRequest &req );
	void sendClusterOpInfo( const char *pmesg, const JagRequest &req );
	void sendHostsInfo( const char *pmesg, const JagRequest &req );
	void sendRemoteHostsInfo( const char *pmesg, const JagRequest &req );
	void sendLocalStat6( const char *pmesg, const JagRequest &req );
	void sendClusterStat6( const char *pmesg, const JagRequest &req );
	void processLocalBackup( const char *pmesg, const JagRequest &req );
	void processRemoteBackup( const char *pmesg, const JagRequest &req );
	void processRestoreRemote( const char *pmesg, const JagRequest &req );
	void repairCheck( const char *pmesg, const JagRequest &req );
	void addCluster( const char *pmesg, const JagRequest &req );
	void sendSchemaToDataCenter( const char *mesg, const JagRequest &req );
	void unpackSchemaInfo( const char *mesg, const JagRequest &req );
	void askDataFromDC( const char *mesg, const JagRequest &req );
	void prepareDataFromDC( const char *mesg, const JagRequest &req );
	void shutDown( const char *pmesg, const JagRequest &req );

	void joinRequestSend( const char *pmesg, const JagRequest &req );
	void reconnectDataCenter( const Jstr &ip, bool doLock = true );
	bool dbExist( const Jstr &dbName, int replicateType );
	bool objExist( const Jstr &dbname, const Jstr &objName, int replicateType );
	


	// data members
	JagUserID   	*_userDB;
	JagUserID   	*_prevuserDB;
	JagUserID   	*_nextuserDB;
	JagUserRole		*_userRole;
	JagUserRole		*_prevuserRole;
	JagUserRole		*_nextuserRole;
	JagTableSchema *_tableschema;
	JagIndexSchema *_indexschema;
	JagTableSchema *_prevtableschema;
	JagIndexSchema *_previndexschema;
	JagTableSchema *_nexttableschema;
	JagIndexSchema *_nextindexschema;
	// maps
	// JagHashMap<AbaxLong,AbaxLong> *_threadMap;
	JagHashMap<AbaxLong,AbaxString> *_taskMap;
	JagHashMap<AbaxString, AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>>> *_joinMap;
	JagHashMap<AbaxString, abaxint> *_internalHostNum;
	JagHashMap<AbaxString, AbaxInt> *_scMap;
	// atomic
	std::atomic<abaxint>    numSelects;
	std::atomic<abaxint>    numInserts;
	std::atomic<abaxint>    numUpdates;
	std::atomic<abaxint>    numDeletes;
	std::atomic<abaxint>    numInInsertBuffers;
	std::atomic<int>	 _exclusiveAdmins;
	std::atomic<int>     _doingRemoteBackup;
	std::atomic<int>     _doingRestoreRemote;
	std::atomic<int>	 _shutDownInProgress;
	std::atomic<int>	 _restartRecover;
	std::atomic<int>	 _addClusterFlag;
	std::atomic<int>	 _newdcTrasmittingFin;
	// int, bool, abaxint, datastring, etc.
	int			_nthServer;
	int			_numPrimaryServers;
	int 		_hashRebalanceFD;
	int			_port;
	int			_flushWait;
	// int         _insertSyncPeriod;
	int         _dumfd;
	int         _beginAddServer;
	int			_faultToleranceCopy;
	int			_isSSD;
	int			_memoryMode;
	int			servtimediff;
	abaxint		_connections;
	abaxint		_sea_records_limit;
	abaxint		_jdbMonitorTimedoutPeriod;
	abaxint		_hashRebalanceLen;
	uabaxint		_xferInsert;
	Jstr		_version;
	Jstr		_localInternalIP;
	Jstr	    _listenIP;
	Jstr      _perfFile;
	Jstr      _servToken;
	static  Jstr	_allLockToken;
	// other members
	JagCfg				*_cfg;
	JDFSMgr				*jdfsMgr;
	JagUUID				*_jagUUID;
	JagDBConnector		*_dbConnector;
	JaguarThreadPool    *_parsePool;
	JaguarThreadPool    *_selectPool;

	// locks
	JagServerObjectLock *_objectLock;
	JagHashLock    *_createIndexLock;
	JagHashLock    *_tableUsingLock;
	JagSystem       _jagSystem;
   	static pthread_mutex_t  g_dbmutex;
   	static pthread_mutex_t  g_flagmutex;
   	static pthread_mutex_t  g_logmutex;
   	static pthread_mutex_t  g_dlogmutex;
   	static pthread_mutex_t  g_dinsertmutex;
   	static pthread_mutex_t  g_datacentermutex;
   	static pthread_mutex_t  g_selectmutex;
	int             _isGate;
	int             _isProxy;
	abaxint   _curClusterNumber;
	abaxint   _totalClusterNumber;
	int	      _numCPUs;

  protected:
  
	// static methods		
	static int checkNonStandardCommandValidation( const char *mesg );
	static int checkSimpleCommandValidation( const char *mesg );
	static int createTable( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, JagParseParam *parseParam, 
		Jstr &reterr, abaxint threadQueryTime );
    static int createMemTable( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, JagParseParam *parseParam, 
		Jstr &reterr, abaxint threadQueryTime );
	static int createIndex( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, JagParseParam *parseParam, 
		JagTable *&ptab, JagIndex *&pindex, Jstr &reterr, abaxint threadQueryTime );
    static int createIndexSchema( const JagRequest &req, JagDBServer *servobj, JagTable* ptab, const Jstr &dbname, 
		JagParseParam *parseParam, Jstr &reterr );
	static int dropTable( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, 
		JagParseParam *parseParam, Jstr &reterr, abaxint threadQueryTime );
	static int dropIndex( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, 
		JagParseParam *parseParam, Jstr &reterr, abaxint threadQueryTime );
	static int truncateTable( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, 
		JagParseParam *parseParam, Jstr &reterr, abaxint threadQueryTime );
    static int renameColumn( JagRequest &req, JagDBServer *servobj, const Jstr &dbname, 
		const JagParseParam *parseParam, Jstr &reterr, abaxint threadQueryTime, abaxint &thrdSchemaTime );
	static int importTable( JagRequest &req, JagDBServer *servobj, const Jstr &dbname,
		JagParseParam *parseParam, Jstr &reterr );
	static int doInsert( JagDBServer *servobj, JagRequest &req, JagParseParam &parseParam, Jstr &reterr, const Jstr &oricmd );
	static int processMultiCmd( JagRequest &req, const char *mesg, abaxint msglen, JagDBServer *servobj, 
		abaxint &threadSchemaTime, abaxint &threadHostTime, abaxint threadQueryTime, bool redoOnly, int isReadOrWriteCommand );
	static int joinObjects( const JagRequest &req, JagDBServer *servobj, JagParseParam *parseParam, Jstr &reterr );
	static abaxint processCmd( JagRequest &req, JagDBServer *servobj, const char *cmd, 
		JagParseParam &parseParam, Jstr &rr, abaxint threadQueryTime, abaxint &threadSchemaTime );
	static bool doAuth ( const JagRequest &req, JagDBServer *servobj, char *pmesg, char *&saveptr, 
		JagSession &session );
	static bool useDB( const JagRequest &req, JagDBServer *servobj, char *pmesg, char *&saveptr );
    static void helpPrintTopic( const JagRequest &req );
    static void helpTopic( const JagRequest &req, const char *topic );
    static void showDatabases(JagCfg *cfg, const JagRequest &req );
    static void showCurrentUser(JagCfg *cfg, const JagRequest &req );
    static void showCurrentDatabase(JagCfg *cfg, const JagRequest &req, const Jstr &dbname );
    static void _showDatabases(JagCfg *cfg, const JagRequest &req );
    static void showTables( const JagRequest &req, const JagParseParam &pparm, const JagTableSchema *tableschema, 
						    const Jstr &dbname, int objtype );
    static void _showTables( const JagRequest &req, const JagTableSchema *tableschema, const Jstr &dbname );
    static void showIndexes( const JagRequest &req, const JagParseParam &pparm, const JagIndexSchema *indexschema, 
								const Jstr &dbtable );
    static void showAllIndexes( const JagRequest &req, const JagParseParam &pparm, const JagIndexSchema *indexschema, 
								const Jstr &dbname );
    static void _showIndexes( const JagRequest &req, const JagIndexSchema *indexschema, const Jstr &dbtable );
    static int  describeTable( int obType, const JagRequest &req, const JagDBServer *servobj, JagTable *ptab, 
								const JagTableSchema *tableschema, 
		const Jstr &dbtable, const JagParseParam &parseParam, bool showCreate=false );
    static void _describeTable( const JagRequest &req, JagTable *ptab, const JagDBServer *servobj, 
								const Jstr &dbtable, int keyOnly );
    static void sendNameValueData( const JagRequest &req, const Jstr &name, const Jstr &value );
    static void sendValueData( const JagParseParam &parseParam, const JagRequest &req );
    static void describeIndex( const JagParseParam &parseParam, const JagRequest &req, const JagDBServer *servobj, 
								const JagIndexSchema *indexschema, 
								const Jstr &dbname, const Jstr &indexName, Jstr &reterr );
	static void refreshSchemaInfo( const JagRequest &req, JagDBServer *servobj, abaxint &schtime );
	static void sendUpDown( const JagRequest &req, JagDBServer* servobj, const Jstr &dbtab );
	static void dropAllTablesAndIndexUnderDatabase( const JagRequest &req, JagDBServer* servobj,
												    JagTableSchema *schema, const Jstr &dbname );
	static void dropAllTablesAndIndex( const JagRequest &req, JagDBServer* servobj,
										JagTableSchema *schema );
	static void addTask(  uabaxint taskID, JagSession *session, const char *mesg );
	static void removeTask( JagDBServer *servobj );
	static void completeTask( JagDBServer *servobj );
	static void showTask( const JagRequest &req, JagDBServer *servobj );
	static void noLinger( const JagRequest &req );
	static void showClusterStatus( const JagRequest &req, JagDBServer *servobj);
	static void showDatacenter( const JagRequest &req, JagDBServer *servobj);
	static void showTools( const JagRequest &req, JagDBServer *servobj);
	static void logCommand( JagSession *session, const char *mesg, abaxint len, int spMode );
	static void dinsertlogCommand( JagSession *session, const char *mesg, abaxint len );
	void deltalogCommand( int mode, JagSession *session, const char *mesg, bool isBatch );
	static void regSplogCommand( JagSession *session, const char *mesg, abaxint len, int spMode );
	static void doCreateIndex( JagTable *ptab, JagIndex *pindex, JagDBServer *servobj );	
	static void changeDB( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void createDB( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void dropDB( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void createUser( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void dropUser( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void changePass( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam, abaxint threadQueryTime );
	static void showUsers( const JagRequest &req, JagDBServer *servobj );
	static void	*threadGroupTask( void *servptr );
   	static void	*oneClientThreadTask( void *passptr );
   	static void	*makeTableObjects( void *servptr );
   	static void	*monitorCommandsMem( void *sessionptr );
   	static void	*monitorLog( void *sessionptr );
   	static void	*monitorRemoteBackup( void *sessionptr );
   	static void	*threadRemoteBackup( void *sessionptr );
   	static void	*threadRestoreRemote( void *sessionptr );
	static void *joinSortStatic( void *ptr );
	static void *joinSortStatic2( void *ptr );
	static void *joinHashStatic( void *ptr );
	static void *joinRequestStatic( void * ptr );
	static void *copyDataToNewDCStatic( void * ptr );
	static Jstr getLocalHostIP( JagDBServer *servobj, const Jstr &allhosts );
	static Jstr getClusterOpInfo( const JagRequest &req, JagDBServer *servobj);
	static Jstr srcDestType( int isGate, const Jstr &destType );
	void   getDestHostPortType( const Jstr &inLine, Jstr& host, Jstr& port, 
								Jstr& destType );
	void printResources();
	static void grantPerm( JagRequest &req, JagDBServer *servobj, const JagParseParam &parseParam, abaxint threadQueryTime );
	static void revokePerm( JagRequest &req, JagDBServer *servobj, const JagParseParam &parsParam, abaxint thrdQueryTime );
	static void showPerm( JagRequest &req, JagDBServer *servobj, const JagParseParam &parseParam, abaxint thrdQueryTime );
	static bool checkUserCommandPermission( JagDBServer *servobj, const JagSchemaRecord *srec, const JagRequest &req, 
											const JagParseParam &parseParam, int i, Jstr &rowFilter, 
											Jstr &reterr );
	static void noGood( JagRequest &req, JagDBServer *servobj, JagParseParam &parseParam );


	// object methods
	int mainInit(int argc, char**argv);
	int mainClose();
	int initObjects();
	int createSocket( int argc, char *argv[] );
	int initConfigs(); 
	int makeThreadGroups( int grps, int grpseq );
	int eventLoop( const char *port );
	int copyLocalData( const Jstr &dirname, const Jstr &policy, const Jstr &tmstr, bool show );
	int schemaChangeCommandSyncCheck( const JagRequest &req, const Jstr &dbobj, abaxint opcode, int isRemove );
	int schemaChangeCommandSyncRemove( const Jstr &dbobj );
	
	bool isInteralIP( const Jstr &ip );
	bool existInAllHosts( const Jstr &ip );	
	void initDirs();
	void checkLicense();
	void createAdmin();
	void makeAdmin( JagUserID *uiddb );
	void doBackup( uabaxint  seq );
	void refreshACL( int bcast );
	void loadACL( );
	void checkPoint( uabaxint  seq);
	void copyData( const Jstr &dirname, bool show=true );
	void writeLoad( uabaxint  seq );
	void numDBTables( int &databases, int &tables );

	void resetWalLog();
	void rotateWalLog();
	void recoverWalLog( const Jstr &fpath );
	void resetDinsertLog();
	void rotateDinsertLog();
	void recoverDinsertLog( const Jstr &fpath );
	void resetDeltaLog();
	void recoverDeltaLog( JagSession *session, bool force=false );
	void resetRegSpLog();
	void recoverRegSpLog();
	void syncLogs();
	void flushAllBlockIndexToDisk();
	void removeAllBlockIndexInDisk();
	void removeAllBlockIndexInDiskAll( const JagTableSchema *tableschema, const char *datapath );
	void flushOneTableAndRelatedIndexsInsertBuffer( const Jstr &dbobj, int replicateType, int isTable, 
		JagTable *iptab, JagIndex *ipindex );
	void flushAllTableAndRelatedIndexsInsertBuffer();
	abaxint redoDinsertLog( const Jstr &fpath );
	abaxint redoLog( const Jstr &fpath );
	Jstr getTaskIDsByThreadID( abaxint threadID );
	JagTableSchema *getTableSchema( int replicateType ) const;
	void getTableIndexSchema( int replicateType, JagTableSchema *& tableschema, JagIndexSchema *&indexschema );
	void openDataCenterConnection();
	int synchToOtherDataCenters(const char *mesg, bool &sucsync, const JagRequest &req);
	int synchFromOtherDataCenters( const JagRequest &req, const char *mesg, const JagParseParam &pparam );
	void closeDataCenterConnection();
	int countOtherDataCenters();
	void refreshDataCenterConnections( abaxint seq );
	void checkAndCreateThreadGroups();
	long getDataCenterQueryCount( const Jstr &ip, bool doLock );
	void refreshUserDB( abaxint seq );
	void refreshUserRole( abaxint seq );
	Jstr  fillDescBuf( const JagSchema *schema, const JagColumn &column, const Jstr &dbobj ) const;
	int processSelectConstData( const JagRequest &req, const JagParseParam *parseParam );



	// data members
	int	_threadGroups;
	int	_threadGroupSize;
	int  _initExtraThreads;
	abaxint _threadGroupNum;
	std::atomic<abaxint> _activeThreadGroups;
	std::atomic<abaxint> _activeClients;
	abaxint  _threadGroupSeq;
	int   _groupSeq;

	JAGSOCK	_sock;
	int	_walLog;
	bool _clusterMode;
	bool _cacheCommand;
	uabaxint _taskID;
	abaxint	_msyncCount;
    static int g_receivedSignal;
	static int g_dtimeout;
	static abaxint g_lastSchemaTime;
	static abaxint g_lastHostTime;
	static char *_logFpath;
	bool  _serverReady;
	
	// data member for testing only
	static abaxint g_icnt1;
	static abaxint g_icnt2;
	static abaxint g_icnt3;
	static abaxint g_icnt4;
	static abaxint g_icnt5;
	static abaxint g_icnt6;
	static abaxint g_icnt7;
	static abaxint g_icnt8;
	static abaxint g_icnt9;
	
	// wallog
	FILE *_fpCommand;
	Jstr _activeWalFpath;

	// dinsertlog
	FILE *_dinsertCommand;
	Jstr _activeDinFpath;
	
	// replicate delta files
	// e.g. serv 0,1,2,3,4
	FILE *_delPrevOriCommand;    // 1 original on 2 for original delta
	FILE *_delPrevRepCommand;	// 2 origianl to 1 on 2 for replicate delta
	FILE *_delPrevOriRepCommand; // 1 original to 0 on 2 for replicate delta
	FILE *_delNextOriCommand; // 3 original on 2 for original delta
	FILE *_delNextRepCommand; // 2 original to 3 on 2 for replicate delta
	FILE *_delNextOriRepCommand; // 3 original to 4 on 2 for replicate delta
	Jstr _actdelPOpath;
	Jstr _actdelPRpath;
	Jstr _actdelPORpath;
	Jstr _actdelNOpath;
	Jstr _actdelNRpath;
	Jstr _actdelNORpath;
	Jstr _actdelPOhost;
	Jstr _actdelPRhost;
	Jstr _actdelPORhost;
	Jstr _actdelNOhost;
	Jstr _actdelNRhost;
	Jstr _actdelNORhost;

	// recovery commands log
	FILE *_recoveryRegCommand;
	FILE *_recoverySpCommand; 
	Jstr _recoveryRegCmdPath;
	Jstr _recoverySpCmdPath;
	Jstr _crecoverFpath;
	Jstr _prevcrecoverFpath;
	Jstr _nextcrecoverFpath;	

	Jstr _walrecoverFpath;
	Jstr _prevwalrecoverFpath;
	Jstr _nextwalrecoverFpath;	

	JagIPACL       *_blackIPList;
	JagIPACL       *_whiteIPList;
	JagDBLogger	   *_dbLogger;
	pthread_rwlock_t       _aclrwlock;

	Jstr _publicKey;
	Jstr _privateKey;
	JaguarAPI      *_dataCenter[JAG_DATACENTER_MAX];
	int			    _numDataCenter;
	Jstr  _centerHostPort[JAG_DATACENTER_MAX];
	bool            _debugClient;

};
#endif
