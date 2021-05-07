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
#include <JagArray.h>
#include <JagSystem.h>
#include <JagMutexMap.h>
#include <JagWalMap.h>
#include <JagHashIntInt.h>

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
class JagBlockLock;
class JagHashLock;
class JagServerObjectLock;
class JagRequest;
class JagSession;
class JagFSMgr;
class JaguarAPI;
class JagDataAggregate;
class JagUUID;
class JagParseAttribute;
class JagDBConnector;
class JagStrSplitWithQuote;
class JagIPACL;
class JagDBLogger;
class JagMergeReader;
class JagFamilyKeyChecker;
class JagDBMap;

template <class Pair> class JagVector;

class JagDBServer
{
  public:

	JagDBServer();
	~JagDBServer();
	void init( JagCfg *cfg );
	void destroy();
    int  main(int argc, char**argv);
    void sig_int(int sig);
    void sig_hup(int sig);

	int processSignal( int signal );
	int getReplicateStatusMode( char *pmesg, int replicateType=-1 );
	int fileTransmit( const Jstr &host, unsigned int port, const Jstr &passwd, 
					  const Jstr &unixSocket, int mode, const Jstr &fpath, int isAddCluster );
	int checkDeltaFileStatus();	
	void organizeCompressDir( int mode, Jstr &fpath );
	void crecoverRefreshSchema( int mode, bool restoreInsertBuffer = false );
	void recoveryFromTransferredFile();
	void onlineRecoverDeltaLog();
	void requestSchemaFromDataCenter();
	Jstr getBroadcastRecoverHosts( int replicateCopy );

	
	// "_xxx" server cmds
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
	void trimTimeSeries();
	void repairCheck( const char *pmesg, const JagRequest &req );
	void addCluster( const char *pmesg, const JagRequest &req );
	void addClusterMigrate( const char *pmesg, const JagRequest &req );
	void addClusterMigrateContinue( const char *pmesg, const JagRequest &req );
	void addClusterMigrateComplete( const char *pmesg, const JagRequest &req );
	void importTableDirect( const char *pmesg, const JagRequest &req );
	void truncateTableDirect( const char *pmesg, const JagRequest &req );
	void sendSchemaToDataCenter( const char *mesg, const JagRequest &req );
	void unpackSchemaInfo( const char *mesg, const JagRequest &req );
	void askDataFromDC( const char *mesg, const JagRequest &req );
	void prepareDataFromDC( const char *mesg, const JagRequest &req );
	void shutDown( const char *pmesg, const JagRequest &req );
	static void makeNeededDirectories();

	void joinRequestSend( const char *pmesg, const JagRequest &req );
	void reconnectDataCenter( const Jstr &ip, bool doLock = true );
	bool dbExist( const Jstr &dbName, int replicateType );
	bool objExist( const Jstr &dbname, const Jstr &objName, int replicateType );

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
	JagHashMap<AbaxLong,AbaxString> *_taskMap;
	JagHashMap<AbaxString, AbaxPair<AbaxPair<AbaxLong, AbaxPair<AbaxLong, AbaxLong>>, AbaxPair<JagDBPair, AbaxPair<AbaxBuffer, AbaxPair<AbaxBuffer, AbaxBuffer>>>>> *_joinMap;
	JagHashMap<AbaxString, jagint> *_internalHostNum;
	JagHashMap<AbaxString, AbaxInt> *_scMap;
	std::atomic<jagint>    numSelects;
	std::atomic<jagint>    numInserts;
	std::atomic<jagint>    numUpdates;
	std::atomic<jagint>    numDeletes;
	std::atomic<jagint>    numInInsertBuffers;
	std::atomic<int>	 _exclusiveAdmins;
	std::atomic<int>     _doingRemoteBackup;
	std::atomic<int>     _doingRestoreRemote;
	std::atomic<int>	 _shutDownInProgress;
	std::atomic<int>	 _restartRecover;
	std::atomic<int>	 _addClusterFlag;
	std::atomic<int>	 _newdcTrasmittingFin;
	int			_nthServer;
	int			_numPrimaryServers;
	int 		_hashRebalanceFD;
	int			_port;
	int			_flushWait;
	int         _dumfd;
	int         _beginAddServer;
	int			_faultToleranceCopy;
	int			_isSSD;
	int			_memoryMode;
	int			servtimediff; // minutes
	jagint		_connections;
	jagint		_jdbMonitorTimedoutPeriod;
	jagint		_hashRebalanceLen;
	jaguint		_xferInsert;
	Jstr		_version;
	Jstr		_localInternalIP;
	Jstr	    _listenIP;
	Jstr      	_perfFile;
	Jstr      	_servToken;
	JagCfg				*_cfg;
	JagFSMgr			*jdfsMgr;
	JagUUID				*_jagUUID;
	JagDBConnector		*_dbConnector;

	// locks
	JagServerObjectLock *_objectLock;
	JagSystem       _jagSystem;
   	static pthread_mutex_t  g_dbschemamutex;

   	static pthread_mutex_t  g_flagmutex;
   	static pthread_mutex_t  g_wallogmutex;
   	static pthread_mutex_t  g_dlogmutex;
   	static pthread_mutex_t  g_dinsertmutex;
   	static pthread_mutex_t  g_datacentermutex;
   	pthread_mutex_t  		g_dbconnectormutex;

	int         _isGate;
	int         _isProxy;
	jagint   	_curClusterNumber;
	jagint   	_totalClusterNumber;
	int	      	_numCPUs;
	int			_scaleMode;

	JagMutexMap   _walMutexMap;
	JagWalMap     _walLogMap;

  protected:
	static int isValidInternalCommand( const char *mesg );
	void processInternalCommands( int op, const JagRequest &req, const char *pmesg ); 
	static int isSimpleCommand( const char *mesg );
	int createTable( JagRequest &req, const Jstr &dbname, JagParseParam *parseParam, 
					Jstr &reterr, jagint threadQueryTime );
	int createSimpleTable( const JagRequest &req, const Jstr &dbname, const JagParseParam *parseParam );
	int dropSimpleTable( const JagRequest &req, const JagParseParam *parseParam, Jstr &err, bool lockSchema );
    int createMemTable( JagRequest &req, const Jstr &dbname, JagParseParam *parseParam, 
						Jstr &reterr, jagint threadQueryTime );
	int createIndex( JagRequest &req, const Jstr &dbname, JagParseParam *parseParam, 
					JagTable *&ptab, JagIndex *&pindex, Jstr &reterr, jagint threadQueryTime );
    int createIndexSchema( const JagRequest &req, const Jstr &dbname, JagParseParam *parseParam, Jstr &reterr, bool lockSch );
	int dropTable( JagRequest &req, JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, Jstr &timeSeries);
	int dropIndex( JagRequest &req, const Jstr &dbname, 
				   JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, Jstr &tser );
	int truncateTable( JagRequest &req, JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime );
    int alterTable( const JagParseAttribute &jpa, JagRequest &req, const Jstr &dbname, 
				    const JagParseParam *parseParam, Jstr &reterr, jagint threadQueryTime, jagint &thrdSchemaTime );
	int importTable( JagRequest &req, const Jstr &dbname, JagParseParam *parseParam, Jstr &reterr );
	int joinObjects( const JagRequest &req, JagParseParam *parseParam, Jstr &reterr );
	jagint processCmd( const JagParseAttribute &jpa, JagRequest &req,  const char *cmd, JagParseParam &parseParam, 
					  Jstr &rr, jagint threadQueryTime, jagint &threadSchemaTime );
	bool doAuth( JagRequest &req, char *pmesg );
	bool useDB( JagRequest &req, char *pmesg );
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
    Jstr  describeTable( int obType, const JagRequest &req, const JagTableSchema *tableschema, 
					     const Jstr &dbtable, bool showDetail, bool showCreate, bool forRollup, const Jstr &retainTser );
    void _describeTable( const JagRequest &req, const Jstr &dbtable, int keyOnly );
    static void sendNameValueData( const JagRequest &req, const Jstr &name, const Jstr &value );
    static void sendValueData( const JagParseParam &parseParam, const JagRequest &req );
    Jstr describeIndex( bool showDetail, const JagRequest &req, const JagIndexSchema *indexschema, 
						 const Jstr &dbname, const Jstr &indexName, Jstr &reterr, bool showCreate, bool forRollup, const Jstr &retainTser );
	void refreshSchemaInfo( int replicateType, jagint &schtime );
	void sendUpDown( const JagRequest &req, const Jstr &dbtab );
	void dropAllTablesAndIndexUnderDatabase( const JagRequest &req, JagTableSchema *schema, const Jstr &dbname );
	void dropAllTablesAndIndex( const JagRequest &req, JagTableSchema *schema );
	static void addTask(  jaguint taskID, JagSession *session, const char *mesg );
	void removeTask( );
	void completeTask( );
	void showTask( const JagRequest &req );
	static void noLinger( const JagRequest &req );
	void showClusterStatus( const JagRequest &req );
	void showDatacenter( const JagRequest &req );
	void showTools( const JagRequest &req );
	void logCommand( const JagParseParam *ppram, JagSession *session, const char *mesg, jagint len, int spMode );
	static void dinsertlogCommand( JagSession *session, const char *mesg, jagint len );
	void deltalogCommand( int mode, JagSession *session, const char *mesg, bool isBatch );
	static void regSplogCommand( JagSession *session, const char *mesg, jagint len, int spMode );
	void doCreateIndex( JagTable *ptab, JagIndex *pindex );	
	void changeDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void createDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void dropDB( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void createUser( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void dropUser( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void changePass( JagRequest &req, JagParseParam &parseParam, jagint threadQueryTime );
	void showUsers( const JagRequest &req );
	static void	*oneThreadGroupTask( void *servptr );
   	static void	*oneClientThreadTask( void *passptr );
   	void	makeTableObjects( bool restoreInsertBuffer = false );
   	static void	*monitorCommandsMem( void *sessionptr );
   	static void	*monitorLog( void *sessionptr );
   	static void	*monitorRemoteBackup( void *sessionptr );
   	static void	*monitorTimeSeries( void *sessionptr );
   	static void	*threadRemoteBackup( void *sessionptr );
   	static void	*threadRestoreRemote( void *sessionptr );
	static void *joinRequestStatic( void * ptr );
	static void *doMergeJoinStatic( void *ptr );
	static void *joinSortStatic2( void *ptr );
	static void *joinHashMapData( void *ptr );
	static void hashJoinStaticGetMap( ParallelJoinPass *pass );
	static void mergeJoinGetData( ParallelJoinPass *pass );
	static void *copyDataToNewDCStatic( void * ptr );
	Jstr getLocalHostIP( const Jstr &allhosts );
	Jstr getClusterOpInfo( const JagRequest &req );
	static Jstr srcDestType( int isGate, const Jstr &destType );
	void   getDestHostPortType( const Jstr &inLine, Jstr& host, Jstr& port, Jstr& destType );
	void printResources();
	void grantPerm( JagRequest &req, const JagParseParam &parseParam, jagint threadQueryTime );
	void revokePerm( JagRequest &req, const JagParseParam &parsParam, jagint thrdQueryTime );
	void showPerm( JagRequest &req, const JagParseParam &parseParam, jagint thrdQueryTime );
	bool checkUserCommandPermission( const JagSchemaRecord *srec, const JagRequest &req, 
											const JagParseParam &parseParam, int i, Jstr &rowFilter, 
											Jstr &reterr );
	void noGood( JagRequest &req, JagParseParam &parseParam );
	int processSimpleCommand( int simplerc, JagRequest &req, char *pmesg, int &authed );
	static int advanceLeftTable( JagMergeReader *jntr, const JagHashStrInt **maps, char *jbuf[], int numKeys[], jagint sklen[],
								 const JagSchemaAttribute *attrs[], JagParseParam *pram, const char *buffers[] );
	static int advanceRightTable( JagMergeReader *jntr, const JagHashStrInt **maps, char *jbuf[], int numKeys[], jagint sklen[],
								const JagSchemaAttribute *attrs[], JagParseParam *pram, const char *buffers[] );

	static void rePositionMessage( JagRequest &req, char *&pmesg, jagint &len );


	int mainInit();
	int mainClose();
	int initObjects();
	int createSocket( int argc, char *argv[] );
	int initConfigs(); 
	int makeThreadGroups( int grps, int grpseq );
	int eventLoop( const char *port );
	int copyLocalData( const Jstr &dirname, const Jstr &policy, const Jstr &tmstr, bool show );
	int schemaChangeCommandSyncCheck( const JagRequest &req, const Jstr &dbobj, jagint opcode, int isRemove );
	int schemaChangeCommandSyncRemove( const Jstr &dbobj );
	
	bool isInteralIP( const Jstr &ip );
	bool existInAllHosts( const Jstr &ip );	
	void initDirs();
	void checkLicense();
	void createAdmin();
	void makeAdmin( JagUserID *uiddb );
	void doBackup( jaguint  seq );
	void refreshACL( int bcast );
	void loadACL( );
	void checkPoint( jaguint  seq);
	void copyData( const Jstr &dirname, bool show=true );
	void writeLoad( jaguint  seq );
	void numDBTables( int &databases, int &tables );

	void resetWalLog();
	void recoverWalLog( );
	void resetDinsertLog();
	void rotateDinsertLog();
	void recoverDinsertLog( const Jstr &fpath );
	void resetDeltaLog();
	void recoverDeltaLog( JagSession *session, bool force=false );
	void resetRegSpLog();
	void recoverRegSpLog();
	void flushAllBlockIndexToDisk();
	void removeAllBlockIndexInDisk();
	void removeAllBlockIndexInDiskAll( const JagTableSchema *tableschema, const char *datapath );
	void flushOneTableAndRelatedIndexsInsertBuffer( const Jstr &dbobj, int replicateType, int isTable, 
													JagTable *iptab, JagIndex *ipindex );
	void flushAllTableAndRelatedIndexsInsertBuffer();
	jagint redoDinsertLog( const Jstr &fpath );
	jagint redoWalLog( const Jstr &fpath );
	jagint redoWalLog( FILE *fp, bool isTopLevel );
	Jstr 	getTaskIDsByThreadID( jagint threadID );
	JagTableSchema *getTableSchema( int replicateType ) const;
	void 	getTableIndexSchema( int replicateType, JagTableSchema *& tableschema, JagIndexSchema *&indexschema );
	void 	openDataCenterConnection();
	int 	synchToOtherDataCenters(const char *mesg, bool &sucsync, const JagRequest &req);
	int 	synchFromOtherDataCenters( const JagRequest &req, const char *mesg, const JagParseParam &pparam );
	void 	closeDataCenterConnection();
	int 	countOtherDataCenters();
	void 	refreshDataCenterConnections( jagint seq );
	void 	checkAndCreateThreadGroups();
	long 	getDataCenterQueryCount( const Jstr &ip, bool doLock );
	void 	refreshUserDB( jagint seq );
	void 	refreshUserRole( jagint seq );
	Jstr  	fillDescBuf( const JagSchema *schema, const JagColumn &column, const Jstr &dbobj, bool doCvt, bool &doneCvt ) const;
	int 	processSelectConstData( const JagRequest &req, const JagParseParam *parseParam );
	Jstr 	columnProperty(const char *ctype, int srid, int metrics ) const;
	void 	logBatchInsertCommand( const JagRequest &req, const JagParseParam* pparam, const Jstr &insertMsg );
	int 	handleRestartRecover( const JagRequest &req, const char *pmesg, jagint len,
							  	char *hdr2, char *&newbuf, char *&newbuf2 );

	int     broadcastSchemaToClients();
	int     broadcastHostsToClients();

	int processMultiSingleCmd( JagRequest &req, const char *mesg, jagint msglen, 
							  jagint &threadSchemaTime, jagint &threadHostTime, 
							  jagint threadQueryTime, bool redoOnly, int isReadOrWriteCommand );
	int  doInsert( JagRequest &req, JagParseParam &parseParam, Jstr &reterr, const Jstr &oricmd );
	void insertToTimeSeries( const JagSchemaRecord &schrec, const JagRequest &req, JagParseParam &pParam, const Jstr &tser, 
							 const Jstr &dbName, const Jstr &tableName,
	                         const JagTableSchema *tableschema, int replicateType, const Jstr &oricmd );
	int createTimeSeriesTables( const JagRequest &req, const Jstr &timeSeries, const Jstr &dbname, const Jstr &dbtable,
	                             const JagParseAttribute &jpa, Jstr &reterr );
	void dropTimeSeriesTables( const JagRequest &req, const Jstr &timeSeries, const Jstr &dbname, const Jstr &dbtable,
	                             const JagParseAttribute &jpa, Jstr &reterr );

	void createTimeSeriesIndexes( const JagParseAttribute &jpa, const JagRequest &req,
	                              const JagParseParam &parseParam, const Jstr &timeSeries, Jstr &reterr );

	int createSimpleIndex( const JagRequest &req, JagParseParam *parseParam,
	                       JagTable *&ptab, JagIndex *&pindex, Jstr &reterr );

	int dropSimpleIndex( const JagRequest &req, const JagParseParam *parseParam, Jstr &reterr, bool lockSchema );
	void dropTimeSeriesIndexes( const JagRequest &req, const JagParseAttribute &jpa,
	                            const Jstr &parentTabName, const Jstr &parentIndexName, const Jstr &timeSeries );

	jagint trimWalLogFile( const JagTable *ptab, const Jstr &db, const Jstr &tab,
	                       const JagDBMap *insertBufferMap, const JagFamilyKeyChecker *keyChecker );
    jagint doTrimWalLogFile( const JagTable *ptab, const Jstr &fpath, const Jstr &dbname, const JagDBMap *insertBufferMap, 
							 const JagFamilyKeyChecker *keyChecker );


	// data members
	int		_threadGroups;
	int		_threadGroupSize;
	int  	_initExtraThreads;
	jagint 	_threadGroupNum;
	std::atomic<jagint> _activeThreadGroups;
	std::atomic<jagint> _activeClients;
	jagint  _threadGroupSeq;
	int   	_groupSeq;

	JAGSOCK	_sock;
	int		_walLog;
	bool 	_clusterMode;
	bool 	_cacheCommand;
	jaguint _taskID;
    static int g_receivedSignal;
	//static int g_dtimeout;
	static jagint g_lastSchemaTime;
	static jagint g_lastHostTime;
	static char *_logFpath;
	bool  _serverReady;
	
	// dinsertlog
	FILE 	*_dinsertCommandFile;
	Jstr 	_activeDinFpath;
	
	// replicate delta files
	// e.g. serv 0,1,2,3,4
	FILE *_delPrevOriCommandFile;    // 1 original on 2 for original delta
	FILE *_delPrevRepCommandFile;	// 2 origianl to 1 on 2 for replicate delta
	FILE *_delPrevOriRepCommandFile; // 1 original to 0 on 2 for replicate delta

	FILE *_delNextOriCommandFile; // 3 original on 2 for original delta
	FILE *_delNextRepCommandFile; // 2 original to 3 on 2 for replicate delta
	FILE *_delNextOriRepCommandFile; // 3 original to 4 on 2 for replicate delta

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
	FILE *_recoveryRegCommandFile;
	FILE *_recoverySpCommandFile; 
	Jstr _recoveryRegCmdPath;
	Jstr _recoverySpCmdPath;
	Jstr _crecoverFpath;
	Jstr _prevcrecoverFpath;
	Jstr _nextcrecoverFpath;	

	Jstr _walrecoverFpath;
	Jstr _prevwalrecoverFpath;
	Jstr _nextwalrecoverFpath;	
	Jstr _migrateOldHosts;

	JagIPACL       		*_blockIPList;
	JagIPACL       		*_allowIPList;
	JagDBLogger	   		*_dbLogger;
	pthread_rwlock_t    _aclrwlock;

	Jstr 			_publicKey;
	Jstr 			_privateKey;
	JaguarAPI      *_dataCenter[JAG_DATACENTER_MAX];
	int			    _numDataCenter;
	Jstr  			_centerHostPort[JAG_DATACENTER_MAX];
	bool            _debugClient;
	bool            _isDirectorNode;

	JagHashIntInt   _clientSocketMap;

};
#endif
