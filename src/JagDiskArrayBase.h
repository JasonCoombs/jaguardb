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
#ifndef _jag_disk_arjag_base_h_
#define _jag_disk_arjag_base_h_

#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <atomic>

#include <abax.h>
#include <JagCfg.h>
#include <JagUtil.h>
#include <JagMutex.h>
#include <JagGapVector.h>
// #include <JagBlock.h>
#include <JagFixBlock.h>
#include <JagArray.h>
#include <JagDBPair.h>
#include <JagFileMgr.h>
#include <JagBlockLock.h>
#include <JagStrSplit.h>
#include <JagParseExpr.h>
#include <JagTableUtil.h>
#include <JagBuffReader.h>
#include <JagBuffWriter.h>
#include <JagParseParam.h>
#include <JagIndexString.h>
#include <JagSchemaRecord.h>
#include <JaguarCPPClient.h>
#include <JagVector.h>
#include <JagDBServer.h>
#include <JDFS.h>
#include <JagRequest.h>
#include <JagHotSpot.h>
#include <JagDBMap.h>

////////////////////////////////////////// disk array class ///////////////////////////////////
class JagDiskArrayBase;
class JaguarCPPClient;
class JagBuffReader;
class JagBuffBackReader;

typedef JagArray<JagDBPair>  PairArray;


// class for separate thread reSize/pushback
class SepThrdDAPass
{
  public:
	JagDiskArrayBase *darr;
	JaguarCPPClient *cli;
	int flag;
	abaxint value;
	abaxint value2;
	bool force;
};

class JagDiskArrayBase
{
	public:
	
		// ctor and dtor
		JagDiskArrayBase( const JagDBServer *servobj, const Jstr &fpathname, 
							const JagSchemaRecord *record, 
							bool buildInitIndex=true, abaxint length=32, bool noMonitor=false );
		JagDiskArrayBase( const JagDBServer *servobj, const Jstr &fpathname, 
						   const JagSchemaRecord *record, 
						const Jstr &pdbobj, JagDBPair &minpair, JagDBPair &maxpair );
		JagDiskArrayBase( const Jstr &fpathname, const JagSchemaRecord *record, abaxint length=32 );
		virtual ~JagDiskArrayBase();
		
		inline const JagSchemaRecord *getSchemaRecord() const { return _schemaRecord; }
		inline Jstr getFilePathName() const { return _pathname; }
		inline Jstr getFilePath() const { return _filePath; }
		inline Jstr getDBName() const { return _dbname; }
		inline Jstr getObjName() const { return _objname; }
		inline Jstr getDBObject() const { return _dbobj; }
		inline int getFD() const { return _jdfs->getFD(); }
		inline int insert( JagDBPair &pair, int &insertCode, bool doFirstRedist, bool direct, JagDBPair &retpair ) { 
			return insertData( pair, insertCode, doFirstRedist, direct, retpair ); 
		}

		void initMonitor();
		void destroyMonitor();
		void cleanInsertBufferCopy();
		abaxint copyInsertBuffer();
		abaxint flushInsertBuffer();
		int getStep( int kmode, abaxint i, abaxint whole, bool cross, bool next );
		bool getFirstLast( const JagDBPair &pair, abaxint &first, abaxint &last );

		virtual void drop() {}
		virtual void buildInitIndex( bool force=false )  {}
		virtual void init( abaxint length, bool buildBlockIndex ) {}
		virtual int _insertData( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doFirstRedist, JagDBPair &retpair ) {}
		virtual int reSize( bool force=false, abaxint newarrlen=-1 ) {}
		// virtual int reSize( bool force=false, abaxint newarrlen=-1, bool doFirstDist=true, bool sepThrdSelfCall=false ) {}
		virtual void reSizeLocal( ) {}
		void copyAndInsertBufferAndClean();
		abaxint waitCopyAndInsertBufferAndClean( JagClock &clock, JagClock &clock2 );
		static Jstr jdbPath( const Jstr &jdbhome, const Jstr &db, const Jstr &tab );
		Jstr jdbPathName( const Jstr &jdbhome, const Jstr &db, const Jstr &tab );
		void debugJDBFile( int flag, abaxint limit, abaxint hold, abaxint instart, abaxint inend );
		abaxint getRegionElements( abaxint first, abaxint length );

		bool flushCompress();
		
		// data memeber for public use, can be get data easily out of class
		int _GEO;
		abaxint KEYLEN; 
		abaxint VALLEN;
		abaxint KEYVALLEN;
		int _keyMode;
		int _isdarrnew;
		bool _doneIndex;
		bool _firstResize;
		bool _isChildFile;
		std::atomic<abaxint> _insertUsers; // how many users are doing insert
		std::atomic<abaxint> _updateUsers; // how many users are doing insert
		std::atomic<abaxint> _resizeUsers; // how many users are doing insert
		std::atomic<abaxint> _removeUsers; // how many users are doing remove
		std::atomic<abaxint> _arrlen; // arrlen for local server
		std::atomic<abaxint> _garrlen; // arrlen for all servers related to this file
		std::atomic<abaxint> _elements; // elements for local server		
		std::atomic<abaxint> _numOfResizes; // number of resizes
		std::atomic<abaxint> _minindex; // max index
		std::atomic<abaxint> _maxindex; // max index
		std::atomic<abaxint> _forceResize;
		std::atomic<abaxint> _ups;
		std::atomic<abaxint> _downs;
		std::atomic<abaxint> _familyInsPairCnt; // how many pairs have been insert to this diskarray from family
		JagDBPair _oldpair;
		JDFS *_jdfs;
		JagBlockLock *_diskLock;
		JagBlockLock *_memLock;
		JagBlockLock *_reSizeLock;
		JagBlockLock *_darrnewLock;
		JagBlockLock *_sepThreadResizeLock; // lock to block update/delete when separate thread resize
		JagReadWriteLock *_lock;
		//JagBlock<JagDBPair> *_blockIndex;
		JagFixBlock *_blockIndex;

		Jstr _dirPath;
		Jstr _filePath;
		Jstr _pdbobj;
		Jstr _dbobj;
		JagDBServer *_servobj;
		time_t		_lastSyncTime;
		pthread_mutex_t 	_insertBufferMutex;
		Jstr _dbname;
		Jstr _objname;
		const JagSchemaRecord *_schemaRecord;
		abaxint _newarrlen;		
		std::atomic<int>  _isFlushing;
		JagHotSpot<JagDBPair>          *_hotspot;
		int _nthserv;
		int _numservs;
		pthread_mutex_t 	_insertBufferCopyMutex;
		abaxint		_lastSyncOneTime;
		abaxint  _shifts;
		std::atomic<int> _lastResizeMode;
		std::atomic<bool> _hasdarrnew;
		std::atomic<bool> _fileCompress;
		std::atomic<bool> _noFlush;

		PairArray * 		_pairarr;
		PairArray *			_pairarrcopy;
		JagDBMap  *			_pairmap;
		JagDBMap  *			_pairmapcopy;
		int					_fd;

		int					_forceSleepTime;

		abaxint  _mergeLimit;
		abaxint  _fileSplitCntLimit;
		abaxint  _fileMaxCntLimit;
		
	protected:

		// data member for protected class use only
		bool _uidORschema;
		JDFS *_jdfs2;
		abaxint _respartMaxBlock;
		Jstr _pathname;
		Jstr _tmpFilePath;
		// JagBlock<JagDBPair> *_newblockIndex;
		JagFixBlock *_newblockIndex;

		void destroy();
		void _getPair( char buffer[], int keylength, int vallength, JagDBPair &pair, bool keyonly ) const;
		// int needResize();
		int pushToBuffer(  JagDBPair &pair );
		int getResizePattern( float ratio, int &span );
		int insertData( JagDBPair &pair, int &insertCode, bool doFirstRedist, bool direct, JagDBPair &retpair );
		
		int updateOldPair( JagDBPair &pair );
		int insertToRange( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doresizePartial=true );
		int insertToAll( JagDBPair &pair, abaxint *retindex, int &insertCode );
		int checkSafe( char *diskbuf, abaxint index, abaxint first, abaxint last, abaxint &left, abaxint &right,
			int &relpos, int &relleft, int &relright, bool &leftfound, bool &rightfound );
		static void *monitorCommandsMem( void *ptr );
		void cleanRegionElements( abaxint first, abaxint length );
		
		abaxint getRealLast();
		bool findPred( const JagDBPair &pair, abaxint *index, abaxint first, abaxint last, JagDBPair &retpair, char *diskbuf );
		
		int insertMerge( int mergeMode, abaxint mergeElements, char *mbuf, const char *sbuf, JagBuffReader *mbr, JagBuffBackReader *mbbr );
		virtual int mergeResize( int mergeMode, abaxint mergeElements, char *mbuf, const char *sbuf, JagBuffReader *mbr ) {}
		void insertMergeUpdateBlockIndex( char *kvbuf, abaxint ipos, abaxint &lastBlock );
		void insertMergeGetMinMaxPair( JagDBPair &minpair, JagDBPair &maxpair, int mergeMode, 
			JagBuffReader *mbr, JagBuffBackReader *mbbr, char *mbuf, const char *sbuf );
		int findMergeRegionFirstLast( abaxint &minfirst, abaxint &maxfirst, 
			abaxint &numBlocks, abaxint &ranArrlen, abaxint &ranElements, abaxint mergeElements );
		// int getNextMergePair( char *kvbuf, char *dbuf, JagBuffReader &dbr, abaxint &mpos,
		//	char *mbuf, JagBuffReader *mbr, const char *sbuf, int &dgoNext, int &mgoNext, int mergeMode );
		int getNextMergePair( char *kvbuf, char *dbuf, JagBuffReader &dbr, FixMap::iterator &mpos,
			char *mbuf, JagBuffReader *mbr, const char *sbuf, int &dgoNext, int &mgoNext, int mergeMode );
		
		void checkElementsCnt();
		static void logInfo( abaxint t1, abaxint t2, abaxint cnt, const JagDiskArrayBase *jda );


		pthread_t  _threadmo;
		int					_hasmonitor;
		std::atomic<int>    _monitordone;
		std::atomic<int>  	_sessionactive;
		pthread_cond_t 		_insertBufferMoreThanMinCond;
		pthread_cond_t    	_insertBufferLessThanMaxCond;

		// debug params
		abaxint  _fulls;
		abaxint  _partials;

		abaxint  _insmrgcnt;
		abaxint  _insdircnt;
		abaxint  _reads;
		abaxint  _writes;
		abaxint  _dupwrites;
		abaxint  _upserts;
		int		 _isClient;
		int      _isSchema;
		int      _isUserID;
		abaxint  _callCounts;
		abaxint  _lastBytes;

};

#endif
