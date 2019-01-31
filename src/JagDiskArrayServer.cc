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

#include <malloc.h>
#include <JagDiskArrayServer.h>
#include <JagHashLock.h>
#include <JDFSMgr.h>
#include <JagDataAggregate.h>
#include <JagHotSpot.h>
#include <JagTime.h>
#include <JagSingleBuffReader.h>
#include <JagBuffBackReader.h>
#include <JagDBConnector.h>
#include <JagTable.h>
#include <JagFileMgr.h>
#include <JagSingleBuffWriter.h>
#include <JagUtil.h>

JagDiskArrayServer::JagDiskArrayServer( const JagDBServer *servobj, const Jstr &filePathName, 
	const JagSchemaRecord *record, bool buildInitIndex, abaxint length, bool noMonitor, bool isLastOne ) 
  :JagDiskArrayBase( servobj, filePathName, record, buildInitIndex, length, noMonitor )
{
	_isClient = 0;
	if ( buildInitIndex ) {
		this->init( length, true, isLastOne );
	} else {
		this->init( length, false, isLastOne );
	}

	if ( !noMonitor ) initMonitor();
	else _hasmonitor = 0;
}

JagDiskArrayServer::JagDiskArrayServer( const JagDBServer *servobj, const Jstr &filePathName, 
	const JagSchemaRecord *record, const Jstr &pdbobj, JagDBPair &minpair, JagDBPair &maxpair ) 
  :JagDiskArrayBase( servobj, filePathName, record, pdbobj, minpair, maxpair )
{
	_isClient = 0;
	this->init( 32, true, true );
	this->_blockIndex->updateMinKey( minpair );
	this->_blockIndex->updateMaxKey( maxpair );	
	_hasmonitor = 0;
}

JagDiskArrayServer::~JagDiskArrayServer()
{

	int rc;
	while ( _hasmonitor && ! _monitordone ) {
		_sessionactive = 0;
		// prt(("s4810 wait monthread done (%s) ...\n", _objname.c_str() ));
		jagsleep(1, JAG_MSEC);
		jaguar_mutex_lock( &(_insertBufferMutex ) );
		rc = jaguar_cond_signal(& _insertBufferMoreThanMinCond );
		if ( rc != 0 ) {
			prt(("s6361 jaguar_cond_signal error rc=%d\n", rc ));
		}
		jaguar_mutex_unlock( &(_insertBufferMutex ) );
	}

	if ( _darrnew ) {
		delete _darrnew;
	}
	_darrnew = NULL;
}

void JagDiskArrayServer::init( abaxint length, bool buildBlockIndex, bool isLastOne )
{
	// printf("s3924 init length=%lld _pathname=[%s] buildBlockIndex=[%d]\n", length, _pathname.c_str(), buildBlockIndex );
	_darrnew = NULL;
	KEYLEN = _schemaRecord->keyLength;
	VALLEN = _schemaRecord->valueLength;
	KEYVALLEN = KEYLEN+VALLEN;
	_keyMode = _schemaRecord->getKeyMode(); // get keyMode
	_forceResize = _maxindex = _numOfResizes = _insertUsers = _updateUsers = _resizeUsers = _removeUsers = _garrlen = 0;
	_minindex = -1;
	_ups = _downs = 1;
	_GEO = 4;
	// _GEO = 2;
	_arrlen = _elements = _doneIndex = _firstResize = _uidORschema = 0;
	_respartMaxBlock = atoll(_servobj->_cfg->getValue("MAX_PARTIAL_RESIZE", "1024").c_str()) * 1024 * 1024;
	_respartMaxBlock /= ( KEYVALLEN * JAG_BLOCK_SIZE );
	const char *dp = strrchr ( _pathname.c_str(), '/' );
	_dirPath = Jstr(_pathname.c_str(), dp-_pathname.c_str());
	_filePath = _pathname + ".jdb"; 
	_tmpFilePath =  _pathname + ".jdb.tmp";
	JagStrSplit split( _pathname, '/' );
	int exist = split.length();
	if ( exist < 3 ) {
		printf("s7482 error _pathname=%s, exit\n", _pathname.c_str() );
		exit(1);
	}

	if ( split[exist-2] == "system" ) {
		_dbname = "system";
	} else {
		_dbname = split[exist-3];
	}

	_objname = split[exist-1];
	_dbobj = _dbname + "." + _objname;
	JagStrSplit split2(_objname, '.');
	if ( split2.length() > 2 ) { // must be tab.idx.1 format, use idx
		_pdbobj = _dbname + "." + split2[1];
		_isChildFile = 1;
	} else if ( split2.length() == 2 ) { // may be tab.idx or tab.1 format; if tab.idx, use idx, otherwise, use tab
		if ( isdigit(*(split2[1].c_str())) ) { // is tab.1 format, use tab
			_pdbobj = _dbname + "." + split2[0];
			_isChildFile = 1;
		} else { // otherwise, tab.idx format, use idx
			_pdbobj = _dbname + "." + split2[1];
		}
	} else {
		_pdbobj = _dbobj;
	}
	if ( _dbname == "system" ) {
		_uidORschema = 1;
		if ( _objname == "TableSchema" || _objname == "IndexSchema" ) {
			_isSchema = 1;
		} else if ( _objname == "UserID" ) {
			_isUserID = 1;
		}
	}
	
	_lock = new JagReadWriteLock();
	_diskLock = new JagBlockLock();
	_memLock = new JagBlockLock();
	_reSizeLock = new JagBlockLock();
	_darrnewLock = new JagBlockLock();
	_sepThreadResizeLock = new JagBlockLock();
	// _blockIndex = new JagBlock<JagDBPair>();
	_blockIndex = new JagFixBlock( KEYLEN );
	_newblockIndex = NULL;
	_jdfs = new JDFS( _servobj, _filePath, KEYVALLEN, length );
	_jdfs2 = NULL;
	
	// set nthserv and numservs
	_nthserv = 0;
	_numservs = 1;
	_firstResize = 1;

	_fileMaxCntLimit = getNearestBlockMultiple( ONE_GIGA_BYTES/2/KEYVALLEN );
	_fileSplitCntLimit = _fileMaxCntLimit * 80 / 100;
	if ( _fileSplitCntLimit > _servobj->_sea_records_limit ) _fileSplitCntLimit = _servobj->_sea_records_limit;
	if ( _uidORschema ) _fileSplitCntLimit = 0;

	// if dir path does not exist, create it, otherwise, do nothing
	if ( ! JagFileMgr::exist( _dirPath ) ) {
		prt(("s3910 makedirPath(%s)\n", _dirPath.c_str() ));
		JagFileMgr::makedirPath( _dirPath, 0755 );
	}
	
	exist = _jdfs->exist();
	_jdfs->open();
	_fd = _jdfs->getFD();
	
	_arrlen = _jdfs->getArrayLength();
	_garrlen = (abaxint)_arrlen;
	_firstResize = 1;
	if ( !exist ) {
		size_t bytes = _arrlen * KEYVALLEN;
		_jdfs->fallocate( 0, bytes );
	} else {
		if ( buildBlockIndex ) {
			// if original file exist, need to check if last server stops unsuccessfully during resize, need to resize when restart server
			if ( !checkInitResizeCondition( isLastOne ) ) {
				// prt(("s8870 %s begin buildInitIndex for init elem=%lld arln=%lld\n", _dbobj.c_str(), (abaxint)_elements, (abaxint)_arrlen));
				JagClock clock;
				clock.start();
				buildInitIndex();
				clock.stop();
				// prt(("s8870 %s buildInitIndex for init %d ms\n", _dbobj.c_str(), clock.elapsed()));
			} else {
				Jstr idxPath = renameFilePath( _filePath, "bid" );
				jagunlink( idxPath.c_str() );
			}
		}
	}
	_familyInsPairCnt = (abaxint)_elements;
}

// method need to check if resize needs to be done when init
// also need to see if file have been compressed or not
// ATTENTION: 
// return 0: no resize needed, please rebuild initBlockIndex
// return 1: resize at init, no need to rebuild initBlockIndex ( already built during resize )
// return 2: compress at init, no need to rebuild initBlockIndex ( already built during compress )
bool JagDiskArrayServer::checkInitResizeCondition( bool isLastOne )
{
	// first check if resize needs to be done;
	// condition 1: if db.tab.jdb.tmp or db.tab_darrnew.jdb or db.tab_darrnew.jdb.tmp exists, always true to do resize
	// otherwise, buildInitIndex to get elements, then use needResize to check if need to do resize
	Jstr path, tpath, dnpath, tdnpath;
	int nrsz = 0, rc;
	path = _filePath;
	tpath = _tmpFilePath;
	dnpath = _pathname + "_darrnew.jdb";
	tdnpath = _pathname + "_darrnew.jdb.tmp";
	if ( _arrlen > 0 ) {
		recountElements();
		if ( needResize() > 0 ) {
			nrsz = 1;
		}
	}
	jagunlink( tpath.c_str() );
	jagunlink( tdnpath.c_str() );

	if ( nrsz ) {
		prt(("s8877 %s begin resize for init elem=%lld arln=%lld\n", _dbobj.c_str(), (abaxint)_elements, (abaxint)_arrlen));
		JagClock clock;
		clock.start();
		_newarrlen = 2*_garrlen;
		reSizeLocal();
		clock.stop();
		prt(("s8877 %s end resize for init %d ms\n", _dbobj.c_str(), clock.elapsed() ));
		struct stat sbuf;
		stat(dnpath.c_str(), &sbuf);
		int dnfd = jagopen( dnpath.c_str(), O_RDONLY|JAG_NOATIME, S_IRWXU );
		if ( dnfd >= 0 && sbuf.st_size > 0 ) {
			prt(("s8810 %s darrnew file exists, begin merge for init\n", _dbobj.c_str() ));
			clock.start();
			abaxint rlimit = getBuffReaderWriterMemorySize( sbuf.st_size/1024/1024 );
			JagSingleBuffReader br( dnfd, sbuf.st_size/KEYVALLEN, KEYLEN, VALLEN, 0, 0, rlimit );
			char *buf = (char*)jagmalloc(KEYVALLEN+1);
			memset(buf, 0, KEYVALLEN+1);
			abaxint idx;
			int  insertCode;
			while ( br.getNext( buf ) ) {
				JagDBPair rpair( buf, KEYLEN, buf+KEYLEN, VALLEN, true );
				rc = _insertData( rpair, &idx, insertCode, false, rpair );
			}
			clock.stop();
			prt(("s8810 %s merge for init %d ms\n", _dbobj.c_str(), clock.elapsed() ));
			if ( buf ) free( buf );
		}
		jagclose( dnfd );
		jagunlink( dnpath.c_str() );
		return 1;
	}
	return 0;
}

void JagDiskArrayServer::recountElements()
{
	_elements = 0;
   	char *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1);
	memset( keyvalbuf, 0,  KEYVALLEN + 1 );
	
	abaxint nserv = _nthserv;
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader nav( this, _arrlen, KEYLEN, VALLEN, nserv*_arrlen, 0, rlimit );
	while ( nav.getNext( keyvalbuf ) ) { 
		++ _elements;
	}
	free( keyvalbuf );
	jagmalloc_trim( 0 );
}


// build and find first last from the blockIndex
// buildindex from disk array file data
// setup _elements
// force: force to build index from disk array
void JagDiskArrayServer::buildInitIndex( bool force )
{
	// prt(("s2283 JagDiskArrayServer::buildInitIndex ...\n"));
	if ( ! force ) {
		int rc = buildInitIndexFromIdxFile();
		if ( rc ) {
			jagmalloc_trim( 0 );
			return;
		}
	}

	// dbg(("s7777 begin buildindex\n"));
	if ( !force && _doneIndex ){
		dbg(("s3820 in buildInitIndex return here\n"));
		return;
	}

	if ( _newblockIndex ) {
		raydebug( stdout, JAG_LOG_LOW, "Error condition. Resize being done. ignore(%s)\n", _dbobj.c_str());
		// abort();
		// delete _newblockIndex;
		return;
	}
	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	
	_elements = 0;
	JagDBPair tpair;
	abaxint ipos, lastBlock = -1;
   	char *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1);
	memset( keyvalbuf, 0,  KEYVALLEN + 1 );
	
	abaxint nserv = _nthserv;
	// raydebug( stdout, JAG_LOG_LOW, "building blockindex for (%s) ...\n", _dbobj.c_str());
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader nav( this, _arrlen, KEYLEN, VALLEN, nserv*_arrlen, 0, rlimit );
	// prt(("s7777 buildInitIndex [%s] %d offsetpos=%lld, _arrlen=%lld, KEYVALLEN=%lld, _garrlen=%lld\n", _filePath.c_str(), nserv, nserv*_arrlen, (abaxint)_arrlen, KEYVALLEN, (abaxint)_garrlen ));
	_minindex = -1;
	while ( nav.getNext( keyvalbuf, KEYVALLEN, ipos ) ) { 
		ipos += nserv*_arrlen;
		// prt(("s7777 buildInitIndex keyvalbuf=[%s], ipos=%d\n", keyvalbuf, ipos ));
		++ _elements;
		_maxindex = ipos;
		if ( _minindex < 0 ) _minindex = ipos;
		_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
		_newblockIndex->updateMaxKey( tpair, false );

		if ( ( ipos/JagCfg::_BLOCK) != lastBlock ) {
			_newblockIndex->updateIndex( tpair, ipos, false, false );
		}
		_newblockIndex->updateCounter( ipos, 1, false, false );
		
		lastBlock = ipos/JagCfg::_BLOCK;
	}
	//raydebug( stdout, JAG_LOG_HIGH, "Built blockindex for (%s)\n", _dbobj.c_str());

	if ( _blockIndex ) delete _blockIndex;
	_blockIndex = _newblockIndex;
	_newblockIndex = NULL;
	_doneIndex = 1;
	free( keyvalbuf );
	jagmalloc_trim( 0 );
	// _blockIndex->print();
	// dbg(("s39 %s, elems=%d, arln=%d, garln=%d\n", _filePath.c_str(), (abaxint)_elements, (abaxint)_arrlen, (abaxint)_garrlen ));
	// prt(("s73 _blockIndex minkey=[%s]  maxkey=[%s]\n", _blockIndex->_minKey.key.c_str(),  _blockIndex->_maxKey.key.c_str() ));
}

// drop file
void JagDiskArrayServer::drop()
{
	JAG_BLURT _reSizeLock->writeLock( -1 ); JAG_OVER;
	JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
	JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;

	_jdfs->remove();
	removeBlockIndexIndDisk();
	jagunlink( _tmpFilePath.c_str() );
	Jstr dnpath = _pathname + "_darrnew.jdb";
	jagunlink( dnpath.c_str() );
	dnpath += ".tmp";
	jagunlink( dnpath.c_str() );


	// if dir is empty, remove dir, otherwise, do nothing
	if ( JagFileMgr::dirEmpty( _dirPath ) ) {
		// prt(("s2830 [%s] is empty, remove it\n", _dirPath.c_str() ));
		JagFileMgr::rmdir( _dirPath );
	}

	_diskLock->writeUnlock( -1 );
	_memLock->writeUnlock( -1 );
	_reSizeLock->writeUnlock( -1 ); 
}

// insertCode: 200 if min-max has changed
// 0: error
int JagDiskArrayServer::_insertData( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doFirstDist, JagDBPair &retpair )
{
	insertCode = 0;
	if ( _arrlen == 0 || _garrlen == 0 ) return 0;

	int rc = -1;
	
	JAG_BLURT _reSizeLock->readLock( -1 ); JAG_OVER;

	// debug
	// prt(("s6831 insert (%s) print blockIndex:\n", pair.key.c_str() ));
	// _blockIndex->print();
	//_insertUsers: at here, _insertUsers must be at least 1
	// first element
	if ( _elements < 1 ) {
		JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;
		if (  _elements < 1 ) {
			*retindex = _nthserv*_arrlen;
			*retindex = *retindex + 1;
			_maxindex = *retindex;
			_minindex = *retindex;

			raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, (*retindex)*KEYVALLEN);
			++_elements;
			rc = _blockIndex->updateIndex( pair, 1 );
			_blockIndex->updateCounter( 1, 1, false );

			if ( rc ) insertCode = 200;
			rc = 1;
			_diskLock->writeUnlock( -1 );
			_reSizeLock->readUnlock( -1 );
			// prt(("s9999 insert %d key=[%s] index=%d _elements=%d\n", rc, pair.key.c_str(), *retindex, (abaxint)_elements));
			return rc;
		}
    	_diskLock->writeUnlock( -1 );
	}
	
	bool doInsertToAll = false;
	rc = insertToRange( pair, retindex, insertCode );
	if ( rc == -99 ) {
		rc = insertToAll( pair, retindex, insertCode );
		doInsertToAll = true;
	}

	if ( rc == -99 ) {
		rc = 0;
	}

	_reSizeLock->readUnlock( -1 );

	if ( ( needResize() > 0 || doInsertToAll ) ) {	
		if ( !doInsertToAll ) reSize( false, -1 );
		else {
			prt(("s7143 rebal idx=%lld elem=%lld arl=%lld midex=%lld mxidx=%lld\n", 
				*retindex, (abaxint)_elements, (abaxint)_arrlen, (abaxint)_minindex, (abaxint)_maxindex));
			reSize( true, -1 );
		}
	}
	// prt(("s9999 %lld insert %d key=[%s] index=%d _elements=%d\n", THREADID, rc, pair.key.c_str(), *retindex, (abaxint)_elements));
	
	if ( ! rc && strcmp(pair.key.c_str(), "admin" ) != 0 ) {
		prt(("s1902 insert into [%s] error:\n", _pdbobj.c_str() )); 
		pair.key.dump();
		// pair.value.dump();
	}
	return rc;
}

int JagDiskArrayServer::reSize( bool force, abaxint newarrlen )
{
	time_t t1 = time(NULL);
	int resizerc = 1;

	// prt(("s3871 %lld %s reSize %d %lld\n", THREADID, _filePath.c_str(), force, newarrlen ));
	++_resizeUsers;
	JAG_BLURT _reSizeLock->writeLock( -1 ); JAG_OVER; 

	if ( !force && needResize() == 0 ) {
		_reSizeLock->writeUnlock( -1 );
		--_resizeUsers;
		return -1;
	}
	
	if ( newarrlen != -1 && _arrlen >= newarrlen ) {
		_reSizeLock->writeUnlock( -1 );
		--_resizeUsers;
		return -2;
	}
	
	if ( newarrlen > 0 ) _newarrlen = newarrlen*_numservs;
	else _newarrlen = _GEO*_garrlen;
	if ( _newarrlen > _fileMaxCntLimit && !_uidORschema ) _newarrlen = _fileMaxCntLimit;

	if ( force ) {
		if ( _elements*100 < _arrlen*JAG_FORCE_RESIZE_EXPAND && newarrlen <= 0 ) {
			_newarrlen = _garrlen;
			// prt(("s7890 force resize newarrlen=%d\n", (abaxint)_newarrlen));
		}
	}
	
	reSizeLocal();
	++_numOfResizes;

	_reSizeLock->writeUnlock( -1 );

	time_t t2 = time(NULL);
	// Do not comment this
	// prt(("s8255 %lld %s finish resize\n", THREADID, _filePath.c_str()));
	prt(("s8246 %s elm=%lld, arl=%lld, grl=%lld ful=%lld prt=%lld bib=%lld geo=%d %d s\n", 
	 		_dbobj.c_str(), (abaxint)_elements, (abaxint)_arrlen, 
			(abaxint)_garrlen, _fulls, _partials, _blockIndex->getBottomCapacity(), _GEO, t2-t1 ));
	--_resizeUsers;
	return 1;
}

//if file already distributes to all server, reSize local server equally
void JagDiskArrayServer::reSizeLocal()
{
	// prt(("s3238 reSizeLocal _pathname=[%s]\n", _pathname.c_str()));
	if ( _newblockIndex ) {
		raydebug( stdout, JAG_LOG_LOW, "s123456789b invalid condition. Someone must be doing resize. abort(%s)\n", _dbobj.c_str());
		// abort();
		return;
	}

	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	bool half = 0, even = 0;
	JagDBPair tpair;
	abaxint ipos, pos, lastBlock = -1;
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	
	// first, need to check if file is too tight
	while ( true ) {
		if ( 100*_elements/_newarrlen < JAG_FILE_RAND_EXPAND ) break;
		_newarrlen *= _GEO;
	}
		
	// for reAlloc
	size_t bytes2 = _newarrlen*KEYVALLEN;
	_jdfs2 = new JDFS( _servobj, _tmpFilePath, KEYVALLEN, _newarrlen );
	_jdfs2->open();
	_jdfs2->fallocate( 0, bytes2 );

	if ( _elements > 0 ) {
		abaxint actualcnt = 0, whole = 0, ascwhole = 0, aschalfelem = 0, mpos = 0;
		double ratio = 0.0;
		int resizeHotSpotMode = _hotspot->getResizeMode();
		_lastResizeMode = resizeHotSpotMode;
		int ascSpareGap = JAG_FILE_OTHER_EXPAND/10;
		if ( !_isdarrnew ) { 
			raydebug( stdout, JAG_LOG_LOW, "s2202 %s darr reszmod=%d\n", _filePath.c_str(), resizeHotSpotMode );
		}

		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING || resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) {
			whole = 2;
			ratio = 0.0;
		} else if ( resizeHotSpotMode == JAG_STRICT_RANDOM ) {
			whole = _newarrlen*JAG_RANDOM_RESIZE_PERCENT / _elements; // 64/18 = 3  avg spacing
			ratio = _newarrlen*JAG_RANDOM_RESIZE_PERCENT /_elements;  // 64/18 = 3.555
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_SMALL ) {
			whole = _newarrlen*JAG_CYCLERIGNT_SMALL_PERC / _elements; // 64/18 = 3  avg spacing
			ratio = 0.0;
			if ( whole > 2 ) ascwhole = whole - 1;
			aschalfelem = _elements / 2;
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_LARGE ) {
			whole = _newarrlen*JAG_CYCLERIGNT_LARGE_PERC / _elements; // 64/18 = 3  avg spacing
			ratio = 0.0;
			if ( whole > 2 ) ascwhole = whole - 1;
			aschalfelem = _elements / 2;
		}
		if ( ratio-whole >= 0.5 ) half = 1; 

		pos = 1;
		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) pos = _newarrlen-2*_elements;

		// for reDist	
		abaxint rlimit = 128;
		JagSingleBuffReader br( _fd, _arrlen, KEYLEN, VALLEN, 0, 0, rlimit );
		abaxint wlimit = 128;
		JagSingleBuffWriter bw( _jdfs2->getFD(), KEYVALLEN, wlimit );

		_minindex = -1;
		while ( br.getNext( keyvalbuf ) ) {
			// printf("s9999 keyvalbuf=[%s] pos=[%d]\n", keyvalbuf, pos);
			jaguar_cond_signal(& _insertBufferLessThanMaxCond );
			++actualcnt;
			ipos = pos;
			bw.writeit( pos, keyvalbuf, KEYVALLEN );
			if ( resizeHotSpotMode == JAG_STRICT_ASCENDING || resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) {
				pos += getStep( JAG_ASC, pos, whole, half, even );
			} else {
				if ( ascwhole > 0 ) {
					if ( actualcnt <= aschalfelem ) {
						pos += getStep( JAG_RAND, pos, ascwhole, half, 0 );
					} else {
						pos += getStep( JAG_RAND, pos, ascwhole+2, half, 0 );
					}
				} else {
					pos += getStep( JAG_RAND, pos, whole, half, even );
				}
			}

			even = !even;
			_maxindex = ipos;
			if ( _minindex < 0 ) _minindex = ipos;
			_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
			_newblockIndex->updateMaxKey( tpair, false );
			if ( ( ipos/JagCfg::_BLOCK) != lastBlock ) {
				_newblockIndex->updateIndex( tpair, ipos, false, false );
			}
			_newblockIndex->updateCounter( ipos, 1, false, false );
			lastBlock = ipos/JagCfg::_BLOCK;
		}
		bw.flushBuffer();
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
		if ( _elements != actualcnt ) {
			raydebug( stdout, JAG_LOG_LOW, 
					  "s9999 [%s] when resize, _elements=%l != actualcnt=%l\n", 
					  _dbobj.c_str(), (abaxint)_elements, actualcnt);
			// abort();
		}

		_hotspot->clean();  
		_hotspot->setMaxPair( tpair );  

	} else {
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
	}

	// prt(("s9999 before delete jdfs _pathname=[%s]\n", _pathname.c_str()));
	if ( _jdfs ) delete _jdfs;
	_jdfs = _jdfs2;
	_jdfs2 = NULL;
	_jdfs->rename( _filePath );
	_fd = _jdfs->getFD();
	// prt(("s9999 after delete jdfs _pathname=[%s]\n", _pathname.c_str()));
	// fsetXattr( _jdfs->getFD(), _filePath.c_str(), "user.garrlen", _garrlen );
	_memLock->writeUnlock( -1 );
	// prt(("s9999 end resize local _pathname=[%s]\n", _pathname.c_str()));
	//prt(("s7124 after resizelocal printRange()...\n"));
	//print( -1, -1, 1000 );
	free( keyvalbuf );
	_shifts = 0;
	// prt(("s1283  resize local _shifts = 0 \n" ));
}

// method to compress jdb file
// return 0: file is already been compressed, not many spare spaces in jdb file, no more compression done
// return 1: file has some spare spaces in jdb file, another compression has been done
bool JagDiskArrayServer::reSizeCompress()
{
	_reSizeLock->writeLock( -1 ); JAG_OVER;
	// first, check and see if more compress needs to be done
	if ( _elements > _arrlen*8/10 ) {
		// already been compressed, and file is still relatively full, no more compress needed to be done
		raydebug( stdout, JAG_LOG_LOW, "s7230 %s _elems=%l > 0.8*_arln=%l already compressed\n", 
				  _objname.c_str(), (abaxint)_elements, (abaxint)_arrlen );
		_reSizeLock->writeUnlock( -1 );
		return 0;
	}

	if ( _newblockIndex ) {
		delete _newblockIndex;
		_newblockIndex = NULL;
	}

	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	JagDBPair tpair;
	abaxint ipos, pos, lastBlock = -1;
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	
	if ( _elements % JAG_BLOCK_SIZE != 0 ) _newarrlen = (_elements/JAG_BLOCK_SIZE+1)*JAG_BLOCK_SIZE;
	else _newarrlen = _elements;
	if ( _newarrlen <= 0 ) _newarrlen = JAG_BLOCK_SIZE;
	// else _newarrlen = (_elements > 0) ? _elements : JAG_BLOCK_SIZE; // empty file may accept new data
	size_t bytes2 = _newarrlen*KEYVALLEN;
	_jdfs2 = new JDFS( _servobj, _tmpFilePath, KEYVALLEN, _newarrlen );
	_jdfs2->open();
	_jdfs2->fallocate( 0, bytes2 );

	if ( _elements > 0 ) {
		abaxint actualcnt = 0, whole = 1;
		bool half = 0, even = 0;
		pos = 0;

		abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
		JagSingleBuffReader br( _fd, _arrlen, KEYLEN, VALLEN, 0, 0, rlimit );
		abaxint wlimit = getBuffReaderWriterMemorySize( _newarrlen*KEYVALLEN/1024/1024 );
		JagSingleBuffWriter bw( _jdfs2->getFD(), KEYVALLEN, wlimit );
		_minindex = 0;
		while ( br.getNext( keyvalbuf ) ) {
			++actualcnt;
			ipos = pos;
			bw.writeit( pos, keyvalbuf, KEYVALLEN );
			++pos;
			_maxindex = ipos;
			if ( _minindex < 0 ) _minindex = ipos;
			_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
			_newblockIndex->updateMaxKey( tpair, false );
			if ( ( ipos/JagCfg::_BLOCK) != lastBlock ) {
				_newblockIndex->updateIndex( tpair, ipos, false, false );
			}
			_newblockIndex->updateCounter( ipos, 1, false, false );
			lastBlock = ipos/JagCfg::_BLOCK;
		}
		bw.flushBuffer();
		
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
		if ( _elements != actualcnt ) {
			raydebug( stdout, JAG_LOG_LOW, 
					  "s9999 [%s] when resize compress,  _elements=%l != actualcnt=%l\n", 
					  _dbobj.c_str(), (abaxint)_elements, actualcnt);
			// abort();
		}

		_hotspot->clean();  
		_hotspot->setMaxPair( tpair );
	} else {
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
	}

	if ( _jdfs ) delete _jdfs;
	_jdfs = _jdfs2;
	_jdfs2 = NULL;
	_jdfs->rename( _filePath );
	_fd = _jdfs->getFD();
	_memLock->writeUnlock( -1 );
	free( keyvalbuf );
	_reSizeLock->writeUnlock( -1 );
	jagmalloc_trim( 0 );
	return 1;
}

int JagDiskArrayServer::mergeResize( int mergeMode, abaxint mergeElements, char *mbuf, const char *sbuf, JagBuffReader *mbr )
{
	time_t t1 = time(NULL);
	int resizerc = 1;

	// prt(("s3871 reSize %d %lld %d %d\n", force, newarrlen, doFirstDist, sepThrdSelfCall ));
	++_resizeUsers;
	JAG_BLURT _reSizeLock->writeLock( -1 ); JAG_OVER;
	
	_newarrlen = _GEO*_arrlen;
	abaxint totelements = _elements + mergeElements;
	while( _newarrlen*JAG_FILE_RAND_EXPAND < totelements*100 ) { _newarrlen *= _GEO; }
	if ( _newarrlen > _fileMaxCntLimit && !_uidORschema ) _newarrlen = _fileMaxCntLimit;
	
	if ( _newblockIndex ) {
		raydebug( stdout, JAG_LOG_LOW, "s123456789b invalid condition. Someone must be doing resize. abort(%s)\n", _dbobj.c_str());
		// abort();
		return 0;
	}

	_newblockIndex = new JagFixBlock( KEYLEN );
	bool half = 0, even = 0;
	JagDBPair tpair;
	abaxint ipos, pos, lastBlock = -1;
	char *dbuf = (char*)jagmalloc(KEYVALLEN+1);
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	memset( dbuf, 0, KEYVALLEN+1 );
	abaxint numservs = _numservs;
	abaxint nthserv = _nthserv;
	
	// for reAlloc
	size_t bytes2 = _newarrlen*KEYVALLEN;
	_jdfs2 = new JDFS( _servobj, _tmpFilePath, KEYVALLEN, _newarrlen );
	_jdfs2->open();
	_jdfs2->fallocate( 0, bytes2 );

	// abaxint mpos = 0;
	FixMap::iterator mpos = _pairmapcopy->_map->begin();
	abaxint actualcnt = 0, whole = 0, ascwhole = 0, aschalfelem = 0;
	int dgoNext = 1, mgoNext = 1;
	double ratio = 0.0;
	int resizeHotSpotMode = _hotspot->getResizeMode();
	_lastResizeMode = resizeHotSpotMode;
	int ascSpareGap = JAG_FILE_OTHER_EXPAND/10;
	int span, pattern;

	if ( totelements > 0 ) {
		if ( !_isdarrnew ) { 
			raydebug( stdout, JAG_LOG_LOW, "s2203 darr mergeRsz reszmod=%d\n", resizeHotSpotMode );
		}

		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING || resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) {
			whole = 2;
			ratio = 0.0;
		} else if ( resizeHotSpotMode == JAG_STRICT_RANDOM ) {
			whole = _newarrlen*JAG_RANDOM_RESIZE_PERCENT / totelements; // 64/18 = 3  avg spacing
			ratio = _newarrlen*JAG_RANDOM_RESIZE_PERCENT /totelements;  // 64/18 = 3.555
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_SMALL ) {
			whole = _newarrlen*JAG_CYCLERIGNT_SMALL_PERC / totelements; // 64/18 = 3  avg spacing
			ratio = 0.0;
			if ( whole > 2 ) ascwhole = whole - 1;
			aschalfelem = totelements / 2;
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_LARGE ) {
			whole = _newarrlen*JAG_CYCLERIGNT_LARGE_PERC / totelements; // 64/18 = 3  avg spacing
			ratio = 0.0;
			if ( whole > 2 ) ascwhole = whole - 1;
			aschalfelem = totelements / 2;
		}
		if ( ratio-whole >= 0.5 ) half = 1;

		ratio = (double)totelements/(double)_newarrlen;	
		pattern = getResizePattern( ratio, span );

		pos = 1;
		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) pos = _newarrlen-2*totelements;
		pos = -1;

		// for reDist	
		abaxint rlimit = 128;
		JagBuffReader br( this, _arrlen, KEYLEN, VALLEN, 0, 0, rlimit );
		abaxint wlimit = 128;
		JagSingleBuffWriter bw( _jdfs2->getFD(), KEYVALLEN, wlimit );

		_minindex = -1;
		while ( getNextMergePair( keyvalbuf, dbuf, br, mpos, mbuf, mbr, sbuf, dgoNext, mgoNext, mergeMode ) ) {
			// printf("s9999 keyvalbuf=[%s] pos=[%d]\n", keyvalbuf, pos);
			if ( 0 == pattern ) {
				pos += 1;
			} else if ( pattern == JAG_EEEV || pattern == JAG_EEV || pattern == JAG_EV ) {
				pos += span;
			} else {
				if ( (actualcnt%span) == 0 ) {
					pos += 2;
				} else {
					pos += 1;
				}
			}
			++actualcnt;
			ipos = pos;
			bw.writeit( pos, keyvalbuf, KEYVALLEN );
			_maxindex = ipos;
			if ( _minindex < 0 ) _minindex = ipos;
			_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
			_newblockIndex->updateMaxKey( tpair, false );
			if ( ( ipos/JagCfg::_BLOCK) != lastBlock ) {
				_newblockIndex->updateIndex( tpair, ipos, false, false );
			}
			_newblockIndex->updateCounter( ipos, 1, false, false );
			lastBlock = ipos/JagCfg::_BLOCK;
		}

		bw.flushBuffer();
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
		if ( totelements != actualcnt ) {
			raydebug( stdout, JAG_LOG_LOW, 
					  "s9999 [%s] when merge resize, totelements=%l != actualcnt=%l\n", 
					  _dbobj.c_str(), totelements, actualcnt);
			abort();
		}

		_hotspot->clean();  
		_hotspot->setMaxPair( tpair );
	} else {
		JAG_BLURT _memLock->writeLock( -1 ); JAG_OVER;
		_arrlen = _newarrlen;
		_garrlen = _newarrlen;
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
	}

	if ( _jdfs ) delete _jdfs;
	_jdfs = _jdfs2;
	_jdfs2 = NULL;
	_jdfs->rename( _filePath );
	_fd = _jdfs->getFD();
	_memLock->writeUnlock( -1 );
	free( keyvalbuf );
	free( dbuf );
	_shifts = 0;
	jagmalloc_trim( 0 );
	
	++_numOfResizes;

	_reSizeLock->writeUnlock( -1 );

	time_t t2 = time(NULL);
	// Do not comment this
	raydebug( stdout, JAG_LOG_LOW, "s124 mergeResize %s elm=%l, arl=%l, grl=%l ful=%l prt=%l bib=%l geo=%d %d s\n", 
	 		_dbobj.c_str(), (abaxint)_elements, (abaxint)_arrlen, 
			(abaxint)_garrlen, _fulls, _partials, _blockIndex->getBottomCapacity(), _GEO, t2-t1 );
	--_resizeUsers;
	return 1;
}

// check diskarray if order is correct
// 1: correct   0: not correct
int JagDiskArrayServer::orderCheckOK()
{
	JagDBPair tpair;
	JagDBPair lastpair;
	abaxint ipos;
   	char *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1);
	memset( keyvalbuf, 0,  KEYVALLEN + 1 );
	abaxint nserv = _nthserv;
	if ( _isdarrnew ) nserv = 0;
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader nav( this, _arrlen, KEYLEN, VALLEN, nserv*_arrlen, 0, rlimit );
	while ( nav.getNext( keyvalbuf, KEYVALLEN, ipos ) ) { 
		_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
		if ( tpair < lastpair || tpair == lastpair ) {
			free( keyvalbuf );
			return 0;
		}

		lastpair = tpair;
	}

	free( keyvalbuf );
	return 1;
}

// buildindex from idx file flushed from blockindex 
// setup _elements
// 1: OK
// 0: error
int JagDiskArrayServer::buildInitIndexFromIdxFile()
{
	if ( _doneIndex ){
		dbg(("s3822 in buildInitIndexFromIdxFile return here\n"));
		return 1;
	}

	Jstr idxPath = renameFilePath( _filePath, "bid");
	// prt(("s2283 buildInitIndexFromIdxFile idxPath=[%s]\n", idxPath.c_str() ));

	int fd = jagopen((char *)idxPath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) {
		if ( ! _isSchema && ! _isUserID ) {
			/**
			prt(("s8122 buildInitIndexFromIdxFile _isSchema=%d _isUserID=%d  _dbname=[%s] _objname=[%s]\n", _isSchema, _isUserID, _dbname.c_str(), _objname.c_str() ));
			prt(("s2483 buildInitIndexFromIdxFile idxPath=[%s] not found\n", idxPath.c_str() ));
			**/
		}
		return 0;
	}

	char *buf = (char*)jagmalloc(JAG_BID_FILE_HEADER_BYTES+2*KEYLEN+1);
	char c;
	memset( buf, 0, JAG_BID_FILE_HEADER_BYTES+2*KEYLEN+1 );
	raysaferead( fd, buf, 1 );
	if ( buf[0] != '0' ) {
		prt(("Bid file corrupted for object [%s]. Rebuild from disk file...\n", _dbobj.c_str()));
		jagclose( fd );
		free( buf );
		jagunlink( idxPath.c_str() );
		return 0;
	}

	struct stat sbuf;
	stat(idxPath.c_str(), &sbuf);
	if ( sbuf.st_size < 1 ) {
		prt(("Bid file corrupted for object [%s]. Rebuild from disk file...\n", _dbobj.c_str()));
		jagclose( fd );
		free( buf );
		jagunlink( idxPath.c_str() );
		return 0;
	}

	raysaferead( fd, buf, JAG_BID_FILE_HEADER_BYTES+2*KEYLEN );
	c = buf[16];
	buf[16] = '\0';
	_elements = atoll(buf);
	buf[16] = c;

	c = buf[32];
	buf[32] = '\0';
	abaxint idxlen = atoll( buf +16 );
	buf[32] = c; 

	c = buf[48];
	buf[48] = '\0';
	_minindex = atoll( buf +32 );
	buf[48] = c; 

	c = buf[64];
	buf[64] = '\0';
	_maxindex = atoll( buf +48 );
	buf[64] = c; 

	if ( _newblockIndex ) {
		raydebug( stdout, JAG_LOG_LOW, " invalid condition. Someone must doing resize. abort(%s)\n", _dbobj.c_str());
		return 0;
	}
	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );

	// read maxKey from bid file and update it
   	char maxKeyBuf[ KEYLEN+1 ];
	memset( maxKeyBuf, 0,  KEYLEN + 1 );
	memcpy( maxKeyBuf, buf+JAG_BID_FILE_HEADER_BYTES+KEYLEN, KEYLEN ); 
	JagDBPair maxPair;
	_getPair( maxKeyBuf, KEYLEN, 0, maxPair, true );
	_newblockIndex->updateMaxKey( maxPair, false );
	
	JagDBPair tpair;
   	char keybuf[ KEYLEN+2];
	memset( keybuf, 0,  KEYLEN + 2 );
	
	abaxint rlimit = getBuffReaderWriterMemorySize( (sbuf.st_size-1-JAG_BID_FILE_HEADER_BYTES-2*KEYLEN)/1024/1024 );
	JagSingleBuffReader br( fd, (sbuf.st_size-1-JAG_BID_FILE_HEADER_BYTES-2*KEYLEN)/(KEYLEN+1), KEYLEN, 1, 0, 1+JAG_BID_FILE_HEADER_BYTES+2*KEYLEN, rlimit );
	abaxint cnt = 0, i, callCounts = -1, lastBytes = 0;
	prt(("s1521 availmem=%lld MB\n", availableMemory( callCounts, lastBytes )/ONE_MEGA_BYTES ));
	raydebug( stdout, JAG_LOG_LOW, "begin reading bid file ...\n" );
	while( br.getNext( keybuf, i ) ) {	
		++cnt;
		_getPair( keybuf, KEYLEN, 0, tpair, true );
		_newblockIndex->updateIndex( tpair, i*JagCfg::_BLOCK, false, false );
		_newblockIndex->updateCounter( i*JagCfg::_BLOCK, keybuf[KEYLEN], true, false );
	}
	raydebug( stdout, JAG_LOG_LOW, "done reading bid file %l records rlimit=%l\n", cnt, rlimit );
	prt(("s1522 availmem=%lld MB\n", availableMemory( callCounts, lastBytes )/ONE_MEGA_BYTES ));
	jagclose( fd );

	if ( _blockIndex ) delete _blockIndex;
	_blockIndex = _newblockIndex;
	_newblockIndex = NULL;
	_doneIndex = 1;
	free( buf );

	jagunlink( idxPath.c_str() );
	return 1;
}

bool JagDiskArrayServer::getLimitStartPos( abaxint &startlen, abaxint limitstart, abaxint &soffset )
{
	bool rc;
	JAG_BLURT _memLock->readLock( -1 );
	JAG_OVER;
	rc = _blockIndex->findLimitStart( startlen, limitstart, soffset );

	if ( ! rc ) {
		soffset = -1 * _elements;
		_memLock->readUnlock( -1 );
		return false;
	}

	_memLock->readUnlock( -1 );
	return true;
}

// check pair exist
bool JagDiskArrayServer::exist( const JagDBPair &pair, abaxint *index, JagDBPair &retpair )
{
	abaxint first, last;
	JAG_BLURT _memLock->readLock( -1 );
	JAG_OVER;
	++_reads;
	bool rc = getFirstLast( pair, first, last );
	if ( rc ) {
		rc = findPred( pair, index, first, last, retpair, NULL );
		_memLock->readUnlock( -1 );
	} else {
		_memLock->readUnlock( -1 );
		return false;
	}

	if ( ! rc ) return false;
	return true;
}

// get a pair
bool JagDiskArrayServer::get( JagDBPair &pair, abaxint &index )
{
	JAG_BLURT _diskLock->readLock( -1 );
	JAG_OVER;
	JagDBPair getpair;
	bool rc = exist( pair, &index, getpair );
	if ( rc ) pair = getpair;
	_diskLock->readUnlock( -1 );
	
	return rc;
}

bool JagDiskArrayServer::getWithRange( JagDBPair &pair, abaxint &index )
{
	abaxint first, last;
	JAG_BLURT _memLock->readLock( -1 );
	JAG_OVER;
	bool rc = getFirstLast( pair, first, last );
	if ( rc ) {
		JagDBPair retpair;
		JAG_BLURT _diskLock->readLock( first );
		JAG_OVER;
		rc = findPred( pair, &index, first, last, retpair, NULL );
		_diskLock->readUnlock( first );
	 	_memLock->readUnlock( -1 );
		if ( ! rc ) return get( pair, index );
		else pair = retpair;
	} else {
		_memLock->readUnlock( -1 );
	}
	
	return rc;
}

// set a pair
bool JagDiskArrayServer::set( JagDBPair &pair, abaxint &index )
{
	++_updateUsers;
	JAG_BLURT _reSizeLock->readLock( -1 ); JAG_OVER;
	JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;
	JagDBPair oldpair;
	bool rc = exist( pair, &index, oldpair );
	if ( rc ) raypwrite(_jdfs, pair.value.c_str(), VALLEN, (index*KEYVALLEN)+KEYLEN);
	_diskLock->writeUnlock( -1 );
	_reSizeLock->readUnlock( -1 );
	--_updateUsers;
	
	return rc;
}

// pair: in pair with keys only; out: keys and values of old record
// retpair: keys and new modified values
bool JagDiskArrayServer::setWithRange( const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
						ExprElementNode *root, JagParseParam *parseParam, int numKeys, 
						const JagSchemaAttribute *schAttr, abaxint setposlist[], JagDBPair &retpair )
{
	++_updateUsers;
	abaxint index, first, last;
	JAG_BLURT _reSizeLock->readLock( -1 ); JAG_OVER;
	bool rc = getFirstLast( pair, first, last );
	if ( rc ) {
		JAG_BLURT _diskLock->writeLock( first ); JAG_OVER;
		rc = findPred( pair, &index, first, last, retpair, NULL );
		if ( rc ) {
			pair = retpair;
			rc = checkSetPairCondition( req, pair, (char**)buffers, uniqueAndHasValueCol, root, parseParam, 
										numKeys, schAttr, setposlist, retpair );
			if ( rc ) {
				raypwrite(_jdfs, retpair.value.c_str(), VALLEN, (index*KEYVALLEN)+KEYLEN);
			}
			_diskLock->writeUnlock( first );
		} else {
			_diskLock->writeUnlock( first );
			JAG_BLURT  _diskLock->writeLock( -1 ); JAG_OVER;
			rc = exist( pair, &index, retpair );
			if ( rc ) {
				pair = retpair;
				rc = checkSetPairCondition( req, pair, (char**)buffers, uniqueAndHasValueCol, root, parseParam, 
											numKeys, schAttr, setposlist, retpair );
				if ( rc ) {
					raypwrite(_jdfs, retpair.value.c_str(), VALLEN, (index*KEYVALLEN)+KEYLEN);
				}
			}
			_diskLock->writeUnlock( -1 );
		}
	}
	_reSizeLock->readUnlock( -1 );
	--_updateUsers;
	return rc;
}

bool JagDiskArrayServer::checkSetPairCondition( const JagRequest &req, JagDBPair &pair, char *buffers[], bool uniqueAndHasValueCol, 
						ExprElementNode *root, JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
						abaxint setposlist[], JagDBPair &retpair )
{
	bool rc, needInit = true;
	char *tbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( tbuf, 0, KEYVALLEN+1 );
	ExprElementNode *updroot;
	Jstr errmsg;
	JagFixString strres;
	int typeMode = 0, treelength = 0;
	Jstr treetype = " ";
	const JagSchemaAttribute *attrs[1];
	attrs[0] = schAttr;
	
	memcpy(buffers[0], pair.key.c_str(), KEYLEN);
	memcpy(buffers[0]+KEYLEN, pair.value.c_str(), VALLEN);
	memcpy(tbuf, pair.key.c_str(), KEYLEN);
	memcpy(tbuf+KEYLEN, pair.value.c_str(), VALLEN);
	dbNaturalFormatExchange( buffers[0], numKeys, schAttr ); // db format -> natural format
	dbNaturalFormatExchange( tbuf, numKeys, schAttr ); // db format -> natural format
	if ( !uniqueAndHasValueCol || 
		root->checkFuncValid( NULL, NULL, attrs, (const char **)buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
		for ( int i = 0; i < parseParam->updSetVec.size(); ++i ) {
			updroot = parseParam->updSetVec[i].tree->getRoot();
			needInit = true;
			if ( updroot->checkFuncValid( NULL, NULL, attrs, (const char **)buffers, strres, typeMode, 
										  treetype, treelength, needInit, 0, 0 ) == 1 ) {
				memset(tbuf+schAttr[setposlist[i]].offset, 0, schAttr[setposlist[i]].length);	
				rc = formatOneCol( req.session->timediff, _servobj->servtimediff, tbuf, strres.c_str(), errmsg, 
					parseParam->updSetVec[i].colName, schAttr[setposlist[i]].offset, 
					schAttr[setposlist[i]].length, schAttr[setposlist[i]].sig, schAttr[setposlist[i]].type );
				if ( !rc ) {
					free( tbuf );
					return false;
				}
			} else {
				free( tbuf );
				return false;
			}
		}
		dbNaturalFormatExchange( buffers[0], numKeys, schAttr ); // natural format -> db format
		dbNaturalFormatExchange( tbuf, numKeys, schAttr ); // natural format -> db format
		retpair = JagDBPair( tbuf, KEYLEN, tbuf+KEYLEN, VALLEN );
	} else {
		free( tbuf );
		return false;
	}

	free( tbuf );
	return true;
}

abaxint JagDiskArrayServer::removeMatchKey( const char *kstr, int klen )
{
	JagFixString fixs(kstr, klen);
	JagDBPair pair(fixs);
	Jstr keys =  getListKeys();

	// key1\n
	// key2\n
	// key3\n
	JagStrSplit sp(keys, '\n', true );
	if ( sp.length() < 1 ) return 0;
	abaxint cnt = 0, retind;
	JagDBPair newpair;
	for ( abaxint i = 0; i < sp.length(); ++i ) {
		if ( 0 == strncasecmp( sp[i].c_str(), pair.key.c_str(), pair.key.size() ) ) {
			newpair.point( sp[i].c_str(), sp[i].size() );
			if ( removeData( newpair, &retind ) ) {
				++cnt;
			}
		}
	}

	return cnt;
}

// remove data
bool JagDiskArrayServer::removeData( const JagDBPair &pair, abaxint *retindex )
{
	if ( _arrlen == 0 || _garrlen == 0 ) return 0;
	bool expand;
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;
	++_removeUsers;
	mutex.writeUnlock();
	JAG_BLURT _reSizeLock->readLock( -1 ); JAG_OVER;
	
	//_removeUsers: at here, _removeUsers must be at least 1
	int rc = removeFromRange( pair, retindex );
	if ( rc == -99 ) rc = removeFromAll( pair, retindex );
	--_removeUsers;

	_reSizeLock->readUnlock( -1 );

	return rc;
}

int JagDiskArrayServer::removeFromRange( const JagDBPair &pair, abaxint *retindex )
{
	bool rc;
    abaxint first, last;
    JagDBPair retpair;

	rc = getFirstLast( pair, first, last );
	if ( ! rc ) return -99;

	JAG_BLURT _diskLock->writeLock( first ); JAG_OVER;
	char *diskbuf = (char*)jagmalloc(JAG_BLOCK_SIZE*KEYVALLEN+1);
	memset( diskbuf, 0, JAG_BLOCK_SIZE*KEYVALLEN+1 );
	rc = findPred( pair, retindex, first, last, retpair, diskbuf );
	if ( !rc ) {
		_diskLock->writeUnlock( first );
		return 0;
	}
	abaxint i, j = *retindex;
	JagDBPair npair;
	// update block index when remove
	for ( i = j+1; i <= last; ++i ) {
		if ( *(diskbuf+(i-first)*KEYVALLEN) != '\0' ) break;
	}
	if ( i <= last ) {
		npair = JagDBPair( diskbuf+(i-first)*KEYVALLEN, KEYLEN, true );
	} else {
		i = j;
	}
	i /= JAG_BLOCK_SIZE;
	_blockIndex->deleteIndex( pair, npair, i, false );
	_blockIndex->updateCounter( j, -1, false );
	if ( _elements > 0 ) --_elements;

	char *nullbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( nullbuf, 0, KEYVALLEN+1 );
	raypwrite(_jdfs, nullbuf, KEYVALLEN, (*retindex)*KEYVALLEN);
	
	_diskLock->writeUnlock( first );
	free( nullbuf );
	free( diskbuf );
	return 1;
}

int JagDiskArrayServer::removeFromAll( const JagDBPair &pair, abaxint *retindex )
{
	bool rc;
    abaxint first, last;
	JagDBPair retpair;
	
	JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;
	rc = getFirstLast( pair, first, last );
	if ( ! rc ) {
		_diskLock->writeUnlock( -1 );
		return -99;
	}

	char *diskbuf = (char*)jagmalloc(JAG_BLOCK_SIZE*KEYVALLEN+1);
	memset( diskbuf, 0, JAG_BLOCK_SIZE*KEYVALLEN+1 );
	rc = findPred( pair, retindex, first, last, retpair, diskbuf );
	if ( !rc ) {
		_diskLock->writeUnlock( -1 );
		return 0;
	}

	abaxint i, j = *retindex;
	JagDBPair npair;
	// update block index when remove
	for ( i = j+1; i <= last; ++i ) {
		if ( *(diskbuf+(i-first)*KEYVALLEN) != '\0' ) break;
	}
	if ( i <= last ) {
		npair = JagDBPair( diskbuf+(i-first)*KEYVALLEN, KEYLEN, true );
	} else {
		i = j;
	}
	i /= JAG_BLOCK_SIZE;
	_blockIndex->deleteIndex( pair, npair, i, false );
	_blockIndex->updateCounter( j, -1, false );
	if ( _elements > 0 ) --_elements;

	char *nullbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( nullbuf, 0, KEYVALLEN+1 );
	raypwrite(_jdfs, nullbuf, KEYVALLEN, (*retindex)*KEYVALLEN);
	
	_diskLock->writeUnlock( -1 );
	free( nullbuf );
	free( diskbuf );
	return 1;
}

// Return a list of keys for non-fixstring keys, such as JagNode, UserID class
// where the key is just a Jstr
Jstr JagDiskArrayServer::getListKeys()
{
	Jstr res;
	char *buf = (char*)jagmalloc(KEYVALLEN+1);
	memset(buf, 0, KEYVALLEN+1);
	abaxint rlimit = getBuffReaderWriterMemorySize( _garrlen*KEYVALLEN/1024/1024 );
	JagBuffReader ntr( this, _garrlen, KEYLEN, VALLEN, 0, 0, rlimit );
	while ( ntr.getNext( buf ) ) {
		JagFixString key ( buf, KEYLEN );
		res += Jstr( key.c_str() ) + "\n";
	}
	free( buf );
	return res;
}

void JagDiskArrayServer::print( abaxint start, abaxint end, abaxint limit ) 
{
	if ( start == -1 || start < _nthserv*_arrlen ) {
		start = _nthserv*_arrlen;
	}
	
	if ( end == -1 || end > _nthserv*_arrlen+_arrlen ) {
		end = _nthserv*_arrlen+_arrlen;
	}
	
	if ( _isdarrnew ) {
		start = 0;
		end = _arrlen;
	}
	
	// char  keybuf[ KEYLEN+1 ];
	char  *keybuf = (char*)jagmalloc( KEYLEN+1 );
	char  *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1 );
	prt(("s9999 start=%d, end=%d\n", start, end));
	prt(("************ print() dbobj=%s _arrlen=%lld _garrlen=%lld _elements=%lld KEYVALLEN=%d  KEYLEN=%d\n", 
		_dbobj.c_str(), (abaxint)_arrlen, (abaxint)_garrlen, (abaxint)_elements, KEYVALLEN, KEYLEN ));

	for ( abaxint i = start; i < end; ++i ) {
		memset( keyvalbuf, 0, KEYVALLEN+1 );
		raypread( _jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN );
		memset( keybuf, 0, KEYLEN+1 );
		memcpy( keybuf, keyvalbuf, KEYLEN );
		prt(("%15d   [%s] --> [%s]\n", i, keybuf, keyvalbuf+KEYLEN ));
		if ( limit != -1 && i > limit ) {
			break;
		}
	}
	prt(("\nDone print\n"));
	free( keyvalbuf );
	free( keybuf );
}

bool JagDiskArrayServer::checkFileOrder( const JagRequest &req )
{
	int rc, percnt = 5;
	abaxint ipos;
	Jstr sendmsg = _pdbobj + " check finished ";
   	char keybuf[ KEYLEN+1];
   	char *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1);
	memset( keybuf, 0,  KEYLEN + 1 );
	memset( keyvalbuf, 0,  KEYVALLEN + 1 );
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader br( this, _arrlen, KEYLEN, VALLEN, _nthserv*_arrlen, 0, rlimit );
	while ( br.getNext( keyvalbuf, KEYVALLEN, ipos ) ) { 
		if ( ipos*100/_arrlen >= percnt ) {
			JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
			percnt += 5;
		}
		if ( keybuf[0] == '\0' ) {
			memcpy( keybuf, keyvalbuf, KEYLEN );
		} else {
			rc = memcmp( keyvalbuf, keybuf, KEYLEN );
			if ( rc <= 0 ) {
				free( keybuf );
				free( keyvalbuf );
				return 0;
			} else {
				memcpy( keybuf, keyvalbuf, KEYLEN );
			}
		}
	}
	sendmsg = _pdbobj + " check 100\% complete.";
	JagTable::sendMessage( req, sendmsg.c_str(), "OK" );
	free( keyvalbuf );
	return 1;
}

// important method, to repair incorrect order of diskarray
abaxint JagDiskArrayServer::orderRepair( const JagRequest &req )
{
	if ( _newblockIndex ) { delete _newblockIndex; }
	_newblockIndex = new JagFixBlock( KEYLEN );
	
	int rc, insertCode, percnt=5;
	Jstr sendmsg = _pdbobj + " repair finished ";
	abaxint oldelem = _elements;
	_elements = 0;
	JagDBPair tpair;
	JagFixString data;
	int rc01, rc02, rc12, cursuccess;
	abaxint pos0, pos1, pos2;
	abaxint lastBlock = -1, errorcnt = 0;
   	char *nullbuf = (char*)jagmalloc( KEYVALLEN+1);
   	char *buf0 = (char*)jagmalloc( KEYVALLEN+1);
	char *buf1 = (char*)jagmalloc( KEYVALLEN+1);
	char *buf2 = (char*)jagmalloc( KEYVALLEN+1);
	memset( nullbuf, 0,  KEYVALLEN + 1 );
	memset( buf0, 0,  KEYVALLEN + 1 );
	memset( buf1, 0,  KEYVALLEN + 1 );
	memset( buf2, 0,  KEYVALLEN + 1 );
	Jstr msg = _pdbobj + "_REPAIR";
	JagDataAggregate jda( true );
	jda.setwrite( msg, msg, false );
	jda.setMemoryLimit( _elements*KEYVALLEN*2 );
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader nav( this, _arrlen, KEYLEN, VALLEN, _nthserv*_arrlen, 0, rlimit );
	_minindex = -1;
	while ( 1 ) {
		cursuccess = 0;
		if ( buf0[0] == '\0' ) {
			// get data for buf0 if empty
			rc = nav.getNext( buf0, KEYVALLEN, pos0 );
		}
		if ( rc && buf1[0] == '\0' ) {
			// get data for buf1 if empty
			rc = nav.getNext( buf1, KEYVALLEN, pos1 );
		}
		if ( rc && buf2[0] == '\0' ) {
			// get data for buf2 if empty
			rc = nav.getNext( buf2, KEYVALLEN, pos2 );
		}
		
		if ( rc == 0 ) { 
			// if no more data, break
			break;
		}
		// otherwise, compare three buffers and remove incorrect data
		rc01 = memcmp( buf0, buf1, KEYLEN );
		rc02 = memcmp( buf0, buf2, KEYLEN );
		rc12 = memcmp( buf1, buf2, KEYLEN );

		if ( rc01 < 0 && rc02 < 0 && rc12 < 0 ) {
			// order of 4-5-6, update 4, window to 5-6-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			memcpy( buf0, buf1, KEYVALLEN );
			memcpy( buf1, buf2, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
			pos1 = pos2;
		} else if ( rc01 < 0 && rc02 < 0 && rc12 > 0 ) {
			// order of 4-6-5, update 4 and move 6 to jda, window to 5-?-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			jda.writeit( msg, buf1, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos1*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf2, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos2;
		} else if ( rc01 > 0 && rc02 < 0 && rc12 < 0 ) {
			// order of 5-4-6, move 5 to jda, window to 4-6-?
			jda.writeit( msg, buf0, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memcpy( buf1, buf2, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
			pos1 = pos2;
		} else if ( rc01 < 0 && rc02 > 0 && rc12 > 0 ) {
			// order of 5-6-4, update 5 and move 4 to jda, window to 6-?-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			jda.writeit( msg, buf2, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
		} else if ( rc01 > 0 && rc02 > 0 && rc12 < 0 ) {
			// order of 6-4-5, move 6 to jda, window to 4-5-?
			jda.writeit( msg, buf0, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memcpy( buf1, buf2, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
			pos1 = pos2;
		} else if ( rc01 > 0 && rc02 > 0 && rc12 > 0 ) {
			// order of 6-5-4, move 6 to jda, window to 5-4-?
			jda.writeit( msg, buf0, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memcpy( buf1, buf2, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
			pos1 = pos2;
		} else if ( rc01 < 0 && rc02 < 0 && rc12 == 0 ) {
			// order of 4-5-5, update 4 and delete second 5, window to 5-?-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
		} else if ( rc01 == 0 && rc02 > 0 && rc12 > 0 ) {
			// order of 5-5-4, move first 5 to jda and delete second 5, window to 4-?-?
			jda.writeit( msg, buf0, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos1*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 2;
			memcpy( buf0, buf2, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos2;
		} else if ( rc01 > 0 && rc02 == 0 && rc12 < 0 ) {
			// order of 5-4-5, delete first 5, window to 4-5-?
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memcpy( buf1, buf2, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
			pos1 = pos2;
		} else if ( rc01 > 0 && rc02 > 0 && rc12 == 0 ) {
			// order of 6-5-5, move 6 to jda and delete second 5, window to 5-?-?
			jda.writeit( msg, buf0, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 2;
			memcpy( buf0, buf1, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
		} else if ( rc01 == 0 && rc02 < 0 && rc12 < 0 ) {
			// order of 5-5-6, update first 5 and delete second 5, window to 6-?-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos1*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf2, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos2;
		} else if ( rc01 < 0 && rc02 == 0 && rc12 > 0 ) {
			// order of 5-6-5, update first 5 and delete second 5, window to 6-?-?
			updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 1;
			memcpy( buf0, buf1, KEYVALLEN );
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
			pos0 = pos1;
		} else if ( rc01 == 0 && rc02 == 0 && rc12 == 0 ) {
			// order of 5-5-5, delete second and third 5, window to 5-?-?
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos1*KEYVALLEN);
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			if ( pos2*100/_arrlen >= percnt ) {
				JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
				percnt += 5;
			}
			errorcnt += 2;
			memset( buf1, 0, KEYVALLEN );
			memset( buf2, 0, KEYVALLEN );
		}
	}
	
	// when break, check how many data left in buffers, must have 0, 1 or 2 buffers used; cannot have 3 buffers all full
	// if zero buffers left, ignore and continue
	// if one buffer has value, update it
	// if more than two buffers have value, move all of them to jda to make sure of correct order
	if ( buf0[0] != '\0' && buf1[1] == '\0' && buf2[2] == '\0' ) {
		updateCorrectDataBlockIndex( buf0, pos0, tpair, lastBlock );
	} else if ( buf0[0] != '\0' && buf1[1] != '\0' ) {
		jda.writeit( msg, buf0, KEYVALLEN );
		raypwrite(_jdfs, nullbuf, KEYVALLEN, pos0*KEYVALLEN);
		jda.writeit( msg, buf1, KEYVALLEN );
		raypwrite(_jdfs, nullbuf, KEYVALLEN, pos1*KEYVALLEN);
		errorcnt += 2;
		if ( buf2[2] != '\0' ) {
			jda.writeit( msg, buf2, KEYVALLEN );
			raypwrite(_jdfs, nullbuf, KEYVALLEN, pos2*KEYVALLEN);
			errorcnt += 1;
		}
		if ( pos2*100/_arrlen >= percnt ) {
			JagTable::sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
			percnt += 5;
		}
	} 
	jda.flushwrite();
	// raydebug( stdout, JAG_LOG_LOW, "Built blockindex for (%s)\n", _dbobj.c_str());
	if ( _blockIndex ) delete _blockIndex;
	_blockIndex = _newblockIndex;
	_newblockIndex = NULL;
	_doneIndex = 1;
	free( nullbuf );
	free( buf0 );
	free( buf1 );
	free( buf2 );
	jagmalloc_trim( 0 );
	
	if ( errorcnt > 0 ) {
    	sendmsg =  _pdbobj + " has " + longToStr( errorcnt ) + 
    		" misplaced records in total " + longToStr( oldelem ) + " records. Insert them again ...";
    	prt(("s1723 %s\n", sendmsg.c_str() ));
    
    	sendmsg =  _pdbobj + " misplaced records are being replaced again ...";
    	JagTable::sendMessage( req, sendmsg.c_str(), "OK" );
    	// finish build index and remove error-ordered data, then insert those data again
    	abaxint icnt = 0;
    	while ( jda.readit( data ) ) {
    		JagDBPair pair( data.c_str(), KEYLEN, data.c_str()+KEYLEN, VALLEN, true );
    		rc = insertData( pair, insertCode, true, false, tpair );
    		++icnt;
    	}
    
    	copyAndInsertBufferAndClean();
    	sendmsg = _pdbobj + " " + intToStr( icnt ) + " misplaced records have been inserted in total";
    	prt(("s1724 %s\n", sendmsg.c_str() ));

    	sendmsg = _pdbobj + " misplaced records have all been corrected";
    	JagTable::sendMessage( req, sendmsg.c_str(), "OK" );
	} else {
    	JagTable::sendMessage( req, "All records are in right order. None are replaced", "OK" );
	}

	return errorcnt;
}

void JagDiskArrayServer::updateCorrectDataBlockIndex( char *buf, abaxint pos, JagDBPair &tpair, abaxint &lastBlock )
{
	pos += _nthserv*_arrlen;
	++ _elements;
	_maxindex = pos;
	if ( _minindex < 0 ) _minindex = pos;
	_getPair( buf, KEYLEN, VALLEN, tpair, true );
	_newblockIndex->updateMaxKey( tpair, false );

	if ( ( pos/JagCfg::_BLOCK) != lastBlock ) {
		_newblockIndex->updateIndex( tpair, pos, false, false );
	}
	_newblockIndex->updateCounter( pos, 1, false, false );
	lastBlock = pos/JagCfg::_BLOCK;	
}

// read bottom level of blockindex and write it to a disk file
void JagDiskArrayServer::flushBlockIndexToDisk()
{
	Jstr idxPath = renameFilePath( _filePath, "bid");
	// printf("s3803 JagDiskArrayServer::flushBlockIndexToDisk() _blockIndex=%0x\n", _blockIndex );
	if ( _blockIndex ) {
		_blockIndex->flushBottomLevel( idxPath, _elements, _arrlen, _minindex, _maxindex );
	}
}

void JagDiskArrayServer::removeBlockIndexIndDisk()
{
	Jstr idxPath = renameFilePath( _filePath, "bid");
	jagunlink( idxPath.c_str() );
}

// check to see if need resize
int JagDiskArrayServer::needResize()
{	
	if ( _fileSplitCntLimit > 0 && _elements-_removeUsers >= _fileSplitCntLimit && !_uidORschema ) return 0;
	else if ( ( _arrlen > _fileSplitCntLimit || _arrlen >= _fileMaxCntLimit ) && !_uidORschema ) return 0;
	else if ( 100*(_elements-_removeUsers) > _arrlen * JAG_FILE_RAND_EXPAND ) return 1;
	return 0;
}
