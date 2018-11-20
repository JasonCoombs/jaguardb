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
#include <JagDiskArrayClient.h>
#include <JagHotSpot.h>
#include <JagTime.h>

JagDiskArrayClient::JagDiskArrayClient( const AbaxDataString &fpathname, 
			const JagSchemaRecord *record, bool dropClean, abaxint length )
    : JagDiskArrayBase( fpathname, record, length )
{
	this->init( length, true );
	_isClient = 1;
	_dropClean = dropClean;
	_hasmonitor = 0;
	_pairarr = new PairArray();
}

JagDiskArrayClient::~JagDiskArrayClient()
{
	if ( _dropClean ) {
		drop();
	}
}

// init method
void JagDiskArrayClient::init( abaxint length, bool buildBlockIndex )
{
	int exist;
	KEYLEN = _schemaRecord->keyLength;
	VALLEN = _schemaRecord->valueLength;
	KEYVALLEN = KEYLEN+VALLEN;
	_keyMode = _schemaRecord->getKeyMode(); // get keyMode
	_maxindex = _numOfResizes = _insertUsers = _removeUsers = _garrlen = 0;
	_minindex = -1;
	_arrlen = _elements = _doneIndex = _firstResize = _uidORschema = 0;
	_ups = _downs = 1;
	
	_respartMaxBlock = 100 * 1024 * 1024;
	_respartMaxBlock /= ( KEYVALLEN * JAG_BLOCK_SIZE );
	_GEO = 2;
	
	_filePath = _pathname + ".jdb"; 
	_tmpFilePath =  _pathname + ".jdb.tmp";
	JagStrSplit split( _pathname, '/' );
	exist = split.length();
	if ( exist < 2 ) {
		printf("s7483 error _pathname=%s, exit\n", _pathname.c_str() );
		exit(1);
	}
	_dbname = split[exist-2];
	_objname = split[exist-1];
	_dbobj = _dbname + "." + _objname;

	JagStrSplit split2(_objname, '.');
	if ( split2.length() > 1 ) {
		_pdbobj = _dbname + "." + split2[1];
	} else {
		_pdbobj = _dbobj;
	}

	if ( _dbname == "system" ) {
		_uidORschema = 1;
	}
	
	_lock = new JagReadWriteLock();
	_diskLock = new JagBlockLock();
	_memLock = new JagBlockLock();
	_reSizeLock = new JagBlockLock();
	_darrnewLock = new JagBlockLock();
	_sepThreadResizeLock = new JagBlockLock();


	// _blockIndex = new JagBlock<JagDBPair>();
	_blockIndex = new JagFixBlock(KEYLEN);
	_newblockIndex = NULL;
	_jdfs = new JDFS( NULL, _filePath, KEYVALLEN, length );
	_jdfs2 = NULL;
	
	exist = _jdfs->exist();
	_jdfs->open();
	_arrlen = _jdfs->getArrayLength();
	abaxint garr = _arrlen;
	if ( -1 == garr ) garr = _arrlen;
	_garrlen = garr;

	if ( !exist ) {
		size_t bytes = _arrlen * KEYVALLEN;
		_jdfs->fallocate( 0, bytes );
	} else {
		if ( buildBlockIndex ) buildInitIndex();
	}

}

// build and find first last from the blockIndex
// buildindex from disk array file data
// setup _elements
// force: force to build index from disk array
void JagDiskArrayClient::buildInitIndex( bool force )
{
	// dbg(("s7777 begin buildindex\n"));
	if ( !force && _doneIndex ){
		dbg(("s3820 in buildInitIndex return here\n"));
		return;
	}

	if ( _newblockIndex ) {
		raydebug( stdout, JAG_LOG_LOW, " invalid condition. Someone must be doing resize. abort(%s)\n", _dbobj.c_str());
		// abort();
		// delete _newblockIndex;
		return;
	}
	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	
	_elements = 0;
	JagDBPair tpair;
	abaxint ipos, lastBlock = -1;
   	// char keyvalbuf[ KEYVALLEN+1];
   	char *keyvalbuf = (char*) jagmalloc ( KEYVALLEN+1 );
	memset( keyvalbuf, 0,  KEYVALLEN + 1 );
	
	abaxint nserv = 0;
	
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader nav( this, _arrlen, KEYLEN, VALLEN, nserv*_arrlen, 0, rlimit );
	dbg(("s7777 buildInitIndex offsetpos=%d\n", nserv*_arrlen ));
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

	if ( _blockIndex ) delete _blockIndex;
	_blockIndex = _newblockIndex;
	_newblockIndex = NULL;
	_doneIndex = 1;
	if ( keyvalbuf ) free ( keyvalbuf );
	jagmalloc_trim( 0 );
	// _blockIndex->print();
	// dbg(("s39 %s, elems=%d, arln=%d, garln=%d\n", _filePath.c_str(), (abaxint)_elements, (abaxint)_arrlen, (abaxint)_garrlen ));
	// prt(("s73 _blockIndex minkey=[%s]  maxkey=[%s]\n", _blockIndex->_minKey.key.c_str(),  _blockIndex->_maxKey.key.c_str() ));

}

// drop file
void JagDiskArrayClient::drop()
{
	/***
	JAG_BLURT _memLock->writeLock( -1 );
	JAG_OVER;
	JAG_BLURT _diskLock->writeLock( -1 );
	JAG_OVER;
	_jdfs->remove();
	_diskLock->writeUnlock( -1 );
	_memLock->writeUnlock( -1 );
	***/
	_jdfs->remove();
}

// insertCode: 200 if min-max has changed
int JagDiskArrayClient::_insertData( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doFirstDist, JagDBPair &retpair )
{
	insertCode = 0;
	if ( _arrlen == 0 || _garrlen == 0 ) return 0;

	// first element
	int rc;
	if ( _elements < 1 ) {
		if (  _elements < 1 ) {
			*retindex = 0;

			if ( _keyMode == JAG_RAND || *retindex % ( JAG_FILE_OTHER_EXPAND / 10 + 1 ) == 0 ) {
				*retindex = *retindex + 1;
			}
			_maxindex = *retindex;
			_minindex = *retindex;

			raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, (*retindex)*KEYVALLEN);
    		++_elements;
    		--_insertUsers;
			rc = _blockIndex->updateIndex( pair, 1 );
			_blockIndex->updateCounter( 1, 1, false );

			rc = 1;
			return rc;
		}
	}
	
	if ( _keyMode == JAG_RAND ) rc = insertToRange( pair, retindex, insertCode, false );
	else rc = -99;

	if ( rc == -99 ) rc = insertToAll( pair, retindex, insertCode );
	if ( rc == -99 ) rc = 0;

	if (  needResize() ) {
		reSize( false, -1 );
	}

	return rc;
}

// -1: no resize done
// -2: no resize done
//expand = true for expand; false for shrink, ignore shrink for now
int JagDiskArrayClient::reSize( bool force, abaxint newarrlen )
{
	_newarrlen = _GEO * (abaxint)_garrlen;
	reSizeLocal();
	return 1; 
}

// insert data first to buffer, flush when it is full
int JagDiskArrayClient::insertSync(  JagDBPair &pair )
{
	int rc = _pairarr->insert( pair );
	if ( _pairarr->elements() > 10000 ) {
		flushInsertBufferSync();
		// prt(("c7734 hotspot=%.4f\n", _hotspot->getHotSpot() ));
	}
	return rc;
}


// flush data in _pairarr buffer to disk
// object method flush *_pairarr data pairs
abaxint JagDiskArrayClient::flushInsertBufferSync()
{
	if (  _pairarr->_elements < 1 ) { return 0; }
	abaxint len = _pairarr->size();
	abaxint cnt, retindex;
	int rc, insertCode;
	cnt = 0;
	char *kvbuf = (char*)jagmalloc(KEYVALLEN+1);
	JagDBPair retpair;
	for ( abaxint i=0; i < len; ++i ) {
		if ( _pairarr->isNull(i) ) { continue; }
		JagDBPair &pair = _pairarr->exactAt(i);
		memset(kvbuf, 0, KEYVALLEN+1);
		memcpy(kvbuf, pair.key.c_str(), KEYLEN);
		memcpy(kvbuf+KEYLEN, pair.value.c_str(), VALLEN);
		JagDBPair ipair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN, true );
		ipair.upsertFlag = pair.upsertFlag;
		rc = _insertData( ipair, &retindex, insertCode, false, retpair );
		if ( rc > 0 ) { ++cnt; ++ _writes; }
	}

	if ( len > 0 ) {
		_pairarr->clean();
	}

	free( kvbuf );
	return cnt;
}


// client own resize local
void JagDiskArrayClient::reSizeLocal( )
{
	if ( _newblockIndex ) {
		delete _newblockIndex;
	}

	// _newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	bool half = 0, even = 0;
	JagDBPair tpair;
	abaxint ipos, pos, lastBlock = -1;
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	abaxint numservs;
	abaxint nthserv;
	numservs = 1;
	nthserv = 0;
	
	// for reAlloc
	size_t bytes2 = _newarrlen*KEYVALLEN;
	 _jdfs2 = new JDFS( NULL, _tmpFilePath, KEYVALLEN, _newarrlen );
	_jdfs2->open();
	_jdfs2->fallocate( 0, bytes2 );
	
	if ( _elements > 0 ) {
		abaxint actualcnt = 0, whole = 0;
		double ratio = 0.0;
		int resizeHotSpotMode = _hotspot->getResizeMode();
		int ascSpareGap = JAG_FILE_OTHER_EXPAND/10;
		// if ( !_isdarrnew ) { prt(("s2203 darr resizeMode=[%d] this=%0x  thrd=%lld\n", resizeHotSpotMode, this, THREADID )); }
		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING || resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) {
			whole = 2;
			ratio = 0.0;
		} else if ( resizeHotSpotMode == JAG_STRICT_RANDOM ) {
			whole = _newarrlen*JAG_RANDOM_RESIZE_PERCENT / _elements; // 64/18 = 3  avg spacing
			ratio = _newarrlen*JAG_RANDOM_RESIZE_PERCENT /_elements;  // 64/18 = 3.555
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_SMALL ) {
			whole = _newarrlen*JAG_CYCLERIGNT_SMALL_PERC / _elements; // 64/18 = 3  avg spacing
			ratio = _newarrlen*JAG_CYCLERIGNT_SMALL_PERC /_elements;  // 64/18 = 3.555
		} else if ( resizeHotSpotMode == JAG_CYCLE_RIGHT_LARGE ) {
			whole = _newarrlen*JAG_CYCLERIGNT_LARGE_PERC / _elements; // 64/18 = 3  avg spacing
			ratio = _newarrlen*JAG_CYCLERIGNT_LARGE_PERC /_elements;  // 64/18 = 3.555
		}
		if ( ratio-whole >= 0.5 ) half = 1;

		pos = nthserv*_newarrlen + 1;
		if ( resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) pos = (nthserv+1)*_newarrlen-2*_elements;

		// for reDist	
		abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
		JagBuffReader br( this, _arrlen, KEYLEN, VALLEN, nthserv*_arrlen, 0, rlimit );
		// prt(("s9999 read _pathname=[%s] _arrlen=[%lld], offset=[%lld]\n", _pathname.c_str(), (abaxint)_arrlen, nthserv*_arrlen ));
		abaxint wlimit = getBuffReaderWriterMemorySize( _newarrlen*KEYVALLEN/1024/1024 );
		JagBuffWriter bw( _jdfs2, KEYVALLEN, nthserv*_newarrlen*KEYVALLEN, wlimit );

		abaxint larrlen = _newarrlen;
		abaxint lgarrlen = (abaxint)_arrlen;
		_minindex = -1;
		// prt(("s9999 larrlen=%lld, lgarrlen=%lld, _arrlen=%lld, _garrlen=%lld, dbobj=[%s]\n", 
				// larrlen, lgarrlen, (abaxint)_arrlen, (abaxint)_garrlen, _dbobj.c_str()));
		while ( br.getNext( keyvalbuf ) ) {
			// printf("s9999 keyvalbuf=[%s] pos=[%d]\n", keyvalbuf, pos);
			++actualcnt;
			ipos = pos;
			bw.writeit( pos, keyvalbuf, KEYVALLEN );
			if ( resizeHotSpotMode == JAG_STRICT_ASCENDING || resizeHotSpotMode == JAG_STRICT_ASCENDING_LEFT ) {
				pos += getStep( JAG_ASC, pos, whole, half, even );
			} else {
				pos += getStep( JAG_RAND, pos, whole, half, even );
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

		_hotspot->clean();
		_hotspot->setMaxPair( tpair );  

		bw.flushBuffer();
		_arrlen = _newarrlen;
		_garrlen = (abaxint)_arrlen;
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
	// prt(("s9999 after delete jdfs _pathname=[%s]\n", _pathname.c_str()));
	// fsetXattr( _jdfs->getFD(), _filePath.c_str(), "user.garrlen", _garrlen );

	//prt(("s7124 after resizelocal printRange()...\n"));
	//print( -1, -1, 1000 );
	free( keyvalbuf );
	_shifts = 0;
	// prt(("s1283  resize local _shifts = 0 \n" ));
}

// check to see if need resize
int JagDiskArrayClient::needResize()
{	
	if ( 100*(_elements-_removeUsers) > _arrlen * JAG_FILE_RAND_EXPAND ) return 1;
	return 0;
}
