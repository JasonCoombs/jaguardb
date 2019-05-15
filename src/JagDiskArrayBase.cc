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
#include <JagDiskArrayBase.h>
#include <JagHashLock.h>
#include <JagHotSpot.h>
#include <JagBuffReader.h>
#include <JagBuffBackReader.h>
#include <JagSingleBuffWriter.h>

// ctor and dtors
// ctor
// filePathName: /home/yzhj/jaguar/data/test/jinhua  no .jdb part
// User by server only
JagDiskArrayBase::JagDiskArrayBase( const JagDBServer *servobj, const Jstr &filePathName, 
	const JagSchemaRecord *record, bool buildInitIndex, abaxint length, bool noMonitor ) : _schemaRecord(record)
{
	_servobj = (JagDBServer*)servobj;
	_pathname = filePathName;
	_fulls = 0;
	_partials = 0;
	_isdarrnew = 0;
	_reads = _writes = _dupwrites = _upserts = 0;
	_insdircnt = _insmrgcnt = 0;
	_lastSyncTime = 0;
	_lastSyncOneTime = 0;
	_isClient = 0;
	_pairarr = NULL;
	_pairarrcopy = NULL;
	_pairmap = NULL;
	_pairmapcopy = NULL;
	_monitordone = 1;
	_hasmonitor = 0;
	_isSchema = 0;
	_isUserID = 0;
	_shifts = 0;
	_isFlushing = 0;
	_nthserv = 0;
	_numservs = 1;
	_lastResizeMode = 0;
	_isChildFile = 0;
	_hasdarrnew = false;
	_fileCompress = false;
	_noFlush = false;
	_mergeLimit = atoll(_servobj->_cfg->getValue("MAX_RESIZEMERGE_LIMIT", "2048").c_str()) * 1024 * 1024;
	_familyInsPairCnt = 0;
	_fileSplitCntLimit = 0;
	_fileMaxCntLimit = 0;
	_forceSleepTime = 0;
	_callCounts = 0;
	_lastBytes = 0;
	_hotspot = new JagHotSpot<JagDBPair>();

	pthread_mutex_init( &_insertBufferMutex, NULL );
	pthread_cond_init(& _insertBufferMoreThanMinCond, NULL);
	pthread_cond_init(& _insertBufferLessThanMaxCond, NULL);
	pthread_mutex_init( &_insertBufferCopyMutex, NULL );
}

// another ctor for darrnew use only
// filePathName: /home/yzhj/jaguar/data/test/jinhua  no .jdb part
// User by server only
JagDiskArrayBase::JagDiskArrayBase( const JagDBServer *servobj, const Jstr &filePathName, 
	const JagSchemaRecord *record, const Jstr &pdbobj, JagDBPair &minpair, JagDBPair &maxpair ) 
	: _schemaRecord(record)
{
	_servobj = (JagDBServer*)servobj;
	_pathname = filePathName;
	_fulls = 0;
	_partials = 0;
	_isdarrnew = 1;
	_pdbobj = pdbobj;
	_isClient = 0;
	_pairarr = NULL;
	_pairarrcopy = NULL;
	_pairmap = NULL;
	_pairmapcopy = NULL;
	_isSchema = 0;
	_isUserID = 0;
	_shifts = 0;
	_isFlushing = 0;
	_nthserv = 0;
	_numservs = 1;
	_lastResizeMode = 0;
	_isChildFile = 0;
	_hasdarrnew = false;
	_fileCompress = false;
	_noFlush = false;
	_mergeLimit = atoll(_servobj->_cfg->getValue("MAX_RESIZEMERGE_LIMIT", "2048").c_str()) * 1024 * 1024;
	_familyInsPairCnt = 0;
	_fileSplitCntLimit = 0;
	_fileMaxCntLimit = 0;
	_forceSleepTime = 0;
	_callCounts = 0;
	_lastBytes = 0;
	_hotspot = new JagHotSpot<JagDBPair>();

	pthread_mutex_init( &_insertBufferMutex, NULL );
	pthread_cond_init(& _insertBufferMoreThanMinCond, NULL);
	pthread_cond_init(& _insertBufferLessThanMaxCond, NULL);
	pthread_mutex_init( &_insertBufferCopyMutex, NULL );
	
}

// client use only
JagDiskArrayBase::JagDiskArrayBase( const Jstr &filePathName, 
				const JagSchemaRecord *record, abaxint length ) 
    :  _schemaRecord(record)
{	
	_servobj = NULL;
	_pathname = filePathName;
	_fulls = 0;
	_partials = 0;
	_isdarrnew = 0;
	// _ups = _downs = 1;
	_reads = _writes = _dupwrites = _upserts = 0;
	_insdircnt = _insmrgcnt = 0;
	_lastSyncTime = 0;
	_lastSyncOneTime = 0;
	_isClient = 1;
	_pairarr = NULL;
	_pairarrcopy = NULL;
	_pairmap = NULL;
	_pairmapcopy = NULL;
	_monitordone = 1;
	_hasmonitor = 0;
	_isSchema = 0;
	_isUserID = 0;
	_shifts = 0;
	_isFlushing = 0;
	_nthserv = 0;
	_numservs = 1;
	_lastResizeMode = 0;
	_isChildFile = 0;
	_hasdarrnew = false;
	_fileCompress = false;
	_noFlush = false;
	_mergeLimit = 512 * 1024 * 1024;
	_familyInsPairCnt = 0;
	_fileSplitCntLimit = 0;
	_fileMaxCntLimit = 0;
	_forceSleepTime = 0;
	_callCounts = 0;
	_lastBytes = 0;
	_hotspot = new JagHotSpot<JagDBPair>();

	pthread_mutex_init( &_insertBufferMutex, NULL );
	pthread_cond_init(& _insertBufferMoreThanMinCond, NULL);
	pthread_cond_init(& _insertBufferLessThanMaxCond, NULL);
	pthread_mutex_init( &_insertBufferCopyMutex, NULL );

}

// dtor
JagDiskArrayBase::~JagDiskArrayBase()
{
	destroy();

	pthread_mutex_destroy( &_insertBufferMutex );
	pthread_cond_destroy( &_insertBufferMoreThanMinCond );
	pthread_cond_destroy( &_insertBufferLessThanMaxCond );
	pthread_mutex_destroy( &_insertBufferCopyMutex );
}

void JagDiskArrayBase::initMonitor()
{
	// setup monitor thread
	_pairarr = new PairArray();
	_pairarrcopy = new PairArray();
	_pairmap = new JagDBMap();
	_pairmapcopy = new JagDBMap();
	_sessionactive = 1;
	_hasmonitor = 1;

	// pthread_t  threadmo;
	// jagpthread_create( &_threadmo, NULL, monitorCommandsMem, (void*)this);
}

void JagDiskArrayBase::destroyMonitor()
{
	// prt(("s4408 destroyMonitor ...\n" ));
	jaguar_mutex_lock( & _insertBufferMutex );
	jaguar_mutex_lock( &(_insertBufferCopyMutex ) );
	// prt(("s4418 delete _pairarr/_pairarrcopy/_pairmap/_pairmapcopy ...\n" ));
	if ( _pairarr ) delete _pairarr; _pairarr = NULL;

	if ( _pairarrcopy ) delete _pairarrcopy; _pairarrcopy=NULL;

	if ( _pairmap ) delete _pairmap; _pairmap=NULL;

	if ( _pairmapcopy ) delete _pairmapcopy; _pairmapcopy = NULL;

	jaguar_mutex_unlock( &(_insertBufferCopyMutex ) );
	jaguar_mutex_unlock( &(_insertBufferMutex ) );
}

void JagDiskArrayBase::destroy()
{
	if ( _lock ) {
		delete _lock;
	}
	_lock = NULL;

	if ( _diskLock ) {
		delete _diskLock;
	}
	_diskLock = NULL;

	if ( _memLock ) {
		delete _memLock;
	}
	_memLock = NULL;

	if ( _reSizeLock ) {
		delete _reSizeLock;
	}
	_reSizeLock = NULL;
	
	if ( _darrnewLock ) {
		delete _darrnewLock;
	}
	_darrnewLock = NULL;
	
	if ( _sepThreadResizeLock ) {
		delete _sepThreadResizeLock;
	}
	_sepThreadResizeLock = NULL;
	
	if ( _blockIndex ) {
		delete _blockIndex;
	}
	_blockIndex = NULL;
	
	if ( _newblockIndex ) {
		delete _newblockIndex;
	}
	_newblockIndex = NULL;
	
	if ( _jdfs ) {
		delete _jdfs;
	}
	_jdfs = NULL;
	
	if ( _jdfs2 ) {
		delete _jdfs2;
	}
	_jdfs2 = NULL;

	if ( _pairarr && !_isClient ) {
		destroyMonitor();
	}

	if ( _hotspot ) {
		delete _hotspot;
		_hotspot = NULL;
	}
}

// simple get attributes methods
// get DBPair for KV both or key only
void JagDiskArrayBase::_getPair( char buffer[], int keylength, int vallength, JagDBPair &pair, bool keyonly ) const 
{
	if (buffer[0] != '\0') {
		if ( keyonly ) {
			pair = JagDBPair( buffer, keylength );
		} else {
			pair = JagDBPair( buffer, keylength, buffer+keylength, vallength );
		}
	} else {
		JagDBPair temp_pair;
		pair = temp_pair;
	}
}

bool JagDiskArrayBase::getFirstLast( const JagDBPair &pair, abaxint &first, abaxint &last )
{
	bool rc;
	rc = _blockIndex->findFirstLast( pair, &first, &last );

	if ( ! rc ) {
		// dbg(("s8392 _blockIndex->findFirstLast return false\n"));
		return false;
	}

	last = first + JagCfg::_BLOCK - 1;
	return true;
}

abaxint JagDiskArrayBase::getRealLast()
{
	abaxint last = _blockIndex->findRealLast();
	last += JagCfg::_BLOCK - 1;
	return last;
}

// equal element or predecessor of pair
bool JagDiskArrayBase::findPred( const JagDBPair &pair, abaxint *index, abaxint first, abaxint last, JagDBPair &retpair, char *diskbuf )
{
	// prt(("s4112 _pathname=[%s] kvlen=%d first=%d _elemts=%d\n", _pathname.c_str(), KEYVALLEN, first, (abaxint)_elements ));
	bool found = 0;

	*index = -1;
	if ( _elements == 0 ) {
		return 0; //  not found
	}

	if ( _elements == 1 ) {
		abaxint pos = -1;
		JagDBPair tmp_pair;
		if ( diskbuf ) {
			raypread(_jdfs, diskbuf, JAG_BLOCK_SIZE*KEYVALLEN, first*KEYVALLEN);
			for ( int i = 0; i < JAG_BLOCK_SIZE; ++i ) {
				if ( *(diskbuf+i*KEYVALLEN) != '\0' ) {
					_getPair( diskbuf+i*KEYVALLEN, KEYLEN, VALLEN, tmp_pair, false );
					pos = first+i;
					break;
				}
			}
		} else {
			ssize_t sz;
			char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
			for ( abaxint i = first; i <= last; ++i ) {
				if ( raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN) > 0 && keyvalbuf[0] != '\0' ) {
					_getPair( keyvalbuf, KEYLEN, VALLEN, tmp_pair, false );
					pos = i;
					break;
				}
			}
			free( keyvalbuf );
		}
		if ( pos >= 0 ) {
			if ( tmp_pair == pair ) {
				found = 1;
				*index = pos;
				retpair = tmp_pair;
			} else if ( tmp_pair < pair ) {
				*index = pos;
			} else {
				*index = pos-1;
			}
		} else {
			// raydebug( stdout, JAG_LOG_LOW, "s5209 thread=%l cannot find elements in first last range %d - %d _elements=1 pair=[%s]\n",
			// THREADID, _filePath.c_str(), first, last, pair.key.c_str() );
		}
		return found;
	}

    JagDBPair arr[JagCfg::_BLOCK];
    JagFixString key, val;
	if (  (KEYVALLEN <= 10 * KEYLEN) || diskbuf ) {
		// if diskbuf !=NULL it is for write data later
		if ( diskbuf ) {
        	memset( diskbuf, 0, JagCfg::_BLOCK*KEYVALLEN+1 );
        	raypread(_jdfs, diskbuf, (JagCfg::_BLOCK)*KEYVALLEN, first*KEYVALLEN);
        	for (int i = 0; i < JagCfg::_BLOCK; ++i ) {
        		key.point( diskbuf+i*KEYVALLEN, KEYLEN );
        		val.point( diskbuf+i*KEYVALLEN+KEYLEN, VALLEN );
        		arr[i].point( key, val );
        	}
			found = binSearchPred( pair, index, arr, JagCfg::_BLOCK, 0, JagCfg::_BLOCK-1 );
			if ( *index != -1 ) retpair = arr[*index];
			else retpair = arr[0];
			*index += first;
			return found;        
		} else {
        	char *ldiskbuf = (char*)jagmalloc(JagCfg::_BLOCK*KEYVALLEN+1);
        	memset( ldiskbuf, 0, JagCfg::_BLOCK*KEYVALLEN+1 );
        	raypread(_jdfs, ldiskbuf, (JagCfg::_BLOCK)*KEYVALLEN, first*KEYVALLEN);
        	for (int i = 0; i < JagCfg::_BLOCK; ++i ) {
        		key.point( ldiskbuf+i*KEYVALLEN, KEYLEN );
        		val.point( ldiskbuf+i*KEYVALLEN+KEYLEN, VALLEN );
        		arr[i].point( key, val );
        	}
			found = binSearchPred( pair, index, arr, JagCfg::_BLOCK, 0, JagCfg::_BLOCK-1 );
			if ( *index != -1 ) retpair = arr[*index];
			else retpair = arr[0];
			*index += first;
			free( ldiskbuf );
			return found;
		}
	} else {
    	// char ldiskbuf[JagCfg::_BLOCK*(KEYLEN+1) + 1];
    	char *ldiskbuf = (char*)jagmalloc(JagCfg::_BLOCK*(KEYLEN+1)+1);
    	memset( ldiskbuf, 0, JagCfg::_BLOCK*(KEYLEN+1) + 1 );
    	for (int i = 0; i < JagCfg::_BLOCK; ++i ) {
    		raypread(_jdfs, ldiskbuf+i*KEYLEN, KEYLEN, (first+i)*KEYVALLEN);
        	arr[i].point( ldiskbuf+i*KEYLEN, KEYLEN );
    	}
    	found = binSearchPred( pair, index, arr, JagCfg::_BLOCK, 0, JagCfg::_BLOCK-1 );
		char *kvbuf = (char*)jagmalloc(KEYVALLEN+1);
    	memset( kvbuf, 0, KEYVALLEN+1 );
    	if ( *index != -1 ) {
			raypread(_jdfs, kvbuf, KEYVALLEN, (first+*index)*KEYVALLEN );
		} else {
			raypread(_jdfs, kvbuf, KEYVALLEN, first*KEYVALLEN );
		}
		retpair = JagDBPair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN );
    	*index += first;
		free( kvbuf );
		free( ldiskbuf );
    	return found;
	}

}

// insert data
// check insertToRange is save or not
// if diskbuf full or relative left/right/pos may out of bound, return false
// else, can do insertToRange
int JagDiskArrayBase::checkSafe( char *diskbuf, abaxint index, abaxint first, abaxint last, abaxint &left, abaxint &right,
	int &relpos, int &relleft, int &relright, bool &leftfound, bool &rightfound )
{
	int max = JagCfg::_BLOCK;
	relpos = index - first;
	if ( relpos < 0 ) relpos = 0;
	relright = relleft = relpos;

	abaxint nth = _nthserv;
	if ( !_isdarrnew && first >= nth*_arrlen+_arrlen ) {
		return 0;
	}
	
	if ( relpos < 0 || relpos >= max ) {
		return 0;
	}

	if ( index >= 0 && *(diskbuf+relpos*KEYVALLEN) == '\0' ) {
		left = right = relleft = relright = -1;
		return 1;
	}

	// otherwise, find pos in diskbuf
	while ( 1 ) {
		if ( relleft == 0 && relright == max-1 ) {
			++ _fulls;
			return -1;
		}
		if ( relleft > 0 ) --relleft;
		if ( relright < max-1 ) ++relright;
		
		// find right first
		if ( *(diskbuf+relright*KEYVALLEN) == '\0' ) {
			rightfound = true;
			break;
		}

		// else find left
		if ( *(diskbuf+relleft*KEYVALLEN) == '\0' ) {
			leftfound = true;
			break;
		}
	}
	
	if ( rightfound && ( relpos+1 < 0 || relpos+1 >= max) ) {	
		return 0;
	}
	// find blank space in diskbuf
	left = relleft + first;
	right = relright + first;
	return 1;
}

// push pair to buffer
int JagDiskArrayBase::pushToBuffer( JagDBPair &pair )
{
	if ( _isClient ) {
		return 1;
	}

	int rc;
	jaguar_mutex_lock( &_insertBufferMutex );
	
	if ( _monitordone ) {
		_monitordone = 0;
		pthread_t  threadmo;
		jagpthread_create( &threadmo, NULL, monitorCommandsMem, (void*)this);
		pthread_detach( threadmo );
	}

	while ( _pairmap->elements()*KEYVALLEN > (_lastBytes=availableMemory( _callCounts, _lastBytes ))/16 && !_resizeUsers ) {
		jaguar_cond_wait( &_insertBufferLessThanMaxCond, &_insertBufferMutex );
	}

	rc = _pairmap->insert( pair );

	if ( !rc ) ++_dupwrites;

	rc = jaguar_cond_signal( &_insertBufferMoreThanMinCond );
	if ( rc != 0 ) {
		prt(("s6629 jaguar_cond_signal error rc=%d\n", rc ));
	}
	jaguar_mutex_unlock( &_insertBufferMutex );		
	return 1;
}

// insertData: direct or save to buffer
int JagDiskArrayBase::insertData( JagDBPair &pair, int &insertCode, bool doFirstDist, bool direct, JagDBPair &retpair )
{
	++_insertUsers;
	abaxint rc, retindex;
	char *kvbuf = (char*)jagmalloc( KEYVALLEN+1 );
	memset(kvbuf, 0, KEYVALLEN+1);
	memcpy(kvbuf, pair.key.c_str(), KEYLEN);
	memcpy(kvbuf+KEYLEN, pair.value.c_str(), VALLEN);
	JagDBPair ipair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN, true );
	ipair.upsertFlag = pair.upsertFlag;

	abaxint first, last, mid, reallast;
	JAG_BLURT _memLock->readLock( -1 ); JAG_OVER;
	rc = getFirstLast( ipair, first, last );
	if ( rc ) {
		mid = ( first + last ) / 2;
	} else {
		mid = _nthserv*_arrlen;
	}
	reallast = getRealLast();
	if ( ! _isdarrnew ) {
		_hotspot->insert( JagTime::utime(), (double)(mid)/(double)reallast, ipair );
		_hotspot->updateUpsAndDowns( ipair );
	}
	_memLock->readUnlock( -1 );

	if ( !_hasmonitor || _uidORschema ) {
		rc =  _insertData( ipair, &retindex, insertCode, doFirstDist, retpair );
		if ( rc ) ++ _writes;
		free( kvbuf );
		--_insertUsers;
		return rc;
	} else {
		// put into insertBuffer
		// prt(("s3002 pushToBuffer(%s) ...\n", pair.key.c_str() ));
		pushToBuffer( ipair );
		// prt(("s3003 pushToBuffer(%s) done\n", pair.key.c_str() ));
	}

	// debug
	// prt(("s3829 bottomCapacity()=%lld topLevel=%d\n", _blockIndex->getBottomCapacity(), _blockIndex->getTopLevel()  ));

	free( kvbuf );
	--_insertUsers;
	return 1;
}

int JagDiskArrayBase::updateOldPair( JagDBPair &pair )
{
}	

// return  -99 unable to insert, need to insertToAll
//         0: data already exists, not inserted
//         22: upsert was performed successfully
//         1: insert success
int JagDiskArrayBase::insertToRange( JagDBPair &pair, abaxint *retindex, int &insertCode, bool doresizePartial )
{

	int rc, relpos, relleft, relright, shiftpos = 0, max = JagCfg::_BLOCK;
	bool changerc = false, leftfound = false, rightfound = false, allempty = true;
	abaxint i, j, index, left, right, first, last;
	JagDBPair tpair;

	insertCode = 0;

	abaxint localmin, localmax;
	localmin = _nthserv*_arrlen;
	localmax = localmin + _arrlen;
	
	JAG_BLURT _diskLock->readLock( -1 );
	JAG_OVER;
	rc = getFirstLast( pair, first, last );
	if ( ! rc ) {
		// printf("s9999 insertToRange firstlast -99\n");
		_diskLock->readUnlock( -1 );
		return -99;
	}
	_diskLock->readUnlock( -1 );

	JAG_BLURT _diskLock->writeLock( first ); JAG_OVER;

	char *diskbuf = (char*)jagmalloc(max*KEYVALLEN+1);
	memset( diskbuf, 0, max*KEYVALLEN+1 );
	rc = findPred( pair, &index, first, last, tpair, diskbuf );
	// printf("s99999999 %lld diskbuf=[%s] max=%d, kvlen=%d\n", pair.upsertFlag, diskbuf, max, KEYVALLEN);
	if ( rc ) {
		*retindex = index;
		if ( pair.upsertFlag == 1 ) {
			raypwrite(_jdfs, pair.key.c_str()+KEYLEN, VALLEN, index*KEYVALLEN+KEYLEN);
			pair.value = tpair.value;
			_diskLock->writeUnlock( first );
			++ _upserts;
			free( diskbuf );
			if ( index > _maxindex ) _maxindex = index;
			if ( index < _minindex ) _minindex = index;
			return 22; // upsert and update
		}
		// printf("s9999 insertToRange exist 0\n");
		++_dupwrites;
		_diskLock->writeUnlock( first );
		free( diskbuf );
		return 0;
	}
	
	if ( index >= _maxindex && index < localmax-1 ) { 
		// if new index is greater than or equal to maxindex, no need to check and do resize partial
		// if index+1 is not out of bound
		++index;
		if ( _lastResizeMode == JAG_STRICT_ASCENDING ||_lastResizeMode == JAG_CYCLE_RIGHT_LARGE 
					|| _lastResizeMode == JAG_CYCLE_RIGHT_SMALL ) {
			if ( index % (JAG_STRICT_CYCLERIGHT_RESIZE_GAP+1) == 0 && index < localmax-1 ) {
				++index;
			}
		}
		j = index;
		if ( ((j-1)/JagCfg::_BLOCK) != (j/JagCfg::_BLOCK) && ( j % JagCfg::_BLOCK == 0 || j % JagCfg::_BLOCK == 1 ) ) {
			_blockIndex->updateCounter( j, 0, true );
		}
		left = right = relleft = relright = -1;
	} else if ( index < _minindex && _minindex >= localmin+2 ) {
		// if new index is smaller than minindex and minindex is larger or equal to the second position of arrlen
		// write it to first position of arrlen
		index = localmin + 1;
		if ( _lastResizeMode == JAG_STRICT_ASCENDING || _lastResizeMode == JAG_STRICT_ASCENDING_LEFT || 
					_lastResizeMode == JAG_CYCLE_RIGHT_LARGE || _lastResizeMode == JAG_CYCLE_RIGHT_SMALL ) {
			if ( index % (JAG_STRICT_CYCLERIGHT_RESIZE_GAP+1) == 0 && index < _minindex-1 ) {
				++index;
			}
		}
		j = index;
		if ( ((j-1)/JagCfg::_BLOCK) != (j/JagCfg::_BLOCK) && ( j % JagCfg::_BLOCK == 0 || j % JagCfg::_BLOCK == 1 ) ) {
			_blockIndex->updateCounter( j, 0, true );
		}
		left = right = relleft = relright = -1;
	} else {
		rc = checkSafe( diskbuf, index, first, last, left, right, relpos, relleft, relright, leftfound, rightfound );
		// printf("s4031 thrd=%lld  checkSafe rc=%d index=%lld\n", THREADID, rc, index );
		if ( rc == 0 ) {
			// printf("s9999 insertToRange not safe -99, pair=[%s]\n", pair.key.c_str());
			_diskLock->writeUnlock( first );
			free( diskbuf );
			return -99;
		} else if ( rc == -1 ) { // resizepartial
			_diskLock->writeUnlock( first );
			if ( doresizePartial ) {
				// printf("s2010 thrd=%lld doresizePartial index=%lld first=%lld ...\n", THREADID, index, first );
				// fflush( stdout );
				JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;
				if ( !_uidORschema ) rc = insertMerge( 2, 1, NULL, pair.key.c_str(), NULL, NULL );
				else rc = 0;
				_diskLock->writeUnlock( -1 );
				if ( rc ) {
					// printf("s4032 backto insertToRange\n");
					free( diskbuf );
					// return insertToRange( pair, retindex, insertCode, 0 );
					return 1;
				} else {
					free( diskbuf );
					return -99;
				}
			} else {
				// printf("s2011 thrd=%lld NOT doresizePartial\n", THREADID );
				// fflush( stdout );
				free( diskbuf );
				return -99;
			}
		}
	}

	/***
	// printf("s99999999 index=[%d]\n", index);
	if ( _keyMode != JAG_RAND 
		  && (index+1) > _maxindex 
		  && ( (index+1) % ( JAG_FILE_OTHER_EXPAND / 10 + 1 ) == 0 ) 
		  && index+2 < localmax ) {
		  index = index+2;
    }
	***/

	// index pos is blank
	if ( left == -1 && right == -1 && relleft == -1 && relright == -1 ) {
		if ( index < localmin || index+1 > localmin+_arrlen ) {
			raydebug( stdout, JAG_LOG_LOW, 
					  "s7792 [%s] pwrite find pos out of server bound localmin=%l, _arrlen=%l, index=%l\n", 
					  _dbobj.c_str(), localmin, (abaxint)_arrlen, index );
			// abort();
			_diskLock->writeUnlock( first );
			free( diskbuf );
			return 0;
		}
		/***
		char kvbuf[KEYVALLEN+1];
		raypread(_jdfs, kvbuf, KEYVALLEN, index*KEYVALLEN);
		if ( *kvbuf != '\0' ) {
			prt(("s9999 error not empty\n"));
			abort();
		}
		***/
		raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, index*KEYVALLEN);	
		if ( index > _maxindex ) _maxindex = index;
		if ( index < _minindex ) _minindex = index;
		j = index;
		changerc = _blockIndex->updateIndex( pair, j ) || changerc;
		*retindex = index;
	} else {
		if ( leftfound ) {
			// printf("c4101 leftshift shiftleft [%d %d]  <-- [%d %d]\n", left, index-1, left+1, index );
			if ( index - left > 0 ) {
				memmove(diskbuf+relleft*KEYVALLEN, diskbuf+(relleft+1)*KEYVALLEN, (index-left)*KEYVALLEN);
				if ( relleft < 0 || relleft+1+index-left > JAG_BLOCK_SIZE ) {
					raydebug( stdout, JAG_LOG_LOW, 
							  "s7890 [%s] memmove left shift out of bound index=%l, relleft=%d, left=%l\n", 
							  _dbobj.c_str(), index, relleft, left);
					_diskLock->writeUnlock( first );
					free( diskbuf );
					return 0;
					// abort();
				}
			}

			for ( i = left; i <= index-1; ++i) {
				j = i;
				_getPair( diskbuf+(relleft+shiftpos)*KEYVALLEN, KEYLEN, VALLEN, tpair, true );
				if ( j%max == 0 ) {
					changerc = _blockIndex->updateIndex(tpair, j, true) || changerc;
				} else if ( j%max == max-1 ) {
					changerc = _blockIndex->updateIndex(tpair, j) || changerc;
				}
				++shiftpos;
			}

			// if N*0 -- left-1 all empty, update at left
			// code ok
			if ( left%max != 0 ) {
				for ( i = relleft-1; i>=0; --i) {
					if ( *(diskbuf+i*KEYVALLEN) != '\0' ) {
						allempty = false;
						break;
					}
				}
				if ( allempty ) {
					_getPair( diskbuf+relleft*KEYVALLEN, KEYLEN, VALLEN, tpair, true );
					j = left;
					changerc = _blockIndex->updateIndex(tpair, j) || changerc;
				}
			}

			if ( index < 0 ) ++index;	// should not happen
			memcpy(diskbuf+relpos*KEYVALLEN, pair.key.c_str(), KEYVALLEN);
			// memcpy(diskbuf+relpos*KEYVALLEN, pair.key.c_str(), KEYLEN);
			// memcpy(diskbuf+relpos*KEYVALLEN+KEYLEN, pair.value.c_str(), VALLEN);
			if ( left < localmin || relleft+index-left+1 > JAG_BLOCK_SIZE || left+index-left+1 > localmin+_arrlen ) {
				raydebug( stdout, JAG_LOG_LOW, 
						  "s7882 [%s] pwrite left shift out of serv bound left=%l, locmin=%l, relleft=%d, index=%l, arrlen=%l\n", 
						  _dbobj.c_str(), left, localmin, relleft, index, (abaxint)_arrlen );
				// abort();
				_diskLock->writeUnlock( first );
				free( diskbuf );
				return 0;
			}
			raypwrite(_jdfs, diskbuf+relleft*KEYVALLEN, (index-left+1)*KEYVALLEN, left*KEYVALLEN);
			if ( index > _maxindex ) _maxindex = index;
			if ( index < _minindex ) _minindex = index;
			j = *retindex = index;
			if ( j%max == 0 ) {
				changerc = _blockIndex->updateIndex(pair, j, true) || changerc;
			} else {
				changerc = _blockIndex->updateIndex(pair, j) || changerc;
			}
		} else if ( rightfound ) {
			// printf("c4402 rightshift shiftright [%d %d]  --> [%d %d]\n", index+1, right-1, index+2, right );
			// if ( index < localmin ) relpos = -1; // may have bug
			if ( index < first ) relpos = -1;  
			if ( right - index > 1 ) {
				memmove(diskbuf+(relpos+2)*KEYVALLEN, diskbuf+(relpos+1)*KEYVALLEN, (right-index-1)*KEYVALLEN);
				if ( relpos+1 < 0 || relpos+2+right-index-1 > JAG_BLOCK_SIZE ) {
					raydebug( stdout, JAG_LOG_LOW, 
						"s7890 [%s] memmove right shift out of bound relpos=%d, right=%l, index=%l\n", 
						_dbobj.c_str(), relpos, right, index);
					// abort();
					_diskLock->writeUnlock( first );
					free( diskbuf );
					return 0;
				}
			}

			for ( i = right; i >= index+2; --i) {
				j = i;
				_getPair( diskbuf+(relright-shiftpos)*KEYVALLEN, KEYLEN, VALLEN, tpair, true );
				if ( j%max == 0 ) {
					changerc = _blockIndex->updateIndex(tpair, j, true) || changerc;
				}
				++shiftpos;
			}

			memcpy(diskbuf+(relpos+1)*KEYVALLEN, pair.key.c_str(), KEYVALLEN);
			// memcpy(diskbuf+(relpos+1)*KEYVALLEN, pair.key.c_str(), KEYLEN);
			// memcpy(diskbuf+(relpos+1)*KEYVALLEN+KEYLEN, pair.value.c_str(), VALLEN);
			if ( index+1 < localmin || relpos+1+right-index > JAG_BLOCK_SIZE || index+1+right-index > localmin+_arrlen ) {
				raydebug( stdout, JAG_LOG_LOW, 
							"s7492 [%s] pwrite right shift out of serv bound, idx=%l, relpos=%d, right=%l, locmin=%l, arrlen=%l\n", 
							_dbobj.c_str(), index, relpos, right, localmin, (abaxint)_arrlen);
				// abort();
				_diskLock->writeUnlock( first );
				free( diskbuf );
				return 0;
			}
			raypwrite(_jdfs, diskbuf+(relpos+1)*KEYVALLEN, (right-index)*KEYVALLEN, (index+1)*KEYVALLEN);
			if ( right > _maxindex ) _maxindex = right;
			if ( index < _minindex ) _minindex = index;
			j = *retindex = index+1;
			changerc = _blockIndex->updateIndex(pair, j) || changerc;
		}
	}
	j = *retindex;
	_blockIndex->updateCounter( j, 1, false );

	++_elements;
	//updateOldPair( pair );
	_diskLock->writeUnlock( first );
	if ( changerc ) insertCode = 200;
	free( diskbuf );
	return 1;
}

// returns:  -99 unable to insert, range not right
//           0: exists already
//           22: upsert was done
//           1: success
int JagDiskArrayBase::insertToAll( JagDBPair &pair, abaxint *retindex, int &insertCode )
{
	/***
	prt(("s3828 insertAll pairk=[%s] pairv=[%s] _elements=%lld _arrlen=%lld\n", 
	     pair.key.c_str(), pair.value.c_str(), (abaxint)_elements, (abaxint)_arrlen ));
	prt(( "s3828 insall _elem=%lld _arl=%lld xins=%lld, _pathname=[%s], _key=[%s]\n", 
		  (abaxint)_elements, (abaxint)_arrlen, _servobj->_xferInsert, _pathname.c_str(), pair.key.c_str() ));
	***/
	// _blockIndex->print();
	//print();
	insertCode = 0;

	int max = JagCfg::_BLOCK;
	bool rc, changerc = false, leftfound = false, rightfound = false, allempty = true;
	abaxint i, j, index, left, right, first, last, localmin, localmax;
	JagDBPair tpair, getpair;
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );

	localmin = _nthserv*_arrlen;
	localmax = localmin + _arrlen;

	JAG_BLURT _diskLock->writeLock( -1 ); JAG_OVER;
	rc = getFirstLast( pair, first, last );
	if ( ! rc ) {
		_diskLock->writeUnlock( -1 );
		free( keyvalbuf );
		//prt(("s2083 getFirstLast rc=%d pair.key=[%s] -99\n", rc, pair.key.c_str() ));
		return -99;
	}
	
	rc = findPred( pair, &index, first, last, getpair, NULL );
	if ( rc ) {
		*retindex = index;
		if ( pair.upsertFlag == 1 ) {
			raypwrite(_jdfs, pair.key.c_str()+KEYLEN, VALLEN, index*KEYVALLEN+KEYLEN);
			if ( index > _maxindex ) _maxindex = index;
			if ( index < _minindex ) _minindex = index;
			pair.value = getpair.value;
			_diskLock->writeUnlock( -1 );
			free( keyvalbuf );
			return 22;
		}
		// printf("s9999 insertToAll exist 0\n");
		_diskLock->writeUnlock( -1 );
		free( keyvalbuf );
		return 0;
	}

	// if index out of bound, return -99
	bool spasc = 0;
	if ( (*(_schemaRecord->columnVector))[0].type != JAG_C_COL_TYPE_TIMESTAMP && _keyMode == JAG_ASC ) {
		spasc = 1;
	}
	if ( ( index < localmin || index >= localmax ) && !spasc ) {
		*retindex = index;
		_diskLock->writeUnlock( -1 );
		free( keyvalbuf );
		// prt(("s2085 index=%d < localmin=%d  || index=%d >= localmax=%d -99\n", index, localmin, index, localmax ));
		// bug: todo for ascending table here returns -99
		return -99;
	}
	
	if ( index >= 0 ) raypread(_jdfs, keyvalbuf, KEYVALLEN, index*KEYVALLEN);
	if ( index >= 0 && keyvalbuf[0] == '\0' ) 
	{
		if ( index < localmin || index+1 > localmin+_arrlen ) {
			raydebug( stdout, JAG_LOG_LOW, 
					 "s7893 [%s] insertToAll pwrite pos out of server bound index=%l, localmin=%l, arrlen=%l\n", 
					 _dbobj.c_str(), index, localmin, (abaxint)_arrlen);
			// abort();
			_diskLock->writeUnlock( -1 );
			free( keyvalbuf );
			return 0;
		}

		raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, index*KEYVALLEN);	
		if ( index > _maxindex ) _maxindex = index;
		if ( index < _minindex ) _minindex = index;
		j = index;
		changerc = _blockIndex->updateIndex( pair, j ) || changerc;
		_blockIndex->updateCounter( j, 1, false );
		*retindex = index;
	} else {
		left = right = index; // pred position
		int cnt = 0;
		abaxint nshifts = 0;
		while ( 1 )
		{
			if ( left == localmin && right == localmax-1 ) {
				_diskLock->writeUnlock( -1 );
				// printf("s9999 insertToAll outofbound 0\n");
				free( keyvalbuf );
				return 0;
			}

			if ( index == -1 && right == localmax-1 ) {
				_diskLock->writeUnlock( -1 );
				// printf("s9999 insertToAll outofbound 0\n");
				free( keyvalbuf );
				return 0;
			}

			if ( left > localmin ) --left;
			if ( right < localmax-1 ) ++right;

			raypread(_jdfs, keyvalbuf, KEYVALLEN, right*KEYVALLEN);
			if ( right < localmax && keyvalbuf[0] == '\0' ) {
				rightfound = true;
				break;
			}

			raypread(_jdfs, keyvalbuf, KEYVALLEN, left*KEYVALLEN);
			if ( left >= localmin && keyvalbuf[0] == '\0' ) {
				leftfound = true;
				break;
			}

			/***
			// debug only
			++cnt;
			if ( cnt > 1000 ) {
				// printf("s4040 Error locating position for insert, exit. left=%lld right=%lld index=%lld\n", left, right, index );
				// printf("_elements=%lld _arrlen=%lld _ups=%lld _downs=%lld \n",(abaxint)_elements,(abaxint)_arrlen,(abaxint)_ups,(abaxint)_downs );
				// printf(" _tabname=[%s] _idxname=[%s]  _objname=[%s]\n", _tabname.c_str(), _idxname.c_str(), _objname.c_str() );
				// print( left-1000, right+1000 );
				abort();
			}
			***/
		}

		if ( leftfound ) {
			// printf("c4301 leftshift shiftleft [%d %d]  <-- [%d %d]\n", left, index-1, left+1, index );
			for ( i = left; i <= index-1; ++i) {
				j = i;
				raypread(_jdfs, keyvalbuf, KEYVALLEN, (i+1)*KEYVALLEN);
				if ( i < localmin || i+1 >= localmin+_arrlen ) {
					raydebug( stdout, JAG_LOG_LOW, 
							"s7893 [%s] insertToAll pwrite left shift out of server bound i=%l, localmin=%l, arrlen=%l\n", 
							_dbobj.c_str(), i, localmin, (abaxint)_arrlen);
					// abort();
					_diskLock->writeUnlock( -1 );
					free( keyvalbuf );
					return 0;
				}
				raypwrite(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN);
				if ( i > _maxindex ) _maxindex = i;
				if ( i < _minindex ) _minindex = i;
				++nshifts;
				++_shifts;
				_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
				if ( j%max == 0 ) {
					changerc = _blockIndex->updateIndex(tpair, j, true) || changerc;
				} else if ( j%max == max-1 ) {
					changerc = _blockIndex->updateIndex(tpair, j) || changerc;
				}
			}

			// if N*0 -- left-1 all empty, update at left
			// code ok
			if ( left%max != 0 ) {
				abaxint block = left / max;
				for ( i = left-1; i>=block*JagCfg::_BLOCK; --i) {
					raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN);
					if ( i < localmin || i >= localmin+_arrlen ) {
						raydebug( stdout, JAG_LOG_LOW, 
							"s7894 [%s] insertToAll left find all empty left shift out of server bound i=%l, localmin=%l, arrlen=%l\n", 
							_dbobj.c_str(), i, localmin, (abaxint)_arrlen);
						// abort();
						_diskLock->writeUnlock( -1 );
						free( keyvalbuf );
						return 0;
					}
					if ( keyvalbuf[0] != '\0' ) {
						allempty = false;
						break;
					}
				}
				if ( allempty ) {
					raypread(_jdfs, keyvalbuf, KEYVALLEN, left*KEYVALLEN);
					if ( left < localmin || left >= localmin+_arrlen ) {
						raydebug( stdout, JAG_LOG_LOW, 
							"s7894 [%s] insertToAll left all empty left shift out of server bound left left=%l, localmin=%l, arrlen=%l\n", 
							_dbobj.c_str(), left, localmin, (abaxint)_arrlen);
						// abort();
						_diskLock->writeUnlock( -1 );
						free( keyvalbuf );
						return 0;
					}
					_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
					j = left;
					changerc = _blockIndex->updateIndex(tpair, j) || changerc;
				}
			}

			if ( index < 0 ) ++index;

			raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, index*KEYVALLEN);	
			if ( index > _maxindex ) _maxindex = index;
			if ( index < _minindex ) _minindex = index;
			if ( index < localmin || index >= localmin+_arrlen ) {
				raydebug( stdout, JAG_LOG_LOW, 
						  "s7195 [%s] insertToAll left shift write current pair out of server bound index=%l, localmin=%l, arrlen=%l\n", 
						  _dbobj.c_str(), index, localmin, (abaxint)_arrlen);
				// abort();
				_diskLock->writeUnlock( -1 );
				free( keyvalbuf );
				return 0;
			}
			j = *retindex = index;
			if ( j%max == 0 ) {
				changerc = _blockIndex->updateIndex(pair, j, true) || changerc;
			} else {
				changerc = _blockIndex->updateIndex(pair, j) || changerc;
			}
		} else if ( rightfound ) {
			// printf("c4402 rightshift shiftright [%d %d]  --> [%d %d]\n", index+1, right-1, index+2, right );
			for ( i = right; i >= index+2; --i) {
				j = i;
				raypread(_jdfs, keyvalbuf, KEYVALLEN, (i-1)*KEYVALLEN);
				if ( i-1 < localmin || i >= localmin+_arrlen ) {
					raydebug( stdout, JAG_LOG_LOW, 
							"s7893 [%s] insertToAll pwrite right shift out of server bound i=%l, localmin=%l, arrlen=%l\n", 
							_dbobj.c_str(), i, localmin, (abaxint)_arrlen);
					// abort();
					_diskLock->writeUnlock( -1 );
					free( keyvalbuf );
					return 0;
				}
				raypwrite(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN);
				if ( i > _maxindex ) _maxindex = i;
				if ( i < _minindex ) _minindex = i;
				++nshifts;
				++_shifts;
				_getPair( keyvalbuf, KEYLEN, VALLEN, tpair, true );
				if ( j%max == 0 ) {
					changerc = _blockIndex->updateIndex(tpair, j, true) || changerc;
				}
			}

			raypwrite(_jdfs, pair.key.c_str(), KEYVALLEN, (index+1)*KEYVALLEN);
			if ( index+1 > _maxindex ) _maxindex = index+1;
			if ( index+1 < _minindex ) _minindex = index+1;
			if ( index+1 < localmin || index+1 >= localmin+_arrlen ) {
				raydebug( stdout, JAG_LOG_LOW, 
					"s7295 [%s] insertToAll right shift write current pair out of server bound index=%l, localmin=%l, arrlen=%l\n", 
					_dbobj.c_str(), index, localmin, (abaxint)_arrlen);
				// abort();
				_diskLock->writeUnlock( -1 );
				free( keyvalbuf );
				return 0;
			}
			j = *retindex = index+1;
			changerc = _blockIndex->updateIndex(pair, j) || changerc;
		}		

		if ( nshifts > 0 ) {
			// update left to right range elements
			left = (left / JagCfg::_BLOCK) * JagCfg::_BLOCK;
			right = (right / JagCfg::_BLOCK) * JagCfg::_BLOCK + JagCfg::_BLOCK;
			// prt(("s5523 left=%lld right=%lld updateCounter ....\n", left, right ));
			for ( i = left; i < right; ++i ) {
				j = i;
				if ( (i % JagCfg::_BLOCK) == 0 ) {
					_blockIndex->updateCounter( j, 0, true );
				}
				raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN);
				if ( keyvalbuf[0] != '\0' ) {
					_blockIndex->updateCounter( j, 1, false );
				}
			}
		} else {
			j = *retindex;
			_blockIndex->updateCounter( j, 1, false );
		}
	}

	++_elements;

	//updateOldPair( pair );
	// printf("s9999 insertToAll end=%lld\n", *retindex);
	_diskLock->writeUnlock( -1 );
	// if ( changerc ) return 200;
	if ( changerc ) { 
		insertCode = 200;
		free( keyvalbuf );
		return 1;
	}

	free( keyvalbuf );
	return 1;
}

// whole:  int of cells/elements   cross: true if elem/cells decimal over 0.5
int JagDiskArrayBase::getStep( int kmode, abaxint i, abaxint whole, bool cross, bool even )
{
	int step = 1;
	if ( JAG_RAND == kmode ) {
		if ( !cross || !even ) step = whole;
		else step = whole+1;		
	} else if ( JAG_ASC == kmode ) {
		// if ( even ) step = 2;
		// else step = 1;
		step = 2;
	} else {
		if ( even ) step = -2;
		else step = -1;
	}

	// prt(("s3144 getStep whole=%d cross=%d even=%d step=%d\n", whole, cross, even, step ));
	return step;
}


abaxint JagDiskArrayBase::getRegionElements( abaxint first, abaxint length )
{
	abaxint rangeElements = 0;
	first /= JAG_BLOCK_SIZE;
	for ( abaxint i = 0; i < length; ++i ) {
		rangeElements += _blockIndex->getPartElements( first+i );
	}
	return rangeElements;
}

int JagDiskArrayBase::getResizePattern( float ratio, int &span )
{
	int pattern = 0;
	if ( ratio < ( 0.25 - 0.0001 ) ) {
		pattern = JAG_EEEV;
		span = 4;
	} else if ( ratio < ( 0.33 - 0.0001 )  ) {
		pattern = JAG_EEV;
		span = 3; 
	}  else if ( ratio < ( 0.50 - 0.0001 )  ) {
		pattern = JAG_EV;
		span = 2;
	} else if ( ratio < ( 0.66 - 0.0001 )  ) {
		pattern = JAG_EVV;
		span = 2;  // 0-th skip 1; others no skip
	} else if ( ratio < ( 0.75 - 0.0001 )  ) {
		pattern = JAG_EVVV;
		span = 3; // 0-th skip 1; others no skip
	} else if ( ratio < ( 0.80 - 0.0001 )  ) {
		pattern = JAG_EVVVV;
		span = 4; // 0-th skip 1; others no skip
	}

	return pattern;
}

// static
void *JagDiskArrayBase::monitorCommandsMem( void *ptr )
{
 	JagDiskArrayBase *jda = (JagDiskArrayBase*)ptr;
	// prt(("s3731 ********* THRD=%lld monitorCommandsMem(%s) ...\n", THREADID, jda->_objname.c_str() ));
	JagDBServer *servobj = jda->_servobj;
	// int period, syncPeriod;
	int period;
	if ( jda->_isClient ) {
		JagCfg ccfg( JAG_CLIENT );
		period = atoi(ccfg.getValue("DISK_FLUSH_INTERVAL", "1000").c_str());
	} else {
		period = atoi(servobj->_cfg->getValue("DISK_FLUSH_INTERVAL", "1500").c_str());
	}
	abaxint cnt = 0;
	JagClock clock;
	JagClock clock2;
	abaxint  t1, t2;
	time_t  nowt;

	while ( 1 ) {
		if ( ! (int)(jda->_sessionactive) ) {
			break;
		}

		if ( jda->_forceSleepTime > 0 ) {
			jagsleep( jda->_forceSleepTime, JAG_MSEC );
		} else {
			jagsleep( period, JAG_MSEC );
		}

		// clock.start();
		cnt = jda->waitCopyAndInsertBufferAndClean( clock, clock2 ); 
		if ( cnt == ETIMEDOUT ) {
				// timeout, destroy monitor
				// prt(("s3722 waitCopyAndInsertBufferAndClean ETIMEDOUT _monitordone=1\n" ));
				jda->_monitordone = 1;
				return NULL;
		}
		raydebug(stdout, JAG_LOG_LOW, "s3428 waitCopyAndInsertBufferAndClean cnt=%d\n", cnt );
		// clock.stop();
		t1 = clock.elapsed();
		t2 = clock2.elapsed();
		if ( cnt > 1 ) {
			// DO not remove this in production
			logInfo( t1, t2, cnt, jda );
		}
	}

	jda->_monitordone = 1;
   	return NULL;
}

// object method
void JagDiskArrayBase::copyAndInsertBufferAndClean()
{
	jaguar_mutex_lock( & _insertBufferMutex );
	if ( _pairmap->elements() < 1 || ! _sessionactive ) {
		jaguar_mutex_unlock( &_insertBufferMutex );
		return;
	}
	
	jaguar_mutex_lock( &(_insertBufferCopyMutex ) );
	copyInsertBuffer();
	jaguar_mutex_unlock( &_insertBufferMutex );

	jaguar_cond_signal(& _insertBufferLessThanMaxCond );
	flushInsertBuffer(); 
	cleanInsertBufferCopy();
	jaguar_mutex_unlock( &(_insertBufferCopyMutex ) );
}

// object method
abaxint JagDiskArrayBase::waitCopyAndInsertBufferAndClean( JagClock &clock, JagClock &clock2 )
{
	//clock.start();
	abaxint cnt = 0; int rc;
	jaguar_mutex_lock( & _insertBufferMutex );
	struct timeval now;
	struct timespec ts;
	raydebug(stdout, JAG_LOG_LOW, "s3028 enter waitCopyAndInsertBufferAndClean() ...\n" );
	while ( _pairmap->elements() < 1 && _sessionactive ) {
		gettimeofday( &now, NULL );
		ts.tv_sec = now.tv_sec;
		ts.tv_nsec = now.tv_usec * 1000;
		ts.tv_sec += _servobj->_jdbMonitorTimedoutPeriod;
		rc = jaguar_cond_timedwait(&_insertBufferMoreThanMinCond, &_insertBufferMutex, &ts );
		if ( rc == ETIMEDOUT ) {
			// timeout, destroy monitor thread
			_monitordone = 1;
			jaguar_mutex_unlock( &_insertBufferMutex );
			raydebug(stdout, JAG_LOG_LOW, "s3528 JagDiskArrayBase cond_timedwait ETIMEDOUT\n" );
			return rc;
		}
	}
	//clock.stop();
	raydebug(stdout, JAG_LOG_LOW, "s3628 in waitCopyAndInsertBufferAndClean() past while\n" );
	
	//clock2.start();
	#ifdef CHECK_LATENCY
	JagClock clock3;
	clock3.start();
	#endif

	jaguar_mutex_lock( &(_insertBufferCopyMutex ) );
	copyInsertBuffer();
	#ifdef CHECK_LATENCY
	clock3.stop();
	raydebug(stdout, JAG_LOG_LOW, "s3028 copyInsertBuffer %l ms\n", clock3.elapsed());
	clock3.start();
	#endif

	jaguar_mutex_unlock( &_insertBufferMutex );
	jaguar_cond_signal(& _insertBufferLessThanMaxCond );

	raydebug(stdout, JAG_LOG_LOW, "s3030 flushInsertBuffer()... \n" );
	cnt = flushInsertBuffer(); 
	raydebug(stdout, JAG_LOG_LOW, "s3030 flushInsertBuffer() cnt=%d done\n", cnt );

	#ifdef CHECK_LATENCY
	clock3.stop();
	raydebug(stdout, JAG_LOG_LOW, "s3028 flushInsertBuffer %l ms\n", clock3.elapsed());
	clock3.start();
	#endif

	cleanInsertBufferCopy();
	#ifdef CHECK_LATENCY
	clock3.stop();
	raydebug(stdout, JAG_LOG_LOW, "s3028 cleanInsertBuffer %l ms\n", clock3.elapsed());
	#endif

	jaguar_mutex_unlock( &(_insertBufferCopyMutex ) );
	//clock2.stop();
	return cnt;
}

// cleanup insertbuffer copy
void JagDiskArrayBase::cleanInsertBufferCopy()
{
	if ( ! _pairmapcopy ) { return; }
	if ( _pairmapcopy ) { delete _pairmapcopy; _pairmapcopy = NULL; jagmalloc_trim(0); }
}

// object method flush *_pairarr data pairs
abaxint JagDiskArrayBase::copyInsertBuffer()
{
	abaxint len = _pairmap->elements();
	if ( _pairmapcopy ) { delete _pairmapcopy; _pairmapcopy = NULL; jagmalloc_trim(0); }
	_pairmapcopy = _pairmap; _pairmap = new JagDBMap();
	return len;
}

// object method flush *_pairarr data pairs
abaxint JagDiskArrayBase::flushInsertBuffer()
{
	//raydebug( stdout, JAG_LOG_LOW, "s4011 testtesttest nothing done in flushInsertBuffer return\n" );
	//return 0;

	// if (  _pairarrcopy->_elements < 1 ) { return 0; }
	if (  _pairmapcopy->elements() < 1 ) { return 0; }
	_isFlushing = 1;

	abaxint len = _pairarrcopy->size();
	abaxint cnt, retindex;
	int rc, insertCode;
	cnt = 0;
	rc = insertMerge( 0, _pairmapcopy->elements(), NULL, NULL, NULL, NULL );
	// prt(("s2883 insertMergerc=%d\n", rc ));
	raydebug( stdout, JAG_LOG_LOW, "s4111 insertMerge %d return rc=%d\n", _pairmapcopy->elements(), rc );
	if ( rc == 2 ) {
		// insert one pair to empty diskarray, then try merge again
		if ( _pairmapcopy->elements() < 1 ) { _isFlushing = 0; return cnt; }
		char *kvbuf = (char*)jagmalloc(KEYVALLEN+1);
		memset( kvbuf, 0, KEYVALLEN+1 );
		JagDBPair retpair;

		FixMap::iterator it;
		for ( it = _pairmapcopy->_map->begin(); it != _pairmapcopy->_map->end(); ++it ) {
			memcpy( kvbuf, it->first.c_str(), KEYLEN );
			memcpy( kvbuf+KEYLEN, it->second.first.c_str(), VALLEN );
			JagDBPair ipair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN, true );
			ipair.upsertFlag = it->second.second;

			rc = _insertData( ipair, &retindex, insertCode, false, retpair );
			// prt(("s5203 insertData pair=[%s] rc=%d\n", pair.key.c_str(), rc ));
			if ( rc > 0 ) {
				++cnt;
				++ _writes;
			}
			_pairmapcopy->remove( ipair );
			// prt(("s2483 _insertData rc=%d\n", rc ));
			break;
		}

		free( kvbuf );		
		_insdircnt += cnt;
		rc = insertMerge( 0, _pairmapcopy->elements(), NULL, NULL, NULL, NULL );
		// prt(("s5830 insertMerge rc=%d\n", rc ));
	}

	if ( rc == 0 ) {
		// not done insert merge, do regular insert
		if ( _pairmapcopy->elements() < 1 ) { _isFlushing = 0; return cnt; }
		char *kvbuf = (char*)jagmalloc(KEYVALLEN+1);
		memset(kvbuf, 0, KEYVALLEN+1);
		JagDBPair retpair;

		FixMap::iterator it;
		for ( it = _pairmapcopy->_map->begin(); it != _pairmapcopy->_map->end(); ++it ) {
			memcpy( kvbuf, it->first.c_str(), KEYLEN );
			memcpy( kvbuf+KEYLEN, it->second.first.c_str(), VALLEN );
			JagDBPair ipair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN, true );
			ipair.upsertFlag = it->second.second;

			rc = _insertData( ipair, &retindex, insertCode, false, retpair );
			// prt(("s5203 insertData pair=[%s] rc=%d\n", ipair.key.c_str(), rc ));
			if ( rc > 0 ) {
				++cnt;
				++ _writes;
			}
		}

		free( kvbuf );		
		_insdircnt += cnt;
	} else {
		// do insert merge for flib
		cnt = _pairmapcopy->elements();
		_writes += _pairmapcopy->elements();
		_insmrgcnt += cnt;
	}
	
	_isFlushing = 0;
	return cnt;
}

Jstr JagDiskArrayBase::jdbPath( const Jstr &jdbhome, const Jstr &db, const Jstr &tab )
{
	Jstr fpath = jdbhome + "/" + db + "/" + tab;
	return fpath;
}

Jstr JagDiskArrayBase::jdbPathName( const Jstr &jdbhome, const Jstr &db, const Jstr &tab )
{
	Jstr fpath = jdbhome + "/" + db + "/" + tab + "/" + tab + ".jdb";
	return fpath;
}

// method to flush pairarr as well as compress
bool JagDiskArrayBase::flushCompress()
{
	_reSizeLock->writeLock( -1 ); JAG_OVER;
	// first, check and see if more compress needs to be done
	if ( _newblockIndex ) {
		delete _newblockIndex;
		_newblockIndex = NULL;
	}

	//_newblockIndex = new JagBlock<JagDBPair>();
	_newblockIndex = new JagFixBlock( KEYLEN );
	JagDBPair tpair;
	const JagDBPair *ppair = NULL;
	abaxint ipos, pos, lastBlock = -1;
	char *keyvalbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( keyvalbuf, 0, KEYVALLEN+1 );
	
	if ( _pairmap->elements() % JAG_BLOCK_SIZE != 0 ) _newarrlen = (_pairmap->elements()/JAG_BLOCK_SIZE+1)*JAG_BLOCK_SIZE;
	else _newarrlen = _pairmap->elements();
	size_t bytes2 = _newarrlen*KEYVALLEN;
	_jdfs2 = new JDFS( _servobj, _tmpFilePath, KEYVALLEN, _newarrlen );
	_jdfs2->open();
	_jdfs2->fallocate( 0, bytes2 );

	if ( _pairmap->elements() > 0 ) {
		abaxint actualcnt = 0, whole = 1;
		bool half = 0, even = 0;
		pos = 0;

		// abaxint wlimit = getBuffReaderWriterMemorySize( _newarrlen*KEYVALLEN/1024/1024 );
		abaxint wlimit = 256;
		JagSingleBuffWriter bw( _jdfs2->getFD(), KEYVALLEN, wlimit );
		_minindex = 0;
		
    	for ( FixMap::iterator it=_pairmap->_map->begin(); it!=_pairmap->_map->end(); ++it) {
			memcpy( keyvalbuf, it->first.c_str(), KEYLEN );
			memcpy( keyvalbuf+KEYLEN, it->second.first.c_str(), VALLEN );
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
		_elements = _pairmap->elements();
		if ( _blockIndex ) delete _blockIndex;
		_blockIndex = _newblockIndex;
		_newblockIndex = NULL;
		_doneIndex = 1;
		if ( _elements != actualcnt ) {
			raydebug( stdout, JAG_LOG_LOW, 
					  "s9999 [%s] when resize flush compress,  _elements=%l != actualcnt=%l\n", 
					  _dbobj.c_str(), (abaxint)_elements, actualcnt);
			// abort();
		}
		_pairmap->clean();
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

// mergeMode 0: flib merge; mergeMode 1: darrnew merge; mergeMode 2: single insert merge ( resize partial )
// mergeElements : num of flibs / darrnew elements / 1
// mbuf, mbr, mbbr: objects for darrnew use only; other mode are NULL
// sbuf: objects for single insert merge use only; other mode are NULL
// mergeType: 0: mem-mem merge   1: mem-disk merge to form a temp region .reg file
int JagDiskArrayBase::insertMerge( int mergeMode, abaxint mergeElements, char *mbuf, const char *sbuf, 
								   JagBuffReader *mbr, JagBuffBackReader *mbbr )
{
	if ( mergeElements < 1 ) return 0; // no more data to merge
	// first, get min pair and max pair from memory array
	if ( _elements < 1 && mergeMode == 2 ) return 0;
	else if ( _elements < 1 && mergeMode == 0 ) return 2; // put first elements in file directly, not by merge

	// check if darrnew is already exist
	if ( mergeMode == 0 && _hasdarrnew ) {
		if ( mergeMode == 0 || mergeMode == 1 ) return 0;
	}

	int rc = 0, mergeType = 0;
	abaxint minfirst = 0, minlast = 0, maxfirst = 0, maxlast = 0;
	JagDBPair minpair, maxpair;
	insertMergeGetMinMaxPair( minpair, maxpair, mergeMode, mbr, mbbr, mbuf, sbuf );
	
	JAG_BLURT _memLock->readLock( -1 ); JAG_OVER;
	// find minpair maxpair first last from diskarray
	getFirstLast( minpair, minfirst, minlast );
	getFirstLast( maxpair, maxfirst, maxlast );
	abaxint numBlocks = (maxfirst-minfirst)/JagCfg::_BLOCK+1;
	abaxint ranArrlen = numBlocks*JagCfg::_BLOCK;
	abaxint ranElements = getRegionElements( minfirst, numBlocks );

	// check if currect diskarray elements + flib elements will cause another resize
	if ( (_elements+mergeElements)*100 > JAG_FILE_RAND_EXPAND*_arrlen && ( _arrlen < _fileMaxCntLimit || _uidORschema ) ) {
		// force resize, then do regular insert
		_memLock->readUnlock( -1 );
		if ( mergeMode == 2 ) return 0;
		abaxint narrlen = _GEO*_arrlen;
		while( narrlen*JAG_FILE_RAND_EXPAND < (_elements+mergeElements)*100 ) narrlen *= _GEO;
		reSize( true, narrlen );
		JAG_BLURT _memLock->readLock( -1 ); JAG_OVER;
		getFirstLast( minpair, minfirst, minlast );
		getFirstLast( maxpair, maxfirst, maxlast );
		findMergeRegionFirstLast( minfirst, maxfirst, numBlocks, ranArrlen, ranElements, mergeElements );
		// else return mergeResize( mergeMode, mergeElements, mbuf, sbuf, mbr );
	}

	// check to see if disk range is relatively small to merge part
	if ( mergeElements*JAG_INSERTMERGE_BLOCK_RATIO < ranElements || mergeElements == 1 ) {
		if ( mergeMode == 0 || mergeMode == 1 ) {
			_memLock->readUnlock( -1 );
			return 0;
		}
	}

	// check if range blocks is relatively empty
	if ( ranArrlen*(JAG_INSERTMERGE_GAP-1) > (ranElements+mergeElements)*JAG_INSERTMERGE_GAP ) {
		// has enough space
		mergeType = 1;
	}
	
	// otherwise, do insertMerge
	// first, use while loop to find actual range which can hold all the flib elements
	if ( !mergeType ) mergeType = findMergeRegionFirstLast( minfirst, maxfirst, numBlocks, ranArrlen, ranElements, mergeElements );
	
	if ( !mergeType && ( _arrlen < _fileMaxCntLimit || _uidORschema ) ) {
		// the whole file does not have enough space for merge, force resize and do regular insert
		_memLock->readUnlock( -1 );
		if ( mergeMode == 2 ) return 0;
		abaxint narrlen = _GEO*_arrlen;
		while( narrlen*JAG_FILE_RAND_EXPAND < (_elements+mergeElements)*100 ) narrlen *= _GEO;
		reSize( true, narrlen );
		JAG_BLURT _memLock->readLock( -1 ); JAG_OVER;
		getFirstLast( minpair, minfirst, minlast );
		getFirstLast( maxpair, maxfirst, maxlast );
		findMergeRegionFirstLast( minfirst, maxfirst, numBlocks, ranArrlen, ranElements, mergeElements );
		// else return mergeResize( mergeMode, mergeElements, mbuf, sbuf, mbr );
	}
	
	JagClock insertMergeClock;
	insertMergeClock.start();
	// else, do insert merge
	char *kvbuf = (char*)jagmalloc(KEYVALLEN+1);
	char *dbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( kvbuf, 0, KEYVALLEN+1 );
	memset( dbuf, 0, KEYVALLEN+1 );
	// abaxint dblimit = getBuffReaderWriterMemorySize( ranArrlen*KEYVALLEN/1024/1024 );
	abaxint dblimit = 64;
	JagBuffReader dbr( this, ranArrlen, KEYLEN, VALLEN, minfirst, 0, dblimit );
	
	int fd = -1, span, pattern;
	bool half = 0, even = 0;
	char *wbuf = NULL;
	Jstr twp = _pathname + ".reg";
	JagSingleBuffWriter *sbw = NULL;
	abaxint wpos = -1, ipos = minfirst, lastBlock = -1, actualcnt = 0;
	int dgoNext = 1, mgoNext = 1;
	// abaxint mpos = 0;
	FixMap::iterator mpos = _pairmapcopy->_map->begin();
	abaxint totElements = ranElements + mergeElements;
	double ratio = (double)totElements/(double)ranArrlen;	
	
	if ( mergeMode == 1 ) {
		// darrnew memory already get one pair
		mgoNext = 0;
	}
	
	// first, check if memory has enough space to do insert
	mergeType = 0;
	if ( ranArrlen*KEYVALLEN > _mergeLimit ) mergeType = 1;

	if ( mergeType == 0 ) {
		// memory is large enough to contain all data for given range
		if ( wbuf ) free ( wbuf );
		wbuf = (char*)jagmalloc(ranArrlen*KEYVALLEN+1);
		memset( wbuf, 0, ranArrlen*KEYVALLEN+1 );
	} else {
		// memory is not large enough. Use dbobj.reg for temporary file for insert Merge
		jagunlink( twp.c_str() );
		fd = jagopen( twp.c_str(), O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU );
		jagftruncate( fd, ranArrlen*KEYVALLEN );
		sbw = new JagSingleBuffWriter( fd, KEYVALLEN, dblimit );
	}
	
	pattern = getResizePattern( ratio, span );
	cleanRegionElements( minfirst, numBlocks );
	
	while ( getNextMergePair( kvbuf, dbuf, dbr, mpos, mbuf, mbr, sbuf, dgoNext, mgoNext, mergeMode ) ) {	
		if ( 0 == pattern ) {
			wpos += 1;
		} else if ( pattern == JAG_EEEV || pattern == JAG_EEV || pattern == JAG_EV ) {
			wpos += span;
		} else {
			if ( (actualcnt%span) == 0 ) {
				wpos += 2;
			} else {
				wpos += 1;
			}
		}
		++actualcnt;
		ipos = wpos+minfirst;
		if ( wpos >= ranArrlen ) {
			prt(("s7799 invalid write out of bound for merge, please check\n"));
			// abort();
			break;
		}
		
		// write kvpair to memory or temp .reg disk file
		if ( mergeType == 0 ) {
			memcpy( wbuf+wpos*KEYVALLEN, kvbuf, KEYVALLEN );
		} else {
			sbw->writeit( wpos, kvbuf, KEYVALLEN );
		}		
		// update blockindex
		insertMergeUpdateBlockIndex( kvbuf, ipos, lastBlock );
	}
	if ( sbw ) sbw->flushBuffer();
	
	if ( totElements != actualcnt ) {
		raydebug( stdout, JAG_LOG_LOW, 
				  "s9227 [%s] when insert merge, actual number != elements. ranElements=%l, mergeElements=%l actualcnt=%l\n", 
				  _dbobj.c_str(), ranElements, mergeElements, actualcnt);
		// abort();
	}
	
	// finally, overwrite the original range with memory or .reg disk data
	if ( mergeType == 0 ) {
		// for memory, just overwrite range with new data
		raypwrite(_jdfs, wbuf, ranArrlen*KEYVALLEN, minfirst*KEYVALLEN);
	} else {
		// for disk, destory buff writer and jagmalloc a piece of memory to read data from .reg piece by piece with overwrite
		jagclose( fd );
		fd = jagopen( twp.c_str(), O_RDONLY|JAG_NOATIME, S_IRWXU );
		abaxint soff = 0, slen, roff = minfirst*KEYVALLEN, rbytes = _mergeLimit/KEYVALLEN;
		if ( sbw ) delete sbw;
		sbw = NULL;
		if ( wbuf ) free ( wbuf );
		wbuf = (char*)jagmalloc(rbytes+1);
		memset( wbuf, 0, rbytes+1 );
		while ( true ) {
			slen = raysafepread( fd, wbuf, rbytes, soff );
			if ( slen <= 0 ) break;
			raypwrite(_jdfs, wbuf, slen, roff);
			soff += slen;
			roff += slen;
		}
	}
	insertMergeClock.stop();

	if ( mergeMode == 0 ) {
		/**
		raydebug(stdout, JAG_LOG_LOW, "s3791 flib insmrg %s(%l->%l) %d ms\n",
			_dbobj.c_str(), mergeElements, ranElements, insertMergeClock.elapsed());
			**/
	} else if ( mergeMode == 1 ) {
		/**
		raydebug(stdout, JAG_LOG_LOW, "s3792 darrnew insmrg %s(%l->%l) %d ms\n",
			_dbobj.c_str(), mergeElements, ranElements, insertMergeClock.elapsed());
			**/
	} else {
		// prt(("s3791 [%s] insert merge resize Partial %lld\n", _dbobj.c_str(), totElements));
	}
	
	// clean up possible memory object usage above
	if ( fd >= 0 ) {
		jagclose( fd );
		jagunlink( twp.c_str() );
	}
	if ( sbw ) delete sbw;
	if ( wbuf ) free( wbuf );
	if ( kvbuf ) free( kvbuf );
	if ( dbuf ) free( dbuf );
	_memLock->readUnlock( -1 );
	return 1;
}

void JagDiskArrayBase::insertMergeUpdateBlockIndex( char *kvbuf, abaxint ipos, abaxint &lastBlock )
{
	JagDBPair tpair;
	if ( ipos > _maxindex ) _maxindex = ipos;
	if ( ipos < _minindex ) _minindex = ipos;
	if ( ( ipos/JagCfg::_BLOCK) != lastBlock ) {
		_getPair( kvbuf, KEYLEN, VALLEN, tpair, true );
		_blockIndex->updateIndex( tpair, ipos, true );
		_blockIndex->updateCounter( ipos, 0, true, false );
	}
	_blockIndex->updateCounter( ipos, 1, false, false );
	lastBlock = ipos/JagCfg::_BLOCK;
}

void JagDiskArrayBase::insertMergeGetMinMaxPair( JagDBPair &minpair, JagDBPair &maxpair, int mergeMode, 
	JagBuffReader *mbr, JagBuffBackReader *mbbr, char *mbuf, const char *sbuf )
{
	if ( mergeMode == 0 ) {
		FixMap::iterator it;
		FixMap::reverse_iterator it2;
		it = _pairmapcopy->_map->begin();
		minpair.point( it->first, it->second.first );
		it2 = _pairmapcopy->_map->rbegin();
		maxpair.point( it2->first, it2->second.first );
	} else if ( mergeMode == 1 ) {
		int rc = mbbr->getNext( mbuf );
		if ( rc ) {
			maxpair = JagDBPair( mbuf, KEYLEN );
		}
		rc = mbr->getNext( mbuf );
		if ( rc ) {
			minpair = JagDBPair( mbuf, KEYLEN );
		}
	} else {
		minpair = JagDBPair( sbuf, KEYLEN );
		maxpair = JagDBPair( sbuf, KEYLEN );
	}
}

// first, use while loop to find actual range which can hold all the flib elements
int JagDiskArrayBase::findMergeRegionFirstLast( abaxint &minfirst, abaxint &maxfirst, 
	abaxint &numBlocks, abaxint &ranArrlen, abaxint &ranElements, abaxint mergeElements )
{
	abaxint block = JagCfg::_BLOCK, istart = JAG_PARTIAL_RESIZE_HALF;
	abaxint localmin = _nthserv*_arrlen;
	abaxint localmax = localmin+_arrlen-block;
	localmin = localmin/block*block;
	localmax = localmax/block*block;
	
	bool rc = 0;
	while ( true ) {		
		if ( minfirst > localmin ) minfirst -= istart*block;
		if ( maxfirst < localmax ) maxfirst += istart*block;
		if ( minfirst < localmin ) minfirst = localmin;
		if ( maxfirst > localmax ) maxfirst = localmax;

		// check new range
		numBlocks = (maxfirst-minfirst)/block+1;
		ranArrlen = numBlocks*block;
		ranElements = getRegionElements( minfirst, numBlocks );

		if ( minfirst == localmin && maxfirst == localmax ) {
			// already full, whole file resize
			break;
		}
		
		if ( ranArrlen*(JAG_INSERTMERGE_GAP-1) > (ranElements+mergeElements)*JAG_INSERTMERGE_GAP ) {
			// has enough space, begin to do insert merge
			rc = 1;
			break;
		}
	}
	return rc;
}

// kvbuf: next buf
// dbuf, dbr: tmp buffer and buffReader for darr
// mpos: memory position for pair copy
// mbuf, mbr: tmp buffer and buffReader for darrnew
// sbuf: tmp buffer for single pair
// dgoNext: darr flag goNext
// mgoNext: merge flag ( pairarrcopy/darrnew/singlePair ) goNext
// int JagDiskArrayBase::getNextMergePair( char *kvbuf, char *dbuf, JagBuffReader &dbr, abaxint &mpos,
// 	char *mbuf, JagBuffReader *mbr, const char *sbuf, int &dgoNext, int &mgoNext, int mergeMode )
int JagDiskArrayBase::getNextMergePair( char *kvbuf, char *dbuf, JagBuffReader &dbr, FixMap::iterator &mpos,
 	char *mbuf, JagBuffReader *mbr, const char *sbuf, int &dgoNext, int &mgoNext, int mergeMode )
{
	int rc = 0;
	int w = 0;
	// const JagDBPair *ppair = NULL;
	JagDBPair ppair;
	if ( dgoNext > 0 ) {
		rc = dbr.getNext( dbuf );
		if ( rc == 0 ) {
			dgoNext = -1;
		} else {
			dgoNext = 0;
		}
	}
	
	rc = 0;
	if ( mergeMode == 0 ) {
		if ( mgoNext > 0 ) {
			w = 30;

			if ( mpos != _pairmapcopy->_map->end() ) {
				ppair.point( mpos->first, mpos->second.first );
				++mpos;
				rc = 1;
			}

			if ( rc == 0 ) {
				mgoNext = -1;
			} else {
				mgoNext = 0;
			}
		} else if ( mgoNext == 0 ) {

		 	if ( mpos == _pairmapcopy->_map->end() ) {
				FixMap::reverse_iterator it;
				it = _pairmapcopy->_map->rbegin();
				ppair.point( it->first, it->second.first );
				w = 100;
			} else if ( mpos != _pairmapcopy->_map->begin() ) {
				--mpos;
				ppair.point( mpos->first, mpos->second.first );
				++mpos;
				w = 200;
			} else {
				ppair.point( mpos->first, mpos->second.first );
				w = 300;
			}
		}		
	} else if ( mergeMode == 1 ) {
		if ( mgoNext > 0 ) {
			rc = mbr->getNext( mbuf );
			if ( rc == 0 ) {
				mgoNext = -1;
			} else {
				mgoNext = 0;
			}
		}
	} else {
		if ( mgoNext > 0 ) {
			mgoNext = 0;
		}		
	}

	
	// dgoNext and mgoNext must be 0 or -1 here
	if ( dgoNext == 0 && mgoNext == 0 ) {
		// disk and memory both read one pair and not to the end
		if ( mergeMode == 0 ) {
			if ( memcmp(dbuf, ppair.key.c_str(), KEYLEN) > 0 ) {
				// memory pair is small, use memory
				memcpy( kvbuf, ppair.key.c_str(), KEYLEN );
				memcpy( kvbuf+KEYLEN, ppair.value.c_str(), VALLEN );
				++_elements;
				mgoNext = 1;
			} else {
				// disk pair is small, use disk
				memcpy( kvbuf, dbuf, KEYVALLEN );
				dgoNext = 1;
			}			
		} else if ( mergeMode == 1 ) {
			if ( memcmp(dbuf, mbuf, KEYLEN) > 0 ) {
				// memory pair is small, use memory
				memcpy( kvbuf, mbuf, KEYVALLEN );
				++_elements;
				mgoNext = 1;
			} else {
				// disk pair is small, use disk
				memcpy( kvbuf, dbuf, KEYVALLEN );
				dgoNext = 1;
			}			
		} else {
			if ( memcmp(dbuf, sbuf, KEYLEN) > 0 ) {
				// memory pair is small, use memory
				memcpy( kvbuf, sbuf, KEYVALLEN );
				++_elements;
				mgoNext = -1;
			} else {
				// disk pair is small, use disk
				memcpy( kvbuf, dbuf, KEYVALLEN );
				dgoNext = 1;
			}			
		}
	} else if ( dgoNext == 0 && mgoNext == -1 ) {
		// disk has more data, while memory finishes reading, use disk
		memcpy( kvbuf, dbuf, KEYVALLEN );
		dgoNext = 1;		
	} else if ( dgoNext == -1 && mgoNext == 0 ) {
		// memory has more data, while disk finishes reading, use memory
		if ( mergeMode == 0 ) {
			memcpy( kvbuf, ppair.key.c_str(), KEYLEN );
			memcpy( kvbuf+KEYLEN, ppair.value.c_str(), VALLEN );
			++_elements;
			mgoNext = 1;
		} else if ( mergeMode == 1 ) {
			memcpy( kvbuf, mbuf, KEYVALLEN );
			++_elements;
			mgoNext = 1;
		} else {
			memcpy( kvbuf, sbuf, KEYVALLEN );
			++_elements;
			mgoNext = -1;
		}
	} else {
		// both memory and disk finish reading, no more data
		return 0;
	}
	
	return 1;
}

// debug use only
void JagDiskArrayBase::checkElementsCnt()
{
	abaxint cnt = 0;
	abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
	JagBuffReader br( this, _arrlen, KEYLEN, VALLEN, 0, 0, rlimit );
	char *kvbuf = (char*) jagmalloc ( KEYVALLEN+1 );
	char *oldbuf = (char*) jagmalloc ( KEYVALLEN+1 );
	kvbuf[KEYVALLEN] = '\0';
	memset( oldbuf, 0, KEYVALLEN+1 );
	while( br.getNext( kvbuf ) ) {
		if ( memcmp( kvbuf, oldbuf, KEYVALLEN ) < 0 ) {
			_blockIndex->print();
			prt(("invalid order check, abort()"));
			abort();
		}
		memcpy( oldbuf, kvbuf, KEYVALLEN );
		++cnt;
	}
	if ( cnt != _elements ) {
		prt(("s9999 invalid actrual data with elements, abort()"));
		abort();
	}
	if ( kvbuf ) free ( kvbuf );
	if ( oldbuf ) free ( oldbuf );
}

void JagDiskArrayBase::cleanRegionElements( abaxint first, abaxint length )
{
	abaxint rangeElements = 0;
	first /= JAG_BLOCK_SIZE;
	for ( abaxint i = 0; i < length; ++i ) {
		_blockIndex->cleanPartIndex( first+i );
	}
}

// debug this jdb file on this server;
// flag = 0, print all rows of this file, can provide range to print range of file;
// flag = 1, check if elements stored for each BLOCK_SIZE is correct or not
// flag = 2, show how many consecutive data of all files, part by part
// flag = 3, show how many consecutive data of all files based on each block, and show how many consecutive blocks 
void JagDiskArrayBase::debugJDBFile( int flag, abaxint limit, abaxint hold, abaxint instart, abaxint inend ) 
{
	abaxint start = _nthserv*_arrlen;
	abaxint end = _nthserv*_arrlen+_arrlen;
	abaxint totcnt = 0;
	abaxint printcnt = 0;
	char  *keybuf = (char*)jagmalloc( KEYLEN+1 );
	char  *keyvalbuf = (char*)jagmalloc( KEYVALLEN+1 );

	if ( _isdarrnew ) {
		start = 0;
		end = _arrlen;
	}

	prt(("debug jdb info dbobj=%s _arrlen=%lld _garrlen=%lld, _elements=%lld, _numOfResizes=%lld, KEYLEN=%d, VALLEN=%d, KEYVALLEN=%d\n", 
		_dbobj.c_str(), (abaxint)_arrlen, (abaxint)_garrlen, (abaxint)_elements, (abaxint)_numOfResizes, KEYLEN, VALLEN, KEYVALLEN ));
	prt(("debug jdb info _keyMode=%d, _isdarrnew=%d, _doneIndex=%d, _firstResize=%d, _insertUsers=%lld, _removeUsers=%lld, _minindex=%lld _maxindex=%lld, _uidORschema=%d, _fulls=%lld, _partials=%lld, _reads=%lld, _writes=%lld, _dupwrites=%lld, _upserts=%lld\n", 
		_keyMode, _isdarrnew, _doneIndex, _firstResize, (abaxint)_insertUsers, (abaxint)_removeUsers, (abaxint)_minindex, (abaxint)_maxindex, _uidORschema, _fulls, _partials, _reads, _writes, _dupwrites, _upserts ));	

	_diskLock->readLock( -1 );
	if ( 0 == flag ) {
		abaxint localstart, localend;
		if ( -1 != instart ) {
			localstart = instart + start;
		} else {
			localstart = start;
		}

		if ( -1 != inend ) {
			localend = inend + start;
		} else {
			localend = end;
		}

		if ( localstart < start || localstart > end ) {
			localstart = start;
		}

		if ( localend < start || localend > end ) {
			localend = end;
		}

		for ( abaxint i = localstart; i < localend; ++i ) {
			memset( keyvalbuf, 0, KEYVALLEN+1 );
			raypread( _jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN );
			memset( keybuf, 0, KEYLEN+1 );
			memcpy( keybuf, keyvalbuf, KEYLEN );
			prt(("%15ld   [%s] --> [%s]\n", i,  keybuf, keyvalbuf+KEYLEN ));
			++totcnt;
			++printcnt;
			if ( -1 != limit && totcnt > limit ) {
				break;
			} 
		}
	} else if ( 1 == flag ) {
		_memLock->readLock( -1 );
		abaxint cnt = 0;
		abaxint partcnt = 0;
		abaxint ipos = 0;
		for ( abaxint i = start; i < end; ++i ) {
			memset( keyvalbuf, 0, KEYVALLEN+1 );
			raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN );
			memset( keybuf, 0, KEYLEN+1 );
			memcpy( keybuf, keyvalbuf, KEYLEN );
			++totcnt;
			if ( *keybuf != '\0' ) { ++cnt; ++partcnt; }
			if ( i % JAG_BLOCK_SIZE == JAG_BLOCK_SIZE-1 ) {
				ipos = i;
				ipos /= JAG_BLOCK_SIZE;
				if ( partcnt != _blockIndex->getPartElements( ipos ) ) {
					prt(( "s9999 incorrect stored BLOCK_SIZE element, actual=%lld, stored=%lld, i=%lld, ipos=%lld\n", 
						partcnt, _blockIndex->getPartElements( ipos ), i, ipos ));
					++printcnt;
					abort();
				}
				partcnt = 0;
			}
		}	

		prt(("s3710 debug jdb info flag=1 actual datacnt=%lld memory elements=%lld\n\n", cnt, (abaxint)_elements ));
		if ( cnt != (abaxint)_elements ) {
			raydebug( stdout, JAG_LOG_LOW, "s3711 [%s] in debugcheck cnt=%l _elements=%l abort\n", _dbobj.c_str(), cnt, (abaxint)_elements );
			abort();
		}
		_memLock->readUnlock( -1 );
	} else if ( 2 == flag ) {
		abaxint cnt = 0;
		for ( abaxint i = start; i < end; ++i ) {
			memset( keyvalbuf, 0, KEYVALLEN+1 );
			raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN );
			memset( keybuf, 0, KEYLEN+1 );
			memcpy( keybuf, keyvalbuf, KEYLEN );
			+totcnt;
			if ( *keybuf != '\0' ) { ++cnt; }
			else {
				if ( cnt > 0 ) {
					if ( -1 != hold && cnt > hold ) {
						prt(( " s9999 %lld consecutive data exist in jdb file\n", cnt ));
						++printcnt;
					} else if ( -1 == hold ) {
						prt(( " s9999 %lld consecutive data exist in jdb file\n", cnt ));
						++printcnt;
					}
				}
				cnt = 0;
			}
		}
	} else if ( 3 == flag ) {
		abaxint cnt = 0;
		abaxint partcnt = 0;
		abaxint consecBlockcnt = 0;
		abaxint lastpos = -1;
		for ( abaxint i = start; i < end; ++i ) {
			memset( keyvalbuf, 0, KEYVALLEN+1 );
			raypread(_jdfs, keyvalbuf, KEYVALLEN, i*KEYVALLEN );
			memset( keybuf, 0, KEYLEN+1 );
			memcpy( keybuf, keyvalbuf, KEYLEN );
			++totcnt;
			if ( *keybuf != '\0' ) { ++cnt; ++partcnt; }
			if ( i % JAG_BLOCK_SIZE == JAG_BLOCK_SIZE-1 ) {
				if ( partcnt * 100 > hold * JAG_BLOCK_SIZE ) {
					if ( -1 == lastpos ) {
						++consecBlockcnt;
					} else if ( i/JAG_BLOCK_SIZE - 1 == lastpos ) {
						++consecBlockcnt;
					} else {
						prt(( " s9999 %lld consecutive blocks over %lld percent\n", consecBlockcnt, hold ));
						consecBlockcnt = 1;
					}
					lastpos = i/JAG_BLOCK_SIZE;
					prt(( " s9999 %lld consecutive data exist in block %lld, about %lld percent\n", 
						   partcnt, i/JAG_BLOCK_SIZE, partcnt*100/JAG_BLOCK_SIZE ));
					++printcnt;
				}
				partcnt = 0;
			}
		}
	} else if ( 4 == flag ) {
		int rc;
		abaxint ipos;
		memset( keybuf, 0,  KEYLEN + 1 );
		memset( keyvalbuf, 0,  KEYVALLEN + 1 );
		abaxint rlimit = getBuffReaderWriterMemorySize( _arrlen*KEYVALLEN/1024/1024 );
		JagBuffReader br( this, _arrlen, KEYLEN, VALLEN, _nthserv*_arrlen, 0, rlimit );
		while ( br.getNext( keyvalbuf, KEYVALLEN, ipos ) ) { 
			if ( keybuf[0] == '\0' ) {
				memcpy( keybuf, keyvalbuf, KEYLEN );
			} else {
				rc = memcmp( keyvalbuf, keybuf, KEYLEN );
				if ( rc <= 0 ) {
					abort();
				} else {
					memcpy( keybuf, keyvalbuf, KEYLEN );
				}
			}
		}
	} // ... more debug later

	_diskLock->readUnlock( -1 );
	free( keyvalbuf );
	free( keybuf );

	prt(("Done debug jdb info printcnt=%lld totcnt=%lld\n\n", printcnt, totcnt ));
}


void JagDiskArrayBase::logInfo( abaxint t1, abaxint t2, abaxint cnt, const JagDiskArrayBase *jda )
{
	if ( t1 > 1000 ) { // in sec
		t1 /= 1000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l s flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 60000 ) { // in min
		t1 /= 60000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l m flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 3600000 ) { // in hour
		t1 /= 3600000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l h flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 86400000 ) { // in day
		t1 /= 86400000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l d flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 2592000000 ) { // in month
		t1 /= 2592000000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l M flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else if ( t1 > 31104000000 ) { // in year
		t1 /= 31104000000;
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l Y flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	} else { // in millisec
		raydebug(stdout, JAG_LOG_HIGH, "s6028 flib %s %d wt=%l ms flsh=%l ms %l/%l/%l/%l/%l/%l/%l\n",
			jda->_dbobj.c_str(), cnt, t1, t2, jda->_writes, jda->_upserts, jda->_dupwrites, 
			jda->_reads, jda->_shifts, jda->_insmrgcnt, jda->_insdircnt );
	}
}
