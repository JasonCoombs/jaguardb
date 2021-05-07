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
#include <values.h>

#include <JagDiskArrayFamily.h>
#include <JagDiskArrayServer.h>
#include <JagSingleBuffReader.h>
#include <JagFileMgr.h>
#include <JagFamilyKeyChecker.h>
#include <JagDiskKeyChecker.h>
#include <JagFixKeyChecker.h>
#include <JagDBMap.h>
#include <JagCompFile.h>

JagDiskArrayFamily::JagDiskArrayFamily( const JagDBServer *servobj, const Jstr &filePathName, const JagSchemaRecord *record, 
									    jagint length, bool buildInitIndex ) : _schemaRecord(record)
{
	prt(("s100439 JagDiskArrayFamily ctor filePathName=[%s]\n", filePathName.c_str() ));
	// filePathName=/home/dev2/jaguar/data/test/jbench/jbench
	int kcrc = 0;
	_insdelcnt = 0;
	_KLEN = record->keyLength;
	_VLEN = record->valueLength;
	_KVLEN = _KLEN + _VLEN;
	_servobj = (JagDBServer*)servobj;
	//_lock = newJagReadWriteLock();
	_pathname = filePathName;
	if ( JAG_MEM_LOW == servobj->_memoryMode ) {
		_keyChecker = new JagDiskKeyChecker( _pathname, _KLEN, _VLEN );
	} else {
		_keyChecker = new JagFixKeyChecker( _pathname, _KLEN, _VLEN );
	}

	Jstr existFiles, fullpath, objname;
	const char *p = strrchr( filePathName.c_str(), '/' );
	if ( p == NULL ) {
		printf("s7482 error _pathname=%s, exit\n", _pathname.c_str() );
		fflush(stdout);
		exit(34);
	}

    _insertBufferMap = new JagDBMap();

	fullpath = Jstr( filePathName.c_str(), p-filePathName.c_str() );
	//prt(("s08283 fullpath=%s\n", fullpath.c_str() )); // /home/dev2/jaguar/data/test/jbench
	_tablepath = fullpath; 
	_sfilepath = fullpath + "/files";
	JagStrSplit sp(fullpath, '/');
	int splen = sp.length();
	//_taboridxname = Jstr( p+1 );
	_taboridxname = sp[splen-1];
	_dbname = sp[splen-2];
	objname = _taboridxname + ".jdb";
	_objname = objname;
	existFiles = JagFileMgr::getFileFamily( fullpath, objname );
	//prt(("s404338 getFileFamily existFiles=[%s]\n", existFiles.c_str() ));
	if ( existFiles.size() > 0 ) {	
		//prt(("s100032 buildInitKeyCheckerFromSigFile...\n"));
		kcrc = _keyChecker->buildInitKeyCheckerFromSigFile();

		JagStrSplit split( existFiles, '|' );
		// need to rearrange files by order, and remove .jdb
		JagVector<Jstr> paths( split.length() );
		int filenum;
		for ( int i = 0; i < split.length(); ++i ) {
			const char *ss = strrchr( split[i].c_str(), '/' );
			if ( !ss ) ss = split[i].c_str();
			JagStrSplit split2( ss, '.' );
			if ( split2.length() < 2 ) {
				raydebug(stdout, JAG_LOG_LOW, "s74182 fatal error _pathname=%s, exit\n", split[i].c_str() );
				exit(35);
			}

			filenum = atoi( split2[split2.length()-2].c_str() );
			if ( filenum >= split.length() ) {
				raydebug( stdout, JAG_LOG_LOW, "s74183 fatal error family has %d files, but %d %s >= max files\n", 
							split.length(), filenum, split[i].c_str() );
				exit(36);
			}

			const char *q = strrchr( split[i].c_str(), '.' );
			paths[filenum] = Jstr(split[i].c_str(), q-split[i].c_str());
			//prt(("s50928 i=%d paths[%d]=[%s]\n", i, filenum, paths[filenum].c_str() ));
			paths._elements++;
		}

		jagint cnt;
		prt(("s51230 paths.size=%d\n", paths.size() ));
		for ( int i = 0; i < paths.size(); ++i ) {
			JagDiskArrayServer *darr = new JagDiskArrayServer( servobj, this, i, paths[i], record, length, buildInitIndex );
				                        // length,  i==paths.size()-1 );
			_darrlist.append(darr);
			//_activepos = _darrlist.size()-1;
			//raydebug( stdout, JAG_LOG_LOW, "s6274 _activepos =%d\n", _activepos );
		}

		bool  readJDB = false;
		if ( 0 == kcrc ) {
			readJDB = true;
			raydebug( stdout, JAG_LOG_LOW, "s6281 skip sig or hdb\n");
		} else {
			// redo log has not done, getElements() will not have memory elements
			/***
			if ( getElements() != _keyChecker->size() ) {
				// remove old .hdb file if exists
				raydebug( stdout, JAG_LOG_LOW, "s6381 famelems not equal hdb elements\n");
				Jstr hdbpath = _keyChecker->getPath() + ".hdb";
				if ( JagFileMgr::exist( hdbpath ) ) {
					jagunlink( hdbpath.c_str() );
				}
				readJDB = true;
			}
			***/
			//prt(("s10292 kcrc true  has read sigfile OK getElements()=%d _keyChecker->size()=%d\n",  getElements(), _keyChecker->size() ));
		}

		// read JDB files and build keychecker
		if ( readJDB ) {
			for ( int i = 0; i < _darrlist.size(); ++i ) {
				cnt = addKeyCheckerFromJDB( _darrlist[i], i );
				raydebug( stdout, JAG_LOG_LOW, "s3736 obj=%s %d/%d readjdb cnt=%l\n", _objname.c_str(), i,  _darrlist.size(), cnt );
			}
		}
	}

	_isFlushing = 0;
	_doForceFlush = false;
}

// reach each jdb file and update keychecker
jagint JagDiskArrayFamily::addKeyCheckerFromJDB( JagDiskArrayServer *ldarr, int activepos )
{
	prt(("s392838 addKeyCheckerFromJDB activepos=%d\n", activepos ));
	char vbuf[3]; vbuf[2] = '\0';
	int div, rem;

 	char *kvbuf = (char*)jagmalloc( _KVLEN+1 + JAG_KEYCHECKER_VLEN);
	memset( kvbuf, 0,  _KVLEN + 1 + JAG_KEYCHECKER_VLEN );
	jagint rlimit = getBuffReaderWriterMemorySize( ldarr->_arrlen*_KVLEN/1024/1024 );
	JagBuffReader nav( ldarr, ldarr->_arrlen, _KLEN, _VLEN, ldarr->_nthserv*ldarr->_arrlen, 0, rlimit );
				
	div = activepos / (JAG_BYTE_MAX+1);
	rem = activepos % (JAG_BYTE_MAX+1);
	vbuf[0] = div; 
	vbuf[1] = rem;

	jagint cnt = 0;
	jagint cntDone = 0;
	while ( nav.getNext( kvbuf ) ) { 
		memcpy(kvbuf+_KLEN, vbuf, 2 );
		if ( _keyChecker->addKeyValueNoLock( kvbuf ) ) {
			++cntDone;
		}

		++cnt;
	}
	free( kvbuf );

	prt(("s1029383 addKeyCheckerFromJDB cnt=%d cntDone=%d\n", cnt, cntDone ));
	return cnt;
}

// reach each jdb file and update keychecker
jagint JagDiskArrayFamily::addKeyCheckerFromInsertBuffer( int darrNum )
{
	char vbuf[3]; vbuf[2] = '\0';
	int div, rem;

 	char *kvbuf = (char*)jagmalloc( _KLEN+1 + JAG_KEYCHECKER_VLEN);
				
	div = darrNum / (JAG_BYTE_MAX+1);
	rem = darrNum % (JAG_BYTE_MAX+1);
	vbuf[0] = div; 
	vbuf[1] = rem;

	jagint cntDone = 0;
	JagFixMapIterator iter = _insertBufferMap->_map->begin();
	while ( iter != _insertBufferMap->_map->end() ) {
		// copy _KLEN bytes from iter.first to keyvalbuf
		memset( kvbuf, 0,  _KLEN + 1 + JAG_KEYCHECKER_VLEN );
		//memcpy( kvbuf, iter->first.c_str(), _KLEN );
		memcpy( kvbuf, iter->first.c_str(), iter->first.size() );
		// copy vbuf to val part
		memcpy(kvbuf+_KLEN, vbuf, JAG_KEYCHECKER_VLEN );

		//prt(("\n"));
		if ( _keyChecker->addKeyValueNoLock( kvbuf ) ) {
			//prt(("s222006  _keyChecker->addKeyValueNoLock( [%s][%d]\n", kvbuf, darrNum ));
			++cntDone;
		}
		++ iter;
	}
	free( kvbuf );
	return cntDone;
}

// dtor
JagDiskArrayFamily::~JagDiskArrayFamily()
{
	if ( _keyChecker ) {
		delete _keyChecker;
		_keyChecker = NULL;
	}

	for ( int i = 0; i < _darrlist.size(); ++i ) {
		if ( _darrlist[i] ) {
			delete _darrlist[i];
			_darrlist[i] = NULL;
		}
	}

	// destroyMonitor(); // delete insertBufferMap
    if ( _insertBufferMap ) { 
		prt(("s22203838 s9999  delete _insertBufferMap \n"));
		delete _insertBufferMap; 
		_insertBufferMap=NULL; 
	}
}


// protected by _insertBufferMutex: _insertBufferMap, _family
jagint JagDiskArrayFamily::processFlushInsertBuffer() 
{
	//prt(("s3004 processFlushInsertBuffer ...\n" ));
	jagint cnt = 0; 
	bool doneFlush = false;

	//prt(("s19834 _darrlist.size=%d\n", _darrlist.size() ));
	if ( _darrlist.size() < 1 ) {
		doneFlush = false;  // new darr will be added, see below
		//raydebug(stdout, JAG_LOG_LOW, "s200828 first seg, new darr will be added\n" );
	} else {
		// find min-cost file and try to merge
		JagVector<JagMergeSeg> vec;
		int mtype;
		//prt(("s448810 findMinCostFile ...\n"));
		int which = findMinCostFile( vec, _doForceFlush, mtype ); // uses _insertBufferMap and _darrlist
		//prt(("s448810 findMinCostFile done which=%d mtype=%d\n", which, mtype ));
		if ( which < 0 ) {
			raydebug(stdout, JAG_LOG_LOW, "s2647 min-cost file not found, _darrlist.size=%d\n", _darrlist.size() );
		} else {
			if (  mtype == JAG_MEET_TIME ) {  
				// flush time is small
				//raydebug(stdout, JAG_LOG_LOW, "s2644 min-cost file found, MEET_TIME which=%d ...\n", which );
				//prt(("s4401851 fam mergeBufferToFile which=%d ...\n", which ));
				cnt = _darrlist[which]->mergeBufferToFile( _insertBufferMap, vec );	
				//prt(("s4502852 fam mergeBufferToFile done elements cnt=%ld\n", cnt ));
				doneFlush = true;
				//jagint addKC = addKeyCheckerFromInsertBuffer( which );
				addKeyCheckerFromInsertBuffer( which );
				//prt(("s20395 merge buffer to darr=%d added keychecker addKC=%d/%d\n", which, addKC, _insertBufferMap->size() ));
				//raydebug(stdout, JAG_LOG_LOW, "s30881 min-cost file found and MEET_TIME merged to which=%d %d\n", which, addKC );
			} else {
				//raydebug(stdout, JAG_LOG_LOW, "s2645 min-cost file found, but no MEET_TIME found which=%d ...\n", which );
			}
		}
	}

	if ( ! doneFlush ) {
		JagDiskArrayServer *extraDarr = flushBufferToNewFile();
		if ( extraDarr != nullptr ) {

			_darrlist.append( extraDarr );

			int which = _darrlist.size() - 1; 
			cnt = extraDarr->size()/_KVLEN;

			addKeyCheckerFromInsertBuffer( which );
			doneFlush = true;
		} else {
			raydebug(stdout, JAG_LOG_LOW, "s20345 Error flushBufferToNewFile()\n" ); 
		}
	}

	if ( doneFlush ) {
		raydebug(stdout, JAG_LOG_LOW, "cleanup insertbuffer\n" ); 
		_insertBufferMap->clear();

		removeAndReopenWalLog();

	 	jagmalloc_trim(0);
	}

	return cnt;
}

// input:  _insertBufferMap and  _darrlist
// remember regions in insertbuffer to be merged to simp files (in case meeting requirement)
int JagDiskArrayFamily::findMinCostFile( JagVector<JagMergeSeg> &vec, bool forceFlush, int &mtype )
{
	// MB bytes/second
	jagint sequentialReadSpeed = _servobj->_cfg->getLongValue("SEQ_READ_SPEED", 200);
	jagint sequentialWriteSpeed = _servobj->_cfg->getLongValue("SEQ_WRITE_SPEED", 150);
	float  allowedFlushTimeLimit  = _servobj->_cfg->getFloatValue("MAX_FLUSH_TIME", 1.0); 

	float spendSeconds = 0;
	float minTime = FLT_MAX;
	int minDarr = -1;
	JagVector<JagMergeSeg> oneVec;

	for ( int i = 0 ; i < _darrlist.size(); ++i ) {
		oneVec.clear();
		spendSeconds = _darrlist[i]->computeMergeCost( _insertBufferMap, sequentialReadSpeed, sequentialWriteSpeed, oneVec );	
		if ( spendSeconds < minTime ) {
			minTime = spendSeconds;
			minDarr = i;
			vec = oneVec;
			//break; // for debug only, choose the first one, to be removed totally in production
		}
	}

	if ( forceFlush ) {
		mtype = JAG_MEET_TIME;
		return minDarr;
	}

	//prt(("s2093444 minTime=%f\n", minTime ));
	if ( minTime <= allowedFlushTimeLimit ) {
		mtype = JAG_MEET_TIME;
		return minDarr;
	} else {
		mtype = 0;
		return -1;
	}
}

// return 0: if exists
// return 1: if not exist
int JagDiskArrayFamily::insert( const JagDBPair &pair, bool doFirstRedist, 
							    JagDBPair &retpair, bool noDupPrint )
{
	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	if ( _keyChecker && _keyChecker->exist( kbuf ) ) {
		prt(("s303928 JagDiskArrayFamily::insert _keyChecker->exist(%s) return 0\n", kbuf ));
		//_keyChecker->print();
		return 0;
	}

	// push to buffer
    int rc;
	if ( _insertBufferMap->exist( pair ) ) {
		prt(("s2038271 _pathname=[%s]\n", _pathname.s() ));
		// _insertBufferMap->print();
		return 0;
	}

    rc = _insertBufferMap->insert( pair );
    if ( !rc ) ++_dupwrites;
	prt(("s22029 _insertBufferMap->insert rc=%d elem=%d\n", rc, _insertBufferMap->elements() ));

	jagint  currentCnt = _insertBufferMap->elements();
	jagint  currentMem = currentCnt *_KVLEN;
	if ( currentMem < JAG_SIMPFILE_LIMIT_BYTES ) {
		return 1; // done put to buffer
	}

	// flush the buffer
	processFlushInsertBuffer();
	
    return 1;
}

jagint JagDiskArrayFamily::getCount( )
{
	jagint mem = 0;
	jagint kchn = 0;
	if ( _insertBufferMap && _insertBufferMap->size() > 0 ) {
		mem = _insertBufferMap->size();
	}

	kchn = _keyChecker->size();
	return mem+kchn;	
}

jagint JagDiskArrayFamily::getElements( )
{
	return getCount();
}

bool JagDiskArrayFamily::isFlushing()
{
	return _isFlushing;
}

bool JagDiskArrayFamily::remove( const JagDBPair &pair )
{
	int rc = 0;
	int pos = 0, div, rem;
	char v[2];

	if ( _insertBufferMap && _insertBufferMap->remove( pair ) ) {
		return true;
	}

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		rc = _darrlist[pos]->remove( pair );

		if ( rc ) {
			rc = _keyChecker->removeKey( pair.key.c_str() );
		}
		return rc;
	} else {
		return 0;
	}
}

// check if pair exist and return data in pair
bool JagDiskArrayFamily::exist( JagDBPair &pair )
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	// check insertBuffer first
	if ( _insertBufferMap && _insertBufferMap->get( pair ) ) {
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	//jagint idx;
	char v[2];

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	//if ( _keyChecker->getValue( pair.key.c_str(), v ) ) 
	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->exist( pair );
		return rc;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::get( JagDBPair &pair )
{
	// check insertBuffer first
	if ( _insertBufferMap && _insertBufferMap->get( pair ) ) {
		//prt(("s222020 _insertBufferMap got pair=[%s][%s]\n", pair.key.c_str(), pair.value.c_str() ));
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	char v[2];

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );
	/**
	//prt(("s33338 pair.key.dump:\n"));
	//pair.key.print();
	**/

	//if ( _keyChecker->getValue( pair.key.c_str(), v ) ) 
	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		rc = _darrlist[pos]->get( pair );

		//prt(("s222023 darr filenum=%d got pair=[%s][%s]\n", pos, pair.key.c_str(), pair.value.c_str() ));
		return rc;
	} else {
		// prt(("s8282 JagDiskArrayFamily get pair.key=[%s] return 0\n", pair.key.c_str() ));
		return 0;
	}
}

// Directly update pair in buffer (if key exists) or diskarr (if key exists)
// return true for OK; false error
bool JagDiskArrayFamily::set( const JagDBPair &pair )
{
	//prt(("s242023 JagDiskArrayFamily::set pair=[%s][%s]\n", pair.key.c_str(), pair.value.c_str() ));
	if ( _insertBufferMap && _insertBufferMap->set( pair ) ) {
		//prt(("s242024 JagDiskArrayFamily::set pair=[%s][%s] _insertBufferMap set true\n", pair.key.c_str(), pair.value.c_str() ));
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	char v[2];
	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );
	//if ( _keyChecker->getValue( pair.key.c_str(), v ) ) 
	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		rc = _darrlist[pos]->set( pair );

		return rc;
	} else {
		//prt(("s242026 JagDiskArrayFamily::set pair=[%s][%s] not in keychecker return 0\n", pair.key.c_str(), pair.value.c_str() ));
		return 0;
	}
}

// Update a record with condition
bool JagDiskArrayFamily::setWithRange( const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
										ExprElementNode *root, const JagParseParam *pParam, int numKeys, const JagSchemaAttribute *schAttr, 
										jagint setposlist[], JagDBPair &retpair )
{
	bool rc = get(pair); 
	if ( ! rc ) {
		//prt(("s939393 JagDiskArrayFamily::setWithRange get false return false\n"));
		return false;
	}
	//prt(("s22233 JagDiskArrayFamily::setWithRange got pair=[%s][%s]\n", pair.key.c_str(), pair.value.c_str() ));

	// get retpair from here
    rc = JagDiskArrayBase::checkSetPairCondition( _servobj, req, pair, (char**)buffers, uniqueAndHasValueCol, root, pParam,
                                				   numKeys, schAttr, _KLEN, _VLEN, setposlist, retpair );
    if ( ! rc ) return false;

	//prt(("s22233 JagDiskArrayFamily::setWithRange retpair=[%s][%s]\n", retpair.key.c_str(), retpair.value.c_str() ));
	if ( _insertBufferMap && _insertBufferMap->set( retpair ) ) {
		//prt(("s222029 _insertBufferMap->set OK return true\n"));
		return true;
	}

	int pos = 0;
	int div, rem;
	char v[2];

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		// modify data record
		rc = _darrlist[pos]->set (retpair ); 

		//prt(("s2220 filenum=pos=%d rc=%d\n", pos, rc ));
		return rc;
	} else {
		//prt(("s5091 _keyChecker->getValue false key=[%s]\n", pair.key.c_str() ));
		return 0;
	}
}


void JagDiskArrayFamily::drop()
{
	// JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	for ( int i = 0; i < _darrlist.size(); ++i ) {
		_darrlist[i]->drop();
	}
	_keyChecker->removeAllKey();
}

void JagDiskArrayFamily::flushBlockIndexToDisk()
{
	for ( int i = 0; i < _darrlist.size(); ++i ) {
		raydebug( stdout, JAG_LOG_LOW, "    Flush Index File %d/%d ...\n", i, _darrlist.size() );
		_darrlist[i]->flushBlockIndexToDisk();
	}
	
	// also, flush key checker md5 string to disk
	if ( JAG_MEM_HIGH == _servobj->_memoryMode ) {
		raydebug( stdout, JAG_LOG_LOW, "    Flush Memory ...\n" );
		flushKeyCheckerString();
	}
}

// read bottom level of blockindex and write it to a disk file
void JagDiskArrayFamily::flushKeyCheckerString()
{
	if( _darrlist.size() < 1 ) {
		//prt(("s002811 _darrlist is zero, return\n"));
		return;
	}

	JagStrSplit sp(  _darrlist[0]->_filePath, '.' );
	Jstr keyCheckerPath = sp[0] + ".sig";
	//prt(("s1122201 flushKeyCheckerString _darrlist[0]->_filePath=[%s]\n", _darrlist[0]->_filePath.c_str() ));

	int fd = jagopen( keyCheckerPath.c_str(), O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
	if ( fd < 0 ) {
		printf("s3804 error open [%s] for write\n", keyCheckerPath.c_str() );
		return;
	}
	
	int klen;
	if ( _KLEN <= JAG_KEYCHECKER_KLEN ) {
		klen = _KLEN;
	} else {
		klen = JAG_KEYCHECKER_KLEN;
	}
	int vlen = 2;
	int kvlen = klen + vlen;
	char buf[2];
	buf[0] = '1';
	raysafewrite( fd, buf, 1 ); 

	// begin flush key checker map to disk
	const char *arr = _keyChecker->array();
	jagint len = _keyChecker->arrayLength();
	if ( len >= 1 ) {
		for ( jagint i = 0; i < len; ++i ) {
			if ( arr[i*kvlen] == '\0' ) continue;
			raysafewrite( fd, arr+i*kvlen, kvlen );
		}
	}
	buf[0] = '0';
	raysafepwrite( fd, buf, 1, 0 );
	jagfdatasync( fd );
	jagclose( fd );
}

// set range family of all jdbs
jagint JagDiskArrayFamily::setFamilyRead( JagMergeReader *&nts, const char *minbuf, const char *maxbuf ) 
{
	//prt(("s222110 setFamilyRead\n"));
	bool startFlag = false;
    jagint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	jagint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_KLEN+1);
			memset(lminbuf, 0, _KLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_KLEN+1);
			memset(lmaxbuf, 255, _KLEN);
			lmaxbuf[_KLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		//prt(("s4480283 minbuf || maxbuf _darrlist.size()=%d\n", _darrlist.size() ));
		JagDBPair minpair( JagFixString( minbuf, _KLEN, _KLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _KLEN, _KLEN ), value );
		for ( int i = _darrlist.size()-1; i >= 0; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			rlimit = index - slimit + 1;
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = 128;
				fRange.append( tempRange );
			}
		}
	} else {
		//prt(("s4140283 no minbuf no maxbuf _darrlist.size()=%d\n", _darrlist.size() ));
		for ( int i = _darrlist.size()-1; i >= 0; --i ) {
			tempRange.darr = _darrlist[i];
			tempRange.startpos = -1;
			tempRange.readlen = -1;
			tempRange.memmax = 128;
			fRange.append( tempRange );
		}
	}
	
	if ( nts ) {
		delete nts;
		nts = NULL;
	}
	
	//prt(("s2222037 before new JagMergeReader frange.size=%d\n", fRange.size() ));

	if ( fRange.size() >= 0 ) {
		nts = new JagMergeReader(this->_insertBufferMap, fRange, fRange.size(), 
								 _KLEN, _VLEN, minbuf, maxbuf );
	} else {
		nts = NULL;
	}

	if ( lminbuf ) { free( lminbuf ); }
	if ( lmaxbuf ) { free( lmaxbuf ); }
	return maxPossibleElem;
}

// set range family of range jdbs
jagint JagDiskArrayFamily::setFamilyReadPartial( JagMergeReader *&nts, const char *minbuf, const char *maxbuf, 
												  jagint spos, jagint epos, jagint mmax ) 
{
	//prt(("s204949 JagDiskArrayFamily::setFamilyReadPartial spos=%d  epos=%d fam=%0x\n", spos, epos, this ));
	if ( spos < 0 ) spos = 0;
	if ( epos >= _darrlist.size() ) epos = _darrlist.size()-1;
	if ( epos < 0 ) epos = 0;
	if ( spos > epos ) spos = epos;
	int darrsize = _darrlist.size();
	//prt(("s10393 darrsize=%d\n", darrsize ));

	bool startFlag = false;
    jagint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	jagint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_KLEN+1);
			memset(lminbuf, 0, _KLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_KLEN+1);
			memset(lmaxbuf, 255, _KLEN);
			lmaxbuf[_KLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		JagDBPair minpair( JagFixString( minbuf, _KLEN, _KLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _KLEN, _KLEN ), value );
		for ( int i = epos; i >= spos && darrsize > 0; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			//prt(("s12233 minpair rc=%d index=%d\n", rc, index ));
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			//prt(("s12234 maxpair rc=%d index=%d\n", rc, index ));
			if ( startFlag && !rc ) ++index;
			rlimit = index - slimit + 1;
			//prt(("s12235 index=%d slimit=%d rlimit=readlen=%d\n", index, slimit, rlimit ));
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = mmax;
				/*** seems not ue full 3-21-2020
				JAG_BLURT _darrlist[i]->_memLock->readLock( -1 ); JAG_OVER;
				maxPossibleElem += _darrlist[i]->getRegionElements( slimit, rlimit/JAG_BLOCK_SIZE );
				_darrlist[i]->_memLock->readUnlock( -1 );
				***/
				//prt(("s23203 i=%d slimit=%d  tempRange.readlen=%d index=%d rc=%d\n", i, slimit, tempRange.readlen, index, rc ));
				fRange.append( tempRange );
			}
		}
	} else {
		for ( int i = epos; i >= spos && darrsize > 0; --i ) {
			tempRange.darr = _darrlist[i];
			tempRange.startpos = -1;
			tempRange.readlen = -1;
			tempRange.memmax = mmax;
			fRange.append( tempRange );
		}
	}

	if ( nts ) {
		delete nts;
		nts = NULL;
	}
	
	//if ( fRange.size() > 0 ) {
	if ( fRange.size() >= 0 ) {
		prt(("s209484  new JagMergeReader  this->_insertBufferMap=%0x fRange.size=%d\n",  this->_insertBufferMap, fRange.size() ));
		nts = new JagMergeReader( this->_insertBufferMap, fRange, fRange.size(), 
								  _KLEN, _VLEN, minbuf, maxbuf );
		prt(("s209484  new JagMergeReader done nts=%0x  nts->_dbmap=%0x\n", nts, nts->_dbmap ));
	} else {
		nts = NULL;
	}

	if ( lminbuf ) { free( lminbuf ); }
	if ( lmaxbuf ) { free( lmaxbuf ); }
	return maxPossibleElem;
}

jagint JagDiskArrayFamily::setFamilyReadBackPartial( JagMergeBackReader *&nts, const char *minbuf, const char *maxbuf, 
												  	  jagint spos, jagint epos, jagint mmax ) 
{
	/***
	if ( spos < 0 ) spos = 0;
	if ( epos >= _darrlist.size() ) epos = _darrlist.size()-1;
	if ( epos < 0 ) epos = 0;
	***/
	prt(("s883933 setFamilyReadBackPartial spos=%d epos=%d\n", spos, epos ));
	int  darrsize = _darrlist.size();

	if ( spos > epos ) spos = epos;
	bool startFlag = false;
    jagint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	jagint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_KLEN+1);
			memset(lminbuf, 0, _KLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_KLEN+1);
			memset(lmaxbuf, 255, _KLEN);
			lmaxbuf[_KLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		JagDBPair minpair( JagFixString( minbuf, _KLEN, _KLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _KLEN, _KLEN ), value );
		for ( int i = epos; i >= spos && darrsize > 0; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			rlimit = index - slimit + 1;
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit+rlimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = mmax;
				/*** seems not useful 3-21-2020
				JAG_BLURT _darrlist[i]->_memLock->readLock( -1 ); JAG_OVER;
				maxPossibleElem += _darrlist[i]->getRegionElements( slimit, rlimit/JAG_BLOCK_SIZE );
				_darrlist[i]->_memLock->readUnlock( -1 );
				***/
				fRange.append( tempRange );
			}
		}
	} else {
		for ( int i = epos; i >= spos && darrsize > 0; --i ) {
			tempRange.darr = _darrlist[i];
			tempRange.startpos = -1;
			tempRange.readlen = -1;
			tempRange.memmax = mmax;
			fRange.append( tempRange );
		}
	}

	if ( nts ) {
		delete nts;
		nts = NULL;
	}
	
	//if ( fRange.size() > 0 ) {
	if ( fRange.size() >= 0 ) {
		nts = new JagMergeBackReader( this->_insertBufferMap, fRange, fRange.size(), 
									  _KLEN, _VLEN, minbuf, maxbuf );
	} else {
		nts = NULL;
	}

	if ( lminbuf ) { free( lminbuf ); }
	if ( lmaxbuf ) { free( lmaxbuf ); }
	return maxPossibleElem;
}

// from waitAndFlushInsertBuffer() which locked _insertBufferMutex
JagDiskArrayServer* JagDiskArrayFamily::flushBufferToNewFile( )
{
	if (  _insertBufferMap->elements() < 1 ) { 
		prt(("s183330 flushBufferToNewFile no data in mem, return nullptr\n"));
		return nullptr; 
	}
	prt(("s183332 flushBufferToNewFile dataitems=%d in mem \n", _insertBufferMap->elements() ));
	_isFlushing = 1;
	//jagint cnt = 0;
	//int rc;
	int darrlistlen = _darrlist.size();
	prt(("s40726 JagDiskArrayFamily::flushBufferToNewFile darrlistlen=%d\n", darrlistlen ));
	jagint len = _insertBufferMap->size();
	// _pathname="/a/b/a/aaa"
	// _pathname="/a/b/a/abc.2"
	prt(("s220983 _pathname=[%s]\n", _pathname.s() ));
	JagStrSplit sp(_pathname, '/');
	int slen = sp.length();
	Jstr fname= sp[slen-1];
	Jstr filePathName;
	if ( ! strchr(fname.c_str(), '.') ) {
		filePathName = _pathname + "." + intToStr( darrlistlen );
		prt(("s220183 filePathName=[%s]\n", filePathName.s() ));
	} else {
		const char *p = strrchr( _pathname.c_str(), '.' );
		Jstr newpathname = Jstr( _pathname.c_str(),  p-_pathname.c_str() );
		// take abc
		filePathName = newpathname + "." + intToStr( darrlistlen );
		prt(("s220184 filePathName=[%s]\n", filePathName.s() ));
	}

	//raydebug(stdout, JAG_LOG_LOW, "s56845 flushBufferToNewFile _pathname=[%s]\n", _pathname.c_str() );
	//raydebug(stdout, JAG_LOG_LOW, "s56845 flushBufferToNewFile  filePathName=[%s]\n", filePathName.c_str() );
	//JagSingleBuffWriter *sbw = NULL;
	//jagint dblimit = 64; 

	JagDiskArrayServer *darr = new JagDiskArrayServer( _servobj, this, darrlistlen, filePathName, _schemaRecord, len, false );
	//prt(("s12099 flushBufferToNewFile...\n"));
	darr->flushBufferToNewFile( _insertBufferMap );
	//prt(("s12099 flushBufferToNewFile done...\n"));

	// remove batch insert wal file and re-open
	//removeAndReopenWalLog();

	return darr;
}

void JagDiskArrayFamily::removeAndReopenWalLog()
{
	Jstr dbtab = _dbname + "." + _taboridxname;
    Jstr walfpath = _servobj->_cfg->getWalLogHOME() + "/" + dbtab + ".wallog";
	prt(("s201226 reset wallog file %s ...\n", walfpath.c_str() ));
	raydebug(stdout, JAG_LOG_LOW, "cleanup wagllog %s \n", walfpath.s() ); 

    FILE *walFile = _servobj->_walLogMap.ensureFile( walfpath );
	fclose( walFile );
    _servobj->_walLogMap.removeKey( walfpath );
	jagunlink( walfpath.c_str() );

    _servobj->_walLogMap.ensureFile( walfpath );

}

jagint JagDiskArrayFamily::memoryBufferSize() 
{
    //jaguar_mutex_lock( &_insertBufferMutex );
	if ( NULL == _insertBufferMap ) {
    	//jaguar_mutex_unlock( &_insertBufferMutex );
		return 0;
	}
	jagint cnt = _insertBufferMap->size();
    //jaguar_mutex_unlock( &_insertBufferMutex );
	return cnt;
}

#if 0
// ttime can be microseconds,nanoseconds, seconds
jagint JagDiskArrayFamily::cleanupOldRecordsByOrder( time_t ttime )
{
	if ( _insertBufferMap ) { 
		// find end positon
		JagFixMapIterator prevIter = _insertBufferMap->getPred();
		JagFixMapIterator iter = _insertBufferMap->_map->begin();
		// time_t tv;
		while ( iter != prevIter ) {
			// C++11 allows delete while iterating
			// _insertBufferMap->cleanupOldRecords( secs );
			//iter.first is key  .second is value
			//tv = rayatol(iter.first.s()+offset, length)
			_keyChecker->removeKey( iter.first.s() );
			iter = _insertBufferMap->_map->erase( iter );
		}
	}

	/*** C++ delete items while iterating
	std::map<K, V>::iterator itr = myMap.begin();
	while (itr != myMap.end()) {
    	if (ShouldDelete(*itr)) {
       		itr = myMap.erase(itr);
    	} else {
       		++itr;
    	}
	}
	***/
}
#endif



