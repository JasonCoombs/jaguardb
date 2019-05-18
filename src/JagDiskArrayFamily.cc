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

#include <JagDiskArrayFamily.h>
#include <JagDiskArrayServer.h>
#include <JagSingleBuffReader.h>
#include <JagFileMgr.h>
#include <JagFamilyKeyChecker.h>
#include <JagDiskKeyChecker.h>
#include <JagFixKeyChecker.h>

JagDiskArrayFamily::JagDiskArrayFamily( const JagDBServer *servobj, const Jstr &filePathName,
    const JagSchemaRecord *record, bool buildInitIndex, abaxint length, bool noMonitor ) : _schemaRecord(record)
{
	int kcrc = 0;
	_insdelcnt = 0;
	_hasCompressThrd = 0;
	_KLEN = record->keyLength;
	_VLEN = record->valueLength;
	_isDestroyed = false;
	_darrlistlen = 0;
	_activepos = 0;
	_servobj = (JagDBServer*)servobj;
	_compthrdhold = _servobj->_sea_records_limit/10; 
	abaxint		_compthrdhold;
	_lock = new JagReadWriteLock();
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
		exit(1);
	}
	fullpath = Jstr( filePathName.c_str(), p-filePathName.c_str() );
	_tablepath = fullpath; 
	_sfilepath = fullpath + "/files";
	objname = Jstr( p+1 ) + ".jdb";
	_objname = objname;
	existFiles = JagFileMgr::getFileFamily( fullpath, objname );
	if ( existFiles.size() < 1 ) {	
		kcrc = _keyChecker->buildInitKeyCheckerFromSigFile();
		JagDiskArrayServer *ldarr = new JagDiskArrayServer( servobj, filePathName, record, buildInitIndex, length, noMonitor );
		_darrlist.append(ldarr);
		_darrlistlen = _darrlist.size();
		_activepos = 0;
		_filecnt = 0;
	} else {
		kcrc = _keyChecker->buildInitKeyCheckerFromSigFile();
		JagStrSplit split( existFiles, '|' );
		// need to rearrange files by order, and remove .jdb
		JagVector<Jstr> paths( split.length() );
		for ( int i = 0; i < split.length(); ++i ) {
			const char *ss = strrchr( split[i].c_str(), '/' );
			if ( !ss ) ss = split[i].c_str();
			JagStrSplit split2( ss, '.' );
			if ( split2.length() < 2 ) {
				printf("s7482 error _pathname=%s, exit\n", split[i].c_str() );
				exit(1);
			}
			int pos = atoi( split2[split2.length()-2].c_str() );
			if ( pos >= split.length() ) {
				printf("s7482 error family has %d files, but %d %s >= max num files\n", split.length(), pos, split[i].c_str() );
				exit(1);
			}
			const char *q = strrchr( split[i].c_str(), '.' );
			paths[pos] = Jstr(split[i].c_str(), q-split[i].c_str());
			paths._elements++;
		}

		abaxint cnt;
		for ( int i = 0; i < paths.size(); ++i ) {
			JagDiskArrayServer *ldarr = 
			    new JagDiskArrayServer( servobj, paths[i], record, buildInitIndex, 
				                        length, noMonitor, i==paths.size()-1 );
			_darrlist.append(ldarr);
			_darrlistlen = _darrlist.size();
			_activepos = _darrlistlen-1;
		}

		bool  readJDB = false;
		if ( 0 == kcrc ) {
			readJDB = true;
			raydebug( stdout, JAG_LOG_LOW, "s6281 skip sig or hdb\n");
		} else {
			abaxint keyCheckerCnt;
			if ( getElements( keyCheckerCnt ) != _keyChecker->size() ) {
				// remove old .hdb file if exists
				raydebug( stdout, JAG_LOG_LOW, "s6381 famelems not equal hdb elements\n");
				Jstr hdbpath = _keyChecker->getPath() + ".hdb";
				if ( JagFileMgr::exist( hdbpath ) ) {
					jagunlink( hdbpath.c_str() );
				}
				readJDB = true;
			}
		}

		// read JDB files and build keychecker
		if ( readJDB ) {
			for ( int i = 0; i < _darrlist.size(); ++i ) {
				cnt = addKeyCheckerFromJDB( _darrlist[i], i );
				raydebug( stdout, JAG_LOG_LOW, "s3736 obj=%s %d/%d readjdb cnt=%l\n", _objname.c_str(), i,  _darrlist.size(), cnt );
			}
		}

		_filecnt = paths.size()-1;

	}

	_needCompressThrd = buildInitIndex || !noMonitor;

}

// reach each jdb file and update keychecker
abaxint JagDiskArrayFamily::addKeyCheckerFromJDB( const JagDiskArrayServer *ldarr, int activepos )
{
	char vbuf[3]; vbuf[2] = '\0';
	int div, rem;

 	char *keyvalbuf = (char*)jagmalloc( ldarr->KEYVALLEN+1 + JAG_KEYCHECKER_VLEN);
	memset( keyvalbuf, 0,  ldarr->KEYVALLEN + 1 + JAG_KEYCHECKER_VLEN );
	abaxint rlimit = getBuffReaderWriterMemorySize( ldarr->_arrlen*ldarr->KEYVALLEN/1024/1024 );
	JagBuffReader nav( ldarr, ldarr->_arrlen, ldarr->KEYLEN, ldarr->VALLEN, 
					   ldarr->_nthserv*ldarr->_arrlen, 0, rlimit );
				
	div = activepos / (JAG_BYTE_MAX+1);
	rem = activepos % (JAG_BYTE_MAX+1);
	vbuf[0] = div; vbuf[1] = rem;
	abaxint cnt = 0;
	while ( nav.getNext( keyvalbuf ) ) { 
		memcpy(keyvalbuf+ldarr->KEYLEN, vbuf, 2 );
		_keyChecker->addKeyValueNoLock( keyvalbuf );
		++cnt;
	}
	free( keyvalbuf );
	return cnt;
}

JagDiskArrayFamily::~JagDiskArrayFamily()
{
	_isDestroyed = true;
	while ( _hasCompressThrd ) {
		jagsleep(1, JAG_SEC);
	}

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

	if ( _lock ) {
		delete _lock;
		_lock = NULL;
	}
}

int JagDiskArrayFamily::insert( JagDBPair &pair, int &insertCode, bool doFirstRedist, bool direct, JagDBPair &retpair, bool noDupPrint )
{
	char buf[pair.key.size() + 2 ];
	char vbuf[3]; vbuf[2] = '\0';
	int pos, rc, div, rem, addkc;

	// find which family file to be inserted
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	if ( _darrlist[_activepos]->_familyInsPairCnt >= _darrlist[_activepos]->_fileSplitCntLimit ) {
		// if current file is full, check previous files to see if any of them is not full
		// if true, use that file; otherwise, use new file
		for ( int i = 0; i < _darrlistlen-1; ++i ) {
			if ( _darrlist[i]->_familyInsPairCnt*100 < _darrlist[i]->_fileSplitCntLimit*JAG_FILE_RAND_EXPAND ) {
				_activepos = i;
				break;
			}
		}
		if ( _darrlistlen-1 == _activepos && _filecnt < JAG_TWOBYTES_MAX ) {
			++_filecnt;
			Jstr nfpath = _pathname + "." + intToStr( _filecnt );
			JagDiskArrayServer *ldarr = new JagDiskArrayServer( _darrlist[_activepos]->_servobj, nfpath, _schemaRecord );
			_darrlist.append(ldarr);
			_darrlistlen = _darrlist.size();
			_activepos = _darrlistlen-1;
		}
	}
	div = _activepos / (JAG_BYTE_MAX+1);
	rem = _activepos % (JAG_BYTE_MAX+1);
	vbuf[0] = div; vbuf[1] = rem;
	memcpy(buf,  pair.key.c_str(), _KLEN );
	memcpy(buf+_KLEN,  vbuf, 2 );
	addkc = _keyChecker->addKeyValue( buf );
	if ( addkc ) ++_darrlist[_activepos]->_familyInsPairCnt;
	pos = _activepos;

	// also, if cnt > sea_record_limit/10, clear the counter;
	// and if no compress thread, create one; otherwise, ignore
	if ( _needCompressThrd ) {
		++_insdelcnt;
		if ( _insdelcnt >= _compthrdhold ) {
			_insdelcnt = 0;
			if ( !_hasCompressThrd ) {
				_hasCompressThrd = 1;
				SepThrdDACompressPass *pass = new SepThrdDACompressPass();
				pass->darrFamily = this;
				pthread_t septhrd;
				// separateDACompressStatic wait until 3AM to do compress
				jagpthread_create( &septhrd, NULL, separateDACompressStatic, (void*)pass );
				pthread_detach( septhrd );
			}
		}
	}
	mutex.writeUnlock();

	// actual insert and check duplicate issue
	if ( !addkc ) {
		if ( pair.upsertFlag ) {
			// prt(("Duplicate key exist [%s]\n", _darrlist[0]->_pdbobj.c_str()));
			// pair.key.dump();
			return set( pair );
		} else {
			return 0;
		}
	} else {
		rc = _darrlist[pos]->insert( pair, insertCode, doFirstRedist, direct, retpair );
		// prt(("s5501 rc=%d\n", rc ));
		if ( rc < 1 ) {
			prt(("s3829 Insert error [%s]\n", _darrlist[0]->_pdbobj.c_str()));
			pair.key.dump();
			rc = 0;
		}
		return rc;
	}
}

// static
// DiskArray Compress
void *JagDiskArrayFamily::separateDACompressStatic( void *ptr )
{
	SepThrdDACompressPass *pass = (SepThrdDACompressPass*)ptr;
	JagDiskArrayFamily *darr = (JagDiskArrayFamily*) pass->darrFamily;

	// sleep until specific time, now at 3AM
	time_t rawtime, rawtime2;
  	struct tm * timeinfo;
  	struct tm  result;
  	time (&rawtime);
  	// timeinfo = localtime (&rawtime);
  	timeinfo = jag_localtime_r (&rawtime, &result );
	timeinfo->tm_sec = 0;
	timeinfo->tm_min = 0;
	timeinfo->tm_hour = 3;
	++timeinfo->tm_mday;

	rawtime2 = mktime( timeinfo );
	double dtime = difftime( rawtime2, rawtime );
	int stime = (int)dtime;
	stime += rand()%1800;
	stime /= 2;
	raydebug( stdout, JAG_LOG_LOW, "s5629 %s wait %d secs then compress\n", darr->_objname.c_str(), stime*2 );
	for ( int i = 0; i < stime; ++i ) {
		jagsleep(2, JAG_SEC);
		if ( darr->_isDestroyed ) {
			raydebug( stdout, JAG_LOG_LOW, "s5630 break waiting compress\n", darr->_objname.c_str(), stime*2 );
			darr->_hasCompressThrd = 0;
			delete pass;
			return NULL;
		}
	}

	// prt(("s2930 darr->_darrlistlen-1=%d\n", darr->_darrlistlen-1 ));
	
	for ( int i = 0; i < darr->_darrlistlen-1; ++i ) {
		darr->_darrlist[i]->_fileCompress = true; // not used now
		/***
		prt(("s2037 darr->_darrlist[i]->_familyInsPairCnt=%d darr->_darrlist[i]->_elements=%d\n",
				(int)darr->_darrlist[i]->_familyInsPairCnt, (int)darr->_darrlist[i]->_elements ));
		***/
		if ( darr->_darrlist[i]->_familyInsPairCnt <= darr->_darrlist[i]->_elements ) {
			// when darr is clean of new insert, compress file
			raydebug( stdout, JAG_LOG_LOW, "s5546 %s begin sep darr comp\n", darr->_darrlist[i]->_dbobj.c_str() );
			if ( darr->_darrlist[i]->reSizeCompress() ) {
				raydebug( stdout, JAG_LOG_LOW, "s5547 %s end sep darr comp\n", darr->_darrlist[i]->_dbobj.c_str() );
			}
			if ( darr->_isDestroyed ) {
				raydebug( stdout, JAG_LOG_LOW, "s5630 break waiting compress\n", darr->_objname.c_str(), stime*2 );
				darr->_hasCompressThrd = 0;
				delete pass;
				return NULL;
			}
		}
	}

	raydebug( stdout, JAG_LOG_LOW, "s5630 finish compress thrd\n", darr->_objname.c_str(), stime*2 );
	darr->_hasCompressThrd = 0;
	delete pass;
	return NULL;
}

/***
void JagDiskArrayFamily::forceSleepTime( int time )
{
	for ( int i = 0; i < _darrlistlen; ++i ) {
		_darrlist[i]->_forceSleepTime = time;
	}
}
***/

abaxint JagDiskArrayFamily::getElements( abaxint &keyCheckerCnt )
{
	abaxint totelem = 0;
	for ( int i = 0; i < _darrlistlen; ++i ) {
		totelem += _darrlist[i]->_elements;
	}
	keyCheckerCnt = _keyChecker->size();
	return totelem;	
}

bool JagDiskArrayFamily::isFlushing()
{
	int totFlushing = 0;
	for ( int i = 0; i < _darrlistlen; ++i ) {
		totFlushing += _darrlist[i]->_isFlushing;	
	}
	return totFlushing;
}

bool JagDiskArrayFamily::hasdarrnew()
{
	for ( int i = 0; i < _darrlistlen; ++i ) {
		if ( _darrlist[i]->_darrnew ) return true;
	}
	return false;
}


bool JagDiskArrayFamily::remove( const JagDBPair &pair )
{
	int rc = 0;
	int pos = 0, div, rem;
	char v[2];
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	// also, if cnt > sea_record_limit/10, clear the counter;
	// and if no compress thread, create one; otherwise, ignore
	if ( _needCompressThrd ) {
		++_insdelcnt;
		if ( _insdelcnt >= _compthrdhold ) {
			_insdelcnt = 0;
			if ( !_hasCompressThrd ) {
				_hasCompressThrd = 1;
				SepThrdDACompressPass *pass = new SepThrdDACompressPass();
				pass->darrFamily = this;
				pthread_t septhrd;
				// separateDACompressStatic wait until 3AM to do compress
				jagpthread_create( &septhrd, NULL, separateDACompressStatic, (void*)pass );
				pthread_detach( septhrd );
			}
		}
	}
	mutex.writeUnlock();

	if ( _keyChecker->getValue( pair.key.c_str(), v ) ) {
		div = (int)v[0];
		rem = (int)v[1];
		if ( div < 0 ) {
			div += JAG_BYTE_MAX+1;
		}
		if ( rem < 0 ) {
			rem += JAG_BYTE_MAX+1;
		}
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->remove( pair );
		if ( rc ) {
			rc = _keyChecker->removeKey( pair.key.c_str() );
			if ( rc ) {
				--_darrlist[pos]->_familyInsPairCnt;
			}
		}
		return rc;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::exist( JagDBPair &pair )
{
	int rc = 0, pos = 0;
	int div, rem;
	abaxint idx;
	char v[2];
	if ( _keyChecker->getValue( pair.key.c_str(), v ) ) {
		div = (int)v[0];
		rem = (int)v[1];
		if ( div < 0 ) {
			div += JAG_BYTE_MAX+1;
		}
		if ( rem < 0 ) {
			rem += JAG_BYTE_MAX+1;
		}
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->exist( pair );
		return 1;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::get( JagDBPair &pair )
{
	int rc = 0, pos = 0;
	int div, rem;
	char v[2];
	if ( _keyChecker->getValue( pair.key.c_str(), v ) ) {
		div = (int)v[0];
		rem = (int)v[1];
		if ( div < 0 ) {
			div += JAG_BYTE_MAX+1;
		}
		if ( rem < 0 ) {
			rem += JAG_BYTE_MAX+1;
		}
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->get( pair );
		// prt(("s0921 pos=%d rc=%d  pair.key=[%s]\n", pos, rc,  pair.key.c_str() ));
		return rc;
	} else {
		// prt(("s8282 JagDiskArrayFamily get pair.key=[%s] return 0\n", pair.key.c_str() ));
		return 0;
	}
}

bool JagDiskArrayFamily::set( JagDBPair &pair )
{
	int rc = 0, pos = 0;
	int div, rem;
	char v[2];
	if ( _keyChecker->getValue( pair.key.c_str(), v ) ) {
		div = (int)v[0];
		rem = (int)v[1];
		if ( div < 0 ) {
			div += JAG_BYTE_MAX+1;
		}
		if ( rem < 0 ) {
			rem += JAG_BYTE_MAX+1;
		}
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->set( pair );
		return rc;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::setWithRange( const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
	ExprElementNode *root, JagParseParam *parseParam, int numKeys, const JagSchemaAttribute *schAttr, 
	abaxint setposlist[], JagDBPair &retpair )
{
	int pos = 0;
	bool rc;
	int div, rem;
	char v[2];
	if ( _keyChecker->getValue( pair.key.c_str(), v ) ) {
		div = (int)v[0];
		rem = (int)v[1];
		if ( div < 0 ) {
			div += JAG_BYTE_MAX+1;
		}
		if ( rem < 0 ) {
			rem += JAG_BYTE_MAX+1;
		}
		pos = div*(JAG_BYTE_MAX+1)+rem;
		rc = _darrlist[pos]->setWithRange( req, pair, buffers, uniqueAndHasValueCol, root, parseParam, 
							numKeys, schAttr, setposlist, retpair );
		// prt(("s2220 rc=%d\n", rc ));
		return rc;
	} else {
		// prt(("s5091 _keyChecker->getValue false key=[%s]\n", pair.key.c_str() ));
		return 0;
	}
}

void JagDiskArrayFamily::drop()
{
	for ( int i = 0; i < _darrlistlen; ++i ) {
		_darrlist[i]->drop();
	}
	_keyChecker->removeAllKey();
}

void JagDiskArrayFamily::flushBlockIndexToDisk()
{
	for ( int i = 0; i < _darrlistlen; ++i ) {
		raydebug( stdout, JAG_LOG_LOW, "    File %d/%d ...\n", i, _darrlistlen );
		_darrlist[i]->flushBlockIndexToDisk();
	}
	
	// also, flush key checker md5 string to disk
	if ( JAG_MEM_HIGH == _servobj->_memoryMode ) {
		raydebug( stdout, JAG_LOG_LOW, "    Mem high ...\n" );
		flushKeyCheckerString();
	}
}

// read bottom level of blockindex and write it to a disk file
void JagDiskArrayFamily::flushKeyCheckerString()
{
	Jstr keyCheckerPath = renameFilePath( _darrlist[0]->_filePath, "sig" );
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
	// char buf[klen+1+1];
	// char *buf = (char*) jagmalloc ( kvlen+1 );
	char buf[2];
	// memset( buf, 0, kvlen+1 );
	buf[0] = '1';
	raysafewrite( fd, buf, 1 ); 

	// begin flush key checker map to disk
	// const AbaxPair<AbaxString, char> *arr = _keyChecker->array();
	// const AbaxPair<AbaxString, JagFixString> *arr = _keyChecker->array();
	const char *arr = _keyChecker->array();
	abaxint len = _keyChecker->arrayLength();
	if ( len >= 1 ) {
		for ( abaxint i = 0; i < len; ++i ) {
			/***
			if ( arr[i].key == AbaxString::NULLVALUE ) continue;
			memcpy( buf, arr[i].key.c_str(), klen );
			memcpy( buf+klen, arr[i].value.c_str(), vlen );
			// buf[klen] = arr[i].value;
			raysafewrite( fd, buf, klen+vlen );
			***/
			if ( arr[i*kvlen] == '\0' ) continue;
			raysafewrite( fd, arr+i*kvlen, kvlen );
		}
	}
	buf[0] = '0';
	raysafepwrite( fd, buf, 1, 0 );
	jagfdatasync( fd );
	jagclose( fd );
}

void JagDiskArrayFamily::copyAndInsertBufferAndClean()
{
	for ( int i = 0; i < _darrlistlen; ++i ) {
		_darrlist[i]->copyAndInsertBufferAndClean();
	}
}


bool JagDiskArrayFamily::checkFileOrder( const JagRequest &req )
{
	int rc = 0;
	for ( int i = 0; i < _darrlistlen; ++i ) {
		rc += _darrlist[i]->checkFileOrder( req );
	}
	return rc;	
}

abaxint JagDiskArrayFamily::orderRepair( const JagRequest &req )
{
	abaxint rc = 0;
	for ( int i = 0; i < _darrlistlen; ++i ) {
		rc += _darrlist[i]->orderRepair( req );
	}
	return rc;
}

void JagDiskArrayFamily::debugJDBFile( int flag, abaxint limit, abaxint hold, abaxint instart, abaxint inend ) 
{ 
	if ( flag == 5 ) {
		for ( int i = 0; i < _darrlist.size(); ++i ) {
			if ( _darrlist[i] ) {
				delete _darrlist[i];
				_darrlist[i] = NULL;
			}
		}
	} else if ( flag == 6 ) {
		if ( _keyChecker ) {
			delete _keyChecker;
			_keyChecker = NULL;
		}
	} else { 
		for ( int i = 0; i < _darrlistlen; ++i ) {
			_darrlist[i]->debugJDBFile( flag, limit, hold, instart, inend ); 
		}
	}
}

// set range family of all jdbs
abaxint JagDiskArrayFamily::setFamilyRead( JagMergeReader *&nts, const char *minbuf, const char *maxbuf ) 
{
	bool startFlag = false;
    abaxint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	abaxint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lminbuf, 0, _darrlist[0]->KEYLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lmaxbuf, 255, _darrlist[0]->KEYLEN);
			lmaxbuf[_darrlist[0]->KEYLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		JagDBPair minpair( JagFixString( minbuf, _darrlist[0]->KEYLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _darrlist[0]->KEYLEN ), value );
		for ( int i = _darrlistlen-1; i >= 0; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			rlimit = index;
			rlimit = rlimit - slimit + 1;
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = 128;
				JAG_BLURT _darrlist[i]->_memLock->readLock( -1 ); JAG_OVER;
				maxPossibleElem += _darrlist[i]->getRegionElements( slimit, rlimit/JAG_BLOCK_SIZE );
				_darrlist[i]->_memLock->readUnlock( -1 );
				fRange.append( tempRange );
			}
		}
	} else {
		for ( int i = _darrlistlen-1; i >= 0; --i ) {
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
	
	if ( fRange.size() > 0 ) {
		nts = new JagMergeReader( fRange, fRange.size(), _darrlist[0]->KEYLEN, _darrlist[0]->VALLEN );
	} else {
		nts = NULL;
	}

	if ( lminbuf ) {
		free( lminbuf );
		minbuf = NULL;
	}
	if ( lmaxbuf ) {
		free( lmaxbuf );
		maxbuf = NULL;
	}
	return maxPossibleElem;
}

// set range family of range jdbs
abaxint JagDiskArrayFamily::setFamilyReadPartial( JagMergeReader *&nts, const char *minbuf, const char *maxbuf, 
	abaxint spos, abaxint epos, abaxint mmax ) 
{
	if ( spos < 0 ) spos = 0;
	if ( epos >= _darrlistlen ) epos = _darrlistlen-1;
	if ( epos < 0 ) epos = 0;
	if ( spos > epos ) spos = epos;
	bool startFlag = false;
    abaxint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	abaxint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lminbuf, 0, _darrlist[0]->KEYLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lmaxbuf, 255, _darrlist[0]->KEYLEN);
			lmaxbuf[_darrlist[0]->KEYLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		JagDBPair minpair( JagFixString( minbuf, _darrlist[0]->KEYLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _darrlist[0]->KEYLEN ), value );
		for ( int i = epos; i >= spos; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			rlimit = index;
			rlimit = rlimit - slimit + 1;
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = mmax;
				JAG_BLURT _darrlist[i]->_memLock->readLock( -1 ); JAG_OVER;
				maxPossibleElem += _darrlist[i]->getRegionElements( slimit, rlimit/JAG_BLOCK_SIZE );
				_darrlist[i]->_memLock->readUnlock( -1 );
				fRange.append( tempRange );
			}
		}
	} else {
		for ( int i = epos; i >= spos; --i ) {
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
	
	if ( fRange.size() > 0 ) {
		nts = new JagMergeReader( fRange, fRange.size(), _darrlist[0]->KEYLEN, _darrlist[0]->VALLEN );
	} else {
		nts = NULL;
	}

	if ( lminbuf ) {
		free( lminbuf );
		minbuf = NULL;
	}
	if ( lmaxbuf ) {
		free( lmaxbuf );
		maxbuf = NULL;
	}
	return maxPossibleElem;
}
abaxint JagDiskArrayFamily::setFamilyReadBackPartial( JagMergeBackReader *&nts, const char *minbuf, const char *maxbuf, 
	abaxint spos, abaxint epos, abaxint mmax ) 
{
	if ( spos < 0 ) spos = 0;
	if ( epos >= _darrlistlen ) epos = _darrlistlen-1;
	if ( epos < 0 ) epos = 0;
	if ( spos > epos ) spos = epos;
	bool startFlag = false;
    abaxint index = 0, rc, slimit, rlimit;
    JagDBPair retpair;
	JagFixString value;
	char *lminbuf = NULL;
	char *lmaxbuf = NULL;
	abaxint maxPossibleElem = 0;
	JagVector<OnefileRange> fRange(8);
	OnefileRange tempRange;

	if ( !minbuf || !maxbuf ) {
		if ( maxbuf ) {
			lminbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lminbuf, 0, _darrlist[0]->KEYLEN+1);
			minbuf = lminbuf;
		} else if ( minbuf ) {
			lmaxbuf = (char*)jagmalloc(_darrlist[0]->KEYLEN+1);
			memset(lmaxbuf, 255, _darrlist[0]->KEYLEN);
			lmaxbuf[_darrlist[0]->KEYLEN] = '\0';
			maxbuf = lmaxbuf;
		}
	}
			
	if ( minbuf || maxbuf ) {
		JagDBPair minpair( JagFixString( minbuf, _darrlist[0]->KEYLEN ), value );
		JagDBPair maxpair( JagFixString( maxbuf, _darrlist[0]->KEYLEN ), value );
		for ( int i = epos; i >= spos; --i ) {
			rc = _darrlist[i]->exist( minpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			slimit = index;
			if ( slimit < 0 ) slimit = 0;
			rc = _darrlist[i]->exist( maxpair, &index, retpair );
			if ( startFlag && !rc ) ++index;
			rlimit = index;
			rlimit = rlimit - slimit + 1;
			if ( rlimit > 0 ) {
				tempRange.darr = _darrlist[i];
				tempRange.startpos = slimit+rlimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = mmax;
				JAG_BLURT _darrlist[i]->_memLock->readLock( -1 ); JAG_OVER;
				maxPossibleElem += _darrlist[i]->getRegionElements( slimit, rlimit/JAG_BLOCK_SIZE );
				_darrlist[i]->_memLock->readUnlock( -1 );
				fRange.append( tempRange );
			}
		}
	} else {
		for ( int i = epos; i >= spos; --i ) {
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
	
	if ( fRange.size() > 0 ) {
		nts = new JagMergeBackReader( fRange, fRange.size(), _darrlist[0]->KEYLEN, _darrlist[0]->VALLEN );
	} else {
		nts = NULL;
	}

	if ( lminbuf ) {
		free( lminbuf );
		minbuf = NULL;
	}
	if ( lmaxbuf ) {
		free( lmaxbuf );
		maxbuf = NULL;
	}
	return maxPossibleElem;
}
