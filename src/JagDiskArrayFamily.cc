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
	int kcrc = 0;
	_insdelcnt = 0;
	_KLEN = record->keyLength;
	_VLEN = record->valueLength;
	_KVLEN = _KLEN + _VLEN;
	_servobj = (JagDBServer*)servobj;
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
	_tablepath = fullpath; 
	_sfilepath = fullpath + "/files";
	JagStrSplit sp(fullpath, '/');
	int splen = sp.length();
	_taboridxname = sp[splen-1];
	_dbname = sp[splen-2];
	objname = _taboridxname + ".jdb";
	_objname = objname;
	existFiles = JagFileMgr::getFileFamily( fullpath, objname );
	if ( existFiles.size() > 0 ) {	
		kcrc = _keyChecker->buildInitKeyCheckerFromSigFile();

		JagStrSplit split( existFiles, '|' );
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
			paths._elements++;
		}

		jagint cnt;
		for ( int i = 0; i < paths.size(); ++i ) {
			JagDiskArrayServer *darr = new JagDiskArrayServer( servobj, this, i, paths[i], record, length, buildInitIndex );
			_darrlist.append(darr);
		}

		bool  readJDB = false;
		if ( 0 == kcrc ) {
			readJDB = true;
			raydebug( stdout, JAG_LOG_LOW, "s6281 skip sig or hdb\n");
		} else {
		}

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

jagint JagDiskArrayFamily::addKeyCheckerFromJDB( JagDiskArrayServer *ldarr, int activepos )
{
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

	return cnt;
}

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
		memset( kvbuf, 0,  _KLEN + 1 + JAG_KEYCHECKER_VLEN );
		memcpy( kvbuf, iter->first.c_str(), iter->first.size() );
		memcpy(kvbuf+_KLEN, vbuf, JAG_KEYCHECKER_VLEN );

		if ( _keyChecker->addKeyValueNoLock( kvbuf ) ) {
			++cntDone;
		}
		++ iter;
	}
	free( kvbuf );
	return cntDone;
}

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

    if ( _insertBufferMap ) { 
		delete _insertBufferMap; 
		_insertBufferMap=NULL; 
	}
}


jagint JagDiskArrayFamily::processFlushInsertBuffer() 
{
	jagint cnt = 0; 
	bool doneFlush = false;

	if ( _darrlist.size() < 1 ) {
		doneFlush = false;  // new darr will be added, see below
	} else {
		JagVector<JagMergeSeg> vec;
		int mtype;
		int which = findMinCostFile( vec, _doForceFlush, mtype ); 
		if ( which < 0 ) {
			raydebug(stdout, JAG_LOG_LOW, "s2647 min-cost file not found, _darrlist.size=%d\n", _darrlist.size() );
		} else {
			if (  mtype == JAG_MEET_TIME ) {  
				cnt = _darrlist[which]->mergeBufferToFile( _insertBufferMap, vec );	
				doneFlush = true;
				addKeyCheckerFromInsertBuffer( which );
			} else {
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

int JagDiskArrayFamily::findMinCostFile( JagVector<JagMergeSeg> &vec, bool forceFlush, int &mtype )
{
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
		}
	}

	if ( forceFlush ) {
		mtype = JAG_MEET_TIME;
		return minDarr;
	}

	if ( minTime <= allowedFlushTimeLimit ) {
		mtype = JAG_MEET_TIME;
		return minDarr;
	} else {
		mtype = 0;
		return -1;
	}
}

int JagDiskArrayFamily::insert( const JagDBPair &pair, bool doFirstRedist, 
							    JagDBPair &retpair, bool noDupPrint )
{
	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	if ( _keyChecker && _keyChecker->exist( kbuf ) ) {
		return 0;
	}

    int rc;
	if ( _insertBufferMap->exist( pair ) ) {
		prt(("s2038271 _pathname=[%s]\n", _pathname.s() ));
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

bool JagDiskArrayFamily::exist( JagDBPair &pair )
{
	if ( _insertBufferMap && _insertBufferMap->get( pair ) ) {
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	char v[2];

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

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
	if ( _insertBufferMap && _insertBufferMap->get( pair ) ) {
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	char v[2];

	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );

	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		rc = _darrlist[pos]->get( pair );

		return rc;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::set( const JagDBPair &pair )
{
	if ( _insertBufferMap && _insertBufferMap->set( pair ) ) {
		return true;
	}

	int rc = 0, pos = 0;
	int div, rem;
	char v[2];
	char kbuf[_KLEN+1];
	memset( kbuf, 0, _KLEN+1);
	memcpy( kbuf, pair.key.c_str(), _KLEN );
	if ( _keyChecker->getValue( kbuf, v ) ) {
		div = (jagbyte)v[0];
		rem = (jagbyte)v[1];
		pos = div*(JAG_BYTE_MAX+1)+rem;

		rc = _darrlist[pos]->set( pair );

		return rc;
	} else {
		return 0;
	}
}

bool JagDiskArrayFamily::setWithRange( const JagRequest &req, JagDBPair &pair, const char *buffers[], bool uniqueAndHasValueCol, 
										ExprElementNode *root, const JagParseParam *pParam, int numKeys, const JagSchemaAttribute *schAttr, 
										jagint setposlist[], JagDBPair &retpair )
{
	bool rc = get(pair); 
	if ( ! rc ) {
		return false;
	}

    rc = JagDiskArrayBase::checkSetPairCondition( _servobj, req, pair, (char**)buffers, uniqueAndHasValueCol, root, pParam,
                                				   numKeys, schAttr, _KLEN, _VLEN, setposlist, retpair );
    if ( ! rc ) return false;

	if ( _insertBufferMap && _insertBufferMap->set( retpair ) ) {
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

		rc = _darrlist[pos]->set (retpair ); 

		return rc;
	} else {
		return 0;
	}
}


void JagDiskArrayFamily::drop()
{
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
	
	if ( JAG_MEM_HIGH == _servobj->_memoryMode ) {
		raydebug( stdout, JAG_LOG_LOW, "    Flush Memory ...\n" );
		flushKeyCheckerString();
	}
}

void JagDiskArrayFamily::flushKeyCheckerString()
{
	if( _darrlist.size() < 1 ) {
		return;
	}

	JagStrSplit sp(  _darrlist[0]->_filePath, '.' );
	Jstr keyCheckerPath = sp[0] + ".sig";

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

jagint JagDiskArrayFamily::setFamilyRead( JagMergeReader *&nts, const char *minbuf, const char *maxbuf ) 
{
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

jagint JagDiskArrayFamily::setFamilyReadPartial( JagMergeReader *&nts, const char *minbuf, const char *maxbuf, 
												  jagint spos, jagint epos, jagint mmax ) 
{
	if ( spos < 0 ) spos = 0;
	if ( epos >= _darrlist.size() ) epos = _darrlist.size()-1;
	if ( epos < 0 ) epos = 0;
	if ( spos > epos ) spos = epos;
	int darrsize = _darrlist.size();

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
				tempRange.startpos = slimit;
				tempRange.readlen = rlimit;
				tempRange.memmax = mmax;
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
	
	if ( fRange.size() >= 0 ) {
		nts = new JagMergeReader( this->_insertBufferMap, fRange, fRange.size(), 
								  _KLEN, _VLEN, minbuf, maxbuf );
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

JagDiskArrayServer* JagDiskArrayFamily::flushBufferToNewFile( )
{
	if (  _insertBufferMap->elements() < 1 ) { 
		prt(("s183330 flushBufferToNewFile no data in mem, return nullptr\n"));
		return nullptr; 
	}
	_isFlushing = 1;
	int darrlistlen = _darrlist.size();
	prt(("s40726 JagDiskArrayFamily::flushBufferToNewFile darrlistlen=%d\n", darrlistlen ));
	jagint len = _insertBufferMap->size();
	JagStrSplit sp(_pathname, '/');
	int slen = sp.length();
	Jstr fname= sp[slen-1];
	Jstr filePathName;
	if ( ! strchr(fname.c_str(), '.') ) {
		filePathName = _pathname + "." + intToStr( darrlistlen );
	} else {
		const char *p = strrchr( _pathname.c_str(), '.' );
		Jstr newpathname = Jstr( _pathname.c_str(),  p-_pathname.c_str() );
		filePathName = newpathname + "." + intToStr( darrlistlen );
		prt(("s220184 filePathName=[%s]\n", filePathName.s() ));
	}

	JagDiskArrayServer *darr = new JagDiskArrayServer( _servobj, this, darrlistlen, filePathName, _schemaRecord, len, false );
	darr->flushBufferToNewFile( _insertBufferMap );

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
	if ( NULL == _insertBufferMap ) {
		return 0;
	}
	jagint cnt = _insertBufferMap->size();
	return cnt;
}

