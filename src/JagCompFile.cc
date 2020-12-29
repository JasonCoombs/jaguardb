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
#include <dirent.h>
#include <sys/sendfile.h>
#include <JagSimpFile.h>
#include <JagCompFile.h>
#include <JagFileMgr.h>
#include <JagFileName.h>
#include <JagSortLinePoints.h>
//#include <JagMutex.h>
#include <JagDBMap.h>
#include <JagDiskArrayFamily.h>

// pathDir: "/a/b/c/d/fileName"
// pathDir: "/a/b/c/d/fileName/part1.....partN"
JagCompFile::JagCompFile( JagDiskArrayFamily *fam, const Jstr &pathDir, jagint KLEN, jagint VLEN )
{
	_KLEN = KLEN;
	_VLEN = VLEN;
	_KVLEN = KLEN + VLEN;
	_pathDir = pathDir;
	_length = 0;
	_family = fam;

	prt(("s02839 JagCompFile ctor _pathDir=[%s] KLEN=%d VLEN=%d\n", _pathDir.c_str(), KLEN, VLEN ));

	_offsetMap = new JagArray< JagOffsetSimpfPair >();
	_offsetMap->useHash( true );

	_keyMap = new JagArray< JagKeyOffsetPair >();

	_open();
}

// open the dir and read any simple files
// if dir does not exist, create it
void  JagCompFile::_open()
{
	JagFileMgr::makedirPath( _pathDir, 0700 );
	prt(("s108373 JagCompFile::_open() _pathDir=[%s]\n", _pathDir.c_str() ));
	// avoid files dir

	DIR             *dp;
	struct dirent   *dirp;
	if ( NULL == (dp=opendir( _pathDir.c_str())) ) {
		prt(("s92110 error opendir [%s]\n", _pathDir.c_str() ));
		return;
	}

	JagVector<JagFileName> vec;
	while( NULL != (dirp=readdir(dp)) ) {
		if ( 0==strcmp(dirp->d_name, ".") || 0==strcmp(dirp->d_name, "..") ) {
			continue;
		}

		if ( 0==strcmp(dirp->d_name, "files" ) ) { continue; }
		if ( strstr(dirp->d_name, ".bid" ) ) { continue; }
		prt(("s30247 simpfile vec.append(%s)\n", dirp->d_name ));
		vec.push_back( JagFileName(dirp->d_name) );
	}

	const JagFileName *arr = vec.array();
	jagint len = vec.size();
	if ( len > 0 ) {
		inlineQuickSort<JagFileName>( (JagFileName*)arr, len );
	}
	prt(("s12228 simpf vec.len=%d\n", len ));

	Jstr fpath;
	jagint offset = 0;
	JagKeyOffsetPair keyOffsetPair;
	char minkbuf[_KLEN+1];
	int rc;
	for ( int i=0; i < len; ++i ) {
		fpath = _pathDir + "/" + vec[i].name;
		prt(("s93110 fpath=[%s] _offset=%ld\n", fpath.c_str(), offset ));

		JagSimpFile *simpf = new JagSimpFile( this, fpath, _KLEN, _VLEN );
		JagOffsetSimpfPair pair( offset, AbaxBuffer(simpf) );
		prt(("s33039 insert JagOffsetSimpfPair offset=%ld simpf=%s\n", offset, fpath.c_str() ));
		_offsetMap->insert( pair );

		memset( minkbuf, 0, _KLEN+1 );
		rc = simpf->getMinKeyBuf( minkbuf );
		if ( 1 || rc ) {
			makeKOPair( minkbuf, offset, keyOffsetPair );
			_keyMap->insert( keyOffsetPair );
			prt(("s22373 _keyMap insert minkbuf=[%s] offset=%ld\n", minkbuf, offset ));

			offset += simpf->_length; 
			_length += simpf->_length; 
		}
	}

}

void JagCompFile::getMinKOPair( const JagSimpFile *simpf, jagint offset, JagKeyOffsetPair & kopair )
{
	char minkbuf[_KLEN+1];
	memset( minkbuf, 0, _KLEN+1 );
	simpf->getMinKeyBuf( minkbuf );
	makeKOPair( minkbuf, offset, kopair );
}

void JagCompFile::makeKOPair( const char *buf, jagint offset, JagKeyOffsetPair & kopair )
{
	kopair = AbaxPair<JagFixString, AbaxLong>( JagFixString(buf, _KLEN, _KLEN ), offset );
}

JagCompFile::~JagCompFile()
{
	// iterate _darrMap and delete simpf in them
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) delete simpf;
	}

	delete _offsetMap;
	delete _keyMap;
}

// read len bytes  starting from global offset
jagint JagCompFile::pread(char *buf, jagint offset, jagint len ) const
{
	//prt(("s228312 JagCompFile::pread offset=%d len=%d\n", offset, len ));

	jagint partOffset;
	jagint offsetIdx;
	int rc1 = _getOffSet( offset, partOffset, offsetIdx );
	if ( rc1 < 0 ) {
		//prt(("s110293 rc1=%d < 0 return rc1\n", rc1 ));
		return rc1;
	}

    // read from SimpFile of partName (offset may be in middle of this file)	
	jagint n;
	JagSimpFile *simpf;
	jagint  remaining = len;
	jagint  totalRead =0;
	jagint arrlen = _offsetMap->size();
	//prt(("a22312 offsetMap->size=%d partOffset=%d offsetIdx=%d\n", _offsetMap->size(), partOffset, offsetIdx ));
	jagint localOffset = offset - partOffset;
	for ( int i = offsetIdx; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) continue;
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		n = simpf->pread(buf, localOffset, remaining );
		//prt(("s202912 i=%d simpf=%0x pread n=%d buf=[%s]\n", i, simpf, n, buf ));
		totalRead += n;
		if ( totalRead == len ) { 
			//prt(("s111022 totalRead == len ==%d break\n", len ));
			break; 
		}

		remaining -= n;
		localOffset = 0;
		buf += n;
	}

	return totalRead;
}

// write len bytes  starting from global offset
jagint JagCompFile::pwrite(const char *buf, jagint offset, jagint len )
{
	prt(("s4029 JagCompFile::pwrite buf=[%s] offset=%d len=%d\n", buf, offset, len ));
	jagint partOffset;
	jagint offsetIdx;
	int rc1 = _getOffSet( offset, partOffset, offsetIdx );
	if ( rc1 < 0 ) {
		prt(("s30298 _getOffSet rc1=%d < 0\n", rc1 ));
		return rc1;
	}
	prt(("s71287 _getOffSet rc1=%d partOffset=%d offsetIdx=%d\n", rc1, partOffset, offsetIdx ));

    // read from SimpFile of partName (offset may be in middle of this file)	
	jagint n;
	JagSimpFile *simpf;
	jagint  remaining = len;
	jagint  totalWrite =0;
	jagint arrlen = _offsetMap->size();
	jagint localOffset = offset - partOffset;
	prt(("s039134 arrlen=%d localOffset=%d\n", arrlen, localOffset ));

	for ( int i = offsetIdx; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) continue;
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		n = simpf->pwrite(buf, localOffset, remaining );
		totalWrite += n;
		if ( totalWrite == len ) { break; }
		if ( n < remaining ) { break; }

		remaining -= n;
		localOffset = 0;
		buf += n;
	}

	prt(("s3399 totalWrite=%d bytes\n", totalWrite ));
	return totalWrite;
}

// insert len bytes  starting from global offset
jagint JagCompFile::insert(const char *buf, jagint position, jagint len )
{
	jagint partOffset;
	jagint offsetIdx;
	int rc1 = _getOffSet( position, partOffset, offsetIdx );
	if ( rc1 < 0 ) return rc1;

    // read from SimpFile of partName (offset may be in middle of this file)	
	JagSimpFile *simpf;
	jagint localOffset = position - partOffset;
	simpf = (JagSimpFile*) (*_offsetMap)[offsetIdx].value.value();

	JagKeyOffsetPair oldpair;
	getMinKOPair( simpf, 0, oldpair );

	// split from localOffset in simpf
	jagint splits = ( simpf->_length + len ) / JAG_SIMPFILE_LIMIT_BYTES;  // e.g.: 3
	jagint remainder = ( simpf->_length + len ) % JAG_SIMPFILE_LIMIT_BYTES;  // bytes
	// combine simpf and buf into 3 new simpfiles
	// name is partName.1 partName.2 partName.3.  partName.3 may have more data

	// first part
	Jstr fpath, pName;
	pName = longToStr( offsetIdx );
	fpath = _pathDir + "/" + pName;
	// write first part in simpf to new files
	JagSimpFile *newfile1 = new JagSimpFile( this, fpath, _KLEN, _VLEN );
	::sendfile( newfile1->_fd, simpf->_fd, NULL, localOffset );
	jagint globalOffset = partOffset;
	(*_offsetMap)[offsetIdx] = JagOffsetSimpfPair(globalOffset, AbaxBuffer(newfile1) );  // replace


	jagint n;
	if ( len <= JAG_SIMPFILE_LIMIT_BYTES - localOffset ) {
		n = len; // len is not large enough to fill a block(fileLimit)
	} else {
		n = JAG_SIMPFILE_LIMIT_BYTES - localOffset;
	}
	jagint bufpos = 0;
	newfile1->pwrite( buf+bufpos, localOffset, n );
	bufpos += n;

	JagKeyOffsetPair newpair;
	getMinKOPair( newfile1, globalOffset, newpair );
	_keyMap->remove( oldpair );
	_keyMap->insert( newpair );

	// write buff to new files
	jagint bytesToWrite = JAG_SIMPFILE_LIMIT_BYTES - localOffset;
	jagint startpos = 0;
	JagSimpFile *newf;
	JagKeyOffsetPair kopair;
	for ( int i=1; i < splits-1; ++i ) {
		globalOffset += JAG_SIMPFILE_LIMIT_BYTES;
		pName = longToStr( globalOffset );
		fpath = _pathDir + "/" + pName;
		newf = new JagSimpFile( this, fpath, _KLEN, _VLEN );
		newf->pwrite( buf+bufpos, startpos, bytesToWrite );
		bufpos += bytesToWrite;
		startpos += bytesToWrite;
		bytesToWrite = JAG_SIMPFILE_LIMIT_BYTES;
		_offsetMap->insert( JagOffsetSimpfPair( globalOffset, AbaxBuffer(newf) ) );

		getMinKOPair( newf, globalOffset, kopair );
		_keyMap->insert( kopair );

		prt(("s102930 _offsetMap insert globalOffset=%ld JagSimpFile fpath=%s\n", globalOffset, fpath.c_str() ));
		prt(("s11129 _keyMap insert kopair.key=%s globalOffset=%ld\n", kopair.key.c_str(), globalOffset ));

	}

	// remaining part in buf
	if ( remainder > 0 ) {
		globalOffset += JAG_SIMPFILE_LIMIT_BYTES;
		pName = longToStr( globalOffset );
		fpath = _pathDir + "/" + pName;
		newf = new JagSimpFile( this, fpath, _KLEN, _VLEN );
		newf->pwrite( buf+bufpos, startpos, remainder );
		bufpos += remainder;
		startpos += remainder;
		_offsetMap->insert( JagOffsetSimpfPair( globalOffset, AbaxBuffer(newf) ) );

		getMinKOPair( newf, globalOffset, kopair );
		_keyMap->insert( kopair );

		prt(("s202930 _offsetMap insert globalOffset=%ld JagSimpFile fpath=%s\n", globalOffset, fpath.c_str() ));
		prt(("s21129 _keyMap insert kopair.key=%s globalOffset=%ld\n", kopair.key.c_str(), globalOffset ));

	}

	// copy remaining part of simpf tp newf(last file)
	::sendfile( newf->_fd, simpf->_fd, NULL, simpf->_length - localOffset );
	delete simpf;

	// update offsets for files following the new-inserted files +len
	// _offsetMap updated, _darrMap needs not update

	jagint startIdx;
	JagOffsetSimpfPair startPair(globalOffset );
	_offsetMap->get( startPair, startIdx );
	JagVector<JagOffsetSimpfPair> vec;
	for ( int i = startIdx+1; i < _offsetMap->size(); ++i ) {
		if ( _offsetMap->isNull(i) ) continue;
		vec.push_back( (*_offsetMap)[i] );
	}

	jagint offset;
	for ( int i=0; i < vec.size(); ++i ) {
		offset =  vec[i].key.value();
		simpf = (JagSimpFile*)vec[i].value.value();
		JagOffsetSimpfPair oldp( offset );
		JagOffsetSimpfPair p( offset + len, vec[i].value );
		_offsetMap->remove( oldp );
		_offsetMap->insert( p );

		getMinKOPair( simpf, offset+len, kopair );
		_keyMap->set( kopair );

		prt(("s302930 _offsetMap remove insert offset=%ld\n",  offset + len ));
		prt(("s31129 _keyMap set kopair.key=%s offset=%ld\n", kopair.key.c_str(),  offset+len ));
	}

	return splits+1;
}

// remove len bytes  starting from global offset
jagint JagCompFile::remove( jagint position, jagint len )
{
	jagint partOffset;
	jagint offsetIdx;
	int rc1 = _getOffSet( position, partOffset, offsetIdx );
	if ( rc1 < 0 ) return rc1;

    // read from SimpFile of partName (offset may be in middle of this file)	
	JagSimpFile *simpf;
	simpf = (JagSimpFile*) (*_offsetMap)[offsetIdx].value.value();

	jagint endPosition = position + len;
	jagint endPartOffset;
	jagint endOffsetIdx;
	rc1 = _getOffSet( endPosition, endPartOffset, endOffsetIdx );
	if ( rc1 < 0 ) return rc1;

	// offsetIdx+1, ... endOffsetIdx-1 should be deleted
	JagSimpFile *sf;
	JagVector<JagOffsetSimpfPair> vec;
	for ( int i = offsetIdx+1; i <= endOffsetIdx-1; ++i ) {
		if ( _offsetMap->isNull(i) ) continue;
		vec.push_back( (*_offsetMap)[i] );
	}

	JagKeyOffsetPair kopair;
	for ( int i=0; i < vec.size(); ++i ) {
		sf = (JagSimpFile*) vec[i].value.value();
		getMinKOPair( sf, 0, kopair );
		sf->removeFile();
		delete sf;
		_offsetMap->remove( vec[i] );
		_keyMap->remove( kopair );
	}

	// merge endFile 
	jagint endLocalOffset = endPosition - endPartOffset;
	sf = (JagSimpFile*) (*_offsetMap)[endOffsetIdx].value.value();
	getMinKOPair( sf, 0, kopair );
	sf->seekTo( endLocalOffset );
	::sendfile( simpf->_fd, sf->_fd, NULL, sf->_length - endLocalOffset );
	sf->removeFile();
	delete sf;
	_offsetMap->remove( (*_offsetMap)[endOffsetIdx] );
	_keyMap->remove( kopair );

	// update offsets after endOffsetIdx
	vec.clear();
	for ( int i = endOffsetIdx+1; i < _offsetMap->size(); ++i ) {
		if ( _offsetMap->isNull(i) ) continue;
		vec.push_back( (*_offsetMap)[i] );
	}

	jagint offset;
	for ( int i=0; i < vec.size(); ++i ) {
		sf = (JagSimpFile*) vec[i].value.value();
		offset = vec[i].key.value();
		JagOffsetSimpfPair oldp( offset );
		JagOffsetSimpfPair p( offset - len, vec[i].value );
		_offsetMap->remove( oldp );
		_offsetMap->insert( p );

		getMinKOPair( sf, offset-len, kopair );
		_keyMap->set( kopair );

		prt(("s111288 _offsetMap->remove/insert offset=%d\n",  offset - len ));
		prt(("s111288 _keyMap- set kopair=%s offset=%ld\n", kopair.key.c_str(),  offset-len ));
	}

	return 0;
}

int JagCompFile::_getOffSet( jagint anyPosition, jagint &partOffset, jagint &offsetIdx )  const
{
	JagOffsetSimpfPair pair( anyPosition);
	prt(("s55621  _offsetMap->get()\n" ));
	if ( _offsetMap->get( pair, offsetIdx ) ) {
		partOffset = anyPosition;
	} else {
		prt(("s51346 _offsetMap->getPred() ...\n"));
		const JagOffsetSimpfPair *predPair = _offsetMap->getPred( pair, offsetIdx );
		prt(("s51347 _offsetMap->getPred() done ...\n"));
		if ( NULL == predPair ) {
			prt(("s10214 _offsetMap->getPred(%ld) NULL\n", pair.key.value() ));
			return -1;
		} else {
			partOffset = predPair->key.value();
		}
	}

	return 0;

}

int JagCompFile::removeFile()
{
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		simpf->removeFile();
		if ( simpf) delete simpf;
	}
}

// return < 0 for error; >=0 for cost
float JagCompFile::computeMergeCost( const JagDBMap *pairmap, jagint seqReadSpeed, jagint seqWriteSpeed, JagVector<JagMergeSeg> &vec )
{
	if ( pairmap->size() < 1 ) return -1;

	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	char kbuf[_KLEN + 1];
	int rc;
	JagFixMapIterator leftIter = pairmap->_map->begin();
	JagFixMapIterator rightIter;
	size_t numBufferKeys;
	jagint  fsz;
	JagMergeSeg seg;
	float rd, wr, tsec;
	tsec = 0.0;

	//prt(("s100343 JagCompFile::computeMergeCost arrlen=%d\n", arrlen ));

	jagint lastSimpfPos = -1;
	for ( jagint i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		//prt(("s3023 i=%d is non-null simpf\n", i )); 
		lastSimpfPos = i;

		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		// min/max key of simpf
		memset( kbuf, 0, _KLEN + 1 );
		rc = simpf->getMaxKeyBuf( kbuf );
		if ( rc < 0 ) {
			//prt(("s111282 cannot get max value of simpf\n"));
			continue;  // cannot get max value
		} 
		//prt(("s2202837 simpfpos=%d MaxKeyBuf=[%s]\n", i, kbuf ));

		JagDBPair maxPair(kbuf, _KLEN );
		rightIter = pairmap->getPredOrEqual( maxPair );
		if ( rightIter == pairmap->_map->end() ) {
			// has no predecesor, maxpair in this simpf is less than the min key in pairmap
			//prt(("s23383 has no predecesor, maxpair in this simpf is less than the min key in pairmap\n"));
			continue;
		}

		// this simpf has no relevant key range in pairmap
		if ( rightIter->first < leftIter->first ) {
			//prt(("s22373 this simpf has no relevant key range in pairmap\n"));
			continue;
		}

		// look at pairmap[ leftIter, rightIter ] and simpf->_length
		numBufferKeys = std::distance( leftIter, rightIter ) + 1; // # of records
		fsz = simpf->size(); // bytes
		//prt(("s123373 i=%d numBufferKeys=%d fsz=%d\n", i, numBufferKeys, fsz ));

		// read: fsz bytes; write: numBufferKeys*_KVLEN + fsz
		rd = (float)fsz/(float)ONE_MEGA_BYTES;
		wr = (float)(numBufferKeys*_KVLEN + fsz )/(float)ONE_MEGA_BYTES;
		tsec += rd/(float)seqWriteSpeed + wr/(float)seqWriteSpeed;

		seg.leftIter = leftIter;
		seg.rightIter = rightIter;
		seg.simpfPos = i;  // remember this position
		vec.push_back( seg );
		//prt(("s34003 simpfPos=%d left=[%s] right=[%s]\n", seg.simpfPos, leftIter->first.c_str(), rightIter->first.c_str() ));


		// move leftIter to right, find
		++rightIter;
		if ( rightIter == pairmap->_map->end() ) {
			//prt(("s491338 rightIter got to end, break\n" ));
			break;
		}

		// check next region
		leftIter = rightIter;
	}

	// after all simpf are inspected
	if ( leftIter !=  pairmap->_map->end() ) {
		// if there are remaining pirs in pairmap, put them into the last simpf
		int veclen = vec.size();
		if ( veclen > 0 ) {
			//prt(("s233083 there are remaining pirs in pairmap\n"));
			seg.leftIter = leftIter;

			rightIter = pairmap->_map->end();
			-- rightIter;
			seg.rightIter = rightIter;

			seg.simpfPos = lastSimpfPos;
			vec.push_back( seg );

			numBufferKeys = std::distance( leftIter, rightIter ) + 1; // # of records
			wr = (float)(numBufferKeys*_KVLEN )/(float)ONE_MEGA_BYTES;
			tsec += wr/(float)seqWriteSpeed;
			//prt(("s1225442 vec.entry left=[%s] right=[%s] simpfpos=%d\n", leftIter->first.c_str(), rightIter->first.c_str(), lastSimpfPos ));
		}
	}

	return tsec;
}

// returns # of bytes
jagint JagCompFile::mergeBufferToFile( const JagDBMap *pairmap, const JagVector<JagMergeSeg> &vec )
{
	int simpfPos;
	JagSimpFile *simpf;
	jagint cnt = 0;
	jagint bytes = 0;

	for ( int i=0; i < vec.size(); ++i ) {
		simpfPos = vec[i].simpfPos;  // position of simpf
		simpf = (JagSimpFile*)(*_offsetMap)[simpfPos].value.value();
		bytes = simpf->mergeSegment( vec[i] );
		cnt += bytes;
	}

	if ( 0 == vec.size() ) {
		cnt += flushBufferToNewSimpFile( pairmap )*_KVLEN;
		//prt(("s30885 flushBufferToNewFile cnt=%d\n", cnt ));
	}

	// update all offsets of all simpfiles 
	refreshAllSimpfileOffsets();

	// prt(("s28735 compfile after merge:\n"  ));
	// this->print();

	return cnt;
}

void JagCompFile::refreshAllSimpfileOffsets()
{
	JagVector<JagOffsetSimpfPair> vec;

	jagint offset = 0;
	jagint arrlen = _offsetMap->size();
	JagSimpFile *simpf;
	_length = 0;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) {
			// delete this simf, insert it again with new offset
			JagOffsetSimpfPair pair(offset, AbaxBuffer(simpf));
			vec.push_back(pair);
			offset += simpf->_length; 
			_length += simpf->_length; 
		}
	}

	delete _offsetMap;
	delete _keyMap;
	//prt(("s111128 deleted old _offsetMap _keyMap\n"));

	_offsetMap = new JagArray< JagOffsetSimpfPair >();
	_offsetMap->useHash( true );
	_keyMap = new JagArray< JagKeyOffsetPair >();

	JagKeyOffsetPair keyOffsetPair;
	char minkbuf[_KLEN+1];
	int rc;
	for ( int i=0; i < vec.size(); ++i ) {
		_offsetMap->insert( vec[i] );

		simpf = (JagSimpFile*)vec[i].value.value();
		offset = vec[i].key.value();
		//prt(("s1228228 offsetmap inserted new i=%d offset=%lld\n", i, offset ));

		memset( minkbuf, 0, _KLEN+1);
		rc = simpf->getMinKeyBuf( minkbuf );
		if ( 1 || rc ) {
			// keyOffset map only
			//prt(("s222280 getMinKeyBuf rc=%d\n", rc ));
			makeKOPair( minkbuf, offset, keyOffsetPair );
			_keyMap->insert( keyOffsetPair );
			//prt(("s1228238 minkeymap inserted new i=%d minkbuf=[%s] offset=%lld\n", i, minkbuf, offset ));
		}
	}

}

void JagCompFile::buildInitIndex( bool force )
{
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) {
			simpf->buildInitIndex( force );
		}
	}
}

int  JagCompFile::buildInitIndexFromIdxFile()
{
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) {
			raydebug( stdout, JAG_LOG_LOW, "s318203 i=%d/%d  simpf->buildInitIndexFromIdxFile ...\n", i, arrlen );
			simpf->buildInitIndexFromIdxFile();
			raydebug( stdout, JAG_LOG_LOW, "s318203 i=%d/%d  simpf->buildInitIndexFromIdxFile done\n", i, arrlen );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "s318203 i=%d/%d  simpf==NULL skip buildInitIndexFromIdxFile\n", i, arrlen );
		}
	}
}

void JagCompFile::flushBlockIndexToDisk()
{
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) {
			raydebug( stdout, JAG_LOG_LOW, "s308103 i=%d flushBlockIndexToDisk ...\n", i );
			simpf->flushBlockIndexToDisk();
			raydebug( stdout, JAG_LOG_LOW, "s308103 i=%d flushBlockIndexToDisk done\n", i );
		} else {
			raydebug( stdout, JAG_LOG_LOW, "s308103 i=%d simpf==NULL\n", i );
		}
	}
}

void JagCompFile::removeBlockIndexIndDisk()
{
	jagint arrlen =  _offsetMap->size();
	JagSimpFile *simpf;
	for ( int i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		if ( simpf) {
			simpf->removeBlockIndexIndDisk();
		}
	}
}

// return # of elements in darr file
jagint JagCompFile::flushBufferToNewSimpFile( const JagDBMap *pairmap )
{
	//prt(("s082838 JagCompFile::flushBufferToNewFile ...\n"));
	jagint offset = _length;
	Jstr fpath =  _pathDir + "/" + longToStr( offset ); 

	JagSimpFile *simpf = new JagSimpFile( this, fpath, _KLEN, _VLEN );
	//prt(("s10292 simpf->flushBufferToNewFile pairmap.size=%d ...\n", pairmap->size() ));
	simpf->flushBufferToNewFile( pairmap );
	//prt(("s10293 simpf->flushBufferToNewFile done ...\n"));
	JagOffsetSimpfPair pair( offset, AbaxBuffer(simpf) );
	prt(("s01934 _offsetMap->insert() offset=%ld simpf=%s\n", offset, fpath.c_str() ));
	_offsetMap->insert( pair );
	//prt(("s01934 _offsetMap->insert() done\n"));

	JagKeyOffsetPair kopair;
	getMinKOPair( simpf, offset, kopair );
	//prt(("s1023 _keyMap->insert kopair.key=%s offset=%ld ..\n", kopair.key.c_str(), offset ));
	_keyMap->insert( kopair );

	_length += simpf->_length; 
	//prt(("s1298025 JagCompFile::flushBufferToNewFile done _length=%d ..\n", _length ));
	return simpf->_length/_KVLEN;
}

int JagCompFile::removePair( const JagDBPair &pair )
{
	JagSimpFile *simpf = getSimpFile( pair );
	if ( nullptr == simpf ) {
		prt(("s10192 JagCompFile::removePair simpf is NULL\n"));
		return -1;
	}

	int rc = simpf->removePair( pair );
	return rc;
}

int JagCompFile::updatePair( const JagDBPair &pair )
{
	JagSimpFile *simpf = getSimpFile( pair );
	if ( nullptr == simpf ) {
		//prt(("s10192 JagCompFile::updatePair simpf is NULL\n"));
		return -1;
	}

	int rc = simpf->updatePair( pair );
	return rc;
}

int JagCompFile::exist( const JagDBPair &pair, JagDBPair &retpair )
{
	JagSimpFile *simpf = getSimpFile( pair );
	if ( nullptr == simpf ) {
		//prt(("s10192 JagCompFile::updatePair simpf is NULL\n"));
		return -1;
	}

	int rc = simpf->exist( pair, retpair );
	return rc;
}


// begib/end record positions  record: 0, 1, 2, ... N-1 (each record has kvlen bytes)
bool JagCompFile::findFirstLast( const JagDBPair &pair, jagint &first, jagint &last ) const
{
	// should locate simpf, and call its own getPartElements, add global offset to it

	JagKeyOffsetPair tpair(pair.key);
	const JagKeyOffsetPair *tp = _keyMap->getPredOrEqual( tpair );
	if ( ! tp ) return false;
	AbaxLong goffset =  tp->value;
	//prt(("s122337 this=%0x JagCompFile::findFirstLast pair=[%s] goffset=%d\n", this, pair.key.c_str(), goffset.value() ));

	JagOffsetSimpfPair ofpair( goffset );
	const JagOffsetSimpfPair *op = _offsetMap->getPredOrEqual( ofpair );
	if ( ! op ) return false;
	jagint goffsetval = goffset.value();

	JagSimpFile *simpf = ( JagSimpFile*) op->value.value();
	bool rc = simpf->getFirstLast( pair, first, last );
	//prt(("s134231 simpf->getFirstLast rc=%d  first=%d last=%d\n", rc, first, last ));
	first = first + goffsetval/_KVLEN;
	last = last + goffsetval/_KVLEN;
	// first is index of blocks (32 elements), but goffset is byte position, so goffset needs /JAG_BLOCK_SIZE
	//prt(("s134231 added goffset=%d first=%d last=%d\n", goffsetval, first, last ));
	//prt(("s2029 print simpf:\n"));
	//simpf->print();
	return rc;
}

jagint JagCompFile::getPartElements( jagint pos ) const
{
    // should locate simpf, and call its own getPartElements, add global offset to it
	JagOffsetSimpfPair ofpair( pos );
	const JagOffsetSimpfPair *op = _offsetMap->getPredOrEqual( ofpair );
	if ( ! op ) return 0;

	JagSimpFile *simpf = ( JagSimpFile*) op->value.value();
	jagint rc = simpf->getPartElements( pos - op->key.value() );
	return rc;
}


JagSimpFile *JagCompFile::getSimpFile(  const JagDBPair &pair )
{
	JagKeyOffsetPair tpair(pair.key);
	const JagKeyOffsetPair *tp = _keyMap->getPredOrEqual( tpair );
	if ( ! tp ) return nullptr;

	AbaxLong offset =  tp->value;
	JagOffsetSimpfPair ofpair( offset );

	const JagOffsetSimpfPair *op = _offsetMap->getPredOrEqual( ofpair );
	if ( ! op ) return nullptr;

	JagSimpFile *simpf = ( JagSimpFile*) op->value.value();
	return simpf;
}

void JagCompFile::print()
{
	jagint arrlen = _offsetMap->size();
	prints(("s4444829 JagCompFile::print() arrlen=%d\n", arrlen ));
	JagSimpFile *simpf;
	for ( jagint i = 0; i < arrlen; ++i ) {
		if ( _offsetMap->isNull(i) ) { continue; }
		prints(("    i=%d simpf:\n", i ));
		simpf = (JagSimpFile*) (*_offsetMap)[i].value.value();
		simpf->print();
	}
}
