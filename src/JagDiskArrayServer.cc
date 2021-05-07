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

#include <JagDiskArrayServer.h>
#include <JagHashLock.h>
#include <JDFS.h>
#include <JagDataAggregate.h>
#include <JagSingleBuffReader.h>
#include <JagBuffBackReader.h>
#include <JagFileMgr.h>
#include <JagUtil.h>
#include <JagDiskArrayFamily.h>
#include <JagCompFile.h>

JagDiskArrayServer::JagDiskArrayServer( const JagDBServer *servobj, JagDiskArrayFamily *fam, int index, const Jstr &filePathName, 
										const JagSchemaRecord *record, jagint length, bool buildInitIndex ) 
    :JagDiskArrayBase( servobj, fam, filePathName, record, index )
{
	_isClient = 0;
	if ( buildInitIndex ) {
		this->init( length, true );
	} else {
		this->init( length, false );
	}
}

JagDiskArrayServer::~JagDiskArrayServer()
{
	if ( _maxKey ) {
		free( _maxKey );
	}
}

void JagDiskArrayServer::init( jagint length, bool buildBlockIndex )
{
	_KLEN = _schemaRecord->keyLength;
	_VLEN = _schemaRecord->valueLength;
	_KVLEN = _KLEN+_VLEN;
	_keyMode = _schemaRecord->getKeyMode(); // get keyMode
	_maxindex = 0;
	_minindex = -1;
	_GEO = 4;
	_arrlen = _doneIndex = 0;
	const char *dp = strrchr ( _pathname.c_str(), '/' );
	_dirPath = Jstr(_pathname.c_str(), dp-_pathname.c_str());
	_filePath = _pathname + ".jdb"; 
	_tmpFilePath =  _pathname + ".jdb.tmp";
	JagStrSplit split( _pathname, '/' );
	int exist = split.length();
	if ( exist < 3 ) {
		printf("s7482 error _pathname=%s, exit\n", _pathname.c_str() );
		exit(70);
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
	} else if ( split2.length() == 2 ) { // may be tab.idx or tab.1 format; if tab.idx, use idx, otherwise, use tab
		if ( isdigit(*(split2[1].c_str())) ) { // is tab.1 format, use tab
			_pdbobj = _dbname + "." + split2[0];
		} else { // otherwise, tab.idx format, use idx
			_pdbobj = _dbname + "." + split2[1];
		}
	} else {
		_pdbobj = _dbobj;
	}

	_jdfs = new JDFS( (JagDBServer*)_servobj, _family, _filePath, _KLEN, _VLEN ); 
	_nthserv = 0;
	_numservs = 1;
	if ( ! JagFileMgr::exist( _dirPath ) ) {
		prt(("s3910 makedirPath(%s)\n", _dirPath.c_str() ));
		JagFileMgr::makedirPath( _dirPath, 0755 );
	}
	
	exist = _jdfs->exist();
	_jdfs->open();
	_compf = _jdfs->getCompf();
	
	_arrlen = _jdfs->getArrayLength();
	if ( !exist ) {
		size_t bytes = _arrlen * _KVLEN;
		_jdfs->fallocate( 0, bytes );
	} else {
		if ( buildBlockIndex ) {
			buildInitIndex();
		}
	}

	_maxKey = jagmalloc( _KLEN + 1 );
	memset( _maxKey, 255, _KLEN );
	_maxKey[_KLEN] = 0;
}

// build and find first last from the blockIndex
// buildindex from disk array file data
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

	_compf->buildInitIndex( force );

}

// buildindex from idx file flushed from blockindex 
// 1: OK
// 0: error
int JagDiskArrayServer::buildInitIndexFromIdxFile()
{
	if ( _doneIndex ){
		dbg(("s3822 in buildInitIndexFromIdxFile return here\n"));
		return 1;
	}

	_compf->buildInitIndexFromIdxFile();
	return 1;
}

// old way
// check pair exist
bool JagDiskArrayServer::exist( const JagDBPair &pair, jagint *index, JagDBPair &retpair )
{
	jagint first, last;
	++_reads;
	bool rc = getFirstLast( pair, first, last ); // first/last is global in this comp file
	if ( rc ) {
		rc = findPred( pair, index, first, last, retpair, NULL );
	} else {
		return false;
	}

	if ( ! rc ) return false;
	return true;
}

// return true for yes; false for not exist
bool JagDiskArrayServer::exist( const JagDBPair &pair, JagDBPair &retpair )
{
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) return false;
	int rc = compf->exist( pair, retpair );
	if ( rc < 0 ) {
		prt(("s2037 compf->exist rc=%d\n", rc ));
		return false;
	}
	return true;
}

// get a pair
// return true for OK; false for error
bool JagDiskArrayServer::get( JagDBPair &pair, jagint &index )
{
	JAG_OVER;
	JagDBPair getpair;
	bool rc = exist( pair, &index, getpair );
	if ( rc ) pair = getpair;
	return rc;
}

// set a pair
// return true for OK; false for error
bool JagDiskArrayServer::set( const JagDBPair &pair, jagint &index )
{
	//prt(("s432276 JagDiskArrayServer::set() pair=[%s][%s] ...\n", pair.key.c_str(), pair.value.s() ));
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) {
		//prt(("s211028 compf NULL, return false\n"));
		return false; 
	}

	int rc = compf->updatePair( pair );
    if ( rc < 0 ) {
        prt(("s20937 compf->updatePair rc=%d pair=[%s][%s] return false\n", rc, pair.key.c_str(), pair.value.s() ));
        return false;
    }

	//prt(("s432278 updatePair=[%s][%s] true\n", pair.key.c_str(), pair.value.s() ));
    return true;
	
}

bool JagDiskArrayServer::remove( const JagDBPair &pair )
{
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) return false;
	int rc = compf->removePair( pair );
	if ( rc < 0 ) {
		prt(("s2037 compf->removePair rc=%d\n", rc ));
		return false;
	}
	return true;
}

void JagDiskArrayServer::print( jagint start, jagint end, jagint limit ) 
{
	if ( start == -1 || start < _nthserv*_arrlen ) {
		start = _nthserv*_arrlen;
	}
	
	if ( end == -1 || end > _nthserv*_arrlen+_arrlen ) {
		end = _nthserv*_arrlen+_arrlen;
	}
	
	char  *keybuf = (char*)jagmalloc( _KLEN+1 );
	char  *keyvalbuf = (char*)jagmalloc( _KVLEN+1 );
	prt(("s9999 start=%d, end=%d\n", start, end));
	prt(("************ print() dbobj=%s _arrlen=%lld _KVLEN=%d  _KLEN=%d\n", 
		_dbobj.c_str(), (jagint)_arrlen, _KVLEN, _KLEN ));

	for ( jagint i = start; i < end; ++i ) {
		memset( keyvalbuf, 0, _KVLEN+1 );
		raypread( _jdfs, keyvalbuf, _KVLEN, i*_KVLEN );
		memset( keybuf, 0, _KLEN+1 );
		memcpy( keybuf, keyvalbuf, _KLEN );
		prt(("%15d   [%s] --> [%s]\n", i, keybuf, keyvalbuf+_KLEN ));
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
	jagint ipos;
	Jstr sendmsg = _pdbobj + " check finished ";
   	char keybuf[ _KLEN+1];
   	char *keyvalbuf = (char*)jagmalloc( _KVLEN+1);
	memset( keybuf, 0,  _KLEN + 1 );
	memset( keyvalbuf, 0,  _KVLEN + 1 );
	jagint rlimit = getBuffReaderWriterMemorySize( _arrlen*_KVLEN/1024/1024 );
	JagBuffReader br( this, _arrlen, _KLEN, _VLEN, _nthserv*_arrlen, 0, rlimit );
	while ( br.getNext( keyvalbuf, _KVLEN, ipos ) ) { 
		if ( ipos*100/_arrlen >= percnt ) {
			sendMessage( req, Jstr(sendmsg+intToStr(percnt)+"% ...").c_str(), "OK" );
			percnt += 5;
		}
		if ( keybuf[0] == '\0' ) {
			memcpy( keybuf, keyvalbuf, _KLEN );
		} else {
			rc = memcmp( keyvalbuf, keybuf, _KLEN );
			if ( rc <= 0 ) {
				//free( keybuf );
				free( keyvalbuf );
				return 0;
			} else {
				memcpy( keybuf, keyvalbuf, _KLEN );
			}
		}
	}
	sendmsg = _pdbobj + " check 100\% complete.";
	sendMessage( req, sendmsg.c_str(), "OK" );
	free( keyvalbuf );
	return 1;
}


// read bottom level of blockindex and write it to a disk file
void JagDiskArrayServer::flushBlockIndexToDisk()
{
	_compf->flushBlockIndexToDisk();
}

void JagDiskArrayServer::removeBlockIndexIndDisk()
{
	_compf->removeBlockIndexIndDisk();
}

float JagDiskArrayServer::computeMergeCost( const JagDBMap *pairmap, jagint seqReadSpeed, 
											jagint seqWriteSpeed, JagVector<JagMergeSeg> &vec )
{
	// look at each simplfile, get its pred from pairmap
	// get cost( number of records) of each simple for read and write, compute data size in bytes to be read and wite
	// compute time to read and write
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) {
		return 0;
	}
	float cost = compf->computeMergeCost( pairmap, seqReadSpeed, seqWriteSpeed, vec );
	return cost ;
}

