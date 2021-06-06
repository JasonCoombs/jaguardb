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
	_keyMode = _schemaRecord->getKeyMode(); 
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
	if ( split2.length() > 2 ) { 
		_pdbobj = _dbname + "." + split2[1];
	} else if ( split2.length() == 2 ) { 
		if ( isdigit(*(split2[1].c_str())) ) { 
			_pdbobj = _dbname + "." + split2[0];
		} else { 
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

void JagDiskArrayServer::buildInitIndex( bool force )
{
	if ( ! force ) {
		int rc = buildInitIndexFromIdxFile();
		if ( rc ) {
			jagmalloc_trim( 0 );
			return;
		}
	}

	if ( !force && _doneIndex ){
		dbg(("s3820 in buildInitIndex return here\n"));
		return;
	}

	_compf->buildInitIndex( force );

}

int JagDiskArrayServer::buildInitIndexFromIdxFile()
{
	if ( _doneIndex ){
		return 1;
	}

	_compf->buildInitIndexFromIdxFile();
	return 1;
}

bool JagDiskArrayServer::exist( const JagDBPair &pair, jagint *index, JagDBPair &retpair )
{
	jagint first, last;
	++_reads;
	bool rc = getFirstLast( pair, first, last ); 
	if ( rc ) {
		rc = findPred( pair, index, first, last, retpair, NULL );
	} else {
		return false;
	}

	if ( ! rc ) return false;
	return true;
}

bool JagDiskArrayServer::exist( const JagDBPair &pair, JagDBPair &retpair )
{
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) return false;
	int rc = compf->exist( pair, retpair );
	if ( rc < 0 ) {
		return false;
	}
	return true;
}

bool JagDiskArrayServer::get( JagDBPair &pair, jagint &index )
{
	JAG_OVER;
	JagDBPair getpair;
	bool rc = exist( pair, &index, getpair );
	if ( rc ) pair = getpair;
	return rc;
}

bool JagDiskArrayServer::set( const JagDBPair &pair, jagint &index )
{
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) {
		return false; 
	}

	int rc = compf->updatePair( pair );
    if ( rc < 0 ) {
        return false;
    }

    return true;
	
}

bool JagDiskArrayServer::remove( const JagDBPair &pair )
{
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) return false;
	int rc = compf->removePair( pair );
	if ( rc < 0 ) {
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
	prt(("s9028 start=%d, end=%d\n", start, end));
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
	JagCompFile *compf = _jdfs->getCompf();
	if ( ! compf ) {
		return 0;
	}
	float cost = compf->computeMergeCost( pairmap, seqReadSpeed, seqWriteSpeed, vec );
	return cost ;
}

