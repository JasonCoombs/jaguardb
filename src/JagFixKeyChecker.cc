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

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <JagFixKeyChecker.h>
#include <JagUtil.h>
#include <JagMD5lib.h>
#include <JagSingleBuffReader.h>
#include <JagFileMgr.h>

JagFixKeyChecker::JagFixKeyChecker( const Jstr &pathName, int klen, int vlen )
: JagFamilyKeyChecker( pathName, klen, vlen )
{
	if ( _useHash ) {
		_keyCheckArr = new JagFixHashArray( _UKLEN, JAG_KEYCHECKER_VLEN );
	} else {
		_keyCheckArr = new JagFixHashArray( _UKLEN, JAG_KEYCHECKER_VLEN );
	}
}

void JagFixKeyChecker::destroy()
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	if ( _keyCheckArr ) {
		delete _keyCheckArr;
		_keyCheckArr = NULL;
		jagmalloc_trim( 0 );
	}
}

bool JagFixKeyChecker::addKeyValue( const char *kv )
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	return addKeyValueNoLock( kv );
}

// kv:  [kkkkk\0\0\0][vv]
//       _KLEN        2
bool JagFixKeyChecker::addKeyValueNoLock( const char *kv )
{
	char ukey[_UKLEN+1];  // 13 or 22(max)
	char uv[_UKLEN+JAG_KEYCHECKER_VLEN];

	getUniqueKey( kv, ukey );  //md5 and hash of kv

	memcpy( uv, ukey, _UKLEN );
	memcpy( uv+_UKLEN, kv+_KLEN, JAG_KEYCHECKER_VLEN );
	bool rc =  _keyCheckArr->insert( uv );
	return rc; 
}

bool JagFixKeyChecker::addKeyValueInit( const char *kv )
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	return _keyCheckArr->insert( kv );
}

bool JagFixKeyChecker::getValue( const char *key, char *value )
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	char ukey[_UKLEN+1];
	getUniqueKey( key, ukey );
	bool rc =  _keyCheckArr->get( ukey, value );
	// prt(("s7636 JagFixKeyChecker::getValue key=[%s] rc=%d\n", key, rc ));
	if ( ! rc ) return false;
	return true;
}

bool JagFixKeyChecker::removeKey( const char *key )
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	char ukey[_UKLEN+1];
	getUniqueKey( key, ukey );
	return _keyCheckArr->remove( ukey );
}

bool JagFixKeyChecker::exist( const char *key ) const
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	char ukey[_UKLEN+1]; jagint idx;
	getUniqueKey( key, ukey );
	bool rc = _keyCheckArr->exist( ukey, &idx );
	// prt(("s7236 JagFixKeyChecker::exist key=[%s] rc=%d\n", key, rc ));
	return rc;
}

void JagFixKeyChecker::removeAllKey()
{
	//JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	_keyCheckArr->removeAll();
}

// build keyChecker from disk file
int JagFixKeyChecker::buildInitKeyCheckerFromSigFile()
{
	raydebug( stdout, JAG_LOG_HIGH, "fixkcheck buildInitKeyCheckerFromSigFile ...\n" );

	int klen = _UKLEN;
	int vlen = JAG_KEYCHECKER_VLEN;
	char buf[klen+vlen+1];
	memset( buf, 0, klen+vlen+1 );

	//Jstr sigfpath = _pathName + ".sig";
	Jstr hdbfpath = _pathName + ".hdb";
	Jstr keyCheckerPath = _pathName + ".sig";

	prt(("s142238 _pathName=[%s]\n", _pathName.c_str() ));
	prt(("s142238 hdbfpath=[%s]\n", hdbfpath.c_str() ));
	prt(("s142238 keyCheckerPath=[%s]\n", keyCheckerPath.c_str() ));

	jagint hdbsize = JagFileMgr::fileSize( hdbfpath );
	if ( hdbsize > 0 && JagFileMgr::fileSize( keyCheckerPath ) < 1 ) {
		// read from hdb 
		prt(("s222293 \n"));
		int fd = jagopen ( hdbfpath.c_str(), O_RDONLY|JAG_NOATIME );
		if ( fd < 0 ) {
			prt(("s1537 open %s error\n", hdbfpath.c_str() ));
			return 0;
		}

		jagint rlimit = getBuffReaderWriterMemorySize( (hdbsize-1)/1024/1024 );
		JagSingleBuffReader nav( fd, hdbsize/(klen+vlen), klen, vlen, 0, 0, rlimit );
		jagint cnt = 0;
		bool rc;
		while( nav.getNext( buf ) ) {
			rc = addKeyValueInit( buf );
			// prt(("s1238 hdb buf=[%s] rc=%d\n", buf, rc ));
			memset( buf, 0, klen+vlen+1 );
			++cnt;
		}
		jagclose( fd );
		jagunlink( hdbfpath.c_str() );
		prt(("s4837 read hdb %s cnt=%lld\n", hdbfpath.c_str(), cnt ));
		return 1;
	}

	int fd = jagopen((char *)keyCheckerPath.c_str(), O_RDONLY|JAG_NOATIME );
	if ( fd < 0 ) {
		prt(("s2283 keyCheckerPath=[%s] not found\n", keyCheckerPath.c_str() ));
		return 0;
	}

	raysaferead( fd, buf, 1 );
	if ( buf[0] != '0' ) {
		prt(("s0981 Sig file corrupted for [%s]. Rebuild from disk file...\n", _pathName.c_str()));
		jagclose( fd );
		jagunlink( keyCheckerPath.c_str() );
		return 0;
	}

	struct stat sbuf;
	stat(keyCheckerPath.c_str(), &sbuf);
	if ( sbuf.st_size < 1 ) {
		prt(("s9830 sig file corrupted for [%s]. Rebuild from disk file...\n", _pathName.c_str()));
		jagclose( fd );
		jagunlink( keyCheckerPath.c_str() );
		return 0;
	}

	jagint rlimit = getBuffReaderWriterMemorySize( (sbuf.st_size-1)/1024/1024 );
	JagSingleBuffReader br( fd, (sbuf.st_size-1)/(klen+vlen), klen, vlen, 0, 1, rlimit );
	jagint cnt = 0; // , callCounts = -1, lastBytes = 0;

	//jagint mem1 = availableMemory( callCounts, lastBytes );
	//prt(("s1421 availmem=%lld MB begin keycheck\n", mem1/ONE_MEGA_BYTES ));
	raydebug( stdout, JAG_LOG_LOW, "begin reading sig file ...\n" );
	JagFixString vstr;
	memset( buf, 0, klen+vlen+1 );
	bool rc;
	while ( br.getNext( buf ) ) {
		++cnt;
		rc = addKeyValueInit( buf );
		// prt(("s1238 sig buf=[%s] rc=%d\n", buf, rc ));
		memset( buf, 0, klen+vlen+1 );
	}
	raydebug( stdout, JAG_LOG_LOW, "done reading sig file %l records rlimit=%l\n", cnt, rlimit );
	jagclose( fd );
	jagunlink( keyCheckerPath.c_str() );
	jagmalloc_trim(0);
	return 1;
}
