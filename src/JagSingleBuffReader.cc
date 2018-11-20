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

#include <fcntl.h>
#include <sys/stat.h>

#include <JagSingleBuffReader.h>
#include <JagCfg.h>
#include <JagUtil.h>

///////////////// implementation /////////////////////////////////

// readlen is number of KEYVAL records, not bytes
JagSingleBuffReader::JagSingleBuffReader ( int fd, abaxint readlen, int keylen, int vallen, abaxint start, abaxint headoffset, abaxint bufferSize ) 
{
	KEYLEN = keylen;
	VALLEN = vallen;
	KEYVALLEN = keylen + vallen;

	abaxint NB = getNumBlocks( KEYVALLEN, bufferSize );
	_elements = NB * JagCfg::_BLOCK;
	_headoffset = headoffset;

	_fd = fd;
	_superbuf = NULL;
	
	if ( _fd < 0 ) { return; }
    struct stat sbuf;
    int rc = fstat( _fd, &sbuf );
    if ( rc < 0 ) { return; }

	_start = start;
	_readlen = readlen;
	if ( start < 0 ) { _start = 0; }
	if ( readlen < 0 || readlen > sbuf.st_size/KEYVALLEN ) _readlen = sbuf.st_size/KEYVALLEN;
	
	// raydebug( stdout, JAG_LOG_HIGH, "s2829 sigbufrdr _elements=%l mem=%l\n", _elements, _elements * (abaxint)KEYVALLEN );  
	_superbuf = (char*) jagmalloc ( _elements * (abaxint)KEYVALLEN );
	while ( NULL == _superbuf ) {
		// printf("s8393 JagBuffReader::JagBuffReader _superbuf malloc failed size=%lld KEYVALLEN=%d \n", _elements * KEYVALLEN, KEYVALLEN );
		_elements = _elements / 2;
		_superbuf = (char*) jagmalloc ( _elements * (abaxint)KEYVALLEN );

		if ( _superbuf ) {
			raydebug( stdout, JAG_LOG_LOW, "JagSingleBuffReader malloc smaller memory %l _elements=%l _fd=%l\n", 
					_elements * (abaxint)KEYVALLEN, _elements, _fd );
		}
	}

	//raydebug( stdout, JAG_LOG_HIGH, "s3829 sigbufrdr _elements=%l mem=%l\n", _elements, _elements * (abaxint)KEYVALLEN );  
	memset( _superbuf, 0, KEYVALLEN );
	_lastSuperBlock = -1;
	_relpos = 0;
}

abaxint JagSingleBuffReader::getNumBlocks( int kvlen, abaxint bufferSize )
{
	abaxint num = 4096;
	abaxint bytes = num * JagCfg::_BLOCK * kvlen;
	abaxint maxmb = 128;

	if ( bufferSize > 0 ) {
		maxmb = bufferSize;
		bytes = maxmb * 1024 * 1024;
		num = bytes/kvlen/JagCfg::_BLOCK;
	}

	if ( bytes > maxmb *1024*1024 ) {
		// num = num / 2;
		num = maxmb*1024*1024/kvlen/JagCfg::_BLOCK;
	}

	// printf("s73849 JagBuffReader::getNumBlocks() num=%d blocks\n", num );
	return num;
}

JagSingleBuffReader::~JagSingleBuffReader ()
{
	if ( _superbuf ) {
		free( _superbuf );
		_superbuf = NULL;
		jagmalloc_trim(0);
		//raydebug( stdout, JAG_LOG_HIGH, "s3829 sigbufrdr _elements=%l dtor\n", _elements );
	}
}

bool JagSingleBuffReader::getNext ( char *buf )
{
	int len = KEYVALLEN;
	abaxint pos;
	return getNext( buf, len, pos );
}

bool JagSingleBuffReader::getNext ( char *buf, abaxint &i )
{
	int len = KEYVALLEN;
	return getNext( buf, len, i );
}

bool JagSingleBuffReader::getNext ( char *buf, int len, abaxint &i )
{
	abaxint rc;
	if ( _fd < 0 ) { return false; }
	
	if ( len < KEYVALLEN ) {
		printf("e8394 error JagSingleBuffReader::getNext passedin len=%d is less than KEYVALLEN=%d\n", len, KEYVALLEN );
		return false;
	}

	
	if ( ( (_lastSuperBlock)*_elements + _relpos ) >= _readlen ) {
		return false;
	}
    if ( -1 == _lastSuperBlock ) {
		if ( _readlen <= _elements ) {  // total is smaller than a single superblock
			rc = raysafepread( _fd, _superbuf, _readlen*KEYVALLEN, _start*KEYVALLEN+_headoffset );
		} else {
			rc = raysafepread( _fd, _superbuf, _elements*KEYVALLEN, _start*KEYVALLEN+_headoffset );
		}
		if ( rc < 0 ) { return false; }
        _lastSuperBlock = 0; 
    }

	rc = findNonblankElement( buf, i );
	if ( !rc ) { return false; }
    return true;
}


bool JagSingleBuffReader::findNonblankElement( char *buf, abaxint &i )
{
	abaxint readbytes;
	abaxint  rc;
	
	abaxint seg, maxcheck;
	seg = ( _readlen - _lastSuperBlock*_elements );

	while ( true ) {
		if ( seg >= _elements ) {
			maxcheck = _elements;
		} else {
			maxcheck = seg;
		}

		if ( _relpos < maxcheck ) {
	 		if ( _superbuf[_relpos*KEYVALLEN] == '\0' ) { 
				++_relpos;
				continue;
			} else {
				memcpy(buf, _superbuf+_relpos*KEYVALLEN, KEYVALLEN);
				i = _lastSuperBlock*_elements + _relpos;
				++_relpos;
				return true;
			}

		} else if ( maxcheck < _elements ) {
			return false;
		} else {
        	_relpos = 0;
			++ _lastSuperBlock;
			seg = ( _readlen - _lastSuperBlock*_elements );
			
			if ( seg == 0 ) {
				return false;
			} else if ( seg < _elements ) {
				readbytes = seg*KEYVALLEN;
        	} else {
				readbytes = _elements*KEYVALLEN;
        	}
			rc = raysafepread( _fd, _superbuf, readbytes, (_start+_lastSuperBlock*_elements)*KEYVALLEN+_headoffset );
			if ( rc < 0 ) { return false; }
		}
		continue;
	}
    return false;
}
