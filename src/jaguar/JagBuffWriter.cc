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

#include <JagBuffWriter.h>
#include <JagUtil.h>
#include <JDFS.h>

// inline JagBuffWriter<Pair>::JagBuffWriter( int fd, int kvlen, abaxint headoffset )
JagBuffWriter::JagBuffWriter( JDFS *jdfs, int kvlen, abaxint headoffset, abaxint bufferSize )
{
	_jdfs = jdfs;
	KVLEN = kvlen;

	if ( bufferSize == -1 ) {
		bufferSize = 128;
	}
	SUPERBLOCK = bufferSize*1024*1024/KVLEN/JagCfg::_BLOCK*JagCfg::_BLOCK;
	SUPERBLOCKLEN = SUPERBLOCK * KVLEN;
	/***
	if ( SUPERBLOCKLEN > 128*1024*1024 ) {
		SUPERBLOCK = (128*1024*1024)/KVLEN;
		SUPERBLOCKLEN = SUPERBLOCK * KVLEN;
	}
	***/

	//raydebug( stdout, JAG_LOG_HIGH, "s2829 bufwtr SUPERBLOCKLEN=%l ...\n", SUPERBLOCKLEN );
	_superbuf = (char*) jagmalloc( SUPERBLOCKLEN );
	//raydebug( stdout, JAG_LOG_HIGH, "s2829 bufwtr SUPERBLOCKLEN=%l done\n", SUPERBLOCKLEN );
	_lastBlock = -1;
	_relpos = -1;
	_headoffset = headoffset;

	// lseek( fd, 0, SEEK_SET );
	memset( _superbuf, 0, SUPERBLOCKLEN );
}

JagBuffWriter::~JagBuffWriter()
{
	if ( _superbuf ) {
		free( _superbuf );
		_superbuf = NULL;
		jagmalloc_trim( 0 );
		//raydebug( stdout, JAG_LOG_HIGH, "s2829 bufwtr SUPERBLOCKLEN=%l dtor\n", SUPERBLOCKLEN );
	}
}

void JagBuffWriter::writeit( abaxint pos, const char *keyvalbuf, abaxint KVLEN )
{
	_relpos = (pos-_headoffset/KVLEN) % SUPERBLOCK;  // elements
	int newBlock = (pos-_headoffset/KVLEN) / SUPERBLOCK;
	if ( -1 == _lastBlock ) {
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		_lastBlock = newBlock;
		return;
	}

	if ( newBlock == _lastBlock ) {
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		return;
	} else { // moved to new block, flush old block
		// prt(("s3005 raypwrite rcc\n" ));
		raypwrite( _jdfs, _superbuf, SUPERBLOCKLEN, _lastBlock * SUPERBLOCKLEN + _headoffset );
		// prt(("s3006 raypwrite rcc\n" ));
		memset( _superbuf, 0, SUPERBLOCKLEN );
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		_lastBlock = newBlock;
		return;
	}
}

void JagBuffWriter::flushBuffer()
{
	if ( _lastBlock == -1 ) {
		return;
	}
	// prt(("s3007 raypwrite rcc\n" ));
	raypwrite( _jdfs, _superbuf, (_relpos+1)*KVLEN, _lastBlock*SUPERBLOCKLEN + _headoffset );
	// prt(("s3008 raypwrite rcc\n" ));
	_lastBlock = -1;
	_relpos = -1;
	return;
}

