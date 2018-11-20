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

#include <JagSingleBuffWriter.h>
#include <JagUtil.h>
#include <JagCfg.h>
#include <JagDef.h>

JagSingleBuffWriter::JagSingleBuffWriter( int fd, int kvlen, abaxint bufferSize )
{
	KVLEN = kvlen;
	_fd = fd;
	_superbuf = NULL;
	if ( _fd < 0 ) { return; }

	if ( bufferSize == -1 ) {
		bufferSize = 128;
	}
	SUPERBLOCK = bufferSize*1024*1024/KVLEN/JagCfg::_BLOCK*JagCfg::_BLOCK;
	SUPERBLOCKLEN = SUPERBLOCK * KVLEN;
	/***
	if ( SUPERBLOCKLEN > 512*1024*1024 ) {
		SUPERBLOCK = (512*1024*1024)/KVLEN;
		SUPERBLOCKLEN = SUPERBLOCK * KVLEN;
	}
	***/

	//raydebug( stdout, JAG_LOG_HIGH, "s2829 sigbufwtr SUPERBLOCKLEN=%l ...\n", SUPERBLOCKLEN );
	_superbuf = (char*) jagmalloc( SUPERBLOCKLEN );
	//raydebug( stdout, JAG_LOG_HIGH, "s2829 sigbufwtr SUPERBLOCKLEN=%l done\n", SUPERBLOCKLEN );
	_lastSuperBlock = -1;
	_relpos = -1;

	memset( _superbuf, 0, SUPERBLOCKLEN );
}

JagSingleBuffWriter::~JagSingleBuffWriter()
{
	if ( _superbuf ) {
		free( _superbuf );
		_superbuf = NULL;
		jagmalloc_trim( 0 );
		raydebug( stdout, JAG_LOG_HIGH, "s2829 sigbufwtr SUPERBLOCKLEN=%l dtor\n", SUPERBLOCKLEN );
	}
}

/***
void JagSingleBuffWriter::resetKVLEN( int newkvlen )
{
	KVLEN = newkvlen;
	SUPERBLOCKLEN = SUPERBLOCK * KVLEN;
	if ( _superbuf ) {
		free( _superbuf );
	}
	_superbuf = (char*) malloc( SUPERBLOCKLEN );
    _lastSuperBlock = -1;
    _relpos = -1;
	
	memset( _superbuf, 0, SUPERBLOCKLEN );
}
***/

void JagSingleBuffWriter::writeit( abaxint pos, const char *keyvalbuf, abaxint KVLEN )
{
	_relpos = pos % SUPERBLOCK;  // elements
	abaxint rc; 
	int newBlock = pos / SUPERBLOCK;
	if ( -1 == _lastSuperBlock ) {
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		_lastSuperBlock = newBlock;
		return;
	}

	if ( newBlock == _lastSuperBlock ) {
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		return;
	} else { // moved to new block, flush old block
		rc = raysafepwrite( _fd, _superbuf, SUPERBLOCKLEN, _lastSuperBlock*SUPERBLOCKLEN );
		memset( _superbuf, 0, SUPERBLOCKLEN );
		memcpy( _superbuf+_relpos*KVLEN, keyvalbuf, KVLEN );
		_lastSuperBlock = newBlock;
		return;
	}
}

void JagSingleBuffWriter::flushBuffer()
{
	if ( _lastSuperBlock == -1 ) { return; }
	abaxint rc = raysafepwrite( _fd, _superbuf, (_relpos+1)*KVLEN, _lastSuperBlock*SUPERBLOCKLEN );
	_lastSuperBlock = -1;
	_relpos = -1;
	return;
}
