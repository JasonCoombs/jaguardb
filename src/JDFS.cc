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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <JagDBServer.h>
#include <JDFS.h>
#include <JDFSMgr.h>
#include <JaguarCPPClient.h>
#include <JagUtil.h>
#include <JagDBConnector.h>

/***************************************************************************
**
** Client object to manage one  file
**
***************************************************************************/

// fpath is full path of a file, without stripe number
// all JDFS methods process localfile only
// ctor
JDFS::JDFS( JagDBServer *servobj, const Jstr &fpath, int kvlen, abaxint stripeSize )
{
	_kvlen = kvlen;
	_fpath = fpath;
	_servobj = servobj;
	if ( _servobj ) {
		_jdfsMgr = _servobj->jdfsMgr;
	} else {
		_jdfsMgr = new JDFSMgr();
	}
	_stripeSize = _jdfsMgr->getStripeSize( _fpath, _kvlen, stripeSize ); // # of kvlens, ie., arrlen
}

// dtor
JDFS::~JDFS()
{
	close();

	if ( !_servobj ) {
		delete _jdfsMgr;
	}
}

// get local host file descriptor
int JDFS::getFD() 
{
	return _jdfsMgr->open( _fpath );
}

abaxint JDFS::getFileSize() const
{
	return _jdfsMgr->getFileSize( _fpath, _kvlen );
}

int JDFS::exist() const
{
	return _jdfsMgr->exist( _fpath );
}

int JDFS::open()
{		
	return _jdfsMgr->open( _fpath, true );
}

int JDFS::close()
{
	return _jdfsMgr->close( _fpath );
}

int JDFS::rename( const Jstr &newpath )
{
	_jdfsMgr->rename( _fpath, newpath );
	_fpath = newpath;
	close();
	open();
	return 1;
}

int JDFS::remove()
{
	return _jdfsMgr->remove( _fpath );
}

int JDFS::fallocate( abaxint offset, abaxint len )
{
	int fd = _jdfsMgr->open( _fpath, true );
	if ( fd < 0 ) return -1;
	return jagftruncate( fd, offset+len );
}

time_t JDFS::getfileUpdateTime() const
{
	time_t upt = 0;
	if ( exist() ) {
		int fd = -1, src;
		struct stat statbuf;
		fd = _jdfsMgr->open( _fpath );
		if ( fd >= 0 ) {
			src = fstat( fd, &statbuf );
			if ( 0 == src ) upt = statbuf.st_mtime;
		}
	}
	return upt;
}

abaxint JDFS::getArrayLength() const
{
	if ( !exist() ) return 0;
	return _stripeSize;
}

// read data from local
abaxint JDFS::pread( char *buf, size_t len, size_t offset ) const
{
	if ( offset < 0 ) offset = 0;
	int fd = _jdfsMgr->open( _fpath );
	if ( fd < 0 ) return -1;
	offset = _jdfsMgr->pread( fd, buf, len, offset );
	return offset;
}

// write data to local
abaxint JDFS::pwrite( const char *buf, size_t len, size_t offset )
{
	if ( offset < 0 ) offset = 0;
	int fd = _jdfsMgr->open( _fpath );
	if ( fd < 0 ) return -1;
	offset = _jdfsMgr->pwrite( fd, buf, len, offset );
	return offset;
}
