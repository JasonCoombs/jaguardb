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
#include <JDFSMgr.h>
#include <JagFileMgr.h>
#include <JagDef.h>
#include <JagUtil.h>

JDFSMgr::JDFSMgr()
{
	_map = new JagHashMap<AbaxString,AbaxInt>( 100 );
}

JDFSMgr::~JDFSMgr()
{
	// for each item, close the FD in _map
	if ( _map ) delete _map;
	_map = NULL;
}

bool JDFSMgr::exist( const AbaxString &fpath )
{
	if ( 0 == jagaccess( fpath.c_str(), R_OK|W_OK ) ) {
		// printf("s3840 JDFSMgr::exist [%s] true \n", fpath.c_str() );
		return true;
	} else {
		// printf("s3840 JDFSMgr::exist [%s] false \n", fpath.c_str() );
		return false;
	}
}

abaxint JDFSMgr::getStripeSize( const AbaxString &fpath, size_t kvlen, size_t stripeSize )
{
	// prt(("s2839 getStripeSize fpath=[%s] kvlen=%d stripeSize=%lld\n", fpath.c_str(), kvlen, stripeSize ));
	
	struct stat sbuf;
	if ( 0 == stat(fpath.c_str(), &sbuf) ) {
		if ( sbuf.st_size > 0 ) {
			// prt(("s1883 return sbuf.st_size/kvlen=%d\n", sbuf.st_size/kvlen ));
			return sbuf.st_size/kvlen;
		}
	}

	// file not exist
	if ( stripeSize > 0 ) {
		// prt(("s2737 return stripeSize=%d\n", stripeSize ));
		return stripeSize;
	} else {
		// prt(("s2738 return JAG_ARRLEN_INIT=%d\n", JAG_ARRLEN_INIT ));
		return JAG_ARRLEN_INIT;
	}

}

abaxint JDFSMgr::getFileSize( const AbaxString &fpath, size_t kvlen )
{
	struct stat sbuf;
	if ( !exist(fpath) || stat(fpath.c_str(), &sbuf) != 0 || sbuf.st_size/kvlen == 0 ) return 0;
	return sbuf.st_size/kvlen;
}
	
// check if file is open. if no, open and insert to hashmap
int JDFSMgr::open( const AbaxString &fpath, bool force )
{
	// printf("s3492 JDFSMgr::open [%s]  force=%d\n", fpath.c_str(), force );
	AbaxInt ai;
	int fd = -1;
	if ( _map->getValue( fpath, ai ) ) {
		// printf("ss44 ai.value()=%d\n", ai.value() );
		return ai.value();
	}

	if ( force ) {
		fd = jagopen( fpath.c_str(), O_CREAT|O_RDWR|JAG_NOATIME, S_IRWXU);
		// printf("s3838 force fd=%d\n", fd );
	}
	else if ( exist( fpath ) ) {
		fd = jagopen( fpath.c_str(), O_RDWR|JAG_NOATIME, S_IRWXU);
		// printf("s3858 fd=%d\n", fd );
	} else {
		// printf("s9393 -1-1-\n");
	}

	if ( fd < 0 ) {
		// printf("s2394 standard open cannot work\n");
		return -1;
	}

	_map->addKeyValue( fpath, fd );
	return fd;
}

// check if file is open. if yes, close and remove from hashmap
int JDFSMgr::close( const AbaxString &fpath )
{
	AbaxInt ai;
	if ( ! _map->getValue( fpath, ai ) ) {
		return -1;
	}

	int fd = ai.value();
	jagclose( fd );
	_map->removeKey( fpath );
	return 1;
}

int JDFSMgr::getFD( const AbaxString &fpath )
{
	AbaxInt ai;
	if ( ! _map->getValue( fpath, ai ) ) {
		return -1;
	}

	return ai.value();
}

// rename a file
int JDFSMgr::rename( const AbaxString &fpath, const AbaxString &newfpath )
{
	close( fpath );
	jagrename( fpath.c_str(), newfpath.c_str() );
	open ( newfpath );
	return 1;
}

// remove a file
int JDFSMgr::remove( const AbaxString &fpath )
{
	close( fpath );
	jagunlink( fpath.c_str() );
	return 1;
}

// make fullpath dir
int JDFSMgr::mkdir( const AbaxString &fpath )
{
	JagFileMgr::makedirPath( fpath.c_str() );
	return 1;
}

// delete fullpath dir
int JDFSMgr::rmdir( const AbaxString &fpath )
{
	JagFileMgr::rmdir( fpath.c_str() );
	return 1;
}

abaxint JDFSMgr::pread( int fd, void *buf, size_t count, abaxint offset)
{
	// return ::pread( fd, buf, count, offset );
	return raysafepread( fd, (char*)buf, count, offset );
}

abaxint JDFSMgr::pwrite(int fd, const void *buf, size_t count, abaxint offset)
{
	// return ::pwrite( fd, buf, count, offset );
	return raysafepwrite( fd, (const char*)buf, count, offset );
}

