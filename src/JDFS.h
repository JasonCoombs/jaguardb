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
#ifndef _JDFS_H_
#define _JDFS_H_

#include <abax.h>

class JagDBServer;
class JDFSMgr;

class JDFS
{
	public:
		JDFS( JagDBServer *servobj, const AbaxDataString &fpath, int kvlen, abaxint arrlen=0 );
		~JDFS();
		int getFD();
		abaxint getFileSize() const;
		int exist() const;
		int open();
		int close();
		int rename( const AbaxDataString &newpath );
		int remove();
		int fallocate( abaxint offset, abaxint len );
		int setStripeSize( size_t ss ) { _stripeSize = ss; }
		time_t getfileUpdateTime() const;
		abaxint getArrayLength() const;
		abaxint pread( char *buf, size_t len, size_t offset ) const; 
		abaxint pwrite( const char *buf, size_t len, size_t offset ); 
		AbaxDataString  _fpath;

  protected:
  		int          _kvlen;  // KEYVAL length
		abaxint      _stripeSize;
		JagDBServer  *_servobj;
		JDFSMgr      *_jdfsMgr;
};

#endif
