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

#include <JagMergeReaderBase.h>
#include <JagUtil.h>

///////////////// implementation /////////////////////////////////

JagMergeReaderBase::JagMergeReaderBase( int veclen, int keylen, int vallen )
{
	_setRestartPos = 0;
	_endcnt = 0;
	_veclen = veclen;
	KEYLEN = keylen;
	VALLEN = vallen;
	KEYVALLEN = keylen + vallen;
	
	_goNext =(int*)calloc(_veclen, sizeof(int));
	_buf = (char*)jagmalloc(KEYVALLEN*_veclen+1);
	_cacheBuf = NULL;

}

JagMergeReaderBase::~JagMergeReaderBase()
{
	if ( _goNext ) {
		free ( _goNext );
	}
	_goNext = NULL;
	
	if ( _buf ) {
		free ( _buf );
	}
	_buf = NULL;
	
}

void JagMergeReaderBase::putBack( const char *buf )
{
	if ( _cacheBuf ) {
		free( _cacheBuf );
	}

	_cacheBuf = (char*)jagmalloc(KEYVALLEN + 1 );
	memcpy( _cacheBuf, buf, KEYVALLEN );
}
