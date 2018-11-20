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

#include <JagMergeBackReader.h>
#include <JagBuffBackReader.h>
#include <JagUtil.h>

///////////////// implementation /////////////////////////////////

JagMergeBackReader::JagMergeBackReader( JagVector<OnefileRange> &fRange, int veclen, int keylen, int vallen )
	: JagMergeReaderBase( veclen, keylen, vallen )
{
	_vec = new JagBuffBackReaderPtr[veclen];
	raydebug( stdout, JAG_LOG_HIGH, "s1829 JagBuffBackReader[%d] ...\n", veclen );
	
	for ( int i = 0; i < veclen; ++i ) {
		_vec[i] = new JagBuffBackReader( (JagDiskArrayBase*)fRange[i].darr, fRange[i].readlen, KEYLEN, VALLEN, 
										 fRange[i].startpos, 0, fRange[i].memmax );
		_goNext[i] = 1;
	}
}

JagMergeBackReader::~JagMergeBackReader()
{
	if ( _vec ) {
		for ( int i = 0; i < _veclen; ++i ) {
			if ( _vec[i] ) delete _vec[i];
			_vec[i] = NULL;
		}
		delete [] _vec;
	}
	_vec = NULL;
}

bool JagMergeBackReader::getNext( char *buf )
{
	abaxint pos;
	return getNext( buf, pos );
}

bool JagMergeBackReader::getNext( char *buf, abaxint &pos )
{

	if ( _cacheBuf ) {
		pos = 0;
		memcpy(buf, _cacheBuf, KEYVALLEN);
		free( _cacheBuf ); _cacheBuf = NULL;
		return true;
	}

	int beginpos = 0;
	int rc;
	for ( int i = 0; i < _veclen; ++i ) {
		if ( _goNext[i] == 1 ) {
			rc = _vec[i]->getNext( _buf+i*KEYVALLEN, pos );
			if ( !rc ) {
				_goNext[i] = -1;
				++_endcnt;
			} else {
				_goNext[i] = 0;
			}
		}
	}
	
	if ( _endcnt == _veclen ) {
		return false;
	}
	
	// get initial maxbuf as first avail buf
	for ( int i = beginpos; i < _veclen; ++i ) {
		if ( _goNext[i] != -1 ) {
			memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
			beginpos = i;
			break;
		}
	}
	
	// find the max buf ( while oldest file first )
	for ( int i = beginpos; i < _veclen; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp(buf, _buf+i*KEYVALLEN, KEYLEN);
			if ( rc <= 0 ) {
				memcpy(buf, _buf+i*KEYVALLEN, KEYVALLEN);
			}
		}
	}
	
	// check and set all other equal buf to getNext
	for ( int i = beginpos; i < _veclen; ++i ) {
		if ( _goNext[i] != -1 ) {
			rc = memcmp(buf, _buf+i*KEYVALLEN, KEYLEN);
			if ( rc <= 0 ) {			
				if ( _goNext[i] != -1 ) {
					_goNext[i] = 1;
				}
			}
		}
	}
	return true;
}

// method to setup restart position for each buffreader ( as current pos when calling this method )
bool JagMergeBackReader::setRestartPos()
{
	if ( _setRestartPos ) return false;
	for ( int i = 0; i < _veclen; ++i ) {
		_vec[i]->setRestartPos();
	}
	_setRestartPos = 1;	
	return true;
}

// method to reset pos to restart position set by the above method 
bool JagMergeBackReader::getRestartPos()
{
	if ( !_setRestartPos ) return false;
	for ( int i = 0; i < _veclen; ++i ) {
		_vec[i]->getRestartPos();
		if ( _goNext[i] < 0 ) --_endcnt;
		_goNext[i] = 1;
	}
	_setRestartPos = 0;
	return true;
}

void JagMergeBackReader::setClearRestartPosFlag() 
{ 
	_setRestartPos = 0; 
	for ( int i = 0; i < _veclen; ++i ) {
		_vec[i]->setClearRestartPosFlag();
	}
}
