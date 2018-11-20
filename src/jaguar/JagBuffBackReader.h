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
#ifndef _jag_buff_back_reader_h_
#define _jag_buff_back_reader_h_

#include <abax.h>
#include <JagDef.h>

class JagDiskArrayBase;

class JagBuffBackReader
{

  public:
	JagBuffBackReader( const JagDiskArrayBase *darr, abaxint readlen, abaxint keylen, abaxint vallen, 
		abaxint end=-1, abaxint headoffset=JAG_ARJAG_FILE_HEAD, abaxint bufferSize=64 );
  	~JagBuffBackReader();
	
  	bool getNext( char *buf );
  	bool getNext( char *buf, abaxint &i );
	bool getNext( char *buf, abaxint len, abaxint &i );
	bool setRestartPos();
	bool getRestartPos();
	void setClearRestartPosFlag();

  protected:
	bool findNonblankElement( char *buf, abaxint &i );
	abaxint getNumBlocks( abaxint kvlen, abaxint bufferSize );
	
	bool _dolock;
	bool  _readAll;
	bool _setRestartPos;
	char *_superbuf;
	abaxint	KEYLEN;
	abaxint VALLEN;
	abaxint KEYVALLEN;
	abaxint _elements;
	abaxint _lastSuperBlock;
	abaxint _relpos;
	abaxint _stlastSuperBlock;
	abaxint _strelpos;
	abaxint _headoffset;
	abaxint _end;
	abaxint _readlen;
	abaxint _numResize;
	abaxint _curBlockElements;
	const JagDiskArrayBase *_darr;	
};

#endif
