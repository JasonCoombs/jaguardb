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
#ifndef _jag_buff_reader_h_
#define _jag_buff_reader_h_

#include <abax.h>
#include <JagDef.h>

class JagDiskArrayBase;

class JagBuffReader
{

  public:
	JagBuffReader( JagDiskArrayBase *darr, jagint readlen, jagint keylen, jagint vallen, 
		jagint start=0, jagint headoffset=JAG_ARJAG_FILE_HEAD, jagint bufferSize=8 );
  	~JagBuffReader();
	
  	bool getNext( char *buf );
  	bool getNext( char *buf, jagint &i );
	bool getNext( char *buf, jagint len, jagint &i );
	bool setRestartPos();
	bool moveToRestartPos();
	void setClearRestartPosFlag();

  protected:
	bool findNonblankElement( char *buf, jagint &i );
	jagint getNumBlocks( jagint kvlen, jagint bufferSize );
	
	bool _dolock;
	bool  _readAll;
	bool _setRestartPos;
	char *_superbuf;
	jagint	KEYLEN;
	jagint VALLEN;
	jagint KEYVALLEN;
	jagint _elements;
	jagint _lastSuperBlock;
	jagint _relpos;
	jagint _stlastSuperBlock;
	jagint _strelpos;
	jagint _headoffset;
	jagint _start;
	jagint _readlen;
	jagint _numResize;
	jagint _curBlockElements;
	JagDiskArrayBase *_darr;	
	jagint  _n;
};

#endif
