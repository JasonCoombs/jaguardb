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
#ifndef _jag_single_buff_reader_h_
#define _jag_single_buff_reader_h_

#include <abax.h>

class JagSingleBuffReader
{

  public:
  	//                           readlen=# of kv
	JagSingleBuffReader( int fd, abaxint readlen, int keylen, int vallen, abaxint start=0, abaxint headoffset=0, abaxint bufferSize=32 );
  	~JagSingleBuffReader( ); 
	
  	bool getNext ( char *buf, int len, abaxint &i );
  	bool getNext ( char *buf );
  	bool getNext ( char *buf, abaxint &i );
		
  protected:

	bool findNonblankElement( char *buf, abaxint &i );
	abaxint getNumBlocks( int kvlen, abaxint bufferSize );

  	abaxint _elements;
  	abaxint _headoffset;
	int  _fd;
	abaxint _start;
	abaxint _readlen;
	char  *_superbuf;
	abaxint	  KEYLEN;
	abaxint   VALLEN;
	abaxint   KEYVALLEN;

	int   _lastSuperBlock;
	abaxint   _relpos;
};

#endif
