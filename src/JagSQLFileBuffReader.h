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
#ifndef _jag_sql_file_buff_reader_h_
#define _jag_sql_file_buff_reader_h_

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <JagUtil.h>

class JagSQLFileBuffReader
{
  public:
  	JagSQLFileBuffReader( const Jstr &fpath );
  	~JagSQLFileBuffReader( ); 
  	bool getNextSQL ( Jstr &cmd );

  protected:
	bool readNextBlock();

    static const int NB = 100000;
	Jstr _cmd[NB];
	jagint    _cmdlen;
	jagint    _cursor;
	FILE  *_fp;
};

#endif

