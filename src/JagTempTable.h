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
#ifndef _jag_temp_table_h_

class JagTempTable
{
	public:

		JagTempTable();
		~JagTempTable();
		bool openWrite( const char *fpath );
		bool addRecord( const char *rec, int reclen );

		bool openRead( const char *fpath );
		bool openWrite( const char *fpath );
		int  getFD() { return _fd; }

	protected:
		int _fd;
};

JagTempTable::JagTempTable()
{
	_fd = -1;
}

JagTempTable::~JagTempTable()
{
	if ( _fd > 0 ) {
		close( _fd );
	}
}

bool JagTempTable::openRead( const char *fpath )
{
	_fd = open( fpath, O_RDONLY );
}

bool JagTempTable::openWrite( const char *fpath )
{
	_fd = open( fpath, O_CREAT|O_RDWR, S_IRWXU);
}

bool JagTempTable::addRecord( const char *rec, int reclen )
{
	char c = '\0';
	::write( _fd, rec, reclen );
	::write( _fd, &c, 1 );
	return true;
}


#endif
