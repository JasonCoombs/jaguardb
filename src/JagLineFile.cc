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
#include <JagLineFile.h>
#include <JagUtil.h>

JagLineFile::JagLineFile( int bufline )
{
	_bufMax = bufline;
	_buf = new Jstr[bufline];
	_bufLen = 0;  // number of lines in buffer
	_fileLen = 0;
	_fp = NULL;
	_hasStartedRead = false;
}

JagLineFile::~JagLineFile()
{
	if ( _buf ) {
		delete [] _buf;
	}

	if ( _fp ) {
		fclose( _fp );
		jagunlink( _fname.c_str() );
	}
}

bool JagLineFile::print() const
{
	prt(("JagLineFile::print() this=%0x\n", this ));
	for ( int i = 0; i < _bufLen; ++i ) {
		prt(("  i=%03d line=[%s]\n", i, _buf[i].c_str() ));
	}
	return true;
}

void JagLineFile::append( const Jstr &line )
{
	if ( _hash.keyExist( line ) ) return;
	//prt(("s3890 JagLineFile::append(%s)\n", line.c_str() ));
	if ( _bufLen < _bufMax ) {
		_buf[ _bufLen ] = line;
		++ _bufLen;
		//prt(("s3710 JagLineFile::append() _bufLen=%d\n", _bufLen ));
		_hash.addKeyValue( line, "1");
		return;
	}

	if ( ! _fp ) {
		_fname = jaguarHome() + "/tmp/" + longToStr(THREADID) + intToStr( rand()%10000 );
		_fp = fopen( _fname.c_str(), "w" );
		if ( ! _fp ) {
			return;
		}
	}

	fprintf(_fp, "%s\n", line.c_str() );
	++ _fileLen;
	_hash.addKeyValue( line, "1");
	return;
}

jagint JagLineFile::size() const
{
	return _bufLen + _fileLen;
}

void JagLineFile::startRead()
{
	if ( _hasStartedRead ) return;

	if ( _fp ) {
		rewind( _fp );
	}
	_i = 0;
	_hasStartedRead = true;
}

bool JagLineFile::getLine( Jstr &line )
{
	//prt(("s3820 getLine _bufLen=%d _i=%d\n", _bufLen, _i ));
	if ( _i <= _bufLen-1 ) {
		line = _buf[_i];
		++ _i;
		//prt(("s3823 getLine _bufLen=%d _i=%d\n", _bufLen, _i ));
		//jaguar_mutex_unlock ( &_mutex );
		return true;
	}

	if ( ! _fp ) {
		return false;
	}

	char *cmdline = NULL;
	size_t sz;
	if ( jaggetline( (char**)&cmdline, &sz, _fp ) < 0 ) {
		return false;
	}

	stripStrEnd( cmdline, sz );
	line = cmdline;
	free( cmdline );
	//prt(("s5581 line=[%s]\n", line.c_str() ));
	return true;
}

bool JagLineFile::hasData()
{
	//prt(("s3388 JagLineFile::hasData() _i=%d _bufLen-1=%d\n", _i, _bufLen-1 ));
	if ( _i <= _bufLen-1 ) {
		return true;
	}

	if ( ! _fp ) {
		return false;
	}

	if ( feof( _fp ) ) {
		return false;
	}
	return true;
}

JagLineFile& JagLineFile::operator+= ( JagLineFile &f2 )
{
	f2.startRead();
	Jstr line;
	while ( f2.getLine( line ) ) {
		append( line );
	}
	return *this;
}


