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

#include <JagBoundFile.h>
#include <JagUtil.h>

// openMode "a" or "r"
JagBoundFile::JagBoundFile( const char *filepath, int bound )
{
	_fpath = filepath;
	_bound = bound;
}

// 0: error  1: OK
int JagBoundFile::openAppend()
{
	_numLines = _getNumLines();
	_fp = jagfopen( _fpath.c_str(), "ab" );
	if ( ! _fp ) return 0;

	return 1;
}

// 0: error  1: OK
int JagBoundFile::openRead()
{
	_numLines = _getNumLines();

	_fp = jagfopen( _fpath.c_str(), "rb" );
	if ( ! _fp ) return 0;
}

int JagBoundFile::_getNumLines()
{
	int num = 0;
	FILE *fp = jagfopen( _fpath.c_str(), "rb" );
	if ( ! fp ) {
		return num;
	}

	char buf[2048];
	while ( NULL != ( fgets( buf, 2048, fp ) ) ) {
		++num;
	}

	jagfclose( fp );
	return num;
}


void JagBoundFile::close()
{
	if ( _fp ) jagfclose( _fp );
	_fp = NULL;
}

JagBoundFile::~JagBoundFile()
{
	if ( _fp ) {
		jagfclose( _fp );
		_fp = NULL;
	}
}

int JagBoundFile::appendLine( const char *line )
{
	if ( ! _fp ) return 0;
	fprintf( _fp, "%s\n", line );
	++ _numLines;

	if ( _numLines > 2 * _bound ) {
		_trimFile();
	}

	return 1;
}

int  JagBoundFile::readLines( int numLines, JagVector<AbaxDataString> &vec )
{
	JagVector<AbaxDataString> allvec;

	FILE *fp = jagfopen( _fpath.c_str(), "rb" );
	if ( ! fp ) {
		return 0;
	}

	char buf[2048];
	AbaxDataString line;
	AbaxDataString pureline;
	while ( NULL != ( fgets( buf, 2048, fp ) ) ) {
		line = buf;
		pureline = trimTailChar( line, '\n' );
		allvec.append( pureline );
	}
	jagfclose( fp );

	int start;
	if ( numLines < allvec.length() ) {
		start = allvec.length() - numLines;
	} else {
		start = 0;
	}

	int n = 0;
	for ( int i = start; i < allvec.length(); ++i ) {
		vec.append( allvec[i] );
		++n;
	}
	return n;
}

int JagBoundFile::_trimFile()
{
	if ( _numLines < 2 * _bound ) {
		return 0;
	}

	jagfclose( _fp );

	// read in data
	JagVector<AbaxDataString> vec;
	int n = readLines( _bound, vec );

	// remove old file
	jagunlink( _fpath.c_str() );

	// write data back
	int rc = openAppend( );
	for ( int i = 0; i < vec.length(); ++i ) {
		appendLine( vec[i].c_str() );
	}

	return 1;
}

