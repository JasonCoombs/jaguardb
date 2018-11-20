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

#include "JagIndexSchema.h"
#include "JagDBServer.h"

JagIndexSchema::JagIndexSchema( JagDBServer *serv, int replicateType )
  :JagSchema( )
{
	this->init( serv, "INDEX", replicateType );
}

JagIndexSchema::~JagIndexSchema()
{
}

int JagIndexSchema::getIndexNames( const AbaxDataString &dbname, const AbaxDataString &tabname, 
									JagVector<AbaxDataString> &vec )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	bool rc;
	abaxint getpos;
	char *buf = (char*) jagmalloc ( KVLEN+1 );
	memset( buf, '\0', KVLEN+1 );
	char *keybuf = (char*) jagmalloc ( KEYLEN+1 );
	abaxint length = _schema->_garrlen;
	JagBuffReader nti( _schema, length, KEYLEN, VALLEN, 0, 0 );
	JagColumn onecolrec;
	AbaxDataString  ks;

	int cnt = 0;
	while ( true ) {
		rc = nti.getNext(buf, KVLEN, getpos);
		if ( !rc ) { break; }
		memset( keybuf, '\0', KEYLEN+1 );
		memcpy( keybuf, buf, KEYLEN );
		ks = buf;
		JagStrSplit split( ks, '.' );
		if ( split.length() < 3 ) { continue; }
		if ( dbname == split[0] && tabname == split[1] ) {
			vec.append( split[2] );
			++ cnt;
		}
	}

	if ( buf ) free ( buf );
	if ( keybuf ) free ( keybuf );

	return cnt;
}

// use memory to read instead of disk, _recordMap, to get all index names under one table
int JagIndexSchema::getIndexNamesFromMem( const AbaxDataString &dbname, const AbaxDataString &tabname, 
											JagVector<AbaxDataString> &vec )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );

	AbaxDataString dbtab = dbname + "." + tabname + ".";
	abaxint hdrlen = dbtab.size(), len = _recordMap->arrayLength();
    const AbaxPair<AbaxString, AbaxString> *arr = _recordMap->array();
    for ( abaxint i = 0; i < len; ++i ) {
    	if ( _recordMap->isNull(i) ) continue;
		if ( memcmp(arr[i].key.c_str(), dbtab.c_str(), hdrlen) != 0 ) { continue; }
		vec.append( AbaxDataString(arr[i].key.c_str()+hdrlen) );
    }
	return 1;
}

// check if the table exists already from index schema file
bool JagIndexSchema::tableExist( const AbaxDataString &dbname, JagParseParam *parseParam )
{
	AbaxDataString tab = parseParam->objectVec[0].tableName;
	return tableExist( dbname, tab );
}

bool JagIndexSchema::tableExist( const AbaxDataString &dbname, const AbaxDataString &tab )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	bool found = false;
	abaxint totlen = _schema->_garrlen;
	char  save;
    abaxint getpos;
	int rc;

	char *buf = (char*) jagmalloc ( KVLEN+1 );
	char *keybuflow = (char*) jagmalloc ( KEYLEN+1 );
    memset( buf, '\0', KVLEN+1 );
    memset( keybuflow, 0, KEYLEN+1 );
	memcpy(keybuflow, dbname.c_str(), dbname.size());
	*(keybuflow+dbname.size()) = '.';

	JagBuffReader nti( _schema, totlen, KEYLEN, VALLEN, 0, 0 );
	while ( nti.getNext(buf, KVLEN, getpos) ) {
		rc = memcmp(buf, keybuflow, dbname.size()+1);
		if ( rc > 0 ) {
			break;
		}

		save = buf[KEYLEN];
		buf[KEYLEN] = '\0';
		JagStrSplit oneSplit( buf, '.' );
		buf[KEYLEN] = save;
		if ( oneSplit.length() < 3 ) { continue; }

		if ( 0 == rc && oneSplit[1] == tab ) {
			found = true;
			break;
		}
	}

	if ( buf ) free ( buf );
	if ( keybuflow ) free ( keybuflow );

	return found;
}

// check if the index exists already from index schema file
bool JagIndexSchema::indexExist( const AbaxDataString &dbname, JagParseParam *parseParam )
{
	AbaxDataString idx  = parseParam->objectVec[0].indexName;
	return indexExist( dbname, idx );
}

bool JagIndexSchema::indexExist( const AbaxDataString &dbname, const AbaxDataString &idx )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	abaxint totlen = _schema->_garrlen;
	bool found = false;
	int rc;
	char  save;
    abaxint getpos;

	char *buf = (char*) jagmalloc ( KVLEN+1 );
	char *keybuflow = (char*) jagmalloc ( KEYLEN+1 );
    memset( buf, '\0', KVLEN+1 );
    memset( keybuflow, 0, KEYLEN+1 );
	memcpy(keybuflow, dbname.c_str(), dbname.size());
	*(keybuflow+dbname.size()) = '.';
	
	JagBuffReader nti( _schema, totlen, KEYLEN, VALLEN, 0, 0 );
	while ( nti.getNext(buf, KVLEN, getpos) ) {
		rc = memcmp(buf, keybuflow, dbname.size()+1);
		if ( rc > 0 ) {
			// no need to read more
			break;
		}

		save = buf[KEYLEN];
		buf[KEYLEN] = '\0';
		JagStrSplit oneSplit( buf, '.' );
		buf[KEYLEN] = save;
		if ( oneSplit.length() < 3 ) { continue; }

		if ( 0 == rc && oneSplit[2] == idx ) {
			found = true;
			break;
		}
	}
	if ( buf ) free ( buf );
	if ( keybuflow ) free ( keybuflow );

	return found;
}

