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
/***************************************************
** Used as a fixed-size key-value store
**  key:  AbaxDataString   (max 256 bytes)
**  value: AbaxDataString  (max 4096 bytes)
**
** This class uses JagDiskArrayServer.h for storage
**
***************************************************/

#ifndef _jag_fixkv_h_
#define _jag_fixkv_h_

#include <JagDiskArrayServer.h>
#include <JagRecord.h>
#include <JagMutex.h>


class JagDBServer;

//////// userid -->  [JagRecord:   PASS for password ROLE: ADMIN/USER   PERM: role READ/WRITE ]
class JagFixKV
{
  public:
    JagFixKV( JagDBServer *servobj, const AbaxDataString &dbname, const AbaxDataString & tabname, int replicateType );
	virtual ~JagFixKV();
	virtual void destroy( bool removelock=true );
	virtual void init( );
	virtual void refresh();

	AbaxString      getValue( const AbaxString &key, const AbaxString& name ) const;
	bool  	        setValue( const AbaxString &key,  const AbaxString& name, const AbaxString& value );
	bool 	        dropKey( const AbaxString &key, bool doLock=true ); 
	AbaxDataString  getListKeys();

  protected:
	JagHashMap<AbaxString, AbaxString> 	*_hashmap;
    JagReadWriteLock  					*_lock;
	JagDBServer       					*_servobj;
	JagDiskArrayServer  				*_darr;
	int									_replicateType;
	abaxint								KLEN, VLEN, KVLEN;
	AbaxDataString  					_dbname;
	AbaxDataString  					_tabname;
	JagSchemaRecord   					_onerecord;
};

#endif
