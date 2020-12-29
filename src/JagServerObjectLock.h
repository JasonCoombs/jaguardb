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
#ifndef _jag_server_object_lock_
#define _jag_server_object_lock_

#include <abax.h>

class JagDBServer;
class JagHashLock;
class JagTable;
class JagIndex;
class JagTableSchema;
class JagIndexSchema;

template <class K, class V> class JagHashMap;

class JagServerObjectLock
{
  public:
  	JagServerObjectLock( const JagDBServer *servobj );
	~JagServerObjectLock();

	void rebuildObjects();
	void setInitDatabases( const Jstr &dblist, int replicateType );
	jagint getnumObjects( int objType, int replicateType );

	int readLockSchema( int replicateType );
	int readUnlockSchema( int replicateType );
	int writeLockSchema( int replicateType );
	int writeUnlockSchema( int replicateType );
	int readLockDatabase( jagint opcode, const Jstr &dbName, int replicateType );
	int readUnlockDatabase( jagint opcode, const Jstr &dbName, int replicateType );
	int writeLockDatabase( jagint opcode, const Jstr &dbName, int replicateType );
	int writeUnlockDatabase( jagint opcode, const Jstr &dbName, int replicateType );

	JagTable *readLockTable( jagint opcode, const Jstr &db, const Jstr &table, int repType, bool lockSelfLevel=0 );
	int readUnlockTable( jagint opcode, const Jstr &dbName, const Jstr &table, int repType, bool lockSelfLevel=0 );

	JagTable *writeLockTable( jagint opcode, const Jstr &db, const Jstr &table, const JagTableSchema *tschema, int repType, bool lockSelfLevel=0 );
	int writeUnlockTable( jagint opcode, const Jstr &db, const Jstr &table, int repType, bool lockSelfLevel=0 );

	JagTable *writeTruncateTable( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
		const JagTableSchema *tschema, int replicateType, bool lockSelfLevel=0 );

	JagIndex *readLockIndex( jagint opcode, const Jstr &dbName, Jstr &tableName, const Jstr &indexName, int replicateType, bool lockSelfLevel=0 );
	int readUnlockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, const Jstr &indexName, int replicateType, bool lockSelfLevel=0 );

	JagIndex *writeLockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, const Jstr &indexName,
							  const JagTableSchema *tschema, const JagIndexSchema *ischema, int replicateType, bool lockSelfLevel=0 );
	int writeUnlockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, const Jstr &indexName,
						  int replicateType, bool lockSelfLevel=0 );
		
	Jstr getAllTableNames( int replicateType );

	
  protected:
	const JagDBServer					*_servobj;
	JagHashLock							*_hashLock;
	JagHashMap<AbaxString, AbaxBuffer>	*_databases;
	JagHashMap<AbaxString, AbaxBuffer>	*_prevdatabases;
	JagHashMap<AbaxString, AbaxBuffer>	*_nextdatabases;	
	JagHashMap<AbaxString, AbaxBuffer>	*_tables;
	JagHashMap<AbaxString, AbaxBuffer>	*_prevtables;
	JagHashMap<AbaxString, AbaxBuffer>	*_nexttables;
	JagHashMap<AbaxString, AbaxBuffer>	*_indexs;
	JagHashMap<AbaxString, AbaxBuffer>	*_previndexs;
	JagHashMap<AbaxString, AbaxBuffer>	*_nextindexs;
	JagHashMap<AbaxString, AbaxString>	*_idxtableNames;

	void initObjects();
	void cleanupObjects();
};

#endif
