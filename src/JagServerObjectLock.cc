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
#include <JagServerObjectLock.h>
#include <JagTableSchema.h>
#include <JagIndexSchema.h>
#include <JagTable.h>
#include <JagIndex.h>
#include <JagHashMap.h>
#include <JagHashLock.h>
#include <JagDBServer.h>

JagServerObjectLock::JagServerObjectLock( const JagDBServer *servobj )
: _servobj ( servobj )
{
	//_hashLock = new JagHashLock();
	_hashLock = newObject<JagHashLock>();
	initObjects();
}

JagServerObjectLock::~JagServerObjectLock()
{
	cleanupObjects();
	if ( _hashLock ) delete _hashLock;	
}

// method to init all data memebers except hashlock
void JagServerObjectLock::initObjects()
{
	_databases = new JagHashMap<AbaxString, AbaxBuffer>();
	_prevdatabases = new JagHashMap<AbaxString, AbaxBuffer>();
	_nextdatabases = new JagHashMap<AbaxString, AbaxBuffer>();
	_tables = new JagHashMap<AbaxString, AbaxBuffer>();
	_prevtables = new JagHashMap<AbaxString, AbaxBuffer>();
	_nexttables = new JagHashMap<AbaxString, AbaxBuffer>();
	_indexs = new JagHashMap<AbaxString, AbaxBuffer>();
	_previndexs = new JagHashMap<AbaxString, AbaxBuffer>();
	_nextindexs = new JagHashMap<AbaxString, AbaxBuffer>();
	_idxtableNames = new JagHashMap<AbaxString, AbaxString>();

	// during init, need to add "system" and "test" default databases into dbmap
	AbaxBuffer bfr; AbaxDataString dbName = "system", dbName2 = "test";
	_databases->addKeyValue( dbName, bfr ); bool rc = _databases->addKeyValue( dbName2, bfr );
	_prevdatabases->addKeyValue( dbName, bfr ); _prevdatabases->addKeyValue( dbName2, bfr );
	_nextdatabases->addKeyValue( dbName, bfr ); _nextdatabases->addKeyValue( dbName2, bfr );
}

// method to set init databases into hashmap, dblist is dbnames separate '|'
void JagServerObjectLock::setInitDatabases( const AbaxDataString &dblist, int replicateType )
{
	AbaxBuffer bfr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
	}
	JagStrSplit sp( dblist.c_str(), '|', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		dbmap->addKeyValue( sp[i], bfr );
	}
}

// method to cleanup all data members except hashlock
void JagServerObjectLock::cleanupObjects()
{
	const AbaxPair<AbaxString, AbaxBuffer> *arr; abaxint len, i;
	
	// clean up _tables, _prevtables, _nexttables, _indexs, _previndexs, _nextindexs
	arr = _tables->array(); len = _tables->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagTable *p = (JagTable*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}

	arr = _prevtables->array(); len = _prevtables->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagTable *p = (JagTable*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}
	
	arr = _nexttables->array(); len = _nexttables->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagTable *p = (JagTable*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}
	
	arr = _indexs->array(); len = _indexs->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagIndex *p = (JagIndex*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}
	
	arr = _previndexs->array(); len = _previndexs->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagIndex *p = (JagIndex*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}
	
	arr = _nextindexs->array(); len = _nextindexs->arrayLength();
	for ( i = 0; i < len; ++i ) {
		if ( arr[i].key.size() > 0 ) {
			JagIndex *p = (JagIndex*) arr[i].value.addr();
			if ( p ) delete p;
		}
	}

	if ( _idxtableNames ) delete _idxtableNames;
	if ( _indexs ) delete _indexs;
	if ( _previndexs ) delete _previndexs;
	if ( _nextindexs ) delete _nextindexs;	
	if ( _tables ) delete _tables;
	if ( _prevtables ) delete _prevtables;
	if ( _nexttables ) delete _nexttables;
	if ( _databases ) delete _databases;
	if ( _prevdatabases ) delete _prevdatabases;
	if ( _nextdatabases ) delete _nextdatabases;
}

// method to rebuild all objects except hashlock
void JagServerObjectLock::rebuildObjects()
{
	cleanupObjects();
	initObjects();
}

// method to get num of objects in hashmap
// objType 0: database; 1: tables; 2: indexs;
abaxint JagServerObjectLock::getnumObjects( int objType, int replicateType )
{
	if ( 0 == objType ) {
		if ( 0 == replicateType ) {
			return _databases->size();
		} else if ( 1 == replicateType ) {
			return _prevdatabases->size();
		} else if ( 2 == replicateType ) {
			return _nextdatabases->size();
		}
	} else if ( 1 == objType ) {
		if ( 0 == replicateType ) {
			return _tables->size();
		} else if ( 1 == replicateType ) {
			return _prevtables->size();
		} else if ( 2 == replicateType ) {
			return _nexttables->size();
		}
	} else if ( 2 == objType ) {
		if ( 0 == replicateType ) {
			return _indexs->size();
		} else if ( 1 == replicateType ) {
			return _previndexs->size();
		} else if ( 2 == replicateType ) {
			return _nextindexs->size();
		}
	}
	return 0;
}

// methods to lock/unlock schema for read only( seldom use )
// always return 1
// replicateType -1: lock all; otherwise 0-2, lock one part ( data, pdata, ndata )
int JagServerObjectLock::readLockSchema( int replicateType )
{
	AbaxDataString repstr;
	if ( replicateType < 0 ) {
		repstr = "0";
 		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		repstr = "1";
 		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		repstr = "2";
 		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	} else {
		repstr = intToStr( replicateType );
 		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	}
	return 1;
}
int JagServerObjectLock::readUnlockSchema( int replicateType )
{
	AbaxDataString repstr;
	if ( replicateType < 0 ) {
		repstr = "0";
 		_hashLock->readUnlock( repstr );
		repstr = "1";
 		_hashLock->readUnlock( repstr );
		repstr = "2";
		_hashLock->readUnlock( repstr );
	} else {
		repstr = intToStr( replicateType );
 		_hashLock->readUnlock( repstr );
	}
	return 1;
}

// methods to lock/unlock schema for write only( seldom use )
// always return 1
int JagServerObjectLock::writeLockSchema( int replicateType )
{
	AbaxDataString repstr;
	if ( replicateType < 0 ) {
		repstr = "0";
 		JAG_BLURT _hashLock->writeLock( repstr ); JAG_OVER;
		repstr = "1";
 		JAG_BLURT _hashLock->writeLock( repstr ); JAG_OVER;
		repstr = "2";
 		JAG_BLURT _hashLock->writeLock( repstr ); JAG_OVER;
	} else {
		repstr = intToStr( replicateType );
 		JAG_BLURT _hashLock->writeLock( repstr ); JAG_OVER;
	}
	return 1;
}
int JagServerObjectLock::writeUnlockSchema( int replicateType )
{
	AbaxDataString repstr;
	if ( replicateType < 0 ) {
		repstr = "0";
 		_hashLock->writeUnlock( repstr );
		repstr = "1";
 		_hashLock->writeUnlock( repstr );
		repstr = "2";
		_hashLock->writeUnlock( repstr );
	} else {
		repstr = intToStr( replicateType );
 		_hashLock->writeUnlock( repstr );
	}
	return 1;
}

// methods to lock/unlock schema and database for read only( use db etc. )
// return 0 error: 
// 	database does not exist
// return 1: success
int JagServerObjectLock::readLockDatabase( abaxint opcode, const AbaxDataString &dbName, int replicateType )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
	}
	
	if ( !dbmap->keyExist( dbName ) ) return 0; // first check before lock
	JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	if ( !dbmap->keyExist( dbName ) ) {
		// second check after lock, if not exist, unlock and return 0
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
		return 0;
	}
	return 1;
}
int JagServerObjectLock::readUnlockDatabase( abaxint opcode, const AbaxDataString &dbName, int replicateType )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	_hashLock->readUnlock( lockdb );
	_hashLock->readUnlock( repstr );
	return 1;
}

// methods to lock/unlock schema and database for write only( createdb/dropdb etc. )
// return 0 error: 
// 	database already exists( createdb ) or database does not exist( other cmds dropdb etc. )
// return 1 success
int JagServerObjectLock::writeLockDatabase( abaxint opcode, const AbaxDataString &dbName, int replicateType )
{
	int rc;
	AbaxBuffer bfr;
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
	}
	
	if ( JAG_CREATEDB_OP == opcode ) {
		// for createdb, if already exists, return 0	
		if ( dbmap->keyExist( dbName ) ) {
			return 0;
		}
	} else {
		// for dropdb and other cmds, if not exists, return 0
		if ( !dbmap->keyExist( dbName ) ) {
			//prt(("s2920 !dbmap->keyExist( dbName ) return 0\n" ));
			return 0;
		}
	}

	// prt(("s2039  inside writeLockDatabase lockdb=%s ...\n",  lockdb.c_str() ));
	JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	JAG_BLURT _hashLock->writeLock( lockdb ); JAG_OVER;
	// prt(("s2039  inside writeLockDatabase lockdb=%s done\n",  lockdb.c_str() ));
	if ( JAG_CREATEDB_OP == opcode ) {
		// for createdb, second check after lock, if already exists, unlock and return 0
		// otherwise, add database to list
		if ( dbmap->keyExist( dbName ) ) {
			_hashLock->writeUnlock( lockdb );
			_hashLock->readUnlock( repstr );
			return 0;
		} else {
			if ( !dbmap->addKeyValue( dbName, bfr ) ) {
				_hashLock->writeUnlock( lockdb );
				_hashLock->readUnlock( repstr );
				return 0;
			}
		}
	} else {
		// for dropdb and other cmds, second check after lock, if not exists, unlock and return 0
		if ( !dbmap->keyExist( dbName ) ) {
			_hashLock->writeUnlock( lockdb );
			_hashLock->readUnlock( repstr );
			//prt(("s2930 error\n" ));
			return 0;
		} else {
			// dropdb clean dblist during unlock
		}
	}
	return 1;
}
int JagServerObjectLock::writeUnlockDatabase( abaxint opcode, const AbaxDataString &dbName, int replicateType )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
	}
	
	if ( JAG_DROPDB_OP == opcode ) {
		// prt(("s2039  inside writeUNLockDatabase unlockdb=%s\n",  lockdb.c_str() ));
		if ( !dbmap->removeKey( dbName ) ) {
			_hashLock->writeUnlock( lockdb );
			_hashLock->readUnlock( repstr );
			return 0;
		} else {
			_hashLock->writeUnlock( lockdb );
			_hashLock->readUnlock( repstr );	
		}
	} else {
		_hashLock->writeUnlock( lockdb );
		_hashLock->readUnlock( repstr );
	}
	return 1;	
}

// methods to lock/unlock schema, database and table( ptab ) for read only( insert/update/delete/select from table etc. )
// return 0/NULL error: 
// 	database or table does not exist
// return 1/ptab success
// pay attention, if opcode is INSERT/UPDATE/DELETE, writeLock IUD to make sure at each time, only one of insert/update/delete
// command can be done
JagTable *JagServerObjectLock::readLockTable( abaxint opcode, const AbaxDataString &dbName, 
	const AbaxDataString &tableName, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	// AbaxString locktabIUD = locktab + "__IUD";
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL;
	// bool isIUD = 0;
	// if ( JAG_INSERT_OP == opcode || JAG_CINSERT_OP == opcode || JAG_DINSERT_OP == opcode || 
	// 	 JAG_UPDATE_OP == opcode || JAG_DELETE_OP == opcode ) isIUD = 1;
	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
	}

	// prt(("s2039 lockd[%s] dbtab=[%s] locktab=[%s] replicateType=[%d]\n", lockdb.c_str(), dbtab.c_str(), locktab.c_str(), replicateType ));
	
	if ( !dbmap->keyExist( dbName ) ) {
		// prt(("s2238 NULL dbName=[%s] replicateType=%d\n", dbName.c_str(), replicateType ));
		return NULL; // first check before lock
	}

	if ( !tabmap->keyExist( dbtab ) ) {
		// prt(("s2239 %s NULL\n", dbtab.c_str() ));
		return NULL; // first check before lock
	}
	// prt(("s2249 %s not NULL\n", dbtab.c_str() ));

	if ( !lockSelfLevel ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	}
	JAG_BLURT _hashLock->readLock( locktab ); JAG_OVER;
	// if ( isIUD ) {
		// JAG_BLURT _hashLock->writeLock( locktabIUD ); JAG_OVER;
	// }
	if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ) {
		// second check after lock, if not exist, unlock and return NULL
		// if ( isIUD ) {
			// _hashLock->writeUnlock( locktabIUD );
		// }
		_hashLock->readUnlock( locktab );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		// prt(("s2009 %s NULL\n", locktab.c_str() ));
		return NULL;
	}
	// else, return ptab
	AbaxBuffer bfr;
	if ( !tabmap->getValue( dbtab, bfr ) ) {
		// not get ptab, unlock and return NULL
		// if ( isIUD ) {
			// _hashLock->writeUnlock( locktabIUD );
		// }
		_hashLock->readUnlock( locktab );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		// prt(("s2309 NULL %s\n", dbtab.c_str() ));
		return NULL;
	}

	JagTable *ptab = (JagTable*) bfr.addr();
	//prt(("s2239 readLockTable ptab=%0x replicateType=%d\n", ptab, replicateType  ));
	return ptab;
}
int JagServerObjectLock::readUnlockTable( abaxint opcode, const AbaxDataString &dbName, 
	const AbaxDataString &tableName, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	// AbaxString locktabIUD = locktab + "__IUD";
	// bool isIUD = 0;
	// if ( JAG_INSERT_OP == opcode || JAG_CINSERT_OP == opcode || JAG_DINSERT_OP == opcode || 
		//  JAG_UPDATE_OP == opcode || JAG_DELETE_OP == opcode ) isIUD = 1;
	// if ( isIUD ) {
		// _hashLock->writeUnlock( locktabIUD );
	// }
	_hashLock->readUnlock( locktab );
	if ( !lockSelfLevel ) {
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
	}
	return 1;	
}

// methods to lock/unlock schema, database and table( ptab ) for write only( create/drop/truncate/alter table etc. )
// if lockSelfLevel is 1, only lock/unlock table( ptab ) lock; otherwise, lock all from schema
// if drop table, remove ptab from list; else if truncate table, recreate ptab
// ptab should be delete outside of this class, since its already returned in writeLock if available
// return 0/NULL error: 
// 	database does not exist or ( table already exists( create table ) or table does not exist( other cmds drop table etc.) )
// return 1/ptab success
JagTable *JagServerObjectLock::writeLockTable( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, 
	const JagTableSchema *tschema, int replicateType, bool lockSelfLevel )
{
	//prt(("s3051 writeLockTable replicateType=%d\n", replicateType ));
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL;

	/***
	prt(("s1090 writeLockTable dbName=[%s] tableName=[%s] replicateType=%d\n", 
			dbName.c_str(), tableName.c_str(), replicateType ));
	***/


	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
	}
	
	if ( JAG_CREATETABLE_OP == opcode || JAG_CREATECHAIN_OP == opcode || JAG_CREATEMEMTABLE_OP == opcode ) {
		// for create table/memtable, if already exists, return NULL	
		if (  !dbmap->keyExist( dbName ) ) {
			// prt(("s3812 dbmap->keyExist(%s) not TRUE, return NULL\n", dbName.c_str() ));
			return NULL;
		}

		if ( tabmap->keyExist( dbtab ) ) {
			// prt(("s3813 tabmap->keyExist(%s) TRUE, return NULL\n", dbtab.c_str() ));
			return NULL;
		}
	} else {
		// for drop table / truncate table and other cmds, if not exists, return NULL
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ) {
			return NULL;
		}
	}

	if ( !lockSelfLevel ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	}
	JAG_BLURT _hashLock->writeLock( locktab ); JAG_OVER;	
	if ( JAG_CREATETABLE_OP == opcode || JAG_CREATECHAIN_OP == opcode || JAG_CREATEMEMTABLE_OP == opcode ) {
		// for create table/memtable, second check after lock, if already exists, unlock and return NULL
		// otherwise, create new ptab and add to list
		if ( !dbmap->keyExist( dbName ) || (dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab )) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		} else {
			const JagSchemaRecord *record = tschema->getAttr( dbtab.c_str() );
			if ( ! record ) {
				// prt(("s3371 tschema->getAttr(%s) NULL\n", dbtab.c_str() ));
				_hashLock->writeUnlock( locktab );
				if ( !lockSelfLevel ) {
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}
			// prt(("s3372 tschema->getAttr(%s) not NULL\n", dbtab.c_str() ));
			JagTable *ptab = new JagTable( replicateType, _servobj, dbName.c_str(), tableName.c_str(), *record );
			//prt(("s3801 createtable ptab=%0x record=%0x replicateType=%d\n", ptab, record, replicateType ));
			AbaxBuffer bfr = (void*) ptab;
			// prt(("s6373 addKeyValue(%s)\n", dbtab.c_str() ));
			if ( !tabmap->addKeyValue( dbtab, bfr ) ) {
				delete ptab;
				_hashLock->writeUnlock( locktab );
				if ( !lockSelfLevel ) {
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}
			return ptab;
		}
	} else {
		// for dropdb and other cmds, second check after lock, if not exists, unlock and return NULL
		// else, return ptab
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}
		AbaxBuffer bfr;
		if ( !tabmap->getValue( dbtab, bfr ) ) {
			// not get ptab, unlock and return NULL
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}		
		JagTable *ptab = (JagTable*) bfr.addr();
		return ptab;
	}
}

int JagServerObjectLock::writeUnlockTable( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, 
	const JagTableSchema *tschema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
	}
	
	if ( JAG_DROPTABLE_OP == opcode ) {
		if ( !tabmap->removeKey( dbtab ) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 0;
		} else {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}	
			return 1;
		}
	} else {
		_hashLock->writeUnlock( locktab );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return 1;
	}
}
// method to recreate ptab again ( used by truncate table )
JagTable *JagServerObjectLock::writeTruncateTable( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, 
	const JagTableSchema *tschema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
	}
	
	if ( JAG_TRUNCATE_OP == opcode ) {
		const JagSchemaRecord *record = tschema->getAttr( dbtab.c_str() );
		if ( !tabmap->removeKey( dbtab ) || ! record ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}
		JagTable *ptab = new JagTable( replicateType, _servobj, dbName.c_str(), tableName.c_str(), *record );
		AbaxBuffer bfr = (void*) ptab;
		if ( !tabmap->addKeyValue( dbtab, bfr ) ) {
			delete ptab;
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevel ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		} else {
			// if success, not unlock
			return ptab;
		}
	} else {
		_hashLock->writeUnlock( locktab );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
}

// methods to lock/unlock schema, database, table( ptab ) and index( pindex ) for read only( insert/update/delete/select from index etc. )
// for index read commands, if tableName is not given( length < 1 ), need to use _idxtableNames hashmap to get table name of the index
// return 0/NULL error:
// 	tableName is not exist in _idxtableNames or any of database, table or index does not exist
// return 1/pindex success
JagIndex *JagServerObjectLock::readLockIndex( abaxint opcode, const AbaxDataString &dbName, AbaxDataString &tableName, const AbaxDataString &indexName,
	const JagTableSchema *tschema, const JagIndexSchema *ischema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString dbidx = dbName + "." + indexName, dbidxrep = dbidx + "." + repstr, tname;
	if ( tableName.size() < 1 ) {
		if ( !_idxtableNames->getValue( dbidxrep, tname ) ) return NULL;
		tableName = tname.c_str();
	}
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	
	AbaxString lockidx = dbtab + "." + indexName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL, *idxmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
		idxmap = _indexs;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
		idxmap = _previndexs;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
		idxmap = _nextindexs;
	}
	if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || 
		!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) return NULL; // first check before lock
	if ( !lockSelfLevel ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( locktab ); JAG_OVER;
	}
	JAG_BLURT _hashLock->readLock( lockidx ); JAG_OVER;
	if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ||
		!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
		// second check after lock, if not exist, unlock and return NULL
		_hashLock->readUnlock( lockidx );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
	// else, return pindex
	AbaxBuffer bfr;
	if ( !idxmap->getValue( dbidx, bfr ) ) {
		// not get pindex, unlock and return NULL
		_hashLock->readUnlock( lockidx );
		if ( !lockSelfLevel ) {
			_hashLock->readUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
	JagIndex *pindex = (JagIndex*) bfr.addr();
	return pindex;
}
int JagServerObjectLock::readUnlockIndex( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, const AbaxDataString &indexName,
	const JagTableSchema *tschema, const JagIndexSchema *ischema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	AbaxString dbidx = dbName + "." + indexName;
	AbaxString lockidx = dbtab + "." + indexName + "." + repstr;
	_hashLock->readUnlock( lockidx );
	if ( !lockSelfLevel ) {
		_hashLock->readUnlock( locktab );
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
	}
	return 1;	
}	

// methods to lock/unlock schema, database, table( ptab ) and index( pindex ) for write only( create/drop index etc. )
// if lockSelfLevel is 1, only lock/unlock table( ptab ) lock; otherwise, lock all from schema
// for create index, store tableName of index to _idxtableNames for future reference
// for drop index, remove pindex from list and remove also from idxtableNames
// pindex should be delete outside of this class, since its already returned in writeLock if available
// return 0/NULL error:
// 	database or table does not exist, or index already exists ( create index etc. ) or index does not exist ( drop index etc. )
// return 1/pindex success
// also, tableName should exists
JagIndex *JagServerObjectLock::writeLockIndex( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, 
												const AbaxDataString &indexName, const JagTableSchema *tschema, 
												const JagIndexSchema *ischema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString dbidx = dbName + "." + indexName, dbidxrep = dbidx + "." + repstr;
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	AbaxString lockidx = dbtab + "." + indexName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL, *idxmap = NULL;

	/***
	prt(("s1092 writeLockIndex dbName=[%s] tableName=[%s] indexName=[%s] replicateType=%d\n", 
			dbName.c_str(), tableName.c_str(), indexName.c_str(), replicateType ));
			***/

	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
		idxmap = _indexs;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
		idxmap = _previndexs;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
		idxmap = _nextindexs;
	}
	if ( JAG_CREATEINDEX_OP == opcode ) {
		// for create index, if already exists, return NULL	
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || _idxtableNames->keyExist( dbidxrep ) ||
			( dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab ) && idxmap->keyExist( dbidx ) ) ) {
			// prt(("s4532 NULL\n" ));
			return NULL;
		}
	} else {
		// for drop index and other cmds, if not exists, return NULL
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || 
			!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
			return NULL;
		}
	}
	if ( !lockSelfLevel ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
		JAG_BLURT _hashLock->writeLock( locktab ); JAG_OVER;
	}
	JAG_BLURT _hashLock->writeLock( lockidx ); JAG_OVER;
	if ( JAG_CREATEINDEX_OP == opcode ) {
		// for create index, second check after lock, if already exists, unlock and return NULL
		// otherwise, create new pindex and add to list
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || _idxtableNames->keyExist( dbidxrep ) ||
			( dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab ) && idxmap->keyExist( dbidx ) ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevel ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			// prt(("s4532 NULL\n" ));
			return NULL;
		} else {
			AbaxDataString path;
			const JagSchemaRecord *trecord, *irecord;
			irecord = ischema->getOneIndexAttr( dbName, indexName, path );// path is returned
			trecord = tschema->getAttr( path );
			// prt(("s4540 tschema=%0x path=[%s] getAttr got trecord=%0x\n", tschema, path.c_str(), trecord ));
			if ( ! irecord || ! trecord ) {
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevel ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				/***
				prt(("s3601 trecord=%0x irecord=%x dbName=[%s] indexName=[%s] path=[%s] NULL\n",
						trecord, irecord, dbName.c_str(), indexName.c_str(), path.c_str() ));
					***/
				return NULL;
			}
			path += AbaxDataString(".") + indexName;
			JagIndex *pindex = new JagIndex( replicateType, _servobj, path, *trecord, *irecord );
			AbaxBuffer bfr = (void*) pindex;
			if ( !idxmap->addKeyValue( dbidx, bfr ) ) {
				delete pindex;
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevel ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				// prt(("s4511 !idxmap->addKeyValue dbidx=[%s] NULL\n", dbidx.c_str() ));
				return NULL;
			}
			
			// final, make sure tableName is inserted to _idxtableNames
			if ( !_idxtableNames->addKeyValue( dbidxrep, tableName ) ) {
				delete pindex;
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevel ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				// prt(("s5611 addKeyValue dbidxrep=[%s] tableName=[%s] NULL\n", dbidxrep.c_str(), tableName.c_str() ));
				return NULL;
			}
			return pindex;
		}
	} else {
		// for drop index and other cmds, second check after lock, if not exists, unlock and return NULL
		// else, return pindex
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || 
			!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevel ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}
		AbaxBuffer bfr;
		if ( !idxmap->getValue( dbidx, bfr ) ) {
			// not get pindex, unlock and return NULL
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevel ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}		
		JagIndex *pindex = (JagIndex*) bfr.addr();
		return pindex;
	}	
}
int JagServerObjectLock::writeUnlockIndex( abaxint opcode, const AbaxDataString &dbName, const AbaxDataString &tableName, 
											const AbaxDataString &indexName, const JagTableSchema *tschema, 
											const JagIndexSchema *ischema, int replicateType, bool lockSelfLevel )
{
	AbaxDataString repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	AbaxString dbidx = dbName + "." + indexName, dbidxrep = dbidx + "." + repstr;
	AbaxString lockidx = dbtab + "." + indexName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL, *tabmap = NULL, *idxmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
		tabmap = _tables;
		idxmap = _indexs;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
		tabmap = _prevtables;
		idxmap = _previndexs;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
		tabmap = _nexttables;
		idxmap = _nextindexs;
	}
	if ( JAG_DROPINDEX_OP == opcode ) {
		if ( !idxmap->removeKey( dbidx ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevel ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 0;
		} else {
			// final, make sure tableName is removed from _idxtableNames
			if ( !_idxtableNames->removeKey( dbidxrep ) ) {
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevel ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return 0;
			}
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevel ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 1;
		}
	} else {
		_hashLock->writeUnlock( lockidx );
		if ( !lockSelfLevel ) {
			_hashLock->writeUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return 1;
	}	
}

// method to get all table names, db.table|db.table, split with '|'
// when calling this method, it should be already locked outside
// no need to lock in this method
AbaxDataString JagServerObjectLock::getAllTableNames( int replicateType )
{
	JagHashMap<AbaxString, AbaxBuffer> *tabmap = NULL;
	if ( 0 == replicateType ) {
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		tabmap = _nexttables;
	}	
	AbaxDataString str; const AbaxPair<AbaxString, AbaxBuffer> *arr = tabmap->array(); abaxint len = tabmap->arrayLength();
    for ( abaxint i = 0; i < len; ++i ) {
		if ( tabmap->isNull(i) ) continue;
		
		if ( str.size() < 1 ) {
			str = arr[i].key.c_str();
		} else {
			str += AbaxDataString("|") + arr[i].key.c_str();
		}
	}
	return str;
}
