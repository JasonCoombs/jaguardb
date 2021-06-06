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
	_hashLock = newObject<JagHashLock>();
	initObjects();
}

JagServerObjectLock::~JagServerObjectLock()
{
	cleanupObjects();
	if ( _hashLock ) delete _hashLock;	
}

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

	AbaxBuffer bfr; Jstr dbName = "system", dbName2 = "test";
	_databases->addKeyValue( dbName, bfr );  _databases->addKeyValue( dbName2, bfr );
	_prevdatabases->addKeyValue( dbName, bfr ); _prevdatabases->addKeyValue( dbName2, bfr );
	_nextdatabases->addKeyValue( dbName, bfr ); _nextdatabases->addKeyValue( dbName2, bfr );
}

void JagServerObjectLock::setInitDatabases( const Jstr &dblist, int replicateType )
{
	AbaxBuffer bfr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( JAG_MAIN == replicateType ) {
		dbmap = _databases;
	} else if ( JAG_PREV == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( JAG_NEXT == replicateType ) {
		dbmap = _nextdatabases;
	}

	JagStrSplit sp( dblist.c_str(), '|', true );
	for ( int i = 0; i < sp.length(); ++i ) {
		dbmap->addKeyValue( sp[i], bfr );
	}
}

void JagServerObjectLock::cleanupObjects()
{
	const AbaxPair<AbaxString, AbaxBuffer> *arr; jagint len, i;
	
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

void JagServerObjectLock::rebuildObjects()
{
	cleanupObjects();
	initObjects();
}

jagint JagServerObjectLock::getnumObjects( int objType, int replicateType )
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

int JagServerObjectLock::readLockSchema( int replicateType )
{
	Jstr repstr;
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
	Jstr repstr;
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

int JagServerObjectLock::writeLockSchema( int replicateType )
{
	Jstr repstr;
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
	Jstr repstr;
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

int JagServerObjectLock::readLockDatabase( jagint opcode, const Jstr &dbName, int replicateType )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	JagHashMap<AbaxString, AbaxBuffer> *dbmap = NULL;
	if ( 0 == replicateType ) {
		dbmap = _databases;
	} else if ( 1 == replicateType ) {
		dbmap = _prevdatabases;
	} else if ( 2 == replicateType ) {
		dbmap = _nextdatabases;
	}
	
	if ( !dbmap->keyExist( dbName ) ) return 0; 
	JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	if ( !dbmap->keyExist( dbName ) ) {
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
		return 0;
	}
	return 1;
}
int JagServerObjectLock::readUnlockDatabase( jagint opcode, const Jstr &dbName, int replicateType )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	_hashLock->readUnlock( lockdb );
	_hashLock->readUnlock( repstr );
	return 1;
}

int JagServerObjectLock::writeLockDatabase( jagint opcode, const Jstr &dbName, int replicateType )
{
	AbaxBuffer bfr;
	Jstr repstr = intToStr( replicateType );
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
		if ( dbmap->keyExist( dbName ) ) {
			return 0;
		}
	} else {
		if ( !dbmap->keyExist( dbName ) ) {
			return 0;
		}
	}

	JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
	JAG_BLURT _hashLock->writeLock( lockdb ); JAG_OVER;
	if ( JAG_CREATEDB_OP == opcode ) {
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
		if ( !dbmap->keyExist( dbName ) ) {
			_hashLock->writeUnlock( lockdb );
			_hashLock->readUnlock( repstr );
			return 0;
		} else {
		}
	}
	return 1;
}
int JagServerObjectLock::writeUnlockDatabase( jagint opcode, const Jstr &dbName, int replicateType )
{
	Jstr repstr = intToStr( replicateType );
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

JagTable *JagServerObjectLock::readLockTable( jagint opcode, const Jstr &dbName, 
							   const Jstr &tableName, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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

	
	if ( !dbmap->keyExist( dbName ) ) {
		return NULL; 
	}

	if ( !tabmap->keyExist( dbtab ) ) {
		return NULL; 
	}

	if ( !lockSelfLevelOnly ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	}

	JAG_BLURT _hashLock->readLock( locktab ); JAG_OVER;
	if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ) {
		_hashLock->readUnlock( locktab );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}

	AbaxBuffer bfr;
	if ( !tabmap->getValue( dbtab, bfr ) ) {
		_hashLock->readUnlock( locktab );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}

	JagTable *ptab = (JagTable*) bfr.addr();
	return ptab;
}

int JagServerObjectLock::readUnlockTable( jagint opcode, const Jstr &dbName, 
	const Jstr &tableName, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	_hashLock->readUnlock( locktab );
	if ( !lockSelfLevelOnly ) {
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
	}
	return 1;	
}

JagTable *JagServerObjectLock::writeLockTable( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
										       const JagTableSchema *tschema, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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
	} else {
		return NULL;
	}

	if ( !lockSelfLevelOnly ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
	}

	JAG_BLURT _hashLock->writeLock( locktab ); JAG_OVER;	
	if ( JAG_CREATETABLE_OP == opcode || JAG_CREATECHAIN_OP == opcode || JAG_CREATEMEMTABLE_OP == opcode ) {
		if ( !dbmap->keyExist( dbName ) || (dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab )) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		} else {
			const JagSchemaRecord *record = tschema->getAttr( dbtab.c_str() );
			if ( ! record ) {
				_hashLock->writeUnlock( locktab );
				if ( !lockSelfLevelOnly ) {
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
				if ( !lockSelfLevelOnly ) {
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}
			return ptab;
		}
	} else {
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}

		AbaxBuffer bfr;
		if ( ! tabmap->getValue( dbtab, bfr ) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}		
		JagTable *ptab = (JagTable*) bfr.addr();
		return ptab;
	}
}

int JagServerObjectLock::writeUnlockTable( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
									       int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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
	} else {
		return -1;
	}
	
	if ( JAG_DROPTABLE_OP == opcode ) {
		if ( !tabmap->removeKey( dbtab ) ) {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 0;
		} else {
			_hashLock->writeUnlock( locktab );
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}	
			return 1;
		}
	} else {
		_hashLock->writeUnlock( locktab );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}

		return 1;
	}
}

JagTable *JagServerObjectLock::getTable( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
										 const JagTableSchema *tschema, int replicateType )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString dbtab = dbName + "." + tableName;
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
	} else {
		return NULL;
	}
	
	if ( ! dbmap->keyExist( dbName ) ) {
		return NULL;
	}

	AbaxBuffer bfr;
	if ( ! tabmap->getValue( dbtab, bfr ) ) {
		if ( JAG_CREATETABLE_OP == opcode ) {
			const JagSchemaRecord *record = tschema->getAttr( dbtab.c_str() );
			if ( ! record ) {
				return NULL;
			}

			JagTable *ptab = new JagTable( replicateType, _servobj, dbName.s(), tableName.s(), *record );
			AbaxBuffer bfr = (void*) ptab;
			if ( !tabmap->addKeyValue( dbtab, bfr ) ) {
				delete ptab;
				return NULL;
			}
			return ptab;
		}
		return NULL;
	}

	JagTable *ptab = (JagTable*) bfr.addr();
	return ptab;
}

JagTable *JagServerObjectLock::
writeTruncateTable( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
				    const JagTableSchema *tschema, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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
			if ( !lockSelfLevelOnly ) {
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
			if ( !lockSelfLevelOnly ) {
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		} else {
			return ptab;
		}
	} else {
		_hashLock->writeUnlock( locktab );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
}

JagIndex *JagServerObjectLock::readLockIndex( jagint opcode, const Jstr &dbName, Jstr &tableName, const Jstr &indexName,
											  int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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
		!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) return NULL; 
	if ( !lockSelfLevelOnly ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( locktab ); JAG_OVER;
	}
	JAG_BLURT _hashLock->readLock( lockidx ); JAG_OVER;
	if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) ||
		!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
		_hashLock->readUnlock( lockidx );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
	AbaxBuffer bfr;
	if ( !idxmap->getValue( dbidx, bfr ) ) {
		_hashLock->readUnlock( lockidx );
		if ( !lockSelfLevelOnly ) {
			_hashLock->readUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return NULL;
	}
	JagIndex *pindex = (JagIndex*) bfr.addr();
	return pindex;
}
int JagServerObjectLock::readUnlockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, const Jstr &indexName,
										  int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString lockdb = dbName + "." + repstr;
	AbaxString dbtab = dbName + "." + tableName;
	AbaxString locktab = dbtab + "." + repstr;
	AbaxString dbidx = dbName + "." + indexName;
	AbaxString lockidx = dbtab + "." + indexName + "." + repstr;
	_hashLock->readUnlock( lockidx );
	if ( !lockSelfLevelOnly ) {
		_hashLock->readUnlock( locktab );
		_hashLock->readUnlock( lockdb );
		_hashLock->readUnlock( repstr );
	}
	return 1;	
}	

JagIndex *JagServerObjectLock::writeLockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
												const Jstr &indexName, const JagTableSchema *tschema, 
												const JagIndexSchema *ischema, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
	AbaxString dbidx = dbName + "." + indexName, dbidxrep = dbidx + "." + repstr;
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

	if ( JAG_CREATEINDEX_OP == opcode ) {
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || _idxtableNames->keyExist( dbidxrep ) ||
			( dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab ) && idxmap->keyExist( dbidx ) ) ) {
			return NULL;
		}
	} else {
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || 
			!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
			return NULL;
		}
	}

	if ( !lockSelfLevelOnly ) {
		JAG_BLURT _hashLock->readLock( repstr ); JAG_OVER;
		JAG_BLURT _hashLock->readLock( lockdb ); JAG_OVER;
		JAG_BLURT _hashLock->writeLock( locktab ); JAG_OVER;
	}

	JAG_BLURT _hashLock->writeLock( lockidx ); JAG_OVER;
	if ( JAG_CREATEINDEX_OP == opcode ) {
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || _idxtableNames->keyExist( dbidxrep ) ||
			( dbmap->keyExist( dbName ) && tabmap->keyExist( dbtab ) && idxmap->keyExist( dbidx ) ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevelOnly ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		} else {
			Jstr path;
			const JagSchemaRecord *trecord, *irecord;
			irecord = ischema->getOneIndexAttr( dbName, indexName, path );
			trecord = tschema->getAttr( path );
			if ( ! irecord || ! trecord ) {
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevelOnly ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}

			path += Jstr(".") + indexName;
			JagIndex *pindex = new JagIndex( replicateType, _servobj, path, *trecord, *irecord );
			AbaxBuffer bfr = (void*) pindex;
			if ( !idxmap->addKeyValue( dbidx, bfr ) ) {
				delete pindex;
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevelOnly ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}
			
			if ( !_idxtableNames->addKeyValue( dbidxrep, tableName ) ) {
				delete pindex;
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevelOnly ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return NULL;
			}
			return pindex;
		}
	} else {
		if ( !dbmap->keyExist( dbName ) || !tabmap->keyExist( dbtab ) || 
			!_idxtableNames->keyExist( dbidxrep ) || !idxmap->keyExist( dbidx ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevelOnly ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return NULL;
		}

		AbaxBuffer bfr;
		if ( !idxmap->getValue( dbidx, bfr ) ) {
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevelOnly ) {
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

JagIndex *JagServerObjectLock::getIndex( const Jstr &dbName, const Jstr &indexName, int replicateType )
{
	AbaxString dbidx = dbName + "." + indexName;

	JagHashMap<AbaxString, AbaxBuffer> *idxmap = NULL;

	if ( 0 == replicateType ) {
		idxmap = _indexs;
	} else if ( 1 == replicateType ) {
		idxmap = _previndexs;
	} else if ( 2 == replicateType ) {
		idxmap = _nextindexs;
	}

	AbaxBuffer bfr;
	if ( !idxmap->getValue( dbidx, bfr ) ) {
		return NULL;
	}

	JagIndex *pindex = (JagIndex*) bfr.addr();
	return pindex;
}

int JagServerObjectLock::writeUnlockIndex( jagint opcode, const Jstr &dbName, const Jstr &tableName, 
											const Jstr &indexName, int replicateType, bool lockSelfLevelOnly )
{
	Jstr repstr = intToStr( replicateType );
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
			if ( !lockSelfLevelOnly ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 0;
		} else {
			if ( !_idxtableNames->removeKey( dbidxrep ) ) {
				_hashLock->writeUnlock( lockidx );
				if ( !lockSelfLevelOnly ) {
					_hashLock->writeUnlock( locktab );
					_hashLock->readUnlock( lockdb );
					_hashLock->readUnlock( repstr );
				}
				return 0;
			}
			_hashLock->writeUnlock( lockidx );
			if ( !lockSelfLevelOnly ) {
				_hashLock->writeUnlock( locktab );
				_hashLock->readUnlock( lockdb );
				_hashLock->readUnlock( repstr );
			}
			return 1;
		}
	} else {
		_hashLock->writeUnlock( lockidx );
		if ( !lockSelfLevelOnly ) {
			_hashLock->writeUnlock( locktab );
			_hashLock->readUnlock( lockdb );
			_hashLock->readUnlock( repstr );
		}
		return 1;
	}	
}

Jstr JagServerObjectLock::getAllTableNames( int replicateType )
{
	JagHashMap<AbaxString, AbaxBuffer> *tabmap = NULL;
	if ( 0 == replicateType ) {
		tabmap = _tables;
	} else if ( 1 == replicateType ) {
		tabmap = _prevtables;
	} else if ( 2 == replicateType ) {
		tabmap = _nexttables;
	}

	Jstr str; 
	const AbaxPair<AbaxString, AbaxBuffer> *arr = tabmap->array(); 
	jagint len = tabmap->arrayLength();
    for ( jagint i = 0; i < len; ++i ) {
		if ( tabmap->isNull(i) ) continue;
		
		if ( str.size() < 1 ) {
			str = arr[i].key.c_str();
		} else {
			str += Jstr("|") + arr[i].key.c_str();
		}
	}
	return str;
}
