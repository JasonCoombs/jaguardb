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

#ifndef _jag_schema_h_
#define _jag_schema_h_

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <JagVector.h>
#include <JagBlock.h>
#include <JagArray.h>
#include <JagDiskArrayServer.h>
#include <JagHashMap.h>
#include <JagBuffReader.h>
#include <JagColumn.h>
#include <JagSchemaRecord.h>
#include <JagUtil.h>
#include <JagCfg.h>
#include <JagMutex.h>
#include <JagParseParam.h>

//////////////////////////////////////////////////////////////////////////////////////////
// JAG_SCHEMA_SPARE_LEN   16  bytes
// spare: 		key or not
// spare+1: 	col type
// spare+2: 	JAG_ASC or desc 
// spare+3: 	key JAG_ASC or desc 
// spare+4: 	JAG_CREATE_DEFUPDATETIME/JAG_CREATE_DEFUPDATE/JAG_CREATE_DEFDATETIME/JAG_CREATE_DEFDATE
// spare+5: 	JAG_KEY_MUTE
// spare+6: 	JAG_SUB_COL
//
//
//
//
//
//////////////////////////////////////////////////////////////////////////////////////////


class JagDBServer;
class JagSchema
{

  public:
	JagSchema();
  	virtual ~JagSchema(); 
	virtual void destroy( bool removelock = true);
	void init( JagDBServer* serv, const Jstr &type, int replicateType );
	void print();
	void printinfo();
	void setupDefvalMap( const Jstr &dbobj, const char *buf, bool isClean );
	
	int insert( const JagParseParam *parseParam, bool isTable=true );
	bool remove( const Jstr &dbtable );
	bool renameColumn( const Jstr &dbtable, const JagParseParam *parseParam );
	bool checkSpareRemains( const Jstr &dbtable, const JagParseParam *parseParam );
	bool dbTableExist( const Jstr &dbname, const Jstr &tabname );
	
	int  isMemTable( const Jstr &dbtable ) const;
	int  isChainTable( const Jstr &dbtable ) const;
	int  objectType( const Jstr &dbtable ) const;
	const JagSchemaRecord*  getAttr( const Jstr & pathName ) const;
	// bool getAttr( const Jstr & pathName, JagSchemaRecord & onerecord ) const;
	bool getAttr( const Jstr & pathName, AbaxString & keyInfo ) const;
	bool getAttrDefVal( const Jstr & pathName, Jstr & keyInfo ) const;
	Jstr getAllDefVals() const;
	bool existAttr( const Jstr & pathName ) const;
	/***
	bool getOneIndexAttr( const Jstr &dbName, const Jstr &indexName, 
						  Jstr &tabPathName, JagSchemaRecord &onerecord ) const;
						  ***/
	const JagSchemaRecord *getOneIndexAttr( const Jstr &dbName, const Jstr &indexName, 
						  Jstr &tabPathName ) const;

	JagVector<AbaxString> *getAllTablesOrIndexesLabel( int objType, const Jstr &dbtable, const Jstr &like ) const;
	JagVector<AbaxString> *getAllTablesOrIndexes( const Jstr &dbtable, const Jstr &like ) const;
	JagVector<AbaxString> *getAllIndexes( const Jstr &dbname, const Jstr &like ) const;
	Jstr getTableName(const Jstr &dbname, const Jstr &idxname) const;
	Jstr getTableNameScan(const Jstr &dbname, const Jstr &idxname) const;
	bool isIndexCol( const Jstr &dbname, const Jstr &colName );

	Jstr    readSchemaText( const Jstr &key ) const;
	void   	writeSchemaText( const Jstr &key, const Jstr &value );
	void   	removeSchemaFile( const Jstr &key );
	const JagColumn *getColumn( const Jstr &dbname, const Jstr &objname, 
								const Jstr &colname ); 

	static Jstr getDatabases( JagCfg *cfg = NULL, int replicateType=0 );
	static const abaxint KEYLEN = JAG_SCHEMA_KEYLEN;
	static const abaxint VALLEN = JAG_SCHEMA_VALLEN;
	static const abaxint KVLEN = KEYLEN + VALLEN;

  protected:	
	JagHashMap<AbaxString,JagSchemaRecord> 	*_schemaMap;

	JagHashMap<AbaxString,AbaxString> 		*_recordMap;

	//JagHashMap<AbaxString,AbaxString> 		*_defvalMap;
	JagHashStrStr 		*_defvalMap;

	JagHashMap<AbaxString,AbaxString> 		*_tableIndexMap;

	JagHashMap<AbaxString, JagColumn>       *_columnMap;

	JagReadWriteLock    	*_lock;
	JagCfg					*_cfg;
	JagDBServer    			*_servobj;
	JagSchemaRecord 		*_schmRecord;
	JagDiskArrayServer  	*_schema;
	Jstr 			_stype;
	int						_replicateType;
	JagColumn               _dummyColumn;

	int  _objectTypeNoLock( const Jstr &dbtable ) const;
	int 	addToColumnMap( const Jstr& dbobj, const JagSchemaRecord &record );
	int removeFromColumnMap( const Jstr& dbtabobj, const Jstr& dbobj );


};

#endif

