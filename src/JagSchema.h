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
	void init( JagDBServer* serv, const AbaxDataString &type, int replicateType );
	void print();
	void printinfo();
	void setupDefvalMap( const AbaxDataString &dbobj, const char *buf, bool isClean );
	
	int insert( const JagParseParam *parseParam, bool isTable=true );
	bool remove( const AbaxDataString &dbtable );
	bool renameColumn( const AbaxDataString &dbtable, const JagParseParam *parseParam );
	bool checkSpareRemains( const AbaxDataString &dbtable, const JagParseParam *parseParam );
	bool dbTableExist( const AbaxDataString &dbname, const AbaxDataString &tabname );
	
	int  isMemTable( const AbaxDataString &dbtable ) const;
	int  isChainTable( const AbaxDataString &dbtable ) const;
	int  objectType( const AbaxDataString &dbtable ) const;
	const JagSchemaRecord*  getAttr( const AbaxDataString & pathName ) const;
	// bool getAttr( const AbaxDataString & pathName, JagSchemaRecord & onerecord ) const;
	bool getAttr( const AbaxDataString & pathName, AbaxString & keyInfo ) const;
	bool getAttrDefVal( const AbaxDataString & pathName, AbaxDataString & keyInfo ) const;
	AbaxDataString getAllDefVals() const;
	bool existAttr( const AbaxDataString & pathName ) const;
	/***
	bool getOneIndexAttr( const AbaxDataString &dbName, const AbaxDataString &indexName, 
						  AbaxDataString &tabPathName, JagSchemaRecord &onerecord ) const;
						  ***/
	const JagSchemaRecord *getOneIndexAttr( const AbaxDataString &dbName, const AbaxDataString &indexName, 
						  AbaxDataString &tabPathName ) const;

	JagVector<AbaxString> *getAllTablesOrIndexesLabel( int objType, const AbaxDataString &dbtable, const AbaxDataString &like ) const;
	JagVector<AbaxString> *getAllTablesOrIndexes( const AbaxDataString &dbtable, const AbaxDataString &like ) const;
	JagVector<AbaxString> *getAllIndexes( const AbaxDataString &dbname, const AbaxDataString &like ) const;
	AbaxDataString getTableName(const AbaxDataString &dbname, const AbaxDataString &idxname) const;
	AbaxDataString getTableNameScan(const AbaxDataString &dbname, const AbaxDataString &idxname) const;
	bool isIndexCol( const AbaxDataString &dbname, const AbaxDataString &colName );

	AbaxDataString      readSchemaText( const AbaxDataString &key ) const;
	void      			writeSchemaText( const AbaxDataString &key, const AbaxDataString &value );
	void      			removeSchemaFile( const AbaxDataString &key );
	const JagColumn *getColumn( const AbaxDataString &dbname, const AbaxDataString &objname, 
								const AbaxDataString &colname ); 

	static AbaxDataString getDatabases( JagCfg *cfg = NULL, int replicateType=0 );
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
	AbaxDataString 			_stype;
	int						_replicateType;
	JagColumn               _dummyColumn;

	int  _objectTypeNoLock( const AbaxDataString &dbtable ) const;
	int 	addToColumnMap( const AbaxDataString& dbobj, const JagSchemaRecord &record );
	int removeFromColumnMap( const AbaxDataString& dbtabobj, const AbaxDataString& dbobj );


};

#endif

