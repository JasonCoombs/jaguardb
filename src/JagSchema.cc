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

#include <dirent.h>
#include <abax.h>
#include <JagSchema.h>
#include <JagStrSplit.h>
#include <JagStrSplitWithQuote.h>
#include <JagDBServer.h>
#include <JDFS.h>
#include <JagDBConnector.h>
#include <JagArray.h>
#include <JagUtil.h>
#include <JagFileMgr.h>

// _schemaRecMap: tableschame key-> db.tab      value->SchemaRecord
// _schemaRecMap: indexschema key-> db.tab.idx  value->SchemaRecord
// _recordmap: tableschame key-> db.tab      value->SchemaText
// _recordMap: indexschema key-> db.tab.idx  value->SchemaText
// _columnMap-> (key-> db.obj.col   val->JagColumn )
// _tableIndexMap-> (key->db.idx    val->tabName );

JagSchema::JagSchema()
{
	_lock = newJagReadWriteLock();
	_cfg = newObject<JagCfg>();
	_replicateType = 0;
}

void JagSchema::init( JagDBServer *serv, const Jstr &stype, int replicateType )
{
	// prt(("s4481 JagSchema::init() ...\n" ));
	_replicateType = replicateType;
	_servobj = serv;

	Jstr jagdatahome = _cfg->getJDBDataHOME( _replicateType );
	Jstr fpath = jagdatahome; 
	fpath += "/system";

	_stype = stype;
	if ( stype == "TABLE" ) {
		fpath += "/TableSchema";
	} else {
		fpath += "/IndexSchema";
	}

	_schmRecord = new JagSchemaRecord( true );
	_schmRecord->keyLength = KEYLEN;
	_schmRecord->valueLength = VALLEN;
	_schmRecord->ovalueLength = VALLEN;

	// prt(("s3038 _schema = new JagDiskArrayServer fpath=[%s]\n", fpath.c_str() ));
	_schema = new JagLocalDiskHash( fpath, KEYLEN, VALLEN );

	if ( ! _schema ) {
		raydebug( stdout, JAG_LOG_LOW, "s0314 error new JagDiskArrayServer, exit\n");
		exit(12);
	}

	_schemaRecMap = new JagHashMap<AbaxString, JagSchemaRecord>;
	_recordMap = new JagHashMap<AbaxString, AbaxString>;
	_defvalMap = new JagHashStrStr;
	_tableIndexMap = new JagHashMap<AbaxString, AbaxString>;
	_columnMap = new JagHashMap<AbaxString, JagColumn>;

	char *buf = (char*)jagmalloc(KVLEN+1);
	memset( buf, '\0', KVLEN+1 );
	jagint length = _schema->getLength();
	Jstr keystr, schemaText, dbobj;
	JagSingleBuffReader nti( _schema->getFD(), length, KEYLEN, VALLEN, 0, 0, 1 );
	int prc;
	while ( nti.getNext(buf) ) {
		keystr = buf;
		// prt(("s3848 keystr=[%s]\n", buf ));
		schemaText = readSchemaText( keystr );
		_recordMap->addKeyValue( AbaxString(keystr), AbaxString(schemaText) );
		setupDefvalMap( keystr, buf+KEYLEN, 0 );
		//JagSchemaRecord record(keystr);
		JagSchemaRecord record(true);
		//prt(("s1902 record.parseRecord() ...\n" ));
		prc = record.parseRecord( schemaText.c_str() );
		// prt(("s2338 done record.parseRecord prc=%d\n", prc ));
		AbaxString sk( keystr );	
		// sk: test.t1  or test.t1.t1idx1
		_schemaRecMap->addKeyValue( sk, record );	
		//prt(("s0271 _schemaRecMap->addKeyValue( sk=[%s] , record )\n", sk.c_str() ));

		JagStrSplit sp(keystr, '.');
		if ( sp.length() == 3 ) { // index
			dbobj = sp[0] + "." + sp[2];
			_tableIndexMap->addKeyValue( /*db.indx*/dbobj, /*tab*/sp[1] ); 
			addToColumnMap( dbobj, record );
		} else if (  sp.length() == 2 ) { // table
			dbobj = sp[0] + "." + sp[1];
			addToColumnMap( dbobj, record );
		}
	}
	free( buf );
}

JagSchema::~JagSchema()
{
	destroy();
}

void JagSchema::destroy( bool removelock )
{
	if ( _schema ) {
		delete _schema;
		_schema = NULL;
	}

	if ( _schemaRecMap ) {
		delete _schemaRecMap;
		_schemaRecMap = NULL;
	}

	if ( _recordMap ) {
		delete _recordMap;
		_recordMap = NULL;
	}

	if ( _defvalMap ) {
		delete _defvalMap;
		_defvalMap = NULL;
	}

	if ( _tableIndexMap ) {
		delete _tableIndexMap;
		_tableIndexMap = NULL;
	}

	if ( _schmRecord ) {
		delete _schmRecord;
		_schmRecord = NULL;
	}

	if ( _lock && removelock ) {
		deleteJagReadWriteLock( _lock );
		_lock = NULL;
	}

	if ( _cfg ) {
		delete _cfg;
		_cfg = NULL;
	}
}

void JagSchema::printinfo()
{
	printf("schema printinfo %s _schema->elements=%lld\n", _schema->getFilePath().c_str(), (jagint)_schema->elements() );
}

void JagSchema::print() 
{ 
	_schemaRecMap->printKeyStringOnly(); 
}

// dbobj: dbtab or dbtabidx; 
// buf: def value string with column names; 
// isClean: 0 is set, 1 is clear
void JagSchema::setupDefvalMap( const Jstr &dbobj, const char *buf, bool isClean )
{
	Jstr key, value;
	if ( !isClean ) {
		//prt(("s5051 setupDefvalMap buf=[%s] buf+1=[%s]\n", buf, buf+1 ));
		JagStrSplitWithQuote sp( buf+1, ':' );
		bool oldStyle = true;
		for ( int i = 0; i < sp.length(); ++i ) {
			if ( strchr(sp[i].c_str(), '=' ) ) {
				oldStyle = false;
				break;
			}
		}

		for ( int i = 0; i < sp.length(); ++i ) {
			if ( oldStyle ) {
    			if ( i%2 == 0 ) {
    				key = dbobj + "." + sp[i];
    				//prt(("s5041 dbobj=[%s] key=[%s]\n", dbobj.c_str(), key.c_str() ));
    			} else {
    				value = sp[i];
    				//prt(("s5442 add key=[%s] value=[%s]\n", key.c_str(), value.c_str() ));
    				if ( !_defvalMap->addKeyValue( key, value ) ) {
    					_defvalMap->removeKey( key );
    					_defvalMap->addKeyValue( key, value );
    					//prt(("s5042 add key=[%s] value=[%s]\n", key.c_str(), value.c_str() ));
    				}
    			}
			} else {
    			if ( sp[i].length() < 1 ) continue;
    			JagStrSplit nv( sp[i], '=' );
    			if ( nv.length() < 2 ) continue;
    			key = dbobj + "." + nv[0];
    			value = nv[1];
    
    			if ( !_defvalMap->addKeyValue( key, value ) ) {
    				_defvalMap->removeKey( key );
    				_defvalMap->addKeyValue( key, value );
    				//prt(("s5042 add key=[%s] value=[%s]\n", key.c_str(), value.c_str() ));
    			}
			}
		}
	} else {
		key = dbobj;
		const JagSchemaRecord *record = _schemaRecMap->getValue( key );
		if (  record ) {
			for ( int i = 0; i < record->columnVector->size(); ++i ) {
				key = dbobj + "." + (*(record->columnVector))[i].name.c_str();
				_defvalMap->removeKey( key );
			}
		}
	}
}

// table: pathName = dbname + "." + tabname;
// index: pathName = dbname + "." + tabname + "." + idxname;
// return 0: error   1: 
int JagSchema::insert( const JagParseParam *parseParam, bool isTable )
{
	JagSchemaRecord record(true);
	JagColumn onecolrec;
	bool rc;
	int length = 0;
	AbaxString dum;
	Jstr  schemaText;
	int buflen=8092;
	char buf[buflen];
	char ddd[32];
	char buf2[2];
	buf2[1] = '\0';

	if ( parseParam->keyLength < 1 ) {
		prt(("E0501 parseParam->keyLength=%d  < 1 error\n", parseParam->keyLength ));
		return 0;
	}

	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;

	Jstr pathName, dbname, tabname, idxname;
	if ( isTable ) {
		dbname = parseParam->objectVec[0].dbName ;
		tabname = parseParam->objectVec[0].tableName;
		pathName = dbname + "." + tabname;
	   // for tblae pathName = db.tab;
	} else {
		dbname = parseParam->objectVec[1].dbName;
		tabname = parseParam->objectVec[1].tableName;
		idxname = parseParam->objectVec[1].indexName;
		pathName = parseParam->objectVec[1].dbName + "." + parseParam->objectVec[1].tableName + "." +
				   parseParam->objectVec[1].indexName;
	    // for index pathName = db.tab.idx;
	}

	memset(buf, 0, buflen );
	Jstr ts = parseParam->timeSeries;
	prt(("s55107 parseParam->timeSeries=[%s]\n", ts.s() ));
	if ( ts.size() < 1 ) ts = "0";
	Jstr tableProperty = intToStr( parseParam->polyDim ) + "!" + ts + "!" + parseParam->retain + "!0!";
	// retain is "100d" or "12m" or "24h" etc
	// 1!timeseries!retain!...!      timeseries: nm_r:nh:nd_r:nw:nM:ny
	prt(("s52134 schema polyDim=%d tableProperty=[%s]\n", parseParam->polyDim, tableProperty.c_str() ));
	if ( parseParam->isMemTable ) {
		// NM means memtable
		sprintf(buf, "NM|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );
	} else if ( parseParam->isChainTable ) { 
		sprintf(buf, "NC|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );
	} else {
		sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );
	}
	schemaText += buf;
	record.keyLength = parseParam->keyLength;
	record.valueLength = parseParam->valueLength;
	record.ovalueLength = parseParam->ovalueLength;
	record.tableProperty = tableProperty;
	
	for (int i = 0; i < parseParam->createAttrVec.size(); i++) {
		memset( buf, 0, buflen );
		sprintf( buf, "!%s!%s!%d!%d", parseParam->createAttrVec[i].objName.colName.c_str(), 
							parseParam->createAttrVec[i].type.c_str(),
							parseParam->createAttrVec[i].offset, parseParam->createAttrVec[i].length);
		
		onecolrec.name = parseParam->createAttrVec[i].objName.colName;
		onecolrec.type = parseParam->createAttrVec[i].type;
		onecolrec.offset = parseParam->createAttrVec[i].offset;
		onecolrec.length = parseParam->createAttrVec[i].length;
		onecolrec.sig = parseParam->createAttrVec[i].sig;
		onecolrec.srid = parseParam->createAttrVec[i].srid;
		onecolrec.begincol = parseParam->createAttrVec[i].begincol;
		onecolrec.endcol = parseParam->createAttrVec[i].endcol;
		//onecolrec.dummy1 = parseParam->createAttrVec[i].dummy1;
		onecolrec.metrics = parseParam->createAttrVec[i].metrics;
		// onecolrec.dummy2 = parseParam->createAttrVec[i].dummy2;
		onecolrec.rollupWhere = parseParam->createAttrVec[i].rollupWhere;
		onecolrec.dummy3 = parseParam->createAttrVec[i].dummy3;
		onecolrec.dummy4 = parseParam->createAttrVec[i].dummy4;
		onecolrec.dummy5 = parseParam->createAttrVec[i].dummy5;
		onecolrec.dummy6 = parseParam->createAttrVec[i].dummy6;
		onecolrec.dummy7 = parseParam->createAttrVec[i].dummy7;
		onecolrec.dummy8 = parseParam->createAttrVec[i].dummy8;
		onecolrec.dummy9 = parseParam->createAttrVec[i].dummy9;
		onecolrec.dummy10 = parseParam->createAttrVec[i].dummy10;
		memcpy(onecolrec.spare, parseParam->createAttrVec[i].spare, JAG_SCHEMA_SPARE_LEN );
		//printf("s83838 onecolrec.type=[%c]\n", onecolrec.type );
		/***
		prt(("s5821 onecolrec.begincol=%d onecolrec.endcol=%d onecolrec.dummy1=%d\n",
				onecolrec.begincol, onecolrec.endcol, onecolrec.dummy1 ));
		***/

		if ( *(onecolrec.spare) == JAG_C_COL_KEY ) {
			onecolrec.iskey = true;
		}
		record.columnVector->append( onecolrec );
		schemaText += buf;
		memset( buf, 0, buflen );
		sprintf( buf, "!%d!", onecolrec.sig );
		for ( int k = 0; k < JAG_SCHEMA_SPARE_LEN; ++k ) {
			buf2[0] = onecolrec.spare[k];
			strcat( buf, buf2 );
		}

		sprintf( ddd, "!%d!", onecolrec.srid ); strcat( buf, ddd );
		sprintf( ddd, "%d!", onecolrec.begincol ); strcat( buf, ddd );
		sprintf( ddd, "%d!", onecolrec.endcol ); strcat(  buf, ddd );
		//sprintf( ddd, "%d!", onecolrec.dummy1 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.metrics ); strcat(  buf, ddd );
		//sprintf( ddd, "%d!", onecolrec.dummy2 ); strcat(  buf, ddd );
		sprintf( ddd, "%s!", onecolrec.rollupWhere.s() ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy3 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy4 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy5 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy6 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy7 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy8 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy9 ); strcat(  buf, ddd );
		sprintf( ddd, "%d!", onecolrec.dummy10 ); strcat(  buf, ddd );

		prt(("s7183 onecolrec.begincol=%d onecolrec.endcol=%d onecolrec.metrics=%d\n", 
				onecolrec.begincol, onecolrec.endcol, onecolrec.metrics ));

		strcat( buf, "|" );
		schemaText += buf;
	}
	schemaText += "}";

	record.setLastKeyColumn();

	char *kvbuf = (char*)jagmalloc(KVLEN+1);
	memset( kvbuf, 0, KVLEN+1 );
	strcpy( kvbuf, pathName.c_str() );
	length = KEYLEN;
	Jstr colName;
	Jstr defValues;
	// for each column, need to add default value if exists
	for (int i = 0; i < parseParam->createAttrVec.size(); ++i) {
		if ( parseParam->createAttrVec[i].defValues.size() > 0 ) {
			colName = parseParam->createAttrVec[i].objName.colName;
			defValues = parseParam->createAttrVec[i].defValues;
			if ( length+1+colName.size()+1+defValues.size() > KVLEN ) {
				free( kvbuf );
				prt(("E0301 Error default value for column [%s]\n", colName.c_str() ));
				return 0;
			}
			kvbuf[length] = ':';
			++length;
			memcpy( kvbuf+length, colName.c_str(), colName.size() );
			length += colName.size();
			kvbuf[length] = '=';
			++length;
			prt(("s5603  parseParam=%0x \n", parseParam ));
			memcpy( kvbuf+length, defValues.c_str(), defValues.size() );
			length += defValues.size();
		}
	}

	JagDBPair pair( kvbuf, KEYLEN, kvbuf+KEYLEN, VALLEN, true );
	bool check = _schema->insert( pair );
	if ( !check ) {
		prt(("ERR0505 _schema->insert check=%d insertCode=%d\n", check ));
		free( kvbuf );
		return 0;
	}
	writeSchemaText( pathName, schemaText );

	rc = _recordMap->addKeyValue( AbaxString(pathName), AbaxString( schemaText ) );
	if ( ! rc ) {
		prt(("WARN0201 _recordMap->addKeyValue pathName=[%s]\n", pathName.c_str() ));
	}

	rc = _schemaRecMap->addKeyValue( AbaxString(pathName), record );
	if ( ! rc ) {
		prt(("WARN0202 _schemaRecMap->addKeyValue pathName=[%s]\n", pathName.c_str() ));
	}

	Jstr dbobj;
	if ( ! isTable ) {
		dbobj = dbname + "." + idxname;
		_tableIndexMap->addKeyValue( dbobj, tabname );
		addToColumnMap( dbobj, record );
	} else {
		dbobj = dbname + "." + tabname;
		addToColumnMap( dbobj, record );
	}

	setupDefvalMap( pathName, kvbuf+KEYLEN, 0 );
	free( kvbuf );

	return 1;
}

//  indexschema->remove( dbtabidx );
// dbtable is db.tab.idx
bool JagSchema::remove( const Jstr &dbtable ) 
{
	// prt(("s0338 JagSchema::remove( %s ) ...\n", dbtable.c_str() ));

	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;

	char *keybuf = (char*)jagmalloc(KEYLEN+1);
	memset(keybuf, '\0', KEYLEN+1);
	strcpy(keybuf, dbtable.c_str());
	JagFixString key(keybuf, KEYLEN, KEYLEN );
	bool rc;

	JagDBPair pair( key );
	rc = _schema->remove( pair );
	removeSchemaFile( key.c_str() );

	// must apply before schemaMap and recordMap removeKey
	setupDefvalMap( dbtable, NULL, 1 );
	
	_recordMap->removeKey( AbaxString(dbtable) );

	JagStrSplit sp( dbtable, '.');
	AbaxString dbobj;
	if ( sp.length() == 3 ) { // index
		dbobj = sp[0] + "." + sp[2];
	} else if (  sp.length() == 2 ) { // table
		dbobj = sp[0] + "." + sp[1];
	}
	_tableIndexMap->removeKey( dbobj );
	// prt(("s0283 remove from _tableIndexMap dbtable=[%s]\n", dbtable.c_str() ));
	removeFromColumnMap( dbtable, dbobj.c_str() );

	_schemaRecMap->removeKey( AbaxString(dbtable) );
	free( keybuf );
	return true;
}

Jstr JagSchema::getHeader( const Jstr &sstr )
{
	//"NN|keyLen|valueLen|tableProperty|{ body ...}
	// returns "NN|keyLen|valueLen|tableProperty|"
	const char *start = sstr.s();
	const char *end = strchr( sstr.s(), '{' );
	return Jstr(start, end - start );
}

Jstr JagSchema::getBody( const Jstr &sstr )
{
	//sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );
	//"NN|keyLen|valueLen|tableProperty|{ body ...}
	// returns "{ body ..... }"
	const char *start = strchr( sstr.s(), '{' );
	// prt(("s41227 getBody input sstr=[%s] bodystart=[%s]\n", sstr.s(), start ));
	return start;
}

// tick: "1M_3M"
bool JagSchema::addTick( const Jstr &dbtable, const Jstr &tick )
{
	prt(("s203171 addTick dbtable=%s tick=[%s]\n", dbtable.s(), tick.s() ));
	//sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );

	JagSchemaRecord record(true);
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;
	bool rc = _schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( ! rc ) {
		return false;
	}

	Jstr sstr = record.getString();
	Jstr hdr = getHeader( sstr );
	Jstr body = getBody( sstr );

	JagStrSplit sp(hdr, '|');
	Jstr tableProperty = sp[3];
	//tableProperty = "polyDim!timeSeries!retain!0!"
	JagStrSplit tp( tableProperty, '!');
	Jstr dbtser = tp[1];

	Jstr allTser = dbtser + ":" + tick;
	Jstr norm;
	int nrc = JagSchemaRecord::normalizeTimeSeries( allTser, norm );
	if ( 0 == nrc ) {
		// need change if tableProperty is augmented to more fields
		// OK
		Jstr newTser = norm;
		Jstr newTabProperty = tp[0] + "!" + newTser + "!" + tp[2] + "!" + tp[3];
		Jstr newHeader = sp[0] + "|" + sp[1] + "|" + sp[2] + "|" + newTabProperty + "|"; 
		Jstr allStr = newHeader + body;
		_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );
		writeSchemaText( dbtable, allStr );

		// record update time series
		record.setTimeSeries( allTser );
		_schemaRecMap->setValue( dbtable, record );
		prt(("s23371 _schemaRecMap->set dbtable=%s to new schemarecord\n", dbtable.s() ));
		return true;
	}

	return false;
}

// tick: "1M"
bool JagSchema::dropTick( const Jstr &dbtable, const Jstr &tick )
{
	JagStrSplit s1(tick, '_');
	Jstr tickTable = s1[0];
	prt(("s203172 dropTick dbtable=%s tick=[%s]\n", dbtable.s(), tick.s() ));
	//sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );

	JagSchemaRecord record(true);
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;
	bool rc = _schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( ! rc ) {
		return false;
	}

	Jstr sstr = record.getString();
	Jstr hdr = getHeader( sstr );
	Jstr body = getBody( sstr );

	JagStrSplit sp(hdr, '|');
	Jstr tableProperty = sp[3];
	//tableProperty = "polyDim!timeSeries!retain!0!"
	JagStrSplit tp( tableProperty, '!');
	Jstr dbtser = tp[1];

	// remove tick from dbtser: "15m_120m:3M_12M:1y_5y"
	JagStrSplit tsp( dbtser, ':');
	Jstr newTser;
	for ( int i = 0; i < tsp.size(); ++i ) {
		JagStrSplit rec( tsp[i], '_' );
		if ( rec[0] == tickTable ) {
			continue;
		}

		if ( newTser.size() < 1 ) {
			newTser = tsp[i];
		} else {
			newTser += Jstr(":") + tsp[i];
		}
	}

	// need change if tableProperty is augmented to more fields
	// OK
	Jstr newTabProperty = tp[0] + "!" + newTser + "!" + tp[2] + "!" + tp[3];
	Jstr newHeader = sp[0] + "|" + sp[1] + "|" + sp[2] + "|" + newTabProperty + "|"; 
	Jstr allStr = newHeader + body;
	_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );
	writeSchemaText( dbtable, allStr );

	// record update time series
	record.setTimeSeries( newTser );
	_schemaRecMap->setValue( dbtable, record );
	prt(("s23371 _schemaRecMap->set dbtable=%s to new schemarecord\n", dbtable.s() ));
	return true;
}

// rentention: "1M" "0h"  "0"
bool JagSchema::changeRetention( const Jstr &dbtable, const Jstr &retention )
{
	if ( strchr( retention.s(), '_') ) {
		prt(("s223303 changeRetention retention=[%s] is not valid. found _\n", retention.s() ));
		return false;
	}

	jagint num = jagatoll( retention );
	if ( num < 0 ) {
		prt(("s225303 changeRetention retention=[%s] is not valid. negative\n", retention.s() ));
		return false;
	}

	char fc = retention.firstChar();
	if ( fc < '0' || fc > '9' ) {
		// retention does not start with digits
		prt(("s22550 changeRetention retention=[%s] is not valid. no leading digit\n", retention.s() ));
		return false;
	} 

	if ( retention != "0" ) {
		char lastc = retention.lastChar();
		if ( ! JagSchemaRecord::validRetention( lastc ) ) {
			prt(("s223308 changeRetention lastc=[%c] is not valid retenetion period\n", lastc ));
			return false;
		}
	}

	prt(("s203172 changeRetention dbtable=%s rentention=[%s]\n", dbtable.s(), retention.s() ));
	//sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );

	JagSchemaRecord record(true);
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;
	bool rc = _schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( ! rc ) {
		return false;
	}

	Jstr sstr = record.getString();
	Jstr hdr = getHeader( sstr );
	Jstr body = getBody( sstr );

	JagStrSplit sp(hdr, '|');
	Jstr tableProperty = sp[3];
	//tableProperty = "polyDim!timeSeries!retain!0!"
	JagStrSplit tp( tableProperty, '!');
	// Jstr dbRetention = tp[2];

	// need change if tableProperty is augmented to more fields
	// OK
	Jstr newTabProperty = tp[0] + "!" + tp[1] + "!" + retention + "!" + tp[3];
	Jstr newHeader = sp[0] + "|" + sp[1] + "|" + sp[2] + "|" + newTabProperty + "|"; 
	Jstr allStr = newHeader + body;
	_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );
	writeSchemaText( dbtable, allStr );

	// record update retention
	record.setRetention( retention );
	_schemaRecMap->setValue( dbtable, record );
	prt(("s23374 _schemaRecMap->set dbtable=%s to new schemarecord\n", dbtable.s() ));
	return true;
}

// change parent table retention for the tick
bool JagSchema::changeTickRetention( const Jstr &dbtable, const Jstr &tick, const Jstr &retention )
{
	prt(("s203372 changeTickRetention dbtable=%s tick=[%s] retention=[%s]\n", dbtable.s(), tick.s(), retention.s() ));
	//sprintf( buf, "NN|%d|%d|%s|{", parseParam->keyLength, parseParam->valueLength, tableProperty.s() );

	JagSchemaRecord record(true);
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;
	bool rc = _schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( ! rc ) {
		return false;
	}

	Jstr sstr = record.getString();
	Jstr hdr = getHeader( sstr );
	Jstr body = getBody( sstr );

	JagStrSplit sp(hdr, '|');
	Jstr tableProperty = sp[3];
	//tableProperty = "polyDim!timeSeries!retain!0!"
	JagStrSplit tp( tableProperty, '!');
	Jstr dbtser = tp[1];

	// remove tick from dbtser: "15m_120m:3M_12M:1y_5y"
	JagStrSplit tsp( dbtser, ':');
	Jstr newTser;
	Jstr tickRetention;
	for ( int i = 0; i < tsp.size(); ++i ) {
		JagStrSplit rec( tsp[i], '_' );
		if ( rec[0] == tick ) {
			//continue;
			tickRetention = tick + "_" + retention; // modify
		} else {
			tickRetention = tsp[i]; // keep old one
		}

		if ( newTser.size() < 1 ) {
			newTser = tickRetention;
		} else {
			newTser += Jstr(":") + tickRetention;
		}
	}

	// need change if tableProperty is augmented to more fields
	// OK
	Jstr newTabProperty = tp[0] + "!" + newTser + "!" + tp[2] + "!" + tp[3];
	Jstr newHeader = sp[0] + "|" + sp[1] + "|" + sp[2] + "|" + newTabProperty + "|"; 
	Jstr allStr = newHeader + body;
	_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );
	writeSchemaText( dbtable, allStr );

	// record update time series
	record.setTimeSeries( newTser );
	_schemaRecMap->setValue( dbtable, record );
	prt(("s23371 _schemaRecMap->set dbtable=%s to new schemarecord\n", dbtable.s() ));
	return true;
}


// return true for OK; false for error
bool JagSchema::addOrRenameColumn( const Jstr &dbtable, const JagParseParam *parseParam )
{
	//prt(("s5590 JagSchema::addOrRenameColumn dbtable=[%s] ...\n", dbtable.s() ));
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;
	Jstr oldName, newName;

	JagSchemaRecord record(true);
	_schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( parseParam->createAttrVec.size() < 1 ) {
		oldName = parseParam->objectVec[0].colName;
		newName = parseParam->objectVec[1].colName;
		record.renameColumn( AbaxString(oldName), AbaxString(newName) );
	} else {
		record.addValueColumnFromSpare( parseParam->createAttrVec[0].objName.colName, parseParam->createAttrVec[0].type,
										parseParam->createAttrVec[0].length, parseParam->createAttrVec[0].sig );
	}
	// first, remove old column names from defvalMap before setValue of schemaMap and recordMap
	setupDefvalMap( dbtable, NULL, 1 );
	_schemaRecMap->setValue( AbaxString(dbtable), record );
	Jstr sstr = record.getString(); // new modified string
	_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );

	// disk array update
	int offset = 0;
	JagDBPair retpair;
	char *keybuf = (char*)jagmalloc(KEYLEN+1);
	memset(keybuf, '\0', KEYLEN+1);
	strcpy(keybuf, dbtable.c_str());
	JagFixString key(keybuf, KEYLEN, KEYLEN );

	char *valbuf = (char*)jagmalloc(VALLEN+1);
	memset( valbuf, 0, VALLEN+1);
	JagFixString value( valbuf, VALLEN, VALLEN );
	JagDBPair pair( key, value );

	if ( _schema->get( pair ) ) {
		if ( parseParam->createAttrVec.size() < 1 ) {
			Jstr cstr = Jstr(":") + oldName + ":";  // find oldName
			const char *p = strstr( pair.value.c_str(), cstr.c_str() );
			if ( p ) {
				// default values of column
				if ( offset+newName.size()-oldName.size()+pair.value.size() > VALLEN ) {
					free( keybuf );
					free( valbuf );
					return false;
				}
				// copy items before cstr
				memcpy( valbuf+offset, pair.value.c_str(), p-pair.value.c_str() );
				offset += p-pair.value.c_str();
				valbuf[offset] = ':';
				++offset;
				memcpy( valbuf+offset, newName.c_str(), newName.size() ); // insert newName
				offset += newName.size();
				valbuf[offset] = ':';
				++offset;
				p += 1 + oldName.size() + 1;
				memcpy( valbuf+offset, p, pair.value.c_str()+pair.value.size()-p ); // save items after oldName
			} else {
				//prt(("s9348 error find [%s]\n", cstr.s() ));
				memcpy( valbuf+offset, pair.value.c_str(), pair.value.size() );  // oldName not found, copy orginal data
			}
		} else {
			memcpy( valbuf+offset, pair.value.c_str(), pair.value.size() );
			if ( parseParam->createAttrVec[0].defValues.size() > 0 ) {
				if ( offset+1+parseParam->createAttrVec[0].objName.colName.size()+1+
						parseParam->createAttrVec[0].defValues.size() > VALLEN ) {
					free( keybuf );
					free( valbuf );
					return false;
				}
				offset += pair.value.size();
				valbuf[offset] = ':';
				++offset;
				memcpy( valbuf+offset, parseParam->createAttrVec[0].objName.colName.c_str(), 
						parseParam->createAttrVec[0].objName.colName.size() );
				offset += parseParam->createAttrVec[0].objName.colName.size();
				valbuf[offset] = '=';
				++offset;
				memcpy( valbuf+offset, parseParam->createAttrVec[0].defValues.c_str(), 
						parseParam->createAttrVec[0].defValues.size() );
				offset += parseParam->createAttrVec[0].defValues.size();
			}
		}
		value = JagFixString( valbuf, VALLEN, VALLEN );
	}

	// then, add new column names to defvalMap after setValue of schemaMap and recordMap
	setupDefvalMap( dbtable, valbuf, 0 );
	JagDBPair pair2( key, value );
	_schema->set( pair2 );

	//prt(("s2038 writeSchemaText() ... keybuf=[%s] sstr=[%s]\n", keybuf, sstr.s() ));
	writeSchemaText( keybuf, sstr );
	free( keybuf );
	free( valbuf );
	return true;
}

// set properties of column
// return true OK; false for error
bool JagSchema::setColumn( const Jstr &dbtable, const JagParseParam *parseParam )
{
	//prt(("s5591 JagSchema::setColumn dbtable=[%s] ...\n", dbtable.s() ));
	Jstr colName, attr, newValue;
	Jstr tabattr = parseParam->objectVec[0].colName;
	newValue = parseParam->value;
	JagStrSplit ta( tabattr, ':' );
	if ( ta.size() < 2 ) return false;
	colName = ta[0];
	attr = ta[1];
	JagSchemaRecord record(true);
	bool rc;

	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK ); JAG_OVER;

	rc = _schemaRecMap->getValue( AbaxString(dbtable), record );
	//prt(("s1338 setColumn colName=[%s] attr=[%s] newValue=[%s] rc=%d\n", colName.c_str(), attr.s(), newValue.s(), rc ));
	if ( ! rc ) {
		return false;
	}

	record.setColumn( AbaxString(colName), AbaxString(attr), AbaxString(newValue) );
	_schemaRecMap->setValue( AbaxString(dbtable), record );
	Jstr sstr = record.getString();
	_recordMap->setValue( AbaxString(dbtable), AbaxString( sstr ) );
	//prt(("s5203 sstr=[%s]\n",sstr.s() ));
	writeSchemaText( dbtable, sstr );
	return true;
}

bool JagSchema::checkSpareRemains( const Jstr &dbtable, const JagParseParam *parseParam )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;
	JagSchemaRecord record(true);
	_schemaRecMap->getValue( AbaxString(dbtable), record );
	if ( parseParam->createAttrVec[0].length > (*record.columnVector)[record.columnVector->size()-1].length ) {
		return 0;
	}
	return 1;
}

/***
return type of object
#define JAG_MEMTABLE_TYPE       10
#define JAG_CHAINTABLE_TYPE     20
#define JAG_TABLE_TYPE          30
***/
int JagSchema::objectType( const Jstr &dbtable ) const 
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	return _objectTypeNoLock( dbtable );
}

/***
return type of object
#define JAG_MEMTABLE_TYPE       10
#define JAG_CHAINTABLE_TYPE     20
#define JAG_TABLE_TYPE          30
***/
int JagSchema::_objectTypeNoLock( const Jstr &dbtable ) const 
{
	AbaxString sstr;
	_recordMap->getValue( dbtable, sstr );
	// printf("s4418 JagSchema::isMemTable dbtable=[%s] sstr=[%s]\n", dbtable.c_str(), sstr.c_str() );
	if ( sstr.size()>=2 && 'M' == *( sstr.c_str()+1 ) ) {
		return JAG_MEMTABLE_TYPE ;
	} else if ( sstr.size()>=2 && 'C' == *( sstr.c_str()+1 ) ) {
		return JAG_CHAINTABLE_TYPE;
	} else {
		return JAG_TABLE_TYPE;
	}
}

int JagSchema::isMemTable( const Jstr &dbtable ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	AbaxString sstr;
	_recordMap->getValue( dbtable, sstr );
	// printf("s4418 JagSchema::isMemTable dbtable=[%s] sstr=[%s]\n", dbtable.c_str(), sstr.c_str() );
	if ( sstr.size()>=2 && 'M' == *( sstr.c_str()+1 ) ) {
		return 1;
	} else {
		return 0;
	}
}

int JagSchema::isChainTable( const Jstr &dbtable ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	AbaxString sstr;
	_recordMap->getValue( dbtable, sstr );
	// printf("s4418 JagSchema::isMemTable dbtable=[%s] sstr=[%s]\n", dbtable.c_str(), sstr.c_str() );
	if ( sstr.size()>=2 && 'C' == *( sstr.c_str()+1 ) ) {
		return 1;
	} else {
		return 0;
	}
}

const JagSchemaRecord* JagSchema::getAttr( const Jstr & dbtable ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	return _schemaRecMap->getValue( dbtable );
}

bool JagSchema::getAttr( const Jstr & dbtable, AbaxString & keyInfo ) const 
{ 
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	return _recordMap->getValue( AbaxString(dbtable), keyInfo ); 
}

bool JagSchema::getAttrDefVal( const Jstr & dbtable, Jstr & keyInfo ) const 
{ 
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	return _defvalMap->getValue( dbtable, keyInfo ); 
}

// return "key1=val1|key2=val2|..."
Jstr JagSchema::getAllDefVals() const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	return _defvalMap->getKVStrings();
}

bool JagSchema::existAttr( const Jstr & pathName ) const 
{ 
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	return _recordMap->keyExist( AbaxString(pathName) ); 
}

const JagSchemaRecord *JagSchema
::getOneIndexAttr( const Jstr &dbName, const Jstr &indexName, Jstr &tabPathName ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK ); JAG_OVER;
	const JagSchemaRecord *rec = NULL;

	// use memory to read instead of disk
    jagint len = _recordMap->arrayLength();
    const AbaxPair<AbaxString, AbaxString> *arr = _recordMap->array();
    for ( jagint i = 0; i < len; ++i ) {
    	if ( _recordMap->isNull(i) ) continue;
		
		JagStrSplit oneSplit( arr[i].key.c_str(), '.' );
		if ( oneSplit.length()>=3 && dbName == oneSplit[0] && indexName == oneSplit[2] ) {
			rec = _schemaRecMap->getValue( arr[i].key );
			tabPathName = oneSplit[0] + "." + oneSplit[1];
			// printf("s3013 tabPathName=[%s] _schemaRecMap->getValue() rc=%d\n", tabPathName.c_str(), rc );
			break;
		}
    }
	return rec;
}

// dbtable can be dbname  -- all table/index for this db
// dbtable can db.tab  -- all indexes for db.tab
// dbtable can be NULL -- all table/index for all dbs
JagVector<AbaxString>* JagSchema
::getAllTablesOrIndexesLabel( int inObjType, const Jstr &dbtable, const Jstr &like ) const
{
	// JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;
	//printf("s0029 after :WRITE_LOCK \n");
	// fflush( stdout );
	if ( ! _schema ) {
		printf("s4091 !!!!!!!!!!! This should not happen\n");
		return NULL;
	}

	JagVector<AbaxString> *vec = new JagVector<AbaxString>();
	// use memory to read instead of disk
    jagint len = _recordMap->arrayLength();
    const AbaxPair<AbaxString, AbaxString> *arr = _recordMap->array();

	JagArray<AbaxPair<AbaxString,char>> xarr;
    for ( jagint i = 0; i < len; ++i ) {
    	if ( _recordMap->isNull(i) ) continue;

		if ( like.size() > 0 ) {
			JagStrSplit sp(arr[i].key.c_str(), '.');
			if ( sp.length() > 1 ) {
				if ( ! likeMatch( sp[1], like ) ) { continue; }
			}
		}

		if ( dbtable.length() > 0 ) {
			if ( 0 == strncmp( arr[i].key.c_str(), dbtable.c_str(), dbtable.length()) ) {
				xarr.insert( AbaxPair<AbaxString,char> (arr[i].key, '1') );
			}
		} else {
			xarr.insert( AbaxPair<AbaxString,char> (arr[i].key, '1') );
		}
    }

	const AbaxPair<AbaxString,char> *parr = xarr.array();
	len = xarr.size();
	AbaxString rec;
	int objtype;
    for ( jagint i = 0; i < len; ++i ) {
		if ( xarr.isNull(i) ) continue;
		rec = "";
		objtype = _objectTypeNoLock( parr[i].key.c_str() );
		if ( inObjType == JAG_TABLE_TYPE ) {
    		if ( JAG_MEMTABLE_TYPE == objtype ) {
    			rec =  parr[i].key;
    		} else if ( JAG_TABLE_TYPE == objtype ) {
    			rec =  parr[i].key;
    		}
		} else if (  inObjType == JAG_CHAINTABLE_TYPE ) {
    		if ( JAG_CHAINTABLE_TYPE == objtype  ) {
    			rec =  parr[i].key;
			}
		} else if ( inObjType == JAG_TABLE_OR_CHAIN_TYPE ) {
			if ( JAG_MEMTABLE_TYPE == objtype || JAG_TABLE_TYPE == objtype 
			     || JAG_CHAINTABLE_TYPE == objtype ) {
    			rec =  parr[i].key;
			}
		}

		if ( rec.size() > 0 ) {
			vec->append( rec );
		}
	}
	
	return vec;
}

// dbtable can be dbname  -- all table/index for this db
// dbtable can db.tab  -- all indexes for db.tab
// dbtable can be NULL -- all table/index for all dbs
JagVector<AbaxString>* JagSchema::getAllTablesOrIndexes( const Jstr &dbtable, const Jstr &like ) const
{
	// JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;
	//printf("s0029 after :WRITE_LOCK \n");
	// fflush( stdout );
	if ( ! _schema ) {
		printf("s4091 !!!!!!!!!!! This should not happen\n");
		return NULL;
	}

	JagVector<AbaxString> *vec = new JagVector<AbaxString>();
	// use memory to read instead of disk
    jagint len = _recordMap->arrayLength();
    const AbaxPair<AbaxString, AbaxString> *arr = _recordMap->array();
	JagArray<AbaxPair<AbaxString,char>> xarr;
    for ( jagint i = 0; i < len; ++i ) {
    	if ( _recordMap->isNull(i) ) continue;

		//prt(("s2339 arr[i].key=[%s]\n", arr[i].key.c_str() ));
		if ( like.size() > 0 ) {
			JagStrSplit sp(arr[i].key.c_str(), '.');
			if ( sp.length() > 1 ) {
				if ( ! likeMatch( sp[1], like ) ) {
					continue;
				}
			}
		}

		if ( dbtable.length() > 0 ) {
			if ( 0 == strncmp( arr[i].key.c_str(), dbtable.c_str(), dbtable.length()) ) {
				xarr.insert( AbaxPair<AbaxString,char> (arr[i].key, '1') );
			}
		} else {
			xarr.insert( AbaxPair<AbaxString,char> (arr[i].key, '1') );
		}
    }

	const AbaxPair<AbaxString,char> *parr = xarr.array();
	len = xarr.size();
    for ( jagint i = 0; i < len; ++i ) {
		if ( xarr.isNull(i) ) continue;
		vec->append( parr[i].key );
	}
	
	return vec;
}

JagVector<AbaxString>* JagSchema::getAllIndexes( const Jstr &dbname,  const Jstr &like ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );
	JAG_OVER;

	JagVector<AbaxString> *vec = new JagVector<AbaxString>();
	Jstr rec;
	char  *buf = (char*)jagmalloc(KVLEN+1);
	char  *keybuf = (char*)jagmalloc(KEYLEN+1);
	memset( buf, '\0', KVLEN+1 );
	memset( keybuf, '\0', KEYLEN+1 );
	jagint getpos;
	jagint length = _schema->getLength();

	JagSingleBuffReader nti( _schema->getFD(), length, KEYLEN, VALLEN, 0, 0, 1 );
	AbaxString nm;
	JagArray<AbaxPair<AbaxString,char>> xarr;
	while ( nti.getNext(buf, KVLEN, getpos) ) {
			memcpy( keybuf, buf, KEYLEN );
			rec = keybuf;
			JagStrSplit sp( rec, '.' );
			if ( sp.length() < 3 ) continue;
			nm = sp[2] + " (" + sp[0] + "." + sp[1] + ")";

			if ( like.size() > 0 ) {
				if ( ! likeMatch( sp[1], like ) ) { continue; }
			}

			xarr.insert(AbaxPair<AbaxString,char> (nm, '1') );
			memset( keybuf, '\0', KEYLEN+1 );
	}
	free( buf );
	free( keybuf );

	const AbaxPair<AbaxString,char> *parr = xarr.array();
	for ( jagint i = 0; i < xarr.size(); ++i ) {
		if ( xarr.isNull(i) ) continue;
		vec->append( parr[i].key );
	}

	return vec;
}

Jstr JagSchema::getTableName( const Jstr &dbName, const Jstr &indexName ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );

	Jstr  tabName;
	AbaxString res;
	AbaxString dbidx = dbName + "." + indexName;
	if ( _tableIndexMap && _tableIndexMap->getValue( dbidx, res ) ) {
		tabName = res.c_str();
		// prt(("s2039 got tabName=[%s] for indexname=[%s]\n", tabName.c_str(), indexName.c_str() ));
		return tabName;
	}

	// for table, it will not get here
	return tabName;
}

Jstr JagSchema::getTableNameScan( const Jstr &dbName, const Jstr &indexName ) const
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );

	Jstr  tabName;

    jagint len = _recordMap->arrayLength();
    const AbaxPair<AbaxString, AbaxString> *arr = _recordMap->array();
    for ( jagint i = 0; i < len; ++i ) {
    	if ( _recordMap->isNull(i) ) continue;
		
		JagStrSplit oneSplit( arr[i].key.c_str(), '.' );
		if ( oneSplit.length()>=3 && dbName == oneSplit[0] && indexName == oneSplit[2] ) {
			tabName = oneSplit[1];
		}
    }
	return tabName;
}

Jstr JagSchema::readSchemaText( const Jstr &key ) const
{
	Jstr fpath = _cfg->getJDBDataHOME( _replicateType ) + "/system/schema/" + key;
	Jstr text;
	JagFileMgr::readTextFile( fpath, text );
	return text;
}

void JagSchema::writeSchemaText( const Jstr &key, const Jstr &text )
{
	//prt(("s87273 writeSchemaText key=[%s] text=[%s]\n", key.s(), text.s() ));
	Jstr fpath = _cfg->getJDBDataHOME( _replicateType ) + "/system/schema/" + key;
	JagFileMgr::writeTextFile( fpath, text );
	// prt(("s8831 writeSchemaText fpath=[%s] text=[%s]\n", fpath.c_str(), text.c_str() ));
}

void JagSchema::removeSchemaFile( const Jstr &key )
{
	Jstr fpath = _cfg->getJDBDataHOME( _replicateType ) + "/system/schema/" + key;
	jagunlink( fpath.c_str() );
	// prt(("s3719 remove schema file [%s]\n", fpath.c_str() ));
}

// static 
Jstr JagSchema::getDatabases( const JagCfg *cfg, int replicateType )
{
	Jstr res, sdir, fpath;
	DIR             *dp;
	struct dirent   *dirp;
	struct stat     statbuf;

	if ( cfg ) {
		fpath = cfg->getJDBDataHOME( replicateType );
	} else {
		JagCfg cfg2;
		fpath = cfg2.getJDBDataHOME( replicateType );
	}

	if ( NULL == (dp=opendir( fpath.c_str() )) ) { return res; }
    while( NULL != (dirp=readdir(dp)) ) {
        if ( 0==strcmp(dirp->d_name, ".") || 0==strcmp(dirp->d_name, "..") ) {
            continue;
        }

		if ( '.' == dirp->d_name[0] ) { continue; }
		sdir = fpath + "/" + Jstr(dirp->d_name);
		if ( stat( sdir.c_str(), &statbuf) < 0 ) { continue; }
		if ( ! S_ISDIR( statbuf.st_mode ) ) { continue; }
        res += dirp->d_name;
		res += "\n";
    }
	closedir( dp );
	return res;
}

 bool JagSchema::dbTableExist( const Jstr &dbname, const Jstr &tabname )
 {
 	if ( ! _recordMap ) return false;

	AbaxString k = AbaxString(dbname) + "." + tabname;
 	return _recordMap->keyExist( k );
 }

const JagColumn* JagSchema::getColumn( const Jstr &dbname, const Jstr &objname,
                            const Jstr &colname )
{
	Jstr key = dbname + "." + objname + "." + colname;
	// prt(("s7220 JagSchema::getColumn key=[%s]\n", key.c_str() ));
	return _columnMap->getValue( key );
}

// dbobj:  db.tab  or db.idx
int JagSchema::addToColumnMap( const Jstr& dbobj, const JagSchemaRecord &record )
{
	// get each column and add db.tab.column  -> JagColumn
	// get each column and add db.idx.column  -> JagColumn
	Jstr key;
	for ( int i = 0; i < record.columnVector->size(); ++i ) {
		key = dbobj + "." + (*(record.columnVector))[i].name.c_str();
		_columnMap->addKeyValue( key, (*(record.columnVector))[i] );
	}

	return 1;
}

// dbobj:  db.tab  or db.idx
int JagSchema::removeFromColumnMap( const Jstr& dbtabobj, const Jstr& dbobj )
{
	// get each column and add db.tab.column  -> JagColumn
	// get each column and add db.idx.column  -> JagColumn
	const JagSchemaRecord *record = _schemaRecMap->getValue( dbtabobj );
	if ( ! record ) {
		// prt(("s3003 JagSchema::removeFromColumnMap error get _schemaRecMap->getValue( %s )\n", dbtabobj.c_str() ));
		return 0;
	} else {
		// prt(("s1120 _schemaRecMap->getValue(%s) OK\n",  dbtabobj.c_str() ));
	}

	Jstr key;
	for ( int i = 0; i < record->columnVector->size(); ++i ) {
		key = dbobj + "." + (*(record->columnVector))[i].name.c_str();
		_columnMap->removeKey( key );
		// prt(("s2039 _columnMap->removeKey(%s) \n", key.c_str() ));
	}

	return 1;
}

bool JagSchema::isIndexCol( const Jstr &dbname, const Jstr &colName )
{
	JAG_BLURT JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );
	AbaxString dbidx = dbname + "." + colName;
	if ( _tableIndexMap && _tableIndexMap->getValue( dbidx ) ) {
		return true;
	}

	return false;
}

