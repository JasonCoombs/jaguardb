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

#include <malloc.h>
#include <JagTable.h>
#include <JagIndex.h>
#include <JaguarCPPClient.h>
#include <JagDBServer.h>
#include <JagUtil.h>
#include <JagUUID.h>
#include <JagIndexString.h>
#include <JagDiskArrayServer.h>
#include <JagDiskArrayClient.h>
#include <JagSession.h>
#include <JagFastCompress.h>
#include <JagDataAggregate.h>
#include <JagDBConnector.h>
#include <JagParallelParse.h>
#include <JagBuffBackReader.h>
#include <JagServerObjectLock.h>
#include <JagStrSplitWithQuote.h>
#include <JagParser.h>
#include <JagLineFile.h>

JagTable::JagTable( int replicateType, const JagDBServer *servobj, const Jstr &dbname, const Jstr &tableName, 
					  const JagSchemaRecord &record, bool buildInitIndex ) : _tableRecord(record), _servobj( servobj )
{
	_cfg = _servobj->_cfg;
	_dbname = dbname;
	_tableName = tableName;
	_dbtable = dbname + "." + tableName;
	_darrFamily = NULL;
	_tablemap = NULL;
	_schAttr = NULL;
	_tableschema = NULL;
	_indexschema = NULL;
	_numKeys = 0;
	_numCols = 0;
	_actualInsert = 0;
	_replicateType = replicateType;
	_isExporting = 0;
	KEYLEN = 0;
	VALLEN = 0;
	KEYVALLEN = 0;
	_schemaRefreshTime = 0;
	init( buildInitIndex );
}

JagTable::~JagTable ()
{
	if ( _darrFamily ) {
		delete _darrFamily;
		_darrFamily = NULL;
	}
	
	if ( _tablemap ) {
		delete _tablemap;
		_tablemap = NULL;
	}	

	if ( _schAttr ) {
		delete [] _schAttr;
		_schAttr = NULL;
	}
}

void JagTable::init( bool buildInitIndex ) 
{
	if ( NULL != _darrFamily ) { return; }
	
	bool rc;
	Jstr dbpath, fpath, dbcolumn;
	Jstr rdbdatahome = _cfg->getJDBDataHOME( _replicateType );
	dbpath = rdbdatahome + "/" + _dbname;

	// new path
	fpath = dbpath +  "/" + _tableName + "/" + _tableName;
	JagFileMgr::makedirPath( dbpath +  "/" + _tableName );
	JagFileMgr::makedirPath( dbpath +  "/" + _tableName + "/files" );

	Jstr oldpath = dbpath +  "/" + _tableName;
	Jstr oldpathname = oldpath + ".jdb";
	Jstr newfpathname = fpath + ".jdb";
	if ( JagFileMgr::exist( oldpathname ) ) {
		jagrename( oldpathname.c_str(), newfpathname.c_str() );
		// prt(("s1028 rename( %s ==> %s )\n", oldpathname.c_str(), newfpathname.c_str() ));

		oldpathname = oldpath + ".bid";
		newfpathname = fpath + ".bid";
		if ( JagFileMgr::exist( oldpathname ) ) {
			jagrename( oldpathname.c_str(), newfpathname.c_str() );
			// prt(("s1029 rename( %s ==> %s )\n", oldpathname.c_str(), newfpathname.c_str() ));
		} else {
			// prt(("s1936 bid oldpathname=[%s] not found\n", oldpathname.c_str() ));
		}
	}

	_darrFamily = new JagDiskArrayFamily ( _servobj, fpath, &_tableRecord, buildInitIndex );

	KEYLEN = _tableRecord.keyLength;
	VALLEN = _tableRecord.valueLength;
	KEYVALLEN = KEYLEN + VALLEN;
	_numCols = _tableRecord.columnVector->size();	
	_schAttr = new JagSchemaAttribute[_numCols];	
	_schAttr[0].record = _tableRecord;
	_tablemap = newObject<JagHashStrInt>();

	_bufferRows = jagatoll((_cfg->getValue("BUFFER_LOAD_SIZE", "300")).c_str());
	_bufferRows = ( _bufferRows * 1024 * 1024 ) / KEYVALLEN;
	
	// set up replicate type for later use
	if ( 0 == _replicateType ) {
		_tableschema = _servobj->_tableschema;
		_indexschema = _servobj->_indexschema;
	} else if ( 1 == _replicateType ) {
		_tableschema = _servobj->_prevtableschema;
		_indexschema = _servobj->_previndexschema;
	} else if ( 2 == _replicateType ) {
		_tableschema = _servobj->_nexttableschema;
		_indexschema = _servobj->_nextindexschema;
	}

	_objectType = _tableschema->objectType( _dbtable );
	setupSchemaMapAttr( _numCols );
}

// build indexlist
void JagTable::buildInitIndexlist()
{
	int rc; Jstr dbtabindex, dbindex; JagIndex *pindex = NULL;
	const JagSchemaRecord *irecord;
	JagVector<Jstr> vec;

	rc = _indexschema->getIndexNamesFromMem( _dbname, _tableName, vec );
    for ( int i = 0; i < vec.length(); ++i ) {
		dbtabindex = _dbtable + "." + vec[i];
		dbindex = _dbname + "." + vec[i];

		AbaxBuffer bfr;
		AbaxPair<AbaxString, AbaxBuffer> pair( AbaxString(dbindex), bfr );
		irecord = _indexschema->getAttr( dbtabindex );
		if ( irecord ) {
			if ( _servobj->_objectLock->writeLockIndex( JAG_CREATEINDEX_OP, _dbname, _tableName, vec[i],
					_tableschema, _indexschema, _replicateType, 1 ) ) {
				_servobj->_objectLock->writeUnlockIndex( JAG_CREATEINDEX_OP, _dbname, _tableName, vec[i],
					_tableschema, _indexschema, _replicateType, 1 );
			}		
			_indexlist.append( vec[i] );
			// prt(("s3830 _indexlist.append(%s)\n", vec[i].c_str() ));
		} 
    }
}

bool JagTable::getPair( JagDBPair &pair ) 
{ 
	if ( _darrFamily->get( pair ) ) return true;
	else return false;
}

// 0: fail to insert
// 1: success to insert
int JagTable::parsePair( int tzdiff, JagParseParam *parseParam, JagVector<JagDBPair> &retpair, Jstr &errmsg )
{
	int getpos = 0;
	int rc, rc2;
	
	Jstr dbcolumn, dbtab = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	if ( _numCols < parseParam->otherVec.size() ) {
		prt(("s4893 Error JagTable::parsePair _numCols=%d parseParam->otherVec.size()=%d\n", _numCols, parseParam->otherVec.size() ));
		errmsg = "E3143  _numCols-1 != parseParam->otherVec.size";
		return 0;
	}

	// debug
	#ifdef DEVDEBUG 
	prt(("\ns2090 _tableRecord.columnVector:\n" ));
	for ( int i = 0; i < (*_tableRecord.columnVector).size(); ++i ) {
		prt(("s1014 colvec i=%d name=[%s] type=[%s] subcol=%d\n", 
			  i, (*_tableRecord.columnVector)[i].name.c_str(), (*_tableRecord.columnVector)[i].type.c_str(),
			  (*_tableRecord.columnVector)[i].issubcol ));
	}
	prt(("\n" ));

	prt(("s2091 initial parseParam->otherVec:\n" ));
	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		prt(("s1015  i=%d othervec name=[%s] value=[%s] type=[%s] point.x=[%s] point.y=[%s] line.point[0].x=[%s] line.point[0].y=[%s]\n", 
			i, parseParam->otherVec[i].objName.colName.c_str(), parseParam->otherVec[i].valueData.c_str(), 
			parseParam->otherVec[i].type.c_str(), parseParam->otherVec[i].point.x, parseParam->otherVec[i].point.y,
			parseParam->otherVec[i].linestr.point[0].x, parseParam->otherVec[i].linestr.point[0].y ));
			// i, parseParam->otherVec[i].objName.colName.c_str(), parseParam->otherVec[i].valueData.c_str(), _schAttr[i].type.c_str() ));
	}
	prt(("\n" ));
	#endif

	// add extra blanks in  parseParam->otherVec if it has fewer items than _tableRecord.columnVector
	#ifdef DEVDEBUG
	prt(("s8613 columnVector.size=%d otherVec.size=%d\n", (*_tableRecord.columnVector).size(), parseParam->otherVec.size() ));
	prt(("s8614 otherVec: print\n" ));
	for ( int k = 0; k < parseParam->otherVec.size(); ++k ) {
		prt(("s8614 k=%d other valuedata=[%s] issubcol=%d linestr.size=%d \n",
			k, parseParam->otherVec[k].valueData.c_str(), parseParam->otherVec[k].issubcol, parseParam->otherVec[k].linestr.size()));
	}
	#endif

	// find bbx coords
	double dblmax = LONG_MAX;
	double dblmin = LONG_MIN;
	double xmin=dblmax, ymin=dblmax, zmin=dblmax, xmax=dblmin, ymax=dblmin, zmax=dblmin;
	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		rc = 0;
		prt(("s4928 parseParam->otherVec[%d].type=[%s]\n", i, parseParam->otherVec[i].type.s() ));
		if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_LINESTRING 
		     || parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTIPOINT  ) {
			rc = JagParser::getLineStringMinMax( ',', parseParam->otherVec[i].valueData.c_str(), xmin, ymin, xmax, ymax );
		} else if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_LINESTRING3D 
					|| parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			rc = JagParser::getLineString3DMinMax(',', parseParam->otherVec[i].valueData.c_str(), xmin, ymin, zmin, xmax, ymax, zmax );
		} else if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_POLYGON
		            || parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTILINESTRING ) {
			rc = JagParser::getPolygonMinMax(  parseParam->otherVec[i].valueData.c_str(), xmin, ymin, xmax, ymax );
		} else if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_POLYGON3D
		            || parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			rc = JagParser::getPolygon3DMinMax( parseParam->otherVec[i].valueData.c_str(), xmin, ymin, zmin, xmax, ymax, zmax );
		} else if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			rc = JagParser::getMultiPolygonMinMax(  parseParam->otherVec[i].valueData.c_str(), xmin, ymin, xmax, ymax );
		} else if ( parseParam->otherVec[i].type == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			rc = JagParser::getMultiPolygon3DMinMax(  parseParam->otherVec[i].valueData.c_str(), xmin, ymin, zmin, xmax, ymax, zmax );
		}

		if ( rc < 0 ) {
			char ebuf[256];
			sprintf( ebuf, "E3145 error in input data type=%s column i=%d\n", parseParam->otherVec[i].type.c_str(), i );
			errmsg = ebuf;
			return 0;
		}
	}
	//prt(("s8021 xmin=%0.3f ymin=%.3f zmin=%.3f\n", xmin, ymin, zmin ));
	//prt(("s8022 xmax=%0.3f ymax=%.3f zmax=%.3f\n", xmax, ymax, zmax ));

	// just need to make up mathcing number of columns, data is not important
	if ( (*_tableRecord.columnVector).size() > parseParam->otherVec.size() ) {
		JagVector<OtherAttribute> otherVec;

		int j = -1;
		Jstr colType;
		for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
			#ifdef DEVDEBUG
			prt(("s6675 parseParam->hasPoly=%d _tableRecord.lastKeyColumn=%d parseParam->polyDim=%d\n",
				  parseParam->hasPoly, _tableRecord.lastKeyColumn, parseParam->polyDim ));
			#endif

			if ( parseParam->hasPoly && j == _tableRecord.lastKeyColumn ) {
				if ( 2 == parseParam->polyDim ) {
					// appendOther( otherVec, 7, 0 );  // bbox(4) + id col i = 7  no min/max z-dim
					appendOther( otherVec, JAG_POLY_HEADER_COLS, 0 );  // bbox(4) + id col i = 7  no min/max z-dim
					//prt(("s6760 i=%d j=%d appendOther( otherVec, JAG_POLY_HEADER_COLS, 0 );\n", i, j));
				} else {
					appendOther( otherVec, JAG_POLY_HEADER_COLS, 0 );  // bbox(6) + id col i = 9
					//prt(("s6761 i=%d j=%d appendOther( otherVec, JAG_POLY_HEADER_COLS, 0 );\n", i, j));
				}
				//prt(("s6761 i=%d j=%d appendOther( otherVec, JAG_POLY_HEADER_COLS, 0 );\n", i, j));
			}

			colType = parseParam->otherVec[i].type;
			prt(("s6761 i=%d colType=[%s]\n", i, colType.c_str() ));
			if ( colType == JAG_C_COL_TYPE_POINT  ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_POINT_DIM );  // x y
				j += JAG_POINT_DIM;
			} else if ( colType == JAG_C_COL_TYPE_POINT3D) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_POINT3D_DIM );
				j += JAG_POINT3D_DIM;
			} else if ( colType == JAG_C_COL_TYPE_CIRCLE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_CIRCLE_DIM );  // x y r
				j += JAG_CIRCLE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_SQUARE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_SQUARE_DIM );  // x y r nx
				j += JAG_SQUARE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_SPHERE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_SPHERE_DIM );  // x y z r
				j += JAG_SPHERE_DIM;
				// x y z radius
			} else if ( colType == JAG_C_COL_TYPE_CIRCLE3D
						|| colType == JAG_C_COL_TYPE_SQUARE3D
						|| colType == JAG_C_COL_TYPE_CUBE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_CIRCLE3D_DIM );  // x y z r nx ny
				j += JAG_CIRCLE3D_DIM;
				// x y z radius
			} else if (  colType == JAG_C_COL_TYPE_LINE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_LINE_DIM );  // x1 y1  x2 y2 
				j += JAG_LINE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_RECTANGLE 
						|| colType == JAG_C_COL_TYPE_ELLIPSE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_RECTANGLE_DIM );  // x y a b nx
				j += JAG_RECTANGLE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_LINE3D_DIM );  // x1 y1 z1 x2 y2 z2
				j += JAG_LINE3D_DIM;
			} else if (  colType == JAG_C_COL_TYPE_LINESTRING ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_LINESTRING_DIM );  // i x1 y1
				j += JAG_LINESTRING_DIM;
			} else if (  colType == JAG_C_COL_TYPE_LINESTRING3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_LINESTRING3D_DIM );  // i x1 y1 z1
				j += JAG_LINESTRING3D_DIM;
			} else if (  colType == JAG_C_COL_TYPE_MULTIPOINT ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_MULTIPOINT_DIM );  // i x1 y1
				j += JAG_MULTIPOINT_DIM;
			} else if (  colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_MULTIPOINT3D_DIM );  // i x1 y1 z1
				j += JAG_MULTIPOINT3D_DIM;
			} else if (  colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_POLYGON_DIM );  // i x1 y1
				j += JAG_POLYGON_DIM;
			} else if (  colType == JAG_C_COL_TYPE_POLYGON3D || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_POLYGON3D_DIM );  // i x1 y1
				j += JAG_POLYGON3D_DIM;
			} else if (  colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_MULTILINESTRING_DIM );  // i x1 y1
				j += JAG_MULTILINESTRING_DIM;
			} else if (  colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_MULTILINESTRING3D_DIM );  // i x1 y1
				j += JAG_MULTILINESTRING3D_DIM;
			} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_TRIANGLE_DIM );  // x1 y1 x2 y2 x3 y3 
				j += JAG_TRIANGLE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_ELLIPSOID ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_ELLIPSOID_DIM );  // x y z width depth height nx ny
				j += JAG_ELLIPSOID_DIM;
			} else if (  colType == JAG_C_COL_TYPE_BOX ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_BOX_DIM );  // x y z width depth height nx ny
				j += JAG_BOX_DIM;
			} else if ( colType == JAG_C_COL_TYPE_CYLINDER
			            || colType == JAG_C_COL_TYPE_CONE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_CONE_DIM );  // x y z r height  nx ny
				j += JAG_CONE_DIM;
			} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_TRIANGLE3D_DIM );  //x1y1z1 x2y2z2 x3y3z3
				j += JAG_TRIANGLE3D_DIM;
			} else if ( colType == JAG_C_COL_TYPE_ELLIPSE3D || colType == JAG_C_COL_TYPE_RECTANGLE3D ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, JAG_ELLIPSE3D_DIM );  // x y z a b nx ny
				j += JAG_ELLIPSE3D_DIM;
			} else if ( colType == JAG_C_COL_TYPE_RANGE ) {
				otherVec.append(  parseParam->otherVec[i] );
				appendOther(  otherVec, 2 );  // begin end
				j += 2;
			} else {
				otherVec.append(  parseParam->otherVec[i] );
				// otherVec.append(  parseParam->otherVec[j] );
			}

			++j;
		}

		// prt(("s5022 parseParam->hasPoly=%d _tableRecord.lastKeyColumn=%d\n", parseParam->hasPoly, _tableRecord.lastKeyColumn ));
		parseParam->otherVec = otherVec;
	}

	// debug only
	#ifdef DEVDEBUG 
	prt(("\ns2093 final parseParam->otherVec:\n" ));
	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		prt(("s1015  i=%d final othervec name=[%s] value=[%s] type=[%s] point.x=[%s] point.y=[%s]\n", 
			i, parseParam->otherVec[i].objName.colName.c_str(), parseParam->otherVec[i].valueData.c_str(), 
			parseParam->otherVec[i].type.c_str(), parseParam->otherVec[i].point.x, parseParam->otherVec[i].point.y ));

		prt(("s1015  i=%d line.point[0].x=[%s] line.point[0].y=[%s]\n", 
			  i, parseParam->otherVec[i].linestr.point[0].x, parseParam->otherVec[i].linestr.point[0].y ));
		prt(("s1015  i=%d line.point[1].x=[%s] line.point[1].y=[%s]\n", 
			  i, parseParam->otherVec[i].linestr.point[1].x, parseParam->otherVec[i].linestr.point[1].y ));
	}
	prt(("\n" ));

	prt(("s8623 final columnVector.size=%d otherVec.size=%d\n", (*_tableRecord.columnVector).size(), parseParam->otherVec.size() ));
	prt(("s8624 otherVec: print\n" ));
	for ( int k = 0; k < parseParam->otherVec.size(); ++k ) {
		prt(("s8624 k=%d other valuedata=[%s] issubcol=%d linestr.size=%d\n",
			k, parseParam->otherVec[k].valueData.c_str(), parseParam->otherVec[k].issubcol, parseParam->otherVec[k].linestr.size() ));
	}
	#endif


	Jstr point, pointi, pointx, pointy, pointz, pointr, pointw, pointd, pointh, colname;
	Jstr pointx1, pointy1, pointz1;
	Jstr pointx2, pointy2, pointz2;
	Jstr pointx3, pointy3, pointz3;
	Jstr pointnx, pointny, colType;
	bool is3D = false, hasDoneAppend = false;
	char *tablekvbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset(tablekvbuf, 0, KEYVALLEN+1);
	int mlineIndex = 0;
	int getxmin, getymin, getzmin, getxmax , getymax , getzmax;
	int getid, getcol, getm, getn, geti, getx, gety, getz;


	int srvtmdiff  = _servobj->servtimediff;
	#ifdef DEVDEBUG
	prt(("s2503 otherVec.size()=%d\n", parseParam->otherVec.size() ));
	prt(("s2504 _tableRecord:\n" ));
	//_tableRecord.print();
	#endif

	Jstr lsuuid = _servobj->_jagUUID->getString();
	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		if ( parseParam->otherVec[i].issubcol ) { 
			#if 0
			prt(("s2003 i=%d parseParam->otherVec[i].issubcol is true name=[%s] value=[%s] skip\n", 
				i, parseParam->otherVec[i].objName.colName.c_str(), parseParam->otherVec[i].valueData.c_str() ));
			#endif
			continue; 
		}

		hasDoneAppend = false;
		colname = (*_tableRecord.columnVector)[i].name.c_str();
		dbcolumn = dbtab + "." + colname;
		rc = _tablemap->getValue(dbcolumn, getpos);
		if ( ! rc ) {
			free( tablekvbuf );
			errmsg = Jstr("E5003  _tablemap->getValue(") + dbcolumn + ") error i=" + intToStr(i);
			return 0;
		}
		//prt(("s5031 i=%d dbcolumn=[%s] getpos=%d\n", i, dbcolumn.c_str(), getpos ));

		OtherAttribute &otherAttr = parseParam->otherVec[i];
		#ifdef DEVDEBUG
		prt(("s7801 otherAttr=%0x  i=%d otherAttr col=[%s]  valueData=[%s] linestr.size=%d \n", 
				&otherAttr, i, otherAttr.objName.colName.c_str(), otherAttr.valueData.c_str(), otherAttr.linestr.size() ));
		#endif

		if ( *((*(_tableRecord.columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM_CHAR &&
				otherAttr.valueData.size() > 0 ) {
			rc2 = false;
			for ( int j = 0; j < _schAttr[getpos].enumList.length(); ++j ) {
				if ( strcmp( _schAttr[getpos].enumList[j].c_str(), otherAttr.valueData.c_str() ) == 0 ) {
					rc2 = true;
					break;
				}
			}
			if ( !rc2 ) {
				free( tablekvbuf );
				errmsg = "E2038 Enum data error";
				return 0;
			}			
		}

		if ( *((*(_tableRecord.columnVector))[i].spare+4) == JAG_CREATE_DEFINSERTVALUE &&
			otherAttr.valueData.size() < 1 ) {
			otherAttr.valueData = _schAttr[getpos].defValue;
		} 

		colType = (*_tableRecord.columnVector)[i].type;
		//prt(("s5051 other i=%d dbcolumn=[%s] colType=[%s]\n", i, dbcolumn.c_str(), colType.c_str() ));
		if ( colType == JAG_C_COL_TYPE_POINT ) {
			pointx = colname + ":x"; pointy = colname + ":y";
			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 						pointx.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
									_schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_RANGE ) {
			pointx = colname + ":begin"; pointy = colname + ":end";
			dbcolumn = dbtab + "." + pointx;
			//prt(("s9383 otherAttr.valueData=[%s]\n", otherAttr.valueData.c_str() ));
			char sep;
			if ( strchr( otherAttr.valueData.c_str(), ',') ) {
				sep = ',';
			} else {
				sep = ' ';
			}
			JagStrSplit sp(otherAttr.valueData, sep );

			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, sp[0].c_str(), errmsg, 
			 						pointx.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
									_schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, sp[1].c_str(), errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_POINT3D ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_CIRCLE || colType == JAG_C_COL_TYPE_SQUARE ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointr = colname + ":a"; 
			pointnx = colname + ":nx"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 						pointx.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
									_schAttr[getpos].sig, _schAttr[getpos].type );
				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				} 
				dbcolumn = dbtab + "." + pointr;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 						   pointr.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
									   _schAttr[getpos].sig, _schAttr[getpos].type );
				} 

				if ( colType == JAG_C_COL_TYPE_SQUARE ) {
    					dbcolumn = dbtab + "." + pointnx;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    				 					   pointnx.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
    									   _schAttr[getpos].sig, _schAttr[getpos].type );
    					} 
				}
			} 
		} else if ( colType == JAG_C_COL_TYPE_SPHERE || colType == JAG_C_COL_TYPE_CUBE ) {
			pointx = colname + ":x"; pointy = colname + ":y";
			pointz = colname + ":z"; pointr = colname + ":a"; 
			pointnx = colname + ":nx"; pointny = colname + ":ny"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointr;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointr.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if (  (*_tableRecord.columnVector)[i].type == JAG_C_COL_TYPE_CUBE ) {
					dbcolumn = dbtab + "." + pointnx;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 				pointnx.c_str(), _schAttr[getpos].offset, 
							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
					}

					dbcolumn = dbtab + "." + pointny;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
			 				pointny.c_str(), _schAttr[getpos].offset, 
							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
					}
				}

			}
		} else if ( colType == JAG_C_COL_TYPE_CIRCLE3D || colType == JAG_C_COL_TYPE_SQUARE3D ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
			pointr = colname + ":a"; 
			pointnx = colname + ":nx"; pointny = colname + ":ny"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointr;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointr.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointnx;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 			pointnx.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointny;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
			 			pointny.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType  == JAG_C_COL_TYPE_RECTANGLE || colType == JAG_C_COL_TYPE_ELLIPSE ) {
			pointx = colname + ":x"; pointy = colname + ":y";
			pointw = colname + ":a"; pointh = colname + ":b"; 
			pointnx = colname + ":nx"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointw;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointw.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointh;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
			 			pointh.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointnx;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 			pointnx.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_ELLIPSE3D || colType == JAG_C_COL_TYPE_RECTANGLE3D ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z";
			pointnx = colname + ":nx"; pointny = colname + ":ny"; 
			pointw = colname + ":a"; pointh = colname + ":b"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			//prt(("s2838 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
			//prt(("s1028 otherAttr.point.x=[%s] otherAttr.point.y=[%s]\n", otherAttr.point.x, otherAttr.point.y ));
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				//prt(("s2830 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				//prt(("s2831 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}


				dbcolumn = dbtab + "." + pointw;
				rc = _tablemap->getValue(dbcolumn, getpos);
				//prt(("s2832 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointw.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointh;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
			 			pointh.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointnx;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 			pointnx.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointny;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
			 			pointny.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

			}
		} else if ( colType == JAG_C_COL_TYPE_BOX || colType == JAG_C_COL_TYPE_ELLIPSOID ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
			pointw = colname + ":a"; pointh = colname + ":c"; 
			pointd = colname + ":b";
			pointnx = colname + ":nx"; pointny = colname + ":ny"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointw;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointw.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointd;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
			 			pointd.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointh;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.c, errmsg, 
			 			pointh.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointnx;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 			pointnx.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointny;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
			 			pointny.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

			}
		} else if ( colType == JAG_C_COL_TYPE_CYLINDER || colType == JAG_C_COL_TYPE_CONE ) {
			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
			pointr = colname + ":a"; pointh = colname + ":c"; 
			pointnx = colname + ":nx"; pointny = colname + ":ny"; 

			dbcolumn = dbtab + "." + pointx;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
			 		pointx.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
			 			pointy.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointz;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
			 			pointz.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointr;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
			 			pointr.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointh;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.c, errmsg, 
			 			pointh.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointnx;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
			 			pointnx.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
				dbcolumn = dbtab + "." + pointny;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
			 			pointny.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_LINE3D || colType == JAG_C_COL_TYPE_LINE ) {
			pointx1 = colname + ":x1"; pointy1 = colname + ":y1"; pointz1 = colname + ":z1";
			pointx2 = colname + ":x2"; pointy2 = colname + ":y2"; pointz2 = colname + ":z2";
			is3D = false;
			if ( colType == JAG_C_COL_TYPE_LINE3D ) { is3D = true; }

			dbcolumn = dbtab + "." + pointx1;
			rc = _tablemap->getValue(dbcolumn, getpos);
			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].x, errmsg, 
			 		pointx1.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy1;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].y, errmsg, 
			 			pointy1.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if ( is3D ) {
					dbcolumn = dbtab + "." + pointz1;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].z, errmsg, 
			 				pointz1.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
							_schAttr[getpos].type );
					}
				}

				dbcolumn = dbtab + "." + pointx2;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].x, errmsg, 
			 			pointx2.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointy2;
				rc = _tablemap->getValue(dbcolumn, getpos);
				//prt(("s2828 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.c_str(), rc, getpos ));
				//prt(("2381 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.c_str(), otherAttr.linestr.point[1].y ));
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].y, errmsg, 
			 			pointy2.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if ( is3D ) {
					dbcolumn = dbtab + "." + pointz2;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].z, errmsg, 
			 				pointz2.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
							_schAttr[getpos].type );
					}
				}
			}
		} else if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_LINESTRING3D
					 || colType == JAG_C_COL_TYPE_MULTIPOINT || colType == JAG_C_COL_TYPE_MULTIPOINT3D
			       ) {
			//prt(("s7650 i=%d colType=[%s] colname=[%s] \n", i, colType.c_str(), colname.c_str() ));
			//pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z";
			prt(("s2181 colType=[%s] colname=[%s]\n", colType.s(), colname.s() ));
			is3D = false;
			if ( colType == JAG_C_COL_TYPE_LINESTRING3D || 
			     colType == JAG_C_COL_TYPE_MULTIPOINT3D ) { is3D = true; }
			getColumnIndex( dbtab, colname, is3D, getx, gety, getz, getxmin, getymin, getzmin,
								getxmax, getymax, getzmax, getid, getcol, getm, getn, geti );
			prt(("s2234 getColumnIndex is3D=%d getxmin=%d getx=%d gety=%d getz=%d\n", is3D, getxmin, getx, gety, getz ));
			if ( getxmin >= 0 ) {
				// char ibuf[32];
				//OtherAttribute other;
				JagLineString line;
				if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_MULTIPOINT ) {
					JagParser::addLineStringData(line, otherAttr.valueData.c_str() );
					prt(("s8032 addLineStringData xmin=%0.3f ymin=%.3f zmin=%.3f\n", xmin, ymin, zmin ));
				} else {
					JagParser::addLineString3DData(line, otherAttr.valueData.c_str() );
					prt(("s8033 addLineString3DData xmin=%0.3f ymin=%.3f zmin=%.3f valueData=[%s]\n", xmin, ymin, zmin, otherAttr.valueData.c_str() ));
					//prt(("s8023 line.print():\n" ));
					//line.print();
				}
				//prt(("s5373 i=%d otherAttr.linestr.size=%d valueData=[%s]\n", i, line.size(), otherAttr.valueData.c_str() ));
				//if ( ! is3D ) { zmin = zmax = 0.0; }

				JagPolyPass ppass;
				ppass.is3D = is3D;
				ppass.xmin = xmin; ppass.ymin = ymin; ppass.zmin = zmin;
				ppass.xmax = xmax; ppass.ymax = ymax; ppass.zmax = zmax;
				ppass.tzdiff = tzdiff; ppass.srvtmdiff = srvtmdiff;
				ppass.getxmin = getxmin; ppass.getymin = getymin; ppass.getzmin = getzmin;
				ppass.getxmax = getxmax; ppass.getymax = getymax; ppass.getzmax = getzmax;
				ppass.getid = getid; ppass.getcol = getcol;
				ppass.getm = getm; ppass.getn = getn; ppass.geti = geti;
				ppass.getx = getx; ppass.gety = gety; ppass.getz = getz;
				ppass.lsuuid = lsuuid;
				ppass.colname = colname;
				ppass.m = 0;
				ppass.n = 0;
				ppass.col = i;

				formatPointsInLineString( line, tablekvbuf, ppass, retpair, errmsg );

				++mlineIndex;
    			hasDoneAppend = true;
				rc = 1;
			}
		} else if ( colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_POLYGON3D
		            || colType == JAG_C_COL_TYPE_MULTILINESTRING || colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			//prt(("s7651 i=%d colType=[%s]\n", i, colType.c_str() ));
			// pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z";
			is3D = false;
			if ( colType == JAG_C_COL_TYPE_POLYGON3D || colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) { is3D = true; }
			getColumnIndex( dbtab, colname, is3D, getx, gety, getz, getxmin, getymin, getzmin,
								getxmax, getymax, getzmax, getid, getcol, getm, getn, geti );
			if ( getxmin >= 0 ) {
				//char ibuf[32];
				//OtherAttribute other;
				JagPolygon pgon;
				if ( colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
					if ( colType == JAG_C_COL_TYPE_POLYGON ) {
						rc = JagParser::addPolygonData( pgon, otherAttr.valueData.c_str(), false, true );
					} else {
						rc = JagParser::addPolygonData( pgon, otherAttr.valueData.c_str(), false, false );
					}
					//prt(("s8041 addPolygonData xmin=%0.3f ymin=%.3f zmin=%.3f \n", xmin, ymin, zmin ));
				} else {
					if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
						rc = JagParser::addPolygon3DData( pgon, otherAttr.valueData.c_str(), false, true );
					} else {
						rc = JagParser::addPolygon3DData( pgon, otherAttr.valueData.c_str(), false, false );
					}
					//prt(("s8042 addPolygonData3D xmin=%0.3f ymin=%.3f zmin=%.3f\n", xmin, ymin, zmin ));
				}

				if ( 0 == rc ) { continue; }
				if ( rc < 0 ) {
					errmsg = Jstr("E3014  Polygon input data error ") + intToStr(rc);
					prt(("E3014 Polygon input data error rc=%d\n", rc ));
					free( tablekvbuf );
					return 0;
				}

				//if ( ! is3D ) { zmin = zmax = 0.0; }
				//pgon.print();

				JagPolyPass ppass;
				ppass.is3D = is3D;
				ppass.xmin = xmin; ppass.ymin = ymin; ppass.zmin = zmin;
				ppass.xmax = xmax; ppass.ymax = ymax; ppass.zmax = zmax;
				ppass.tzdiff = tzdiff; ppass.srvtmdiff = srvtmdiff;
				ppass.getxmin = getxmin; ppass.getymin = getymin; ppass.getzmin = getzmin;
				ppass.getxmax = getxmax; ppass.getymax = getymax; ppass.getzmax = getzmax;
				ppass.getid = getid; ppass.getcol = getcol;

				ppass.getx = getx; ppass.gety = gety; ppass.getz = getz;
				ppass.lsuuid = lsuuid;
				ppass.colname = colname;
				ppass.getm = getm; ppass.getn = getn; ppass.geti = geti;
				ppass.col = i;

				//prt(("s7830 pgon.size()=%d\n", pgon.size() ));
				ppass.m = 0;
				JagLineString linestr;
				for ( int n=0; n < pgon.size(); ++n ) {
					ppass.n = n;
					linestr = pgon.linestr[n]; // convert
					formatPointsInLineString( linestr, tablekvbuf, ppass, retpair, errmsg );
				}

				// formatPointsInLineString( line, tablekvbuf, ppass, retpair, errmsg );
				rc = 1;

				++mlineIndex;
    			hasDoneAppend = true;
			} else {
				prt(("s2773 error polygon\n" ));
			}
		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			//prt(("s7651 i=%d colType=[%s]\n", i, colType.c_str() ));
			// pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z";
			is3D = false;
			if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) { is3D = true; }
			getColumnIndex( dbtab, colname, is3D, getx, gety, getz, getxmin, getymin, getzmin,
								getxmax, getymax, getzmax, getid, getcol, getm, getn, geti );
			if ( getxmin >= 0 ) {
				//char ibuf[32];
				//OtherAttribute other;
				JagVector<JagPolygon> pgvec;
				if ( is3D ) {
					//prt(("s8260 addMultiPolygonData 3d\n" ));
					rc = JagParser::addMultiPolygonData( pgvec, otherAttr.valueData.c_str(), false, false, true );
				} else {
					//prt(("s8260 addMultiPolygonData 2d\n" ));
					rc = JagParser::addMultiPolygonData( pgvec, otherAttr.valueData.c_str(), false, false, false );
				}

				if ( 0 == rc ) continue;
				if ( rc < 0 ) {
					errmsg = Jstr("E3015  MultiPolygon input data error ") + intToStr(rc);
					//prt(("E3015 Polygon input data error rc=%d\n", rc ));
					free( tablekvbuf );
					return 0;
				}

				//if ( ! is3D ) { zmin = zmax = 0.0; }
				//pgon.print();

				JagPolyPass ppass;
				ppass.is3D = is3D;
				ppass.xmin = xmin; ppass.ymin = ymin; ppass.zmin = zmin;
				ppass.xmax = xmax; ppass.ymax = ymax; ppass.zmax = zmax;
				ppass.tzdiff = tzdiff; ppass.srvtmdiff = srvtmdiff;
				ppass.getxmin = getxmin; ppass.getymin = getymin; ppass.getzmin = getzmin;
				ppass.getxmax = getxmax; ppass.getymax = getymax; ppass.getzmax = getzmax;
				ppass.getid = getid; ppass.getcol = getcol;

				ppass.getx = getx; ppass.gety = gety; ppass.getz = getz;
				ppass.lsuuid = lsuuid;
				ppass.colname = colname;
				ppass.getm = getm; ppass.getn = getn; ppass.geti = geti;
				ppass.col = i;

				JagLineString linestr;
    			//prt(("s7830 pgvec.size()=%d\n", pgvec.size() ));
				for ( int i = 0; i < pgvec.size(); ++i ) {
    				ppass.m = i;
    				//prt(("s7830 pgvec.size()=%d  ppass.m=%d\n", pgvec.size(),  ppass.m ));
					const JagPolygon& pgon = pgvec[i];
    				for ( int n=0; n < pgon.size(); ++n ) {
    					ppass.n = n;
						linestr = pgon.linestr[n];
    					formatPointsInLineString( linestr, tablekvbuf, ppass, retpair, errmsg );
    				}
				}

				rc = 1;

				++mlineIndex;
    			hasDoneAppend = true;
			}
		} else if ( colType == JAG_C_COL_TYPE_TRIANGLE || colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
			pointx1 = colname + ":x1"; pointy1 = colname + ":y1"; pointz1 = colname + ":z1"; 
			pointx2 = colname + ":x2"; pointy2 = colname + ":y2"; pointz2 = colname + ":z2";
			pointx3 = colname + ":x3"; pointy3 = colname + ":y3"; pointz3 = colname + ":z3";

			is3D = false;
			if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) { is3D = true; }

			dbcolumn = dbtab + "." + pointx1;
			rc = _tablemap->getValue(dbcolumn, getpos);

			//prt(("s5828 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.c_str(), rc, getpos ));
			//prt(("s5381 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.c_str(), otherAttr.linestr.point[1].y ));

			if ( rc ) {
				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].x, errmsg, 
			 		pointx1.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );

				dbcolumn = dbtab + "." + pointy1;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].y, errmsg, 
			 			pointy1.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if ( is3D ) {
					dbcolumn = dbtab + "." + pointz1;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].z, errmsg, 
			 				pointz1.c_str(), _schAttr[getpos].offset, 
							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
					}
				}

				dbcolumn = dbtab + "." + pointx2;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].x, errmsg, 
			 			pointx2.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointy2;
				rc = _tablemap->getValue(dbcolumn, getpos);
				//prt(("s5428 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.c_str(), rc, getpos ));
				//prt(("s5581 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.c_str(), otherAttr.linestr.point[1].y ));
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].y, errmsg, 
			 			pointy2.c_str(), _schAttr[getpos].offset, 
						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if ( is3D ) {
					dbcolumn = dbtab + "." + pointz2;
					rc = _tablemap->getValue(dbcolumn, getpos);
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].z, errmsg, 
			 				pointz2.c_str(), _schAttr[getpos].offset, 
							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
					}
				}

				dbcolumn = dbtab + "." + pointx3;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].x, errmsg, 
			 			pointx3.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				dbcolumn = dbtab + "." + pointy3;
				rc = _tablemap->getValue(dbcolumn, getpos);
				if ( rc ) {
					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].y, errmsg, 
			 			pointy3.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
				}

				if ( is3D ) {
					dbcolumn = dbtab + "." + pointz3;
					rc = _tablemap->getValue(dbcolumn, getpos);
					// prt(("s5201 is3D getValue pointz3 rc=%d\n", rc ));
					if ( rc ) {
						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].z, errmsg, 
			 				pointz3.c_str(), _schAttr[getpos].offset, 
							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
						// prt(("s5203 formatOneCol rc=%d\n", rc ));
					}
				}
			} 
		} else {
			/***
			prt(("s5128 i=%d name=[%s] otherAttr.valueData=[%s]\n", 
					i, (*_tableRecord.columnVector)[i].name.c_str(), otherAttr.valueData.c_str() ));
					***/
			if ( otherAttr.valueData.size() < 1 ) {
				rc = 1;
			} else {
			    rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.valueData.c_str(), errmsg, 
				 	(*_tableRecord.columnVector)[i].name.c_str(), _schAttr[getpos].offset, 
					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
			}
		}

		if ( !rc ) {
			free( tablekvbuf );
			// errmsg is set in formatOneCol()
			//prt(("s2903 return 0 here\n" ));
			return 0;
		}

		// debug only
		/***
		prt(("\n"));
		prt(("s2033 i=%d getpos dbcolumn=[%s] tablekvbuf: \n", i, dbcolumn.c_str() ));
		jagfwrite( tablekvbuf,  KEYVALLEN, stdout );
		prt(("\n"));
		***/

	}  // end for ( int i = 0; i < parseParam->otherVec.size(); ++i ) 

	if ( *tablekvbuf == '\0' ) {
		errmsg = "E1101 First key is NULL";
		free( tablekvbuf );
		return 0;
	}

	// setup default value if exists
	for ( int i = 0; i < _defvallist.size(); ++i ) {
		if ( _defvallist[i] < parseParam->otherVec.size() ) {
			continue;
		}
		rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, _schAttr[_defvallist[i]].defValue.c_str(), errmsg, 
			 (*_tableRecord.columnVector)[_defvallist[i]].name.c_str(), _schAttr[_defvallist[i]].offset, 
			_schAttr[_defvallist[i]].length, _schAttr[_defvallist[i]].sig, _schAttr[_defvallist[i]].type );
		if ( !rc ) {
			free( tablekvbuf );
			errmsg = "E3039 formatOneCol default error";
			return 0;
		}			
	}

	// debug only
	/**
	prt(("s2037 tablekvbuf: \n" ));
	jagfwrite( tablekvbuf,  KEYVALLEN, stdout );
	prt(("\n"));
	**/
	//prt(("s5042 hasDoneAppend=%d\n", hasDoneAppend ));
	if ( ! hasDoneAppend ) {
		dbNaturalFormatExchange( tablekvbuf, _numKeys, _schAttr, 0, 0, " " ); 
		retpair.append( JagDBPair( tablekvbuf, KEYLEN, tablekvbuf+KEYLEN, VALLEN, true ) );
		/**
		prt(("s4106 no hasDoneAppend  tablekvbuf:\n" ));
		dumpmem( tablekvbuf, KEYLEN+VALLEN);
		**/
	}
	free( tablekvbuf );
	return 1;
}

// mode 0: insert; mode 1: cinsert( check insert ); mode 2: dinsert ( delete insert );
int JagTable::insertPair( JagDBPair &pair, int &insertCode, bool direct, int mode ) 
{	
	/**
	prt(("s5130 insertPair pair.key=[%s]\n", pair.key.c_str() ));
	prt(("s5130 insertPair pair.value: " ));
	pair.value.dump();
	prt(("\n"));
	**/

	int rc;
	insertCode = 0;
	JagDBPair retpair;
	if ( 0 == mode ) {
		rc = _darrFamily->insert( pair, insertCode, true, direct, retpair );
		// if duplicate, need to check if the pair exact exists in jdb
		// prt(("s5838 insertPair rc=%d\n", rc ));
		if ( !rc ) {
			retpair.key = pair.key;
			if ( _darrFamily->get( retpair ) && retpair.key == pair.key && retpair.value == pair.value ) rc = true;
		}
		// check if table has indexs, if yes, insert to self server index
		if ( rc ) formatIndexCmd( pair, mode );
		++_actualInsert;
	} else if ( 1 == mode ) {
		// check to see if this pair is already exist
		rc = _darrFamily->exist( pair );
	} else if ( 2 == mode ) {
		rc = _darrFamily->remove( pair );
		// if actually removed, also remove index data if needed
		if ( rc ) {
			formatIndexCmd( pair, mode );
			char *buf = (char*)jagmalloc(KEYVALLEN+1);
			memcpy( buf, pair.key.c_str(), KEYLEN );
			memcpy( buf+KEYLEN, pair.value.c_str(), VALLEN );
			removeColFiles( buf );
			free( buf );
		}
	}

	return rc;
}

// insert : insert cmd into jdb file
int JagTable::insert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( req.session->timediff, parseParam, pair, errmsg );
	#ifdef DEVDEBUG
	// prt(("s4920 rc=%d pair.key=[%s] pair.value=[%s]\n", rc, pair.key.c_str(), pair.c_str()));
	#endif
	if ( rc ) {
		for ( int i=0; i < pair.length(); ++i ) {
			pair[i].upsertFlag = 0;
			rc = insertPair( pair[i], insertCode, direct, 0 );
			// prt(("s3481 insertPair rc=%d\n", rc ));
			if ( !rc ) {
				errmsg = Jstr("E2108 Insert error key: ") + pair[i].key.c_str();
			}
		}
	} else {
		errmsg += Jstr(" E5208 Error insert pair");
	}
	
	return rc;
}

// single insert, used by inserted data with file transfer
int JagTable::sinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( req.session->timediff, parseParam, pair, errmsg );
	// prt(("s4920 sinsert rc=%d pair.key=[%s] pair.value=[%s]\n", rc, pair.key.c_str(), pair.c_str()));
	if ( rc ) {
		for ( int k=0; k < pair.size(); ++k ) {
    		pair[k].upsertFlag = 0;
    		rc = insertPair( pair[k], insertCode, 1, 0 );
    		if ( !rc ) {
    			errmsg = Jstr("E4003 Insert error key: ") + pair[k].key.c_str();
    		} else {
    			rc = 0;
    			Jstr hdir = fileHashDir( pair[k].key );
    			jaguar_mutex_lock ( &req.session->dataMutex );
    			// receive _BEGINFILEUPLOAD_ to begin file transfer
    			char *newbuf = NULL; char hdr[JAG_SOCK_MSG_HDR_LEN+1]; ssize_t rlen = 0;
    			memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
    			rlen = recvData( req.session->sock, hdr, newbuf );
    			if ( rlen > 0 && 0 == strncmp( newbuf, "_BEGINFILEUPLOAD_", 17 ) ) {
    				for ( int i = 0; i < _numCols; ++i ) {
    					if ( _schAttr[i].isFILE ) {
    						JagFileMgr::makedirPath( _darrFamily->_sfilepath + "/" + hdir );
    						rc = oneFileReceiver( req.session->sock, _darrFamily->_sfilepath + "/" + hdir );
    					}
    				}
    				memset( hdr, 0, JAG_SOCK_MSG_HDR_LEN+1);
    				rlen = recvData( req.session->sock, hdr, newbuf );
    				if ( rlen > 0 && 0 == strncmp( newbuf, "_ENDFILEUPLOAD_", 15 ) ) { rc = 1; }
    			} else {
    				prt(("s3728 error hdr=[%s] newbuf=[%s]\n", hdr, newbuf ));
    			}
    			jaguar_mutex_unlock ( &req.session->dataMutex );
    		}
		}
	} else {
		errmsg = Jstr("E4123 Error insert pair");
	}
	
	return rc;
}

// cinsert : check if this pair is exist or not
int JagTable::cinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( req.session->timediff, parseParam, pair, errmsg );
	// prt(("pair.key=[%s] pair.value=[%s]\n", pair.key.c_str(), pair.c_str()));
	if ( rc ) {
		for ( int i=0; i < pair.size(); ++i ) {
			rc = insertPair( pair[i], insertCode, direct, 1 );
		}
	} else {
		errmsg += Jstr(" E3043 Error cinsert pair");
	}
	
	return rc;
}

// dinsert: delete if this pair is exist
int JagTable::dinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( parseParam->timediff, parseParam, pair, errmsg );
	// prt(("pair.key=[%s] pair.value=[%s]\n", pair.key.c_str(), pair.c_str()));
	if ( rc ) {
		for ( int i=0; i < pair.size(); ++i ) {
			rc = insertPair( pair[i], insertCode, direct, 2 );
		}
	} else {
		errmsg += Jstr(" E0221 Error dinsert pair");
	}
	
	return rc;
}

// method to form insert/upsert into index cmd for each table insert/upsert cmd
int JagTable::formatIndexCmd( JagDBPair &pair, int mode )
{
	if ( _indexlist.size() < 1 ) return 0;

	char *tablebuf = (char*)jagmalloc(KEYVALLEN+1);
	memcpy( tablebuf, pair.key.c_str(), KEYLEN );
	memcpy( tablebuf+KEYLEN, pair.value.c_str(), VALLEN );
	tablebuf[KEYVALLEN] = '\0';
	dbNaturalFormatExchange( tablebuf, _numKeys, _schAttr, 0, 0, " " ); // db format -> natural format
	
	bool rc;
	abaxint cnt = 0;
	AbaxBuffer bfr;
	JagIndex *pindex = NULL;
	const char *p;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		pindex = _servobj->_objectLock->readLockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
													_tableschema, _indexschema, _replicateType, 1 );
		if ( pindex ) {
			pindex->formatIndexCmdFromTable( tablebuf, mode );
			++ cnt;
			_servobj->_objectLock->readUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
											_tableschema, _indexschema, _replicateType, 1 );
		}
	}
	free( tablebuf );
	return cnt;
}

// method to do create index concurrently
int JagTable::formatCreateIndex( JagIndex *pindex ) 
{
	if ( _darrFamily->_darrlistlen < 1 ) {
		prt(("s6402 JagTable::formatCreateIndex return 0\n" ));
		return 0;
	}
	abaxint callCounts = -1, lastBytes = 0, memlim = availableMemory( callCounts, lastBytes )/8/_darrFamily->_darrlistlen/ONE_MEGA_BYTES;
	if ( memlim <= 0 ) memlim = 1;
	pthread_t thrd[_darrFamily->_darrlistlen];
	for ( abaxint i = 0; i < _darrFamily->_darrlistlen; ++i ) {
		//ParallelCmdPass *psp = new ParallelCmdPass();
		ParallelCmdPass *psp = newObject<ParallelCmdPass>();
		psp->ptab = this;
		psp->pindex = pindex;
		psp->pos = i;
		psp->memlimit = memlim;
		jagpthread_create( &thrd[i], NULL, parallelCreateIndexStatic, (void*)psp );
	}

	for ( abaxint i = 0; i < _darrFamily->_darrlistlen; ++i ) {
		pthread_join(thrd[i], NULL);
	}
	return 1;
}

// static method for create index 
void *JagTable::parallelCreateIndexStatic( void * ptr )
{	
	ParallelCmdPass *pass = (ParallelCmdPass*)ptr;

	JagDiskArrayServer *darr = pass->ptab->_darrFamily->_darrlist[pass->pos];
	char *tablebuf = (char*)jagmalloc(darr->KEYVALLEN+1);
	memset( tablebuf, 0, darr->KEYVALLEN+1 );
	abaxint mlimit = pass->memlimit;
	JagBuffReader br( darr, darr->_arrlen, darr->KEYLEN, darr->VALLEN, 0, 0, mlimit );
	while ( br.getNext( tablebuf ) ) {
		dbNaturalFormatExchange( tablebuf, pass->ptab->_numKeys, pass->ptab->_schAttr, 0, 0, " " ); 
		/**
		prt(("s7648 tablebuf:\n" ));
		dumpmem( tablebuf, darr->KEYVALLEN );
		**/
		pass->pindex->formatIndexCmdFromTable( tablebuf, 0 );
	}
	free( tablebuf );
	delete pass;
}

abaxint JagTable::update( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode )
{
	// chain not able to update
	if ( JAG_CHAINTABLE_TYPE == _objectType ) {
		errmsg = "Chain cannot be updated";
		return 0;
	}

	int setnum = parseParam->updSetVec.size();
	int rc, collen, siglen, setindexnum = _indexlist.size(), constMode = 0, typeMode = 0, tabnum = 0, treelength = 0;
	Jstr treetype;
	bool keyset, uniqueAndHasValueCol = 0, setKey;
	abaxint cnt = 0;
	const char *buffers[1];
	abaxint setposlist[setnum];
	int getpos = 0;
	JagIndex *lpindex[_indexlist.size()];
	JagIndex *pindex = NULL;
	AbaxBuffer bfr;
	Jstr dbcolumn;
	char *tableoldbuf = (char*)jagmalloc(KEYVALLEN+1);
	char *tablenewbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( tableoldbuf, 0, KEYVALLEN+1 );
	memset( tablenewbuf, 0, KEYVALLEN+1 );
	
	ExprElementNode *updroot;
	const JagHashStrInt *maps[1];
	const JagSchemaAttribute *attrs[1];	
	maps[0] = _tablemap;
	attrs[0] = _schAttr;
	
	JagFixString strres, treestr;
	bool needInit = true;
	
	// check updSetVec validation
	setKey = 0;
	for ( int i = 0; i < setnum; ++i ) {
		dbcolumn = _dbtable + "." + parseParam->updSetVec[i].colName;
		if ( isFileColumn( parseParam->updSetVec[i].colName ) ) {
			if ( tableoldbuf ) free ( tableoldbuf );
			if ( tablenewbuf ) free ( tablenewbuf );
			errmsg = Jstr("E0283 column ") + parseParam->updSetVec[i].colName + " is file type";
			return -1;
		}

		if ( _tablemap->getValue(dbcolumn, getpos) ) {
			setposlist[i] = getpos;
			if ( getpos < _numKeys ) setKey = 1;
			bool isAggregate = false;
			updroot = parseParam->updSetVec[i].tree->getRoot();
			rc = updroot->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, treetype, collen, siglen );
			if ( 0 == rc || isAggregate ) {
				if ( tableoldbuf ) free ( tableoldbuf );
				if ( tablenewbuf ) free ( tablenewbuf );
				errmsg = Jstr("E0383 wrong update type. Char column must use single quotes.");
				return -1;
			}
		} else {
			if ( tableoldbuf ) free ( tableoldbuf );
			if ( tablenewbuf ) free ( tablenewbuf );
			errmsg = Jstr("E0384 column ") + dbcolumn + " does not exist";
			return -1;
		}
	}
	
	// build init index list
	if ( setKey ) {
		for ( int i = 0; i < _indexlist.size(); ++i ) {
			lpindex[i] = _servobj->_objectLock->readLockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i],
				_tableschema, _indexschema, _replicateType, 1 );
		}
	} else {
		setindexnum = 0;
		for ( int i = 0; i < _indexlist.size(); ++i ) {
			lpindex[i] = NULL;
			lpindex[setindexnum] = _servobj->_objectLock->readLockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i],
				_tableschema, _indexschema, _replicateType, 1 );
			for ( int i = 0; i < setnum; ++i ) {
				if ( lpindex[setindexnum] ) {
					if ( lpindex[setindexnum]->needUpdate( parseParam->updSetVec[i].colName ) ) {
						++setindexnum; break;
					} else {
						_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[setindexnum]->getIndexName(),
							_tableschema, _indexschema, _replicateType, 1 );
					}
				}
			}
		}
	}

	// parse tree		
	int keylen[1];
	int numKeys[1];	
	JagMinMax minmax[1];	
	keylen[0] = KEYLEN;
	numKeys[0] = _numKeys;
	minmax[0].setbuflen( keylen[0] );
	buffers[0] = tableoldbuf;

	ExprElementNode *root = parseParam->whereVec[0].tree->getRoot();
	rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, treestr, typeMode, tabnum );
	if ( 0 == rc ) {
		memset( minmax[0].minbuf, 0, keylen[0]+1 );
		memset( minmax[0].maxbuf, 255, keylen[0] );
		(minmax[0].maxbuf)[keylen[0]] = '\0';
	} else if ( rc < 0 ) {
		if ( tableoldbuf ) free ( tableoldbuf );
		if ( tablenewbuf ) free ( tablenewbuf );
		for ( int i = 0; i < _indexlist.size(); ++i ) {
			if ( lpindex[i] ) {
				_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[i]->getIndexName(),
					_tableschema, _indexschema, _replicateType, 1 );
			}
		}
		errmsg = Jstr("E0385 invalid where range found ");
		return -1;
	}

	if ( memcmp(minmax[0].minbuf, minmax[0].maxbuf, KEYLEN) == 0 ) {
		JagDBPair setpair;
		JagFixString getkey ( minmax[0].minbuf, KEYLEN );
		JagDBPair getpair( getkey );
		if ( setKey ) {  // set key to new key, may need to disable later
			if ( _darrFamily->get( getpair ) ) {
				memcpy(tableoldbuf, getpair.key.c_str(), KEYLEN);
				memcpy(tableoldbuf+KEYLEN, getpair.value.c_str(), VALLEN);
				memcpy(tablenewbuf, getpair.key.c_str(), KEYLEN);
				memcpy(tablenewbuf+KEYLEN, getpair.value.c_str(), VALLEN);
				dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
				dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0, 0, " " ); // db format -> natural format
				if ( !uniqueAndHasValueCol ||
					root->checkFuncValid( NULL, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
					for ( int i = 0; i < setnum; ++i ) {					
						updroot = parseParam->updSetVec[i].tree->getRoot();
						needInit = true;
						if ( updroot->checkFuncValid( NULL, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
							memset(tablenewbuf+_schAttr[setposlist[i]].offset, 0, _schAttr[setposlist[i]].length);
							rc = formatOneCol( req.session->timediff, _servobj->servtimediff, tablenewbuf, strres.c_str(), errmsg, 
								parseParam->updSetVec[i].colName, _schAttr[setposlist[i]].offset, 
								_schAttr[setposlist[i]].length, _schAttr[setposlist[i]].sig, _schAttr[setposlist[i]].type );
							if ( !rc ) {
								if ( tableoldbuf ) free ( tableoldbuf );
								if ( tablenewbuf ) free ( tablenewbuf );
								for ( int i = 0; i < _indexlist.size(); ++i ) {
									if ( lpindex[i] ) {
										_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[i]->getIndexName(),
											_tableschema, _indexschema, _replicateType, 1 );
									}
								}
								errmsg = Jstr("E0386 key update error");
								return -1;						
							}								
						} else {
							if ( tableoldbuf ) free ( tableoldbuf );
							if ( tablenewbuf ) free ( tablenewbuf );
							for ( int i = 0; i < _indexlist.size(); ++i ) {
								if ( lpindex[i] ) {
									_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[i]->getIndexName(),
										_tableschema, _indexschema, _replicateType, 1 );
								}
							}
							errmsg = Jstr("E0387 update error");
							return -1;
						}
					}
					dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr,0,0, " " ); // natural format -> db format
					dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
					setpair.point( tablenewbuf, KEYLEN, tablenewbuf+KEYLEN, VALLEN );
					JagDBPair retpair;
					if ( cnt = _darrFamily->insert( setpair, insertCode, true, false, retpair ) ) { // may need to use direct=false?
						_darrFamily->remove( getpair );
					}
				}
			}
		} else {
			if ( _darrFamily->setWithRange( req, getpair, buffers, uniqueAndHasValueCol, root, parseParam, 
									 _numKeys, _schAttr, setposlist, setpair ) ) {
				// out pair is db format
				memcpy(tableoldbuf, getpair.key.c_str(), KEYLEN);
				memcpy(tableoldbuf+KEYLEN, getpair.value.c_str(), VALLEN);
				memcpy(tablenewbuf, setpair.key.c_str(), KEYLEN);
				memcpy(tablenewbuf+KEYLEN, setpair.value.c_str(), VALLEN);
				++cnt;
			}
		}
		
		if ( cnt && _indexlist.size() > 0 && setindexnum ) {
			dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			for ( int i = 0; i < setindexnum; ++i ) {
				if ( lpindex[i] ) {
					lpindex[i]->updateFromTable( tableoldbuf, tablenewbuf );
				}
			}
		}
	} else {
		if ( setKey ) {
			if ( tableoldbuf ) free ( tableoldbuf );
			if ( tablenewbuf ) free ( tablenewbuf );
			for ( int i = 0; i < _indexlist.size(); ++i ) {
				if ( lpindex[i] ) {
					_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[i]->getIndexName(),
						_tableschema, _indexschema, _replicateType, 1 );
				}
			}
			errmsg = "E0392 Range key update unsupported";
			return -1;			
		} else {
			JagMergeReader *ntu = NULL;
			_darrFamily->setFamilyRead( ntu, minmax[0].minbuf, minmax[0].maxbuf );

			if ( ntu ) {				
				while ( true ) {
					rc = ntu->getNext( tableoldbuf );
					if ( !rc ) {
						break;
					}
					
					dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
					rc = root->checkFuncValid( ntu, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
					if ( rc == 1 ) {
						memcpy(tablenewbuf, tableoldbuf, KEYVALLEN);
						JagDBPair getpair( tableoldbuf, KEYLEN, tableoldbuf+KEYLEN, VALLEN );
						for ( int i = 0; i < setnum; ++i ) {
							updroot = parseParam->updSetVec[i].tree->getRoot();
							needInit = true;
							if ( updroot->checkFuncValid( ntu, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
								memset(tablenewbuf+_schAttr[setposlist[i]].offset, 0, _schAttr[setposlist[i]].length);
								rc = formatOneCol( req.session->timediff, _servobj->servtimediff, tablenewbuf, strres.c_str(), errmsg, 
									parseParam->updSetVec[i].colName, _schAttr[setposlist[i]].offset, 
									_schAttr[setposlist[i]].length, _schAttr[setposlist[i]].sig, _schAttr[setposlist[i]].type );
								if ( !rc ) {
									continue;						
								}								
							} else {
								continue;
							}
						}
						dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
						dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
						JagDBPair setpair( tablenewbuf, KEYLEN,  tablenewbuf+KEYLEN, VALLEN, true );
						if ( _darrFamily->set( setpair ) ) {
							++cnt;							
							if ( _indexlist.size() > 0 ) {
								dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
								dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
								for ( int i = 0; i < setindexnum; ++i ) {
									if ( lpindex[i] ) {
										lpindex[i]->updateFromTable( tableoldbuf, tablenewbuf );
									}
								}
							}
						}
					}
				}
				delete ntu;
			}
		}
	}
	if ( tableoldbuf ) free ( tableoldbuf );
	if ( tablenewbuf ) free ( tablenewbuf );
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		if ( lpindex[i] ) {
			_servobj->_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[i]->getIndexName(),
				_tableschema, _indexschema, _replicateType, 1 );
		}
	}
	
	return cnt;
}

abaxint JagTable::remove( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )	
{	
	// chain not able to remove
	if ( JAG_CHAINTABLE_TYPE == _objectType ) {
		errmsg = "Chain cannot be deleted";
		return 0;
	}

	int rc, retval, typeMode = 0, tabnum = 0, treelength = 0;
	bool uniqueAndHasValueCol = 0, needInit = true;
	abaxint cnt = 0;
	const char *buffers[1];
	char *buf = (char*)jagmalloc(KEYVALLEN+1);
	Jstr treetype;
	JagIndex *pindex = NULL;
	AbaxBuffer bfr;
	memset( buf, 0, KEYVALLEN+1 );	
	buffers[0] = buf;
	JagFixString strres;
	
	int keylen[1];
	int numKeys[1];
	const JagHashStrInt *maps[1];
	const JagSchemaAttribute *attrs[1];	
	JagMinMax minmax[1];	
	keylen[0] = KEYLEN;
	numKeys[0] = _numKeys;
	maps[0] = _tablemap;
	attrs[0] = _schAttr;
	minmax[0].setbuflen( keylen[0] );
	JagFixString treestr;

	ExprElementNode *root = parseParam->whereVec[0].tree->getRoot();
	rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, treestr, typeMode, tabnum );
	if ( 0 == rc ) {
		memset( minmax[0].minbuf, 0, keylen[0]+1 );
		memset( minmax[0].maxbuf, 255, keylen[0] );
		(minmax[0].maxbuf)[keylen[0]] = '\0';
	} else if ( rc < 0 ) {
		if ( buf ) free ( buf );
		errmsg = "E0382 invalid range";
		return -1;
	}
	
	if ( memcmp(minmax[0].minbuf, minmax[0].maxbuf, KEYLEN) == 0 ) {
		JagDBPair pair( minmax[0].minbuf, KEYLEN );
		rc = _darrFamily->get( pair );
		if ( rc ) {
			memcpy(buf, pair.key.c_str(), KEYLEN);
			memcpy(buf+KEYLEN, pair.value.c_str(), VALLEN);
			dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			if ( !uniqueAndHasValueCol ) rc = 1;
			else {
				rc = root->checkFuncValid( NULL, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
			}
			if ( rc == 1 ) {
				retval = _darrFamily->remove( pair ); // pair is db format, buf is natural format
				if ( retval == 1 ) {
					++cnt;
					_removeIndexRecords( buf );
					removeColFiles(buf );
				}	
			}			
		}
	} else {
		JagMergeReader *ntr = NULL;
		_darrFamily->setFamilyRead( ntr, minmax[0].minbuf, minmax[0].maxbuf );

		if ( ntr ) {
			while ( true ) {
				rc = ntr->getNext(buf);
				if ( !rc ) {
					break;
				}
				
				dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
				rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
				if ( rc == 1 ) {
					dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
					JagDBPair pair( buf, KEYLEN );
					retval = _darrFamily->remove( pair );
					if ( retval == 1 ) {
						++cnt;
						dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
						_removeIndexRecords( buf );
						removeColFiles(buf );
					}
				}
			}
			delete ntr;
		}
	}

	if ( buf ) free ( buf );
	return cnt;
}

abaxint JagTable::count( const char *cmd, const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, abaxint &keyCheckerCnt )
{
	if ( parseParam->hasWhere ) {
		JagDataAggregate *jda = NULL;
		abaxint cnt = select( jda, cmd, req, parseParam, errmsg, false );
		if ( jda ) delete jda;
		return cnt;
	}
	else {
		return _darrFamily->getElements( keyCheckerCnt );
	}
}

// nowherecnt: false if  select count(*) from t123 where
// nowherecnt: true if  select ... from t123 where ... (there is no cunt in where)
abaxint JagTable::select( JagDataAggregate *&jda, const char *cmd, const JagRequest &req, JagParseParam *parseParam, 
						  Jstr &errmsg, bool nowherecnt, bool isInsertSelect )
{
	// set up timeout for select starting timestamp
	// prt(("s8773 JagTable::select cmd=[%s]\n", cmd ));
	struct timeval now;
	gettimeofday( &now, NULL ); 
	abaxint bsec = now.tv_sec;
	bool timeoutFlag = 0;
	Jstr treetype;
	int rc, typeMode = 0, tabnum = 0, treelength = 0;
	bool uniqueAndHasValueCol = 0, needInit = true;
	abaxint tpossibleElem = 0, treadcnt = 0;
	abaxint nm = parseParam->limit;
	std::atomic<abaxint> recordcnt;

	recordcnt = 0;
	if ( parseParam->hasLimit && nm == 0 && nowherecnt ) return 0;
	if ( parseParam->exportType == JAG_EXPORT && _isExporting ) return 0;
	if ( _darrFamily->_darrlistlen < 1 ) return 0;
	else if ( parseParam->exportType == JAG_EXPORT ) _isExporting = 1;
	if ( JAG_INSERTSELECT_OP == parseParam->opcode && ! parseParam->hasTimeout ) parseParam->timeout = -1;

	int keylen[1];
	int numKeys[1];
	keylen[0] = KEYLEN;
	numKeys[0] = _numKeys;
	const JagHashStrInt *maps[1];
	const JagSchemaAttribute *attrs[1];
	maps[0] = _tablemap;
	attrs[0] = _schAttr;
	JagMinMax minmax[1];
	Jstr newhdr, gbvheader;
	abaxint finalsendlen = 0;
	abaxint gbvsendlen = 0;
	JagSchemaRecord nrec;
	JagFixString treestr, strres;
	ExprElementNode *root = NULL;

	minmax[0].setbuflen( keylen[0] ); // KEYLEN
	//prt(("s2028 minmax[0].setbuflen( keylen=%d )\n", keylen[0] ));

	if ( nowherecnt ) {
		// not select count
		JagVector<SetHdrAttr> hspa;
		SetHdrAttr honespa;
		AbaxString getstr;
		_tableschema->getAttr( _dbtable, getstr );
		honespa.setattr( _numKeys, false, _dbtable, &_tableRecord, getstr.c_str() );
		hspa.append( honespa );
		prt(("s5640 nowherecnt rearrangeHdr ...\n" ));
		rc = rearrangeHdr( 1, maps, attrs, parseParam, hspa, newhdr, gbvheader, finalsendlen, gbvsendlen );
		if ( !rc ) {
			errmsg = "E0823 Error header for select";
			if ( parseParam->exportType == JAG_EXPORT && _isExporting ) _isExporting = 0;
			return -1;			
		}
		nrec.parseRecord( newhdr.c_str() );
		//prt(("s0573 nowherecnt newhdr=[%s]\n",  newhdr.c_str() ));
	}
	
	//prt(("s9283 parseParam->hasWhere=%d\n", parseParam->hasWhere ));
	if ( parseParam->hasWhere ) {
		root = parseParam->whereVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, 
								  treestr, typeMode, tabnum );
		//prt(("s8374 tab select setWhereRange done rc=%d\n", rc ));
		if ( 0 == rc ) {
			memset( minmax[0].minbuf, 0, keylen[0]+1 );
			memset( minmax[0].maxbuf, 255, keylen[0] );
			(minmax[0].maxbuf)[keylen[0]] = '\0';
		} else if (  rc < 0 ) {
			if ( parseParam->exportType == JAG_EXPORT && _isExporting ) _isExporting = 0;
			errmsg = "E0825 Error where range";
			return -1;
		}

		#if 0
		prt(("s7430 dumpmem: minbuf \n" ));
		dumpmem( minmax[0].minbuf, keylen[0] );
		prt(("s7431 dumpmem: maxbuf \n" ));
		dumpmem( minmax[0].maxbuf, keylen[0] );
		#endif
	}
	
	// finalbuf, hasColumn len or KEYVALLEN if !hasColumn
	// gbvbuf, if has group by
	char *finalbuf = (char*)jagmalloc(finalsendlen+1);
	memset(finalbuf, 0, finalsendlen+1);
	char *gbvbuf = (char*)jagmalloc(gbvsendlen+1);
	memset(gbvbuf, 0, gbvsendlen+1);
	prt(("s1028 finalsendlen=%d gbvsendlen=%d\n", finalsendlen, gbvsendlen ));
	JagMemDiskSortArray *gmdarr = NULL;
	if ( gbvsendlen > 0 ) {
		gmdarr = newObject<JagMemDiskSortArray>();
		gmdarr->init( atoi((_cfg->getValue("GROUPBY_SORT_SIZE_MB", "1024")).c_str()), gbvheader.c_str(), "GroupByValue" );
		gmdarr->beginWrite();
	}
	
	// if insert into ... select syntax, create cpp client object to send insert cmd to corresponding server
	JaguarCPPClient *pcli = NULL;
	if ( JAG_INSERTSELECT_OP == parseParam->opcode ) {
		//pcli = new JaguarCPPClient();
		pcli = newObject<JaguarCPPClient>();
		Jstr host = "localhost", unixSocket = Jstr("/TOKEN=") + _servobj->_servToken;
		if ( _servobj->_listenIP.size() > 0 ) { host = _servobj->_listenIP; }
		if ( !pcli->connect( host.c_str(), _servobj->_port, "admin", "dummy", "test", unixSocket.c_str(), 0 ) ) {
			raydebug( stdout, JAG_LOG_LOW, "s4055 Connect (%s:%s) (%s:%d) error [%s], retry ...\n",
					  "admin", "jaguar", host.c_str(), _servobj->_port, pcli->error() );
			pcli->close();
			if ( pcli ) delete pcli;
			if ( gmdarr ) delete gmdarr;
			if ( finalbuf ) free ( finalbuf );
			if ( gbvbuf ) free ( gbvbuf );
			errmsg = "E0826 Error connection";
			return -1;
		}
	}
	
	// prt(("s1028 tab here\n" ));
	// point query, one record
	if ( memcmp(minmax[0].minbuf, minmax[0].maxbuf, KEYLEN) == 0 ) {
		// point query, one record
		JagDBPair pair( minmax[0].minbuf, KEYLEN );
		//prt(("s9291 point query\n"));
		if ( _darrFamily->get( pair ) ) {
			//prt(("s2291 get pai true\n"));
			const char *buffers[1];
			char *buf = (char*)jagmalloc(KEYVALLEN+1);
			memset( buf, 0, KEYVALLEN+1 );	
			memcpy(buf, pair.key.c_str(), KEYLEN);
			memcpy(buf+KEYLEN, pair.value.c_str(), VALLEN);
			buffers[0] = buf;
			Jstr hdir = fileHashDir( pair.key );
			dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			//prt((" point query s1293 uniqueAndHasValueCol=%d\n", uniqueAndHasValueCol ));
			if ( !uniqueAndHasValueCol ||
				( root && root->checkFuncValid( NULL, maps, attrs, buffers, strres, typeMode, treetype, 
											    treelength, needInit, 0, 0 ) == 1 ) ) {
				//raydebug( stdout, JAG_LOG_HIGH, "s5241 opcode=%d\n", parseParam->opcode  );
				if ( parseParam->opcode == JAG_GETFILE_OP ) { 
					setGetFileAttributes( hdir, parseParam, buffers ); 
				}
				nonAggregateFinalbuf( NULL, maps, attrs, req, buffers, parseParam, finalbuf, finalsendlen, jda, 
									  _dbtable, recordcnt, nowherecnt, NULL, true );
				//prt(("s5820 opcode=%d getfileActualData=%d\n", parseParam->opcode, parseParam->getfileActualData ));
				if ( parseParam->opcode == JAG_GETFILE_OP && parseParam->getfileActualData ) {
					Jstr hdir = fileHashDir( pair.key );
					jaguar_mutex_lock ( &req.session->dataMutex );
					//prt(("s4320 tab sendDirectToSock _BEGINFILEUPLOAD_ ...\n" ));
					sendDirectToSock( req.session->sock, "_BEGINFILEUPLOAD_", 17 );
					//prt(("s4320 tab sendDirectToSock _BEGINFILEUPLOAD_ done ...\n" ));
					Jstr ddcol, inpath; int getpos; char fname[JAG_FILE_FIELD_LEN+1];
					for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
						ddcol = _dbtable + "." + parseParam->selColVec[i].getfileCol.c_str();
						if ( _tablemap->getValue(ddcol, getpos) ) {
							memcpy( fname, buffers[0]+_schAttr[getpos].offset, _schAttr[getpos].length );
							inpath = _darrFamily->_sfilepath + "/" + hdir + "/" + fname;
							oneFileSender( req.session->sock, inpath );
						}
					}
					//prt(("s4323 tab sendDirectToSock _ENDFILEUPLOAD_ ...\n" ));
					sendDirectToSock( req.session->sock, "_ENDFILEUPLOAD_", 15 );
					//prt(("s4323 tab sendDirectToSock _ENDFILEUPLOAD_ done to cli ...\n" ));
					jaguar_mutex_unlock ( &req.session->dataMutex );
				} else {
					if ( JAG_INSERTSELECT_OP == parseParam->opcode ) {
						Jstr iscmd;
						if ( formatInsertSelectCmdHeader( parseParam, iscmd ) ) {
							formatInsertFromSelect( parseParam, attrs[0], finalbuf, buffers[0], finalsendlen, 
												    _numCols, pcli, recordcnt, iscmd );
						}
					} else {
						// raydebug( stdout, JAG_LOG_HIGH, "s5541 opcode=%d\n", parseParam->opcode  );
					}
				}
			} else {
				// raydebug( stdout, JAG_LOG_HIGH, "s0341 uniqueAndHasValueCol=%d\n", uniqueAndHasValueCol  );
				if ( parseParam->getfileActualData ) {
					sendDirectToSock( req.session->sock, "_BEGINFILEUPLOAD_", 17 );
					sendDirectToSock( req.session->sock, "_ENDFILEUPLOAD_", 15 );
				}
			}

			free( buf );
		} else {
			// prt(("s4501 getpair error pair.key=[%s]\n", pair.key.c_str() ));
			if ( parseParam->getfileActualData ) {
				sendDirectToSock( req.session->sock, "_BEGINFILEUPLOAD_", 17 );
				sendDirectToSock( req.session->sock, "_ENDFILEUPLOAD_", 15 );
			}
		}
	} else { // range query
		// range query
		//prt(("s7272 range query\n" ));
		abaxint keyCheckerCnt = 0, callCounts = -1, lastBytes = 0;
		if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
			//if ( !jda ) jda = new JagDataAggregate();
			if ( !jda ) jda = newObject<JagDataAggregate>();
			jda->setwrite( _dbtable, _dbtable, parseParam->exportType == JAG_EXPORT );
			jda->setMemoryLimit( _darrFamily->getElements( keyCheckerCnt )*KEYVALLEN*2 );
		}
		// get num of threads
		bool lcpu = false; abaxint numthrds = _darrFamily->_darrlistlen/_servobj->_numCPUs;
		if ( numthrds < 1 ) {
			numthrds = _darrFamily->_darrlistlen;
			lcpu = true;
		}
		
		//prt(("s7349 range query ...\n" ));
		JagParseParam *pparam[numthrds];
		JagParseAttribute jpa( _servobj, req.session->timediff, _servobj->servtimediff, 
							   req.session->dbname, _servobj->_cfg );
		if ( parseParam->hasGroup ) {
			// group by, no insert into ... select ... syntax allowed
			JagMemDiskSortArray *lgmdarr[numthrds];
			for ( abaxint i = 0; i < numthrds; ++i ) {
				lgmdarr[i] = newObject<JagMemDiskSortArray>();
				pparam[i] = newObject<JagParseParam>();
			    pparam[i]->parent = parseParam;
				lgmdarr[i]->init( 40, gbvheader.c_str(), "GroupByValue" );
				lgmdarr[i]->beginWrite();
			}
			int numCPUs = _servobj->_jagSystem.getNumCPUs();
			JagParallelParse pparser( numCPUs, _servobj->_parsePool );
			pparser.parseSame( jpa, cmd, numthrds, pparam );
			abaxint memlim = availableMemory( callCounts, lastBytes )/8/numthrds/1024/1024;
			if ( memlim <= 0 ) memlim = 1;
			
			jaguar_mutex_lock ( &_servobj->g_selectmutex );
			_servobj->_selectPool->wait();
			ParallelCmdPass psp[numthrds];
			for ( int i = 0; i < numthrds; ++i ) {
				#if 1
				psp[i].ptab = this;
				psp[i].pindex = NULL;
				psp[i].pos = i;
				psp[i].sendlen = gbvsendlen;
				psp[i].parseParam = pparam[i];
				psp[i].gmdarr = lgmdarr[i];
				psp[i].req = req;
				psp[i].jda = jda;
				psp[i].writeName = _dbtable;
				psp[i].recordcnt = &recordcnt;
				psp[i].actlimit = nm;
				psp[i].nowherecnt = nowherecnt;
				psp[i].nrec = &nrec;
				psp[i].memlimit = memlim;
				psp[i].minbuf = minmax[0].minbuf;
				psp[i].maxbuf = minmax[0].maxbuf;
				psp[i].starttime = bsec;
				psp[i].kvlen = KEYVALLEN;
				if ( lcpu ) {
					psp[i].spos = i; psp[i].epos = i;
				} else {
					psp[i].spos = i*_servobj->_numCPUs;
					psp[i].epos = psp[i].spos+_servobj->_numCPUs-1;
					if ( i == numthrds-1 ) psp[i].epos = _darrFamily->_darrlistlen-1;
				}

				#else
				prt(("s6711 gbvsendlen=%d\n", gbvsendlen ));
				fillCmdParse( JAG_TABLE, this, i, gbvsendlen, pparam, 1, lgmdarr, req, jda, _dbtable,  
								recordcnt, nm, nowherecnt, nrec, memlim, minmax, bsec, KEYVALLEN, 
								_servobj, numthrds, _darrFamily, lcpu );

				prt(("s6714 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%0x minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, psp[i].cnt, 
					 psp[i].nrec, psp[i].minbuf, psp[i].maxbuf ));
				#endif

				if ( JAG_INSERTSELECT_OP == parseParam->opcode && ! parseParam->hasTimeout ) {
					pparam[i]->timeout = -1;
				}

				_servobj->_selectPool->addWork( parallelSelectStatic, (void*)&psp[i] );

			}

			_servobj->_selectPool->wait();
			jaguar_mutex_unlock ( &_servobj->g_selectmutex );

			for ( abaxint i = 0; i < numthrds; ++i ) {
				if ( psp[i].timeoutFlag ) timeoutFlag = 1;
				lgmdarr[i]->endWrite();
				lgmdarr[i]->beginRead();
				while ( 1 ) {
					rc = lgmdarr[i]->get( gbvbuf );
					if ( !rc ) break;
					JagDBPair pair(gbvbuf, gmdarr->_keylen, gbvbuf+gmdarr->_keylen, gmdarr->_vallen, true );
					rc = gmdarr->groupByUpdate( pair );
				}
				lgmdarr[i]->endRead();
				delete lgmdarr[i];
				delete pparam[i];
			}

			groupByFinalCalculation( gbvbuf, nowherecnt, finalsendlen, recordcnt, nm, req, _dbtable, 
									 parseParam, jda, gmdarr, &nrec );
		} else {
			// has no group
			// check if has aggregate
			//prt(("s1283 query with no group\n" ));
			bool hAggregate = false;
			if ( parseParam->hasColumn ) {
				for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
					if ( parseParam->selColVec[i].isAggregate ) hAggregate = true;
					break;
				}
			}

			// parse for multiple jdb files
			for ( abaxint i = 0; i < numthrds; ++i ) {
				//pparam[i] = new JagParseParam();
				pparam[i] = newObject<JagParseParam>();
			    pparam[i]->parent = parseParam;
			}
			int numCPUs = _servobj->_jagSystem.getNumCPUs();
			JagParallelParse pparser( numCPUs, _servobj->_parsePool );
			// prt(("s4408 JagParallelParse pparser.parseSame(%s)\n", cmd ));
			pparser.parseSame( jpa, cmd, numthrds, pparam );

			abaxint memlim = availableMemory( callCounts, lastBytes )/8/numthrds/1024/1024;
			if ( memlim <= 0 ) memlim = 1;
			// pthread_t thrd[_darrFamily->_darrlistlen];
			jaguar_mutex_lock ( &_servobj->g_selectmutex );
			_servobj->_selectPool->wait();
			ParallelCmdPass psp[numthrds];
			JagMemDiskSortArray *lgmdarr[1];
			for ( abaxint i = 0; i < numthrds; ++i ) {
				psp[i].cli = pcli;

				#if 1
				psp[i].ptab = this;
				psp[i].pindex = NULL;
				psp[i].pos = i;
				psp[i].sendlen = finalsendlen;
				psp[i].parseParam = pparam[i];
				psp[i].gmdarr = NULL;
				psp[i].req = req;
				psp[i].jda = jda;
				psp[i].writeName = _dbtable;
				psp[i].recordcnt = &recordcnt;
				psp[i].nrec = &nrec;
				psp[i].actlimit = nm;
				psp[i].nowherecnt = nowherecnt;
				psp[i].memlimit = memlim;
				psp[i].minbuf = minmax[0].minbuf;
				psp[i].maxbuf = minmax[0].maxbuf;
				psp[i].starttime = bsec;
				psp[i].kvlen = KEYVALLEN;
				if ( lcpu ) {
					psp[i].spos = i; psp[i].epos = i;
				} else {
					psp[i].spos = i*_servobj->_numCPUs;
					psp[i].epos = psp[i].spos+_servobj->_numCPUs-1;
					if ( i == numthrds-1 ) psp[i].epos = _darrFamily->_darrlistlen-1;
				}
				#else
				prt(("s6513 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%d minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, *psp[i].cnt, psp[i].nrec, 
					 	psp[i].minbuf, psp[i].maxbuf ));
				prt(("s6712 gbvsendlen=%d\n", gbvsendlen ));
				fillCmdParse( JAG_TABLE, this, i, gbvsendlen, pparam, 0, lgmdarr, req, jda, _dbtable,  
							 cnt, nm, nowherecnt, nrec, memlim, minmax, 
							 bsec, KEYVALLEN, _servobj, numthrds, _darrFamily, lcpu );
				prt(("s6514 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%d minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, *psp[i].cnt, psp[i].nrec, 
					 psp[i].minbuf, psp[i].maxbuf ));
				#endif


				if ( JAG_INSERTSELECT_OP == parseParam->opcode && ! parseParam->hasTimeout ) {
					pparam[i]->timeout = -1;
				}

				_servobj->_selectPool->addWork( parallelSelectStatic, (void*)&psp[i] );

			}
			_servobj->_selectPool->wait();
			//prt(("s0293 _selectPool->wait() done ...\n" ));
			jaguar_mutex_unlock ( &_servobj->g_selectmutex );

			for ( abaxint i = 0; i < numthrds; ++i ) {
				// pthread_join(thrd[i], NULL);
				if ( psp[i].timeoutFlag ) timeoutFlag = 1;
			}

			//prt(("s5701 in ::select hAggregate=%d recordcnt=%d\n", hAggregate, (int)recordcnt ));
			if ( hAggregate ) {
				aggregateFinalbuf( req, newhdr, numthrds, pparam, finalbuf, finalsendlen, jda, _dbtable, 
								   recordcnt, nowherecnt, &nrec );
			    //prt(("s3848 aggregateFinalbuf cnt=%d\n", (int)cnt ));
			}
		
			for ( abaxint i = 0; i < numthrds; ++i ) {
				delete pparam[i];
			}	
		}	

		if ( jda ) {
			//prt(("s5003 jda->flushwrite() ...\n" ));
			jda->flushwrite();
			//prt(("s5003 jda->flushwrite() done ...\n" ));
		}
	}

	if ( timeoutFlag ) {
		Jstr timeoutStr = "E0283 Table select has timed out. Results have been truncated;";
		sendMessage( req, timeoutStr.c_str(), "ER" );
	}
	
	if ( parseParam->exportType == JAG_EXPORT ) recordcnt = 0;
	if ( parseParam->exportType == JAG_EXPORT && _isExporting ) _isExporting = 0;
	if ( pcli ) {
		pcli->close();
		delete pcli;
	}
	if ( gmdarr ) delete gmdarr;
	if ( finalbuf ) free ( finalbuf );
	if ( gbvbuf ) free ( gbvbuf );

	//prt(("s6273 JagTable select recordcnt=%d\n", (int)recordcnt ));
	return (abaxint)recordcnt;
}

// static select function
void *JagTable::parallelSelectStatic( void * ptr )
{	
	//prt(("s2298 parallelSelectStatic ...\n" ));
	ParallelCmdPass *pass = (ParallelCmdPass*)ptr;
	ExprElementNode *root;
	int rc, collen, siglen, constMode = 0, typeMode = 0, treelength = 0, tabnum = 0;
	bool isAggregate, hasAggregate = false, hasFirst = false, needInit = true, uniqueAndHasValueCol;
	abaxint offset = 0;
	int numCols[1];
	int numKeys[1];
	int keylen[1];
	JagMinMax minmax[1];
	const JagHashStrInt *maps[1];
	const JagSchemaAttribute *attrs[1];	
	JagFixString strres, treestr;
	Jstr treetype;

	if ( pass->ptab ) {
		numCols[0] = pass->ptab->_numCols;
		numKeys[0] = pass->ptab->_numKeys;
		maps[0] = pass->ptab->_tablemap;
		attrs[0] = pass->ptab->_schAttr;
		keylen[0] = pass->ptab->KEYLEN;
		minmax[0].setbuflen( keylen[0] );
	} else {
		numCols[0] = pass->pindex->_numCols;
		numKeys[0] = pass->pindex->_numKeys;
		maps[0] = pass->pindex->_indexmap;
		attrs[0] = pass->pindex->_schAttr;
		keylen[0] = pass->pindex->KEYLEN;
		minmax[0].setbuflen( keylen[0] );
	}

	// set param select tree and where tree, if needed
	if ( pass->parseParam->hasColumn ) {
		//prt(("s8722 pass->parseParam->hasColumn\n" ));
		for ( int i = 0; i < pass->parseParam->selColVec.size(); ++i ) {
			isAggregate = false;
			root = pass->parseParam->selColVec[i].tree->getRoot();
			rc = root->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, treetype, collen, siglen );
			prt(("s2390 setFuncAttribute rc=%d collen=%d\n", rc, collen ));
			if ( 0 == rc ) {
				prt(("s3006 error setFuncAttribute()\n"));
				return NULL;
			}
			pass->parseParam->selColVec[i].offset = offset;
			pass->parseParam->selColVec[i].length = collen;
			pass->parseParam->selColVec[i].sig = siglen;
			pass->parseParam->selColVec[i].type = treetype;
			pass->parseParam->selColVec[i].isAggregate = isAggregate;
			if ( isAggregate ) hasAggregate = true;
			offset += collen;

			#ifdef DEVDEBUG
			prt(("s2235 parseParam->selColVec i=%d name=[%s] asName=[%s]\n", 
				 i, pass->parseParam->selColVec[i].name.c_str(), pass->parseParam->selColVec[i].asName.c_str() ));
		    #endif
		}
	}

	if ( pass->parseParam->hasWhere ) {
		//prt(("s0377 pass->parseParam->hasWhere \n" ));
		root = pass->parseParam->whereVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, 
							      treestr, typeMode, tabnum );
		if (  rc < 0 ) {
			prt(("E3006 Error where range"));
			return NULL;
		}
	}

	Jstr iscmd;
	formatInsertSelectCmdHeader( pass->parseParam, iscmd );

	char *buf = (char*)jagmalloc(pass->kvlen+1); memset(buf, 0, pass->kvlen+1);
	char *sendbuf = (char*)jagmalloc(pass->sendlen+1); memset(sendbuf, 0, pass->sendlen+1);
	const char *buffers[1]; buffers[0] = buf;
	JagDiskArrayFamily *fdarr = NULL;
	if ( pass->ptab ) fdarr = pass->ptab->_darrFamily;
	else fdarr = pass->pindex->_darrFamily;
	
	if ( pass->parseParam->hasOrder && !pass->parseParam->orderVec[0].isAsc ) {
		JagMergeBackReader *ntr = NULL;
		fdarr->setFamilyReadBackPartial( ntr, pass->minbuf, pass->maxbuf, pass->spos, pass->epos, pass->memlimit );
	
		if ( ntr ) {
			while ( 1 ) {
				if ( !pass->parseParam->hasExport && checkCmdTimeout( pass->starttime, pass->parseParam->timeout ) ) {
					pass->timeoutFlag = 1;
					break;
				}

				rc = ntr->getNext( buf );  ////// read a new row

				if ( pass->req.session->sessionBroken ) rc = false;
				if ( !rc ) { break; }
				dbNaturalFormatExchange( buf, numKeys[0], attrs[0], 0,0, " " ); // db format -> natural format
				if ( pass->parseParam->hasWhere ) {
					root = pass->parseParam->whereVec[0].tree->getRoot();
					rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
				} else {
					rc = 1;
				}

				if ( rc == 1 ) {
					if ( pass->gmdarr ) { // has group by
						rc = JagTable::buildDiskArrayForGroupBy( ntr, maps, attrs, pass->req, buffers, pass->parseParam, 
																 pass->gmdarr, sendbuf );
						if ( 0 == rc ) break;
					} else { // no group by
						if ( hasAggregate ) { // has aggregate functions
							JagTable::aggregateDataFormat( ntr, maps, attrs, pass->req, buffers, pass->parseParam, !hasFirst );
						} else { // non aggregate functions
							JagTable::nonAggregateFinalbuf( ntr, maps, attrs, pass->req, buffers, pass->parseParam, 
												            sendbuf, pass->sendlen, pass->jda, pass->writeName, *(pass->recordcnt), 
															pass->nowherecnt, pass->nrec, false );
							if ( JAG_INSERTSELECT_OP == pass->parseParam->opcode ) {
								JagTable::formatInsertFromSelect( pass->parseParam, attrs[0], sendbuf, buffers[0], pass->sendlen, numCols[0], 
																  pass->cli, *(pass->recordcnt), iscmd );
							}
							if ( pass->parseParam->hasLimit && *(pass->recordcnt) >= pass->actlimit ) break;
						}
					}
					hasFirst = true;
				}	
			}
		}
		if ( ntr ) delete ntr;
	} else {
		// no order
		//prt(("s8302 no order range select\n" ));
		JagMergeReader *ntr = NULL;
        fdarr->setFamilyReadPartial( ntr, pass->minbuf, pass->maxbuf, pass->spos, pass->epos, pass->memlimit );

		if ( ntr ) {			
			while ( 1 ) {
				if ( !pass->parseParam->hasExport && checkCmdTimeout( pass->starttime, pass->parseParam->timeout ) ) {
					prt(("s1929 timeoutFlag set to 1  pass->starttime=%ld pass->parseParam->timeout=%ld\n", 
						  pass->starttime, pass->parseParam->timeout ));
					pass->timeoutFlag = 1;
					break;
				}

				rc = ntr->getNext( buf );  // read a new row
				//prt(("s3366 ntr->getNext( buf ) buf=[%s] rc=%d\n", buf, rc ));

				if ( pass->req.session->sessionBroken ) rc = false;
				if ( !rc ) { 
					if ( pass->parseParam->hasWhere ) {
						root = pass->parseParam->whereVec[0].tree->getRoot();
    					if ( root && root->_builder->_pparam->_rowHash ) {
    						//prt(("s2042 root->_builder->_pparam->_rowHash=%0x\n", root->_builder->_pparam->_rowHash ));
    						Jstr str =  root->_builder->_pparam->_rowHash->getKVStrings("#");
    						if ( ! pass->parseParam->parent->_lineFile ) {
    							pass->parseParam->parent->_lineFile = new JagLineFile();
    						} 
    						//prt(("s2138 _lineFile->append(%s) _lineFile=%x\n", str.c_str(), pass->parseParam->parent->_lineFile ));
    						pass->parseParam->parent->_lineFile->append( str );
    					} 
					}

					break; 
				}

				dbNaturalFormatExchange( buf, numKeys[0], attrs[0],0,0, " " ); // db format -> natural format
				if ( pass->parseParam->hasWhere ) {
					root = pass->parseParam->whereVec[0].tree->getRoot();
					rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
					//prt(("s2021 checkFuncValid rc=%d root=%0x strres=[%s]\n", rc, root, strres.c_str() ));

					if ( root->_builder->_pparam->_rowHash ) {
						//prt(("s2022 root->_builder->_pparam->_rowHash=%0x\n", root->_builder->_pparam->_rowHash ));
						if ( rc ) {
							Jstr str =  root->_builder->_pparam->_rowHash->getKVStrings("#");
							if ( ! pass->parseParam->parent->_lineFile ) {
								pass->parseParam->parent->_lineFile = newObject<JagLineFile>();
							} 
							//prt(("s2038 _lineFile->append(%s) _lineFile=%x\n", str.c_str(), pass->parseParam->parent->_lineFile ));
							pass->parseParam->parent->_lineFile->append( str );
						}
					} else {
						//prt(("s2023 root->_builder->_pparam->_rowHash=NULL root=%0x\n", root ));
					}
				} else {
					rc = 1;
				}

				if ( rc == 1 ) {
					if ( pass->gmdarr ) { // has group by
						rc = JagTable::buildDiskArrayForGroupBy( ntr, maps, attrs, pass->req, buffers, 
															     pass->parseParam, pass->gmdarr, sendbuf );
						if ( 0 == rc ) break;
					} else { // no group by
						if ( hasAggregate ) { // has aggregate functions
							JagTable::aggregateDataFormat( ntr, maps, attrs, pass->req, buffers, pass->parseParam, !hasFirst );
						} else { // non aggregate functions
							//prt(("s0296 JagTable::nonAggregateFinalbuf recordcnt=%d ...\n", (int)*(pass->recordcnt) ));
							JagTable::nonAggregateFinalbuf( ntr, maps, attrs, pass->req, buffers, pass->parseParam, sendbuf, 
														   pass->sendlen, pass->jda, 
															pass->writeName, *(pass->recordcnt), pass->nowherecnt, pass->nrec, false );
							if ( JAG_INSERTSELECT_OP == pass->parseParam->opcode ) {			
								JagTable::formatInsertFromSelect( pass->parseParam, attrs[0], sendbuf, buffers[0], 
																pass->sendlen, numCols[0], 
																  pass->cli, *(pass->recordcnt), iscmd );
							}
							if ( pass->parseParam->hasLimit && *(pass->recordcnt) >= pass->actlimit ) break;
						}
					}
					hasFirst = true;
				}	
			}
		}
		if ( ntr ) delete ntr;
	}

	free( sendbuf );
	free( buf );
}

// return 0: error or no more data accepted
// return 1: success
// method to calculate group by value, the rearrange buffer to insert into diskarray
// multiple threads calling
// input buffers are natural data
int JagTable::buildDiskArrayForGroupBy( JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
										const JagRequest &req, const char *buffers[], JagParseParam *parseParam, 
										JagMemDiskSortArray *gmdarr, char *gbvbuf )
{
	Jstr treetype;
	abaxint totoff = 0;
	ExprElementNode *root;
	Jstr errmsg;
	bool init = 1;
	int rc = 0, treelength = 0, typeMode = 0, treesig;

	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		init = 1;
		root = parseParam->selColVec[i].tree->getRoot();
		if ( root->checkFuncValid( ntr, maps, attrs, buffers, parseParam->selColVec[i].strResult, typeMode, 
								   treetype, treelength, init, 1, 1 ) != 1 ) {
			return 0;
		}
		
		treelength = parseParam->selColVec[i].length;
		treesig = parseParam->selColVec[i].sig;
		treetype = parseParam->selColVec[i].type;
		formatOneCol( req.session->timediff, req.session->servobj->servtimediff, gbvbuf, 
			parseParam->selColVec[i].strResult.c_str(), errmsg, "GARBAGE", 
			totoff, treelength, treesig, treetype );
		totoff += treelength;
		// prt(("i=%d gbvbuf=[%s] len=%d sig=%d typ=%c\n", i, gbvbuf, treelength, treesig, treetype));
	}

	// then, insert data to groupby value diskarray
	if ( gbvbuf[0] != '\0' ) {	
		JagDBPair pair(gbvbuf, gmdarr->_keylen, gbvbuf+gmdarr->_keylen, gmdarr->_vallen, true );
		rc = gmdarr->groupByUpdate( pair );
		rc = 1;
	} else {
		rc = 1;
	}
	return rc;
}

// main thread calling
// input buffers are natural data
void JagTable::groupByFinalCalculation( char *gbvbuf, bool nowherecnt, abaxint finalsendlen, std::atomic<abaxint> &cnt, abaxint actlimit,
										const JagRequest &req, const Jstr &writeName, JagParseParam *parseParam, 
										JagDataAggregate *jda, JagMemDiskSortArray *gmdarr, const JagSchemaRecord *nrec )
{
	//prt(("s9282 groupByFinalCalculation ...\n" ));
	gmdarr->endWrite();
	gmdarr->beginRead();
	while ( true ) {
		if ( !gmdarr->get( gbvbuf ) ) { break; }
		if ( !nowherecnt ) ++cnt;
		else {
			if ( *gbvbuf != '\0' ) {
				if ( jda ) doWriteIt( jda, parseParam, req, writeName, gbvbuf, finalsendlen, nrec ); 
				++cnt;
				if ( parseParam->hasLimit && cnt >= actlimit ) {
					break;
				}
			}
		}
	}
	gmdarr->endRead();
}

// method to finalize non aggregate range query, point query data
// multiple threads calling
// input buffers are natural data
void JagTable::nonAggregateFinalbuf(JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									const JagRequest &req, const char *buffers[], 
									JagParseParam *parseParam, char *finalbuf, abaxint finalsendlen,
									JagDataAggregate *jda, const Jstr &writeName, std::atomic<abaxint> &cnt, 
									bool nowherecnt, const JagSchemaRecord *nrec, bool oneLine )
{
	prt(("s4817 nonAggregateFinalbuf parseParam->hasColumn=%d\n", parseParam->hasColumn ));
	if ( parseParam->hasColumn ) {
		memset(finalbuf, 0, finalsendlen);
		Jstr treetype;
		int rc, typeMode = 0, treelength = 0, treesig = 0;
		bool init = 1;
		abaxint offset;
		ExprElementNode *root;
		Jstr errmsg;
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			init = 1;
			root = parseParam->selColVec[i].tree->getRoot();
			rc = root->checkFuncValid( ntr, maps, attrs, buffers, parseParam->selColVec[i].strResult, typeMode, 
									   treetype, treelength, init, 1, 1 );
			//prt(("s7263 strResult=[%s]\n", parseParam->selColVec[i].strResult.c_str() ));
			//prt(("s2082 in JagTable::nonAggregateFinalbuf() checkFuncValid rc=%d\n", rc ));
			if ( rc != 1 ) { return; }
			offset = parseParam->selColVec[i].offset;
			treelength = parseParam->selColVec[i].length;
			treesig = parseParam->selColVec[i].sig;
			treetype = parseParam->selColVec[i].type;
			formatOneCol( req.session->timediff, req.session->servobj->servtimediff, finalbuf,
						  parseParam->selColVec[i].strResult.c_str(), errmsg, "GARBAGE", offset, 
						  treelength, treesig, treetype );
		    //prt(("s3981 formatOneCol done\n" ));
		}
	}
	
	if ( JAG_GETFILE_OP == parseParam->opcode ) {
		// if getfile, arrange point query to be size of file/modify time of file/md5sum of file/file path ( for actural data )
		Jstr ddcol, inpath, instr, outstr; 
		abaxint getpos; char fname[JAG_FILE_FIELD_LEN+1]; struct stat sbuf;
		raydebug( stdout, JAG_LOG_HIGH, "s0391 JAG_GETFILE_OP selColVec.size()=%d\n", parseParam->selColVec.size() );
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			snprintf(finalbuf+parseParam->selColVec[i].offset, parseParam->selColVec[i].length+1, "%s", 
					 parseParam->selColVec[i].strResult.c_str());
		}
	}
	
	if ( oneLine ) { // point query
		if ( !nowherecnt ) cnt = -111; // select count(*) with where, no actual data sentry
		else { // one line data, direct send to client
			if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
				if ( parseParam->hasColumn || JAG_GETFILE_OP == parseParam->opcode ) { 
				    // has select coulumn or getfile command, use finalbuf
					sendMessageLength( req, finalbuf, finalsendlen, "X1" );
				} else { // select *, use original buf, buffer[0]
					sendMessageLength( req, buffers[0], finalsendlen, "X1" );
				}
				cnt = -111;
			}
		}
	} else { // range, non aggregate query
		if ( !nowherecnt ) ++cnt; // select count(*) with where, no actual data sentry
		else { // range data, write to data aggregate file
			if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
				if ( parseParam->hasColumn ) { // has select coulumn, use finalbuf
					//prt(("s0281 finalbuf=[%s]\n", finalbuf ));
					if ( *finalbuf != '\0' ) {
						if ( jda ) doWriteIt( jda, parseParam, req, writeName, finalbuf, finalsendlen, nrec ); 
						++cnt;
						//prt(("s2200 cnt=%d\n", (int)cnt ));
					}
				} else { // select *, use original buf, buffer[0]
					if ( *buffers[0] != '\0' ) {
						if ( jda ) {
							//prt(("s2238 doWriteIt ...finalsendlen=%d\n", finalsendlen ));
							doWriteIt( jda, parseParam, req, writeName, buffers[0], finalsendlen, nrec ); 
							++cnt;
						}
					}
				}
			}
		}
	}
}

// must be range query, and must have select column, do aggregation 
// multiple threads calling
// input buffers are natural data
void JagTable::aggregateDataFormat( JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									const JagRequest &req, const char *buffers[], JagParseParam *parseParam, bool init )
{
	ExprElementNode *root;
	int typeMode = 0, treelength = 0;
	Jstr treetype;
	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		root = parseParam->selColVec[i].tree->getRoot();
		if ( root->checkFuncValid( ntr, maps, attrs, buffers, parseParam->selColVec[i].strResult, typeMode, 
								   treetype, treelength, init, 1, 1 ) != 1 ) {
			prt(("ERROR i=%d [%s]\n", i, parseParam->selColVec[i].asName.c_str()));
			return;
		}
	}
}

// finish all data, merge all aggregation results to form one final result to send
// main thread calling
// input buffers are natural data
void JagTable::aggregateFinalbuf( const JagRequest &req, const Jstr &sendhdr, abaxint len, JagParseParam *parseParam[], 
								  char *finalbuf, abaxint finalsendlen, JagDataAggregate *jda, const Jstr &writeName, 
								  std::atomic<abaxint> &cnt, bool nowherecnt, const JagSchemaRecord *nrec )
{
	JagSchemaRecord aggrec;
	//prt(("c2036 aggregateFinalbuf sendhdr=[%s]\n", sendhdr.c_str() ));
	int rc = aggrec.parseRecord( sendhdr.c_str() );
	char *aggbuf = (char*)jagmalloc(finalsendlen+1);
	memset(aggbuf, 0, finalsendlen+1);
	JagVector<JagFixString> oneSetData;
	JagVector<int> selectPartsOpcode;
	
	Jstr type;
	abaxint offset, length, sig;
	ExprElementNode *root;
	Jstr errmsg;
	
	for ( int m = 0; m < len; ++m ) {
		memset(finalbuf, 0, finalsendlen);
		for ( int i = 0; i < parseParam[m]->selColVec.size(); ++i ) {
			root = parseParam[m]->selColVec[i].tree->getRoot();
			offset = parseParam[m]->selColVec[i].offset;
			length = parseParam[m]->selColVec[i].length;
			sig = parseParam[m]->selColVec[i].sig;
			type = parseParam[m]->selColVec[i].type;
			formatOneCol( req.session->timediff, req.session->servobj->servtimediff, finalbuf,
				parseParam[m]->selColVec[i].strResult.c_str(), errmsg, "GARBAGE", 
				offset, length, sig, type );
			if ( 0 == m ) selectPartsOpcode.append( root->getBinaryOp() );
		}
		oneSetData.append( JagFixString(finalbuf, finalsendlen) );		
	}

	JaguarCPPClient::doAggregateCalculation( aggrec, selectPartsOpcode, oneSetData, aggbuf, finalsendlen, 0 );

	//prt(("s2879 nowherecnt=%d aggbuf=[%s] jda=%0x\n", nowherecnt, aggbuf, jda ));
	if ( nowherecnt ) { // send actural data
		if ( *aggbuf != '\0' ) {
			if ( jda ) {
				doWriteIt( jda, parseParam[0], req, writeName, aggbuf, finalsendlen, nrec );
			}
			++cnt;
			//prt(("s2287 cnt incremented to %d\n", (int)cnt ));
		}
	}

	free( aggbuf );
}

Jstr JagTable::drop( Jstr &errmsg, bool isTruncate )
{
	JagIndex *pindex = NULL;
	Jstr idxNames;
	if ( ! _darrFamily ) return idxNames;
	if ( _indexlist.size() > 0 ) {
		if ( isTruncate ) {	
			// if isTruncate, store names to indexNames
			for ( int i = 0; i < _indexlist.size(); ++i ) {
				if ( idxNames.size() < 1 ) {
					idxNames = _indexlist[i];
				} else {
					idxNames += Jstr("|") + _indexlist[i];
				}
			}
		}
		Jstr dbtab = _dbname + "." + _tableName;
		Jstr tabIndex;
		Jstr dbtabIndex;
		while ( _indexlist.size() > 0 ) {
			pindex = _servobj->_objectLock->writeLockIndex( JAG_DROPINDEX_OP, _dbname, _tableName, _indexlist[_indexlist.size()-1],
				_tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[_indexlist.size()-1];
				if ( !isTruncate ) {
					_indexschema->remove( dbtabIndex );
				}
				if ( pindex ) delete pindex; pindex = NULL;
				_servobj->_objectLock->writeUnlockIndex( JAG_DROPINDEX_OP, _dbname, _tableName, _indexlist[_indexlist.size()-1],
					_tableschema, _indexschema, _replicateType, 1 );
			}
			_indexlist.removepos( _indexlist.size()-1 );				
		}
	}	

	// prt(("s7373 _darrFamily->_tablepath=[%s]\n", _darrFamily->_tablepath.c_str() ));

	_darrFamily->drop();
	if ( _darrFamily ) delete _darrFamily;
	_darrFamily = NULL;
	// rmdir
	Jstr rdbdatahome = _cfg->getJDBDataHOME( _replicateType ), dbpath = rdbdatahome + "/" + _dbname;
	JagFileMgr::rmdir( dbpath + "/" + _tableName, true );

	jagmalloc_trim(0);

	return idxNames;
}

void JagTable::dropFromIndexList( const Jstr &indexName )
{
	for ( int k = 0; k < _indexlist.size(); ++k ) {
		const char *idxName = strrchr(_indexlist[k].c_str(), '.');
		if ( !idxName ) {
			continue;
		}
		++idxName;

		if ( strcmp(indexName.c_str(), idxName) == 0 ) {
			_indexlist.removepos( k );
			break;
		}
	}
}

// find columns in buf that are keys of some index, and removed the index records
int JagTable::_removeIndexRecords( const char *buf )
{
	// printf("s2930 _removeIndexRecords _indexlist.size=%d\n", _indexlist.size() );
	if ( _indexlist.size() < 1 ) return 0;

	bool rc;
	AbaxBuffer bfr;
	JagIndex *pindex = NULL;

	int cnt = 0;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		pindex = _servobj->_objectLock->readLockIndex( JAG_DELETE_OP, _dbname, _tableName, _indexlist[i],
			_tableschema, _indexschema, _replicateType, 1 );
		if ( pindex ) {
			pindex->removeFromTable( buf );
			++ cnt;
			_servobj->_objectLock->readUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
				_tableschema, _indexschema, _replicateType, 1 );
		}
	}

	return cnt;
}

// return "db.idx1,db.idx2,db.idx3" 
Jstr JagTable::getIndexNameList()
{
	Jstr res;
	if ( ! _darrFamily ) return res;
	if ( _indexlist.size() < 1 ) { return res; }

	for ( int i=0; i < _indexlist.length(); ++i ) {
		if ( res.size() < 1 ) {
			res = _indexlist[i];
		} else {
			res += Jstr(",") + _indexlist[i];
		}
	}

	return res;
}

int JagTable::renameIndexColumn( const JagParseParam *parseParam, Jstr &errmsg )
{
	JagIndex *pindex = NULL;
	AbaxBuffer bfr;
	bool rc;
	if ( _indexlist.size() > 0 ) {
		Jstr dbtab  = _dbname + "." + _tableName;
		Jstr dbtabIndex;
		for ( int k = 0; k < _indexlist.size(); ++k ) {
			pindex = _servobj->_objectLock->writeLockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
				_tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[k];
				_indexschema->renameColumn( dbtabIndex, parseParam );
				pindex->refreshSchema();
				_servobj->_objectLock->writeUnlockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
					_tableschema, _indexschema, _replicateType, 1 );
			}
		}
	}

	return 1;	
}

int JagTable::setIndexColumn( const JagParseParam *parseParam, Jstr &errmsg )
{
	JagIndex *pindex = NULL;
	AbaxBuffer bfr;
	bool rc;
	if ( _indexlist.size() > 0 ) {
		Jstr dbtab  = _dbname + "." + _tableName;
		Jstr dbtabIndex;
		for ( int k = 0; k < _indexlist.size(); ++k ) {
			pindex = _servobj->_objectLock->writeLockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
				_tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[k];
				_indexschema->setColumn( dbtabIndex, parseParam );
				pindex->refreshSchema();
				_servobj->_objectLock->writeUnlockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
					_tableschema, _indexschema, _replicateType, 1 );
			}
		}
	}

	return 1;	
}

// send to socket with header
abaxint JagTable::sendMessage( const JagRequest &req, const char *mesg, const char *type )
{
    abaxint len = strlen( mesg );
	// prt(("s7383 sendMessage mesg=[%s]\n", mesg ));
	return sendMessageLength2( req.session, mesg, len, type );
}

// send to socket with header
abaxint JagTable::sendMessageLength( const JagRequest &req, const char *mesg, abaxint len, const char *type )
{
	return sendMessageLength2( req.session, mesg, len, type );
}

// actual send to socket with header
abaxint JagTable::sendMessageLength2( JagSession *session, const char *mesg, abaxint len, const char *type ) 
{
	char *buf = NULL;
	abaxint rc = 0;
    if ( strlen( type ) < 2 ) { if ( buf ) free( buf ); return -200; }
	//if ( len > 1 ) { prt(("s2800 sock=%d SENDMEGLEN [%s], len=%lld\n", session->sock, mesg, len)); }

	// check if message is last one, e.g. "_END_" for mesg or "X1" for type
	int lastone = false;
	if ( strncmp( mesg, "_END_", 5 ) == 0 || (type[0] == 'X' && type[1] == '1') || (type[0] == 'S' && type[1] == 'S') ) {
		lastone = true;
	}
	// check if message is hard bit
	int isHB = false;
	if ( type[0] == 'H' && type[1] == 'B' ) {
		isHB = true;
	}

	#if 0
    if ( !isHB ) { 
		prt(("s2800 THREADID=%ld sock=%d SENDMEGLEN [%s], len=%lld\n", THREADID, session->sock, mesg, len));
	}
	#endif

	// compress or no: if no, len is original; if yes, use new length
	if ( len >= JAG_SOCK_COMPRSS_MIN ) {
		Jstr comp;
		JagFastCompress::compress( mesg, len, comp );
		buf = (char*)jagmalloc( JAG_SOCK_MSG_HDR_LEN+comp.size()+1+64 );
		sprintf( buf, "%0*lldZ%c%cC", JAG_SOCK_MSG_HDR_LEN-4, comp.size(), type[0], type[1] );
		len = comp.size();
		memcpy( buf+JAG_SOCK_MSG_HDR_LEN, comp.c_str(), len );
	} else {
 		buf = (char*)jagmalloc( JAG_SOCK_MSG_HDR_LEN+len+1+64 );
    	sprintf( buf, "%0*lldC%c%cC", JAG_SOCK_MSG_HDR_LEN-4, len, type[0], type[1] );
		memcpy( buf+JAG_SOCK_MSG_HDR_LEN, mesg, len );
	}
    buf[JAG_SOCK_MSG_HDR_LEN+len] = '\0';

	jaguar_mutex_lock ( &session->dataMutex );
	if ( !isHB ) {
		rc = sendData( session->sock, buf, JAG_SOCK_MSG_HDR_LEN+len );
	}
	else if ( session->hasTimer ) rc = sendData( session->sock, buf, JAG_SOCK_MSG_HDR_LEN+len );
	if ( rc < 0 ) session->sessionBroken = 1; // session send error, broken 
	jaguar_mutex_unlock ( &session->dataMutex );

	if ( rc < JAG_SOCK_MSG_HDR_LEN+len ) {
		rc = -1;
	}
	if ( buf ) free( buf );
    return rc;
}

int JagTable::refreshSchema()
{
	Jstr dbtable = _dbname + "." + _tableName;
	Jstr dbcolumn;
	AbaxString schemaStr;
	const JagSchemaRecord *record =  _tableschema->getAttr( dbtable );
	if ( ! record ) {
		prt(("s8912 _tableschema->getAttr(%s) error, return 0, _tableRecord is not updated\n", dbtable.c_str() ));
		return 0;
	}

	#if 0
	prt(("s7640 _tableRecord is updated with *record record->columnVector=%0x print:\n", record->columnVector ));
	_tableRecord = *record;
	//_tableRecord.print();
	#endif

	if ( _schAttr ) {
		delete [] _schAttr;
	}

	// prt(("s2008 _origHasSpare=%d _numCols=%d\n", _origHasSpare, _numCols ));
	_numCols = record->columnVector->size();
	/***
	_origHasSpare = hasSpareColumn();
	prt(("s2009 _origHasSpare=%d _numCols=%d\n", _origHasSpare, _numCols ));
	if ( ! _origHasSpare ) {
		++ _numCols; //add extram empty spare_ col
	}
	prt(("s2009 _origHasSpare=%d _numCols=%d\n", _origHasSpare, _numCols ));
	***/

	_schAttr = new JagSchemaAttribute[_numCols];

	// refresh
	_tableRecord = *record;

	_schAttr[0].record = _tableRecord;

	if ( _tablemap ) {
		delete _tablemap;
	}
	//_tablemap = new JagHashStrInt();
	_tablemap = newObject<JagHashStrInt>();
	prt(("s4982 created a new _tablemap \n" ));

	// setupSchemaMapAttr( _numCols, _origHasSpare );
	setupSchemaMapAttr( _numCols );

	return 1;
}

int JagTable::refreshIndexList()
{
	// create index : _indexlist.append( dbindex 
	JagVector<Jstr> vec;
	int cnt = _indexschema->getIndexNames( _dbname, _tableName, vec );
	if ( cnt < 1 ) return 0;

	Jstr dbindex, idxname;
	_indexlist.destroy();
	_indexlist.init( 8 );

  	char buf[32];
	for ( int i = 0; i < vec.size(); ++i ) {	
		idxname = vec[i];
    	dbindex = _dbname + "." + idxname;
		// printf("s8380 _indexlist.append(%s)\n", dbindex.c_str() );
		 _indexlist.append( dbindex );
	}

	return 1;
}

void JagTable::flushBlockIndexToDisk()
{
	// flush table 
	if ( _darrFamily ) { _darrFamily->flushBlockIndexToDisk(); }

	// then flush related indexs
	JagIndex *pindex;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		pindex = _servobj->_objectLock->readLockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
			_tableschema, _indexschema, _replicateType, 1 );
		if ( pindex ) {
			pindex->flushBlockIndexToDisk();
			_servobj->_objectLock->readUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
				_tableschema, _indexschema, _replicateType, 1 );
		}
	}
}

void JagTable::copyAndInsertBufferAndClean( JagIndex *ipindex )
{
	// flush table 
	if ( _darrFamily ) { _darrFamily->copyAndInsertBufferAndClean(); }

	// then flush related indexs
	JagIndex *pindex;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		if ( ipindex && ipindex->getdbName() == _dbname && ipindex->getTableName() == _tableName && ipindex->getIndexName() == _indexlist[i] ) {
			ipindex->copyAndInsertBufferAndClean();
		} else {
			pindex = _servobj->_objectLock->readLockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
				_tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->copyAndInsertBufferAndClean();
				_servobj->_objectLock->readUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
					_tableschema, _indexschema, _replicateType, 1 );
			}
		}
	}
}

// static
// currently, doWriteIt has only one host name, so it can only been write one by one ( use mutex in jda to protect, with last flag true )
void JagTable::doWriteIt( JagDataAggregate *jda, const JagParseParam *parseParam, const JagRequest &req, 
						  const Jstr &host, const char *buf, abaxint buflen, const JagSchemaRecord *nrec )
{

	// first check if there is any _having clause
	// if having exists, convert natrual format to dbformat,
	if ( parseParam->hasHaving ) {
		// todo having tree
		// ExprElementNode *root = (parseParam->havingTree).getRoot();
	} else {
		if ( jda ) jda->writeit( host, buf, buflen, nrec, true );
	}
}

// static
// format insert into cmd, and use pcli to find servers to be sent for insert
// iscmd should be either "insert into TABLE (COL1,COL2)" or "insert into TABLE" format
void JagTable::formatInsertFromSelect( const JagParseParam *parseParam, const JagSchemaAttribute *attrs, 
	const char *finalbuf, const char *buffers, abaxint finalsendlen, abaxint numCols,
	JaguarCPPClient *pcli, std::atomic<abaxint> &cnt, const Jstr &iscmd )
{
	// format insert cmd
	Jstr insertCmd = iscmd + " values ( ", oneData;
	if ( parseParam->hasColumn ) {
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			oneData = formOneColumnNaturalData( finalbuf, parseParam->selColVec[i].offset, 
				parseParam->selColVec[i].length, parseParam->selColVec[i].type );
			if ( 0 == i ) {
				insertCmd += Jstr("'") + oneData.c_str() + "'";
			} else {
				insertCmd += Jstr(",'") + oneData.c_str() + "'";
			}
		}
		insertCmd += ");";
	} else {
		for ( int i = 0; i < numCols; ++i ) {
			if ( attrs[i].colname != "spare_" ) {
				oneData = formOneColumnNaturalData( buffers, attrs[i].offset, attrs[i].length, attrs[i].type );
				if ( 0 == i ) {
					insertCmd += Jstr("'") + oneData.c_str() + "'";
				} else {
					insertCmd += Jstr(",'") + oneData.c_str() + "'";
				}
			}			
		}
		insertCmd += ");";
	}
	
	// put insert cmd into cli
	pcli->concurrentDirectInsert( insertCmd.c_str() );
}

bool JagTable::hasSpareColumn()
{
	Jstr colname;
	for ( int i = 0; i < _numCols; ++i ) {
		colname = (*(_tableRecord.columnVector))[i].name.c_str();
		// prt(("s4772 hasSpareColumn i=%d colname=[%s]\n", i, colname.c_str() ));
		if ( colname == "spare_" ) return true;
	}
	return false;
}

void JagTable::setupSchemaMapAttr( int numCols )
{
	prt(("s3800 JagTable::setupSchemaMapAttr numCols=%d ...\n", numCols ));
	/**
	prt(("s2039 setupSchemaMapAttr numCols=%d\n", numCols ));
	int  N;
	if ( hasSpare ) N = numCols;
	else N = numCols-1;
	**/
	int N = numCols;

	_numKeys = 0;
	int i = 0, rc, rc2;
	int j = 0;
	Jstr dbcolumn, defvalStr;
	for ( i = 0; i < N; ++i ) {
		if ( _schAttr[i].isKey ) { ++ _numKeys; }
		dbcolumn = _dbtable + "." + (*(_tableRecord.columnVector))[i].name.c_str();
		
		_schAttr[i].dbcol = dbcolumn;
		_schAttr[i].objcol = _tableName + "." + (*(_tableRecord.columnVector))[i].name.c_str();
		_schAttr[i].colname = (*(_tableRecord.columnVector))[i].name.c_str();
		_schAttr[i].isKey = (*(_tableRecord.columnVector))[i].iskey;
		_schAttr[i].offset = (*(_tableRecord.columnVector))[i].offset;
		_schAttr[i].length = (*(_tableRecord.columnVector))[i].length;
		_schAttr[i].sig = (*(_tableRecord.columnVector))[i].sig;
		_schAttr[i].type = (*(_tableRecord.columnVector))[i].type;
		_schAttr[i].begincol = (*(_tableRecord.columnVector))[i].begincol;
		_schAttr[i].endcol = (*(_tableRecord.columnVector))[i].endcol;
		_schAttr[i].srid = (*(_tableRecord.columnVector))[i].srid;
		_schAttr[i].isUUID = false;
		_schAttr[i].isFILE = false;

		_tablemap->addKeyValue(dbcolumn, i);
		//prt(("s4015 _tablemap->addKeyValue(dbcolumn=[%s] ==> i=%d\n", dbcolumn.c_str(), i ));

		rc = *((*(_tableRecord.columnVector))[i].spare+1);
		rc2 = *((*(_tableRecord.columnVector))[i].spare+4);
		if ( rc == JAG_C_COL_TYPE_UUID_CHAR ) {
			_schAttr[i].isUUID = true;
		} else if ( rc == JAG_C_COL_TYPE_FILE_CHAR ) {
			_schAttr[i].isFILE = true;
		}
		
		// setup enum string list and/or default value for empty
		if ( rc == JAG_C_COL_TYPE_ENUM_CHAR || rc2 == JAG_CREATE_DEFINSERTVALUE ) {
			defvalStr = "";
			_tableschema->getAttrDefVal( dbcolumn, defvalStr );
			// for default string, strsplit with '|', and use last one as default value
			// regular type has only one defvalStr and use it
			// enum type has many default strings separated by '|', where the first several strings are enum range
			// and the last one is default value if empty
			JagStrSplitWithQuote sp( defvalStr.c_str(), '|' );
			
			if ( rc == JAG_C_COL_TYPE_ENUM_CHAR ) {
				// _schAttr[i].enumList = new JagVector<Jstr>();
				for ( int k = 0; k < sp.length()-1; ++k ) {
					_schAttr[i].enumList.append( strRemoveQuote( sp[k].c_str() ) );
					// sp.length()-1 are enum options
				}
			}
			if ( rc2 == JAG_CREATE_DEFINSERTVALUE && sp.length()>0 ) {
				_schAttr[i].defValue = strRemoveQuote( sp[sp.length()-1].c_str() );
				// last one is default value
				_defvallist.append( i );
			}
		}

		if ( _schAttr[i].length > 0 ) {
			++j;
		}
	}

}

// in kvbuf, there may be files, if so, remove them
// return: 0 no files;  N: # of files removed
int  JagTable::removeColFiles(const char *kvbuf )
{
	if ( kvbuf == NULL ) return 0;
	if ( *kvbuf == '\0' ) return 0;
	int rc = 0;
	Jstr fpath, db, tab, fname;
	JagFixString fstr = JagFixString( kvbuf, KEYLEN );
	Jstr hdir = fileHashDir( fstr );
	for ( int i = 0; i < _numCols; ++i ) {
		if ( _schAttr[i].isFILE ) {
			JagStrSplit sp( _schAttr[i].dbcol, '.' );
			if (sp.length() < 3 ) continue;
			db = sp[0];
			tab = sp[1];
			fname = Jstr( kvbuf+_schAttr[i].offset,  _schAttr[i].length );
			// fpath = jaguarHome() + "/data/" + db + "/" + tab + "/files/" + fname;
			fpath = _darrFamily->_sfilepath + "/" + hdir + "/" + fname;
			jagunlink( fpath.c_str() );

			/***
			prt(("s6360 i=%d dbcol=[%s] objcol=[%s] colname=[%s] offset=%d length=%d fpath=[%s]\n",
				i, _schAttr[i].dbcol.c_str(), _schAttr[i].objcol.c_str(), _schAttr[i].colname.c_str(),  
				_schAttr[i].offset, _schAttr[i].length, fpath.c_str() ));
			***/
			++ rc;
		}
	}
	return rc;
}

bool JagTable::isFileColumn( const Jstr &colname )
{
	Jstr col;
	for ( int i = 0; i < _numCols; ++i ) {
		if ( _schAttr[i].isFILE ) {
			JagStrSplit sp( _schAttr[i].dbcol, '.' );
			if (sp.length() < 3 ) continue;
			if ( colname == sp[2] ) {
				// prt(("s0293 colname=[%s] is a file type\n", colname.c_str() ));
				return true;
			}
		}
	}

	return false;
}

void JagTable::setGetFileAttributes( const Jstr &hdir, JagParseParam *parseParam, const char *buffers[] )
{
	// if getfile, arrange point query to be size of file/modify time of file/md5sum of file/file path ( for actural data )
	Jstr ddcol, inpath, instr, outstr; int getpos; char fname[JAG_FILE_FIELD_LEN+1]; struct stat sbuf;
	raydebug( stdout, JAG_LOG_HIGH, "s3331 setGetFileAttributes selColVec.size=%d\n", parseParam->selColVec.size() );
	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		ddcol = _dbtable + "." + parseParam->selColVec[i].getfileCol.c_str();
		if ( _tablemap->getValue(ddcol, getpos) ) {
			memcpy( fname, buffers[0]+_schAttr[getpos].offset, _schAttr[getpos].length );
			inpath = _darrFamily->_sfilepath + "/" + hdir + "/" + fname;
			if ( stat(inpath.c_str(), &sbuf) == 0 ) {
				if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_SIZE ) {
					outstr = longToStr( sbuf.st_size );
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_TIME ) {
					char octime[JAG_CTIME_LEN]; memset(octime, 0, JAG_CTIME_LEN);
 					jag_ctime_r(&sbuf.st_mtime, octime);
					octime[JAG_CTIME_LEN-1] = '\0';
					outstr = octime;
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_MD5SUM ) {
					if ( lastChar(inpath) != '/' ) {
						instr = Jstr("md5sum ") + inpath;
						outstr = Jstr(psystem( instr.c_str() ).c_str(), 32);
					} else {
						outstr = "";
					}
				} else {
					// actual data trasmit, filename only
					outstr = fname;
				}
				parseParam->selColVec[i].strResult = outstr;
			} else {
				parseParam->selColVec[i].strResult = "error file status";
				raydebug( stdout, JAG_LOG_HIGH, "s4331 error file status\n" );
			}
		} else {
			parseParam->selColVec[i].strResult = "error file column";
			raydebug( stdout, JAG_LOG_HIGH, "s4391 error file column\n" );
		}
	}
}

void JagTable::formatPointsInLineString( const JagLineString &line, char *tablekvbuf, const JagPolyPass &pass, 
										 JagVector<JagDBPair> &retpair, Jstr &errmsg )
{
	int getpos;
	Jstr point;
	char ibuf[32];
	int rc;
	Jstr pointx = pass.colname + ":x";
	Jstr pointy = pass.colname + ":y";
	Jstr pointz = pass.colname + ":z";

	//prt(("s2935 pointx=[%s] pointz=[%s] pass.getxmin=%d pass.getymin=%d \n", pointx.c_str(), pointz.c_str(), pass.getxmin, pass.getymin ));

	for ( int j=0; j < line.size(); ++j ) {
		point = "geo:xmin";
		getpos = pass.getxmin;
     	rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.xmin).c_str(), errmsg, point.c_str(), 
							_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
							_schAttr[getpos].type );

		if ( pass.getymin >= 0 ) {
    		point =  "geo:ymin";
    		getpos = pass.getymin;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.ymin).c_str(), errmsg, point.c_str(), 
								_schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
   	    if ( pass.getzmin >= 0 ) {
   			point = "geo:zmin";
   			getpos = pass.getzmin;
			if ( getpos >= 0 ) {
       				rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.zmin).c_str(), errmsg, point.c_str(), 
									_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
									_schAttr[getpos].type );
		   }
   	    }
    
		if ( pass.getxmax >= 0 ) {
   			point = "geo:xmax";
   			getpos = pass.getxmax;
   			rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.xmax).c_str(), errmsg, point.c_str(), 
								_schAttr[getpos].offset, 
    						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
    
		if ( pass.getymax >= 0 ) {
    		point =  "geo:ymax";
    		getpos = pass.getymax;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.ymax).c_str(), errmsg, point.c_str(), 
								_schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
   	    // if ( is3D && getzmax >=0 ) 
   	    if (  pass.getzmax >=0 ) {
    		point = "geo:zmax";
    		getpos = pass.getzmax;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.zmax).c_str(), errmsg, 
								point.c_str(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
   	    }

		if ( pass.getid >= 0 ) {
    		point = "geo:id";
    		getpos = pass.getid;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, pass.lsuuid.c_str(), errmsg, point.c_str(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }

		if ( pass.getcol >= 0 ) {
    		point = "geo:col";
			sprintf(ibuf, "%d", pass.col+1 );
			//prt(("s5033 getcol=%d ibuf=[%s]\n", pass.getcol, ibuf ));
    		getpos = pass.getcol;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.c_str(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }

		if ( pass.getm >= 0 ) {
    		point = "geo:m"; sprintf(ibuf, "%d", pass.m+1 ); getpos = pass.getm;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.c_str(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}

		if ( pass.getn >= 0 ) {
    		point = "geo:n"; sprintf(ibuf, "%d", pass.n+1 ); getpos = pass.getn;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.c_str(), _schAttr[getpos].offset, 
        									_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}

		if ( pass.geti >= 0 ) {
    		point = "geo:i";
			sprintf(ibuf, "%09d", j+1 );
			//prt(("s5034 geti=%d ibuf=[%s]\n", pass.geti, ibuf ));
    		getpos = pass.geti;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.c_str(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}
        
       	if ( pass.getx >= 0 ) {
    		getpos = pass.getx;
			#if 0
			prt(("s4005 getx=%d pointx=[%s] line.point[j].x=[%s]\n", getpos, pointx.c_str(), line.point[j].x ));
			prt(("_schAttr[getpos].offset=%d _schAttr[getpos].length=%d type=[%s]\n", 
					_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].type.c_str() ));
			#endif
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, line.point[j].x, errmsg, 
       	 						pointx.c_str(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
       	}
      	if ( pass.gety >= 0 ) {
    		getpos = pass.gety;
			#if 0
			prt(("s4005 gety=%d pointy=[%s] line.point[j].y=[%s]\n", getpos, pointy.c_str(), line.point[j].y ));
			prt(("_schAttr[getpos].offset=%d _schAttr[getpos].length=%d type=[%s]\n", 
					_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].type.c_str() ));
			#endif
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, line.point[j].y, errmsg, 
       		 					pointy.c_str(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
       	}
        
		//prt(("s7830 pass.is3D=%d pass.getz=%d\n", pass.is3D, pass.getz ));
       	if ( pass.is3D && pass.getz >= 0 ) {
    		getpos = pass.getz;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, 
								line.point[j].z, errmsg, 
       							pointz.c_str(), _schAttr[getpos].offset, _schAttr[getpos].length, 
								_schAttr[getpos].sig, _schAttr[getpos].type );
       	}

		dbNaturalFormatExchange( tablekvbuf, _numKeys, _schAttr, 0, 0, " " ); // natural format -> db format
		retpair.append( JagDBPair( tablekvbuf, KEYLEN, tablekvbuf+KEYLEN, VALLEN, true ) );
		/**
		prt(("s4105 retpair.append tablekvbuf\n" ));
		prt(("s4105 tablekvbuf:\n" ));
		dumpmem( tablekvbuf, KEYLEN+VALLEN);
		**/

       	if ( pass.getx >= 0 ) {
			memset(tablekvbuf+_schAttr[pass.getx].offset, 0, _schAttr[pass.getx].length );
		}
       	if ( pass.gety>= 0 ) {
			memset(tablekvbuf+_schAttr[pass.gety].offset, 0, _schAttr[pass.gety].length );
		}
       	if ( pass.is3D && pass.getz >= 0 ) {
			memset(tablekvbuf+_schAttr[pass.getz].offset, 0, _schAttr[pass.getz].length );
		}

   	} // end of all points of this linestring column

}

void JagTable::getColumnIndex( const Jstr &dbtab, const Jstr &colname, bool is3D,
								int &getx, int &gety, int &getz, int &getxmin, int &getymin, int &getzmin,
								int &getxmax, int &getymax, int &getzmax,
								int &getid, int &getcol, int &getm, int &getn, int &geti )
{
	int getpos;
	Jstr dbcolumn;
	Jstr pointx = colname + ":x";
	Jstr pointy = colname + ":y";
	Jstr pointz = colname + ":z";

	getx=gety=getz=-1;
	getxmin = getymin = getzmin = getxmax = getymax = getzmax = -1;
	getid = getcol = getm = getn = geti = -1;

	dbcolumn = dbtab + ".geo:xmin";
	prt(("s1029 in getColumnIndex() dbcolumn=[%s]\n", dbcolumn.s() ));
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getxmin = getpos; }

	dbcolumn = dbtab + ".geo:ymin";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getymin = getpos; }

	dbcolumn = dbtab + ".geo:zmin";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getzmin = getpos; }

	dbcolumn = dbtab + ".geo:xmax";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getxmax = getpos; }

	dbcolumn = dbtab + ".geo:ymax";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getymax = getpos; }

	dbcolumn = dbtab + ".geo:zmax";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getzmax = getpos; }

	dbcolumn = dbtab + ".geo:id";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getid = getpos; }

	dbcolumn = dbtab + ".geo:m";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getm = getpos; }

	dbcolumn = dbtab + ".geo:n";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getn = getpos; }

	dbcolumn = dbtab + ".geo:col";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getcol = getpos; }

	dbcolumn = dbtab + ".geo:i";
	if ( _tablemap->getValue(dbcolumn, getpos) ) { geti = getpos; }

	dbcolumn = dbtab + "." + pointx;
	if ( _tablemap->getValue(dbcolumn, getpos) ) { getx = getpos; }
	dbcolumn = dbtab + "." + pointy;
	if ( _tablemap->getValue(dbcolumn, getpos) ) { gety = getpos; }
	if ( is3D ) {
		dbcolumn = dbtab + "." + pointz;
		if ( _tablemap->getValue(dbcolumn, getpos) ) { getz = getpos; }
	}
}


