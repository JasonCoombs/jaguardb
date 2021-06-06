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
#include <JagSession.h>
#include <JagFastCompress.h>
#include <JagDataAggregate.h>
#include <JagDBConnector.h>
#include <JagBuffBackReader.h>
#include <JagServerObjectLock.h>
#include <JagStrSplitWithQuote.h>
#include <JagParser.h>
#include <JagLineFile.h>
#include <JagPriorityQueue.h>

JagTable::JagTable( int replicateType, const JagDBServer *servobj, const Jstr &dbname, const Jstr &tableName, 
					  const JagSchemaRecord &record, bool buildInitIndex ) 
		: _tableRecord(record), _servobj( servobj )
{
	prt(("s111022 JagTable ctor ...\n"));
	_cfg = _servobj->_cfg;
	_objectLock = servobj->_objectLock;
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
	_replicateType = replicateType;
	_isExporting = 0;
	KEYLEN = 0;
	VALLEN = 0;
	KEYVALLEN = 0;
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

	pthread_mutex_destroy( &_parseParamParentMutex );

}

void JagTable::init( bool buildInitIndex ) 
{
	if ( NULL != _darrFamily ) { 
		prt(("s111023 JagTable init() _darrFamily not NULL, return\n"));
		return; 
	}

	prt(("s111023 JagTable init() _darrFamily is NULL, .....\n"));
	_cfg = _servobj->_cfg;
	
	Jstr dbpath, fpath, dbcolumn;
	Jstr rdbdatahome = _cfg->getJDBDataHOME( _replicateType );
	dbpath = rdbdatahome + "/" + _dbname;

	fpath = dbpath +  "/" + _tableName + "/" + _tableName;
	JagFileMgr::makedirPath( dbpath +  "/" + _tableName );
	JagFileMgr::makedirPath( dbpath +  "/" + _tableName + "/files" );

	Jstr oldpath = dbpath +  "/" + _tableName;
	Jstr oldpathname = oldpath + ".jdb";
	Jstr newfpathname = fpath + ".jdb";
	if ( JagFileMgr::exist( oldpathname ) ) {
		jagrename( oldpathname.s(), newfpathname.s() );

		oldpathname = oldpath + ".bid";
		newfpathname = fpath + ".bid";
		if ( JagFileMgr::exist( oldpathname ) ) {
			jagrename( oldpathname.s(), newfpathname.s() );
		} else {
		}
	}

	_darrFamily = new JagDiskArrayFamily ( _servobj, fpath, &_tableRecord, 0, buildInitIndex );
	prt(("s210822 jagtable init() new _darrFamily\n"));

	KEYLEN = _tableRecord.keyLength;
	VALLEN = _tableRecord.valueLength;
	KEYVALLEN = KEYLEN + VALLEN;
	_numCols = _tableRecord.columnVector->size();	
	_schAttr = new JagSchemaAttribute[_numCols];	
	_schAttr[0].record = _tableRecord;
	_tablemap = newObject<JagHashStrInt>();

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

	pthread_mutex_init( &_parseParamParentMutex, NULL );

	_counterOffset = -1;
	_counterLength = 0;
	bool iskey;
	int totCols = _tableRecord.columnVector->size();
	for ( int i = 0; i < totCols; i ++ ) {
		iskey = (*(_tableRecord.columnVector))[i].iskey;
		if ( iskey ) continue;  // "counter" field is not in keys
		if ( 0 == strcmp( (*(_tableRecord.columnVector))[i].name.s(), "counter" ) ) {
			_counterOffset = (*(_tableRecord.columnVector))[i].offset;
			_counterLength = (*(_tableRecord.columnVector))[i].length;
			break;
		}
	}

}

void JagTable::buildInitIndexlist()
{
	int rc; Jstr dbtabindex, dbindex; 
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
			if ( _objectLock->writeLockIndex( JAG_CREATEINDEX_OP, _dbname, _tableName, vec[i],
										      _tableschema, _indexschema, _replicateType, 1 ) ) {
				_objectLock->writeUnlockIndex( JAG_CREATEINDEX_OP, _dbname, _tableName, vec[i], _replicateType, 1 );
			}		
			_indexlist.append( vec[i] );
		} 
    }
}

bool JagTable::getPair( JagDBPair &pair ) 
{ 
	if ( _darrFamily->get( pair ) ) return true;
	else return false;
}

int JagTable::parsePair( int tzdiff, JagParseParam *parseParam, JagVector<JagDBPair> &retpair, Jstr &errmsg ) const
{
	int getpos = 0;
	int metrics;
	int rc, rc2;
	
	Jstr dbcolumn, dbtab = parseParam->objectVec[0].dbName + "." + parseParam->objectVec[0].tableName;
	if ( _numCols < parseParam->otherVec.size() ) {
		prt(("s4893 Error parsePair _numCols=%d parseParam->otherVec.size()=%d\n", _numCols, parseParam->otherVec.size() ));
		errmsg = "E3143  _numCols-1 != parseParam->otherVec.size";
		return 0;
	}

	// debug
	#ifdef DEVDEBUG 
	prt(("\ns2090 _tableRecord.columnVector:\n" ));
	for ( int i = 0; i < (*_tableRecord.columnVector).size(); ++i ) {
		prt(("s1014 colvec i=%d name=[%s] type=[%s] issubcol=%d\n", 
			  i, (*_tableRecord.columnVector)[i].name.s(), (*_tableRecord.columnVector)[i].type.s(),
			  (*_tableRecord.columnVector)[i].issubcol ));
	}
	prt(("\n" ));

	prt(("s2091 initial parseParam->otherVec:\n" ));
	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		prt(("s1015  i=%d othervec name=[%s] value=[%s] type=[%s] point.x=[%s] point.y=[%s] line.point[0].x=[%s] line.point[0].y=[%s]\n", 
			i, parseParam->otherVec[i].objName.colName.s(), parseParam->otherVec[i].valueData.s(), 
			parseParam->otherVec[i].type.s(), parseParam->otherVec[i].point.x, parseParam->otherVec[i].point.y,
			parseParam->otherVec[i].linestr.point[0].x, parseParam->otherVec[i].linestr.point[0].y ));
			// i, parseParam->otherVec[i].objName.colName.s(), parseParam->otherVec[i].valueData.s(), _schAttr[i].type.s() ));
	}
	prt(("\n" ));
	#endif

	// add extra blanks in  parseParam->otherVec if it has fewer items than _tableRecord.columnVector
	#ifdef DEVDEBUG
	prt(("s8613 columnVector.size=%d otherVec.size=%d\n", (*_tableRecord.columnVector).size(), parseParam->otherVec.size() ));
	prt(("s8614 otherVec: print\n" ));
	for ( int k = 0; k < parseParam->otherVec.size(); ++k ) {
		prt(("s8614 k=%d other valuedata=[%s] issubcol=%d linestr.size=%d \n",
			k, parseParam->otherVec[k].valueData.s(), parseParam->otherVec[k].issubcol, parseParam->otherVec[k].linestr.size()));
	}
	#endif

	// find bbx coords
	double dblmax = LONG_MAX;
	double dblmin = LONG_MIN;
	double xmin=dblmax, ymin=dblmax, zmin=dblmax, xmax=dblmin, ymax=dblmin, zmax=dblmin;
	Jstr otype;

	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		rc = 0;
		//prt(("s4928 parseParam->otherVec[%d].type=[%s]\n", i, parseParam->otherVec[i].type.s() ));
		otype = parseParam->otherVec[i].type;

		if ( otype.size() > 0 ) {
    		if ( otype  == JAG_C_COL_TYPE_LINESTRING || otype == JAG_C_COL_TYPE_MULTIPOINT  ) {
    			rc = JagParser::getLineStringMinMax( ',', parseParam->otherVec[i].valueData.s(), xmin, ymin, xmax, ymax );
    		} else if ( otype == JAG_C_COL_TYPE_LINESTRING3D || otype == JAG_C_COL_TYPE_MULTIPOINT3D ) {
    			rc = JagParser::getLineString3DMinMax(',', parseParam->otherVec[i].valueData.s(), xmin, ymin, zmin, xmax, ymax, zmax );
    		} else if ( otype == JAG_C_COL_TYPE_POLYGON || otype == JAG_C_COL_TYPE_MULTILINESTRING ) {
    			rc = JagParser::getPolygonMinMax(  parseParam->otherVec[i].valueData.s(), xmin, ymin, xmax, ymax );
    		} else if ( otype == JAG_C_COL_TYPE_POLYGON3D || otype == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
    			rc = JagParser::getPolygon3DMinMax( parseParam->otherVec[i].valueData.s(), xmin, ymin, zmin, xmax, ymax, zmax );
    		} else if ( otype == JAG_C_COL_TYPE_MULTIPOLYGON ) {
    			rc = JagParser::getMultiPolygonMinMax(  parseParam->otherVec[i].valueData.s(), xmin, ymin, xmax, ymax );
    		} else if ( otype == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
    			rc = JagParser::getMultiPolygon3DMinMax(  parseParam->otherVec[i].valueData.s(), xmin, ymin, zmin, xmax, ymax, zmax );
    		}
		}

		if ( rc < 0 ) {
			char ebuf[256];
			sprintf( ebuf, "E3145 error in input data type=%s column i=%d\n", parseParam->otherVec[i].type.s(), i );
			errmsg = ebuf;
			return 0;
		}
	}

	//prt(("s8021 xmin=%0.3f ymin=%.3f zmin=%.3f\n", xmin, ymin, zmin ));
	//prt(("s8022 xmax=%0.3f ymax=%.3f zmax=%.3f\n", xmax, ymax, zmax ));

	// just need to make up matching number of columns, data is not important
	if ( (*_tableRecord.columnVector).size() > parseParam->otherVec.size() ) {
		JagVector<OtherAttribute> otherVec;

		int j = -1;
		Jstr colType;
		int numMetrics;

		for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
			#ifdef DEVDEBUG
			prt(("s6675 parseParam->hasPoly=%d _tableRecord.lastKeyColumn=%d parseParam->polyDim=%d\n",
				  parseParam->hasPoly, _tableRecord.lastKeyColumn, parseParam->polyDim ));
			#endif

			numMetrics = parseParam->otherVec[i].point.metrics.size();

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
			//prt(("s1110202 i=%d colType=[%s]\n", i, colType.s() ));
			if ( colType.size() > 0 ) {
				// prt(("s6761 i=%d colType=[%s]\n", i, colType.s() ));
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
					//prt(("s202008 here  otherVec.append \n" ));
    				otherVec.append(  parseParam->otherVec[i] );
    				// otherVec.append(  parseParam->otherVec[j] );
    			}

    			appendOther(  otherVec, numMetrics );  
    			j += numMetrics; 
			} else {
				//prt(("s20293 otherVec.append( i=%d )\n", i ));
    			otherVec.append(  parseParam->otherVec[i] );
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
			i, parseParam->otherVec[i].objName.colName.s(), parseParam->otherVec[i].valueData.s(), 
			parseParam->otherVec[i].type.s(), parseParam->otherVec[i].point.x, parseParam->otherVec[i].point.y ));

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
			k, parseParam->otherVec[k].valueData.s(), parseParam->otherVec[k].issubcol, parseParam->otherVec[k].linestr.size() ));
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

	prt(("s2503 otherVec.size()=%d\n", parseParam->otherVec.size() ));
	//prt(("s2504 _tableRecord:\n" ));

	Jstr lsuuid = _servobj->_jagUUID->getString();

	for ( int i = 0; i < parseParam->otherVec.size(); ++i ) {
		if ( parseParam->otherVec[i].issubcol ) { 
			prt(("s21003 i=%d parseParam->otherVec[i].issubcol is true name=[%s] value=[%s] skip\n", 
			i, parseParam->otherVec[i].objName.colName.s(), parseParam->otherVec[i].valueData.s() ));
			continue; 
		}

		hasDoneAppend = false;
		colname = (*_tableRecord.columnVector)[i].name.s();
		dbcolumn = dbtab + "." + colname;
		rc = _tablemap->getValue(dbcolumn, getpos);
		//prt(("s2239 _tablemap->getValue dbcolumn=[%s] rc=%d\n", dbcolumn.s(), rc ));
		if ( ! rc ) {
			free( tablekvbuf );
			errmsg = Jstr("E15003  _tablemap->getValue(") + dbcolumn + ") error i=" + intToStr(i);
			//prt(("s233032 return 0\n"));
			prt(("s20281 i=%d  dbtab=[%s] colname=[%s] issubcol=false\n", i, dbtab.s(), colname.s() ));  
			return 0;
		}

		prt(("s5031 i=%d dbcolumn=[%s] getpos=%d\n", i, dbcolumn.s(), getpos ));

		OtherAttribute &otherAttr = parseParam->otherVec[i];
		#ifdef DEVDEBUG
		prt(("s7801 otherAttr=%0x  i=%d otherAttr col=[%s]  valueData=[%s] linestr.size=%d \n", 
				&otherAttr, i, otherAttr.objName.colName.s(), otherAttr.valueData.s(), otherAttr.linestr.size() ));
		#endif

		if ( *((*(_tableRecord.columnVector))[i].spare+1) == JAG_C_COL_TYPE_ENUM[0] ) {
			if ( otherAttr.valueData.size() > 0 ) {
    			rc2 = false;
    			prt(("i=%d is enum getpos=%d vData=[%s] listlen=%d\n", i, getpos, otherAttr.valueData.s(), _schAttr[getpos].enumList.length() ));
    			for ( int j = 0; j < _schAttr[getpos].enumList.length(); ++j ) {
    				//prt(("s331109 oompare  enum=[%s]  valueData=[%s]\n", _schAttr[getpos].enumList[j].s(), otherAttr.valueData.s() ));
    				if ( strcmp( _schAttr[getpos].enumList[j].s(), otherAttr.valueData.s() ) == 0 ) {
    					rc2 = true;
    					break;
    				}
    			}
    			//prt(("s22229 rc2=%d\n", rc2 ));
    			if ( !rc2 ) {
    				free( tablekvbuf );
    				errmsg = Jstr("E12036 Error: invalid ENUM value [") + otherAttr.valueData + "]";
    				return 0;
    			}
			} else {
				/**
    			free( tablekvbuf );
    			errmsg = "E12037 Error: empty enum column value";
    			return 0;
				**/
			}
		}

		prt(("s4220228 i=%d spare+4=[%c]  othAt.vD.size=%d \n", i, *((*(_tableRecord.columnVector))[i].spare+4),  otherAttr.valueData.size() ));
		if ( *((*(_tableRecord.columnVector))[i].spare+4) == JAG_CREATE_DEFINSERTVALUE 
			 && otherAttr.valueData.size() < 1 ) {
			prt(("s33022 JAG_CREATE_DEFINSERTVALUE i=%d\n", i));
			otherAttr.valueData = _schAttr[getpos].defValue;
			prt(("s33022 JAG_CREATE_DEFINSERTVALUE otherAttr.valueData=[%s]\n", otherAttr.valueData.s() ));
		} else {
		}

		colType = (*_tableRecord.columnVector)[i].type;
		metrics = (*_tableRecord.columnVector)[i].metrics;
		//prt(("s521051 other i=%d dbcolumn=[%s] colType=[%s]\n", i, dbcolumn.s(), colType.s() ));

		if ( colType.size() > 0 ) {
    		if ( colType == JAG_C_COL_TYPE_POINT ) {
    			pointx = colname + ":x"; pointy = colname + ":y";
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
				prt(("s50025 JAG_C_COL_TYPE_POINT dbcolumn=% getpos=%d rc=%d\n", dbcolumn.s(), getpos, rc ));
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 						pointx.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
    									_schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
					prt(("s50026 JAG_C_COL_TYPE_POINT dbcolumn=% getpos=%d rc=%d\n", dbcolumn.s(), getpos, rc ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    			}
    		} else if ( colType == JAG_C_COL_TYPE_RANGE ) {
    			pointx = colname + ":begin"; pointy = colname + ":end";
    			dbcolumn = dbtab + "." + pointx;
    			//prt(("s9383 otherAttr.valueData=[%s]\n", otherAttr.valueData.s() ));
    			char sep;
    			if ( strchr( otherAttr.valueData.s(), ',') ) {
    				sep = ',';
    			} else {
    				sep = ' ';
    			}
    			JagStrSplit sp(otherAttr.valueData, sep );
    
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, sp[0].s(), errmsg, 
    			 						pointx.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
    									_schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, sp[1].s(), errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    			}
    		} else if ( colType == JAG_C_COL_TYPE_POINT3D ) {
    			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    			}
    		} else if ( colType == JAG_C_COL_TYPE_CIRCLE || colType == JAG_C_COL_TYPE_SQUARE ) {
    			pointx = colname + ":x"; pointy = colname + ":y"; pointr = colname + ":a"; 
    			pointnx = colname + ":nx"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 						pointx.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
    									_schAttr[getpos].sig, _schAttr[getpos].type );
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				} 
    				dbcolumn = dbtab + "." + pointr;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 						   pointr.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
    									   _schAttr[getpos].sig, _schAttr[getpos].type );
    				} 
    
    				if ( colType == JAG_C_COL_TYPE_SQUARE ) {
        					dbcolumn = dbtab + "." + pointnx;
        					rc = _tablemap->getValue(dbcolumn, getpos);
        					if ( rc ) {
        						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
        				 					   pointnx.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
        									   _schAttr[getpos].sig, _schAttr[getpos].type );
        					} 
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    			} 
    		} else if ( colType == JAG_C_COL_TYPE_SPHERE || colType == JAG_C_COL_TYPE_CUBE ) {
    			pointx = colname + ":x"; pointy = colname + ":y";
    			pointz = colname + ":z"; pointr = colname + ":a"; 
    			pointnx = colname + ":nx"; pointny = colname + ":ny"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointr;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointr.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if (  (*_tableRecord.columnVector)[i].type == JAG_C_COL_TYPE_CUBE ) {
    					dbcolumn = dbtab + "." + pointnx;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 				pointnx.s(), _schAttr[getpos].offset, 
    							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    					}
    
    					dbcolumn = dbtab + "." + pointny;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
    			 				pointny.s(), _schAttr[getpos].offset, 
    							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    					}
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    
    			}
    		} else if ( colType == JAG_C_COL_TYPE_CIRCLE3D || colType == JAG_C_COL_TYPE_SQUARE3D ) {
    			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
    			pointr = colname + ":a"; 
    			pointnx = colname + ":nx"; pointny = colname + ":ny"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointr;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointr.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointnx;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 			pointnx.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointny;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
    			 			pointny.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    			}
    		} else if ( colType  == JAG_C_COL_TYPE_RECTANGLE || colType == JAG_C_COL_TYPE_ELLIPSE ) {
    			pointx = colname + ":x"; pointy = colname + ":y";
    			pointw = colname + ":a"; pointh = colname + ":b"; 
    			pointnx = colname + ":nx"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointw;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointw.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointh;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
    			 			pointh.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointnx;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 			pointnx.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
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
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				//prt(("s2830 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				//prt(("s2831 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    
    				dbcolumn = dbtab + "." + pointw;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				//prt(("s2832 _tablemap->getValue %s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointw.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointh;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
    			 			pointh.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointnx;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 			pointnx.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointny;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
    			 			pointny.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
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
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointw;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointw.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointd;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.b, errmsg, 
    			 			pointd.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointh;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.c, errmsg, 
    			 			pointh.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointnx;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 			pointnx.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointny;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
    			 			pointny.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
    
    			}
    		} else if ( colType == JAG_C_COL_TYPE_CYLINDER || colType == JAG_C_COL_TYPE_CONE ) {
    			pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z"; 
    			pointr = colname + ":a"; pointh = colname + ":c"; 
    			pointnx = colname + ":nx"; pointny = colname + ":ny"; 
    
    			dbcolumn = dbtab + "." + pointx;
    			rc = _tablemap->getValue(dbcolumn, getpos);
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.x, errmsg, 
    			 		pointx.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.y, errmsg, 
    			 			pointy.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointz;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.z, errmsg, 
    			 			pointz.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointr;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.a, errmsg, 
    			 			pointr.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointh;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.c, errmsg, 
    			 			pointh.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointnx;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.nx, errmsg, 
    			 			pointnx.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    				dbcolumn = dbtab + "." + pointny;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.point.ny, errmsg, 
    			 			pointny.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.point.metrics, tablekvbuf );
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
    			 		pointx1.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy1;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].y, errmsg, 
    			 			pointy1.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if ( is3D ) {
    					dbcolumn = dbtab + "." + pointz1;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].z, errmsg, 
    			 				pointz1.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
    							_schAttr[getpos].type );
    					}
    				}
    
    				dbcolumn = dbtab + "." + pointx2;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].x, errmsg, 
    			 			pointx2.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointy2;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				//prt(("s2828 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    				//prt(("2381 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.s(), otherAttr.linestr.point[1].y ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].y, errmsg, 
    			 			pointy2.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if ( is3D ) {
    					dbcolumn = dbtab + "." + pointz2;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].z, errmsg, 
    			 				pointz2.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
    							_schAttr[getpos].type );
    					}
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.linestr.point[0].metrics, tablekvbuf );
    				//formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.linestr.point[1].metrics, tablekvbuf );
    			}
    		} else if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_LINESTRING3D
    					 || colType == JAG_C_COL_TYPE_MULTIPOINT || colType == JAG_C_COL_TYPE_MULTIPOINT3D
    			       ) {
    			//prt(("s7650 i=%d colType=[%s] colname=[%s] \n", i, colType.s(), colname.s() ));
    			//pointx = colname + ":x"; pointy = colname + ":y"; pointz = colname + ":z";
    			prt(("s2181 colType=[%s] colname=[%s]\n", colType.s(), colname.s() ));
    			is3D = false;
    			if ( colType == JAG_C_COL_TYPE_LINESTRING3D || 
    			     colType == JAG_C_COL_TYPE_MULTIPOINT3D ) { is3D = true; }
    			getColumnIndex( dbtab, colname, is3D, getx, gety, getz, getxmin, getymin, getzmin,
    								getxmax, getymax, getzmax, getid, getcol, getm, getn, geti );
    			prt(("s2234 getColumnIndex is3D=%d getxmin=%d getx=%d gety=%d getz=%d\n", is3D, getxmin, getx, gety, getz ));
    			if ( getxmin >= 0 ) {
    				JagLineString line;
    				if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_MULTIPOINT ) {
    					JagParser::addLineStringData(line, otherAttr.valueData.s() );
    					prt(("s8032 addLineStringData xmin=%0.3f ymin=%.3f zmin=%.3f\n", xmin, ymin, zmin ));
    				} else {
    					JagParser::addLineString3DData(line, otherAttr.valueData.s() );
    					prt(("s8033 addLineString3DData xmin=%0.3f ymin=%.3f zmin=%.3f vData=[%s]\n", xmin, ymin, zmin, otherAttr.valueData.s() ));
    				}
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
    				ppass.dbtab = dbtab;
    				ppass.colname = colname;
    				ppass.m = 0;
    				ppass.n = 0;
    				ppass.col = i;
    
    				formatPointsInLineString( metrics, line, tablekvbuf, ppass, retpair, errmsg );
    
    				++mlineIndex;
        			hasDoneAppend = true;
    				rc = 1;
    			}
    		} else if ( colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_POLYGON3D
    		            || colType == JAG_C_COL_TYPE_MULTILINESTRING || colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
    			//prt(("s7651 i=%d colType=[%s]\n", i, colType.s() ));
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
    						rc = JagParser::addPolygonData( pgon, otherAttr.valueData.s(), false, true );
    					} else {
    						rc = JagParser::addPolygonData( pgon, otherAttr.valueData.s(), false, false );
    					}
    					//prt(("s8041 addPolygonData xmin=%0.3f ymin=%.3f zmin=%.3f \n", xmin, ymin, zmin ));
    				} else {
    					if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
    						rc = JagParser::addPolygon3DData( pgon, otherAttr.valueData.s(), false, true );
    					} else {
    						rc = JagParser::addPolygon3DData( pgon, otherAttr.valueData.s(), false, false );
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
    				ppass.dbtab = dbtab;
    				ppass.colname = colname;
    				ppass.getm = getm; ppass.getn = getn; ppass.geti = geti;
    				ppass.col = i;
    
    				//prt(("s7830 pgon.size()=%d\n", pgon.size() ));
    				ppass.m = 0;
    				JagLineString linestr;
    				for ( int n=0; n < pgon.size(); ++n ) {
    					ppass.n = n;
    					linestr = pgon.linestr[n]; // convert
    					formatPointsInLineString( metrics, linestr, tablekvbuf, ppass, retpair, errmsg );
    				}
    
    				rc = 1;
    
    				++mlineIndex;
        			hasDoneAppend = true;
    			} else {
    				prt(("s2773 error polygon\n" ));
    			}
    		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
    			//prt(("s7651 i=%d colType=[%s]\n", i, colType.s() ));
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
    					rc = JagParser::addMultiPolygonData( pgvec, otherAttr.valueData.s(), false, false, true );
    				} else {
    					//prt(("s8260 addMultiPolygonData 2d\n" ));
    					rc = JagParser::addMultiPolygonData( pgvec, otherAttr.valueData.s(), false, false, false );
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
    				ppass.dbtab = dbtab;
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
        					formatPointsInLineString( metrics, linestr, tablekvbuf, ppass, retpair, errmsg );
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
    
    			//prt(("s5828 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    			//prt(("s5381 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.s(), otherAttr.linestr.point[1].y ));
    
    			if ( rc ) {
    				rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].x, errmsg, 
    			 		pointx1.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    
    				dbcolumn = dbtab + "." + pointy1;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].y, errmsg, 
    			 			pointy1.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if ( is3D ) {
    					dbcolumn = dbtab + "." + pointz1;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[0].z, errmsg, 
    			 				pointz1.s(), _schAttr[getpos].offset, 
    							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    					}
    				}
    
    				dbcolumn = dbtab + "." + pointx2;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].x, errmsg, 
    			 			pointx2.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointy2;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				//prt(("s5428 dbcolumn=%s rc=%d getpos=%d\n", dbcolumn.s(), rc, getpos ));
    				//prt(("s5581 pointy2=[%s] otherAttr.linestr.point[1].y=[%s]\n", pointy2.s(), otherAttr.linestr.point[1].y ));
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].y, errmsg, 
    			 			pointy2.s(), _schAttr[getpos].offset, 
    						_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if ( is3D ) {
    					dbcolumn = dbtab + "." + pointz2;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[1].z, errmsg, 
    			 				pointz2.s(), _schAttr[getpos].offset, 
    							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    					}
    				}
    
    				dbcolumn = dbtab + "." + pointx3;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].x, errmsg, 
    			 			pointx3.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				dbcolumn = dbtab + "." + pointy3;
    				rc = _tablemap->getValue(dbcolumn, getpos);
    				if ( rc ) {
    					rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].y, errmsg, 
    			 			pointy3.s(), _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    				}
    
    				if ( is3D ) {
    					dbcolumn = dbtab + "." + pointz3;
    					rc = _tablemap->getValue(dbcolumn, getpos);
    					// prt(("s5201 is3D getValue pointz3 rc=%d\n", rc ));
    					if ( rc ) {
    						rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.linestr.point[2].z, errmsg, 
    			 				pointz3.s(), _schAttr[getpos].offset, 
    							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    						// prt(("s5203 formatOneCol rc=%d\n", rc ));
    					}
    				}
    
    				formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.linestr.point[0].metrics, tablekvbuf );
    				//formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.linestr.point[1].metrics, tablekvbuf );
    				//formatMetricCols( tzdiff, srvtmdiff, dbtab, colname, metrics, otherAttr.linestr.point[2].metrics, tablekvbuf );
    			} 
    		} else {
    			//prt(("s5128 i=%d name=[%s] other.valData=[%s]\n", i, (*tableRecord.columnVector)[i].name.s(), otherAttr.valueData.s() ));
				prt(("s30358 colType is not empty colType=[%s]\n", colType.s() ));
    			if ( otherAttr.valueData.size() < 1 ) {
    				rc = 1;
					prt(("s49448 otherAttr.valueData.size is 0\n" ));
    			} else {
					prt(("s52014 formatOneCol col=%s getpos=%d offset=%d length=%d sig=%d\n", 
						 (*_tableRecord.columnVector)[i].name.s(), getpos, _schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig ));

    			    rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.valueData.s(), errmsg, 
    				 	(*_tableRecord.columnVector)[i].name.s(), _schAttr[getpos].offset, 
    					_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
					prt(("s42448 formatOneCol %s rc=%d\n", (*_tableRecord.columnVector)[i].name.s(), rc ));
    			}
    		}
		} else {
			// colType is empty
			//prt(("s22020 \n"));
    		if ( otherAttr.valueData.size() < 1 ) {
    			rc = 1;
				prt(("s464408 otherAttr.valueData.size is 0\n" ));
    		} else {
				prt(("s52016 formatOneCol col=%s getpos=%d offset=%d length=%d sig=%d\n", 
					(*_tableRecord.columnVector)[i].name.s(), getpos, _schAttr[getpos].offset, _schAttr[getpos].length,  _schAttr[getpos].sig ));
    		    rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, otherAttr.valueData.s(), errmsg, 
    			 	(*_tableRecord.columnVector)[i].name.s(), _schAttr[getpos].offset, 
    				_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
    		}
		}


		if ( !rc ) {
			free( tablekvbuf );
			prt(("s29023 return 0 here\n" ));
			return 0;
		}

		// debug only
		/***
		prt(("\n"));
		prt(("s2033 i=%d getpos dbcolumn=[%s] tablekvbuf: \n", i, dbcolumn.s() ));
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
	/***
	prt(("s444448 _defvallist.size=%d\n",  _defvallist.size() ));
	for ( int i = 0; i < _defvallist.size(); ++i ) {
		if ( _defvallist[i] < parseParam->otherVec.size() ) {
			prt(("s4555508 i=%d defvaliit=%d < othervec=%d continue\n", i, _defvallist[i], parseParam->otherVec.size() ));
			continue;
		}
		prt(("s3028371 i=%d _defvallist[i]]=[%d]\n", i, _defvallist[i] ));

		rc = formatOneCol( tzdiff, srvtmdiff, tablekvbuf, schAttr[_defvallist[i]].defValue.s(), errmsg, 
			 (*tableRecord.columnVector)[_defvallist[i]].name.s(), schAttr[_defvallist[i]].offset, 
			schAttr[_defvallist[i]].length, schAttr[_defvallist[i]].sig, schAttr[_defvallist[i]].type );

		if ( !rc ) {
			free( tablekvbuf );
			errmsg = "E3039 formatOneCol default error";
			return 0;
		}			
	}
	***/

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
		//prt(("s30066 parsed pair:\n")); // 
		//retpair[retpair.size() - 1].print();
		//retpair[retpair.size() - 1].printkv();
	}

	free( tablekvbuf );
	prt(("s42831 parsePair return 1\n"));
	return 1;
}

// insert one record 
// int JagTable::insert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
// return 1: OK   0: error
int JagTable::insert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	JagVector<JagDBPair> pairVec;
	int rc = parsePair( req.session->timediff, parseParam, pairVec, errmsg );
	prt(("s14920 rc=%d\n", rc ));
	// pair.print();
	if ( rc ) {
		for ( int i=0; i < pairVec.length(); ++i ) {
			//rc = insertPair( pair[i], 0, true );
			rc = insertPair( pairVec[i], 0, false );
			prt(("s3481 insertPair rc=%d\n", rc ));
			if ( !rc ) {
				errmsg = Jstr("E2108 InsertPair error key: ") + pairVec[i].key.s();
			} else {
				prt(("s33308 insertPair OK rc=%d\n", rc ));
			}
		}
	} else {
		// errmsg is from parsePair 
		prt(("s222201 parsePair error [%s] rc=%d\n", errmsg.s(), rc ));
	}

	return rc;
}

// mode 0: insert; 
// mode 1: cinsert( check insert ); 
// mode 2: dinsert ( delete insert );
// return: 0 for error
int JagTable::insertPair( JagDBPair &pair, int mode, bool doIndexLock ) 
{
	prt(("s5130 insertPair pair.key=[%s]\n", pair.key.s() ));
	prt(("s5130 insertPair pair.value: " ));
	//pair.value.print();  // 
	prt(("\n"));

	int rc;
	JagDBPair retpair;
	if ( 0 == mode ) {
		prt(("s333058 JagTable::insertPair mode=%d _replicateType=%d pair=[%s][%s]\n", mode, _replicateType, pair.key.s(), pair.value.s() ));
		rc = _darrFamily->insert( pair, true, retpair );
		// check if table has indexs, if yes, insert to self server index
		prt(("s53974 _darrFamily->insert rc=%d\n", rc ));
		if ( rc ) {
			formatIndexCmd( pair, mode, doIndexLock );
		}
		prt(("s53974 formatIndexCmd done\n" ));
	} else if ( 1 == mode ) {
		rc = _darrFamily->exist( pair );
	} else if ( 2 == mode ) {
		rc = _darrFamily->remove( pair );
		// if actually removed, also remove index data if needed
		if ( rc ) {
			formatIndexCmd( pair, mode, doIndexLock );
			char *buf = (char*)jagmalloc(KEYVALLEN+1);
			memcpy( buf, pair.key.s(), KEYLEN );
			memcpy( buf+KEYLEN, pair.value.s(), VALLEN );
			removeColFiles( buf );
			free( buf );
		}
	}

	prt(("s30992 insertPair done rc=%d\n", rc ));
	return rc;
}

// single insert, used by inserted data with file transfer
// int JagTable::finsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
int JagTable::finsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	prt(("s602221 finsert ...\n"));
	JagVector<JagDBPair> pair;
	int rc = parsePair( req.session->timediff, parseParam, pair, errmsg );
	if ( rc ) {
		for ( int k=0; k < pair.size(); ++k ) {
    		rc = insertPair( pair[k], 0, true );
    		if ( !rc ) {
    			errmsg = Jstr("E4003 Insert error key: ") + pair[k].key.s();
    		} else {
    			rc = 0;
    			Jstr hdir = fileHashDir( pair[k].key );

				if ( req.session->sock > 0 ) {
    				for ( int i = 0; i < _numCols; ++i ) {
    					if ( _schAttr[i].isFILE ) {
							Jstr fpath = _darrFamily->_sfilepath + "/" + hdir;
    						JagFileMgr::makedirPath( fpath );
							prt(("s52004 oneFileReceiver [%s]\n", fpath.s() ));
    						rc = oneFileReceiver( req.session->sock, fpath );
    					}
    				}
				}
    		}
		}
	} else {
		errmsg = Jstr("E4123 Error insert pair");
	}
	
	return rc;
}

// cinsert : check if this pair is exist or not
// int JagTable::cinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
int JagTable::cinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( req.session->timediff, parseParam, pair, errmsg );
	if ( rc ) {
		for ( int i=0; i < pair.size(); ++i ) {
			rc = insertPair( pair[i], 1, true );
		}
	} else {
		errmsg += Jstr(" E3043 Error cinsert pair");
	}
	
	return rc;
}

// dinsert: delete if this pair is exist
// int JagTable::dinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct )
int JagTable::dinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	JagVector<JagDBPair> pair;
	int rc = parsePair( parseParam->timediff, parseParam, pair, errmsg );
	// prt(("pair.key=[%s] pair.value=[%s]\n", pair.key.s(), pair.s()));
	if ( rc ) {
		for ( int i=0; i < pair.size(); ++i ) {
			rc = insertPair( pair[i], 2, true );
		}
	} else {
		errmsg += Jstr("E0221 Error dinsert pair");
	}
	
	return rc;
}

// method to form insert/upsert into index cmd for each table insert/upsert cmd
int JagTable::formatIndexCmd( JagDBPair &pair, int mode, bool doIndexLock  )
{
	if ( _indexlist.size() < 1 ) return 0;
	prt(("s500132 formatIndexCmd ...\n"));

	char *tablebuf = (char*)jagmalloc(KEYVALLEN+1);
	memcpy( tablebuf, pair.key.s(), KEYLEN );
	memcpy( tablebuf+KEYLEN, pair.value.s(), VALLEN );
	tablebuf[KEYVALLEN] = '\0';
	dbNaturalFormatExchange( tablebuf, _numKeys, _schAttr, 0, 0, " " ); // db format -> natural format
	
	jagint cnt = 0;
	AbaxBuffer bfr;
	JagIndex *pindex = NULL;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		//prt(("s40124 writeLockIndex [%s] ... \n", _indexlist[i].s() ));
		if ( doIndexLock ) {
			pindex = _objectLock->writeLockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i],
										      _tableschema, _indexschema, _replicateType, 1 );
		} else {
			pindex = _objectLock->getIndex(  _dbname, _indexlist[i], _replicateType );
		}

		if ( pindex ) {

			pindex->formatIndexCmdFromTable( tablebuf, mode );

			++ cnt;
			if ( doIndexLock ) {
				_objectLock->writeUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
			}
			//prt(("s40124 writeLockIndex [%s] done\n", _indexlist[i].s() ));
		}
	}
	free( tablebuf );
	prt(("s500132 formatIndexCmd done\n"));
	return cnt;
}

// method to do create index concurrently
// pindex was writelock protected
int JagTable::formatCreateIndex( JagIndex *pindex ) 
{
	ParallelCmdPass *psp = newObject<ParallelCmdPass>();
	psp->ptab = this;
	psp->pindex = pindex;
	parallelCreateIndexStatic ((void*)psp );
	return 1;
}

void *JagTable::parallelCreateIndexStatic( void * ptr )
{	
	ParallelCmdPass *pass = (ParallelCmdPass*)ptr;

	int KLEN = pass->ptab->_darrFamily->_KLEN;
	int KVLEN = pass->ptab->_darrFamily->_KVLEN;

	char *tablebuf = (char*)jagmalloc(KVLEN+1);
	memset( tablebuf, 0, KVLEN+1 );

	char minbuf[KLEN+1];
	char maxbuf[KLEN+1];
	memset( minbuf, 0,   KLEN+1 );
	memset( maxbuf, 255, KLEN+1 );

	JagMergeReader *ntu = NULL;
	pass->ptab->_darrFamily->setFamilyRead( ntu, minbuf, maxbuf );
	if ( ntu ) {
		while ( ntu->getNext( tablebuf ) ) {
			dbNaturalFormatExchange( tablebuf, pass->ptab->_numKeys, pass->ptab->_schAttr, 0, 0, " " ); 
			pass->pindex->formatIndexCmdFromTable( tablebuf, 0 );
		}
	}

	free( tablebuf );
	delete pass;
}


// return 0 for error; > 0 OK
jagint JagTable::update( const JagRequest &req, const JagParseParam *parseParam, bool upsert, Jstr &errmsg )
{
	// chain not able to update
	if ( JAG_CHAINTABLE_TYPE == _objectType ) {
		errmsg = "E23072 Chain cannot be updated";
		return 0;
	}

	prt(("s344082 JagTable::update parseParam->updSetVec.print():\n" ));
	//parseParam->updSetVec.print();

	prt(("s344083 JagTable::update parseParam->otherVec.print():\n" ));
	//parseParam->otherVec.print();


	int 	numUpdateCols = parseParam->updSetVec.size();
	int 	rc, collen, siglen, setindexnum = _indexlist.size(); 
	int 	constMode = 0, typeMode = 0, tabnum = 0, treelength = 0;
	Jstr 	treetype;
	bool 	uniqueAndHasValueCol = 0, setKey;
	jagint 	cnt = 0, scanned = 0;
	const char *buffers[1];
	jagint 	setposlist[numUpdateCols];
	int 	getpos = 0;
	int     numIndexes = _indexlist.size();
	JagIndex *lpindex[ numIndexes ];
	AbaxBuffer bfr;
	Jstr 	dbcolumn;

	ExprElementNode *updroot;
	const JagHashStrInt *maps[1];
	const JagSchemaAttribute *attrs[1];	
	maps[0] = _tablemap;
	attrs[0] = _schAttr;
	
	JagFixString strres, treestr;
	bool needInit = true;
	bool singleInsert = false;
	
	// check updSetVec validation
	setKey = 0;
	for ( int i = 0; i < numUpdateCols; ++i ) {
		dbcolumn = _dbtable + "." + parseParam->updSetVec[i].colName;
		if ( isFileColumn( parseParam->updSetVec[i].colName ) ) {
			errmsg = Jstr("E10283 column ") + parseParam->updSetVec[i].colName + " is file type";
			return -1;
		}

		if ( _tablemap->getValue(dbcolumn, getpos) ) {
			setposlist[i] = getpos;
			if ( getpos < _numKeys ) { 
				setKey = 1;
				errmsg = Jstr("E03875 Key column cannot be updated");
				return -1;
			}

			bool isAggregate = false;
			updroot = parseParam->updSetVec[i].tree->getRoot();
			rc = updroot->setFuncAttribute( maps, attrs, constMode, typeMode, isAggregate, treetype, collen, siglen );
			if ( 0 == rc || isAggregate ) {
				errmsg = Jstr("E0383 wrong update type. Char column must use single quotes.");
				return -1;
			}
		} else {
			errmsg = Jstr("E0384 column ") + dbcolumn + " not found";
			return -1;
		}
	}
	
	// build init index list
	/**
	if ( setKey ) {
		for ( int i = 0; i < _indexlist.size(); ++i ) {
			lpindex[i] = _objectLock->readLockIndex( JAG_UPDATE_OP, _dbname, _tableName, 
															   _indexlist[i], _replicateType, 1 );
		}
	} else {
		setindexnum = 0;
		for ( int i = 0; i < _indexlist.size(); ++i ) {
			lpindex[i] = NULL;
			lpindex[setindexnum] = _objectLock->readLockIndex( JAG_UPDATE_OP, _dbname, _tableName, 
																	     _indexlist[i], _replicateType, 1 );
			for ( int i = 0; i < numUpdateCols; ++i ) {
				if ( lpindex[setindexnum] ) {
					if ( lpindex[setindexnum]->needUpdate( parseParam->updSetVec[i].colName ) ) {
						++setindexnum; break;
					} else {
						_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, 
																lpindex[setindexnum]->getIndexName(), _replicateType, 1 );
					}
				}
			}
		}
	}
	**/

	/***
	setindexnum = 0;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		lpindex[i] = NULL;
		lpindex[setindexnum] = _objectLock->readLockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
		for ( int i = 0; i < numUpdateCols; ++i ) {
			if ( lpindex[setindexnum] ) {
				if ( lpindex[setindexnum]->needUpdate( parseParam->updSetVec[i].colName ) ) {
					++setindexnum; 
					break;
				} else {
					_objectLock->readUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, lpindex[setindexnum]->getIndexName(), _replicateType, 1 );
				}
			}
		}
	}
	***/

	setindexnum = 0;
	for ( int i = 0; i < numIndexes; ++i ) {
		lpindex[i] = _objectLock->writeLockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _tableschema, _indexschema, _replicateType, 1 );
		if ( lpindex[i] ) {
			bool needUpdate = false;
			for ( int j = 0; j < numUpdateCols; ++j ) {
				if ( lpindex[i]->needUpdate( parseParam->updSetVec[j].colName ) ) {
					needUpdate = true;
				}
			}
			if ( ! needUpdate ) {
				_objectLock->writeUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
				lpindex[i] = NULL;
			} else {
				++ setindexnum;
			}
		} 
	}

	// if setindexnum == 0: then all index locks are freeed, and lpindex[i] set to NULL

	// parse tree		
	int keylen[1];
	int numKeys[1];	
	JagMinMax minmax[1];	
	keylen[0] = KEYLEN;
	numKeys[0] = _numKeys;
	minmax[0].setbuflen( keylen[0] );

	/***
	char 	*tableoldbuf = (char*)jagmalloc(KEYVALLEN+1);
	char 	*tablenewbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( tableoldbuf, 0, KEYVALLEN+1 );
	memset( tablenewbuf, 0, KEYVALLEN+1 );
	buffers[0] = tableoldbuf;
	***/

	ExprElementNode *root = parseParam->whereVec[0].tree->getRoot();
	rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, treestr, typeMode, tabnum );
	if ( rc < 0 ) {
		//if ( tableoldbuf ) free ( tableoldbuf );
		//if ( tablenewbuf ) free ( tablenewbuf );
		for ( int i = 0; i < numIndexes; ++i ) {
			if ( ! lpindex[i] ) continue;
			_objectLock->writeUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
		}
		errmsg = Jstr("E10385 Invalid where range found");
		return -1;
	}

	if ( 0 == rc ) {
		memset( minmax[0].minbuf, 0, keylen[0]+1 );
		memset( minmax[0].maxbuf, 255, keylen[0] );
		(minmax[0].maxbuf)[keylen[0]] = '\0';
	} 

	char 	*tableoldbuf = (char*)jagmalloc(KEYVALLEN+1);
	char 	*tablenewbuf = (char*)jagmalloc(KEYVALLEN+1);
	memset( tableoldbuf, 0, KEYVALLEN+1 );
	memset( tablenewbuf, 0, KEYVALLEN+1 );
	buffers[0] = tableoldbuf;

	if ( memcmp(minmax[0].minbuf, minmax[0].maxbuf, KEYLEN) == 0 ) {
		// single record update
		singleInsert = true;
		prt(("s244377 single record update\n"));
		JagFixString getkey ( minmax[0].minbuf, KEYLEN, KEYLEN );
		JagDBPair getpair( getkey );
		JagDBPair setpair;

		prt(("s441011 single update _darrFamily->setWithRange() \n"));
		//setpair.printkv();

		// update data record
		if ( _darrFamily->setWithRange( req, getpair, buffers, uniqueAndHasValueCol, root, parseParam, 
								 		_numKeys, _schAttr, setposlist, setpair ) ) {
			// out pair is db format
			prt(("s333400 single setWithRange true\n"));
			/**
			memcpy(tableoldbuf, getpair.key.s(), KEYLEN);
			memcpy(tableoldbuf+KEYLEN, getpair.value.s(), VALLEN);

			memcpy(tablenewbuf, setpair.key.s(), KEYLEN);
			memcpy(tablenewbuf+KEYLEN, setpair.value.s(), VALLEN);
			**/
			++cnt;
		} else {
			prt(("s333401 setWithRange false\n"));
		}
		
		if ( cnt && numIndexes > 0 && setindexnum ) {

			memcpy(tableoldbuf, getpair.key.s(), KEYLEN);
			memcpy(tableoldbuf+KEYLEN, getpair.value.s(), VALLEN);
			memcpy(tablenewbuf, setpair.key.s(), KEYLEN);
			memcpy(tablenewbuf+KEYLEN, setpair.value.s(), VALLEN);

			dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			for ( int i = 0; i < numIndexes; ++i ) {
				if ( ! lpindex[i] ) { continue; }
				lpindex[i]->updateFromTable( tableoldbuf, tablenewbuf );
			}
		}

		// if ( cnt < 1 && upsert ) { }

	} else {
		// range update
		prt(("s244378 multiple record update\n"));

		JagMergeReader *ntu = NULL;
		_darrFamily->setFamilyRead( ntu, minmax[0].minbuf, minmax[0].maxbuf );

		if ( ntu ) {				
			while ( true ) {
				rc = ntu->getNext( tableoldbuf );
				if ( !rc ) { break; }
				++scanned;
				
				dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
				rc = root->checkFuncValid( ntu, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
				if ( rc == 1 ) {
					memcpy(tablenewbuf, tableoldbuf, KEYVALLEN);
					// JagDBPair getpair( tableoldbuf, KEYLEN, tableoldbuf+KEYLEN, VALLEN );
					for ( int i = 0; i < numUpdateCols; ++i ) {
						updroot = parseParam->updSetVec[i].tree->getRoot();
						needInit = true;
						if ( updroot->checkFuncValid( ntu, maps, attrs, buffers, strres, 
													  typeMode, treetype, treelength, needInit, 0, 0 ) == 1 ) {
							memset(tablenewbuf+_schAttr[setposlist[i]].offset, 0, _schAttr[setposlist[i]].length);
							rc = formatOneCol( req.session->timediff, _servobj->servtimediff, tablenewbuf, 
												strres.s(), errmsg, 
												parseParam->updSetVec[i].colName, _schAttr[setposlist[i]].offset, 
												_schAttr[setposlist[i]].length, _schAttr[setposlist[i]].sig, 
												_schAttr[setposlist[i]].type );
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

					prt(("s303390 range _darrFamily->set( setpair )  ...\n"));
					// setpair.printkv();

					if ( _darrFamily->set( setpair ) ) {
						++cnt;							
						if ( numIndexes  > 0 ) {
							dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
							dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
							for ( int i = 0; i < numIndexes; ++i ) {
								if ( ! lpindex[i] ) continue;
								lpindex[i]->updateFromTable( tableoldbuf, tablenewbuf );
							}
						}
					}
				}
			}
			delete ntu;
		}
	}

	if ( tableoldbuf ) free ( tableoldbuf );
	if ( tablenewbuf ) free ( tablenewbuf );

	for ( int i = 0; i < numIndexes; ++i ) {
		if ( ! lpindex[i] ) continue;
		_objectLock->writeUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
	}
	
	prt(("s50284 updated=%lld/scanned=%lld records\n", cnt, scanned ));
	return cnt;
}

// return 0 error; > 0 OK
jagint JagTable::remove( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )	
{
	// chain not able to remove
	if ( JAG_CHAINTABLE_TYPE == _objectType ) {
		errmsg = "Chain cannot be deleted";
		return 0;
	}

	int rc, retval, typeMode = 0, tabnum = 0, treelength = 0;
	bool uniqueAndHasValueCol = 0, needInit = true;
	jagint cnt = 0;
	const char *buffers[1];
	char *buf = (char*)jagmalloc(KEYVALLEN+1);
	Jstr treetype;
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
		//prt(("s222029 remove single record\n"));
		JagDBPair pair( minmax[0].minbuf, KEYLEN );
		rc = _darrFamily->get( pair );
		if ( rc ) {
			memcpy(buf, pair.key.s(), KEYLEN);
			memcpy(buf+KEYLEN, pair.value.s(), VALLEN);
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
		//prt(("s222039 remove range records ...\n"));
		JagMergeReader *ntr = NULL;
		_darrFamily->setFamilyRead( ntr, minmax[0].minbuf, minmax[0].maxbuf );

		if ( ntr ) {
			while ( true ) {
				rc = ntr->getNext(buf);
				if ( !rc ) { break; }
				
				dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
				rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
				if ( rc == 1 ) {
					dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
					JagDBPair pair( buf, KEYLEN );
					rc = _darrFamily->remove( pair );
					if ( rc ) {
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

// select count  from ...
jagint JagTable::getCount( const char *cmd, const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	if ( parseParam->hasWhere ) {
		JagDataAggregate *jda = NULL;
		jagint cnt = select( jda, cmd, req, parseParam, errmsg, false );
		if ( jda ) delete jda;
		return cnt;
	}
	else {
		return _darrFamily->getCount( );
	}
}

jagint JagTable::getElements( const char *cmd, const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg )
{
	if ( parseParam->hasWhere ) {
		JagDataAggregate *jda = NULL;
		jagint cnt = select( jda, cmd, req, parseParam, errmsg, false );
		if ( jda ) delete jda;
		return cnt;
	}
	else {
		return _darrFamily->getElements( ); // count from all diskarr
	}
}

// nowherecnt: false if  select count(*) from t123 where
// nowherecnt: true if  select ... from t123 where ... (there is no count in where)
jagint JagTable::select( JagDataAggregate *&jda, const char *cmd, const JagRequest &req, JagParseParam *parseParam, 
						  Jstr &errmsg, bool nowherecnt, bool isInsertSelect )
{
	// set up timeout for select starting timestamp
	prt(("s8773 select cmd=[%s]\n", cmd ));

	struct timeval now;
	gettimeofday( &now, NULL ); 
	jagint bsec = now.tv_sec;
	bool timeoutFlag = 0;
	Jstr treetype;
	int rc, typeMode = 0, tabnum = 0, treelength = 0;
	bool uniqueAndHasValueCol = 0, needInit = true;
	jagint nm = parseParam->limit;
	std::atomic<jagint> recordcnt;
	//prt(("s87734 _darrFamily->_darrlist.size()=[%d]\n", _darrFamily->_darrlist.size() ));
	//prt(("s87714 _darrFamily->memoryBufferSize().size()=[%d]\n", _darrFamily->memoryBufferSize() ));

	prt(("s222081 parseParam->hasLimit=%d parseParam->limit=%d nowherecnt=%d\n", parseParam->hasLimit, nm, nowherecnt ));

	recordcnt = 0;
	if ( parseParam->hasLimit && nm == 0 && nowherecnt ) {
		prt(("s20171727 here return 0 parseParam->hasLimit && nm == 0 && nowherecnt \n"));
		return 0;
	}

	if ( parseParam->exportType == JAG_EXPORT && _isExporting ) {
		prt(("s222277  parseParam->exportType == JAG_EXPORT && _isExporting return 0\n"));
		return 0;
	}

	if ( _darrFamily->_darrlist.size() < 1 && _darrFamily->memoryBufferSize() < 1 ) {
		// check memory part too
		prt(("s80910 _darrlist.size < 1 && _darrFamily->memoryBufferSize()<1 return 0 _replicateType=%d\n", _replicateType ));
		return 0;
	} 

	if ( parseParam->exportType == JAG_EXPORT ) _isExporting = 1;

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
	jagint finalsendlen = 0;
	jagint gbvsendlen = 0;
	JagSchemaRecord nrec;
	JagFixString treestr, strres;
	ExprElementNode *root = NULL;

	minmax[0].setbuflen( keylen[0] ); // KEYLEN
	//prt(("s2028 minmax[0].setbuflen( keylen=%d )\n", keylen[0] ));

	if ( nowherecnt ) {
		// NOT "select count(*) from ...."
		JagVector<SetHdrAttr> hspa;
		SetHdrAttr honespa;
		AbaxString getstr;
		_tableschema->getAttr( _dbtable, getstr );
		honespa.setattr( _numKeys, false, _dbtable, &_tableRecord, getstr.s() );
		hspa.append( honespa );
		prt(("s5640 nowherecnt rearrangeHdr ...\n" ));
		rc = rearrangeHdr( 1, maps, attrs, parseParam, hspa, newhdr, gbvheader, finalsendlen, gbvsendlen );
		if ( !rc ) {
			errmsg = "E0823 Error header for select";
			if ( parseParam->exportType == JAG_EXPORT && _isExporting ) _isExporting = 0;
			return -1;			
		}
		nrec.parseRecord( newhdr.s() );
		prt(("s0573 nowherecnt newhdr=[%s]\n",  newhdr.s() ));
		//nrec.print(); 
	}
	
	prt(("s92830 parseParam->hasWhere=%d\n", parseParam->hasWhere ));
	if ( parseParam->hasWhere ) {
		root = parseParam->whereVec[0].tree->getRoot();
		prt(("s334087 root->setWhereRange() ...\n"));
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, 
								  treestr, typeMode, tabnum );
		prt(("s83734 tab select setWhereRange done rc=%d\n", rc ));
		if ( 0 == rc ) {
			// not able to determine min max
			prt(("s20097 not able to determine min max\n"));
			memset( minmax[0].minbuf, 0, keylen[0]+1 );
			memset( minmax[0].maxbuf, 255, keylen[0] );
			(minmax[0].maxbuf)[keylen[0]] = '\0';
		} else if (  rc < 0 ) {
			if ( parseParam->exportType == JAG_EXPORT && _isExporting ) _isExporting = 0;
			errmsg = "E0825 Error where range";
			return -1;
		}

		#if 0
		prints(("s7430 dumpmem: minbuf \n" )); 
		dumpmem( minmax[0].minbuf, keylen[0] );
		prints(("s7431 dumpmem: maxbuf \n" ));
		dumpmem( minmax[0].maxbuf, keylen[0] );
		#endif
	}
	
	// finalbuf, hasColumn len or KEYVALLEN if !hasColumn
	// gbvbuf, if has group by
	char *finalbuf = (char*)jagmalloc(finalsendlen+1);
	memset(finalbuf, 0, finalsendlen+1);

	//char *gbvbuf = (char*)jagmalloc(gbvsendlen+1);
	//memset(gbvbuf, 0, gbvsendlen+1);
	char *gbvbuf = NULL;

	prt(("s1028 finalsendlen=%d gbvsendlen=%d\n", finalsendlen, gbvsendlen ));
	JagMemDiskSortArray *gmdarr = NULL;
	if ( gbvsendlen > 0 ) {
		gmdarr = newObject<JagMemDiskSortArray>();
		int sortmb = atoi((_cfg->getValue("GROUPBY_SORT_SIZE_MB", "1024")).s()); 
		gmdarr->init( sortmb, gbvheader.s(), "GroupByValue" );
		gmdarr->beginWrite();
	}
	
	// if insert into ... select syntax, create cpp client object to send insert cmd to corresponding server
	JaguarCPPClient *pcli = NULL;
	if ( JAG_INSERTSELECT_OP == parseParam->opcode ) {
		pcli = newObject<JaguarCPPClient>();
		Jstr host = "localhost", unixSocket = Jstr("/TOKEN=") + _servobj->_servToken;
		if ( _servobj->_listenIP.size() > 0 ) { host = _servobj->_listenIP; }
		if ( !pcli->connect( host.s(), _servobj->_port, "admin", "dummy", "test", unixSocket.s(), 0 ) ) {
			raydebug( stdout, JAG_LOG_LOW, "s4055 Connect (%s:%s) (%s:%d) error [%s], retry ...\n",
					  "admin", "jaguar", host.s(), _servobj->_port, pcli->error() );
			pcli->close();
			if ( pcli ) delete pcli;
			if ( gmdarr ) delete gmdarr;
			if ( finalbuf ) free ( finalbuf );
			if ( gbvbuf ) free ( gbvbuf );
			errmsg = "E0826 Error connection";
			return -1;
		}
	}
	
	// Point query, one record
	if ( memcmp(minmax[0].minbuf, minmax[0].maxbuf, KEYLEN) == 0 ) {
		JagDBPair pair( minmax[0].minbuf, KEYLEN );
		if ( _darrFamily->get( pair ) ) {
			// get point data from family 
			const char *buffers[1];
			char *buf = (char*)jagmalloc(KEYVALLEN+1);
			memset( buf, 0, KEYVALLEN+1 );	
			memcpy(buf, pair.key.s(), KEYLEN);
			memcpy(buf+KEYLEN, pair.value.s(), VALLEN);
			buffers[0] = buf;
			Jstr hdir = fileHashDir( pair.key );
			dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			if ( !uniqueAndHasValueCol ||
				( root && root->checkFuncValid( NULL, maps, attrs, buffers, strres, typeMode, treetype, 
											    treelength, needInit, 0, 0 ) == 1 ) ) {
				if ( parseParam->opcode == JAG_GETFILE_OP ) { 
					setGetFileAttributes( hdir, parseParam, buffers ); 
				}
				nonAggregateFinalbuf( NULL, maps, attrs, &req, buffers, parseParam, finalbuf, finalsendlen, jda, 
									  _dbtable, recordcnt, nowherecnt, NULL, true );
				if ( parseParam->opcode == JAG_GETFILE_OP && parseParam->getfileActualData ) {
					Jstr hdir = fileHashDir( pair.key );
					Jstr ddcol, inpath; int getpos; char fname[JAG_FILE_FIELD_LEN+1];
					for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
						ddcol = _dbtable + "." + parseParam->selColVec[i].getfileCol.s();
						if ( _tablemap->getValue(ddcol, getpos) ) {
							memcpy( fname, buffers[0]+_schAttr[getpos].offset, _schAttr[getpos].length );
							inpath = _darrFamily->_sfilepath + "/" + hdir + "/" + fname;
							prt(("s52118 oneFileSender %s ...\n", inpath.s() ));
							req.session->active = false;
							oneFileSender( req.session->sock, inpath );
							req.session->active = true;
						}
					}
				} else {
					if ( JAG_INSERTSELECT_OP == parseParam->opcode ) {
						Jstr iscmd;
						if ( formatInsertSelectCmdHeader( parseParam, iscmd ) ) {
							//prt(("s02938  formatInsertFromSelect ...\n"));
							formatInsertFromSelect( parseParam, attrs[0], finalbuf, buffers[0], finalsendlen, 
												    _numCols, pcli, iscmd );
						}
					} else {
						// raydebug( stdout, JAG_LOG_HIGH, "s5541 opcode=%d\n", parseParam->opcode  );
					}
				}
			} else {
				// raydebug( stdout, JAG_LOG_HIGH, "s0341 uniqueAndHasValueCol=%d\n", uniqueAndHasValueCol  );
				if ( parseParam->getfileActualData ) {
				}
			}

			free( buf );
		} else {
			// prt(("s4501 getpair error pair.key=[%s]\n", pair.key.s() ));
			if ( parseParam->getfileActualData ) {
			}
		}
	} else { // range query
		jagint  callCounts = -1, lastBytes = 0;
		if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
			if ( !jda ) jda = newObject<JagDataAggregate>();
			prt(("s222081 jda->setwrite  parseParam->exportType=%d JAG_EXPORT=%d\n", parseParam->exportType, JAG_EXPORT ));
			jda->setwrite( _dbtable, _dbtable, parseParam->exportType == JAG_EXPORT );  // to /export file or not
			jda->setMemoryLimit( _darrFamily->getElements()*KEYVALLEN*2 );
		}

		int numBatches = 1;
		JagParseParam *pparam[numBatches];
		JagParseAttribute jpa( _servobj, req.session->timediff, _servobj->servtimediff, req.session->dbname, _servobj->_cfg );

		if ( ! parseParam->hasGroup ) {
			// has no "group by"
			// check if has aggregate
			bool hAggregate = false;
			if ( parseParam->hasColumn ) {
				for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
					if ( parseParam->selColVec[i].isAggregate ) {
						hAggregate = true; break;
					}
				}
			}

			JagParser parser((void*)NULL);
			for ( jagint i = 0; i < numBatches; ++i ) {
				pparam[i] = new JagParseParam(&parser);
				parser.parseCommand( jpa, cmd, pparam[i], errmsg);
			    pparam[i]->_parent = parseParam;
			}

			jagint memlim = availableMemory( callCounts, lastBytes )/8/numBatches/1024/1024;
			if ( memlim <= 0 ) memlim = 1;

			ParallelCmdPass psp[numBatches];
			for ( jagint i = 0; i < numBatches; ++i ) {
				psp[i].cli = pcli;

				#if 1
				psp[i].ptab = this;
				psp[i].pindex = NULL;
				psp[i].pos = i;
				psp[i].sendlen = finalsendlen;
				psp[i].parseParam = pparam[i];
				psp[i].gmdarr = NULL;
				psp[i].req = (JagRequest*)&req;
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

				/***
				if ( lcpu ) {
					psp[i].spos = i; 
					psp[i].epos = i;
				} else {
					psp[i].spos = i*_servobj->_numCPUs;
					psp[i].epos = psp[i].spos + _servobj->_numCPUs-1;
					if ( i == numBatches-1 ) {
						psp[i].epos = _darrFamily->_darrlist.size()-1;
					}
				}
				***/
				psp[i].spos = 0;
				psp[i].epos = _darrFamily->_darrlist.size()-1;

				#else
				prt(("s6513 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%d minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, *psp[i].cnt, psp[i].nrec, 
					 	psp[i].minbuf, psp[i].maxbuf ));
				prt(("s6712 gbvsendlen=%d\n", gbvsendlen ));
				fillCmdParse( JAG_TABLE, this, i, gbvsendlen, pparam, 0, lgmdarr, req, jda, _dbtable,  
							 cnt, nm, nowherecnt, nrec, memlim, minmax, 
							 bsec, KEYVALLEN, _servobj, numBatches, _darrFamily, lcpu );
				prt(("s6514 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%d minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, *psp[i].cnt, psp[i].nrec, 
					 psp[i].minbuf, psp[i].maxbuf ));
				#endif


				if ( JAG_INSERTSELECT_OP == parseParam->opcode && ! parseParam->hasTimeout ) {
					pparam[i]->timeout = -1;
				}

				parallelSelectStatic( (void*)&psp[i] );  // no group by

			} // end of for i in numBatches

			for ( jagint i = 0; i < numBatches; ++i ) {
				if ( psp[i].timeoutFlag ) timeoutFlag = 1;
			}

			if ( hAggregate ) {
				aggregateFinalbuf( &req, newhdr, numBatches, pparam, finalbuf, finalsendlen, jda, _dbtable, 
								   recordcnt, nowherecnt, &nrec );
			}
		
			for ( jagint i = 0; i < numBatches; ++i ) {
				delete pparam[i];
			}	

		} else {
			// has "group by", no insert into ... select ... syntax allowed
			prt(("s20393 has groupby numBatches=%d ...\n", numBatches ));
			JagMemDiskSortArray *lgmdarr[numBatches];
			JagParser parser((void*)NULL);
			for ( jagint i = 0; i < numBatches; ++i ) {
				pparam[i] = new JagParseParam(&parser);
				prt(("s40038 parser.parseCommand\n"));
				parser.parseCommand( jpa, cmd, pparam[i], errmsg );
			    pparam[i]->_parent = parseParam;

				lgmdarr[i] = newObject<JagMemDiskSortArray>();
				lgmdarr[i]->init( 40, gbvheader.s(), "GroupByValue" );
				lgmdarr[i]->beginWrite();
			}

			jagint memlim = availableMemory( callCounts, lastBytes )/8/numBatches/1024/1024;
			if ( memlim <= 0 ) memlim = 1;
			
			ParallelCmdPass psp[numBatches];
			for ( int i = 0; i < numBatches; ++i ) {
				#if 1
				psp[i].ptab = this;
				psp[i].pindex = NULL;
				psp[i].pos = i;
				psp[i].sendlen = gbvsendlen;
				psp[i].parseParam = pparam[i];
				psp[i].gmdarr = lgmdarr[i];
				psp[i].req = (JagRequest*)&req;
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
				/***
				if ( lcpu ) {
					psp[i].spos = i; psp[i].epos = i;
				} else {
					psp[i].spos = i*_servobj->_numCPUs;
					psp[i].epos = psp[i].spos+_servobj->_numCPUs-1;
					if ( i == numthreads-1 ) psp[i].epos = _darrFamily->_darrlist.size()-1;
				}
				***/
				psp[i].spos = 0;
				psp[i].epos = _darrFamily->_darrlist.size()-1;

				#else
				prt(("s6711 gbvsendlen=%d\n", gbvsendlen ));
				fillCmdParse( JAG_TABLE, this, i, gbvsendlen, pparam, 1, lgmdarr, req, jda, _dbtable,  
								recordcnt, nm, nowherecnt, nrec, memlim, minmax, bsec, KEYVALLEN, 
								_servobj, numthreads, _darrFamily, lcpu );

				prt(("s6714 psp[%d]:  ptab=%0x parseParam=%0x gmdarr=%0x jda=%0x cnt=%0x minbuf=%0x maxbuf=%0x \n",
					 i,  psp[i].ptab, psp[i].parseParam, psp[i].gmdarr, psp[i].jda, psp[i].cnt, 
					 psp[i].nrec, psp[i].minbuf, psp[i].maxbuf ));
				#endif

				if ( JAG_INSERTSELECT_OP == parseParam->opcode && ! parseParam->hasTimeout ) {
					pparam[i]->timeout = -1;
				}

				 parallelSelectStatic( (void*)&psp[i] );  // has group by
			}

			gbvbuf = (char*)jagmalloc(gbvsendlen+1);
			memset(gbvbuf, 0, gbvsendlen+1);

			for ( jagint i = 0; i < numBatches; ++i ) {
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

			groupByFinalCalculation( gbvbuf, nowherecnt, finalsendlen, recordcnt, nm, _dbtable, 
									 parseParam, jda, gmdarr, &nrec );
		} 
		
		//delete selectPool;

		if ( jda ) {
			//prt(("s5003 jda->flushwrite() ...\n" ));
			jda->flushwrite();
			//prt(("s5003 jda->flushwrite() done ...\n" ));
		}
	}

	if ( timeoutFlag ) {
		Jstr timeoutStr = "E0283 Table select has timed out. Results have been truncated;";
		sendMessage( req, timeoutStr.s(), "ER" );
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

	//prt(("s611273 JagTable select recordcnt=%d\n", (int)recordcnt ));
	return (jagint)recordcnt;
}

// static select function
void *JagTable::parallelSelectStatic( void * ptr )
{	
	ParallelCmdPass *pass = (ParallelCmdPass*)ptr;
	ExprElementNode *root;
	int rc, collen, siglen, constMode = 0, typeMode = 0, treelength = 0, tabnum = 0;
	bool isAggregate, hasAggregate = false, hasFirst = false, needInit = true, uniqueAndHasValueCol;
	jagint offset = 0;
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
				 i, pass->parseParam->selColVec[i].name.s(), pass->parseParam->selColVec[i].asName.s() ));
		    #endif
		}
	}

	if ( pass->parseParam->hasWhere ) {
		prt(("s0377 pass->parseParam->hasWhere \n" ));
		root = pass->parseParam->whereVec[0].tree->getRoot();
		rc = root->setWhereRange( maps, attrs, keylen, numKeys, 1, uniqueAndHasValueCol, minmax, 
							      treestr, typeMode, tabnum );
		if (  rc < 0 ) {
			prt(("E3006 Error where range"));
			return NULL;
		}
	}
	
	prt(("s20487 before formatInsertSelectCmdHeader...\n"));

	Jstr iscmd;
	formatInsertSelectCmdHeader( pass->parseParam, iscmd );
	prt(("s20488 after formatInsertSelectCmdHeader...\n"));

	char *buf = (char*)jagmalloc(pass->kvlen+1); 
	memset(buf, 0, pass->kvlen+1);
	char *sendbuf = (char*)jagmalloc(pass->sendlen+1); 
	memset(sendbuf, 0, pass->sendlen+1);
	//prt(("s8333092 kvlen=%d sendlen=%d\n",  pass->kvlen, pass->sendlen ));

	const char *buffers[1]; 
	buffers[0] = buf;

	JagDiskArrayFamily *darrfam = NULL;
	if ( pass->ptab ) darrfam = pass->ptab->_darrFamily;
	else darrfam = pass->pindex->_darrFamily;
	
	if ( pass->parseParam->hasOrder && !pass->parseParam->orderVec[0].isAsc ) {
		// order by col1 desc  descending
		JagMergeBackReader *ntr = NULL;
		//prt(("s22039 setFamilyReadBackPartial\n"));
		darrfam->setFamilyReadBackPartial( ntr, pass->minbuf, pass->maxbuf, pass->spos, pass->epos, pass->memlimit );
	
		if ( ntr != NULL ) {
			prt(("s722200 ntr=%0x\n", ntr ));
			//ntr->initHeap();
			while ( 1 ) {
				if ( !pass->parseParam->hasExport && checkCmdTimeout( pass->starttime, pass->parseParam->timeout ) ) {
					pass->timeoutFlag = 1;
					break;
				}

				rc = ntr->getNext( buf );  ////// read a new row

				if ( pass->req->session->sessionBroken ) rc = false;
				if ( !rc ) { break; }
				dbNaturalFormatExchange( buf, numKeys[0], attrs[0], 0,0, " " ); // db format -> natural format
				if ( pass->parseParam->hasWhere ) {
					root = pass->parseParam->whereVec[0].tree->getRoot();
					rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
				} else {
					rc = 1;
				}

				if ( rc == 1 ) {
					// buf: if pass->parseParam.window.size() > 0 : converToWindow()
					if ( pass->parseParam->window.size() > 0 ) {
						Jstr period, pcolName;
						bool prc =  pass->parseParam->getWindowPeriod( pass->parseParam->window, period, pcolName );
						if ( prc ) {
							prt(("s552238 convertTimeToWindow period=[%s] pcolName=[%s]\n", period.s(), pcolName.s() ));
							pass->ptab->convertTimeToWindow( pass->req->session->timediff, period, buf, pcolName );
						}
					}

					if ( pass->gmdarr ) { // has group by
						rc = JagTable::buildDiskArrayForGroupBy( ntr, maps, attrs, pass->req, buffers, 
																 pass->parseParam, pass->gmdarr, sendbuf );
						if ( 0 == rc ) break;
					} else { // no group by
						if ( hasAggregate ) { // has aggregate functions
							JagTable::aggregateDataFormat( ntr, maps, attrs, pass->req, buffers, pass->parseParam, !hasFirst );
						} else { // non aggregate functions
							JagTable::nonAggregateFinalbuf( ntr, maps, attrs, pass->req, buffers, pass->parseParam, 
												            sendbuf, pass->sendlen, pass->jda, pass->writeName, *(pass->recordcnt), 
															pass->nowherecnt, pass->nrec, false );
							if ( JAG_INSERTSELECT_OP == pass->parseParam->opcode ) {
								//prt(("s202299 formatInsertFromSelect \n"));
								JagTable::formatInsertFromSelect( pass->parseParam, attrs[0], sendbuf, buffers[0], pass->sendlen, numCols[0], 
																  pass->cli, iscmd );
							}

							if ( pass->parseParam->hasLimit && *(pass->recordcnt) >= pass->actlimit ) {
								prt(("s200992 order  pass->parseParam->hasLimit pass->recordcnt=%d pass->actlimit=%d  break\n", 
										(int)*(pass->recordcnt), pass->actlimit ));
								break;
							}
						}
					}
					hasFirst = true;
				}	
			}
		}
		if ( ntr ) delete ntr;
	} else {
		// no order
		//prt(("s8302 no order range select pass->spos=%d  pass->epos=%d\n", pass->spos, pass->epos ));
		JagMergeReader *ntr = NULL;
        darrfam->setFamilyReadPartial( ntr, pass->minbuf, pass->maxbuf, pass->spos, pass->epos, pass->memlimit );
		//prt(("s8303 setFamilyReadPartial dn=one ntr=%0x\n", ntr ));

		if ( ntr ) {			
			//ntr->initHeap();
			prt(("s8305 initHeap done this->ntr=%0x\n", ntr));
			while ( 1 ) {
				if ( !pass->parseParam->hasExport && checkCmdTimeout( pass->starttime, pass->parseParam->timeout ) ) {
					prt(("s1929 timeoutFlag set to 1  pass->starttime=%ld pass->parseParam->timeout=%ld\n", 
						  pass->starttime, pass->parseParam->timeout ));
					pass->timeoutFlag = 1;
					break;
				}

				prt(("s81307 before getNext() \n"));
				rc = ntr->getNext( buf );  // read a new row
				prt(("s31366 ntr->getNext( buf ) buf=[%s] rc=%d\n", buf, rc ));

				if ( pass->req->session->sessionBroken ) rc = false;
				if ( !rc ) {
					if ( pass->parseParam->hasWhere ) {
						root = pass->parseParam->whereVec[0].tree->getRoot();
    					if ( root && root->_builder->_pparam->_rowHash ) {
    						prt(("s2042 root->_builder->_pparam->_rowHash=%0x\n", root->_builder->_pparam->_rowHash ));
    						Jstr str =  root->_builder->_pparam->_rowHash->getKVStrings("#");
							if ( pass->ptab ) {
								JAG_BLURT jaguar_mutex_lock ( &pass->ptab->_parseParamParentMutex ); JAG_OVER;
							} else {
								JAG_BLURT jaguar_mutex_lock ( &pass->pindex->_parseParamParentMutex ); JAG_OVER;
							}
    						if ( ! pass->parseParam->_parent->_lineFile ) {
    							pass->parseParam->_parent->_lineFile = new JagLineFile();
    						} 
    						//prt(("s2138 _lineFile->append(%s) _lineFile=%x\n", str.s(), pass->parseParam->parent->_lineFile ));
    						pass->parseParam->_parent->_lineFile->append( str );
							if ( pass->ptab ) {
								JAG_BLURT jaguar_mutex_unlock ( &pass->ptab->_parseParamParentMutex ); JAG_OVER;
							} else {
								JAG_BLURT jaguar_mutex_unlock ( &pass->pindex->_parseParamParentMutex ); JAG_OVER;
							}
    					} 
					}

					break; 
				}

				dbNaturalFormatExchange( buf, numKeys[0], attrs[0],0,0, " " ); // db format -> natural format
				if ( pass->parseParam->hasWhere ) {
					root = pass->parseParam->whereVec[0].tree->getRoot();
					/***
					prt(("s302933 after tree->getRoot root=%0x\n", root ));
					root->print();
					prt(("\n"));
					***/
					rc = root->checkFuncValid( ntr, maps, attrs, buffers, strres, typeMode, treetype, treelength, needInit, 0, 0 );
					prt(("s2021 checkFuncValid rc=%d root=%0x strres=[%s]\n", rc, root, strres.s() ));

					if ( root->_builder->_pparam->_rowHash ) {
						prt(("s2022 root->_builder->_pparam->_rowHash=%0x\n", root->_builder->_pparam->_rowHash ));
						if ( rc ) {
							Jstr str =  root->_builder->_pparam->_rowHash->getKVStrings("#");
							if ( pass->ptab ) {
								JAG_BLURT jaguar_mutex_lock ( &pass->ptab->_parseParamParentMutex ); JAG_OVER;
							} else {
								JAG_BLURT jaguar_mutex_lock ( &pass->pindex->_parseParamParentMutex ); JAG_OVER;
							}
							if ( ! pass->parseParam->_parent->_lineFile ) {
								pass->parseParam->_parent->_lineFile = newObject<JagLineFile>();
							} 
							//prt(("s2038 _lineFile->append(%s) _lineFile=%x\n", str.s(), pass->parseParam->parent->_lineFile ));
							pass->parseParam->_parent->_lineFile->append( str );
							if ( pass->ptab ) {
								JAG_BLURT jaguar_mutex_unlock ( &pass->ptab->_parseParamParentMutex ); JAG_OVER;
							} else {
								JAG_BLURT jaguar_mutex_unlock ( &pass->pindex->_parseParamParentMutex ); JAG_OVER;
							}
						}
					} else {
						prt(("s2023 root->_builder->_pparam->_rowHash=NULL root=%0x\n", root ));
					}
				} else {
					// no where in select
					rc = 1;
				}

				if ( rc == 1 ) {
					// buf: if pass->parseParam.window.size() > 0 : converToWindow()
					if ( pass->parseParam->window.size() > 0 ) {
						Jstr period, pcolName;
						bool prc =  pass->parseParam->getWindowPeriod( pass->parseParam->window, period, pcolName );
						if ( prc ) {
							prt(("s552238 convertTimeToWindow period=[%s] pcolName=[%s]\n", period.s(), pcolName.s() ));
							pass->ptab->convertTimeToWindow( pass->req->session->timediff, period, buf, pcolName );
						}
					}

					if ( pass->gmdarr ) { // has group by
						prt(("s2112034 gmdarr not null, has group by\n" ));
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
								//prt(("s20229 formatInsertFromSelect ...\n"));
								JagTable::formatInsertFromSelect( pass->parseParam, attrs[0], sendbuf, buffers[0], 
																  pass->sendlen, numCols[0], pass->cli, iscmd );
							}

							if ( pass->parseParam->hasLimit && *(pass->recordcnt) >= pass->actlimit ) {
								break;
							}
						}
					}
					hasFirst = true;
				}	
			}
		}

		if ( ntr ) {
			prt(("s08484 delete ntr\n"));
			delete ntr;
			ntr = NULL;
		}
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
										const JagRequest *req, const char *buffers[], JagParseParam *parseParam, 
										JagMemDiskSortArray *gmdarr, char *gbvbuf )
{
	Jstr treetype;
	jagint totoff = 0;
	ExprElementNode *root;
	Jstr errmsg;
	bool init = 1;
	int rc = 0, treelength = 0, typeMode = 0, treesig;
	prt(("s40013 buildDiskArrayForGroupBy selColVec.size=%d  ...\n", parseParam->selColVec.size() ));

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
		formatOneCol( req->session->timediff, req->session->servobj->servtimediff, gbvbuf, 
					  parseParam->selColVec[i].strResult.s(), errmsg, "GAR", 
					  totoff, treelength, treesig, treetype );
		totoff += treelength;
		prt(("i=%d gbvbuf=[%s] len=%d sig=%d typ=%s\n", i, gbvbuf, treelength, treesig, treetype.s() ));
	}

	// then, insert data to groupby value diskarray
	if ( gbvbuf[0] != '\0' ) {	
		prt(("s22018 pair ... groupByUpdat.. keylen=%d vallen=%d\n", gmdarr->_keylen, gmdarr->_vallen ));
		JagDBPair pair(gbvbuf, gmdarr->_keylen, gbvbuf+gmdarr->_keylen, gmdarr->_vallen, true );
		rc = gmdarr->groupByUpdate( pair );
		rc = 1;
	} else {
		prt(("s22201 gbvbuf[0] is empty\n"));
		rc = 1;
	}
	return rc;
}

// main thread calling
// input buffers are natural data
void JagTable::groupByFinalCalculation( char *gbvbuf, bool nowherecnt, jagint finalsendlen, std::atomic<jagint> &cnt, jagint actlimit,
										const Jstr &writeName, JagParseParam *parseParam, 
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
				if ( jda ) doWriteIt( jda, parseParam, writeName, gbvbuf, finalsendlen, nrec ); 
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
									const JagRequest *req, const char *buffers[], 
									JagParseParam *parseParam, char *finalbuf, jagint finalsendlen,
									JagDataAggregate *jda, const Jstr &writeName, std::atomic<jagint> &cnt, 
									bool nowherecnt, const JagSchemaRecord *nrec, bool oneLine )
{
	prt(("s4817 nonAggregateFinalbuf parseParam->hasColumn=%d\n", parseParam->hasColumn ));
	if ( parseParam->hasColumn ) {
		memset(finalbuf, 0, finalsendlen);
		Jstr treetype;
		int rc, typeMode = 0, treelength = 0, treesig = 0;
		bool init = 1;
		jagint offset;
		ExprElementNode *root;
		Jstr errmsg;
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			init = 1;
			root = parseParam->selColVec[i].tree->getRoot();
			rc = root->checkFuncValid( ntr, maps, attrs, buffers, parseParam->selColVec[i].strResult, typeMode, 
									   treetype, treelength, init, 1, 1 );
			//prt(("s7263 strResult=[%s]\n", parseParam->selColVec[i].strResult.s() ));
			//prt(("s2082 in JagTable::nonAggregateFinalbuf() checkFuncValid rc=%d\n", rc ));
			if ( rc != 1 ) { return; }
			offset = parseParam->selColVec[i].offset;
			treelength = parseParam->selColVec[i].length;
			treesig = parseParam->selColVec[i].sig;
			treetype = parseParam->selColVec[i].type;
			formatOneCol( req->session->timediff, req->session->servobj->servtimediff, finalbuf,
						  parseParam->selColVec[i].strResult.s(), errmsg, "GAR", offset, 
						  treelength, treesig, treetype );
		    //prt(("s3981 formatOneCol done\n" ));
		}
	}
	
	if ( JAG_GETFILE_OP == parseParam->opcode ) {
		// if getfile, arrange point query to be size of file/modify time of file/md5sum of file/file path ( for actural data )
		Jstr ddcol, inpath, instr, outstr; 
		//char fname[JAG_FILE_FIELD_LEN+1]; struct stat sbuf;
		//raydebug( stdout, JAG_LOG_HIGH, "s0391 JAG_GETFILE_OP selColVec.size()=%d\n", parseParam->selColVec.size() );
		for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
			snprintf(finalbuf+parseParam->selColVec[i].offset, parseParam->selColVec[i].length+1, "%s", 
					 parseParam->selColVec[i].strResult.s());
		}
	}
	
	if ( oneLine ) { // point query
		if ( !nowherecnt ) cnt = -111; // select count(*) with where, no actual data sentry
		else { // one line data, direct send to client
			if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
				if ( parseParam->hasColumn || JAG_GETFILE_OP == parseParam->opcode ) { 
				    // has select coulumn or getfile command, use finalbuf
					sendMessageLength( *req, finalbuf, finalsendlen, "X1" );
				} else { // select *, use original buf, buffer[0]
					sendMessageLength( *req, buffers[0], finalsendlen, "X1" );
				}
				cnt = -111;
			}
		}
	} else { // range, non aggregate query
		if ( !nowherecnt ) ++cnt; // select count(*) with where, no actual data sentry
		else { // range data, write to data aggregate file
			if ( JAG_INSERTSELECT_OP != parseParam->opcode ) {
				if ( parseParam->hasColumn ) { // has select coulumn, use finalbuf
					if ( *finalbuf != '\0' ) {
						if ( jda ) doWriteIt( jda, parseParam, writeName, finalbuf, finalsendlen, nrec ); 
						++cnt;
					}
				} else { // select *, use original buf, buffer[0], include /export
					if ( *buffers[0] != '\0' ) {
						if ( jda ) {
							prt(("s2238 s999 doWriteIt ...finalsendlen=%d cnt=%d\n", finalsendlen, int(cnt) ));
							doWriteIt( jda, parseParam, writeName, buffers[0], finalsendlen, nrec ); 
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
									const JagRequest *req, const char *buffers[], JagParseParam *parseParam, bool init )
{
	ExprElementNode *root;
	int typeMode = 0, treelength = 0;
	Jstr treetype;
	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		root = parseParam->selColVec[i].tree->getRoot();
		if ( root->checkFuncValid( ntr, maps, attrs, buffers, parseParam->selColVec[i].strResult, typeMode, 
								   treetype, treelength, init, 1, 1 ) != 1 ) {
			prt(("ERROR i=%d [%s]\n", i, parseParam->selColVec[i].asName.s()));
			return;
		}
	}
}

// finish all data, merge all aggregation results to form one final result to send
// main thread calling
// input buffers are natural data
void JagTable::aggregateFinalbuf( const JagRequest *req, const Jstr &sendhdr, jagint len, JagParseParam *parseParam[], 
								  char *finalbuf, jagint finalsendlen, JagDataAggregate *jda, const Jstr &writeName, 
								  std::atomic<jagint> &cnt, bool nowherecnt, const JagSchemaRecord *nrec )
{
	JagSchemaRecord aggrec;
	//prt(("c2036 aggregateFinalbuf sendhdr=[%s]\n", sendhdr.s() ));
	aggrec.parseRecord( sendhdr.s() );
	char *aggbuf = (char*)jagmalloc(finalsendlen+1);
	memset(aggbuf, 0, finalsendlen+1);
	JagVector<JagFixString> oneSetData;
	JagVector<int> selectPartsOpcode;
	
	Jstr type;
	jagint offset, length, sig;
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
			formatOneCol( req->session->timediff, req->session->servobj->servtimediff, finalbuf,
				parseParam[m]->selColVec[i].strResult.s(), errmsg, "GAR", 
				offset, length, sig, type );
			if ( 0 == m ) selectPartsOpcode.append( root->getBinaryOp() );
		}
		oneSetData.append( JagFixString(finalbuf, finalsendlen, finalsendlen) );		
	}

	JaguarCPPClient::doAggregateCalculation( aggrec, selectPartsOpcode, oneSetData, aggbuf, finalsendlen, 0 );

	//prt(("s2879 nowherecnt=%d aggbuf=[%s] jda=%0x\n", nowherecnt, aggbuf, jda ));
	if ( nowherecnt ) { // send actural data
		if ( *aggbuf != '\0' ) {
			if ( jda ) {
				doWriteIt( jda, parseParam[0], writeName, aggbuf, finalsendlen, nrec );
			}
			++cnt;
			//prt(("s2287 cnt incremented to %d\n", (int)cnt ));
		}
	}

	free( aggbuf );
}

// drop indexes, removed files of a table
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
			pindex = _objectLock->writeLockIndex( JAG_DROPINDEX_OP, _dbname, _tableName, _indexlist[_indexlist.size()-1],
											      _tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[_indexlist.size()-1];
				if ( !isTruncate ) {
					_indexschema->remove( dbtabIndex );
				}
				if ( pindex ) delete pindex; pindex = NULL;
				_objectLock->writeUnlockIndex( JAG_DROPINDEX_OP, _dbname, _tableName, _indexlist[_indexlist.size()-1],
												_replicateType, 1 );
			}
			_indexlist.removepos( _indexlist.size()-1 );				
		}
	}	

	// prt(("s7373 _darrFamily->_tablepath=[%s]\n", _darrFamily->_tablepath.s() ));

	_darrFamily->drop();
	delete _darrFamily;
	_darrFamily = NULL;
	// rmdir
	Jstr rdbdatahome = _cfg->getJDBDataHOME( _replicateType ), dbpath = rdbdatahome + "/" + _dbname;
	Jstr dbtabpath = dbpath + "/" + _tableName;
	JagFileMgr::rmdir( dbtabpath, true );
	prt(("s4222939 rmdir(%s)\n", dbtabpath.s() ));

	jagmalloc_trim(0);

	return idxNames;
}

void JagTable::dropFromIndexList( const Jstr &indexName )
{
	for ( int k = 0; k < _indexlist.size(); ++k ) {
		const char *idxName = strrchr(_indexlist[k].s(), '.');
		if ( !idxName ) {
			continue;
		}
		++idxName;

		if ( strcmp(indexName.s(), idxName) == 0 ) {
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

	//bool rc;
	AbaxBuffer bfr;
	JagIndex *pindex = NULL;

	int cnt = 0;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		pindex = _objectLock->writeLockIndex( JAG_DELETE_OP, _dbname, _tableName, _indexlist[i], _tableschema, _indexschema, _replicateType, 1 );
		if ( pindex ) {
			pindex->removeFromTable( buf );
			++ cnt;
			_objectLock->writeUnlockIndex( JAG_DELETE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
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
	if ( _indexlist.size() > 0 ) {
		Jstr dbtab  = _dbname + "." + _tableName;
		Jstr dbtabIndex;
		for ( int k = 0; k < _indexlist.size(); ++k ) {
			pindex = _objectLock->writeLockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
												  _tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[k];
				_indexschema->addOrRenameColumn( dbtabIndex, parseParam );
				pindex->refreshSchema();
				_objectLock->writeUnlockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k], _replicateType, 1 );
			}
		}
	}

	return 1;	
}

int JagTable::setIndexColumn( const JagParseParam *parseParam, Jstr &errmsg )
{
	JagIndex *pindex = NULL;
	AbaxBuffer bfr;
	//bool rc;
	if ( _indexlist.size() > 0 ) {
		Jstr dbtab  = _dbname + "." + _tableName;
		Jstr dbtabIndex;
		for ( int k = 0; k < _indexlist.size(); ++k ) {
			pindex = _objectLock->writeLockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k],
												   _tableschema, _indexschema, _replicateType, 1 );
			if ( pindex ) {
				pindex->drop();
				dbtabIndex = dbtab + "." + _indexlist[k];
				_indexschema->setColumn( dbtabIndex, parseParam );
				pindex->refreshSchema();
				_objectLock->writeUnlockIndex( JAG_ALTER_OP, _dbname, _tableName, _indexlist[k], _replicateType, 1 );
			}
		}
	}

	return 1;	
}

int JagTable::refreshSchema()
{
	prt(("s222286 JagTable::refreshSchema()...\n"));

	Jstr dbtable = _dbname + "." + _tableName;
	Jstr dbcolumn;
	AbaxString schemaStr;
	const JagSchemaRecord *record =  _tableschema->getAttr( dbtable );
	if ( ! record ) {
		prt(("s8912 _tableschema->getAttr(%s) error, _tableRecord is not updated\n", dbtable.s() ));
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

  	//char buf[32];
	for ( int i = 0; i < vec.size(); ++i ) {	
		idxname = vec[i];
    	dbindex = _dbname + "." + idxname;
		// printf("s8380 _indexlist.append(%s)\n", dbindex.s() );
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
		pindex = _objectLock->writeLockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i], _tableschema, _indexschema, _replicateType, 1 );
		if ( pindex ) {
			pindex->flushBlockIndexToDisk();
			_objectLock->writeUnlockIndex( JAG_INSERT_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
		}
	}
}

// static
// currently, doWriteIt has only one host name, so it can only been write one by one 
// ( use mutex in jda to protect, with last flag true )
void JagTable::doWriteIt( JagDataAggregate *jda, const JagParseParam *parseParam, 
						  const Jstr &host, const char *buf, jagint buflen, const JagSchemaRecord *nrec )
{

	// first check if there is any _having clause
	// if having exists, convert natrual format to dbformat,
	if ( parseParam->hasHaving ) {
		// having tree
		// ExprElementNode *root = (parseParam->havingTree).getRoot();
	} else {
		//if ( jda ) jda->writeit( host, buf, buflen, nrec, true );
		if ( jda ) jda->writeit( 0, buf, buflen, nrec, true );
	}
}

// static
// format insert into cmd, and use pcli to find servers to be sent for insert
// iscmd should be either "insert into TABLE (COL1,COL2)" or "insert into TABLE" format
void JagTable::formatInsertFromSelect( const JagParseParam *pParam, const JagSchemaAttribute *attrs, 
									   const char *finalbuf, const char *buffers, jagint finalsendlen, jagint numCols,
									   JaguarCPPClient *pcli, const Jstr &iscmd )
{
	// format insert cmd
	Jstr insertCmd = iscmd + " values ( ", oneData;
	//prt(("s10098 insertCmd=[%s]\n", insertCmd.s() ));
	if ( pParam->hasColumn ) {
		for ( int i = 0; i < pParam->selColVec.size(); ++i ) {
			oneData = formOneColumnNaturalData( finalbuf, pParam->selColVec[i].offset, 
												pParam->selColVec[i].length, pParam->selColVec[i].type );
			if ( 0 == i ) {
				insertCmd += Jstr("'") + oneData.s() + "'";
			} else {
				insertCmd += Jstr(",'") + oneData.s() + "'";
			}
		}
		insertCmd += ");";
		//prt(("s10028 hasColumn insertCmd=[%s]\n", insertCmd.s() ));
	} else {
		for ( int i = 0; i < numCols; ++i ) {
			if ( attrs[i].colname != "spare_" ) {
				oneData = formOneColumnNaturalData( buffers, attrs[i].offset, attrs[i].length, attrs[i].type );
				if ( 0 == i ) {
					insertCmd += Jstr("'") + oneData.s() + "'";
				} else {
					insertCmd += Jstr(",'") + oneData.s() + "'";
				}
			}			
		}
		insertCmd += ");";
		//prt(("s10024 has no column insertCmd=[%s]\n", insertCmd.s() ));
	}
	
	// put insert cmd into cli
	pcli->concurrentDirectInsert( insertCmd.s(), insertCmd.size() );
}

bool JagTable::hasSpareColumn()
{
	Jstr colname;
	for ( int i = 0; i < _numCols; ++i ) {
		colname = (*(_tableRecord.columnVector))[i].name.s();
		// prt(("s4772 hasSpareColumn i=%d colname=[%s]\n", i, colname.s() ));
		if ( colname == "spare_" ) return true;
	}
	return false;
}

void JagTable::setupSchemaMapAttr( int numCols )
{
	//prt(("s38007 JagTable::setupSchemaMapAttr numCols=%d ...\n", numCols ));
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
		dbcolumn = _dbtable + "." + (*(_tableRecord.columnVector))[i].name.s();
		
		_schAttr[i].dbcol = dbcolumn;
		_schAttr[i].objcol = _tableName + "." + (*(_tableRecord.columnVector))[i].name.s();
		_schAttr[i].colname = (*(_tableRecord.columnVector))[i].name.s();
		_schAttr[i].isKey = (*(_tableRecord.columnVector))[i].iskey;

		if ( _schAttr[i].isKey ) { 
			++ _numKeys; 
			prt(("s48863 JagTable::setupSchemaMapAttr found _numKeys=%d\n", _numKeys ));
		}

		_schAttr[i].offset = (*(_tableRecord.columnVector))[i].offset;
		_schAttr[i].length = (*(_tableRecord.columnVector))[i].length;
		_schAttr[i].sig = (*(_tableRecord.columnVector))[i].sig;
		_schAttr[i].type = (*(_tableRecord.columnVector))[i].type;
		_schAttr[i].begincol = (*(_tableRecord.columnVector))[i].begincol;
		_schAttr[i].endcol = (*(_tableRecord.columnVector))[i].endcol;
		_schAttr[i].srid = (*(_tableRecord.columnVector))[i].srid;
		// dummy1 dummy2
		_schAttr[i].metrics = (*(_tableRecord.columnVector))[i].metrics;
		//_schAttr[i].rollupWhere = (*(_tableRecord.columnVector))[i].rollupWhere;
		// _schAttr[i].dummy3 = (*(_tableRecord.columnVector))[i].dummy3;
		_schAttr[i].isUUID = false;
		_schAttr[i].isFILE = false;

		_tablemap->addKeyValue(dbcolumn, i);
		//prt(("s4015 _tablemap->addKeyValue(dbcolumn=[%s] ==> i=%d\n", dbcolumn.s(), i ));

		rc = *((*(_tableRecord.columnVector))[i].spare+1);
		rc2 = *((*(_tableRecord.columnVector))[i].spare+4);
		if ( rc == JAG_C_COL_TYPE_UUID[0]) {
			_schAttr[i].isUUID = true;
		} else if ( rc == JAG_C_COL_TYPE_FILE[0] ) {
			_schAttr[i].isFILE = true;
		}
		
		// setup enum string list and/or default value for empty
		if ( rc == JAG_C_COL_TYPE_ENUM[0] || rc2 == JAG_CREATE_DEFINSERTVALUE ) {
			defvalStr = "";
			_tableschema->getAttrDefVal( dbcolumn, defvalStr );
			// for default string, strsplit with '|', and use last one as default value
			// regular type has only one defvalStr and use it
			// enum type has many default strings separated by '|', where the first several strings are enum range
			// and the last one is default value if empty
			//prt(("s2202292 after getAttrDefVal dbcolumn=[%s] defvalStr=[%s]\n", dbcolumn.s(), defvalStr.s() ));
			JagStrSplitWithQuote sp( defvalStr.s(), '|' );
			
			if ( rc == JAG_C_COL_TYPE_ENUM[0] ) {
				// _schAttr[i].enumList = new JagVector<Jstr>();
				for ( int k = 0; k < sp.length(); ++k ) {
					_schAttr[i].enumList.append( strRemoveQuote( sp[k].s() ) );
					//prt(("s111029 i=%d enumList.append [%s]\n", i, sp[k].s() ));
				}
			}

			if ( rc2 == JAG_CREATE_DEFINSERTVALUE && sp.length()>0 ) {
				_schAttr[i].defValue = strRemoveQuote( sp[sp.length()-1].s() );
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
	JagFixString fstr = JagFixString( kvbuf, KEYLEN, KEYLEN );
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
			jagunlink( fpath.s() );

			/***
			prt(("s6360 i=%d dbcol=[%s] objcol=[%s] colname=[%s] offset=%d length=%d fpath=[%s]\n",
				i, _schAttr[i].dbcol.s(), _schAttr[i].objcol.s(), _schAttr[i].colname.s(),  
				_schAttr[i].offset, _schAttr[i].length, fpath.s() ));
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
				// prt(("s0293 colname=[%s] is a file type\n", colname.s() ));
				return true;
			}
		}
	}

	return false;
}

void JagTable::setGetFileAttributes( const Jstr &hdir, JagParseParam *parseParam, const char *buffers[] )
{
	// if getfile, arrange point query to be size of file/modify time of file/md5sum of file/file path ( for actural data )
	Jstr ddcol, inpath, instr, outstr; 
	int getpos; 
	char fname[JAG_FILE_FIELD_LEN+1]; 
	struct stat sbuf;
	// raydebug( stdout, JAG_LOG_HIGH, "s3331 setGetFileAttributes selColVec.size=%d\n", parseParam->selColVec.size() );
	for ( int i = 0; i < parseParam->selColVec.size(); ++i ) {
		ddcol = _dbtable + "." + parseParam->selColVec[i].getfileCol.s();
		if ( _tablemap->getValue(ddcol, getpos) ) {
			memcpy( fname, buffers[0]+_schAttr[getpos].offset, _schAttr[getpos].length );
			inpath = _darrFamily->_sfilepath + "/" + hdir + "/" + fname;
			if ( stat(inpath.s(), &sbuf) == 0 ) {
				if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_SIZE ) {
					outstr = longToStr( sbuf.st_size );
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_SIZEGB ) {
					outstr = longToStr( sbuf.st_size/ONE_GIGA_BYTES);
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_SIZEMB ) {
					outstr = longToStr( sbuf.st_size/ONE_MEGA_BYTES);
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_TIME ) {
					char octime[JAG_CTIME_LEN]; memset(octime, 0, JAG_CTIME_LEN);
 					jag_ctime_r(&sbuf.st_mtime, octime);
					octime[JAG_CTIME_LEN-1] = '\0';
					outstr = octime;
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_MD5SUM ) {
					if ( lastChar(inpath) != '/' ) {
						instr = Jstr("md5sum ") + inpath;
						outstr = Jstr(psystem( instr.s() ).s(), 32);
					} else {
						outstr = "";
					}
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_FPATH ) {
					outstr = inpath;
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_HOST ) {
					outstr = _servobj->_localInternalIP;
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_HOSTFPATH ) {
					outstr = _servobj->_localInternalIP + ":" + inpath;
				} else if ( parseParam->selColVec[i].getfileType == JAG_GETFILE_TYPE ) {
					JagStrSplit sp(fname, '.');
					if ( sp.length() >= 2 ) {
						outstr = sp[sp.length()-1];
					} else {
						outstr = "?";
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

void JagTable::formatPointsInLineString( int nmetrics, JagLineString &line, char *tablekvbuf, const JagPolyPass &pass, 
										 JagVector<JagDBPair> &retpair, Jstr &errmsg ) const
{
	prt(("s4928 formatPointsInLineString line.size=%d\n", line.size() ));
	int getpos;
	Jstr point;
	char ibuf[32];
	int rc;
	Jstr pointx = pass.colname + ":x";
	Jstr pointy = pass.colname + ":y";
	Jstr pointz = pass.colname + ":z";

	//prt(("s2935 pointx=[%s] pointz=[%s] pass.getxmin=%d pass.getymin=%d \n", pointx.s(), pointz.s(), pass.getxmin, pass.getymin ));

	for ( int j=0; j < line.size(); ++j ) {
		JagPoint &jPoint = line.point[j];
		point = "geo:xmin";
		getpos = pass.getxmin;
     	rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.xmin).s(), errmsg, point.s(), 
							_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
							_schAttr[getpos].type );

		if ( pass.getymin >= 0 ) {
    		point =  "geo:ymin";
    		getpos = pass.getymin;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.ymin).s(), errmsg, point.s(), 
								_schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
   	    if ( pass.getzmin >= 0 ) {
   			point = "geo:zmin";
   			getpos = pass.getzmin;
			if ( getpos >= 0 ) {
       				rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.zmin).s(), errmsg, point.s(), 
									_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].sig, 
									_schAttr[getpos].type );
		   }
   	    }
    
		if ( pass.getxmax >= 0 ) {
   			point = "geo:xmax";
   			getpos = pass.getxmax;
   			rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.xmax).s(), errmsg, point.s(), 
								_schAttr[getpos].offset, 
    						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
    
		if ( pass.getymax >= 0 ) {
    		point =  "geo:ymax";
    		getpos = pass.getymax;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.ymax).s(), errmsg, point.s(), 
								_schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }
   	    // if ( is3D && getzmax >=0 ) 
   	    if (  pass.getzmax >=0 ) {
    		point = "geo:zmax";
    		getpos = pass.getzmax;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, doubleToStr(pass.zmax).s(), errmsg, 
								point.s(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
   	    }

		if ( pass.getid >= 0 ) {
    		point = "geo:id";
    		getpos = pass.getid;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, pass.lsuuid.s(), errmsg, point.s(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }

		if ( pass.getcol >= 0 ) {
    		point = "geo:col";
			sprintf(ibuf, "%d", pass.col+1 );
			//prt(("s5033 getcol=%d ibuf=[%s]\n", pass.getcol, ibuf ));
    		getpos = pass.getcol;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.s(), _schAttr[getpos].offset, 
       						   _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	    }

		if ( pass.getm >= 0 ) {
    		point = "geo:m"; sprintf(ibuf, "%d", pass.m+1 ); getpos = pass.getm;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.s(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}

		if ( pass.getn >= 0 ) {
    		point = "geo:n"; sprintf(ibuf, "%d", pass.n+1 ); getpos = pass.getn;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.s(), _schAttr[getpos].offset, 
        									_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}

		if ( pass.geti >= 0 ) {
    		point = "geo:i";
			sprintf(ibuf, "%09d", j+1 );
			//prt(("s5034 geti=%d ibuf=[%s]\n", pass.geti, ibuf ));
    		getpos = pass.geti;
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, ibuf, errmsg, point.s(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
		}
        
       	if ( pass.getx >= 0 ) {
    		getpos = pass.getx;
			#if 1
			prt(("s4005 getx=%d pointx=[%s] line.point[j].x=[%s]\n", getpos, pointx.s(), line.point[j].x ));
			prt(("_schAttr[getpos].offset=%d _schAttr[getpos].length=%d type=[%s]\n", 
					_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].type.s() ));
			#endif
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, line.point[j].x, errmsg, 
       	 						pointx.s(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
       	}
      	if ( pass.gety >= 0 ) {
    		getpos = pass.gety;
			#if 1
			prt(("s4005 gety=%d pointy=[%s] line.point[j].y=[%s]\n", getpos, pointy.s(), line.point[j].y ));
			prt(("_schAttr[getpos].offset=%d _schAttr[getpos].length=%d type=[%s]\n", 
					_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].type.s() ));
			#endif
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, line.point[j].y, errmsg, 
       		 					pointy.s(), _schAttr[getpos].offset, 
       							_schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
       	}
        
		//prt(("s7830 pass.is3D=%d pass.getz=%d\n", pass.is3D, pass.getz ));
       	if ( pass.is3D && pass.getz >= 0 ) {
    		getpos = pass.getz;
			#if 1
			prt(("s4005 getz=%d pointz=[%s] line.point[j].z=[%s]\n", getpos, pointz.s(), line.point[j].z ));
			prt(("_schAttr[getpos].offset=%d _schAttr[getpos].length=%d type=[%s]\n", 
					_schAttr[getpos].offset, _schAttr[getpos].length, _schAttr[getpos].type.s() ));
			#endif
       		rc = formatOneCol( pass.tzdiff, pass.srvtmdiff, tablekvbuf, 
								line.point[j].z, errmsg, 
       							pointz.s(), _schAttr[getpos].offset, _schAttr[getpos].length, 
								_schAttr[getpos].sig, _schAttr[getpos].type );
       	}

		prt(("s2238 j=%d jPoint.metrics.size=%d\n", j, jPoint.metrics.size() ));
		formatMetricCols( pass.tzdiff, pass.srvtmdiff, pass.dbtab, pass.colname, nmetrics, jPoint.metrics, tablekvbuf );

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
								int &getid, int &getcol, int &getm, int &getn, int &geti ) const
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

void JagTable::appendOther( JagVector<OtherAttribute> &otherVec,  int n, bool isSub ) const
{
    OtherAttribute other;
    other.issubcol = isSub;
    for ( int i=0; i < n; ++i ) {
        otherVec.append( other );
    }
}

void JagTable::formatMetricCols( int tzdiff, int srvtmdiff, const Jstr &dbtab, const Jstr &colname, 
								 int nmetrics, const JagVector<Jstr> &metrics, char *tablekvbuf ) const
{
	int len = metrics.size();
	if ( len < 1 ) return;
	Jstr col, dbcolumn;
	int getpos;
	int rc;
	Jstr errmsg;
	for ( int i = 0; i < len; ++i ) {
		col = colname + ":m" + intToStr(i+1);
		dbcolumn = dbtab + "." + col;  // test.tab1.col:m1  test.tab1.col:m2 test.tab1.col:m3 
		rc = _tablemap->getValue(dbcolumn, getpos);
		if ( ! rc ) continue;
		formatOneCol( tzdiff, srvtmdiff, tablekvbuf, metrics[i].s(), errmsg,
		              col.s(), _schAttr[getpos].offset,
		              _schAttr[getpos].length, _schAttr[getpos].sig, _schAttr[getpos].type );
	}
}

bool JagTable::hasRollupColumn() const
{
	bool rc = _tableRecord.hasRollupColumn();
	return rc;
}

bool JagTable::hasTimeSeries()  const
{
	Jstr sysTser;
	bool rc = _tableRecord.hasTimeSeries( sysTser );
	return rc;
}

bool JagTable::hasTimeSeries( Jstr &tser )  const
{
	Jstr sysTser;
	bool rc  = _tableRecord.hasTimeSeries( sysTser );
	if (! rc ) return false;

	tser = JagSchemaRecord::translateTimeSeriesToStrs( sysTser );
	return true;

}

Jstr JagTable::timeSeriesRentention() const
{
	return _tableRecord.timeSeriesRentention();
}

int JagTable
::rollupPair( const JagRequest &req, JagDBPair &inspair, const JagVector<OtherAttribute> &rollupVec )
{
	int 		setindexnum = _indexlist.size(); 
	int 		cnt = 0;
	JagIndex 	*lpindex[_indexlist.size()];
	AbaxBuffer 	bfr;
	Jstr 		dbcolumn;
	const 		JagHashStrInt *maps[1];
	const 		JagSchemaAttribute *attrs[1];	

	maps[0] = _tablemap;
	attrs[0] = _schAttr;
	
	setindexnum = 0;
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		lpindex[i] = _objectLock->writeLockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], 
										   	    _tableschema, _indexschema, _replicateType, 1 );
		++setindexnum; 
	}

	int keylen[1];
	int numKeys[1];	
	keylen[0] = KEYLEN;
	numKeys[0] = _numKeys;
	cnt = 0;

	JagFixString getkey ( inspair.key.s(), KEYLEN, KEYLEN );
	JagStrSplit sp( _tableName, '@');
	if ( sp.size() < 2 ) return 0;
	Jstr twindow = sp[1];
	convertTimeToWindow( req.session->timediff, twindow, (char*)getkey.s(), "" );
	char 	*tableoldbuf = (char*)jagmalloc(KEYVALLEN+1);
	char 	*tablenewbuf = (char*)jagmalloc(KEYVALLEN+1);

	JagDBPair getDBpair( getkey );
	inspair.key = getkey;

	findPairRollupOrInsert( inspair, getDBpair, tableoldbuf, tablenewbuf, setindexnum, lpindex );
	++cnt;

	int totCols = _tableRecord.columnVector->size();
	int nonDateTimeCols = 0;
	Jstr colType;
	for ( int i = 0; i < totCols; i ++ ) {
		if ( ! (*(_tableRecord.columnVector))[i].iskey ) break;
		colType = (*(_tableRecord.columnVector))[i].type;
		if ( colType == JAG_C_COL_TYPE_DATETIMESEC ) {
			continue;
		}
		++ nonDateTimeCols;
	}
	for ( int K = 1; K <= nonDateTimeCols; ++K ) {
		starCombinations( nonDateTimeCols, K, inspair, getDBpair, tableoldbuf, tablenewbuf, setindexnum, lpindex );
	}
	free( tableoldbuf );
	free( tablenewbuf );
	for ( int i = 0; i < _indexlist.size(); ++i ) {
		if ( ! lpindex[i] ) { continue; }
		_objectLock->writeUnlockIndex( JAG_UPDATE_OP, _dbname, _tableName, _indexlist[i], _replicateType, 1 );
	}
	return cnt;
}

void JagTable::doRollUp( const JagDBPair &inspair, const char *dbBuf, char *newbuf )
{
	Jstr  colType, errmsg;
	int   offset, length, sig;
	bool  rc, iskey;
	double inv ;
	assert( inspair.key.size() == KEYLEN );
	assert( inspair.value.size() == VALLEN );
	memcpy( newbuf, dbBuf, KEYLEN+VALLEN );

	if ( _counterOffset < 0 ) {
		return;
	}
	char *inBuf = inspair.newBuffer(); 
	double dbCounter = rayatof( dbBuf + _counterOffset, _counterLength );
	bool hasValidCol = false;
	char  cbuf[64];

	Jstr rootname;
	Jstr name;
	int totCols = _tableRecord.columnVector->size();
	for ( int i = 0; i < totCols; i ++ ) {
		iskey = (*(_tableRecord.columnVector))[i].iskey;
		if ( iskey ) continue;  

		colType = (*(_tableRecord.columnVector))[i].type;
		if ( !isInteger( colType ) && ! isFloat( colType ) ) {
			continue;
		} 
		offset = (*(_tableRecord.columnVector))[i].offset;
		length = (*(_tableRecord.columnVector))[i].length;
		sig = (*(_tableRecord.columnVector))[i].sig;
		inv = rayatof( inBuf + offset, length );

		rootname = (*(_tableRecord.columnVector))[i].name.s();
		if ( rootname.containsStr("::") || rootname == "counter" || rootname == "spare_" ) {
			prt(("s53215 skip column %s in rollup\n", rootname.s() ));
			continue;
		}

		name = rootname + "::sum";
		rc = rollupType( name, colType, inv, dbCounter, dbBuf, newbuf, errmsg );
		if ( rc ) { hasValidCol = true; }

		name = rootname + "::min";
		rc = rollupType( name, colType, inv, dbCounter, dbBuf, newbuf, errmsg );
		if ( rc ) { hasValidCol = true; }

		name = rootname + "::max";
		rc = rollupType( name, colType, inv, dbCounter, dbBuf, newbuf, errmsg );
		if ( rc ) { hasValidCol = true; }

		name = rootname + "::var";
		rc = rollupType( name, colType, inv, dbCounter, dbBuf, newbuf, errmsg );
		if ( rc ) { hasValidCol = true; }

		name = rootname + "::avg";
		rc = rollupType( name, colType, inv, dbCounter, dbBuf, newbuf, errmsg );
		if ( rc ) { hasValidCol = true; }

		if ( isInteger( colType ) ) {
			sprintf( cbuf, "%lld", (long long)(round(inv)) );
		} else { 
			sprintf( cbuf, "%.10f", inv );
		} 
		formatOneCol( 0, 0, newbuf, cbuf, errmsg, "dummy", offset, length, sig, colType );

	}

	free( inBuf );

	if ( hasValidCol ) {
		dbCounter = dbCounter + 1.0; 
		sprintf( cbuf, "%lld", (long long)(round(dbCounter)) );
		rc = formatOneCol( 0, 0, newbuf, cbuf, errmsg, "dummy", _counterOffset, _counterLength, 0, JAG_C_COL_TYPE_DBIGINT );
		if ( 1 == rc ) { // OK
			JagDBPair resPair;
			resPair.point( newbuf, KEYLEN, newbuf+KEYLEN, VALLEN );
			rc = _darrFamily->set( resPair );
			if ( rc ) {
				prt(("s443010 _darrFamily->set OK\n" )); 
			} else {
				prt(("s444014 _darrFamily->set error\n" )); 
			}
		} else {
			prt(("s402339 formatOneCol error\n"));
		}
	} else {
		prt(("s444034 rollup not done\n" )); 
	}
}

bool JagTable
::rollupType( const Jstr &name, const Jstr &colType, double inv, 
              double dbCounter, const char *dbBuf, char *newbuf, Jstr &errmsg )
{
	double finv;
	int rc, offset, length, sig, getpos;

	Jstr dbcolName = _dbtable + "." + name;
	rc = _tablemap->getValue( dbcolName, getpos);
	if ( ! rc ) {
		return false;
	}

	offset = (*(_tableRecord.columnVector))[getpos].offset;
	length = (*(_tableRecord.columnVector))[getpos].length;
	double dbv = rayatof( dbBuf + offset, length );

	char  cbuf[64];
	if ( name.containsStr("::sum") ) {
		finv = inv + dbv;
	} else if ( name.containsStr("::avg") ) {
		finv = dbv + (inv - dbv)/( dbCounter + 1.0);
	} else if ( name.containsStr("::min") ) {
		finv = ( inv < dbv ) ? inv : dbv;
	} else if ( name.containsStr("::max")  ) {
		finv = ( inv > dbv ) ? inv : dbv;
	} else if ( name.containsStr("::var")  ) {
		const char *pc = strchr( name.s(), ':');
		Jstr avgname(name.s(), pc-name.s() );
		avgname += "::avg";
		Jstr dbcolName = _dbtable + "." + avgname;
		int avgpos;
		rc = _tablemap->getValue( dbcolName, avgpos);
		if ( ! rc ) {
			return false;
		}

		int avgoffset = (*(_tableRecord.columnVector))[avgpos].offset;
		int avglength = (*(_tableRecord.columnVector))[avgpos].length;
		double avgVal = rayatof( dbBuf + avgoffset, avglength );
		double avgNew = avgVal + ( double(inv) - avgVal)/dbCounter;
		finv = dbv + (inv - avgVal ) * ( inv - avgNew );
		prt(("s43220 ::var dbv=%.3f inv=%.3f avgOff=%d avgL=%d avgV=%.5f avgN=%.4f finv=%.4f Cnt=%.4f\n", 
			  dbv, inv, avgoffset, avglength, avgVal, avgNew, finv, dbCounter ));
	} else {
		return false;
	}

	if ( length >= 63 ) return false;
	sig = (*(_tableRecord.columnVector))[getpos].sig;

	if ( isInteger( colType ) ) {
		sprintf( cbuf, "%lld", (long long)(round(finv)) );
	} else if ( isFloat( colType ) ) {
		sprintf( cbuf, "%.10f", finv );
	} else {
		return false;
	}

	rc = formatOneCol( 0, 0, newbuf, cbuf, errmsg, "dummy", offset, length, sig, colType );
	if ( 1 == rc ) {
		return true;
	} else {
		return false;
	}
}

bool JagTable::convertTimeToWindow( int tzdiff, const Jstr &twindow, char *kbuf, const Jstr &givenColName )
{
	bool 	iskey;
	Jstr 	colType;
	jagint  tval;
	int  	offset, len;
	jagint 	startTime;
	Jstr 	errmsg, name;
	char  	inbuf[32];
	bool 	rc;
	int 	srvtmdiff  = _servobj->servtimediff;

	for ( int i = 0; i < (*_tableRecord.columnVector).size(); ++i ) {
		iskey = (*(_tableRecord.columnVector))[i].iskey;
		if ( ! iskey ) break; 
		colType = (*_tableRecord.columnVector)[i].type;
		if ( ! isDateAndTime( colType ) ) { continue; }

		name = (*_tableRecord.columnVector)[i].name.s();
		if ( givenColName.size() > 0 ) {
			if ( name != givenColName ) {
				continue; 
			}
		} else {
		}

		offset = (*_tableRecord.columnVector)[i].offset;
		len = (*_tableRecord.columnVector)[i].length;
		tval = rayatol( kbuf+offset, len ); 

		if ( colType == JAG_C_COL_TYPE_TIMESTAMPSEC  || colType == JAG_C_COL_TYPE_DATETIMESEC ) {
			startTime = segmentTime( tzdiff, tval, twindow ); // seconds
		} else if ( colType == JAG_C_COL_TYPE_TIMESTAMPMILL || colType == JAG_C_COL_TYPE_DATETIMESEC ) {
			tval = tval / 1000; // to seconds
			startTime = segmentTime( tzdiff, tval, twindow ); // seconds
			startTime *= 1000;
		} else if ( colType == JAG_C_COL_TYPE_DATETIME || colType == JAG_C_COL_TYPE_TIMESTAMP ) {
			tval = tval / 1000000; // to seconds
			startTime = segmentTime( tzdiff, tval, twindow ); // seconds
			startTime *= 1000000;
		} else if ( colType == JAG_C_COL_TYPE_TIMESTAMPNANO || colType == JAG_C_COL_TYPE_DATETIMENANO ) {
			tval = tval / 1000000000; // to seconds
			startTime = segmentTime( tzdiff, tval, twindow ); // seconds
			startTime *= 1000000000;
		}
		prt(("s353402 process this col cvted tval=%lld\n", tval));

		sprintf(inbuf, "%lld", startTime );
		rc = formatOneCol( tzdiff, srvtmdiff, kbuf, inbuf, errmsg, name, offset, len, 0, colType );
		if ( 1 != rc ) {
			prt(("s0391838 error formatOneCol name=[%s] colType=[%s] rc=%d\n", name.s(), colType.s(), rc ));
			return false;
		} else {
			prt(("s333301 formatOneCol name=[%s] colType=[%s] offset=%d len=%d tval=%lld startTime=%lld\n", 
					name.s(), colType.s(), offset, len, tval, startTime, startTime ));
		}
		prt(("s033204 inbuf=[%s]\n", inbuf ));

		break;
	}

	return true;
}

jagint JagTable::segmentTime( int tzdiff, jagint tval, const Jstr &twindow )
{
	jagint  cycle = twindow.toInt();
	char unit = twindow.lastChar();

	tval += tzdiff*60; 

	jagint seg = -1;
	if ( 's' == unit ) {
		seg =  JagTime::getStartTimeSecOfSecond( tval, cycle ); // 1--15: start from 0-hour of the day
	} else if ( 'm' == unit ) {
		seg =  JagTime::getStartTimeSecOfMinute( tval, cycle ); // 1--15: start from 0-hour of the day
	} else if ( 'h' == unit ) {
		seg =  JagTime::getStartTimeSecOfHour( tval, cycle ); // 1--15: start from 0-hour of the day
	} else if ( 'd' == unit ) {
		seg =  JagTime::getStartTimeSecOfDay( tval, cycle ); // 1--15: start from 0-hour of the day
	} else if ( 'w' == unit ) {
		seg =  JagTime::getStartTimeSecOfWeek( tval ); // 1 week only; start from Sunday
	} else if ( 'M' == unit ) {
		seg =  JagTime::getStartTimeSecOfMonth( tval, cycle ); // cycle: 1, 2, 3, 4, 6; start from first of month
	} else if ( 'q' == unit ) {
		seg =  JagTime::getStartTimeSecOfQuarter( tval, cycle ); // cycle 1 or 2; start from first day of quarter
	} else if ( 'y' == unit ) {
		seg =  JagTime::getStartTimeSecOfYear( tval, cycle );  // any year; start from new year's day
	} else if ( 'D' == unit ) {
		seg =  JagTime::getStartTimeSecOfDecade( tval, cycle );  // any decade; start from new year's day
	} else {
	}
	
	return seg;
}

int JagTable::findPairRollupOrInsert( JagDBPair &inspair, JagDBPair &getDBpair, 
									  char *tableoldbuf, char *tablenewbuf, int setindexnum, JagIndex *lpindex[] )
{
	int cnt;
	bool didRollUp;
	if (  ! _darrFamily->get( getDBpair ) ) {
		insertPair( inspair, 0, false );
		cnt = 0;
		didRollUp = false;
	} else {
		memset( tableoldbuf, 0, KEYVALLEN+1 );
		memset( tablenewbuf, 0, KEYVALLEN+1 );
		getDBpair.toBuffer( tableoldbuf );

		doRollUp( inspair, tableoldbuf, tablenewbuf );

		cnt = 1;
		didRollUp = true;
	}

	if ( didRollUp && _indexlist.size() > 0 && setindexnum ) {
		dbNaturalFormatExchange( tableoldbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
		dbNaturalFormatExchange( tablenewbuf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
		for ( int i = 0; i < setindexnum; ++i ) {
			if ( lpindex[i] ) {
				lpindex[i]->updateFromTable( tableoldbuf, tablenewbuf );
			}
		}
	}

	return cnt;
}

void JagTable::initStarPositions( JagVector<int> &pointer, int K )
{
	bool iskey;
	int totCols = _tableRecord.columnVector->size();
	Jstr colType;
	for ( int i = 0; i < totCols; i ++ ) {
		iskey = (*(_tableRecord.columnVector))[i].iskey;
		if ( ! iskey ) break;
		colType = (*(_tableRecord.columnVector))[i].type;
		if ( isDateAndTime( colType ) ) { continue; }

		pointer.append(i);
		if ( pointer.size() == K ) {
			break;
		}
	}
}

void JagTable::starCombinations( int N, int K, JagDBPair &inspair, JagDBPair &getDBpair, 
						 char *tableoldbuf, char *tablenewbuf, int setindexnum, JagIndex *lpindex[] )
{
	if ( N < K ) return;
	bool rc;
	JagVector<int> pointer(K);
	initStarPositions( pointer, K );
	while ( true ) {
		fillStarsAndRollup( pointer, inspair, getDBpair, tableoldbuf, tablenewbuf, setindexnum, lpindex );
		rc = movePointerPositions( pointer ); 
		if ( ! rc ) break;
	}
}

void JagTable::fillStarsAndRollup( const JagVector<int> &pointer, JagDBPair &inspair, JagDBPair &getDBpair, 
								   char *tableoldbuf, char *tablenewbuf, int setindexnum, JagIndex *lpindex[] )
{
	JagDBPair starGetDBPair( getDBpair.key );
	JagDBPair starInsPair( inspair.key, inspair.value );
	fillStars( pointer, starGetDBPair );
	fillStars( pointer, starInsPair );
	findPairRollupOrInsert( starInsPair, starGetDBPair, tableoldbuf, tablenewbuf, setindexnum, lpindex );
}

bool JagTable::movePointerPositions( JagVector<int> &pointer )
{
	bool rc;
	int k = pointer.size()-1;
	if ( k < 0 ) return false;

	while ( true ) {
		rc = findNextPositions( pointer, k );
		if ( rc ) {
			return true;
		} else {
			k = k -1;  
			if ( k < 0 ) {
				return false;
			}
		}
	}
}

bool JagTable::findNextPositions( JagVector<int> &pointer, int k  )
{
	int N = pointer.size();
	int startPosition;
	if ( pointer[k] == -1 ) {
		startPosition = pointer[k-1]+1;
	} else {
		startPosition = pointer[k]+1;
	}

	int nextPosition = findNextFreePosition( startPosition );
	if ( nextPosition < 0 ) {
		pointer[k] = -1;
		return false;
	}
	pointer[k] = nextPosition;

	for ( int i = k+1; i < N; ++i ) {
		nextPosition = findNextFreePosition( nextPosition+1 );
		if ( nextPosition < 0 ) {
			pointer[i] = -1;
			return false;
		}
		pointer[i] = nextPosition;
	}

	return true;
}

int JagTable::findNextFreePosition( int startPosition )
{
	int pos = -1;
	int totCols = _tableRecord.columnVector->size();
	bool found = false;
	for ( int i = startPosition; i < totCols; ++i ) {
		if ( ! (*(_tableRecord.columnVector))[i].iskey ) break;
		if ( isDateAndTime( (*(_tableRecord.columnVector))[i].type ) ) { continue; }
		found = true;
		pos = i;
		break;
	}

	return pos;
}

void JagTable::fillStars( const JagVector<int> &pointer, JagDBPair &starDBPair )
{
	char *buf = (char*)starDBPair.key.s();
	int  i, offset, len;
	for ( int k = 0; k < pointer.size(); ++k ) {
		i = pointer[k];
		offset = (*(_tableRecord.columnVector))[i].offset;
		len = (*(_tableRecord.columnVector))[i].length;
		memset( buf+offset, 0, len );
		*(buf+offset) = JAG_STARC;
	}
}

jagint JagTable::cleanupOldRecords( time_t tsec )
{
	Jstr colType;
	if ( _tableRecord.isFirstColumnDateTime( colType ) ) {
		time_t ttime = JagTime::getTypeTime( tsec, colType );
		if ( ttime == 0) {
			return 0;
		}

		jagint cnt = cleanupOldRecordsByOrderOrScan( 0, ttime, true );
		return cnt;
	} else {
		int colidx = _tableRecord.getFirstDateTimeKeyCol();
		if ( colidx < 0 ) {
			return 0;
		} 

		colType = (*_tableRecord.columnVector)[colidx].type;
		time_t ttime = JagTime::getTypeTime( tsec, colType );
		if ( ttime == 0) {
			return 0;
		}

		jagint cnt = cleanupOldRecordsByOrderOrScan( colidx, ttime, false );
		return cnt;
	}
}

jagint JagTable::cleanupOldRecordsByOrderOrScan( int idx, time_t ttime, bool byOrder )
{
	jagint cnt = 0;
	JagMinMax minmax;	
	minmax.setbuflen( KEYLEN );

 	minmax.offset = (*(_tableRecord.columnVector))[idx].offset;
    minmax.length = (*(_tableRecord.columnVector))[idx].length;

	JagMergeReader *ntr = NULL;
	_darrFamily->setFamilyRead( ntr, minmax.minbuf, minmax.maxbuf );
	if ( ! ntr ) {
		return 0;
	}

	char *buf = (char*)jagmalloc(KEYVALLEN+1);
	bool rc;
	time_t dbtime;

	while ( true ) {
		rc = ntr->getNext(buf);
		if ( !rc ) {
			prt(("s11103 getNext rc=%d break\n", rc ));
			break;
		}

		dbtime = rayatol( buf+minmax.offset, minmax.length );
		if ( byOrder ) {
			if ( dbtime >= ttime ) {
				break;
			}
		} else {
			if ( dbtime >= ttime ) {
				continue;
			}
		}
		
		dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // natural format -> db format
		JagDBPair pair( buf, KEYLEN );
		rc = _darrFamily->remove( pair );
		if ( rc ) {
			++cnt;
			dbNaturalFormatExchange( buf, _numKeys, _schAttr, 0,0, " " ); // db format -> natural format
			_removeIndexRecords( buf );
			removeColFiles(buf );
		} else {
		}
	}

	delete ntr;
	free( buf );
	return cnt;
}

void JagTable::refreshTableRecord()
{
	const JagSchemaRecord* rec= _tableschema->getAttr( _dbtable );
	_tableRecord = *rec;
}
