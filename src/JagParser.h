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
#ifndef _jag_parser_h_
#define _jag_parser_h_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <string>

#include <abax.h>
#include <JagCfg.h>
#include <JagUtil.h>
#include <JagVector.h>
#include <JagStrSplit.h>
#include <JagParseExpr.h>
#include <JagParseParam.h>
#include <JagStrSplitWithQuote.h>
#include <JagColumn.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

class JaguarCPPClient;
class JagDBServer;

class JagParser
{
  public:
    JagParser( const JagDBServer *srv = NULL, const JaguarCPPClient *cli = NULL );
	bool parseCommand( const JagParseAttribute &jpa, const AbaxDataString &cmd, JagParseParam *parseParam, 
						AbaxDataString &errmsg );
	const JagColumn* getColumn( const JagParseParam *pparam, const AbaxDataString &colName ) const;
	const JagColumn* getColumn( const AbaxDataString &db, const AbaxDataString &obj, const AbaxDataString &colName ) const;
	static bool  isComplexType( const AbaxDataString &rcs );
	static bool  isGeoType( const AbaxDataString &rcs );
	static bool  isPolyType( const AbaxDataString &rcs );
	bool isIndexCol( const AbaxDataString &db, const AbaxDataString &colName ) const;
	static AbaxDataString getColumns( const char *str );
	static int  checkLineStringData( const char *p );
	static int  checkLineString3DData( const char *p );
	static int  checkPolygonData( const char *p, bool mustClose );
	static int  checkMultiPolygonData( const char *p, bool mustClose, bool is3D );
	static int  checkPolygon3DData( const char *p, bool mustClose );
	static int  addLineStringData( JagLineString &linestr, const char *p );
	static int  getLineStringMinMax( const char *p, double &xmin, double &ymin, double &xmax, double &ymax );
	static int  addLineString3DData( JagLineString &linestr, const char *p );
	static int  getLineString3DMinMax( const char *p, double &xmin, double &ymin, double &zmin, 
										double &xmax, double &ymax, double &zmax );
	static int  addPolygonData( JagPolygon &pgon, const char *p, bool firstOnly, bool mustClose );
	static int  getPolygonMinMax( const char *p, double &xmin, double &ymin, double &xmax, double &ymax );
	static int  addPolygon3DData( JagPolygon &pgon, const char *p, bool firstOnly, bool mustClose );
	static int  getPolygon3DMinMax( const char *p, double &xmin, double &ymin, double &zmin, 
								    double &xmax, double &ymax, double &zmax );
	static int  addPolygonData( JagPolygon &pgon, const JagStrSplit &sp, bool firstOnly );
	static int  addPolygon3DData( JagPolygon &pgon, const JagStrSplit &sp, bool firstOnly );
	static void  addLineStringData( JagLineString &linestr, const JagStrSplit &sp );

	static int  addMultiPolygonData( JagVector<JagPolygon> &pgvec, const char *p, bool firstOnly, bool mustClose, bool is3D );
	static int  addMultiPolygonData( JagVector<JagPolygon> &pgvec, const JagStrSplit &sp, bool firstOnly, bool is3D );
	static int  getMultiPolygonMinMax( const char *p, double &xmin, double &ymin, double &xmax, double &ymax );
	static int  getMultiPolygon3DMinMax( const char *p, double &xmin, double &ymin, double &zmin, 
								         double &xmax, double &ymax, double &zmax );
	static AbaxDataString getFieldType( int srid );
	static AbaxDataString getFieldTypeString( int srid );
	static void removeEndUnevenBracket( char *str );
	static void removeEndUnevenBracketAll( char *str );

	
  private:
	int parseSQL( const JagParseAttribute &jpa, JagParseParam *parseParam, char *cmd, int len );
	int init( const JagParseAttribute &jpa, JagParseParam *parseParam );
	int setTableIndexList( short setType );
	int getAllClauses( short setType );
	int setAllClauses( short setType );
	int setSelectColumn();
	int setGetfileColumn();
	int setSelectGroupBy();
	int setSelectHaving();
	int setSelectOrderBy();
	int setSelectLimit();
	int setSelectTimeout();
	int setSelectPivot();
	int setSelectExport();
	int setLoadColumn();
	int setLoadLine();
	int setLoadQuote();
	int setInsertVector();
	int setUpdateVector();
	int setCreateVector( short setType );
	int setOneCreateColumnAttribute( CreateAttribute &cattr );
	AbaxDataString fillDataType( const char *gettok );
	int getColumnLength( const AbaxDataString &colType );
	
	// int setupCheckMap();
	bool  isValidGrantPerm( AbaxDataString &perm );
	bool  isValidGrantObj(  AbaxDataString &obj );
	void replaceChar( char *start, char oldc, char newc, char stopchar );
	void addCreateAttrAndColumn( bool isValue, CreateAttribute &cattr, int &coloffset );
	void addExtraOtherCols( const JagColumn *pcol, OtherAttribute &other, int &numCols );
	void setToRealType( const AbaxDataString &rcs, CreateAttribute &cattr );
	int getTypeNameArg( const char *gettok, AbaxDataString &tname, AbaxDataString &targ, int &collen, int &sig );
	bool hasPolyGeoType( const char *createSQL, int &dim );
	void addBBoxGeomKeyColumns( CreateAttribute &cattr, int polyDim, bool lead, int &offset ); 
	int convertJsonToOther( OtherAttribute &other, const char *json, int jsonlen ); 
	int convertJsonToPoint( const rapidjson::Document &dom, OtherAttribute &other );
	int convertJsonToLineString( const rapidjson::Document &dom, OtherAttribute &other );
	int convertJsonToPolygon( const rapidjson::Document &dom, OtherAttribute &other );
	int convertJsonToMultiPolygon( const rapidjson::Document &dom, OtherAttribute &other );
	int getEachRangeFieldLength( int srid ) const;


	
	// data members
	char *_gettok, *_saveptr;
	JagStrSplit _split;
	JagStrSplitWithQuote _splitwq;
	JagParseParam *_ptrParam;
	JagCfg *_cfg;

	const JagDBServer *_srv;
	const JaguarCPPClient *_cli;
	JagColumn   _dummy;
};

#endif
